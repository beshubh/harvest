use std::collections::HashSet;

use anyhow::Result;
use dashmap::DashSet;
use reqwest::Url;
use scraper::{Html, Selector};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;

use crate::analyzer::Analyzer;
use crate::data_models::Page;
use crate::db::PageRepo;

const MAX_FETCH_RETRIES: usize = 4;

pub struct Crawler {
    visited_urls: DashSet<String>,
    max_depth: usize,
    pages_repo: Arc<PageRepo>,
    crawl_tx: mpsc::Sender<(String, usize, bool)>,
    crawl_rx: Mutex<mpsc::Receiver<(String, usize, bool)>>,
    fetched_tx: mpsc::UnboundedSender<Page>,
    fetched_rx: Mutex<mpsc::UnboundedReceiver<Page>>,
    concurrency_semaphore: Arc<Semaphore>,
    text_analyzer: Arc<Analyzer>,
}

impl Crawler {
    pub fn new(
        max_depth: usize,
        pages_repo: PageRepo,
        max_concurrent_fetches: usize,
        frontier_size: usize,
        max_concurrent_scraps: usize,
    ) -> Crawler {
        let (crawl_tx, crawl_rx) = mpsc::channel(frontier_size);
        let (fetched_tx, fetched_rx) = mpsc::unbounded_channel();

        let pages_repo = Arc::new(pages_repo);
        Crawler {
            visited_urls: DashSet::new(),
            max_depth,
            pages_repo: pages_repo.clone(),
            crawl_tx: crawl_tx.clone(),
            crawl_rx: Mutex::new(crawl_rx),
            fetched_tx: fetched_tx.clone(),
            fetched_rx: Mutex::new(fetched_rx),
            concurrency_semaphore: Arc::new(Semaphore::new(max_concurrent_fetches)),
            text_analyzer: Arc::new(Analyzer::new(
                vec![Box::new(crate::analyzer::HTMLTagFilter::default())],
                Box::new(crate::analyzer::WhiteSpaceTokenizer),
                vec![
                    Box::new(crate::analyzer::StopWordTokenFilter),
                    Box::new(crate::analyzer::PorterStemmerTokenFilter),
                ],
                max_concurrent_scraps,
                pages_repo.clone(),
            )),
        }
    }

    pub async fn crawl(self: Arc<Self>, starting_url: String) -> Result<()> {
        self.clone().spawn_crawler(starting_url, 0).await.unwrap();
        self.clone().spawn_mongo_inserter().await;
        self.clone().text_analyzer.clone().spin()?;
        Ok(())
    }

    fn crawl_url(self: Arc<Self>, url: String, depth: usize, is_seed: bool) {
        let self_clone = self.clone();
        let semaphore = self_clone.concurrency_semaphore.clone();
        tokio::spawn(async move {
            let tx = self_clone.crawl_tx.clone();
            let fetched_tx = self_clone.fetched_tx.clone();

            let permit = semaphore.acquire().await.unwrap();
            let mut retried = 0;
            let mut html = Option::None;
            loop {
                if retried >= MAX_FETCH_RETRIES {
                    log::error!("max retries reached for url: {url}");
                    break;
                }
                let res = self_clone.fetch_page(&url).await;
                if let Err(e) = res {
                    log::error!("error fetching page {url}, error: {:#}", e);
                    retried += 1;
                } else {
                    html = Some(res.unwrap());
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;
            }
            drop(permit);
            if html.is_none() {
                return;
            }
            let html = html.unwrap();
            let res = self_clone.parse_html(&url, &html).await;
            match res {
                Ok((title, body, seen)) => {
                    // send fetched to be inserted to mongo.
                    let page = Page::new(
                        url.clone(),
                        title,
                        body,
                        seen.into_iter().collect(),
                        depth as u32,
                        is_seed,
                    );
                    fetched_tx.send(page.clone()).unwrap();

                    for link in &page.outgoing_links {
                        tx.send((link.clone(), depth + 1, false)).await.unwrap();
                    }
                }
                Err(e) => {
                    log::error!("error parsing html {url}, error: {:#}", e);
                }
            }
        });
    }

    async fn spawn_crawler(self: Arc<Self>, starting_url: String, depth: usize) -> Result<()> {
        let self_clone = self.clone();

        self.crawl_tx
            .send((starting_url, depth, true))
            .await
            .unwrap();
        tokio::spawn(async move {
            let mut rx = self_clone.crawl_rx.lock().await;

            while let Some((url, depth, is_seed)) = rx.recv().await {
                if depth > self_clone.max_depth {
                    continue;
                }
                // This is atomic - it checks AND inserts in one operation, preventing race conditions.
                if !self_clone.visited_urls.insert(url.clone()) {
                    log::warn!("url already visited: {url}");
                    continue;
                }
                log::info!("crawling url: {url}");
                self_clone.clone().crawl_url(url.clone(), depth, is_seed);
            }
            drop(rx); // why am I doing this? paranoia
        });

        Ok(())
    }

    async fn spawn_mongo_inserter(self: Arc<Self>) {
        let self_clone = self.clone();
        tokio::spawn(async move {
            let mut rx = self_clone.fetched_rx.lock().await;
            while let Some(page) = rx.recv().await {
                match self_clone.pages_repo.upsert(&page).await {
                    Ok(_) => {
                        if let Err(e) = self_clone.text_analyzer.analyze_tx.send(page) {
                            log::error!("error sending page to scrapper, error: {:#}", e);
                        }
                    }
                    Err(e) => {
                        log::error!("error inserting to mongo, error: {:#}", e);
                    }
                }
            }
        });
    }

    async fn fetch_page(&self, url: &str) -> Result<String> {
        let client = reqwest::Client::new();
        let res = client.get(url).send().await?;
        let body = res.text().await?;
        Ok(body)
    }

    async fn parse_html(
        &self,
        base_url: &str,
        html: &str,
    ) -> Result<(String, String, HashSet<String>)> {
        let base = Url::parse(base_url)?;
        let document = Html::parse_document(html);

        // TODO: handle errors
        let href_selector = Selector::parse("a").unwrap();
        let title_selector = Selector::parse("title").unwrap();

        // extract links
        let hrefs = document.select(&href_selector);
        let mut seen = HashSet::new();

        for element in hrefs {
            if let Some(href) = element.value().attr("href") {
                if let Ok(resolved) = base.join(href) {
                    if resolved.scheme() == "http" || resolved.scheme() == "https" {
                        if !self.visited_urls.contains(&resolved.to_string()) {
                            seen.insert(resolved.to_string());
                        }
                    }
                }
            }
        }

        let title = document
            .select(&title_selector)
            .next()
            .map(|t| t.text().collect::<String>().trim().to_string());
        let title = title.unwrap_or_else(|| "".to_string());

        Ok((title, html.to_string(), seen))
    }
}
