use std::collections::HashSet;

use anyhow::Result;
use dashmap::DashSet;
use reqwest::Url;
use scraper::{Html, Selector};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

use crate::data_models::CrawlResult;
use crate::db::CrawlResultRepo;

pub struct Crawler {
    visited_urls: DashSet<String>,
    max_depth: usize,
    crawl_results_repo: Arc<CrawlResultRepo>,
    crawl_tx: mpsc::Sender<(String, usize, bool)>,
    crawl_rx: Mutex<mpsc::Receiver<(String, usize, bool)>>,
    fetched_tx: mpsc::UnboundedSender<CrawlResult>,
    fetched_rx: Mutex<mpsc::UnboundedReceiver<CrawlResult>>,
}

impl Crawler {
    pub fn new(
        max_depth: usize,
        crawl_results_repo: CrawlResultRepo,
        concurrency: usize,
    ) -> Crawler {
        let (crawl_tx, crawl_rx) = mpsc::channel(concurrency);
        let (fetched_tx, fetched_rx) = mpsc::unbounded_channel();

        Crawler {
            visited_urls: DashSet::new(),
            max_depth,
            crawl_results_repo: Arc::new(crawl_results_repo),
            crawl_tx: crawl_tx.clone(),
            crawl_rx: Mutex::new(crawl_rx),
            fetched_tx: fetched_tx.clone(),
            fetched_rx: Mutex::new(fetched_rx),
        }
    }

    pub async fn crawl(self: Arc<Self>, starting_url: String) -> Result<()> {
        self.clone().spawn_crawler(starting_url, 0).await.unwrap();
        self.clone().spawn_mongo_inserter().await;
        Ok(())
    }

    fn crawl_url(self: Arc<Self>, url: String, depth: usize, is_seed: bool) {
        let self_clone = self.clone();
        tokio::spawn(async move {
            let tx = self_clone.crawl_tx.clone();
            let fetched_tx = self_clone.fetched_tx.clone();
            let result = self_clone.fetch_page(&url).await;
            match result {
                Ok(html) => {
                    self.visited_urls.insert(url.clone());
                    let res = self_clone.parse_html(&url, &html).await;
                    match res {
                        Ok((title, body, seen)) => {
                            // send fetched to be inserted to mongo.
                            let result = CrawlResult::new(
                                url.clone(),
                                title,
                                body,
                                seen.into_iter().collect(),
                                depth as u32,
                                is_seed,
                            );
                            fetched_tx.send(result.clone()).unwrap();

                            for link in &result.outgoing_links {
                                tx.send((link.clone(), depth + 1, false)).await.unwrap();
                            }
                        }
                        Err(e) => {
                            log::error!("error parsing html {url}, error: {:#}", e);
                        }
                    }
                }
                Err(e) => {
                    log::error!("error fetching page {url}, error: {:#}", e);
                }
            }
        });
    }

    async fn spawn_crawler(self: Arc<Self>, starting_url: String, depth: usize) -> Result<()> {
        let self_clone = self.clone();

        self.crawl_tx.send((starting_url, depth, true)).await.unwrap();
        tokio::spawn(async move {
            let mut rx = self_clone.crawl_rx.lock().await;

            while let Some((url, depth, is_seed)) = rx.recv().await {
                if depth > self_clone.max_depth {
                    continue;
                }
                if self_clone.visited_urls.contains(&url) {
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
            while let Some(result) = rx.recv().await {
                match self_clone.crawl_results_repo.insert(&result).await {
                    Ok(id) => {
                        log::info!("inserted to mongo db: {:?}", id)
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
        let body_selector = Selector::parse("body").unwrap();

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

        // extract title and body;
        let title = document
            .select(&title_selector)
            .next()
            .map(|t| t.text().collect::<String>().trim().to_string());
        let title = title.unwrap_or_else(|| "".to_string());
        let body = document
            .select(&body_selector)
            .next()
            .map(|t| t.text().collect::<String>().trim().to_string());
        let body = body.unwrap_or_else(|| "".to_string());

        Ok((title, body, seen))
    }
}
