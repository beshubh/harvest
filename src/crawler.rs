use std::collections::HashSet;

use anyhow::Result;
use dashmap::DashSet;
use reqwest::Url;
use scraper::{Html, Selector};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;

use crate::data_models::Page;
use crate::db::PageRepo;

const MAX_FETCH_RETRIES: usize = 4;
const MAX_DOCUMENT_SIZE_BYTES: usize = 15 * 1024 * 1024; // 15 MB (leaving margin for MongoDB's 16MB limit)

// List of file extensions that indicate non-HTML files (images, audio, pdf, documents, archives, etc)
const NON_HTML_EXTENSIONS: [&str; 27] = [
    // Images
    ".png", ".jpg", ".jpeg", ".gif", ".svg", ".bmp", ".webp", ".tiff", ".mov", // Audio/Video
    ".mp3", ".mp4", ".wav", ".ogg", // Documents
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", // Archives
    ".zip", ".rar", ".tar", ".gz", ".m", ".bib", ".txt",
];

/// Error type for fetch_page failures.
/// Used to differentiate between "not HTML" content and "other" errors.
#[derive(Debug, thiserror::Error)]
pub enum FetchPageError {
    #[error("Response content is not HTML (content-type: {0})")]
    NotHtml(String),

    #[error("Non-HTML file extension: {0}")]
    NonHtmlExtension(String),

    #[error("Other fetch error: {0}")]
    Other(#[from] anyhow::Error),
}

impl From<reqwest::Error> for FetchPageError {
    fn from(value: reqwest::Error) -> Self {
        FetchPageError::Other(value.into())
    }
}

pub struct Crawler {
    visited_urls: DashSet<String>,
    max_depth: usize,
    pages_repo: Arc<PageRepo>,
    crawl_tx: mpsc::Sender<(String, usize, bool)>,
    crawl_rx: Mutex<mpsc::Receiver<(String, usize, bool)>>,
    fetched_tx: mpsc::UnboundedSender<Page>,
    fetched_rx: Mutex<mpsc::UnboundedReceiver<Page>>,
    concurrency_semaphore: Arc<Semaphore>,
}

impl Crawler {
    pub fn new(
        max_depth: usize,
        pages_repo: PageRepo,
        max_concurrent_fetches: usize,
        frontier_size: usize,
    ) -> Crawler {
        let (crawl_tx, crawl_rx) = mpsc::channel(frontier_size);
        let (fetched_tx, fetched_rx) = mpsc::unbounded_channel();

        let pages_repo = Arc::new(pages_repo);

        // Create text analyzer

        Crawler {
            visited_urls: DashSet::new(),
            max_depth,
            pages_repo: pages_repo.clone(),
            crawl_tx: crawl_tx.clone(),
            crawl_rx: Mutex::new(crawl_rx),
            fetched_tx: fetched_tx.clone(),
            fetched_rx: Mutex::new(fetched_rx),
            concurrency_semaphore: Arc::new(Semaphore::new(max_concurrent_fetches)),
        }
    }

    pub async fn crawl(self: Arc<Self>, starting_url: String) -> Result<()> {
        self.clone().spawn_crawler(starting_url, 0).await.unwrap();
        self.clone().spawn_mongo_inserter().await;
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
                    match e {
                        FetchPageError::NonHtmlExtension(_) | FetchPageError::NotHtml(_) => {
                            break;
                        }
                        FetchPageError::Other(msg) => {
                            log::error!("error fetching page {url}, error: {:#}", msg);
                            retried += 1;
                        }
                    }
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

            let estimated_size = html.len() + url.len();
            if estimated_size > MAX_DOCUMENT_SIZE_BYTES {
                log::warn!(
                    "skipping url {} - document too large ({} bytes)",
                    url,
                    estimated_size
                );
                return;
            }

            let res = self_clone.parse_html(&url, &html).await;
            match res {
                Ok((title, body, seen)) => {
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
                    Ok(id) => {
                        log::info!("inserted to mongo: {}", id);
                    }
                    Err(e) => {
                        log::error!("error inserting to mongo, error: {:#}", e);
                    }
                }
            }
        });
    }

    async fn fetch_page(&self, url: &str) -> Result<String, FetchPageError> {
        let client = reqwest::Client::new();
        if NON_HTML_EXTENSIONS.iter().any(|ext| url.ends_with(ext)) {
            return Err(FetchPageError::NonHtmlExtension(
                "URL ext does not ends with html".into(),
            ));
        }

        let res = client.get(url).send().await?;
        if let Some(content_type) = res.headers().get(reqwest::header::CONTENT_TYPE) {
            let ct = content_type.to_str().unwrap_or("").to_lowercase();
            if !ct.contains("html") {
                return Err(FetchPageError::NotHtml(format!(
                    "Content-Type is not HTML: {} for url: {}",
                    ct, url
                )));
            }
        }

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
                        let resolved_str = resolved.to_string();
                        let lower_resolved = resolved_str.to_lowercase();
                        // Skip adding if URL ends with a known non-HTML file extension
                        if NON_HTML_EXTENSIONS
                            .iter()
                            .any(|ext| lower_resolved.ends_with(ext))
                        {
                            continue;
                        }
                        if !self.visited_urls.contains(&resolved_str) {
                            seen.insert(resolved_str);
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
