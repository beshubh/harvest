use anyhow::Result;
use html2text::from_read;
use porter_stemmer::stem;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;

use crate::data_models::Page;
use crate::db::PageRepo;

// 1. Define Static Sets so we don't rebuild them for every single page
static STOP_WORDS: OnceLock<HashSet<String>> = OnceLock::new();
static JS_WORDS: OnceLock<HashSet<&'static str>> = OnceLock::new();

fn get_stop_words() -> &'static HashSet<String> {
    STOP_WORDS.get_or_init(|| {
        stop_words::get(stop_words::LANGUAGE::English)
            .into_iter()
            .map(|x| x.to_string())
            .collect()
    })
}

fn get_js_words() -> &'static HashSet<&'static str> {
    JS_WORDS.get_or_init(|| {
        HashSet::from([
            // Keywords
            "var",
            "let",
            "const",
            "function",
            "return",
            "class",
            "if",
            "else",
            "for",
            "while",
            "this",
            "new",
            "try",
            "catch",
            "async",
            "await",
            "import",
            "export",
            "default",
            "null",
            "undefined",
            "true",
            "false",
            // DOM / Browser Objects (The ones you were seeing)
            "window",
            "document",
            "console",
            "navigator",
            "history",
            "location",
            "screen",
            "localstorage",
            "sessionstorage",
            "alert",
            "prompt",
            "fetch",
            "jquery",
            "ajax",
            "json",
            "body",
            "html",
            "div",
            "span",
            "href",
            "src",
            "onclick",
            "onload",
        ])
    })
}

pub struct Scrapper {
    pub scrap_tx: mpsc::UnboundedSender<Page>,
    scrap_rx: Mutex<mpsc::UnboundedReceiver<Page>>,
    concurrent_scraps: Arc<Semaphore>,
    pages_repo: Arc<PageRepo>,
}

impl Scrapper {
    pub fn new(max_concurrent_scraps: usize, pages_repo: Arc<PageRepo>) -> Scrapper {
        let (tx, rx) = mpsc::unbounded_channel();
        Scrapper {
            scrap_tx: tx,
            scrap_rx: Mutex::new(rx),
            concurrent_scraps: Arc::new(Semaphore::new(max_concurrent_scraps)),
            pages_repo,
        }
    }

    pub async fn spin(self: Arc<Self>) -> Result<String> {
        let self_clone = self.clone();
        tokio::spawn(async move {
            let mut scrap_rx = self_clone.scrap_rx.lock().await;
            let semaphore = self_clone.concurrent_scraps.clone();
            while let Some(data) = scrap_rx.recv().await {
                let permit = semaphore.acquire().await.unwrap();
                let self_clone = self_clone.clone();
                tokio::spawn(async move {
                    let page = Self::process_page(data);
                    if let Ok(page) = page {
                        if let Err(e) = self_clone.pages_repo.upsert(&page).await {
                            eprintln!("Error upserting page after scrapping: {}", e);
                        }
                    }
                });
                drop(permit);
            }
        });
        Ok("".to_string())
    }

    fn process_page(mut page: Page) -> Result<Page> {
        let content = Self::process_and_tokenize(&page.html_body)?;
        let content = Self::stem_words(content);
        page.cleaned_content = content;
        Ok(page)
    }

    fn process_and_tokenize(raw_html: &str) -> Result<Vec<String>> {
        // strips html tags
        // removes stop words
        // tokenizes
        let content = raw_html;
        let content = from_read(content.as_bytes(), 80)?;
        let stop_words = get_stop_words();
        let javascript_words = get_js_words();

        let tokens = content
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .map(|w| w.to_lowercase())
            .filter(|w| {
                if w.len() < 2 {
                    return false;
                }
                // filter out numbers
                if w.chars().all(char::is_numeric) {
                    return false;
                }

                if stop_words.contains(w) {
                    return false;
                }
                if javascript_words.contains(w.as_str()) {
                    return false;
                }
                true
            })
            .collect::<Vec<String>>();

        Ok(tokens)
    }

    fn stem_words(content: Vec<String>) -> String {
        let stemmed_words = content.iter().map(|w| stem(w)).collect::<Vec<String>>();
        stemmed_words.join(" ")
    }
}
