use anyhow::Result;
use html5ever::parse_document;
use html5ever::tendril::TendrilSink;
use markup5ever_rcdom::{Handle, NodeData, RcDom};
use porter_stemmer::stem;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;

use crate::data_models::Page;
use crate::db::PageRepo;

#[allow(dead_code)]
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

    fn walk(handle: &Handle) -> String {
        let node = handle;
        let mut text = String::new();
        match &node.data {
            NodeData::Text { contents } => {
                let _text = contents.borrow();
                // Collapse whitespace if you like:
                let s = _text.trim();
                if !s.is_empty() {
                    text.push_str(s);
                }
            }
            NodeData::Element { name, .. } => {
                // For some block-level elements, you can print a newline
                let local = &name.local;
                if &**local == "p" || &**local == "div" || &**local == "br" {
                    text.push('\n');
                }
            }
            _ => {}
        }

        for child in node.children.borrow().iter() {
            text.push_str(&Self::walk(child));
            text.push('\n');
        }
        return text;
    }

    fn compress_whitespaces(text: &str) -> String {
        let mut result = Vec::new();
        let mut last_was_blank = false;
        for line in text.lines() {
            let trimmed = line.trim();
            if trimmed.trim().is_empty() {
                if !last_was_blank {
                    // result.push(String::new());
                    last_was_blank = true;
                }
            } else {
                result.push(trimmed.to_string());
                last_was_blank = true;
            }
        }
        result.join("\n")
    }

    fn process_and_tokenize(raw_html: &str) -> Result<Vec<String>> {
        // strips html tags
        // removes stop words
        // tokenizes
        let dom = parse_document(RcDom::default(), Default::default())
            .from_utf8()
            .read_from(&mut std::io::Cursor::new(raw_html.as_bytes()))
            .unwrap();

        let content = Self::walk(&dom.document);
        let content = Self::compress_whitespaces(&content);
        // let stop_words = get_stop_words();
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

                // if stop_words.contains(w) {
                //     return false;
                // }
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


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_whitespaces() {
        let input = "\n\n\n\n\n\n\n\n\n";
        let output = Scrapper::compress_whitespaces(input);
        assert_eq!(output, "");
    }

    #[test]
    fn test_compress_whitespaces2() {
        let input = "something\n\n\n\n else is going one\n\n\n\n\n\n\n\n\n\nsomething                  ";
        let output = Scrapper::compress_whitespaces(input);
        assert_eq!(output, "something\nelse is going one\nsomething");
    }

    #[test]
    fn test_something() {
        let mut a = Vec::new();
        a.push(String::new());
        assert_eq!("", String::new());
    }
}
