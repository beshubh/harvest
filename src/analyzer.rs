use anyhow::Result;
use html5ever::tendril::TendrilSink;
use html5ever::{Attribute, LocalName, parse_document};
use markup5ever_rcdom::{Handle, NodeData, RcDom};
use porter_stemmer::stem;
use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;

use crate::data_models::Page;
use crate::db::PageRepo;

static STOP_WORDS: OnceLock<HashSet<String>> = OnceLock::new();

#[allow(dead_code)]
static JS_WORDS: OnceLock<HashSet<&'static str>> = OnceLock::new();

fn get_stop_words() -> &'static HashSet<String> {
    STOP_WORDS.get_or_init(|| {
        stop_words::get(stop_words::LANGUAGE::English)
            .into_iter()
            .map(|x| x.to_string())
            .collect()
    })
}

#[allow(dead_code)]
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

#[derive(Clone, Default, Debug)]
pub struct ExtractedText {
    title: String,
    headings: Vec<String>,
    body: String,
    anchors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum Context {
    Title,
    Heading,
    Anchor,
    Body,
}

/// A character filter receives the original text as a stream of characters and can transform the stream by adding,
/// removing, or changing characters. For instance, a character filter could be used to convert Hindu-Arabic
/// numerals (٠‎١٢٣٤٥٦٧٨‎٩‎) into their Arabic-Latin equivalents (0123456789), or to strip HTML elements like <b> from the stream.
pub trait CharacterFilter: Send + Sync {
    fn filter(&self, text: String) -> String;
}

/// Make sure you are giving it a valid html text.
#[derive(Debug, Default)]
pub struct HTMLTagFilter;

impl HTMLTagFilter {
    pub fn get_dom(html: &str) -> RcDom {
        let dom = parse_document(RcDom::default(), Default::default())
            .from_utf8()
            .read_from(&mut std::io::Cursor::new(html))
            .unwrap();
        dom
    }

    pub fn has_boilerplate_class_or_id(attrs: &RefCell<Vec<Attribute>>) -> bool {
        use std::borrow::Cow;
        for attr in attrs.borrow().iter() {
            let value: Cow<str> = attr.value.to_string().into();
            let v = value.to_lowercase();
            if v.contains("nav")
                || v.contains("menu")
                || v.contains("sidebar")
                || v.contains("footer")
                || v.contains("header")
                || v.contains("cookie")
                || v.contains("banner")
                || v.contains("promo")
                || v.contains("ads")
                || v.contains("badge")
            {
                return true;
            }
        }

        false
    }

    pub fn is_block_like(local: &LocalName) -> bool {
        matches!(
            &**local,
            "p" | "div" | "section" | "article" | "li" | "ul" | "ol" | "header" | "footer"
        )
    }

    pub fn walk_html(handle: &Handle, ctx: Context, out: &mut ExtractedText) {
        let node = handle;
        let mut text = String::new();
        match &node.data {
            NodeData::Text { contents } => {
                let s = contents.borrow();
                // Collapse whitespace if you like:
                let s = s.trim();
                if !s.is_empty() {
                    text.push_str(s);
                }

                match ctx {
                    Context::Title => {
                        if !out.title.is_empty() {
                            out.title.push(' ');
                        }
                        out.title.push_str(s);
                    }
                    Context::Heading => {
                        if let Some(last) = out.headings.last_mut() {
                            if !last.is_empty() {
                                last.push(' ');
                            }
                            last.push_str(s);
                        } else {
                            // NEVER HAPPENS
                            out.headings.push(s.to_string());
                        }
                    }
                    Context::Anchor => {
                        if let Some(last) = out.anchors.last_mut() {
                            if !last.is_empty() {
                                last.push(' ');
                            }
                            last.push_str(s);
                        } else {
                            // NEVER HAPPENS
                            out.anchors.push(s.to_string());
                        }
                    }
                    Context::Body => {
                        if !out.body.is_empty() && !out.body.ends_with(' ') {
                            out.body.push(' ');
                        }
                        out.body.push_str(s);
                    }
                }
            }
            NodeData::Element { name, attrs, .. } => {
                let local = &name.local;

                if &**local == "script" || &**local == "style" || &**local == "noscript" {
                    return;
                }

                if Self::has_boilerplate_class_or_id(attrs) {
                    return;
                }

                #[allow(unused_assignments)]
                let mut new_ctx = ctx;

                if &**local == "title" {
                    new_ctx = Context::Title;
                } else if matches!(&**local, "h1" | "h2" | "h3" | "h4" | "h5" | "h6") {
                    new_ctx = Context::Heading;
                    // new heading entry
                    out.headings.push(String::new());
                } else if &**local == "a" {
                    new_ctx = Context::Anchor;
                    // new anchor entry
                    out.anchors.push(String::new());
                } else {
                    new_ctx = match ctx {
                        // after leaving <title>/<a>/<h1>, fall back to body
                        Context::Title | Context::Anchor | Context::Heading => Context::Body,
                        other => other,
                    };
                }

                if new_ctx == Context::Body && Self::is_block_like(local) {
                    if !out.body.ends_with('\n') {
                        out.body.push('\n');
                    }
                }

                for child in node.children.borrow().iter() {
                    Self::walk_html(child, new_ctx, out);
                }
            }
            _ => {
                for child in node.children.borrow().iter() {
                    Self::walk_html(child, ctx, out);
                }
            }
        }
    }

    #[allow(dead_code)]
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
}

impl CharacterFilter for HTMLTagFilter {
    fn filter(&self, html: String) -> String {
        let dom = Self::get_dom(&html);
        let mut out = ExtractedText::default();
        Self::walk_html(&dom.document, Context::Body, &mut out);
        out.body
    }
}

/// A tokenizer receives a stream of characters, breaks it up into individual tokens (usually individual words),
/// and outputs a stream of tokens.
/// For instance, a whitespace tokenizer breaks text into tokens whenever it sees any whitespace.
/// It would convert the text "Quick brown fox!" into the terms [Quick, brown, fox!].
pub trait Tokenizer: Send + Sync {
    fn tokenize(&self, text: String) -> Vec<String>;
}

pub struct WhiteSpaceTokenizer;

impl Tokenizer for WhiteSpaceTokenizer {
    fn tokenize(&self, text: String) -> Vec<String> {
        text.split_whitespace()
            .map(|w| w.to_string())
            .collect::<Vec<String>>()
    }
}

/// A token filter receives the token stream and may add, remove, or change tokens.
/// For example, a lowercase token filter converts all tokens to lowercase, a stop token
/// filter removes common words (stop words) like the from the token stream,
/// and a synonym token filter introduces synonyms into the token stream.
pub trait TokenFilter: Send + Sync {
    fn filter(&self, tokens: Vec<String>) -> Vec<String>;
}

pub struct LowerCaseTokenFilter;

impl TokenFilter for LowerCaseTokenFilter {
    fn filter(&self, tokens: Vec<String>) -> Vec<String> {
        tokens.iter().map(|w| w.to_lowercase()).collect::<Vec<String>>()
    }
}

pub struct StopWordTokenFilter;

impl TokenFilter for StopWordTokenFilter {
    fn filter(&self, mut tokens: Vec<String>) -> Vec<String> {
        let stop_words = get_stop_words();
        tokens.retain(|w| !stop_words.contains(w));
        tokens
    }
}

pub struct PorterStemmerTokenFilter;

impl TokenFilter for PorterStemmerTokenFilter {
    fn filter(&self, tokens: Vec<String>) -> Vec<String> {
        tokens.iter().map(|w| stem(w)).collect::<Vec<String>>()
    }
}

pub struct Analyzer {
    char_filters: Vec<Box<dyn CharacterFilter>>,
    tokenizer: Box<dyn Tokenizer>,
    token_filters: Vec<Box<dyn TokenFilter>>,

    pub analyze_tx: mpsc::UnboundedSender<Page>,
    analyze_rx: Mutex<mpsc::UnboundedReceiver<Page>>,
    concurrent_analysis: Arc<Semaphore>,
    pages_repo: Arc<PageRepo>,
}

impl Analyzer {
    pub fn new(
        char_filters: Vec<Box<dyn CharacterFilter>>,
        tokenizer: Box<dyn Tokenizer>,
        token_filters: Vec<Box<dyn TokenFilter>>,
        max_concurrent_analysiss: usize,
        pages_repo: Arc<PageRepo>,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            char_filters,
            tokenizer,
            token_filters,
            analyze_tx: tx,
            analyze_rx: Mutex::new(rx),
            concurrent_analysis: Arc::new(Semaphore::new(max_concurrent_analysiss)),
            pages_repo,
        }
    }

    pub fn spin(self: Arc<Self>) -> Result<()> {
        let self_clone = self.clone();
        tokio::spawn(async move {
            let mut analyze_rx = self_clone.analyze_rx.lock().await;
            let semaphore = self_clone.concurrent_analysis.clone();
            while let Some(data) = analyze_rx.recv().await {
                let permit = semaphore.acquire().await.unwrap();
                let self_clone = self_clone.clone();
                tokio::spawn(async move {
                    let page = self_clone.clone().process(data);
                    if let Ok(page) = page {
                        if let Err(e) = self_clone.pages_repo.upsert(&page).await {
                            eprintln!("Error upserting page after scrapping: {}", e);
                        }
                    }
                });
                drop(permit);
            }
        });
        Ok(())
    }

    fn process(self: Arc<Self>, mut page: Page) -> Result<Page> {
        //character filtering
        let mut content = page.html_body.clone();
        for filter in self.char_filters.iter() {
            content = filter.filter(content);
        }

        // tokenization
        let mut tokens = self.tokenizer.tokenize(content);

        // token filtering
        for filter in self.token_filters.iter() {
            tokens = filter.filter(tokens);
        }

        let content = tokens.join(" ");
        page.cleaned_content = content;
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_whitespaces() {
        let input = "\n\n\n\n\n\n\n\n\n";
        let output = HTMLTagFilter::compress_whitespaces(input);
        assert_eq!(output, "");
    }

    #[test]
    fn test_compress_whitespaces2() {
        let input =
            "something\n\n\n\n else is going one\n\n\n\n\n\n\n\n\n\nsomething                  ";
        let output = HTMLTagFilter::compress_whitespaces(input);
        assert_eq!(output, "something\nelse is going one\nsomething");
    }

    #[test]
    fn test_something() {
        let mut a = Vec::new();
        a.push(String::new());
        assert_eq!("", String::new());
    }

    #[test]
    fn test_html_tag_filter() {
        let html = "<html><head><title>Hello World</title></head><body><h1>Hello World</h1><p>This is a test</p></body></html>";
        let filter = HTMLTagFilter;
        let filtered = filter.filter(html.into());
        assert_eq!("\n This is a test", &filtered);
    }

    #[test]
    fn test_html_tag_filter_extracted_content() {
        let html = r#"<html><head><title>New World Order</title></head><body><h1>Hello World</h1><p>This is a test</p>
            <h1>New Heading</h1>
            <p>Some other content</p>
            <script>alert('Hello World')</script>
            <a href="https://www.google.com">Link to Google</a>
            </body></html>"#;
        let dom = HTMLTagFilter::get_dom(html);
        let mut extracted = ExtractedText::default();
        HTMLTagFilter::walk_html(&dom.document, Context::Body, &mut extracted);
        assert_eq!("New World Order", &extracted.title);
        assert_eq!("Hello World", &extracted.headings[0]);
        assert_eq!("New Heading", &extracted.headings[1]);
        assert_eq!("Link to Google", &extracted.anchors[0]);
        assert_eq!("\n This is a test \n Some other content ", &extracted.body);
    }

}
