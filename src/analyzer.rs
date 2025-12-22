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
    fn filter(&self, tokens: Vec<TextToken>) -> Vec<TextToken>;
}

pub struct LowerCaseTokenFilter;

impl TokenFilter for LowerCaseTokenFilter {
    fn filter(&self, tokens: Vec<TextToken>) -> Vec<TextToken> {
        tokens
            .into_iter()
            .map(|mut t| {
                t.term = t.term.to_lowercase();
                t
            })
            .collect()
    }
}

pub struct StopWordTokenFilter;

impl TokenFilter for StopWordTokenFilter {
    fn filter(&self, mut tokens: Vec<TextToken>) -> Vec<TextToken> {
        let stop_words = get_stop_words();
        tokens.retain(|w| !stop_words.contains(&w.term));
        tokens
    }
}

pub struct PorterStemmerTokenFilter;

impl TokenFilter for PorterStemmerTokenFilter {
    fn filter(&self, tokens: Vec<TextToken>) -> Vec<TextToken> {
        tokens
            .into_iter()
            .map(|mut w| {
                w.term = stem(&w.term);
                w
            })
            .collect::<Vec<TextToken>>()
    }
}

/// Strips punctuation from tokens and filters out tokens that become empty or are too short
pub struct PunctuationStripFilter {
    min_length: usize,
}

impl PunctuationStripFilter {
    pub fn new(min_length: usize) -> Self {
        Self { min_length }
    }
}

impl Default for PunctuationStripFilter {
    fn default() -> Self {
        Self { min_length: 2 }
    }
}

impl TokenFilter for PunctuationStripFilter {
    fn filter(&self, tokens: Vec<TextToken>) -> Vec<TextToken> {
        tokens
            .into_iter()
            .filter_map(|mut token| {
                // Strip leading and trailing punctuation
                let trimmed: String = token
                    .term
                    .trim_matches(|c: char| !c.is_alphanumeric())
                    .to_string();

                // Filter out tokens that are:
                // 1. Empty after trimming
                // 2. Too short (less than min_length)
                // 3. Only contain non-alphanumeric characters
                if trimmed.len() >= self.min_length && trimmed.chars().any(|c| c.is_alphanumeric())
                {
                    token.term = trimmed;
                    Some(token)
                } else {
                    None
                }
            })
            .collect()
    }
}

/// Filters out tokens that are purely numeric (like "123", "45.67", etc.)
pub struct NumericTokenFilter;

impl TokenFilter for NumericTokenFilter {
    fn filter(&self, tokens: Vec<TextToken>) -> Vec<TextToken> {
        tokens
            .into_iter()
            .filter(|token| {
                // Keep tokens that have at least one alphabetic character
                token.term.chars().any(|c| c.is_alphabetic())
            })
            .collect()
    }
}

/// Pure text analysis pipeline - no async, no DB, just text transformations
pub struct TextAnalyzer {
    char_filters: Vec<Box<dyn CharacterFilter>>,
    tokenizer: Box<dyn Tokenizer>,
    token_filters: Vec<Box<dyn TokenFilter>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextToken {
    pub term: String,
    pub pos: usize,
}

impl std::ops::Deref for TextToken {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.term
    }
}

impl Into<mongodb::bson::Bson> for TextToken {
    fn into(self) -> mongodb::bson::Bson {
        mongodb::bson::Bson::String(self.term)
    }
}

impl TextAnalyzer {
    pub fn new(
        char_filters: Vec<Box<dyn CharacterFilter>>,
        tokenizer: Box<dyn Tokenizer>,
        token_filters: Vec<Box<dyn TokenFilter>>,
    ) -> Self {
        Self {
            char_filters,
            tokenizer,
            token_filters,
        }
    }

    pub fn char_filter(&self, mut content: String) -> String {
        for filter in self.char_filters.iter() {
            content = filter.filter(content);
        }
        content
    }

    pub fn tokenize(&self, content: String) -> Vec<TextToken> {
        let tokens = self.tokenizer.tokenize(content);
        tokens
            .iter()
            .enumerate()
            .map(|(idx, tok)| TextToken {
                term: tok.clone(),
                pos: idx,
            })
            .collect()
    }

    pub fn token_filter(&self, mut tokens: Vec<TextToken>) -> Vec<TextToken> {
        for filter in self.token_filters.iter() {
            tokens = filter.filter(tokens);
        }
        tokens
    }

    /// Analyzes raw content and returns a list of tokens
    pub fn analyze(&self, raw_content: String) -> Result<Vec<TextToken>> {
        let content = self.char_filter(raw_content);

        let mut tokens = self.tokenize(content);

        tokens = self.token_filter(tokens);
        Ok(tokens)
    }
}

/// Handles async page processing queue and database persistence
pub struct PageProcessor {
    pub process_tx: mpsc::UnboundedSender<Page>,
    process_rx: Mutex<mpsc::UnboundedReceiver<Page>>,
    concurrent_processing: Arc<Semaphore>,
    pages_repo: Arc<PageRepo>,
    text_analyzer: TextAnalyzer,
}

impl PageProcessor {
    pub fn new(
        text_analyzer: TextAnalyzer,
        max_concurrent_processing: usize,
        pages_repo: Arc<PageRepo>,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            text_analyzer,
            process_tx: tx,
            process_rx: Mutex::new(rx),
            concurrent_processing: Arc::new(Semaphore::new(max_concurrent_processing)),
            pages_repo,
        }
    }

    /// Spins up the async processing loop
    pub fn spin(self: Arc<Self>) -> Result<()> {
        let self_clone = self.clone();
        tokio::spawn(async move {
            let mut process_rx = self_clone.process_rx.lock().await;
            while let Some(page) = process_rx.recv().await {
                let self_for_task = self_clone.clone();
                tokio::spawn(async move {
                    let permit = self_for_task.concurrent_processing.acquire().await.unwrap();
                    if let Ok(processed_page) = self_for_task.process_page(page) {
                        if let Err(e) = self_for_task.pages_repo.upsert(&processed_page).await {
                            eprintln!("Error upserting page after processing: {}", e);
                        }
                    }
                    drop(permit);
                });
            }
        });
        Ok(())
    }

    /// Processes a single page by analyzing its content
    fn process_page(&self, mut page: Page) -> Result<Page> {
        let tokens = self.text_analyzer.analyze(page.html_body.clone())?;
        page.cleaned_content = tokens
            .into_iter()
            .map(|t| t.term)
            .collect::<Vec<String>>()
            .join(" ");
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_tokens(terms: &[&str]) -> Vec<TextToken> {
        terms
            .iter()
            .enumerate()
            .map(|(pos, term)| TextToken {
                term: (*term).to_string(),
                pos,
            })
            .collect()
    }

    fn terms(tokens: Vec<TextToken>) -> Vec<String> {
        tokens.into_iter().map(|t| t.term).collect()
    }

    fn assert_contains(tokens: &[TextToken], term: &str) {
        assert!(
            tokens.iter().any(|t| t.term == term),
            "expected token stream to contain term {:?}, but got {:?}",
            term,
            tokens.iter().map(|t| t.term.as_str()).collect::<Vec<_>>()
        );
    }

    fn assert_not_contains(tokens: &[TextToken], term: &str) {
        assert!(
            !tokens.iter().any(|t| t.term == term),
            "expected token stream to NOT contain term {:?}, but got {:?}",
            term,
            tokens.iter().map(|t| t.term.as_str()).collect::<Vec<_>>()
        );
    }

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

    #[test]
    fn test_punctuation_strip_filter() {
        let filter = PunctuationStripFilter::default();
        let tokens = mk_tokens(&[
            "!.",
            "!=",
            "!==",
            "=======",
            "![](banner.jpg)",
            "!important",
            "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"",
            "\"(...)le\"",
            "hello",
            "world!",
            "test123",
            "...dots...",
            "a",  // too short
            "ab", // min length is 2, this should pass
        ]);
        let result = terms(filter.filter(tokens));
        assert_eq!(
            result,
            vec![
                "banner.jpg".to_string(), // extracted from ![](banner.jpg)
                "important".to_string(),
                "le".to_string(),
                "hello".to_string(),
                "world".to_string(),
                "test123".to_string(),
                "dots".to_string(),
                "ab".to_string(),
            ]
        );
    }

    #[test]
    fn test_numeric_token_filter() {
        let filter = NumericTokenFilter;
        let tokens = mk_tokens(&["123", "45.67", "test123", "hello", "2024", "abc123def"]);
        let result = terms(filter.filter(tokens));
        assert_eq!(
            result,
            vec![
                "test123".to_string(),
                "hello".to_string(),
                "abc123def".to_string(),
            ]
        );
    }

    #[test]
    fn test_full_analyzer_pipeline() {
        // Simulate the full pipeline
        let html = r#"<html><body>
            <p>The !important CSS rule is used with ======= separators.</p>
            <p>Check out ![](banner.jpg) for more info!!!</p>
            <p>Contact us at 123-456-7890 or visit https://example.com</p>
        </body></html>"#;

        // Character filter
        let html_filter = HTMLTagFilter;
        let text = html_filter.filter(html.to_string());

        // Tokenizer
        let tokenizer = WhiteSpaceTokenizer;
        let tokens = tokenizer.tokenize(text);
        let tokens = tokens
            .into_iter()
            .enumerate()
            .map(|(pos, term)| TextToken { term, pos })
            .collect::<Vec<TextToken>>();

        // Token filters
        let punct_filter = PunctuationStripFilter::default();
        let tokens = punct_filter.filter(tokens);

        let lower_filter = LowerCaseTokenFilter;
        let tokens = lower_filter.filter(tokens);

        let numeric_filter = NumericTokenFilter;
        let tokens = numeric_filter.filter(tokens);

        let stop_filter = StopWordTokenFilter;
        let tokens = stop_filter.filter(tokens);

        let stem_filter = PorterStemmerTokenFilter;
        let tokens = stem_filter.filter(tokens);

        // Should have clean tokens now, without punctuation noise
        // and without stop words like "the", "is", "with", "at", "or", "for", "used"
        assert_contains(&tokens, "css");
        assert_contains(&tokens, "rule");
        assert_contains(&tokens, "separ"); // stemmed from "separators"
        assert_contains(&tokens, "check");
        assert_contains(&tokens, "info");

        // These should NOT be in the tokens
        assert_not_contains(&tokens, "!important");
        assert_not_contains(&tokens, "=======");
        assert_not_contains(&tokens, "![](banner.jpg)");
        assert_not_contains(&tokens, "the"); // stop word
        assert_not_contains(&tokens, "123"); // numeric only
        assert_not_contains(&tokens, "456"); // numeric only
        assert_not_contains(&tokens, "7890"); // numeric only

        // Make sure we're cleaning up the index properly
        // by removing pure punctuation terms
        assert_not_contains(&tokens, "!.");
        assert_not_contains(&tokens, "!=");
        assert_not_contains(&tokens, "!==");
    }
}
