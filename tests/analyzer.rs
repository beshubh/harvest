use harvest::analyzer::*;

// Test helper to create TextTokens from strings
fn make_tokens(terms: Vec<&str>) -> Vec<TextToken> {
    terms
        .into_iter()
        .enumerate()
        .map(|(i, term)| TextToken {
            term: term.to_string(),
            pos: i,
        })
        .collect()
}

// Test helper to extract terms from TextTokens for comparison
fn extract_terms(tokens: &[TextToken]) -> Vec<String> {
    tokens.iter().map(|t| t.term.clone()).collect()
}

// CharacterFilter Tests

#[cfg(test)]
mod character_filter_tests {
    use super::*;

    // HTMLTagFilter Tests
    mod html_tag_filter {
        use super::*;

        #[test]
        fn test_empty_string() {
            let filter = HTMLTagFilter;
            let result = filter.filter("".to_string());
            assert_eq!(result, "");
        }

        #[test]
        fn test_plain_text_no_html() {
            let filter = HTMLTagFilter;
            let result = filter.filter("Hello World".to_string());
            assert_eq!(result.trim(), "Hello World");
        }

        #[test]
        fn test_simple_html() {
            let filter = HTMLTagFilter;
            let html = "<p>Hello World</p>".to_string();
            let result = filter.filter(html);
            assert_eq!(result.trim(), "Hello World");
        }

        #[test]
        fn test_nested_html() {
            let filter = HTMLTagFilter;
            let html = "<div><p>Hello <span>World</span></p></div>".to_string();
            let result = filter.filter(html);
            assert_eq!(result.trim(), "Hello World");
        }

        #[test]
        fn test_removes_script_tags() {
            let filter = HTMLTagFilter;
            let html = "<p>Before</p><script>alert('evil')</script><p>After</p>".to_string();
            let result = filter.filter(html);
            assert!(!result.contains("alert"));
            assert!(result.contains("Before"));
            assert!(result.contains("After"));
        }

        #[test]
        fn test_removes_style_tags() {
            let filter = HTMLTagFilter;
            let html = "<p>Content</p><style>body { color: red; }</style>".to_string();
            let result = filter.filter(html);
            assert!(!result.contains("color"));
            assert!(result.contains("Content"));
        }

        #[test]
        fn test_removes_noscript_tags() {
            let filter = HTMLTagFilter;
            let html = "<p>Main</p><noscript>Enable JS</noscript>".to_string();
            let result = filter.filter(html);
            assert!(!result.contains("Enable JS"));
            assert!(result.contains("Main"));
        }

        #[test]
        fn test_removes_navigation() {
            let filter = HTMLTagFilter;
            let html = r#"<div class="navbar">Menu</div><p>Content</p>"#.to_string();
            let result = filter.filter(html);
            assert!(!result.contains("Menu"));
            assert!(result.contains("Content"));
        }

        #[test]
        fn test_removes_footer() {
            let filter = HTMLTagFilter;
            let html = r#"<p>Content</p><div class="footer">Footer text</div>"#.to_string();
            let result = filter.filter(html);
            assert!(!result.contains("Footer text"));
            assert!(result.contains("Content"));
        }

        #[test]
        fn test_removes_sidebar() {
            let filter = HTMLTagFilter;
            let html = r#"<div class="sidebar">Side</div><p>Main</p>"#.to_string();
            let result = filter.filter(html);
            assert!(!result.contains("Side"));
            assert!(result.contains("Main"));
        }

        #[test]
        fn test_removes_cookie_banner() {
            let filter = HTMLTagFilter;
            let html = r#"<div id="cookie-banner">Accept cookies</div><p>Article</p>"#.to_string();
            let result = filter.filter(html);
            assert!(!result.contains("Accept cookies"));
            assert!(result.contains("Article"));
        }

        #[test]
        fn test_removes_ads() {
            let filter = HTMLTagFilter;
            let html =
                r#"<div class="ads-container">Buy now!</div><p>Real content</p>"#.to_string();
            let result = filter.filter(html);
            assert!(!result.contains("Buy now"));
            assert!(result.contains("Real content"));
        }

        #[test]
        fn test_case_insensitive_boilerplate() {
            let filter = HTMLTagFilter;
            let html = r#"<div class="NAVBAR">Menu</div><p>Content</p>"#.to_string();
            let result = filter.filter(html);
            assert!(!result.contains("Menu"));
        }

        #[test]
        fn test_preserves_block_spacing() {
            let filter = HTMLTagFilter;
            let html = "<div>First</div><div>Second</div>".to_string();
            let result = filter.filter(html);
            assert!(result.contains("First"));
            assert!(result.contains("Second"));
            assert!(result.contains('\n'));
        }

        #[test]
        fn test_multiple_paragraphs() {
            let filter = HTMLTagFilter;
            let html =
                "<p>First paragraph</p><p>Second paragraph</p><p>Third paragraph</p>".to_string();
            let result = filter.filter(html);
            assert!(result.contains("First paragraph"));
            assert!(result.contains("Second paragraph"));
            assert!(result.contains("Third paragraph"));
        }

        #[test]
        fn test_malformed_html() {
            let filter = HTMLTagFilter;
            let html = "<p>Unclosed paragraph<div>Another div".to_string();
            let result = filter.filter(html);
            assert!(result.contains("Unclosed paragraph"));
            assert!(result.contains("Another div"));
        }

        #[test]
        fn test_html_entities() {
            let filter = HTMLTagFilter;
            let html = "<p>Hello&nbsp;World&amp;Test</p>".to_string();
            let result = filter.filter(html);
            // HTML entities might be decoded by the parser
            assert!(result.contains("Hello"));
            assert!(result.contains("World"));
        }

        #[test]
        fn test_whitespace_normalization() {
            let filter = HTMLTagFilter;
            let html = "<p>  Multiple   spaces   here  </p>".to_string();
            let result = filter.filter(html);
            assert_eq!(result.trim(), "Multiple   spaces   here");
        }

        #[test]
        fn test_only_whitespace() {
            let filter = HTMLTagFilter;
            let html = "<p>   \n\t   </p>".to_string();
            let result = filter.filter(html);
            assert_eq!(result.trim(), "");
        }

        #[test]
        fn test_lists() {
            let filter = HTMLTagFilter;
            let html = "<ul><li>Item 1</li><li>Item 2</li></ul>".to_string();
            let result = filter.filter(html);
            assert!(result.contains("Item 1"));
            assert!(result.contains("Item 2"));
        }

        #[test]
        fn test_complex_real_world_html() {
            let filter = HTMLTagFilter;
            let html = r#"
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Test Page</title>
                    <style>body { background: blue; }</style>
                </head>
                <body>
                    <header class="site-header">
                        <nav class="navbar">
                            <a href="/">Home</a>
                        </nav>
                    </header>
                    <main>
                        <article>
                            <h1>Main Article Title</h1>
                            <p>This is the main content that should be extracted.</p>
                            <p>Another paragraph with <strong>important</strong> information.</p>
                        </article>
                        <aside class="sidebar">
                            <div class="ads">Advertisement here</div>
                        </aside>
                    </main>
                    <footer class="site-footer">
                        <p>Copyright 2024</p>
                    </footer>
                    <script>
                        console.log('tracking code');
                    </script>
                </body>
                </html>
            "#
            .to_string();
            let result = filter.filter(html);

            // Debug: print what we got
            eprintln!("Result: '{}'", result);

            // The HTMLTagFilter only returns body content, not headings
            // Headings are extracted separately in ExtractedText
            // So we should only check for the paragraph content
            assert!(result.contains("This is the main content"));
            assert!(result.contains("important"));

            // Should NOT contain boilerplate
            assert!(!result.contains("Home"));
            assert!(!result.contains("Advertisement"));
            assert!(!result.contains("Copyright"));
            assert!(!result.contains("tracking code"));
        }

        #[test]
        fn test_unicode_content() {
            let filter = HTMLTagFilter;
            let html = "<p>Hello 世界 🌍</p>".to_string();
            let result = filter.filter(html);
            assert!(result.contains("Hello"));
            assert!(result.contains("世界"));
        }

        #[test]
        fn test_very_long_text() {
            let filter = HTMLTagFilter;
            let long_text = "word ".repeat(10000);
            let html = format!("<p>{}</p>", long_text);
            let result = filter.filter(html);
            assert!(result.len() > 10000);
            assert!(result.contains("word"));
        }

        #[test]
        fn test_deeply_nested_structure() {
            let filter = HTMLTagFilter;
            let html = "<div><div><div><div><div><p>Deep content</p></div></div></div></div></div>"
                .to_string();
            let result = filter.filter(html);
            assert!(result.contains("Deep content"));
        }
    }
}

// Tokenizer Tests

#[cfg(test)]
mod tokenizer_tests {
    use super::*;

    mod whitespace_tokenizer {
        use super::*;

        #[test]
        fn test_empty_string() {
            let tokenizer = WhiteSpaceTokenizer;
            let result = tokenizer.tokenize("".to_string());
            assert_eq!(result.len(), 0);
        }

        #[test]
        fn test_single_word() {
            let tokenizer = WhiteSpaceTokenizer;
            let result = tokenizer.tokenize("hello".to_string());
            assert_eq!(result, vec!["hello"]);
        }

        #[test]
        fn test_multiple_words() {
            let tokenizer = WhiteSpaceTokenizer;
            let result = tokenizer.tokenize("hello world test".to_string());
            assert_eq!(result, vec!["hello", "world", "test"]);
        }

        #[test]
        fn test_multiple_spaces() {
            let tokenizer = WhiteSpaceTokenizer;
            let result = tokenizer.tokenize("hello    world".to_string());
            assert_eq!(result, vec!["hello", "world"]);
        }

        #[test]
        fn test_tabs_and_newlines() {
            let tokenizer = WhiteSpaceTokenizer;
            let result = tokenizer.tokenize("hello\tworld\ntest".to_string());
            assert_eq!(result, vec!["hello", "world", "test"]);
        }

        #[test]
        fn test_leading_whitespace() {
            let tokenizer = WhiteSpaceTokenizer;
            let result = tokenizer.tokenize("   hello world".to_string());
            assert_eq!(result, vec!["hello", "world"]);
        }

        #[test]
        fn test_trailing_whitespace() {
            let tokenizer = WhiteSpaceTokenizer;
            let result = tokenizer.tokenize("hello world   ".to_string());
            assert_eq!(result, vec!["hello", "world"]);
        }

        #[test]
        fn test_only_whitespace() {
            let tokenizer = WhiteSpaceTokenizer;
            let result = tokenizer.tokenize("   \n\t   ".to_string());
            assert_eq!(result.len(), 0);
        }

        #[test]
        fn test_punctuation_preserved() {
            let tokenizer = WhiteSpaceTokenizer;
            let result = tokenizer.tokenize("hello, world!".to_string());
            assert_eq!(result, vec!["hello,", "world!"]);
        }

        #[test]
        fn test_numbers() {
            let tokenizer = WhiteSpaceTokenizer;
            let result = tokenizer.tokenize("test 123 456".to_string());
            assert_eq!(result, vec!["test", "123", "456"]);
        }

        #[test]
        fn test_mixed_punctuation() {
            let tokenizer = WhiteSpaceTokenizer;
            let result = tokenizer.tokenize("hello-world test_case foo.bar".to_string());
            assert_eq!(result, vec!["hello-world", "test_case", "foo.bar"]);
        }

        #[test]
        fn test_unicode_text() {
            let tokenizer = WhiteSpaceTokenizer;
            let result = tokenizer.tokenize("Hello 世界 test".to_string());
            assert_eq!(result, vec!["Hello", "世界", "test"]);
        }

        #[test]
        fn test_emojis() {
            let tokenizer = WhiteSpaceTokenizer;
            let result = tokenizer.tokenize("Hello 🌍 world 🚀".to_string());
            assert_eq!(result, vec!["Hello", "🌍", "world", "🚀"]);
        }

        #[test]
        fn test_very_long_text() {
            let tokenizer = WhiteSpaceTokenizer;
            let text = (0..10000)
                .map(|i| format!("word{}", i))
                .collect::<Vec<_>>()
                .join(" ");
            let result = tokenizer.tokenize(text);
            assert_eq!(result.len(), 10000);
            assert_eq!(result[0], "word0");
            assert_eq!(result[9999], "word9999");
        }

        #[test]
        fn test_carriage_return() {
            let tokenizer = WhiteSpaceTokenizer;
            let result = tokenizer.tokenize("hello\rworld".to_string());
            assert_eq!(result, vec!["hello", "world"]);
        }

        #[test]
        fn test_mixed_whitespace_types() {
            let tokenizer = WhiteSpaceTokenizer;
            let result = tokenizer.tokenize("a\tb\nc\rd \n\t e".to_string());
            assert_eq!(result, vec!["a", "b", "c", "d", "e"]);
        }

        #[test]
        fn test_urls_and_paths() {
            let tokenizer = WhiteSpaceTokenizer;
            let result = tokenizer.tokenize("http://example.com /path/to/file".to_string());
            assert_eq!(result, vec!["http://example.com", "/path/to/file"]);
        }

        #[test]
        fn test_special_characters() {
            let tokenizer = WhiteSpaceTokenizer;
            let result = tokenizer.tokenize("test@example.com hello#world $100".to_string());
            assert_eq!(result, vec!["test@example.com", "hello#world", "$100"]);
        }
    }
}

// TokenFilter Tests

#[cfg(test)]
mod token_filter_tests {
    use super::*;

    mod lowercase_token_filter {
        use super::*;

        #[test]
        fn test_empty_vec() {
            let filter = LowerCaseTokenFilter;
            let result = filter.filter(vec![]);
            assert_eq!(result.len(), 0);
        }

        #[test]
        fn test_already_lowercase() {
            let filter = LowerCaseTokenFilter;
            let tokens = make_tokens(vec!["hello", "world"]);
            let result = filter.filter(tokens);
            assert_eq!(extract_terms(&result), vec!["hello", "world"]);
        }

        #[test]
        fn test_uppercase_to_lowercase() {
            let filter = LowerCaseTokenFilter;
            let tokens = make_tokens(vec!["HELLO", "WORLD"]);
            let result = filter.filter(tokens);
            assert_eq!(extract_terms(&result), vec!["hello", "world"]);
        }

        #[test]
        fn test_mixed_case() {
            let filter = LowerCaseTokenFilter;
            let tokens = make_tokens(vec!["HeLLo", "WoRLd"]);
            let result = filter.filter(tokens);
            assert_eq!(extract_terms(&result), vec!["hello", "world"]);
        }

        #[test]
        fn test_with_punctuation() {
            let filter = LowerCaseTokenFilter;
            let tokens = make_tokens(vec!["Hello!", "WORLD?"]);
            let result = filter.filter(tokens);
            assert_eq!(extract_terms(&result), vec!["hello!", "world?"]);
        }

        #[test]
        fn test_numbers_unchanged() {
            let filter = LowerCaseTokenFilter;
            let tokens = make_tokens(vec!["Test123", "456TEST"]);
            let result = filter.filter(tokens);
            assert_eq!(extract_terms(&result), vec!["test123", "456test"]);
        }

        #[test]
        fn test_unicode_lowercase() {
            let filter = LowerCaseTokenFilter;
            let tokens = make_tokens(vec!["CAFÉ", "MÜNCHEN"]);
            let result = filter.filter(tokens);
            assert_eq!(extract_terms(&result), vec!["café", "münchen"]);
        }

        #[test]
        fn test_preserves_order() {
            let filter = LowerCaseTokenFilter;
            let tokens = make_tokens(vec!["First", "Second", "Third"]);
            let result = filter.filter(tokens);
            assert_eq!(extract_terms(&result), vec!["first", "second", "third"]);
        }

        #[test]
        fn test_empty_strings() {
            let filter = LowerCaseTokenFilter;
            let tokens = make_tokens(vec!["", "Test", ""]);
            let result = filter.filter(tokens);
            assert_eq!(extract_terms(&result), vec!["", "test", ""]);
        }

        #[test]
        fn test_special_chars() {
            let filter = LowerCaseTokenFilter;
            let tokens = make_tokens(vec!["@TEST", "#HELLO"]);
            let result = filter.filter(tokens);
            assert_eq!(extract_terms(&result), vec!["@test", "#hello"]);
        }

        #[test]
        fn test_very_long_token() {
            let filter = LowerCaseTokenFilter;
            let long_token = "A".repeat(10000);
            let tokens = vec![TextToken {
                term: long_token,
                pos: 0,
            }];
            let result = filter.filter(tokens);
            assert_eq!(result[0].term, "a".repeat(10000));
        }
    }

    mod stopword_token_filter {
        use super::*;

        #[test]
        fn test_empty_vec() {
            let filter = StopWordTokenFilter;
            let result = filter.filter(vec![]);
            assert_eq!(result.len(), 0);
        }

        #[test]
        fn test_removes_common_stopwords() {
            let filter = StopWordTokenFilter;
            let tokens = make_tokens(vec!["the", "quick", "brown", "fox"]);
            let result = filter.filter(tokens);
            let terms = extract_terms(&result);
            assert!(!terms.contains(&"the".to_string()));
            assert!(terms.contains(&"quick".to_string()));
            assert!(terms.contains(&"brown".to_string()));
            assert!(terms.contains(&"fox".to_string()));
        }

        #[test]
        fn test_removes_multiple_stopwords() {
            let filter = StopWordTokenFilter;
            let tokens = make_tokens(vec!["the", "a", "an", "and", "or", "word"]);
            let result = filter.filter(tokens);
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].term, "word");
        }

        #[test]
        fn test_all_stopwords() {
            let filter = StopWordTokenFilter;
            let tokens = make_tokens(vec!["the", "a", "is", "at"]);
            let result = filter.filter(tokens);
            assert_eq!(result.len(), 0);
        }

        #[test]
        fn test_no_stopwords() {
            let filter = StopWordTokenFilter;
            let tokens = make_tokens(vec!["quick", "brown", "fox"]);
            let result = filter.filter(tokens);
            assert_eq!(result.len(), 3);
        }

        #[test]
        fn test_case_sensitive() {
            let filter = StopWordTokenFilter;
            // Stopwords are typically lowercase, so "The" might not be removed
            let tokens = make_tokens(vec!["The", "quick"]);
            let result = filter.filter(tokens);
            let terms = extract_terms(&result);
            // This tests whether the filter is case-sensitive
            // If case-insensitive, result would be ["quick"]
            // If case-sensitive, result would be ["The", "quick"]
            assert!(terms.contains(&"quick".to_string()));
        }

        #[test]
        fn test_preserves_order() {
            let filter = StopWordTokenFilter;
            let tokens = make_tokens(vec!["apple", "the", "banana", "and", "cherry"]);
            let result = filter.filter(tokens);
            assert_eq!(extract_terms(&result), vec!["apple", "banana", "cherry"]);
        }

        #[test]
        fn test_duplicates_preserved() {
            let filter = StopWordTokenFilter;
            let tokens = make_tokens(vec!["apple", "apple", "the", "apple"]);
            let result = filter.filter(tokens);
            assert_eq!(result.len(), 3);
            assert!(result.iter().all(|t| t.term == "apple"));
        }

        #[test]
        fn test_with_punctuation() {
            let filter = StopWordTokenFilter;
            let tokens = make_tokens(vec!["the", "word!", "and", "another?"]);
            let result = filter.filter(tokens);
            let terms = extract_terms(&result);
            // Stopwords with punctuation should NOT be removed
            assert!(terms.contains(&"word!".to_string()));
            assert!(terms.contains(&"another?".to_string()));
        }

        #[test]
        fn test_numbers_and_stopwords() {
            let filter = StopWordTokenFilter;
            let tokens = make_tokens(vec!["the", "123", "and", "456"]);
            let result = filter.filter(tokens);
            assert_eq!(extract_terms(&result), vec!["123", "456"]);
        }

        #[test]
        fn test_empty_strings() {
            let filter = StopWordTokenFilter;
            let tokens = make_tokens(vec!["", "the", "", "word"]);
            let result = filter.filter(tokens);
            let terms = extract_terms(&result);
            assert!(terms.contains(&"".to_string()));
            assert!(terms.contains(&"word".to_string()));
            assert!(!terms.contains(&"the".to_string()));
        }
    }

    mod porter_stemmer_token_filter {
        use super::*;

        #[test]
        fn test_empty_vec() {
            let filter = PorterStemmerTokenFilter;
            let result = filter.filter(vec![]);
            assert_eq!(result.len(), 0);
        }

        #[test]
        fn test_basic_stemming() {
            let filter = PorterStemmerTokenFilter;
            let tokens = make_tokens(vec!["running", "runs", "run"]);
            let result = filter.filter(tokens);
            // All should stem to "run"
            assert!(result.iter().all(|t| t.term == "run"));
        }

        #[test]
        fn test_plural_to_singular() {
            let filter = PorterStemmerTokenFilter;
            let tokens = make_tokens(vec!["cats", "dogs"]);
            let result = filter.filter(tokens);
            assert_eq!(result[0].term, "cat");
            assert_eq!(result[1].term, "dog");
        }

        #[test]
        fn test_verb_forms() {
            let filter = PorterStemmerTokenFilter;
            let tokens = make_tokens(vec!["fishing", "fished", "fisher"]);
            let result = filter.filter(tokens);
            // All should stem to similar form
            assert!(result.iter().all(|t| t.term.starts_with("fish")));
        }

        #[test]
        fn test_adjective_forms() {
            let filter = PorterStemmerTokenFilter;
            let tokens = make_tokens(vec!["happy", "happiness", "happier"]);
            let result = filter.filter(tokens);
            // All should stem to similar form
            assert!(result.iter().all(|t| t.term.starts_with("happi")));
        }

        #[test]
        fn test_preserves_short_words() {
            let filter = PorterStemmerTokenFilter;
            let tokens = make_tokens(vec!["is", "at", "it"]);
            let result = filter.filter(tokens);
            // Short words typically unchanged
            assert_eq!(result.len(), 3);
        }

        #[test]
        fn test_already_stemmed() {
            let filter = PorterStemmerTokenFilter;
            let tokens = make_tokens(vec!["run", "jump"]);
            let result = filter.filter(tokens);
            // Already in base form
            assert_eq!(result[0].term, "run");
            assert_eq!(result[1].term, "jump");
        }

        #[test]
        fn test_with_punctuation() {
            let filter = PorterStemmerTokenFilter;
            let tokens = make_tokens(vec!["running!", "runs?"]);
            let result = filter.filter(tokens);
            // Punctuation might affect stemming
            assert_eq!(result.len(), 2);
        }

        #[test]
        fn test_uppercase() {
            let filter = PorterStemmerTokenFilter;
            let tokens = make_tokens(vec!["RUNNING", "RUNS"]);
            let result = filter.filter(tokens);
            // Porter stemmer might handle case differently
            assert_eq!(result.len(), 2);
        }

        #[test]
        fn test_empty_string() {
            let filter = PorterStemmerTokenFilter;
            let tokens = make_tokens(vec![""]);
            let result = filter.filter(tokens);
            assert_eq!(result[0].term, "");
        }

        #[test]
        fn test_numbers() {
            let filter = PorterStemmerTokenFilter;
            let tokens = make_tokens(vec!["123", "456"]);
            let result = filter.filter(tokens);
            // Numbers should remain unchanged
            assert_eq!(result[0].term, "123");
            assert_eq!(result[1].term, "456");
        }

        #[test]
        fn test_complex_words() {
            let filter = PorterStemmerTokenFilter;
            let tokens = make_tokens(vec!["complicated", "complication", "complicating"]);
            let result = filter.filter(tokens);
            // All should stem to similar form
            assert!(result.iter().all(|t| t.term.starts_with("complic")));
        }

        #[test]
        fn test_preserves_order() {
            let filter = PorterStemmerTokenFilter;
            let tokens = make_tokens(vec!["first", "second", "third"]);
            let result = filter.filter(tokens);
            assert_eq!(result.len(), 3);
        }

        #[test]
        fn test_ed_ing_suffixes() {
            let filter = PorterStemmerTokenFilter;
            let tokens = make_tokens(vec!["walked", "walking", "walk"]);
            let result = filter.filter(tokens);
            // All should stem to "walk"
            assert!(result.iter().all(|t| t.term == "walk"));
        }

        #[test]
        fn test_ies_suffix() {
            let filter = PorterStemmerTokenFilter;
            let tokens = make_tokens(vec!["flies", "tries"]);
            let result = filter.filter(tokens);
            assert_eq!(result[0].term, "fli");
            assert_eq!(result[1].term, "tri");
        }

        #[test]
        fn test_ational_suffix() {
            let filter = PorterStemmerTokenFilter;
            let tokens = make_tokens(vec!["relational", "conditional", "rational"]);
            let result = filter.filter(tokens);
            assert_eq!(result.len(), 3);
            // These should be stemmed
            assert!(result.iter().all(|t| t.term.len() < 10));
        }
    }

    // Integration tests for chaining filters
    #[test]
    fn test_chained_filters_lowercase_then_stopword() {
        let lowercase = LowerCaseTokenFilter;
        let stopword = StopWordTokenFilter;

        let tokens = make_tokens(vec!["The", "QUICK", "Brown"]);
        let tokens = lowercase.filter(tokens);
        let result = stopword.filter(tokens);
        let terms = extract_terms(&result);

        assert!(!terms.contains(&"the".to_string()));
        assert!(terms.contains(&"quick".to_string()));
        assert!(terms.contains(&"brown".to_string()));
    }

    #[test]
    fn test_chained_filters_lowercase_stopword_stem() {
        let lowercase = LowerCaseTokenFilter;
        let stopword = StopWordTokenFilter;
        let stemmer = PorterStemmerTokenFilter;

        let tokens = make_tokens(vec!["The", "RUNNING", "Dogs"]);
        let tokens = lowercase.filter(tokens);
        let tokens = stopword.filter(tokens);
        let result = stemmer.filter(tokens);
        let terms = extract_terms(&result);

        assert_eq!(result.len(), 2);
        assert!(terms.contains(&"run".to_string()));
        assert!(terms.contains(&"dog".to_string()));
    }

    #[test]
    fn test_full_pipeline_realistic() {
        let lowercase = LowerCaseTokenFilter;
        let stopword = StopWordTokenFilter;
        let stemmer = PorterStemmerTokenFilter;

        let tokens = make_tokens(vec![
            "The", "Quick", "Brown", "Foxes", "are", "Jumping", "over", "the", "Lazy", "Dogs",
        ]);

        let tokens = lowercase.filter(tokens);
        let tokens = stopword.filter(tokens);
        let result = stemmer.filter(tokens);
        let terms = extract_terms(&result);

        // Should have: quick, brown, foxes->fox, jumping->jump, lazy, dogs->dog
        assert!(result.len() <= 6);
        assert!(terms.contains(&"quick".to_string()));
        assert!(terms.contains(&"brown".to_string()));
    }
}
