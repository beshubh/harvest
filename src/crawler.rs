use std::collections::{HashSet, VecDeque};

use anyhow::Result;
use dashmap::DashSet;
use reqwest::Url;
use scraper::{Html, Selector};

use crate::data_models::CrawlResult;
use crate::db::CrawlResultRepo;


pub struct Crawler {
    visited_urls: DashSet<String>,
    max_depth: usize,
    crawl_results_repo: CrawlResultRepo,
}

impl Crawler {
    pub fn new(max_depth: usize, crawl_results_repo: CrawlResultRepo) -> Crawler {
        Crawler {
            visited_urls: DashSet::new(),
            max_depth,
            crawl_results_repo,
        }
    }

    pub async fn crawl(&self, starting_url: String) -> Result<()> {
        self.crawl_inner(starting_url, 0).await
    }

    async fn crawl_inner(&self, starting_url: String, depth: usize) -> Result<()> {
        let mut stack = VecDeque::new();
        stack.push_back((starting_url, depth));
        // BFS traversal
        let mut crawled_results = vec![];
        while let Some((url, depth)) = stack.pop_front() {
            if depth > self.max_depth {
                log::info!("max depth reached, skipping...");
                continue;
            }
            if self.visited_urls.contains(&url) {
                continue;
            }
            log::info!("Crawling {}", url);
            self.visited_urls.insert(url.clone());
            let html = self.fetch_page(&url).await?;
            let result = self.parse_html(&url, &html).await?;
            crawled_results.push(result.clone());

            stack.push_back((result.url, depth + 1));
            for link in &result.outgoing_links {
                stack.push_back((link.clone(), depth + 1));
            }
        }
        log::info!("Crawled {} results", crawled_results.len());
        let ids = self.crawl_results_repo.insert_many(&crawled_results).await?;
        log::info!("inserted to mongo, results: {}", ids.len());
        Ok(())
    }

    async fn fetch_page(&self, url: &str) -> Result<String> {
        let client = reqwest::Client::new();
        let res = client.get(url).send().await?;
        let body = res.text().await?;
        Ok(body)
    }

    async fn parse_html(&self, base_url: &str, html: &str) -> Result<CrawlResult> {
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

        let result = CrawlResult::new(
            base_url.to_string(),
            title,
            body,
            seen.into_iter().collect(),
            0,     // depth will be set by caller if needed
            false, // is_seed will be set by caller if needed
        );
        Ok(result)
    }
}
