use std::sync::Arc;

use harvest::crawler::Crawler;
use harvest::db::{PageRepo, Database};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing subscriber (handles both tracing and log crate)
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(true)
        .init();

    // Bridge log crate -> tracing (so log::info! etc. work)
    // tracing_log::LogTracer::init()?;

    Database::init_global().await?;
    let pages_repo = PageRepo::new(&Database::get());
    let max_depth = 5;
    let max_concurrent_fetches = 150_usize;
    let frontier_size = 500_usize;
    let crawler = Crawler::new(max_depth, pages_repo, max_concurrent_fetches, frontier_size);
    let crawler = Arc::new(crawler);
    let res = crawler.crawl("https://books.toscrape.com/".into()).await;
    match res {
        Ok(res) => println!("{:?}", res),
        Err(e) => println!("{:#}", e),
    }
    futures::future::pending::<()>().await;
    Ok(())
}
