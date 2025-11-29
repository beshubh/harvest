use harvest::crawler::Crawler;
use harvest::db::{CrawlResultRepo, Database};

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
    let crawl_results_repo = CrawlResultRepo::new(&Database::get());

    let crawler = Crawler::new(1, crawl_results_repo);
    let res = crawler.crawl("https://books.toscrape.com/".into()).await;
    match res {
        Ok(res) => println!("{:?}", res),
        Err(e) => println!("{:#}", e),
    }
    Ok(())
}
