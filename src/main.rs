use std::sync::Arc;

use clap::{Parser, Subcommand};
use futures::future;
use harvest::crawler::Crawler;
use harvest::db::{Database, PageRepo};
use harvest::indexer::Indexer;

#[derive(Parser)]
#[command(name = "harvest")]
#[command(about = "A web crawler and indexer", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Crawl websites starting from a seed URL
    Crawl {
        /// The seed URL to start crawling from
        #[arg(short, long)]
        url: String,

        /// Maximum crawl depth (0 = only the seed URL)
        #[arg(short = 'd', long, default_value_t = 0)]
        max_depth: usize,

        /// Maximum number of concurrent HTTP fetches
        #[arg(short = 'f', long, default_value_t = 150)]
        max_concurrent_fetches: usize,

        /// Size of the URL frontier queue
        #[arg(short = 's', long, default_value_t = 500)]
        frontier_size: usize,
    },
    /// Index the documents that were previously crawled
    Index {
        /// Number of pages to fetch per batch during indexing
        #[arg(short, long, default_value_t = 10000)]
        page_fetch_limit: i64,

        /// Memory budget in bytes for SPIMI indexing before flushing to disk
        #[arg(short, long, default_value_t = 100_000_000)]
        budget_bytes: usize,
    },
    /// Start the web server to serve the search API and UI
    Serve {
        /// Port to bind the server to
        #[arg(short, long, default_value_t = 3000)]
        port: u16,

        /// Host to bind the server to
        #[arg(short = 'H', long, default_value = "127.0.0.1")]
        host: String,
    },
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(true)
        .init();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(8 * 1024 * 1024) // 8 MB stack size per worker thread
        .build()?;

    runtime.block_on(async_main())
}

async fn async_main() -> anyhow::Result<()> {
    Database::init_global().await?;

    let cli = Cli::parse();

    match cli.command {
        Commands::Crawl {
            url,
            max_depth,
            max_concurrent_fetches,
            frontier_size,
        } => {
            run_crawl(
                url,
                max_depth,
                max_concurrent_fetches,
                frontier_size,
            )
            .await?;
        }
        Commands::Index {
            page_fetch_limit,
            budget_bytes,
        } => {
            run_index(page_fetch_limit, budget_bytes).await?;
        }
        Commands::Serve { port, host } => {
            run_serve(port, host).await?;
        }
    }

    Ok(())
}

async fn run_crawl(
    url: String,
    max_depth: usize,
    max_concurrent_fetches: usize,
    frontier_size: usize,
) -> anyhow::Result<()> {
    let pages_repo = PageRepo::new(&Database::get());

    let crawler = Crawler::new(max_depth, pages_repo, max_concurrent_fetches, frontier_size);
    let crawler = Arc::new(crawler);

    log::info!(
        "Starting crawl from {} with max_depth={}, max_concurrent_fetches={}, frontier_size={}",
        url,
        max_depth,
        max_concurrent_fetches,
        frontier_size,
    );

    match crawler.crawl(url).await {
        Ok(res) => {
            log::info!("Crawl Spinned: {:?}", res);
        }
        Err(e) => {
            log::error!("Crawl failed: {:#}", e);
            return Err(e);
        }
    }
    future::pending::<()>().await;
    Ok(())
}

async fn run_index(page_fetch_limit: i64, budget_bytes: usize) -> anyhow::Result<()> {
    let db = Database::get().clone();
    let pages_repo = Arc::new(PageRepo::new(&db));

    log::info!(
        "Starting indexing with page_fetch_limit={}, budget_bytes={}",
        page_fetch_limit,
        budget_bytes
    );

    let indexer = Arc::new(Indexer::new(pages_repo, page_fetch_limit, db));
    indexer.run(budget_bytes).await?;
    log::info!("Indexing completed");
    Ok(())
}

async fn run_serve(port: u16, host: String) -> anyhow::Result<()> {
    use harvest::analyzer::TextAnalyzer;
    use harvest::api::create_router;
    use harvest::query_engine::QueryEngine;

    let db = Database::get().clone();

    log::info!("Initializing search engine...");

    // Initialize TextAnalyzer with same pipeline as indexer
    let analyzer = TextAnalyzer::new(
        vec![Box::new(harvest::analyzer::HTMLTagFilter::default())],
        Box::new(harvest::analyzer::WhiteSpaceTokenizer),
        vec![
            Box::new(harvest::analyzer::PunctuationStripFilter::default()),
            Box::new(harvest::analyzer::LowerCaseTokenFilter),
            Box::new(harvest::analyzer::NumericTokenFilter),
            Box::new(harvest::analyzer::StopWordTokenFilter),
            Box::new(harvest::analyzer::PorterStemmerTokenFilter),
        ],
    );

    // Initialize QueryEngine
    let query_engine = Arc::new(QueryEngine::new(db, analyzer));

    // Create router with all routes
    let app = create_router(query_engine);

    // Bind to address
    let addr = format!("{}:{}", host, port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    log::info!("🚀 Server running at http://{}", addr);
    log::info!("📂 Serving static files from ./static");
    log::info!("🔍 API endpoint: POST http://{}/api/search", addr);

    // Start server
    axum::serve(listener, app).await?;

    Ok(())
}
