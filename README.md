# Harvest 🌾

A full-text search engine built from scratch in Rust. Crawls the web, builds an inverted index with positional data, and supports phrase queries.

## What This Project Demonstrates

- **Web Crawling**: Concurrent BFS crawler with configurable depth, rate limiting via semaphores, and URL deduplication
- **Text Analysis Pipeline**: Modular design with character filters, tokenizers, and token filters (lowercase, stop words, Porter stemming)
- **SPIMI Indexing**: Single-Pass In-Memory Indexing with memory budgets, disk block persistence, and k-way merge
- **Positional Index**: Stores term positions per document for phrase/proximity query support
- **Phrase Queries**: Positional intersection algorithm to match exact phrases across documents
- **Incremental Indexing**: Re-running the indexer only processes new pages, appends to existing term buckets

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Rust (2024 edition) |
| Async Runtime | Tokio |
| Database | MongoDB |
| Web Framework | Axum |
| HTML Parsing | scraper, html5ever |

## Quick Start

### Prerequisites

- Rust (latest stable)
- MongoDB running locally or accessible via `MONGO_URI`

### Setup

```bash
# Clone and build
git clone https://github.com/yourusername/harvest.git
cd harvest
cargo build --release

# Configure (optional)
cp .env.example .env
# Edit .env with your MongoDB URI if needed
```

### Usage

**1. Crawl websites**
```bash
# Crawl Wikipedia starting from the search engine article, depth 2
# Recommendation for max-depth is not make it too big like even with number say 10
# it will take a lot of time as the crawler will go to deep in web.
# BUT, you can always just stop the crawler.
cargo run --release -- crawl \
  --url "https://en.wikipedia.org/wiki/Search_engine" \
  --max-depth 2 \
  --max-concurrent-fetches 100

```

**2. Build the index**
```bash
# Index all crawled pages (100MB memory budget for SPIMI blocks)
cargo run --release -- index \
  --page-fetch-limit 10000 \
  --budget-bytes 100000000
```

**3. Start the search server**
```bash
cargo run --release -- serve --port 3000
```

Then open http://localhost:3000 in your browser.

## CLI Reference

```
harvest <command> [options]

Commands:
  crawl   Crawl websites starting from a seed URL
  index   Build inverted index from crawled pages
  serve   Start the web server with search API and UI

crawl:
  -u, --url <URL>                    Seed URL to start crawling
  -d, --max-depth <N>                Maximum crawl depth [default: 0]
  -f, --max-concurrent-fetches <N>   Concurrent HTTP requests [default: 150]
  -s, --frontier-size <N>            URL frontier queue size [default: 500]

index:
  -p, --page-fetch-limit <N>         Pages per batch [default: 10000]
  -b, --budget-bytes <N>             Memory budget before flush [default: 100MB]

serve:
  -p, --port <N>                     Server port [default: 3000]
  -H, --host <ADDR>                  Bind address [default: 127.0.0.1]
```

## Architecture

See [ARCHITECTURE.md](./ARCHITECTURE.md) for system design and component details.

## Running Tests

```bash
# Unit tests
cargo test

# Integration tests (requires MongoDB)
cargo test --test query_engine_tests
cargo test --test indexer_tests
```

## TODOs
 - [ ] IP rotation service integration so we don't get blacklisted by websites.
 - [ ] Spell correction. (Did you mean x?)
 - [ ] Re-Indexing of the same pages, crawled after some time of indexing. (Currently we only support incremental indexing)


## License

MIT
