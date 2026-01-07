# Harvest Architecture

## System Overview

```mermaid
flowchart TB
    subgraph "Data Ingestion"
        direction TB
        SEED["🌐 Seed URLs"]
        CRAWLER["Crawler"]
        FRONTIER["URL Frontier<br/>(mpsc channel)"]
        FETCHER["HTTP Fetcher<br/>(reqwest + Semaphore)"]
        PARSER["HTML Parser<br/>(scraper)"]
    end

    subgraph "Text Processing"
        direction TB
        ANALYZER["Text Analyzer"]
        subgraph "Analysis Pipeline"
            CHAR_FILTER["Character Filters<br/>• HTMLTagFilter"]
            TOKENIZER["Tokenizer<br/>• WhiteSpaceTokenizer"]
            TOKEN_FILTER["Token Filters<br/>• LowerCase<br/>• Punctuation Strip<br/>• Stop Words<br/>• Numeric Filter<br/>• Porter Stemmer"]
        end
    end

    subgraph "Indexing Engine"
        direction TB
        INDEXER["Indexer"]
        TOKEN_STREAM["Token Stream<br/>(mpsc channel)"]
        SPIMI["SPIMI Invert<br/>(in-memory blocks)"]
        DISK_BLOCKS["Disk Blocks<br/>(JSON files)"]
        MERGE["K-Way Merge<br/>(MinHeap)"]
    end

    subgraph "MongoDB Storage"
        direction TB
        PAGES_COLL[("pages<br/>collection")]
        INDEX_COLL[("inverted_index<br/>collection")]
    end

    subgraph "Query Engine"
        direction TB
        QUERY_ANALYZER["Query Analyzer"]
        INDEX_LOOKUP["Index Lookup"]
        INTERSECT["Positional Intersect"]
        RESULTS["Result Doc IDs"]
    end

    subgraph "REST API"
        direction TB
        AXUM["Axum Server"]
        SEARCH_API["/api/search"]
        STATIC["Static UI<br/>(HTML/CSS/JS)"]
    end

    USER["👤 User"]

    %% Data Ingestion Flow
    SEED --> CRAWLER
    CRAWLER --> FRONTIER
    FRONTIER --> FETCHER
    FETCHER --> PARSER
    PARSER --> PAGES_COLL
    PARSER -.->|"outgoing links"| FRONTIER

    %% Indexing Flow
    PAGES_COLL -->|"unindexed pages"| INDEXER
    INDEXER --> ANALYZER
    ANALYZER --> CHAR_FILTER --> TOKENIZER --> TOKEN_FILTER
    TOKEN_FILTER --> TOKEN_STREAM
    TOKEN_STREAM --> SPIMI
    SPIMI -->|"memory limit"| DISK_BLOCKS
    DISK_BLOCKS --> MERGE
    MERGE --> INDEX_COLL

    %% Query Flow
    USER --> STATIC
    STATIC --> SEARCH_API
    SEARCH_API --> AXUM
    AXUM --> QUERY_ANALYZER
    QUERY_ANALYZER --> ANALYZER
    INDEX_LOOKUP --> INDEX_COLL
    QUERY_ANALYZER --> INDEX_LOOKUP
    INDEX_LOOKUP --> INTERSECT
    INTERSECT --> RESULTS
    RESULTS --> PAGES_COLL
    PAGES_COLL --> AXUM
    AXUM --> USER
```

## Component Details

### Crawler
- **Concurrent fetching** with configurable semaphore limits
- **BFS traversal** with depth tracking
- **Deduplication** via DashSet (concurrent HashSet)
- Respects non-HTML content types

### Text Analyzer Pipeline
```mermaid
flowchart LR
    INPUT["Raw HTML"] --> CF["Character<br/>Filters"]
    CF --> TK["Tokenizer"]
    TK --> TF["Token<br/>Filters"]
    TF --> OUTPUT["Terms +<br/>Positions"]
    
    style INPUT fill:#e1f5fe
    style OUTPUT fill:#c8e6c9
```

| Stage | Components |
|-------|------------|
| Character Filters | `HTMLTagFilter` - strips tags, extracts text |
| Tokenizer | `WhiteSpaceTokenizer` - splits on whitespace |
| Token Filters | `LowerCase`, `PunctuationStrip`, `StopWord`, `Numeric`, `PorterStemmer` |

### Indexer (SPIMI Algorithm)
```mermaid
flowchart LR
    subgraph "Phase 1: SPIMI Invert"
        TOKENS["Token<br/>Stream"] --> DICT["In-Memory<br/>Dictionary"]
        DICT -->|"budget exceeded"| BLOCK["Write Block<br/>to Disk"]
    end
    
    subgraph "Phase 2: Merge"
        BLOCKS["Sorted<br/>Blocks"] --> HEAP["Min-Heap<br/>K-Way Merge"]
        HEAP --> DB["MongoDB<br/>inverted_index"]
    end
    
    BLOCK --> BLOCKS
```

- **Memory-bounded**: Flush to disk when memory budget exceeded
- **Incremental indexing**: Only processes unindexed pages
- **Position tracking**: Stores term positions for phrase queries

### Query Engine
```mermaid
flowchart LR
    Q["Query Text"] --> A["Analyze<br/>(same pipeline)"]
    A --> T["Query Terms"]
    T --> L["Lookup<br/>Posting Lists"]
    L --> I["Positional<br/>Intersect"]
    I --> D["Doc IDs"]
    
    style Q fill:#fff3e0
    style D fill:#c8e6c9
```

- **Phrase queries**: Uses positional intersection with k-distance matching
- **Multi-term queries**: Intersects posting lists starting from shortest

### Data Models

```mermaid
erDiagram
    Page {
        ObjectId _id PK
        string url
        string title
        string html_body
        string cleaned_content
        array outgoing_links
        int depth
        bool is_seed
        bool indexed
        datetime crawled_at
    }
    
    InvertedIndexDoc {
        ObjectId _id PK
        string term
        int bucket
        int document_frequency
        array postings
        map positions
    }
    
    Page ||--o{ InvertedIndexDoc : "indexed as"
```

## Technology Stack

| Layer | Technology |
|-------|------------|
| Language | Rust |
| HTTP Client | reqwest |
| HTML Parsing | scraper, html5ever |
| Stemming | porter_stemmer |
| Database | MongoDB |
| Web Framework | Axum |
| Async Runtime | Tokio |
| Frontend | Vanilla HTML/CSS/JS |
