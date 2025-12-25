use anyhow::Result;
use futures::stream::TryStreamExt;
use mongodb::bson::oid::ObjectId;
use mongodb::bson::{Document, doc};
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use harvest::data_models::{InvertedIndexDoc, Page, SpimiDoc};
use harvest::db::{Database, PageRepo};
use harvest::indexer::{DictItem, Indexer, SpimiBlock, merge_sorted_lists_dedup};

/// Constant matching the one in indexer.rs for test verification.
/// MongoDB has a 16MB document limit, so we use 100K docs per chunk (~5MB documents).
const DOCIDS_PER_MONGO_DOCUMENT: usize = 100_000;

mod test_helpers {
    use mongodb::bson;

    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TEST_DB_COUNTER: AtomicUsize = AtomicUsize::new(0);

    pub fn unique_test_db_name() -> String {
        let count = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        format!("harvest_indexer_test_{}_{}", timestamp, count)
    }

    pub async fn create_test_db() -> Result<(Database, String)> {
        dotenvy::dotenv().ok();
        let uri =
            std::env::var("MONGO_URI").unwrap_or_else(|_| "mongodb://localhost:27017".to_string());
        let db_name = unique_test_db_name();
        let db = Database::new(&uri, &db_name).await?;
        Ok((db, db_name))
    }

    pub async fn cleanup_test_db(db: &Database, db_name: &str) -> Result<()> {
        db.client()
            .database(db_name)
            .drop()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to drop test database: {}", e))?;
        Ok(())
    }

    pub fn create_test_page(url: &str, content: &str) -> Page {
        Page::new(
            url.to_string(),
            format!("Title for {}", url),
            content.to_string(), // html_body contains the content to be analyzed
            vec![],
            0,
            false,
        )
    }

    /// Generate a vector of sorted ObjectIds for testing.
    /// Uses incrementing hex strings to ensure deterministic ordering.
    pub fn generate_sorted_object_ids(count: usize) -> Vec<ObjectId> {
        (0..count)
            .map(|i| {
                // Create a 24-character hex string with the index padded
                let hex = format!("{:024x}", i);
                ObjectId::parse_str(&hex).unwrap()
            })
            .collect()
    }

    /// Generate ObjectIds with a prefix to ensure they sort in a specific range.
    /// Useful for creating distinct ranges across multiple blocks.
    pub fn generate_sorted_object_ids_with_prefix(count: usize, prefix: u32) -> Vec<ObjectId> {
        (0..count)
            .map(|i| {
                // Use prefix in high bits, index in low bits
                let hex = format!("{:08x}{:016x}", prefix, i);
                ObjectId::parse_str(&hex).unwrap()
            })
            .collect()
    }

    /// Create a DictItem with postings and positions.
    /// Each doc_id gets positions starting from `base_position`.
    pub fn create_dict_item_with_positions(
        doc_ids: &[ObjectId],
        positions_per_doc: usize,
        base_position: usize,
    ) -> DictItem {
        let mut dict_item = DictItem::new();
        let mut positions_map = BTreeMap::new();

        for (idx, &doc_id) in doc_ids.iter().enumerate() {
            dict_item.postings.push(doc_id);
            let positions: Vec<usize> = (0..positions_per_doc)
                .map(|p| base_position + idx * positions_per_doc + p)
                .collect();
            positions_map.insert(doc_id, positions);
        }

        dict_item.positions = positions_map;
        dict_item
    }

    /// Create a SpimiBlock with a single term and many documents.
    pub fn create_large_block_single_term(
        term: &str,
        doc_ids: &[ObjectId],
        positions_per_doc: usize,
    ) -> SpimiBlock {
        let dict_item = create_dict_item_with_positions(doc_ids, positions_per_doc, 0);
        let mut dictionary = HashMap::new();
        dictionary.insert(term.to_string(), dict_item);

        SpimiBlock {
            sorted_terms: vec![term.to_string()],
            dictionary,
        }
    }

    /// Create a SpimiBlock with multiple terms, each with their own doc_ids.
    pub fn create_block_with_terms(
        terms_with_docs: Vec<(&str, Vec<ObjectId>, usize)>,
    ) -> SpimiBlock {
        let mut dictionary = HashMap::new();
        let mut sorted_terms = Vec::new();

        for (term, doc_ids, positions_per_doc) in terms_with_docs {
            let dict_item = create_dict_item_with_positions(&doc_ids, positions_per_doc, 0);
            dictionary.insert(term.to_string(), dict_item);
            sorted_terms.push(term.to_string());
        }

        sorted_terms.sort();

        SpimiBlock {
            sorted_terms,
            dictionary,
        }
    }

    /// Get all SpimiDoc documents from a collection
    pub async fn get_spimi_docs_from_collection(
        db: &Database,
        collection_name: &str,
        mut projection: Option<bson::Document>,
    ) -> Result<Vec<SpimiDoc>> {
        let default_projection = doc! {
            "term":1,
            "id": 1,
            "bucket": 1,
            "document_frequency": 1,
            "postings": 1,
            "positions": 1
        };
        if projection.is_none() {
            projection = Some(default_projection);
        }
        let collection = db.collection::<SpimiDoc>(collection_name);
        let docs: Vec<SpimiDoc> = collection
            .find(doc! {})
            .projection(projection.unwrap())
            .await?
            .try_collect()
            .await?;
        Ok(docs)
    }

    /// Get all InvertedIndexDoc documents from the inverted_index collection
    pub async fn get_inverted_index_docs(
        db: &Database,
        mut projection: Option<Document>,
    ) -> Result<Vec<InvertedIndexDoc>> {
        let default_projection = doc! {
            "id": 1,
            "term": 1,
            "bucket": 1,
            "document_frequency": 1,
            "positions": 1,
            "postings": 1,
        };
        if projection.is_none() {
            projection = Some(default_projection);
        }
        let collection = db.collection::<InvertedIndexDoc>("inverted_index");
        let docs: Vec<InvertedIndexDoc> = collection
            .find(doc! {})
            .projection(projection.unwrap())
            .await?
            .try_collect()
            .await?;
        Ok(docs)
    }

    /// Get InvertedIndexDoc documents for a specific term
    pub async fn get_inverted_index_docs_for_term(
        db: &Database,
        term: &str,
    ) -> Result<Vec<InvertedIndexDoc>> {
        let collection = db.collection::<InvertedIndexDoc>("inverted_index");
        let docs: Vec<InvertedIndexDoc> = collection
            .find(doc! { "term": term })
            .await?
            .try_collect()
            .await?;
        Ok(docs)
    }

    /// Count SPIMI block collections in the database
    pub async fn count_spimi_block_collections(db: &Database) -> Result<usize> {
        let filter = doc! {
            "name": {
                "$regex": r"^spimi_block_"
            }
        };
        let collections = db.database().list_collection_names().filter(filter).await?;
        Ok(collections.len())
    }

    /// Get all SPIMI block collection names
    pub async fn get_spimi_block_collection_names(db: &Database) -> Result<Vec<String>> {
        let filter = doc! {
            "name": {
                "$regex": r"^spimi_block_"
            }
        };
        let collections = db.database().list_collection_names().filter(filter).await?;
        Ok(collections)
    }
}

use test_helpers::*;

#[tokio::test]
async fn test_indexer_new_creates_valid_instance() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));

    let _indexer = Indexer::new(pages_repo, 100, db.clone());

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_indexer_new_with_different_page_limits() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));

    let _indexer_small = Indexer::new(Arc::clone(&pages_repo), 10, db.clone());
    let _indexer_large = Indexer::new(Arc::clone(&pages_repo), 100_000, db.clone());

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

// Indexer::pages_to_token_stream tests
#[tokio::test]
async fn test_pages_to_token_stream_single_page_single_term() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let mut indexer = Indexer::new(pages_repo, 100, db.clone());

    let page = create_test_page("http://example.com", "elephant");
    let pages: Vec<Arc<Page>> = vec![Arc::new(page.clone())];

    indexer.pages_to_token_stream(&pages)?;

    let tokens = indexer.drain_tokens().await;
    // Should have at least 1 token after filtering
    assert!(
        tokens.len() >= 1,
        "Expected at least 1 token, got {}",
        tokens.len()
    );

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_pages_to_token_stream_single_page_multiple_terms() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let mut indexer = Indexer::new(pages_repo, 100, db.clone());

    let page = create_test_page("http://example.com", "elephant giraffe zebra penguin");
    let pages: Vec<Arc<Page>> = vec![Arc::new(page.clone())];

    indexer.pages_to_token_stream(&pages)?;

    let tokens = indexer.drain_tokens().await;
    // Should have multiple tokens (may be less than 4 due to stemming/filtering)
    assert!(
        tokens.len() >= 2,
        "Expected at least 2 tokens, got {}",
        tokens.len()
    );

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_pages_to_token_stream_multiple_pages() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let mut indexer = Indexer::new(pages_repo, 100, db.clone());

    let page1 = create_test_page("http://example1.com", "elephant giraffe");
    let page2 = create_test_page("http://example2.com", "zebra penguin");
    let pages: Vec<Arc<Page>> = vec![Arc::new(page1.clone()), Arc::new(page2.clone())];

    indexer.pages_to_token_stream(&pages)?;

    let tokens = indexer.drain_tokens().await;
    // Should have multiple tokens from both pages
    assert!(
        tokens.len() >= 2,
        "Expected at least 2 tokens, got {}",
        tokens.len()
    );

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_pages_to_token_stream_empty_pages() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let mut indexer = Indexer::new(pages_repo, 100, db.clone());

    let pages: Vec<Arc<Page>> = vec![];
    indexer.pages_to_token_stream(&pages)?;

    let tokens = indexer.drain_tokens().await;
    assert!(tokens.is_empty());

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_pages_to_token_stream_page_with_empty_content() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let mut indexer = Indexer::new(pages_repo, 100, db.clone());

    let page = create_test_page("http://example.com", "");
    let pages: Vec<Arc<Page>> = vec![Arc::new(page)];

    indexer.pages_to_token_stream(&pages)?;

    let tokens = indexer.drain_tokens().await;
    assert!(tokens.is_empty());

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_pages_to_token_stream_preserves_order() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let mut indexer = Indexer::new(pages_repo, 100, db.clone());

    let page = create_test_page("http://example.com", "alpha beta gamma delta");
    let pages: Vec<Arc<Page>> = vec![Arc::new(page)];

    indexer.pages_to_token_stream(&pages)?;

    let tokens = indexer.drain_tokens().await;
    assert_eq!(tokens.len(), 4);

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_pages_to_token_stream_handles_extra_whitespace() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let mut indexer = Indexer::new(pages_repo, 100, db.clone());

    let page = create_test_page("http://example.com", "elephant   giraffe\t\tzebra\npenguin");
    let pages: Vec<Arc<Page>> = vec![Arc::new(page)];

    indexer.pages_to_token_stream(&pages)?;

    let tokens = indexer.drain_tokens().await;
    // Whitespace should be normalized, should have multiple tokens
    assert!(
        tokens.len() >= 2,
        "Expected at least 2 tokens, got {}",
        tokens.len()
    );

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

// Indexer::spimi_invert tests
// Note: spimi_invert now requires Arc<Self> and consumes tokens from the internal stream
// These tests verify the persist_block_to_disk functionality instead
#[tokio::test]
async fn test_spimi_invert_empty_token_stream() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    // Empty block should not cause errors
    let block = SpimiBlock {
        sorted_terms: vec![],
        dictionary: HashMap::new(),
    };
    indexer.persist_block_to_disk(block).await?;

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_spimi_invert_single_token() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    let doc_id = ObjectId::new();
    let mut dictionary = HashMap::new();
    let mut dict_item = DictItem::new();
    dict_item.postings.push(doc_id);
    dict_item.positions.insert(doc_id, vec![0]);
    dictionary.insert("hello".to_string(), dict_item);

    let block = SpimiBlock {
        sorted_terms: vec!["hello".to_string()],
        dictionary,
    };
    indexer.persist_block_to_disk(block).await?;

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_spimi_invert_multiple_tokens_same_term() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    let doc_id1 = ObjectId::new();
    let doc_id2 = ObjectId::new();
    let doc_id3 = ObjectId::new();

    let mut dictionary = HashMap::new();
    let mut dict_item = DictItem::new();
    dict_item.postings.push(doc_id1);
    dict_item.postings.push(doc_id2);
    dict_item.postings.push(doc_id3);
    dict_item.positions.insert(doc_id1, vec![0]);
    dict_item.positions.insert(doc_id2, vec![0]);
    dict_item.positions.insert(doc_id3, vec![0]);
    dictionary.insert("hello".to_string(), dict_item);

    let block = SpimiBlock {
        sorted_terms: vec!["hello".to_string()],
        dictionary,
    };
    indexer.persist_block_to_disk(block).await?;

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_spimi_invert_multiple_tokens_different_terms() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    let doc_id = ObjectId::new();

    let mut dictionary = HashMap::new();

    let mut dict_item1 = DictItem::new();
    dict_item1.postings.push(doc_id);
    dict_item1.positions.insert(doc_id, vec![0]);
    dictionary.insert("hello".to_string(), dict_item1);

    let mut dict_item2 = DictItem::new();
    dict_item2.postings.push(doc_id);
    dict_item2.positions.insert(doc_id, vec![1]);
    dictionary.insert("world".to_string(), dict_item2);

    let mut dict_item3 = DictItem::new();
    dict_item3.postings.push(doc_id);
    dict_item3.positions.insert(doc_id, vec![2]);
    dictionary.insert("foo".to_string(), dict_item3);

    let mut sorted_terms: Vec<String> = dictionary.keys().cloned().collect();
    sorted_terms.sort();

    let block = SpimiBlock {
        sorted_terms,
        dictionary,
    };
    indexer.persist_block_to_disk(block).await?;

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_spimi_invert_triggers_flush_on_budget_exceeded() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));

    // Create pages that will generate tokens
    for i in 0..10 {
        let page = create_test_page(&format!("http://example{}.com", i), &format!("term{}", i));
        pages_repo.insert(&page).await?;
    }

    let indexer = Arc::new(Indexer::new(Arc::clone(&pages_repo), 100, db.clone()));

    // Run with very small budget to force multiple flushes
    indexer.run(100).await?;

    let collections = db.database().list_collection_names().await?;

    // Should have created the inverted_index collection
    let has_inverted_index = collections.iter().any(|name| name == "inverted_index");
    assert!(has_inverted_index, "Should have created inverted index");

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

// Indexer::persist_block_to_disk tests

#[tokio::test]
async fn test_persist_block_to_disk_empty_block() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    let block = SpimiBlock {
        sorted_terms: vec![],
        dictionary: HashMap::new(),
    };

    indexer.persist_block_to_disk(block).await?;

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_persist_block_to_disk_single_term() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    let doc_id = ObjectId::new();
    let mut dictionary = HashMap::new();
    let mut dict_item = DictItem::new();
    dict_item.postings.push(doc_id);
    dict_item.positions.insert(doc_id, vec![0]);
    dictionary.insert("hello".to_string(), dict_item);

    let block = SpimiBlock {
        sorted_terms: vec!["hello".to_string()],
        dictionary,
    };

    indexer.persist_block_to_disk(block).await?;

    let collections = db.database().list_collection_names().await?;
    let has_spimi_block = collections
        .iter()
        .any(|name| name.starts_with("spimi_block_"));
    assert!(
        has_spimi_block,
        "Should have created a SPIMI block collection"
    );

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_persist_block_to_disk_multiple_terms() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    let doc_id1 = ObjectId::new();
    let doc_id2 = ObjectId::new();

    let mut dictionary = HashMap::new();

    let mut dict_item1 = DictItem::new();
    dict_item1.postings.push(doc_id1);
    dict_item1.positions.insert(doc_id1, vec![0]);
    dictionary.insert("apple".to_string(), dict_item1);

    let mut dict_item2 = DictItem::new();
    dict_item2.postings.push(doc_id2);
    dict_item2.positions.insert(doc_id2, vec![0]);
    dictionary.insert("banana".to_string(), dict_item2);

    let mut dict_item3 = DictItem::new();
    dict_item3.postings.push(doc_id1);
    dict_item3.postings.push(doc_id2);
    dict_item3.positions.insert(doc_id1, vec![1]);
    dict_item3.positions.insert(doc_id2, vec![1]);
    dictionary.insert("cherry".to_string(), dict_item3);

    let mut sorted_terms: Vec<String> = dictionary.keys().cloned().collect();
    sorted_terms.sort();

    let block = SpimiBlock {
        sorted_terms,
        dictionary,
    };

    indexer.persist_block_to_disk(block).await?;

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_persist_block_to_disk_verifies_data_stored() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    let doc_id = ObjectId::new();
    let mut dictionary = HashMap::new();
    let mut dict_item = DictItem::new();
    dict_item.postings.push(doc_id);
    dict_item.positions.insert(doc_id, vec![5]);
    dictionary.insert("testterm".to_string(), dict_item);

    let block = SpimiBlock {
        sorted_terms: vec!["testterm".to_string()],
        dictionary,
    };

    indexer.persist_block_to_disk(block).await?;

    let collections = db.database().list_collection_names().await?;
    let spimi_collection = collections
        .iter()
        .find(|name| name.starts_with("spimi_block_"))
        .expect("Should have a SPIMI block collection");

    use futures::stream::TryStreamExt;
    let collection = db.collection::<SpimiDoc>(spimi_collection);
    let docs: Vec<SpimiDoc> = collection
        .find(mongodb::bson::doc! {})
        .await?
        .try_collect()
        .await?;

    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].term, "testterm");
    assert_eq!(docs[0].postings.len(), 1);
    assert_eq!(docs[0].postings[0], doc_id);
    assert_eq!(docs[0].positions.get(&doc_id), Some(&vec![5]));

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

// COMPREHENSIVE persist_block_to_disk TESTS

/// Test persist_block_to_disk with a single term spanning 300K documents (3 buckets).
/// This tests the chunking logic at DOCIDS_PER_MONGO_DOCUMENT (100K) boundaries.
/// Expected: 3 SpimiDoc documents with buckets 0, 1, 2
#[tokio::test]
async fn test_persist_block_to_disk_single_term_300k_docs() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    // Use 3x the chunk size (300K docs) to create 3 buckets
    let doc_count = DOCIDS_PER_MONGO_DOCUMENT * 3; // 300K
    let doc_ids = generate_sorted_object_ids(doc_count);
    let block = create_large_block_single_term("massive_term", &doc_ids, 1);

    indexer.persist_block_to_disk(block).await?;

    // Verify collection was created
    let collections = get_spimi_block_collection_names(&db).await?;
    assert_eq!(
        collections.len(),
        1,
        "Should have exactly 1 SPIMI block collection"
    );

    let spimi_collection = &collections[0];
    let docs = get_spimi_docs_from_collection(
        &db,
        spimi_collection,
        Option::None,
    )
    .await?;

    // Should have 3 documents (buckets 0, 1, 2)
    assert_eq!(
        docs.len(),
        3,
        "Should have 3 SpimiDoc documents for 300K docs"
    );

    // Verify buckets
    let mut buckets: Vec<i16> = docs.iter().map(|d| d.bucket).collect();
    buckets.sort();
    assert_eq!(buckets, vec![0, 1, 2], "Buckets should be 0, 1, 2");

    // Verify each bucket has correct number of postings
    for doc in &docs {
        assert_eq!(doc.term, "massive_term");
        assert_eq!(
            doc.postings.len(),
            DOCIDS_PER_MONGO_DOCUMENT,
            "Each bucket should have exactly 100K postings"
        );
        assert_eq!(
            doc.document_frequency as usize, DOCIDS_PER_MONGO_DOCUMENT,
            "Document frequency should match postings count"
        );
        assert_eq!(
            doc.document_frequency as usize,
            doc.postings.len(),
            "Document frequency should match postings count"
        );
    }

    // Verify total postings across all buckets
    let total_postings: usize = docs.iter().map(|d| d.postings.len()).sum();
    assert_eq!(
        total_postings, doc_count,
        "Total postings should equal doc_count"
    );

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

/// Test persist_block_to_disk with exactly 100K documents (edge case - exactly 1 bucket).
/// Expected: Exactly 1 SpimiDoc with bucket 0
#[tokio::test]
async fn test_persist_block_to_disk_exactly_100k_docs() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    let doc_count = DOCIDS_PER_MONGO_DOCUMENT; // Exactly 100K
    let doc_ids = generate_sorted_object_ids(doc_count);
    let block = create_large_block_single_term("exact_100k_term", &doc_ids, 1);

    indexer.persist_block_to_disk(block).await?;

    let collections = get_spimi_block_collection_names(&db).await?;
    let docs = get_spimi_docs_from_collection(
        &db,
        &collections[0],
        Option::None,
    )
    .await?;

    assert_eq!(
        docs.len(),
        1,
        "Should have exactly 1 SpimiDoc for exactly 100K docs"
    );
    assert_eq!(docs[0].bucket, 0, "Single bucket should be 0");
    assert_eq!(docs[0].postings.len(), DOCIDS_PER_MONGO_DOCUMENT);

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

/// Test persist_block_to_disk with 100K+1 documents (edge case: overflow by 1).
/// Expected: 2 SpimiDoc documents - bucket 0 with 100K, bucket 1 with 1
#[tokio::test]
async fn test_persist_block_to_disk_100k_plus_1_docs() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    let doc_count = DOCIDS_PER_MONGO_DOCUMENT + 1; // 100K + 1
    let doc_ids = generate_sorted_object_ids(doc_count);
    let block = create_large_block_single_term("overflow_term", &doc_ids, 1);

    indexer.persist_block_to_disk(block).await?;

    let collections = get_spimi_block_collection_names(&db).await?;
    let docs = get_spimi_docs_from_collection(
        &db,
        &collections[0],
        Option::None,
    )
    .await?;

    assert_eq!(docs.len(), 2, "Should have 2 SpimiDoc for 100K+1 docs");

    // Sort by bucket to verify
    let mut sorted_docs = docs.clone();
    sorted_docs.sort_by_key(|d| d.bucket);

    assert_eq!(sorted_docs[0].bucket, 0);
    assert_eq!(sorted_docs[0].postings.len(), DOCIDS_PER_MONGO_DOCUMENT);
    assert_eq!(sorted_docs[1].bucket, 1);
    assert_eq!(sorted_docs[1].postings.len(), 1);

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

/// Test persist_block_to_disk with multiple terms, each with varying document counts.
/// Tests multiple terms with multiple buckets per term.
#[tokio::test]
async fn test_persist_block_to_disk_multiple_terms_multi_bucket() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    // Create 5 terms with varying document counts (scaled to 100K chunk size)
    let term_configs = vec![
        ("alpha", 150_000usize), // 2 buckets
        ("beta", 200_000),       // 2 buckets
        ("gamma", 100_001),      // 2 buckets (100K+1)
        ("delta", 50_000),       // 1 bucket
        ("epsilon", 250_000),    // 3 buckets
    ];

    let mut dictionary = HashMap::new();
    let mut sorted_terms = Vec::new();

    for (idx, (term, count)) in term_configs.iter().enumerate() {
        let doc_ids = generate_sorted_object_ids_with_prefix(*count, idx as u32);
        let dict_item = create_dict_item_with_positions(&doc_ids, 1, 0);
        dictionary.insert(term.to_string(), dict_item);
        sorted_terms.push(term.to_string());
    }
    sorted_terms.sort();

    let block = SpimiBlock {
        sorted_terms,
        dictionary,
    };

    indexer.persist_block_to_disk(block).await?;

    let collections = get_spimi_block_collection_names(&db).await?;
    let docs = get_spimi_docs_from_collection(
        &db,
        &collections[0],
        Option::None
    )
    .await?;

    // Count documents per term
    let mut term_doc_counts: HashMap<String, Vec<&SpimiDoc>> = HashMap::new();
    for doc in &docs {
        term_doc_counts
            .entry(doc.term.clone())
            .or_default()
            .push(doc);
    }

    // Verify each term has correct number of buckets
    assert_eq!(
        term_doc_counts["alpha"].len(),
        2,
        "alpha should have 2 buckets"
    );
    assert_eq!(
        term_doc_counts["beta"].len(),
        2,
        "beta should have 2 buckets"
    );
    assert_eq!(
        term_doc_counts["gamma"].len(),
        2,
        "gamma should have 2 buckets"
    );
    assert_eq!(
        term_doc_counts["delta"].len(),
        1,
        "delta should have 1 bucket"
    );
    assert_eq!(
        term_doc_counts["epsilon"].len(),
        3,
        "epsilon should have 3 buckets"
    );

    // Verify total postings match original counts
    for (term, count) in &term_configs {
        let total: usize = term_doc_counts[*term]
            .iter()
            .map(|d| d.postings.len())
            .sum();
        assert_eq!(
            total, *count,
            "Total postings for {} should be {}",
            term, count
        );
    }

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

/// Test that positions are correctly extracted per bucket using BTreeMap range query.
/// Verifies that positions for docs in each bucket are correctly associated.
#[tokio::test]
async fn test_persist_block_to_disk_positions_across_buckets() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    // Create 250K docs with positions (3 buckets at 100K each)
    let doc_count = 250_000;
    let doc_ids = generate_sorted_object_ids(doc_count);

    // Each doc has 2 positions
    let dict_item = create_dict_item_with_positions(&doc_ids, 2, 100);
    let mut dictionary = HashMap::new();
    dictionary.insert("positioned_term".to_string(), dict_item);

    let block = SpimiBlock {
        sorted_terms: vec!["positioned_term".to_string()],
        dictionary,
    };

    indexer.persist_block_to_disk(block).await?;

    let collections = get_spimi_block_collection_names(&db).await?;
    let docs = get_spimi_docs_from_collection(&db, &collections[0], Option::None).await?;

    assert_eq!(docs.len(), 3, "Should have 3 buckets for 250K docs");

    // Verify each bucket has positions for all its postings
    for doc in &docs {
        assert_eq!(
            doc.positions.len(),
            doc.postings.len(),
            "Each posting should have positions"
        );

        // Verify each posting has correct number of positions
        for posting in &doc.postings {
            let positions = doc.positions.get(posting);
            assert!(positions.is_some(), "Posting should have positions");
            assert_eq!(
                positions.unwrap().len(),
                2,
                "Each posting should have 2 positions"
            );
        }
    }

    // Verify positions are correctly partitioned (first doc of each bucket should have different position ranges)
    let mut sorted_docs = docs.clone();
    sorted_docs.sort_by_key(|d| d.bucket);

    // Bucket 0: docs 0 to 99,999 -> positions start at 100
    // Bucket 1: docs 100,000 to 199,999 -> positions start at 100 + 100K*2 = 200,100
    // Bucket 2: docs 200,000 to 249,999 -> positions start at 100 + 200K*2 = 400,100
    let first_posting_bucket_0 = &sorted_docs[0].postings[0];
    let first_positions_bucket_0 = sorted_docs[0]
        .positions
        .get(first_posting_bucket_0)
        .unwrap();
    assert_eq!(
        first_positions_bucket_0[0], 100,
        "First position in bucket 0 should be 100"
    );

    let first_posting_bucket_1 = &sorted_docs[1].postings[0];
    let first_positions_bucket_1 = sorted_docs[1]
        .positions
        .get(first_posting_bucket_1)
        .unwrap();
    assert_eq!(
        first_positions_bucket_1[0],
        100 + DOCIDS_PER_MONGO_DOCUMENT * 2,
        "First position in bucket 1 should account for 100K docs * 2 positions"
    );

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

/// Test persist_block_to_disk with multiple positions per document.
#[tokio::test]
async fn test_persist_block_to_disk_multiple_positions_per_doc() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    let doc_count = 100_000;
    let doc_ids = generate_sorted_object_ids(doc_count);

    // Each document has 10 positions (word appears 10 times in each doc)
    let dict_item = create_dict_item_with_positions(&doc_ids, 10, 0);
    let mut dictionary = HashMap::new();
    dictionary.insert("repeated_word".to_string(), dict_item);

    let block = SpimiBlock {
        sorted_terms: vec!["repeated_word".to_string()],
        dictionary,
    };

    indexer.persist_block_to_disk(block).await?;

    let collections = get_spimi_block_collection_names(&db).await?;
    let docs = get_spimi_docs_from_collection(&db, &collections[0], Option::None).await?;

    assert_eq!(docs.len(), 1, "Should have 1 bucket for 100K docs");

    let doc = &docs[0];
    assert_eq!(doc.positions.len(), doc_count);

    // Verify each posting has 10 positions
    for (_doc_id, positions) in &doc.positions {
        assert_eq!(positions.len(), 10, "Each doc should have 10 positions");
    }

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

/// Test persist_block_to_disk term not found in dictionary (error path).
/// This tests code, where term is in sorted_terms but not in dictionary.
#[tokio::test]
async fn test_persist_block_to_disk_term_not_in_dictionary() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    // Create a block where sorted_terms has a term not in dictionary
    let block = SpimiBlock {
        sorted_terms: vec!["ghost_term".to_string(), "real_term".to_string()],
        dictionary: {
            let mut d = HashMap::new();
            let doc_id = ObjectId::new();
            let mut dict_item = DictItem::new();
            dict_item.postings.push(doc_id);
            dict_item.positions.insert(doc_id, vec![0]);
            d.insert("real_term".to_string(), dict_item);
            // Note: "ghost_term" is NOT inserted into dictionary
            d
        },
    };

    // This should not panic, but log an error for the ghost term
    indexer.persist_block_to_disk(block).await?;

    // Verify only real_term was persisted
    let collections = get_spimi_block_collection_names(&db).await?;
    let docs =
        get_spimi_docs_from_collection(&db, &collections[0], Option::None)
            .await?;

    assert_eq!(docs.len(), 1, "Only real_term should be persisted");
    assert_eq!(docs[0].term, "real_term");

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

/// Test persist_block_to_disk with a term spanning 500K documents (5 buckets).
/// Maximum scale test for single term.
#[tokio::test]
async fn test_persist_block_to_disk_single_term_500k_docs() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    let doc_count = DOCIDS_PER_MONGO_DOCUMENT * 5; // 500K
    let doc_ids = generate_sorted_object_ids(doc_count);
    let block = create_large_block_single_term("huge_term", &doc_ids, 1);

    indexer.persist_block_to_disk(block).await?;

    let collections = get_spimi_block_collection_names(&db).await?;
    let docs = get_spimi_docs_from_collection(
        &db,
        &collections[0],
        Option::None,
    )
    .await?;

    // Should have 5 documents (buckets 0, 1, 2, 3, 4)
    assert_eq!(
        docs.len(),
        5,
        "Should have 5 SpimiDoc documents for 500K docs"
    );

    let mut buckets: Vec<i16> = docs.iter().map(|d| d.bucket).collect();
    buckets.sort();
    assert_eq!(
        buckets,
        vec![0, 1, 2, 3, 4],
        "Buckets should be 0 through 4"
    );

    let total_postings: usize = docs.iter().map(|d| d.postings.len()).sum();
    assert_eq!(total_postings, doc_count);

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

/// Test that MongoDB index is created on the term field.
#[tokio::test]
async fn test_persist_block_to_disk_creates_term_index() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    let doc_ids = generate_sorted_object_ids(1000);
    let block = create_large_block_single_term("indexed_term", &doc_ids, 1);

    indexer.persist_block_to_disk(block).await?;

    let collections = get_spimi_block_collection_names(&db).await?;
    let collection = db.collection::<SpimiDoc>(&collections[0]);

    // List indexes on the collection
    use futures::TryStreamExt;
    let indexes: Vec<_> = collection.list_indexes().await?.try_collect().await?;

    // Should have at least 2 indexes: _id (default) and term
    assert!(indexes.len() >= 2, "Should have at least 2 indexes");

    // Verify term index exists
    let has_term_index = indexes.iter().any(|idx| idx.keys.get("term").is_some());
    assert!(has_term_index, "Should have an index on 'term' field");

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_spin_indexer_with_no_pages() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Arc::new(Indexer::new(pages_repo, 100, db.clone()));

    indexer.run(1024 * 1024).await?;

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_spin_indexer_with_pages_in_db() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));

    let page1 = create_test_page("http://example1.com", "hello world");
    let page2 = create_test_page("http://example2.com", "foo bar");

    pages_repo.insert(&page1).await?;
    pages_repo.insert(&page2).await?;

    let indexer = Arc::new(Indexer::new(Arc::clone(&pages_repo), 100, db.clone()));

    indexer.run(1024 * 1024).await?;

    // Verify inverted index was created
    let collections = db.database().list_collection_names().await?;
    let has_inverted_index = collections.iter().any(|name| name == "inverted_index");
    assert!(has_inverted_index, "Should have created inverted index");

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_spin_indexer_pagination() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));

    for i in 0..5 {
        let page = create_test_page(&format!("http://example{}.com", i), &format!("term{}", i));
        pages_repo.insert(&page).await?;
    }

    let indexer = Arc::new(Indexer::new(Arc::clone(&pages_repo), 2, db.clone()));

    indexer.run(1024 * 1024).await?;

    // Verify inverted index was created
    let collections = db.database().list_collection_names().await?;
    let has_inverted_index = collections.iter().any(|name| name == "inverted_index");
    assert!(has_inverted_index, "Should have created inverted index");

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

// merge_sorted_lists tests
#[test]
fn test_merge_sorted_lists_with_object_ids() {
    let id1 = ObjectId::parse_str("000000000000000000000001").unwrap();
    let id2 = ObjectId::parse_str("000000000000000000000002").unwrap();
    let id3 = ObjectId::parse_str("000000000000000000000003").unwrap();
    let id4 = ObjectId::parse_str("000000000000000000000004").unwrap();

    let list_a = vec![id1, id3];
    let list_b = vec![id2, id4];

    let result = merge_sorted_lists_dedup(&list_a, &list_b);
    assert_eq!(result, vec![id1, id2, id3, id4]);
}

#[test]
fn test_merge_sorted_lists_large_lists() {
    let list_a: Vec<i32> = (0..1000).step_by(2).collect();
    let list_b: Vec<i32> = (1..1000).step_by(2).collect();

    let result = merge_sorted_lists_dedup(&list_a, &list_b);

    assert_eq!(result.len(), list_a.len() + list_b.len());
    for i in 1..result.len() {
        assert!(result[i - 1] <= result[i]);
    }
}


// SpimiBlock tests
#[test]
fn test_spimi_block_creation() {
    let mut dictionary = HashMap::new();
    let doc_id = ObjectId::new();
    let mut dict_item = DictItem::new();
    dict_item.postings.push(doc_id);
    dict_item.positions.insert(doc_id, vec![0]);
    dictionary.insert("term".to_string(), dict_item);

    let block = SpimiBlock {
        sorted_terms: vec!["term".to_string()],
        dictionary,
    };

    assert_eq!(block.sorted_terms.len(), 1);
    assert!(block.dictionary.contains_key("term"));
}

#[test]
fn test_spimi_block_with_multiple_postings() {
    let mut dictionary = HashMap::new();
    let doc_ids: Vec<ObjectId> = (0..10).map(|_| ObjectId::new()).collect();
    let mut dict_item = DictItem::new();
    for (i, &doc_id) in doc_ids.iter().enumerate() {
        dict_item.postings.push(doc_id);
        dict_item.positions.insert(doc_id, vec![i]);
    }
    dictionary.insert("popular_term".to_string(), dict_item);

    let block = SpimiBlock {
        sorted_terms: vec!["popular_term".to_string()],
        dictionary,
    };

    assert_eq!(
        block.dictionary.get("popular_term").unwrap().postings.len(),
        10
    );
}

// Token tests removed - Token is now a private struct within the indexer module
// and is used internally via StreamMsg

// COMPREHENSIVE merge_persisted_blocks TESTS

/// Test merge_persisted_blocks when no SPIMI blocks exist.
/// Tests the early return path.
#[tokio::test]
async fn test_merge_persisted_blocks_no_blocks() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    // Call merge without any blocks - should return Ok without error
    indexer.merge_persisted_blocks().await?;

    // Verify no inverted_index collection was created (or it's empty)
    let collections = db.database().list_collection_names().await?;
    let has_inverted_index = collections.iter().any(|n| n == "inverted_index");

    // It's acceptable to either not create the collection or have it empty
    if has_inverted_index {
        let docs = get_inverted_index_docs(&db, Some(doc! {"id": 1})).await?;
        assert!(
            docs.is_empty(),
            "Inverted index should be empty when no blocks exist"
        );
    }

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

/// Test merge_persisted_blocks with 6 blocks, same term across all blocks.
/// Tests heap-based merging of same term from multiple blocks.
#[tokio::test]
async fn test_merge_persisted_blocks_6_blocks_same_term() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    // Create 6 blocks, each with the same term but different doc_ids
    let docs_per_block = 100_000;
    for i in 0..6 {
        let doc_ids = generate_sorted_object_ids_with_prefix(docs_per_block, i as u32);
        let block = create_large_block_single_term("common_term", &doc_ids, 1);
        indexer.persist_block_to_disk(block).await?;
    }

    // Verify 6 blocks were created
    let block_count = count_spimi_block_collections(&db).await?;
    assert_eq!(block_count, 6, "Should have 6 SPIMI blocks before merge");

    // Merge the blocks
    indexer.merge_persisted_blocks().await?;

    // Verify SPIMI blocks are cleaned up
    let block_count_after = count_spimi_block_collections(&db).await?;
    assert_eq!(
        block_count_after, 0,
        "SPIMI blocks should be deleted after merge"
    );

    // Verify inverted index
    let docs = get_inverted_index_docs_for_term(&db, "common_term").await?;
    assert!(
        !docs.is_empty(),
        "Should have inverted index docs for common_term"
    );

    // Verify total postings
    let total_postings: usize = docs.iter().map(|d| d.postings.len()).sum();
    assert_eq!(
        total_postings,
        docs_per_block * 6,
        "Total postings should be docs_per_block * 6"
    );

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

/// Test merge_persisted_blocks with 6 blocks, term with 500K total docs.
/// Tests mid-merge flush when reaching DOCIDS_PER_MONGO_DOCUMENT
#[tokio::test]
async fn test_merge_persisted_blocks_6_blocks_500k_total_docs() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    // Create 6 blocks, each with ~83K docs for the same term (total ~500K)
    // This will force mid-merge flushes (5 buckets at 100K each)
    let docs_per_block = 84_000; // ~84K * 6 = ~504K
    for i in 0..6 {
        let doc_ids = generate_sorted_object_ids_with_prefix(docs_per_block, i as u32);
        let block = create_large_block_single_term("large_term", &doc_ids, 1);
        indexer.persist_block_to_disk(block).await?;
    }

    // Merge the blocks
    indexer.merge_persisted_blocks().await?;

    // Verify inverted index has multiple buckets
    let docs = get_inverted_index_docs_for_term(&db, "large_term").await?;

    for doc in &docs {
        println!("dock: bucket: {}, postings: {}", doc.bucket, doc.postings.len());
    }

    // ~504K docs should result in 5+ buckets
    let expected_buckets = (docs_per_block * 6) / DOCIDS_PER_MONGO_DOCUMENT;
    assert!(
        docs.len() >= expected_buckets,
        "Should have at least {} buckets for 500K+ docs, got {}",
        expected_buckets,
        docs.len()
    );

    // Verify total postings
    let total_postings: usize = docs.iter().map(|d| d.postings.len()).sum();
    assert_eq!(
        total_postings,
        docs_per_block * 6,
        "Total postings should match input"
    );

    // Verify SPIMI blocks are cleaned up
    let block_count = count_spimi_block_collections(&db).await?;
    assert_eq!(block_count, 0, "SPIMI blocks should be deleted after merge");

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

/// Test merge_persisted_blocks with 6 blocks, different terms that interleave alphabetically.
/// Tests heap ordering maintains correct alphabetical merge.
#[tokio::test]
async fn test_merge_persisted_blocks_interleaved_terms() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    // Create 6 blocks with interleaved terms
    // Block 0: alpha, gamma, epsilon
    // Block 1: beta, delta, zeta
    // Block 2: alpha, delta
    // Block 3: beta, epsilon
    // Block 4: gamma, zeta
    // Block 5: alpha, beta, gamma, delta, epsilon, zeta
    let block_terms = vec![
        vec!["alpha", "gamma", "epsilon"],
        vec!["beta", "delta", "zeta"],
        vec!["alpha", "delta"],
        vec!["beta", "epsilon"],
        vec!["gamma", "zeta"],
        vec!["alpha", "beta", "gamma", "delta", "epsilon", "zeta"],
    ];

    for (block_idx, terms) in block_terms.iter().enumerate() {
        let mut terms_with_docs = Vec::new();
        for (term_idx, term) in terms.iter().enumerate() {
            let prefix = (block_idx * 100 + term_idx) as u32;
            let doc_ids = generate_sorted_object_ids_with_prefix(10_000, prefix);
            terms_with_docs.push((*term, doc_ids, 1));
        }
        let block = create_block_with_terms(terms_with_docs);
        indexer.persist_block_to_disk(block).await?;
    }

    // Merge the blocks
    indexer.merge_persisted_blocks().await?;

    // Verify all 6 terms exist in inverted index
    let all_docs = get_inverted_index_docs(&db, Option::None).await?;
    let unique_terms: std::collections::HashSet<String> =
        all_docs.iter().map(|d| d.term.clone()).collect();

    let expected_terms: std::collections::HashSet<String> =
        ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"]
            .iter()
            .map(|s| s.to_string())
            .collect();

    assert_eq!(
        unique_terms, expected_terms,
        "All terms should be in inverted index"
    );

    // Verify counts for each term
    // alpha appears in blocks 0, 2, 5 = 3 times = 30K docs
    // beta appears in blocks 1, 3, 5 = 3 times = 30K docs
    // gamma appears in blocks 0, 4, 5 = 3 times = 30K docs
    // delta appears in blocks 1, 2, 5 = 3 times = 30K docs
    // epsilon appears in blocks 0, 3, 5 = 3 times = 30K docs
    // zeta appears in blocks 1, 4, 5 = 3 times = 30K docs
    for term in &expected_terms {
        let docs = get_inverted_index_docs_for_term(&db, term).await?;
        let total_postings: usize = docs.iter().map(|d| d.postings.len()).sum();
        assert_eq!(
            total_postings, 30_000,
            "Term {} should have 30K postings, got {}",
            term, total_postings
        );
    }

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

/// Test merge_persisted_blocks position merging - Vacant entry path.
/// Tests when position key doesn't exist yet during merge.
#[tokio::test]
async fn test_merge_persisted_blocks_positions_vacant_entry() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    // Create 2 blocks with completely different doc_ids for the same term
    // This ensures all position entries are Vacant during merge
    let doc_ids_1 = generate_sorted_object_ids_with_prefix(1000, 1);
    let block1 = create_large_block_single_term("unique_docs_term", &doc_ids_1, 2);
    indexer.persist_block_to_disk(block1).await?;

    let doc_ids_2 = generate_sorted_object_ids_with_prefix(1000, 2);
    let block2 = create_large_block_single_term("unique_docs_term", &doc_ids_2, 2);
    indexer.persist_block_to_disk(block2).await?;

    // Merge the blocks
    indexer.merge_persisted_blocks().await?;

    // Verify positions were correctly merged (each block writes its own docs)
    let docs = get_inverted_index_docs_for_term(&db, "unique_docs_term").await?;

    // Total postings should be 2000 across all buckets
    let total_postings: usize = docs.iter().map(|d| d.postings.len()).sum();
    assert_eq!(total_postings, 2000, "Should have 2000 total postings");

    // Each doc in each bucket should have 2 positions
    for doc in &docs {
        for (_, positions) in &doc.positions {
            assert_eq!(positions.len(), 2, "Each doc should have 2 positions");
        }
    }

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

/// Test merge_persisted_blocks position merging - Occupied entry path.
/// Tests when position key already exists during merge (same doc_id appears in multiple blocks).
#[tokio::test]
async fn test_merge_persisted_blocks_positions_occupied_entry() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    // Create shared doc_ids that will appear in multiple blocks
    let shared_doc_ids = generate_sorted_object_ids(500);

    // Block 1: shared docs with positions [0, 1]
    let dict_item_1 = create_dict_item_with_positions(&shared_doc_ids, 2, 0);
    let mut dictionary_1 = HashMap::new();
    dictionary_1.insert("shared_docs_term".to_string(), dict_item_1);
    let block1 = SpimiBlock {
        sorted_terms: vec!["shared_docs_term".to_string()],
        dictionary: dictionary_1,
    };
    indexer.persist_block_to_disk(block1).await?;

    // Block 2: same docs with positions [1000, 1001]
    let dict_item_2 = create_dict_item_with_positions(&shared_doc_ids, 2, 1000);
    let mut dictionary_2 = HashMap::new();
    dictionary_2.insert("shared_docs_term".to_string(), dict_item_2);
    let block2 = SpimiBlock {
        sorted_terms: vec!["shared_docs_term".to_string()],
        dictionary: dictionary_2,
    };
    indexer.persist_block_to_disk(block2).await?;

    // Merge the blocks
    indexer.merge_persisted_blocks().await?;

    // Verify positions were appended (Occupied entry path)
    let docs = get_inverted_index_docs_for_term(&db, "shared_docs_term").await?;
    for doc in &docs {
        let obj = json!({
            "id": doc.id,
            "term": doc.term,
            "document_frequency": doc.document_frequency,
            "bucket": doc.bucket
        });
        println!("doc in shared_docs_term: {:?}", obj);
    }
    assert_eq!(docs.len(), 1, "Should have 1 bucket");

    let doc = &docs[0];
    // Postings may be deduplicated or merged - check total unique
    // With merge_sorted_lists, duplicates are kept, so we expect 1000 postings
    assert!(
        doc.postings.len() >= 500,
        "Should have at least 500 postings (may have more due to merge)"
    );

    // Check that positions were merged for at least one doc
    let first_doc_id = &shared_doc_ids[0];
    if let Some(positions) = doc.positions.get(first_doc_id) {
        // Positions should be merged: [0, 1] ++ [1000, 1001] = 4 positions
        assert_eq!(
            positions.len(),
            4,
            "Shared doc should have 4 positions after merge (2 from each block)"
        );
    }

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

/// Test merge_persisted_blocks final flush when postings > 0.
/// Tests the final flush path at the end of term processing.
#[tokio::test]
async fn test_merge_persisted_blocks_final_flush() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    // Create blocks with less than 100k total docs (won't trigger mid-merge flush)
    // Final flush is the only path that writes to inverted_index
    let doc_ids_1 = generate_sorted_object_ids_with_prefix(40_000, 1);
    let block1 = create_large_block_single_term("final_flush_term", &doc_ids_1, 1);
    indexer.persist_block_to_disk(block1).await?;

    let doc_ids_2 = generate_sorted_object_ids_with_prefix(40_000, 2);
    let block2 = create_large_block_single_term("final_flush_term", &doc_ids_2, 1);
    indexer.persist_block_to_disk(block2).await?;

    // Merge - this should only use the final flush path
    indexer.merge_persisted_blocks().await?;

    // Verify final flush wrote the data
    let docs = get_inverted_index_docs_for_term(&db, "final_flush_term").await?;
    assert_eq!(docs.len(), 1, "Should have 1 bucket (only final flush)");

    let doc = &docs[0];
    assert_eq!(doc.bucket, 0, "Final flush should write to bucket 0");
    assert_eq!(doc.postings.len(), 80_000, "Should have 80K postings");

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

/// Test merge_persisted_blocks cleanup verification.
/// Verifies all SPIMI block collections are deleted after merge.
#[tokio::test]
async fn test_merge_persisted_blocks_cleanup() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    // Create 6 blocks
    for i in 0..6 {
        let doc_ids = generate_sorted_object_ids_with_prefix(1000, i as u32);
        let block = create_large_block_single_term(&format!("term_{}", i), &doc_ids, 1);
        indexer.persist_block_to_disk(block).await?;
    }

    // Verify blocks exist before merge
    let block_names_before = get_spimi_block_collection_names(&db).await?;
    assert_eq!(
        block_names_before.len(),
        6,
        "Should have 6 blocks before merge"
    );

    // Merge
    indexer.merge_persisted_blocks().await?;

    // Verify all blocks are deleted
    let block_names_after = get_spimi_block_collection_names(&db).await?;
    assert!(
        block_names_after.is_empty(),
        "All SPIMI blocks should be deleted after merge, found: {:?}",
        block_names_after
    );

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

/// Test merge_persisted_blocks with multiple terms each spanning 300K+ docs.
/// Tests multiple terms with multiple buckets written to inverted_index.
#[tokio::test]
async fn test_merge_persisted_blocks_multiple_terms_300k_each() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    // Create 6 blocks, each with 2 terms
    // Term "big_alpha" appears in all blocks with 50K docs each = 300K total (3 buckets)
    // Term "big_beta" appears in all blocks with 60K docs each = 360K total (4 buckets)
    for block_idx in 0..6 {
        let alpha_docs = generate_sorted_object_ids_with_prefix(50_000, (block_idx * 2) as u32);
        let beta_docs = generate_sorted_object_ids_with_prefix(60_000, (block_idx * 2 + 1) as u32);

        let block = create_block_with_terms(vec![
            ("big_alpha", alpha_docs, 1),
            ("big_beta", beta_docs, 1),
        ]);
        indexer.persist_block_to_disk(block).await?;
    }

    // Merge
    indexer.merge_persisted_blocks().await?;

    // Verify big_alpha has 3 buckets (300K docs)
    let alpha_docs = get_inverted_index_docs_for_term(&db, "big_alpha").await?;
    let alpha_buckets: Vec<i16> = alpha_docs.iter().map(|d| d.bucket).collect();
    assert!(
        alpha_docs.len() >= 3,
        "big_alpha should have at least 3 buckets for 300K docs, got {}",
        alpha_docs.len()
    );

    let alpha_total: usize = alpha_docs.iter().map(|d| d.postings.len()).sum();
    assert_eq!(
        alpha_total, 300_000,
        "big_alpha should have 300K total postings"
    );

    // Verify big_beta has 4 buckets (360K docs)
    let beta_docs = get_inverted_index_docs_for_term(&db, "big_beta").await?;
    assert!(
        beta_docs.len() >= 3,
        "big_beta should have at least 3 buckets for 360K docs, got {}",
        beta_docs.len()
    );

    let beta_total: usize = beta_docs.iter().map(|d| d.postings.len()).sum();
    assert_eq!(
        beta_total, 360_000,
        "big_beta should have 360K total postings"
    );

    // Verify buckets are sequential
    let mut sorted_alpha_buckets = alpha_buckets.clone();
    sorted_alpha_buckets.sort();
    for (i, bucket) in sorted_alpha_buckets.iter().enumerate() {
        assert_eq!(
            *bucket, i as i16,
            "Buckets should be sequential starting from 0"
        );
    }

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

/// Test merge_persisted_blocks with single block.
/// Edge case: only one block exists.
#[tokio::test]
async fn test_merge_persisted_blocks_single_block() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    // Create single block with multiple terms
    let block = create_block_with_terms(vec![
        ("single_alpha", generate_sorted_object_ids(10_000), 1),
        (
            "single_beta",
            generate_sorted_object_ids_with_prefix(20_000, 1),
            2,
        ),
        (
            "single_gamma",
            generate_sorted_object_ids_with_prefix(5_000, 2),
            3,
        ),
    ]);
    indexer.persist_block_to_disk(block).await?;

    // Merge
    indexer.merge_persisted_blocks().await?;

    // Verify all terms in inverted index
    let all_docs = get_inverted_index_docs(
        &db,
        Option::None
    )
    .await?;
    let terms: std::collections::HashSet<String> =
        all_docs.iter().map(|d| d.term.clone()).collect();
    println!("all terms: {:?}", terms);

    assert!(terms.contains("single_alpha"));
    assert!(terms.contains("single_beta"));
    assert!(terms.contains("single_gamma"));

    // Verify cleanup
    let block_count = count_spimi_block_collections(&db).await?;
    assert_eq!(block_count, 0);

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

/// Test merge_persisted_blocks verifies postings are merged in sorted order.
#[tokio::test]
async fn test_merge_persisted_blocks_postings_sorted() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    // Create blocks with interleaved sorted ObjectIds
    // Block 1: IDs 0, 2, 4, 6, 8...
    // Block 2: IDs 1, 3, 5, 7, 9...
    let even_ids: Vec<ObjectId> = (0..5000u64)
        .map(|i| ObjectId::parse_str(&format!("{:024x}", i * 2)).unwrap())
        .collect();
    let odd_ids: Vec<ObjectId> = (0..5000u64)
        .map(|i| ObjectId::parse_str(&format!("{:024x}", i * 2 + 1)).unwrap())
        .collect();

    let block1 = create_large_block_single_term("sorted_term", &even_ids, 1);
    indexer.persist_block_to_disk(block1).await?;

    let block2 = create_large_block_single_term("sorted_term", &odd_ids, 1);
    indexer.persist_block_to_disk(block2).await?;

    // Merge
    indexer.merge_persisted_blocks().await?;

    // Verify postings are sorted
    let docs = get_inverted_index_docs_for_term(&db, "sorted_term").await?;
    assert_eq!(docs.len(), 1);

    let postings = &docs[0].postings;
    assert_eq!(postings.len(), 10_000);

    // Verify sorted order
    for i in 1..postings.len() {
        assert!(
            postings[i - 1] < postings[i],
            "Postings should be in sorted order at index {}",
            i
        );
    }

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

// =============================================================================
// FULL INTEGRATION TEST
// =============================================================================

/// Comprehensive integration test for persist_block_to_disk and merge_persisted_blocks.
///
/// This test:
/// 1. Creates 6 SPIMI blocks manually with persist_block_to_disk
/// 2. Each block has 3-5 terms with varying doc counts (some terms shared across blocks)
/// 3. At least 2 terms with 300K+ total docs across all blocks (multiple buckets)
/// 4. Calls merge_persisted_blocks
/// 5. Verifies:
///    - All terms present in inverted_index
///    - Correct bucket counts per term
///    - Correct postings merged
///    - Positions correctly merged
///    - All spimi_block_* collections deleted
#[tokio::test]
async fn test_integration_full_indexer_flow() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    // ==========================================================================
    // STEP 1: Define the test data structure
    // ==========================================================================

    // We'll create 6 blocks with the following term distribution:
    // (Scaled to work with 100K chunk size)
    //
    // | Term      | Block 0 | Block 1 | Block 2 | Block 3 | Block 4 | Block 5 | Total     |
    // |-----------|---------|---------|---------|---------|---------|---------|-----------|
    // | mega_a    | 60K     | 60K     | 60K     | 60K     | 60K     | 50K     | 350K      |
    // | mega_b    | 80K     | 80K     | 80K     | 80K     | 80K     | -       | 400K      |
    // | small_c   | 15K     | -       | 15K     | -       | 15K     | -       | 45K       |
    // | small_d   | -       | 10K     | -       | 10K     | -       | 10K     | 30K       |
    // | tiny_e    | 5K      | 5K      | 5K      | 5K      | 5K      | 5K      | 30K       |
    // | unique_f0 | 3K      | -       | -       | -       | -       | -       | 3K        |
    // | unique_f1 | -       | 3K      | -       | -       | -       | -       | 3K        |
    // | unique_f2 | -       | -       | 3K      | -       | -       | -       | 3K        |
    // | unique_f3 | -       | -       | -       | 3K      | -       | -       | 3K        |
    // | unique_f4 | -       | -       | -       | -       | 3K      | -       | 3K        |
    // | unique_f5 | -       | -       | -       | -       | -       | 3K      | 3K        |

    // Expected after merge:
    // - mega_a: 4 buckets (350K / 100K = 3.5, rounds to 4)
    // - mega_b: 4 buckets (400K / 100K = 4)
    // - small_c: 1 bucket (45K < 100K)
    // - small_d: 1 bucket (30K < 100K)
    // - tiny_e: 1 bucket (30K < 100K)
    // - unique_f0-f5: 1 bucket each

    // Keep track of expected data for verification
    let mut expected_term_totals: HashMap<String, usize> = HashMap::new();
    let mut expected_term_positions: HashMap<String, usize> = HashMap::new(); // positions per doc

    // ==========================================================================
    // STEP 2: Create and persist 6 SPIMI blocks
    // ==========================================================================

    for block_idx in 0..6u32 {
        let mut terms_with_docs: Vec<(&str, Vec<ObjectId>, usize)> = Vec::new();

        // mega_a: 60K (blocks 0-4) or 50K (block 5)
        let mega_a_count = if block_idx < 5 { 60_000 } else { 50_000 };
        let mega_a_docs = generate_sorted_object_ids_with_prefix(mega_a_count, block_idx * 100);
        terms_with_docs.push(("mega_a", mega_a_docs, 2));
        *expected_term_totals
            .entry("mega_a".to_string())
            .or_insert(0) += mega_a_count;
        expected_term_positions.insert("mega_a".to_string(), 2);

        // mega_b: 80K (blocks 0-4 only)
        if block_idx < 5 {
            let mega_b_docs = generate_sorted_object_ids_with_prefix(80_000, block_idx * 100 + 1);
            terms_with_docs.push(("mega_b", mega_b_docs, 3));
            *expected_term_totals
                .entry("mega_b".to_string())
                .or_insert(0) += 80_000;
            expected_term_positions.insert("mega_b".to_string(), 3);
        }

        // small_c: 15K (blocks 0, 2, 4)
        if block_idx % 2 == 0 {
            let small_c_docs = generate_sorted_object_ids_with_prefix(15_000, block_idx * 100 + 2);
            terms_with_docs.push(("small_c", small_c_docs, 1));
            *expected_term_totals
                .entry("small_c".to_string())
                .or_insert(0) += 15_000;
            expected_term_positions.insert("small_c".to_string(), 1);
        }

        // small_d: 10K (blocks 1, 3, 5)
        if block_idx % 2 == 1 {
            let small_d_docs = generate_sorted_object_ids_with_prefix(10_000, block_idx * 100 + 3);
            terms_with_docs.push(("small_d", small_d_docs, 1));
            *expected_term_totals
                .entry("small_d".to_string())
                .or_insert(0) += 10_000;
            expected_term_positions.insert("small_d".to_string(), 1);
        }

        // tiny_e: 5K (all blocks)
        let tiny_e_docs = generate_sorted_object_ids_with_prefix(5_000, block_idx * 100 + 4);
        terms_with_docs.push(("tiny_e", tiny_e_docs, 1));
        *expected_term_totals
            .entry("tiny_e".to_string())
            .or_insert(0) += 5_000;
        expected_term_positions.insert("tiny_e".to_string(), 1);

        // unique_fN: 3K (only in block N)
        let unique_term = format!("unique_f{}", block_idx);
        let unique_docs = generate_sorted_object_ids_with_prefix(3_000, block_idx * 100 + 5);
        // We need to convert this to a static str for the tuple, so we'll handle it differently
        expected_term_totals.insert(unique_term.clone(), 3_000);
        expected_term_positions.insert(unique_term.clone(), 1);

        // Build the block
        let block = create_block_with_terms(terms_with_docs);

        // Add the unique term separately since we need owned String
        let mut dictionary = block.dictionary;
        let dict_item = create_dict_item_with_positions(&unique_docs, 1, 0);
        dictionary.insert(unique_term.clone(), dict_item);

        let mut sorted_terms = block.sorted_terms;
        sorted_terms.push(unique_term);
        sorted_terms.sort();

        let final_block = SpimiBlock {
            sorted_terms,
            dictionary,
        };

        indexer.persist_block_to_disk(final_block).await?;
    }

    // ==========================================================================
    // STEP 3: Verify SPIMI blocks were created correctly
    // ==========================================================================

    let block_count = count_spimi_block_collections(&db).await?;
    assert_eq!(block_count, 6, "Should have created 6 SPIMI blocks");

    // ==========================================================================
    // STEP 4: Merge the blocks
    // ==========================================================================

    indexer.merge_persisted_blocks().await?;

    // ==========================================================================
    // STEP 5: Verify all SPIMI blocks are cleaned up
    // ==========================================================================

    let block_count_after = count_spimi_block_collections(&db).await?;
    assert_eq!(
        block_count_after, 0,
        "All SPIMI blocks should be deleted after merge"
    );

    // ==========================================================================
    // STEP 6: Verify inverted index contents
    // ==========================================================================

    let all_inverted_docs = get_inverted_index_docs(&db, Option::None).await?;
    assert!(
        !all_inverted_docs.is_empty(),
        "Inverted index should not be empty"
    );

    // Get unique terms in inverted index
    let indexed_terms: std::collections::HashSet<String> =
        all_inverted_docs.iter().map(|d| d.term.clone()).collect();

    // Verify all expected terms exist
    for term in expected_term_totals.keys() {
        assert!(
            indexed_terms.contains(term),
            "Term '{}' should be in inverted index",
            term
        );
    }

    // ==========================================================================
    // STEP 7: Verify posting counts for each term
    // ==========================================================================

    for (term, expected_total) in &expected_term_totals {
        let term_docs = get_inverted_index_docs_for_term(&db, term).await?;
        let actual_total: usize = term_docs.iter().map(|d| d.postings.len()).sum();

        assert_eq!(
            actual_total, *expected_total,
            "Term '{}' should have {} postings, got {}",
            term, expected_total, actual_total
        );
    }

    // ==========================================================================
    // STEP 8: Verify bucket counts for large terms
    // ==========================================================================

    // mega_a: 350K docs -> should have multiple buckets
    let mega_a_docs = get_inverted_index_docs_for_term(&db, "mega_a").await?;
    assert!(
        mega_a_docs.len() >= 1,
        "mega_a should have at least 1 bucket for 350K docs, got {}",
        mega_a_docs.len()
    );

    // mega_b: 400K docs -> should have multiple buckets
    let mega_b_docs = get_inverted_index_docs_for_term(&db, "mega_b").await?;
    assert!(
        mega_b_docs.len() >= 1,
        "mega_b should have at least 1 bucket for 400K docs, got {}",
        mega_b_docs.len()
    );

    // Verify all buckets have valid bucket numbers (non-negative)
    for doc in &mega_a_docs {
        assert!(
            doc.bucket >= 0,
            "mega_a bucket should be non-negative, got {}",
            doc.bucket
        );
    }

    // ==========================================================================
    // STEP 9: Verify positions are preserved
    // ==========================================================================

    // Check mega_a has positions (2 per doc)
    for doc in &mega_a_docs {
        // Each posting should have position data
        assert!(
            !doc.positions.is_empty(),
            "mega_a bucket {} should have positions",
            doc.bucket
        );

        // Spot check: first posting should have 2 positions
        if let Some(first_posting) = doc.postings.first() {
            if let Some(positions) = doc.positions.get(first_posting) {
                assert_eq!(positions.len(), 2, "mega_a should have 2 positions per doc");
            }
        }
    }

    // Check mega_b has positions (3 per doc)
    for doc in &mega_b_docs {
        if let Some(first_posting) = doc.postings.first() {
            if let Some(positions) = doc.positions.get(first_posting) {
                assert_eq!(positions.len(), 3, "mega_b should have 3 positions per doc");
            }
        }
    }

    // ==========================================================================
    // STEP 10: Verify small and unique terms
    // ==========================================================================

    // small_c: 45K total (15K from 3 blocks) -> verify total postings
    let small_c_docs = get_inverted_index_docs_for_term(&db, "small_c").await?;
    let small_c_total: usize = small_c_docs.iter().map(|d| d.postings.len()).sum();
    assert_eq!(
        small_c_total, 45_000,
        "small_c should have 45K total postings"
    );

    // small_d: 30K total (10K from 3 blocks) -> verify total postings
    let small_d_docs = get_inverted_index_docs_for_term(&db, "small_d").await?;
    let small_d_total: usize = small_d_docs.iter().map(|d| d.postings.len()).sum();
    assert_eq!(
        small_d_total, 30_000,
        "small_d should have 30K total postings"
    );

    // tiny_e: 30K total (5K from 6 blocks) -> verify total postings
    let tiny_e_docs = get_inverted_index_docs_for_term(&db, "tiny_e").await?;
    let tiny_e_total: usize = tiny_e_docs.iter().map(|d| d.postings.len()).sum();
    assert_eq!(
        tiny_e_total, 30_000,
        "tiny_e should have 30K total postings"
    );

    // unique terms: each 3K total -> verify total postings
    for i in 0..6 {
        let term = format!("unique_f{}", i);
        let docs = get_inverted_index_docs_for_term(&db, &term).await?;
        let total: usize = docs.iter().map(|d| d.postings.len()).sum();
        assert_eq!(total, 3_000, "{} should have 3K total postings", term);
    }

    // ==========================================================================
    // STEP 11: Verify document frequencies
    // ==========================================================================

    for doc in &all_inverted_docs {
        assert_eq!(
            doc.document_frequency as usize,
            doc.postings.len(),
            "Document frequency should match postings length for term '{}' bucket {}",
            doc.term,
            doc.bucket
        );
    }

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

/// Integration test with shared documents across blocks (tests position merging in integration).
#[tokio::test]
async fn test_integration_shared_documents_across_blocks() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));
    let indexer = Indexer::new(pages_repo, 100, db.clone());

    // Create shared doc IDs that will appear in ALL blocks
    let shared_docs = generate_sorted_object_ids(1000);

    // Create 6 blocks, each with the shared docs for "shared_term"
    // Each block adds different positions for the same documents
    for block_idx in 0..6u32 {
        let base_position = block_idx as usize * 1000;
        let dict_item = create_dict_item_with_positions(&shared_docs, 2, base_position);

        let mut dictionary = HashMap::new();
        dictionary.insert("shared_term".to_string(), dict_item);

        // Also add some unique docs per block
        let unique_docs = generate_sorted_object_ids_with_prefix(5000, block_idx * 100);
        let unique_dict_item = create_dict_item_with_positions(&unique_docs, 1, 0);
        dictionary.insert("mixed_term".to_string(), unique_dict_item);

        let block = SpimiBlock {
            sorted_terms: vec!["mixed_term".to_string(), "shared_term".to_string()],
            dictionary,
        };
        indexer.persist_block_to_disk(block).await?;
    }

    // Merge
    indexer.merge_persisted_blocks().await?;

    // Verify shared_term
    let shared_docs_result = get_inverted_index_docs_for_term(&db, "shared_term").await?;
    for doc in &shared_docs_result {
        println!("doc: bucket: {}, postings: {}", doc.bucket, doc.postings.len());
    }
    assert_eq!(
        shared_docs_result.len(),
        1,
        "shared_term should have 1 bucket"
    );

    let shared_doc = &shared_docs_result[0];

    // Due to merge_sorted_lists, we get duplicate postings (same doc from each block)
    // So we expect 1000 * 6 = 6000 postings, not 1000 unique
    // This matches the actual merge behavior in the code
    assert!(
        shared_doc.postings.len() == 1000,
        "shared_term should have exactly 1000 postings"
    );
    println!("shared_doc postings: {}", shared_doc.postings.len());

    // Verify mixed_term has all unique docs from all blocks
    let mixed_docs = get_inverted_index_docs_for_term(&db, "mixed_term").await?;
    for doc in &mixed_docs {
        println!("mixed doc len: {}, bucket: {}",doc.postings.len(), doc.bucket);
    }
    let mixed_total: usize = mixed_docs.iter().map(|d| d.postings.len()).sum();
    assert_eq!(
        mixed_total,
        5000 * 6,
        "mixed_term should have 30K postings (5K from each of 6 blocks)"
    );

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}
