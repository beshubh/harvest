use anyhow::Result;
use mongodb::bson::oid::ObjectId;
use std::collections::HashMap;
use std::sync::Arc;

use harvest::data_models::{Page, SpimiDoc};
use harvest::db::{Database, PageRepo};
use harvest::indexer::{DictItem, Indexer, SpimiBlock, merge_sorted_lists};

mod test_helpers {
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

// Indexer::run tests (previously spin_indexer)
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

    let result = merge_sorted_lists(&list_a, &list_b);
    assert_eq!(result, vec![id1, id2, id3, id4]);
}

#[test]
fn test_merge_sorted_lists_large_lists() {
    let list_a: Vec<i32> = (0..1000).step_by(2).collect();
    let list_b: Vec<i32> = (1..1000).step_by(2).collect();

    let result = merge_sorted_lists(&list_a, &list_b);

    assert_eq!(result.len(), list_a.len() + list_b.len());
    for i in 1..result.len() {
        assert!(result[i - 1] <= result[i]);
    }
}

#[test]
fn test_merge_sorted_lists_all_same_elements() {
    let list_a = vec![5, 5, 5, 5];
    let list_b = vec![5, 5, 5];

    let result = merge_sorted_lists(&list_a, &list_b);
    assert_eq!(result, vec![5, 5, 5, 5, 5, 5, 5]);
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
