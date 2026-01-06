use anyhow::Result;
use futures::stream::TryStreamExt;
use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;
use std::collections::HashMap;
use std::sync::Arc;

use harvest::analyzer::TextAnalyzer;
use harvest::data_models::{InvertedIndexDoc, Page};
use harvest::db::{Database, PageRepo, collections};
use harvest::indexer::Indexer;
use harvest::query_engine::QueryEngine;

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
        format!("harvest_query_engine_test_{}_{}", timestamp, count)
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

    pub fn create_text_analyzer() -> TextAnalyzer {
        TextAnalyzer::new(
            vec![Box::new(harvest::analyzer::HTMLTagFilter::default())],
            Box::new(harvest::analyzer::WhiteSpaceTokenizer),
            vec![
                Box::new(harvest::analyzer::PunctuationStripFilter::default()),
                Box::new(harvest::analyzer::LowerCaseTokenFilter),
                Box::new(harvest::analyzer::NumericTokenFilter),
                Box::new(harvest::analyzer::StopWordTokenFilter),
                Box::new(harvest::analyzer::PorterStemmerTokenFilter),
            ],
        )
    }

    /// Helper to insert InvertedIndexDoc documents into the database
    pub async fn insert_inverted_index_docs(
        db: &Database,
        docs: Vec<InvertedIndexDoc>,
    ) -> Result<()> {
        let collection = db.collection::<InvertedIndexDoc>("inverted_index");
        collection.insert_many(docs).await?;
        Ok(())
    }

    /// Generate a vector of sorted ObjectIds for testing.
    /// Uses incrementing hex strings to ensure deterministic ordering.
    pub fn generate_sorted_object_ids(count: usize) -> Vec<ObjectId> {
        (0..count)
            .map(|i| {
                let hex = format!("{:024x}", i);
                ObjectId::parse_str(&hex).unwrap()
            })
            .collect()
    }
}

use test_helpers::*;

#[tokio::test]
async fn test_query_empty_string() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let analyzer = create_text_analyzer();
    let query_engine = QueryEngine::new(db.clone(), analyzer);

    let results = query_engine.query("").await?;
    assert!(results.is_empty(), "Empty query should return no results");

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_query_whitespace_only() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let analyzer = create_text_analyzer();
    let query_engine = QueryEngine::new(db.clone(), analyzer);

    let results = query_engine.query("   \t\n  ").await?;
    assert!(
        results.is_empty(),
        "Whitespace-only query should return no results"
    );

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_query_no_matching_terms() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let analyzer = create_text_analyzer();
    let query_engine = QueryEngine::new(db.clone(), analyzer);

    // Insert some index docs for different terms
    let doc_ids = generate_sorted_object_ids(5);
    let index_docs = vec![
        InvertedIndexDoc::new(
            "elephant".to_string(),
            0,
            3,
            vec![doc_ids[0], doc_ids[1], doc_ids[2]],
            HashMap::new(),
        ),
        InvertedIndexDoc::new(
            "giraffe".to_string(),
            0,
            2,
            vec![doc_ids[1], doc_ids[3]],
            HashMap::new(),
        ),
    ];
    insert_inverted_index_docs(&db, index_docs).await?;

    // Query for a term that doesn't exist in the index
    let results = query_engine.query("penguin").await?;
    assert!(
        results.is_empty(),
        "Query for non-existent term should return no results"
    );

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_query_single_term_single_bucket() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let analyzer = create_text_analyzer();
    let query_engine = QueryEngine::new(db.clone(), analyzer);

    // Insert index doc for "elephant" term
    let doc_ids = generate_sorted_object_ids(5);
    let elephant_docs = vec![doc_ids[0], doc_ids[2], doc_ids[4]];
    let index_docs = vec![InvertedIndexDoc::new(
        "eleph".to_string(), // Stemmed form of "elephant"
        0,
        elephant_docs.len() as u64,
        elephant_docs.clone(),
        HashMap::new(),
    )];
    insert_inverted_index_docs(&db, index_docs).await?;

    // Query for "elephant" (will be stemmed to "eleph")
    let results = query_engine.query("elephant").await?;
    assert_eq!(
        results.len(),
        3,
        "Should return 3 documents containing 'elephant'"
    );
    assert_eq!(
        results, elephant_docs,
        "Results should match expected doc IDs"
    );

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_query_single_term_multiple_buckets() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let analyzer = create_text_analyzer();
    let query_engine = QueryEngine::new(db.clone(), analyzer);

    // Insert index docs for "elephant" term across 3 buckets
    let doc_ids = generate_sorted_object_ids(15);
    let bucket0_docs = vec![doc_ids[0], doc_ids[1], doc_ids[2], doc_ids[3], doc_ids[4]];
    let bucket1_docs = vec![doc_ids[5], doc_ids[6], doc_ids[7], doc_ids[8]];
    let bucket2_docs = vec![doc_ids[9], doc_ids[10], doc_ids[11]];

    let index_docs = vec![
        InvertedIndexDoc::new(
            "eleph".to_string(),
            0,
            bucket0_docs.len() as u64,
            bucket0_docs.clone(),
            HashMap::new(),
        ),
        InvertedIndexDoc::new(
            "eleph".to_string(),
            1,
            bucket1_docs.len() as u64,
            bucket1_docs.clone(),
            HashMap::new(),
        ),
        InvertedIndexDoc::new(
            "eleph".to_string(),
            2,
            bucket2_docs.len() as u64,
            bucket2_docs.clone(),
            HashMap::new(),
        ),
    ];
    insert_inverted_index_docs(&db, index_docs).await?;

    // Query for "elephant" (will be stemmed to "eleph")
    let results = query_engine.query("elephant").await?;
    assert_eq!(
        results.len(),
        12,
        "Should return all 12 documents across 3 buckets"
    );

    // Verify results are merged correctly (sorted by ObjectId)
    let mut all_expected_docs = vec![];
    all_expected_docs.extend(bucket0_docs);
    all_expected_docs.extend(bucket1_docs);
    all_expected_docs.extend(bucket2_docs);
    all_expected_docs.sort();

    assert_eq!(
        results, all_expected_docs,
        "Results should contain all docs from all buckets, sorted"
    );

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_query_multiple_terms_all_match() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let analyzer = create_text_analyzer();
    let query_engine = QueryEngine::new(db.clone(), analyzer);

    // Create docs where some documents contain both terms
    let doc_ids = generate_sorted_object_ids(10);

    // "eleph" appears in docs: 0, 2, 4, 5, 7
    let elephant_docs = vec![doc_ids[0], doc_ids[2], doc_ids[4], doc_ids[5], doc_ids[7]];

    // "giraff" appears in docs: 2, 4, 5, 8, 9
    let giraffe_docs = vec![doc_ids[2], doc_ids[4], doc_ids[5], doc_ids[8], doc_ids[9]];

    // Intersection should be: 2, 4, 5
    let expected_intersection = vec![doc_ids[2], doc_ids[4], doc_ids[5]];

    let index_docs = vec![
        InvertedIndexDoc::new(
            "eleph".to_string(),
            0,
            elephant_docs.len() as u64,
            elephant_docs.clone(),
            HashMap::new(),
        ),
        InvertedIndexDoc::new(
            "giraff".to_string(),
            0,
            giraffe_docs.len() as u64,
            giraffe_docs.clone(),
            HashMap::new(),
        ),
    ];
    insert_inverted_index_docs(&db, index_docs).await?;

    // Query for "elephant giraffe"
    let results = query_engine.query("elephant giraffe").await?;
    assert_eq!(
        results.len(),
        3,
        "Should return 3 documents containing both terms"
    );
    assert_eq!(
        results, expected_intersection,
        "Results should be the intersection of both posting lists"
    );

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_query_multiple_terms_no_intersection() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let analyzer = create_text_analyzer();
    let query_engine = QueryEngine::new(db.clone(), analyzer);

    // Create docs where no document contains both terms
    let doc_ids = generate_sorted_object_ids(10);

    // "eleph" appears in docs: 0, 1, 2
    let elephant_docs = vec![doc_ids[0], doc_ids[1], doc_ids[2]];

    // "giraff" appears in docs: 5, 6, 7 (no overlap)
    let giraffe_docs = vec![doc_ids[5], doc_ids[6], doc_ids[7]];

    let index_docs = vec![
        InvertedIndexDoc::new(
            "eleph".to_string(),
            0,
            elephant_docs.len() as u64,
            elephant_docs.clone(),
            HashMap::new(),
        ),
        InvertedIndexDoc::new(
            "giraff".to_string(),
            0,
            giraffe_docs.len() as u64,
            giraffe_docs.clone(),
            HashMap::new(),
        ),
    ];
    insert_inverted_index_docs(&db, index_docs).await?;

    // Query for "elephant giraffe"
    let results = query_engine.query("elephant giraffe").await?;
    assert!(
        results.is_empty(),
        "Should return no results when there's no intersection"
    );

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

// TODO: This test is failing - investigate why doc 1 is appearing in results
// when it should only contain "run" and not "quickli". The query returns 3 results
// instead of the expected 2. May indicate a bug in query method or test setup.
//
#[tokio::test]
async fn test_query_with_text_analysis() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let analyzer = create_text_analyzer();
    let query_engine = QueryEngine::new(db.clone(), analyzer);

    // Insert index docs using stemmed/analyzed terms
    let doc_ids = generate_sorted_object_ids(10);

    let index_docs = vec![
        // "running" stems to "run"
        InvertedIndexDoc::new(
            "run".to_string(),
            0,
            3,
            vec![doc_ids[1], doc_ids[3], doc_ids[5]],
            HashMap::new(),
        ),
        // "quickly" stems to "quickli"
        InvertedIndexDoc::new(
            "quickli".to_string(),
            0,
            2,
            vec![doc_ids[3], doc_ids[5]],
            HashMap::new(),
        ),
    ];
    insert_inverted_index_docs(&db, index_docs).await?;

    // Query with variations that should be stemmed and analyzed
    // "Running" -> lowercase -> "running" -> stem -> "run"
    // "QUICKLY!" -> lowercase -> "quickly!" -> strip punct -> "quickly" -> stem -> "quickli"
    let results = query_engine.query("Running QUICKLY!").await?;

    assert_eq!(
        results.len(),
        2,
        "Should return documents 3 and 5 containing both stemmed terms"
    );
    assert_eq!(
        results,
        vec![doc_ids[3], doc_ids[5]],
        "Results should match expected intersection"
    );

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_query_multiple_terms_partial_missing() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let analyzer = create_text_analyzer();
    let query_engine = QueryEngine::new(db.clone(), analyzer);

    // Insert index doc for only one of the query terms
    let doc_ids = generate_sorted_object_ids(5);
    let elephant_docs = vec![doc_ids[0], doc_ids[2], doc_ids[4]];

    let index_docs = vec![InvertedIndexDoc::new(
        "eleph".to_string(),
        0,
        elephant_docs.len() as u64,
        elephant_docs.clone(),
        HashMap::new(),
    )];
    insert_inverted_index_docs(&db, index_docs).await?;

    // Query for "elephant giraffe" - giraffe doesn't exist in index
    let results = query_engine.query("elephant giraffe").await?;
    assert!(
        results.is_empty(),
        "Should return no results when not all query terms are in the index"
    );

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_query_with_stop_words_filtered() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let analyzer = create_text_analyzer();
    let query_engine = QueryEngine::new(db.clone(), analyzer);

    // Insert index docs (stop words like "the", "a", "is" are not indexed)
    let doc_ids = generate_sorted_object_ids(5);

    let index_docs = vec![
        // Only meaningful words are indexed
        InvertedIndexDoc::new(
            "quick".to_string(),
            0,
            3,
            vec![doc_ids[0], doc_ids[2], doc_ids[4]],
            HashMap::new(),
        ),
        InvertedIndexDoc::new(
            "fox".to_string(),
            0,
            2,
            vec![doc_ids[0], doc_ids[2]],
            HashMap::new(),
        ),
    ];
    insert_inverted_index_docs(&db, index_docs).await?;

    // Query with stop words - they should be filtered out during analysis
    // "the quick fox is running" -> analyzed to ["quick", "fox", "run"]
    // But only "quick" and "fox" are in our index
    let results = query_engine.query("the quick fox").await?;
    assert_eq!(
        results.len(),
        2,
        "Should return documents containing 'quick' and 'fox' (stop words filtered)"
    );
    assert_eq!(
        results,
        vec![doc_ids[0], doc_ids[2]],
        "Results should match intersection of non-stop-word terms"
    );

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_query_complex_intersection_three_terms() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let analyzer = create_text_analyzer();
    let query_engine = QueryEngine::new(db.clone(), analyzer);

    // Create a scenario with 3 terms and complex overlaps
    let doc_ids = generate_sorted_object_ids(20);

    // "alpha" in: 0, 2, 3, 5, 7, 10, 12, 15
    let alpha_docs = vec![
        doc_ids[0],
        doc_ids[2],
        doc_ids[3],
        doc_ids[5],
        doc_ids[7],
        doc_ids[10],
        doc_ids[12],
        doc_ids[15],
    ];

    // "beta" in: 2, 3, 5, 8, 10, 11, 15, 17
    let beta_docs = vec![
        doc_ids[2],
        doc_ids[3],
        doc_ids[5],
        doc_ids[8],
        doc_ids[10],
        doc_ids[11],
        doc_ids[15],
        doc_ids[17],
    ];

    // "gamma" in: 3, 5, 9, 10, 13, 15, 18
    let gamma_docs = vec![
        doc_ids[3],
        doc_ids[5],
        doc_ids[9],
        doc_ids[10],
        doc_ids[13],
        doc_ids[15],
        doc_ids[18],
    ];

    // Intersection of all three: 3, 5, 10, 15
    let expected_intersection = vec![doc_ids[3], doc_ids[5], doc_ids[10], doc_ids[15]];

    let index_docs = vec![
        InvertedIndexDoc::new(
            "alpha".to_string(),
            0,
            alpha_docs.len() as u64,
            alpha_docs,
            HashMap::new(),
        ),
        InvertedIndexDoc::new(
            "beta".to_string(),
            0,
            beta_docs.len() as u64,
            beta_docs,
            HashMap::new(),
        ),
        InvertedIndexDoc::new(
            "gamma".to_string(),
            0,
            gamma_docs.len() as u64,
            gamma_docs,
            HashMap::new(),
        ),
    ];
    insert_inverted_index_docs(&db, index_docs).await?;

    let results = query_engine.query("alpha beta gamma").await?;
    assert_eq!(
        results.len(),
        4,
        "Should return 4 documents containing all three terms"
    );
    assert_eq!(
        results, expected_intersection,
        "Results should be the intersection of all three posting lists"
    );

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}

#[tokio::test]
async fn test_query_terms_different_bucket_counts() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let analyzer = create_text_analyzer();
    let query_engine = QueryEngine::new(db.clone(), analyzer);

    // Create scenario where different terms have different numbers of buckets
    let doc_ids = generate_sorted_object_ids(30);

    // Term "common" has 2 buckets with many docs
    let common_bucket0 = vec![
        doc_ids[0],
        doc_ids[2],
        doc_ids[4],
        doc_ids[6],
        doc_ids[8],
        doc_ids[10],
        doc_ids[12],
        doc_ids[14],
    ];
    let common_bucket1 = vec![
        doc_ids[16],
        doc_ids[18],
        doc_ids[20],
        doc_ids[22],
        doc_ids[24],
        doc_ids[26],
    ];

    // Term "rare" has 1 bucket with few docs
    let rare_bucket0 = vec![doc_ids[2], doc_ids[10], doc_ids[20]];

    // Expected intersection: docs that appear in both terms
    let expected_intersection = vec![doc_ids[2], doc_ids[10], doc_ids[20]];

    let index_docs = vec![
        InvertedIndexDoc::new(
            "common".to_string(),
            0,
            common_bucket0.len() as u64,
            common_bucket0,
            HashMap::new(),
        ),
        InvertedIndexDoc::new(
            "common".to_string(),
            1,
            common_bucket1.len() as u64,
            common_bucket1,
            HashMap::new(),
        ),
        InvertedIndexDoc::new(
            "rare".to_string(),
            0,
            rare_bucket0.len() as u64,
            rare_bucket0,
            HashMap::new(),
        ),
    ];
    insert_inverted_index_docs(&db, index_docs).await?;

    let results = query_engine.query("common rare").await?;
    assert_eq!(
        results.len(),
        3,
        "Should correctly merge buckets and intersect"
    );
    assert_eq!(
        results, expected_intersection,
        "Results should be intersection after merging 'common' buckets"
    );

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}
// Place this helper within your test module
async fn assert_query_match(
    query_engine: &QueryEngine,
    pages_collection: &mongodb::Collection<Page>,
    query_text: &str,
    expected_url: Option<&str>,
) -> Result<()> {
    // 1. Run the query
    let results = query_engine.query(query_text).await?;

    // 2. Handle "Expect No Results" case
    if expected_url.is_none() {
        assert!(
            results.is_empty(),
            "Expected no results for query '{}', but found {}",
            query_text,
            results.len()
        );
        return Ok(());
    }

    // 3. Handle "Expect Match" case
    let target_url = expected_url.unwrap();
    assert!(
        !results.is_empty(),
        "Query '{}' returned no results, expected match for {}",
        query_text,
        target_url
    );

    // 4. Fetch the actual Page document to verify URL
    let filter = doc! { "_id": { "$in": results } };
    let pages: Vec<Page> = pages_collection
        .find(filter)
        .await?
        .try_collect()
        .await?;

    // 5. Assert the URL matches
    // Note: This assumes the top result matches. If checking for *any* match,
    // you might want pages.iter().any(|p| p.url == target_url)
    assert_eq!(
        pages[0].url, target_url,
        "Query '{}' returned wrong page. Expected {}, got {}",
        query_text, target_url, pages[0].url
    );

    Ok(())
}

#[tokio::test]
async fn test_query_phrase_with_full_indexing_pipeline() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));

    // --- Data Setup (Same as before) ---
    let page_karpathy = Page::new(
        "https://example.com/karpathy".to_string(),
        "Andrej Karpathy Bio".to_string(),
        r#"
        Andrej Karpathy is a renowned AI researcher who has made significant contributions
        to the field of deep learning. He previously worked at Tesla as the Director of
        AI and Autopilot Vision, where he led the team building neural networks for
        autonomous driving. Before Tesla, Karpathy was a research scientist at OpenAI,
        working on generative models and reinforcement learning. He is well known for
        his Stanford CS231n course on convolutional neural networks and computer vision.
        "#
        .to_string(),
        vec![], 0, false,
    );

    let page_musk = Page::new(
        "https://example.com/musk".to_string(),
        "Elon Musk Profile".to_string(),
        r#"
        Elon Musk is a genius entrepreneur and business magnate known for founding and
        leading several revolutionary companies. He is the CEO of Tesla, the electric
        vehicle company that has transformed the automotive industry. Musk also founded
        SpaceX with the goal of making humanity a multi-planetary species. Many consider
        Musk a genius person who thinks decades ahead of current technology.
        "#
        .to_string(),
        vec![], 0, false,
    );

    let page_science = Page::new(
        "https://example.com/science/sky".to_string(),
        "Why is the Sky Blue".to_string(),
        r#"
        The sky is blue due to a phenomenon called Rayleigh scattering. When sunlight
        enters Earth's atmosphere, it collides with gas molecules and small particles.
        Blue light has a shorter wavelength and is scattered more than other colors.
        "#
        .to_string(),
        vec![], 0, false,
    );

    pages_repo.insert(&page_karpathy).await?;
    pages_repo.insert(&page_musk).await?;
    pages_repo.insert(&page_science).await?;

    // --- Indexing ---
    let indexer = Arc::new(Indexer::new(Arc::clone(&pages_repo), 100, db.clone()));
    indexer.run(1024 * 1024).await?;

    // --- Testing ---
    let analyzer = create_text_analyzer();
    let query_engine = QueryEngine::new(db.clone(), analyzer);
    let pages_collection = query_engine.db().collection::<Page>(collections::PAGES);

    // Test 1: Karpathy Basics
    assert_query_match(
        &query_engine,
        &pages_collection,
        "ai researcher",
        Some("https://example.com/karpathy")
    ).await?;

    // Test 2: Musk Phrase
    assert_query_match(
        &query_engine,
        &pages_collection,
        "Many consider Musk a genius person who thinks decades",
        Some("https://example.com/musk")
    ).await?;

    // Test 3: Science Long Phrase
    assert_query_match(
        &query_engine,
        &pages_collection,
        "it collides with gas molecules and small particles blue light has a shorter wavelength and",
        Some("https://example.com/science/sky")
    ).await?;

    // Test 4: Karpathy Complex Phrase
    assert_query_match(
        &query_engine,
        &pages_collection,
        "course on convolutional neural networks and computer vision",
        Some("https://example.com/karpathy")
    ).await?;

    // Test 4.1: Karpathy Biography Detail
    assert_query_match(
        &query_engine,
        &pages_collection,
        "Before Tesla, Karpathy was a research scientist at OpenAI",
        Some("https://example.com/karpathy")
    ).await?;

    // Test 5: Musk Company
    assert_query_match(
        &query_engine,
        &pages_collection,
        "electric vehicle company",
        Some("https://example.com/musk")
    ).await?;

    // Test 6: Non-existent phrase (Expect None)
    assert_query_match(
        &query_engine,
        &pages_collection,
        "quantum blockchain cryptocurrency",
        None
    ).await?;

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}
#[tokio::test]
async fn test_edge_cases_and_ambiguity() -> Result<()> {
    let (db, db_name) = create_test_db().await?;
    let pages_repo = Arc::new(PageRepo::new(&db));

    // 1. Setup Trick Pages

    // Shared Prefix Trap
    // Doc: "The quick brown fox..."
    // Index: "quick"(0), "brown"(1), "fox"(2) ... ("The" is stripped)
    let page_fox = Page::new(
        "https://example.com/fox".to_string(),
        "Fox Page".to_string(),
        "The quick brown fox jumps over the lazy dog".to_string(),
        vec![], 0, false,
    );

    let page_cat = Page::new(
        "https://example.com/cat".to_string(),
        "Cat Page".to_string(),
        "The quick brown cat jumps over the lazy dog".to_string(),
        vec![], 0, false,
    );

    // Order Chaos
    // Doc: "We are discussing learning deep concepts..."
    // Index: "discussing"(0), "learning"(1), "deep"(2)... ("We", "are" stripped)
    let page_reverse = Page::new(
        "https://example.com/reverse".to_string(),
        "Reverse Page".to_string(),
        "We are discussing learning deep concepts today".to_string(),
        vec![], 0, false,
    );

    // Repetition Torture Test
    let page_buffalo = Page::new(
        "https://example.com/buffalo".to_string(),
        "Buffalo Page".to_string(),
        "Buffalo buffalo Buffalo buffalo buffalo buffalo Buffalo buffalo".to_string(),
        vec![], 0, false,
    );

    // Distance/Proximity Limit Test
    // Doc: "The Magic is a stone Kingdom..."
    // Stop words removed: "The", "is", "a"
    // Remaining Index: "Magic"(0), "stone"(1), "Kingdom"(2)
    // Gap: Kingdom is at pos 2, Magic at pos 0. Diff = 2.
    let page_distance = Page::new(
        "https://example.com/distance".to_string(),
        "Distance Page".to_string(),
        "The Magic is a stone Kingdom from here".to_string(),
        vec![], 0, false,
    );

    pages_repo.insert(&page_fox).await?;
    pages_repo.insert(&page_cat).await?;
    pages_repo.insert(&page_reverse).await?;
    pages_repo.insert(&page_buffalo).await?;
    pages_repo.insert(&page_distance).await?;

    // 2. Indexing
    let indexer = Arc::new(Indexer::new(Arc::clone(&pages_repo), 100, db.clone()));
    indexer.run(1024 * 1024).await?;

    let analyzer = create_text_analyzer();
    let query_engine = QueryEngine::new(db.clone(), analyzer);
    let pages_collection = query_engine.db().collection::<Page>(collections::PAGES);

    // --- EDGE CASE 1: Shared Prefixes ---
    // Query: "The quick brown fox" -> Filtered: "quick", "brown", "fox"
    // Expect Match: Indices match perfectly (0, 1, 2)
    assert_query_match(
        &query_engine,
        &pages_collection,
        "The quick brown fox",
        Some("https://example.com/fox"),
    ).await?;

    // Query: "The quick brown" -> Filtered: "quick", "brown"
    // Expect: Ambiguity (Matches both pages)
    let results_common = query_engine.query("The quick brown").await?;
    assert!(results_common.len() >= 2, "Should match both fox and cat pages");


    // --- EDGE CASE 2: Repetition ---
    // Query: "Buffalo buffalo" -> Filtered: "buffalo", "buffalo"
    // Expect Match: Logic should handle multiple postings for same term ID
    assert_query_match(
        &query_engine,
        &pages_collection,
        "Buffalo buffalo",
        Some("https://example.com/buffalo"),
    ).await?;


    // --- EDGE CASE 3: Distance Sensitivity ---
    // Query: "Magic Kingdom" -> Filtered: "magic", "kingdom"
    // Query Distance: 1 (Adjacent)
    // Document Distance: 2 ("magic"(0) ... "stone"(1) ... "kingdom"(2))
    // Result: 2 > 1. Should FAIL.
    assert_query_match(
        &query_engine,
        &pages_collection,
        "Magic Kingdom",
        None,
    ).await?;


    // --- EDGE CASE 4: Reverse Order ---
    // Query: "deep learning"
    // Document: "...learning deep..."
    // Because our logic uses `abs_diff`, this currently MATCHES.
    assert_query_match(
        &query_engine,
        &pages_collection,
        "deep learning",
        Some("https://example.com/reverse"),
    ).await?;

    cleanup_test_db(&db, &db_name).await?;
    Ok(())
}
