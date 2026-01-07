use anyhow::Result;
use futures::StreamExt;
use mongodb::bson::oid::ObjectId;
use mongodb::options::IndexOptions;
use nanoid::nanoid;

use mongodb::IndexModel;
use mongodb::bson::doc;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

use crate::analyzer::TextAnalyzer;
use crate::data_models::InvertedIndexDoc;
use crate::data_models::Page;
use crate::data_models::SpimiDoc;
use crate::db::Database;
use crate::db::InvertedIndexRepo;
use crate::db::PageRepo;

/// Single Pass In Memory Indexing
/// using mongo db
///
/// token = tuple[term, docId]
/// token_stream is sorted by docIds
/// SPIMI(token_stream)
/// ```text
///     output_file = NEWFILE()
///     dictionary = HashMap()
///     while free_mem_available:
///         do token <- next(token_stream)
///            if token.term not in dictionary:
///                 postings_list = add_dict(dictionary, token.term)
///            else postings_list = get_dict(dicitonary, token.term)
///
///           postings_list.push(token.docId)
///
///     sorted_terms = sort_dict_keys(dictionary)
///     write_block_to_disk_storage(sorted_terms, dictionary, output_file)
/// ```
///

const DOCID_BYTES: usize = size_of::<ObjectId>();
/// Maximum number of document IDs per MongoDB document.
/// MongoDB has a 16MB document limit. With ObjectIds (12 bytes each) plus
/// positions (HashMap with Vec<usize>), each entry uses roughly 50 bytes.
/// 100K entries * 50 bytes = 5MB, providing safe margin under 16MB.
const DOCIDS_PER_MONGO_DOCUMENT: usize = 100_000;

pub struct Token {
    pub term: String,
    pub doc_id: ObjectId,
    pub pos: usize,
}

pub enum StreamMsg {
    Token(Token),
    End,
}

pub struct SpimiBlock {
    pub sorted_terms: Vec<String>,
    pub dictionary: HashMap<String, DictItem>,
}

pub struct Indexer {
    db: Database,
    pages_repo: Arc<PageRepo>,
    inverted_index_repo: Arc<InvertedIndexRepo>,
    page_fetch_limit: i64,
    token_stream_tx: mpsc::UnboundedSender<StreamMsg>,
    token_stream_rx: Mutex<mpsc::UnboundedReceiver<StreamMsg>>,
    text_analyzer: Arc<TextAnalyzer>,
}

pub struct DictItem {
    pub postings: Vec<ObjectId>,
    pub positions: BTreeMap<ObjectId, Vec<usize>>,
}

impl DictItem {
    pub fn new() -> Self {
        Self {
            postings: vec![],
            positions: BTreeMap::new(),
        }
    }
}

impl Indexer {
    pub fn new(pages_repo: Arc<PageRepo>, page_fetch_limit: i64, db: Database) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let text_analyzer = TextAnalyzer::new(
            vec![Box::new(crate::analyzer::HTMLTagFilter::default())],
            Box::new(crate::analyzer::WhiteSpaceTokenizer),
            vec![
                Box::new(crate::analyzer::PunctuationStripFilter::default()),
                Box::new(crate::analyzer::LowerCaseTokenFilter),
                Box::new(crate::analyzer::NumericTokenFilter),
                Box::new(crate::analyzer::StopWordTokenFilter),
                Box::new(crate::analyzer::PorterStemmerTokenFilter),
            ],
        );
        Self {
            page_fetch_limit,
            pages_repo,
            inverted_index_repo: Arc::new(InvertedIndexRepo::new(&db)),
            token_stream_tx: tx,
            token_stream_rx: Mutex::new(rx),
            db,
            text_analyzer: Arc::new(text_analyzer),
        }
    }

    pub async fn run(self: Arc<Self>, budget_bytes: usize) -> Result<()> {
        log::info!(
            "Starting indexer with {}GB memory budget",
            budget_bytes / 1_000_000_000
        );
        // list the unindexed pages to prevent duplicated indexing on the same pages.

        let (mut pages, mut cursor) = self
            .pages_repo
            .list_unindexed_paginated(self.page_fetch_limit, Option::None)
            .await?;

        log::info!("Fetched initial batch of {} unindexed pages", pages.len());

        let self_clone = self.clone();
        // convert all the pages to a stream of tokens (text terms like hello, world, planet, etc)
        // all of those tokens are sent to a channel `token_stream` which then is processed in `spimi_invert`
        tokio::spawn(async move {
            log::info!("Starting page tokenization stream");
            let mut total_pages_processed = 0;
            while pages.len() != 0 {
                total_pages_processed += pages.len();

                // Collect page IDs before moving pages into Arc
                let page_ids: Vec<ObjectId> = pages.iter().map(|p| p.id).collect();
                let rc_pages = pages.into_iter().map(Arc::new).collect();

                if let Err(e) = self_clone.pages_to_token_stream(&rc_pages) {
                    log::error!("Error converting pages to token stream: {:#}", e);
                } else {
                    // Mark pages as indexed after successful processing
                    if let Err(e) = self_clone.pages_repo.mark_many_as_indexed(&page_ids).await {
                        log::error!("Error marking pages as indexed: {:#}", e);
                    } else {
                        log::debug!("Marked {} pages as indexed", page_ids.len());
                    }
                }

                log::debug!(
                    "Processed {} pages (total: {})",
                    rc_pages.len(),
                    total_pages_processed
                );

                let res = self_clone
                    .pages_repo
                    .list_unindexed_paginated(self_clone.page_fetch_limit, cursor)
                    .await;
                if let Err(e) = res {
                    log::error!("Error fetching pages: {:#}", e);
                    break;
                } else {
                    (pages, cursor) = res.unwrap();
                }
            }
            self_clone.token_stream_tx.send(StreamMsg::End).unwrap();
            log::info!(
                "Finished page tokenization stream. Total pages: {}",
                total_pages_processed
            );
        });

        let self_clone = self.clone();
        self_clone.clone().spimi_invert(budget_bytes).await?;

        log::info!("Indexer run completed successfully");
        Ok(())
    }

    pub fn pages_to_token_stream(&self, pages: &Vec<Arc<Page>>) -> Result<()> {
        let token_stream = self.token_stream_tx.clone();
        let mut total_tokens = 0;

        let text_analyzer = self.text_analyzer.clone();
        for page in pages {
            let cleaned_terms = text_analyzer.analyze(page.html_body.clone())?;
            for text_token in cleaned_terms {
                let term = text_token.term.trim();
                if term.is_empty() {
                    continue;
                }
                if let Err(e) = token_stream.send(StreamMsg::Token(Token {
                    term: term.to_string(),
                    doc_id: page.id,
                    pos: text_token.pos,
                })) {
                    log::error!("Error sending token to token stream: {:#}", e);
                }
                total_tokens += 1;
            }
        }
        log::debug!(
            "Extracted {} tokens from {} pages",
            total_tokens,
            pages.len()
        );
        Ok(())
    }

    // SPIMI invert is an algorithm that is an optimization on top of block sort based index (BSBI)
    // see ARCHITECTURE for details on both the algorithms.
    pub async fn spimi_invert(self: Arc<Self>, budget_bytes: usize) -> Result<()> {
        log::info!("Starting SPIMI inversion");

        let mut dict: HashMap<String, DictItem> = HashMap::new();
        let mut used_bytes = 0_usize;
        let mut token_stream = self.token_stream_rx.lock().await;
        let mut tokens_processed = 0;
        let mut blocks_written = 0;

        // we receive the tokens already sorted by the term, so once a term is processed fully, we don't need to worry
        // that after processing some other term after this we again receive the older term.
        while let Some(token) = token_stream.recv().await {
            let token = match token {
                StreamMsg::Token(token) => token,
                StreamMsg::End => break,
            };

            let (term, doc_id, pos) = (token.term, token.doc_id, token.pos);
            tokens_processed += 1;
            if !dict.contains_key(&term) {
                used_bytes += term.len();
                used_bytes += std::mem::size_of::<DictItem>(); // struct overhead
            }

            let dict_item = dict.entry(term.clone()).or_insert_with(DictItem::new);

            let postings = &mut dict_item.postings;
            let positions = &mut dict_item.positions;

            match positions.entry(doc_id) {
                std::collections::btree_map::Entry::Vacant(e) => {
                    // first time seeing this term in this document
                    // adding to postings only when vacant to keep it unique
                    postings.push(doc_id);
                    e.insert(vec![pos]);
                    used_bytes += DOCID_BYTES + std::mem::size_of::<Vec<u32>>() + 4;
                }
                std::collections::btree_map::Entry::Occupied(mut e) => {
                    // already seen this term in this document
                    e.get_mut().push(pos);

                    used_bytes += 4; // just above pos
                }
            }

            if tokens_processed % 100_000 == 0 {
                log::debug!(
                    "Processed {} tokens, memory usage: {:.2}MB / {:.2}MB",
                    tokens_processed,
                    used_bytes as f64 / 1_000_000.0,
                    budget_bytes as f64 / 1_000_000.0
                );
            }

            if used_bytes >= budget_bytes {
                blocks_written += 1;
                log::info!(
                    "Memory budget reached. Flushing block #{} to disk ({} unique terms, {} tokens processed)",
                    blocks_written,
                    dict.keys().len(),
                    tokens_processed
                );

                // flush to the disk
                let mut sorted_terms = dict.keys().cloned().collect::<Vec<String>>();
                sorted_terms.sort();
                self.persist_block_to_disk(SpimiBlock {
                    sorted_terms: sorted_terms,
                    dictionary: dict,
                })
                .await?;

                log::info!("Block #{} persisted successfully", blocks_written);
                dict = HashMap::new();
                used_bytes = 0;
            }
        }

        // final flush
        let mut sorted_terms = dict.keys().cloned().collect::<Vec<String>>();
        sorted_terms.sort();

        self.persist_block_to_disk(SpimiBlock {
            sorted_terms: sorted_terms,
            dictionary: dict,
        })
        .await?;

        log::info!(
            "SPIMI inversion complete. Processed {} tokens across {} blocks",
            tokens_processed,
            blocks_written
        );

        self.merge_persisted_blocks().await?;
        Ok(())
    }

    // TODO: refactor this
    pub async fn try_recv_token(&mut self) -> Option<StreamMsg> {
        let mut token_stream = self.token_stream_rx.lock().await;
        token_stream.try_recv().ok()
    }

    pub async fn drain_tokens(&mut self) -> Vec<StreamMsg> {
        let mut tokens = Vec::new();
        let mut token_stream = self.token_stream_rx.lock().await;
        while let Ok(token) = token_stream.try_recv() {
            tokens.push(token);
        }
        tokens
    }

    pub async fn persist_block_to_disk(&self, block: SpimiBlock) -> Result<()> {
        let collection_name = format!("spimi_block_{}", nanoid!(4));
        log::debug!("Persisting block to collection: {}", collection_name);

        let collection = self.db.collection::<SpimiDoc>(&collection_name);
        let total_terms = block.sorted_terms.len();
        let mut terms_written = 0;

        for term in block.sorted_terms {
            if let Some(dict_item) = block.dictionary.get(&term) {
                let postings = &dict_item.postings;

                // part the postings by 1 Million
                // insert each part with term to mongo
                let mut bucket = 0_i16;
                for part in postings.chunks(DOCIDS_PER_MONGO_DOCUMENT) {
                    let df = part.len() as u64;
                    // range query on postions to get the positions for all the docs less than the last doc in this part.
                    // Since postings are sorted, the range is simply [first_doc..last_doc]
                    let start_doc = part.first().expect("chunk should not be empty, start doc");
                    let end_doc = part.last().expect("chunk should not be empty, end doc");
                    let this_positions: HashMap<ObjectId, Vec<usize>> = dict_item
                        .positions
                        .range(start_doc..=end_doc)
                        .map(|(k, v)| (*k, v.clone()))
                        .collect();
                    // TODO: abstract out the persistance in a separate interface
                    let doc =
                        SpimiDoc::new(term.clone(), bucket, df, part.to_vec(), this_positions); // NOTE: can we optimize part.to_vec() ?
                    let _ = collection.insert_one(doc).await?;
                    bucket += 1;
                }

                terms_written += 1;
                if terms_written % 1000 == 0 {
                    log::debug!("  Written {}/{} terms", terms_written, total_terms);
                }
            } else {
                log::error!("IMPOSSIBLE! term {} not found in dictionary", term);
            }
        }

        // create index in background
        let options = IndexOptions::builder().background(Some(true)).build();
        collection
            .create_index(
                IndexModel::builder()
                    .options(options)
                    .keys(doc! { "term": 1 })
                    .build(),
            )
            .await?;
        log::debug!(
            "Block persisted: {} ({} terms)",
            collection_name,
            total_terms
        );
        Ok(())
    }

    pub async fn merge_persisted_blocks(&self) -> Result<()> {
        log::info!("Starting merge of persisted blocks");

        let filter = doc! {
            "name": {
                "$regex": r"^spimi_block_*"
            }
        };
        let collections = self
            .db
            .database()
            .list_collection_names()
            .filter(filter)
            .await
            .unwrap();

        let num_blocks = collections.len();
        if num_blocks == 0 {
            log::warn!("No SPIMI blocks found to merge");
            return Ok(());
        }

        log::info!("Found {} blocks to merge", num_blocks);

        let mut streamers = Vec::new();
        for coll in collections {
            log::debug!("  Opening cursor for block: {}", coll);
            let collection = self.db.collection::<SpimiDoc>(&coll);
            let options = mongodb::options::FindOptions::builder()
                .sort(doc! { "term": 1, "bucket": 1})
                .build();
            let cursor = collection
                .find(doc! {})
                .with_options(options)
                .await
                .unwrap();
            streamers.push(cursor);
        }

        let mut min_terms: BinaryHeap<Reverse<HeapItem>> = BinaryHeap::new();
        // prime the min terms heap
        for (idx, streamer) in streamers.iter_mut().enumerate() {
            if streamer.has_next() {
                let doc = streamer.next().await.unwrap().unwrap();
                min_terms.push(Reverse(HeapItem {
                    term: doc.term.clone(),
                    streamer_idx: idx,
                    doc,
                }));
            }
        }

        let mut terms_merged = 0;
        let mut docs_written = 0;

        // STATE TRACKING
        let mut current_postings = vec![];
        let mut current_positions = HashMap::new();
        let mut bucket = 0_i16;
        let mut active_term: Option<String> = Option::None;
        // Track if we're appending to an existing bucket (for incremental indexing)
        let mut existing_bucket_id: Option<ObjectId> = None;

        while let Some(Reverse(item)) = min_terms.pop() {
            let doc = item.doc;

            // When term changes, flush the current term and load state for new term
            if let Some(ref term) = active_term {
                if *term != doc.term {
                    // Flush the old term
                    self.flush_term_to_db(
                        term,
                        &mut current_postings,
                        &mut current_positions,
                        &mut bucket,
                        &mut docs_written,
                        &mut existing_bucket_id,
                    )
                    .await?;
                    current_positions.clear();
                    current_postings.clear();

                    // Load existing bucket state for the new term (incremental indexing)
                    if let Some(last_bucket) =
                        self.inverted_index_repo.get_last_bucket(&doc.term).await?
                    {
                        let space_used = last_bucket.postings.len();
                        if space_used < DOCIDS_PER_MONGO_DOCUMENT {
                            // We can continue appending to this bucket
                            log::debug!(
                                "Continuing from existing bucket {} for term '{}' ({}/{} docs)",
                                last_bucket.bucket,
                                doc.term,
                                space_used,
                                DOCIDS_PER_MONGO_DOCUMENT
                            );
                            bucket = last_bucket.bucket;
                            existing_bucket_id = Some(last_bucket.id);
                            // Note: We don't load existing postings/positions - we only append new ones
                        } else {
                            // Bucket is full, start a new one
                            bucket = last_bucket.bucket + 1;
                            existing_bucket_id = None;
                        }
                    } else {
                        // No existing bucket, start fresh
                        bucket = 0;
                        existing_bucket_id = None;
                    }
                }
            } else {
                // First term - check if it exists in the index
                if let Some(last_bucket) =
                    self.inverted_index_repo.get_last_bucket(&doc.term).await?
                {
                    let space_used = last_bucket.postings.len();
                    if space_used < DOCIDS_PER_MONGO_DOCUMENT {
                        bucket = last_bucket.bucket;
                        existing_bucket_id = Some(last_bucket.id);
                    } else {
                        bucket = last_bucket.bucket + 1;
                        existing_bucket_id = None;
                    }
                }
            }
            active_term = Some(doc.term.clone());
            let merged_postings = merge_sorted_lists_dedup(&current_postings, &doc.postings);
            current_postings = merged_postings;
            current_positions = merge_hashmaps(current_positions, doc.positions);

            if current_postings.len() >= DOCIDS_PER_MONGO_DOCUMENT {
                let overflow_postings = current_postings.split_off(DOCIDS_PER_MONGO_DOCUMENT);
                let mut flush_positions = HashMap::new();
                for doc_id in &current_postings {
                    // current only have the postings that we are going to flush.
                    if let Some(pos) = current_positions.remove(doc_id) {
                        // remove all these postions from `current_positions` because we are going to flush them.
                        flush_positions.insert(*doc_id, pos);
                    }
                }
                self.flush_term_to_db(
                    active_term.as_ref().unwrap(),
                    &mut current_postings,
                    &mut flush_positions,
                    &mut bucket,
                    &mut docs_written,
                    &mut existing_bucket_id,
                )
                .await?;
                current_postings = overflow_postings;
                // no need to do anything with `current_positions` as we have already removed.
            }

            // ADVANCE the streamer where this term came from.
            let cursor = &mut streamers[item.streamer_idx];
            if cursor.has_next() {
                let next_spimi = cursor.next().await.unwrap().unwrap();
                min_terms.push(Reverse(HeapItem {
                    term: next_spimi.term.clone(),
                    streamer_idx: item.streamer_idx,
                    doc: next_spimi,
                }));
            }
            terms_merged += 1;
            if terms_merged % 10_000 == 0 {
                log::info!(
                    "  Merged {} terms, written {} documents to inverted index",
                    terms_merged,
                    docs_written
                );
            }
        }

        if let Some(last_term) = active_term {
            if !current_postings.is_empty() {
                self.flush_term_to_db(
                    &last_term,
                    &mut current_postings,
                    &mut current_positions,
                    &mut bucket,
                    &mut docs_written,
                    &mut existing_bucket_id,
                )
                .await?;
            }
        }

        log::info!(
            "Merge complete! Processed {} unique terms, wrote {} documents to inverted index",
            terms_merged,
            docs_written
        );

        // Clean up temporary SPIMI block collections
        self.cleanup_spimi_blocks().await?;

        log::info!("Indexing complete! Safe to quit now.");

        Ok(())
    }

    async fn cleanup_spimi_blocks(&self) -> Result<()> {
        log::info!("Cleaning up temporary SPIMI block collections");

        let filter = doc! {
            "name": {
                "$regex": r"^spimi_block_*"
            }
        };
        let collections = self
            .db
            .database()
            .list_collection_names()
            .filter(filter)
            .await?;

        let num_collections = collections.len();
        if num_collections == 0 {
            log::info!("No SPIMI block collections to clean up");
            return Ok(());
        }

        log::info!(
            "Found {} SPIMI block collections to delete",
            num_collections
        );

        for collection_name in collections {
            log::debug!("  Dropping collection: {}", collection_name);
            self.db
                .database()
                .collection::<SpimiDoc>(&collection_name)
                .drop()
                .await?;
        }

        log::info!(
            "Successfully deleted {} SPIMI block collections",
            num_collections
        );
        Ok(())
    }

    async fn flush_term_to_db(
        &self,
        term: &str,
        postings: &mut Vec<ObjectId>,
        positions: &mut HashMap<ObjectId, Vec<usize>>,
        bucket: &mut i16,
        docs_written: &mut usize,
        existing_bucket_id: &mut Option<ObjectId>,
    ) -> Result<()> {
        if postings.is_empty() {
            return Ok(());
        }

        // If we have an existing bucket to append to, use update instead of insert
        if let Some(doc_id) = existing_bucket_id.take() {
            log::debug!(
                "Appending {} docs to existing bucket for term '{}'",
                postings.len(),
                term
            );
            self.inverted_index_repo
                .append_to_bucket(doc_id, postings, positions)
                .await?;
        } else {
            // Insert a new bucket document
            let doc = InvertedIndexDoc::new(
                term.to_string(),
                *bucket,
                postings.len() as u64,
                postings.clone(),
                positions.clone(),
            );
            self.inverted_index_repo.insert(doc).await?;
        }

        *docs_written += 1;
        *bucket += 1;
        Ok(())
    }
}

// Helper to keep the main loop clean

fn merge_hashmaps(
    mut map_a: HashMap<ObjectId, Vec<usize>>,
    map_b: HashMap<ObjectId, Vec<usize>>,
) -> HashMap<ObjectId, Vec<usize>> {
    for (doc_id, mut new_positions) in map_b {
        match map_a.entry(doc_id) {
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(new_positions);
            }
            std::collections::hash_map::Entry::Occupied(mut e) => {
                let x = e.get_mut();
                x.append(&mut new_positions);
            }
        }
    }
    map_a
}

struct HeapItem {
    term: String,
    streamer_idx: usize,
    doc: SpimiDoc,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.term == other.term && self.streamer_idx == other.streamer_idx
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.term
            .cmp(&other.term)
            .then_with(|| self.streamer_idx.cmp(&other.streamer_idx))
    }
}

pub fn merge_sorted_lists_dedup<T>(list_a: &Vec<T>, list_b: &Vec<T>) -> Vec<T>
where
    T: PartialOrd + Clone + Copy,
{
    let mut a_ptr = 0;
    let mut b_ptr = 0;
    let mut res = Vec::with_capacity(list_a.len() + list_b.len());
    while a_ptr < list_a.len() && b_ptr < list_b.len() {
        if list_a[a_ptr] < list_b[b_ptr] {
            res.push(list_a[a_ptr]);
            a_ptr += 1;
        } else if list_a[a_ptr] > list_b[b_ptr] {
            res.push(list_b[b_ptr]);
            b_ptr += 1;
        } else {
            // EQUAL
            res.push(list_a[a_ptr]);
            a_ptr += 1;
            b_ptr += 1;
        }
    }

    // AS we are assuming both the lists themselves are free of duplicates, we don't need any extra handling here.
    while a_ptr < list_a.len() {
        res.push(list_a[a_ptr]);
        a_ptr += 1;
    }
    while b_ptr < list_b.len() {
        res.push(list_b[b_ptr]);
        b_ptr += 1;
    }
    res
}
