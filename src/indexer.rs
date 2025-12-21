use anyhow::Result;
use futures::StreamExt;
use mongodb::bson::oid::ObjectId;
use mongodb::options::IndexOptions;
use nanoid::nanoid;

use mongodb::IndexModel;
use mongodb::bson::doc;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

use crate::data_models::InvertedIndexDoc;
use crate::data_models::Page;
use crate::data_models::SpimiDoc;
use crate::db::Database;
use crate::db::PageRepo;

/// Single Pass In Memory Indexing
/// using mongo db
///
/// token = tuple[term, docId]
/// token_stream is sorted by docIds
/// SPIMI(token_stream)
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
///

const DOCID_BYTES: usize = size_of::<ObjectId>();
const DOCIDS_PER_MONGO_DOCUMENT: usize = 1_000_000;
const BUDGET_IN_MEM_BYTES: usize = 1_000_000_000; // 1 GB

pub struct Token(pub String, pub ObjectId);

pub enum StreamMsg {
    Token(Token),
    End,
}
pub struct SpimiBlock {
    pub sorted_terms: Vec<String>,
    pub dictionary: HashMap<String, Vec<ObjectId>>,
}

pub struct Indexer {
    db: Database,
    pages_repo: Arc<PageRepo>,
    page_fetch_limit: i64,
    token_stream_tx: mpsc::UnboundedSender<StreamMsg>,
    token_stream_rx: Mutex<mpsc::UnboundedReceiver<StreamMsg>>,
}

impl Indexer {
    pub fn new(pages_repo: Arc<PageRepo>, page_fetch_limit: i64, db: Database) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            page_fetch_limit,
            pages_repo,
            token_stream_tx: tx,
            token_stream_rx: Mutex::new(rx),
            db,
        }
    }

    pub async fn run(self: Arc<Self>, budget_bytes: usize) -> Result<()> {
        log::info!(
            "Starting indexer with {}GB memory budget",
            budget_bytes / 1_000_000_000
        );

        let (mut pages, mut cursor) = self
            .pages_repo
            .list_paginated(self.page_fetch_limit, Option::None)
            .await?;

        log::info!("Fetched initial batch of {} pages", pages.len());

        let self_clone = self.clone();
        tokio::spawn(async move {
            log::info!("Starting page tokenization stream");
            let mut total_pages_processed = 0;
            while pages.len() != 0 {
                total_pages_processed += pages.len();
                if let Err(e) = self_clone.pages_to_token_stream(&pages) {
                    log::error!("Error converting pages to token stream: {:#}", e);
                }
                log::debug!(
                    "Processed {} pages (total: {})",
                    pages.len(),
                    total_pages_processed
                );

                let res = self_clone
                    .pages_repo
                    .list_paginated(self_clone.page_fetch_limit, cursor)
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

    pub async fn spin_indexer(self: Arc<Self>) -> Result<()> {
        self.run(BUDGET_IN_MEM_BYTES).await
    }

    pub fn pages_to_token_stream(&self, pages: &Vec<Page>) -> Result<()> {
        let token_stream = self.token_stream_tx.clone();
        let mut total_tokens = 0;
        for page in pages {
            let terms = page.cleaned_content.split_ascii_whitespace();
            for term in terms {
                let term = term.trim();
                if term.is_empty() {
                    continue;
                }
                if let Err(e) =
                    token_stream.send(StreamMsg::Token(Token(term.to_string(), page.id)))
                {
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

    pub async fn spimi_invert(self: Arc<Self>, budget_bytes: usize) -> Result<()> {
        log::info!("Starting SPIMI inversion");

        let mut dict: HashMap<String, Vec<ObjectId>> = HashMap::new();
        let mut used_bytes = 0_usize;
        let mut token_stream = self.token_stream_rx.lock().await;
        let mut tokens_processed = 0;
        let mut blocks_written = 0;

        while let Some(token) = token_stream.recv().await {
            let token = match token {
                StreamMsg::Token(token) => token,
                StreamMsg::End => break,
            };

            let (term, doc_id) = (token.0, token.1);
            tokens_processed += 1;

            if !dict.contains_key(&term) {
                used_bytes += term.len();
                used_bytes += 3 * std::mem::size_of::<usize>(); // Vec {len, capacity, ptr}
            }
            let postings = dict.entry(term).or_insert_with(Vec::new);
            let cap_before = postings.capacity();
            postings.push(doc_id);
            let cap_after = postings.capacity();
            if cap_after > cap_before {
                used_bytes += (cap_after - cap_before) * DOCID_BYTES;
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
            if let Some(postings) = block.dictionary.get(&term) {
                // part the postings by 1 Million
                // insert each part with term to mongo
                let part_size = 1_000_000;
                let collection = collection.clone();
                for part in postings.chunks(part_size) {
                    let doc = SpimiDoc::new(term.clone(), part.to_vec()); // NOTE: can we optimize part.to_vec() ?
                    let _ = collection.insert_one(doc).await?;
                }

                terms_written += 1;
                if terms_written % 1000 == 0 {
                    log::debug!("  Written {}/{} terms", terms_written, total_terms);
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
            } else {
                log::error!("IMPOSSIBLE! term {} not found in dictionary", term);
            }
        }

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
                .sort(doc! { "term": 1})
                .build();
            let cursor = collection
                .find(doc! {})
                .with_options(options)
                .await
                .unwrap();
            streamers.push(cursor);
        }

        let mut min_terms: BinaryHeap<Reverse<HeapItem>> = BinaryHeap::new();

        for (idx, streamer) in streamers.iter_mut().enumerate() {
            if streamer.has_next() {
                let e = streamer.next().await.unwrap().unwrap();
                min_terms.push(Reverse(HeapItem {
                    term: e.term.clone(),
                    streamer_idx: idx,
                    doc: e,
                }));
            }
        }

        let mut terms_merged = 0;
        let mut docs_written = 0;

        while let Some(Reverse(item)) = min_terms.pop() {
            let cursor = &mut streamers[item.streamer_idx];
            let mut current_postings = item.doc.postings;
            let mut bucket = 0_i16;
            while let Some(spimi_doc) = cursor.next().await {
                let doc = spimi_doc.unwrap();
                if doc.term != item.term {
                    min_terms.push(Reverse(HeapItem {
                        term: doc.term.clone(),
                        streamer_idx: item.streamer_idx,
                        doc,
                    }));
                    break;
                }

                let postings = doc.postings;
                let result_postings = merge_sorted_lists(&current_postings, &postings);
                current_postings = result_postings;
                if current_postings.len() >= DOCIDS_PER_MONGO_DOCUMENT {
                    let doc =
                        InvertedIndexDoc::new(item.term.clone(), bucket, current_postings.clone());
                    self.db
                        .collection::<InvertedIndexDoc>("inverted_index")
                        .insert_one(doc)
                        .await
                        .unwrap();
                    docs_written += 1;
                    bucket += 1;
                    current_postings.clear();
                }
            }
            if current_postings.len() > 0 {
                let doc =
                    InvertedIndexDoc::new(item.term.clone(), bucket, current_postings.clone());
                self.db
                    .collection::<InvertedIndexDoc>("inverted_index")
                    .insert_one(doc)
                    .await
                    .unwrap();
                docs_written += 1;
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
        // min-heap: reverse the whole ordering
        other
            .term
            .cmp(&self.term)
            .then_with(|| other.streamer_idx.cmp(&self.streamer_idx))
    }
}

pub fn merge_sorted_lists<T>(list_a: &Vec<T>, list_b: &Vec<T>) -> Vec<T>
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
        } else {
            res.push(list_b[b_ptr]);
            b_ptr += 1;
        }
    }

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
