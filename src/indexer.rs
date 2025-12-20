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
use tokio::sync::mpsc;

use crate::data_models::InvertedIndexDoc;
use crate::data_models::Page;
use crate::data_models::SpimiDoc;
use crate::db::Database;
use crate::db::PageRepo;

///
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

pub struct Token(pub String, pub ObjectId);
const DOCID_BYTES: usize = size_of::<ObjectId>();
const DOCIDS_PER_MONGO_DOCUMENT: usize = 1_000_000;

pub struct SpimiBlock {
    pub sorted_terms: Vec<String>,
    pub dictionary: HashMap<String, Vec<ObjectId>>,
}

pub struct Indexer {
    db: Database,
    pages_repo: Arc<PageRepo>,
    page_fetch_limit: i64,
    token_stream_tx: mpsc::UnboundedSender<Token>,
    token_stream_rx: mpsc::UnboundedReceiver<Token>,
}

impl Indexer {
    pub fn new(pages_repo: Arc<PageRepo>, page_fetch_limit: i64, db: Database) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            page_fetch_limit,
            pages_repo,
            token_stream_tx: tx,
            token_stream_rx: rx,
            db,
        }
    }

    pub async fn spin_indexer(&self) -> Result<()> {
        // query mongodb for the pages, sorted by the _id (DocId)
        // query using pagination in batches let's say of of 10k each.
        // and emit or stream Token(Term, docId) to spimi
        let (mut pages, mut cursor) = self
            .pages_repo
            .list_paginated(self.page_fetch_limit, Option::None)
            .await?;
        while pages.len() != 0 {
            self.pages_to_token_stream(&pages)?;
            (pages, cursor) = self
                .pages_repo
                .list_paginated(self.page_fetch_limit, cursor)
                .await?;
        }

        Ok(())
    }

    pub fn pages_to_token_stream(&self, pages: &Vec<Page>) -> Result<()> {
        let token_stream = self.token_stream_tx.clone();
        for page in pages {
            let terms = page.cleaned_content.split_ascii_whitespace();
            for term in terms {
                if let Err(e) = token_stream.send(Token(term.to_string(), page.id)) {
                    log::error!("error sending token to token stream: {:#}", e);
                }
            }
        }
        Ok(())
    }

    pub async fn spimi_invert<I>(&self, token_stream: I, budget_bytes: usize) -> Result<()>
    where
        I: IntoIterator<Item = Token>,
    {
        let mut dict: HashMap<String, Vec<ObjectId>> = HashMap::new();
        let mut used_bytes = 0_usize;
        for token in token_stream {
            let (term, doc_id) = (token.0, token.1);
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
            if used_bytes >= budget_bytes {
                // flush to the disk
                self.persist_block_to_disk(SpimiBlock {
                    sorted_terms: dict.keys().cloned().collect(),
                    dictionary: dict,
                })
                .await?;
                dict = HashMap::new();
                used_bytes = 0;
            }
        }
        self.merge_persisted_blocks().await?;
        Ok(())
    }

    pub fn try_recv_token(&mut self) -> Option<Token> {
        self.token_stream_rx.try_recv().ok()
    }

    pub fn drain_tokens(&mut self) -> Vec<Token> {
        let mut tokens = Vec::new();
        while let Ok(token) = self.token_stream_rx.try_recv() {
            tokens.push(token);
        }
        tokens
    }

    pub async fn persist_block_to_disk(&self, block: SpimiBlock) -> Result<()> {
        let collection_name = format!("spimi_block_{}", nanoid!(4));
        let collection = self.db.collection::<SpimiDoc>(&collection_name);
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
        Ok(())
    }

    pub async fn merge_persisted_blocks(&self) -> Result<()> {
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
        let mut streamers = Vec::new();
        for coll in collections {
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
        let mut min_terms: BinaryHeap<Reverse<(String, usize)>> = BinaryHeap::new();

        for (idx, streamer) in streamers.iter_mut().enumerate() {
            if streamer.has_next() {
                let e = streamer.next().await.unwrap().unwrap();
                min_terms.push(Reverse((e.term, idx)));
            }
        }

        while let Some(Reverse((current_term, idx))) = min_terms.pop() {
            let cursor = &mut streamers[idx];
            let mut current_postings = vec![];
            while let Some(spimi_doc) = cursor.next().await {
                let doc = spimi_doc.unwrap();
                if doc.term != current_term {
                    min_terms.push(Reverse((doc.term, idx)));
                    break;
                }
                let postings = doc.postings;
                let result_postings = merge_sorted_lists(&current_postings, &postings);
                current_postings = result_postings;
                if current_postings.len() >= DOCIDS_PER_MONGO_DOCUMENT {
                    let doc = InvertedIndexDoc::new(current_term.clone(), current_postings.clone());
                    self.db
                        .collection::<InvertedIndexDoc>("inverted_index")
                        .insert_one(doc)
                        .await
                        .unwrap();
                    current_postings.clear();
                }
            }
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    // A helper type to test tie-breaking behavior.
    // We define ordering ONLY by `key`, ignoring `origin`,
    // so we can detect whether ties come from B before A.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct Item {
        key: i32,
        origin: u8, // 0 = A, 1 = B
    }

    impl PartialOrd for Item {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            self.key.partial_cmp(&other.key)
        }
    }

    // Helper: verify sortedness for types with total ordering that also implement PartialOrd
    fn is_non_decreasing<T: PartialOrd>(v: &[T]) -> bool {
        v.windows(2).all(|w| !(w[1] < w[0]))
    }

    #[test]
    fn empty_empty() {
        let a: Vec<i32> = vec![];
        let b: Vec<i32> = vec![];
        let res = merge_sorted_lists(&a, &b);
        assert_eq!(res, Vec::<i32>::new());
    }

    #[test]
    fn empty_nonempty() {
        let a: Vec<i32> = vec![];
        let b = vec![1, 2, 3];
        assert_eq!(merge_sorted_lists(&a, &b), vec![1, 2, 3]);
    }

    #[test]
    fn nonempty_empty() {
        let a = vec![1, 2, 3];
        let b: Vec<i32> = vec![];
        assert_eq!(merge_sorted_lists(&a, &b), vec![1, 2, 3]);
    }

    #[test]
    fn single_single_a_less() {
        let a = vec![1];
        let b = vec![2];
        assert_eq!(merge_sorted_lists(&a, &b), vec![1, 2]);
    }

    #[test]
    fn single_single_b_less() {
        let a = vec![2];
        let b = vec![1];
        assert_eq!(merge_sorted_lists(&a, &b), vec![1, 2]);
    }

    #[test]
    fn single_single_equal_tie_goes_to_b_first() {
        let a = vec![Item { key: 5, origin: 0 }];
        let b = vec![Item { key: 5, origin: 1 }];
        let res = merge_sorted_lists(&a, &b);

        assert_eq!(res.len(), 2);
        assert_eq!(res[0].origin, 1, "tie should take from B first");
        assert_eq!(res[1].origin, 0, "then take from A");
        assert!(is_non_decreasing(&res));
    }

    #[test]
    fn a_exhausts_first_then_append_b_tail() {
        let a = vec![1, 2, 3];
        let b = vec![10, 11];
        assert_eq!(merge_sorted_lists(&a, &b), vec![1, 2, 3, 10, 11]);
    }

    #[test]
    fn b_exhausts_first_then_append_a_tail() {
        let a = vec![10, 11];
        let b = vec![1, 2, 3];
        assert_eq!(merge_sorted_lists(&a, &b), vec![1, 2, 3, 10, 11]);
    }

    #[test]
    fn perfect_alternation() {
        let a = vec![1, 3, 5];
        let b = vec![2, 4, 6];
        assert_eq!(merge_sorted_lists(&a, &b), vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn run_patterns() {
        let a = vec![1, 2, 9, 10];
        let b = vec![3, 4, 5, 6];
        assert_eq!(merge_sorted_lists(&a, &b), vec![1, 2, 3, 4, 5, 6, 9, 10]);
    }

    #[test]
    fn duplicates_within_and_across_lists() {
        let a = vec![1, 2, 2, 4];
        let b = vec![2, 2, 3];
        assert_eq!(merge_sorted_lists(&a, &b), vec![1, 2, 2, 2, 2, 3, 4]);
    }

    #[test]
    fn equal_blocks_start_tie_rule_visible() {
        let a = vec![
            Item { key: 1, origin: 0 },
            Item { key: 1, origin: 0 },
            Item { key: 2, origin: 0 },
        ];
        let b = vec![
            Item { key: 1, origin: 1 },
            Item { key: 1, origin: 1 },
            Item { key: 3, origin: 1 },
        ];
        let res = merge_sorted_lists(&a, &b);

        // First two 1s should come from B, then A's 1s.
        assert_eq!(res[0].origin, 1);
        assert_eq!(res[1].origin, 1);
        assert_eq!(res[2].origin, 0);
        assert_eq!(res[3].origin, 0);

        assert!(is_non_decreasing(&res));
        assert_eq!(
            res.iter().map(|x| x.key).collect::<Vec<_>>(),
            vec![1, 1, 1, 1, 2, 3]
        );
    }

    #[test]
    fn negatives_and_mixed_sign() {
        let a = vec![-5, -1, 0];
        let b = vec![-3, 2];
        assert_eq!(merge_sorted_lists(&a, &b), vec![-5, -3, -1, 0, 2]);
    }

    #[test]
    fn integer_extremes() {
        let a = vec![i32::MIN, 0, i32::MAX];
        let b = vec![i32::MIN, i32::MAX];
        let res = merge_sorted_lists(&a, &b);
        assert_eq!(res, vec![i32::MIN, i32::MIN, 0, i32::MAX, i32::MAX]);
    }

    #[test]
    fn chars() {
        let a = vec!['a', 'c'];
        let b = vec!['b', 'd'];
        assert_eq!(merge_sorted_lists(&a, &b), vec!['a', 'b', 'c', 'd']);
    }

    #[test]
    fn str_slices_static() {
        // &'static str is Copy, so it works with your bounds.
        let a: Vec<&'static str> = vec!["apple", "carrot", "pear"];
        let b: Vec<&'static str> = vec!["banana", "peach"];
        assert_eq!(
            merge_sorted_lists(&a, &b),
            vec!["apple", "banana", "carrot", "peach", "pear"]
        );
    }

    #[test]
    fn str_slices_with_duplicates() {
        let a: Vec<&'static str> = vec!["a", "b", "b", "z"];
        let b: Vec<&'static str> = vec!["b", "b", "c"];
        assert_eq!(
            merge_sorted_lists(&a, &b),
            vec!["a", "b", "b", "b", "b", "c", "z"]
        );
    }

    #[test]
    fn output_is_sorted_for_total_order_inputs_i32() {
        let a = vec![1, 2, 2, 10];
        let b = vec![0, 2, 3, 3, 9];
        let res = merge_sorted_lists(&a, &b);
        assert!(is_non_decreasing(&res));
        assert_eq!(res.len(), a.len() + b.len());
    }
}
