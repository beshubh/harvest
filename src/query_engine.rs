use std::usize;

use anyhow::Result;
use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;

use crate::analyzer::TextAnalyzer;
use crate::data_models::InvertedIndexDoc;
use crate::db::Database;
use crate::db::collections;

pub struct QueryEngine {
    db: Database,
    analyzer: TextAnalyzer,
}

pub fn intersect_two_postings<'a, T>(
    posting_list1: &'a [T],
    posting_list2: &'a [T],
    out: &mut Vec<T>,
) where
    T: Ord + Clone,
{
    let (mut p1i, mut p2i) = (0_usize, 0_usize);
    while p1i < posting_list1.len() && p2i < posting_list2.len() {
        match posting_list1[p1i].cmp(&posting_list2[p2i]) {
            std::cmp::Ordering::Equal => {
                out.push(posting_list1[p1i].clone());
                p1i += 1;
                p2i += 1;
            }
            std::cmp::Ordering::Less => p1i += 1,
            std::cmp::Ordering::Greater => p2i += 1,
        }
    }
}
#[test]
fn test_intersect_two_postings() {
    {
        let p1 = vec![1, 2, 3, 4, 5];
        let p2 = vec![2, 10, 12, 15];
        let expected = vec![2];

        let mut out = Vec::new();
        intersect_two_postings(&p1, &p2, &mut out);
        assert_eq!(out, expected);
    }

    {
        let p1 = vec![2, 10, 45, 100, 1000];
        let p2 = vec![2, 20, 45, 1000];
        let expected = vec![2, 45, 1000];

        let mut out = Vec::new();
        intersect_two_postings(&p1, &p2, &mut out);
        assert_eq!(out, expected);
    }

    {
        let p1 = vec![100, 101, 102, 105];
        let p2 = vec![101];
        let expected = vec![101];

        let mut out = Vec::new();
        intersect_two_postings(&p1, &p2, &mut out);
        assert_eq!(out, expected);
    }

    {
        let p1 = vec![100, 101, 102, 105];
        let p2 = vec![1, 2, 3, 4, 5];

        let mut out = Vec::new();
        intersect_two_postings(&p1, &p2, &mut out);
        assert!(out.is_empty());
    }
}

impl QueryEngine {
    pub fn new(db: Database, analyzer: TextAnalyzer) -> Self {
        Self { db, analyzer }
    }

    // NOTE: two lists are expected to be sorted in asc order.

    fn intersect_postings<T>(posting_lists: &[&[T]]) -> Vec<T>
    where
        T: Ord + Clone,
    {
        if posting_lists.is_empty() {
            return Vec::new();
        }
        let mut smallest_idx = 0usize;
        for (idx, pl) in posting_lists.iter().enumerate() {
            if pl.len() < posting_lists[smallest_idx].len() {
                smallest_idx = idx;
            }
        }
        let mut result: Vec<T> = posting_lists[smallest_idx].to_vec();
        let mut scratch: Vec<T> = Vec::new();
        for (idx, pl) in posting_lists.iter().enumerate() {
            if idx == smallest_idx {
                continue;
            }
            scratch.clear();
            intersect_two_postings(&result, pl, &mut scratch);
            std::mem::swap(&mut result, &mut scratch);
            if result.is_empty() {
                break;
            }
        }

        result
    }

    pub async fn query(&self, query: &str) -> Result<Vec<ObjectId>> {
        // Analyze the query text using the same pipeline as documents
        let text_tokens = self.analyzer.analyze(query.to_string())?;

        let terms = text_tokens
            .iter()
            .map(|t| t.term.clone())
            .collect::<Vec<String>>();
        // TODO: Implement actual query logic using the inverted index
        let i_index = self.db.collection::<InvertedIndexDoc>(collections::INDEX);
        let init_filter = doc! { "term": {
            "$in": terms.clone(),
            "bucket": 0, // initialy just get the first bucket for all the terms
        }};
        let options = mongodb::options::FindOptions::builder()
            .limit(terms.len() as i64)
            .build();

        let index_docs = i_index.find(init_filter).await?;

        unimplemented!()
    }
}

#[test]
fn test_intersect_postings_edgy_multilist_cascade() {
    {
        let l1 = vec![-10, -5, 0, 2, 3, 5, 8, 13, 21, 34];
        let l2 = vec![0, 2, 3, 5, 5, 8, 34, 55];
        let l3 = vec![2, 3, 8]; // <-- smallest
        let l4 = vec![-999, 2, 3, 8, 999999];
        let l5 = vec![2, 3, 8, 100];

        // Across all lists:
        // 2 appears: l1, l2, l3, l4, l5 => min = 1
        // 3 appears: l1(1), l2(1), l3(1), l4(1), l5(1) => min = 1
        // 8 appears: l1(1), l2(1), l3(2), l4(1), l5(3) => min = 1
        let expected = vec![2, 3, 8];

        // You likely have this as a method; here I’ll call a free helper for illustration.
        // Replace `intersect_postings_impl(...)` with `engine.intersect_postings(...)`.
        let got = QueryEngine::intersect_postings(&[&l1, &l2, &l3, &l4, &l5]);

        assert_eq!(got, expected);
    }
    // Smallest list is in the middle, intersection shrinks gradually
    {
        let l1 = vec![1u32, 2, 3, 4, 5, 8, 13, 21, 34, 55];
        let l2 = vec![2u32, 3, 5, 8, 13, 34, 89];
        let l3 = vec![3u32, 8, 34]; // smallest (3)
        let l4 = vec![0u32, 3, 8, 34, 144, 233];

        let expected = vec![3u32, 8, 34];
        let got = QueryEngine::intersect_postings(&[&l1, &l2, &l3, &l4]);
        assert_eq!(got, expected);
    }
    // Tie for smallest (two lists same min length); should still intersect correctly
    {
        let l1 = vec![10u32, 20, 30, 40, 50];
        let l2 = vec![20u32, 40]; // smallest tie
        let l3 = vec![0u32, 20, 40, 60];
        let l4 = vec![20u32, 40]; // smallest tie
        let l5 = vec![20u32, 70, 80, 90];

        let expected = vec![20u32];
        let got = QueryEngine::intersect_postings(&[&l1, &l2, &l3, &l4, &l5]);
        assert_eq!(got, expected);
    }
    // Contains an empty list => empty result (also tests early-break)
    {
        let l1 = vec![1u32, 2, 3, 4, 5];
        let l2: Vec<u32> = vec![];
        let l3 = vec![2u32, 3, 4];

        let got = QueryEngine::intersect_postings(&[&l1, &l2, &l3]);
        assert!(got.is_empty());
    }
    // Single list input: intersection == that list (identity)
     {
         let l1 = vec![7u32, 9, 11, 13];
         let got = QueryEngine::intersect_postings(&[&l1]);
         assert_eq!(got, l1);
     }

     // Disjoint after first/second intersection => becomes empty mid-way
     {
         let l1 = vec![1u32, 2, 3, 4, 5, 6, 7, 8];
         let l2 = vec![2u32, 4, 6, 8];         // intersection would be [2,4,6,8]
         let l3 = vec![1u32, 3, 5, 7];         // kills it to []
         let l4 = vec![2u32, 4, 6, 8, 10, 12]; // should never matter after empty

         let got = QueryEngine::intersect_postings(&[&l1, &l2, &l3, &l4]);
         assert!(got.is_empty());
     }

     // Huge gaps / big IDs: stresses pointer advancement (no negatives, no duplicates)
     {
         let l1 = vec![1u32, 10, 1_000, 1_000_000, 4_000_000_000u32];
         let l2 = vec![0u32, 10, 999, 1_000_000, 3_000_000_000u32, 4_000_000_000u32];
         let l3 = vec![10u32, 1_000_000, 4_000_000_000u32];

         let expected = vec![10u32, 1_000_000, 4_000_000_000u32];
         let got = QueryEngine::intersect_postings(&[&l1, &l2, &l3]);
         assert_eq!(got, expected);
     }
}
