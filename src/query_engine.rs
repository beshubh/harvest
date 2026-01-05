use anyhow::Result;
use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;
use std::collections::{HashMap, hash_map::Entry};
use std::hash::Hash;

use crate::analyzer::TextAnalyzer;
use crate::data_models::InvertedIndexDoc;
use crate::db::Database;
use crate::db::collections;

pub fn intersect_two_postings<'a, T>(
    posting_list1: &'a [T],
    posting_list2: &'a [T],
    out: &mut Vec<T>,
) where
    T: Ord + Clone,
{
    let (mut p1i, mut p2i) = (0usize, 0usize);
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

/// Represents a positional match between two terms in a document
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PositionalMatch<T> {
    pub doc_id: T,
    pub position1: usize,
    pub position2: usize,
}

impl<T> PositionalMatch<T> {
    pub fn new(doc_id: T, position1: usize, position2: usize) -> Self {
        Self {
            doc_id,
            position1,
            position2,
        }
    }
}

pub fn positional_intersect<T>(
    pl1: &Vec<T>,
    positions_pl1: &HashMap<T, Vec<usize>>,
    pl2: &Vec<T>,
    positions_pl2: &HashMap<T, Vec<usize>>,
    k: usize,
) -> Vec<PositionalMatch<T>>
where
    T: Ord + Clone + Hash,
{
    let mut out = Vec::new();
    let mut p1 = 0;
    let mut p2 = 0;
    while p1 < pl1.len() && p2 < pl2.len() {
        if pl1[p1] == pl2[p2] {
            let mut l = Vec::new();
            let positions_p1 = positions_pl1.get(&pl1[p1]).unwrap();
            let positions_p2 = positions_pl2.get(&pl2[p2]).unwrap();
            let mut pp1 = 0;
            while pp1 < positions_p1.len() {
                let mut pp2 = 0;
                while pp2 < positions_p2.len() {
                    if positions_p1[pp1].abs_diff(positions_p2[pp2]) <= k {
                        l.push(positions_p2[pp2]);
                    } else if positions_p2[pp2] > positions_p1[pp1] {
                        // nothing in positions_p2 can ever be smallers than k distance
                        break;
                    }
                    pp2 += 1;
                }
                while !l.is_empty() && l[0].abs_diff(positions_p1[pp1]) > k {
                    l.remove(0); // NOTE: this is not the most efficient way to do this
                }
                for pos in &l {
                    out.push(PositionalMatch::new(
                        pl1[p1].clone(),
                        positions_p1[pp1],
                        *pos,
                    ));
                }
                pp1 += 1;
            }
            p1 += 1;
            p2 += 1;
        } else {
            if pl1[p1] < pl2[p2] {
                p1 += 1;
            } else {
                p2 += 1;
            }
        }
    }
    out
}

pub struct QueryEngine {
    db: Database,
    analyzer: TextAnalyzer,
}

impl QueryEngine {
    pub fn new(db: Database, analyzer: TextAnalyzer) -> Self {
        Self { db, analyzer }
    }

    pub fn db(&self) -> &Database {
        &self.db
    }

    pub fn analyzer(&self) -> &TextAnalyzer {
        &self.analyzer
    }

    fn intersect_postings<T>(posting_lists: &[(&[T], &HashMap<T, Vec<usize>>)]) -> Vec<T>
    where
        T: Ord + Clone + Hash,
    {
        if posting_lists.is_empty() {
            return Vec::new();
        }
        let mut smallest_idx = 0usize;
        for (idx, (pl, _)) in posting_lists.iter().enumerate() {
            if pl.len() < posting_lists[smallest_idx].0.len() {
                smallest_idx = idx;
            }
        }
        let mut result = posting_lists[smallest_idx].0.to_vec();
        let mut result_positions = posting_lists[smallest_idx].1.clone();

        for (idx, (pl, positions)) in posting_lists.iter().enumerate() {
            if idx == smallest_idx {
                continue;
            }

            // Get positional matches between current result and this posting list
            let matches =
                positional_intersect(&result, &result_positions, &pl.to_vec(), &positions, 1);

            // Early exit if no matches
            if matches.is_empty() {
                return Vec::new();
            }

            // Build new result and positions from the matches
            // Group matches by doc_id and collect position2 values
            let mut new_positions: HashMap<T, Vec<usize>> = HashMap::new();
            for m in &matches {
                new_positions
                    .entry(m.doc_id.clone())
                    .or_insert_with(Vec::new)
                    .push(m.position2);
            }

            // Extract unique doc_ids (in sorted order to maintain consistency)
            result = new_positions.keys().cloned().collect();
            result.sort();

            // Update result_positions for next iteration
            result_positions = new_positions;
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
        println!("DEBUG, query: terms, {:?}", terms);
        if terms.is_empty() {
            return Ok(Vec::new());
        }
        let i_index = self.db.collection::<InvertedIndexDoc>(collections::INDEX);
        let filter = doc! {
            "term": {
                "$in": terms.clone()
            }
        };
        let options = mongodb::options::FindOptions::builder()
            .sort(doc! {"bucket": 1})
            .build();

        let index_docs: Vec<InvertedIndexDoc> = i_index
            .find(filter)
            .with_options(options)
            .await?
            .try_collect()
            .await?;
       println!("DEBUG, query result terms");
        for d in &index_docs {
            print!("{:?}, ", d.term);
        }
        println!("");
        println!("-----------------");

        let mut term_posting_and_positions: HashMap<
            String,
            (Vec<ObjectId>, HashMap<ObjectId, Vec<usize>>),
        > = HashMap::new();

        for doc in index_docs {
            match term_posting_and_positions.entry(doc.term.clone()) {
                Entry::Occupied(mut entry) => {
                    let (postings, positions) = entry.get_mut();
                    // extend works here because a `doc` will have unique postings and for those postings, it will have positions map.
                    postings.extend(doc.postings);
                    positions.extend(doc.positions);
                }
                Entry::Vacant(entry) => {
                    entry.insert((doc.postings, doc.positions));
                }
            }
        }

        if term_posting_and_positions.len() != terms.len() {
            return Ok(Vec::new());
        }

        let mut posting_lists: Vec<(&[ObjectId], &HashMap<ObjectId, Vec<usize>>)> = Vec::new();
        for term in &terms { // process the terms in query order.
            let (pl, pos) = term_posting_and_positions.get(term).unwrap();
            posting_lists.push((pl.as_slice(), pos));
        }
        let result = Self::intersect_postings(&posting_lists);
        Ok(result)
    }
}

#[cfg(test)]
mod test {
    use super::*;

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

    // #[test]
    // fn test_intersect_postings_edgy_multilist_cascade() {
    //     {
    //         let l1 = vec![-10, -5, 0, 2, 3, 5, 8, 13, 21, 34];
    //         let l2 = vec![0, 2, 3, 5, 5, 8, 34, 55];
    //         let l3 = vec![2, 3, 8]; // <-- smallest
    //         let l4 = vec![-999, 2, 3, 8, 999999];
    //         let l5 = vec![2, 3, 8, 100];

    //         // Across all lists:
    //         // 2 appears: l1, l2, l3, l4, l5 => min = 1
    //         // 3 appears: l1(1), l2(1), l3(1), l4(1), l5(1) => min = 1
    //         // 8 appears: l1(1), l2(1), l3(2), l4(1), l5(3) => min = 1
    //         let expected = vec![2, 3, 8];

    //         // You likely have this as a method; here I’ll call a free helper for illustration.
    //         // Replace `intersect_postings_impl(...)` with `engine.intersect_postings(...)`.
    //         let got = QueryEngine::intersect_postings(&[&l1, &l2, &l3, &l4, &l5]);

    //         assert_eq!(got, expected);
    //     }
    //     // Smallest list is in the middle, intersection shrinks gradually
    //     {
    //         let l1 = vec![1u32, 2, 3, 4, 5, 8, 13, 21, 34, 55];
    //         let l2 = vec![2u32, 3, 5, 8, 13, 34, 89];
    //         let l3 = vec![3u32, 8, 34]; // smallest (3)
    //         let l4 = vec![0u32, 3, 8, 34, 144, 233];

    //         let expected = vec![3u32, 8, 34];
    //         let got = QueryEngine::intersect_postings(&[&l1, &l2, &l3, &l4]);
    //         assert_eq!(got, expected);
    //     }
    //     // Tie for smallest (two lists same min length); should still intersect correctly
    //     {
    //         let l1 = vec![10u32, 20, 30, 40, 50];
    //         let l2 = vec![20u32, 40]; // smallest tie
    //         let l3 = vec![0u32, 20, 40, 60];
    //         let l4 = vec![20u32, 40]; // smallest tie
    //         let l5 = vec![20u32, 70, 80, 90];

    //         let expected = vec![20u32];
    //         let got = QueryEngine::intersect_postings(&[&l1, &l2, &l3, &l4, &l5]);
    //         assert_eq!(got, expected);
    //     }
    //     // Contains an empty list => empty result (also tests early-break)
    //     {
    //         let l1 = vec![1u32, 2, 3, 4, 5];
    //         let l2: Vec<u32> = vec![];
    //         let l3 = vec![2u32, 3, 4];

    //         let got = QueryEngine::intersect_postings(&[&l1, &l2, &l3]);
    //         assert!(got.is_empty());
    //     }
    //     // Single list input: intersection == that list (identity)
    //     {
    //         let l1 = vec![7u32, 9, 11, 13];
    //         let got = QueryEngine::intersect_postings(&[&l1]);
    //         assert_eq!(got, l1);
    //     }

    //     // Disjoint after first/second intersection => becomes empty mid-way
    //     {
    //         let l1 = vec![1u32, 2, 3, 4, 5, 6, 7, 8];
    //         let l2 = vec![2u32, 4, 6, 8]; // intersection would be [2,4,6,8]
    //         let l3 = vec![1u32, 3, 5, 7]; // kills it to []
    //         let l4 = vec![2u32, 4, 6, 8, 10, 12]; // should never matter after empty

    //         let got = QueryEngine::intersect_postings(&[&l1, &l2, &l3, &l4]);
    //         assert!(got.is_empty());
    //     }

    //     // Huge gaps / big IDs: stresses pointer advancement (no negatives, no duplicates)
    //     {
    //         let l1 = vec![1u32, 10, 1_000, 1_000_000, 4_000_000_000u32];
    //         let l2 = vec![0u32, 10, 999, 1_000_000, 3_000_000_000u32, 4_000_000_000u32];
    //         let l3 = vec![10u32, 1_000_000, 4_000_000_000u32];

    //         let expected = vec![10u32, 1_000_000, 4_000_000_000u32];
    //         let got = QueryEngine::intersect_postings(&[&l1, &l2, &l3]);
    //         assert_eq!(got, expected);
    //     }
    // }
    // POSITIONAL INTERSECTION TEST CASES
    use std::collections::HashMap;
    use std::hash::Hash;

    // Compare as unordered sets (since output ordering isn't a stable contract).
    fn assert_unordered_eq<T: Ord + std::fmt::Debug>(mut a: Vec<T>, mut b: Vec<T>) {
        a.sort();
        b.sort();
        assert_eq!(a, b);
    }

    fn hm<K: Eq + Hash>(pairs: Vec<(K, Vec<usize>)>) -> HashMap<K, Vec<usize>> {
        pairs.into_iter().collect()
    }

    fn run(
        pl1: Vec<u32>,
        pos1: HashMap<u32, Vec<usize>>,
        pl2: Vec<u32>,
        pos2: HashMap<u32, Vec<usize>>,
        k: usize,
    ) -> Vec<PositionalMatch<u32>> {
        positional_intersect(&pl1, &pos1, &pl2, &pos2, k)
    }

    // ----------------------------
    // Contract: postings sorted, unique docIDs; positions sorted, unique
    // ----------------------------

    #[test]
    fn empty_both_lists() {
        let out = run(vec![], HashMap::new(), vec![], HashMap::new(), 1);
        assert!(out.is_empty());
    }

    #[test]
    fn empty_one_side() {
        let out = run(
            vec![1, 2],
            hm(vec![(1, vec![1]), (2, vec![2])]),
            vec![],
            HashMap::new(),
            1,
        );
        assert!(out.is_empty());

        let out = run(
            vec![],
            HashMap::new(),
            vec![1, 2],
            hm(vec![(1, vec![1]), (2, vec![2])]),
            1,
        );
        assert!(out.is_empty());
    }

    #[test]
    fn no_common_docids() {
        let out = run(
            vec![1, 3, 5],
            hm(vec![(1, vec![1]), (3, vec![3]), (5, vec![5])]),
            vec![2, 4, 6],
            hm(vec![(2, vec![2]), (4, vec![4]), (6, vec![6])]),
            1,
        );
        assert!(out.is_empty());
    }

    #[test]
    fn common_docid_but_one_positions_list_empty() {
        let out = run(
            vec![7],
            hm(vec![(7, vec![])]),
            vec![7],
            hm(vec![(7, vec![1, 2, 3])]),
            2,
        );
        assert!(out.is_empty());
    }

    #[test]
    fn common_docid_no_position_match() {
        let out = run(
            vec![1],
            hm(vec![(1, vec![10, 20])]),
            vec![1],
            hm(vec![(1, vec![100, 200])]),
            5,
        );
        assert!(out.is_empty());
    }

    // ----------------------------
    // Unordered NEAR-k semantics: abs_diff <= k
    // ----------------------------

    #[test]
    fn k0_only_exact_same_positions_match() {
        let out = run(
            vec![9],
            hm(vec![(9, vec![1, 5, 10])]),
            vec![9],
            hm(vec![(9, vec![2, 5, 7])]),
            0,
        );
        let expected = vec![PositionalMatch::new(9, 5, 5)];
        assert_eq!(out, expected);
    }

    #[test]
    fn single_position_matches_both_sides_unordered() {
        // p1=10 matches p2=9 and p2=11 for k=1
        let out = run(
            vec![1],
            hm(vec![(1, vec![10])]),
            vec![1],
            hm(vec![(1, vec![9, 11, 13])]),
            1,
        );

        let expected = vec![
            PositionalMatch::new(1, 10, 9),
            PositionalMatch::new(1, 10, 11),
        ];
        assert_unordered_eq(out, expected);
    }

    #[test]
    fn multiple_positions_multiple_matches_same_doc() {
        // p1=[5,10], p2=[4,6,9,12], k=1
        // matches: (5,4), (5,6), (10,9)
        let out = run(
            vec![7],
            hm(vec![(7, vec![5, 10])]),
            vec![7],
            hm(vec![(7, vec![4, 6, 9, 12])]),
            1,
        );

        let expected = vec![
            PositionalMatch::new(7, 5, 4),
            PositionalMatch::new(7, 5, 6),
            PositionalMatch::new(7, 10, 9),
        ];
        assert_unordered_eq(out, expected);
    }

    #[test]
    fn k_boundary_inclusive() {
        // exactly k distance should match
        let out = run(
            vec![4],
            hm(vec![(4, vec![10])]),
            vec![4],
            hm(vec![(4, vec![12])]),
            2,
        );
        let expected = vec![PositionalMatch::new(4, 10, 12)];
        assert_eq!(out, expected);
    }

    #[test]
    fn k_large_produces_many_pairs_but_only_within_k() {
        // k=100
        // p1=[10,200], p2=[50,150,260]
        // pairs: (10,50), (200,150), (200,260)
        let out = run(
            vec![4],
            hm(vec![(4, vec![10, 200])]),
            vec![4],
            hm(vec![(4, vec![50, 150, 260])]),
            100,
        );

        let expected = vec![
            PositionalMatch::new(4, 10, 50),
            PositionalMatch::new(4, 200, 150),
            PositionalMatch::new(4, 200, 260),
        ];
        assert_unordered_eq(out, expected);
    }

    // ----------------------------
    // Multiple docs: merge docIDs + match positions only where docIDs overlap
    // ----------------------------

    #[test]
    fn partial_docid_overlap_multiple_docs() {
        // overlap only on docs 2 and 4
        let out = run(
            vec![1, 2, 4],
            hm(vec![(1, vec![1, 2]), (2, vec![1, 10]), (4, vec![100])]),
            vec![2, 3, 4],
            hm(vec![(2, vec![2, 9]), (3, vec![1]), (4, vec![98, 101, 105])]),
            1,
        );

        // doc 2: (1,2) diff1; (10,9) diff1
        // doc 4: (100,101) diff1 (98 is diff2, 105 diff5)
        let expected = vec![
            PositionalMatch::new(2, 1, 2),
            PositionalMatch::new(2, 10, 9),
            PositionalMatch::new(4, 100, 101),
        ];
        assert_unordered_eq(out, expected);
    }

    #[test]
    fn full_docid_overlap_some_docs_match_some_dont() {
        // docs 1,2,3 overlap; only doc2 has near matches
        let out = run(
            vec![1, 2, 3],
            hm(vec![(1, vec![1]), (2, vec![10, 30]), (3, vec![100])]),
            vec![1, 2, 3],
            hm(vec![(1, vec![50]), (2, vec![9, 29, 31]), (3, vec![1000])]),
            1,
        );

        // doc2: (10,9) diff1; (30,29) diff1; (30,31) diff1
        let expected = vec![
            PositionalMatch::new(2, 10, 9),
            PositionalMatch::new(2, 30, 29),
            PositionalMatch::new(2, 30, 31),
        ];
        assert_unordered_eq(out, expected);
    }

    // ----------------------------
    // out parameter behavior
    // ----------------------------

    // #[test]
    // fn out_is_appended_not_cleared() {
    //     let pl1 = vec![1u32];
    //     let pl2 = vec![1u32];
    //     let pos1 = hm(vec![(1u32, vec![5])]);
    //     let pos2 = hm(vec![(1u32, vec![6])]);

    //     let mut out = vec![PositionalMatch::new(999u32, 0usize, 0usize)];
    //      positional_intersect(&pl1, &pos1, &pl2, &pos2, 1, &mut out);

    //     assert!(out.contains(&PositionalMatch::new(999u32, 0, 0)));
    //     assert!(out.contains(&PositionalMatch::new(1u32, 5, 6)));
    //     assert_eq!(out.len(), 2);
    // }

    // ----------------------------
    // Invalid input should panic (current implementation uses unwrap())
    // ----------------------------

    #[test]
    #[should_panic]
    fn missing_positions_map_entry_panics_pl1() {
        let _out = run(
            vec![1u32],
            HashMap::new(), // missing key 1
            vec![1u32],
            hm(vec![(1u32, vec![2])]),
            1,
        );
    }

    #[test]
    #[should_panic]
    fn missing_positions_map_entry_panics_pl2() {
        let _out = run(
            vec![1u32],
            hm(vec![(1u32, vec![2])]),
            vec![1u32],
            HashMap::new(), // missing key 1
            1,
        );
    }

    #[test]
    fn pp2_len_off_by_one_panics_until_fixed() {
        let _out = run(
            vec![1u32],
            hm(vec![(1u32, vec![100])]),
            vec![1u32],
            hm(vec![(1u32, vec![1])]),
            1,
        );
    }

    // ----------------------------
    // Sanity / stress (small) to ensure no weirdness with many positions
    // (Will also panic until <= bug is fixed, so keep it ignored until then.)
    // ----------------------------

    #[test]
    #[ignore]
    fn stress_many_positions_no_panic_after_fix() {
        let p1: Vec<usize> = (0..200).collect();
        let p2: Vec<usize> = (0..200).collect();

        let out = run(
            vec![1u32],
            hm(vec![(1u32, p1)]),
            vec![1u32],
            hm(vec![(1u32, p2)]),
            2,
        );
        assert!(!out.is_empty());
    }
}
