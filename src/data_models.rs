use std::collections::HashMap;

use mongodb::bson::{DateTime, oid::ObjectId};
use serde::{Deserialize, Serialize};

// Helper module for serializing HashMap<ObjectId, T> as HashMap<String, T>
mod objectid_hashmap_serde {
    use super::*;
    use serde::{Deserializer, Serializer};
    use std::collections::HashMap;

    pub fn serialize<S, T>(map: &HashMap<ObjectId, T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Serialize,
    {
        let string_map: HashMap<String, &T> = map.iter().map(|(k, v)| (k.to_hex(), v)).collect();
        string_map.serialize(serializer)
    }

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<HashMap<ObjectId, T>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        let string_map: HashMap<String, T> = HashMap::deserialize(deserializer)?;
        string_map
            .into_iter()
            .map(|(k, v)| {
                ObjectId::parse_str(&k)
                    .map(|oid| (oid, v))
                    .map_err(serde::de::Error::custom)
            })
            .collect()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Page {
    #[serde(rename = "_id")]
    pub id: ObjectId,

    pub url: String,
    pub title: String,
    pub html_body: String,
    pub cleaned_content: String,
    pub outgoing_links: Vec<String>,
    pub depth: u32,
    pub is_seed: bool,
    pub crawled_at: DateTime,
    #[serde(default)]
    pub indexed: bool,
}

impl Page {
    pub fn new(
        url: String,
        title: String,
        html_body: String,
        outgoing_links: Vec<String>,
        depth: u32,
        is_seed: bool,
    ) -> Page {
        Page {
            id: ObjectId::new(),
            url,
            title,
            html_body,
            outgoing_links,
            cleaned_content: "".to_string(), // will be populated later by scrapper.
            depth,
            is_seed,
            crawled_at: DateTime::now(),
            indexed: false,
        }
    }
}

// TODO: add schema for holding both the postings list and positions per doc for the term.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SpimiDoc {
    #[serde(rename = "_id")]
    pub id: ObjectId,
    pub term: String,
    pub bucket: i16,
    pub document_frequency: u64,
    pub postings: Vec<ObjectId>,
    #[serde(with = "objectid_hashmap_serde")]
    pub positions: HashMap<ObjectId, Vec<usize>>,
}

impl SpimiDoc {
    pub fn new(
        term: String,
        bucket: i16,
        document_frequency: u64,
        postings: Vec<ObjectId>,
        positions: HashMap<ObjectId, Vec<usize>>,
    ) -> SpimiDoc {
        SpimiDoc {
            id: ObjectId::new(),
            term,
            bucket,
            document_frequency,
            postings,
            positions,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InvertedIndexDoc {
    #[serde(rename = "_id")]
    pub id: ObjectId,
    pub term: String,
    pub bucket: i16,
    pub document_frequency: u64,
    pub postings: Vec<ObjectId>,
    #[serde(with = "objectid_hashmap_serde")]
    pub positions: HashMap<ObjectId, Vec<usize>>,
}

impl InvertedIndexDoc {
    pub fn new(
        term: String,
        bucket: i16,
        document_frequency: u64,
        postings: Vec<ObjectId>,
        positions: HashMap<ObjectId, Vec<usize>>,
    ) -> InvertedIndexDoc {
        InvertedIndexDoc {
            id: ObjectId::new(),
            bucket,
            term,
            postings,
            document_frequency,
            positions,
        }
    }
}

/// Checkpoint for tracking merge progress to enable resumption after crashes.
/// Stores the last successfully merged term + bucket per SPIMI block collection.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MergeCheckpoint {
    #[serde(rename = "_id")]
    pub id: ObjectId,
    /// Name of the SPIMI block collection being merged
    pub collection_name: String,
    /// Last term that was successfully flushed to inverted_index
    pub last_merged_term: Option<String>,
    /// Bucket number for the last term (for multi-bucket terms)
    pub last_merged_bucket: i16,
    /// Timestamp of last update
    pub updated_at: DateTime,
    /// Whether this block has been fully merged
    pub completed: bool,
}

impl MergeCheckpoint {
    pub fn new(collection_name: String) -> Self {
        Self {
            id: ObjectId::new(),
            collection_name,
            last_merged_term: None,
            last_merged_bucket: -1,
            updated_at: DateTime::now(),
            completed: false,
        }
    }
}
