use mongodb::bson::{DateTime, oid::ObjectId};
use serde::{Deserialize, Serialize};

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
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SpimiDoc {
    #[serde(rename = "_id")]
    pub id: ObjectId,
    pub term: String,
    pub postings: Vec<ObjectId>,
}

impl SpimiDoc {
    pub fn new(term: String, postings: Vec<ObjectId>) -> SpimiDoc {
        SpimiDoc {
            id: ObjectId::new(),
            term,
            postings,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InvertedIndexDoc {
    #[serde(rename = "_id")]
    pub id: ObjectId,
    term: String,
    bucket: i16,
    postings: Vec<ObjectId>,
}

impl InvertedIndexDoc {
    pub fn new(term: String, bucket: i16, postings: Vec<ObjectId>) -> InvertedIndexDoc {
        InvertedIndexDoc {
            id: ObjectId::new(),
            bucket,
            term,
            postings,
        }
    }
}
