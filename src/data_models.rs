use mongodb::bson::{DateTime, oid::ObjectId};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Page {
    #[serde(rename = "_id")]
    pub id: ObjectId,

    pub url: String,
    pub title: String,
    pub html_body: String,
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
            depth,
            is_seed,
            crawled_at: DateTime::now(),
        }
    }
}
