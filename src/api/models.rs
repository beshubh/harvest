use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct SearchRequest {
    pub query: String,
}

#[derive(Debug, Serialize)]
pub struct SearchResponse {
    pub query: String,
    pub results: Vec<PageResult>,
    pub total_results: usize,
    pub processing_time_ms: u128,
}

#[derive(Debug, Serialize)]
pub struct PageResult {
    pub id: String,
    pub title: String,
    pub url: String,
    pub snippet: String,
    pub depth: u32,
}
