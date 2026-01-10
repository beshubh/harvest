use axum::{Json, extract::State, http::StatusCode};
use futures::TryStreamExt;
use mongodb::bson::doc;
use std::sync::Arc;
use std::time::Instant;

use crate::data_models::Page;
use crate::db::collections;
use crate::query_engine::QueryEngine;

use super::models::{PageResult, SearchRequest, SearchResponse};

pub async fn search_handler(
    State(query_engine): State<Arc<QueryEngine>>,
    Json(request): Json<SearchRequest>,
) -> Result<Json<SearchResponse>, (StatusCode, String)> {
    let start = Instant::now();

    if request.query.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "Query cannot be empty".to_string()));
    }

    let highlighted_terms: Vec<String> = request
        .query
        .split_whitespace()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let document_ids = query_engine.query(&request.query).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Search error: {}", e),
        )
    })?;

    // Fetch full page documents for the matching IDs
    let pages_collection = query_engine.db().collection::<Page>(collections::PAGES);

    let filter = doc! {
        "_id": {
            "$in": document_ids.clone()
        }
    };

    let pages: Vec<Page> = pages_collection
        .find(filter)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
        })?
        .try_collect()
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            )
        })?;

    // Convert pages to results
    let results: Vec<PageResult> = pages
        .into_iter()
        .map(|page| {
            // Create a snippet from cleaned content, or fall back to html_body
            let content = if !page.cleaned_content.is_empty() {
                page.cleaned_content.clone()
            } else {
                page.html_body.clone()
            };

            let snippet = if content.len() > 200 {
                format!("{}...", &content[..200])
            } else {
                content
            };

            PageResult {
                id: page.id.to_hex(),
                title: page.title,
                url: page.url,
                snippet,
                depth: page.depth,
            }
        })
        .collect();

    let total_results = results.len();
    let processing_time_ms = start.elapsed().as_millis();

    Ok(Json(SearchResponse {
        query: request.query,
        results,
        total_results,
        processing_time_ms,
        highlighted_terms,
    }))
}
