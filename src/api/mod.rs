use axum::{Router, routing::post};
use std::sync::Arc;
use tower_http::{
    cors::{Any, CorsLayer},
    services::ServeDir,
};

use crate::query_engine::QueryEngine;

pub mod handlers;
pub mod models;

pub fn create_router(query_engine: Arc<QueryEngine>) -> Router {
    // CORS configuration
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        // API routes
        .route("/api/search", post(handlers::search_handler))
        .with_state(query_engine)
        // Static file serving for the UI
        .nest_service("/", ServeDir::new("static"))
        .layer(cors)
}
