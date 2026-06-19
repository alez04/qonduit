//! REST API endpoints.
//!
//! These are Qonduit-native endpoints (not Bob-compatible).
//! All return JSON responses.

use std::sync::Arc;

use axum::{
    Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use serde::Deserialize;

use crate::AppState;

/// Build REST routes.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/health", get(health))
        .route("/system-info", get(system_info))
        .route("/v1/tick", get(current_tick))
        .route("/v1/tick/{tick}", get(get_tick))
        .route("/v1/tick/{tick}/tx", get(get_tick_transactions))
        .route("/v1/tx/{hash}", get(get_transaction))
        .route("/v1/entity/{id}", get(get_entity))
        .route("/v1/spectrum/{id}", get(get_spectrum_entry))
        .route("/v1/computors", get(get_computors))
        .route("/v1/computors/{epoch}", get(get_computors_epoch))
        .route("/v1/issued-assets", get(get_issued_assets))
        .route("/v1/owned-assets/{id}", get(get_owned_assets))
        .route("/v1/possessed-assets/{id}", get(get_possessed_assets))
        .route("/v1/assets/{index}", get(get_asset))
        .route("/v1/contract-ipo/{index}", get(get_contract_ipo))
        .route("/v1/entity/{id}/transactions", get(get_entity_transactions))
        .route("/v1/search/{query}", get(search))
}

// --- Handlers ---

async fn health() -> impl IntoResponse {
    StatusCode::OK
}

async fn system_info(State(_state): State<Arc<AppState>>) -> impl IntoResponse {
    // TODO: Return system info from storage
    StatusCode::NOT_IMPLEMENTED
}

async fn current_tick(State(_state): State<Arc<AppState>>) -> impl IntoResponse {
    // TODO: Return current tick from storage
    StatusCode::NOT_IMPLEMENTED
}

async fn get_tick(
    State(_state): State<Arc<AppState>>,
    Path(_tick): Path<u32>,
) -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn get_tick_transactions(
    State(_state): State<Arc<AppState>>,
    Path(_tick): Path<u32>,
) -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn get_transaction(
    State(_state): State<Arc<AppState>>,
    Path(_hash): Path<String>,
) -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn get_entity(
    State(_state): State<Arc<AppState>>,
    Path(_id): Path<String>,
) -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn get_spectrum_entry(
    State(_state): State<Arc<AppState>>,
    Path(_id): Path<String>,
) -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn get_computors(State(_state): State<Arc<AppState>>) -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn get_computors_epoch(
    State(_state): State<Arc<AppState>>,
    Path(_epoch): Path<u16>,
) -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn get_issued_assets(State(_state): State<Arc<AppState>>) -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn get_owned_assets(
    State(_state): State<Arc<AppState>>,
    Path(_id): Path<String>,
) -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn get_possessed_assets(
    State(_state): State<Arc<AppState>>,
    Path(_id): Path<String>,
) -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn get_asset(
    State(_state): State<Arc<AppState>>,
    Path(_index): Path<u32>,
) -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn get_contract_ipo(
    State(_state): State<Arc<AppState>>,
    Path(_index): Path<u32>,
) -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

async fn get_entity_transactions(
    State(_state): State<Arc<AppState>>,
    Path(_id): Path<String>,
) -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

#[derive(Deserialize)]
struct SearchParams {
    #[allow(dead_code)]
    q: Option<String>,
}

async fn search(
    State(_state): State<Arc<AppState>>,
    Query(_params): Query<SearchParams>,
) -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}
