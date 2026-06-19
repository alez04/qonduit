//! REST API endpoints.
//!
//! These are Qonduit-native endpoints (not Bob-compatible).
//! All return JSON responses.

use std::sync::Arc;

use axum::{
    Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json,
};
use serde::Deserialize;
use qonduit_core::identity;

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

// --- Helpers ---

/// Convert a storage error into a 500 response.
fn storage_err(e: anyhow::Error) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("Storage error: {e}")).into_response()
}

/// Return raw JSON bytes with correct content-type, or 404.
fn json_or_404(data: Option<Vec<u8>>) -> Response {
    match data {
        Some(bytes) => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            bytes,
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

// --- Handlers ---

async fn health() -> impl IntoResponse {
    StatusCode::OK
}

async fn system_info(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match (
        state.storage.get_current_tick(),
        state.storage.get_current_epoch(),
    ) {
        (Ok(tick), Ok(epoch)) => {
            let info = serde_json::json!({
                "currentTick": tick,
                "currentEpoch": epoch,
                "version": env!("CARGO_PKG_VERSION"),
            });
            Json(info).into_response()
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn current_tick(State(state): State<Arc<AppState>>) -> Response {
    match state.storage.get_current_tick() {
        Ok(Some(tick)) => match state.storage.get_tick(tick) {
            Ok(data) => json_or_404(data),
            Err(e) => storage_err(e),
        },
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => storage_err(e),
    }
}

async fn get_tick(
    State(state): State<Arc<AppState>>,
    Path(tick): Path<u32>,
) -> Response {
    match state.storage.get_tick(tick) {
        Ok(data) => json_or_404(data),
        Err(e) => storage_err(e),
    }
}

async fn get_tick_transactions(
    State(state): State<Arc<AppState>>,
    Path(tick): Path<u32>,
) -> impl IntoResponse {
    match state.storage.get_tx_hashes_for_tick(tick) {
        Ok(hashes) => {
            let mut txs = Vec::new();
            for hash in hashes {
                if let Ok(Some(data)) = state.storage.get_tx(&hash) {
                    if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&data) {
                        txs.push(val);
                    }
                }
            }
            Json(txs).into_response()
        }
        Err(e) => storage_err(e),
    }
}

async fn get_transaction(
    State(state): State<Arc<AppState>>,
    Path(hash_str): Path<String>,
) -> Response {
    let hash_bytes = match hex::decode(&hash_str) {
        Ok(bytes) if bytes.len() == 32 => {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            arr
        }
        _ => return (StatusCode::BAD_REQUEST, "Invalid transaction hash").into_response(),
    };
    match state.storage.get_tx(&hash_bytes) {
        Ok(data) => json_or_404(data),
        Err(e) => storage_err(e),
    }
}

async fn get_entity(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    let key = match identity::decode_base26(&id) {
        Some(k) => k,
        None => return (StatusCode::BAD_REQUEST, "Invalid identity").into_response(),
    };
    match state.storage.get_entity(&key) {
        Ok(data) => json_or_404(data),
        Err(e) => storage_err(e),
    }
}

async fn get_spectrum_entry(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    let key = match identity::decode_base26(&id) {
        Some(k) => k,
        None => return (StatusCode::BAD_REQUEST, "Invalid identity").into_response(),
    };
    match state.storage.get_spectrum_entry(&key) {
        Ok(data) => json_or_404(data),
        Err(e) => storage_err(e),
    }
}

async fn get_computors(State(state): State<Arc<AppState>>) -> Response {
    match state.storage.get_latest_computors() {
        Ok(Some((_epoch, data))) => json_or_404(Some(data)),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => storage_err(e),
    }
}

async fn get_computors_epoch(
    State(state): State<Arc<AppState>>,
    Path(epoch): Path<u16>,
) -> Response {
    match state.storage.get_computors(epoch) {
        Ok(data) => json_or_404(data),
        Err(e) => storage_err(e),
    }
}

async fn get_issued_assets(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.storage.get_all_assets(1000) {
        Ok(assets) => {
            let items: Vec<serde_json::Value> = assets
                .into_iter()
                .filter_map(|(_, data)| serde_json::from_slice(&data).ok())
                .collect();
            Json(items).into_response()
        }
        Err(e) => storage_err(e),
    }
}

async fn get_owned_assets(
    State(_state): State<Arc<AppState>>,
    Path(_id): Path<String>,
) -> impl IntoResponse {
    // TODO: Need entity->assets index in storage
    StatusCode::NOT_IMPLEMENTED
}

async fn get_possessed_assets(
    State(_state): State<Arc<AppState>>,
    Path(_id): Path<String>,
) -> impl IntoResponse {
    // TODO: Need entity->assets index in storage
    StatusCode::NOT_IMPLEMENTED
}

async fn get_asset(
    State(state): State<Arc<AppState>>,
    Path(index): Path<u32>,
) -> Response {
    match state.storage.get_asset(index) {
        Ok(data) => json_or_404(data),
        Err(e) => storage_err(e),
    }
}

async fn get_contract_ipo(
    State(state): State<Arc<AppState>>,
    Path(index): Path<u32>,
) -> Response {
    match state.storage.get_contract_ipo(index) {
        Ok(data) => json_or_404(data),
        Err(e) => storage_err(e),
    }
}

async fn get_entity_transactions(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let key = match identity::decode_base26(&id) {
        Some(k) => k,
        None => return (StatusCode::BAD_REQUEST, "Invalid identity").into_response(),
    };
    match state.storage.get_tx_hashes_for_entity(&key, 100) {
        Ok(hashes) => {
            let mut txs = Vec::new();
            for hash in hashes {
                if let Ok(Some(data)) = state.storage.get_tx(&hash) {
                    if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&data) {
                        txs.push(val);
                    }
                }
            }
            Json(txs).into_response()
        }
        Err(e) => storage_err(e),
    }
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
    // TODO: Full-text search index
    StatusCode::NOT_IMPLEMENTED
}
