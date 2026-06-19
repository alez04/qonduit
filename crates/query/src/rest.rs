//! REST API endpoints.
//!
//! These are Qonduit-native endpoints (not Bob-compatible).
//! All return JSON responses.

use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Instant;

use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json,
};
use qonduit_core::identity;

use crate::AppState;

/// Server start time for uptime tracking.
static START_TIME: OnceLock<Instant> = OnceLock::new();

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
        .route("/metrics", get(metrics))
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

async fn metrics() -> impl IntoResponse {
    crate::metrics::REST_REQUESTS.inc();
    let body = crate::metrics::render_metrics();
    (
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

async fn health() -> impl IntoResponse {
    let start = START_TIME.get_or_init(Instant::now);
    let uptime_secs = start.elapsed().as_secs();
    Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_seconds": uptime_secs,
    }))
}

async fn system_info(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    crate::metrics::REST_REQUESTS.inc();
    let tick = match state.storage.get_current_tick() {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("Failed to read current tick: {e}")})),
            )
                .into_response();
        }
    };
    let epoch = match state.storage.get_current_epoch() {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("Failed to read current epoch: {e}")})),
            )
                .into_response();
        }
    };
    let info = serde_json::json!({
        "currentTick": tick,
        "currentEpoch": epoch,
        "version": env!("CARGO_PKG_VERSION"),
    });
    Json(info).into_response()
}

async fn current_tick(State(state): State<Arc<AppState>>) -> Response {
    crate::metrics::REST_REQUESTS.inc();
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
    crate::metrics::REST_REQUESTS.inc();
    match state.storage.get_tick(tick) {
        Ok(data) => json_or_404(data),
        Err(e) => storage_err(e),
    }
}

async fn get_tick_transactions(
    State(state): State<Arc<AppState>>,
    Path(tick): Path<u32>,
) -> impl IntoResponse {
    crate::metrics::REST_REQUESTS.inc();
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
    crate::metrics::REST_REQUESTS.inc();
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
    crate::metrics::REST_REQUESTS.inc();
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
    crate::metrics::REST_REQUESTS.inc();
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
    crate::metrics::REST_REQUESTS.inc();
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
    crate::metrics::REST_REQUESTS.inc();
    match state.storage.get_computors(epoch) {
        Ok(data) => json_or_404(data),
        Err(e) => storage_err(e),
    }
}

async fn get_issued_assets(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    crate::metrics::REST_REQUESTS.inc();
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
    Path(id): Path<String>,
) -> Response {
    crate::metrics::REST_REQUESTS.inc();
    let _key = match identity::decode_base26(&id) {
        Some(k) => k,
        None => return (StatusCode::BAD_REQUEST, "Invalid identity").into_response(),
    };
    // TODO: Need entity->assets index in storage to return actual owned assets.
    // For now, return an empty array as graceful degradation.
    Json(serde_json::json!([])).into_response()
}

async fn get_possessed_assets(
    State(_state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    crate::metrics::REST_REQUESTS.inc();
    let _key = match identity::decode_base26(&id) {
        Some(k) => k,
        None => return (StatusCode::BAD_REQUEST, "Invalid identity").into_response(),
    };
    // TODO: Need entity->possessed assets index in storage to return actual possessed assets.
    // For now, return an empty array as graceful degradation.
    Json(serde_json::json!([])).into_response()
}

async fn get_asset(
    State(state): State<Arc<AppState>>,
    Path(index): Path<u32>,
) -> Response {
    crate::metrics::REST_REQUESTS.inc();
    match state.storage.get_asset(index) {
        Ok(data) => json_or_404(data),
        Err(e) => storage_err(e),
    }
}

async fn get_contract_ipo(
    State(state): State<Arc<AppState>>,
    Path(index): Path<u32>,
) -> Response {
    crate::metrics::REST_REQUESTS.inc();
    match state.storage.get_contract_ipo(index) {
        Ok(data) => json_or_404(data),
        Err(e) => storage_err(e),
    }
}

async fn get_entity_transactions(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    crate::metrics::REST_REQUESTS.inc();
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

async fn search(
    State(state): State<Arc<AppState>>,
    Path(query): Path<String>,
) -> Response {
    crate::metrics::REST_REQUESTS.inc();
    let q = query.trim().to_string();

    if q.is_empty() {
        return (StatusCode::BAD_REQUEST, "Empty search query").into_response();
    }

    // If it looks like a tick number (all digits), redirect to tick data
    if q.chars().all(|c| c.is_ascii_digit()) {
        if let Ok(tick) = q.parse::<u32>() {
            return match state.storage.get_tick(tick) {
                Ok(data) => json_or_404(data),
                Err(e) => storage_err(e),
            };
        }
    }

    // If it looks like a hex hash (64 hex chars), redirect to transaction lookup
    if q.len() == 64 && q.chars().all(|c| c.is_ascii_hexdigit()) {
        if let Ok(bytes) = hex::decode(&q) {
            if bytes.len() == 32 {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes);
                return match state.storage.get_tx(&arr) {
                    Ok(data) => json_or_404(data),
                    Err(e) => storage_err(e),
                };
            }
        }
    }

    // TODO: Full-text search index not yet available; return empty results.
    Json(serde_json::json!([])).into_response()
}
