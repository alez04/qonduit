//! WebSocket subscription endpoints.
//!
//! Supports real-time streaming of blockchain events via WebSocket.
//! Topics: tick, tx, entity, spectrum, custom-message, contract-fn.

use std::sync::Arc;

use axum::{
    Router,
    extract::State,
    routing::get,
};

use crate::AppState;

/// Build WebSocket routes.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/ws/tick", get(ws_tick_placeholder))
        .route("/ws/tx", get(ws_tx_placeholder))
        .route("/ws/entity", get(ws_entity_placeholder))
        .route("/ws/spectrum", get(ws_spectrum_placeholder))
        .route("/ws/custom-message", get(ws_custom_message_placeholder))
        .route("/ws/contract-fn", get(ws_contract_fn_placeholder))
}

// Placeholder handlers — will be replaced with actual WebSocket upgrade logic.

async fn ws_tick_placeholder(
    State(_state): State<Arc<AppState>>,
) -> &'static str {
    "WebSocket endpoint: tick — not yet implemented"
}

async fn ws_tx_placeholder(
    State(_state): State<Arc<AppState>>,
) -> &'static str {
    "WebSocket endpoint: tx — not yet implemented"
}

async fn ws_entity_placeholder(
    State(_state): State<Arc<AppState>>,
) -> &'static str {
    "WebSocket endpoint: entity — not yet implemented"
}

async fn ws_spectrum_placeholder(
    State(_state): State<Arc<AppState>>,
) -> &'static str {
    "WebSocket endpoint: spectrum — not yet implemented"
}

async fn ws_custom_message_placeholder(
    State(_state): State<Arc<AppState>>,
) -> &'static str {
    "WebSocket endpoint: custom-message — not yet implemented"
}

async fn ws_contract_fn_placeholder(
    State(_state): State<Arc<AppState>>,
) -> &'static str {
    "WebSocket endpoint: contract-fn — not yet implemented"
}
