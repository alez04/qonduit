//! JSON-RPC 2.0 endpoint.
//!
//! Implements the full Bob-compatible API as a superset, plus
//! native `qonduit_*` methods.

use std::sync::Arc;

use axum::{
    Router,
    extract::State,
    response::IntoResponse,
    routing::post,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::AppState;

/// JSON-RPC 2.0 request.
#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    pub params: Option<serde_json::Value>,
    pub id: Option<serde_json::Value>,
}

/// JSON-RPC 2.0 response.
#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
    pub id: Option<serde_json::Value>,
}

/// JSON-RPC 2.0 error object.
#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// Build JSON-RPC routes.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/rpc", post(handle_rpc))
        .route("/json-rpc", post(handle_rpc))
}

async fn handle_rpc(
    State(_state): State<Arc<AppState>>,
    Json(request): Json<JsonRpcRequest>,
) -> impl IntoResponse {
    let id = request.id.clone();

    match request.method.as_str() {
        // --- Bob-compatible methods (27 total) ---
        "getTickInfo" => not_implemented(id),
        "getEntity" => not_implemented(id),
        "getBalance" => not_implemented(id),
        "getTransactionsForTick" => not_implemented(id),
        "getTransaction" => not_implemented(id),
        "getBlock" => not_implemented(id),
        "getQubicInfo" => not_implemented(id),
        "getSystemInfo" => not_implemented(id),
        "getCurrentTickInfo" => not_implemented(id),
        "getComputors" => not_implemented(id),
        "getContractIPO" => not_implemented(id),
        "getIssuedAssets" => not_implemented(id),
        "getOwnedAssets" => not_implemented(id),
        "getPossessedAssets" => not_implemented(id),
        "getAssetsByOwner" => not_implemented(id),
        "getSpectrumStats" => not_implemented(id),
        "getSpectrum" => not_implemented(id),
        "getTickTransactions" => not_implemented(id),
        "getProposal" => not_implemented(id),
        "getBallot" => not_implemented(id),
        "getVotesForProposal" => not_implemented(id),
        "getVotesForVoter" => not_implemented(id),
        "getActiveIPOs" => not_implemented(id),
        "getIPOBids" => not_implemented(id),
        "getContractFunction" => not_implemented(id),
        "getSyncState" => not_implemented(id),
        "getContractFunctionResult" => not_implemented(id),

        // --- Qonduit-native methods ---
        "qonduit_getTick" => not_implemented(id),
        "qonduit_getTickTransactions" => not_implemented(id),
        "qonduit_getEntityActivity" => not_implemented(id),
        "qonduit_search" => not_implemented(id),
        "qonduit_getAssetHolders" => not_implemented(id),
        "qonduit_getCustomMessages" => not_implemented(id),
        "qonduit_getOracleData" => not_implemented(id),
        "qonduit_getEntityTokens" => not_implemented(id),
        "qonduit_getDeFiPositions" => not_implemented(id),
        "qonduit_getEpochInfo" => not_implemented(id),
        "qonduit_getLogEvents" => not_implemented(id),
        "qonduit_getSpectrumChanges" => not_implemented(id),
        "qonduit_getEntityBalances" => not_implemented(id),

        _ => Json(JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code: -32601,
                message: "Method not found".to_string(),
                data: None,
            }),
            id,
        }),
    }
}

fn not_implemented(id: Option<serde_json::Value>) -> Json<JsonRpcResponse> {
    Json(JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        result: None,
        error: Some(JsonRpcError {
            code: -32001,
            message: "Not implemented yet".to_string(),
            data: None,
        }),
        id,
    })
}
