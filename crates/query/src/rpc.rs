use std::sync::Arc;
use axum::{Router, extract::State, routing::post, Json};
use serde::{Deserialize, Serialize};
use qonduit_core::identity;

use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    pub params: Option<serde_json::Value>,
    pub id: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
    pub id: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/rpc", post(handle_rpc))
        .route("/json-rpc", post(handle_rpc))
}

async fn handle_rpc(
    State(state): State<Arc<AppState>>,
    Json(request): Json<JsonRpcRequest>,
) -> Json<JsonRpcResponse> {
    crate::metrics::RPC_REQUESTS.inc();
    let id = request.id.clone();
    let result = dispatch_method(&state, &request.method, request.params.as_ref()).await;
    match result {
        Ok(value) => Json(JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            result: Some(value),
            error: None,
            id,
        }),
        Err(e) => Json(JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code: -32000,
                message: e.to_string(),
                data: None,
            }),
            id,
        }),
    }
}

async fn dispatch_method(
    state: &AppState,
    method: &str,
    params: Option<&serde_json::Value>,
) -> Result<serde_json::Value, anyhow::Error> {
    match method {
        // --- Bob-compatible methods ---
        "getTickInfo" => {
            let tick = state.storage.get_current_tick()?.unwrap_or(0);
            let epoch = state.storage.get_current_epoch()?.unwrap_or(0);
            Ok(serde_json::json!({"tick": tick, "epoch": epoch}))
        }
        "getCurrentTickInfo" => {
            let tick = state.storage.get_current_tick()?.unwrap_or(0);
            let epoch = state.storage.get_current_epoch()?.unwrap_or(0);
            Ok(serde_json::json!({
                "epoch": epoch,
                "tick": tick,
            }))
        }
        "getEntity" => {
            let id = extract_string_param(params, 0)?;
            let key = identity::decode_base26(&id)
                .ok_or_else(|| anyhow::anyhow!("Invalid identity"))?;
            match state.storage.get_entity(&key)? {
                Some(data) => Ok(serde_json::from_slice(&data)?),
                None => Ok(serde_json::json!(null)),
            }
        }
        "getBalance" => {
            let id = extract_string_param(params, 0)?;
            let key = identity::decode_base26(&id)
                .ok_or_else(|| anyhow::anyhow!("Invalid identity"))?;
            match state.storage.get_spectrum_entry(&key)? {
                Some(data) => {
                    let entry: serde_json::Value = serde_json::from_slice(&data)?;
                    Ok(serde_json::json!({"balance": entry.get("balance")}))
                }
                None => Ok(serde_json::json!({"balance": 0})),
            }
        }
        "getTransactionsForTick" | "getTickTransactions" => {
            let tick = extract_u32_param(params, 0)?;
            let hashes = state.storage.get_tx_hashes_for_tick(tick)?;
            let mut txs = Vec::new();
            for hash in hashes {
                if let Some(data) = state.storage.get_tx(&hash)? {
                    if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&data) {
                        txs.push(val);
                    }
                }
            }
            Ok(serde_json::json!(txs))
        }
        "getTransaction" => {
            let hash_str = extract_string_param(params, 0)?;
            let hash_bytes = hex::decode(&hash_str)?;
            if hash_bytes.len() != 32 {
                anyhow::bail!("Invalid hash length");
            }
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&hash_bytes);
            match state.storage.get_tx(&hash)? {
                Some(data) => Ok(serde_json::from_slice(&data)?),
                None => Ok(serde_json::json!(null)),
            }
        }
        "getBlock" => {
            // Bob's getBlock returns tick data
            let tick = extract_u32_param(params, 0)?;
            match state.storage.get_tick(tick)? {
                Some(data) => Ok(serde_json::from_slice(&data)?),
                None => Ok(serde_json::json!(null)),
            }
        }
        "getQubicInfo" => {
            let tick = state.storage.get_current_tick()?.unwrap_or(0);
            let epoch = state.storage.get_current_epoch()?.unwrap_or(0);
            Ok(serde_json::json!({
                "epoch": epoch,
                "tick": tick,
                "version": env!("CARGO_PKG_VERSION"),
            }))
        }
        "getSystemInfo" => {
            let tick = state.storage.get_current_tick()?.unwrap_or(0);
            let epoch = state.storage.get_current_epoch()?.unwrap_or(0);
            Ok(serde_json::json!({
                "currentTick": tick,
                "currentEpoch": epoch,
            }))
        }
        "getComputors" => {
            match state.storage.get_latest_computors()? {
                Some((_epoch, data)) => Ok(serde_json::from_slice(&data)?),
                None => Ok(serde_json::json!(null)),
            }
        }
        "getContractIPO" => {
            let index = extract_u32_param(params, 0)?;
            match state.storage.get_contract_ipo(index)? {
                Some(data) => Ok(serde_json::from_slice(&data)?),
                None => Ok(serde_json::json!(null)),
            }
        }
        "getIssuedAssets" => {
            let assets = state.storage.get_all_assets(1000)?;
            let items: Vec<serde_json::Value> = assets.into_iter()
                .filter_map(|(_, data)| serde_json::from_slice(&data).ok())
                .collect();
            Ok(serde_json::json!(items))
        }
        "getOwnedAssets" => {
            let _id = extract_string_param(params, 0)?;
            // TODO: Need entity->owned assets index
            Ok(serde_json::json!([]))
        }
        "getPossessedAssets" => {
            let _id = extract_string_param(params, 0)?;
            // TODO: Need entity->possessed assets index
            Ok(serde_json::json!([]))
        }
        "getAssetsByOwner" => {
            let _id = extract_string_param(params, 0)?;
            Ok(serde_json::json!([]))
        }
        "getSpectrumStats" => {
            let tick = state.storage.get_current_tick()?.unwrap_or(0);
            let epoch = state.storage.get_current_epoch()?.unwrap_or(0);
            Ok(serde_json::json!({
                "tick": tick,
                "epoch": epoch,
                "numberOfEntities": 0, // TODO
            }))
        }
        "getSpectrum" => {
            let id = extract_string_param(params, 0)?;
            let key = identity::decode_base26(&id)
                .ok_or_else(|| anyhow::anyhow!("Invalid identity"))?;
            match state.storage.get_spectrum_entry(&key)? {
                Some(data) => Ok(serde_json::from_slice(&data)?),
                None => Ok(serde_json::json!(null)),
            }
        }
        "getProposal" => Ok(serde_json::json!(null)),
        "getBallot" => Ok(serde_json::json!(null)),
        "getVotesForProposal" => Ok(serde_json::json!([])),
        "getVotesForVoter" => Ok(serde_json::json!([])),
        "getActiveIPOs" => {
            // TODO: query active IPOs
            Ok(serde_json::json!([]))
        }
        "getIPOBids" => {
            let index = extract_u32_param(params, 0)?;
            match state.storage.get_contract_ipo(index)? {
                Some(data) => Ok(serde_json::from_slice(&data)?),
                None => Ok(serde_json::json!(null)),
            }
        }
        "getContractFunction" => {
            // Forward to node via TCP -- for now return not supported
            Err(anyhow::anyhow!("Contract function calls not yet supported"))
        }
        "getSyncState" => {
            let tick = state.storage.get_current_tick()?.unwrap_or(0);
            Ok(serde_json::json!({"syncing": false, "currentTick": tick}))
        }
        "getContractFunctionResult" => {
            Err(anyhow::anyhow!("Contract function calls not yet supported"))
        }
        
        // --- Qonduit-native methods ---
        "qonduit_getTick" => {
            let tick = extract_u32_param(params, 0)?;
            match state.storage.get_tick(tick)? {
                Some(data) => Ok(serde_json::from_slice(&data)?),
                None => Ok(serde_json::json!(null)),
            }
        }
        "qonduit_getTickTransactions" => {
            let tick = extract_u32_param(params, 0)?;
            let hashes = state.storage.get_tx_hashes_for_tick(tick)?;
            let mut txs = Vec::new();
            for hash in hashes {
                if let Some(data) = state.storage.get_tx(&hash)? {
                    if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&data) {
                        txs.push(val);
                    }
                }
            }
            Ok(serde_json::json!(txs))
        }
        "qonduit_getEntityActivity" => {
            let id = extract_string_param(params, 0)?;
            let limit = extract_u32_param(params, 1).unwrap_or(100);
            let key = identity::decode_base26(&id)
                .ok_or_else(|| anyhow::anyhow!("Invalid identity"))?;
            let hashes = state.storage.get_tx_hashes_for_entity(&key, limit as usize)?;
            let mut txs = Vec::new();
            for hash in hashes {
                if let Some(data) = state.storage.get_tx(&hash)? {
                    if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&data) {
                        txs.push(val);
                    }
                }
            }
            Ok(serde_json::json!(txs))
        }
        "qonduit_search" => {
            let _query = extract_string_param(params, 0)?;
            // TODO: full-text search
            Ok(serde_json::json!({"results": []}))
        }
        "qonduit_getAssetHolders" => {
            Ok(serde_json::json!([]))
        }
        "qonduit_getCustomMessages" => {
            let tick = extract_u32_param(params, 0)?;
            let msgs = state.storage.get_custom_messages_for_tick(tick)?;
            let items: Vec<serde_json::Value> = msgs.into_iter()
                .filter_map(|data| serde_json::from_slice(&data).ok())
                .collect();
            Ok(serde_json::json!(items))
        }
        "qonduit_getOracleData" => {
            Ok(serde_json::json!(null))
        }
        "qonduit_getEntityTokens" => {
            Ok(serde_json::json!([]))
        }
        "qonduit_getDeFiPositions" => {
            Ok(serde_json::json!([]))
        }
        "qonduit_getEpochInfo" => {
            let epoch = state.storage.get_current_epoch()?.unwrap_or(0);
            let tick = state.storage.get_current_tick()?.unwrap_or(0);
            Ok(serde_json::json!({"epoch": epoch, "currentTick": tick}))
        }
        "qonduit_getLogEvents" => {
            let tick = extract_u32_param(params, 0)?;
            let msgs = state.storage.get_custom_messages_for_tick(tick)?;
            Ok(serde_json::json!(msgs.len()))
        }
        "qonduit_getSpectrumChanges" => {
            Ok(serde_json::json!([]))
        }
        "qonduit_getEntityBalances" => {
            let id = extract_string_param(params, 0)?;
            let key = identity::decode_base26(&id)
                .ok_or_else(|| anyhow::anyhow!("Invalid identity"))?;
            match state.storage.get_spectrum_entry(&key)? {
                Some(data) => Ok(serde_json::from_slice(&data)?),
                None => Ok(serde_json::json!(null)),
            }
        }
        
        _ => Err(anyhow::anyhow!("Method not found: {}", method)),
    }
}

// --- Param extraction helpers ---

fn extract_string_param(params: Option<&serde_json::Value>, index: usize) -> Result<String, anyhow::Error> {
    let params = params.ok_or_else(|| anyhow::anyhow!("Missing params"))?;
    let arr = params.as_array().ok_or_else(|| anyhow::anyhow!("Params must be array"))?;
    arr.get(index)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow::anyhow!("Missing param at index {index}"))
}

fn extract_u32_param(params: Option<&serde_json::Value>, index: usize) -> Result<u32, anyhow::Error> {
    let params = params.ok_or_else(|| anyhow::anyhow!("Missing params"))?;
    let arr = params.as_array().ok_or_else(|| anyhow::anyhow!("Params must be array"))?;
    arr.get(index)
        .and_then(|v| v.as_u64())
        .map(|n| n as u32)
        .ok_or_else(|| anyhow::anyhow!("Missing param at index {index}"))
}
