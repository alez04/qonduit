use std::sync::Arc;
use axum::{Router, extract::State, routing::post, Json};
use serde::{Deserialize, Serialize};
use qonduit_core::identity;

#[allow(unused_imports)]
use hex;

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
        Err(e) => {
            let msg = e.to_string();
            let code = if msg.starts_with("Method not found") {
                -32601
            } else if msg.starts_with("Missing params")
                || msg.starts_with("Params must be array")
                || msg.starts_with("Missing param at index")
            {
                -32602
            } else {
                -32000
            };
            Json(JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                result: None,
                error: Some(JsonRpcError {
                    code,
                    message: msg,
                    data: None,
                }),
                id,
            })
        }
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
            let id = extract_string_param(params, 0)?;
            let key = identity::decode_base26(&id)
                .ok_or_else(|| anyhow::anyhow!("Invalid identity"))?;

            // Try the entity→asset index first
            if let Ok(indices) = state.storage.get_assets_for_entity(&key) {
                if !indices.is_empty() {
                    let mut assets = Vec::new();
                    for idx in indices {
                        if let Ok(Some(data)) = state.storage.get_asset(idx) {
                            if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&data) {
                                assets.push(val);
                            }
                        }
                    }
                    return Ok(serde_json::json!(assets));
                }
            }

            // Fallback: scan the asset column family for assets owned by this entity
            let entity_json = serde_json::json!(key);
            let assets = state.storage.get_all_assets(10000)?;
            let owned: Vec<serde_json::Value> = assets
                .into_iter()
                .filter_map(|(_, data)| serde_json::from_slice::<serde_json::Value>(&data).ok())
                .filter(|v| v.get("owning_entity") == Some(&entity_json))
                .collect();
            Ok(serde_json::json!(owned))
        }
        "getPossessedAssets" => {
            let id = extract_string_param(params, 0)?;
            let key = identity::decode_base26(&id)
                .ok_or_else(|| anyhow::anyhow!("Invalid identity"))?;

            // Try the entity→asset index first
            if let Ok(indices) = state.storage.get_assets_for_entity(&key) {
                if !indices.is_empty() {
                    let mut assets = Vec::new();
                    for idx in indices {
                        if let Ok(Some(data)) = state.storage.get_asset(idx) {
                            if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&data) {
                                assets.push(val);
                            }
                        }
                    }
                    return Ok(serde_json::json!(assets));
                }
            }

            // Fallback: scan the asset column family for assets possessed by this entity
            let entity_json = serde_json::json!(key);
            let assets = state.storage.get_all_assets(10000)?;
            let possessed: Vec<serde_json::Value> = assets
                .into_iter()
                .filter_map(|(_, data)| serde_json::from_slice::<serde_json::Value>(&data).ok())
                .filter(|v| v.get("possessing_entity") == Some(&entity_json))
                .collect();
            Ok(serde_json::json!(possessed))
        }
        "getAssetsByOwner" => {
            let id = extract_string_param(params, 0)?;
            let key = identity::decode_base26(&id)
                .ok_or_else(|| anyhow::anyhow!("Invalid identity"))?;

            // Try the entity→asset index first
            if let Ok(indices) = state.storage.get_assets_for_entity(&key) {
                if !indices.is_empty() {
                    let mut assets = Vec::new();
                    for idx in indices {
                        if let Ok(Some(data)) = state.storage.get_asset(idx) {
                            if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&data) {
                                assets.push(val);
                            }
                        }
                    }
                    return Ok(serde_json::json!(assets));
                }
            }

            // Fallback: scan for owned assets
            let entity_json = serde_json::json!(key);
            let assets = state.storage.get_all_assets(10000)?;
            let owned: Vec<serde_json::Value> = assets
                .into_iter()
                .filter_map(|(_, data)| serde_json::from_slice::<serde_json::Value>(&data).ok())
                .filter(|v| v.get("owning_entity") == Some(&entity_json))
                .collect();
            Ok(serde_json::json!(owned))
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
            let ipos = state.storage.get_all_contract_ipos(1000)?;
            let items: Vec<serde_json::Value> = ipos
                .into_iter()
                .filter_map(|(_, data)| serde_json::from_slice(&data).ok())
                .collect();
            Ok(serde_json::json!(items))
        }
        "getIPOBids" => {
            let index = extract_u32_param(params, 0)?;
            match state.storage.get_contract_ipo(index)? {
                Some(data) => Ok(serde_json::from_slice(&data)?),
                None => Ok(serde_json::json!(null)),
            }
        }
        "getContractFunction" => {
            // TODO: Forward REQUEST_CONTRACT_FUNCTION (type 42) to a Qubic node via TCP.
            // The request format is:
            //   [0..4]  contractIndex (u32 LE)
            //   [4..6]  inputType (u16 LE)
            //   [6..8]  inputSize (u16 LE)
            //   [8..]   input payload (variable)
            //
            // The node responds with RESPOND_CONTRACT_FUNCTION (type 43):
            //   Variable-size output (0 bytes = invocation failed or no function registered)
            //
            // Implementation plan: The ingestion layer already handles type 43 responses
            // and publishes them to NATS subject QONDUIT.CFNR. We need to:
            // 1. Add a NATS request/reply subject for contract function calls
            // 2. Ingestion subscribes to QONDUIT.CFNR.REQUEST, sends REQUEST_CONTRACT_FUNCTION
            //    to the node, collects RESPOND_CONTRACT_FUNCTION, and publishes result
            // 3. This RPC handler sends a NATS request and waits for the reply
            //
            // For now, return a clear error indicating this is not yet implemented.
            Err(anyhow::anyhow!(
                "getContractFunction is not yet supported. \
                 This requires TCP forwarding to a Qubic node. \
                 TODO: Implement via NATS request/reply through the ingestion layer."
            ))
        }
        "getSyncState" => {
            let tick = state.storage.get_current_tick()?.unwrap_or(0);
            Ok(serde_json::json!({"syncing": false, "currentTick": tick}))
        }
        "getContractFunctionResult" => {
            // TODO: This would query the result of a previously submitted contract function
            // call, identified by dejavu. Currently all contract function responses are
            // published to NATS QONDUIT.CFNR with the dejavu as correlation ID.
            //
            // Implementation requires:
            // 1. A pending-response cache in the query layer keyed by dejavu
            // 2. Subscribe to QONDUIT.CFNR, store results temporarily
            // 3. This method looks up the cached result by dejavu
            //
            // For now, return a clear error.
            Err(anyhow::anyhow!(
                "getContractFunctionResult is not yet supported. \
                 This requires a pending-response cache for contract function calls. \
                 TODO: Implement via NATS subscription + dejavu-keyed cache."
            ))
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
            let query = extract_string_param(params, 0)?;
            let q = query.trim().to_string();

            if q.is_empty() {
                return Ok(serde_json::json!({"results": []}));
            }

            let mut results = Vec::new();

            // If numeric, search by tick
            if q.chars().all(|c| c.is_ascii_digit()) {
                if let Ok(tick) = q.parse::<u32>() {
                    if let Some(data) = state.storage.get_tick(tick)? {
                        if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&data) {
                            results.push(serde_json::json!({"type": "tick", "data": val}));
                        }
                    }
                }
            }

            // If hex hash (full 64 chars), search transaction
            if q.len() == 64 && q.chars().all(|c| c.is_ascii_hexdigit()) {
                if let Ok(bytes) = hex::decode(&q) {
                    if bytes.len() == 32 {
                        let mut hash = [0u8; 32];
                        hash.copy_from_slice(&bytes);
                        if let Some(data) = state.storage.get_tx(&hash)? {
                            if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&data) {
                                results.push(serde_json::json!({"type": "transaction", "data": val}));
                            }
                        }
                    }
                }
            }

            // If uppercase A-Z, try as identity prefix or exact match
            if q.chars().all(|c| c.is_ascii_uppercase()) {
                // Exact identity lookup if 60 chars
                if q.len() == 60 {
                    if let Some(key) = identity::decode_base26(&q) {
                        if let Some(data) = state.storage.get_entity(&key)? {
                            if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&data) {
                                results.push(serde_json::json!({"type": "entity", "data": val}));
                            }
                        }
                    }
                }
                // Prefix search (4-59 chars)
                if q.len() >= 4 && q.len() < 60 {
                    if let Ok(keys) = state.storage.get_all_entity_keys(10000) {
                        for key in &keys {
                            let identity_str = qonduit_core::identity::encode_base26(key);
                            if identity_str.starts_with(&q) {
                                if let Ok(Some(data)) = state.storage.get_entity(key) {
                                    if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&data) {
                                        results.push(serde_json::json!({"type": "entity", "data": val}));
                                    }
                                }
                            }
                        }
                    }
                }
            }

            Ok(serde_json::json!({"results": results}))
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
            let id = extract_string_param(params, 0)?;
            let key = identity::decode_base26(&id)
                .ok_or_else(|| anyhow::anyhow!("Invalid identity"))?;
            match state.storage.get_spectrum_entry(&key)? {
                Some(data) => {
                    let entry: serde_json::Value = serde_json::from_slice(&data)?;
                    let balance = entry.get("balance").cloned().unwrap_or(serde_json::json!(0));
                    Ok(serde_json::json!({
                        "identity": id,
                        "balance": balance,
                        "note": "Returns Qubic balance from spectrum. Token and contract balances not yet indexed."
                    }))
                }
                None => Ok(serde_json::json!({
                    "identity": id,
                    "balance": 0,
                    "note": "Entity not found in spectrum"
                })),
            }
        }
        "qonduit_getDeFiPositions" => {
            let _id = extract_string_param(params, 0)?;
            Ok(serde_json::json!({
                "positions": [],
                "note": "DeFi position indexing is not yet implemented. This endpoint will be populated once DeFi contract state indexing is added."
            }))
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
