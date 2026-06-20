//! Logging types from RespondPruningLog (type 57),
//! RespondLogStateDigest (type 59),
//! RespondAllLogIdRangesFromTick (type 51),
//! and RespondTxStatus (type 202).

use serde::{Deserialize, Serialize};

use crate::constants::LOG_TX_PER_TICK;

/// Response to a log pruning request (type 57).
///
/// Layout: 8 bytes, `success` as i64 (0 = success, non-zero = error code).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PruningLogResponse {
    /// 0 on success, non-zero error code on failure.
    pub success: i64,
}

/// Response to a log state digest request (type 59).
///
/// Layout: 32 bytes, a single m256i digest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogStateDigest {
    /// 32-byte SHA-256 digest of the log event state (hex).
    pub digest_hex: String,
}

/// Response logId ranges (fromLogId, length) of all txs from a tick (type 51).
///
/// Layout from C++ `logging.h`:
/// ```text
/// fromLogId[LOG_TX_PER_TICK] — i64 array (4102 * 8 bytes)
/// length[LOG_TX_PER_TICK]    — i64 array (4102 * 8 bytes)
/// ```
/// Total: 65632 bytes.
///
/// We decode the first `tx_count` entries (those are the only ones populated).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogIdRangesResponse {
    /// Tick these log ranges belong to (from the requesting packet).
    pub tick: u32,
    /// Number of valid entries (from the request, or LOG_TX_PER_TICK).
    pub tx_count: usize,
    /// fromLogId for each transaction slot.
    pub from_log_ids: Vec<i64>,
    /// length for each transaction slot.
    pub lengths: Vec<i64>,
}

/// Response to a transaction status request (type 202, tx addon only).
///
/// Layout from LFG-Qubic `structs.h` (RespondTxStatus):
/// ```text
/// Offset  Size  Field
/// 0       4     currentTickOfNode (u32)
/// 4       4     tick (u32)
/// 8       4     txCount (u32)
/// 12      512   moneyFlew bitfield ((4096+7)/8 bytes)
/// 524     var   txDigests[txCount] (32 bytes each)
/// ```
///
/// We decode the header and the tx digest list. The moneyFlew bitfield is
/// preserved as raw hex since each bit has contract-specific semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxStatusResponse {
    /// Current tick of the responding node.
    pub current_tick_of_node: u32,
    /// Requested tick.
    pub tick: u32,
    /// Number of transaction digests in this response.
    pub tx_count: u32,
    /// Money-flew bitfield (raw hex, 512 bytes).
    pub money_flew_hex: String,
    /// Transaction digests (hex strings).
    pub tx_digests: Vec<String>,
}
