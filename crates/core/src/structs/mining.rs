//! Custom mining types from BroadcastCustomMiningTask (type 68)
//! and BroadcastCustomMiningSolution (type 69).
//!
//! Wire format from `broadcast_message.h` + `custom_mining.h`.

use serde::{Deserialize, Serialize};

/// Custom mining type identifiers from `custom_mining.h`.
pub const CUSTOM_MINING_TYPE_DOGE: u8 = 0;

/// BroadcastCustomMiningTask (type 68) payload.
///
/// Outer wrapper from `broadcast_message.h`:
/// ```text
/// sourcePublicKey (32B) + zero (32B) + gammingNonce (32B)
/// + codeFileTrailerDigest (32B) + dataFileTrailerDigest (32B) = 160 bytes
/// ```
/// Inner `CustomQubicMiningTask`:
/// ```text
/// jobId (u64 LE) + customMiningType (u8) + task-specific payload
/// ```
/// Followed by dispatcher signature (64 bytes).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomMiningTask {
    /// Public key of the dispatcher (hex).
    pub source_public_key_hex: String,
    /// Code file trailer digest (hex).
    pub code_file_trailer_digest_hex: String,
    /// Data file trailer digest (hex).
    pub data_file_trailer_digest_hex: String,
    /// Job ID (millisecond timestamp).
    pub job_id: u64,
    /// Custom mining type (e.g., 0 = DOGE).
    pub custom_mining_type: u8,
    /// Task-specific payload (hex).
    pub payload_hex: String,
    /// Dispatcher signature (hex).
    pub signature_hex: String,
}

/// BroadcastCustomMiningSolution (type 69) payload.
///
/// Outer wrapper from `broadcast_message.h`:
/// ```text
/// sourcePublicKey (32B) + zero (32B) + gammingNonce (32B) = 96 bytes
/// ```
/// Inner `CustomQubicMiningSolution`:
/// ```text
/// sourcePublicKey (32B) + jobId (u64 LE) + customMiningType (u8)
/// + solution-specific payload
/// ```
/// Followed by sender's signature (64 bytes).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomMiningSolution {
    /// Public key of the dispatcher (hex).
    pub source_public_key_hex: String,
    /// Miner's public key (hex).
    pub miner_public_key_hex: String,
    /// Job ID (millisecond timestamp).
    pub job_id: u64,
    /// Custom mining type (e.g., 0 = DOGE).
    pub custom_mining_type: u8,
    /// Solution-specific payload (hex).
    pub payload_hex: String,
    /// Sender's signature (hex).
    pub signature_hex: String,
}
