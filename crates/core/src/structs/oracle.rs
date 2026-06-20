//! Oracle data response from RespondOracleData (type 67).
//!
//! The first 4 bytes of the payload are the `resType` field, followed by
//! type-specific data. We decode common types and fall back to raw hex
//! for unknown or variable-length payloads.

use serde::{Deserialize, Serialize};

/// Top-level oracle data response (type 67).
///
/// Always present in the payload. The `payload_hex` contains the
/// type-specific data after the 4-byte resType header.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OracleDataResponse {
    /// Response type identifier.
    pub res_type: u32,
    /// Raw hex-encoded payload for this response type.
    pub payload_hex: String,
}

/// Oracle query metadata (resType = 1).
///
/// Layout (72 bytes, little-endian):
/// ```text
/// Offset  Size  Field
/// 0       8     queryId (i64)
/// 8       1     type (u8)
/// 9       1     status (u8)
/// 10      2     statusFlags (u16)
/// 12      4     queryTick (u32)
/// 16      32    queryingEntity (m256i)
/// 48      8     timeout (u64)
/// 56      4     interfaceIndex (u32)
/// 60      4     subscriptionId (i32)
/// 64      4     revealTick (u32)
/// 68      2     totalCommits (u16)
/// 70      2     agreeingCommits (u16)
/// ```
pub const ORACLE_QUERY_METADATA_SIZE: usize = 72;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OracleQueryMetadata {
    pub query_id: i64,
    pub query_type: u8,
    pub status: u8,
    pub status_flags: u16,
    pub query_tick: u32,
    pub querying_entity_hex: String,
    pub timeout: u64,
    pub interface_index: u32,
    pub subscription_id: i32,
    pub reveal_tick: u32,
    pub total_commits: u16,
    pub agreeing_commits: u16,
}

/// Oracle query statistics (resType = 7).
///
/// Layout (variable, we parse the header fields).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OracleQueryStatistics {
    pub raw_hex: String,
}

/// Oracle revenue points response (resType = 8).
///
/// Layout: 676 * 8 bytes (one i64 per computor).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OracleRevenuePoints {
    pub points: Vec<i64>,
}
