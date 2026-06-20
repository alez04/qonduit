//! Tick data from BroadcastFutureTickData (type 8).
//!
//! Layout matches C++ TickData struct from `tick.h`:
//!
//! ```text
//! Offset  Size  Field
//! 0       2     computorIndex (u16)
//! 2       2     epoch (u16)
//! 4       4     tick (u32)
//! 8       2     millisecond (u16)
//! 10      1     second (u8)
//! 11      1     minute (u8)
//! 12      1     hour (u8)
//! 13      1     day (u8)
//! 14      1     month (u8)
//! 15      1     year (u8)
//! 16      32    timelock (m256i)
//! 48      ...   transactionDigests[4096] (32 bytes each)
//! ...     ...   contractFees[1024] (8 bytes each)
//! end     64    signature
//! ```

use serde::{Deserialize, Serialize};

/// Decoded tick for storage/query (serializable).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickData {
    pub computor_index: u16,
    pub epoch: u16,
    pub tick: u32,
    pub timestamp: u64,
    pub time_lock: [u8; 32],
    /// SHA-256 digests of the 4096 transaction slots.
    /// Stored as hex string for JSON compatibility (131,072 bytes raw).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub transaction_digests_hex: String,
    /// Contract fees for the 1024 contracts (8 bytes each = 8,192 bytes raw).
    /// Stored as hex string.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub contract_fees_hex: String,
    /// SchnorrQ signature (64 bytes) as hex string.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub signature_hex: String,
}
