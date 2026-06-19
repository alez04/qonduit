/// Tick data from BroadcastTick (type 3).
///
/// Size: 1708 bytes payload. Layout matches C++ Tick struct from
/// `structures.h`:
///
/// ```text
/// Offset  Size  Field
/// 0       2     epoch (u16)
/// 2       1     number_of_transactions (u8)
/// 3       1     number_of_special_events (u8)
/// 4       4     tick (u32)
/// 8       8     timestamp (u64)
/// 16      32    salt / time_lock ([u8; 32])
/// 48      32    salted_spectrum_hash ([u8; 32])
/// 80      32    salted_universe_hash ([u8; 32])
/// 112     32    salted_computor_hash ([u8; 32])
/// 144     1560  (reserved / compressed tick flags)
/// 1704    4     mining_nonce (u32)
/// ```

use serde::{Deserialize, Serialize};

/// Decoded tick for storage/query (serializable).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickData {
    pub epoch: u16,
    pub tick: u32,
    pub timestamp: u64,
    pub time_lock: [u8; 32],
    pub mining_nonce: u32,
    pub salted_spectrum_hash: [u8; 32],
    pub salted_universe_hash: [u8; 32],
    pub salted_computor_hash: [u8; 32],
    pub number_of_transactions: u8,
    pub number_of_special_events: u8,
    pub transaction_count: u16,
    pub contract_counters: Vec<u16>,
    pub signature_count: u32,
}


