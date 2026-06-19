/// Tick data from BroadcastTick (type 3).
///
/// Size: 1716 bytes total packet (header + 1708 payload).
/// Layout matches C++ Tick struct from `structures.h`.

use serde::{Deserialize, Serialize};

/// Raw tick broadcast payload (1708 bytes).
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct RawTick {
    pub epoch: u16,
    pub tick: u32,
    /// 676 computor signatures (each 64 bytes)
    pub signatures: [[u8; 64]; 676],
    pub timestamp: u64,
    pub time_lock: [u8; 32],
    pub transaction_counters: [u16; 1024],
    pub contract_counters: [u16; 1024],
    pub salted_spectrum_hash: [u8; 32],
    pub salted_universe_hash: [u8; 32],
    pub salted_computor_hash: [u8; 32],
    pub mining_nonce: u32,
}

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
    pub transaction_count: u16,
    pub contract_counters: Vec<u16>,
    pub signature_count: u32,
}
