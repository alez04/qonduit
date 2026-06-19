//! Spectrum (balance sheet) entry.
//!
//! Entry size: 144 bytes.
//! Layout:
//!   [0..32]   pub key
//!   [32..40]  incoming_amount i64
//!   [40..48]  outgoing_amount i64
//!   [48..56]  number_of_incoming_transfers u64
//!   [56..64]  number_of_outgoing_transfers u64
//!   [64..68]  latest_incoming_transfer_tick u32
//!   [68..72]  latest_outgoing_transfer_tick u32
//!   [72..88]  -- unused (asset issuance data)
//!   [88..128] -- unused
//!   [128..136] asset_issued u32 + asset_owned u32
//!   [136..144] -- unused

use serde::{Deserialize, Serialize};

pub const SPECTRUM_ENTRY_SIZE: usize = 144;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpectrumEntry {
    pub identity: String,
    pub incoming_amount: i64,
    pub outgoing_amount: i64,
    pub balance: i64,
    pub number_of_incoming_transfers: u64,
    pub number_of_outgoing_transfers: u64,
    pub latest_incoming_transfer_tick: u32,
    pub latest_outgoing_transfer_tick: u32,
    pub asset_issued: u32,
    pub asset_owned: u32,
}
