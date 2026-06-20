//! Entity response from RespondEntity (type 32).
//!
//! Size: 64 bytes EntityRecord + 4 tick + 4 spectrumIndex + 768 siblings.
//! Layout matches C++ EntityRecord:
//! ```text
//! [0..32]   pub key (m256i)
//! [32..40]  incomingAmount (i64)
//! [40..48]  outgoingAmount (i64)
//! [48..52]  numberOfIncomingTransfers (u32)
//! [52..56]  numberOfOutgoingTransfers (u32)
//! [56..60]  latestIncomingTransferTick (u32)
//! [60..64]  latestOutgoingTransferTick (u32)
//! ```

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityData {
    pub identity: String,
    pub incoming: i64,
    pub outgoing: i64,
    pub number_of_incoming_transfers: u32,
    pub number_of_outgoing_transfers: u32,
    pub latest_incoming_transfer_tick: u32,
    pub latest_outgoing_transfer_tick: u32,
}
