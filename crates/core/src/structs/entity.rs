/// Entity response from RespondEntity (type 32).
///
/// Size: 481 bytes total packet (header + 473 payload).
/// Layout:
///   [0..32]   pub key
///   [32..40]  incoming i64
///   [40..48]  outgoing i64
///   [48..56]  number of outgoing transfers u64
///   [56..64]  latest incoming transfer tick u32 + latest outgoing transfer tick u32
///   [64..128] iotensor (first 64 bytes only)

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityData {
    pub identity: String,
    pub incoming: i64,
    pub outgoing: i64,
    pub number_of_outgoing_transfers: u64,
    pub latest_incoming_transfer_tick: u32,
    pub latest_outgoing_transfer_tick: u32,
}
