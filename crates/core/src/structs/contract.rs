/// Contract IPO bid data.
///
/// Packet size: 8 + 676*32 + 64 = 21672 bytes.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractIpo {
    pub contract_index: u32,
    pub bids: Vec<ContractBid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractBid {
    pub identity: String,
    pub bid_amount: i64,
}
