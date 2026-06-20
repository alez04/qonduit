//! Transaction and input structs from `structures.h`.
//!
//! A transaction is 80 bytes header + up to 1024 bytes payload + 64 bytes signature.
//! Max transaction size: 1168 bytes.

use serde::{Deserialize, Serialize};

/// Transaction type / input type.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionType {
    /// Simple transfer, no input payload (type 0).
    Transfer = 0,
    /// Additional issuance (type 1).
    AdditionalIssuance = 1,
    /// Transfer ownership (type 2).
    TransferOwnership = 2,
    /// Transfer possession (type 3).
    TransferPossession = 3,
    /// Bid (type 24).
    Bid = 24,
    /// Ask (type 25).
    Ask = 25,
    /// Distribute dividends (type 26).
    DistributeDividends = 26,
}

/// Decoded transaction input classification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InputType {
    /// Simple transfer (input_type = 0), no payload.
    SimpleTransfer,
    /// Smart contract call (input_type > 0, not a known asset operation).
    SmartContractCall(u16),
    /// Asset issuance (input_type = 1).
    AssetIssuance,
    /// Asset ownership transfer (input_type = 2).
    AssetOwnershipTransfer,
    /// Asset possession transfer (input_type = 3).
    AssetPossessionTransfer,
    /// Dividend distribution (input_type = 26).
    DividendDistribution,
    /// Bid on contract IPO (input_type = 24).
    ContractBid,
    /// Ask on contract IPO (input_type = 25).
    ContractAsk,
}

impl InputType {
    /// Classify a raw input_type value.
    pub fn classify(input_type: u16) -> Self {
        match input_type {
            0 => Self::SimpleTransfer,
            1 => Self::AssetIssuance,
            2 => Self::AssetOwnershipTransfer,
            3 => Self::AssetPossessionTransfer,
            24 => Self::ContractBid,
            25 => Self::ContractAsk,
            26 => Self::DividendDistribution,
            other => Self::SmartContractCall(other),
        }
    }

    /// Return a human-readable label for this input type.
    pub fn label(&self) -> &'static str {
        match self {
            Self::SimpleTransfer => "simple_transfer",
            Self::AssetIssuance => "asset_issuance",
            Self::AssetOwnershipTransfer => "asset_ownership_transfer",
            Self::AssetPossessionTransfer => "asset_possession_transfer",
            Self::DividendDistribution => "dividend_distribution",
            Self::ContractBid => "contract_bid",
            Self::ContractAsk => "contract_ask",
            Self::SmartContractCall(_) => "smart_contract_call",
        }
    }
}

/// Raw transaction header (80 bytes) from the wire.
///
/// Matches the C++ `Transaction` struct layout:
/// ```text
/// [0..32]   source (m256i)
/// [32..64]  destination (m256i)
/// [64..72]  amount (i64)
/// [72..76]  tick (u32)
/// [76..78]  input_type (u16)
/// [78..80]  input_size (u16)
/// ```
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct RawTransactionHeader {
    /// Sender public key.
    pub source: [u8; 32],
    /// Destination public key.
    pub destination: [u8; 32],
    /// Amount.
    pub amount: i64,
    /// Target tick.
    pub tick: u32,
    /// Input type (determines transaction type).
    pub input_type: u16,
    /// Input payload size.
    pub input_size: u16,
}

/// Decoded transaction for storage/query.
///
/// Binary fields (source, destination, signature) are hex-encoded strings
/// for JSON serialization, since serde doesn't auto-derive for [u8; 64].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub hash: String,
    pub source_hex: String,
    pub source_identity: String,
    pub destination_hex: String,
    pub destination_identity: String,
    pub amount: i64,
    pub tick: u32,
    pub input_type: u16,
    pub input_size: u16,
    pub input_hex: String,
    pub input_type_name: String,
    pub signature_hex: String,
}

/// Input size for transfer dividends (8 bytes = 2 * u32).
pub const TRANSFER_DIVIDENDS_INPUT_SIZE: usize = 8;

/// Input size for additional issuance (8 bytes = 2 * u32).
pub const ADDITIONAL_ISSUANCE_INPUT_SIZE: usize = 8;

/// Input struct for distribute dividends.
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct DistributeDividendsInput {
    pub asset_index: u32,
    pub amount_per_share: u32,
}

/// Input struct for additional issuance.
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct AdditionalIssuanceInput {
    pub asset_index: u32,
    pub additional_amount: u32,
}
