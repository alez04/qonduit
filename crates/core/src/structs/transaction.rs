//! Transaction and input structs from `structures.h`.
//!
//! A transaction is 80 bytes header + up to 1024 bytes payload + 64 bytes signature.
//! Max transaction size: 1168 bytes.

use serde::{Deserialize, Serialize};

/// Transaction type / input type.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionType {
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

/// Raw transaction header (80 bytes) from the wire.
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct RawTransactionHeader {
    /// Transaction type.
    pub tx_type: u8,
    /// Sender public key.
    pub source: [u8; 32],
    /// Destination public key.
    pub destination: [u8; 32],
    /// Amount.
    pub amount: i64,
    /// Target tick.
    pub tick: u32,
    /// Input payload size.
    pub input_size: u16,
    /// Input type.
    pub input_type: u16,
}

/// Decoded transaction for storage/query.
///
/// Binary fields (source, destination, signature) are hex-encoded strings
/// for JSON serialization, since serde doesn't auto-derive for [u8; 64].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub hash: String,
    pub tx_type: u8,
    pub source_hex: String,
    pub source_identity: String,
    pub destination_hex: String,
    pub destination_identity: String,
    pub amount: i64,
    pub tick: u32,
    pub input_type: u16,
    pub input_size: u16,
    pub input_hex: String,
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
