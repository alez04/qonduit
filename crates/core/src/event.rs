//! NATS JetStream subject definitions and event payload types.
//!
//! All inter-component communication flows through NATS subjects.
//! Subject hierarchy: `Q.{epoch}.QONDUIT.{category}.{subkey}`

use serde::{Deserialize, Serialize};

// --- NATS Subject Constants ---

/// Tick broadcast subject. Payload: JSON-encoded TickData.
pub const SUBJECT_TICK: &str = "Q.*.QONDUIT.TICK";

/// Transaction broadcast subject. Payload: JSON-encoded Transaction.
pub const SUBJECT_TX: &str = "Q.*.QONDUIT.TX";

/// Entity update subject. Payload: JSON-encoded EntityData.
pub const SUBJECT_ENTITY: &str = "Q.*.QONDUIT.ENTITY";

/// Spectrum update subject.
pub const SUBJECT_SPECTRUM: &str = "Q.*.QONDUIT.SPECTRUM";

/// Computors broadcast subject.
pub const SUBJECT_COMPUTORS: &str = "Q.*.QONDUIT.COMPUTORS";

/// Custom message subject (burning, oracle, dust, etc.).
pub const SUBJECT_CUSTOM_MESSAGE: &str = "Q.*.QONDUIT.CUSTMSG";

/// Oracle status change subject.
pub const SUBJECT_ORACLE: &str = "Q.*.QONDUIT.ORACLE";

/// Asset update subject.
pub const SUBJECT_ASSET: &str = "Q.*.QONDUIT.ASSET";

/// Contract IPO update subject.
pub const SUBJECT_CONTRACT: &str = "Q.*.QONDUIT.CONTRACT";

/// Contract function response subject.
pub const SUBJECT_CONTRACT_FN: &str = "Q.*.QONDUIT.CFNR";

/// Stream names for NATS JetStream.
pub const STREAM_TICKS: &str = "QONDUIT_TICKS";
pub const STREAM_TRANSACTIONS: &str = "QONDUIT_TX";
pub const STREAM_ENTITIES: &str = "QONDUIT_ENTITIES";
pub const STREAM_SPECTRUM: &str = "QONDUIT_SPECTRUM";
pub const STREAM_COMPUTORS: &str = "QONDUIT_COMPUTORS";
pub const STREAM_CUSTOM_MESSAGES: &str = "QONDUIT_CUSTMSG";
pub const STREAM_ORACLE: &str = "QONDUIT_ORACLE";
pub const STREAM_ASSETS: &str = "QONDUIT_ASSETS";
pub const STREAM_CONTRACTS: &str = "QONDUIT_CONTRACTS";

// --- Event Payload Types ---

/// Envelope for all events published to NATS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope<T> {
    /// Source node address.
    pub source: String,
    /// Epoch at time of event.
    pub epoch: u16,
    /// Tick at time of event.
    pub tick: u32,
    /// Timestamp (UTC microseconds).
    pub timestamp: u64,
    /// Event payload.
    pub data: T,
}

// --- Custom Message Types ---

/// Custom message categories from `custom_message.h`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CustomMessageCategory {
    Transfer = 0,
    Dividend = 1,
    EpochManagement = 2,
    Issuance = 3,
    Burn = 4,
    OracleQuery = 5,
    OracleVote = 6,
    Oracle = 7,
    SpectrumManagement = 8,
    DeFi = 9,
    ContractManagement = 10,
}

/// Decoded custom message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomMessage {
    pub category: CustomMessageCategory,
    pub contract_index: u32,
    pub operation: Option<u64>,
    pub raw_hex: String,
}
