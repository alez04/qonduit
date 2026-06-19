/// Error types for Qonduit.
use thiserror::Error;

#[derive(Error, Debug)]
pub enum QonduitError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Packet too small: got {got} bytes, expected at least {expected}")]
    PacketTooSmall { got: usize, expected: usize },

    #[error("Invalid message type: {0}")]
    InvalidMessageType(u8),

    #[error("Invalid identity: {0}")]
    InvalidIdentity(String),

    #[error("Connection closed")]
    ConnectionClosed,

    #[error("Timeout")]
    Timeout,

    #[error("NATS error: {0}")]
    Nats(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("{0}")]
    Other(#[from] anyhow::Error),
}
