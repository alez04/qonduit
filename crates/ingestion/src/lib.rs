//! Ingestion worker: connects to Qubic node via TCP, decodes packets,
//! and publishes events to NATS JetStream.

pub mod client;
pub mod decoder;
pub mod decoders;
pub mod metrics;
pub mod nats_publish;
pub mod nats_setup;
pub mod pending;
pub mod peer_manager;
pub mod protocol;

pub use client::{IngestionClient, IngestionHandle};
pub use nats_publish::NatsPublisher;
pub use peer_manager::PeerManager;
