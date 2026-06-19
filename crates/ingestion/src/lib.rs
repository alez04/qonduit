//! Ingestion worker: connects to Qubic node via TCP, decodes packets,
//! and publishes events to NATS JetStream.

pub mod client;
pub mod decoder;
pub mod nats_publish;
pub mod nats_setup;
pub mod protocol;

pub use client::IngestionClient;
pub use nats_publish::NatsPublisher;
