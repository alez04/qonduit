//! Ingestion worker: connects to Qubic node via TCP, decodes packets,
//! and publishes events to NATS JetStream.

pub mod client;
pub mod decoder;

pub use client::IngestionClient;
