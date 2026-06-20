//! Event processor: consumes events from NATS JetStream, builds derived
//! indexes in the warm tier (RocksDB).

pub mod consumer;
pub mod indexer;

use std::sync::Arc;

use anyhow::Result;
use async_nats::Client as NatsClient;
use qonduit_core::PipelineState;
use tracing::info;

use consumer::Consumer;
use indexer::Indexer;

/// Configuration for the processor.
#[derive(Debug, Clone)]
pub struct ProcessorConfig {
    /// NATS consumer group name.
    pub consumer_group: String,

    /// When true, consumers replay from the start of all streams (catch-up mode).
    /// When false, consumers only receive new messages (live mode).
    pub catch_up: bool,

    /// Number of messages to fetch per batch from each consumer.
    /// Defaults to 100 in catch-up mode, 10 in live mode.
    pub batch_size: usize,
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            consumer_group: "qonduit-processors".to_string(),
            catch_up: true,
            batch_size: 100,
        }
    }
}

/// Runs the processor, consuming from NATS and indexing into storage.
pub async fn run(
    config: ProcessorConfig,
    nats: NatsClient,
    storage: Arc<qonduit_storage::WarmStorage>,
    pipeline: Arc<PipelineState>,
) -> Result<()> {
    info!(
        "Starting processor (group: {}, catch_up: {}, batch_size: {})...",
        config.consumer_group, config.catch_up, config.batch_size
    );

    let indexer = Indexer::new(storage, pipeline.clone());
    let consumer = Consumer::new(nats, indexer, pipeline, config.catch_up, config.batch_size);
    consumer.run().await
}
