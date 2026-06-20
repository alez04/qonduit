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
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            consumer_group: "qonduit-processors".to_string(),
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
    info!("Starting processor (group: {})...", config.consumer_group);

    let indexer = Indexer::new(storage, pipeline);
    let consumer = Consumer::new(nats, indexer);
    consumer.run().await
}
