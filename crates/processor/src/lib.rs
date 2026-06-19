//! Event processor: consumes events from NATS JetStream, builds derived
//! indexes in the warm tier (RocksDB).

use anyhow::Result;
use async_nats::Client as NatsClient;
use tracing::info;

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
pub async fn run(config: ProcessorConfig, _nats: NatsClient) -> Result<()> {
    info!("Starting processor (group: {})...", config.consumer_group);

    // TODO: Create JetStream consumers for each stream
    // TODO: Process events and build indexes

    // Keep the task alive
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}
