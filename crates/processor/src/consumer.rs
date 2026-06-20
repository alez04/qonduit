//! NATS JetStream consumer: subscribes to event streams and dispatches to indexers.

use std::sync::Arc;

use anyhow::Result;
use async_nats::jetstream::{self, consumer::DeliverPolicy, AckKind};
use async_nats::jetstream::consumer::PullConsumer;
use async_nats::Client as NatsClient;
use futures_util::StreamExt;
use qonduit_core::PipelineState;
use tracing::{debug, error, info, warn};

use crate::indexer::Indexer;

/// Read available memory from `/proc/meminfo` on Linux.
/// Returns megabytes. Falls back to 4096 if unable to read.
fn available_memory_mb() -> u64 {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("MemAvailable:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse().ok())
                .map(|kb: u64| kb / 1024)
        })
        .unwrap_or(4096) // default 4GB if can't read
}

/// Dynamically calculate batch size based on pending message count and available memory.
fn calculate_batch(base_batch: usize, pending: u64, mem_mb: u64) -> usize {
    // Pending-based scaling
    let pending_batch = if pending > 10_000 {
        base_batch * 10 // Large catch-up: bigger batches
    } else if pending > 1_000 {
        base_batch * 5
    } else if pending > 100 {
        base_batch * 2
    } else {
        base_batch // Near live: small batches
    };

    // Memory-aware adjustment
    if mem_mb < 1_024 {
        // Low memory: halve the batch size, minimum 1
        std::cmp::max(pending_batch / 2, 1)
    } else if mem_mb > 8_192 {
        // High memory: double the batch size
        pending_batch * 2
    } else {
        pending_batch
    }
}

pub struct Consumer {
    nats: NatsClient,
    indexer: Indexer,
    pipeline: Arc<PipelineState>,
    catch_up: bool,
    batch_size: usize,
}

impl Consumer {
    pub fn new(
        nats: NatsClient,
        indexer: Indexer,
        pipeline: Arc<PipelineState>,
        catch_up: bool,
        batch_size: usize,
    ) -> Self {
        Self {
            nats,
            indexer,
            pipeline,
            catch_up,
            batch_size,
        }
    }

    /// Run all stream consumers concurrently.
    pub async fn run(&self) -> Result<()> {
        let js = jetstream::new(self.nats.clone());
        let pipeline = self.pipeline.clone();

        let handles = vec![
            tokio::spawn(Self::consume_stream(
                js.clone(),
                "QONDUIT_TICK",
                "qonduit-processor-ticks",
                |payload, indexer| async move { indexer.index_tick(&payload).await },
                self.indexer.clone(),
                pipeline.clone(),
                self.catch_up,
                self.batch_size,
                StreamLag::Tick,
            )),
            tokio::spawn(Self::consume_stream(
                js.clone(),
                "QONDUIT_TX",
                "qonduit-processor-tx",
                |payload, indexer| async move { indexer.index_transaction(&payload).await },
                self.indexer.clone(),
                pipeline.clone(),
                self.catch_up,
                self.batch_size,
                StreamLag::Tx,
            )),
            tokio::spawn(Self::consume_stream(
                js.clone(),
                "QONDUIT_ENTITY",
                "qonduit-processor-entities",
                |payload, indexer| async move { indexer.index_entity(&payload).await },
                self.indexer.clone(),
                pipeline.clone(),
                self.catch_up,
                self.batch_size,
                StreamLag::Entity,
            )),
            tokio::spawn(Self::consume_stream(
                js.clone(),
                "QONDUIT_COMPUTORS",
                "qonduit-processor-computors",
                |payload, indexer| async move { indexer.index_computors(&payload).await },
                self.indexer.clone(),
                pipeline.clone(),
                self.catch_up,
                self.batch_size,
                StreamLag::None,
            )),
            tokio::spawn(Self::consume_stream(
                js.clone(),
                "QONDUIT_ASSET",
                "qonduit-processor-assets",
                |payload, indexer| async move { indexer.index_asset(&payload).await },
                self.indexer.clone(),
                pipeline.clone(),
                self.catch_up,
                self.batch_size,
                StreamLag::None,
            )),
            tokio::spawn(Self::consume_stream(
                js.clone(),
                "QONDUIT_CONTRACT",
                "qonduit-processor-contracts",
                |payload, indexer| async move { indexer.index_contract_ipo(&payload).await },
                self.indexer.clone(),
                pipeline.clone(),
                self.catch_up,
                self.batch_size,
                StreamLag::None,
            )),
            tokio::spawn(Self::consume_stream(
                js.clone(),
                "QONDUIT_CUSTMSG",
                "qonduit-processor-custmsg",
                |payload, indexer| async move { indexer.index_custom_message(&payload).await },
                self.indexer.clone(),
                pipeline.clone(),
                self.catch_up,
                self.batch_size,
                StreamLag::None,
            )),
            tokio::spawn(Self::consume_stream(
                js.clone(),
                "QONDUIT_SPECTRUM",
                "qonduit-processor-spectrum",
                |payload, indexer| async move { indexer.index_spectrum(&payload).await },
                self.indexer.clone(),
                pipeline.clone(),
                self.catch_up,
                self.batch_size,
                StreamLag::None,
            )),
            tokio::spawn(Self::consume_stream(
                js.clone(),
                "QONDUIT_LOG",
                "qonduit-processor-log",
                |payload, indexer| async move { indexer.index_log_events(&payload).await },
                self.indexer.clone(),
                pipeline.clone(),
                self.catch_up,
                self.batch_size,
                StreamLag::None,
            )),
            tokio::spawn(Self::consume_stream(
                js.clone(),
                "QONDUIT_TICKVOTE",
                "qonduit-processor-tickvote",
                |payload, indexer| async move { indexer.index_tick_vote(&payload).await },
                self.indexer.clone(),
                pipeline.clone(),
                self.catch_up,
                self.batch_size,
                StreamLag::None,
            )),
            tokio::spawn(Self::consume_stream(
                js.clone(),
                "QONDUIT_ORACLE",
                "qonduit-processor-oracle",
                |payload, indexer| async move { indexer.index_oracle(&payload).await },
                self.indexer.clone(),
                pipeline.clone(),
                self.catch_up,
                self.batch_size,
                StreamLag::None,
            )),
        ];

        for handle in handles {
            if let Err(e) = handle.await {
                error!("Consumer task panicked: {e}");
            }
        }

        Ok(())
    }

    async fn consume_stream<F, Fut>(
        js: jetstream::Context,
        stream_name: &str,
        durable_name: &str,
        handler: F,
        indexer: Indexer,
        pipeline: Arc<PipelineState>,
        catch_up: bool,
        batch_size: usize,
        lag_target: StreamLag,
    ) where
        F: Fn(Vec<u8>, Indexer) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        // Get or create the stream
        let stream = match js.get_stream(stream_name).await {
            Ok(s) => s,
            Err(e) => {
                warn!("Stream {stream_name} not found: {e}, skipping");
                return;
            }
        };

        // Choose deliver policy based on catch_up mode
        let deliver_policy = if catch_up {
            DeliverPolicy::All
        } else {
            DeliverPolicy::New
        };

        // In catch-up mode, delete existing durable consumers so they're
        // recreated with DeliverPolicy::All. Otherwise the old consumer (from
        // a previous run with DeliverPolicy::New) is returned unchanged and
        // ignores the new deliver_policy.
        if catch_up {
            if let Err(e) = stream.delete_consumer(durable_name).await {
                // Consumer may not exist yet, which is fine
                debug!("Delete existing consumer {durable_name}: {e}");
            } else {
                info!("Deleted existing consumer {durable_name} for catch-up recreation");
            }
        }

        // Create durable pull consumer (or get existing one)
        let mut consumer: PullConsumer = match stream
            .create_consumer(jetstream::consumer::pull::Config {
                durable_name: Some(durable_name.to_string()),
                deliver_policy,
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                max_deliver: 5,
                ..Default::default()
            })
            .await
        {
            Ok(c) => c,
            Err(_) => match stream.get_consumer(durable_name).await {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to create/get consumer {durable_name}: {e}");
                    return;
                }
            },
        };

        // Get initial pending count and calculate dynamic batch size
        let mem_mb = available_memory_mb();
        let initial_pending = consumer
            .info()
            .await
            .map(|info| info.num_pending)
            .unwrap_or(0);

        let dynamic_batch = calculate_batch(batch_size, initial_pending, mem_mb);
        info!(
            "Consuming {stream_name} as {durable_name} (catch_up={catch_up}, base_batch={batch_size}, dynamic_batch={dynamic_batch}, pending={initial_pending}, mem_mb={mem_mb})"
        );

        // Log initial pending count so we can see catch-up progress
        if initial_pending > 0 {
            info!(
                stream = stream_name,
                pending = initial_pending,
                "Stream has {initial_pending} pending messages"
            );
        }

        let mut consumer = consumer;
        let mut current_batch = dynamic_batch;

        loop {
            let mut messages = match consumer
                .fetch()
                .max_messages(current_batch)
                .expires(std::time::Duration::from_secs(5))
                .messages()
                .await
            {
                Ok(m) => m,
                Err(e) => {
                    debug!("Fetch error on {stream_name}: {e}");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
            };

            while let Some(msg) = messages.next().await {
                match msg {
                    Ok(msg) => {
                        let payload = msg.payload.to_vec();
                        if let Err(e) = handler(payload, indexer.clone()).await {
                            warn!("Handler error on {stream_name}: {e}");
                            let _ = msg.ack_with(AckKind::Nak(None)).await;
                        } else {
                            let _ = msg.ack().await;
                        }
                    }
                    Err(e) => {
                        warn!("Message error on {stream_name}: {e}");
                    }
                }
            }

            // Update lag metrics and recalculate dynamic batch size
            match consumer.info().await {
                Ok(info) => {
                    let pending = info.num_pending;

                    // Recalculate batch size based on current pending count and memory
                    current_batch = calculate_batch(batch_size, pending, mem_mb);

                    if lag_target != StreamLag::None {
                        match lag_target {
                            StreamLag::Tick => {
                                pipeline.tick_lag.store(pending, std::sync::atomic::Ordering::Relaxed);
                            }
                            StreamLag::Tx => {
                                pipeline.tx_lag.store(pending, std::sync::atomic::Ordering::Relaxed);
                            }
                            StreamLag::Entity => {
                                pipeline.entity_lag.store(pending, std::sync::atomic::Ordering::Relaxed);
                            }
                            StreamLag::None => {}
                        }
                    }

                    debug!(
                        stream = stream_name,
                        pending,
                        batch = current_batch,
                        delivered = info.delivered.stream_sequence,
                        acked = info.ack_floor.stream_sequence,
                        "Consumer lag updated"
                    );
                }
                Err(e) => {
                    warn!(
                        stream = stream_name,
                        "Failed to fetch consumer info for lag tracking: {e}"
                    );
                }
            }
        }
    }
}

/// Identifies which lag counter a stream maps to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamLag {
    Tick,
    Tx,
    Entity,
    /// No lag tracking for this stream.
    None,
}
