//! NATS JetStream consumer: subscribes to event streams and dispatches to indexers.

use std::sync::Arc;

use anyhow::Result;
use async_nats::jetstream::{self, consumer::DeliverPolicy};
use async_nats::jetstream::consumer::PullConsumer;
use async_nats::Client as NatsClient;
use futures_util::stream::{StreamExt, FuturesUnordered};
use qonduit_core::PipelineState;
use tracing::{debug, error, info, warn};

use crate::indexer::Indexer;

pub struct Consumer {
    nats: NatsClient,
    indexer: Indexer,
    pipeline: Arc<PipelineState>,
    catch_up: bool,
    batch_size: usize,
    concurrency: usize,
}

impl Consumer {
    pub fn new(
        nats: NatsClient,
        indexer: Indexer,
        pipeline: Arc<PipelineState>,
        catch_up: bool,
        batch_size: usize,
        concurrency: usize,
    ) -> Self {
        Self {
            nats,
            indexer,
            pipeline,
            catch_up,
            batch_size,
            concurrency,
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
                self.concurrency,
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
                self.concurrency,
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
                self.concurrency,
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
                self.concurrency,
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
                self.concurrency,
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
                self.concurrency,
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
                self.concurrency,
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
                self.concurrency,
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
                self.concurrency,
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
                self.concurrency,
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
                self.concurrency,
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
        concurrency: usize,
        lag_target: StreamLag,
    ) where
        F: Fn(Vec<u8>, Indexer) -> Fut + Sync + Send + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
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
        let initial_pending = consumer
            .info()
            .await
            .map(|info| info.num_pending)
            .unwrap_or(0);

        info!(
            "Consuming {stream_name} as {durable_name} (catch_up={catch_up}, batch={batch_size}, concurrency={concurrency}, pending={initial_pending})"
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
        let mut current_batch = batch_size;
        let handler = Arc::new(handler);

        loop {
            let messages = match consumer
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

            // Collect the batch into owned messages, then process concurrently.
            // This avoids higher-ranked lifetime issues with stream combinators.
            let mut batch_items: Vec<_> = Vec::with_capacity(current_batch);
            futures_util::pin_mut!(messages);
            while let Some(msg_result) = messages.next().await {
                match msg_result {
                    Ok(msg) => batch_items.push(msg),
                    Err(e) => {
                        warn!("Message error on {stream_name}: {e}");
                    }
                }
            }

            if batch_items.is_empty() {
                // No messages available, brief pause before next fetch
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }

            // Process the batch concurrently using FuturesUnordered.
            // Each message handler runs independently, up to `concurrency` at once.
            let mut futs: FuturesUnordered<_> = FuturesUnordered::new();

            for msg in batch_items {
                let handler = handler.clone();
                let indexer = indexer.clone();
                futs.push(async move {
                    let payload = msg.payload.to_vec();
                    let result = handler(payload, indexer).await;
                    (msg, result)
                });
            }

            while let Some((msg, result)) = futs.next().await {
                match result {
                    Ok(()) => {
                        let _ = msg.ack().await;
                    }
                    Err(e) => {
                        warn!("Handler error on {stream_name} (acking): {e}");
                        let _ = msg.ack().await;
                    }
                }
            }

            // Update lag metrics and dynamically adjust batch size
            match consumer.info().await {
                Ok(info) => {
                    let pending = info.num_pending;

                    // Scale batch size up when far behind, down when near live
                    current_batch = if pending > 10_000 {
                        batch_size * 10
                    } else if pending > 1_000 {
                        batch_size * 5
                    } else if pending > 100 {
                        batch_size * 2
                    } else {
                        batch_size
                    };

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
