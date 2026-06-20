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

        info!(
            "Consuming {stream_name} as {durable_name} (catch_up={catch_up}, batch={batch_size})"
        );

        // Log initial pending count so we can see catch-up progress
        match consumer.info().await {
            Ok(info) => {
                info!(
                    stream = stream_name,
                    pending = info.num_pending,
                    "Stream has {} pending messages",
                    info.num_pending
                );
            }
            Err(e) => {
                warn!(
                    stream = stream_name,
                    "Could not fetch initial consumer info: {e}"
                );
            }
        }

        let mut consumer = consumer;

        loop {
            let mut messages = match consumer
                .fetch()
                .max_messages(batch_size)
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

            // Update lag metrics by querying consumer info after each batch
            if lag_target != StreamLag::None {
                match consumer.info().await {
                    Ok(info) => {
                        let pending = info.num_pending;
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
                        debug!(
                            stream = stream_name,
                            pending,
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
