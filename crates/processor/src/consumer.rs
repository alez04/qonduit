//! NATS JetStream consumer: subscribes to event streams and dispatches to indexers.

use anyhow::Result;
use async_nats::jetstream::{self, AckKind};
use async_nats::jetstream::consumer::PullConsumer;
use async_nats::Client as NatsClient;
use futures_util::StreamExt;
use tracing::{debug, error, info, warn};

use crate::indexer::Indexer;

pub struct Consumer {
    nats: NatsClient,
    indexer: Indexer,
}

impl Consumer {
    pub fn new(nats: NatsClient, indexer: Indexer) -> Self {
        Self { nats, indexer }
    }

    /// Run all stream consumers concurrently.
    pub async fn run(&self) -> Result<()> {
        let js = jetstream::new(self.nats.clone());

        let handles = vec![
            tokio::spawn(Self::consume_stream(
                js.clone(),
                "QONDUIT_TICK",
                "qonduit-processor-ticks",
                |payload, indexer| async move { indexer.index_tick(&payload).await },
                self.indexer.clone(),
            )),
            tokio::spawn(Self::consume_stream(
                js.clone(),
                "QONDUIT_TX",
                "qonduit-processor-tx",
                |payload, indexer| async move { indexer.index_transaction(&payload).await },
                self.indexer.clone(),
            )),
            tokio::spawn(Self::consume_stream(
                js.clone(),
                "QONDUIT_ENTITY",
                "qonduit-processor-entities",
                |payload, indexer| async move { indexer.index_entity(&payload).await },
                self.indexer.clone(),
            )),
            tokio::spawn(Self::consume_stream(
                js.clone(),
                "QONDUIT_COMPUTORS",
                "qonduit-processor-computors",
                |payload, indexer| async move { indexer.index_computors(&payload).await },
                self.indexer.clone(),
            )),
            tokio::spawn(Self::consume_stream(
                js.clone(),
                "QONDUIT_ASSET",
                "qonduit-processor-assets",
                |payload, indexer| async move { indexer.index_asset(&payload).await },
                self.indexer.clone(),
            )),
            tokio::spawn(Self::consume_stream(
                js.clone(),
                "QONDUIT_CONTRACT",
                "qonduit-processor-contracts",
                |payload, indexer| async move { indexer.index_contract_ipo(&payload).await },
                self.indexer.clone(),
            )),
            tokio::spawn(Self::consume_stream(
                js.clone(),
                "QONDUIT_CUSTMSG",
                "qonduit-processor-custmsg",
                |payload, indexer| async move { indexer.index_custom_message(&payload).await },
                self.indexer.clone(),
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

        // Create durable pull consumer (or get existing one)
        let consumer: PullConsumer = match stream
            .create_consumer(jetstream::consumer::pull::Config {
                durable_name: Some(durable_name.to_string()),
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

        info!("Consuming {stream_name} as {durable_name}");

        loop {
            let mut messages = match consumer
                .fetch()
                .max_messages(10)
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
        }
    }
}
