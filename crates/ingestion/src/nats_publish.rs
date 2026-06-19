//! NATS JetStream publisher helpers.
//!
//! Provides typed publish methods for each Qonduit event type.
//! All messages are serialized as JSON and published to the appropriate
//! JetStream subject: `QONDUIT.{TYPE}`.

use anyhow::{Context, Result};
use async_nats::jetstream;
use tracing::debug;

use qonduit_core::{
    AssetRecord, Computors, ContractIpo, EntityData, TickData, Transaction,
};

/// Placeholder for custom (broadcast) messages.
///
/// Full decoding is pending; for now we publish the raw payload as hex.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CustomMessage {
    pub tick: u32,
    pub message_type: u32,
    pub payload_hex: String,
}

/// A computor's tick vote.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TickVote {
    pub computor_index: u16,
    pub epoch: u16,
    pub tick: u32,
}

/// Publishes Qonduit events to NATS JetStream.
///
/// Each publish method serializes the typed struct to JSON and publishes
/// to the appropriate `QONDUIT.*` subject.
#[derive(Debug, Clone)]
pub struct NatsPublisher {
    js: jetstream::Context,
}

impl NatsPublisher {
    /// Create a new publisher from a NATS client.
    pub fn new(nats: async_nats::Client) -> Self {
        Self {
            js: jetstream::new(nats),
        }
    }

    /// Create a publisher directly from a JetStream context.
    pub fn from_context(js: jetstream::Context) -> Self {
        Self { js }
    }

    /// Publish a tick to `QONDUIT.TICK`.
    pub async fn publish_tick(&self, epoch: u16, tick: &TickData) -> Result<()> {
        let subject = "QONDUIT.TICK".to_string();
        let payload = serde_json::to_vec(tick).context("Failed to serialize TickData")?;
        self.js
            .publish(subject.clone(), payload.into())
            .await
            .with_context(|| format!("Failed to publish to {subject}"))?
            .await
            .with_context(|| format!("Publish ack failed for {subject}"))?;
        debug!("Published tick epoch={epoch}, tick={}", tick.tick);
        Ok(())
    }

    /// Publish a transaction to `QONDUIT.TX`.
    pub async fn publish_tx(&self, epoch: u16, tx: &Transaction) -> Result<()> {
        let subject = "QONDUIT.TX".to_string();
        let payload = serde_json::to_vec(tx).context("Failed to serialize Transaction")?;
        self.js
            .publish(subject.clone(), payload.into())
            .await
            .with_context(|| format!("Failed to publish to {subject}"))?
            .await
            .with_context(|| format!("Publish ack failed for {subject}"))?;
        debug!("Published tx epoch={epoch} to QONDUIT.TX");
        Ok(())
    }

    /// Publish an entity to `QONDUIT.ENTITY`.
    pub async fn publish_entity(&self, _epoch: u16, entity: &EntityData) -> Result<()> {
        let subject = "QONDUIT.ENTITY".to_string();
        let payload = serde_json::to_vec(entity).context("Failed to serialize EntityData")?;
        self.js
            .publish(subject.clone(), payload.into())
            .await
            .with_context(|| format!("Failed to publish to {subject}"))?
            .await
            .with_context(|| format!("Publish ack failed for {subject}"))?;
        debug!("Published entity to {subject}");
        Ok(())
    }

    /// Publish computors to `QONDUIT.COMPUTORS`.
    pub async fn publish_computors(&self, epoch: u16, computors: &Computors) -> Result<()> {
        let subject = "QONDUIT.COMPUTORS".to_string();
        let payload =
            serde_json::to_vec(computors).context("Failed to serialize Computors")?;
        self.js
            .publish(subject.clone(), payload.into())
            .await
            .with_context(|| format!("Failed to publish to {subject}"))?
            .await
            .with_context(|| format!("Publish ack failed for {subject}"))?;
        debug!("Published computors epoch={epoch} to {subject}");
        Ok(())
    }

    /// Publish a custom (broadcast) message to `QONDUIT.CUSTMSG`.
    pub async fn publish_custom_message(
        &self,
        epoch: u16,
        tick: u32,
        msg: &CustomMessage,
    ) -> Result<()> {
        let subject = "QONDUIT.CUSTMSG".to_string();
        let payload = serde_json::to_vec(msg).context("Failed to serialize CustomMessage")?;
        self.js
            .publish(subject.clone(), payload.into())
            .await
            .with_context(|| format!("Failed to publish to {subject}"))?
            .await
            .with_context(|| format!("Publish ack failed for {subject}"))?;
        debug!("Published custom message epoch={epoch} tick={tick} to {subject}");
        Ok(())
    }

    /// Publish oracle data to `QONDUIT.ORACLE`.
    pub async fn publish_oracle(
        &self,
        epoch: u16,
        tick: u32,
        data: &serde_json::Value,
    ) -> Result<()> {
        let subject = "QONDUIT.ORACLE".to_string();
        let payload = serde_json::to_vec(data).context("Failed to serialize oracle data")?;
        self.js
            .publish(subject.clone(), payload.into())
            .await
            .with_context(|| format!("Failed to publish to {subject}"))?
            .await
            .with_context(|| format!("Publish ack failed for {subject}"))?;
        debug!("Published oracle epoch={epoch} tick={tick} to {subject}");
        Ok(())
    }

    /// Publish an asset record to `QONDUIT.ASSET`.
    pub async fn publish_asset(&self, epoch: u16, asset: &AssetRecord) -> Result<()> {
        let subject = "QONDUIT.ASSET".to_string();
        let payload = serde_json::to_vec(asset).context("Failed to serialize AssetRecord")?;
        self.js
            .publish(subject.clone(), payload.into())
            .await
            .with_context(|| format!("Failed to publish to {subject}"))?
            .await
            .with_context(|| format!("Publish ack failed for {subject}"))?;
        debug!("Published asset epoch={epoch} to {subject}");
        Ok(())
    }

    /// Publish a contract IPO to `QONDUIT.CONTRACT`.
    pub async fn publish_contract_ipo(&self, epoch: u16, ipo: &ContractIpo) -> Result<()> {
        let subject = "QONDUIT.CONTRACT".to_string();
        let payload = serde_json::to_vec(ipo).context("Failed to serialize ContractIpo")?;
        self.js
            .publish(subject.clone(), payload.into())
            .await
            .with_context(|| format!("Failed to publish to {subject}"))?
            .await
            .with_context(|| format!("Publish ack failed for {subject}"))?;
        debug!("Published contract IPO epoch={epoch} to {subject}");
        Ok(())
    }

    /// Publish contract function response to `QONDUIT.CFNR`.
    pub async fn publish_contract_fn(
        &self,
        epoch: u16,
        dejavu: u32,
        data: &[u8],
    ) -> Result<()> {
        let subject = "QONDUIT.CFNR".to_string();
        let payload = serde_json::to_vec(&serde_json::json!({
            "dejavu": dejavu,
            "data_hex": hex::encode(data),
        }))
        .context("Failed to serialize contract function data")?;
        self.js
            .publish(subject.clone(), payload.into())
            .await
            .with_context(|| format!("Failed to publish to {subject}"))?
            .await
            .with_context(|| format!("Publish ack failed for {subject}"))?;
        debug!("Published contract fn epoch={epoch} dejavu={dejavu} to {subject}");
        Ok(())
    }

    /// Publish a tick vote to `QONDUIT.TICKVOTE`.
    pub async fn publish_tick_vote(&self, epoch: u16, vote: &TickVote) -> Result<()> {
        let subject = "QONDUIT.TICKVOTE".to_string();
        let payload = serde_json::to_vec(vote).context("Failed to serialize TickVote")?;
        self.js
            .publish(subject.clone(), payload.into())
            .await
            .with_context(|| format!("Failed to publish to {subject}"))?
            .await
            .with_context(|| format!("Publish ack failed for {subject}"))?;
        debug!("Published tick vote epoch={epoch} tick={}", vote.tick);
        Ok(())
    }
}
