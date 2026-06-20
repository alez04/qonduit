//! NATS JetStream publisher helpers.
//!
//! Provides typed publish methods for each Qonduit event type.
//! All messages are serialized as JSON and published to the appropriate
//! JetStream subject: `QONDUIT.{TYPE}`.

use anyhow::{Context, Result};
use async_nats::jetstream;
use tracing::debug;

use qonduit_core::{
    AssetRecord, Computors, ContractIpo, CustomMiningSolution, CustomMiningTask,
    EntityData, LogStateDigest, OracleDataResponse, QuorumTick, TickData, Transaction,
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
    /// When true, publishes fire-and-forget (skip ack wait).
    /// Use during catch-up to maximize throughput.
    fire_and_forget: bool,
}

impl NatsPublisher {
    /// Create a new publisher from a NATS client.
    pub fn new(nats: async_nats::Client) -> Self {
        Self {
            js: jetstream::new(nats),
            fire_and_forget: false,
        }
    }

    /// Create a publisher directly from a JetStream context.
    pub fn from_context(js: jetstream::Context) -> Self {
        Self { js, fire_and_forget: false }
    }

    /// Enable fire-and-forget mode (skip ack wait for higher throughput).
    pub fn set_fire_and_forget(&mut self, enabled: bool) {
        self.fire_and_forget = enabled;
    }

    /// Internal helper: publish and optionally wait for ack.
    async fn do_publish(&self, subject: &str, payload: bytes::Bytes) -> Result<()> {
        let publish_ack = self.js
            .publish(subject.to_string(), payload)
            .await
            .with_context(|| format!("Failed to publish to {subject}"))?;

        if !self.fire_and_forget {
            publish_ack
                .await
                .with_context(|| format!("Publish ack failed for {subject}"))?;
        }
        Ok(())
    }

    /// Publish a tick to `Q.{epoch}.QONDUIT.TICK`.
    pub async fn publish_tick(&self, epoch: u16, tick: &TickData) -> Result<()> {
        let subject = format!("Q.{epoch}.QONDUIT.TICK");
        // Strip large hex fields before NATS publish to stay under 1MB payload limit.
        // Transaction digests (131KB raw -> 262KB hex) blow up the JSON size.
        // These fields are stored directly in RocksDB by the decoder, not needed in NATS.
        let mut slim = tick.clone();
        slim.transaction_digests_hex.clear();
        slim.contract_fees_hex.clear();
        slim.signature_hex.clear();
        let payload = serde_json::to_vec(&slim).context("Failed to serialize TickData")?;
        self.do_publish(&subject, payload.into()).await?;
        debug!("Published tick epoch={epoch}, tick={}", tick.tick);
        Ok(())
    }

    /// Publish a transaction to `Q.{epoch}.QONDUIT.TX`.
    pub async fn publish_tx(&self, epoch: u16, tx: &Transaction) -> Result<()> {
        let subject = format!("Q.{epoch}.QONDUIT.TX");
        let payload = serde_json::to_vec(tx).context("Failed to serialize Transaction")?;
        self.do_publish(&subject, payload.into()).await?;
        debug!("Published tx epoch={epoch} to QONDUIT.TX");
        Ok(())
    }

    /// Publish an entity to `Q.{epoch}.QONDUIT.ENTITY`.
    pub async fn publish_entity(&self, _epoch: u16, entity: &EntityData) -> Result<()> {
        let subject = format!("Q.{_epoch}.QONDUIT.ENTITY");
        let payload = serde_json::to_vec(entity).context("Failed to serialize EntityData")?;
        self.do_publish(&subject, payload.into()).await?;
        debug!("Published entity to {subject}");
        Ok(())
    }

    /// Publish computors to `Q.{epoch}.QONDUIT.COMPUTORS`.
    pub async fn publish_computors(&self, epoch: u16, computors: &Computors) -> Result<()> {
        let subject = format!("Q.{epoch}.QONDUIT.COMPUTORS");
        let payload =
            serde_json::to_vec(computors).context("Failed to serialize Computors")?;
        self.do_publish(&subject, payload.into()).await?;
        debug!("Published computors epoch={epoch} to {subject}");
        Ok(())
    }

    /// Publish a custom (broadcast) message to `Q.{epoch}.QONDUIT.CUSTMSG`.
    pub async fn publish_custom_message(
        &self,
        epoch: u16,
        tick: u32,
        msg: &CustomMessage,
    ) -> Result<()> {
        let subject = format!("Q.{epoch}.QONDUIT.CUSTMSG");
        let payload = serde_json::to_vec(msg).context("Failed to serialize CustomMessage")?;
        self.do_publish(&subject, payload.into()).await?;
        debug!("Published custom message epoch={epoch} tick={tick} to {subject}");
        Ok(())
    }

    /// Publish oracle data to `Q.{epoch}.QONDUIT.ORACLE`.
    pub async fn publish_oracle(
        &self,
        epoch: u16,
        tick: u32,
        data: &serde_json::Value,
    ) -> Result<()> {
        let subject = format!("Q.{epoch}.QONDUIT.ORACLE");
        let payload = serde_json::to_vec(data).context("Failed to serialize oracle data")?;
        self.do_publish(&subject, payload.into()).await?;
        debug!("Published oracle epoch={epoch} tick={tick} to {subject}");
        Ok(())
    }

    /// Publish an asset record to `Q.{epoch}.QONDUIT.ASSET`.
    pub async fn publish_asset(&self, epoch: u16, asset: &AssetRecord) -> Result<()> {
        let subject = format!("Q.{epoch}.QONDUIT.ASSET");
        let payload = serde_json::to_vec(asset).context("Failed to serialize AssetRecord")?;
        self.do_publish(&subject, payload.into()).await?;
        debug!("Published asset epoch={epoch} to {subject}");
        Ok(())
    }

    /// Publish a contract IPO to `Q.{epoch}.QONDUIT.CONTRACT`.
    pub async fn publish_contract_ipo(&self, epoch: u16, ipo: &ContractIpo) -> Result<()> {
        let subject = format!("Q.{epoch}.QONDUIT.CONTRACT");
        let payload = serde_json::to_vec(ipo).context("Failed to serialize ContractIpo")?;
        self.do_publish(&subject, payload.into()).await?;
        debug!("Published contract IPO epoch={epoch} to {subject}");
        Ok(())
    }

    /// Publish contract function response to `Q.{epoch}.QONDUIT.CFNR`.
    pub async fn publish_contract_fn(
        &self,
        epoch: u16,
        dejavu: u32,
        data: &[u8],
    ) -> Result<()> {
        let subject = format!("Q.{epoch}.QONDUIT.CFNR");
        let payload = serde_json::to_vec(&serde_json::json!({
            "dejavu": dejavu,
            "data_hex": hex::encode(data),
        }))
        .context("Failed to serialize contract function data")?;
        self.do_publish(&subject, payload.into()).await?;
        debug!("Published contract fn epoch={epoch} dejavu={dejavu} to {subject}");
        Ok(())
    }

    /// Publish a tick vote to `Q.{epoch}.QONDUIT.TICKVOTE`.
    pub async fn publish_tick_vote(&self, epoch: u16, vote: &TickVote) -> Result<()> {
        let subject = format!("Q.{epoch}.QONDUIT.TICKVOTE");
        let payload = serde_json::to_vec(vote).context("Failed to serialize TickVote")?;
        self.do_publish(&subject, payload.into()).await?;
        debug!("Published tick vote epoch={epoch} tick={}", vote.tick);
        Ok(())
    }

    /// Publish a spectrum entry to `Q.{epoch}.QONDUIT.SPECTRUM`.
    pub async fn publish_spectrum(
        &self,
        epoch: u16,
        entry: &serde_json::Value,
    ) -> Result<()> {
        let subject = format!("Q.{epoch}.QONDUIT.SPECTRUM");
        let payload =
            serde_json::to_vec(entry).context("Failed to serialize spectrum entry")?;
        self.do_publish(&subject, payload.into()).await?;
        debug!("Published spectrum entry epoch={epoch} to {subject}");
        Ok(())
    }

    /// Publish decoded log events to `Q.{epoch}.QONDUIT.LOG`.
    pub async fn publish_log_events(
        &self,
        epoch: u16,
        events: &[qonduit_core::LogEvent],
    ) -> Result<()> {
        let subject = format!("Q.{epoch}.QONDUIT.LOG");
        let payload =
            serde_json::to_vec(events).context("Failed to serialize log events")?;
        self.do_publish(&subject, payload.into()).await?;
        debug!(
            count = events.len(),
            epoch = epoch,
            "Published log events to {subject}"
        );
        Ok(())
    }

    /// Publish an aggregated quorum tick to `Q.{epoch}.QONDUIT.QUORUM`.
    pub async fn publish_quorum_tick(&self, epoch: u16, qt: &QuorumTick) -> Result<()> {
        let subject = format!("Q.{epoch}.QONDUIT.QUORUM");
        let payload = serde_json::to_vec(qt).context("Failed to serialize QuorumTick")?;
        self.do_publish(&subject, payload.into()).await?;
        debug!(
            "Published quorum tick epoch={epoch} tick={} votes={}",
            qt.tick, qt.vote_count
        );
        Ok(())
    }

    /// Publish a log state digest to `Q.{epoch}.QONDUIT.LOGDIGEST`.
    pub async fn publish_log_state_digest(&self, digest: &LogStateDigest) -> Result<()> {
        // Log state digest is not epoch-specific; use 0 as placeholder.
        let subject = "Q.0.QONDUIT.LOGDIGEST".to_string();
        let payload =
            serde_json::to_vec(digest).context("Failed to serialize LogStateDigest")?;
        self.do_publish(&subject, payload.into()).await?;
        debug!("Published log state digest to {subject}");
        Ok(())
    }

    /// Publish oracle data response to `Q.{epoch}.QONDUIT.ORACLE`.
    pub async fn publish_oracle_data(
        &self,
        epoch: u16,
        oracle: &OracleDataResponse,
    ) -> Result<()> {
        let subject = format!("Q.{epoch}.QONDUIT.ORACLE");
        let payload =
            serde_json::to_vec(oracle).context("Failed to serialize OracleDataResponse")?;
        self.do_publish(&subject, payload.into()).await?;
        debug!(
            "Published oracle data epoch={epoch} resType={}",
            oracle.res_type
        );
        Ok(())
    }

    /// Publish a custom mining task to `Q.{epoch}.QONDUIT.MINING`.
    pub async fn publish_custom_mining_task(
        &self,
        epoch: u16,
        task: &CustomMiningTask,
    ) -> Result<()> {
        let subject = format!("Q.{epoch}.QONDUIT.MINING");
        let payload =
            serde_json::to_vec(task).context("Failed to serialize CustomMiningTask")?;
        self.do_publish(&subject, payload.into()).await?;
        debug!(
            "Published custom mining task epoch={epoch} job_id={}",
            task.job_id
        );
        Ok(())
    }

    /// Publish a custom mining solution to `Q.{epoch}.QONDUIT.MINING`.
    pub async fn publish_custom_mining_solution(
        &self,
        epoch: u16,
        solution: &CustomMiningSolution,
    ) -> Result<()> {
        let subject = format!("Q.{epoch}.QONDUIT.MINING");
        let payload = serde_json::to_vec(solution)
            .context("Failed to serialize CustomMiningSolution")?;
        self.do_publish(&subject, payload.into()).await?;
        debug!(
            "Published custom mining solution epoch={epoch} job_id={}",
            solution.job_id
        );
        Ok(())
    }
}
