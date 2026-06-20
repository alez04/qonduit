//! Indexer: writes decoded events to RocksDB via the warm storage layer.
//!
//! Each `index_*` method deserializes a JSON payload from NATS and writes
//! the appropriate keys and values into the warm tier column families.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use qonduit_core::{AssetRecord, Computors, ContractIpo, EntityData, EpochStats, PipelineState, TickData, Transaction};
use qonduit_storage::WarmStorage;
use tracing::debug;

pub struct Indexer {
    storage: Arc<WarmStorage>,
    pipeline: Arc<PipelineState>,
    /// Last tick seen by index_transaction (for sequential tx_index).
    tx_last_tick: Arc<AtomicU32>,
    /// Sequential counter for transactions within the current tick.
    tx_counter: Arc<AtomicU32>,
}

impl Clone for Indexer {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
            pipeline: Arc::clone(&self.pipeline),
            tx_last_tick: Arc::clone(&self.tx_last_tick),
            tx_counter: Arc::clone(&self.tx_counter),
        }
    }
}

impl Indexer {
    pub fn new(storage: Arc<WarmStorage>, pipeline: Arc<PipelineState>) -> Self {
        Self {
            storage,
            pipeline,
            tx_last_tick: Arc::new(AtomicU32::new(0)),
            tx_counter: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Get-or-create epoch stats and apply a mutation, then persist.
    fn update_epoch_stats(&self, epoch: u16, f: impl FnOnce(&mut EpochStats)) -> Result<()> {
        let mut stats: EpochStats = match self.storage.get_epoch_stats(epoch)? {
            Some(data) => serde_json::from_slice(&data).unwrap_or_default(),
            None => EpochStats { epoch, ..Default::default() },
        };
        f(&mut stats);
        let json = serde_json::to_vec(&stats)?;
        self.storage.put_epoch_stats(epoch, &json)?;
        Ok(())
    }

    // ------------------------------------------------------------------
    // Tick
    // ------------------------------------------------------------------

    /// Index a tick event: store tick data, update current tick and epoch.
    pub async fn index_tick(&self, payload: &[u8]) -> Result<()> {
        let tick: TickData =
            serde_json::from_slice(payload).context("Failed to deserialize TickData")?;

        // Batch write: tick data + meta update in a single disk write
        let mut batch = self.storage.create_batch();
        self.storage.batch_put_tick(&mut batch, tick.tick, tick.epoch, payload);

        // Detect epoch transition before updating pipeline state
        let previous_epoch = self.pipeline.indexed_epoch.load(std::sync::atomic::Ordering::Relaxed);
        if previous_epoch > 0 && tick.epoch != previous_epoch {
            tracing::warn!(
                epoch_transition = true,
                from_epoch = previous_epoch,
                to_epoch = tick.epoch,
                tick = tick.tick,
                "Epoch transition detected: {previous_epoch} -> {} at tick {}",
                tick.epoch, tick.tick
            );

            let previous_tick = self.pipeline.indexed_tick.load(std::sync::atomic::Ordering::Relaxed);
            if previous_tick > 0 {
                let tick_range = (0, previous_tick);
                match qonduit_storage::ColdStorage::new(std::path::PathBuf::from("./data/cold")).export_epoch(
                    &self.storage,
                    previous_epoch,
                    tick_range,
                ) {
                    Ok(()) => {
                        tracing::warn!(
                            epoch = previous_epoch,
                            "Cold tier export completed for epoch {previous_epoch}"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            epoch = previous_epoch,
                            error = %e,
                            "Cold tier export failed for epoch {previous_epoch}, will retry later"
                        );
                    }
                }
            }
        }

        self.storage.batch_write(batch)?;

        // Update pipeline state
        self.pipeline.indexed_tick.store(tick.tick, std::sync::atomic::Ordering::Relaxed);
        self.pipeline.indexed_epoch.store(tick.epoch, std::sync::atomic::Ordering::Relaxed);
        self.pipeline.ticks_indexed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.pipeline.total_ticks_indexed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Update epoch statistics
        let tick_num = tick.tick;
        self.update_epoch_stats(tick.epoch, |stats| {
            stats.tick_count += 1;
            stats.first_tick = Some(stats.first_tick.unwrap_or(tick_num).min(tick_num));
            stats.last_tick = Some(stats.last_tick.unwrap_or(tick_num).max(tick_num));
        })?;

        debug!(tick = tick.tick, epoch = tick.epoch, "Indexed tick");
        Ok(())
    }

    // ------------------------------------------------------------------
    // Transaction
    // ------------------------------------------------------------------

    /// Index a transaction: store by hash, index by tick and by source entity.
    pub async fn index_transaction(&self, payload: &[u8]) -> Result<()> {
        let tx: Transaction = match serde_json::from_slice(payload) {
            Ok(tx) => tx,
            Err(e) => {
                let preview = String::from_utf8_lossy(&payload[..payload.len().min(200)]);
                tracing::warn!("Transaction deserialization failed: {e} — payload preview: {preview}");
                anyhow::bail!("Transaction deserialization failed: {e}");
            }
        };

        let hash_bytes = qonduit_core::decode_base26(&tx.hash).unwrap_or_else(|| {
            hex::decode(&tx.hash)
                .ok()
                .filter(|b| b.len() == 32)
                .map(|b| {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(&b);
                    arr
                })
                .unwrap_or([0u8; 32])
        });

        // Decode entity keys
        let source = qonduit_core::decode_base26(&tx.source_identity).or_else(|| {
            hex::decode(&tx.source_hex)
                .ok()
                .filter(|b| b.len() == 32)
                .map(|b| {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(&b);
                    arr
                })
        });
        let destination = qonduit_core::decode_base26(&tx.destination_identity).or_else(|| {
            hex::decode(&tx.destination_hex)
                .ok()
                .filter(|b| b.len() == 32)
                .map(|b| {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(&b);
                    arr
                })
        });

        // Compute sequential tx index within the tick
        let current = self.tx_last_tick.load(Ordering::Relaxed);
        if tx.tick != current {
            self.tx_last_tick.store(tx.tick, Ordering::Relaxed);
            self.tx_counter.store(0, Ordering::Relaxed);
        }
        let tx_index = self.tx_counter.fetch_add(1, Ordering::Relaxed);

        // Single batch write: payload + tick index + entity indexes
        let mut batch = self.storage.create_batch();
        self.storage.batch_put_tx(
            &mut batch,
            &hash_bytes,
            payload,
            tx.tick,
            tx_index,
            source.as_ref(),
            destination.as_ref(),
        );
        self.storage.batch_write(batch)?;

        debug!(tick = tx.tick, hash = %tx.hash, "Indexed transaction");
        self.pipeline.txs_indexed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Update epoch statistics
        let epoch = self.pipeline.indexed_epoch.load(std::sync::atomic::Ordering::Relaxed);
        if epoch > 0 {
            self.update_epoch_stats(epoch, |stats| {
                stats.tx_count += 1;
            })?;
        }

        Ok(())
    }

    // ------------------------------------------------------------------
    // Entity
    // ------------------------------------------------------------------

    /// Index an entity update.
    pub async fn index_entity(&self, payload: &[u8]) -> Result<()> {
        let entity: EntityData =
            serde_json::from_slice(payload).context("Failed to deserialize EntityData")?;

        if let Some(key) = qonduit_core::decode_base26(&entity.identity) {
            self.storage.put_entity(&key, payload)?;

            // Update pipeline state
            self.pipeline.entities_indexed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            // Update epoch statistics
            let epoch = self.pipeline.indexed_epoch.load(std::sync::atomic::Ordering::Relaxed);
            if epoch > 0 {
                self.update_epoch_stats(epoch, |stats| {
                    stats.entity_count += 1;
                })?;
            }

            debug!(identity = %entity.identity, "Indexed entity");
        }

        Ok(())
    }

    // ------------------------------------------------------------------
    // Spectrum
    // ------------------------------------------------------------------

    /// Index a spectrum entry.
    pub async fn index_spectrum(&self, payload: &[u8]) -> Result<()> {
        let entry: serde_json::Value =
            serde_json::from_slice(payload).context("Failed to deserialize spectrum entry")?;

        let identity = entry["identity"].as_str().unwrap_or("");
        if let Some(key) = qonduit_core::decode_base26(identity) {
            self.storage.put_spectrum_entry(&key, payload)?;
            debug!(identity = %identity, "Indexed spectrum entry");
        }

        Ok(())
    }

    // ------------------------------------------------------------------
    // Computors
    // ------------------------------------------------------------------

    /// Index a computors list.
    pub async fn index_computors(&self, payload: &[u8]) -> Result<()> {
        let computors: Computors =
            serde_json::from_slice(payload).context("Failed to deserialize Computors")?;

        self.storage.put_computors(computors.epoch, payload)?;

        debug!(epoch = computors.epoch, "Indexed computors");
        Ok(())
    }

    // ------------------------------------------------------------------
    // Asset
    // ------------------------------------------------------------------

    /// Index an asset record.
    pub async fn index_asset(&self, payload: &[u8]) -> Result<()> {
        let asset: AssetRecord =
            serde_json::from_slice(payload).context("Failed to deserialize AssetRecord")?;

        self.storage.put_asset(asset.issuance_index, payload)?;

        // Wire entity→asset index for issuing, owning, and possessing entities
        for entity_key in [
            asset.issuing_entity,
            asset.owning_entity,
            asset.possessing_entity,
        ] {
            // Only index non-zero entities (empty 32-byte arrays)
            if entity_key != [0u8; 32] {
                if let Err(e) = self.storage.put_entity_asset(&entity_key, asset.issuance_index) {
                    debug!(
                        index = asset.issuance_index,
                        "Failed to index entity→asset: {e}"
                    );
                }
            }
        }

        debug!(index = asset.issuance_index, name = %asset.name, "Indexed asset");
        Ok(())
    }

    // ------------------------------------------------------------------
    // Log events
    // ------------------------------------------------------------------

    /// Index a batch of log events (from BroadcastMessage type 1).
    pub async fn index_log_events(&self, payload: &[u8]) -> Result<()> {
        let events: Vec<qonduit_core::LogEvent> =
            serde_json::from_slice(payload).context("Failed to deserialize log events")?;

        for event in &events {
            let event_json = serde_json::to_vec(event)
                .context("Failed to serialize log event")?;
            self.storage.put_log_event(
                event.tick,
                event.tx_index,
                event.event_type,
                &event_json,
            )?;
        }

        debug!(count = events.len(), "Indexed log events");
        Ok(())
    }

    // ------------------------------------------------------------------
    // Tick vote aggregation
    // ------------------------------------------------------------------

    /// Index a tick vote from a computor.
    pub async fn index_tick_vote(&self, payload: &[u8]) -> Result<()> {
        let vote: serde_json::Value =
            serde_json::from_slice(payload).context("Failed to deserialize tick vote")?;

        let tick = vote["tick"].as_u64().unwrap_or(0) as u32;
        let computor_index = vote["computor_index"].as_u64().unwrap_or(0) as u16;

        self.storage.put_tick_vote(tick, computor_index, payload)?;

        // Check if we have reached quorum for this tick
        let vote_count = self.storage.count_votes_for_tick(tick).unwrap_or(0);
        if vote_count >= qonduit_core::QUORUM {
            debug!(
                tick = tick,
                votes = vote_count,
                "Quorum reached for tick"
            );
        }

        debug!(
            tick = tick,
            computor = computor_index,
            total_votes = vote_count,
            "Indexed tick vote"
        );
        Ok(())
    }

    // ------------------------------------------------------------------
    // Contract IPO
    // ------------------------------------------------------------------

    /// Index a contract IPO.
    pub async fn index_contract_ipo(&self, payload: &[u8]) -> Result<()> {
        let ipo: ContractIpo =
            serde_json::from_slice(payload).context("Failed to deserialize ContractIpo")?;

        self.storage.put_contract_ipo(ipo.contract_index, payload)?;

        debug!(contract_index = ipo.contract_index, "Indexed contract IPO");
        Ok(())
    }

    // ------------------------------------------------------------------
    // Oracle data
    // ------------------------------------------------------------------

    /// Index oracle data, keyed by tick.
    pub async fn index_oracle(&self, payload: &[u8]) -> Result<()> {
        let entry: serde_json::Value =
            serde_json::from_slice(payload).context("Failed to deserialize oracle data")?;

        let tick = entry["tick"].as_u64().unwrap_or(0) as u32;
        let tick = if tick == 0 {
            self.pipeline.indexed_tick.load(std::sync::atomic::Ordering::Relaxed)
        } else {
            tick
        };

        self.storage.put_oracle(tick, payload)?;
        debug!(tick = tick, "Indexed oracle data");
        Ok(())
    }

    // ------------------------------------------------------------------
    // Custom message
    // ------------------------------------------------------------------

    /// Index a custom message.
    pub async fn index_custom_message(&self, payload: &[u8]) -> Result<()> {
        let msg: serde_json::Value =
            serde_json::from_slice(payload).context("Failed to deserialize custom message")?;

        let tick = msg["tick"].as_u64().unwrap_or(0) as u32;

        // Use a sequential index within this tick to avoid collisions when
        // multiple messages share the same message_type. Count existing
        // entries to determine the next available index.
        let index = self.storage.count_custom_messages_for_tick(tick).unwrap_or(0) as u32;

        self.storage.put_custom_message(tick, index, payload)?;

        debug!(tick = tick, index = index, "Indexed custom message");
        Ok(())
    }
}
