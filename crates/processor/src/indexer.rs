//! Indexer: writes decoded events to RocksDB via the warm storage layer.
//!
//! Each `index_*` method deserializes a JSON payload from NATS and writes
//! the appropriate keys and values into the warm tier column families.

use std::sync::Arc;

use anyhow::{Context, Result};
use qonduit_core::{AssetRecord, Computors, ContractIpo, EntityData, TickData, Transaction};
use qonduit_storage::WarmStorage;
use tracing::debug;

#[derive(Clone)]
pub struct Indexer {
    storage: Arc<WarmStorage>,
}

impl Indexer {
    pub fn new(storage: Arc<WarmStorage>) -> Self {
        Self { storage }
    }

    // ------------------------------------------------------------------
    // Tick
    // ------------------------------------------------------------------

    /// Index a tick event: store tick data, update current tick and epoch.
    pub async fn index_tick(&self, payload: &[u8]) -> Result<()> {
        let tick: TickData =
            serde_json::from_slice(payload).context("Failed to deserialize TickData")?;

        self.storage.put_tick(tick.tick, payload)?;

        // Update current tick if newer
        if let Ok(current) = self.storage.get_current_tick() {
            if current.is_none_or(|c| tick.tick > c) {
                self.storage.set_current_tick(tick.tick)?;
            }
        }

        // Always update epoch
        self.storage.set_current_epoch(tick.epoch)?;

        debug!(tick = tick.tick, epoch = tick.epoch, "Indexed tick");
        Ok(())
    }

    // ------------------------------------------------------------------
    // Transaction
    // ------------------------------------------------------------------

    /// Index a transaction: store by hash, index by tick and by source entity.
    pub async fn index_transaction(&self, payload: &[u8]) -> Result<()> {
        let tx: Transaction =
            serde_json::from_slice(payload).context("Failed to deserialize Transaction")?;

        // Decode the transaction hash (base-26 identity string, 60 chars)
        let hash_bytes = qonduit_core::decode_base26(&tx.hash).unwrap_or_else(|| {
            // Fallback: try hex decode
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

        // Store the transaction payload keyed by hash
        self.storage.put_tx(&hash_bytes, payload)?;

        // Index by tick. Use trailing bytes of the hash as a tx_index to
        // avoid collisions (the ingestion layer doesn't currently attach a
        // per-tick index).
        let tx_index = u32::from_be_bytes([
            hash_bytes[28],
            hash_bytes[29],
            hash_bytes[30],
            hash_bytes[31],
        ]);
        self.storage.put_tx_for_tick(tx.tick, tx_index, &hash_bytes)?;

        // Index by source entity
        if let Some(source_key) = qonduit_core::decode_base26(&tx.source_identity) {
            self.storage
                .put_tx_for_entity(&source_key, tx.tick, tx_index, &hash_bytes)?;
        } else if let Some(src) = hex::decode(&tx.source_hex)
            .ok()
            .filter(|b| b.len() == 32)
        {
            let mut key = [0u8; 32];
            key.copy_from_slice(&src);
            self.storage
                .put_tx_for_entity(&key, tx.tick, tx_index, &hash_bytes)?;
        }

        debug!(tick = tx.tick, hash = %tx.hash, "Indexed transaction");
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

        debug!(index = asset.issuance_index, name = %asset.name, "Indexed asset");
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
    // Custom message
    // ------------------------------------------------------------------

    /// Index a custom message.
    pub async fn index_custom_message(&self, payload: &[u8]) -> Result<()> {
        let msg: serde_json::Value =
            serde_json::from_slice(payload).context("Failed to deserialize custom message")?;

        let tick = msg["tick"].as_u64().unwrap_or(0) as u32;
        let index = msg["message_type"].as_u64().unwrap_or(0) as u32;

        self.storage.put_custom_message(tick, index, payload)?;

        debug!(tick = tick, index = index, "Indexed custom message");
        Ok(())
    }
}
