//! Warm tier: RocksDB-backed index storage.
//!
//! Column families:
//! - `tick` — tick data by tick number
//! - `tx` — transactions by hash
//! - `tx_by_tick` — transaction hashes by tick (for tick history)
//! - `tx_by_entity` — transaction hashes by entity (for address history)
//! - `entity` — entity data by identity
//! - `spectrum` — spectrum entries by identity
//! - `asset` — asset records by index
//! - `computors` — computors list by epoch
//! - `contract_ipo` — IPO bids by contract index
//! - `custom_message` — custom messages by tick+index
//! - `meta` — metadata (current tick, epoch, etc.)

use std::path::Path;

use anyhow::{Context, Result};
use rocksdb::{ColumnFamilyDescriptor, DB, Options, WriteBatch};
use tracing::info;

/// Column family names.
pub const CF_TICK: &str = "tick";
pub const CF_TX: &str = "tx";
pub const CF_TX_BY_TICK: &str = "tx_by_tick";
pub const CF_TX_BY_ENTITY: &str = "tx_by_entity";
pub const CF_ENTITY: &str = "entity";
pub const CF_SPECTRUM: &str = "spectrum";
pub const CF_ASSET: &str = "asset";
pub const CF_COMPUTORS: &str = "computors";
pub const CF_CONTRACT_IPO: &str = "contract_ipo";
pub const CF_CUSTOM_MESSAGE: &str = "custom_message";
pub const CF_META: &str = "meta";

const ALL_CFS: &[&str] = &[
    CF_TICK,
    CF_TX,
    CF_TX_BY_TICK,
    CF_TX_BY_ENTITY,
    CF_ENTITY,
    CF_SPECTRUM,
    CF_ASSET,
    CF_COMPUTORS,
    CF_CONTRACT_IPO,
    CF_CUSTOM_MESSAGE,
    CF_META,
];

/// Warm tier storage backed by RocksDB.
pub struct WarmStorage {
    db: DB,
}

impl WarmStorage {
    /// Open the database at the given path, creating column families as needed.
    pub fn open(path: &Path) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Zstd);

        let cf_descriptors: Vec<ColumnFamilyDescriptor> = ALL_CFS
            .iter()
            .map(|name| {
                let mut cf_opts = Options::default();
                cf_opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
                ColumnFamilyDescriptor::new(*name, cf_opts)
            })
            .collect();

        let db = DB::open_cf_descriptors(&opts, path, cf_descriptors)
            .context("Failed to open RocksDB")?;

        info!("Opened warm storage at {}", path.display());
        Ok(Self { db })
    }

    // --- Tick ---

    pub fn put_tick(&self, tick: u32, data: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_TICK).unwrap();
        let key = tick.to_be_bytes();
        self.db.put_cf(cf, key, data)?;
        Ok(())
    }

    pub fn get_tick(&self, tick: u32) -> Result<Option<Vec<u8>>> {
        let cf = self.db.cf_handle(CF_TICK).unwrap();
        let key = tick.to_be_bytes();
        Ok(self.db.get_cf(cf, key)?)
    }

    // --- Transaction ---

    pub fn put_tx(&self, hash: &[u8; 32], data: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_TX).unwrap();
        self.db.put_cf(cf, hash, data)?;
        Ok(())
    }

    pub fn get_tx(&self, hash: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        let cf = self.db.cf_handle(CF_TX).unwrap();
        Ok(self.db.get_cf(cf, hash)?)
    }

    // --- Entity ---

    pub fn put_entity(&self, identity: &[u8; 32], data: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_ENTITY).unwrap();
        self.db.put_cf(cf, identity, data)?;
        Ok(())
    }

    pub fn get_entity(&self, identity: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        let cf = self.db.cf_handle(CF_ENTITY).unwrap();
        Ok(self.db.get_cf(cf, identity)?)
    }

    // --- Spectrum ---

    pub fn put_spectrum_entry(&self, identity: &[u8; 32], data: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_SPECTRUM).unwrap();
        self.db.put_cf(cf, identity, data)?;
        Ok(())
    }

    pub fn get_spectrum_entry(&self, identity: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        let cf = self.db.cf_handle(CF_SPECTRUM).unwrap();
        Ok(self.db.get_cf(cf, identity)?)
    }

    // --- Meta ---

    pub fn put_meta(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_META).unwrap();
        self.db.put_cf(cf, key, value)?;
        Ok(())
    }

    pub fn get_meta(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let cf = self.db.cf_handle(CF_META).unwrap();
        Ok(self.db.get_cf(cf, key)?)
    }

    /// Get the current tick from meta.
    pub fn get_current_tick(&self) -> Result<Option<u32>> {
        match self.get_meta(b"current_tick")? {
            Some(data) if data.len() == 4 => {
                Ok(Some(u32::from_be_bytes([data[0], data[1], data[2], data[3]])))
            }
            _ => Ok(None),
        }
    }

    /// Set the current tick in meta.
    pub fn set_current_tick(&self, tick: u32) -> Result<()> {
        self.put_meta(b"current_tick", &tick.to_be_bytes())
    }

    /// Get the current epoch from meta.
    pub fn get_current_epoch(&self) -> Result<Option<u16>> {
        match self.get_meta(b"current_epoch")? {
            Some(data) if data.len() == 2 => {
                Ok(Some(u16::from_be_bytes([data[0], data[1]])))
            }
            _ => Ok(None),
        }
    }

    /// Set the current epoch in meta.
    pub fn set_current_epoch(&self, epoch: u16) -> Result<()> {
        self.put_meta(b"current_epoch", &epoch.to_be_bytes())
    }

    /// Atomic batch write.
    pub fn batch_write(&self, batch: WriteBatch) -> Result<()> {
        self.db.write(batch)?;
        Ok(())
    }
}
