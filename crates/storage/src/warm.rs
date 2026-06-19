//! Warm tier: RocksDB-backed index storage.
//!
//! Column families:
//! - `tick` -- tick data by tick number (key: u32 BE)
//! - `tx` -- transactions by hash (key: 32-byte hash)
//! - `tx_by_tick` -- transaction hashes indexed by tick (key: tick BE || tx_index BE)
//! - `tx_by_entity` -- transaction hashes indexed by entity (key: entity || tick BE || tx_index BE)
//! - `entity` -- entity data by identity (key: 32-byte identity)
//! - `spectrum` -- spectrum entries by identity (key: 32-byte identity)
//! - `asset` -- asset records by index (key: u32 BE)
//! - `computors` -- computors list by epoch (key: u16 BE)
//! - `contract_ipo` -- IPO bids by contract index (key: u32 BE)
//! - `custom_message` -- custom messages by tick+index (key: tick BE || index BE)
//! - `meta` -- metadata (current tick, epoch, etc.)

use std::path::Path;

use anyhow::{Context, Result};
use rocksdb::{ColumnFamilyDescriptor, DB, Direction, IteratorMode, Options, WriteBatch};
use tracing::info;

// ---------------------------------------------------------------------------
// Column family names
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// WarmStorage
// ---------------------------------------------------------------------------

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
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64 MB
        opts.set_max_write_buffer_number(4);
        opts.set_target_file_size_base(64 * 1024 * 1024);
        opts.set_max_open_files(1024);

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

        info!(path = %path.display(), "Opened warm storage");
        Ok(Self { db })
    }

    // ------------------------------------------------------------------
    // Tick operations
    // ------------------------------------------------------------------

    /// Store tick data.
    pub fn put_tick(&self, tick: u32, data: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_TICK).unwrap();
        self.db.put_cf(cf, tick.to_be_bytes(), data)?;
        Ok(())
    }

    /// Retrieve tick data by tick number.
    pub fn get_tick(&self, tick: u32) -> Result<Option<Vec<u8>>> {
        let cf = self.db.cf_handle(CF_TICK).unwrap();
        Ok(self.db.get_cf(cf, tick.to_be_bytes())?)
    }

    /// Retrieve a range of ticks `[from, to]` inclusive.
    pub fn get_tick_range(&self, from: u32, to: u32) -> Result<Vec<(u32, Vec<u8>)>> {
        let cf = self.db.cf_handle(CF_TICK).unwrap();
        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(&from.to_be_bytes(), Direction::Forward));

        let mut results = Vec::new();
        for item in iter {
            let (key, value) = item?;
            if key.len() != 4 {
                continue;
            }
            let tick = u32::from_be_bytes([key[0], key[1], key[2], key[3]]);
            if tick > to {
                break;
            }
            results.push((tick, value.to_vec()));
        }
        Ok(results)
    }

    /// Get the latest tick stored in the database by reverse-scanning CF_TICK.
    pub fn get_latest_tick(&self) -> Result<Option<u32>> {
        let cf = self.db.cf_handle(CF_TICK).unwrap();
        let iter = self.db.iterator_cf(cf, IteratorMode::End);
        for item in iter {
            let (key, _value) = item?;
            if key.len() == 4 {
                return Ok(Some(u32::from_be_bytes([
                    key[0], key[1], key[2], key[3],
                ])));
            }
        }
        Ok(None)
    }

    // ------------------------------------------------------------------
    // Transaction operations
    // ------------------------------------------------------------------

    /// Store transaction data keyed by its 32-byte hash.
    pub fn put_tx(&self, hash: &[u8; 32], data: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_TX).unwrap();
        self.db.put_cf(cf, hash, data)?;
        Ok(())
    }

    /// Retrieve a transaction by its hash.
    pub fn get_tx(&self, hash: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        let cf = self.db.cf_handle(CF_TX).unwrap();
        Ok(self.db.get_cf(cf, hash)?)
    }

    /// Index a transaction hash under a tick number.
    ///
    /// Key in `CF_TX_BY_TICK`: `tick(4 bytes BE) || tx_index(4 bytes BE)` (8 bytes total).
    /// Value: 32-byte transaction hash.
    pub fn put_tx_for_tick(&self, tick: u32, tx_index: u32, tx_hash: &[u8; 32]) -> Result<()> {
        let cf = self.db.cf_handle(CF_TX_BY_TICK).unwrap();
        let mut key = [0u8; 8];
        key[..4].copy_from_slice(&tick.to_be_bytes());
        key[4..].copy_from_slice(&tx_index.to_be_bytes());
        self.db.put_cf(cf, key, tx_hash)?;
        Ok(())
    }

    /// Get all transaction hashes for a given tick.
    pub fn get_tx_hashes_for_tick(&self, tick: u32) -> Result<Vec<[u8; 32]>> {
        let cf = self.db.cf_handle(CF_TX_BY_TICK).unwrap();
        let prefix = tick.to_be_bytes();
        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward));

        let mut hashes = Vec::new();
        for item in iter {
            let (key, value) = item?;
            // Keys are 8 bytes: tick(4) || tx_index(4)
            if key.len() != 8 || key[..4] != prefix {
                break;
            }
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&value);
            hashes.push(hash);
        }
        Ok(hashes)
    }

    /// Index a transaction hash under an entity identity.
    ///
    /// Key in `CF_TX_BY_ENTITY`: `entity(32 bytes) || tick(4 bytes BE) || tx_index(4 bytes BE)` (40 bytes).
    /// Value: 32-byte transaction hash.
    pub fn put_tx_for_entity(
        &self,
        entity: &[u8; 32],
        tick: u32,
        tx_index: u32,
        tx_hash: &[u8; 32],
    ) -> Result<()> {
        let cf = self.db.cf_handle(CF_TX_BY_ENTITY).unwrap();
        let mut key = [0u8; 40];
        key[..32].copy_from_slice(entity);
        key[32..36].copy_from_slice(&tick.to_be_bytes());
        key[36..40].copy_from_slice(&tx_index.to_be_bytes());
        self.db.put_cf(cf, key, tx_hash)?;
        Ok(())
    }

    /// Get recent transaction hashes for an entity, scanning in reverse (most recent first).
    pub fn get_tx_hashes_for_entity(
        &self,
        entity: &[u8; 32],
        limit: usize,
    ) -> Result<Vec<[u8; 32]>> {
        let cf = self.db.cf_handle(CF_TX_BY_ENTITY).unwrap();

        // Build an upper-bound key: entity || 0xFF * 8 to start reverse iteration
        // from the end of this entity's key space.
        let mut upper = [0xFFu8; 40];
        upper[..32].copy_from_slice(entity);

        let iter = self.db.iterator_cf(
            cf,
            IteratorMode::From(&upper, Direction::Reverse),
        );

        let mut hashes = Vec::new();
        for item in iter {
            let (key, value) = item?;
            if key.len() != 40 || key[..32] != *entity {
                break;
            }
            if hashes.len() >= limit {
                break;
            }
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&value);
            hashes.push(hash);
        }
        Ok(hashes)
    }

    // ------------------------------------------------------------------
    // Entity operations
    // ------------------------------------------------------------------

    /// Store entity data.
    pub fn put_entity(&self, identity: &[u8; 32], data: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_ENTITY).unwrap();
        self.db.put_cf(cf, identity, data)?;
        Ok(())
    }

    /// Retrieve entity data.
    pub fn get_entity(&self, identity: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        let cf = self.db.cf_handle(CF_ENTITY).unwrap();
        Ok(self.db.get_cf(cf, identity)?)
    }

    // ------------------------------------------------------------------
    // Spectrum operations
    // ------------------------------------------------------------------

    /// Store a spectrum entry.
    pub fn put_spectrum_entry(&self, identity: &[u8; 32], data: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_SPECTRUM).unwrap();
        self.db.put_cf(cf, identity, data)?;
        Ok(())
    }

    /// Retrieve a spectrum entry.
    pub fn get_spectrum_entry(&self, identity: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        let cf = self.db.cf_handle(CF_SPECTRUM).unwrap();
        Ok(self.db.get_cf(cf, identity)?)
    }

    /// Get a range of spectrum entries starting from `start` identity, returning up to `count`.
    pub fn get_spectrum_range(
        &self,
        start: &[u8; 32],
        count: usize,
    ) -> Result<Vec<([u8; 32], Vec<u8>)>> {
        let cf = self.db.cf_handle(CF_SPECTRUM).unwrap();
        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(start, Direction::Forward));

        let mut results = Vec::new();
        for item in iter {
            let (key, value) = item?;
            if key.len() != 32 {
                continue;
            }
            if results.len() >= count {
                break;
            }
            let mut identity = [0u8; 32];
            identity.copy_from_slice(&key);
            results.push((identity, value.to_vec()));
        }
        Ok(results)
    }

    // ------------------------------------------------------------------
    // Asset operations
    // ------------------------------------------------------------------

    /// Store an asset record.
    pub fn put_asset(&self, index: u32, data: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_ASSET).unwrap();
        self.db.put_cf(cf, index.to_be_bytes(), data)?;
        Ok(())
    }

    /// Retrieve an asset record.
    pub fn get_asset(&self, index: u32) -> Result<Option<Vec<u8>>> {
        let cf = self.db.cf_handle(CF_ASSET).unwrap();
        Ok(self.db.get_cf(cf, index.to_be_bytes())?)
    }

    /// Get all assets up to a limit.
    pub fn get_all_assets(&self, limit: usize) -> Result<Vec<(u32, Vec<u8>)>> {
        let cf = self.db.cf_handle(CF_ASSET).unwrap();
        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(&[0u8; 4], Direction::Forward));

        let mut results = Vec::new();
        for item in iter {
            let (key, value) = item?;
            if key.len() != 4 {
                continue;
            }
            if results.len() >= limit {
                break;
            }
            let index = u32::from_be_bytes([key[0], key[1], key[2], key[3]]);
            results.push((index, value.to_vec()));
        }
        Ok(results)
    }

    // ------------------------------------------------------------------
    // Computors operations
    // ------------------------------------------------------------------

    /// Store the computors list for an epoch.
    pub fn put_computors(&self, epoch: u16, data: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_COMPUTORS).unwrap();
        self.db.put_cf(cf, epoch.to_be_bytes(), data)?;
        Ok(())
    }

    /// Retrieve the computors list for an epoch.
    pub fn get_computors(&self, epoch: u16) -> Result<Option<Vec<u8>>> {
        let cf = self.db.cf_handle(CF_COMPUTORS).unwrap();
        Ok(self.db.get_cf(cf, epoch.to_be_bytes())?)
    }

    /// Get the latest computors entry (highest epoch) by reverse-scanning.
    pub fn get_latest_computors(&self) -> Result<Option<(u16, Vec<u8>)>> {
        let cf = self.db.cf_handle(CF_COMPUTORS).unwrap();
        let iter = self.db.iterator_cf(cf, IteratorMode::End);
        for item in iter {
            let (key, value) = item?;
            if key.len() == 2 {
                let epoch = u16::from_be_bytes([key[0], key[1]]);
                return Ok(Some((epoch, value.to_vec())));
            }
        }
        Ok(None)
    }

    // ------------------------------------------------------------------
    // Contract IPO operations
    // ------------------------------------------------------------------

    /// Store contract IPO data.
    pub fn put_contract_ipo(&self, contract_index: u32, data: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_CONTRACT_IPO).unwrap();
        self.db.put_cf(cf, contract_index.to_be_bytes(), data)?;
        Ok(())
    }

    /// Retrieve contract IPO data.
    pub fn get_contract_ipo(&self, contract_index: u32) -> Result<Option<Vec<u8>>> {
        let cf = self.db.cf_handle(CF_CONTRACT_IPO).unwrap();
        Ok(self.db.get_cf(cf, contract_index.to_be_bytes())?)
    }

    // ------------------------------------------------------------------
    // Custom message operations
    // ------------------------------------------------------------------

    /// Store a custom message.
    ///
    /// Key: `tick(4 bytes BE) || index(4 bytes BE)` (8 bytes).
    pub fn put_custom_message(&self, tick: u32, index: u32, data: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_CUSTOM_MESSAGE).unwrap();
        let mut key = [0u8; 8];
        key[..4].copy_from_slice(&tick.to_be_bytes());
        key[4..].copy_from_slice(&index.to_be_bytes());
        self.db.put_cf(cf, key, data)?;
        Ok(())
    }

    /// Get all custom messages for a given tick.
    pub fn get_custom_messages_for_tick(&self, tick: u32) -> Result<Vec<Vec<u8>>> {
        let cf = self.db.cf_handle(CF_CUSTOM_MESSAGE).unwrap();
        let prefix = tick.to_be_bytes();
        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward));

        let mut messages = Vec::new();
        for item in iter {
            let (key, value) = item?;
            if key.len() != 8 || key[..4] != prefix {
                break;
            }
            messages.push(value.to_vec());
        }
        Ok(messages)
    }

    // ------------------------------------------------------------------
    // Meta operations
    // ------------------------------------------------------------------

    /// Store a metadata key-value pair.
    pub fn put_meta(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_META).unwrap();
        self.db.put_cf(cf, key, value)?;
        Ok(())
    }

    /// Retrieve a metadata value.
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

    // ------------------------------------------------------------------
    // Batch operations
    // ------------------------------------------------------------------

    /// Execute an atomic batch write.
    pub fn batch_write(&self, batch: WriteBatch) -> Result<()> {
        self.db.write(batch)?;
        Ok(())
    }

    /// Create a new `WriteBatch` for building atomic multi-key writes.
    pub fn create_batch(&self) -> WriteBatch {
        WriteBatch::default()
    }
}
