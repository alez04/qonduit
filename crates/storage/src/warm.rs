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
use std::sync::Arc;

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
pub const CF_ENTITY_ASSET: &str = "entity_asset";
pub const CF_LOG_EVENT: &str = "log_event";
pub const CF_TICK_VOTE: &str = "tick_vote";
pub const CF_META: &str = "meta";
pub const CF_ORACLE: &str = "oracle";

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
    CF_ENTITY_ASSET,
    CF_LOG_EVENT,
    CF_TICK_VOTE,
    CF_META,
    CF_ORACLE,
];

// ---------------------------------------------------------------------------
// WarmStorage
// ---------------------------------------------------------------------------

/// Warm tier storage backed by RocksDB.
#[derive(Clone)]
pub struct WarmStorage {
    db: Arc<DB>,
}

impl WarmStorage {
    /// Open the database at the given path, creating column families as needed.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_resources(path, None)
    }

    /// Open with explicit resource parameters for optimal performance.
    pub fn open_with_resources(
        path: &Path,
        resources: Option<&qonduit_core::system::SystemResources>,
    ) -> Result<Self> {
        let write_buffer_size = resources
            .map(|r| r.rocksdb_write_buffer_size)
            .unwrap_or(64 * 1024 * 1024);
        let max_write_buffers = resources
            .map(|r| r.rocksdb_max_write_buffers)
            .unwrap_or(4);

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
        opts.set_write_buffer_size(write_buffer_size);
        opts.set_max_write_buffer_number(max_write_buffers);
        opts.set_target_file_size_base(write_buffer_size as u64);
        opts.set_max_open_files(2048);
        // Level compaction tuning for write-heavy workloads
        opts.set_level_compaction_dynamic_level_bytes(true);
        opts.set_bytes_per_sync(16 * 1024 * 1024); // 16MB sync interval
        opts.set_compaction_readahead_size(2 * 1024 * 1024); // 2MB compaction readahead

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

        info!(
            path = %path.display(),
            write_buffer_mb = write_buffer_size / (1024 * 1024),
            max_write_buffers = max_write_buffers,
            "Opened warm storage (auto-tuned for performance)"
        );
        Ok(Self { db: Arc::new(db) })
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

    /// Get all contract IPOs up to a limit.
    pub fn get_all_contract_ipos(&self, limit: usize) -> Result<Vec<(u32, Vec<u8>)>> {
        let cf = self.db.cf_handle(CF_CONTRACT_IPO).unwrap();
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

    /// Get the number of custom messages for a given tick.
    pub fn count_custom_messages_for_tick(&self, tick: u32) -> Result<usize> {
        let cf = self.db.cf_handle(CF_CUSTOM_MESSAGE).unwrap();
        let prefix = tick.to_be_bytes();
        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward));

        let mut count = 0;
        for item in iter {
            let (key, _value) = item?;
            if key.len() != 8 || key[..4] != prefix {
                break;
            }
            count += 1;
        }
        Ok(count)
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

    /// Scan all entity identity keys up to a limit.
    pub fn get_all_entity_keys(&self, limit: usize) -> Result<Vec<[u8; 32]>> {
        let cf = self.db.cf_handle(CF_ENTITY).unwrap();
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);
        let mut keys = Vec::new();
        for item in iter {
            let (key, _value) = item?;
            if key.len() != 32 {
                continue;
            }
            if keys.len() >= limit {
                break;
            }
            let mut identity = [0u8; 32];
            identity.copy_from_slice(&key);
            keys.push(identity);
        }
        Ok(keys)
    }

    // ------------------------------------------------------------------
    // Entity → Asset index operations
    // ------------------------------------------------------------------

    /// Associate an entity with an asset index.
    ///
    /// Key in `CF_ENTITY_ASSET`: `entity(32 bytes) || asset_index(4 bytes BE)` (36 bytes).
    /// Value: empty (existence-based index).
    pub fn put_entity_asset(&self, entity: &[u8; 32], asset_index: u32) -> Result<()> {
        let cf = self.db.cf_handle(CF_ENTITY_ASSET).unwrap();
        let mut key = [0u8; 36];
        key[..32].copy_from_slice(entity);
        key[32..36].copy_from_slice(&asset_index.to_be_bytes());
        self.db.put_cf(cf, key, [])?;
        Ok(())
    }

    /// Get all asset indices associated with an entity.
    pub fn get_assets_for_entity(&self, entity: &[u8; 32]) -> Result<Vec<u32>> {
        let cf = self.db.cf_handle(CF_ENTITY_ASSET).unwrap();
        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(entity, Direction::Forward));

        let mut assets = Vec::new();
        for item in iter {
            let (key, _value) = item?;
            if key.len() != 36 || key[..32] != *entity {
                break;
            }
            let asset_index = u32::from_be_bytes([key[32], key[33], key[34], key[35]]);
            assets.push(asset_index);
        }
        Ok(assets)
    }

    /// Get all entities that hold a given asset (reverse of get_assets_for_entity).
    ///
    /// Scans the `entity_asset` column family for entries where the last 4 bytes
    /// (the asset index) match `asset_index`.
    pub fn get_holders_for_asset(&self, asset_index: u32) -> Result<Vec<[u8; 32]>> {
        let cf = self.db.cf_handle(CF_ENTITY_ASSET).unwrap();
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);

        let mut holders = Vec::new();
        let idx_bytes = asset_index.to_be_bytes();
        for item in iter {
            let (key, _value) = item?;
            if key.len() != 36 {
                continue;
            }
            if key[32..36] == idx_bytes {
                let mut entity = [0u8; 32];
                entity.copy_from_slice(&key[..32]);
                holders.push(entity);
            }
        }
        Ok(holders)
    }

    // ------------------------------------------------------------------
    // Log event operations
    // ------------------------------------------------------------------

    /// Store a log event.
    ///
    /// Key in `CF_LOG_EVENT`: `tick(4 bytes BE) || tx_index(4 bytes BE) || event_type(1 byte)` (9 bytes).
    /// Value: JSON-encoded event payload.
    pub fn put_log_event(
        &self,
        tick: u32,
        tx_index: u32,
        event_type: u8,
        data: &[u8],
    ) -> Result<()> {
        let cf = self.db.cf_handle(CF_LOG_EVENT).unwrap();
        let mut key = [0u8; 9];
        key[..4].copy_from_slice(&tick.to_be_bytes());
        key[4..8].copy_from_slice(&tx_index.to_be_bytes());
        key[8] = event_type;
        self.db.put_cf(cf, key, data)?;
        Ok(())
    }

    /// Get all log events for a given tick.
    pub fn get_log_events_for_tick(&self, tick: u32) -> Result<Vec<Vec<u8>>> {
        let cf = self.db.cf_handle(CF_LOG_EVENT).unwrap();
        let prefix = tick.to_be_bytes();
        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward));

        let mut events = Vec::new();
        for item in iter {
            let (key, value) = item?;
            if key.len() != 9 || key[..4] != prefix {
                break;
            }
            events.push(value.to_vec());
        }
        Ok(events)
    }

    // ------------------------------------------------------------------
    // Tick vote operations
    // ------------------------------------------------------------------

    /// Store a tick vote.
    ///
    /// Key in `CF_TICK_VOTE`: `tick(4 bytes BE) || computor_index(2 bytes BE)` (6 bytes).
    /// Value: JSON-encoded vote data.
    pub fn put_tick_vote(&self, tick: u32, computor_index: u16, data: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_TICK_VOTE).unwrap();
        let mut key = [0u8; 6];
        key[..4].copy_from_slice(&tick.to_be_bytes());
        key[4..6].copy_from_slice(&computor_index.to_be_bytes());
        self.db.put_cf(cf, key, data)?;
        Ok(())
    }

    /// Get all votes for a given tick, returning (computor_index, vote_data) pairs.
    pub fn get_votes_for_tick(&self, tick: u32) -> Result<Vec<(u16, Vec<u8>)>> {
        let cf = self.db.cf_handle(CF_TICK_VOTE).unwrap();
        let prefix = tick.to_be_bytes();
        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward));

        let mut votes = Vec::new();
        for item in iter {
            let (key, value) = item?;
            if key.len() != 6 || key[..4] != prefix {
                break;
            }
            let computor_index = u16::from_be_bytes([key[4], key[5]]);
            votes.push((computor_index, value.to_vec()));
        }
        Ok(votes)
    }

    /// Get the number of votes for a given tick.
    pub fn count_votes_for_tick(&self, tick: u32) -> Result<usize> {
        let cf = self.db.cf_handle(CF_TICK_VOTE).unwrap();
        let prefix = tick.to_be_bytes();
        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward));

        let mut count = 0;
        for item in iter {
            let (key, _value) = item?;
            if key.len() != 6 || key[..4] != prefix {
                break;
            }
            count += 1;
        }
        Ok(count)
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
    // Oracle operations
    // ------------------------------------------------------------------

    /// Store oracle data, keyed by tick.
    pub fn put_oracle(&self, tick: u32, data: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_ORACLE).unwrap();
        self.db.put_cf(cf, tick.to_be_bytes(), data)?;
        Ok(())
    }

    /// Retrieve oracle data for a specific tick.
    pub fn get_oracle(&self, tick: u32) -> Result<Option<Vec<u8>>> {
        let cf = self.db.cf_handle(CF_ORACLE).unwrap();
        Ok(self.db.get_cf(cf, tick.to_be_bytes())?)
    }

    /// Get the latest oracle entry (highest tick) by reverse-scanning.
    pub fn get_latest_oracle(&self) -> Result<Option<(u32, Vec<u8>)>> {
        let cf = self.db.cf_handle(CF_ORACLE).unwrap();
        let iter = self.db.iterator_cf(cf, IteratorMode::End);
        for item in iter {
            let (key, value) = item?;
            if key.len() == 4 {
                let tick = u32::from_be_bytes([key[0], key[1], key[2], key[3]]);
                return Ok(Some((tick, value.to_vec())));
            }
        }
        Ok(None)
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

    // ------------------------------------------------------------------
    // Batch-optimized write helpers (used by the indexer for throughput)
    // ------------------------------------------------------------------

    /// Atomically write a transaction: store payload + tick index + entity indexes.
    ///
    /// Combines what were previously 4 separate `put_cf` calls into a single
    /// `WriteBatch`, dramatically reducing write amplification and WAL overhead.
    pub fn batch_put_tx(
        &self,
        batch: &mut WriteBatch,
        hash: &[u8; 32],
        data: &[u8],
        tick: u32,
        tx_index: u32,
        source: Option<&[u8; 32]>,
        destination: Option<&[u8; 32]>,
    ) {
        let cf_tx = self.db.cf_handle(CF_TX).unwrap();
        batch.put_cf(cf_tx, hash, data);

        let cf_tick = self.db.cf_handle(CF_TX_BY_TICK).unwrap();
        let mut tick_key = [0u8; 8];
        tick_key[..4].copy_from_slice(&tick.to_be_bytes());
        tick_key[4..].copy_from_slice(&tx_index.to_be_bytes());
        batch.put_cf(cf_tick, tick_key, hash);

        if let Some(src) = source {
            let cf_entity = self.db.cf_handle(CF_TX_BY_ENTITY).unwrap();
            let mut key = [0u8; 40];
            key[..32].copy_from_slice(src);
            key[32..36].copy_from_slice(&tick.to_be_bytes());
            key[36..40].copy_from_slice(&tx_index.to_be_bytes());
            batch.put_cf(cf_entity, key, hash);
        }

        if let Some(dst) = destination {
            let cf_entity = self.db.cf_handle(CF_TX_BY_ENTITY).unwrap();
            let mut key = [0u8; 40];
            key[..32].copy_from_slice(dst);
            key[32..36].copy_from_slice(&tick.to_be_bytes());
            key[36..40].copy_from_slice(&tx_index.to_be_bytes());
            batch.put_cf(cf_entity, key, hash);
        }
    }

    /// Atomically write a tick: store payload + update meta current_tick + epoch.
    pub fn batch_put_tick(
        &self,
        batch: &mut WriteBatch,
        tick: u32,
        epoch: u16,
        data: &[u8],
    ) {
        let cf_tick = self.db.cf_handle(CF_TICK).unwrap();
        batch.put_cf(cf_tick, tick.to_be_bytes(), data);

        let cf_meta = self.db.cf_handle(CF_META).unwrap();
        batch.put_cf(cf_meta, b"current_tick", tick.to_be_bytes());
        batch.put_cf(cf_meta, b"current_epoch", epoch.to_be_bytes());
    }
}
