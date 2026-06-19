//! Hot tier: in-memory cache for frequently accessed data.
//!
//! Caches the current tick, current epoch, and the latest N ticks/entities
//! in memory for fast query responses without hitting the warm tier.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Internal cached entries
// ---------------------------------------------------------------------------

struct CachedTick {
    data: Vec<u8>,
    cached_at: Instant,
}

struct CachedEntity {
    data: Vec<u8>,
    cached_at: Instant,
}

struct CachedComputors {
    epoch: u16,
    data: Vec<u8>,
}

// ---------------------------------------------------------------------------
// HotCache
// ---------------------------------------------------------------------------

/// Hot-tier in-memory cache.
///
/// Stores frequently read data so the query layer can serve requests without
/// hitting RocksDB on every call. All fields are protected by `RwLock` for
/// concurrent read access.
pub struct HotCache {
    current_tick: RwLock<Option<u32>>,
    current_epoch: RwLock<Option<u16>>,
    tick_cache: RwLock<HashMap<u32, CachedTick>>,
    entity_cache: RwLock<HashMap<[u8; 32], CachedEntity>>,
    computors_cache: RwLock<Option<CachedComputors>>,
    max_ticks: usize,
    max_entities: usize,
}

impl HotCache {
    /// Create a new hot cache with the given capacity limits.
    ///
    /// - `max_ticks`: maximum number of tick entries to keep in memory.
    /// - `max_entities`: maximum number of entity entries to keep in memory.
    pub fn new(max_ticks: usize, max_entities: usize) -> Self {
        Self {
            current_tick: RwLock::new(None),
            current_epoch: RwLock::new(None),
            tick_cache: RwLock::new(HashMap::with_capacity(max_ticks)),
            entity_cache: RwLock::new(HashMap::with_capacity(max_entities)),
            computors_cache: RwLock::new(None),
            max_ticks,
            max_entities,
        }
    }

    // ------------------------------------------------------------------
    // Current tick / epoch
    // ------------------------------------------------------------------

    /// Get the cached current tick.
    pub fn get_current_tick(&self) -> Option<u32> {
        *self.current_tick.read().unwrap()
    }

    /// Set the current tick in the cache.
    pub fn set_current_tick(&self, tick: u32) {
        *self.current_tick.write().unwrap() = Some(tick);
    }

    /// Get the cached current epoch.
    pub fn get_current_epoch(&self) -> Option<u16> {
        *self.current_epoch.read().unwrap()
    }

    /// Set the current epoch in the cache.
    pub fn set_current_epoch(&self, epoch: u16) {
        *self.current_epoch.write().unwrap() = Some(epoch);
    }

    // ------------------------------------------------------------------
    // Tick cache
    // ------------------------------------------------------------------

    /// Try to get a tick from the cache. Returns `None` if not cached.
    pub fn get_tick(&self, tick: u32) -> Option<Vec<u8>> {
        let cache = self.tick_cache.read().unwrap();
        cache.get(&tick).map(|entry| entry.data.clone())
    }

    /// Insert a tick into the cache. If the cache is full, evicts the oldest entry.
    pub fn put_tick(&self, tick: u32, data: Vec<u8>) {
        let mut cache = self.tick_cache.write().unwrap();

        // Evict oldest if at capacity and this is a new key.
        if cache.len() >= self.max_ticks && !cache.contains_key(&tick) {
            if let Some(oldest_key) = cache
                .iter()
                .min_by_key(|(_, v)| v.cached_at)
                .map(|(k, _)| *k)
            {
                cache.remove(&oldest_key);
            }
        }

        cache.insert(
            tick,
            CachedTick {
                data,
                cached_at: Instant::now(),
            },
        );
    }

    // ------------------------------------------------------------------
    // Entity cache
    // ------------------------------------------------------------------

    /// Try to get an entity from the cache.
    pub fn get_entity(&self, identity: &[u8; 32]) -> Option<Vec<u8>> {
        let cache = self.entity_cache.read().unwrap();
        cache.get(identity).map(|entry| entry.data.clone())
    }

    /// Insert an entity into the cache.
    pub fn put_entity(&self, identity: [u8; 32], data: Vec<u8>) {
        let mut cache = self.entity_cache.write().unwrap();

        if cache.len() >= self.max_entities && !cache.contains_key(&identity) {
            if let Some(oldest_key) = cache
                .iter()
                .min_by_key(|(_, v)| v.cached_at)
                .map(|(k, _)| *k)
            {
                cache.remove(&oldest_key);
            }
        }

        cache.insert(
            identity,
            CachedEntity {
                data,
                cached_at: Instant::now(),
            },
        );
    }

    // ------------------------------------------------------------------
    // Computors cache
    // ------------------------------------------------------------------

    /// Get the cached computors (epoch, data).
    pub fn get_computors(&self) -> Option<(u16, Vec<u8>)> {
        let cache = self.computors_cache.read().unwrap();
        cache
            .as_ref()
            .map(|entry| (entry.epoch, entry.data.clone()))
    }

    /// Cache a computors list for the given epoch.
    pub fn put_computors(&self, epoch: u16, data: Vec<u8>) {
        let mut cache = self.computors_cache.write().unwrap();
        *cache = Some(CachedComputors { epoch, data });
    }

    // ------------------------------------------------------------------
    // Eviction
    // ------------------------------------------------------------------

    /// Evict all cached entries older than `max_age`.
    pub fn evict_stale(&self, max_age: Duration) {
        let now = Instant::now();

        {
            let mut ticks = self.tick_cache.write().unwrap();
            ticks.retain(|_, entry| now.duration_since(entry.cached_at) < max_age);
        }

        {
            let mut entities = self.entity_cache.write().unwrap();
            entities.retain(|_, entry| now.duration_since(entry.cached_at) < max_age);
        }
    }
}
