//! Pipeline state: shared, lock-free counters for tracking ingestion and indexing progress.

use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;

/// Lock-free shared state for the ingestion -> processor -> query pipeline.
///
/// All fields use atomics so they can be updated from any task without
/// contention. The `PipelineStatusResponse` snapshot is cheap to take.
#[derive(Debug)]
pub struct PipelineState {
    /// Whether the ingestion client is currently connected to a Qubic node.
    pub ingestion_connected: AtomicBool,
    /// Whether ingestion is disabled (query-only mode).
    pub ingestion_disabled: AtomicBool,
    /// Latest tick reported by the Qubic node.
    pub node_tick: AtomicU32,
    /// Latest epoch reported by the Qubic node.
    pub node_epoch: AtomicU16,
    /// Latest tick that has been indexed into RocksDB.
    pub indexed_tick: AtomicU32,
    /// Latest epoch that has been indexed into RocksDB.
    pub indexed_epoch: AtomicU16,
    /// Total number of ticks indexed since startup.
    pub ticks_indexed: AtomicU64,
    /// Total number of transactions indexed since startup.
    pub txs_indexed: AtomicU64,
    /// Total number of entities indexed since startup.
    pub entities_indexed: AtomicU64,

    // ------------------------------------------------------------------
    // Consumer lag metrics (estimated unprocessed messages per stream)
    // ------------------------------------------------------------------
    /// Estimated number of unprocessed tick messages in the NATS stream.
    pub tick_lag: AtomicU64,
    /// Estimated number of unprocessed transaction messages in the NATS stream.
    pub tx_lag: AtomicU64,
    /// Estimated number of unprocessed entity messages in the NATS stream.
    pub entity_lag: AtomicU64,

    // ------------------------------------------------------------------
    // Indexing rate tracking (for ETA estimation)
    // ------------------------------------------------------------------
    /// Unix timestamp (seconds) when indexing started.
    pub indexing_start_time: AtomicU64,
    /// Total ticks indexed (monotonically increasing, used for rate calculation).
    pub total_ticks_indexed: AtomicU64,

    // ------------------------------------------------------------------
    // Backfill progress tracking
    // ------------------------------------------------------------------
    /// Whether the historical backfill is currently running.
    pub backfill_running: AtomicBool,
    /// Total ticks completed by the backfill process.
    pub backfill_ticks_completed: AtomicU64,
    /// Total transactions discovered by the backfill process.
    pub backfill_txs_discovered: AtomicU64,
    /// Total tick data items discovered by the backfill process.
    pub backfill_ticks_discovered: AtomicU64,
    /// Ticks that failed during backfill.
    pub backfill_ticks_failed: AtomicU32,
    /// Backfill start tick.
    pub backfill_start_tick: AtomicU32,
    /// Backfill end tick.
    pub backfill_end_tick: AtomicU32,

    /// When the pipeline started.
    started_at: Instant,
}

/// Serializable snapshot of pipeline status for the `/system-info` endpoint.
#[derive(Debug, Serialize)]
pub struct PipelineStatusResponse {
    /// Derived pipeline status: "live", "catching_up", "disconnected", or "query_only".
    pub pipeline_status: String,
    /// Whether ingestion is currently connected.
    pub ingestion_connected: bool,
    /// Latest tick from the Qubic node.
    pub node_tick: u32,
    /// Latest epoch from the Qubic node.
    pub node_epoch: u16,
    /// Latest tick indexed into RocksDB.
    pub indexed_tick: u32,
    /// Latest epoch indexed into RocksDB.
    pub indexed_epoch: u16,
    /// Number of ticks behind the node (positive = behind, 0 = caught up).
    pub ticks_behind: i64,
    /// Total ticks indexed since startup.
    pub ticks_indexed: u64,
    /// Total transactions indexed since startup.
    pub txs_indexed: u64,
    /// Total entities indexed since startup.
    pub entities_indexed: u64,
    /// Estimated number of unprocessed tick messages in the NATS stream.
    pub tick_lag: u64,
    /// Estimated number of unprocessed transaction messages in the NATS stream.
    pub tx_lag: u64,
    /// Estimated number of unprocessed entity messages in the NATS stream.
    pub entity_lag: u64,
    /// Seconds since the pipeline started.
    pub uptime_seconds: u64,
    /// Estimated seconds until the processor catches up to the node (0 if caught up or unknown).
    pub estimated_seconds_to_live: u64,
    /// Average indexing rate in ticks per second (computed over uptime).
    pub avg_indexing_rate: f64,
    // ------------------------------------------------------------------
    // Backfill progress
    // ------------------------------------------------------------------
    /// Whether the historical backfill is running.
    pub backfill_running: bool,
    /// Total ticks completed by backfill.
    pub backfill_ticks_completed: u64,
    /// Total transactions discovered by backfill.
    pub backfill_txs_discovered: u64,
    /// Total tick data items discovered by backfill.
    pub backfill_ticks_discovered: u64,
    /// Ticks that failed during backfill.
    pub backfill_ticks_failed: u32,
    /// Backfill start tick.
    pub backfill_start_tick: u32,
    /// Backfill end tick.
    pub backfill_end_tick: u32,
}

impl PipelineState {
    /// Create a new, zeroed pipeline state.
    pub fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            ingestion_connected: AtomicBool::new(false),
            ingestion_disabled: AtomicBool::new(false),
            node_tick: AtomicU32::new(0),
            node_epoch: AtomicU16::new(0),
            indexed_tick: AtomicU32::new(0),
            indexed_epoch: AtomicU16::new(0),
            ticks_indexed: AtomicU64::new(0),
            txs_indexed: AtomicU64::new(0),
            entities_indexed: AtomicU64::new(0),
            tick_lag: AtomicU64::new(0),
            tx_lag: AtomicU64::new(0),
            entity_lag: AtomicU64::new(0),
            indexing_start_time: AtomicU64::new(now),
            total_ticks_indexed: AtomicU64::new(0),
            backfill_running: AtomicBool::new(false),
            backfill_ticks_completed: AtomicU64::new(0),
            backfill_txs_discovered: AtomicU64::new(0),
            backfill_ticks_discovered: AtomicU64::new(0),
            backfill_ticks_failed: AtomicU32::new(0),
            backfill_start_tick: AtomicU32::new(0),
            backfill_end_tick: AtomicU32::new(0),
            started_at: Instant::now(),
        }
    }

    /// Take a snapshot of the current pipeline status.
    pub fn status(&self) -> PipelineStatusResponse {
        let node_tick = self.node_tick.load(Ordering::Relaxed);
        let indexed_tick = self.indexed_tick.load(Ordering::Relaxed);
        let behind = if node_tick > 0 && indexed_tick > 0 {
            node_tick as i64 - indexed_tick as i64
        } else if node_tick > 0 {
            // Node reports a tick but nothing indexed yet.
            node_tick as i64
        } else {
            0
        };

        let connected = self.ingestion_connected.load(Ordering::Relaxed);
        let disabled = self.ingestion_disabled.load(Ordering::Relaxed);

        let pipeline_status = if disabled {
            "query_only".to_string()
        } else if !connected {
            "disconnected".to_string()
        } else if behind > 100 {
            "catching_up".to_string()
        } else {
            "live".to_string()
        };

        // Calculate average indexing rate and estimated time to live
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let start_time = self.indexing_start_time.load(Ordering::Relaxed);
        let total_indexed = self.total_ticks_indexed.load(Ordering::Relaxed);

        let elapsed = now.saturating_sub(start_time);
        let avg_indexing_rate = if elapsed > 0 {
            total_indexed as f64 / elapsed as f64
        } else {
            0.0
        };

        let ticks_behind = behind.max(0) as u64;
        let estimated_seconds_to_live = if avg_indexing_rate > 0.0 && ticks_behind > 0 {
            (ticks_behind as f64 / avg_indexing_rate) as u64
        } else {
            0
        };

        PipelineStatusResponse {
            pipeline_status,
            ingestion_connected: connected,
            node_tick,
            node_epoch: self.node_epoch.load(Ordering::Relaxed),
            indexed_tick,
            indexed_epoch: self.indexed_epoch.load(Ordering::Relaxed),
            ticks_behind: behind,
            ticks_indexed: self.ticks_indexed.load(Ordering::Relaxed),
            txs_indexed: self.txs_indexed.load(Ordering::Relaxed),
            entities_indexed: self.entities_indexed.load(Ordering::Relaxed),
            tick_lag: self.tick_lag.load(Ordering::Relaxed),
            tx_lag: self.tx_lag.load(Ordering::Relaxed),
            entity_lag: self.entity_lag.load(Ordering::Relaxed),
            uptime_seconds: self.started_at.elapsed().as_secs(),
            estimated_seconds_to_live,
            avg_indexing_rate,
            backfill_running: self.backfill_running.load(Ordering::Relaxed),
            backfill_ticks_completed: self.backfill_ticks_completed.load(Ordering::Relaxed),
            backfill_txs_discovered: self.backfill_txs_discovered.load(Ordering::Relaxed),
            backfill_ticks_discovered: self.backfill_ticks_discovered.load(Ordering::Relaxed),
            backfill_ticks_failed: self.backfill_ticks_failed.load(Ordering::Relaxed),
            backfill_start_tick: self.backfill_start_tick.load(Ordering::Relaxed),
            backfill_end_tick: self.backfill_end_tick.load(Ordering::Relaxed),
        }
    }
}

impl Default for PipelineState {
    fn default() -> Self {
        Self::new()
    }
}
