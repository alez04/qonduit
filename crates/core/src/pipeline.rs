//! Pipeline state: shared, lock-free counters for tracking ingestion and indexing progress.

use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::time::Instant;

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
    /// Seconds since the pipeline started.
    pub uptime_seconds: u64,
}

impl PipelineState {
    /// Create a new, zeroed pipeline state.
    pub fn new() -> Self {
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
            uptime_seconds: self.started_at.elapsed().as_secs(),
        }
    }
}

impl Default for PipelineState {
    fn default() -> Self {
        Self::new()
    }
}
