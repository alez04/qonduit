//! Prometheus metrics for Qonduit.
//!
//! All application-level gauges are registered here. The `/metrics` endpoint
//! gathers from both this registry and the ingestion registry, then renders
//! a single combined Prometheus text output.

use std::sync::atomic::Ordering;

use prometheus::{
    Encoder, IntCounter, IntCounterVec, IntGauge, TextEncoder,
    opts,
};
use std::sync::OnceLock;

pub static REGISTRY: OnceLock<prometheus::Registry> = OnceLock::new();

fn registry() -> &'static prometheus::Registry {
    REGISTRY.get_or_init(prometheus::Registry::new)
}

// =========================================================================
// API Counters
// =========================================================================

pub static RPC_REQUESTS: once_cell::sync::Lazy<IntCounter> = once_cell::sync::Lazy::new(|| {
    let c = IntCounter::with_opts(opts!(
        "qonduit_rpc_requests_total",
        "Total JSON-RPC requests"
    )).unwrap();
    registry().register(Box::new(c.clone())).unwrap();
    c
});

pub static REST_REQUESTS: once_cell::sync::Lazy<IntCounter> = once_cell::sync::Lazy::new(|| {
    let c = IntCounter::with_opts(opts!(
        "qonduit_rest_requests_total",
        "Total REST API requests"
    )).unwrap();
    registry().register(Box::new(c.clone())).unwrap();
    c
});

pub static WS_CONNECTIONS: once_cell::sync::Lazy<IntCounter> = once_cell::sync::Lazy::new(|| {
    let c = IntCounter::with_opts(opts!(
        "qonduit_ws_connections_total",
        "Total WebSocket connections opened"
    )).unwrap();
    registry().register(Box::new(c.clone())).unwrap();
    c
});

/// REST requests broken down by route.
pub static REST_REQUESTS_BY_ROUTE: once_cell::sync::Lazy<IntCounterVec> =
    once_cell::sync::Lazy::new(|| {
        let c = IntCounterVec::new(
            opts!("qonduit_rest_requests_by_route", "REST requests by route"),
            &["route"],
        ).unwrap();
        registry().register(Box::new(c.clone())).unwrap();
        c
    });

/// RPC requests broken down by method.
pub static RPC_REQUESTS_BY_METHOD: once_cell::sync::Lazy<IntCounterVec> =
    once_cell::sync::Lazy::new(|| {
        let c = IntCounterVec::new(
            opts!("qonduit_rpc_requests_by_method", "RPC requests by method"),
            &["method"],
        ).unwrap();
        registry().register(Box::new(c.clone())).unwrap();
        c
    });

/// Counter for entity history API requests (both REST and RPC).
pub static ENTITY_HISTORY_REQUESTS: once_cell::sync::Lazy<IntCounter> =
    once_cell::sync::Lazy::new(|| {
        let c = IntCounter::with_opts(opts!(
            "qonduit_entity_history_requests_total",
            "Total entity transaction history requests"
        )).unwrap();
        registry().register(Box::new(c.clone())).unwrap();
        c
    });

// =========================================================================
// Pipeline Gauges (updated from PipelineState on each /metrics scrape)
// =========================================================================

/// Current tick reported by the connected Qubic node.
pub static NODE_TICK: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_node_tick",
        "Latest tick reported by the Qubic node"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Current epoch reported by the connected Qubic node.
pub static NODE_EPOCH: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_node_epoch",
        "Latest epoch reported by the Qubic node"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Latest tick indexed into RocksDB.
pub static INDEXED_TICK: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_indexed_tick",
        "Latest tick indexed into RocksDB"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Latest epoch indexed into RocksDB.
pub static INDEXED_EPOCH: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_indexed_epoch",
        "Latest epoch indexed into RocksDB"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Number of ticks behind the node (positive = behind, 0 = caught up).
pub static TICKS_BEHIND: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_ticks_behind",
        "Ticks behind the Qubic node (0 = caught up)"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Total ticks indexed since startup.
pub static TICKS_INDEXED_TOTAL: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_ticks_indexed_total",
        "Total ticks indexed since startup"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Total transactions indexed since startup.
pub static TXS_INDEXED_TOTAL: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_txs_indexed_total",
        "Total transactions indexed since startup"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Total entities indexed since startup.
pub static ENTITIES_INDEXED_TOTAL: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_entities_indexed_total",
        "Total entities indexed since startup"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Average indexing rate in ticks per second (all-time).
pub static INDEXING_RATE_AVG: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_indexing_rate_avg_ticks_per_sec",
        "Average indexing rate in ticks per second (all-time)"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Current indexing rate in ticks per second (rolling window ~3s).
pub static INDEXING_RATE_CURRENT: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_indexing_rate_current_ticks_per_sec",
        "Current indexing rate in ticks per second (rolling window)"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Pipeline uptime in seconds.
pub static UPTIME_SECONDS: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_uptime_seconds",
        "Pipeline uptime in seconds"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Estimated seconds until processor catches up to the node (0 if caught up).
pub static ETA_TO_LIVE_SECONDS: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_eta_to_live_seconds",
        "Estimated seconds until processor catches up to the node (0 = live)"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

// =========================================================================
// Ingestion Status Gauges
// =========================================================================

/// Whether ingestion is connected to a Qubic node (1=yes, 0=no).
pub static INGESTION_CONNECTED: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_ingestion_connected",
        "Whether ingestion is connected to a Qubic node (1=yes)"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Whether ingestion is disabled (query-only mode).
pub static INGESTION_DISABLED: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_ingestion_disabled",
        "Whether ingestion is disabled (1=yes, query-only mode)"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

// =========================================================================
// NATS Consumer Lag Gauges
// =========================================================================

/// Estimated unprocessed messages in the tick NATS stream.
pub static NATS_TICK_LAG: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_nats_tick_lag",
        "Estimated unprocessed messages in tick NATS stream"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Estimated unprocessed messages in the tx NATS stream.
pub static NATS_TX_LAG: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_nats_tx_lag",
        "Estimated unprocessed messages in tx NATS stream"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Estimated unprocessed messages in the entity NATS stream.
pub static NATS_ENTITY_LAG: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_nats_entity_lag",
        "Estimated unprocessed messages in entity NATS stream"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

// =========================================================================
// Epoch Progress Gauges
// =========================================================================

/// Number of ticks in the current epoch (estimated from initial_tick to current).
pub static EPOCH_TICK_SPAN: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_epoch_tick_span",
        "Estimated number of ticks in the current epoch"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Number of ticks indexed in the current epoch.
pub static EPOCH_TICKS_INDEXED: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_epoch_ticks_indexed",
        "Number of ticks indexed in the current epoch"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Current epoch indexing progress as a percentage (0-100).
pub static EPOCH_PROGRESS_PCT: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_epoch_progress_pct",
        "Current epoch indexing progress percentage (0-100)"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Total number of epochs that have been fully indexed.
pub static EPOCHS_FULLY_INDEXED: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_epochs_fully_indexed",
        "Number of epochs that have been fully indexed"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

// =========================================================================
// Backfill Gauges
// =========================================================================

/// Whether the historical backfill is running (1=yes).
pub static BACKFILL_RUNNING: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_backfill_running",
        "Whether historical backfill is running (1=yes)"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Total ticks processed by backfill.
pub static BACKFILL_TICKS_COMPLETED: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_backfill_ticks_completed_total",
        "Total ticks processed by backfill"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Total transactions discovered by backfill.
pub static BACKFILL_TXS_DISCOVERED: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_backfill_txs_discovered_total",
        "Total transactions discovered by backfill"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Total tick data items discovered by backfill.
pub static BACKFILL_TICKS_DISCOVERED: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_backfill_ticks_discovered_total",
        "Total tick data items discovered by backfill"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Ticks that failed during backfill.
pub static BACKFILL_TICKS_FAILED: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_backfill_ticks_failed_total",
        "Ticks that failed during backfill"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Backfill start tick.
pub static BACKFILL_START_TICK: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_backfill_start_tick",
        "Backfill start tick"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Backfill end tick.
pub static BACKFILL_END_TICK: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_backfill_end_tick",
        "Backfill end tick"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

/// Backfill progress percentage (0-100).
pub static BACKFILL_PROGRESS_PCT: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let g = IntGauge::with_opts(opts!(
        "qonduit_backfill_progress_pct",
        "Backfill progress percentage (0-100)"
    )).unwrap();
    registry().register(Box::new(g.clone())).unwrap();
    g
});

// =========================================================================
// Helpers
// =========================================================================

/// Update all pipeline-state gauges from the current `PipelineState` snapshot.
///
/// Call this before rendering metrics so gauges reflect live data.
pub fn update_pipeline_gauges(pipeline: &qonduit_core::PipelineState) {
    let node_tick = pipeline.node_tick.load(Ordering::Relaxed);
    let indexed_tick = pipeline.indexed_tick.load(Ordering::Relaxed);
    let node_epoch = pipeline.node_epoch.load(Ordering::Relaxed);
    let indexed_epoch = pipeline.indexed_epoch.load(Ordering::Relaxed);

    NODE_TICK.set(node_tick as i64);
    NODE_EPOCH.set(node_epoch as i64);
    INDEXED_TICK.set(indexed_tick as i64);
    INDEXED_EPOCH.set(indexed_epoch as i64);

    let behind = if node_tick > 0 && indexed_tick > 0 {
        node_tick as i64 - indexed_tick as i64
    } else if node_tick > 0 {
        node_tick as i64
    } else {
        0
    };
    TICKS_BEHIND.set(behind.max(0));

    TICKS_INDEXED_TOTAL.set(pipeline.ticks_indexed.load(Ordering::Relaxed) as i64);
    TXS_INDEXED_TOTAL.set(pipeline.txs_indexed.load(Ordering::Relaxed) as i64);
    ENTITIES_INDEXED_TOTAL.set(pipeline.entities_indexed.load(Ordering::Relaxed) as i64);

    // Ingestion status
    INGESTION_CONNECTED.set(
        pipeline.ingestion_connected.load(Ordering::Relaxed) as i64,
    );
    INGESTION_DISABLED.set(
        pipeline.ingestion_disabled.load(Ordering::Relaxed) as i64,
    );

    // NATS lag
    NATS_TICK_LAG.set(pipeline.tick_lag.load(Ordering::Relaxed) as i64);
    NATS_TX_LAG.set(pipeline.tx_lag.load(Ordering::Relaxed) as i64);
    NATS_ENTITY_LAG.set(pipeline.entity_lag.load(Ordering::Relaxed) as i64);

    // Uptime and rate
    let status = pipeline.status();
    UPTIME_SECONDS.set(status.uptime_seconds as i64);
    INDEXING_RATE_AVG.set((status.avg_indexing_rate * 1000.0) as i64); // stored as millis
    INDEXING_RATE_CURRENT.set((status.current_indexing_rate * 1000.0) as i64); // stored as millis
    ETA_TO_LIVE_SECONDS.set(status.estimated_seconds_to_live as i64);

    // Epoch progress (precise, using RPC epoch interval data)
    let epoch_progress = if let Some(pct) = qonduit_core::epoch_intervals::epoch_progress_pct(
        node_epoch,
        indexed_tick,
    ) {
        pct as i64
    } else {
        // Fallback: rough estimate if epoch data not available
        if behind > 0 {
            0i64
        } else if node_tick > 0 {
            let epoch_size_guess = 218_000u32;
            let position_in_epoch = node_tick % epoch_size_guess;
            ((position_in_epoch as f64 / epoch_size_guess as f64) * 100.0) as i64
        } else {
            0
        }
    };
    EPOCH_PROGRESS_PCT.set(epoch_progress);
    EPOCHS_FULLY_INDEXED.set(qonduit_core::epoch_intervals::epochs_fully_indexed(indexed_tick) as i64);

    // Backfill
    BACKFILL_RUNNING.set(
        pipeline.backfill_running.load(Ordering::Relaxed) as i64,
    );
    BACKFILL_TICKS_COMPLETED.set(
        pipeline.backfill_ticks_completed.load(Ordering::Relaxed) as i64,
    );
    BACKFILL_TXS_DISCOVERED.set(
        pipeline.backfill_txs_discovered.load(Ordering::Relaxed) as i64,
    );
    BACKFILL_TICKS_DISCOVERED.set(
        pipeline.backfill_ticks_discovered.load(Ordering::Relaxed) as i64,
    );
    BACKFILL_TICKS_FAILED.set(
        pipeline.backfill_ticks_failed.load(Ordering::Relaxed) as i64,
    );
    let bf_start = pipeline.backfill_start_tick.load(Ordering::Relaxed);
    let bf_end = pipeline.backfill_end_tick.load(Ordering::Relaxed);
    BACKFILL_START_TICK.set(bf_start as i64);
    BACKFILL_END_TICK.set(bf_end as i64);

    let bf_completed = pipeline.backfill_ticks_completed.load(Ordering::Relaxed);
    let bf_total = bf_end.saturating_sub(bf_start) as i64;
    let bf_pct = if bf_total > 0 {
        ((bf_completed as f64 / bf_total as f64) * 100.0) as i64
    } else {
        0
    };
    BACKFILL_PROGRESS_PCT.set(bf_pct);
}

/// Render all metrics as Prometheus text format.
///
/// Gathers from both the query registry and the ingestion registry,
/// producing a single combined output.
pub fn render_metrics() -> String {
    let encoder = TextEncoder::new();

    // Gather from query registry
    let mut families = registry().gather();

    // Gather from ingestion registry and append
    if let Some(ingestion_registry) = qonduit_ingestion::metrics::REGISTRY.get() {
        families.extend(ingestion_registry.gather());
    }

    let mut buffer = Vec::new();
    encoder.encode(&families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}
