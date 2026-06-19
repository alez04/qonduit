//! Prometheus metrics for Qonduit.

use prometheus::{
    Encoder, IntCounter, IntGauge, Registry, TextEncoder,
    opts,
};
use std::sync::OnceLock;

pub static REGISTRY: OnceLock<Registry> = OnceLock::new();

fn registry() -> &'static Registry {
    REGISTRY.get_or_init(Registry::new)
}

// --- Counters ---
pub static TICKS_RECEIVED: once_cell::sync::Lazy<IntCounter> = once_cell::sync::Lazy::new(|| {
    let counter = IntCounter::with_opts(
        opts!("qonduit_ticks_received_total", "Total ticks received from the node")
    ).unwrap();
    registry().register(Box::new(counter.clone())).unwrap();
    counter
});

pub static TRANSACTIONS_RECEIVED: once_cell::sync::Lazy<IntCounter> = once_cell::sync::Lazy::new(|| {
    let counter = IntCounter::with_opts(
        opts!("qonduit_transactions_received_total", "Total transactions received")
    ).unwrap();
    registry().register(Box::new(counter.clone())).unwrap();
    counter
});

pub static ENTITIES_RECEIVED: once_cell::sync::Lazy<IntCounter> = once_cell::sync::Lazy::new(|| {
    let counter = IntCounter::with_opts(
        opts!("qonduit_entities_received_total", "Total entities received")
    ).unwrap();
    registry().register(Box::new(counter.clone())).unwrap();
    counter
});

pub static RPC_REQUESTS: once_cell::sync::Lazy<IntCounter> = once_cell::sync::Lazy::new(|| {
    let counter = IntCounter::with_opts(
        opts!("qonduit_rpc_requests_total", "Total JSON-RPC requests")
    ).unwrap();
    registry().register(Box::new(counter.clone())).unwrap();
    counter
});

pub static REST_REQUESTS: once_cell::sync::Lazy<IntCounter> = once_cell::sync::Lazy::new(|| {
    let counter = IntCounter::with_opts(
        opts!("qonduit_rest_requests_total", "Total REST API requests")
    ).unwrap();
    registry().register(Box::new(counter.clone())).unwrap();
    counter
});

pub static WS_CONNECTIONS: once_cell::sync::Lazy<IntCounter> = once_cell::sync::Lazy::new(|| {
    let counter = IntCounter::with_opts(
        opts!("qonduit_ws_connections_total", "Total WebSocket connections")
    ).unwrap();
    registry().register(Box::new(counter.clone())).unwrap();
    counter
});

// --- Gauges ---
pub static CURRENT_TICK: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let gauge = IntGauge::with_opts(
        opts!("qonduit_current_tick", "Current tick being processed")
    ).unwrap();
    registry().register(Box::new(gauge.clone())).unwrap();
    gauge
});

pub static CURRENT_EPOCH: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let gauge = IntGauge::with_opts(
        opts!("qonduit_current_epoch", "Current epoch")
    ).unwrap();
    registry().register(Box::new(gauge.clone())).unwrap();
    gauge
});

pub static NATS_CONNECTED: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let gauge = IntGauge::with_opts(
        opts!("qonduit_nats_connected", "NATS connection status (1=connected)")
    ).unwrap();
    registry().register(Box::new(gauge.clone())).unwrap();
    gauge
});

pub static ACTIVE_WS_CLIENTS: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let gauge = IntGauge::with_opts(
        opts!("qonduit_active_ws_clients", "Number of active WebSocket clients")
    ).unwrap();
    registry().register(Box::new(gauge.clone())).unwrap();
    gauge
});

/// Render all metrics as Prometheus text format.
pub fn render_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = registry().gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}
