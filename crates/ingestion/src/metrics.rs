//! Prometheus metrics for the ingestion pipeline.
//!
//! Follows the same registry pattern as `qonduit_query::metrics` so all
//! metrics are gathered from a single `Registry`.

use prometheus::{
    opts, Encoder, IntCounter, IntCounterVec, IntGauge, Registry, TextEncoder,
};
use std::sync::OnceLock;

pub static REGISTRY: OnceLock<Registry> = OnceLock::new();

fn registry() -> &'static Registry {
    REGISTRY.get_or_init(Registry::new)
}

// --- Counters ---

/// Total packets received from Qubic nodes.
pub static PACKETS_RECEIVED: once_cell::sync::Lazy<IntCounter> = once_cell::sync::Lazy::new(|| {
    let counter = IntCounter::with_opts(
        opts!(
            "qonduit_ingestion_packets_received_total",
            "Total packets received from Qubic nodes"
        ),
    )
    .unwrap();
    registry().register(Box::new(counter.clone())).unwrap();
    counter
});

/// Packets broken down by message type label.
pub static PACKETS_BY_TYPE: once_cell::sync::Lazy<IntCounterVec> =
    once_cell::sync::Lazy::new(|| {
        let counter = IntCounterVec::new(
            opts!(
                "qonduit_ingestion_packets_by_type",
                "Packets by message type"
            ),
            &["msg_type"],
        )
        .unwrap();
        registry().register(Box::new(counter.clone())).unwrap();
        counter
    });

/// Total packets successfully published to NATS.
pub static PACKETS_PUBLISHED: once_cell::sync::Lazy<IntCounter> =
    once_cell::sync::Lazy::new(|| {
        let counter = IntCounter::with_opts(
            opts!(
                "qonduit_ingestion_packets_published_total",
                "Total packets published to NATS"
            ),
        )
        .unwrap();
        registry().register(Box::new(counter.clone())).unwrap();
        counter
    });

/// Total decode errors.
pub static PACKETS_DECODE_ERRORS: once_cell::sync::Lazy<IntCounter> =
    once_cell::sync::Lazy::new(|| {
        let counter = IntCounter::with_opts(
            opts!(
                "qonduit_ingestion_decode_errors_total",
                "Total decode errors"
            ),
        )
        .unwrap();
        registry().register(Box::new(counter.clone())).unwrap();
        counter
    });

// --- Gauges ---

/// Number of known peers (healthy + unhealthy).
pub static PEER_COUNT: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let gauge = IntGauge::with_opts(
        opts!(
            "qonduit_ingestion_peer_count",
            "Number of known peers"
        ),
    )
    .unwrap();
    registry().register(Box::new(gauge.clone())).unwrap();
    gauge
});

/// Current epoch reported by the connected node.
pub static CURRENT_EPOCH: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let gauge = IntGauge::with_opts(
        opts!(
            "qonduit_ingestion_current_epoch",
            "Current epoch from node"
        ),
    )
    .unwrap();
    registry().register(Box::new(gauge.clone())).unwrap();
    gauge
});

/// Current tick reported by the connected node.
pub static CURRENT_TICK: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    let gauge = IntGauge::with_opts(
        opts!(
            "qonduit_ingestion_current_tick",
            "Current tick from node"
        ),
    )
    .unwrap();
    registry().register(Box::new(gauge.clone())).unwrap();
    gauge
});

/// Render all ingestion metrics as Prometheus text format.
pub fn render_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = registry().gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}
