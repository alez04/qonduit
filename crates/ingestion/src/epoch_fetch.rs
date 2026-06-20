//! Fetches epoch tick intervals from the official Qubic RPC and caches them
//! for precise epoch progress calculations.

use anyhow::{Context, Result};
use qonduit_core::epoch_intervals;
use tracing::{info, warn};

/// Fetch epoch intervals from the Qubic RPC and cache them locally.
///
/// Calls `https://rpc.qubic.org/query/v1/getProcessedTickIntervals` and
/// populates the global epoch interval cache. Safe to call multiple times.
pub async fn fetch_epoch_intervals() -> Result<usize> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .context("Failed to create HTTP client")?;

    let raw: Vec<epoch_intervals::IntervalEntry> = client
        .get("https://rpc.qubic.org/query/v1/getProcessedTickIntervals")
        .send()
        .await
        .context("Failed to fetch epoch intervals from RPC")?
        .json()
        .await
        .context("Failed to parse epoch intervals response")?;

    let count = raw.len();
    epoch_intervals::set_epoch_ranges(raw);

    let ranges = epoch_intervals::get_epoch_ranges();
    let total_ticks: u64 = ranges.iter().map(|r| r.tick_count as u64).sum();
    let epoch_count = ranges.len();

    info!(
        "Epoch intervals cached: {epoch_count} epochs, {total_ticks} total ticks (from {count} RPC entries)"
    );

    Ok(epoch_count)
}

/// Fetch epoch intervals, logging warnings on failure (non-fatal).
pub async fn fetch_epoch_intervals_safe() {
    match fetch_epoch_intervals().await {
        Ok(_) => {}
        Err(e) => {
            warn!("Failed to fetch epoch intervals (non-fatal, using estimates): {e:#}");
        }
    }
}
