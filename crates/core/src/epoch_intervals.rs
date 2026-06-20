//! Epoch interval data from the official Qubic RPC.
//!
//! Stores exact tick ranges per epoch for precise epoch progress calculations.
//! Data is populated by `qonduit_ingestion::epoch_fetch::fetch_and_cache()`.

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

/// Merged epoch info: for each epoch, the absolute first and last tick.
#[derive(Debug, Clone)]
pub struct EpochRange {
    pub epoch: u16,
    pub first_tick: u32,
    pub last_tick: u32,
    pub tick_count: u32,
}

/// JSON response entry from getProcessedTickIntervals.
#[derive(serde::Deserialize)]
pub struct IntervalEntry {
    pub epoch: u16,
    #[serde(rename = "firstTick")]
    pub first_tick: u32,
    #[serde(rename = "lastTick")]
    pub last_tick: u32,
}

static EPOCH_RANGES: OnceLock<RwLock<Vec<EpochRange>>> = OnceLock::new();

fn ranges() -> &'static RwLock<Vec<EpochRange>> {
    EPOCH_RANGES.get_or_init(|| RwLock::new(Vec::new()))
}

/// Replace the cached epoch ranges (called after fetching from RPC).
pub fn set_epoch_ranges(entries: Vec<IntervalEntry>) {
    let mut map: HashMap<u16, (u32, u32)> = HashMap::new();
    for entry in &entries {
        map.entry(entry.epoch)
            .and_modify(|(first, last)| {
                *first = (*first).min(entry.first_tick);
                *last = (*last).max(entry.last_tick);
            })
            .or_insert((entry.first_tick, entry.last_tick));
    }

    let mut ranges_vec: Vec<EpochRange> = map
        .into_iter()
        .map(|(epoch, (first, last))| {
            let tick_count = last.saturating_sub(first).saturating_add(1);
            EpochRange {
                epoch,
                first_tick: first,
                last_tick: last,
                tick_count,
            }
        })
        .collect();

    ranges_vec.sort_by_key(|r| r.epoch);

    if let Ok(mut w) = ranges().write() {
        *w = ranges_vec;
    }
}

/// Get the cached epoch ranges (returns empty vec if not yet fetched).
pub fn get_epoch_ranges() -> Vec<EpochRange> {
    ranges().read().map(|g| g.clone()).unwrap_or_default()
}

/// Get the range for a specific epoch, if known.
pub fn get_epoch_range(epoch: u16) -> Option<EpochRange> {
    ranges()
        .read()
        .ok()
        .and_then(|v| v.iter().find(|r| r.epoch == epoch).cloned())
}

/// Calculate indexing progress (0.0 - 100.0) for a given epoch and tick.
///
/// Returns `None` if epoch data is not available.
pub fn epoch_progress_pct(epoch: u16, indexed_tick: u32) -> Option<f64> {
    let range = get_epoch_range(epoch)?;
    if range.tick_count == 0 {
        return Some(100.0);
    }

    let position = indexed_tick.saturating_sub(range.first_tick);
    let pct = (position as f64 / range.tick_count as f64) * 100.0;
    Some(pct.min(100.0))
}

/// Calculate total ticks across all known epochs.
pub fn total_known_ticks() -> u64 {
    ranges()
        .read()
        .map(|v| v.iter().map(|r| r.tick_count as u64).sum())
        .unwrap_or(0)
}

/// Number of fully completed epochs (indexed tick >= last_tick).
pub fn epochs_fully_indexed(indexed_tick: u32) -> u32 {
    ranges()
        .read()
        .map(|v| v.iter().filter(|r| indexed_tick >= r.last_tick).count() as u32)
        .unwrap_or(0)
}
