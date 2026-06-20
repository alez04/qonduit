//! Tick vote aggregation for quorum detection.
//!
//! When enough computors (>= QUORUM) vote for the same tick,
//! a QuorumTick event is published.

use serde::{Deserialize, Serialize};

/// An aggregated quorum tick: enough computors voted for this tick.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuorumTick {
    pub epoch: u16,
    pub tick: u32,
    /// Number of computors that voted for this tick.
    pub vote_count: u16,
    /// Sorted list of computor indices that voted.
    pub voted_computors: Vec<u16>,
}
