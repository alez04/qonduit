/// System info response from RespondSystemInfo (type 47).
///
/// Packet size: 152 bytes total (8 + 144 payload).
/// Layout matches C++ SystemInfoReply.

use serde::{Deserialize, Serialize};

/// Current tick info response (type 28).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentTickInfo {
    pub epoch: u16,
    pub tick: u32,
    pub number_of_aligned_votes: u16,
    pub number_of_misaligned_votes: u16,
    pub initial_tick: u32,
    pub latest_voting_tick: u32,
    pub time_since_last_voting_tick: u64,
}

/// System info reply (type 47).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfoReply {
    pub version: u64,
    pub system_time_start: u64,
    pub system_time_end: u64,
    pub peer_count: u64,
    pub first_epoch_start_tick: u64,
    pub last_epoch_start_tick: u64,
    pub current_tick: u64,
    pub last_computor_event_tick: u64,
    pub last_tick_transaction_count: u64,
    pub max_peer_count: u64,
}

/// Bob sync status (from Bob RPC).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BobSyncStatus {
    NotSyncing {
        syncing: bool,
    },
    Syncing {
        syncing: bool,
        epoch: u16,
        initial_tick: u32,
        current_fetching_tick: u32,
        latest_voting_tick: u32,
        processing_duration_per_tick: u64,
        time_since_last_voting_tick: u64,
    },
}

impl BobSyncStatus {
    pub fn not_syncing() -> Self {
        Self::NotSyncing {
            syncing: false,
        }
    }

    pub fn is_syncing(&self) -> bool {
        match self {
            Self::NotSyncing { syncing } => *syncing,
            Self::Syncing { syncing, .. } => *syncing,
        }
    }
}
