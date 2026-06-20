//! System info response from RespondSystemInfo (type 47).
//!
//! Packet size: 136 bytes total (8 header + 128 payload).
//! Layout matches C++ RespondSystemInfo from `system_info.h` (128 bytes, #pragma pack(1)):
//!
//! ```text
//! Offset  Size  Field
//! 0       2     version (i16)
//! 2       2     epoch (u16)
//! 4       4     tick (u32)
//! 8       4     initialTick (u32)
//! 12      4     latestCreatedTick (u32)
//! 16      2     initialMillisecond (u16)
//! 18      1     initialSecond (u8)
//! 19      1     initialMinute (u8)
//! 20      1     initialHour (u8)
//! 21      1     initialDay (u8)
//! 22      1     initialMonth (u8)
//! 23      1     initialYear (u8)
//! 24      4     numberOfEntities (u32)
//! 28      4     numberOfTransactions (u32)
//! 32      32    randomMiningSeed (m256i)
//! 64      4     solutionThreshold (i32)
//! 68      8     totalSpectrumAmount (u64)
//! 76      8     currentEntityBalanceDustThreshold (u64)
//! 84      4     targetTickVoteSignature (u32)
//! 88      8     computorPacketSignature (u64)
//! 96      8     solutionAdditionalThreshold (u64)
//! 104     8     _reserve2 (u64)
//! 112     8     _reserve3 (u64)
//! 120     8     _reserve4 (u64)
//! ```

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

/// System info reply (type 47) matching C++ RespondSystemInfo layout.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfoReply {
    pub version: i16,
    pub epoch: u16,
    pub tick: u32,
    pub initial_tick: u32,
    pub latest_created_tick: u32,
    pub initial_millisecond: u16,
    pub initial_second: u8,
    pub initial_minute: u8,
    pub initial_hour: u8,
    pub initial_day: u8,
    pub initial_month: u8,
    pub initial_year: u8,
    pub number_of_entities: u32,
    pub number_of_transactions: u32,
    pub random_mining_seed_hex: String,
    pub solution_threshold: i32,
    pub total_spectrum_amount: u64,
    pub current_entity_balance_dust_threshold: u64,
    pub target_tick_vote_signature: u32,
    pub computor_packet_signature: u64,
    pub solution_additional_threshold: u64,
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
