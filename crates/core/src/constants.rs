//! Qubic protocol constants from `common_def.h` and `public_settings.h`.
//!
//! All values verified against the C++ core source.

/// Number of computors (26^2).
pub const NUMBER_OF_COMPUTORS: usize = 676;

/// Quorum threshold: 2/3 + 1 of computors.
pub const QUORUM: usize = NUMBER_OF_COMPUTORS * 2 / 3 + 1; // 451

/// SchnorrQ signature size in bytes.
pub const SIGNATURE_SIZE: usize = 64;

/// Public key size (m256i).
pub const PUBLIC_KEY_SIZE: usize = 32;

/// Maximum transaction input payload size.
pub const MAX_INPUT_SIZE: usize = 1024;

/// Transaction header size (without payload or signature).
pub const TX_HEADER_SIZE: usize = 80;

/// Maximum contract index.
pub const MAX_NUMBER_OF_CONTRACTS: usize = 1024;

/// Number of transaction slots per tick (post-epoch 214).
pub const NUMBER_OF_TRANSACTIONS_PER_TICK: usize = 4096;

/// Legacy transaction slots per tick (pre-epoch 214).
pub const LEGACY_NUMBER_OF_TRANSACTIONS_PER_TICK: usize = 1024;

/// First epoch using the 4096-slot tick layout.
pub const EPOCH_FIRST_4096_TX_PER_TICK: u16 = 214;

/// Number of special events per tick (contract lifecycle).
pub const NUMBER_OF_SPECIAL_EVENT_PER_TICK: usize = 6;

/// Total log-producing slots per tick (transactions + special events).
pub const LOG_TX_PER_TICK: usize =
    NUMBER_OF_TRANSACTIONS_PER_TICK + NUMBER_OF_SPECIAL_EVENT_PER_TICK; // 4102

/// Spectrum hash map capacity (2^24).
pub const SPECTRUM_CAPACITY: u64 = 1 << SPECTRUM_DEPTH;

/// Spectrum Merkle tree depth.
pub const SPECTRUM_DEPTH: u32 = 24;

/// Assets hash map capacity (2^24).
pub const ASSETS_CAPACITY: u64 = 1 << ASSETS_DEPTH;

/// Assets Merkle tree depth.
pub const ASSETS_DEPTH: u32 = 24;

/// QU issuance rate per tick.
pub const ISSUANCE_RATE: i64 = 1_000_000_000_000; // 1e12

/// Maximum transfer amount.
pub const MAX_AMOUNT: i64 = ISSUANCE_RATE * 1000; // 1e15

/// Total supply cap.
pub const MAX_SUPPLY: u64 = ISSUANCE_RATE as u64 * 200; // 2e14

/// Target tick duration in milliseconds.
pub const TARGET_TICK_DURATION_MS: u32 = 1000;

/// Tick vote signing difficulty.
pub const TARGET_TICK_VOTE_SIGNATURE: u32 = 0x0009_5CBE;

/// Default Qubic node TCP port.
pub const DEFAULT_PORT: u16 = 21841;

/// Bob node TCP port.
pub const BOB_PORT: u16 = 21842;

/// Identity string length.
pub const IDENTITY_LENGTH: usize = 60;

/// Seed string length.
pub const SEED_LENGTH: usize = 55;

/// Transaction hash length (same as identity).
pub const TX_HASH_LENGTH: usize = 60;

/// Packet header size.
pub const HEADER_SIZE: usize = 8;

/// Maximum packet size (3 bytes in header = 16 MB - 1).
pub const MAX_MESSAGE_SIZE: u32 = 0x00FF_FFFF;

// --- Special transaction indices (contract lifecycle) ---

/// Contract initialization pseudo-transaction.
pub const SC_INITIALIZE_TX: usize = NUMBER_OF_TRANSACTIONS_PER_TICK;

/// Begin-epoch pseudo-transaction.
pub const SC_BEGIN_EPOCH_TX: usize = NUMBER_OF_TRANSACTIONS_PER_TICK + 1;

/// Begin-tick pseudo-transaction.
pub const SC_BEGIN_TICK_TX: usize = NUMBER_OF_TRANSACTIONS_PER_TICK + 2;

/// End-tick pseudo-transaction.
pub const SC_END_TICK_TX: usize = NUMBER_OF_TRANSACTIONS_PER_TICK + 3;

/// End-epoch pseudo-transaction.
pub const SC_END_EPOCH_TX: usize = NUMBER_OF_TRANSACTIONS_PER_TICK + 4;

/// Contract notification pseudo-transaction.
pub const SC_NOTIFICATION_TX: usize = NUMBER_OF_TRANSACTIONS_PER_TICK + 5;

// --- Log event types ---

pub const LOG_QU_TRANSFER: u8 = 0;
pub const LOG_ASSET_ISSUANCE: u8 = 1;
pub const LOG_ASSET_OWNERSHIP_CHANGE: u8 = 2;
pub const LOG_ASSET_POSSESSION_CHANGE: u8 = 3;
pub const LOG_CONTRACT_ERROR_MESSAGE: u8 = 4;
pub const LOG_CONTRACT_WARNING_MESSAGE: u8 = 5;
pub const LOG_CONTRACT_INFORMATION_MESSAGE: u8 = 6;
pub const LOG_CONTRACT_DEBUG_MESSAGE: u8 = 7;
pub const LOG_BURNING: u8 = 8;
pub const LOG_DUST_BURNING: u8 = 9;
pub const LOG_SPECTRUM_STATS: u8 = 10;
pub const LOG_ASSET_OWNERSHIP_MANAGING_CONTRACT_CHANGE: u8 = 11;
pub const LOG_ASSET_POSSESSION_MANAGING_CONTRACT_CHANGE: u8 = 12;
pub const LOG_CONTRACT_RESERVE_DEDUCTION: u8 = 13;
pub const LOG_ORACLE_QUERY_STATUS_CHANGE: u8 = 14;
pub const LOG_ORACLE_SUBSCRIBER_MESSAGE: u8 = 15;
pub const LOG_CUSTOM_MESSAGE: u8 = 255;

/// Log event packed header size.
pub const LOG_HEADER_SIZE: usize = 26;

// --- Asset types ---

pub const ASSET_TYPE_EMPTY: u8 = 0;
pub const ASSET_TYPE_ISSUANCE: u8 = 1;
pub const ASSET_TYPE_OWNERSHIP: u8 = 2;
pub const ASSET_TYPE_POSSESSION: u8 = 3;

// --- Oracle constants ---

pub const ORACLE_QUERY_TYPE_CONTRACT_QUERY: u8 = 0;
pub const ORACLE_QUERY_TYPE_CONTRACT_SUBSCRIPTION: u8 = 1;
pub const ORACLE_QUERY_TYPE_USER_QUERY: u8 = 2;

pub const ORACLE_QUERY_STATUS_UNKNOWN: u8 = 0;
pub const ORACLE_QUERY_STATUS_PENDING: u8 = 1;
pub const ORACLE_QUERY_STATUS_COMMITTED: u8 = 2;
pub const ORACLE_QUERY_STATUS_SUCCESS: u8 = 3;
pub const ORACLE_QUERY_STATUS_TIMEOUT: u8 = 4;
pub const ORACLE_QUERY_STATUS_UNRESOLVABLE: u8 = 5;

// --- Custom message operation types ---

/// STA_DDIV: Start distribute dividends.
pub const CUSTOM_MESSAGE_OP_START_DISTRIBUTE_DIVIDENDS: u64 = 6_217_575_821_008_262_227;
/// END_DDIV: End distribute dividends.
pub const CUSTOM_MESSAGE_OP_END_DISTRIBUTE_DIVIDENDS: u64 = 6_217_575_821_008_457_285;
/// STA_EPOC: Start epoch.
pub const CUSTOM_MESSAGE_OP_START_EPOCH: u64 = 4_850_183_582_582_395_987;
/// END_EPOC: End epoch.
pub const CUSTOM_MESSAGE_OP_END_EPOCH: u64 = 4_850_183_582_582_591_045;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quorum() {
        assert_eq!(QUORUM, 451);
        assert!(QUORUM > NUMBER_OF_COMPUTORS / 2);
    }

    #[test]
    fn test_special_event_indices() {
        assert_eq!(SC_INITIALIZE_TX, 4096);
        assert_eq!(SC_BEGIN_EPOCH_TX, 4097);
        assert_eq!(SC_BEGIN_TICK_TX, 4098);
        assert_eq!(SC_END_TICK_TX, 4099);
        assert_eq!(SC_END_EPOCH_TX, 4100);
        assert_eq!(SC_NOTIFICATION_TX, 4101);
    }

    #[test]
    fn test_log_tx_per_tick() {
        assert_eq!(
            LOG_TX_PER_TICK,
            NUMBER_OF_TRANSACTIONS_PER_TICK + NUMBER_OF_SPECIAL_EVENT_PER_TICK
        );
        assert_eq!(LOG_TX_PER_TICK, 4102);
    }

    #[test]
    fn test_log_event_types_contiguous() {
        assert_eq!(LOG_QU_TRANSFER, 0);
        assert_eq!(LOG_ORACLE_SUBSCRIBER_MESSAGE, 15);
        assert_eq!(LOG_CUSTOM_MESSAGE, 255);
    }

    #[test]
    fn test_issuance_rate() {
        assert_eq!(ISSUANCE_RATE, 1_000_000_000_000);
        assert_eq!(MAX_AMOUNT, ISSUANCE_RATE * 1000);
    }

    #[test]
    fn test_asset_types() {
        assert_eq!(ASSET_TYPE_EMPTY, 0);
        assert_eq!(ASSET_TYPE_ISSUANCE, 1);
        assert_eq!(ASSET_TYPE_OWNERSHIP, 2);
        assert_eq!(ASSET_TYPE_POSSESSION, 3);
    }
}
