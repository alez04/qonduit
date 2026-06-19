//! Integration tests for the ingestion decoders.

use qonduit_ingestion::decoders;

#[test]
fn test_decode_current_tick_info_valid() {
    // 14-byte payload for CurrentTickInfo
    let mut payload = vec![0u8; 14];
    // epoch = 100
    payload[0] = 100;
    payload[1] = 0;
    // tick = 50000
    payload[2..6].copy_from_slice(&50000u32.to_le_bytes());
    // aligned_votes = 450
    payload[6..8].copy_from_slice(&450u16.to_le_bytes());
    // misaligned_votes = 10
    payload[8..10].copy_from_slice(&10u16.to_le_bytes());
    // initial_tick = 49000
    payload[10..14].copy_from_slice(&49000u32.to_le_bytes());

    let result = decoders::decode_current_tick_info(&payload).unwrap();
    assert_eq!(result.epoch, 100);
    assert_eq!(result.tick, 50000);
    assert_eq!(result.number_of_aligned_votes, 450);
    assert_eq!(result.number_of_misaligned_votes, 10);
    assert_eq!(result.initial_tick, 49000);
}

#[test]
fn test_decode_current_tick_info_too_small() {
    let payload = vec![0u8; 10];
    assert!(decoders::decode_current_tick_info(&payload).is_err());
}

#[test]
fn test_decode_system_info_valid() {
    let mut payload = vec![0u8; 144];
    // version = 135
    payload[0..8].copy_from_slice(&135u64.to_le_bytes());
    // peer_count = 42
    payload[24..32].copy_from_slice(&42u64.to_le_bytes());
    // current_tick = 100000
    payload[48..56].copy_from_slice(&100000u64.to_le_bytes());

    let result = decoders::decode_system_info(&payload).unwrap();
    assert_eq!(result.version, 135);
    assert_eq!(result.peer_count, 42);
    assert_eq!(result.current_tick, 100000);
}

#[test]
fn test_decode_system_info_too_small() {
    let payload = vec![0u8; 100];
    assert!(decoders::decode_system_info(&payload).is_err());
}

#[test]
fn test_decode_entity_valid() {
    let mut payload = vec![0u8; 64]; // minimum size is 64 bytes
    // Set a recognizable public key (all 0x42)
    payload[0..32].fill(0x42);
    // incoming = 1000000
    payload[32..40].copy_from_slice(&1000000i64.to_le_bytes());
    // outgoing = 500000
    payload[40..48].copy_from_slice(&500000i64.to_le_bytes());

    let result = decoders::decode_entity(&payload).unwrap();
    assert_eq!(result.incoming, 1000000);
    assert_eq!(result.outgoing, 500000);
    assert!(!result.identity.is_empty());
}

#[test]
fn test_decode_transaction_valid() {
    let mut payload = vec![0u8; 224]; // 80 header + 80 input + 64 sig
    // tx_type = 0 (transfer)
    payload[0] = 0;
    // source (32 bytes of 0x01)
    payload[1..33].fill(0x01);
    // destination (32 bytes of 0x02)
    payload[33..65].fill(0x02);
    // amount = 1000
    payload[65..73].copy_from_slice(&1000i64.to_le_bytes());
    // tick = 50000
    payload[73..77].copy_from_slice(&50000u32.to_le_bytes());
    // input_size = 80
    payload[77..79].copy_from_slice(&80u16.to_le_bytes());
    // input_type = 0
    payload[79..81].copy_from_slice(&0u16.to_le_bytes());

    let result = decoders::decode_transaction(&payload).unwrap();
    assert_eq!(result.tx_type, 0);
    assert_eq!(result.amount, 1000);
    assert_eq!(result.tick, 50000);
    assert_eq!(result.input_size, 80);
    assert!(!result.source_identity.is_empty());
    assert!(!result.destination_identity.is_empty());
}

#[test]
fn test_decode_transaction_too_small() {
    let payload = vec![0u8; 50];
    assert!(decoders::decode_transaction(&payload).is_err());
}

#[test]
fn test_decode_computors_valid() {
    // 21626 bytes: 2 epoch + 2 padding + 676*32 keys + 64 sig
    let mut payload = vec![0u8; 21626];
    // epoch = 42 (at offset 0, little-endian u16)
    payload[0] = 42;
    payload[1] = 0;
    // First key at offset 4 (2-byte epoch + 2-byte padding): all 0xAA
    payload[4..36].fill(0xAA);

    let result = decoders::decode_computors(&payload).unwrap();
    assert_eq!(result.epoch, 42);
    assert!(!result.public_keys.is_empty());
    assert!(!result.public_key_identities.is_empty());
}

#[test]
fn test_decode_tick_valid() {
    let mut payload = vec![0u8; 1708];
    // epoch = 10 at offset 0
    payload[0] = 10;
    payload[1] = 0;
    // numberOfTransactions = 5 at offset 2
    payload[2] = 5;
    // numberOfSpecialEvents = 2 at offset 3
    payload[3] = 2;
    // tick = 99999 at offset 4
    payload[4..8].copy_from_slice(&99999u32.to_le_bytes());
    // timestamp at offset 8
    payload[8..16].copy_from_slice(&1234567890u64.to_le_bytes());
    // salted_spectrum_hash at offset 48 (first byte = 0xAA)
    payload[48] = 0xAA;
    // salted_universe_hash at offset 80 (first byte = 0xBB)
    payload[80] = 0xBB;
    // salted_computor_hash at offset 112 (first byte = 0xCC)
    payload[112] = 0xCC;
    // mining_nonce at offset 1704
    payload[1704..1708].copy_from_slice(&42u32.to_le_bytes());

    let result = decoders::decode_tick(&payload).unwrap();
    assert_eq!(result.epoch, 10);
    assert_eq!(result.tick, 99999);
    assert_eq!(result.timestamp, 1234567890);
    assert_eq!(result.number_of_transactions, 5);
    assert_eq!(result.number_of_special_events, 2);
    assert_eq!(result.transaction_count, 5);
    assert_eq!(result.mining_nonce, 42);
    assert_eq!(result.salted_spectrum_hash[0], 0xAA);
    assert_eq!(result.salted_universe_hash[0], 0xBB);
    assert_eq!(result.salted_computor_hash[0], 0xCC);
    assert_eq!(result.signature_count, 0);
}
