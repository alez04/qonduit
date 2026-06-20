//! Integration tests for the ingestion decoders.

use qonduit_ingestion::decoders;

#[test]
fn test_decode_current_tick_info_valid() {
    // 16-byte payload for CurrentTickInfo
    let mut payload = vec![0u8; 16];
    // tickDuration = 200 at offset 0
    payload[0..2].copy_from_slice(&200u16.to_le_bytes());
    // epoch = 100 at offset 2
    payload[2..4].copy_from_slice(&100u16.to_le_bytes());
    // tick = 50000 at offset 4
    payload[4..8].copy_from_slice(&50000u32.to_le_bytes());
    // aligned_votes = 450 at offset 8
    payload[8..10].copy_from_slice(&450u16.to_le_bytes());
    // misaligned_votes = 10 at offset 10
    payload[10..12].copy_from_slice(&10u16.to_le_bytes());
    // initial_tick = 49000 at offset 12
    payload[12..16].copy_from_slice(&49000u32.to_le_bytes());

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
    let mut payload = vec![0u8; 128];
    // version = 135 (i16 LE) at offset 0
    payload[0..2].copy_from_slice(&135i16.to_le_bytes());
    // epoch = 10 at offset 2
    payload[2..4].copy_from_slice(&10u16.to_le_bytes());
    // tick = 100000 at offset 4
    payload[4..8].copy_from_slice(&100000u32.to_le_bytes());

    let result = decoders::decode_system_info(&payload).unwrap();
    assert_eq!(result.version, 135);
    assert_eq!(result.epoch, 10);
    assert_eq!(result.tick, 100000);
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
    // Payload: 2 epoch + 676*32 keys = 21634 minimum (no signature needed for key parsing)
    let mut payload = vec![0u8; 21634];
    // epoch = 42 (at offset 0, little-endian u16)
    payload[0] = 42;
    payload[1] = 0;
    // First key at offset 2 (no padding in C++ struct): all 0xAA
    payload[2..34].fill(0xAA);

    let result = decoders::decode_computors(&payload).unwrap();
    assert_eq!(result.epoch, 42);
    assert!(!result.public_keys.is_empty());
    assert!(!result.public_key_identities.is_empty());
}

#[test]
fn test_decode_tick_valid() {
    // Minimum: header (16) + timelock (32) + signature (64) = 112 bytes
    let mut payload = vec![0u8; 112];
    // computor_index = 7 at offset 0
    payload[0..2].copy_from_slice(&7u16.to_le_bytes());
    // epoch = 10 at offset 2
    payload[2..4].copy_from_slice(&10u16.to_le_bytes());
    // tick = 99999 at offset 4
    payload[4..8].copy_from_slice(&99999u32.to_le_bytes());
    // millisecond = 500 at offset 8
    payload[8..10].copy_from_slice(&500u16.to_le_bytes());
    // second = 30 at offset 10
    payload[10] = 30;
    // minute = 15 at offset 11
    payload[11] = 15;
    // hour = 10 at offset 12
    payload[12] = 10;
    // day = 20 at offset 13
    payload[13] = 20;
    // month = 6 at offset 14
    payload[14] = 6;
    // year = 26 at offset 15
    payload[15] = 26;
    // timelock at offset 16 (first byte = 0xDD)
    payload[16] = 0xDD;

    let result = decoders::decode_tick(&payload).unwrap();
    assert_eq!(result.computor_index, 7);
    assert_eq!(result.epoch, 10);
    assert_eq!(result.tick, 99999);
    assert_eq!(result.time_lock[0], 0xDD);
}
