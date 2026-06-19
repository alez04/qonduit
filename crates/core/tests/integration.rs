use qonduit_core::*;

#[test]
fn test_header_and_type_together() {
    let header = RequestResponseHeader::new_request(
        NetworkMessageType::RequestEntity as u8,
        32,
        42,
    );
    assert_eq!(header.network_type(), Some(NetworkMessageType::RequestEntity));
    assert_eq!(header.payload_size(), 32);
}

#[test]
fn test_identity_display() {
    let key = [0u8; 32];
    let id = qonduit_core::identity::encode_base26(&key);
    assert_eq!(id.len(), 60);
    // Zero public key: first 56 chars are A, last 4 are K12 checksum
    assert!(id[..56].chars().all(|c| c == 'A'));
    assert_eq!(&id[56..], "FXIB");
}

#[test]
fn test_constants_consistency() {
    // Spectrum capacity = 2^depth
    assert_eq!(SPECTRUM_CAPACITY, 1u64 << SPECTRUM_DEPTH);
    assert_eq!(ASSETS_CAPACITY, 1u64 << ASSETS_DEPTH);
}
