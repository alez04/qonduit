//! Cryptographic hash utilities for Qubic.
//!
//! Uses SHA-256 for transaction hashing and identity derivation.

use sha2::{Digest, Sha256};

/// Compute SHA-256 hash of data.
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&result);
    out
}

/// Compute transaction hash from raw transaction bytes.
/// The hash covers the transaction header + input payload (without signature).
pub fn compute_tx_hash(tx_bytes: &[u8], input_size: u16) -> [u8; 32] {
    // Hash the first 80 + input_size bytes (everything before the signature)
    let hash_len = 80 + input_size as usize;
    let hash_input = if tx_bytes.len() >= hash_len {
        &tx_bytes[..hash_len]
    } else {
        tx_bytes
    };
    sha256(hash_input)
}

/// Encode a 32-byte hash as a base-26 identity string (for display).
pub fn hash_to_identity(hash: &[u8; 32]) -> String {
    crate::identity::encode_base26(hash)
}

/// Encode a 32-byte hash as hex string.
pub fn hash_to_hex(hash: &[u8; 32]) -> String {
    hex::encode(hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha256_empty() {
        let hash = sha256(b"");
        // SHA-256 of empty string
        assert_eq!(
            hex::encode(hash),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_sha256_hello() {
        let hash = sha256(b"hello");
        assert_eq!(
            hex::encode(hash),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn test_compute_tx_hash_deterministic() {
        let data = vec![0u8; 160]; // 80 header + 80 input
        let h1 = compute_tx_hash(&data, 80);
        let h2 = compute_tx_hash(&data, 80);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_compute_tx_hash_different_inputs() {
        let mut data = vec![0u8; 160];
        let h1 = compute_tx_hash(&data, 80);
        data[80] = 1; // Change first byte of input
        let h2 = compute_tx_hash(&data, 80);
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_hash_to_hex() {
        let hash = sha256(b"test");
        let hex_str = hash_to_hex(&hash);
        assert_eq!(hex_str.len(), 64);
        assert!(hex_str.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
