//! Qubic identity encoding (base-26 with K12 checksum).
//!
//! A Qubic identity is a 60-char base-26 uppercase string (A..Z).
//! - Characters 0..56: public key encoded as 4 × 14 base-26 digits (each 8-byte LE chunk)
//! - Characters 56..60: K12 checksum (18 bits from K12(publicKey)[0..3], encoded as 4 base-26 digits)

use std::fmt;

use kangarootwelve::KangarooTwelve;

use crate::{IDENTITY_LENGTH, PUBLIC_KEY_SIZE};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QubicIdentity {
    pub public_key: [u8; PUBLIC_KEY_SIZE],
}

impl QubicIdentity {
    pub fn from_public_key(bytes: &[u8; PUBLIC_KEY_SIZE]) -> Self {
        Self { public_key: *bytes }
    }

    /// Encode to base-26 identity string (60 chars with checksum).
    pub fn to_identity(&self) -> String {
        encode_base26(&self.public_key)
    }

    /// Decode from base-26 identity string (verifies checksum).
    pub fn from_identity(s: &str) -> Option<Self> {
        let bytes = decode_base26(s)?;
        let mut public_key = [0u8; PUBLIC_KEY_SIZE];
        public_key.copy_from_slice(&bytes);
        Some(Self { public_key })
    }
}

impl fmt::Display for QubicIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_identity())
    }
}

/// Compute K12 checksum for a public key.
///
/// Returns a 4-char base-26 string from 18 bits of K12(publicKey).
fn compute_checksum(public_key: &[u8; PUBLIC_KEY_SIZE]) -> [char; 4] {
    let mut hasher = KangarooTwelve::hash(public_key, b"");
    let mut bytes = [0u8; 3];
    hasher.squeeze(&mut bytes);

    // Form 18-bit checksum from first 3 bytes (little-endian)
    let checksum: u32 =
        bytes[0] as u32 | ((bytes[1] as u32) << 8) | ((bytes[2] as u32) << 16);
    let checksum = checksum & 0x3FFFF; // mask to 18 bits

    let mut chars = ['A'; 4];
    let mut val = checksum;
    for c in chars.iter_mut() {
        *c = (b'A' + (val % 26) as u8) as char;
        val /= 26;
    }
    chars
}

/// Encode 32-byte public key into a 60-char base-26 identity string (A..Z).
///
/// The 32 bytes are split into 4 little-endian 8-byte fragments.
/// Each fragment is independently base-26 encoded into 14 characters.
/// The last 4 characters are a K12 checksum.
pub fn encode_base26(bytes: &[u8; PUBLIC_KEY_SIZE]) -> String {
    let mut identity = ['A'; IDENTITY_LENGTH];

    // Encode each 8-byte LE fragment into 14 base-26 characters
    for frag_idx in 0..4 {
        let offset = frag_idx * 8;
        let mut fragment = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);

        let base = frag_idx * 14;
        for digit in 0..14 {
            identity[base + digit] = (b'A' + (fragment % 26) as u8) as char;
            fragment /= 26;
        }
    }

    // Compute and append checksum
    let checksum = compute_checksum(bytes);
    identity[56] = checksum[0];
    identity[57] = checksum[1];
    identity[58] = checksum[2];
    identity[59] = checksum[3];

    identity.iter().collect()
}

/// Decode a 60-char base-26 string into 32-byte public key.
///
/// Verifies the K12 checksum (last 4 characters).
pub fn decode_base26(s: &str) -> Option<[u8; PUBLIC_KEY_SIZE]> {
    if s.len() != IDENTITY_LENGTH {
        return None;
    }

    let bytes = s.as_bytes();
    let mut public_key = [0u8; PUBLIC_KEY_SIZE];

    // Decode each 14-char fragment into an 8-byte LE chunk
    for frag_idx in 0..4 {
        let base = frag_idx * 14;
        let mut fragment: u64 = 0;

        // Read most-significant digit first (index 13 down to 0)
        for digit in (0..14).rev() {
            let ch = bytes[base + digit];
            if !ch.is_ascii_uppercase() {
                return None;
            }
            fragment = fragment * 26 + (ch - b'A') as u64;
        }

        let offset = frag_idx * 8;
        let le_bytes = fragment.to_le_bytes();
        public_key[offset..offset + 8].copy_from_slice(&le_bytes);
    }

    // Verify checksum
    let expected = compute_checksum(&public_key);
    if bytes[56] != expected[0] as u8
        || bytes[57] != expected[1] as u8
        || bytes[58] != expected[2] as u8
        || bytes[59] != expected[3] as u8
    {
        return None;
    }

    Some(public_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let key = [0x42u8; 32];
        let identity = encode_base26(&key);
        assert_eq!(identity.len(), IDENTITY_LENGTH);
        let decoded = decode_base26(&identity).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn test_zero_key_identity() {
        // Zero public key: first 56 chars are all A's, last 4 are the K12 checksum
        let key = [0u8; 32];
        let identity = encode_base26(&key);
        assert_eq!(identity.len(), IDENTITY_LENGTH);
        // First 56 chars should all be A
        assert!(identity[..56].chars().all(|c| c == 'A'));
        // Last 4 chars are the checksum (user reported it should be FXIB)
        assert_eq!(&identity[56..], "FXIB");
        // Roundtrip
        let decoded = decode_base26(&identity).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn test_checksum_verification() {
        let key = [0x42u8; 32];
        let identity = encode_base26(&key);
        // Tamper with last char
        let mut bad = identity.clone();
        let last = bad.pop().unwrap();
        bad.push(if last == 'A' { 'B' } else { 'A' });
        assert!(decode_base26(&bad).is_none());
    }

    #[test]
    fn test_invalid_length() {
        assert!(decode_base26("ABC").is_none());
        assert!(decode_base26(&"A".repeat(56)).is_none());
        // 60 A's has the right length but wrong checksum (zero key = FXIB, not AAAA)
        assert!(decode_base26(&"A".repeat(60)).is_none());
    }

    #[test]
    fn test_invalid_chars() {
        let s = "a".repeat(IDENTITY_LENGTH);
        assert!(decode_base26(&s).is_none());
    }

    #[test]
    fn test_encode_length() {
        let key = [0xFFu8; 32];
        let identity = encode_base26(&key);
        assert_eq!(identity.len(), IDENTITY_LENGTH);
    }

    #[test]
    fn test_max_key() {
        let key = [0xFFu8; 32];
        let identity = encode_base26(&key);
        assert!(identity.chars().all(|c| c >= 'A' && c <= 'Z'));
        let decoded = decode_base26(&identity).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn test_deterministic() {
        let key = [1u8; 32];
        let a = encode_base26(&key);
        let b = encode_base26(&key);
        assert_eq!(a, b);
    }

    #[test]
    fn test_different_keys_different_identity() {
        let a = encode_base26(&[0u8; 32]);
        let b = encode_base26(&[1u8; 32]);
        assert_ne!(a, b);
    }

    #[test]
    fn test_contract_index_1() {
        // Contract index 1: public key is [1, 0, 0, ..., 0] (LE)
        let mut key = [0u8; 32];
        key[0] = 1;
        let identity = encode_base26(&key);
        assert_eq!(identity.len(), IDENTITY_LENGTH);
        let decoded = decode_base26(&identity).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn test_identity_only_uppercase() {
        let key = [0xABu8; 32];
        let identity = encode_base26(&key);
        assert!(identity.chars().all(|c| c.is_ascii_uppercase()));
    }
}
