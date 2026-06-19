//! Qubic identity encoding (base-26, bidirectional).
//!
//! An identity is a 56-char base-26 uppercase string (A..Z),
//! typically representing a public key or address.

use std::fmt;

use crate::{IDENTITY_LENGTH, PUBLIC_KEY_SIZE};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QubicIdentity {
    pub public_key: [u8; PUBLIC_KEY_SIZE],
}

impl QubicIdentity {
    pub fn from_public_key(bytes: &[u8; PUBLIC_KEY_SIZE]) -> Self {
        Self { public_key: *bytes }
    }

    /// Encode to base-26 identity string.
    pub fn to_identity(&self) -> String {
        encode_base26(&self.public_key)
    }

    /// Decode from base-26 identity string.
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

/// Encode 32 bytes into a 56-char base-26 string (A..Z).
///
/// Works by treating the 32-byte array as a big-endian number and repeatedly
/// dividing by 26. Uses a simple big-number representation (Vec<u8> in base-256).
pub fn encode_base26(bytes: &[u8; PUBLIC_KEY_SIZE]) -> String {
    // Copy bytes into a mutable big-endian number (most significant first)
    let mut number: Vec<u8> = bytes.to_vec();
    let mut chars = Vec::with_capacity(IDENTITY_LENGTH);

    // Repeatedly divide by 26, collecting remainders
    loop {
        let mut remainder: u32 = 0;
        for item in &mut number {
            let val = remainder * 256 + *item as u32;
            *item = (val / 26) as u8;
            remainder = val % 26;
        }
        chars.push((b'A' + remainder as u8) as char);

        // Check if number is zero
        if number.iter().all(|&b| b == 0) {
            break;
        }
    }

    // Pad to IDENTITY_LENGTH
    while chars.len() < IDENTITY_LENGTH {
        chars.push('A');
    }

    chars.reverse();
    chars.into_iter().collect()
}

/// Decode a 56-char base-26 string into 32 bytes.
pub fn decode_base26(s: &str) -> Option<[u8; PUBLIC_KEY_SIZE]> {
    if s.len() != IDENTITY_LENGTH {
        return None;
    }

    // Accumulate the base-26 number into a big-endian byte vector
    let mut number: Vec<u8> = vec![0];

    for ch in s.chars() {
        let val = ch as u32;
        if val < 'A' as u32 || val > 'Z' as u32 {
            return None;
        }
        let digit = val - 'A' as u32;

        // Multiply number by 26
        let mut carry: u32 = digit;
        for i in (0..number.len()).rev() {
            let val = number[i] as u32 * 26 + carry;
            number[i] = (val & 0xFF) as u8;
            carry = val >> 8;
        }
        if carry > 0 {
            number.insert(0, carry as u8);
        }
    }

    // Convert to 32-byte array (big-endian)
    if number.len() > PUBLIC_KEY_SIZE {
        return None;
    }

    let mut result = [0u8; PUBLIC_KEY_SIZE];
    result[PUBLIC_KEY_SIZE - number.len()..].copy_from_slice(&number);
    Some(result)
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
    fn test_zero_key() {
        let key = [0u8; 32];
        let identity = encode_base26(&key);
        assert_eq!(identity.len(), IDENTITY_LENGTH);
        assert!(identity.chars().all(|c| c == 'A'));
        let decoded = decode_base26(&identity).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn test_invalid_length() {
        assert!(decode_base26("ABC").is_none());
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
    fn test_all_a_identity() {
        // All A's should decode to all zeros
        let identity = "A".repeat(IDENTITY_LENGTH);
        let bytes = decode_base26(&identity).unwrap();
        assert_eq!(bytes, [0u8; 32]);
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
    fn test_max_key() {
        let key = [0xFFu8; 32];
        let identity = encode_base26(&key);
        assert!(identity.chars().all(|c| c >= 'A' && c <= 'Z'));
        let decoded = decode_base26(&identity).unwrap();
        assert_eq!(decoded, key);
    }
}
