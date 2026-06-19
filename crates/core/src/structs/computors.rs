/// Computors list from BroadcastComputors (type 2).
///
/// Size: 21634 bytes total packet (header + 21626 payload).
/// Layout: 4 bytes epoch + 676 * 32 bytes public keys + 64 bytes signature.

use serde::{Deserialize, Serialize};

/// Decoded computors list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Computors {
    pub epoch: u16,
    pub public_keys: Vec<[u8; 32]>,
    pub public_key_identities: Vec<String>,
}
