/// Asset structs (issued, owned, possessed).
///
/// Each asset packet is a single `AssetRecord`.
/// Sizes: issued=824, owned=872, possessed=920 bytes (including header).

use serde::{Deserialize, Serialize};

pub const ASSET_ISSUANCE_RECORD_SIZE: usize = 816; // payload only
pub const ASSET_OWNERSHIP_RECORD_SIZE: usize = 864;
pub const ASSET_POSSESSION_RECORD_SIZE: usize = 912;

/// Asset type discriminator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AssetType {
    Empty,
    Issuance,
    Ownership,
    Possession,
}

impl AssetType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Empty),
            1 => Some(Self::Issuance),
            2 => Some(Self::Ownership),
            3 => Some(Self::Possession),
            _ => None,
        }
    }
}

/// Decoded asset record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetRecord {
    pub asset_type: AssetType,
    pub managed: bool,
    pub issuing_entity: [u8; 32],
    pub owning_entity: [u8; 32],
    pub possessing_entity: [u8; 32],
    pub issuance_index: u32,
    pub issuance_amount: i64,
    pub number_of_units: i64,
    pub unit_of_measurement: u64,
    pub denomination: u32,
    pub value_per_unit: i64,
    pub name: String,
    pub description: String,
    pub url: String,
    pub hash: [u8; 32],
    pub managing_contract_index: i32,
}

/// Simplified asset listing for /issued-assets, etc.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetSummary {
    pub asset_type: AssetType,
    pub issuance_index: u32,
    pub issuance_amount: i64,
    pub number_of_units: i64,
    pub value_per_unit: i64,
    pub name: String,
    pub description: String,
    pub url: String,
    pub issuing_entity: String,
    pub owning_entity: String,
    pub possessing_entity: String,
}
