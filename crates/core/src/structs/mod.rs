// Core protocol structs from `packet_reader.h`, `structures.h`, etc.
// Each struct uses `#[repr(C)]` + zerocopy for zero-copy TCP packet parsing.

pub mod tick;
pub mod transaction;
pub mod computors;
pub mod entity;
pub mod spectrum;
pub mod asset;
pub mod contract;
pub mod system_info;

pub use tick::*;
pub use transaction::*;
pub use computors::*;
pub use entity::*;
pub use spectrum::*;
pub use asset::*;
pub use contract::*;
pub use system_info::*;
