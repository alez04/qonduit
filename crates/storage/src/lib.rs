//! Storage engine: hot tier (RAM cache), warm tier (RocksDB), cold tier (Parquet).
//!
//! The hot tier provides in-memory caching for frequently accessed data.
//! The warm tier stores the canonical index using RocksDB with column families
//! for different entity types.

pub mod hot;
pub mod warm;

pub use hot::HotCache;
pub use warm::WarmStorage;
