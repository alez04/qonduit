//! Storage engine: hot tier (RAM cache), warm tier (RocksDB), cold tier (Parquet).
//!
//! The warm tier stores the canonical index using RocksDB with column families
//! for different entity types.

pub mod warm;

pub use warm::WarmStorage;
