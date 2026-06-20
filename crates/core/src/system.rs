//! System resource detection for auto-tuning pipeline parameters.
//!
//! Reads CPU cores, available memory, and computes optimal settings for:
//! - RocksDB write buffer sizes
//! - NATS consumer batch sizes
//! - Concurrent message processing parallelism

/// Detected system resources and computed optimal parameters.
#[derive(Debug, Clone)]
pub struct SystemResources {
    /// Logical CPU cores.
    pub cpu_cores: usize,
    /// Available memory in MB.
    pub memory_mb: u64,
    /// Optimal RocksDB write buffer size per memtable (bytes).
    pub rocksdb_write_buffer_size: usize,
    /// Optimal number of RocksDB memtables.
    pub rocksdb_max_write_buffers: i32,
    /// Optimal NATS consumer batch size (messages per fetch).
    pub batch_size: usize,
    /// Optimal concurrent message handlers per consumer stream.
    pub concurrency: usize,
}

impl SystemResources {
    /// Detect system resources and compute optimal parameters.
    pub fn detect() -> Self {
        let cpu_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(2);
        let memory_mb = available_memory_mb();

        // RocksDB tuning: allocate ~25% of available memory to write buffers
        // across all column families, with a floor of 64MB and cap of 512MB per buffer.
        let total_write_budget = (memory_mb * 1024 * 1024) / 4; // 25% of RAM
        let num_cfs = 15; // approximate number of column families
        let per_cf_budget = total_write_budget / num_cfs as u64;
        let write_buffer_size = (per_cf_budget as usize)
            .clamp(64 * 1024 * 1024, 512 * 1024 * 1024);

        // More memtables = more write parallelism, but more memory.
        // 2-6 based on available memory.
        let max_write_buffers = if memory_mb > 16_384 {
            6
        } else if memory_mb > 8_192 {
            4
        } else if memory_mb > 4_096 {
            3
        } else {
            2
        };

        // Batch size: scale with memory and pending backlog.
        // During catch-up (high pending), we want large batches.
        let batch_size = if memory_mb > 8_192 {
            500
        } else if memory_mb > 4_096 {
            250
        } else if memory_mb > 2_048 {
            100
        } else {
            50
        };

        // Concurrency: process up to N messages in parallel per consumer stream.
        // More cores = more parallelism, but we need headroom for Tokio and RocksDB.
        let concurrency = if cpu_cores >= 8 {
            32
        } else if cpu_cores >= 4 {
            16
        } else if cpu_cores >= 2 {
            8
        } else {
            4
        };

        let resources = Self {
            cpu_cores,
            memory_mb,
            rocksdb_write_buffer_size: write_buffer_size,
            rocksdb_max_write_buffers: max_write_buffers,
            batch_size,
            concurrency,
        };

        tracing::info!(
            cpu_cores = resources.cpu_cores,
            memory_mb = resources.memory_mb,
            rocksdb_write_buffer_mb = resources.rocksdb_write_buffer_size / (1024 * 1024),
            rocksdb_max_write_buffers = resources.rocksdb_max_write_buffers,
            batch_size = resources.batch_size,
            concurrency = resources.concurrency,
            "System resources detected, auto-tuned pipeline parameters"
        );

        resources
    }

    /// Create resources with manual overrides (env vars take precedence).
    pub fn detect_with_overrides(
        override_batch_size: Option<usize>,
        override_concurrency: Option<usize>,
    ) -> Self {
        let mut res = Self::detect();
        if let Some(bs) = override_batch_size {
            res.batch_size = bs;
        }
        if let Some(c) = override_concurrency {
            res.concurrency = c;
        }
        res
    }
}

/// Read available memory from `/proc/meminfo` on Linux.
/// Returns megabytes. Falls back to 4096 if unable to read.
fn available_memory_mb() -> u64 {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("MemAvailable:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse().ok())
                .map(|kb: u64| kb / 1024)
        })
        .unwrap_or(4096)
}
