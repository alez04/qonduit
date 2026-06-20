//! Qonduit: high-performance Qubic blockchain indexer and RPC server.
//!
//! Main entry point. Reads config, initializes storage, connects to NATS,
//! and spawns the ingestion, processing, and query tasks with graceful shutdown.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use serde::Deserialize;
use tracing::info;
use tracing_subscriber::EnvFilter;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(name = "qonduit", version, about = "Qubic blockchain indexer and RPC server")]
struct Cli {
    /// Path to config file.
    #[arg(short, long, default_value = "qonduit.toml")]
    config: PathBuf,
}

// ---------------------------------------------------------------------------
// Config structs
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Default)]
struct Config {
    #[serde(default)]
    nats: NatsConfig,
    #[serde(default)]
    storage: StorageConfig,
    #[serde(default)]
    query: QueryConfig,
    #[serde(default)]
    ingestion: IngestionConfig,
    #[serde(default)]
    backfill: BackfillConfig,
    #[serde(default)]
    processor: ProcessorConfig,
}

#[derive(Debug, Deserialize)]
struct NatsConfig {
    #[serde(default = "default_nats_url")]
    url: String,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            url: default_nats_url(),
        }
    }
}

fn default_nats_url() -> String {
    "nats://localhost:4222".to_string()
}

#[derive(Debug, Deserialize)]
struct StorageConfig {
    #[serde(default = "default_data_dir")]
    data_dir: PathBuf,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
        }
    }
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("./data")
}

#[derive(Debug, Deserialize)]
struct QueryConfig {
    #[serde(default = "default_listen_addr")]
    listen_addr: String,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            listen_addr: default_listen_addr(),
        }
    }
}

fn default_listen_addr() -> String {
    "0.0.0.0:8080".to_string()
}

#[derive(Debug, Deserialize, Default)]
struct IngestionConfig {
    #[serde(default)]
    node_addr: Option<String>,
    #[serde(default)]
    bootstrap_addrs: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct BackfillConfig {
    /// Enable historical backfill.
    #[serde(default)]
    enabled: bool,
    /// Number of parallel TCP workers for backfill.
    #[serde(default = "default_backfill_workers")]
    workers: usize,
    /// Start tick for backfill (0 = start from epoch 1).
    #[serde(default)]
    start_tick: u32,
    /// End tick for backfill (0 = auto-detect from node).
    #[serde(default)]
    end_tick: u32,
    /// Delay between ticks per worker (ms). 0 = no delay.
    #[serde(default)]
    tick_delay_ms: u64,
}

impl Default for BackfillConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            workers: default_backfill_workers(),
            start_tick: 0,
            end_tick: 0,
            tick_delay_ms: 0,
        }
    }
}

fn default_backfill_workers() -> usize { 4 }

#[derive(Debug, Deserialize)]
struct ProcessorConfig {
    /// Replay all messages from stream start (catch-up mode).
    #[serde(default = "default_catch_up")]
    catch_up: bool,
    /// Number of messages per fetch batch (defaults to 10 for live, 100 for catch-up).
    #[serde(default)]
    batch_size: Option<usize>,
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self { catch_up: true, batch_size: None }
    }
}

fn default_catch_up() -> bool { true }


// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    // Load config
    let mut config: Config = if cli.config.exists() {
        let contents = std::fs::read_to_string(&cli.config)
            .with_context(|| format!("Failed to read config: {:?}", cli.config))?;
        toml::from_str(&contents).context("Failed to parse config")?
    } else {
        info!("No config file found, using defaults");
        Config::default()
    };

    // Env var overrides
    if let Ok(v) = std::env::var("QONDUIT_NATS_URL") {
        config.nats.url = v;
    }
    if let Ok(v) = std::env::var("QONDUIT_LISTEN_ADDR") {
        config.query.listen_addr = v;
    }
    if let Ok(v) = std::env::var("QONDUIT_NODE_ADDR") {
        config.ingestion.node_addr = Some(v);
    }
    if let Ok(v) = std::env::var("QONDUIT_BOOTSTRAP_ADDRS") {
        config.ingestion.bootstrap_addrs = v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
    }
    // Backfill env var overrides
    if let Ok(v) = std::env::var("QONDUIT_BACKFILL_ENABLED") {
        config.backfill.enabled = v == "true" || v == "1";
    }
    if let Ok(v) = std::env::var("QONDUIT_BACKFILL_WORKERS") {
        if let Ok(n) = v.parse::<usize>() {
            config.backfill.workers = n;
        }
    }
    if let Ok(v) = std::env::var("QONDUIT_BACKFILL_START_TICK") {
        if let Ok(n) = v.parse::<u32>() {
            config.backfill.start_tick = n;
        }
    }
    if let Ok(v) = std::env::var("QONDUIT_BACKFILL_END_TICK") {
        if let Ok(n) = v.parse::<u32>() {
            config.backfill.end_tick = n;
        }
    }
    if let Ok(v) = std::env::var("QONDUIT_BACKFILL_TICK_DELAY_MS") {
        if let Ok(n) = v.parse::<u64>() {
            config.backfill.tick_delay_ms = n;
        }
    }
    if let Ok(v) = std::env::var("QONDUIT_DATA_DIR") {
        config.storage.data_dir = PathBuf::from(v);
    }
    if let Ok(v) = std::env::var("QONDUIT_CATCH_UP") {
        config.processor.catch_up = v == "true" || v == "1";
    }
    if let Ok(v) = std::env::var("QONDUIT_BATCH_SIZE") {
        if let Ok(n) = v.parse::<usize>() {
            config.processor.batch_size = Some(n);
        }
    }

    info!("Qonduit v{} starting...", env!("CARGO_PKG_VERSION"));
    info!("  NATS: {}", config.nats.url);
    info!("  Storage: {:?}", config.storage.data_dir);
    info!("  Query: {}", config.query.listen_addr);
    let node_str = config.ingestion.node_addr.as_deref().unwrap_or("");
    info!("  Node: {}", if node_str.is_empty() { "(none - query-only)" } else { node_str });
    info!("  Bootstrap: {:?}", config.ingestion.bootstrap_addrs);
    info!("  Processor: catch_up={}, batch_size={:?}", config.processor.catch_up, config.processor.batch_size);
    if config.backfill.enabled {
        info!("  Backfill: enabled, workers={}, start_tick={}, end_tick={}, tick_delay={}ms",
            config.backfill.workers, config.backfill.start_tick, config.backfill.end_tick, config.backfill.tick_delay_ms);
    }

    // --- Phase 1: Storage (warm tier + hot cache) ---
    let warm_storage = qonduit_storage::WarmStorage::open(&config.storage.data_dir)
        .context("Failed to open warm storage")?;
    let _hot_cache = qonduit_storage::HotCache::new(1_000, 10_000);
    info!("Storage initialized (warm tier + hot cache)");

    // --- Phase 2: NATS ---
    let nats = async_nats::connect(&config.nats.url)
        .await
        .context("Failed to connect to NATS")?;
    info!("Connected to NATS");

    // Ensure JetStream streams exist
    if let Err(e) = qonduit_ingestion::nats_setup::ensure_streams(&nats).await {
        tracing::warn!("Failed to ensure NATS streams (may already exist): {e}");
    }

    // --- Phase 3: Build shared state ---
    let pipeline = Arc::new(qonduit_core::PipelineState::new());

    // Clone WarmStorage for AppState (RocksDB DB uses Arc internally, so clone is cheap)
    let app_state = Arc::new(qonduit_query::AppState {
        storage: warm_storage.clone(),
        nats: nats.clone(),
        pipeline: pipeline.clone(),
    });
    let warm_storage = Arc::new(warm_storage);

    // --- Phase 4: Spawn tasks with graceful ordered shutdown ---
    //
    // Shutdown order:
    //   1. Stop ingestion  — no new data enters the pipeline
    //   2. Wait for processor to drain — process remaining NATS messages
    //   3. Stop query server — last to go so clients can still read

    // Per-service shutdown signals (watch channels: send `true` to stop)
    let (ingestion_stop_tx, mut ingestion_stop_rx) = tokio::sync::watch::channel(false);
    let (processor_stop_tx, mut processor_stop_rx) = tokio::sync::watch::channel(false);
    let (query_stop_tx, mut query_stop_rx) = tokio::sync::watch::channel(false);
    let (backfill_stop_tx, mut backfill_stop_rx) = tokio::sync::watch::channel(false);

    // Query server
    let query_handle = {
        let state = app_state.clone();
        let addr: std::net::SocketAddr = config
            .query
            .listen_addr
            .parse()
            .context("Invalid listen address")?;
        tokio::spawn(async move {
            tokio::select! {
                result = qonduit_query::run(
                    qonduit_query::QueryConfig { listen_addr: addr },
                    state,
                ) => {
                    if let Err(e) = result {
                        tracing::error!("Query server error: {e:#}");
                    }
                }
                _ = query_stop_rx.changed() => {
                    info!("Query server shutting down");
                }
            }
        })
    };

    // Processor
    let processor_handle = {
        let storage = warm_storage.clone();
        let nats = nats.clone();
        let pipeline = pipeline.clone();
        let processor_config = qonduit_processor::ProcessorConfig {
            consumer_group: "qonduit-processors".to_string(),
            catch_up: config.processor.catch_up,
            batch_size: config.processor.batch_size.unwrap_or(
                if config.processor.catch_up { 100 } else { 10 },
            ),
        };
        tokio::spawn(async move {
            tokio::select! {
                result = qonduit_processor::run(
                    processor_config,
                    nats,
                    storage,
                    pipeline,
                ) => {
                    if let Err(e) = result {
                        tracing::error!("Processor error: {e:#}");
                    }
                }
                _ = processor_stop_rx.changed() => {
                    info!("Processor shutting down");
                }
            }
        })
    };

    // Ingestion client — always runs (peer discovery finds nodes automatically).
    // Set QONDUIT_NODE_ADDR=disabled to skip ingestion (query-only mode).
    let ingestion_disabled = config.ingestion.node_addr.as_deref() == Some("disabled");
    pipeline.ingestion_disabled.store(ingestion_disabled, std::sync::atomic::Ordering::Relaxed);
    let ingestion_handle = if ingestion_disabled {
        info!("Ingestion disabled via config, running in query-only mode");
        None
    } else {
        // Build ingestion config
        let mut ingestion_config = qonduit_ingestion::client::IngestionConfig {
            node_addr: None,
            bootstrap_addrs: Vec::new(),
            tcp_timeout: Duration::from_secs(30),
            reconnect_delay: Duration::from_secs(5),
        };

        if let Some(ref node_str) = config.ingestion.node_addr {
            match node_str.parse::<std::net::SocketAddr>() {
                Ok(addr) => ingestion_config.node_addr = Some(addr),
                Err(e) => {
                    tracing::warn!("Invalid node address '{node_str}', ignoring: {e}");
                }
            }
        }

        for bs in &config.ingestion.bootstrap_addrs {
            match bs.parse::<std::net::SocketAddr>() {
                Ok(addr) => ingestion_config.bootstrap_addrs.push(addr),
                Err(e) => {
                    tracing::warn!("Invalid bootstrap address '{bs}', ignoring: {e}");
                }
            }
        }

        let nats = nats.clone();
        let pipeline = pipeline.clone();
        Some(tokio::spawn(async move {
            let mut client = qonduit_ingestion::IngestionClient::new(
                ingestion_config,
                nats,
                pipeline,
            );
            tokio::select! {
                result = client.run() => {
                    if let Err(e) = result {
                        tracing::error!("Ingestion error: {e:#}");
                    }
                }
                _ = ingestion_stop_rx.changed() => {
                    info!("Ingestion shutting down");
                }
            }
        }))
    };

    // Backfill client — runs in background when enabled, after main ingestion starts.
    let backfill_handle = if config.backfill.enabled {
        let backfill_config = qonduit_ingestion::backfill::BackfillConfig {
            node_addr: config.ingestion.node_addr.as_deref()
                .and_then(|s| s.parse().ok()),
            bootstrap_addrs: config.ingestion.bootstrap_addrs.iter()
                .filter_map(|s| s.parse().ok())
                .collect(),
            workers: config.backfill.workers,
            start_tick: config.backfill.start_tick,
            end_tick: config.backfill.end_tick,
            tick_delay: Duration::from_millis(config.backfill.tick_delay_ms),
            tcp_timeout: Duration::from_secs(30),
        };
        let nats = nats.clone();
        let pipeline = pipeline.clone();
        Some(tokio::spawn(async move {
            let mut client = qonduit_ingestion::BackfillClient::new(
                backfill_config,
                nats,
                pipeline,
            );
            tokio::select! {
                result = client.run() => {
                    if let Err(e) = result {
                        tracing::error!("Backfill error: {e:#}");
                    }
                }
                _ = backfill_stop_rx.changed() => {
                    info!("Backfill shutting down");
                }
            }
        }))
    } else {
        None
    };

    info!("Qonduit is running — all services started");

    // --- Wait for shutdown signal ---
    tokio::signal::ctrl_c().await?;
    info!("Shutdown signal received, stopping services in order...");

    // Phase 1: Stop ingestion — no new data enters the pipeline
    let _ = ingestion_stop_tx.send(true);
    if let Some(handle) = ingestion_handle {
        match tokio::time::timeout(Duration::from_secs(10), handle).await {
            Ok(Ok(())) => info!("Ingestion stopped gracefully"),
            Ok(Err(e)) => tracing::warn!("Ingestion task panicked: {e}"),
            Err(_) => info!("Ingestion did not stop within timeout"),
        }
    }

    // Stop backfill (if running)
    let _ = backfill_stop_tx.send(true);
    if let Some(handle) = backfill_handle {
        match tokio::time::timeout(Duration::from_secs(10), handle).await {
            Ok(Ok(())) => info!("Backfill stopped gracefully"),
            Ok(Err(e)) => tracing::warn!("Backfill task panicked: {e}"),
            Err(_) => info!("Backfill did not stop within timeout"),
        }
    }

    // Phase 2: Wait for processor to drain remaining NATS messages
    let _ = processor_stop_tx.send(true);
    match tokio::time::timeout(Duration::from_secs(30), processor_handle).await {
        Ok(Ok(())) => info!("Processor stopped gracefully"),
        Ok(Err(e)) => tracing::warn!("Processor task panicked: {e}"),
        Err(_) => info!("Processor did not stop within timeout"),
    }

    // Phase 3: Stop query server — last so clients can still read during drain
    let _ = query_stop_tx.send(true);
    match tokio::time::timeout(Duration::from_secs(5), query_handle).await {
        Ok(Ok(())) => info!("Query server stopped gracefully"),
        Ok(Err(e)) => tracing::warn!("Query server task panicked: {e}"),
        Err(_) => info!("Query server did not stop within timeout"),
    }

    info!("Qonduit stopped");
    Ok(())
}
