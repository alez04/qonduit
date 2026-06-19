//! Qonduit: high-performance Qubic blockchain indexer and RPC server.
//!
//! Main entry point. Reads config, initializes storage, connects to NATS,
//! and spawns the ingestion, processing, and query tasks.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use serde::Deserialize;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "qonduit", version, about = "Qubic blockchain indexer and RPC server")]
struct Cli {
    /// Path to config file.
    #[arg(short, long, default_value = "qonduit.toml")]
    config: PathBuf,
}

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

#[derive(Debug, Deserialize)]
struct IngestionConfig {
    #[serde(default = "default_node_addr")]
    node_addr: String,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            node_addr: default_node_addr(),
        }
    }
}

fn default_node_addr() -> String {
    "127.0.0.1:21841".to_string()
}

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
    if let Ok(nats_url) = std::env::var("QONDUIT_NATS_URL") {
        config.nats.url = nats_url;
    }
    if let Ok(listen) = std::env::var("QONDUIT_LISTEN_ADDR") {
        config.query.listen_addr = listen;
    }
    if let Ok(node) = std::env::var("QONDUIT_NODE_ADDR") {
        config.ingestion.node_addr = node;
    }
    if let Ok(data) = std::env::var("QONDUIT_DATA_DIR") {
        config.storage.data_dir = PathBuf::from(data);
    }

    info!("Qonduit starting...");
    info!("  NATS: {}", config.nats.url);
    info!("  Storage: {:?}", config.storage.data_dir);
    info!("  Query: {}", config.query.listen_addr);
    info!("  Ingestion: {}", config.ingestion.node_addr);

    // Initialize storage
    let storage = qonduit_storage::WarmStorage::open(&config.storage.data_dir)
        .context("Failed to open storage")?;

    // Connect to NATS
    let nats = async_nats::connect(&config.nats.url)
        .await
        .context("Failed to connect to NATS")?;

    info!("Connected to NATS");

    // Build shared app state
    let app_state = Arc::new(qonduit_query::AppState {
        storage,
        nats: nats.clone(),
    });

    // Spawn tasks
    let _query_handle = {
        let state = app_state.clone();
        let addr = config.query.listen_addr.parse().context("Invalid listen address")?;
        tokio::spawn(async move {
            if let Err(e) = qonduit_query::run(
                qonduit_query::QueryConfig { listen_addr: addr },
                state,
            )
            .await
            {
                tracing::error!("Query server error: {e:#}");
            }
        })
    };

    // TODO: Spawn ingestion and processor tasks

    info!("Qonduit is running");

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");

    Ok(())
}
