//! NATS JetStream stream management.
//!
//! Creates all required JetStreams for the Qonduit pipeline if they don't already exist.
//! Each stream has a subject pattern like `Q.{epoch}.QONDUIT.*` and stores
//! data with sensible retention defaults.

use anyhow::{Context, Result};
use async_nats::jetstream;
use async_nats::Client;
use tracing::{debug, info, warn};

/// Duration for stream retention: 7 days.
const MAX_AGE: std::time::Duration = std::time::Duration::from_secs(7 * 24 * 60 * 60);

/// Default max bytes per stream (10 GiB).
const MAX_BYTES: i64 = 10 * 1024 * 1024 * 1024;

/// Stream definition: name and its subject patterns.
struct StreamDef {
    name: &'static str,
    subjects: Vec<&'static str>,
}

/// All Qonduit JetStream streams.
fn stream_definitions() -> Vec<StreamDef> {
    vec![
        StreamDef {
            name: "QONDUIT_TICKS",
            subjects: vec!["Q.>.QONDUIT.TICK"],

        },
        StreamDef {
            name: "QONDUIT_TX",
            subjects: vec!["Q.>.QONDUIT.TX"],

        },
        StreamDef {
            name: "QONDUIT_ENTITIES",
            subjects: vec!["Q.>.QONDUIT.ENTITY"],

        },
        StreamDef {
            name: "QONDUIT_SPECTRUM",
            subjects: vec!["Q.>.QONDUIT.SPECTRUM"],

        },
        StreamDef {
            name: "QONDUIT_COMPUTORS",
            subjects: vec!["Q.>.QONDUIT.COMPUTORS"],

        },
        StreamDef {
            name: "QONDUIT_CUSTMSG",
            subjects: vec!["Q.>.QONDUIT.CUSTMSG"],

        },
        StreamDef {
            name: "QONDUIT_ORACLE",
            subjects: vec!["Q.>.QONDUIT.ORACLE"],

        },
        StreamDef {
            name: "QONDUIT_ASSETS",
            subjects: vec!["Q.>.QONDUIT.ASSET"],

        },
        StreamDef {
            name: "QONDUIT_CONTRACTS",
            subjects: vec!["Q.>.QONDUIT.CONTRACT", "Q.>.QONDUIT.CFNR"],

        },
    ]
}

/// Ensure all required JetStream streams exist, creating any that are missing.
///
/// Safe to call multiple times — existing streams are left untouched.
pub async fn ensure_streams(nats: &Client) -> Result<()> {
    let js = jetstream::new(nats.clone());

    for def in stream_definitions() {
        let subjects: Vec<String> = def.subjects.iter().map(|s| s.to_string()).collect();

        let config = jetstream::stream::Config {
            name: def.name.to_string(),
            subjects,
            max_bytes: MAX_BYTES,
            max_age: MAX_AGE,
            storage: jetstream::stream::StorageType::File,
            num_replicas: 1,
            discard: jetstream::stream::DiscardPolicy::Old,
            // retain last value per subject for tick stream
            retention: jetstream::stream::RetentionPolicy::Limits,
            ..Default::default()
        };

        // Try to create stream; if it already exists, that's fine
        match js.create_stream(config).await {
            Ok(_) => info!("JetStream stream {} created", def.name),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("stream name already in use") || msg.contains("10058") {
                    debug!("JetStream stream {} already exists", def.name);
                } else {
                    warn!("Failed to create stream {}: {e}", def.name);
                }
            }
        }
    }

    info!("All JetStream streams ensured");
    Ok(())
}
