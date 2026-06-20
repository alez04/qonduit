//! NATS JetStream stream management.
//!
//! Creates all required JetStreams for the Qonduit pipeline if they don't already exist.
//! Each stream captures messages from the corresponding `QONDUIT.*` subject.

use anyhow::Result;
use async_nats::jetstream;
use async_nats::Client;
use tracing::{debug, info, warn};

/// Duration for stream retention: 30 days (messages are critical for catch-up).
/// The processor needs historical data to catch up from cold start.
const MAX_AGE: std::time::Duration = std::time::Duration::from_secs(30 * 24 * 60 * 60);

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
            name: "QONDUIT_TICK",
            subjects: vec!["QONDUIT.TICK"],
        },
        StreamDef {
            name: "QONDUIT_TX",
            subjects: vec!["QONDUIT.TX"],
        },
        StreamDef {
            name: "QONDUIT_ENTITY",
            subjects: vec!["QONDUIT.ENTITY"],
        },
        StreamDef {
            name: "QONDUIT_COMPUTORS",
            subjects: vec!["QONDUIT.COMPUTORS"],
        },
        StreamDef {
            name: "QONDUIT_CUSTMSG",
            subjects: vec!["QONDUIT.CUSTMSG"],
        },
        StreamDef {
            name: "QONDUIT_ORACLE",
            subjects: vec!["QONDUIT.ORACLE"],
        },
        StreamDef {
            name: "QONDUIT_ASSET",
            subjects: vec!["QONDUIT.ASSET"],
        },
        StreamDef {
            name: "QONDUIT_CONTRACT",
            subjects: vec!["QONDUIT.CONTRACT"],
        },
        StreamDef {
            name: "QONDUIT_TICKVOTE",
            subjects: vec!["QONDUIT.TICKVOTE"],
        },
        StreamDef {
            name: "QONDUIT_CFNR",
            subjects: vec!["QONDUIT.CFNR"],
        },
        StreamDef {
            name: "QONDUIT_QUORUM",
            subjects: vec!["QONDUIT.QUORUM"],
        },
        StreamDef {
            name: "QONDUIT_LOG",
            subjects: vec!["QONDUIT.LOG"],
        },
        StreamDef {
            name: "QONDUIT_LOGDIGEST",
            subjects: vec!["QONDUIT.LOGDIGEST"],
        },
        StreamDef {
            name: "QONDUIT_MINING",
            subjects: vec!["QONDUIT.MINING"],
        },
        StreamDef {
            name: "QONDUIT_SPECTRUM",
            subjects: vec!["QONDUIT.SPECTRUM"],
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
