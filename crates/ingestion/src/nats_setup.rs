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
            subjects: vec!["Q.*.QONDUIT.TICK"],
        },
        StreamDef {
            name: "QONDUIT_TX",
            subjects: vec!["Q.*.QONDUIT.TX"],
        },
        StreamDef {
            name: "QONDUIT_ENTITY",
            subjects: vec!["Q.*.QONDUIT.ENTITY"],
        },
        StreamDef {
            name: "QONDUIT_COMPUTORS",
            subjects: vec!["Q.*.QONDUIT.COMPUTORS"],
        },
        StreamDef {
            name: "QONDUIT_CUSTMSG",
            subjects: vec!["Q.*.QONDUIT.CUSTMSG"],
        },
        StreamDef {
            name: "QONDUIT_ORACLE",
            subjects: vec!["Q.*.QONDUIT.ORACLE"],
        },
        StreamDef {
            name: "QONDUIT_ASSET",
            subjects: vec!["Q.*.QONDUIT.ASSET"],
        },
        StreamDef {
            name: "QONDUIT_CONTRACT",
            subjects: vec!["Q.*.QONDUIT.CONTRACT"],
        },
        StreamDef {
            name: "QONDUIT_TICKVOTE",
            subjects: vec!["Q.*.QONDUIT.TICKVOTE"],
        },
        StreamDef {
            name: "QONDUIT_CFNR",
            subjects: vec!["Q.*.QONDUIT.CFNR"],
        },
        StreamDef {
            name: "QONDUIT_QUORUM",
            subjects: vec!["Q.*.QONDUIT.QUORUM"],
        },
        StreamDef {
            name: "QONDUIT_LOG",
            subjects: vec!["Q.*.QONDUIT.LOG"],
        },
        StreamDef {
            name: "QONDUIT_LOGDIGEST",
            subjects: vec!["Q.*.QONDUIT.LOGDIGEST"],
        },
        StreamDef {
            name: "QONDUIT_MINING",
            subjects: vec!["Q.*.QONDUIT.MINING"],
        },
        StreamDef {
            name: "QONDUIT_SPECTRUM",
            subjects: vec!["Q.*.QONDUIT.SPECTRUM"],
        },
    ]
}

/// Ensure all required JetStream streams exist, creating any that are missing.
///
/// If a stream exists but has the wrong subject patterns (e.g., `QONDUIT.TICK`
/// instead of `Q.*.QONDUIT.TICK`), it will be deleted and recreated.
pub async fn ensure_streams(nats: &Client) -> Result<()> {
    let js = jetstream::new(nats.clone());

    for def in stream_definitions() {
        let subjects: Vec<String> = def.subjects.iter().map(|s| s.to_string()).collect();

        let config = jetstream::stream::Config {
            name: def.name.to_string(),
            subjects: subjects.clone(),
            max_bytes: MAX_BYTES,
            max_age: MAX_AGE,
            storage: jetstream::stream::StorageType::File,
            num_replicas: 1,
            discard: jetstream::stream::DiscardPolicy::Old,
            retention: jetstream::stream::RetentionPolicy::Limits,
            ..Default::default()
        };

        // Try to get existing stream to check if subjects match
        if let Ok(mut stream) = js.get_stream(def.name).await {
            if let Ok(info) = stream.info().await {
                let existing_subjects: Vec<String> = info
                    .config
                    .subjects
                    .iter()
                    .map(|s| s.to_string())
                    .collect();

                if existing_subjects != subjects {
                    info!(
                        "Stream {} has outdated subjects {:?}, recreating with {:?}",
                        def.name, existing_subjects, subjects
                    );
                    // Delete and recreate with correct subjects
                    if let Err(e) = js.delete_stream(def.name).await {
                        warn!("Failed to delete stream {}: {e}", def.name);
                        continue;
                    }
                    match js.create_stream(config).await {
                        Ok(_) => info!("JetStream stream {} recreated", def.name),
                        Err(e) => warn!("Failed to recreate stream {}: {e}", def.name),
                    }
                }
            }
            // else: couldn't get info, skip migration check
        } else {
            // Stream doesn't exist, create it
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
    }

    info!("All JetStream streams ensured");
    Ok(())
}
