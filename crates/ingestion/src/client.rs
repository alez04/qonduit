//! TCP client: connects to Qubic nodes via PeerManager and reads packets.
//!
//! Implements multi-peer failover: the client picks the best peer from the
//! PeerManager, attempts connection, and on failure marks the peer down and
//! tries the next one. Successful connections are recorded so the manager
//! can make better selections over time.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use async_nats::Client as NatsClient;
use qonduit_core::PipelineState;
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};

use crate::decoder::PacketDecoder;
use crate::metrics::{self, PACKETS_DECODE_ERRORS, PEER_COUNT};
use crate::peer_manager::PeerManager;
use crate::protocol;

/// Maximum number of peers to try per reconnection cycle before giving up
/// and waiting for the reconnect delay.
const MAX_PEERS_PER_ATTEMPT: usize = 8;

/// How often to send CurrentTickInfo requests (in seconds).
const TICK_REQUEST_INTERVAL: Duration = Duration::from_secs(2);

/// How often to send entity requests (in seconds).
const ENTITY_REQUEST_INTERVAL: Duration = Duration::from_secs(2);

/// Max pending entity identities to avoid unbounded growth.
const MAX_PENDING_ENTITIES: usize = 500;

/// Max entity requests per batch.
const ENTITY_BATCH_SIZE: usize = 10;

/// Configuration for the ingestion client.
#[derive(Debug, Clone)]
pub struct IngestionConfig {
    /// Optional explicit node address. If provided, it is added to the
    /// PeerManager as a bootstrap peer.
    pub node_addr: Option<SocketAddr>,
    /// Additional bootstrap addresses for peer discovery.
    pub bootstrap_addrs: Vec<SocketAddr>,
    /// Timeout for TCP operations.
    pub tcp_timeout: Duration,
    /// Reconnect delay on connection loss (or when all peers fail).
    pub reconnect_delay: Duration,
}

impl IngestionConfig {
    /// Create a config targeting a single node address (legacy behaviour).
    pub fn single(node_addr: SocketAddr) -> Self {
        Self {
            node_addr: Some(node_addr),
            bootstrap_addrs: Vec::new(),
            tcp_timeout: Duration::from_secs(30),
            reconnect_delay: Duration::from_secs(5),
        }
    }

    /// Create a config with only bootstrap peers (no explicit node).
    pub fn with_bootstrap(addrs: Vec<SocketAddr>) -> Self {
        Self {
            node_addr: None,
            bootstrap_addrs: addrs,
            tcp_timeout: Duration::from_secs(30),
            reconnect_delay: Duration::from_secs(5),
        }
    }
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self::single("127.0.0.1:21841".parse().expect("valid default addr"))
    }
}

/// Handle to a running ingestion client, exposing live metrics.
#[derive(Debug, Clone)]
pub struct IngestionHandle {
    peer_manager: Arc<PeerManager>,
}

impl IngestionHandle {
    /// Number of known peers (healthy + unhealthy).
    pub async fn peer_count(&self) -> usize {
        self.peer_manager.peer_count().await
    }

    /// Return a reference to the underlying PeerManager.
    pub fn peer_manager(&self) -> &Arc<PeerManager> {
        &self.peer_manager
    }
}

/// Ingestion client that reads from Qubic nodes and publishes to NATS.
///
/// Uses a `PeerManager` for peer discovery and health tracking. On each
/// reconnection cycle the client tries up to `MAX_PEERS_PER_ATTEMPT` peers,
/// marking successes and failures as it goes.
pub struct IngestionClient {
    config: IngestionConfig,
    decoder: PacketDecoder,
    peer_manager: Arc<PeerManager>,
    pipeline: Arc<PipelineState>,
    current_epoch: u16,
    current_tick: u32,
    /// Entity identities extracted from transactions, pending request.
    pending_entities: Arc<Mutex<HashSet<[u8; 32]>>>,
}

impl IngestionClient {
    /// Create a new client from config, NATS connection, and shared pipeline state.
    pub fn new(config: IngestionConfig, nats: NatsClient, pipeline: Arc<PipelineState>) -> Self {
        let mut bootstrap: Vec<SocketAddr> = config.bootstrap_addrs.clone();
        if let Some(addr) = config.node_addr {
            if !bootstrap.contains(&addr) {
                bootstrap.push(addr);
            }
        }

        let peer_manager = Arc::new(PeerManager::new(&bootstrap));

        Self {
            config,
            decoder: PacketDecoder::new(nats),
            peer_manager,
            pipeline,
            current_epoch: 0,
            current_tick: 0,
            pending_entities: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Return a handle for querying live metrics.
    pub fn handle(&self) -> IngestionHandle {
        IngestionHandle {
            peer_manager: Arc::clone(&self.peer_manager),
        }
    }

    /// The current epoch reported by the node.
    pub fn current_epoch(&self) -> u16 {
        self.current_epoch
    }

    /// The current tick reported by the node.
    pub fn current_tick(&self) -> u32 {
        self.current_tick
    }

    /// Run the ingestion loop. Reconnects on failure.
    pub async fn run(&mut self) -> Result<()> {
        // Step 1: Attempt to bootstrap from the Qubic discovery API.
        match self.peer_manager.bootstrap_from_api().await {
            Ok(()) => {
                info!(
                    "Bootstrap from API succeeded, {} peers known",
                    self.peer_manager.peer_count().await
                );
            }
            Err(e) => {
                warn!("Bootstrap from API failed (non-fatal): {e:#}");
            }
        }

        // Step 2: Main reconnection loop.
        loop {
            match self.connect_and_read().await {
                Ok(()) => {
                    warn!("Connection closed cleanly, reconnecting...");
                }
                Err(e) => {
                    error!("Ingestion cycle error: {e:#}");
                }
            }

            // Mark as disconnected when the connection drops.
            self.pipeline
                .ingestion_connected
                .store(false, std::sync::atomic::Ordering::Relaxed);

            // Periodically prune stale peers.
            self.peer_manager.prune_stale().await;

            info!(
                "Reconnecting in {}s... ({} peers known)",
                self.config.reconnect_delay.as_secs(),
                self.peer_manager.peer_count().await,
            );
            tokio::time::sleep(self.config.reconnect_delay).await;
        }
    }

    /// Try to connect to peers, perform handshake, and enter the read loop.
    async fn connect_and_read(&mut self) -> Result<()> {
        let mut attempts = 0usize;

        while attempts < MAX_PEERS_PER_ATTEMPT {
            let addr = match self.peer_manager.best_peer().await {
                Some(a) => a,
                None => {
                    if attempts == 0 {
                        anyhow::bail!("No peers available for connection");
                    }
                    warn!("No more peers to try after {attempts} attempts");
                    anyhow::bail!("Exhausted all peer candidates ({attempts} attempted)");
                }
            };

            attempts += 1;
            info!("Connecting to {addr} (attempt {attempts}/{MAX_PEERS_PER_ATTEMPT})...");

            // TCP connect.
            let mut stream = match tokio::time::timeout(
                self.config.tcp_timeout,
                TcpStream::connect(addr),
            )
            .await
            {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => {
                    warn!("TCP connect to {addr} failed: {e:#}");
                    self.peer_manager.mark_failure(&addr).await;
                    continue;
                }
                Err(_) => {
                    warn!("TCP connect to {addr} timed out");
                    self.peer_manager.mark_failure(&addr).await;
                    continue;
                }
            };

            // Enable TCP_NODELAY for low latency.
            let _ = stream.set_nodelay(true);

            // Peer exchange handshake — fire-and-forget (no response expected).
            let local_peers: [[u8; 4]; 4] = [[0, 0, 0, 0]; 4];
            if let Err(e) = protocol::exchange_public_peers(&mut stream, &local_peers).await {
                warn!("Peer exchange with {addr} failed: {e:#}");
                self.peer_manager.mark_failure(&addr).await;
                continue;
            }

            // Mark the peer as healthy immediately after successful handshake.
            self.peer_manager.mark_success(&addr).await;
            info!("Connected and authenticated with {addr}");
            self.pipeline.ingestion_connected.store(true, std::sync::atomic::Ordering::Relaxed);
            PEER_COUNT.set(self.peer_manager.peer_count().await as i64);

            // Request current tick info to bootstrap epoch/tick state.
            match protocol::request_current_tick_info(&mut stream).await {
                Ok(data) if data.len() >= 8 => {
                    // CurrentTickInfo layout (after header):
                    // [0..2]  tickDuration (u16)
                    // [2..4]  epoch (u16)
                    // [4..8]  tick (u32)
                    let _tick_duration = u16::from_le_bytes([data[0], data[1]]);
                    let epoch = u16::from_le_bytes([data[2], data[3]]);
                    let tick = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
                    info!("Current state: epoch={epoch}, tick={tick}");
                    self.current_epoch = epoch;
                    self.current_tick = tick;
                    self.pipeline.node_tick.store(tick, std::sync::atomic::Ordering::Relaxed);
                    self.pipeline.node_epoch.store(epoch, std::sync::atomic::Ordering::Relaxed);
                    metrics::CURRENT_EPOCH.set(epoch as i64);
                    metrics::CURRENT_TICK.set(tick as i64);
                }
                Ok(data) => {
                    warn!("CurrentTickInfo response too short: {} bytes", data.len());
                }
                Err(e) => {
                    warn!("Failed to request current tick info: {e:#}");
                    // Don't bail — the node might still broadcast data.
                    // We'll keep trying in the read loop.
                }
            }

            // Enter the main read loop — runs until the connection drops.
            return self.read_loop(&mut stream, addr).await;
        }

        anyhow::bail!("Failed to connect after {attempts} peer attempts")
    }

    /// Read packets from the stream and publish them to NATS.
    ///
    /// Periodically sends CurrentTickInfo requests. Handles type 28 responses
    /// inline when they arrive from the read loop. Extracts entity identities
    /// from transactions and periodically requests entity data from the node.
    async fn read_loop(
        &mut self,
        stream: &mut TcpStream,
        addr: SocketAddr,
    ) -> Result<()> {
        let mut tick_request_interval = tokio::time::interval(TICK_REQUEST_INTERVAL);
        tick_request_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut entity_request_interval = tokio::time::interval(ENTITY_REQUEST_INTERVAL);
        entity_request_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut stats_interval = tokio::time::interval(std::time::Duration::from_secs(30));
        stats_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut packets_since_last_stats: u64 = 0;
        let mut published_since_last_stats: u64 = 0;

        loop {
            tokio::select! {
                // Read incoming packets
                result = protocol::read_packet(stream) => {
                    match result {
                        Ok((msg_type, dejavu, payload)) => {
                            // Handle CurrentTickInfo responses inline
                            if msg_type == 28 && payload.len() >= 8 {
                                let _tick_duration = u16::from_le_bytes([payload[0], payload[1]]);
                                let epoch = u16::from_le_bytes([payload[2], payload[3]]);
                                let tick = u32::from_le_bytes([payload[4], payload[5], payload[6], payload[7]]);
                                if epoch != self.current_epoch || tick != self.current_tick {
                                    info!("Tick updated: epoch={epoch}, tick={tick}");
                                    self.current_epoch = epoch;
                                    self.current_tick = tick;
                                    self.pipeline.node_tick.store(tick, std::sync::atomic::Ordering::Relaxed);
                                    self.pipeline.node_epoch.store(epoch, std::sync::atomic::Ordering::Relaxed);
                                    metrics::CURRENT_EPOCH.set(epoch as i64);
                                    metrics::CURRENT_TICK.set(tick as i64);
                                }
                                packets_since_last_stats += 1;
                                continue;
                            }

                            // Handle RespondEntity (type 32) inline — publish to NATS
                            if msg_type == 32 {
                                packets_since_last_stats += 1;
                                if let Err(e) = self.decoder.decode_and_publish(msg_type, dejavu, &payload, self.current_epoch).await {
                                    warn!("Entity decode/publish error: {e:#}");
                                }
                                continue;
                            }

                            // For transactions (type 24), extract source/destination identities
                            // Wire layout: [0..32] source, [32..64] destination
                            if msg_type == 24 && payload.len() >= 64 {
                                let mut source = [0u8; 32];
                                source.copy_from_slice(&payload[0..32]);
                                let mut destination = [0u8; 32];
                                destination.copy_from_slice(&payload[32..64]);

                                let mut pending = self.pending_entities.lock().unwrap();
                                if pending.len() < MAX_PENDING_ENTITIES {
                                    pending.insert(source);
                                    pending.insert(destination);
                                }
                            }

                            debug!(
                                "Packet: type={msg_type}, dejavu={dejavu}, payload_len={}",
                                payload.len()
                            );
                            packets_since_last_stats += 1;

                            if let Err(e) = self
                                .decoder
                                .decode_and_publish(msg_type, dejavu, &payload, self.current_epoch)
                                .await
                            {
                                warn!("Decode/publish error for type {msg_type}: {e:#}");
                                PACKETS_DECODE_ERRORS.inc();
                            }
                        }
                        Err(e) => {
                            warn!("Read loop error on {addr}: {e:#}");
                            return Err(e);
                        }
                    }
                }
                // Periodically request current tick info (fire-and-forget)
                _ = tick_request_interval.tick() => {
                    if let Err(e) = protocol::send_raw(stream, 27, &[], rand::random::<u32>().max(1)).await {
                        warn!("Failed to send tick info request: {e:#}");
                        return Err(e);
                    }
                }
                // Periodically request entities for pending identities
                _ = entity_request_interval.tick() => {
                    let batch: Vec<[u8; 32]> = {
                        let mut pending = self.pending_entities.lock().unwrap();
                        let take_n = pending.iter().take(ENTITY_BATCH_SIZE).cloned().collect::<Vec<_>>();
                        for id in &take_n {
                            pending.remove(id);
                        }
                        take_n
                    };

                    for identity in &batch {
                        match protocol::request_entity(stream, identity).await {
                            Ok(data) => {
                                // Publish the entity response to NATS
                                if let Err(e) = self.decoder.decode_and_publish(32, 0, &data, self.current_epoch).await {
                                    debug!("Entity publish error: {e:#}");
                                }
                            }
                            Err(e) => {
                                debug!("Entity request failed for identity: {e:#}");
                            }
                        }
                    }
                }
                // Periodic stats summary
                _ = stats_interval.tick() => {
                    info!(
                        "Ingestion stats: packets_rcvd={packets_since_last_stats}, published={published_since_last_stats}, epoch={}, tick={}",
                        self.current_epoch, self.current_tick
                    );
                    packets_since_last_stats = 0;
                    published_since_last_stats = 0;
                }
            }
        }
    }
}
