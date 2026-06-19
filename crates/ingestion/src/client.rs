//! TCP client: connects to Qubic nodes via PeerManager and reads packets.
//!
//! Implements multi-peer failover: the client picks the best peer from the
//! PeerManager, attempts connection, and on failure marks the peer down and
//! tries the next one. Successful connections are recorded so the manager
//! can make better selections over time.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_nats::Client as NatsClient;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};

use qonduit_core::RequestResponseHeader;

use crate::decoder::PacketDecoder;
use crate::peer_manager::PeerManager;
use crate::protocol;

/// Maximum number of peers to try per reconnection cycle before giving up
/// and waiting for the reconnect delay.
const MAX_PEERS_PER_ATTEMPT: usize = 8;

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
    nats: NatsClient,
    decoder: PacketDecoder,
    peer_manager: Arc<PeerManager>,
    current_epoch: u16,
    current_tick: u32,
}

impl IngestionClient {
    /// Create a new client from config and NATS connection.
    ///
    /// Builds the `PeerManager` from the config's bootstrap addresses
    /// and optional explicit `node_addr`.
    pub fn new(config: IngestionConfig, nats: NatsClient) -> Self {
        // Collect bootstrap addresses: explicit node + any additional bootstraps.
        let mut bootstrap: Vec<SocketAddr> = config.bootstrap_addrs.clone();
        if let Some(addr) = config.node_addr {
            if !bootstrap.contains(&addr) {
                bootstrap.push(addr);
            }
        }

        let peer_manager = Arc::new(PeerManager::new(&bootstrap));

        Self {
            config,
            nats,
            decoder: PacketDecoder::new(),
            peer_manager,
            current_epoch: 0,
            current_tick: 0,
        }
    }

    /// Create a client with an externally-provided PeerManager.
    pub fn with_peer_manager(
        config: IngestionConfig,
        nats: NatsClient,
        peer_manager: Arc<PeerManager>,
    ) -> Self {
        Self {
            config,
            nats,
            decoder: PacketDecoder::new(),
            peer_manager,
            current_epoch: 0,
            current_tick: 0,
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
        // This is non-fatal -- if it fails we proceed with static peers.
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
    ///
    /// Iterates through up to `MAX_PEERS_PER_ATTEMPT` peers before returning
    /// an error to let the outer loop apply the reconnect delay.
    async fn connect_and_read(&mut self) -> Result<()> {
        let mut attempts = 0usize;

        while attempts < MAX_PEERS_PER_ATTEMPT {
            // Pick the best peer from the manager.
            let addr = match self.peer_manager.best_peer().await {
                Some(a) => a,
                None => {
                    if attempts == 0 {
                        anyhow::bail!("No peers available for connection");
                    }
                    warn!("No more peers to try after {attempts} attempts");
                    anyhow::bail!(
                        "Exhausted all peer candidates ({attempts} attempted)"
                    );
                }
            };

            attempts += 1;
            info!(
                "Connecting to {addr} (attempt {attempts}/{MAX_PEERS_PER_ATTEMPT})..."
            );

            // Attempt TCP connection.
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

            // Peer exchange handshake.
            let local_peers: [[u8; 4]; 4] = [[127, 0, 0, 1]; 4];
            match protocol::exchange_public_peers(&mut stream, &local_peers).await {
                Ok(peers) => {
                    info!(
                        "Peer exchange complete with {addr}, received {} peer entries",
                        peers.len()
                    );
                    // Feed discovered peers back into the manager.
                    self.peer_manager
                        .add_peers_from_exchange(&peers)
                        .await;
                }
                Err(e) => {
                    warn!("Peer exchange with {addr} failed: {e:#}");
                    self.peer_manager.mark_failure(&addr).await;
                    continue;
                }
            }

            // Mark the peer as healthy.
            self.peer_manager.mark_success(&addr).await;
            info!("Connected and authenticated with {addr}");

            // Request current tick info to bootstrap epoch/tick state.
            match protocol::request_current_tick_info(&mut stream).await {
                Ok(data) if data.len() >= 6 => {
                    let epoch = u16::from_le_bytes([data[0], data[1]]);
                    let tick =
                        u32::from_le_bytes([data[2], data[3], data[4], data[5]]);
                    info!("Current state: epoch={epoch}, tick={tick}");
                    self.current_epoch = epoch;
                    self.current_tick = tick;
                }
                Ok(data) => {
                    warn!(
                        "CurrentTickInfo response too short: {} bytes",
                        data.len()
                    );
                }
                Err(e) => {
                    warn!("Failed to request current tick info: {e:#}");
                }
            }

            // Read loop -- runs until the connection drops.
            return self.read_loop(&mut stream, addr).await;
        }

        anyhow::bail!("Failed to connect after {attempts} peer attempts")
    }

    /// Read packets from the stream and publish them to NATS.
    ///
    /// Returns when the connection is lost or an unrecoverable read error
    /// occurs. The `addr` is used only for marking failure on abnormal exit.
    async fn read_loop(
        &self,
        stream: &mut TcpStream,
        addr: SocketAddr,
    ) -> Result<()> {
        loop {
            match self.read_packet(stream).await {
                Ok((msg_type, dejavu, payload)) => {
                    debug!(
                        "Packet: type={msg_type}, dejavu={dejavu}, payload_len={}",
                        payload.len()
                    );

                    if let Err(e) = self
                        .decoder
                        .decode_and_publish(msg_type, dejavu, &payload, &self.nats)
                        .await
                    {
                        warn!("Decode/publish error for type {msg_type}: {e:#}");
                    }
                }
                Err(e) => {
                    warn!("Read loop error on {addr}: {e:#}");
                    return Err(e);
                }
            }
        }
    }

    /// Read a single packet (header + payload) from the stream.
    async fn read_packet(
        &self,
        stream: &mut TcpStream,
    ) -> Result<(u8, u32, Vec<u8>)> {
        // Read 8-byte header.
        let mut header_buf = [0u8; 8];
        self.read_exact_timeout(stream, &mut header_buf)
            .await
            .context("Failed to read header")?;

        // Parse header.
        let header =
            unsafe { &*(&header_buf as *const [u8; 8] as *const RequestResponseHeader) };

        let msg_type = header.msg_type();
        let dejavu = header.dejavu();
        let payload_size = header.payload_size() as usize;

        // Read payload.
        let payload = if payload_size > 0 {
            let mut buf = vec![0u8; payload_size];
            self.read_exact_timeout(stream, &mut buf)
                .await
                .with_context(|| format!("Failed to read payload (type={msg_type})"))?;
            buf
        } else {
            Vec::new()
        };

        Ok((msg_type, dejavu, payload))
    }

    /// Read exactly `buf.len()` bytes with a timeout.
    async fn read_exact_timeout(
        &self,
        stream: &mut TcpStream,
        buf: &mut [u8],
    ) -> Result<()> {
        match tokio::time::timeout(self.config.tcp_timeout, stream.read_exact(buf)).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e).context("Read error"),
            Err(_) => Err(anyhow::anyhow!("Read timeout")),
        }
    }
}
