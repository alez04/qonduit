//! TCP client: connects to a Qubic node and reads packets from the stream.
//!
//! Implements the initial peer exchange handshake, then enters a read loop
//! dispatching decoded packets to NATS.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use async_nats::Client as NatsClient;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};

use qonduit_core::RequestResponseHeader;

use crate::decoder::PacketDecoder;
use crate::protocol;

/// Configuration for the ingestion client.
#[derive(Debug, Clone)]
pub struct IngestionConfig {
    /// Qubic node TCP address.
    pub node_addr: SocketAddr,
    /// Timeout for TCP operations.
    pub tcp_timeout: Duration,
    /// Reconnect delay on connection loss.
    pub reconnect_delay: Duration,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            node_addr: "127.0.0.1:21841".parse().unwrap(),
            tcp_timeout: Duration::from_secs(30),
            reconnect_delay: Duration::from_secs(5),
        }
    }
}

/// Ingestion client that reads from a Qubic node and publishes to NATS.
pub struct IngestionClient {
    config: IngestionConfig,
    nats: NatsClient,
    decoder: PacketDecoder,
    current_epoch: u16,
    current_tick: u32,
}

impl IngestionClient {
    pub fn new(config: IngestionConfig, nats: NatsClient) -> Self {
        Self {
            config,
            nats,
            decoder: PacketDecoder::new(),
            current_epoch: 0,
            current_tick: 0,
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
    pub async fn run(&self) -> Result<()> {
        loop {
            match self.connect_and_read().await {
                Ok(()) => {
                    warn!("Connection closed cleanly, reconnecting...");
                }
                Err(e) => {
                    error!("Ingestion error: {e:#}");
                }
            }

            info!(
                "Reconnecting in {}s...",
                self.config.reconnect_delay.as_secs()
            );
            tokio::time::sleep(self.config.reconnect_delay).await;
        }
    }

    /// Connect to the node and enter the packet read loop.
    async fn connect_and_read(&self) -> Result<()> {
        info!("Connecting to {}...", self.config.node_addr);

        let mut stream = tokio::time::timeout(
            self.config.tcp_timeout,
            TcpStream::connect(self.config.node_addr),
        )
        .await
        .context("TCP connect timeout")?
        .context("TCP connect failed")?;

        info!("Connected to {}", self.config.node_addr);

        // Exchange public peers (handshake)
        let local_peers: [[u8; 4]; 4] = [[127, 0, 0, 1]; 4];
        match protocol::exchange_public_peers(&mut stream, &local_peers).await {
            Ok(peers) => {
                info!("Peer exchange complete, received {} peer entries", peers.len());
            }
            Err(e) => {
                warn!("Peer exchange failed: {e:#}");
            }
        }

        // Request current tick info to bootstrap epoch/tick state
        match protocol::request_current_tick_info(&mut stream).await {
            Ok(data) if data.len() >= 6 => {
                let epoch = u16::from_le_bytes([data[0], data[1]]);
                let tick = u32::from_le_bytes([data[2], data[3], data[4], data[5]]);
                info!("Current state: epoch={epoch}, tick={tick}");
                // Store in client (requires &mut self for connect_and_read)
                // For now we log; mutation will be wired once run() takes &mut self.
                let _ = (epoch, tick);
            }
            Ok(data) => {
                warn!("CurrentTickInfo response too short: {} bytes", data.len());
            }
            Err(e) => {
                warn!("Failed to request current tick info: {e:#}");
            }
        }

        // Read loop
        loop {
            let (msg_type, dejavu, payload) =
                self.read_packet(&mut stream).await?;

            debug!(
                "Packet: type={msg_type}, dejavu={dejavu}, payload_len={}",
                payload.len()
            );

            // Decode and publish
            if let Err(e) = self.decoder.decode_and_publish(
                msg_type,
                dejavu,
                &payload,
                &self.nats,
            ).await {
                warn!("Decode/publish error for type {msg_type}: {e:#}");
            }
        }
    }

    /// Read a single packet (header + payload) from the stream.
    async fn read_packet(
        &self,
        stream: &mut TcpStream,
    ) -> Result<(u8, u32, Vec<u8>)> {
        // Read 8-byte header
        let mut header_buf = [0u8; 8];
        self.read_exact_timeout(stream, &mut header_buf)
            .await
            .context("Failed to read header")?;

        // Parse header
        let header =
            unsafe { &*(&header_buf as *const [u8; 8] as *const RequestResponseHeader) };

        let msg_type = header.msg_type();
        let dejavu = header.dejavu();
        let payload_size = header.payload_size() as usize;

        // Read payload
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
