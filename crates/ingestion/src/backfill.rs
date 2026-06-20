//! Historical backfill: connects to Qubic nodes via parallel TCP connections
//! and requests historical tick data using REQUEST_TICK_DATA (type 16) and
//! REQUEST_TICK_TRANSACTIONS (type 29) protocol messages.
//!
//! Runs as a background task alongside the main live ingestion. The backfill
//! partitions the tick range across N workers, each with its own TCP connection,
//! and feeds decoded events through the same NATS pipeline.
//!
//! Uses a sliding-window pipeline: keeps PIPELINE_DEPTH requests in-flight
//! simultaneously and uses a FIFO queue to correctly match responses to requests.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_nats::Client as NatsClient;
use qonduit_core::PipelineState;
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};

use crate::nats_publish::NatsPublisher;
use crate::peer_manager::PeerManager;
use crate::protocol;

/// Maximum consecutive tick-data failures before skipping ahead.
/// Qubic nodes only serve recent ticks from the current epoch. When we
/// hit this many EndResponses in a row, we know the node doesn't have
/// this range and skip forward to avoid wasting TCP round-trips.
const MAX_CONSECUTIVE_FAILURES: u32 = 30;

/// Pipeline depth: number of tick data requests to keep in-flight simultaneously.
/// Overlaps network round-trips for dramatically higher throughput.
const PIPELINE_DEPTH: usize = 20;

/// Timeout for the overall sliding-window read loop. If no response arrives
/// within this window, we break out and reconnect.
const PIPELINE_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Initial skip size when hitting consecutive failures. Grows exponentially
/// on repeated skips (10K -> 20K -> 40K -> ... -> 1M max).
const FAILURE_SKIP_INITIAL: u32 = 10_000;

/// Maximum skip size to avoid overshooting the epoch range.
const FAILURE_SKIP_MAX: u32 = 1_000_000;

/// Maximum number of peers to try per worker connection cycle.
const MAX_PEERS_PER_WORKER: usize = 8;

/// Delay between reconnection attempts for a single worker.
const WORKER_RECONNECT_DELAY: Duration = Duration::from_secs(3);

/// Progress reporting interval.
const PROGRESS_REPORT_INTERVAL: Duration = Duration::from_secs(30);

/// Configuration for the backfill client.
#[derive(Debug, Clone)]
pub struct BackfillConfig {
    /// Optional explicit node address.
    pub node_addr: Option<SocketAddr>,
    /// Additional bootstrap addresses for peer discovery.
    pub bootstrap_addrs: Vec<SocketAddr>,
    /// Number of parallel worker connections.
    pub workers: usize,
    /// Start tick (inclusive). 0 means "start from epoch 1 tick 1".
    pub start_tick: u32,
    /// End tick (exclusive). 0 means "fill up to the current node tick".
    pub end_tick: u32,
    /// Delay between ticks (rate limiting, per worker). 0 = no delay.
    pub tick_delay: Duration,
    /// Timeout for TCP operations.
    pub tcp_timeout: Duration,
}

impl Default for BackfillConfig {
    fn default() -> Self {
        Self {
            node_addr: None,
            bootstrap_addrs: Vec::new(),
            workers: 4,
            start_tick: 0,
            end_tick: 0,
            tick_delay: Duration::from_millis(0),
            tcp_timeout: Duration::from_secs(30),
        }
    }
}

/// Expected response type for a pipelined request.
///
/// Qubic nodes process requests in FIFO order, so responses arrive in the same
/// order as requests. We use this to correctly interpret ambiguous type 35
/// (EndResponse) packets: they could mean "tick data not available" (for type 16)
/// or "end of transaction list" (for type 29).
#[derive(Debug)]
#[allow(dead_code)]
enum ExpectedResponse {
    /// Expecting type 8 (tick data) or type 35 (tick not available).
    TickData { tick: u32, dejavu: u32 },
    /// Expecting type 24(s) (transactions) followed by type 35 (end of list).
    TransactionEnd { tick: u32, dejavu: u32 },
}

impl ExpectedResponse {
    fn dejavu(&self) -> u32 {
        match self {
            ExpectedResponse::TickData { dejavu, .. } | ExpectedResponse::TransactionEnd { dejavu, .. } => *dejavu,
        }
    }
}

/// Shared state for the backfill process.
#[derive(Debug)]
struct BackfillShared {
    /// Whether the backfill is currently running.
    pub running: AtomicBool,
    /// Total ticks successfully processed by all workers.
    pub ticks_completed: AtomicU64,
    /// Total transactions discovered.
    pub txs_discovered: AtomicU64,
    /// Total tick data items discovered.
    pub ticks_discovered: AtomicU64,
    /// Ticks that failed (not found or error). u64 to prevent overflow during
    /// full-epoch scans where most ticks are unavailable on the node.
    pub ticks_failed: AtomicU64,
    /// The resolved start tick.
    pub start_tick: AtomicU32,
    /// The resolved end tick.
    pub end_tick: AtomicU32,
    // NOTE: processed_ticks removed — workers already have non-overlapping ranges
    // (partitioned at line ~295), so no cross-worker deduplication is needed.
}

impl BackfillShared {
    fn new() -> Self {
        Self {
            running: AtomicBool::new(false),
            ticks_completed: AtomicU64::new(0),
            txs_discovered: AtomicU64::new(0),
            ticks_discovered: AtomicU64::new(0),
            ticks_failed: AtomicU64::new(0),
            start_tick: AtomicU32::new(0),
            end_tick: AtomicU32::new(0),
        }
    }
}

/// Handle to a running backfill, exposing live metrics.
#[derive(Debug, Clone)]
pub struct BackfillHandle {
    shared: Arc<BackfillShared>,
}

impl BackfillHandle {
    /// Whether the backfill is currently running.
    pub fn is_running(&self) -> bool {
        self.shared.running.load(Ordering::Relaxed)
    }

    /// Total ticks completed.
    pub fn ticks_completed(&self) -> u64 {
        self.shared.ticks_completed.load(Ordering::Relaxed)
    }

    /// Total transactions discovered.
    pub fn txs_discovered(&self) -> u64 {
        self.shared.txs_discovered.load(Ordering::Relaxed)
    }

    /// Total tick data items discovered.
    pub fn ticks_discovered(&self) -> u64 {
        self.shared.ticks_discovered.load(Ordering::Relaxed)
    }

    /// Ticks that failed.
    pub fn ticks_failed(&self) -> u64 {
        self.shared.ticks_failed.load(Ordering::Relaxed)
    }

    /// The start tick.
    pub fn start_tick(&self) -> u32 {
        self.shared.start_tick.load(Ordering::Relaxed)
    }

    /// The end tick.
    pub fn end_tick(&self) -> u32 {
        self.shared.end_tick.load(Ordering::Relaxed)
    }
}

/// Historical backfill client.
///
/// Connects to Qubic nodes via multiple parallel TCP connections and
/// requests historical tick data and transactions for a specified tick range.
pub struct BackfillClient {
    config: BackfillConfig,
    nats: NatsClient,
    pipeline: Arc<PipelineState>,
    shared: Arc<BackfillShared>,
}

impl BackfillClient {
    /// Create a new backfill client.
    pub fn new(
        config: BackfillConfig,
        nats: NatsClient,
        pipeline: Arc<PipelineState>,
    ) -> Self {
        Self {
            config,
            nats,
            pipeline,
            shared: Arc::new(BackfillShared::new()),
        }
    }

    /// Return a handle for querying live metrics.
    pub fn handle(&self) -> BackfillHandle {
        BackfillHandle {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Run the backfill. This is the main entry point.
    pub async fn run(&mut self) -> Result<()> {
        self.shared.running.store(true, Ordering::Relaxed);
        self.pipeline.backfill_running.store(true, Ordering::Relaxed);

        // Bootstrap peer discovery
        let peer_manager = Arc::new(PeerManager::new(&self.bootstrap_addrs()));
        match peer_manager.bootstrap_from_api().await {
            Ok(()) => {
                info!(
                    "Backfill: API bootstrap OK, {} peers known",
                    peer_manager.peer_count().await
                );
            }
            Err(e) => {
                warn!("Backfill: API bootstrap failed (non-fatal): {e:#}");
            }
        }

        // Resolve end tick if not specified (connect to node, get current tick)
        let end_tick = if self.config.end_tick == 0 {
            self.resolve_end_tick(&peer_manager).await?
        } else {
            self.config.end_tick
        };

        let mut start_tick = self.config.start_tick;

        // When start_tick=0 (default), only backfill recent history that nodes
        // actually store. Qubic nodes typically keep ~100K recent ticks.
        if start_tick == 0 && end_tick > 100_000 {
            start_tick = end_tick.saturating_sub(100_000);
            info!(
                "Backfill: auto-setting start_tick to {start_tick} (end_tick={end_tick}, \
                 nodes only serve recent ~100K ticks. Set QONDUIT_BACKFILL_START_TICK=0 to scan all)"
            );
        }

        if start_tick >= end_tick {
            info!("Backfill: start_tick ({start_tick}) >= end_tick ({end_tick}), nothing to do");
            self.shared.running.store(false, Ordering::Relaxed);
            return Ok(());
        }

        self.shared.start_tick.store(start_tick, Ordering::Relaxed);
        self.shared.end_tick.store(end_tick, Ordering::Relaxed);
        self.pipeline.backfill_start_tick.store(start_tick, Ordering::Relaxed);
        self.pipeline.backfill_end_tick.store(end_tick, Ordering::Relaxed);

        let total_ticks = end_tick - start_tick;
        info!(
            "Backfill: range {start_tick}..{end_tick} ({total_ticks} ticks), {} workers",
            self.config.workers
        );

        // Partition the tick range across workers
        let workers = self.config.workers.max(1);
        let chunk_size = (total_ticks as usize / workers).max(1);

        let mut handles = Vec::new();
        for worker_id in 0..workers {
            let worker_start = start_tick + (worker_id as u32 * chunk_size as u32);
            let worker_end = if worker_id == workers - 1 {
                end_tick
            } else {
                (start_tick + ((worker_id as u32 + 1) * chunk_size as u32)).min(end_tick)
            };

            if worker_start >= worker_end {
                continue;
            }

            let config = self.config.clone();
            let nats = self.nats.clone();
            let pipeline = self.pipeline.clone();
            let shared = Arc::clone(&self.shared);
            let peer_manager = Arc::clone(&peer_manager);

            handles.push(tokio::spawn(async move {
                let mut worker = BackfillWorker::new(
                    config,
                    worker_id,
                    worker_start,
                    worker_end,
                    pipeline,
                    shared,
                    peer_manager,
                    nats,
                );
                if let Err(e) = worker.run().await {
                    error!("Backfill worker {worker_id} failed: {e:#}");
                }
            }));
        }

        // Spawn progress reporter
        let shared_progress = Arc::clone(&self.shared);
        let pipeline_progress = self.pipeline.clone();
        let progress_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(PROGRESS_REPORT_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let completed = shared_progress.ticks_completed.load(Ordering::Relaxed);
                let failed = shared_progress.ticks_failed.load(Ordering::Relaxed);
                let txs = shared_progress.txs_discovered.load(Ordering::Relaxed);
                let ticks = shared_progress.ticks_discovered.load(Ordering::Relaxed);
                let start = pipeline_progress.backfill_start_tick.load(Ordering::Relaxed);
                let end = pipeline_progress.backfill_end_tick.load(Ordering::Relaxed);
                let total = end.saturating_sub(start) as u64;
                let progress = if total > 0 {
                    (completed as f64 / total as f64 * 100.0) as u64
                } else {
                    0
                };
                pipeline_progress.backfill_ticks_completed.store(completed, Ordering::Relaxed);
                pipeline_progress.backfill_txs_discovered.store(txs, Ordering::Relaxed);
                pipeline_progress.backfill_ticks_discovered.store(ticks, Ordering::Relaxed);
                pipeline_progress.backfill_ticks_failed.store(failed, Ordering::Relaxed);
                info!(
                    "Backfill progress: {completed}/{total} ticks ({progress}%), {ticks} tick_data, {txs} txs, {failed} failed"
                );
            }
        });

        // Wait for all workers to complete
        for handle in handles {
            let _ = handle.await;
        }
        progress_handle.abort();

        self.shared.running.store(false, Ordering::Relaxed);
        self.pipeline.backfill_running.store(false, Ordering::Relaxed);

        let completed = self.shared.ticks_completed.load(Ordering::Relaxed);
        let failed = self.shared.ticks_failed.load(Ordering::Relaxed);
        let txs = self.shared.txs_discovered.load(Ordering::Relaxed);
        let ticks = self.shared.ticks_discovered.load(Ordering::Relaxed);
        self.pipeline.backfill_ticks_completed.store(completed, Ordering::Relaxed);
        self.pipeline.backfill_txs_discovered.store(txs, Ordering::Relaxed);
        self.pipeline.backfill_ticks_discovered.store(ticks, Ordering::Relaxed);
        self.pipeline.backfill_ticks_failed.store(failed, Ordering::Relaxed);
        info!(
            "Backfill complete: {completed} ticks processed, {ticks} tick_data, {txs} txs, {failed} failed"
        );

        Ok(())
    }

    /// Build bootstrap address list from config.
    fn bootstrap_addrs(&self) -> Vec<SocketAddr> {
        let mut addrs = self.config.bootstrap_addrs.clone();
        if let Some(addr) = self.config.node_addr {
            if !addrs.contains(&addr) {
                addrs.push(addr);
            }
        }
        addrs
    }

    /// Connect to a node and request CurrentTickInfo to resolve the end tick.
    async fn resolve_end_tick(&self, peer_manager: &PeerManager) -> Result<u32> {
        info!("Backfill: resolving end tick from node...");
        let mut attempts = 0;
        while attempts < MAX_PEERS_PER_WORKER {
            let addr = match peer_manager.best_peer().await {
                Some(a) => a,
                None => {
                    anyhow::bail!("No peers available to resolve end tick");
                }
            };
            attempts += 1;

            match self.try_resolve_from_peer(addr).await {
                Ok(tick) => {
                    info!("Backfill: resolved end tick = {tick} from {addr}");
                    return Ok(tick);
                }
                Err(e) => {
                    warn!("Backfill: failed to resolve from {addr}: {e:#}");
                    peer_manager.mark_failure(&addr).await;
                }
            }
        }
        anyhow::bail!("Failed to resolve end tick after {attempts} peer attempts")
    }

    /// Try to connect to a specific peer and get the current tick.
    async fn try_resolve_from_peer(&self, addr: SocketAddr) -> Result<u32> {
        let mut stream = match tokio::time::timeout(
            self.config.tcp_timeout,
            TcpStream::connect(addr),
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => anyhow::bail!("TCP connect failed: {e:#}"),
            Err(_) => anyhow::bail!("TCP connect timed out"),
        };

        let _ = stream.set_nodelay(true);

        // Peer exchange handshake
        let local_peers: [[u8; 4]; 4] = [[0, 0, 0, 0]; 4];
        protocol::exchange_public_peers(&mut stream, &local_peers).await?;

        // Request current tick info
        let data = protocol::request_current_tick_info(&mut stream).await?;
        if data.len() >= 8 {
            let tick = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
            Ok(tick)
        } else {
            anyhow::bail!("CurrentTickInfo response too short: {} bytes", data.len());
        }
    }
}

/// A single backfill worker that processes a partition of the tick range.
struct BackfillWorker {
    config: BackfillConfig,
    worker_id: usize,
    worker_start: u32,
    worker_end: u32,
    pipeline: Arc<PipelineState>,
    shared: Arc<BackfillShared>,
    peer_manager: Arc<PeerManager>,
    publisher: NatsPublisher,
}

impl BackfillWorker {
    fn new(
        config: BackfillConfig,
        worker_id: usize,
        worker_start: u32,
        worker_end: u32,
        pipeline: Arc<PipelineState>,
        shared: Arc<BackfillShared>,
        peer_manager: Arc<PeerManager>,
        nats: NatsClient,
    ) -> Self {
        let js = async_nats::jetstream::new(nats);
        let mut publisher = NatsPublisher::from_context(js);
        publisher.set_fire_and_forget(true);
        Self {
            config,
            worker_id,
            worker_start,
            worker_end,
            pipeline,
            shared,
            peer_manager,
            publisher,
        }
    }

    /// Run this worker. Reconnects on failure.
    async fn run(&mut self) -> Result<()> {
        info!(
            "Worker {}: processing ticks {}..{}",
            self.worker_id, self.worker_start, self.worker_end
        );

        let mut current_tick = self.worker_start;

        while current_tick < self.worker_end {
            match self.connect_and_process(&mut current_tick).await {
                Ok(()) => break,
                Err(e) => {
                    warn!("Worker {}: connection error at tick {current_tick}: {e:#}", self.worker_id);
                    tokio::time::sleep(WORKER_RECONNECT_DELAY).await;
                }
            }
        }

        info!(
            "Worker {}: finished (processed up to tick {})",
            self.worker_id, current_tick
        );
        Ok(())
    }

    /// Connect to a peer and process ticks from current_tick onwards.
    async fn connect_and_process(&mut self, current_tick: &mut u32) -> Result<()> {
        let mut attempts = 0;
        while attempts < MAX_PEERS_PER_WORKER {
            // Prefer BOB peers (port 21842) for backfill — they store full history.
            // Falls back to best_peer() if no BOB peers are available.
            let addr = match self.peer_manager.best_bob_peer().await {
                Some(a) => a,
                None => anyhow::bail!("No peers available"),
            };
            attempts += 1;

            match self.try_peer(addr, current_tick).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    warn!("Worker {}: peer {addr} failed: {e:#}", self.worker_id);
                    self.peer_manager.mark_failure(&addr).await;
                }
            }
        }

        anyhow::bail!("Exhausted all peers after {attempts} attempts")
    }

    /// Connect to a specific peer and process ticks using a sliding-window pipeline.
    ///
    /// Keeps PIPELINE_DEPTH requests in-flight simultaneously. Uses a FIFO queue
    /// to correctly match responses to requests, avoiding ambiguity when type 35
    /// (EndResponse) can mean either "tick not available" or "end of transactions".
    async fn try_peer(&mut self, addr: SocketAddr, current_tick: &mut u32) -> Result<()> {
        let mut stream = match tokio::time::timeout(
            self.config.tcp_timeout,
            TcpStream::connect(addr),
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => anyhow::bail!("TCP connect failed: {e:#}"),
            Err(_) => anyhow::bail!("TCP connect timed out"),
        };

        let _ = stream.set_nodelay(true);

        // Peer exchange handshake
        let local_peers: [[u8; 4]; 4] = [[0, 0, 0, 0]; 4];
        let remote_peers = protocol::exchange_public_peers(&mut stream, &local_peers).await?;
        self.peer_manager.add_peers_from_exchange(&remote_peers).await;
        self.peer_manager.mark_success(&addr).await;

        // Request current tick info to get epoch
        let epoch = match protocol::request_current_tick_info(&mut stream).await {
            Ok(data) if data.len() >= 4 => u16::from_le_bytes([data[2], data[3]]),
            _ => self.pipeline.node_epoch.load(Ordering::Relaxed),
        };

        info!(
            "Worker {}: connected to {addr}, epoch={epoch}, pipelining from tick {}",
            self.worker_id, *current_tick
        );

        // Sliding-window pipeline state
        let mut expected: VecDeque<ExpectedResponse> = VecDeque::with_capacity(PIPELINE_DEPTH + 10);
        let mut next_tick_to_request = *current_tick;
        let mut consecutive_failures: u32 = 0;
        let mut skip_size: u32 = FAILURE_SKIP_INITIAL;

        // Fill the pipeline with initial batch of type 16 (tick data) requests
        Self::fill_pipeline(
            &self.shared,
            self.worker_id,
            self.worker_end,
            &mut stream,
            &mut next_tick_to_request,
            &mut expected,
            &mut consecutive_failures,
            &mut skip_size,
        )
        .await?;

        // Sliding window: read responses and keep the pipeline full
        while !expected.is_empty() {
            match tokio::time::timeout(
                PIPELINE_READ_TIMEOUT,
                protocol::read_packet(&mut stream),
            )
            .await
            {
                Ok(Ok((msg_type, dejavu, payload))) => {
                    match msg_type {
                        8 => {
                            // BROADCAST_FUTURE_TICK_DATA: tick data response.
                            // Search the deque by dejavu (not just front) because interleaved
                            // tx requests can put TransactionEnd entries ahead of TickData.
                            if let Some(idx) = expected.iter().position(|e| e.dejavu() == dejavu) {
                                let resp_tick = if payload.len() >= 8 {
                                    u32::from_le_bytes([payload[4], payload[5], payload[6], payload[7]])
                                } else {
                                    // Payload too short - remove and count as failure
                                    expected.remove(idx);
                                    self.shared.ticks_failed.fetch_add(1, Ordering::Relaxed);
                                    Self::fill_pipeline(
                                        &self.shared, self.worker_id, self.worker_end,
                                        &mut stream, &mut next_tick_to_request, &mut expected,
                                        &mut consecutive_failures, &mut skip_size,
                                    ).await?;
                                    continue;
                                };

                                // Remove the matching TickData entry
                                expected.remove(idx);

                                // Reset failure tracking
                                consecutive_failures = 0;
                                skip_size = FAILURE_SKIP_INITIAL;

                                // Decode and publish tick data
                                // Extract epoch from tick payload (offset 2..4) instead of
                                // using the connection-time epoch, which is wrong for
                                // historical ticks that may span epoch boundaries.
                                let tick_epoch = if payload.len() >= 4 {
                                    u16::from_le_bytes([payload[2], payload[3]])
                                } else {
                                    epoch
                                };
                                if let Err(e) = self.decode_and_publish(8, &payload, tick_epoch).await {
                                    debug!("Worker {}: tick data decode error for {resp_tick}: {e:#}", self.worker_id);
                                } else {
                                    self.shared.ticks_discovered.fetch_add(1, Ordering::Relaxed);
                                }

                                // NOTE: We do NOT send type 29 (transaction requests) during
                                // pipelined backfill. Transaction responses contain thousands of
                                // packets that would block the pipeline for minutes per tick.
                                // Transactions for recent ticks are handled by the live ingestion.
                                // For historical data, a separate tx-only pass can be added later.

                                self.shared.ticks_completed.fetch_add(1, Ordering::Relaxed);
                                self.pipeline.indexed_tick.fetch_max(resp_tick, Ordering::Relaxed);

                                // Refill the pipeline with more type 16 requests
                                Self::fill_pipeline(
                                    &self.shared, self.worker_id, self.worker_end,
                                    &mut stream, &mut next_tick_to_request, &mut expected,
                                    &mut consecutive_failures, &mut skip_size,
                                ).await?;
                            } else {
                                // Dejavu doesn't match any pending request — broadcast from live node, skip
                                debug!("Worker {}: skipping broadcast type 8 dejavu={dejavu}", self.worker_id);
                            }
                        }
                        35 => {
                            // END_RESPONSE: search deque by dejavu to handle interleaved requests.
                            if let Some(idx) = expected.iter().position(|e| e.dejavu() == dejavu) {
                                match expected.remove(idx) {
                                    Some(ExpectedResponse::TransactionEnd { .. }) => {
                                        // Normal end-of-transactions, nothing to do
                                    }
                                    Some(ExpectedResponse::TickData { .. }) => {
                                        // Tick data not available on this node
                                        consecutive_failures += 1;
                                        self.shared.ticks_failed.fetch_add(1, Ordering::Relaxed);
                                        Self::fill_pipeline(
                                            &self.shared, self.worker_id, self.worker_end,
                                            &mut stream, &mut next_tick_to_request, &mut expected,
                                            &mut consecutive_failures, &mut skip_size,
                                        ).await?;
                                    }
                                    None => {}
                                }
                            } else {
                                // Dejavu doesn't match any pending request — broadcast EndResponse, skip
                                debug!("Worker {}: skipping type 35 with dejavu={dejavu} (no match in queue)", self.worker_id);
                            }
                        }
                        24 => {
                            // BROADCAST_TRANSACTION: a transaction response
                            // For transactions, extract tick from payload (offset 72..76)
                            // and use the connection-time epoch as a best-effort fallback.
                            let tx_epoch = if payload.len() >= 76 {
                                let tx_tick = u32::from_le_bytes([payload[72], payload[73], payload[74], payload[75]]);
                                // If we could look up the epoch from the tick, we would.
                                // For now, the connection-time epoch is the best we have.
                                // The processor will index it under whatever epoch it gets.
                                let _ = tx_tick;
                                epoch
                            } else {
                                epoch
                            };
                            if let Err(e) = self.decode_and_publish(24, &payload, tx_epoch).await {
                                debug!("Worker {}: tx decode error: {e:#}", self.worker_id);
                            } else {
                                self.shared.txs_discovered.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        54 => {
                            // TRY_AGAIN — search deque by dejavu, count as failure
                            if let Some(idx) = expected.iter().position(|e| e.dejavu() == dejavu) {
                                if let Some(ExpectedResponse::TickData { .. }) = expected.remove(idx) {
                                    consecutive_failures += 1;
                                    self.shared.ticks_failed.fetch_add(1, Ordering::Relaxed);
                                    Self::fill_pipeline(
                                        &self.shared, self.worker_id, self.worker_end,
                                        &mut stream, &mut next_tick_to_request, &mut expected,
                                        &mut consecutive_failures, &mut skip_size,
                                    ).await?;
                                }
                            }
                        }
                        _ => {
                            // Type 28 (CurrentTickInfo broadcast), etc. — skip
                            debug!("Worker {}: skipping unexpected msg_type {msg_type} in pipeline", self.worker_id);
                        }
                    }
                }
                Ok(Err(e)) => {
                    return Err(e).context("Read error in sliding-window pipeline");
                }
                Err(_) => {
                    // Timeout: if pipeline is stuck, break and reconnect
                    let pending = expected.len();
                    if pending > 0 {
                        warn!(
                            "Worker {}: pipeline timeout with {pending} pending requests, reconnecting",
                            self.worker_id
                        );
                        // Count all pending TickData as failures
                        for entry in &expected {
                            if matches!(entry, ExpectedResponse::TickData { .. }) {
                                self.shared.ticks_failed.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    break;
                }
            }
        }

        *current_tick = next_tick_to_request;
        Ok(())
    }

    /// Fill the sliding window with type 16 (tick data) requests.
    ///
    /// Keeps `PIPELINE_DEPTH` requests in-flight by sending new requests
    /// whenever slots are available. Applies the consecutive-failure skip
    /// logic when the node doesn't have data for the requested range.
    fn fill_pipeline<'a>(
        _shared: &'a BackfillShared,
        worker_id: usize,
        worker_end: u32,
        stream: &'a mut TcpStream,
        next_tick_to_request: &'a mut u32,
        expected: &'a mut VecDeque<ExpectedResponse>,
        consecutive_failures: &'a mut u32,
        skip_size: &'a mut u32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            while *next_tick_to_request < worker_end && expected.len() < PIPELINE_DEPTH {
                // Skip ahead if we've hit too many consecutive failures
                if *consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                    let skip_to = (*next_tick_to_request + *skip_size).min(worker_end);
                    info!(
                        "Worker {}: skipping tick {}..{} ({} ticks, {} consecutive failures)",
                        worker_id, *next_tick_to_request, skip_to, *skip_size, *consecutive_failures
                    );
                    *next_tick_to_request = skip_to;
                    *consecutive_failures = 0;
                    *skip_size = (*skip_size * 2).min(FAILURE_SKIP_MAX);
                    continue;
                }

                // Send type 16 (REQUEST_TICK_DATA)
                let tick_dejavu = rand::random::<u32>().max(1);
                protocol::send_raw(
                    stream,
                    16,
                    &(*next_tick_to_request).to_le_bytes(),
                    tick_dejavu,
                )
                .await?;
                expected.push_back(ExpectedResponse::TickData { tick: *next_tick_to_request, dejavu: tick_dejavu });
                *next_tick_to_request += 1;
            }
            Ok(())
        })
    }

    /// Decode a raw packet and publish to the appropriate NATS subject.
    ///
    /// Reuses the stored publisher (fire-and-forget) for maximum throughput.
    async fn decode_and_publish(
        &mut self,
        msg_type: u8,
        payload: &[u8],
        epoch: u16,
    ) -> Result<()> {
        match msg_type {
            8 => {
                // BroadcastFutureTickData (tick data)
                let tick = crate::decoders::decode_tick(payload)?;
                self.publisher.publish_tick(epoch, &tick).await?;
            }
            24 => {
                // BroadcastTransaction
                let tx = crate::decoders::decode_transaction(payload)?;
                self.publisher.publish_tx(epoch, &tx).await?;
            }
            _ => {
                debug!("Worker {}: unhandled msg_type {msg_type} in backfill", self.worker_id);
            }
        }

        Ok(())
    }
}
