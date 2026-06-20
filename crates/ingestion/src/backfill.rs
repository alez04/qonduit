//! Historical backfill: connects to Qubic nodes via parallel TCP connections
//! and requests historical tick data using REQUEST_TICK_DATA (type 16) and
//! REQUEST_TICK_TRANSACTIONS (type 29) protocol messages.
//!
//! Runs as a background task alongside the main live ingestion. The backfill
//! partitions the tick range across N workers, each with its own TCP connection,
//! and feeds decoded events through the same NATS pipeline.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
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
const MAX_CONSECUTIVE_FAILURES: u32 = 50;

/// Pipeline depth: number of tick data requests to keep in-flight simultaneously.
/// Overlaps network round-trips for dramatically higher throughput.
const PIPELINE_DEPTH: usize = 20;

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
    /// Set of ticks that have been processed (for deduplication across workers).
    processed_ticks: Mutex<HashSet<u32>>,
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
            processed_ticks: Mutex::new(HashSet::new()),
        }
    }

    /// Check and mark a tick as being processed. Returns true if this tick
    /// should be processed (not already claimed by another worker).
    fn claim_tick(&self, tick: u32) -> bool {
        let mut set = self.processed_ticks.lock().unwrap();
        set.insert(tick) // insert returns true if the value was not already present
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
    ///
    /// 1. Resolves the tick range (connects to node to determine end_tick if needed).
    /// 2. Partitions the range across workers.
    /// 3. Spawns parallel worker tasks.
    /// 4. Reports progress periodically.
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
        // actually store. Qubic nodes typically keep ~100K recent ticks. Going
        // further back just wastes TCP round-trips getting EndResponses.
        // Set QONDUIT_BACKFILL_START_TICK explicitly to scan a wider range.
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
                // Write to pipeline state for system-info endpoint
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
        // Write final stats to pipeline
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
            // Try to connect and process ticks
            match self.connect_and_process(&mut current_tick).await {
                Ok(()) => {
                    // All ticks processed
                    break;
                }
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
            let addr = match self.peer_manager.best_peer().await {
                Some(a) => a,
                None => {
                    anyhow::bail!("No peers available");
                }
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

    /// Fill the pipeline with tick data requests up to PIPELINE_DEPTH in-flight.
    ///
    /// Skips ticks already claimed by other workers and applies the
    /// consecutive-failure skip logic when the node lacks data.
    async fn fill_pipeline(
        shared: &BackfillShared,
        worker_id: usize,
        worker_end: u32,
        stream: &mut TcpStream,
        next_tick_to_request: &mut u32,
        pending_tick_data: &mut u32,
        consecutive_failures: &mut u32,
        skip_size: &mut u32,
    ) -> Result<()> {
        while *next_tick_to_request < worker_end
            && *pending_tick_data < PIPELINE_DEPTH as u32
        {
            // Check if another worker already claimed this tick
            if !shared.claim_tick(*next_tick_to_request) {
                *next_tick_to_request += 1;
                continue;
            }

            // Skip ahead if we've hit too many consecutive failures.
            // This happens when scanning into tick ranges the node doesn't have
            // (e.g. early epoch ticks that were pruned long ago).
            // Skip size doubles each time to cross large gaps quickly.
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

            protocol::send_raw(
                stream,
                16,
                &(*next_tick_to_request).to_le_bytes(),
                rand::random(),
            )
            .await?;
            *pending_tick_data += 1;
            *next_tick_to_request += 1;
        }
        Ok(())
    }

    /// Connect to a specific peer and process ticks using request pipelining.
    ///
    /// Instead of sending one request and waiting for the response, we keep
    /// PIPELINE_DEPTH requests in-flight simultaneously. As tick data responses
    /// arrive, we send transaction requests and new tick data requests to keep
    /// the pipeline full. This overlaps network latency with processing for
    /// dramatically higher throughput.
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

        // Pipelined processing state
        let mut consecutive_failures: u32 = 0;
        let mut skip_size: u32 = FAILURE_SKIP_INITIAL;
        let mut pending_tick_data: u32 = 0;
        let mut pending_tx: u32 = 0;
        let mut next_tick_to_request: u32 = *current_tick;

        // Phase 1: Fill the pipeline with initial batch of tick data requests
        Self::fill_pipeline(
            &self.shared,
            self.worker_id,
            self.worker_end,
            &mut stream,
            &mut next_tick_to_request,
            &mut pending_tick_data,
            &mut consecutive_failures,
            &mut skip_size,
        )
        .await?;

        // Phase 2: Read responses and refill pipeline (sliding window)
        while pending_tick_data > 0 || pending_tx > 0 {
            match protocol::read_packet(&mut stream).await {
                Ok((msg_type, _dejavu, payload)) => {
                    match msg_type {
                        8 => {
                            // Tick data response
                            pending_tick_data -= 1;
                            consecutive_failures = 0;
                            skip_size = FAILURE_SKIP_INITIAL;

                            // Extract tick number from payload (offset 4-7)
                            let resp_tick = if payload.len() >= 8 {
                                u32::from_le_bytes([payload[4], payload[5], payload[6], payload[7]])
                            } else {
                                debug!(
                                    "Worker {}: tick data payload too short ({} bytes)",
                                    self.worker_id,
                                    payload.len()
                                );
                                self.shared.ticks_failed.fetch_add(1, Ordering::Relaxed);
                                Self::fill_pipeline(
                                    &self.shared,
                                    self.worker_id,
                                    self.worker_end,
                                    &mut stream,
                                    &mut next_tick_to_request,
                                    &mut pending_tick_data,
                                    &mut consecutive_failures,
                                    &mut skip_size,
                                )
                                .await?;
                                continue;
                            };

                            // Decode and publish tick data
                            if let Err(e) = self
                                .decode_and_publish(8, &payload, epoch)
                                .await
                            {
                                debug!(
                                    "Worker {}: tick data decode error for {resp_tick}: {e:#}",
                                    self.worker_id
                                );
                            } else {
                                self.shared
                                    .ticks_discovered
                                    .fetch_add(1, Ordering::Relaxed);
                            }

                            // Request transactions for this tick
                            if let Err(e) = protocol::send_raw(
                                &mut stream,
                                29,
                                &resp_tick.to_le_bytes(),
                                rand::random(),
                            )
                            .await
                            {
                                debug!(
                                    "Worker {}: failed to send tx request for {resp_tick}: {e:#}",
                                    self.worker_id
                                );
                            } else {
                                pending_tx += 1;
                            }

                            self.shared
                                .ticks_completed
                                .fetch_add(1, Ordering::Relaxed);
                            self.pipeline
                                .indexed_tick
                                .fetch_max(resp_tick, Ordering::Relaxed);

                            // Refill pipeline
                            Self::fill_pipeline(
                                &self.shared,
                                self.worker_id,
                                self.worker_end,
                                &mut stream,
                                &mut next_tick_to_request,
                                &mut pending_tick_data,
                                &mut consecutive_failures,
                                &mut skip_size,
                            )
                            .await?;
                        }
                        35 => {
                            // EndResponse: either tick data unavailable or end
                            // of transaction list.
                            if pending_tx > 0 {
                                // End of transaction list (normal completion)
                                pending_tx -= 1;
                            } else if pending_tick_data > 0 {
                                // Tick data not available for this tick
                                pending_tick_data -= 1;
                                consecutive_failures += 1;
                                self.shared
                                    .ticks_failed
                                    .fetch_add(1, Ordering::Relaxed);

                                // Refill pipeline
                                Self::fill_pipeline(
                                    &self.shared,
                                    self.worker_id,
                                    self.worker_end,
                                    &mut stream,
                                    &mut next_tick_to_request,
                                    &mut pending_tick_data,
                                    &mut consecutive_failures,
                                    &mut skip_size,
                                )
                                .await?;
                            }
                        }
                        24 => {
                            // Transaction response
                            if let Err(e) = self
                                .decode_and_publish(24, &payload, epoch)
                                .await
                            {
                                debug!(
                                    "Worker {}: tx decode error: {e:#}",
                                    self.worker_id
                                );
                            } else {
                                self.shared
                                    .txs_discovered
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        _ => {
                            debug!(
                                "Worker {}: unexpected msg_type {msg_type} in pipeline",
                                self.worker_id
                            );
                        }
                    }
                }
                Err(e) => {
                    warn!("Worker {}: pipeline read error: {e:#}", self.worker_id);
                    break;
                }
            }
        }

        *current_tick = next_tick_to_request;
        Ok(())
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
