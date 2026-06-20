//! Peer discovery, health tracking, and selection for Qubic node connections.
//!
//! Maintains a shared list of known peers with success/failure metadata,
//! supports bootstrapping from the Qubic public API, peer exchange
//! integration, and intelligent peer selection.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rand::seq::SliceRandom;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Default Qubic protocol port.
const QUBIC_PORT: u16 = 21841;

/// Bob node P2P port.
const BOB_PORT: u16 = 21842;

/// Timeout for the bootstrap HTTP request.
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(10);

/// Peers not seen within this duration and meeting failure criteria are pruned.
const STALE_THRESHOLD: Duration = Duration::from_secs(600); // 10 minutes

/// Minimum failure count to consider a peer for pruning.
const PRUNE_FAILURE_THRESHOLD: u32 = 5;

/// A known peer with health metadata.
#[derive(Debug, Clone)]
pub struct Peer {
    pub addr: SocketAddr,
    pub last_seen: Option<Instant>,
    pub last_failure: Option<Instant>,
    pub success_count: u32,
    pub failure_count: u32,
    /// Number of times this peer has actually sent broadcast data (type 8, 24, etc.).
    /// Nodes that never broadcast are deprioritized.
    pub broadcast_count: u32,
}

impl Peer {
    /// Create a new peer with no history.
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            last_seen: None,
            last_failure: None,
            success_count: 0,
            failure_count: 0,
            broadcast_count: 0,
        }
    }

    /// Compute a selection score in [0.0, 1.0]. Higher is better.
    ///
    /// - Peers that actually broadcast get a major boost (they're what we need).
    /// - Success ratio is a baseline for non-broadcasting connections.
    /// - Unseen peers start at 0.3 (below broadcast peers).
    fn score(&self) -> f64 {
        if self.broadcast_count > 0 {
            // Broadcast peers score 0.7-1.0 based on broadcast frequency
            let broadcast_bonus = (self.broadcast_count as f64).min(10.0) / 10.0 * 0.3;
            return 0.7 + broadcast_bonus;
        }
        let total = self.success_count + self.failure_count;
        if total == 0 {
            return 0.3;
        }
        // Non-broadcasting peers max out at 0.5
        self.success_count as f64 / total as f64 * 0.5
    }

    /// Whether this peer is eligible for pruning.
    fn is_stale(&self) -> bool {
        let last_seen = match self.last_seen {
            Some(t) => t,
            None => return false, // never tried yet, keep them
        };
        self.failure_count >= PRUNE_FAILURE_THRESHOLD
            && self.success_count == 0
            && last_seen.elapsed() >= STALE_THRESHOLD
    }
}

/// JSON response from `api.qubic.global/random-peers`.
///
/// Both lite and BOB peers are collected. BOB peers (port 21842) store
/// full historical data and are preferred for backfill operations.
#[derive(Debug, serde::Deserialize)]
struct RandomPeersResponse {
    #[serde(default, rename = "litePeers")]
    lite_peers: Vec<String>,
    #[serde(default, rename = "bobPeers")]
    bob_peers: Vec<String>,
}

/// Manages peer discovery, health tracking, and selection for Qubic node connections.
#[derive(Debug)]
pub struct PeerManager {
    peers: Arc<RwLock<Vec<Peer>>>,
    #[allow(dead_code)]
    bootstrap_urls: Vec<String>,
}

impl PeerManager {
    /// Create a new PeerManager with optional known bootstrap addresses.
    pub fn new(bootstrap_addrs: &[SocketAddr]) -> Self {
        let peers: Vec<Peer> = bootstrap_addrs.iter().map(|&addr| Peer::new(addr)).collect();
        Self {
            peers: Arc::new(RwLock::new(peers)),
            bootstrap_urls: vec![
                "https://api.qubic.global/random-peers?service=bobNode&litePeers=8&bobPeers=8"
                    .to_string(),
            ],
        }
    }

    /// Bootstrap peers from the Qubic public API.
    ///
    /// Fetches random peers from `api.qubic.global/random-peers`, parses
    /// the JSON response, and adds all returned addresses to the peer list.
    /// Lite peers use port 21841; Bob peers use port 21842.
    pub async fn bootstrap_from_api(&self) -> Result<()> {
        let url = &self.bootstrap_urls[0];
        info!("Bootstrapping peers from {url}");

        let client = reqwest::Client::builder()
            .timeout(BOOTSTRAP_TIMEOUT)
            .build()
            .context("Failed to build HTTP client")?;

        let resp = client
            .get(url)
            .send()
            .await
            .context("HTTP request to Qubic API failed")?;

        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!("Qubic API returned HTTP {status}");
        }

        let body: RandomPeersResponse = resp
            .json()
            .await
            .context("Failed to parse Qubic API response as JSON")?;

        let mut added = 0usize;
        let mut peers = self.peers.write().await;

        // Add lite peers (port 21841) — consensus nodes for live data.
        for ip_str in &body.lite_peers {
            if let Some(addr) = parse_peer_addr(ip_str, QUBIC_PORT) {
                if !peers.iter().any(|p| p.addr == addr) {
                    peers.push(Peer::new(addr));
                    added += 1;
                    debug!("Added lite peer {addr}");
                }
            }
        }

        // Add BOB peers (port 21842) — indexer nodes with full historical data.
        // BOB peers are critical for backfill operations.
        for ip_str in &body.bob_peers {
            if let Some(addr) = parse_peer_addr(ip_str, BOB_PORT) {
                if !peers.iter().any(|p| p.addr == addr) {
                    peers.push(Peer::new(addr));
                    added += 1;
                    debug!("Added BOB peer {addr}");
                }
            }
        }

        info!(
            "Bootstrap complete: {} new peers added ({} total)",
            added,
            peers.len()
        );
        Ok(())
    }

    /// Record peers received from a peer exchange handshake.
    ///
    /// Takes the raw 4-byte IPv4 addresses (as returned by
    /// `protocol::exchange_public_peers`), filters out zeroes and
    /// duplicates, and adds them on port 21841.
    pub async fn add_peers_from_exchange(&self, raw_peers: &[[u8; 4]]) {
        let mut peers = self.peers.write().await;
        let mut added = 0usize;

        for raw in raw_peers {
            let ip = Ipv4Addr::new(raw[0], raw[1], raw[2], raw[3]);

            // Filter out 0.0.0.0 (invalid / placeholder)
            if ip.is_unspecified() {
                debug!("Skipping unspecified address 0.0.0.0 from peer exchange");
                continue;
            }

            let addr = SocketAddr::new(ip.into(), QUBIC_PORT);

            if peers.iter().any(|p| p.addr == addr) {
                debug!("Peer {addr} already known, skipping");
                continue;
            }

            peers.push(Peer::new(addr));
            added += 1;
            debug!("Added peer {addr} from exchange");
        }

        if added > 0 {
            info!(
                "Added {added} peers from exchange ({} total)",
                peers.len()
            );
        }
    }

    /// Mark a peer as having sent broadcast data.
    ///
    /// This is the strongest positive signal — nodes that broadcast are
    /// the ones we actually need for live ingestion.
    pub async fn mark_broadcast(&self, addr: &SocketAddr) {
        let mut peers = self.peers.write().await;
        if let Some(peer) = peers.iter_mut().find(|p| p.addr == *addr) {
            peer.broadcast_count = peer.broadcast_count.saturating_add(1);
            peer.last_seen = Some(Instant::now());
        }
    }

    /// Mark a peer as successfully connected.
    pub async fn mark_success(&self, addr: &SocketAddr) {
        let mut peers = self.peers.write().await;
        if let Some(peer) = peers.iter_mut().find(|p| p.addr == *addr) {
            peer.success_count = peer.success_count.saturating_add(1);
            peer.last_seen = Some(Instant::now());
            debug!("Marked {addr} as success (score={:.2})", peer.score());
        } else {
            // Unknown peer -- add it with one success.
            let mut peer = Peer::new(*addr);
            peer.success_count = 1;
            peer.last_seen = Some(Instant::now());
            peers.push(peer);
            debug!("Discovered new peer {addr} via successful connection");
        }
    }

    /// Mark a peer as failed.
    pub async fn mark_failure(&self, addr: &SocketAddr) {
        let mut peers = self.peers.write().await;
        if let Some(peer) = peers.iter_mut().find(|p| p.addr == *addr) {
            peer.failure_count = peer.failure_count.saturating_add(1);
            peer.last_failure = Some(Instant::now());
            debug!(
                "Marked {addr} as failure (failures={}, score={:.2})",
                peer.failure_count,
                peer.score()
            );
        } else {
            warn!("mark_failure called for unknown peer {addr}");
        }
    }

    /// Get the best peer to connect to.
    ///
    /// Selection strategy:
    /// 1. Filter to peers with score >= 0.3 (avoid nodes with terrible track records).
    /// 2. Among qualifying peers, prefer those not tried recently.
    /// 3. Break ties randomly.
    pub async fn best_peer(&self) -> Option<SocketAddr> {
        let peers = self.peers.read().await;

        let mut candidates: Vec<&Peer> = peers
            .iter()
            .filter(|p| p.score() >= 0.2)
            .collect();

        if candidates.is_empty() {
            // Fall back to any peer if nothing qualifies.
            candidates = peers.iter().collect();
        }

        if candidates.is_empty() {
            return None;
        }

        // Sort: highest score first, then least recently seen first.
        let now = Instant::now();
        candidates.sort_by(|a, b| {
            b.score()
                .partial_cmp(&a.score())
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    let a_seen = a.last_seen.map(|t| now.duration_since(t).as_secs()).unwrap_or(u64::MAX);
                    let b_seen = b.last_seen.map(|t| now.duration_since(t).as_secs()).unwrap_or(u64::MAX);
                    // Higher "time since last seen" is better (less recently tried).
                    b_seen.cmp(&a_seen)
                })
        });

        // Add some randomness among the top candidates to avoid thundering herd.
        let top_n = candidates.len().min(3);
        let pick = rand::random::<usize>() % top_n;
        Some(candidates[pick].addr)
    }

    /// Get a random peer for load distribution.
    pub async fn random_peer(&self) -> Option<SocketAddr> {
        let peers = self.peers.read().await;
        peers.choose(&mut rand::thread_rng()).map(|p| p.addr)
    }

    /// Get the best BOB peer (port 21842) for backfill operations.
    ///
    /// BOB peers store full historical data and are preferred for backfill.
    /// Falls back to the general `best_peer()` if no BOB peers are available.
    pub async fn best_bob_peer(&self) -> Option<SocketAddr> {
        let peers = self.peers.read().await;

        let mut bob_candidates: Vec<&Peer> = peers
            .iter()
            .filter(|p| p.addr.port() == BOB_PORT && p.score() >= 0.2)
            .collect();

        if bob_candidates.is_empty() {
            // No BOB peers available, fall back to any peer
            drop(peers);
            return self.best_peer().await;
        }

        // Sort: highest score first, then least recently seen first
        let now = Instant::now();
        bob_candidates.sort_by(|a, b| {
            b.score()
                .partial_cmp(&a.score())
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    let a_seen = a.last_seen.map(|t| now.duration_since(t).as_secs()).unwrap_or(u64::MAX);
                    let b_seen = b.last_seen.map(|t| now.duration_since(t).as_secs()).unwrap_or(u64::MAX);
                    b_seen.cmp(&a_seen)
                })
        });

        let top_n = bob_candidates.len().min(3);
        let pick = rand::random::<usize>() % top_n;
        Some(bob_candidates[pick].addr)
    }

    /// Get all known peers.
    pub async fn all_peers(&self) -> Vec<Peer> {
        self.peers.read().await.clone()
    }

    /// Get count of known peers.
    pub async fn peer_count(&self) -> usize {
        self.peers.read().await.len()
    }

    /// Prune stale peers.
    ///
    /// Removes peers that have never succeeded, have >= 5 failures,
    /// and haven't been seen in 10 minutes.
    pub async fn prune_stale(&self) {
        let before = self.peers.read().await.len();
        self.peers
            .write()
            .await
            .retain(|p| !p.is_stale());
        let after = self.peers.read().await.len();
        let pruned = before.saturating_sub(after);
        if pruned > 0 {
            info!("Pruned {pruned} stale peers ({after} remaining)");
        }
    }
}

/// Parse an IP string into a SocketAddr with the given port.
///
/// Returns `None` if the string is not a valid IPv4 address.
fn parse_peer_addr(ip_str: &str, port: u16) -> Option<SocketAddr> {
    let ip_str = ip_str.trim();
    let ip: Ipv4Addr = ip_str.parse().ok()?;
    Some(SocketAddr::new(ip.into(), port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_score_zero_total() {
        let peer = Peer::new("127.0.0.1:21841".parse().unwrap());
        // Unseen peers with no history score 0.3
        assert!((peer.score() - 0.3).abs() < f64::EPSILON);
    }

    #[test]
    fn test_peer_score_all_success() {
        let mut peer = Peer::new("127.0.0.1:21841".parse().unwrap());
        peer.success_count = 10;
        // Non-broadcasting peers max at 0.5
        assert!((peer.score() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_peer_score_mixed() {
        let mut peer = Peer::new("127.0.0.1:21841".parse().unwrap());
        peer.success_count = 3;
        peer.failure_count = 7;
        // 0.5 * (3/10) = 0.15
        assert!((peer.score() - 0.15).abs() < f64::EPSILON);
    }

    #[test]
    fn test_peer_score_broadcast() {
        let mut peer = Peer::new("127.0.0.1:21841".parse().unwrap());
        peer.broadcast_count = 5;
        // Broadcast peers score 0.7 + 0.15 = 0.85
        assert!((peer.score() - 0.85).abs() < f64::EPSILON);
    }

    #[test]
    fn test_peer_is_stale_never_seen() {
        let peer = Peer::new("127.0.0.1:21841".parse().unwrap());
        assert!(!peer.is_stale()); // never tried = not stale
    }

    #[test]
    fn test_peer_is_stale_with_successes() {
        let mut peer = Peer::new("127.0.0.1:21841".parse().unwrap());
        peer.last_seen = Some(Instant::now() - Duration::from_secs(1200));
        peer.success_count = 1;
        peer.failure_count = 10;
        assert!(!peer.is_stale()); // has successes, so not stale
    }

    #[test]
    fn test_parse_peer_addr_valid() {
        let addr = parse_peer_addr("192.168.1.1", 21841);
        assert_eq!(
            addr,
            Some("192.168.1.1:21841".parse::<SocketAddr>().unwrap())
        );
    }

    #[test]
    fn test_parse_peer_addr_with_whitespace() {
        let addr = parse_peer_addr("  10.0.0.1  ", 21841);
        assert_eq!(
            addr,
            Some("10.0.0.1:21841".parse::<SocketAddr>().unwrap())
        );
    }

    #[test]
    fn test_parse_peer_addr_invalid() {
        assert!(parse_peer_addr("not-an-ip", 21841).is_none());
        assert!(parse_peer_addr("999.999.999.999", 21841).is_none());
    }

    #[tokio::test]
    async fn test_add_peers_from_exchange_filters_zeroes() {
        let mgr = PeerManager::new(&[]);
        let raw = [[1, 2, 3, 4], [0, 0, 0, 0], [5, 6, 7, 8], [1, 2, 3, 4]];
        mgr.add_peers_from_exchange(&raw).await;

        let peers = mgr.all_peers().await;
        assert_eq!(peers.len(), 2);
        assert!(peers.iter().any(|p| p.addr.ip() == Ipv4Addr::new(1, 2, 3, 4)));
        assert!(peers.iter().any(|p| p.addr.ip() == Ipv4Addr::new(5, 6, 7, 8)));
    }

    #[tokio::test]
    async fn test_mark_success_and_failure() {
        let addr: SocketAddr = "1.2.3.4:21841".parse().unwrap();
        let mgr = PeerManager::new(&[addr]);

        mgr.mark_success(&addr).await;
        mgr.mark_success(&addr).await;
        mgr.mark_failure(&addr).await;

        let peers = mgr.all_peers().await;
        let peer = peers.iter().find(|p| p.addr == addr).unwrap();
        assert_eq!(peer.success_count, 2);
        assert_eq!(peer.failure_count, 1);
        // Non-broadcasting: 0.5 * (2/3) = 0.333...
        assert!((peer.score() - (2.0 / 3.0 * 0.5)).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_best_peer_empty() {
        let mgr = PeerManager::new(&[]);
        assert!(mgr.best_peer().await.is_none());
    }

    #[tokio::test]
    async fn test_best_peer_with_entries() {
        let addr1: SocketAddr = "1.2.3.4:21841".parse().unwrap();
        let addr2: SocketAddr = "5.6.7.8:21841".parse().unwrap();
        let mgr = PeerManager::new(&[addr1, addr2]);

        mgr.mark_success(&addr1).await;
        mgr.mark_failure(&addr2).await;

        let best = mgr.best_peer().await;
        assert!(best.is_some());
    }

    #[tokio::test]
    async fn test_peer_count() {
        let mgr = PeerManager::new(&[]);
        assert_eq!(mgr.peer_count().await, 0);

        let addr: SocketAddr = "1.2.3.4:21841".parse().unwrap();
        let mgr = PeerManager::new(&[addr]);
        assert_eq!(mgr.peer_count().await, 1);
    }

    #[tokio::test]
    async fn test_prune_stale() {
        let mut peer = Peer::new("1.2.3.4:21841".parse().unwrap());
        peer.last_seen = Some(Instant::now() - Duration::from_secs(1200));
        peer.failure_count = 10;
        peer.success_count = 0;

        let mgr = PeerManager::new(&[]);
        mgr.peers.write().await.push(peer);
        assert_eq!(mgr.peer_count().await, 1);

        mgr.prune_stale().await;
        assert_eq!(mgr.peer_count().await, 0);
    }

    #[tokio::test]
    async fn test_add_peers_from_exchange_deduplicates() {
        let mgr = PeerManager::new(&[]);
        let raw = [[10, 0, 0, 1], [10, 0, 0, 1], [10, 0, 0, 1], [10, 0, 0, 1]];
        mgr.add_peers_from_exchange(&raw).await;
        assert_eq!(mgr.peer_count().await, 1);
    }

    #[tokio::test]
    async fn test_mark_success_unknown_peer() {
        let addr: SocketAddr = "42.42.42.42:21841".parse().unwrap();
        let mgr = PeerManager::new(&[]);

        mgr.mark_success(&addr).await;
        assert_eq!(mgr.peer_count().await, 1);

        let peers = mgr.all_peers().await;
        let peer = peers.iter().find(|p| p.addr == addr).unwrap();
        assert_eq!(peer.success_count, 1);
    }
}
