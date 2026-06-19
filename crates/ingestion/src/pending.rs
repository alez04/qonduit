//! Pending request map for correlating TCP responses to requests.
//!
//! When a request is sent, a random dejavu ID is generated. The response
//! will echo this dejavu. The pending map stores a oneshot sender for each
//! pending dejavu, so the requester can await the response.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, oneshot};

pub type ResponseSender = oneshot::Sender<Vec<Vec<u8>>>;

/// Maps dejavu IDs to their response channels.
#[derive(Clone)]
pub struct PendingRequests {
    map: Arc<Mutex<HashMap<u32, ResponseSender>>>,
}

impl PendingRequests {
    pub fn new() -> Self {
        Self {
            map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register a pending request and return a receiver for the response.
    pub async fn register(&self, dejavu: u32) -> oneshot::Receiver<Vec<Vec<u8>>> {
        let (tx, rx) = oneshot::channel();
        self.map.lock().await.insert(dejavu, tx);
        rx
    }

    /// Deliver a response to a pending request. Returns true if found.
    pub async fn deliver(&self, dejavu: u32, responses: Vec<Vec<u8>>) -> bool {
        if let Some(tx) = self.map.lock().await.remove(&dejavu) {
            let _ = tx.send(responses);
            true
        } else {
            false
        }
    }

    /// Remove expired entries (where the receiver has been dropped).
    pub async fn cleanup(&self) {
        // The oneshot channels will be dropped when the timeout fires.
        // Drop entries where the receiver has been dropped.
        let mut map = self.map.lock().await;
        map.retain(|_, tx| !tx.is_closed());
    }

    pub async fn pending_count(&self) -> usize {
        self.map.lock().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_and_deliver() {
        let pending = PendingRequests::new();
        let rx = pending.register(42).await;

        assert_eq!(pending.pending_count().await, 1);

        let delivered = pending.deliver(42, vec![vec![1, 2, 3]]).await;
        assert!(delivered);

        let result = rx.await.unwrap();
        assert_eq!(result, vec![vec![1, 2, 3]]);

        assert_eq!(pending.pending_count().await, 0);
    }

    #[tokio::test]
    async fn test_deliver_unknown() {
        let pending = PendingRequests::new();
        let delivered = pending.deliver(99, vec![]).await;
        assert!(!delivered);
    }

    #[tokio::test]
    async fn test_cleanup() {
        let pending = PendingRequests::new();
        let _rx = pending.register(1).await;
        let rx2 = pending.register(2).await;

        assert_eq!(pending.pending_count().await, 2);

        // Drop rx2 so its sender becomes closed
        drop(rx2);

        pending.cleanup().await;

        // Only entry 1 should remain (entry 2's sender is closed)
        assert_eq!(pending.pending_count().await, 1);
    }
}
