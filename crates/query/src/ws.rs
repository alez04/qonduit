//! WebSocket subscription endpoints.
//!
//! Supports real-time streaming of blockchain events via WebSocket.
//! Topics: tick, tx, entity, spectrum, custom-message, contract-fn.

use std::sync::Arc;

use axum::{
    Router,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::get,
};
use futures_util::StreamExt;

use tracing::{info, warn};

use crate::AppState;



/// Build WebSocket routes.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/ws/tick", get(ws_tick))
        .route("/ws/tx", get(ws_tx))
        .route("/ws/entity", get(ws_entity))
        .route("/ws/spectrum", get(ws_spectrum))
        .route("/ws/custom-message", get(ws_custom_message))
        .route("/ws/contract-fn", get(ws_contract_fn))
}

// ---------------------------------------------------------------------------
// Per-topic upgrade handlers
// ---------------------------------------------------------------------------

async fn ws_tick(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let subject = "QONDUIT.TICK".to_string();
    ws.on_upgrade(move |socket| handle_ws_subscription(socket, state, subject))
}

async fn ws_tx(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let subject = "QONDUIT.TX".to_string();
    ws.on_upgrade(move |socket| handle_ws_subscription(socket, state, subject))
}

async fn ws_entity(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let subject = "QONDUIT.ENTITY".to_string();
    ws.on_upgrade(move |socket| handle_ws_subscription(socket, state, subject))
}

async fn ws_spectrum(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let subject = "QONDUIT.SPECTRUM".to_string();
    ws.on_upgrade(move |socket| handle_ws_subscription(socket, state, subject))
}

async fn ws_custom_message(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let subject = "QONDUIT.CUSTMSG".to_string();
    ws.on_upgrade(move |socket| handle_ws_subscription(socket, state, subject))
}

async fn ws_contract_fn(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let subject = "QONDUIT.CFNR".to_string();
    ws.on_upgrade(move |socket| handle_ws_subscription(socket, state, subject))
}

// ---------------------------------------------------------------------------
// Shared handler
// ---------------------------------------------------------------------------

/// Subscribe to a NATS subject and forward every message to the WebSocket
/// client. The connection stays open until either side closes it.
async fn handle_ws_subscription(
    mut socket: WebSocket,
    state: Arc<AppState>,
    subject: String,
) {
    info!("WebSocket client subscribing to: {subject}");

    // Subscribe to NATS
    let mut sub = match state.nats.subscribe(subject.clone()).await {
        Ok(sub) => sub,
        Err(e) => {
            warn!("Failed to subscribe to {subject}: {e}");
            let _ = socket
                .send(Message::Text(
                    format!("{{\"error\": \"subscribe failed: {e}\"}}"),
                ))
                .await;
            return;
        }
    };

    // Send connected confirmation
    let _ = socket
        .send(Message::Text(
            serde_json::json!({"status": "connected", "subject": subject})
                .to_string(),
        ))
        .await;

    // Bidirectional loop: forward NATS messages to WS, handle WS control
    // messages (ping/pong/close).
    loop {
        tokio::select! {
            // NATS message -> WebSocket
            msg = sub.next() => {
                match msg {
                    Some(msg) => {
                        let payload = String::from_utf8_lossy(&msg.payload);
                        if socket
                            .send(Message::Text(payload.to_string()))
                            .await
                            .is_err()
                        {
                            break; // Client disconnected
                        }
                    }
                    None => break, // NATS subscription closed
                }
            }
            // WebSocket message -> handle ping/pong/close
            ws_msg = socket.recv() => {
                match ws_msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(data))) => {
                        let _ = socket.send(Message::Pong(data)).await;
                    }
                    _ => {} // Ignore other messages
                }
            }
        }
    }

    info!("WebSocket client disconnected from: {subject}");
}
