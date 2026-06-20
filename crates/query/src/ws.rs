//! WebSocket subscription endpoints.
//!
//! Supports real-time streaming of blockchain events via WebSocket.
//! Topics: tick, tx, entity, spectrum, custom-message, contract-fn,
//! computors, asset, contract, tickvote, oracle, log, quorum,
//! logdigest, mining.

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
        .route("/ws/computors", get(ws_computors))
        .route("/ws/asset", get(ws_asset))
        .route("/ws/contract", get(ws_contract))
        .route("/ws/tickvote", get(ws_tickvote))
        .route("/ws/oracle", get(ws_oracle))
        .route("/ws/log", get(ws_log))
        .route("/ws/quorum", get(ws_quorum))
        .route("/ws/logdigest", get(ws_logdigest))
        .route("/ws/mining", get(ws_mining))
}

// ---------------------------------------------------------------------------
// Per-topic upgrade handlers
// ---------------------------------------------------------------------------

async fn ws_tick(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let subject = "Q.*.QONDUIT.TICK".to_string();
    ws.on_upgrade(move |socket| handle_ws_subscription(socket, state, subject))
}

async fn ws_tx(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let subject = "Q.*.QONDUIT.TX".to_string();
    ws.on_upgrade(move |socket| handle_ws_subscription(socket, state, subject))
}

async fn ws_entity(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let subject = "Q.*.QONDUIT.ENTITY".to_string();
    ws.on_upgrade(move |socket| handle_ws_subscription(socket, state, subject))
}

async fn ws_spectrum(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let subject = "Q.*.QONDUIT.SPECTRUM".to_string();
    ws.on_upgrade(move |socket| handle_ws_subscription(socket, state, subject))
}

async fn ws_custom_message(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let subject = "Q.*.QONDUIT.CUSTMSG".to_string();
    ws.on_upgrade(move |socket| handle_ws_subscription(socket, state, subject))
}

async fn ws_contract_fn(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let subject = "Q.*.QONDUIT.CFNR".to_string();
    ws.on_upgrade(move |socket| handle_ws_subscription(socket, state, subject))
}

async fn ws_computors(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let subject = "Q.*.QONDUIT.COMPUTORS".to_string();
    ws.on_upgrade(move |socket| handle_ws_subscription(socket, state, subject))
}

async fn ws_asset(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let subject = "Q.*.QONDUIT.ASSET".to_string();
    ws.on_upgrade(move |socket| handle_ws_subscription(socket, state, subject))
}

async fn ws_contract(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let subject = "Q.*.QONDUIT.CONTRACT".to_string();
    ws.on_upgrade(move |socket| handle_ws_subscription(socket, state, subject))
}

async fn ws_tickvote(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let subject = "Q.*.QONDUIT.TICKVOTE".to_string();
    ws.on_upgrade(move |socket| handle_ws_subscription(socket, state, subject))
}

async fn ws_oracle(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let subject = "Q.*.QONDUIT.ORACLE".to_string();
    ws.on_upgrade(move |socket| handle_ws_subscription(socket, state, subject))
}

async fn ws_log(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let subject = "Q.*.QONDUIT.LOG".to_string();
    ws.on_upgrade(move |socket| handle_ws_subscription(socket, state, subject))
}

async fn ws_quorum(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let subject = "Q.*.QONDUIT.QUORUM".to_string();
    ws.on_upgrade(move |socket| handle_ws_subscription(socket, state, subject))
}

async fn ws_logdigest(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let subject = "Q.*.QONDUIT.LOGDIGEST".to_string();
    ws.on_upgrade(move |socket| handle_ws_subscription(socket, state, subject))
}

async fn ws_mining(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let subject = "Q.*.QONDUIT.MINING".to_string();
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
