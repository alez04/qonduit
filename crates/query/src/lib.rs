//! Query server: HTTP/REST, JSON-RPC 2.0, and WebSocket endpoints.
//!
//! Serves the public API that dApps and clients use to query indexed data.
//! Compatible with Bob JSON-RPC methods as a superset.

pub mod docs;
pub mod metrics;
pub mod rpc;
pub mod rest;
pub mod ws;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use async_nats::Client as NatsClient;
use axum::Router;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

/// Shared state for the query server.
pub struct AppState {
    pub storage: qonduit_storage::WarmStorage,
    pub nats: NatsClient,
    pub pipeline: std::sync::Arc<qonduit_core::PipelineState>,
}

/// Configuration for the query server.
#[derive(Debug, Clone)]
pub struct QueryConfig {
    pub listen_addr: SocketAddr,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:8080".parse().unwrap(),
        }
    }
}

/// Build the Axum router with all routes.
pub fn build_router(state: Arc<AppState>) -> Router {
    // TODO: Rate limiting middleware.
    // `tower-http` does not include rate limiting out of the box.
    // Options for future implementation:
    //   - `governor` crate: per-IP rate limiting with in-memory state
    //   - Custom `tower::Layer` using `tokio::sync::Semaphore` per IP
    //   - External rate limiter (e.g., nginx, cloudflare) in front of the service
    Router::new()
        .merge(rest::routes())
        .merge(rpc::routes())
        .merge(ws::routes())
        .merge(docs::docs_routes())
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Start the query server.
pub async fn run(config: QueryConfig, state: Arc<AppState>) -> Result<()> {
    let app = build_router(state);

    info!("Query server listening on {}", config.listen_addr);

    let listener = tokio::net::TcpListener::bind(config.listen_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
