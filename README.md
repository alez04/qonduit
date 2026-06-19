# Qonduit

High-performance Qubic blockchain indexer and RPC server.

Qonduit connects to a Qubic node via TCP, decodes the binary protocol in real-time, indexes all blockchain data into RocksDB, and serves it through a REST API, JSON-RPC 2.0 (Bob-compatible), and WebSocket subscriptions.

## Architecture

```
Qubic Node ─TCP─→ Ingestion ─JSON─→ NATS JetStream ──→ Processor ──→ RocksDB
   (21841)           │                    │               Hot Cache
                     │                    │               Cold Tier (Parquet)
                     │                    │
                     └── HTTP/WS ─────────┴──→ REST / JSON-RPC / WebSocket
                        (/metrics)              (/metrics Prometheus)
```

## Features

- **TCP Ingestion**: Peer exchange handshake, packet decoder for all 42 message types, SHA-256 transaction hashing
- **NATS JetStream**: 9 typed event streams, durable consumers, publisher helpers
- **3-Tier Storage**: Hot (RAM LRU cache) + Warm (RocksDB, 11 column families) + Cold (Parquet export)
- **REST API**: 17 endpoints for tick, transaction, entity, spectrum, asset, computors, and contract queries
- **JSON-RPC 2.0**: 27 Bob-compatible methods + 13 Qonduit-native methods
- **WebSocket**: 6 real-time subscription topics (tick, tx, entity, spectrum, custom-message, contract-fn)
- **Prometheus Metrics**: `/metrics` endpoint with 11 counters and gauges
- **Graceful Shutdown**: Broadcast channel with 30s timeout across all services
- **Docker**: Multi-stage Dockerfile + docker-compose with NATS

## Quick Start

### Docker Compose (recommended)

```bash
docker compose up -d
```

This starts Qonduit + NATS JetStream. Configure the Qubic node address via environment variable:

```bash
QONDUIT_NODE_ADDR=your-node-ip:21841 docker compose up -d
```

### From Source

**Prerequisites**: Rust 1.96+, CMake, libclang, pkg-config, zlib

```bash
# Clone
git clone https://github.com/alez04/qonduit.git
cd qonduit

# Build
cargo build --release

# Run (needs NATS running locally)
./target/release/qonduit --config qonduit.example.toml
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `QONDUIT_NATS_URL` | `nats://localhost:4222` | NATS server URL |
| `QONDUIT_LISTEN_ADDR` | `0.0.0.0:8080` | API listen address |
| `QONDUIT_NODE_ADDR` | `127.0.0.1:21841` | Qubic node TCP address |
| `QONDUIT_DATA_DIR` | `./data` | RocksDB data directory |
| `RUST_LOG` | `info` | Log level filter |

## API

### Health Check

```bash
curl http://localhost:8080/health
# {"status":"ok","version":"0.1.0"}
```

### REST API

```bash
# Current tick
curl http://localhost:8080/v1/tick

# Specific tick
curl http://localhost:8080/v1/tick/100000

# Entity by identity
curl http://localhost:8080/v1/entity/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA

# Computors
curl http://localhost:8080/v1/computors

# Assets
curl http://localhost:8080/v1/issued-assets
```

### JSON-RPC 2.0

```bash
# Bob-compatible
curl -X POST http://localhost:8080/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"getTickInfo","id":1}'

curl -X POST http://localhost:8080/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"getEntity","params":["IDENTITY_HERE"],"id":2}'

# Qonduit-native
curl -X POST http://localhost:8080/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"qonduit_getEntityActivity","params":["IDENTITY", 100],"id":3}'
```

### WebSocket

```bash
# Subscribe to new ticks (use wscat or similar)
wscat -c "ws://localhost:8080/ws/tick"
wscat -c "ws://localhost:8080/ws/tx?epoch=100"
```

### Prometheus Metrics

```bash
curl http://localhost:8080/metrics
```

## Configuration

Copy and edit the example config:

```bash
cp qonduit.example.toml qonduit.toml
```

```toml
[nats]
url = "nats://localhost:4222"

[storage]
data_dir = "./data"

[query]
listen_addr = "0.0.0.0:8080"

[ingestion]
node_addr = "127.0.0.1:21841"
```

## Project Structure

```
qonduit/
├── crates/
│   ├── core/          # Protocol structs, constants, identity encoding, hashing
│   ├── ingestion/     # TCP client, wire decoders, NATS publishers
│   ├── processor/     # NATS consumer, index builders
│   ├── storage/       # RocksDB warm tier, RAM hot cache, Parquet cold tier
│   └── query/         # REST API, JSON-RPC, WebSocket, metrics
├── Dockerfile
├── docker-compose.yml
└── qonduit.example.toml
```

## Testing

```bash
# Unit + integration tests
cargo test

# Clippy lint
cargo clippy --workspace -- -D warnings
```

## License

MIT
