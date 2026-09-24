# ObsChain (Backend)

[![CI](https://github.com/j-kon/obschain/actions/workflows/ci.yml/badge.svg)](https://github.com/j-kon/obschain/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

> **Observe Bitcoin. Understand the event.**

ObsChain is an open-source Bitcoin network observation, anomaly-detection, incident-analysis, and on-chain intelligence engine built in Rust.

Frontend: https://github.com/j-kon/obschain-web

---

## Capabilities

- **Network Ingestion**: Modular support for Bitcoin Core (RPC / ZMQ) and mempool.space (REST / WebSocket).
- **Anomaly Detection**: Configurable detectors for large transfers, unusual block intervals, fee spikes, and structural patterns.
- **Incident Intelligence**: Structured case management tracking incidents with verifiable evidence provenance.
- **Strict Evidence Standard**: Uncompromising separation between cryptographic on-chain facts and heuristic claims.
- **High Performance**: Native async Rust built on Tokio, Axum, and SQLx.

---

## Workspace Layout

```text
obschain/
├── crates/
│   ├── obschain-core/          # Core domain models, events, incidents, observations
│   ├── obschain-detectors/     # Detection logic (large transfers, intervals, etc.)
│   ├── obschain-ingest/        # Multi-source ingestion clients (RPC, ZMQ, REST, WS)
│   ├── obschain-incidents/     # Provenance verification & incident tracking
│   ├── obschain-intelligence/  # Graph modeling, clustering, report generation
│   └── obschain-storage/       # PostgreSQL / In-Memory repository storage
├── src/                        # API server binary (Axum)
├── migrations/                 # PostgreSQL schema migrations
└── docs/                       # Architecture, roadmaps, security, and event models
```

---

## Getting Started

### Prerequisites

- Rust 1.85+ (stable toolchain)
- Cargo
- Docker & Docker Compose (optional for PostgreSQL)

## Architecture & Live Bitcoin Ingestion

```text
mempool.space (REST + WebSocket)
               ↓
   ObsChain Ingestion Engine
               ↓
    Normalized Observations
               ↓
        Detector Pipeline
               ↓
           ChainEvent
               ↓
   Bounded Store + Live WS Broadcast (/api/v1/ws)
```

> **Note**: `mempool.space` is currently the initial live observation source. Bitcoin Core integration (RPC + ZMQ) is planned as the long-term independent sovereign source.

### How Live Ingestion Works

1. **Dual Ingestion (REST + WebSocket)**:
   - **WebSocket Stream**: Connects to `MEMPOOL_WS_URL` with auto-reconnection and exponential backoff. Subscribes to live blocks, mempool stats, and transaction broadcasts.
   - **REST Sync**: Periodically validates chain tip height via `MEMPOOL_API_URL`, establishing an initial baseline on startup and synchronizing current mempool backlog state.
2. **Strict Normalization**:
   - External raw payloads are normalized immediately into internal `Observation` domain models (`BlockObservation`, `TransactionObservation`, `MempoolObservation`).
   - **Integer Satoshis**: All amounts remain integer `u64` satoshis internally to eliminate floating-point precision issues.
   - **Safe Fee Rates**: Derived in sat/vB using integer vsize calculations with zero-division protection.
   - **Provenance Tracking**: Every observation and event tags its exact ingestion origin (`mempool_ws`, `mempool_rest`, etc.).
3. **Anomaly Detectors & Engine**:
   - Normalized observations flow through a bounded channel (`mpsc(1000)`) into the `DetectorEngine`.
   - When an observation triggers an anomaly rule, a `ChainEvent` is emitted and stored in thread-safe bounded memory (retaining up to `OBSCHAIN_EVENT_STORE_LIMIT` events).
4. **Live Streaming Broadcast**:
   - Detected events are broadcast instantaneously to connected browser clients over Axum WebSocket (`GET /api/v1/ws`).

---

## Active Detectors

- **`LargeTransactionDetector`**: Emits `EventType::LargeTransfer` when an on-chain transaction exceeds `OBSCHAIN_LARGE_TX_THRESHOLD_SATS` (default: 10,000,000,000 sats / 100 BTC). Categorizes severity into `Medium`, `High`, or `Critical`.
- **`LongBlockIntervalDetector`**: Emits `EventType::LongBlockInterval` when elapsed time between sequential blocks exceeds `OBSCHAIN_LONG_BLOCK_INTERVAL_SECONDS` (default: 1800 seconds / 30 minutes). Validates non-monotonic timestamps.

---

## Getting Started

### Prerequisites

- Rust 1.85+ (stable toolchain)
- Cargo

### Configuration

Copy the example environment file:

```bash
cp .env.example .env
```

Key environment variables:

| Variable | Default | Description |
|---|---|---|
| `OBSCHAIN_HOST` | `127.0.0.1` | API server listen host |
| `OBSCHAIN_PORT` | `8080` | API server listen port |
| `MEMPOOL_API_URL` | `https://mempool.space/api` | Mempool.space REST base endpoint |
| `MEMPOOL_WS_URL` | `wss://mempool.space/api/v1/ws` | Mempool.space WebSocket stream endpoint |
| `OBSCHAIN_LARGE_TX_THRESHOLD_SATS` | `10000000000` | Large transfer detector threshold (100 BTC) |
| `OBSCHAIN_LONG_BLOCK_INTERVAL_SECONDS` | `1800` | Block interval alert threshold (30 minutes) |
| `OBSCHAIN_EVENT_STORE_LIMIT` | `10000` | Maximum recent events in circular memory store |
| `OBSCHAIN_MOCK_FEED` | `false` | Run live ingestion (`false`) or mock demo data (`true`) |

### Running ObsChain

```bash
cargo run --bin obschain
```

The server binds to `http://127.0.0.1:8080` by default and immediately initiates live Bitcoin observation.

---

## API & WebSocket Endpoints

- `GET /health` - Service health status
- `GET /api/v1/status` - Live network telemetry, tip height, active sources, and detector metrics
- `GET /api/v1/events` - Paginated list of real detected on-chain anomalies
- `GET /api/v1/events/:id` - Detailed observation payload for a specific event
- `GET /api/v1/incidents` - Active and historical security incident dossiers
- `GET /api/v1/incidents/:id` - Incident breakdown with evidence and facts
- `GET /api/v1/ws` - **ObsChain Live Stream WebSocket**: Broadcasts newly detected `ChainEvent`s to frontends in real-time

### Connecting to the Live WebSocket Feed

Connect any WebSocket client to `ws://localhost:8080/api/v1/ws`:

```javascript
const ws = new WebSocket("ws://localhost:8080/api/v1/ws");

ws.onmessage = (event) => {
  const chainEvent = JSON.parse(event.data);
  console.log("Observed event:", chainEvent.event_type, chainEvent.title);
};
```

---

## Known Limitations

- **Mempool.space Dependency**: Initial live observation relies on mempool.space REST and WebSocket APIs. Direct validation against a local Bitcoin Core full node (RPC/ZMQ) is in development.
- **In-Memory Retention**: Recent events are buffered in bounded circular memory (`OBSCHAIN_EVENT_STORE_LIMIT`). Persistent PostgreSQL storage will be hooked in a subsequent phase.

---

## Testing & Quality

Run full workspace checks:

```bash
cargo fmt --check
cargo check --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo audit
```

---

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
