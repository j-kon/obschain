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

- **`DormantCoinDetector`**: Emits `EventType::DormantCoinsMoved` when Bitcoin UTXOs dormant for 5+ years are spent (`OBSCHAIN_DORMANT_MIN_AGE_DAYS`, default: 1,825 days, `OBSCHAIN_DORMANT_MIN_VALUE_SATS`, default: 1 BTC). Derives age strictly from historical confirmation context (`SpentOutputContext`), calculating Coin Age Destroyed in satoshi-days without floating-point overflow. Classifies outputs into `Dormant`, `VeryOld`, `Ancient`, or `EarlyBitcoin` (<2011 cutoff).
- **`ConsolidationDetector`**: Emits `EventType::Consolidation` when transactions merge many inputs into few outputs (`OBSCHAIN_CONSOLIDATION_MIN_INPUTS`, default: 20 inputs into <= 5 outputs). Avoids false-positives on batch payouts or CoinJoins.
- **`FanOutDetector`**: Emits `EventType::FanOut` when transactions distribute funds across an unusually high number of outputs (`OBSCHAIN_FANOUT_MIN_OUTPUTS`, default: 50 outputs).
- **`ExtremeFeeDetector`**: Emits `EventType::ExtremeFee` when a transaction incurs extreme absolute fees (`OBSCHAIN_EXTREME_FEE_SATS`, default: 0.1 BTC) or extreme fee rates (`OBSCHAIN_EXTREME_FEE_RATE_SAT_VB`, default: 200 sat/vB).
- **`RbfDetector`**: Emits `EventType::TransactionReplacement` upon observing confirmed transaction replacements in the mempool. Reports fee delta and percentage increase neutrally without assuming malicious intent.
- **`LargeTransactionDetector`**: Emits `EventType::LargeTransfer` when an on-chain transaction exceeds `OBSCHAIN_LARGE_TX_THRESHOLD_SATS` (default: 10,000,000,000 sats / 100 BTC). Categorizes severity into `Medium`, `High`, or `Critical`.
- **`LongBlockIntervalDetector`**: Emits `EventType::LongBlockInterval` when elapsed time between sequential blocks exceeds `OBSCHAIN_LONG_BLOCK_INTERVAL_SECONDS` (default: 1800 seconds / 30 minutes). Validates non-monotonic timestamps.

> **Important Boundary**: Anomaly detectors evaluate transaction structure and historical confirmation. They describe observed blockchain mechanics, not wallet identity, entity attribution, or human intent.

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
| `OBSCHAIN_DORMANT_MIN_AGE_DAYS` | `1825` | Dormant coin detector age threshold (5 years) |
| `OBSCHAIN_DORMANT_MIN_VALUE_SATS` | `100000000` | Dormant coin minimum value threshold (1 BTC) |
| `OBSCHAIN_CONSOLIDATION_MIN_INPUTS`| `20` | Consolidation minimum inputs threshold |
| `OBSCHAIN_CONSOLIDATION_MAX_OUTPUTS`| `5` | Consolidation maximum outputs threshold |
| `OBSCHAIN_FANOUT_MIN_OUTPUTS` | `50` | Fan-out minimum outputs threshold |
| `OBSCHAIN_EXTREME_FEE_SATS` | `10000000` | Extreme fee absolute threshold (0.1 BTC) |
| `OBSCHAIN_EXTREME_FEE_RATE_SAT_VB` | `200.0` | Extreme fee rate threshold (sat/vB) |
| `OBSCHAIN_UTXO_CACHE_LIMIT` | `50000` | In-memory historical UTXO cache capacity |
| `OBSCHAIN_UTXO_CACHE_TTL_SECONDS` | `3600` | Historical UTXO cache TTL (1 hour) |
| `OBSCHAIN_MAX_INPUT_ENRICHMENT` | `500` | Maximum input lookups per transaction |
| `OBSCHAIN_UTXO_LOOKUP_CONCURRENCY`| `16` | Bounded concurrency for historical HTTP lookups |
| `OBSCHAIN_DEDUP_CAPACITY` | `10000` | Deterministic event deduplication capacity |
| `OBSCHAIN_LARGE_TX_THRESHOLD_SATS` | `10000000000` | Large transfer detector threshold (100 BTC) |
| `OBSCHAIN_LONG_BLOCK_INTERVAL_SECONDS` | `1800` | Block interval alert threshold (30 minutes) |
| `OBSCHAIN_EVENT_STORE_LIMIT` | `10000` | Maximum recent events in circular memory store |
| `OBSCHAIN_ACTIVITY_STORE_LIMIT` | `10000` | Maximum recent incident activities in circular memory store |
| `OBSCHAIN_INCIDENT_FOLLOW_DEPTH` | `3` | Maximum hop depth for tracking UTXO descendants (bounded 1-5) |
| `OBSCHAIN_MOCK_FEED` | `false` | Run live ingestion (`false`) or mock demo data (`true`) |

### Running ObsChain

```bash
cargo run --bin obschain
```

The server binds to `http://127.0.0.1:8080` by default and immediately initiates live Bitcoin observation.

---

## API & WebSocket Endpoints

- `GET /health` - Service health status
- `GET /api/v1/status` - Live network telemetry, tip height, active sources, detector metrics, and watch engine metrics
- `GET /api/v1/events` - Paginated list of real detected on-chain anomalies
- `GET /api/v1/events/:id` - Detailed observation payload for a specific event
- `GET /api/v1/incidents` - Active and historical security incident dossiers
- `GET /api/v1/incidents/:id` - Complete incident dossier (summary, status, recovery, evidence, timeline, graph) by Case ID (`OC-2026-0001`) or UUID
- `GET /api/v1/incidents/:id/timeline` - Chronological incident milestones with evidence and transaction references
- `GET /api/v1/incidents/:id/evidence` - Evidence items with strict provenance classification
- `GET /api/v1/incidents/:id/graph` - Forensic relationship graph (nodes & typed edges) for interactive UI visualization
- `GET /api/v1/incidents/:id/activity` - Incident-specific activity feed (filterable by `activity_type`, `correlation_strength`, `min_confidence`, `limit`)
- `GET /api/v1/incidents/:id/watch-targets` - Public metadata for active watch targets associated with an incident (internal parameters redacted)
- `GET /api/v1/incident-activity` - Global cross-incident activity feed
- `GET /api/v1/ws` - **ObsChain Live Stream WebSocket**: Broadcasts newly detected `ChainEvent`s, `IncidentActivity`, and `IncidentAlert` in real-time

### Connecting to the Live WebSocket Feed

Connect any WebSocket client to `ws://localhost:8080/api/v1/ws`.

The stream broadcasts tagged JSON envelopes:
- `chain_event`: General network anomaly event (also backwards-compatible with flat `ChainEvent` fields).
- `incident_activity`: Verified correlation with a monitored security incident.
- `incident_alert`: High-priority alert triggered by significant incident-linked movement.

```javascript
const ws = new WebSocket("ws://localhost:8080/api/v1/ws");

ws.onmessage = (event) => {
  const msg = JSON.parse(event.data);
  if (msg.type === "incident_activity" || msg.type === "incident_alert") {
    console.log("Incident alert:", msg.type, msg.data.case_id, msg.data.title);
  } else {
    // Chain event (supports both msg.data and legacy flat properties)
    const chainEvent = msg.data || msg;
    console.log("Observed event:", chainEvent.event_type, chainEvent.title);
  }
};
```

---

## Storage Architecture & PostgreSQL Persistence

ObsChain supports two interchangeable storage backends behind unified repository traits:

1. **`memory` (Default)**: In-memory bounded circular storage using thread-safe `RwLock<VecDeque>` and hash maps. Ideal for fast local development, unit tests, and CI without database infrastructure.
2. **`postgres` (Durable)**: Production-grade persistent storage powered by SQLx, connection pooling, and 17 normalized relational tables. Preserves all chain events, incident intelligence, recovery snapshots, watch targets, on-chain activities, and alerts across restarts.

### Switching Storage Backend

Set via environment variables:

```bash
# In-Memory mode (default)
export OBSCHAIN_STORAGE_BACKEND=memory

# PostgreSQL mode
export OBSCHAIN_STORAGE_BACKEND=postgres
export DATABASE_URL=postgres://postgres:postgrespassword@localhost:5433/obschain
export OBSCHAIN_DB_MAX_CONNECTIONS=10
export OBSCHAIN_DB_MIN_CONNECTIONS=1
export OBSCHAIN_DB_ACQUIRE_TIMEOUT_SECONDS=5
```

> **Safety Rule**: If `OBSCHAIN_STORAGE_BACKEND=postgres` is set but the database is unreachable or migrations fail, ObsChain **fails startup immediately with a clear error**. It does **NOT** silently fall back to in-memory mode, preventing operators from mistakenly assuming durability.

### Local PostgreSQL Setup with Docker Compose

Start the PostgreSQL service in the background:

```bash
docker compose up -d postgres
```

The service is pre-configured on port `5433` (avoiding local standard port 5432 conflicts) with database `obschain`.

### Running Migrations

Database migrations in `migrations/` are applied automatically by the daemon on startup via SQLx embedded migrations (`sqlx::migrate!`). You can also execute them manually using the SQLx CLI:

```bash
cargo install sqlx-cli --no-default-features --features rustls,postgres
sqlx migrate run
```

### Persistence Guarantees

- **Event Idempotency**: Anomaly detections and activities employ deterministic deduplication keys and `ON CONFLICT` database constraints. Replaying observations will never create duplicate records.
- **Historical Recovery Preservation**: Incident recovery state is append-only (`incident_recovery_snapshots`). Historical recovery figures are never overwritten, maintaining full audit trails.
- **Rule 19 (Movement != Recovery)**: Observed on-chain activity movements never automatically mutate incident recovery balances. Recovery balances change only via explicit verified recovery updates.
- **Satoshi Precision**: All satoshi values are validated against signed 64-bit bounds (`BIGINT`) with checked Rust `u64` <-> SQL `i64` conversions.
- **Credential Protection**: Database passwords and connection URIs are strictly redacted from logs, status endpoints, and panic payloads.

---

## Known Limitations

- **Mempool.space Dependency**: Initial live observation relies on mempool.space REST and WebSocket APIs. Direct validation against a local Bitcoin Core full node (RPC/ZMQ) is in development.

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
