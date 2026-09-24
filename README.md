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

### Configuration

Copy the example environment file:

```bash
cp .env.example .env
```

### Running the API Server

```bash
cargo run --bin obschain
```

The server binds to `http://127.0.0.1:8080` by default.

### Endpoints

- `GET /health` - Service health status
- `GET /api/v1/status` - Engine status, network telemetry, and active detectors
- `GET /api/v1/events` - Paginated list of detected on-chain anomalies
- `GET /api/v1/events/:id` - Detailed observation payload for a specific event
- `GET /api/v1/incidents` - Active and historical security incident dossiers
- `GET /api/v1/incidents/:id` - Complete incident breakdown with evidence and facts

---

## Testing & Quality

Run full workspace checks:

```bash
cargo fmt --check
cargo check --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

---

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
