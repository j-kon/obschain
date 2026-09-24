# ObsChain Engineering Roadmap

## Phase 1: Foundation & Clean Separation (Current)
- [x] Independent repository layout for `obschain` (Rust) and `obschain-web` (React/TypeScript).
- [x] Workspace crate structure (`obschain-core`, `detectors`, `ingest`, `incidents`, `intelligence`, `storage`).
- [x] Core domain models with provenance classifications.
- [x] Foundational detectors:
  - `LargeTransactionDetector`
  - `LongBlockIntervalDetector`
- [x] Axum REST API with `/health`, `/status`, `/events`, `/incidents`.
- [x] SQLx PostgreSQL schema and migrations.
- [x] Dedicated CI and linting pipelines.

## Phase 2: Live Bitcoin Network Ingestion
- [ ] Connect live mempool.space WebSocket feed for realtime mempool delta observations.
- [ ] Implement Bitcoin Core RPC poller for block verification and reorg checks.
- [ ] Implement Bitcoin Core ZMQ subscriber for low-latency block/tx announcements.
- [ ] Add dormant UTXO movement detector (e.g. UTXOs untouched > 5 years).
- [ ] Add fee spike and fee anomaly detectors.

## Phase 3: Advanced Anomaly Detectors & Analysis
- [ ] Consolidation transaction detector (many inputs to single output).
- [ ] Fan-out transaction detector (single input to hundreds of outputs).
- [ ] RBF replacement sequence tracking.
- [ ] Reorg detection and notification engine.
- [ ] Mining pool coinbase tag anomaly detection.

## Phase 4: Incident Investigation Suite
- [ ] Automated incident dossier generation.
- [ ] Interactive transaction flow graph visualizer with Cytoscape/D3 in `obschain-web`.
- [ ] Public cryptographic proof-of-ownership verification tool.
- [ ] Exportable ObsChain Observed Reports (PDF / JSON-LD).
