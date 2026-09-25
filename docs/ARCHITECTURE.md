# ObsChain Architecture Specification

## Overview

ObsChain is an open-source Bitcoin network observation, anomaly-detection, incident-analysis, and on-chain intelligence engine.

It observes Bitcoin blocks, transactions, and mempools, enriches transactions with historical UTXO confirmation context, executes modular detectors with failure isolation, and broadcasts structured chain events to client dashboards.

---

## Architectural Principles

1. **Strict Cryptographic Provenance**
   - Blockchain transactions, script outpoints, and block headers are mathematically verifiable facts.
   - Historical UTXO enrichment preserves exact source origin (`mempool_rest`, `bitcoin_core`, etc.).
   - Heuristics describe transaction structure and pattern mechanics—they NEVER imply verified identity or human intent.

2. **Decoupled Ingestion Hierarchy**
   - Ingestion is abstracted over multiple providers (`mempool.space` REST/WS, Bitcoin Core JSON-RPC, Bitcoin Core ZeroMQ).
   - Bitcoin Core is designed as the ultimate sovereign source of truth, with public API feeds available as convenience layers.

3. **Bounded Resources & Concurrency Safety**
   - No unbounded `HashMap` or memory allocations.
   - Channel backpressure (`mpsc(1000)`).
   - UTXO cache bounds (`UtxoCache`) with FIFO eviction and TTL.
   - Throttled concurrency on historical HTTP lookups via `tokio::sync::Semaphore`.
   - Per-transaction input enrichment limits to protect against huge consolidation transactions.

4. **Detector Failure Isolation**
   - Panics in individual detectors are captured via `std::panic::catch_unwind` and safely isolated without crashing the observation pipeline or terminating other detectors.
   - Failed UTXO lookups leave `historical_utxo: None`, allowing normal non-dormant detectors to evaluate unimpeded.

5. **Deterministic Event Deduplication**
   - Eliminates duplicate event storms during WebSocket reconnection or feed re-sync.
   - Keys events deterministically on `event_type + txid` or `event_type + block_hash`.

6. **Decimal-Safe Bitcoin Arithmetic**
   - Internal accounting uses exact integer satoshis (`u64`).
   - Coin Age Destroyed is computed in satoshi-days using `u128` arithmetic to prevent integer overflow.
   - Parsing avoids binary floating-point representation (`f64`).

---

## System Topology

```text
 ┌─────────────────────────────────────────────────────────────┐
 │                     INGESTION LAYER                         │
 │                                                             │
 │   mempool.space WS          mempool.space REST              │
 │   - blocks                  - tip synchronization           │
 │   - live txs                - historical /tx/:txid lookup   │
 │   - rbf replacements                                        │
 └──────────────┬───────────────────────────────┬──────────────┘
                │                               │
                ▼                               ▼
 ┌─────────────────────────────────────────────────────────────┐
 │                 UTXO ENRICHMENT LAYER                       │
 │                                                             │
 │   TransactionEnricher                                       │
 │   ├── UtxoCache (Bounded, TTL-aware, thread-safe RwLock)    │
 │   ├── Outpoint TXID Deduplication                           │
 │   ├── Throttled Concurrency (Semaphore)                     │
 │   └── Max Input Limit Bounds                                │
 └──────────────────────────────┬──────────────────────────────┘
                                │ Enriched Observations
                                ▼
 ┌─────────────────────────────────────────────────────────────┐
 │                   DETECTOR ENGINE                           │
 │                                                             │
 │   ├── LargeTransactionDetector                              │
 │   ├── LongBlockIntervalDetector                             │
 │   ├── DormantCoinDetector                                   │
 │   ├── ConsolidationDetector                                 │
 │   ├── FanOutDetector                                        │
 │   ├── ExtremeFeeDetector                                    │
 │   └── RbfDetector                                           │
 │                                                             │
 │   [Panic Isolation: std::panic::catch_unwind]               │
 └──────────────────────────────┬──────────────────────────────┘
                                │ Raw ChainEvents
                                ▼
 ┌─────────────────────────────────────────────────────────────┐
 │                EVENT DEDUPLICATION FILTER                   │
 │                                                             │
 │   EventDeduplicator (Deterministic key + Bounded TTL)       │
 └──────────────────────────────┬──────────────────────────────┘
                                │ Fresh ChainEvents
                                ▼
 ┌─────────────────────────────────────────────────────────────┐
 │                 STORAGE & BROADCAST LAYER                   │
 │                                                             │
 │   ├── Bounded In-Memory Event Store (Circular VecDeque)     │
 │   ├── PostgreSQL Persistent Store (Optional)                │
 │   ├── Live WebSocket Broadcaster (/api/v1/ws)               │
 │   └── REST API Endpoints (/api/v1/status, /events, ...)     │
 └─────────────────────────────────────────────────────────────┘
```

---

## Workspace Crates

```text
crates/
├── obschain-core/          # Domain primitives, observations, events, amount math, incident models, provenance
├── obschain-detectors/     # Detection logic (Dormant, Consolidation, FanOut, ExtremeFee, RBF, etc.)
├── obschain-ingest/        # Multi-source ingestion clients, UtxoCache, and TransactionEnricher
├── obschain-incidents/     # Incident intelligence, canonical case seeders, validation invariants, builder
├── obschain-intelligence/  # Graph modeling, clustering models, report generator
└── obschain-storage/       # PostgreSQL / InMemory repository implementations

---

## Incident Intelligence Architecture

Incident intelligence sits cleanly on top of the observation architecture without altering the detector pipeline.

```text
 ┌─────────────────────────────────────────────────────────────┐
 │                INCIDENT INTELLIGENCE SUBSYSTEM              │
 │                                                             │
 │   ├── Domain Models (Incident, Evidence, Timeline, Sources) │
 │   ├── Provenance Verification Engine (Validation Invariants)│
 │   ├── Historical Snapshot State & Append-Only Updates       │
 │   ├── Safe Fund Tracking (Checked Integer Satoshi Math)     │
 │   ├── Relationship Graph (Nodes & Neutral Typed Edges)      │
 │   └── Canonical Seed Cases (e.g. Liquid Network 2026)       │
 └──────────────────────────────┬──────────────────────────────┘
                                │ Exposes
                                ▼
 ┌─────────────────────────────────────────────────────────────┐
 │                      INCIDENT API                           │
 │                                                             │
 │   ├── GET /api/v1/incidents                                 │
 │   ├── GET /api/v1/incidents/:id                             │
 │   ├── GET /api/v1/incidents/:id/timeline                    │
 │   ├── GET /api/v1/incidents/:id/evidence                    │
 │   └── GET /api/v1/incidents/:id/graph                       │
 └─────────────────────────────────────────────────────────────┘
```

---

## Live Incident Watch & Chain Correlation Architecture

The Live Incident Watch Engine correlates live Bitcoin blockchain and mempool transactions against monitored incident targets without relying on subjective entity inference or wallet clustering.

```text
               Enriched Normalized Observations (Blocks & Txs)
                                     │
              ┌──────────────────────┴──────────────────────┐
              ▼                                             ▼
   ┌──────────────────────┐                     ┌────────────────────────┐
   │ Anomaly Detectors    │                     │  IncidentWatchEngine   │
   │ (7 Generic Detectors)│                     │  ├── Outpoint Index    │
   │ - Dormant, FanOut    │                     │  ├── ScriptPubKey Index│
   │ - Extreme Fee, RBF   │                     │  ├── Address Index     │
   │ - Large Tx, Interval │                     │  ├── TxID Index        │
   │                      │                     │  └── Descendant DAG    │
   └──────────┬───────────┘                     └───────────┬────────────┘
              │ ChainEvent                                  │ IncidentActivity / Alert
              ▼                                             ▼
   ┌──────────────────────┐                     ┌────────────────────────┐
   │  Event Deduplicator  │                     │ In-Memory Activity Rep │
   └──────────┬───────────┘                     └───────────┬────────────┘
              │                                             │
              └──────────────────────┬──────────────────────┘
                                     ▼
                      ┌─────────────────────────────┐
                      │ WebSocket Broadcaster       │
                      │ (/api/v1/ws)                │
                      │ - type: chain_event         │
                      │ - type: incident_activity   │
                      │ - type: incident_alert      │
                      └─────────────────────────────┘
```

### 1. In-Memory O(1) Indexing
The engine maintains four thread-safe lookup tables:
- **Outpoint Index**: `(txid, vout)` mapping for exact unspent watched outputs.
- **ScriptPubKey Index**: Hex-encoded script mapping for deterministic script executions.
- **Address Index**: Standard Bitcoin address string matching (preserving heuristic boundary for heuristic addresses).
- **TxID Index**: Transaction reference mapping (e.g., OP_RETURN communication transactions).

### 2. Bounded Dynamic Descendant Tracking
- When a watched UTXO is spent, the engine registers resulting non-OP_RETURN outputs as `DescendantOutpoint` targets.
- Descendants are tagged with `depth = parent_depth + 1`.
- Tracking is strictly bounded to `OBSCHAIN_INCIDENT_FOLLOW_DEPTH` (default 3, clamped between 1 and 5).
- Total tracked descendants are capped in a bounded LRU/FIFO buffer to prevent memory exhaustion attacks.

### 3. Epistemological Decoupling
- **Recovery Immutability**: On-chain fund movement proves cryptographic transfer, NOT fund recovery. `incident.recovery.recovered_sats` is NEVER modified automatically by the watch engine.
- **Severity Boundaries**: Heuristic cluster activity alerts are capped at `Medium` severity, preventing false alarm inflation.

---

## Pipeline Telemetry & Observability

The pipeline tracks key metrics exported in `StatusResponse.metrics`:
- `transactions_observed`: Total transaction observations processed
- `blocks_observed`: Total block observations processed
- `transactions_enriched`: Transactions with at least one historical UTXO resolved
- `utxo_lookup_failures`: Upstream HTTP lookup errors or missing historical transactions
- `cache_hits`: In-memory UTXO cache hits
- `cache_misses`: In-memory UTXO cache misses requiring REST fetch
- `events_generated`: Total unique chain events detected
- `events_deduplicated`: Replayed or duplicate events dropped by the filter
- `active_incident_watchers`: Number of active incidents currently under live observation
- `incident_watch_targets`: Total active watch targets loaded into engine indices
- `incident_activities_detected`: Total incident correlation activities detected
- `incident_alerts_emitted`: High-priority incident alerts emitted and broadcasted
- `storage_write_errors`: Count of transient or exhausted storage write errors

---

## Durable PostgreSQL Storage Architecture (Phase 4B)

### 1. Repository Abstraction Layer

Storage is fully decoupled from the pipeline via asynchronous repository traits defined in `obschain-storage`:

```rust
#[async_trait]
pub trait EventRepository: Send + Sync {
    async fn save_event(&self, event: &ChainEvent) -> Result<(), StorageError>;
    async fn get_event(&self, id: Uuid) -> Result<Option<ChainEvent>, StorageError>;
    async fn list_events(&self, limit: usize, offset: usize) -> Result<Vec<ChainEvent>, StorageError>;
}

#[async_trait]
pub trait IncidentRepository: Send + Sync {
    async fn save_incident(&self, incident: &Incident) -> Result<(), StorageError>;
    async fn get_incident(&self, id: Uuid) -> Result<Option<Incident>, StorageError>;
    async fn get_incident_by_case_id(&self, case_id: &str) -> Result<Option<Incident>, StorageError>;
    async fn list_incidents(&self, limit: usize, offset: usize) -> Result<Vec<Incident>, StorageError>;
}

#[async_trait]
pub trait WatchTargetRepository: Send + Sync {
    async fn save_watch_target(&self, target: &WatchTarget) -> Result<(), StorageError>;
    async fn list_watch_targets_by_incident(&self, incident_id: Uuid) -> Result<Vec<WatchTarget>, StorageError>;
}

#[async_trait]
pub trait IncidentActivityRepository: Send + Sync {
    async fn save_activity(&self, activity: &IncidentActivity) -> Result<(), StorageError>;
    async fn list_activities(&self, incident_id: Option<Uuid>, status: Option<ActivityStatus>, limit: usize, offset: usize) -> Result<Vec<IncidentActivity>, StorageError>;
}

#[async_trait]
pub trait IncidentAlertRepository: Send + Sync {
    async fn save_alert(&self, alert: &IncidentAlert) -> Result<(), StorageError>;
    async fn list_alerts(&self, incident_id: Option<Uuid>, limit: usize, offset: usize) -> Result<Vec<IncidentAlert>, StorageError>;
}
```

The unified `Storage` enum wraps either `InMemoryStorage` or `PostgresStorage`, allowing daemon and API handlers to interact exclusively with domain contracts.

### 2. Normalized Relational Schema

The PostgreSQL backend organizes incident intelligence into 17 relational tables:

1. `chain_events`: Anomaly detection events with deterministic deduplication.
2. `incidents`: Core dossier metadata (UUID, case_id, title, status, severity, recovery balances).
3. `incident_recovery_snapshots`: Append-only historical recovery milestones.
4. `incident_sources`: External disclosures, research papers, and advisories with URL normalization.
5. `incident_evidence`: Provenance-classified evidence hierarchy with chain references.
6. `incident_transactions`: Exploitation and remediation transaction references.
7. `incident_blocks`: Chain block references.
8. `incident_entities`: Entities and custodian organizations.
9. `incident_messages`: Canonical on-chain communication messages (OP_RETURN).
10. `incident_timeline`: Chronological incident events.
11. `incident_technical_findings`: Cryptographic, script, or structural analysis findings.
12. `incident_updates`: Append-only investigation updates.
13. `incident_graph_nodes`: Forensic relationship graph nodes.
14. `incident_graph_edges`: Directional typed graph relationships with referential integrity.
15. `incident_watch_targets`: Monitored outpoints, scripts, transactions, and addresses with deterministic UUIDs.
16. `incident_activities`: Observed on-chain activity correlated with watched targets.
17. `incident_alerts`: High-priority alert emissions for critical incident movements.

### 3. Epistemological Invariant: Rule 19

ObsChain enforces strict separation between on-chain movement and financial recovery:
- Moving incident funds proves cryptographic possession transfer, **not** that funds have been legally or operationally recovered.
- `IncidentActivity` insertion **never** updates `incident_recovery_snapshots` or `incident.recovery`.
- Recovery figures change solely when an explicit `RecoverySummary` or append-only update is recorded from authoritative disclosures.

### 4. Write Failure & Retry Policy

When live Bitcoin ingestion encounters transient database write failures:
- The daemon retries up to 3 times using exponential backoff (100ms, 200ms, 400ms).
- If retries are exhausted, the failure increments `metrics.storage_write_errors` and logs an error, while allowing the live pipeline to maintain ingestion without crash loops.
- Read failures in API handlers return `503 Service Unavailable` rather than false `404 Not Found`.

### 5. Performance & Telemetry Caching

- `GET /api/v1/status` avoids expensive `SELECT COUNT(*)` queries on hot paths by leveraging cheap atomic counters pre-populated at startup and incremented on writes.
- All query endpoints enforce clamped pagination (`default: 50`, `max: 200`).
- Thoughtful composite indexes are placed on `(detected_at DESC)`, `(incident_id, observed_at DESC)`, and `(incident_id, as_of_timestamp DESC)`.

---

## Sovereign Bitcoin Core Ingestion Architecture (Phase 5)

ObsChain establishes a locally operated Bitcoin Core full node as the primary, authoritative data source for all block and transaction observations, while retaining public API sources (`mempool.space`) as supplementary witnesses and enrichment fallbacks.

```text
 ┌─────────────────────────────────────────────────────────────┐
 │                  SOVEREIGN INGESTION CORE                   │
 │                                                             │
 │   BitcoinCoreRpcClient           BitcoinZmqSubscriber       │
 │   ├── Safe URL Validation        ├── rawtx (ZeroMQ TCP)     │
 │   ├── Cookie Auth (.cookie)      ├── rawblock (ZeroMQ TCP)  │
 │   ├── 401 Cookie Auto-Refresh    └── sequence (ZeroMQ TCP)  │
 │   ├── Network Validation                │                   │
 │   └── Capability Inspector              │                   │
 └─────────────────┬───────────────────────┼───────────────────┘
                   │                       │
                   ▼                       ▼
 ┌─────────────────────────────────────────────────────────────┐
 │                 BITCOIN CORE COORDINATOR                    │
 │                                                             │
 │   ├── Source Health State Machine (Connecting/Connected/...)│
 │   ├── Tip Continuity & Parent Hash Verification             │
 │   ├── Reorganization Pipeline (Ancestor Trace)              │
 │   ├── Bounded Gap Reconciliation (Max Replay Limit)         │
 │   └── Source Reconciliation & Provenance Preservation       │
 └──────────────────────────────┬──────────────────────────────┘
                                │
                                ▼
 ┌─────────────────────────────────────────────────────────────┐
 │               MULTI-WITNESS OBSERVATION PIPELINE            │
 │                                                             │
 │   ObservationWitness                                        │
 │   ├── Primary Witness: BitcoinCoreZmq / BitcoinCoreRpc      │
 │   ├── Secondary Witness: MempoolSpaceWs / MempoolSpaceRest  │
 │   └── Deduplication: Multi-Witness Accumulation on Event    │
 └─────────────────────────────────────────────────────────────┘
```

### 1. Bitcoin Core RPC Adapter (`BitcoinCoreRpcClient`)

The RPC adapter implements a robust, typed JSON-RPC 1.0 client tailored for long-running daemon operations:
- **Security Boundaries**: Strictly accepts only `http://` and `https://` schemas; rejects `file://` or arbitrary URIs to prevent SSRF vulnerabilities. Redacts credentials and cookie contents from logs and API payloads.
- **Authentication**: Supports static credentials (`BITCOIN_RPC_USER` / `BITCOIN_RPC_PASSWORD`) or dynamic cookie authentication (`BITCOIN_COOKIE_FILE`). On receiving `401 Unauthorized` during cookie operation, the client invalidates its cached token, re-reads the cookie file from disk, and retries once automatically.
- **Network Validation**: Inspects `getblockchaininfo.chain` against the configured network (`OBSCHAIN_BITCOIN_NETWORK`). Rejects startup if a mismatch is detected (e.g., node reports `testnet` when application expects `bitcoin`).
- **Capability Inspection (`BitcoinNodeCapabilities`)**: Checks node version, verification progress, IBD status, pruning mode, and txindex availability (tested via genesis transaction lookup).
- **Core RPC Methods**: Implements typed bindings for `getblockchaininfo`, `getnetworkinfo`, `getmempoolinfo`, `getrawmempool`, `getrawtransaction`, `getmempoolentry`, `getblockhash`, `getblock`, `getblockheader`, `gettxout`, and `getchaintips`.

### 2. Bitcoin Core ZeroMQ Adapter (`BitcoinZmqSubscriber`)

The ZMQ subscriber provides ultra-low-latency real-time streaming using pure-Rust asynchronous Tokio sockets (`zeromq`):
- **Topics Subscribed**:
  - `rawtx`: Streams unconfirmed transaction hexes directly as they enter the node's memory pool.
  - `rawblock`: Streams raw serialized block bytes immediately upon consensus validation by Bitcoin Core.
  - `sequence`: Streams 1-byte ASCII tagged sequence notifications:
    - `'C'`: Block connected (includes 8-byte LE height).
    - `'D'`: Block disconnected (indicates reorganization; includes 8-byte LE height).
    - `'A'`: Transaction added to mempool (includes 8-byte LE sequence counter).
    - `'R'`: Transaction removed from mempool (e.g. replaced or evicted; includes 8-byte LE sequence counter).
- **Memory & Parsing Safety**: Limits raw payload parsing to 16 MB frame size. Payload deserialization errors are safely handled without panics.
- **Connection Lifecycle**: Employs bounded exponential backoff reconnection (500ms to 10s).

### 3. Source Reconciliation & Priority Hierarchy

ObsChain evaluates observations across observers using deterministic source priority:
1. **Authoritative Local Chain Data**: Bitcoin Core validated chain data (`BitcoinCoreZmq` / `BitcoinCoreRpc`).
2. **Authoritative Local Mempool Data**: Bitcoin Core mempool observations (`sequence` / `rawtx`).
3. **Supplementary Public Ingestion**: `mempool.space` WebSocket and REST feeds.

Observations from public feeds never overwrite authoritative local node data. Instead, differences in tip height or transaction arrival timestamps are captured as secondary observer telemetry.

### 4. Multi-Witness Architecture (`ObservationWitness`)

Rather than dropping duplicate events when a transaction is observed across multiple network observer nodes, ObsChain records an `ObservationWitness` entry for each witness:
- Stored on `ChainEvent.witnesses`.
- Retains observer timestamp, source origin (`ObservationSource`), and verification status.
- Preserves the structural foundation for cross-node propagation analysis and single-observer anomaly isolation.

### 5. Reorganization Pipeline & ReorgDetector

Chain reorganizations are detected and tracked as first-class domain occurrences:
- **Tip Discontinuity**: When a new block arrives whose `prev_blockhash` does not equal the current known tip hash, ObsChain initiates chain tip investigation.
- **Ancestor Tracing**: Uses `getblockheader` and `getchaintips` to trace backwards and locate the common ancestor between the old tip and new tip.
- **`ReorgObservation`**: Emitted with `old_tip_hash`, `new_tip_hash`, `depth`, `common_ancestor_hash`, `disconnected_blocks`, and `connected_blocks`.
- **`ReorgDetector`**: Maps depth deterministically to event severity:
  - `depth <= 1`: `Low` severity (stale/competing block).
  - `depth == 2`: `Medium` severity.
  - `depth 3..=5`: `High` severity.
  - `depth >= 6`: `Critical` severity.

### 6. Bounded Gap Reconciliation

If the daemon is offline during Bitcoin Core block production, or after a bitcoind restart:
- The coordinator compares `stored_tip` against node tip height.
- If `gap <= OBSCHAIN_RECONCILE_MAX_BLOCKS` (default 100), the coordinator fetches missed block hashes via RPC `getblockhash` / `getblock`, deserializes them, and feeds them into the normal observation channel.
- If `gap > OBSCHAIN_RECONCILE_MAX_BLOCKS`, the coordinator logs a warning and updates its baseline tip without replaying thousands of blocks on the live stream.

### 7. Sovereign-Only Mode & Privacy Guarantees

When `OBSCHAIN_SOVEREIGN_ONLY=true` is enabled:
- Public WebSocket and REST clients for `mempool.space` are never instantiated.
- `TransactionEnricher` executes strictly against the local UTXO cache and local Bitcoin Core RPC.
- No incident outpoints, watched addresses, or transaction lookups are ever transmitted across the public internet.


