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
