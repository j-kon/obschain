# ObsChain Historical Replay & Bitcoin Research Engine

## 1. Overview

The Historical Replay & Bitcoin Research Engine allows ObsChain to process historical Bitcoin blocks through the **same** normalization, detector, persistence, and intelligence architecture used for live real-time observations.

ObsChain does **not** create a second, divergent detector codebase for historical data. Instead, observations are contextualized through an explicit execution clock (`ObservationContext`) and observation mode (`ObservationMode::HistoricalReplay`), ensuring absolute behavioral equivalence between live operations and historical research.

---

## 2. Core Architecture

The historical replay pipeline follows a clean, single-path flow:

```text
       Bitcoin Core JSON-RPC (or P2P/raw hex)
                         │
                         ▼
             HistoricalReplayEngine
                         │
        ┌────────────────┴────────────────┐
        ▼                                 ▼
 Raw Block Deserialization        Local Block Map (txid -> Tx)
        │                                 │
        ▼                                 ▼
 Transaction Normalization       Historical UTXO Enrichment
        │                                 │
        └────────────────┬────────────────┘
                         ▼
            ObservationContext (Timestamp, Height, Job ID)
                         │
                         ▼
               DetectorEngine Pipeline
         (Replay-Safe Detectors Evaluated)
                         │
                         ▼
             Deduplicated ChainEvents
        (Deterministic UUID v5 Event Identity)
                         │
        ┌────────────────┴────────────────┐
        ▼                                 ▼
PostgreSQL Event Persistence      Incident Watch Engine
 (ON CONFLICT (id) DO UPDATE)     (Historical IncidentActivity)
        │                                 │
        └────────────────┬────────────────┘
                         ▼
            Checkpoints & Status Tracking
         (Persisted Every N Blocks to DB)
```

---

## 3. Replay Job Model & Lifecycle

Each historical replay operation is tracked as a strongly typed `ReplayJob`:

```rust
pub struct ReplayJob {
    pub id: Uuid,
    pub network: BitcoinNetwork,
    pub start_height: u64,
    pub end_height: u64,
    pub current_height: u64,
    pub status: ReplayJobStatus,
    pub source_type: String,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub blocks_processed: u64,
    pub transactions_processed: u64,
    pub events_generated: u64,
    pub error_count: u64,
    pub last_error: Option<String>,
}
```

### Job States

- `PENDING`: Created and awaiting worker acquisition.
- `RUNNING`: Actively processing blocks in sequential order.
- `PAUSED`: Execution paused safely at a block boundary. Can be resumed.
- `COMPLETED`: All blocks in range `[start_height, end_height]` successfully processed and committed.
- `FAILED`: Fatal error encountered (e.g. RPC connection severed, invalid block payload). Checkpointed at last safe block.
- `CANCELLED`: Operator cancelled job. Stopped cleanly at block boundary.

---

## 4. Replay Checkpoints & Crash Safety

Processing large ranges of historical blocks (e.g., 50,000 to 100,000 blocks) may take hours or days. To guarantee crash recovery:

1. **Transactional Boundaries**:
   - Blocks and events are committed to PostgreSQL.
   - Every `OBSCHAIN_REPLAY_CHECKPOINT_INTERVAL` blocks (default 25 blocks) or upon job completion, a `ReplayCheckpoint` record is saved and the `replay_jobs` table is updated.
2. **Resume Behavior**:
   - If the daemon crashes or restarts while processing block `905432`, the last committed checkpoint records `last_completed_height = 905425`.
   - Upon calling resume (or restarting the CLI replay on the same job), execution resumes from `last_completed_height + 1` (block `905426`).
   - Any reprocessed block is safely idempotent; deterministic UUID v5 event IDs prevent duplicate event insertions in PostgreSQL.

---

## 5. Bitcoin Core Historical Source Behavior

ObsChain uses Bitcoin Core JSON-RPC as the authoritative sovereign source of historical truth:

- Block hashes are retrieved via `getblockhash(height)`.
- Full block data is fetched via `getblock(hash, 0)` (raw serialized hex).
- Deserializing raw block bytes locally provides all transactions in their exact on-chain order, eliminating redundant `getrawtransaction` RPC calls for transactions inside the block.

### Pruned Nodes Handling

Before starting a replay job, `HistoricalReplayEngine` inspects `getblockchaininfo`:
- `pruned: bool`
- `pruneheight: Option<u64>`

If the requested `start_height` is below the node's available history:
```text
Requested replay starts at block 500000, but this pruned node only retains history from block 820000.
```
ObsChain returns a clear, explicit error and halts immediately. It **never** silently skips unavailable blocks.

### Txindex Handling

- When full block bytes are fetched, all transaction details are already present in memory.
- For historical input-history enrichment:
  1. ObsChain first searches the ephemeral `block_map` (for intra-block spends).
  2. Next, it queries the bounded in-memory `HistoricalTxCache`.
  3. If unindexed and unavailable in cache, it requests `getrawtransaction` from Bitcoin Core.
  4. If `txindex=0` and previous transactions are pruned or out of mempool, input confirmation context is gracefully marked unresolved (`historical_utxo: None`), allowing detectors that do not require prior UTXO age to continue unimpeded.

---

## 6. Historical UTXO Engine & Enrichment

The `DormantCoinDetector` requires previous-output confirmation context:
- `input txid` and `vout`
- `value_sats`
- `confirmation_height`
- `confirmation_timestamp`

### Enrichment Layers

To resolve historical inputs without producing millions of RPC calls:
1. **Intra-Block Transaction Map**: A temporary `HashMap<Txid, Transaction>` is built for the current block. Any input that spends an output created earlier in the same block is resolved instantaneously in memory. The map is dropped when the block finishes processing.
2. **Bounded Historical Transaction Cache (`HistoricalTxCache`)**:
   - Configurable capacity via `OBSCHAIN_REPLAY_TX_CACHE_LIMIT` (default 100,000 entries).
   - Tracks metrics: `hits`, `misses`, `rpc_lookups`, `evictions`, and `hit_rate_percent`.
   - Prevents memory exhaustion while capturing temporal locality (e.g. outputs spent within subsequent blocks).
3. **Bitcoin Core JSON-RPC**:
   - Fallback lookup for previous transactions not present in memory.
   - Throttled through bounded concurrency to avoid node starvation.

---

## 7. Deterministic Replay Time & Clock Semantics

Detectors in ObsChain evaluate time-sensitive anomalies (e.g., dormant coin age, long block intervals).

### The Historical Clock Rule

**Historical detectors must NEVER use wall-clock `Utc::now()`.**

If a transaction confirmed in 2013 is replayed in 2026:
- **Incorrect (wall-clock)**: Evaluated as 13 years old relative to 2026.
- **Correct (deterministic replay time)**: Evaluated relative to the block's timestamp in 2013.

### Implementation: `ObservationContext`

```rust
pub struct ObservationContext {
    pub mode: ObservationMode,
    pub block_height: Option<u64>,
    pub block_hash: Option<BlockHash>,
    pub block_time: Option<DateTime<Utc>>,
    pub replay_job_id: Option<Uuid>,
    pub network: BitcoinNetwork,
}
```

- When `context.mode == ObservationMode::HistoricalReplay`, `tx.timestamp` is set to the block's actual header timestamp.
- `DormantCoinDetector` calculates dormancy as:
  $$\Delta t = \text{block\_timestamp} - \text{previous\_output\_timestamp}$$
- Coin Age Destroyed (CAD) is computed deterministically:
  $$\text{CAD}_{\text{satoshi-days}} = \frac{\text{value\_sats} \times \Delta t_{\text{seconds}}}{86,400}$$
  All calculations utilize exact integer arithmetic (`u128`).

---

## 8. Detector Availability & Replay Suitability

Certain Bitcoin anomaly detectors require real-time ephemeral network state that is not preserved in the immutable blockchain ledger.

| Detector | Replayable | Requires Mempool | Requires Multi-Observer | Notes |
| :--- | :---: | :---: | :---: | :--- |
| **LargeTransactionDetector** | **Yes** | No | No | Evaluates transaction value against threshold |
| **LongBlockIntervalDetector** | **Yes** | No | No | Evaluates timestamp delta between block $N$ and $N-1$ |
| **DormantCoinDetector** | **Yes** | No | No | Uses block timestamp and historical UTXO age |
| **ConsolidationDetector** | **Yes** | No | No | Evaluates input-to-output fan-in ratio |
| **FanOutDetector** | **Yes** | No | No | Evaluates output fan-out dispersion |
| **ExtremeFeeDetector** | **Yes** | No | No | Evaluates absolute fee and sat/vB rate |
| **RbfDetector** | **No** | **Yes** | No | Replacement-by-Fee requires unconfirmed mempool state; unavailable in confirmed block replay |
| **ReorgDetector** | **No** | No | **Yes** | Active-chain replay follows consensus canonical tip; stale fork blocks are unavailable without specialized sidecar sources |

During historical replay, non-replayable detectors are cleanly bypassed.

---

## 9. Event Identity & Idempotency

Running a replay of blocks `900000 -> 901000` twice must **never** create duplicate `ChainEvents` or inflate incident metrics.

### Deterministic UUID v5 Event Identity

Logical event IDs are generated deterministically using UUID v5 (SHA-1 hashing over a dedicated DNS/ObsChain namespace):

$$\text{Event ID} = \text{UUIDv5}(\text{Namespace}, \text{event\_type} + \text{":"} + (\text{txid} \mid \text{block\_hash} \mid \text{height}))$$

- A large transaction at block `900123` with txid `abc...` will produce the **exact same** UUID v5 whether observed live in 2026 or replayed in 2028.
- PostgreSQL table `chain_events` enforces a primary key on `id`.
- Insertion uses `ON CONFLICT (id) DO UPDATE SET detected_at = EXCLUDED.detected_at, observation_mode = EXCLUDED.observation_mode`, guaranteeing complete database idempotency.

---

## 10. Incident Watch Engine Correlation

Historical replay observations are fed directly into the `IncidentWatchEngine`:
- Replayed transactions are matched against active incident outpoints and watched addresses.
- If an outpoint from an incident is spent in a historical block, an `IncidentActivity` record is emitted with the historical block height and timestamp.
- Correlation runs identically for live and historical data without code duplication.

---

## 11. Configuration Reference

```env
# Replay Batching and Concurrency
OBSCHAIN_REPLAY_BATCH_SIZE=10              # Number of blocks fetched per batch
OBSCHAIN_REPLAY_CONCURRENCY=2              # Maximum concurrent block fetch tasks
OBSCHAIN_REPLAY_CHECKPOINT_INTERVAL=25     # Blocks between persistent checkpoints
OBSCHAIN_REPLAY_MAX_RANGE=100000           # Maximum allowed block range per job

# Caching & Resource Limits
OBSCHAIN_REPLAY_TX_CACHE_LIMIT=100000      # Bounded previous-transaction cache entries
OBSCHAIN_REPLAY_DB_CONCURRENCY=2           # Dedicated PostgreSQL connection limit for replay

# Operational & API Controls
OBSCHAIN_REPLAY_API_ENABLED=false          # Controls HTTP POST /api/v1/replay/jobs (default: false)
OBSCHAIN_SOVEREIGN_ONLY=true               # Strictly forbids external API lookups
```

---

## 12. CLI Commands

Replay can be executed directly from the terminal via the `obschain` binary:

```bash
# Replay blocks 900000 through 900050
cargo run --bin obschain -- replay --start 900000 --end 900050

# Replay with custom batch size and checkpoint interval
cargo run --bin obschain -- replay --start 800000 --end 800500 --batch-size 20 --checkpoint-interval 50
```

---

## 13. REST API Endpoints

### 1. Create Replay Job
`POST /api/v1/replay/jobs`
*(Requires `OBSCHAIN_REPLAY_API_ENABLED=true`)*

Request:
```json
{
  "start_height": 900000,
  "end_height": 901000
}
```

Response:
```json
{
  "id": "e57c6b90-1c52-4f10-91bf-a3d20eb0707a",
  "network": "bitcoin",
  "start_height": 900000,
  "end_height": 901000,
  "current_height": 900000,
  "status": "pending",
  "created_at": "2026-09-25T03:00:00Z"
}
```

### 2. List Replay Jobs
`GET /api/v1/replay/jobs`

### 3. Get Job Details
`GET /api/v1/replay/jobs/:id`

### 4. Job Lifecycle Controls
- `POST /api/v1/replay/jobs/:id/cancel`
- `POST /api/v1/replay/jobs/:id/pause`
- `POST /api/v1/replay/jobs/:id/resume`

### 5. Research Event Queries
`GET /api/v1/events?from_height=900000&to_height=901000&observation_mode=historical_replay&event_type=large_transaction&limit=50`
