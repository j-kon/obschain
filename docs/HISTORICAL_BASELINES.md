# ObsChain Historical Baselines, Rarity & Impact Intelligence

## 1. Overview & Core Philosophy

ObsChain provides sovereign, verifiable, empirical context for Bitcoin network anomalies and chain events. Rather than relying on arbitrary hype scores, machine learning "black boxes", or subjective alarmism, ObsChain answers:

```text
How unusual is this event compared with Bitcoin history?
```

using transparent statistical distributions computed over canonical historical event populations.

Every percentile, rarity classification, and impact score returned by ObsChain is:
1. **Empirically Grounded**: Derived strictly from observed canonical events, not synthetic or manufactured distributions.
2. **Mathematically Defensible**: Calculated via explicit, documented discrete quantiles and empirical CDF formulas.
3. **Reference-Exposed**: Always paired with its exact reference population, block-height range, sample size, tail count, algorithm version, and data quality metrics.
4. **Sovereign**: Executed 100% locally against Bitcoin Core and PostgreSQL without third-party analytics telemetry.

---

## 2. Statistical Architecture

The historical intelligence pipeline is decoupled from detection and replay ingestion:

```text
Canonical Historical Events (chain_events)
               │
               ▼
   Typed Metric Extraction (EventMetricExtractor)
               │
               ▼
       Baseline Population (Sample Filtering & Deduplication)
               │
               ▼
   Distribution Aggregation (BaselineCalculator: min, max, mean, discrete quantiles)
               │
               ▼
       Quantiles & Ranks (p50, p75, p90, p95, p99, p99.9 & empirical CDF)
               │
               ▼
    Rarity Classification (Common, Notable, Unusual, Rare, Extreme, InsufficientData)
               │
               ▼
 Explainable Impact Breakdown (Normalized Component Weights & Experimental Total)
```

Detectors remain focused on detecting objective network conditions. The baseline engine operates on top of canonical event data.

---

## 3. The Canonical-Event Invariant

### Population Definition
Statistical baselines **MUST** operate on canonical events (`chain_events`) and **NEVER** count observation occurrences (`event_observations`) as independent samples.

- **Canonical Event (`chain_events`)**: The intrinsic logical event on the Bitcoin network (e.g., a specific large transfer in txid `X` or a block interval at height `Y`).
- **Observation (`event_observations`)**: Ingestion provenance recording when and how ObsChain witnessed that canonical event (e.g., live mempool seen, confirmed in block, reconstructed during replay job `A`, reconstructed during replay job `B`).

### Mandatory Invariant
If a canonical event has 100 observation occurrences across multiple historical replays or live witnesses, it contributes **exactly 1 sample** to the baseline population:

$$\text{Canonical Event Count} = 1 \implies \text{Baseline Sample Count} = 1$$

Ten replay iterations over block 840,000 do not inflate statistical frequency tenfold.

---

## 4. Supported Event Types & Metric Registry

Replayable detectors with objective on-chain historical metrics are supported in Phase 6B:

| Event Type | Primary Metric | Secondary Metrics | Units | Rarity Direction |
|---|---|---|---|---|
| `LargeTransfer` | `value_sats` | `input_count`, `output_count`, `vsize` | Satoshis, Count | `HigherIsRarer` |
| `LongBlockInterval` | `interval_seconds` | None | Seconds | `HigherIsRarer` |
| `DormantCoinsMoved` | `dormant_value_sats` | `oldest_input_age_days`, `average_input_age_days`, `coin_age_destroyed_satoshi_days`, `input_count` | Satoshis, Days, Satoshi-Days, Count | `HigherIsRarer` |
| `Consolidation` | `input_count` | `output_count`, `value_sats`, `consolidation_ratio` | Count, Satoshis, Ratio | `HigherIsRarer` (input_count), `LowerIsRarer` (output_count) |
| `FanOut` | `output_count` | `distributed_value_sats`, `median_output_sats` | Count, Satoshis | `HigherIsRarer` |
| `ExtremeFee` | `fee_rate_sat_vb` | `fee_sats` | SatoshisPerVbyte, Satoshis | `HigherIsRarer` |

### Non-Replayable Detectors Excluded
- `TransactionReplacement` (RBF): Requires historical mempool state sequences not preserved in active-chain block archives alone.
- `ReorgDetected`: Requires historical stale-fork archives or multi-node tip disagreement data.

---

## 5. Metric Extraction & Numerical Precision

All numeric extraction is centralized in `EventMetricExtractor` avoiding ad-hoc parsing:

```rust
extract_metric(event, BaselineMetric::DormantValueSats) -> Option<MetricValue>
```

### Supported Metric Value Types
- `MetricValue::U64(u64)`: Satoshis (up to 21M BTC = $2.1 \times 10^{15}$ sats), counts, seconds, days.
- `MetricValue::U128(u128)`: Coin Age Destroyed satoshi-days ($\text{sats} \times \text{days}$). Safely exceeds $u64::\text{MAX}$ ($1.84 \times 10^{19}$) without overflow or floating-point truncation.
- `MetricValue::BasisPoints(u32)`: Ratios (e.g. consolidation ratio: $100 = 1.00\times$).
- `MetricValue::DecimalScaled { value, scale }`: Fee rates (e.g. 15.50 sat/vB as value: 1550, scale: 2).

> **Satoshi Safety**: Satoshi and coin-age arithmetic are NEVER converted to floating-point BTC before computing distributions. BTC formatting is presentation-only. In PostgreSQL, all distribution metrics are stored in `NUMERIC(38, 4)`.

---

## 6. Distribution Methodology & Quantile Semantics

For each metric in a baseline run, ObsChain computes:
- Sample count ($N$)
- Candidate event count ($M$)
- Missing count ($M - N$)
- Coverage percentage ($\frac{N}{M} \times 100$)
- Minimum observed value
- Maximum observed value
- Arithmetic Mean
- Quantiles: **p50, p75, p90, p95, p99, p99.9**

### Quantile Formula (`percentile_disc`)
ObsChain utilizes discrete quantiles (`percentile_disc`) matching actual observed historical values:

$$\text{index} = \lceil p \times N \rceil$$

This guarantees that integer monetary thresholds (e.g., p99 large transfer) correspond to actual transactions mined on the Bitcoin network rather than interpolated fractional satoshis.

### Empirical CDF Percentile-Rank Formula
When ranking an event value $x$ against a reference population:

$$\text{percentile}(x) = \left( \frac{\text{count}(s \le x)}{N} \right) \times 100.0$$

### Inclusive Tie Handling
Ties are treated **inclusively** in both cumulative distribution and tail counts:
- An event with value equal to 3 other historical events is counted as $\le x$ for all matching events.
- Tail count represents the number of events with value $\ge x$:

$$\text{tail\_count}(x) = \text{count}(s \ge x)$$

**Example Dataset**: $[10, 10, 10, 20, 20, 30]$ ($N = 6$):
- Value $10$: $3 / 6 = 50.00\%$, Tail count = $6$
- Value $20$: $5 / 6 = 83.33\%$, Tail count = $3$
- Value $30$: $6 / 6 = 100.00\%$, Tail count = $1$

---

## 7. Rarity Bands & Safeguards

Percentiles are mapped to descriptive, measured rarity classifications:

| Percentile Range | Rarity Band | Description |
|---|---|---|
| $< 90.0\%$ | `COMMON` | Typical network activity within the bulk distribution |
| $90.0\% \le p < 95.0\%$ | `NOTABLE` | Upper decile anomaly worthy of monitoring |
| $95.0\% \le p < 99.0\%$ | `UNUSUAL` | Top 5% anomaly |
| $99.0\% \le p < 99.9\%$ | `RARE` | Top 1% network occurrence |
| $\ge 99.9\%$ | `EXTREME` | Tail anomaly ($1 \text{ in } 1,000$) |
| $N < \text{minimum\_sample\_size}$ | `INSUFFICIENT_DATA` | Population too small for defensible statistical claims |

> **No "Historic" Hype**: ObsChain rejects labeling events as "HISTORIC" purely from mathematical quantiles. Historical significance requires multi-dimensional qualitative and temporal context beyond statistical rarity.

### Sample-Size Safeguard
If the reference population has fewer than `baseline_min_sample_size` qualifying events (default: **100**):
- Rarity band returns `INSUFFICIENT_DATA`.
- Percentile is withheld (`None`) to prevent misleading users with pseudo-precise tail statistics from small sample sizes.
- Impact composite score returns `None` (unavailable).

---

## 8. Data Quality & Coverage Accounting

Every baseline discloses its metric coverage:

$$\text{Coverage Ratio} = \frac{\text{Events with Metric Available}}{\text{Total Candidate Events of Type}}$$

| Sample Count ($N$) | Metric Coverage | Baseline Quality Rating |
|---|---|---|
| $N \ge 500$ | $\ge 95\%$ | `HIGH` |
| $N \ge 100$ | $\ge 80\%$ | `MODERATE` |
| $N \ge 30$ | $\ge 50\%$ | `DEGRADED` |
| $N < 30$ | $< 50\%$ | `INSUFFICIENT` |

If historical blocks lack UTXO input age due to sovereign unindexed nodes, the coverage percentage immediately discloses the gap.

---

## 9. Historical Look-Ahead Bias / Evaluation Mode

When evaluating an event against a baseline, ObsChain distinguishes:

1. **`RETROSPECTIVE`**: The baseline population includes blocks confirmed **after** the event's block height ($h_{\text{event}} \le h_{\text{baseline\_end}}$). The rarity percentile reflects historical significance in hindsight across the full window.
2. **`POINT_IN_TIME`**: The baseline population only includes blocks confirmed **before** the event ($h_{\text{baseline\_end}} < h_{\text{event}}$). The rarity percentile reflects what was known contemporaneously at the moment the event occurred.

---

## 10. Explainable Impact Intelligence (`ImpactBreakdown`)

Composite anomaly significance is broken down into visible components:

```text
Value anomaly                 0–25 points
Coin-age anomaly              0–25 points
Fee anomaly                   0–15 points
Transaction structure anomaly 0–20 points
Network anomaly               0–50 points
```

### Deterministic Percentile-to-Points Formula
Anomaly points scale linearly for upper-half anomalies ($p \ge 50.0\%$):

$$\text{normalized} = \frac{p - 50.0}{50.0}$$
$$\text{points\_awarded} = \text{max\_points} \times \text{normalized}$$

Events at or below median ($p < 50.0\%$) receive 0 anomaly points.

### Impact Metadata & Status
- `model_version`: `obschain-impact-v1`
- `status`: `EXPERIMENTAL` (disclosed on all API responses)
- If required data is insufficient, `total_score` is `None` rather than manufactured precision.

---

## 11. PostgreSQL Schema (`0006_historical_baselines.sql`)

```sql
CREATE TABLE baseline_runs (
    id UUID PRIMARY KEY,
    network VARCHAR(32) NOT NULL,
    start_height BIGINT NOT NULL,
    end_height BIGINT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    status VARCHAR(32) NOT NULL,
    algorithm_version VARCHAR(64) NOT NULL,
    canonical_event_count BIGINT NOT NULL DEFAULT 0,
    error_message TEXT,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE baseline_distributions (
    id UUID PRIMARY KEY,
    baseline_run_id UUID NOT NULL REFERENCES baseline_runs(id) ON DELETE CASCADE,
    event_type VARCHAR(64) NOT NULL,
    metric VARCHAR(64) NOT NULL,
    unit VARCHAR(32) NOT NULL,
    sample_count BIGINT NOT NULL,
    candidate_count BIGINT NOT NULL,
    missing_count BIGINT NOT NULL,
    coverage_ratio DOUBLE PRECISION NOT NULL,
    quality VARCHAR(32) NOT NULL,
    minimum NUMERIC(38, 4) NOT NULL,
    maximum NUMERIC(38, 4) NOT NULL,
    mean DOUBLE PRECISION NOT NULL,
    p50 NUMERIC(38, 4) NOT NULL,
    p75 NUMERIC(38, 4) NOT NULL,
    p90 NUMERIC(38, 4) NOT NULL,
    p95 NUMERIC(38, 4) NOT NULL,
    p99 NUMERIC(38, 4) NOT NULL,
    p999 NUMERIC(38, 4) NOT NULL,
    samples_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_baseline_dist_metric UNIQUE (baseline_run_id, event_type, metric)
);

CREATE TABLE event_rarity (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL REFERENCES chain_events(id) ON DELETE CASCADE,
    baseline_run_id UUID NOT NULL REFERENCES baseline_runs(id) ON DELETE CASCADE,
    event_type VARCHAR(64) NOT NULL,
    metric VARCHAR(64) NOT NULL,
    raw_value NUMERIC(38, 4) NOT NULL,
    percentile DOUBLE PRECISION,
    rarity_band VARCHAR(32) NOT NULL,
    population_size BIGINT NOT NULL,
    tail_count BIGINT NOT NULL,
    evaluation_mode VARCHAR(32) NOT NULL,
    impact_breakdown JSONB,
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_event_rarity_metric UNIQUE (event_id, baseline_run_id, metric)
);
```

---

## 12. CLI Operations

### 1. Generate Historical Baseline
Generate a baseline across an explicit block height range:

```bash
cargo run --bin obschain -- baseline \
  --start 840000 \
  --end 850000 \
  --network mainnet \
  --algorithm-version obschain-baseline-v1
```

Optional event-specific filter:

```bash
cargo run --bin obschain -- baseline \
  --event-type dormant_coins_moved \
  --start 800000 \
  --end 970000
```

### 2. Research Rarity Query
Inspect the rarity profile of an event:

```bash
cargo run --bin obschain -- rarity \
  --event-id <UUID>
```

Example CLI Output:

```text
============================================================
ObsChain Historical Rarity & Impact Context
============================================================
Event ID:     3b2d1c9a-5f04-4b9d-bc11-a89c31023fe1
Event Type:   DORMANT_COINS_MOVED
Title:        Dormant Coins Moved: 4,281 BTC (12.4 yrs)
Height:       850124 | Txid: 4a5e1e...

Baseline Context:
  Run ID:        9c2b1a4f-...
  Network:       mainnet
  Window:        Blocks 700000 -> 970000
  Population:    18421 qualifying events
  Quality:       HIGH
  Mode:          RETROSPECTIVE
  Algorithm:     obschain-baseline-v1

Primary Metric:
  Metric:        dormant_value_sats
  Raw Value:     428104000000 (4281.04 BTC)
  Percentile:    99.94%
  Tail Count:    11
  Rarity Band:   EXTREME
  Frequency:     11 comparable-or-rarer events across 18,421 qualifying events (approx. 1 in 1674)

Secondary Metrics:
  - oldest_input_age_days: 4526 days | Percentile: 99.72% | Rarity: EXTREME
  - coin_age_destroyed_satoshi_days: 19375987040000000 | Percentile: 99.88% | Rarity: EXTREME
  - input_count: 2 | Percentile: 65.40% | Rarity: COMMON

Composite Impact (EXPERIMENTAL):
  Model Version: obschain-impact-v1
  Total Score:   72.4 / 100.0
  Components:
    - Value anomaly:                 24.9 / 25.0
    - Coin-age anomaly:              24.9 / 25.0
    - Transaction structure anomaly: 6.2 / 20.0
============================================================
```

---

## 13. REST API Endpoints

| Method | Endpoint | Description | Status |
|---|---|---|---|
| `GET` | `/api/v1/research/baselines` | List completed baseline runs (filtered by network) | 200 OK |
| `GET` | `/api/v1/research/baselines/:id` | Get baseline metadata and summary | 200 OK |
| `GET` | `/api/v1/research/distributions` | Query distributions by `baseline_run_id`, `event_type`, `metric` | 200 OK |
| `POST` | `/api/v1/research/baselines` | Trigger baseline computation job (**admin only**) | 201 Created / 403 Forbidden |
| `GET` | `/api/v1/events/:id/rarity` | Full rarity analysis and impact breakdown for an event | 200 OK |
| `GET` | `/api/v1/events/:id` | Standard event detail enriched with fast, non-blocking rarity summary | 200 OK |

### Administrative Baseline Creation Security
`POST /api/v1/research/baselines` is disabled by default to prevent expensive resource consumption and DoS. It requires `OBSCHAIN_BASELINE_API_ENABLED=true`.

---

## 14. Known Statistical Limitations

1. **Active-Chain Only**: Replay baselines reflect verified, confirmed blocks on the active chain. Unconfirmed mempool dynamics (e.g. historical mempool depth) are not represented.
2. **Era Non-Stationarity**: Bitcoin fee markets and transaction volume changed dramatically between 2011 and 2026. A 500 BTC transfer in Epoch 0 (2010) represents a different network context than in Epoch 4 (2024). Baselines disclose their height range and epoch boundaries to avoid naive cross-era conflation.
3. **UTXO Incompleteness**: If a node operator runs in pruned mode without `txindex` or historical UTXO snapshots, historical coin-age metrics will have degraded coverage. ObsChain explicitly reports this coverage percentage.
