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
   Normalized Metric Storage (event_metric_values)
   [Unique: (event_id, metric, metric_definition_version)]
               │
               ▼
       Baseline Population (Sample Filtering & Deduplication)
               │
               ▼
   Distribution Aggregation (BaselineCalculator: min, max, mean, discrete quantiles)
               │
               ▼
   Quantile Distributions & Summaries (p50, p75, p90, p95, p99, p99.9)
               │
               ├─────────────────────────────────────────┐
               ▼                                         ▼
   Exact Empirical CDF Rank                  Quantile Interpolation Estimate
   (ExactEmpiricalCdf, estimated: false)     (QuantileInterpolationEstimate, estimated: true)
   [Evaluated against actual metric values]  [Fast approximation from sparse quantiles]
               │                                         │
               └────────────────────┬────────────────────┘
                                    ▼
       Rarity Classification (Common, Notable, Unusual, Rare, Extreme, InsufficientData)
                                    │
                                    ▼
       Event-Type-Specific Impact Model (Normalized Component Weights Sum to 100.0)
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

## 5. Metric Extraction, Versioning & Numerical Precision

All numeric extraction is centralized in `EventMetricExtractor` avoiding ad-hoc parsing:

```rust
extract_metric(event, BaselineMetric::DormantValueSats) -> Option<MetricValue>
```

### Metric Definition Versioning
Metrics themselves evolve over time. To preserve historical reproducibility and prevent extraction logic changes from silently altering historical interpretations, every metric definition includes an explicit version string:
- `value-v1`: Large transfer transaction output value (sats).
- `input-count-v1`, `output-count-v1`: Transaction input/output counts.
- `vsize-v1`: Transaction virtual size.
- `interval-v1`: Long block interval duration (seconds).
- `dormant-value-v1`: Total value of dormant inputs spent (sats).
- `oldest-input-age-v1`, `average-input-age-v1`: Input age metrics (days).
- `coin-age-destroyed-v1`: Coin Age Destroyed ($\text{sats} \times \text{days}$).
- `consolidation-ratio-v1`: Input-to-output consolidation ratio (basis points).
- `distributed-value-v1`, `median-output-v1`: Fan-out distribution metrics (sats).
- `fee-rate-v1`: Fee rate (sat/vB, scaled decimal).
- `fee-sats-v1`: Absolute transaction fee (sats).

### Normalized Canonical Metric Storage (`event_metric_values`)
Canonical event metrics are normalized into `event_metric_values`:
- Enforces uniqueness on `(event_id, metric, metric_definition_version)`.
- Replaying 100 observations of an event creates **exactly 1 canonical metric row** per metric version.
- Avoids repeated JSON parsing during baseline generation and exact empirical rank lookups.
- Backed by clean B-Tree indexes on `(network, event_type, metric, metric_definition_version, block_height, value_numeric)`.

### Supported Metric Value Types & PostgreSQL Numeric Precision
- `MetricValue::U64(u64)`: Satoshis (up to 21M BTC = $2.1 \times 10^{15}$ sats), counts, seconds, days.
- `MetricValue::U128(u128)`: Coin Age Destroyed satoshi-days ($\text{sats} \times \text{days}$). Safely exceeds $u64::\text{MAX}$ ($1.84 \times 10^{19}$) without overflow or floating-point truncation.
- `MetricValue::BasisPoints(u32)`: Ratios (e.g. consolidation ratio: $100 = 1.00\times$).
- `MetricValue::DecimalScaled { value, scale }`: Fee rates (e.g. 15.50 sat/vB as value: 1550, scale: 2).

> **Numeric Domain Safety (`NUMERIC(50, 4)`)**:
> In Phase 6B.1, PostgreSQL storage was upgraded from `NUMERIC(38, 4)` to `NUMERIC(50, 4)`. Rust's `u128::MAX` is $340,282,366,920,938,463,463,374,607,431,768,211,455$, requiring 39 decimal integer digits. `NUMERIC(38, 4)` reserved 4 decimal places, leaving only 34 digits before the decimal point, causing numeric overflow for large `u128` values. `NUMERIC(50, 4)` allows 46 digits before the decimal, guaranteeing 100% lossless round-trip persistence for all Rust `u128` and Coin Age Destroyed metrics.
>
> **Mean and StdDev Precision Semantics**:
> - `mean` is stored as `NUMERIC(50, 4)` in baseline distributions to prevent floating-point precision loss across large sample totals.
> - `std_dev` is stored as `FLOAT8` (`DOUBLE PRECISION`) and is explicitly documented as an approximate descriptive aggregate, not a lossless integer representation.
> - Integer quantile thresholds (`p50` through `p999`) remain exact numeric values with zero float conversion.

---

## 6. Distribution Methodology & Percentile Semantics

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

### Exact Empirical Percentile vs Quantile Interpolation

ObsChain makes an explicit mathematical distinction between two ranking methods:

```rust
pub enum PercentileMethod {
    /// Exact empirical rank computed against the actual historical canonical metric population.
    ExactEmpiricalCdf,
    /// Linear interpolation between precomputed discrete quantiles (p50, p75, p90, p95, p99, p99.9).
    QuantileInterpolationEstimate,
}
```

Every rarity result records which method produced it and sets `estimated: bool` accordingly (`false` for exact CDF, `true` for interpolation).

#### 1. Exact Empirical CDF (`ExactEmpiricalCdf`)
When exact empirical ranking is performed, the query calculates the rank directly against the actual canonical metric values in `event_metric_values` within the baseline window:

$$\text{percentile}(x) = \left( \frac{\text{count}(s \le x)}{N} \right) \times 100.0$$

Tail count is the exact number of events matching or exceeding $x$:

$$\text{tail\_count}(x) = \text{count}(s \ge x)$$

Ties are treated **inclusively** in both cumulative distribution and tail counts.

Parameterized single-pass SQL implementation:
```sql
SELECT
    COUNT(*) FILTER (WHERE value_numeric <= $1),
    COUNT(*) FILTER (WHERE value_numeric >= $1),
    COUNT(*)
FROM event_metric_values
WHERE network = $2
  AND event_type = $3
  AND metric = $4
  AND metric_definition_version = $5
  AND block_height BETWEEN $6 AND $7;
```

#### 2. Quantile Interpolation Estimate (`QuantileInterpolationEstimate`)
When evaluating against stored distribution summaries without scanning the raw metric population:
- The percentile is estimated by piece-wise linear interpolation between discrete quantiles ($p50, p75, p90, p95, p99, p99.9$).
- Marked explicitly as `estimated: true` and `percentile_method: QUANTILE_INTERPOLATION_ESTIMATE`.
- Never labeled as `EXACT_EMPIRICAL_CDF`. Useful for visualization, rapid inspection, and lightweight summary previews.

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

In Phase 6B.1, the impact scoring model was completely overhauled to eliminate weight saturation flaws.

### Previous Model Flaw
Previously, a universal model assigned component weights totaling 135:
```text
Value anomaly                 0–25 points
Coin-age anomaly              0–25 points
Fee anomaly                   0–15 points
Transaction structure anomaly 0–20 points
Network anomaly               0–50 points
Total:                        135 points (clamped to 100.0)
```
Clamping a >100 weighted model caused premature saturation (events reached 100 before their metrics reached the tail) and obscured composition evidence. Furthermore, non-applicable components (e.g. asking for transaction structure in a `LongBlockInterval`) distorted scoring.

### Event-Type-Specific Impact Models
ObsChain implements dedicated, isolated impact models for each supported event type where component weights sum to **exactly 100.0**:

1. **`LargeTransfer` (`obschain-impact-large-transfer-v1`)**:
   - `value_sats`: 60.0%
   - `input_count`: 15.0%
   - `output_count`: 15.0%
   - `vsize`: 10.0%
   - **Total**: 100.0%

2. **`DormantCoinsMoved` (`obschain-impact-dormant-coins-v1`)**:
   - `dormant_value_sats`: 35.0%
   - `oldest_input_age_days`: 30.0%
   - `coin_age_destroyed_satoshi_days`: 25.0%
   - `input_count`: 10.0%
   - **Total**: 100.0%

3. **`LongBlockInterval` (`obschain-impact-long-block-interval-v1`)**:
   - `interval_seconds`: 100.0%
   - **Total**: 100.0% *(Contains NO fee, coin-age, or transaction structure components)*

4. **`Consolidation` (`obschain-impact-consolidation-v1`)**:
   - `input_count`: 40.0%
   - `consolidation_ratio`: 30.0%
   - `value_sats`: 20.0%
   - `output_count`: 10.0%
   - **Total**: 100.0%

5. **`FanOut` (`obschain-impact-fan-out-v1`)**:
   - `output_count`: 40.0%
   - `distributed_value_sats`: 35.0%
   - `median_output_sats`: 25.0%
   - **Total**: 100.0%

6. **`ExtremeFee` (`obschain-impact-extreme-fee-v1`)**:
   - `fee_rate_sat_vb`: 60.0%
   - `fee_sats`: 40.0%
   - **Total**: 100.0%

### Mathematical Range Proof (No Structural Clamping)
Anomaly points scale linearly for upper-half anomalies ($p \ge 50.0\%$):

$$\text{normalized}_i = \frac{\max(0, p_i - 50.0)}{50.0}$$
$$\text{points\_awarded}_i = \text{weight}_i \times \text{normalized}_i$$

Since $p_i \le 100.0$, $\text{normalized}_i \le 1.0$, which implies:

$$\text{points\_awarded}_i \le \text{weight}_i$$
$$\sum_{i} \text{points\_awarded}_i \le \sum_{i} \text{weight}_i = 100.0$$

The score is mathematically bounded within $[0.0, 100.0]$ **by construction**. Structural clamping (`score.min(100.0)`) is removed, leaving only a defensive floating-point epsilon check at `100.00000001`.

### Component Coverage & Score Availability
If certain metrics are missing (e.g. UTXO input age unavailable), ObsChain calculates coverage:

$$\text{model\_coverage} = \frac{\sum \text{applicable\_weights}}{100.0}$$

- If `model_coverage` is below 50% ($0.50$), or if the population sample count is below `min_sample_size` (100), the composite score is **suppressed** (`total_score: None`).
- If `model_coverage >= 0.50`, the score is normalized over available components: $\text{score} = \frac{\sum \text{points}}{\text{model\_coverage}}$.

### Cross-Event Comparability Boundary
An impact score for `DormantCoinsMoved` and `LongBlockInterval` cannot be naively compared as identical universal danger rankings. They represent relative anomaly indexes within their respective event-type models. Every score explicitly exposes:
- `model_id`: e.g. `obschain-impact-dormant-coins-v1`
- `event_type`: e.g. `DORMANT_COINS_MOVED`
- `model_coverage`: e.g. `1.0` (100%)
- `status`: `EXPERIMENTAL`

---

## 11. PostgreSQL Schema (`0006_historical_baselines.sql` & `0007_statistical_correctness.sql`)

```sql
-- Normalized Canonical Event Metrics (0007_statistical_correctness.sql)
CREATE TABLE event_metric_values (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL REFERENCES chain_events(id) ON DELETE CASCADE,
    event_type VARCHAR(64) NOT NULL,
    metric VARCHAR(64) NOT NULL,
    value_numeric NUMERIC(50, 4) NOT NULL,
    metric_scale INTEGER NOT NULL DEFAULT 0,
    block_height BIGINT,
    event_time TIMESTAMPTZ NOT NULL,
    network VARCHAR(32) NOT NULL,
    metric_definition_version VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_event_metric_version UNIQUE (event_id, metric, metric_definition_version)
);

CREATE INDEX idx_event_metric_lookup ON event_metric_values(
    network, event_type, metric, metric_definition_version, block_height, value_numeric
);
CREATE INDEX idx_event_metric_event_id ON event_metric_values(event_id);

-- Baseline Distributions (0006 & 0007 migrations)
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
    minimum NUMERIC(50, 4) NOT NULL,
    maximum NUMERIC(50, 4) NOT NULL,
    mean NUMERIC(50, 4) NOT NULL,
    p50 NUMERIC(50, 4) NOT NULL,
    p75 NUMERIC(50, 4) NOT NULL,
    p90 NUMERIC(50, 4) NOT NULL,
    p95 NUMERIC(50, 4) NOT NULL,
    p99 NUMERIC(50, 4) NOT NULL,
    p999 NUMERIC(50, 4) NOT NULL,
    samples_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_baseline_dist_metric UNIQUE (baseline_run_id, event_type, metric)
);

-- Event Rarity Evaluations (0006 & 0007 migrations)
CREATE TABLE event_rarity (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL REFERENCES chain_events(id) ON DELETE CASCADE,
    baseline_run_id UUID NOT NULL REFERENCES baseline_runs(id) ON DELETE CASCADE,
    event_type VARCHAR(64) NOT NULL,
    metric VARCHAR(64) NOT NULL,
    raw_value NUMERIC(50, 4) NOT NULL,
    percentile DOUBLE PRECISION,
    rarity_band VARCHAR(32) NOT NULL,
    population_size BIGINT NOT NULL,
    tail_count BIGINT NOT NULL,
    evaluation_mode VARCHAR(32) NOT NULL,
    impact_breakdown JSONB,
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    percentile_method VARCHAR(64) NOT NULL DEFAULT 'EXACT_EMPIRICAL_CDF',
    estimated BOOLEAN NOT NULL DEFAULT FALSE,
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
  --algorithm-version obschain-baseline-v2
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
  Algorithm:     obschain-baseline-v2

Primary Metric:
  Metric:        dormant_value_sats
  Raw Value:     428104000000 (4281.04 BTC)
  Percentile:    99.94%
  Method:        EXACT_EMPIRICAL_CDF (Exact)
  Tail Count:    11
  Rarity Band:   EXTREME
  Frequency:     11 comparable-or-rarer events across 18,421 qualifying events (approx. 1 in 1674)

Secondary Metrics:
  - oldest_input_age_days: 4526 days | Percentile: 99.72% (EXACT_EMPIRICAL_CDF) | Rarity: EXTREME
  - coin_age_destroyed_satoshi_days: 19375987040000000 | Percentile: 99.88% (EXACT_EMPIRICAL_CDF) | Rarity: EXTREME
  - input_count: 2 | Percentile: 65.40% (EXACT_EMPIRICAL_CDF) | Rarity: COMMON

Composite Impact (EXPERIMENTAL):
  Model ID:      obschain-impact-dormant-coins-v1
  Event Type:    DORMANT_COINS_MOVED
  Coverage:      100.0%
  Total Score:   92.7 / 100.0
  Components:
    - dormant_value_sats:              34.9 / 35.0
    - oldest_input_age_days:           29.8 / 30.0
    - coin_age_destroyed_satoshi_days: 24.9 / 25.0
    - input_count:                      3.1 / 10.0
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
