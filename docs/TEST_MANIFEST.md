# ObsChain Automated Test Manifest & Baseline Verification

This document provides the authoritative record of ObsChain automated test suites, test counts, and suite inventory as of **Phase 6A.2**.

---

## 1. Authoritative Test Inventory (Phase 6A.2 Baseline)

Total Automated Tests: **180 passed, 0 failed, 0 ignored** across 15 test binaries.

| Test Binary / Suite | Source Path | Test Count | Status | Notes |
|---|---|---|---|---|
| `obschain` (unit) | `src/lib.rs` | 3 | Passing | Pipeline metrics & configuration tests |
| `obschain` (main) | `src/main.rs` | 0 | Passing | Executable harness |
| `api_tests` (integration) | `tests/api_tests.rs` | 14 | Passing | REST and WebSocket API endpoints |
| `bitcoin_core_regtest_tests` (integration) | `tests/bitcoin_core_regtest_tests.rs` | 8 | Passing | Live bitcoind regtest RPC & ZMQ integration |
| `historical_replay_tests` (integration) | `tests/historical_replay_tests.rs` | 8 | Passing | Historical block replay & time-semantics |
| `incident_watch_tests` (integration) | `tests/incident_watch_tests.rs` | 1 | Passing | Incident watch telemetry & alerts |
| `provenance_tests` (integration) | `tests/provenance_tests.rs` | 14 | Passing | Event vs observation separation & lifecycle transitions |
| `obschain-core` (unit) | `crates/obschain-core/src/lib.rs` | 7 | Passing | Domain models, deterministic UUIDv5, observations |
| `obschain-detectors` (unit) | `crates/obschain-detectors/src/lib.rs` | 40 | Passing | Anomaly detectors (large tx, intervals, fees, RBF, reorg) |
| `obschain-incidents` (unit) | `crates/obschain-incidents/src/lib.rs` | 2 | Passing | Incident seed invariants & JSON fixture validation |
| `incident_intelligence_tests` (integration) | `crates/obschain-incidents/tests/incident_intelligence_tests.rs` | 14 | Passing | Graph modeling, evidence hierarchy, fund arithmetic |
| `obschain-ingest` (unit) | `crates/obschain-ingest/src/lib.rs` | 49 | Passing | Bitcoin RPC, ZMQ sequence/rawtx, mempool REST/WS, UTXO cache |
| `obschain-intelligence` (unit) | `crates/obschain-intelligence/src/lib.rs` | 6 | Passing | Clustering, watch engine, descendant tracking |
| `obschain-storage` (unit) | `crates/obschain-storage/src/lib.rs` | 4 | Passing | In-memory repository & bounded retention |
| `postgres_integration_tests` (integration) | `crates/obschain-storage/tests/postgres_integration_tests.rs` | 10 | Passing | PostgreSQL schema, migrations 0001-0005, transactions |
| **TOTAL** | | **180** | **180 Passed** | **0 Failed, 0 Ignored** |

---

## 2. Reconciliation: Phase 6A (190) vs Phase 6A.1 (174) Discrepancy

During the Phase 6A.2 audit, the discrepancy between the Phase 6A report (190 tests) and Phase 6A.1 report (174 tests) was fully investigated.

### Investigation Findings
1. **No tests were deleted or renamed.**
2. **No feature flags or configurations changed.**
3. **The 190 figure in Phase 6A was an arithmetic / transcription error in the manual report table:**
   - The Phase 6A manual markdown table listed `obschain-core` as having **38** tests; the actual count was **7** (+31 error).
   - The Phase 6A manual markdown table listed `incident_watch_tests` as having **5** tests; the actual count was **1** (+4 error).
   - The Phase 6A manual markdown table omitted `obschain (src/lib.rs)` (**3** tests) and `tests/bitcoin_core_regtest_tests.rs` (**8** tests) (-11 omission).
   - Net discrepancy in table: $+31 + 4 - 11 = +24$.
   - The actual `cargo test --workspace` count at Phase 6A was **166** passed tests ($166 + 24 = 190$).
4. **Phase 6A.1 Progression**:
   - Phase 6A.1 introduced `tests/provenance_tests.rs` with **8** new tests.
   - $166 + 8 = \mathbf{174}$ passed tests.
   - Phase 6A.1 reported the exact raw output from `cargo test --workspace` without manual arithmetic errors.
5. **Phase 6A.2 Progression**:
   - Phase 6A.2 added **6** new lifecycle and PostgreSQL integration tests to `tests/provenance_tests.rs` (bringing that suite from 8 to 14 tests).
   - $174 + 6 = \mathbf{180}$ passed tests.

---

## 3. Automated Verification Script

To prevent future manual counting errors, execute the manifest script:

```bash
./scripts/test_manifest.sh
```

This script executes `cargo test --workspace`, parses the individual binary outputs, and produces an authoritative summary table.
