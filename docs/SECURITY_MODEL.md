# ObsChain Security Model

## Threat Vectors & Defenses

ObsChain processes data from untrusted network sources (peer-to-peer gossip, external public APIs, WebSockets). Robust security guarantees are mandatory.

### 1. Memory Safety & Rust Code Quality
- All code runs in safe Rust. `#![forbid(unsafe_code)]` is enforced across all crates where possible.
- Avoid `.unwrap()` and `.expect()` in runtime data processing paths. Explicit `Result` and `Option` handling with `thiserror` and `anyhow`.

### 2. Denial of Service (DoS) Protections
- **Unbounded Memory Allocation**: All collections, buffers, and queries enforce strict caps.
- **Request Size Limiting**: Axum API enforces a 1MB max body size layer (`RequestBodyLimitLayer`).
- **Pagination Limits**: Endpoints clamp `limit` between 1 and 100 items.
- **Ingestion Backpressure**: Stream buffers utilize bounded Tokio channels (`mpsc::channel`).

### 3. Untrusted Data Deserialization
- Untrusted JSON-RPC or WebSocket payloads are sanitized and deserialized into strictly typed structs.
- Malformed inputs are rejected with logging without crashing the process.

### 4. Injection & Secret Handling
- **SQL Injection**: All database queries use parameterized SQLx prepared statements. No raw string interpolation.
- **Secret Leaks**: Environment variables (`BITCOIN_RPC_PASSWORD`, `DATABASE_URL`) are never logged or exposed via API status endpoints.
- **SSRF**: Upstream RPC/API endpoints are strictly configured through environment variables at startup, never taken from user input.

### 5. Dependency Audit
- Automated `cargo audit` in continuous integration to catch vulnerable crates.

### 6. Incident Ingestion & Provenance Security
- **Identity Forgery Resistance**: Self-attributed claims (such as "we are white hats" messages in OP_RETURN) are programmatically rejected if submitted as `ON_CHAIN_VERIFIED` or `OFFICIALLY_ATTRIBUTED` identity.
- **SSRF & Source URL Sanitization**: URLs in external sources must use public HTTPS schemes. Arbitrary `file://`, loopback, or private internal network lookups from seed dossiers are blocked.
- **Checked Integer Arithmetic**: All monetary quantities (`affected_sats`, `recovered_sats`, `outstanding_sats`) use 64-bit unsigned integer arithmetic with checked bounds (`recovered_sats <= affected_sats`). Floating-point balances are prohibited internally.
- **Graph Topology Integrity**: Graphs are validated at build time to prevent dangling edges or duplicate node IDs, preventing invalid graph structures in client visualizers.
- **Chronological Monotonicity**: Timelines and updates must strictly maintain chronological ordering.

### 7. Incident Watch Engine & Correlation Security
- **Public Watch Target Sanitization**: The endpoint `GET /api/v1/incidents/:id/watch-targets` strips all internal investigation configurations, private detection thresholds, and operator notes, returning only safe public metadata (`PublicWatchTarget`).
- **Bounded Descendant Tracking**: Dynamic descendant tracking enforces `OBSCHAIN_INCIDENT_FOLLOW_DEPTH` (default 3, clamped between 1 and 5) and bounds the maximum number of tracked descendants to prevent memory exhaustion or exponential graph explosion attacks from fan-out spam.
- **Bounded In-Memory Activity Store**: Incident activity and alert events are buffered in bounded circular ring buffers (`OBSCHAIN_ACTIVITY_STORE_LIMIT`, default 10,000) to protect against memory exhaustion under high-throughput transaction loads.
- **Epistemological Integrity & Anti-Inflation**: The engine strictly enforces that on-chain movement is not confused with fund recovery (`recovery.recovered_sats` is immutable to on-chain movement detection alone). Heuristic targets are strictly capped at `Medium` severity, preventing automated escalation to `Critical` on probabilistic evidence.

### 8. Database & Persistence Security (Phase 4B)
- **Parameterized SQL Everywhere**: All queries in `obschain-storage` execute as parameterized queries using SQLx prepared statements (`$1, $2, ...`). Dynamic SQL and string interpolation are strictly prohibited.
- **Database Credential Masking**: The database connection URI contains sensitive passwords. The configuration subsystem scrubs passwords using `redact_database_url` prior to logging. The full connection string is never logged, printed in panic handlers, or exposed via `/health` or `/status` APIs.
- **Satoshi Integer Conversion Validation**: PostgreSQL `BIGINT` is signed 64-bit (`-9.22e18` to `+9.22e18`). Satoshis are unsigned 64-bit (`u64`) in Rust. All mapping boundaries use checked converters (`u64_to_i64_checked` and `i64_to_u64_checked`) that explicitly reject negative integers and values exceeding `i64::MAX`, preventing integer underflow or wrap-around exploits.
- **Fail-Closed Startup Safety**: If `OBSCHAIN_STORAGE_BACKEND=postgres` is requested, startup terminates immediately with exit code 1 if connection or migrations fail. ObsChain NEVER silently falls back to in-memory mode when the operator requested persistent storage.
- **Bounded Write Retry Policy**: Database writes during live Bitcoin ingestion retry at most 3 times with exponential backoff (100ms, 200ms, 400ms). Retries do not grow unbounded memory queues or block pipeline threads.
- **Transaction Rollback Guarantees**: Multi-table incident inserts (sources, evidence, timeline, graph) execute within SQL transactions (`tx.begin()`). Failures automatically roll back to prevent half-written dossiers.
- **Structured Error Status Mapping**: Database lookup and query errors are returned to API callers as HTTP `503 Service Unavailable`, preventing internal database connection failures from being masked as HTTP `404 Not Found`.
- **Query Bounds & Clamped Pagination**: All list queries enforce bounds (`LIMIT $1 OFFSET $2`) where `$1` is strictly clamped to a maximum of 200 items, preventing denial-of-service memory exhaustion via unbounded `SELECT` statements.

