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

