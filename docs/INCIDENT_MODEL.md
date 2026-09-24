# ObsChain Incident Intelligence & Provenance Model

## 1. Overview & Core Philosophy

The ObsChain Incident Intelligence subsystem provides structured investigation and evidence tracking for major Bitcoin network events (sidechain exploits, reserve extractions, exchange attacks, and protocol anomalies).

### Fundamental Product Rule
**ObsChain NEVER treats all information inside an incident as equally certain.**

Every incident claim belongs to an explicit evidence classification. The system rigorously distinguishes:
- **What Bitcoin proves** (cryptographic consensus, confirmed script executions, immutable UTXO movements).
- **What an official party says** (statements, post-mortems, and internal event timelines from maintainers or custodians).
- **What analysts infer** (heuristic address clustering, co-spending, peel chain tracing).
- **What remains unknown or disputed** (real-world actor identity, subjective intent, or unverified claims).

### Inviolable Tenets
1. **On-Chain Evidence != Identity Attribution**:
   A verified on-chain OP_RETURN stating *"we are whitehats. contact us on chain."* proves cryptographically that the transaction contained that message. It does **NOT** prove that the actor is an ethical white-hat researcher or establish their identity. In ObsChain, the message content is `ON_CHAIN_VERIFIED`, while the sender's identity remains `UNVERIFIED` / `SELF_ATTRIBUTED`.
2. **Official Reporting != Cryptographic Proof**:
   Official statements from core maintainers or affected services (such as Blockstream or SideSwap) are classified as `OFFICIALLY_ATTRIBUTED`. They are treated as authoritative accounts of internal operations, but are never conflated with independent cryptographic on-chain consensus.
3. **Heuristics Cannot Assert Definitive Ownership**:
   Address clustering based on heuristic models must never assert `OWNED_BY`. Neutral graph edges like `POSSIBLY_RELATED` or `ASSOCIATED_WITH` are enforced.
4. **Monetary Safety in Integer Satoshis**:
   All balances, movements, and recovery figures are stored and calculated strictly as integer satoshis (`u64`) using checked arithmetic. Decimal BTC values are derived display properties only.

---

## 2. Provenance Classification Hierarchy

| Level | Classification | Meaning & Verification Standard |
| :--- | :--- | :--- |
| **1** | `ON_CHAIN_VERIFIED` | Cryptographically proven via valid Bitcoin or sidechain block inclusion, script execution, or confirmed UTXO transfer. Immutable ground truth. |
| **2** | `OFFICIALLY_ATTRIBUTED` | Published official advisory or disclosure by verified custodians, infrastructure operators, or cryptographic signature proofs (e.g. proof-of-reserves). |
| **3** | `REPUTABLE_REPORTING` | Independent investigations published by established security research firms with transparent methodologies and cross-corroborated evidence. |
| **4** | `HEURISTIC` | Derived from algorithmic or heuristic behavioral clustering (common-input ownership, change address heuristics). Cannot establish definitive identity. |
| **5** | `UNVERIFIED` | Self-attributions, anonymous forum claims, social media speculation, or uncorroborated single-source assertions. |
| **6** | `DISPUTED` | Contested claims where counterparties present conflicting accounts or where claims directly conflict with verified on-chain observations. |

---

## 3. Incident Lifecycle Statuses

Incidents progress through a defined lifecycle without implying premature legal conclusions:

- `DETECTED`: Initial abnormal network behavior observed.
- `INVESTIGATING`: Active analysis of transaction flows, affected addresses, and root causes.
- `VERIFIED`: Confirmed security incident with verified on-chain impact.
- `MONITORING`: Ongoing surveillance of outstanding funds and potential return or dispersal movements.
- `RECOVERY`: Active remediation, interim patching, or fund negotiation/return underway.
- `RESOLVED`: Technical remediation complete, root causes patched, and fund status settled.
- `CLOSED`: Investigation concluded; final dossier archived.

Historical incidents (such as the September 2026 Liquid Network incident) remain in `MONITORING` or `RECOVERY` as long as outstanding funds remain unrecovered.

---

## 4. Domain Models

### Evidence Model
```rust
pub struct Evidence {
    pub id: Uuid,
    pub incident_id: Uuid,
    pub evidence_type: EvidenceType,
    pub confidence: ProvenanceClassification,
    pub title: String,
    pub description: String,
    pub observed_at: Option<DateTime<Utc>>,
    pub source_id: Option<Uuid>,
    pub source_reference: Option<String>,
    pub txid: Option<String>,
    pub block_hash: Option<String>,
    pub block_height: Option<u64>,
    pub chain: Chain,
    pub verified: bool,
    pub raw_data: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
}
```

### Source Provenance
Every external claim retains its complete origin:
- `publisher`: Organization or network ledger (e.g., "Blockstream", "Bitcoin Mainnet").
- `title`: Document or advisory title.
- `url`: Canonical HTTPS link to primary source.
- `publication_timestamp`: Date of external publication.
- `retrieved_timestamp`: Timestamp when ingested by ObsChain.
- `source_category`: `BitcoinBlockchain`, `LiquidBlockchain`, `OfficialTechnicalReport`, `OfficialPublicStatement`, `SourceRepository`, `SecurityAdvisory`, `IndependentReporting`.
- `reliability_score`: Calibrated confidence weight (0.0 to 1.0).

### Chain Scope
ObsChain maintains an objective chain identifier:
```rust
pub enum Chain {
    Bitcoin,
    Liquid,
}
```
*Note:* ObsChain is strictly a Bitcoin observation and intelligence platform. Sidechains (like Liquid) and Layer-2 protocols (like Lightning) are supported solely because incidents affecting them impact the Bitcoin ecosystem and Bitcoin mainnet peg reserves.

### Fund Tracking & Safe Integer Satoshis
```rust
pub struct RecoverySummary {
    pub affected_sats: u64,
    pub recovered_sats: u64,
    pub outstanding_sats: u64,
    pub as_of_timestamp: DateTime<Utc>,
    pub source: Option<String>,
    pub is_estimate: bool,
}
```
- Invariant: `affected_sats >= recovered_sats`.
- Invariant: When not an estimate, `outstanding_sats == affected_sats - recovered_sats`.
- Safe display accessors: `affected_btc()`, `recovered_btc()`, `outstanding_btc()`, `recovery_percentage()`.

### On-Chain Messages
Stores script messages (such as `OP_RETURN` communication) while strictly decoupling message payload from sender identity:
```rust
pub struct OnChainMessage {
    pub txid: String,
    pub chain: Chain,
    pub encoding: String,
    pub decoded_text: String,
    pub raw_hex: String,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub block_height: Option<u64>,
    pub attributed_sender: Option<String>,
    pub sender_attribution_confidence: ProvenanceClassification,
}
```

### Relationship Graph Model
Structures relations between transactions, blocks, entities, evidence, and sources for frontend visualization:
- **Node Types**: `Transaction`, `Block`, `Address`, `Entity`, `Source`, `Evidence`.
- **Edge Types**: `Spends`, `ConfirmedIn`, `ForwardsTo`, `ReturnsTo`, `References`, `Supports`, `AttributedTo`, `PossiblyRelated`.
- Strict validation: Duplicate node IDs and dangling edges (references to missing nodes) are rejected at build time.

---

## 5. First Case: Liquid Network Security Incident (September 2026)

- **Case Identifier**: `OC-2026-0001`
- **Canonical UUID**: `0c202600-0001-0000-0000-000000000001`
- **Status**: `MONITORING`
- **Severity**: `CRITICAL`
- **Technical Vulnerability**: Consensus-critical range-proof verification cache flaw in Elements software. Distinct input tuples produced the same concatenated cache key due to missing length framing. Fixed in Elements PR #1600 (commit `9400096`) using `CHashWriter`/`SER_GETHASH` and the `-norangeproofcache` runtime flag.
- **Fund Summary (as of 23 September 2026)**:
  - Affected: `399,602,000,000` sats (~3,996.02 BTC)
  - Recovered: `340,000,000,000` sats (3,400.00 BTC returned on block 965,950)
  - Outstanding: `59,602,000,000` sats (~596.02 BTC reported ~602 BTC)
  - Recovery Percentage: `85.08%`
- **Verified Transactions**:
  - Liquid Exploit Tx: `f24a4b179b5cc7e88b25a763911f7cbdf2bf45d1d1b5ab611e94461cef0a183f` (Liquid block 4,050,336)
  - Bitcoin Peg-out Release Tx: `8db751a650ae2f12006b7e8c69a75e4df360e8afd6b9e05ae0b9fa6458a7b140` (Bitcoin block 965,783)
  - Actor On-chain Msg Tx: `c103de95817b43f2df635ec6f35ff126ca26a7c6d20570c4b01866b2b3e69a19` (Bitcoin block 965,818)
  - Blockstream Response Tx: `91271efcbb5ab29abfc38ae635f0644e3ba042aad56f92d40136e1dde4742fe8` (Bitcoin block 965,822)
  - Return Tx: `a6d697a25266ce3c78774fd1d75f896b7af522ada209b0f6228ea497bc49a46d` (Bitcoin block 965,950)

---

## 6. Live Watch Targets & Chain Correlation Engine

The Live Incident Watch Engine actively correlates incoming Bitcoin network observations against monitored incident artifacts in real time.

### Watch Target Kinds
- `DirectOutpoint`: Specific `(txid, vout)` output proven to hold unspent incident funds (e.g. peg-out UTXO or change).
- `ScriptPubKey`: Exact script bytecode execution associated with incident contracts or federation multisig scripts.
- `Address`: Known Bitcoin address associated with the incident (e.g., disclosed federation recovery address or heuristic cluster).
- `TransactionRef`: Known transaction reference (e.g., OP_RETURN negotiation transactions).
- `DescendantOutpoint`: Dynamically enrolled unspent output generated by spending an existing watched target.

### Activity & Correlation Classification
- `WatchedUtxoSpent`: Direct cryptographic spend of an active watched incident UTXO (`Direct`).
- `TargetAddressReceived`: Funds received by a watched address (`Direct` for disclosed addresses, `Heuristic` for cluster addresses).
- `TargetAddressSent`: Funds sent from a watched address (`Direct` or `Heuristic`).
- `DescendantMoved`: Spend of a dynamically tracked descendant UTXO (`Direct` / `Heuristic`).
- `NewDescendantObserved`: Enrollment of a new descendant output generated from a watched spend.
- `CommunicationTxObserved`: Observed confirmed or mempool transaction referencing known incident communication (`Direct`).
- `HeuristicClusterActivity`: Movement associated with heuristic address clusters (`Heuristic`).

### Inviolable Correlation Invariants
1. **Movement != Recovery (Strict Recovery Immutability)**:
   - Moving watched incident funds on-chain is proof of *movement*, NOT proof of *recovery*.
   - The watch engine and ingestion pipeline MUST NEVER automatically modify, increment, or overwrite `incident.recovery.recovered_sats` based on on-chain activity detection alone.
   - An incident's recovery figures may only be adjusted via explicit, verified administrative updates incorporating official custodial confirmations.
2. **Deterministic Spend != Entity Ownership**:
   - Confirming a transaction that spends a watched outpoint proves execution of the cryptographic script authorizing that UTXO.
   - It does NOT prove who controls the destination address or that the actor has changed identities.
3. **Severity Caps on Heuristic Indicators**:
   - Alerts generated from `Heuristic` targets are strictly capped at `Medium` severity, preventing automated alert inflation from probabilistic clustering.
4. **Bounded Descendant Tracking**:
   - Dynamic UTXO child tracking is bounded to `OBSCHAIN_INCIDENT_FOLLOW_DEPTH` (default 3 hops, hard cap 5) to prevent combinatorial state explosions and tracking dilution.

