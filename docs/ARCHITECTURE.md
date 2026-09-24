# ObsChain Architecture Specification

## Overview

ObsChain is an open-source Bitcoin network observation, anomaly-detection, incident-analysis, and on-chain intelligence engine.

It observes Bitcoin blocks and mempools, runs modular heuristic and statistical detectors, and models on-chain incidents with strict evidence provenance.

## Architectural Principles

1. **Strict Cryptographic Provenance**
   - Blockchain transactions, scripts, and block headers are mathematically verifiable facts.
   - Off-chain attribution, social claims, and clustering heuristics are explicitly demarcated as non-definitive.
   - Heuristics are NEVER converted to ground-truth facts.

2. **Decoupled Ingestion Hierarchy**
   - Ingestion is abstracted over multiple providers (`mempool.space` REST/WS, Bitcoin Core JSON-RPC, Bitcoin Core ZeroMQ).
   - Bitcoin Core is designed as the ultimate sovereign source of truth, with public API feeds available as convenience layers.

3. **Pluggable Anomaly Detection**
   - Detectors implement the `Detector` trait.
   - Execution is deterministic and decoupled from ingestion conduits.
   - Thresholds and statistical bounds are independently configurable.

4. **Zero Monorepo Entanglement**
   - The Rust backend (`obschain`) and the React frontend (`obschain-web`) are completely independent repositories.

## Workspace Crates

```text
crates/
├── obschain-core/          # Domain primitives, observations, events, incident types
├── obschain-detectors/     # Detection algorithms (Large transfers, block intervals, etc.)
├── obschain-ingest/        # Ingestion interfaces (RPC, ZMQ, REST, WebSocket)
├── obschain-incidents/     # Incident tracking, timeline building, and provenance verification
├── obschain-intelligence/  # Graph modeling, clustering models, report generator
└── obschain-storage/       # PostgreSQL / InMemory repository implementations
```

## System Topology

```
                  ┌──────────────────────┐
                  │  Bitcoin Core (RPC)  │
                  └──────────┬───────────┘
                             │
                             ▼
┌─────────────────┐   ┌──────────────┐   ┌──────────────────────┐
│  mempool.space  │──▶│obschain-ingest│◀──│  Bitcoin Core (ZMQ)  │
└─────────────────┘   └──────┬───────┘   └──────────────────────┘
                             │ Observations
                             ▼
                    ┌─────────────────┐
                    │obschain-detector│
                    └────────┬────────┘
                             │ ChainEvents
                             ▼
                    ┌─────────────────┐
                    │obschain-incident│
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ obschain-storage│
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  Axum REST API  │ (Port 8080)
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  obschain-web   │ (React Frontend)
                    └─────────────────┘
```
