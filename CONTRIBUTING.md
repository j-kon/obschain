# Contributing to ObsChain

Thank you for your interest in contributing to ObsChain!

## Design Principles

1. **Separation of Concerns**: Do not bundle frontend logic or monorepo tools into this repository.
2. **Provenance Integrity**: Any new heuristic or detection rule must be strictly labeled as `HEURISTIC`. Never conflate heuristic clustering with cryptographic on-chain verification.
3. **No Panics in Ingestion Paths**: Untrusted blockchain or network payloads must be parsed safely.

## Pull Request Checklist

Before submitting code, ensure the following commands pass:

```bash
cargo fmt --check
cargo check --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```
