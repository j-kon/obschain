# ObsChain Incident & Provenance Model

## Purpose

The Incident Subsystem provides case management and longitudinal investigation tracking for significant Bitcoin network events (thefts, exchange exploits, protocol anomalies, dormant whale awakenings, large-scale consolidations).

## Provenance Classification Hierarchy

ObsChain mandates an inviolable boundary between verified blockchain truth and external assertions:

1. `ON_CHAIN_VERIFIED`:
   - Cryptographically proven via valid Bitcoin transaction inputs/outputs, script validation, or consensus block inclusion.
   - Ground truth.

2. `OFFICIALLY_ATTRIBUTED`:
   - Confirmed by cryptographic signatures (e.g. key signed messages, proof of reserves) or published official advisories by known custodians/entities.

3. `HIGH_CONFIDENCE_REPORTING`:
   - Published by reputable intelligence research teams with transparent methodology, corroborated across independent sources.

4. `HEURISTIC`:
   - Derived from address clustering (common-input ownership, change address detection, peel chains).
   - **Crucial Rule:** Heuristics must NEVER be presented as ground truth or definitive address ownership.

5. `UNVERIFIED`:
   - Social media assertions, rumors, or unvetted single-source claims.

6. `DISPUTED`:
   - Claims contested by counterparties or contradicting on-chain evidence.

## Tripartite Separation of Knowledge

Every ObsChain Incident strictly bifurcates its narrative into:

- **Observed Facts**: Immutable, on-chain verified cryptographic occurrences.
- **Reported Information**: Corroborated external analysis and official disclosures.
- **Unverified Claims**: Speculation, community hypotheses, and raw heuristic links.
