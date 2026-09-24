use std::collections::HashSet;

use obschain_core::{CoreError, Evidence, Incident, IncidentGraph, ProvenanceClassification};

/// Enforces ObsChain core invariant: Heuristic claims must NEVER be recorded as definitive facts.
pub fn validate_evidence_claim(
    evidence: &Evidence,
    is_definitive_ownership_claim: bool,
) -> Result<(), CoreError> {
    if is_definitive_ownership_claim && !evidence.confidence.allows_definitive_ownership_claim() {
        return Err(CoreError::ProvenanceViolation(format!(
            "Address ownership claim cannot be made with classification '{:?}'. Requires ON_CHAIN_VERIFIED or OFFICIALLY_ATTRIBUTED.",
            evidence.confidence
        )));
    }
    Ok(())
}

/// Enforces that verified facts on an incident must have on-chain cryptographic backing.
pub fn require_on_chain_backing(classification: ProvenanceClassification) -> Result<(), CoreError> {
    if !classification.is_cryptographically_verified() {
        return Err(CoreError::ProvenanceViolation(
            "Only ON_CHAIN_VERIFIED observations can be registered as verifiable facts."
                .to_string(),
        ));
    }
    Ok(())
}

/// Enforces that a self-attributed on-chain message (e.g. self-described "white hat")
/// cannot be accepted as verified actor identity.
pub fn validate_sender_identity_claim(
    is_self_attributed: bool,
    claimed_classification: ProvenanceClassification,
) -> Result<(), CoreError> {
    if is_self_attributed && claimed_classification.is_cryptographically_verified() {
        return Err(CoreError::ProvenanceViolation(
            "Self-attributed on-chain communication cannot be classified as ON_CHAIN_VERIFIED identity. Message existence is verified on-chain, but identity attribution remains UNVERIFIED or HEURISTIC."
                .to_string(),
        ));
    }
    Ok(())
}

/// Validates relationship graph integrity:
/// 1. Node uniqueness (no duplicate node IDs).
/// 2. No dangling edges (every edge source and target must exist in the node set).
pub fn validate_incident_graph(graph: &IncidentGraph) -> Result<(), CoreError> {
    let mut node_ids = HashSet::new();

    for node in &graph.nodes {
        if !node_ids.insert(&node.id) {
            return Err(CoreError::Validation(format!(
                "Duplicate graph node id detected: '{}'",
                node.id
            )));
        }
    }

    for edge in &graph.edges {
        if !node_ids.contains(&edge.source) {
            return Err(CoreError::Validation(format!(
                "Dangling graph edge: source node '{}' does not exist in graph nodes",
                edge.source
            )));
        }
        if !node_ids.contains(&edge.target) {
            return Err(CoreError::Validation(format!(
                "Dangling graph edge: target node '{}' does not exist in graph nodes",
                edge.target
            )));
        }
    }

    Ok(())
}

/// Validates fund tracking arithmetic for an incident case.
pub fn validate_fund_arithmetic(incident: &Incident) -> Result<(), CoreError> {
    let recovery = &incident.recovery;

    if recovery.recovered_sats > recovery.affected_sats {
        return Err(CoreError::Validation(format!(
            "Invalid fund balance: recovered_sats ({}) exceeds affected_sats ({})",
            recovery.recovered_sats, recovery.affected_sats
        )));
    }

    let calculated_outstanding = recovery
        .affected_sats
        .saturating_sub(recovery.recovered_sats);
    if !recovery.is_estimate && recovery.outstanding_sats != calculated_outstanding {
        return Err(CoreError::Validation(format!(
            "Fund balance discrepancy: recorded outstanding_sats ({}) does not match affected - recovered ({})",
            recovery.outstanding_sats, calculated_outstanding
        )));
    }

    Ok(())
}

/// Performs comprehensive validation of an entire Incident case dossier.
pub fn validate_incident(incident: &Incident) -> Result<(), CoreError> {
    // 1. Validate fund arithmetic
    validate_fund_arithmetic(incident)?;

    // 2. Validate graph integrity
    validate_incident_graph(&incident.graph)?;

    // 3. Validate timeline entries have chronologically valid references
    let mut prev_ts = None;
    for entry in &incident.timeline {
        if let Some(prev) = prev_ts {
            if entry.timestamp < prev {
                return Err(CoreError::Validation(format!(
                    "Timeline entries must be sorted chronologically: entry '{}' ({}) occurs before previous entry ({})",
                    entry.title, entry.timestamp, prev
                )));
            }
        }
        prev_ts = Some(entry.timestamp);
    }

    // 4. Validate evidence references
    let mut evidence_ids = HashSet::new();
    for ev in &incident.evidence {
        if !evidence_ids.insert(ev.id) {
            return Err(CoreError::Validation(format!(
                "Duplicate evidence ID detected: '{}'",
                ev.id
            )));
        }
    }

    // 5. Check on-chain messages
    for msg in &incident.on_chain_messages {
        if msg.attributed_sender.is_some() {
            validate_sender_identity_claim(true, msg.sender_attribution_confidence)?;
        }
    }

    // 6. Check incident updates are in chronological order
    let mut prev_update_ts = None;
    for update in &incident.updates {
        if let Some(prev) = prev_update_ts {
            if update.timestamp < prev {
                return Err(CoreError::Validation(format!(
                    "Incident updates must be in chronological order: update '{}' ({}) occurs before previous update ({})",
                    update.title, update.timestamp, prev
                )));
            }
        }
        prev_update_ts = Some(update.timestamp);
    }

    Ok(())
}
