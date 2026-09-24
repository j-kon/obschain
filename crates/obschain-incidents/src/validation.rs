use obschain_core::{CoreError, Evidence, ProvenanceClassification};

/// Enforces ObsChain core invariant: Heuristic claims must NEVER be recorded as definitive facts.
pub fn validate_evidence_claim(
    evidence: &Evidence,
    is_definitive_ownership_claim: bool,
) -> Result<(), CoreError> {
    if is_definitive_ownership_claim && !evidence.classification.allows_definitive_ownership_claim()
    {
        return Err(CoreError::ProvenanceViolation(format!(
            "Address ownership claim cannot be made with classification '{:?}'. Requires ON_CHAIN_VERIFIED or OFFICIALLY_ATTRIBUTED.",
            evidence.classification
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

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use obschain_core::EvidenceType;
    use uuid::Uuid;

    use super::*;

    #[test]
    fn test_rejects_heuristic_address_ownership() {
        let evidence = Evidence {
            id: Uuid::new_v4(),
            incident_id: Uuid::new_v4(),
            evidence_type: EvidenceType::HeuristicCluster,
            classification: ProvenanceClassification::Heuristic,
            description: "Clustered wallet via common input heuristic".to_string(),
            reference: "cluster-heuristics-v1".to_string(),
            raw_data: None,
            created_at: Utc::now(),
        };

        let result = validate_evidence_claim(&evidence, true);
        assert!(result.is_err());
    }

    #[test]
    fn test_allows_officially_attributed_ownership() {
        let evidence = Evidence {
            id: Uuid::new_v4(),
            incident_id: Uuid::new_v4(),
            evidence_type: EvidenceType::OfficialStatement,
            classification: ProvenanceClassification::OfficiallyAttributed,
            description: "Proof of reserves message signed with cold wallet key".to_string(),
            reference: "https://proof-of-reserves.example.com".to_string(),
            raw_data: None,
            created_at: Utc::now(),
        };

        let result = validate_evidence_claim(&evidence, true);
        assert!(result.is_ok());
    }
}
