use obschain_core::ProvenanceClassification;
use serde::{Deserialize, Serialize};

/// Address cluster heuristic representation.
/// Explicitly tagged as HEURISTIC to satisfy security and architectural integrity rules.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddressCluster {
    pub cluster_id: String,
    pub primary_addresses: Vec<String>,
    pub heuristic_type: String,
    pub confidence_score: f32,
    pub classification: ProvenanceClassification,
    pub caveats: String,
}

impl AddressCluster {
    pub fn new(
        cluster_id: impl Into<String>,
        addresses: Vec<String>,
        heuristic_type: impl Into<String>,
        confidence_score: f32,
    ) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            primary_addresses: addresses,
            heuristic_type: heuristic_type.into(),
            confidence_score,
            classification: ProvenanceClassification::Heuristic,
            caveats:
                "Heuristic clustering output. Never represents definitive ownership of addresses."
                    .to_string(),
        }
    }
}
