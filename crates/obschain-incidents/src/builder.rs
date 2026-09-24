use chrono::Utc;
use obschain_core::{
    CoreError, EventSeverity, Evidence, EvidenceType, Incident, IncidentStatus,
    ProvenanceClassification, Source, TimelineEvent,
};
use uuid::Uuid;

use crate::validation::validate_evidence_claim;

/// Builder to construct an Incident case while maintaining strict provenance standards.
pub struct IncidentBuilder {
    incident: Incident,
}

impl IncidentBuilder {
    pub fn new(
        title: impl Into<String>,
        summary: impl Into<String>,
        severity: EventSeverity,
    ) -> Self {
        let now = Utc::now();
        Self {
            incident: Incident {
                id: Uuid::new_v4(),
                title: title.into(),
                summary: summary.into(),
                status: IncidentStatus::Open,
                severity,
                total_btc_affected: 0.0,
                total_btc_recovered: 0.0,
                first_observed_at: now,
                last_updated_at: now,
                facts: Vec::new(),
                reported_claims: Vec::new(),
                unverified_claims: Vec::new(),
                associated_txids: Vec::new(),
                associated_block_heights: Vec::new(),
                timeline: Vec::new(),
                evidence: Vec::new(),
                sources: Vec::new(),
            },
        }
    }

    pub fn with_id(mut self, id: Uuid) -> Self {
        self.incident.id = id;
        self
    }

    pub fn status(mut self, status: IncidentStatus) -> Self {
        self.incident.status = status;
        self
    }

    pub fn btc_affected(mut self, btc: f64) -> Self {
        self.incident.total_btc_affected = btc;
        self
    }

    pub fn btc_recovered(mut self, btc: f64) -> Self {
        self.incident.total_btc_recovered = btc;
        self
    }

    pub fn add_fact(mut self, fact: impl Into<String>) -> Self {
        self.incident.facts.push(fact.into());
        self
    }

    pub fn add_reported_claim(mut self, claim: impl Into<String>) -> Self {
        self.incident.reported_claims.push(claim.into());
        self
    }

    pub fn add_unverified_claim(mut self, claim: impl Into<String>) -> Self {
        self.incident.unverified_claims.push(claim.into());
        self
    }

    pub fn add_txid(mut self, txid: impl Into<String>) -> Self {
        self.incident.associated_txids.push(txid.into());
        self
    }

    pub fn add_block_height(mut self, height: u64) -> Self {
        self.incident.associated_block_heights.push(height);
        self
    }

    pub fn add_evidence(
        mut self,
        evidence_type: EvidenceType,
        classification: ProvenanceClassification,
        description: impl Into<String>,
        reference: impl Into<String>,
        raw_data: Option<serde_json::Value>,
        is_ownership_claim: bool,
    ) -> Result<Self, CoreError> {
        let evidence = Evidence {
            id: Uuid::new_v4(),
            incident_id: self.incident.id,
            evidence_type,
            classification,
            description: description.into(),
            reference: reference.into(),
            raw_data,
            created_at: Utc::now(),
        };

        validate_evidence_claim(&evidence, is_ownership_claim)?;
        self.incident.evidence.push(evidence);
        Ok(self)
    }

    pub fn add_timeline_event(
        mut self,
        title: impl Into<String>,
        description: impl Into<String>,
        evidence_id: Option<Uuid>,
        classification: ProvenanceClassification,
    ) -> Self {
        self.incident.timeline.push(TimelineEvent {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            title: title.into(),
            description: description.into(),
            evidence_id,
            classification,
        });
        self
    }

    pub fn add_source(mut self, name: impl Into<String>, url: Option<String>, score: f32) -> Self {
        self.incident.sources.push(Source {
            id: Uuid::new_v4(),
            name: name.into(),
            url,
            reliability_score: score,
            published_at: Some(Utc::now()),
        });
        self
    }

    pub fn build(self) -> Incident {
        self.incident
    }
}
