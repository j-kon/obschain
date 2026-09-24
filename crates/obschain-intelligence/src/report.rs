use chrono::{DateTime, Utc};
use obschain_core::Incident;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// An ObsChain Observed Report: Structured intelligence dossier summarizing incident findings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservedReport {
    pub report_id: Uuid,
    pub incident_id: Uuid,
    pub title: String,
    pub generated_at: DateTime<Utc>,
    pub executive_summary: String,
    pub verified_on_chain_facts: Vec<String>,
    pub external_claims_and_reporting: Vec<String>,
    pub unverified_claims: Vec<String>,
    pub total_btc_affected: f64,
    pub total_btc_recovered: f64,
    pub methodology_notes: String,
}

impl ObservedReport {
    pub fn from_incident(incident: &Incident) -> Self {
        Self {
            report_id: Uuid::new_v4(),
            incident_id: incident.id,
            title: format!("ObsChain Observed Report: {}", incident.title),
            generated_at: Utc::now(),
            executive_summary: incident.summary.clone(),
            verified_on_chain_facts: incident.facts.clone(),
            external_claims_and_reporting: incident.reported_claims.clone(),
            unverified_claims: incident.unverified_claims.clone(),
            total_btc_affected: incident.total_btc_affected,
            total_btc_recovered: incident.total_btc_recovered,
            methodology_notes: "ObsChain separates verified on-chain cryptographical proof from external reporting. External claims do not establish blockchain fact."
                .to_string(),
        }
    }
}
