use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::event::EventSeverity;

/// Provenance and verification level of an evidence item or claim.
/// Strict boundary between verifiable cryptographic facts and external assertions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProvenanceClassification {
    /// Mathematically verified on the Bitcoin blockchain (e.g. valid tx, block header, script execution).
    OnChainVerified,
    /// Officially claimed by a recognized authority, exchange, or asset holder via cryptographically signed message or official channel.
    OfficiallyAttributed,
    /// Reported by multiple credible security research organizations or forensic firms with methodology provided.
    HighConfidenceReporting,
    /// Inferred via clustering algorithms, change heuristics, or pattern analysis. Never treated as fact.
    Heuristic,
    /// Asserted by public social media or single unverified reports.
    Unverified,
    /// Contested by affected parties or conflicting data.
    Disputed,
}

impl ProvenanceClassification {
    /// Returns true if this piece of evidence is immutable and cryptographically confirmed on-chain.
    pub fn is_cryptographically_verified(&self) -> bool {
        matches!(self, Self::OnChainVerified)
    }

    /// Checks whether an address attribution can be considered definitive.
    /// Heuristics MUST NOT ever be considered definitive address ownership.
    pub fn allows_definitive_ownership_claim(&self) -> bool {
        matches!(self, Self::OnChainVerified | Self::OfficiallyAttributed)
    }
}

/// Status of an active or historical incident.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IncidentStatus {
    Open,
    Investigating,
    Mitigated,
    Closed,
    Disputed,
}

/// Type of evidence linked to an incident.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EvidenceType {
    OnChainTransaction,
    OnChainBlock,
    MempoolSnapshot,
    OfficialStatement,
    SecurityAdvisory,
    HeuristicCluster,
    DisputedClaim,
}

/// Individual piece of evidence linked to an incident case.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Evidence {
    pub id: Uuid,
    pub incident_id: Uuid,
    pub evidence_type: EvidenceType,
    pub classification: ProvenanceClassification,
    pub description: String,
    pub reference: String,
    pub raw_data: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

/// Timeline entry tracking milestones during an incident.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimelineEvent {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub title: String,
    pub description: String,
    pub evidence_id: Option<Uuid>,
    pub classification: ProvenanceClassification,
}

/// External source or advisory reference.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Source {
    pub id: Uuid,
    pub name: String,
    pub url: Option<String>,
    pub reliability_score: f32,
    pub published_at: Option<DateTime<Utc>>,
}

/// Complete Incident investigation record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Incident {
    pub id: Uuid,
    pub title: String,
    pub summary: String,
    pub status: IncidentStatus,
    pub severity: EventSeverity,
    pub total_btc_affected: f64,
    pub total_btc_recovered: f64,
    pub first_observed_at: DateTime<Utc>,
    pub last_updated_at: DateTime<Utc>,
    /// Verifiable facts confirmed directly on-chain.
    pub facts: Vec<String>,
    /// Reported information from external security researchers and official channels.
    pub reported_claims: Vec<String>,
    /// Unverified or community assertions requiring further verification.
    pub unverified_claims: Vec<String>,
    pub associated_txids: Vec<String>,
    pub associated_block_heights: Vec<u64>,
    pub timeline: Vec<TimelineEvent>,
    pub evidence: Vec<Evidence>,
    pub sources: Vec<Source>,
}
