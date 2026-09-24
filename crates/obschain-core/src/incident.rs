use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::event::EventSeverity;

/// Identifies the blockchain network for an incident or transaction.
/// Limited multi-chain context for Bitcoin sidechains and federated systems.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Chain {
    Bitcoin,
    Liquid,
}

impl std::fmt::Display for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bitcoin => write!(f, "Bitcoin"),
            Self::Liquid => write!(f, "Liquid"),
        }
    }
}

/// Provenance and verification level of an evidence item or claim.
/// Strict boundary between verifiable cryptographic facts, official disclosures,
/// and unverified or heuristic assertions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProvenanceClassification {
    /// Mathematically verified on the Bitcoin or sidechain blockchain (valid tx, block header, script execution).
    OnChainVerified,
    /// Officially claimed by a recognized authority, exchange, or asset holder via cryptographically signed message or official channel.
    OfficiallyAttributed,
    /// Reported by multiple credible security research organizations or forensic firms with verifiable methodology.
    #[serde(alias = "HIGH_CONFIDENCE_REPORTING")]
    ReputableReporting,
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

    /// Checks whether an author or actor identity can be considered verified.
    /// Self-attributions (e.g. claiming to be a white hat) are NOT verified identity.
    pub fn allows_verified_identity_claim(&self) -> bool {
        matches!(self, Self::OfficiallyAttributed)
    }
}

/// Status of an active or historical incident lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IncidentStatus {
    Detected,
    Investigating,
    Verified,
    Monitoring,
    Recovery,
    Resolved,
    Closed,
    // Backward compatibility aliases
    #[serde(alias = "OPEN")]
    Open,
    #[serde(alias = "MITIGATED")]
    Mitigated,
    #[serde(alias = "DISPUTED")]
    Disputed,
}

/// Categorized type of evidence linked to an incident.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EvidenceType {
    OnChainTransaction,
    OnChainBlock,
    MempoolSnapshot,
    OfficialStatement,
    OfficialTechnicalReport,
    SecurityAdvisory,
    SourceRepository,
    HeuristicCluster,
    DisputedClaim,
    IndependentReporting,
}

/// Category of external source or intelligence provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SourceCategory {
    BitcoinBlockchain,
    LiquidBlockchain,
    OfficialTechnicalReport,
    OfficialPublicStatement,
    SourceRepository,
    SecurityAdvisory,
    IndependentReporting,
}

/// External source or advisory reference with provenance metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Source {
    pub id: Uuid,
    pub publisher: String,
    pub title: String,
    pub url: Option<String>,
    pub publication_timestamp: Option<DateTime<Utc>>,
    pub retrieved_timestamp: Option<DateTime<Utc>>,
    pub source_category: SourceCategory,
    pub reliability_score: f32,
    #[serde(default)]
    pub name: Option<String>,
}

impl Source {
    pub fn new(
        publisher: impl Into<String>,
        title: impl Into<String>,
        category: SourceCategory,
        url: Option<String>,
        reliability_score: f32,
    ) -> Self {
        let now = Utc::now();
        let pub_str = publisher.into();
        Self {
            id: Uuid::new_v4(),
            publisher: pub_str.clone(),
            title: title.into(),
            url,
            publication_timestamp: Some(now),
            retrieved_timestamp: Some(now),
            source_category: category,
            reliability_score,
            name: Some(pub_str),
        }
    }
}

/// Individual piece of evidence linked to an incident case.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    #[serde(default)]
    pub reference: Option<String>,
    #[serde(default)]
    pub classification: Option<ProvenanceClassification>,
}

/// Role of a transaction in the context of an incident investigation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TransactionRole {
    Exploit,
    PegOut,
    Transfer,
    Forwarding,
    Return,
    Recovery,
    Communication,
    Other,
}

/// Structured transaction reference associated with an incident.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IncidentTransaction {
    pub chain: Chain,
    pub txid: String,
    pub role: TransactionRole,
    pub amount_sats: Option<u64>,
    pub block_height: Option<u64>,
    pub block_hash: Option<String>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub evidence_id: Option<Uuid>,
    pub notes: Option<String>,
}

/// Structured block reference associated with an incident.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IncidentBlock {
    pub chain: Chain,
    pub height: u64,
    pub hash: String,
    pub timestamp: DateTime<Utc>,
    pub tx_count: Option<usize>,
    pub evidence_id: Option<Uuid>,
}

/// An entity (individual, organization, service, or federation) involved in an incident.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IncidentEntity {
    pub id: Uuid,
    pub name: String,
    pub entity_type: String,
    pub description: String,
    pub attribution_confidence: ProvenanceClassification,
}

/// An on-chain message extracted from transaction script data (e.g. OP_RETURN).
/// Separates message content from attributed sender identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

/// Category of a timeline milestone during an incident.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TimelineCategory {
    Exploit,
    OnChainMovement,
    Discovery,
    Containment,
    Communication,
    Disclosure,
    Patch,
    Recovery,
    NetworkRestart,
    Other,
}

/// Timeline entry tracking milestones during an incident.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimelineEntry {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub title: String,
    pub description: String,
    pub category: TimelineCategory,
    pub source_id: Option<Uuid>,
    pub evidence_ids: Vec<Uuid>,
    pub transaction_txids: Vec<String>,
    pub block_heights: Vec<u64>,
    pub classification: ProvenanceClassification,
}

/// Legacy timeline event definition for backward compatibility.
pub type TimelineEvent = TimelineEntry;

/// Incident-level fund tracking summary.
/// All monetary values originate from integer satoshis using checked arithmetic.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecoverySummary {
    pub affected_sats: u64,
    pub recovered_sats: u64,
    pub outstanding_sats: u64,
    pub as_of_timestamp: DateTime<Utc>,
    pub source: Option<String>,
    pub is_estimate: bool,
}

impl RecoverySummary {
    pub fn new(affected_sats: u64, recovered_sats: u64, as_of: DateTime<Utc>) -> Self {
        let outstanding_sats = affected_sats.saturating_sub(recovered_sats);
        Self {
            affected_sats,
            recovered_sats,
            outstanding_sats,
            as_of_timestamp: as_of,
            source: None,
            is_estimate: false,
        }
    }

    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    pub fn with_estimate(mut self, is_estimate: bool) -> Self {
        self.is_estimate = is_estimate;
        self
    }

    pub fn recovery_percentage(&self) -> f64 {
        if self.affected_sats == 0 {
            0.0
        } else {
            (self.recovered_sats as f64 / self.affected_sats as f64) * 100.0
        }
    }

    pub fn affected_btc(&self) -> f64 {
        self.affected_sats as f64 / 100_000_000.0
    }

    pub fn recovered_btc(&self) -> f64 {
        self.recovered_sats as f64 / 100_000_000.0
    }

    pub fn outstanding_btc(&self) -> f64 {
        self.outstanding_sats as f64 / 100_000_000.0
    }
}

/// Specific fund transfer milestone.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundMovement {
    pub id: Uuid,
    pub txid: String,
    pub chain: Chain,
    pub amount_sats: u64,
    pub from_label: Option<String>,
    pub to_label: Option<String>,
    pub movement_type: TransactionRole,
    pub timestamp: DateTime<Utc>,
    pub classification: ProvenanceClassification,
}

/// Append-only update to an ongoing or historical incident.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IncidentUpdate {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub title: String,
    pub summary: String,
    pub source_id: Option<Uuid>,
    pub recovery_state: Option<RecoverySummary>,
}

/// Structured technical finding describing the root cause and remediation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TechnicalFinding {
    pub component: String,
    pub area: String,
    pub category: String,
    pub summary: String,
    pub root_cause_details: String,
    pub fix_summary: String,
    pub repository_url: Option<String>,
    pub pull_request_id: Option<String>,
    pub commit_hash: Option<String>,
}

/// Structured summary categorizing incident observations into strict certainty tiers.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StructuredClaimsSummary {
    pub verified_on_chain: Vec<String>,
    pub officially_attributed: Vec<String>,
    pub reported: Vec<String>,
    pub heuristic: Vec<String>,
    pub unknown: Vec<String>,
}

/// Type of node in an incident relationship graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum GraphNodeType {
    Transaction,
    Block,
    Address,
    Entity,
    Source,
    Evidence,
}

/// Node in an incident relationship graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphNode {
    pub id: String,
    pub label: String,
    pub node_type: GraphNodeType,
    pub chain: Option<Chain>,
    pub metadata: Option<serde_json::Value>,
}

/// Type of edge in an incident relationship graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum GraphEdgeType {
    Spends,
    ConfirmedIn,
    ForwardsTo,
    ReturnsTo,
    References,
    Supports,
    AttributedTo,
    PossiblyRelated,
}

/// Edge connecting two nodes in an incident relationship graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphEdge {
    pub source: String,
    pub target: String,
    pub relationship: GraphEdgeType,
    pub confidence: ProvenanceClassification,
}

/// Relationship graph representing forensic connections between transactions,
/// blocks, entities, sources, and evidence.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct IncidentGraph {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

impl IncidentGraph {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    pub fn add_node(&mut self, node: GraphNode) {
        if !self.nodes.iter().any(|n| n.id == node.id) {
            self.nodes.push(node);
        }
    }

    pub fn add_edge(&mut self, edge: GraphEdge) {
        self.edges.push(edge);
    }
}

/// Complete Incident investigation dossier.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Incident {
    pub id: Uuid,
    pub case_id: String,
    pub title: String,
    pub summary: String,
    pub status: IncidentStatus,
    pub severity: EventSeverity,
    pub recovery: RecoverySummary,
    pub first_observed_at: DateTime<Utc>,
    pub last_updated_at: DateTime<Utc>,
    pub structured_claims: StructuredClaimsSummary,
    pub entities: Vec<IncidentEntity>,
    pub transactions: Vec<IncidentTransaction>,
    pub blocks: Vec<IncidentBlock>,
    pub on_chain_messages: Vec<OnChainMessage>,
    pub timeline: Vec<TimelineEntry>,
    pub evidence: Vec<Evidence>,
    pub sources: Vec<Source>,
    pub technical_findings: Vec<TechnicalFinding>,
    pub updates: Vec<IncidentUpdate>,
    pub graph: IncidentGraph,

    // Legacy fields for backward compatibility with existing API responses and frontend:
    #[serde(default)]
    pub total_btc_affected: f64,
    #[serde(default)]
    pub total_btc_recovered: f64,
    #[serde(default)]
    pub facts: Vec<String>,
    #[serde(default)]
    pub reported_claims: Vec<String>,
    #[serde(default)]
    pub unverified_claims: Vec<String>,
    #[serde(default)]
    pub associated_txids: Vec<String>,
    #[serde(default)]
    pub associated_block_heights: Vec<u64>,
}
