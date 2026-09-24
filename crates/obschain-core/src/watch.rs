use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{event::EventSeverity, incident::ProvenanceClassification, source::ObservationSource};

/// Strongly typed watch target for deterministic on-chain incident correlation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(
    tag = "target_type",
    content = "details",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
pub enum WatchTargetKind {
    /// Exact transaction outpoint (txid + vout). Cryptographically deterministic.
    OutPoint { txid: String, vout: u32 },
    /// Exact scriptPubKey hex representation. Deterministic at the script consensus level.
    ScriptPubKey { script_hex: String },
    /// An address monitored by an incident with a provenance rating.
    /// Heuristically related addresses MUST NEVER be treated as establishing ownership.
    Address {
        address: String,
        attribution: ProvenanceClassification,
    },
    /// A specific transaction hash being monitored (e.g. for confirmation, RBF replacement, or descendant tracking).
    Transaction { txid: String },
}

impl WatchTargetKind {
    pub fn target_type_str(&self) -> &'static str {
        match self {
            Self::OutPoint { .. } => "OUTPOINT",
            Self::ScriptPubKey { .. } => "SCRIPT_PUBKEY",
            Self::Address { .. } => "ADDRESS",
            Self::Transaction { .. } => "TRANSACTION",
        }
    }

    pub fn summary(&self) -> String {
        match self {
            Self::OutPoint { txid, vout } => {
                let short_tx = if txid.len() > 12 {
                    format!("{}...{}", &txid[..6], &txid[txid.len() - 6..])
                } else {
                    txid.clone()
                };
                format!("OutPoint: {}:{}", short_tx, vout)
            }
            Self::ScriptPubKey { script_hex } => {
                let short_script = if script_hex.len() > 16 {
                    format!(
                        "{}...{}",
                        &script_hex[..8],
                        &script_hex[script_hex.len() - 8..]
                    )
                } else {
                    script_hex.clone()
                };
                format!("Script: {}", short_script)
            }
            Self::Address {
                address,
                attribution,
            } => {
                let short_addr = if address.len() > 14 {
                    format!("{}...{}", &address[..6], &address[address.len() - 6..])
                } else {
                    address.clone()
                };
                format!("Address: {} [{:?}]", short_addr, attribution)
            }
            Self::Transaction { txid } => {
                let short_tx = if txid.len() > 12 {
                    format!("{}...{}", &txid[..6], &txid[txid.len() - 6..])
                } else {
                    txid.clone()
                };
                format!("Tx: {}", short_tx)
            }
        }
    }
}

/// An object or reference monitored in the context of an incident investigation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchTarget {
    pub id: Uuid,
    pub incident_id: Uuid,
    pub case_id: String,
    pub kind: WatchTargetKind,
    pub classification: ProvenanceClassification,
    pub source: String,
    pub evidence_id: Option<Uuid>,
    pub label: Option<String>,
    pub created_at: DateTime<Utc>,
    pub active: bool,
}

impl WatchTarget {
    pub fn new_outpoint(
        incident_id: Uuid,
        case_id: impl Into<String>,
        txid: impl Into<String>,
        vout: u32,
        classification: ProvenanceClassification,
        source: impl Into<String>,
        label: Option<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            incident_id,
            case_id: case_id.into(),
            kind: WatchTargetKind::OutPoint {
                txid: txid.into(),
                vout,
            },
            classification,
            source: source.into(),
            evidence_id: None,
            label,
            created_at: Utc::now(),
            active: true,
        }
    }

    pub fn new_script(
        incident_id: Uuid,
        case_id: impl Into<String>,
        script_hex: impl Into<String>,
        classification: ProvenanceClassification,
        source: impl Into<String>,
        label: Option<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            incident_id,
            case_id: case_id.into(),
            kind: WatchTargetKind::ScriptPubKey {
                script_hex: script_hex.into(),
            },
            classification,
            source: source.into(),
            evidence_id: None,
            label,
            created_at: Utc::now(),
            active: true,
        }
    }

    pub fn new_address(
        incident_id: Uuid,
        case_id: impl Into<String>,
        address: impl Into<String>,
        classification: ProvenanceClassification,
        source: impl Into<String>,
        label: Option<String>,
    ) -> Self {
        let addr = address.into();
        Self {
            id: Uuid::new_v4(),
            incident_id,
            case_id: case_id.into(),
            kind: WatchTargetKind::Address {
                address: addr,
                attribution: classification,
            },
            classification,
            source: source.into(),
            evidence_id: None,
            label,
            created_at: Utc::now(),
            active: true,
        }
    }

    pub fn new_transaction(
        incident_id: Uuid,
        case_id: impl Into<String>,
        txid: impl Into<String>,
        classification: ProvenanceClassification,
        source: impl Into<String>,
        label: Option<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            incident_id,
            case_id: case_id.into(),
            kind: WatchTargetKind::Transaction { txid: txid.into() },
            classification,
            source: source.into(),
            evidence_id: None,
            label,
            created_at: Utc::now(),
            active: true,
        }
    }

    pub fn with_evidence_id(mut self, evidence_id: Uuid) -> Self {
        self.evidence_id = Some(evidence_id);
        self
    }

    /// Converts to safe public metadata representation for external API exposure.
    /// Redacts internal or sensitive investigation configuration while providing
    /// transparent provenance and target type classification.
    pub fn to_public_metadata(&self) -> PublicWatchTarget {
        PublicWatchTarget {
            id: self.id,
            incident_id: self.incident_id,
            case_id: self.case_id.clone(),
            target_type: self.kind.target_type_str().to_string(),
            target_summary: self.kind.summary(),
            classification: self.classification,
            label: self.label.clone(),
            created_at: self.created_at,
            active: self.active,
        }
    }
}

/// Redacted, safe public watch target metadata for API consumers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicWatchTarget {
    pub id: Uuid,
    pub incident_id: Uuid,
    pub case_id: String,
    pub target_type: String,
    pub target_summary: String,
    pub classification: ProvenanceClassification,
    pub label: Option<String>,
    pub created_at: DateTime<Utc>,
    pub active: bool,
}

/// Category of on-chain activity observed relating to an incident watch target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IncidentActivityType {
    WatchedTransactionObserved,
    WatchedTransactionConfirmed,
    WatchedOutpointSpent,
    WatchedScriptReceived,
    WatchedAddressReceived,
    WatchedAddressSpent,
    TransactionReplacement,
    NewDescendantObserved,
}

/// Lifecycle status of an activity detection (mempool observation vs confirmed vs reorged).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ActivityStatus {
    Mempool,
    Confirmed,
    ReorgedOut,
}

/// Epistemological strength of the correlation between the activity and the incident.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CorrelationStrength {
    /// Deterministic cryptographic UTXO spend or script match.
    Direct,
    /// Structural transaction DAG relation (e.g. descendant within bounded depth or RBF replacement).
    Structural,
    /// Statistical clustering or co-spend heuristic. Does NOT prove wallet ownership.
    Heuristic,
}

/// Observed on-chain activity correlating with an incident watch target.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IncidentActivity {
    pub id: Uuid,
    pub incident_id: Uuid,
    pub case_id: String,
    pub activity_type: IncidentActivityType,
    pub observed_at: DateTime<Utc>,
    pub trigger_txid: Option<String>,
    pub block_height: Option<u64>,
    pub block_hash: Option<String>,
    pub value_sats: Option<u64>,
    pub watch_target_id: Uuid,
    pub confidence: ProvenanceClassification,
    pub correlation_strength: CorrelationStrength,
    pub status: ActivityStatus,
    pub source: ObservationSource,
    pub evidence: Vec<Uuid>,
    pub description: String,
    pub details: Option<serde_json::Value>,
    /// Deterministic deduplication key: incident_id:activity_type:trigger_txid:watch_target_id
    pub dedup_key: String,
}

impl IncidentActivity {
    pub fn generate_dedup_key(
        incident_id: &Uuid,
        activity_type: &IncidentActivityType,
        trigger_txid: Option<&str>,
        watch_target_id: &Uuid,
    ) -> String {
        format!(
            "{}:{:?}:{}:{}",
            incident_id,
            activity_type,
            trigger_txid.unwrap_or("none"),
            watch_target_id
        )
    }
}

/// Higher-level public alert generated when significant incident activity is detected.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IncidentAlert {
    pub id: Uuid,
    pub incident_id: Uuid,
    pub case_id: String,
    pub incident_title: String,
    pub activity_id: Uuid,
    pub severity: EventSeverity,
    pub title: String,
    pub summary: String,
    pub confidence: ProvenanceClassification,
    pub correlation_strength: CorrelationStrength,
    pub observed_at: DateTime<Utc>,
    pub value_sats: Option<u64>,
    pub trigger_txid: Option<String>,
}
