use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Severity classification of an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventSeverity {
    Info,
    Low,
    Medium,
    High,
    Critical,
}

/// Confidence rating of the observation or attribution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConfidenceLevel {
    VerifiedOnChain,
    High,
    Moderate,
    Heuristic,
    Low,
}

/// Categorized type of chain event detected.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventType {
    LargeTransfer,
    DormantUtxoSpent,
    Consolidation,
    FanOut,
    FeeSpike,
    RbfReplacement,
    LongBlockInterval,
    ReorgDetected,
    MiningAnomaly,
    UnusualFeeRatio,
}

/// An anomaly, pattern, or significant occurrence detected on the Bitcoin network.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChainEvent {
    pub id: Uuid,
    pub event_type: EventType,
    pub severity: EventSeverity,
    pub confidence: ConfidenceLevel,
    pub title: String,
    pub description: String,
    pub detected_at: DateTime<Utc>,
    pub block_height: Option<u64>,
    pub block_hash: Option<String>,
    pub txid: Option<String>,
    pub metadata: serde_json::Value,
}

impl ChainEvent {
    pub fn new(
        event_type: EventType,
        severity: EventSeverity,
        confidence: ConfidenceLevel,
        title: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            event_type,
            severity,
            confidence,
            title: title.into(),
            description: description.into(),
            detected_at: Utc::now(),
            block_height: None,
            block_hash: None,
            txid: None,
            metadata: serde_json::json!({}),
        }
    }
}
