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
    #[serde(alias = "DORMANT_UTXO_SPENT")]
    DormantCoinsMoved,
    Consolidation,
    FanOut,
    #[serde(alias = "FEE_SPIKE")]
    ExtremeFee,
    #[serde(alias = "RBF_REPLACEMENT")]
    TransactionReplacement,
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
    #[serde(default)]
    pub source: Option<crate::source::ObservationSource>,
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
            source: None,
            metadata: serde_json::json!({}),
        }
    }

    /// Sets typed metadata serialized as JSON value.
    pub fn with_typed_metadata<T: Serialize>(mut self, meta: &T) -> Self {
        if let Ok(val) = serde_json::to_value(meta) {
            self.metadata = val;
        }
        self
    }
}

/// Classification of dormant coin age based on objective historical age/date.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DormantClassification {
    /// 5+ years dormant
    Dormant,
    /// 10+ years dormant
    VeryOld,
    /// 15+ years dormant
    Ancient,
    /// Mined / confirmed in early Bitcoin era (e.g. before 2011 or block <= 100,000)
    EarlyBitcoin,
}

/// Typed structured metadata for `DormantCoinsMoved` events.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DormantCoinsMetadata {
    pub total_dormant_sats: u64,
    pub total_dormant_btc: f64,
    pub dormant_input_count: u32,
    pub total_input_count: u32,
    pub oldest_input_age_days: u64,
    pub oldest_input_age_seconds: u64,
    pub youngest_qualifying_age_days: u64,
    pub total_input_sats: u64,
    pub total_output_sats: u64,
    pub dormant_ratio: f64,
    pub coin_age_destroyed_sats_days: u128,
    pub coin_age_destroyed_btc_days: f64,
    pub coin_age_destroyed_btc_years: f64,
    pub classification: DormantClassification,
}

/// Typed structured metadata for `Consolidation` events.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConsolidationMetadata {
    pub input_count: u32,
    pub output_count: u32,
    pub input_output_ratio: f64,
    pub total_input_sats: u64,
    pub total_output_sats: u64,
    pub fee_sats: u64,
}

/// Typed structured metadata for `FanOut` events.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FanOutMetadata {
    pub input_count: u32,
    pub output_count: u32,
    pub output_input_ratio: f64,
    pub total_distributed_sats: u64,
    pub total_distributed_btc: f64,
    pub median_output_sats: u64,
    pub smallest_output_sats: u64,
    pub largest_output_sats: u64,
}

/// Trigger reason for `ExtremeFee` events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExtremeFeeTriggerType {
    HighAbsoluteFee,
    HighFeeRate,
    Both,
}

/// Typed structured metadata for `ExtremeFee` events.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtremeFeeMetadata {
    pub fee_sats: u64,
    pub fee_btc: f64,
    pub fee_rate_sat_vb: Option<f64>,
    pub vsize: u64,
    pub total_input_sats: u64,
    pub total_output_sats: u64,
    pub fee_trigger_type: ExtremeFeeTriggerType,
}

/// Typed structured metadata for `TransactionReplacement` events.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplacementMetadata {
    pub replaced_txids: Vec<String>,
    pub replacement_txid: String,
    pub replaced_count: usize,
    pub old_fee_sats: u64,
    pub new_fee_sats: u64,
    pub fee_delta_sats: i64,
    pub fee_increase_percent: Option<f64>,
    pub old_fee_rate_sat_vb: Option<f64>,
    pub new_fee_rate_sat_vb: Option<f64>,
}
