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

/// An occurrence or witness of a chain event, recording how and when ObsChain learned about it.
/// Separates observation provenance from intrinsic canonical event identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventObservation {
    pub id: Uuid,
    pub event_id: Uuid,
    pub mode: crate::replay::ObservationMode,
    pub source: crate::source::ObservationSource,
    pub observed_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bitcoin_time: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_job_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_height: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub witness: Option<crate::source::ObservationWitness>,
}

impl EventObservation {
    /// Computes a stable, deterministic UUID for this observation occurrence.
    /// Replay observations are unique on (event_id, replay_job_id).
    /// Live observations are unique on (event_id, mode, provider, transport).
    pub fn deterministic_id(
        event_id: Uuid,
        mode: crate::replay::ObservationMode,
        source: &crate::source::ObservationSource,
        replay_job_id: Option<Uuid>,
    ) -> Uuid {
        let key = match (mode, replay_job_id) {
            (crate::replay::ObservationMode::HistoricalReplay, Some(job_id)) => {
                format!("obs:{event_id}:replay:{job_id}")
            }
            _ => {
                format!(
                    "obs:{event_id}:{}:{}:{}",
                    mode.as_str(),
                    source.provider,
                    source.transport
                )
            }
        };
        Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_bytes())
    }

    /// Creates a new LIVE observation record.
    pub fn live(
        event_id: Uuid,
        source: crate::source::ObservationSource,
        observed_at: DateTime<Utc>,
        bitcoin_time: Option<DateTime<Utc>>,
        block_height: Option<u64>,
        block_hash: Option<String>,
    ) -> Self {
        let id = Self::deterministic_id(
            event_id,
            crate::replay::ObservationMode::Live,
            &source,
            None,
        );
        let witness = Some(crate::source::ObservationWitness {
            source: source.clone(),
            observed_at,
            metadata: None,
        });
        Self {
            id,
            event_id,
            mode: crate::replay::ObservationMode::Live,
            source,
            observed_at,
            bitcoin_time,
            replay_job_id: None,
            block_height,
            block_hash,
            witness,
        }
    }

    /// Creates a new HISTORICAL REPLAY observation record.
    pub fn historical_replay(
        event_id: Uuid,
        replay_job_id: Uuid,
        source: crate::source::ObservationSource,
        observed_at: DateTime<Utc>,
        bitcoin_time: Option<DateTime<Utc>>,
        block_height: Option<u64>,
        block_hash: Option<String>,
    ) -> Self {
        let id = Self::deterministic_id(
            event_id,
            crate::replay::ObservationMode::HistoricalReplay,
            &source,
            Some(replay_job_id),
        );
        Self {
            id,
            event_id,
            mode: crate::replay::ObservationMode::HistoricalReplay,
            source,
            observed_at,
            bitcoin_time,
            replay_job_id: Some(replay_job_id),
            block_height,
            block_hash,
            witness: None,
        }
    }
}

/// An anomaly, pattern, or significant occurrence detected on the Bitcoin network.
/// Represents canonical "what happened" on the Bitcoin blockchain.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChainEvent {
    pub id: Uuid,
    pub event_type: EventType,
    pub severity: EventSeverity,
    pub confidence: ConfidenceLevel,
    pub title: String,
    pub description: String,
    /// When the event occurred in Bitcoin history (block header time for confirmed transactions and blocks; mempool time for mempool events).
    #[serde(default = "chrono::Utc::now")]
    pub event_time: DateTime<Utc>,
    /// The wall-clock timestamp when ObsChain first observed or reconstructed this event.
    #[serde(default = "chrono::Utc::now")]
    pub first_observed_at: DateTime<Utc>,
    /// Legacy synonym of `event_time` kept for backward compatibility with existing APIs and tests.
    #[serde(default = "chrono::Utc::now")]
    pub detected_at: DateTime<Utc>,
    pub block_height: Option<u64>,
    pub block_hash: Option<String>,
    pub txid: Option<String>,
    #[serde(default)]
    pub source: Option<crate::source::ObservationSource>,
    #[serde(default)]
    pub witnesses: Vec<crate::source::ObservationWitness>,
    /// Deprecated: provenance is now tracked per observation in EventObservation. Kept for backward compatibility.
    #[serde(default)]
    pub observation_mode: crate::replay::ObservationMode,
    /// Deprecated: provenance is now tracked per observation in EventObservation. Kept for backward compatibility.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_job_id: Option<Uuid>,
    pub metadata: serde_json::Value,
    /// All recorded observation occurrences for this canonical event (populated in detail views).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub observations: Vec<EventObservation>,
}

impl ChainEvent {
    pub fn new(
        event_type: EventType,
        severity: EventSeverity,
        confidence: ConfidenceLevel,
        title: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            event_type,
            severity,
            confidence,
            title: title.into(),
            description: description.into(),
            event_time: now,
            first_observed_at: now,
            detected_at: now,
            block_height: None,
            block_hash: None,
            txid: None,
            source: None,
            witnesses: Vec::new(),
            observation_mode: crate::replay::ObservationMode::Live,
            replay_job_id: None,
            metadata: serde_json::json!({}),
            observations: Vec::new(),
        }
    }

    /// Sets the Bitcoin event timestamp (updating both `event_time` and legacy `detected_at`).
    pub fn with_event_time(mut self, time: DateTime<Utc>) -> Self {
        self.event_time = time;
        self.detected_at = time;
        self
    }

    /// Computes a stable, deterministic UUID for this logical event based on
    /// its event type and target entity (txid, block_hash, or block_height).
    /// Guarantees that live observation and historical replay generate identical event IDs.
    pub fn deterministic_id(&self) -> Uuid {
        let key = if let Some(ref tx) = self.txid {
            format!("{:?}:tx:{}", self.event_type, tx)
        } else if let Some(ref b) = self.block_hash {
            format!("{:?}:block:{}", self.event_type, b)
        } else if let Some(h) = self.block_height {
            format!("{:?}:height:{}", self.event_type, h)
        } else {
            format!("{:?}:title:{}", self.event_type, self.title)
        };
        Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_bytes())
    }

    /// Replaces the event's random ID with its deterministic ID.
    pub fn with_deterministic_id(mut self) -> Self {
        self.id = self.deterministic_id();
        self
    }

    /// Appends an independent observation witness if not already present.
    pub fn add_witness(&mut self, witness: crate::source::ObservationWitness) {
        if !self.witnesses.iter().any(|w| w.source == witness.source) {
            self.witnesses.push(witness);
        }
    }

    /// Appends an observation occurrence if not already present.
    pub fn add_observation(&mut self, obs: EventObservation) {
        if !self.observations.iter().any(|o| o.id == obs.id) {
            self.observations.push(obs);
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
