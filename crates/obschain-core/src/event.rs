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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

/// Categorizes the lifecycle stage or role of an observation.
/// Distinguishes between mempool discovery, block confirmation, reorg invalidation,
/// secondary witnesses, and historical replay without manufacturing unsupported states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventObservationKind {
    FirstSeen,
    MempoolSeen,
    Confirmed,
    ReorgedOut,
    Witnessed,
    HistoricalReplay,
}

impl EventObservationKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::FirstSeen => "FIRST_SEEN",
            Self::MempoolSeen => "MEMPOOL_SEEN",
            Self::Confirmed => "CONFIRMED",
            Self::ReorgedOut => "REORGED_OUT",
            Self::Witnessed => "WITNESSED",
            Self::HistoricalReplay => "HISTORICAL_REPLAY",
        }
    }
}

impl std::fmt::Display for EventObservationKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for EventObservationKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "FIRST_SEEN" => Ok(Self::FirstSeen),
            "MEMPOOL_SEEN" => Ok(Self::MempoolSeen),
            "CONFIRMED" => Ok(Self::Confirmed),
            "REORGED_OUT" => Ok(Self::ReorgedOut),
            "WITNESSED" => Ok(Self::Witnessed),
            "HISTORICAL_REPLAY" => Ok(Self::HistoricalReplay),
            other => Err(format!("Unknown observation kind: {other}")),
        }
    }
}

/// An occurrence or witness of a chain event, recording how and when ObsChain learned about it.
/// Separates observation provenance from intrinsic canonical event identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventObservation {
    pub id: Uuid,
    pub event_id: Uuid,
    pub mode: crate::replay::ObservationMode,
    pub kind: EventObservationKind,
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
    pub confirmation_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_sequence: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mempool_sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub witness: Option<crate::source::ObservationWitness>,
}

impl EventObservation {
    /// Builds a discriminator string from available block hash, sequence numbers, or confirmation status.
    pub fn build_discriminator(
        block_hash: Option<&str>,
        source_sequence: Option<u32>,
        mempool_sequence: Option<u64>,
        confirmation_status: Option<&str>,
    ) -> String {
        if let Some(bh) = block_hash {
            bh.to_string()
        } else if let Some(mseq) = mempool_sequence {
            format!("mempool_seq:{mseq}")
        } else if let Some(sseq) = source_sequence {
            format!("source_seq:{sseq}")
        } else if let Some(status) = confirmation_status {
            status.to_string()
        } else {
            String::new()
        }
    }

    /// Computes a stable, deterministic UUID for this observation occurrence.
    /// Replay observations are unique on (event_id, replay_job_id).
    /// Live observations are unique on (event_id, mode, provider, transport, kind, discriminator).
    pub fn deterministic_id(
        event_id: Uuid,
        mode: crate::replay::ObservationMode,
        source: &crate::source::ObservationSource,
        kind: EventObservationKind,
        replay_job_id: Option<Uuid>,
        discriminator: Option<&str>,
    ) -> Uuid {
        let key = match (mode, replay_job_id) {
            (crate::replay::ObservationMode::HistoricalReplay, Some(job_id)) => {
                format!("obs:{event_id}:replay:{job_id}")
            }
            _ => {
                let disc = discriminator.unwrap_or("");
                format!(
                    "obs:{event_id}:{}:{}:{}:{}:{}",
                    mode.as_str(),
                    source.provider,
                    source.transport,
                    kind.as_str(),
                    disc
                )
            }
        };
        Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_bytes())
    }

    /// Creates a new LIVE observation record.
    pub fn live(
        event_id: Uuid,
        source: crate::source::ObservationSource,
        kind: EventObservationKind,
        observed_at: DateTime<Utc>,
        bitcoin_time: Option<DateTime<Utc>>,
        block_height: Option<u64>,
        block_hash: Option<String>,
    ) -> Self {
        let disc = Self::build_discriminator(block_hash.as_deref(), None, None, None);
        let id = Self::deterministic_id(
            event_id,
            crate::replay::ObservationMode::Live,
            &source,
            kind,
            None,
            Some(&disc),
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
            kind,
            source,
            observed_at,
            bitcoin_time,
            replay_job_id: None,
            block_height,
            block_hash,
            confirmation_status: None,
            source_sequence: None,
            mempool_sequence: None,
            witness,
        }
    }

    pub fn with_confirmation_status(mut self, status: impl Into<String>) -> Self {
        self.confirmation_status = Some(status.into());
        self.recompute_id();
        self
    }

    pub fn with_source_sequence(mut self, seq: u32) -> Self {
        self.source_sequence = Some(seq);
        self.recompute_id();
        self
    }

    pub fn with_mempool_sequence(mut self, seq: u64) -> Self {
        self.mempool_sequence = Some(seq);
        self.recompute_id();
        self
    }

    pub fn recompute_id(&mut self) {
        let disc = Self::build_discriminator(
            self.block_hash.as_deref(),
            self.source_sequence,
            self.mempool_sequence,
            self.confirmation_status.as_deref(),
        );
        self.id = Self::deterministic_id(
            self.event_id,
            self.mode,
            &self.source,
            self.kind,
            self.replay_job_id,
            Some(&disc),
        );
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
            EventObservationKind::HistoricalReplay,
            Some(replay_job_id),
            None,
        );
        Self {
            id,
            event_id,
            mode: crate::replay::ObservationMode::HistoricalReplay,
            kind: EventObservationKind::HistoricalReplay,
            source,
            observed_at,
            bitcoin_time,
            replay_job_id: Some(replay_job_id),
            block_height,
            block_hash,
            confirmation_status: Some("confirmed".to_string()),
            source_sequence: None,
            mempool_sequence: None,
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

    /// Retrieves the network name associated with this event from metadata if present.
    pub fn network(&self) -> Option<&str> {
        self.metadata.get("network").and_then(|v| v.as_str())
    }

    /// Sets the network name in event metadata.
    pub fn with_network(mut self, network: impl Into<String>) -> Self {
        if let Some(obj) = self.metadata.as_object_mut() {
            obj.insert(
                "network".to_string(),
                serde_json::Value::String(network.into()),
            );
        } else {
            self.metadata = serde_json::json!({ "network": network.into() });
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
