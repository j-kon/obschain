use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use uuid::Uuid;

use crate::event::{
    ChainEvent, ConsolidationMetadata, DormantCoinsMetadata, EventType, ExtremeFeeMetadata,
    FanOutMetadata,
};

/// Strongly-typed identifier of numeric metrics used for historical baselining.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BaselineMetric {
    // LargeTransaction / General Transfer
    ValueSats,
    InputCount,
    OutputCount,
    Vsize,

    // DormantCoinsMoved
    DormantValueSats,
    OldestInputAgeDays,
    AverageInputAgeDays,
    CoinAgeDestroyedSatoshiDays,

    // Consolidation
    ConsolidationRatio,

    // FanOut
    DistributedValueSats,
    MedianOutputSats,

    // ExtremeFee
    FeeSats,
    FeeRateSatVb,

    // LongBlockInterval
    IntervalSeconds,
}

impl BaselineMetric {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ValueSats => "value_sats",
            Self::InputCount => "input_count",
            Self::OutputCount => "output_count",
            Self::Vsize => "vsize",
            Self::DormantValueSats => "dormant_value_sats",
            Self::OldestInputAgeDays => "oldest_input_age_days",
            Self::AverageInputAgeDays => "average_input_age_days",
            Self::CoinAgeDestroyedSatoshiDays => "coin_age_destroyed_satoshi_days",
            Self::ConsolidationRatio => "consolidation_ratio",
            Self::DistributedValueSats => "distributed_value_sats",
            Self::MedianOutputSats => "median_output_sats",
            Self::FeeSats => "fee_sats",
            Self::FeeRateSatVb => "fee_rate_sat_vb",
            Self::IntervalSeconds => "interval_seconds",
        }
    }
}

impl std::fmt::Display for BaselineMetric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for BaselineMetric {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "value_sats" | "total_output_sats" => Ok(Self::ValueSats),
            "input_count" | "inputs_count" => Ok(Self::InputCount),
            "output_count" | "outputs_count" => Ok(Self::OutputCount),
            "vsize" => Ok(Self::Vsize),
            "dormant_value_sats" | "total_dormant_sats" => Ok(Self::DormantValueSats),
            "oldest_input_age_days" => Ok(Self::OldestInputAgeDays),
            "average_input_age_days" => Ok(Self::AverageInputAgeDays),
            "coin_age_destroyed_satoshi_days" | "coin_age_destroyed_sats_days" => {
                Ok(Self::CoinAgeDestroyedSatoshiDays)
            }
            "consolidation_ratio" | "input_output_ratio" => Ok(Self::ConsolidationRatio),
            "distributed_value_sats" | "total_distributed_sats" => Ok(Self::DistributedValueSats),
            "median_output_sats" => Ok(Self::MedianOutputSats),
            "fee_sats" => Ok(Self::FeeSats),
            "fee_rate_sat_vb" => Ok(Self::FeeRateSatVb),
            "interval_seconds" => Ok(Self::IntervalSeconds),
            other => Err(format!("Unknown baseline metric: {other}")),
        }
    }
}

/// Unit of measurement for a baseline metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricUnit {
    Satoshis,
    SatoshiDays,
    Count,
    Seconds,
    Days,
    SatoshisPerVbyte,
    Ratio,
}

impl MetricUnit {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Satoshis => "satoshis",
            Self::SatoshiDays => "satoshi_days",
            Self::Count => "count",
            Self::Seconds => "seconds",
            Self::Days => "days",
            Self::SatoshisPerVbyte => "sat_vb",
            Self::Ratio => "ratio",
        }
    }
}

impl FromStr for MetricUnit {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "satoshis" | "sats" => Ok(Self::Satoshis),
            "satoshi_days" => Ok(Self::SatoshiDays),
            "count" => Ok(Self::Count),
            "seconds" => Ok(Self::Seconds),
            "days" => Ok(Self::Days),
            "sat_vb" | "satoshis_per_vbyte" => Ok(Self::SatoshisPerVbyte),
            "ratio" | "basis_points" => Ok(Self::Ratio),
            other => Err(format!("Unknown MetricUnit: {other}")),
        }
    }
}

/// Direction in which a metric value is considered statistically anomalous/rare.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RarityDirection {
    HigherIsRarer,
    LowerIsRarer,
    TwoSided,
}

/// Definition of a baseline metric for an event type in the registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaselineMetricDefinition {
    pub event_type: EventType,
    pub metric: BaselineMetric,
    pub unit: MetricUnit,
    pub direction: RarityDirection,
    pub is_primary: bool,
}

/// Typed registry of baseline metrics for supported replayable event types.
pub struct MetricRegistry;

impl MetricRegistry {
    /// Returns the initial set of supported replayable event types for baseline creation.
    pub fn supported_event_types() -> Vec<EventType> {
        vec![
            EventType::LargeTransfer,
            EventType::LongBlockInterval,
            EventType::DormantCoinsMoved,
            EventType::Consolidation,
            EventType::FanOut,
            EventType::ExtremeFee,
        ]
    }

    /// Returns all registered metric definitions for the given event type.
    pub fn metrics_for_event_type(event_type: EventType) -> Vec<BaselineMetricDefinition> {
        match event_type {
            EventType::LargeTransfer => vec![
                BaselineMetricDefinition {
                    event_type,
                    metric: BaselineMetric::ValueSats,
                    unit: MetricUnit::Satoshis,
                    direction: RarityDirection::HigherIsRarer,
                    is_primary: true,
                },
                BaselineMetricDefinition {
                    event_type,
                    metric: BaselineMetric::InputCount,
                    unit: MetricUnit::Count,
                    direction: RarityDirection::HigherIsRarer,
                    is_primary: false,
                },
                BaselineMetricDefinition {
                    event_type,
                    metric: BaselineMetric::OutputCount,
                    unit: MetricUnit::Count,
                    direction: RarityDirection::HigherIsRarer,
                    is_primary: false,
                },
                BaselineMetricDefinition {
                    event_type,
                    metric: BaselineMetric::Vsize,
                    unit: MetricUnit::Count,
                    direction: RarityDirection::HigherIsRarer,
                    is_primary: false,
                },
            ],
            EventType::DormantCoinsMoved => vec![
                BaselineMetricDefinition {
                    event_type,
                    metric: BaselineMetric::DormantValueSats,
                    unit: MetricUnit::Satoshis,
                    direction: RarityDirection::HigherIsRarer,
                    is_primary: true,
                },
                BaselineMetricDefinition {
                    event_type,
                    metric: BaselineMetric::OldestInputAgeDays,
                    unit: MetricUnit::Days,
                    direction: RarityDirection::HigherIsRarer,
                    is_primary: false,
                },
                BaselineMetricDefinition {
                    event_type,
                    metric: BaselineMetric::CoinAgeDestroyedSatoshiDays,
                    unit: MetricUnit::SatoshiDays,
                    direction: RarityDirection::HigherIsRarer,
                    is_primary: false,
                },
                BaselineMetricDefinition {
                    event_type,
                    metric: BaselineMetric::InputCount,
                    unit: MetricUnit::Count,
                    direction: RarityDirection::HigherIsRarer,
                    is_primary: false,
                },
            ],
            EventType::Consolidation => vec![
                BaselineMetricDefinition {
                    event_type,
                    metric: BaselineMetric::InputCount,
                    unit: MetricUnit::Count,
                    direction: RarityDirection::HigherIsRarer,
                    is_primary: true,
                },
                BaselineMetricDefinition {
                    event_type,
                    metric: BaselineMetric::OutputCount,
                    unit: MetricUnit::Count,
                    direction: RarityDirection::LowerIsRarer,
                    is_primary: false,
                },
                BaselineMetricDefinition {
                    event_type,
                    metric: BaselineMetric::ValueSats,
                    unit: MetricUnit::Satoshis,
                    direction: RarityDirection::HigherIsRarer,
                    is_primary: false,
                },
                BaselineMetricDefinition {
                    event_type,
                    metric: BaselineMetric::ConsolidationRatio,
                    unit: MetricUnit::Ratio,
                    direction: RarityDirection::HigherIsRarer,
                    is_primary: false,
                },
            ],
            EventType::FanOut => vec![
                BaselineMetricDefinition {
                    event_type,
                    metric: BaselineMetric::OutputCount,
                    unit: MetricUnit::Count,
                    direction: RarityDirection::HigherIsRarer,
                    is_primary: true,
                },
                BaselineMetricDefinition {
                    event_type,
                    metric: BaselineMetric::DistributedValueSats,
                    unit: MetricUnit::Satoshis,
                    direction: RarityDirection::HigherIsRarer,
                    is_primary: false,
                },
                BaselineMetricDefinition {
                    event_type,
                    metric: BaselineMetric::MedianOutputSats,
                    unit: MetricUnit::Satoshis,
                    direction: RarityDirection::HigherIsRarer,
                    is_primary: false,
                },
            ],
            EventType::ExtremeFee => vec![
                BaselineMetricDefinition {
                    event_type,
                    metric: BaselineMetric::FeeRateSatVb,
                    unit: MetricUnit::SatoshisPerVbyte,
                    direction: RarityDirection::HigherIsRarer,
                    is_primary: true,
                },
                BaselineMetricDefinition {
                    event_type,
                    metric: BaselineMetric::FeeSats,
                    unit: MetricUnit::Satoshis,
                    direction: RarityDirection::HigherIsRarer,
                    is_primary: false,
                },
            ],
            EventType::LongBlockInterval => vec![BaselineMetricDefinition {
                event_type,
                metric: BaselineMetric::IntervalSeconds,
                unit: MetricUnit::Seconds,
                direction: RarityDirection::HigherIsRarer,
                is_primary: true,
            }],
            _ => Vec::new(),
        }
    }

    /// Returns the primary metric definition for an event type if supported.
    pub fn primary_metric(event_type: EventType) -> Option<BaselineMetricDefinition> {
        Self::metrics_for_event_type(event_type)
            .into_iter()
            .find(|m| m.is_primary)
    }

    /// Whether this event type supports historical replay baselines.
    pub fn is_replayable_for_baselines(event_type: EventType) -> bool {
        matches!(
            event_type,
            EventType::LargeTransfer
                | EventType::LongBlockInterval
                | EventType::DormantCoinsMoved
                | EventType::Consolidation
                | EventType::FanOut
                | EventType::ExtremeFee
        )
    }
}

/// Representation of extracted metric values preserving exact numeric precision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum MetricValue {
    U64(u64),
    U128(u128),
    BasisPoints(u32),
    DecimalScaled { value: u64, scale: u32 },
}

impl MetricValue {
    pub fn as_u64(&self) -> Option<u64> {
        match *self {
            Self::U64(v) => Some(v),
            Self::BasisPoints(v) => Some(v as u64),
            Self::DecimalScaled { value, scale: 0 } => Some(value),
            _ => None,
        }
    }

    pub fn as_u128(&self) -> Option<u128> {
        match *self {
            Self::U128(v) => Some(v),
            Self::U64(v) => Some(v as u128),
            Self::BasisPoints(v) => Some(v as u128),
            Self::DecimalScaled { value, scale: 0 } => Some(value as u128),
            _ => None,
        }
    }

    pub fn to_u128(&self) -> u128 {
        match *self {
            Self::U64(v) => v as u128,
            Self::U128(v) => v,
            Self::BasisPoints(v) => v as u128,
            Self::DecimalScaled { value, .. } => value as u128,
        }
    }

    pub fn to_f64(&self) -> f64 {
        match *self {
            Self::U64(v) => v as f64,
            Self::U128(v) => v as f64,
            Self::BasisPoints(v) => (v as f64) / 100.0,
            Self::DecimalScaled { value, scale } => (value as f64) / 10f64.powi(scale as i32),
        }
    }

    pub fn to_numeric_string(&self) -> String {
        match *self {
            Self::U64(v) => v.to_string(),
            Self::U128(v) => v.to_string(),
            Self::BasisPoints(v) => {
                let whole = v / 100;
                let frac = v % 100;
                format!("{whole}.{frac:02}")
            }
            Self::DecimalScaled { value, scale } => {
                if scale == 0 {
                    value.to_string()
                } else {
                    let divisor = 10u64.pow(scale);
                    let whole = value / divisor;
                    let frac = value % divisor;
                    format!("{whole}.{frac:0width$}", width = scale as usize)
                }
            }
        }
    }

    pub fn from_str_and_metric(s: &str, metric: BaselineMetric) -> Self {
        let clean = s.trim();
        match metric {
            BaselineMetric::CoinAgeDestroyedSatoshiDays => {
                if let Ok(v) = clean.parse::<u128>() {
                    Self::U128(v)
                } else if let Ok(f) = clean.parse::<f64>() {
                    Self::U128(f.round() as u128)
                } else {
                    Self::U128(0)
                }
            }
            BaselineMetric::FeeRateSatVb => {
                if let Ok(f) = clean.parse::<f64>() {
                    Self::DecimalScaled {
                        value: (f * 100.0).round() as u64,
                        scale: 2,
                    }
                } else {
                    Self::DecimalScaled { value: 0, scale: 2 }
                }
            }
            BaselineMetric::ConsolidationRatio => {
                if let Ok(f) = clean.parse::<f64>() {
                    Self::BasisPoints((f * 100.0).round() as u32)
                } else {
                    Self::BasisPoints(0)
                }
            }
            _ => {
                if let Ok(v) = clean.parse::<u64>() {
                    Self::U64(v)
                } else if let Ok(f) = clean.parse::<f64>() {
                    Self::U64(f.round() as u64)
                } else {
                    Self::U64(0)
                }
            }
        }
    }
}

impl std::fmt::Display for MetricValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_numeric_string())
    }
}

/// Centralized extractor of typed numeric metrics from ChainEvent records.
pub struct EventMetricExtractor;

impl EventMetricExtractor {
    /// Extracts a typed numeric metric from a ChainEvent.
    pub fn extract_metric(event: &ChainEvent, metric: BaselineMetric) -> Option<MetricValue> {
        match (event.event_type, metric) {
            // LargeTransfer
            (EventType::LargeTransfer, BaselineMetric::ValueSats) => event
                .metadata
                .get("total_output_sats")
                .or_else(|| event.metadata.get("value_sats"))
                .and_then(|v| v.as_u64())
                .map(MetricValue::U64),
            (EventType::LargeTransfer, BaselineMetric::InputCount) => event
                .metadata
                .get("inputs_count")
                .or_else(|| event.metadata.get("input_count"))
                .and_then(|v| v.as_u64())
                .map(MetricValue::U64),
            (EventType::LargeTransfer, BaselineMetric::OutputCount) => event
                .metadata
                .get("outputs_count")
                .or_else(|| event.metadata.get("output_count"))
                .and_then(|v| v.as_u64())
                .map(MetricValue::U64),
            (EventType::LargeTransfer, BaselineMetric::Vsize) => event
                .metadata
                .get("vsize")
                .and_then(|v| v.as_u64())
                .map(MetricValue::U64),

            // DormantCoinsMoved
            (EventType::DormantCoinsMoved, BaselineMetric::DormantValueSats) => {
                if let Ok(d) =
                    serde_json::from_value::<DormantCoinsMetadata>(event.metadata.clone())
                {
                    Some(MetricValue::U64(d.total_dormant_sats))
                } else {
                    event
                        .metadata
                        .get("total_dormant_sats")
                        .or_else(|| event.metadata.get("dormant_value_sats"))
                        .and_then(|v| v.as_u64())
                        .map(MetricValue::U64)
                }
            }
            (EventType::DormantCoinsMoved, BaselineMetric::OldestInputAgeDays) => {
                if let Ok(d) =
                    serde_json::from_value::<DormantCoinsMetadata>(event.metadata.clone())
                {
                    Some(MetricValue::U64(d.oldest_input_age_days))
                } else {
                    event
                        .metadata
                        .get("oldest_input_age_days")
                        .and_then(|v| v.as_u64())
                        .map(MetricValue::U64)
                }
            }
            (EventType::DormantCoinsMoved, BaselineMetric::CoinAgeDestroyedSatoshiDays) => {
                if let Ok(d) =
                    serde_json::from_value::<DormantCoinsMetadata>(event.metadata.clone())
                {
                    Some(MetricValue::U128(d.coin_age_destroyed_sats_days))
                } else {
                    event
                        .metadata
                        .get("coin_age_destroyed_sats_days")
                        .or_else(|| event.metadata.get("coin_age_destroyed_satoshi_days"))
                        .and_then(|v| {
                            v.as_u64()
                                .map(|n| n as u128)
                                .or_else(|| v.as_str().and_then(|s| s.parse::<u128>().ok()))
                        })
                        .map(MetricValue::U128)
                }
            }
            (EventType::DormantCoinsMoved, BaselineMetric::InputCount) => {
                if let Ok(d) =
                    serde_json::from_value::<DormantCoinsMetadata>(event.metadata.clone())
                {
                    Some(MetricValue::U64(d.dormant_input_count as u64))
                } else {
                    event
                        .metadata
                        .get("dormant_input_count")
                        .or_else(|| event.metadata.get("input_count"))
                        .and_then(|v| v.as_u64())
                        .map(MetricValue::U64)
                }
            }

            // Consolidation
            (EventType::Consolidation, BaselineMetric::InputCount) => {
                if let Ok(c) =
                    serde_json::from_value::<ConsolidationMetadata>(event.metadata.clone())
                {
                    Some(MetricValue::U64(c.input_count as u64))
                } else {
                    event
                        .metadata
                        .get("input_count")
                        .and_then(|v| v.as_u64())
                        .map(MetricValue::U64)
                }
            }
            (EventType::Consolidation, BaselineMetric::OutputCount) => {
                if let Ok(c) =
                    serde_json::from_value::<ConsolidationMetadata>(event.metadata.clone())
                {
                    Some(MetricValue::U64(c.output_count as u64))
                } else {
                    event
                        .metadata
                        .get("output_count")
                        .and_then(|v| v.as_u64())
                        .map(MetricValue::U64)
                }
            }
            (EventType::Consolidation, BaselineMetric::ValueSats) => {
                if let Ok(c) =
                    serde_json::from_value::<ConsolidationMetadata>(event.metadata.clone())
                {
                    Some(MetricValue::U64(c.total_input_sats))
                } else {
                    event
                        .metadata
                        .get("total_input_sats")
                        .or_else(|| event.metadata.get("value_sats"))
                        .and_then(|v| v.as_u64())
                        .map(MetricValue::U64)
                }
            }
            (EventType::Consolidation, BaselineMetric::ConsolidationRatio) => {
                if let Ok(c) =
                    serde_json::from_value::<ConsolidationMetadata>(event.metadata.clone())
                {
                    let bps = (c.input_output_ratio * 100.0).round() as u32;
                    Some(MetricValue::BasisPoints(bps))
                } else {
                    event
                        .metadata
                        .get("input_output_ratio")
                        .and_then(|v| v.as_f64())
                        .map(|r| MetricValue::BasisPoints((r * 100.0).round() as u32))
                }
            }

            // FanOut
            (EventType::FanOut, BaselineMetric::OutputCount) => {
                if let Ok(f) = serde_json::from_value::<FanOutMetadata>(event.metadata.clone()) {
                    Some(MetricValue::U64(f.output_count as u64))
                } else {
                    event
                        .metadata
                        .get("output_count")
                        .and_then(|v| v.as_u64())
                        .map(MetricValue::U64)
                }
            }
            (EventType::FanOut, BaselineMetric::DistributedValueSats) => {
                if let Ok(f) = serde_json::from_value::<FanOutMetadata>(event.metadata.clone()) {
                    Some(MetricValue::U64(f.total_distributed_sats))
                } else {
                    event
                        .metadata
                        .get("total_distributed_sats")
                        .and_then(|v| v.as_u64())
                        .map(MetricValue::U64)
                }
            }
            (EventType::FanOut, BaselineMetric::MedianOutputSats) => {
                if let Ok(f) = serde_json::from_value::<FanOutMetadata>(event.metadata.clone()) {
                    Some(MetricValue::U64(f.median_output_sats))
                } else {
                    event
                        .metadata
                        .get("median_output_sats")
                        .and_then(|v| v.as_u64())
                        .map(MetricValue::U64)
                }
            }

            // ExtremeFee
            (EventType::ExtremeFee, BaselineMetric::FeeRateSatVb) => {
                if let Ok(ef) = serde_json::from_value::<ExtremeFeeMetadata>(event.metadata.clone())
                {
                    ef.fee_rate_sat_vb.map(|rate| MetricValue::DecimalScaled {
                        value: (rate * 100.0).round() as u64,
                        scale: 2,
                    })
                } else {
                    event
                        .metadata
                        .get("fee_rate_sat_vb")
                        .and_then(|v| v.as_f64())
                        .map(|rate| MetricValue::DecimalScaled {
                            value: (rate * 100.0).round() as u64,
                            scale: 2,
                        })
                }
            }
            (EventType::ExtremeFee, BaselineMetric::FeeSats) => {
                if let Ok(ef) = serde_json::from_value::<ExtremeFeeMetadata>(event.metadata.clone())
                {
                    Some(MetricValue::U64(ef.fee_sats))
                } else {
                    event
                        .metadata
                        .get("fee_sats")
                        .and_then(|v| v.as_u64())
                        .map(MetricValue::U64)
                }
            }

            // LongBlockInterval
            (EventType::LongBlockInterval, BaselineMetric::IntervalSeconds) => event
                .metadata
                .get("interval_seconds")
                .and_then(|v| v.as_u64())
                .map(MetricValue::U64),

            _ => None,
        }
    }
}

/// Historical window defining the population boundaries of a baseline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BaselineWindow {
    pub start_height: u64,
    pub end_height: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_time: Option<DateTime<Utc>>,
    pub network: String,
}

impl BaselineWindow {
    pub fn new(network: impl Into<String>, start_height: u64, end_height: u64) -> Self {
        Self {
            start_height,
            end_height,
            start_time: None,
            end_time: None,
            network: network.into(),
        }
    }

    pub fn with_time_bounds(
        mut self,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Self {
        self.start_time = start_time;
        self.end_time = end_time;
        self
    }
}

/// Rarity classification band derived from percentile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RarityBand {
    Common,
    Notable,
    Unusual,
    Rare,
    Extreme,
    InsufficientData,
}

impl RarityBand {
    /// Classifies an empirical percentile rank into a rarity band.
    /// Returns `InsufficientData` if sample count is below minimum threshold.
    pub fn from_percentile(percentile: f64, sample_count: u64, min_sample_size: u64) -> Self {
        if sample_count < min_sample_size {
            return Self::InsufficientData;
        }

        if percentile >= 99.9 {
            Self::Extreme
        } else if percentile >= 99.0 {
            Self::Rare
        } else if percentile >= 95.0 {
            Self::Unusual
        } else if percentile >= 90.0 {
            Self::Notable
        } else {
            Self::Common
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Common => "COMMON",
            Self::Notable => "NOTABLE",
            Self::Unusual => "UNUSUAL",
            Self::Rare => "RARE",
            Self::Extreme => "EXTREME",
            Self::InsufficientData => "INSUFFICIENT_DATA",
        }
    }
}

impl std::fmt::Display for RarityBand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for RarityBand {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "COMMON" => Ok(Self::Common),
            "NOTABLE" => Ok(Self::Notable),
            "UNUSUAL" => Ok(Self::Unusual),
            "RARE" => Ok(Self::Rare),
            "EXTREME" => Ok(Self::Extreme),
            "INSUFFICIENT_DATA" => Ok(Self::InsufficientData),
            other => Err(format!("Unknown RarityBand: {other}")),
        }
    }
}

/// Evaluation mode distinguishing whether a baseline was contemporaneous or retrospective.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EvaluationMode {
    #[default]
    Retrospective,
    PointInTime,
}

impl EvaluationMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Retrospective => "RETROSPECTIVE",
            Self::PointInTime => "POINT_IN_TIME",
        }
    }
}

impl std::fmt::Display for EvaluationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for EvaluationMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "RETROSPECTIVE" => Ok(Self::Retrospective),
            "POINT_IN_TIME" => Ok(Self::PointInTime),
            other => Err(format!("Unknown EvaluationMode: {other}")),
        }
    }
}

/// Quality status of a baseline distribution based on sample count and metric coverage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BaselineQuality {
    High,
    Moderate,
    Degraded,
    Insufficient,
}

impl BaselineQuality {
    /// Evaluates baseline quality deterministically based on sample count and coverage ratio.
    pub fn evaluate(sample_count: u64, coverage_ratio: f64) -> Self {
        if sample_count >= 500 && coverage_ratio >= 0.95 {
            Self::High
        } else if sample_count >= 100 && coverage_ratio >= 0.80 {
            Self::Moderate
        } else if sample_count >= 30 && coverage_ratio >= 0.50 {
            Self::Degraded
        } else {
            Self::Insufficient
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::High => "HIGH",
            Self::Moderate => "MODERATE",
            Self::Degraded => "DEGRADED",
            Self::Insufficient => "INSUFFICIENT",
        }
    }
}

impl std::fmt::Display for BaselineQuality {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for BaselineQuality {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "HIGH" => Ok(Self::High),
            "MODERATE" => Ok(Self::Moderate),
            "DEGRADED" => Ok(Self::Degraded),
            "INSUFFICIENT" => Ok(Self::Insufficient),
            other => Err(format!("Unknown BaselineQuality: {other}")),
        }
    }
}

/// Helper functions for Bitcoin Halving Epochs.
pub struct HalvingEpoch;

impl HalvingEpoch {
    pub const BLOCKS_PER_HALVING: u64 = 210_000;

    /// Computes halving epoch number from block height.
    /// Epoch 0: 0 .. 209,999 (50 BTC)
    /// Epoch 1: 210,000 .. 419,999 (25 BTC)
    /// Epoch 2: 420,000 .. 629,999 (12.5 BTC)
    /// Epoch 3: 630,000 .. 839,999 (6.25 BTC)
    /// Epoch 4: 840,000 .. 1,049,999 (3.125 BTC)
    pub fn epoch_from_height(height: u64) -> u32 {
        (height / Self::BLOCKS_PER_HALVING) as u32
    }

    /// Returns the block range (inclusive start, inclusive end) for a given epoch.
    pub fn epoch_range(epoch: u32) -> (u64, u64) {
        let start = (epoch as u64) * Self::BLOCKS_PER_HALVING;
        let end = start + Self::BLOCKS_PER_HALVING - 1;
        (start, end)
    }
}

/// Precomputed quantiles and statistical summary of a metric distribution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuantileDistribution {
    pub sample_count: u64,
    pub candidate_count: u64,
    pub missing_count: u64,
    pub coverage_ratio: f64,
    pub minimum: MetricValue,
    pub maximum: MetricValue,
    pub mean: f64,
    pub p50: MetricValue,
    pub p75: MetricValue,
    pub p90: MetricValue,
    pub p95: MetricValue,
    pub p99: MetricValue,
    pub p999: MetricValue,
    pub quality: BaselineQuality,
}

/// Status of a baseline generation job run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BaselineRunStatus {
    #[default]
    Pending,
    Running,
    Completed,
    Failed,
}

impl BaselineRunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
        }
    }
}

impl std::fmt::Display for BaselineRunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for BaselineRunStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "PENDING" => Ok(Self::Pending),
            "RUNNING" => Ok(Self::Running),
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            other => Err(format!("Unknown BaselineRunStatus: {other}")),
        }
    }
}

/// Durable baseline run record representing an executed baseline computation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BaselineRun {
    pub id: Uuid,
    pub network: String,
    pub start_height: u64,
    pub end_height: u64,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub status: BaselineRunStatus,
    pub algorithm_version: String,
    pub canonical_event_count: u64,
    pub error_message: Option<String>,
    pub metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

/// Durable statistical distribution for a single (event_type, metric) within a baseline run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BaselineDistribution {
    pub id: Uuid,
    pub baseline_run_id: Uuid,
    pub event_type: EventType,
    pub metric: BaselineMetric,
    pub unit: MetricUnit,
    pub sample_count: u64,
    pub candidate_count: u64,
    pub missing_count: u64,
    pub coverage_ratio: f64,
    pub minimum: MetricValue,
    pub maximum: MetricValue,
    pub mean: f64,
    pub p50: MetricValue,
    pub p75: MetricValue,
    pub p90: MetricValue,
    pub p95: MetricValue,
    pub p99: MetricValue,
    pub p999: MetricValue,
    pub quality: BaselineQuality,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub samples_json: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

/// Evaluated rarity of a single metric on a canonical event against a baseline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventRarityResult {
    pub event_id: Uuid,
    pub baseline_run_id: Uuid,
    pub event_type: EventType,
    pub metric: BaselineMetric,
    pub value: MetricValue,
    pub percentile: Option<f64>,
    pub rarity_band: RarityBand,
    pub population_size: u64,
    pub tail_count: u64,
    pub evaluation_mode: EvaluationMode,
}

impl EventRarityResult {
    /// Descriptive frequency string e.g. "11 comparable-or-greater events across 18,421 qualifying events (1 in 1,674)".
    pub fn frequency_description(&self) -> String {
        let one_in = self
            .population_size
            .checked_div(self.tail_count)
            .unwrap_or(self.population_size);
        format!(
            "{} comparable-or-rarer events across {} qualifying events (approx. 1 in {})",
            self.tail_count, self.population_size, one_in
        )
    }
}

/// Explainable component contributing to composite event impact.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImpactComponent {
    pub component_name: String,
    pub metric: BaselineMetric,
    pub raw_value: String,
    pub percentile: Option<f64>,
    pub weight: f64,
    pub points_awarded: f64,
    pub population_size: u64,
}

/// Explainable impact breakdown with visible component contributions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImpactBreakdown {
    pub model_version: String,
    pub status: String,           // "EXPERIMENTAL"
    pub total_score: Option<f64>, // 0.0 .. 100.0 or None if insufficient data
    pub max_possible_points: f64,
    pub coverage_ratio: f64,
    pub components: Vec<ImpactComponent>,
}

/// Combined event rarity context exposed in API and research detail views.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventRarityContext {
    pub baseline_id: Uuid,
    pub baseline_version: String,
    pub evaluation_mode: EvaluationMode,
    pub primary: EventRarityResult,
    pub secondary: Vec<EventRarityResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub impact: Option<ImpactBreakdown>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{
        ConfidenceLevel, ConsolidationMetadata, DormantClassification, DormantCoinsMetadata,
        EventSeverity, ExtremeFeeMetadata, ExtremeFeeTriggerType, FanOutMetadata,
    };

    #[test]
    fn test_metric_registry_primary_metrics() {
        assert_eq!(
            MetricRegistry::primary_metric(EventType::LargeTransfer)
                .unwrap()
                .metric,
            BaselineMetric::ValueSats
        );
        assert_eq!(
            MetricRegistry::primary_metric(EventType::DormantCoinsMoved)
                .unwrap()
                .metric,
            BaselineMetric::DormantValueSats
        );
        assert_eq!(
            MetricRegistry::primary_metric(EventType::Consolidation)
                .unwrap()
                .metric,
            BaselineMetric::InputCount
        );
        assert_eq!(
            MetricRegistry::primary_metric(EventType::FanOut)
                .unwrap()
                .metric,
            BaselineMetric::OutputCount
        );
        assert_eq!(
            MetricRegistry::primary_metric(EventType::ExtremeFee)
                .unwrap()
                .metric,
            BaselineMetric::FeeRateSatVb
        );
        assert_eq!(
            MetricRegistry::primary_metric(EventType::LongBlockInterval)
                .unwrap()
                .metric,
            BaselineMetric::IntervalSeconds
        );

        // Active chain replay must NOT baseline RBF or Reorgs
        assert!(!MetricRegistry::is_replayable_for_baselines(
            EventType::TransactionReplacement
        ));
        assert!(!MetricRegistry::is_replayable_for_baselines(
            EventType::ReorgDetected
        ));
    }

    #[test]
    fn test_metric_value_arithmetic_and_string_representation() {
        let sat_val = MetricValue::U64(2_100_000_000_000_000);
        assert_eq!(sat_val.to_u128(), 2_100_000_000_000_000u128);
        assert_eq!(sat_val.to_numeric_string(), "2100000000000000");

        // u128 safe handling for coin-age destroyed
        let cad = MetricValue::U128(999_999_999_999_999_999_999u128);
        assert_eq!(cad.to_u128(), 999_999_999_999_999_999_999u128);
        assert_eq!(cad.to_numeric_string(), "999999999999999999999");

        // Scaled decimal (e.g. fee rate 7.14 sat/vB)
        let rate = MetricValue::DecimalScaled {
            value: 714,
            scale: 2,
        };
        assert_eq!(rate.to_f64(), 7.14);
        assert_eq!(rate.to_numeric_string(), "7.14");

        // Basis points (e.g. 15.25 ratio -> 1525 bps)
        let bps = MetricValue::BasisPoints(1525);
        assert_eq!(bps.to_f64(), 15.25);
        assert_eq!(bps.to_numeric_string(), "15.25");
    }

    #[test]
    fn test_event_metric_extractor_all_supported_types() {
        // 1. LargeTransfer
        let mut large_tx = ChainEvent::new(
            EventType::LargeTransfer,
            EventSeverity::High,
            ConfidenceLevel::VerifiedOnChain,
            "Large Transfer",
            "100 BTC transfer",
        );
        large_tx.metadata = serde_json::json!({
            "total_output_sats": 10_000_000_000u64,
            "inputs_count": 5u32,
            "outputs_count": 2u32,
            "vsize": 250u64,
        });
        assert_eq!(
            EventMetricExtractor::extract_metric(&large_tx, BaselineMetric::ValueSats),
            Some(MetricValue::U64(10_000_000_000))
        );
        assert_eq!(
            EventMetricExtractor::extract_metric(&large_tx, BaselineMetric::InputCount),
            Some(MetricValue::U64(5))
        );
        assert_eq!(
            EventMetricExtractor::extract_metric(&large_tx, BaselineMetric::OutputCount),
            Some(MetricValue::U64(2))
        );
        assert_eq!(
            EventMetricExtractor::extract_metric(&large_tx, BaselineMetric::Vsize),
            Some(MetricValue::U64(250))
        );

        // 2. DormantCoinsMoved
        let dormant_meta = DormantCoinsMetadata {
            total_dormant_sats: 500_000_000,
            total_dormant_btc: 5.0,
            dormant_input_count: 1,
            total_input_count: 2,
            oldest_input_age_days: 3650,
            oldest_input_age_seconds: 315360000,
            youngest_qualifying_age_days: 3650,
            total_input_sats: 600_000_000,
            total_output_sats: 599_980_000,
            dormant_ratio: 0.833,
            coin_age_destroyed_sats_days: 1_825_000_000_000,
            coin_age_destroyed_btc_days: 18250.0,
            coin_age_destroyed_btc_years: 50.0,
            classification: DormantClassification::VeryOld,
        };
        let dormant_ev = ChainEvent::new(
            EventType::DormantCoinsMoved,
            EventSeverity::High,
            ConfidenceLevel::VerifiedOnChain,
            "Dormant",
            "Desc",
        )
        .with_typed_metadata(&dormant_meta);

        assert_eq!(
            EventMetricExtractor::extract_metric(&dormant_ev, BaselineMetric::DormantValueSats),
            Some(MetricValue::U64(500_000_000))
        );
        assert_eq!(
            EventMetricExtractor::extract_metric(&dormant_ev, BaselineMetric::OldestInputAgeDays),
            Some(MetricValue::U64(3650))
        );
        assert_eq!(
            EventMetricExtractor::extract_metric(
                &dormant_ev,
                BaselineMetric::CoinAgeDestroyedSatoshiDays
            ),
            Some(MetricValue::U128(1_825_000_000_000))
        );

        // 3. Consolidation
        let consol_meta = ConsolidationMetadata {
            input_count: 100,
            output_count: 1,
            input_output_ratio: 100.0,
            total_input_sats: 50_000_000,
            total_output_sats: 49_950_000,
            fee_sats: 50_000,
        };
        let consol_ev = ChainEvent::new(
            EventType::Consolidation,
            EventSeverity::Medium,
            ConfidenceLevel::VerifiedOnChain,
            "Consolidation",
            "Desc",
        )
        .with_typed_metadata(&consol_meta);
        assert_eq!(
            EventMetricExtractor::extract_metric(&consol_ev, BaselineMetric::InputCount),
            Some(MetricValue::U64(100))
        );
        assert_eq!(
            EventMetricExtractor::extract_metric(&consol_ev, BaselineMetric::OutputCount),
            Some(MetricValue::U64(1))
        );
        assert_eq!(
            EventMetricExtractor::extract_metric(&consol_ev, BaselineMetric::ConsolidationRatio),
            Some(MetricValue::BasisPoints(10000))
        );

        // 4. FanOut
        let fan_meta = FanOutMetadata {
            input_count: 1,
            output_count: 150,
            output_input_ratio: 150.0,
            total_distributed_sats: 10_000_000,
            total_distributed_btc: 0.1,
            median_output_sats: 66_000,
            smallest_output_sats: 546,
            largest_output_sats: 200_000,
        };
        let fan_ev = ChainEvent::new(
            EventType::FanOut,
            EventSeverity::Medium,
            ConfidenceLevel::VerifiedOnChain,
            "FanOut",
            "Desc",
        )
        .with_typed_metadata(&fan_meta);
        assert_eq!(
            EventMetricExtractor::extract_metric(&fan_ev, BaselineMetric::OutputCount),
            Some(MetricValue::U64(150))
        );
        assert_eq!(
            EventMetricExtractor::extract_metric(&fan_ev, BaselineMetric::DistributedValueSats),
            Some(MetricValue::U64(10_000_000))
        );
        assert_eq!(
            EventMetricExtractor::extract_metric(&fan_ev, BaselineMetric::MedianOutputSats),
            Some(MetricValue::U64(66_000))
        );

        // 5. ExtremeFee
        let fee_meta = ExtremeFeeMetadata {
            fee_sats: 5_000_000,
            fee_btc: 0.05,
            fee_rate_sat_vb: Some(250.75),
            vsize: 200,
            total_input_sats: 10_000_000,
            total_output_sats: 5_000_000,
            fee_trigger_type: ExtremeFeeTriggerType::HighFeeRate,
        };
        let fee_ev = ChainEvent::new(
            EventType::ExtremeFee,
            EventSeverity::High,
            ConfidenceLevel::VerifiedOnChain,
            "Fee",
            "Desc",
        )
        .with_typed_metadata(&fee_meta);
        assert_eq!(
            EventMetricExtractor::extract_metric(&fee_ev, BaselineMetric::FeeRateSatVb),
            Some(MetricValue::DecimalScaled {
                value: 25075,
                scale: 2
            })
        );
        assert_eq!(
            EventMetricExtractor::extract_metric(&fee_ev, BaselineMetric::FeeSats),
            Some(MetricValue::U64(5_000_000))
        );

        // 6. LongBlockInterval
        let mut interval_ev = ChainEvent::new(
            EventType::LongBlockInterval,
            EventSeverity::High,
            ConfidenceLevel::VerifiedOnChain,
            "Interval",
            "Desc",
        );
        interval_ev.metadata = serde_json::json!({ "interval_seconds": 5400u64 });
        assert_eq!(
            EventMetricExtractor::extract_metric(&interval_ev, BaselineMetric::IntervalSeconds),
            Some(MetricValue::U64(5400))
        );
    }

    #[test]
    fn test_rarity_band_classification() {
        let min_samples = 100;

        // Insufficient data safeguard
        assert_eq!(
            RarityBand::from_percentile(99.99, 50, min_samples),
            RarityBand::InsufficientData
        );

        // Bands with adequate sample size
        assert_eq!(
            RarityBand::from_percentile(85.0, 1000, min_samples),
            RarityBand::Common
        );
        assert_eq!(
            RarityBand::from_percentile(92.4, 1000, min_samples),
            RarityBand::Notable
        );
        assert_eq!(
            RarityBand::from_percentile(97.1, 1000, min_samples),
            RarityBand::Unusual
        );
        assert_eq!(
            RarityBand::from_percentile(99.4, 1000, min_samples),
            RarityBand::Rare
        );
        assert_eq!(
            RarityBand::from_percentile(99.95, 1000, min_samples),
            RarityBand::Extreme
        );
    }

    #[test]
    fn test_baseline_quality_evaluation() {
        assert_eq!(BaselineQuality::evaluate(1000, 0.98), BaselineQuality::High);
        assert_eq!(
            BaselineQuality::evaluate(200, 0.85),
            BaselineQuality::Moderate
        );
        assert_eq!(
            BaselineQuality::evaluate(50, 0.60),
            BaselineQuality::Degraded
        );
        assert_eq!(
            BaselineQuality::evaluate(20, 0.40),
            BaselineQuality::Insufficient
        );
    }

    #[test]
    fn test_halving_epochs() {
        assert_eq!(HalvingEpoch::epoch_from_height(0), 0);
        assert_eq!(HalvingEpoch::epoch_from_height(209_999), 0);
        assert_eq!(HalvingEpoch::epoch_from_height(210_000), 1);
        assert_eq!(HalvingEpoch::epoch_from_height(420_000), 2);
        assert_eq!(HalvingEpoch::epoch_from_height(630_000), 3);
        assert_eq!(HalvingEpoch::epoch_from_height(840_000), 4);
        assert_eq!(HalvingEpoch::epoch_from_height(968_000), 4);

        assert_eq!(HalvingEpoch::epoch_range(0), (0, 209_999));
        assert_eq!(HalvingEpoch::epoch_range(1), (210_000, 419_999));
    }
}
