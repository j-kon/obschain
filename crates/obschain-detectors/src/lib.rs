pub mod consolidation;
pub mod dedup;
pub mod dormant;
pub mod engine;
pub mod extreme_fee;
pub mod fan_out;
pub mod large_tx;
pub mod long_interval;
pub mod rbf;
pub mod reorg;

pub use consolidation::ConsolidationDetector;
pub use dedup::EventDeduplicator;
pub use dormant::DormantCoinDetector;
pub use engine::DetectorEngine;
pub use extreme_fee::ExtremeFeeDetector;
pub use fan_out::FanOutDetector;
pub use large_tx::LargeTransactionDetector;
pub use long_interval::LongBlockIntervalDetector;
use obschain_core::{ChainEvent, Observation};
pub use rbf::RbfDetector;
pub use reorg::ReorgDetector;
use serde::{Deserialize, Serialize};

/// Metadata describing the applicability, observer requirements, and historical replayability
/// of a detector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DetectorAvailability {
    pub historically_replayable: bool,
    pub requires_mempool_history: bool,
    pub requires_multiple_observers: bool,
    pub requires_external_labels: bool,
    pub notes: Option<&'static str>,
}

impl Default for DetectorAvailability {
    fn default() -> Self {
        Self {
            historically_replayable: true,
            requires_mempool_history: false,
            requires_multiple_observers: false,
            requires_external_labels: false,
            notes: None,
        }
    }
}

/// Pluggable detection interface for observing Bitcoin transactions and blocks.
pub trait Detector: Send + Sync {
    /// Unique identifier of the detector.
    fn name(&self) -> &'static str;

    /// Human-readable explanation of detector criteria.
    fn description(&self) -> &'static str;

    /// Evaluates an observation and produces zero or more chain anomaly events.
    fn detect(&self, observation: &Observation) -> Vec<ChainEvent>;

    /// Declares whether this detector can be accurately evaluated over historical block replay.
    fn availability(&self) -> DetectorAvailability {
        DetectorAvailability::default()
    }
}
