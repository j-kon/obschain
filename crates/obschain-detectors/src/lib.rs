pub mod large_tx;
pub mod long_interval;

pub use large_tx::LargeTransactionDetector;
pub use long_interval::LongBlockIntervalDetector;
use obschain_core::{ChainEvent, Observation};

/// Pluggable detection interface for observing Bitcoin transactions and blocks.
pub trait Detector: Send + Sync {
    /// Unique identifier of the detector.
    fn name(&self) -> &'static str;

    /// Human-readable explanation of detector criteria.
    fn description(&self) -> &'static str;

    /// Evaluates an observation and produces zero or more chain anomaly events.
    fn detect(&self, observation: &Observation) -> Vec<ChainEvent>;
}
