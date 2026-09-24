use chrono::Utc;
use obschain_core::{ChainEvent, ConfidenceLevel, EventSeverity, EventType, Observation};
use serde_json::json;

use crate::Detector;

/// Detects blocks whose interval since the previous block exceeds a configurable threshold.
pub struct LongBlockIntervalDetector {
    threshold_seconds: u64,
}

impl LongBlockIntervalDetector {
    /// Default threshold is 3600 seconds (60 minutes).
    pub const DEFAULT_THRESHOLD_SECONDS: u64 = 3600;

    pub fn new() -> Self {
        Self {
            threshold_seconds: Self::DEFAULT_THRESHOLD_SECONDS,
        }
    }

    pub fn with_threshold_seconds(threshold_seconds: u64) -> Self {
        Self { threshold_seconds }
    }

    pub fn threshold_seconds(&self) -> u64 {
        self.threshold_seconds
    }
}

impl Default for LongBlockIntervalDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl Detector for LongBlockIntervalDetector {
    fn name(&self) -> &'static str {
        "long_block_interval_detector"
    }

    fn description(&self) -> &'static str {
        "Detects unusually long intervals between successive Bitcoin blocks"
    }

    fn detect(&self, observation: &Observation) -> Vec<ChainEvent> {
        let Observation::Block(block) = observation else {
            return Vec::new();
        };

        let Some(interval) = block.interval_seconds else {
            return Vec::new();
        };

        if interval < self.threshold_seconds {
            return Vec::new();
        }

        let minutes = interval / 60;
        let severity = if interval >= 7200 {
            // >= 2 hours
            EventSeverity::Critical
        } else if interval >= 5400 {
            // >= 1.5 hours
            EventSeverity::High
        } else {
            EventSeverity::Medium
        };

        let mut event = ChainEvent::new(
            EventType::LongBlockInterval,
            severity,
            ConfidenceLevel::VerifiedOnChain,
            format!("Long Block Interval: {minutes} min at height {}", block.height),
            format!(
                "Block {} at height {} was mined {minutes} minutes ({interval}s) after the previous block. Expected target is 10 minutes.",
                block.block_hash, block.height
            ),
        );

        event.block_hash = Some(block.block_hash.clone());
        event.block_height = Some(block.height);
        event.detected_at = Utc::now();
        event.metadata = json!({
            "interval_seconds": interval,
            "interval_minutes": minutes,
            "tx_count": block.tx_count,
            "miner_tag": block.miner_tag,
            "size_bytes": block.size_bytes,
        });

        vec![event]
    }
}

#[cfg(test)]
mod tests {
    use obschain_core::BlockObservation;

    use super::*;

    fn mock_block(interval_seconds: Option<u64>) -> BlockObservation {
        BlockObservation {
            block_hash: "00000000000000000002a0a4c0eb8379ef54580bf3ec457388cf0c6e8e89f929"
                .to_string(),
            height: 885000,
            timestamp: Utc::now(),
            previous_block_hash: "00000000000000000001099645903b6e82810a950bc490d1bfca722a5fbef8f3"
                .to_string(),
            tx_count: 2450,
            size_bytes: 1_650_000,
            weight: 3_990_000,
            difficulty: Some(105_000_000_000_000.0),
            miner_tag: Some("Foundry USA".to_string()),
            interval_seconds,
        }
    }

    #[test]
    fn test_normal_interval_ignored() {
        let detector = LongBlockIntervalDetector::new();
        let block = mock_block(Some(600)); // 10 minutes
        let events = detector.detect(&Observation::Block(block));
        assert!(events.is_empty());
    }

    #[test]
    fn test_long_interval_detected() {
        let detector = LongBlockIntervalDetector::new();
        let block = mock_block(Some(4200)); // 70 minutes
        let events = detector.detect(&Observation::Block(block));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, EventType::LongBlockInterval);
        assert_eq!(events[0].severity, EventSeverity::Medium);
    }

    #[test]
    fn test_critical_interval() {
        let detector = LongBlockIntervalDetector::new();
        let block = mock_block(Some(7500)); // 125 minutes
        let events = detector.detect(&Observation::Block(block));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].severity, EventSeverity::Critical);
    }
}
