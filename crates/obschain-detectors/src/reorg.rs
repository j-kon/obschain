use obschain_core::{ChainEvent, ConfidenceLevel, EventSeverity, EventType, Observation};
use serde_json::json;

use crate::Detector;

/// Detects and evaluates Bitcoin blockchain reorganizations and stale block races.
pub struct ReorgDetector;

impl ReorgDetector {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ReorgDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl Detector for ReorgDetector {
    fn name(&self) -> &'static str {
        "reorg_detector"
    }

    fn description(&self) -> &'static str {
        "Detects blockchain reorganizations, competing chain tips, and stale blocks"
    }

    fn detect(&self, observation: &Observation) -> Vec<ChainEvent> {
        let Observation::Reorg(reorg) = observation else {
            return Vec::new();
        };

        let severity = match reorg.depth {
            0 | 1 => EventSeverity::Low,
            2 => EventSeverity::Medium,
            3..=5 => EventSeverity::High,
            _ => EventSeverity::Critical,
        };

        let title = if reorg.depth <= 1 {
            "Single-Block Chain Reorganization (Stale Block)".to_string()
        } else {
            format!("Chain Reorganization Detected (Depth {})", reorg.depth)
        };

        let description = if reorg.depth <= 1 {
            format!(
                "Competing block resolved at height {}. Block {} replaced by {}",
                reorg.old_tip_height,
                &reorg.old_tip_hash[..reorg.old_tip_hash.len().min(16)],
                &reorg.new_tip_hash[..reorg.new_tip_hash.len().min(16)]
            )
        } else {
            format!(
                "Chain reorganization of depth {} detected. Replaced tip {} (height {}) with {} (height {})",
                reorg.depth,
                &reorg.old_tip_hash[..reorg.old_tip_hash.len().min(16)],
                reorg.old_tip_height,
                &reorg.new_tip_hash[..reorg.new_tip_hash.len().min(16)],
                reorg.new_tip_height
            )
        };

        let metadata = json!({
            "old_tip": reorg.old_tip_hash,
            "old_tip_height": reorg.old_tip_height,
            "new_tip": reorg.new_tip_hash,
            "new_tip_height": reorg.new_tip_height,
            "common_ancestor": reorg.common_ancestor_hash,
            "depth": reorg.depth,
            "disconnected_blocks": reorg.disconnected_blocks,
            "connected_blocks": reorg.connected_blocks,
            "observed_at": reorg.observed_at,
        });

        let mut event = ChainEvent::new(
            EventType::ReorgDetected,
            severity,
            ConfidenceLevel::VerifiedOnChain,
            title,
            description,
        );

        event.detected_at = reorg.observed_at;
        event.block_height = Some(reorg.new_tip_height);
        event.block_hash = Some(reorg.new_tip_hash.clone());
        event.source = reorg.source.clone();
        event.metadata = metadata;

        if let Some(src) = &reorg.source {
            event.add_witness(obschain_core::ObservationWitness {
                source: src.clone(),
                observed_at: reorg.observed_at,
                metadata: None,
            });
        }

        vec![event]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use obschain_core::{ObservationSource, ReorgObservation};

    #[test]
    fn test_single_block_stale_reorg_emits_low_severity() {
        let detector = ReorgDetector::new();
        let reorg = ReorgObservation {
            old_tip_hash: "00000000000000000001old".to_string(),
            old_tip_height: 850000,
            new_tip_hash: "00000000000000000001new".to_string(),
            new_tip_height: 850000,
            common_ancestor_hash: Some("00000000000000000000common".to_string()),
            depth: 1,
            disconnected_blocks: vec!["00000000000000000001old".to_string()],
            connected_blocks: vec!["00000000000000000001new".to_string()],
            observed_at: Utc::now(),
            source: Some(ObservationSource::bitcoin_core_rpc("http://127.0.0.1:8332")),
        };

        let events = detector.detect(&Observation::Reorg(reorg));
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.event_type, EventType::ReorgDetected);
        assert_eq!(event.severity, EventSeverity::Low);
        assert!(event.title.contains("Stale Block"));
        assert_eq!(event.block_height, Some(850000));
        assert_eq!(event.witnesses.len(), 1);
    }

    #[test]
    fn test_deep_reorg_escalates_to_critical() {
        let detector = ReorgDetector::new();
        let reorg = ReorgObservation {
            old_tip_hash: "00000000000000000006old".to_string(),
            old_tip_height: 850005,
            new_tip_hash: "00000000000000000006new".to_string(),
            new_tip_height: 850006,
            common_ancestor_hash: Some("00000000000000000000common".to_string()),
            depth: 6,
            disconnected_blocks: vec!["b1".to_string(), "b2".to_string()],
            connected_blocks: vec!["b3".to_string(), "b4".to_string(), "b5".to_string()],
            observed_at: Utc::now(),
            source: Some(ObservationSource::bitcoin_core_zmq("tcp://127.0.0.1:28333")),
        };

        let events = detector.detect(&Observation::Reorg(reorg));
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.severity, EventSeverity::Critical);
        assert!(event.title.contains("Depth 6"));
    }
}
