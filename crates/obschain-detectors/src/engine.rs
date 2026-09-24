use std::sync::Arc;

use obschain_core::{BlockObservation, ChainEvent, Observation};

use crate::Detector;

/// Orchestrates execution of anomaly detectors over normalized observation streams.
pub struct DetectorEngine {
    detectors: Vec<Arc<dyn Detector>>,
    last_block: Option<BlockObservation>,
}

impl DetectorEngine {
    pub fn new(detectors: Vec<Arc<dyn Detector>>) -> Self {
        Self {
            detectors,
            last_block: None,
        }
    }

    pub fn detectors(&self) -> &[Arc<dyn Detector>] {
        &self.detectors
    }

    /// Evaluates an observation through all registered detectors:
    /// - If block observation arrives, validates and derives interval relative to previously observed block.
    /// - Attaches provenance source to generated events.
    pub fn process_observation(&mut self, mut observation: Observation) -> Vec<ChainEvent> {
        let obs_source = observation.source().cloned();

        if let Observation::Block(ref mut block) = observation {
            if let Some(ref prev) = self.last_block {
                if block.height > prev.height || block.block_hash != prev.block_hash {
                    let diff_secs = block
                        .timestamp
                        .signed_duration_since(prev.timestamp)
                        .num_seconds();

                    // Non-monotonic or negative intervals are ignored for safety
                    if diff_secs > 0 {
                        block.interval_seconds = Some(diff_secs as u64);
                    }
                }
            }
            self.last_block = Some(block.clone());
        }

        let mut events = Vec::new();
        for detector in &self.detectors {
            let detected = detector.detect(&observation);
            for mut ev in detected {
                if ev.source.is_none() {
                    ev.source = obs_source.clone();
                }
                events.push(ev);
            }
        }

        events
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use obschain_core::{
        BlockObservation, EventType, Observation, ObservationSource, TransactionObservation,
    };

    use super::*;
    use crate::{LargeTransactionDetector, LongBlockIntervalDetector};

    #[test]
    fn test_engine_normal_tx_no_event() {
        let detectors: Vec<Arc<dyn Detector>> = vec![
            Arc::new(LargeTransactionDetector::new()),
            Arc::new(LongBlockIntervalDetector::new()),
        ];
        let mut engine = DetectorEngine::new(detectors);

        let tx = TransactionObservation {
            txid: "1111111111111111111111111111111111111111111111111111111111111111".to_string(),
            timestamp: Utc::now(),
            block_hash: None,
            block_height: None,
            fee_sats: 1000,
            size: 200,
            weight: 800,
            vsize: 200,
            fee_rate_sat_vb: Some(5.0),
            total_input_sats: 21_000,
            total_output_sats: 20_000,
            input_count: 1,
            output_count: 2,
            inputs: Vec::new(),
            outputs: Vec::new(),
            is_rbf: false,
            confirmed: false,
            source: Some(ObservationSource::mempool_ws("wss://test")),
        };

        let events = engine.process_observation(Observation::Transaction(tx));
        assert!(events.is_empty());
    }

    #[test]
    fn test_engine_large_tx_emits_event_with_provenance() {
        let detectors: Vec<Arc<dyn Detector>> = vec![Arc::new(
            LargeTransactionDetector::with_threshold_sats(100 * 100_000_000),
        )];
        let mut engine = DetectorEngine::new(detectors);

        let tx = TransactionObservation {
            txid: "2222222222222222222222222222222222222222222222222222222222222222".to_string(),
            timestamp: Utc::now(),
            block_hash: None,
            block_height: None,
            fee_sats: 5000,
            size: 250,
            weight: 1000,
            vsize: 250,
            fee_rate_sat_vb: Some(20.0),
            total_input_sats: 500 * 100_000_000 + 5000,
            total_output_sats: 500 * 100_000_000,
            input_count: 2,
            output_count: 2,
            inputs: Vec::new(),
            outputs: Vec::new(),
            is_rbf: false,
            confirmed: false,
            source: Some(ObservationSource::mempool_ws(
                "wss://mempool.space/api/v1/ws",
            )),
        };

        let events = engine.process_observation(Observation::Transaction(tx));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, EventType::LargeTransfer);
        assert_eq!(
            events[0].source,
            Some(ObservationSource::mempool_ws(
                "wss://mempool.space/api/v1/ws"
            ))
        );
    }

    #[test]
    fn test_engine_tracks_interval_between_blocks() {
        let detectors: Vec<Arc<dyn Detector>> = vec![
            Arc::new(LongBlockIntervalDetector::with_threshold_seconds(1800)), // 30 minutes
        ];
        let mut engine = DetectorEngine::new(detectors);

        let now = Utc::now();
        let b1 = BlockObservation {
            block_hash: "hash1".to_string(),
            height: 900000,
            timestamp: now,
            previous_block_hash: "prev".to_string(),
            tx_count: 2000,
            size_bytes: 1_500_000,
            weight: 3_900_000,
            difficulty: None,
            miner_tag: None,
            interval_seconds: None,
            source: Some(ObservationSource::mempool_ws("wss://test")),
        };

        // First block: no previous block, so interval_seconds is None -> no event
        let events1 = engine.process_observation(Observation::Block(b1));
        assert!(events1.is_empty());

        // Second block mined 45 minutes later (> 30 min threshold)
        let b2 = BlockObservation {
            block_hash: "hash2".to_string(),
            height: 900001,
            timestamp: now + chrono::Duration::minutes(45),
            previous_block_hash: "hash1".to_string(),
            tx_count: 3500,
            size_bytes: 1_800_000,
            weight: 3_990_000,
            difficulty: None,
            miner_tag: None,
            interval_seconds: None,
            source: Some(ObservationSource::mempool_ws("wss://test")),
        };

        let events2 = engine.process_observation(Observation::Block(b2));
        assert_eq!(events2.len(), 1);
        assert_eq!(events2[0].event_type, EventType::LongBlockInterval);
    }
}
