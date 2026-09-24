use chrono::Utc;
use obschain_core::{
    ChainEvent, ConfidenceLevel, ConsolidationMetadata, EventSeverity, EventType, Observation,
    SATS_PER_BTC,
};

use crate::Detector;

/// Detects consolidation transactions that merge many UTXO inputs into few outputs.
pub struct ConsolidationDetector {
    min_inputs: u32,
    max_outputs: u32,
    min_value_sats: u64,
}

impl ConsolidationDetector {
    pub const DEFAULT_MIN_INPUTS: u32 = 20;
    pub const DEFAULT_MAX_OUTPUTS: u32 = 5;
    pub const DEFAULT_MIN_VALUE_SATS: u64 = 0;

    pub fn new() -> Self {
        Self {
            min_inputs: Self::DEFAULT_MIN_INPUTS,
            max_outputs: Self::DEFAULT_MAX_OUTPUTS,
            min_value_sats: Self::DEFAULT_MIN_VALUE_SATS,
        }
    }

    pub fn with_thresholds(min_inputs: u32, max_outputs: u32, min_value_sats: u64) -> Self {
        Self {
            min_inputs,
            max_outputs,
            min_value_sats,
        }
    }

    pub fn min_inputs(&self) -> u32 {
        self.min_inputs
    }

    pub fn max_outputs(&self) -> u32 {
        self.max_outputs
    }

    pub fn min_value_sats(&self) -> u64 {
        self.min_value_sats
    }
}

impl Default for ConsolidationDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl Detector for ConsolidationDetector {
    fn name(&self) -> &'static str {
        "consolidation_detector"
    }

    fn description(&self) -> &'static str {
        "Detects transactions consolidating 20+ inputs into 5 or fewer outputs"
    }

    fn detect(&self, observation: &Observation) -> Vec<ChainEvent> {
        let Observation::Transaction(tx) = observation else {
            return Vec::new();
        };

        // Consolidation requires many inputs AND few outputs
        if tx.input_count < self.min_inputs as usize || tx.output_count > self.max_outputs as usize
        {
            return Vec::new();
        }

        if tx.total_input_sats < self.min_value_sats {
            return Vec::new();
        }

        let input_output_ratio = tx.input_count as f64 / tx.output_count.max(1) as f64;
        let total_input_btc = tx.total_input_sats as f64 / SATS_PER_BTC as f64;

        let severity = if tx.input_count >= 100 || tx.total_input_sats >= 100 * SATS_PER_BTC {
            EventSeverity::High
        } else if tx.input_count >= 50 {
            EventSeverity::Medium
        } else {
            EventSeverity::Low
        };

        let title = format!(
            "UTXO Consolidation: {} inputs -> {} outputs ({:.2} BTC)",
            tx.input_count, tx.output_count, total_input_btc
        );
        let description = format!(
            "Transaction {} consolidated {} inputs into {} outputs (ratio {:.1}:1) moving {:.4} BTC with fee {} sats.",
            tx.txid, tx.input_count, tx.output_count, input_output_ratio, total_input_btc, tx.fee_sats
        );

        let metadata = ConsolidationMetadata {
            input_count: tx.input_count as u32,
            output_count: tx.output_count as u32,
            input_output_ratio,
            total_input_sats: tx.total_input_sats,
            total_output_sats: tx.total_output_sats,
            fee_sats: tx.fee_sats,
        };

        let mut event = ChainEvent::new(
            EventType::Consolidation,
            severity,
            ConfidenceLevel::VerifiedOnChain,
            title,
            description,
        )
        .with_typed_metadata(&metadata);

        event.txid = Some(tx.txid.clone());
        event.block_hash = tx.block_hash.clone();
        event.block_height = tx.block_height;
        event.detected_at = Utc::now();

        vec![event]
    }
}

#[cfg(test)]
mod tests {
    use obschain_core::TransactionObservation;

    use super::*;

    fn make_tx(inputs: u32, outputs: u32, total_in: u64) -> TransactionObservation {
        TransactionObservation {
            txid: "consolidation_tx_1".to_string(),
            timestamp: Utc::now(),
            block_hash: None,
            block_height: Some(850000),
            fee_sats: 10_000,
            size: 1500,
            weight: 6000,
            vsize: 1500,
            fee_rate_sat_vb: Some(6.6),
            total_input_sats: total_in,
            total_output_sats: total_in.saturating_sub(10_000),
            input_count: inputs as usize,
            output_count: outputs as usize,
            inputs: Vec::new(),
            outputs: Vec::new(),
            is_rbf: false,
            confirmed: false,
            source: None,
        }
    }

    #[test]
    fn test_normal_two_input_tx_produces_no_event() {
        let detector = ConsolidationDetector::new();
        let tx = make_tx(2, 2, 500_000);
        let events = detector.detect(&Observation::Transaction(tx));
        assert!(events.is_empty());
    }

    #[test]
    fn test_twenty_inputs_few_outputs_emits_consolidation_event() {
        let detector = ConsolidationDetector::new();
        let tx = make_tx(25, 2, 10 * SATS_PER_BTC);
        let events = detector.detect(&Observation::Transaction(tx));
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.event_type, EventType::Consolidation);
        assert_eq!(ev.severity, EventSeverity::Low);

        let meta: ConsolidationMetadata =
            serde_json::from_value(ev.metadata.clone()).expect("valid metadata");
        assert_eq!(meta.input_count, 25);
        assert_eq!(meta.output_count, 2);
        assert!((meta.input_output_ratio - 12.5).abs() < 1e-6);
    }

    #[test]
    fn test_many_inputs_many_outputs_avoids_false_positive() {
        let detector = ConsolidationDetector::new();
        // 50 inputs into 50 outputs is a batch payout or coinjoin, not a consolidation!
        let tx = make_tx(50, 50, 20 * SATS_PER_BTC);
        let events = detector.detect(&Observation::Transaction(tx));
        assert!(events.is_empty());
    }

    #[test]
    fn test_high_input_count_escalates_severity() {
        let detector = ConsolidationDetector::new();
        let tx = make_tx(120, 1, 50 * SATS_PER_BTC);
        let events = detector.detect(&Observation::Transaction(tx));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].severity, EventSeverity::High);
    }
}
