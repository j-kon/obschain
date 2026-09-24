use chrono::Utc;
use obschain_core::{
    ChainEvent, ConfidenceLevel, EventSeverity, EventType, ExtremeFeeMetadata,
    ExtremeFeeTriggerType, Observation, SATS_PER_BTC,
};

use crate::Detector;

/// Detects transactions with abnormally high absolute fees or extreme fee rates.
pub struct ExtremeFeeDetector {
    extreme_fee_sats: u64,
    extreme_fee_rate_sat_vb: f64,
}

impl ExtremeFeeDetector {
    /// Default absolute fee threshold: 0.1 BTC (10,000,000 satoshis).
    pub const DEFAULT_EXTREME_FEE_SATS: u64 = 10_000_000;
    /// Default fee rate threshold: 200 sat/vB.
    pub const DEFAULT_EXTREME_FEE_RATE_SAT_VB: f64 = 200.0;

    pub fn new() -> Self {
        Self {
            extreme_fee_sats: Self::DEFAULT_EXTREME_FEE_SATS,
            extreme_fee_rate_sat_vb: Self::DEFAULT_EXTREME_FEE_RATE_SAT_VB,
        }
    }

    pub fn with_thresholds(extreme_fee_sats: u64, extreme_fee_rate_sat_vb: f64) -> Self {
        Self {
            extreme_fee_sats,
            extreme_fee_rate_sat_vb,
        }
    }

    pub fn extreme_fee_sats(&self) -> u64 {
        self.extreme_fee_sats
    }

    pub fn extreme_fee_rate_sat_vb(&self) -> f64 {
        self.extreme_fee_rate_sat_vb
    }
}

impl Default for ExtremeFeeDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl Detector for ExtremeFeeDetector {
    fn name(&self) -> &'static str {
        "extreme_fee_detector"
    }

    fn description(&self) -> &'static str {
        "Detects transactions with extreme absolute fees (>= 0.1 BTC) or extreme fee rates (>= 200 sat/vB)"
    }

    fn detect(&self, observation: &Observation) -> Vec<ChainEvent> {
        let Observation::Transaction(tx) = observation else {
            return Vec::new();
        };

        let fee_rate = tx.fee_rate_sat_vb.unwrap_or_else(|| {
            if tx.vsize > 0 {
                tx.fee_sats as f64 / tx.vsize as f64
            } else {
                0.0
            }
        });

        let is_high_fee = tx.fee_sats >= self.extreme_fee_sats;
        let is_high_rate = fee_rate >= self.extreme_fee_rate_sat_vb;

        if !is_high_fee && !is_high_rate {
            return Vec::new();
        }

        let trigger_type = match (is_high_fee, is_high_rate) {
            (true, true) => ExtremeFeeTriggerType::Both,
            (true, false) => ExtremeFeeTriggerType::HighAbsoluteFee,
            (false, true) => ExtremeFeeTriggerType::HighFeeRate,
            (false, false) => unreachable!(),
        };

        // Escalate severity for truly extreme or potentially anomalous/fat-finger fees
        let severity = if tx.fee_sats >= SATS_PER_BTC || fee_rate >= 1000.0 {
            EventSeverity::Critical
        } else if tx.fee_sats >= 50_000_000 || fee_rate >= 500.0 {
            EventSeverity::High
        } else {
            EventSeverity::Medium
        };

        let fee_btc = tx.fee_sats as f64 / SATS_PER_BTC as f64;
        let trigger_desc = match trigger_type {
            ExtremeFeeTriggerType::HighAbsoluteFee => "High absolute fee",
            ExtremeFeeTriggerType::HighFeeRate => "High fee rate",
            ExtremeFeeTriggerType::Both => "High fee and fee rate",
        };

        let title = format!(
            "Extreme Fee: {:.4} BTC ({:.1} sat/vB) [{trigger_desc}]",
            fee_btc, fee_rate
        );
        let description = format!(
            "Transaction {} paid an extreme fee of {} sats ({:.4} BTC) at {:.2} sat/vB (vsize {} vB).",
            tx.txid, tx.fee_sats, fee_btc, fee_rate, tx.vsize
        );

        let metadata = ExtremeFeeMetadata {
            fee_sats: tx.fee_sats,
            fee_btc,
            fee_rate_sat_vb: Some(fee_rate),
            vsize: tx.vsize,
            total_input_sats: tx.total_input_sats,
            total_output_sats: tx.total_output_sats,
            fee_trigger_type: trigger_type,
        };

        let mut event = ChainEvent::new(
            EventType::ExtremeFee,
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

    fn make_fee_tx(fee_sats: u64, vsize: u64, fee_rate: Option<f64>) -> TransactionObservation {
        let total_in = fee_sats + 100_000_000;
        let total_out = 100_000_000;
        TransactionObservation {
            txid: "fee_tx_1".to_string(),
            timestamp: Utc::now(),
            block_hash: None,
            block_height: Some(850000),
            fee_sats,
            size: vsize * 4,
            weight: vsize * 4,
            vsize,
            fee_rate_sat_vb: fee_rate,
            total_input_sats: total_in,
            total_output_sats: total_out,
            input_count: 1,
            output_count: 2,
            inputs: Vec::new(),
            outputs: Vec::new(),
            is_rbf: false,
            confirmed: false,
            source: None,
        }
    }

    #[test]
    fn test_normal_fee_produces_no_event() {
        let detector = ExtremeFeeDetector::new();
        let tx = make_fee_tx(5000, 250, Some(20.0));
        let events = detector.detect(&Observation::Transaction(tx));
        assert!(events.is_empty());
    }

    #[test]
    fn test_high_absolute_fee_triggers_event() {
        let detector = ExtremeFeeDetector::new();
        // 0.15 BTC fee, but standard 25 sat/vB rate because it's a huge tx
        let tx = make_fee_tx(15_000_000, 600_000, Some(25.0));
        let events = detector.detect(&Observation::Transaction(tx));
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.event_type, EventType::ExtremeFee);

        let meta: ExtremeFeeMetadata =
            serde_json::from_value(ev.metadata.clone()).expect("valid metadata");
        assert_eq!(
            meta.fee_trigger_type,
            ExtremeFeeTriggerType::HighAbsoluteFee
        );
        assert_eq!(meta.fee_sats, 15_000_000);
    }

    #[test]
    fn test_high_fee_rate_triggers_event() {
        let detector = ExtremeFeeDetector::new();
        // Small absolute fee (50,000 sats = 0.0005 BTC) but 350 sat/vB on a tiny 140vB tx
        let tx = make_fee_tx(50_000, 140, Some(350.0));
        let events = detector.detect(&Observation::Transaction(tx));
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.event_type, EventType::ExtremeFee);

        let meta: ExtremeFeeMetadata =
            serde_json::from_value(ev.metadata.clone()).expect("valid metadata");
        assert_eq!(meta.fee_trigger_type, ExtremeFeeTriggerType::HighFeeRate);
    }

    #[test]
    fn test_both_high_absolute_and_rate_triggers_critical() {
        let detector = ExtremeFeeDetector::new();
        // 1.5 BTC fee at 1500 sat/vB (e.g. fat finger)
        let tx = make_fee_tx(150_000_000, 1000, Some(1500.0));
        let events = detector.detect(&Observation::Transaction(tx));
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.severity, EventSeverity::Critical);

        let meta: ExtremeFeeMetadata =
            serde_json::from_value(ev.metadata.clone()).expect("valid metadata");
        assert_eq!(meta.fee_trigger_type, ExtremeFeeTriggerType::Both);
    }

    #[test]
    fn test_zero_vsize_safety_does_not_panic() {
        let detector = ExtremeFeeDetector::new();
        let tx = make_fee_tx(0, 0, None);
        let events = detector.detect(&Observation::Transaction(tx));
        assert!(events.is_empty());
    }
}
