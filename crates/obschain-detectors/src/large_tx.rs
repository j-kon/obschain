use chrono::Utc;
use obschain_core::{ChainEvent, ConfidenceLevel, EventSeverity, EventType, Observation};
use serde_json::json;

use crate::Detector;

/// Detects transfers exceeding a configurable threshold in satoshis.
pub struct LargeTransactionDetector {
    threshold_sats: u64,
}

impl LargeTransactionDetector {
    /// Default threshold is 100 BTC (10,000,000,000 satoshis).
    pub const DEFAULT_THRESHOLD_SATS: u64 = 100 * 100_000_000;

    pub fn new() -> Self {
        Self {
            threshold_sats: Self::DEFAULT_THRESHOLD_SATS,
        }
    }

    pub fn with_threshold_sats(threshold_sats: u64) -> Self {
        Self { threshold_sats }
    }

    pub fn threshold_sats(&self) -> u64 {
        self.threshold_sats
    }
}

impl Default for LargeTransactionDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl Detector for LargeTransactionDetector {
    fn name(&self) -> &'static str {
        "large_transaction_detector"
    }

    fn description(&self) -> &'static str {
        "Detects on-chain transactions moving unusually large volumes of Bitcoin"
    }

    fn detect(&self, observation: &Observation) -> Vec<ChainEvent> {
        let Observation::Transaction(tx) = observation else {
            return Vec::new();
        };

        if tx.total_output_sats < self.threshold_sats {
            return Vec::new();
        }

        let btc_amount = tx.total_output_sats as f64 / 100_000_000.0;
        let severity = if tx.total_output_sats >= 10_000 * 100_000_000 {
            EventSeverity::Critical
        } else if tx.total_output_sats >= 1_000 * 100_000_000 {
            EventSeverity::High
        } else {
            EventSeverity::Medium
        };

        let mut event = ChainEvent::new(
            EventType::LargeTransfer,
            severity,
            ConfidenceLevel::VerifiedOnChain,
            format!("Large Transfer: {btc_amount:.2} BTC"),
            format!(
                "Transaction {} transferred {btc_amount:.2} BTC across {} outputs with fee {} sats.",
                tx.txid,
                tx.output_count,
                tx.fee_sats
            ),
        );

        event.txid = Some(tx.txid.clone());
        event.block_hash = tx.block_hash.clone();
        event.block_height = tx.block_height;
        event.detected_at = Utc::now();
        event.metadata = json!({
            "total_output_sats": tx.total_output_sats,
            "total_output_btc": btc_amount,
            "fee_sats": tx.fee_sats,
            "fee_rate_sat_vb": tx.fee_rate_sat_vb,
            "vsize": tx.vsize,
            "inputs_count": tx.input_count,
            "outputs_count": tx.output_count,
        });

        vec![event]
    }
}

#[cfg(test)]
mod tests {
    use obschain_core::TransactionObservation;

    use super::*;

    fn mock_tx(output_sats: u64) -> TransactionObservation {
        TransactionObservation {
            txid: "0000000000000000000000000000000000000000000000000000000000000001".to_string(),
            timestamp: Utc::now(),
            block_hash: None,
            block_height: None,
            fee_sats: 1500,
            size: 250,
            weight: 840,
            vsize: 210,
            fee_rate_sat_vb: Some(7.14),
            total_input_sats: output_sats + 1500,
            total_output_sats: output_sats,
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
    fn test_below_threshold_ignores() {
        let detector = LargeTransactionDetector::new();
        let tx = mock_tx(50 * 100_000_000); // 50 BTC < 100 BTC
        let events = detector.detect(&Observation::Transaction(tx));
        assert!(events.is_empty());
    }

    #[test]
    fn test_above_threshold_detects() {
        let detector = LargeTransactionDetector::new();
        let tx = mock_tx(250 * 100_000_000); // 250 BTC
        let events = detector.detect(&Observation::Transaction(tx));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, EventType::LargeTransfer);
        assert_eq!(events[0].severity, EventSeverity::Medium);
    }

    #[test]
    fn test_critical_threshold() {
        let detector = LargeTransactionDetector::new();
        let tx = mock_tx(15_000 * 100_000_000); // 15,000 BTC
        let events = detector.detect(&Observation::Transaction(tx));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].severity, EventSeverity::Critical);
    }
}
