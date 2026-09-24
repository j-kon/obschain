use chrono::Utc;
use obschain_core::{
    ChainEvent, ConfidenceLevel, EventSeverity, EventType, FanOutMetadata, Observation,
    SATS_PER_BTC,
};

use crate::Detector;

/// Detects transactions with high fan-out (unusually large number of outputs, e.g. batch payouts).
pub struct FanOutDetector {
    min_outputs: u32,
    min_value_sats: u64,
}

impl FanOutDetector {
    pub const DEFAULT_MIN_OUTPUTS: u32 = 50;
    pub const DEFAULT_MIN_VALUE_SATS: u64 = 0;

    pub fn new() -> Self {
        Self {
            min_outputs: Self::DEFAULT_MIN_OUTPUTS,
            min_value_sats: Self::DEFAULT_MIN_VALUE_SATS,
        }
    }

    pub fn with_thresholds(min_outputs: u32, min_value_sats: u64) -> Self {
        Self {
            min_outputs,
            min_value_sats,
        }
    }

    pub fn min_outputs(&self) -> u32 {
        self.min_outputs
    }

    pub fn min_value_sats(&self) -> u64 {
        self.min_value_sats
    }
}

impl Default for FanOutDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl Detector for FanOutDetector {
    fn name(&self) -> &'static str {
        "fan_out_detector"
    }

    fn description(&self) -> &'static str {
        "Detects batch payment or distribution transactions creating 50 or more outputs"
    }

    fn detect(&self, observation: &Observation) -> Vec<ChainEvent> {
        let Observation::Transaction(tx) = observation else {
            return Vec::new();
        };

        if tx.output_count < self.min_outputs as usize || tx.total_output_sats < self.min_value_sats
        {
            return Vec::new();
        }

        let output_input_ratio = tx.output_count as f64 / tx.input_count.max(1) as f64;
        let total_distributed_sats = tx.total_output_sats;
        let total_distributed_btc = total_distributed_sats as f64 / SATS_PER_BTC as f64;

        // Calculate output value statistics safely without large intermediate copies
        let (smallest_output_sats, largest_output_sats, median_output_sats) =
            if !tx.outputs.is_empty() {
                let mut vals: Vec<u64> = tx.outputs.iter().map(|o| o.value_sats).collect();
                vals.sort_unstable();
                let min = vals.first().copied().unwrap_or(0);
                let max = vals.last().copied().unwrap_or(0);
                let mid = vals.len() / 2;
                let median = if vals.len().is_multiple_of(2) && vals.len() >= 2 {
                    (vals[mid - 1] + vals[mid]) / 2
                } else {
                    vals[mid]
                };
                (min, max, median)
            } else {
                // If detailed outputs were omitted in compact feed, estimate from total and count
                let avg = total_distributed_sats / tx.output_count.max(1) as u64;
                (avg, avg, avg)
            };

        let severity = if tx.output_count >= 200 || total_distributed_sats >= 100 * SATS_PER_BTC {
            EventSeverity::High
        } else if tx.output_count >= 100 {
            EventSeverity::Medium
        } else {
            EventSeverity::Low
        };

        let title = format!(
            "High Fan-Out Transaction: {} outputs ({:.2} BTC)",
            tx.output_count, total_distributed_btc
        );
        let description = format!(
            "Transaction {} created {} outputs across {} input(s) (ratio {:.1}:1) distributing {:.4} BTC. Median output: {} sats.",
            tx.txid, tx.output_count, tx.input_count, output_input_ratio, total_distributed_btc, median_output_sats
        );

        let metadata = FanOutMetadata {
            input_count: tx.input_count as u32,
            output_count: tx.output_count as u32,
            output_input_ratio,
            total_distributed_sats,
            total_distributed_btc,
            median_output_sats,
            smallest_output_sats,
            largest_output_sats,
        };

        let mut event = ChainEvent::new(
            EventType::FanOut,
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
    use obschain_core::{TransactionObservation, TxOutputObservation};

    use super::*;

    fn make_fanout_tx(
        input_count: u32,
        output_count: u32,
        outputs: Vec<TxOutputObservation>,
        total_out: u64,
    ) -> TransactionObservation {
        TransactionObservation {
            txid: "fanout_tx_1".to_string(),
            timestamp: Utc::now(),
            block_hash: None,
            block_height: Some(850000),
            fee_sats: 50_000,
            size: 4000,
            weight: 16000,
            vsize: 4000,
            fee_rate_sat_vb: Some(12.5),
            total_input_sats: total_out + 50_000,
            total_output_sats: total_out,
            input_count: input_count as usize,
            output_count: output_count as usize,
            inputs: Vec::new(),
            outputs,
            is_rbf: false,
            confirmed: false,
            source: None,
        }
    }

    #[test]
    fn test_normal_tx_produces_no_event() {
        let detector = FanOutDetector::new();
        let tx = make_fanout_tx(1, 2, Vec::new(), 100_000);
        let events = detector.detect(&Observation::Transaction(tx));
        assert!(events.is_empty());
    }

    #[test]
    fn test_fifty_plus_outputs_triggers_fanout_event() {
        let detector = FanOutDetector::new();
        let mut outputs = Vec::new();
        for i in 0..60 {
            outputs.push(TxOutputObservation {
                value_sats: 100_000 + (i as u64 * 1000),
                n: i,
                script_pubkey_type: Some("v0_p2wpkh".to_string()),
                address: None,
                scriptpubkey_hex: None,
            });
        }
        let total_out = outputs.iter().map(|o| o.value_sats).sum();
        let tx = make_fanout_tx(1, 60, outputs, total_out);

        let events = detector.detect(&Observation::Transaction(tx));
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.event_type, EventType::FanOut);
        assert_eq!(ev.severity, EventSeverity::Low);

        let meta: FanOutMetadata =
            serde_json::from_value(ev.metadata.clone()).expect("valid metadata");
        assert_eq!(meta.output_count, 60);
        assert_eq!(meta.smallest_output_sats, 100_000);
        assert_eq!(meta.largest_output_sats, 159_000);
    }

    #[test]
    fn test_extreme_output_count_escalates_severity() {
        let detector = FanOutDetector::new();
        let tx = make_fanout_tx(2, 250, Vec::new(), 5 * SATS_PER_BTC);
        let events = detector.detect(&Observation::Transaction(tx));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].severity, EventSeverity::High);
    }
}
