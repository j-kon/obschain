use chrono::Utc;
use obschain_core::{
    ChainEvent, ConfidenceLevel, EventSeverity, EventType, Observation, ReplacementMetadata,
    SATS_PER_BTC,
};

use crate::Detector;

/// Detects actual observed transaction replacements (RBF) in the mempool.
///
/// NOTE: BIP-125 opt-in signaling on individual transactions only indicates replaceability
/// and does NOT trigger a replacement event. A replacement event is emitted only when an
/// actual transaction replacement (`Observation::Replacement`) is observed.
pub struct RbfDetector;

impl RbfDetector {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RbfDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl Detector for RbfDetector {
    fn name(&self) -> &'static str {
        "rbf_detector"
    }

    fn description(&self) -> &'static str {
        "Detects confirmed mempool transaction replacements (RBF) with fee and delta analytics"
    }

    fn detect(&self, observation: &Observation) -> Vec<ChainEvent> {
        let Observation::Replacement(repl) = observation else {
            // BIP-125 signaling alone on transactions does not constitute a replacement event
            return Vec::new();
        };

        // Validate basic integrity of replacement observation
        if repl.replacement_txid.is_empty() || repl.replaced_txids.is_empty() {
            return Vec::new();
        }

        let replaced_count = repl.replaced_txids.len();
        let fee_delta_sats = (repl.new_fee_sats as i64) - (repl.old_fee_sats as i64);

        let fee_increase_percent = if repl.old_fee_sats > 0 {
            Some(((fee_delta_sats as f64) / (repl.old_fee_sats as f64)) * 100.0)
        } else {
            None
        };

        // Determine severity objectively from fee delta and replaced transaction volume
        let severity = if fee_delta_sats >= 1_000_000 || replaced_count >= 5 {
            EventSeverity::High
        } else if fee_delta_sats >= 100_000 || replaced_count >= 2 {
            EventSeverity::Medium
        } else {
            EventSeverity::Low
        };

        let new_fee_btc = repl.new_fee_sats as f64 / SATS_PER_BTC as f64;
        let delta_desc = if let Some(pct) = fee_increase_percent {
            format!("{fee_delta_sats:+} sats ({pct:+.1}%)")
        } else {
            format!("{fee_delta_sats:+} sats")
        };

        // Title and description must remain strictly neutral; RBF is a standard network mechanism
        let title = format!(
            "Transaction replacement observed: {} ({} txs replaced)",
            &repl.replacement_txid[..12.min(repl.replacement_txid.len())],
            replaced_count
        );
        let description = format!(
            "Observed RBF replacement: transaction {} replaced {} transaction(s). New fee: {} sats ({:.4} BTC), fee delta: {}.",
            repl.replacement_txid, replaced_count, repl.new_fee_sats, new_fee_btc, delta_desc
        );

        let metadata = ReplacementMetadata {
            replaced_txids: repl.replaced_txids.clone(),
            replacement_txid: repl.replacement_txid.clone(),
            replaced_count,
            old_fee_sats: repl.old_fee_sats,
            new_fee_sats: repl.new_fee_sats,
            fee_delta_sats,
            fee_increase_percent,
            old_fee_rate_sat_vb: repl.old_fee_rate_sat_vb,
            new_fee_rate_sat_vb: repl.new_fee_rate_sat_vb,
        };

        let mut event = ChainEvent::new(
            EventType::TransactionReplacement,
            severity,
            ConfidenceLevel::VerifiedOnChain,
            title,
            description,
        )
        .with_typed_metadata(&metadata);

        event.txid = Some(repl.replacement_txid.clone());
        event.detected_at = Utc::now();
        event.source = repl.source.clone();

        vec![event]
    }

    fn availability(&self) -> crate::DetectorAvailability {
        crate::DetectorAvailability {
            historically_replayable: false,
            requires_mempool_history: true,
            requires_multiple_observers: false,
            requires_external_labels: false,
            notes: Some("Requires unconfirmed mempool observation stream; cannot be reconstructed purely from confirmed blockchain history"),
        }
    }
}

#[cfg(test)]
mod tests {
    use obschain_core::{ObservationSource, TransactionObservation, TransactionReplacement};

    use super::*;

    #[test]
    fn test_rbf_signaling_alone_does_not_emit_replacement_event() {
        let detector = RbfDetector::new();
        let tx = TransactionObservation {
            txid: "rbf_opt_in_tx".to_string(),
            timestamp: Utc::now(),
            block_hash: None,
            block_height: None,
            fee_sats: 1000,
            size: 200,
            weight: 800,
            vsize: 200,
            fee_rate_sat_vb: Some(5.0),
            total_input_sats: 50_000,
            total_output_sats: 49_000,
            input_count: 1,
            output_count: 2,
            inputs: Vec::new(),
            outputs: Vec::new(),
            is_rbf: true, // Opt-in signaled!
            confirmed: false,
            source: None,
        };

        let events = detector.detect(&Observation::Transaction(tx));
        assert!(
            events.is_empty(),
            "RBF signaling alone must not create an event"
        );
    }

    #[test]
    fn test_actual_replacement_emits_event_with_neutral_title() {
        let detector = RbfDetector::new();
        let repl = TransactionReplacement {
            replaced_txids: vec!["old_tx_1".to_string()],
            replacement_txid: "new_tx_2".to_string(),
            old_fee_sats: 2000,
            new_fee_sats: 5000,
            fee_delta_sats: 3000,
            old_vsize: Some(200),
            new_vsize: Some(200),
            old_fee_rate_sat_vb: Some(10.0),
            new_fee_rate_sat_vb: Some(25.0),
            observed_at: Utc::now(),
            source: Some(ObservationSource::mempool_ws("wss://test")),
        };

        let events = detector.detect(&Observation::Replacement(repl));
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.event_type, EventType::TransactionReplacement);
        assert!(ev.title.contains("Transaction replacement observed"));
        assert!(!ev.title.contains("Double-spend"));

        let meta: ReplacementMetadata =
            serde_json::from_value(ev.metadata.clone()).expect("valid metadata");
        assert_eq!(meta.replaced_count, 1);
        assert_eq!(meta.fee_delta_sats, 3000);
        assert_eq!(meta.fee_increase_percent, Some(150.0));
    }

    #[test]
    fn test_malformed_replacement_is_ignored() {
        let detector = RbfDetector::new();
        let repl = TransactionReplacement {
            replaced_txids: Vec::new(), // empty
            replacement_txid: "".to_string(),
            old_fee_sats: 0,
            new_fee_sats: 0,
            fee_delta_sats: 0,
            old_vsize: None,
            new_vsize: None,
            old_fee_rate_sat_vb: None,
            new_fee_rate_sat_vb: None,
            observed_at: Utc::now(),
            source: None,
        };

        let events = detector.detect(&Observation::Replacement(repl));
        assert!(events.is_empty());
    }

    #[test]
    fn test_large_fee_bump_escalates_severity() {
        let detector = RbfDetector::new();
        let repl = TransactionReplacement {
            replaced_txids: vec!["tx1".to_string(), "tx2".to_string()],
            replacement_txid: "tx_new".to_string(),
            old_fee_sats: 10_000,
            new_fee_sats: 1_200_000, // fee bump > 1,000,000 sats
            fee_delta_sats: 1_190_000,
            old_vsize: Some(2000),
            new_vsize: Some(10000),
            old_fee_rate_sat_vb: Some(5.0),
            new_fee_rate_sat_vb: Some(120.0),
            observed_at: Utc::now(),
            source: None,
        };

        let events = detector.detect(&Observation::Replacement(repl));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].severity, EventSeverity::High);
    }
}
