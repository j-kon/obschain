use chrono::{DateTime, Utc};
use obschain_core::{
    calculate_satoshi_days, satoshi_days_to_btc_days, ChainEvent, ConfidenceLevel,
    DormantClassification, DormantCoinsMetadata, EventSeverity, EventType, Observation,
    SATS_PER_BTC,
};

use crate::Detector;

/// Historical cutoff for Early Bitcoin classification: January 1, 2011 00:00:00 UTC.
pub const EARLY_BITCOIN_TIMESTAMP_CUTOFF_SECS: i64 = 1_293_840_000;
/// Early Bitcoin block height cutoff (approx Block 100,000, reached late 2010).
pub const EARLY_BITCOIN_BLOCK_HEIGHT_CUTOFF: u64 = 100_000;

/// Detects spending of long-dormant Bitcoin UTXOs based on historical confirmation age.
pub struct DormantCoinDetector {
    min_age_days: u64,
    min_value_sats: u64,
}

impl DormantCoinDetector {
    /// Default threshold: 5 years (1825 days).
    pub const DEFAULT_MIN_AGE_DAYS: u64 = 1825;
    /// Default minimum dormant value: 1 BTC (100,000,000 sats).
    pub const DEFAULT_MIN_VALUE_SATS: u64 = SATS_PER_BTC;

    pub fn new() -> Self {
        Self {
            min_age_days: Self::DEFAULT_MIN_AGE_DAYS,
            min_value_sats: Self::DEFAULT_MIN_VALUE_SATS,
        }
    }

    pub fn with_thresholds(min_age_days: u64, min_value_sats: u64) -> Self {
        Self {
            min_age_days,
            min_value_sats,
        }
    }

    pub fn min_age_days(&self) -> u64 {
        self.min_age_days
    }

    pub fn min_value_sats(&self) -> u64 {
        self.min_value_sats
    }

    /// Evaluates objective classification based strictly on coin age and confirmation epoch.
    pub fn classify(
        oldest_age_days: u64,
        confirmed_at: Option<DateTime<Utc>>,
        confirmed_height: Option<u64>,
    ) -> DormantClassification {
        // Objective Early Bitcoin cutoff: Block <= 100,000 or confirmed before 2011-01-01
        if let Some(height) = confirmed_height {
            if height <= EARLY_BITCOIN_BLOCK_HEIGHT_CUTOFF {
                return DormantClassification::EarlyBitcoin;
            }
        }
        if let Some(ts) = confirmed_at {
            if ts.timestamp() < EARLY_BITCOIN_TIMESTAMP_CUTOFF_SECS {
                return DormantClassification::EarlyBitcoin;
            }
        }

        if oldest_age_days >= 5475 {
            // 15+ years
            DormantClassification::Ancient
        } else if oldest_age_days >= 3650 {
            // 10+ years
            DormantClassification::VeryOld
        } else {
            // 5+ years
            DormantClassification::Dormant
        }
    }

    /// Deterministic severity weighting combining coin age and value in satoshis.
    /// Dust-sized old inputs will not trigger critical severity.
    pub fn determine_severity(
        classification: DormantClassification,
        total_dormant_sats: u64,
    ) -> EventSeverity {
        let dormant_btc = total_dormant_sats as f64 / SATS_PER_BTC as f64;

        match classification {
            DormantClassification::EarlyBitcoin => {
                if dormant_btc >= 50.0 {
                    EventSeverity::Critical
                } else if dormant_btc >= 1.0 {
                    EventSeverity::High
                } else {
                    EventSeverity::Medium
                }
            }
            DormantClassification::Ancient => {
                if dormant_btc >= 100.0 {
                    EventSeverity::Critical
                } else if dormant_btc >= 10.0 {
                    EventSeverity::High
                } else {
                    EventSeverity::Medium
                }
            }
            DormantClassification::VeryOld => {
                if dormant_btc >= 200.0 {
                    EventSeverity::Critical
                } else if dormant_btc >= 50.0 {
                    EventSeverity::High
                } else if dormant_btc >= 5.0 {
                    EventSeverity::Medium
                } else {
                    EventSeverity::Low
                }
            }
            DormantClassification::Dormant => {
                if dormant_btc >= 500.0 {
                    EventSeverity::Critical
                } else if dormant_btc >= 100.0 {
                    EventSeverity::High
                } else if dormant_btc >= 10.0 {
                    EventSeverity::Medium
                } else {
                    EventSeverity::Low
                }
            }
        }
    }
}

impl Default for DormantCoinDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl Detector for DormantCoinDetector {
    fn name(&self) -> &'static str {
        "dormant_coin_detector"
    }

    fn description(&self) -> &'static str {
        "Detects transactions spending Bitcoin UTXOs dormant for 5+ years based on historical confirmation"
    }

    fn detect(&self, observation: &Observation) -> Vec<ChainEvent> {
        let Observation::Transaction(tx) = observation else {
            return Vec::new();
        };

        let mut qualifying_inputs_count = 0u32;
        let mut total_dormant_sats = 0u64;
        let mut oldest_age_seconds = 0u64;
        let mut youngest_qualifying_age_seconds = u64::MAX;
        let mut coin_age_destroyed_sats_days = 0u128;

        let mut oldest_confirmed_at: Option<DateTime<Utc>> = None;
        let mut oldest_confirmed_height: Option<u64> = None;

        for input in &tx.inputs {
            let Some(ref utxo) = input.historical_utxo else {
                continue;
            };

            // Derive age in seconds strictly from historical confirmation timestamp
            let age_secs = if let Some(confirmed_at) = utxo.confirmed_at {
                tx.timestamp
                    .signed_duration_since(confirmed_at)
                    .num_seconds()
                    .max(0) as u64
            } else if let Some(confirmed_height) = utxo.confirmed_height {
                if let Some(tx_height) = tx.block_height {
                    if tx_height > confirmed_height {
                        (tx_height - confirmed_height) * 600
                    } else {
                        0
                    }
                } else {
                    0
                }
            } else {
                0
            };

            let age_days = age_secs / 86400;

            if age_days >= self.min_age_days {
                qualifying_inputs_count += 1;
                total_dormant_sats = total_dormant_sats.saturating_add(utxo.value_sats);

                let cad = calculate_satoshi_days(utxo.value_sats, age_days);
                coin_age_destroyed_sats_days = coin_age_destroyed_sats_days.saturating_add(cad);

                if age_secs > oldest_age_seconds {
                    oldest_age_seconds = age_secs;
                    oldest_confirmed_at = utxo.confirmed_at;
                    oldest_confirmed_height = utxo.confirmed_height;
                }

                if age_secs < youngest_qualifying_age_seconds {
                    youngest_qualifying_age_seconds = age_secs;
                }
            }
        }

        // Must have at least one qualifying dormant input AND total dormant value >= threshold
        if qualifying_inputs_count == 0 || total_dormant_sats < self.min_value_sats {
            return Vec::new();
        }

        let oldest_age_days = oldest_age_seconds / 86400;
        let youngest_qualifying_age_days = youngest_qualifying_age_seconds / 86400;
        let classification = Self::classify(
            oldest_age_days,
            oldest_confirmed_at,
            oldest_confirmed_height,
        );
        let severity = Self::determine_severity(classification, total_dormant_sats);

        let total_dormant_btc = total_dormant_sats as f64 / SATS_PER_BTC as f64;
        let dormant_ratio = if tx.total_input_sats > 0 {
            total_dormant_sats as f64 / tx.total_input_sats as f64
        } else {
            1.0
        };

        let coin_age_destroyed_btc_days = satoshi_days_to_btc_days(coin_age_destroyed_sats_days);
        let coin_age_destroyed_btc_years = coin_age_destroyed_btc_days / 365.25;

        let class_label = match classification {
            DormantClassification::EarlyBitcoin => "Early Bitcoin Era",
            DormantClassification::Ancient => "Ancient (15+ Years)",
            DormantClassification::VeryOld => "Very Old (10+ Years)",
            DormantClassification::Dormant => "Dormant (5+ Years)",
        };

        let title = format!(
            "Dormant Coins Moved: {:.2} BTC ({class_label})",
            total_dormant_btc
        );
        let description = format!(
            "Transaction {} moved {:.4} BTC dormant for {} days (oldest UTXO) across {} input(s). Destroyed {:.0} satoshi-days ({:.2} BTC-years).",
            tx.txid, total_dormant_btc, oldest_age_days, qualifying_inputs_count, coin_age_destroyed_sats_days, coin_age_destroyed_btc_years
        );

        let metadata = DormantCoinsMetadata {
            total_dormant_sats,
            total_dormant_btc,
            dormant_input_count: qualifying_inputs_count,
            total_input_count: tx.input_count as u32,
            oldest_input_age_days: oldest_age_days,
            oldest_input_age_seconds: oldest_age_seconds,
            youngest_qualifying_age_days,
            total_input_sats: tx.total_input_sats,
            total_output_sats: tx.total_output_sats,
            dormant_ratio,
            coin_age_destroyed_sats_days,
            coin_age_destroyed_btc_days,
            coin_age_destroyed_btc_years,
            classification,
        };

        let mut event = ChainEvent::new(
            EventType::DormantCoinsMoved,
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
    use chrono::{Duration, TimeZone};
    use obschain_core::{SpentOutputContext, TransactionObservation, TxInputObservation};

    use super::*;

    fn make_test_tx(
        inputs: Vec<TxInputObservation>,
        total_in: u64,
        total_out: u64,
    ) -> TransactionObservation {
        let input_count = inputs.len();
        TransactionObservation {
            txid: "aabbcc112233".to_string(),
            timestamp: Utc::now(),
            block_hash: None,
            block_height: Some(850000),
            fee_sats: total_in.saturating_sub(total_out),
            size: 250,
            weight: 1000,
            vsize: 250,
            fee_rate_sat_vb: Some(10.0),
            total_input_sats: total_in,
            total_output_sats: total_out,
            input_count,
            output_count: 1,
            inputs,
            outputs: Vec::new(),
            is_rbf: false,
            confirmed: false,
            source: None,
        }
    }

    #[test]
    fn test_recent_utxo_produces_no_event() {
        let detector = DormantCoinDetector::new();
        let now = Utc::now();

        // 30 days old input
        let input = TxInputObservation {
            txid: "prev_tx".to_string(),
            vout: 0,
            sequence: 0xffffffff,
            prev_out_value_sats: Some(10 * SATS_PER_BTC),
            prev_out_address: None,
            is_coinbase: false,
            historical_utxo: Some(SpentOutputContext {
                txid: "prev_tx".to_string(),
                vout: 0,
                value_sats: 10 * SATS_PER_BTC,
                confirmed_height: Some(840000),
                confirmed_at: Some(now - Duration::days(30)),
            }),
        };

        let tx = make_test_tx(vec![input], 10 * SATS_PER_BTC, 10 * SATS_PER_BTC);
        let events = detector.detect(&Observation::Transaction(tx));
        assert!(events.is_empty());
    }

    #[test]
    fn test_missing_historical_context_produces_no_event() {
        let detector = DormantCoinDetector::new();

        let input = TxInputObservation {
            txid: "prev_tx".to_string(),
            vout: 0,
            sequence: 0xffffffff,
            prev_out_value_sats: Some(50 * SATS_PER_BTC),
            prev_out_address: None,
            is_coinbase: false,
            historical_utxo: None, // No enrichment available
        };

        let tx = make_test_tx(vec![input], 50 * SATS_PER_BTC, 50 * SATS_PER_BTC);
        let events = detector.detect(&Observation::Transaction(tx));
        assert!(events.is_empty());
    }

    #[test]
    fn test_five_year_meaningful_utxo_emits_event() {
        let detector = DormantCoinDetector::new();
        let now = Utc::now();

        // 1826 days (~5 years and 1 day), 5 BTC
        let input = TxInputObservation {
            txid: "old_tx".to_string(),
            vout: 0,
            sequence: 0xffffffff,
            prev_out_value_sats: Some(5 * SATS_PER_BTC),
            prev_out_address: None,
            is_coinbase: false,
            historical_utxo: Some(SpentOutputContext {
                txid: "old_tx".to_string(),
                vout: 0,
                value_sats: 5 * SATS_PER_BTC,
                confirmed_height: Some(600000),
                confirmed_at: Some(now - Duration::days(1826)),
            }),
        };

        let tx = make_test_tx(vec![input], 5 * SATS_PER_BTC, 5 * SATS_PER_BTC);
        let events = detector.detect(&Observation::Transaction(tx));
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.event_type, EventType::DormantCoinsMoved);
        assert_eq!(ev.severity, EventSeverity::Low);

        let meta: DormantCoinsMetadata =
            serde_json::from_value(ev.metadata.clone()).expect("valid metadata");
        assert_eq!(meta.classification, DormantClassification::Dormant);
        assert_eq!(meta.oldest_input_age_days, 1826);
        assert_eq!(meta.total_dormant_sats, 5 * SATS_PER_BTC);
        assert!(meta.coin_age_destroyed_sats_days > 0);
    }

    #[test]
    fn test_ten_year_utxo_triggers_very_old_classification() {
        let detector = DormantCoinDetector::new();
        let now = Utc::now();

        // 3700 days (~10.1 years), 60 BTC -> High severity
        let input = TxInputObservation {
            txid: "ancient_tx".to_string(),
            vout: 0,
            sequence: 0xffffffff,
            prev_out_value_sats: Some(60 * SATS_PER_BTC),
            prev_out_address: None,
            is_coinbase: false,
            historical_utxo: Some(SpentOutputContext {
                txid: "ancient_tx".to_string(),
                vout: 0,
                value_sats: 60 * SATS_PER_BTC,
                confirmed_height: Some(300000),
                confirmed_at: Some(now - Duration::days(3700)),
            }),
        };

        let tx = make_test_tx(vec![input], 60 * SATS_PER_BTC, 60 * SATS_PER_BTC);
        let events = detector.detect(&Observation::Transaction(tx));
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.severity, EventSeverity::High);

        let meta: DormantCoinsMetadata =
            serde_json::from_value(ev.metadata.clone()).expect("valid metadata");
        assert_eq!(meta.classification, DormantClassification::VeryOld);
    }

    #[test]
    fn test_very_old_dust_does_not_become_critical() {
        let detector = DormantCoinDetector::with_thresholds(1825, 10_000); // lower value threshold to allow 50_000 sats
        let now = Utc::now();

        // 5500 days (>15 years, Ancient), but only 50,000 sats (0.0005 BTC dust)
        let input = TxInputObservation {
            txid: "dust_tx".to_string(),
            vout: 0,
            sequence: 0xffffffff,
            prev_out_value_sats: Some(50_000),
            prev_out_address: None,
            is_coinbase: false,
            historical_utxo: Some(SpentOutputContext {
                txid: "dust_tx".to_string(),
                vout: 0,
                value_sats: 50_000,
                confirmed_height: Some(50000),
                confirmed_at: Some(now - Duration::days(5500)),
            }),
        };

        let tx = make_test_tx(vec![input], 50_000, 49_000);
        let events = detector.detect(&Observation::Transaction(tx));
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        // Must NOT be Critical because amount is tiny!
        assert_ne!(ev.severity, EventSeverity::Critical);
    }

    #[test]
    fn test_mixed_recent_and_dormant_inputs() {
        let detector = DormantCoinDetector::new();
        let now = Utc::now();

        let recent_input = TxInputObservation {
            txid: "recent_tx".to_string(),
            vout: 0,
            sequence: 0xffffffff,
            prev_out_value_sats: Some(2 * SATS_PER_BTC),
            prev_out_address: None,
            is_coinbase: false,
            historical_utxo: Some(SpentOutputContext {
                txid: "recent_tx".to_string(),
                vout: 0,
                value_sats: 2 * SATS_PER_BTC,
                confirmed_height: Some(800000),
                confirmed_at: Some(now - Duration::days(10)),
            }),
        };

        let dormant_input = TxInputObservation {
            txid: "old_tx".to_string(),
            vout: 1,
            sequence: 0xffffffff,
            prev_out_value_sats: Some(3 * SATS_PER_BTC),
            prev_out_address: None,
            is_coinbase: false,
            historical_utxo: Some(SpentOutputContext {
                txid: "old_tx".to_string(),
                vout: 1,
                value_sats: 3 * SATS_PER_BTC,
                confirmed_height: Some(500000),
                confirmed_at: Some(now - Duration::days(2000)),
            }),
        };

        let tx = make_test_tx(
            vec![recent_input, dormant_input],
            5 * SATS_PER_BTC,
            5 * SATS_PER_BTC,
        );
        let events = detector.detect(&Observation::Transaction(tx));
        assert_eq!(events.len(), 1);

        let meta: DormantCoinsMetadata =
            serde_json::from_value(events[0].metadata.clone()).expect("valid metadata");
        assert_eq!(meta.dormant_input_count, 1);
        assert_eq!(meta.total_input_count, 2);
        assert_eq!(meta.total_dormant_sats, 3 * SATS_PER_BTC);
        assert_eq!(meta.total_input_sats, 5 * SATS_PER_BTC);
        assert!((meta.dormant_ratio - 0.6).abs() < 1e-6);
    }

    #[test]
    fn test_early_bitcoin_era_objective_cutoff() {
        let cutoff_date = Utc.with_ymd_and_hms(2010, 10, 1, 0, 0, 0).unwrap();
        let class = DormantCoinDetector::classify(5500, Some(cutoff_date), Some(80_000));
        assert_eq!(class, DormantClassification::EarlyBitcoin);
    }
}
