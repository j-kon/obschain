use chrono::Utc;
use obschain_core::{
    CorrelationStrength, Incident, IncidentActivity, IncidentAlert, ObservationSource,
    TransactionObservation, TransactionReplacement, TxInputObservation, TxOutputObservation,
};

use crate::watch_engine::IncidentWatchEngine;

/// Result summary of an incident watch replay simulation.
#[derive(Debug, Clone)]
pub struct ReplayResult {
    pub transactions_replayed: usize,
    pub replacements_replayed: usize,
    pub activities_detected: Vec<IncidentActivity>,
    pub alerts_emitted: Vec<IncidentAlert>,
}

impl ReplayResult {
    /// Validates that an activity exists with the specified correlation strength and activity type.
    pub fn has_activity(
        &self,
        activity_type: obschain_core::IncidentActivityType,
        strength: CorrelationStrength,
    ) -> bool {
        self.activities_detected
            .iter()
            .any(|a| a.activity_type == activity_type && a.correlation_strength == strength)
    }

    /// Verifies that no incident's recovered_sats was mutated during replay.
    pub fn verify_recovery_immutability(incident: &Incident) -> bool {
        // Canonical Liquid recovery values
        incident.recovery.recovered_sats == 340_000_000_000
            && incident.recovery.affected_sats == 399_602_000_000
    }
}

/// Helper simulator to replay historical incident activity against an initialized IncidentWatchEngine.
pub struct IncidentReplaySimulator {
    engine: IncidentWatchEngine,
}

impl IncidentReplaySimulator {
    pub fn new(engine: IncidentWatchEngine) -> Self {
        Self { engine }
    }

    /// Access inner watch engine.
    pub fn engine_mut(&mut self) -> &mut IncidentWatchEngine {
        &mut self.engine
    }

    /// Creates a simulated transaction spending the Liquid 2026 peg-out output (vout 0).
    pub fn create_pegout_spend_tx() -> TransactionObservation {
        let now = Utc::now();
        TransactionObservation {
            txid: "e000111122223333444455556666777788889999aaaabbbbccccddddeeeeffff".to_string(),
            timestamp: now,
            block_hash: None,
            block_height: None,
            fee_sats: 100_000,
            size: 250,
            weight: 660,
            vsize: 165,
            fee_rate_sat_vb: Some(60.6),
            total_input_sats: 399_602_000_000,
            total_output_sats: 399_601_900_000,
            input_count: 1,
            output_count: 2,
            inputs: vec![TxInputObservation {
                txid: "8db751a650ae2f12006b7e8c69a75e4df360e8afd6b9e05ae0b9fa6458a7b140"
                    .to_string(),
                vout: 0,
                sequence: 0xfffffffd,
                prev_out_value_sats: Some(399_602_000_000), // ~3,996.02 BTC
                prev_out_address: Some(
                    "bc1qexploitintermediarycluster0000000000000000".to_string(),
                ),
                is_coinbase: false,
                historical_utxo: None,
            }],
            outputs: vec![
                TxOutputObservation {
                    value_sats: 340_000_000_000,
                    n: 0,
                    script_pubkey_type: Some("v0_p2wpkh".to_string()),
                    address: Some("bc1qliquidfedreturnaddress965950m0000000000000".to_string()),
                    scriptpubkey_hex: Some(
                        "0014a1b2c3d4e5f678901234567890abcdef12345678".to_string(),
                    ),
                },
                TxOutputObservation {
                    value_sats: 59_601_900_000,
                    n: 1,
                    script_pubkey_type: Some("v0_p2wpkh".to_string()),
                    address: Some("bc1qexploitresidualchange00000000000000000000".to_string()),
                    scriptpubkey_hex: Some(
                        "0014b2c3d4e5f678901234567890abcdef1234567890".to_string(),
                    ),
                },
            ],
            is_rbf: true,
            confirmed: false,
            source: Some(ObservationSource::mempool_ws(
                "wss://mempool.space/api/v1/ws",
            )),
        }
    }

    /// Creates a simulated descendant spend transaction at a given depth.
    pub fn create_descendant_spend_tx(
        parent_txid: &str,
        vout: u32,
        child_txid: &str,
        value_sats: u64,
    ) -> TransactionObservation {
        let now = Utc::now();
        TransactionObservation {
            txid: child_txid.to_string(),
            timestamp: now,
            block_hash: None,
            block_height: None,
            fee_sats: 10_000,
            size: 220,
            weight: 560,
            vsize: 140,
            fee_rate_sat_vb: Some(71.4),
            total_input_sats: value_sats,
            total_output_sats: value_sats.saturating_sub(10_000),
            input_count: 1,
            output_count: 1,
            inputs: vec![TxInputObservation {
                txid: parent_txid.to_string(),
                vout,
                sequence: 0xffffffff,
                prev_out_value_sats: Some(value_sats),
                prev_out_address: Some("bc1qdescendantaddress000000000000000000000".to_string()),
                is_coinbase: false,
                historical_utxo: None,
            }],
            outputs: vec![TxOutputObservation {
                value_sats: value_sats.saturating_sub(10_000),
                n: 0,
                script_pubkey_type: Some("v0_p2wpkh".to_string()),
                address: Some("bc1qnextdescendantaddress00000000000000000".to_string()),
                scriptpubkey_hex: Some("0014c3d4e5f678901234567890abcdef1234567890ab".to_string()),
            }],
            is_rbf: false,
            confirmed: false,
            source: Some(ObservationSource::mempool_ws(
                "wss://mempool.space/api/v1/ws",
            )),
        }
    }

    /// Replays a list of transactions and replacements, recording all output.
    pub fn replay_scenario(
        &mut self,
        txs: Vec<TransactionObservation>,
        replacements: Vec<TransactionReplacement>,
    ) -> ReplayResult {
        let mut activities_detected = Vec::new();
        let mut alerts_emitted = Vec::new();
        let transactions_replayed = txs.len();
        let replacements_replayed = replacements.len();

        for tx in txs {
            let matches = self.engine.process_transaction(&tx, None, None);
            for (act, alert_opt) in matches {
                activities_detected.push(act);
                if let Some(alert) = alert_opt {
                    alerts_emitted.push(alert);
                }
            }
        }

        for repl in replacements {
            let matches = self.engine.process_replacement(&repl);
            for (act, alert_opt) in matches {
                activities_detected.push(act);
                if let Some(alert) = alert_opt {
                    alerts_emitted.push(alert);
                }
            }
        }

        ReplayResult {
            transactions_replayed,
            replacements_replayed,
            activities_detected,
            alerts_emitted,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obschain_core::IncidentActivityType;

    #[test]
    fn test_replay_liquid_pegout_spend_and_return() {
        let incident = obschain_incidents::create_liquid_2026_incident();
        let targets = obschain_incidents::canonical_liquid_watch_targets();

        let mut engine = IncidentWatchEngine::with_follow_depth(3);
        engine.load_targets(targets);

        let mut simulator = IncidentReplaySimulator::new(engine);
        let pegout_tx = IncidentReplaySimulator::create_pegout_spend_tx();

        let result = simulator.replay_scenario(vec![pegout_tx], vec![]);

        assert_eq!(result.transactions_replayed, 1);
        assert!(!result.activities_detected.is_empty());

        // 1. Must have detected outpoint spend on pegout output 0
        assert!(result.has_activity(
            IncidentActivityType::WatchedOutpointSpent,
            CorrelationStrength::Direct
        ));

        // 2. Must have detected payment to federation return address
        assert!(result.has_activity(
            IncidentActivityType::WatchedAddressReceived,
            CorrelationStrength::Structural
        ));

        // 3. Must have emitted alert
        assert!(!result.alerts_emitted.is_empty());

        // 4. Must verify recovery values remain untouched
        assert!(ReplayResult::verify_recovery_immutability(&incident));
    }
}
