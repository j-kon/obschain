use chrono::{DateTime, Utc};
use obschain_core::{
    calculate_fee_rate_sat_vb, calculate_vsize_from_weight, BlockObservation, MempoolObservation,
    ObservationSource, TransactionObservation, TxInputObservation, TxOutputObservation,
};
use serde::{Deserialize, Serialize};

/// Mempool.space block summary representation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MempoolBlock {
    pub id: String,
    pub height: u64,
    pub version: u32,
    pub timestamp: i64,
    pub tx_count: usize,
    pub size: u64,
    pub weight: u64,
    pub merkle_root: Option<String>,
    pub previousblockhash: Option<String>,
    pub mediantime: Option<i64>,
    pub nonce: Option<u64>,
    pub bits: Option<u32>,
    pub difficulty: Option<f64>,
}

impl MempoolBlock {
    pub fn into_observation(self, endpoint: &str) -> BlockObservation {
        let timestamp = DateTime::from_timestamp(self.timestamp, 0).unwrap_or_else(Utc::now);

        BlockObservation {
            block_hash: self.id,
            height: self.height,
            timestamp,
            previous_block_hash: self.previousblockhash.unwrap_or_default(),
            tx_count: self.tx_count,
            size_bytes: self.size,
            weight: self.weight,
            difficulty: self.difficulty,
            miner_tag: None,
            interval_seconds: None,
            source: Some(ObservationSource::mempool_rest(endpoint)),
        }
    }
}

/// Status of a Bitcoin transaction on mempool.space.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MempoolTxStatus {
    pub confirmed: bool,
    pub block_height: Option<u64>,
    pub block_hash: Option<String>,
    pub block_time: Option<i64>,
}

/// Previous output spent by a transaction input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MempoolTxPrevout {
    pub scriptpubkey: Option<String>,
    pub scriptpubkey_asm: Option<String>,
    pub scriptpubkey_type: Option<String>,
    pub scriptpubkey_address: Option<String>,
    #[serde(default)]
    pub value: u64,
}

/// Transaction input in a mempool.space transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MempoolTxVin {
    pub txid: String,
    pub vout: u32,
    pub prevout: Option<MempoolTxPrevout>,
    pub scriptsig: Option<String>,
    pub scriptsig_asm: Option<String>,
    pub witness: Option<Vec<String>>,
    #[serde(default)]
    pub is_coinbase: bool,
    pub sequence: u32,
}

/// Transaction output in a mempool.space transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MempoolTxVout {
    pub scriptpubkey: Option<String>,
    pub scriptpubkey_asm: Option<String>,
    pub scriptpubkey_type: Option<String>,
    pub scriptpubkey_address: Option<String>,
    pub value: u64,
}

/// Full Bitcoin transaction object returned by mempool.space.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MempoolTx {
    pub txid: String,
    #[serde(default)]
    pub version: i32,
    #[serde(default)]
    pub locktime: u32,
    #[serde(default)]
    pub vin: Vec<MempoolTxVin>,
    #[serde(default)]
    pub vout: Vec<MempoolTxVout>,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub weight: u64,
    pub fee: Option<u64>,
    #[serde(default)]
    pub status: MempoolTxStatus,
}

impl MempoolTx {
    pub fn into_observation(self, endpoint: &str) -> TransactionObservation {
        let timestamp = self
            .status
            .block_time
            .and_then(|t| DateTime::from_timestamp(t, 0))
            .unwrap_or_else(Utc::now);

        let fee_sats = self.fee.unwrap_or(0);
        let vsize = calculate_vsize_from_weight(self.weight);
        let fee_rate_sat_vb = calculate_fee_rate_sat_vb(fee_sats, vsize);

        let mut total_input_sats: u64 = 0;
        let inputs: Vec<TxInputObservation> = self
            .vin
            .into_iter()
            .map(|vin| {
                let prev_val = vin.prevout.as_ref().map(|p| p.value);
                if let Some(val) = prev_val {
                    total_input_sats = total_input_sats.saturating_add(val);
                }
                TxInputObservation {
                    txid: vin.txid,
                    vout: vin.vout,
                    sequence: vin.sequence,
                    prev_out_value_sats: prev_val,
                    prev_out_address: vin.prevout.and_then(|p| p.scriptpubkey_address),
                    is_coinbase: vin.is_coinbase,
                    historical_utxo: None,
                }
            })
            .collect();

        let mut total_output_sats: u64 = 0;
        let outputs: Vec<TxOutputObservation> = self
            .vout
            .into_iter()
            .enumerate()
            .map(|(n, vout)| {
                total_output_sats = total_output_sats.saturating_add(vout.value);
                TxOutputObservation {
                    value_sats: vout.value,
                    n: n as u32,
                    script_pubkey_type: vout.scriptpubkey_type,
                    address: vout.scriptpubkey_address,
                    scriptpubkey_hex: vout.scriptpubkey,
                }
            })
            .collect();

        // BIP-125 opt-in RBF signaling check
        let is_rbf = inputs
            .iter()
            .any(|inp: &TxInputObservation| inp.sequence < 0xFFFF_FFFE);

        TransactionObservation {
            txid: self.txid,
            timestamp,
            block_hash: self.status.block_hash,
            block_height: self.status.block_height,
            fee_sats,
            size: self.size,
            weight: self.weight,
            vsize,
            fee_rate_sat_vb,
            total_input_sats,
            total_output_sats,
            input_count: inputs.len(),
            output_count: outputs.len(),
            inputs,
            outputs,
            is_rbf,
            confirmed: self.status.confirmed,
            source: Some(ObservationSource::mempool_rest(endpoint)),
        }
    }
}

/// Mempool backlog summary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MempoolStats {
    pub count: usize,
    pub vsize: u64,
    pub total_fee: u64,
}

impl MempoolStats {
    pub fn into_observation(self, endpoint: &str) -> MempoolObservation {
        MempoolObservation {
            count: self.count,
            vsize_bytes: self.vsize,
            total_fee_sats: self.total_fee,
            min_fee_rate_sat_vb: None,
            timestamp: Utc::now(),
            source: Some(ObservationSource::mempool_rest(endpoint)),
        }
    }
}

/// Brief recent transaction record in mempool.space `/mempool/recent`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MempoolRecentTx {
    pub txid: String,
    pub fee: u64,
    pub vsize: u64,
    pub value: u64,
}

impl MempoolRecentTx {
    pub fn into_observation(self, endpoint: &str) -> TransactionObservation {
        let fee_rate_sat_vb = calculate_fee_rate_sat_vb(self.fee, self.vsize);
        TransactionObservation {
            txid: self.txid,
            timestamp: Utc::now(),
            block_hash: None,
            block_height: None,
            fee_sats: self.fee,
            size: self.vsize,
            weight: self.vsize.saturating_mul(4),
            vsize: self.vsize,
            fee_rate_sat_vb,
            total_input_sats: self.value.saturating_add(self.fee),
            total_output_sats: self.value,
            input_count: 1,
            output_count: 1,
            inputs: Vec::new(),
            outputs: Vec::new(),
            is_rbf: false,
            confirmed: false,
            source: Some(ObservationSource::mempool_rest(endpoint)),
        }
    }
}

/// Recommended fee estimates from `/v1/fees/recommended`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MempoolRecommendedFees {
    pub fastest_fee: u32,
    pub half_hour_fee: u32,
    pub hour_fee: u32,
    pub minimum_fee: u32,
    pub economy_fee: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mempool_block_conversion() {
        let json_str = r#"{
            "id": "00000000000000000002a0a4c0eb8379ef54580bf3ec457388cf0c6e8e89f929",
            "height": 885000,
            "version": 654321,
            "timestamp": 1735000000,
            "tx_count": 2845,
            "size": 1645000,
            "weight": 3992000,
            "difficulty": 105123456789.0,
            "previousblockhash": "00000000000000000001099645903b6e82810a950bc490d1bfca722a5fbef8f3"
        }"#;

        let block: MempoolBlock = serde_json::from_str(json_str).expect("Valid JSON block");
        let obs = block.into_observation("https://mempool.space/api");

        assert_eq!(obs.height, 885000);
        assert_eq!(obs.tx_count, 2845);
        assert_eq!(obs.weight, 3992000);
        assert_eq!(obs.size_bytes, 1645000);
        assert_eq!(obs.timestamp.timestamp(), 1735000000);
        assert_eq!(
            obs.source.unwrap().endpoint.as_deref(),
            Some("https://mempool.space/api")
        );
    }

    #[test]
    fn test_mempool_tx_normalization_and_satoshi_safety() {
        let json_str = r#"{
            "txid": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "version": 2,
            "locktime": 0,
            "vin": [
                {
                    "txid": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "vout": 0,
                    "prevout": {
                        "scriptpubkey": "0014...",
                        "scriptpubkey_type": "v0_p2wpkh",
                        "scriptpubkey_address": "bc1qtest1",
                        "value": 500000000
                    },
                    "sequence": 4294967293
                }
            ],
            "vout": [
                {
                    "scriptpubkey": "0014...",
                    "scriptpubkey_type": "v0_p2wpkh",
                    "scriptpubkey_address": "bc1qtest2",
                    "value": 499990000
                }
            ],
            "size": 220,
            "weight": 560,
            "fee": 10000,
            "status": {
                "confirmed": false
            }
        }"#;

        let tx: MempoolTx = serde_json::from_str(json_str).expect("Valid JSON tx");
        let obs = tx.into_observation("https://mempool.space/api");

        assert_eq!(
            obs.txid,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(obs.fee_sats, 10000);
        assert_eq!(obs.vsize, 140); // 560 / 4 = 140 vB
        assert_eq!(obs.total_input_sats, 500000000);
        assert_eq!(obs.total_output_sats, 499990000);
        assert_eq!(obs.input_count, 1);
        assert_eq!(obs.output_count, 1);
        assert!(obs.is_rbf); // sequence < 0xFFFFFFFE signals RBF
        assert!(!obs.confirmed);
        // fee rate: 10000 sats / 140 vB = 71.428...
        let fee_rate = obs.fee_rate_sat_vb.unwrap();
        assert!((fee_rate - 71.428).abs() < 0.01);
    }

    #[test]
    fn test_mempool_stats_normalization() {
        let json_str = r#"{
            "count": 154200,
            "vsize": 185000000,
            "total_fee": 1250000000
        }"#;

        let stats: MempoolStats = serde_json::from_str(json_str).expect("Valid JSON stats");
        let obs = stats.into_observation("https://mempool.space/api");

        assert_eq!(obs.count, 154200);
        assert_eq!(obs.vsize_bytes, 185000000);
        assert_eq!(obs.total_fee_sats, 1250000000);
    }

    #[test]
    fn test_mempool_recent_tx_normalization() {
        let recent = MempoolRecentTx {
            txid: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
            fee: 5000,
            vsize: 250,
            value: 2000000000,
        };

        let obs = recent.into_observation("https://mempool.space/api");
        assert_eq!(obs.fee_sats, 5000);
        assert_eq!(obs.vsize, 250);
        assert_eq!(obs.total_output_sats, 2000000000);
        assert_eq!(obs.total_input_sats, 2000005000);
        assert_eq!(obs.fee_rate_sat_vb, Some(20.0));
    }
}
