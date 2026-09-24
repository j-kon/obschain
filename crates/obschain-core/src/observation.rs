use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::source::ObservationSource;

/// Represents an observed Bitcoin block.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BlockObservation {
    pub block_hash: String,
    pub height: u64,
    pub timestamp: DateTime<Utc>,
    pub previous_block_hash: String,
    pub tx_count: usize,
    pub size_bytes: u64,
    pub weight: u64,
    pub difficulty: Option<f64>,
    pub miner_tag: Option<String>,
    pub interval_seconds: Option<u64>,
    #[serde(default)]
    pub source: Option<ObservationSource>,
}

/// Historical context for a spent transaction output (UTXO).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpentOutputContext {
    pub txid: String,
    pub vout: u32,
    pub value_sats: u64,
    pub confirmed_height: Option<u64>,
    pub confirmed_at: Option<DateTime<Utc>>,
}

/// Represents an input in an observed Bitcoin transaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TxInputObservation {
    pub txid: String,
    pub vout: u32,
    pub sequence: u32,
    pub prev_out_value_sats: Option<u64>,
    pub prev_out_address: Option<String>,
    pub is_coinbase: bool,
    #[serde(default)]
    pub historical_utxo: Option<SpentOutputContext>,
}

/// Represents an output in an observed Bitcoin transaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TxOutputObservation {
    pub value_sats: u64,
    pub n: u32,
    pub script_pubkey_type: Option<String>,
    pub address: Option<String>,
    #[serde(default)]
    pub scriptpubkey_hex: Option<String>,
}

impl TxOutputObservation {
    pub fn is_op_return(&self) -> bool {
        self.script_pubkey_type.as_deref() == Some("op_return")
            || self
                .scriptpubkey_hex
                .as_deref()
                .map(|s| s.starts_with("6a") || s.starts_with("6A"))
                .unwrap_or(false)
    }
}

/// Represents an observed Bitcoin transaction.
/// Strictly uses integer satoshis (u64) for all value representations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransactionObservation {
    pub txid: String,
    pub timestamp: DateTime<Utc>,
    pub block_hash: Option<String>,
    pub block_height: Option<u64>,
    pub fee_sats: u64,
    pub size: u64,
    pub weight: u64,
    pub vsize: u64,
    pub fee_rate_sat_vb: Option<f64>,
    pub total_input_sats: u64,
    pub total_output_sats: u64,
    pub input_count: usize,
    pub output_count: usize,
    pub inputs: Vec<TxInputObservation>,
    pub outputs: Vec<TxOutputObservation>,
    pub is_rbf: bool,
    #[serde(default)]
    pub confirmed: bool,
    #[serde(default)]
    pub source: Option<ObservationSource>,
}

/// Represents an observation of mempool state and congestion.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MempoolObservation {
    pub count: usize,
    pub vsize_bytes: u64,
    pub total_fee_sats: u64,
    pub min_fee_rate_sat_vb: Option<f64>,
    pub timestamp: DateTime<Utc>,
    #[serde(default)]
    pub source: Option<ObservationSource>,
}

/// Represents an observed Bitcoin transaction replacement (e.g. RBF / fee-bump).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransactionReplacement {
    pub replaced_txids: Vec<String>,
    pub replacement_txid: String,
    pub old_fee_sats: u64,
    pub new_fee_sats: u64,
    pub fee_delta_sats: i64,
    pub old_vsize: Option<u64>,
    pub new_vsize: Option<u64>,
    pub old_fee_rate_sat_vb: Option<f64>,
    pub new_fee_rate_sat_vb: Option<f64>,
    pub observed_at: DateTime<Utc>,
    #[serde(default)]
    pub source: Option<ObservationSource>,
}

/// High-level normalized observation payload passed to detectors.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum Observation {
    Block(BlockObservation),
    Transaction(TransactionObservation),
    Mempool(MempoolObservation),
    Replacement(TransactionReplacement),
}

impl Observation {
    pub fn source(&self) -> Option<&ObservationSource> {
        match self {
            Observation::Block(b) => b.source.as_ref(),
            Observation::Transaction(t) => t.source.as_ref(),
            Observation::Mempool(m) => m.source.as_ref(),
            Observation::Replacement(r) => r.source.as_ref(),
        }
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Observation::Block(b) => b.timestamp,
            Observation::Transaction(t) => t.timestamp,
            Observation::Mempool(m) => m.timestamp,
            Observation::Replacement(r) => r.observed_at,
        }
    }
}
