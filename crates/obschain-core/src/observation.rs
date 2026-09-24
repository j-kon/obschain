use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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
}

/// Represents an output in an observed Bitcoin transaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TxOutputObservation {
    pub value_sats: u64,
    pub n: u32,
    pub script_pubkey_type: Option<String>,
    pub address: Option<String>,
}

/// Represents an observed Bitcoin transaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransactionObservation {
    pub txid: String,
    pub timestamp: DateTime<Utc>,
    pub block_hash: Option<String>,
    pub block_height: Option<u64>,
    pub fee_sats: u64,
    pub vsize: u64,
    pub fee_rate_sat_vb: f64,
    pub total_input_sats: u64,
    pub total_output_sats: u64,
    pub input_count: usize,
    pub output_count: usize,
    pub inputs: Vec<TxInputObservation>,
    pub outputs: Vec<TxOutputObservation>,
    pub is_rbf: bool,
}

/// High-level observation payload passed to detectors.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum Observation {
    Block(BlockObservation),
    Transaction(TransactionObservation),
}
