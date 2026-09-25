use std::{sync::Arc, time::Duration};

use chrono::Utc;
use obschain_core::{
    observation::{
        BlockObservation, TransactionObservation, TxInputObservation, TxOutputObservation,
    },
    source::{ObservationSource, SourceHealthState},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};
use zeromq::{Socket, SocketRecv, SubSocket};

use crate::source::{IngestSource, IngestionSourceType};

/// Maximum accepted ZMQ frame payload size (16 MB) to prevent memory exhaustion attacks.
pub const DEFAULT_MAX_ZMQ_FRAME_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum BitcoinZmqError {
    #[error("Message payload exceeds size limit: {size} bytes > {limit} bytes")]
    PayloadTooLarge { size: usize, limit: usize },

    #[error("Malformed ZMQ message: {0}")]
    MalformedMessage(String),

    #[error("Bitcoin deserialization error: {0}")]
    Deserialization(String),

    #[error("Invalid sequence notification tag: 0x{0:02x}")]
    InvalidSequenceTag(u8),

    #[error("ZeroMQ socket transport error: {0}")]
    Transport(String),
}

/// Bitcoin Core ZMQ topic subscriptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BitcoinZmqTopic {
    RawBlock,
    RawTx,
    Sequence,
}

impl BitcoinZmqTopic {
    pub fn as_topic_str(&self) -> &'static str {
        match self {
            Self::RawBlock => "rawblock",
            Self::RawTx => "rawtx",
            Self::Sequence => "sequence",
        }
    }
}

/// Configuration for Bitcoin Core ZeroMQ subscriber.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitcoinZmqConfig {
    pub rawtx_endpoint: Option<String>,
    pub rawblock_endpoint: Option<String>,
    pub sequence_endpoint: Option<String>,
    pub max_message_size: usize,
    pub initial_reconnect_ms: u64,
    pub max_reconnect_ms: u64,
}

impl Default for BitcoinZmqConfig {
    fn default() -> Self {
        Self {
            rawtx_endpoint: Some("tcp://127.0.0.1:28332".to_string()),
            rawblock_endpoint: Some("tcp://127.0.0.1:28333".to_string()),
            sequence_endpoint: Some("tcp://127.0.0.1:28334".to_string()),
            max_message_size: DEFAULT_MAX_ZMQ_FRAME_BYTES,
            initial_reconnect_ms: 500,
            max_reconnect_ms: 10_000,
        }
    }
}

/// Real-time sequence notification event emitted by Bitcoin Core's `sequence` topic.
/// Contains strictly typed events separating ZMQ notification sequence numbers
/// from mempool sequence numbers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BitcoinSequenceEvent {
    BlockConnected {
        block_hash: String,
        zmq_sequence: u32,
    },
    BlockDisconnected {
        block_hash: String,
        zmq_sequence: u32,
    },
    TransactionAdded {
        txid: String,
        mempool_sequence: u64,
        zmq_sequence: u32,
    },
    TransactionRemoved {
        txid: String,
        mempool_sequence: u64,
        zmq_sequence: u32,
    },
}

pub type ZmqSequenceEvent = BitcoinSequenceEvent;

impl BitcoinSequenceEvent {
    pub fn zmq_sequence(&self) -> u32 {
        match self {
            Self::BlockConnected { zmq_sequence, .. } => *zmq_sequence,
            Self::BlockDisconnected { zmq_sequence, .. } => *zmq_sequence,
            Self::TransactionAdded { zmq_sequence, .. } => *zmq_sequence,
            Self::TransactionRemoved { zmq_sequence, .. } => *zmq_sequence,
        }
    }

    pub fn mempool_sequence(&self) -> Option<u64> {
        match self {
            Self::TransactionAdded {
                mempool_sequence, ..
            } => Some(*mempool_sequence),
            Self::TransactionRemoved {
                mempool_sequence, ..
            } => Some(*mempool_sequence),
            _ => None,
        }
    }

    pub fn hash(&self) -> &str {
        match self {
            Self::BlockConnected { block_hash, .. } => block_hash,
            Self::BlockDisconnected { block_hash, .. } => block_hash,
            Self::TransactionAdded { txid, .. } => txid,
            Self::TransactionRemoved { txid, .. } => txid,
        }
    }
}

/// Ingested and parsed message emitted by Bitcoin Core ZeroMQ.
#[derive(Debug, Clone)]
pub enum BitcoinZmqMessage {
    RawBlock {
        block: BlockObservation,
        transactions: Vec<TransactionObservation>,
        zmq_sequence: u32,
    },
    RawTx {
        transaction: TransactionObservation,
        zmq_sequence: u32,
    },
    Sequence(BitcoinSequenceEvent),
    SequenceGap {
        topic: BitcoinZmqTopic,
        previous: u32,
        current: u32,
        missed: u32,
    },
}

/// Status of the individual ZMQ topic endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitcoinZmqEndpointsStatus {
    pub rawtx: SourceHealthState,
    pub rawblock: SourceHealthState,
    pub sequence: SourceHealthState,
}

// ---------------------------------------------------------------------------
// Pure Parsing Helpers & Sequence Tracker
// ---------------------------------------------------------------------------

/// Tracks per-topic 4-byte notification sequence numbers published by Bitcoin Core
/// to detect potential notification loss and gap conditions.
#[derive(Debug, Clone, Default)]
pub struct ZmqSequenceTracker {
    last_seq: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceCheckResult {
    /// First sequence observed for this connection.
    Initial(u32),
    /// Consecutive sequence: current == previous.wrapping_add(1).
    Consecutive(u32),
    /// Duplicate notification: current == previous.
    Duplicate(u32),
    /// Notification gap detected: missed >= 1 notifications.
    Gap {
        previous: u32,
        current: u32,
        missed: u32,
    },
    /// Stale or backward jump: e.g. reconnect to restarted node.
    Stale { previous: u32, current: u32 },
}

impl ZmqSequenceTracker {
    pub fn new() -> Self {
        Self { last_seq: None }
    }

    pub fn last_seq(&self) -> Option<u32> {
        self.last_seq
    }

    pub fn reset(&mut self) {
        self.last_seq = None;
    }

    pub fn observe(&mut self, current: u32) -> SequenceCheckResult {
        let prev = match self.last_seq {
            None => {
                self.last_seq = Some(current);
                return SequenceCheckResult::Initial(current);
            }
            Some(p) => p,
        };

        let diff = current.wrapping_sub(prev);

        if diff == 0 {
            SequenceCheckResult::Duplicate(current)
        } else if diff == 1 {
            self.last_seq = Some(current);
            SequenceCheckResult::Consecutive(current)
        } else if diff < 0x8000_0000 {
            let missed = diff - 1;
            self.last_seq = Some(current);
            SequenceCheckResult::Gap {
                previous: prev,
                current,
                missed,
            }
        } else {
            SequenceCheckResult::Stale {
                previous: prev,
                current,
            }
        }
    }
}

/// Formats a 32-byte hash buffer received from Bitcoin Core ZeroMQ.
/// In Bitcoin Core's ZMQ implementation, hashes are already published in reversed
/// byte order (matching RPC and block explorer display hex), so bytes 0..32 are
/// formatted directly as hex without double-reversing.
pub fn format_zmq_hash(bytes: &[u8; 32]) -> String {
    bytes
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join("")
}

#[inline]
pub fn format_hash_display(bytes: &[u8; 32]) -> String {
    format_zmq_hash(bytes)
}

/// Validates that an incoming ZMQ multipart message has exactly 3 frames:
/// frame 1: topic
/// frame 2: body
/// frame 3: 4-byte LE sequence number
pub fn validate_and_extract_multipart(
    frames: &[bytes::Bytes],
    expected_topic: BitcoinZmqTopic,
) -> Result<(&[u8], u32), BitcoinZmqError> {
    if frames.len() != 3 {
        return Err(BitcoinZmqError::MalformedMessage(format!(
            "Expected exactly 3 multipart frames (topic, body, sequence), got {}",
            frames.len()
        )));
    }

    let topic_str = std::str::from_utf8(&frames[0])
        .map_err(|e| BitcoinZmqError::MalformedMessage(format!("Invalid topic UTF-8: {e}")))?;
    if topic_str != expected_topic.as_topic_str() {
        return Err(BitcoinZmqError::MalformedMessage(format!(
            "Unexpected topic '{}', expected '{}'",
            topic_str,
            expected_topic.as_topic_str()
        )));
    }

    if frames[2].len() != 4 {
        return Err(BitcoinZmqError::MalformedMessage(format!(
            "ZMQ sequence frame must be exactly 4 bytes, got {}",
            frames[2].len()
        )));
    }

    let mut seq_bytes = [0u8; 4];
    seq_bytes.copy_from_slice(&frames[2]);
    let zmq_sequence = u32::from_le_bytes(seq_bytes);

    Ok((&frames[1], zmq_sequence))
}

/// Parses raw transaction bytes received from ZMQ `rawtx`.
pub fn parse_raw_tx(
    payload: &[u8],
    source_endpoint: Option<&str>,
    max_size: usize,
) -> Result<TransactionObservation, BitcoinZmqError> {
    if payload.len() > max_size {
        return Err(BitcoinZmqError::PayloadTooLarge {
            size: payload.len(),
            limit: max_size,
        });
    }

    let tx: bitcoin::Transaction = bitcoin::consensus::deserialize(payload)
        .map_err(|e| BitcoinZmqError::Deserialization(e.to_string()))?;

    let txid = tx.compute_txid().to_string();
    let weight = tx.weight().to_wu();
    let vsize = tx.vsize() as u64;
    let size = payload.len() as u64;

    let is_rbf = tx
        .input
        .iter()
        .any(|i| i.sequence.to_consensus_u32() < 0xFFFF_FFFE);

    let inputs: Vec<TxInputObservation> = tx
        .input
        .iter()
        .map(|inp| {
            let is_coinbase = inp.previous_output.is_null();
            TxInputObservation {
                txid: inp.previous_output.txid.to_string(),
                vout: inp.previous_output.vout,
                sequence: inp.sequence.to_consensus_u32(),
                prev_out_value_sats: None,
                prev_out_address: None,
                is_coinbase,
                historical_utxo: None,
            }
        })
        .collect();

    let mut total_output_sats = 0u64;
    let outputs: Vec<TxOutputObservation> = tx
        .output
        .iter()
        .enumerate()
        .map(|(idx, out)| {
            let val = out.value.to_sat();
            total_output_sats = total_output_sats.saturating_add(val);
            let script_hex = out
                .script_pubkey
                .to_bytes()
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>();

            let script_type = if out.script_pubkey.is_op_return() {
                Some("op_return".to_string())
            } else if out.script_pubkey.is_p2pkh() {
                Some("p2pkh".to_string())
            } else if out.script_pubkey.is_p2sh() {
                Some("p2sh".to_string())
            } else if out.script_pubkey.is_p2wpkh() {
                Some("v0_p2wpkh".to_string())
            } else if out.script_pubkey.is_p2wsh() {
                Some("v0_p2wsh".to_string())
            } else if out.script_pubkey.is_p2tr() {
                Some("v1_p2tr".to_string())
            } else {
                None
            };

            let address =
                bitcoin::Address::from_script(&out.script_pubkey, bitcoin::Network::Bitcoin)
                    .ok()
                    .map(|a| a.to_string());

            TxOutputObservation {
                value_sats: val,
                n: idx as u32,
                script_pubkey_type: script_type,
                address,
                scriptpubkey_hex: Some(script_hex),
            }
        })
        .collect();

    Ok(TransactionObservation {
        txid,
        timestamp: Utc::now(),
        block_hash: None,
        block_height: None,
        fee_sats: 0,
        size,
        weight,
        vsize,
        fee_rate_sat_vb: None,
        total_input_sats: 0,
        total_output_sats,
        input_count: inputs.len(),
        output_count: outputs.len(),
        inputs,
        outputs,
        is_rbf,
        confirmed: false,
        source: Some(ObservationSource::bitcoin_core_zmq(
            source_endpoint.unwrap_or("zmq:rawtx"),
        )),
    })
}

/// Parses raw block bytes received from ZMQ `rawblock`.
pub fn parse_raw_block(
    payload: &[u8],
    source_endpoint: Option<&str>,
    max_size: usize,
) -> Result<(BlockObservation, Vec<TransactionObservation>), BitcoinZmqError> {
    if payload.len() > max_size {
        return Err(BitcoinZmqError::PayloadTooLarge {
            size: payload.len(),
            limit: max_size,
        });
    }

    let block: bitcoin::Block = bitcoin::consensus::deserialize(payload)
        .map_err(|e| BitcoinZmqError::Deserialization(e.to_string()))?;

    let block_hash = block.block_hash().to_string();
    let prev_block_hash = block.header.prev_blockhash.to_string();
    let timestamp = chrono::DateTime::<Utc>::from_timestamp(block.header.time as i64, 0)
        .unwrap_or_else(Utc::now);

    let height = block.bip34_block_height().unwrap_or(0);
    let size_bytes = payload.len() as u64;
    let weight = block.weight().to_wu();
    let tx_count = block.txdata.len();

    let block_obs = BlockObservation {
        block_hash: block_hash.clone(),
        height,
        timestamp,
        previous_block_hash: prev_block_hash,
        tx_count,
        size_bytes,
        weight,
        difficulty: Some(block.header.difficulty_float()),
        miner_tag: None,
        interval_seconds: None,
        source: Some(ObservationSource::bitcoin_core_zmq(
            source_endpoint.unwrap_or("zmq:rawblock"),
        )),
    };

    let mut tx_observations = Vec::with_capacity(block.txdata.len());
    for tx in &block.txdata {
        let txid = tx.compute_txid().to_string();
        let tx_weight = tx.weight().to_wu();
        let tx_vsize = tx.vsize() as u64;
        let is_rbf = tx
            .input
            .iter()
            .any(|i| i.sequence.to_consensus_u32() < 0xFFFF_FFFE);

        let inputs: Vec<TxInputObservation> = tx
            .input
            .iter()
            .map(|inp| {
                let is_coinbase = inp.previous_output.is_null();
                TxInputObservation {
                    txid: inp.previous_output.txid.to_string(),
                    vout: inp.previous_output.vout,
                    sequence: inp.sequence.to_consensus_u32(),
                    prev_out_value_sats: None,
                    prev_out_address: None,
                    is_coinbase,
                    historical_utxo: None,
                }
            })
            .collect();

        let mut total_output_sats = 0u64;
        let outputs: Vec<TxOutputObservation> = tx
            .output
            .iter()
            .enumerate()
            .map(|(idx, out)| {
                let val = out.value.to_sat();
                total_output_sats = total_output_sats.saturating_add(val);
                let script_hex = out
                    .script_pubkey
                    .to_bytes()
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect::<String>();

                let script_type = if out.script_pubkey.is_op_return() {
                    Some("op_return".to_string())
                } else if out.script_pubkey.is_p2pkh() {
                    Some("p2pkh".to_string())
                } else if out.script_pubkey.is_p2sh() {
                    Some("p2sh".to_string())
                } else if out.script_pubkey.is_p2wpkh() {
                    Some("v0_p2wpkh".to_string())
                } else if out.script_pubkey.is_p2wsh() {
                    Some("v0_p2wsh".to_string())
                } else if out.script_pubkey.is_p2tr() {
                    Some("v1_p2tr".to_string())
                } else {
                    None
                };

                let address =
                    bitcoin::Address::from_script(&out.script_pubkey, bitcoin::Network::Bitcoin)
                        .ok()
                        .map(|a| a.to_string());

                TxOutputObservation {
                    value_sats: val,
                    n: idx as u32,
                    script_pubkey_type: script_type,
                    address,
                    scriptpubkey_hex: Some(script_hex),
                }
            })
            .collect();

        tx_observations.push(TransactionObservation {
            txid,
            timestamp,
            block_hash: Some(block_hash.clone()),
            block_height: Some(height),
            fee_sats: 0,
            size: tx.total_size() as u64,
            weight: tx_weight,
            vsize: tx_vsize,
            fee_rate_sat_vb: None,
            total_input_sats: 0,
            total_output_sats,
            input_count: inputs.len(),
            output_count: outputs.len(),
            inputs,
            outputs,
            is_rbf,
            confirmed: true,
            source: Some(ObservationSource::bitcoin_core_zmq(
                source_endpoint.unwrap_or("zmq:rawblock"),
            )),
        });
    }

    Ok((block_obs, tx_observations))
}

/// Parses the payload of Bitcoin Core's `sequence` ZMQ notification.
///
/// Specification (Bitcoin Core v31.1 `doc/zmq.md`):
/// - 'C' (Block connected): 32-byte hash (display order) + 'C' (1 byte) = 33 bytes total.
/// - 'D' (Block disconnected): 32-byte hash (display order) + 'D' (1 byte) = 33 bytes total.
/// - 'A' (Tx added to mempool): 32-byte hash (display order) + 'A' (1 byte) + 8-byte LE mempool sequence = 41 bytes total.
/// - 'R' (Tx removed from mempool): 32-byte hash (display order) + 'R' (1 byte) + 8-byte LE mempool sequence = 41 bytes total.
pub fn parse_sequence_event(
    payload: &[u8],
    zmq_sequence: u32,
) -> Result<BitcoinSequenceEvent, BitcoinZmqError> {
    if payload.len() < 33 {
        return Err(BitcoinZmqError::MalformedMessage(format!(
            "Sequence payload too short: {} bytes < 33",
            payload.len()
        )));
    }

    let tag = payload[32];
    let mut hash_bytes = [0u8; 32];
    hash_bytes.copy_from_slice(&payload[0..32]);
    let hash_display = format_zmq_hash(&hash_bytes);

    match tag {
        b'C' => {
            if payload.len() != 33 {
                return Err(BitcoinZmqError::MalformedMessage(format!(
                    "Block connected 'C' payload must be exactly 33 bytes (32 hash + 1 tag), got {}",
                    payload.len()
                )));
            }
            Ok(BitcoinSequenceEvent::BlockConnected {
                block_hash: hash_display,
                zmq_sequence,
            })
        }
        b'D' => {
            if payload.len() != 33 {
                return Err(BitcoinZmqError::MalformedMessage(format!(
                    "Block disconnected 'D' payload must be exactly 33 bytes (32 hash + 1 tag), got {}",
                    payload.len()
                )));
            }
            Ok(BitcoinSequenceEvent::BlockDisconnected {
                block_hash: hash_display,
                zmq_sequence,
            })
        }
        b'A' => {
            if payload.len() != 41 {
                return Err(BitcoinZmqError::MalformedMessage(format!(
                    "Transaction added 'A' payload must be exactly 41 bytes (32 hash + 1 tag + 8 mempool seq), got {}",
                    payload.len()
                )));
            }
            let mut s_bytes = [0u8; 8];
            s_bytes.copy_from_slice(&payload[33..41]);
            let mempool_sequence = u64::from_le_bytes(s_bytes);
            Ok(BitcoinSequenceEvent::TransactionAdded {
                txid: hash_display,
                mempool_sequence,
                zmq_sequence,
            })
        }
        b'R' => {
            if payload.len() != 41 {
                return Err(BitcoinZmqError::MalformedMessage(format!(
                    "Transaction removed 'R' payload must be exactly 41 bytes (32 hash + 1 tag + 8 mempool seq), got {}",
                    payload.len()
                )));
            }
            let mut s_bytes = [0u8; 8];
            s_bytes.copy_from_slice(&payload[33..41]);
            let mempool_sequence = u64::from_le_bytes(s_bytes);
            Ok(BitcoinSequenceEvent::TransactionRemoved {
                txid: hash_display,
                mempool_sequence,
                zmq_sequence,
            })
        }
        other => Err(BitcoinZmqError::InvalidSequenceTag(other)),
    }
}

// ---------------------------------------------------------------------------
// ZeroMQ Subscriber Implementation
// ---------------------------------------------------------------------------

/// Manages connections to Bitcoin Core ZeroMQ endpoints.
pub struct BitcoinZmqSubscriber {
    config: BitcoinZmqConfig,
    status: Arc<RwLock<BitcoinZmqEndpointsStatus>>,
}

impl BitcoinZmqSubscriber {
    pub fn new(config: BitcoinZmqConfig) -> Self {
        let status = Arc::new(RwLock::new(BitcoinZmqEndpointsStatus {
            rawtx: if config.rawtx_endpoint.is_some() {
                SourceHealthState::Disconnected
            } else {
                SourceHealthState::NotConfigured
            },
            rawblock: if config.rawblock_endpoint.is_some() {
                SourceHealthState::Disconnected
            } else {
                SourceHealthState::NotConfigured
            },
            sequence: if config.sequence_endpoint.is_some() {
                SourceHealthState::Disconnected
            } else {
                SourceHealthState::NotConfigured
            },
        }));

        Self { config, status }
    }

    pub fn config(&self) -> &BitcoinZmqConfig {
        &self.config
    }

    pub async fn get_status(&self) -> BitcoinZmqEndpointsStatus {
        self.status.read().await.clone()
    }

    /// Spawns the ingestion loops for each configured topic endpoint.
    pub fn start(
        self: Arc<Self>,
        sender: mpsc::Sender<BitcoinZmqMessage>,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        let mut handles = Vec::new();

        if let Some(endpoint) = &self.config.rawtx_endpoint {
            let s = Arc::clone(&self);
            let tx = sender.clone();
            let ep = endpoint.clone();
            handles.push(tokio::spawn(async move {
                s.run_topic_loop(BitcoinZmqTopic::RawTx, &ep, tx).await;
            }));
        }

        if let Some(endpoint) = &self.config.rawblock_endpoint {
            let s = Arc::clone(&self);
            let tx = sender.clone();
            let ep = endpoint.clone();
            handles.push(tokio::spawn(async move {
                s.run_topic_loop(BitcoinZmqTopic::RawBlock, &ep, tx).await;
            }));
        }

        if let Some(endpoint) = &self.config.sequence_endpoint {
            let s = Arc::clone(&self);
            let tx = sender.clone();
            let ep = endpoint.clone();
            handles.push(tokio::spawn(async move {
                s.run_topic_loop(BitcoinZmqTopic::Sequence, &ep, tx).await;
            }));
        }

        handles
    }

    pub async fn set_topic_status(&self, topic: BitcoinZmqTopic, state: SourceHealthState) {
        let mut w = self.status.write().await;
        match topic {
            BitcoinZmqTopic::RawTx => w.rawtx = state,
            BitcoinZmqTopic::RawBlock => w.rawblock = state,
            BitcoinZmqTopic::Sequence => w.sequence = state,
        }
    }

    async fn run_topic_loop(
        &self,
        topic: BitcoinZmqTopic,
        endpoint: &str,
        sender: mpsc::Sender<BitcoinZmqMessage>,
    ) {
        let mut backoff = Duration::from_millis(self.config.initial_reconnect_ms);
        let max_backoff = Duration::from_millis(self.config.max_reconnect_ms);

        loop {
            self.set_topic_status(topic, SourceHealthState::Connecting)
                .await;
            info!(
                topic = topic.as_topic_str(),
                endpoint = endpoint,
                "Connecting to Bitcoin Core ZeroMQ endpoint"
            );

            let mut socket = SubSocket::new();
            match socket.connect(endpoint).await {
                Ok(_) => {
                    if let Err(e) = socket.subscribe(topic.as_topic_str()).await {
                        warn!(
                            topic = topic.as_topic_str(),
                            error = %e,
                            "Failed to subscribe to ZMQ topic; retrying"
                        );
                        self.set_topic_status(topic, SourceHealthState::Disconnected)
                            .await;
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(max_backoff);
                        continue;
                    }

                    info!(
                        topic = topic.as_topic_str(),
                        endpoint = endpoint,
                        "Successfully connected and subscribed to Bitcoin Core ZeroMQ"
                    );
                    self.set_topic_status(topic, SourceHealthState::Connected)
                        .await;
                    backoff = Duration::from_millis(self.config.initial_reconnect_ms);

                    let mut seq_tracker = ZmqSequenceTracker::new();

                    // Message receive loop
                    loop {
                        match socket.recv().await {
                            Ok(msg) => {
                                let frames = msg.into_vec();
                                let (payload, zmq_seq) =
                                    match validate_and_extract_multipart(&frames, topic) {
                                        Ok(res) => res,
                                        Err(e) => {
                                            warn!(
                                                topic = topic.as_topic_str(),
                                                error = %e,
                                                "Rejected malformed ZMQ multipart message"
                                            );
                                            continue;
                                        }
                                    };

                                match seq_tracker.observe(zmq_seq) {
                                    SequenceCheckResult::Gap {
                                        previous,
                                        current,
                                        missed,
                                    } => {
                                        warn!(
                                            topic = topic.as_topic_str(),
                                            previous,
                                            current,
                                            missed,
                                            "ZMQ notification sequence gap detected: {missed} notification(s) potentially lost"
                                        );
                                        self.set_topic_status(topic, SourceHealthState::Degraded)
                                            .await;
                                        if sender
                                            .send(BitcoinZmqMessage::SequenceGap {
                                                topic,
                                                previous,
                                                current,
                                                missed,
                                            })
                                            .await
                                            .is_err()
                                        {
                                            warn!("Bitcoin ZMQ receiver channel closed");
                                            return;
                                        }
                                    }
                                    SequenceCheckResult::Duplicate(seq) => {
                                        tracing::debug!(
                                            topic = topic.as_topic_str(),
                                            seq,
                                            "Duplicate ZMQ notification sequence"
                                        );
                                    }
                                    SequenceCheckResult::Stale { previous, current } => {
                                        tracing::debug!(
                                            topic = topic.as_topic_str(),
                                            previous,
                                            current,
                                            "Stale or backward ZMQ notification sequence"
                                        );
                                    }
                                    SequenceCheckResult::Initial(_)
                                    | SequenceCheckResult::Consecutive(_) => {}
                                }

                                match topic {
                                    BitcoinZmqTopic::RawTx => {
                                        match parse_raw_tx(
                                            payload,
                                            Some(endpoint),
                                            self.config.max_message_size,
                                        ) {
                                            Ok(tx_obs) => {
                                                if sender
                                                    .send(BitcoinZmqMessage::RawTx {
                                                        transaction: tx_obs,
                                                        zmq_sequence: zmq_seq,
                                                    })
                                                    .await
                                                    .is_err()
                                                {
                                                    warn!("Bitcoin ZMQ receiver channel closed");
                                                    return;
                                                }
                                            }
                                            Err(e) => {
                                                warn!(
                                                    error = %e,
                                                    "Failed to parse rawtx ZMQ message"
                                                );
                                            }
                                        }
                                    }
                                    BitcoinZmqTopic::RawBlock => {
                                        match parse_raw_block(
                                            payload,
                                            Some(endpoint),
                                            self.config.max_message_size,
                                        ) {
                                            Ok((block_obs, txs)) => {
                                                if sender
                                                    .send(BitcoinZmqMessage::RawBlock {
                                                        block: block_obs,
                                                        transactions: txs,
                                                        zmq_sequence: zmq_seq,
                                                    })
                                                    .await
                                                    .is_err()
                                                {
                                                    warn!("Bitcoin ZMQ receiver channel closed");
                                                    return;
                                                }
                                            }
                                            Err(e) => {
                                                warn!(
                                                    error = %e,
                                                    "Failed to parse rawblock ZMQ message"
                                                );
                                            }
                                        }
                                    }
                                    BitcoinZmqTopic::Sequence => {
                                        match parse_sequence_event(payload, zmq_seq) {
                                            Ok(seq_event) => {
                                                if sender
                                                    .send(BitcoinZmqMessage::Sequence(seq_event))
                                                    .await
                                                    .is_err()
                                                {
                                                    warn!("Bitcoin ZMQ receiver channel closed");
                                                    return;
                                                }
                                            }
                                            Err(e) => {
                                                warn!(
                                                    error = %e,
                                                    "Failed to parse sequence ZMQ message"
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    topic = topic.as_topic_str(),
                                    error = %e,
                                    "ZMQ socket recv error; connection interrupted"
                                );
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        topic = topic.as_topic_str(),
                        endpoint = endpoint,
                        error = %e,
                        "Failed to connect to Bitcoin Core ZeroMQ; will retry"
                    );
                }
            }

            self.set_topic_status(topic, SourceHealthState::Disconnected)
                .await;
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(max_backoff);
        }
    }
}

impl IngestSource for BitcoinZmqSubscriber {
    fn source_type(&self) -> IngestionSourceType {
        IngestionSourceType::BitcoinCoreZmq
    }

    fn name(&self) -> &'static str {
        "bitcoin_core_zmq"
    }

    fn is_healthy(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_hash_byte_order_known_hash() {
        // Bitcoin Core Genesis Block Hash:
        // 000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f
        let raw_hex = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f";
        let mut raw = [0u8; 32];
        for (i, chunk) in raw_hex.as_bytes().chunks(2).enumerate() {
            let hex_str = std::str::from_utf8(chunk).unwrap();
            raw[i] = u8::from_str_radix(hex_str, 16).unwrap();
        }

        // Must preserve display hex without double-reversing
        let display = format_zmq_hash(&raw);
        assert_eq!(display, raw_hex);
    }

    #[test]
    fn test_sequence_c_body_33_bytes() {
        let mut payload = Vec::new();
        // 32-byte hash
        payload.extend_from_slice(&[0xaa; 32]);
        // tag 'C'
        payload.push(b'C');
        assert_eq!(payload.len(), 33);

        let event = parse_sequence_event(&payload, 10).expect("parsed sequence C");
        match event {
            BitcoinSequenceEvent::BlockConnected {
                block_hash,
                zmq_sequence,
            } => {
                assert_eq!(
                    block_hash,
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                );
                assert_eq!(zmq_sequence, 10);
            }
            _ => panic!("Expected BlockConnected"),
        }
    }

    #[test]
    fn test_sequence_d_body_33_bytes() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&[0xbb; 32]);
        payload.push(b'D');
        assert_eq!(payload.len(), 33);

        let event = parse_sequence_event(&payload, 11).expect("parsed sequence D");
        match event {
            BitcoinSequenceEvent::BlockDisconnected {
                block_hash,
                zmq_sequence,
            } => {
                assert_eq!(
                    block_hash,
                    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                );
                assert_eq!(zmq_sequence, 11);
            }
            _ => panic!("Expected BlockDisconnected"),
        }
    }

    #[test]
    fn test_sequence_a_body_41_bytes() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&[0x11; 32]);
        payload.push(b'A');
        let mempool_seq: u64 = 42;
        payload.extend_from_slice(&mempool_seq.to_le_bytes());
        assert_eq!(payload.len(), 41);

        let event = parse_sequence_event(&payload, 12).expect("parsed sequence A");
        match event {
            BitcoinSequenceEvent::TransactionAdded {
                txid,
                mempool_sequence,
                zmq_sequence,
            } => {
                assert_eq!(
                    txid,
                    "1111111111111111111111111111111111111111111111111111111111111111"
                );
                assert_eq!(mempool_sequence, 42);
                assert_eq!(zmq_sequence, 12);
            }
            _ => panic!("Expected TransactionAdded"),
        }
    }

    #[test]
    fn test_sequence_r_body_41_bytes() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&[0x22; 32]);
        payload.push(b'R');
        let mempool_seq: u64 = 43;
        payload.extend_from_slice(&mempool_seq.to_le_bytes());
        assert_eq!(payload.len(), 41);

        let event = parse_sequence_event(&payload, 13).expect("parsed sequence R");
        match event {
            BitcoinSequenceEvent::TransactionRemoved {
                txid,
                mempool_sequence,
                zmq_sequence,
            } => {
                assert_eq!(
                    txid,
                    "2222222222222222222222222222222222222222222222222222222222222222"
                );
                assert_eq!(mempool_sequence, 43);
                assert_eq!(zmq_sequence, 13);
            }
            _ => panic!("Expected TransactionRemoved"),
        }
    }

    #[test]
    fn test_sequence_malformed_32_byte_body() {
        let payload = vec![0u8; 32];
        let err = parse_sequence_event(&payload, 1).unwrap_err();
        assert!(matches!(err, BitcoinZmqError::MalformedMessage(_)));
    }

    #[test]
    fn test_sequence_malformed_34_byte_body() {
        let mut payload = vec![0u8; 33];
        payload[32] = b'C';
        payload.push(0x01); // 34 bytes
        let err = parse_sequence_event(&payload, 1).unwrap_err();
        assert!(matches!(err, BitcoinZmqError::MalformedMessage(_)));
    }

    #[test]
    fn test_sequence_malformed_ar_without_mempool_sequence() {
        // A without 8-byte LE sequence (only 33 bytes)
        let mut payload = vec![0u8; 33];
        payload[32] = b'A';
        let err = parse_sequence_event(&payload, 1).unwrap_err();
        assert!(matches!(err, BitcoinZmqError::MalformedMessage(_)));
    }

    #[test]
    fn test_sequence_invalid_tag() {
        let mut payload = vec![0u8; 33];
        payload[32] = b'Z'; // Invalid tag
        let err = parse_sequence_event(&payload, 1).unwrap_err();
        assert!(matches!(err, BitcoinZmqError::InvalidSequenceTag(b'Z')));
    }

    #[test]
    fn test_tracker_sequence_gap_detection() {
        let mut tracker = ZmqSequenceTracker::new();
        assert_eq!(tracker.observe(100), SequenceCheckResult::Initial(100));
        assert_eq!(tracker.observe(101), SequenceCheckResult::Consecutive(101));
        assert_eq!(tracker.observe(101), SequenceCheckResult::Duplicate(101));
        // Gap: 101 -> 105 (missed 3)
        assert_eq!(
            tracker.observe(105),
            SequenceCheckResult::Gap {
                previous: 101,
                current: 105,
                missed: 3,
            }
        );
    }

    #[test]
    fn test_tracker_u32_wraparound() {
        let mut tracker = ZmqSequenceTracker::new();
        assert_eq!(
            tracker.observe(u32::MAX),
            SequenceCheckResult::Initial(u32::MAX)
        );
        // u32::MAX -> 0 is consecutive
        assert_eq!(tracker.observe(0), SequenceCheckResult::Consecutive(0));
        // 0 -> 4 has gap of 3
        assert_eq!(
            tracker.observe(4),
            SequenceCheckResult::Gap {
                previous: 0,
                current: 4,
                missed: 3,
            }
        );

        // Wraparound with gap: u32::MAX - 2 -> 1 (diff = 4, missed = 3)
        let mut tracker2 = ZmqSequenceTracker::new();
        tracker2.observe(u32::MAX - 2);
        assert_eq!(
            tracker2.observe(1),
            SequenceCheckResult::Gap {
                previous: u32::MAX - 2,
                current: 1,
                missed: 3,
            }
        );
    }

    #[test]
    fn test_multipart_validation() {
        let topic = Bytes::from_static(b"rawtx");
        let body = Bytes::from_static(b"sample_body");
        let seq = Bytes::copy_from_slice(&100u32.to_le_bytes());

        // Valid 3-frame message
        let valid_frames = vec![topic.clone(), body.clone(), seq.clone()];
        let (extracted_body, extracted_seq) =
            validate_and_extract_multipart(&valid_frames, BitcoinZmqTopic::RawTx)
                .expect("valid multipart");
        assert_eq!(extracted_body, b"sample_body");
        assert_eq!(extracted_seq, 100);

        // Missing sequence frame (only 2 frames)
        let two_frames = vec![topic.clone(), body.clone()];
        assert!(matches!(
            validate_and_extract_multipart(&two_frames, BitcoinZmqTopic::RawTx),
            Err(BitcoinZmqError::MalformedMessage(_))
        ));

        // Extra unexpected frame (4 frames)
        let four_frames = vec![
            topic.clone(),
            body.clone(),
            seq.clone(),
            Bytes::from_static(b"extra"),
        ];
        assert!(matches!(
            validate_and_extract_multipart(&four_frames, BitcoinZmqTopic::RawTx),
            Err(BitcoinZmqError::MalformedMessage(_))
        ));

        // Invalid sequence length (3 bytes instead of 4)
        let bad_seq_frames = vec![topic.clone(), body.clone(), Bytes::from_static(&[1, 2, 3])];
        assert!(matches!(
            validate_and_extract_multipart(&bad_seq_frames, BitcoinZmqTopic::RawTx),
            Err(BitcoinZmqError::MalformedMessage(_))
        ));

        // Topic mismatch
        assert!(matches!(
            validate_and_extract_multipart(&valid_frames, BitcoinZmqTopic::RawBlock),
            Err(BitcoinZmqError::MalformedMessage(_))
        ));
    }

    #[test]
    fn test_oversized_payload_rejected() {
        let big = vec![0u8; 1024];
        let err = parse_raw_tx(&big, None, 500).unwrap_err();
        assert!(matches!(err, BitcoinZmqError::PayloadTooLarge { .. }));
    }

    #[test]
    fn test_malformed_raw_tx_rejected() {
        let garbage = vec![0xde, 0xad, 0xbe, 0xef];
        let err = parse_raw_tx(&garbage, None, 1024).unwrap_err();
        assert!(matches!(err, BitcoinZmqError::Deserialization(_)));
    }

    #[test]
    fn test_valid_raw_tx_parse() {
        let tx = bitcoin::Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::TxOut {
                value: bitcoin::Amount::from_sat(50_000_000),
                script_pubkey: bitcoin::ScriptBuf::new(),
            }],
        };

        let raw_bytes = bitcoin::consensus::serialize(&tx);
        let obs = parse_raw_tx(&raw_bytes, Some("tcp://127.0.0.1:28332"), 1024 * 1024)
            .expect("parse valid raw tx");

        assert_eq!(obs.total_output_sats, 50_000_000);
        assert_eq!(obs.input_count, 1);
        assert_eq!(obs.output_count, 1);
        assert!(obs.is_rbf);
        assert!(!obs.confirmed);
        assert!(obs.source.is_some());
    }

    #[test]
    fn test_valid_raw_block_parse() {
        use bitcoin::hashes::Hash;

        let tx = bitcoin::Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::TxOut {
                value: bitcoin::Amount::from_sat(50_000_000),
                script_pubkey: bitcoin::ScriptBuf::new(),
            }],
        };

        let block = bitcoin::Block {
            header: bitcoin::block::Header {
                version: bitcoin::block::Version::ONE,
                prev_blockhash: bitcoin::BlockHash::all_zeros(),
                merkle_root: bitcoin::TxMerkleNode::all_zeros(),
                time: 1700000000,
                bits: bitcoin::CompactTarget::from_consensus(0x1d00ffff),
                nonce: 0,
            },
            txdata: vec![tx],
        };

        let raw_bytes = bitcoin::consensus::serialize(&block);
        let (block_obs, tx_obs) =
            parse_raw_block(&raw_bytes, Some("tcp://127.0.0.1:28333"), 16 * 1024 * 1024)
                .expect("parse valid raw block");

        assert_eq!(block_obs.tx_count, 1);
        assert_eq!(tx_obs.len(), 1);
        assert!(tx_obs[0].confirmed);
    }

    #[test]
    fn test_malformed_raw_block_rejected() {
        let garbage = vec![0xca, 0xfe, 0xba, 0xbe];
        let err = parse_raw_block(&garbage, None, 1024).unwrap_err();
        assert!(matches!(err, BitcoinZmqError::Deserialization(_)));
    }

    #[test]
    fn test_oversized_block_rejected() {
        let big = vec![0u8; 2048];
        let err = parse_raw_block(&big, None, 1024).unwrap_err();
        assert!(matches!(err, BitcoinZmqError::PayloadTooLarge { .. }));
    }
}
