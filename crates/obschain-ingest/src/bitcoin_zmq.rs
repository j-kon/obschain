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

/// Real-time sequence event emitted by Bitcoin Core's `sequence` topic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ZmqSequenceEvent {
    BlockConnected {
        block_hash: String,
        height: Option<u64>,
    },
    BlockDisconnected {
        block_hash: String,
        height: Option<u64>,
    },
    TxAddedToMempool {
        txid: String,
        mempool_seq: u64,
    },
    TxRemovedFromMempool {
        txid: String,
        mempool_seq: u64,
    },
}

/// Ingested and parsed message emitted by Bitcoin Core ZeroMQ.
#[derive(Debug, Clone)]
pub enum BitcoinZmqMessage {
    RawBlock {
        block: BlockObservation,
        transactions: Vec<TransactionObservation>,
    },
    RawTx(TransactionObservation),
    Sequence(ZmqSequenceEvent),
}

/// Status of the individual ZMQ topic endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitcoinZmqEndpointsStatus {
    pub rawtx: SourceHealthState,
    pub rawblock: SourceHealthState,
    pub sequence: SourceHealthState,
}

// ---------------------------------------------------------------------------
// Pure Parsing Helpers
// ---------------------------------------------------------------------------

/// Reverses a 32-byte hash buffer to format standard Bitcoin display hex (big-endian display).
pub fn format_hash_display(bytes: &[u8; 32]) -> String {
    let mut reversed = *bytes;
    reversed.reverse();
    reversed
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join("")
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
/// Format:
/// - 32 bytes: hash (block hash or txid in wire endianness)
/// - 1 byte: ASCII character tag
///   - 'C': Block connected. Followed by optional 8-byte LE height.
///   - 'D': Block disconnected. Followed by optional 8-byte LE height.
///   - 'A': Tx added to mempool. Followed by 8-byte LE mempool sequence.
///   - 'R': Tx removed from mempool. Followed by 8-byte LE mempool sequence.
pub fn parse_sequence_event(payload: &[u8]) -> Result<ZmqSequenceEvent, BitcoinZmqError> {
    if payload.len() < 33 {
        return Err(BitcoinZmqError::MalformedMessage(format!(
            "Sequence payload too short: {} bytes < 33",
            payload.len()
        )));
    }

    let mut hash_bytes = [0u8; 32];
    hash_bytes.copy_from_slice(&payload[0..32]);
    let hash_display = format_hash_display(&hash_bytes);

    let tag = payload[32];

    match tag {
        b'C' => {
            let height = if payload.len() >= 41 {
                let mut h_bytes = [0u8; 8];
                h_bytes.copy_from_slice(&payload[33..41]);
                Some(u64::from_le_bytes(h_bytes))
            } else {
                None
            };
            Ok(ZmqSequenceEvent::BlockConnected {
                block_hash: hash_display,
                height,
            })
        }
        b'D' => {
            let height = if payload.len() >= 41 {
                let mut h_bytes = [0u8; 8];
                h_bytes.copy_from_slice(&payload[33..41]);
                Some(u64::from_le_bytes(h_bytes))
            } else {
                None
            };
            Ok(ZmqSequenceEvent::BlockDisconnected {
                block_hash: hash_display,
                height,
            })
        }
        b'A' => {
            let mempool_seq = if payload.len() >= 41 {
                let mut s_bytes = [0u8; 8];
                s_bytes.copy_from_slice(&payload[33..41]);
                u64::from_le_bytes(s_bytes)
            } else {
                0
            };
            Ok(ZmqSequenceEvent::TxAddedToMempool {
                txid: hash_display,
                mempool_seq,
            })
        }
        b'R' => {
            let mempool_seq = if payload.len() >= 41 {
                let mut s_bytes = [0u8; 8];
                s_bytes.copy_from_slice(&payload[33..41]);
                u64::from_le_bytes(s_bytes)
            } else {
                0
            };
            Ok(ZmqSequenceEvent::TxRemovedFromMempool {
                txid: hash_display,
                mempool_seq,
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

    async fn set_topic_status(&self, topic: BitcoinZmqTopic, state: SourceHealthState) {
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

                    // Message receive loop
                    loop {
                        match socket.recv().await {
                            Ok(msg) => {
                                let frames = msg.into_vec();
                                if frames.len() < 2 {
                                    continue;
                                }

                                let payload = &frames[1];

                                match topic {
                                    BitcoinZmqTopic::RawTx => {
                                        match parse_raw_tx(
                                            payload,
                                            Some(endpoint),
                                            self.config.max_message_size,
                                        ) {
                                            Ok(tx_obs) => {
                                                if sender
                                                    .send(BitcoinZmqMessage::RawTx(tx_obs))
                                                    .await
                                                    .is_err()
                                                {
                                                    warn!("Bitcoin ZMQ receiver channel closed");
                                                    return;
                                                }
                                            }
                                            Err(e) => {
                                                warn!(error = %e, "Failed to parse rawtx ZMQ message");
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
                                                    })
                                                    .await
                                                    .is_err()
                                                {
                                                    warn!("Bitcoin ZMQ receiver channel closed");
                                                    return;
                                                }
                                            }
                                            Err(e) => {
                                                warn!(error = %e, "Failed to parse rawblock ZMQ message");
                                            }
                                        }
                                    }
                                    BitcoinZmqTopic::Sequence => {
                                        match parse_sequence_event(payload) {
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
                                                warn!(error = %e, "Failed to parse sequence ZMQ message");
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

    #[test]
    fn test_format_hash_display() {
        let mut raw = [0u8; 32];
        raw[0] = 0x12;
        raw[31] = 0xab;

        let display = format_hash_display(&raw);
        assert!(display.starts_with("ab"));
        assert!(display.ends_with("12"));
    }

    #[test]
    fn test_sequence_block_connected_parsing() {
        let mut payload = Vec::new();
        // 32-byte hash
        payload.extend_from_slice(&[0xaa; 32]);
        // tag 'C'
        payload.push(b'C');
        // 8-byte LE height = 800000
        let height: u64 = 800_000;
        payload.extend_from_slice(&height.to_le_bytes());

        let event = parse_sequence_event(&payload).expect("parsed sequence");
        match event {
            ZmqSequenceEvent::BlockConnected { block_hash, height } => {
                assert_eq!(height, Some(800_000));
                assert_eq!(
                    block_hash,
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                );
            }
            _ => panic!("Expected BlockConnected"),
        }
    }

    #[test]
    fn test_sequence_block_disconnected_parsing() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&[0xbb; 32]);
        payload.push(b'D');
        let height: u64 = 799_999;
        payload.extend_from_slice(&height.to_le_bytes());

        let event = parse_sequence_event(&payload).expect("parsed sequence");
        match event {
            ZmqSequenceEvent::BlockDisconnected { block_hash, height } => {
                assert_eq!(height, Some(799_999));
                assert_eq!(
                    block_hash,
                    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                );
            }
            _ => panic!("Expected BlockDisconnected"),
        }
    }

    #[test]
    fn test_sequence_mempool_add_and_remove_parsing() {
        let mut add_payload = Vec::new();
        add_payload.extend_from_slice(&[0x11; 32]);
        add_payload.push(b'A');
        let seq: u64 = 42;
        add_payload.extend_from_slice(&seq.to_le_bytes());

        let add_event = parse_sequence_event(&add_payload).expect("parsed add");
        assert!(matches!(
            add_event,
            ZmqSequenceEvent::TxAddedToMempool {
                mempool_seq: 42,
                ..
            }
        ));

        let mut rem_payload = Vec::new();
        rem_payload.extend_from_slice(&[0x22; 32]);
        rem_payload.push(b'R');
        let seq: u64 = 43;
        rem_payload.extend_from_slice(&seq.to_le_bytes());

        let rem_event = parse_sequence_event(&rem_payload).expect("parsed rem");
        assert!(matches!(
            rem_event,
            ZmqSequenceEvent::TxRemovedFromMempool {
                mempool_seq: 43,
                ..
            }
        ));
    }

    #[test]
    fn test_sequence_invalid_tag() {
        let mut payload = vec![0u8; 32];
        payload.push(b'Z'); // Invalid tag
        let err = parse_sequence_event(&payload).unwrap_err();
        assert!(matches!(err, BitcoinZmqError::InvalidSequenceTag(b'Z')));
    }

    #[test]
    fn test_sequence_too_short() {
        let payload = vec![0u8; 10];
        let err = parse_sequence_event(&payload).unwrap_err();
        assert!(matches!(err, BitcoinZmqError::MalformedMessage(_)));
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
