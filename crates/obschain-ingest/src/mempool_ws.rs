use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use obschain_core::{BlockObservation, MempoolObservation, Observation, ObservationSource};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use crate::mempool_types::MempoolTx;

#[derive(Debug, Error)]
pub enum MempoolWsError {
    #[error("WebSocket transport error: {0}")]
    Tungstenite(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("JSON parsing error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Channel full or closed: {0}")]
    Channel(String),

    #[error("Connection closed by server")]
    Closed,
}

/// Incoming block structure pushed over mempool.space WebSocket.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MempoolWsBlock {
    pub id: String,
    pub height: u64,
    pub version: Option<u32>,
    pub timestamp: i64,
    pub tx_count: usize,
    pub size: u64,
    #[serde(default)]
    pub weight: u64,
    pub difficulty: Option<f64>,
}

impl MempoolWsBlock {
    pub fn into_observation(self, endpoint: &str) -> BlockObservation {
        let timestamp = DateTime::from_timestamp(self.timestamp, 0).unwrap_or_else(Utc::now);

        BlockObservation {
            block_hash: self.id,
            height: self.height,
            timestamp,
            previous_block_hash: String::new(),
            tx_count: self.tx_count,
            size_bytes: self.size,
            weight: if self.weight > 0 {
                self.weight
            } else {
                self.size * 4
            },
            difficulty: self.difficulty,
            miner_tag: None,
            interval_seconds: None,
            source: Some(ObservationSource::mempool_ws(endpoint)),
        }
    }
}

/// Incoming mempool information over mempool.space WebSocket.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MempoolWsInfo {
    pub size: usize,
    pub bytes: u64,
    #[serde(default)]
    pub total_fee: Option<f64>, // Fee reported in BTC
}

impl MempoolWsInfo {
    pub fn into_observation(self, endpoint: &str) -> MempoolObservation {
        // Safe conversion of external BTC float into integer satoshis with boundary checks
        let total_fee_sats = self
            .total_fee
            .and_then(|btc| obschain_core::safe_btc_f64_to_sats(btc).ok())
            .unwrap_or(0);

        MempoolObservation {
            count: self.size,
            vsize_bytes: self.bytes,
            total_fee_sats,
            min_fee_rate_sat_vb: None,
            timestamp: Utc::now(),
            source: Some(ObservationSource::mempool_ws(endpoint)),
        }
    }
}

/// Ingested transaction replacement / RBF event over WebSocket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MempoolWsRbfTx {
    pub txid: String,
    #[serde(default)]
    pub replaces: Vec<String>,
    #[serde(default)]
    pub replaced_txid: Option<String>,
    #[serde(default)]
    pub fee: Option<u64>,
    #[serde(default)]
    pub old_fee: Option<u64>,
    #[serde(default)]
    pub vsize: Option<u64>,
    #[serde(default)]
    pub old_vsize: Option<u64>,
    #[serde(default)]
    pub value: Option<u64>,
}

impl MempoolWsRbfTx {
    pub fn into_observation(self, endpoint: &str) -> obschain_core::TransactionReplacement {
        let mut replaced = self.replaces;
        if replaced.is_empty() {
            if let Some(r) = self.replaced_txid {
                replaced.push(r);
            }
        }
        let new_fee_sats = self.fee.unwrap_or(0);
        let old_fee_sats = self.old_fee.unwrap_or(0);
        let fee_delta_sats = (new_fee_sats as i64) - (old_fee_sats as i64);

        let old_fee_rate = self
            .old_vsize
            .and_then(|v| obschain_core::calculate_fee_rate_sat_vb(old_fee_sats, v));
        let new_fee_rate = self
            .vsize
            .and_then(|v| obschain_core::calculate_fee_rate_sat_vb(new_fee_sats, v));

        obschain_core::TransactionReplacement {
            replaced_txids: replaced,
            replacement_txid: self.txid,
            old_fee_sats,
            new_fee_sats,
            fee_delta_sats,
            old_vsize: self.old_vsize,
            new_vsize: self.vsize,
            old_fee_rate_sat_vb: old_fee_rate,
            new_fee_rate_sat_vb: new_fee_rate,
            observed_at: Utc::now(),
            source: Some(ObservationSource::mempool_ws(endpoint)),
        }
    }
}

/// Generic message container parsing mempool.space WebSocket events.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MempoolWsEnvelope {
    pub block: Option<MempoolWsBlock>,
    pub blocks: Option<Vec<MempoolWsBlock>>,
    #[serde(rename = "mempoolInfo")]
    pub mempool_info: Option<MempoolWsInfo>,
    pub tx: Option<MempoolTx>,
    pub transactions: Option<Vec<MempoolTx>>,
    #[serde(rename = "rbfTransaction")]
    pub rbf_transaction: Option<MempoolWsRbfTx>,
    #[serde(rename = "rbfTransactions")]
    pub rbf_transactions: Option<Vec<MempoolWsRbfTx>>,
}

/// Parses a raw text WebSocket frame into zero or more normalized observations.
pub fn parse_ws_frame(text: &str, endpoint: &str) -> Result<Vec<Observation>, serde_json::Error> {
    let envelope: MempoolWsEnvelope = serde_json::from_str(text)?;
    let mut observations = Vec::new();

    if let Some(block) = envelope.block {
        observations.push(Observation::Block(block.into_observation(endpoint)));
    }

    if let Some(blocks) = envelope.blocks {
        for b in blocks {
            observations.push(Observation::Block(b.into_observation(endpoint)));
        }
    }

    if let Some(info) = envelope.mempool_info {
        observations.push(Observation::Mempool(info.into_observation(endpoint)));
    }

    if let Some(tx) = envelope.tx {
        observations.push(Observation::Transaction(tx.into_observation(endpoint)));
    }

    if let Some(txs) = envelope.transactions {
        for tx in txs {
            observations.push(Observation::Transaction(tx.into_observation(endpoint)));
        }
    }

    if let Some(rbf) = envelope.rbf_transaction {
        observations.push(Observation::Replacement(rbf.into_observation(endpoint)));
    }

    if let Some(rbfs) = envelope.rbf_transactions {
        for rbf in rbfs {
            observations.push(Observation::Replacement(rbf.into_observation(endpoint)));
        }
    }

    Ok(observations)
}

/// Production-ready asynchronous WebSocket client for mempool.space stream.
pub struct MempoolWebSocketClient {
    endpoint_url: String,
    connected: Arc<AtomicBool>,
}

impl MempoolWebSocketClient {
    pub fn new(endpoint_url: impl Into<String>) -> Self {
        Self {
            endpoint_url: endpoint_url.into(),
            connected: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    pub fn connected_handle(&self) -> Arc<AtomicBool> {
        self.connected.clone()
    }

    /// Spawns the connection supervisor loop, automatically reconnecting with exponential backoff.
    pub async fn run_supervisor(
        &self,
        observation_tx: mpsc::Sender<Observation>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(30);

        while !*shutdown_rx.borrow() {
            info!(endpoint = %self.endpoint_url, "Connecting to mempool.space WebSocket");

            match connect_async(&self.endpoint_url).await {
                Ok((ws_stream, _resp)) => {
                    self.connected.store(true, Ordering::SeqCst);
                    info!(endpoint = %self.endpoint_url, "Connected to mempool.space WebSocket");
                    backoff = Duration::from_secs(1); // Reset backoff on success

                    let (mut write, mut read) = ws_stream.split();

                    // Send init handshake
                    let init_msg = serde_json::json!({ "action": "init" }).to_string();
                    if let Err(e) = write.send(Message::Text(init_msg.into())).await {
                        warn!(error = %e, "Failed to send init handshake to WebSocket");
                        self.connected.store(false, Ordering::SeqCst);
                        continue;
                    }

                    // Subscribe to live blocks, mempool-blocks, stats, rbfTransactions
                    let want_msg = serde_json::json!({
                        "action": "want",
                        "data": ["blocks", "mempool-blocks", "stats", "rbfTransactions"]
                    })
                    .to_string();

                    if let Err(e) = write.send(Message::Text(want_msg.into())).await {
                        warn!(error = %e, "Failed to send want subscription to WebSocket");
                        self.connected.store(false, Ordering::SeqCst);
                        continue;
                    }

                    // Message processing loop
                    loop {
                        tokio::select! {
                            _ = shutdown_rx.changed() => {
                                if *shutdown_rx.borrow() {
                                    info!("Shutdown signaled, closing mempool.space WebSocket");
                                    let _ = write.send(Message::Close(None)).await;
                                    self.connected.store(false, Ordering::SeqCst);
                                    return;
                                }
                            }
                            msg_opt = read.next() => {
                                match msg_opt {
                                    Some(Ok(Message::Text(text))) => {
                                        // Bound message size parsing: protect against oversized frames
                                        if text.len() > 16 * 1024 * 1024 {
                                            warn!("Received oversized WebSocket frame, discarding");
                                            continue;
                                        }

                                        match parse_ws_frame(&text, &self.endpoint_url) {
                                            Ok(observations) => {
                                                for obs in observations {
                                                    if let Err(e) = observation_tx.send(obs).await {
                                                        error!(error = %e, "Observation channel closed or dropped");
                                                        self.connected.store(false, Ordering::SeqCst);
                                                        return;
                                                    }
                                                }
                                            }
                                            Err(err) => {
                                                debug!(error = %err, "Ignored unparsed mempool.space WS message");
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Ping(payload))) => {
                                        let _ = write.send(Message::Pong(payload)).await;
                                    }
                                    Some(Ok(Message::Close(_))) => {
                                        info!("Mempool.space WebSocket connection closed by server");
                                        break;
                                    }
                                    Some(Err(err)) => {
                                        warn!(error = %err, "Mempool.space WebSocket read error");
                                        break;
                                    }
                                    None => {
                                        warn!("Mempool.space WebSocket stream ended");
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }

                    self.connected.store(false, Ordering::SeqCst);
                }
                Err(err) => {
                    self.connected.store(false, Ordering::SeqCst);
                    warn!(
                        error = %err,
                        endpoint = %self.endpoint_url,
                        backoff_secs = backoff.as_secs(),
                        "Failed to connect to mempool.space WebSocket, will retry"
                    );
                }
            }

            // Sleep with backoff before reconnecting, watching for shutdown
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        return;
                    }
                }
                _ = tokio::time::sleep(backoff) => {
                    backoff = (backoff * 2).min(max_backoff);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WS_ENDPOINT: &str = "wss://mempool.space/api/v1/ws";

    #[test]
    fn test_parse_ws_single_block() {
        let payload = r#"{
            "block": {
                "id": "00000000000000000001099645903b6e82810a950bc490d1bfca722a5fbef8f3",
                "height": 885001,
                "version": 536870912,
                "timestamp": 1735000600,
                "tx_count": 2100,
                "size": 1500000,
                "weight": 3980000,
                "difficulty": 105000000000.0
            }
        }"#;

        let obs = parse_ws_frame(payload, WS_ENDPOINT).expect("Should parse single block");
        assert_eq!(obs.len(), 1);
        if let Observation::Block(b) = &obs[0] {
            assert_eq!(b.height, 885001);
            assert_eq!(b.tx_count, 2100);
            assert_eq!(b.weight, 3980000);
            assert_eq!(
                b.source.as_ref().unwrap().endpoint.as_deref(),
                Some(WS_ENDPOINT)
            );
        } else {
            panic!("Expected Observation::Block");
        }
    }

    #[test]
    fn test_parse_ws_blocks_array() {
        let payload = r#"{
            "blocks": [
                {
                    "id": "00000000000000000001",
                    "height": 885000,
                    "timestamp": 1735000000,
                    "tx_count": 1000,
                    "size": 1000000,
                    "weight": 3500000
                },
                {
                    "id": "00000000000000000002",
                    "height": 885001,
                    "timestamp": 1735000600,
                    "tx_count": 1500,
                    "size": 1200000,
                    "weight": 3800000
                }
            ]
        }"#;

        let obs = parse_ws_frame(payload, WS_ENDPOINT).expect("Should parse blocks array");
        assert_eq!(obs.len(), 2);
    }

    #[test]
    fn test_parse_ws_mempool_info_satoshi_safety() {
        let payload = r#"{
            "mempoolInfo": {
                "size": 145000,
                "bytes": 210000000,
                "total_fee": 3.996
            }
        }"#;

        let obs = parse_ws_frame(payload, WS_ENDPOINT).expect("Should parse mempoolInfo");
        assert_eq!(obs.len(), 1);
        if let Observation::Mempool(mp) = &obs[0] {
            assert_eq!(mp.count, 145000);
            assert_eq!(mp.vsize_bytes, 210000000);
            // 3.996 BTC * 100,000,000 sats/BTC = 399,600,000 sats
            assert_eq!(mp.total_fee_sats, 399_600_000);
            assert_eq!(
                mp.source.as_ref().unwrap().endpoint.as_deref(),
                Some(WS_ENDPOINT)
            );
        } else {
            panic!("Expected Observation::Mempool");
        }
    }

    #[test]
    fn test_parse_ws_malformed_json_returns_error() {
        let payload = r#"{"block": { invalid json "#;
        let res = parse_ws_frame(payload, WS_ENDPOINT);
        assert!(res.is_err());
    }

    #[test]
    fn test_parse_ws_unknown_fields_safely_ignored() {
        let payload = r#"{
            "action": "pong",
            "future_field": 12345
        }"#;
        let obs = parse_ws_frame(payload, WS_ENDPOINT).expect("Should ignore unknown fields");
        assert!(obs.is_empty());
    }

    #[test]
    fn test_parse_ws_rbf_replacement_frame() {
        let payload = r#"{
            "rbfTransaction": {
                "txid": "new_tx_replace_9999",
                "replaces": ["old_tx_1111", "old_tx_2222"],
                "fee": 25000,
                "old_fee": 15000,
                "vsize": 150,
                "old_vsize": 200
            }
        }"#;

        let obs = parse_ws_frame(payload, WS_ENDPOINT).expect("Should parse RBF frame");
        assert_eq!(obs.len(), 1);
        if let Observation::Replacement(r) = &obs[0] {
            assert_eq!(r.replacement_txid, "new_tx_replace_9999");
            assert_eq!(r.replaced_txids.len(), 2);
            assert_eq!(r.old_fee_sats, 15000);
            assert_eq!(r.new_fee_sats, 25000);
            assert_eq!(r.fee_delta_sats, 10000);
            assert_eq!(r.new_vsize, Some(150));
            assert_eq!(
                r.source.as_ref().unwrap().endpoint.as_deref(),
                Some(WS_ENDPOINT)
            );
        } else {
            panic!("Expected Observation::Replacement");
        }
    }
}
