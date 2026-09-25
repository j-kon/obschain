use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use bitcoin::hex::FromHex;
use chrono::Utc;
use obschain_core::{
    observation::{BlockObservation, Observation, ReorgObservation, TransactionObservation},
    source::{ObservationSource, SourceHealthState},
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch, RwLock};
use tracing::{debug, info, warn};

use crate::{
    bitcoin_rpc::{BitcoinCoreRpcClient, BitcoinNodeCapabilities, BitcoinRpcError},
    bitcoin_zmq::{
        parse_raw_block, BitcoinZmqEndpointsStatus, BitcoinZmqMessage, BitcoinZmqSubscriber,
        ZmqSequenceEvent,
    },
};

/// Configuration for the Bitcoin Core coordinator and reconciler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitcoinCoordinatorConfig {
    pub reconcile_max_blocks: u64,
    pub health_poll_interval_secs: u64,
}

impl Default for BitcoinCoordinatorConfig {
    fn default() -> Self {
        Self {
            reconcile_max_blocks: 100,
            health_poll_interval_secs: 10,
        }
    }
}

/// Operational telemetry status for Bitcoin Core.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitcoinCoordinatorStatus {
    pub enabled: bool,
    pub rpc_health: SourceHealthState,
    pub capabilities: Option<BitcoinNodeCapabilities>,
    pub zmq_status: BitcoinZmqEndpointsStatus,
}

/// Ingest coordinator orchestrating Bitcoin Core RPC, ZMQ streams, tip tracking,
/// reorg detection, and bounded gap reconciliation.
pub struct BitcoinCoordinator {
    rpc: Arc<BitcoinCoreRpcClient>,
    zmq: Arc<BitcoinZmqSubscriber>,
    config: BitcoinCoordinatorConfig,
    last_tip_hash: Arc<RwLock<Option<String>>>,
    last_tip_height: Arc<AtomicU64>,
    status: Arc<RwLock<BitcoinCoordinatorStatus>>,
    pub reorgs_detected: Arc<AtomicU64>,
    pub gap_blocks_reconciled: Arc<AtomicU64>,
    pub zmq_transactions_received: Arc<AtomicU64>,
    pub zmq_blocks_received: Arc<AtomicU64>,
    pub rpc_requests_total: Arc<AtomicU64>,
    pub rpc_errors_total: Arc<AtomicU64>,
}

impl BitcoinCoordinator {
    pub fn new(
        rpc: Arc<BitcoinCoreRpcClient>,
        zmq: Arc<BitcoinZmqSubscriber>,
        config: BitcoinCoordinatorConfig,
    ) -> Self {
        Self {
            rpc,
            zmq,
            config,
            last_tip_hash: Arc::new(RwLock::new(None)),
            last_tip_height: Arc::new(AtomicU64::new(0)),
            status: Arc::new(RwLock::new(BitcoinCoordinatorStatus {
                enabled: true,
                rpc_health: SourceHealthState::Connecting,
                capabilities: None,
                zmq_status: BitcoinZmqEndpointsStatus {
                    rawtx: SourceHealthState::Connecting,
                    rawblock: SourceHealthState::Connecting,
                    sequence: SourceHealthState::Connecting,
                },
            })),
            reorgs_detected: Arc::new(AtomicU64::new(0)),
            gap_blocks_reconciled: Arc::new(AtomicU64::new(0)),
            zmq_transactions_received: Arc::new(AtomicU64::new(0)),
            zmq_blocks_received: Arc::new(AtomicU64::new(0)),
            rpc_requests_total: Arc::new(AtomicU64::new(0)),
            rpc_errors_total: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn last_tip_height(&self) -> u64 {
        self.last_tip_height.load(Ordering::Relaxed)
    }

    pub async fn set_tip(&self, height: u64, hash: String) {
        self.last_tip_height.store(height, Ordering::Relaxed);
        *self.last_tip_hash.write().await = Some(hash);
    }

    pub async fn get_status(&self) -> BitcoinCoordinatorStatus {
        let mut st = self.status.read().await.clone();
        st.zmq_status = self.zmq.get_status().await;
        st
    }

    /// Initializes and verifies Bitcoin Core node capabilities, network match, and IBD status.
    pub async fn initialize(&self) -> Result<BitcoinNodeCapabilities, BitcoinRpcError> {
        self.rpc_requests_total.fetch_add(1, Ordering::Relaxed);
        let caps = match self.rpc.inspect_capabilities().await {
            Ok(c) => c,
            Err(e) => {
                self.rpc_errors_total.fetch_add(1, Ordering::Relaxed);
                let mut w = self.status.write().await;
                w.rpc_health = SourceHealthState::Disconnected;
                return Err(e);
            }
        };

        let rpc_health = if caps.initial_block_download {
            SourceHealthState::Syncing
        } else {
            SourceHealthState::Connected
        };

        {
            let mut w = self.status.write().await;
            w.rpc_health = rpc_health;
            w.capabilities = Some(caps.clone());
        }

        // Establish initial tip baseline
        self.last_tip_height.store(caps.blocks, Ordering::Relaxed);
        if let Ok(hash) = self.rpc.get_block_hash(caps.blocks).await {
            let mut tip = self.last_tip_hash.write().await;
            *tip = Some(hash);
        }

        info!(
            network = %caps.network,
            blocks = caps.blocks,
            headers = caps.headers,
            ibd = caps.initial_block_download,
            verification_progress = caps.verification_progress,
            pruned = caps.pruned,
            txindex = caps.txindex_available,
            "Sovereign Bitcoin Core node validated and connected"
        );

        Ok(caps)
    }

    /// Starts all asynchronous ingestion pipelines: ZMQ message streaming and RPC tip reconciliation.
    pub fn start(
        self: Arc<Self>,
        obs_sender: mpsc::Sender<Observation>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        let (zmq_tx, mut zmq_rx) = mpsc::channel::<BitcoinZmqMessage>(2048);

        // 1. Start ZMQ subscriber tasks
        let mut handles = self.zmq.clone().start(zmq_tx);

        // 2. ZMQ message processing loop
        let coord = Arc::clone(&self);
        let obs_tx = obs_sender.clone();
        let mut zmq_shutdown = shutdown_rx.clone();

        handles.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = zmq_shutdown.changed() => {
                        if *zmq_shutdown.borrow() {
                            break;
                        }
                    }
                    msg_opt = zmq_rx.recv() => {
                        let Some(msg) = msg_opt else {
                            break;
                        };

                        match msg {
                            BitcoinZmqMessage::RawTx(tx) => {
                                coord.zmq_transactions_received.fetch_add(1, Ordering::Relaxed);
                                if obs_tx.send(Observation::Transaction(tx)).await.is_err() {
                                    break;
                                }
                            }
                            BitcoinZmqMessage::RawBlock { block, transactions } => {
                                coord.zmq_blocks_received.fetch_add(1, Ordering::Relaxed);
                                coord.process_zmq_block(block, transactions, &obs_tx).await;
                            }
                            BitcoinZmqMessage::Sequence(seq) => {
                                match seq {
                                    ZmqSequenceEvent::BlockConnected { block_hash, height } => {
                                        debug!(hash = %block_hash, height = ?height, "ZMQ sequence: block connected");
                                    }
                                    ZmqSequenceEvent::BlockDisconnected { block_hash, height } => {
                                        info!(hash = %block_hash, height = ?height, "ZMQ sequence: block disconnected (reorg triggered)");
                                        coord.investigate_chain_tips(&obs_tx).await;
                                    }
                                    ZmqSequenceEvent::TxAddedToMempool { txid, mempool_seq } => {
                                        debug!(txid = %txid, seq = mempool_seq, "ZMQ sequence: tx added to mempool");
                                    }
                                    ZmqSequenceEvent::TxRemovedFromMempool { txid, mempool_seq } => {
                                        debug!(txid = %txid, seq = mempool_seq, "ZMQ sequence: tx removed from mempool");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }));

        // 3. Periodic RPC health check and tip gap reconciler
        let coord_poll = Arc::clone(&self);
        let obs_tx_poll = obs_sender.clone();

        handles.push(tokio::spawn(async move {
            let interval = Duration::from_secs(coord_poll.config.health_poll_interval_secs);
            while !*shutdown_rx.borrow() {
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    _ = tokio::time::sleep(interval) => {
                        coord_poll.reconcile_tip_and_gap(&obs_tx_poll).await;
                    }
                }
            }
        }));

        handles
    }

    /// Processes an incoming raw block from ZMQ, verifying chain continuity and detecting reorgs.
    async fn process_zmq_block(
        &self,
        block: BlockObservation,
        transactions: Vec<TransactionObservation>,
        obs_tx: &mpsc::Sender<Observation>,
    ) {
        let block_hash = block.block_hash.clone();
        let block_height = block.height;
        let prev_hash = block.previous_block_hash.clone();

        let known_tip = self.last_tip_hash.read().await.clone();

        if let Some(expected_prev) = known_tip {
            if prev_hash == expected_prev {
                // Normal sequential chain extension
                *self.last_tip_hash.write().await = Some(block_hash);
                self.last_tip_height.store(block_height, Ordering::Relaxed);

                let _ = obs_tx.send(Observation::Block(block)).await;
                for tx in transactions {
                    let _ = obs_tx.send(Observation::Transaction(tx)).await;
                }
            } else {
                // Tip mismatch: potential chain reorganization or competing tip!
                warn!(
                    received_block = %block_hash,
                    prev_hash = %prev_hash,
                    expected_tip = %expected_prev,
                    "Chain tip mismatch on incoming ZMQ block; investigating reorganization"
                );

                self.handle_potential_reorg(
                    &expected_prev,
                    &block_hash,
                    block_height,
                    &prev_hash,
                    obs_tx,
                )
                .await;

                *self.last_tip_hash.write().await = Some(block_hash);
                self.last_tip_height.store(block_height, Ordering::Relaxed);

                let _ = obs_tx.send(Observation::Block(block)).await;
                for tx in transactions {
                    let _ = obs_tx.send(Observation::Transaction(tx)).await;
                }
            }
        } else {
            // First observed block
            *self.last_tip_hash.write().await = Some(block_hash);
            self.last_tip_height.store(block_height, Ordering::Relaxed);

            let _ = obs_tx.send(Observation::Block(block)).await;
            for tx in transactions {
                let _ = obs_tx.send(Observation::Transaction(tx)).await;
            }
        }
    }

    /// Evaluates competing tips and emits ReorgObservation.
    async fn handle_potential_reorg(
        &self,
        old_tip_hash: &str,
        new_tip_hash: &str,
        new_tip_height: u64,
        new_prev_hash: &str,
        obs_tx: &mpsc::Sender<Observation>,
    ) {
        self.rpc_requests_total.fetch_add(1, Ordering::Relaxed);
        let old_height = self.last_tip_height.load(Ordering::Relaxed);

        // Trace common ancestor
        let mut disconnected = Vec::new();
        let mut connected = vec![new_tip_hash.to_string()];

        let mut current_old = old_tip_hash.to_string();
        let mut current_new = new_prev_hash.to_string();
        let mut common_ancestor = None;
        let mut depth = 1u64;

        // Bounded search for common ancestor (up to 100 blocks depth)
        for _ in 0..100 {
            if current_old == current_new {
                common_ancestor = Some(current_old);
                break;
            }

            disconnected.push(current_old.clone());
            connected.push(current_new.clone());

            let old_hdr = self.rpc.get_block_header(&current_old).await;
            let new_hdr = self.rpc.get_block_header(&current_new).await;

            match (old_hdr, new_hdr) {
                (Ok(o), Ok(n)) => {
                    if let (Some(op), Some(np)) = (o.previousblockhash, n.previousblockhash) {
                        current_old = op;
                        current_new = np;
                        depth += 1;
                    } else {
                        break;
                    }
                }
                _ => break,
            }
        }

        self.reorgs_detected.fetch_add(1, Ordering::Relaxed);

        let reorg_obs = ReorgObservation {
            old_tip_hash: old_tip_hash.to_string(),
            old_tip_height: old_height,
            new_tip_hash: new_tip_hash.to_string(),
            new_tip_height,
            common_ancestor_hash: common_ancestor,
            depth,
            disconnected_blocks: disconnected,
            connected_blocks: connected,
            observed_at: Utc::now(),
            source: Some(ObservationSource::bitcoin_core_zmq("zmq:sequence")),
        };

        info!(
            old_tip = %reorg_obs.old_tip_hash,
            new_tip = %reorg_obs.new_tip_hash,
            depth = reorg_obs.depth,
            "Emitting ReorgObservation"
        );

        let _ = obs_tx.send(Observation::Reorg(reorg_obs)).await;
    }

    /// Investigates competing tips via RPC `getchaintips`.
    pub async fn investigate_chain_tips(&self, obs_tx: &mpsc::Sender<Observation>) {
        self.rpc_requests_total.fetch_add(1, Ordering::Relaxed);
        let tips = match self.rpc.get_chain_tips().await {
            Ok(t) => t,
            Err(e) => {
                self.rpc_errors_total.fetch_add(1, Ordering::Relaxed);
                warn!(error = %e, "Failed to fetch chain tips during reorg investigation");
                return;
            }
        };

        let active_tip = tips.iter().find(|t| t.status == "active");
        if let Some(active) = active_tip {
            let last_hash = self.last_tip_hash.read().await.clone();
            if let Some(last) = last_hash {
                if last != active.hash {
                    self.handle_potential_reorg(&last, &active.hash, active.height, "", obs_tx)
                        .await;
                    *self.last_tip_hash.write().await = Some(active.hash.clone());
                    self.last_tip_height.store(active.height, Ordering::Relaxed);
                }
            }
        }
    }

    /// Reconciles missed blocks and updates node health state periodically.
    pub async fn reconcile_tip_and_gap(&self, obs_tx: &mpsc::Sender<Observation>) {
        self.rpc_requests_total.fetch_add(1, Ordering::Relaxed);
        let btc_info = match self.rpc.get_blockchain_info().await {
            Ok(info) => {
                let mut w = self.status.write().await;
                w.rpc_health = if info.initialblockdownload {
                    SourceHealthState::Syncing
                } else {
                    SourceHealthState::Connected
                };
                if let Some(ref mut caps) = w.capabilities {
                    caps.blocks = info.blocks;
                    caps.headers = info.headers;
                    caps.verification_progress = info.verificationprogress;
                    caps.initial_block_download = info.initialblockdownload;
                }
                info
            }
            Err(e) => {
                self.rpc_errors_total.fetch_add(1, Ordering::Relaxed);
                let mut w = self.status.write().await;
                w.rpc_health = SourceHealthState::Disconnected;
                warn!(error = %e, "Bitcoin Core RPC periodic health poll failed");
                return;
            }
        };

        let current_stored_height = self.last_tip_height.load(Ordering::Relaxed);
        if btc_info.blocks > current_stored_height && current_stored_height > 0 {
            let gap = btc_info.blocks - current_stored_height;

            if gap <= self.config.reconcile_max_blocks {
                info!(
                    stored_tip = current_stored_height,
                    node_tip = btc_info.blocks,
                    gap_blocks = gap,
                    "Reconciling missed blocks via Bitcoin Core RPC"
                );

                for height in (current_stored_height + 1)..=btc_info.blocks {
                    self.rpc_requests_total.fetch_add(1, Ordering::Relaxed);
                    if let Ok(hash) = self.rpc.get_block_hash(height).await {
                        if let Ok(hex_str) = self.rpc.get_block_raw_hex(&hash).await {
                            if let Ok(bytes) = Vec::<u8>::from_hex(&hex_str) {
                                if let Ok((block_obs, txs)) = parse_raw_block(
                                    &bytes,
                                    Some("rpc:gap_reconciliation"),
                                    16 * 1024 * 1024,
                                ) {
                                    let _ = obs_tx.send(Observation::Block(block_obs)).await;
                                    for tx in txs {
                                        let _ = obs_tx.send(Observation::Transaction(tx)).await;
                                    }
                                    self.gap_blocks_reconciled.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        *self.last_tip_hash.write().await = Some(hash);
                        self.last_tip_height.store(height, Ordering::Relaxed);
                    }
                }
            } else {
                warn!(
                    gap = gap,
                    max_allowed = self.config.reconcile_max_blocks,
                    "Large history gap exceeds reconciliation threshold; updating tip baseline without replaying history"
                );
                self.last_tip_height
                    .store(btc_info.blocks, Ordering::Relaxed);
                if let Ok(hash) = self.rpc.get_block_hash(btc_info.blocks).await {
                    *self.last_tip_hash.write().await = Some(hash);
                }
            }
        }
    }
}
