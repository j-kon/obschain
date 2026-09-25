use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::Instant,
};

use chrono::{DateTime, Utc};
use obschain_core::{
    calculate_fee_rate_sat_vb, BlockObservation, ChainEvent, Observation, ObservationContext,
    ObservationSource, ReplayCheckpoint, ReplayJob, ReplayJobStatus, SpentOutputContext,
    TransactionObservation, TxInputObservation, TxOutputObservation,
};
use obschain_detectors::DetectorEngine;
use obschain_intelligence::IncidentWatchEngine;
use obschain_storage::{
    EventRepository, IncidentActivityRepository, IncidentAlertRepository, ReplayRepository,
    Storage, StorageError,
};
use tracing::{error, info};
use uuid::Uuid;

use crate::bitcoin_rpc::{BitcoinCoreRpcClient, BitcoinNodeCapabilities, BitcoinRpcError};

// ---------------------------------------------------------------------------
// Historical Transaction Cache
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CachedTxOutput {
    pub value_sats: u64,
    pub address: Option<String>,
    pub confirmed_height: u64,
    pub confirmed_time: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct CachedTx {
    pub txid: String,
    pub confirmed_height: u64,
    pub confirmed_time: DateTime<Utc>,
    pub outputs: Vec<CachedTxOutput>,
}

#[derive(Debug, Clone, Default)]
pub struct CacheMetrics {
    pub hits: u64,
    pub misses: u64,
    pub rpc_lookups: u64,
    pub evictions: u64,
    pub size: usize,
    pub capacity: usize,
    pub hit_rate_percent: f64,
}

/// Bounded LRU/FIFO cache storing previous transactions and output metadata for UTXO enrichment.
pub struct HistoricalTxCache {
    capacity: usize,
    entries: RwLock<HashMap<String, CachedTx>>,
    order: RwLock<VecDeque<String>>,
    hits: AtomicU64,
    misses: AtomicU64,
    rpc_lookups: AtomicU64,
    evictions: AtomicU64,
}

impl HistoricalTxCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: RwLock::new(HashMap::with_capacity(capacity.min(5000))),
            order: RwLock::new(VecDeque::with_capacity(capacity.min(5000))),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            rpc_lookups: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    pub fn get(&self, txid: &str, vout: u32) -> Option<SpentOutputContext> {
        let entries = self.entries.read().ok()?;
        if let Some(tx) = entries.get(txid) {
            if let Some(out) = tx.outputs.get(vout as usize) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(SpentOutputContext {
                    txid: txid.to_string(),
                    vout,
                    value_sats: out.value_sats,
                    confirmed_height: Some(out.confirmed_height),
                    confirmed_at: Some(out.confirmed_time),
                });
            }
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    pub fn insert(&self, tx: CachedTx) {
        let mut entries = match self.entries.write() {
            Ok(e) => e,
            Err(_) => return,
        };
        let mut order = match self.order.write() {
            Ok(o) => o,
            Err(_) => return,
        };

        if entries.contains_key(&tx.txid) {
            entries.insert(tx.txid.clone(), tx);
            return;
        }

        while entries.len() >= self.capacity {
            if let Some(old_txid) = order.pop_front() {
                entries.remove(&old_txid);
                self.evictions.fetch_add(1, Ordering::Relaxed);
            } else {
                break;
            }
        }

        order.push_back(tx.txid.clone());
        entries.insert(tx.txid.clone(), tx);
    }

    pub fn record_rpc_lookup(&self) {
        self.rpc_lookups.fetch_add(1, Ordering::Relaxed);
    }

    pub fn metrics(&self) -> CacheMetrics {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let rpc_lookups = self.rpc_lookups.load(Ordering::Relaxed);
        let evictions = self.evictions.load(Ordering::Relaxed);
        let size = self.entries.read().map(|e| e.len()).unwrap_or(0);
        let total = hits + misses;
        let hit_rate_percent = if total > 0 {
            (hits as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        CacheMetrics {
            hits,
            misses,
            rpc_lookups,
            evictions,
            size,
            capacity: self.capacity,
            hit_rate_percent,
        }
    }
}

// ---------------------------------------------------------------------------
// Configuration & Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ReplayConfig {
    pub batch_size: usize,
    pub concurrency: usize,
    pub checkpoint_interval: u64,
    pub max_range: u64,
    pub tx_cache_limit: usize,
    pub db_concurrency: usize,
    pub sovereign_only: bool,
    pub api_enabled: bool,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            batch_size: 10,
            concurrency: 2,
            checkpoint_interval: 25,
            max_range: 100_000,
            tx_cache_limit: 100_000,
            db_concurrency: 2,
            sovereign_only: true,
            api_enabled: false,
        }
    }
}

impl ReplayConfig {
    pub fn from_env() -> Self {
        let batch_size = std::env::var("OBSCHAIN_REPLAY_BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10);

        let concurrency = std::env::var("OBSCHAIN_REPLAY_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2);

        let checkpoint_interval = std::env::var("OBSCHAIN_REPLAY_CHECKPOINT_INTERVAL")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(25);

        let max_range = std::env::var("OBSCHAIN_REPLAY_MAX_RANGE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100_000);

        let tx_cache_limit = std::env::var("OBSCHAIN_REPLAY_TX_CACHE_LIMIT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100_000);

        let db_concurrency = std::env::var("OBSCHAIN_REPLAY_DB_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2);

        let sovereign_only = std::env::var("OBSCHAIN_SOVEREIGN_ONLY")
            .map(|v| v.to_lowercase() != "false")
            .unwrap_or(true);

        let api_enabled = std::env::var("OBSCHAIN_REPLAY_API_ENABLED")
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(false);

        Self {
            batch_size,
            concurrency,
            checkpoint_interval,
            max_range,
            tx_cache_limit,
            db_concurrency,
            sovereign_only,
            api_enabled,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Requested replay starts at block {requested}, but this pruned node only retains history from block {retained_from}.")]
    PrunedHistoryUnavailable { requested: u64, retained_from: u64 },

    #[error("Bitcoin Core node is still in initial block download (IBD). Replay cannot proceed.")]
    InitialBlockDownloadActive,

    #[error("Requested end height {requested} exceeds node chain tip {tip}")]
    HeightBeyondTip { requested: u64, tip: u64 },

    #[error("Requested range {requested_blocks} blocks exceeds maximum configured range of {max_range} blocks")]
    RangeExceedsMaximum {
        requested_blocks: u64,
        max_range: u64,
    },

    #[error("Job not found: {0}")]
    JobNotFound(Uuid),

    #[error("Job {0} is in terminal status ({1:?}) and cannot be resumed")]
    JobAlreadyFinished(Uuid, ReplayJobStatus),

    #[error("RPC error: {0}")]
    Rpc(#[from] BitcoinRpcError),

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Block deserialization error: {0}")]
    Deserialization(String),

    #[error("Sovereign privacy violation: {0}")]
    SovereignViolation(String),
}

// ---------------------------------------------------------------------------
// Historical Replay Engine
// ---------------------------------------------------------------------------

pub struct HistoricalReplayEngine {
    rpc_client: Arc<BitcoinCoreRpcClient>,
    storage: Storage,
    detector_engine: Arc<tokio::sync::Mutex<DetectorEngine>>,
    watch_engine: Option<Arc<tokio::sync::RwLock<IncidentWatchEngine>>>,
    tx_cache: Arc<HistoricalTxCache>,
    config: ReplayConfig,
    active_cancel_tokens: Arc<RwLock<HashMap<Uuid, Arc<AtomicBool>>>>,
    active_pause_tokens: Arc<RwLock<HashMap<Uuid, Arc<AtomicBool>>>>,
}

impl HistoricalReplayEngine {
    pub fn new(
        rpc_client: Arc<BitcoinCoreRpcClient>,
        storage: Storage,
        detector_engine: Arc<tokio::sync::Mutex<DetectorEngine>>,
        watch_engine: Option<Arc<tokio::sync::RwLock<IncidentWatchEngine>>>,
        config: ReplayConfig,
    ) -> Self {
        let tx_cache = Arc::new(HistoricalTxCache::new(config.tx_cache_limit));
        Self {
            rpc_client,
            storage,
            detector_engine,
            watch_engine,
            tx_cache,
            config,
            active_cancel_tokens: Arc::new(RwLock::new(HashMap::new())),
            active_pause_tokens: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn config(&self) -> &ReplayConfig {
        &self.config
    }

    pub fn tx_cache(&self) -> &HistoricalTxCache {
        &self.tx_cache
    }

    /// Pre-flight validation verifying node synchronization, history availability, and range bounds.
    pub async fn validate_capabilities_and_range(
        &self,
        start_height: u64,
        end_height: u64,
    ) -> Result<BitcoinNodeCapabilities, ReplayError> {
        if start_height > end_height {
            return Err(ReplayError::Validation(format!(
                "start_height ({start_height}) must be <= end_height ({end_height})"
            )));
        }

        let requested_blocks = end_height - start_height + 1;
        if requested_blocks > self.config.max_range {
            return Err(ReplayError::RangeExceedsMaximum {
                requested_blocks,
                max_range: self.config.max_range,
            });
        }

        let capabilities = self.rpc_client.inspect_capabilities().await?;

        // 1. Initial block download check (allow override for regtest/dev)
        let is_regtest = capabilities.network.eq_ignore_ascii_case("regtest");
        if capabilities.initial_block_download && !is_regtest {
            return Err(ReplayError::InitialBlockDownloadActive);
        }

        // 2. Pruned node check
        if capabilities.pruned {
            if let Some(prune_h) = capabilities.prune_height {
                if start_height < prune_h {
                    return Err(ReplayError::PrunedHistoryUnavailable {
                        requested: start_height,
                        retained_from: prune_h,
                    });
                }
            }
        }

        // 3. Chain tip range check
        if end_height > capabilities.blocks {
            return Err(ReplayError::HeightBeyondTip {
                requested: end_height,
                tip: capabilities.blocks,
            });
        }

        Ok(capabilities)
    }

    /// Creates and persists a new ReplayJob, verifying all constraints.
    pub async fn create_replay_job(
        &self,
        start_height: u64,
        end_height: u64,
    ) -> Result<ReplayJob, ReplayError> {
        let capabilities = self
            .validate_capabilities_and_range(start_height, end_height)
            .await?;

        let source_type = format!("bitcoin_core_rpc:{}", capabilities.network);
        let job = ReplayJob::new(capabilities.network, start_height, end_height, source_type);

        self.storage.create_job(&job).await?;
        info!(
            job_id = %job.id,
            start = job.start_height,
            end = job.end_height,
            network = %job.network,
            "Historical replay job created"
        );
        Ok(job)
    }

    /// Signals cancellation for an active replay job.
    pub async fn cancel_replay(&self, job_id: Uuid) -> Result<(), ReplayError> {
        if let Ok(tokens) = self.active_cancel_tokens.read() {
            if let Some(token) = tokens.get(&job_id) {
                token.store(true, Ordering::SeqCst);
            }
        }

        if let Some(mut job) = self.storage.get_job(job_id).await? {
            if !job.status.is_terminal() {
                job.status = ReplayJobStatus::Cancelled;
                job.completed_at = Some(Utc::now());
                self.storage.update_job(&job).await?;
                info!(job_id = %job_id, "Replay job marked cancelled");
            }
        }
        Ok(())
    }

    /// Signals pause for an active replay job.
    pub async fn pause_replay(&self, job_id: Uuid) -> Result<(), ReplayError> {
        if let Ok(tokens) = self.active_pause_tokens.read() {
            if let Some(token) = tokens.get(&job_id) {
                token.store(true, Ordering::SeqCst);
            }
        }
        Ok(())
    }

    /// Resumes a paused or stopped replay job from its latest persisted checkpoint.
    pub async fn resume_replay(&self, job_id: Uuid) -> Result<ReplayJob, ReplayError> {
        let Some(mut job) = self.storage.get_job(job_id).await? else {
            return Err(ReplayError::JobNotFound(job_id));
        };

        if job.status == ReplayJobStatus::Completed || job.status == ReplayJobStatus::Cancelled {
            return Err(ReplayError::JobAlreadyFinished(job_id, job.status));
        }

        if let Some(cp) = self.storage.get_latest_checkpoint(job_id).await? {
            job.current_height = cp.completed_height + 1;
            job.blocks_processed = cp.blocks_processed;
            job.transactions_processed = cp.transactions_processed;
            job.events_generated = cp.events_generated;
            info!(
                job_id = %job_id,
                resume_height = job.current_height,
                blocks_already_processed = job.blocks_processed,
                "Resuming replay job from checkpoint"
            );
        }

        job.status = ReplayJobStatus::Running;
        self.storage.update_job(&job).await?;
        self.run_job(job_id).await
    }

    /// Executes the replay job sequentially across blocks from `current_height` to `end_height`.
    pub async fn run_job(&self, job_id: Uuid) -> Result<ReplayJob, ReplayError> {
        let Some(mut job) = self.storage.get_job(job_id).await? else {
            return Err(ReplayError::JobNotFound(job_id));
        };

        let cancel_token = Arc::new(AtomicBool::new(false));
        let pause_token = Arc::new(AtomicBool::new(false));

        if let Ok(mut tokens) = self.active_cancel_tokens.write() {
            tokens.insert(job_id, Arc::clone(&cancel_token));
        }
        if let Ok(mut tokens) = self.active_pause_tokens.write() {
            tokens.insert(job_id, Arc::clone(&pause_token));
        }

        job.status = ReplayJobStatus::Running;
        if job.started_at.is_none() {
            job.started_at = Some(Utc::now());
        }
        self.storage.update_job(&job).await?;

        let mut prev_block_time: Option<DateTime<Utc>> = None;
        let start_height = job.current_height;
        let end_height = job.end_height;

        let start_instant = Instant::now();

        for height in start_height..=end_height {
            // Check cancellation
            if cancel_token.load(Ordering::SeqCst) {
                job.status = ReplayJobStatus::Cancelled;
                job.completed_at = Some(Utc::now());
                self.storage.update_job(&job).await?;
                info!(job_id = %job_id, height, "Replay cancelled at block boundary");
                break;
            }

            // Check pause
            if pause_token.load(Ordering::SeqCst) {
                job.status = ReplayJobStatus::Paused;
                self.storage.update_job(&job).await?;
                info!(job_id = %job_id, height, "Replay paused at block boundary");
                break;
            }

            // Process single block
            match self.process_block(&mut job, height, prev_block_time).await {
                Ok((block_time, tx_count, event_count)) => {
                    prev_block_time = Some(block_time);
                    job.current_height = height;
                    job.blocks_processed += 1;
                    job.transactions_processed += tx_count as u64;
                    job.events_generated += event_count as u64;

                    // Checkpoint every checkpoint_interval or at final block
                    let is_checkpoint = (job.blocks_processed % self.config.checkpoint_interval
                        == 0)
                        || height == end_height;

                    if is_checkpoint {
                        let checkpoint = ReplayCheckpoint::new(
                            job.id,
                            height,
                            job.blocks_processed,
                            job.transactions_processed,
                            job.events_generated,
                        );
                        self.storage.save_checkpoint(&checkpoint).await?;
                        self.storage.update_job(&job).await?;

                        let elapsed = start_instant.elapsed().as_secs_f64();
                        let blocks_per_sec = if elapsed > 0.0 {
                            job.blocks_processed as f64 / elapsed
                        } else {
                            0.0
                        };

                        info!(
                            job_id = %job.id,
                            height,
                            progress = format!("{:.1}%", job.progress_percentage()),
                            blocks_processed = job.blocks_processed,
                            events_generated = job.events_generated,
                            blocks_per_sec = format!("{:.2}", blocks_per_sec),
                            "Replay progress checkpoint saved"
                        );
                    }
                }
                Err(err) => {
                    error!(
                        job_id = %job.id,
                        height,
                        error = %err,
                        "Replay failed at block height"
                    );
                    job.error_count += 1;
                    job.last_error = Some(err.to_string());
                    job.status = ReplayJobStatus::Failed;
                    job.completed_at = Some(Utc::now());
                    self.storage.update_job(&job).await?;
                    return Err(err);
                }
            }

            // Yield cooperatively to prevent starving live monitoring threads
            tokio::task::yield_now().await;
        }

        if job.status == ReplayJobStatus::Running && job.current_height == end_height {
            job.status = ReplayJobStatus::Completed;
            job.completed_at = Some(Utc::now());
            self.storage.update_job(&job).await?;
            info!(
                job_id = %job.id,
                total_blocks = job.blocks_processed,
                total_transactions = job.transactions_processed,
                total_events = job.events_generated,
                "Historical replay job completed successfully"
            );
        }

        // Clean up tokens
        if let Ok(mut tokens) = self.active_cancel_tokens.write() {
            tokens.remove(&job_id);
        }
        if let Ok(mut tokens) = self.active_pause_tokens.write() {
            tokens.remove(&job_id);
        }

        Ok(job)
    }

    /// Fetches, parses, enriches, detects, and persists a single historical block.
    async fn process_block(
        &self,
        job: &mut ReplayJob,
        height: u64,
        prev_block_time: Option<DateTime<Utc>>,
    ) -> Result<(DateTime<Utc>, usize, usize), ReplayError> {
        // 1. Fetch block hash and raw block hex
        let block_hash = self.rpc_client.get_block_hash(height).await?;
        let raw_hex = self.rpc_client.get_block_raw_hex(&block_hash).await?;
        let raw_bytes = hex::decode(&raw_hex)
            .map_err(|e| ReplayError::Deserialization(format!("Hex decode error: {e}")))?;

        // 2. Deserialize into bitcoin::Block
        let block: bitcoin::Block = bitcoin::consensus::deserialize(&raw_bytes)
            .map_err(|e| ReplayError::Deserialization(format!("Block deserialize error: {e}")))?;

        let block_time =
            DateTime::<Utc>::from_timestamp(block.header.time as i64, 0).unwrap_or_else(Utc::now);

        // 3. Compute interval using historical timestamps (not wall-clock)
        let interval_seconds = if let Some(p_time) = prev_block_time {
            Some(
                block_time
                    .signed_duration_since(p_time)
                    .num_seconds()
                    .max(0) as u64,
            )
        } else if height > 0 {
            // For first block in range, fetch previous block header to compute accurate interval
            let prev_hash = block.header.prev_blockhash.to_string();
            if let Ok(prev_header) = self.rpc_client.get_block_header(&prev_hash).await {
                let p_time = DateTime::<Utc>::from_timestamp(prev_header.time as i64, 0)
                    .unwrap_or_else(Utc::now);
                Some(
                    block_time
                        .signed_duration_since(p_time)
                        .num_seconds()
                        .max(0) as u64,
                )
            } else {
                None
            }
        } else {
            None
        };

        // 4. Construct BlockObservation
        let block_obs = BlockObservation {
            block_hash: block_hash.clone(),
            height,
            timestamp: block_time,
            previous_block_hash: block.header.prev_blockhash.to_string(),
            tx_count: block.txdata.len(),
            size_bytes: raw_bytes.len() as u64,
            weight: block.weight().to_wu(),
            difficulty: Some(block.header.difficulty_float()),
            miner_tag: None,
            interval_seconds,
            source: Some(ObservationSource::bitcoin_core_rpc(
                &self.rpc_client.config().rpc_url,
            )),
        };

        // 5. Build block-local transaction map for intra-block spends
        let mut block_map: HashMap<String, CachedTx> =
            HashMap::with_capacity(block.txdata.len().min(10_000));
        for tx in &block.txdata {
            let txid = tx.compute_txid().to_string();
            let outputs = tx
                .output
                .iter()
                .map(|out| {
                    let address = bitcoin::Address::from_script(
                        &out.script_pubkey,
                        bitcoin::Network::Bitcoin,
                    )
                    .ok()
                    .map(|a| a.to_string());

                    CachedTxOutput {
                        value_sats: out.value.to_sat(),
                        address,
                        confirmed_height: height,
                        confirmed_time: block_time,
                    }
                })
                .collect();

            block_map.insert(
                txid.clone(),
                CachedTx {
                    txid,
                    confirmed_height: height,
                    confirmed_time: block_time,
                    outputs,
                },
            );
        }

        // 6. Enrich and normalize each transaction
        let mut tx_observations = Vec::with_capacity(block.txdata.len());
        for tx in &block.txdata {
            let txid = tx.compute_txid().to_string();
            let tx_weight = tx.weight().to_wu();
            let tx_vsize = tx.vsize() as u64;

            let mut inputs = Vec::with_capacity(tx.input.len());
            let mut total_input_sats = 0u64;
            let mut all_inputs_resolved = true;

            for inp in &tx.input {
                let is_coinbase = inp.previous_output.is_null();
                let prev_txid = inp.previous_output.txid.to_string();
                let vout = inp.previous_output.vout;

                let mut utxo_ctx: Option<SpentOutputContext> = None;

                if !is_coinbase {
                    // Check order:
                    // 1. Block-local map (same-block spend)
                    if let Some(local_tx) = block_map.get(&prev_txid) {
                        if let Some(out) = local_tx.outputs.get(vout as usize) {
                            utxo_ctx = Some(SpentOutputContext {
                                txid: prev_txid.clone(),
                                vout,
                                value_sats: out.value_sats,
                                confirmed_height: Some(out.confirmed_height),
                                confirmed_at: Some(out.confirmed_time),
                            });
                        }
                    }

                    // 2. Bounded historical tx cache
                    if utxo_ctx.is_none() {
                        utxo_ctx = self.tx_cache.get(&prev_txid, vout);
                    }

                    // 3. Fallback to Bitcoin Core RPC if sovereign mode permits and txindex is available
                    if utxo_ctx.is_none() {
                        if let Ok(prev_tx_verbose) = self
                            .rpc_client
                            .get_raw_transaction_verbose(&prev_txid)
                            .await
                        {
                            self.tx_cache.record_rpc_lookup();
                            let prev_time = prev_tx_verbose
                                .blocktime
                                .or(prev_tx_verbose.time)
                                .and_then(|t| DateTime::<Utc>::from_timestamp(t, 0))
                                .unwrap_or(block_time);

                            let prev_outputs: Vec<CachedTxOutput> = prev_tx_verbose
                                .vout
                                .iter()
                                .map(|vo| {
                                    let sats = (vo.value * 100_000_000.0).round() as u64;
                                    let addr =
                                        vo.script_pubkey.as_ref().and_then(|sp| sp.address.clone());
                                    CachedTxOutput {
                                        value_sats: sats,
                                        address: addr,
                                        confirmed_height: height.saturating_sub(
                                            prev_tx_verbose.confirmations.unwrap_or(1),
                                        ),
                                        confirmed_time: prev_time,
                                    }
                                })
                                .collect();

                            if let Some(target_out) = prev_outputs.get(vout as usize) {
                                utxo_ctx = Some(SpentOutputContext {
                                    txid: prev_txid.clone(),
                                    vout,
                                    value_sats: target_out.value_sats,
                                    confirmed_height: Some(target_out.confirmed_height),
                                    confirmed_at: Some(prev_time),
                                });
                            }

                            // Store in bounded cache for subsequent lookups
                            self.tx_cache.insert(CachedTx {
                                txid: prev_txid.clone(),
                                confirmed_height: height
                                    .saturating_sub(prev_tx_verbose.confirmations.unwrap_or(1)),
                                confirmed_time: prev_time,
                                outputs: prev_outputs,
                            });
                        }
                    }
                }

                if let Some(ref ctx) = utxo_ctx {
                    total_input_sats = total_input_sats.saturating_add(ctx.value_sats);
                } else if !is_coinbase {
                    all_inputs_resolved = false;
                }

                inputs.push(TxInputObservation {
                    txid: prev_txid,
                    vout,
                    sequence: inp.sequence.to_consensus_u32(),
                    prev_out_value_sats: utxo_ctx.as_ref().map(|c| c.value_sats),
                    prev_out_address: None,
                    is_coinbase,
                    historical_utxo: utxo_ctx,
                });
            }

            let mut total_output_sats = 0u64;
            let outputs: Vec<TxOutputObservation> = tx
                .output
                .iter()
                .enumerate()
                .map(|(n, out)| {
                    let val = out.value.to_sat();
                    total_output_sats = total_output_sats.saturating_add(val);

                    let script_hex = hex::encode(out.script_pubkey.as_bytes());
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

                    let address = bitcoin::Address::from_script(
                        &out.script_pubkey,
                        bitcoin::Network::Bitcoin,
                    )
                    .ok()
                    .map(|a| a.to_string());

                    TxOutputObservation {
                        n: n as u32,
                        value_sats: val,
                        scriptpubkey_hex: Some(script_hex),
                        script_pubkey_type: script_type,
                        address,
                    }
                })
                .collect();

            let fee_sats = if all_inputs_resolved && total_input_sats >= total_output_sats {
                total_input_sats - total_output_sats
            } else {
                0
            };

            let fee_rate_sat_vb = calculate_fee_rate_sat_vb(fee_sats, tx_vsize);

            let is_rbf = tx
                .input
                .iter()
                .any(|i| i.sequence.to_consensus_u32() < 0xFFFF_FFFE);

            let input_count = inputs.len();
            let output_count = outputs.len();

            tx_observations.push(TransactionObservation {
                txid,
                timestamp: block_time,
                block_hash: Some(block_hash.clone()),
                block_height: Some(height),
                fee_sats,
                size: tx.total_size() as u64,
                weight: tx_weight,
                vsize: tx_vsize,
                fee_rate_sat_vb,
                total_input_sats,
                total_output_sats,
                input_count,
                output_count,
                inputs,
                outputs,
                is_rbf,
                confirmed: true,
                source: Some(ObservationSource::bitcoin_core_rpc(
                    &self.rpc_client.config().rpc_url,
                )),
            });
        }

        // 7. Store block transactions in historical cache and discard block_map
        for (_, cached_tx) in block_map.into_iter() {
            self.tx_cache.insert(cached_tx);
        }

        // 8. Run Detectors with ObservationContext
        let obs_context = ObservationContext::historical_replay(
            job.id,
            height,
            block_hash.clone(),
            block_time,
            job.network.clone(),
        );

        let mut events: Vec<ChainEvent> = Vec::new();
        let mut detector_engine = self.detector_engine.lock().await;

        // Detect on block
        let block_events = detector_engine.process_observation_with_context(
            Observation::Block(block_obs.clone()),
            Some(&obs_context),
        );
        events.extend(block_events);

        // Detect on transactions
        for tx_obs in &tx_observations {
            let tx_events = detector_engine.process_observation_with_context(
                Observation::Transaction(tx_obs.clone()),
                Some(&obs_context),
            );
            events.extend(tx_events);
        }
        drop(detector_engine);

        // 9. Persist events idempotently
        for event in &events {
            self.storage.save_event(event).await?;
        }

        // 10. Pass to IncidentWatchEngine if configured
        if let Some(ref watch_engine_arc) = self.watch_engine {
            let mut watch_engine = watch_engine_arc.write().await;

            // Watch block
            let b_results = watch_engine.process_block(&block_obs);
            for (act, alt_opt) in b_results {
                self.storage.save_activity(&act).await?;
                if let Some(alt) = alt_opt {
                    self.storage.save_alert(&alt).await?;
                }
            }

            // Watch transactions
            for tx_obs in &tx_observations {
                let t_results =
                    watch_engine.process_transaction(tx_obs, Some(height), Some(&block_hash));
                for (act, alt_opt) in t_results {
                    self.storage.save_activity(&act).await?;
                    if let Some(alt) = alt_opt {
                        self.storage.save_alert(&alt).await?;
                    }
                }
            }
        }

        Ok((block_time, block.txdata.len(), events.len()))
    }
}
