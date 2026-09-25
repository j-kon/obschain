use std::{
    path::PathBuf,
    process::{Child, Command},
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Utc};
use obschain_core::{
    EventType, Observation, ObservationContext, ObservationMode, SpentOutputContext,
    TransactionObservation, TxInputObservation, TxOutputObservation,
};
use obschain_detectors::{
    ConsolidationDetector, Detector, DetectorEngine, DormantCoinDetector, ExtremeFeeDetector,
    FanOutDetector, LargeTransactionDetector, LongBlockIntervalDetector, RbfDetector,
    ReorgDetector,
};
use obschain_ingest::{
    BitcoinCoreRpcClient, BitcoinRpcConfig, CachedTx, CachedTxOutput, HistoricalReplayEngine,
    HistoricalTxCache, ReplayConfig, ReplayError,
};
use obschain_storage::{EventRepository, InMemoryStorage, ReplayRepository, Storage};
use uuid::Uuid;

// ===========================================================================
// Regtest Test Harness
// ===========================================================================

#[allow(dead_code)]
struct RegtestHarness {
    datadir: PathBuf,
    rpc_port: u16,
    zmq_rawtx_port: u16,
    zmq_rawblock_port: u16,
    zmq_sequence_port: u16,
    child: Option<Child>,
    cookie_path: PathBuf,
}

fn get_distinct_free_ports() -> (u16, u16, u16, u16) {
    let l1 = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port 1");
    let l2 = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port 2");
    let l3 = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port 3");
    let l4 = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port 4");
    let p1 = l1.local_addr().expect("port 1").port();
    let p2 = l2.local_addr().expect("port 2").port();
    let p3 = l3.local_addr().expect("port 3").port();
    let p4 = l4.local_addr().expect("port 4").port();
    drop(l1);
    drop(l2);
    drop(l3);
    drop(l4);
    (p1, p2, p3, p4)
}

impl RegtestHarness {
    fn is_bitcoind_available() -> bool {
        Command::new("bitcoind")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    async fn start() -> Option<Self> {
        if !Self::is_bitcoind_available() {
            eprintln!("bitcoind not available in PATH; skipping regtest tests");
            return None;
        }

        let run_id = uuid::Uuid::new_v4();
        let datadir = std::env::temp_dir().join(format!("obschain_replay_test_{run_id}"));
        std::fs::create_dir_all(&datadir).expect("create regtest datadir");

        let (rpc_port, zmq_rawtx_port, zmq_rawblock_port, zmq_sequence_port) =
            get_distinct_free_ports();

        let conf_content = format!(
            "regtest=1\n\
             server=1\n\
             listen=0\n\
             txindex=1\n\
             fallbackfee=0.0001\n\
             [regtest]\n\
             rpcbind=127.0.0.1\n\
             rpcport={rpc_port}\n\
             rpcallowip=127.0.0.1\n\
             zmqpubrawtx=tcp://127.0.0.1:{zmq_rawtx_port}\n\
             zmqpubrawblock=tcp://127.0.0.1:{zmq_rawblock_port}\n\
             zmqpubsequence=tcp://127.0.0.1:{zmq_sequence_port}\n"
        );
        std::fs::write(datadir.join("bitcoin.conf"), conf_content).expect("write bitcoin.conf");

        let child = Command::new("bitcoind")
            .arg(format!("-datadir={}", datadir.display()))
            .spawn()
            .expect("spawn bitcoind");

        let cookie_path = datadir.join("regtest").join(".cookie");

        let harness = Self {
            datadir,
            rpc_port,
            zmq_rawtx_port,
            zmq_rawblock_port,
            zmq_sequence_port,
            child: Some(child),
            cookie_path,
        };

        let mut ready = false;
        for _ in 0..60 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            if harness.cookie_path.exists() {
                let out = Command::new("bitcoin-cli")
                    .arg(format!("-datadir={}", harness.datadir.display()))
                    .arg(format!("-rpcport={}", harness.rpc_port))
                    .arg("getblockchaininfo")
                    .output();
                if let Ok(res) = out {
                    if res.status.success() {
                        ready = true;
                        break;
                    }
                }
            }
        }

        if !ready {
            eprintln!("bitcoind failed to become ready within 15 seconds");
            return None;
        }

        let _ = Command::new("bitcoin-cli")
            .arg(format!("-datadir={}", harness.datadir.display()))
            .arg(format!("-rpcport={}", harness.rpc_port))
            .arg("createwallet")
            .arg("testwallet")
            .output();

        Some(harness)
    }

    fn cli(&self, args: &[&str]) -> String {
        let output = Command::new("bitcoin-cli")
            .arg(format!("-datadir={}", self.datadir.display()))
            .arg(format!("-rpcport={}", self.rpc_port))
            .args(args)
            .output()
            .expect("execute bitcoin-cli");
        assert!(
            output.status.success(),
            "cli error: {:?}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    fn mine_blocks(&self, count: u32) -> Vec<String> {
        let addr = self.cli(&["-rpcwallet=testwallet", "getnewaddress"]);
        let json_str = self.cli(&["generatetoaddress", &count.to_string(), &addr]);
        serde_json::from_str(&json_str).unwrap_or_default()
    }

    fn rpc_client(&self) -> BitcoinCoreRpcClient {
        let rpc_config = BitcoinRpcConfig {
            rpc_url: format!("http://127.0.0.1:{}", self.rpc_port),
            cookie_file: Some(self.cookie_path.clone()),
            expected_network: Some("regtest".to_string()),
            timeout: Duration::from_secs(10),
            ..Default::default()
        };
        BitcoinCoreRpcClient::new(rpc_config).expect("create rpc client")
    }
}

impl Drop for RegtestHarness {
    fn drop(&mut self) {
        let _ = Command::new("bitcoin-cli")
            .arg(format!("-datadir={}", self.datadir.display()))
            .arg(format!("-rpcport={}", self.rpc_port))
            .arg("stop")
            .output();

        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.datadir);
    }
}

// ===========================================================================
// Unit Tests: Deterministic Replay Time & UTXO Engine
// ===========================================================================

#[test]
fn test_historical_tx_cache_bounded_eviction_and_metrics() {
    let cache = HistoricalTxCache::new(3);

    // Initial state
    let m = cache.metrics();
    assert_eq!(m.size, 0);
    assert_eq!(m.hits, 0);
    assert_eq!(m.misses, 0);
    assert_eq!(m.evictions, 0);

    let dummy_time = Utc::now();
    let tx1 = CachedTx {
        txid: "tx1".to_string(),
        confirmed_height: 100,
        confirmed_time: dummy_time,
        outputs: vec![CachedTxOutput {
            value_sats: 50_000,
            address: None,
            confirmed_height: 100,
            confirmed_time: dummy_time,
        }],
    };
    let tx2 = CachedTx {
        txid: "tx2".to_string(),
        confirmed_height: 101,
        confirmed_time: dummy_time,
        outputs: vec![CachedTxOutput {
            value_sats: 60_000,
            address: None,
            confirmed_height: 101,
            confirmed_time: dummy_time,
        }],
    };
    let tx3 = CachedTx {
        txid: "tx3".to_string(),
        confirmed_height: 102,
        confirmed_time: dummy_time,
        outputs: vec![CachedTxOutput {
            value_sats: 70_000,
            address: None,
            confirmed_height: 102,
            confirmed_time: dummy_time,
        }],
    };
    let tx4 = CachedTx {
        txid: "tx4".to_string(),
        confirmed_height: 103,
        confirmed_time: dummy_time,
        outputs: vec![CachedTxOutput {
            value_sats: 80_000,
            address: None,
            confirmed_height: 103,
            confirmed_time: dummy_time,
        }],
    };

    cache.insert(tx1);
    cache.insert(tx2);
    cache.insert(tx3);
    assert_eq!(cache.metrics().size, 3);
    assert_eq!(cache.metrics().evictions, 0);

    // Hit tx1
    let hit = cache.get("tx1", 0);
    assert!(hit.is_some());
    assert_eq!(hit.unwrap().value_sats, 50_000);

    // Miss non-existent
    let miss = cache.get("non_existent", 0);
    assert!(miss.is_none());

    // Insert 4th: should evict oldest (tx2, because tx1 was accessed)
    cache.insert(tx4);
    assert_eq!(cache.metrics().size, 3);
    assert_eq!(cache.metrics().evictions, 1);

    let m_after = cache.metrics();
    assert_eq!(m_after.hits, 1);
    assert_eq!(m_after.misses, 1);
    assert!(m_after.hit_rate_percent >= 49.0 && m_after.hit_rate_percent <= 51.0);
}

#[test]
fn test_dormant_coin_age_historical_deterministic_time() {
    // Critical correctness test:
    // A transaction confirmed in 2013 spending a 2011 output must be evaluated
    // as ~2 years dormant relative to 2013, NOT 15 years dormant relative to 2026 wall-clock!

    let detector = DormantCoinDetector::with_thresholds(1825, 100_000_000); // 5 years, 1 BTC

    // Output confirmed in 2011 (e.g. Jan 1, 2011)
    let confirmed_2011 = DateTime::parse_from_rfc3339("2011-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    // Spending transaction in 2013 (2 years later: 730 days)
    // 730 days < 1825 days threshold, so this must NOT trigger a dormant coin alert!
    let replay_time_2013 = DateTime::parse_from_rfc3339("2013-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let historical_ctx_2013 = ObservationContext::historical_replay(
        Uuid::new_v4(),
        210000,
        "000000000000048b95347e83192f69cf0366076336c639f9b7228e9ba171342e".to_string(),
        replay_time_2013,
        "bitcoin".to_string(),
    );

    let spent_utxo = SpentOutputContext {
        txid: "prevtx123".to_string(),
        vout: 0,
        value_sats: 500_000_000, // 5 BTC
        confirmed_height: Some(105000),
        confirmed_at: Some(confirmed_2011),
    };

    let tx_obs = TransactionObservation {
        txid: "spendtx123".to_string(),
        timestamp: replay_time_2013,
        block_hash: Some(
            "000000000000048b95347e83192f69cf0366076336c639f9b7228e9ba171342e".to_string(),
        ),
        block_height: Some(210000),
        fee_sats: 10_000,
        size: 250,
        weight: 1000,
        vsize: 250,
        fee_rate_sat_vb: Some(40.0),
        total_input_sats: 500_000_000,
        total_output_sats: 499_990_000,
        input_count: 1,
        output_count: 1,
        inputs: vec![TxInputObservation {
            txid: "prevtx123".to_string(),
            vout: 0,
            sequence: 0xFFFFFFFF,
            prev_out_value_sats: Some(500_000_000),
            prev_out_address: None,
            is_coinbase: false,
            historical_utxo: Some(spent_utxo.clone()),
        }],
        outputs: vec![TxOutputObservation {
            value_sats: 499_990_000,
            n: 0,
            scriptpubkey_hex: Some("76a914".to_string()),
            address: None,
            script_pubkey_type: Some("p2pkh".to_string()),
        }],
        is_rbf: false,
        confirmed: true,
        source: None,
    };

    // 1. Evaluate with 2013 historical replay context:
    // Age = 2013 - 2011 = ~2 years. Threshold = 5 years.
    // Result: NO EVENT.
    let mut engine = DetectorEngine::new(vec![Arc::new(detector)]);
    let events_2013 = engine.process_observation_with_context(
        Observation::Transaction(tx_obs.clone()),
        Some(&historical_ctx_2013),
    );
    assert!(
        events_2013.is_empty(),
        "Transaction replayed at 2013 context (2-year-old coins) must NOT trigger 5-year dormant detector!"
    );

    // 2. Now simulate spending at 2018 (7 years later: 2556 days > 1825 days threshold).
    // Result: MUST TRIGGER EVENT.
    let replay_time_2018 = DateTime::parse_from_rfc3339("2018-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let historical_ctx_2018 = ObservationContext::historical_replay(
        Uuid::new_v4(),
        500000,
        "0000000000000000002b95347e83192f69cf0366076336c639f9b7228e9ba171".to_string(),
        replay_time_2018,
        "bitcoin".to_string(),
    );

    let mut tx_obs_2018 = tx_obs.clone();
    tx_obs_2018.timestamp = replay_time_2018;

    let events_2018 = engine.process_observation_with_context(
        Observation::Transaction(tx_obs_2018),
        Some(&historical_ctx_2018),
    );
    assert_eq!(
        events_2018.len(),
        1,
        "Transaction replayed at 2018 context (7-year-old coins) MUST trigger dormant detector"
    );
    let ev = &events_2018[0];
    assert_eq!(ev.event_type, EventType::DormantCoinsMoved);
    assert_eq!(ev.observation_mode, ObservationMode::HistoricalReplay);
    assert_eq!(ev.replay_job_id, historical_ctx_2018.replay_job_id);
}

#[test]
fn test_detector_availability_model() {
    let rbf = RbfDetector::new();
    let reorg = ReorgDetector::new();
    let dormant = DormantCoinDetector::with_thresholds(100, 100);
    let large_tx = LargeTransactionDetector::with_threshold_sats(1000);

    assert!(!rbf.availability().historically_replayable);
    assert!(rbf.availability().requires_mempool_history);

    assert!(!reorg.availability().historically_replayable);

    assert!(dormant.availability().historically_replayable);
    assert!(!dormant.availability().requires_mempool_history);

    assert!(large_tx.availability().historically_replayable);
}

#[test]
fn test_pruned_node_error_formatting() {
    let err = ReplayError::PrunedHistoryUnavailable {
        requested: 500_000,
        retained_from: 820_000,
    };
    let formatted = err.to_string();
    assert_eq!(
        formatted,
        "Requested replay starts at block 500000, but this pruned node only retains history from block 820000."
    );
}

// ===========================================================================
// Regtest Integration Tests
// ===========================================================================

#[tokio::test]
async fn test_regtest_historical_replay_block_range() {
    let harness = match RegtestHarness::start().await {
        Some(h) => h,
        None => return,
    };

    // 1. Mine 110 blocks to unlock coinbase maturity (100 blocks required on regtest)
    harness.mine_blocks(110);

    // 2. Generate a large transaction (e.g. 50 BTC transfer)
    let recipient = harness.cli(&["-rpcwallet=testwallet", "getnewaddress"]);
    let txid = harness.cli(&["-rpcwallet=testwallet", "sendtoaddress", &recipient, "25"]);
    assert!(!txid.is_empty());

    // 3. Mine a block confirming the transaction
    let mined = harness.mine_blocks(1);
    assert_eq!(mined.len(), 1);

    let tip_info: serde_json::Value =
        serde_json::from_str(&harness.cli(&["getblockchaininfo"])).unwrap();
    let tip_height = tip_info["blocks"].as_u64().unwrap();
    assert!(tip_height >= 111);

    // 4. Setup HistoricalReplayEngine
    let rpc = Arc::new(harness.rpc_client());
    let storage = Storage::from(InMemoryStorage::new_empty(10_000));

    let large_tx_detector = Arc::new(LargeTransactionDetector::with_threshold_sats(
        1_000_000_000, // 10 BTC threshold
    ));
    let long_interval_detector = Arc::new(LongBlockIntervalDetector::with_threshold_seconds(1800));
    let consolidation_detector = Arc::new(ConsolidationDetector::with_thresholds(20, 5, 0));
    let fanout_detector = Arc::new(FanOutDetector::with_thresholds(50, 0));
    let extreme_fee_detector = Arc::new(ExtremeFeeDetector::with_thresholds(10_000_000, 200.0));
    let rbf_detector = Arc::new(RbfDetector::new());
    let reorg_detector = Arc::new(ReorgDetector::new());

    let detectors: Vec<Arc<dyn Detector>> = vec![
        large_tx_detector,
        long_interval_detector,
        consolidation_detector,
        fanout_detector,
        extreme_fee_detector,
        rbf_detector,
        reorg_detector,
    ];

    let detector_engine = Arc::new(tokio::sync::Mutex::new(DetectorEngine::new(detectors)));
    let replay_config = ReplayConfig {
        batch_size: 10,
        concurrency: 1,
        checkpoint_interval: 10,
        max_range: 1000,
        tx_cache_limit: 1000,
        db_concurrency: 1,
        sovereign_only: true,
        api_enabled: true,
    };

    let engine =
        HistoricalReplayEngine::new(rpc, storage.clone(), detector_engine, None, replay_config);

    // 5. Replay blocks 105 through tip_height
    let start_h = 105;
    let end_h = tip_height;
    let job = engine
        .create_replay_job(start_h, end_h)
        .await
        .expect("create replay job");

    let completed_job = engine.run_job(job.id).await.expect("run replay job");
    assert_eq!(
        completed_job.status,
        obschain_core::ReplayJobStatus::Completed
    );
    assert_eq!(completed_job.blocks_processed, end_h - start_h + 1);
    assert!(completed_job.transactions_processed >= (end_h - start_h + 1));

    // 6. Verify detected events in storage
    let events = storage
        .list_events(100, 0)
        .await
        .expect("list persisted events");
    assert!(
        !events.is_empty(),
        "Expected at least one event from historical replay (e.g. large transfer)"
    );

    for ev in &events {
        assert_eq!(ev.observation_mode, ObservationMode::HistoricalReplay);
        assert_eq!(ev.replay_job_id, Some(job.id));
        assert!(ev.block_height >= Some(start_h) && ev.block_height <= Some(end_h));
    }
}

#[tokio::test]
async fn test_regtest_replay_idempotency_run_twice() {
    let harness = match RegtestHarness::start().await {
        Some(h) => h,
        None => return,
    };

    harness.mine_blocks(105);

    let recipient = harness.cli(&["-rpcwallet=testwallet", "getnewaddress"]);
    let _ = harness.cli(&["-rpcwallet=testwallet", "sendtoaddress", &recipient, "25"]);
    harness.mine_blocks(1);

    let tip_info: serde_json::Value =
        serde_json::from_str(&harness.cli(&["getblockchaininfo"])).unwrap();
    let tip_height = tip_info["blocks"].as_u64().unwrap();

    let rpc = Arc::new(harness.rpc_client());
    let storage = Storage::from(InMemoryStorage::new_empty(10_000));

    let large_tx = Arc::new(LargeTransactionDetector::with_threshold_sats(1_000_000_000));
    let detector_engine = Arc::new(tokio::sync::Mutex::new(DetectorEngine::new(vec![large_tx])));

    let replay_config = ReplayConfig {
        batch_size: 5,
        concurrency: 1,
        checkpoint_interval: 5,
        max_range: 1000,
        tx_cache_limit: 1000,
        db_concurrency: 1,
        sovereign_only: true,
        api_enabled: true,
    };

    let engine =
        HistoricalReplayEngine::new(rpc, storage.clone(), detector_engine, None, replay_config);

    // Run 1: blocks 100..=tip_height
    let job1 = engine
        .create_replay_job(100, tip_height)
        .await
        .expect("job1");
    let res1 = engine.run_job(job1.id).await.expect("run job1");
    assert_eq!(res1.status, obschain_core::ReplayJobStatus::Completed);

    let events_run1 = storage.list_events(100, 0).await.expect("events after 1");
    let count1 = events_run1.len();
    assert!(count1 > 0);

    // Run 2: EXACT SAME RANGE 100..=tip_height
    let job2 = engine
        .create_replay_job(100, tip_height)
        .await
        .expect("job2");
    let res2 = engine.run_job(job2.id).await.expect("run job2");
    assert_eq!(res2.status, obschain_core::ReplayJobStatus::Completed);

    let events_run2 = storage.list_events(100, 0).await.expect("events after 2");
    let count2 = events_run2.len();

    // Idempotency guarantee: Replay job count became 2, but logical events count must NOT double!
    assert_eq!(
        count1, count2,
        "Idempotency violation: running the same historical block range twice created duplicate events!"
    );
}

#[tokio::test]
async fn test_regtest_replay_checkpoint_and_resume() {
    let harness = match RegtestHarness::start().await {
        Some(h) => h,
        None => return,
    };

    harness.mine_blocks(120);

    let tip_info: serde_json::Value =
        serde_json::from_str(&harness.cli(&["getblockchaininfo"])).unwrap();
    let tip_height = tip_info["blocks"].as_u64().unwrap();
    assert!(tip_height >= 120);

    let rpc = Arc::new(harness.rpc_client());
    let storage = Storage::from(InMemoryStorage::new_empty(10_000));

    let large_tx = Arc::new(LargeTransactionDetector::with_threshold_sats(1_000_000_000));
    let detector_engine = Arc::new(tokio::sync::Mutex::new(DetectorEngine::new(vec![large_tx])));

    // Configure checkpoint interval to 5 blocks
    let replay_config = ReplayConfig {
        batch_size: 5,
        concurrency: 1,
        checkpoint_interval: 5,
        max_range: 1000,
        tx_cache_limit: 1000,
        db_concurrency: 1,
        sovereign_only: true,
        api_enabled: true,
    };

    let engine =
        HistoricalReplayEngine::new(rpc, storage.clone(), detector_engine, None, replay_config);

    let start_h = 100;
    let end_h = 120; // 21 blocks
    let job = engine
        .create_replay_job(start_h, end_h)
        .await
        .expect("create job");

    // Pause replay after 100ms
    let engine_clone = Arc::new(engine);
    let job_id = job.id;
    let engine_spawn = engine_clone.clone();

    let handle = tokio::spawn(async move {
        let _ = engine_spawn.run_job(job_id).await;
    });

    // Request pause
    tokio::time::sleep(Duration::from_millis(10)).await;
    let _ = engine_clone.pause_replay(job_id).await;
    let _ = handle.await;

    // Check intermediate state
    let intermediate_job = storage.get_job(job_id).await.unwrap().unwrap();
    assert!(intermediate_job.current_height >= start_h);
    let checkpoint = storage.get_latest_checkpoint(job_id).await.unwrap();
    if let Some(cp) = checkpoint {
        assert!(cp.completed_height >= start_h);
    }

    // Now resume replay to completion
    let completed_job = engine_clone
        .resume_replay(job_id)
        .await
        .expect("resume replay");
    assert_eq!(
        completed_job.status,
        obschain_core::ReplayJobStatus::Completed
    );
    assert_eq!(completed_job.current_height, end_h);
}

#[tokio::test]
async fn test_regtest_live_vs_replay_equivalence() {
    let harness = match RegtestHarness::start().await {
        Some(h) => h,
        None => return,
    };

    // 1. Setup deterministic regtest chain
    harness.mine_blocks(105);

    // 2. Perform a transaction
    let recipient = harness.cli(&["-rpcwallet=testwallet", "getnewaddress"]);
    let txid = harness.cli(&["-rpcwallet=testwallet", "sendtoaddress", &recipient, "30"]);
    let mined = harness.mine_blocks(1);
    let block_hash = mined[0].clone();

    let block_info: serde_json::Value =
        serde_json::from_str(&harness.cli(&["getblock", &block_hash, "1"])).unwrap();
    let block_height = block_info["height"].as_u64().unwrap();
    let block_time_epoch = block_info["time"].as_i64().unwrap();
    let block_time = DateTime::<Utc>::from_timestamp(block_time_epoch, 0).unwrap();

    // 3. Live observation simulation
    let large_tx_detector = Arc::new(LargeTransactionDetector::with_threshold_sats(
        1_000_000_000, // 10 BTC
    ));
    let mut live_engine = DetectorEngine::new(vec![large_tx_detector.clone()]);

    let live_tx_obs = TransactionObservation {
        txid: txid.clone(),
        timestamp: block_time,
        block_hash: Some(block_hash.clone()),
        block_height: Some(block_height),
        fee_sats: 10_000,
        size: 250,
        weight: 1000,
        vsize: 250,
        fee_rate_sat_vb: Some(40.0),
        total_input_sats: 5_000_000_000, // 50 BTC coinbase input
        total_output_sats: 4_999_990_000,
        input_count: 1,
        output_count: 2,
        inputs: vec![],
        outputs: vec![],
        is_rbf: false,
        confirmed: true,
        source: None,
    };

    let live_events = live_engine.process_observation(Observation::Transaction(live_tx_obs));
    assert_eq!(live_events.len(), 1);
    let live_event = &live_events[0];
    assert_eq!(live_event.event_type, EventType::LargeTransfer);
    assert_eq!(live_event.txid.as_deref(), Some(txid.as_str()));

    // 4. Historical replay over this block
    let rpc = Arc::new(harness.rpc_client());
    let replay_storage = Storage::from(InMemoryStorage::new_empty(10_000));
    let replay_detector_engine = Arc::new(tokio::sync::Mutex::new(DetectorEngine::new(vec![
        large_tx_detector,
    ])));

    let replay_config = ReplayConfig {
        batch_size: 5,
        concurrency: 1,
        checkpoint_interval: 5,
        max_range: 1000,
        tx_cache_limit: 1000,
        db_concurrency: 1,
        sovereign_only: true,
        api_enabled: true,
    };

    let replay_engine = HistoricalReplayEngine::new(
        rpc,
        replay_storage.clone(),
        replay_detector_engine,
        None,
        replay_config,
    );

    let job = replay_engine
        .create_replay_job(block_height, block_height)
        .await
        .expect("create replay job");
    let completed = replay_engine.run_job(job.id).await.expect("run replay job");
    assert_eq!(completed.status, obschain_core::ReplayJobStatus::Completed);

    let replayed_events = replay_storage
        .list_events(100, 0)
        .await
        .expect("get replayed events");
    assert_eq!(
        replayed_events.len(),
        2,
        "Expected 2 large transfers in block: 50 BTC coinbase + 30 BTC user transfer"
    );
    let replayed_event = replayed_events
        .iter()
        .find(|e| e.txid.as_deref() == Some(txid.as_str()))
        .expect("find replayed event matching user txid");

    // 5. Compare Equivalence:
    // Logical event type, txid, block height, severity must match!
    assert_eq!(replayed_event.event_type, live_event.event_type);
    assert_eq!(replayed_event.txid, live_event.txid);
    assert_eq!(replayed_event.block_height, live_event.block_height);
    assert_eq!(replayed_event.severity, live_event.severity);

    // Logical identity: deterministic UUID v5 preserves identical identity for the same logical event!
    assert_eq!(
        replayed_event.id,
        live_event.deterministic_id(),
        "Deterministic event identity must match between live and replay!"
    );

    // Observation mode explicitly distinguishes provenance
    assert_eq!(
        replayed_event.observation_mode,
        ObservationMode::HistoricalReplay
    );
    assert_eq!(replayed_event.replay_job_id, Some(job.id));
}
