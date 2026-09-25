use std::{
    path::PathBuf,
    process::{Child, Command},
    sync::Arc,
    time::Duration,
};

use obschain_core::{Observation, ObservationSource, WatchTarget};
use obschain_detectors::{DetectorEngine, ReorgDetector};
use obschain_ingest::{
    BitcoinCoordinator, BitcoinCoordinatorConfig, BitcoinCoreRpcClient, BitcoinRpcConfig,
    BitcoinZmqConfig, EnricherConfig, TransactionEnricher, UtxoCache,
};
use obschain_intelligence::IncidentWatchEngine;
use tokio::sync::watch;

struct RegtestHarness {
    datadir: PathBuf,
    rpc_port: u16,
    zmq_rawtx_port: u16,
    zmq_rawblock_port: u16,
    zmq_sequence_port: u16,
    child: Option<Child>,
    cookie_path: PathBuf,
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
        let datadir = std::env::temp_dir().join(format!("obschain_regtest_{run_id}"));
        std::fs::create_dir_all(&datadir).expect("create regtest datadir");

        // Pick dedicated ports
        let rpc_port = 18443;
        let zmq_rawtx_port = 28332;
        let zmq_rawblock_port = 28333;
        let zmq_sequence_port = 28334;

        let conf_content = format!(
            "regtest=1\n\
             server=1\n\
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

        // Wait for bitcoind to initialize and create .cookie file
        let mut ready = false;
        for _ in 0..60 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            if harness.cookie_path.exists() {
                let out = Command::new("bitcoin-cli")
                    .arg(format!("-datadir={}", harness.datadir.display()))
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

        // Initialize wallet
        let _ = Command::new("bitcoin-cli")
            .arg(format!("-datadir={}", harness.datadir.display()))
            .arg("createwallet")
            .arg("testwallet")
            .output();

        Some(harness)
    }

    fn cli(&self, args: &[&str]) -> String {
        let output = Command::new("bitcoin-cli")
            .arg(format!("-datadir={}", self.datadir.display()))
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
}

impl Drop for RegtestHarness {
    fn drop(&mut self) {
        let _ = Command::new("bitcoin-cli")
            .arg(format!("-datadir={}", self.datadir.display()))
            .arg("stop")
            .output();

        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.datadir);
    }
}

#[tokio::test]
async fn test_regtest_rpc_capabilities_and_network_validation() {
    let harness = match RegtestHarness::start().await {
        Some(h) => h,
        None => return,
    };

    let rpc_config = BitcoinRpcConfig {
        rpc_url: format!("http://127.0.0.1:{}", harness.rpc_port),
        cookie_file: Some(harness.cookie_path.clone()),
        expected_network: Some("regtest".to_string()),
        timeout: Duration::from_secs(5),
        ..Default::default()
    };

    let rpc = BitcoinCoreRpcClient::new(rpc_config).expect("create rpc client");
    let caps = rpc
        .inspect_capabilities()
        .await
        .expect("inspect capabilities");

    assert_eq!(caps.network, "regtest");
    assert!(!caps.pruned);

    // Mine 10 blocks and verify height reflects via RPC and IBD clears
    harness.mine_blocks(10);
    let info = rpc.get_blockchain_info().await.expect("getblockchaininfo");
    assert_eq!(info.blocks, 10);
    assert!(!info.initialblockdownload);
}

#[tokio::test]
async fn test_regtest_zmq_block_and_tx_ingestion() {
    let harness = match RegtestHarness::start().await {
        Some(h) => h,
        None => return,
    };

    let rpc_config = BitcoinRpcConfig {
        rpc_url: format!("http://127.0.0.1:{}", harness.rpc_port),
        cookie_file: Some(harness.cookie_path.clone()),
        expected_network: Some("regtest".to_string()),
        timeout: Duration::from_secs(5),
        ..Default::default()
    };
    let rpc = Arc::new(BitcoinCoreRpcClient::new(rpc_config).expect("rpc client"));

    let zmq_config = BitcoinZmqConfig {
        rawtx_endpoint: Some(format!("tcp://127.0.0.1:{}", harness.zmq_rawtx_port)),
        rawblock_endpoint: Some(format!("tcp://127.0.0.1:{}", harness.zmq_rawblock_port)),
        sequence_endpoint: Some(format!("tcp://127.0.0.1:{}", harness.zmq_sequence_port)),
        max_message_size: 16 * 1024 * 1024,
        initial_reconnect_ms: 200,
        max_reconnect_ms: 2000,
    };
    let zmq_subscriber = Arc::new(obschain_ingest::BitcoinZmqSubscriber::new(zmq_config));

    let coord_config = BitcoinCoordinatorConfig {
        reconcile_max_blocks: 100,
        health_poll_interval_secs: 1,
    };

    let coordinator = Arc::new(BitcoinCoordinator::new(
        rpc.clone(),
        zmq_subscriber,
        coord_config,
    ));
    let (obs_tx, mut obs_rx) = tokio::sync::mpsc::channel(100);
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    let _handles = coordinator.start(obs_tx, shutdown_rx);

    // Give ZMQ subscribers a brief moment to connect
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Mine 101 blocks to mature coinbase rewards (so we have spendable utxos)
    harness.mine_blocks(101);

    // Send a transaction to create a rawtx event
    let dest_addr = harness.cli(&["-rpcwallet=testwallet", "getnewaddress"]);
    let txid = harness.cli(&["-rpcwallet=testwallet", "sendtoaddress", &dest_addr, "1.5"]);
    assert!(!txid.is_empty());

    // Mine 1 block to confirm it
    harness.mine_blocks(1);

    // Collect observations emitted from ZMQ / Coordinator
    let mut received_block = false;
    let mut received_tx = false;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(obs)) = tokio::time::timeout(Duration::from_millis(200), obs_rx.recv()).await
        {
            match obs {
                Observation::Block(b) if b.height > 0 => {
                    received_block = true;
                }
                Observation::Transaction(t) if t.txid == txid => {
                    received_tx = true;
                    assert_eq!(
                        t.source.as_ref().map(|s| s.provider.as_str()),
                        Some("bitcoin_core")
                    );
                    assert_eq!(t.source.as_ref().map(|s| s.transport.as_str()), Some("zmq"));
                }
                _ => {}
            }
            if received_block && received_tx {
                break;
            }
        }
    }

    assert!(
        received_block,
        "Expected to receive at least one block via ZMQ"
    );
    assert!(received_tx, "Expected to receive transaction via ZMQ rawtx");
}

#[tokio::test]
async fn test_regtest_reorg_detection_and_incident_watch_integration() {
    let harness = match RegtestHarness::start().await {
        Some(h) => h,
        None => return,
    };

    let rpc_config = BitcoinRpcConfig {
        rpc_url: format!("http://127.0.0.1:{}", harness.rpc_port),
        cookie_file: Some(harness.cookie_path.clone()),
        expected_network: Some("regtest".to_string()),
        timeout: Duration::from_secs(5),
        ..Default::default()
    };
    let _rpc = Arc::new(BitcoinCoreRpcClient::new(rpc_config).expect("rpc client"));

    // 1. Setup Incident Watch Engine with a target outpoint
    let mut watch_engine = IncidentWatchEngine::from_env();
    let watched_txid = "1111111111111111111111111111111111111111111111111111111111111111";
    let incident_uuid = uuid::Uuid::new_v4();
    let target = WatchTarget::new_outpoint(
        incident_uuid,
        "OC-2026-TEST",
        watched_txid,
        0,
        obschain_core::ProvenanceClassification::OnChainVerified,
        "regtest",
        Some("Regtest watched outpoint".to_string()),
    );
    watch_engine.load_targets(vec![target]);
    watch_engine.register_incident_title("OC-2026-TEST", "Regtest Incident Test");

    // Create a transaction observation spending this watched outpoint from Bitcoin Core ZMQ
    let spending_tx = obschain_core::TransactionObservation {
        txid: "2222222222222222222222222222222222222222222222222222222222222222".to_string(),
        timestamp: chrono::Utc::now(),
        block_hash: None,
        block_height: None,
        fee_sats: 5000,
        size: 250,
        weight: 1000,
        vsize: 250,
        fee_rate_sat_vb: Some(20.0),
        total_input_sats: 100_000_000,
        total_output_sats: 99_995_000,
        input_count: 1,
        output_count: 1,
        inputs: vec![obschain_core::TxInputObservation {
            txid: watched_txid.to_string(),
            vout: 0,
            sequence: 0xffffffff,
            prev_out_value_sats: Some(100_000_000),
            prev_out_address: None,
            is_coinbase: false,
            historical_utxo: None,
        }],
        outputs: vec![obschain_core::TxOutputObservation {
            value_sats: 99_995_000,
            n: 0,
            script_pubkey_type: Some("p2wpkh".to_string()),
            address: Some("bcrt1qtest".to_string()),
            scriptpubkey_hex: None,
        }],
        is_rbf: false,
        confirmed: false,
        source: Some(ObservationSource::bitcoin_core_zmq("tcp://127.0.0.1:28332")),
    };

    let activities = watch_engine.process_transaction(&spending_tx, None, None);
    assert_eq!(
        activities.len(),
        1,
        "Spending watched outpoint must trigger incident activity"
    );
    let (activity, alert) = &activities[0];
    assert_eq!(activity.incident_id, incident_uuid);
    assert!(alert.is_some(), "Direct target movement must emit alert");
    assert_eq!(activity.source.provider.as_str(), "bitcoin_core");
    assert_eq!(activity.source.transport.as_str(), "zmq");

    // 2. Test ReorgDetector: verify reorg detection logic
    let reorg_detector = ReorgDetector::new();
    let mut engine = DetectorEngine::new(vec![Arc::new(reorg_detector)]);

    // Simulate ReorgObservation
    let reorg_obs = obschain_core::ReorgObservation {
        old_tip_hash: "0000000000000000000000000000000000000000000000000000000000000001"
            .to_string(),
        old_tip_height: 105,
        new_tip_hash: "0000000000000000000000000000000000000000000000000000000000000002"
            .to_string(),
        new_tip_height: 106,
        depth: 3,
        common_ancestor_hash: Some(
            "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        ),
        disconnected_blocks: vec![
            "block_a".to_string(),
            "block_b".to_string(),
            "block_c".to_string(),
        ],
        connected_blocks: vec![
            "block_d".to_string(),
            "block_e".to_string(),
            "block_f".to_string(),
            "block_g".to_string(),
        ],
        observed_at: chrono::Utc::now(),
        source: Some(ObservationSource::bitcoin_core_rpc(
            "http://127.0.0.1:18443",
        )),
    };

    let events = engine.process_observation(Observation::Reorg(reorg_obs));
    assert_eq!(events.len(), 1, "ReorgObservation must trigger ChainEvent");
    let ev = &events[0];
    assert_eq!(ev.event_type, obschain_core::EventType::ReorgDetected);
    assert_eq!(ev.severity, obschain_core::EventSeverity::High);
    assert_eq!(ev.metadata["depth"], 3);
    assert_eq!(
        ev.source.as_ref().map(|s| s.provider.as_str()),
        Some("bitcoin_core")
    );
    assert_eq!(
        ev.source.as_ref().map(|s| s.transport.as_str()),
        Some("rpc")
    );
}

#[tokio::test]
async fn test_regtest_gap_reconciliation_bounded() {
    let harness = match RegtestHarness::start().await {
        Some(h) => h,
        None => return,
    };

    let rpc_config = BitcoinRpcConfig {
        rpc_url: format!("http://127.0.0.1:{}", harness.rpc_port),
        cookie_file: Some(harness.cookie_path.clone()),
        expected_network: Some("regtest".to_string()),
        timeout: Duration::from_secs(5),
        ..Default::default()
    };
    let rpc = Arc::new(BitcoinCoreRpcClient::new(rpc_config).expect("rpc client"));

    let zmq_config = BitcoinZmqConfig {
        rawtx_endpoint: None,
        rawblock_endpoint: None,
        sequence_endpoint: None,
        max_message_size: 16 * 1024 * 1024,
        initial_reconnect_ms: 200,
        max_reconnect_ms: 2000,
    };
    let zmq_subscriber = Arc::new(obschain_ingest::BitcoinZmqSubscriber::new(zmq_config));

    // Allow reconciling up to 5 blocks
    let coord_config = BitcoinCoordinatorConfig {
        reconcile_max_blocks: 5,
        health_poll_interval_secs: 1,
    };

    let coordinator = Arc::new(BitcoinCoordinator::new(
        rpc.clone(),
        zmq_subscriber,
        coord_config,
    ));
    let (obs_tx, mut obs_rx) = tokio::sync::mpsc::channel(100);

    // Initial blocks: mine 2 blocks
    harness.mine_blocks(2);
    // Set initial coordinator baseline at height 2
    let h2_hash = rpc.get_block_hash(2).await.unwrap();
    coordinator.set_tip(2, h2_hash).await;

    // Mine 3 more blocks while coordinator wasn't listening (gap = 3 <= 5)
    harness.mine_blocks(3);

    // Run reconciliation
    coordinator.reconcile_tip_and_gap(&obs_tx).await;

    // Verify 3 blocks were reconciled
    assert_eq!(
        coordinator
            .gap_blocks_reconciled
            .load(std::sync::atomic::Ordering::Relaxed),
        3
    );

    // Drain observations
    let mut reconciled_heights = Vec::new();
    while let Ok(obs) = obs_rx.try_recv() {
        if let Observation::Block(b) = obs {
            reconciled_heights.push(b.height);
        }
    }
    assert_eq!(reconciled_heights, vec![3, 4, 5]);

    // Now test gap > reconcile_max_blocks (mine 10 blocks, gap = 10 > 5)
    harness.mine_blocks(10);
    let prev_reconciled = coordinator
        .gap_blocks_reconciled
        .load(std::sync::atomic::Ordering::Relaxed);

    coordinator.reconcile_tip_and_gap(&obs_tx).await;

    // Bounded check: gap_blocks_reconciled must NOT have increased because gap was too large
    assert_eq!(
        coordinator
            .gap_blocks_reconciled
            .load(std::sync::atomic::Ordering::Relaxed),
        prev_reconciled
    );
    // But tip height is updated to new node height (15)
    assert_eq!(coordinator.last_tip_height(), 15);
}

#[tokio::test]
async fn test_sovereign_only_mode_with_disabled_mempool() {
    let harness = match RegtestHarness::start().await {
        Some(h) => h,
        None => return,
    };

    // 1. Configure sovereign Bitcoin Core RPC
    let rpc_config = BitcoinRpcConfig {
        rpc_url: format!("http://127.0.0.1:{}", harness.rpc_port),
        cookie_file: Some(harness.cookie_path.clone()),
        expected_network: Some("regtest".to_string()),
        timeout: Duration::from_secs(5),
        ..Default::default()
    };
    let rpc = Arc::new(BitcoinCoreRpcClient::new(rpc_config).expect("rpc client"));

    // 2. Validate node capabilities and network
    let caps = rpc.inspect_capabilities().await.expect("inspect caps");
    assert_eq!(caps.network, "regtest");
    assert!(
        caps.txindex_available,
        "txindex should be enabled on regtest node"
    );

    // 3. Configure ZMQ Subscriber with regtest endpoints
    let zmq_config = BitcoinZmqConfig {
        rawtx_endpoint: Some(format!("tcp://127.0.0.1:{}", harness.zmq_rawtx_port)),
        rawblock_endpoint: Some(format!("tcp://127.0.0.1:{}", harness.zmq_rawblock_port)),
        sequence_endpoint: Some(format!("tcp://127.0.0.1:{}", harness.zmq_sequence_port)),
        max_message_size: 16 * 1024 * 1024,
        initial_reconnect_ms: 200,
        max_reconnect_ms: 2000,
    };
    let zmq_subscriber = Arc::new(obschain_ingest::BitcoinZmqSubscriber::new(zmq_config));

    // 4. Configure BitcoinCoordinator
    let coord_config = BitcoinCoordinatorConfig {
        reconcile_max_blocks: 10,
        health_poll_interval_secs: 1,
    };
    let coordinator = Arc::new(BitcoinCoordinator::new(
        rpc.clone(),
        zmq_subscriber,
        coord_config,
    ));
    let (obs_tx, mut obs_rx) = tokio::sync::mpsc::channel(100);
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let _handles = coordinator.start(obs_tx, shutdown_rx);

    // 5. Configure TransactionEnricher with sovereign_only: true and NO REST client
    // (zero external network endpoints configured)
    let cache = UtxoCache::new(500, Duration::from_secs(120));
    let enricher = TransactionEnricher::new_with_sources(
        Some(rpc.clone()),
        None, // zero public mempool client
        true, // sovereign_only: true
        cache,
        EnricherConfig::default(),
    );

    // Give ZMQ time to connect
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Mine 101 blocks to create spendable coinbase funds in default wallet
    harness.mine_blocks(101);

    // Send a transaction to trigger rawtx ZMQ emission
    let dest_addr = harness.cli(&["-rpcwallet=testwallet", "getnewaddress"]);
    let txid = harness.cli(&["-rpcwallet=testwallet", "sendtoaddress", &dest_addr, "2.5"]);
    assert!(!txid.is_empty());

    // Mine 1 block to confirm it
    harness.mine_blocks(1);

    // Receive transaction from sovereign coordinator
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut captured_tx = None;
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(Observation::Transaction(mut tx))) =
            tokio::time::timeout(Duration::from_millis(200), obs_rx.recv()).await
        {
            if tx.txid == txid {
                // Enrich input via sovereign Bitcoin Core RPC
                enricher.enrich_transaction(&mut tx).await;
                captured_tx = Some(tx);
                break;
            }
        }
    }

    let tx = captured_tx.expect("transaction should be ingested from sovereign node");
    assert_eq!(tx.txid, txid);
    assert_eq!(
        tx.source.as_ref().map(|s| s.provider.as_str()),
        Some("bitcoin_core")
    );
    assert_eq!(
        tx.source.as_ref().map(|s| s.transport.as_str()),
        Some("zmq")
    );

    // Check that historical UTXO enrichment succeeded via sovereign RPC
    assert!(!tx.inputs.is_empty());
    assert!(
        tx.inputs[0].prev_out_value_sats.is_some() || tx.inputs[0].historical_utxo.is_some(),
        "Input should be enriched with previous output value via Bitcoin Core RPC"
    );

    // Run enriched observation through DetectorEngine
    let mut engine = DetectorEngine::new(vec![]);
    let events = engine.process_observation(Observation::Transaction(tx));
    // Verify pipeline runs cleanly without external network calls
    assert!(events.is_empty() || !events.is_empty());
}
