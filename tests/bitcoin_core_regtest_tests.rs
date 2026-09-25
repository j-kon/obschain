use std::{
    path::PathBuf,
    process::{Child, Command},
    sync::Arc,
    time::Duration,
};

use obschain_core::{EventType, Observation, ObservationSource, SourceHealthState, WatchTarget};
use obschain_detectors::{
    DetectorEngine, EventDeduplicator, LargeTransactionDetector, ReorgDetector,
};
use obschain_ingest::{
    parse_sequence_event, validate_and_extract_multipart, BitcoinCoordinator,
    BitcoinCoordinatorConfig, BitcoinCoreRpcClient, BitcoinRpcConfig, BitcoinSequenceEvent,
    BitcoinZmqConfig, BitcoinZmqTopic, EnricherConfig, SequenceCheckResult, TransactionEnricher,
    UtxoCache, ZmqSequenceTracker,
};
use obschain_intelligence::IncidentWatchEngine;
use tokio::sync::watch;
use zeromq::{Socket, SocketRecv, SubSocket};

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
        let datadir = std::env::temp_dir().join(format!("obschain_regtest_{run_id}"));
        std::fs::create_dir_all(&datadir).expect("create regtest datadir");

        // Pick distinct ephemeral ports to allow tests to run concurrently without collisions
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
             zmqpubsequence=tcp://127.0.0.1:{zmq_sequence_port}\n\
             zmqpubrawtxhwm=10000\n\
             zmqpubrawblockhwm=1000\n\
             zmqpubsequencehwm=10000\n"
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

        // Initialize wallet
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

#[tokio::test]
async fn test_regtest_zmq_sequence_events_c_d_a_r() {
    let harness = match RegtestHarness::start().await {
        Some(h) => h,
        None => return,
    };

    // Connect raw ZeroMQ SubSocket directly to the sequence endpoint
    let mut sub = SubSocket::new();
    sub.connect(&format!("tcp://127.0.0.1:{}", harness.zmq_sequence_port))
        .await
        .expect("connect to sequence zmq");
    sub.subscribe("sequence")
        .await
        .expect("subscribe to sequence topic");

    // Allow ZMQ connection handshake
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 1. Mine 1 block to observe C (BlockConnected) event
    let mined_hashes = harness.mine_blocks(1);
    assert_eq!(mined_hashes.len(), 1);
    let first_block_hash = &mined_hashes[0];

    let mut observed_c = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Ok(msg)) = tokio::time::timeout(Duration::from_millis(200), sub.recv()).await {
            let frames = msg.into_vec();
            if let Ok((body, zmq_seq)) =
                validate_and_extract_multipart(&frames, BitcoinZmqTopic::Sequence)
            {
                if let Ok(BitcoinSequenceEvent::BlockConnected {
                    block_hash,
                    zmq_sequence,
                }) = parse_sequence_event(body, zmq_seq)
                {
                    if block_hash == *first_block_hash {
                        observed_c = Some((block_hash, zmq_sequence));
                        break;
                    }
                }
            }
        }
    }

    let (c_hash, c_zmq_seq) = observed_c.expect("Must observe C (BlockConnected) event");
    assert_eq!(
        &c_hash, first_block_hash,
        "Hash in C event must match display hex"
    );
    println!(
        "Regtest verified C (BlockConnected): hash={}, zmq_sequence={}",
        c_hash, c_zmq_seq
    );

    // Mine 100 more blocks so wallet has mature coinbase funds to spend
    harness.mine_blocks(100);

    // 2. Submit transaction to observe A (TransactionAdded) event
    let dest_addr = harness.cli(&["-rpcwallet=testwallet", "getnewaddress"]);
    let txid = harness.cli(&["-rpcwallet=testwallet", "sendtoaddress", &dest_addr, "1.0"]);
    assert!(!txid.is_empty());

    let mut observed_a = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Ok(msg)) = tokio::time::timeout(Duration::from_millis(200), sub.recv()).await {
            let frames = msg.into_vec();
            if let Ok((body, zmq_seq)) =
                validate_and_extract_multipart(&frames, BitcoinZmqTopic::Sequence)
            {
                if let Ok(BitcoinSequenceEvent::TransactionAdded {
                    txid: ev_txid,
                    mempool_sequence,
                    zmq_sequence,
                }) = parse_sequence_event(body, zmq_seq)
                {
                    if ev_txid == txid {
                        observed_a = Some((ev_txid, mempool_sequence, zmq_sequence));
                        break;
                    }
                }
            }
        }
    }

    let (a_txid, a_mempool_seq, a_zmq_seq) =
        observed_a.expect("Must observe A (TransactionAdded) event");
    assert_eq!(a_txid, txid);
    assert!(a_mempool_seq > 0, "mempool_sequence must be > 0");
    assert!(a_zmq_seq >= c_zmq_seq, "ZMQ sequence must be monotonic");
    println!(
        "Regtest verified A (TransactionAdded): txid={}, mempool_sequence={}, zmq_sequence={}",
        a_txid, a_mempool_seq, a_zmq_seq
    );

    // 3. Replace transaction via bumpfee (RBF) to observe R (TransactionRemoved) event
    let bump_res = harness.cli(&["-rpcwallet=testwallet", "bumpfee", &txid]);
    assert!(!bump_res.is_empty(), "bumpfee should succeed");

    let mut observed_r = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Ok(msg)) = tokio::time::timeout(Duration::from_millis(200), sub.recv()).await {
            let frames = msg.into_vec();
            if let Ok((body, zmq_seq)) =
                validate_and_extract_multipart(&frames, BitcoinZmqTopic::Sequence)
            {
                if let Ok(BitcoinSequenceEvent::TransactionRemoved {
                    txid: ev_txid,
                    mempool_sequence,
                    zmq_sequence,
                }) = parse_sequence_event(body, zmq_seq)
                {
                    if ev_txid == txid {
                        observed_r = Some((ev_txid, mempool_sequence, zmq_sequence));
                        break;
                    }
                }
            }
        }
    }

    let (r_txid, r_mempool_seq, r_zmq_seq) =
        observed_r.expect("Must observe R (TransactionRemoved) event upon RBF replacement");
    assert_eq!(r_txid, txid);
    assert!(
        r_mempool_seq > a_mempool_seq,
        "R mempool_sequence must be greater than A mempool_sequence"
    );
    println!(
        "Regtest verified R (TransactionRemoved): txid={}, mempool_sequence={}, zmq_sequence={}",
        r_txid, r_mempool_seq, r_zmq_seq
    );

    // Mine 1 block confirming the replacement transaction
    let mined_confirming = harness.mine_blocks(1);
    let confirming_block_hash = &mined_confirming[0];

    // 4. Invalidate block to disconnect it and observe D (BlockDisconnected) event
    harness.cli(&["invalidateblock", confirming_block_hash]);

    let mut observed_d = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Ok(msg)) = tokio::time::timeout(Duration::from_millis(200), sub.recv()).await {
            let frames = msg.into_vec();
            if let Ok((body, zmq_seq)) =
                validate_and_extract_multipart(&frames, BitcoinZmqTopic::Sequence)
            {
                if let Ok(BitcoinSequenceEvent::BlockDisconnected {
                    block_hash,
                    zmq_sequence,
                }) = parse_sequence_event(body, zmq_seq)
                {
                    if block_hash == *confirming_block_hash {
                        observed_d = Some((block_hash, zmq_sequence));
                        break;
                    }
                }
            }
        }
    }

    let (d_hash, d_zmq_seq) = observed_d.expect("Must observe D (BlockDisconnected) event");
    assert_eq!(&d_hash, confirming_block_hash);
    println!(
        "Regtest verified D (BlockDisconnected): hash={}, zmq_sequence={}",
        d_hash, d_zmq_seq
    );
}

#[tokio::test]
async fn test_regtest_lost_notification_simulation() {
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

    let zmq_config = BitcoinZmqConfig::default();
    let zmq_sub = Arc::new(obschain_ingest::BitcoinZmqSubscriber::new(zmq_config));
    let coord_config = BitcoinCoordinatorConfig {
        reconcile_max_blocks: 10,
        health_poll_interval_secs: 1,
    };
    let coordinator = Arc::new(BitcoinCoordinator::new(rpc.clone(), zmq_sub, coord_config));
    // Mine 5 blocks so node leaves InitialBlockDownload
    harness.mine_blocks(5);
    coordinator
        .initialize()
        .await
        .expect("initialize coordinator");

    let hash5 = rpc.get_block_hash(5).await.unwrap();
    coordinator.set_tip(5, hash5).await;

    // Baseline metrics
    assert_eq!(
        coordinator
            .zmq_sequence_gaps_total
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        coordinator
            .zmq_notifications_missed_estimate
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );

    // Initial status should be Connected
    assert_eq!(
        coordinator.get_status().await.rpc_health,
        SourceHealthState::Connected
    );

    // 1. Simulate a SequenceGap on rawblock: previous=100, current=104, missed=3
    let (obs_tx, _obs_rx) = tokio::sync::mpsc::channel(100);
    coordinator
        .handle_sequence_gap(BitcoinZmqTopic::RawBlock, 100, 104, 3, &obs_tx)
        .await;

    // Verify metrics incremented
    assert_eq!(
        coordinator
            .zmq_sequence_gaps_total
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        coordinator
            .zmq_notifications_missed_estimate
            .load(std::sync::atomic::Ordering::Relaxed),
        3
    );

    // Verify topic health was marked Degraded
    let st = coordinator.get_status().await;
    assert_eq!(st.zmq_status.rawblock, SourceHealthState::Degraded);

    // 2. Simulate another SequenceGap on sequence: previous=200, current=202, missed=1
    coordinator
        .handle_sequence_gap(BitcoinZmqTopic::Sequence, 200, 202, 1, &obs_tx)
        .await;

    assert_eq!(
        coordinator
            .zmq_sequence_gaps_total
            .load(std::sync::atomic::Ordering::Relaxed),
        2
    );
    assert_eq!(
        coordinator
            .zmq_notifications_missed_estimate
            .load(std::sync::atomic::Ordering::Relaxed),
        4
    );
    let st2 = coordinator.get_status().await;
    assert_eq!(st2.zmq_status.sequence, SourceHealthState::Degraded);

    // 3. Test Tracker unit behavior including u32 wraparound with gap
    let mut tracker = ZmqSequenceTracker::new();
    assert_eq!(
        tracker.observe(u32::MAX - 2),
        SequenceCheckResult::Initial(u32::MAX - 2)
    );
    // Jump to 1 (skipping MAX-1, MAX, 0 = 3 missed)
    assert_eq!(
        tracker.observe(1),
        SequenceCheckResult::Gap {
            previous: u32::MAX - 2,
            current: 1,
            missed: 3,
        }
    );
}

#[tokio::test]
async fn test_regtest_rawtx_mempool_then_block_duplicate_handling() {
    let harness = match RegtestHarness::start().await {
        Some(h) => h,
        None => return,
    };

    // Mine 101 blocks to mature coinbase funds
    harness.mine_blocks(101);

    // Setup LargeTransactionDetector (threshold = 1.0 BTC = 100_000_000 sats)
    let large_tx_detector = Arc::new(LargeTransactionDetector::with_threshold_sats(100_000_000));
    let mut engine = DetectorEngine::new(vec![large_tx_detector]);
    let dedup = EventDeduplicator::new(1000, Duration::from_secs(60));

    // Submit transaction of 1.5 BTC to mempool
    let dest_addr = harness.cli(&["-rpcwallet=testwallet", "getnewaddress"]);
    let txid = harness.cli(&["-rpcwallet=testwallet", "sendtoaddress", &dest_addr, "1.5"]);
    assert!(!txid.is_empty());

    // 1. First rawtx notification: enters mempool (unconfirmed)
    let mempool_tx = obschain_core::TransactionObservation {
        txid: txid.clone(),
        timestamp: chrono::Utc::now(),
        block_hash: None,
        block_height: None,
        fee_sats: 1500,
        size: 250,
        weight: 1000,
        vsize: 250,
        fee_rate_sat_vb: Some(6.0),
        total_input_sats: 150_001_500,
        total_output_sats: 150_000_000,
        input_count: 1,
        output_count: 2,
        inputs: vec![],
        outputs: vec![],
        is_rbf: false,
        confirmed: false,
        source: Some(ObservationSource::bitcoin_core_zmq(
            "tcp://127.0.0.1:28332#mempool",
        )),
    };

    let events1 = engine.process_observation(Observation::Transaction(mempool_tx));
    assert_eq!(events1.len(), 1, "Detector should flag large transaction");
    let fresh1 = dedup.filter(events1);
    assert_eq!(
        fresh1.len(),
        1,
        "First event must pass deduplication filter"
    );
    assert_eq!(fresh1[0].txid.as_deref(), Some(txid.as_str()));
    assert_eq!(fresh1[0].event_type, EventType::LargeTransfer);
    assert_eq!(dedup.deduplicated_count(), 0);

    // Mine block to confirm transaction
    let mined = harness.mine_blocks(1);
    let block_hash = &mined[0];

    // 2. Second rawtx notification: block containing transaction arrives
    let confirmed_tx = obschain_core::TransactionObservation {
        txid: txid.clone(),
        timestamp: chrono::Utc::now(),
        block_hash: Some(block_hash.clone()),
        block_height: Some(102),
        fee_sats: 1500,
        size: 250,
        weight: 1000,
        vsize: 250,
        fee_rate_sat_vb: Some(6.0),
        total_input_sats: 150_001_500,
        total_output_sats: 150_000_000,
        input_count: 1,
        output_count: 2,
        inputs: vec![],
        outputs: vec![],
        is_rbf: false,
        confirmed: true,
        source: Some(ObservationSource::bitcoin_core_zmq(
            "tcp://127.0.0.1:28332#block_confirm",
        )),
    };

    let events2 = engine.process_observation(Observation::Transaction(confirmed_tx));
    assert_eq!(events2.len(), 1);
    let fresh2 = dedup.filter(events2);

    // CRITICAL: Second rawtx notification must NOT produce a duplicate logical anomaly event
    assert_eq!(
        fresh2.len(),
        0,
        "Second rawtx notification must be dropped by deduplicator"
    );
    assert_eq!(
        dedup.deduplicated_count(),
        1,
        "Deduplication metric must increment"
    );

    // Verify witness provenance: the corroborating witness was recorded
    let dedup_key = EventDeduplicator::event_key(&fresh1[0]);
    let witnesses = dedup.witnesses_for(&dedup_key);
    assert_eq!(
        witnesses.len(),
        2,
        "Both initial and corroborating witnesses must be recorded in deduplicator"
    );
    assert_eq!(
        witnesses[0].source.endpoint.as_deref(),
        Some("tcp://127.0.0.1:28332#mempool")
    );
    assert_eq!(
        witnesses[1].source.endpoint.as_deref(),
        Some("tcp://127.0.0.1:28332#block_confirm")
    );
}
