use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use obschain::{create_router, AppConfig, AppState};
use obschain_core::Observation;
use obschain_detectors::{DetectorEngine, LargeTransactionDetector, LongBlockIntervalDetector};
use obschain_ingest::{MempoolRestClient, MempoolRestConfig, MempoolWebSocketClient};
use obschain_storage::{EventRepository, InMemoryStorage};
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "info,obschain=debug,obschain_ingest=debug,tower_http=info".into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = AppConfig::from_env();
    let addr = config.socket_addr()?;

    info!(
        host = %config.host,
        port = %config.port,
        mempool_api = %config.mempool_api_url,
        mempool_ws = %config.mempool_ws_url,
        large_tx_threshold_sats = config.large_tx_threshold_sats,
        long_block_interval_secs = config.long_block_interval_seconds,
        "ObsChain backend initializing live Bitcoin observation engine"
    );

    // 1. Setup bounded storage and channels
    let storage = if config.mock_feed {
        InMemoryStorage::with_limit(config.event_store_limit)
    } else {
        InMemoryStorage::new_empty(config.event_store_limit)
    };

    // Bounded observation channel: protects against unbounded memory growth under high tx volume
    let (obs_tx, mut obs_rx) = mpsc::channel::<Observation>(1000);

    // Graceful shutdown coordination channel
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // 2. Setup Detectors and DetectorEngine
    let large_tx_detector = Arc::new(LargeTransactionDetector::with_threshold_sats(
        config.large_tx_threshold_sats,
    ));
    let long_interval_detector = Arc::new(LongBlockIntervalDetector::with_threshold_seconds(
        config.long_block_interval_seconds,
    ));

    let detectors: Vec<Arc<dyn obschain_detectors::Detector>> =
        vec![large_tx_detector, long_interval_detector];
    let mut engine = DetectorEngine::new(detectors.clone());

    // 3. Setup AppState for Axum HTTP and WebSocket API
    let (state, event_broadcaster) = AppState::new(storage.clone(), detectors, config.mock_feed);
    let app = create_router(state.clone());

    // 4. Mempool WebSocket ingestion supervisor
    let ws_client = MempoolWebSocketClient::new(config.mempool_ws_url.clone());
    let ws_connected = ws_client.connected_handle();
    let ws_sources = state.sources.clone();
    let mut ws_status_shutdown = shutdown_rx.clone();

    // WS connection status tracking worker
    tokio::spawn(async move {
        let mut last_state = false;
        while !*ws_status_shutdown.borrow() {
            let current = ws_connected.load(Ordering::Relaxed);
            if current != last_state {
                last_state = current;
                if let Ok(mut sources) = ws_sources.write() {
                    sources.mempool_websocket = if current {
                        "connected".to_string()
                    } else {
                        "connecting".to_string()
                    };
                }
            }
            tokio::select! {
                _ = ws_status_shutdown.changed() => break,
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
        }
    });

    // Spawn WebSocket ingestion supervisor
    let ws_obs_tx = obs_tx.clone();
    let ws_shutdown_rx = shutdown_rx.clone();
    tokio::spawn(async move {
        ws_client.run_supervisor(ws_obs_tx, ws_shutdown_rx).await;
    });

    // 5. Mempool REST poller for tip synchronization and fallback ingestion
    let rest_config = MempoolRestConfig {
        base_url: config.mempool_api_url.clone(),
        timeout: Duration::from_secs(10),
        connect_timeout: Duration::from_secs(5),
        max_retries: 3,
        max_response_bytes: 10 * 1024 * 1024,
    };

    let rest_client = match MempoolRestClient::new(rest_config) {
        Ok(c) => Some(c),
        Err(e) => {
            warn!(error = %e, "Failed to initialize MempoolRestClient");
            None
        }
    };

    if let Some(rest) = rest_client {
        let rest_sources = state.sources.clone();
        let rest_tip = state.tip_height.clone();
        let rest_obs_tx = obs_tx.clone();
        let mut rest_shutdown = shutdown_rx.clone();

        tokio::spawn(async move {
            let mut last_polled_height = 0u64;

            // Initial REST sync
            match rest.get_tip_height().await {
                Ok(height) => {
                    info!(tip_height = height, "Mempool REST client synced tip height");
                    rest_tip.store(height, Ordering::Relaxed);
                    last_polled_height = height;

                    if let Ok(mut sources) = rest_sources.write() {
                        sources.mempool_rest = "connected".to_string();
                    }

                    // Fetch current tip block details to establish baseline observation
                    if let Ok(hash) = rest.get_tip_hash().await {
                        if let Ok(block) = rest.get_block(&hash).await {
                            let obs = block.into_observation(rest.base_url());
                            let _ = rest_obs_tx.send(Observation::Block(obs)).await;
                        }
                    }

                    // Ingest recent mempool transactions
                    if let Ok(recent_txs) = rest.get_mempool_recent().await {
                        for tx in recent_txs {
                            let obs = tx.into_observation(rest.base_url());
                            let _ = rest_obs_tx.send(Observation::Transaction(obs)).await;
                        }
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Initial Mempool REST tip check failed");
                    if let Ok(mut sources) = rest_sources.write() {
                        sources.mempool_rest = "error".to_string();
                    }
                }
            }

            // Periodic REST health and tip verification loop (every 30 seconds)
            while !*rest_shutdown.borrow() {
                tokio::select! {
                    _ = rest_shutdown.changed() => break,
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {
                        match rest.get_tip_height().await {
                            Ok(height) => {
                                if let Ok(mut sources) = rest_sources.write() {
                                    sources.mempool_rest = "connected".to_string();
                                }

                                if height > last_polled_height {
                                    info!(new_height = height, prev_height = last_polled_height, "Bitcoin tip updated via REST check");
                                    rest_tip.store(height, Ordering::Relaxed);
                                    last_polled_height = height;

                                    if let Ok(hash) = rest.get_tip_hash().await {
                                        if let Ok(block) = rest.get_block(&hash).await {
                                            let obs = block.into_observation(rest.base_url());
                                            let _ = rest_obs_tx.send(Observation::Block(obs)).await;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(error = %e, "Mempool REST periodic health check failed");
                                if let Ok(mut sources) = rest_sources.write() {
                                    sources.mempool_rest = "error".to_string();
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    // 6. Detector pipeline worker: Observation channel -> Normalization -> Engine -> ChainEvent -> Storage & Broadcast
    let pipeline_storage = storage.clone();
    let pipeline_broadcaster = event_broadcaster.clone();
    let pipeline_events_detected = state.events_detected.clone();
    let pipeline_tip_height = state.tip_height.clone();
    let mut pipeline_shutdown = shutdown_rx.clone();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = pipeline_shutdown.changed() => {
                    if *pipeline_shutdown.borrow() {
                        info!("Detector pipeline worker shutting down, processing remaining queue");
                        break;
                    }
                }
                obs_opt = obs_rx.recv() => {
                    let Some(obs) = obs_opt else {
                        info!("Observation channel closed");
                        break;
                    };

                    match &obs {
                        Observation::Block(block) => {
                            pipeline_tip_height.store(block.height, Ordering::Relaxed);
                            info!(
                                height = block.height,
                                hash = %block.block_hash,
                                tx_count = block.tx_count,
                                interval_secs = ?block.interval_seconds,
                                "New Bitcoin block observed"
                            );
                        }
                        Observation::Transaction(tx) => {
                            tracing::debug!(
                                txid = %tx.txid,
                                total_output_sats = tx.total_output_sats,
                                fee_sats = tx.fee_sats,
                                "Bitcoin transaction observed"
                            );
                        }
                        Observation::Mempool(mp) => {
                            tracing::debug!(
                                tx_count = mp.count,
                                total_fee_sats = mp.total_fee_sats,
                                "Mempool backlog stats updated"
                            );
                        }
                    }

                    let detected_events = engine.process_observation(obs);
                    for event in detected_events {
                        info!(
                            id = %event.id,
                            event_type = ?event.event_type,
                            severity = ?event.severity,
                            title = %event.title,
                            "ObsChain anomaly event detected"
                        );

                        if let Err(e) = pipeline_storage.save_event(&event).await {
                            error!(error = %e, "Failed to persist detected ChainEvent");
                        }

                        pipeline_events_detected.fetch_add(1, Ordering::Relaxed);

                        // Broadcast to live connected clients via Axum WebSocket
                        let _ = pipeline_broadcaster.send(event);
                    }
                }
            }
        }
    });

    // 7. Start Axum server with graceful shutdown
    info!(
        http_addr = %addr,
        "ObsChain HTTP and WebSocket server running on http://{}",
        addr
    );

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let server = axum::serve(listener, app);

    // Wait for shutdown signals
    let shutdown_signal = async {
        let ctrl_c = async {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to install Ctrl+C handler");
        };

        #[cfg(unix)]
        let terminate = async {
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("Failed to install SIGTERM signal handler")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => {
                info!("SIGINT (Ctrl+C) received");
            },
            _ = terminate => {
                info!("SIGTERM received");
            },
        }
    };

    server
        .with_graceful_shutdown(async move {
            shutdown_signal.await;
            info!("Graceful shutdown initiated, signaling all ingestion and pipeline tasks");
            let _ = shutdown_tx.send(true);
        })
        .await?;

    info!("ObsChain backend shutdown completed cleanly");
    Ok(())
}
