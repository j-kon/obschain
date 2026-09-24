use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use obschain::{
    create_router, redact_database_url, AppConfig, AppState, PipelineMetrics, StorageBackendConfig,
    WebSocketBroadcast,
};
use obschain_core::{ChainEvent, IncidentActivity, IncidentAlert, Observation};
use obschain_detectors::{
    ConsolidationDetector, DetectorEngine, DormantCoinDetector, EventDeduplicator,
    ExtremeFeeDetector, FanOutDetector, LargeTransactionDetector, LongBlockIntervalDetector,
    RbfDetector,
};
use obschain_ingest::{
    EnricherConfig, MempoolRestClient, MempoolRestConfig, MempoolWebSocketClient,
    TransactionEnricher, UtxoCache,
};
use obschain_intelligence::IncidentWatchEngine;
use obschain_storage::{
    EventRepository, InMemoryStorage, IncidentActivityRepository, IncidentAlertRepository,
    PostgresStorage, Storage,
};
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
        storage_backend = %config.storage_backend,
        mempool_api = %config.mempool_api_url,
        mempool_ws = %config.mempool_ws_url,
        large_tx_threshold_sats = config.large_tx_threshold_sats,
        long_block_interval_secs = config.long_block_interval_seconds,
        dormant_min_age_days = config.dormant_min_age_days,
        dormant_min_value_sats = config.dormant_min_value_sats,
        consolidation_min_inputs = config.consolidation_min_inputs,
        fanout_min_outputs = config.fanout_min_outputs,
        extreme_fee_sats = config.extreme_fee_sats,
        "ObsChain backend initializing live Bitcoin observation engine"
    );

    // 1. Setup storage (in-memory or postgres) and channels
    let storage: Storage = match config.storage_backend {
        StorageBackendConfig::Memory => {
            info!(
                storage_backend = "memory",
                limit = config.event_store_limit,
                "Using in-memory bounded storage backend"
            );
            if config.mock_feed {
                InMemoryStorage::with_limit(config.event_store_limit).into()
            } else {
                InMemoryStorage::new_empty(config.event_store_limit).into()
            }
        }
        StorageBackendConfig::Postgres => {
            let raw_db_url = config.database_url.as_deref().ok_or_else(|| {
                anyhow::anyhow!(
                    "OBSCHAIN_STORAGE_BACKEND is set to 'postgres', but DATABASE_URL environment variable is missing"
                )
            })?;

            let redacted_url = redact_database_url(raw_db_url);
            info!(
                storage_backend = "postgres",
                database = %redacted_url,
                max_connections = config.db_max_connections,
                min_connections = config.db_min_connections,
                acquire_timeout_secs = config.db_acquire_timeout_seconds,
                "Connecting to PostgreSQL database"
            );

            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(config.db_max_connections)
                .min_connections(config.db_min_connections)
                .acquire_timeout(Duration::from_secs(config.db_acquire_timeout_seconds))
                .connect(raw_db_url)
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to connect to PostgreSQL database (ensure database is running and reachable): {e}"
                    )
                })?;

            let pg_storage = PostgresStorage::new(pool);

            info!("Applying pending PostgreSQL database migrations");
            pg_storage
                .run_migrations()
                .await
                .map_err(|e| anyhow::anyhow!("Database migration failed during startup: {e}"))?;
            info!(
                storage_backend = "postgres",
                database_connected = true,
                migrations_applied = true,
                "PostgreSQL schema migrated successfully"
            );

            info!("Seeding canonical incident intelligence and watch targets idempotently");
            pg_storage.seed_canonical_incidents().await.map_err(|e| {
                anyhow::anyhow!("Failed to seed canonical incident into PostgreSQL: {e}")
            })?;
            info!("Canonical incident and watch targets seeded successfully");

            pg_storage
                .initialize_telemetry_counters()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to initialize telemetry counters: {e}"))?;

            pg_storage.into()
        }
    };

    // Bounded observation channel: protects against unbounded memory growth under high tx volume
    let (obs_tx, mut obs_rx) = mpsc::channel::<Observation>(1000);

    // Graceful shutdown coordination channel
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // 2. Setup Mempool REST client
    let rest_config = MempoolRestConfig {
        base_url: config.mempool_api_url.clone(),
        timeout: Duration::from_secs(10),
        connect_timeout: Duration::from_secs(5),
        max_retries: 3,
        max_response_bytes: 10 * 1024 * 1024,
    };

    let rest_client = match MempoolRestClient::new(rest_config) {
        Ok(c) => Some(Arc::new(c)),
        Err(e) => {
            warn!(error = %e, "Failed to initialize MempoolRestClient");
            None
        }
    };

    // 3. Setup UTXO cache and bounded enrichment layer
    let utxo_cache = UtxoCache::new(
        config.utxo_cache_limit,
        Duration::from_secs(config.utxo_cache_ttl_seconds),
    );

    let enricher_config = EnricherConfig {
        max_input_enrichment: config.max_input_enrichment,
        lookup_concurrency: config.utxo_lookup_concurrency,
        request_timeout: Duration::from_secs(5),
    };

    let enricher = rest_client
        .as_ref()
        .map(|rc| TransactionEnricher::new(rc.clone(), utxo_cache.clone(), enricher_config));

    let deduplicator = EventDeduplicator::new(
        config.dedup_cache_capacity,
        Duration::from_secs(config.dedup_cache_ttl_seconds),
    );

    // 4. Setup Detectors and DetectorEngine
    let large_tx_detector = Arc::new(LargeTransactionDetector::with_threshold_sats(
        config.large_tx_threshold_sats,
    ));
    let long_interval_detector = Arc::new(LongBlockIntervalDetector::with_threshold_seconds(
        config.long_block_interval_seconds,
    ));
    let dormant_detector = Arc::new(DormantCoinDetector::with_thresholds(
        config.dormant_min_age_days,
        config.dormant_min_value_sats,
    ));
    let consolidation_detector = Arc::new(ConsolidationDetector::with_thresholds(
        config.consolidation_min_inputs,
        config.consolidation_max_outputs,
        config.consolidation_min_value_sats,
    ));
    let fanout_detector = Arc::new(FanOutDetector::with_thresholds(
        config.fanout_min_outputs,
        config.fanout_min_value_sats,
    ));
    let extreme_fee_detector = Arc::new(ExtremeFeeDetector::with_thresholds(
        config.extreme_fee_sats,
        config.extreme_fee_rate_sat_vb,
    ));
    let rbf_detector = Arc::new(RbfDetector::new());

    let detectors: Vec<Arc<dyn obschain_detectors::Detector>> = vec![
        large_tx_detector,
        long_interval_detector,
        dormant_detector,
        consolidation_detector,
        fanout_detector,
        extreme_fee_detector,
        rbf_detector,
    ];
    let mut engine = DetectorEngine::new(detectors.clone());

    // 5. Setup IncidentWatchEngine, AppState, and Metrics for Axum HTTP and WebSocket API
    let mut watch_engine = IncidentWatchEngine::from_env();
    let canonical_targets = obschain_incidents::canonical_liquid_watch_targets();
    let targets_count = canonical_targets.len();
    watch_engine.load_targets(canonical_targets);
    watch_engine.register_incident_title(
        obschain_incidents::LIQUID_CASE_ID,
        "Liquid Network Security Incident",
    );
    info!(
        targets_loaded = targets_count,
        follow_depth = watch_engine.max_descendant_depth(),
        "IncidentWatchEngine initialized with canonical Liquid targets"
    );
    let watch_engine_arc = Arc::new(tokio::sync::RwLock::new(watch_engine));

    let metrics = PipelineMetrics::default();
    let (state, event_broadcaster) = AppState::with_metrics_and_watch_engine(
        storage.clone(),
        detectors,
        config.mock_feed,
        metrics.clone(),
        watch_engine_arc.clone(),
    );
    let app = create_router(state.clone());

    // 6. Mempool WebSocket ingestion supervisor
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

    // 7. Mempool REST poller for tip synchronization and fallback ingestion
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

    // 8. Detector pipeline worker: Observation channel -> Enrichment -> Engine -> ChainEvent -> Dedup -> Storage & Broadcast
    let pipeline_storage = storage.clone();
    let pipeline_broadcaster = event_broadcaster.clone();
    let pipeline_ws_broadcaster = state.ws_broadcaster.clone();
    let pipeline_watch_engine = watch_engine_arc.clone();
    let pipeline_events_detected = state.events_detected.clone();
    let pipeline_tip_height = state.tip_height.clone();
    let pipeline_metrics = metrics.clone();
    let enricher_handle = enricher.clone();
    let dedup_handle = deduplicator.clone();
    let utxo_cache_handle = utxo_cache.clone();
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
                    let Some(mut obs) = obs_opt else {
                        info!("Observation channel closed");
                        break;
                    };

                    let mut incident_matches = Vec::new();

                    match &mut obs {
                        Observation::Block(block) => {
                            pipeline_metrics.blocks_observed.fetch_add(1, Ordering::Relaxed);
                            pipeline_tip_height.store(block.height, Ordering::Relaxed);
                            info!(
                                height = block.height,
                                hash = %block.block_hash,
                                tx_count = block.tx_count,
                                interval_secs = ?block.interval_seconds,
                                "New Bitcoin block observed"
                            );

                            let mut engine_lock = pipeline_watch_engine.write().await;
                            incident_matches = engine_lock.process_block(block);
                        }
                        Observation::Transaction(ref mut tx) => {
                            pipeline_metrics.transactions_observed.fetch_add(1, Ordering::Relaxed);
                            tracing::debug!(
                                txid = %tx.txid,
                                total_output_sats = tx.total_output_sats,
                                fee_sats = tx.fee_sats,
                                "Bitcoin transaction observed"
                            );

                            // Historical UTXO enrichment for dormant-coin detection
                            if let Some(ref enricher) = enricher_handle {
                                enricher.enrich_transaction(tx).await;
                                let has_history = tx.inputs.iter().any(|i| i.historical_utxo.is_some());
                                if has_history {
                                    pipeline_metrics.transactions_enriched.fetch_add(1, Ordering::Relaxed);
                                }
                                pipeline_metrics.utxo_lookup_failures.store(enricher.lookup_failures(), Ordering::Relaxed);
                            }

                            pipeline_metrics.cache_hits.store(utxo_cache_handle.hits(), Ordering::Relaxed);
                            pipeline_metrics.cache_misses.store(utxo_cache_handle.misses(), Ordering::Relaxed);

                            let mut engine_lock = pipeline_watch_engine.write().await;
                            incident_matches = engine_lock.process_transaction(tx, tx.block_height, tx.block_hash.as_deref());
                        }
                        Observation::Replacement(repl) => {
                            info!(
                                replacement_txid = %repl.replacement_txid,
                                replaced_count = repl.replaced_txids.len(),
                                fee_delta_sats = repl.fee_delta_sats,
                                "Bitcoin mempool transaction replacement observed"
                            );

                            let mut engine_lock = pipeline_watch_engine.write().await;
                            incident_matches = engine_lock.process_replacement(repl);
                        }
                        Observation::Mempool(mp) => {
                            tracing::debug!(
                                tx_count = mp.count,
                                total_fee_sats = mp.total_fee_sats,
                                "Mempool backlog stats updated"
                            );
                        }
                    }

                    // Process and broadcast any incident watch matches
                    for (activity, alert_opt) in incident_matches {
                        info!(
                            incident_id = %activity.incident_id,
                            case_id = %activity.case_id,
                            activity_type = ?activity.activity_type,
                            correlation = ?activity.correlation_strength,
                            confidence = ?activity.confidence,
                            "ObsChain incident activity detected"
                        );

                        save_activity_with_retry(&pipeline_storage, &activity, &pipeline_metrics).await;

                        pipeline_metrics
                            .incident_activities_detected
                            .fetch_add(1, Ordering::Relaxed);

                        let _ = pipeline_ws_broadcaster
                            .send(WebSocketBroadcast::incident_activity(activity.clone()));

                        if let Some(alert) = alert_opt {
                            info!(
                                case_id = %alert.case_id,
                                severity = ?alert.severity,
                                title = %alert.title,
                                "ObsChain incident alert emitted"
                            );

                            save_alert_with_retry(&pipeline_storage, &alert, &pipeline_metrics).await;

                            pipeline_metrics
                                .incident_alerts_emitted
                                .fetch_add(1, Ordering::Relaxed);

                            let _ = pipeline_ws_broadcaster
                                .send(WebSocketBroadcast::incident_alert(alert));
                        }
                    }

                    let detected_events = engine.process_observation(obs);
                    let fresh_events = dedup_handle.filter(detected_events);
                    pipeline_metrics.events_deduplicated.store(dedup_handle.deduplicated_count(), Ordering::Relaxed);
                    pipeline_metrics.events_generated.fetch_add(fresh_events.len() as u64, Ordering::Relaxed);

                    for event in fresh_events {
                        info!(
                            id = %event.id,
                            event_type = ?event.event_type,
                            severity = ?event.severity,
                            title = %event.title,
                            "ObsChain anomaly event detected"
                        );

                        save_event_with_retry(&pipeline_storage, &event, &pipeline_metrics).await;

                        pipeline_events_detected.fetch_add(1, Ordering::Relaxed);

                        // Broadcast to live connected clients via Axum WebSocket
                        let _ = pipeline_broadcaster.send(event);
                    }
                }
            }
        }
    });

    // 9. Periodic pipeline telemetry heartbeat logging (every 60s)
    let heartbeat_metrics = metrics.clone();
    let mut heartbeat_shutdown = shutdown_rx.clone();
    tokio::spawn(async move {
        while !*heartbeat_shutdown.borrow() {
            tokio::select! {
                _ = heartbeat_shutdown.changed() => break,
                _ = tokio::time::sleep(Duration::from_secs(60)) => {
                    let snap = heartbeat_metrics.snapshot();
                    info!(
                        txs_observed = snap.transactions_observed,
                        blocks_observed = snap.blocks_observed,
                        txs_enriched = snap.transactions_enriched,
                        lookup_failures = snap.utxo_lookup_failures,
                        cache_hits = snap.cache_hits,
                        cache_misses = snap.cache_misses,
                        events_generated = snap.events_generated,
                        events_deduplicated = snap.events_deduplicated,
                        incident_activities = snap.incident_activities_detected,
                        incident_alerts = snap.incident_alerts_emitted,
                        storage_write_errors = snap.storage_write_errors,
                        "ObsChain pipeline telemetry heartbeat"
                    );
                }
            }
        }
    });

    // 10. Start Axum server with graceful shutdown
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

async fn save_activity_with_retry(
    storage: &Storage,
    activity: &IncidentActivity,
    metrics: &PipelineMetrics,
) {
    let mut attempts = 0;
    let max_attempts = 3;
    let mut delay = Duration::from_millis(50);
    loop {
        attempts += 1;
        match storage.save_activity(activity).await {
            Ok(()) => return,
            Err(e) => {
                if attempts >= max_attempts {
                    error!(
                        error = %e,
                        activity_id = %activity.id,
                        case_id = %activity.case_id,
                        attempts = attempts,
                        "Failed to persist detected IncidentActivity after max retries; continuing operation"
                    );
                    metrics.storage_write_errors.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                warn!(
                    error = %e,
                    activity_id = %activity.id,
                    attempt = attempts,
                    "Transient storage error saving IncidentActivity; retrying"
                );
                tokio::time::sleep(delay).await;
                delay *= 2;
            }
        }
    }
}

async fn save_alert_with_retry(
    storage: &Storage,
    alert: &IncidentAlert,
    metrics: &PipelineMetrics,
) {
    let mut attempts = 0;
    let max_attempts = 3;
    let mut delay = Duration::from_millis(50);
    loop {
        attempts += 1;
        match storage.save_alert(alert).await {
            Ok(()) => return,
            Err(e) => {
                if attempts >= max_attempts {
                    error!(
                        error = %e,
                        alert_id = %alert.id,
                        case_id = %alert.case_id,
                        attempts = attempts,
                        "Failed to persist IncidentAlert after max retries; continuing operation"
                    );
                    metrics.storage_write_errors.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                warn!(
                    error = %e,
                    alert_id = %alert.id,
                    attempt = attempts,
                    "Transient storage error saving IncidentAlert; retrying"
                );
                tokio::time::sleep(delay).await;
                delay *= 2;
            }
        }
    }
}

async fn save_event_with_retry(storage: &Storage, event: &ChainEvent, metrics: &PipelineMetrics) {
    let mut attempts = 0;
    let max_attempts = 3;
    let mut delay = Duration::from_millis(50);
    loop {
        attempts += 1;
        match storage.save_event(event).await {
            Ok(()) => return,
            Err(e) => {
                if attempts >= max_attempts {
                    error!(
                        error = %e,
                        event_id = %event.id,
                        event_type = ?event.event_type,
                        attempts = attempts,
                        "Failed to persist detected ChainEvent after max retries; continuing operation"
                    );
                    metrics.storage_write_errors.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                warn!(
                    error = %e,
                    event_id = %event.id,
                    attempt = attempts,
                    "Transient storage error saving ChainEvent; retrying"
                );
                tokio::time::sleep(delay).await;
                delay *= 2;
            }
        }
    }
}
