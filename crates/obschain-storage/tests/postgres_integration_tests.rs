use chrono::Utc;
use obschain_core::{
    ActivityStatus, ChainEvent, ConfidenceLevel, CorrelationStrength, EventSeverity, EventType,
    IncidentActivity, IncidentActivityType, IncidentAlert, ObservationMode, ObservationSource,
    ProvenanceClassification, ReplayCheckpoint, ReplayJob, ReplayJobStatus, WatchTarget,
    WatchTargetKind,
};
use obschain_storage::{
    EventFilter, EventRepository, IncidentActivityRepository, IncidentAlertRepository,
    IncidentRepository, PostgresStorage, ReplayRepository, WatchTargetRepository,
};
use std::time::Duration;
use uuid::Uuid;

async fn get_test_storage() -> Option<PostgresStorage> {
    let url = std::env::var("TEST_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| {
            "postgres://postgres:postgrespassword@localhost:5433/obschain".to_string()
        });

    let pool = match sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(3))
        .connect(&url)
        .await
    {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Skipping PostgreSQL integration test: could not connect to {url}: {e}");
            return None;
        }
    };

    let storage = PostgresStorage::new(pool);
    if let Err(e) = storage.run_migrations().await {
        eprintln!("Skipping PostgreSQL integration test: migration failed: {e}");
        return None;
    }

    Some(storage)
}

#[tokio::test]
async fn test_postgres_event_insertion_and_idempotency() {
    let Some(storage) = get_test_storage().await else {
        return;
    };

    let event_id = Uuid::new_v4();
    let txid = format!("{:064x}", 42);
    let event = ChainEvent {
        id: event_id,
        event_type: EventType::LargeTransfer,
        severity: EventSeverity::High,
        confidence: ConfidenceLevel::VerifiedOnChain,
        title: "Test 100 BTC Whale Movement".to_string(),
        description: "100.00 BTC transferred in test transaction".to_string(),
        detected_at: Utc::now(),
        txid: Some(txid.clone()),
        block_hash: None,
        block_height: None,
        source: Some(ObservationSource::mempool_ws(
            "wss://mempool.space/api/v1/ws",
        )),
        metadata: serde_json::json!({
            "amount_sats": 10_000_000_000u64,
            "amount_btc": 100.0,
            "fee_sats": 50_000,
            "fee_rate_sat_vb": 12.5,
            "threshold_sats": 1_000_000_000,
        }),
        witnesses: Vec::new(),
        observation_mode: obschain_core::ObservationMode::Live,
        replay_job_id: None,
    };

    // 1. Initial insert
    storage
        .save_event(&event)
        .await
        .expect("Failed to save event");

    // 2. Retrieval
    let retrieved = storage
        .get_event_by_id(event_id)
        .await
        .expect("Failed to query event")
        .expect("Event should exist");
    assert_eq!(retrieved.id, event_id);
    assert_eq!(retrieved.title, event.title);
    assert_eq!(retrieved.txid.as_deref(), Some(txid.as_str()));

    // 3. Idempotent re-insert (ON CONFLICT (id) DO UPDATE)
    let updated_event = ChainEvent {
        title: "Test 100 BTC Whale Movement (Updated)".to_string(),
        ..event.clone()
    };
    storage
        .save_event(&updated_event)
        .await
        .expect("Idempotent event insert should succeed");

    let retrieved_updated = storage
        .get_event_by_id(event_id)
        .await
        .expect("Failed to query event")
        .expect("Event should exist");
    assert_eq!(
        retrieved_updated.title,
        "Test 100 BTC Whale Movement (Updated)"
    );

    // 4. Pagination
    let events = storage
        .list_events(10, 0)
        .await
        .expect("Failed to list events");
    assert!(!events.is_empty());
}

#[tokio::test]
async fn test_postgres_incident_seed_and_idempotency() {
    let Some(storage) = get_test_storage().await else {
        return;
    };

    // 1. Seed canonical incident (OC-2026-0001)
    storage
        .seed_canonical_incidents()
        .await
        .expect("Failed to seed canonical incidents");

    // 2. Retrieve by case_id
    let incident = storage
        .get_incident_by_id_or_case_id(obschain_incidents::LIQUID_CASE_ID)
        .await
        .expect("Failed to get incident")
        .expect("Liquid incident must exist");

    assert_eq!(incident.case_id, obschain_incidents::LIQUID_CASE_ID);
    assert_eq!(incident.title, "Liquid Network Security Incident");
    assert!(!incident.timeline.is_empty());
    assert!(!incident.evidence.is_empty());
    assert!(!incident.graph.nodes.is_empty());
    assert!(!incident.graph.edges.is_empty());
    assert_eq!(incident.recovery.recovered_sats, 340_000_000_000);

    // 3. Idempotent seed re-run: must not crash, must not create duplicate cases
    storage
        .seed_canonical_incidents()
        .await
        .expect("Subsequent seed must succeed idempotently");

    let count = storage
        .count_incidents()
        .await
        .expect("Failed to count incidents");
    assert_eq!(
        count, 1,
        "There should still be exactly 1 incident after re-seeding"
    );
}

#[tokio::test]
async fn test_postgres_watch_target_persistence_and_uniqueness() {
    let Some(storage) = get_test_storage().await else {
        return;
    };

    // Ensure parent incident exists
    storage
        .seed_canonical_incidents()
        .await
        .expect("Seed incidents");

    let incident = storage
        .get_incident_by_id_or_case_id(obschain_incidents::LIQUID_CASE_ID)
        .await
        .expect("Get incident")
        .expect("Incident exists");

    let target_id = Uuid::new_v4();
    let txid = format!("aabb{:x}00112233445566778899", Uuid::new_v4().simple());
    let target = WatchTarget {
        id: target_id,
        incident_id: incident.id,
        case_id: incident.case_id.clone(),
        kind: WatchTargetKind::OutPoint {
            txid: txid.clone(),
            vout: 1,
        },
        label: Some("Attacker Controlled UTXO Test".to_string()),
        classification: ProvenanceClassification::OnChainVerified,
        source: "test_provenance".to_string(),
        evidence_id: None,
        active: true,
        created_at: Utc::now(),
    };

    storage
        .save_watch_target(&target)
        .await
        .expect("Save watch target");

    let found_target = storage
        .get_watch_target_by_id(target_id)
        .await
        .expect("Query target")
        .expect("Target must be persisted");
    assert_eq!(
        found_target.label.as_deref(),
        Some("Attacker Controlled UTXO Test")
    );

    // Update target
    let mut updated = target.clone();
    updated.label = Some("Attacker Controlled UTXO (Updated)".to_string());
    storage
        .save_watch_target(&updated)
        .await
        .expect("Update watch target");

    let updated_target = storage
        .get_watch_target_by_id(target_id)
        .await
        .expect("Query updated target")
        .expect("Target must exist");
    assert_eq!(
        updated_target.label.as_deref(),
        Some("Attacker Controlled UTXO (Updated)")
    );

    // Verify uniqueness constraint: duplicate outpoint under different ID must be rejected by database
    let duplicate_target = WatchTarget {
        id: Uuid::new_v4(), // Different UUID
        incident_id: incident.id,
        case_id: incident.case_id.clone(),
        kind: WatchTargetKind::OutPoint {
            txid: txid.clone(),
            vout: 1, // Same outpoint
        },
        label: Some("Duplicate Outpoint".to_string()),
        classification: ProvenanceClassification::OnChainVerified,
        source: "test_provenance".to_string(),
        evidence_id: None,
        active: true,
        created_at: Utc::now(),
    };

    let dup_res = storage.save_watch_target(&duplicate_target).await;
    assert!(
        dup_res.is_err(),
        "Duplicate outpoint under same incident must be rejected by uniqueness constraint"
    );
}

#[tokio::test]
async fn test_postgres_activity_persistence_and_confirmation_upsert() {
    let Some(storage) = get_test_storage().await else {
        return;
    };

    storage
        .seed_canonical_incidents()
        .await
        .expect("Seed incidents");

    let incident = storage
        .get_incident_by_id_or_case_id(obschain_incidents::LIQUID_CASE_ID)
        .await
        .expect("Get incident")
        .expect("Incident exists");

    let targets = storage
        .list_watch_targets(Some(incident.id), 10, 0)
        .await
        .expect("List targets");
    let watch_target_id = targets.first().expect("Must have canonical target").id;

    let activity_id = Uuid::new_v4();
    let trigger_txid = format!("{:064x}", Uuid::new_v4().as_u128());
    let dedup_key = IncidentActivity::generate_dedup_key(
        &incident.id,
        &IncidentActivityType::WatchedOutpointSpent,
        Some(&trigger_txid),
        &watch_target_id,
    );

    // 1. Initial observation in MEMPOOL
    let mempool_activity = IncidentActivity {
        id: activity_id,
        incident_id: incident.id,
        case_id: incident.case_id.clone(),
        activity_type: IncidentActivityType::WatchedOutpointSpent,
        observed_at: Utc::now(),
        trigger_txid: Some(trigger_txid.clone()),
        block_height: None,
        block_hash: None,
        value_sats: Some(1_500_000_000),
        watch_target_id,
        confidence: ProvenanceClassification::OnChainVerified,
        correlation_strength: CorrelationStrength::Direct,
        status: ActivityStatus::Mempool,
        source: ObservationSource::mempool_ws("wss://mempool.space/api/v1/ws"),
        evidence: Vec::new(),
        description: "Mempool outpoint spend observed".to_string(),
        details: Some(serde_json::json!({ "fee_sats": 25000 })),
        dedup_key: dedup_key.clone(),
    };

    storage
        .save_activity(&mempool_activity)
        .await
        .expect("Save mempool activity");

    let retrieved_mempool = storage
        .get_activity_by_id(activity_id)
        .await
        .expect("Get activity")
        .expect("Activity must exist");
    assert_eq!(retrieved_mempool.status, ActivityStatus::Mempool);
    assert_eq!(retrieved_mempool.block_height, None);

    // 2. Confirmation transition: block included the transaction
    let confirmed_activity = IncidentActivity {
        status: ActivityStatus::Confirmed,
        block_height: Some(890_555),
        block_hash: Some("00000000000000000003bfa89c02d1e2".to_string()),
        description: "Confirmed outpoint spend observed".to_string(),
        ..mempool_activity.clone()
    };

    // Save with same dedup_key: must update status and block details without creating duplicate
    storage
        .save_activity(&confirmed_activity)
        .await
        .expect("Save confirmed activity update");

    let retrieved_confirmed = storage
        .get_activity_by_id(activity_id)
        .await
        .expect("Get activity")
        .expect("Activity must exist");
    assert_eq!(retrieved_confirmed.status, ActivityStatus::Confirmed);
    assert_eq!(retrieved_confirmed.block_height, Some(890_555));
    assert_eq!(
        retrieved_confirmed.block_hash.as_deref(),
        Some("00000000000000000003bfa89c02d1e2")
    );
}

#[tokio::test]
async fn test_postgres_alert_persistence() {
    let Some(storage) = get_test_storage().await else {
        return;
    };

    storage
        .seed_canonical_incidents()
        .await
        .expect("Seed incidents");

    let incident = storage
        .get_incident_by_id_or_case_id(obschain_incidents::LIQUID_CASE_ID)
        .await
        .expect("Get incident")
        .expect("Incident exists");

    let targets = storage
        .list_watch_targets(Some(incident.id), 10, 0)
        .await
        .expect("List targets");
    let target = targets.first().expect("Must have target");

    let activity_id = Uuid::new_v4();
    let trigger_txid = format!("{:064x}", Uuid::new_v4().as_u128());
    let activity = IncidentActivity {
        id: activity_id,
        incident_id: incident.id,
        case_id: incident.case_id.clone(),
        activity_type: IncidentActivityType::WatchedAddressSpent,
        observed_at: Utc::now(),
        trigger_txid: Some(trigger_txid.clone()),
        block_height: Some(890_700),
        block_hash: None,
        value_sats: Some(1_500_000_000),
        watch_target_id: target.id,
        confidence: ProvenanceClassification::OnChainVerified,
        correlation_strength: CorrelationStrength::Direct,
        status: ActivityStatus::Confirmed,
        source: ObservationSource::mempool_ws("wss://mempool.space/api/v1/ws"),
        evidence: Vec::new(),
        description: "Whale address spend activity".to_string(),
        details: None,
        dedup_key: format!("{}:test_alert_act:{}", incident.id, Uuid::new_v4()),
    };
    storage
        .save_activity(&activity)
        .await
        .expect("Save activity for alert");

    let alert_id = Uuid::new_v4();
    let alert = IncidentAlert {
        id: alert_id,
        incident_id: incident.id,
        case_id: incident.case_id.clone(),
        incident_title: incident.title.clone(),
        activity_id,
        severity: EventSeverity::Critical,
        title: "Whale Sweep Alert".to_string(),
        summary: "15.00 BTC swept from attacker address".to_string(),
        confidence: ProvenanceClassification::OnChainVerified,
        correlation_strength: CorrelationStrength::Direct,
        observed_at: Utc::now(),
        value_sats: Some(1_500_000_000),
        trigger_txid: Some(trigger_txid),
    };

    storage.save_alert(&alert).await.expect("Save alert");

    let retrieved = storage
        .get_alert_by_id(alert_id)
        .await
        .expect("Get alert")
        .expect("Alert must exist");
    assert_eq!(retrieved.id, alert_id);
    assert_eq!(retrieved.severity, EventSeverity::Critical);
    assert_eq!(retrieved.title, "Whale Sweep Alert");
    assert_eq!(retrieved.value_sats, Some(1_500_000_000));

    let alerts = storage
        .list_alerts(Some(incident.id), 10, 0)
        .await
        .expect("List alerts");
    assert!(!alerts.is_empty());
}

#[tokio::test]
async fn test_postgres_movement_does_not_equal_recovery_invariant() {
    // ObsChain Fundamental Invariant (Rule 19):
    // IncidentActivity must NEVER automatically modify incident recovery snapshots.
    let Some(storage) = get_test_storage().await else {
        return;
    };

    storage
        .seed_canonical_incidents()
        .await
        .expect("Seed incidents");

    let incident_before = storage
        .get_incident_by_id_or_case_id(obschain_incidents::LIQUID_CASE_ID)
        .await
        .expect("Get incident")
        .expect("Incident exists");

    let initial_recovered = incident_before.recovery.recovered_sats;
    let initial_affected = incident_before.recovery.affected_sats;

    let targets = storage
        .list_watch_targets(Some(incident_before.id), 10, 0)
        .await
        .expect("List targets");
    let target_id = targets.first().expect("Must have target").id;
    let trigger_txid = format!("{:064x}", Uuid::new_v4().as_u128());

    // Simulate an on-chain activity movement of 15 BTC linked to this incident
    let moved_activity = IncidentActivity {
        id: Uuid::new_v4(),
        incident_id: incident_before.id,
        case_id: incident_before.case_id.clone(),
        activity_type: IncidentActivityType::NewDescendantObserved,
        observed_at: Utc::now(),
        trigger_txid: Some(trigger_txid.clone()),
        block_height: Some(890_600),
        block_hash: Some("00000000000000000001abcdef123456".to_string()),
        value_sats: Some(1_500_000_000),
        watch_target_id: target_id,
        confidence: ProvenanceClassification::OnChainVerified,
        correlation_strength: CorrelationStrength::Structural,
        status: ActivityStatus::Confirmed,
        source: ObservationSource::bitcoin_core_rpc("http://127.0.0.1:8332"),
        evidence: Vec::new(),
        description: "Descendant transaction spent 15 BTC".to_string(),
        details: None,
        dedup_key: format!("{}:test_movement:{}", incident_before.id, Uuid::new_v4()),
    };

    storage
        .save_activity(&moved_activity)
        .await
        .expect("Save activity");

    // Fetch incident after activity persisted
    let incident_after = storage
        .get_incident_by_id_or_case_id(obschain_incidents::LIQUID_CASE_ID)
        .await
        .expect("Get incident")
        .expect("Incident exists");

    // Enforce Rule 19: recovery amounts MUST be completely untouched by movement
    assert_eq!(
        incident_after.recovery.recovered_sats, initial_recovered,
        "CRITICAL INVARIANT VIOLATION: on-chain movement must never modify recovered_sats"
    );
    assert_eq!(
        incident_after.recovery.affected_sats, initial_affected,
        "CRITICAL INVARIANT VIOLATION: on-chain movement must never modify affected_sats"
    );
}

#[tokio::test]
async fn test_postgres_transaction_rollback() {
    let Some(storage) = get_test_storage().await else {
        return;
    };

    let test_id = Uuid::new_v4();

    // 1. Begin a transaction
    let mut tx = storage.pool().begin().await.expect("Begin transaction");

    // 2. Perform an insert inside the transaction
    sqlx::query(
        r#"
        INSERT INTO chain_events (
            id, event_type, severity, confidence, title, description, detected_at, metadata
        )
        VALUES ($1, 'LARGE_TRANSFER', 'HIGH', 'VERIFIED_ON_CHAIN', 'Rollback Test Event', 'Description', NOW(), '{}'::jsonb)
        "#,
    )
    .bind(test_id)
    .execute(&mut *tx)
    .await
    .expect("Execute in tx");

    // 3. Rollback the transaction explicitly
    tx.rollback().await.expect("Rollback tx");

    // 4. Verify that the event was NOT persisted
    let exists = storage.get_event_by_id(test_id).await.expect("Query event");
    assert!(
        exists.is_none(),
        "Rolled back transaction must leave no persistent trace"
    );
}

#[tokio::test]
async fn test_postgres_recovery_snapshot_history() {
    let Some(storage) = get_test_storage().await else {
        return;
    };

    storage
        .seed_canonical_incidents()
        .await
        .expect("Seed incidents");

    let incident = storage
        .get_incident_by_id_or_case_id(obschain_incidents::LIQUID_CASE_ID)
        .await
        .expect("Get incident")
        .expect("Incident exists");

    // Query recovery snapshots from incident_recovery_snapshots table directly
    let rows = sqlx::query(
        r#"
        SELECT id, affected_sats, recovered_sats, outstanding_sats, is_estimate, source_label
        FROM incident_recovery_snapshots
        WHERE incident_id = $1
        ORDER BY created_at ASC
        "#,
    )
    .bind(incident.id)
    .fetch_all(storage.pool())
    .await
    .expect("Fetch snapshots");

    assert!(
        !rows.is_empty(),
        "Incident recovery snapshots must be recorded"
    );
}

#[tokio::test]
async fn test_postgres_replay_jobs_and_checkpoints() {
    let Some(storage) = get_test_storage().await else {
        return;
    };

    let mut job = ReplayJob::new("regtest", 100, 200, "bitcoin_core_rpc");
    let job_id = job.id;

    // 1. Create job
    storage.create_job(&job).await.expect("Create replay job");

    let fetched = storage
        .get_job(job_id)
        .await
        .expect("Get replay job")
        .expect("Job exists");
    assert_eq!(fetched.id, job_id);
    assert_eq!(fetched.start_height, 100);
    assert_eq!(fetched.end_height, 200);
    assert_eq!(fetched.status, ReplayJobStatus::Pending);

    // 2. Update job
    job.status = ReplayJobStatus::Running;
    job.current_height = 150;
    job.blocks_processed = 50;
    job.transactions_processed = 120;
    job.events_generated = 3;
    storage.update_job(&job).await.expect("Update replay job");

    let updated = storage
        .get_job(job_id)
        .await
        .expect("Get updated replay job")
        .expect("Job exists");
    assert_eq!(updated.status, ReplayJobStatus::Running);
    assert_eq!(updated.current_height, 150);
    assert_eq!(updated.blocks_processed, 50);

    // 3. Save checkpoints
    let cp1 = ReplayCheckpoint::new(job_id, 125, 25, 60, 1);
    let cp2 = ReplayCheckpoint::new(job_id, 150, 50, 120, 3);
    storage
        .save_checkpoint(&cp1)
        .await
        .expect("Save checkpoint 1");
    storage
        .save_checkpoint(&cp2)
        .await
        .expect("Save checkpoint 2");

    let latest_cp = storage
        .get_latest_checkpoint(job_id)
        .await
        .expect("Get latest checkpoint")
        .expect("Checkpoint exists");
    assert_eq!(latest_cp.completed_height, 150);
    assert_eq!(latest_cp.blocks_processed, 50);

    // 4. List jobs
    let jobs = storage.list_jobs(10, 0).await.expect("List replay jobs");
    assert!(!jobs.is_empty());
    assert!(jobs.iter().any(|j| j.id == job_id));
}

#[tokio::test]
async fn test_postgres_event_filter_query() {
    let Some(storage) = get_test_storage().await else {
        return;
    };

    let replay_job_id = Uuid::new_v4();
    let ev1 = ChainEvent {
        id: Uuid::new_v4(),
        event_type: EventType::LargeTransfer,
        severity: EventSeverity::High,
        confidence: ConfidenceLevel::VerifiedOnChain,
        title: "Replay Event 1".to_string(),
        description: "Replayed block 500".to_string(),
        detected_at: Utc::now() - chrono::Duration::hours(1),
        block_height: Some(500),
        block_hash: Some("0000000000000000000500".to_string()),
        txid: Some("tx_500_a".to_string()),
        metadata: serde_json::json!({"sats": 50000}),
        witnesses: vec![],
        source: None,
        observation_mode: ObservationMode::HistoricalReplay,
        replay_job_id: Some(replay_job_id),
    };

    let ev2 = ChainEvent {
        id: Uuid::new_v4(),
        event_type: EventType::DormantCoinsMoved,
        severity: EventSeverity::Critical,
        confidence: ConfidenceLevel::VerifiedOnChain,
        title: "Replay Event 2".to_string(),
        description: "Replayed block 505".to_string(),
        detected_at: Utc::now() - chrono::Duration::minutes(30),
        block_height: Some(505),
        block_hash: Some("0000000000000000000505".to_string()),
        txid: Some("tx_505_b".to_string()),
        metadata: serde_json::json!({"sats": 100000}),
        witnesses: vec![],
        source: None,
        observation_mode: ObservationMode::HistoricalReplay,
        replay_job_id: Some(replay_job_id),
    };

    storage.save_event(&ev1).await.expect("Save ev1");
    storage.save_event(&ev2).await.expect("Save ev2");

    // Filter by replay_job_id
    let res = storage
        .query_events(&EventFilter {
            replay_job_id: Some(replay_job_id),
            ..Default::default()
        })
        .await
        .expect("Query events by replay_job_id");
    assert_eq!(res.len(), 2);

    // Filter by height range 502..510
    let res_height = storage
        .query_events(&EventFilter {
            replay_job_id: Some(replay_job_id),
            from_height: Some(502),
            to_height: Some(510),
            ..Default::default()
        })
        .await
        .expect("Query events by height");
    assert_eq!(res_height.len(), 1);
    assert_eq!(res_height[0].block_height, Some(505));

    // Filter by event_type
    let res_type = storage
        .query_events(&EventFilter {
            replay_job_id: Some(replay_job_id),
            event_type: Some(EventType::LargeTransfer),
            ..Default::default()
        })
        .await
        .expect("Query events by type");
    assert_eq!(res_type.len(), 1);
    assert_eq!(res_type[0].event_type, EventType::LargeTransfer);

    // Filter by observation_mode
    let res_mode = storage
        .query_events(&EventFilter {
            replay_job_id: Some(replay_job_id),
            observation_mode: Some(ObservationMode::HistoricalReplay),
            ..Default::default()
        })
        .await
        .expect("Query events by mode");
    assert_eq!(res_mode.len(), 2);
}
