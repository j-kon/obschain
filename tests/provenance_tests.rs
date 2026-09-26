use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{DateTime, Utc};
use obschain_core::{
    ChainEvent, ConfidenceLevel, EventObservation, EventSeverity, EventType, ObservationMode,
    ObservationSource, ObservationWitness, ReplayJob,
};
use obschain_storage::{
    EventFilter, EventRepository, InMemoryStorage, PostgresStorage, ReplayRepository,
};
use tower::ServiceExt;
use uuid::Uuid;

fn create_test_chain_event(txid: &str, amount_sats: u64, event_time: DateTime<Utc>) -> ChainEvent {
    let mut ev = ChainEvent::new(
        EventType::LargeTransfer,
        EventSeverity::High,
        ConfidenceLevel::VerifiedOnChain,
        format!("Large Transfer: {amount_sats} sats"),
        format!("Whale transaction {txid}"),
    );
    ev.txid = Some(txid.to_string());
    ev.block_height = Some(840000);
    ev.block_hash = Some("00000000000000000002a1b2c3d4e5f60718293a4b5c6d7e8f".to_string());
    ev.metadata = serde_json::json!({
        "amount_sats": amount_sats,
        "txid": txid,
    });
    ev.with_event_time(event_time)
}

async fn get_test_postgres_storage() -> Option<PostgresStorage> {
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
        Err(_) => return None,
    };

    let storage = PostgresStorage::new(pool);
    if storage.run_migrations().await.is_err() {
        return None;
    }
    Some(storage)
}

// ---------------------------------------------------------------------------
// 1. Live Then Replay Test: 1 Canonical Event, 2 Observations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_live_then_replay_provenance_preservation() {
    let storage = InMemoryStorage::new_empty(1000);
    let event_time = Utc::now() - chrono::Duration::hours(2);
    let canonical = create_test_chain_event("tx_whale_1", 500_000_000_000, event_time);
    let event_id = canonical.id;

    // A. Live observation arrives via Bitcoin Core ZMQ
    let live_observed_at = Utc::now() - chrono::Duration::hours(2);
    let live_source = ObservationSource::bitcoin_core_zmq("tcp://127.0.0.1:28332");
    storage.save_event(&canonical).await.unwrap();

    let live_obs = EventObservation::live(
        event_id,
        live_source.clone(),
        live_observed_at,
        Some(event_time),
        canonical.block_height,
        canonical.block_hash.clone(),
    );
    storage.save_event_observation(&live_obs).await.unwrap();

    // Verify 1 canonical event and 1 observation
    let ev_after_live = storage.get_event_by_id(event_id).await.unwrap().unwrap();
    assert_eq!(ev_after_live.id, event_id);
    assert_eq!(storage.list_events(100, 0).await.unwrap().len(), 1);
    assert_eq!(
        storage
            .count_event_observations(Some(event_id))
            .await
            .unwrap(),
        1
    );

    // B. Historical Replay Job processes the exact same logical event 2 hours later
    let replay_job_id = Uuid::new_v4();
    let replay_observed_at = Utc::now();
    let replay_source = ObservationSource::new("bitcoin_core", "rpc_historical_replay", None);

    // Replay saves canonical event (conservative upsert)
    storage.save_event(&canonical).await.unwrap();

    let replay_obs = EventObservation::historical_replay(
        event_id,
        replay_job_id,
        replay_source.clone(),
        replay_observed_at,
        Some(event_time),
        canonical.block_height,
        canonical.block_hash.clone(),
    );
    storage.save_event_observation(&replay_obs).await.unwrap();

    // VERIFICATION:
    // 1. Canonical event count MUST be exactly 1 (no duplicate ChainEvent)
    let all_events = storage.list_events(100, 0).await.unwrap();
    assert_eq!(
        all_events.len(),
        1,
        "Canonical event must not be duplicated"
    );

    // 2. Observations count MUST be 2 (live + replay)
    let observations = storage.list_event_observations(event_id).await.unwrap();
    assert_eq!(
        observations.len(),
        2,
        "Must preserve both live and replay observations"
    );

    let live_recorded = observations
        .iter()
        .find(|o| o.mode == ObservationMode::Live)
        .unwrap();
    assert_eq!(live_recorded.source.provider, "bitcoin_core");
    assert_eq!(live_recorded.source.transport, "zmq");
    assert_eq!(live_recorded.observed_at, live_observed_at);
    assert_eq!(live_recorded.bitcoin_time, Some(event_time));

    let replay_recorded = observations
        .iter()
        .find(|o| o.mode == ObservationMode::HistoricalReplay)
        .unwrap();
    assert_eq!(replay_recorded.replay_job_id, Some(replay_job_id));
    assert_eq!(replay_recorded.observed_at, replay_observed_at);
    assert_eq!(replay_recorded.bitcoin_time, Some(event_time));

    // 3. Detail view populates observations
    let detail = storage.get_event_by_id(event_id).await.unwrap().unwrap();
    assert_eq!(detail.observations.len(), 2);
}

// ---------------------------------------------------------------------------
// 2. Replay Twice Test: 1 Canonical Event, 2 Replay Observations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_replay_twice_distinct_jobs() {
    let storage = InMemoryStorage::new_empty(1000);
    let event_time = Utc::now() - chrono::Duration::days(5);
    let canonical = create_test_chain_event("tx_whale_2", 1_000_000_000_000, event_time);
    let event_id = canonical.id;

    let job_a_id = Uuid::new_v4();
    let job_b_id = Uuid::new_v4();

    // Replay Job A
    storage.save_event(&canonical).await.unwrap();
    let obs_a = EventObservation::historical_replay(
        event_id,
        job_a_id,
        ObservationSource::new("bitcoin_core", "rpc_historical_replay", None),
        Utc::now() - chrono::Duration::hours(1),
        Some(event_time),
        canonical.block_height,
        canonical.block_hash.clone(),
    );
    storage.save_event_observation(&obs_a).await.unwrap();

    // Replay Job B
    storage.save_event(&canonical).await.unwrap();
    let obs_b = EventObservation::historical_replay(
        event_id,
        job_b_id,
        ObservationSource::new("bitcoin_core", "rpc_historical_replay", None),
        Utc::now(),
        Some(event_time),
        canonical.block_height,
        canonical.block_hash.clone(),
    );
    storage.save_event_observation(&obs_b).await.unwrap();

    // Invariant: 1 canonical event, 2 historical observations
    let events = storage.list_events(100, 0).await.unwrap();
    assert_eq!(events.len(), 1);

    let observations = storage.list_event_observations(event_id).await.unwrap();
    assert_eq!(observations.len(), 2);

    let job_ids: Vec<Uuid> = observations
        .iter()
        .filter_map(|o| o.replay_job_id)
        .collect();
    assert!(job_ids.contains(&job_a_id));
    assert!(job_ids.contains(&job_b_id));
}

// ---------------------------------------------------------------------------
// 3. Dual Source Multi-Witness Test: 1 Canonical Event, 2 Live Observations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_dual_source_multi_witness_observations() {
    let storage = InMemoryStorage::new_empty(1000);
    let event_time = Utc::now();
    let mut canonical = create_test_chain_event("tx_whale_3", 750_000_000_000, event_time);
    let event_id = canonical.id;

    let btc_core_source = ObservationSource::bitcoin_core_zmq("tcp://127.0.0.1:28332");
    let mempool_source = ObservationSource::mempool_ws("wss://mempool.space/api/v1/ws");

    // Add first witness (Bitcoin Core ZMQ)
    canonical
        .witnesses
        .push(ObservationWitness::new(btc_core_source.clone()));
    storage.save_event(&canonical).await.unwrap();

    let obs_core = EventObservation::live(
        event_id,
        btc_core_source.clone(),
        Utc::now() - chrono::Duration::seconds(2),
        Some(event_time),
        canonical.block_height,
        canonical.block_hash.clone(),
    );
    storage.save_event_observation(&obs_core).await.unwrap();

    // Add second witness (mempool.space WebSocket)
    canonical
        .witnesses
        .push(ObservationWitness::new(mempool_source.clone()));
    storage.save_event(&canonical).await.unwrap();

    let obs_mempool = EventObservation::live(
        event_id,
        mempool_source.clone(),
        Utc::now() - chrono::Duration::seconds(1),
        Some(event_time),
        canonical.block_height,
        canonical.block_hash.clone(),
    );
    storage.save_event_observation(&obs_mempool).await.unwrap();

    // Invariant: 1 canonical event, 2 distinct live observations
    let events = storage.list_events(100, 0).await.unwrap();
    assert_eq!(events.len(), 1);

    let observations = storage.list_event_observations(event_id).await.unwrap();
    assert_eq!(observations.len(), 2);

    let providers: Vec<String> = observations
        .iter()
        .map(|o| o.source.provider.clone())
        .collect();
    assert!(providers.contains(&"bitcoin_core".to_string()));
    assert!(providers.contains(&"mempool.space".to_string()));
}

// ---------------------------------------------------------------------------
// 4. Query Mode Test: Event observed both Live and Replayed matches BOTH
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_mode_matches_both_live_and_replay() {
    let storage = InMemoryStorage::new_empty(1000);
    let event_time = Utc::now() - chrono::Duration::hours(3);
    let canonical = create_test_chain_event("tx_whale_4", 200_000_000_000, event_time);
    let event_id = canonical.id;

    let replay_job_id = Uuid::new_v4();

    storage.save_event(&canonical).await.unwrap();

    let live_obs = EventObservation::live(
        event_id,
        ObservationSource::bitcoin_core_zmq("tcp://127.0.0.1:28332"),
        Utc::now() - chrono::Duration::hours(3),
        Some(event_time),
        canonical.block_height,
        canonical.block_hash.clone(),
    );
    storage.save_event_observation(&live_obs).await.unwrap();

    let replay_obs = EventObservation::historical_replay(
        event_id,
        replay_job_id,
        ObservationSource::new("bitcoin_core", "rpc_historical_replay", None),
        Utc::now(),
        Some(event_time),
        canonical.block_height,
        canonical.block_hash.clone(),
    );
    storage.save_event_observation(&replay_obs).await.unwrap();

    // Query 1: observation_mode = Live
    let live_results = storage
        .query_events(&EventFilter {
            observation_mode: Some(ObservationMode::Live),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(live_results.len(), 1, "Must match live query filter");
    assert_eq!(live_results[0].id, event_id);

    // Query 2: observation_mode = HistoricalReplay
    let replay_results = storage
        .query_events(&EventFilter {
            observation_mode: Some(ObservationMode::HistoricalReplay),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        replay_results.len(),
        1,
        "Must match historical replay query filter"
    );
    assert_eq!(replay_results[0].id, event_id);
}

// ---------------------------------------------------------------------------
// 5. Replay Job Query Isolation Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_replay_job_query_isolation() {
    let storage = InMemoryStorage::new_empty(1000);

    let job_a = Uuid::new_v4();
    let job_b = Uuid::new_v4();

    let ev1 = create_test_chain_event("tx_job_1", 100_000_000, Utc::now());
    let ev2 = create_test_chain_event("tx_job_2", 200_000_000, Utc::now());
    let ev3 = create_test_chain_event("tx_job_3", 300_000_000, Utc::now());

    storage.save_event(&ev1).await.unwrap();
    storage.save_event(&ev2).await.unwrap();
    storage.save_event(&ev3).await.unwrap();

    let src = ObservationSource::new("bitcoin_core", "rpc_historical_replay", None);

    // ev1 observed in Job A only
    storage
        .save_event_observation(&EventObservation::historical_replay(
            ev1.id,
            job_a,
            src.clone(),
            Utc::now(),
            Some(ev1.event_time),
            ev1.block_height,
            ev1.block_hash.clone(),
        ))
        .await
        .unwrap();

    // ev2 observed in Job B only
    storage
        .save_event_observation(&EventObservation::historical_replay(
            ev2.id,
            job_b,
            src.clone(),
            Utc::now(),
            Some(ev2.event_time),
            ev2.block_height,
            ev2.block_hash.clone(),
        ))
        .await
        .unwrap();

    // ev3 observed in BOTH Job A and Job B
    storage
        .save_event_observation(&EventObservation::historical_replay(
            ev3.id,
            job_a,
            src.clone(),
            Utc::now(),
            Some(ev3.event_time),
            ev3.block_height,
            ev3.block_hash.clone(),
        ))
        .await
        .unwrap();
    storage
        .save_event_observation(&EventObservation::historical_replay(
            ev3.id,
            job_b,
            src.clone(),
            Utc::now(),
            Some(ev3.event_time),
            ev3.block_height,
            ev3.block_hash.clone(),
        ))
        .await
        .unwrap();

    // Query Job A -> ev1 and ev3 (2 events)
    let res_a = storage
        .query_events(&EventFilter {
            replay_job_id: Some(job_a),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(res_a.len(), 2);
    let ids_a: Vec<Uuid> = res_a.iter().map(|e| e.id).collect();
    assert!(ids_a.contains(&ev1.id));
    assert!(ids_a.contains(&ev3.id));
    assert!(!ids_a.contains(&ev2.id));

    // Query Job B -> ev2 and ev3 (2 events)
    let res_b = storage
        .query_events(&EventFilter {
            replay_job_id: Some(job_b),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(res_b.len(), 2);
    let ids_b: Vec<Uuid> = res_b.iter().map(|e| e.id).collect();
    assert!(ids_b.contains(&ev2.id));
    assert!(ids_b.contains(&ev3.id));
    assert!(!ids_b.contains(&ev1.id));
}

// ---------------------------------------------------------------------------
// 6. Research Dedup Invariant & Baseline Count Safety Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_baseline_count_dedup_invariant() {
    let storage = InMemoryStorage::new_empty(1000);
    let event_time = Utc::now() - chrono::Duration::days(10);

    // One 4,000 BTC transaction
    let canonical = create_test_chain_event("tx_4000_btc", 400_000_000_000_000, event_time);
    let event_id = canonical.id;

    // Replay 10 times across 10 distinct replay jobs
    for i in 0..10 {
        let job_id = Uuid::new_v4();
        storage.save_event(&canonical).await.unwrap();

        let obs = EventObservation::historical_replay(
            event_id,
            job_id,
            ObservationSource::new("bitcoin_core", "rpc_historical_replay", None),
            Utc::now() + chrono::Duration::seconds(i),
            Some(event_time),
            canonical.block_height,
            canonical.block_hash.clone(),
        );
        storage.save_event_observation(&obs).await.unwrap();
    }

    // Explicit Invariant Check:
    // Replaying 10 times produces:
    // - Canonical events = 1
    // - Observations = 10
    // Statistical distributions built on canonical events are NOT inflated.
    let canonical_events = storage.list_events(100, 0).await.unwrap();
    assert_eq!(
        canonical_events.len(),
        1,
        "Dedup Invariant Failed: 10 replays produced more than 1 canonical event!"
    );

    let observations = storage.list_event_observations(event_id).await.unwrap();
    assert_eq!(
        observations.len(),
        10,
        "Expected exactly 10 observation provenance records"
    );
}

// ---------------------------------------------------------------------------
// 7. PostgreSQL Integration Tests: FK Integrity, Constraints & Cascades
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_postgres_provenance_database_constraints() {
    let Some(storage) = get_test_postgres_storage().await else {
        println!("Postgres not available, skipping test_postgres_provenance_database_constraints");
        return;
    };

    let event_time = Utc::now() - chrono::Duration::hours(1);
    let event = create_test_chain_event("tx_pg_prov_1", 300_000_000, event_time);
    let event_id = event.id;

    // 1. Foreign Key Rejection on non-existent event
    let non_existent_event_id = Uuid::new_v4();
    let orphaned_obs = EventObservation::live(
        non_existent_event_id,
        ObservationSource::new("bitcoin_core", "zmq", None),
        Utc::now(),
        Some(event_time),
        None,
        None,
    );
    let fk_err = storage.save_event_observation(&orphaned_obs).await;
    assert!(
        fk_err.is_err(),
        "Must reject observation referencing non-existent event"
    );

    // 2. Save canonical event
    storage
        .save_event(&event)
        .await
        .expect("Save canonical event");

    // 3. Create a replay job
    let replay_job = ReplayJob::new("testnet", 800000, 800010, "bitcoin_core_rpc");
    let job_id = replay_job.id;
    storage
        .create_job(&replay_job)
        .await
        .expect("Create replay job");

    // 4. Save replay observation linked to replay_jobs(id)
    let replay_obs = EventObservation::historical_replay(
        event_id,
        job_id,
        ObservationSource::new("bitcoin_core", "rpc_historical_replay", None),
        Utc::now(),
        Some(event_time),
        event.block_height,
        event.block_hash.clone(),
    );
    storage
        .save_event_observation(&replay_obs)
        .await
        .expect("Save replay observation");

    // 5. Uniqueness constraint: saving same observation again is idempotent
    storage
        .save_event_observation(&replay_obs)
        .await
        .expect("Re-saving identical observation must be idempotent");

    let obs_list = storage
        .list_event_observations(event_id)
        .await
        .expect("List observations");
    assert_eq!(
        obs_list.len(),
        1,
        "Idempotent insert must not duplicate observation rows"
    );

    // 6. Canonical event upsert does NOT overwrite observation provenance
    let mut updated_canonical = event.clone();
    updated_canonical.title = "Updated Title from Subsequent Inspection".to_string();
    storage
        .save_event(&updated_canonical)
        .await
        .expect("Upsert canonical event");

    let fetched = storage
        .get_event_by_id(event_id)
        .await
        .expect("Get event")
        .unwrap();
    assert_eq!(fetched.title, "Updated Title from Subsequent Inspection");
    assert_eq!(fetched.observations.len(), 1);
    assert_eq!(fetched.observations[0].replay_job_id, Some(job_id));

    // 7. Cascade deletion: deleting canonical event cleans up observations
    sqlx::query("DELETE FROM chain_events WHERE id = $1")
        .bind(event_id)
        .execute(storage.pool())
        .await
        .expect("Delete chain event");

    let count_after_delete = storage
        .count_event_observations(Some(event_id))
        .await
        .expect("Count observations");
    assert_eq!(
        count_after_delete, 0,
        "Deleting canonical event must cascade delete observations"
    );
}

// ---------------------------------------------------------------------------
// 8. Observation API Endpoint Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_observation_api_endpoint() {
    let storage = InMemoryStorage::new_empty(1000);
    let event_time = Utc::now() - chrono::Duration::hours(4);
    let canonical = create_test_chain_event("tx_api_prov", 800_000_000, event_time);
    let event_id = canonical.id;

    storage.save_event(&canonical).await.unwrap();

    let live_obs = EventObservation::live(
        event_id,
        ObservationSource::bitcoin_core_zmq("tcp://127.0.0.1:28332"),
        Utc::now() - chrono::Duration::hours(4),
        Some(event_time),
        canonical.block_height,
        canonical.block_hash.clone(),
    );
    storage.save_event_observation(&live_obs).await.unwrap();

    let detectors: Vec<Arc<dyn obschain_detectors::Detector>> = vec![];
    let (state, _) = obschain::AppState::new(storage, detectors, false);
    let app = obschain::create_router(state);

    // GET /api/v1/events/:id/observations
    let req = Request::builder()
        .uri(format!("/api/v1/events/{event_id}/observations"))
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["event_id"], event_id.to_string());
    assert_eq!(json["count"], 1);
    let observations = json["observations"].as_array().unwrap();
    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0]["mode"], "LIVE");
    assert_eq!(observations[0]["source"]["provider"], "bitcoin_core");

    // Nonexistent event returns 404
    let nonexistent_id = Uuid::new_v4();
    let req_404 = Request::builder()
        .uri(format!("/api/v1/events/{nonexistent_id}/observations"))
        .body(Body::empty())
        .unwrap();

    let resp_404 = app.oneshot(req_404).await.unwrap();
    assert_eq!(resp_404.status(), StatusCode::NOT_FOUND);
}
