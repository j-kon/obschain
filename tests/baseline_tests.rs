use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use obschain::api::AppState;
use obschain::create_router;
use obschain_core::baseline::{
    BaselineDistribution, BaselineMetric, BaselineQuality, BaselineRunStatus, EvaluationMode,
    EventRarityResult, HalvingEpoch, MetricUnit, MetricValue, RarityBand, RarityDirection,
};
use obschain_core::{
    ChainEvent, ConfidenceLevel, EventObservation, EventObservationKind, EventSeverity, EventType,
    ObservationSource,
};
use obschain_detectors::{LargeTransactionDetector, LongBlockIntervalDetector};
use obschain_intelligence::baseline::{BaselineCalculator, BaselineEngine, ImpactCalculator};
use obschain_storage::{
    BaselineRepository, EventRepository, InMemoryStorage, PostgresStorage, Storage,
};
use tower::ServiceExt;
use uuid::Uuid;

fn create_large_transfer_event(
    txid: &str,
    value_sats: u64,
    height: u64,
    network: &str,
) -> ChainEvent {
    let mut ev = ChainEvent::new(
        EventType::LargeTransfer,
        EventSeverity::High,
        ConfidenceLevel::VerifiedOnChain,
        format!("Large Transfer: {value_sats} sats"),
        format!("Transfer txid {txid}"),
    );
    ev.txid = Some(txid.to_string());
    ev.block_height = Some(height);
    ev.block_hash = Some(format!("0000000000000000000{height:08x}"));
    ev.metadata = serde_json::json!({
        "value_sats": value_sats,
        "amount_sats": value_sats,
        "input_count": 2,
        "output_count": 2,
        "vsize": 250,
        "network": network,
    });
    ev
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
// 1. Section 49 & 8: Canonical-Event Dedup Invariant Test
// 1 Canonical LargeTransfer Event + 100 EventObservation records = 1 sample!
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_canonical_event_dedup_invariant() {
    let storage = Storage::from(InMemoryStorage::new());
    let txid = format!("dedup_tx_{}", Uuid::new_v4());
    let mut canonical_event =
        create_large_transfer_event(&txid, 500_000_000_000, 850_000, "mainnet");
    canonical_event = canonical_event.with_deterministic_id();
    let event_id = canonical_event.id;

    // Save the canonical event
    storage.save_event(&canonical_event).await.unwrap();

    // Create and save 100 distinct observation occurrences referencing this canonical event
    let source = ObservationSource::bitcoin_core_rpc("http://127.0.0.1:8332");
    for i in 0..100 {
        let obs = EventObservation::live(
            event_id,
            source.clone(),
            EventObservationKind::Confirmed,
            Utc::now(),
            Some(Utc::now()),
            Some(850_000),
            Some(format!("00000000000000000000000000000000{i:04x}")),
        );
        storage.save_event_observation(&obs).await.unwrap();
    }

    // Query canonical events for baseline population across the height range
    let qualifying_events = storage
        .query_events_for_baseline(Some(EventType::LargeTransfer), 840_000, 860_000)
        .await
        .unwrap();

    // Invariant: population must contain exactly 1 event, NOT 100!
    assert_eq!(
        qualifying_events.len(),
        1,
        "Baseline population MUST count canonical events (chain_events), not observations"
    );
    assert_eq!(qualifying_events[0].id, event_id);

    // Compute distribution using baseline engine
    let run =
        BaselineEngine::create_run_record("mainnet", 840_000, 860_000, "obschain-baseline-v1");
    let dists = BaselineEngine::generate_distributions(run.id, &qualifying_events, None);

    let value_dist = dists
        .iter()
        .find(|d| d.metric == BaselineMetric::ValueSats)
        .expect("ValueSats distribution must be computed");

    assert_eq!(
        value_dist.sample_count, 1,
        "Distribution sample_count MUST be 1 despite 100 observation records"
    );
    assert_eq!(value_dist.minimum.as_u64(), Some(500_000_000_000));
    assert_eq!(value_dist.maximum.as_u64(), Some(500_000_000_000));
}

// ---------------------------------------------------------------------------
// 2. Section 50: Deterministic Distribution Quantiles Test
// Fixture samples: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
// Verify exact p50, p75, p90, p95, p99
// ---------------------------------------------------------------------------
#[test]
fn test_deterministic_distribution_quantiles() {
    let raw_samples = vec![10u64, 20, 30, 40, 50, 60, 70, 80, 90, 100];
    let metric_values: Vec<MetricValue> = raw_samples.into_iter().map(MetricValue::U64).collect();

    let qdist = BaselineCalculator::compute_distribution(&metric_values, 10)
        .expect("Quantile distribution must succeed");

    assert_eq!(qdist.sample_count, 10);
    assert_eq!(qdist.minimum.as_u64(), Some(10));
    assert_eq!(qdist.maximum.as_u64(), Some(100));
    assert_eq!(qdist.mean, 55.0);

    // Discrete quantile (percentile_disc) exact expectations:
    // N = 10, index = ceil(p * N)
    // p50  -> ceil(0.50 * 10) = 5  -> value 50
    // p75  -> ceil(0.75 * 10) = 8  -> value 80
    // p90  -> ceil(0.90 * 10) = 9  -> value 90
    // p95  -> ceil(0.95 * 10) = 10 -> value 100
    // p99  -> ceil(0.99 * 10) = 10 -> value 100
    // p999 -> ceil(0.999 * 10) = 10 -> value 100
    assert_eq!(qdist.p50.as_u64(), Some(50));
    assert_eq!(qdist.p75.as_u64(), Some(80));
    assert_eq!(qdist.p90.as_u64(), Some(90));
    assert_eq!(qdist.p95.as_u64(), Some(100));
    assert_eq!(qdist.p99.as_u64(), Some(100));
    assert_eq!(qdist.p999.as_u64(), Some(100));
}

// ---------------------------------------------------------------------------
// 3. Section 51: Tie Handling and Empirical CDF Percentile Rank Test
// Dataset: [10, 10, 10, 20, 20, 30]
// ---------------------------------------------------------------------------
#[test]
fn test_tie_handling_and_empirical_cdf_rank() {
    let raw = vec![10u64, 10, 10, 20, 20, 30];
    let population: Vec<MetricValue> = raw.into_iter().map(MetricValue::U64).collect();

    // 10: 3 values <= 10 out of 6 total -> (3 / 6) * 100 = 50.00%
    // Tail count (values >= 10): 6
    let (p10, tail10) = BaselineCalculator::rank_value(
        &population,
        MetricValue::U64(10),
        RarityDirection::HigherIsRarer,
    );
    assert!((p10 - 50.0).abs() < 0.01);
    assert_eq!(tail10, 6);

    // 20: 5 values <= 20 out of 6 total -> (5 / 6) * 100 = 83.33%
    // Tail count (values >= 20): 3 (20, 20, 30)
    let (p20, tail20) = BaselineCalculator::rank_value(
        &population,
        MetricValue::U64(20),
        RarityDirection::HigherIsRarer,
    );
    assert!((p20 - 83.33).abs() < 0.01);
    assert_eq!(tail20, 3);

    // 30: 6 values <= 30 out of 6 total -> (6 / 6) * 100 = 100.00%
    // Tail count (values >= 30): 1 (30)
    let (p30, tail30) = BaselineCalculator::rank_value(
        &population,
        MetricValue::U64(30),
        RarityDirection::HigherIsRarer,
    );
    assert!((p30 - 100.0).abs() < 0.01);
    assert_eq!(tail30, 1);
}

// ---------------------------------------------------------------------------
// 4. Section 52: Large Integer & u128 Coin Age Destroyed Tests
// 21M BTC = 2_100_000_000_000_000 sats
// CAD = 2_100_000_000_000_000 * 5000 = 10_500_000_000_000_000_000 sats-days
// ---------------------------------------------------------------------------
#[test]
fn test_large_integer_and_u128_satoshi_days() {
    let max_btc_sats = 2_100_000_000_000_000u64;
    let mv_sats = MetricValue::U64(max_btc_sats);
    assert_eq!(mv_sats.as_u64(), Some(max_btc_sats));
    assert_eq!(mv_sats.to_numeric_string(), "2100000000000000");

    // Coin Age Destroyed satoshi-days exceeding u64::MAX (18_446_744_073_709_551_615)
    // 10M BTC held for 4000 days = 10_000_000 * 100_000_000 * 4000 = 4_000_000_000_000_000_000_000 sat-days
    let huge_cad: u128 = 4_000_000_000_000_000_000_000u128;
    let mv_cad = MetricValue::U128(huge_cad);

    assert_eq!(mv_cad.as_u128(), Some(huge_cad));
    assert_eq!(mv_cad.to_numeric_string(), "4000000000000000000000");

    // Test parsing back from string
    let parsed = MetricValue::from_str_and_metric(
        "4000000000000000000000",
        BaselineMetric::CoinAgeDestroyedSatoshiDays,
    );

    assert_eq!(parsed, mv_cad);
}

// ---------------------------------------------------------------------------
// 5. Section 53 & 17: Insufficient Population Safeguards
// sample_count < 100 -> RarityBand::InsufficientData & ImpactScore == None
// ---------------------------------------------------------------------------
#[test]
fn test_insufficient_population_safeguards() {
    let samples: Vec<MetricValue> = (1..=5).map(|v| MetricValue::U64(v * 100)).collect();
    let qdist = BaselineCalculator::compute_distribution(&samples, 5).unwrap();

    let dist = BaselineDistribution {
        id: Uuid::new_v4(),
        baseline_run_id: Uuid::new_v4(),
        event_type: EventType::LargeTransfer,
        metric: BaselineMetric::ValueSats,
        unit: MetricUnit::Satoshis,
        sample_count: 5,
        candidate_count: 5,
        missing_count: 0,
        coverage_ratio: 1.0,
        quality: BaselineQuality::Insufficient, // sample count < 100
        minimum: qdist.minimum,
        maximum: qdist.maximum,
        mean: qdist.mean,
        p50: qdist.p50,
        p75: qdist.p75,
        p90: qdist.p90,
        p95: qdist.p95,
        p99: qdist.p99,
        p999: qdist.p999,
        samples_json: None,
        created_at: Utc::now(),
    };

    let mut run =
        BaselineEngine::create_run_record("mainnet", 800_000, 810_000, "obschain-baseline-v1");
    run.id = dist.baseline_run_id;
    run.status = BaselineRunStatus::Completed;
    run.canonical_event_count = 5;

    let event = create_large_transfer_event("tx_whale", 999_999, 805_000, "mainnet");

    // Min sample size requirement = 100
    let rarity_context = BaselineEngine::evaluate_event_rarity(&event, &run, &[dist], 100);

    // Primary rarity result must have band INSUFFICIENT_DATA and percentile None
    let primary = &rarity_context.primary;
    assert_eq!(
        primary.rarity_band,
        RarityBand::InsufficientData,
        "Small sample size (<100) must return INSUFFICIENT_DATA, not EXTREME"
    );
    assert_eq!(
        primary.percentile, None,
        "Percentile must be withheld when sample size is insufficient"
    );

    // Impact score must be unavailable
    let impact = ImpactCalculator::calculate_impact(
        EventType::LargeTransfer,
        std::slice::from_ref(primary),
        Some("obschain-impact-v1"),
    );
    assert!(
        impact.total_score.is_none(),
        "Impact score must be None when data is insufficient"
    );
}

// ---------------------------------------------------------------------------
// 6. Section 54: Data Quality and Coverage Accounting Test
// 100% -> High, 97% -> Moderate, 80% -> Degraded, 20% -> Insufficient
// ---------------------------------------------------------------------------
#[test]
fn test_data_quality_and_coverage_accounting() {
    // 600 samples, 0.98 -> High
    let q_high = BaselineQuality::evaluate(600, 0.98);
    assert_eq!(q_high, BaselineQuality::High);

    // 200 samples, 0.90 -> Moderate
    let q_mod = BaselineQuality::evaluate(200, 0.90);
    assert_eq!(q_mod, BaselineQuality::Moderate);

    // 50 samples, 0.60 -> Degraded
    let q_deg = BaselineQuality::evaluate(50, 0.60);
    assert_eq!(q_deg, BaselineQuality::Degraded);

    // 20 samples, 0.20 -> Insufficient
    let q_insuff = BaselineQuality::evaluate(20, 0.20);
    assert_eq!(q_insuff, BaselineQuality::Insufficient);
}

// ---------------------------------------------------------------------------
// 7. Section 55 & 30: Network Isolation Tests
// Regtest event against mainnet baseline -> rejected!
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_network_isolation_enforcement() {
    let storage = Storage::from(InMemoryStorage::new());

    // Create a mainnet baseline run
    let mut mainnet_run =
        BaselineEngine::create_run_record("mainnet", 800_000, 850_000, "obschain-baseline-v1");
    mainnet_run.status = BaselineRunStatus::Completed;
    mainnet_run.completed_at = Some(Utc::now());
    storage.create_baseline_run(&mainnet_run).await.unwrap();

    // Create regtest event
    let regtest_event = create_large_transfer_event("tx_regtest", 50_000_000_000, 150, "regtest");
    storage.save_event(&regtest_event).await.unwrap();

    // Query compatible baseline for regtest
    let compatible = storage
        .get_latest_compatible_baseline_run("regtest", Some(150), Some("obschain-baseline-v1"))
        .await
        .unwrap();

    assert!(
        compatible.is_none(),
        "Mainnet baseline must NEVER be returned as compatible for a regtest event"
    );

    // In-memory or API isolation test: verify network check
    assert_ne!(
        regtest_event.network().unwrap(),
        mainnet_run.network,
        "Network isolation must distinguish regtest and mainnet"
    );
}

// ---------------------------------------------------------------------------
// 8. Section 56 & 10: Baseline Versioning and Reproducibility
// Multiple algorithm versions do not collide or silently overwrite
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_baseline_versioning_and_reproducibility() {
    let storage = Storage::from(InMemoryStorage::new());

    let mut run_v1 =
        BaselineEngine::create_run_record("mainnet", 800_000, 850_000, "obschain-baseline-v1");
    run_v1.status = BaselineRunStatus::Completed;
    run_v1.completed_at = Some(Utc::now());
    storage.create_baseline_run(&run_v1).await.unwrap();

    let mut run_v2 =
        BaselineEngine::create_run_record("mainnet", 800_000, 850_000, "obschain-baseline-v2");
    run_v2.status = BaselineRunStatus::Completed;
    run_v2.completed_at = Some(Utc::now());
    storage.create_baseline_run(&run_v2).await.unwrap();

    // Query v1
    let found_v1 = storage
        .get_latest_compatible_baseline_run("mainnet", Some(840_000), Some("obschain-baseline-v1"))
        .await
        .unwrap()
        .expect("v1 run must be found");
    assert_eq!(found_v1.algorithm_version, "obschain-baseline-v1");
    assert_eq!(found_v1.id, run_v1.id);

    // Query v2
    let found_v2 = storage
        .get_latest_compatible_baseline_run("mainnet", Some(840_000), Some("obschain-baseline-v2"))
        .await
        .unwrap()
        .expect("v2 run must be found");
    assert_eq!(found_v2.algorithm_version, "obschain-baseline-v2");
    assert_eq!(found_v2.id, run_v2.id);
}

// ---------------------------------------------------------------------------
// 9. Section 36-42: Explainable Impact Breakdown
// Score is mathematically derived with component weights and model version
// ---------------------------------------------------------------------------
#[test]
fn test_explainable_impact_breakdown_formula() {
    let primary = EventRarityResult {
        event_id: Uuid::new_v4(),
        baseline_run_id: Uuid::new_v4(),
        event_type: EventType::LargeTransfer,
        metric: BaselineMetric::ValueSats,
        value: MetricValue::U64(1_000_000_000_000),
        percentile: Some(99.0),
        rarity_band: RarityBand::Rare,
        population_size: 10_000,
        tail_count: 100,
        evaluation_mode: EvaluationMode::Retrospective,
    };

    let impact = ImpactCalculator::calculate_impact(
        EventType::LargeTransfer,
        &[primary],
        Some("obschain-impact-v1"),
    );

    assert_eq!(impact.model_version, "obschain-impact-v1");
    assert_eq!(impact.status, "EXPERIMENTAL");
    assert!(impact.total_score.is_some());

    // Value rarity max points = 25
    // Percentile 99.0% -> normalized points = 25.0 * (99.0 / 100.0) = 24.75 -> rounded 24.75
    let comp = &impact.components[0];
    assert_eq!(comp.component_name, "Value anomaly");
    assert_eq!(comp.weight, 25.0);
    assert_eq!(comp.points_awarded, 24.5);
    assert_eq!(comp.percentile, Some(99.0));
    assert_eq!(comp.population_size, 10_000);
}

// ---------------------------------------------------------------------------
// 10. Section 33, 34: Halving Epoch Categorization
// ---------------------------------------------------------------------------
#[test]
fn test_halving_epoch_categorization() {
    assert_eq!(HalvingEpoch::epoch_from_height(0), 0);
    assert_eq!(HalvingEpoch::epoch_from_height(209_999), 0);
    assert_eq!(HalvingEpoch::epoch_from_height(210_000), 1);
    assert_eq!(HalvingEpoch::epoch_from_height(419_999), 1);
    assert_eq!(HalvingEpoch::epoch_from_height(420_000), 2);
    assert_eq!(HalvingEpoch::epoch_from_height(629_999), 2);
    assert_eq!(HalvingEpoch::epoch_from_height(630_000), 3);
    assert_eq!(HalvingEpoch::epoch_from_height(839_999), 3);
    assert_eq!(HalvingEpoch::epoch_from_height(840_000), 4);
    assert_eq!(HalvingEpoch::epoch_from_height(1_049_999), 4);
    assert_eq!(HalvingEpoch::epoch_from_height(1_050_000), 5);

    let (start, end) = HalvingEpoch::epoch_range(4);
    assert_eq!(start, 840_000);
    assert_eq!(end, 1_049_999);
}

// ---------------------------------------------------------------------------
// 11. Section 27, 28: REST API Baseline & Rarity Endpoints
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_api_baseline_and_rarity_endpoints() {
    let storage = Storage::from(InMemoryStorage::new());
    let detectors: Vec<Arc<dyn obschain_detectors::Detector>> = vec![
        Arc::new(LargeTransactionDetector::new()),
        Arc::new(LongBlockIntervalDetector::new()),
    ];

    let (mut state, _) = AppState::new(storage.clone(), detectors, true);
    state = state.with_baseline(false, "obschain-baseline-v1", "obschain-impact-v1", 100);

    let app = create_router(state);

    // 1. GET /api/v1/research/baselines -> should return empty list initially
    let req = Request::builder()
        .uri("/api/v1/research/baselines")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 2. POST /api/v1/research/baselines when disabled -> 403 Forbidden (Section 61)
    let create_payload = serde_json::json!({
        "network": "mainnet",
        "start_height": 840_000,
        "end_height": 850_000,
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/research/baselines")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&create_payload).unwrap()))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    // 3. Create completed baseline and distribution in storage
    let mut run =
        BaselineEngine::create_run_record("mainnet", 840_000, 850_000, "obschain-baseline-v1");
    run.status = BaselineRunStatus::Completed;
    run.completed_at = Some(Utc::now());
    storage.create_baseline_run(&run).await.unwrap();

    let dist = BaselineDistribution {
        id: Uuid::new_v4(),
        baseline_run_id: run.id,
        event_type: EventType::LargeTransfer,
        metric: BaselineMetric::ValueSats,
        unit: MetricUnit::Satoshis,
        sample_count: 500,
        candidate_count: 500,
        missing_count: 0,
        coverage_ratio: 1.0,
        quality: BaselineQuality::High,
        minimum: MetricValue::U64(10_000_000),
        maximum: MetricValue::U64(1_000_000_000_000),
        mean: 50_000_000_000.0,
        p50: MetricValue::U64(20_000_000_000),
        p75: MetricValue::U64(50_000_000_000),
        p90: MetricValue::U64(100_000_000_000),
        p95: MetricValue::U64(250_000_000_000),
        p99: MetricValue::U64(800_000_000_000),
        p999: MetricValue::U64(950_000_000_000),
        samples_json: None,
        created_at: Utc::now(),
    };
    storage.save_baseline_distributions(&[dist]).await.unwrap();

    // 4. Create and save canonical event
    let event = create_large_transfer_event("tx_whale_api", 850_000_000_000, 845_000, "mainnet");
    storage.save_event(&event).await.unwrap();

    // 5. GET /api/v1/research/baselines/:id -> 200 OK
    let req = Request::builder()
        .uri(format!("/api/v1/research/baselines/{}", run.id))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 6. GET /api/v1/research/distributions -> 200 OK
    let req = Request::builder()
        .uri(format!(
            "/api/v1/research/distributions?baseline_run_id={}",
            run.id
        ))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // 7. GET /api/v1/events/:id/rarity -> 200 OK with transparent rarity components
    let req = Request::builder()
        .uri(format!("/api/v1/events/{}/rarity", event.id))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["event_type"], "LARGE_TRANSFER");
    assert_eq!(json["baseline"]["id"], run.id.to_string());
    assert_eq!(json["baseline"]["quality"], "HIGH");
    assert!(json["primary"].is_object());
    assert_eq!(json["primary"]["metric"], "value_sats");
    assert_eq!(json["primary"]["rarity_band"], "RARE"); // 850B is between p99 800B and p99.9 950B

    // 8. GET /api/v1/events/:id -> 200 OK with fast non-blocking rarity enrichment
    let req = Request::builder()
        .uri(format!("/api/v1/events/{}", event.id))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let event_json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(event_json["rarity"].is_object());
    assert_eq!(event_json["rarity"]["baseline_id"], run.id.to_string());
    assert_eq!(event_json["rarity"]["primary"]["metric"], "value_sats");
}

// ---------------------------------------------------------------------------
// 12. PostgreSQL Persistence Integration for Baseline Engine (if PG available)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_postgres_baseline_persistence() {
    let pg = match get_test_postgres_storage().await {
        Some(s) => s,
        None => {
            eprintln!("Skipping test_postgres_baseline_persistence: PostgreSQL not reachable");
            return;
        }
    };
    let storage = Storage::from(pg);

    let mut run =
        BaselineEngine::create_run_record("mainnet", 700_000, 750_000, "obschain-baseline-v1");
    run.status = BaselineRunStatus::Completed;
    run.completed_at = Some(Utc::now());
    run.canonical_event_count = 1500;
    storage.create_baseline_run(&run).await.unwrap();

    let dist = BaselineDistribution {
        id: Uuid::new_v4(),
        baseline_run_id: run.id,
        event_type: EventType::LargeTransfer,
        metric: BaselineMetric::ValueSats,
        unit: MetricUnit::Satoshis,
        sample_count: 1500,
        candidate_count: 1500,
        missing_count: 10,
        coverage_ratio: 0.9933,
        quality: BaselineQuality::High,
        minimum: MetricValue::U64(10_000_000),
        maximum: MetricValue::U64(2_100_000_000_000_000),
        mean: 45_000_000_000.0,
        p50: MetricValue::U64(25_000_000_000),
        p75: MetricValue::U64(75_000_000_000),
        p90: MetricValue::U64(200_000_000_000),
        p95: MetricValue::U64(500_000_000_000),
        p99: MetricValue::U64(1_500_000_000_000),
        p999: MetricValue::U64(10_000_000_000_000),
        samples_json: None,
        created_at: Utc::now(),
    };
    storage
        .save_baseline_distributions(std::slice::from_ref(&dist))
        .await
        .unwrap();

    // Query back from postgres
    let runs = storage
        .list_baseline_runs(Some("mainnet"), 10, 0)
        .await
        .unwrap();
    assert!(runs.iter().any(|r| r.id == run.id));

    let dists = storage.get_baseline_distributions(run.id).await.unwrap();
    assert_eq!(dists.len(), 1);
    assert_eq!(dists[0].metric, BaselineMetric::ValueSats);
    assert_eq!(dists[0].maximum.as_u64(), Some(2_100_000_000_000_000));
}
