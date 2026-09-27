use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use obschain::api::AppState;
use obschain::create_router;
use obschain_core::baseline::{
    BaselineDistribution, BaselineMetric, BaselineQuality, BaselineRunStatus, EvaluationMode,
    EventRarityResult, HalvingEpoch, MetricUnit, MetricValue, PercentileMethod, RarityBand,
    RarityDirection,
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
        percentile_method: PercentileMethod::ExactEmpiricalCdf,
        estimated: false,
        rarity_band: RarityBand::Rare,
        population_size: 10_000,
        tail_count: 100,
        evaluation_mode: EvaluationMode::Retrospective,
    };
    let secondary = EventRarityResult {
        event_id: primary.event_id,
        baseline_run_id: primary.baseline_run_id,
        event_type: EventType::LargeTransfer,
        metric: BaselineMetric::OutputCount,
        value: MetricValue::U64(50),
        percentile: Some(95.0),
        percentile_method: PercentileMethod::ExactEmpiricalCdf,
        estimated: false,
        rarity_band: RarityBand::Unusual,
        population_size: 10_000,
        tail_count: 500,
        evaluation_mode: EvaluationMode::Retrospective,
    };

    let impact =
        ImpactCalculator::calculate_impact(EventType::LargeTransfer, &[primary, secondary], None);

    assert_eq!(impact.model_id, "obschain-impact-large-transfer-v1");
    assert_eq!(impact.status, "EXPERIMENTAL");
    assert!(impact.total_score.is_some());

    // Value rarity max points = 60.0
    // (99.0 - 50.0)/50.0 = 0.98 -> 60.0 * 0.98 = 58.8 points
    let comp = &impact.components[0];
    assert_eq!(comp.component_name, "Value rarity");
    assert_eq!(comp.weight, 60.0);
    assert!((comp.points_awarded - 58.8).abs() < 0.1);
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

// ---------------------------------------------------------------------------
// 13. Section 34: Exact Empirical CDF vs Quantile Interpolation on Skewed Dataset
// ---------------------------------------------------------------------------
#[test]
fn test_exact_empirical_cdf_vs_quantile_interpolation_skewed() {
    // Deliberately skewed dataset of 10,000 samples:
    // 9,900 samples with value 100
    // 90 samples with value 500
    // 10 samples with value 10,000
    let mut samples = Vec::with_capacity(10_000);
    for _ in 0..9_900 {
        samples.push(MetricValue::U64(100));
    }
    for _ in 0..90 {
        samples.push(MetricValue::U64(500));
    }
    for _ in 0..10 {
        samples.push(MetricValue::U64(10_000));
    }
    samples.sort();

    // Query value: 499 (just below 500)
    let query_val = MetricValue::U64(499);

    // Exact empirical rank:
    // Elements <= 499 is exactly 9,900 out of 10,000 -> 99.00%
    let (exact_p, tail_count) =
        BaselineCalculator::rank_value(&samples, query_val, RarityDirection::HigherIsRarer);
    assert_eq!(exact_p, 99.0);
    assert_eq!(tail_count, 100); // 90 with 500 + 10 with 10,000

    // Construct distribution quantiles representing this dataset:
    // p99 (sample 9900) = 100
    // p99.9 (sample 9990) = 500
    let dist = BaselineDistribution {
        id: Uuid::new_v4(),
        baseline_run_id: Uuid::new_v4(),
        event_type: EventType::LargeTransfer,
        metric: BaselineMetric::ValueSats,
        unit: MetricUnit::Satoshis,
        sample_count: 10_000,
        candidate_count: 10_000,
        missing_count: 0,
        coverage_ratio: 1.0,
        quality: BaselineQuality::High,
        minimum: MetricValue::U64(100),
        maximum: MetricValue::U64(10_000),
        mean: 113.6,
        p50: MetricValue::U64(100),
        p75: MetricValue::U64(100),
        p90: MetricValue::U64(100),
        p95: MetricValue::U64(100),
        p99: MetricValue::U64(100),
        p999: MetricValue::U64(500),
        samples_json: None,
        created_at: Utc::now(),
    };

    // Quantile interpolation estimate between p99 (100) and p99.9 (500):
    // frac = (499 - 100) / (500 - 100) = 399 / 400 = 0.9975
    // estimated percentile = 99.0 + 0.9975 * 0.9 = 99.89775%
    let (est_p, _est_tail) = BaselineCalculator::rank_from_distribution(
        &dist,
        query_val,
        RarityDirection::HigherIsRarer,
    );
    assert!((est_p - 99.897).abs() < 0.05);

    // Material difference demonstration: exact is 99.00%, estimate is 99.90%
    let diff = (est_p - exact_p).abs();
    assert!(
        diff > 0.85,
        "Expected material difference between exact eCDF and linear interpolation, got {diff}"
    );

    // Verify evaluation engine tags methods accurately:
    let ev = create_large_transfer_event("tx_skewed", 499, 850_000, "mainnet");
    let baseline =
        BaselineEngine::create_run_record("mainnet", 800_000, 900_000, "obschain-baseline-v2");

    // Evaluation with exact rank:
    let mut exact_map = std::collections::HashMap::new();
    exact_map.insert(BaselineMetric::ValueSats, (exact_p, tail_count, 10_000));
    let ctx_exact = BaselineEngine::evaluate_event_rarity_with_exact_ranks(
        &ev,
        &baseline,
        std::slice::from_ref(&dist),
        &exact_map,
        100,
    );
    assert_eq!(
        ctx_exact.primary.percentile_method,
        PercentileMethod::ExactEmpiricalCdf
    );
    assert!(!ctx_exact.primary.estimated);
    assert_eq!(ctx_exact.primary.percentile, Some(99.0));

    // Evaluation without exact rank (falls back to quantile interpolation estimate):
    let empty_map = std::collections::HashMap::new();
    let ctx_est = BaselineEngine::evaluate_event_rarity_with_exact_ranks(
        &ev,
        &baseline,
        &[dist],
        &empty_map,
        100,
    );
    assert_eq!(
        ctx_est.primary.percentile_method,
        PercentileMethod::QuantileInterpolationEstimate
    );
    assert!(ctx_est.primary.estimated);
    assert!((ctx_est.primary.percentile.unwrap() - 99.897).abs() < 0.05);
}

// ---------------------------------------------------------------------------
// 14. Section 35: Impact Weight Saturation Bound (Never Exceeds 100 by Construction)
// ---------------------------------------------------------------------------
#[test]
fn test_impact_weight_saturation_never_exceeds_100_by_construction() {
    let supported = obschain_core::MetricRegistry::supported_event_types();
    assert_eq!(supported.len(), 6);

    for event_type in supported {
        let defs = obschain_core::MetricRegistry::metrics_for_event_type(event_type);
        // Create 100th percentile for EVERY registered metric of this event type
        let rarities: Vec<EventRarityResult> = defs
            .iter()
            .map(|def| EventRarityResult {
                event_id: Uuid::new_v4(),
                baseline_run_id: Uuid::new_v4(),
                event_type,
                metric: def.metric,
                value: MetricValue::U64(1_000_000),
                percentile: Some(100.0),
                percentile_method: PercentileMethod::ExactEmpiricalCdf,
                estimated: false,
                rarity_band: RarityBand::Extreme,
                population_size: 50_000,
                tail_count: 1,
                evaluation_mode: EvaluationMode::Retrospective,
            })
            .collect();

        let breakdown = ImpactCalculator::calculate_impact(event_type, &rarities, None);
        assert_eq!(breakdown.status, "EXPERIMENTAL");
        assert_eq!(breakdown.model_coverage, 1.0);
        assert!(breakdown.total_score.is_some());

        let total = breakdown.total_score.unwrap();
        // Mathematical proof: total score must be exactly 100.0 by construction, never > 100.0
        assert!(
            (total - 100.0).abs() < 0.0001,
            "Event {:?} max impact score should be 100.0, got {}",
            event_type,
            total
        );

        let raw_points_sum: f64 = breakdown.components.iter().map(|c| c.points_awarded).sum();
        assert!(
            (raw_points_sum - 100.0).abs() < 0.0001,
            "Event {:?} sum of component points must equal 100.0 without structural clamp",
            event_type
        );
    }
}

// ---------------------------------------------------------------------------
// 15. Section 36: Event-Specific Impact Models (No Irrelevant Components)
// ---------------------------------------------------------------------------
#[test]
fn test_event_specific_impact_models_no_irrelevant_components() {
    // Test LongBlockInterval: must ONLY contain IntervalSeconds (weight 100.0)
    // Irrelevant metrics (e.g. ValueSats, FeeSats, CoinAgeDestroyed) must NOT appear.
    let lbi_rarities = vec![
        EventRarityResult {
            event_id: Uuid::new_v4(),
            baseline_run_id: Uuid::new_v4(),
            event_type: EventType::LongBlockInterval,
            metric: BaselineMetric::IntervalSeconds,
            value: MetricValue::U64(7200),
            percentile: Some(99.5),
            percentile_method: PercentileMethod::ExactEmpiricalCdf,
            estimated: false,
            rarity_band: RarityBand::Rare,
            population_size: 20_000,
            tail_count: 100,
            evaluation_mode: EvaluationMode::Retrospective,
        },
        // Injected irrelevant metrics from other event types:
        EventRarityResult {
            event_id: Uuid::new_v4(),
            baseline_run_id: Uuid::new_v4(),
            event_type: EventType::LongBlockInterval,
            metric: BaselineMetric::ValueSats,
            value: MetricValue::U64(10_000_000_000),
            percentile: Some(99.9),
            percentile_method: PercentileMethod::ExactEmpiricalCdf,
            estimated: false,
            rarity_band: RarityBand::Extreme,
            population_size: 20_000,
            tail_count: 2,
            evaluation_mode: EvaluationMode::Retrospective,
        },
    ];

    let lbi_impact =
        ImpactCalculator::calculate_impact(EventType::LongBlockInterval, &lbi_rarities, None);

    assert_eq!(
        lbi_impact.model_id,
        "obschain-impact-long-block-interval-v1"
    );
    assert_eq!(lbi_impact.components.len(), 1);
    assert_eq!(
        lbi_impact.components[0].component_name,
        "Network interval rarity"
    );
    assert_eq!(lbi_impact.components[0].weight, 100.0);
    assert_eq!(
        lbi_impact.components[0].metric,
        BaselineMetric::IntervalSeconds
    );

    // Verify ExtremeFee: only FeeRateSatVb (60) and FeeSats (40) = 100
    let fee_defs = obschain_core::MetricRegistry::metrics_for_event_type(EventType::ExtremeFee);
    assert_eq!(fee_defs.len(), 2);
    let fee_rarities = vec![
        EventRarityResult {
            event_id: Uuid::new_v4(),
            baseline_run_id: Uuid::new_v4(),
            event_type: EventType::ExtremeFee,
            metric: BaselineMetric::FeeRateSatVb,
            value: MetricValue::DecimalScaled {
                value: 15000,
                scale: 2,
            },
            percentile: Some(98.0),
            percentile_method: PercentileMethod::ExactEmpiricalCdf,
            estimated: false,
            rarity_band: RarityBand::Rare,
            population_size: 5_000,
            tail_count: 100,
            evaluation_mode: EvaluationMode::Retrospective,
        },
        EventRarityResult {
            event_id: Uuid::new_v4(),
            baseline_run_id: Uuid::new_v4(),
            event_type: EventType::ExtremeFee,
            metric: BaselineMetric::FeeSats,
            value: MetricValue::U64(50_000_000),
            percentile: Some(95.0),
            percentile_method: PercentileMethod::ExactEmpiricalCdf,
            estimated: false,
            rarity_band: RarityBand::Unusual,
            population_size: 5_000,
            tail_count: 250,
            evaluation_mode: EvaluationMode::Retrospective,
        },
    ];
    let fee_impact = ImpactCalculator::calculate_impact(EventType::ExtremeFee, &fee_rarities, None);
    assert_eq!(fee_impact.model_id, "obschain-impact-extreme-fee-v1");
    assert_eq!(fee_impact.components.len(), 2);
    assert_eq!(
        fee_impact.components[0].weight + fee_impact.components[1].weight,
        100.0
    );
}

// ---------------------------------------------------------------------------
// 16. Section 37: Impact Model Coverage Availability Thresholds
// ---------------------------------------------------------------------------
#[test]
fn test_impact_model_coverage_thresholds() {
    let event_type = EventType::LargeTransfer; // Has 4 components: Value (60), Inputs (15), Outputs (15), Vsize (10)
    let make_rarity = |metric: BaselineMetric| EventRarityResult {
        event_id: Uuid::new_v4(),
        baseline_run_id: Uuid::new_v4(),
        event_type,
        metric,
        value: MetricValue::U64(1_000),
        percentile: Some(90.0),
        percentile_method: PercentileMethod::ExactEmpiricalCdf,
        estimated: false,
        rarity_band: RarityBand::Unusual,
        population_size: 10_000,
        tail_count: 1_000,
        evaluation_mode: EvaluationMode::Retrospective,
    };

    // 100% coverage (4 of 4)
    let r4 = vec![
        make_rarity(BaselineMetric::ValueSats),
        make_rarity(BaselineMetric::InputCount),
        make_rarity(BaselineMetric::OutputCount),
        make_rarity(BaselineMetric::Vsize),
    ];
    let imp4 = ImpactCalculator::calculate_impact(event_type, &r4, None);
    assert_eq!(imp4.model_coverage, 1.0);
    assert!(imp4.total_score.is_some());

    // 75% coverage (3 of 4)
    let r3 = vec![
        make_rarity(BaselineMetric::ValueSats),
        make_rarity(BaselineMetric::InputCount),
        make_rarity(BaselineMetric::OutputCount),
    ];
    let imp3 = ImpactCalculator::calculate_impact(event_type, &r3, None);
    assert_eq!(imp3.model_coverage, 0.75);
    assert!(imp3.total_score.is_some());

    // 50% coverage (2 of 4)
    let r2 = vec![
        make_rarity(BaselineMetric::ValueSats),
        make_rarity(BaselineMetric::OutputCount),
    ];
    let imp2 = ImpactCalculator::calculate_impact(event_type, &r2, None);
    assert_eq!(imp2.model_coverage, 0.50);
    assert!(imp2.total_score.is_some());

    // 25% coverage (1 of 4) -> Below 50% minimum threshold, score must be suppressed!
    let r1 = vec![make_rarity(BaselineMetric::ValueSats)];
    let imp1 = ImpactCalculator::calculate_impact(event_type, &r1, None);
    assert_eq!(imp1.model_coverage, 0.25);
    assert_eq!(
        imp1.total_score, None,
        "Score must be suppressed when coverage < 50%"
    );

    // 0% coverage -> score must be None
    let imp0 = ImpactCalculator::calculate_impact(event_type, &[], None);
    assert_eq!(imp0.model_coverage, 0.0);
    assert_eq!(imp0.total_score, None);
}

// ---------------------------------------------------------------------------
// 17. Section 38: Full u128 Persistence & Lossless Round-Trip (No Scientific Notation or f64 Corruption)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_lossless_u128_max_and_large_cad_storage_roundtrip() {
    let u128_max = u128::MAX; // 340,282,366,920,938,463,463,374,607,431,768,211,455 (39 digits)
    let large_cad_val = 50_000_000_000u128 * 365 * 100_000_000; // 50B BTC-days in satoshi-days (26 digits)

    // 1. Rust Domain & String representation test
    let metric_val_max = MetricValue::U128(u128_max);
    let str_max = metric_val_max.to_numeric_string();
    assert_eq!(str_max, "340282366920938463463374607431768211455");

    let parsed_max =
        MetricValue::from_str_and_metric(&str_max, BaselineMetric::CoinAgeDestroyedSatoshiDays);
    assert_eq!(parsed_max.as_u128(), Some(u128_max));

    // Simulate PostgreSQL NUMERIC(50, 4) formatting (appends .0000)
    let pg_formatted_max = format!("{str_max}.0000");
    let parsed_pg = MetricValue::from_str_and_metric(
        &pg_formatted_max,
        BaselineMetric::CoinAgeDestroyedSatoshiDays,
    );
    assert_eq!(
        parsed_pg.as_u128(),
        Some(u128_max),
        "NUMERIC(50,4) text must parse u128::MAX without truncation or f64 float conversion"
    );

    let metric_val_cad = MetricValue::U128(large_cad_val);
    let pg_formatted_cad = format!("{}.0000", metric_val_cad.to_numeric_string());
    let parsed_cad = MetricValue::from_str_and_metric(
        &pg_formatted_cad,
        BaselineMetric::CoinAgeDestroyedSatoshiDays,
    );
    assert_eq!(parsed_cad.as_u128(), Some(large_cad_val));

    // 2. In-Memory Storage persistence round-trip
    let mem_storage = InMemoryStorage::new_empty(100);
    let event_id = Uuid::new_v4();
    let metric_row = obschain_core::EventMetricValue {
        id: Uuid::new_v4(),
        event_id,
        network: "mainnet".to_string(),
        event_type: EventType::DormantCoinsMoved,
        metric: BaselineMetric::CoinAgeDestroyedSatoshiDays,
        metric_definition_version: "coin-age-destroyed-satoshi-days-v1".to_string(),
        value: metric_val_max,
        metric_scale: 0,
        block_height: 840_000,
        event_time: Utc::now(),
        created_at: Utc::now(),
    };
    mem_storage.save_event_metrics(&[metric_row]).await.unwrap();

    let fetched = mem_storage.get_event_metrics(event_id).await.unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].value.as_u128(), Some(u128_max));

    // 3. PostgreSQL durable persistence round-trip (if PG reachable)
    if let Some(pg) = get_test_postgres_storage().await {
        let storage = Storage::from(pg);
        let pg_event_id = Uuid::new_v4();
        let pg_metric_row = obschain_core::EventMetricValue {
            id: Uuid::new_v4(),
            event_id: pg_event_id,
            network: "mainnet".to_string(),
            event_type: EventType::DormantCoinsMoved,
            metric: BaselineMetric::CoinAgeDestroyedSatoshiDays,
            metric_definition_version: "coin-age-destroyed-satoshi-days-v1".to_string(),
            value: metric_val_max,
            metric_scale: 0,
            block_height: 850_000,
            event_time: Utc::now(),
            created_at: Utc::now(),
        };
        storage.save_event_metrics(&[pg_metric_row]).await.unwrap();

        let pg_fetched = storage.get_event_metrics(pg_event_id).await.unwrap();
        assert_eq!(pg_fetched.len(), 1);
        assert_eq!(
            pg_fetched[0].value.as_u128(),
            Some(u128_max),
            "PostgreSQL NUMERIC(50, 4) must round-trip u128::MAX losslessly"
        );
    }
}

// ---------------------------------------------------------------------------
// 18. Section 39: Observation Dedup Invariant (1 Canonical Event = 1 Metric Row Set)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_observation_dedup_metric_table_invariant() {
    let storage = Storage::from(InMemoryStorage::new_empty(100));

    let txid = format!("dedup_tx_{}", Uuid::new_v4());
    let mut event = create_large_transfer_event(&txid, 500_000_000, 840_000, "mainnet");
    event = event.with_deterministic_id();
    let event_id = event.id;
    storage.save_event(&event).await.unwrap();

    let source = ObservationSource::bitcoin_core_rpc("http://127.0.0.1:8332");
    // Simulate 100 observation calls for the same canonical event
    for i in 0..100 {
        let obs = EventObservation::live(
            event_id,
            source.clone(),
            EventObservationKind::Confirmed,
            Utc::now(),
            Some(Utc::now()),
            Some(840_000),
            Some(format!("00000000000000000000000000000000{i:04x}")),
        );
        storage.save_event_observation(&obs).await.unwrap();
    }

    // Metric rows must belong to canonical ChainEvent ONLY, not EventObservations
    let metrics = storage.get_event_metrics(event_id).await.unwrap();
    let defs = obschain_core::MetricRegistry::metrics_for_event_type(EventType::LargeTransfer);
    assert_eq!(
        metrics.len(),
        defs.len(),
        "Expected exactly {} metric rows for canonical event, got {}",
        defs.len(),
        metrics.len()
    );

    // Save event again (replay / idempotent update)
    storage.save_event(&event).await.unwrap();
    let metrics_after = storage.get_event_metrics(event_id).await.unwrap();
    assert_eq!(
        metrics_after.len(),
        defs.len(),
        "Metric rows must remain strictly deduplicated after replay"
    );
}
