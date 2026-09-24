use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use chrono::Utc;
use obschain::{create_router, AppState, PipelineMetrics};
use obschain_core::{
    ObservationSource, TransactionObservation, TxInputObservation, TxOutputObservation,
};
use obschain_intelligence::IncidentWatchEngine;
use obschain_storage::{InMemoryStorage, IncidentActivityRepository, IncidentRepository};
use tower::ServiceExt;

#[tokio::test]
async fn test_incident_watch_pipeline_end_to_end() {
    let storage = InMemoryStorage::new_empty(100);
    let detectors: Vec<Arc<dyn obschain_detectors::Detector>> = vec![];
    let metrics = PipelineMetrics::default();

    let mut watch_engine = IncidentWatchEngine::from_env();
    let canonical_targets = obschain_incidents::canonical_liquid_watch_targets();
    watch_engine.load_targets(canonical_targets);
    watch_engine.register_incident_title(
        obschain_incidents::LIQUID_CASE_ID,
        "Liquid Network Security Incident",
    );
    let watch_engine_arc = Arc::new(tokio::sync::RwLock::new(watch_engine));

    let (state, _) = AppState::with_metrics_and_watch_engine(
        storage.clone(),
        detectors,
        true,
        metrics.clone(),
        watch_engine_arc.clone(),
    );

    let app = create_router(state.clone());

    // 1. Verify watch targets endpoint returns 6 public targets with redacted internals
    let req_targets = Request::builder()
        .uri("/api/v1/incidents/OC-2026-0001/watch-targets")
        .body(Body::empty())
        .unwrap();
    let res_targets = app.clone().oneshot(req_targets).await.unwrap();
    assert_eq!(res_targets.status(), StatusCode::OK);

    let body_targets = axum::body::to_bytes(res_targets.into_body(), usize::MAX)
        .await
        .unwrap();
    let json_targets: serde_json::Value = serde_json::from_slice(&body_targets).unwrap();
    assert_eq!(json_targets["case_id"], "OC-2026-0001");
    let targets = json_targets["watch_targets"].as_array().unwrap();
    assert_eq!(targets.len(), 6);

    // Verify all targets have valid public representations
    for target in targets {
        assert!(target["id"].is_string());
        assert!(target["target_type"].is_string());
        assert!(target["target_summary"].is_string());
        assert!(target["classification"].is_string());
        assert_eq!(target["case_id"], "OC-2026-0001");
        assert!(target["active"].as_bool().unwrap());
    }

    // 2. Simulate transaction spending the Liquid peg-out output (vout 0)
    let pegout_spend_tx = TransactionObservation {
        txid: "00000000000000000000000000000000000000000000000000000000deadbeef".to_string(),
        timestamp: Utc::now(),
        block_hash: None,
        block_height: None,
        fee_sats: 50_000,
        size: 250,
        weight: 1000,
        vsize: 250,
        fee_rate_sat_vb: Some(20.0),
        total_input_sats: 399_602_000_000,
        total_output_sats: 399_601_950_000,
        input_count: 1,
        output_count: 2,
        inputs: vec![TxInputObservation {
            txid: "8db751a650ae2f12006b7e8c69a75e4df360e8afd6b9e05ae0b9fa6458a7b140".to_string(),
            vout: 0,
            sequence: 0xfffffffd,
            prev_out_value_sats: Some(399_602_000_000),
            prev_out_address: Some("bc1qexploitintermediarycluster0000000000000000".to_string()),
            is_coinbase: false,
            historical_utxo: None,
        }],
        outputs: vec![
            TxOutputObservation {
                value_sats: 340_000_000_000,
                n: 0,
                script_pubkey_type: Some("v0_p2wpkh".to_string()),
                address: Some("bc1qliquidfedreturnaddress965950m0000000000000".to_string()),
                scriptpubkey_hex: Some("0014a1b2c3d4e5f678901234567890abcdef12345678".to_string()),
            },
            TxOutputObservation {
                value_sats: 59_601_950_000,
                n: 1,
                script_pubkey_type: Some("v0_p2wpkh".to_string()),
                address: Some("bc1qchange00000000000000000000000000000000000".to_string()),
                scriptpubkey_hex: None,
            },
        ],
        is_rbf: false,
        confirmed: false,
        source: Some(ObservationSource::mempool_ws(
            "wss://mempool.space/api/v1/ws",
        )),
    };

    // Run transaction through watch engine
    let matches = {
        let mut engine = watch_engine_arc.write().await;
        engine.process_transaction(&pegout_spend_tx, None, None)
    };

    assert!(!matches.is_empty(), "Peg-out spend must produce matches");
    for (activity, alert_opt) in &matches {
        storage.save_activity(activity).await.unwrap();
        metrics
            .incident_activities_detected
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Some(_alert) = alert_opt {
            metrics
                .incident_alerts_emitted
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    // 3. Query incident-scoped activity endpoint
    let req_act = Request::builder()
        .uri("/api/v1/incidents/OC-2026-0001/activity")
        .body(Body::empty())
        .unwrap();
    let res_act = app.clone().oneshot(req_act).await.unwrap();
    assert_eq!(res_act.status(), StatusCode::OK);

    let body_act = axum::body::to_bytes(res_act.into_body(), usize::MAX)
        .await
        .unwrap();
    let json_act: serde_json::Value = serde_json::from_slice(&body_act).unwrap();
    assert_eq!(json_act["case_id"], "OC-2026-0001");
    let act_list = json_act["activity"].as_array().unwrap();
    assert!(!act_list.is_empty());

    // Verify outpoint spend detection details
    let outpoint_spend = act_list
        .iter()
        .find(|a| a["activity_type"] == "WATCHED_OUTPOINT_SPENT")
        .expect("Watched outpoint spend activity must be present");
    assert_eq!(outpoint_spend["correlation_strength"], "DIRECT");
    assert_eq!(outpoint_spend["confidence"], "ON_CHAIN_VERIFIED");
    assert_eq!(outpoint_spend["status"], "MEMPOOL");

    // 4. Verify global activity feed
    let req_global = Request::builder()
        .uri("/api/v1/incident-activity")
        .body(Body::empty())
        .unwrap();
    let res_global = app.clone().oneshot(req_global).await.unwrap();
    assert_eq!(res_global.status(), StatusCode::OK);

    // 5. Verify /api/v1/status metrics reflect incident watch telemetry
    let req_status = Request::builder()
        .uri("/api/v1/status")
        .body(Body::empty())
        .unwrap();
    let res_status = app.clone().oneshot(req_status).await.unwrap();
    assert_eq!(res_status.status(), StatusCode::OK);
    let body_status = axum::body::to_bytes(res_status.into_body(), usize::MAX)
        .await
        .unwrap();
    let json_status: serde_json::Value = serde_json::from_slice(&body_status).unwrap();

    assert_eq!(json_status["incident_watch_targets"], 6);
    assert!(
        json_status["incident_activities_detected"]
            .as_u64()
            .unwrap()
            >= 1
    );
    assert!(json_status["incident_alerts_emitted"].as_u64().unwrap() >= 1);

    // 6. STRICT RECOVERY IMMUTABILITY CHECK:
    // Ensure the incident's recovered_sats was NOT mutated by the movement!
    let incident = storage
        .get_incident_by_id_or_case_id("OC-2026-0001")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        incident.recovery.recovered_sats, 340_000_000_000,
        "Recovery balance must remain untouched by fund movement"
    );
    assert_eq!(
        incident.recovery.affected_sats, 399_602_000_000,
        "Affected balance must remain untouched by fund movement"
    );
}
