use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use obschain::{create_router, AppState};
use obschain_detectors::{LargeTransactionDetector, LongBlockIntervalDetector};
use obschain_storage::InMemoryStorage;
use tower::ServiceExt;
use uuid::Uuid;

fn test_app() -> axum::Router {
    let storage = InMemoryStorage::new();
    let detectors: Vec<Arc<dyn obschain_detectors::Detector>> = vec![
        Arc::new(LargeTransactionDetector::new()),
        Arc::new(LongBlockIntervalDetector::new()),
    ];

    let (state, _) = AppState::new(storage, detectors, true);

    create_router(state)
}

#[tokio::test]
async fn test_health_check() {
    let app = test_app();
    let req = Request::builder()
        .uri("/health")
        .body(Body::empty())
        .unwrap();

    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_status_endpoint() {
    let app = test_app();
    let req = Request::builder()
        .uri("/api/v1/status")
        .body(Body::empty())
        .unwrap();

    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let status_json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(status_json["service"], "obschain");
    assert_eq!(status_json["network"], "bitcoin");
    assert_eq!(status_json["status"], "running");
    assert!(status_json["sources"].is_object());
    assert_eq!(status_json["tip_height"], 0);
    assert_eq!(status_json["events_detected"], 0);
    assert!(status_json["bitcoin_core"].is_object());
    assert_eq!(status_json["bitcoin_core"]["enabled"], false);
    assert_eq!(status_json["bitcoin_core"]["connected"], false);
    assert_eq!(
        status_json["bitcoin_core"]["zmq"]["rawtx"],
        "not_configured"
    );
}

#[tokio::test]
async fn test_list_events() {
    let app = test_app();
    let req = Request::builder()
        .uri("/api/v1/events")
        .body(Body::empty())
        .unwrap();

    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_get_event_not_found() {
    let app = test_app();
    let random_id = Uuid::new_v4();
    let req = Request::builder()
        .uri(format!("/api/v1/events/{random_id}"))
        .body(Body::empty())
        .unwrap();

    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_list_incidents() {
    let app = test_app();
    let req = Request::builder()
        .uri("/api/v1/incidents")
        .body(Body::empty())
        .unwrap();

    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let incidents = json["incidents"].as_array().unwrap();
    assert!(
        !incidents.is_empty(),
        "Should return seeded canonical incident"
    );
    assert_eq!(incidents[0]["case_id"], "OC-2026-0001");
}

#[tokio::test]
async fn test_get_incident_by_case_id_and_uuid() {
    // 1. By Case ID: OC-2026-0001
    let app1 = test_app();
    let req1 = Request::builder()
        .uri("/api/v1/incidents/OC-2026-0001")
        .body(Body::empty())
        .unwrap();
    let res1 = app1.oneshot(req1).await.unwrap();
    assert_eq!(res1.status(), StatusCode::OK);

    let body1 = axum::body::to_bytes(res1.into_body(), usize::MAX)
        .await
        .unwrap();
    let incident: obschain_core::Incident = serde_json::from_slice(&body1).unwrap();
    assert_eq!(incident.case_id, "OC-2026-0001");
    assert_eq!(incident.recovery.affected_sats, 399_602_000_000);
    assert_eq!(incident.recovery.recovered_sats, 340_000_000_000);
    assert_eq!(incident.recovery.outstanding_sats, 59_602_000_000);

    // 2. By UUID
    let app2 = test_app();
    let req2 = Request::builder()
        .uri(format!("/api/v1/incidents/{}", incident.id))
        .body(Body::empty())
        .unwrap();
    let res2 = app2.oneshot(req2).await.unwrap();
    assert_eq!(res2.status(), StatusCode::OK);

    // 3. Not Found
    let app3 = test_app();
    let req3 = Request::builder()
        .uri("/api/v1/incidents/OC-NONEXISTENT")
        .body(Body::empty())
        .unwrap();
    let res3 = app3.oneshot(req3).await.unwrap();
    assert_eq!(res3.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_get_incident_subresources() {
    let app = test_app();

    // Timeline sub-resource
    let req = Request::builder()
        .uri("/api/v1/incidents/OC-2026-0001/timeline")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["timeline"].as_array().unwrap().len() >= 10);

    // Evidence sub-resource
    let app2 = test_app();
    let req2 = Request::builder()
        .uri("/api/v1/incidents/OC-2026-0001/evidence")
        .body(Body::empty())
        .unwrap();
    let res2 = app2.oneshot(req2).await.unwrap();
    assert_eq!(res2.status(), StatusCode::OK);

    // Graph sub-resource
    let app3 = test_app();
    let req3 = Request::builder()
        .uri("/api/v1/incidents/OC-2026-0001/graph")
        .body(Body::empty())
        .unwrap();
    let res3 = app3.oneshot(req3).await.unwrap();
    assert_eq!(res3.status(), StatusCode::OK);
    let graph_body = axum::body::to_bytes(res3.into_body(), usize::MAX)
        .await
        .unwrap();
    let graph: obschain_core::IncidentGraph = serde_json::from_slice(&graph_body).unwrap();
    assert!(!graph.nodes.is_empty());
    assert!(!graph.edges.is_empty());
}

#[tokio::test]
async fn test_ws_live_broadcast() {
    use futures_util::StreamExt;
    use obschain_core::{ChainEvent, ConfidenceLevel, EventSeverity, EventType};

    let storage = InMemoryStorage::new_empty(100);
    let detectors: Vec<Arc<dyn obschain_detectors::Detector>> = vec![];
    let (state, broadcaster) = AppState::new(storage, detectors, false);
    let app = create_router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let ws_url = format!("ws://{}/api/v1/ws", addr);
    let (ws_stream, _) = tokio_tungstenite::connect_async(ws_url).await.unwrap();
    let (_, mut read) = ws_stream.split();

    // Broadcast a test event
    let event = ChainEvent::new(
        EventType::LargeTransfer,
        EventSeverity::High,
        ConfidenceLevel::VerifiedOnChain,
        "Test Live WS Broadcast".to_string(),
        "Testing ObsChain frontend WebSocket feed".to_string(),
    );
    let _ = broadcaster.send(event);

    // Read message from WS client
    let msg = read.next().await.unwrap().unwrap();
    if let tokio_tungstenite::tungstenite::Message::Text(txt) = msg {
        let received: ChainEvent = serde_json::from_str(&txt).unwrap();
        assert_eq!(received.title, "Test Live WS Broadcast");
    } else {
        panic!("Expected text frame from WebSocket");
    }
}

#[tokio::test]
async fn test_get_incident_watch_targets() {
    let app = test_app();
    let req = Request::builder()
        .uri("/api/v1/incidents/OC-2026-0001/watch-targets")
        .body(Body::empty())
        .unwrap();

    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["case_id"], "OC-2026-0001");
    let targets = json["watch_targets"].as_array().unwrap();
    assert_eq!(targets.len(), 6);

    // Verify public watch target shape
    let first = &targets[0];
    assert!(first["target_type"].is_string());
    assert!(first["target_summary"].is_string());
    assert!(first["classification"].is_string());
    assert_eq!(first["case_id"], "OC-2026-0001");
    assert!(first["active"].as_bool().unwrap());
}

#[tokio::test]
async fn test_incident_activity_endpoints() {
    use chrono::Utc;
    use obschain_core::{
        ActivityStatus, CorrelationStrength, IncidentActivity, IncidentActivityType,
        ObservationSource, ProvenanceClassification,
    };
    use obschain_storage::IncidentActivityRepository;

    let storage = InMemoryStorage::new_empty(100);
    let detectors: Vec<Arc<dyn obschain_detectors::Detector>> = vec![];
    let (state, _) = AppState::new(storage.clone(), detectors, false);

    // Create and save test activity for Liquid incident
    let liquid_id = Uuid::parse_str("0c202600-0001-0000-0000-000000000001").unwrap();
    let target_id = Uuid::new_v4();
    let act = IncidentActivity {
        id: Uuid::new_v4(),
        incident_id: liquid_id,
        case_id: "OC-2026-0001".to_string(),
        activity_type: IncidentActivityType::WatchedOutpointSpent,
        observed_at: Utc::now(),
        trigger_txid: Some("tx_test_activity_001".to_string()),
        block_height: None,
        block_hash: None,
        value_sats: Some(100_000_000),
        watch_target_id: target_id,
        confidence: ProvenanceClassification::OnChainVerified,
        correlation_strength: CorrelationStrength::Direct,
        status: ActivityStatus::Mempool,
        source: ObservationSource::new("bitcoin", "websocket", None),
        evidence: vec![],
        description: "Test outpoint spent".to_string(),
        details: None,
        dedup_key: "act_test_key".to_string(),
    };
    storage.save_activity(&act).await.unwrap();

    let app = create_router(state);

    // 1. Test incident-scoped activity endpoint
    let req1 = Request::builder()
        .uri("/api/v1/incidents/OC-2026-0001/activity")
        .body(Body::empty())
        .unwrap();
    let res1 = app.clone().oneshot(req1).await.unwrap();
    assert_eq!(res1.status(), StatusCode::OK);
    let body1 = axum::body::to_bytes(res1.into_body(), usize::MAX)
        .await
        .unwrap();
    let json1: serde_json::Value = serde_json::from_slice(&body1).unwrap();
    assert_eq!(json1["count"], 1);
    assert_eq!(json1["activity"][0]["trigger_txid"], "tx_test_activity_001");

    // 2. Test global activity endpoint
    let req2 = Request::builder()
        .uri("/api/v1/incident-activity")
        .body(Body::empty())
        .unwrap();
    let res2 = app.oneshot(req2).await.unwrap();
    assert_eq!(res2.status(), StatusCode::OK);
    let body2 = axum::body::to_bytes(res2.into_body(), usize::MAX)
        .await
        .unwrap();
    let json2: serde_json::Value = serde_json::from_slice(&body2).unwrap();
    assert_eq!(json2["count"], 1);
    assert_eq!(json2["activity"][0]["trigger_txid"], "tx_test_activity_001");
}

#[tokio::test]
async fn test_ws_incident_broadcast() {
    use chrono::Utc;
    use futures_util::StreamExt;
    use obschain::WebSocketBroadcast;
    use obschain_core::{
        ActivityStatus, CorrelationStrength, EventSeverity, IncidentActivity, IncidentActivityType,
        IncidentAlert, ObservationSource, ProvenanceClassification,
    };

    let storage = InMemoryStorage::new_empty(100);
    let detectors: Vec<Arc<dyn obschain_detectors::Detector>> = vec![];
    let (state, _) = AppState::new(storage, detectors, false);
    let ws_broadcaster = state.ws_broadcaster.clone();
    let app = create_router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let ws_url = format!("ws://{}/api/v1/ws", addr);
    let (ws_stream, _) = tokio_tungstenite::connect_async(ws_url).await.unwrap();
    let (_, mut read) = ws_stream.split();

    // Broadcast IncidentActivity
    let act = IncidentActivity {
        id: Uuid::new_v4(),
        incident_id: Uuid::new_v4(),
        case_id: "OC-2026-0001".to_string(),
        activity_type: IncidentActivityType::WatchedOutpointSpent,
        observed_at: Utc::now(),
        trigger_txid: Some("tx_ws_act".to_string()),
        block_height: None,
        block_hash: None,
        value_sats: Some(50_000_000),
        watch_target_id: Uuid::new_v4(),
        confidence: ProvenanceClassification::OnChainVerified,
        correlation_strength: CorrelationStrength::Direct,
        status: ActivityStatus::Mempool,
        source: ObservationSource::new("bitcoin", "websocket", None),
        evidence: vec![],
        description: "WS Activity Test".to_string(),
        details: None,
        dedup_key: "ws_key_1".to_string(),
    };
    let _ = ws_broadcaster.send(WebSocketBroadcast::incident_activity(act));

    let msg1 = read.next().await.unwrap().unwrap();
    if let tokio_tungstenite::tungstenite::Message::Text(txt) = msg1 {
        let json: serde_json::Value = serde_json::from_str(&txt).unwrap();
        assert_eq!(json["type"], "incident_activity");
        assert_eq!(json["data"]["trigger_txid"], "tx_ws_act");
    } else {
        panic!("Expected text frame");
    }

    // Broadcast IncidentAlert
    let alert = IncidentAlert {
        id: Uuid::new_v4(),
        incident_id: Uuid::new_v4(),
        case_id: "OC-2026-0001".to_string(),
        incident_title: "Liquid Network Incident".to_string(),
        activity_id: Uuid::new_v4(),
        severity: EventSeverity::Critical,
        title: "[OC-2026-0001] Watched Outpoint Spent".to_string(),
        summary: "Critical spend observed".to_string(),
        confidence: ProvenanceClassification::OnChainVerified,
        correlation_strength: CorrelationStrength::Direct,
        observed_at: Utc::now(),
        value_sats: Some(399_602_000_000),
        trigger_txid: Some("tx_ws_alert".to_string()),
    };
    let _ = ws_broadcaster.send(WebSocketBroadcast::incident_alert(alert));

    let msg2 = read.next().await.unwrap().unwrap();
    if let tokio_tungstenite::tungstenite::Message::Text(txt) = msg2 {
        let json: serde_json::Value = serde_json::from_str(&txt).unwrap();
        assert_eq!(json["type"], "incident_alert");
        assert_eq!(json["data"]["trigger_txid"], "tx_ws_alert");
        assert_eq!(json["data"]["severity"], "CRITICAL");
    } else {
        panic!("Expected text frame");
    }
}

#[tokio::test]
async fn test_replay_api_disabled_by_default() {
    let app = test_app();
    let body = serde_json::json!({
        "start_height": 900000,
        "end_height": 901000
    });

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/replay/jobs")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();

    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["code"], 403);
    assert!(json["error"].as_str().unwrap().contains("disabled"));
}

#[tokio::test]
async fn test_replay_jobs_listing_and_not_found() {
    let app = test_app();

    // List jobs
    let req = Request::builder()
        .uri("/api/v1/replay/jobs")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["count"], 0);

    // Job not found
    let random_id = Uuid::new_v4();
    let req2 = Request::builder()
        .uri(format!("/api/v1/replay/jobs/{random_id}"))
        .body(Body::empty())
        .unwrap();
    let res2 = app.oneshot(req2).await.unwrap();
    assert_eq!(res2.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_events_research_filter_query() {
    use chrono::Utc;
    use obschain_core::{ChainEvent, ConfidenceLevel, EventSeverity, EventType, ObservationMode};
    use obschain_storage::EventRepository;

    let storage = InMemoryStorage::new_empty(1000);

    let ev1 = ChainEvent {
        id: Uuid::new_v4(),
        event_type: EventType::LargeTransfer,
        severity: EventSeverity::High,
        confidence: ConfidenceLevel::High,
        title: "Large Transfer 1".to_string(),
        description: "100 BTC".to_string(),
        detected_at: Utc::now(),
        block_height: Some(150),
        block_hash: None,
        txid: Some("tx1".to_string()),
        source: None,
        witnesses: vec![],
        metadata: serde_json::json!({}),
        observation_mode: ObservationMode::Live,
        replay_job_id: None,
    };

    let ev2 = ChainEvent {
        id: Uuid::new_v4(),
        event_type: EventType::LongBlockInterval,
        severity: EventSeverity::Medium,
        confidence: ConfidenceLevel::High,
        title: "Long Interval".to_string(),
        description: "45 mins".to_string(),
        detected_at: Utc::now(),
        block_height: Some(250),
        block_hash: None,
        txid: None,
        source: None,
        witnesses: vec![],
        metadata: serde_json::json!({}),
        observation_mode: ObservationMode::HistoricalReplay,
        replay_job_id: Some(Uuid::new_v4()),
    };

    storage.save_event(&ev1).await.unwrap();
    storage.save_event(&ev2).await.unwrap();

    let detectors: Vec<Arc<dyn obschain_detectors::Detector>> = vec![];
    let (state, _) = AppState::new(storage, detectors, false);
    let app = create_router(state);

    // Filter by height range 100..200
    let req = Request::builder()
        .uri("/api/v1/events?from_height=100&to_height=200")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["count"], 1);
    assert_eq!(json["events"][0]["block_height"], 150);

    // Filter by observation_mode=HISTORICAL_REPLAY
    let req2 = Request::builder()
        .uri("/api/v1/events?observation_mode=HISTORICAL_REPLAY")
        .body(Body::empty())
        .unwrap();
    let res2 = app.clone().oneshot(req2).await.unwrap();
    assert_eq!(res2.status(), StatusCode::OK);
    let bytes2 = axum::body::to_bytes(res2.into_body(), usize::MAX)
        .await
        .unwrap();
    let json2: serde_json::Value = serde_json::from_slice(&bytes2).unwrap();
    assert_eq!(json2["count"], 1);
    assert_eq!(json2["events"][0]["block_height"], 250);

    // Filter by event_type=LARGE_TRANSFER
    let req3 = Request::builder()
        .uri("/api/v1/events?event_type=LARGE_TRANSFER")
        .body(Body::empty())
        .unwrap();
    let res3 = app.oneshot(req3).await.unwrap();
    assert_eq!(res3.status(), StatusCode::OK);
    let bytes3 = axum::body::to_bytes(res3.into_body(), usize::MAX)
        .await
        .unwrap();
    let json3: serde_json::Value = serde_json::from_slice(&bytes3).unwrap();
    assert_eq!(json3["count"], 1);
    assert_eq!(json3["events"][0]["event_type"], "LARGE_TRANSFER");
}
