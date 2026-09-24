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
