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
