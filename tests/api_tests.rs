use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use chrono::Utc;
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

    let state = AppState {
        storage,
        detectors,
        started_at: Utc::now(),
        is_mock_feed: true,
    };

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
