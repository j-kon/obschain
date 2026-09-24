use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use obschain_detectors::Detector;
use obschain_storage::{EventRepository, InMemoryStorage, IncidentRepository};
use serde::{Deserialize, Serialize};
use tower_http::{
    cors::{Any, CorsLayer},
    limit::RequestBodyLimitLayer,
    trace::TraceLayer,
};
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    pub storage: InMemoryStorage,
    pub detectors: Vec<Arc<dyn Detector>>,
    pub started_at: DateTime<Utc>,
    pub is_mock_feed: bool,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub timestamp: DateTime<Utc>,
    pub version: &'static str,
}

#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub status: &'static str,
    pub version: &'static str,
    pub network: &'static str,
    pub engine: &'static str,
    pub timestamp: DateTime<Utc>,
    pub uptime_seconds: i64,
    pub active_detectors: Vec<&'static str>,
    pub storage_backend: &'static str,
    pub is_mock_feed: bool,
    pub mock_data_disclaimer: &'static str,
}

#[derive(Debug, Deserialize)]
pub struct PaginationQuery {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct ApiErrorResponse {
    pub error: String,
    pub code: u16,
}

pub fn create_router(state: AppState) -> Router {
    // Permissive CORS for local frontend development
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .route("/health", get(health_handler))
        .route("/api/v1/status", get(status_handler))
        .route("/api/v1/events", get(list_events_handler))
        .route("/api/v1/events/{id}", get(get_event_handler))
        .route("/api/v1/incidents", get(list_incidents_handler))
        .route("/api/v1/incidents/{id}", get(get_incident_handler))
        .layer(TraceLayer::new_for_http())
        // Guard against oversized request DOS (limit to 1MB)
        .layer(RequestBodyLimitLayer::new(1024 * 1024))
        .layer(cors)
        .with_state(state)
}

async fn health_handler() -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok",
        timestamp: Utc::now(),
        version: env!("CARGO_PKG_VERSION"),
    })
}

async fn status_handler(State(state): State<AppState>) -> impl IntoResponse {
    let now = Utc::now();
    let uptime = (now - state.started_at).num_seconds();
    let active_detectors = state.detectors.iter().map(|d| d.name()).collect();

    Json(StatusResponse {
        status: "operational",
        version: env!("CARGO_PKG_VERSION"),
        network: "bitcoin-mainnet",
        engine: "ObsChain Core v0.1.0",
        timestamp: now,
        uptime_seconds: uptime,
        active_detectors,
        storage_backend: "in-memory (mock demo seed)",
        is_mock_feed: state.is_mock_feed,
        mock_data_disclaimer:
            "Initial seed data contains simulated/mock events and incidents clearly demarcated.",
    })
}

async fn list_events_handler(
    State(state): State<AppState>,
    Query(pagination): Query<PaginationQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiErrorResponse>)> {
    // Bound limit to prevent denial of service (unbounded memory allocations)
    let limit = pagination.limit.unwrap_or(50).clamp(1, 100);
    let offset = pagination.offset.unwrap_or(0);

    let events = state
        .storage
        .list_events(limit, offset)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiErrorResponse {
                    error: e.to_string(),
                    code: 500,
                }),
            )
        })?;

    Ok(Json(serde_json::json!({
        "events": events,
        "count": events.len(),
        "limit": limit,
        "offset": offset,
        "is_mock_feed": state.is_mock_feed
    })))
}

async fn get_event_handler(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiErrorResponse>)> {
    let event_opt = state.storage.get_event_by_id(id).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiErrorResponse {
                error: e.to_string(),
                code: 500,
            }),
        )
    })?;

    match event_opt {
        Some(event) => Ok(Json(event)),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ApiErrorResponse {
                error: format!("ChainEvent with id '{id}' not found"),
                code: 404,
            }),
        )),
    }
}

async fn list_incidents_handler(
    State(state): State<AppState>,
    Query(pagination): Query<PaginationQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiErrorResponse>)> {
    let limit = pagination.limit.unwrap_or(50).clamp(1, 100);
    let offset = pagination.offset.unwrap_or(0);

    let incidents = state
        .storage
        .list_incidents(limit, offset)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiErrorResponse {
                    error: e.to_string(),
                    code: 500,
                }),
            )
        })?;

    Ok(Json(serde_json::json!({
        "incidents": incidents,
        "count": incidents.len(),
        "limit": limit,
        "offset": offset,
        "is_mock_feed": state.is_mock_feed
    })))
}

async fn get_incident_handler(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiErrorResponse>)> {
    let incident_opt = state.storage.get_incident_by_id(id).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiErrorResponse {
                error: e.to_string(),
                code: 500,
            }),
        )
    })?;

    match incident_opt {
        Some(incident) => Ok(Json(incident)),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ApiErrorResponse {
                error: format!("Incident with id '{id}' not found"),
                code: 404,
            }),
        )),
    }
}
