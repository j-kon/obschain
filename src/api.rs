use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use obschain_core::ChainEvent;
use obschain_detectors::Detector;
use obschain_storage::{EventRepository, InMemoryStorage, IncidentRepository};
use serde::{Deserialize, Serialize};
use tower_http::{
    cors::{Any, CorsLayer},
    limit::RequestBodyLimitLayer,
    trace::TraceLayer,
};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourcesStatus {
    pub mempool_rest: String,
    pub mempool_websocket: String,
    pub bitcoin_core: String,
}

#[derive(Clone, Default)]
pub struct PipelineMetrics {
    pub transactions_observed: Arc<AtomicU64>,
    pub blocks_observed: Arc<AtomicU64>,
    pub transactions_enriched: Arc<AtomicU64>,
    pub utxo_lookup_failures: Arc<AtomicU64>,
    pub cache_hits: Arc<AtomicU64>,
    pub cache_misses: Arc<AtomicU64>,
    pub events_generated: Arc<AtomicU64>,
    pub events_deduplicated: Arc<AtomicU64>,
}

impl PipelineMetrics {
    pub fn snapshot(&self) -> PipelineMetricsResponse {
        PipelineMetricsResponse {
            transactions_observed: self.transactions_observed.load(Ordering::Relaxed),
            blocks_observed: self.blocks_observed.load(Ordering::Relaxed),
            transactions_enriched: self.transactions_enriched.load(Ordering::Relaxed),
            utxo_lookup_failures: self.utxo_lookup_failures.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            events_generated: self.events_generated.load(Ordering::Relaxed),
            events_deduplicated: self.events_deduplicated.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineMetricsResponse {
    pub transactions_observed: u64,
    pub blocks_observed: u64,
    pub transactions_enriched: u64,
    pub utxo_lookup_failures: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub events_generated: u64,
    pub events_deduplicated: u64,
}

#[derive(Clone)]
pub struct AppState {
    pub storage: InMemoryStorage,
    pub detectors: Vec<Arc<dyn Detector>>,
    pub started_at: DateTime<Utc>,
    pub is_mock_feed: bool,
    pub event_broadcaster: tokio::sync::broadcast::Sender<ChainEvent>,
    pub tip_height: Arc<AtomicU64>,
    pub events_detected: Arc<AtomicU64>,
    pub sources: Arc<RwLock<SourcesStatus>>,
    pub metrics: PipelineMetrics,
}

impl AppState {
    pub fn new(
        storage: InMemoryStorage,
        detectors: Vec<Arc<dyn Detector>>,
        is_mock_feed: bool,
    ) -> (Self, tokio::sync::broadcast::Sender<ChainEvent>) {
        Self::with_metrics(storage, detectors, is_mock_feed, PipelineMetrics::default())
    }

    pub fn with_metrics(
        storage: InMemoryStorage,
        detectors: Vec<Arc<dyn Detector>>,
        is_mock_feed: bool,
        metrics: PipelineMetrics,
    ) -> (Self, tokio::sync::broadcast::Sender<ChainEvent>) {
        let (tx, _) = tokio::sync::broadcast::channel(1024);
        let state = Self {
            storage,
            detectors,
            started_at: Utc::now(),
            is_mock_feed,
            event_broadcaster: tx.clone(),
            tip_height: Arc::new(AtomicU64::new(0)),
            events_detected: Arc::new(AtomicU64::new(0)),
            sources: Arc::new(RwLock::new(SourcesStatus {
                mempool_rest: "configured".to_string(),
                mempool_websocket: "disconnected".to_string(),
                bitcoin_core: "not_configured".to_string(),
            })),
            metrics,
        };
        (state, tx)
    }
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub timestamp: DateTime<Utc>,
    pub version: &'static str,
}

#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub service: &'static str,
    pub network: &'static str,
    pub status: &'static str,
    pub version: &'static str,
    pub engine: &'static str,
    pub timestamp: DateTime<Utc>,
    pub uptime_seconds: i64,
    pub sources: SourcesStatus,
    pub tip_height: u64,
    pub events_detected: u64,
    pub active_detectors: Vec<&'static str>,
    pub storage_backend: &'static str,
    pub is_mock_feed: bool,
    pub metrics: PipelineMetricsResponse,
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
        .route("/api/v1/ws", get(ws_handler))
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
    let sources = state
        .sources
        .read()
        .map(|s| s.clone())
        .unwrap_or_else(|_| SourcesStatus {
            mempool_rest: "unknown".to_string(),
            mempool_websocket: "unknown".to_string(),
            bitcoin_core: "not_configured".to_string(),
        });
    let tip_height = state.tip_height.load(Ordering::Relaxed);
    let events_detected = state.events_detected.load(Ordering::Relaxed);

    Json(StatusResponse {
        service: "obschain",
        network: "bitcoin",
        status: "running",
        version: env!("CARGO_PKG_VERSION"),
        engine: "ObsChain Core v0.1.0",
        timestamp: now,
        uptime_seconds: uptime,
        sources,
        tip_height,
        events_detected,
        active_detectors,
        storage_backend: "in-memory (bounded VecDeque)",
        is_mock_feed: state.is_mock_feed,
        metrics: state.metrics.snapshot(),
    })
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_client_socket(socket, state))
}

async fn handle_client_socket(mut socket: WebSocket, state: AppState) {
    let mut rx = state.event_broadcaster.subscribe();
    tracing::debug!("ObsChain WebSocket client connected");

    loop {
        tokio::select! {
            client_msg = socket.recv() => {
                match client_msg {
                    Some(Ok(Message::Close(_))) | None => {
                        tracing::debug!("ObsChain WebSocket client disconnected");
                        break;
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        tracing::debug!("WebSocket client receive error: {e}");
                        break;
                    }
                }
            }
            event_result = rx.recv() => {
                match event_result {
                    Ok(event) => {
                        let json = match serde_json::to_string(&event) {
                            Ok(j) => j,
                            Err(e) => {
                                tracing::error!("Failed to serialize ChainEvent for WebSocket broadcast: {e}");
                                continue;
                            }
                        };
                        if socket.send(Message::Text(json.into())).await.is_err() {
                            tracing::debug!("ObsChain WebSocket client dropped during send");
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!("Slow WebSocket client lagged, skipped {skipped} events");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        }
    }
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
