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
use obschain_core::{
    ActivityStatus, ChainEvent, IncidentActivity, IncidentAlert, PublicWatchTarget,
};
use obschain_detectors::Detector;
use obschain_intelligence::IncidentWatchEngine;
use obschain_storage::{
    EventRepository, InMemoryStorage, IncidentActivityRepository, IncidentRepository,
    WatchTargetRepository,
};
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
    pub incident_activities_detected: Arc<AtomicU64>,
    pub incident_alerts_emitted: Arc<AtomicU64>,
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
            incident_activities_detected: self.incident_activities_detected.load(Ordering::Relaxed),
            incident_alerts_emitted: self.incident_alerts_emitted.load(Ordering::Relaxed),
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
    pub incident_activities_detected: u64,
    pub incident_alerts_emitted: u64,
}

/// Unified tagged WebSocket broadcast message for real-time streaming to connected clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WebSocketBroadcast {
    #[serde(rename = "chain_event")]
    ChainEvent {
        data: ChainEvent,
        #[serde(flatten)]
        compat: ChainEvent,
    },
    #[serde(rename = "incident_activity")]
    IncidentActivity { data: IncidentActivity },
    #[serde(rename = "incident_alert")]
    IncidentAlert { data: IncidentAlert },
}

impl WebSocketBroadcast {
    pub fn chain_event(event: ChainEvent) -> Self {
        Self::ChainEvent {
            compat: event.clone(),
            data: event,
        }
    }

    pub fn incident_activity(activity: IncidentActivity) -> Self {
        Self::IncidentActivity { data: activity }
    }

    pub fn incident_alert(alert: IncidentAlert) -> Self {
        Self::IncidentAlert { data: alert }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub storage: InMemoryStorage,
    pub detectors: Vec<Arc<dyn Detector>>,
    pub started_at: DateTime<Utc>,
    pub is_mock_feed: bool,
    pub event_broadcaster: tokio::sync::broadcast::Sender<ChainEvent>,
    pub ws_broadcaster: tokio::sync::broadcast::Sender<WebSocketBroadcast>,
    pub watch_engine: Arc<tokio::sync::RwLock<IncidentWatchEngine>>,
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
        let mut engine = IncidentWatchEngine::from_env();
        engine.load_targets(obschain_incidents::canonical_liquid_watch_targets());
        Self::with_metrics_and_watch_engine(
            storage,
            detectors,
            is_mock_feed,
            PipelineMetrics::default(),
            Arc::new(tokio::sync::RwLock::new(engine)),
        )
    }

    pub fn with_metrics(
        storage: InMemoryStorage,
        detectors: Vec<Arc<dyn Detector>>,
        is_mock_feed: bool,
        metrics: PipelineMetrics,
    ) -> (Self, tokio::sync::broadcast::Sender<ChainEvent>) {
        let mut engine = IncidentWatchEngine::from_env();
        engine.load_targets(obschain_incidents::canonical_liquid_watch_targets());
        Self::with_metrics_and_watch_engine(
            storage,
            detectors,
            is_mock_feed,
            metrics,
            Arc::new(tokio::sync::RwLock::new(engine)),
        )
    }

    pub fn with_metrics_and_watch_engine(
        storage: InMemoryStorage,
        detectors: Vec<Arc<dyn Detector>>,
        is_mock_feed: bool,
        metrics: PipelineMetrics,
        watch_engine: Arc<tokio::sync::RwLock<IncidentWatchEngine>>,
    ) -> (Self, tokio::sync::broadcast::Sender<ChainEvent>) {
        let (tx, _) = tokio::sync::broadcast::channel(1024);
        let (ws_tx, _) = tokio::sync::broadcast::channel(2048);

        // Forward raw ChainEvents to WebSocket broadcaster
        let mut event_rx = tx.subscribe();
        let ws_tx_clone = ws_tx.clone();
        tokio::spawn(async move {
            while let Ok(event) = event_rx.recv().await {
                let _ = ws_tx_clone.send(WebSocketBroadcast::chain_event(event));
            }
        });

        let state = Self {
            storage,
            detectors,
            started_at: Utc::now(),
            is_mock_feed,
            event_broadcaster: tx.clone(),
            ws_broadcaster: ws_tx,
            watch_engine,
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
    pub active_incident_watchers: usize,
    pub incident_watch_targets: usize,
    pub incident_activities_detected: u64,
    pub incident_alerts_emitted: u64,
}

#[derive(Debug, Deserialize)]
pub struct PaginationQuery {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct IncidentActivityQuery {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub status: Option<ActivityStatus>,
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
        .route(
            "/api/v1/incidents/{id}/timeline",
            get(get_incident_timeline_handler),
        )
        .route(
            "/api/v1/incidents/{id}/evidence",
            get(get_incident_evidence_handler),
        )
        .route(
            "/api/v1/incidents/{id}/graph",
            get(get_incident_graph_handler),
        )
        .route(
            "/api/v1/incidents/{id}/activity",
            get(get_incident_activity_handler),
        )
        .route(
            "/api/v1/incidents/{id}/watch-targets",
            get(get_incident_watch_targets_handler),
        )
        .route(
            "/api/v1/incident-activity",
            get(list_incident_activity_handler),
        )
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
    let active_incident_watchers = 1;
    let incident_watch_targets = state.watch_engine.read().await.active_target_count();
    let incident_activities_detected = state
        .metrics
        .incident_activities_detected
        .load(Ordering::Relaxed);
    let incident_alerts_emitted = state
        .metrics
        .incident_alerts_emitted
        .load(Ordering::Relaxed);

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
        active_incident_watchers,
        incident_watch_targets,
        incident_activities_detected,
        incident_alerts_emitted,
    })
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_client_socket(socket, state))
}

async fn handle_client_socket(mut socket: WebSocket, state: AppState) {
    let mut rx = state.ws_broadcaster.subscribe();
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
            broadcast_result = rx.recv() => {
                match broadcast_result {
                    Ok(msg) => {
                        let json = match serde_json::to_string(&msg) {
                            Ok(j) => j,
                            Err(e) => {
                                tracing::error!("Failed to serialize WebSocketBroadcast: {e}");
                                continue;
                            }
                        };
                        if socket.send(Message::Text(json.into())).await.is_err() {
                            tracing::debug!("ObsChain WebSocket client dropped during send");
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!("Slow WebSocket client lagged, skipped {skipped} messages");
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
                error: format!("Event {id} not found"),
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

async fn get_incident_or_404(
    state: &AppState,
    identifier: &str,
) -> Result<obschain_core::Incident, (StatusCode, Json<ApiErrorResponse>)> {
    let incident_opt = state
        .storage
        .get_incident_by_id_or_case_id(identifier)
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

    incident_opt.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ApiErrorResponse {
                error: format!("Incident '{identifier}' not found"),
                code: 404,
            }),
        )
    })
}

async fn get_incident_handler(
    State(state): State<AppState>,
    Path(identifier): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiErrorResponse>)> {
    let incident = get_incident_or_404(&state, &identifier).await?;
    Ok(Json(incident))
}

async fn get_incident_timeline_handler(
    State(state): State<AppState>,
    Path(identifier): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiErrorResponse>)> {
    let incident = get_incident_or_404(&state, &identifier).await?;
    Ok(Json(serde_json::json!({
        "incident_id": incident.id,
        "case_id": incident.case_id,
        "timeline": incident.timeline,
        "count": incident.timeline.len(),
    })))
}

async fn get_incident_evidence_handler(
    State(state): State<AppState>,
    Path(identifier): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiErrorResponse>)> {
    let incident = get_incident_or_404(&state, &identifier).await?;
    Ok(Json(serde_json::json!({
        "incident_id": incident.id,
        "case_id": incident.case_id,
        "evidence": incident.evidence,
        "count": incident.evidence.len(),
    })))
}

async fn get_incident_graph_handler(
    State(state): State<AppState>,
    Path(identifier): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiErrorResponse>)> {
    let incident = get_incident_or_404(&state, &identifier).await?;
    Ok(Json(incident.graph))
}

async fn get_incident_activity_handler(
    State(state): State<AppState>,
    Path(identifier): Path<String>,
    Query(query): Query<IncidentActivityQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiErrorResponse>)> {
    let incident = get_incident_or_404(&state, &identifier).await?;
    let limit = query.limit.unwrap_or(50).clamp(1, 100);
    let offset = query.offset.unwrap_or(0);

    let activity = state
        .storage
        .list_activities(Some(incident.id), query.status, limit, offset)
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
        "incident_id": incident.id,
        "case_id": incident.case_id,
        "activity": activity,
        "count": activity.len(),
        "limit": limit,
        "offset": offset,
    })))
}

async fn get_incident_watch_targets_handler(
    State(state): State<AppState>,
    Path(identifier): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiErrorResponse>)> {
    let incident = get_incident_or_404(&state, &identifier).await?;

    let targets = state
        .storage
        .list_watch_targets(Some(incident.id), 100, 0)
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

    // Safe public metadata conversion: redacts internal/sensitive investigation settings
    let public_targets: Vec<PublicWatchTarget> = targets
        .into_iter()
        .map(|t| t.to_public_metadata())
        .collect();

    Ok(Json(serde_json::json!({
        "incident_id": incident.id,
        "case_id": incident.case_id,
        "watch_targets": public_targets,
        "count": public_targets.len(),
    })))
}

async fn list_incident_activity_handler(
    State(state): State<AppState>,
    Query(query): Query<IncidentActivityQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiErrorResponse>)> {
    let limit = query.limit.unwrap_or(50).clamp(1, 100);
    let offset = query.offset.unwrap_or(0);

    let activity = state
        .storage
        .list_activities(None, query.status, limit, offset)
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
        "activity": activity,
        "count": activity.len(),
        "limit": limit,
        "offset": offset,
    })))
}
