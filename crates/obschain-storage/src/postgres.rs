use chrono::{DateTime, Utc};
use obschain_core::{
    ActivityStatus, Chain, ChainEvent, ConfidenceLevel, CorrelationStrength, EventObservation,
    EventObservationKind, EventSeverity, EventType, Evidence, EvidenceType, GraphEdge,
    GraphEdgeType, GraphNode, GraphNodeType, Incident, IncidentActivity, IncidentActivityType,
    IncidentAlert, IncidentBlock, IncidentEntity, IncidentGraph, IncidentStatus,
    IncidentTransaction, IncidentUpdate, ObservationMode, ObservationSource, OnChainMessage,
    ProvenanceClassification, RecoverySummary, ReplayCheckpoint, ReplayJob, ReplayJobStatus,
    Source, SourceCategory, TechnicalFinding, TimelineCategory, TimelineEntry, TransactionRole,
    WatchTarget, WatchTargetKind,
};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use obschain_core::baseline::{
    BaselineDistribution, BaselineMetric, BaselineQuality, BaselineRun, BaselineRunStatus,
    EvaluationMode, EventMetricExtractor, EventMetricValue, EventRarityResult, ImpactBreakdown,
    MetricUnit, MetricValue, PercentileMethod, RarityBand, RarityDirection,
};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::repository::{
    i64_to_u64_checked, u64_to_i64_checked, BaselineRepository, EventFilter, EventRepository,
    IncidentActivityRepository, IncidentAlertRepository, IncidentRepository, ReplayRepository,
    StorageError, WatchTargetRepository,
};

/// SQLx PostgreSQL durable storage backend implementing all repository abstractions.
#[derive(Clone)]
pub struct PostgresStorage {
    pool: PgPool,
    cached_events: Arc<AtomicUsize>,
    cached_incidents: Arc<AtomicUsize>,
    cached_activities: Arc<AtomicUsize>,
    cached_targets: Arc<AtomicUsize>,
}

impl PostgresStorage {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            cached_events: Arc::new(AtomicUsize::new(0)),
            cached_incidents: Arc::new(AtomicUsize::new(0)),
            cached_activities: Arc::new(AtomicUsize::new(0)),
            cached_targets: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Pre-populate atomic telemetry counters from database on startup.
    pub async fn initialize_telemetry_counters(&self) -> Result<(), StorageError> {
        let events = self.count_events().await?;
        let incidents = self.count_incidents().await?;
        let activities = self.count_activities().await?;
        let targets = self.count_watch_targets().await?;

        self.cached_events.store(events, Ordering::Relaxed);
        self.cached_incidents.store(incidents, Ordering::Relaxed);
        self.cached_activities.store(activities, Ordering::Relaxed);
        self.cached_targets.store(targets, Ordering::Relaxed);
        Ok(())
    }

    /// Read cheap cached counts without issuing COUNT(*) queries.
    pub fn telemetry_counts(&self) -> (usize, usize, usize, usize) {
        (
            self.cached_events.load(Ordering::Relaxed),
            self.cached_incidents.load(Ordering::Relaxed),
            self.cached_activities.load(Ordering::Relaxed),
            self.cached_targets.load(Ordering::Relaxed),
        )
    }

    /// Automatically run pending SQLx migrations located in migrations/ directory.
    pub async fn run_migrations(&self) -> Result<(), StorageError> {
        sqlx::migrate!("../../migrations")
            .run(&self.pool)
            .await
            .map_err(|e| StorageError::Database(format!("SQLx migration failed: {e}")))?;
        Ok(())
    }

    /// Seed canonical Liquid Network incident case (OC-2026-0001) and canonical watch targets idempotently.
    pub async fn seed_canonical_incidents(&self) -> Result<(), StorageError> {
        let liquid_incident = obschain_incidents::create_liquid_2026_incident();
        self.save_incident(&liquid_incident).await?;

        let targets = obschain_incidents::canonical_liquid_watch_targets();
        for target in targets {
            self.save_watch_target(&target).await?;
        }
        Ok(())
    }

    pub async fn count_events(&self) -> Result<usize, StorageError> {
        let row = sqlx::query("SELECT COUNT(*) as count FROM chain_events")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;
        let count: i64 = row.get("count");
        Ok(count as usize)
    }

    pub async fn count_incidents(&self) -> Result<usize, StorageError> {
        let row = sqlx::query("SELECT COUNT(*) as count FROM incidents")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;
        let count: i64 = row.get("count");
        Ok(count as usize)
    }

    pub async fn count_watch_targets(&self) -> Result<usize, StorageError> {
        let row = sqlx::query("SELECT COUNT(*) as count FROM incident_watch_targets")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;
        let count: i64 = row.get("count");
        Ok(count as usize)
    }

    pub async fn count_activities(&self) -> Result<usize, StorageError> {
        let row = sqlx::query("SELECT COUNT(*) as count FROM incident_activities")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;
        let count: i64 = row.get("count");
        Ok(count as usize)
    }

    pub async fn count_alerts(&self) -> Result<usize, StorageError> {
        let row = sqlx::query("SELECT COUNT(*) as count FROM incident_alerts")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;
        let count: i64 = row.get("count");
        Ok(count as usize)
    }

    pub async fn count_replay_jobs(&self) -> Result<usize, StorageError> {
        let row = sqlx::query("SELECT COUNT(*) as count FROM replay_jobs")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;
        let count: i64 = row.get("count");
        Ok(count as usize)
    }

    pub async fn count_event_observations(
        &self,
        event_id: Option<Uuid>,
    ) -> Result<usize, StorageError> {
        let row = if let Some(eid) = event_id {
            sqlx::query("SELECT COUNT(*) as count FROM event_observations WHERE event_id = $1")
                .bind(eid)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| StorageError::Database(e.to_string()))?
        } else {
            sqlx::query("SELECT COUNT(*) as count FROM event_observations")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| StorageError::Database(e.to_string()))?
        };
        let count: i64 = row.get("count");
        Ok(count as usize)
    }
}

// ---------------------------------------------------------------------------
// EventRepository Implementation
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl EventRepository for PostgresStorage {
    async fn save_event(&self, event: &ChainEvent) -> Result<(), StorageError> {
        let event_type_str = serde_json::to_string(&event.event_type)
            .map_err(StorageError::Serialization)?
            .trim_matches('"')
            .to_string();
        let severity_str = serde_json::to_string(&event.severity)
            .map_err(StorageError::Serialization)?
            .trim_matches('"')
            .to_string();
        let confidence_str = serde_json::to_string(&event.confidence)
            .map_err(StorageError::Serialization)?
            .trim_matches('"')
            .to_string();

        let source_json = match &event.source {
            Some(s) => Some(serde_json::to_value(s)?),
            None => None,
        };

        let block_height_i64 = match event.block_height {
            Some(h) => Some(u64_to_i64_checked(h)?),
            None => None,
        };

        let mode_str = event.observation_mode.as_str();

        // Canonical event upsert preserves intrinsic event details.
        // It NEVER overwrites provenance fields (observation_mode, replay_job_id, first_observed_at, detected_at).
        sqlx::query(
            r#"
            INSERT INTO chain_events (
                id, event_type, severity, confidence, title, description,
                event_time, first_observed_at, detected_at, block_height, block_hash, txid, metadata, source,
                observation_mode, replay_job_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                metadata = EXCLUDED.metadata
            "#,
        )
        .bind(event.id)
        .bind(event_type_str)
        .bind(severity_str)
        .bind(confidence_str)
        .bind(&event.title)
        .bind(&event.description)
        .bind(event.event_time)
        .bind(event.first_observed_at)
        .bind(event.detected_at)
        .bind(block_height_i64)
        .bind(&event.block_hash)
        .bind(&event.txid)
        .bind(&event.metadata)
        .bind(source_json)
        .bind(mode_str)
        .bind(event.replay_job_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        self.cached_events.fetch_add(1, Ordering::Relaxed);

        // Deduplicated canonical metric extraction
        let extracted = EventMetricExtractor::extract_event_metrics(event, None);
        if !extracted.is_empty() {
            let _ = self.save_event_metrics(&extracted).await;
        }

        Ok(())
    }

    async fn list_events(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ChainEvent>, StorageError> {
        let limit_clamped = limit.clamp(1, 200) as i64;
        let offset_i64 = offset as i64;

        let rows = sqlx::query(
            r#"
            SELECT id, event_type, severity, confidence, title, description,
                   COALESCE(event_time, detected_at) as event_time,
                   COALESCE(first_observed_at, detected_at) as first_observed_at,
                   detected_at, block_height, block_hash, txid, metadata, source,
                   observation_mode, replay_job_id
            FROM chain_events
            ORDER BY COALESCE(event_time, detected_at) DESC
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(limit_clamped)
        .bind(offset_i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let event_type_str: String = row.get("event_type");
            let severity_str: String = row.get("severity");
            let confidence_str: String = row.get("confidence");

            let event_type: EventType = serde_json::from_str(&format!("\"{event_type_str}\""))
                .unwrap_or(EventType::LargeTransfer);
            let severity: EventSeverity =
                serde_json::from_str(&format!("\"{severity_str}\"")).unwrap_or(EventSeverity::Info);
            let confidence: ConfidenceLevel =
                serde_json::from_str(&format!("\"{confidence_str}\""))
                    .unwrap_or(ConfidenceLevel::Heuristic);

            let block_height_i64: Option<i64> = row.get("block_height");
            let block_height = match block_height_i64 {
                Some(h) => Some(i64_to_u64_checked(h)?),
                None => None,
            };

            let source_json: Option<serde_json::Value> = row.get("source");
            let source = match source_json {
                Some(v) => serde_json::from_value(v).ok(),
                None => None,
            };

            let obs_mode_str: Option<String> = row.try_get("observation_mode").ok();
            let observation_mode = obs_mode_str
                .map(|s| match s.to_lowercase().as_str() {
                    "historical_replay" => ObservationMode::HistoricalReplay,
                    _ => ObservationMode::Live,
                })
                .unwrap_or(ObservationMode::Live);
            let replay_job_id: Option<Uuid> = row.try_get("replay_job_id").ok().flatten();

            let detected_at: DateTime<Utc> = row.get("detected_at");
            let event_time: DateTime<Utc> = row.try_get("event_time").unwrap_or(detected_at);
            let first_observed_at: DateTime<Utc> =
                row.try_get("first_observed_at").unwrap_or(detected_at);

            events.push(ChainEvent {
                id: row.get("id"),
                event_type,
                severity,
                confidence,
                title: row.get("title"),
                description: row.get("description"),
                event_time,
                first_observed_at,
                detected_at,
                block_height,
                block_hash: row.get("block_hash"),
                txid: row.get("txid"),
                metadata: row.get("metadata"),
                witnesses: source
                    .clone()
                    .map(|s| vec![obschain_core::ObservationWitness::new(s)])
                    .unwrap_or_default(),
                source,
                observation_mode,
                replay_job_id,
                observations: Vec::new(),
            });
        }

        Ok(events)
    }

    async fn get_event_by_id(&self, id: Uuid) -> Result<Option<ChainEvent>, StorageError> {
        let row_opt = sqlx::query(
            r#"
            SELECT id, event_type, severity, confidence, title, description,
                   COALESCE(event_time, detected_at) as event_time,
                   COALESCE(first_observed_at, detected_at) as first_observed_at,
                   detected_at, block_height, block_hash, txid, metadata, source,
                   observation_mode, replay_job_id
            FROM chain_events
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let Some(row) = row_opt else {
            return Ok(None);
        };

        let event_type_str: String = row.get("event_type");
        let severity_str: String = row.get("severity");
        let confidence_str: String = row.get("confidence");

        let event_type: EventType = serde_json::from_str(&format!("\"{event_type_str}\""))
            .unwrap_or(EventType::LargeTransfer);
        let severity: EventSeverity =
            serde_json::from_str(&format!("\"{severity_str}\"")).unwrap_or(EventSeverity::Info);
        let confidence: ConfidenceLevel = serde_json::from_str(&format!("\"{confidence_str}\""))
            .unwrap_or(ConfidenceLevel::Heuristic);

        let block_height_i64: Option<i64> = row.get("block_height");
        let block_height = match block_height_i64 {
            Some(h) => Some(i64_to_u64_checked(h)?),
            None => None,
        };

        let source_json: Option<serde_json::Value> = row.get("source");
        let source = match source_json {
            Some(v) => serde_json::from_value(v).ok(),
            None => None,
        };

        let obs_mode_str: Option<String> = row.try_get("observation_mode").ok();
        let observation_mode = obs_mode_str
            .map(|s| match s.to_lowercase().as_str() {
                "historical_replay" => ObservationMode::HistoricalReplay,
                _ => ObservationMode::Live,
            })
            .unwrap_or(ObservationMode::Live);
        let replay_job_id: Option<Uuid> = row.try_get("replay_job_id").ok().flatten();

        let detected_at: DateTime<Utc> = row.get("detected_at");
        let event_time: DateTime<Utc> = row.try_get("event_time").unwrap_or(detected_at);
        let first_observed_at: DateTime<Utc> =
            row.try_get("first_observed_at").unwrap_or(detected_at);

        let observations = self.list_event_observations(id).await?;

        Ok(Some(ChainEvent {
            id: row.get("id"),
            event_type,
            severity,
            confidence,
            title: row.get("title"),
            description: row.get("description"),
            event_time,
            first_observed_at,
            detected_at,
            block_height,
            block_hash: row.get("block_hash"),
            txid: row.get("txid"),
            metadata: row.get("metadata"),
            witnesses: source
                .clone()
                .map(|s| vec![obschain_core::ObservationWitness::new(s)])
                .unwrap_or_default(),
            source,
            observation_mode,
            replay_job_id,
            observations,
        }))
    }

    async fn query_events(&self, filter: &EventFilter) -> Result<Vec<ChainEvent>, StorageError> {
        let limit_clamped = filter.limit.unwrap_or(100).clamp(1, 1000) as i64;
        let offset_i64 = filter.offset.unwrap_or(0) as i64;

        let from_height_i64 = match filter.from_height {
            Some(h) => Some(u64_to_i64_checked(h)?),
            None => None,
        };
        let to_height_i64 = match filter.to_height {
            Some(h) => Some(u64_to_i64_checked(h)?),
            None => None,
        };

        let event_type_str = filter.event_type.as_ref().map(|et| {
            serde_json::to_string(et)
                .unwrap_or_default()
                .trim_matches('"')
                .to_string()
        });
        let severity_str = filter.severity.map(|s| {
            serde_json::to_string(&s)
                .unwrap_or_default()
                .trim_matches('"')
                .to_string()
        });
        let mode_str = filter.observation_mode.map(|m| m.as_str().to_string());

        let rows = sqlx::query(
            r#"
            SELECT id, event_type, severity, confidence, title, description,
                   COALESCE(event_time, detected_at) as event_time,
                   COALESCE(first_observed_at, detected_at) as first_observed_at,
                   detected_at, block_height, block_hash, txid, metadata, source,
                   observation_mode, replay_job_id
            FROM chain_events
            WHERE ($1::BIGINT IS NULL OR block_height >= $1)
              AND ($2::BIGINT IS NULL OR block_height <= $2)
              AND ($3::TIMESTAMPTZ IS NULL OR COALESCE(event_time, detected_at) >= $3)
              AND ($4::TIMESTAMPTZ IS NULL OR COALESCE(event_time, detected_at) <= $4)
              AND ($5::VARCHAR IS NULL OR event_type = $5)
              AND ($6::VARCHAR IS NULL OR severity = $6)
              AND ($7::VARCHAR IS NULL OR EXISTS (
                  SELECT 1 FROM event_observations eo
                  WHERE eo.event_id = chain_events.id AND eo.observation_mode = $7
              ) OR observation_mode = $7)
              AND ($8::UUID IS NULL OR EXISTS (
                  SELECT 1 FROM event_observations eo
                  WHERE eo.event_id = chain_events.id AND eo.replay_job_id = $8
              ) OR replay_job_id = $8)
              AND ($9::VARCHAR IS NULL OR txid = $9)
              AND ($10::VARCHAR IS NULL OR block_hash = $10)
            ORDER BY COALESCE(event_time, detected_at) DESC
            LIMIT $11 OFFSET $12
            "#,
        )
        .bind(from_height_i64)
        .bind(to_height_i64)
        .bind(filter.from_time)
        .bind(filter.to_time)
        .bind(event_type_str)
        .bind(severity_str)
        .bind(mode_str)
        .bind(filter.replay_job_id)
        .bind(&filter.txid)
        .bind(&filter.block_hash)
        .bind(limit_clamped)
        .bind(offset_i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let event_type_str: String = row.get("event_type");
            let severity_str: String = row.get("severity");
            let confidence_str: String = row.get("confidence");

            let event_type: EventType = serde_json::from_str(&format!("\"{event_type_str}\""))
                .unwrap_or(EventType::LargeTransfer);
            let severity: EventSeverity =
                serde_json::from_str(&format!("\"{severity_str}\"")).unwrap_or(EventSeverity::Info);
            let confidence: ConfidenceLevel =
                serde_json::from_str(&format!("\"{confidence_str}\""))
                    .unwrap_or(ConfidenceLevel::Heuristic);

            let block_height_i64: Option<i64> = row.get("block_height");
            let block_height = match block_height_i64 {
                Some(h) => Some(i64_to_u64_checked(h)?),
                None => None,
            };

            let source_json: Option<serde_json::Value> = row.get("source");
            let source = match source_json {
                Some(v) => serde_json::from_value(v).ok(),
                None => None,
            };

            let obs_mode_str: Option<String> = row.try_get("observation_mode").ok();
            let observation_mode = obs_mode_str
                .map(|s| match s.to_lowercase().as_str() {
                    "historical_replay" => ObservationMode::HistoricalReplay,
                    _ => ObservationMode::Live,
                })
                .unwrap_or(ObservationMode::Live);
            let replay_job_id: Option<Uuid> = row.try_get("replay_job_id").ok().flatten();

            let detected_at: DateTime<Utc> = row.get("detected_at");
            let event_time: DateTime<Utc> = row.try_get("event_time").unwrap_or(detected_at);
            let first_observed_at: DateTime<Utc> =
                row.try_get("first_observed_at").unwrap_or(detected_at);

            events.push(ChainEvent {
                id: row.get("id"),
                event_type,
                severity,
                confidence,
                title: row.get("title"),
                description: row.get("description"),
                event_time,
                first_observed_at,
                detected_at,
                block_height,
                block_hash: row.get("block_hash"),
                txid: row.get("txid"),
                metadata: row.get("metadata"),
                witnesses: source
                    .clone()
                    .map(|s| vec![obschain_core::ObservationWitness::new(s)])
                    .unwrap_or_default(),
                source,
                observation_mode,
                replay_job_id,
                observations: Vec::new(),
            });
        }

        Ok(events)
    }

    async fn save_event_observation(
        &self,
        observation: &EventObservation,
    ) -> Result<(), StorageError> {
        let source_json = serde_json::to_value(&observation.source)?;
        let witness_json = match &observation.witness {
            Some(w) => Some(serde_json::to_value(w)?),
            None => None,
        };
        let block_height_i64 = match observation.block_height {
            Some(h) => Some(u64_to_i64_checked(h)?),
            None => None,
        };
        let mempool_seq_i64 = match observation.mempool_sequence {
            Some(s) => Some(u64_to_i64_checked(s)?),
            None => None,
        };
        let source_seq_i64 = observation.source_sequence.map(|s| s as i64);
        let mode_str = observation.mode.as_str();
        let kind_str = observation.kind.as_str();

        sqlx::query(
            r#"
            INSERT INTO event_observations (
                id, event_id, observation_mode, observation_kind, source, observed_at,
                bitcoin_time, replay_job_id, block_height, block_hash,
                confirmation_status, source_sequence, mempool_sequence, witness
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (id) DO UPDATE SET
                source = EXCLUDED.source,
                witness = EXCLUDED.witness,
                confirmation_status = EXCLUDED.confirmation_status,
                source_sequence = EXCLUDED.source_sequence,
                mempool_sequence = EXCLUDED.mempool_sequence
            "#,
        )
        .bind(observation.id)
        .bind(observation.event_id)
        .bind(mode_str)
        .bind(kind_str)
        .bind(source_json)
        .bind(observation.observed_at)
        .bind(observation.bitcoin_time)
        .bind(observation.replay_job_id)
        .bind(block_height_i64)
        .bind(&observation.block_hash)
        .bind(&observation.confirmation_status)
        .bind(source_seq_i64)
        .bind(mempool_seq_i64)
        .bind(witness_json)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        Ok(())
    }

    async fn list_event_observations(
        &self,
        event_id: Uuid,
    ) -> Result<Vec<EventObservation>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT id, event_id, observation_mode, observation_kind, source, observed_at,
                   bitcoin_time, replay_job_id, block_height, block_hash,
                   confirmation_status, source_sequence, mempool_sequence, witness
            FROM event_observations
            WHERE event_id = $1
            ORDER BY observed_at ASC
            "#,
        )
        .bind(event_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut observations = Vec::with_capacity(rows.len());
        for row in rows {
            let mode_str: String = row.get("observation_mode");
            let mode = match mode_str.to_lowercase().as_str() {
                "historical_replay" => ObservationMode::HistoricalReplay,
                _ => ObservationMode::Live,
            };
            let kind_str: String = row.get("observation_kind");
            let kind = kind_str
                .parse::<EventObservationKind>()
                .unwrap_or(EventObservationKind::Witnessed);
            let source_json: serde_json::Value = row.get("source");
            let source: ObservationSource = serde_json::from_value(source_json)
                .unwrap_or_else(|_| ObservationSource::new("bitcoin_core", "zmq", None));
            let witness_json: Option<serde_json::Value> = row.get("witness");
            let witness = witness_json.and_then(|v| serde_json::from_value(v).ok());
            let block_height_i64: Option<i64> = row.get("block_height");
            let block_height = match block_height_i64 {
                Some(h) => Some(i64_to_u64_checked(h)?),
                None => None,
            };
            let confirmation_status: Option<String> = row.get("confirmation_status");
            let source_seq_i64: Option<i64> = row.get("source_sequence");
            let source_sequence = source_seq_i64.map(|s| s as u32);
            let mempool_seq_i64: Option<i64> = row.get("mempool_sequence");
            let mempool_sequence = match mempool_seq_i64 {
                Some(s) => Some(i64_to_u64_checked(s)?),
                None => None,
            };

            observations.push(EventObservation {
                id: row.get("id"),
                event_id: row.get("event_id"),
                mode,
                kind,
                source,
                observed_at: row.get("observed_at"),
                bitcoin_time: row.get("bitcoin_time"),
                replay_job_id: row.get("replay_job_id"),
                block_height,
                block_hash: row.get("block_hash"),
                confirmation_status,
                source_sequence,
                mempool_sequence,
                witness,
            });
        }
        Ok(observations)
    }

    async fn count_event_observations(
        &self,
        event_id: Option<Uuid>,
    ) -> Result<usize, StorageError> {
        self.count_event_observations(event_id).await
    }
}

// ---------------------------------------------------------------------------
// IncidentRepository Implementation
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl IncidentRepository for PostgresStorage {
    async fn save_incident(&self, incident: &Incident) -> Result<(), StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;

        let status_str = serde_json::to_string(&incident.status)
            .map_err(StorageError::Serialization)?
            .trim_matches('"')
            .to_string();
        let severity_str = serde_json::to_string(&incident.severity)
            .map_err(StorageError::Serialization)?
            .trim_matches('"')
            .to_string();

        let structured_claims_json = serde_json::to_value(&incident.structured_claims)?;
        let facts_json = serde_json::to_value(&incident.facts)?;
        let reported_json = serde_json::to_value(&incident.reported_claims)?;
        let unverified_json = serde_json::to_value(&incident.unverified_claims)?;
        let txids_json = serde_json::to_value(&incident.associated_txids)?;
        let blocks_json = serde_json::to_value(&incident.associated_block_heights)?;

        // 1. Core incident dossier upsert
        sqlx::query(
            r#"
            INSERT INTO incidents (
                id, case_id, title, summary, status, severity,
                first_observed_at, last_updated_at, structured_claims,
                facts, reported_claims, unverified_claims,
                associated_txids, associated_block_heights, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW())
            ON CONFLICT (id) DO UPDATE SET
                case_id = EXCLUDED.case_id,
                title = EXCLUDED.title,
                summary = EXCLUDED.summary,
                status = EXCLUDED.status,
                severity = EXCLUDED.severity,
                last_updated_at = EXCLUDED.last_updated_at,
                structured_claims = EXCLUDED.structured_claims,
                facts = EXCLUDED.facts,
                reported_claims = EXCLUDED.reported_claims,
                unverified_claims = EXCLUDED.unverified_claims,
                associated_txids = EXCLUDED.associated_txids,
                associated_block_heights = EXCLUDED.associated_block_heights,
                updated_at = NOW()
            "#,
        )
        .bind(incident.id)
        .bind(&incident.case_id)
        .bind(&incident.title)
        .bind(&incident.summary)
        .bind(status_str)
        .bind(severity_str)
        .bind(incident.first_observed_at)
        .bind(incident.last_updated_at)
        .bind(structured_claims_json)
        .bind(facts_json)
        .bind(reported_json)
        .bind(unverified_json)
        .bind(txids_json)
        .bind(blocks_json)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        // 2. Historical Recovery Snapshot (Preserves historical recovery without overwriting)
        let affected_sats_i64 = u64_to_i64_checked(incident.recovery.affected_sats)?;
        let recovered_sats_i64 = u64_to_i64_checked(incident.recovery.recovered_sats)?;
        let outstanding_sats_i64 = u64_to_i64_checked(incident.recovery.outstanding_sats)?;

        let existing_snapshot = sqlx::query(
            r#"
            SELECT id FROM incident_recovery_snapshots
            WHERE incident_id = $1 AND as_of_timestamp = $2
            LIMIT 1
            "#,
        )
        .bind(incident.id)
        .bind(incident.recovery.as_of_timestamp)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        if existing_snapshot.is_none() {
            sqlx::query(
                r#"
                INSERT INTO incident_recovery_snapshots (
                    incident_id, affected_sats, recovered_sats, outstanding_sats,
                    is_estimate, as_of_timestamp, source_label
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
            )
            .bind(incident.id)
            .bind(affected_sats_i64)
            .bind(recovered_sats_i64)
            .bind(outstanding_sats_i64)
            .bind(incident.recovery.is_estimate)
            .bind(incident.recovery.as_of_timestamp)
            .bind(&incident.recovery.source)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;
        }

        // 3. Incident Sources
        for src in &incident.sources {
            let cat_str = serde_json::to_string(&src.source_category)
                .map_err(StorageError::Serialization)?
                .trim_matches('"')
                .to_string();

            sqlx::query(
                r#"
                INSERT INTO incident_sources (
                    id, incident_id, publisher, title, url, publication_timestamp,
                    retrieved_timestamp, source_category, reliability_score, name
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (id) DO UPDATE SET
                    publisher = EXCLUDED.publisher,
                    title = EXCLUDED.title,
                    url = EXCLUDED.url,
                    reliability_score = EXCLUDED.reliability_score
                "#,
            )
            .bind(src.id)
            .bind(incident.id)
            .bind(&src.publisher)
            .bind(&src.title)
            .bind(&src.url)
            .bind(src.publication_timestamp)
            .bind(src.retrieved_timestamp)
            .bind(cat_str)
            .bind(src.reliability_score)
            .bind(&src.name)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;
        }

        // 4. Incident Evidence
        for ev in &incident.evidence {
            let type_str = serde_json::to_string(&ev.evidence_type)
                .map_err(StorageError::Serialization)?
                .trim_matches('"')
                .to_string();
            let conf_str = serde_json::to_string(&ev.confidence)
                .map_err(StorageError::Serialization)?
                .trim_matches('"')
                .to_string();
            let chain_str = ev.chain.to_string();
            let block_height_i64 = match ev.block_height {
                Some(h) => Some(u64_to_i64_checked(h)?),
                None => None,
            };

            sqlx::query(
                r#"
                INSERT INTO incident_evidence (
                    id, incident_id, evidence_type, confidence, title, description,
                    observed_at, source_id, source_reference, txid, block_hash,
                    block_height, chain, verified, raw_data
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                ON CONFLICT (id) DO UPDATE SET
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    verified = EXCLUDED.verified,
                    raw_data = EXCLUDED.raw_data
                "#,
            )
            .bind(ev.id)
            .bind(incident.id)
            .bind(type_str)
            .bind(conf_str)
            .bind(&ev.title)
            .bind(&ev.description)
            .bind(ev.observed_at)
            .bind(ev.source_id)
            .bind(&ev.source_reference)
            .bind(&ev.txid)
            .bind(&ev.block_hash)
            .bind(block_height_i64)
            .bind(chain_str)
            .bind(ev.verified)
            .bind(&ev.raw_data)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;
        }

        // 5. Incident Transactions
        for itx in &incident.transactions {
            let chain_str = itx.chain.to_string();
            let role_str = serde_json::to_string(&itx.role)
                .map_err(StorageError::Serialization)?
                .trim_matches('"')
                .to_string();
            let amount_sats_i64 = match itx.amount_sats {
                Some(s) => Some(u64_to_i64_checked(s)?),
                None => None,
            };
            let block_height_i64 = match itx.block_height {
                Some(h) => Some(u64_to_i64_checked(h)?),
                None => None,
            };

            sqlx::query(
                r#"
                INSERT INTO incident_transactions (
                    incident_id, chain, txid, role, amount_sats, block_height,
                    block_hash, confirmed_at, evidence_id, notes
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (incident_id, chain, txid) DO UPDATE SET
                    role = EXCLUDED.role,
                    amount_sats = EXCLUDED.amount_sats,
                    block_height = EXCLUDED.block_height,
                    block_hash = EXCLUDED.block_hash,
                    confirmed_at = EXCLUDED.confirmed_at,
                    evidence_id = EXCLUDED.evidence_id,
                    notes = EXCLUDED.notes
                "#,
            )
            .bind(incident.id)
            .bind(chain_str)
            .bind(&itx.txid)
            .bind(role_str)
            .bind(amount_sats_i64)
            .bind(block_height_i64)
            .bind(&itx.block_hash)
            .bind(itx.confirmed_at)
            .bind(itx.evidence_id)
            .bind(&itx.notes)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;
        }

        // 6. Incident Blocks
        for blk in &incident.blocks {
            let chain_str = blk.chain.to_string();
            let height_i64 = u64_to_i64_checked(blk.height)?;
            let tx_count_i32 = blk.tx_count.map(|c| c as i32);

            sqlx::query(
                r#"
                INSERT INTO incident_blocks (
                    incident_id, chain, height, hash, timestamp, tx_count, evidence_id
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (incident_id, chain, height) DO UPDATE SET
                    hash = EXCLUDED.hash,
                    timestamp = EXCLUDED.timestamp,
                    tx_count = EXCLUDED.tx_count,
                    evidence_id = EXCLUDED.evidence_id
                "#,
            )
            .bind(incident.id)
            .bind(chain_str)
            .bind(height_i64)
            .bind(&blk.hash)
            .bind(blk.timestamp)
            .bind(tx_count_i32)
            .bind(blk.evidence_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;
        }

        // 7. Incident Entities
        for ent in &incident.entities {
            let conf_str = serde_json::to_string(&ent.attribution_confidence)
                .map_err(StorageError::Serialization)?
                .trim_matches('"')
                .to_string();

            sqlx::query(
                r#"
                INSERT INTO incident_entities (
                    id, incident_id, name, entity_type, description, attribution_confidence
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    entity_type = EXCLUDED.entity_type,
                    description = EXCLUDED.description,
                    attribution_confidence = EXCLUDED.attribution_confidence
                "#,
            )
            .bind(ent.id)
            .bind(incident.id)
            .bind(&ent.name)
            .bind(&ent.entity_type)
            .bind(&ent.description)
            .bind(conf_str)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;
        }

        // 8. On-Chain Messages
        sqlx::query("DELETE FROM incident_messages WHERE incident_id = $1")
            .bind(incident.id)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;

        for msg in &incident.on_chain_messages {
            let chain_str = msg.chain.to_string();
            let conf_str = serde_json::to_string(&msg.sender_attribution_confidence)
                .map_err(StorageError::Serialization)?
                .trim_matches('"')
                .to_string();
            let block_height_i64 = match msg.block_height {
                Some(h) => Some(u64_to_i64_checked(h)?),
                None => None,
            };

            sqlx::query(
                r#"
                INSERT INTO incident_messages (
                    incident_id, txid, chain, encoding, decoded_text, raw_hex,
                    confirmed_at, block_height, attributed_sender, sender_attribution_confidence
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                "#,
            )
            .bind(incident.id)
            .bind(&msg.txid)
            .bind(chain_str)
            .bind(&msg.encoding)
            .bind(&msg.decoded_text)
            .bind(&msg.raw_hex)
            .bind(msg.confirmed_at)
            .bind(block_height_i64)
            .bind(&msg.attributed_sender)
            .bind(conf_str)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;
        }

        // 9. Timeline Entries
        for tl in &incident.timeline {
            let cat_str = serde_json::to_string(&tl.category)
                .map_err(StorageError::Serialization)?
                .trim_matches('"')
                .to_string();
            let conf_str = serde_json::to_string(&tl.classification)
                .map_err(StorageError::Serialization)?
                .trim_matches('"')
                .to_string();
            let ev_ids_json = serde_json::to_value(&tl.evidence_ids)?;
            let txids_json = serde_json::to_value(&tl.transaction_txids)?;
            let blocks_json = serde_json::to_value(&tl.block_heights)?;

            sqlx::query(
                r#"
                INSERT INTO incident_timeline (
                    id, incident_id, timestamp, title, description, category,
                    source_id, evidence_ids, transaction_txids, block_heights, classification
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (id) DO UPDATE SET
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    category = EXCLUDED.category,
                    evidence_ids = EXCLUDED.evidence_ids,
                    transaction_txids = EXCLUDED.transaction_txids,
                    block_heights = EXCLUDED.block_heights,
                    classification = EXCLUDED.classification
                "#,
            )
            .bind(tl.id)
            .bind(incident.id)
            .bind(tl.timestamp)
            .bind(&tl.title)
            .bind(&tl.description)
            .bind(cat_str)
            .bind(tl.source_id)
            .bind(ev_ids_json)
            .bind(txids_json)
            .bind(blocks_json)
            .bind(conf_str)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;
        }

        // 10. Technical Findings
        sqlx::query("DELETE FROM incident_technical_findings WHERE incident_id = $1")
            .bind(incident.id)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;

        for tf in &incident.technical_findings {
            sqlx::query(
                r#"
                INSERT INTO incident_technical_findings (
                    incident_id, component, area, category, summary, root_cause_details,
                    fix_summary, repository_url, pull_request_id, commit_hash
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                "#,
            )
            .bind(incident.id)
            .bind(&tf.component)
            .bind(&tf.area)
            .bind(&tf.category)
            .bind(&tf.summary)
            .bind(&tf.root_cause_details)
            .bind(&tf.fix_summary)
            .bind(&tf.repository_url)
            .bind(&tf.pull_request_id)
            .bind(&tf.commit_hash)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;
        }

        // 11. Incident Updates (Append-only)
        for upd in &incident.updates {
            let recovery_json = match &upd.recovery_state {
                Some(r) => Some(serde_json::to_value(r)?),
                None => None,
            };

            sqlx::query(
                r#"
                INSERT INTO incident_updates (
                    id, incident_id, timestamp, title, summary, source_id, recovery_state
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (id) DO NOTHING
                "#,
            )
            .bind(upd.id)
            .bind(incident.id)
            .bind(upd.timestamp)
            .bind(&upd.title)
            .bind(&upd.summary)
            .bind(upd.source_id)
            .bind(recovery_json)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;
        }

        // 12. Graph Nodes & Edges
        for node in &incident.graph.nodes {
            let node_type_str = serde_json::to_string(&node.node_type)
                .map_err(StorageError::Serialization)?
                .trim_matches('"')
                .to_string();
            let chain_str = node.chain.map(|c| c.to_string());

            sqlx::query(
                r#"
                INSERT INTO incident_graph_nodes (
                    incident_id, node_id, label, node_type, chain, metadata
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (incident_id, node_id) DO UPDATE SET
                    label = EXCLUDED.label,
                    node_type = EXCLUDED.node_type,
                    chain = EXCLUDED.chain,
                    metadata = EXCLUDED.metadata
                "#,
            )
            .bind(incident.id)
            .bind(&node.id)
            .bind(&node.label)
            .bind(node_type_str)
            .bind(chain_str)
            .bind(&node.metadata)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;
        }

        sqlx::query("DELETE FROM incident_graph_edges WHERE incident_id = $1")
            .bind(incident.id)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;

        for edge in &incident.graph.edges {
            let rel_str = serde_json::to_string(&edge.relationship)
                .map_err(StorageError::Serialization)?
                .trim_matches('"')
                .to_string();
            let conf_str = serde_json::to_string(&edge.confidence)
                .map_err(StorageError::Serialization)?
                .trim_matches('"')
                .to_string();

            sqlx::query(
                r#"
                INSERT INTO incident_graph_edges (
                    incident_id, source_node_id, target_node_id, relationship, confidence
                )
                VALUES ($1, $2, $3, $4, $5)
                "#,
            )
            .bind(incident.id)
            .bind(&edge.source)
            .bind(&edge.target)
            .bind(rel_str)
            .bind(conf_str)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;

        Ok(())
    }

    async fn list_incidents(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Incident>, StorageError> {
        let limit_clamped = limit.clamp(1, 100) as i64;
        let offset_i64 = offset as i64;

        let rows = sqlx::query(
            r#"
            SELECT id FROM incidents
            ORDER BY last_updated_at DESC
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(limit_clamped)
        .bind(offset_i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut incidents = Vec::with_capacity(rows.len());
        for row in rows {
            let id: Uuid = row.get("id");
            if let Some(inc) = self.get_incident_by_id(id).await? {
                incidents.push(inc);
            }
        }

        Ok(incidents)
    }

    async fn get_incident_by_id(&self, id: Uuid) -> Result<Option<Incident>, StorageError> {
        let row_opt = sqlx::query(
            r#"
            SELECT id, case_id, title, summary, status, severity,
                   first_observed_at, last_updated_at, structured_claims,
                   facts, reported_claims, unverified_claims,
                   associated_txids, associated_block_heights
            FROM incidents
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let Some(row) = row_opt else {
            return Ok(None);
        };

        let status_str: String = row.get("status");
        let severity_str: String = row.get("severity");

        let status: IncidentStatus = serde_json::from_str(&format!("\"{status_str}\""))
            .unwrap_or(IncidentStatus::Investigating);
        let severity: EventSeverity =
            serde_json::from_str(&format!("\"{severity_str}\"")).unwrap_or(EventSeverity::High);

        let structured_claims: serde_json::Value = row.get("structured_claims");
        let facts: serde_json::Value = row.get("facts");
        let reported: serde_json::Value = row.get("reported_claims");
        let unverified: serde_json::Value = row.get("unverified_claims");
        let txids: serde_json::Value = row.get("associated_txids");
        let blocks: serde_json::Value = row.get("associated_block_heights");
        let updated_at: DateTime<Utc> = row.get("last_updated_at");

        // 1. Fetch latest recovery snapshot
        let snap_opt = sqlx::query(
            r#"
            SELECT affected_sats, recovered_sats, outstanding_sats,
                   as_of_timestamp, is_estimate, source_label
            FROM incident_recovery_snapshots
            WHERE incident_id = $1
            ORDER BY as_of_timestamp DESC
            LIMIT 1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let recovery = match snap_opt {
            Some(s) => {
                let affected_i64: i64 = s.get("affected_sats");
                let recovered_i64: i64 = s.get("recovered_sats");
                let outstanding_i64: i64 = s.get("outstanding_sats");
                RecoverySummary {
                    affected_sats: i64_to_u64_checked(affected_i64)?,
                    recovered_sats: i64_to_u64_checked(recovered_i64)?,
                    outstanding_sats: i64_to_u64_checked(outstanding_i64)?,
                    as_of_timestamp: s.get("as_of_timestamp"),
                    source: s.get("source_label"),
                    is_estimate: s.get("is_estimate"),
                }
            }
            None => RecoverySummary::new(0, 0, updated_at),
        };

        // 2. Fetch Sources
        let src_rows = sqlx::query(
            r#"
            SELECT id, publisher, title, url, publication_timestamp,
                   retrieved_timestamp, source_category, reliability_score, name
            FROM incident_sources
            WHERE incident_id = $1
            ORDER BY publication_timestamp ASC NULLS LAST
            "#,
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut sources = Vec::with_capacity(src_rows.len());
        for s in src_rows {
            let cat_str: String = s.get("source_category");
            let source_category: SourceCategory = serde_json::from_str(&format!("\"{cat_str}\""))
                .unwrap_or(SourceCategory::BitcoinBlockchain);

            sources.push(Source {
                id: s.get("id"),
                publisher: s.get("publisher"),
                title: s.get("title"),
                url: s.get("url"),
                publication_timestamp: s.get("publication_timestamp"),
                retrieved_timestamp: s.get("retrieved_timestamp"),
                source_category,
                reliability_score: s.get("reliability_score"),
                name: s.get("name"),
            });
        }

        // 3. Fetch Evidence
        let ev_rows = sqlx::query(
            r#"
            SELECT id, incident_id, evidence_type, confidence, title, description,
                   observed_at, source_id, source_reference, txid, block_hash,
                   block_height, chain, verified, raw_data, created_at
            FROM incident_evidence
            WHERE incident_id = $1
            ORDER BY observed_at ASC NULLS LAST
            "#,
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut evidence = Vec::with_capacity(ev_rows.len());
        for ev in ev_rows {
            let type_str: String = ev.get("evidence_type");
            let conf_str: String = ev.get("confidence");
            let chain_str: String = ev.get("chain");
            let block_height_i64: Option<i64> = ev.get("block_height");
            let block_height = match block_height_i64 {
                Some(h) => Some(i64_to_u64_checked(h)?),
                None => None,
            };

            let evidence_type: EvidenceType = serde_json::from_str(&format!("\"{type_str}\""))
                .unwrap_or(EvidenceType::OnChainTransaction);
            let confidence: ProvenanceClassification =
                serde_json::from_str(&format!("\"{conf_str}\""))
                    .unwrap_or(ProvenanceClassification::Heuristic);
            let chain = match chain_str.to_lowercase().as_str() {
                "liquid" => Chain::Liquid,
                _ => Chain::Bitcoin,
            };

            evidence.push(Evidence {
                id: ev.get("id"),
                incident_id: ev.get("incident_id"),
                evidence_type,
                confidence,
                title: ev.get("title"),
                description: ev.get("description"),
                observed_at: ev.get("observed_at"),
                source_id: ev.get("source_id"),
                source_reference: ev.get("source_reference"),
                txid: ev.get("txid"),
                block_hash: ev.get("block_hash"),
                block_height,
                chain,
                verified: ev.get("verified"),
                raw_data: ev.get("raw_data"),
                created_at: ev.get("created_at"),
                reference: None,
                classification: None,
            });
        }

        // 4. Fetch Transactions
        let tx_rows = sqlx::query(
            r#"
            SELECT chain, txid, role, amount_sats, block_height, block_hash,
                   confirmed_at, evidence_id, notes
            FROM incident_transactions
            WHERE incident_id = $1
            ORDER BY confirmed_at ASC NULLS LAST
            "#,
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut transactions = Vec::with_capacity(tx_rows.len());
        for t in tx_rows {
            let chain_str: String = t.get("chain");
            let role_str: String = t.get("role");
            let amount_sats_i64: Option<i64> = t.get("amount_sats");
            let amount_sats = match amount_sats_i64 {
                Some(s) => Some(i64_to_u64_checked(s)?),
                None => None,
            };
            let block_height_i64: Option<i64> = t.get("block_height");
            let block_height = match block_height_i64 {
                Some(h) => Some(i64_to_u64_checked(h)?),
                None => None,
            };
            let chain = match chain_str.to_lowercase().as_str() {
                "liquid" => Chain::Liquid,
                _ => Chain::Bitcoin,
            };
            let role: TransactionRole =
                serde_json::from_str(&format!("\"{role_str}\"")).unwrap_or(TransactionRole::Other);

            transactions.push(IncidentTransaction {
                chain,
                txid: t.get("txid"),
                role,
                amount_sats,
                block_height,
                block_hash: t.get("block_hash"),
                confirmed_at: t.get("confirmed_at"),
                evidence_id: t.get("evidence_id"),
                notes: t.get("notes"),
            });
        }

        // 5. Fetch Blocks
        let blk_rows = sqlx::query(
            r#"
            SELECT chain, height, hash, timestamp, tx_count, evidence_id
            FROM incident_blocks
            WHERE incident_id = $1
            ORDER BY height ASC
            "#,
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut blocks_list = Vec::with_capacity(blk_rows.len());
        for b in blk_rows {
            let chain_str: String = b.get("chain");
            let height_i64: i64 = b.get("height");
            let tx_count_i32: Option<i32> = b.get("tx_count");
            let chain = match chain_str.to_lowercase().as_str() {
                "liquid" => Chain::Liquid,
                _ => Chain::Bitcoin,
            };

            blocks_list.push(IncidentBlock {
                chain,
                height: i64_to_u64_checked(height_i64)?,
                hash: b.get("hash"),
                timestamp: b.get("timestamp"),
                tx_count: tx_count_i32.map(|c| c as usize),
                evidence_id: b.get("evidence_id"),
            });
        }

        // 6. Fetch Entities
        let ent_rows = sqlx::query(
            r#"
            SELECT id, name, entity_type, description, attribution_confidence
            FROM incident_entities
            WHERE incident_id = $1
            "#,
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut entities = Vec::with_capacity(ent_rows.len());
        for ent in ent_rows {
            let conf_str: String = ent.get("attribution_confidence");
            let attribution_confidence: ProvenanceClassification =
                serde_json::from_str(&format!("\"{conf_str}\""))
                    .unwrap_or(ProvenanceClassification::Heuristic);

            entities.push(IncidentEntity {
                id: ent.get("id"),
                name: ent.get("name"),
                entity_type: ent.get("entity_type"),
                description: ent.get("description"),
                attribution_confidence,
            });
        }

        // 7. Fetch Messages
        let msg_rows = sqlx::query(
            r#"
            SELECT txid, chain, encoding, decoded_text, raw_hex,
                   confirmed_at, block_height, attributed_sender, sender_attribution_confidence
            FROM incident_messages
            WHERE incident_id = $1
            ORDER BY confirmed_at ASC NULLS LAST
            "#,
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut on_chain_messages = Vec::with_capacity(msg_rows.len());
        for m in msg_rows {
            let chain_str: String = m.get("chain");
            let conf_str: String = m.get("sender_attribution_confidence");
            let block_height_i64: Option<i64> = m.get("block_height");
            let block_height = match block_height_i64 {
                Some(h) => Some(i64_to_u64_checked(h)?),
                None => None,
            };
            let chain = match chain_str.to_lowercase().as_str() {
                "liquid" => Chain::Liquid,
                _ => Chain::Bitcoin,
            };
            let sender_attribution_confidence: ProvenanceClassification =
                serde_json::from_str(&format!("\"{conf_str}\""))
                    .unwrap_or(ProvenanceClassification::Unverified);

            on_chain_messages.push(OnChainMessage {
                txid: m.get("txid"),
                chain,
                encoding: m.get("encoding"),
                decoded_text: m.get("decoded_text"),
                raw_hex: m.get("raw_hex"),
                confirmed_at: m.get("confirmed_at"),
                block_height,
                attributed_sender: m.get("attributed_sender"),
                sender_attribution_confidence,
            });
        }

        // 8. Fetch Timeline
        let tl_rows = sqlx::query(
            r#"
            SELECT id, timestamp, title, description, category, source_id,
                   evidence_ids, transaction_txids, block_heights, classification
            FROM incident_timeline
            WHERE incident_id = $1
            ORDER BY timestamp ASC
            "#,
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut timeline = Vec::with_capacity(tl_rows.len());
        for tl in tl_rows {
            let cat_str: String = tl.get("category");
            let conf_str: String = tl.get("classification");
            let ev_ids_val: serde_json::Value = tl.get("evidence_ids");
            let txids_val: serde_json::Value = tl.get("transaction_txids");
            let heights_val: serde_json::Value = tl.get("block_heights");

            let category: TimelineCategory =
                serde_json::from_str(&format!("\"{cat_str}\"")).unwrap_or(TimelineCategory::Other);
            let classification: ProvenanceClassification =
                serde_json::from_str(&format!("\"{conf_str}\""))
                    .unwrap_or(ProvenanceClassification::Heuristic);

            timeline.push(TimelineEntry {
                id: tl.get("id"),
                timestamp: tl.get("timestamp"),
                title: tl.get("title"),
                description: tl.get("description"),
                category,
                source_id: tl.get("source_id"),
                evidence_ids: serde_json::from_value(ev_ids_val).unwrap_or_default(),
                transaction_txids: serde_json::from_value(txids_val).unwrap_or_default(),
                block_heights: serde_json::from_value(heights_val).unwrap_or_default(),
                classification,
            });
        }

        // 9. Fetch Technical Findings
        let tf_rows = sqlx::query(
            r#"
            SELECT component, area, category, summary, root_cause_details,
                   fix_summary, repository_url, pull_request_id, commit_hash
            FROM incident_technical_findings
            WHERE incident_id = $1
            "#,
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut technical_findings = Vec::with_capacity(tf_rows.len());
        for tf in tf_rows {
            technical_findings.push(TechnicalFinding {
                component: tf.get("component"),
                area: tf.get("area"),
                category: tf.get("category"),
                summary: tf.get("summary"),
                root_cause_details: tf.get("root_cause_details"),
                fix_summary: tf.get("fix_summary"),
                repository_url: tf.get("repository_url"),
                pull_request_id: tf.get("pull_request_id"),
                commit_hash: tf.get("commit_hash"),
            });
        }

        // 10. Fetch Updates
        let upd_rows = sqlx::query(
            r#"
            SELECT id, timestamp, title, summary, source_id, recovery_state
            FROM incident_updates
            WHERE incident_id = $1
            ORDER BY timestamp DESC
            "#,
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut updates = Vec::with_capacity(upd_rows.len());
        for upd in upd_rows {
            let recovery_val: Option<serde_json::Value> = upd.get("recovery_state");
            let recovery_state = match recovery_val {
                Some(v) => serde_json::from_value(v).ok(),
                None => None,
            };

            updates.push(IncidentUpdate {
                id: upd.get("id"),
                timestamp: upd.get("timestamp"),
                title: upd.get("title"),
                summary: upd.get("summary"),
                source_id: upd.get("source_id"),
                recovery_state,
            });
        }

        // 11. Fetch Graph Nodes & Edges
        let node_rows = sqlx::query(
            r#"
            SELECT node_id, label, node_type, chain, metadata
            FROM incident_graph_nodes
            WHERE incident_id = $1
            "#,
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut graph_nodes = Vec::with_capacity(node_rows.len());
        for n in node_rows {
            let node_type_str: String = n.get("node_type");
            let chain_str: Option<String> = n.get("chain");
            let node_type: GraphNodeType = serde_json::from_str(&format!("\"{node_type_str}\""))
                .unwrap_or(GraphNodeType::Transaction);
            let chain = chain_str.map(|s| {
                if s.eq_ignore_ascii_case("liquid") {
                    Chain::Liquid
                } else {
                    Chain::Bitcoin
                }
            });

            graph_nodes.push(GraphNode {
                id: n.get("node_id"),
                label: n.get("label"),
                node_type,
                chain,
                metadata: n.get("metadata"),
            });
        }

        let edge_rows = sqlx::query(
            r#"
            SELECT source_node_id, target_node_id, relationship, confidence
            FROM incident_graph_edges
            WHERE incident_id = $1
            "#,
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut graph_edges = Vec::with_capacity(edge_rows.len());
        for ed in edge_rows {
            let rel_str: String = ed.get("relationship");
            let conf_str: String = ed.get("confidence");
            let relationship: GraphEdgeType = serde_json::from_str(&format!("\"{rel_str}\""))
                .unwrap_or(GraphEdgeType::PossiblyRelated);
            let confidence: ProvenanceClassification =
                serde_json::from_str(&format!("\"{conf_str}\""))
                    .unwrap_or(ProvenanceClassification::Heuristic);

            graph_edges.push(GraphEdge {
                source: ed.get("source_node_id"),
                target: ed.get("target_node_id"),
                relationship,
                confidence,
            });
        }

        let total_btc_affected = recovery.affected_btc();
        let total_btc_recovered = recovery.recovered_btc();

        Ok(Some(Incident {
            id,
            case_id: row.get("case_id"),
            title: row.get("title"),
            summary: row.get("summary"),
            status,
            severity,
            recovery,
            first_observed_at: row.get("first_observed_at"),
            last_updated_at: updated_at,
            structured_claims: serde_json::from_value(structured_claims).unwrap_or_default(),
            entities,
            transactions,
            blocks: blocks_list,
            on_chain_messages,
            timeline,
            evidence,
            sources,
            technical_findings,
            updates,
            graph: IncidentGraph {
                nodes: graph_nodes,
                edges: graph_edges,
            },
            total_btc_affected,
            total_btc_recovered,
            facts: serde_json::from_value(facts).unwrap_or_default(),
            reported_claims: serde_json::from_value(reported).unwrap_or_default(),
            unverified_claims: serde_json::from_value(unverified).unwrap_or_default(),
            associated_txids: serde_json::from_value(txids).unwrap_or_default(),
            associated_block_heights: serde_json::from_value(blocks).unwrap_or_default(),
        }))
    }

    async fn get_incident_by_id_or_case_id(
        &self,
        identifier: &str,
    ) -> Result<Option<Incident>, StorageError> {
        if let Ok(id) = Uuid::parse_str(identifier) {
            return self.get_incident_by_id(id).await;
        }

        let row_opt = sqlx::query("SELECT id FROM incidents WHERE LOWER(case_id) = LOWER($1)")
            .bind(identifier)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;

        if let Some(row) = row_opt {
            let id: Uuid = row.get("id");
            self.get_incident_by_id(id).await
        } else {
            Ok(None)
        }
    }
}

// ---------------------------------------------------------------------------
// WatchTargetRepository Implementation
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl WatchTargetRepository for PostgresStorage {
    async fn save_watch_target(&self, target: &WatchTarget) -> Result<(), StorageError> {
        let (txid, vout, script_pubkey, address) = match &target.kind {
            WatchTargetKind::OutPoint { txid, vout } => {
                (Some(txid.clone()), Some(*vout as i32), None, None)
            }
            WatchTargetKind::ScriptPubKey { script_hex } => {
                (None, None, Some(script_hex.clone()), None)
            }
            WatchTargetKind::Address { address, .. } => (None, None, None, Some(address.clone())),
            WatchTargetKind::Transaction { txid } => (Some(txid.clone()), None, None, None),
        };

        let kind_str = target.kind.target_type_str().to_string();
        let conf_str = serde_json::to_string(&target.classification)
            .map_err(StorageError::Serialization)?
            .trim_matches('"')
            .to_string();
        let kind_payload_json = serde_json::to_value(&target.kind)?;

        sqlx::query(
            r#"
            INSERT INTO incident_watch_targets (
                id, incident_id, case_id, target_kind, txid, vout, script_pubkey,
                address, classification, source, evidence_id,
                label, active, kind_payload, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW())
            ON CONFLICT (id) DO UPDATE SET
                classification = EXCLUDED.classification,
                source = EXCLUDED.source,
                evidence_id = EXCLUDED.evidence_id,
                label = EXCLUDED.label,
                active = EXCLUDED.active,
                kind_payload = EXCLUDED.kind_payload,
                updated_at = NOW()
            "#,
        )
        .bind(target.id)
        .bind(target.incident_id)
        .bind(&target.case_id)
        .bind(kind_str)
        .bind(txid)
        .bind(vout)
        .bind(script_pubkey)
        .bind(address)
        .bind(conf_str)
        .bind(&target.source)
        .bind(target.evidence_id)
        .bind(&target.label)
        .bind(target.active)
        .bind(kind_payload_json)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        Ok(())
    }

    async fn list_watch_targets(
        &self,
        incident_id: Option<Uuid>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<WatchTarget>, StorageError> {
        let limit_clamped = limit.clamp(1, 200) as i64;
        let offset_i64 = offset as i64;

        let rows = sqlx::query(
            r#"
            SELECT id, incident_id, case_id, target_kind, txid, vout, script_pubkey,
                   address, classification, source, evidence_id,
                   label, active, kind_payload, created_at
            FROM incident_watch_targets
            WHERE ($1 IS NULL OR incident_id = $1)
            ORDER BY created_at ASC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(incident_id)
        .bind(limit_clamped)
        .bind(offset_i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut targets = Vec::with_capacity(rows.len());
        for row in rows {
            let conf_str: String = row.get("classification");
            let classification: ProvenanceClassification =
                serde_json::from_str(&format!("\"{conf_str}\""))
                    .unwrap_or(ProvenanceClassification::Heuristic);

            let source: String = row.get("source");

            let kind_payload_val: serde_json::Value = row.get("kind_payload");
            let kind: WatchTargetKind =
                serde_json::from_value(kind_payload_val).map_err(StorageError::Serialization)?;

            targets.push(WatchTarget {
                id: row.get("id"),
                incident_id: row.get("incident_id"),
                case_id: row.get("case_id"),
                kind,
                classification,
                source,
                evidence_id: row.get("evidence_id"),
                label: row.get("label"),
                created_at: row.get("created_at"),
                active: row.get("active"),
            });
        }

        Ok(targets)
    }

    async fn get_watch_target_by_id(&self, id: Uuid) -> Result<Option<WatchTarget>, StorageError> {
        let row_opt = sqlx::query(
            r#"
            SELECT id, incident_id, case_id, target_kind, txid, vout, script_pubkey,
                   address, classification, source, evidence_id,
                   label, active, kind_payload, created_at
            FROM incident_watch_targets
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let Some(row) = row_opt else {
            return Ok(None);
        };

        let conf_str: String = row.get("classification");
        let classification: ProvenanceClassification =
            serde_json::from_str(&format!("\"{conf_str}\""))
                .unwrap_or(ProvenanceClassification::Heuristic);

        let source: String = row.get("source");

        let kind_payload_val: serde_json::Value = row.get("kind_payload");
        let kind: WatchTargetKind =
            serde_json::from_value(kind_payload_val).map_err(StorageError::Serialization)?;

        Ok(Some(WatchTarget {
            id: row.get("id"),
            incident_id: row.get("incident_id"),
            case_id: row.get("case_id"),
            kind,
            classification,
            source,
            evidence_id: row.get("evidence_id"),
            label: row.get("label"),
            created_at: row.get("created_at"),
            active: row.get("active"),
        }))
    }
}

// ---------------------------------------------------------------------------
// IncidentActivityRepository Implementation
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl IncidentActivityRepository for PostgresStorage {
    async fn save_activity(&self, activity: &IncidentActivity) -> Result<(), StorageError> {
        let act_type_str = serde_json::to_string(&activity.activity_type)
            .map_err(StorageError::Serialization)?
            .trim_matches('"')
            .to_string();
        let status_str = serde_json::to_string(&activity.status)
            .map_err(StorageError::Serialization)?
            .trim_matches('"')
            .to_string();
        let conf_str = serde_json::to_string(&activity.confidence)
            .map_err(StorageError::Serialization)?
            .trim_matches('"')
            .to_string();
        let strength_str = serde_json::to_string(&activity.correlation_strength)
            .map_err(StorageError::Serialization)?
            .trim_matches('"')
            .to_string();

        let block_height_i64 = match activity.block_height {
            Some(h) => Some(u64_to_i64_checked(h)?),
            None => None,
        };
        let value_sats_i64 = match activity.value_sats {
            Some(s) => Some(u64_to_i64_checked(s)?),
            None => None,
        };

        let source_json = serde_json::to_value(&activity.source)?;
        let evidence_json = serde_json::to_value(&activity.evidence)?;

        // Deduplication & Confirmation Update Upsert:
        // Updating mempool status to confirmed must update the existing record rather than create a duplicate.
        sqlx::query(
            r#"
            INSERT INTO incident_activities (
                id, incident_id, case_id, activity_type, status, observed_at,
                trigger_txid, block_height, block_hash, value_sats, watch_target_id,
                confidence, correlation_strength, source, evidence, description,
                details, dedup_key, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, NOW())
            ON CONFLICT (dedup_key) DO UPDATE SET
                status = EXCLUDED.status,
                block_height = EXCLUDED.block_height,
                block_hash = EXCLUDED.block_hash,
                observed_at = EXCLUDED.observed_at,
                updated_at = NOW()
            "#,
        )
        .bind(activity.id)
        .bind(activity.incident_id)
        .bind(&activity.case_id)
        .bind(act_type_str)
        .bind(status_str)
        .bind(activity.observed_at)
        .bind(&activity.trigger_txid)
        .bind(block_height_i64)
        .bind(&activity.block_hash)
        .bind(value_sats_i64)
        .bind(activity.watch_target_id)
        .bind(conf_str)
        .bind(strength_str)
        .bind(source_json)
        .bind(evidence_json)
        .bind(&activity.description)
        .bind(&activity.details)
        .bind(&activity.dedup_key)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        self.cached_activities.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn list_activities(
        &self,
        incident_id: Option<Uuid>,
        status: Option<ActivityStatus>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<IncidentActivity>, StorageError> {
        let limit_clamped = limit.clamp(1, 200) as i64;
        let offset_i64 = offset as i64;

        let status_str_opt = match status {
            Some(st) => Some(
                serde_json::to_string(&st)
                    .map_err(StorageError::Serialization)?
                    .trim_matches('"')
                    .to_string(),
            ),
            None => None,
        };

        let rows = sqlx::query(
            r#"
            SELECT id, incident_id, case_id, activity_type, status, observed_at,
                   trigger_txid, block_height, block_hash, value_sats, watch_target_id,
                   confidence, correlation_strength, source, evidence, description,
                   details, dedup_key
            FROM incident_activities
            WHERE ($1 IS NULL OR incident_id = $1)
              AND ($2 IS NULL OR status = $2)
            ORDER BY observed_at DESC
            LIMIT $3 OFFSET $4
            "#,
        )
        .bind(incident_id)
        .bind(status_str_opt)
        .bind(limit_clamped)
        .bind(offset_i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut activities = Vec::with_capacity(rows.len());
        for row in rows {
            let act_type_str: String = row.get("activity_type");
            let status_str: String = row.get("status");
            let conf_str: String = row.get("confidence");
            let strength_str: String = row.get("correlation_strength");

            let activity_type: IncidentActivityType =
                serde_json::from_str(&format!("\"{act_type_str}\""))
                    .unwrap_or(IncidentActivityType::WatchedTransactionObserved);
            let status: ActivityStatus = serde_json::from_str(&format!("\"{status_str}\""))
                .unwrap_or(ActivityStatus::Confirmed);
            let confidence: ProvenanceClassification =
                serde_json::from_str(&format!("\"{conf_str}\""))
                    .unwrap_or(ProvenanceClassification::Heuristic);
            let correlation_strength: CorrelationStrength =
                serde_json::from_str(&format!("\"{strength_str}\""))
                    .unwrap_or(CorrelationStrength::Heuristic);

            let block_height_i64: Option<i64> = row.get("block_height");
            let block_height = match block_height_i64 {
                Some(h) => Some(i64_to_u64_checked(h)?),
                None => None,
            };

            let value_sats_i64: Option<i64> = row.get("value_sats");
            let value_sats = match value_sats_i64 {
                Some(s) => Some(i64_to_u64_checked(s)?),
                None => None,
            };

            let source_val: serde_json::Value = row.get("source");
            let source: ObservationSource = serde_json::from_value(source_val)
                .unwrap_or_else(|_| ObservationSource::mempool_ws("unknown"));

            let evidence_val: serde_json::Value = row.get("evidence");
            let evidence: Vec<Uuid> = serde_json::from_value(evidence_val).unwrap_or_default();

            activities.push(IncidentActivity {
                id: row.get("id"),
                incident_id: row.get("incident_id"),
                case_id: row.get("case_id"),
                activity_type,
                observed_at: row.get("observed_at"),
                trigger_txid: row.get("trigger_txid"),
                block_height,
                block_hash: row.get("block_hash"),
                value_sats,
                watch_target_id: row.get("watch_target_id"),
                confidence,
                correlation_strength,
                status,
                source,
                evidence,
                description: row.get("description"),
                details: row.get("details"),
                dedup_key: row.get("dedup_key"),
            });
        }

        Ok(activities)
    }

    async fn get_activity_by_id(&self, id: Uuid) -> Result<Option<IncidentActivity>, StorageError> {
        let row_opt = sqlx::query(
            r#"
            SELECT id, incident_id, case_id, activity_type, status, observed_at,
                   trigger_txid, block_height, block_hash, value_sats, watch_target_id,
                   confidence, correlation_strength, source, evidence, description,
                   details, dedup_key
            FROM incident_activities
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let Some(row) = row_opt else {
            return Ok(None);
        };

        let act_type_str: String = row.get("activity_type");
        let status_str: String = row.get("status");
        let conf_str: String = row.get("confidence");
        let strength_str: String = row.get("correlation_strength");

        let activity_type: IncidentActivityType =
            serde_json::from_str(&format!("\"{act_type_str}\""))
                .unwrap_or(IncidentActivityType::WatchedTransactionObserved);
        let status: ActivityStatus =
            serde_json::from_str(&format!("\"{status_str}\"")).unwrap_or(ActivityStatus::Confirmed);
        let confidence: ProvenanceClassification = serde_json::from_str(&format!("\"{conf_str}\""))
            .unwrap_or(ProvenanceClassification::Heuristic);
        let correlation_strength: CorrelationStrength =
            serde_json::from_str(&format!("\"{strength_str}\""))
                .unwrap_or(CorrelationStrength::Heuristic);

        let block_height_i64: Option<i64> = row.get("block_height");
        let block_height = match block_height_i64 {
            Some(h) => Some(i64_to_u64_checked(h)?),
            None => None,
        };

        let value_sats_i64: Option<i64> = row.get("value_sats");
        let value_sats = match value_sats_i64 {
            Some(s) => Some(i64_to_u64_checked(s)?),
            None => None,
        };

        let source_val: serde_json::Value = row.get("source");
        let source: ObservationSource = serde_json::from_value(source_val)
            .unwrap_or_else(|_| ObservationSource::mempool_ws("unknown"));

        let evidence_val: serde_json::Value = row.get("evidence");
        let evidence: Vec<Uuid> = serde_json::from_value(evidence_val).unwrap_or_default();

        Ok(Some(IncidentActivity {
            id: row.get("id"),
            incident_id: row.get("incident_id"),
            case_id: row.get("case_id"),
            activity_type,
            observed_at: row.get("observed_at"),
            trigger_txid: row.get("trigger_txid"),
            block_height,
            block_hash: row.get("block_hash"),
            value_sats,
            watch_target_id: row.get("watch_target_id"),
            confidence,
            correlation_strength,
            status,
            source,
            evidence,
            description: row.get("description"),
            details: row.get("details"),
            dedup_key: row.get("dedup_key"),
        }))
    }

    async fn get_activity_by_dedup_key(
        &self,
        dedup_key: &str,
    ) -> Result<Option<IncidentActivity>, StorageError> {
        let row_opt = sqlx::query(
            r#"
            SELECT id, incident_id, case_id, activity_type, status, observed_at,
                   trigger_txid, block_height, block_hash, value_sats, watch_target_id,
                   confidence, correlation_strength, source, evidence, description,
                   details, dedup_key
            FROM incident_activities
            WHERE dedup_key = $1
            "#,
        )
        .bind(dedup_key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let Some(row) = row_opt else {
            return Ok(None);
        };

        let act_type_str: String = row.get("activity_type");
        let status_str: String = row.get("status");
        let conf_str: String = row.get("confidence");
        let strength_str: String = row.get("correlation_strength");

        let activity_type: IncidentActivityType =
            serde_json::from_str(&format!("\"{act_type_str}\""))
                .unwrap_or(IncidentActivityType::WatchedTransactionObserved);
        let status: ActivityStatus =
            serde_json::from_str(&format!("\"{status_str}\"")).unwrap_or(ActivityStatus::Confirmed);
        let confidence: ProvenanceClassification = serde_json::from_str(&format!("\"{conf_str}\""))
            .unwrap_or(ProvenanceClassification::Heuristic);
        let correlation_strength: CorrelationStrength =
            serde_json::from_str(&format!("\"{strength_str}\""))
                .unwrap_or(CorrelationStrength::Heuristic);

        let block_height_i64: Option<i64> = row.get("block_height");
        let block_height = match block_height_i64 {
            Some(h) => Some(i64_to_u64_checked(h)?),
            None => None,
        };

        let value_sats_i64: Option<i64> = row.get("value_sats");
        let value_sats = match value_sats_i64 {
            Some(s) => Some(i64_to_u64_checked(s)?),
            None => None,
        };

        let source_val: serde_json::Value = row.get("source");
        let source: ObservationSource = serde_json::from_value(source_val)
            .unwrap_or_else(|_| ObservationSource::mempool_ws("unknown"));

        let evidence_val: serde_json::Value = row.get("evidence");
        let evidence: Vec<Uuid> = serde_json::from_value(evidence_val).unwrap_or_default();

        Ok(Some(IncidentActivity {
            id: row.get("id"),
            incident_id: row.get("incident_id"),
            case_id: row.get("case_id"),
            activity_type,
            observed_at: row.get("observed_at"),
            trigger_txid: row.get("trigger_txid"),
            block_height,
            block_hash: row.get("block_hash"),
            value_sats,
            watch_target_id: row.get("watch_target_id"),
            confidence,
            correlation_strength,
            status,
            source,
            evidence,
            description: row.get("description"),
            details: row.get("details"),
            dedup_key: row.get("dedup_key"),
        }))
    }
}

// ---------------------------------------------------------------------------
// IncidentAlertRepository Implementation
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl IncidentAlertRepository for PostgresStorage {
    async fn save_alert(&self, alert: &IncidentAlert) -> Result<(), StorageError> {
        let severity_str = serde_json::to_string(&alert.severity)
            .map_err(StorageError::Serialization)?
            .trim_matches('"')
            .to_string();
        let conf_str = serde_json::to_string(&alert.confidence)
            .map_err(StorageError::Serialization)?
            .trim_matches('"')
            .to_string();
        let strength_str = serde_json::to_string(&alert.correlation_strength)
            .map_err(StorageError::Serialization)?
            .trim_matches('"')
            .to_string();

        let value_sats_i64 = match alert.value_sats {
            Some(s) => Some(u64_to_i64_checked(s)?),
            None => None,
        };

        sqlx::query(
            r#"
            INSERT INTO incident_alerts (
                id, incident_id, case_id, incident_title, activity_id, severity,
                title, summary, confidence, correlation_strength, observed_at,
                value_sats, trigger_txid
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(alert.id)
        .bind(alert.incident_id)
        .bind(&alert.case_id)
        .bind(&alert.incident_title)
        .bind(alert.activity_id)
        .bind(severity_str)
        .bind(&alert.title)
        .bind(&alert.summary)
        .bind(conf_str)
        .bind(strength_str)
        .bind(alert.observed_at)
        .bind(value_sats_i64)
        .bind(&alert.trigger_txid)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        Ok(())
    }

    async fn list_alerts(
        &self,
        incident_id: Option<Uuid>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<IncidentAlert>, StorageError> {
        let limit_clamped = limit.clamp(1, 200) as i64;
        let offset_i64 = offset as i64;

        let rows = sqlx::query(
            r#"
            SELECT id, incident_id, case_id, incident_title, activity_id, severity,
                   title, summary, confidence, correlation_strength, observed_at,
                   value_sats, trigger_txid
            FROM incident_alerts
            WHERE ($1 IS NULL OR incident_id = $1)
            ORDER BY observed_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(incident_id)
        .bind(limit_clamped)
        .bind(offset_i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut alerts = Vec::with_capacity(rows.len());
        for row in rows {
            let severity_str: String = row.get("severity");
            let conf_str: String = row.get("confidence");
            let strength_str: String = row.get("correlation_strength");

            let severity: EventSeverity =
                serde_json::from_str(&format!("\"{severity_str}\"")).unwrap_or(EventSeverity::Info);
            let confidence: ProvenanceClassification =
                serde_json::from_str(&format!("\"{conf_str}\""))
                    .unwrap_or(ProvenanceClassification::Heuristic);
            let correlation_strength: CorrelationStrength =
                serde_json::from_str(&format!("\"{strength_str}\""))
                    .unwrap_or(CorrelationStrength::Heuristic);

            let value_sats_i64: Option<i64> = row.get("value_sats");
            let value_sats = match value_sats_i64 {
                Some(s) => Some(i64_to_u64_checked(s)?),
                None => None,
            };

            alerts.push(IncidentAlert {
                id: row.get("id"),
                incident_id: row.get("incident_id"),
                case_id: row.get("case_id"),
                incident_title: row.get("incident_title"),
                activity_id: row.get("activity_id"),
                severity,
                title: row.get("title"),
                summary: row.get("summary"),
                confidence,
                correlation_strength,
                observed_at: row.get("observed_at"),
                value_sats,
                trigger_txid: row.get("trigger_txid"),
            });
        }

        Ok(alerts)
    }

    async fn get_alert_by_id(&self, id: Uuid) -> Result<Option<IncidentAlert>, StorageError> {
        let row_opt = sqlx::query(
            r#"
            SELECT id, incident_id, case_id, incident_title, activity_id, severity,
                   title, summary, confidence, correlation_strength, observed_at,
                   value_sats, trigger_txid
            FROM incident_alerts
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let Some(row) = row_opt else {
            return Ok(None);
        };

        let severity_str: String = row.get("severity");
        let conf_str: String = row.get("confidence");
        let strength_str: String = row.get("correlation_strength");

        let severity: EventSeverity =
            serde_json::from_str(&format!("\"{severity_str}\"")).unwrap_or(EventSeverity::Info);
        let confidence: ProvenanceClassification = serde_json::from_str(&format!("\"{conf_str}\""))
            .unwrap_or(ProvenanceClassification::Heuristic);
        let correlation_strength: CorrelationStrength =
            serde_json::from_str(&format!("\"{strength_str}\""))
                .unwrap_or(CorrelationStrength::Heuristic);

        let value_sats_i64: Option<i64> = row.get("value_sats");
        let value_sats = match value_sats_i64 {
            Some(s) => Some(i64_to_u64_checked(s)?),
            None => None,
        };

        Ok(Some(IncidentAlert {
            id: row.get("id"),
            incident_id: row.get("incident_id"),
            case_id: row.get("case_id"),
            incident_title: row.get("incident_title"),
            activity_id: row.get("activity_id"),
            severity,
            title: row.get("title"),
            summary: row.get("summary"),
            confidence,
            correlation_strength,
            observed_at: row.get("observed_at"),
            value_sats,
            trigger_txid: row.get("trigger_txid"),
        }))
    }
}

// ---------------------------------------------------------------------------
// ReplayRepository Implementation
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl ReplayRepository for PostgresStorage {
    async fn create_job(&self, job: &ReplayJob) -> Result<(), StorageError> {
        let start_height_i64 = u64_to_i64_checked(job.start_height)?;
        let end_height_i64 = u64_to_i64_checked(job.end_height)?;
        let current_height_i64 = u64_to_i64_checked(job.current_height)?;
        let blocks_i64 = u64_to_i64_checked(job.blocks_processed)?;
        let txs_i64 = u64_to_i64_checked(job.transactions_processed)?;
        let events_i64 = u64_to_i64_checked(job.events_generated)?;
        let events_created_i64 = u64_to_i64_checked(job.events_created)?;
        let events_existing_i64 = u64_to_i64_checked(job.events_existing)?;
        let obs_recorded_i64 = u64_to_i64_checked(job.observations_recorded)?;
        let errors_i64 = u64_to_i64_checked(job.error_count)?;
        let status_str = job.status.as_str();

        sqlx::query(
            r#"
            INSERT INTO replay_jobs (
                id, network, start_height, end_height, current_height, status, source_type,
                created_at, started_at, completed_at, blocks_processed, transactions_processed,
                events_generated, events_created, events_existing, observations_recorded,
                error_count, last_error
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
            ON CONFLICT (id) DO UPDATE SET
                current_height = EXCLUDED.current_height,
                status = EXCLUDED.status,
                started_at = EXCLUDED.started_at,
                completed_at = EXCLUDED.completed_at,
                blocks_processed = EXCLUDED.blocks_processed,
                transactions_processed = EXCLUDED.transactions_processed,
                events_generated = EXCLUDED.events_generated,
                events_created = EXCLUDED.events_created,
                events_existing = EXCLUDED.events_existing,
                observations_recorded = EXCLUDED.observations_recorded,
                error_count = EXCLUDED.error_count,
                last_error = EXCLUDED.last_error
            "#,
        )
        .bind(job.id)
        .bind(&job.network)
        .bind(start_height_i64)
        .bind(end_height_i64)
        .bind(current_height_i64)
        .bind(status_str)
        .bind(&job.source_type)
        .bind(job.created_at)
        .bind(job.started_at)
        .bind(job.completed_at)
        .bind(blocks_i64)
        .bind(txs_i64)
        .bind(events_i64)
        .bind(events_created_i64)
        .bind(events_existing_i64)
        .bind(obs_recorded_i64)
        .bind(errors_i64)
        .bind(&job.last_error)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        Ok(())
    }

    async fn get_job(&self, id: Uuid) -> Result<Option<ReplayJob>, StorageError> {
        let row_opt = sqlx::query(
            r#"
            SELECT id, network, start_height, end_height, current_height, status, source_type,
                   created_at, started_at, completed_at, blocks_processed, transactions_processed,
                   events_generated,
                   COALESCE(events_created, 0) as events_created,
                   COALESCE(events_existing, 0) as events_existing,
                   COALESCE(observations_recorded, 0) as observations_recorded,
                   error_count, last_error
            FROM replay_jobs
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let Some(row) = row_opt else {
            return Ok(None);
        };

        let status_str: String = row.get("status");
        let status = match status_str.to_lowercase().as_str() {
            "pending" => ReplayJobStatus::Pending,
            "running" => ReplayJobStatus::Running,
            "paused" => ReplayJobStatus::Paused,
            "completed" => ReplayJobStatus::Completed,
            "cancelled" => ReplayJobStatus::Cancelled,
            "failed" => ReplayJobStatus::Failed,
            _ => ReplayJobStatus::Failed,
        };

        let start_height: i64 = row.get("start_height");
        let end_height: i64 = row.get("end_height");
        let current_height: i64 = row.get("current_height");
        let blocks_processed: i64 = row.get("blocks_processed");
        let transactions_processed: i64 = row.get("transactions_processed");
        let events_generated: i64 = row.get("events_generated");
        let events_created: i64 = row.get("events_created");
        let events_existing: i64 = row.get("events_existing");
        let observations_recorded: i64 = row.get("observations_recorded");
        let error_count: i64 = row.get("error_count");

        Ok(Some(ReplayJob {
            id: row.get("id"),
            network: row.get("network"),
            start_height: i64_to_u64_checked(start_height)?,
            end_height: i64_to_u64_checked(end_height)?,
            current_height: i64_to_u64_checked(current_height)?,
            status,
            source_type: row.get("source_type"),
            created_at: row.get("created_at"),
            started_at: row.get("started_at"),
            completed_at: row.get("completed_at"),
            blocks_processed: i64_to_u64_checked(blocks_processed)?,
            transactions_processed: i64_to_u64_checked(transactions_processed)?,
            events_generated: i64_to_u64_checked(events_generated)?,
            events_created: i64_to_u64_checked(events_created)?,
            events_existing: i64_to_u64_checked(events_existing)?,
            observations_recorded: i64_to_u64_checked(observations_recorded)?,
            error_count: i64_to_u64_checked(error_count)?,
            last_error: row.get("last_error"),
        }))
    }

    async fn update_job(&self, job: &ReplayJob) -> Result<(), StorageError> {
        self.create_job(job).await
    }

    async fn list_jobs(&self, limit: usize, offset: usize) -> Result<Vec<ReplayJob>, StorageError> {
        let limit_clamped = limit.clamp(1, 100) as i64;
        let offset_i64 = offset as i64;

        let rows = sqlx::query(
            r#"
            SELECT id, network, start_height, end_height, current_height, status, source_type,
                   created_at, started_at, completed_at, blocks_processed, transactions_processed,
                   events_generated,
                   COALESCE(events_created, 0) as events_created,
                   COALESCE(events_existing, 0) as events_existing,
                   COALESCE(observations_recorded, 0) as observations_recorded,
                   error_count, last_error
            FROM replay_jobs
            ORDER BY created_at DESC
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(limit_clamped)
        .bind(offset_i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut jobs = Vec::with_capacity(rows.len());
        for row in rows {
            let status_str: String = row.get("status");
            let status = match status_str.to_lowercase().as_str() {
                "pending" => ReplayJobStatus::Pending,
                "running" => ReplayJobStatus::Running,
                "paused" => ReplayJobStatus::Paused,
                "completed" => ReplayJobStatus::Completed,
                "cancelled" => ReplayJobStatus::Cancelled,
                "failed" => ReplayJobStatus::Failed,
                _ => ReplayJobStatus::Failed,
            };

            let start_height: i64 = row.get("start_height");
            let end_height: i64 = row.get("end_height");
            let current_height: i64 = row.get("current_height");
            let blocks_processed: i64 = row.get("blocks_processed");
            let transactions_processed: i64 = row.get("transactions_processed");
            let events_generated: i64 = row.get("events_generated");
            let events_created: i64 = row.get("events_created");
            let events_existing: i64 = row.get("events_existing");
            let observations_recorded: i64 = row.get("observations_recorded");
            let error_count: i64 = row.get("error_count");

            jobs.push(ReplayJob {
                id: row.get("id"),
                network: row.get("network"),
                start_height: i64_to_u64_checked(start_height)?,
                end_height: i64_to_u64_checked(end_height)?,
                current_height: i64_to_u64_checked(current_height)?,
                status,
                source_type: row.get("source_type"),
                created_at: row.get("created_at"),
                started_at: row.get("started_at"),
                completed_at: row.get("completed_at"),
                blocks_processed: i64_to_u64_checked(blocks_processed)?,
                transactions_processed: i64_to_u64_checked(transactions_processed)?,
                events_generated: i64_to_u64_checked(events_generated)?,
                events_created: i64_to_u64_checked(events_created)?,
                events_existing: i64_to_u64_checked(events_existing)?,
                observations_recorded: i64_to_u64_checked(observations_recorded)?,
                error_count: i64_to_u64_checked(error_count)?,
                last_error: row.get("last_error"),
            });
        }

        Ok(jobs)
    }

    async fn save_checkpoint(&self, checkpoint: &ReplayCheckpoint) -> Result<(), StorageError> {
        let completed_height_i64 = u64_to_i64_checked(checkpoint.completed_height)?;
        let blocks_i64 = u64_to_i64_checked(checkpoint.blocks_processed)?;
        let txs_i64 = u64_to_i64_checked(checkpoint.transactions_processed)?;
        let events_i64 = u64_to_i64_checked(checkpoint.events_generated)?;
        let events_created_i64 = u64_to_i64_checked(checkpoint.events_created)?;
        let events_existing_i64 = u64_to_i64_checked(checkpoint.events_existing)?;
        let obs_recorded_i64 = u64_to_i64_checked(checkpoint.observations_recorded)?;

        sqlx::query(
            r#"
            INSERT INTO replay_checkpoints (
                id, job_id, completed_height, blocks_processed, transactions_processed,
                events_generated, events_created, events_existing, observations_recorded,
                checkpointed_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(checkpoint.id)
        .bind(checkpoint.job_id)
        .bind(completed_height_i64)
        .bind(blocks_i64)
        .bind(txs_i64)
        .bind(events_i64)
        .bind(events_created_i64)
        .bind(events_existing_i64)
        .bind(obs_recorded_i64)
        .bind(checkpoint.checkpointed_at)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        Ok(())
    }

    async fn get_latest_checkpoint(
        &self,
        job_id: Uuid,
    ) -> Result<Option<ReplayCheckpoint>, StorageError> {
        let row_opt = sqlx::query(
            r#"
            SELECT id, job_id, completed_height, blocks_processed, transactions_processed,
                   events_generated,
                   COALESCE(events_created, 0) as events_created,
                   COALESCE(events_existing, 0) as events_existing,
                   COALESCE(observations_recorded, 0) as observations_recorded,
                   checkpointed_at
            FROM replay_checkpoints
            WHERE job_id = $1
            ORDER BY completed_height DESC
            LIMIT 1
            "#,
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let Some(row) = row_opt else {
            return Ok(None);
        };

        let completed_height: i64 = row.get("completed_height");
        let blocks_processed: i64 = row.get("blocks_processed");
        let transactions_processed: i64 = row.get("transactions_processed");
        let events_generated: i64 = row.get("events_generated");
        let events_created: i64 = row.get("events_created");
        let events_existing: i64 = row.get("events_existing");
        let observations_recorded: i64 = row.get("observations_recorded");

        Ok(Some(ReplayCheckpoint {
            id: row.get("id"),
            job_id: row.get("job_id"),
            completed_height: i64_to_u64_checked(completed_height)?,
            blocks_processed: i64_to_u64_checked(blocks_processed)?,
            transactions_processed: i64_to_u64_checked(transactions_processed)?,
            events_generated: i64_to_u64_checked(events_generated)?,
            events_created: i64_to_u64_checked(events_created)?,
            events_existing: i64_to_u64_checked(events_existing)?,
            observations_recorded: i64_to_u64_checked(observations_recorded)?,
            checkpointed_at: row.get("checkpointed_at"),
        }))
    }
}

// ---------------------------------------------------------------------------
// BaselineRepository Implementation
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl BaselineRepository for PostgresStorage {
    async fn create_baseline_run(&self, run: &BaselineRun) -> Result<(), StorageError> {
        let start_height_i64 = u64_to_i64_checked(run.start_height)?;
        let end_height_i64 = u64_to_i64_checked(run.end_height)?;
        let canonical_count_i64 = u64_to_i64_checked(run.canonical_event_count)?;
        let status_str = run.status.as_str();

        sqlx::query(
            r#"
            INSERT INTO baseline_runs (
                id, network, start_height, end_height, started_at, completed_at,
                status, algorithm_version, canonical_event_count, error_message,
                metadata, created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (id) DO UPDATE SET
                status = EXCLUDED.status,
                completed_at = EXCLUDED.completed_at,
                canonical_event_count = EXCLUDED.canonical_event_count,
                error_message = EXCLUDED.error_message,
                metadata = EXCLUDED.metadata
            "#,
        )
        .bind(run.id)
        .bind(&run.network)
        .bind(start_height_i64)
        .bind(end_height_i64)
        .bind(run.started_at)
        .bind(run.completed_at)
        .bind(status_str)
        .bind(&run.algorithm_version)
        .bind(canonical_count_i64)
        .bind(&run.error_message)
        .bind(&run.metadata)
        .bind(run.created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        Ok(())
    }

    async fn update_baseline_run_status(
        &self,
        id: Uuid,
        status: BaselineRunStatus,
        completed_at: Option<DateTime<Utc>>,
        canonical_event_count: u64,
        error_message: Option<String>,
    ) -> Result<(), StorageError> {
        let canonical_count_i64 = u64_to_i64_checked(canonical_event_count)?;
        let status_str = status.as_str();

        let rows_affected = sqlx::query(
            r#"
            UPDATE baseline_runs
            SET status = $2,
                completed_at = $3,
                canonical_event_count = $4,
                error_message = $5
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(status_str)
        .bind(completed_at)
        .bind(canonical_count_i64)
        .bind(error_message)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?
        .rows_affected();

        if rows_affected == 0 {
            return Err(StorageError::NotFound(format!(
                "BaselineRun {id} not found"
            )));
        }

        Ok(())
    }

    async fn get_baseline_run(&self, id: Uuid) -> Result<Option<BaselineRun>, StorageError> {
        let row_opt = sqlx::query(
            r#"
            SELECT id, network, start_height, end_height, started_at, completed_at,
                   status, algorithm_version, canonical_event_count, error_message,
                   metadata, created_at
            FROM baseline_runs
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let Some(row) = row_opt else {
            return Ok(None);
        };

        row_to_baseline_run(&row).map(Some)
    }

    async fn list_baseline_runs(
        &self,
        network: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<BaselineRun>, StorageError> {
        let limit_i64 = limit.clamp(1, 200) as i64;
        let offset_i64 = offset as i64;

        let rows = sqlx::query(
            r#"
            SELECT id, network, start_height, end_height, started_at, completed_at,
                   status, algorithm_version, canonical_event_count, error_message,
                   metadata, created_at
            FROM baseline_runs
            WHERE ($1::VARCHAR IS NULL OR network = $1)
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(network)
        .bind(limit_i64)
        .bind(offset_i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut runs = Vec::with_capacity(rows.len());
        for row in rows {
            runs.push(row_to_baseline_run(&row)?);
        }

        Ok(runs)
    }

    async fn get_latest_compatible_baseline_run(
        &self,
        network: &str,
        height: Option<u64>,
        version: Option<&str>,
    ) -> Result<Option<BaselineRun>, StorageError> {
        let height_i64 = match height {
            Some(h) => Some(u64_to_i64_checked(h)?),
            None => None,
        };

        let row_opt = sqlx::query(
            r#"
            SELECT id, network, start_height, end_height, started_at, completed_at,
                   status, algorithm_version, canonical_event_count, error_message,
                   metadata, created_at
            FROM baseline_runs
            WHERE network = $1
              AND status = 'COMPLETED'
              AND ($2::BIGINT IS NULL OR start_height <= $2)
              AND ($3::VARCHAR IS NULL OR algorithm_version = $3)
            ORDER BY end_height DESC, completed_at DESC
            LIMIT 1
            "#,
        )
        .bind(network)
        .bind(height_i64)
        .bind(version)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let Some(row) = row_opt else {
            return Ok(None);
        };

        row_to_baseline_run(&row).map(Some)
    }

    async fn save_baseline_distributions(
        &self,
        distributions: &[BaselineDistribution],
    ) -> Result<(), StorageError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;

        for dist in distributions {
            let event_type_str = serde_json::to_string(&dist.event_type)
                .map_err(StorageError::Serialization)?
                .trim_matches('"')
                .to_string();
            let metric_str = serde_json::to_string(&dist.metric)
                .map_err(StorageError::Serialization)?
                .trim_matches('"')
                .to_string();
            let unit_str = dist.unit.as_str();
            let quality_str = dist.quality.as_str();
            let mean_str = format!("{:.4}", dist.mean);

            sqlx::query(
                r#"
                INSERT INTO baseline_distributions (
                    id, baseline_run_id, event_type, metric, unit, sample_count,
                    candidate_count, missing_count, coverage_ratio,
                    minimum, maximum, mean, p50, p75, p90, p95, p99, p999,
                    quality, samples_json, created_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9,
                    $10::NUMERIC, $11::NUMERIC, $12::NUMERIC, $13::NUMERIC, $14::NUMERIC,
                    $15::NUMERIC, $16::NUMERIC, $17::NUMERIC, $18::NUMERIC,
                    $19, $20, $21
                )
                ON CONFLICT (baseline_run_id, event_type, metric) DO UPDATE SET
                    sample_count = EXCLUDED.sample_count,
                    candidate_count = EXCLUDED.candidate_count,
                    missing_count = EXCLUDED.missing_count,
                    coverage_ratio = EXCLUDED.coverage_ratio,
                    minimum = EXCLUDED.minimum,
                    maximum = EXCLUDED.maximum,
                    mean = EXCLUDED.mean,
                    p50 = EXCLUDED.p50,
                    p75 = EXCLUDED.p75,
                    p90 = EXCLUDED.p90,
                    p95 = EXCLUDED.p95,
                    p99 = EXCLUDED.p99,
                    p999 = EXCLUDED.p999,
                    quality = EXCLUDED.quality,
                    samples_json = EXCLUDED.samples_json
                "#,
            )
            .bind(dist.id)
            .bind(dist.baseline_run_id)
            .bind(event_type_str)
            .bind(metric_str)
            .bind(unit_str)
            .bind(u64_to_i64_checked(dist.sample_count)?)
            .bind(u64_to_i64_checked(dist.candidate_count)?)
            .bind(u64_to_i64_checked(dist.missing_count)?)
            .bind(dist.coverage_ratio)
            .bind(dist.minimum.to_numeric_string())
            .bind(dist.maximum.to_numeric_string())
            .bind(mean_str)
            .bind(dist.p50.to_numeric_string())
            .bind(dist.p75.to_numeric_string())
            .bind(dist.p90.to_numeric_string())
            .bind(dist.p95.to_numeric_string())
            .bind(dist.p99.to_numeric_string())
            .bind(dist.p999.to_numeric_string())
            .bind(quality_str)
            .bind(&dist.samples_json)
            .bind(dist.created_at)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;

        Ok(())
    }

    async fn get_baseline_distributions(
        &self,
        baseline_run_id: Uuid,
    ) -> Result<Vec<BaselineDistribution>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT id, baseline_run_id, event_type, metric, unit, sample_count,
                   candidate_count, missing_count, coverage_ratio,
                   minimum::TEXT as minimum, maximum::TEXT as maximum, mean::FLOAT8 as mean,
                   p50::TEXT as p50, p75::TEXT as p75, p90::TEXT as p90,
                   p95::TEXT as p95, p99::TEXT as p99, p999::TEXT as p999,
                   quality, samples_json, created_at
            FROM baseline_distributions
            WHERE baseline_run_id = $1
            ORDER BY event_type ASC, metric ASC
            "#,
        )
        .bind(baseline_run_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut dists = Vec::with_capacity(rows.len());
        for row in rows {
            dists.push(row_to_baseline_distribution(&row)?);
        }

        Ok(dists)
    }

    async fn query_distributions(
        &self,
        event_type: Option<EventType>,
        metric: Option<BaselineMetric>,
        limit: usize,
    ) -> Result<Vec<BaselineDistribution>, StorageError> {
        let limit_i64 = limit.clamp(1, 200) as i64;
        let event_type_str = event_type.as_ref().map(|et| {
            serde_json::to_string(et)
                .unwrap_or_default()
                .trim_matches('"')
                .to_string()
        });
        let metric_str = metric.as_ref().map(|m| {
            serde_json::to_string(m)
                .unwrap_or_default()
                .trim_matches('"')
                .to_string()
        });

        let rows = sqlx::query(
            r#"
            SELECT id, baseline_run_id, event_type, metric, unit, sample_count,
                   candidate_count, missing_count, coverage_ratio,
                   minimum::TEXT as minimum, maximum::TEXT as maximum, mean::FLOAT8 as mean,
                   p50::TEXT as p50, p75::TEXT as p75, p90::TEXT as p90,
                   p95::TEXT as p95, p99::TEXT as p99, p999::TEXT as p999,
                   quality, samples_json, created_at
            FROM baseline_distributions
            WHERE ($1::VARCHAR IS NULL OR event_type = $1)
              AND ($2::VARCHAR IS NULL OR metric = $2)
            ORDER BY created_at DESC
            LIMIT $3
            "#,
        )
        .bind(event_type_str)
        .bind(metric_str)
        .bind(limit_i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut dists = Vec::with_capacity(rows.len());
        for row in rows {
            dists.push(row_to_baseline_distribution(&row)?);
        }

        Ok(dists)
    }

    async fn save_event_rarity(
        &self,
        rarity: &EventRarityResult,
        impact_breakdown: Option<&ImpactBreakdown>,
    ) -> Result<(), StorageError> {
        let event_type_str = serde_json::to_string(&rarity.event_type)
            .map_err(StorageError::Serialization)?
            .trim_matches('"')
            .to_string();
        let metric_str = serde_json::to_string(&rarity.metric)
            .map_err(StorageError::Serialization)?
            .trim_matches('"')
            .to_string();
        let band_str = rarity.rarity_band.as_str();
        let mode_str = rarity.evaluation_mode.as_str();
        let impact_json = match impact_breakdown {
            Some(ib) => Some(serde_json::to_value(ib)?),
            None => None,
        };
        let impact_score = impact_breakdown.and_then(|ib| ib.total_score);

        sqlx::query(
            r#"
            INSERT INTO event_rarity (
                id, event_id, baseline_run_id, event_type, metric,
                value_numeric, value_text, percentile, rarity_band,
                population_size, tail_count, evaluation_mode,
                impact_score, impact_json, percentile_method, estimated, created_at
            )
            VALUES (
                $1, $2, $3, $4, $5,
                $6::NUMERIC, $7, $8, $9,
                $10, $11, $12,
                $13, $14, $15, $16, NOW()
            )
            ON CONFLICT (event_id, baseline_run_id, metric) DO UPDATE SET
                value_numeric = EXCLUDED.value_numeric,
                value_text = EXCLUDED.value_text,
                percentile = EXCLUDED.percentile,
                rarity_band = EXCLUDED.rarity_band,
                population_size = EXCLUDED.population_size,
                tail_count = EXCLUDED.tail_count,
                evaluation_mode = EXCLUDED.evaluation_mode,
                impact_score = EXCLUDED.impact_score,
                impact_json = EXCLUDED.impact_json,
                percentile_method = EXCLUDED.percentile_method,
                estimated = EXCLUDED.estimated
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(rarity.event_id)
        .bind(rarity.baseline_run_id)
        .bind(event_type_str)
        .bind(metric_str)
        .bind(rarity.value.to_numeric_string())
        .bind(rarity.value.to_string())
        .bind(rarity.percentile)
        .bind(band_str)
        .bind(u64_to_i64_checked(rarity.population_size)?)
        .bind(rarity.tail_count.map(u64_to_i64_checked).transpose()?)
        .bind(mode_str)
        .bind(impact_score)
        .bind(impact_json)
        .bind(rarity.percentile_method.map(|pm| pm.as_str()))
        .bind(rarity.estimated)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        Ok(())
    }

    async fn get_event_rarity(
        &self,
        event_id: Uuid,
        baseline_run_id: Option<Uuid>,
    ) -> Result<Vec<EventRarityResult>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT id, event_id, baseline_run_id, event_type, metric,
                   value_numeric::TEXT as value_numeric, value_text, percentile,
                   rarity_band, population_size, tail_count, evaluation_mode,
                   percentile_method, estimated
            FROM event_rarity
            WHERE event_id = $1
              AND ($2::UUID IS NULL OR baseline_run_id = $2)
            ORDER BY created_at ASC
            "#,
        )
        .bind(event_id)
        .bind(baseline_run_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            let event_type_str: String = row.get("event_type");
            let metric_str: String = row.get("metric");
            let event_type: EventType = serde_json::from_str(&format!("\"{event_type_str}\""))
                .unwrap_or(EventType::LargeTransfer);
            let metric: BaselineMetric = metric_str.parse().unwrap_or(BaselineMetric::ValueSats);

            let value_num_str: String = row.get("value_numeric");
            let value = MetricValue::from_str_and_metric(&value_num_str, metric);

            let band_str: String = row.get("rarity_band");
            let rarity_band = band_str.parse::<RarityBand>().unwrap_or(RarityBand::Common);
            let mode_str: String = row.get("evaluation_mode");
            let evaluation_mode = mode_str
                .parse::<EvaluationMode>()
                .unwrap_or(EvaluationMode::Retrospective);

            let pop_size_i64: i64 = row.get("population_size");
            let tail_count_i64: Option<i64> = row.try_get("tail_count").ok().flatten();
            let tail_count = tail_count_i64.map(i64_to_u64_checked).transpose()?;
            let method_str_opt: Option<String> = row.try_get("percentile_method").ok().flatten();
            let percentile_method: Option<PercentileMethod> =
                method_str_opt.and_then(|s| s.parse().ok());
            let estimated: bool = row.try_get("estimated").unwrap_or(false);

            results.push(EventRarityResult {
                event_id: row.get("event_id"),
                baseline_run_id: row.get("baseline_run_id"),
                event_type,
                metric,
                value,
                percentile: row.get("percentile"),
                rarity_band,
                population_size: i64_to_u64_checked(pop_size_i64)?,
                tail_count,
                evaluation_mode,
                percentile_method,
                estimated,
                baseline_quality: None,
            });
        }

        Ok(results)
    }

    async fn query_events_for_baseline(
        &self,
        event_type: Option<EventType>,
        start_height: u64,
        end_height: u64,
    ) -> Result<Vec<ChainEvent>, StorageError> {
        let start_i64 = u64_to_i64_checked(start_height)?;
        let end_i64 = u64_to_i64_checked(end_height)?;
        let event_type_str = event_type.as_ref().map(|et| {
            serde_json::to_string(et)
                .unwrap_or_default()
                .trim_matches('"')
                .to_string()
        });

        let rows = sqlx::query(
            r#"
            SELECT id, event_type, severity, confidence, title, description,
                   COALESCE(event_time, detected_at) as event_time,
                   COALESCE(first_observed_at, detected_at) as first_observed_at,
                   detected_at, block_height, block_hash, txid, metadata, source,
                   observation_mode, replay_job_id
            FROM chain_events
            WHERE block_height IS NOT NULL
              AND block_height >= $1
              AND block_height <= $2
              AND ($3::VARCHAR IS NULL OR event_type = $3)
            ORDER BY block_height ASC, detected_at ASC
            "#,
        )
        .bind(start_i64)
        .bind(end_i64)
        .bind(event_type_str)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let et_str: String = row.get("event_type");
            let sev_str: String = row.get("severity");
            let conf_str: String = row.get("confidence");

            let event_type: EventType =
                serde_json::from_str(&format!("\"{et_str}\"")).unwrap_or(EventType::LargeTransfer);
            let severity: EventSeverity =
                serde_json::from_str(&format!("\"{sev_str}\"")).unwrap_or(EventSeverity::Info);
            let confidence: ConfidenceLevel = serde_json::from_str(&format!("\"{conf_str}\""))
                .unwrap_or(ConfidenceLevel::Heuristic);

            let bh_i64: Option<i64> = row.get("block_height");
            let block_height = match bh_i64 {
                Some(h) => Some(i64_to_u64_checked(h)?),
                None => None,
            };

            let src_json: Option<serde_json::Value> = row.get("source");
            let source = match src_json {
                Some(v) => serde_json::from_value(v).ok(),
                None => None,
            };

            let obs_mode_str: Option<String> = row.try_get("observation_mode").ok();
            let observation_mode = obs_mode_str
                .map(|s| match s.to_lowercase().as_str() {
                    "historical_replay" => ObservationMode::HistoricalReplay,
                    _ => ObservationMode::Live,
                })
                .unwrap_or(ObservationMode::Live);
            let replay_job_id: Option<Uuid> = row.try_get("replay_job_id").ok().flatten();

            let detected_at: DateTime<Utc> = row.get("detected_at");
            let event_time: DateTime<Utc> = row.try_get("event_time").unwrap_or(detected_at);
            let first_observed_at: DateTime<Utc> =
                row.try_get("first_observed_at").unwrap_or(detected_at);

            events.push(ChainEvent {
                id: row.get("id"),
                event_type,
                severity,
                confidence,
                title: row.get("title"),
                description: row.get("description"),
                event_time,
                first_observed_at,
                detected_at,
                block_height,
                block_hash: row.get("block_hash"),
                txid: row.get("txid"),
                metadata: row.get("metadata"),
                witnesses: source
                    .clone()
                    .map(|s| vec![obschain_core::ObservationWitness::new(s)])
                    .unwrap_or_default(),
                source,
                observation_mode,
                replay_job_id,
                observations: Vec::new(),
            });
        }

        Ok(events)
    }

    async fn save_event_metrics(&self, metrics: &[EventMetricValue]) -> Result<(), StorageError> {
        for m in metrics {
            if m.value.scale() > obschain_core::MAX_SUPPORTED_DECIMAL_SCALE {
                return Err(StorageError::InvalidData(format!(
                    "Metric value scale {} exceeds maximum supported scale {}",
                    m.value.scale(),
                    obschain_core::MAX_SUPPORTED_DECIMAL_SCALE
                )));
            }
            let event_type_str = serde_json::to_string(&m.event_type)
                .map_err(StorageError::Serialization)?
                .trim_matches('"')
                .to_string();
            let metric_str = m.metric.as_str();
            let block_height_i64 = u64_to_i64_checked(m.block_height)?;

            sqlx::query(
                r#"
                INSERT INTO event_metric_values (
                    id, event_id, network, event_type, metric,
                    metric_definition_version, value_numeric, metric_scale,
                    block_height, event_time, created_at
                )
                VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7::NUMERIC, $8,
                    $9, $10, NOW()
                )
                ON CONFLICT (event_id, metric, metric_definition_version) DO UPDATE SET
                    value_numeric = EXCLUDED.value_numeric,
                    metric_scale = EXCLUDED.metric_scale,
                    block_height = EXCLUDED.block_height,
                    event_time = EXCLUDED.event_time,
                    network = EXCLUDED.network
                "#,
            )
            .bind(m.id)
            .bind(m.event_id)
            .bind(&m.network)
            .bind(event_type_str)
            .bind(metric_str)
            .bind(&m.metric_definition_version)
            .bind(m.value.to_numeric_string())
            .bind(m.metric_scale as i32)
            .bind(block_height_i64)
            .bind(m.event_time)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;
        }
        Ok(())
    }

    async fn get_event_metrics(
        &self,
        event_id: Uuid,
    ) -> Result<Vec<EventMetricValue>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT id, event_id, network, event_type, metric,
                   metric_definition_version, value_numeric::TEXT as value_numeric,
                   metric_scale, block_height, event_time, created_at
            FROM event_metric_values
            WHERE event_id = $1
            ORDER BY metric ASC
            "#,
        )
        .bind(event_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            let event_type_str: String = row.get("event_type");
            let metric_str: String = row.get("metric");
            let event_type: EventType = serde_json::from_str(&format!("\"{event_type_str}\""))
                .unwrap_or(EventType::LargeTransfer);
            let metric: BaselineMetric = metric_str.parse().unwrap_or(BaselineMetric::ValueSats);
            let scale: i32 = row.get("metric_scale");
            let val_num_str: String = row.get("value_numeric");
            let value = MetricValue::from_str_scale_and_metric(&val_num_str, scale as u32, metric);
            let bh_i64: i64 = row.get("block_height");

            results.push(EventMetricValue {
                id: row.get("id"),
                event_id: row.get("event_id"),
                network: row.get("network"),
                event_type,
                metric,
                metric_definition_version: row.get("metric_definition_version"),
                value,
                metric_scale: scale as u32,
                block_height: i64_to_u64_checked(bh_i64)?,
                event_time: row.get("event_time"),
                created_at: row.get("created_at"),
            });
        }

        Ok(results)
    }

    #[allow(clippy::too_many_arguments)]
    async fn get_exact_empirical_rank(
        &self,
        network: &str,
        event_type: EventType,
        metric: BaselineMetric,
        metric_definition_version: &str,
        start_height: u64,
        end_height: u64,
        query_value: &MetricValue,
        direction: RarityDirection,
    ) -> Result<Option<(f64, u64, u64)>, StorageError> {
        let start_i64 = u64_to_i64_checked(start_height)?;
        let end_i64 = u64_to_i64_checked(end_height)?;
        let event_type_str = serde_json::to_string(&event_type)
            .map_err(StorageError::Serialization)?
            .trim_matches('"')
            .to_string();
        let metric_str = metric.as_str();

        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE value_numeric <= $7::NUMERIC) as count_le,
                COUNT(*) FILTER (WHERE value_numeric >= $7::NUMERIC) as count_ge,
                COUNT(*) as total_count
            FROM event_metric_values
            WHERE network = $1
              AND event_type = $2
              AND metric = $3
              AND metric_definition_version = $4
              AND block_height >= $5
              AND block_height <= $6
            "#,
        )
        .bind(network)
        .bind(event_type_str)
        .bind(metric_str)
        .bind(metric_definition_version)
        .bind(start_i64)
        .bind(end_i64)
        .bind(query_value.to_numeric_string())
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let total_count: i64 = row.get("total_count");
        if total_count == 0 {
            return Ok(None);
        }

        let count_le: i64 = row.get("count_le");
        let count_ge: i64 = row.get("count_ge");
        let n = total_count as u64;

        match direction {
            RarityDirection::HigherIsRarer | RarityDirection::TwoSided => {
                let tail_count = count_ge as u64;
                let percentile = (count_le as f64 / total_count as f64) * 100.0;
                Ok(Some((percentile, tail_count, n)))
            }
            RarityDirection::LowerIsRarer => {
                let tail_count = count_le as u64;
                let percentile = (count_ge as f64 / total_count as f64) * 100.0;
                Ok(Some((percentile, tail_count, n)))
            }
        }
    }
}

fn row_to_baseline_run(row: &sqlx::postgres::PgRow) -> Result<BaselineRun, StorageError> {
    let start_height: i64 = row.get("start_height");
    let end_height: i64 = row.get("end_height");
    let status_str: String = row.get("status");
    let status = status_str
        .parse::<BaselineRunStatus>()
        .unwrap_or(BaselineRunStatus::Pending);
    let event_count: i64 = row.get("canonical_event_count");

    Ok(BaselineRun {
        id: row.get("id"),
        network: row.get("network"),
        start_height: i64_to_u64_checked(start_height)?,
        end_height: i64_to_u64_checked(end_height)?,
        started_at: row.get("started_at"),
        completed_at: row.get("completed_at"),
        status,
        algorithm_version: row.get("algorithm_version"),
        canonical_event_count: i64_to_u64_checked(event_count)?,
        error_message: row.get("error_message"),
        metadata: row.get("metadata"),
        created_at: row.get("created_at"),
    })
}

fn row_to_baseline_distribution(
    row: &sqlx::postgres::PgRow,
) -> Result<BaselineDistribution, StorageError> {
    let event_type_str: String = row.get("event_type");
    let metric_str: String = row.get("metric");
    let unit_str: String = row.get("unit");
    let quality_str: String = row.get("quality");

    let event_type: EventType =
        serde_json::from_str(&format!("\"{event_type_str}\"")).unwrap_or(EventType::LargeTransfer);
    let metric: BaselineMetric = metric_str.parse().unwrap_or(BaselineMetric::ValueSats);
    let unit = unit_str.parse().unwrap_or(MetricUnit::Satoshis);
    let quality = quality_str.parse().unwrap_or(BaselineQuality::Insufficient);

    let sample_count: i64 = row.get("sample_count");
    let candidate_count: i64 = row.get("candidate_count");
    let missing_count: i64 = row.get("missing_count");
    let coverage_ratio: f64 = row.get("coverage_ratio");
    let mean: f64 = row.get("mean");

    let min_str: String = row.get("minimum");
    let max_str: String = row.get("maximum");
    let p50_str: String = row.get("p50");
    let p75_str: String = row.get("p75");
    let p90_str: String = row.get("p90");
    let p95_str: String = row.get("p95");
    let p99_str: String = row.get("p99");
    let p999_str: String = row.get("p999");

    let minimum = MetricValue::from_str_and_metric(&min_str, metric);
    let maximum = MetricValue::from_str_and_metric(&max_str, metric);
    let p50 = MetricValue::from_str_and_metric(&p50_str, metric);
    let p75 = MetricValue::from_str_and_metric(&p75_str, metric);
    let p90 = MetricValue::from_str_and_metric(&p90_str, metric);
    let p95 = MetricValue::from_str_and_metric(&p95_str, metric);
    let p99 = MetricValue::from_str_and_metric(&p99_str, metric);
    let p999 = MetricValue::from_str_and_metric(&p999_str, metric);

    let samples_json: Option<serde_json::Value> = row.get("samples_json");

    Ok(BaselineDistribution {
        id: row.get("id"),
        baseline_run_id: row.get("baseline_run_id"),
        event_type,
        metric,
        unit,
        sample_count: i64_to_u64_checked(sample_count)?,
        candidate_count: i64_to_u64_checked(candidate_count)?,
        missing_count: i64_to_u64_checked(missing_count)?,
        coverage_ratio,
        minimum,
        maximum,
        mean,
        p50,
        p75,
        p90,
        p95,
        p99,
        p999,
        quality,
        samples_json,
        created_at: row.get("created_at"),
    })
}
