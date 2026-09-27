pub mod postgres;
pub mod repository;

pub use postgres::PostgresStorage;
pub use repository::{
    i64_to_u64_checked, u64_to_i64_checked, BaselineRepository, EventFilter, EventRepository,
    InMemoryStorage, IncidentActivityRepository, IncidentAlertRepository, IncidentRepository,
    ReplayRepository, StorageError, WatchTargetRepository,
};

use chrono::{DateTime, Utc};
use obschain_core::baseline::{
    BaselineDistribution, BaselineMetric, BaselineRun, BaselineRunStatus, EventRarityResult,
    ImpactBreakdown,
};
use obschain_core::{
    ActivityStatus, ChainEvent, EventObservation, EventType, Incident, IncidentActivity,
    IncidentAlert, ReplayCheckpoint, ReplayJob, WatchTarget,
};
use uuid::Uuid;

/// Unified storage enum supporting both in-memory and PostgreSQL persistent backends.
#[derive(Clone)]
pub enum Storage {
    Memory(InMemoryStorage),
    Postgres(PostgresStorage),
}

impl From<InMemoryStorage> for Storage {
    fn from(s: InMemoryStorage) -> Self {
        Self::Memory(s)
    }
}

impl From<PostgresStorage> for Storage {
    fn from(s: PostgresStorage) -> Self {
        Self::Postgres(s)
    }
}

impl Storage {
    pub fn backend_name(&self) -> &'static str {
        match self {
            Self::Memory(_) => "memory",
            Self::Postgres(_) => "postgres",
        }
    }

    pub fn is_memory(&self) -> bool {
        matches!(self, Self::Memory(_))
    }

    pub fn is_postgres(&self) -> bool {
        matches!(self, Self::Postgres(_))
    }

    pub fn as_memory(&self) -> Option<&InMemoryStorage> {
        match self {
            Self::Memory(ref m) => Some(m),
            Self::Postgres(_) => None,
        }
    }

    pub fn as_postgres(&self) -> Option<&PostgresStorage> {
        match self {
            Self::Postgres(ref p) => Some(p),
            Self::Memory(_) => None,
        }
    }

    /// Read cheap cached counts without issuing COUNT(*) queries.
    pub fn telemetry_counts(&self) -> (usize, usize, usize, usize) {
        match self {
            Self::Memory(m) => (
                m.event_count(),
                1, // Canonical Liquid incident
                0,
                m.watch_target_count(),
            ),
            Self::Postgres(p) => p.telemetry_counts(),
        }
    }

    pub async fn count_events(&self) -> usize {
        match self {
            Self::Memory(m) => m.event_count(),
            Self::Postgres(p) => p.count_events().await.unwrap_or(0),
        }
    }

    pub async fn count_incidents(&self) -> usize {
        match self {
            Self::Memory(m) => m
                .list_incidents(1000, 0)
                .await
                .map(|v| v.len())
                .unwrap_or(0),
            Self::Postgres(p) => p.count_incidents().await.unwrap_or(0),
        }
    }

    pub async fn count_watch_targets(&self) -> usize {
        match self {
            Self::Memory(m) => m.watch_target_count(),
            Self::Postgres(p) => p.count_watch_targets().await.unwrap_or(0),
        }
    }

    pub async fn count_activities(&self) -> usize {
        match self {
            Self::Memory(m) => m.activity_count(),
            Self::Postgres(p) => p.count_activities().await.unwrap_or(0),
        }
    }

    pub async fn count_alerts(&self) -> usize {
        match self {
            Self::Memory(m) => m.alert_count(),
            Self::Postgres(p) => p.count_alerts().await.unwrap_or(0),
        }
    }

    pub async fn count_replay_jobs(&self) -> usize {
        match self {
            Self::Memory(m) => m.replay_job_count(),
            Self::Postgres(p) => p.count_replay_jobs().await.unwrap_or(0),
        }
    }

    pub async fn count_event_observations(&self, event_id: Option<Uuid>) -> usize {
        match self {
            Self::Memory(m) => m.count_event_observations(event_id).await.unwrap_or(0),
            Self::Postgres(p) => p.count_event_observations(event_id).await.unwrap_or(0),
        }
    }
}

#[async_trait::async_trait]
impl EventRepository for Storage {
    async fn save_event(&self, event: &ChainEvent) -> Result<(), StorageError> {
        match self {
            Self::Memory(m) => m.save_event(event).await,
            Self::Postgres(p) => p.save_event(event).await,
        }
    }

    async fn list_events(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ChainEvent>, StorageError> {
        match self {
            Self::Memory(m) => m.list_events(limit, offset).await,
            Self::Postgres(p) => p.list_events(limit, offset).await,
        }
    }

    async fn get_event_by_id(&self, id: Uuid) -> Result<Option<ChainEvent>, StorageError> {
        match self {
            Self::Memory(m) => m.get_event_by_id(id).await,
            Self::Postgres(p) => p.get_event_by_id(id).await,
        }
    }

    async fn query_events(&self, filter: &EventFilter) -> Result<Vec<ChainEvent>, StorageError> {
        match self {
            Self::Memory(m) => m.query_events(filter).await,
            Self::Postgres(p) => p.query_events(filter).await,
        }
    }

    async fn save_event_observation(
        &self,
        observation: &EventObservation,
    ) -> Result<(), StorageError> {
        match self {
            Self::Memory(m) => m.save_event_observation(observation).await,
            Self::Postgres(p) => p.save_event_observation(observation).await,
        }
    }

    async fn list_event_observations(
        &self,
        event_id: Uuid,
    ) -> Result<Vec<EventObservation>, StorageError> {
        match self {
            Self::Memory(m) => m.list_event_observations(event_id).await,
            Self::Postgres(p) => p.list_event_observations(event_id).await,
        }
    }

    async fn count_event_observations(
        &self,
        event_id: Option<Uuid>,
    ) -> Result<usize, StorageError> {
        match self {
            Self::Memory(m) => m.count_event_observations(event_id).await,
            Self::Postgres(p) => p.count_event_observations(event_id).await,
        }
    }
}

#[async_trait::async_trait]
impl IncidentRepository for Storage {
    async fn save_incident(&self, incident: &Incident) -> Result<(), StorageError> {
        match self {
            Self::Memory(m) => m.save_incident(incident).await,
            Self::Postgres(p) => p.save_incident(incident).await,
        }
    }

    async fn list_incidents(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Incident>, StorageError> {
        match self {
            Self::Memory(m) => m.list_incidents(limit, offset).await,
            Self::Postgres(p) => p.list_incidents(limit, offset).await,
        }
    }

    async fn get_incident_by_id(&self, id: Uuid) -> Result<Option<Incident>, StorageError> {
        match self {
            Self::Memory(m) => m.get_incident_by_id(id).await,
            Self::Postgres(p) => p.get_incident_by_id(id).await,
        }
    }

    async fn get_incident_by_id_or_case_id(
        &self,
        identifier: &str,
    ) -> Result<Option<Incident>, StorageError> {
        match self {
            Self::Memory(m) => m.get_incident_by_id_or_case_id(identifier).await,
            Self::Postgres(p) => p.get_incident_by_id_or_case_id(identifier).await,
        }
    }
}

#[async_trait::async_trait]
impl WatchTargetRepository for Storage {
    async fn save_watch_target(&self, target: &WatchTarget) -> Result<(), StorageError> {
        match self {
            Self::Memory(m) => m.save_watch_target(target).await,
            Self::Postgres(p) => p.save_watch_target(target).await,
        }
    }

    async fn list_watch_targets(
        &self,
        incident_id: Option<Uuid>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<WatchTarget>, StorageError> {
        match self {
            Self::Memory(m) => m.list_watch_targets(incident_id, limit, offset).await,
            Self::Postgres(p) => p.list_watch_targets(incident_id, limit, offset).await,
        }
    }

    async fn get_watch_target_by_id(&self, id: Uuid) -> Result<Option<WatchTarget>, StorageError> {
        match self {
            Self::Memory(m) => m.get_watch_target_by_id(id).await,
            Self::Postgres(p) => p.get_watch_target_by_id(id).await,
        }
    }
}

#[async_trait::async_trait]
impl IncidentActivityRepository for Storage {
    async fn save_activity(&self, activity: &IncidentActivity) -> Result<(), StorageError> {
        match self {
            Self::Memory(m) => m.save_activity(activity).await,
            Self::Postgres(p) => p.save_activity(activity).await,
        }
    }

    async fn list_activities(
        &self,
        incident_id: Option<Uuid>,
        status: Option<ActivityStatus>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<IncidentActivity>, StorageError> {
        match self {
            Self::Memory(m) => m.list_activities(incident_id, status, limit, offset).await,
            Self::Postgres(p) => p.list_activities(incident_id, status, limit, offset).await,
        }
    }

    async fn get_activity_by_id(&self, id: Uuid) -> Result<Option<IncidentActivity>, StorageError> {
        match self {
            Self::Memory(m) => m.get_activity_by_id(id).await,
            Self::Postgres(p) => p.get_activity_by_id(id).await,
        }
    }

    async fn get_activity_by_dedup_key(
        &self,
        dedup_key: &str,
    ) -> Result<Option<IncidentActivity>, StorageError> {
        match self {
            Self::Memory(m) => m.get_activity_by_dedup_key(dedup_key).await,
            Self::Postgres(p) => p.get_activity_by_dedup_key(dedup_key).await,
        }
    }
}

#[async_trait::async_trait]
impl IncidentAlertRepository for Storage {
    async fn save_alert(&self, alert: &IncidentAlert) -> Result<(), StorageError> {
        match self {
            Self::Memory(m) => m.save_alert(alert).await,
            Self::Postgres(p) => p.save_alert(alert).await,
        }
    }

    async fn list_alerts(
        &self,
        incident_id: Option<Uuid>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<IncidentAlert>, StorageError> {
        match self {
            Self::Memory(m) => m.list_alerts(incident_id, limit, offset).await,
            Self::Postgres(p) => p.list_alerts(incident_id, limit, offset).await,
        }
    }

    async fn get_alert_by_id(&self, id: Uuid) -> Result<Option<IncidentAlert>, StorageError> {
        match self {
            Self::Memory(m) => m.get_alert_by_id(id).await,
            Self::Postgres(p) => p.get_alert_by_id(id).await,
        }
    }
}

#[async_trait::async_trait]
impl ReplayRepository for Storage {
    async fn create_job(&self, job: &ReplayJob) -> Result<(), StorageError> {
        match self {
            Self::Memory(m) => m.create_job(job).await,
            Self::Postgres(p) => p.create_job(job).await,
        }
    }

    async fn get_job(&self, id: Uuid) -> Result<Option<ReplayJob>, StorageError> {
        match self {
            Self::Memory(m) => m.get_job(id).await,
            Self::Postgres(p) => p.get_job(id).await,
        }
    }

    async fn update_job(&self, job: &ReplayJob) -> Result<(), StorageError> {
        match self {
            Self::Memory(m) => m.update_job(job).await,
            Self::Postgres(p) => p.update_job(job).await,
        }
    }

    async fn list_jobs(&self, limit: usize, offset: usize) -> Result<Vec<ReplayJob>, StorageError> {
        match self {
            Self::Memory(m) => m.list_jobs(limit, offset).await,
            Self::Postgres(p) => p.list_jobs(limit, offset).await,
        }
    }

    async fn save_checkpoint(&self, checkpoint: &ReplayCheckpoint) -> Result<(), StorageError> {
        match self {
            Self::Memory(m) => m.save_checkpoint(checkpoint).await,
            Self::Postgres(p) => p.save_checkpoint(checkpoint).await,
        }
    }

    async fn get_latest_checkpoint(
        &self,
        job_id: Uuid,
    ) -> Result<Option<ReplayCheckpoint>, StorageError> {
        match self {
            Self::Memory(m) => m.get_latest_checkpoint(job_id).await,
            Self::Postgres(p) => p.get_latest_checkpoint(job_id).await,
        }
    }
}

#[async_trait::async_trait]
impl BaselineRepository for Storage {
    async fn create_baseline_run(&self, run: &BaselineRun) -> Result<(), StorageError> {
        match self {
            Self::Memory(m) => m.create_baseline_run(run).await,
            Self::Postgres(p) => p.create_baseline_run(run).await,
        }
    }

    async fn update_baseline_run_status(
        &self,
        id: Uuid,
        status: BaselineRunStatus,
        completed_at: Option<DateTime<Utc>>,
        canonical_event_count: u64,
        error_message: Option<String>,
    ) -> Result<(), StorageError> {
        match self {
            Self::Memory(m) => {
                m.update_baseline_run_status(
                    id,
                    status,
                    completed_at,
                    canonical_event_count,
                    error_message,
                )
                .await
            }
            Self::Postgres(p) => {
                p.update_baseline_run_status(
                    id,
                    status,
                    completed_at,
                    canonical_event_count,
                    error_message,
                )
                .await
            }
        }
    }

    async fn get_baseline_run(&self, id: Uuid) -> Result<Option<BaselineRun>, StorageError> {
        match self {
            Self::Memory(m) => m.get_baseline_run(id).await,
            Self::Postgres(p) => p.get_baseline_run(id).await,
        }
    }

    async fn list_baseline_runs(
        &self,
        network: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<BaselineRun>, StorageError> {
        match self {
            Self::Memory(m) => m.list_baseline_runs(network, limit, offset).await,
            Self::Postgres(p) => p.list_baseline_runs(network, limit, offset).await,
        }
    }

    async fn get_latest_compatible_baseline_run(
        &self,
        network: &str,
        height: Option<u64>,
        version: Option<&str>,
    ) -> Result<Option<BaselineRun>, StorageError> {
        match self {
            Self::Memory(m) => {
                m.get_latest_compatible_baseline_run(network, height, version)
                    .await
            }
            Self::Postgres(p) => {
                p.get_latest_compatible_baseline_run(network, height, version)
                    .await
            }
        }
    }

    async fn save_baseline_distributions(
        &self,
        distributions: &[BaselineDistribution],
    ) -> Result<(), StorageError> {
        match self {
            Self::Memory(m) => m.save_baseline_distributions(distributions).await,
            Self::Postgres(p) => p.save_baseline_distributions(distributions).await,
        }
    }

    async fn get_baseline_distributions(
        &self,
        baseline_run_id: Uuid,
    ) -> Result<Vec<BaselineDistribution>, StorageError> {
        match self {
            Self::Memory(m) => m.get_baseline_distributions(baseline_run_id).await,
            Self::Postgres(p) => p.get_baseline_distributions(baseline_run_id).await,
        }
    }

    async fn query_distributions(
        &self,
        event_type: Option<EventType>,
        metric: Option<BaselineMetric>,
        limit: usize,
    ) -> Result<Vec<BaselineDistribution>, StorageError> {
        match self {
            Self::Memory(m) => m.query_distributions(event_type, metric, limit).await,
            Self::Postgres(p) => p.query_distributions(event_type, metric, limit).await,
        }
    }

    async fn save_event_rarity(
        &self,
        rarity: &EventRarityResult,
        impact_breakdown: Option<&ImpactBreakdown>,
    ) -> Result<(), StorageError> {
        match self {
            Self::Memory(m) => m.save_event_rarity(rarity, impact_breakdown).await,
            Self::Postgres(p) => p.save_event_rarity(rarity, impact_breakdown).await,
        }
    }

    async fn get_event_rarity(
        &self,
        event_id: Uuid,
        baseline_run_id: Option<Uuid>,
    ) -> Result<Vec<EventRarityResult>, StorageError> {
        match self {
            Self::Memory(m) => m.get_event_rarity(event_id, baseline_run_id).await,
            Self::Postgres(p) => p.get_event_rarity(event_id, baseline_run_id).await,
        }
    }

    async fn query_events_for_baseline(
        &self,
        event_type: Option<EventType>,
        start_height: u64,
        end_height: u64,
    ) -> Result<Vec<ChainEvent>, StorageError> {
        match self {
            Self::Memory(m) => {
                m.query_events_for_baseline(event_type, start_height, end_height)
                    .await
            }
            Self::Postgres(p) => {
                p.query_events_for_baseline(event_type, start_height, end_height)
                    .await
            }
        }
    }
}
