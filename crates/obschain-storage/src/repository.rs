use std::{
    collections::VecDeque,
    sync::{Arc, RwLock},
};

use chrono::Utc;
use obschain_core::{
    ActivityStatus, ChainEvent, ConfidenceLevel, EventSeverity, EventType, Incident,
    IncidentActivity, IncidentAlert, ObservationMode, ReplayCheckpoint, ReplayJob, WatchTarget,
};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Database error: {0}")]
    Database(String),

    #[error("Entity not found: {0}")]
    NotFound(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

/// Safely convert Rust u64 satoshis / block heights to PostgreSQL signed BIGINT (i64).
pub fn u64_to_i64_checked(val: u64) -> Result<i64, StorageError> {
    i64::try_from(val).map_err(|_| {
        StorageError::Database(format!("Value {val} exceeds signed 64-bit integer limit"))
    })
}

/// Safely convert PostgreSQL signed BIGINT (i64) back to Rust u64.
pub fn i64_to_u64_checked(val: i64) -> Result<u64, StorageError> {
    u64::try_from(val).map_err(|_| {
        StorageError::Database(format!("Negative integer {val} cannot convert to u64"))
    })
}

/// Structured filter parameters for querying historical and live chain events.
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct EventFilter {
    pub from_height: Option<u64>,
    pub to_height: Option<u64>,
    pub from_time: Option<chrono::DateTime<Utc>>,
    pub to_time: Option<chrono::DateTime<Utc>>,
    pub event_type: Option<EventType>,
    pub severity: Option<EventSeverity>,
    pub observation_mode: Option<ObservationMode>,
    pub replay_job_id: Option<Uuid>,
    pub txid: Option<String>,
    pub block_hash: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[async_trait::async_trait]
pub trait EventRepository: Send + Sync {
    async fn save_event(&self, event: &ChainEvent) -> Result<(), StorageError>;
    async fn list_events(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ChainEvent>, StorageError>;
    async fn get_event_by_id(&self, id: Uuid) -> Result<Option<ChainEvent>, StorageError>;
    async fn query_events(&self, filter: &EventFilter) -> Result<Vec<ChainEvent>, StorageError>;
}

#[async_trait::async_trait]
pub trait ReplayRepository: Send + Sync {
    async fn create_job(&self, job: &ReplayJob) -> Result<(), StorageError>;
    async fn get_job(&self, id: Uuid) -> Result<Option<ReplayJob>, StorageError>;
    async fn update_job(&self, job: &ReplayJob) -> Result<(), StorageError>;
    async fn list_jobs(&self, limit: usize, offset: usize) -> Result<Vec<ReplayJob>, StorageError>;
    async fn save_checkpoint(&self, checkpoint: &ReplayCheckpoint) -> Result<(), StorageError>;
    async fn get_latest_checkpoint(
        &self,
        job_id: Uuid,
    ) -> Result<Option<ReplayCheckpoint>, StorageError>;
}

#[async_trait::async_trait]
pub trait IncidentRepository: Send + Sync {
    async fn save_incident(&self, incident: &Incident) -> Result<(), StorageError>;
    async fn list_incidents(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Incident>, StorageError>;
    async fn get_incident_by_id(&self, id: Uuid) -> Result<Option<Incident>, StorageError>;
    async fn get_incident_by_id_or_case_id(
        &self,
        identifier: &str,
    ) -> Result<Option<Incident>, StorageError>;
}

#[async_trait::async_trait]
pub trait WatchTargetRepository: Send + Sync {
    async fn save_watch_target(&self, target: &WatchTarget) -> Result<(), StorageError>;
    async fn list_watch_targets(
        &self,
        incident_id: Option<Uuid>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<WatchTarget>, StorageError>;
    async fn get_watch_target_by_id(&self, id: Uuid) -> Result<Option<WatchTarget>, StorageError>;
}

#[async_trait::async_trait]
pub trait IncidentActivityRepository: Send + Sync {
    async fn save_activity(&self, activity: &IncidentActivity) -> Result<(), StorageError>;
    async fn list_activities(
        &self,
        incident_id: Option<Uuid>,
        status: Option<ActivityStatus>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<IncidentActivity>, StorageError>;
    async fn get_activity_by_id(&self, id: Uuid) -> Result<Option<IncidentActivity>, StorageError>;
    async fn get_activity_by_dedup_key(
        &self,
        dedup_key: &str,
    ) -> Result<Option<IncidentActivity>, StorageError>;
}

#[async_trait::async_trait]
pub trait IncidentAlertRepository: Send + Sync {
    async fn save_alert(&self, alert: &IncidentAlert) -> Result<(), StorageError>;
    async fn list_alerts(
        &self,
        incident_id: Option<Uuid>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<IncidentAlert>, StorageError>;
    async fn get_alert_by_id(&self, id: Uuid) -> Result<Option<IncidentAlert>, StorageError>;
}

/// Thread-safe in-memory event, incident, watch target, and activity store with bounded retention.
/// Uses circular VecDeques to bound maximum memory consumption.
#[derive(Clone)]
pub struct InMemoryStorage {
    events: Arc<RwLock<VecDeque<ChainEvent>>>,
    incidents: Arc<RwLock<Vec<Incident>>>,
    watch_targets: Arc<RwLock<Vec<WatchTarget>>>,
    activities: Arc<RwLock<VecDeque<IncidentActivity>>>,
    alerts: Arc<RwLock<VecDeque<IncidentAlert>>>,
    replay_jobs: Arc<RwLock<Vec<ReplayJob>>>,
    replay_checkpoints: Arc<RwLock<Vec<ReplayCheckpoint>>>,
    max_events: usize,
    max_activities: usize,
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStorage {
    pub const DEFAULT_MAX_EVENTS: usize = 10_000;
    pub const DEFAULT_MAX_ACTIVITIES: usize = 5_000;

    pub fn new() -> Self {
        Self::with_limits(Self::DEFAULT_MAX_EVENTS, Self::DEFAULT_MAX_ACTIVITIES)
    }

    pub fn with_limit(max_events: usize) -> Self {
        Self::with_limits(max_events, Self::DEFAULT_MAX_ACTIVITIES)
    }

    pub fn with_limits(max_events: usize, max_activities: usize) -> Self {
        let storage = Self {
            events: Arc::new(RwLock::new(VecDeque::with_capacity(max_events.min(1000)))),
            incidents: Arc::new(RwLock::new(Vec::new())),
            watch_targets: Arc::new(RwLock::new(Vec::new())),
            activities: Arc::new(RwLock::new(VecDeque::with_capacity(
                max_activities.min(1000),
            ))),
            alerts: Arc::new(RwLock::new(VecDeque::with_capacity(
                max_activities.min(1000),
            ))),
            replay_jobs: Arc::new(RwLock::new(Vec::new())),
            replay_checkpoints: Arc::new(RwLock::new(Vec::new())),
            max_events,
            max_activities,
        };
        storage.seed_canonical_incidents();
        storage.seed_mock_data();
        storage
    }

    pub fn new_empty(max_events: usize) -> Self {
        let storage = Self {
            events: Arc::new(RwLock::new(VecDeque::with_capacity(max_events.min(1000)))),
            incidents: Arc::new(RwLock::new(Vec::new())),
            watch_targets: Arc::new(RwLock::new(Vec::new())),
            activities: Arc::new(RwLock::new(VecDeque::with_capacity(
                Self::DEFAULT_MAX_ACTIVITIES.min(1000),
            ))),
            alerts: Arc::new(RwLock::new(VecDeque::with_capacity(
                Self::DEFAULT_MAX_ACTIVITIES.min(1000),
            ))),
            replay_jobs: Arc::new(RwLock::new(Vec::new())),
            replay_checkpoints: Arc::new(RwLock::new(Vec::new())),
            max_events,
            max_activities: Self::DEFAULT_MAX_ACTIVITIES,
        };
        storage.seed_canonical_incidents();
        storage
    }

    pub fn seed_canonical_incidents(&self) {
        let liquid_incident = obschain_incidents::create_liquid_2026_incident();
        if let Ok(mut lock) = self.incidents.write() {
            if !lock.iter().any(|i| i.case_id == liquid_incident.case_id) {
                lock.push(liquid_incident);
            }
        }

        let targets = obschain_incidents::canonical_liquid_watch_targets();
        if let Ok(mut lock) = self.watch_targets.write() {
            for target in targets {
                if !lock.iter().any(|t| {
                    t.id == target.id
                        || (t.incident_id == target.incident_id && t.kind == target.kind)
                }) {
                    lock.push(target);
                }
            }
        }
    }

    pub fn event_count(&self) -> usize {
        self.events.read().map(|l| l.len()).unwrap_or(0)
    }

    pub fn activity_count(&self) -> usize {
        self.activities.read().map(|l| l.len()).unwrap_or(0)
    }

    pub fn replay_job_count(&self) -> usize {
        self.replay_jobs.read().map(|l| l.len()).unwrap_or(0)
    }

    pub fn watch_target_count(&self) -> usize {
        self.watch_targets.read().map(|l| l.len()).unwrap_or(0)
    }

    pub fn clear_events(&self) {
        if let Ok(mut lock) = self.events.write() {
            lock.clear();
        }
    }

    pub fn clear_activities(&self) {
        if let Ok(mut lock) = self.activities.write() {
            lock.clear();
        }
    }

    pub fn alert_count(&self) -> usize {
        self.alerts.read().map(|l| l.len()).unwrap_or(0)
    }

    pub fn clear_alerts(&self) {
        if let Ok(mut lock) = self.alerts.write() {
            lock.clear();
        }
    }

    fn seed_mock_data(&self) {
        let now = Utc::now();

        // Sample Event 1: Whale Transfer
        let mut ev1 = ChainEvent::new(
            EventType::LargeTransfer,
            EventSeverity::High,
            ConfidenceLevel::VerifiedOnChain,
            "Large Transfer: 2,450.00 BTC",
            "Observed transfer of 2,450.00 BTC across 2 outputs in block 884920.",
        );
        ev1.id = Uuid::parse_str("a0000000-0000-0000-0000-000000000001").unwrap();
        ev1.txid =
            Some("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b".to_string());
        ev1.block_height = Some(884920);
        ev1.block_hash =
            Some("000000000000000000021b34e56997427ce090ef6d38e2195ec4188b449102c1".to_string());
        ev1.detected_at = now - chrono::Duration::minutes(15);
        ev1.metadata = serde_json::json!({
            "is_mock": true,
            "btc_volume": 2450.0,
            "fee_sats": 3410,
            "fee_rate_sat_vb": 16.2
        });

        // Sample Event 2: Long Block Interval
        let mut ev2 = ChainEvent::new(
            EventType::LongBlockInterval,
            EventSeverity::Medium,
            ConfidenceLevel::VerifiedOnChain,
            "Long Block Interval: 74 min at height 884918",
            "Block interval of 4,440 seconds observed between heights 884917 and 884918.",
        );
        ev2.id = Uuid::parse_str("a0000000-0000-0000-0000-000000000002").unwrap();
        ev2.block_height = Some(884918);
        ev2.block_hash =
            Some("00000000000000000001f37e42d87e07663f73367809bfccb34ba85df1176b66".to_string());
        ev2.detected_at = now - chrono::Duration::hours(2);
        ev2.metadata = serde_json::json!({
            "is_mock": true,
            "interval_seconds": 4440,
            "expected_interval_seconds": 600,
            "miner_tag": "AntPool"
        });

        // Sample Event 3: Extreme Fee
        let mut ev3 = ChainEvent::new(
            EventType::ExtremeFee,
            EventSeverity::Low,
            ConfidenceLevel::VerifiedOnChain,
            "Mempool Fee Spike: Median > 85 sat/vB",
            "Mempool congestion spike observed following rapid sequential transactions.",
        );
        ev3.id = Uuid::parse_str("a0000000-0000-0000-0000-000000000003").unwrap();
        ev3.detected_at = now - chrono::Duration::hours(5);
        ev3.metadata = serde_json::json!({
            "is_mock": true,
            "median_fee_rate": 85.4,
            "mempool_vsize_mb": 142.5
        });

        if let Ok(mut lock) = self.events.write() {
            lock.push_back(ev1);
            lock.push_back(ev2);
            lock.push_back(ev3);
        }
    }
}

#[async_trait::async_trait]
impl EventRepository for InMemoryStorage {
    async fn save_event(&self, event: &ChainEvent) -> Result<(), StorageError> {
        let mut lock = self
            .events
            .write()
            .map_err(|e| StorageError::Database(e.to_string()))?;

        // Idempotent update if event already exists
        if let Some(pos) = lock.iter().position(|e| e.id == event.id) {
            lock[pos] = event.clone();
            return Ok(());
        }

        // Enforce bounded memory retention
        if lock.len() >= self.max_events {
            lock.pop_front();
        }

        lock.push_back(event.clone());
        Ok(())
    }

    async fn list_events(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ChainEvent>, StorageError> {
        let lock = self
            .events
            .read()
            .map_err(|e| StorageError::Database(e.to_string()))?;
        let events = lock
            .iter()
            .rev()
            .skip(offset)
            .take(limit)
            .cloned()
            .collect();
        Ok(events)
    }

    async fn get_event_by_id(&self, id: Uuid) -> Result<Option<ChainEvent>, StorageError> {
        let lock = self
            .events
            .read()
            .map_err(|e| StorageError::Database(e.to_string()))?;
        Ok(lock.iter().find(|e| e.id == id).cloned())
    }

    async fn query_events(&self, filter: &EventFilter) -> Result<Vec<ChainEvent>, StorageError> {
        let lock = self
            .events
            .read()
            .map_err(|e| StorageError::Database(e.to_string()))?;

        let limit = filter.limit.unwrap_or(100).clamp(1, 1000);
        let offset = filter.offset.unwrap_or(0);

        let filtered = lock
            .iter()
            .rev()
            .filter(|e| {
                if let Some(from_h) = filter.from_height {
                    if e.block_height.map(|h| h < from_h).unwrap_or(true) {
                        return false;
                    }
                }
                if let Some(to_h) = filter.to_height {
                    if e.block_height.map(|h| h > to_h).unwrap_or(true) {
                        return false;
                    }
                }
                if let Some(from_t) = filter.from_time {
                    if e.detected_at < from_t {
                        return false;
                    }
                }
                if let Some(to_t) = filter.to_time {
                    if e.detected_at > to_t {
                        return false;
                    }
                }
                if let Some(ref et) = filter.event_type {
                    if &e.event_type != et {
                        return false;
                    }
                }
                if let Some(sev) = filter.severity {
                    if e.severity != sev {
                        return false;
                    }
                }
                if let Some(mode) = filter.observation_mode {
                    if e.observation_mode != mode {
                        return false;
                    }
                }
                if let Some(job_id) = filter.replay_job_id {
                    if e.replay_job_id != Some(job_id) {
                        return false;
                    }
                }
                if let Some(ref txid) = filter.txid {
                    if e.txid.as_ref() != Some(txid) {
                        return false;
                    }
                }
                if let Some(ref bh) = filter.block_hash {
                    if e.block_hash.as_ref() != Some(bh) {
                        return false;
                    }
                }
                true
            })
            .skip(offset)
            .take(limit)
            .cloned()
            .collect();

        Ok(filtered)
    }
}

#[async_trait::async_trait]
impl IncidentRepository for InMemoryStorage {
    async fn save_incident(&self, incident: &Incident) -> Result<(), StorageError> {
        let mut lock = self
            .incidents
            .write()
            .map_err(|e| StorageError::Database(e.to_string()))?;
        if let Some(pos) = lock.iter().position(|i| i.id == incident.id) {
            lock[pos] = incident.clone();
        } else {
            lock.push(incident.clone());
        }
        Ok(())
    }

    async fn list_incidents(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Incident>, StorageError> {
        let lock = self
            .incidents
            .read()
            .map_err(|e| StorageError::Database(e.to_string()))?;
        let list = lock
            .iter()
            .rev()
            .skip(offset)
            .take(limit)
            .cloned()
            .collect();
        Ok(list)
    }

    async fn get_incident_by_id(&self, id: Uuid) -> Result<Option<Incident>, StorageError> {
        let lock = self
            .incidents
            .read()
            .map_err(|e| StorageError::Database(e.to_string()))?;
        Ok(lock.iter().find(|i| i.id == id).cloned())
    }

    async fn get_incident_by_id_or_case_id(
        &self,
        identifier: &str,
    ) -> Result<Option<Incident>, StorageError> {
        let lock = self
            .incidents
            .read()
            .map_err(|e| StorageError::Database(e.to_string()))?;

        if let Ok(parsed_uuid) = Uuid::parse_str(identifier) {
            if let Some(inc) = lock.iter().find(|i| i.id == parsed_uuid) {
                return Ok(Some(inc.clone()));
            }
        }
        Ok(lock
            .iter()
            .find(|i| i.case_id.eq_ignore_ascii_case(identifier))
            .cloned())
    }
}

#[async_trait::async_trait]
impl WatchTargetRepository for InMemoryStorage {
    async fn save_watch_target(&self, target: &WatchTarget) -> Result<(), StorageError> {
        let mut lock = self
            .watch_targets
            .write()
            .map_err(|e| StorageError::Database(e.to_string()))?;
        if let Some(pos) = lock.iter().position(|t| t.id == target.id) {
            lock[pos] = target.clone();
        } else {
            lock.push(target.clone());
        }
        Ok(())
    }

    async fn list_watch_targets(
        &self,
        incident_id: Option<Uuid>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<WatchTarget>, StorageError> {
        let lock = self
            .watch_targets
            .read()
            .map_err(|e| StorageError::Database(e.to_string()))?;
        let filtered: Vec<WatchTarget> = lock
            .iter()
            .filter(|t| {
                if let Some(inc_id) = incident_id {
                    t.incident_id == inc_id
                } else {
                    true
                }
            })
            .skip(offset)
            .take(limit)
            .cloned()
            .collect();
        Ok(filtered)
    }

    async fn get_watch_target_by_id(&self, id: Uuid) -> Result<Option<WatchTarget>, StorageError> {
        let lock = self
            .watch_targets
            .read()
            .map_err(|e| StorageError::Database(e.to_string()))?;
        Ok(lock.iter().find(|t| t.id == id).cloned())
    }
}

#[async_trait::async_trait]
impl IncidentActivityRepository for InMemoryStorage {
    async fn save_activity(&self, activity: &IncidentActivity) -> Result<(), StorageError> {
        let mut lock = self
            .activities
            .write()
            .map_err(|e| StorageError::Database(e.to_string()))?;

        // Deterministic deduplication or update by dedup_key
        if let Some(pos) = lock.iter().position(|a| a.dedup_key == activity.dedup_key) {
            lock[pos] = activity.clone();
            return Ok(());
        }

        if lock.len() >= self.max_activities {
            lock.pop_front();
        }
        lock.push_back(activity.clone());
        Ok(())
    }

    async fn list_activities(
        &self,
        incident_id: Option<Uuid>,
        status: Option<ActivityStatus>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<IncidentActivity>, StorageError> {
        let lock = self
            .activities
            .read()
            .map_err(|e| StorageError::Database(e.to_string()))?;

        let filtered: Vec<IncidentActivity> = lock
            .iter()
            .rev()
            .filter(|a| {
                if let Some(inc_id) = incident_id {
                    if a.incident_id != inc_id {
                        return false;
                    }
                }
                if let Some(st) = status {
                    if a.status != st {
                        return false;
                    }
                }
                true
            })
            .skip(offset)
            .take(limit)
            .cloned()
            .collect();

        Ok(filtered)
    }

    async fn get_activity_by_id(&self, id: Uuid) -> Result<Option<IncidentActivity>, StorageError> {
        let lock = self
            .activities
            .read()
            .map_err(|e| StorageError::Database(e.to_string()))?;
        Ok(lock.iter().find(|a| a.id == id).cloned())
    }

    async fn get_activity_by_dedup_key(
        &self,
        dedup_key: &str,
    ) -> Result<Option<IncidentActivity>, StorageError> {
        let lock = self
            .activities
            .read()
            .map_err(|e| StorageError::Database(e.to_string()))?;
        Ok(lock.iter().find(|a| a.dedup_key == dedup_key).cloned())
    }
}

#[async_trait::async_trait]
impl IncidentAlertRepository for InMemoryStorage {
    async fn save_alert(&self, alert: &IncidentAlert) -> Result<(), StorageError> {
        let mut lock = self
            .alerts
            .write()
            .map_err(|e| StorageError::Database(e.to_string()))?;

        if !lock.iter().any(|a| a.id == alert.id) {
            if lock.len() >= self.max_activities {
                lock.pop_front();
            }
            lock.push_back(alert.clone());
        }

        Ok(())
    }

    async fn list_alerts(
        &self,
        incident_id: Option<Uuid>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<IncidentAlert>, StorageError> {
        let lock = self
            .alerts
            .read()
            .map_err(|e| StorageError::Database(e.to_string()))?;

        let filtered: Vec<IncidentAlert> = lock
            .iter()
            .rev()
            .filter(|a| {
                if let Some(inc_id) = incident_id {
                    if a.incident_id != inc_id {
                        return false;
                    }
                }
                true
            })
            .skip(offset)
            .take(limit)
            .cloned()
            .collect();

        Ok(filtered)
    }

    async fn get_alert_by_id(&self, id: Uuid) -> Result<Option<IncidentAlert>, StorageError> {
        let lock = self
            .alerts
            .read()
            .map_err(|e| StorageError::Database(e.to_string()))?;
        Ok(lock.iter().find(|a| a.id == id).cloned())
    }
}

#[async_trait::async_trait]
impl ReplayRepository for InMemoryStorage {
    async fn create_job(&self, job: &ReplayJob) -> Result<(), StorageError> {
        let mut lock = self
            .replay_jobs
            .write()
            .map_err(|e| StorageError::Database(e.to_string()))?;
        if let Some(pos) = lock.iter().position(|j| j.id == job.id) {
            lock[pos] = job.clone();
        } else {
            lock.push(job.clone());
        }
        Ok(())
    }

    async fn get_job(&self, id: Uuid) -> Result<Option<ReplayJob>, StorageError> {
        let lock = self
            .replay_jobs
            .read()
            .map_err(|e| StorageError::Database(e.to_string()))?;
        Ok(lock.iter().find(|j| j.id == id).cloned())
    }

    async fn update_job(&self, job: &ReplayJob) -> Result<(), StorageError> {
        let mut lock = self
            .replay_jobs
            .write()
            .map_err(|e| StorageError::Database(e.to_string()))?;
        if let Some(pos) = lock.iter().position(|j| j.id == job.id) {
            lock[pos] = job.clone();
            Ok(())
        } else {
            Err(StorageError::NotFound(format!(
                "ReplayJob {} not found",
                job.id
            )))
        }
    }

    async fn list_jobs(&self, limit: usize, offset: usize) -> Result<Vec<ReplayJob>, StorageError> {
        let lock = self
            .replay_jobs
            .read()
            .map_err(|e| StorageError::Database(e.to_string()))?;
        let mut jobs = lock.clone();
        jobs.sort_by_key(|b| std::cmp::Reverse(b.created_at));
        let res = jobs.into_iter().skip(offset).take(limit).collect();
        Ok(res)
    }

    async fn save_checkpoint(&self, checkpoint: &ReplayCheckpoint) -> Result<(), StorageError> {
        let mut lock = self
            .replay_checkpoints
            .write()
            .map_err(|e| StorageError::Database(e.to_string()))?;
        lock.push(checkpoint.clone());
        Ok(())
    }

    async fn get_latest_checkpoint(
        &self,
        job_id: Uuid,
    ) -> Result<Option<ReplayCheckpoint>, StorageError> {
        let lock = self
            .replay_checkpoints
            .read()
            .map_err(|e| StorageError::Database(e.to_string()))?;
        let latest = lock
            .iter()
            .filter(|c| c.job_id == job_id)
            .max_by_key(|c| c.completed_height)
            .cloned();
        Ok(latest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_in_memory_event_storage_and_lookup() {
        let storage = InMemoryStorage::new_empty(10);
        assert_eq!(storage.event_count(), 0);

        let event = ChainEvent::new(
            EventType::LargeTransfer,
            EventSeverity::High,
            ConfidenceLevel::VerifiedOnChain,
            "Test Event",
            "Details",
        );
        let id = event.id;

        storage.save_event(&event).await.unwrap();
        assert_eq!(storage.event_count(), 1);

        let retrieved = storage.get_event_by_id(id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().title, "Test Event");
    }

    #[tokio::test]
    async fn test_in_memory_bounded_retention() {
        let storage = InMemoryStorage::new_empty(3); // Limit to 3 events

        for i in 1..=5 {
            let event = ChainEvent::new(
                EventType::LargeTransfer,
                EventSeverity::Low,
                ConfidenceLevel::VerifiedOnChain,
                format!("Event {i}"),
                "Details",
            );
            storage.save_event(&event).await.unwrap();
        }

        // Bounded capacity must remain at 3
        assert_eq!(storage.event_count(), 3);

        let list = storage.list_events(10, 0).await.unwrap();
        assert_eq!(list.len(), 3);
        // Reverse chronological order: newest (5) first, then 4, then 3
        assert_eq!(list[0].title, "Event 5");
        assert_eq!(list[1].title, "Event 4");
        assert_eq!(list[2].title, "Event 3");
    }

    #[tokio::test]
    async fn test_in_memory_seeded_watch_targets() {
        let storage = InMemoryStorage::new_empty(10);
        let targets = storage.list_watch_targets(None, 50, 0).await.unwrap();
        // Liquid 2026 canonical targets: 6 seeded targets
        assert_eq!(targets.len(), 6);
        assert_eq!(storage.watch_target_count(), 6);

        // Verify finding by ID
        let first = &targets[0];
        let found = storage.get_watch_target_by_id(first.id).await.unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().case_id, "OC-2026-0001");
    }

    #[tokio::test]
    async fn test_in_memory_activity_dedup_and_bounded_retention() {
        let storage = InMemoryStorage::with_limits(10, 3);
        assert_eq!(storage.activity_count(), 0);

        let inc_id = Uuid::new_v4();
        let target_id = Uuid::new_v4();

        // 1. Create activity 1
        let act1 = IncidentActivity {
            id: Uuid::new_v4(),
            incident_id: inc_id,
            case_id: "OC-2026-0001".to_string(),
            activity_type: obschain_core::IncidentActivityType::WatchedOutpointSpent,
            observed_at: Utc::now(),
            trigger_txid: Some("tx001".to_string()),
            block_height: None,
            block_hash: None,
            value_sats: Some(100_000),
            watch_target_id: target_id,
            confidence: obschain_core::ProvenanceClassification::OnChainVerified,
            correlation_strength: obschain_core::CorrelationStrength::Direct,
            status: ActivityStatus::Mempool,
            source: obschain_core::ObservationSource::mempool_ws("wss://mempool.space/api/v1/ws"),
            evidence: vec![],
            description: "Watched outpoint spent in mempool".to_string(),
            details: None,
            dedup_key: IncidentActivity::generate_dedup_key(
                &inc_id,
                &obschain_core::IncidentActivityType::WatchedOutpointSpent,
                Some("tx001"),
                &target_id,
            ),
        };

        storage.save_activity(&act1).await.unwrap();
        assert_eq!(storage.activity_count(), 1);

        // 2. Duplicate activity with same dedup_key but updated status
        let mut act1_confirmed = act1.clone();
        act1_confirmed.status = ActivityStatus::Confirmed;
        act1_confirmed.block_height = Some(888_000);

        storage.save_activity(&act1_confirmed).await.unwrap();
        // Count should still be 1 (updated, not duplicated)
        assert_eq!(storage.activity_count(), 1);
        let retrieved = storage.get_activity_by_id(act1.id).await.unwrap().unwrap();
        assert_eq!(retrieved.status, ActivityStatus::Confirmed);
        assert_eq!(retrieved.block_height, Some(888_000));

        // 3. Add more activities to test bounded limit (3)
        for i in 2..=5 {
            let act = IncidentActivity {
                id: Uuid::new_v4(),
                incident_id: inc_id,
                case_id: "OC-2026-0001".to_string(),
                activity_type: obschain_core::IncidentActivityType::WatchedAddressReceived,
                observed_at: Utc::now(),
                trigger_txid: Some(format!("tx00{i}")),
                block_height: None,
                block_hash: None,
                value_sats: Some(10_000 * i),
                watch_target_id: target_id,
                confidence: obschain_core::ProvenanceClassification::OnChainVerified,
                correlation_strength: obschain_core::CorrelationStrength::Direct,
                status: ActivityStatus::Confirmed,
                source: obschain_core::ObservationSource::mempool_ws(
                    "wss://mempool.space/api/v1/ws",
                ),
                evidence: vec![],
                description: format!("Activity {i}"),
                details: None,
                dedup_key: format!("key-{i}"),
            };
            storage.save_activity(&act).await.unwrap();
        }

        // Bounded capacity must remain at 3
        assert_eq!(storage.activity_count(), 3);
        let list = storage.list_activities(None, None, 10, 0).await.unwrap();
        assert_eq!(list.len(), 3);
        // Newest first
        assert_eq!(list[0].description, "Activity 5");
        assert_eq!(list[1].description, "Activity 4");
        assert_eq!(list[2].description, "Activity 3");
    }
}
