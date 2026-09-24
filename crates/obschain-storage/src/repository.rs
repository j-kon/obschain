use std::{
    collections::VecDeque,
    sync::{Arc, RwLock},
};

use chrono::Utc;
use obschain_core::{
    ChainEvent, ConfidenceLevel, EventSeverity, EventType, Evidence, EvidenceType, Incident,
    IncidentStatus, ProvenanceClassification, Source, TimelineEvent,
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

#[async_trait::async_trait]
pub trait EventRepository: Send + Sync {
    async fn save_event(&self, event: &ChainEvent) -> Result<(), StorageError>;
    async fn list_events(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ChainEvent>, StorageError>;
    async fn get_event_by_id(&self, id: Uuid) -> Result<Option<ChainEvent>, StorageError>;
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
}

/// Thread-safe in-memory event and incident store with bounded retention.
/// Uses a circular VecDeque to bound maximum memory consumption.
#[derive(Clone)]
pub struct InMemoryStorage {
    events: Arc<RwLock<VecDeque<ChainEvent>>>,
    incidents: Arc<RwLock<Vec<Incident>>>,
    max_events: usize,
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStorage {
    pub const DEFAULT_MAX_EVENTS: usize = 10_000;

    pub fn new() -> Self {
        Self::with_limit(Self::DEFAULT_MAX_EVENTS)
    }

    pub fn with_limit(max_events: usize) -> Self {
        let storage = Self {
            events: Arc::new(RwLock::new(VecDeque::with_capacity(max_events.min(1000)))),
            incidents: Arc::new(RwLock::new(Vec::new())),
            max_events,
        };
        storage.seed_mock_data();
        storage
    }

    pub fn new_empty(max_events: usize) -> Self {
        Self {
            events: Arc::new(RwLock::new(VecDeque::with_capacity(max_events.min(1000)))),
            incidents: Arc::new(RwLock::new(Vec::new())),
            max_events,
        }
    }

    pub fn event_count(&self) -> usize {
        self.events.read().map(|l| l.len()).unwrap_or(0)
    }

    pub fn clear_events(&self) {
        if let Ok(mut lock) = self.events.write() {
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

        // Sample Incident: Exchange Hot Wallet Extraction
        let incident_id = Uuid::parse_str("b0000000-0000-0000-0000-000000000001").unwrap();
        let evidence_id1 = Uuid::parse_str("c0000000-0000-0000-0000-000000000001").unwrap();
        let evidence_id2 = Uuid::parse_str("c0000000-0000-0000-0000-000000000002").unwrap();

        let incident = Incident {
            id: incident_id,
            title: "Simulated Exchange Cold-to-Hot Dispersal Anomaly".to_string(),
            summary: "Rapid multi-stage dispersal of 1,200 BTC from known reserve clusters into newly created Taproot addresses."
                .to_string(),
            status: IncidentStatus::Investigating,
            severity: EventSeverity::Critical,
            total_btc_affected: 1200.0,
            total_btc_recovered: 0.0,
            first_observed_at: now - chrono::Duration::days(1),
            last_updated_at: now - chrono::Duration::minutes(40),
            facts: vec![
                "Block 884800 confirmed transaction 9f23... transferring 1,200.00 BTC."
                    .to_string(),
                "Output scripts utilize P2TR (Taproot key-path spending).".to_string(),
                "Funds were split into 24 distinct 50 BTC tranches within 3 blocks."
                    .to_string(),
            ],
            reported_claims: vec![
                "Security advisory reports unauthorized API key compromise at Exchange X."
                    .to_string(),
            ],
            unverified_claims: vec![
                "Social media claims attributing destination addresses to specific entity are currently unverified heuristic clusters."
                    .to_string(),
            ],
            associated_txids: vec![
                "9f238b7d415f3e9a117cf6b4887321e06fa78e124efbda7128f522f87a8b30d1".to_string(),
                "3c914bf6874229dafe11029c4ba598007e2cf1796b341f2e1a3bc89110ab78f2".to_string(),
            ],
            associated_block_heights: vec![884800, 884801, 884803],
            timeline: vec![
                TimelineEvent {
                    id: Uuid::new_v4(),
                    timestamp: now - chrono::Duration::hours(24),
                    title: "First Dispersal Tx Confirmed".to_string(),
                    description: "Initial 1,200 BTC split into four 300 BTC intermediate UTXOs."
                        .to_string(),
                    evidence_id: Some(evidence_id1),
                    classification: ProvenanceClassification::OnChainVerified,
                },
                TimelineEvent {
                    id: Uuid::new_v4(),
                    timestamp: now - chrono::Duration::hours(18),
                    title: "Security Advisory Published".to_string(),
                    description: "Exchange published preliminary post-incident notification."
                        .to_string(),
                    evidence_id: Some(evidence_id2),
                    classification: ProvenanceClassification::OfficiallyAttributed,
                },
            ],
            evidence: vec![
                Evidence {
                    id: evidence_id1,
                    incident_id,
                    evidence_type: EvidenceType::OnChainTransaction,
                    classification: ProvenanceClassification::OnChainVerified,
                    description: "Initial dispersal transaction on Bitcoin mainnet".to_string(),
                    reference:
                        "9f238b7d415f3e9a117cf6b4887321e06fa78e124efbda7128f522f87a8b30d1"
                            .to_string(),
                    raw_data: Some(serde_json::json!({
                        "is_mock": true,
                        "confirmations": 144,
                        "value_btc": 1200.0
                    })),
                    created_at: now - chrono::Duration::hours(24),
                },
                Evidence {
                    id: evidence_id2,
                    incident_id,
                    evidence_type: EvidenceType::OfficialStatement,
                    classification: ProvenanceClassification::OfficiallyAttributed,
                    description: "Cryptographically signed confirmation from affected custodian"
                        .to_string(),
                    reference: "https://advisories.example.org/sec-2026-001".to_string(),
                    raw_data: Some(serde_json::json!({
                        "is_mock": true,
                        "signature_type": "secp256k1"
                    })),
                    created_at: now - chrono::Duration::hours(18),
                },
            ],
            sources: vec![Source {
                id: Uuid::new_v4(),
                name: "ObsChain Bitcoin Node Telemetry".to_string(),
                url: Some("https://obschain.internal/status".to_string()),
                reliability_score: 1.0,
                published_at: Some(now - chrono::Duration::hours(24)),
            }],
        };

        if let Ok(mut lock) = self.incidents.write() {
            lock.push(incident);
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
}
