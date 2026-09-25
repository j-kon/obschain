use chrono::{DateTime, Utc};
use obschain_core::{
    ActivityStatus, Chain, ChainEvent, ConfidenceLevel, CorrelationStrength, EventSeverity,
    EventType, Evidence, EvidenceType, GraphEdge, GraphEdgeType, GraphNode, GraphNodeType,
    Incident, IncidentActivity, IncidentActivityType, IncidentAlert, IncidentBlock, IncidentEntity,
    IncidentGraph, IncidentStatus, IncidentTransaction, IncidentUpdate, ObservationSource,
    OnChainMessage, ProvenanceClassification, RecoverySummary, Source, SourceCategory,
    TechnicalFinding, TimelineCategory, TimelineEntry, TransactionRole, WatchTarget,
    WatchTargetKind,
};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::repository::{
    i64_to_u64_checked, u64_to_i64_checked, EventRepository, IncidentActivityRepository,
    IncidentAlertRepository, IncidentRepository, StorageError, WatchTargetRepository,
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

        sqlx::query(
            r#"
            INSERT INTO chain_events (
                id, event_type, severity, confidence, title, description,
                detected_at, block_height, block_hash, txid, metadata, source
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                metadata = EXCLUDED.metadata,
                source = EXCLUDED.source
            "#,
        )
        .bind(event.id)
        .bind(event_type_str)
        .bind(severity_str)
        .bind(confidence_str)
        .bind(&event.title)
        .bind(&event.description)
        .bind(event.detected_at)
        .bind(block_height_i64)
        .bind(&event.block_hash)
        .bind(&event.txid)
        .bind(&event.metadata)
        .bind(source_json)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        self.cached_events.fetch_add(1, Ordering::Relaxed);
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
                   detected_at, block_height, block_hash, txid, metadata, source
            FROM chain_events
            ORDER BY detected_at DESC
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

            events.push(ChainEvent {
                id: row.get("id"),
                event_type,
                severity,
                confidence,
                title: row.get("title"),
                description: row.get("description"),
                detected_at: row.get("detected_at"),
                block_height,
                block_hash: row.get("block_hash"),
                txid: row.get("txid"),
                metadata: row.get("metadata"),
                witnesses: source
                    .clone()
                    .map(|s| vec![obschain_core::ObservationWitness::new(s)])
                    .unwrap_or_default(),
                source,
            });
        }

        Ok(events)
    }

    async fn get_event_by_id(&self, id: Uuid) -> Result<Option<ChainEvent>, StorageError> {
        let row_opt = sqlx::query(
            r#"
            SELECT id, event_type, severity, confidence, title, description,
                   detected_at, block_height, block_hash, txid, metadata, source
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

        Ok(Some(ChainEvent {
            id: row.get("id"),
            event_type,
            severity,
            confidence,
            title: row.get("title"),
            description: row.get("description"),
            detected_at: row.get("detected_at"),
            block_height,
            block_hash: row.get("block_hash"),
            txid: row.get("txid"),
            metadata: row.get("metadata"),
            witnesses: source
                .clone()
                .map(|s| vec![obschain_core::ObservationWitness::new(s)])
                .unwrap_or_default(),
            source,
        }))
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
