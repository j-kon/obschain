use obschain_core::{
    ChainEvent, ConfidenceLevel, EventSeverity, EventType, Incident, IncidentStatus,
};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::repository::{EventRepository, IncidentRepository, StorageError};

/// SQLx PostgreSQL storage backend.
#[derive(Clone)]
pub struct PostgresStorage {
    pool: PgPool,
}

impl PostgresStorage {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

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

        sqlx::query(
            r#"
            INSERT INTO events (id, event_type, severity, confidence, title, description, detected_at, block_height, block_hash, txid, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                metadata = EXCLUDED.metadata
            "#
        )
        .bind(event.id)
        .bind(event_type_str)
        .bind(severity_str)
        .bind(confidence_str)
        .bind(&event.title)
        .bind(&event.description)
        .bind(event.detected_at)
        .bind(event.block_height.map(|h| h as i64))
        .bind(&event.block_hash)
        .bind(&event.txid)
        .bind(&event.metadata)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        Ok(())
    }

    async fn list_events(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ChainEvent>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT id, event_type, severity, confidence, title, description, detected_at, block_height, block_hash, txid, metadata
            FROM events
            ORDER BY detected_at DESC
            LIMIT $1 OFFSET $2
            "#
        )
        .bind(limit as i64)
        .bind(offset as i64)
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

            events.push(ChainEvent {
                id: row.get("id"),
                event_type,
                severity,
                confidence,
                title: row.get("title"),
                description: row.get("description"),
                detected_at: row.get("detected_at"),
                block_height: block_height_i64.map(|h| h as u64),
                block_hash: row.get("block_hash"),
                txid: row.get("txid"),
                metadata: row.get("metadata"),
            });
        }

        Ok(events)
    }

    async fn get_event_by_id(&self, id: Uuid) -> Result<Option<ChainEvent>, StorageError> {
        let row_opt = sqlx::query(
            r#"
            SELECT id, event_type, severity, confidence, title, description, detected_at, block_height, block_hash, txid, metadata
            FROM events
            WHERE id = $1
            "#
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

        Ok(Some(ChainEvent {
            id: row.get("id"),
            event_type,
            severity,
            confidence,
            title: row.get("title"),
            description: row.get("description"),
            detected_at: row.get("detected_at"),
            block_height: block_height_i64.map(|h| h as u64),
            block_hash: row.get("block_hash"),
            txid: row.get("txid"),
            metadata: row.get("metadata"),
        }))
    }
}

#[async_trait::async_trait]
impl IncidentRepository for PostgresStorage {
    async fn save_incident(&self, incident: &Incident) -> Result<(), StorageError> {
        let status_str = serde_json::to_string(&incident.status)
            .map_err(StorageError::Serialization)?
            .trim_matches('"')
            .to_string();
        let severity_str = serde_json::to_string(&incident.severity)
            .map_err(StorageError::Serialization)?
            .trim_matches('"')
            .to_string();

        let facts_json = serde_json::to_value(&incident.facts)?;
        let reported_json = serde_json::to_value(&incident.reported_claims)?;
        let unverified_json = serde_json::to_value(&incident.unverified_claims)?;

        sqlx::query(
            r#"
            INSERT INTO incidents (id, title, summary, status, severity, total_btc_affected, total_btc_recovered, first_observed_at, last_updated_at, facts, reported_claims, unverified_claims)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                summary = EXCLUDED.summary,
                status = EXCLUDED.status,
                severity = EXCLUDED.severity,
                total_btc_affected = EXCLUDED.total_btc_affected,
                total_btc_recovered = EXCLUDED.total_btc_recovered,
                last_updated_at = EXCLUDED.last_updated_at,
                facts = EXCLUDED.facts,
                reported_claims = EXCLUDED.reported_claims,
                unverified_claims = EXCLUDED.unverified_claims
            "#
        )
        .bind(incident.id)
        .bind(&incident.title)
        .bind(&incident.summary)
        .bind(status_str)
        .bind(severity_str)
        .bind(incident.total_btc_affected)
        .bind(incident.total_btc_recovered)
        .bind(incident.first_observed_at)
        .bind(incident.last_updated_at)
        .bind(facts_json)
        .bind(reported_json)
        .bind(unverified_json)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        Ok(())
    }

    async fn list_incidents(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Incident>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT id, title, summary, status, severity, total_btc_affected, total_btc_recovered, first_observed_at, last_updated_at, facts, reported_claims, unverified_claims
            FROM incidents
            ORDER BY last_updated_at DESC
            LIMIT $1 OFFSET $2
            "#
        )
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Database(e.to_string()))?;

        let mut list = Vec::with_capacity(rows.len());
        for row in rows {
            let status_str: String = row.get("status");
            let severity_str: String = row.get("severity");

            let status: IncidentStatus = serde_json::from_str(&format!("\"{status_str}\""))
                .unwrap_or(IncidentStatus::Investigating);
            let severity: EventSeverity =
                serde_json::from_str(&format!("\"{severity_str}\"")).unwrap_or(EventSeverity::High);

            let facts: serde_json::Value = row.get("facts");
            let reported: serde_json::Value = row.get("reported_claims");
            let unverified: serde_json::Value = row.get("unverified_claims");

            list.push(Incident {
                id: row.get("id"),
                title: row.get("title"),
                summary: row.get("summary"),
                status,
                severity,
                total_btc_affected: row.get("total_btc_affected"),
                total_btc_recovered: row.get("total_btc_recovered"),
                first_observed_at: row.get("first_observed_at"),
                last_updated_at: row.get("last_updated_at"),
                facts: serde_json::from_value(facts).unwrap_or_default(),
                reported_claims: serde_json::from_value(reported).unwrap_or_default(),
                unverified_claims: serde_json::from_value(unverified).unwrap_or_default(),
                associated_txids: Vec::new(),
                associated_block_heights: Vec::new(),
                timeline: Vec::new(),
                evidence: Vec::new(),
                sources: Vec::new(),
            });
        }

        Ok(list)
    }

    async fn get_incident_by_id(&self, id: Uuid) -> Result<Option<Incident>, StorageError> {
        let row_opt = sqlx::query(
            r#"
            SELECT id, title, summary, status, severity, total_btc_affected, total_btc_recovered, first_observed_at, last_updated_at, facts, reported_claims, unverified_claims
            FROM incidents
            WHERE id = $1
            "#
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

        let facts: serde_json::Value = row.get("facts");
        let reported: serde_json::Value = row.get("reported_claims");
        let unverified: serde_json::Value = row.get("unverified_claims");

        Ok(Some(Incident {
            id: row.get("id"),
            title: row.get("title"),
            summary: row.get("summary"),
            status,
            severity,
            total_btc_affected: row.get("total_btc_affected"),
            total_btc_recovered: row.get("total_btc_recovered"),
            first_observed_at: row.get("first_observed_at"),
            last_updated_at: row.get("last_updated_at"),
            facts: serde_json::from_value(facts).unwrap_or_default(),
            reported_claims: serde_json::from_value(reported).unwrap_or_default(),
            unverified_claims: serde_json::from_value(unverified).unwrap_or_default(),
            associated_txids: Vec::new(),
            associated_block_heights: Vec::new(),
            timeline: Vec::new(),
            evidence: Vec::new(),
            sources: Vec::new(),
        }))
    }
}
