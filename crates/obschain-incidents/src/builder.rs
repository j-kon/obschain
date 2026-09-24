use chrono::{DateTime, Utc};
use obschain_core::{
    Chain, CoreError, EventSeverity, Evidence, GraphEdge, GraphEdgeType, GraphNode, GraphNodeType,
    Incident, IncidentBlock, IncidentEntity, IncidentGraph, IncidentStatus, IncidentTransaction,
    IncidentUpdate, OnChainMessage, ProvenanceClassification, RecoverySummary, Source,
    StructuredClaimsSummary, TechnicalFinding, TimelineEntry,
};
use uuid::Uuid;

use crate::validation::validate_incident;

/// Builder to construct an Incident case while maintaining strict provenance standards.
pub struct IncidentBuilder {
    incident: Incident,
}

impl IncidentBuilder {
    pub fn new(
        case_id: impl Into<String>,
        title: impl Into<String>,
        summary: impl Into<String>,
        severity: EventSeverity,
    ) -> Self {
        let now = Utc::now();
        let cid = case_id.into();
        Self {
            incident: Incident {
                id: Uuid::new_v4(),
                case_id: cid,
                title: title.into(),
                summary: summary.into(),
                status: IncidentStatus::Detected,
                severity,
                recovery: RecoverySummary::new(0, 0, now),
                first_observed_at: now,
                last_updated_at: now,
                structured_claims: StructuredClaimsSummary::default(),
                entities: Vec::new(),
                transactions: Vec::new(),
                blocks: Vec::new(),
                on_chain_messages: Vec::new(),
                timeline: Vec::new(),
                evidence: Vec::new(),
                sources: Vec::new(),
                technical_findings: Vec::new(),
                updates: Vec::new(),
                graph: IncidentGraph::new(),
                total_btc_affected: 0.0,
                total_btc_recovered: 0.0,
                facts: Vec::new(),
                reported_claims: Vec::new(),
                unverified_claims: Vec::new(),
                associated_txids: Vec::new(),
                associated_block_heights: Vec::new(),
            },
        }
    }

    pub fn with_id(mut self, id: Uuid) -> Self {
        self.incident.id = id;
        self
    }

    pub fn status(mut self, status: IncidentStatus) -> Self {
        self.incident.status = status;
        self
    }

    pub fn observed_at(mut self, observed_at: DateTime<Utc>) -> Self {
        self.incident.first_observed_at = observed_at;
        self
    }

    pub fn updated_at(mut self, updated_at: DateTime<Utc>) -> Self {
        self.incident.last_updated_at = updated_at;
        self
    }

    pub fn recovery(mut self, recovery: RecoverySummary) -> Self {
        self.incident.total_btc_affected = recovery.affected_btc();
        self.incident.total_btc_recovered = recovery.recovered_btc();
        self.incident.recovery = recovery;
        self
    }

    pub fn add_verified_fact(mut self, fact: impl Into<String>) -> Self {
        let text = fact.into();
        self.incident
            .structured_claims
            .verified_on_chain
            .push(text.clone());
        self.incident.facts.push(text);
        self
    }

    pub fn add_official_claim(mut self, claim: impl Into<String>) -> Self {
        let text = claim.into();
        self.incident
            .structured_claims
            .officially_attributed
            .push(text.clone());
        self.incident.reported_claims.push(text);
        self
    }

    pub fn add_reported_claim(mut self, claim: impl Into<String>) -> Self {
        let text = claim.into();
        self.incident.structured_claims.reported.push(text.clone());
        self.incident.reported_claims.push(text);
        self
    }

    pub fn add_heuristic_claim(mut self, claim: impl Into<String>) -> Self {
        let text = claim.into();
        self.incident.structured_claims.heuristic.push(text.clone());
        self.incident.unverified_claims.push(text);
        self
    }

    pub fn add_unknown_claim(mut self, claim: impl Into<String>) -> Self {
        let text = claim.into();
        self.incident.structured_claims.unknown.push(text.clone());
        self.incident.unverified_claims.push(text);
        self
    }

    pub fn add_entity(mut self, entity: IncidentEntity) -> Self {
        self.incident.entities.push(entity);
        self
    }

    pub fn add_transaction(mut self, tx: IncidentTransaction) -> Self {
        if !self.incident.associated_txids.contains(&tx.txid) {
            self.incident.associated_txids.push(tx.txid.clone());
        }
        self.incident.transactions.push(tx);
        self
    }

    pub fn add_block(mut self, block: IncidentBlock) -> Self {
        if !self
            .incident
            .associated_block_heights
            .contains(&block.height)
        {
            self.incident.associated_block_heights.push(block.height);
        }
        self.incident.blocks.push(block);
        self
    }

    pub fn add_on_chain_message(mut self, msg: OnChainMessage) -> Self {
        self.incident.on_chain_messages.push(msg);
        self
    }

    pub fn add_timeline_entry(mut self, entry: TimelineEntry) -> Self {
        self.incident.timeline.push(entry);
        self
    }

    pub fn add_evidence(mut self, evidence: Evidence) -> Self {
        self.incident.evidence.push(evidence);
        self
    }

    pub fn add_source(mut self, source: Source) -> Self {
        self.incident.sources.push(source);
        self
    }

    pub fn add_technical_finding(mut self, finding: TechnicalFinding) -> Self {
        self.incident.technical_findings.push(finding);
        self
    }

    pub fn add_update(mut self, update: IncidentUpdate) -> Self {
        self.incident.updates.push(update);
        self
    }

    pub fn add_graph_node(
        mut self,
        id: impl Into<String>,
        label: impl Into<String>,
        node_type: GraphNodeType,
        chain: Option<Chain>,
        metadata: Option<serde_json::Value>,
    ) -> Self {
        self.incident.graph.add_node(GraphNode {
            id: id.into(),
            label: label.into(),
            node_type,
            chain,
            metadata,
        });
        self
    }

    pub fn add_graph_edge(
        mut self,
        source: impl Into<String>,
        target: impl Into<String>,
        relationship: GraphEdgeType,
        confidence: ProvenanceClassification,
    ) -> Self {
        self.incident.graph.add_edge(GraphEdge {
            source: source.into(),
            target: target.into(),
            relationship,
            confidence,
        });
        self
    }

    pub fn sort_timeline(mut self) -> Self {
        self.incident.timeline.sort_by_key(|e| e.timestamp);
        self
    }

    /// Validates all invariants and returns the completed Incident.
    pub fn build(self) -> Result<Incident, CoreError> {
        validate_incident(&self.incident)?;
        Ok(self.incident)
    }

    /// Build without validation (useful for test assertions of invalid states).
    pub fn build_unvalidated(self) -> Incident {
        self.incident
    }
}
