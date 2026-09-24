-- Migration 0002: Durable PostgreSQL Persistence (Phase 4B)
-- ObsChain normalized relational schema for chain events, incidents,
-- evidence hierarchy, timeline, graph, watch targets, activities, and alerts.

-- Clean up any prototype bootstrap tables from initial workspace scaffold
DROP TABLE IF EXISTS sources CASCADE;
DROP TABLE IF EXISTS timeline_events CASCADE;
DROP TABLE IF EXISTS evidence CASCADE;
DROP TABLE IF EXISTS incident_blocks CASCADE;
DROP TABLE IF EXISTS incident_transactions CASCADE;
DROP TABLE IF EXISTS incidents CASCADE;
DROP TABLE IF EXISTS events CASCADE;

-- 1. Chain Events
CREATE TABLE IF NOT EXISTS chain_events (
    id UUID PRIMARY KEY,
    event_type VARCHAR(64) NOT NULL,
    severity VARCHAR(32) NOT NULL,
    confidence VARCHAR(32) NOT NULL,
    title VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    block_height BIGINT CHECK (block_height IS NULL OR block_height >= 0),
    block_hash VARCHAR(64),
    txid VARCHAR(64),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    source JSONB
);

CREATE INDEX IF NOT EXISTS idx_chain_events_detected_at ON chain_events(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_chain_events_type_detected ON chain_events(event_type, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_chain_events_severity ON chain_events(severity);
CREATE INDEX IF NOT EXISTS idx_chain_events_txid ON chain_events(txid) WHERE txid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_chain_events_block_height ON chain_events(block_height) WHERE block_height IS NOT NULL;

-- 2. Incidents Core Dossier
CREATE TABLE IF NOT EXISTS incidents (
    id UUID PRIMARY KEY,
    case_id VARCHAR(64) NOT NULL UNIQUE,
    title VARCHAR(255) NOT NULL,
    summary TEXT NOT NULL,
    status VARCHAR(32) NOT NULL,
    severity VARCHAR(32) NOT NULL,
    first_observed_at TIMESTAMPTZ NOT NULL,
    last_updated_at TIMESTAMPTZ NOT NULL,
    structured_claims JSONB NOT NULL DEFAULT '{}'::jsonb,
    facts JSONB NOT NULL DEFAULT '[]'::jsonb,
    reported_claims JSONB NOT NULL DEFAULT '[]'::jsonb,
    unverified_claims JSONB NOT NULL DEFAULT '[]'::jsonb,
    associated_txids JSONB NOT NULL DEFAULT '[]'::jsonb,
    associated_block_heights JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_incidents_case_id ON incidents(case_id);
CREATE INDEX IF NOT EXISTS idx_incidents_status ON incidents(status);
CREATE INDEX IF NOT EXISTS idx_incidents_severity ON incidents(severity);
CREATE INDEX IF NOT EXISTS idx_incidents_last_updated ON incidents(last_updated_at DESC);

-- 3. Incident Recovery Snapshots (Preserves historical recovery numbers without overwriting)
CREATE TABLE IF NOT EXISTS incident_recovery_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    affected_sats BIGINT NOT NULL CHECK (affected_sats >= 0),
    recovered_sats BIGINT NOT NULL CHECK (recovered_sats >= 0),
    outstanding_sats BIGINT NOT NULL CHECK (outstanding_sats >= 0),
    is_estimate BOOLEAN NOT NULL DEFAULT FALSE,
    as_of_timestamp TIMESTAMPTZ NOT NULL,
    source_id UUID,
    source_label TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_recovered_sats_lte_affected CHECK (recovered_sats <= affected_sats)
);

CREATE INDEX IF NOT EXISTS idx_recovery_snapshots_incident_time 
    ON incident_recovery_snapshots(incident_id, as_of_timestamp DESC);

-- 4. Incident Sources (Canonical external disclosures, advisories, reports)
CREATE TABLE IF NOT EXISTS incident_sources (
    id UUID PRIMARY KEY,
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    publisher VARCHAR(255) NOT NULL,
    title VARCHAR(255) NOT NULL,
    url TEXT,
    publication_timestamp TIMESTAMPTZ,
    retrieved_timestamp TIMESTAMPTZ,
    source_category VARCHAR(64) NOT NULL,
    reliability_score REAL NOT NULL DEFAULT 0.5 CHECK (reliability_score >= 0.0 AND reliability_score <= 1.0),
    name VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sources_incident ON incident_sources(incident_id);

-- 5. Incident Evidence Items
CREATE TABLE IF NOT EXISTS incident_evidence (
    id UUID PRIMARY KEY,
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    evidence_type VARCHAR(64) NOT NULL,
    confidence VARCHAR(64) NOT NULL,
    title VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    observed_at TIMESTAMPTZ,
    source_id UUID REFERENCES incident_sources(id) ON DELETE SET NULL,
    source_reference TEXT,
    txid VARCHAR(64),
    block_hash VARCHAR(64),
    block_height BIGINT,
    chain VARCHAR(32) NOT NULL DEFAULT 'Bitcoin',
    verified BOOLEAN NOT NULL DEFAULT FALSE,
    raw_data JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_evidence_incident ON incident_evidence(incident_id);
CREATE INDEX IF NOT EXISTS idx_evidence_confidence ON incident_evidence(incident_id, confidence);
CREATE INDEX IF NOT EXISTS idx_evidence_txid ON incident_evidence(txid) WHERE txid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_evidence_block ON incident_evidence(block_height) WHERE block_height IS NOT NULL;

-- 6. Incident Transactions
CREATE TABLE IF NOT EXISTS incident_transactions (
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    chain VARCHAR(32) NOT NULL DEFAULT 'Bitcoin',
    txid VARCHAR(64) NOT NULL,
    role VARCHAR(64) NOT NULL,
    amount_sats BIGINT CHECK (amount_sats IS NULL OR amount_sats >= 0),
    block_height BIGINT CHECK (block_height IS NULL OR block_height >= 0),
    block_hash VARCHAR(64),
    confirmed_at TIMESTAMPTZ,
    evidence_id UUID REFERENCES incident_evidence(id) ON DELETE SET NULL,
    notes TEXT,
    PRIMARY KEY (incident_id, chain, txid)
);

CREATE INDEX IF NOT EXISTS idx_incident_txs_txid ON incident_transactions(txid);

-- 7. Incident Blocks
CREATE TABLE IF NOT EXISTS incident_blocks (
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    chain VARCHAR(32) NOT NULL DEFAULT 'Bitcoin',
    height BIGINT NOT NULL CHECK (height >= 0),
    hash VARCHAR(64) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    tx_count INTEGER,
    evidence_id UUID REFERENCES incident_evidence(id) ON DELETE SET NULL,
    PRIMARY KEY (incident_id, chain, height)
);

-- 8. Incident Entities
CREATE TABLE IF NOT EXISTS incident_entities (
    id UUID PRIMARY KEY,
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    entity_type VARCHAR(64) NOT NULL,
    description TEXT NOT NULL,
    attribution_confidence VARCHAR(64) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_entities_incident ON incident_entities(incident_id);

-- 9. On-Chain Messages (OP_RETURN / scripts)
CREATE TABLE IF NOT EXISTS incident_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    txid VARCHAR(64) NOT NULL,
    chain VARCHAR(32) NOT NULL DEFAULT 'Bitcoin',
    encoding VARCHAR(32) NOT NULL,
    decoded_text TEXT NOT NULL,
    raw_hex TEXT NOT NULL,
    confirmed_at TIMESTAMPTZ,
    block_height BIGINT,
    attributed_sender VARCHAR(255),
    sender_attribution_confidence VARCHAR(64) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_incident ON incident_messages(incident_id);

-- 10. Incident Timeline
CREATE TABLE IF NOT EXISTS incident_timeline (
    id UUID PRIMARY KEY,
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ NOT NULL,
    title VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    category VARCHAR(64) NOT NULL,
    source_id UUID REFERENCES incident_sources(id) ON DELETE SET NULL,
    evidence_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    transaction_txids JSONB NOT NULL DEFAULT '[]'::jsonb,
    block_heights JSONB NOT NULL DEFAULT '[]'::jsonb,
    classification VARCHAR(64) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_timeline_incident_ts ON incident_timeline(incident_id, timestamp ASC);

-- 11. Incident Technical Findings
CREATE TABLE IF NOT EXISTS incident_technical_findings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    component VARCHAR(255) NOT NULL,
    area VARCHAR(255) NOT NULL,
    category VARCHAR(255) NOT NULL,
    summary TEXT NOT NULL,
    root_cause_details TEXT NOT NULL,
    fix_summary TEXT NOT NULL,
    repository_url TEXT,
    pull_request_id VARCHAR(64),
    commit_hash VARCHAR(64)
);

CREATE INDEX IF NOT EXISTS idx_findings_incident ON incident_technical_findings(incident_id);

-- 12. Incident Updates (Append-only)
CREATE TABLE IF NOT EXISTS incident_updates (
    id UUID PRIMARY KEY,
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ NOT NULL,
    title VARCHAR(255) NOT NULL,
    summary TEXT NOT NULL,
    source_id UUID REFERENCES incident_sources(id) ON DELETE SET NULL,
    recovery_state JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_updates_incident_ts ON incident_updates(incident_id, timestamp DESC);

-- 13. Incident Graph Nodes
CREATE TABLE IF NOT EXISTS incident_graph_nodes (
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    node_id VARCHAR(255) NOT NULL,
    label VARCHAR(255) NOT NULL,
    node_type VARCHAR(64) NOT NULL,
    chain VARCHAR(32),
    metadata JSONB,
    PRIMARY KEY (incident_id, node_id)
);

-- 14. Incident Graph Edges (Foreign keys prevent dangling edges)
CREATE TABLE IF NOT EXISTS incident_graph_edges (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    source_node_id VARCHAR(255) NOT NULL,
    target_node_id VARCHAR(255) NOT NULL,
    relationship VARCHAR(64) NOT NULL,
    confidence VARCHAR(64) NOT NULL,
    FOREIGN KEY (incident_id, source_node_id) REFERENCES incident_graph_nodes(incident_id, node_id) ON DELETE CASCADE,
    FOREIGN KEY (incident_id, target_node_id) REFERENCES incident_graph_nodes(incident_id, node_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_graph_edges_incident ON incident_graph_edges(incident_id);

-- 15. Incident Watch Targets
CREATE TABLE IF NOT EXISTS incident_watch_targets (
    id UUID PRIMARY KEY,
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    case_id VARCHAR(64) NOT NULL,
    target_kind VARCHAR(64) NOT NULL,
    txid VARCHAR(64),
    vout INTEGER,
    script_pubkey TEXT,
    address VARCHAR(128),
    classification VARCHAR(64) NOT NULL,
    source TEXT NOT NULL DEFAULT '',
    evidence_id UUID REFERENCES incident_evidence(id) ON DELETE SET NULL,
    label VARCHAR(255),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    kind_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_watch_targets_incident ON incident_watch_targets(incident_id);
CREATE INDEX IF NOT EXISTS idx_watch_targets_kind ON incident_watch_targets(target_kind);
CREATE UNIQUE INDEX IF NOT EXISTS uq_watch_targets_outpoint 
    ON incident_watch_targets(incident_id, txid, vout) 
    WHERE txid IS NOT NULL AND vout IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_watch_targets_address 
    ON incident_watch_targets(incident_id, address) 
    WHERE address IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_watch_targets_tx 
    ON incident_watch_targets(incident_id, txid) 
    WHERE target_kind = 'TRANSACTION' AND txid IS NOT NULL;

-- 16. Incident Activities
CREATE TABLE IF NOT EXISTS incident_activities (
    id UUID PRIMARY KEY,
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    case_id VARCHAR(64) NOT NULL,
    activity_type VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    trigger_txid VARCHAR(64),
    block_height BIGINT,
    block_hash VARCHAR(64),
    value_sats BIGINT CHECK (value_sats IS NULL OR value_sats >= 0),
    watch_target_id UUID NOT NULL REFERENCES incident_watch_targets(id) ON DELETE CASCADE,
    confidence VARCHAR(64) NOT NULL,
    correlation_strength VARCHAR(64) NOT NULL,
    source JSONB NOT NULL DEFAULT '{}'::jsonb,
    evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
    description TEXT NOT NULL,
    details JSONB,
    dedup_key VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_incident_activities_incident_time 
    ON incident_activities(incident_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_incident_activities_trigger_txid 
    ON incident_activities(trigger_txid) 
    WHERE trigger_txid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_incident_activities_status 
    ON incident_activities(status);
CREATE INDEX IF NOT EXISTS idx_incident_activities_dedup 
    ON incident_activities(dedup_key);

-- 17. Incident Alerts
CREATE TABLE IF NOT EXISTS incident_alerts (
    id UUID PRIMARY KEY,
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    case_id VARCHAR(64) NOT NULL,
    incident_title VARCHAR(255) NOT NULL,
    activity_id UUID NOT NULL REFERENCES incident_activities(id) ON DELETE CASCADE,
    severity VARCHAR(32) NOT NULL,
    title VARCHAR(255) NOT NULL,
    summary TEXT NOT NULL,
    confidence VARCHAR(64) NOT NULL,
    correlation_strength VARCHAR(64) NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    value_sats BIGINT CHECK (value_sats IS NULL OR value_sats >= 0),
    trigger_txid VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_incident_time 
    ON incident_alerts(incident_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_severity 
    ON incident_alerts(severity);
CREATE INDEX IF NOT EXISTS idx_alerts_activity 
    ON incident_alerts(activity_id);
