-- ObsChain Core Schema Migration 0001

CREATE TABLE IF NOT EXISTS events (
    id UUID PRIMARY KEY,
    event_type VARCHAR(64) NOT NULL,
    severity VARCHAR(32) NOT NULL,
    confidence VARCHAR(32) NOT NULL,
    title VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    block_height BIGINT,
    block_hash VARCHAR(64),
    txid VARCHAR(64),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_events_detected_at ON events(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_severity ON events(severity);
CREATE INDEX IF NOT EXISTS idx_events_txid ON events(txid) WHERE txid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_block_height ON events(block_height) WHERE block_height IS NOT NULL;

CREATE TABLE IF NOT EXISTS incidents (
    id UUID PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    summary TEXT NOT NULL,
    status VARCHAR(32) NOT NULL,
    severity VARCHAR(32) NOT NULL,
    total_btc_affected DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    total_btc_recovered DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    first_observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    facts JSONB NOT NULL DEFAULT '[]'::jsonb,
    reported_claims JSONB NOT NULL DEFAULT '[]'::jsonb,
    unverified_claims JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_incidents_status ON incidents(status);
CREATE INDEX IF NOT EXISTS idx_incidents_severity ON incidents(severity);

CREATE TABLE IF NOT EXISTS incident_transactions (
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    txid VARCHAR(64) NOT NULL,
    note TEXT,
    PRIMARY KEY (incident_id, txid)
);

CREATE TABLE IF NOT EXISTS incident_blocks (
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    block_height BIGINT NOT NULL,
    block_hash VARCHAR(64) NOT NULL,
    note TEXT,
    PRIMARY KEY (incident_id, block_height)
);

CREATE TABLE IF NOT EXISTS evidence (
    id UUID PRIMARY KEY,
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    evidence_type VARCHAR(64) NOT NULL,
    classification VARCHAR(64) NOT NULL,
    description TEXT NOT NULL,
    reference TEXT NOT NULL,
    raw_data JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_evidence_incident ON evidence(incident_id);

CREATE TABLE IF NOT EXISTS timeline_events (
    id UUID PRIMARY KEY,
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    evidence_id UUID REFERENCES evidence(id) ON DELETE SET NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    title VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    classification VARCHAR(64) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_timeline_incident ON timeline_events(incident_id, timestamp ASC);

CREATE TABLE IF NOT EXISTS sources (
    id UUID PRIMARY KEY,
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    url TEXT,
    reliability_score REAL NOT NULL DEFAULT 0.5,
    published_at TIMESTAMPTZ
);
