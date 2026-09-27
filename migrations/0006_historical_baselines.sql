-- Migration 0006: Historical Baselines, Rarity & Impact Intelligence
-- Supports deterministic statistical distributions over canonical Bitcoin events

-- 1. Baseline generation job runs
CREATE TABLE IF NOT EXISTS baseline_runs (
    id UUID PRIMARY KEY,
    network VARCHAR(32) NOT NULL DEFAULT 'mainnet',
    start_height BIGINT NOT NULL,
    end_height BIGINT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    algorithm_version VARCHAR(64) NOT NULL DEFAULT 'obschain-baseline-v1',
    canonical_event_count BIGINT NOT NULL DEFAULT 0,
    error_message TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for finding the latest compatible baseline for live evaluation
CREATE INDEX IF NOT EXISTS idx_baseline_runs_network_status_end 
ON baseline_runs(network, status, end_height DESC);

-- 2. Statistical distributions for each metric under a baseline run
CREATE TABLE IF NOT EXISTS baseline_distributions (
    id UUID PRIMARY KEY,
    baseline_run_id UUID NOT NULL REFERENCES baseline_runs(id) ON DELETE CASCADE,
    event_type VARCHAR(64) NOT NULL,
    metric VARCHAR(64) NOT NULL,
    unit VARCHAR(32) NOT NULL,
    sample_count BIGINT NOT NULL,
    candidate_count BIGINT NOT NULL DEFAULT 0,
    missing_count BIGINT NOT NULL DEFAULT 0,
    coverage_ratio DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    minimum NUMERIC(38, 4) NOT NULL,
    maximum NUMERIC(38, 4) NOT NULL,
    mean NUMERIC(38, 4) NOT NULL,
    p50 NUMERIC(38, 4) NOT NULL,
    p75 NUMERIC(38, 4) NOT NULL,
    p90 NUMERIC(38, 4) NOT NULL,
    p95 NUMERIC(38, 4) NOT NULL,
    p99 NUMERIC(38, 4) NOT NULL,
    p999 NUMERIC(38, 4) NOT NULL,
    quality VARCHAR(32) NOT NULL,
    samples_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_baseline_dist_run_type_metric UNIQUE(baseline_run_id, event_type, metric)
);

CREATE INDEX IF NOT EXISTS idx_baseline_dist_event_metric 
ON baseline_distributions(event_type, metric);

-- 3. Evaluated rarity records for canonical events
CREATE TABLE IF NOT EXISTS event_rarity (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL REFERENCES chain_events(id) ON DELETE CASCADE,
    baseline_run_id UUID NOT NULL REFERENCES baseline_runs(id) ON DELETE CASCADE,
    event_type VARCHAR(64) NOT NULL,
    metric VARCHAR(64) NOT NULL,
    value_numeric NUMERIC(38, 4) NOT NULL,
    value_text VARCHAR(128) NOT NULL,
    percentile DOUBLE PRECISION,
    rarity_band VARCHAR(32) NOT NULL,
    population_size BIGINT NOT NULL,
    tail_count BIGINT NOT NULL,
    evaluation_mode VARCHAR(32) NOT NULL DEFAULT 'RETROSPECTIVE',
    impact_score DOUBLE PRECISION,
    impact_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_event_rarity_event_run_metric UNIQUE(event_id, baseline_run_id, metric)
);

CREATE INDEX IF NOT EXISTS idx_event_rarity_event_id 
ON event_rarity(event_id);

-- Performance index on chain_events for fast baseline extraction queries
CREATE INDEX IF NOT EXISTS idx_chain_events_baseline_lookup 
ON chain_events(event_type, block_height) 
WHERE block_height IS NOT NULL;
