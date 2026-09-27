-- Migration 0007: Statistical Correctness Hardening
-- Expands numeric columns to NUMERIC(50, 4) for full u128 safety, adds event_metric_values table,
-- and adds explicit percentile_method and estimated flags to event_rarity.

-- 1. Expand numeric precision on baseline_distributions to NUMERIC(50, 4)
ALTER TABLE baseline_distributions
    ALTER COLUMN minimum TYPE NUMERIC(50, 4),
    ALTER COLUMN maximum TYPE NUMERIC(50, 4),
    ALTER COLUMN mean TYPE NUMERIC(50, 4),
    ALTER COLUMN p50 TYPE NUMERIC(50, 4),
    ALTER COLUMN p75 TYPE NUMERIC(50, 4),
    ALTER COLUMN p90 TYPE NUMERIC(50, 4),
    ALTER COLUMN p95 TYPE NUMERIC(50, 4),
    ALTER COLUMN p99 TYPE NUMERIC(50, 4),
    ALTER COLUMN p999 TYPE NUMERIC(50, 4);

-- 2. Expand numeric precision and add method/estimated columns on event_rarity
ALTER TABLE event_rarity
    ALTER COLUMN value_numeric TYPE NUMERIC(50, 4),
    ADD COLUMN IF NOT EXISTS percentile_method VARCHAR(64) NOT NULL DEFAULT 'EXACT_EMPIRICAL_CDF',
    ADD COLUMN IF NOT EXISTS estimated BOOLEAN NOT NULL DEFAULT FALSE;

-- 3. Normalized event metric storage for canonical events
CREATE TABLE IF NOT EXISTS event_metric_values (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL REFERENCES chain_events(id) ON DELETE CASCADE,
    network VARCHAR(32) NOT NULL DEFAULT 'mainnet',
    event_type VARCHAR(64) NOT NULL,
    metric VARCHAR(64) NOT NULL,
    metric_definition_version VARCHAR(64) NOT NULL DEFAULT 'v1',
    value_numeric NUMERIC(50, 4) NOT NULL,
    metric_scale INTEGER NOT NULL DEFAULT 0,
    block_height BIGINT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_event_metric_version UNIQUE (event_id, metric, metric_definition_version)
);

-- Compound index for exact empirical rank queries:
-- SELECT COUNT(*) FILTER (...) FROM event_metric_values
-- WHERE network = $1 AND event_type = $2 AND metric = $3 AND metric_definition_version = $4 AND block_height BETWEEN $5 AND $6
CREATE INDEX IF NOT EXISTS idx_event_metric_values_query
ON event_metric_values (network, event_type, metric, metric_definition_version, block_height, value_numeric);

CREATE INDEX IF NOT EXISTS idx_event_metric_values_event_id
ON event_metric_values (event_id);
