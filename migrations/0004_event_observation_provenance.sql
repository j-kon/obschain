-- Migration: 0004_event_observation_provenance.sql
-- Phase 6A.1: Canonical Event & Observation Provenance Hardening
-- Separates canonical ChainEvent ('what happened') from EventObservation ('how / when ObsChain learned about it').

-- 1. Create event_observations table
CREATE TABLE IF NOT EXISTS event_observations (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL REFERENCES chain_events(id) ON DELETE CASCADE,
    observation_mode VARCHAR(32) NOT NULL DEFAULT 'live',
    source JSONB NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    bitcoin_time TIMESTAMPTZ,
    replay_job_id UUID NULL REFERENCES replay_jobs(id) ON DELETE SET NULL,
    block_height BIGINT,
    block_hash VARCHAR(64),
    witness JSONB NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Add explicit event_time and first_observed_at columns to chain_events
ALTER TABLE chain_events ADD COLUMN IF NOT EXISTS event_time TIMESTAMPTZ;
ALTER TABLE chain_events ADD COLUMN IF NOT EXISTS first_observed_at TIMESTAMPTZ;

-- Backfill event_time and first_observed_at from detected_at
UPDATE chain_events
SET event_time = detected_at,
    first_observed_at = detected_at
WHERE event_time IS NULL;

ALTER TABLE chain_events ALTER COLUMN event_time SET DEFAULT NOW();
ALTER TABLE chain_events ALTER COLUMN event_time SET NOT NULL;
ALTER TABLE chain_events ALTER COLUMN first_observed_at SET DEFAULT NOW();
ALTER TABLE chain_events ALTER COLUMN first_observed_at SET NOT NULL;

-- 3. Add provenance telemetry metrics to replay_jobs and replay_checkpoints
ALTER TABLE replay_jobs ADD COLUMN IF NOT EXISTS events_created BIGINT NOT NULL DEFAULT 0;
ALTER TABLE replay_jobs ADD COLUMN IF NOT EXISTS events_existing BIGINT NOT NULL DEFAULT 0;
ALTER TABLE replay_jobs ADD COLUMN IF NOT EXISTS observations_recorded BIGINT NOT NULL DEFAULT 0;

ALTER TABLE replay_checkpoints ADD COLUMN IF NOT EXISTS events_created BIGINT NOT NULL DEFAULT 0;
ALTER TABLE replay_checkpoints ADD COLUMN IF NOT EXISTS events_existing BIGINT NOT NULL DEFAULT 0;
ALTER TABLE replay_checkpoints ADD COLUMN IF NOT EXISTS observations_recorded BIGINT NOT NULL DEFAULT 0;

-- 4. Migrate existing chain_events provenance rows into event_observations
INSERT INTO event_observations (
    id,
    event_id,
    observation_mode,
    source,
    observed_at,
    bitcoin_time,
    replay_job_id,
    block_height,
    block_hash,
    created_at
)
SELECT
    gen_random_uuid(),
    id,
    COALESCE(observation_mode, 'live'),
    COALESCE(source, '{"provider": "unknown", "transport": "unknown"}'::jsonb),
    COALESCE(detected_at, NOW()),
    detected_at,
    replay_job_id,
    block_height,
    block_hash,
    NOW()
FROM chain_events
WHERE NOT EXISTS (
    SELECT 1 FROM event_observations eo WHERE eo.event_id = chain_events.id
);

-- 5. Indexes for fast research and observation queries
CREATE INDEX IF NOT EXISTS idx_event_observations_event_id
    ON event_observations(event_id);

CREATE INDEX IF NOT EXISTS idx_event_observations_mode
    ON event_observations(observation_mode);

CREATE INDEX IF NOT EXISTS idx_event_observations_event_mode
    ON event_observations(event_id, observation_mode);

CREATE INDEX IF NOT EXISTS idx_event_observations_replay_job
    ON event_observations(replay_job_id)
    WHERE replay_job_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_event_observations_event_job
    ON event_observations(event_id, replay_job_id)
    WHERE replay_job_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_event_observations_observed_at
    ON event_observations(observed_at DESC);

-- Partial unique indexes to prevent duplicate observation records:
-- A. Replay: one observation per (event_id, replay_job_id)
CREATE UNIQUE INDEX IF NOT EXISTS idx_event_obs_replay_uniq
    ON event_observations (event_id, replay_job_id)
    WHERE replay_job_id IS NOT NULL;

-- B. Live: one observation per (event_id, provider, transport)
CREATE UNIQUE INDEX IF NOT EXISTS idx_event_obs_live_source_uniq
    ON event_observations (event_id, observation_mode, (source->>'provider'), (source->>'transport'))
    WHERE replay_job_id IS NULL;
