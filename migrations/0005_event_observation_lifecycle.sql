-- Migration: 0005_event_observation_lifecycle.sql
-- Phase 6A.2: Observation Lifecycle Integrity & Test Baseline Verification
-- Allows multiple meaningful observations from the SAME provider/transport across lifecycle stages
-- (e.g. MempoolSeen, Confirmed, ReorgedOut, Witnessed, HistoricalReplay) without collapsing observations
-- or duplicating canonical ChainEvent records.

-- 1. Add lifecycle columns to event_observations
ALTER TABLE event_observations ADD COLUMN IF NOT EXISTS observation_kind VARCHAR(32) NOT NULL DEFAULT 'WITNESSED';
ALTER TABLE event_observations ADD COLUMN IF NOT EXISTS confirmation_status VARCHAR(32);
ALTER TABLE event_observations ADD COLUMN IF NOT EXISTS source_sequence BIGINT;
ALTER TABLE event_observations ADD COLUMN IF NOT EXISTS mempool_sequence BIGINT;

-- 2. Backfill observation_kind for existing records
UPDATE event_observations
SET observation_kind = CASE
    WHEN replay_job_id IS NOT NULL THEN 'HISTORICAL_REPLAY'
    WHEN block_hash IS NOT NULL THEN 'CONFIRMED'
    ELSE 'WITNESSED'
END
WHERE observation_kind = 'WITNESSED';

-- Backfill confirmation_status where appropriate
UPDATE event_observations
SET confirmation_status = 'confirmed'
WHERE observation_kind IN ('CONFIRMED', 'HISTORICAL_REPLAY')
  AND confirmation_status IS NULL;

-- 3. Replace overly broad live source unique index
DROP INDEX IF EXISTS idx_event_obs_live_source_uniq;

-- 4. Create new lifecycle-aware partial unique index
-- Distinguishes live observations by event_id, observation_mode, provider, transport,
-- observation_kind, and semantic discriminator (block_hash, source_sequence, mempool_sequence, or confirmation_status).
CREATE UNIQUE INDEX IF NOT EXISTS idx_event_obs_live_lifecycle_uniq
    ON event_observations (
        event_id,
        observation_mode,
        (source->>'provider'),
        (source->>'transport'),
        observation_kind,
        COALESCE(block_hash, source_sequence::text, mempool_sequence::text, confirmation_status, '')
    )
    WHERE replay_job_id IS NULL;

-- 5. Additional index for querying observations by kind
CREATE INDEX IF NOT EXISTS idx_event_observations_kind
    ON event_observations(observation_kind);
