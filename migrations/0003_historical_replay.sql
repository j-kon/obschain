-- Migration 0003: Historical Replay & Research Engine (Phase 6A)
-- Adds replay provenance columns to chain_events and introduces replay_jobs and replay_checkpoints tables.

-- 1. Extend chain_events with observation provenance
ALTER TABLE chain_events ADD COLUMN IF NOT EXISTS observation_mode VARCHAR(32) NOT NULL DEFAULT 'live';
ALTER TABLE chain_events ADD COLUMN IF NOT EXISTS replay_job_id UUID;

CREATE INDEX IF NOT EXISTS idx_chain_events_observation_mode ON chain_events(observation_mode);
CREATE INDEX IF NOT EXISTS idx_chain_events_replay_job_id ON chain_events(replay_job_id) WHERE replay_job_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_chain_events_type_height ON chain_events(event_type, block_height) WHERE block_height IS NOT NULL;

-- 2. Replay Jobs
CREATE TABLE IF NOT EXISTS replay_jobs (
    id UUID PRIMARY KEY,
    network VARCHAR(32) NOT NULL,
    start_height BIGINT NOT NULL CHECK (start_height >= 0),
    end_height BIGINT NOT NULL CHECK (end_height >= start_height),
    current_height BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL,
    source_type VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    blocks_processed BIGINT NOT NULL DEFAULT 0 CHECK (blocks_processed >= 0),
    transactions_processed BIGINT NOT NULL DEFAULT 0 CHECK (transactions_processed >= 0),
    events_generated BIGINT NOT NULL DEFAULT 0 CHECK (events_generated >= 0),
    error_count BIGINT NOT NULL DEFAULT 0 CHECK (error_count >= 0),
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_replay_jobs_status ON replay_jobs(status);
CREATE INDEX IF NOT EXISTS idx_replay_jobs_created_at ON replay_jobs(created_at DESC);

-- 3. Replay Checkpoints
CREATE TABLE IF NOT EXISTS replay_checkpoints (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL REFERENCES replay_jobs(id) ON DELETE CASCADE,
    completed_height BIGINT NOT NULL CHECK (completed_height >= 0),
    blocks_processed BIGINT NOT NULL DEFAULT 0 CHECK (blocks_processed >= 0),
    transactions_processed BIGINT NOT NULL DEFAULT 0 CHECK (transactions_processed >= 0),
    events_generated BIGINT NOT NULL DEFAULT 0 CHECK (events_generated >= 0),
    checkpointed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_replay_checkpoints_job_height ON replay_checkpoints(job_id, completed_height DESC);
