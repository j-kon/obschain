-- Migration 0008: Statistical Guardrails & Contract Nullability
-- Allows tail_count and percentile_method to be NULL in event_rarity when sample size is insufficient.

ALTER TABLE event_rarity
    ALTER COLUMN tail_count DROP NOT NULL,
    ALTER COLUMN percentile_method DROP NOT NULL;
