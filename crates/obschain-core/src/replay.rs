use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Operational status of a historical replay job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReplayJobStatus {
    Pending,
    Running,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

impl ReplayJobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    pub fn is_active(&self) -> bool {
        matches!(self, Self::Running | Self::Pending)
    }
}

/// Explicit observation provenance mode distinguishing live real-time observation
/// from historical block replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ObservationMode {
    #[default]
    Live,
    HistoricalReplay,
}

impl ObservationMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::HistoricalReplay => "historical_replay",
        }
    }
}

/// Execution and temporal context attached to an observation.
/// Enables deterministic historical replay time semantics without wall-clock skew.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObservationContext {
    pub mode: ObservationMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_job_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub historical_height: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub historical_block_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub historical_timestamp: Option<DateTime<Utc>>,
    pub network: String,
}

impl ObservationContext {
    pub fn live(network: impl Into<String>) -> Self {
        Self {
            mode: ObservationMode::Live,
            replay_job_id: None,
            historical_height: None,
            historical_block_hash: None,
            historical_timestamp: None,
            network: network.into(),
        }
    }

    pub fn historical_replay(
        job_id: Uuid,
        height: u64,
        block_hash: impl Into<String>,
        timestamp: DateTime<Utc>,
        network: impl Into<String>,
    ) -> Self {
        Self {
            mode: ObservationMode::HistoricalReplay,
            replay_job_id: Some(job_id),
            historical_height: Some(height),
            historical_block_hash: Some(block_hash.into()),
            historical_timestamp: Some(timestamp),
            network: network.into(),
        }
    }
}

/// Strongly typed historical block replay job tracking progress, configuration, and state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplayJob {
    pub id: Uuid,
    pub network: String,
    pub start_height: u64,
    pub end_height: u64,
    pub current_height: u64,
    pub status: ReplayJobStatus,
    pub source_type: String,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    pub blocks_processed: u64,
    pub transactions_processed: u64,
    pub events_generated: u64,
    pub error_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl ReplayJob {
    pub fn new(
        network: impl Into<String>,
        start_height: u64,
        end_height: u64,
        source_type: impl Into<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            network: network.into(),
            start_height,
            end_height,
            current_height: start_height,
            status: ReplayJobStatus::Pending,
            source_type: source_type.into(),
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            blocks_processed: 0,
            transactions_processed: 0,
            events_generated: 0,
            error_count: 0,
            last_error: None,
        }
    }

    pub fn total_blocks(&self) -> u64 {
        if self.end_height >= self.start_height {
            self.end_height - self.start_height + 1
        } else {
            0
        }
    }

    pub fn progress_percentage(&self) -> f64 {
        let total = self.total_blocks();
        if total == 0 {
            return 100.0;
        }
        ((self.blocks_processed as f64) / (total as f64) * 100.0).clamp(0.0, 100.0)
    }
}

/// Durable checkpoint for a replay job allowing crash recovery and resumption.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplayCheckpoint {
    pub id: Uuid,
    pub job_id: Uuid,
    pub completed_height: u64,
    pub blocks_processed: u64,
    pub transactions_processed: u64,
    pub events_generated: u64,
    pub checkpointed_at: DateTime<Utc>,
}

impl ReplayCheckpoint {
    pub fn new(
        job_id: Uuid,
        completed_height: u64,
        blocks_processed: u64,
        transactions_processed: u64,
        events_generated: u64,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            job_id,
            completed_height,
            blocks_processed,
            transactions_processed,
            events_generated,
            checkpointed_at: Utc::now(),
        }
    }
}
