use serde::{Deserialize, Serialize};

use crate::source::{IngestSource, IngestionSourceType};

/// Subscription options for mempool.space WebSocket stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MempoolWsSubscription {
    pub track_mempool_blocks: bool,
    pub track_live_blocks: bool,
    pub track_transactions: bool,
}

impl Default for MempoolWsSubscription {
    fn default() -> Self {
        Self {
            track_mempool_blocks: true,
            track_live_blocks: true,
            track_transactions: false,
        }
    }
}

/// Incoming WebSocket message envelopes from mempool.space.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MempoolWsMessage {
    Block(MempoolWsBlock),
    Conversions(serde_json::Value),
    General(serde_json::Value),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MempoolWsBlock {
    pub id: String,
    pub height: u64,
    pub timestamp: i64,
    pub tx_count: usize,
    pub size: u64,
}

/// WebSocket client interface for mempool.space.
pub struct MempoolWsClient {
    pub endpoint_url: String,
}

impl MempoolWsClient {
    pub fn new(endpoint_url: impl Into<String>) -> Self {
        Self {
            endpoint_url: endpoint_url.into(),
        }
    }
}

impl IngestSource for MempoolWsClient {
    fn source_type(&self) -> IngestionSourceType {
        IngestionSourceType::MempoolSpaceWs
    }

    fn name(&self) -> &'static str {
        "mempool_space_ws"
    }

    fn is_healthy(&self) -> bool {
        true
    }
}
