use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::source::{IngestSource, IngestionSourceType};

#[derive(Debug, Error)]
pub enum MempoolRestError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("API parse error: {0}")]
    Parse(String),

    #[error("Rate limited or server error (status {0})")]
    Status(u16),
}

/// Client configuration for mempool.space REST API.
#[derive(Debug, Clone)]
pub struct MempoolRestConfig {
    pub base_url: String,
    pub timeout: Duration,
}

impl Default for MempoolRestConfig {
    fn default() -> Self {
        Self {
            base_url: "https://mempool.space/api".to_string(),
            timeout: Duration::from_secs(10),
        }
    }
}

/// REST ingestion client for public or self-hosted mempool.space instances.
pub struct MempoolRestClient {
    config: MempoolRestConfig,
    client: Client,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MempoolBlockSummary {
    pub id: String,
    pub height: u64,
    pub version: u32,
    pub timestamp: i64,
    pub tx_count: usize,
    pub size: u64,
    pub weight: u64,
    pub previousblockhash: Option<String>,
    pub difficulty: Option<f64>,
}

impl MempoolRestClient {
    pub fn new(config: MempoolRestConfig) -> Result<Self, MempoolRestError> {
        let client = Client::builder().timeout(config.timeout).build()?;
        Ok(Self { config, client })
    }

    pub fn base_url(&self) -> &str {
        &self.config.base_url
    }

    /// Fetches the latest tip height from mempool.space.
    pub async fn fetch_tip_height(&self) -> Result<u64, MempoolRestError> {
        let url = format!("{}/blocks/tip/height", self.config.base_url);
        let resp = self.client.get(&url).send().await?;
        if !resp.status().is_success() {
            return Err(MempoolRestError::Status(resp.status().as_u16()));
        }
        let text = resp.text().await?;
        text.trim()
            .parse::<u64>()
            .map_err(|e| MempoolRestError::Parse(e.to_string()))
    }
}

impl IngestSource for MempoolRestClient {
    fn source_type(&self) -> IngestionSourceType {
        IngestionSourceType::MempoolSpaceRest
    }

    fn name(&self) -> &'static str {
        "mempool_space_rest"
    }

    fn is_healthy(&self) -> bool {
        true
    }
}
