use std::time::Duration;

use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use thiserror::Error;
use tracing::warn;

use crate::{
    mempool_types::{
        MempoolBlock, MempoolRecentTx, MempoolRecommendedFees, MempoolStats, MempoolTx,
    },
    source::{IngestSource, IngestionSourceType},
};

#[derive(Debug, Error)]
pub enum MempoolRestError {
    #[error("HTTP transport error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("HTTP error status {status}: {message}")]
    Status { status: u16, message: String },

    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Response body exceeded maximum allowed limit ({limit} bytes)")]
    ResponseTooLarge { limit: usize },

    #[error("Failed to parse JSON response: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Parse error: {0}")]
    Parse(String),
}

/// Configuration for the Mempool.space REST client.
#[derive(Debug, Clone)]
pub struct MempoolRestConfig {
    pub base_url: String,
    pub timeout: Duration,
    pub connect_timeout: Duration,
    pub max_retries: u32,
    pub max_response_bytes: usize,
}

impl Default for MempoolRestConfig {
    fn default() -> Self {
        Self {
            base_url: "https://mempool.space/api".to_string(),
            timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(5),
            max_retries: 3,
            max_response_bytes: 10 * 1024 * 1024, // 10 MB limit
        }
    }
}

/// Production-quality asynchronous REST client for mempool.space API.
#[derive(Clone)]
pub struct MempoolRestClient {
    config: MempoolRestConfig,
    client: Client,
}

impl MempoolRestClient {
    pub fn new(config: MempoolRestConfig) -> Result<Self, MempoolRestError> {
        let client = Client::builder()
            .timeout(config.timeout)
            .connect_timeout(config.connect_timeout)
            .build()?;
        Ok(Self { config, client })
    }

    pub fn base_url(&self) -> &str {
        &self.config.base_url
    }

    /// Internal request executor with retry logic and bounded body size limit.
    async fn execute_get_with_retry<T: DeserializeOwned>(
        &self,
        endpoint: &str,
    ) -> Result<T, MempoolRestError> {
        let url = format!("{}{}", self.config.base_url, endpoint);
        let mut attempts = 0;
        let mut delay = Duration::from_millis(200);

        loop {
            attempts += 1;
            match self.client.get(&url).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status == StatusCode::NOT_FOUND {
                        return Err(MempoolRestError::NotFound(endpoint.to_string()));
                    }

                    if !status.is_success() {
                        let err_text = resp.text().await.unwrap_or_default();
                        if (status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS)
                            && attempts < self.config.max_retries
                        {
                            warn!(
                                status = %status,
                                endpoint = %endpoint,
                                attempt = attempts,
                                "Retrying mempool.space request after transient error"
                            );
                            tokio::time::sleep(delay).await;
                            delay = delay.saturating_mul(2);
                            continue;
                        }
                        return Err(MempoolRestError::Status {
                            status: status.as_u16(),
                            message: err_text,
                        });
                    }

                    // Enforce response body size limit
                    if let Some(content_length) = resp.content_length() {
                        if content_length as usize > self.config.max_response_bytes {
                            return Err(MempoolRestError::ResponseTooLarge {
                                limit: self.config.max_response_bytes,
                            });
                        }
                    }

                    let bytes = resp.bytes().await?;
                    if bytes.len() > self.config.max_response_bytes {
                        return Err(MempoolRestError::ResponseTooLarge {
                            limit: self.config.max_response_bytes,
                        });
                    }

                    let parsed: T = serde_json::from_slice(&bytes)?;
                    return Ok(parsed);
                }
                Err(err) => {
                    if attempts < self.config.max_retries && (err.is_timeout() || err.is_connect())
                    {
                        warn!(
                            error = %err,
                            endpoint = %endpoint,
                            attempt = attempts,
                            "Retrying mempool.space request after network timeout"
                        );
                        tokio::time::sleep(delay).await;
                        delay = delay.saturating_mul(2);
                        continue;
                    }
                    return Err(MempoolRestError::Http(err));
                }
            }
        }
    }

    /// Fetches the latest tip height: `GET /blocks/tip/height`
    pub async fn get_tip_height(&self) -> Result<u64, MempoolRestError> {
        let url = format!("{}/blocks/tip/height", self.config.base_url);
        let resp = self.client.get(&url).send().await?;
        if !resp.status().is_success() {
            return Err(MempoolRestError::Status {
                status: resp.status().as_u16(),
                message: resp.text().await.unwrap_or_default(),
            });
        }
        let text = resp.text().await?;
        text.trim()
            .parse::<u64>()
            .map_err(|e| MempoolRestError::Parse(e.to_string()))
    }

    /// Fetches the latest tip block hash: `GET /blocks/tip/hash`
    pub async fn get_tip_hash(&self) -> Result<String, MempoolRestError> {
        let url = format!("{}/blocks/tip/hash", self.config.base_url);
        let resp = self.client.get(&url).send().await?;
        if !resp.status().is_success() {
            return Err(MempoolRestError::Status {
                status: resp.status().as_u16(),
                message: resp.text().await.unwrap_or_default(),
            });
        }
        let text = resp.text().await?;
        Ok(text.trim().to_string())
    }

    /// Fetches block details by hash: `GET /block/:hash`
    pub async fn get_block(&self, hash: &str) -> Result<MempoolBlock, MempoolRestError> {
        let endpoint = format!("/block/{}", hash);
        self.execute_get_with_retry(&endpoint).await
    }

    /// Fetches transactions in a block: `GET /block/:hash/txs`
    pub async fn get_block_txs(&self, hash: &str) -> Result<Vec<MempoolTx>, MempoolRestError> {
        let endpoint = format!("/block/{}/txs", hash);
        self.execute_get_with_retry(&endpoint).await
    }

    /// Fetches transaction by txid: `GET /tx/:txid`
    pub async fn get_tx(&self, txid: &str) -> Result<MempoolTx, MempoolRestError> {
        let endpoint = format!("/tx/{}", txid);
        self.execute_get_with_retry(&endpoint).await
    }

    /// Fetches current mempool backlog statistics: `GET /mempool`
    pub async fn get_mempool(&self) -> Result<MempoolStats, MempoolRestError> {
        self.execute_get_with_retry("/mempool").await
    }

    /// Fetches recent mempool transactions: `GET /mempool/recent`
    pub async fn get_mempool_recent(&self) -> Result<Vec<MempoolRecentTx>, MempoolRestError> {
        self.execute_get_with_retry("/mempool/recent").await
    }

    /// Fetches recommended fee estimates: `GET /v1/fees/recommended`
    pub async fn get_recommended_fees(&self) -> Result<MempoolRecommendedFees, MempoolRestError> {
        self.execute_get_with_retry("/v1/fees/recommended").await
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Json, Router};
    use tokio::net::TcpListener;

    async fn start_mock_server() -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new()
            .route("/blocks/tip/height", get(|| async { "885000\n" }))
            .route(
                "/blocks/tip/hash",
                get(|| async { "00000000000000000001099645903b6e82810a950bc490d1bfca722a5fbef8f3" }),
            )
            .route(
                "/block/00000000000000000001099645903b6e82810a950bc490d1bfca722a5fbef8f3",
                get(|| async {
                    Json(serde_json::json!({
                        "id": "00000000000000000001099645903b6e82810a950bc490d1bfca722a5fbef8f3",
                        "height": 885000,
                        "version": 536870912,
                        "timestamp": 1735000000,
                        "tx_count": 2500,
                        "size": 1600000,
                        "weight": 3990000
                    }))
                }),
            )
            .route(
                "/tx/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                get(|| async {
                    Json(serde_json::json!({
                        "txid": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                        "version": 2,
                        "locktime": 0,
                        "vin": [],
                        "vout": [],
                        "size": 225,
                        "weight": 560,
                        "fee": 12000,
                        "status": {
                            "confirmed": true,
                            "block_height": 885000,
                            "block_hash": "00000000000000000001099645903b6e82810a950bc490d1bfca722a5fbef8f3",
                            "block_time": 1735000000
                        }
                    }))
                }),
            )
            .route(
                "/mempool",
                get(|| async {
                    Json(serde_json::json!({
                        "count": 120000,
                        "vsize": 150000000,
                        "total_fee": 980000000
                    }))
                }),
            )
            .route(
                "/mempool/recent",
                get(|| async {
                    Json(serde_json::json!([
                        {
                            "txid": "1111111111111111111111111111111111111111111111111111111111111111",
                            "fee": 1500,
                            "vsize": 140,
                            "value": 50000000
                        }
                    ]))
                }),
            )
            .route(
                "/v1/fees/recommended",
                get(|| async {
                    Json(serde_json::json!({
                        "fastestFee": 25,
                        "halfHourFee": 18,
                        "hourFee": 12,
                        "minimumFee": 5
                    }))
                }),
            )
            .route(
                "/server-error",
                get(|| async { (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error") }),
            )
            .route(
                "/malformed-json",
                get(|| async { "{\"broken\": json" }),
            )
            .route(
                "/timeout",
                get(|| async {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    "delayed response"
                }),
            );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        (format!("http://{}", addr), handle)
    }

    #[tokio::test]
    async fn test_rest_client_successful_endpoints() {
        let (base_url, _handle) = start_mock_server().await;
        let config = MempoolRestConfig {
            base_url,
            timeout: Duration::from_secs(2),
            connect_timeout: Duration::from_secs(1),
            max_retries: 1,
            max_response_bytes: 1024 * 1024,
        };
        let client = MempoolRestClient::new(config).unwrap();

        // 1. Tip height
        let height = client.get_tip_height().await.unwrap();
        assert_eq!(height, 885000);

        // 2. Tip hash
        let hash = client.get_tip_hash().await.unwrap();
        assert_eq!(
            hash,
            "00000000000000000001099645903b6e82810a950bc490d1bfca722a5fbef8f3"
        );

        // 3. Block
        let block = client.get_block(&hash).await.unwrap();
        assert_eq!(block.height, 885000);
        assert_eq!(block.tx_count, 2500);

        // 4. Tx
        let tx = client
            .get_tx("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
            .await
            .unwrap();
        assert_eq!(tx.fee, Some(12000));
        assert!(tx.status.confirmed);

        // 5. Mempool stats
        let stats = client.get_mempool().await.unwrap();
        assert_eq!(stats.count, 120000);
        assert_eq!(stats.total_fee, 980000000);

        // 6. Recent mempool txs
        let recent = client.get_mempool_recent().await.unwrap();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].fee, 1500);

        // 7. Recommended fees
        let fees = client.get_recommended_fees().await.unwrap();
        assert_eq!(fees.fastest_fee, 25);
        assert_eq!(fees.minimum_fee, 5);
    }

    #[tokio::test]
    async fn test_rest_client_not_found_error() {
        let (base_url, _handle) = start_mock_server().await;
        let config = MempoolRestConfig {
            base_url,
            timeout: Duration::from_secs(2),
            connect_timeout: Duration::from_secs(1),
            max_retries: 1,
            max_response_bytes: 1024 * 1024,
        };
        let client = MempoolRestClient::new(config).unwrap();

        let res = client.get_block("nonexistent").await;
        assert!(matches!(res, Err(MempoolRestError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_rest_client_server_error() {
        let (base_url, _handle) = start_mock_server().await;
        let config = MempoolRestConfig {
            base_url,
            timeout: Duration::from_secs(2),
            connect_timeout: Duration::from_secs(1),
            max_retries: 1,
            max_response_bytes: 1024 * 1024,
        };
        let client = MempoolRestClient::new(config).unwrap();

        let res: Result<serde_json::Value, _> =
            client.execute_get_with_retry("/server-error").await;
        assert!(matches!(
            res,
            Err(MempoolRestError::Status { status: 500, .. })
        ));
    }

    #[tokio::test]
    async fn test_rest_client_malformed_json_error() {
        let (base_url, _handle) = start_mock_server().await;
        let config = MempoolRestConfig {
            base_url,
            timeout: Duration::from_secs(2),
            connect_timeout: Duration::from_secs(1),
            max_retries: 1,
            max_response_bytes: 1024 * 1024,
        };
        let client = MempoolRestClient::new(config).unwrap();

        let res: Result<serde_json::Value, _> =
            client.execute_get_with_retry("/malformed-json").await;
        assert!(matches!(res, Err(MempoolRestError::Json(_))));
    }

    #[tokio::test]
    async fn test_rest_client_timeout() {
        let (base_url, _handle) = start_mock_server().await;
        let config = MempoolRestConfig {
            base_url,
            timeout: Duration::from_millis(50), // very short timeout
            connect_timeout: Duration::from_millis(50),
            max_retries: 1,
            max_response_bytes: 1024 * 1024,
        };
        let client = MempoolRestClient::new(config).unwrap();

        let res: Result<serde_json::Value, _> = client.execute_get_with_retry("/timeout").await;
        assert!(matches!(res, Err(MempoolRestError::Http(_))));
    }
}
