use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use reqwest::Client;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::debug;

use crate::source::{IngestSource, IngestionSourceType};

#[derive(Debug, Error)]
pub enum BitcoinRpcError {
    #[error("HTTP transport error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Bitcoin Core JSON-RPC error ({code}): {message}")]
    RpcError { code: i64, message: String },

    #[error("Serialization / Deserialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Authentication failed: {0}")]
    AuthError(String),

    #[error("Invalid RPC URL: {0}")]
    InvalidUrl(String),

    #[error("Node warming up / in initial block download: {0}")]
    NodeWarmingUp(String),

    #[error("Network mismatch: expected {expected}, node reported {reported}")]
    NetworkMismatch { expected: String, reported: String },

    #[error("Missing or unavailable historical transaction data (txindex may be disabled or node is pruned)")]
    TxIndexUnavailable,
}

/// Connection and authentication parameters for Bitcoin Core JSON-RPC.
#[derive(Debug, Clone)]
pub struct BitcoinRpcConfig {
    pub rpc_url: String,
    pub rpc_user: Option<String>,
    pub rpc_password: Option<String>,
    pub cookie_file: Option<PathBuf>,
    pub timeout: Duration,
    pub expected_network: Option<String>,
}

impl Default for BitcoinRpcConfig {
    fn default() -> Self {
        Self {
            rpc_url: "http://127.0.0.1:8332".to_string(),
            rpc_user: None,
            rpc_password: None,
            cookie_file: None,
            timeout: Duration::from_secs(15),
            expected_network: None,
        }
    }
}

/// High-level node capabilities and status inspection model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BitcoinNodeCapabilities {
    pub network: String,
    pub version: u64,
    pub subversion: String,
    pub blocks: u64,
    pub headers: u64,
    pub verification_progress: f64,
    pub initial_block_download: bool,
    pub pruned: bool,
    pub prune_height: Option<u64>,
    pub txindex_available: bool,
    pub zmq_rawtx: bool,
    pub zmq_rawblock: bool,
    pub zmq_sequence: bool,
}

// ---------------------------------------------------------------------------
// Typed JSON-RPC Response Models
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct GetBlockchainInfoResult {
    pub chain: String,
    pub blocks: u64,
    pub headers: u64,
    pub bestblockhash: String,
    pub difficulty: f64,
    pub verificationprogress: f64,
    pub initialblockdownload: bool,
    pub pruned: bool,
    pub pruneheight: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetNetworkInfoResult {
    pub version: u64,
    pub subversion: String,
    pub protocolversion: u64,
    pub connections: u64,
    #[serde(default)]
    pub networkactive: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetMempoolInfoResult {
    pub loaded: Option<bool>,
    pub size: u64,
    pub bytes: u64,
    pub usage: u64,
    pub total_fee: Option<f64>,
    pub mempoolminfee: Option<f64>,
    pub minrelaytxfee: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetTxOutResult {
    pub bestblock: String,
    pub confirmations: u64,
    pub value: f64,
    #[serde(rename = "scriptPubKey")]
    pub script_pubkey: GetTxOutScriptPubKey,
    pub coinbase: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetTxOutScriptPubKey {
    pub asm: Option<String>,
    pub hex: String,
    #[serde(rename = "type")]
    pub script_type: Option<String>,
    pub address: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetRawTransactionVerboseResult {
    pub txid: String,
    pub hash: Option<String>,
    pub size: Option<u64>,
    pub vsize: Option<u64>,
    pub weight: Option<u64>,
    pub version: Option<i32>,
    pub locktime: Option<u32>,
    pub blockhash: Option<String>,
    pub confirmations: Option<u64>,
    pub time: Option<i64>,
    pub blocktime: Option<i64>,
    #[serde(default)]
    pub vout: Vec<GetRawTransactionVout>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetRawTransactionVout {
    pub value: f64,
    pub n: u32,
    #[serde(rename = "scriptPubKey")]
    pub script_pubkey: Option<GetTxOutScriptPubKey>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetBlockHeaderResult {
    pub hash: String,
    pub confirmations: i64,
    pub height: u64,
    pub version: i32,
    pub merkleroot: String,
    pub time: u64,
    pub nonce: u64,
    pub bits: String,
    pub difficulty: f64,
    pub previousblockhash: Option<String>,
    pub nextblockhash: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetBlockVerboseResult {
    pub hash: String,
    pub confirmations: i64,
    pub size: u64,
    pub weight: u64,
    pub height: u64,
    pub version: i32,
    pub merkleroot: String,
    pub tx: Vec<String>,
    pub time: u64,
    pub nonce: u64,
    pub bits: String,
    pub difficulty: f64,
    pub previousblockhash: Option<String>,
    pub nextblockhash: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChainTip {
    pub height: u64,
    pub hash: String,
    pub branchlen: u64,
    pub status: String,
}

// ---------------------------------------------------------------------------
// Internal JSON-RPC Envelope
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct RpcRequest<'a> {
    jsonrpc: &'static str,
    id: &'static str,
    method: &'a str,
    params: serde_json::Value,
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcErrorPayload>,
}

#[derive(Deserialize)]
struct RpcErrorPayload {
    code: i64,
    message: String,
}

// ---------------------------------------------------------------------------
// Client Implementation
// ---------------------------------------------------------------------------

/// Typed asynchronous JSON-RPC client for interacting with Bitcoin Core.
pub struct BitcoinCoreRpcClient {
    config: BitcoinRpcConfig,
    client: Client,
    cached_cookie: Arc<RwLock<Option<(String, String)>>>,
}

impl BitcoinCoreRpcClient {
    pub fn new(config: BitcoinRpcConfig) -> Result<Self, BitcoinRpcError> {
        // Validate URL protocol safety
        if !config.rpc_url.starts_with("http://") && !config.rpc_url.starts_with("https://") {
            return Err(BitcoinRpcError::InvalidUrl(
                "Bitcoin Core RPC URL must use http:// or https:// scheme".to_string(),
            ));
        }

        let client = Client::builder().timeout(config.timeout).build()?;

        Ok(Self {
            config,
            client,
            cached_cookie: Arc::new(RwLock::new(None)),
        })
    }

    pub fn config(&self) -> &BitcoinRpcConfig {
        &self.config
    }

    /// Safely resolves credentials either from static configuration or by reading the cookie file.
    async fn resolve_auth(&self) -> Result<Option<(String, String)>, BitcoinRpcError> {
        if let (Some(u), Some(p)) = (&self.config.rpc_user, &self.config.rpc_password) {
            return Ok(Some((u.clone(), p.clone())));
        }

        if let Some(cookie_path) = &self.config.cookie_file {
            {
                let read = self.cached_cookie.read().await;
                if let Some(auth) = &*read {
                    return Ok(Some(auth.clone()));
                }
            }

            let auth = Self::read_cookie_file(cookie_path).await?;
            let mut write = self.cached_cookie.write().await;
            *write = Some(auth.clone());
            return Ok(Some(auth));
        }

        Ok(None)
    }

    /// Reads credentials from a Bitcoin Core .cookie file safely.
    async fn read_cookie_file(path: &Path) -> Result<(String, String), BitcoinRpcError> {
        let content = tokio::fs::read_to_string(path).await.map_err(|e| {
            BitcoinRpcError::AuthError(format!("Failed to read Bitcoin Core cookie file: {}", e))
        })?;

        let trimmed = content.trim();
        let parts: Vec<&str> = trimmed.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(BitcoinRpcError::AuthError(
                "Malformed Bitcoin Core cookie file format (expected user:password)".to_string(),
            ));
        }

        Ok((parts[0].to_string(), parts[1].to_string()))
    }

    /// Invokes a raw JSON-RPC method with automatic cookie rotation retry.
    pub async fn call<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, BitcoinRpcError> {
        let req = RpcRequest {
            jsonrpc: "1.0",
            id: "obschain",
            method,
            params: params.clone(),
        };

        let auth = self.resolve_auth().await?;
        let mut req_builder = self.client.post(&self.config.rpc_url).json(&req);

        if let Some((u, p)) = &auth {
            req_builder = req_builder.basic_auth(u, Some(p));
        }

        let resp = req_builder.send().await?;

        // Handle 401 Unauthorized: if using cookie auth, invalidate and re-read cookie once
        if resp.status() == reqwest::StatusCode::UNAUTHORIZED && self.config.cookie_file.is_some() {
            debug!("RPC returned 401 Unauthorized; attempting to refresh cookie from disk");
            {
                let mut write = self.cached_cookie.write().await;
                *write = None;
            }

            let new_auth = self.resolve_auth().await?;
            let mut retry_builder = self.client.post(&self.config.rpc_url).json(&req);
            if let Some((u, p)) = &new_auth {
                retry_builder = retry_builder.basic_auth(u, Some(p));
            }

            let retry_resp = retry_builder.send().await?;
            return Self::parse_response(retry_resp).await;
        }

        Self::parse_response(resp).await
    }

    async fn parse_response<T: for<'de> Deserialize<'de>>(
        resp: reqwest::Response,
    ) -> Result<T, BitcoinRpcError> {
        if resp.status() == reqwest::StatusCode::UNAUTHORIZED {
            return Err(BitcoinRpcError::AuthError(
                "Bitcoin Core RPC authentication failed (401 Unauthorized)".to_string(),
            ));
        }

        let rpc_res: RpcResponse<T> = resp.json().await?;

        if let Some(err) = rpc_res.error {
            // Error code -28: RPC in warm-up
            if err.code == -28 {
                return Err(BitcoinRpcError::NodeWarmingUp(err.message));
            }
            // Error code -5: No such mempool or blockchain transaction (missing txindex on non-mempool query)
            if err.code == -5
                && err
                    .message
                    .contains("No such mempool or blockchain transaction")
            {
                return Err(BitcoinRpcError::TxIndexUnavailable);
            }
            return Err(BitcoinRpcError::RpcError {
                code: err.code,
                message: err.message,
            });
        }

        rpc_res.result.ok_or_else(|| BitcoinRpcError::RpcError {
            code: -1,
            message: "Empty result in RPC response".to_string(),
        })
    }

    // -----------------------------------------------------------------------
    // Core RPC Methods
    // -----------------------------------------------------------------------

    pub async fn get_blockchain_info(&self) -> Result<GetBlockchainInfoResult, BitcoinRpcError> {
        self.call("getblockchaininfo", serde_json::json!([])).await
    }

    pub async fn get_network_info(&self) -> Result<GetNetworkInfoResult, BitcoinRpcError> {
        self.call("getnetworkinfo", serde_json::json!([])).await
    }

    pub async fn get_mempool_info(&self) -> Result<GetMempoolInfoResult, BitcoinRpcError> {
        self.call("getmempoolinfo", serde_json::json!([])).await
    }

    pub async fn get_raw_mempool(&self) -> Result<Vec<String>, BitcoinRpcError> {
        self.call("getrawmempool", serde_json::json!([false])).await
    }

    pub async fn get_block_hash(&self, height: u64) -> Result<String, BitcoinRpcError> {
        self.call("getblockhash", serde_json::json!([height])).await
    }

    pub async fn get_block_header(
        &self,
        hash: &str,
    ) -> Result<GetBlockHeaderResult, BitcoinRpcError> {
        self.call("getblockheader", serde_json::json!([hash, true]))
            .await
    }

    /// Fetches block with verbosity 1 (JSON with txid list).
    pub async fn get_block(&self, hash: &str) -> Result<GetBlockVerboseResult, BitcoinRpcError> {
        self.call("getblock", serde_json::json!([hash, 1])).await
    }

    /// Fetches raw hex serialized block (verbosity 0).
    pub async fn get_block_raw_hex(&self, hash: &str) -> Result<String, BitcoinRpcError> {
        self.call("getblock", serde_json::json!([hash, 0])).await
    }

    /// Fetches raw transaction hex (verbosity false).
    pub async fn get_raw_transaction_hex(&self, txid: &str) -> Result<String, BitcoinRpcError> {
        self.call("getrawtransaction", serde_json::json!([txid, false]))
            .await
    }

    /// Fetches decoded transaction JSON with confirmations and output details (verbosity true / 1).
    pub async fn get_raw_transaction_verbose(
        &self,
        txid: &str,
    ) -> Result<GetRawTransactionVerboseResult, BitcoinRpcError> {
        self.call("getrawtransaction", serde_json::json!([txid, true]))
            .await
    }

    /// Fetches an unspent transaction output (UTXO) via `gettxout`.
    /// Returns `None` if the output has been spent.
    pub async fn get_tx_out(
        &self,
        txid: &str,
        vout: u32,
        include_mempool: bool,
    ) -> Result<Option<GetTxOutResult>, BitcoinRpcError> {
        let res: serde_json::Value = self
            .call("gettxout", serde_json::json!([txid, vout, include_mempool]))
            .await?;

        if res.is_null() {
            Ok(None)
        } else {
            let parsed = serde_json::from_value(res)?;
            Ok(Some(parsed))
        }
    }

    /// Fetches competing tips and forks via `getchaintips`.
    pub async fn get_chain_tips(&self) -> Result<Vec<ChainTip>, BitcoinRpcError> {
        self.call("getchaintips", serde_json::json!([])).await
    }

    /// Inspects and validates Bitcoin Core capabilities, network, and indices.
    pub async fn inspect_capabilities(&self) -> Result<BitcoinNodeCapabilities, BitcoinRpcError> {
        let btc_info = self.get_blockchain_info().await?;
        let net_info = self.get_network_info().await?;

        // Validate network matches configuration if requested
        if let Some(expected) = &self.config.expected_network {
            let exp_lower = expected.to_lowercase();
            let normalized_expected = match exp_lower.as_str() {
                "bitcoin" | "mainnet" => "main",
                "testnet" => "test",
                "signet" => "signet",
                "regtest" => "regtest",
                other => other,
            };

            let node_chain = btc_info.chain.to_lowercase();
            if node_chain != normalized_expected {
                return Err(BitcoinRpcError::NetworkMismatch {
                    expected: expected.clone(),
                    reported: btc_info.chain,
                });
            }
        }

        // Query txindex availability via authoritative getindexinfo RPC
        let txindex_available = match self
            .call::<serde_json::Value>("getindexinfo", serde_json::json!([]))
            .await
        {
            Ok(indices) => indices.get("txindex").is_some(),
            Err(_) => false,
        };

        Ok(BitcoinNodeCapabilities {
            network: btc_info.chain,
            version: net_info.version,
            subversion: net_info.subversion,
            blocks: btc_info.blocks,
            headers: btc_info.headers,
            verification_progress: btc_info.verificationprogress,
            initial_block_download: btc_info.initialblockdownload,
            pruned: btc_info.pruned,
            prune_height: btc_info.pruneheight,
            txindex_available,
            zmq_rawtx: true,
            zmq_rawblock: true,
            zmq_sequence: true,
        })
    }
}

impl IngestSource for BitcoinCoreRpcClient {
    fn source_type(&self) -> IngestionSourceType {
        IngestionSourceType::BitcoinCoreRpc
    }

    fn name(&self) -> &'static str {
        "bitcoin_core_rpc"
    }

    fn is_healthy(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{http::StatusCode, routing::post, Router};
    use tokio::net::TcpListener;

    #[test]
    fn test_rpc_config_url_safety_validation() {
        let invalid_cfg = BitcoinRpcConfig {
            rpc_url: "file:///etc/passwd".to_string(),
            ..Default::default()
        };
        let err = BitcoinCoreRpcClient::new(invalid_cfg).err().unwrap();
        assert!(matches!(err, BitcoinRpcError::InvalidUrl(_)));
    }

    #[tokio::test]
    async fn test_cookie_file_parsing() {
        let temp_dir = std::env::temp_dir();
        let cookie_path = temp_dir.join(format!("test_cookie_{}.cookie", uuid::Uuid::new_v4()));
        tokio::fs::write(&cookie_path, "__cookie__:secret_token_12345\n")
            .await
            .expect("write cookie");

        let auth = BitcoinCoreRpcClient::read_cookie_file(&cookie_path)
            .await
            .expect("read cookie");
        assert_eq!(auth.0, "__cookie__");
        assert_eq!(auth.1, "secret_token_12345");

        let _ = tokio::fs::remove_file(&cookie_path).await;
    }

    async fn start_mock_rpc_server<F, Fut>(handler: F) -> (String, tokio::task::JoinHandle<()>)
    where
        F: Fn(bytes::Bytes) -> Fut + Clone + Send + Sync + 'static,
        Fut: std::future::Future<Output = (StatusCode, String)> + Send + 'static,
    {
        let app = Router::new().route(
            "/",
            post(move |body: bytes::Bytes| {
                let handler = handler.clone();
                async move {
                    let (status, resp_str) = handler(body).await;
                    (status, [("content-type", "application/json")], resp_str)
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{}", addr), handle)
    }

    #[tokio::test]
    async fn test_rpc_valid_response() {
        let (url, server_handle) = start_mock_rpc_server(|_body| async move {
            (
                StatusCode::OK,
                serde_json::json!({
                    "jsonrpc": "1.0",
                    "id": "obschain",
                    "result": {
                        "chain": "main",
                        "blocks": 968500,
                        "headers": 968500,
                        "bestblockhash": "00000000000000000001099645903b6e82810a950bc490d1bfca722a5fbef8f3",
                        "difficulty": 1.0,
                        "verificationprogress": 0.999999,
                        "initialblockdownload": false,
                        "pruned": false
                    },
                    "error": null
                })
                .to_string(),
            )
        })
        .await;

        let cfg = BitcoinRpcConfig {
            rpc_url: url,
            timeout: Duration::from_secs(5),
            ..Default::default()
        };
        let client = BitcoinCoreRpcClient::new(cfg).unwrap();
        let info = client.get_blockchain_info().await.unwrap();
        assert_eq!(info.chain, "main");
        assert_eq!(info.blocks, 968500);
        assert!(!info.initialblockdownload);

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_rpc_error_handling() {
        let (url, server_handle) = start_mock_rpc_server(|_body| async move {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                serde_json::json!({
                    "jsonrpc": "1.0",
                    "id": "obschain",
                    "result": null,
                    "error": {
                        "code": -1,
                        "message": "Method not found"
                    }
                })
                .to_string(),
            )
        })
        .await;

        let cfg = BitcoinRpcConfig {
            rpc_url: url,
            timeout: Duration::from_secs(5),
            ..Default::default()
        };
        let client = BitcoinCoreRpcClient::new(cfg).unwrap();
        let err = client.get_blockchain_info().await.unwrap_err();
        match err {
            BitcoinRpcError::RpcError { code, message } => {
                assert_eq!(code, -1);
                assert_eq!(message, "Method not found");
            }
            other => panic!("Expected RpcError, got: {:?}", other),
        }

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_rpc_http_authentication_failure() {
        let (url, server_handle) = start_mock_rpc_server(|_body| async move {
            (
                StatusCode::UNAUTHORIZED,
                "401 Unauthorized: Access Denied".to_string(),
            )
        })
        .await;

        let cfg = BitcoinRpcConfig {
            rpc_url: url,
            timeout: Duration::from_secs(5),
            ..Default::default()
        };
        let client = BitcoinCoreRpcClient::new(cfg).unwrap();
        let err = client.get_blockchain_info().await.unwrap_err();
        assert!(matches!(err, BitcoinRpcError::AuthError(_)));

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_rpc_timeout() {
        let (url, server_handle) = start_mock_rpc_server(|_body| async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            (StatusCode::OK, "{}".to_string())
        })
        .await;

        let cfg = BitcoinRpcConfig {
            rpc_url: url,
            timeout: Duration::from_millis(30),
            ..Default::default()
        };
        let client = BitcoinCoreRpcClient::new(cfg).unwrap();
        let err = client.get_blockchain_info().await.unwrap_err();
        assert!(matches!(err, BitcoinRpcError::Http(e) if e.is_timeout()));

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_rpc_malformed_json() {
        let (url, server_handle) = start_mock_rpc_server(|_body| async move {
            (StatusCode::OK, "{not-a-valid-json-string".to_string())
        })
        .await;

        let cfg = BitcoinRpcConfig {
            rpc_url: url,
            timeout: Duration::from_secs(5),
            ..Default::default()
        };
        let client = BitcoinCoreRpcClient::new(cfg).unwrap();
        let err = client.get_blockchain_info().await.unwrap_err();
        assert!(matches!(
            err,
            BitcoinRpcError::Http(_) | BitcoinRpcError::Json(_)
        ));

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_rpc_wrong_result_type() {
        let (url, server_handle) = start_mock_rpc_server(|_body| async move {
            (
                StatusCode::OK,
                serde_json::json!({
                    "jsonrpc": "1.0",
                    "id": "obschain",
                    "result": "this-is-a-string-not-an-object",
                    "error": null
                })
                .to_string(),
            )
        })
        .await;

        let cfg = BitcoinRpcConfig {
            rpc_url: url,
            timeout: Duration::from_secs(5),
            ..Default::default()
        };
        let client = BitcoinCoreRpcClient::new(cfg).unwrap();
        let err = client.get_blockchain_info().await.unwrap_err();
        assert!(matches!(
            err,
            BitcoinRpcError::Http(_) | BitcoinRpcError::Json(_)
        ));

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_rpc_node_warming_up() {
        let (url, server_handle) = start_mock_rpc_server(|_body| async move {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                serde_json::json!({
                    "jsonrpc": "1.0",
                    "id": "obschain",
                    "result": null,
                    "error": {
                        "code": -28,
                        "message": "Loading block index..."
                    }
                })
                .to_string(),
            )
        })
        .await;

        let cfg = BitcoinRpcConfig {
            rpc_url: url,
            timeout: Duration::from_secs(5),
            ..Default::default()
        };
        let client = BitcoinCoreRpcClient::new(cfg).unwrap();
        let err = client.get_blockchain_info().await.unwrap_err();
        match err {
            BitcoinRpcError::NodeWarmingUp(msg) => {
                assert!(msg.contains("Loading block index"));
            }
            other => panic!("Expected NodeWarmingUp, got: {:?}", other),
        }

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_rpc_network_mismatch() {
        let (url, server_handle) = start_mock_rpc_server(|body| async move {
            let req: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
            let method = req["method"].as_str().unwrap_or_default();

            if method == "getblockchaininfo" {
                (
                    StatusCode::OK,
                    serde_json::json!({
                        "jsonrpc": "1.0",
                        "id": "obschain",
                        "result": {
                            "chain": "testnet",
                            "blocks": 2500000,
                            "headers": 2500000,
                            "bestblockhash": "00000000000000000001099645903b6e82810a950bc490d1bfca722a5fbef8f3",
                            "difficulty": 1.0,
                            "verificationprogress": 0.999999,
                            "initialblockdownload": false,
                            "pruned": false
                        },
                        "error": null
                    })
                    .to_string(),
                )
            } else if method == "getnetworkinfo" {
                (
                    StatusCode::OK,
                    serde_json::json!({
                        "jsonrpc": "1.0",
                        "id": "obschain",
                        "result": {
                            "version": 260000,
                            "subversion": "/Satoshi:26.0.0/",
                            "protocolversion": 70016,
                            "connections": 8,
                            "networkactive": true
                        },
                        "error": null
                    })
                    .to_string(),
                )
            } else {
                (StatusCode::NOT_FOUND, "{}".to_string())
            }
        })
        .await;

        let cfg = BitcoinRpcConfig {
            rpc_url: url,
            expected_network: Some("bitcoin".to_string()),
            timeout: Duration::from_secs(5),
            ..Default::default()
        };
        let client = BitcoinCoreRpcClient::new(cfg).unwrap();
        let err = client.inspect_capabilities().await.unwrap_err();
        match err {
            BitcoinRpcError::NetworkMismatch { expected, reported } => {
                assert_eq!(expected, "bitcoin");
                assert_eq!(reported, "testnet");
            }
            other => panic!("Expected NetworkMismatch, got: {:?}", other),
        }

        server_handle.abort();
    }
}
