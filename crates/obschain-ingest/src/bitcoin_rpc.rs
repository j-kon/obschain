use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::source::{IngestSource, IngestionSourceType};

#[derive(Debug, Error)]
pub enum BitcoinRpcError {
    #[error("HTTP transport error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Bitcoin Core JSON-RPC error ({code}): {message}")]
    RpcError { code: i64, message: String },

    #[error("Serialization / Deserialization error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Connection parameters for Bitcoin Core RPC node.
#[derive(Debug, Clone)]
pub struct BitcoinRpcConfig {
    pub rpc_url: String,
    pub rpc_user: Option<String>,
    pub rpc_password: Option<String>,
    pub timeout: Duration,
}

impl Default for BitcoinRpcConfig {
    fn default() -> Self {
        Self {
            rpc_url: "http://127.0.0.1:8332".to_string(),
            rpc_user: None,
            rpc_password: None,
            timeout: Duration::from_secs(15),
        }
    }
}

/// Client interface for querying local or remote Bitcoin Core nodes via JSON-RPC.
pub struct BitcoinRpcClient {
    config: BitcoinRpcConfig,
    client: Client,
}

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

impl BitcoinRpcClient {
    pub fn new(config: BitcoinRpcConfig) -> Result<Self, BitcoinRpcError> {
        let client = Client::builder().timeout(config.timeout).build()?;
        Ok(Self { config, client })
    }

    /// Calls an arbitrary Bitcoin Core RPC method safely.
    pub async fn call<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, BitcoinRpcError> {
        let req = RpcRequest {
            jsonrpc: "1.0",
            id: "obschain",
            method,
            params,
        };

        let mut req_builder = self.client.post(&self.config.rpc_url).json(&req);

        if let (Some(u), Some(p)) = (&self.config.rpc_user, &self.config.rpc_password) {
            req_builder = req_builder.basic_auth(u, Some(p));
        }

        let resp = req_builder.send().await?;
        let rpc_res: RpcResponse<T> = resp.json().await?;

        if let Some(err) = rpc_res.error {
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

    /// Fetches the current block count from the connected Bitcoin Core node.
    pub async fn get_block_count(&self) -> Result<u64, BitcoinRpcError> {
        self.call("getblockcount", serde_json::json!([])).await
    }
}

impl IngestSource for BitcoinRpcClient {
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
