use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageBackendConfig {
    Memory,
    Postgres,
}

impl std::fmt::Display for StorageBackendConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Memory => write!(f, "memory"),
            Self::Postgres => write!(f, "postgres"),
        }
    }
}

impl FromStr for StorageBackendConfig {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "memory" | "mem" | "in-memory" | "" => Ok(Self::Memory),
            "postgres" | "postgresql" | "pg" => Ok(Self::Postgres),
            other => Err(format!(
                "Invalid OBSCHAIN_STORAGE_BACKEND '{other}'. Supported backends: 'memory', 'postgres'"
            )),
        }
    }
}

/// Primary authoritative data source preference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrimarySourceConfig {
    BitcoinCore,
    MempoolSpace,
    Auto,
}

impl std::fmt::Display for PrimarySourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BitcoinCore => write!(f, "bitcoin_core"),
            Self::MempoolSpace => write!(f, "mempool_space"),
            Self::Auto => write!(f, "auto"),
        }
    }
}

impl FromStr for PrimarySourceConfig {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "bitcoin_core" | "bitcoind" | "core" => Ok(Self::BitcoinCore),
            "mempool_space" | "mempool" => Ok(Self::MempoolSpace),
            "auto" | "" => Ok(Self::Auto),
            other => Err(format!(
                "Invalid OBSCHAIN_PRIMARY_SOURCE '{other}'. Supported values: 'bitcoin_core', 'mempool_space', 'auto'"
            )),
        }
    }
}

/// Helper function to safely redact passwords from database connection URLs before logging.
pub fn redact_database_url(url: &str) -> String {
    let (scheme, rest) = match url.split_once("://") {
        Some((s, r)) => (s, r),
        None => return "<redacted>".to_string(),
    };
    if let Some((user_pass, host_db)) = rest.split_once('@') {
        if let Some((user, _pass)) = user_pass.split_once(':') {
            format!("{}://{}:***@{}", scheme, user, host_db)
        } else {
            format!("{}://***@{}", scheme, host_db)
        }
    } else {
        format!("{}://{}", scheme, rest)
    }
}

/// Helper function to safely redact credentials from an RPC URL before logging.
pub fn redact_rpc_url(url: &str) -> String {
    let (scheme, rest) = match url.split_once("://") {
        Some((s, r)) => (s, r),
        None => return "<redacted>".to_string(),
    };
    if let Some((user_pass, host_port)) = rest.split_once('@') {
        if let Some((user, _pass)) = user_pass.split_once(':') {
            format!("{}://{}:***@{}", scheme, user, host_port)
        } else {
            format!("{}://***@{}", scheme, host_port)
        }
    } else {
        format!("{}://{}", scheme, rest)
    }
}

pub struct AppConfig {
    pub host: String,
    pub port: u16,
    pub storage_backend: StorageBackendConfig,
    pub database_url: Option<String>,
    pub db_max_connections: u32,
    pub db_min_connections: u32,
    pub db_acquire_timeout_seconds: u64,
    pub mempool_api_url: String,
    pub mempool_ws_url: String,
    // Phase 5 Sovereign Bitcoin Core Configuration
    pub bitcoin_core_enabled: bool,
    pub bitcoin_rpc_url: String,
    pub bitcoin_rpc_user: Option<String>,
    pub bitcoin_rpc_password: Option<String>,
    pub bitcoin_cookie_file: Option<std::path::PathBuf>,
    pub bitcoin_zmq_rawtx: Option<String>,
    pub bitcoin_zmq_rawblock: Option<String>,
    pub bitcoin_zmq_sequence: Option<String>,
    pub bitcoin_network: String,
    pub primary_source: PrimarySourceConfig,
    pub sovereign_only: bool,
    pub reconcile_max_blocks: u64,
    pub large_tx_threshold_sats: u64,
    pub long_block_interval_seconds: u64,
    pub event_store_limit: usize,
    pub mock_feed: bool,
    // Phase 2A Detector and Cache Configuration
    pub dormant_min_age_days: u64,
    pub dormant_min_value_sats: u64,
    pub consolidation_min_inputs: u32,
    pub consolidation_max_outputs: u32,
    pub consolidation_min_value_sats: u64,
    pub fanout_min_outputs: u32,
    pub fanout_min_value_sats: u64,
    pub extreme_fee_sats: u64,
    pub extreme_fee_rate_sat_vb: f64,
    pub utxo_cache_limit: usize,
    pub utxo_cache_ttl_seconds: u64,
    pub max_input_enrichment: usize,
    pub utxo_lookup_concurrency: usize,
    pub dedup_cache_capacity: usize,
    pub dedup_cache_ttl_seconds: u64,
}

impl AppConfig {
    pub fn from_env() -> Self {
        let storage_backend = std::env::var("OBSCHAIN_STORAGE_BACKEND")
            .ok()
            .and_then(|v| StorageBackendConfig::from_str(&v).ok())
            .unwrap_or(StorageBackendConfig::Memory);

        let db_max_connections = std::env::var("OBSCHAIN_DB_MAX_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10);

        let db_min_connections = std::env::var("OBSCHAIN_DB_MIN_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1);

        let db_acquire_timeout_seconds = std::env::var("OBSCHAIN_DB_ACQUIRE_TIMEOUT_SECONDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5);
        let host = std::env::var("OBSCHAIN_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let port = std::env::var("OBSCHAIN_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8080);

        let large_tx_threshold_sats =
            if let Ok(sats_str) = std::env::var("OBSCHAIN_LARGE_TX_THRESHOLD_SATS") {
                sats_str.parse().unwrap_or(10_000_000_000)
            } else if let Ok(btc_str) = std::env::var("OBSCHAIN_LARGE_TX_THRESHOLD_BTC") {
                obschain_core::btc_str_to_sats(&btc_str).unwrap_or(10_000_000_000)
            } else {
                10_000_000_000 // 100 BTC
            };

        let long_block_interval_seconds = std::env::var("OBSCHAIN_LONG_BLOCK_INTERVAL_SECONDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1800); // 30 minutes

        let event_store_limit = std::env::var("OBSCHAIN_EVENT_STORE_LIMIT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000);

        let mock_feed = std::env::var("OBSCHAIN_MOCK_FEED")
            .map(|v| v.to_lowercase() == "true" || v == "1")
            .unwrap_or(false);

        // Phase 2A Detector parameters
        let dormant_min_age_days = std::env::var("OBSCHAIN_DORMANT_MIN_AGE_DAYS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1825); // 5 years

        let dormant_min_value_sats =
            if let Ok(sats_str) = std::env::var("OBSCHAIN_DORMANT_MIN_VALUE_SATS") {
                sats_str.parse().unwrap_or(100_000_000)
            } else if let Ok(btc_str) = std::env::var("OBSCHAIN_DORMANT_MIN_VALUE_BTC") {
                obschain_core::btc_str_to_sats(&btc_str).unwrap_or(100_000_000)
            } else {
                100_000_000 // 1 BTC
            };

        let consolidation_min_inputs = std::env::var("OBSCHAIN_CONSOLIDATION_MIN_INPUTS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20);

        let consolidation_max_outputs = std::env::var("OBSCHAIN_CONSOLIDATION_MAX_OUTPUTS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5);

        let consolidation_min_value_sats =
            if let Ok(sats_str) = std::env::var("OBSCHAIN_CONSOLIDATION_MIN_VALUE_SATS") {
                sats_str.parse().unwrap_or(0)
            } else if let Ok(btc_str) = std::env::var("OBSCHAIN_CONSOLIDATION_MIN_VALUE_BTC") {
                obschain_core::btc_str_to_sats(&btc_str).unwrap_or(0)
            } else {
                0
            };

        let fanout_min_outputs = std::env::var("OBSCHAIN_FANOUT_MIN_OUTPUTS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(50);

        let fanout_min_value_sats =
            if let Ok(sats_str) = std::env::var("OBSCHAIN_FANOUT_MIN_VALUE_SATS") {
                sats_str.parse().unwrap_or(0)
            } else if let Ok(btc_str) = std::env::var("OBSCHAIN_FANOUT_MIN_VALUE_BTC") {
                obschain_core::btc_str_to_sats(&btc_str).unwrap_or(0)
            } else {
                0
            };

        let extreme_fee_sats = if let Ok(sats_str) = std::env::var("OBSCHAIN_EXTREME_FEE_SATS") {
            sats_str.parse().unwrap_or(10_000_000)
        } else if let Ok(btc_str) = std::env::var("OBSCHAIN_EXTREME_FEE_BTC") {
            obschain_core::btc_str_to_sats(&btc_str).unwrap_or(10_000_000)
        } else {
            10_000_000 // 0.1 BTC
        };

        let extreme_fee_rate_sat_vb = std::env::var("OBSCHAIN_EXTREME_FEE_RATE_SAT_VB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200.0);

        let utxo_cache_limit = std::env::var("OBSCHAIN_UTXO_CACHE_LIMIT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(50_000);

        let utxo_cache_ttl_seconds = std::env::var("OBSCHAIN_UTXO_CACHE_TTL_SECONDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3600); // 1 hour

        let max_input_enrichment = std::env::var("OBSCHAIN_MAX_INPUT_ENRICHMENT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(500);

        let utxo_lookup_concurrency = std::env::var("OBSCHAIN_UTXO_LOOKUP_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(16);

        let dedup_cache_capacity = std::env::var("OBSCHAIN_DEDUP_CAPACITY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000);

        let dedup_cache_ttl_seconds = std::env::var("OBSCHAIN_DEDUP_TTL_SECONDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1800); // 30 mins

        let bitcoin_core_enabled = std::env::var("OBSCHAIN_BITCOIN_CORE_ENABLED")
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false);

        let bitcoin_rpc_url = std::env::var("BITCOIN_RPC_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8332".to_string());

        let bitcoin_rpc_user = std::env::var("BITCOIN_RPC_USER")
            .ok()
            .filter(|s| !s.is_empty());
        let bitcoin_rpc_password = std::env::var("BITCOIN_RPC_PASSWORD")
            .ok()
            .filter(|s| !s.is_empty());
        let bitcoin_cookie_file = std::env::var("BITCOIN_COOKIE_FILE")
            .ok()
            .filter(|s| !s.is_empty())
            .map(std::path::PathBuf::from);

        let bitcoin_zmq_rawtx = std::env::var("BITCOIN_ZMQ_RAWTX")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| Some("tcp://127.0.0.1:28332".to_string()));

        let bitcoin_zmq_rawblock = std::env::var("BITCOIN_ZMQ_RAWBLOCK")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| Some("tcp://127.0.0.1:28333".to_string()));

        let bitcoin_zmq_sequence = std::env::var("BITCOIN_ZMQ_SEQUENCE")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| Some("tcp://127.0.0.1:28334".to_string()));

        let bitcoin_network =
            std::env::var("OBSCHAIN_BITCOIN_NETWORK").unwrap_or_else(|_| "bitcoin".to_string());

        let primary_source = std::env::var("OBSCHAIN_PRIMARY_SOURCE")
            .ok()
            .and_then(|v| PrimarySourceConfig::from_str(&v).ok())
            .unwrap_or(PrimarySourceConfig::Auto);

        let sovereign_only = std::env::var("OBSCHAIN_SOVEREIGN_ONLY")
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false);

        let reconcile_max_blocks = std::env::var("OBSCHAIN_RECONCILE_MAX_BLOCKS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100);

        Self {
            host,
            port,
            storage_backend,
            database_url: std::env::var("DATABASE_URL").ok(),
            db_max_connections,
            db_min_connections,
            db_acquire_timeout_seconds,
            mempool_api_url: std::env::var("MEMPOOL_API_URL")
                .unwrap_or_else(|_| "https://mempool.space/api".to_string()),
            mempool_ws_url: std::env::var("MEMPOOL_WS_URL")
                .unwrap_or_else(|_| "wss://mempool.space/api/v1/ws".to_string()),
            bitcoin_core_enabled,
            bitcoin_rpc_url,
            bitcoin_rpc_user,
            bitcoin_rpc_password,
            bitcoin_cookie_file,
            bitcoin_zmq_rawtx,
            bitcoin_zmq_rawblock,
            bitcoin_zmq_sequence,
            bitcoin_network,
            primary_source,
            sovereign_only,
            reconcile_max_blocks,
            large_tx_threshold_sats,
            long_block_interval_seconds,
            event_store_limit,
            mock_feed,
            dormant_min_age_days,
            dormant_min_value_sats,
            consolidation_min_inputs,
            consolidation_max_outputs,
            consolidation_min_value_sats,
            fanout_min_outputs,
            fanout_min_value_sats,
            extreme_fee_sats,
            extreme_fee_rate_sat_vb,
            utxo_cache_limit,
            utxo_cache_ttl_seconds,
            max_input_enrichment,
            utxo_lookup_concurrency,
            dedup_cache_capacity,
            dedup_cache_ttl_seconds,
        }
    }

    pub fn socket_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        format!("{}:{}", self.host, self.port).parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redact_rpc_url_with_credentials() {
        let raw = "http://myuser:secretpassword123@127.0.0.1:8332";
        let redacted = redact_rpc_url(raw);
        assert_eq!(redacted, "http://myuser:***@127.0.0.1:8332");
        assert!(!redacted.contains("secretpassword123"));
    }

    #[test]
    fn test_redact_rpc_url_without_credentials() {
        let raw = "http://127.0.0.1:8332";
        let redacted = redact_rpc_url(raw);
        assert_eq!(redacted, "http://127.0.0.1:8332");
    }

    #[test]
    fn test_primary_source_parsing() {
        assert_eq!(
            PrimarySourceConfig::from_str("bitcoin_core").unwrap(),
            PrimarySourceConfig::BitcoinCore
        );
        assert_eq!(
            PrimarySourceConfig::from_str("mempool_space").unwrap(),
            PrimarySourceConfig::MempoolSpace
        );
        assert_eq!(
            PrimarySourceConfig::from_str("auto").unwrap(),
            PrimarySourceConfig::Auto
        );
        assert!(PrimarySourceConfig::from_str("invalid_source").is_err());
    }
}
