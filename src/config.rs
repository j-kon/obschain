use std::net::SocketAddr;

pub struct AppConfig {
    pub host: String,
    pub port: u16,
    pub database_url: Option<String>,
    pub mempool_api_url: String,
    pub mempool_ws_url: String,
    pub bitcoin_rpc_url: Option<String>,
    pub bitcoin_rpc_user: Option<String>,
    pub bitcoin_rpc_password: Option<String>,
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

        Self {
            host,
            port,
            database_url: std::env::var("DATABASE_URL").ok(),
            mempool_api_url: std::env::var("MEMPOOL_API_URL")
                .unwrap_or_else(|_| "https://mempool.space/api".to_string()),
            mempool_ws_url: std::env::var("MEMPOOL_WS_URL")
                .unwrap_or_else(|_| "wss://mempool.space/api/v1/ws".to_string()),
            bitcoin_rpc_url: std::env::var("BITCOIN_RPC_URL").ok(),
            bitcoin_rpc_user: std::env::var("BITCOIN_RPC_USER").ok(),
            bitcoin_rpc_password: std::env::var("BITCOIN_RPC_PASSWORD").ok(),
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
