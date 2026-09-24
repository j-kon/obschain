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
                btc_str
                    .parse::<f64>()
                    .map(|btc| (btc * 100_000_000.0).round() as u64)
                    .unwrap_or(10_000_000_000)
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
        }
    }

    pub fn socket_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        format!("{}:{}", self.host, self.port).parse()
    }
}
