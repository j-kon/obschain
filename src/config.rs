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
}

impl AppConfig {
    pub fn from_env() -> Self {
        let host = std::env::var("OBSCHAIN_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let port = std::env::var("OBSCHAIN_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8080);

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
        }
    }

    pub fn socket_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        format!("{}:{}", self.host, self.port).parse()
    }
}
