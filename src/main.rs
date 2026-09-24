use std::sync::Arc;

use chrono::Utc;
use obschain::{create_router, AppConfig, AppState};
use obschain_detectors::{LargeTransactionDetector, LongBlockIntervalDetector};
use obschain_storage::InMemoryStorage;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,obschain=debug,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = AppConfig::from_env();
    let addr = config.socket_addr()?;

    let storage = InMemoryStorage::new();
    let detectors: Vec<Arc<dyn obschain_detectors::Detector>> = vec![
        Arc::new(LargeTransactionDetector::new()),
        Arc::new(LongBlockIntervalDetector::new()),
    ];

    let state = AppState {
        storage,
        detectors,
        started_at: Utc::now(),
        is_mock_feed: true,
    };

    let app = create_router(state);

    tracing::info!(
        host = %config.host,
        port = %config.port,
        "ObsChain backend server starting on http://{}",
        addr
    );

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
