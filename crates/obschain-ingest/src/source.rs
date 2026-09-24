use serde::{Deserialize, Serialize};

/// Type of ingestion source feeding ObsChain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IngestionSourceType {
    /// Bitcoin Core JSON-RPC node connection.
    BitcoinCoreRpc,
    /// Bitcoin Core ZeroMQ message stream.
    BitcoinCoreZmq,
    /// mempool.space REST API.
    MempoolSpaceRest,
    /// mempool.space WebSocket stream.
    MempoolSpaceWs,
    /// Synthetic or offline replay feed for testing.
    SyntheticReplay,
}

/// Generic interface implemented by all observation streams.
pub trait IngestSource: Send + Sync {
    /// Source type descriptor.
    fn source_type(&self) -> IngestionSourceType;

    /// Descriptive name of the source endpoint.
    fn name(&self) -> &'static str;

    /// Checks connectivity to the upstream source.
    fn is_healthy(&self) -> bool;
}
