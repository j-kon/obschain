use serde::{Deserialize, Serialize};

/// Identifies the origin and transport mechanism of an observation.
/// Critical for maintaining evidence provenance and incident attribution.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObservationSource {
    pub provider: String,
    pub transport: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
}

impl ObservationSource {
    pub fn new(
        provider: impl Into<String>,
        transport: impl Into<String>,
        endpoint: Option<String>,
    ) -> Self {
        Self {
            provider: provider.into(),
            transport: transport.into(),
            endpoint,
        }
    }

    pub fn mempool_rest(endpoint: impl Into<String>) -> Self {
        Self {
            provider: "mempool.space".to_string(),
            transport: "rest".to_string(),
            endpoint: Some(endpoint.into()),
        }
    }

    pub fn mempool_ws(endpoint: impl Into<String>) -> Self {
        Self {
            provider: "mempool.space".to_string(),
            transport: "websocket".to_string(),
            endpoint: Some(endpoint.into()),
        }
    }

    pub fn bitcoin_core_rpc(endpoint: impl Into<String>) -> Self {
        Self {
            provider: "bitcoin_core".to_string(),
            transport: "rpc".to_string(),
            endpoint: Some(endpoint.into()),
        }
    }

    pub fn bitcoin_core_zmq(endpoint: impl Into<String>) -> Self {
        Self {
            provider: "bitcoin_core".to_string(),
            transport: "zmq".to_string(),
            endpoint: Some(endpoint.into()),
        }
    }
}

/// Operational and connectivity state of an ingestion source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SourceHealthState {
    Connected,
    Connecting,
    Syncing,
    Degraded,
    Disconnected,
    NotConfigured,
}

impl SourceHealthState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Connected => "connected",
            Self::Connecting => "connecting",
            Self::Syncing => "syncing",
            Self::Degraded => "degraded",
            Self::Disconnected => "disconnected",
            Self::NotConfigured => "not_configured",
        }
    }
}

/// An independent observer or corroborating witness for a chain event or transaction.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObservationWitness {
    pub source: ObservationSource,
    pub observed_at: chrono::DateTime<chrono::Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl ObservationWitness {
    pub fn new(source: ObservationSource) -> Self {
        Self {
            source,
            observed_at: chrono::Utc::now(),
            metadata: None,
        }
    }

    pub fn with_metadata(source: ObservationSource, metadata: serde_json::Value) -> Self {
        Self {
            source,
            observed_at: chrono::Utc::now(),
            metadata: Some(metadata),
        }
    }
}
