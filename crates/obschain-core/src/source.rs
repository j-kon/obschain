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
