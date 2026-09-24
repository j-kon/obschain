use serde::{Deserialize, Serialize};

use crate::source::{IngestSource, IngestionSourceType};

/// Bitcoin Core ZMQ topic subscriptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BitcoinZmqTopic {
    RawBlock,
    RawTx,
    HashBlock,
    HashTx,
    Sequence,
}

impl BitcoinZmqTopic {
    pub fn as_topic_str(&self) -> &'static str {
        match self {
            Self::RawBlock => "rawblock",
            Self::RawTx => "rawtx",
            Self::HashBlock => "hashblock",
            Self::HashTx => "hashtx",
            Self::Sequence => "sequence",
        }
    }
}

/// Configuration for Bitcoin Core ZeroMQ subscriber.
#[derive(Debug, Clone)]
pub struct BitcoinZmqConfig {
    pub endpoint: String,
    pub topics: Vec<BitcoinZmqTopic>,
}

impl Default for BitcoinZmqConfig {
    fn default() -> Self {
        Self {
            endpoint: "tcp://127.0.0.1:28332".to_string(),
            topics: vec![
                BitcoinZmqTopic::HashBlock,
                BitcoinZmqTopic::RawBlock,
                BitcoinZmqTopic::RawTx,
            ],
        }
    }
}

/// ZeroMQ event subscriber interface for Bitcoin Core.
pub struct BitcoinZmqSubscriber {
    config: BitcoinZmqConfig,
}

impl BitcoinZmqSubscriber {
    pub fn new(config: BitcoinZmqConfig) -> Self {
        Self { config }
    }

    pub fn endpoint(&self) -> &str {
        &self.config.endpoint
    }
}

impl IngestSource for BitcoinZmqSubscriber {
    fn source_type(&self) -> IngestionSourceType {
        IngestionSourceType::BitcoinCoreZmq
    }

    fn name(&self) -> &'static str {
        "bitcoin_core_zmq"
    }

    fn is_healthy(&self) -> bool {
        true
    }
}
