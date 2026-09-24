use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::DateTime;
use futures_util::stream::{FuturesUnordered, StreamExt};
use obschain_core::TransactionObservation;
use tokio::sync::Semaphore;
use tracing::debug;

use crate::{
    mempool_rest::MempoolRestClient,
    utxo_cache::{CachedTxOutputs, UtxoCache},
};

/// Configuration for transaction historical UTXO enrichment.
#[derive(Debug, Clone)]
pub struct EnricherConfig {
    pub max_input_enrichment: usize,
    pub lookup_concurrency: usize,
    pub request_timeout: Duration,
}

impl Default for EnricherConfig {
    fn default() -> Self {
        Self {
            max_input_enrichment: 500,
            lookup_concurrency: 16,
            request_timeout: Duration::from_secs(5),
        }
    }
}

/// Enriches transaction inputs with historical UTXO confirmation context
/// using bounded concurrency, request deduplication, and an in-memory TTL cache.
#[derive(Clone)]
pub struct TransactionEnricher {
    rest_client: Arc<MempoolRestClient>,
    cache: UtxoCache,
    config: EnricherConfig,
    transactions_enriched: Arc<AtomicU64>,
    lookup_failures: Arc<AtomicU64>,
}

impl TransactionEnricher {
    pub fn new(
        rest_client: Arc<MempoolRestClient>,
        cache: UtxoCache,
        config: EnricherConfig,
    ) -> Self {
        Self {
            rest_client,
            cache,
            config,
            transactions_enriched: Arc::new(AtomicU64::new(0)),
            lookup_failures: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn cache(&self) -> &UtxoCache {
        &self.cache
    }

    pub fn transactions_enriched(&self) -> u64 {
        self.transactions_enriched.load(Ordering::Relaxed)
    }

    pub fn lookup_failures(&self) -> u64 {
        self.lookup_failures.load(Ordering::Relaxed)
    }

    /// Enriches inputs of a transaction with historical confirmation metadata.
    /// Safely handles cache lookups, bounded concurrency HTTP queries, and request deduplication.
    pub async fn enrich_transaction(&self, tx: &mut TransactionObservation) {
        if tx.inputs.is_empty() {
            return;
        }

        // 1. First pass: resolve already cached inputs
        let mut missing_txids = HashSet::new();

        for input in &mut tx.inputs {
            if input.is_coinbase || input.txid.is_empty() {
                continue;
            }

            if let Some(ctx) = self.cache.get(&input.txid, input.vout) {
                input.historical_utxo = Some(ctx);
            } else if missing_txids.len() < self.config.max_input_enrichment {
                missing_txids.insert(input.txid.clone());
            }
        }

        // 2. Second pass: fetch missing TXIDs concurrently with semaphore throttling
        if !missing_txids.is_empty() {
            let semaphore = Arc::new(Semaphore::new(self.config.lookup_concurrency.max(1)));
            let mut tasks = FuturesUnordered::new();

            for prev_txid in missing_txids {
                let client = self.rest_client.clone();
                let sem = semaphore.clone();
                let failures = self.lookup_failures.clone();

                tasks.push(tokio::spawn(async move {
                    let _permit = match sem.acquire().await {
                        Ok(p) => p,
                        Err(_) => return None,
                    };

                    match client.get_tx(&prev_txid).await {
                        Ok(mempool_tx) => {
                            let confirmed_at = mempool_tx
                                .status
                                .block_time
                                .and_then(|t| DateTime::from_timestamp(t, 0));

                            let outputs = mempool_tx
                                .vout
                                .into_iter()
                                .enumerate()
                                .map(|(idx, vout)| (idx as u32, vout.value))
                                .collect();

                            Some((
                                prev_txid,
                                CachedTxOutputs {
                                    confirmed: mempool_tx.status.confirmed,
                                    block_height: mempool_tx.status.block_height,
                                    confirmed_at,
                                    outputs,
                                },
                            ))
                        }
                        Err(e) => {
                            debug!(txid = %prev_txid, error = %e, "Failed to fetch historical transaction for UTXO enrichment");
                            failures.fetch_add(1, Ordering::Relaxed);
                            None
                        }
                    }
                }));
            }

            while let Some(task_res) = tasks.next().await {
                if let Ok(Some((txid, cached_info))) = task_res {
                    self.cache.insert(txid, cached_info);
                }
            }

            // 3. Third pass: assign newly cached context to inputs
            for input in &mut tx.inputs {
                if input.historical_utxo.is_none() && !input.is_coinbase {
                    if let Some(ctx) = self.cache.get(&input.txid, input.vout) {
                        input.historical_utxo = Some(ctx);
                    }
                }
            }
        }

        self.transactions_enriched.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Json, Router};
    use chrono::Utc;
    use obschain_core::TxInputObservation;
    use tokio::net::TcpListener;

    use crate::mempool_rest::MempoolRestConfig;

    async fn start_mock_enrichment_server() -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new()
            .route(
                "/tx/aaaa000000000000000000000000000000000000000000000000000000000001",
                get(|| async {
                    Json(serde_json::json!({
                        "txid": "aaaa000000000000000000000000000000000000000000000000000000000001",
                        "version": 1,
                        "locktime": 0,
                        "vin": [],
                        "vout": [
                            { "value": 50000000, "scriptpubkey": "0014..." },
                            { "value": 75000000, "scriptpubkey": "0014..." }
                        ],
                        "size": 200,
                        "weight": 800,
                        "status": {
                            "confirmed": true,
                            "block_height": 700000,
                            "block_time": 1630000000
                        }
                    }))
                }),
            )
            .route(
                "/tx/failed_tx",
                get(|| async { (axum::http::StatusCode::NOT_FOUND, "Not found") }),
            );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        (format!("http://{}", addr), handle)
    }

    #[tokio::test]
    async fn test_enricher_populates_historical_utxo() {
        let (base_url, _handle) = start_mock_enrichment_server().await;
        let rest_client = Arc::new(
            MempoolRestClient::new(MempoolRestConfig {
                base_url,
                timeout: Duration::from_secs(2),
                connect_timeout: Duration::from_secs(1),
                max_retries: 1,
                max_response_bytes: 1024 * 1024,
            })
            .unwrap(),
        );

        let cache = UtxoCache::new(100, Duration::from_secs(60));
        let enricher =
            TransactionEnricher::new(rest_client, cache.clone(), EnricherConfig::default());

        let mut tx = TransactionObservation {
            txid: "spending_tx_1".to_string(),
            timestamp: Utc::now(),
            block_hash: None,
            block_height: None,
            fee_sats: 1000,
            size: 220,
            weight: 560,
            vsize: 140,
            fee_rate_sat_vb: Some(7.14),
            total_input_sats: 50000000,
            total_output_sats: 49999000,
            input_count: 1,
            output_count: 1,
            inputs: vec![TxInputObservation {
                txid: "aaaa000000000000000000000000000000000000000000000000000000000001"
                    .to_string(),
                vout: 0,
                sequence: 0xFFFFFFFF,
                prev_out_value_sats: None,
                prev_out_address: None,
                is_coinbase: false,
                historical_utxo: None,
            }],
            outputs: vec![],
            is_rbf: false,
            confirmed: false,
            source: None,
        };

        enricher.enrich_transaction(&mut tx).await;

        let enriched_utxo = tx.inputs[0]
            .historical_utxo
            .as_ref()
            .expect("Should be enriched");
        assert_eq!(enriched_utxo.value_sats, 50000000);
        assert_eq!(enriched_utxo.confirmed_height, Some(700000));
        assert_eq!(enriched_utxo.confirmed_at.unwrap().timestamp(), 1630000000);

        // Verify cache now has it (cache hit on second pass)
        assert_eq!(cache.hits(), 1);
    }

    #[tokio::test]
    async fn test_enricher_failure_isolation() {
        let (base_url, _handle) = start_mock_enrichment_server().await;
        let rest_client = Arc::new(
            MempoolRestClient::new(MempoolRestConfig {
                base_url,
                timeout: Duration::from_secs(2),
                connect_timeout: Duration::from_secs(1),
                max_retries: 1,
                max_response_bytes: 1024 * 1024,
            })
            .unwrap(),
        );

        let cache = UtxoCache::new(100, Duration::from_secs(60));
        let enricher = TransactionEnricher::new(rest_client, cache, EnricherConfig::default());

        let mut tx = TransactionObservation {
            txid: "tx_with_unknown_input".to_string(),
            timestamp: Utc::now(),
            block_hash: None,
            block_height: None,
            fee_sats: 500,
            size: 200,
            weight: 500,
            vsize: 125,
            fee_rate_sat_vb: Some(4.0),
            total_input_sats: 10000,
            total_output_sats: 9500,
            input_count: 1,
            output_count: 1,
            inputs: vec![TxInputObservation {
                txid: "failed_tx".to_string(),
                vout: 0,
                sequence: 0xFFFFFFFF,
                prev_out_value_sats: None,
                prev_out_address: None,
                is_coinbase: false,
                historical_utxo: None,
            }],
            outputs: vec![],
            is_rbf: false,
            confirmed: false,
            source: None,
        };

        // Must not panic or return error
        enricher.enrich_transaction(&mut tx).await;
        assert!(tx.inputs[0].historical_utxo.is_none());
        assert_eq!(enricher.lookup_failures(), 1);
    }
}
