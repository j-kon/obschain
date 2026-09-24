use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use obschain_core::SpentOutputContext;

/// Cached outputs and confirmation context for a historical Bitcoin transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedTxOutputs {
    pub confirmed: bool,
    pub block_height: Option<u64>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub outputs: Vec<(u32, u64)>, // (vout_index, value_sats)
}

struct CacheEntry {
    data: CachedTxOutputs,
    inserted_at: Instant,
}

struct UtxoCacheInner {
    map: HashMap<String, CacheEntry>,
    order: VecDeque<String>,
    max_capacity: usize,
    ttl: Duration,
}

impl UtxoCacheInner {
    fn clean_expired(&mut self) {
        let ttl = self.ttl;
        let mut expired_keys = Vec::new();

        for (k, entry) in &self.map {
            if entry.inserted_at.elapsed() >= ttl {
                expired_keys.push(k.clone());
            }
        }

        for k in expired_keys {
            self.map.remove(&k);
            self.order.retain(|item| item != &k);
        }
    }
}

/// Thread-safe, bounded, TTL-aware in-memory cache for historical UTXO lookups.
#[derive(Clone)]
pub struct UtxoCache {
    inner: Arc<RwLock<UtxoCacheInner>>,
    hits: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
    evictions: Arc<AtomicU64>,
}

impl UtxoCache {
    pub const DEFAULT_MAX_CAPACITY: usize = 50_000;
    pub const DEFAULT_TTL_SECS: u64 = 3600; // 1 hour

    pub fn new(max_capacity: usize, ttl: Duration) -> Self {
        let capacity = max_capacity.max(1);
        Self {
            inner: Arc::new(RwLock::new(UtxoCacheInner {
                map: HashMap::with_capacity(capacity.min(5000)),
                order: VecDeque::with_capacity(capacity.min(5000)),
                max_capacity: capacity,
                ttl,
            })),
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
            evictions: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Retrieves historical spent output context for a given outpoint `(txid, vout)`.
    pub fn get(&self, txid: &str, vout: u32) -> Option<SpentOutputContext> {
        let mut inner = self.inner.write().ok()?;

        if let Some(entry) = inner.map.get(txid) {
            if entry.inserted_at.elapsed() < inner.ttl {
                self.hits.fetch_add(1, Ordering::Relaxed);
                let (confirmed_height, confirmed_at) =
                    (entry.data.block_height, entry.data.confirmed_at);

                for (idx, val) in &entry.data.outputs {
                    if *idx == vout {
                        return Some(SpentOutputContext {
                            txid: txid.to_string(),
                            vout,
                            value_sats: *val,
                            confirmed_height,
                            confirmed_at,
                        });
                    }
                }
                return None;
            } else {
                // Expired entry
                inner.map.remove(txid);
                inner.order.retain(|k| k != txid);
            }
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Inserts a transaction's confirmation details and outputs into the bounded cache.
    pub fn insert(&self, txid: String, tx_info: CachedTxOutputs) {
        if let Ok(mut inner) = self.inner.write() {
            // Evict expired entries if approaching capacity
            if inner.map.len() >= inner.max_capacity {
                inner.clean_expired();
            }

            // Bounded eviction: pop oldest from order queue
            while inner.map.len() >= inner.max_capacity {
                if let Some(oldest) = inner.order.pop_front() {
                    inner.map.remove(&oldest);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                } else {
                    break;
                }
            }

            inner.map.insert(
                txid.clone(),
                CacheEntry {
                    data: tx_info,
                    inserted_at: Instant::now(),
                },
            );
            inner.order.push_back(txid);
        }
    }

    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    pub fn len(&self) -> usize {
        self.inner.read().map(|i| i.map.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for UtxoCache {
    fn default() -> Self {
        Self::new(
            Self::DEFAULT_MAX_CAPACITY,
            Duration::from_secs(Self::DEFAULT_TTL_SECS),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_hit_and_miss() {
        let cache = UtxoCache::new(100, Duration::from_secs(60));
        let txid = "0000000000000000000000000000000000000000000000000000000000000001".to_string();

        assert!(cache.get(&txid, 0).is_none());
        assert_eq!(cache.misses(), 1);
        assert_eq!(cache.hits(), 0);

        let info = CachedTxOutputs {
            confirmed: true,
            block_height: Some(700000),
            confirmed_at: Some(Utc::now()),
            outputs: vec![(0, 50000000), (1, 100000000)],
        };

        cache.insert(txid.clone(), info);
        assert_eq!(cache.len(), 1);

        let ctx0 = cache.get(&txid, 0).expect("Should find vout 0");
        assert_eq!(ctx0.value_sats, 50000000);
        assert_eq!(ctx0.confirmed_height, Some(700000));

        let ctx1 = cache.get(&txid, 1).expect("Should find vout 1");
        assert_eq!(ctx1.value_sats, 100000000);

        assert_eq!(cache.hits(), 2);
    }

    #[test]
    fn test_cache_bounded_capacity_eviction() {
        let capacity = 3;
        let cache = UtxoCache::new(capacity, Duration::from_secs(60));

        for i in 0..5 {
            let txid = format!("tx_{i}");
            cache.insert(
                txid,
                CachedTxOutputs {
                    confirmed: true,
                    block_height: Some(800000 + i),
                    confirmed_at: Some(Utc::now()),
                    outputs: vec![(0, 1000)],
                },
            );
        }

        // Must not exceed max capacity
        assert_eq!(cache.len(), capacity);
        assert_eq!(cache.evictions(), 2);

        // Oldest (tx_0, tx_1) should have been evicted
        assert!(cache.get("tx_0", 0).is_none());
        assert!(cache.get("tx_1", 0).is_none());
        // Newer (tx_2, tx_3, tx_4) should be present
        assert!(cache.get("tx_2", 0).is_some());
        assert!(cache.get("tx_3", 0).is_some());
        assert!(cache.get("tx_4", 0).is_some());
    }

    #[test]
    fn test_cache_ttl_expiry() {
        // Cache with 50ms TTL
        let cache = UtxoCache::new(100, Duration::from_millis(50));
        let txid = "tx_expiring".to_string();

        cache.insert(
            txid.clone(),
            CachedTxOutputs {
                confirmed: true,
                block_height: Some(500000),
                confirmed_at: Some(Utc::now()),
                outputs: vec![(0, 20000)],
            },
        );

        assert!(cache.get(&txid, 0).is_some());

        // Wait for TTL to expire
        std::thread::sleep(Duration::from_millis(60));
        assert!(cache.get(&txid, 0).is_none());
    }
}
