use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use obschain_core::ChainEvent;

/// Deterministic, bounded in-memory event deduplicator to prevent duplicate
/// events from live feed reconnects or replay bursts.
#[derive(Clone)]
pub struct EventDeduplicator {
    inner: Arc<RwLock<DedupInner>>,
    deduplicated_count: Arc<AtomicU64>,
    passed_count: Arc<AtomicU64>,
}

struct DedupInner {
    seen: HashMap<String, Instant>,
    order: VecDeque<String>,
    max_capacity: usize,
    ttl: Duration,
}

impl EventDeduplicator {
    pub const DEFAULT_MAX_CAPACITY: usize = 10_000;
    pub const DEFAULT_TTL_SECS: u64 = 1800; // 30 minutes

    pub fn new(max_capacity: usize, ttl: Duration) -> Self {
        let capacity = max_capacity.max(1);
        Self {
            inner: Arc::new(RwLock::new(DedupInner {
                seen: HashMap::with_capacity(capacity.min(5000)),
                order: VecDeque::with_capacity(capacity.min(5000)),
                max_capacity: capacity,
                ttl,
            })),
            deduplicated_count: Arc::new(AtomicU64::new(0)),
            passed_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Generates a deterministic deduplication key for an event.
    pub fn event_key(event: &ChainEvent) -> String {
        if let Some(ref txid) = event.txid {
            format!("{:?}:tx:{}", event.event_type, txid)
        } else if let Some(ref block_hash) = event.block_hash {
            format!("{:?}:block:{}", event.event_type, block_hash)
        } else if let Some(block_height) = event.block_height {
            format!("{:?}:height:{}", event.event_type, block_height)
        } else {
            format!("{:?}:title:{}", event.event_type, event.title)
        }
    }

    /// Filters a batch of events, returning only fresh events and dropping duplicates.
    pub fn filter(&self, events: Vec<ChainEvent>) -> Vec<ChainEvent> {
        let mut fresh = Vec::with_capacity(events.len());

        let Ok(mut inner) = self.inner.write() else {
            return events;
        };

        let now = Instant::now();
        let ttl = inner.ttl;

        for event in events {
            let key = Self::event_key(&event);

            if let Some(seen_at) = inner.seen.get(&key) {
                if now.duration_since(*seen_at) < ttl {
                    // Duplicate within TTL window
                    self.deduplicated_count.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(event_type = ?event.event_type, key = %key, "Dropping duplicate chain event");
                    continue;
                }
            }

            // Fresh event: maintain capacity bounds
            while inner.seen.len() >= inner.max_capacity {
                if let Some(oldest) = inner.order.pop_front() {
                    inner.seen.remove(&oldest);
                } else {
                    break;
                }
            }

            inner.seen.insert(key.clone(), now);
            inner.order.push_back(key);
            self.passed_count.fetch_add(1, Ordering::Relaxed);
            fresh.push(event);
        }

        fresh
    }

    pub fn deduplicated_count(&self) -> u64 {
        self.deduplicated_count.load(Ordering::Relaxed)
    }

    pub fn passed_count(&self) -> u64 {
        self.passed_count.load(Ordering::Relaxed)
    }

    pub fn len(&self) -> usize {
        self.inner.read().map(|i| i.seen.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for EventDeduplicator {
    fn default() -> Self {
        Self::new(
            Self::DEFAULT_MAX_CAPACITY,
            Duration::from_secs(Self::DEFAULT_TTL_SECS),
        )
    }
}

#[cfg(test)]
mod tests {
    use obschain_core::{ConfidenceLevel, EventSeverity, EventType};

    use super::*;

    fn make_test_event(event_type: EventType, txid: Option<&str>) -> ChainEvent {
        let mut ev = ChainEvent::new(
            event_type,
            EventSeverity::Low,
            ConfidenceLevel::VerifiedOnChain,
            "Test Event",
            "Test Description",
        );
        ev.txid = txid.map(|s| s.to_string());
        ev
    }

    #[test]
    fn test_deduplicates_identical_event_txid() {
        let dedup = EventDeduplicator::default();

        let ev1 = make_test_event(EventType::LargeTransfer, Some("tx_123"));
        let ev2 = make_test_event(EventType::LargeTransfer, Some("tx_123"));

        let res1 = dedup.filter(vec![ev1]);
        assert_eq!(res1.len(), 1);
        assert_eq!(dedup.passed_count(), 1);
        assert_eq!(dedup.deduplicated_count(), 0);

        let res2 = dedup.filter(vec![ev2]);
        assert_eq!(res2.len(), 0, "Duplicate event must be dropped");
        assert_eq!(dedup.passed_count(), 1);
        assert_eq!(dedup.deduplicated_count(), 1);
    }

    #[test]
    fn test_different_event_types_for_same_txid_are_allowed() {
        let dedup = EventDeduplicator::default();

        let ev1 = make_test_event(EventType::Consolidation, Some("tx_shared"));
        let ev2 = make_test_event(EventType::ExtremeFee, Some("tx_shared"));

        let res = dedup.filter(vec![ev1, ev2]);
        assert_eq!(res.len(), 2, "Different event types must not collide");
    }

    #[test]
    fn test_ttl_expiry_allows_new_event() {
        let dedup = EventDeduplicator::new(100, Duration::from_millis(50));

        let ev1 = make_test_event(EventType::Consolidation, Some("tx_expiring"));
        assert_eq!(dedup.filter(vec![ev1.clone()]).len(), 1);

        // Immediate replay is dropped
        assert_eq!(dedup.filter(vec![ev1.clone()]).len(), 0);

        // Wait for TTL expiry
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(dedup.filter(vec![ev1]).len(), 1);
    }

    #[test]
    fn test_bounded_capacity_eviction() {
        let dedup = EventDeduplicator::new(3, Duration::from_secs(60));

        for i in 0..5 {
            let ev = make_test_event(EventType::LargeTransfer, Some(&format!("tx_{i}")));
            assert_eq!(dedup.filter(vec![ev]).len(), 1);
        }

        assert_eq!(dedup.len(), 3);
        // Oldest (tx_0) should have been evicted and can be accepted again
        let ev_old = make_test_event(EventType::LargeTransfer, Some("tx_0"));
        assert_eq!(dedup.filter(vec![ev_old]).len(), 1);
    }
}
