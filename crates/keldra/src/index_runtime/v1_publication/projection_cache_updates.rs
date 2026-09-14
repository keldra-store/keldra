//! Bounded, ordered maintenance of the disposable local projection cache.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use keldra_store::Store;

#[derive(Clone)]
pub(super) struct ProjectionCacheAdvancer(Arc<AdvancerInner>);

struct AdvancerInner {
    sender: Mutex<Option<tokio::sync::mpsc::UnboundedSender<AdmittedCacheAdvance>>>,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
    bytes: CacheByteBudget,
}

pub(super) struct AdmittedCacheAdvance {
    partition: Vec<u8>,
    predecessor_generation: Option<[u8; 32]>,
    current_generation: [u8; 32],
    updates: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    _admission: Vec<CacheBytePermit>,
}

#[derive(Clone)]
struct CacheByteBudget {
    used: Arc<AtomicUsize>,
    maximum: usize,
}

struct CacheBytePermit {
    used: Arc<AtomicUsize>,
    bytes: usize,
}

impl ProjectionCacheAdvancer {
    pub(super) fn start(store: Store, maximum_bytes: usize) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let worker = tokio::spawn(run_worker(store, receiver));
        Self(Arc::new(AdvancerInner {
            sender: Mutex::new(Some(sender)),
            worker: Mutex::new(Some(worker)),
            bytes: CacheByteBudget {
                used: Arc::new(AtomicUsize::new(0)),
                maximum: maximum_bytes,
            },
        }))
    }

    pub(super) fn admit(
        &self,
        partition: Vec<u8>,
        predecessor_generation: Option<[u8; 32]>,
        current_generation: [u8; 32],
        updates: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> Option<AdmittedCacheAdvance> {
        let updates = updates.into_iter().collect::<BTreeMap<_, _>>();
        let Some(bytes) = resident_bytes(&partition, &updates) else {
            return None;
        };
        let Some(admission) = self.0.bytes.try_acquire(bytes) else {
            return None;
        };
        Some(AdmittedCacheAdvance {
            partition,
            predecessor_generation,
            current_generation,
            updates,
            _admission: vec![admission],
        })
    }

    pub(super) fn schedule(&self, advance: AdmittedCacheAdvance) -> bool {
        self.0
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|sender| sender.send(advance).is_ok())
    }
}

impl Drop for AdvancerInner {
    fn drop(&mut self) {
        self.sender
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = self
            .worker
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            // Do not let disposable cache backlog extend runtime shutdown.
            // One blocking store operation may finish; queued work is dropped
            // with the receiver.
            worker.abort();
        }
    }
}

impl AdmittedCacheAdvance {
    fn coalesce(&mut self, newer: Self) {
        debug_assert_eq!(self.partition, newer.partition);
        for (key, value) in newer.updates {
            self.updates.insert(key, value);
        }
        self.current_generation = newer.current_generation;
        self._admission.extend(newer._admission);
        // Retain the oldest predecessor: the merged deltas cover every
        // successor through the newest Current.
    }
}

impl CacheByteBudget {
    fn try_acquire(&self, bytes: usize) -> Option<CacheBytePermit> {
        let mut used = self.used.load(Ordering::Relaxed);
        loop {
            let next = used.checked_add(bytes)?;
            if next > self.maximum {
                return None;
            }
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => {
                    return Some(CacheBytePermit {
                        used: self.used.clone(),
                        bytes,
                    });
                }
                Err(observed) => used = observed,
            }
        }
    }
}

impl Drop for CacheBytePermit {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

async fn run_worker(
    store: Store,
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<AdmittedCacheAdvance>,
) {
    let mut pending = BTreeMap::<Vec<u8>, AdmittedCacheAdvance>::new();
    let mut order = VecDeque::new();
    loop {
        if pending.is_empty() {
            let Some(advance) = receiver.recv().await else {
                return;
            };
            merge_pending(&mut pending, &mut order, advance);
        }
        // Bound receive-side batching so a continuously busy publisher cannot
        // starve cache application indefinitely.
        for _ in 0..256 {
            let Ok(advance) = receiver.try_recv() else {
                break;
            };
            merge_pending(&mut pending, &mut order, advance);
        }
        let Some(partition) = order.pop_front() else {
            continue;
        };
        let advance = pending
            .remove(&partition)
            .expect("projection cache order names pending work");
        let store = store.clone();
        let started = Instant::now();
        let outcome = tokio::task::spawn_blocking(move || {
            let updates = advance.updates.into_iter().collect::<Vec<_>>();
            store.advance_index_projection_state(
                &advance.partition,
                advance.predecessor_generation,
                advance.current_generation,
                &updates,
            )
        })
        .await;
        match outcome {
            Ok(Ok(cache_advanced)) => tracing::info!(
                histogram.keldra_index_v1_projection_cache_advance_duration_seconds =
                    started.elapsed().as_secs_f64(),
                cache_advanced,
                "v1 disposable projection cache update completed"
            ),
            Ok(Err(error)) => tracing::warn!(
                %error,
                histogram.keldra_index_v1_projection_cache_advance_duration_seconds =
                    started.elapsed().as_secs_f64(),
                "v1 local projected-state cache update failed"
            ),
            Err(error) => tracing::warn!(
                %error,
                "v1 local projected-state cache worker join failed"
            ),
        }
    }
}

fn merge_pending(
    pending: &mut BTreeMap<Vec<u8>, AdmittedCacheAdvance>,
    order: &mut VecDeque<Vec<u8>>,
    advance: AdmittedCacheAdvance,
) {
    if let Some(current) = pending.get_mut(&advance.partition) {
        current.coalesce(advance);
    } else {
        order.push_back(advance.partition.clone());
        pending.insert(advance.partition.clone(), advance);
    }
}

fn resident_bytes(
    partition: &Vec<u8>,
    updates: &BTreeMap<Vec<u8>, Option<Vec<u8>>>,
) -> Option<usize> {
    // Charge payload capacities plus conservative tree node/link storage. The
    // allocator's private bookkeeping may round upward, so this intentionally
    // over-admits rather than pretending payload lengths are resident bytes.
    let node_bytes = std::mem::size_of::<(Vec<u8>, Option<Vec<u8>>)>()
        .checked_add(4 * std::mem::size_of::<usize>())?;
    let mut bytes = partition
        .capacity()
        .checked_add(updates.len().checked_mul(node_bytes)?)?;
    for (key, value) in updates {
        bytes = bytes.checked_add(key.capacity())?;
        if let Some(value) = value {
            bytes = bytes.checked_add(value.capacity())?;
        }
    }
    Some(bytes.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn advance(
        predecessor: Option<[u8; 32]>,
        current: [u8; 32],
        updates: &[(&[u8], Option<&[u8]>)],
        admission: CacheBytePermit,
    ) -> AdmittedCacheAdvance {
        AdmittedCacheAdvance {
            partition: b"partition".to_vec(),
            predecessor_generation: predecessor,
            current_generation: current,
            updates: updates
                .iter()
                .map(|(key, value)| (key.to_vec(), value.map(<[u8]>::to_vec)))
                .collect(),
            _admission: vec![admission],
        }
    }

    #[test]
    fn coalescing_retains_oldest_predecessor_and_newest_values() {
        let budget = CacheByteBudget {
            used: Arc::new(AtomicUsize::new(0)),
            maximum: 100,
        };
        let mut older = advance(
            Some([1; 32]),
            [2; 32],
            &[(b"a", Some(b"old")), (b"b", Some(b"kept"))],
            budget.try_acquire(10).unwrap(),
        );
        older.coalesce(advance(
            Some([2; 32]),
            [3; 32],
            &[(b"a", Some(b"new"))],
            budget.try_acquire(10).unwrap(),
        ));
        assert_eq!(older.predecessor_generation, Some([1; 32]));
        assert_eq!(older.current_generation, [3; 32]);
        assert_eq!(older.updates[b"a".as_slice()], Some(b"new".to_vec()));
        assert_eq!(older.updates[b"b".as_slice()], Some(b"kept".to_vec()));
        assert_eq!(budget.used.load(Ordering::Relaxed), 20);
    }

    #[test]
    fn byte_budget_refuses_unaccounted_backlog() {
        let budget = CacheByteBudget {
            used: Arc::new(AtomicUsize::new(0)),
            maximum: 10,
        };
        let permit = budget.try_acquire(7).unwrap();
        assert!(budget.try_acquire(4).is_none());
        drop(permit);
        assert!(budget.try_acquire(10).is_some());
    }
}
