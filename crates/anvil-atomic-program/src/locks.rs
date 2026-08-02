use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, Weak},
};

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use crate::model::ObjectPath;

/// The nominated executor's node-local lock table.
///
/// It never decides nomination, placement, or networking. The caller must
/// resolve the complete bounded path set before acquiring any lock.
#[derive(Debug, Clone, Default)]
pub struct LocalLockManager {
    inner: Arc<Mutex<BTreeMap<ObjectPath, Weak<AsyncMutex<()>>>>>,
}

impl LocalLockManager {
    /// Acquires a deduplicated set in canonical `ObjectPath` order.
    pub async fn acquire(&self, paths: &[ObjectPath]) -> LocalLockGuard {
        let ordered: Vec<_> = paths
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();

        let mutexes = {
            let mut table = self.inner.lock().expect("local lock table poisoned");
            table.retain(|_, lock| lock.strong_count() > 0);
            ordered
                .iter()
                .map(|path| {
                    if let Some(lock) = table.get(path).and_then(Weak::upgrade) {
                        lock
                    } else {
                        let lock = Arc::new(AsyncMutex::new(()));
                        table.insert(path.clone(), Arc::downgrade(&lock));
                        lock
                    }
                })
                .collect::<Vec<_>>()
        };

        let mut guards = Vec::with_capacity(mutexes.len());
        for lock in mutexes {
            guards.push(lock.lock_owned().await);
        }
        LocalLockGuard {
            paths: ordered,
            _guards: guards,
        }
    }
}

/// Holds all requested local path locks until dropped.
#[derive(Debug)]
pub struct LocalLockGuard {
    paths: Vec<ObjectPath>,
    _guards: Vec<OwnedMutexGuard<()>>,
}

impl LocalLockGuard {
    pub fn paths(&self) -> &[ObjectPath] {
        &self.paths
    }
}
