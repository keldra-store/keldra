use std::collections::HashMap;
use std::sync::{Arc, Weak};

use tokio::sync::{Mutex, OwnedMutexGuard};

#[derive(Default)]
pub(crate) struct GroupLocks {
    entries: Mutex<HashMap<String, Weak<Mutex<()>>>>,
}

impl GroupLocks {
    pub(crate) async fn acquire(&self, group_id: &str) -> OwnedMutexGuard<()> {
        let lock = {
            let mut entries = self.entries.lock().await;
            entries.retain(|_, lock| lock.strong_count() > 0);
            match entries.get(group_id).and_then(Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    let lock = Arc::new(Mutex::new(()));
                    entries.insert(group_id.to_string(), Arc::downgrade(&lock));
                    lock
                }
            }
        };
        lock.lock_owned().await
    }

    #[cfg(test)]
    async fn cached_groups(&self) -> usize {
        self.entries.lock().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn same_group_serializes_and_idle_entries_are_pruned() {
        let locks = Arc::new(GroupLocks::default());
        let first = locks.acquire("one").await;
        let contender = {
            let locks = locks.clone();
            tokio::spawn(async move { locks.acquire("one").await })
        };
        tokio::task::yield_now().await;
        assert!(!contender.is_finished());
        drop(first);
        drop(contender.await.unwrap());

        drop(locks.acquire("two").await);
        drop(locks.acquire("three").await);
        assert_eq!(locks.cached_groups().await, 1);
    }
}
