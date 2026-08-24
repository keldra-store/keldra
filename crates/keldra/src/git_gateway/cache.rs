use std::collections::HashMap;
use std::sync::{Arc, Weak};

#[derive(Default)]
pub(crate) struct RepositoryLocks {
    values: std::sync::Mutex<HashMap<String, Weak<tokio::sync::RwLock<()>>>>,
}

impl RepositoryLocks {
    pub(super) fn get(&self, repository_id: &str) -> Arc<tokio::sync::RwLock<()>> {
        let mut values = self.values.lock().expect("Git lock catalogue poisoned");
        if let Some(lock) = values.get(repository_id).and_then(Weak::upgrade) {
            return lock;
        }
        values.retain(|_, value| value.strong_count() > 0);
        let lock = Arc::new(tokio::sync::RwLock::new(()));
        values.insert(repository_id.to_owned(), Arc::downgrade(&lock));
        lock
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn serializes_one_repository_without_blocking_another() {
        let locks = RepositoryLocks::default();
        let first = locks.get("first");
        assert!(Arc::ptr_eq(&first, &locks.get("first")));

        let _first_guard = first.write().await;
        let second = locks.get("second");
        assert!(second.try_write().is_ok());
        let first_again = locks.get("first");
        assert!(first_again.try_write().is_err());
    }

    #[test]
    fn removes_catalogue_entries_after_last_handle_drops() {
        let locks = RepositoryLocks::default();
        let first = locks.get("first");
        drop(first);
        let replacement = locks.get("first");
        assert_eq!(Arc::strong_count(&replacement), 1);
    }
}
