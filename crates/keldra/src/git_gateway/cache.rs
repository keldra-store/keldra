use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

const DEFAULT_REPOSITORY_CACHE_BYTES: u64 = 4 * 1024 * 1024 * 1024;

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

    fn active_keys(&self) -> HashSet<String> {
        let mut values = self.values.lock().expect("Git lock catalogue poisoned");
        values.retain(|_, value| value.strong_count() > 0);
        values.keys().cloned().collect()
    }
}

pub(crate) struct RepositoryCache {
    maximum_bytes: u64,
    maintenance: tokio::sync::Mutex<()>,
}

impl Default for RepositoryCache {
    fn default() -> Self {
        Self {
            maximum_bytes: DEFAULT_REPOSITORY_CACHE_BYTES,
            maintenance: tokio::sync::Mutex::new(()),
        }
    }
}

impl RepositoryCache {
    pub(super) async fn reconcile(&self, root: &Path, locks: &RepositoryLocks) -> io::Result<()> {
        let _maintenance = self.maintenance.lock().await;
        let root = root.to_owned();
        let active = locks.active_keys();
        let maximum = self.maximum_bytes;
        tokio::task::spawn_blocking(move || prune_repository_cache(&root, &active, maximum))
            .await
            .map_err(|error| io::Error::other(format!("join Git cache maintenance: {error}")))?
    }
}

fn prune_repository_cache(
    root: &Path,
    active: &HashSet<String>,
    maximum_bytes: u64,
) -> io::Result<()> {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    let mut total = 0_u64;
    let mut removable = Vec::new();
    for entry in entries {
        let entry = entry?;
        let metadata = entry.path().symlink_metadata()?;
        if !metadata.file_type().is_dir() {
            continue;
        }
        let path = entry.path();
        let bytes = directory_bytes(&path)?;
        total = total.saturating_add(bytes);
        let key = entry.file_name().to_string_lossy().into_owned();
        if !active.contains(&key) {
            removable.push((metadata.modified().ok(), path, bytes));
        }
    }
    removable.sort_by_key(|(modified, path, _)| (*modified, path.clone()));
    for (_, path, bytes) in removable {
        if total <= maximum_bytes {
            break;
        }
        std::fs::remove_dir_all(&path)?;
        total = total.saturating_sub(bytes);
    }
    if total > maximum_bytes {
        return Err(io::Error::other(format!(
            "active Git repositories use {total} cache bytes, above the {maximum_bytes}-byte budget"
        )));
    }
    Ok(())
}

fn directory_bytes(root: &Path) -> io::Result<u64> {
    let mut bytes = 0_u64;
    let mut pending = vec![PathBuf::from(root)];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let metadata = entry.path().symlink_metadata()?;
            let kind = metadata.file_type();
            if kind.is_dir() {
                pending.push(entry.path());
            } else if kind.is_file() {
                bytes = bytes.saturating_add(metadata.len());
            }
        }
    }
    Ok(bytes)
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

    #[tokio::test]
    async fn disk_budget_removes_old_inactive_materializations_but_keeps_active_ones() {
        let root = tempfile::tempdir().unwrap();
        let old = root.path().join("old");
        let active = root.path().join("active");
        std::fs::create_dir_all(&old).unwrap();
        std::fs::create_dir_all(&active).unwrap();
        std::fs::write(old.join("pack"), vec![1_u8; 8]).unwrap();
        std::fs::write(active.join("pack"), vec![2_u8; 8]).unwrap();
        let locks = RepositoryLocks::default();
        let _pin = locks.get("active");
        let cache = RepositoryCache {
            maximum_bytes: 8,
            maintenance: tokio::sync::Mutex::new(()),
        };

        cache.reconcile(root.path(), &locks).await.unwrap();

        assert!(!old.exists());
        assert!(active.exists());
    }

    #[tokio::test]
    async fn over_budget_materialization_is_reclaimable_after_its_pin_drops() {
        let root = tempfile::tempdir().unwrap();
        let repository = root.path().join("repository");
        std::fs::create_dir_all(&repository).unwrap();
        std::fs::write(repository.join("pack"), vec![1_u8; 16]).unwrap();
        let locks = RepositoryLocks::default();
        let pin = locks.get("repository");
        let cache = RepositoryCache {
            maximum_bytes: 8,
            maintenance: tokio::sync::Mutex::new(()),
        };

        assert!(cache.reconcile(root.path(), &locks).await.is_err());
        drop(pin);
        cache.reconcile(root.path(), &locks).await.unwrap();
        assert!(!repository.exists());
    }
}
