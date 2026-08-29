use std::path::Path;

use super::*;

pub(super) fn wal_directory_bytes(directory: &Path) -> std::io::Result<u64> {
    let mut total = 0_u64;
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        if entry
            .path()
            .extension()
            .is_none_or(|extension| extension != "log")
        {
            continue;
        }
        match entry.metadata() {
            Ok(metadata) if metadata.is_file() => {
                total = total
                    .checked_add(metadata.len())
                    .ok_or_else(|| std::io::Error::other("RocksDB WAL byte count overflow"))?;
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(total)
}

impl StoreOptions {
    pub fn new(root: impl AsRef<Path>, node_id: u16) -> Self {
        let root = root.as_ref().to_path_buf();
        Self {
            metadata_directory: root.join("metadata"),
            metadata_wal_directory: root.join("metadata"),
            payload_directory: root.join("blobs"),
            pending_upload_max_bytes: crate::blob::DEFAULT_PENDING_UPLOAD_MAX_BYTES,
            max_total_wal_bytes: DEFAULT_MAX_TOTAL_WAL_BYTES,
            root,
            node_id,
            sync_writes: true,
            watch_retention: WatchRetention::default(),
            mutation_receipt_retention: MutationReceiptRetention::default(),
            awaiting_publish_ttl_seconds: DEFAULT_AWAITING_PUBLISH_TTL_SECONDS,
        }
    }

    pub fn with_metadata_directory(mut self, directory: impl AsRef<Path>) -> Self {
        self.metadata_directory = directory.as_ref().to_path_buf();
        self
    }

    pub fn with_metadata_wal_directory(mut self, directory: impl AsRef<Path>) -> Self {
        self.metadata_wal_directory = directory.as_ref().to_path_buf();
        self
    }

    pub fn with_payload_directory(mut self, directory: impl AsRef<Path>) -> Self {
        self.payload_directory = directory.as_ref().to_path_buf();
        self
    }

    pub fn with_pending_upload_max_bytes(mut self, max_bytes: u64) -> Self {
        self.pending_upload_max_bytes = max_bytes;
        self
    }

    pub fn with_max_total_wal_bytes(mut self, max_total_wal_bytes: u64) -> Self {
        self.max_total_wal_bytes = max_total_wal_bytes;
        self
    }

    pub fn with_watch_retention(mut self, watch_retention: WatchRetention) -> Self {
        self.watch_retention = watch_retention;
        self
    }

    pub fn with_mutation_receipt_retention(
        mut self,
        mutation_receipt_retention: MutationReceiptRetention,
    ) -> Self {
        self.mutation_receipt_retention = mutation_receipt_retention;
        self
    }

    pub fn with_awaiting_publish_ttl_seconds(mut self, ttl_seconds: u64) -> Self {
        self.awaiting_publish_ttl_seconds = ttl_seconds;
        self
    }
}

pub(super) async fn validate_authoritative_roots(
    options: &StoreOptions,
    existing_database: bool,
) -> Result<()> {
    let roots = [
        ("metadata", &options.metadata_directory),
        ("WAL", &options.metadata_wal_directory),
        ("payload", &options.payload_directory),
    ];
    if existing_database {
        for (role, path) in roots {
            let metadata = tokio::fs::metadata(path).await.with_context(|| {
                format!(
                    "required Keldra {role} root is unavailable: {}",
                    path.display()
                )
            })?;
            if !metadata.is_dir() {
                anyhow::bail!(
                    "required Keldra {role} root is not a directory: {}",
                    path.display()
                );
            }
        }
        return Ok(());
    }

    let mut fresh_roots = BTreeMap::<PathBuf, BTreeSet<&str>>::new();
    for (role, path) in roots {
        fresh_roots.entry(path.clone()).or_default().insert(role);
    }
    for (path, roles) in fresh_roots {
        if !tokio::fs::try_exists(&path).await? {
            continue;
        }
        let role = roles.iter().copied().collect::<Vec<_>>().join("/");
        let metadata = tokio::fs::metadata(&path).await?;
        if !metadata.is_dir() {
            anyhow::bail!(
                "fresh Keldra {role} root is not a directory: {}",
                path.display()
            );
        }
        let mut allowed = BTreeSet::new();
        if roles.contains("metadata") {
            allowed.insert(".keldra-metadata-root-v1.json");
        }
        if roles.contains("WAL") {
            allowed.insert(".keldra-metadata_wal-root-v1.json");
        }
        if roles.contains("payload") {
            allowed.insert(".keldra-payload-root-v1.json");
        }
        if path == options.root {
            allowed.insert(".keldra-state-root-v1.json");
            allowed.insert("storage-layout-v1.json");
        }
        let mut entries = tokio::fs::read_dir(&path).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let permitted = name.to_str().is_some_and(|name| allowed.contains(name))
                && entry.file_type().await?.is_file();
            if !permitted {
                anyhow::bail!(
                    "fresh Keldra initialization refuses non-empty authoritative {role} root: {}",
                    path.display()
                );
            }
        }
    }
    Ok(())
}
