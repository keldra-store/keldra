use std::path::{Path, PathBuf};

use super::*;

/// Bounded RocksDB-native resources owned by one [`Store`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RocksDbResourceBudget {
    pub block_cache_bytes: u64,
    pub write_buffer_manager_bytes: u64,
    pub column_family_write_buffer_bytes: u64,
    pub background_jobs: u32,
    pub subcompactions: u32,
}

impl RocksDbResourceBudget {
    pub fn validate(self) -> Result<Self, MutationError> {
        if self.block_cache_bytes == 0
            || self.write_buffer_manager_bytes == 0
            || self.column_family_write_buffer_bytes == 0
            || self.background_jobs == 0
            || self.subcompactions == 0
            || self.column_family_write_buffer_bytes > self.write_buffer_manager_bytes
            || self.subcompactions > self.background_jobs
            || usize::try_from(self.block_cache_bytes).is_err()
            || usize::try_from(self.write_buffer_manager_bytes).is_err()
            || usize::try_from(self.column_family_write_buffer_bytes).is_err()
            || i32::try_from(self.background_jobs).is_err()
        {
            return Err(MutationError::Storage(
                "RocksDB resource budget is invalid".into(),
            ));
        }
        Ok(self)
    }
}

impl Default for RocksDbResourceBudget {
    fn default() -> Self {
        Self {
            block_cache_bytes: DEFAULT_ROCKSDB_BLOCK_CACHE_BYTES,
            write_buffer_manager_bytes: DEFAULT_ROCKSDB_WRITE_BUFFER_MANAGER_BYTES,
            column_family_write_buffer_bytes: DEFAULT_ROCKSDB_COLUMN_FAMILY_WRITE_BUFFER_BYTES,
            background_jobs: DEFAULT_ROCKSDB_BACKGROUND_JOBS,
            subcompactions: DEFAULT_ROCKSDB_SUBCOMPACTIONS,
        }
    }
}

#[derive(Clone, Debug)]
pub struct StoreOptions {
    /// Root from which default authoritative paths are derived.
    pub root: PathBuf,
    /// RocksDB directory containing the metadata column families and SSTs.
    pub metadata_directory: PathBuf,
    /// RocksDB directory containing the metadata write-ahead log.
    pub metadata_wal_directory: PathBuf,
    /// RocksDB column-family path containing integrated payload SST/blob files.
    pub payload_directory: PathBuf,
    /// Aggregate hard capacity admitted across active unfinished uploads.
    pub pending_upload_max_bytes: u64,
    /// Shared RocksDB WAL flush/admission high-water target.
    pub max_total_wal_bytes: u64,
    /// Explicit native cache, memtable, and background-work allocation.
    pub rocksdb_resources: RocksDbResourceBudget,
    pub node_id: u16,
    pub sync_writes: bool,
    pub watch_retention: WatchRetention,
    pub mutation_receipt_retention: MutationReceiptRetention,
    pub single_node_group_commit: SingleNodeGroupCommitConfig,
    /// Blob inactivity grace. The production server requires this to cover
    /// its fixed 24-hour atomic-replay window; short values are only useful to
    /// embedded callers such as focused garbage-collection tests.
    pub awaiting_publish_ttl_seconds: u64,
}

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
            rocksdb_resources: RocksDbResourceBudget::default(),
            root,
            node_id,
            sync_writes: true,
            watch_retention: WatchRetention::default(),
            mutation_receipt_retention: MutationReceiptRetention::default(),
            single_node_group_commit: SingleNodeGroupCommitConfig::default(),
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

    pub fn with_rocksdb_resources(mut self, resources: RocksDbResourceBudget) -> Self {
        self.rocksdb_resources = resources;
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

    pub fn with_single_node_group_commit(mut self, config: SingleNodeGroupCommitConfig) -> Self {
        self.single_node_group_commit = config;
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
