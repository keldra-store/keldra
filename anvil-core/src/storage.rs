use anyhow::{Context, Result};
use futures_util::StreamExt;
use sha2::Digest;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::Instant;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast;
use tracing::info;

const STORAGE_DIR: &str = "anvil-data";
const CORESTORE_DIR: &str = "corestore";
const CORESTORE_STAGING_DIR: &str = "staging";
const CORESTORE_TMP_DIR: &str = "tmp";

type StreamNotifierMap = Mutex<std::collections::BTreeMap<String, broadcast::Sender<()>>>;

static STREAM_NOTIFIER_REGISTRIES: LazyLock<
    Mutex<std::collections::BTreeMap<PathBuf, Weak<StreamNotifierMap>>>,
> = LazyLock::new(|| Mutex::new(std::collections::BTreeMap::new()));

#[derive(Debug, Clone)]
pub struct Storage {
    storage_path: PathBuf,
    temp_path: PathBuf,
    stream_notifiers: Arc<StreamNotifierMap>,
}

impl Storage {
    pub async fn new() -> Result<Self> {
        Self::new_at(Path::new(STORAGE_DIR)).await
    }

    pub async fn new_at(storage_path: impl AsRef<Path>) -> Result<Self> {
        let storage_path = storage_path.as_ref().to_path_buf();
        let temp_path = core_store_staging_tmp_path(&storage_path);
        fs::create_dir_all(&storage_path).await?;
        fs::create_dir_all(&temp_path).await?;
        let stream_notifiers = stream_notifiers_for_path(&storage_path)?;
        Ok(Self {
            storage_path,
            temp_path,
            stream_notifiers,
        })
    }

    pub fn new_at_sync(storage_path: impl AsRef<Path>) -> Result<Self> {
        let storage_path = storage_path.as_ref().to_path_buf();
        let temp_path = core_store_staging_tmp_path(&storage_path);
        std::fs::create_dir_all(&storage_path)?;
        std::fs::create_dir_all(&temp_path)?;
        let stream_notifiers = stream_notifiers_for_path(&storage_path)?;
        Ok(Self {
            storage_path,
            temp_path,
            stream_notifiers,
        })
    }

    pub fn subscribe_stream(&self, stream_id: &str) -> broadcast::Receiver<()> {
        let mut notifiers = self
            .stream_notifiers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        notifiers
            .entry(stream_id.to_string())
            .or_insert_with(|| broadcast::channel(1).0)
            .subscribe()
    }

    pub fn notify_stream(&self, stream_id: &str) {
        let mut notifiers = self
            .stream_notifiers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let remove = notifiers.get(stream_id).is_some_and(|notifier| {
            if notifier.receiver_count() == 0 {
                true
            } else {
                let _ = notifier.send(());
                false
            }
        });
        if remove {
            notifiers.remove(stream_id);
        }
    }

    pub fn temp_dir_path(&self) -> &Path {
        &self.temp_path
    }

    pub fn core_store_root_path(&self) -> PathBuf {
        self.storage_path.join(CORESTORE_DIR)
    }

    pub fn core_store_meta_path(&self) -> PathBuf {
        self.core_store_root_path().join("meta").join("rocksdb")
    }

    pub fn core_store_staging_path(&self) -> PathBuf {
        self.core_store_root_path().join(CORESTORE_STAGING_DIR)
    }

    pub fn core_store_blocks_path(&self) -> PathBuf {
        self.core_store_root_path().join("blocks")
    }

    pub fn core_store_local_block_cache_path(&self) -> PathBuf {
        self.core_store_blocks_path().join("local-cache")
    }

    pub fn core_store_admission_path(&self) -> PathBuf {
        self.storage_path.join("admission")
    }

    pub fn core_store_landed_bytes_path(&self) -> PathBuf {
        self.core_store_admission_path().join("landed-bytes")
    }

    pub fn relative_storage_path(&self, path: &Path) -> Result<String> {
        let relative = path.strip_prefix(&self.storage_path)?;
        Ok(relative.to_string_lossy().replace('\\', "/"))
    }

    pub fn resolve_relative_storage_path(&self, relative: &str) -> Result<PathBuf> {
        let path = Path::new(relative);
        if path.is_absolute() {
            anyhow::bail!("storage-relative path must not be absolute");
        }

        let mut clean = PathBuf::new();
        for component in path.components() {
            match component {
                std::path::Component::Normal(part) => clean.push(part),
                std::path::Component::CurDir => {}
                _ => anyhow::bail!("storage-relative path must not escape storage root"),
            }
        }
        Ok(self.storage_path.join(clean))
    }

    fn staged_upload_scratch_path(&self, upload_id: &str) -> PathBuf {
        self.temp_path.join(upload_id)
    }

    pub async fn stream_to_temp_file(
        &self,
        mut data_stream: impl futures_util::Stream<Item = Result<Vec<u8>, tonic::Status>> + Unpin,
    ) -> Result<(PathBuf, i64, String)> {
        info!("stream_to_temp_file called");
        let upload_id = uuid::Uuid::new_v4().to_string();
        // Class C scratch: callers must route durable bytes into CoreStore before publishing refs.
        let temp_path = self.staged_upload_scratch_path(&upload_id);
        let started_at = Instant::now();
        let mut file = fs::File::create(&temp_path).await?;
        crate::perf::record_io_duration(
            "storage",
            "temp_file_create",
            &temp_path,
            0,
            started_at.elapsed(),
        );

        let mut overall_hasher = sha2::Sha256::new();
        let mut total_bytes = 0;
        let mut chunk_count = 0u64;
        let mut write_duration = std::time::Duration::ZERO;

        while let Some(chunk_result) = data_stream.next().await {
            let chunk = chunk_result.map_err(|e| anyhow::anyhow!(e.to_string()))?;
            let started_at = Instant::now();
            file.write_all(&chunk).await?;
            write_duration += started_at.elapsed();
            overall_hasher.update(&chunk);
            total_bytes += chunk.len() as i64;
            chunk_count = chunk_count.saturating_add(1);
        }
        crate::perf::record_io_duration(
            "storage",
            "temp_file_write_all_chunks",
            &temp_path,
            total_bytes as u64,
            write_duration,
        );
        crate::perf::record_counter(
            "anvil_file_io_chunks",
            &[
                ("component", "storage"),
                ("operation", "temp_file_write_all_chunks"),
                ("file_path", temp_path.to_string_lossy().as_ref()),
            ],
            chunk_count,
        );
        // This file is non-authoritative scratch. CoreStore is responsible for
        // durable publication after it ingests these bytes, so forcing an fsync
        // here only adds latency and can block callers on container filesystems.
        let started_at = Instant::now();
        file.flush().await?;
        crate::perf::record_io_duration(
            "storage",
            "temp_file_flush",
            &temp_path,
            total_bytes as u64,
            started_at.elapsed(),
        );

        let content_hash = hex::encode(overall_hasher.finalize());
        info!(
            ?temp_path,
            total_bytes,
            %content_hash,
            "stream_to_temp_file finished"
        );
        Ok((temp_path, total_bytes, content_hash))
    }
}

fn stream_notifiers_for_path(storage_path: &Path) -> Result<Arc<StreamNotifierMap>> {
    let registry_key = std::fs::canonicalize(storage_path)
        .with_context(|| format!("canonicalize storage path {}", storage_path.display()))?;
    let mut registries = STREAM_NOTIFIER_REGISTRIES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    registries.retain(|_, registry| registry.strong_count() > 0);
    if let Some(registry) = registries.get(&registry_key).and_then(Weak::upgrade) {
        return Ok(registry);
    }
    let registry = Arc::new(Mutex::new(std::collections::BTreeMap::new()));
    registries.insert(registry_key, Arc::downgrade(&registry));
    Ok(registry)
}

fn core_store_staging_tmp_path(storage_path: &Path) -> PathBuf {
    storage_path
        .join(CORESTORE_DIR)
        .join(CORESTORE_STAGING_DIR)
        .join(CORESTORE_TMP_DIR)
}

pub fn ensure_operator_path_outside_storage(
    storage_path: impl AsRef<Path>,
    candidate_path: impl AsRef<Path>,
    field_name: &str,
    path_kind: &str,
) -> Result<()> {
    let storage_path = std::path::absolute(storage_path.as_ref())
        .with_context(|| format!("resolve {field_name} storage path"))?;
    let candidate_path = std::path::absolute(candidate_path.as_ref())
        .with_context(|| format!("resolve {field_name} path"))?;
    if candidate_path == storage_path || candidate_path.starts_with(&storage_path) {
        anyhow::bail!(
            "{field_name} must be outside storage_path; {path_kind} is operator-controlled state, not Anvil CoreStore data"
        );
    }
    Ok(())
}
