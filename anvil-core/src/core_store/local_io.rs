use super::*;

pub(super) async fn read_file(
    path: &PathBuf,
    component: &'static str,
    operation: &'static str,
) -> std::io::Result<Vec<u8>> {
    let started_at = Instant::now();
    let result = fs::read(path).await;
    let bytes = result.as_ref().map(|bytes| bytes.len() as u64).unwrap_or(0);
    crate::perf::record_io_duration(component, operation, path, bytes, started_at.elapsed());
    result
}

pub(crate) fn record_corestore_trace_event(operation: &'static str, status: &'static str) {
    let trace_id = format!("corestore-{}", sha256_hex(operation.as_bytes()));
    let span_id = format!("span-{}", sha256_hex(status.as_bytes()));
    crate::perf::record_trace_event(crate::perf::TraceEvent {
        trace_id: &trace_id,
        span_id: &span_id,
        parent_span_id: None,
        request_id: None,
        component: "corestore",
        operation,
        writer_family: None,
        bucket_hash: None,
        boundary_schema_generation: None,
        duration: Duration::ZERO,
        bytes_in: 0,
        bytes_out: 0,
        fsync_count: u64::from(operation.ends_with("fsync")),
        status,
    });
}

pub(super) fn record_admission_duration(
    operation_family: &str,
    writer_family: &str,
    boundary_schema_generation: &str,
    status: &'static str,
    duration: Duration,
) {
    crate::perf::record_duration(
        "anvil_admission_duration_ms",
        &[
            ("operation_family", operation_family),
            ("writer_family", writer_family),
            ("bucket", "local"),
            ("boundary_schema_generation", boundary_schema_generation),
            ("status", status),
        ],
        duration,
    );
}

pub(super) fn record_landed_bytes_duration(
    operation: &'static str,
    status: &'static str,
    bytes: u64,
    duration: Duration,
) {
    crate::perf::record_duration(
        "anvil_landed_bytes_duration_ms",
        &[
            ("operation", operation),
            ("content_class", "large_payload"),
            ("status", status),
        ],
        duration,
    );
    crate::perf::record_counter(
        "anvil_landed_bytes_total",
        &[
            ("operation", operation),
            ("content_class", "large_payload"),
            ("status", status),
        ],
        bytes,
    );
}

pub(super) fn record_byte_pipeline_stage_duration(
    stage: &'static str,
    writer_family: &str,
    compression: &str,
    encryption: &str,
    erasure_profile: &str,
    duration: Duration,
) {
    crate::perf::record_duration(
        "anvil_byte_pipeline_stage_duration_ms",
        &[
            ("stage", stage),
            ("writer_family", writer_family),
            ("compression", compression),
            ("encryption", encryption),
            ("erasure_profile", erasure_profile),
        ],
        duration,
    );
}

pub(super) fn record_block_write_duration(
    node_id: &str,
    region_id: &str,
    cell_id: &str,
    operation: &'static str,
    status: &'static str,
    duration: Duration,
) {
    crate::perf::record_duration(
        "anvil_block_write_duration_ms",
        &[
            ("node_id", node_id),
            ("region_id", region_id),
            ("cell_id", cell_id),
            ("operation", operation),
            ("fsync_mode", "sync_all"),
            ("status", status),
        ],
        duration,
    );
}

pub(super) fn record_block_read_duration(
    node_id: &str,
    region_id: &str,
    cell_id: &str,
    operation: &'static str,
    cache_status: &'static str,
    status: &'static str,
    duration: Duration,
) {
    crate::perf::record_duration(
        "anvil_block_read_duration_ms",
        &[
            ("node_id", node_id),
            ("region_id", region_id),
            ("cell_id", cell_id),
            ("operation", operation),
            ("cache_status", cache_status),
            ("status", status),
        ],
        duration,
    );
}

pub(crate) async fn write_file_atomic(path: &PathBuf, bytes: &[u8]) -> Result<()> {
    write_file_atomic_with_shard_boundary(path, bytes, false).await
}

pub(crate) async fn write_shard_file_atomic(path: &PathBuf, bytes: &[u8]) -> Result<()> {
    write_file_atomic_with_shard_boundary(path, bytes, true).await
}

async fn write_file_atomic_with_shard_boundary(
    path: &PathBuf,
    bytes: &[u8],
    shard_boundary: bool,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        let started_at = Instant::now();
        fs::create_dir_all(parent).await?;
        crate::perf::record_io_duration(
            "core_store",
            "create_dir_all",
            parent,
            0,
            started_at.elapsed(),
        );
    }
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("CoreStore atomic write path has no file name"))?;
    let tmp_path = path.with_file_name(format!(".{file_name}.{}.tmp", uuid::Uuid::new_v4()));
    let started_at = Instant::now();
    let mut file = fs::File::create(&tmp_path).await?;
    crate::perf::record_io_duration(
        "core_store",
        "file_create",
        &tmp_path,
        0,
        started_at.elapsed(),
    );
    let started_at = Instant::now();
    if let Err(error) = file.write_all(bytes).await {
        drop(file);
        let _ = fs::remove_file(&tmp_path).await;
        return Err(error).with_context(|| {
            format!("write CoreStore atomic temporary file {}", tmp_path.display())
        });
    }
    crate::perf::record_io_duration(
        "core_store",
        "write_all",
        &tmp_path,
        bytes.len() as u64,
        started_at.elapsed(),
    );
    #[cfg(any(test, debug_assertions))]
    if shard_boundary {
        if let Err(error) =
            crate::mvcc_fault_injection::hit(crate::mvcc_fault_injection::FaultPoint::ShardWrite)
        {
            drop(file);
            let _ = fs::remove_file(&tmp_path).await;
            return Err(error.into());
        }
    }
    let started_at = Instant::now();
    if let Err(error) = file.sync_all().await {
        drop(file);
        let _ = fs::remove_file(&tmp_path).await;
        return Err(error).with_context(|| {
            format!("sync CoreStore atomic temporary file {}", tmp_path.display())
        });
    }
    let elapsed = started_at.elapsed();
    crate::perf::record_io_duration(
        "core_store",
        "sync_all",
        &tmp_path,
        bytes.len() as u64,
        elapsed,
    );
    crate::perf::record_fsync_duration(
        "core_store",
        "control_file",
        "atomic_write_sync_all",
        elapsed,
    );
    drop(file);
    let started_at = Instant::now();
    let rename_result = fs::rename(&tmp_path, path).await;
    crate::perf::record_io_duration(
        "core_store",
        "rename",
        path,
        bytes.len() as u64,
        started_at.elapsed(),
    );
    if let Err(err) = rename_result {
        let started_at = Instant::now();
        let _ = fs::remove_file(&tmp_path).await;
        crate::perf::record_io_duration(
            "core_store",
            "remove_temp_after_failed_rename",
            &tmp_path,
            bytes.len() as u64,
            started_at.elapsed(),
        );
        return Err(err).with_context(|| {
            format!(
                "commit CoreStore atomic write {} -> {}",
                tmp_path.display(),
                path.display()
            )
        });
    }
    sync_parent_dir(path, "atomic_write_sync_parent_dir").await?;
    Ok(())
}

pub(super) async fn sync_parent_dir(path: &PathBuf, operation: &'static str) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    let parent = parent.to_path_buf();
    let started_at = Instant::now();
    tokio::task::spawn_blocking({
        let parent = parent.clone();
        move || -> std::io::Result<()> {
            let dir = std::fs::File::open(&parent)?;
            dir.sync_all()
        }
    })
    .await
    .map_err(|err| anyhow!("CoreStore directory fsync task failed: {err}"))??;
    let elapsed = started_at.elapsed();
    crate::perf::record_io_duration("core_store", operation, &parent, 0, elapsed);
    crate::perf::record_fsync_duration("core_store", "directory", operation, elapsed);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ClearFaults;

    impl Drop for ClearFaults {
        fn drop(&mut self) {
            crate::mvcc_fault_injection::clear();
        }
    }

    #[tokio::test]
    async fn shard_disk_fault_never_publishes_or_leaves_a_retry_tail() {
        let _clear = ClearFaults;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("final.shard");
        crate::mvcc_fault_injection::install(
            crate::mvcc_fault_injection::DeterministicFaults::default()
                .fail_at(crate::mvcc_fault_injection::FaultPoint::ShardWrite, 1),
        );

        let error = write_shard_file_atomic(&path, b"encoded shard")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("ShardWrite"));
        assert!(!path.exists());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);

        crate::mvcc_fault_injection::clear();
        write_shard_file_atomic(&path, b"encoded shard")
            .await
            .unwrap();
        assert_eq!(std::fs::read(path).unwrap(), b"encoded shard");
    }
}

pub(super) async fn sum_files_with_extension(root: &PathBuf, extensions: &[&str]) -> Result<u64> {
    let mut total = 0_u64;
    let mut pending = vec![root.clone()];

    while let Some(path) = pending.pop() {
        let mut entries = match fs::read_dir(&path).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(err).with_context(|| format!("read directory {}", path.display()));
            }
        };

        while let Some(entry) = entries.next_entry().await? {
            let entry_path = entry.path();
            let metadata = match entry.metadata().await {
                Ok(metadata) => metadata,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("read metadata for {}", entry_path.display()));
                }
            };
            if metadata.is_dir() {
                pending.push(entry_path);
                continue;
            }
            if !metadata.is_file() {
                continue;
            }
            let Some(extension) = entry_path.extension().and_then(|value| value.to_str()) else {
                continue;
            };
            if extensions.contains(&extension) {
                total = total.saturating_add(metadata.len());
            }
        }
    }

    Ok(total)
}
