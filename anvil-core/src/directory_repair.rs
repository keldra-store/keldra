use crate::{
    metadata_journal::{self, DirectoryIndexSnapshot, SealedObjectMetadataSegments},
    partition_fence::PartitionWritePermit,
    persistence::Bucket,
    repair_finding::{
        RepairActionKind, RepairFinding, RepairFindingSeverity, RepairFindingStatus,
        RepairFindingWrite, RepairSubjectRef, write_repair_finding,
    },
    storage::Storage,
};
use anyhow::{Result, anyhow};
use serde_json::json;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirectoryIndexRepairReason {
    InvalidDirectoryIndex(String),
    EntryCountMismatch { expected: usize, actual: usize },
    SnapshotHashMismatch { expected: String, actual: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirectoryIndexRepairStatus {
    EmptySource,
    UpToDate,
    NeedsRepair(DirectoryIndexRepairReason),
    Rebuilt(DirectoryIndexRepairReason),
}

#[derive(Debug, Clone)]
pub struct DirectoryIndexRepairReport {
    pub status: DirectoryIndexRepairStatus,
    pub bucket_name: String,
    pub source_cursor: u128,
    pub expected: DirectoryIndexSnapshot,
    pub actual: Option<DirectoryIndexSnapshot>,
    pub finding: Option<RepairFinding>,
    pub rebuilt: Option<SealedObjectMetadataSegments>,
}

pub async fn repair_directory_index(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    bucket: &Bucket,
    rebuild: bool,
    permit: &PartitionWritePermit,
    signing_key: &[u8],
) -> Result<DirectoryIndexRepairReport> {
    let stats =
        metadata_journal::active_object_journal_stats(storage, mvcc, bucket, signing_key).await?;
    let source_cursor = u128::from(stats.last_sequence.max(stats.compacted_through_sequence));
    let expected = metadata_journal::expected_directory_index_snapshot_from_metadata(
        storage,
        mvcc,
        bucket,
        signing_key,
    )
    .await?;
    let actual = match metadata_journal::current_directory_index_snapshot_from_index(
        storage,
        mvcc,
        bucket,
        signing_key,
    )
    .await
    {
        Ok(actual) => Some(actual),
        Err(error) => {
            let reason = DirectoryIndexRepairReason::InvalidDirectoryIndex(error.to_string());
            return finish_repair(
                storage,
                mvcc,
                bucket,
                rebuild,
                permit,
                signing_key,
                source_cursor,
                expected,
                None,
                reason,
            )
            .await;
        }
    };

    let actual_ref = actual.as_ref().expect("actual set above");
    if expected.entry_count == 0 && actual_ref.entry_count == 0 && source_cursor == 0 {
        return Ok(DirectoryIndexRepairReport {
            status: DirectoryIndexRepairStatus::EmptySource,
            bucket_name: bucket.name.clone(),
            source_cursor,
            expected,
            actual,
            finding: None,
            rebuilt: None,
        });
    }
    if expected.entry_count != actual_ref.entry_count {
        let reason = DirectoryIndexRepairReason::EntryCountMismatch {
            expected: expected.entry_count,
            actual: actual_ref.entry_count,
        };
        return finish_repair(
            storage,
            mvcc,
            bucket,
            rebuild,
            permit,
            signing_key,
            source_cursor,
            expected,
            actual,
            reason,
        )
        .await;
    }
    if expected.snapshot_hash != actual_ref.snapshot_hash {
        let reason = DirectoryIndexRepairReason::SnapshotHashMismatch {
            expected: expected.snapshot_hash.clone(),
            actual: actual_ref.snapshot_hash.clone(),
        };
        return finish_repair(
            storage,
            mvcc,
            bucket,
            rebuild,
            permit,
            signing_key,
            source_cursor,
            expected,
            actual,
            reason,
        )
        .await;
    }

    Ok(DirectoryIndexRepairReport {
        status: DirectoryIndexRepairStatus::UpToDate,
        bucket_name: bucket.name.clone(),
        source_cursor,
        expected,
        actual,
        finding: None,
        rebuilt: None,
    })
}

#[allow(clippy::too_many_arguments)]
async fn finish_repair(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    bucket: &Bucket,
    rebuild: bool,
    permit: &PartitionWritePermit,
    signing_key: &[u8],
    source_cursor: u128,
    expected: DirectoryIndexSnapshot,
    actual: Option<DirectoryIndexSnapshot>,
    reason: DirectoryIndexRepairReason,
) -> Result<DirectoryIndexRepairReport> {
    let mut rebuilt = None;
    let status = if rebuild {
        rebuilt = Some(
            metadata_journal::rebuild_directory_index_from_metadata_with_permit(
                storage,
                mvcc,
                bucket,
                signing_key,
                permit,
                signing_key,
            )
            .await?,
        );
        DirectoryIndexRepairStatus::Rebuilt(reason.clone())
    } else {
        DirectoryIndexRepairStatus::NeedsRepair(reason.clone())
    };
    let finding_status = if rebuild {
        RepairFindingStatus::RebuiltDerivedIndex
    } else {
        RepairFindingStatus::Open
    };
    let finding = write_repair_finding(
        mvcc,
        repair_finding_write(
            bucket,
            source_cursor,
            &expected,
            actual.as_ref(),
            &reason,
            finding_status,
            permit.fence_token,
        )?,
        signing_key,
    )
    .await?;
    Ok(DirectoryIndexRepairReport {
        status,
        bucket_name: bucket.name.clone(),
        source_cursor,
        expected,
        actual,
        finding: Some(finding),
        rebuilt,
    })
}

fn repair_finding_write(
    bucket: &Bucket,
    source_cursor: u128,
    expected: &DirectoryIndexSnapshot,
    actual: Option<&DirectoryIndexSnapshot>,
    reason: &DirectoryIndexRepairReason,
    status: RepairFindingStatus,
    lease_fence_token: u64,
) -> Result<RepairFindingWrite> {
    if lease_fence_token == 0 {
        return Err(anyhow!(
            "directory repair lease fence token must be nonzero"
        ));
    }
    let now_nanos = chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("current timestamp is out of range"))?;
    let code = reason_code(reason);
    let finding_seed = format!(
        "{}:{}:{}:{}:{}",
        bucket.tenant_id, bucket.id, source_cursor, expected.snapshot_hash, code
    );
    let finding_hash = crate::formats::hash32(finding_seed.as_bytes());
    Ok(RepairFindingWrite {
        finding_id: format!("directory-{}", hex::encode(&finding_hash[..8])),
        scope_kind: "bucket".to_string(),
        scope_id: format!("tenant-{}-bucket-{}", bucket.tenant_id, bucket.id),
        repair_task_id: format!("directory-repair-{}", bucket.id),
        lease_fence_token,
        severity: RepairFindingSeverity::Error,
        status,
        code: code.to_string(),
        message: reason_message(reason).to_string(),
        subjects: vec![RepairSubjectRef {
            subject_kind: "directory_index".to_string(),
            subject_id: bucket.name.clone(),
            generation: None,
            cursor: Some(source_cursor),
            expected_hash: Some(expected.snapshot_hash.clone()),
            actual_hash: actual.map(|snapshot| snapshot.snapshot_hash.clone()),
        }],
        proposed_action: RepairActionKind::RebuildDirectoryIndex,
        evidence: json!({
            "tenant_id": bucket.tenant_id,
            "bucket_id": bucket.id,
            "bucket_name": bucket.name,
            "source_cursor": source_cursor.to_string(),
            "expected_entry_count": expected.entry_count,
            "expected_snapshot_hash": expected.snapshot_hash,
            "actual_entry_count": actual.map(|snapshot| snapshot.entry_count),
            "actual_snapshot_hash": actual.map(|snapshot| snapshot.snapshot_hash.clone()),
            "repair_reason": code,
            "reason_detail": reason_detail(reason),
        }),
        created_at_nanos: now_nanos,
    })
}

pub fn status_name(status: &DirectoryIndexRepairStatus) -> &'static str {
    match status {
        DirectoryIndexRepairStatus::EmptySource => "empty_source",
        DirectoryIndexRepairStatus::UpToDate => "up_to_date",
        DirectoryIndexRepairStatus::NeedsRepair(_) => "needs_repair",
        DirectoryIndexRepairStatus::Rebuilt(_) => "rebuilt_directory_index",
    }
}

pub fn status_reason(status: &DirectoryIndexRepairStatus) -> String {
    match status {
        DirectoryIndexRepairStatus::EmptySource | DirectoryIndexRepairStatus::UpToDate => {
            String::new()
        }
        DirectoryIndexRepairStatus::NeedsRepair(reason)
        | DirectoryIndexRepairStatus::Rebuilt(reason) => reason_code(reason).to_string(),
    }
}

fn reason_code(reason: &DirectoryIndexRepairReason) -> &'static str {
    match reason {
        DirectoryIndexRepairReason::InvalidDirectoryIndex(_) => "DirectoryIndexInvalid",
        DirectoryIndexRepairReason::EntryCountMismatch { .. } => "DirectoryIndexEntryCountMismatch",
        DirectoryIndexRepairReason::SnapshotHashMismatch { .. } => {
            "DirectoryIndexSnapshotHashMismatch"
        }
    }
}

fn reason_message(reason: &DirectoryIndexRepairReason) -> &'static str {
    match reason {
        DirectoryIndexRepairReason::InvalidDirectoryIndex(_) => {
            "directory index cannot be decoded or verified"
        }
        DirectoryIndexRepairReason::EntryCountMismatch { .. } => {
            "directory index entry count does not match object metadata"
        }
        DirectoryIndexRepairReason::SnapshotHashMismatch { .. } => {
            "directory index content does not match object metadata"
        }
    }
}

fn reason_detail(reason: &DirectoryIndexRepairReason) -> serde_json::Value {
    match reason {
        DirectoryIndexRepairReason::InvalidDirectoryIndex(message) => json!({"message": message}),
        DirectoryIndexRepairReason::EntryCountMismatch { expected, actual } => {
            json!({"expected": expected, "actual": actual})
        }
        DirectoryIndexRepairReason::SnapshotHashMismatch { expected, actual } => {
            json!({"expected": expected, "actual": actual})
        }
    }
}
