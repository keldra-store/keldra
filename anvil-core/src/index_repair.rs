use crate::{
    derived_index_proof::{
        self, DerivedIndexProof, DerivedIndexValidity, validate_derived_index_source,
    },
    full_text_segment, index_builder,
    persistence::{Bucket, IndexDefinition},
    repair_finding::{
        RepairActionKind, RepairFinding, RepairFindingSeverity, RepairFindingStatus,
        RepairFindingWrite, RepairSubjectRef,
    },
    storage::Storage,
    vector_segment,
};
use anyhow::{Result, anyhow};
use serde_json::json;

#[derive(Debug, Clone, PartialEq)]
pub enum IndexRepairReason {
    MissingProof,
    InvalidProof(String),
    StaleProof,
    MissingSegment(Vec<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum IndexRepairStatus {
    EmptySource,
    UpToDate,
    NeedsRepair(IndexRepairReason),
    Rebuilt(IndexRepairReason),
}

#[derive(Debug, Clone)]
pub struct IndexRepairReport {
    pub status: IndexRepairStatus,
    pub bucket_name: String,
    pub index_name: String,
    pub index_storage_id: String,
    pub source_cursor: u128,
    pub finding: Option<RepairFinding>,
    pub build: Option<index_builder::IndexBuildOutcome>,
}

pub async fn assess_derived_index(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    index: &IndexDefinition,
    index_storage_id: &str,
    source_cursor: u128,
    source_manifest_hash: &str,
    signing_key: &[u8],
) -> Result<IndexRepairStatus> {
    if source_cursor == 0 {
        return Ok(IndexRepairStatus::EmptySource);
    }

    let proof = match derived_index_proof::read_latest_derived_index_proof_mvcc(
        mvcc,
        index_storage_id,
        signing_key,
    ) {
        Ok(Some(proof)) => proof,
        Ok(None) => {
            return Ok(IndexRepairStatus::NeedsRepair(
                IndexRepairReason::MissingProof,
            ));
        }
        Err(err) => {
            return Ok(IndexRepairStatus::NeedsRepair(
                IndexRepairReason::InvalidProof(err.to_string()),
            ));
        }
    };

    match validate_derived_index_source(
        &proof,
        source_cursor,
        source_manifest_hash,
        index.version as u64,
        signing_key,
    )? {
        DerivedIndexValidity::Valid => {}
        DerivedIndexValidity::RebuildRequired => {
            return Ok(IndexRepairStatus::NeedsRepair(
                IndexRepairReason::StaleProof,
            ));
        }
    }

    let missing = missing_proof_segments(storage, &index.kind, index_storage_id, &proof).await?;
    if missing.is_empty() {
        Ok(IndexRepairStatus::UpToDate)
    } else {
        Ok(IndexRepairStatus::NeedsRepair(
            IndexRepairReason::MissingSegment(missing),
        ))
    }
}

pub fn source_cursor_from_stats(stats: crate::metadata_journal::ActiveObjectJournalStats) -> u128 {
    u128::from(stats.last_sequence.max(stats.compacted_through_sequence))
}

pub fn repair_finding_write(
    bucket: &Bucket,
    index: &IndexDefinition,
    index_storage_id: &str,
    source_cursor: u128,
    source_manifest_hash: &str,
    reason: &IndexRepairReason,
    status: RepairFindingStatus,
    lease_fence_token: u64,
) -> Result<RepairFindingWrite> {
    let now_nanos = chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("current timestamp is out of range"))?;
    let code = reason_code(reason);
    let finding_seed = format!(
        "{}:{}:{}:{}:{}:{}",
        bucket.tenant_id, bucket.id, index.id, index.version, source_cursor, code
    );
    let finding_hash = crate::formats::hash32(finding_seed.as_bytes());

    Ok(RepairFindingWrite {
        finding_id: format!("index-{}", hex::encode(&finding_hash[..8])),
        scope_kind: "bucket".to_string(),
        scope_id: format!("tenant-{}-bucket-{}", bucket.tenant_id, bucket.id),
        repair_task_id: format!("index-repair-{index_storage_id}"),
        lease_fence_token,
        severity: RepairFindingSeverity::Error,
        status,
        code: code.to_string(),
        message: reason_message(reason).to_string(),
        subjects: vec![RepairSubjectRef {
            subject_kind: "derived_index".to_string(),
            subject_id: index_storage_id.to_string(),
            generation: Some(index.version as u64),
            cursor: Some(source_cursor),
            expected_hash: Some(source_manifest_hash.to_string()),
            actual_hash: None,
        }],
        proposed_action: RepairActionKind::RebuildDerivedIndex,
        evidence: json!({
            "tenant_id": bucket.tenant_id,
            "bucket_id": bucket.id,
            "bucket_name": bucket.name,
            "index_id": index.id,
            "index_name": index.name,
            "index_kind": index.kind,
            "index_version": index.version,
            "index_storage_id": index_storage_id,
            "source_cursor": source_cursor.to_string(),
            "source_manifest_hash": source_manifest_hash,
            "repair_reason": reason_code(reason),
            "missing_segments": missing_segments(reason),
        }),
        created_at_nanos: now_nanos,
    })
}

pub fn reason_code(reason: &IndexRepairReason) -> &'static str {
    match reason {
        IndexRepairReason::MissingProof => "DerivedIndexProofMissing",
        IndexRepairReason::InvalidProof(_) => "DerivedIndexProofInvalid",
        IndexRepairReason::StaleProof => "DerivedIndexProofStale",
        IndexRepairReason::MissingSegment(_) => "DerivedIndexSegmentMissing",
    }
}

pub fn reason_message(reason: &IndexRepairReason) -> &'static str {
    match reason {
        IndexRepairReason::MissingProof => "derived index proof is missing",
        IndexRepairReason::InvalidProof(_) => "derived index proof is invalid",
        IndexRepairReason::StaleProof => "derived index proof no longer matches the source cursor",
        IndexRepairReason::MissingSegment(_) => {
            "derived index proof references segment files that are absent"
        }
    }
}

pub fn status_name(status: &IndexRepairStatus) -> &'static str {
    match status {
        IndexRepairStatus::EmptySource => "empty_source",
        IndexRepairStatus::UpToDate => "up_to_date",
        IndexRepairStatus::NeedsRepair(_) => "needs_repair",
        IndexRepairStatus::Rebuilt(_) => "rebuilt_derived_index",
    }
}

pub fn status_reason(status: &IndexRepairStatus) -> String {
    match status {
        IndexRepairStatus::EmptySource | IndexRepairStatus::UpToDate => String::new(),
        IndexRepairStatus::NeedsRepair(reason) | IndexRepairStatus::Rebuilt(reason) => {
            reason_code(reason).to_string()
        }
    }
}

fn missing_segments(reason: &IndexRepairReason) -> Vec<String> {
    match reason {
        IndexRepairReason::MissingSegment(segments) => segments.clone(),
        _ => Vec::new(),
    }
}

async fn missing_proof_segments(
    storage: &Storage,
    index_kind: &str,
    index_storage_id: &str,
    proof: &DerivedIndexProof,
) -> Result<Vec<String>> {
    let mut missing = Vec::new();
    for segment_hash in &proof.segment_hashes {
        let exists = match index_kind {
            "full_text" => {
                full_text_segment::full_text_segment_hash_exists(
                    storage,
                    index_storage_id,
                    proof.generation,
                    segment_hash,
                )
                .await?
            }
            "vector" => {
                vector_segment::vector_segment_hash_exists(
                    storage,
                    index_storage_id,
                    proof.generation,
                    segment_hash,
                )
                .await?
            }
            "hybrid" => {
                let full_text_exists = full_text_segment::full_text_segment_hash_exists(
                    storage,
                    index_storage_id,
                    proof.generation,
                    segment_hash,
                )
                .await?;
                let vector_exists = vector_segment::vector_segment_hash_exists(
                    storage,
                    index_storage_id,
                    proof.generation,
                    segment_hash,
                )
                .await?;
                full_text_exists || vector_exists
            }
            _ => false,
        };
        if !exists {
            missing.push(segment_hash.clone());
        }
    }

    if missing.is_empty()
        && let Err(error) =
            validate_latest_segment_readable(storage, index_kind, index_storage_id).await
    {
        let error_hash = crate::formats::hash32(error.to_string().as_bytes());
        missing.push(format!("unreadable-{}", hex::encode(&error_hash[..8])));
    }
    Ok(missing)
}

async fn validate_latest_segment_readable(
    storage: &Storage,
    index_kind: &str,
    index_storage_id: &str,
) -> Result<()> {
    match index_kind {
        "full_text" => {
            full_text_segment::read_latest_full_text_segment(storage, index_storage_id)
                .await?
                .ok_or_else(|| anyhow!("full text derived index segment is absent"))?;
        }
        "vector" => {
            vector_segment::read_latest_vector_segment(storage, index_storage_id)
                .await?
                .ok_or_else(|| anyhow!("vector derived index segment is absent"))?;
        }
        "hybrid" => {
            full_text_segment::read_latest_full_text_segment(storage, index_storage_id)
                .await?
                .ok_or_else(|| anyhow!("hybrid full text derived index segment is absent"))?;
            vector_segment::read_latest_vector_segment(storage, index_storage_id)
                .await?
                .ok_or_else(|| anyhow!("hybrid vector derived index segment is absent"))?;
        }
        _ => {}
    }
    Ok(())
}
