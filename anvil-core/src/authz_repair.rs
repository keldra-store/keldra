use crate::{
    authz_userset_index::{
        AuthzDerivedUsersetIndex, build_expected_derived_userset_index, read_derived_userset_index,
        rebuild_derived_userset_index,
    },
    repair_finding::{
        RepairActionKind, RepairFinding, RepairFindingSeverity, RepairFindingStatus,
        RepairFindingWrite, RepairSubjectRef, write_repair_finding,
    },
    storage::Storage,
};
use anyhow::{Result, anyhow};
use serde_json::json;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthzDerivedIndexRepairReason {
    MissingIndex,
    InvalidIndex(String),
    StaleRevision {
        processed_revision: u64,
        latest_revision: u64,
    },
    SourceHashMismatch {
        expected_hash: String,
        actual_hash: String,
    },
    EntryMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthzDerivedIndexRepairStatus {
    EmptySource,
    UpToDate,
    NeedsRepair(AuthzDerivedIndexRepairReason),
    Rebuilt(AuthzDerivedIndexRepairReason),
}

#[derive(Debug, Clone)]
pub struct AuthzDerivedIndexRepairReport {
    pub status: AuthzDerivedIndexRepairStatus,
    pub tenant_id: i64,
    pub derived_index_id: String,
    pub processed_revision: u64,
    pub latest_revision: u64,
    pub source_records_hash: String,
    pub finding: Option<RepairFinding>,
    pub rebuilt_index: Option<AuthzDerivedUsersetIndex>,
}

pub async fn repair_authz_derived_userset_index(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    derived_index_id: &str,
    rebuild: bool,
    lease_fence_token: u64,
    signing_key: &[u8],
) -> Result<AuthzDerivedIndexRepairReport> {
    let expected =
        build_expected_derived_userset_index(storage, mvcc, tenant_id, derived_index_id).await?;
    let latest_revision = expected.processed_revision;
    let mut status =
        assess_current_index(storage, mvcc, tenant_id, derived_index_id, &expected).await?;
    let mut rebuilt_index = None;
    let mut finding = None;

    if let AuthzDerivedIndexRepairStatus::NeedsRepair(reason) = status.clone() {
        if rebuild {
            let rebuilt =
                rebuild_derived_userset_index(storage, mvcc, tenant_id, derived_index_id).await?;
            status = AuthzDerivedIndexRepairStatus::Rebuilt(reason.clone());
            rebuilt_index = Some(rebuilt);
        }
        let finding_status = if rebuild {
            RepairFindingStatus::RebuiltDerivedIndex
        } else {
            RepairFindingStatus::Open
        };
        let write = repair_finding_write(
            tenant_id,
            derived_index_id,
            &expected,
            &reason,
            finding_status,
            lease_fence_token,
        )?;
        finding = Some(write_repair_finding(mvcc, write, signing_key).await?);
    }

    let processed_revision = rebuilt_index
        .as_ref()
        .map(|index| index.processed_revision)
        .unwrap_or_else(|| reported_processed_revision(&status, latest_revision));
    Ok(AuthzDerivedIndexRepairReport {
        status,
        tenant_id,
        derived_index_id: derived_index_id.to_string(),
        processed_revision,
        latest_revision,
        source_records_hash: expected.source_records_hash,
        finding,
        rebuilt_index,
    })
}

fn reported_processed_revision(
    status: &AuthzDerivedIndexRepairStatus,
    latest_revision: u64,
) -> u64 {
    match status {
        AuthzDerivedIndexRepairStatus::EmptySource | AuthzDerivedIndexRepairStatus::UpToDate => {
            latest_revision
        }
        AuthzDerivedIndexRepairStatus::NeedsRepair(
            AuthzDerivedIndexRepairReason::StaleRevision {
                processed_revision, ..
            },
        ) => *processed_revision,
        AuthzDerivedIndexRepairStatus::NeedsRepair(
            AuthzDerivedIndexRepairReason::MissingIndex
            | AuthzDerivedIndexRepairReason::InvalidIndex(_),
        ) => 0,
        AuthzDerivedIndexRepairStatus::NeedsRepair(
            AuthzDerivedIndexRepairReason::SourceHashMismatch { .. }
            | AuthzDerivedIndexRepairReason::EntryMismatch,
        ) => latest_revision,
        AuthzDerivedIndexRepairStatus::Rebuilt(_) => latest_revision,
    }
}

async fn assess_current_index(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    derived_index_id: &str,
    expected: &AuthzDerivedUsersetIndex,
) -> Result<AuthzDerivedIndexRepairStatus> {
    let current = match read_derived_userset_index(storage, mvcc, tenant_id, derived_index_id).await
    {
        Ok(Some(current)) => current,
        Ok(None) if expected.processed_revision == 0 => {
            return Ok(AuthzDerivedIndexRepairStatus::EmptySource);
        }
        Ok(None) => {
            return Ok(AuthzDerivedIndexRepairStatus::NeedsRepair(
                AuthzDerivedIndexRepairReason::MissingIndex,
            ));
        }
        Err(error) => {
            return Ok(AuthzDerivedIndexRepairStatus::NeedsRepair(
                AuthzDerivedIndexRepairReason::InvalidIndex(error.to_string()),
            ));
        }
    };

    if current.processed_revision != expected.processed_revision {
        return Ok(AuthzDerivedIndexRepairStatus::NeedsRepair(
            AuthzDerivedIndexRepairReason::StaleRevision {
                processed_revision: current.processed_revision,
                latest_revision: expected.processed_revision,
            },
        ));
    }
    if current.source_records_hash != expected.source_records_hash {
        return Ok(AuthzDerivedIndexRepairStatus::NeedsRepair(
            AuthzDerivedIndexRepairReason::SourceHashMismatch {
                expected_hash: expected.source_records_hash.clone(),
                actual_hash: current.source_records_hash,
            },
        ));
    }
    if current.entries != expected.entries {
        return Ok(AuthzDerivedIndexRepairStatus::NeedsRepair(
            AuthzDerivedIndexRepairReason::EntryMismatch,
        ));
    }
    Ok(AuthzDerivedIndexRepairStatus::UpToDate)
}

pub fn status_name(status: &AuthzDerivedIndexRepairStatus) -> &'static str {
    match status {
        AuthzDerivedIndexRepairStatus::EmptySource => "empty_source",
        AuthzDerivedIndexRepairStatus::UpToDate => "up_to_date",
        AuthzDerivedIndexRepairStatus::NeedsRepair(_) => "needs_repair",
        AuthzDerivedIndexRepairStatus::Rebuilt(_) => "rebuilt_derived_index",
    }
}

pub fn status_reason(status: &AuthzDerivedIndexRepairStatus) -> String {
    match status {
        AuthzDerivedIndexRepairStatus::EmptySource | AuthzDerivedIndexRepairStatus::UpToDate => {
            String::new()
        }
        AuthzDerivedIndexRepairStatus::NeedsRepair(reason)
        | AuthzDerivedIndexRepairStatus::Rebuilt(reason) => reason_code(reason).to_string(),
    }
}

fn repair_finding_write(
    tenant_id: i64,
    derived_index_id: &str,
    expected: &AuthzDerivedUsersetIndex,
    reason: &AuthzDerivedIndexRepairReason,
    status: RepairFindingStatus,
    lease_fence_token: u64,
) -> Result<RepairFindingWrite> {
    if lease_fence_token == 0 {
        return Err(anyhow!(
            "authorization repair lease fence token must be nonzero"
        ));
    }
    let now_nanos = chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("current timestamp is out of range"))?;
    let finding_seed = format!(
        "{tenant_id}:{derived_index_id}:{}:{}",
        expected.processed_revision,
        reason_code(reason)
    );
    let finding_hash = crate::formats::hash32(finding_seed.as_bytes());

    Ok(RepairFindingWrite {
        finding_id: format!("authz-{}", hex::encode(&finding_hash[..8])),
        scope_kind: "authz".to_string(),
        scope_id: format!("tenant-{tenant_id}"),
        repair_task_id: format!("authz-repair-{derived_index_id}"),
        lease_fence_token,
        severity: RepairFindingSeverity::Error,
        status,
        code: reason_code(reason).to_string(),
        message: reason_message(reason).to_string(),
        subjects: vec![RepairSubjectRef {
            subject_kind: "derived_authz_index".to_string(),
            subject_id: derived_index_id.to_string(),
            generation: Some(expected.generation),
            cursor: Some(u128::from(expected.processed_revision)),
            expected_hash: Some(expected.source_records_hash.clone()),
            actual_hash: actual_hash(reason),
        }],
        proposed_action: RepairActionKind::RebuildDerivedIndex,
        evidence: json!({
            "tenant_id": tenant_id,
            "derived_index_id": derived_index_id,
            "latest_revision": expected.processed_revision,
            "source_record_count": expected.source_record_count,
            "source_records_hash": expected.source_records_hash,
            "repair_reason": reason_code(reason),
        }),
        created_at_nanos: now_nanos,
    })
}

fn reason_code(reason: &AuthzDerivedIndexRepairReason) -> &'static str {
    match reason {
        AuthzDerivedIndexRepairReason::MissingIndex => "AuthzDerivedIndexMissing",
        AuthzDerivedIndexRepairReason::InvalidIndex(_) => "AuthzDerivedIndexInvalid",
        AuthzDerivedIndexRepairReason::StaleRevision { .. } => "AuthzDerivedIndexStaleRevision",
        AuthzDerivedIndexRepairReason::SourceHashMismatch { .. } => {
            "AuthzDerivedIndexSourceHashMismatch"
        }
        AuthzDerivedIndexRepairReason::EntryMismatch => "AuthzDerivedIndexEntryMismatch",
    }
}

fn reason_message(reason: &AuthzDerivedIndexRepairReason) -> &'static str {
    match reason {
        AuthzDerivedIndexRepairReason::MissingIndex => "authorization derived index is missing",
        AuthzDerivedIndexRepairReason::InvalidIndex(_) => {
            "authorization derived index cannot be decoded or verified"
        }
        AuthzDerivedIndexRepairReason::StaleRevision { .. } => {
            "authorization derived index has not processed the latest tuple revision"
        }
        AuthzDerivedIndexRepairReason::SourceHashMismatch { .. } => {
            "authorization derived index source hash does not match tuple journal"
        }
        AuthzDerivedIndexRepairReason::EntryMismatch => {
            "authorization derived index entries do not match tuple journal"
        }
    }
}

fn actual_hash(reason: &AuthzDerivedIndexRepairReason) -> Option<String> {
    match reason {
        AuthzDerivedIndexRepairReason::SourceHashMismatch { actual_hash, .. } => {
            Some(actual_hash.clone())
        }
        _ => None,
    }
}
