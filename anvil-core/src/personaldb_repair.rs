use crate::{
    formats::{Hash32, hash32, personaldb::PersonalDbLogRecord},
    personaldb_commit_store::{
        decode_commit_certificate, read_personaldb_changeset_payload_by_index,
        read_personaldb_changeset_payload_ref, read_personaldb_commit_certificate,
        read_personaldb_commit_certificate_ref,
    },
    personaldb_heads::{
        PersonalDbCommittedHead, read_personaldb_committed_head, read_personaldb_group_manifest,
    },
    personaldb_segment::{list_personaldb_log_segment_refs, read_personaldb_log_segment},
    repair_finding::{
        RepairActionKind, RepairFinding, RepairFindingSeverity, RepairFindingStatus,
        RepairFindingWrite, RepairSubjectRef, write_repair_finding,
    },
    storage::Storage,
};
use anyhow::{Result, anyhow};
use personaldb_protocol::PublicKeyTrustStore;
use serde_json::json;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersonalDbLogChainRepairReason {
    MissingManifest,
    InvalidManifest(String),
    MissingCommittedHead,
    InvalidCommittedHead(String),
    HeadManifestMismatch,
    GenesisHeadMismatch,
    MissingLogSegment { expected_log_index: u64 },
    InvalidLogSegment { segment_id: String, message: String },
    NonContiguousLogChain { log_index: u64 },
    LogPreviousHashMismatch { log_index: u64 },
    CommittedHeadMismatch,
    MissingChangesetPayload { log_index: u64 },
    InvalidChangesetPayload { log_index: u64, message: String },
    MissingCommitCertificate { log_index: u64 },
    InvalidCommitCertificate { log_index: u64, message: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersonalDbLogChainRepairStatus {
    EmptySource,
    UpToDate,
    NeedsReview(PersonalDbLogChainRepairReason),
}

#[derive(Debug, Clone)]
pub struct PersonalDbLogChainRepairReport {
    pub status: PersonalDbLogChainRepairStatus,
    pub tenant_id: i64,
    pub database_id: String,
    pub committed_log_index: u64,
    pub verified_log_index: u64,
    pub committed_log_hash: String,
    pub finding: Option<RepairFinding>,
}

pub async fn repair_personaldb_log_chain(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    database_id: &str,
    lease_fence_token: u64,
    personaldb_trust_store: &PublicKeyTrustStore,
    repair_finding_signing_key: &[u8],
) -> Result<PersonalDbLogChainRepairReport> {
    if lease_fence_token == 0 {
        return Err(anyhow!(
            "PersonalDB repair lease fence token must be nonzero"
        ));
    }

    let mut report =
        assess_log_chain(storage, tenant_id, database_id, personaldb_trust_store).await?;
    if let PersonalDbLogChainRepairStatus::NeedsReview(reason) = report.status.clone() {
        let write = repair_finding_write(
            tenant_id,
            database_id,
            report.committed_log_index,
            &report.committed_log_hash,
            &reason,
            lease_fence_token,
        )?;
        report.finding = Some(write_repair_finding(mvcc, write, repair_finding_signing_key).await?);
    }
    Ok(report)
}

async fn assess_log_chain(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    trust_store: &PublicKeyTrustStore,
) -> Result<PersonalDbLogChainRepairReport> {
    let manifest =
        match read_personaldb_group_manifest(storage, tenant_id, database_id, trust_store).await {
            Ok(Some(manifest)) => manifest,
            Ok(None) => {
                return Ok(report_without_head(
                    tenant_id,
                    database_id,
                    PersonalDbLogChainRepairReason::MissingManifest,
                ));
            }
            Err(err) => {
                return Ok(report_without_head(
                    tenant_id,
                    database_id,
                    PersonalDbLogChainRepairReason::InvalidManifest(err.to_string()),
                ));
            }
        };
    let head =
        match read_personaldb_committed_head(storage, tenant_id, database_id, trust_store).await {
            Ok(Some(head)) => head,
            Ok(None) => {
                return Ok(report_without_head(
                    tenant_id,
                    database_id,
                    PersonalDbLogChainRepairReason::MissingCommittedHead,
                ));
            }
            Err(err) => {
                return Ok(report_without_head(
                    tenant_id,
                    database_id,
                    PersonalDbLogChainRepairReason::InvalidCommittedHead(err.to_string()),
                ));
            }
        };

    if head.schema_hash != manifest.schema_hash || head.database_id != manifest.database_id {
        return Ok(report_with_head(
            tenant_id,
            database_id,
            &head,
            0,
            PersonalDbLogChainRepairReason::HeadManifestMismatch,
        ));
    }

    if head.log_index == 0 {
        let status = if head.log_hash == manifest.genesis_hash {
            PersonalDbLogChainRepairStatus::EmptySource
        } else {
            PersonalDbLogChainRepairStatus::NeedsReview(
                PersonalDbLogChainRepairReason::GenesisHeadMismatch,
            )
        };
        return Ok(PersonalDbLogChainRepairReport {
            status,
            tenant_id,
            database_id: database_id.to_string(),
            committed_log_index: head.log_index,
            verified_log_index: 0,
            committed_log_hash: head.log_hash,
            finding: None,
        });
    }

    let records = match read_records_through_head(storage, tenant_id, database_id, &head).await {
        Ok(records) => records,
        Err(reason) => return Ok(report_with_head(tenant_id, database_id, &head, 0, reason)),
    };

    let genesis_hash = match hex32(&manifest.genesis_hash) {
        Ok(hash) => hash,
        Err(err) => {
            return Ok(report_with_head(
                tenant_id,
                database_id,
                &head,
                0,
                PersonalDbLogChainRepairReason::InvalidManifest(err.to_string()),
            ));
        }
    };
    if let Err(reason) = validate_chain_against_manifest_and_head(&records, genesis_hash, &head) {
        return Ok(report_with_head(
            tenant_id,
            database_id,
            &head,
            verified_index_before_reason(&reason),
            reason,
        ));
    }

    let mut verified_log_index = 0;
    for record in &records {
        if let Err(reason) = verify_record_payload(storage, tenant_id, database_id, record).await {
            return Ok(report_with_head(
                tenant_id,
                database_id,
                &head,
                verified_log_index,
                reason,
            ));
        }
        if let Err(reason) =
            verify_record_certificate(storage, tenant_id, database_id, record, trust_store).await
        {
            return Ok(report_with_head(
                tenant_id,
                database_id,
                &head,
                verified_log_index,
                reason,
            ));
        }
        verified_log_index = record.log_index;
    }

    Ok(PersonalDbLogChainRepairReport {
        status: PersonalDbLogChainRepairStatus::UpToDate,
        tenant_id,
        database_id: database_id.to_string(),
        committed_log_index: head.log_index,
        verified_log_index,
        committed_log_hash: head.log_hash,
        finding: None,
    })
}

async fn read_records_through_head(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    head: &PersonalDbCommittedHead,
) -> std::result::Result<Vec<PersonalDbLogRecord>, PersonalDbLogChainRepairReason> {
    let segment_refs = list_log_segment_refs(storage, tenant_id, database_id)
        .await
        .map_err(|err| PersonalDbLogChainRepairReason::InvalidLogSegment {
            segment_id: "segment-directory".to_string(),
            message: err.to_string(),
        })?;
    if segment_refs.is_empty() {
        return Err(PersonalDbLogChainRepairReason::MissingLogSegment {
            expected_log_index: 1,
        });
    }

    let mut records = Vec::new();
    for segment_ref in segment_refs {
        let segment_id = segment_ref.clone();
        let segment = read_personaldb_log_segment(storage, &segment_ref)
            .await
            .map_err(|err| PersonalDbLogChainRepairReason::InvalidLogSegment {
                segment_id: segment_id.clone(),
                message: err.to_string(),
            })?;
        if segment.header.tenant_id != tenant_id.to_string()
            || segment.header.database_id != database_id
        {
            return Err(PersonalDbLogChainRepairReason::InvalidLogSegment {
                segment_id,
                message: "personaldb log segment scope does not match requested group".to_string(),
            });
        }
        records.extend(
            segment
                .records
                .into_iter()
                .filter(|record| record.log_index <= head.log_index),
        );
    }
    records.sort_by_key(|record| record.log_index);
    Ok(records)
}

async fn list_log_segment_refs(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
) -> Result<Vec<String>> {
    list_personaldb_log_segment_refs(storage, tenant_id, database_id).await
}

fn validate_chain_against_manifest_and_head(
    records: &[PersonalDbLogRecord],
    genesis_hash: Hash32,
    head: &PersonalDbCommittedHead,
) -> std::result::Result<(), PersonalDbLogChainRepairReason> {
    let Some(first) = records.first() else {
        return Err(PersonalDbLogChainRepairReason::MissingLogSegment {
            expected_log_index: 1,
        });
    };
    if first.log_index != 1 {
        return Err(PersonalDbLogChainRepairReason::MissingLogSegment {
            expected_log_index: 1,
        });
    }
    if first.previous_log_hash != genesis_hash {
        return Err(PersonalDbLogChainRepairReason::GenesisHeadMismatch);
    }

    for pair in records.windows(2) {
        let previous = &pair[0];
        let current = &pair[1];
        if current.log_index != previous.log_index.saturating_add(1) {
            return Err(PersonalDbLogChainRepairReason::NonContiguousLogChain {
                log_index: current.log_index,
            });
        }
        if current.previous_log_hash != previous.entry_hash {
            return Err(PersonalDbLogChainRepairReason::LogPreviousHashMismatch {
                log_index: current.log_index,
            });
        }
    }

    let Some(last) = records.last() else {
        return Err(PersonalDbLogChainRepairReason::MissingLogSegment {
            expected_log_index: head.log_index,
        });
    };
    if last.log_index != head.log_index {
        return Err(PersonalDbLogChainRepairReason::MissingLogSegment {
            expected_log_index: last.log_index.saturating_add(1),
        });
    }
    if hex::encode(last.entry_hash) != head.log_hash {
        return Err(PersonalDbLogChainRepairReason::CommittedHeadMismatch);
    }
    Ok(())
}

async fn verify_record_payload(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    record: &PersonalDbLogRecord,
) -> std::result::Result<(), PersonalDbLogChainRepairReason> {
    let bytes = if !record.payload_ref.is_empty() {
        let payload_ref = std::str::from_utf8(&record.payload_ref).map_err(|err| {
            PersonalDbLogChainRepairReason::InvalidChangesetPayload {
                log_index: record.log_index,
                message: err.to_string(),
            }
        })?;
        match read_personaldb_changeset_payload_ref(
            storage,
            payload_ref,
            record.changeset_payload_hash,
        )
        .await
        {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                return Err(PersonalDbLogChainRepairReason::MissingChangesetPayload {
                    log_index: record.log_index,
                });
            }
            Err(err) => {
                return Err(PersonalDbLogChainRepairReason::InvalidChangesetPayload {
                    log_index: record.log_index,
                    message: err.to_string(),
                });
            }
        }
    } else {
        match read_personaldb_changeset_payload_by_index(
            storage,
            tenant_id,
            database_id,
            record.log_index,
            record.changeset_payload_hash,
        )
        .await
        {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                return Err(PersonalDbLogChainRepairReason::MissingChangesetPayload {
                    log_index: record.log_index,
                });
            }
            Err(err) => {
                return Err(PersonalDbLogChainRepairReason::InvalidChangesetPayload {
                    log_index: record.log_index,
                    message: err.to_string(),
                });
            }
        }
    };

    if hash32(&bytes) != record.changeset_payload_hash {
        return Err(PersonalDbLogChainRepairReason::InvalidChangesetPayload {
            log_index: record.log_index,
            message: "personaldb changeset payload hash mismatch".to_string(),
        });
    }
    Ok(())
}

async fn verify_record_certificate(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    record: &PersonalDbLogRecord,
    trust_store: &PublicKeyTrustStore,
) -> std::result::Result<(), PersonalDbLogChainRepairReason> {
    let certificate = if !record.inline_certificate_bytes.is_empty() {
        decode_commit_certificate(&record.inline_certificate_bytes).map_err(|err| {
            PersonalDbLogChainRepairReason::InvalidCommitCertificate {
                log_index: record.log_index,
                message: err.to_string(),
            }
        })?
    } else if !record.certificate_ref.is_empty() {
        let certificate_ref = std::str::from_utf8(&record.certificate_ref).map_err(|err| {
            PersonalDbLogChainRepairReason::InvalidCommitCertificate {
                log_index: record.log_index,
                message: err.to_string(),
            }
        })?;
        match read_personaldb_commit_certificate_ref(storage, certificate_ref, trust_store).await {
            Ok(Some(certificate)) => certificate,
            Ok(None) => {
                return Err(PersonalDbLogChainRepairReason::MissingCommitCertificate {
                    log_index: record.log_index,
                });
            }
            Err(err) => {
                return Err(PersonalDbLogChainRepairReason::InvalidCommitCertificate {
                    log_index: record.log_index,
                    message: err.to_string(),
                });
            }
        }
    } else {
        let entry_hash = hex::encode(record.entry_hash);
        match read_personaldb_commit_certificate(
            storage,
            tenant_id,
            database_id,
            record.log_index,
            &entry_hash,
            trust_store,
        )
        .await
        {
            Ok(Some(certificate)) => certificate,
            Ok(None) => {
                return Err(PersonalDbLogChainRepairReason::MissingCommitCertificate {
                    log_index: record.log_index,
                });
            }
            Err(err) => {
                return Err(PersonalDbLogChainRepairReason::InvalidCommitCertificate {
                    log_index: record.log_index,
                    message: err.to_string(),
                });
            }
        }
    };

    certificate.verify(trust_store).map_err(|err| {
        PersonalDbLogChainRepairReason::InvalidCommitCertificate {
            log_index: record.log_index,
            message: err.to_string(),
        }
    })?;
    let certificate_hash = certificate.certificate_hash.as_deref().ok_or_else(|| {
        PersonalDbLogChainRepairReason::InvalidCommitCertificate {
            log_index: record.log_index,
            message: "personaldb commit certificate hash is missing".to_string(),
        }
    })?;
    let certificate_hash = hex32(certificate_hash).map_err(|err| {
        PersonalDbLogChainRepairReason::InvalidCommitCertificate {
            log_index: record.log_index,
            message: err.to_string(),
        }
    })?;
    if certificate_hash != record.certificate_hash {
        return Err(PersonalDbLogChainRepairReason::InvalidCommitCertificate {
            log_index: record.log_index,
            message: "personaldb commit certificate hash does not match log record".to_string(),
        });
    }
    if certificate.log_index != record.log_index
        || certificate.entry_hash != hex::encode(record.entry_hash)
    {
        return Err(PersonalDbLogChainRepairReason::InvalidCommitCertificate {
            log_index: record.log_index,
            message: "personaldb commit certificate does not match log record".to_string(),
        });
    }
    let payload_hash = hex32(&certificate.changeset_payload_hash).map_err(|err| {
        PersonalDbLogChainRepairReason::InvalidCommitCertificate {
            log_index: record.log_index,
            message: err.to_string(),
        }
    })?;
    if payload_hash != record.changeset_payload_hash {
        return Err(PersonalDbLogChainRepairReason::InvalidCommitCertificate {
            log_index: record.log_index,
            message: "personaldb commit certificate payload hash mismatch".to_string(),
        });
    }
    Ok(())
}

fn report_without_head(
    tenant_id: i64,
    database_id: &str,
    reason: PersonalDbLogChainRepairReason,
) -> PersonalDbLogChainRepairReport {
    PersonalDbLogChainRepairReport {
        status: PersonalDbLogChainRepairStatus::NeedsReview(reason),
        tenant_id,
        database_id: database_id.to_string(),
        committed_log_index: 0,
        verified_log_index: 0,
        committed_log_hash: String::new(),
        finding: None,
    }
}

fn report_with_head(
    tenant_id: i64,
    database_id: &str,
    head: &PersonalDbCommittedHead,
    verified_log_index: u64,
    reason: PersonalDbLogChainRepairReason,
) -> PersonalDbLogChainRepairReport {
    PersonalDbLogChainRepairReport {
        status: PersonalDbLogChainRepairStatus::NeedsReview(reason),
        tenant_id,
        database_id: database_id.to_string(),
        committed_log_index: head.log_index,
        verified_log_index,
        committed_log_hash: head.log_hash.clone(),
        finding: None,
    }
}

pub fn status_name(status: &PersonalDbLogChainRepairStatus) -> &'static str {
    match status {
        PersonalDbLogChainRepairStatus::EmptySource => "empty_source",
        PersonalDbLogChainRepairStatus::UpToDate => "up_to_date",
        PersonalDbLogChainRepairStatus::NeedsReview(_) => "needs_review",
    }
}

pub fn status_reason(status: &PersonalDbLogChainRepairStatus) -> String {
    match status {
        PersonalDbLogChainRepairStatus::EmptySource | PersonalDbLogChainRepairStatus::UpToDate => {
            String::new()
        }
        PersonalDbLogChainRepairStatus::NeedsReview(reason) => reason_code(reason).to_string(),
    }
}

fn repair_finding_write(
    tenant_id: i64,
    database_id: &str,
    committed_log_index: u64,
    committed_log_hash: &str,
    reason: &PersonalDbLogChainRepairReason,
    lease_fence_token: u64,
) -> Result<RepairFindingWrite> {
    let now_nanos = chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("current timestamp is out of range"))?;
    let reason_code = reason_code(reason);
    let finding_seed = format!(
        "{tenant_id}:{database_id}:{committed_log_index}:{committed_log_hash}:{reason_code}"
    );
    let finding_hash = hash32(finding_seed.as_bytes());
    let expected_hash = if committed_log_hash.len() == 64 {
        Some(committed_log_hash.to_string())
    } else {
        None
    };

    Ok(RepairFindingWrite {
        finding_id: format!("personaldb-{}", hex::encode(&finding_hash[..8])),
        scope_kind: "personaldb".to_string(),
        scope_id: format!("tenant-{tenant_id}-database-{database_id}"),
        repair_task_id: format!("personaldb-repair-{database_id}"),
        lease_fence_token,
        severity: RepairFindingSeverity::Error,
        status: RepairFindingStatus::RequiresOperatorReview,
        code: reason_code.to_string(),
        message: reason_message(reason).to_string(),
        subjects: vec![RepairSubjectRef {
            subject_kind: "personaldb_log_chain".to_string(),
            subject_id: database_id.to_string(),
            generation: None,
            cursor: Some(u128::from(committed_log_index)),
            expected_hash,
            actual_hash: None,
        }],
        proposed_action: RepairActionKind::VerifyOnly,
        evidence: json!({
            "tenant_id": tenant_id,
            "database_id": database_id,
            "committed_log_index": committed_log_index,
            "committed_log_hash": committed_log_hash,
            "repair_reason": reason_code,
            "reason_detail": reason_detail(reason),
        }),
        created_at_nanos: now_nanos,
    })
}

fn verified_index_before_reason(reason: &PersonalDbLogChainRepairReason) -> u64 {
    match reason {
        PersonalDbLogChainRepairReason::NonContiguousLogChain { log_index }
        | PersonalDbLogChainRepairReason::LogPreviousHashMismatch { log_index }
        | PersonalDbLogChainRepairReason::MissingChangesetPayload { log_index }
        | PersonalDbLogChainRepairReason::InvalidChangesetPayload { log_index, .. }
        | PersonalDbLogChainRepairReason::MissingCommitCertificate { log_index }
        | PersonalDbLogChainRepairReason::InvalidCommitCertificate { log_index, .. } => {
            log_index.saturating_sub(1)
        }
        PersonalDbLogChainRepairReason::MissingLogSegment { expected_log_index } => {
            expected_log_index.saturating_sub(1)
        }
        _ => 0,
    }
}

fn reason_code(reason: &PersonalDbLogChainRepairReason) -> &'static str {
    match reason {
        PersonalDbLogChainRepairReason::MissingManifest => "PersonalDbManifestMissing",
        PersonalDbLogChainRepairReason::InvalidManifest(_) => "PersonalDbManifestInvalid",
        PersonalDbLogChainRepairReason::MissingCommittedHead => "PersonalDbCommittedHeadMissing",
        PersonalDbLogChainRepairReason::InvalidCommittedHead(_) => "PersonalDbCommittedHeadInvalid",
        PersonalDbLogChainRepairReason::HeadManifestMismatch => "PersonalDbHeadManifestMismatch",
        PersonalDbLogChainRepairReason::GenesisHeadMismatch => "PersonalDbGenesisHeadMismatch",
        PersonalDbLogChainRepairReason::MissingLogSegment { .. } => "PersonalDbLogSegmentMissing",
        PersonalDbLogChainRepairReason::InvalidLogSegment { .. } => "PersonalDbLogSegmentInvalid",
        PersonalDbLogChainRepairReason::NonContiguousLogChain { .. } => {
            "PersonalDbLogChainNonContiguous"
        }
        PersonalDbLogChainRepairReason::LogPreviousHashMismatch { .. } => {
            "PersonalDbLogPreviousHashMismatch"
        }
        PersonalDbLogChainRepairReason::CommittedHeadMismatch => "PersonalDbCommittedHeadMismatch",
        PersonalDbLogChainRepairReason::MissingChangesetPayload { .. } => {
            "PersonalDbChangesetPayloadMissing"
        }
        PersonalDbLogChainRepairReason::InvalidChangesetPayload { .. } => {
            "PersonalDbChangesetPayloadInvalid"
        }
        PersonalDbLogChainRepairReason::MissingCommitCertificate { .. } => {
            "PersonalDbCommitCertificateMissing"
        }
        PersonalDbLogChainRepairReason::InvalidCommitCertificate { .. } => {
            "PersonalDbCommitCertificateInvalid"
        }
    }
}

fn reason_message(reason: &PersonalDbLogChainRepairReason) -> &'static str {
    match reason {
        PersonalDbLogChainRepairReason::MissingManifest => "PersonalDB group manifest is missing",
        PersonalDbLogChainRepairReason::InvalidManifest(_) => {
            "PersonalDB group manifest cannot be decoded or verified"
        }
        PersonalDbLogChainRepairReason::MissingCommittedHead => {
            "PersonalDB committed head is missing"
        }
        PersonalDbLogChainRepairReason::InvalidCommittedHead(_) => {
            "PersonalDB committed head cannot be decoded or verified"
        }
        PersonalDbLogChainRepairReason::HeadManifestMismatch => {
            "PersonalDB committed head does not match group manifest"
        }
        PersonalDbLogChainRepairReason::GenesisHeadMismatch => {
            "PersonalDB genesis hash does not match the committed chain"
        }
        PersonalDbLogChainRepairReason::MissingLogSegment { .. } => {
            "PersonalDB log segment is missing"
        }
        PersonalDbLogChainRepairReason::InvalidLogSegment { .. } => {
            "PersonalDB log segment cannot be decoded or verified"
        }
        PersonalDbLogChainRepairReason::NonContiguousLogChain { .. } => {
            "PersonalDB log chain has a gap or duplicate index"
        }
        PersonalDbLogChainRepairReason::LogPreviousHashMismatch { .. } => {
            "PersonalDB log chain previous hash link is invalid"
        }
        PersonalDbLogChainRepairReason::CommittedHeadMismatch => {
            "PersonalDB committed head does not match the readable log chain"
        }
        PersonalDbLogChainRepairReason::MissingChangesetPayload { .. } => {
            "PersonalDB changeset payload is missing"
        }
        PersonalDbLogChainRepairReason::InvalidChangesetPayload { .. } => {
            "PersonalDB changeset payload cannot be verified"
        }
        PersonalDbLogChainRepairReason::MissingCommitCertificate { .. } => {
            "PersonalDB commit certificate is missing"
        }
        PersonalDbLogChainRepairReason::InvalidCommitCertificate { .. } => {
            "PersonalDB commit certificate cannot be verified"
        }
    }
}

fn reason_detail(reason: &PersonalDbLogChainRepairReason) -> serde_json::Value {
    match reason {
        PersonalDbLogChainRepairReason::InvalidManifest(message)
        | PersonalDbLogChainRepairReason::InvalidCommittedHead(message) => {
            json!({"message": message})
        }
        PersonalDbLogChainRepairReason::MissingLogSegment { expected_log_index } => {
            json!({"expected_log_index": expected_log_index})
        }
        PersonalDbLogChainRepairReason::InvalidLogSegment {
            segment_id,
            message,
        } => json!({"segment_id": segment_id, "message": message}),
        PersonalDbLogChainRepairReason::NonContiguousLogChain { log_index }
        | PersonalDbLogChainRepairReason::LogPreviousHashMismatch { log_index }
        | PersonalDbLogChainRepairReason::MissingChangesetPayload { log_index }
        | PersonalDbLogChainRepairReason::MissingCommitCertificate { log_index } => {
            json!({"log_index": log_index})
        }
        PersonalDbLogChainRepairReason::InvalidChangesetPayload { log_index, message }
        | PersonalDbLogChainRepairReason::InvalidCommitCertificate { log_index, message } => {
            json!({"log_index": log_index, "message": message})
        }
        _ => json!({}),
    }
}

fn hex32(value: &str) -> Result<Hash32> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("value must be 32 bytes encoded as hex"));
    }
    Ok(hex::decode(value)?
        .try_into()
        .map_err(|_| anyhow!("value must be hex32"))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        personaldb_commit_store::{
            write_personaldb_changeset_payload, write_personaldb_commit_certificate,
        },
        personaldb_control::{PersonalDbCommitCertificate, PersonalDbGroupManifest},
        personaldb_coremeta::delete_personaldb_data_locator_row,
        personaldb_heads::{
            PersonalDbCommittedHead, write_personaldb_committed_head,
            write_personaldb_group_manifest,
        },
        personaldb_segment::{PersonalDbLogSegmentWrite, write_personaldb_log_segment},
        test_support::personaldb_protocol_keyring,
    };
    use tempfile::{TempDir, tempdir};

    const KEY: &[u8] = b"personaldb repair signing key";

    #[tokio::test]
    async fn healthy_chain_is_up_to_date() {
        let fixture = Fixture::create().await;
        let report = repair_personaldb_log_chain(
            &fixture.storage,
            &fixture.mvcc,
            7,
            "db-alpha",
            9,
            fixture.protocol_keyring.trust_store(),
            KEY,
        )
        .await
        .unwrap();
        assert_eq!(report.status, PersonalDbLogChainRepairStatus::UpToDate);
        assert_eq!(report.committed_log_index, 1);
        assert_eq!(report.verified_log_index, 1);
        assert!(report.finding.is_none());
    }

    #[tokio::test]
    async fn missing_payload_requires_operator_review() {
        let fixture = Fixture::create().await;
        delete_personaldb_data_locator_row(
            &fixture.storage,
            7,
            "db-alpha",
            &fixture.payload_ref,
            "test-delete-payload-locator",
        )
        .await
        .unwrap();

        let report = repair_personaldb_log_chain(
            &fixture.storage,
            &fixture.mvcc,
            7,
            "db-alpha",
            9,
            fixture.protocol_keyring.trust_store(),
            KEY,
        )
        .await
        .unwrap();
        assert_eq!(status_name(&report.status), "needs_review");
        assert_eq!(
            status_reason(&report.status),
            "PersonalDbChangesetPayloadMissing"
        );
        let finding = report.finding.expect("repair finding");
        assert_eq!(finding.status, RepairFindingStatus::RequiresOperatorReview);
        assert_eq!(finding.proposed_action, RepairActionKind::VerifyOnly);
    }

    #[tokio::test]
    async fn missing_certificate_requires_operator_review() {
        let fixture = Fixture::create().await;
        delete_personaldb_data_locator_row(
            &fixture.storage,
            7,
            "db-alpha",
            &fixture.certificate_ref,
            "test-delete-certificate-locator",
        )
        .await
        .unwrap();

        let report = repair_personaldb_log_chain(
            &fixture.storage,
            &fixture.mvcc,
            7,
            "db-alpha",
            9,
            fixture.protocol_keyring.trust_store(),
            KEY,
        )
        .await
        .unwrap();
        assert_eq!(status_name(&report.status), "needs_review");
        assert_eq!(
            status_reason(&report.status),
            "PersonalDbCommitCertificateMissing"
        );
        let finding = report.finding.expect("repair finding");
        assert_eq!(finding.status, RepairFindingStatus::RequiresOperatorReview);
        assert_eq!(finding.proposed_action, RepairActionKind::VerifyOnly);
    }

    struct Fixture {
        _temp: TempDir,
        storage: Storage,
        mvcc: crate::mvcc_bootstrap::MvccSubsystem,
        payload_ref: String,
        certificate_ref: String,
        protocol_keyring: crate::personaldb_signing::PersonalDbProtocolKeyring,
    }

    impl Fixture {
        async fn create() -> Self {
            let temp = tempdir().unwrap();
            let storage = Storage::new_at(temp.path()).await.unwrap();
            let config = crate::Config {
                node_id: "personaldb-repair-test".into(),
                storage_path: temp.path().to_string_lossy().into_owned(),
                public_api_addr: "127.0.0.1:0".into(),
                ..crate::Config::default()
            };
            let meta =
                crate::core_store::CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
            let mvcc = crate::mvcc_bootstrap::MvccSubsystem::bootstrap(&config, meta.database())
                .await
                .unwrap();
            let protocol_keyring = personaldb_protocol_keyring();
            let schema_hash = hash32(b"schema");
            let genesis_hash = hash32(b"genesis");
            let payload = b"changeset";
            let payload_hash = hash32(payload);
            let payload_paths = write_personaldb_changeset_payload(
                &storage,
                7,
                "db-alpha",
                1,
                payload_hash,
                payload,
            )
            .await
            .unwrap();
            let payload_ref = payload_paths.by_index_ref.clone();
            let provisional = PersonalDbLogRecord::new(
                1,
                1,
                1,
                1,
                genesis_hash,
                payload_hash,
                hash32(b"envelope"),
                [0; 32],
                payload_ref.clone().into_bytes(),
                Vec::new(),
                Vec::new(),
            );
            let certificate = PersonalDbCommitCertificate {
                format_version: 2,
                tenant_id: "7".to_string(),
                database_id: "db-alpha".to_string(),
                log_index: 1,
                previous_log_hash: hex::encode(genesis_hash),
                entry_hash: hex::encode(provisional.entry_hash),
                changeset_payload_hash: hex::encode(payload_hash),
                verified_envelope_hash: hex::encode(hash32(b"envelope")),
                client_log_epoch: 1,
                membership_epoch: 1,
                policy_epoch: 1,
                leader_replica_id: "leader".to_string(),
                voter_acks_hash: hex::encode(hash32(b"acks")),
                authz_revision: 1,
                witness_node_id: "node".to_string(),
                witnessed_at: "2026-06-28T00:00:00Z".to_string(),
                certificate_hash: None,
                witness_signature: None,
            }
            .seal(&protocol_keyring)
            .await
            .unwrap();
            let certificate_ref = write_personaldb_commit_certificate(
                &storage,
                7,
                "db-alpha",
                &certificate,
                protocol_keyring.trust_store(),
            )
            .await
            .unwrap();
            let record = PersonalDbLogRecord::new(
                1,
                1,
                1,
                1,
                genesis_hash,
                payload_hash,
                hash32(b"envelope"),
                hex32(certificate.certificate_hash.as_deref().unwrap()).unwrap(),
                payload_ref.clone().into_bytes(),
                certificate_ref.clone().into_bytes(),
                Vec::new(),
            );
            let manifest = PersonalDbGroupManifest {
                format_version: 2,
                tenant_id: "7".to_string(),
                database_id: "db-alpha".to_string(),
                schema_hash: hex::encode(schema_hash),
                genesis_hash: hex::encode(genesis_hash),
                created_at: "2026-06-28T00:00:00Z".to_string(),
                created_by: "node".to_string(),
                consistency_policy: "StrictWitnessed".to_string(),
                object_layout_version: 1,
                active_membership_epoch: 1,
                active_policy_epoch: 1,
                current_row_index_generation: 0,
                current_projection_generation: 0,
                manifest_hash: None,
                manifest_signature: None,
            }
            .seal(&protocol_keyring)
            .await
            .unwrap();
            write_personaldb_group_manifest(&storage, 7, &manifest, protocol_keyring.trust_store())
                .await
                .unwrap();
            let segment_ref = write_personaldb_log_segment(
                &storage,
                PersonalDbLogSegmentWrite {
                    tenant_id: 7,
                    database_id: "db-alpha",
                    schema_hash,
                    source_fence_token: 3,
                    records: std::slice::from_ref(&record),
                },
            )
            .await
            .unwrap();
            let head = PersonalDbCommittedHead {
                format_version: 2,
                tenant_id: "7".to_string(),
                database_id: "db-alpha".to_string(),
                log_index: 1,
                log_hash: hex::encode(record.entry_hash),
                segment_ref,
                row_index_generation: 0,
                policy_epoch: 1,
                membership_epoch: 1,
                schema_hash: hex::encode(schema_hash),
                updated_at: "2026-06-28T00:00:00Z".to_string(),
                updated_by_node: "node".to_string(),
                head_hash: None,
                head_signature: None,
            }
            .seal(&protocol_keyring)
            .await
            .unwrap();
            write_personaldb_committed_head(
                &storage,
                7,
                "db-alpha",
                &head,
                protocol_keyring.trust_store(),
            )
            .await
            .unwrap();
            Self {
                _temp: temp,
                storage,
                mvcc,
                payload_ref,
                certificate_ref,
                protocol_keyring,
            }
        }
    }
}
