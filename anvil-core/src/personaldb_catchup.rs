use crate::{
    formats::personaldb::PersonalDbLogRecord,
    personaldb_commit_store::{
        decode_commit_certificate, encode_commit_certificate,
        read_personaldb_changeset_payload_by_index, read_personaldb_changeset_payload_ref,
        read_personaldb_commit_certificate, read_personaldb_commit_certificate_ref,
    },
    personaldb_control::PersonalDbCommitCertificate,
    personaldb_heads::{
        PersonalDbCommittedHead, PersonalDbSnapshotsHead, read_personaldb_group_manifest,
    },
    personaldb_segment::{list_personaldb_log_segment_refs, read_personaldb_log_segment},
    storage::Storage,
};
use anyhow::{Context, Result, anyhow};
use personaldb_protocol::PublicKeyTrustStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbCatchUpRequest {
    pub tenant_id: i64,
    pub database_id: String,
    pub principal: String,
    pub replica_id: String,
    pub have_log_index: u64,
    pub have_log_hash: String,
    pub max_entries: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersonalDbCatchUpResponse {
    Entries(PersonalDbCatchUpEntries),
    SnapshotRequired(PersonalDbSnapshotRestore),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbCatchUpEntries {
    pub committed_head: PersonalDbCommittedHead,
    pub entries: Vec<PersonalDbCatchUpEntry>,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbCatchUpEntry {
    pub record: PersonalDbLogRecord,
    pub changeset_bytes: Vec<u8>,
    pub certificate: PersonalDbCommitCertificate,
    pub certificate_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbSnapshotRestore {
    pub committed_head: Option<PersonalDbCommittedHead>,
    pub snapshots_head: Option<PersonalDbSnapshotsHead>,
    pub reason: PersonalDbSnapshotRestoreReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersonalDbSnapshotRestoreReason {
    MissingCommittedHead,
    DivergentReplica,
}

pub async fn personaldb_catch_up(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    request: PersonalDbCatchUpRequest,
    trust_store: &PublicKeyTrustStore,
) -> Result<PersonalDbCatchUpResponse> {
    validate_request(&request)?;
    let snapshot_version = mvcc.runtime.applied_version()?;
    let Some(committed_head) =
        crate::personaldb_proposal_admission::read_personaldb_committed_head_at_snapshot(
            mvcc,
            request.tenant_id,
            &request.database_id,
            trust_store,
            snapshot_version,
        )?
    else {
        return Ok(snapshot_required(
            storage,
            mvcc,
            snapshot_version,
            &request,
            None,
            trust_store,
            PersonalDbSnapshotRestoreReason::MissingCommittedHead,
        )
        .await?);
    };

    let records = read_canonical_records(storage, &request.database_id, &committed_head).await?;
    ensure_head_matches_records(&committed_head, &records)?;
    if !is_replica_position_on_chain(storage, &request, trust_store, &records).await? {
        return Ok(snapshot_required(
            storage,
            mvcc,
            snapshot_version,
            &request,
            Some(committed_head),
            trust_store,
            PersonalDbSnapshotRestoreReason::DivergentReplica,
        )
        .await?);
    }

    let available = records
        .iter()
        .filter(|record| record.log_index > request.have_log_index)
        .cloned()
        .collect::<Vec<_>>();
    let selected_len = available.len().min(request.max_entries);
    let has_more = selected_len < available.len();
    let mut entries = Vec::with_capacity(selected_len);
    for record in available.into_iter().take(selected_len) {
        entries.push(
            load_catch_up_entry(
                storage,
                request.tenant_id,
                &request.database_id,
                record,
                trust_store,
            )
            .await?,
        );
    }

    Ok(PersonalDbCatchUpResponse::Entries(
        PersonalDbCatchUpEntries {
            committed_head,
            entries,
            has_more,
        },
    ))
}

async fn read_canonical_records(
    storage: &Storage,
    database_id: &str,
    committed_head: &PersonalDbCommittedHead,
) -> Result<Vec<PersonalDbLogRecord>> {
    if committed_head.log_index == 0 {
        return Ok(Vec::new());
    }
    let segment_refs = list_log_segment_refs(storage, committed_head, database_id).await?;
    let mut records = Vec::new();
    for segment_ref in segment_refs {
        let segment = read_personaldb_log_segment(storage, &segment_ref).await?;
        for record in segment.records {
            if record.log_index <= committed_head.log_index {
                records.push(record);
            }
        }
    }
    records.sort_by_key(|record| record.log_index);
    ensure_contiguous_chain(&records)?;
    Ok(records)
}

async fn list_log_segment_refs(
    storage: &Storage,
    committed_head: &PersonalDbCommittedHead,
    database_id: &str,
) -> Result<Vec<String>> {
    let tenant_id = committed_head
        .tenant_id
        .parse::<i64>()
        .context("personaldb committed head tenant id must be numeric")?;
    list_personaldb_log_segment_refs(storage, tenant_id, database_id).await
}

fn ensure_head_matches_records(
    committed_head: &PersonalDbCommittedHead,
    records: &[PersonalDbLogRecord],
) -> Result<()> {
    if committed_head.log_index == 0 {
        return Ok(());
    }
    let Some(last) = records.last() else {
        return Err(anyhow!(
            "personaldb committed head has no readable log records"
        ));
    };
    if last.log_index != committed_head.log_index {
        return Err(anyhow!(
            "personaldb committed head log index is not readable"
        ));
    }
    if hex::encode(last.entry_hash) != committed_head.log_hash {
        return Err(anyhow!(
            "personaldb committed head hash does not match log chain"
        ));
    }
    Ok(())
}

async fn is_replica_position_on_chain(
    storage: &Storage,
    request: &PersonalDbCatchUpRequest,
    trust_store: &PublicKeyTrustStore,
    records: &[PersonalDbLogRecord],
) -> Result<bool> {
    if request.have_log_index == 0 {
        let Some(manifest) = read_personaldb_group_manifest(
            storage,
            request.tenant_id,
            &request.database_id,
            trust_store,
        )
        .await?
        else {
            return Ok(false);
        };
        return Ok(request.have_log_hash == manifest.genesis_hash);
    }
    Ok(records.iter().any(|record| {
        record.log_index == request.have_log_index
            && hex::encode(record.entry_hash) == request.have_log_hash
    }))
}

async fn snapshot_required(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot_version: u64,
    request: &PersonalDbCatchUpRequest,
    committed_head: Option<PersonalDbCommittedHead>,
    trust_store: &PublicKeyTrustStore,
    reason: PersonalDbSnapshotRestoreReason,
) -> Result<PersonalDbCatchUpResponse> {
    let snapshots_head = crate::personaldb_heads::read_personaldb_snapshots_head_at_snapshot(
        mvcc,
        request.tenant_id,
        &request.database_id,
        trust_store,
        snapshot_version,
    )?;
    Ok(PersonalDbCatchUpResponse::SnapshotRequired(
        PersonalDbSnapshotRestore {
            committed_head,
            snapshots_head,
            reason,
        },
    ))
}

async fn load_catch_up_entry(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    record: PersonalDbLogRecord,
    trust_store: &PublicKeyTrustStore,
) -> Result<PersonalDbCatchUpEntry> {
    let changeset_bytes = load_changeset_bytes(storage, tenant_id, database_id, &record).await?;
    let (certificate, certificate_bytes) =
        load_certificate(storage, tenant_id, database_id, &record, trust_store).await?;
    Ok(PersonalDbCatchUpEntry {
        record,
        changeset_bytes,
        certificate,
        certificate_bytes,
    })
}

async fn load_changeset_bytes(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    record: &PersonalDbLogRecord,
) -> Result<Vec<u8>> {
    if !record.payload_ref.is_empty() {
        let payload_ref = std::str::from_utf8(&record.payload_ref)?;
        return read_personaldb_changeset_payload_ref(
            storage,
            payload_ref,
            record.changeset_payload_hash,
        )
        .await?
        .ok_or_else(|| anyhow!("personaldb changeset payload is missing"));
    }
    read_personaldb_changeset_payload_by_index(
        storage,
        tenant_id,
        database_id,
        record.log_index,
        record.changeset_payload_hash,
    )
    .await?
    .ok_or_else(|| anyhow!("personaldb changeset payload is missing"))
}

async fn load_certificate(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    record: &PersonalDbLogRecord,
    trust_store: &PublicKeyTrustStore,
) -> Result<(PersonalDbCommitCertificate, Vec<u8>)> {
    let certificate_bytes = if !record.inline_certificate_bytes.is_empty() {
        record.inline_certificate_bytes.clone()
    } else if !record.certificate_ref.is_empty() {
        let certificate_ref = std::str::from_utf8(&record.certificate_ref)?;
        let certificate =
            read_personaldb_commit_certificate_ref(storage, certificate_ref, trust_store)
                .await?
                .ok_or_else(|| anyhow!("personaldb commit certificate is missing"))?;
        encode_commit_certificate(&certificate)?
    } else {
        let entry_hash = hex::encode(record.entry_hash);
        let certificate = read_personaldb_commit_certificate(
            storage,
            tenant_id,
            database_id,
            record.log_index,
            &entry_hash,
            trust_store,
        )
        .await?
        .ok_or_else(|| anyhow!("personaldb commit certificate is missing"))?;
        encode_commit_certificate(&certificate)?
    };
    let certificate = decode_commit_certificate(&certificate_bytes)?;
    certificate.verify(trust_store)?;
    let certificate_hash = certificate
        .certificate_hash
        .as_deref()
        .ok_or_else(|| anyhow!("personaldb commit certificate hash is missing"))?;
    if hex::decode(certificate_hash)?.as_slice() != record.certificate_hash {
        return Err(anyhow!(
            "personaldb commit certificate hash does not match log record"
        ));
    }
    if certificate.log_index != record.log_index {
        return Err(anyhow!("personaldb commit certificate log index mismatch"));
    }
    if certificate.entry_hash != hex::encode(record.entry_hash) {
        return Err(anyhow!("personaldb commit certificate entry hash mismatch"));
    }
    if hex::decode(&certificate.changeset_payload_hash)?.as_slice() != record.changeset_payload_hash
    {
        return Err(anyhow!(
            "personaldb commit certificate payload hash mismatch"
        ));
    }
    Ok((certificate, certificate_bytes))
}

fn ensure_contiguous_chain(records: &[PersonalDbLogRecord]) -> Result<()> {
    for pair in records.windows(2) {
        let previous = &pair[0];
        let current = &pair[1];
        if current.log_index != previous.log_index + 1 {
            return Err(anyhow!("personaldb log chain has a gap"));
        }
        if current.previous_log_hash != previous.entry_hash {
            return Err(anyhow!("personaldb log chain previous hash mismatch"));
        }
    }
    Ok(())
}

fn validate_request(request: &PersonalDbCatchUpRequest) -> Result<()> {
    if request.database_id.is_empty() {
        return Err(anyhow!("personaldb catch-up database id must not be empty"));
    }
    if request.principal.is_empty() {
        return Err(anyhow!("personaldb catch-up principal must not be empty"));
    }
    if request.replica_id.is_empty() {
        return Err(anyhow!("personaldb catch-up replica id must not be empty"));
    }
    if request.have_log_hash.len() != 64
        || !request
            .have_log_hash
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(anyhow!("personaldb catch-up log hash must be hex32"));
    }
    Ok(())
}
