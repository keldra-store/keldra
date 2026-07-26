use crate::{
    anvil_personaldb_sqlite_changeset::apply_changeset_to_snapshot_builder,
    formats::{Hash32, hash32, personaldb::PersonalDbLogRecord},
    personaldb_commit_store::{
        read_personaldb_changeset_payload_by_index, read_personaldb_changeset_payload_ref,
    },
    personaldb_control::PersonalDbSnapshotManifest,
    personaldb_heads::{
        PersonalDbCommittedHead, PersonalDbSnapshotsHead, read_personaldb_group_manifest,
    },
    personaldb_segment::{list_personaldb_log_segment_refs, read_personaldb_log_segment},
    personaldb_signing::PersonalDbProtocolKeyring,
    personaldb_snapshot_store::{
        personaldb_snapshot_manifest_ref_name, personaldb_snapshot_object_ref_name,
        read_personaldb_snapshot_manifest_by_ref, read_personaldb_snapshot_object,
        write_personaldb_snapshot,
    },
    storage::Storage,
};
use anyhow::{Context, Result, anyhow};
use personaldb_protocol::PublicKeyTrustStore;
use rusqlite::Connection;
use std::{io::Cursor, path::Path};
use tempfile::NamedTempFile;

pub const DEFAULT_SNAPSHOT_ENTRY_THRESHOLD: u64 = 1024;
pub const DEFAULT_SNAPSHOT_PAYLOAD_BYTES_THRESHOLD: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersonalDbSnapshotPolicy {
    pub entry_threshold: u64,
    pub payload_bytes_threshold: u64,
}

impl Default for PersonalDbSnapshotPolicy {
    fn default() -> Self {
        Self {
            entry_threshold: DEFAULT_SNAPSHOT_ENTRY_THRESHOLD,
            payload_bytes_threshold: DEFAULT_SNAPSHOT_PAYLOAD_BYTES_THRESHOLD,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersonalDbSnapshotBuildRequest<'a> {
    pub tenant_id: i64,
    pub database_id: &'a str,
    pub schema_sql: &'a str,
    pub created_by_node: &'a str,
    pub policy: PersonalDbSnapshotPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbSnapshotBuildResult {
    pub manifest: PersonalDbSnapshotManifest,
    pub compressed_sqlite_bytes: Vec<u8>,
    pub uncompressed_state_hash: Hash32,
}

pub async fn maybe_build_personaldb_snapshot(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    request: PersonalDbSnapshotBuildRequest<'_>,
    protocol_keyring: &PersonalDbProtocolKeyring,
) -> Result<Option<PersonalDbSnapshotBuildResult>> {
    validate_request(&request)?;
    let snapshot_version = mvcc.runtime.applied_version()?;
    let manifest = read_personaldb_group_manifest(
        storage,
        mvcc,
        request.tenant_id,
        request.database_id,
        protocol_keyring.trust_store(),
        snapshot_version,
    )
    .await?
    .ok_or_else(|| anyhow!("personaldb group manifest missing"))?;
    let committed_head =
        crate::personaldb_proposal_admission::read_personaldb_committed_head_at_snapshot(
            mvcc,
            request.tenant_id,
            request.database_id,
            protocol_keyring.trust_store(),
            snapshot_version,
        )?
        .ok_or_else(|| anyhow!("personaldb committed head missing"))?;
    if committed_head.log_index == 0 {
        return Ok(None);
    }
    if manifest.schema_hash != hex::encode(hash32(request.schema_sql.as_bytes())) {
        return Err(anyhow!("personaldb snapshot schema hash mismatch"));
    }

    let previous_snapshot = crate::personaldb_heads::read_personaldb_snapshots_head_at_snapshot(
        mvcc,
        request.tenant_id,
        request.database_id,
        protocol_keyring.trust_store(),
        snapshot_version,
    )?;
    let base_log_index = previous_snapshot
        .as_ref()
        .map(|head| head.latest_snapshot_log_index)
        .unwrap_or(0);
    if committed_head.log_index <= base_log_index {
        return Ok(None);
    }

    let records = read_canonical_records(
        storage,
        mvcc,
        request.database_id,
        &committed_head,
        snapshot_version,
    )
    .await?;
    ensure_head_matches_records(&committed_head, &records)?;
    let new_records = records
        .iter()
        .filter(|record| record.log_index > base_log_index)
        .cloned()
        .collect::<Vec<_>>();
    let payload_bytes = sum_changeset_payload_bytes(
        storage,
        mvcc,
        request.tenant_id,
        request.database_id,
        &new_records,
        snapshot_version,
    )
    .await?;
    if (new_records.len() as u64) < request.policy.entry_threshold
        && payload_bytes < request.policy.payload_bytes_threshold
    {
        return Ok(None);
    }

    let result = build_snapshot(
        storage,
        mvcc,
        request,
        protocol_keyring,
        previous_snapshot.as_ref(),
        &committed_head,
        &new_records,
        snapshot_version,
    )
    .await?;
    publish_snapshots_head(
        mvcc,
        request,
        protocol_keyring,
        previous_snapshot.as_ref(),
        &result.manifest,
    )
    .await?;
    Ok(Some(result))
}

async fn build_snapshot(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    request: PersonalDbSnapshotBuildRequest<'_>,
    protocol_keyring: &PersonalDbProtocolKeyring,
    previous_snapshot: Option<&PersonalDbSnapshotsHead>,
    committed_head: &PersonalDbCommittedHead,
    new_records: &[PersonalDbLogRecord],
    snapshot_version: u64,
) -> Result<PersonalDbSnapshotBuildResult> {
    // Class C scratch: the SQLite file is a build workspace, not the snapshot's durable state.
    let temp = NamedTempFile::new_in(storage.temp_dir_path())?;
    let temp_path = temp.path().to_path_buf();
    drop(temp);

    if let Some(snapshot_head) = previous_snapshot {
        restore_snapshot_database_scratch(
            storage,
            mvcc,
            request,
            protocol_keyring.trust_store(),
            snapshot_head,
            &temp_path,
            snapshot_version,
        )
        .await?;
    }

    {
        let connection = Connection::open(&temp_path)?;
        if previous_snapshot.is_none() {
            connection.execute_batch(request.schema_sql)?;
        }
        for record in new_records {
            let changeset = load_changeset_bytes(
                storage,
                mvcc,
                request.tenant_id,
                request.database_id,
                record,
                snapshot_version,
            )
            .await?;
            apply_changeset_to_snapshot_builder(&connection, &changeset)?;
        }
        connection.execute_batch("PRAGMA optimize;")?;
    }

    let sqlite_bytes = tokio::fs::read(&temp_path)
        .await
        .with_context(|| format!("read personaldb snapshot builder {}", temp_path.display()))?;
    let uncompressed_state_hash = hash32(&sqlite_bytes);
    let compressed_sqlite_bytes = zstd::stream::encode_all(Cursor::new(&sqlite_bytes), 3)?;
    let snapshot_object_hash = hash32(&compressed_sqlite_bytes);
    let state_hash = hex::encode(uncompressed_state_hash);
    let snapshot_object_key = personaldb_snapshot_object_ref_name(
        request.tenant_id,
        request.database_id,
        committed_head.log_index,
        &state_hash,
    )?;
    let manifest = PersonalDbSnapshotManifest {
        format_version: 1,
        tenant_id: request.tenant_id.to_string(),
        database_id: request.database_id.to_string(),
        log_index: committed_head.log_index,
        log_hash: committed_head.log_hash.clone(),
        state_hash,
        schema_hash: committed_head.schema_hash.clone(),
        snapshot_object_key,
        snapshot_object_hash: hex::encode(snapshot_object_hash),
        source_segment_start: new_records
            .first()
            .map(|record| record.log_index)
            .unwrap_or(0),
        source_segment_end: committed_head.log_index,
        row_index_generation: committed_head.row_index_generation,
        created_at: chrono::Utc::now().to_rfc3339(),
        created_by_node: request.created_by_node.to_string(),
        manifest_hash: None,
        manifest_signature: None,
    }
    .seal(protocol_keyring)
    .await?;

    write_personaldb_snapshot(
        storage,
        mvcc,
        request.tenant_id,
        request.database_id,
        &compressed_sqlite_bytes,
        &manifest,
        protocol_keyring.trust_store(),
    )
    .await?;
    let _ = tokio::fs::remove_file(&temp_path).await;
    Ok(PersonalDbSnapshotBuildResult {
        manifest,
        compressed_sqlite_bytes,
        uncompressed_state_hash,
    })
}

async fn restore_snapshot_database_scratch(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    request: PersonalDbSnapshotBuildRequest<'_>,
    trust_store: &PublicKeyTrustStore,
    snapshot_head: &PersonalDbSnapshotsHead,
    target_path: &Path,
    snapshot_version: u64,
) -> Result<()> {
    let manifest = read_personaldb_snapshot_manifest_by_ref(
        storage,
        mvcc,
        &snapshot_head.latest_snapshot_manifest_ref,
        trust_store,
        snapshot_version,
    )
    .await?
    .ok_or_else(|| anyhow!("personaldb snapshot manifest missing"))?;
    if manifest.tenant_id != request.tenant_id.to_string()
        || manifest.database_id != request.database_id
        || manifest.log_index != snapshot_head.latest_snapshot_log_index
        || manifest.log_hash != snapshot_head.latest_snapshot_log_hash
    {
        return Err(anyhow!("personaldb snapshot head does not match manifest"));
    }
    let compressed = read_personaldb_snapshot_object(
        storage,
        mvcc,
        request.tenant_id,
        request.database_id,
        &manifest,
        trust_store,
        snapshot_version,
    )
    .await?
    .ok_or_else(|| anyhow!("personaldb snapshot object missing"))?;
    let sqlite_bytes = zstd::stream::decode_all(Cursor::new(compressed))?;
    tokio::fs::write(target_path, sqlite_bytes)
        .await
        .with_context(|| format!("restore personaldb snapshot {}", target_path.display()))?;
    Ok(())
}

async fn publish_snapshots_head(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    request: PersonalDbSnapshotBuildRequest<'_>,
    protocol_keyring: &PersonalDbProtocolKeyring,
    previous_snapshot: Option<&PersonalDbSnapshotsHead>,
    manifest: &PersonalDbSnapshotManifest,
) -> Result<()> {
    let manifest_ref = personaldb_snapshot_manifest_ref_name(
        request.tenant_id,
        request.database_id,
        manifest.log_index,
        &manifest.state_hash,
    )?;
    let head = PersonalDbSnapshotsHead {
        format_version: 2,
        tenant_id: request.tenant_id.to_string(),
        database_id: request.database_id.to_string(),
        latest_snapshot_log_index: manifest.log_index,
        latest_snapshot_log_hash: manifest.log_hash.clone(),
        latest_snapshot_manifest_ref: manifest_ref,
        retained_snapshot_count: 1,
        updated_at: chrono::Utc::now().to_rfc3339(),
        updated_by_node: request.created_by_node.to_string(),
        head_hash: None,
        head_signature: None,
    }
    .seal(protocol_keyring)
    .await?;
    crate::personaldb_heads::write_personaldb_snapshots_head_mvcc(
        mvcc,
        request.tenant_id,
        request.database_id,
        previous_snapshot,
        &head,
        protocol_keyring.trust_store(),
        request.created_by_node,
    )
    .await
    .map(|_| ())
}

async fn read_canonical_records(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    database_id: &str,
    committed_head: &PersonalDbCommittedHead,
    snapshot_version: u64,
) -> Result<Vec<PersonalDbLogRecord>> {
    if committed_head.log_index == 0 {
        return Ok(Vec::new());
    }
    let tenant_id = committed_head
        .tenant_id
        .parse::<i64>()
        .context("personaldb committed head tenant id must be numeric")?;
    let segment_refs =
        list_personaldb_log_segment_refs(mvcc, tenant_id, database_id, snapshot_version).await?;
    let mut records = Vec::new();
    for segment_ref in segment_refs {
        let segment =
            read_personaldb_log_segment(storage, mvcc, &segment_ref, snapshot_version).await?;
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

async fn sum_changeset_payload_bytes(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    database_id: &str,
    records: &[PersonalDbLogRecord],
    snapshot_version: u64,
) -> Result<u64> {
    let mut total = 0u64;
    for record in records {
        let len = load_changeset_bytes(
            storage,
            mvcc,
            tenant_id,
            database_id,
            record,
            snapshot_version,
        )
        .await?
        .len();
        total = total
            .checked_add(len as u64)
            .ok_or_else(|| anyhow!("personaldb snapshot payload byte count overflow"))?;
    }
    Ok(total)
}

async fn load_changeset_bytes(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    database_id: &str,
    record: &PersonalDbLogRecord,
    snapshot_version: u64,
) -> Result<Vec<u8>> {
    if !record.payload_ref.is_empty() {
        let payload_ref = std::str::from_utf8(&record.payload_ref)?;
        return read_personaldb_changeset_payload_ref(
            storage,
            mvcc,
            payload_ref,
            record.changeset_payload_hash,
            snapshot_version,
        )
        .await?
        .ok_or_else(|| anyhow!("personaldb changeset payload is missing"));
    }
    read_personaldb_changeset_payload_by_index(
        storage,
        mvcc,
        tenant_id,
        database_id,
        record.log_index,
        record.changeset_payload_hash,
        snapshot_version,
    )
    .await?
    .ok_or_else(|| anyhow!("personaldb changeset payload is missing"))
}

fn ensure_head_matches_records(
    committed_head: &PersonalDbCommittedHead,
    records: &[PersonalDbLogRecord],
) -> Result<()> {
    let Some(last) = records.last() else {
        return Err(anyhow!(
            "personaldb committed head has no readable log records"
        ));
    };
    if last.log_index != committed_head.log_index
        || hex::encode(last.entry_hash) != committed_head.log_hash
    {
        return Err(anyhow!(
            "personaldb committed head does not match readable log chain"
        ));
    }
    Ok(())
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

fn validate_request(request: &PersonalDbSnapshotBuildRequest<'_>) -> Result<()> {
    if request.database_id.is_empty() {
        return Err(anyhow!("personaldb snapshot database id must not be empty"));
    }
    if request.schema_sql.trim().is_empty() {
        return Err(anyhow!("personaldb snapshot schema SQL must not be empty"));
    }
    if request.created_by_node.is_empty() {
        return Err(anyhow!("personaldb snapshot creator must not be empty"));
    }
    if request.policy.entry_threshold == 0 && request.policy.payload_bytes_threshold == 0 {
        return Err(anyhow!(
            "personaldb snapshot policy cannot disable both thresholds"
        ));
    }
    Ok(())
}
