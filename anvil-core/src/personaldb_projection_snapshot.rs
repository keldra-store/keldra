use crate::{
    mvcc_bootstrap::MvccSubsystem,
    mvcc_worker_authority::AssignmentGuard,
    personaldb_commit_store::read_personaldb_commit_certificate,
    personaldb_heads::{
        PersonalDbCommittedHead, read_personaldb_group_manifest,
        read_personaldb_snapshots_head_at_snapshot,
    },
    personaldb_projection::{
        ProjectionDefinition, read_projection_definition, sha256_projection_definition,
    },
    personaldb_proposal_admission::read_personaldb_committed_head_at_snapshot,
    personaldb_schema::read_personaldb_schema_sql,
    personaldb_signing::PersonalDbProtocolKeyring,
    personaldb_signing_object::PersonalDbSigningObject,
    personaldb_snapshot_builder::{
        PERSONALDB_SNAPSHOT_COMPRESSION_LEVEL, PersonalDbSnapshotBuildRequest,
        PersonalDbSnapshotPolicy, maybe_build_personaldb_snapshot,
    },
    personaldb_snapshot_store::{
        PersonalDbSnapshotObjectRange, read_personaldb_snapshot_manifest_by_ref,
        read_personaldb_snapshot_object_range,
    },
    storage::Storage,
};
use anyhow::{Context, Result, anyhow, bail};
use personaldb_protocol::{
    CommittedHeadV2, DatabaseGroupKind, DatabaseId, GroupDescriptor, Sha256Digest,
    SignedSnapshotTargetManifestV1, SnapshotCompressionV1, SnapshotTargetManifestV1, SourceHeadV1,
    StateCommitmentV1, UnsignedCommittedHeadV2, UnsignedGroupDescriptorV1,
};
use personaldb_protocol::{MAX_SYNC_CHUNK_BYTES, SignaturePurpose};
use std::sync::Arc;

pub const MAX_SNAPSHOT_PAGE_BYTES: u64 = 32 * 1024 * 1024;
pub const MAX_SNAPSHOT_COMPRESSED_BYTES: u64 = 16 * 1024 * 1024 * 1024;
pub const MAX_SNAPSHOT_UNCOMPRESSED_BYTES: u64 = 64 * 1024 * 1024 * 1024;
pub const MAX_SNAPSHOT_EXPANSION_RATIO: u64 = 64;
pub const SNAPSHOT_CHUNK_BYTES: u32 = MAX_SYNC_CHUNK_BYTES as u32;
const ZSTD_MAX_FRAME_HEADER_BYTES: u64 = 18;

#[derive(Debug, Clone)]
pub struct PreparedProjectionSnapshot {
    pub group_descriptor: GroupDescriptor,
    pub committed_head: CommittedHeadV2,
    pub signed_manifest: SignedSnapshotTargetManifestV1,
    pub source_manifest: crate::personaldb_control::PersonalDbSnapshotManifest,
    pub compressed_length: u64,
    pub trust_bundle_version: u64,
    pub snapshot_version: u64,
}

impl PreparedProjectionSnapshot {
    pub fn snapshot_id(&self) -> &str {
        &self.signed_manifest.manifest.snapshot_id
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn prepare_projection_snapshot(
    storage: &Storage,
    mvcc: &Arc<MvccSubsystem>,
    keyring: &PersonalDbProtocolKeyring,
    tenant_id: i64,
    database_id: &str,
    projection_id: &str,
    assignment: &AssignmentGuard,
) -> Result<PreparedProjectionSnapshot> {
    let trust_bundle_version = required_trust_bundle_version(keyring)?;
    ensure_current_snapshot(
        storage,
        mvcc,
        keyring,
        tenant_id,
        database_id,
        &assignment.owner.node_id,
    )
    .await?;

    let snapshot_version = mvcc.runtime.applied_version()?;
    let definition = read_projection_definition(
        storage,
        mvcc,
        tenant_id,
        database_id,
        projection_id,
        snapshot_version,
    )
    .await?
    .ok_or_else(|| anyhow!("PersonalDB projection definition is missing"))?;
    definition.verify()?;
    ensure_projection_scope(tenant_id, database_id, projection_id, &definition)?;

    let group_manifest = read_personaldb_group_manifest(
        storage,
        mvcc,
        tenant_id,
        database_id,
        keyring.trust_store(),
        snapshot_version,
    )
    .await?
    .ok_or_else(|| anyhow!("PersonalDB projection group manifest is missing"))?;
    let committed = read_personaldb_committed_head_at_snapshot(
        mvcc,
        tenant_id,
        database_id,
        keyring.trust_store(),
        snapshot_version,
    )?
    .ok_or_else(|| anyhow!("PersonalDB projection committed head is missing"))?;
    let snapshot_head = read_personaldb_snapshots_head_at_snapshot(
        mvcc,
        tenant_id,
        database_id,
        keyring.trust_store(),
        snapshot_version,
    )?
    .ok_or_else(|| anyhow!("PersonalDB projection snapshot head is missing"))?;
    if snapshot_head.latest_snapshot_log_index != committed.log_index
        || snapshot_head.latest_snapshot_log_hash != committed.log_hash
    {
        bail!("PersonalDB projection snapshot is not at the committed head");
    }
    let source_manifest = read_personaldb_snapshot_manifest_by_ref(
        storage,
        mvcc,
        &snapshot_head.latest_snapshot_manifest_ref,
        keyring.trust_store(),
        snapshot_version,
    )
    .await?
    .ok_or_else(|| anyhow!("PersonalDB projection snapshot manifest is missing"))?;
    validate_source_manifest(
        tenant_id,
        database_id,
        &committed,
        &group_manifest.schema_hash,
        &source_manifest,
    )?;

    let metadata_range = read_personaldb_snapshot_object_range(
        storage,
        mvcc,
        tenant_id,
        database_id,
        &source_manifest,
        keyring.trust_store(),
        snapshot_version,
        0,
        0,
    )
    .await?
    .ok_or_else(|| anyhow!("PersonalDB projection snapshot object is missing"))?;
    validate_snapshot_bounds(metadata_range.total_length)?;
    let header_end = ZSTD_MAX_FRAME_HEADER_BYTES.min(metadata_range.total_length);
    let header_range = read_personaldb_snapshot_object_range(
        storage,
        mvcc,
        tenant_id,
        database_id,
        &source_manifest,
        keyring.trust_store(),
        snapshot_version,
        0,
        header_end,
    )
    .await?
    .ok_or_else(|| anyhow!("PersonalDB projection snapshot object is missing"))?;
    if header_range.total_length != metadata_range.total_length {
        bail!("PersonalDB projection snapshot object changed while reading its header");
    }
    let uncompressed_length =
        validate_zstd_frame(&header_range.bytes, metadata_range.total_length)?;

    let certificate = read_personaldb_commit_certificate(
        storage,
        mvcc,
        tenant_id,
        database_id,
        committed.log_index,
        &committed.log_hash,
        keyring.trust_store(),
        snapshot_version,
    )
    .await?
    .ok_or_else(|| anyhow!("PersonalDB projection commit certificate is missing"))?;
    let certificate_hash = certificate
        .certificate_hash
        .as_deref()
        .ok_or_else(|| anyhow!("PersonalDB projection commit certificate is not sealed"))?;
    definition
        .definition_hash
        .as_deref()
        .ok_or_else(|| anyhow!("PersonalDB projection definition is not sealed"))?;
    let schema_sql = read_personaldb_schema_sql(
        storage,
        mvcc,
        tenant_id,
        database_id,
        &committed.schema_hash,
        snapshot_version,
    )
    .await?
    .ok_or_else(|| anyhow!("PersonalDB projection schema is missing"))?;
    let state_sha256 = &source_manifest.state_sha256;
    let snapshot_object_sha256 = &source_manifest.snapshot_object_sha256;
    let state = StateCommitmentV1 {
        database_id: DatabaseId::new(database_id),
        log_index: committed.log_index,
        log_hash: sha256_of_internal_digest(&committed.log_hash, "committed log hash")?,
        database_state_root: digest(state_sha256, "database state root")?,
        schema_hash: Sha256Digest::hash(schema_sql.as_bytes()),
        projection_definition_hash: Some(Sha256Digest::from_bytes(sha256_projection_definition(
            &definition,
        )?)),
        group_kind: DatabaseGroupKind::Projection,
    };
    let unsigned_head = UnsignedCommittedHeadV2 {
        state: state.clone(),
        commit_certificate_hash: sha256_of_internal_digest(
            certificate_hash,
            "commit certificate hash",
        )?,
        primary_server_id: assignment.owner.node_id.clone(),
        placement_epoch: assignment.assignment_epoch,
    };
    let head_signature = keyring
        .sign(PersonalDbSigningObject::CanonicalProtocol(
            unsigned_head.prepare_signature(projection_id)?,
        ))
        .await?;
    let committed_head = unsigned_head.attach_verified_signature(
        projection_id,
        head_signature,
        keyring.trust_store(),
    )?;

    let descriptor = UnsignedGroupDescriptorV1 {
        group_id: projection_id.to_string(),
        database_id: DatabaseId::new(database_id),
        group_kind: DatabaseGroupKind::Projection,
        schema_hash: state.schema_hash,
        projection_definition_hash: state.projection_definition_hash,
        committed_head: committed_head.clone(),
        trust_bundle_version,
    };
    let descriptor_signature = keyring
        .sign(PersonalDbSigningObject::CanonicalProtocol(
            descriptor.prepare_signature()?,
        ))
        .await?;
    let group_descriptor =
        descriptor.attach_verified_signature(descriptor_signature, keyring.trust_store())?;

    let compressed_sha256 = digest(snapshot_object_sha256, "compressed snapshot hash")?;
    let snapshot_id = format!("sha256-{}", hex::encode(compressed_sha256.as_bytes()));
    let target_manifest = SnapshotTargetManifestV1 {
        snapshot_id,
        group_id: projection_id.to_string(),
        committed_head: committed_head.clone(),
        schema_hash: state.schema_hash,
        projection_definition_hash: state.projection_definition_hash,
        ordered_source_heads: source_heads(
            storage,
            mvcc,
            keyring,
            tenant_id,
            &definition,
            snapshot_version,
        )
        .await?,
        compression: SnapshotCompressionV1::ZstdFrameV1,
        compression_level: PERSONALDB_SNAPSHOT_COMPRESSION_LEVEL,
        compression_checksum: true,
        compression_content_size: true,
        dictionary_id: None,
        chunk_size: SNAPSHOT_CHUNK_BYTES,
        compressed_length: metadata_range.total_length,
        compressed_sha256,
        uncompressed_length,
        uncompressed_sha256: digest(state_sha256, "uncompressed snapshot hash")?,
        object_id: source_manifest.snapshot_object_key.clone(),
    };
    let snapshot_signature = keyring
        .sign(PersonalDbSigningObject::CanonicalProtocol(
            target_manifest.prepare_signature()?,
        ))
        .await?;
    let signed_manifest =
        target_manifest.attach_verified_signature(snapshot_signature, keyring.trust_store())?;
    group_descriptor.verify(keyring.trust_store())?;
    signed_manifest.verify(keyring.trust_store())?;

    Ok(PreparedProjectionSnapshot {
        group_descriptor,
        committed_head,
        signed_manifest,
        source_manifest,
        compressed_length: metadata_range.total_length,
        trust_bundle_version,
        snapshot_version,
    })
}

#[allow(clippy::too_many_arguments)]
pub async fn read_projection_snapshot_range(
    storage: &Storage,
    mvcc: &Arc<MvccSubsystem>,
    keyring: &PersonalDbProtocolKeyring,
    tenant_id: i64,
    database_id: &str,
    prepared: &PreparedProjectionSnapshot,
    snapshot_version: u64,
    start: u64,
    end_exclusive: u64,
) -> Result<PersonalDbSnapshotObjectRange> {
    let range = read_personaldb_snapshot_object_range(
        storage,
        mvcc,
        tenant_id,
        database_id,
        &prepared.source_manifest,
        keyring.trust_store(),
        snapshot_version,
        start,
        end_exclusive,
    )
    .await?
    .ok_or_else(|| anyhow!("PersonalDB projection snapshot object disappeared"))?;
    if range.total_length != prepared.compressed_length {
        bail!("PersonalDB projection snapshot object changed during streaming");
    }
    Ok(range)
}

async fn ensure_current_snapshot(
    storage: &Storage,
    mvcc: &Arc<MvccSubsystem>,
    keyring: &PersonalDbProtocolKeyring,
    tenant_id: i64,
    database_id: &str,
    created_by_node: &str,
) -> Result<()> {
    let snapshot_version = mvcc.runtime.applied_version()?;
    let committed = read_personaldb_committed_head_at_snapshot(
        mvcc,
        tenant_id,
        database_id,
        keyring.trust_store(),
        snapshot_version,
    )?
    .ok_or_else(|| anyhow!("PersonalDB committed head is missing"))?;
    if committed.log_index == 0 {
        bail!("PersonalDB projection has no committed state to snapshot");
    }
    let snapshot_head = read_personaldb_snapshots_head_at_snapshot(
        mvcc,
        tenant_id,
        database_id,
        keyring.trust_store(),
        snapshot_version,
    )?;
    if snapshot_head.as_ref().is_some_and(|head| {
        head.latest_snapshot_log_index == committed.log_index
            && head.latest_snapshot_log_hash == committed.log_hash
    }) {
        return Ok(());
    }
    let schema_sql = read_personaldb_schema_sql(
        storage,
        mvcc,
        tenant_id,
        database_id,
        &committed.schema_hash,
        snapshot_version,
    )
    .await?
    .ok_or_else(|| anyhow!("PersonalDB projection schema is missing"))?;
    maybe_build_personaldb_snapshot(
        storage,
        mvcc,
        PersonalDbSnapshotBuildRequest {
            tenant_id,
            database_id,
            schema_sql: &schema_sql,
            created_by_node,
            policy: PersonalDbSnapshotPolicy {
                entry_threshold: 1,
                payload_bytes_threshold: u64::MAX,
            },
        },
        keyring,
    )
    .await?
    .ok_or_else(|| anyhow!("PersonalDB projection snapshot was not published"))?;
    Ok(())
}

fn required_trust_bundle_version(keyring: &PersonalDbProtocolKeyring) -> Result<u64> {
    [
        SignaturePurpose::GroupControl,
        SignaturePurpose::Witness,
        SignaturePurpose::Snapshot,
    ]
    .into_iter()
    .map(|purpose| {
        keyring
            .trust_record_for_purpose(purpose)
            .map(|record| record.key_generation.get())
    })
    .collect::<Result<Vec<_>>>()?
    .into_iter()
    .max()
    .ok_or_else(|| anyhow!("PersonalDB projection signing keys are not configured"))
}

fn ensure_projection_scope(
    tenant_id: i64,
    database_id: &str,
    projection_id: &str,
    definition: &ProjectionDefinition,
) -> Result<()> {
    if definition.tenant_id != tenant_id.to_string()
        || definition.database_id != database_id
        || definition.target_database_id != database_id
        || definition.projection_id != projection_id
    {
        bail!("PersonalDB projection definition scope mismatch");
    }
    Ok(())
}

fn validate_source_manifest(
    tenant_id: i64,
    database_id: &str,
    committed: &PersonalDbCommittedHead,
    schema_hash: &str,
    manifest: &crate::personaldb_control::PersonalDbSnapshotManifest,
) -> Result<()> {
    if manifest.tenant_id != tenant_id.to_string()
        || manifest.database_id != database_id
        || manifest.log_index != committed.log_index
        || manifest.log_hash != committed.log_hash
        || manifest.schema_hash != committed.schema_hash
        || manifest.schema_hash != schema_hash
    {
        bail!("PersonalDB projection snapshot manifest scope mismatch");
    }
    Ok(())
}

async fn source_heads(
    storage: &Storage,
    mvcc: &Arc<MvccSubsystem>,
    keyring: &PersonalDbProtocolKeyring,
    tenant_id: i64,
    definition: &ProjectionDefinition,
    snapshot_version: u64,
) -> Result<Vec<SourceHeadV1>> {
    let source_ids = canonical_source_ids(&definition.source_database_ids);
    let mut heads = Vec::with_capacity(source_ids.len());
    for source_id in source_ids {
        let source_manifest = read_personaldb_group_manifest(
            storage,
            mvcc,
            tenant_id,
            &source_id,
            keyring.trust_store(),
            snapshot_version,
        )
        .await?
        .ok_or_else(|| anyhow!("PersonalDB projection source manifest is missing"))?;
        if source_manifest.database_id != source_id {
            bail!("PersonalDB projection source manifest scope mismatch");
        }
        let head = read_personaldb_committed_head_at_snapshot(
            mvcc,
            tenant_id,
            &source_id,
            keyring.trust_store(),
            snapshot_version,
        )?
        .ok_or_else(|| anyhow!("PersonalDB projection source head is missing"))?;
        heads.push(SourceHeadV1 {
            database_id: DatabaseId::new(source_id),
            log_index: head.log_index,
            log_hash: sha256_of_internal_digest(&head.log_hash, "source log hash")?,
        });
    }
    Ok(heads)
}

fn canonical_source_ids(observations: &[String]) -> Vec<String> {
    let mut canonical = observations.to_vec();
    canonical.sort();
    canonical.dedup();
    canonical
}

fn validate_snapshot_bounds(compressed_length: u64) -> Result<()> {
    if compressed_length == 0 || compressed_length > MAX_SNAPSHOT_COMPRESSED_BYTES {
        bail!("PersonalDB projection snapshot compressed length is out of bounds");
    }
    Ok(())
}

fn validate_zstd_frame(prefix: &[u8], compressed_length: u64) -> Result<u64> {
    validate_snapshot_bounds(compressed_length)?;
    if prefix.len() < 5 || prefix[..4] != [0x28, 0xb5, 0x2f, 0xfd] {
        bail!("PersonalDB projection snapshot is not a canonical zstd frame");
    }
    if prefix[4] & 0x04 == 0 {
        bail!("PersonalDB projection snapshot zstd checksum is missing");
    }
    if zstd::zstd_safe::get_dict_id_from_frame(prefix).is_some() {
        bail!("PersonalDB projection snapshot uses an unsupported zstd dictionary");
    }
    let uncompressed_length = zstd::zstd_safe::get_frame_content_size(prefix)
        .map_err(|_| anyhow!("PersonalDB projection snapshot zstd header is invalid"))?
        .ok_or_else(|| anyhow!("PersonalDB projection snapshot content size is missing"))?;
    if uncompressed_length == 0
        || uncompressed_length > MAX_SNAPSHOT_UNCOMPRESSED_BYTES
        || uncompressed_length > compressed_length.saturating_mul(MAX_SNAPSHOT_EXPANSION_RATIO)
    {
        bail!("PersonalDB projection snapshot uncompressed length is out of bounds");
    }
    Ok(uncompressed_length)
}

fn digest(value: &str, field: &'static str) -> Result<Sha256Digest> {
    Sha256Digest::from_prefixed_hex(value).with_context(|| format!("invalid {field}"))
}

fn sha256_of_internal_digest(value: &str, field: &'static str) -> Result<Sha256Digest> {
    let internal = digest(value, field)?;
    Ok(Sha256Digest::hash(internal.as_bytes()))
}

pub fn delivered_digest(bytes: &[u8]) -> Sha256Digest {
    Sha256Digest::hash(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    #[test]
    fn zstd_contract_rejects_missing_checksum_and_accepts_canonical_frame() {
        let source = b"bounded projection snapshot";
        let missing_checksum = zstd::stream::encode_all(source.as_slice(), 9).unwrap();
        assert!(validate_zstd_frame(&missing_checksum, missing_checksum.len() as u64).is_err());

        let mut encoder = zstd::stream::Encoder::new(Vec::new(), 9).unwrap();
        encoder.include_checksum(true).unwrap();
        encoder.include_contentsize(true).unwrap();
        encoder.include_dictid(false).unwrap();
        encoder
            .set_pledged_src_size(Some(source.len() as u64))
            .unwrap();
        encoder.write_all(source).unwrap();
        let canonical = encoder.finish().unwrap();
        assert_eq!(
            validate_zstd_frame(&canonical, canonical.len() as u64).unwrap(),
            source.len() as u64
        );
    }

    #[test]
    fn snapshot_and_page_bounds_fail_closed() {
        assert!(validate_snapshot_bounds(0).is_err());
        assert!(validate_snapshot_bounds(MAX_SNAPSHOT_COMPRESSED_BYTES + 1).is_err());
        assert_eq!(SNAPSHOT_CHUNK_BYTES as usize, MAX_SYNC_CHUNK_BYTES);
        assert!(MAX_SNAPSHOT_PAGE_BYTES >= SNAPSHOT_CHUNK_BYTES as u64);
    }

    #[test]
    fn source_observation_order_and_duplicates_do_not_change_canonical_identity() {
        let first = vec!["source-b".into(), "source-a".into(), "source-b".into()];
        let second = vec!["source-a".into(), "source-b".into()];
        assert_eq!(canonical_source_ids(&first), canonical_source_ids(&second));
        assert_eq!(
            canonical_source_ids(&first),
            vec!["source-a".to_string(), "source-b".to_string()]
        );
    }
}
