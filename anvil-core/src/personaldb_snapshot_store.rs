use crate::{
    anvil_api::SignatureEnvelopeV1 as WireSignatureEnvelopeV1,
    core_store::{decode_deterministic_proto, encode_deterministic_proto},
    formats::{Hash32, hash32},
    personaldb_control::PersonalDbSnapshotManifest,
    personaldb_coremeta::{
        read_personaldb_data_locator_bytes, read_personaldb_data_locator_row,
        write_personaldb_bytes_as_data_locator,
    },
    personaldb_signing::{signature_envelope_from_proto, signature_envelope_to_proto},
    storage::Storage,
};
use anyhow::{Result, anyhow};
use personaldb_protocol::PublicKeyTrustStore;
use prost::Message;

const PERSONALDB_SNAPSHOT_OBJECT_REF_PREFIX: &str = "personaldb_snapshot_object:";
const PERSONALDB_SNAPSHOT_MANIFEST_REF_PREFIX: &str = "personaldb_snapshot_manifest:";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbSnapshotWriteResult {
    pub object_ref: String,
    pub manifest_ref: String,
}

#[derive(Clone, PartialEq, Message)]
struct PersonalDbSnapshotManifestProto {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(string, tag = "2")]
    tenant_id: String,
    #[prost(string, tag = "3")]
    database_id: String,
    #[prost(uint64, tag = "4")]
    log_index: u64,
    #[prost(string, tag = "5")]
    log_hash: String,
    #[prost(string, tag = "6")]
    state_hash: String,
    #[prost(string, tag = "7")]
    schema_hash: String,
    #[prost(string, tag = "8")]
    snapshot_object_key: String,
    #[prost(string, tag = "9")]
    snapshot_object_hash: String,
    #[prost(uint64, tag = "10")]
    source_segment_start: u64,
    #[prost(uint64, tag = "11")]
    source_segment_end: u64,
    #[prost(uint64, tag = "12")]
    row_index_generation: u64,
    #[prost(string, tag = "13")]
    created_at: String,
    #[prost(string, tag = "14")]
    created_by_node: String,
    #[prost(string, optional, tag = "15")]
    manifest_hash: Option<String>,
    #[prost(message, optional, tag = "16")]
    manifest_signature: Option<WireSignatureEnvelopeV1>,
}

pub async fn write_personaldb_snapshot(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    compressed_sqlite_bytes: &[u8],
    manifest: &PersonalDbSnapshotManifest,
    trust_store: &PublicKeyTrustStore,
) -> Result<PersonalDbSnapshotWriteResult> {
    manifest.verify(trust_store)?;
    ensure_manifest_scope(tenant_id, database_id, manifest)?;
    let state_hash = decode_hex32(&manifest.state_hash, "state_hash")?;
    let snapshot_object_hash =
        decode_hex32(&manifest.snapshot_object_hash, "snapshot_object_hash")?;
    if hash32(compressed_sqlite_bytes) != snapshot_object_hash {
        return Err(anyhow!("personaldb snapshot object hash mismatch"));
    }

    let object_ref = personaldb_snapshot_object_ref_name(
        tenant_id,
        database_id,
        manifest.log_index,
        &manifest.state_hash,
    )?;
    if manifest.snapshot_object_key != object_ref {
        return Err(anyhow!(
            "personaldb snapshot object key does not match CoreStore object identity"
        ));
    }

    write_personaldb_bytes_as_data_locator(
        storage,
        tenant_id,
        database_id,
        &object_ref,
        "snapshot_object",
        manifest.log_index,
        compressed_sqlite_bytes.to_vec(),
        manifest.snapshot_object_hash.clone(),
        vec![format!("state_hash:{}", manifest.state_hash)],
        format!(
            "personaldb-snapshot-object:{tenant_id}:{database_id}:{}",
            manifest.log_index
        ),
    )
    .await?;
    let manifest_ref = personaldb_snapshot_manifest_ref_name(
        tenant_id,
        database_id,
        manifest.log_index,
        &hex::encode(state_hash),
    )?;
    let manifest_bytes = encode_snapshot_manifest(manifest)?;
    write_personaldb_bytes_as_data_locator(
        storage,
        tenant_id,
        database_id,
        &manifest_ref,
        "snapshot_manifest",
        manifest.log_index,
        manifest_bytes,
        manifest
            .manifest_hash
            .clone()
            .unwrap_or_else(|| hex::encode(hash32(manifest_ref.as_bytes()))),
        vec![format!("state_hash:{}", manifest.state_hash)],
        format!(
            "personaldb-snapshot-manifest:{tenant_id}:{database_id}:{}",
            manifest.log_index
        ),
    )
    .await?;
    Ok(PersonalDbSnapshotWriteResult {
        object_ref,
        manifest_ref,
    })
}

pub async fn read_personaldb_snapshot_manifest(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    log_index: u64,
    state_hash: &str,
    trust_store: &PublicKeyTrustStore,
) -> Result<Option<PersonalDbSnapshotManifest>> {
    let manifest_ref =
        personaldb_snapshot_manifest_ref_name(tenant_id, database_id, log_index, state_hash)?;
    let Some(manifest) =
        read_personaldb_snapshot_manifest_by_ref(storage, &manifest_ref, trust_store).await?
    else {
        return Ok(None);
    };
    ensure_manifest_scope(tenant_id, database_id, &manifest)?;
    if manifest.log_index != log_index || manifest.state_hash != state_hash {
        return Err(anyhow!("personaldb snapshot manifest ref scope mismatch"));
    }
    Ok(Some(manifest))
}

pub async fn read_personaldb_snapshot_manifest_by_ref(
    storage: &Storage,
    manifest_ref: &str,
    trust_store: &PublicKeyTrustStore,
) -> Result<Option<PersonalDbSnapshotManifest>> {
    let (tenant_id, database_id) = personaldb_ref_scope(manifest_ref)?;
    let Some(row) =
        read_personaldb_data_locator_row(storage, tenant_id, &database_id, manifest_ref).await?
    else {
        return Ok(None);
    };
    if row.data_kind != "snapshot_manifest" {
        return Err(anyhow!(
            "personaldb snapshot manifest locator has wrong data kind"
        ));
    }
    let bytes = read_personaldb_data_locator_bytes(storage, &row).await?;
    let manifest = decode_snapshot_manifest(&bytes)?;
    manifest.verify(trust_store)?;
    Ok(Some(manifest))
}

pub async fn read_personaldb_snapshot_object(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    manifest: &PersonalDbSnapshotManifest,
    trust_store: &PublicKeyTrustStore,
) -> Result<Option<Vec<u8>>> {
    manifest.verify(trust_store)?;
    ensure_manifest_scope(tenant_id, database_id, manifest)?;
    let expected_object_ref = personaldb_snapshot_object_ref_name(
        tenant_id,
        database_id,
        manifest.log_index,
        &manifest.state_hash,
    )?;
    if manifest.snapshot_object_key != expected_object_ref {
        return Err(anyhow!(
            "personaldb snapshot object key does not match CoreStore object identity"
        ));
    }
    let Some(row) = read_personaldb_data_locator_row(
        storage,
        tenant_id,
        database_id,
        &manifest.snapshot_object_key,
    )
    .await?
    else {
        return Ok(None);
    };
    if row.data_kind != "snapshot_object" {
        return Err(anyhow!(
            "personaldb snapshot object locator has wrong data kind"
        ));
    }
    let bytes = read_personaldb_data_locator_bytes(storage, &row).await?;
    if hash32(&bytes) != decode_hex32(&manifest.snapshot_object_hash, "snapshot_object_hash")? {
        return Err(anyhow!("personaldb snapshot object hash mismatch"));
    }
    Ok(Some(bytes))
}

fn encode_snapshot_manifest(manifest: &PersonalDbSnapshotManifest) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(&snapshot_manifest_to_proto(
        manifest,
    )))
}

fn decode_snapshot_manifest(bytes: &[u8]) -> Result<PersonalDbSnapshotManifest> {
    snapshot_manifest_from_proto(
        decode_deterministic_proto::<PersonalDbSnapshotManifestProto>(
            bytes,
            "personaldb snapshot manifest",
        )?,
    )
}

fn snapshot_manifest_to_proto(
    manifest: &PersonalDbSnapshotManifest,
) -> PersonalDbSnapshotManifestProto {
    PersonalDbSnapshotManifestProto {
        format_version: u32::from(manifest.format_version),
        tenant_id: manifest.tenant_id.clone(),
        database_id: manifest.database_id.clone(),
        log_index: manifest.log_index,
        log_hash: manifest.log_hash.clone(),
        state_hash: manifest.state_hash.clone(),
        schema_hash: manifest.schema_hash.clone(),
        snapshot_object_key: manifest.snapshot_object_key.clone(),
        snapshot_object_hash: manifest.snapshot_object_hash.clone(),
        source_segment_start: manifest.source_segment_start,
        source_segment_end: manifest.source_segment_end,
        row_index_generation: manifest.row_index_generation,
        created_at: manifest.created_at.clone(),
        created_by_node: manifest.created_by_node.clone(),
        manifest_hash: manifest.manifest_hash.clone(),
        manifest_signature: manifest
            .manifest_signature
            .as_ref()
            .map(signature_envelope_to_proto),
    }
}

fn snapshot_manifest_from_proto(
    proto: PersonalDbSnapshotManifestProto,
) -> Result<PersonalDbSnapshotManifest> {
    Ok(PersonalDbSnapshotManifest {
        format_version: u16::try_from(proto.format_version)
            .map_err(|_| anyhow!("personaldb snapshot manifest version exceeds u16"))?,
        tenant_id: proto.tenant_id,
        database_id: proto.database_id,
        log_index: proto.log_index,
        log_hash: proto.log_hash,
        state_hash: proto.state_hash,
        schema_hash: proto.schema_hash,
        snapshot_object_key: proto.snapshot_object_key,
        snapshot_object_hash: proto.snapshot_object_hash,
        source_segment_start: proto.source_segment_start,
        source_segment_end: proto.source_segment_end,
        row_index_generation: proto.row_index_generation,
        created_at: proto.created_at,
        created_by_node: proto.created_by_node,
        manifest_hash: proto.manifest_hash,
        manifest_signature: proto
            .manifest_signature
            .map(signature_envelope_from_proto)
            .transpose()?,
    })
}

fn ensure_manifest_scope(
    expected_tenant_id: i64,
    expected_database_id: &str,
    manifest: &PersonalDbSnapshotManifest,
) -> Result<()> {
    if manifest.tenant_id != expected_tenant_id.to_string() {
        return Err(anyhow!("personaldb snapshot tenant scope mismatch"));
    }
    if manifest.database_id != expected_database_id {
        return Err(anyhow!("personaldb snapshot database scope mismatch"));
    }
    Ok(())
}

fn decode_hex32(value: &str, field: &'static str) -> Result<Hash32> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(hex::decode(value)?
        .try_into()
        .map_err(|_| anyhow!("{field} must be hex32"))?)
}

pub fn personaldb_snapshot_object_ref_name(
    tenant_id: i64,
    database_id: &str,
    log_index: u64,
    state_hash: &str,
) -> Result<String> {
    validate_scope_component(tenant_id, database_id)?;
    decode_hex32(state_hash, "state_hash")?;
    Ok(format!(
        "{PERSONALDB_SNAPSHOT_OBJECT_REF_PREFIX}tenant:{tenant_id}:database:{database_id}:log:{log_index:020}:state:{state_hash}"
    ))
}

pub fn personaldb_snapshot_manifest_ref_name(
    tenant_id: i64,
    database_id: &str,
    log_index: u64,
    state_hash: &str,
) -> Result<String> {
    validate_scope_component(tenant_id, database_id)?;
    decode_hex32(state_hash, "state_hash")?;
    Ok(format!(
        "{PERSONALDB_SNAPSHOT_MANIFEST_REF_PREFIX}tenant:{tenant_id}:database:{database_id}:log:{log_index:020}:state:{state_hash}"
    ))
}

fn validate_scope_component(tenant_id: i64, database_id: &str) -> Result<()> {
    if tenant_id < 0 {
        return Err(anyhow!("personaldb snapshot tenant id must be nonnegative"));
    }
    if database_id.is_empty()
        || database_id == "."
        || database_id == ".."
        || database_id.contains('/')
        || database_id.contains('\\')
        || database_id.contains(':')
        || database_id.chars().any(char::is_control)
    {
        return Err(anyhow!("database_id is not a safe component"));
    }
    Ok(())
}

fn personaldb_ref_scope(ref_name: &str) -> Result<(i64, String)> {
    if ![
        PERSONALDB_SNAPSHOT_OBJECT_REF_PREFIX,
        PERSONALDB_SNAPSHOT_MANIFEST_REF_PREFIX,
    ]
    .iter()
    .any(|prefix| ref_name.starts_with(prefix))
    {
        return Err(anyhow!(
            "personaldb snapshot data id has unsupported ref prefix"
        ));
    }
    if ref_name.contains('/') || ref_name.contains('\\') || ref_name.chars().any(char::is_control) {
        return Err(anyhow!(
            "personaldb snapshot data id must not be a storage path"
        ));
    }
    let tenant_marker = "tenant:";
    let database_marker = ":database:";
    let tenant_start = ref_name
        .find(tenant_marker)
        .ok_or_else(|| anyhow!("personaldb snapshot data id is missing tenant"))?
        + tenant_marker.len();
    let database_marker_offset = ref_name[tenant_start..]
        .find(database_marker)
        .ok_or_else(|| anyhow!("personaldb snapshot data id is missing database"))?
        + tenant_start;
    let tenant_id = ref_name[tenant_start..database_marker_offset]
        .parse::<i64>()
        .map_err(|_| anyhow!("personaldb snapshot data id tenant is invalid"))?;
    let database_start = database_marker_offset + database_marker.len();
    let database_end = ref_name[database_start..]
        .find(':')
        .map(|offset| database_start + offset)
        .unwrap_or(ref_name.len());
    let database_id = ref_name[database_start..database_end].to_string();
    validate_scope_component(tenant_id, &database_id)?;
    Ok((tenant_id, database_id))
}

#[cfg(test)]
fn encode_core_object_ref_target(object_ref: &crate::core_store::CoreObjectRef) -> Result<String> {
    crate::core_store::encode_core_object_ref_target(object_ref)
}

#[cfg(test)]
fn decode_core_object_ref_target(target: &str) -> Result<crate::core_store::CoreObjectRef> {
    crate::core_store::decode_core_object_ref_target(target)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::personaldb_protocol_keyring;
    use tempfile::tempdir;

    #[tokio::test]
    async fn snapshot_object_and_manifest_round_trip_through_corestore_refs() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bytes = b"zstd sqlite snapshot bytes";
        let keyring = personaldb_protocol_keyring();
        let manifest = sample_manifest(&storage, bytes)
            .seal(&keyring)
            .await
            .unwrap();

        let result = write_personaldb_snapshot(
            &storage,
            6,
            "db-alpha",
            bytes,
            &manifest,
            keyring.trust_store(),
        )
        .await
        .unwrap();
        assert_eq!(result.object_ref, manifest.snapshot_object_key);
        assert!(
            result
                .object_ref
                .starts_with("personaldb_snapshot_object:tenant:6:database:db-alpha:")
        );
        assert!(
            result
                .manifest_ref
                .starts_with("personaldb_snapshot_manifest:tenant:6:database:db-alpha:")
        );

        let read_manifest = read_personaldb_snapshot_manifest(
            &storage,
            6,
            "db-alpha",
            1024,
            &manifest.state_hash,
            keyring.trust_store(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(read_manifest, manifest);
        assert_eq!(
            read_manifest
                .manifest_signature
                .unwrap()
                .signature
                .as_bytes()
                .len(),
            64
        );
        let read_bytes = read_personaldb_snapshot_object(
            &storage,
            6,
            "db-alpha",
            &manifest,
            keyring.trust_store(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(read_bytes, bytes);
    }

    #[tokio::test]
    async fn snapshot_write_rejects_hash_scope_and_path_mismatch() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bytes = b"zstd sqlite snapshot bytes";
        let keyring = personaldb_protocol_keyring();
        let manifest = sample_manifest(&storage, bytes)
            .seal(&keyring)
            .await
            .unwrap();
        assert!(
            write_personaldb_snapshot(
                &storage,
                6,
                "db-alpha",
                b"wrong",
                &manifest,
                keyring.trust_store(),
            )
            .await
            .is_err()
        );

        let wrong_scope = PersonalDbSnapshotManifest {
            database_id: "db-beta".to_string(),
            ..sample_manifest(&storage, bytes)
        }
        .seal(&keyring)
        .await
        .unwrap();
        assert!(
            write_personaldb_snapshot(
                &storage,
                6,
                "db-alpha",
                bytes,
                &wrong_scope,
                keyring.trust_store(),
            )
            .await
            .is_err()
        );

        let wrong_path = PersonalDbSnapshotManifest {
            snapshot_object_key: "personaldb_snapshot_object:wrong".to_string(),
            ..sample_manifest(&storage, bytes)
        }
        .seal(&keyring)
        .await
        .unwrap();
        assert!(
            write_personaldb_snapshot(
                &storage,
                6,
                "db-alpha",
                bytes,
                &wrong_path,
                keyring.trust_store(),
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn snapshot_reader_rejects_tampered_object() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bytes = b"zstd sqlite snapshot bytes";
        let keyring = personaldb_protocol_keyring();
        let manifest = sample_manifest(&storage, bytes)
            .seal(&keyring)
            .await
            .unwrap();
        write_personaldb_snapshot(
            &storage,
            6,
            "db-alpha",
            bytes,
            &manifest,
            keyring.trust_store(),
        )
        .await
        .unwrap();
        write_personaldb_bytes_as_data_locator(
            &storage,
            6,
            "db-alpha",
            &manifest.snapshot_object_key,
            "snapshot_object",
            manifest.log_index + 1,
            b"corrupt".to_vec(),
            hex::encode(hash32(b"corrupt")),
            vec![format!("state_hash:{}", manifest.state_hash)],
            "corrupt-snapshot".to_string(),
        )
        .await
        .unwrap();
        assert!(
            read_personaldb_snapshot_object(
                &storage,
                6,
                "db-alpha",
                &manifest,
                keyring.trust_store(),
            )
            .await
            .is_err()
        );
    }

    fn sample_manifest(_storage: &Storage, bytes: &[u8]) -> PersonalDbSnapshotManifest {
        let state_hash = hex::encode([1; 32]);
        PersonalDbSnapshotManifest {
            format_version: 1,
            tenant_id: "6".to_string(),
            database_id: "db-alpha".to_string(),
            log_index: 1024,
            log_hash: hex::encode([2; 32]),
            state_hash: state_hash.clone(),
            schema_hash: hex::encode([3; 32]),
            snapshot_object_key: personaldb_snapshot_object_ref_name(
                6,
                "db-alpha",
                1024,
                &state_hash,
            )
            .unwrap(),
            snapshot_object_hash: hex::encode(hash32(bytes)),
            source_segment_start: 1,
            source_segment_end: 1024,
            row_index_generation: 4,
            created_at: "2026-06-27T00:00:00.000000000Z".to_string(),
            created_by_node: "node-a".to_string(),
            manifest_hash: None,
            manifest_signature: None,
        }
    }
}
