use crate::{
    core_store::{
        CF_REGISTRY, CoreMetaTuplePart, TABLE_GIT_SOURCE_MANIFEST_ROW,
        core_meta_committed_row_common, core_meta_root_key_hash, core_meta_tuple_key,
        decode_deterministic_proto, encode_deterministic_proto,
    },
    formats::unix_nanos_from_rfc3339,
};
use anyhow::{Result, anyhow};
use prost::Message;
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Message)]
struct GitSourceRepositoryManifestProto {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(string, tag = "3")]
    repository_id: String,
    #[prost(string, tag = "4")]
    bucket_name: String,
    #[prost(string, tag = "5")]
    object_key: String,
    #[prost(string, tag = "6")]
    pack_object_version_id: String,
    #[prost(string, tag = "7")]
    source_hash: String,
    #[prost(uint64, tag = "8")]
    generation: u64,
    #[prost(uint64, tag = "9")]
    record_count: u64,
    #[prost(string, tag = "10")]
    index_path: String,
    #[prost(string, tag = "11")]
    updated_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct GitSourceRepositoryManifestRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(bytes, tag = "3")]
    manifest_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitSourceRepositoryManifest {
    pub format_version: u32,
    pub tenant_id: i64,
    pub repository_id: String,
    pub bucket_name: String,
    pub object_key: String,
    pub pack_object_version_id: String,
    pub source_hash: String,
    pub generation: u64,
    pub record_count: u64,
    pub index_path: String,
    pub updated_at: String,
}

pub async fn write_git_source_repository_manifest(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    manifest: &GitSourceRepositoryManifest,
) -> Result<()> {
    validate_manifest(manifest)?;
    let payload = encode_git_source_manifest_row(manifest)?;
    let tuple_key = manifest_tuple_key(manifest.tenant_id, &manifest.repository_id)?;
    let mutation_id = format!(
        "git-source-manifest:{}:{}:{}",
        manifest.tenant_id, manifest.repository_id, manifest.generation
    );
    let key = crate::mvcc_product::coremeta_logical_key(
        CF_REGISTRY,
        TABLE_GIT_SOURCE_MANIFEST_ROW,
        &tuple_key,
    )?;
    let current = mvcc.read_latest_value(&key)?;
    if let Some(existing) = current.as_deref() {
        let existing = decode_git_source_manifest_row(existing)?;
        if existing.generation > manifest.generation {
            return Err(anyhow!(
                "git source manifest generation cannot move backwards"
            ));
        }
        if existing.generation == manifest.generation && existing != *manifest {
            return Err(anyhow!(
                "git source manifest diverges at an existing generation"
            ));
        }
    }
    mvcc.autocommit_product_mutations_with_predicates(
        "git-source-manifest",
        &mutation_id,
        vec![crate::mvcc_product::ProductMutation::put(
            key.clone(),
            payload,
        )],
        vec![(
            key,
            match current {
                Some(payload) => crate::mvcc_transaction::PredicateKind::ValueHash(
                    *blake3::hash(&payload).as_bytes(),
                ),
                None => crate::mvcc_transaction::PredicateKind::Absent,
            },
        )],
        crate::mvcc_transaction::DurabilityLevel::Quorum,
        u64::try_from(chrono::Utc::now().timestamp_millis())
            .map_err(|_| anyhow!("git manifest timestamp predates Unix epoch"))?,
    )
    .await?;
    Ok(())
}

pub async fn read_git_source_repository_manifest(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    repository_id: &str,
) -> Result<Option<GitSourceRepositoryManifest>> {
    let key = crate::mvcc_product::coremeta_logical_key(
        CF_REGISTRY,
        TABLE_GIT_SOURCE_MANIFEST_ROW,
        &manifest_tuple_key(tenant_id, repository_id)?,
    )?;
    let Some(payload) = mvcc.read_latest_value(&key)? else {
        return Ok(None);
    };
    let manifest = decode_git_source_manifest_row(&payload)?;
    validate_manifest(&manifest)?;
    if manifest.tenant_id != tenant_id || manifest.repository_id != repository_id {
        return Err(anyhow!("git source manifest path scope mismatch"));
    }
    Ok(Some(manifest))
}

fn encode_git_source_manifest(manifest: &GitSourceRepositoryManifest) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(
        &GitSourceRepositoryManifestProto {
            format_version: manifest.format_version,
            tenant_id: manifest.tenant_id,
            repository_id: manifest.repository_id.clone(),
            bucket_name: manifest.bucket_name.clone(),
            object_key: manifest.object_key.clone(),
            pack_object_version_id: manifest.pack_object_version_id.clone(),
            source_hash: manifest.source_hash.clone(),
            generation: manifest.generation,
            record_count: manifest.record_count,
            index_path: manifest.index_path.clone(),
            updated_at: manifest.updated_at.clone(),
        },
    ))
}

fn encode_git_source_manifest_row(manifest: &GitSourceRepositoryManifest) -> Result<Vec<u8>> {
    validate_manifest(manifest)?;
    Ok(encode_deterministic_proto(
        &GitSourceRepositoryManifestRowProto {
            common: Some(core_meta_committed_row_common(
                format!("tenant/{}", manifest.tenant_id),
                core_meta_root_key_hash(&format!(
                    "git-source-manifest/{}/{}",
                    manifest.tenant_id, manifest.repository_id
                )),
                manifest.generation,
                format!(
                    "git-source-manifest:{}:{}:{}",
                    manifest.tenant_id, manifest.repository_id, manifest.generation
                ),
                unix_nanos_from_rfc3339(&manifest.updated_at),
            )),
            schema: "anvil.coremeta.git_source_manifest.v1".to_string(),
            manifest_bytes: encode_git_source_manifest(manifest)?,
        },
    ))
}

fn decode_git_source_manifest(bytes: &[u8]) -> Result<GitSourceRepositoryManifest> {
    let proto = decode_deterministic_proto::<GitSourceRepositoryManifestProto>(
        bytes,
        "git source repository manifest",
    )?;
    Ok(GitSourceRepositoryManifest {
        format_version: proto.format_version,
        tenant_id: proto.tenant_id,
        repository_id: proto.repository_id,
        bucket_name: proto.bucket_name,
        object_key: proto.object_key,
        pack_object_version_id: proto.pack_object_version_id,
        source_hash: proto.source_hash,
        generation: proto.generation,
        record_count: proto.record_count,
        index_path: proto.index_path,
        updated_at: proto.updated_at,
    })
}

fn decode_git_source_manifest_row(bytes: &[u8]) -> Result<GitSourceRepositoryManifest> {
    let row = decode_deterministic_proto::<GitSourceRepositoryManifestRowProto>(
        bytes,
        "git source repository manifest row",
    )?;
    if row.schema != "anvil.coremeta.git_source_manifest.v1" {
        return Err(anyhow!("git source manifest row has invalid schema"));
    }
    row.common
        .as_ref()
        .ok_or_else(|| anyhow!("git source manifest row missing CoreMeta common"))?;
    decode_git_source_manifest(&row.manifest_bytes)
}

fn validate_manifest(manifest: &GitSourceRepositoryManifest) -> Result<()> {
    if manifest.format_version != 1 {
        return Err(anyhow!("unsupported git source manifest version"));
    }
    if manifest.tenant_id <= 0 {
        return Err(anyhow!("git source manifest tenant id must be positive"));
    }
    require_nonempty(&manifest.repository_id, "repository_id")?;
    require_nonempty(&manifest.bucket_name, "bucket_name")?;
    require_nonempty(&manifest.object_key, "object_key")?;
    require_nonempty(&manifest.pack_object_version_id, "pack_object_version_id")?;
    require_nonempty(&manifest.source_hash, "source_hash")?;
    require_nonempty(&manifest.index_path, "index_path")?;
    if manifest.generation == 0 {
        return Err(anyhow!("git source manifest generation must be positive"));
    }
    if !manifest.index_path.starts_with("git_source_index:") {
        return Err(anyhow!(
            "git source manifest index path must be a CoreStore git source index ref"
        ));
    }
    Ok(())
}

fn manifest_tuple_key(tenant_id: i64, repository_id: &str) -> Result<Vec<u8>> {
    if tenant_id <= 0 {
        return Err(anyhow!("git source manifest tenant id must be positive"));
    }
    require_safe_component(repository_id, "repository_id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("git-source-manifest"),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(repository_id),
    ])
}

fn require_safe_component(value: &str, field: &'static str) -> Result<()> {
    require_nonempty(value, field)?;
    if value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.contains(':')
        || value.chars().any(char::is_control)
    {
        return Err(anyhow!(
            "git source manifest {field} is not a safe component"
        ));
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(anyhow!("git source manifest {field} must not be empty"));
    }
    Ok(())
}
