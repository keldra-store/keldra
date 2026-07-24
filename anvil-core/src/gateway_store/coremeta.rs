use super::{
    GATEWAY_METADATA_CANDIDATE_GENERATION, GATEWAY_METADATA_CANDIDATE_TRANSACTION_ID,
    GatewayBlobRecord, GatewayTagRecord,
};
use crate::{
    core_store::{
        CF_REGISTRY, CORE_LOGICAL_FILE_LOCATOR_REF_PREFIX, CoreMetaBatchOp, CoreMetaBatchOpKind,
        CoreMetaLocatorProto, CoreMetaTuplePart, CoreStore, TABLE_REGISTRY_BLOB_LOCATOR_ROW,
        TABLE_REGISTRY_VERSION_ROW, core_meta_committed_row_common,
        core_meta_locator_from_manifest_locator, core_meta_locator_to_manifest_locator,
        core_meta_root_key_hash, core_meta_tuple_key, decode_deterministic_proto,
        decode_manifest_locator_proto, encode_deterministic_proto,
    },
    formats::hash32,
    storage::Storage,
};
use anyhow::{Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use prost::Message;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RegistryBlobLocatorCoreMetaRow {
    pub tenant_id: i64,
    pub registry_kind: String,
    pub namespace: String,
    pub blob_hash: String,
    pub blob_length: u64,
    pub blob_locator: crate::core_store::CoreManifestLocator,
    pub media_type: String,
    pub refcount_key: String,
    pub created_at_unix_nanos: u64,
}

#[derive(Clone, PartialEq, Message)]
struct RegistryVersionRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    registry_kind: String,
    #[prost(string, tag = "3")]
    namespace: String,
    #[prost(string, tag = "4")]
    package_name: String,
    #[prost(string, tag = "5")]
    version: String,
    #[prost(string, tag = "6")]
    manifest_hash: String,
    #[prost(message, optional, tag = "7")]
    manifest_locator: Option<CoreMetaLocatorProto>,
    #[prost(string, tag = "8")]
    published_by_principal: String,
    #[prost(uint64, tag = "9")]
    tag_generation: u64,
}

#[derive(Clone, PartialEq, Message)]
struct RegistryBlobLocatorRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    registry_kind: String,
    #[prost(string, tag = "3")]
    blob_hash: String,
    #[prost(uint64, tag = "4")]
    blob_length: u64,
    #[prost(message, optional, tag = "5")]
    blob_locator: Option<CoreMetaLocatorProto>,
    #[prost(string, tag = "6")]
    media_type: String,
    #[prost(string, tag = "7")]
    refcount_key: String,
    #[prost(string, tag = "8")]
    namespace: String,
}

pub(super) async fn write_registry_blob_locator_row(
    storage: &Storage,
    record: &GatewayBlobRecord,
    locator: &crate::core_store::CoreManifestLocator,
) -> Result<()> {
    let row = RegistryBlobLocatorRowProto {
        common: Some(core_meta_committed_row_common(
            registry_realm_id(record.tenant_id),
            registry_root_key_hash(record.tenant_id, &record.gateway, &record.repository),
            GATEWAY_METADATA_CANDIDATE_GENERATION,
            GATEWAY_METADATA_CANDIDATE_TRANSACTION_ID,
            current_unix_nanos()?,
        )),
        registry_kind: record.gateway.clone(),
        blob_hash: record.digest.clone(),
        blob_length: record.size_bytes,
        blob_locator: Some(core_meta_locator_from_manifest_locator(locator)?),
        media_type: record.media_type.clone(),
        refcount_key: registry_blob_refcount_key(record),
        namespace: record.registry_instance_id.clone(),
    };
    let tuple_key = registry_blob_tuple_key(
        record.tenant_id,
        &record.gateway,
        &record.registry_instance_id,
        &record.digest,
    )?;
    let payload = encode_deterministic_proto(&row);
    let store = CoreStore::new(storage.clone()).await?;
    let op = CoreMetaBatchOp {
        cf: CF_REGISTRY,
        table_id: TABLE_REGISTRY_BLOB_LOCATOR_ROW,
        tuple_key: &tuple_key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(&payload),
    };
    store
        .commit_coremeta_root_groups(
            &format!("registry-blob:{}:{}", record.gateway, record.digest),
            &[op],
            &[crate::core_store::CoreMetaRootPublication::new(
                format!(
                    "registry/{}/{}/{}",
                    record.tenant_id, record.gateway, record.repository
                ),
                crate::formats::writer::WriterFamily::Registry,
            )],
        )
        .await?;
    Ok(())
}

pub(super) async fn write_registry_blob_locator_row_from_record(
    storage: &Storage,
    record: &GatewayBlobRecord,
) -> Result<()> {
    let locator = manifest_locator_from_object_ref(&record.object_ref)?;
    write_registry_blob_locator_row(storage, record, &locator).await
}

pub(super) async fn read_registry_blob_locator_row(
    storage: &Storage,
    tenant_id: i64,
    registry_kind: &str,
    namespace: &str,
    blob_hash: &str,
) -> Result<Option<RegistryBlobLocatorCoreMetaRow>> {
    let Some(bytes) = CoreStore::new(storage.clone()).await?.read_coremeta_row(
        CF_REGISTRY,
        TABLE_REGISTRY_BLOB_LOCATOR_ROW,
        &registry_blob_tuple_key(tenant_id, registry_kind, namespace, blob_hash)?,
    )?
    else {
        return Ok(None);
    };
    let proto = decode_deterministic_proto::<RegistryBlobLocatorRowProto>(
        &bytes,
        "registry blob locator row",
    )?;
    let common = proto
        .common
        .ok_or_else(|| anyhow!("registry blob locator row missing CoreMeta common"))?;
    let blob_locator = proto
        .blob_locator
        .as_ref()
        .ok_or_else(|| anyhow!("registry blob locator row missing locator"))
        .and_then(core_meta_locator_to_manifest_locator)?;
    if proto.registry_kind != registry_kind {
        bail!("registry blob locator kind mismatch");
    }
    if proto.namespace != namespace {
        bail!("registry blob locator namespace mismatch");
    }
    Ok(Some(RegistryBlobLocatorCoreMetaRow {
        tenant_id,
        registry_kind: proto.registry_kind,
        namespace: proto.namespace,
        blob_hash: proto.blob_hash,
        blob_length: proto.blob_length,
        blob_locator,
        media_type: proto.media_type,
        refcount_key: proto.refcount_key,
        created_at_unix_nanos: common.created_at_unix_nanos,
    }))
}

pub(super) async fn write_registry_version_row_for_tag(
    storage: &Storage,
    tag: &GatewayTagRecord,
    blob: &RegistryBlobLocatorCoreMetaRow,
    tag_generation: u64,
) -> Result<()> {
    if tag_generation == 0 {
        bail!("registry tag logical generation must be nonzero");
    }
    if tag.tenant_id != blob.tenant_id
        || tag.gateway != blob.registry_kind
        || tag.registry_instance_id != blob.namespace
        || tag.target_digest != blob.blob_hash
    {
        bail!("registry tag does not match blob locator row");
    }
    let row = RegistryVersionRowProto {
        common: Some(core_meta_committed_row_common(
            registry_realm_id(tag.tenant_id),
            registry_root_key_hash(tag.tenant_id, &tag.gateway, &tag.repository),
            GATEWAY_METADATA_CANDIDATE_GENERATION,
            GATEWAY_METADATA_CANDIDATE_TRANSACTION_ID,
            current_unix_nanos()?,
        )),
        registry_kind: tag.gateway.clone(),
        namespace: tag.registry_instance_id.clone(),
        package_name: tag.repository.clone(),
        version: tag.tag.clone(),
        manifest_hash: tag.target_digest.clone(),
        manifest_locator: Some(core_meta_locator_from_manifest_locator(&blob.blob_locator)?),
        published_by_principal: tag.updated_by_principal.clone(),
        tag_generation,
    };
    let tuple_key = registry_version_tuple_key(
        tag.tenant_id,
        &tag.gateway,
        &tag.registry_instance_id,
        &tag.repository,
        &tag.tag,
    )?;
    let payload = encode_deterministic_proto(&row);
    let store = CoreStore::new(storage.clone()).await?;
    let op = CoreMetaBatchOp {
        cf: CF_REGISTRY,
        table_id: TABLE_REGISTRY_VERSION_ROW,
        tuple_key: &tuple_key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(&payload),
    };
    store
        .commit_coremeta_root_groups(
            &format!("registry-version:{}", hex::encode(hash32(&payload))),
            &[op],
            &[crate::core_store::CoreMetaRootPublication::new(
                format!(
                    "registry/{}/{}/{}",
                    tag.tenant_id, tag.gateway, tag.repository
                ),
                crate::formats::writer::WriterFamily::Registry,
            )],
        )
        .await?;
    Ok(())
}

fn registry_blob_tuple_key(
    tenant_id: i64,
    registry_kind: &str,
    namespace: &str,
    blob_hash: &str,
) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(&registry_realm_id(tenant_id)),
        CoreMetaTuplePart::Utf8(registry_kind),
        CoreMetaTuplePart::Utf8(namespace),
        CoreMetaTuplePart::Hash(blob_hash),
    ])
}

fn registry_version_tuple_key(
    tenant_id: i64,
    registry_kind: &str,
    namespace: &str,
    package_name: &str,
    version: &str,
) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(&registry_realm_id(tenant_id)),
        CoreMetaTuplePart::Utf8(registry_kind),
        CoreMetaTuplePart::Utf8(namespace),
        CoreMetaTuplePart::Utf8(package_name),
        CoreMetaTuplePart::Utf8(version),
    ])
}

fn registry_realm_id(tenant_id: i64) -> String {
    format!("tenant:{tenant_id}")
}

fn registry_root_key_hash(tenant_id: i64, registry_kind: &str, package_name: &str) -> String {
    core_meta_root_key_hash(&format!(
        "registry/{tenant_id}/{registry_kind}/{package_name}"
    ))
}

fn registry_blob_refcount_key(record: &GatewayBlobRecord) -> String {
    format!(
        "gateway_blob:{}:{}:{}:{}:{}",
        record.tenant_id,
        record.gateway,
        record.registry_instance_id,
        record.repository,
        record.digest
    )
}

fn manifest_locator_from_object_ref(
    object_ref: &crate::core_store::CoreObjectRef,
) -> Result<crate::core_store::CoreManifestLocator> {
    let encoded = object_ref
        .manifest_ref
        .strip_prefix(CORE_LOGICAL_FILE_LOCATOR_REF_PREFIX)
        .ok_or_else(|| anyhow!("registry blob object ref does not carry a manifest locator"))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|err| anyhow!("registry blob manifest locator is invalid base64: {err}"))?;
    decode_manifest_locator_proto(&bytes)
}

fn current_unix_nanos() -> Result<u64> {
    let nanos = chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("current timestamp cannot be represented in nanoseconds"))?;
    u64::try_from(nanos).map_err(|_| anyhow!("current timestamp is negative"))
}
