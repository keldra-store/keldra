use super::*;
use crate::object_links;
use crate::persistence::{Bucket, Object, ObjectVersion, ObjectVersionsPage};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Utc};
use prost::Message;
use serde_json::Value as JsonValue;

#[path = "local_object_metadata/projections.rs"]
mod projections;
use projections::*;
pub(crate) use projections::{
    CurrentObjectMetadataPage, ObjectMetadataPageCursor, ObjectVersionsMetadataPage,
};

#[path = "local_object_metadata/mutation.rs"]
mod mutation;
pub(crate) use mutation::{
    ObjectMetadataMutationGuard, ObjectMetadataPreconditionSnapshot,
    ObjectMetadataProjectionMutation, PreparedObjectMetadataProjection,
};

const CORE_OBJECT_METADATA_SCHEMA: &str = "anvil.core.object_metadata.v1";

#[derive(Clone, PartialEq, Message)]
struct ObjectMetadataRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(int64, tag = "3")]
    id: i64,
    #[prost(int64, tag = "4")]
    tenant_id: i64,
    #[prost(int64, tag = "5")]
    bucket_id: i64,
    #[prost(string, tag = "6")]
    key: String,
    #[prost(string, tag = "7")]
    kind: String,
    #[prost(string, tag = "8")]
    content_hash: String,
    #[prost(int64, tag = "9")]
    size: i64,
    #[prost(string, tag = "10")]
    etag: String,
    #[prost(string, tag = "11")]
    content_type: String,
    #[prost(bool, tag = "12")]
    has_content_type: bool,
    #[prost(string, tag = "13")]
    version_id: String,
    #[prost(string, tag = "14")]
    mutation_id: String,
    #[prost(string, tag = "15")]
    index_policy_snapshot: String,
    #[prost(string, tag = "16")]
    user_metadata_hash: String,
    #[prost(int64, tag = "17")]
    authz_revision: i64,
    #[prost(string, tag = "18")]
    record_hash: String,
    #[prost(string, tag = "19")]
    created_at: String,
    #[prost(string, tag = "20")]
    deleted_at: String,
    #[prost(bool, tag = "21")]
    has_deleted_at: bool,
    #[prost(string, tag = "22")]
    storage_class: String,
    #[prost(bool, tag = "23")]
    has_storage_class: bool,
    #[prost(bytes = "vec", tag = "24")]
    user_meta_json: Vec<u8>,
    #[prost(bool, tag = "25")]
    has_user_meta: bool,
    #[prost(bytes = "vec", tag = "26")]
    shard_map_target: Vec<u8>,
    #[prost(bool, tag = "27")]
    has_shard_map: bool,
    #[prost(bytes = "vec", tag = "28")]
    checksum: Vec<u8>,
    #[prost(bool, tag = "29")]
    has_checksum: bool,
    #[prost(message, optional, tag = "30")]
    link: Option<ObjectLinkTargetProto>,
    #[prost(string, tag = "31")]
    shard_map_kind: String,
    #[prost(bool, tag = "32")]
    delete_marker: bool,
}

#[derive(Clone, PartialEq, Message)]
struct ObjectMetadataCounterProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(int64, tag = "3")]
    max_id: i64,
}

#[derive(Clone, PartialEq, Message)]
struct ObjectLinkTargetProto {
    #[prost(string, tag = "1")]
    target_key: String,
    #[prost(string, tag = "2")]
    target_version: String,
    #[prost(bool, tag = "3")]
    has_target_version: bool,
    #[prost(string, tag = "4")]
    resolution: String,
    #[prost(uint64, tag = "5")]
    generation: u64,
    #[prost(string, tag = "6")]
    created_at: String,
    #[prost(string, tag = "7")]
    created_by: String,
}

fn object_metadata_common_at_generation(
    object: &Object,
    root_generation: u64,
    transaction_id: impl Into<String>,
) -> CoreMetaRowCommonProto {
    core_meta_committed_row_common(
        object_metadata_realm_id(object.tenant_id),
        object_metadata_root_key_hash(object.tenant_id, object.bucket_id),
        root_generation,
        transaction_id,
        object.created_at.timestamp_nanos_opt().unwrap_or_default() as u64,
    )
}

fn validate_object_metadata_common(
    common: &CoreMetaRowCommonProto,
    tenant_id: i64,
    bucket_id: i64,
    _mutation_id: &str,
) -> Result<()> {
    if common.realm_id != object_metadata_realm_id(tenant_id) {
        bail!("CoreStore object metadata row realm mismatch");
    }
    if common.root_key_hash != object_metadata_root_key_hash(tenant_id, bucket_id) {
        bail!("CoreStore object metadata row root hash mismatch");
    }
    if common.transaction_id.is_empty() {
        bail!("CoreStore object metadata row transaction id is empty");
    }
    if common.visibility_state_enum() != CoreMetaVisibilityState::Committed {
        bail!("CoreStore object metadata row is not committed");
    }
    Ok(())
}

fn object_metadata_realm_id(tenant_id: i64) -> String {
    format!("tenant/{tenant_id}")
}

fn object_metadata_root_key_hash(tenant_id: i64, bucket_id: i64) -> String {
    core_meta_root_key_hash(&object_metadata_root_anchor_key(tenant_id, bucket_id))
}

fn object_metadata_root_anchor_key(tenant_id: i64, bucket_id: i64) -> String {
    let mut bytes = Vec::with_capacity(16);
    bytes.extend_from_slice(&tenant_id.to_le_bytes());
    bytes.extend_from_slice(&bucket_id.to_le_bytes());
    hex::encode(crate::formats::hash32(&bytes))
}

fn validate_object_scope(bucket: &Bucket, object: &Object) -> Result<()> {
    if object.tenant_id != bucket.tenant_id || object.bucket_id != bucket.id {
        bail!("CoreStore object metadata row scope mismatch");
    }
    Ok(())
}

fn encode_object_metadata_row_at_generation(
    object: &Object,
    root_generation: u64,
) -> Result<Vec<u8>> {
    encode_object_metadata_row_at_generation_for_transaction(
        object,
        root_generation,
        &object.mutation_id.to_string(),
    )
}

fn encode_object_metadata_row_at_generation_for_transaction(
    object: &Object,
    root_generation: u64,
    transaction_id: &str,
) -> Result<Vec<u8>> {
    encode_object_metadata_row_at_generation_with_delete_marker_for_transaction(
        object,
        root_generation,
        object.deleted_at.is_some(),
        transaction_id,
    )
}

fn encode_object_metadata_row_at_generation_with_delete_marker(
    object: &Object,
    root_generation: u64,
    delete_marker: bool,
) -> Result<Vec<u8>> {
    encode_object_metadata_row_at_generation_with_delete_marker_for_transaction(
        object,
        root_generation,
        delete_marker,
        &object.mutation_id.to_string(),
    )
}

fn encode_object_metadata_row_at_generation_with_delete_marker_for_transaction(
    object: &Object,
    root_generation: u64,
    delete_marker: bool,
    transaction_id: &str,
) -> Result<Vec<u8>> {
    let proto = ObjectMetadataRowProto {
        common: Some(object_metadata_common_at_generation(
            object,
            root_generation,
            transaction_id,
        )),
        schema: CORE_OBJECT_METADATA_SCHEMA.to_string(),
        id: object.id,
        tenant_id: object.tenant_id,
        bucket_id: object.bucket_id,
        key: object.key.clone(),
        kind: match object.kind {
            object_links::ObjectEntryKind::Blob => "blob".to_string(),
            object_links::ObjectEntryKind::Link => "link".to_string(),
        },
        content_hash: object.content_hash.clone(),
        size: object.size,
        etag: object.etag.clone(),
        content_type: object.content_type.clone().unwrap_or_default(),
        has_content_type: object.content_type.is_some(),
        version_id: object.version_id.to_string(),
        mutation_id: object.mutation_id.to_string(),
        index_policy_snapshot: object.index_policy_snapshot.clone(),
        user_metadata_hash: object.user_metadata_hash.clone(),
        authz_revision: object.authz_revision,
        record_hash: object.record_hash.clone(),
        created_at: object.created_at.to_rfc3339(),
        deleted_at: object
            .deleted_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_default(),
        has_deleted_at: object.deleted_at.is_some(),
        storage_class: object.storage_class.clone().unwrap_or_default(),
        has_storage_class: object.storage_class.is_some(),
        user_meta_json: optional_json_bytes(object.user_meta.as_ref())?.unwrap_or_default(),
        has_user_meta: object.user_meta.is_some(),
        shard_map_target: optional_object_data_target_bytes(object.shard_map.as_ref())?
            .map(|target| target.1)
            .unwrap_or_default(),
        has_shard_map: object.shard_map.is_some(),
        checksum: object.checksum.clone().unwrap_or_default(),
        has_checksum: object.checksum.is_some(),
        link: object.link.as_ref().map(link_to_proto),
        shard_map_kind: optional_object_data_target_bytes(object.shard_map.as_ref())?
            .map(|target| target.0)
            .unwrap_or_default(),
        delete_marker,
    };
    encode_deterministic(&proto)
}

#[derive(Debug, Clone)]
struct DecodedObjectMetadataRow {
    object: Object,
    root_generation: u64,
    delete_marker: bool,
}

fn decode_object_metadata_row(bytes: &[u8]) -> Result<Object> {
    Ok(decode_object_metadata_row_with_common(bytes)?.object)
}

fn decode_object_metadata_row_with_common(bytes: &[u8]) -> Result<DecodedObjectMetadataRow> {
    let proto = ObjectMetadataRowProto::decode(bytes)?;
    ensure_round_trips(&proto, bytes, "object metadata row")?;
    if proto.schema != CORE_OBJECT_METADATA_SCHEMA {
        bail!("CoreStore object metadata row has invalid schema");
    }
    let common = proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("CoreStore object metadata row missing CoreMeta common"))?;
    validate_object_metadata_common(common, proto.tenant_id, proto.bucket_id, &proto.mutation_id)?;
    let root_generation = common.root_generation;
    let object = Object {
        id: proto.id,
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        key: proto.key,
        kind: match proto.kind.as_str() {
            "blob" => object_links::ObjectEntryKind::Blob,
            "link" => object_links::ObjectEntryKind::Link,
            _ => bail!("CoreStore object metadata row has invalid object kind"),
        },
        content_hash: proto.content_hash,
        size: proto.size,
        etag: proto.etag,
        content_type: proto.has_content_type.then_some(proto.content_type),
        version_id: uuid::Uuid::parse_str(&proto.version_id)
            .context("CoreStore object metadata row version_id is invalid")?,
        mutation_id: uuid::Uuid::parse_str(&proto.mutation_id)
            .context("CoreStore object metadata row mutation_id is invalid")?,
        index_policy_snapshot: proto.index_policy_snapshot,
        user_metadata_hash: proto.user_metadata_hash,
        authz_revision: proto.authz_revision,
        record_hash: proto.record_hash,
        created_at: parse_datetime(&proto.created_at, "created_at")?,
        deleted_at: if proto.has_deleted_at {
            Some(parse_datetime(&proto.deleted_at, "deleted_at")?)
        } else {
            None
        },
        storage_class: if proto.has_storage_class {
            Some(proto.storage_class)
        } else {
            None
        },
        user_meta: if proto.has_user_meta {
            Some(decode_canonical_json_bytes(
                &proto.user_meta_json,
                "CoreStore object metadata user_meta",
            )?)
        } else {
            None
        },
        shard_map: if proto.has_shard_map {
            Some(shard_map_from_object_data_target(
                &proto.shard_map_kind,
                &proto.shard_map_target,
            )?)
        } else {
            None
        },
        checksum: proto.has_checksum.then_some(proto.checksum),
        link: proto.link.map(link_from_proto).transpose()?,
    };
    Ok(DecodedObjectMetadataRow {
        object,
        root_generation,
        delete_marker: proto.delete_marker,
    })
}

fn encode_object_metadata_counter_at_generation(
    bucket: &Bucket,
    max_id: i64,
    root_generation: u64,
    transaction_id: &str,
) -> Result<Vec<u8>> {
    encode_deterministic(&ObjectMetadataCounterProto {
        common: Some(core_meta_committed_row_common(
            object_metadata_realm_id(bucket.tenant_id),
            object_metadata_root_key_hash(bucket.tenant_id, bucket.id),
            root_generation,
            transaction_id,
            unix_timestamp_nanos(),
        )),
        schema: "anvil.core.object_metadata_counter.v1".to_string(),
        max_id,
    })
}

fn decode_object_metadata_counter(bytes: &[u8]) -> Result<ObjectMetadataCounterProto> {
    let proto = ObjectMetadataCounterProto::decode(bytes)?;
    ensure_round_trips(&proto, bytes, "object metadata counter")?;
    if proto.schema != "anvil.core.object_metadata_counter.v1" {
        bail!("CoreStore object metadata counter row has invalid schema");
    }
    proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("CoreStore object metadata counter row missing CoreMeta common"))?;
    Ok(proto)
}

fn decode_object_metadata_counter_for_bucket(
    bytes: &[u8],
    bucket: &Bucket,
) -> Result<ObjectMetadataCounterProto> {
    let proto = decode_object_metadata_counter(bytes)?;
    let common = proto
        .common
        .as_ref()
        .expect("counter decoder requires CoreMeta common");
    validate_object_metadata_common(common, bucket.tenant_id, bucket.id, "")?;
    if proto.max_id < 0 {
        bail!("CoreStore object metadata counter max id must be non-negative");
    }
    Ok(proto)
}

fn link_to_proto(link: &object_links::ObjectLinkTarget) -> ObjectLinkTargetProto {
    ObjectLinkTargetProto {
        target_key: link.target_key.clone(),
        target_version: link
            .target_version
            .map(|value| value.to_string())
            .unwrap_or_default(),
        has_target_version: link.target_version.is_some(),
        resolution: match link.resolution {
            object_links::ObjectLinkResolution::Follow => "follow".to_string(),
            object_links::ObjectLinkResolution::Redirect => "redirect".to_string(),
        },
        generation: link.generation,
        created_at: link.created_at.to_rfc3339(),
        created_by: link.created_by.clone(),
    }
}

fn link_from_proto(proto: ObjectLinkTargetProto) -> Result<object_links::ObjectLinkTarget> {
    Ok(object_links::ObjectLinkTarget {
        target_key: proto.target_key,
        target_version: if proto.has_target_version {
            Some(
                uuid::Uuid::parse_str(&proto.target_version)
                    .context("CoreStore object link target version is invalid")?,
            )
        } else {
            None
        },
        resolution: match proto.resolution.as_str() {
            "follow" => object_links::ObjectLinkResolution::Follow,
            "redirect" => object_links::ObjectLinkResolution::Redirect,
            _ => bail!("CoreStore object link resolution is invalid"),
        },
        generation: proto.generation,
        created_at: parse_datetime(&proto.created_at, "link.created_at")?,
        created_by: proto.created_by,
    })
}

fn parse_datetime(value: &str, field: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("CoreStore object metadata row {field} is invalid"))?
        .with_timezone(&Utc))
}

fn optional_json_bytes(value: Option<&JsonValue>) -> Result<Option<Vec<u8>>> {
    value
        .map(|value| serde_json::to_vec(&canonical_json(value)))
        .transpose()
        .map_err(Into::into)
}

fn decode_canonical_json_bytes(bytes: &[u8], label: &str) -> Result<JsonValue> {
    let value: JsonValue = serde_json::from_slice(bytes)?;
    if serde_json::to_vec(&canonical_json(&value))? != bytes {
        bail!("{label} is not canonical JSON");
    }
    Ok(value)
}

enum ObjectDataTarget {
    LogicalFile {
        locator: CoreManifestLocator,
        target: String,
        bytes: Vec<u8>,
    },
    ObjectRef {
        object_ref: CoreObjectRef,
        target: String,
        bytes: Vec<u8>,
    },
}

impl ObjectDataTarget {
    fn kind(&self) -> &'static str {
        match self {
            Self::LogicalFile { .. } => "logical_file",
            Self::ObjectRef { .. } => "object_ref",
        }
    }

    fn bytes(&self) -> &[u8] {
        match self {
            Self::LogicalFile { bytes, .. } => bytes,
            Self::ObjectRef { bytes, .. } => bytes,
        }
    }

    fn target_string(&self) -> &str {
        match self {
            Self::LogicalFile { target, .. } => target,
            Self::ObjectRef { target, .. } => target,
        }
    }
}

fn optional_object_data_target_bytes(
    value: Option<&JsonValue>,
) -> Result<Option<(String, Vec<u8>)>> {
    value
        .map(|value| {
            object_data_target_from_json(value)
                .map(|target| (target.kind().to_string(), target.bytes().to_vec()))
        })
        .transpose()
}

fn object_data_target_from_shard_map(
    value: Option<&JsonValue>,
) -> Result<Option<ObjectDataTarget>> {
    value.map(object_data_target_from_json).transpose()
}

fn object_data_target_from_json(value: &JsonValue) -> Result<ObjectDataTarget> {
    if value.get("schema").and_then(JsonValue::as_str) != Some("anvil.core.object_data_target.v1") {
        bail!("CoreStore object metadata shard map is not canonical object data target");
    }
    let kind = value
        .get("kind")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| anyhow!("CoreStore object metadata shard map kind is missing"))?;
    let target = value
        .get("target")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| anyhow!("CoreStore object metadata shard map target is missing"))?;
    match kind {
        "logical_file" => {
            let bytes = URL_SAFE_NO_PAD
                .decode(target)
                .context("CoreStore object metadata logical-file target is not base64url")?;
            let locator = decode_manifest_locator_proto(&bytes)?;
            Ok(ObjectDataTarget::LogicalFile {
                locator,
                target: target.to_string(),
                bytes,
            })
        }
        "object_ref" => {
            let object_ref = decode_core_object_ref_target(target)?;
            Ok(ObjectDataTarget::ObjectRef {
                object_ref,
                target: target.to_string(),
                bytes: target.as_bytes().to_vec(),
            })
        }
        other => {
            bail!("CoreStore object metadata logical-file shard map kind {other:?} is unsupported")
        }
    }
}

fn shard_map_from_object_data_target(kind: &str, target: &[u8]) -> Result<JsonValue> {
    match kind {
        "logical_file" => {
            decode_manifest_locator_proto(target)?;
            Ok(serde_json::json!({
                "schema": "anvil.core.object_data_target.v1",
                "kind": "logical_file",
                "target": URL_SAFE_NO_PAD.encode(target),
            }))
        }
        "object_ref" => {
            let target = std::str::from_utf8(target)
                .context("CoreStore object metadata object-ref target is not UTF-8")?;
            decode_core_object_ref_target(target)?;
            Ok(serde_json::json!({
                "schema": "anvil.core.object_data_target.v1",
                "kind": "object_ref",
                "target": target,
            }))
        }
        other => {
            bail!("CoreStore object metadata logical-file shard map kind {other:?} is unsupported")
        }
    }
}

fn canonical_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Array(values) => JsonValue::Array(values.iter().map(canonical_json).collect()),
        JsonValue::Object(values) => {
            let mut sorted = serde_json::Map::new();
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                sorted.insert(key.clone(), canonical_json(&values[key]));
            }
            JsonValue::Object(sorted)
        }
        scalar => scalar.clone(),
    }
}

fn object_metadata_bucket_lock_id(bucket: &Bucket) -> String {
    format!("{}:{}", bucket.tenant_id, bucket.id)
}

fn encode_deterministic<M: Message>(message: &M) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    message.encode(&mut bytes)?;
    Ok(bytes)
}

fn ensure_round_trips<M: Message>(message: &M, bytes: &[u8], label: &str) -> Result<()> {
    let mut canonical = Vec::new();
    message.encode(&mut canonical)?;
    if canonical != bytes {
        bail!("CoreStore {label} protobuf is not deterministic canonical encoding");
    }
    Ok(())
}
