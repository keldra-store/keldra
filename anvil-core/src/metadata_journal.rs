use crate::bucket_journal;
use crate::core_store::{
    CF_OBJECT_HEADS, CoreMetaRowCommonProto, CoreMetaTuplePart, CoreMutationBatch,
    CoreMutationOperation, CoreMutationPrecondition, CorePipelinePolicy, CoreStore,
    CoreTraceContext, CoreTransaction, CoreTransactionState, CoreTransactionUpdate, ReadStream,
    TABLE_OBJECT_METADATA_PARTITION_MANIFEST_ROW, WriteLogicalFileRequest,
    core_meta_committed_row_common, core_meta_payload_digest, core_meta_root_key_hash,
    core_meta_tuple_key, core_mutation_publication_attempt_id, decode_deterministic_proto,
    is_stream_head_mismatch,
};
use crate::formats::{
    FileFamily, Hash32, decode_writer_segment, encode_writer_segment_header, hash32,
    header_field_string, header_field_u64,
    segment::SegmentRecord,
    single_body_range_index, unix_nanos_from_rfc3339,
    writer::{
        WriterBuildOutput, WriterFamily, WriterSegmentBuildInput,
        build_writer_segment_logical_file, canonical_logical_file_id,
    },
};
use crate::object_links;
use crate::partition_fence::{PartitionWritePermit, partition_write_precondition};
use crate::persistence::{Bucket, Object, ObjectVersion, ObjectVersionsPage};
use crate::storage::Storage;
use crate::task_execution_guard::TaskExecutionGuard;
use crate::writer_segment_catalog::{
    WriterSegmentCatalogRecord, read_writer_segment_catalog_record,
    write_writer_segment_catalog_record,
};
use anyhow::{Context, Result, anyhow};
use hmac::{Hmac, Mac};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

const MANIFEST_SEGMENT_REF_PREFIX: &str = "coresegment:";
const METADATA_SEGMENT_REF_PREFIX: &str = "metadata_segment:";
const DIRECTORY_SEGMENT_REF_PREFIX: &str = "directory_segment:";
const METADATA_MANIFEST_REF_PREFIX: &str = "metadata_manifest:";
const OBJECT_METADATA_SEGMENT_CATALOG_FAMILY: &str = "object_metadata_segment";
const OBJECT_METADATA_PARTITION_MANIFEST_ROW_SCHEMA: &str =
    "anvil.coremeta.object_metadata_partition_manifest.v1";
const OBJECT_VERSION_RECORD_KIND: &str = "object_metadata.object_version";
const DELETE_MARKER_RECORD_KIND: &str = "object_metadata.delete_marker";
const OBJECT_METADATA_BODY_SCHEMA: &str = "anvil.object_metadata.body.v1";
const PARTITION_MANIFEST_SCHEMA: &str = "anvil.object_metadata.partition_manifest.v1";

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectJournalMutation {
    Put,
    Copy,
    DeleteMarker,
    DeleteVersion,
}

impl ObjectJournalMutation {
    fn from_event_name(value: &str) -> Result<Self> {
        match value {
            "put" => Ok(Self::Put),
            "copy" => Ok(Self::Copy),
            "delete_marker" => Ok(Self::DeleteMarker),
            "delete_version" => Ok(Self::DeleteVersion),
            other => Err(anyhow!("unknown object metadata mutation event {other}")),
        }
    }

    fn event_name(self) -> &'static str {
        match self {
            Self::Put => "put",
            Self::Copy => "copy",
            Self::DeleteMarker => "delete_marker",
            Self::DeleteVersion => "delete_version",
        }
    }

    fn object_record_kind(self) -> &'static str {
        match self {
            Self::Put | Self::Copy | Self::DeleteVersion => OBJECT_VERSION_RECORD_KIND,
            Self::DeleteMarker => DELETE_MARKER_RECORD_KIND,
        }
    }

    fn is_delete_marker(self) -> bool {
        matches!(self, Self::DeleteMarker)
    }

    fn watch_event_name(self) -> &'static str {
        match self {
            Self::Put => "put",
            Self::Copy => "copy",
            Self::DeleteMarker => "delete",
            Self::DeleteVersion => "delete_version",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObjectVersionBody {
    fence_token: u64,
    id: i64,
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: String,
    object_key: String,
    event: String,
    kind: object_links::ObjectEntryKind,
    version_id: String,
    mutation_id: String,
    content_hash: String,
    size: i64,
    etag: String,
    content_type: Option<String>,
    user_metadata_hash: String,
    authz_revision: i64,
    index_policy_snapshot: String,
    record_hash: String,
    storage_class: Option<String>,
    user_meta: Option<serde_json::Value>,
    shard_map: Option<serde_json::Value>,
    checksum: Option<Vec<u8>>,
    link: Option<object_links::ObjectLinkTarget>,
    delete_marker: bool,
    created_at: String,
    deleted_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DirectoryEntryBody {
    fence_token: u64,
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: String,
    object_key: String,
    event: String,
    kind: object_links::ObjectEntryKind,
    id: i64,
    version_id: String,
    mutation_id: String,
    content_hash: String,
    size: i64,
    etag: String,
    content_type: Option<String>,
    user_metadata_hash: String,
    authz_revision: i64,
    index_policy_snapshot: String,
    record_hash: String,
    storage_class: Option<String>,
    user_meta: Option<serde_json::Value>,
    shard_map: Option<serde_json::Value>,
    checksum: Option<Vec<u8>>,
    link: Option<object_links::ObjectLinkTarget>,
    delete_marker: bool,
    created_at: String,
    deleted_at: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct ObjectMetadataBodyProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(uint64, tag = "2")]
    fence_token: u64,
    #[prost(int64, tag = "3")]
    id: i64,
    #[prost(int64, tag = "4")]
    tenant_id: i64,
    #[prost(int64, tag = "5")]
    bucket_id: i64,
    #[prost(string, tag = "6")]
    bucket_name: String,
    #[prost(string, tag = "7")]
    object_key: String,
    #[prost(string, tag = "8")]
    event: String,
    #[prost(string, tag = "9")]
    kind: String,
    #[prost(string, tag = "10")]
    version_id: String,
    #[prost(string, tag = "11")]
    mutation_id: String,
    #[prost(string, tag = "12")]
    content_hash: String,
    #[prost(int64, tag = "13")]
    size: i64,
    #[prost(string, tag = "14")]
    etag: String,
    #[prost(string, optional, tag = "15")]
    content_type: Option<String>,
    #[prost(string, tag = "16")]
    user_metadata_hash: String,
    #[prost(int64, tag = "17")]
    authz_revision: i64,
    #[prost(string, tag = "18")]
    index_policy_snapshot: String,
    #[prost(string, tag = "19")]
    record_hash: String,
    #[prost(string, optional, tag = "20")]
    storage_class: Option<String>,
    #[prost(bytes = "vec", optional, tag = "21")]
    user_meta_json: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "22")]
    shard_map_target: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "23")]
    checksum: Option<Vec<u8>>,
    #[prost(message, optional, tag = "24")]
    link: Option<ObjectLinkTargetProto>,
    #[prost(bool, tag = "25")]
    delete_marker: bool,
    #[prost(string, tag = "26")]
    created_at: String,
    #[prost(string, optional, tag = "27")]
    deleted_at: Option<String>,
    #[prost(string, optional, tag = "28")]
    shard_map_kind: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct ObjectLinkTargetProto {
    #[prost(string, tag = "1")]
    target_key: String,
    #[prost(string, optional, tag = "2")]
    target_version: Option<String>,
    #[prost(string, tag = "3")]
    resolution: String,
    #[prost(uint64, tag = "4")]
    generation: u64,
    #[prost(string, tag = "5")]
    created_at: String,
    #[prost(string, tag = "6")]
    created_by: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObjectMetadataRecordKind {
    ObjectVersion,
    DeleteMarker,
}

impl ObjectMetadataRecordKind {
    fn from_str(value: &str) -> Result<Self> {
        match value {
            OBJECT_VERSION_RECORD_KIND => Ok(Self::ObjectVersion),
            DELETE_MARKER_RECORD_KIND => Ok(Self::DeleteMarker),
            other => Err(anyhow!("unknown object metadata record kind {other}")),
        }
    }

    fn is_object_version_like(self) -> bool {
        matches!(self, Self::ObjectVersion | Self::DeleteMarker)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObjectMetadataRecord {
    partition_sequence: u64,
    event_hash: String,
    record_kind: ObjectMetadataRecordKind,
    payload: Vec<u8>,
    body: ObjectVersionBody,
}

impl ObjectMetadataRecord {
    fn object_version_body(&self) -> Result<ObjectVersionBody> {
        if !self.record_kind.is_object_version_like() {
            return Err(anyhow!("object metadata record is not an object version"));
        }
        Ok(self.body.clone())
    }
}

fn encode_object_version_body(body: &ObjectVersionBody) -> Result<Vec<u8>> {
    encode_object_metadata_body_proto(body)
}

fn decode_object_version_body(bytes: &[u8]) -> Result<ObjectVersionBody> {
    decode_object_metadata_body_proto(bytes)
}

fn encode_directory_entry_body(body: &DirectoryEntryBody) -> Result<Vec<u8>> {
    encode_object_metadata_body_proto(&object_version_body_from_directory_entry(body))
}

fn decode_directory_entry_body(bytes: &[u8]) -> Result<DirectoryEntryBody> {
    Ok(directory_entry_from_object_version_body(
        &decode_object_metadata_body_proto(bytes)?,
    ))
}

fn encode_object_metadata_body_proto(body: &ObjectVersionBody) -> Result<Vec<u8>> {
    let proto = ObjectMetadataBodyProto {
        schema: OBJECT_METADATA_BODY_SCHEMA.to_string(),
        fence_token: body.fence_token,
        id: body.id,
        tenant_id: body.tenant_id,
        bucket_id: body.bucket_id,
        bucket_name: body.bucket_name.clone(),
        object_key: body.object_key.clone(),
        event: body.event.clone(),
        kind: object_entry_kind_name(body.kind).to_string(),
        version_id: body.version_id.clone(),
        mutation_id: body.mutation_id.clone(),
        content_hash: body.content_hash.clone(),
        size: body.size,
        etag: body.etag.clone(),
        content_type: body.content_type.clone(),
        user_metadata_hash: body.user_metadata_hash.clone(),
        authz_revision: body.authz_revision,
        index_policy_snapshot: body.index_policy_snapshot.clone(),
        record_hash: body.record_hash.clone(),
        storage_class: body.storage_class.clone(),
        user_meta_json: body
            .user_meta
            .as_ref()
            .map(canonical_json_bytes)
            .transpose()?,
        shard_map_target: body
            .shard_map
            .as_ref()
            .map(object_data_target_bytes)
            .transpose()?,
        shard_map_kind: body
            .shard_map
            .as_ref()
            .map(object_data_target_kind)
            .transpose()?,
        checksum: body.checksum.clone(),
        link: body.link.as_ref().map(link_target_to_proto),
        delete_marker: body.delete_marker,
        created_at: body.created_at.clone(),
        deleted_at: body.deleted_at.clone(),
    };
    encode_deterministic_proto(&proto)
}

fn decode_object_metadata_body_proto(bytes: &[u8]) -> Result<ObjectVersionBody> {
    let proto = ObjectMetadataBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "object metadata body")?;
    if proto.schema != OBJECT_METADATA_BODY_SCHEMA {
        return Err(anyhow!("object metadata body schema mismatch"));
    }
    Ok(ObjectVersionBody {
        fence_token: proto.fence_token,
        id: proto.id,
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        bucket_name: proto.bucket_name,
        object_key: proto.object_key,
        event: proto.event,
        kind: object_entry_kind_from_name(&proto.kind)?,
        version_id: proto.version_id,
        mutation_id: proto.mutation_id,
        content_hash: proto.content_hash,
        size: proto.size,
        etag: proto.etag,
        content_type: proto.content_type,
        user_metadata_hash: proto.user_metadata_hash,
        authz_revision: proto.authz_revision,
        index_policy_snapshot: proto.index_policy_snapshot,
        record_hash: proto.record_hash,
        storage_class: proto.storage_class,
        user_meta: proto
            .user_meta_json
            .as_deref()
            .map(|bytes| decode_canonical_json_bytes(bytes, "object metadata user_meta"))
            .transpose()?,
        shard_map: proto
            .shard_map_target
            .as_deref()
            .map(|target| {
                shard_map_from_object_data_target(
                    proto.shard_map_kind.as_deref().unwrap_or_default(),
                    target,
                )
            })
            .transpose()?,
        checksum: proto.checksum,
        link: proto.link.map(link_target_from_proto).transpose()?,
        delete_marker: proto.delete_marker,
        created_at: proto.created_at,
        deleted_at: proto.deleted_at,
    })
}

fn canonical_json_bytes(value: &serde_json::Value) -> Result<Vec<u8>> {
    serde_json::to_vec(&canonical_json(value)).map_err(Into::into)
}

fn decode_canonical_json_bytes(bytes: &[u8], label: &str) -> Result<serde_json::Value> {
    let value: serde_json::Value = serde_json::from_slice(bytes)?;
    if canonical_json_bytes(&value)? != bytes {
        return Err(anyhow!("{label} is not canonical JSON"));
    }
    Ok(value)
}

fn canonical_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(canonical_json).collect())
        }
        serde_json::Value::Object(values) => {
            let mut sorted = serde_json::Map::new();
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                sorted.insert(key.clone(), canonical_json(&values[key]));
            }
            serde_json::Value::Object(sorted)
        }
        scalar => scalar.clone(),
    }
}

fn encode_deterministic_proto(message: &impl Message) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes)?;
    Ok(bytes)
}

fn ensure_deterministic_proto(message: &impl Message, bytes: &[u8], label: &str) -> Result<()> {
    if encode_deterministic_proto(message)? != bytes {
        return Err(anyhow!("{label} is not deterministically encoded"));
    }
    Ok(())
}

fn object_version_body_from_directory_entry(body: &DirectoryEntryBody) -> ObjectVersionBody {
    ObjectVersionBody {
        fence_token: body.fence_token,
        id: body.id,
        tenant_id: body.tenant_id,
        bucket_id: body.bucket_id,
        bucket_name: body.bucket_name.clone(),
        object_key: body.object_key.clone(),
        event: body.event.clone(),
        kind: body.kind,
        version_id: body.version_id.clone(),
        mutation_id: body.mutation_id.clone(),
        content_hash: body.content_hash.clone(),
        size: body.size,
        etag: body.etag.clone(),
        content_type: body.content_type.clone(),
        user_metadata_hash: body.user_metadata_hash.clone(),
        authz_revision: body.authz_revision,
        index_policy_snapshot: body.index_policy_snapshot.clone(),
        record_hash: body.record_hash.clone(),
        storage_class: body.storage_class.clone(),
        user_meta: body.user_meta.clone(),
        shard_map: body.shard_map.clone(),
        checksum: body.checksum.clone(),
        link: body.link.clone(),
        delete_marker: body.delete_marker,
        created_at: body.created_at.clone(),
        deleted_at: body.deleted_at.clone(),
    }
}

fn object_entry_kind_name(kind: object_links::ObjectEntryKind) -> &'static str {
    match kind {
        object_links::ObjectEntryKind::Blob => "blob",
        object_links::ObjectEntryKind::Link => "link",
    }
}

fn object_entry_kind_from_name(value: &str) -> Result<object_links::ObjectEntryKind> {
    match value {
        "blob" => Ok(object_links::ObjectEntryKind::Blob),
        "link" => Ok(object_links::ObjectEntryKind::Link),
        other => Err(anyhow!("unknown object entry kind {other}")),
    }
}

fn link_resolution_name(resolution: object_links::ObjectLinkResolution) -> &'static str {
    match resolution {
        object_links::ObjectLinkResolution::Follow => "follow",
        object_links::ObjectLinkResolution::Redirect => "redirect",
    }
}

fn link_resolution_from_name(value: &str) -> Result<object_links::ObjectLinkResolution> {
    match value {
        "follow" => Ok(object_links::ObjectLinkResolution::Follow),
        "redirect" => Ok(object_links::ObjectLinkResolution::Redirect),
        other => Err(anyhow!("unknown object link resolution {other}")),
    }
}

fn link_target_to_proto(value: &object_links::ObjectLinkTarget) -> ObjectLinkTargetProto {
    ObjectLinkTargetProto {
        target_key: value.target_key.clone(),
        target_version: value.target_version.map(|version| version.to_string()),
        resolution: link_resolution_name(value.resolution).to_string(),
        generation: value.generation,
        created_at: value
            .created_at
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        created_by: value.created_by.clone(),
    }
}

fn link_target_from_proto(value: ObjectLinkTargetProto) -> Result<object_links::ObjectLinkTarget> {
    Ok(object_links::ObjectLinkTarget {
        target_key: value.target_key,
        target_version: value
            .target_version
            .as_deref()
            .map(uuid::Uuid::parse_str)
            .transpose()?,
        resolution: link_resolution_from_name(&value.resolution)?,
        generation: value.generation,
        created_at: chrono::DateTime::parse_from_rfc3339(&value.created_at)?
            .with_timezone(&chrono::Utc),
        created_by: value.created_by,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedObjectMetadataSegments {
    pub generation: u64,
    pub metadata_ref: String,
    pub directory_ref: String,
    pub metadata_record_count: usize,
    pub directory_record_count: usize,
    pub manifest_ref: String,
    pub manifest_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredObjectMetadataPartition {
    pub manifest: PartitionManifest,
    pub metadata_records: Vec<SegmentRecord>,
    pub directory_records: Vec<SegmentRecord>,
}

#[derive(Debug, Clone)]
pub struct NativeObjectListing {
    pub objects: Vec<Object>,
    pub common_prefixes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryIndexSnapshot {
    pub entry_count: usize,
    pub snapshot_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryIndexComparison {
    pub source_cursor: u128,
    pub expected: DirectoryIndexSnapshot,
    pub actual: DirectoryIndexSnapshot,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ActiveObjectJournalStats {
    pub uncompacted_frame_count: u64,
    pub uncompacted_encoded_bytes: u64,
    pub last_sequence: u64,
    pub compacted_through_sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionManifest {
    pub format_version: u16,
    pub partition_family: String,
    pub partition_id: String,
    pub generation: u64,
    pub fence_token: u64,
    pub sealed_journals: Vec<ManifestJournalRef>,
    pub active_journal: Option<ManifestJournalRef>,
    pub segments: Vec<ManifestSegmentRef>,
    pub compacted_through_sequence: u64,
    pub last_record_hash: String,
    pub published_at: String,
    pub manifest_hash: Option<String>,
    pub manifest_signature: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestJournalRef {
    pub path: String,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub last_record_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestSegmentRef {
    pub family: String,
    pub path: String,
    pub generation: u64,
    pub record_count: u64,
    pub file_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct PartitionManifestProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(uint32, tag = "2")]
    format_version: u32,
    #[prost(string, tag = "3")]
    partition_family: String,
    #[prost(string, tag = "4")]
    partition_id: String,
    #[prost(uint64, tag = "5")]
    generation: u64,
    #[prost(uint64, tag = "6")]
    fence_token: u64,
    #[prost(message, repeated, tag = "7")]
    sealed_journals: Vec<ManifestJournalRefProto>,
    #[prost(message, optional, tag = "8")]
    active_journal: Option<ManifestJournalRefProto>,
    #[prost(message, repeated, tag = "9")]
    segments: Vec<ManifestSegmentRefProto>,
    #[prost(uint64, tag = "10")]
    compacted_through_sequence: u64,
    #[prost(string, tag = "11")]
    last_record_hash: String,
    #[prost(string, tag = "12")]
    published_at: String,
    #[prost(string, optional, tag = "13")]
    manifest_hash: Option<String>,
    #[prost(string, optional, tag = "14")]
    manifest_signature: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ObjectMetadataPartitionManifestRow {
    pub manifest_ref: String,
    pub object_ref_target: String,
    pub manifest_hash: String,
    pub generation: u64,
    pub published_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct ObjectMetadataPartitionManifestRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    manifest_ref: String,
    #[prost(string, tag = "4")]
    object_ref_target: String,
    #[prost(string, tag = "5")]
    manifest_hash: String,
    #[prost(uint64, tag = "6")]
    generation: u64,
    #[prost(string, tag = "7")]
    published_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct ManifestJournalRefProto {
    #[prost(string, tag = "1")]
    path: String,
    #[prost(uint64, tag = "2")]
    first_sequence: u64,
    #[prost(uint64, tag = "3")]
    last_sequence: u64,
    #[prost(string, tag = "4")]
    last_record_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct ManifestSegmentRefProto {
    #[prost(string, tag = "1")]
    family: String,
    #[prost(string, tag = "2")]
    path: String,
    #[prost(uint64, tag = "3")]
    generation: u64,
    #[prost(uint64, tag = "4")]
    record_count: u64,
    #[prost(string, tag = "5")]
    file_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WrittenSegment {
    family: FileFamily,
    ref_name: String,
    record_count: u64,
    file_hash: String,
    catalog_record: WriterSegmentCatalogRecord,
}

#[derive(Debug)]
struct StagedPartitionManifest {
    manifest: PartitionManifest,
    manifest_ref: String,
    manifest_payload: Vec<u8>,
    manifest_tuple_key: Vec<u8>,
    manifest_root_anchor_key: String,
    transaction_id: String,
}

#[derive(Debug)]
struct StagedObjectMetadataCompaction {
    segments: Vec<WrittenSegment>,
    partition_manifest: StagedPartitionManifest,
    metadata_record_count: usize,
    directory_record_count: usize,
    payload_bytes: u64,
}

pub(super) fn encode_partition_manifest(manifest: &PartitionManifest) -> Result<Vec<u8>> {
    encode_deterministic_proto(&partition_manifest_to_proto(manifest))
}

fn partition_manifest_to_proto(manifest: &PartitionManifest) -> PartitionManifestProto {
    PartitionManifestProto {
        schema: PARTITION_MANIFEST_SCHEMA.to_string(),
        format_version: u32::from(manifest.format_version),
        partition_family: manifest.partition_family.clone(),
        partition_id: manifest.partition_id.clone(),
        generation: manifest.generation,
        fence_token: manifest.fence_token,
        sealed_journals: manifest
            .sealed_journals
            .iter()
            .map(manifest_journal_ref_to_proto)
            .collect(),
        active_journal: manifest
            .active_journal
            .as_ref()
            .map(manifest_journal_ref_to_proto),
        segments: manifest
            .segments
            .iter()
            .map(manifest_segment_ref_to_proto)
            .collect(),
        compacted_through_sequence: manifest.compacted_through_sequence,
        last_record_hash: manifest.last_record_hash.clone(),
        published_at: manifest.published_at.clone(),
        manifest_hash: manifest.manifest_hash.clone(),
        manifest_signature: manifest.manifest_signature.clone(),
    }
}

fn partition_manifest_from_proto(proto: PartitionManifestProto) -> Result<PartitionManifest> {
    if proto.schema != PARTITION_MANIFEST_SCHEMA {
        return Err(anyhow!("partition manifest schema mismatch"));
    }
    Ok(PartitionManifest {
        format_version: u16::try_from(proto.format_version)
            .context("partition manifest format version exceeds u16")?,
        partition_family: proto.partition_family,
        partition_id: proto.partition_id,
        generation: proto.generation,
        fence_token: proto.fence_token,
        sealed_journals: proto
            .sealed_journals
            .into_iter()
            .map(manifest_journal_ref_from_proto)
            .collect::<Result<Vec<_>>>()?,
        active_journal: proto
            .active_journal
            .map(manifest_journal_ref_from_proto)
            .transpose()?,
        segments: proto
            .segments
            .into_iter()
            .map(manifest_segment_ref_from_proto)
            .collect::<Result<Vec<_>>>()?,
        compacted_through_sequence: proto.compacted_through_sequence,
        last_record_hash: proto.last_record_hash,
        published_at: proto.published_at,
        manifest_hash: proto.manifest_hash,
        manifest_signature: proto.manifest_signature,
    })
}

fn manifest_journal_ref_to_proto(value: &ManifestJournalRef) -> ManifestJournalRefProto {
    ManifestJournalRefProto {
        path: value.path.clone(),
        first_sequence: value.first_sequence,
        last_sequence: value.last_sequence,
        last_record_hash: value.last_record_hash.clone(),
    }
}

fn manifest_journal_ref_from_proto(value: ManifestJournalRefProto) -> Result<ManifestJournalRef> {
    Ok(ManifestJournalRef {
        path: value.path,
        first_sequence: value.first_sequence,
        last_sequence: value.last_sequence,
        last_record_hash: value.last_record_hash,
    })
}

fn manifest_segment_ref_to_proto(value: &ManifestSegmentRef) -> ManifestSegmentRefProto {
    ManifestSegmentRefProto {
        family: value.family.clone(),
        path: value.path.clone(),
        generation: value.generation,
        record_count: value.record_count,
        file_hash: value.file_hash.clone(),
    }
}

fn manifest_segment_ref_from_proto(value: ManifestSegmentRefProto) -> Result<ManifestSegmentRef> {
    Ok(ManifestSegmentRef {
        family: value.family,
        path: value.path,
        generation: value.generation,
        record_count: value.record_count,
        file_hash: value.file_hash,
    })
}

#[derive(Debug, Serialize)]
struct SegmentHeader {
    tenant_id: String,
    bucket_id: String,
    partition_family: &'static str,
    partition_id: String,
    generation: u64,
    key_order: &'static str,
    compression: &'static str,
    block_size_uncompressed: u32,
    bloom_bits_per_key: u8,
}

#[cfg(test)]
async fn seal_object_journal_segments(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<SealedObjectMetadataSegments> {
    let compaction_started_at = std::time::Instant::now();
    let staged = stage_object_journal_segments(storage, bucket, manifest_signing_key, 0).await?;
    publish_staged_compaction(storage, bucket, &staged, &[]).await?;
    complete_staged_compaction(staged, "object_metadata_seal", compaction_started_at)
}

pub(crate) async fn seal_object_journal_segments_with_permit(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<SealedObjectMetadataSegments> {
    require_object_metadata_permit(bucket, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    let compaction_started_at = std::time::Instant::now();
    let staged =
        stage_object_journal_segments(storage, bucket, manifest_signing_key, permit.fence_token)
            .await?;
    publish_staged_compaction(storage, bucket, &staged, &[partition_precondition]).await?;
    complete_staged_compaction(staged, "object_metadata_seal", compaction_started_at)
}

pub(crate) async fn seal_object_journal_segments_with_task_guard(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    task_guard: &TaskExecutionGuard,
) -> Result<SealedObjectMetadataSegments> {
    require_object_metadata_permit(bucket, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    let compaction_started_at = std::time::Instant::now();
    let staged =
        stage_object_journal_segments(storage, bucket, manifest_signing_key, permit.fence_token)
            .await?;
    publish_staged_compaction_for_task(
        storage,
        bucket,
        &staged,
        &partition_precondition,
        task_guard,
    )
    .await?;
    complete_staged_compaction(staged, "object_metadata_seal", compaction_started_at)
}

async fn stage_object_journal_segments(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    fence_token: u64,
) -> Result<StagedObjectMetadataCompaction> {
    let records = read_all_metadata_journal_records(storage, bucket).await?;
    let generation = records
        .last()
        .map(|record| record.partition_sequence)
        .ok_or_else(|| anyhow!("metadata journal has no records to seal"))?;

    let mut metadata_records = Vec::new();
    let mut directory_latest = std::collections::BTreeMap::<Vec<u8>, Vec<u8>>::new();
    for record in &records {
        let body = record.object_version_body()?;
        metadata_records.push(SegmentRecord::new(
            metadata_segment_key(&body),
            record.payload.clone(),
        ));
        let directory = directory_entry_from_object_version_body(&body);
        directory_latest.insert(
            directory_segment_key(&directory),
            encode_directory_entry_body(&directory)?,
        );
    }
    metadata_records.sort_by(|left, right| left.key.cmp(&right.key));
    let directory_records = directory_latest
        .into_iter()
        .map(|(key, value)| SegmentRecord::new(key, value))
        .collect::<Vec<_>>();

    stage_object_metadata_compaction(
        storage,
        bucket,
        generation,
        &records,
        &metadata_records,
        &directory_records,
        manifest_signing_key,
        fence_token,
    )
    .await
}

async fn stage_object_metadata_compaction(
    storage: &Storage,
    bucket: &Bucket,
    generation: u64,
    records: &[ObjectMetadataRecord],
    metadata_records: &[SegmentRecord],
    directory_records: &[SegmentRecord],
    manifest_signing_key: &[u8],
    fence_token: u64,
) -> Result<StagedObjectMetadataCompaction> {
    // Derive publication time from the immutable source journal so retries of
    // the same generation produce byte-identical segment and manifest rows.
    let source_timestamp = records
        .last()
        .map(|record| parse_body_timestamp(&record.body.created_at))
        .transpose()?
        .ok_or_else(|| anyhow!("object metadata compaction requires source records"))?;
    let source_timestamp_nanos = source_timestamp
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("object metadata source timestamp cannot be represented"))?;
    let created_at_unix_nanos = u64::try_from(source_timestamp_nanos)
        .map_err(|_| anyhow!("object metadata source timestamp must be nonnegative"))?;
    let published_at = source_timestamp.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    let metadata_segment = stage_segment_file(
        storage,
        bucket,
        generation,
        FileFamily::MetadataSegment,
        segment_header(
            bucket,
            generation,
            "object_metadata",
            "tenant_bucket_key_version",
        ),
        metadata_records,
        created_at_unix_nanos,
    )
    .await?;
    let directory_segment = stage_segment_file(
        storage,
        bucket,
        generation,
        FileFamily::DirectorySegment,
        segment_header(bucket, generation, "directory", "tenant_bucket_prefix_key"),
        directory_records,
        created_at_unix_nanos,
    )
    .await?;
    let segments = vec![metadata_segment, directory_segment];
    let partition_manifest = stage_partition_manifest(
        storage,
        bucket,
        generation,
        records,
        &segments,
        manifest_signing_key,
        fence_token,
        published_at,
    )
    .await?;
    Ok(StagedObjectMetadataCompaction {
        segments,
        partition_manifest,
        metadata_record_count: metadata_records.len(),
        directory_record_count: directory_records.len(),
        payload_bytes: segment_payload_bytes(metadata_records)
            .saturating_add(segment_payload_bytes(directory_records)),
    })
}

async fn publish_staged_compaction(
    storage: &Storage,
    bucket: &Bucket,
    staged: &StagedObjectMetadataCompaction,
    additional_preconditions: &[CoreMutationPrecondition],
) -> Result<()> {
    for segment in &staged.segments {
        publish_segment_catalog(storage, segment, additional_preconditions).await?;
    }
    publish_partition_manifest(
        storage,
        bucket,
        &staged.partition_manifest,
        additional_preconditions,
    )
    .await
}

async fn publish_staged_compaction_for_task(
    storage: &Storage,
    bucket: &Bucket,
    staged: &StagedObjectMetadataCompaction,
    partition_precondition: &CoreMutationPrecondition,
    task_guard: &TaskExecutionGuard,
) -> Result<()> {
    for segment in &staged.segments {
        let permit = task_guard.publication_permit().await?;
        permit
            .publish_with(|task_precondition| async move {
                let preconditions = [partition_precondition.clone(), task_precondition];
                publish_segment_catalog(storage, segment, &preconditions).await
            })
            .await?;
    }

    let permit = task_guard.publication_permit().await?;
    permit
        .publish_with(|task_precondition| async move {
            let preconditions = [partition_precondition.clone(), task_precondition];
            publish_partition_manifest(storage, bucket, &staged.partition_manifest, &preconditions)
                .await
        })
        .await
}

fn complete_staged_compaction(
    staged: StagedObjectMetadataCompaction,
    operation: &'static str,
    started_at: std::time::Instant,
) -> Result<SealedObjectMetadataSegments> {
    crate::perf::record_compaction_duration(
        operation,
        "object_blob",
        "ok",
        staged.payload_bytes,
        started_at.elapsed(),
    );
    let StagedObjectMetadataCompaction {
        partition_manifest,
        metadata_record_count,
        directory_record_count,
        ..
    } = staged;
    let StagedPartitionManifest {
        manifest,
        manifest_ref,
        ..
    } = partition_manifest;
    Ok(SealedObjectMetadataSegments {
        generation: manifest.generation,
        metadata_ref: manifest.segments[0].path.clone(),
        directory_ref: manifest.segments[1].path.clone(),
        metadata_record_count,
        directory_record_count,
        manifest_ref,
        manifest_hash: manifest
            .manifest_hash
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow!("partition manifest hash was not set"))?,
    })
}

pub fn decode_segment_file(
    input: &[u8],
    expected_family: FileFamily,
) -> Result<Vec<SegmentRecord>> {
    let (records, _) = decode_segment_file_with_footer(input, expected_family)?;
    Ok(records)
}

fn decode_segment_file_with_footer(
    input: &[u8],
    expected_family: FileFamily,
) -> Result<(Vec<SegmentRecord>, crate::formats::WriterSegmentTrailer)> {
    let segment = decode_writer_segment(input, expected_family)?;
    let records = decode_object_segment_body_table(segment.body)?;
    Ok((records, segment.footer))
}

pub async fn recover_object_metadata_partition(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<RecoveredObjectMetadataPartition> {
    let manifest = read_latest_partition_manifest(storage, bucket, manifest_signing_key)
        .await?
        .ok_or_else(|| anyhow!("object metadata partition manifest is missing"))?;
    let expected_partition_id =
        hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id));
    if manifest.partition_family != "object_metadata" {
        return Err(anyhow!("partition manifest family mismatch"));
    }
    if manifest.partition_id != expected_partition_id {
        return Err(anyhow!("partition manifest id mismatch"));
    }

    let mut metadata_records = Vec::new();
    let mut directory_latest = std::collections::BTreeMap::<Vec<u8>, SegmentRecord>::new();
    for segment in &manifest.segments {
        let family = file_family_from_manifest_name(&segment.family)?;
        let bytes = read_manifest_segment(storage, segment).await?;
        let (mut records, footer) = decode_segment_file_with_footer(&bytes, family)?;
        if hex::encode(footer.file_hash) != segment.file_hash {
            return Err(anyhow!("partition segment file hash mismatch"));
        }
        if footer.record_count != segment.record_count {
            return Err(anyhow!("partition segment record count mismatch"));
        }

        match family {
            FileFamily::MetadataSegment => metadata_records.append(&mut records),
            FileFamily::DirectorySegment => {
                for record in records {
                    directory_latest.insert(record.key.clone(), record);
                }
            }
            _ => {
                return Err(anyhow!(
                    "unexpected segment family in object metadata manifest"
                ));
            }
        }
    }

    if let Some(active_journal) = &manifest.active_journal {
        let records = read_manifest_journal_ref_records(storage, active_journal).await?;
        let first = records
            .first()
            .ok_or_else(|| anyhow!("active journal manifest entry points at an empty stream"))?;
        let last = records
            .last()
            .ok_or_else(|| anyhow!("active journal manifest entry points at an empty stream"))?;
        if first.partition_sequence != active_journal.first_sequence
            || last.partition_sequence != active_journal.last_sequence
            || last.event_hash != active_journal.last_record_hash
        {
            return Err(anyhow!("active journal manifest reference mismatch"));
        }
        for record in records {
            let body = record.object_version_body()?;
            metadata_records.push(SegmentRecord::new(
                metadata_segment_key(&body),
                record.payload.clone(),
            ));
            let directory = directory_entry_from_object_version_body(&body);
            let segment_record = SegmentRecord::new(
                directory_segment_key(&directory),
                encode_directory_entry_body(&directory)?,
            );
            directory_latest.insert(segment_record.key.clone(), segment_record);
        }
    }

    metadata_records.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(RecoveredObjectMetadataPartition {
        manifest,
        metadata_records,
        directory_records: directory_latest.into_values().collect(),
    })
}

async fn recover_object_directory_partition(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<(
    PartitionManifest,
    std::collections::BTreeMap<Vec<u8>, DirectoryEntryBody>,
)> {
    let manifest = read_latest_partition_manifest(storage, bucket, manifest_signing_key)
        .await?
        .ok_or_else(|| anyhow!("object metadata partition manifest is missing"))?;
    let expected_partition_id =
        hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id));
    if manifest.partition_family != "object_metadata" {
        return Err(anyhow!("partition manifest family mismatch"));
    }
    if manifest.partition_id != expected_partition_id {
        return Err(anyhow!("partition manifest id mismatch"));
    }

    let mut directory_latest = std::collections::BTreeMap::<Vec<u8>, DirectoryEntryBody>::new();
    for segment in &manifest.segments {
        let family = file_family_from_manifest_name(&segment.family)?;
        if family != FileFamily::DirectorySegment {
            continue;
        }
        let bytes = read_manifest_segment(storage, segment).await?;
        let (records, footer) =
            decode_segment_file_with_footer(&bytes, FileFamily::DirectorySegment)?;
        if hex::encode(footer.file_hash) != segment.file_hash {
            return Err(anyhow!("directory segment file hash mismatch"));
        }
        if footer.record_count != segment.record_count {
            return Err(anyhow!("directory segment record count mismatch"));
        }
        for record in records {
            let entry = decode_directory_entry_body(&record.value)?;
            directory_latest.insert(record.key, entry);
        }
    }

    if let Some(active_journal) = &manifest.active_journal {
        let records = read_manifest_journal_ref_records(storage, active_journal).await?;
        let first = records
            .first()
            .ok_or_else(|| anyhow!("active journal manifest entry points at an empty stream"))?;
        let last = records
            .last()
            .ok_or_else(|| anyhow!("active journal manifest entry points at an empty stream"))?;
        if first.partition_sequence != active_journal.first_sequence
            || last.partition_sequence != active_journal.last_sequence
            || last.event_hash != active_journal.last_record_hash
        {
            return Err(anyhow!("active journal manifest reference mismatch"));
        }
        for record in records {
            let body = directory_entry_from_object_version_body(&record.object_version_body()?);
            directory_latest.insert(directory_segment_key(&body), body);
        }
    }

    Ok((manifest, directory_latest))
}

pub async fn next_object_id(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<i64> {
    let _ = manifest_signing_key;
    CoreStore::new(storage.clone())
        .await?
        .next_object_metadata_id(bucket)
        .await
}

pub async fn read_current_object(
    storage: &Storage,
    bucket: &Bucket,
    _manifest_signing_key: &[u8],
    object_key: &str,
) -> Result<Option<Object>> {
    CoreStore::new(storage.clone())
        .await?
        .read_current_object_metadata(bucket, object_key)
        .await
}

pub async fn read_object_version(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    object_key: &str,
    version_id: uuid::Uuid,
) -> Result<Option<Object>> {
    let _ = manifest_signing_key;
    CoreStore::new(storage.clone())
        .await?
        .read_object_version_metadata(bucket, object_key, version_id)
        .await
}

pub async fn read_object_version_by_id(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    version_id: uuid::Uuid,
) -> Result<Option<Object>> {
    let _ = manifest_signing_key;
    CoreStore::new(storage.clone())
        .await?
        .read_object_version_metadata_by_id(bucket, version_id)
        .await
}

pub async fn list_current_objects(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    prefix: &str,
    start_after: &str,
    limit: i32,
    delimiter: &str,
) -> Result<NativeObjectListing> {
    let mut objects = read_current_directory_objects(storage, bucket, manifest_signing_key).await?;
    objects.retain(|object| {
        object.key.starts_with(prefix)
            && object.key.as_str() > start_after
            && !crate::validation::is_reserved_internal_key(&object.key)
    });
    objects.sort_by(|left, right| left.key.cmp(&right.key));

    let limit = limit.max(1) as usize;
    if delimiter.is_empty() {
        objects.truncate(limit);
        return Ok(NativeObjectListing {
            objects,
            common_prefixes: Vec::new(),
        });
    }

    enum ListingEntry {
        Object(Object),
        CommonPrefix(String),
    }

    let mut merged = std::collections::BTreeMap::<String, ListingEntry>::new();
    for object in objects {
        let suffix = &object.key[prefix.len()..];
        if let Some(position) = suffix.find(delimiter) {
            let common_prefix = format!("{}{}", prefix, &suffix[..position + delimiter.len()]);
            merged
                .entry(common_prefix.clone())
                .or_insert(ListingEntry::CommonPrefix(common_prefix));
        } else {
            merged.insert(object.key.clone(), ListingEntry::Object(object));
        }
        if merged.len() >= limit {
            break;
        }
    }

    let mut listing = NativeObjectListing {
        objects: Vec::new(),
        common_prefixes: Vec::new(),
    };
    for (_, entry) in merged.into_iter().take(limit) {
        match entry {
            ListingEntry::Object(object) => listing.objects.push(object),
            ListingEntry::CommonPrefix(common_prefix) => {
                listing.common_prefixes.push(common_prefix)
            }
        }
    }
    Ok(listing)
}

pub(crate) async fn read_current_directory_objects(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<Vec<Object>> {
    let _ = manifest_signing_key;
    CoreStore::new(storage.clone())
        .await?
        .list_current_object_metadata(bucket)
        .await
}

pub async fn read_current_objects(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<Vec<Object>> {
    read_current_directory_objects(storage, bucket, manifest_signing_key).await
}

pub async fn read_current_objects_through_sequence(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    max_sequence: u128,
) -> Result<Vec<Object>> {
    if max_sequence == 0 {
        return Ok(Vec::new());
    }
    let body_records = read_object_version_bodies_through_sequence(
        storage,
        bucket,
        manifest_signing_key,
        max_sequence,
    )
    .await?;
    current_objects_from_version_bodies(body_records)
}

pub async fn compare_directory_index_to_metadata(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<DirectoryIndexComparison> {
    let stats = active_object_journal_stats(storage, bucket, manifest_signing_key).await?;
    let source_cursor = u128::from(stats.last_sequence.max(stats.compacted_through_sequence));
    Ok(DirectoryIndexComparison {
        source_cursor,
        expected: expected_directory_index_snapshot_from_metadata(
            storage,
            bucket,
            manifest_signing_key,
        )
        .await?,
        actual: current_directory_index_snapshot_from_index(storage, bucket, manifest_signing_key)
            .await?,
    })
}

pub async fn expected_directory_index_snapshot_from_metadata(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<DirectoryIndexSnapshot> {
    let expected_entries =
        expected_directory_entries_from_metadata(storage, bucket, manifest_signing_key).await?;
    directory_index_snapshot(&expected_entries)
}

pub async fn current_directory_index_snapshot_from_index(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<DirectoryIndexSnapshot> {
    let actual_entries =
        current_directory_entries_from_index(storage, bucket, manifest_signing_key).await?;
    directory_index_snapshot(&actual_entries)
}

pub async fn rebuild_directory_index_from_metadata_with_permit(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<SealedObjectMetadataSegments> {
    let compaction_started_at = std::time::Instant::now();
    require_object_metadata_permit(bucket, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    let body_records =
        read_object_version_bodies_from_metadata_only(storage, bucket, manifest_signing_key)
            .await?;
    let records = read_all_metadata_journal_records(storage, bucket).await?;
    let generation = records
        .last()
        .map(|record| record.partition_sequence)
        .ok_or_else(|| anyhow!("metadata journal has no records to rebuild directory index"))?;

    let mut metadata_records = body_records
        .iter()
        .map(|(_, body)| {
            Ok(SegmentRecord::new(
                metadata_segment_key(body),
                encode_object_version_body(body)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    metadata_records.sort_by(|left, right| left.key.cmp(&right.key));

    let directory_entries = directory_entries_from_object_version_bodies(body_records)?;
    let directory_records = directory_entries
        .into_iter()
        .map(|(key, body)| Ok(SegmentRecord::new(key, encode_directory_entry_body(&body)?)))
        .collect::<Result<Vec<_>>>()?;

    let staged = stage_object_metadata_compaction(
        storage,
        bucket,
        generation,
        &records,
        &metadata_records,
        &directory_records,
        manifest_signing_key,
        permit.fence_token,
    )
    .await?;
    publish_staged_compaction(storage, bucket, &staged, &[partition_precondition]).await?;
    complete_staged_compaction(staged, "directory_index_rebuild", compaction_started_at)
}

fn current_objects_from_version_bodies(
    body_records: Vec<(usize, ObjectVersionBody)>,
) -> Result<Vec<Object>> {
    let mut versions_by_key = object_versions_by_key(body_records);

    let mut current = Vec::new();
    for versions in versions_by_key.values_mut() {
        sort_versions_for_key(versions);
        if let Some((_, body)) = versions.last() {
            if !body.delete_marker && body.deleted_at.is_none() {
                current.push(object_from_body(body)?);
            }
        }
    }
    current.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(current)
}

pub async fn read_object_versions(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    prefix: &str,
    key_marker: &str,
    version_id_marker: Option<uuid::Uuid>,
    limit: i32,
) -> Result<ObjectVersionsPage> {
    let body_records = read_object_version_bodies(storage, bucket, manifest_signing_key).await?;
    let mut versions_by_key = object_versions_by_key(body_records);
    let marker = if let Some(version_id_marker) = version_id_marker {
        let marker = versions_by_key
            .get(key_marker)
            .and_then(|versions| {
                versions
                    .iter()
                    .find(|(_, body)| body.version_id == version_id_marker.to_string())
            })
            .cloned();
        let Some(marker) = marker else {
            return Ok(ObjectVersionsPage {
                versions: Vec::new(),
                is_truncated: false,
                next_key_marker: None,
                next_version_id_marker: None,
            });
        };
        Some(marker)
    } else {
        None
    };

    let mut flattened = Vec::<(usize, ObjectVersionBody, bool)>::new();
    for versions in versions_by_key.values_mut() {
        sort_versions_for_key_descending(versions);
        let mut visible_index = 0_usize;
        for (order, body) in versions.iter() {
            if !body.delete_marker && body.deleted_at.is_some() {
                continue;
            }
            flattened.push((*order, body.clone(), visible_index == 0));
            visible_index += 1;
        }
    }
    flattened.sort_by(|(left_order, left, _), (right_order, right, _)| {
        left.object_key
            .cmp(&right.object_key)
            .then_with(|| {
                parse_body_timestamp(&right.created_at)
                    .ok()
                    .cmp(&parse_body_timestamp(&left.created_at).ok())
            })
            .then_with(|| right_order.cmp(left_order))
    });

    let mut selected = Vec::new();
    for (order, body, is_latest) in flattened {
        if !body.object_key.starts_with(prefix)
            || crate::validation::is_reserved_internal_key(&body.object_key)
            || (!body.delete_marker && body.deleted_at.is_some())
        {
            continue;
        }
        if let Some((marker_order, marker_body)) = marker.as_ref() {
            if body.object_key.as_str() < key_marker {
                continue;
            }
            if body.object_key == key_marker
                && !version_sorts_after_marker(order, &body, *marker_order, marker_body)?
            {
                continue;
            }
        } else if body.object_key.as_str() <= key_marker {
            continue;
        }

        selected.push(ObjectVersion {
            is_delete_marker: body.delete_marker,
            is_latest,
            object: object_from_body(&body)?,
        });
    }

    let limit = limit.max(1) as usize;
    let is_truncated = selected.len() > limit;
    if is_truncated {
        selected.truncate(limit);
    }
    let (next_key_marker, next_version_id_marker) = if is_truncated {
        selected
            .last()
            .map(|version| {
                (
                    Some(version.object.key.clone()),
                    Some(version.object.version_id),
                )
            })
            .unwrap_or((None, None))
    } else {
        (None, None)
    };
    Ok(ObjectVersionsPage {
        versions: selected,
        is_truncated,
        next_key_marker,
        next_version_id_marker,
    })
}

async fn read_object_version_bodies(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<Vec<(usize, ObjectVersionBody)>> {
    read_object_version_bodies_inner(storage, bucket, manifest_signing_key, None).await
}

async fn read_object_version_bodies_through_sequence(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    max_sequence: u128,
) -> Result<Vec<(usize, ObjectVersionBody)>> {
    let max_sequence = u64::try_from(max_sequence)
        .map_err(|_| anyhow!("object metadata source cursor exceeds u64 sequence range"))?;
    read_object_version_bodies_inner(storage, bucket, manifest_signing_key, Some(max_sequence))
        .await
}

async fn read_object_version_bodies_inner(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    max_sequence: Option<u64>,
) -> Result<Vec<(usize, ObjectVersionBody)>> {
    let mut body_records = Vec::<(usize, ObjectVersionBody)>::new();
    let mut order = 0usize;
    let mut compacted_through_sequence = 0u64;

    if partition_manifest_exists(storage, bucket).await? {
        let recovered = recover_object_metadata_partition(storage, bucket, manifest_signing_key)
            .await
            .context("recover object metadata partition from CoreStore manifest")?;
        compacted_through_sequence = recovered.manifest.compacted_through_sequence;
        if let Some(max_sequence) = max_sequence {
            if compacted_through_sequence > max_sequence {
                return Err(anyhow!(
                    "object metadata source cursor is older than manifest checkpoint"
                ));
            }
        }
        for record in recovered.metadata_records {
            let body = decode_object_version_body(&record.value)?;
            body_records.push((order, body));
            order += 1;
        }
    }

    for record in read_all_metadata_journal_records(storage, bucket).await? {
        if record.partition_sequence <= compacted_through_sequence {
            continue;
        }
        if max_sequence.is_some_and(|max_sequence| record.partition_sequence > max_sequence) {
            continue;
        }
        if record.record_kind.is_object_version_like() {
            body_records.push((order, record.object_version_body()?));
            order += 1;
        }
    }
    Ok(body_records)
}

async fn read_object_version_bodies_from_metadata_only(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<Vec<(usize, ObjectVersionBody)>> {
    let mut body_records = Vec::<(usize, ObjectVersionBody)>::new();
    let mut order = 0usize;
    let mut compacted_through_sequence = 0u64;

    if let Some(manifest) =
        read_latest_partition_manifest(storage, bucket, manifest_signing_key).await?
    {
        let expected_partition_id =
            hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id));
        if manifest.partition_family != "object_metadata" {
            return Err(anyhow!("partition manifest family mismatch"));
        }
        if manifest.partition_id != expected_partition_id {
            return Err(anyhow!("partition manifest id mismatch"));
        }
        compacted_through_sequence = manifest.compacted_through_sequence;
        for segment in &manifest.segments {
            let family = file_family_from_manifest_name(&segment.family)?;
            if family != FileFamily::MetadataSegment {
                continue;
            }
            let bytes = read_manifest_segment(storage, segment).await?;
            let (records, footer) =
                decode_segment_file_with_footer(&bytes, FileFamily::MetadataSegment)?;
            if hex::encode(footer.file_hash) != segment.file_hash {
                return Err(anyhow!("metadata segment file hash mismatch"));
            }
            if footer.record_count != segment.record_count {
                return Err(anyhow!("metadata segment record count mismatch"));
            }
            for record in records {
                let body = decode_object_version_body(&record.value)?;
                body_records.push((order, body));
                order += 1;
            }
        }
    }

    for record in read_all_metadata_journal_records(storage, bucket).await? {
        if record.partition_sequence <= compacted_through_sequence {
            continue;
        }
        if record.record_kind.is_object_version_like() {
            body_records.push((order, record.object_version_body()?));
            order += 1;
        }
    }
    Ok(body_records)
}

async fn current_directory_entries_from_index(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<std::collections::BTreeMap<Vec<u8>, DirectoryEntryBody>> {
    let mut directory_records = std::collections::BTreeMap::<Vec<u8>, DirectoryEntryBody>::new();
    let mut compacted_through_sequence = 0u64;

    if partition_manifest_exists(storage, bucket).await? {
        let (manifest, recovered_directory) =
            recover_object_directory_partition(storage, bucket, manifest_signing_key)
                .await
                .context("recover object directory partition from CoreStore manifest")?;
        compacted_through_sequence = manifest.compacted_through_sequence;
        directory_records.extend(recovered_directory);
    }

    for record in read_all_metadata_journal_records(storage, bucket).await? {
        if record.partition_sequence <= compacted_through_sequence {
            continue;
        }
        let body = directory_entry_from_object_version_body(&record.object_version_body()?);
        directory_records.insert(directory_segment_key(&body), body);
    }
    Ok(directory_records)
}

async fn expected_directory_entries_from_metadata(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<std::collections::BTreeMap<Vec<u8>, DirectoryEntryBody>> {
    directory_entries_from_object_version_bodies(
        read_object_version_bodies_from_metadata_only(storage, bucket, manifest_signing_key)
            .await?,
    )
}

fn directory_entries_from_object_version_bodies(
    body_records: Vec<(usize, ObjectVersionBody)>,
) -> Result<std::collections::BTreeMap<Vec<u8>, DirectoryEntryBody>> {
    let mut versions_by_key = object_versions_by_key(body_records);
    let mut entries = std::collections::BTreeMap::<Vec<u8>, DirectoryEntryBody>::new();
    for versions in versions_by_key.values_mut() {
        sort_versions_for_key(versions);
        if let Some((_, body)) = versions.last() {
            let directory = directory_entry_from_object_version_body(body);
            entries.insert(directory_segment_key(&directory), directory);
        }
    }
    Ok(entries)
}

fn directory_entry_from_object_version_body(body: &ObjectVersionBody) -> DirectoryEntryBody {
    DirectoryEntryBody {
        fence_token: body.fence_token,
        tenant_id: body.tenant_id,
        bucket_id: body.bucket_id,
        bucket_name: body.bucket_name.clone(),
        object_key: body.object_key.clone(),
        event: body.event.clone(),
        kind: body.kind,
        id: body.id,
        version_id: body.version_id.clone(),
        mutation_id: body.mutation_id.clone(),
        content_hash: body.content_hash.clone(),
        size: body.size,
        etag: body.etag.clone(),
        content_type: body.content_type.clone(),
        user_metadata_hash: body.user_metadata_hash.clone(),
        authz_revision: body.authz_revision,
        index_policy_snapshot: body.index_policy_snapshot.clone(),
        record_hash: body.record_hash.clone(),
        storage_class: body.storage_class.clone(),
        user_meta: body.user_meta.clone(),
        shard_map: body.shard_map.clone(),
        checksum: body.checksum.clone(),
        link: body.link.clone(),
        delete_marker: body.delete_marker,
        created_at: body.created_at.clone(),
        deleted_at: body.deleted_at.clone(),
    }
}

fn directory_index_snapshot(
    entries: &std::collections::BTreeMap<Vec<u8>, DirectoryEntryBody>,
) -> Result<DirectoryIndexSnapshot> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.directory_index.snapshot.v1");
    for (key, body) in entries {
        hasher.update(&(key.len() as u64).to_le_bytes());
        hasher.update(key);
        let body = encode_directory_entry_body(body)?;
        hasher.update(&(body.len() as u64).to_le_bytes());
        hasher.update(&body);
    }
    Ok(DirectoryIndexSnapshot {
        entry_count: entries.len(),
        snapshot_hash: hasher.finalize().to_hex().to_string(),
    })
}

async fn read_all_metadata_journal_records(
    storage: &Storage,
    bucket: &Bucket,
) -> Result<Vec<ObjectMetadataRecord>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    read_metadata_journal_records_from_store(
        &core_store,
        &object_metadata_stream_id(bucket.tenant_id, bucket.id),
    )
    .await
}

mod object_data_target;
use self::object_data_target::{
    object_data_target_bytes, object_data_target_kind, shard_map_from_object_data_target,
};

mod object_mutation;
#[cfg(test)]
pub(crate) use self::object_mutation::append_object_mutation;
#[cfg(test)]
use self::object_mutation::append_object_mutation_inner;
pub(crate) use self::object_mutation::{
    append_object_mutation_with_permit, append_object_mutation_with_permit_in_transaction,
    append_object_put_mutations_with_permit_in_transaction,
    commit_object_put_mutations_with_permit,
};

mod transaction_projection;
pub use transaction_projection::*;

mod version_sort;
use self::version_sort::{
    object_versions_by_key, sort_versions_for_key, sort_versions_for_key_descending,
};

mod helpers;
pub use helpers::*;

#[cfg(test)]
mod atomicity_tests;

#[cfg(test)]
mod task_compaction_tests;

#[cfg(test)]
mod tests;
