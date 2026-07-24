use anyhow::{Result, anyhow, bail};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Notify, OnceCell};

use crate::{
    append_journal, authz_journal, authz_repair,
    bucket_journal::{self, BucketJournalMutation},
    config::Config,
    control_journal,
    core_store::{CoreMutationBatchAdditions, CoreObjectRef, CoreStore},
    directory_repair,
    embedding_provider::EmbeddingProviderRegistry,
    hf_journal, index_builder, index_diagnostic_journal, index_journal, index_repair,
    manifest_journal, mesh_control_stream, mesh_directory, metadata_journal, model_journal,
    multipart_journal, object_links,
    partition_fence::{
        AcquireOwnership, ForceExpireOwnership, MAX_OWNERSHIP_LEASE_MS,
        MAX_PARTITION_FENCE_PAGE_SIZE, OWNERSHIP_HELD, OwnershipPrincipal, OwnershipResource,
        OwnershipResourceKind, PartitionOwnerStatus, PartitionRecoveryAcquire,
        PartitionWritePermit, RenewOwnership, acquire_ownership, acquire_partition_recovery,
        force_expire_ownership, force_expire_partition_owner_for_node,
        list_active_ownership_fences_for_node_page, list_partition_owners_for_node_page,
        partition_owner_is_force_expired, publish_partition_ready, read_ownership_fence,
        read_partition_owner, renew_ownership,
    },
    personaldb_repair, repair_finding,
    storage::Storage,
    task_journal, task_lease, watch_checkpoint, watch_log,
};

#[derive(Debug, Clone)]
pub struct Persistence {
    storage: Storage,
    core_store: Arc<OnceCell<CoreStore>>,
    task_notify: Arc<Notify>,
    mesh_id: String,
    region: String,
    cell_id: String,
    owner_node_id: String,
    task_actor_instance_id: String,
    partition_owner_signing_key: Vec<u8>,
    embedding_providers: EmbeddingProviderRegistry,
    object_metadata_compaction_frame_threshold: u64,
    object_metadata_compaction_bytes_threshold: u64,
    task_lease_ttl_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionDrainBucketOverride {
    pub tenant_id: String,
    pub bucket_name: String,
    pub disposition: crate::mesh_lifecycle::BucketDrainDisposition,
    pub reason: String,
    pub expires_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionDrainBucketDecision {
    pub tenant_id: String,
    pub bucket_name: String,
    pub bucket_locator_generation_before: u64,
    pub bucket_locator_generation_after: u64,
    pub status_before: mesh_directory::BucketLocatorStatus,
    pub status_after: mesh_directory::BucketLocatorStatus,
    pub disposition: crate::mesh_lifecycle::BucketDrainDisposition,
    pub reason: String,
    pub expires_at: Option<String>,
    pub exception_written: bool,
    pub locator_updated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionDrainPlanReport {
    pub region: String,
    pub decisions: Vec<RegionDrainBucketDecision>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HfKey {
    pub(crate) id: i64,
    pub(crate) tenant_id: i64,
    pub(crate) name: String,
    pub(crate) token_encrypted: Vec<u8>,
    pub(crate) note: Option<String>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HfIngestion {
    pub(crate) id: i64,
    pub(crate) key_id: i64,
    pub(crate) tenant_id: i64,
    pub(crate) requester_app_id: i64,
    pub(crate) repo: String,
    pub(crate) revision: String,
    pub(crate) target_bucket: String,
    pub(crate) target_region: String,
    pub(crate) target_prefix: String,
    pub(crate) include_globs: Vec<String>,
    pub(crate) exclude_globs: Vec<String>,
    pub(crate) state: crate::tasks::HFIngestionState,
    pub(crate) error: Option<String>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) started_at: Option<DateTime<Utc>>,
    pub(crate) finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HfIngestionItem {
    pub(crate) id: i64,
    pub(crate) ingestion_id: i64,
    pub(crate) path: String,
    pub(crate) size: Option<i64>,
    pub(crate) etag: Option<String>,
    pub(crate) state: crate::tasks::HFIngestionItemState,
    pub(crate) error: Option<String>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) started_at: Option<DateTime<Utc>>,
    pub(crate) finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tenant {
    pub id: i64,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct App {
    pub id: i64,
    pub name: String,
    pub client_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bucket {
    pub id: i64,
    pub tenant_id: i64,
    pub name: String,
    pub region: String,
    pub created_at: DateTime<Utc>,
    pub is_public_read: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketMetadataEvent {
    pub id: i64,
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub bucket_name: String,
    pub event_type: String,
    pub mutation_id: uuid::Uuid,
    pub bucket_metadata: JsonValue,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Object {
    pub id: i64,
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub key: String,
    #[serde(default)]
    pub kind: object_links::ObjectEntryKind,
    pub content_hash: String,
    pub size: i64,
    pub etag: String,
    pub content_type: Option<String>,
    pub version_id: uuid::Uuid,
    pub mutation_id: uuid::Uuid,
    pub index_policy_snapshot: String,
    pub user_metadata_hash: String,
    pub authz_revision: i64,
    pub record_hash: String,
    pub created_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub storage_class: Option<String>,
    pub user_meta: Option<JsonValue>,
    pub shard_map: Option<JsonValue>,
    pub checksum: Option<Vec<u8>>,
    #[serde(default)]
    pub link: Option<object_links::ObjectLinkTarget>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectVersion {
    pub object: Object,
    pub is_delete_marker: bool,
    pub is_latest: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectVersionsPage {
    pub versions: Vec<ObjectVersion>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_version_id_marker: Option<uuid::Uuid>,
}

struct ObjectVersionRecordHashInput<'a> {
    tenant_id: i64,
    bucket_id: i64,
    key: &'a str,
    version_id: uuid::Uuid,
    mutation_id: uuid::Uuid,
    content_hash: &'a str,
    size: i64,
    etag: &'a str,
    content_type: Option<&'a str>,
    storage_class: Option<&'a str>,
    user_metadata_hash: &'a str,
    index_policy_snapshot: &'a str,
    authz_revision: i64,
    delete_marker: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartUpload {
    pub id: i64,
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub key: String,
    pub upload_id: uuid::Uuid,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub aborted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartUploadMutation {
    pub upload: MultipartUpload,
    pub receipt: MetadataMutationReceipt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartUploadsPage {
    pub uploads: Vec<MultipartUpload>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<uuid::Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartUploadPart {
    pub id: i64,
    pub upload_id: i64,
    pub part_number: i32,
    pub content_hash: String,
    pub object_ref: CoreObjectRef,
    pub size: i64,
    pub etag: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartUploadPartMutation {
    pub part: MultipartUploadPart,
    pub receipt: MetadataMutationReceipt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartCompletionMutation {
    pub completed: bool,
    pub receipt: Option<MetadataMutationReceipt>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartAbortMutation {
    pub aborted: bool,
    pub receipt: Option<MetadataMutationReceipt>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartPartsPage {
    pub parts: Vec<MultipartUploadPart>,
    pub is_truncated: bool,
    pub next_part_number_marker: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectWatchEvent {
    pub id: i64,
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub bucket_name: String,
    pub key: String,
    pub event_type: String,
    pub version_id: Option<uuid::Uuid>,
    pub mutation_id: uuid::Uuid,
    pub payload_hash: String,
    pub etag: Option<String>,
    pub size: i64,
    pub is_delete_marker: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataMutationReceipt {
    pub mutation_id: uuid::Uuid,
    pub payload_hash: String,
    pub record_hash: String,
    pub watch_cursor: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendStream {
    pub id: i64,
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub bucket_name: String,
    pub stream_key: String,
    pub stream_id: uuid::Uuid,
    pub created_at: DateTime<Utc>,
    pub sealed_at: Option<DateTime<Utc>>,
    pub segment_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendStreamMutation {
    pub stream: AppendStream,
    pub receipt: MetadataMutationReceipt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendStreamRecord {
    pub id: i64,
    pub stream_id: i64,
    pub record_sequence: i64,
    pub payload_hash: String,
    pub payload_object_ref: CoreObjectRef,
    pub payload_size: i64,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub user_meta: Option<JsonValue>,
    #[serde(default)]
    pub authenticated_principal: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendStreamRecordMutation {
    pub record: AppendStreamRecord,
    pub receipt: MetadataMutationReceipt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SealAppendStreamMutation {
    pub sealed: bool,
    pub receipt: Option<MetadataMutationReceipt>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestCasResult {
    pub revision: i64,
    pub manifest_hash: String,
    pub receipt: MetadataMutationReceipt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthzTupleRecord {
    pub revision: i64,
    #[serde(default)]
    pub revision_ordinal: u32,
    pub tenant_id: i64,
    pub namespace: String,
    pub object_id: String,
    pub relation: String,
    pub subject_kind: String,
    pub subject_id: String,
    pub caveat_hash: String,
    pub operation: String,
    pub written_by: String,
    pub reason: String,
    pub mutation_id: uuid::Uuid,
    pub record_hash: String,
    pub written_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct AuthzTupleBatchMutation {
    pub namespace: String,
    pub object_id: String,
    pub relation: String,
    pub subject_kind: String,
    pub subject_id: String,
    pub caveat_hash: String,
    pub operation: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthzSchemaBindingPrecondition {
    pub tuple_key: Vec<u8>,
    pub expected_payload_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthzTupleBatchWriteOptions {
    pub authz_realm_id: String,
    pub operation_id: Option<String>,
    pub expected_revision: Option<i64>,
    pub schema_binding_precondition: Option<AuthzSchemaBindingPrecondition>,
}

#[derive(Debug, Clone)]
pub struct AuthzTupleBatchWriteOutcome {
    pub records: Vec<AuthzTupleRecord>,
    pub replayed: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum AuthzTupleBatchWriteError {
    #[error("authorization batch operation identity was already used for a different request")]
    OperationConflict,
    #[error(
        "authorization revision conflict: expected revision {expected}, current revision is {actual}"
    )]
    RevisionConflict { expected: i64, actual: i64 },
    #[error("authorization schema binding changed while validating the tuple batch")]
    SchemaBindingChanged,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub id: i64,
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub name: String,
    pub kind: String,
    pub selector: JsonValue,
    pub extractor: JsonValue,
    pub authorization_mode: String,
    pub build_policy: JsonValue,
    pub enabled: bool,
    pub version: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinitionEvent {
    pub id: i64,
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub bucket_name: String,
    pub index_id: i64,
    pub index_name: String,
    pub event_type: String,
    pub index_version: i64,
    pub mutation_id: uuid::Uuid,
    pub definition: JsonValue,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDiagnostic {
    pub id: i64,
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub bucket_name: String,
    pub index_id: Option<i64>,
    pub index_name: String,
    pub object_key: String,
    pub version_id: Option<uuid::Uuid>,
    pub severity: String,
    pub code: String,
    pub message: String,
    pub details: JsonValue,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppDetails {
    pub id: i64,
    pub client_secret_encrypted: Vec<u8>,
    pub tenant_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    pub id: i64,
    pub task_type: crate::tasks::TaskType,
    pub payload: JsonValue,
    pub priority: i32,
    pub status: crate::tasks::TaskStatus,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub scheduled_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct TaskPage {
    pub tasks: Vec<TaskRecord>,
    pub next_tuple_key: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TaskLeaseTarget {
    partition_family: String,
    partition_id: String,
    source_cursor: u128,
}

#[derive(Debug, Clone)]
pub struct HfIngestionJob {
    pub key_id: i64,
    pub tenant_id: i64,
    pub requester_app_id: i64,
    pub repo: String,
    pub revision: String,
    pub target_bucket: String,
    pub target_region: String,
    pub target_prefix: String,
    pub include_globs: Vec<String>,
    pub exclude_globs: Vec<String>,
}

fn object_version_record_hash(input: ObjectVersionRecordHashInput<'_>) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&input.tenant_id.to_le_bytes());
    hasher.update(&input.bucket_id.to_le_bytes());
    hasher.update(input.key.as_bytes());
    hasher.update(input.version_id.as_bytes());
    hasher.update(input.mutation_id.as_bytes());
    hasher.update(input.content_hash.as_bytes());
    hasher.update(&input.size.to_le_bytes());
    hasher.update(input.etag.as_bytes());
    if let Some(content_type) = input.content_type {
        hasher.update(content_type.as_bytes());
    }
    hasher.update(&[0]);
    if let Some(storage_class) = input.storage_class {
        hasher.update(storage_class.as_bytes());
    }
    hasher.update(&[0]);
    hasher.update(input.user_metadata_hash.as_bytes());
    hasher.update(input.index_policy_snapshot.as_bytes());
    hasher.update(&input.authz_revision.to_le_bytes());
    hasher.update(&[u8::from(input.delete_marker)]);
    hasher.finalize().to_hex().to_string()
}

fn user_metadata_hash(user_meta: Option<&JsonValue>) -> String {
    let Some(user_meta) = user_meta else {
        return blake3::hash(&[]).to_hex().to_string();
    };
    blake3::hash(&canonical_json_bytes(user_meta))
        .to_hex()
        .to_string()
}

fn is_retryable_partition_fence_error(error: &anyhow::Error) -> bool {
    if crate::core_store::is_retryable_mutation_conflict(error) {
        return true;
    }
    let message = error.to_string();
    message.contains("generation mismatch")
        || message.contains("target mismatch")
        || message.contains("must be absent")
        || message.contains("must be present")
        || message.contains("CAS lock was not acquired")
        || message.contains("stale")
}

fn canonical_json_bytes(value: &JsonValue) -> Vec<u8> {
    match value {
        JsonValue::Null => b"null".to_vec(),
        JsonValue::Bool(value) => {
            if *value {
                b"true".to_vec()
            } else {
                b"false".to_vec()
            }
        }
        JsonValue::Number(value) => value.to_string().into_bytes(),
        JsonValue::String(value) => serde_json::to_vec(value).unwrap_or_default(),
        JsonValue::Array(values) => {
            let mut out = vec![b'['];
            for (idx, value) in values.iter().enumerate() {
                if idx > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(&canonical_json_bytes(value));
            }
            out.push(b']');
            out
        }
        JsonValue::Object(values) => {
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort();
            let mut out = vec![b'{'];
            for (idx, key) in keys.into_iter().enumerate() {
                if idx > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(&serde_json::to_vec(key).unwrap_or_default());
                out.push(b':');
                out.extend_from_slice(&canonical_json_bytes(&values[key]));
            }
            out.push(b'}');
            out
        }
    }
}

mod helpers;
mod index_definition_lifecycle;
mod indexes;
mod lifecycle;
mod models;
mod objects;
mod partitioning;
mod streams;
mod tasks;
mod tenancy;

use helpers::*;
pub use index_definition_lifecycle::{IndexDefinitionMutation, IndexDefinitionMutationOutcome};
pub(crate) use objects::ObjectBatchCreateInput;
pub use objects::ObjectCreateOptions;

#[cfg(test)]
mod tests;
