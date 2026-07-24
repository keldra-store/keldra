use crate::{
    access_control, auth, bucket_journal,
    core_store::{
        AppendStreamRecord as CoreAppendStreamRecord, AuthzScopeRef, CoreBoundarySchema,
        CoreBoundarySource, CoreBoundaryValue, CoreByteRange, CoreManifestLocator,
        CoreMutationPrecondition, CoreObjectRef, CorePrefetchPolicy, CoreStore, CoreTransaction,
        GetBlob, PutBlob, SealStreamSegment, WriteLogicalFilePathRequest, WriteLogicalFileRequest,
        core_object_ref_from_logical_file_write, decode_core_object_ref_target,
        decode_manifest_locator_proto, encode_core_object_ref_target,
        encode_manifest_locator_proto,
    },
    error_codes::AnvilErrorCode,
    formats::writer::WriterFamily,
    object_links,
    observability::{
        OBJECT_READ_LATENCY, OBJECT_WRITE_LATENCY, Observability, PREFIX_LIST_LATENCY,
        RESERVED_NAMESPACE_REJECTION_COUNT,
    },
    permissions::AnvilAction,
    persistence::{Bucket, MetadataMutationReceipt, Object, Persistence},
    routing::{self, CrossRegionRoutingPolicy},
    storage::Storage,
    validation, watch_log,
};
use anyhow::{Result as AnyhowResult, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::{Stream, StreamExt};
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::path::Path;
use std::pin::Pin;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use tonic::metadata::MetadataValue;
use tracing::info;

mod batch_write;
mod write_visibility;
pub(crate) use batch_write::ObjectBatchPut;
pub use write_visibility::{
    AuthzMaterializationVisibility, AuthzRevisionVisibility, BoundaryExtractionVisibility,
    IndexMaintenanceVisibility, IndexPolicySnapshotVisibility, ObjectWriteOptions,
    ObjectWriteVisibility, WatchVisibility,
};

#[derive(Debug, Clone)]
pub struct ObjectManager {
    persistence: Persistence,
    storage: Storage,
    core_store: CoreStore,
    region: String,
    cross_region_routing_policy: CrossRegionRoutingPolicy,
    signing_key: Vec<u8>,
    observability: Observability,
}

#[derive(Debug, Clone)]
pub struct ComposeSource {
    pub bucket_name: String,
    pub object_key: String,
    pub version_id: Option<uuid::Uuid>,
}

#[derive(Debug, Clone)]
pub struct CompleteMultipartPart {
    pub part_number: i32,
    pub etag: String,
}

#[derive(Debug, Clone)]
pub struct InitiateMultipartUploadResult {
    pub upload_id: uuid::Uuid,
    pub receipt: MetadataMutationReceipt,
}

#[derive(Debug, Clone)]
pub struct UploadPartResult {
    pub etag: String,
    pub payload_hash: String,
    pub receipt: MetadataMutationReceipt,
}

#[derive(Debug, Clone)]
pub struct AbortMultipartUploadResult {
    pub upload_id: uuid::Uuid,
    pub receipt: MetadataMutationReceipt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectLinkReadMode {
    Follow,
    Metadata,
}

static DEFERRED_OBJECT_MAINTENANCE: OnceLock<Mutex<HashMap<(i64, i64), HashSet<String>>>> =
    OnceLock::new();
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ObjectReadConsistency {
    #[default]
    Latest,
    AtRootGeneration(u64),
    AtAuthzRevision(i64),
}

impl ObjectReadConsistency {
    pub fn root_generation(self) -> Option<u64> {
        match self {
            Self::AtRootGeneration(generation) => Some(generation),
            Self::Latest | Self::AtAuthzRevision(_) => None,
        }
    }

    pub fn authz_revision(self) -> Option<i64> {
        match self {
            Self::AtAuthzRevision(revision) => Some(revision),
            Self::Latest | Self::AtRootGeneration(_) => None,
        }
    }
}

pub struct ObjectReadResult {
    pub object: Object,
    pub stream: Pin<Box<dyn Stream<Item = Result<Vec<u8>, Status>> + Send + 'static>>,
    pub followed_link: Option<object_links::FollowedObjectLink>,
    pub range_start: u64,
}

#[derive(Debug, Clone)]
pub struct AppendStreamRecordRead {
    pub record_sequence: u64,
    pub payload_hash: String,
    pub payload_size: i64,
    pub content_type: Option<String>,
    pub user_metadata: Option<JsonValue>,
    pub authenticated_principal: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub payload: Option<Vec<u8>>,
}

struct ComposeStreamState {
    manager: ObjectManager,
    claims: auth::Claims,
    sources: std::vec::IntoIter<ComposeSource>,
    current: Option<Pin<Box<dyn Stream<Item = Result<Vec<u8>, Status>> + Send + 'static>>>,
}

pub fn transaction_principal_from_claims(claims: &auth::Claims) -> String {
    format!("tenant/{}/principal/{}", claims.tenant_id, claims.sub)
}

#[derive(Debug, Clone)]
pub struct ObjectHeadResult {
    pub object: Object,
    pub followed_link: Option<object_links::FollowedObjectLink>,
}

pub(crate) struct ObjectMutationPreconditionSnapshot {
    pub(crate) object: Option<Object>,
    pub(crate) precondition: CoreMutationPrecondition,
}

#[derive(Debug, Clone)]
pub struct AppendStreamRecordResult {
    pub record_sequence: u64,
    pub payload_hash: String,
    pub payload_size: i64,
    pub content_type: Option<String>,
    pub user_metadata: Option<JsonValue>,
    pub receipt: MetadataMutationReceipt,
}

#[derive(Debug, Clone)]
pub struct CreateAppendStreamResult {
    pub stream_id: uuid::Uuid,
    pub receipt: MetadataMutationReceipt,
}

#[derive(Debug, Clone)]
pub struct SealAppendStreamResult {
    pub record_count: u64,
    pub segment_hash: String,
    pub receipt: MetadataMutationReceipt,
}

#[derive(Debug, Clone)]
pub struct ManifestCasResult {
    pub revision: u64,
    pub manifest_hash: String,
    pub receipt: MetadataMutationReceipt,
}

impl ObjectManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        persistence: Persistence,
        storage: Storage,
        core_store: CoreStore,
        region: String,
        cross_region_routing_policy: CrossRegionRoutingPolicy,
        signing_key: Vec<u8>,
        observability: Observability,
    ) -> Self {
        Self {
            persistence,
            storage,
            core_store,
            region,
            cross_region_routing_policy,
            signing_key,
            observability,
        }
    }

    fn record_reserved_namespace_rejection(&self, operation: &'static str) {
        self.observability.increment_counter(
            RESERVED_NAMESPACE_REJECTION_COUNT,
            &[("api", "native"), ("operation", operation)],
        );
    }

    async fn object_write_boundary_values_from_file(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        object_key: &str,
        content_type: Option<&str>,
        user_metadata: Option<&JsonValue>,
        payload_path: &Path,
        payload_len: u64,
    ) -> Result<Vec<CoreBoundaryValue>, Status> {
        let boundary_schema_key =
            crate::core_store::boundary_schema_bucket_key(tenant_id, bucket_name);
        let Some(schema) = self
            .core_store
            .read_boundary_schema(&boundary_schema_key)
            .await
            .map_err(|error| Status::internal(error.to_string()))?
        else {
            return Ok(Vec::new());
        };
        let requires_body = schema.dimensions.iter().any(|dimension| {
            matches!(
                &dimension.source,
                CoreBoundarySource::BodyJsonPointer { .. }
            )
        });
        if !requires_body {
            return extract_object_boundary_values(
                &schema,
                tenant_id,
                bucket_name,
                object_key,
                content_type,
                user_metadata,
                payload_len,
                &[],
            )
            .map_err(|error| Status::invalid_argument(error.to_string()));
        }
        if let Some(limit) = schema
            .dimensions
            .iter()
            .filter_map(|dimension| {
                if let CoreBoundarySource::BodyJsonPointer { max_body_bytes, .. } =
                    &dimension.source
                {
                    Some(*max_body_bytes)
                } else {
                    None
                }
            })
            .min()
            && payload_len > limit
        {
            return Err(Status::invalid_argument(format!(
                "{}: boundary body exceeds {} bytes",
                AnvilErrorCode::BoundaryExtractorBodyTooLarge.as_str(),
                limit
            )));
        }
        let payload = tokio::fs::read(payload_path)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        extract_object_boundary_values(
            &schema,
            tenant_id,
            bucket_name,
            object_key,
            content_type,
            user_metadata,
            payload_len,
            &payload,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))
    }

    async fn object_write_boundary_values_from_hints(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        object_key: &str,
        content_type: Option<&str>,
        user_metadata: Option<&JsonValue>,
        payload_len: u64,
    ) -> Result<Vec<CoreBoundaryValue>, Status> {
        let boundary_schema_key =
            crate::core_store::boundary_schema_bucket_key(tenant_id, bucket_name);
        let Some(schema) = self
            .core_store
            .read_boundary_schema(&boundary_schema_key)
            .await
            .map_err(|error| Status::internal(error.to_string()))?
        else {
            return Ok(Vec::new());
        };
        if schema.dimensions.iter().any(|dimension| {
            matches!(
                &dimension.source,
                CoreBoundarySource::BodyJsonPointer { .. }
            )
        }) {
            return Err(Status::failed_precondition(format!(
                "{}: bucket boundary schema requires payload-derived boundary extraction; set boundary_extraction=BOUNDARY_EXTRACTION_PAYLOAD_NOW or supply non-payload boundary dimensions",
                AnvilErrorCode::BoundaryExtractorUnsupportedContentType.as_str()
            )));
        }
        extract_object_boundary_values(
            &schema,
            tenant_id,
            bucket_name,
            object_key,
            content_type,
            user_metadata,
            payload_len,
            &[],
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))
    }

    fn schedule_deferred_object_maintenance(&self, bucket: Bucket, object_key: &str) {
        let key = (bucket.tenant_id, bucket.id);
        let pending = DEFERRED_OBJECT_MAINTENANCE.get_or_init(|| Mutex::new(HashMap::new()));
        let should_spawn = {
            let mut guard = pending.lock().expect("deferred maintenance lock poisoned");
            match guard.entry(key) {
                Entry::Vacant(entry) => {
                    entry.insert(HashSet::from([object_key.to_owned()]));
                    true
                }
                Entry::Occupied(mut entry) => {
                    entry.get_mut().insert(object_key.to_owned());
                    false
                }
            }
        };
        if !should_spawn {
            return;
        }

        let persistence = self.persistence.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(250)).await;
            loop {
                let object_keys = DEFERRED_OBJECT_MAINTENANCE
                    .get()
                    .and_then(|pending| {
                        let mut pending = pending.lock().ok()?;
                        pending.get_mut(&key).map(std::mem::take)
                    })
                    .unwrap_or_default()
                    .into_iter()
                    .collect::<Vec<_>>();
                if let Err(error) = persistence
                    .enqueue_object_write_maintenance_for_keys_if_due(
                        &bucket,
                        &object_keys,
                        true,
                        true,
                    )
                    .await
                {
                    tracing::warn!(
                        tenant_id = bucket.tenant_id,
                        bucket_id = bucket.id,
                        bucket_name = %bucket.name,
                        %error,
                        "deferred object write maintenance failed"
                    );
                }

                let has_more = DEFERRED_OBJECT_MAINTENANCE
                    .get()
                    .and_then(|pending| {
                        let mut pending = pending.lock().ok()?;
                        let has_more = pending.get(&key).is_some_and(|keys| !keys.is_empty());
                        if !has_more {
                            pending.remove(&key);
                        }
                        Some(has_more)
                    })
                    .unwrap_or(false);
                if !has_more {
                    break;
                }
            }
        });
    }

    pub async fn put_object(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        object_key: &str,
        data_stream: impl Stream<Item = Result<Vec<u8>, Status>> + Unpin,
        options: ObjectWriteOptions,
    ) -> Result<Object, Status> {
        let _latency = self
            .observability
            .latency_guard(OBJECT_WRITE_LATENCY, &[("api", "native")]);
        info!(
            tenant_id = claims.tenant_id,
            bucket_name,
            object_key,
            principal = %claims.sub,
            "put_object called"
        );
        let tenant_id = claims.tenant_id;
        let transaction_id = options.transaction_id.clone();
        let total_start = std::time::Instant::now();
        if matches!(
            options.visibility.indexes,
            IndexMaintenanceVisibility::CaughtUp
        ) {
            return Err(Status::unimplemented(
                "INDEX_MAINTENANCE_CAUGHT_UP is reserved but not yet available for object writes; use INDEX_MAINTENANCE_ENQUEUED to synchronously enqueue catch-up work",
            ));
        }

        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(object_key) {
            self.record_reserved_namespace_rejection("put_object");
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !validation::is_valid_object_key(object_key) {
            return Err(Status::invalid_argument("Invalid object key"));
        }

        let step_start = std::time::Instant::now();
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        access_control::require_object_permission(
            &self.storage,
            claims,
            &bucket,
            object_key,
            "put",
        )
        .await?;
        crate::emit_test_timing(
            "object_manager.put_object get_tenant_bucket",
            step_start.elapsed(),
        );
        let step_start = std::time::Instant::now();
        let (temp_path, total_bytes, stream_hash) = self
            .storage
            .stream_to_temp_file(data_stream)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        crate::emit_test_timing(
            "object_manager.put_object stream_to_temp_file",
            step_start.elapsed(),
        );
        let total_bytes_u64 =
            u64::try_from(total_bytes).map_err(|_| Status::internal("Negative payload size"))?;
        let boundary_values = if options.visibility.requires_payload_boundary_extraction() {
            self.object_write_boundary_values_from_file(
                tenant_id,
                &bucket.name,
                object_key,
                options.content_type.as_deref(),
                options.user_metadata.as_ref(),
                &temp_path,
                total_bytes_u64,
            )
            .await?
        } else {
            self.object_write_boundary_values_from_hints(
                tenant_id,
                &bucket.name,
                object_key,
                options.content_type.as_deref(),
                options.user_metadata.as_ref(),
                total_bytes_u64,
            )
            .await?
        };
        let step_start = std::time::Instant::now();
        let effective_storage_class_id = self
            .core_store
            .resolve_storage_class_id(options.storage_class_id.as_deref())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let storage_class = self
            .core_store
            .get_storage_class(&effective_storage_class_id)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let pipeline_policy = self
            .core_store
            .pipeline_policy_for_storage_class(Some(effective_storage_class_id.as_str()))
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let core_mutation_id = uuid::Uuid::new_v4().to_string();
        let logical_file_id = format!(
            "tenant:{tenant_id}/bucket:{}/object:{object_key}",
            bucket.name
        );
        let inline_cap = storage_class
            .inline_payload_policy
            .effective_raw_payload_cap_bytes();
        let inline_eligible =
            storage_class.inline_payload_policy.enabled && total_bytes_u64 <= inline_cap;

        let (content_hash, shard_map) = if inline_eligible {
            let payload = tokio::fs::read(&temp_path)
                .await
                .map_err(|error| Status::internal(error.to_string()))?;
            let object_ref = self
                .core_store
                .put_blob_with_storage_class(
                    PutBlob {
                        logical_name: logical_file_id,
                        bytes: payload,
                        boundary_values: boundary_values.clone(),
                        region_id: self.region.clone(),
                        mutation_id: core_mutation_id,
                    },
                    Some(effective_storage_class_id.as_str()),
                )
                .await
                .map_err(core_store_status)?;
            let content_hash = object_ref.hash.clone();
            let shard_map = Some(
                object_data_target_to_shard_map(&ObjectDataTarget::ObjectRef(object_ref))
                    .map_err(|e| Status::internal(e.to_string()))?,
            );
            (content_hash, shard_map)
        } else {
            let logical_write = self
                .core_store
                .write_logical_file_path_with_locator(WriteLogicalFilePathRequest {
                    writer_family: WriterFamily::ObjectBlob.as_str().to_string(),
                    generation: 0,
                    logical_file_id,
                    source_path: temp_path.clone(),
                    source_len: total_bytes_u64,
                    source_hash: format!("sha256:{stream_hash}"),
                    range_hints: Vec::new(),
                    pipeline_policy,
                    trace_context: Default::default(),
                    boundary_values: boundary_values.clone(),
                    mutation_id: core_mutation_id,
                    region_id: self.region.clone(),
                })
                .await
                .map_err(core_store_status)?;
            let content_hash = logical_write.manifest.content_hash.clone();
            let shard_map = Some(
                object_data_target_to_shard_map(&ObjectDataTarget::LogicalFile(
                    logical_write.locator,
                ))
                .map_err(|e| Status::internal(e.to_string()))?,
            );
            (content_hash, shard_map)
        };
        let io_start = Instant::now();
        let remove_result = tokio::fs::remove_file(&temp_path).await;
        crate::perf::record_io_duration(
            "object_manager",
            "remove_temp_payload",
            &temp_path,
            total_bytes_u64,
            io_start.elapsed(),
        );
        if let Err(error) = remove_result {
            tracing::warn!(
                path = %temp_path.display(),
                %error,
                "failed to remove non-authoritative staged object payload"
            );
        }
        crate::emit_test_timing(
            "object_manager.put_object core_store_write_logical_file_path",
            step_start.elapsed(),
        );

        let step_start = std::time::Instant::now();
        let object = self
            .persistence
            .create_object_with_storage_class_with_options(
                tenant_id,
                bucket.id,
                object_key,
                &content_hash,
                total_bytes,
                &content_hash,
                options.content_type.as_deref(),
                options.user_metadata,
                shard_map,
                None,
                transaction_id.as_deref(),
                options.transaction_principal.as_deref(),
                Some(effective_storage_class_id),
                options.visibility.persistence_options(),
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        crate::emit_test_timing(
            "object_manager.put_object persistence_create_object",
            step_start.elapsed(),
        );
        if transaction_id.is_none() {
            if options.visibility.defers_write_maintenance() {
                self.schedule_deferred_object_maintenance(bucket.clone(), object_key);
            }
            if options.visibility.requires_authz_materialization() {
                let step_start = std::time::Instant::now();
                access_control::grant_object_defaults(
                    &self.persistence,
                    &bucket,
                    object_key,
                    "grant object parent bucket relation",
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
                crate::emit_test_timing(
                    "object_manager.put_object grant_object_defaults",
                    step_start.elapsed(),
                );
            }
        }
        crate::emit_test_timing("object_manager.put_object total", total_start.elapsed());

        Ok(object)
    }

    pub async fn initiate_multipart_upload(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        object_key: &str,
        transaction_id: Option<&str>,
        transaction_principal: Option<&str>,
    ) -> Result<InitiateMultipartUploadResult, Status> {
        self.validate_write_request(claims, bucket_name, object_key)
            .await?;
        let tenant_id = claims.tenant_id;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;

        let mutation = if let Some(transaction_id) = transaction_id {
            self.persistence
                .create_multipart_upload_in_transaction(
                    tenant_id,
                    bucket.id,
                    object_key,
                    transaction_id,
                    transaction_principal.ok_or_else(|| {
                        Status::invalid_argument("transaction principal is required")
                    })?,
                )
                .await
        } else {
            self.persistence
                .create_multipart_upload(tenant_id, bucket.id, object_key)
                .await
        }
        .map_err(|e| Status::internal(e.to_string()))?;
        Ok(InitiateMultipartUploadResult {
            upload_id: mutation.upload.upload_id,
            receipt: mutation.receipt,
        })
    }

    pub async fn upload_part(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        object_key: &str,
        upload_id: uuid::Uuid,
        part_number: i32,
        data_stream: impl Stream<Item = Result<Vec<u8>, Status>> + Unpin,
        transaction_id: Option<&str>,
        transaction_principal: Option<&str>,
    ) -> Result<UploadPartResult, Status> {
        self.validate_write_request(claims, bucket_name, object_key)
            .await?;
        let tenant_id = claims.tenant_id;
        validate_multipart_part_number(part_number)?;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        let upload = if let Some(transaction_id) = transaction_id {
            self.persistence
                .get_active_multipart_upload_in_transaction(
                    tenant_id,
                    bucket.id,
                    object_key,
                    upload_id,
                    transaction_id,
                    transaction_principal.ok_or_else(|| {
                        Status::invalid_argument("transaction principal is required")
                    })?,
                )
                .await
        } else {
            self.persistence
                .get_active_multipart_upload(tenant_id, bucket.id, object_key, upload_id)
                .await
        }
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found("Multipart upload not found"))?;

        let (temp_path, bytes, stream_hash) = self
            .storage
            .stream_to_temp_file(data_stream)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let bytes_u64 =
            u64::try_from(bytes).map_err(|_| Status::internal("Negative multipart part size"))?;
        let storage_class_id = self
            .core_store
            .resolve_storage_class_id(None)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let pipeline_policy = self
            .core_store
            .pipeline_policy_for_storage_class(Some(storage_class_id.as_str()))
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let write_result = self
            .core_store
            .write_logical_file_path_with_locator(WriteLogicalFilePathRequest {
                writer_family: WriterFamily::ObjectBlob.as_str().to_string(),
                generation: 0,
                logical_file_id: format!(
                    "tenant:{tenant_id}/bucket:{}/multipart:{upload_id}/part:{part_number}",
                    bucket.name
                ),
                source_path: temp_path.clone(),
                source_len: bytes_u64,
                source_hash: format!("sha256:{stream_hash}"),
                range_hints: Vec::new(),
                pipeline_policy,
                trace_context: Default::default(),
                boundary_values: Vec::new(),
                mutation_id: uuid::Uuid::new_v4().to_string(),
                region_id: self.region.clone(),
            })
            .await;
        let io_start = Instant::now();
        let remove_result = tokio::fs::remove_file(&temp_path).await;
        crate::perf::record_io_duration(
            "object_manager",
            "remove_temp_multipart_part",
            &temp_path,
            bytes_u64,
            io_start.elapsed(),
        );
        let write = write_result.map_err(core_store_status)?;
        if let Err(error) = remove_result {
            tracing::warn!(
                path = %temp_path.display(),
                %error,
                "failed to remove non-authoritative staged multipart payload"
            );
        }
        let object_ref = core_object_ref_from_logical_file_write(&write);
        let content_hash = object_ref.hash.clone();

        let mutation = if let Some(transaction_id) = transaction_id {
            self.persistence
                .upsert_multipart_part_in_transaction(
                    upload.id,
                    part_number,
                    object_ref,
                    bytes as i64,
                    &content_hash,
                    transaction_id,
                    transaction_principal.ok_or_else(|| {
                        Status::invalid_argument("transaction principal is required")
                    })?,
                )
                .await
        } else {
            self.persistence
                .upsert_multipart_part(
                    upload.id,
                    part_number,
                    object_ref,
                    bytes as i64,
                    &content_hash,
                )
                .await
        }
        .map_err(|e| Status::internal(e.to_string()))?;
        Ok(UploadPartResult {
            etag: mutation.part.etag,
            payload_hash: content_hash,
            receipt: mutation.receipt,
        })
    }

    pub async fn complete_multipart_upload(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        object_key: &str,
        upload_id: uuid::Uuid,
        parts: Vec<CompleteMultipartPart>,
        transaction_id: Option<&str>,
        transaction_principal: Option<&str>,
    ) -> Result<Object, Status> {
        self.validate_write_request(claims, bucket_name, object_key)
            .await?;
        let tenant_id = claims.tenant_id;
        if parts.is_empty() {
            return Err(Status::invalid_argument(
                "CompleteMultipartUpload requires at least one part",
            ));
        }
        for part in &parts {
            validate_multipart_part_number(part.part_number)?;
        }

        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        let upload = if let Some(transaction_id) = transaction_id {
            self.persistence
                .get_active_multipart_upload_in_transaction(
                    tenant_id,
                    bucket.id,
                    object_key,
                    upload_id,
                    transaction_id,
                    transaction_principal.ok_or_else(|| {
                        Status::invalid_argument("transaction principal is required")
                    })?,
                )
                .await
        } else {
            self.persistence
                .get_active_multipart_upload(tenant_id, bucket.id, object_key, upload_id)
                .await
        }
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found("Multipart upload not found"))?;
        let stored_parts = if let Some(transaction_id) = transaction_id {
            self.persistence
                .list_multipart_parts_in_transaction(
                    upload.id,
                    transaction_id,
                    transaction_principal.ok_or_else(|| {
                        Status::invalid_argument("transaction principal is required")
                    })?,
                )
                .await
        } else {
            self.persistence.list_multipart_parts(upload.id).await
        }
        .map_err(|e| Status::internal(e.to_string()))?;

        let mut ordered_part_refs = Vec::with_capacity(parts.len());
        for expected in parts {
            let stored = stored_parts
                .iter()
                .find(|part| part.part_number == expected.part_number)
                .ok_or_else(|| {
                    Status::invalid_argument("Complete request references missing part")
                })?;
            if trim_s3_etag(&stored.etag) != trim_s3_etag(&expected.etag) {
                return Err(Status::invalid_argument(
                    "Complete request part ETag mismatch",
                ));
            }
            ordered_part_refs.push(stored.object_ref.clone());
        }

        let core_store = self.core_store.clone();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            for object_ref in ordered_part_refs {
                let result = core_store
                    .read_object_ref_chunks(object_ref, None, 1024 * 64, |chunk| {
                        let tx = tx.clone();
                        async move {
                            tx.send(Ok(chunk))
                                .await
                                .map_err(|_| anyhow!("multipart completion stream closed"))
                        }
                    })
                    .await;
                if let Err(error) = result {
                    let _ = tx.send(Err(Status::internal(error.to_string()))).await;
                    return;
                }
            }
        });

        let object = self
            .put_object(
                claims,
                bucket_name,
                object_key,
                ReceiverStream::new(rx),
                ObjectWriteOptions {
                    transaction_id: transaction_id.map(ToOwned::to_owned),
                    transaction_principal: transaction_principal.map(ToOwned::to_owned),
                    visibility: ObjectWriteVisibility::strict(),
                    ..Default::default()
                },
            )
            .await?;

        let completion = if let Some(transaction_id) = transaction_id {
            self.persistence
                .complete_multipart_upload_in_transaction(
                    upload.id,
                    transaction_id,
                    transaction_principal.ok_or_else(|| {
                        Status::invalid_argument("transaction principal is required")
                    })?,
                )
                .await
        } else {
            self.persistence.complete_multipart_upload(upload.id).await
        }
        .map_err(|e| Status::internal(e.to_string()))?;
        if !completion.completed {
            return Err(Status::not_found("Multipart upload not found"));
        }

        Ok(object)
    }

    pub async fn abort_multipart_upload(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        object_key: &str,
        upload_id: uuid::Uuid,
        transaction_id: Option<&str>,
        transaction_principal: Option<&str>,
    ) -> Result<AbortMultipartUploadResult, Status> {
        self.validate_write_request(claims, bucket_name, object_key)
            .await?;
        let tenant_id = claims.tenant_id;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        let mutation = if let Some(transaction_id) = transaction_id {
            self.persistence
                .abort_multipart_upload_in_transaction(
                    tenant_id,
                    bucket.id,
                    object_key,
                    upload_id,
                    transaction_id,
                    transaction_principal.ok_or_else(|| {
                        Status::invalid_argument("transaction principal is required")
                    })?,
                )
                .await
        } else {
            self.persistence
                .abort_multipart_upload(tenant_id, bucket.id, object_key, upload_id)
                .await
        }
        .map_err(|e| Status::internal(e.to_string()))?;
        if let Some(receipt) = mutation.receipt {
            Ok(AbortMultipartUploadResult { upload_id, receipt })
        } else {
            Err(Status::not_found("Multipart upload not found"))
        }
    }

    pub async fn list_multipart_parts(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        object_key: &str,
        upload_id: uuid::Uuid,
        part_number_marker: i32,
        limit: i32,
    ) -> Result<crate::persistence::MultipartPartsPage, Status> {
        self.validate_write_request(claims, bucket_name, object_key)
            .await?;
        let tenant_id = claims.tenant_id;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        let upload = self
            .persistence
            .get_active_multipart_upload(tenant_id, bucket.id, object_key, upload_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Multipart upload not found"))?;
        self.persistence
            .list_multipart_parts_page(upload.id, part_number_marker, limit)
            .await
            .map_err(|e| Status::internal(e.to_string()))
    }

    pub async fn list_multipart_uploads(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        prefix: &str,
        key_marker: &str,
        upload_id_marker: Option<uuid::Uuid>,
        limit: i32,
    ) -> Result<crate::persistence::MultipartUploadsPage, Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(prefix) {
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !prefix.is_empty() && !validation::is_valid_object_key(prefix) {
            return Err(Status::invalid_argument("Invalid object key prefix"));
        }
        access_control::require_action(
            &self.storage,
            &self.persistence,
            claims,
            AnvilAction::ObjectList,
            bucket_name,
        )
        .await?;

        let tenant_id = claims.tenant_id;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        self.persistence
            .list_active_multipart_uploads(bucket.id, prefix, key_marker, upload_id_marker, limit)
            .await
            .map_err(|e| Status::internal(e.to_string()))
    }

    pub async fn resolve_prefix_watch_scope(
        &self,
        claims: auth::Claims,
        bucket_name: &str,
        prefix: &str,
    ) -> Result<i64, Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(prefix) {
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !prefix.is_empty() && !validation::is_valid_object_key(prefix) {
            return Err(Status::invalid_argument("Invalid object key prefix"));
        }
        let bucket = self
            .get_tenant_bucket(claims.tenant_id, bucket_name)
            .await?;
        access_control::require_bucket_permission(&self.storage, &claims, &bucket, "list_objects")
            .await?;
        Ok(bucket.id)
    }

    pub async fn create_append_stream(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        stream_key: &str,
        transaction_id: Option<&str>,
        transaction_principal: Option<&str>,
    ) -> Result<CreateAppendStreamResult, Status> {
        self.validate_object_path_only(bucket_name, stream_key)?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            claims,
            AnvilAction::StreamCreate,
            &format!("{bucket_name}/{stream_key}"),
        )
        .await?;
        let tenant_id = claims.tenant_id;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        let mutation = if let Some(transaction_id) = transaction_id {
            let transaction_principal = transaction_principal.ok_or_else(|| {
                Status::invalid_argument(
                    "transaction principal is required for append stream create",
                )
            })?;
            self.persistence
                .create_append_stream_in_transaction(
                    tenant_id,
                    bucket.id,
                    &bucket.name,
                    stream_key,
                    transaction_id,
                    transaction_principal,
                )
                .await
        } else {
            self.persistence
                .create_append_stream(tenant_id, bucket.id, &bucket.name, stream_key)
                .await
        }
        .map_err(|e| Status::internal(e.to_string()))?;
        if transaction_id.is_none() {
            access_control::grant_stream_defaults(
                &self.persistence,
                &bucket,
                stream_key,
                &claims.sub,
                &claims.sub,
                "grant creator stream owner",
            )
            .await
            .map_err(core_store_status)?;
        }
        Ok(CreateAppendStreamResult {
            stream_id: mutation.stream.stream_id,
            receipt: mutation.receipt,
        })
    }

    pub async fn append_stream_record(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        stream_key: &str,
        stream_id: uuid::Uuid,
        payload: Vec<u8>,
        content_type: Option<String>,
        user_metadata: Option<JsonValue>,
        transaction_id: Option<&str>,
    ) -> Result<AppendStreamRecordResult, Status> {
        self.validate_object_path_only(bucket_name, stream_key)?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            claims,
            AnvilAction::StreamAppend,
            &format!("{bucket_name}/{stream_key}"),
        )
        .await?;
        let tenant_id = claims.tenant_id;
        let authenticated_principal = transaction_principal_from_claims(claims);
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        let stream = if let Some(transaction_id) = transaction_id {
            self.persistence
                .get_active_append_stream_in_transaction(
                    tenant_id,
                    bucket.id,
                    stream_key,
                    stream_id,
                    transaction_id,
                    &authenticated_principal,
                )
                .await
        } else {
            self.persistence
                .get_active_append_stream(tenant_id, bucket.id, stream_key, stream_id)
                .await
        }
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found("Append stream not found"))?;

        let payload_size = payload.len() as i64;
        let core_stream_payload = payload.clone();
        let stream_payload_mutation_id = uuid::Uuid::new_v4().to_string();
        let object_ref = self
            .core_store
            .write_logical_file_ref(WriteLogicalFileRequest {
                writer_family: WriterFamily::Stream.as_str().to_string(),
                generation: 0,
                logical_file_id: format!(
                    "tenant:{tenant_id}/bucket:{}/append:{stream_key}/record:{}",
                    bucket.name, stream_payload_mutation_id
                ),
                source: payload,
                range_hints: Vec::new(),
                pipeline_policy: Default::default(),
                trace_context: Default::default(),
                boundary_values: Vec::new(),
                region_id: self.region.clone(),
                mutation_id: stream_payload_mutation_id.clone(),
            })
            .await
            .map_err(core_store_status)?;
        let payload_hash = object_ref.hash.clone();
        self.core_store
            .append_stream_authenticated(
                CoreAppendStreamRecord {
                    stream_id: core_append_stream_id(tenant_id, bucket.id, stream_id),
                    partition_id: core_append_stream_partition_id(tenant_id, bucket.id),
                    record_kind: "append_stream.record".to_string(),
                    payload: core_stream_payload,
                    content_type: content_type.clone(),
                    user_metadata_json: user_metadata
                        .as_ref()
                        .map(serde_json::Value::to_string)
                        .unwrap_or_else(|| "{}".to_string()),
                    fence: None,
                    transaction_id: transaction_id.map(ToOwned::to_owned),
                    idempotency_key: Some(format!(
                        "append-stream:{tenant_id}:{}:{stream_id}:{}",
                        bucket.id, stream_payload_mutation_id
                    )),
                },
                &authenticated_principal,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let mutation = if let Some(transaction_id) = transaction_id {
            self.persistence
                .append_stream_record_in_transaction(
                    tenant_id,
                    bucket.id,
                    &stream,
                    object_ref,
                    payload_size,
                    content_type.clone(),
                    user_metadata.clone(),
                    transaction_id,
                    &authenticated_principal,
                )
                .await
        } else {
            self.persistence
                .append_stream_record(
                    tenant_id,
                    bucket.id,
                    &stream,
                    object_ref,
                    payload_size,
                    content_type.clone(),
                    user_metadata.clone(),
                    &authenticated_principal,
                )
                .await
        }
        .map_err(|e| Status::internal(e.to_string()))?;

        Ok(AppendStreamRecordResult {
            record_sequence: u64::try_from(mutation.record.record_sequence)
                .map_err(|_| Status::internal("Invalid record sequence"))?,
            payload_hash,
            payload_size,
            content_type,
            user_metadata,
            receipt: mutation.receipt,
        })
    }

    pub async fn seal_append_stream_segment(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        stream_key: &str,
        stream_id: uuid::Uuid,
        transaction_id: Option<&str>,
        transaction_principal: Option<&str>,
    ) -> Result<SealAppendStreamResult, Status> {
        self.validate_object_path_only(bucket_name, stream_key)?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            claims,
            AnvilAction::StreamSealSegment,
            &format!("{bucket_name}/{stream_key}"),
        )
        .await?;
        let tenant_id = claims.tenant_id;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        let stream = if let Some(transaction_id) = transaction_id {
            let transaction_principal = transaction_principal.ok_or_else(|| {
                Status::invalid_argument("transaction principal is required for append stream seal")
            })?;
            self.persistence
                .get_active_append_stream_in_transaction(
                    tenant_id,
                    bucket.id,
                    stream_key,
                    stream_id,
                    transaction_id,
                    transaction_principal,
                )
                .await
        } else {
            self.persistence
                .get_active_append_stream(tenant_id, bucket.id, stream_key, stream_id)
                .await
        }
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found("Append stream not found"))?;
        let transaction = transaction_id.zip(transaction_principal);
        let has_records = self
            .persistence
            .append_stream_has_records(&stream, transaction)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if !has_records {
            return Err(Status::failed_precondition(
                "Append stream has no records to seal",
            ));
        }

        let core_segment = self
            .core_store
            .seal_stream_segment(SealStreamSegment {
                stream_id: core_append_stream_id(tenant_id, bucket.id, stream_id),
                partition_id: core_append_stream_partition_id(tenant_id, bucket.id),
                through_sequence: None,
                segment_kind: "append_stream.segment".to_string(),
                mutation_id: format!("append-stream-seal-{stream_id}-{}", uuid::Uuid::new_v4()),
            })
            .await
            .map_err(core_store_status)?;
        let segment_hash = core_segment.object_ref.hash.clone();
        let sealed = if let Some(transaction_id) = transaction_id {
            let transaction_principal = transaction_principal.ok_or_else(|| {
                Status::invalid_argument("transaction principal is required for append stream seal")
            })?;
            self.persistence
                .seal_append_stream_in_transaction(
                    tenant_id,
                    bucket.id,
                    &stream,
                    &segment_hash,
                    transaction_id,
                    transaction_principal,
                )
                .await
        } else {
            self.persistence
                .seal_append_stream(tenant_id, bucket.id, &stream, &segment_hash)
                .await
        }
        .map_err(|e| Status::internal(e.to_string()))?;
        let Some(receipt) = sealed.receipt else {
            return Err(Status::failed_precondition(
                "Append stream is already sealed",
            ));
        };

        Ok(SealAppendStreamResult {
            record_count: core_segment.record_count,
            segment_hash,
            receipt,
        })
    }

    pub async fn read_append_stream_records(
        &self,
        claims: auth::Claims,
        bucket_name: &str,
        stream_key: &str,
        stream_id: uuid::Uuid,
        after_sequence: u64,
        limit: u32,
        include_payload: bool,
        consistency: ObjectReadConsistency,
    ) -> Result<Vec<AppendStreamRecordRead>, Status> {
        self.validate_object_path_only(bucket_name, stream_key)?;
        let bucket = self
            .get_tenant_bucket(claims.tenant_id, bucket_name)
            .await?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::StreamRead,
            &format!("{bucket_name}/{stream_key}"),
        )
        .await?;
        let _stream = self
            .persistence
            .get_active_append_stream(claims.tenant_id, bucket.id, stream_key, stream_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Append stream not found"))?;
        let limit = if limit == 0 { 100 } else { limit.min(1000) } as usize;
        let read = crate::core_store::ReadStream {
            stream_id: core_append_stream_id(claims.tenant_id, bucket.id, stream_id),
            after_sequence,
            limit,
        };
        let mut records = match consistency.root_generation() {
            Some(root_generation) => {
                self.core_store
                    .read_stream_at_generation(read, root_generation)
                    .await
            }
            None => self.core_store.read_stream(read).await,
        }
        .map_err(|e| Status::internal(e.to_string()))?;
        records.sort_by_key(|record| record.sequence);

        let mut out = Vec::with_capacity(records.len());
        for record in records {
            let user_metadata = serde_json::from_str::<JsonValue>(&record.user_metadata_json)
                .ok()
                .filter(|value| value.is_object());
            let payload = if include_payload {
                Some(record.payload.clone())
            } else {
                None
            };
            out.push(AppendStreamRecordRead {
                record_sequence: record.sequence,
                payload_hash: record.payload_hash,
                payload_size: i64::try_from(record.payload.len())
                    .map_err(|_| Status::internal("Append record payload exceeds i64"))?,
                content_type: record.content_type,
                user_metadata,
                authenticated_principal: record.authenticated_principal,
                created_at: chrono::DateTime::parse_from_rfc3339(&record.created_at)
                    .map_err(|_| Status::internal("Invalid append record timestamp"))?
                    .with_timezone(&chrono::Utc),
                payload,
            });
        }
        Ok(out)
    }

    pub async fn compare_and_swap_manifest(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        manifest_key: &str,
        expected_revision: u64,
        manifest_json: &str,
        transaction_id: Option<&str>,
        transaction_principal: Option<&str>,
    ) -> Result<ManifestCasResult, Status> {
        self.validate_write_request(claims, bucket_name, manifest_key)
            .await?;
        let tenant_id = claims.tenant_id;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        let expected_revision = i64::try_from(expected_revision)
            .map_err(|_| Status::invalid_argument("expected_revision exceeds supported range"))?;
        let manifest: JsonValue = serde_json::from_str(manifest_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid manifest JSON: {}", e)))?;
        let manifest_bytes = canonical_json_bytes(&manifest)
            .map_err(|e| Status::internal(format!("Failed to encode manifest JSON: {}", e)))?;
        let manifest_hash = blake3::hash(&manifest_bytes).to_hex().to_string();

        let result = if let Some(transaction_id) = transaction_id {
            let transaction_principal = transaction_principal.ok_or_else(|| {
                Status::invalid_argument("transaction principal is required for manifest CAS")
            })?;
            self.persistence
                .compare_and_swap_manifest_in_transaction(
                    tenant_id,
                    bucket.id,
                    &bucket.name,
                    manifest_key,
                    expected_revision,
                    manifest,
                    &manifest_hash,
                    transaction_id,
                    transaction_principal,
                )
                .await
        } else {
            self.persistence
                .compare_and_swap_manifest(
                    tenant_id,
                    bucket.id,
                    &bucket.name,
                    manifest_key,
                    expected_revision,
                    manifest,
                    &manifest_hash,
                )
                .await
        }
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::failed_precondition("Manifest revision mismatch"))?;

        Ok(ManifestCasResult {
            revision: u64::try_from(result.revision)
                .map_err(|_| Status::internal("Invalid manifest revision"))?,
            manifest_hash: result.manifest_hash,
            receipt: result.receipt,
        })
    }
}

mod read;

fn normalized_list_limit(limit: i32) -> i32 {
    if limit <= 0 { 1000 } else { limit }
}

async fn collect_stream_bytes(
    mut stream: Pin<Box<dyn Stream<Item = Result<Vec<u8>, Status>> + Send + 'static>>,
) -> Result<Vec<u8>, Status> {
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk?);
    }
    Ok(bytes)
}

fn apply_json_merge_patch(target: &mut JsonValue, patch: JsonValue) {
    match patch {
        JsonValue::Object(patch_object) => {
            if !target.is_object() {
                *target = JsonValue::Object(serde_json::Map::new());
            }
            let target_object = target.as_object_mut().expect("target set to object");
            for (key, value) in patch_object {
                if value.is_null() {
                    target_object.remove(&key);
                } else {
                    apply_json_merge_patch(
                        target_object.entry(key).or_insert(JsonValue::Null),
                        value,
                    );
                }
            }
        }
        replacement => {
            *target = replacement;
        }
    }
}

fn validate_multipart_part_number(part_number: i32) -> Result<(), Status> {
    if (1..=10_000).contains(&part_number) {
        Ok(())
    } else {
        Err(Status::invalid_argument(
            "Multipart part number must be between 1 and 10000",
        ))
    }
}

enum ObjectDataTarget {
    LogicalFile(CoreManifestLocator),
    ObjectRef(CoreObjectRef),
}

fn object_data_target_to_shard_map(target: &ObjectDataTarget) -> AnyhowResult<JsonValue> {
    match target {
        ObjectDataTarget::LogicalFile(locator) => Ok(serde_json::json!({
            "schema": "anvil.core.object_data_target.v1",
            "kind": "logical_file",
            "target": URL_SAFE_NO_PAD.encode(encode_manifest_locator_proto(locator)?),
        })),
        ObjectDataTarget::ObjectRef(object_ref) => Ok(serde_json::json!({
            "schema": "anvil.core.object_data_target.v1",
            "kind": "object_ref",
            "target": encode_core_object_ref_target(object_ref)?,
        })),
    }
}

fn object_data_target_from_shard_map(value: &JsonValue) -> AnyhowResult<ObjectDataTarget> {
    if value.get("schema").and_then(JsonValue::as_str) == Some("anvil.core.object_data_target.v1") {
        let kind = value
            .get("kind")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| anyhow!("object data target kind is missing"))?;
        let target = value
            .get("target")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| anyhow!("object data target bytes are missing"))?;
        return match kind {
            "logical_file" => {
                let bytes = URL_SAFE_NO_PAD.decode(target)?;
                Ok(ObjectDataTarget::LogicalFile(
                    decode_manifest_locator_proto(&bytes)?,
                ))
            }
            "object_ref" => Ok(ObjectDataTarget::ObjectRef(decode_core_object_ref_target(
                target,
            )?)),
            other => bail!("unsupported CoreStore object logical-file target kind {other}"),
        };
    }
    bail!("object shard map is not a canonical CoreStore object data target");
}

fn canonical_json_bytes(value: &JsonValue) -> AnyhowResult<Vec<u8>> {
    serde_json::to_vec(&canonical_json(value)).map_err(Into::into)
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

fn extract_object_boundary_values(
    schema: &CoreBoundarySchema,
    tenant_id: i64,
    bucket_name: &str,
    object_key: &str,
    content_type: Option<&str>,
    user_metadata: Option<&JsonValue>,
    payload_len: u64,
    payload: &[u8],
) -> AnyhowResult<Vec<CoreBoundaryValue>> {
    let mut values = Vec::new();
    for dimension in &schema.dimensions {
        let (source_kind, raw_value) = match &dimension.source {
            CoreBoundarySource::UserMetadataJsonPointer { pointer } => (
                "user_metadata_json_pointer",
                user_metadata
                    .and_then(|metadata| metadata.pointer(pointer))
                    .cloned(),
            ),
            CoreBoundarySource::SystemMetadataField { field } => (
                "system_metadata_field",
                object_boundary_system_metadata(
                    tenant_id,
                    bucket_name,
                    object_key,
                    content_type,
                    payload_len,
                    field,
                ),
            ),
            CoreBoundarySource::PathTemplate { template } => (
                "path_template",
                extract_path_template_capture(template, object_key, &dimension.name),
            ),
            CoreBoundarySource::BodyJsonPointer {
                pointer,
                max_body_bytes,
            } => {
                if !content_type.is_some_and(is_json_content_type) {
                    bail!(
                        "{}: boundary dimension {} requires JSON content type",
                        AnvilErrorCode::BoundaryExtractorUnsupportedContentType.as_str(),
                        dimension.name
                    );
                }
                if payload.len() as u64 > *max_body_bytes {
                    bail!(
                        "{}: boundary dimension {} body exceeds {} bytes",
                        AnvilErrorCode::BoundaryExtractorBodyTooLarge.as_str(),
                        dimension.name,
                        max_body_bytes
                    );
                }
                let body: JsonValue = serde_json::from_slice(payload).map_err(|error| {
                    anyhow!(
                        "{}: boundary dimension {} body is not valid JSON: {error}",
                        AnvilErrorCode::BoundaryTypeMismatch.as_str(),
                        dimension.name
                    )
                })?;
                ("body_json_pointer", body.pointer(pointer).cloned())
            }
            CoreBoundarySource::WriterSuppliedBoundary {
                writer_family,
                field,
            } => {
                if writer_family != WriterFamily::ObjectBlob.as_str() {
                    bail!(
                        "{}: boundary dimension {} requires writer family {}, not {}",
                        AnvilErrorCode::BoundaryTypeMismatch.as_str(),
                        dimension.name,
                        writer_family,
                        WriterFamily::ObjectBlob.as_str()
                    );
                }
                (
                    "writer_supplied_boundary",
                    user_metadata.and_then(|metadata| {
                        metadata
                            .get("_anvil_writer_boundaries")
                            .and_then(|boundaries| boundaries.get(field))
                            .cloned()
                    }),
                )
            }
        };

        let Some(raw_value) = raw_value else {
            if dimension.required {
                bail!(
                    "{}: required boundary dimension {} is missing",
                    AnvilErrorCode::BoundaryRequiredMissing.as_str(),
                    dimension.name
                );
            }
            continue;
        };
        let value = normalise_boundary_value(&dimension.value_type, &raw_value)
            .map_err(|error| anyhow!("{} for dimension {}", error, dimension.name))?;
        values.push(CoreBoundaryValue {
            schema_generation: schema.generation,
            name: dimension.name.clone(),
            value_type: dimension.value_type.clone(),
            value,
            categories: dimension.categories.clone(),
            source_kind: source_kind.to_string(),
            required: dimension.required,
            max_values_per_block: dimension.max_values_per_block,
            placement_affinity: dimension.placement_affinity.clone(),
            compaction_scope: dimension.compaction_scope.clone(),
            shared_ranges_allowed: dimension.shared_ranges_allowed,
            shared_record_kinds: dimension.shared_record_kinds.clone(),
        });
    }
    Ok(values)
}

fn object_boundary_system_metadata(
    tenant_id: i64,
    bucket_name: &str,
    object_key: &str,
    content_type: Option<&str>,
    payload_len: u64,
    field: &str,
) -> Option<JsonValue> {
    match field {
        "tenant_id" => Some(JsonValue::Number(tenant_id.into())),
        "bucket_name" => Some(JsonValue::String(bucket_name.to_string())),
        "object_key" => Some(JsonValue::String(object_key.to_string())),
        "content_type" => content_type.map(|value| JsonValue::String(value.to_string())),
        "payload_length" => Some(JsonValue::Number(payload_len.into())),
        _ => None,
    }
}

fn is_json_content_type(content_type: &str) -> bool {
    let content_type = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .to_ascii_lowercase();
    content_type == "application/json" || content_type.ends_with("+json")
}

fn extract_path_template_capture(
    template: &str,
    object_key: &str,
    capture_name: &str,
) -> Option<JsonValue> {
    let template_segments = template
        .trim_start_matches('/')
        .split('/')
        .collect::<Vec<_>>();
    let object_segments = object_key
        .trim_start_matches('/')
        .split('/')
        .collect::<Vec<_>>();
    let mut captures = serde_json::Map::new();
    let mut object_index = 0usize;
    for segment in template_segments {
        if segment == "**" {
            break;
        }
        let object_segment = object_segments.get(object_index)?;
        object_index += 1;
        if let Some(capture) = segment
            .strip_prefix('{')
            .and_then(|value| value.strip_suffix('}'))
        {
            let name = capture.split(':').next().unwrap_or(capture);
            captures.insert(
                name.to_string(),
                JsonValue::String((*object_segment).to_string()),
            );
        } else if segment != *object_segment {
            return None;
        }
    }
    captures.remove(capture_name)
}

fn normalise_boundary_value(value_type: &str, value: &JsonValue) -> AnyhowResult<String> {
    match value_type {
        "string" => value.as_str().map(str::to_string).ok_or_else(|| {
            anyhow!(
                "{}: expected string boundary value",
                AnvilErrorCode::BoundaryTypeMismatch.as_str()
            )
        }),
        "uuid" => {
            let value = value.as_str().ok_or_else(|| {
                anyhow!(
                    "{}: expected uuid string boundary value",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            })?;
            let uuid = uuid::Uuid::parse_str(value).map_err(|_| {
                anyhow!(
                    "{}: expected canonical uuid boundary value",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            })?;
            Ok(uuid.to_string())
        }
        "u64" => value
            .as_u64()
            .map(|value| value.to_string())
            .or_else(|| {
                value
                    .as_str()?
                    .parse::<u64>()
                    .ok()
                    .map(|value| value.to_string())
            })
            .ok_or_else(|| {
                anyhow!(
                    "{}: expected u64 boundary value",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            }),
        "i64" => value
            .as_i64()
            .map(|value| value.to_string())
            .or_else(|| {
                value
                    .as_str()?
                    .parse::<i64>()
                    .ok()
                    .map(|value| value.to_string())
            })
            .ok_or_else(|| {
                anyhow!(
                    "{}: expected i64 boundary value",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            }),
        "date" => {
            let value = value.as_str().ok_or_else(|| {
                anyhow!(
                    "{}: expected date string boundary value",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            })?;
            let date = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| {
                anyhow!(
                    "{}: expected YYYY-MM-DD boundary date",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            })?;
            Ok(date.to_string())
        }
        "timestamp" => {
            let value = value.as_str().ok_or_else(|| {
                anyhow!(
                    "{}: expected timestamp string boundary value",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            })?;
            let timestamp = chrono::DateTime::parse_from_rfc3339(value).map_err(|_| {
                anyhow!(
                    "{}: expected RFC3339 boundary timestamp",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            })?;
            Ok(timestamp.to_rfc3339())
        }
        _ => bail!("unsupported boundary value type {value_type}"),
    }
}

fn trim_s3_etag(value: &str) -> &str {
    value.trim().trim_matches('"')
}

fn core_append_stream_id(tenant_id: i64, bucket_id: i64, stream_id: uuid::Uuid) -> String {
    format!("object-append-stream-{tenant_id}-{bucket_id}-{stream_id}")
}

fn core_append_stream_partition_id(tenant_id: i64, bucket_id: i64) -> String {
    format!("object-append-partition-{tenant_id}-{bucket_id}")
}

fn core_store_status(error: anyhow::Error) -> Status {
    if let Some(status) = crate::services::core_store_status::availability_status(&error) {
        status
    } else {
        Status::internal(error.to_string())
    }
}

#[cfg(test)]
mod tests;
