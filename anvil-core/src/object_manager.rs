use crate::{
    access_control, auth, bucket_journal,
    core_store::{
        AuthzScopeRef, CoreBoundarySchema, CoreBoundarySource, CoreBoundaryValue, CoreByteRange,
        CoreManifestLocator, CoreObjectRef, CorePrefetchPolicy, CoreStore, GetBlob,
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
use anyhow::{Context, Result as AnyhowResult, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::{Stream, StreamExt, TryStreamExt};
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::StreamReader;
use tonic::Status;
use tonic::metadata::MetadataValue;
use tracing::info;

mod batch_write;
mod boundary_extraction;
mod write_visibility;
pub(crate) use batch_write::ObjectBatchPut;
pub(crate) use boundary_extraction::extract_object_boundary_values;
use boundary_extraction::{canonical_json, canonical_json_bytes};
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
    implicit_write_durability: crate::mvcc_transaction::DurabilityLevel,
    mvcc: std::sync::Arc<std::sync::OnceLock<std::sync::Arc<crate::mvcc_bootstrap::MvccSubsystem>>>,
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

struct PreparedMvccObjectIngest {
    object_hash: String,
    object_length: u64,
    shard_map: JsonValue,
    object_ref: CoreObjectRef,
    boundary_payload: Option<Vec<u8>>,
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
    AtCommitVersion(u64),
    AtAuthzRevision(i64),
}

impl ObjectReadConsistency {
    pub fn commit_version(self) -> Option<u64> {
        match self {
            Self::AtCommitVersion(version) => Some(version),
            Self::Latest | Self::AtAuthzRevision(_) => None,
        }
    }

    pub fn authz_revision(self) -> Option<i64> {
        match self {
            Self::AtAuthzRevision(revision) => Some(revision),
            Self::Latest | Self::AtCommitVersion(_) => None,
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
    pub(crate) precondition: (
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    ),
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
        implicit_write_durability: crate::mvcc_transaction::DurabilityLevel,
    ) -> Self {
        Self {
            persistence,
            storage,
            core_store,
            region,
            cross_region_routing_policy,
            signing_key,
            observability,
            implicit_write_durability,
            mvcc: Default::default(),
        }
    }

    pub fn install_mvcc(
        &self,
        mvcc: std::sync::Arc<crate::mvcc_bootstrap::MvccSubsystem>,
    ) -> AnyhowResult<()> {
        self.mvcc
            .set(mvcc)
            .map_err(|_| anyhow!("MVCC object runtime is already installed"))
    }

    fn installed_mvcc(&self) -> Result<&crate::mvcc_bootstrap::MvccSubsystem, tonic::Status> {
        self.mvcc
            .get()
            .map(std::sync::Arc::as_ref)
            .ok_or_else(|| tonic::Status::unavailable("MVCC object runtime is not installed"))
    }

    fn installed_mvcc_arc(
        &self,
    ) -> Result<std::sync::Arc<crate::mvcc_bootstrap::MvccSubsystem>, tonic::Status> {
        self.mvcc
            .get()
            .cloned()
            .ok_or_else(|| tonic::Status::unavailable("MVCC object runtime is not installed"))
    }

    fn record_reserved_namespace_rejection(&self, operation: &'static str) {
        self.observability.increment_counter(
            RESERVED_NAMESPACE_REJECTION_COUNT,
            &[("api", "native"), ("operation", operation)],
        );
    }

    pub async fn put_object_with_implicit_transaction<S>(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        object_key: &str,
        data_stream: S,
        mut options: ObjectWriteOptions,
        idempotency_key: String,
    ) -> Result<Object, Status>
    where
        S: Stream<Item = Result<Vec<u8>, Status>> + Unpin + Send,
    {
        if options.transaction_id.is_some() || options.transaction_principal.is_some() {
            return Err(Status::invalid_argument(
                "implicit object write must not provide an existing transaction",
            ));
        }
        let mvcc = self.installed_mvcc()?;
        let principal = transaction_principal_from_claims(claims);
        let now = Self::current_unix_ms_for_object()?;
        let handle = mvcc
            .open_transactions
            .begin(
                mvcc.runtime.as_ref(),
                mvcc.cluster_id().to_string(),
                principal.clone(),
                idempotency_key,
                Duration::from_secs(300),
                self.implicit_write_durability,
                crate::mvcc_transaction::ReadConsistency::Linearized,
                now,
            )
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        options.transaction_id = Some(handle.transaction_id.clone());
        options.transaction_principal = Some(principal.clone());
        let object = match self
            .put_object(claims, bucket_name, object_key, data_stream, options)
            .await
        {
            Ok(object) => object,
            Err(status) => {
                let _ = mvcc.open_transactions.rollback(
                    &handle.transaction_id,
                    &principal,
                    Self::current_unix_ms_for_object().unwrap_or(now),
                );
                return Err(status);
            }
        };
        let outcome = mvcc
            .open_transactions
            .commit(
                mvcc.runtime.as_ref(),
                &handle.transaction_id,
                &principal,
                Self::current_unix_ms_for_object()?,
            )
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        if let crate::mvcc_transaction::CertificationResult::Aborted { reason } =
            outcome.certification
        {
            return Err(Status::aborted(format!(
                "implicit MVCC transaction aborted: {reason:?}"
            )));
        }
        Ok(object)
    }

    async fn begin_internal_transaction(
        &self,
        principal: String,
        idempotency_key: String,
    ) -> Result<crate::mvcc_open_transactions::TransactionHandle, Status> {
        let mvcc = self.installed_mvcc()?;
        mvcc.open_transactions
            .begin(
                mvcc.runtime.as_ref(),
                mvcc.cluster_id().to_string(),
                principal,
                idempotency_key,
                Duration::from_secs(300),
                self.implicit_write_durability,
                crate::mvcc_transaction::ReadConsistency::Linearized,
                Self::current_unix_ms_for_object()?,
            )
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))
    }

    async fn commit_internal_transaction(
        &self,
        transaction_id: &str,
        principal: &str,
    ) -> Result<(), Status> {
        let mvcc = self.installed_mvcc()?;
        let outcome = mvcc
            .open_transactions
            .commit(
                mvcc.runtime.as_ref(),
                transaction_id,
                principal,
                Self::current_unix_ms_for_object()?,
            )
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        if let crate::mvcc_transaction::CertificationResult::Aborted { reason } =
            outcome.certification
        {
            return Err(Status::aborted(format!(
                "implicit MVCC transaction aborted: {reason:?}"
            )));
        }
        Ok(())
    }

    /// Consume an object stream directly into its transaction durability
    /// representation. Distributed durability is encoded one bounded stripe at
    /// a time and each shard is sent to its final holder; no complete local
    /// payload file is created.
    async fn prepare_mvcc_object_ingest<S>(
        &self,
        data_stream: S,
        transaction_id: &str,
        transaction_principal: &str,
        bucket_name: &str,
        object_key: &str,
        boundary_capture_limit: Option<u64>,
    ) -> Result<PreparedMvccObjectIngest, Status>
    where
        S: Stream<Item = Result<Vec<u8>, Status>> + Unpin + Send,
    {
        let boundary_capture =
            boundary_capture_limit.map(|_| std::sync::Arc::new(Mutex::new(Vec::new())));
        let capture = boundary_capture.clone();
        let capture_bytes = boundary_capture_limit
            .map(|limit| limit.saturating_add(1))
            .unwrap_or(0);
        let reader_stream = data_stream
            .map_ok(move |bytes| {
                if let Some(capture) = &capture {
                    let mut captured = capture
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let remaining = usize::try_from(capture_bytes)
                        .unwrap_or(usize::MAX)
                        .saturating_sub(captured.len());
                    captured.extend_from_slice(&bytes[..bytes.len().min(remaining)]);
                }
                std::io::Cursor::new(bytes)
            })
            .map_err(|status| std::io::Error::other(status.to_string()));
        let mut reader = StreamReader::new(reader_stream);
        let now = Self::current_unix_ms_for_object()?;
        let prepared = crate::mvcc_physical_payload::prepare_mvcc_physical_payload(
            self.installed_mvcc()?,
            &mut reader,
            crate::mvcc_physical_payload::PrepareMvccPhysicalPayload {
                transaction_id,
                transaction_principal,
                logical_scope: bucket_name,
                logical_key: object_key,
                prepared_at_unix_ms: now,
            },
        )
        .await?;
        let boundary_payload = boundary_capture.map(|capture| {
            capture
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        });
        Ok(PreparedMvccObjectIngest {
            object_hash: prepared.object_hash.clone(),
            object_length: prepared.object_length,
            shard_map: prepared.shard_map(),
            object_ref: prepared
                .object_ref()
                .map_err(|error| Status::internal(error.to_string()))?,
            boundary_payload,
        })
    }

    fn mvcc_ingest_object_ref(
        prepared: &PreparedMvccObjectIngest,
    ) -> Result<CoreObjectRef, Status> {
        Ok(prepared.object_ref.clone())
    }

    async fn read_mvcc_compatible_object_ref(
        &self,
        object_ref: CoreObjectRef,
    ) -> Result<Vec<u8>, Status> {
        if let Some(bytes) = crate::mvcc_physical_payload::read_mvcc_core_object_ref(
            self.installed_mvcc()?,
            &object_ref,
        )
        .await
        .map_err(|error| Status::data_loss(error.to_string()))?
        {
            return Ok(bytes);
        }
        self.core_store
            .get_blob(GetBlob { object_ref })
            .await
            .map_err(|error| Status::internal(error.to_string()))
    }

    async fn stream_mvcc_compatible_object_ref(
        &self,
        object_ref: CoreObjectRef,
        output: mpsc::Sender<Result<Vec<u8>, Status>>,
    ) -> Result<(), Status> {
        let Some(locator) =
            crate::mvcc_physical_payload::decode_core_object_ref_locator(&object_ref)
                .map_err(|error| Status::data_loss(error.to_string()))?
        else {
            return self
                .core_store
                .read_object_ref_chunks(object_ref, None, 256 * 1024, |chunk| {
                    let output = output.clone();
                    async move {
                        output
                            .send(Ok(chunk))
                            .await
                            .map_err(|_| anyhow!("multipart completion stream closed"))
                    }
                })
                .await
                .map_err(|error| Status::internal(error.to_string()));
        };
        match locator {
            crate::mvcc_physical_payload::MvccPhysicalPayloadLocator::Local(manifest) => {
                let file = self
                    .installed_mvcc()?
                    .local_objects
                    .open_verified(&manifest)
                    .await
                    .map_err(|error| Status::data_loss(error.to_string()))?;
                let mut chunks = tokio_util::io::ReaderStream::with_capacity(file, 256 * 1024);
                while let Some(chunk) = chunks.next().await {
                    output
                        .send(Ok(chunk
                            .map_err(|error| Status::data_loss(error.to_string()))?
                            .to_vec()))
                        .await
                        .map_err(|_| Status::cancelled("multipart completion stream closed"))?;
                }
                Ok(())
            }
            crate::mvcc_physical_payload::MvccPhysicalPayloadLocator::Shards(manifest) => manifest
                .read_range_chunks(
                    &self.installed_mvcc()?.replication_client,
                    0,
                    manifest.object_length,
                    move |chunk| {
                        let output = output.clone();
                        async move {
                            output
                                .send(Ok(chunk))
                                .await
                                .map_err(|_| anyhow!("multipart completion stream closed"))
                        }
                    },
                )
                .await
                .map_err(|error| Status::data_loss(error.to_string())),
        }
    }

    async fn object_boundary_capture_limit(
        &self,
        tenant_id: i64,
        bucket_name: &str,
    ) -> Result<Option<u64>, Status> {
        let boundary_schema_key =
            crate::core_store::boundary_schema_bucket_key(tenant_id, bucket_name);
        let schema = self.read_committed_boundary_schema(&boundary_schema_key)?;
        Ok(schema.and_then(|schema| {
            schema
                .dimensions
                .iter()
                .filter_map(|dimension| match &dimension.source {
                    CoreBoundarySource::BodyJsonPointer { max_body_bytes, .. } => {
                        Some(*max_body_bytes)
                    }
                    _ => None,
                })
                .min()
        }))
    }

    async fn object_write_boundary_values_from_payload(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        object_key: &str,
        content_type: Option<&str>,
        user_metadata: Option<&JsonValue>,
        payload_len: u64,
        payload: &[u8],
    ) -> Result<Vec<CoreBoundaryValue>, Status> {
        let boundary_schema_key =
            crate::core_store::boundary_schema_bucket_key(tenant_id, bucket_name);
        let Some(schema) = self.read_committed_boundary_schema(&boundary_schema_key)? else {
            return Ok(Vec::new());
        };
        extract_object_boundary_values(
            &schema,
            tenant_id,
            bucket_name,
            object_key,
            content_type,
            user_metadata,
            payload_len,
            payload,
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
        let Some(schema) = self.read_committed_boundary_schema(&boundary_schema_key)? else {
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

    fn read_committed_boundary_schema(
        &self,
        boundary_schema_key: &str,
    ) -> Result<Option<crate::core_store::CoreBoundarySchema>, Status> {
        let tuple_key =
            crate::core_store::CoreStore::boundary_schema_current_tuple_key(boundary_schema_key)
                .map_err(|error| Status::internal(error.to_string()))?;
        let logical_key = crate::mvcc_product::coremeta_logical_key(
            crate::core_store::CF_BOUNDARY,
            crate::core_store::TABLE_BOUNDARY_SCHEMA_CURRENT_ROW,
            &tuple_key,
        )
        .map_err(|error| Status::internal(error.to_string()))?;
        let Some(bytes) = self
            .installed_mvcc()?
            .read_latest_value(&logical_key)
            .map_err(|error| Status::internal(error.to_string()))?
        else {
            return Ok(None);
        };
        crate::core_store::CoreStore::decode_boundary_schema_from_mvcc(&bytes)
            .map(Some)
            .map_err(|error| Status::internal(error.to_string()))
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
        data_stream: impl Stream<Item = Result<Vec<u8>, Status>> + Unpin + Send,
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
        let transaction_id = options
            .transaction_id
            .clone()
            .ok_or_else(|| Status::failed_precondition("ObjectWriteRequiresClusterTransaction"))?;
        let transaction_principal = options.transaction_principal.clone().ok_or_else(|| {
            Status::failed_precondition("ObjectWriteRequiresTransactionPrincipal")
        })?;
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
            self.installed_mvcc()?,
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
        let boundary_capture_limit = if options.visibility.requires_payload_boundary_extraction() {
            self.object_boundary_capture_limit(tenant_id, &bucket.name)
                .await?
        } else {
            None
        };
        let prepared_ingest = self
            .prepare_mvcc_object_ingest(
                data_stream,
                &transaction_id,
                &transaction_principal,
                &bucket.name,
                object_key,
                boundary_capture_limit,
            )
            .await?;
        let total_bytes = i64::try_from(prepared_ingest.object_length)
            .map_err(|_| Status::invalid_argument("Object exceeds supported size"))?;
        crate::emit_test_timing(
            "object_manager.put_object prepare_payload",
            step_start.elapsed(),
        );
        let total_bytes_u64 =
            u64::try_from(total_bytes).map_err(|_| Status::internal("Negative payload size"))?;
        let boundary_values = if options.visibility.requires_payload_boundary_extraction() {
            self.object_write_boundary_values_from_payload(
                tenant_id,
                &bucket.name,
                object_key,
                options.content_type.as_deref(),
                options.user_metadata.as_ref(),
                total_bytes_u64,
                prepared_ingest.boundary_payload.as_deref().unwrap_or(&[]),
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
        let effective_storage_class_id = self
            .core_store
            .resolve_storage_class_id(options.storage_class_id.as_deref())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        self.core_store
            .get_storage_class(&effective_storage_class_id)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let content_hash = prepared_ingest.object_hash;
        let shard_map = Some(prepared_ingest.shard_map);

        let step_start = std::time::Instant::now();
        let materialisation_content_type = options.content_type.clone();
        let materialisation_user_metadata = options
            .user_metadata
            .clone()
            .unwrap_or_else(|| serde_json::json!({}));
        let materialisation_representation = shard_map
            .clone()
            .expect("transactional object ingest always produces a representation");
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
                Some(&transaction_id),
                Some(&transaction_principal),
                Some(effective_storage_class_id),
                options.visibility.persistence_options(),
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        {
            let transaction_id = transaction_id.as_str();
            let principal = transaction_principal.as_str();
            let representation = materialisation_representation;
            let mvcc = self.installed_mvcc()?;
            let boundary_schema = self.read_committed_boundary_schema(
                &crate::core_store::boundary_schema_bucket_key(tenant_id, &bucket.name),
            )?;
            let boundary_schema_value = boundary_schema
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .map_err(|error| Status::internal(error.to_string()))?;
            let boundary_schema_hash = boundary_schema_value.as_ref().map(|schema| {
                use sha2::Digest as _;
                format!(
                    "sha256:{}",
                    hex::encode(sha2::Sha256::digest(
                        serde_json::to_vec(schema).expect("schema serializes")
                    ))
                )
            });
            let target = format!(
                "tenant/{tenant_id}/bucket/{}/object/{object_key}/version/{}",
                bucket.id, object.version_id
            );
            let now = Self::current_unix_ms_for_object()?;
            let binding = mvcc
                .open_transactions
                .handle(transaction_id)
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            let mut frozen_index_definitions = self
                .persistence
                .list_index_definitions(tenant_id, bucket.id, false)
                .await
                .map_err(|error| Status::internal(error.to_string()))?
                .into_iter()
                .filter(|definition| definition.enabled)
                .filter(|definition| {
                    crate::index_builder::helpers::selector_matches(&definition.selector, &object)
                })
                .map(
                    |definition| crate::object_materialisation::FrozenIndexDefinition {
                        id: definition.id,
                        version: definition.version,
                        name: definition.name,
                        kind: definition.kind,
                        selector: definition.selector,
                        extractor: definition.extractor,
                        authorization_mode: definition.authorization_mode,
                        build_policy: definition.build_policy,
                    },
                )
                .collect::<Vec<_>>();
            frozen_index_definitions.sort_by_key(|definition| (definition.id, definition.version));
            let frozen_object = serde_json::to_value(&object)
                .map(|value| canonical_json(&value))
                .map_err(|error| Status::internal(error.to_string()))?;
            let source_manifest_hash = {
                use sha2::Digest as _;
                let mut digest = sha2::Sha256::new();
                digest.update(b"anvil.mvcc.object-materialisation-source.v1\0");
                digest.update(binding.snapshot_version.to_be_bytes());
                digest.update(
                    canonical_json_bytes(&frozen_object)
                        .map_err(|error| Status::internal(error.to_string()))?,
                );
                hex::encode(digest.finalize())
            };
            let job = crate::object_materialisation::ObjectMaterialisationJob {
                schema: crate::object_materialisation::ObjectMaterialisationJob::SCHEMA.into(),
                cluster_id: mvcc.cluster_id().to_string(),
                transaction_id: transaction_id.to_string(),
                tenant_id,
                bucket_id: bucket.id,
                bucket_name: bucket.name.clone(),
                object_key: object_key.to_string(),
                object_version_id: object.version_id.to_string(),
                target_logical_identity: target,
                representation,
                content_hash: object.content_hash.clone(),
                payload_length: total_bytes_u64,
                frozen_object,
                source_manifest_hash,
                content_type: materialisation_content_type,
                user_metadata: materialisation_user_metadata,
                index_policy_snapshot: serde_json::json!({
                    "snapshot": object.index_policy_snapshot.clone(),
                }),
                originating_snapshot_version: binding.snapshot_version,
                frozen_index_definitions,
                authz_revision: object.authz_revision,
                boundary_schema_generation: boundary_schema
                    .as_ref()
                    .map(|schema| schema.generation)
                    .unwrap_or(0),
                boundary_schema: boundary_schema_value,
                boundary_schema_hash,
                requested_operations:
                    crate::object_materialisation::ObjectMaterialisationOperations {
                        extract_boundaries: options
                            .visibility
                            .requires_payload_boundary_extraction(),
                        maintain_indexes: matches!(
                            options.visibility.indexes,
                            IndexMaintenanceVisibility::Enqueued
                                | IndexMaintenanceVisibility::CaughtUp
                        ),
                    },
                requested_at_unix_ms: now,
            };
            if job.requested_operations.extract_boundaries
                || job.requested_operations.maintain_indexes
            {
                let job_id = job
                    .job_id()
                    .map_err(|error| Status::internal(error.to_string()))?;
                let pending = crate::object_materialisation::ObjectMaterialisationResult {
                    schema: crate::object_materialisation::ObjectMaterialisationResult::SCHEMA
                        .into(),
                    cluster_id: job.cluster_id.clone(),
                    target_logical_identity: job.target_logical_identity.clone(),
                    job_id,
                    state: crate::object_materialisation::ObjectMaterialisationState::Pending,
                    boundary_schema_hash: job.boundary_schema_hash.clone(),
                    derived_boundaries: serde_json::to_value(&boundary_values)
                        .map_err(|error| Status::internal(error.to_string()))?,
                    index_marker: serde_json::json!({"pending": true}),
                    updated_at_unix_ms: now,
                };
                mvcc.stage_product_mutations(
                    transaction_id,
                    principal,
                    vec![crate::mvcc_product::ProductMutation::put(
                        pending
                            .status_key()
                            .map_err(|error| Status::internal(error.to_string()))?,
                        pending
                            .canonical_bytes()
                            .map_err(|error| Status::internal(error.to_string()))?,
                    )],
                    now,
                )
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
                mvcc.open_transactions
                    .add_job(
                        transaction_id,
                        job.canonical_bytes()
                            .map_err(|error| Status::internal(error.to_string()))?,
                        now,
                    )
                    .map_err(|error| Status::failed_precondition(error.to_string()))?;
            }
        }
        crate::emit_test_timing(
            "object_manager.put_object persistence_create_object",
            step_start.elapsed(),
        );
        crate::emit_test_timing("object_manager.put_object total", total_start.elapsed());

        Ok(object)
    }

    fn current_unix_ms_for_object() -> Result<u64, Status> {
        let elapsed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Status::internal("system clock precedes Unix epoch"))?;
        u64::try_from(elapsed.as_millis()).map_err(|_| Status::internal("system time exceeds u64"))
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
        let principal = transaction_principal
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| transaction_principal_from_claims(claims));
        let implicit_transaction = if transaction_id.is_none() {
            Some(
                self.begin_internal_transaction(
                    principal.clone(),
                    format!(
                        "multipart-initiate:{tenant_id}:{}:{object_key}:{}",
                        bucket.id,
                        uuid::Uuid::new_v4()
                    ),
                )
                .await?,
            )
        } else {
            None
        };
        let transaction_id = transaction_id
            .or_else(|| {
                implicit_transaction
                    .as_ref()
                    .map(|handle| handle.transaction_id.as_str())
            })
            .expect("explicit or implicit transaction");
        let mutation = self
            .persistence
            .create_multipart_upload_in_transaction(
                tenant_id,
                bucket.id,
                object_key,
                transaction_id,
                &principal,
                None,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if implicit_transaction.is_some() {
            self.commit_internal_transaction(transaction_id, &principal)
                .await?;
        }
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
        data_stream: impl Stream<Item = Result<Vec<u8>, Status>> + Unpin + Send,
        transaction_id: Option<&str>,
        transaction_principal: Option<&str>,
    ) -> Result<UploadPartResult, Status> {
        self.validate_write_request(claims, bucket_name, object_key)
            .await?;
        let tenant_id = claims.tenant_id;
        validate_multipart_part_number(part_number)?;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        let principal = transaction_principal
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| transaction_principal_from_claims(claims));
        let implicit_transaction = if transaction_id.is_none() {
            Some(
                self.begin_internal_transaction(
                    principal.clone(),
                    format!(
                        "multipart-part:{tenant_id}:{}:{upload_id}:{part_number}:{}",
                        bucket.id,
                        uuid::Uuid::new_v4()
                    ),
                )
                .await?,
            )
        } else {
            None
        };
        let transaction_id = transaction_id
            .or_else(|| {
                implicit_transaction
                    .as_ref()
                    .map(|handle| handle.transaction_id.as_str())
            })
            .expect("explicit or implicit transaction");
        let upload = self
            .persistence
            .get_active_multipart_upload_in_transaction(
                tenant_id,
                bucket.id,
                object_key,
                upload_id,
                transaction_id,
                &principal,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Multipart upload not found"))?;

        let prepared = self
            .prepare_mvcc_object_ingest(
                data_stream,
                transaction_id,
                &principal,
                &bucket.name,
                &format!("{object_key}/multipart/{upload_id}/part/{part_number}"),
                None,
            )
            .await?;
        let bytes = i64::try_from(prepared.object_length)
            .map_err(|_| Status::invalid_argument("Multipart part exceeds supported size"))?;
        let content_hash = prepared.object_hash.clone();
        let object_ref = Self::mvcc_ingest_object_ref(&prepared)?;

        let mutation = self
            .persistence
            .upsert_multipart_part_in_transaction(
                upload.id,
                part_number,
                object_ref,
                bytes,
                &content_hash,
                transaction_id,
                &principal,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if implicit_transaction.is_some() {
            self.commit_internal_transaction(transaction_id, &principal)
                .await?;
        }
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
        let principal = transaction_principal
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| transaction_principal_from_claims(claims));
        let implicit_transaction = if transaction_id.is_none() {
            Some(
                self.begin_internal_transaction(
                    principal.clone(),
                    format!("multipart-complete:{tenant_id}:{}:{upload_id}", bucket.id),
                )
                .await?,
            )
        } else {
            None
        };
        let transaction_id = transaction_id
            .or_else(|| {
                implicit_transaction
                    .as_ref()
                    .map(|handle| handle.transaction_id.as_str())
            })
            .expect("explicit or implicit transaction");
        let upload = self
            .persistence
            .get_active_multipart_upload_in_transaction(
                tenant_id,
                bucket.id,
                object_key,
                upload_id,
                transaction_id,
                &principal,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Multipart upload not found"))?;
        let stored_parts = self
            .persistence
            .list_multipart_parts_in_transaction(upload.id, transaction_id, &principal)
            .await
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

        let object_manager = self.clone();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            for object_ref in ordered_part_refs {
                if let Err(error) = object_manager
                    .stream_mvcc_compatible_object_ref(object_ref, tx.clone())
                    .await
                {
                    let _ = tx.send(Err(error)).await;
                    return;
                }
            }
        });

        let options = ObjectWriteOptions {
            transaction_id: Some(transaction_id.to_owned()),
            transaction_principal: Some(principal.clone()),
            visibility: ObjectWriteVisibility::strict(),
            ..Default::default()
        };
        let part_stream = ReceiverStream::new(rx);
        let object = self
            .put_object(claims, bucket_name, object_key, part_stream, options)
            .await?;

        let completion = self
            .persistence
            .complete_multipart_upload_in_transaction(upload.id, transaction_id, &principal)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if !completion.completed {
            return Err(Status::not_found("Multipart upload not found"));
        }
        if implicit_transaction.is_some() {
            self.commit_internal_transaction(transaction_id, &principal)
                .await?;
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
        let principal = transaction_principal
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| transaction_principal_from_claims(claims));
        let implicit_transaction = if transaction_id.is_none() {
            Some(
                self.begin_internal_transaction(
                    principal.clone(),
                    format!("multipart-abort:{tenant_id}:{}:{upload_id}", bucket.id),
                )
                .await?,
            )
        } else {
            None
        };
        let transaction_id = transaction_id
            .or_else(|| {
                implicit_transaction
                    .as_ref()
                    .map(|handle| handle.transaction_id.as_str())
            })
            .expect("explicit or implicit transaction");
        let mutation = self
            .persistence
            .abort_multipart_upload_in_transaction(
                tenant_id,
                bucket.id,
                object_key,
                upload_id,
                transaction_id,
                &principal,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if let Some(receipt) = mutation.receipt {
            if implicit_transaction.is_some() {
                self.commit_internal_transaction(transaction_id, &principal)
                    .await?;
            }
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
        access_control::require_bucket_permission(
            &self.storage,
            self.installed_mvcc()?,
            &claims,
            &bucket,
            "list_objects",
        )
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
        let transaction_id = transaction_id.ok_or_else(|| {
            Status::failed_precondition("append stream create requires an MVCC transaction")
        })?;
        let transaction_principal = transaction_principal.ok_or_else(|| {
            Status::invalid_argument("transaction principal is required for append stream create")
        })?;
        let mutation = self
            .persistence
            .create_append_stream_in_transaction(
                tenant_id,
                bucket.id,
                &bucket.name,
                stream_key,
                transaction_id,
                transaction_principal,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        access_control::stage_stream_defaults(
            &self.persistence,
            &bucket,
            stream_key,
            &claims.sub,
            &claims.sub,
            "grant creator stream owner",
            transaction_id,
            transaction_principal,
        )
        .await
        .map_err(core_store_status)?;
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
        let transaction_id = transaction_id.ok_or_else(|| {
            Status::failed_precondition("append stream write requires an MVCC transaction")
        })?;
        let mvcc = self
            .mvcc
            .get()
            .ok_or_else(|| Status::failed_precondition("MVCC runtime is not installed"))?;
        let stream = self
            .persistence
            .get_active_append_stream_in_transaction(
                mvcc,
                tenant_id,
                bucket.id,
                stream_key,
                stream_id,
                transaction_id,
                &authenticated_principal,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Append stream not found"))?;

        let prepared = self
            .prepare_mvcc_object_ingest(
                futures_util::stream::iter(std::iter::once(Ok(payload))),
                transaction_id,
                &authenticated_principal,
                &bucket.name,
                &format!("{stream_key}/append-record"),
                None,
            )
            .await?;
        let payload_size = i64::try_from(prepared.object_length)
            .map_err(|_| Status::invalid_argument("Append payload exceeds supported size"))?;
        let payload_hash = prepared.object_hash.clone();
        let object_ref = Self::mvcc_ingest_object_ref(&prepared)?;
        let mutation = self
            .persistence
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
        let transaction_id = transaction_id.ok_or_else(|| {
            Status::failed_precondition("append stream seal requires an MVCC transaction")
        })?;
        let transaction_principal = transaction_principal.ok_or_else(|| {
            Status::invalid_argument("transaction principal is required for append stream seal")
        })?;
        let mvcc = self
            .mvcc
            .get()
            .ok_or_else(|| Status::failed_precondition("MVCC runtime is not installed"))?;
        let stream = self
            .persistence
            .get_active_append_stream_in_transaction(
                mvcc,
                tenant_id,
                bucket.id,
                stream_key,
                stream_id,
                transaction_id,
                transaction_principal,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Append stream not found"))?;
        let transaction = Some((transaction_id, transaction_principal));
        let has_records = self
            .persistence
            .append_stream_has_records(Some(mvcc.as_ref()), &stream, transaction)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if !has_records {
            return Err(Status::failed_precondition(
                "Append stream has no records to seal",
            ));
        }

        let (segment_hash, record_count) = self
            .persistence
            .append_stream_segment_hash(Some(mvcc.as_ref()), &stream, transaction)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        let sealed = self
            .persistence
            .seal_append_stream_in_transaction(
                tenant_id,
                bucket.id,
                &stream,
                &segment_hash,
                transaction_id,
                transaction_principal,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let Some(receipt) = sealed.receipt else {
            return Err(Status::failed_precondition(
                "Append stream is already sealed",
            ));
        };

        Ok(SealAppendStreamResult {
            record_count,
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
        let stream = self
            .persistence
            .get_active_append_stream(claims.tenant_id, bucket.id, stream_key, stream_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Append stream not found"))?;
        let limit = if limit == 0 { 100 } else { limit.min(1000) } as usize;
        let snapshot = match consistency {
            ObjectReadConsistency::AtCommitVersion(version) => version,
            ObjectReadConsistency::Latest | ObjectReadConsistency::AtAuthzRevision(_) => self
                .persistence
                .mvcc()
                .and_then(|mvcc| mvcc.runtime.applied_version())
                .map_err(|error| Status::internal(error.to_string()))?,
        };
        let records = self
            .persistence
            .list_append_stream_records_at_snapshot(&stream, snapshot, after_sequence, limit)
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .records;

        let mut out = Vec::with_capacity(records.len());
        for record in records {
            let payload = if include_payload {
                let bytes = self
                    .read_mvcc_compatible_object_ref(record.payload_object_ref.clone())
                    .await?;
                if record.payload_object_ref.hash != record.payload_hash
                    || i64::try_from(bytes.len()).ok() != Some(record.payload_size)
                {
                    return Err(Status::data_loss(
                        "Append record payload does not match its immutable reference",
                    ));
                }
                Some(bytes)
            } else {
                None
            };
            out.push(AppendStreamRecordRead {
                record_sequence: u64::try_from(record.record_sequence)
                    .map_err(|_| Status::internal("Append record sequence is negative"))?,
                payload_hash: record.payload_hash,
                payload_size: record.payload_size,
                content_type: record.content_type,
                user_metadata: record.user_meta,
                authenticated_principal: record.authenticated_principal,
                created_at: record.created_at,
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

        let transaction_id = transaction_id.ok_or_else(|| {
            Status::failed_precondition("manifest CAS requires an MVCC transaction")
        })?;
        let transaction_principal = transaction_principal.ok_or_else(|| {
            Status::invalid_argument("transaction principal is required for manifest CAS")
        })?;
        let result = self
            .persistence
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
    MvccShards(crate::object_shard_manifest::PhysicalObjectShardManifest),
    MvccLocal(crate::local_object_store::LocalObjectManifest),
}

pub(crate) fn local_object_manifest(
    object: &crate::persistence::Object,
) -> AnyhowResult<Option<crate::local_object_store::LocalObjectManifest>> {
    let target = object
        .shard_map
        .as_ref()
        .context("object shard map is missing")
        .and_then(object_data_target_from_shard_map)?;
    Ok(match target {
        ObjectDataTarget::MvccLocal(manifest) => Some(manifest),
        ObjectDataTarget::MvccShards(_)
        | ObjectDataTarget::LogicalFile(_)
        | ObjectDataTarget::ObjectRef(_) => None,
    })
}

/// Read a payload represented directly by the MVCC object layer.
///
/// `None` means the object uses a legacy CoreStore representation and lets
/// callers retain their existing CoreStore read path.
pub(crate) async fn read_mvcc_object_payload(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    object: &crate::persistence::Object,
) -> AnyhowResult<Option<Vec<u8>>> {
    let target = object
        .shard_map
        .as_ref()
        .context("object shard map is missing")
        .and_then(object_data_target_from_shard_map)?;
    match target {
        ObjectDataTarget::MvccLocal(manifest) => mvcc
            .local_objects
            .read_range(&manifest, 0, manifest.object_length)
            .map(Some),
        ObjectDataTarget::MvccShards(manifest) => {
            let bytes = Arc::new(std::sync::Mutex::new(Vec::new()));
            manifest
                .read_range_chunks(&mvcc.replication_client, 0, manifest.object_length, {
                    let bytes = bytes.clone();
                    move |chunk| {
                        let bytes = bytes.clone();
                        async move {
                            bytes.lock().unwrap().extend_from_slice(&chunk);
                            Ok(())
                        }
                    }
                })
                .await?;
            Ok(Some(Arc::try_unwrap(bytes).unwrap().into_inner().unwrap()))
        }
        ObjectDataTarget::LogicalFile(_) | ObjectDataTarget::ObjectRef(_) => Ok(None),
    }
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
        ObjectDataTarget::MvccShards(manifest) => {
            Ok(crate::mvcc_physical_payload::encode_shard_map(
                &crate::mvcc_physical_payload::MvccPhysicalPayloadLocator::Shards(manifest.clone()),
            ))
        }
        ObjectDataTarget::MvccLocal(manifest) => {
            Ok(crate::mvcc_physical_payload::encode_shard_map(
                &crate::mvcc_physical_payload::MvccPhysicalPayloadLocator::Local(manifest.clone()),
            ))
        }
    }
}

fn object_data_target_from_shard_map(value: &JsonValue) -> AnyhowResult<ObjectDataTarget> {
    if let Some(locator) = crate::mvcc_physical_payload::decode_shard_map(value)? {
        return Ok(match locator {
            crate::mvcc_physical_payload::MvccPhysicalPayloadLocator::Local(manifest) => {
                ObjectDataTarget::MvccLocal(manifest)
            }
            crate::mvcc_physical_payload::MvccPhysicalPayloadLocator::Shards(manifest) => {
                ObjectDataTarget::MvccShards(manifest)
            }
        });
    }
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

fn trim_s3_etag(value: &str) -> &str {
    value.trim().trim_matches('"')
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
