use std::fmt;

use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

mod generated {
    tonic::include_proto!("anvil");
}

pub mod proto {
    pub use super::generated::{
        AbortMultipartRequest, AbortMultipartResponse, AbortSagaDraftRequest,
        AcquireTaskLeaseRequest, AnvilError, AppendStreamRecordInfo, AppendStreamRecordRequest,
        AppendStreamRecordResponse, ApplicationDescriptor, ApplyAuthzSchemaRequest,
        ApplyAuthzSchemaResponse, ApplySagaRequest, ApplySagaResponse, AuditEventsResponse,
        AuthzAllowedSubject, AuthzMaterializationMode, AuthzNamespaceSchema, AuthzRelationRule,
        AuthzRelationSchema, AuthzRevisionMode, AuthzSchemaMemberKind, AuthzSchemaRef, AuthzScope,
        AuthzSubject, AuthzSubjectSelectorKind, AuthzTuple, AuthzTupleMutation,
        BeginSagaCompensationTransactionRequest, BeginSagaCompensationTransactionResponse,
        BeginSagaTransactionRequest, BeginSagaTransactionResponse, BeginTransactionRequest,
        BeginTransactionResponse, BindAuthzSchemaRequest, BindAuthzSchemaResponse,
        BoundaryDimension, BoundaryExtractionMode, BoundaryMigrationMode, BoundaryMigrationStatus,
        BoundarySchemaRecord, BoundarySchemaResponse, BoundarySource, BoundaryValue, Bucket,
        ByteRange, CancelHfIngestionRequest, CancelHfIngestionResponse, CancelSagaRequest,
        CheckPermissionRequest, CheckPermissionResponse, CheckPermissionsRequest,
        CheckPermissionsResponse, CheckpointTaskLeaseRequest, CommitTaskLeaseRequest,
        CommitTaskLeaseResponse, CommitTransactionRequest, CompareAndSwapManifestRequest,
        CompareAndSwapManifestResponse, CompleteMultipartPart, CompleteMultipartRequest,
        CompleteMultipartResponse, ComposeObjectRequest, ComposeObjectResponse,
        ComposeObjectSource, ConsistencyMode, CopyObjectRequest, CopyObjectResponse,
        CreateAppendStreamRequest, CreateAppendStreamResponse, CreateApplicationCredentialRequest,
        CreateBucketRequest, CreateBucketResponse, CreateHfKeyRequest, CreateHfKeyResponse,
        CreateHostAliasRequest, CreateIndexRequest, CreateObjectLinkRequest,
        CreatePersonalDbGroupRequest, CreatePersonalDbProjectionRequest, DType,
        DeleteApplicationCredentialRequest, DeleteApplicationCredentialResponse,
        DeleteBucketRequest, DeleteBucketResponse, DeleteHfKeyRequest, DeleteHfKeyResponse,
        DeleteHostAliasRequest, DeleteObjectLinkRequest, DeleteObjectRequest, DeleteObjectResponse,
        DisableIndexRequest, DropIndexRequest, DropIndexResponse, ForceReleaseTaskLeaseRequest,
        ForceReleaseTaskLeaseResponse, GetAccessTokenRequest, GetAccessTokenResponse,
        GetAuthzSchemaBindingRequest, GetAuthzSchemaBindingResponse, GetAuthzSchemaRequest,
        GetAuthzSchemaResponse, GetBoundaryMigrationRequest, GetBoundarySchemaRequest,
        GetBucketPolicyRequest, GetBucketPolicyResponse, GetGitBlobByPathRequest,
        GetGitBlobByPathResponse, GetGitObjectRequest, GetGitObjectResponse,
        GetHfIngestionStatusRequest, GetHfIngestionStatusResponse, GetObjectRequest,
        GetObjectResponse, GetPackageVersionRequest, GetPartitionMapRequest,
        GetPersonalDbGroupRequest, GetPersonalDbProjectionRequest, GetSagaRequest,
        GetStorageClassRequest, GetTensorChunk, GetTensorRequest, GetTensorsRequest,
        GetTransactionRequest, GitBlobLocation, GitPackMetadata, GitTreeEntryRecord,
        GrantAccessRequest, GrantAccessResponse, HeadObjectRequest, HeadObjectResponse, HfKey,
        IndexBuildRecord, IndexDefinitionRecord, IndexDefinitionResponse, IndexDiagnosticRecord,
        IndexKind, IndexMaintenanceMode, IndexPolicySnapshotMode, IndexQueryHit,
        InitiateMultipartRequest, InitiateMultipartResponse, LeaseFencePrecondition,
        ListAccessGrantsRequest, ListAccessGrantsResponse, ListApplicationsRequest,
        ListApplicationsResponse, ListAuditEventsRequest, ListAuthzObjectsRequest,
        ListAuthzObjectsResponse, ListAuthzSubjectsRequest, ListAuthzSubjectsResponse,
        ListBucketsRequest, ListBucketsResponse, ListGitTreeRequest, ListGitTreeResponse,
        ListHfKeysRequest, ListHfKeysResponse, ListIndexDiagnosticsRequest,
        ListIndexDiagnosticsResponse, ListIndexesRequest, ListIndexesResponse,
        ListObjectLinksRequest, ListObjectLinksResponse, ListObjectVersionsRequest,
        ListObjectVersionsResponse, ListObjectsRequest, ListObjectsResponse,
        ListPackageVersionsRequest, ListPackageVersionsResponse, ListRepairFindingsRequest,
        ListRepairFindingsResponse, ListStorageClassesRequest, ListStorageClassesResponse,
        ListTensorsRequest, ListTensorsResponse, ModelManifest, MoveBucketRequest,
        MutationBatchAppendStreamRecord, MutationBatchCheckpointTaskLease,
        MutationBatchCommitTaskLease, MutationBatchCompareAndSwapManifest,
        MutationBatchDeleteObject, MutationBatchOperation, MutationBatchOperationReceipt,
        MutationBatchPatchJsonObject, MutationBatchPutObject, MutationBatchRequest,
        MutationBatchResponse, MutationResponse, NativeMutationContext, ObjectInfo,
        ObjectLinkResponse, ObjectMetadata, ObjectRef, ObjectSummary, ObjectVersionPrecondition,
        ObjectVersionSummary, PackageVersion, PageRequest, PageResponse, PartitionMap,
        PatchJsonObjectRequest, PatchJsonObjectResponse, PersonalDbCatchUpEntry,
        PersonalDbCatchUpRequest, PersonalDbCatchUpResponse, PersonalDbCommitCertificateRecord,
        PersonalDbCommittedHeadRecord, PersonalDbGroupManifestRecord, PersonalDbGroupResponse,
        PersonalDbLogRecord, PersonalDbProjectionResponse, PersonalDbSnapshotsHeadRecord,
        PersonalDbVoterAck, PublicMutationContext, PutAuthzSchemaRequest, PutAuthzSchemaResponse,
        PutBoundarySchemaRequest, PutBucketPolicyRequest, PutBucketPolicyResponse, PutCellRequest,
        PutGitPackRequest, PutGitPackResponse, PutModelManifestRequest, PutModelManifestResponse,
        PutNodeRequest, PutObjectRequest, PutObjectResponse, PutPackageBlobRequest,
        PutPackageVersionRequest, PutRegionRequest, PutRegistryRefRequest, QueryIndexRequest,
        QueryIndexResponse, QuerySpecRequest, QuerySpecResponse, ReadAppendStreamRequest,
        ReadAppendStreamResponse, ReadAuthzTuplesRequest, ReadAuthzTuplesResponse, ReadConsistency,
        ReadObjectLinkRequest, ReadTaskLeaseRequest, ReadTaskLeaseResponse,
        RepairAuthzDerivedIndexRequest, RepairAuthzDerivedIndexResponse,
        RepairDirectoryIndexRequest, RepairDirectoryIndexResponse, RepairFindingRecord,
        RepairIndexRequest, RepairIndexResponse, RepairPersonalDbLogChainRequest,
        RepairPersonalDbLogChainResponse, RepairSubjectRecord, ResolveBlockedSagaRequest,
        RevokeAccessRequest, RevokeAccessResponse, RollbackTransactionRequest,
        RollbackTransactionResponse, RotateApplicationCredentialSecretRequest, SagaBlockSemantics,
        SagaCompensationKind, SagaCompensationOperationContext, SagaCompensationSpec, SagaEvent,
        SagaExecutionPolicy, SagaLifecycleState, SagaOperationContext, SagaOutcome,
        SagaRecordedCompensationOperationReceipt, SagaRecordedOperationReceipt, SagaRetryPolicy,
        SagaStatus, SagaTargetRoot, SagaTransactionBlockState, SealAppendStreamSegmentRequest,
        SealAppendStreamSegmentResponse, SealSagaCompensationProgramRequest,
        SealSagaCompensationProgramResponse, SealSagaCompensationTransactionRequest,
        SealSagaCompensationTransactionResponse, SealSagaTransactionRequest,
        SealSagaTransactionResponse, SetPublicAccessRequest, SetPublicAccessResponse,
        StartBoundaryMigrationRequest, StartHfIngestionRequest, StartHfIngestionResponse,
        StartSagaCompensationProgramRequest, StartSagaCompensationProgramResponse,
        StartSagaRequest, StartSagaResponse, StorageClassDescriptor, StorageClassResponse,
        SubmitPersonalDbChangesetRequest, SubmitPersonalDbChangesetResponse,
        TailAppendStreamRequest, TailAppendStreamResponse, TaskLease, TaskLeaseResponse,
        TenantScope, TensorIndexRow, TransactionScope, TransactionStatus, UpdateIndexRequest,
        UpdateObjectLinkRequest, UploadPartMetadata, UploadPartRequest, UploadPartResponse,
        VerifyHostAliasRequest, WatchAuthzDerivedLagRequest, WatchAuthzDerivedLagResponse,
        WatchAuthzNamespaceRequest, WatchAuthzNamespaceResponse, WatchAuthzTupleLogRequest,
        WatchAuthzTupleLogResponse, WatchBucketMetadataRequest, WatchBucketMetadataResponse,
        WatchEventEnvelope, WatchGitSourceRequest, WatchGitSourceResponse,
        WatchIndexDefinitionRequest, WatchIndexDefinitionResponse, WatchIndexPartitionRequest,
        WatchIndexPartitionResponse, WatchPersonalDbGroupRequest, WatchPersonalDbGroupResponse,
        WatchPersonalDbProjectionRequest, WatchPersonalDbProjectionResponse, WatchPrefixRequest,
        WatchPrefixResponse, WatchSagaRequest, WatchVisibilityMode, WriteAuthzTupleRequest,
        WriteAuthzTupleResponse, WriteAuthzTuplesRequest, WriteAuthzTuplesResponse, WriteOptions,
        WritePrecondition, WriteResponse, WriteResponseSagaExtension, WriteState,
        WriteVisibilityOptions, admin_service_client, audit_service_client, auth_service_client,
        bucket_service_client, coordination_service_client, get_object_response,
        git_source_service_client, hf_ingestion_service_client, hugging_face_key_service_client,
        index_service_client, mesh_control_service_client, model_manifest, model_service_client,
        mutation_batch_operation, object_service_client, personal_db_service_client,
        put_git_pack_request, put_object_request, registry_service_client, repair_service_client,
        saga_service_client, stream_service_client, transaction_service_client,
        upload_part_request, write_options, write_response_saga_extension,
    };
}

#[derive(Clone, Default)]
pub struct BearerInterceptor {
    authorization: Option<MetadataValue<Ascii>>,
}

impl BearerInterceptor {
    pub fn new(
        token: impl AsRef<str>,
    ) -> Result<Self, tonic::metadata::errors::InvalidMetadataValue> {
        Ok(Self {
            authorization: Some(bearer_metadata(token)?),
        })
    }

    pub fn anonymous() -> Self {
        Self {
            authorization: None,
        }
    }
}

impl Interceptor for BearerInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(value) = &self.authorization {
            request
                .metadata_mut()
                .insert("authorization", value.clone());
        }
        Ok(request)
    }
}

pub fn bearer_metadata(
    token: impl AsRef<str>,
) -> Result<MetadataValue<Ascii>, tonic::metadata::errors::InvalidMetadataValue> {
    let mut value: MetadataValue<Ascii> = format!("Bearer {}", token.as_ref()).parse()?;
    value.set_sensitive(true);
    Ok(value)
}

pub fn native_context_with_transaction(
    mut context: proto::NativeMutationContext,
    transaction_id: impl Into<String>,
) -> proto::NativeMutationContext {
    context.transaction_id = Some(transaction_id.into());
    context
}

pub fn write_options_with_transaction(
    mut options: proto::WriteOptions,
    transaction_id: impl Into<String>,
) -> proto::WriteOptions {
    options.execution = Some(proto::write_options::Execution::TransactionId(
        transaction_id.into(),
    ));
    options
}

#[derive(Clone)]
pub struct BeginTransaction {
    request: proto::BeginTransactionRequest,
}

impl BeginTransaction {
    pub fn new(
        idempotency_key: impl Into<String>,
        scope: proto::TransactionScope,
        ttl_ms: u64,
        purpose: impl Into<String>,
    ) -> Self {
        Self {
            request: proto::BeginTransactionRequest {
                idempotency_key: idempotency_key.into(),
                scope: Some(scope),
                preconditions: Vec::new(),
                boundary_values: Vec::new(),
                ttl_ms,
                purpose: purpose.into(),
            },
        }
    }

    pub fn with_preconditions(
        mut self,
        preconditions: impl IntoIterator<Item = proto::WritePrecondition>,
    ) -> Self {
        self.request.preconditions = preconditions.into_iter().collect();
        self
    }

    pub fn with_boundary_values(
        mut self,
        boundary_values: impl IntoIterator<Item = proto::BoundaryValue>,
    ) -> Self {
        self.request.boundary_values = boundary_values.into_iter().collect();
        self
    }

    pub fn into_proto(self) -> proto::BeginTransactionRequest {
        self.request
    }
}

impl From<BeginTransaction> for proto::BeginTransactionRequest {
    fn from(value: BeginTransaction) -> Self {
        value.into_proto()
    }
}

impl tonic::IntoRequest<proto::BeginTransactionRequest> for BeginTransaction {
    fn into_request(self) -> Request<proto::BeginTransactionRequest> {
        Request::new(self.into_proto())
    }
}

#[derive(Clone)]
pub struct CommitTransaction {
    request: proto::CommitTransactionRequest,
}

impl CommitTransaction {
    pub fn new(transaction_id: impl Into<String>, consistency: proto::ConsistencyMode) -> Self {
        Self {
            request: proto::CommitTransactionRequest {
                transaction_id: transaction_id.into(),
                consistency: consistency as i32,
                wait_for_finalization: false,
                final_preconditions: Vec::new(),
            },
        }
    }

    pub fn wait_for_finalization(mut self, wait_for_finalization: bool) -> Self {
        self.request.wait_for_finalization = wait_for_finalization;
        self
    }

    pub fn with_final_preconditions(
        mut self,
        final_preconditions: impl IntoIterator<Item = proto::WritePrecondition>,
    ) -> Self {
        self.request.final_preconditions = final_preconditions.into_iter().collect();
        self
    }

    pub fn into_proto(self) -> proto::CommitTransactionRequest {
        self.request
    }
}

impl From<CommitTransaction> for proto::CommitTransactionRequest {
    fn from(value: CommitTransaction) -> Self {
        value.into_proto()
    }
}

impl tonic::IntoRequest<proto::CommitTransactionRequest> for CommitTransaction {
    fn into_request(self) -> Request<proto::CommitTransactionRequest> {
        Request::new(self.into_proto())
    }
}

#[derive(Clone)]
pub struct RollbackTransaction {
    request: proto::RollbackTransactionRequest,
}

impl RollbackTransaction {
    pub fn new(transaction_id: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            request: proto::RollbackTransactionRequest {
                transaction_id: transaction_id.into(),
                reason: reason.into(),
            },
        }
    }

    pub fn into_proto(self) -> proto::RollbackTransactionRequest {
        self.request
    }
}

impl From<RollbackTransaction> for proto::RollbackTransactionRequest {
    fn from(value: RollbackTransaction) -> Self {
        value.into_proto()
    }
}

impl tonic::IntoRequest<proto::RollbackTransactionRequest> for RollbackTransaction {
    fn into_request(self) -> Request<proto::RollbackTransactionRequest> {
        Request::new(self.into_proto())
    }
}

#[derive(Clone)]
pub struct GetTransaction {
    request: proto::GetTransactionRequest,
}

impl GetTransaction {
    pub fn new(transaction_id: impl Into<String>) -> Self {
        Self {
            request: proto::GetTransactionRequest {
                transaction_id: transaction_id.into(),
            },
        }
    }

    pub fn into_proto(self) -> proto::GetTransactionRequest {
        self.request
    }
}

impl From<GetTransaction> for proto::GetTransactionRequest {
    fn from(value: GetTransaction) -> Self {
        value.into_proto()
    }
}

impl tonic::IntoRequest<proto::GetTransactionRequest> for GetTransaction {
    fn into_request(self) -> Request<proto::GetTransactionRequest> {
        Request::new(self.into_proto())
    }
}

#[derive(Clone)]
pub struct MutationBatch {
    request: proto::MutationBatchRequest,
}

impl MutationBatch {
    pub fn new(
        bucket_name: impl Into<String>,
        mutation_context: proto::NativeMutationContext,
    ) -> Self {
        Self {
            request: proto::MutationBatchRequest {
                bucket_name: bucket_name.into(),
                mutation_context: Some(mutation_context),
                precondition: None,
                operations: Vec::new(),
            },
        }
    }

    pub fn with_precondition(mut self, precondition: proto::WritePrecondition) -> Self {
        self.request.precondition = Some(precondition);
        self
    }

    pub fn with_operations(
        mut self,
        operations: impl IntoIterator<Item = proto::MutationBatchOperation>,
    ) -> Self {
        self.request.operations = operations.into_iter().collect();
        self
    }

    pub fn push_operation(mut self, operation: proto::MutationBatchOperation) -> Self {
        self.request.operations.push(operation);
        self
    }

    pub fn put_object(operation: proto::MutationBatchPutObject) -> proto::MutationBatchOperation {
        proto::MutationBatchOperation {
            op: Some(proto::mutation_batch_operation::Op::PutObject(operation)),
        }
    }

    pub fn patch_json_object(
        operation: proto::MutationBatchPatchJsonObject,
    ) -> proto::MutationBatchOperation {
        proto::MutationBatchOperation {
            op: Some(proto::mutation_batch_operation::Op::PatchJsonObject(
                operation,
            )),
        }
    }

    pub fn delete_object(
        operation: proto::MutationBatchDeleteObject,
    ) -> proto::MutationBatchOperation {
        proto::MutationBatchOperation {
            op: Some(proto::mutation_batch_operation::Op::DeleteObject(operation)),
        }
    }

    pub fn append_stream_record(
        operation: proto::MutationBatchAppendStreamRecord,
    ) -> proto::MutationBatchOperation {
        proto::MutationBatchOperation {
            op: Some(proto::mutation_batch_operation::Op::AppendStreamRecord(
                operation,
            )),
        }
    }

    pub fn checkpoint_task_lease(
        operation: proto::MutationBatchCheckpointTaskLease,
    ) -> proto::MutationBatchOperation {
        proto::MutationBatchOperation {
            op: Some(proto::mutation_batch_operation::Op::CheckpointTaskLease(
                operation,
            )),
        }
    }

    pub fn commit_task_lease(
        operation: proto::MutationBatchCommitTaskLease,
    ) -> proto::MutationBatchOperation {
        proto::MutationBatchOperation {
            op: Some(proto::mutation_batch_operation::Op::CommitTaskLease(
                operation,
            )),
        }
    }

    pub fn compare_and_swap_manifest(
        operation: proto::MutationBatchCompareAndSwapManifest,
    ) -> proto::MutationBatchOperation {
        proto::MutationBatchOperation {
            op: Some(proto::mutation_batch_operation::Op::CompareAndSwapManifest(
                operation,
            )),
        }
    }

    pub fn into_proto(self) -> proto::MutationBatchRequest {
        self.request
    }
}

impl From<MutationBatch> for proto::MutationBatchRequest {
    fn from(value: MutationBatch) -> Self {
        value.into_proto()
    }
}

impl tonic::IntoRequest<proto::MutationBatchRequest> for MutationBatch {
    fn into_request(self) -> Request<proto::MutationBatchRequest> {
        Request::new(self.into_proto())
    }
}

pub type InterceptedChannel =
    tonic::service::interceptor::InterceptedService<Channel, BearerInterceptor>;

impl fmt::Debug for BearerInterceptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BearerInterceptor")
            .field(
                "authorization",
                &self.authorization.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[derive(Clone)]
pub struct AnvilClient {
    channel: Channel,
    interceptor: BearerInterceptor,
}

impl fmt::Debug for AnvilClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnvilClient")
            .field("channel", &"<channel>")
            .field("interceptor", &self.interceptor)
            .finish()
    }
}

impl AnvilClient {
    pub async fn connect<D>(endpoint: D) -> Result<Self, tonic::transport::Error>
    where
        D: TryInto<Endpoint>,
        D::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let channel = Endpoint::new(endpoint)?.connect().await?;
        Ok(Self::from_channel(channel))
    }

    pub async fn connect_with_bearer<D>(
        endpoint: D,
        token: impl AsRef<str>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>>
    where
        D: TryInto<Endpoint>,
        D::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let channel = Endpoint::new(endpoint)?.connect().await?;
        Ok(Self::from_channel_with_bearer(channel, token)?)
    }

    pub fn from_channel(channel: Channel) -> Self {
        Self {
            channel,
            interceptor: BearerInterceptor::anonymous(),
        }
    }

    pub fn from_channel_with_bearer(
        channel: Channel,
        token: impl AsRef<str>,
    ) -> Result<Self, tonic::metadata::errors::InvalidMetadataValue> {
        Ok(Self {
            channel,
            interceptor: BearerInterceptor::new(token)?,
        })
    }

    pub fn admin(&self) -> proto::admin_service_client::AdminServiceClient<InterceptedChannel> {
        proto::admin_service_client::AdminServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn auth(&self) -> proto::auth_service_client::AuthServiceClient<InterceptedChannel> {
        proto::auth_service_client::AuthServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn coordination(
        &self,
    ) -> proto::coordination_service_client::CoordinationServiceClient<InterceptedChannel> {
        proto::coordination_service_client::CoordinationServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn buckets(&self) -> proto::bucket_service_client::BucketServiceClient<InterceptedChannel> {
        proto::bucket_service_client::BucketServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn objects(&self) -> proto::object_service_client::ObjectServiceClient<InterceptedChannel> {
        proto::object_service_client::ObjectServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub async fn put_boundary_schema(
        &self,
        request: impl tonic::IntoRequest<proto::PutBoundarySchemaRequest>,
    ) -> Result<tonic::Response<proto::BoundarySchemaResponse>, Status> {
        self.objects().put_boundary_schema(request).await
    }

    pub async fn get_boundary_schema(
        &self,
        request: impl tonic::IntoRequest<proto::GetBoundarySchemaRequest>,
    ) -> Result<tonic::Response<proto::BoundarySchemaResponse>, Status> {
        self.objects().get_boundary_schema(request).await
    }

    pub fn indexes(&self) -> proto::index_service_client::IndexServiceClient<InterceptedChannel> {
        proto::index_service_client::IndexServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn git_sources(
        &self,
    ) -> proto::git_source_service_client::GitSourceServiceClient<InterceptedChannel> {
        proto::git_source_service_client::GitSourceServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn personaldb(
        &self,
    ) -> proto::personal_db_service_client::PersonalDbServiceClient<InterceptedChannel> {
        proto::personal_db_service_client::PersonalDbServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn repair(&self) -> proto::repair_service_client::RepairServiceClient<InterceptedChannel> {
        proto::repair_service_client::RepairServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn hugging_face_keys(
        &self,
    ) -> proto::hugging_face_key_service_client::HuggingFaceKeyServiceClient<InterceptedChannel>
    {
        proto::hugging_face_key_service_client::HuggingFaceKeyServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn hf_ingestion(
        &self,
    ) -> proto::hf_ingestion_service_client::HfIngestionServiceClient<InterceptedChannel> {
        proto::hf_ingestion_service_client::HfIngestionServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn models(&self) -> proto::model_service_client::ModelServiceClient<InterceptedChannel> {
        proto::model_service_client::ModelServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn transactions(
        &self,
    ) -> proto::transaction_service_client::TransactionServiceClient<InterceptedChannel> {
        proto::transaction_service_client::TransactionServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub async fn begin_transaction(
        &self,
        request: impl tonic::IntoRequest<proto::BeginTransactionRequest>,
    ) -> Result<tonic::Response<proto::BeginTransactionResponse>, Status> {
        self.transactions().begin_transaction(request).await
    }

    pub async fn commit_transaction(
        &self,
        request: impl tonic::IntoRequest<proto::CommitTransactionRequest>,
    ) -> Result<tonic::Response<proto::WriteResponse>, Status> {
        self.transactions().commit_transaction(request).await
    }

    pub async fn rollback_transaction(
        &self,
        request: impl tonic::IntoRequest<proto::RollbackTransactionRequest>,
    ) -> Result<tonic::Response<proto::RollbackTransactionResponse>, Status> {
        self.transactions().rollback_transaction(request).await
    }

    pub async fn get_transaction(
        &self,
        request: impl tonic::IntoRequest<proto::GetTransactionRequest>,
    ) -> Result<tonic::Response<proto::TransactionStatus>, Status> {
        self.transactions().get_transaction(request).await
    }

    pub async fn mutation_batch(
        &self,
        request: impl tonic::IntoRequest<proto::MutationBatchRequest>,
    ) -> Result<tonic::Response<proto::MutationBatchResponse>, Status> {
        self.objects().mutation_batch(request).await
    }

    pub async fn list_storage_classes(
        &self,
        request: impl tonic::IntoRequest<proto::ListStorageClassesRequest>,
    ) -> Result<tonic::Response<proto::ListStorageClassesResponse>, Status> {
        self.admin().list_storage_classes(request).await
    }

    pub async fn get_storage_class(
        &self,
        request: impl tonic::IntoRequest<proto::GetStorageClassRequest>,
    ) -> Result<tonic::Response<proto::StorageClassResponse>, Status> {
        self.admin().get_storage_class(request).await
    }

    pub fn audit(&self) -> proto::audit_service_client::AuditServiceClient<InterceptedChannel> {
        proto::audit_service_client::AuditServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn sagas(&self) -> proto::saga_service_client::SagaServiceClient<InterceptedChannel> {
        proto::saga_service_client::SagaServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }
}

pub mod saga {
    use super::{AnvilClient, proto};
    use std::marker::PhantomData;

    const RESERVED_MESSAGE: &str =
        "Anvil saga high-level client API is reserved in this release and is not implemented";

    #[cold]
    #[track_caller]
    fn reserved() -> ! {
        panic!("{RESERVED_MESSAGE}")
    }

    #[derive(Clone, Debug)]
    pub struct StartSagaOptions {
        pub idempotency_key: String,
        pub realm_id: String,
        pub draft_ttl_ms: u64,
        pub purpose: String,
        pub execution_policy: proto::SagaExecutionPolicy,
    }

    impl StartSagaOptions {
        pub fn new(
            idempotency_key: impl Into<String>,
            realm_id: impl Into<String>,
            purpose: impl Into<String>,
        ) -> Self {
            Self {
                idempotency_key: idempotency_key.into(),
                realm_id: realm_id.into(),
                draft_ttl_ms: 0,
                purpose: purpose.into(),
                execution_policy: proto::SagaExecutionPolicy::default(),
            }
        }

        pub fn with_draft_ttl_ms(mut self, draft_ttl_ms: u64) -> Self {
            self.draft_ttl_ms = draft_ttl_ms;
            self
        }

        pub fn with_execution_policy(
            mut self,
            execution_policy: proto::SagaExecutionPolicy,
        ) -> Self {
            self.execution_policy = execution_policy;
            self
        }

        pub fn into_proto(self) -> proto::StartSagaRequest {
            proto::StartSagaRequest {
                idempotency_key: self.idempotency_key,
                realm_id: self.realm_id,
                draft_ttl_ms: self.draft_ttl_ms,
                purpose: self.purpose,
                execution_policy: Some(self.execution_policy),
            }
        }
    }

    #[derive(Clone, Debug)]
    pub struct OngoingSaga {
        client: AnvilClient,
        saga_id: String,
        draft_revision: u64,
    }

    impl OngoingSaga {
        pub async fn begin_transaction(
            &mut self,
            _target_root: proto::SagaTargetRoot,
        ) -> SagaTransaction<'_> {
            reserved()
        }

        pub async fn apply(self) -> AppliedSaga {
            reserved()
        }

        pub fn saga_id(&self) -> &str {
            &self.saga_id
        }

        pub fn draft_revision(&self) -> u64 {
            self.draft_revision
        }

        pub fn raw_saga_client(
            &self,
        ) -> proto::saga_service_client::SagaServiceClient<super::InterceptedChannel> {
            self.client.sagas()
        }
    }

    #[derive(Debug)]
    pub struct SagaTransaction<'saga> {
        saga: &'saga mut OngoingSaga,
        saga_transaction_id: String,
    }

    impl<'saga> SagaTransaction<'saga> {
        pub async fn put_object(
            &mut self,
            _request: proto::PutObjectRequest,
            _compensation: ObjectPutCompensation,
        ) -> SagaObjectWriteRef {
            reserved()
        }

        pub async fn delete_object(
            &mut self,
            _request: proto::DeleteObjectRequest,
            _compensation: ObjectDeleteCompensation,
        ) -> SagaObjectWriteRef {
            reserved()
        }

        pub async fn commit(self) -> OngoingSaga {
            reserved()
        }

        pub fn saga_transaction_id(&self) -> &str {
            &self.saga_transaction_id
        }

        pub fn saga(&self) -> &OngoingSaga {
            self.saga
        }
    }

    #[derive(Clone, Debug)]
    pub struct AppliedSaga {
        saga_id: String,
        sealed_plan_hash: String,
    }

    impl AppliedSaga {
        pub fn saga_id(&self) -> &str {
            &self.saga_id
        }

        pub fn sealed_plan_hash(&self) -> &str {
            &self.sealed_plan_hash
        }
    }

    #[derive(Clone, Debug)]
    pub struct SagaObjectWriteRef {
        pub operation_id: String,
        pub object_key: String,
    }

    #[derive(Clone, Debug)]
    pub enum ObjectPutCompensation {
        RestorePreviousHeadV1,
        ExplicitProgram(String),
        CoveredByOperation(String),
        IrreversiblePivotV1,
    }

    impl From<ObjectPutCompensation> for proto::SagaCompensationSpec {
        fn from(value: ObjectPutCompensation) -> Self {
            match value {
                ObjectPutCompensation::RestorePreviousHeadV1 => proto::SagaCompensationSpec {
                    kind: proto::SagaCompensationKind::ObjectPutRestorePreviousHeadV1 as i32,
                    ..Default::default()
                },
                ObjectPutCompensation::ExplicitProgram(compensation_program_id) => {
                    proto::SagaCompensationSpec {
                        kind: proto::SagaCompensationKind::ExplicitProgramV1 as i32,
                        compensation_program_id,
                        ..Default::default()
                    }
                }
                ObjectPutCompensation::CoveredByOperation(covered_by_operation_id) => {
                    proto::SagaCompensationSpec {
                        kind: proto::SagaCompensationKind::CoveredByOperationV1 as i32,
                        covered_by_operation_id,
                        ..Default::default()
                    }
                }
                ObjectPutCompensation::IrreversiblePivotV1 => proto::SagaCompensationSpec {
                    kind: proto::SagaCompensationKind::IrreversiblePivotV1 as i32,
                    ..Default::default()
                },
            }
        }
    }

    #[derive(Clone, Debug)]
    pub enum ObjectDeleteCompensation {
        RestorePreviousHeadV1,
        ExplicitProgram(String),
        CoveredByOperation(String),
    }

    impl From<ObjectDeleteCompensation> for proto::SagaCompensationSpec {
        fn from(value: ObjectDeleteCompensation) -> Self {
            match value {
                ObjectDeleteCompensation::RestorePreviousHeadV1 => proto::SagaCompensationSpec {
                    kind: proto::SagaCompensationKind::ObjectDeleteRestorePreviousHeadV1 as i32,
                    ..Default::default()
                },
                ObjectDeleteCompensation::ExplicitProgram(compensation_program_id) => {
                    proto::SagaCompensationSpec {
                        kind: proto::SagaCompensationKind::ExplicitProgramV1 as i32,
                        compensation_program_id,
                        ..Default::default()
                    }
                }
                ObjectDeleteCompensation::CoveredByOperation(covered_by_operation_id) => {
                    proto::SagaCompensationSpec {
                        kind: proto::SagaCompensationKind::CoveredByOperationV1 as i32,
                        covered_by_operation_id,
                        ..Default::default()
                    }
                }
            }
        }
    }

    #[derive(Clone, Debug)]
    pub struct SagaRoot<T = ()> {
        inner: proto::SagaTargetRoot,
        _marker: PhantomData<T>,
    }

    impl<T> SagaRoot<T> {
        pub fn new(inner: proto::SagaTargetRoot) -> Self {
            Self {
                inner,
                _marker: PhantomData,
            }
        }

        pub fn into_proto(self) -> proto::SagaTargetRoot {
            self.inner
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use tonic::transport::Endpoint;

        #[tokio::test]
        #[should_panic(expected = "Anvil saga high-level client API is reserved")]
        async fn high_level_start_saga_panics_until_engine_is_implemented() {
            let endpoint = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
            let client = AnvilClient::from_channel(endpoint);
            let _ = client
                .start_saga(StartSagaOptions::new("idem", "realm", "purpose"))
                .await;
        }
    }
}

impl AnvilClient {
    pub async fn start_saga(&self, _options: saga::StartSagaOptions) -> saga::OngoingSaga {
        panic!(
            "Anvil saga high-level client API is reserved in this release and is not implemented"
        )
    }
}
