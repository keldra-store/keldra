use std::pin::Pin;

use anvil_api::v1::batch_get_outcome::Outcome as BatchGetResult;
use anvil_api::v1::object_chunk::Value as ObjectChunkValue;
use anvil_api::v1::object_head::State as ObjectState;
use anvil_api::v1::object_service_server::ObjectService;
use anvil_api::v1::write_condition::Condition;
use anvil_api::v1::{
    BatchGetObject, BatchGetOutcome, BatchGetRequest, BatchGetResponse, BlobRef as ApiBlobRef,
    BucketPolicy, BulkOperation, BulkOutcome, BulkWriteRequest, BulkWriteResponse,
    DeleteObjectRequest, DeletedObject, GetObjectRequest, HeadObjectRequest, InvokeProgramRequest,
    InvokeProgramResponse, MutationFailure, MutationFailureCode,
    MutationReceipt as ApiMutationReceipt, NeverExisted, ObjectAddress, ObjectChunk, ObjectHead,
    PresentObject, PublishObjectRequest, PutObjectRequest, SetBucketPolicyRequest, UploadBlobChunk,
    WriteCondition,
};
use anvil_store::{
    AuthzStoreError, BatchOperation, BlobReader, BlobRef, DeleteRequest, MutationError,
    MutationReceipt, ObjectKey, Precondition, PublishRequest, PutRequest, Store, Version,
    VersionId,
};
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::authentication::Caller;
use crate::authorization::{ObjectPermission, SystemAuthorization, SystemAuthorizer};
use crate::programs::ProgramCoordinator;

const OBJECT_CHUNK_BYTES: usize = 64 * 1024;
const MAX_BULK_ITEMS: usize = 1_000;
const MAX_BULK_BYTES: usize = 64 * 1024 * 1024;
const MAX_BATCH_GET_ITEMS: usize = 1_000;
const MAX_BATCH_GET_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone)]
pub struct ObjectServiceImpl {
    store: Store,
    system_authorizer: SystemAuthorizer,
    _programs: ProgramCoordinator,
    max_blob_bytes: u64,
}

impl ObjectServiceImpl {
    pub(crate) fn new(store: Store, programs: ProgramCoordinator, max_blob_bytes: u64) -> Self {
        Self {
            system_authorizer: SystemAuthorizer::new(store.authz()),
            store,
            _programs: programs,
            max_blob_bytes,
        }
    }

    async fn system_authorization(&self) -> Result<SystemAuthorization, Status> {
        let authorizer = self.system_authorizer.clone();
        tokio::task::spawn_blocking(move || authorizer.load())
            .await
            .map_err(|error| internal(format!("authorization worker failed: {error}")))?
            .map_err(authorization_store_status)
    }
}

type GetObjectStream =
    Pin<Box<dyn Stream<Item = Result<ObjectChunk, Status>> + Send + Sync + 'static>>;

#[tonic::async_trait]
impl ObjectService for ObjectServiceImpl {
    async fn upload_blob(
        &self,
        request: Request<Streaming<UploadBlobChunk>>,
    ) -> Result<Response<ApiBlobRef>, Status> {
        let _caller = authenticated_caller(&request)?;
        let mut stream = request.into_inner();
        let mut upload = self.store.begin_blob_upload().await.map_err(status)?;
        let mut length = 0_u64;
        while let Some(chunk) = stream.message().await? {
            length = length
                .checked_add(chunk.bytes.len() as u64)
                .ok_or_else(|| Status::resource_exhausted("blob length overflow"))?;
            if length > self.max_blob_bytes {
                return Err(Status::resource_exhausted("blob exceeds server limit"));
            }
            upload.write(&chunk.bytes).await.map_err(internal)?;
        }
        let blob = upload.finish().await.map_err(internal)?;
        Ok(Response::new(api_blob(&blob)))
    }

    async fn put_object(
        &self,
        request: Request<PutObjectRequest>,
    ) -> Result<Response<ApiMutationReceipt>, Status> {
        let caller = authenticated_caller(&request)?;
        let request = request.into_inner();
        if request.bytes.len() as u64 > self.max_blob_bytes {
            return Err(Status::resource_exhausted("object exceeds server limit"));
        }
        let request = put_request(request)?;
        self.authorize_object(&caller, &request.key, ObjectPermission::Put)
            .await?;
        self.store
            .put(request)
            .await
            .map(api_receipt)
            .map(Response::new)
            .map_err(status)
    }

    async fn publish_object(
        &self,
        request: Request<PublishObjectRequest>,
    ) -> Result<Response<ApiMutationReceipt>, Status> {
        let caller = authenticated_caller(&request)?;
        let request = request.into_inner();
        if request
            .blob
            .as_ref()
            .is_some_and(|blob| blob.length > self.max_blob_bytes)
        {
            return Err(Status::resource_exhausted("object exceeds server limit"));
        }
        let request = publish_request(request)?;
        self.authorize_object(&caller, &request.key, ObjectPermission::Put)
            .await?;
        self.store
            .publish(request)
            .await
            .map(api_receipt)
            .map(Response::new)
            .map_err(status)
    }

    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<ApiMutationReceipt>, Status> {
        let caller = authenticated_caller(&request)?;
        let request = request.into_inner();
        let request = delete_request(request)?;
        self.authorize_object(&caller, &request.key, ObjectPermission::Delete)
            .await?;
        self.store
            .delete(request)
            .await
            .map(api_receipt)
            .map(Response::new)
            .map_err(status)
    }

    async fn head_object(
        &self,
        request: Request<HeadObjectRequest>,
    ) -> Result<Response<ObjectHead>, Status> {
        let caller = authenticated_caller(&request)?;
        let key = object_key(request.into_inner().address)?;
        self.authorize_object(&caller, &key, ObjectPermission::Get)
            .await?;
        let Some(head) = self.store.head(&key).map_err(status)? else {
            return Ok(Response::new(never_existed()));
        };
        let version = self
            .store
            .version_metadata(&key, head.version)
            .map_err(status)?
            .ok_or_else(|| Status::data_loss("object head references a missing version"))?;
        Ok(Response::new(api_head(&version)))
    }

    type GetObjectStream = GetObjectStream;

    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<Self::GetObjectStream>, Status> {
        let caller = authenticated_caller(&request)?;
        let request = request.into_inner();
        let key = object_key(request.address)?;
        self.authorize_object(&caller, &key, ObjectPermission::Get)
            .await?;
        let selected = select_object_for_stream(&self.store, &key, request.version).await?;
        let (sender, receiver) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            let (head, payload) = match selected {
                Some(object) => (api_head(&object.version), object.payload),
                None => (never_existed(), SelectedPayload::Empty),
            };
            if sender
                .send(Ok(ObjectChunk {
                    value: Some(ObjectChunkValue::Head(head)),
                }))
                .await
                .is_err()
            {
                return;
            }
            match payload {
                SelectedPayload::Empty => {}
                SelectedPayload::Inline(bytes) => {
                    for bytes in bytes.chunks(OBJECT_CHUNK_BYTES) {
                        if sender
                            .send(Ok(ObjectChunk {
                                value: Some(ObjectChunkValue::Bytes(bytes.to_vec())),
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
                SelectedPayload::Blob(mut reader) => {
                    let mut bytes = vec![0_u8; OBJECT_CHUNK_BYTES];
                    loop {
                        let read = match reader.read(&mut bytes).await {
                            Ok(0) => break,
                            Ok(read) => read,
                            Err(error) => {
                                let _ = sender.send(Err(internal(error))).await;
                                return;
                            }
                        };
                        if sender
                            .send(Ok(ObjectChunk {
                                value: Some(ObjectChunkValue::Bytes(bytes[..read].to_vec())),
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    async fn bulk_write(
        &self,
        request: Request<BulkWriteRequest>,
    ) -> Result<Response<BulkWriteResponse>, Status> {
        let caller = authenticated_caller(&request)?;
        let operations = request.into_inner().operations;
        validate_bulk_limits(&operations)?;
        let authorization = self.system_authorization().await?;

        let mut accepted = Vec::with_capacity(operations.len());
        let mut outcomes = Vec::new();
        for (index, operation) in operations.into_iter().enumerate() {
            match batch_operation(operation, self.max_blob_bytes) {
                Ok(operation) => {
                    match authorize_batch_operation(&authorization, &caller, &operation) {
                        Ok(()) => accepted.push((index, operation)),
                        Err(error) if error.code() == tonic::Code::PermissionDenied => {
                            outcomes.push(BulkOutcome {
                                index: index as u32,
                                outcome: Some(anvil_api::v1::bulk_outcome::Outcome::Failure(
                                    MutationFailure {
                                        code: MutationFailureCode::AuthorizationDenied as i32,
                                        message: error.message().to_owned(),
                                        current_version: None,
                                    },
                                )),
                            });
                        }
                        Err(error) => return Err(error),
                    }
                }
                Err(error) => outcomes.push(BulkOutcome {
                    index: index as u32,
                    outcome: Some(anvil_api::v1::bulk_outcome::Outcome::Failure(
                        api_request_failure(error),
                    )),
                }),
            }
        }
        let accepted_indices = accepted.iter().map(|(index, _)| *index).collect::<Vec<_>>();
        let accepted_operations = accepted
            .into_iter()
            .map(|(_, operation)| operation)
            .collect();
        outcomes.extend(
            self.store
                .bulk_write(accepted_operations)
                .await
                .into_iter()
                .map(|outcome| BulkOutcome {
                    index: accepted_indices[outcome.index] as u32,
                    outcome: Some(match outcome.result {
                        Ok(receipt) => {
                            anvil_api::v1::bulk_outcome::Outcome::Receipt(api_receipt(receipt))
                        }
                        Err(error) => {
                            anvil_api::v1::bulk_outcome::Outcome::Failure(api_failure(error))
                        }
                    }),
                }),
        );
        outcomes.sort_unstable_by_key(|outcome| outcome.index);
        Ok(Response::new(BulkWriteResponse { outcomes }))
    }

    async fn batch_get(
        &self,
        request: Request<BatchGetRequest>,
    ) -> Result<Response<BatchGetResponse>, Status> {
        let caller = authenticated_caller(&request)?;
        let objects = request.into_inner().objects;
        if objects.len() > MAX_BATCH_GET_ITEMS {
            return Err(Status::resource_exhausted(format!(
                "batch read contains more than {MAX_BATCH_GET_ITEMS} items"
            )));
        }
        let authorization = self.system_authorization().await?;
        let mut accepted = Vec::with_capacity(objects.len());
        let mut outcomes = Vec::new();
        for (index, request) in objects.into_iter().enumerate() {
            let address = request.address.clone();
            let key = match object_key(request.address) {
                Ok(key) => key,
                Err(error) => {
                    outcomes.push(BatchGetOutcome {
                        index: index as u32,
                        address,
                        outcome: Some(BatchGetResult::Error(error.message().to_owned())),
                    });
                    continue;
                }
            };
            let authorization_result = require_caller_tenant(&caller, &key).and_then(|()| {
                authorization
                    .allows_object(caller.subject(), &key, ObjectPermission::Get)
                    .map_err(crate::authz_api::authz_status)
                    .and_then(|allowed| {
                        require_authorized(allowed, "object read is not authorized")
                    })
            });
            match authorization_result {
                Ok(()) => accepted.push((index, key, request.version.map(VersionId))),
                Err(error) if error.code() == tonic::Code::PermissionDenied => {
                    outcomes.push(BatchGetOutcome {
                        index: index as u32,
                        address: Some(api_address(&key)),
                        outcome: Some(BatchGetResult::Error(error.message().to_owned())),
                    });
                }
                Err(error) => return Err(error),
            }
        }
        let requests = accepted
            .iter()
            .map(|(_, key, version)| (key.clone(), *version))
            .collect::<Vec<_>>();
        let selection = self.store.select_batch_get(&requests);
        enforce_batch_get_payload_limit(selection.declared_present_payload_bytes())?;
        for (outcome, (index, key, requested_version)) in self
            .store
            .read_batch_get_selection(selection)
            .await
            .into_iter()
            .zip(accepted)
        {
            let outcome = match outcome {
                Ok(Some(object)) => BatchGetResult::Object(BatchGetObject {
                    head: Some(api_head(&object.version)),
                    bytes: object.bytes,
                }),
                Ok(None) if requested_version.is_none() => BatchGetResult::Object(BatchGetObject {
                    head: Some(never_existed()),
                    bytes: Vec::new(),
                }),
                Ok(None) => BatchGetResult::Error("requested version was not found".into()),
                Err(error) => BatchGetResult::Error(error.to_string()),
            };
            outcomes.push(BatchGetOutcome {
                index: index as u32,
                address: Some(api_address(&key)),
                outcome: Some(outcome),
            });
        }
        outcomes.sort_unstable_by_key(|outcome| outcome.index);
        Ok(Response::new(BatchGetResponse { outcomes }))
    }

    async fn set_bucket_policy(
        &self,
        request: Request<SetBucketPolicyRequest>,
    ) -> Result<Response<BucketPolicy>, Status> {
        let caller = authenticated_caller(&request)?;
        let request = request.into_inner();
        let policy = request
            .policy
            .ok_or_else(|| Status::invalid_argument("policy is required"))?;
        let key = ObjectKey::new(&request.tenant, &request.bucket, "_anvil/policy")
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        require_caller_tenant(&caller, &key)?;
        let authorization = self.system_authorization().await?;
        require_authorized(
            authorization
                .allows_bucket_policy(caller.subject(), &request.tenant, &request.bucket)
                .map_err(crate::authz_api::authz_status)?,
            "bucket policy mutation is not authorized",
        )?;
        self.store
            .set_bucket_policy(
                &request.tenant,
                &request.bucket,
                anvil_store::BucketPolicy {
                    create_once_prefixes: policy.immutable_path_prefixes.clone(),
                    program_only_prefixes: policy.program_only_path_prefixes.clone(),
                },
            )
            .await
            .map_err(status)?;
        Ok(Response::new(policy))
    }

    async fn invoke_program(
        &self,
        request: Request<InvokeProgramRequest>,
    ) -> Result<Response<InvokeProgramResponse>, Status> {
        let caller = authenticated_caller(&request)?;
        let program = object_key(request.into_inner().program)?;
        self.authorize_object(&caller, &program, ObjectPermission::Get)
            .await?;
        Err(Status::unimplemented(
            "the program definition's full object address is unresolved; no bucket is inferred",
        ))
    }
}

impl ObjectServiceImpl {
    async fn authorize_object(
        &self,
        caller: &Caller,
        key: &ObjectKey,
        permission: ObjectPermission,
    ) -> Result<(), Status> {
        require_caller_tenant(caller, key)?;
        let authorization = self.system_authorization().await?;
        require_authorized(
            authorization
                .allows_object(caller.subject(), key, permission)
                .map_err(crate::authz_api::authz_status)?,
            "object operation is not authorized",
        )
    }
}

fn authenticated_caller<T>(request: &Request<T>) -> Result<Caller, Status> {
    request
        .extensions()
        .get::<Caller>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("authenticated caller identity is missing"))
}

fn require_caller_tenant(caller: &Caller, key: &ObjectKey) -> Result<(), Status> {
    if caller.storage_tenant().as_str() == key.tenant() {
        Ok(())
    } else {
        Err(Status::permission_denied(
            "object address does not belong to the authenticated tenant",
        ))
    }
}

fn require_authorized(allowed: bool, message: &'static str) -> Result<(), Status> {
    if allowed {
        Ok(())
    } else {
        Err(Status::permission_denied(message))
    }
}

fn authorize_batch_operation(
    authorization: &SystemAuthorization,
    caller: &Caller,
    operation: &BatchOperation,
) -> Result<(), Status> {
    let (key, permission) = match operation {
        BatchOperation::Put(request) => (&request.key, ObjectPermission::Put),
        BatchOperation::Publish(request) => (&request.key, ObjectPermission::Put),
        BatchOperation::Delete(request) => (&request.key, ObjectPermission::Delete),
    };
    require_caller_tenant(caller, key)?;
    require_authorized(
        authorization
            .allows_object(caller.subject(), key, permission)
            .map_err(crate::authz_api::authz_status)?,
        "bulk object operation is not authorized",
    )
}

fn authorization_store_status(error: AuthzStoreError) -> Status {
    match error {
        AuthzStoreError::MissingBinding(_, _) | AuthzStoreError::SchemaNotFound(_, _) => {
            Status::failed_precondition(error.to_string())
        }
        AuthzStoreError::Authorization(error) => crate::authz_api::authz_status(error),
        AuthzStoreError::InvalidInput(_) => Status::invalid_argument(error.to_string()),
        AuthzStoreError::RevisionConflict { .. }
        | AuthzStoreError::BindingGenerationConflict { .. }
        | AuthzStoreError::RevisionNotAvailable { .. }
        | AuthzStoreError::OperationMismatch => Status::failed_precondition(error.to_string()),
        AuthzStoreError::Storage(_) => Status::internal(error.to_string()),
    }
}

struct SelectedObject {
    version: Version,
    payload: SelectedPayload,
}

enum SelectedPayload {
    Empty,
    Inline(Vec<u8>),
    Blob(BlobReader),
}

async fn select_object_for_stream(
    store: &Store,
    key: &ObjectKey,
    requested_version: Option<u64>,
) -> Result<Option<SelectedObject>, Status> {
    let version = match requested_version {
        Some(version) => store
            .version_metadata(key, VersionId(version))
            .map_err(status)?
            .ok_or_else(|| Status::not_found("requested version was not found"))?,
        None => {
            let Some(head) = store.head(key).map_err(status)? else {
                return Ok(None);
            };
            store
                .version_metadata(key, head.version)
                .map_err(status)?
                .ok_or_else(|| Status::data_loss("object head references a missing version"))?
        }
    };
    let payload = match (&version.inline, &version.blob, version.deleted) {
        (Some(inline), None, false) if inline.is_valid() => {
            SelectedPayload::Inline(inline.bytes.clone())
        }
        (None, Some(blob), false) => {
            SelectedPayload::Blob(store.open_blob(blob).await.map_err(status)?)
        }
        (None, None, true) => SelectedPayload::Empty,
        _ => {
            return Err(Status::internal("version has an invalid payload shape"));
        }
    };
    Ok(Some(SelectedObject { version, payload }))
}

fn validate_bulk_limits(operations: &[BulkOperation]) -> Result<(), Status> {
    if operations.len() > MAX_BULK_ITEMS {
        return Err(Status::resource_exhausted(format!(
            "bulk contains more than {MAX_BULK_ITEMS} items"
        )));
    }
    let mut payload_bytes = 0_usize;
    for operation in operations {
        if let Some(anvil_api::v1::bulk_operation::Operation::Put(request)) =
            operation.operation.as_ref()
        {
            payload_bytes = payload_bytes
                .checked_add(request.bytes.len())
                .ok_or_else(|| Status::resource_exhausted("bulk payload overflow"))?;
        }
    }
    if payload_bytes > MAX_BULK_BYTES {
        return Err(Status::resource_exhausted(format!(
            "bulk payload exceeds {MAX_BULK_BYTES} bytes"
        )));
    }
    Ok(())
}

fn put_request(request: PutObjectRequest) -> Result<PutRequest, Status> {
    require_durability_class(&request.durability_class)?;
    Ok(PutRequest {
        key: object_key(request.address)?,
        bytes: request.bytes,
        content_type: nonempty(request.content_type),
        precondition: precondition(request.condition)?,
        command_id: Some(required_command_id(request.command_id)?),
        durability_class: request.durability_class,
    })
}

fn publish_request(request: PublishObjectRequest) -> Result<PublishRequest, Status> {
    require_durability_class(&request.durability_class)?;
    Ok(PublishRequest {
        key: object_key(request.address)?,
        blob: blob(request.blob)?,
        content_type: nonempty(request.content_type),
        precondition: precondition(request.condition)?,
        command_id: Some(required_command_id(request.command_id)?),
        durability_class: request.durability_class,
    })
}

fn delete_request(request: DeleteObjectRequest) -> Result<DeleteRequest, Status> {
    require_durability_class(&request.durability_class)?;
    Ok(DeleteRequest {
        key: object_key(request.address)?,
        precondition: delete_precondition(request.condition)?,
        command_id: Some(required_command_id(request.command_id)?),
        durability_class: request.durability_class,
    })
}

fn batch_operation(
    operation: BulkOperation,
    max_blob_bytes: u64,
) -> Result<BatchOperation, Status> {
    match operation.operation {
        Some(anvil_api::v1::bulk_operation::Operation::Put(request)) => {
            let request = put_request(request)?;
            if request.bytes.len() as u64 > max_blob_bytes {
                return Err(Status::resource_exhausted(
                    "bulk put item exceeds the object-size limit",
                ));
            }
            Ok(BatchOperation::Put(request))
        }
        Some(anvil_api::v1::bulk_operation::Operation::Publish(request)) => {
            let request = publish_request(request)?;
            if request.blob.length > max_blob_bytes {
                return Err(Status::resource_exhausted(
                    "bulk publish item exceeds the object-size limit",
                ));
            }
            Ok(BatchOperation::Publish(request))
        }
        Some(anvil_api::v1::bulk_operation::Operation::Delete(request)) => {
            delete_request(request).map(BatchOperation::Delete)
        }
        None => Err(Status::invalid_argument("bulk operation is required")),
    }
}

fn api_request_failure(error: Status) -> MutationFailure {
    let code = match error.code() {
        tonic::Code::ResourceExhausted => MutationFailureCode::ResourceLimit,
        tonic::Code::Unavailable => MutationFailureCode::DurabilityUnavailable,
        _ => MutationFailureCode::Invalid,
    };
    MutationFailure {
        code: code as i32,
        message: error.message().to_owned(),
        current_version: None,
    }
}

fn object_key(address: Option<ObjectAddress>) -> Result<ObjectKey, Status> {
    let address = address.ok_or_else(|| Status::invalid_argument("object address is required"))?;
    ObjectKey::new(address.tenant, address.bucket, address.path)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

fn precondition(value: Option<WriteCondition>) -> Result<Precondition, Status> {
    match value.and_then(|value| value.condition) {
        None | Some(Condition::Any(true)) => Ok(Precondition::Any),
        Some(Condition::Absent(true)) => Ok(Precondition::Absent),
        Some(Condition::Version(version)) => Ok(Precondition::Version(VersionId(version))),
        Some(_) => Err(Status::invalid_argument(
            "boolean condition marker must be true",
        )),
    }
}

fn delete_precondition(value: Option<WriteCondition>) -> Result<Precondition, Status> {
    let condition = precondition(value)?;
    match condition {
        Precondition::Any | Precondition::Version(_) => Ok(condition),
        Precondition::Absent => Err(Status::invalid_argument(
            "delete condition must be any or an exact version",
        )),
    }
}

fn blob(value: Option<ApiBlobRef>) -> Result<BlobRef, Status> {
    let value = value.ok_or_else(|| Status::invalid_argument("blob reference is required"))?;
    let hash: [u8; 32] = value
        .blake3_hash
        .try_into()
        .map_err(|_| Status::invalid_argument("BLAKE3 hash must contain 32 bytes"))?;
    Ok(BlobRef {
        hash,
        length: value.length,
    })
}

fn api_blob(blob: &BlobRef) -> ApiBlobRef {
    ApiBlobRef {
        blake3_hash: blob.hash.to_vec(),
        length: blob.length,
    }
}

fn api_head(version: &Version) -> ObjectHead {
    let state = if version.deleted {
        ObjectState::Deleted(DeletedObject {
            version: version.id.0,
        })
    } else {
        let blob = version.blob.as_ref().map(api_blob).or_else(|| {
            version.inline.as_ref().map(|inline| ApiBlobRef {
                blake3_hash: inline.hash.to_vec(),
                length: inline.length,
            })
        });
        ObjectState::Present(PresentObject {
            version: version.id.0,
            blob,
            content_type: version.content_type.clone().unwrap_or_default(),
        })
    };
    ObjectHead { state: Some(state) }
}

fn never_existed() -> ObjectHead {
    ObjectHead {
        state: Some(ObjectState::NeverExisted(NeverExisted {})),
    }
}

fn api_receipt(receipt: MutationReceipt) -> ApiMutationReceipt {
    ApiMutationReceipt {
        command_id: receipt.command_id.unwrap_or_default(),
        fingerprint: receipt.fingerprint.to_vec(),
        version: receipt.version.0,
        deleted: receipt.deleted,
        replayed: receipt.replayed,
    }
}

fn api_address(key: &ObjectKey) -> ObjectAddress {
    ObjectAddress {
        tenant: key.tenant().into(),
        bucket: key.bucket().into(),
        path: key.path().into(),
    }
}

fn api_failure(error: MutationError) -> MutationFailure {
    let (code, current_version) = match &error {
        MutationError::PreconditionFailed { current } => (
            MutationFailureCode::ConditionFailed,
            current.map(|value| value.0),
        ),
        MutationError::Immutable => (MutationFailureCode::Immutable, None),
        MutationError::ProgramOnly => (MutationFailureCode::ProgramOnly, None),
        MutationError::IdempotencyConflict => (MutationFailureCode::IdempotencyInputMismatch, None),
        MutationError::InvalidCommandId
        | MutationError::InvalidPolicy(_)
        | MutationError::BlobNotFound => (MutationFailureCode::Invalid, None),
        MutationError::Storage(_) => (MutationFailureCode::Internal, None),
    };
    MutationFailure {
        code: code as i32,
        message: error.to_string(),
        current_version,
    }
}

fn status(error: MutationError) -> Status {
    match error {
        MutationError::PreconditionFailed { .. }
        | MutationError::Immutable
        | MutationError::ProgramOnly => Status::failed_precondition(error.to_string()),
        MutationError::IdempotencyConflict => Status::already_exists(error.to_string()),
        MutationError::InvalidCommandId | MutationError::InvalidPolicy(_) => {
            Status::invalid_argument(error.to_string())
        }
        MutationError::BlobNotFound => Status::not_found(error.to_string()),
        MutationError::Storage(_) => Status::internal(error.to_string()),
    }
}

fn require_durability_class(value: &str) -> Result<(), Status> {
    if value.trim().is_empty() || value.len() > 256 {
        return Err(Status::invalid_argument(
            "durability_class must contain between 1 and 256 bytes",
        ));
    }
    Ok(())
}

fn enforce_batch_get_payload_limit(declared_payload_bytes: u64) -> Result<(), Status> {
    if declared_payload_bytes > MAX_BATCH_GET_BYTES as u64 {
        return Err(Status::resource_exhausted(format!(
            "batch response exceeds {MAX_BATCH_GET_BYTES} bytes"
        )));
    }
    Ok(())
}

fn internal(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

fn nonempty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

fn required_command_id(value: String) -> Result<String, Status> {
    nonempty(value).ok_or_else(|| Status::invalid_argument("command_id is required"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(path: &str) -> Option<ObjectAddress> {
        Some(ObjectAddress {
            tenant: "tenant".into(),
            bucket: "bucket".into(),
            path: path.into(),
        })
    }

    #[test]
    fn never_existed_and_deleted_remain_distinct() {
        assert!(matches!(
            never_existed().state,
            Some(ObjectState::NeverExisted(_))
        ));
        let deleted = Version {
            id: VersionId(9),
            blob: None,
            inline: None,
            content_type: None,
            deleted: true,
            committed_at_unix_millis: 0,
        };
        assert!(matches!(
            api_head(&deleted).state,
            Some(ObjectState::Deleted(DeletedObject { version: 9 }))
        ));
    }

    #[test]
    fn delete_rejects_the_put_only_absent_condition() {
        let absent = WriteCondition {
            condition: Some(Condition::Absent(true)),
        };
        assert_eq!(
            delete_precondition(Some(absent)).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );

        let exact = WriteCondition {
            condition: Some(Condition::Version(17)),
        };
        assert_eq!(
            delete_precondition(Some(exact)).unwrap(),
            Precondition::Version(VersionId(17))
        );
    }

    #[test]
    fn invalid_bulk_item_is_reported_as_an_item_failure() {
        let operation = BulkOperation {
            operation: Some(anvil_api::v1::bulk_operation::Operation::Put(
                PutObjectRequest {
                    address: Some(ObjectAddress {
                        tenant: "tenant".into(),
                        bucket: "bucket".into(),
                        path: "object".into(),
                    }),
                    bytes: vec![0; 2],
                    command_id: "command".into(),
                    durability_class: "configured".into(),
                    ..Default::default()
                },
            )),
        };

        let error = batch_operation(operation, 1).unwrap_err();
        let failure = api_request_failure(error);
        assert_eq!(failure.code, MutationFailureCode::ResourceLimit as i32);
    }

    #[test]
    fn bulk_conversion_preserves_each_opaque_durability_class() {
        let operations = [
            BulkOperation {
                operation: Some(anvil_api::v1::bulk_operation::Operation::Put(
                    PutObjectRequest {
                        address: address("put"),
                        bytes: b"value".to_vec(),
                        command_id: "put-command".into(),
                        durability_class: "put-opaque".into(),
                        ..Default::default()
                    },
                )),
            },
            BulkOperation {
                operation: Some(anvil_api::v1::bulk_operation::Operation::Publish(
                    PublishObjectRequest {
                        address: address("publish"),
                        blob: Some(ApiBlobRef {
                            blake3_hash: vec![7; 32],
                            length: 5,
                        }),
                        command_id: "publish-command".into(),
                        durability_class: "publish-opaque".into(),
                        ..Default::default()
                    },
                )),
            },
            BulkOperation {
                operation: Some(anvil_api::v1::bulk_operation::Operation::Delete(
                    DeleteObjectRequest {
                        address: address("delete"),
                        command_id: "delete-command".into(),
                        durability_class: "delete-opaque".into(),
                        ..Default::default()
                    },
                )),
            },
        ];

        let classes = operations
            .into_iter()
            .map(
                |operation| match batch_operation(operation, u64::MAX).unwrap() {
                    BatchOperation::Put(request) => request.durability_class,
                    BatchOperation::Publish(request) => request.durability_class,
                    BatchOperation::Delete(request) => request.durability_class,
                },
            )
            .collect::<Vec<_>>();
        assert_eq!(classes, ["put-opaque", "publish-opaque", "delete-opaque"]);
    }

    #[test]
    fn durability_class_shape_is_bounded_without_interpreting_names() {
        assert!(require_durability_class("x").is_ok());
        assert!(require_durability_class(&"x".repeat(256)).is_ok());
        assert!(require_durability_class("").is_err());
        assert!(require_durability_class("  ").is_err());
        assert!(require_durability_class(&"x".repeat(257)).is_err());
    }

    #[test]
    fn batch_get_payload_limit_accepts_the_boundary_and_rejects_larger_totals() {
        assert!(enforce_batch_get_payload_limit(MAX_BATCH_GET_BYTES as u64).is_ok());
        let error = enforce_batch_get_payload_limit(MAX_BATCH_GET_BYTES as u64 + 1).unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    }
}
