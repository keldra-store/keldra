use anvil_api::v1::object_service_server::ObjectService;
use anvil_api::v1::put_header::Operation;
use anvil_api::v1::{
    DeleteRequest, Durability, GetObjectRequest, HeadObjectRequest, ListObjectsRequest,
    ListObjectsResponse, MutationReceipt, ObjectAddress, ObjectHead, PutHeader,
    PutIfAbsentOperation, PutIfVersionOperation, PutOperation,
};
use anvil_store::{BlobRef, ObjectKey, StorageTenantId};
use axum::body::Body;
use http_body_util::BodyExt as _;
use sha2::{Digest as _, Sha256};
use subtle::ConstantTimeEq as _;
use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use super::{
    CanonicalPutHeader, GetObjectStream, ObjectServiceImpl, require_upload_phase,
    write_upload_chunk,
};
use crate::authentication::{AnonymousObjectRequest, Caller, JwtManager};
use crate::authorization::ObjectPermission;
use crate::distributed_list::OriginalBearer;
use crate::object_path_access;

#[derive(Clone, Debug)]
pub(crate) enum GatewayIdentity {
    Authenticated { caller: Caller, bearer: String },
    Anonymous,
}

impl GatewayIdentity {
    pub(crate) fn authenticated(tokens: &JwtManager, caller: Caller) -> Result<Self, Status> {
        let app_id = caller
            .authenticated_app_id()
            .map_err(|_| Status::internal("gateway caller is not an application"))?;
        let bearer = tokens
            .mint(caller.storage_tenant().clone(), app_id)
            .map_err(|_| Status::internal("gateway bearer could not be minted"))?;
        Ok(Self::Authenticated { caller, bearer })
    }

    fn request<T>(&self, value: T) -> Result<Request<T>, Status> {
        let mut request = Request::new(value);
        match self {
            Self::Authenticated { caller, bearer } => {
                let value = format!("Bearer {bearer}")
                    .parse::<MetadataValue<_>>()
                    .map_err(|_| Status::internal("gateway bearer is malformed"))?;
                request.metadata_mut().insert("authorization", value);
                request.extensions_mut().insert(caller.clone());
            }
            Self::Anonymous => {
                request.extensions_mut().insert(AnonymousObjectRequest);
            }
        }
        Ok(request)
    }

    fn internal_request<T>(&self, value: T) -> Result<Request<T>, Status> {
        let mut request = self.request(value)?;
        object_path_access::mark_gateway(&mut request);
        Ok(request)
    }

    pub(crate) fn caller(&self) -> Option<&Caller> {
        match self {
            Self::Authenticated { caller, .. } => Some(caller),
            Self::Anonymous => None,
        }
    }

    pub(crate) fn original_bearer(&self) -> Result<OriginalBearer, Status> {
        match self {
            Self::Authenticated { bearer, .. } => {
                Ok(OriginalBearer::from_signed_token(bearer.clone()))
            }
            Self::Anonymous => Err(Status::unauthenticated(
                "anonymous gateway request has no bearer",
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GatewayPutMode {
    Put,
    IfAbsent,
    IfVersion(u64),
}

#[derive(Clone)]
pub(crate) struct GatewayObjectAdapter {
    service: ObjectServiceImpl,
}

pub(crate) struct GatewayPutResult {
    pub(crate) receipt: MutationReceipt,
    pub(crate) content_length: u64,
}

impl GatewayObjectAdapter {
    pub(crate) fn new(service: ObjectServiceImpl) -> Self {
        Self { service }
    }

    pub(crate) fn record_gateway_ingress(&self, key: &ObjectKey, bytes: u64) {
        self.service.record_gateway_ingress(key, bytes);
    }

    pub(crate) fn record_gateway_egress(&self, key: &ObjectKey, bytes: u64) {
        self.service.record_gateway_egress(key, bytes);
    }

    pub(crate) async fn head(
        &self,
        identity: &GatewayIdentity,
        key: &ObjectKey,
    ) -> Result<ObjectHead, Status> {
        ObjectService::head_object(
            &self.service,
            identity.request(HeadObjectRequest {
                address: Some(address(key)),
            })?,
        )
        .await
        .map(|response| response.into_inner())
    }

    pub(crate) async fn get(
        &self,
        identity: &GatewayIdentity,
        key: &ObjectKey,
    ) -> Result<GetObjectStream, Status> {
        ObjectService::get_object(
            &self.service,
            identity.request(GetObjectRequest {
                address: Some(address(key)),
                version: None,
            })?,
        )
        .await
        .map(|response| response.into_inner())
    }

    pub(crate) async fn list(
        &self,
        identity: &GatewayIdentity,
        tenant: &str,
        bucket: &str,
        prefix: String,
        start_after: Option<String>,
        limit: u32,
    ) -> Result<ListObjectsResponse, Status> {
        ObjectService::list_objects(
            &self.service,
            identity.request(ListObjectsRequest {
                tenant: tenant.to_owned(),
                bucket: bucket.to_owned(),
                prefix,
                start_after,
                limit,
            })?,
        )
        .await
        .map(|response| response.into_inner())
    }

    pub(crate) async fn delete(
        &self,
        identity: &GatewayIdentity,
        key: &ObjectKey,
        command_id: String,
    ) -> Result<MutationReceipt, Status> {
        ObjectService::delete(
            &self.service,
            identity.request(DeleteRequest {
                address: Some(address(key)),
                command_id,
                durability: Durability::Local as i32,
            })?,
        )
        .await
        .map(|response| response.into_inner())
    }

    pub(crate) async fn put(
        &self,
        identity: &GatewayIdentity,
        key: &ObjectKey,
        content_type: Option<String>,
        command_id: String,
        mode: GatewayPutMode,
        expected_sha256: Option<[u8; 32]>,
        body: Body,
    ) -> Result<GatewayPutResult, Status> {
        self.put_with_access(
            identity,
            key,
            content_type,
            command_id,
            mode,
            expected_sha256,
            body,
            false,
        )
        .await
    }

    pub(crate) async fn git_head(
        &self,
        identity: &GatewayIdentity,
        key: &ObjectKey,
    ) -> Result<ObjectHead, Status> {
        require_git_key(key)?;
        ObjectService::head_object(
            &self.service,
            identity.internal_request(HeadObjectRequest {
                address: Some(address(key)),
            })?,
        )
        .await
        .map(|response| response.into_inner())
    }

    pub(crate) async fn git_get(
        &self,
        identity: &GatewayIdentity,
        key: &ObjectKey,
    ) -> Result<GetObjectStream, Status> {
        require_git_key(key)?;
        let caller = match identity {
            GatewayIdentity::Authenticated { caller, .. } => caller.clone(),
            GatewayIdentity::Anonymous => Caller::from_anonymous(
                StorageTenantId::parse(key.tenant())
                    .map_err(|error| Status::invalid_argument(error.to_string()))?,
            ),
        };
        self.service
            .authorize_object(&caller, key, ObjectPermission::Get)
            .await?;
        ObjectService::get_object(
            &self.service,
            identity.internal_request(GetObjectRequest {
                address: Some(address(key)),
                version: None,
            })?,
        )
        .await
        .map(|response| response.into_inner())
    }

    pub(crate) async fn git_require_write(
        &self,
        identity: &GatewayIdentity,
        key: &ObjectKey,
    ) -> Result<(), Status> {
        require_git_key(key)?;
        let caller = identity
            .caller()
            .ok_or_else(|| Status::unauthenticated("Git push requires credentials"))?;
        self.service
            .authorize_object(caller, key, ObjectPermission::Put)
            .await
    }

    pub(crate) async fn git_put(
        &self,
        identity: &GatewayIdentity,
        key: &ObjectKey,
        command_id: String,
        mode: GatewayPutMode,
        body: Body,
    ) -> Result<GatewayPutResult, Status> {
        require_git_key(key)?;
        self.put_with_access(
            identity,
            key,
            Some("application/vnd.git.bundle".into()),
            command_id,
            mode,
            None,
            body,
            true,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn put_with_access(
        &self,
        identity: &GatewayIdentity,
        key: &ObjectKey,
        content_type: Option<String>,
        command_id: String,
        mode: GatewayPutMode,
        expected_sha256: Option<[u8; 32]>,
        mut body: Body,
        internal: bool,
    ) -> Result<GatewayPutResult, Status> {
        if matches!(identity, GatewayIdentity::Anonymous) {
            return Err(Status::unauthenticated(
                "gateway writes require credentials",
            ));
        }
        let operation = match mode {
            GatewayPutMode::Put => Operation::Put(PutOperation {}),
            GatewayPutMode::IfAbsent => Operation::PutIfAbsent(PutIfAbsentOperation {}),
            GatewayPutMode::IfVersion(expected_version) => {
                Operation::PutIfVersion(PutIfVersionOperation { expected_version })
            }
        };
        let header = PutHeader {
            address: Some(address(key)),
            content_type: content_type.unwrap_or_default(),
            command_id,
            durability: Durability::Local as i32,
            operation: Some(operation),
        };
        let request = if internal {
            identity.internal_request(header)?
        } else {
            identity.request(header)?
        };
        let upload_token = ObjectService::start_put(&self.service, request)
            .await?
            .into_inner();
        let caller = match identity {
            GatewayIdentity::Authenticated { caller, .. } => caller,
            GatewayIdentity::Anonymous => unreachable!(),
        };
        let capability = self.service.verify_put_token(caller, &upload_token)?;
        let header: CanonicalPutHeader = require_upload_phase(capability)?;
        let mut upload = self
            .service
            .store
            .begin_blob_upload()
            .await
            .map_err(super::status)?;
        let mut length = 0_u64;
        let mut sha256 = expected_sha256.map(|_| Sha256::new());
        while let Some(frame) = body
            .frame()
            .await
            .transpose()
            .map_err(|error| Status::invalid_argument(format!("upload body failed: {error}")))?
        {
            let bytes = frame
                .into_data()
                .map_err(|_| Status::invalid_argument("upload body trailers are not supported"))?;
            if let Some(hasher) = sha256.as_mut() {
                hasher.update(&bytes);
            }
            write_upload_chunk(
                &mut upload,
                &mut length,
                &bytes,
                self.service.max_blob_bytes,
            )
            .await?;
        }
        if let (Some(expected), Some(actual)) = (expected_sha256, sha256) {
            if actual.finalize().as_slice().ct_eq(&expected).unwrap_u8() != 1 {
                return Err(Status::invalid_argument(
                    "request body does not match x-amz-content-sha256",
                ));
            }
        }
        let blob: BlobRef = self
            .service
            .store
            .seal_blob_upload(upload)
            .await
            .map_err(super::status)?;
        let ready = self.service.issue_ready_token(caller, header, &blob)?;
        let request = if internal {
            identity.internal_request(ready)?
        } else {
            identity.request(ready)?
        };
        let receipt = ObjectService::put_end(&self.service, request)
            .await
            .map(|response| response.into_inner())?;
        if !internal {
            self.service.record_gateway_ingress(key, length);
        }
        Ok(GatewayPutResult {
            receipt,
            content_length: length,
        })
    }
}

fn require_git_key(key: &ObjectKey) -> Result<(), Status> {
    let suffix = key
        .path()
        .strip_prefix("_anvil/git/")
        .ok_or_else(|| Status::permission_denied("Git adapter key is outside _anvil/git"))?;
    if suffix.is_empty() || suffix.split('/').any(str::is_empty) {
        return Err(Status::invalid_argument("Git adapter key is malformed"));
    }
    Ok(())
}

fn address(key: &ObjectKey) -> ObjectAddress {
    ObjectAddress {
        tenant: key.tenant().to_owned(),
        bucket: key.bucket().to_owned(),
        path: key.path().to_owned(),
    }
}
