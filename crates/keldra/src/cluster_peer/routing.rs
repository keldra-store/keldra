use std::collections::BTreeSet;
use std::sync::{Arc, OnceLock};

use keldra_api::v1::{
    BucketPolicy as ApiBucketPolicy, BulkWriteRequest, BulkWriteResponse, DeleteIfVersionRequest,
    DeleteRequest, DeleteVersionRequest, DeleteVersionResponse, InvokeProgramRequest,
    InvokeProgramResponse, MutationReceipt, PutToken, SetBucketPolicyRequest,
};
use keldra_consensus::NodeId;
use keldra_store::{DefinitionKind, DefinitionMutationIntent, PlacementLogId};
use prost::Message;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use super::{ClusterPeerService, wire};

const MAX_ROUTED_BULK_ITEMS: usize = 1_000;
const MAX_ROUTED_BULK_BYTES: usize = 64 * 1024 * 1024;

/// A one-hop call reconstructed from authenticated metadata and a typed
/// protobuf request. The bearer remains signed and unevaluated so the final
/// handler must perform the authoritative JWT and Zanzibar checks itself.
pub(crate) struct RoutedCall<T> {
    bearer: Arc<str>,
    source_node: NodeId,
    placement_fence: PlacementLogId,
    request: T,
    definition_intents: Vec<(usize, DefinitionMutationIntent)>,
}

impl<T> RoutedCall<T> {
    pub(crate) fn bearer(&self) -> &str {
        &self.bearer
    }

    pub(crate) const fn source_node(&self) -> NodeId {
        self.source_node
    }

    pub(crate) const fn placement_fence(&self) -> PlacementLogId {
        self.placement_fence
    }

    pub(crate) fn request(&self) -> &T {
        &self.request
    }

    pub(crate) fn definition_intents(&self) -> &[(usize, DefinitionMutationIntent)] {
        &self.definition_intents
    }

    pub(crate) fn into_request(self) -> T {
        self.request
    }
}

#[tonic::async_trait]
/// The production implementation must independently verify the signed bearer,
/// perform the Zanzibar check, resolve mutable names, and prove that this node
/// is the coordinator under `call.placement_fence()` before mutating storage.
pub(crate) trait RoutedPublicHandler: Send + Sync + 'static {
    async fn put_end(&self, call: RoutedCall<PutToken>) -> Result<MutationReceipt, Status>;
    async fn delete(&self, call: RoutedCall<DeleteRequest>) -> Result<MutationReceipt, Status>;
    async fn delete_if_version(
        &self,
        call: RoutedCall<DeleteIfVersionRequest>,
    ) -> Result<MutationReceipt, Status>;
    async fn bulk_write(
        &self,
        call: RoutedCall<BulkWriteRequest>,
    ) -> Result<BulkWriteResponse, Status>;
    async fn internal_put_end(&self, call: RoutedCall<PutToken>)
    -> Result<MutationReceipt, Status>;
    async fn internal_delete_if_version(
        &self,
        call: RoutedCall<DeleteIfVersionRequest>,
    ) -> Result<MutationReceipt, Status>;
    async fn internal_bulk_write(
        &self,
        call: RoutedCall<BulkWriteRequest>,
    ) -> Result<BulkWriteResponse, Status>;
    async fn set_bucket_policy(
        &self,
        call: RoutedCall<SetBucketPolicyRequest>,
    ) -> Result<ApiBucketPolicy, Status>;
    async fn delete_version(
        &self,
        call: RoutedCall<DeleteVersionRequest>,
    ) -> Result<DeleteVersionResponse, Status>;
    async fn invoke_program(
        &self,
        call: RoutedCall<InvokeProgramRequest>,
    ) -> Result<InvokeProgramResponse, Status>;
}

/// One-time late binding lets the private listener start before join and
/// serving-fence recovery finish. No request queues while the handler is absent.
#[derive(Clone, Default)]
pub(crate) struct RoutedPublicHandlers {
    inner: Arc<OnceLock<Arc<dyn RoutedPublicHandler>>>,
}

impl RoutedPublicHandlers {
    pub(crate) fn install(
        &self,
        handler: Arc<dyn RoutedPublicHandler>,
    ) -> Result<(), Arc<dyn RoutedPublicHandler>> {
        self.inner.set(handler)
    }

    fn get(&self) -> Result<Arc<dyn RoutedPublicHandler>, Status> {
        self.inner
            .get()
            .cloned()
            .ok_or_else(|| Status::unavailable("routed public handler is not ready"))
    }
}

impl ClusterPeerService {
    pub(super) async fn route_put_end_call(
        &self,
        request: Request<wire::RoutePutEndRequest>,
    ) -> Result<Response<MutationReceipt>, Status> {
        let (call, timeout) = self.routed_call(
            &request,
            request.get_ref().peer.as_ref(),
            request
                .get_ref()
                .request
                .clone()
                .ok_or_else(|| Status::invalid_argument("PutEnd request is required"))?,
        )?;
        let fence = call.placement_fence;
        let response = tokio::time::timeout(timeout, self.routed.get()?.put_end(call))
            .await
            .map_err(|_| Status::deadline_exceeded("routed PutEnd deadline exceeded"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_delete_call(
        &self,
        request: Request<wire::RouteDeleteRequest>,
    ) -> Result<Response<MutationReceipt>, Status> {
        let (call, timeout) = self.routed_call(
            &request,
            request.get_ref().peer.as_ref(),
            request
                .get_ref()
                .request
                .clone()
                .ok_or_else(|| Status::invalid_argument("Delete request is required"))?,
        )?;
        let fence = call.placement_fence;
        let response = tokio::time::timeout(timeout, self.routed.get()?.delete(call))
            .await
            .map_err(|_| Status::deadline_exceeded("routed Delete deadline exceeded"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_delete_if_version_call(
        &self,
        request: Request<wire::RouteDeleteIfVersionRequest>,
    ) -> Result<Response<MutationReceipt>, Status> {
        let (call, timeout) =
            self.routed_call(
                &request,
                request.get_ref().peer.as_ref(),
                request.get_ref().request.clone().ok_or_else(|| {
                    Status::invalid_argument("DeleteIfVersion request is required")
                })?,
            )?;
        let fence = call.placement_fence;
        let response = tokio::time::timeout(timeout, self.routed.get()?.delete_if_version(call))
            .await
            .map_err(|_| Status::deadline_exceeded("routed DeleteIfVersion deadline exceeded"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_bulk_write_call(
        &self,
        request: Request<wire::RouteBulkWriteRequest>,
    ) -> Result<Response<BulkWriteResponse>, Status> {
        let bulk = request
            .get_ref()
            .request
            .clone()
            .ok_or_else(|| Status::invalid_argument("BulkWrite request is required"))?;
        if bulk.operations.is_empty()
            || bulk.operations.len() > MAX_ROUTED_BULK_ITEMS
            || bulk.encoded_len() > MAX_ROUTED_BULK_BYTES
        {
            return Err(Status::resource_exhausted(
                "routed bulk must contain 1..=1000 items within 64 MiB",
            ));
        }
        if !request.get_ref().definition_intents.is_empty() {
            return Err(Status::permission_denied(
                "definition mutation evidence is valid only on the internal bulk route",
            ));
        }
        let (call, timeout) = self.routed_call(&request, request.get_ref().peer.as_ref(), bulk)?;
        let fence = call.placement_fence;
        let response = tokio::time::timeout(timeout, self.routed.get()?.bulk_write(call))
            .await
            .map_err(|_| Status::deadline_exceeded("routed BulkWrite deadline exceeded"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_internal_delete_if_version_call(
        &self,
        request: Request<wire::RouteDeleteIfVersionRequest>,
    ) -> Result<Response<MutationReceipt>, Status> {
        let (call, timeout) = self.routed_call(
            &request,
            request.get_ref().peer.as_ref(),
            request.get_ref().request.clone().ok_or_else(|| {
                Status::invalid_argument("internal DeleteIfVersion request is required")
            })?,
        )?;
        let fence = call.placement_fence;
        let response =
            tokio::time::timeout(timeout, self.routed.get()?.internal_delete_if_version(call))
                .await
                .map_err(|_| {
                    Status::deadline_exceeded("routed internal DeleteIfVersion deadline exceeded")
                })??;
        self.require_unchanged(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_internal_put_end_call(
        &self,
        request: Request<wire::RoutePutEndRequest>,
    ) -> Result<Response<MutationReceipt>, Status> {
        let (call, timeout) =
            self.routed_call(
                &request,
                request.get_ref().peer.as_ref(),
                request.get_ref().request.clone().ok_or_else(|| {
                    Status::invalid_argument("internal PutEnd request is required")
                })?,
            )?;
        let fence = call.placement_fence;
        let response = tokio::time::timeout(timeout, self.routed.get()?.internal_put_end(call))
            .await
            .map_err(|_| Status::deadline_exceeded("routed internal PutEnd deadline exceeded"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_internal_bulk_write_call(
        &self,
        request: Request<wire::RouteBulkWriteRequest>,
    ) -> Result<Response<BulkWriteResponse>, Status> {
        let bulk =
            request.get_ref().request.clone().ok_or_else(|| {
                Status::invalid_argument("internal BulkWrite request is required")
            })?;
        if bulk.operations.is_empty()
            || bulk.operations.len() > MAX_ROUTED_BULK_ITEMS
            || bulk.encoded_len() > MAX_ROUTED_BULK_BYTES
        {
            return Err(Status::resource_exhausted(
                "routed internal bulk must contain 1..=1000 items within 64 MiB",
            ));
        }
        let definition_intents = decode_definition_intents(
            &request.get_ref().definition_intents,
            bulk.operations.len(),
        )?;
        let (mut call, timeout) =
            self.routed_call(&request, request.get_ref().peer.as_ref(), bulk)?;
        call.definition_intents = definition_intents;
        let fence = call.placement_fence;
        let response = tokio::time::timeout(timeout, self.routed.get()?.internal_bulk_write(call))
            .await
            .map_err(|_| {
                Status::deadline_exceeded("routed internal BulkWrite deadline exceeded")
            })??;
        self.require_unchanged(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_set_bucket_policy_call(
        &self,
        request: Request<wire::RouteSetBucketPolicyRequest>,
    ) -> Result<Response<ApiBucketPolicy>, Status> {
        let value = request
            .get_ref()
            .request
            .clone()
            .ok_or_else(|| Status::invalid_argument("SetBucketPolicy request is required"))?;
        let (call, timeout) = self.routed_call(&request, request.get_ref().peer.as_ref(), value)?;
        let fence = call.placement_fence;
        let response = tokio::time::timeout(timeout, self.routed.get()?.set_bucket_policy(call))
            .await
            .map_err(|_| Status::deadline_exceeded("routed SetBucketPolicy deadline exceeded"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_delete_version_call(
        &self,
        request: Request<wire::RouteDeleteVersionRequest>,
    ) -> Result<Response<DeleteVersionResponse>, Status> {
        let value = request
            .get_ref()
            .request
            .clone()
            .ok_or_else(|| Status::invalid_argument("DeleteVersion request is required"))?;
        let (call, timeout) = self.routed_call(&request, request.get_ref().peer.as_ref(), value)?;
        let fence = call.placement_fence;
        let response = tokio::time::timeout(timeout, self.routed.get()?.delete_version(call))
            .await
            .map_err(|_| Status::deadline_exceeded("routed DeleteVersion deadline exceeded"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_invoke_program_call(
        &self,
        request: Request<wire::RouteInvokeProgramRequest>,
    ) -> Result<Response<InvokeProgramResponse>, Status> {
        let value = request
            .get_ref()
            .request
            .clone()
            .ok_or_else(|| Status::invalid_argument("InvokeProgram request is required"))?;
        let (call, timeout) = self.routed_call(&request, request.get_ref().peer.as_ref(), value)?;
        let fence = call.placement_fence;
        let response = tokio::time::timeout(timeout, self.routed.get()?.invoke_program(call))
            .await
            .map_err(|_| Status::deadline_exceeded("routed InvokeProgram deadline exceeded"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(response))
    }

    pub(super) fn routed_call<T>(
        &self,
        request: &Request<impl Sized>,
        context: Option<&wire::PeerContext>,
        value: T,
    ) -> Result<(RoutedCall<T>, std::time::Duration), Status> {
        let admitted = self.admit(request, context, 1)?;
        let bearer = bearer_from_metadata(request.metadata())?;
        Ok((
            RoutedCall {
                bearer,
                source_node: admitted.authenticated.node_id,
                placement_fence: admitted.placement.fence(),
                request: value,
                definition_intents: Vec::new(),
            },
            admitted.timeout,
        ))
    }

    pub(super) fn require_unchanged(&self, expected: PlacementLogId) -> Result<(), Status> {
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
        let placement = crate::cluster_placement::ClusterPlacement::from_applied(&state)
            .map_err(|error| Status::unavailable(error.to_string()))?;
        if placement.fence() == expected {
            Ok(())
        } else {
            Err(Status::unavailable(
                "active placement changed during cluster operation",
            ))
        }
    }
}

fn decode_definition_intents(
    values: &[wire::RoutedDefinitionMutationIntent],
    operation_count: usize,
) -> Result<Vec<(usize, DefinitionMutationIntent)>, Status> {
    if values.len() > operation_count {
        return Err(Status::invalid_argument(
            "too many routed definition mutation intents",
        ));
    }
    let mut seen = BTreeSet::new();
    let mut intents = Vec::with_capacity(values.len());
    for value in values {
        let operation_index = value.operation_index as usize;
        if operation_index >= operation_count || !seen.insert(operation_index) {
            return Err(Status::invalid_argument(
                "routed definition mutation intent has an invalid operation index",
            ));
        }
        let kind = match wire::RoutedDefinitionKind::try_from(value.kind) {
            Ok(wire::RoutedDefinitionKind::Index) => DefinitionKind::Index,
            Ok(wire::RoutedDefinitionKind::Accounting) => DefinitionKind::Accounting,
            Ok(wire::RoutedDefinitionKind::Unspecified) | Err(_) => {
                return Err(Status::invalid_argument(
                    "routed definition mutation intent has an invalid kind",
                ));
            }
        };
        let intent = DefinitionMutationIntent::new(kind, value.definition_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        intents.push((operation_index, intent));
    }
    Ok(intents)
}

fn bearer_from_metadata(metadata: &MetadataMap) -> Result<Arc<str>, Status> {
    let mut values = metadata.get_all("authorization").iter();
    let value = values
        .next()
        .ok_or_else(|| Status::unauthenticated("a bearer token is required"))?;
    if values.next().is_some() {
        return Err(Status::unauthenticated(
            "exactly one bearer token is required",
        ));
    }
    let value = value
        .to_str()
        .map_err(|_| Status::unauthenticated("the bearer token is malformed"))?;
    let token = value
        .strip_prefix("Bearer ")
        .filter(|token| !token.is_empty())
        .ok_or_else(|| Status::unauthenticated("the bearer token is malformed"))?;
    Ok(Arc::from(token))
}

#[cfg(test)]
pub(super) fn test_bearer(metadata: &MetadataMap) -> Result<Arc<str>, Status> {
    bearer_from_metadata(metadata)
}
