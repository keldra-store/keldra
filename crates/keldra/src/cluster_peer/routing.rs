use std::collections::BTreeSet;
use std::sync::{Arc, OnceLock};

use keldra_api::v1::{
    BucketPolicy as ApiBucketPolicy, BulkWriteRequest, BulkWriteResponse, CloneObjectRequest,
    DeleteIfVersionRequest, DeleteRequest, DeleteVersionRequest, DeleteVersionResponse,
    InvokeProgramRequest, InvokeProgramResponse, LinkObjectRequest, MutationReceipt, PutToken,
    SetBucketPolicyRequest, UnlinkObjectRequest,
};
use keldra_consensus::NodeId;
use keldra_store::{DefinitionKind, DefinitionMutationIntent, PlacementLogId};
use prost::Message;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use super::{
    CLUSTER_PEER_SCHEMA_VERSION, ClusterPeerService, MAX_CLUSTER_BULK_OPERATION_TIME, encode_json,
    wire,
};

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
    atomic_executor_replay_checked: bool,
    delete_version_original_alias: Option<keldra_store::ObjectKey>,
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

    pub(crate) const fn atomic_executor_replay_checked(&self) -> bool {
        self.atomic_executor_replay_checked
    }

    pub(crate) fn delete_version_original_alias(&self) -> Option<&keldra_store::ObjectKey> {
        self.delete_version_original_alias.as_ref()
    }

    fn with_atomic_executor_replay_checked(mut self, checked: bool) -> Self {
        self.atomic_executor_replay_checked = checked;
        self
    }

    fn with_delete_version_original_alias(
        mut self,
        alias: Option<keldra_store::ObjectKey>,
    ) -> Self {
        self.delete_version_original_alias = alias;
        self
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
    async fn clone_object(
        &self,
        call: RoutedCall<CloneObjectRequest>,
    ) -> Result<MutationReceipt, Status>;
    async fn link_object(
        &self,
        call: RoutedCall<LinkObjectRequest>,
    ) -> Result<MutationReceipt, Status>;
    async fn unlink_object(
        &self,
        call: RoutedCall<UnlinkObjectRequest>,
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
    async fn replay_builtin_batch(
        &self,
        lookups: Vec<crate::programs::BuiltInReplayLookup>,
    ) -> Result<Vec<Result<Option<crate::programs::InvokedProgramResult>, Status>>, Status>;
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
        let checked = request.get_ref().atomic_executor_replay_checked;
        let (call, timeout) = self.routed_call(
            &request,
            request.get_ref().peer.as_ref(),
            request
                .get_ref()
                .request
                .clone()
                .ok_or_else(|| Status::invalid_argument("Delete request is required"))?,
        )?;
        let call = call.with_atomic_executor_replay_checked(checked);
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
        let checked = request.get_ref().atomic_executor_replay_checked;
        let (call, timeout) =
            self.routed_call(
                &request,
                request.get_ref().peer.as_ref(),
                request.get_ref().request.clone().ok_or_else(|| {
                    Status::invalid_argument("DeleteIfVersion request is required")
                })?,
            )?;
        let call = call.with_atomic_executor_replay_checked(checked);
        let fence = call.placement_fence;
        let response = tokio::time::timeout(timeout, self.routed.get()?.delete_if_version(call))
            .await
            .map_err(|_| Status::deadline_exceeded("routed DeleteIfVersion deadline exceeded"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_clone_object_call(
        &self,
        request: Request<wire::RouteCloneObjectRequest>,
    ) -> Result<Response<MutationReceipt>, Status> {
        let (call, timeout) = self.routed_call(
            &request,
            request.get_ref().peer.as_ref(),
            request
                .get_ref()
                .request
                .clone()
                .ok_or_else(|| Status::invalid_argument("CloneObject request is required"))?,
        )?;
        let fence = call.placement_fence;
        let response = tokio::time::timeout(timeout, self.routed.get()?.clone_object(call))
            .await
            .map_err(|_| Status::deadline_exceeded("routed CloneObject deadline exceeded"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_link_object_call(
        &self,
        request: Request<wire::RouteLinkObjectRequest>,
    ) -> Result<Response<MutationReceipt>, Status> {
        let (call, timeout) = self.routed_call(
            &request,
            request.get_ref().peer.as_ref(),
            request
                .get_ref()
                .request
                .clone()
                .ok_or_else(|| Status::invalid_argument("LinkObject request is required"))?,
        )?;
        let fence = call.placement_fence;
        let response = tokio::time::timeout(timeout, self.routed.get()?.link_object(call))
            .await
            .map_err(|_| Status::deadline_exceeded("routed LinkObject deadline exceeded"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_unlink_object_call(
        &self,
        request: Request<wire::RouteUnlinkObjectRequest>,
    ) -> Result<Response<MutationReceipt>, Status> {
        let (call, timeout) = self.routed_call(
            &request,
            request.get_ref().peer.as_ref(),
            request
                .get_ref()
                .request
                .clone()
                .ok_or_else(|| Status::invalid_argument("UnlinkObject request is required"))?,
        )?;
        let fence = call.placement_fence;
        let response = tokio::time::timeout(timeout, self.routed.get()?.unlink_object(call))
            .await
            .map_err(|_| Status::deadline_exceeded("routed UnlinkObject deadline exceeded"))??;
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
        let (call, timeout) =
            self.routed_bulk_call(&request, request.get_ref().peer.as_ref(), bulk)?;
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
            self.routed_bulk_call(&request, request.get_ref().peer.as_ref(), bulk)?;
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
        let original_alias = request
            .get_ref()
            .original_alias
            .as_ref()
            .map(|alias| {
                keldra_store::ObjectKey::new(&alias.tenant, &alias.bucket, &alias.path)
                    .map_err(|error| Status::invalid_argument(error.to_string()))
            })
            .transpose()?;
        let (call, timeout) = self.routed_call(&request, request.get_ref().peer.as_ref(), value)?;
        let call = call.with_delete_version_original_alias(original_alias);
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

    pub(super) async fn route_builtin_replay_batch_call(
        &self,
        request: Request<wire::RouteBuiltInReplayBatchRequest>,
    ) -> Result<Response<wire::RouteBuiltInReplayBatchResponse>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let nomination = self.require_program_executor(
            &admitted.placement,
            admitted.authenticated.node_id,
            request.get_ref().executor_nomination_log_index,
        )?;
        if request.get_ref().lookups.len() > keldra_store::MAX_ATOMIC_BATCH_MUTATIONS {
            return Err(Status::resource_exhausted(
                "built-in replay batch exceeds the atomic mutation bound",
            ));
        }
        let mut original_indices = std::collections::BTreeSet::new();
        let lookups = request
            .get_ref()
            .lookups
            .iter()
            .map(|lookup| {
                if !original_indices.insert(lookup.original_index) {
                    return Err(Status::invalid_argument(
                        "built-in replay original indices must be unique",
                    ));
                }
                Ok(crate::programs::BuiltInReplayLookup {
                    original_index: lookup.original_index,
                    authority_kind: u16::try_from(lookup.authority_kind).map_err(|_| {
                        Status::invalid_argument("built-in replay kind exceeds u16")
                    })?,
                    contract_version: u16::try_from(lookup.contract_version).map_err(|_| {
                        Status::invalid_argument("built-in replay contract exceeds u16")
                    })?,
                    invocation_id: lookup.invocation_id.as_slice().try_into().map_err(|_| {
                        Status::invalid_argument("built-in replay invocation id must be 32 bytes")
                    })?,
                    input_fingerprint: lookup.input_fingerprint.as_slice().try_into().map_err(
                        |_| {
                            Status::invalid_argument("built-in replay fingerprint must be 32 bytes")
                        },
                    )?,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let results = tokio::time::timeout(
            admitted.timeout,
            self.routed.get()?.replay_builtin_batch(lookups),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("built-in replay batch deadline exceeded"))??;
        if results.len() != request.get_ref().lookups.len() {
            return Err(Status::data_loss(
                "built-in replay handler returned the wrong outcome cardinality",
            ));
        }
        self.require_program_fence(admitted.placement.fence(), nomination)?;
        let outcomes = request
            .get_ref()
            .lookups
            .iter()
            .zip(results)
            .map(|(lookup, result)| match result {
                Ok(result) => Ok(wire::BuiltInReplayOutcome {
                    original_index: lookup.original_index,
                    result_json: result
                        .as_ref()
                        .map(encode_json)
                        .transpose()?
                        .unwrap_or_default(),
                    error_code: 0,
                    error_message: String::new(),
                }),
                Err(error) => Ok(wire::BuiltInReplayOutcome {
                    original_index: lookup.original_index,
                    result_json: Vec::new(),
                    error_code: error.code() as i32,
                    error_message: error.message().to_owned(),
                }),
            })
            .collect::<Result<Vec<_>, Status>>()?;
        Ok(Response::new(wire::RouteBuiltInReplayBatchResponse {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            outcomes,
        }))
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
                atomic_executor_replay_checked: false,
                delete_version_original_alias: None,
            },
            admitted.timeout,
        ))
    }

    fn routed_bulk_call<T>(
        &self,
        request: &Request<impl Sized>,
        context: Option<&wire::PeerContext>,
        value: T,
    ) -> Result<(RoutedCall<T>, std::time::Duration), Status> {
        let admitted = self.admit_with_timeout_limit(
            request,
            context,
            1,
            self.bulk_write_timeout.min(MAX_CLUSTER_BULK_OPERATION_TIME),
        )?;
        let bearer = bearer_from_metadata(request.metadata())?;
        Ok((
            RoutedCall {
                bearer,
                source_node: admitted.authenticated.node_id,
                placement_fence: admitted.placement.fence(),
                request: value,
                definition_intents: Vec::new(),
                atomic_executor_replay_checked: false,
                delete_version_original_alias: None,
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
