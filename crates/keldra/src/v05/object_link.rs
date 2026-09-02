use crate::cluster_object_read::ClusterOpenedObject;
use keldra_api::v1::{
    DeleteIfVersionRequest, DeleteRequest as ApiDeleteRequest, LinkObjectRequest, MutationReceipt,
    PutToken, UnlinkObjectRequest,
};
use keldra_atomic_program::{CommandReceipt, HeadPrecondition, ObjectPath, ObservedHead};
use keldra_store::{
    BlobRef, BuiltInAliasObservation, BuiltInAliasRegistryAccess, BuiltInObjectTransactionPlan,
    BuiltInReadProof, BuiltInTransactionAssertion, BuiltInVersionWrite, BuiltInWritePayload,
    CurrentObjectSnapshot, OBJECT_LINK_CONTENT_TYPE, ObjectAliasRegistry, ObjectKey,
    ObjectLinkDescriptor, ObjectMutationGovernance, PROGRAM_PARTICIPANT_MANIFEST_FORMAT,
    Precondition, ProgramAliasRegistryCondition, ProgramGovernanceParticipant,
    ProgramObjectParticipant, ProgramParticipantIntent, ProgramParticipantManifest,
    ProgramPathCondition, PublishRequest, PutMode, ResolvedObjectLink, Version,
    object_link_command_fingerprint, resolve_descriptor,
};
use std::collections::BTreeMap;
use tonic::{Request, Response, Status};

use super::{
    ObjectServiceImpl, api_receipt, deadline_remaining, durability, object_key,
    plugin_object_scope, request_deadline, require_plugin_key_scope, required_command_id,
};
use crate::authorization::ObjectPermission;
use crate::distributed_list::OriginalBearer;
use crate::object_path_access;
use crate::v05::request_auth::authenticated_caller;
use crate::v05::routed_writes;

const LINK_OBJECT_AUTHORITY_KIND: u16 = 2;
pub(super) const UNLINK_OBJECT_AUTHORITY_KIND: u16 = 3;
pub(super) const PUT_THROUGH_LINK_AUTHORITY_KIND: u16 = 4;
pub(super) const PUT_IMMUTABLE_THROUGH_LINK_AUTHORITY_KIND: u16 = 5;
pub(super) const OBJECT_LINK_CONTRACT_VERSION: u16 = 1;

#[derive(Clone)]
pub(super) enum ResolvedAddress {
    Ordinary(ObjectKey),
    Link(ResolvedObjectLink),
}

impl ResolvedAddress {
    pub(super) fn canonical(&self) -> &ObjectKey {
        match self {
            Self::Ordinary(key) => key,
            Self::Link(link) => &link.target,
        }
    }
}

/// Resolves at most one protected descriptor. A descriptor target which is
/// itself a descriptor is corrupt persisted state: link creation must flatten
/// it before commit.
pub(super) async fn resolve_current(
    service: &ObjectServiceImpl,
    key: ObjectKey,
) -> Result<ResolvedAddress, Status> {
    let (tenant_id, bucket_id) = service
        .name_resolver
        .resolve_bucket_ids(key.tenant(), key.bucket())
        .await?;
    resolve_current_with_ids(service, key, tenant_id, bucket_id).await
}

pub(super) async fn resolve_current_with_ids(
    service: &ObjectServiceImpl,
    key: ObjectKey,
    tenant_id: u64,
    bucket_id: u64,
) -> Result<ResolvedAddress, Status> {
    let mut current = service
        .reader
        .current_head_snapshots_stable(
            std::slice::from_ref(&key),
            tenant_id,
            bucket_id,
            service.atomic_program_timeout,
        )
        .await?;
    let current = current
        .pop()
        .ok_or_else(|| Status::internal("current object batch omitted its requested path"))?;
    resolve_current_snapshot(service, key, current, tenant_id, bucket_id).await
}

pub(super) async fn resolve_current_batch_with_ids(
    service: &ObjectServiceImpl,
    keys: &[ObjectKey],
    tenant_id: u64,
    bucket_id: u64,
) -> Result<Vec<Result<ResolvedAddress, Status>>, Status> {
    let current = service
        .reader
        .current_head_snapshots_stable(keys, tenant_id, bucket_id, service.atomic_program_timeout)
        .await?;
    if current.len() != keys.len() {
        return Err(Status::data_loss(
            "current object batch returned the wrong result count",
        ));
    }
    let mut resolved = Vec::with_capacity(keys.len());
    for (key, current) in keys.iter().cloned().zip(current) {
        resolved.push(resolve_current_snapshot(service, key, current, tenant_id, bucket_id).await);
    }
    Ok(resolved)
}

async fn resolve_current_snapshot(
    service: &ObjectServiceImpl,
    key: ObjectKey,
    current: Option<CurrentObjectSnapshot>,
    tenant_id: u64,
    bucket_id: u64,
) -> Result<ResolvedAddress, Status> {
    let Some(current) = current else {
        return Ok(ResolvedAddress::Ordinary(key));
    };
    let Some((descriptor_version, descriptor_blob)) = protected_descriptor(&current.version)?
    else {
        return Ok(ResolvedAddress::Ordinary(key));
    };
    let encoded = service.reader.read_blob_bytes(&descriptor_blob).await?;
    let resolved = decode_protected_descriptor(key, descriptor_version, &encoded)?;

    let target = current_target_with_ids(service, &resolved.target, tenant_id, bucket_id)
        .await?
        .ok_or_else(|| Status::data_loss("protected object-link descriptor has no live target"))?;
    validate_resolved_target(&resolved, &target)?;
    Ok(ResolvedAddress::Link(resolved))
}

fn validate_resolved_target(
    resolved: &ResolvedObjectLink,
    target: &CurrentObjectSnapshot,
) -> Result<(), Status> {
    if target
        .alias_registry
        .as_ref()
        .and_then(|registry| {
            registry
                .aliases
                .binary_search_by(|path| path.as_str().cmp(resolved.link.path()))
                .ok()
        })
        .is_none()
    {
        return Err(Status::data_loss(
            "protected object-link descriptor is absent from its target sidecar",
        ));
    }
    if target.version.deleted {
        return Err(Status::data_loss(
            "object-link target is deleted despite protected registry membership",
        ));
    }
    require_ordinary_link_target(&target.version)?;
    Ok(())
}

fn protected_descriptor(
    version: &Version,
) -> Result<Option<(keldra_store::VersionId, BlobRef)>, Status> {
    if !version.protected_link_descriptor {
        return Ok(None);
    }
    if version.content_type.as_deref() != Some(OBJECT_LINK_CONTENT_TYPE) {
        return Err(Status::data_loss(
            "protected object-link descriptor has the wrong content type",
        ));
    }
    let blob = version
        .blob
        .clone()
        .ok_or_else(|| Status::data_loss("object-link descriptor has no payload"))?;
    Ok(Some((version.id, blob)))
}

fn decode_protected_descriptor(
    key: ObjectKey,
    descriptor_version: keldra_store::VersionId,
    encoded: &[u8],
) -> Result<ResolvedObjectLink, Status> {
    let descriptor = ObjectLinkDescriptor::decode(encoded)
        .map_err(|error| Status::data_loss(error.to_string()))?;
    resolve_descriptor(key, descriptor_version, &descriptor)
        .map_err(|error| Status::data_loss(error.to_string()))
}

fn require_ordinary_link_target(version: &Version) -> Result<(), Status> {
    if version.protected_link_descriptor {
        Err(Status::data_loss(
            "protected object-link descriptor points to another protected descriptor",
        ))
    } else {
        Ok(())
    }
}

#[tonic::async_trait]
impl crate::index_service::IndexLiveVersionReader for ObjectServiceImpl {
    async fn resolved_current_snapshots(
        &self,
        keys: &[ObjectKey],
        tenant_id: u64,
        bucket_id: u64,
        budget: std::time::Duration,
    ) -> Result<Vec<crate::index_service::ResolvedIndexCurrentSnapshot>, Status> {
        let deadline = tokio::time::Instant::now() + budget;
        let current = self
            .reader
            .current_head_snapshots_stable(keys, tenant_id, bucket_id, budget)
            .await?;
        let mut resolved = Vec::with_capacity(keys.len());
        for (key, snapshot) in keys.iter().cloned().zip(current) {
            if snapshot
                .as_ref()
                .is_none_or(|snapshot| !snapshot.version.protected_link_descriptor)
            {
                resolved.push(crate::index_service::ResolvedIndexCurrentSnapshot {
                    canonical: key,
                    snapshot,
                });
                continue;
            }
            let address = resolve_current(self, key.clone()).await?;
            let ResolvedAddress::Link(link) = &address else {
                resolved.push(crate::index_service::ResolvedIndexCurrentSnapshot {
                    canonical: key,
                    snapshot,
                });
                continue;
            };
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(Status::deadline_exceeded(
                    "index alias resolution deadline exceeded",
                ));
            }
            let target = self
                .reader
                .current_head_snapshots_stable(
                    std::slice::from_ref(&link.target),
                    tenant_id,
                    bucket_id,
                    remaining,
                )
                .await?
                .pop()
                .flatten();
            if !revalidate(self, &address).await? {
                resolved.push(crate::index_service::ResolvedIndexCurrentSnapshot {
                    canonical: key,
                    snapshot: None,
                });
                continue;
            }
            resolved.push(crate::index_service::ResolvedIndexCurrentSnapshot {
                canonical: link.target.clone(),
                snapshot: target,
            });
        }
        Ok(resolved)
    }
}

async fn current_target(
    service: &ObjectServiceImpl,
    target: &ObjectKey,
) -> Result<Option<keldra_store::CurrentObjectSnapshot>, Status> {
    let (tenant_id, bucket_id) = service
        .name_resolver
        .resolve_bucket_ids(target.tenant(), target.bucket())
        .await?;
    current_target_with_ids(service, target, tenant_id, bucket_id).await
}

async fn current_target_with_ids(
    service: &ObjectServiceImpl,
    target: &ObjectKey,
    tenant_id: u64,
    bucket_id: u64,
) -> Result<Option<keldra_store::CurrentObjectSnapshot>, Status> {
    service
        .reader
        .current_head_snapshot_stable(target, tenant_id, bucket_id)
        .await
}

pub(super) async fn require_no_inbound_links(
    service: &ObjectServiceImpl,
    target: &ObjectKey,
) -> Result<(), Status> {
    if current_target(service, target)
        .await?
        .and_then(|current| current.alias_registry)
        .is_some_and(|registry| !registry.aliases.is_empty())
    {
        return Err(Status::failed_precondition(
            "object target cannot be deleted while inbound links exist",
        ));
    }
    Ok(())
}

pub(super) async fn require_public_version(
    service: &ObjectServiceImpl,
    key: &ObjectKey,
    version: keldra_store::VersionId,
) -> Result<(), Status> {
    let opened = service.reader.open(key, Some(version)).await?;
    require_public_version_metadata(opened.as_ref().map(|object| &object.version))
}

fn require_public_version_metadata(version: Option<&Version>) -> Result<(), Status> {
    if version.is_some_and(|version| version.protected_link_descriptor) {
        return Err(Status::not_found("requested version was not found"));
    }
    Ok(())
}

pub(super) async fn replay_unlink(
    service: &ObjectServiceImpl,
    link: &ObjectKey,
    command_id: &str,
    durability: keldra_store::Durability,
) -> Result<Option<(Response<MutationReceipt>, ObjectKey)>, Status> {
    let fingerprint = object_link_command_fingerprint(link, None, durability);
    let Some(result) = service
        .programs
        .replay_builtin_object_transaction(
            UNLINK_OBJECT_AUTHORITY_KIND,
            OBJECT_LINK_CONTRACT_VERSION,
            crate::programs::builtin_invocation_identity(UNLINK_OBJECT_AUTHORITY_KIND, command_id),
            fingerprint,
        )
        .await?
    else {
        return Ok(None);
    };
    let link_path = atomic_path(link)?;
    let target = result
        .alias_targets
        .get(&link_path)
        .ok_or_else(|| Status::data_loss("Unlink replay omitted its canonical target"))?;
    let target = ObjectKey::new(&target.tenant, &target.bucket, &target.path)
        .map_err(|error| Status::data_loss(error.to_string()))?;
    link_response(result, command_id, fingerprint, &link_path)
        .map(|response| Some((response, target)))
}

pub(super) enum DeleteReplayCheck {
    Response(Response<MutationReceipt>),
    Checked,
}

fn authenticated_executor_replay_checked(
    service: &ObjectServiceImpl,
    marker: Option<routed_writes::AtomicExecutorReplayChecked>,
) -> Result<bool, Status> {
    let Some(marker) = marker else {
        return Ok(false);
    };
    match service.programs.executor_routing_target()? {
        Some((executor, _)) if executor == marker.source_node => Ok(true),
        _ => Err(Status::permission_denied(
            "atomic replay marker was not issued by the active executor",
        )),
    }
}

pub(super) async fn route_or_replay_delete(
    service: &ObjectServiceImpl,
    caller: &crate::authentication::Caller,
    path_access: &object_path_access::ObjectPathAccess,
    plugin_scope: Option<&crate::authentication::PluginObjectScope>,
    peer_routed: bool,
    replay_marker: Option<routed_writes::AtomicExecutorReplayChecked>,
    bearer: &str,
    api_request: &ApiDeleteRequest,
    mutation: &keldra_store::DeleteRequest,
    budget: std::time::Duration,
) -> Result<DeleteReplayCheck, Status> {
    if authenticated_executor_replay_checked(service, replay_marker)? {
        return Ok(DeleteReplayCheck::Checked);
    }
    if !service.programs.generalized_atomic_paths_active()? {
        return Ok(DeleteReplayCheck::Checked);
    }
    match service.programs.executor_routing_target()? {
        Some(_) if peer_routed => {
            return Err(Status::failed_precondition(
                "a routed Delete reached a node that is not the atomic executor",
            ));
        }
        Some((target, address)) => {
            return service
                .cluster_peers
                .route_delete(target, &address, bearer, api_request.clone(), false, budget)
                .await
                .map(Response::new)
                .map(DeleteReplayCheck::Response);
        }
        None => {}
    }
    let command_id = mutation
        .command_id
        .as_deref()
        .ok_or_else(|| Status::invalid_argument("Delete command ID is required"))?;
    Ok(
        match replay_unlink(service, &mutation.key, command_id, mutation.durability).await? {
            Some((response, target)) => {
                object_path_access::require_key(path_access, &target)?;
                require_plugin_key_scope(plugin_scope, &target)?;
                service
                    .authorize_object(caller, &target, ObjectPermission::Delete)
                    .await?;
                DeleteReplayCheck::Response(response)
            }
            None => DeleteReplayCheck::Checked,
        },
    )
}

async fn open_visible(
    service: &ObjectServiceImpl,
    key: &ObjectKey,
) -> Result<Option<ClusterOpenedObject>, Status> {
    loop {
        let opened = service.reader.open(key, None).await?;
        let Some(cursor) = opened
            .as_ref()
            .and_then(|object| object.program_commit_cursor)
        else {
            return Ok(opened);
        };
        if service.programs.cursor_is_visible(cursor)? {
            return Ok(opened);
        }
        service
            .programs
            .wait_for_cursor(cursor, service.atomic_program_timeout)
            .await?;
    }
}

pub(super) async fn revalidate(
    service: &ObjectServiceImpl,
    resolution: &ResolvedAddress,
) -> Result<bool, Status> {
    let ResolvedAddress::Link(expected) = resolution else {
        return Ok(true);
    };
    let current = resolve_current(service, expected.link.clone()).await?;
    Ok(matches!(
        current,
        ResolvedAddress::Link(actual) if actual == *expected
    ))
}

pub(super) async fn link_object(
    service: &ObjectServiceImpl,
    request: Request<LinkObjectRequest>,
) -> Result<Response<MutationReceipt>, Status> {
    let deadline = request_deadline(request.metadata(), service.atomic_program_timeout)?;
    let peer_routed = request
        .extensions()
        .get::<routed_writes::RoutedDestination>()
        .is_some();
    let caller = authenticated_caller(&request)?;
    let path_access = object_path_access::access_for(&request);
    let plugin_scope = plugin_object_scope(&request);
    let bearer = OriginalBearer::from_metadata(request.metadata())?;
    let api_request = request.into_inner();
    let link = object_key(api_request.link.clone())?;
    let supplied_target = object_key(api_request.target.clone())?;
    if link.tenant() != supplied_target.tenant()
        || link.bucket() != supplied_target.bucket()
        || link == supplied_target
    {
        return Err(Status::invalid_argument(
            "link and target must be distinct paths in the same tenant and bucket",
        ));
    }
    ObjectLinkDescriptor::new(link.path())
        .map_err(|error| Status::invalid_argument(format!("invalid link path: {error}")))?;
    object_path_access::require_key(&path_access, &link)?;
    object_path_access::require_key(&path_access, &supplied_target)?;
    require_plugin_key_scope(plugin_scope.as_ref(), &link)?;
    require_plugin_key_scope(plugin_scope.as_ref(), &supplied_target)?;
    service
        .authorize_object(&caller, &link, ObjectPermission::Put)
        .await?;
    let command_id = required_command_id(api_request.command_id.clone())?;
    let durability = durability(api_request.durability)?;
    let fingerprint = object_link_command_fingerprint(&link, Some(&supplied_target), durability);
    service
        .distribution
        .require_durability_available(durability)?;
    let governance = service
        .bucket_governance
        .resolve(link.tenant(), link.bucket())
        .await?;
    match service.programs.executor_routing_target()? {
        Some(_) if peer_routed => {
            return Err(Status::failed_precondition(
                "a routed LinkObject reached a node that is not the atomic executor",
            ));
        }
        Some((target, address)) => {
            return service
                .cluster_peers
                .route_link_object(
                    target,
                    &address,
                    bearer.signed_token(),
                    api_request,
                    deadline_remaining(deadline)?,
                )
                .await
                .map(Response::new);
        }
        None => {}
    }
    let link_path = atomic_path(&link)?;
    if let Some(result) = service
        .programs
        .replay_builtin_object_transaction(
            LINK_OBJECT_AUTHORITY_KIND,
            OBJECT_LINK_CONTRACT_VERSION,
            crate::programs::builtin_invocation_identity(LINK_OBJECT_AUTHORITY_KIND, &command_id),
            fingerprint,
        )
        .await?
    {
        let target = result
            .alias_targets
            .get(&link_path)
            .ok_or_else(|| Status::data_loss("Link replay omitted its canonical target"))?;
        let target = ObjectKey::new(&target.tenant, &target.bucket, &target.path)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        object_path_access::require_key(&path_access, &target)?;
        require_plugin_key_scope(plugin_scope.as_ref(), &target)?;
        service
            .authorize_object(&caller, &target, ObjectPermission::Get)
            .await?;
        return link_response(result, &command_id, fingerprint, &link_path);
    }
    let target_resolution = resolve_current(service, supplied_target).await?;
    let canonical_target = target_resolution.canonical();
    if canonical_target == &link {
        return Err(Status::invalid_argument("object-link cycle is not allowed"));
    }
    ObjectLinkDescriptor::new(canonical_target.path())
        .map_err(|error| Status::invalid_argument(format!("invalid link target: {error}")))?;
    object_path_access::require_key(&path_access, canonical_target)?;
    require_plugin_key_scope(plugin_scope.as_ref(), canonical_target)?;
    service
        .authorize_object(&caller, canonical_target, ObjectPermission::Get)
        .await?;
    let Some(target) = open_visible(service, canonical_target).await? else {
        return Err(Status::failed_precondition(
            "object-link target must be a present object",
        ));
    };
    if target.version.deleted {
        return Err(Status::failed_precondition(
            "object-link target must be a present object",
        ));
    }
    let _placement_guard = service.distribution.enter_mutation()?;
    let link_current = service
        .reader
        .current_head_snapshot_stable(&link, governance.tenant_id, governance.bucket_id)
        .await?;
    if link_current.is_some() {
        return Err(Status::failed_precondition(
            "object-link path must never have existed",
        ));
    }
    let target_current = service
        .reader
        .current_head_snapshot_stable(canonical_target, governance.tenant_id, governance.bucket_id)
        .await?
        .filter(|current| !current.version.deleted)
        .ok_or_else(|| Status::failed_precondition("object-link target must be present"))?;
    if target_current.version.protected_link_descriptor {
        return Err(Status::failed_precondition(
            "object-link target must be an ordinary object",
        ));
    }
    let prior_registry = target_current.alias_registry.clone();
    let replacement_aliases = aliases_with_inserted(
        prior_registry.as_ref(),
        canonical_target.path(),
        link.path(),
    )?;
    let descriptor = ObjectLinkDescriptor::new(canonical_target.path())
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let target_path = atomic_path(canonical_target)?;
    let target_participant_index = u32::from(target_path > link_path);
    let plan = link_plan(
        LINK_OBJECT_AUTHORITY_KIND,
        &command_id,
        fingerprint,
        &governance,
        link.tenant(),
        link.bucket(),
        vec![
            exact_read_participant(canonical_target, target_current.version, &governance)?,
            participant(
                &link_path,
                ObservedHead::NeverExisted,
                true,
                false,
                &governance,
            ),
        ],
        vec![BuiltInVersionWrite {
            path: link_path.clone(),
            expected: ObservedHead::NeverExisted,
            previous_version: None,
            payload: BuiltInWritePayload::Inline {
                bytes: descriptor.encode(),
                content_type: OBJECT_LINK_CONTENT_TYPE.to_owned(),
            },
        }],
        vec![BuiltInAliasRegistryAccess::Write {
            target_participant_index,
            expected: prior_registry,
            replacement_aliases,
        }],
        vec![(link_path.clone(), target_path.clone(), false)],
        Vec::new(),
    )?;
    invoke_link_plan(
        service,
        plan,
        LINK_OBJECT_AUTHORITY_KIND,
        &command_id,
        fingerprint,
        durability,
        deadline_remaining(deadline)?,
        &link_path,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn route_or_replay_conditional_delete(
    service: &ObjectServiceImpl,
    caller: &crate::authentication::Caller,
    path_access: &object_path_access::ObjectPathAccess,
    plugin_scope: Option<&crate::authentication::PluginObjectScope>,
    peer_routed: bool,
    replay_marker: Option<routed_writes::AtomicExecutorReplayChecked>,
    internal: bool,
    bearer: &str,
    api_request: &DeleteIfVersionRequest,
    mutation: &keldra_store::DeleteRequest,
    budget: std::time::Duration,
) -> Result<DeleteReplayCheck, Status> {
    if authenticated_executor_replay_checked(service, replay_marker)? {
        return Ok(DeleteReplayCheck::Checked);
    }
    if !service.programs.generalized_atomic_paths_active()? {
        return Ok(DeleteReplayCheck::Checked);
    }
    match service.programs.executor_routing_target()? {
        Some(_) if peer_routed => {
            return Err(Status::failed_precondition(
                "a routed DeleteIfVersion reached a node that is not the atomic executor",
            ));
        }
        Some((target, address)) => {
            let receipt = if internal {
                service
                    .cluster_peers
                    .route_internal_delete_if_version(
                        target,
                        &address,
                        bearer,
                        api_request.clone(),
                        false,
                        budget,
                    )
                    .await?
            } else {
                service
                    .cluster_peers
                    .route_delete_if_version(
                        target,
                        &address,
                        bearer,
                        api_request.clone(),
                        false,
                        budget,
                    )
                    .await?
            };
            return Ok(DeleteReplayCheck::Response(Response::new(receipt)));
        }
        None => {}
    }
    let command_id = mutation
        .command_id
        .as_deref()
        .ok_or_else(|| Status::invalid_argument("DeleteIfVersion command ID is required"))?;
    let Precondition::Version(expected) = mutation.precondition else {
        return Err(Status::internal(
            "conditional delete omitted its exact version",
        ));
    };
    let fingerprint = conditional_unlink_fingerprint(&mutation.key, expected, mutation.durability);
    let Some(result) = service
        .programs
        .replay_builtin_object_transaction(
            UNLINK_OBJECT_AUTHORITY_KIND,
            OBJECT_LINK_CONTRACT_VERSION,
            crate::programs::builtin_invocation_identity(UNLINK_OBJECT_AUTHORITY_KIND, command_id),
            fingerprint,
        )
        .await?
    else {
        return Ok(DeleteReplayCheck::Checked);
    };
    let link_path = atomic_path(&mutation.key)?;
    let target = result
        .alias_targets
        .get(&link_path)
        .ok_or_else(|| Status::data_loss("conditional Unlink replay omitted its target"))?;
    let target = ObjectKey::new(&target.tenant, &target.bucket, &target.path)
        .map_err(|error| Status::data_loss(error.to_string()))?;
    object_path_access::require_key(path_access, &target)?;
    require_plugin_key_scope(plugin_scope, &target)?;
    service
        .authorize_object(caller, &target, ObjectPermission::Delete)
        .await?;
    link_response(result, command_id, fingerprint, &link_path).map(DeleteReplayCheck::Response)
}

pub(super) async fn unlink_object(
    service: &ObjectServiceImpl,
    request: Request<UnlinkObjectRequest>,
) -> Result<Response<MutationReceipt>, Status> {
    let deadline = request_deadline(request.metadata(), service.atomic_program_timeout)?;
    let peer_routed = request
        .extensions()
        .get::<routed_writes::RoutedDestination>()
        .is_some();
    let caller = authenticated_caller(&request)?;
    let path_access = object_path_access::access_for(&request);
    let plugin_scope = plugin_object_scope(&request);
    let bearer = OriginalBearer::from_metadata(request.metadata())?;
    let api_request = request.into_inner();
    let link = object_key(api_request.link.clone())?;
    object_path_access::require_key(&path_access, &link)?;
    require_plugin_key_scope(plugin_scope.as_ref(), &link)?;
    let command_id = required_command_id(api_request.command_id.clone())?;
    let durability = durability(api_request.durability)?;
    let fingerprint = object_link_command_fingerprint(&link, None, durability);
    service
        .distribution
        .require_durability_available(durability)?;
    let governance = service
        .bucket_governance
        .resolve(link.tenant(), link.bucket())
        .await?;
    match service.programs.executor_routing_target()? {
        Some(_) if peer_routed => {
            return Err(Status::failed_precondition(
                "a routed UnlinkObject reached a node that is not the atomic executor",
            ));
        }
        Some((target, address)) => {
            return service
                .cluster_peers
                .route_unlink_object(
                    target,
                    &address,
                    bearer.signed_token(),
                    api_request,
                    deadline_remaining(deadline)?,
                )
                .await
                .map(Response::new);
        }
        None => {}
    }
    if let Some((response, target)) = replay_unlink(service, &link, &command_id, durability).await?
    {
        object_path_access::require_key(&path_access, &target)?;
        require_plugin_key_scope(plugin_scope.as_ref(), &target)?;
        service
            .authorize_object(&caller, &target, ObjectPermission::Delete)
            .await?;
        return Ok(response);
    }
    let resolution = match resolve_current(service, link.clone()).await? {
        ResolvedAddress::Link(link) => link,
        ResolvedAddress::Ordinary(_) => {
            return Err(Status::failed_precondition(
                "UnlinkObject requires a current object-link descriptor",
            ));
        }
    };
    object_path_access::require_key(&path_access, &resolution.target)?;
    require_plugin_key_scope(plugin_scope.as_ref(), &resolution.target)?;
    service
        .authorize_object(&caller, &resolution.target, ObjectPermission::Delete)
        .await?;
    unlink_resolved(
        service,
        &link,
        resolution,
        None,
        &command_id,
        fingerprint,
        durability,
        governance,
        deadline_remaining(deadline)?,
    )
    .await
}

pub(super) async fn delete_if_version_link(
    service: &ObjectServiceImpl,
    request: Request<DeleteIfVersionRequest>,
) -> Result<Response<MutationReceipt>, Status> {
    let deadline = request_deadline(request.metadata(), service.atomic_program_timeout)?;
    let peer_routed = request
        .extensions()
        .get::<routed_writes::RoutedDestination>()
        .is_some();
    let caller = authenticated_caller(&request)?;
    let path_access = object_path_access::access_for(&request);
    let plugin_scope = plugin_object_scope(&request);
    let bearer = OriginalBearer::from_metadata(request.metadata())?;
    let api_request = request.into_inner();
    let link = object_key(api_request.address.clone())?;
    object_path_access::require_key(&path_access, &link)?;
    require_plugin_key_scope(plugin_scope.as_ref(), &link)?;
    let command_id = required_command_id(api_request.command_id.clone())?;
    let durability = durability(api_request.durability)?;
    let expected_target_version = keldra_store::VersionId(api_request.expected_version);
    if expected_target_version.0 == 0 {
        return Err(Status::invalid_argument(
            "DeleteIfVersion expected version must be nonzero",
        ));
    }
    let fingerprint = conditional_unlink_fingerprint(&link, expected_target_version, durability);
    service
        .distribution
        .require_durability_available(durability)?;
    let governance = service
        .bucket_governance
        .resolve(link.tenant(), link.bucket())
        .await?;
    match service.programs.executor_routing_target()? {
        Some(_) if peer_routed => {
            return Err(Status::failed_precondition(
                "a routed DeleteIfVersion reached a node that is not the atomic executor",
            ));
        }
        Some((target, address)) => {
            return service
                .cluster_peers
                .route_delete_if_version(
                    target,
                    &address,
                    bearer.signed_token(),
                    api_request,
                    false,
                    deadline_remaining(deadline)?,
                )
                .await
                .map(Response::new);
        }
        None => {}
    }
    let link_path = atomic_path(&link)?;
    if let Some(result) = service
        .programs
        .replay_builtin_object_transaction(
            UNLINK_OBJECT_AUTHORITY_KIND,
            OBJECT_LINK_CONTRACT_VERSION,
            crate::programs::builtin_invocation_identity(UNLINK_OBJECT_AUTHORITY_KIND, &command_id),
            fingerprint,
        )
        .await?
    {
        let target = result
            .alias_targets
            .get(&link_path)
            .ok_or_else(|| Status::data_loss("conditional Unlink replay omitted its target"))?;
        let target = ObjectKey::new(&target.tenant, &target.bucket, &target.path)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        object_path_access::require_key(&path_access, &target)?;
        require_plugin_key_scope(plugin_scope.as_ref(), &target)?;
        service
            .authorize_object(&caller, &target, ObjectPermission::Delete)
            .await?;
        return link_response(result, &command_id, fingerprint, &link_path);
    }
    let resolution = match resolve_current(service, link.clone()).await? {
        ResolvedAddress::Link(link) => link,
        ResolvedAddress::Ordinary(_) => {
            return Err(Status::failed_precondition(
                "DeleteIfVersion requires a current object link",
            ));
        }
    };
    object_path_access::require_key(&path_access, &resolution.target)?;
    require_plugin_key_scope(plugin_scope.as_ref(), &resolution.target)?;
    service
        .authorize_object(&caller, &resolution.target, ObjectPermission::Delete)
        .await?;
    unlink_resolved(
        service,
        &link,
        resolution,
        Some(expected_target_version),
        &command_id,
        fingerprint,
        durability,
        governance,
        deadline_remaining(deadline)?,
    )
    .await
}

pub(super) fn conditional_unlink_fingerprint(
    link: &ObjectKey,
    expected_target_version: keldra_store::VersionId,
    durability: keldra_store::Durability,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key("keldra.conditional-object-unlink/v1");
    for component in [link.tenant(), link.bucket(), link.path()] {
        hasher.update(&(component.len() as u64).to_be_bytes());
        hasher.update(component.as_bytes());
    }
    hasher.update(&expected_target_version.0.to_be_bytes());
    hasher.update(&[match durability {
        keldra_store::Durability::Local => 0,
        keldra_store::Durability::Replicated => 1,
    }]);
    *hasher.finalize().as_bytes()
}

#[allow(clippy::too_many_arguments)]
async fn unlink_resolved(
    service: &ObjectServiceImpl,
    link: &ObjectKey,
    resolution: ResolvedObjectLink,
    expected_target_version: Option<keldra_store::VersionId>,
    command_id: &str,
    fingerprint: [u8; 32],
    durability: keldra_store::Durability,
    governance: ObjectMutationGovernance,
    budget: std::time::Duration,
) -> Result<Response<MutationReceipt>, Status> {
    let _placement_guard = service.distribution.enter_mutation()?;
    let link_current = service
        .reader
        .current_head_snapshot_stable(&link, governance.tenant_id, governance.bucket_id)
        .await?
        .filter(|current| {
            current.head.version == resolution.descriptor_version && !current.version.deleted
        })
        .ok_or_else(|| Status::failed_precondition("object-link descriptor changed"))?;
    let target_current = service
        .reader
        .current_head_snapshot_stable(
            &resolution.target,
            governance.tenant_id,
            governance.bucket_id,
        )
        .await?
        .filter(|current| !current.version.deleted)
        .ok_or_else(|| Status::data_loss("object-link target is absent"))?;
    if expected_target_version.is_some_and(|expected| expected != target_current.version.id) {
        return Err(Status::failed_precondition(
            "DeleteIfVersion object-link target version changed",
        ));
    }
    let prior_registry = target_current
        .alias_registry
        .clone()
        .ok_or_else(|| Status::data_loss("object-link target sidecar is absent"))?;
    let replacement_aliases =
        aliases_with_removed(&prior_registry, resolution.target.path(), link.path())?;
    let link_path = atomic_path(link)?;
    let target_path = atomic_path(&resolution.target)?;
    let link_expected = observed(Some(&link_current.version));
    let target_participant_index = u32::from(target_path > link_path);
    let plan = link_plan(
        UNLINK_OBJECT_AUTHORITY_KIND,
        &command_id,
        fingerprint,
        &governance,
        link.tenant(),
        link.bucket(),
        vec![
            exact_read_participant(&resolution.target, target_current.version, &governance)?,
            participant(&link_path, link_expected.clone(), true, true, &governance),
        ],
        vec![BuiltInVersionWrite {
            path: link_path.clone(),
            expected: link_expected,
            previous_version: Some(link_current.version.clone()),
            payload: BuiltInWritePayload::Tombstone,
        }],
        vec![BuiltInAliasRegistryAccess::Write {
            target_participant_index,
            expected: Some(prior_registry),
            replacement_aliases,
        }],
        vec![(link_path.clone(), target_path.clone(), true)],
        vec![(
            link_path.clone(),
            link_current.version.clone(),
            ObjectLinkDescriptor::new(resolution.target.path())
                .map_err(|error| Status::data_loss(error.to_string()))?
                .encode(),
        )],
    )?;
    invoke_link_plan(
        service,
        plan,
        UNLINK_OBJECT_AUTHORITY_KIND,
        &command_id,
        fingerprint,
        durability,
        budget,
        &link_path,
    )
    .await
}

pub(super) async fn bulk_delete_through_link(
    service: &ObjectServiceImpl,
    caller: &crate::authentication::Caller,
    expected_link: ResolvedObjectLink,
    precondition: Precondition,
    command_id: &str,
    durability: keldra_store::Durability,
    bearer: &str,
    deadline: tokio::time::Instant,
) -> Result<MutationReceipt, Status> {
    let link = expected_link.link.clone();
    let expected = match precondition {
        Precondition::Any => None,
        Precondition::Version(version) if version.0 != 0 => Some(version),
        Precondition::Version(_) => {
            return Err(Status::invalid_argument(
                "bulk DeleteIfVersion expected version must be nonzero",
            ));
        }
        Precondition::Absent => {
            return Err(Status::internal(
                "bulk linked Delete unexpectedly used an absent precondition",
            ));
        }
    };
    let fingerprint = expected.map_or_else(
        || object_link_command_fingerprint(&link, None, durability),
        |version| conditional_unlink_fingerprint(&link, version, durability),
    );
    match service.programs.executor_routing_target()? {
        Some((target, address)) => {
            let response = if let Some(expected_version) = expected {
                service
                    .cluster_peers
                    .route_delete_if_version(
                        target,
                        &address,
                        bearer,
                        DeleteIfVersionRequest {
                            address: Some(super::api_address(&link)),
                            expected_version: expected_version.0,
                            command_id: command_id.to_owned(),
                            durability: api_durability(durability),
                        },
                        false,
                        deadline_remaining(deadline)?,
                    )
                    .await?
            } else {
                service
                    .cluster_peers
                    .route_unlink_object(
                        target,
                        &address,
                        bearer,
                        UnlinkObjectRequest {
                            link: Some(super::api_address(&link)),
                            command_id: command_id.to_owned(),
                            durability: api_durability(durability),
                        },
                        deadline_remaining(deadline)?,
                    )
                    .await?
            };
            return Ok(response);
        }
        None => {}
    }
    let link_path = atomic_path(&link)?;
    if let Some(result) = service
        .programs
        .replay_builtin_object_transaction(
            UNLINK_OBJECT_AUTHORITY_KIND,
            OBJECT_LINK_CONTRACT_VERSION,
            crate::programs::builtin_invocation_identity(UNLINK_OBJECT_AUTHORITY_KIND, command_id),
            fingerprint,
        )
        .await?
    {
        let target = result
            .alias_targets
            .get(&link_path)
            .ok_or_else(|| Status::data_loss("bulk Unlink replay omitted its target"))?;
        let target = ObjectKey::new(&target.tenant, &target.bucket, &target.path)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        service
            .authorize_object(caller, &target, ObjectPermission::Delete)
            .await?;
        return link_response(result, command_id, fingerprint, &link_path)
            .map(|response| response.into_inner());
    }
    if !revalidate(service, &ResolvedAddress::Link(expected_link.clone())).await? {
        return Err(Status::failed_precondition(
            "bulk delete alias is no longer the authorized object link",
        ));
    }
    service
        .authorize_object(caller, &expected_link.target, ObjectPermission::Delete)
        .await?;
    let governance = service
        .bucket_governance
        .resolve(link.tenant(), link.bucket())
        .await?;
    unlink_resolved(
        service,
        &link,
        expected_link,
        expected,
        command_id,
        fingerprint,
        durability,
        governance,
        deadline_remaining(deadline)?,
    )
    .await
    .map(|response| response.into_inner())
}

fn api_durability(durability: keldra_store::Durability) -> i32 {
    match durability {
        keldra_store::Durability::Local => keldra_api::v1::Durability::Local as i32,
        keldra_store::Durability::Replicated => keldra_api::v1::Durability::Replicated as i32,
    }
}

fn atomic_path(key: &ObjectKey) -> Result<ObjectPath, Status> {
    ObjectPath::new(key.tenant(), key.bucket(), key.path()).map_err(Status::invalid_argument)
}

fn observed(version: Option<&Version>) -> ObservedHead {
    version.map_or(ObservedHead::NeverExisted, |version| {
        ObservedHead::Version {
            version: version.id.0.to_string(),
        }
    })
}

fn participant(
    path: &ObjectPath,
    expected: ObservedHead,
    put: bool,
    delete: bool,
    governance: &ObjectMutationGovernance,
) -> ProgramObjectParticipant {
    ProgramObjectParticipant {
        tenant_id: governance.tenant_id,
        bucket_id: governance.bucket_id,
        path: path.clone(),
        condition: ProgramPathCondition::Head(expected),
        intent: ProgramParticipantIntent {
            read: true,
            put,
            delete,
        },
        alias_registry: None,
    }
}

fn link_plan(
    authority_kind: u16,
    command_id: &str,
    fingerprint: [u8; 32],
    governance: &ObjectMutationGovernance,
    tenant: &str,
    bucket: &str,
    mut objects: Vec<ProgramObjectParticipant>,
    mut writes: Vec<BuiltInVersionWrite>,
    alias_registries: Vec<BuiltInAliasRegistryAccess>,
    alias_paths: Vec<(ObjectPath, ObjectPath, bool)>,
    proof_payloads: Vec<(ObjectPath, Version, Vec<u8>)>,
) -> Result<BuiltInObjectTransactionPlan, Status> {
    objects.sort_by(|left, right| left.path.cmp(&right.path));
    for access in &alias_registries {
        let (index, expected) = match access {
            BuiltInAliasRegistryAccess::Read {
                target_participant_index,
                expected,
            }
            | BuiltInAliasRegistryAccess::Write {
                target_participant_index,
                expected,
                ..
            } => (*target_participant_index as usize, expected),
        };
        let target = objects
            .get_mut(index)
            .ok_or_else(|| Status::internal("object-link sidecar target is absent"))?;
        target.alias_registry = Some(match expected {
            Some(registry) => ProgramAliasRegistryCondition::Exact(registry.clone()),
            None => ProgramAliasRegistryCondition::Absent,
        });
    }
    writes.sort_by(|left, right| left.path.cmp(&right.path));
    let mut read_proofs = proof_payloads
        .into_iter()
        .map(|(path, expected, bytes)| {
            objects
                .iter()
                .position(|participant| participant.path == path)
                .and_then(|index| u32::try_from(index).ok())
                .map(|participant_index| BuiltInReadProof {
                    participant_index,
                    expected,
                    bytes,
                })
                .ok_or_else(|| Status::internal("object-link read proof has no participant"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    read_proofs.sort_by_key(|proof| proof.participant_index);
    let mut alias_observations = alias_paths
        .into_iter()
        .map(|(requested, canonical, deleted)| {
            objects
                .iter()
                .position(|participant| participant.path == canonical)
                .and_then(|index| u32::try_from(index).ok())
                .map(|canonical_participant_index| BuiltInAliasObservation {
                    requested_path: requested,
                    canonical_participant_index,
                    deleted,
                })
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| Status::internal("object-link alias observation has no participant"))?;
    alias_observations.sort_by(|left, right| left.requested_path.cmp(&right.requested_path));
    let head_preconditions = objects
        .iter()
        .map(|participant| match &participant.condition {
            ProgramPathCondition::Head(expected) => Ok(HeadPrecondition {
                path: participant.path.clone(),
                expected: expected.clone(),
            }),
            ProgramPathCondition::RetainedVersion { .. }
            | ProgramPathCondition::HeadAndRetainedVersion { .. } => Err(Status::internal(
                "object-link plan unexpectedly contains a retained-version participant",
            )),
            ProgramPathCondition::HeadVersion { expected } => Ok(HeadPrecondition {
                path: participant.path.clone(),
                expected: observed(Some(expected)),
            }),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BuiltInObjectTransactionPlan {
        authority_kind,
        contract_version: OBJECT_LINK_CONTRACT_VERSION,
        participant_manifest: ProgramParticipantManifest {
            format: PROGRAM_PARTICIPANT_MANIFEST_FORMAT,
            objects,
            governance: vec![ProgramGovernanceParticipant {
                tenant: tenant.to_owned(),
                bucket: bucket.to_owned(),
                tenant_id: governance.tenant_id,
                bucket_id: governance.bucket_id,
                policy: governance.policy.clone(),
                versioning: governance.versioning,
            }],
        },
        head_preconditions,
        read_proofs,
        assertions: Vec::new(),
        alias_registries,
        alias_observations,
        writes,
        receipt: CommandReceipt {
            program_path_hash: [0; 32],
            command_id: command_id.to_owned(),
            input_fingerprint: hex::encode(fingerprint),
            outputs: BTreeMap::new(),
        },
    })
}

async fn invoke_link_plan(
    service: &ObjectServiceImpl,
    plan: BuiltInObjectTransactionPlan,
    authority_kind: u16,
    command_id: &str,
    fingerprint: [u8; 32],
    durability: keldra_store::Durability,
    budget: std::time::Duration,
    receipt_path: &ObjectPath,
) -> Result<Response<MutationReceipt>, Status> {
    let durability_class = match durability {
        keldra_store::Durability::Local => "local",
        keldra_store::Durability::Replicated => "replicated",
    };
    let result = service
        .programs
        .invoke_builtin_object_transaction(
            plan,
            crate::programs::builtin_invocation_identity(authority_kind, command_id),
            fingerprint,
            durability_class,
            budget,
        )
        .await?;
    link_response(result, command_id, fingerprint, receipt_path)
}

fn link_response(
    result: crate::programs::InvokedProgramResult,
    command_id: &str,
    fingerprint: [u8; 32],
    receipt_path: &ObjectPath,
) -> Result<Response<MutationReceipt>, Status> {
    let published = result.published_versions.get(receipt_path).ok_or_else(|| {
        Status::data_loss("object-link transaction omitted its descriptor result")
    })?;
    Ok(Response::new(api_receipt(keldra_store::MutationReceipt {
        command_id: Some(command_id.to_owned()),
        fingerprint,
        version: published.version,
        deleted: published.deleted,
        replayed: result.replayed,
        replay_guarantee_expires_at_unix_millis: result.replay_guarantee_expires_at_unix_millis,
    })))
}

fn aliases_with_inserted(
    expected: Option<&ObjectAliasRegistry>,
    target: &str,
    link: &str,
) -> Result<Vec<String>, Status> {
    if let Some(expected) = expected {
        expected
            .validate(target)
            .map_err(|error| Status::data_loss(error.to_string()))?;
    }
    let mut aliases = expected.map_or_else(Vec::new, |registry| registry.aliases.clone());
    if aliases.len() >= keldra_store::MAX_INBOUND_OBJECT_LINKS {
        return Err(Status::resource_exhausted(
            "object target has the maximum number of inbound links",
        ));
    }
    let index = match aliases.binary_search_by(|path| path.as_str().cmp(link)) {
        Ok(_) => return Err(Status::failed_precondition("object link already exists")),
        Err(index) => index,
    };
    aliases.insert(index, link.to_owned());
    Ok(aliases)
}

fn aliases_with_removed(
    expected: &ObjectAliasRegistry,
    target: &str,
    link: &str,
) -> Result<Vec<String>, Status> {
    expected
        .validate(target)
        .map_err(|error| Status::data_loss(error.to_string()))?;
    let mut aliases = expected.aliases.clone();
    let index = aliases
        .binary_search_by(|path| path.as_str().cmp(link))
        .map_err(|_| Status::failed_precondition("object link is absent from target sidecar"))?;
    aliases.remove(index);
    Ok(aliases)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn publish_through_link(
    service: &ObjectServiceImpl,
    publish: PublishRequest,
    link: ResolvedObjectLink,
    upload_source_node_id: u64,
    bearer: &str,
    token: PutToken,
    peer_routed: bool,
    deadline: tokio::time::Instant,
) -> Result<MutationReceipt, Status> {
    match service.programs.executor_routing_target()? {
        Some(_) if peer_routed => {
            return Err(Status::failed_precondition(
                "a routed PutEnd through a link reached a node that is not the atomic executor",
            ));
        }
        Some((target, address)) => {
            return service
                .cluster_peers
                .route_put_end(
                    target,
                    &address,
                    bearer,
                    token,
                    deadline_remaining(deadline)?,
                )
                .await;
        }
        None => {}
    }
    if publish.key != link.target || upload_source_node_id == 0 {
        return Err(Status::invalid_argument(
            "linked Put token has an invalid canonical target or upload source",
        ));
    }
    let command_id = publish
        .command_id
        .as_deref()
        .ok_or_else(|| Status::invalid_argument("linked Put command ID is required"))?;
    let fingerprint = linked_put_fingerprint(&link.link, &publish);
    let target_path = atomic_path(&publish.key)?;
    let authority_kind = if publish.mode == PutMode::PutImmutable {
        PUT_IMMUTABLE_THROUGH_LINK_AUTHORITY_KIND
    } else {
        PUT_THROUGH_LINK_AUTHORITY_KIND
    };
    if let Some(result) = service
        .programs
        .replay_builtin_object_transaction(
            authority_kind,
            OBJECT_LINK_CONTRACT_VERSION,
            crate::programs::builtin_invocation_identity(authority_kind, command_id),
            fingerprint,
        )
        .await?
    {
        return if authority_kind == PUT_IMMUTABLE_THROUGH_LINK_AUTHORITY_KIND {
            linked_immutable_response(result, command_id, fingerprint, &target_path)
        } else {
            linked_put_response(result, command_id, fingerprint, &target_path)
        };
    }
    let governance = service
        .bucket_governance
        .resolve(publish.key.tenant(), publish.key.bucket())
        .await?;
    let _placement_guard = service.distribution.enter_mutation()?;
    let descriptor_current = service
        .reader
        .current_head_snapshot_stable(&link.link, governance.tenant_id, governance.bucket_id)
        .await?
        .filter(|current| {
            current.head.version == link.descriptor_version && !current.version.deleted
        })
        .ok_or_else(|| Status::failed_precondition("object link changed during upload"))?;
    let target_current = service
        .reader
        .current_head_snapshot_stable(&publish.key, governance.tenant_id, governance.bucket_id)
        .await?
        .filter(|current| !current.version.deleted)
        .ok_or_else(|| Status::data_loss("object-link target is absent"))?;
    let expected = match publish.mode {
        PutMode::Put => observed(Some(&target_current.version)),
        PutMode::PutIfAbsent => {
            return Err(Status::failed_precondition(
                "linked PutIfAbsent target already exists",
            ));
        }
        PutMode::PutIfVersion(expected) if expected == target_current.head.version => {
            ObservedHead::Version {
                version: expected.0.to_string(),
            }
        }
        PutMode::PutIfVersion(_) => {
            return Err(Status::failed_precondition(
                "linked PutIfVersion target version changed",
            ));
        }
        PutMode::PutImmutable => {
            if target_current.version.blob.as_ref() != Some(&publish.blob)
                || target_current.version.content_type != publish.content_type
            {
                return Err(Status::failed_precondition(
                    "linked PutImmutable target already has different content",
                ));
            }
            return publish_immutable_through_link(
                service,
                &publish,
                &link,
                upload_source_node_id,
                command_id,
                fingerprint,
                governance,
                descriptor_current.version,
                target_current.version,
                target_current.alias_registry,
                deadline_remaining(deadline)?,
            )
            .await;
        }
    };
    let link_path = atomic_path(&link.link)?;
    let registry = target_current
        .alias_registry
        .clone()
        .ok_or_else(|| Status::data_loss("object-link target sidecar is absent"))?;
    if registry
        .aliases
        .binary_search_by(|path| path.as_str().cmp(link.link.path()))
        .is_err()
    {
        return Err(Status::failed_precondition(
            "object link changed during publication",
        ));
    }
    let target_participant_index = u32::from(target_path > link_path);
    let writes = vec![BuiltInVersionWrite {
        path: target_path.clone(),
        expected: expected.clone(),
        previous_version: Some(target_current.version.clone()),
        payload: BuiltInWritePayload::StagedReference {
            blob_hash: publish.blob.hash,
            blob_length: publish.blob.length,
            content_type: publish.content_type.clone(),
            upload_source_node_id,
        },
    }];
    let plan = link_plan(
        PUT_THROUGH_LINK_AUTHORITY_KIND,
        command_id,
        fingerprint,
        &governance,
        publish.key.tenant(),
        publish.key.bucket(),
        vec![
            exact_read_participant(&link.link, descriptor_current.version.clone(), &governance)?,
            participant(&target_path, expected, true, false, &governance),
        ],
        writes,
        vec![BuiltInAliasRegistryAccess::Read {
            target_participant_index,
            expected: Some(registry.clone()),
        }],
        registry
            .aliases
            .iter()
            .map(|path| {
                ObjectPath::new(link.link.tenant(), link.link.bucket(), path)
                    .map(|requested| (requested, target_path.clone(), false))
                    .map_err(Status::data_loss)
            })
            .collect::<Result<Vec<_>, _>>()?,
        vec![(
            link_path,
            descriptor_current.version,
            ObjectLinkDescriptor::new(link.target.path())
                .map_err(|error| Status::data_loss(error.to_string()))?
                .encode(),
        )],
    )?;
    let durability_class = match publish.durability {
        keldra_store::Durability::Local => "local",
        keldra_store::Durability::Replicated => "replicated",
    };
    let result = service
        .programs
        .invoke_builtin_object_transaction(
            plan,
            crate::programs::builtin_invocation_identity(
                PUT_THROUGH_LINK_AUTHORITY_KIND,
                command_id,
            ),
            fingerprint,
            durability_class,
            deadline_remaining(deadline)?,
        )
        .await?;
    linked_put_response(result, command_id, fingerprint, &target_path)
}

#[allow(clippy::too_many_arguments)]
async fn publish_immutable_through_link(
    service: &ObjectServiceImpl,
    publish: &PublishRequest,
    link: &ResolvedObjectLink,
    upload_source_node_id: u64,
    command_id: &str,
    fingerprint: [u8; 32],
    governance: ObjectMutationGovernance,
    descriptor_version: Version,
    target_version: Version,
    registry: Option<ObjectAliasRegistry>,
    budget: std::time::Duration,
) -> Result<MutationReceipt, Status> {
    let registry =
        registry.ok_or_else(|| Status::data_loss("object-link target sidecar is absent"))?;
    if registry
        .aliases
        .binary_search_by(|path| path.as_str().cmp(link.link.path()))
        .is_err()
    {
        return Err(Status::failed_precondition(
            "object link changed during immutable publication",
        ));
    }
    let mut objects = vec![
        exact_read_participant(&link.link, descriptor_version.clone(), &governance)?,
        exact_read_participant(&link.target, target_version.clone(), &governance)?,
    ];
    objects.sort_by(|left, right| left.path.cmp(&right.path));
    let target_path = atomic_path(&link.target)?;
    let target_participant_index = objects
        .iter()
        .position(|participant| participant.path == target_path)
        .and_then(|index| u32::try_from(index).ok())
        .ok_or_else(|| Status::internal("immutable target participant is absent"))?;
    objects[target_participant_index as usize].alias_registry =
        Some(ProgramAliasRegistryCondition::Exact(registry.clone()));
    let mut read_proofs = vec![proof_for(
        &objects,
        &atomic_path(&link.link)?,
        descriptor_version,
        ObjectLinkDescriptor::new(link.target.path())
            .map_err(|error| Status::data_loss(error.to_string()))?
            .encode(),
    )?];
    read_proofs.sort_by_key(|proof| proof.participant_index);
    let head_preconditions = objects
        .iter()
        .map(|participant| {
            let ProgramPathCondition::HeadVersion { expected } = &participant.condition else {
                unreachable!();
            };
            HeadPrecondition {
                path: participant.path.clone(),
                expected: observed(Some(expected)),
            }
        })
        .collect();
    let plan = BuiltInObjectTransactionPlan {
        authority_kind: PUT_IMMUTABLE_THROUGH_LINK_AUTHORITY_KIND,
        contract_version: OBJECT_LINK_CONTRACT_VERSION,
        participant_manifest: ProgramParticipantManifest {
            format: PROGRAM_PARTICIPANT_MANIFEST_FORMAT,
            objects,
            governance: vec![ProgramGovernanceParticipant {
                tenant: publish.key.tenant().to_owned(),
                bucket: publish.key.bucket().to_owned(),
                tenant_id: governance.tenant_id,
                bucket_id: governance.bucket_id,
                policy: governance.policy,
                versioning: governance.versioning,
            }],
        },
        head_preconditions,
        read_proofs,
        assertions: vec![BuiltInTransactionAssertion::PutImmutableMatches {
            target_participant_index,
            blob_hash: publish.blob.hash,
            blob_length: publish.blob.length,
            content_type: publish.content_type.clone(),
            upload_source_node_id,
        }],
        alias_registries: vec![BuiltInAliasRegistryAccess::Read {
            target_participant_index,
            expected: Some(registry),
        }],
        alias_observations: Vec::new(),
        writes: Vec::new(),
        receipt: CommandReceipt {
            program_path_hash: [0; 32],
            command_id: command_id.to_owned(),
            input_fingerprint: hex::encode(fingerprint),
            outputs: BTreeMap::new(),
        },
    };
    let result = service
        .programs
        .invoke_builtin_object_transaction(
            plan,
            crate::programs::builtin_invocation_identity(
                PUT_IMMUTABLE_THROUGH_LINK_AUTHORITY_KIND,
                command_id,
            ),
            fingerprint,
            match publish.durability {
                keldra_store::Durability::Local => "local",
                keldra_store::Durability::Replicated => "replicated",
            },
            budget,
        )
        .await?;
    linked_immutable_response(result, command_id, fingerprint, &target_path)
}

fn linked_immutable_response(
    result: crate::programs::InvokedProgramResult,
    command_id: &str,
    fingerprint: [u8; 32],
    target_path: &ObjectPath,
) -> Result<MutationReceipt, Status> {
    let version = result
        .asserted_versions
        .get(target_path)
        .ok_or_else(|| Status::data_loss("linked PutImmutable omitted its asserted target"))?;
    if version.deleted {
        return Err(Status::data_loss(
            "linked PutImmutable asserted a deleted target",
        ));
    }
    Ok(api_receipt(keldra_store::MutationReceipt {
        command_id: Some(command_id.to_owned()),
        fingerprint,
        version: version.id,
        deleted: false,
        replayed: result.replayed,
        replay_guarantee_expires_at_unix_millis: result.replay_guarantee_expires_at_unix_millis,
    }))
}

fn exact_read_participant(
    key: &ObjectKey,
    expected: Version,
    governance: &ObjectMutationGovernance,
) -> Result<ProgramObjectParticipant, Status> {
    Ok(ProgramObjectParticipant {
        tenant_id: governance.tenant_id,
        bucket_id: governance.bucket_id,
        path: atomic_path(key)?,
        condition: ProgramPathCondition::HeadVersion { expected },
        intent: ProgramParticipantIntent {
            read: true,
            put: false,
            delete: false,
        },
        alias_registry: None,
    })
}

fn proof_for(
    objects: &[ProgramObjectParticipant],
    path: &ObjectPath,
    expected: Version,
    bytes: Vec<u8>,
) -> Result<BuiltInReadProof, Status> {
    objects
        .iter()
        .position(|participant| participant.path == *path)
        .and_then(|index| u32::try_from(index).ok())
        .map(|participant_index| BuiltInReadProof {
            participant_index,
            expected,
            bytes,
        })
        .ok_or_else(|| Status::internal("immutable read proof participant is absent"))
}

fn linked_put_response(
    result: crate::programs::InvokedProgramResult,
    command_id: &str,
    fingerprint: [u8; 32],
    target_path: &ObjectPath,
) -> Result<MutationReceipt, Status> {
    let (version, deleted) = result
        .published_versions
        .get(target_path)
        .map(|published| (published.version, published.deleted))
        .ok_or_else(|| Status::data_loss("linked Put published no target version"))?;
    Ok(api_receipt(keldra_store::MutationReceipt {
        command_id: Some(command_id.to_owned()),
        fingerprint,
        version,
        deleted,
        replayed: result.replayed,
        replay_guarantee_expires_at_unix_millis: result.replay_guarantee_expires_at_unix_millis,
    }))
}

pub(super) fn bulk_replay_result(
    result: crate::programs::InvokedProgramResult,
    requested: &ObjectKey,
    command_id: &str,
    fingerprint: [u8; 32],
    authority_kind: u16,
) -> Result<(ObjectKey, MutationReceipt), Status> {
    let requested_path = atomic_path(requested)?;
    let target_path = result
        .alias_targets
        .get(&requested_path)
        .cloned()
        .ok_or_else(|| Status::data_loss("bulk linked replay omitted its canonical target"))?;
    let target = ObjectKey::new(&target_path.tenant, &target_path.bucket, &target_path.path)
        .map_err(|error| Status::data_loss(error.to_string()))?;
    let receipt = match authority_kind {
        UNLINK_OBJECT_AUTHORITY_KIND => {
            link_response(result, command_id, fingerprint, &requested_path)?.into_inner()
        }
        PUT_THROUGH_LINK_AUTHORITY_KIND => {
            linked_put_response(result, command_id, fingerprint, &target_path)?
        }
        PUT_IMMUTABLE_THROUGH_LINK_AUTHORITY_KIND => {
            linked_immutable_response(result, command_id, fingerprint, &target_path)?
        }
        _ => return Err(Status::internal("unsupported bulk linked replay authority")),
    };
    Ok((target, receipt))
}

pub(super) fn linked_put_fingerprint(link: &ObjectKey, publish: &PublishRequest) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key("keldra.put-through-object-link/v2");
    for value in [link.tenant(), link.bucket(), link.path()] {
        hasher.update(&(value.len() as u64).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    match publish.mode {
        PutMode::Put => {
            hasher.update(&[0]);
        }
        PutMode::PutIfAbsent => {
            hasher.update(&[1]);
        }
        PutMode::PutIfVersion(version) => {
            hasher.update(&[2]);
            hasher.update(&version.0.to_be_bytes());
        }
        PutMode::PutImmutable => {
            hasher.update(&[3]);
        }
    };
    hasher.update(&[match publish.durability {
        keldra_store::Durability::Local => 0,
        keldra_store::Durability::Replicated => 1,
    }]);
    match &publish.content_type {
        Some(content_type) => {
            hasher.update(&[1]);
            hasher.update(&(content_type.len() as u64).to_be_bytes());
            hasher.update(content_type.as_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
    hasher.update(&publish.blob.hash);
    hasher.update(&publish.blob.length.to_be_bytes());
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use keldra_store::{BlobRef, VersionId};

    use super::*;

    fn version(protected_link_descriptor: bool) -> Version {
        Version {
            id: keldra_store::VersionId(1),
            blob: Some(BlobRef {
                hash: [1; 32],
                length: 1,
            }),
            content_type: Some(OBJECT_LINK_CONTENT_TYPE.into()),
            deleted: false,
            committed_at_unix_millis: 1,
            protected_link_descriptor,
        }
    }

    #[test]
    fn protected_origin_controls_delete_visibility_and_link_targets() {
        let historical = version(false);
        assert!(require_public_version_metadata(Some(&historical)).is_ok());
        assert!(require_ordinary_link_target(&historical).is_ok());
        assert_eq!(
            require_public_version_metadata(Some(&version(true)))
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
        assert_eq!(
            require_ordinary_link_target(&version(true))
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );
    }

    #[test]
    fn ordinary_versions_do_not_request_descriptor_payloads() {
        assert!(protected_descriptor(&version(false)).unwrap().is_none());
    }

    #[test]
    fn protected_descriptor_keeps_its_exact_version_binding() {
        let link = ObjectKey::new("tenant", "bucket", "aliases/current").unwrap();
        let encoded = ObjectLinkDescriptor::new("objects/target")
            .unwrap()
            .encode();
        let resolved = decode_protected_descriptor(link.clone(), VersionId(41), &encoded).unwrap();

        assert_eq!(resolved.link, link);
        assert_eq!(resolved.descriptor_version, VersionId(41));
        assert_eq!(resolved.target.path(), "objects/target");
    }

    #[test]
    fn protected_descriptor_corruption_fails_closed_before_dispatch() {
        let mut wrong_type = version(true);
        wrong_type.content_type = Some("application/octet-stream".into());
        assert_eq!(
            protected_descriptor(&wrong_type).unwrap_err().code(),
            tonic::Code::DataLoss
        );

        let mut missing_payload = version(true);
        missing_payload.blob = None;
        assert_eq!(
            protected_descriptor(&missing_payload).unwrap_err().code(),
            tonic::Code::DataLoss
        );

        let link = ObjectKey::new("tenant", "bucket", "aliases/current").unwrap();
        assert_eq!(
            decode_protected_descriptor(link, VersionId(41), b"not-a-descriptor")
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );
    }

    #[test]
    fn protected_link_requires_target_registry_membership_and_an_ordinary_live_target() {
        let link = ObjectKey::new("tenant", "bucket", "aliases/current").unwrap();
        let target_key = ObjectKey::new("tenant", "bucket", "objects/target").unwrap();
        let resolved = ResolvedObjectLink {
            link: link.clone(),
            descriptor_version: VersionId(41),
            target: target_key,
        };
        let target_version = Version {
            content_type: Some("application/octet-stream".into()),
            ..version(false)
        };
        let mut target = CurrentObjectSnapshot {
            tenant_id: 1,
            bucket_id: 1,
            exact_path: "objects/target".into(),
            head: keldra_store::Head {
                version: target_version.id,
                deleted: false,
                mutation_stamp: None,
            },
            version: target_version,
            alias_registry: Some(ObjectAliasRegistry {
                format: keldra_store::OBJECT_ALIAS_REGISTRY_FORMAT,
                revision: 1,
                aliases: vec![link.path().into()],
                program_commit_cursor: Some(1),
            }),
        };
        assert!(validate_resolved_target(&resolved, &target).is_ok());

        target.alias_registry.as_mut().unwrap().aliases = vec!["aliases/other".into()];
        assert_eq!(
            validate_resolved_target(&resolved, &target)
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );

        target.alias_registry.as_mut().unwrap().aliases = vec![link.path().into()];
        target.version.protected_link_descriptor = true;
        assert_eq!(
            validate_resolved_target(&resolved, &target)
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );
    }
}
