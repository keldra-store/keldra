use keldra_api::v1::clone_object_request::Operation;
use keldra_api::v1::{CloneObjectRequest as ApiCloneObjectRequest, MutationReceipt};
use keldra_atomic_program::{CommandReceipt, HeadPrecondition, ObjectPath, ObservedHead};
use keldra_store::{
    BuiltInAliasObservation, BuiltInAliasRegistryAccess, BuiltInObjectTransactionPlan,
    BuiltInReadProof, BuiltInTransactionAssertion, BuiltInVersionWrite, BuiltInWritePayload,
    ExistingReferenceWrite, ObjectKey, PROGRAM_PARTICIPANT_MANIFEST_FORMAT,
    ProgramAliasRegistryCondition, ProgramGovernanceParticipant, ProgramObjectParticipant,
    ProgramParticipantIntent, ProgramParticipantManifest, ProgramPathCondition, PutMode, Version,
    VersionId,
};
use std::collections::BTreeMap;
use std::io::Read;
use tonic::{Request, Response, Status};

use super::{
    ObjectServiceImpl, api_receipt, deadline_remaining, durability, object_key, object_link,
    plugin_object_scope, request_deadline, require_plugin_key_scope, required_command_id,
    routed_writes,
};
use crate::authentication::{Caller, PluginObjectScope};
use crate::authorization::ObjectPermission;
use crate::distributed_list::OriginalBearer;
use crate::object_path_access;
use crate::v05::request_auth::authenticated_caller;

pub(super) async fn clone_object(
    service: &ObjectServiceImpl,
    request: Request<ApiCloneObjectRequest>,
) -> Result<Response<MutationReceipt>, Status> {
    let plugin_scope = plugin_object_scope(&request);
    let peer_routed = request
        .extensions()
        .get::<routed_writes::RoutedDestination>()
        .is_some();
    let caller = authenticated_caller(&request)?;
    let path_access = object_path_access::access_for(&request);
    let bearer = OriginalBearer::from_metadata(request.metadata())?;
    let deadline = request_deadline(request.metadata(), service.atomic_program_timeout)?;
    let api_request = request.into_inner();
    let source = object_key(api_request.source.clone())?;
    let destination = object_key(api_request.destination.clone())?;
    if api_request.source_version == 0
        || source.tenant() != destination.tenant()
        || source.bucket() != destination.bucket()
        || source == destination
    {
        return Err(Status::invalid_argument(
            "clone requires a non-zero exact source version and a distinct destination in the same tenant and bucket",
        ));
    }
    object_path_access::require_key(&path_access, &source)?;
    object_path_access::require_key(&path_access, &destination)?;
    require_plugin_key_scope(plugin_scope.as_ref(), &source)?;
    require_plugin_key_scope(plugin_scope.as_ref(), &destination)?;
    let command_id = required_command_id(api_request.command_id.clone())?;
    let durability = durability(api_request.durability)?;
    service
        .distribution
        .require_durability_available(durability)?;
    let mode = clone_mode(api_request.operation.as_ref())?;
    let source_requested_path = object_path(&source)?;
    let destination_requested_path = object_path(&destination)?;
    let fingerprint = clone_input_fingerprint(
        &source_requested_path,
        api_request.source_version,
        &destination_requested_path,
        mode,
        durability,
    );
    let governance = service
        .bucket_governance
        .resolve(destination.tenant(), destination.bucket())
        .await?;

    match service.programs.executor_routing_target()? {
        Some(_) if peer_routed => {
            return Err(Status::failed_precondition(
                "a routed CloneObject reached a node that is not the atomic executor",
            ));
        }
        Some((target, address)) => {
            return service
                .cluster_peers
                .route_clone_object(
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

    if let Some(result) = service
        .programs
        .replay_builtin_object_transaction(
            1,
            1,
            crate::programs::builtin_invocation_identity(1, &command_id),
            fingerprint,
        )
        .await?
    {
        authorize_clone_result_targets(
            service,
            &caller,
            &path_access,
            plugin_scope.as_ref(),
            &result,
            &source_requested_path,
            &destination_requested_path,
        )
        .await?;
        return clone_result_response(
            result,
            &destination_requested_path,
            &command_id,
            fingerprint,
        );
    }

    let source_resolution = object_link::resolve_current(service, source.clone()).await?;
    let destination_resolution = object_link::resolve_current(service, destination.clone()).await?;
    let canonical_source = source_resolution.canonical().clone();
    let canonical_destination = destination_resolution.canonical().clone();
    object_path_access::require_key(&path_access, &canonical_source)?;
    object_path_access::require_key(&path_access, &canonical_destination)?;
    require_plugin_key_scope(plugin_scope.as_ref(), &canonical_source)?;
    require_plugin_key_scope(plugin_scope.as_ref(), &canonical_destination)?;
    service
        .authorize_object(&caller, &canonical_source, ObjectPermission::Get)
        .await?;
    service
        .authorize_object(&caller, &canonical_destination, ObjectPermission::Put)
        .await?;

    // Membership cutover cannot split exact source selection from destination
    // publication. The destination mutation performs its own exact-path
    // reconciliation and authoritative CAS under the same admission epoch.
    let _placement_guard = service.distribution.enter_mutation()?;
    let selected = service
        .reader
        .exact_versions_stable(
            std::slice::from_ref(&canonical_source),
            &[VersionId(api_request.source_version)],
            governance.tenant_id,
            governance.bucket_id,
        )
        .await?
        .pop()
        .flatten()
        .ok_or_else(|| Status::not_found("clone source version was not found"))?;
    if selected.deleted {
        return Err(Status::not_found("clone source version is deleted"));
    }
    let blob = selected
        .blob
        .clone()
        .ok_or_else(|| Status::data_loss("clone source version has no payload reference"))?;
    if selected.protected_link_descriptor {
        return Err(Status::invalid_argument(
            "CloneObject cannot copy Keldra's reserved object-link representations",
        ));
    }
    let destination_current = service
        .reader
        .current_head_snapshot_stable(
            &canonical_destination,
            governance.tenant_id,
            governance.bucket_id,
        )
        .await?;
    let destination_expected = match (mode, destination_current.as_ref()) {
        (PutMode::Put, None) | (PutMode::PutIfAbsent, None) => ObservedHead::NeverExisted,
        (PutMode::Put, Some(current)) => ObservedHead::Version {
            version: current.head.version.0.to_string(),
        },
        (PutMode::PutIfAbsent, Some(_)) => {
            return Err(Status::failed_precondition(
                "clone destination already exists",
            ));
        }
        (PutMode::PutIfVersion(expected), Some(current)) if current.head.version == expected => {
            ObservedHead::Version {
                version: expected.0.to_string(),
            }
        }
        (PutMode::PutIfVersion(_), _) => {
            return Err(Status::failed_precondition(
                "clone destination version precondition failed",
            ));
        }
        (PutMode::PutImmutable, _) => unreachable!("clone_mode rejects PutImmutable"),
    };
    let source_path = ObjectPath::new(
        canonical_source.tenant(),
        canonical_source.bucket(),
        canonical_source.path(),
    )
    .map_err(Status::invalid_argument)?;
    let destination_path = ObjectPath::new(
        canonical_destination.tenant(),
        canonical_destination.bucket(),
        canonical_destination.path(),
    )
    .map_err(Status::invalid_argument)?;
    let source_alias = alias_proof(service, &source_resolution).await?;
    let destination_alias = alias_proof(service, &destination_resolution).await?;
    let source_registry = if source_alias.is_some() {
        Some(
            service
                .reader
                .current_head_snapshot_stable(
                    &canonical_source,
                    governance.tenant_id,
                    governance.bucket_id,
                )
                .await?
                .and_then(|current| current.alias_registry)
                .ok_or_else(|| Status::data_loss("clone source alias sidecar is absent"))?,
        )
    } else {
        None
    };
    let destination_registry = destination_current
        .as_ref()
        .and_then(|current| current.alias_registry.clone());
    let mut objects = if source_path == destination_path {
        let head = destination_current
            .as_ref()
            .ok_or_else(|| Status::data_loss("same-target clone has no current head"))?
            .version
            .clone();
        vec![clone_participant(
            &governance,
            source_path.clone(),
            ProgramPathCondition::HeadAndRetainedVersion {
                head,
                retained: selected.clone(),
            },
            true,
        )]
    } else {
        vec![
            clone_participant(
                &governance,
                source_path.clone(),
                ProgramPathCondition::RetainedVersion {
                    expected: selected.clone(),
                },
                false,
            ),
            clone_participant(
                &governance,
                destination_path.clone(),
                ProgramPathCondition::Head(destination_expected.clone()),
                true,
            ),
        ]
    };
    for alias in [source_alias.as_ref(), destination_alias.as_ref()]
        .into_iter()
        .flatten()
    {
        if objects
            .iter()
            .all(|participant| participant.path != alias.path)
        {
            objects.push(clone_participant(
                &governance,
                alias.path.clone(),
                ProgramPathCondition::HeadVersion {
                    expected: alias.version.clone(),
                },
                false,
            ));
        }
    }
    objects.sort_by(|left, right| left.path.cmp(&right.path));
    let source_participant_index = objects
        .iter()
        .position(|participant| participant.path == source_path)
        .ok_or_else(|| Status::internal("clone source participant disappeared"))?
        as u32;
    let destination_participant_index = objects
        .iter()
        .position(|participant| participant.path == destination_path)
        .ok_or_else(|| Status::internal("clone destination participant disappeared"))?
        as u32;
    let mut alias_registries = Vec::new();
    if source_alias.is_some() {
        alias_registries.push(BuiltInAliasRegistryAccess::Read {
            target_participant_index: source_participant_index,
            expected: source_registry.clone(),
        });
    }
    alias_registries.push(BuiltInAliasRegistryAccess::Read {
        target_participant_index: destination_participant_index,
        expected: destination_registry.clone(),
    });
    alias_registries.sort_by_key(|access| match access {
        BuiltInAliasRegistryAccess::Read {
            target_participant_index,
            ..
        } => *target_participant_index,
        BuiltInAliasRegistryAccess::Write { .. } => unreachable!(),
    });
    alias_registries.dedup_by_key(|access| match access {
        BuiltInAliasRegistryAccess::Read {
            target_participant_index,
            ..
        } => *target_participant_index,
        BuiltInAliasRegistryAccess::Write { .. } => unreachable!(),
    });
    for access in &alias_registries {
        let BuiltInAliasRegistryAccess::Read {
            target_participant_index,
            expected,
        } = access
        else {
            unreachable!();
        };
        objects[*target_participant_index as usize].alias_registry = Some(match expected {
            Some(registry) => ProgramAliasRegistryCondition::Exact(registry.clone()),
            None => ProgramAliasRegistryCondition::Absent,
        });
    }
    let mut read_proofs = [source_alias.as_ref(), destination_alias.as_ref()]
        .into_iter()
        .flatten()
        .map(|alias| alias_read_proof(&objects, alias))
        .collect::<Result<Vec<_>, _>>()?;
    read_proofs.sort_by_key(|proof| proof.participant_index);
    read_proofs.dedup_by_key(|proof| proof.participant_index);
    let alias_observations = destination_registry
        .as_ref()
        .map_or(&[][..], |registry| registry.aliases.as_slice())
        .iter()
        .map(|path| {
            Ok(BuiltInAliasObservation {
                requested_path: ObjectPath::new(destination.tenant(), destination.bucket(), path)
                    .map_err(Status::data_loss)?,
                canonical_participant_index: destination_participant_index,
                deleted: false,
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    let head_preconditions = objects_head_preconditions(&objects);
    let plan = BuiltInObjectTransactionPlan {
        authority_kind: 1,
        contract_version: 1,
        participant_manifest: ProgramParticipantManifest {
            format: PROGRAM_PARTICIPANT_MANIFEST_FORMAT,
            objects,
            governance: vec![ProgramGovernanceParticipant {
                tenant: destination.tenant().to_owned(),
                bucket: destination.bucket().to_owned(),
                tenant_id: governance.tenant_id,
                bucket_id: governance.bucket_id,
                policy: governance.policy.clone(),
                versioning: governance.versioning,
            }],
        },
        head_preconditions,
        read_proofs,
        assertions: vec![BuiltInTransactionAssertion::ClonePaths {
            source_requested_path,
            destination_requested_path: destination_requested_path.clone(),
            source_participant_index,
            destination_participant_index,
        }],
        alias_registries,
        alias_observations,
        writes: vec![BuiltInVersionWrite {
            path: destination_path,
            expected: destination_expected,
            previous_version: destination_current.map(|current| current.version),
            payload: BuiltInWritePayload::ExistingReference(ExistingReferenceWrite {
                source_participant_index,
                blob_hash: blob.hash,
                blob_length: blob.length,
                content_type: selected.content_type,
            }),
        }],
        receipt: CommandReceipt {
            program_path_hash: [0; 32],
            command_id: command_id.clone(),
            input_fingerprint: hex::encode(fingerprint),
            outputs: BTreeMap::new(),
        },
    };
    let durability_class = match durability {
        keldra_store::Durability::Local => "local",
        keldra_store::Durability::Replicated => "replicated",
    };
    let result = service
        .programs
        .invoke_builtin_object_transaction(
            plan,
            crate::programs::builtin_invocation_identity(1, &command_id),
            fingerprint,
            durability_class,
            deadline_remaining(deadline)?,
        )
        .await?;
    clone_result_response(
        result,
        &destination_requested_path,
        &command_id,
        fingerprint,
    )
}

async fn authorize_clone_result_targets(
    service: &ObjectServiceImpl,
    caller: &Caller,
    path_access: &object_path_access::ObjectPathAccess,
    plugin_scope: Option<&PluginObjectScope>,
    result: &crate::programs::InvokedProgramResult,
    source_requested: &ObjectPath,
    destination_requested: &ObjectPath,
) -> Result<(), Status> {
    // Requested paths remain constrained by the request's process-local path
    // capability and plugin token. Zanzibar authorization follows the sealed
    // canonical paths, including during replay after an alias was unlinked.
    for requested in [source_requested, destination_requested] {
        let key = ObjectKey::new(&requested.tenant, &requested.bucket, &requested.path)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        object_path_access::require_key(path_access, &key)?;
        require_plugin_key_scope(plugin_scope, &key)?;
    }
    let source = result
        .alias_targets
        .get(source_requested)
        .ok_or_else(|| Status::data_loss("Clone replay omitted its canonical source"))?;
    let destination = result
        .alias_targets
        .get(destination_requested)
        .ok_or_else(|| Status::data_loss("Clone replay omitted its canonical destination"))?;
    let source = ObjectKey::new(&source.tenant, &source.bucket, &source.path)
        .map_err(|error| Status::data_loss(error.to_string()))?;
    let destination = ObjectKey::new(&destination.tenant, &destination.bucket, &destination.path)
        .map_err(|error| Status::data_loss(error.to_string()))?;
    object_path_access::require_key(path_access, &source)?;
    object_path_access::require_key(path_access, &destination)?;
    require_plugin_key_scope(plugin_scope, &source)?;
    require_plugin_key_scope(plugin_scope, &destination)?;
    service
        .authorize_object(caller, &source, ObjectPermission::Get)
        .await?;
    service
        .authorize_object(caller, &destination, ObjectPermission::Put)
        .await
}

fn clone_result_response(
    result: crate::programs::InvokedProgramResult,
    destination_requested: &ObjectPath,
    command_id: &str,
    fingerprint: [u8; 32],
) -> Result<Response<MutationReceipt>, Status> {
    let destination = result
        .alias_targets
        .get(destination_requested)
        .ok_or_else(|| Status::data_loss("Clone result omitted its canonical destination"))?;
    let published = result
        .published_versions
        .get(destination)
        .ok_or_else(|| Status::data_loss("Clone published no destination version"))?;
    Ok(Response::new(api_receipt(keldra_store::MutationReceipt {
        command_id: Some(command_id.to_owned()),
        fingerprint,
        version: published.version,
        deleted: published.deleted,
        replayed: result.replayed,
        replay_guarantee_expires_at_unix_millis: result.replay_guarantee_expires_at_unix_millis,
    })))
}

struct CloneAliasProof {
    path: ObjectPath,
    version: Version,
    bytes: Vec<u8>,
}

async fn alias_proof(
    service: &ObjectServiceImpl,
    resolution: &object_link::ResolvedAddress,
) -> Result<Option<CloneAliasProof>, Status> {
    let object_link::ResolvedAddress::Link(link) = resolution else {
        return Ok(None);
    };
    let opened = service
        .reader
        .open(&link.link, Some(link.descriptor_version))
        .await?
        .ok_or_else(|| Status::failed_precondition("clone alias descriptor changed"))?;
    if !opened.version.protected_link_descriptor
        || opened.version.content_type.as_deref() != Some(keldra_store::OBJECT_LINK_CONTENT_TYPE)
    {
        return Err(Status::failed_precondition(
            "clone alias descriptor changed",
        ));
    }
    let mut payload = opened
        .payload
        .ok_or_else(|| Status::data_loss("clone alias descriptor has no payload"))?
        .into_spool();
    let mut bytes = Vec::new();
    payload
        .read_to_end(&mut bytes)
        .map_err(|error| Status::internal(format!("read clone alias descriptor: {error}")))?;
    Ok(Some(CloneAliasProof {
        path: object_path(&link.link)?,
        version: opened.version,
        bytes,
    }))
}

fn object_path(key: &keldra_store::ObjectKey) -> Result<ObjectPath, Status> {
    ObjectPath::new(key.tenant(), key.bucket(), key.path()).map_err(Status::invalid_argument)
}

fn clone_participant(
    governance: &keldra_store::ObjectMutationGovernance,
    path: ObjectPath,
    condition: ProgramPathCondition,
    put: bool,
) -> ProgramObjectParticipant {
    ProgramObjectParticipant {
        tenant_id: governance.tenant_id,
        bucket_id: governance.bucket_id,
        path,
        condition,
        alias_registry: None,
        intent: ProgramParticipantIntent {
            read: true,
            put,
            delete: false,
        },
    }
}

fn alias_read_proof(
    objects: &[ProgramObjectParticipant],
    alias: &CloneAliasProof,
) -> Result<BuiltInReadProof, Status> {
    let participant_index = objects
        .iter()
        .position(|participant| participant.path == alias.path)
        .and_then(|index| u32::try_from(index).ok())
        .ok_or_else(|| Status::internal("clone alias proof participant disappeared"))?;
    Ok(BuiltInReadProof {
        participant_index,
        expected: alias.version.clone(),
        bytes: alias.bytes.clone(),
    })
}

fn objects_head_preconditions(objects: &[ProgramObjectParticipant]) -> Vec<HeadPrecondition> {
    objects
        .iter()
        .filter_map(|participant| {
            let expected = match &participant.condition {
                ProgramPathCondition::Head(expected) => expected.clone(),
                ProgramPathCondition::HeadVersion { expected }
                | ProgramPathCondition::HeadAndRetainedVersion { head: expected, .. } => {
                    ObservedHead::Version {
                        version: expected.id.0.to_string(),
                    }
                }
                ProgramPathCondition::RetainedVersion { .. } => return None,
            };
            Some(HeadPrecondition {
                path: participant.path.clone(),
                expected,
            })
        })
        .collect()
}

fn clone_input_fingerprint(
    source: &ObjectPath,
    source_version: u64,
    destination: &ObjectPath,
    mode: PutMode,
    durability: keldra_store::Durability,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key("keldra.clone-object/v3");
    for value in [
        source.tenant.as_str(),
        source.bucket.as_str(),
        source.path.as_str(),
        destination.tenant.as_str(),
        destination.bucket.as_str(),
        destination.path.as_str(),
    ] {
        hasher.update(&(value.len() as u64).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    hasher.update(&source_version.to_be_bytes());
    hasher.update(format!("{mode:?}:{durability:?}").as_bytes());
    *hasher.finalize().as_bytes()
}

fn clone_mode(operation: Option<&Operation>) -> Result<PutMode, Status> {
    Ok(match operation {
        Some(Operation::Put(_)) => PutMode::Put,
        Some(Operation::PutIfAbsent(_)) => PutMode::PutIfAbsent,
        Some(Operation::PutIfVersion(value)) if value.expected_version != 0 => {
            PutMode::PutIfVersion(VersionId(value.expected_version))
        }
        Some(Operation::PutIfVersion(_)) => {
            return Err(Status::invalid_argument(
                "clone destination expected_version must be non-zero",
            ));
        }
        None => {
            return Err(Status::invalid_argument(
                "clone destination operation is required",
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use keldra_api::v1::{PutIfAbsentOperation, PutIfVersionOperation, PutOperation};

    use super::*;

    #[test]
    fn clone_accepts_only_the_three_ordinary_destination_operations() {
        assert_eq!(
            clone_mode(Some(&Operation::Put(PutOperation {}))).unwrap(),
            PutMode::Put
        );
        assert_eq!(
            clone_mode(Some(&Operation::PutIfAbsent(PutIfAbsentOperation {}))).unwrap(),
            PutMode::PutIfAbsent
        );
        assert_eq!(
            clone_mode(Some(&Operation::PutIfVersion(PutIfVersionOperation {
                expected_version: 9,
            })))
            .unwrap(),
            PutMode::PutIfVersion(VersionId(9))
        );
        assert_eq!(
            clone_mode(None).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            clone_mode(Some(&Operation::PutIfVersion(PutIfVersionOperation {
                expected_version: 0,
            })))
            .unwrap_err()
            .code(),
            tonic::Code::InvalidArgument
        );
    }
}
