use std::time::Duration;

use keldra_api::v1::{
    MaterializePersonalDbProjectionRequest, PersonalDbCommit, PersonalDbMaterialization,
};
use keldra_store::PlacementLogId;
use personaldb_protocol::{
    CommitCertificateV2, DatabaseGroupKind, LogEntryCoreV2, MAX_SYNC_PAGE_BYTES,
    MAX_SYNC_PAGE_ENTRIES, ProjectionDefinitionV1, ProjectionDerivationV1, Sha256Digest,
    SignedProjectionDerivationV1, SourceHeadV1, StateCommitmentV1, UnsignedCommitCertificateV2,
    UnsignedCommittedHeadV2,
};
use tonic::{Request, Status};

use super::authorization::GroupPermission;
use crate::authentication::Caller;
use crate::distributed_list::OriginalBearer;
use crate::v05::{deadline_remaining, request_deadline};

use super::model::{
    GroupScope, digest, entry_certificate_path, entry_payload_path, projection_definition_path,
    protocol_status, storage_command_id, validate_command_id,
};
use super::service::{PersonalDbServiceImpl, authenticated_caller};
use super::storage::ConditionalWrite;

pub(super) async fn materialize(
    service: &PersonalDbServiceImpl,
    request: Request<MaterializePersonalDbProjectionRequest>,
) -> Result<PersonalDbMaterialization, Status> {
    let deadline = request_deadline(request.metadata(), service.request_timeout)?;
    let caller = authenticated_caller(&request)?;
    let bearer = OriginalBearer::from_metadata(request.metadata())?;
    let request = request.into_inner();
    let scope = prepare(service, caller, bearer, &request).await?;
    service
        .require_permission(&scope, GroupPermission::Materialize, "materialization")
        .await?;
    route_or_execute(service, scope, request, deadline_remaining(deadline)?).await
}

pub(super) async fn execute_routed_materialization(
    service: &PersonalDbServiceImpl,
    caller: Caller,
    bearer: OriginalBearer,
    request: MaterializePersonalDbProjectionRequest,
    fence: PlacementLogId,
) -> Result<PersonalDbMaterialization, Status> {
    let scope = prepare(service, caller, bearer, &request).await?;
    service
        .require_permission(&scope, GroupPermission::Materialize, "materialization")
        .await?;
    service.placement.require_local_primary(&scope, fence)?;
    execute_local(service, scope, request, fence).await
}

async fn prepare(
    service: &PersonalDbServiceImpl,
    caller: Caller,
    bearer: OriginalBearer,
    request: &MaterializePersonalDbProjectionRequest,
) -> Result<GroupScope, Status> {
    validate_request(request)?;
    service
        .request_scope(
            caller,
            bearer,
            &request.bucket,
            &request.database_id,
            &request.group_id,
        )
        .await
}

async fn route_or_execute(
    service: &PersonalDbServiceImpl,
    scope: GroupScope,
    request: MaterializePersonalDbProjectionRequest,
    remaining: Duration,
) -> Result<PersonalDbMaterialization, Status> {
    let primary = service.placement.primary(&scope)?;
    if let Some(address) = primary.address {
        return service
            .peers
            .route_materialize_personaldb_projection(
                primary.node_id,
                &address,
                scope.bearer.signed_token(),
                request,
                remaining,
            )
            .await;
    }
    execute_local(service, scope, request, primary.fence).await
}

async fn execute_local(
    service: &PersonalDbServiceImpl,
    target: GroupScope,
    request: MaterializePersonalDbProjectionRequest,
    fence: PlacementLogId,
) -> Result<PersonalDbMaterialization, Status> {
    service.placement.require_local_primary(&target, fence)?;
    let lock = service.lock_index(&target);
    let _guard = service.locks[lock].lock().await;
    service.placement.require_local_primary(&target, fence)?;
    let target_manifest = service.require_manifest(&target).await?;
    if target_manifest.kind() != DatabaseGroupKind::Projection {
        return Err(Status::failed_precondition(
            "MaterializeProjection requires a projection group",
        ));
    }
    let definition_bytes = service
        .objects
        .read(&target, projection_definition_path())
        .await?
        .bytes
        .ok_or_else(|| Status::data_loss("projection definition is missing"))?;
    let definition =
        ProjectionDefinitionV1::decode_canonical(&definition_bytes).map_err(protocol_status)?;
    if definition.projection_database_id != target.database_id
        || definition.projection_group_id != target.group_id
        || Some(definition.canonical_sha256().map_err(protocol_status)?)
            != target_manifest.projection_hash()
    {
        return Err(Status::data_loss(
            "projection definition does not match its published group",
        ));
    }
    let source = service
        .request_scope(
            target.caller.clone(),
            target.bearer.clone(),
            &definition.source_bucket,
            &definition.source_database_id.0,
            &definition.source_group_id,
        )
        .await?;
    let source_authorization = service
        .require_permission(&source, GroupPermission::Read, "projection source read")
        .await?;
    let source_manifest = service.require_manifest(&source).await?;
    if source_manifest.kind() != DatabaseGroupKind::Source
        || source_manifest.schema_hash() != target_manifest.schema_hash()
    {
        return Err(Status::failed_precondition(
            "mirror source and target schemas or group kinds are incompatible",
        ));
    }
    let requested_hash = digest(
        "through_source_log_hash_sha256",
        &request.through_source_log_hash_sha256,
    )?;
    let (_, source_head, _) = service.load_head(&source).await?;
    if request.through_source_log_index > source_head.state().log_index {
        return Err(Status::failed_precondition(
            "requested source checkpoint is newer than the committed source head",
        ));
    }
    let actual_requested_hash =
        source_hash_at(service, &source, request.through_source_log_index).await?;
    if actual_requested_hash != requested_hash {
        return Err(Status::failed_precondition(
            "requested source checkpoint hash does not match its committed index",
        ));
    }

    let (mut target_head_version, mut target_head, _) = service.load_head(&target).await?;
    let (mut source_index, mut source_hash) =
        target_source_checkpoint(service, &target, &target_head, &definition).await?;
    if request.through_source_log_index < source_index {
        return Err(Status::failed_precondition(
            "projection is already ahead of the requested source checkpoint",
        ));
    }
    if request.through_source_log_index == source_index {
        if requested_hash != source_hash {
            return Err(Status::failed_precondition(
                "projection source checkpoint disagrees with the requested hash",
            ));
        }
        return Ok(PersonalDbMaterialization {
            commits: Vec::new(),
            source_log_index: source_index,
            source_log_hash_sha256: source_hash.as_bytes().to_vec(),
        });
    }

    let max_entries = effective_entries(request.max_entries);
    let max_bytes = effective_bytes(request.max_bytes);
    let mut delivered_bytes = 0_u64;
    let mut commits = Vec::new();
    while source_index < request.through_source_log_index && commits.len() < max_entries {
        let next_source_index = source_index + 1;
        let source_entry = load_source_entry(service, &source, next_source_index).await?;
        if source_entry
            .certificate
            .unsigned()
            .entry_core
            .previous_entry_hash
            != source_hash
        {
            return Err(Status::data_loss(
                "source certificate chain is not predecessor-linked",
            ));
        }
        let length = u64::try_from(source_entry.payload.len()).unwrap_or(u64::MAX);
        if delivered_bytes.saturating_add(length) > max_bytes {
            if commits.is_empty() {
                return Err(Status::resource_exhausted(
                    "the next projection entry exceeds max_bytes",
                ));
            }
            break;
        }
        let (commit, next_head, next_version) = commit_mirror_entry(
            service,
            &target,
            &target_manifest,
            &definition,
            source_authorization.revision.0,
            target_head_version,
            &target_head,
            source_entry,
            &request.command_id,
            fence,
        )
        .await?;
        source_index = next_source_index;
        source_hash = source_hash_at(service, &source, source_index).await?;
        target_head = next_head;
        target_head_version = next_version;
        delivered_bytes += length;
        commits.push(commit);
    }
    service.placement.require_unchanged(fence)?;
    Ok(PersonalDbMaterialization {
        commits,
        source_log_index: source_index,
        source_log_hash_sha256: source_hash.as_bytes().to_vec(),
    })
}

struct SourceEntry {
    certificate: CommitCertificateV2,
    certificate_bytes: Vec<u8>,
    payload: Vec<u8>,
}

async fn load_source_entry(
    service: &PersonalDbServiceImpl,
    source: &GroupScope,
    index: u64,
) -> Result<SourceEntry, Status> {
    let certificate_bytes = service
        .objects
        .read(source, &entry_certificate_path(index))
        .await?
        .bytes
        .ok_or_else(|| Status::data_loss("source commit certificate is missing"))?;
    let certificate =
        CommitCertificateV2::decode_canonical(&certificate_bytes).map_err(protocol_status)?;
    if certificate.unsigned().entry_core.log_index != index
        || certificate.unsigned().entry_core.database_id != source.database_id
    {
        return Err(Status::data_loss(
            "source commit certificate has the wrong identity",
        ));
    }
    let payload = service
        .objects
        .read(source, &entry_payload_path(index))
        .await?
        .bytes
        .ok_or_else(|| Status::data_loss("source changeset payload is missing"))?;
    if Sha256Digest::hash(&payload) != certificate.unsigned().entry_core.changeset_payload_hash {
        return Err(Status::data_loss(
            "source changeset payload does not match its certificate",
        ));
    }
    Ok(SourceEntry {
        certificate,
        certificate_bytes,
        payload,
    })
}

async fn source_hash_at(
    service: &PersonalDbServiceImpl,
    source: &GroupScope,
    index: u64,
) -> Result<Sha256Digest, Status> {
    if index == 0 {
        return Ok(Sha256Digest::ZERO);
    }
    let bytes = service
        .objects
        .read(source, &entry_certificate_path(index))
        .await?
        .bytes
        .ok_or_else(|| Status::data_loss("source commit certificate is missing"))?;
    let certificate = CommitCertificateV2::decode_canonical(&bytes).map_err(protocol_status)?;
    if certificate.unsigned().entry_core.log_index != index {
        return Err(Status::data_loss(
            "source certificate index is inconsistent",
        ));
    }
    certificate
        .unsigned()
        .entry_core
        .entry_hash()
        .map_err(protocol_status)
}

async fn target_source_checkpoint(
    service: &PersonalDbServiceImpl,
    target: &GroupScope,
    target_head: &personaldb_protocol::CommittedHeadV2,
    definition: &ProjectionDefinitionV1,
) -> Result<(u64, Sha256Digest), Status> {
    if target_head.state().log_index == 0 {
        return Ok((0, Sha256Digest::ZERO));
    }
    let bytes = service
        .objects
        .read(
            target,
            &entry_certificate_path(target_head.state().log_index),
        )
        .await?
        .bytes
        .ok_or_else(|| Status::data_loss("projection commit certificate is missing"))?;
    let certificate = CommitCertificateV2::decode_canonical(&bytes).map_err(protocol_status)?;
    target_head
        .verify_certificate(&certificate)
        .map_err(protocol_status)?;
    let derivation = certificate
        .unsigned()
        .signed_projection_derivation
        .as_deref()
        .ok_or_else(|| Status::data_loss("projection commit has no derivation evidence"))?;
    let derivation =
        SignedProjectionDerivationV1::decode_canonical(derivation).map_err(protocol_status)?;
    let [source] = derivation.derivation.ordered_source_heads.as_slice() else {
        return Err(Status::data_loss(
            "mirror projection derivation must contain exactly one source head",
        ));
    };
    if source.database_id != definition.source_database_id {
        return Err(Status::data_loss(
            "projection derivation names another source database",
        ));
    }
    Ok((source.log_index, source.log_hash))
}

#[allow(clippy::too_many_arguments)]
async fn commit_mirror_entry(
    service: &PersonalDbServiceImpl,
    target: &GroupScope,
    manifest: &super::model::GroupManifest,
    definition: &ProjectionDefinitionV1,
    authorization_revision: u64,
    head_version: u64,
    current_head: &personaldb_protocol::CommittedHeadV2,
    source: SourceEntry,
    command_id: &str,
    fence: PlacementLogId,
) -> Result<(PersonalDbCommit, personaldb_protocol::CommittedHeadV2, u64), Status> {
    let target_index = current_head
        .state()
        .log_index
        .checked_add(1)
        .ok_or_else(|| Status::out_of_range("projection log index overflowed"))?;
    let source_core = &source.certificate.unsigned().entry_core;
    let source_state = &source.certificate.unsigned().resulting_state;
    let projection_hash = definition.canonical_sha256().map_err(protocol_status)?;
    let payload_hash = Sha256Digest::hash(&source.payload);
    let core = LogEntryCoreV2 {
        database_id: target.database_id.clone(),
        group_kind: DatabaseGroupKind::Projection,
        log_index: target_index,
        previous_entry_hash: current_head.state().log_hash,
        changeset_payload_hash: payload_hash,
        client_proposal_hash: Sha256Digest::hash(&source.certificate_bytes),
        database_state_root: source_state.database_state_root,
        schema_hash: manifest.schema_hash(),
        projection_definition_hash: Some(projection_hash),
        membership_revision: source_core.membership_revision,
        placement_epoch: fence.index,
        client_log_epoch: source_core.client_log_epoch,
        proposal_admission_hash: Sha256Digest::hash(
            &definition.encode_deterministic().map_err(protocol_status)?,
        ),
    };
    let resulting_state = StateCommitmentV1 {
        database_id: target.database_id.clone(),
        log_index: target_index,
        log_hash: core.entry_hash().map_err(protocol_status)?,
        database_state_root: source_state.database_state_root,
        schema_hash: manifest.schema_hash(),
        projection_definition_hash: Some(projection_hash),
        group_kind: DatabaseGroupKind::Projection,
    };
    let source_head = SourceHeadV1 {
        database_id: source_core.database_id.clone(),
        log_index: source_core.log_index,
        log_hash: source_core.entry_hash().map_err(protocol_status)?,
    };
    let placement_key = target.placement_key();
    let target_index_bytes = target_index.to_be_bytes();
    let batch_seed = [
        source.certificate_bytes.as_slice(),
        placement_key.as_slice(),
        target_index_bytes.as_slice(),
    ]
    .concat();
    let derivation = SignedProjectionDerivationV1::sign(
        ProjectionDerivationV1 {
            projection_database_id: target.database_id.clone(),
            projection_definition_hash: projection_hash,
            policy_epoch: 1,
            authorization_revision,
            ordered_source_heads: vec![source_head],
            previous_projection_log_index: current_head.state().log_index,
            previous_projection_log_hash: current_head.state().log_hash,
            changeset_payload_hash: payload_hash,
            resulting_state: resulting_state.clone(),
            deterministic_batch_id: Sha256Digest::hash(&batch_seed).to_prefixed_hex(),
        },
        service.signers.projection_builder(),
    )
    .and_then(|value| value.encode_deterministic())
    .map_err(protocol_status)?;
    let certificate = UnsignedCommitCertificateV2 {
        entry_core: core,
        resulting_state: resulting_state.clone(),
        signed_client_proposal: source.certificate_bytes,
        signed_voter_acknowledgements: source
            .certificate
            .unsigned()
            .signed_voter_acknowledgements
            .clone(),
        signed_projection_derivation: Some(derivation),
        primary_server_id: super::model::primary_server_id(service.local_node),
        signed_proposal_admission: definition_bytes(definition)?,
    }
    .sign(&target.group_id, service.signers.witness())
    .map_err(protocol_status)?;
    let certificate_bytes = certificate
        .encode_deterministic()
        .map_err(protocol_status)?;
    let next_head = UnsignedCommittedHeadV2 {
        state: resulting_state,
        commit_certificate_hash: certificate.certificate_hash().map_err(protocol_status)?,
        primary_server_id: super::model::primary_server_id(service.local_node),
        placement_epoch: fence.index,
    }
    .sign(&target.group_id, service.signers.witness())
    .map_err(protocol_status)?;
    let head_bytes = next_head.encode_deterministic().map_err(protocol_status)?;
    service.placement.require_unchanged(fence)?;
    service
        .put_hidden_if_absent(
            target,
            &entry_payload_path(target_index),
            source.payload,
            command_id,
        )
        .await?;
    service
        .put_hidden_if_absent(
            target,
            &entry_certificate_path(target_index),
            certificate_bytes.clone(),
            command_id,
        )
        .await?;
    match service
        .objects
        .put_if_version(
            target,
            super::model::head_path(),
            head_bytes.clone(),
            head_version,
            storage_command_id(
                target,
                command_id,
                &format!("projection-head:{target_index}"),
            ),
        )
        .await?
    {
        ConditionalWrite::Applied => {
            let (next_version, stored_head, _) = service.load_head(target).await?;
            if stored_head.state() != next_head.state() {
                return Err(Status::data_loss(
                    "projection head readback differs from the committed head",
                ));
            }
            Ok((
                PersonalDbCommit {
                    commit_certificate: certificate_bytes,
                    committed_head: head_bytes,
                    replayed: false,
                },
                next_head,
                next_version,
            ))
        }
        ConditionalWrite::ConditionFailed => Err(Status::aborted(
            "projection head changed before materialization could commit",
        )),
    }
}

fn definition_bytes(definition: &ProjectionDefinitionV1) -> Result<Vec<u8>, Status> {
    definition.encode_deterministic().map_err(protocol_status)
}

fn effective_entries(value: u32) -> usize {
    usize::try_from(if value == 0 {
        MAX_SYNC_PAGE_ENTRIES
    } else {
        value.min(MAX_SYNC_PAGE_ENTRIES)
    })
    .expect("u32 fits usize on supported targets")
}

fn effective_bytes(value: u64) -> u64 {
    if value == 0 {
        MAX_SYNC_PAGE_BYTES
    } else {
        value.min(MAX_SYNC_PAGE_BYTES)
    }
}

fn validate_request(request: &MaterializePersonalDbProjectionRequest) -> Result<(), Status> {
    validate_command_id(&request.command_id)?;
    if request.through_source_log_index == 0
        && request.through_source_log_hash_sha256.as_slice() != Sha256Digest::ZERO.as_bytes()
    {
        return Err(Status::invalid_argument(
            "source log index zero requires the zero hash",
        ));
    }
    Ok(())
}
