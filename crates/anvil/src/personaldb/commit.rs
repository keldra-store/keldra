use std::time::Duration;

use anvil_api::v1::{AppendPersonalDbEntryRequest, PersonalDbCommit};
use anvil_store::PlacementLogId;
use personaldb_protocol::{
    CommitCertificateV2, CommittedHeadV2, DatabaseGroupKind, LogEntryCoreV2, MAX_SYNC_ENTRY_BYTES,
    Sha256Digest, StateCommitmentV1, UnsignedCommitCertificateV2, UnsignedCommittedHeadV2,
};
use tonic::{Request, Status};

use super::authorization::GroupPermission;
use crate::authentication::Caller;
use crate::distributed_list::OriginalBearer;
use crate::v05::{deadline_remaining, request_deadline};

use super::model::{
    GroupScope, digest, entry_certificate_path, entry_payload_path, protocol_status,
    storage_command_id, validate_command_id,
};
use super::service::{PersonalDbServiceImpl, authenticated_caller};
use super::storage::ConditionalWrite;

pub(super) async fn append(
    service: &PersonalDbServiceImpl,
    request: Request<AppendPersonalDbEntryRequest>,
) -> Result<PersonalDbCommit, Status> {
    let deadline = request_deadline(request.metadata(), service.request_timeout)?;
    let caller = authenticated_caller(&request)?;
    let bearer = OriginalBearer::from_metadata(request.metadata())?;
    let request = request.into_inner();
    let scope = prepare(service, caller, bearer, &request).await?;
    service
        .require_permission(&scope, GroupPermission::Write, "append")
        .await?;
    route_or_execute(service, scope, request, deadline_remaining(deadline)?).await
}

pub(super) async fn execute_routed_append(
    service: &PersonalDbServiceImpl,
    caller: Caller,
    bearer: OriginalBearer,
    request: AppendPersonalDbEntryRequest,
    fence: PlacementLogId,
) -> Result<PersonalDbCommit, Status> {
    let scope = prepare(service, caller, bearer, &request).await?;
    service
        .require_permission(&scope, GroupPermission::Write, "append")
        .await?;
    service.placement.require_local_primary(&scope, fence)?;
    execute_local(service, scope, request, fence).await
}

async fn prepare(
    service: &PersonalDbServiceImpl,
    caller: Caller,
    bearer: OriginalBearer,
    request: &AppendPersonalDbEntryRequest,
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
    request: AppendPersonalDbEntryRequest,
    remaining: Duration,
) -> Result<PersonalDbCommit, Status> {
    let primary = service.placement.primary(&scope)?;
    if let Some(address) = primary.address {
        return service
            .peers
            .route_append_personaldb_entry(
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

pub(super) async fn execute_local(
    service: &PersonalDbServiceImpl,
    scope: GroupScope,
    request: AppendPersonalDbEntryRequest,
    fence: PlacementLogId,
) -> Result<PersonalDbCommit, Status> {
    service.placement.require_local_primary(&scope, fence)?;
    let lock = service.lock_index(&scope);
    let _guard = service.locks[lock].lock().await;
    service.placement.require_local_primary(&scope, fence)?;
    let manifest = service.require_manifest(&scope).await?;
    if manifest.kind() == DatabaseGroupKind::Projection {
        return Err(Status::failed_precondition(
            "projection groups are written only by MaterializeProjection",
        ));
    }
    let expected_hash = digest(
        "expected_log_hash_sha256",
        &request.expected_log_hash_sha256,
    )?;
    let proposal_hash = digest(
        "client_proposal_hash_sha256",
        &request.client_proposal_hash_sha256,
    )?;
    let state_root = digest(
        "database_state_root_sha256",
        &request.database_state_root_sha256,
    )?;
    let schema_hash = digest("schema_hash_sha256", &request.schema_hash_sha256)?;
    if schema_hash != manifest.schema_hash() {
        return Err(Status::failed_precondition(
            "append schema hash differs from the immutable group schema",
        ));
    }

    let (head_version, current, _) = service.load_head(&scope).await?;
    if let Some(replayed) = replay_if_committed(
        service,
        &scope,
        &current,
        request.expected_log_index,
        expected_hash,
        proposal_hash,
        Sha256Digest::hash(&request.changeset),
    )
    .await?
    {
        return Ok(replayed);
    }
    if current.state().log_index != request.expected_log_index
        || current.state().log_hash != expected_hash
    {
        return Err(Status::failed_precondition(
            "PersonalDB committed head differs from the expected predecessor",
        ));
    }
    let log_index = request
        .expected_log_index
        .checked_add(1)
        .ok_or_else(|| Status::out_of_range("PersonalDB log index overflowed"))?;
    let payload_hash = Sha256Digest::hash(&request.changeset);
    let core = LogEntryCoreV2 {
        database_id: scope.database_id.clone(),
        group_kind: manifest.kind(),
        log_index,
        previous_entry_hash: expected_hash,
        changeset_payload_hash: payload_hash,
        client_proposal_hash: proposal_hash,
        database_state_root: state_root,
        schema_hash,
        projection_definition_hash: None,
        membership_revision: request.membership_revision,
        placement_epoch: fence.index,
        client_log_epoch: request.client_log_epoch,
        proposal_admission_hash: Sha256Digest::hash(&request.signed_proposal_admission),
    };
    let resulting_state = StateCommitmentV1 {
        database_id: scope.database_id.clone(),
        log_index,
        log_hash: core.entry_hash().map_err(protocol_status)?,
        database_state_root: state_root,
        schema_hash,
        projection_definition_hash: None,
        group_kind: manifest.kind(),
    };
    let certificate = UnsignedCommitCertificateV2 {
        entry_core: core,
        resulting_state: resulting_state.clone(),
        signed_client_proposal: request.signed_client_proposal,
        signed_voter_acknowledgements: request.signed_voter_acknowledgements,
        signed_projection_derivation: None,
        primary_server_id: super::model::primary_server_id(service.local_node),
        signed_proposal_admission: request.signed_proposal_admission,
    }
    .sign(&scope.group_id, service.signers.witness())
    .map_err(protocol_status)?;
    let certificate_bytes = certificate
        .encode_deterministic()
        .map_err(protocol_status)?;
    let committed_head = UnsignedCommittedHeadV2 {
        state: resulting_state,
        commit_certificate_hash: certificate.certificate_hash().map_err(protocol_status)?,
        primary_server_id: super::model::primary_server_id(service.local_node),
        placement_epoch: fence.index,
    }
    .sign(&scope.group_id, service.signers.witness())
    .map_err(protocol_status)?;
    let head_bytes = committed_head
        .encode_deterministic()
        .map_err(protocol_status)?;

    service.placement.require_unchanged(fence)?;
    service
        .put_hidden_if_absent(
            &scope,
            &entry_payload_path(log_index),
            request.changeset,
            &request.command_id,
        )
        .await?;
    service
        .put_hidden_if_absent(
            &scope,
            &entry_certificate_path(log_index),
            certificate_bytes.clone(),
            &request.command_id,
        )
        .await?;
    service.placement.require_unchanged(fence)?;
    match service
        .objects
        .put_if_version(
            &scope,
            super::model::head_path(),
            head_bytes.clone(),
            head_version,
            storage_command_id(&scope, &request.command_id, "commit-head"),
        )
        .await?
    {
        ConditionalWrite::Applied => {
            service.placement.require_unchanged(fence)?;
            Ok(PersonalDbCommit {
                commit_certificate: certificate_bytes,
                committed_head: head_bytes,
                replayed: false,
            })
        }
        ConditionalWrite::ConditionFailed => {
            let (_, current, _) = service.load_head(&scope).await?;
            replay_if_committed(
                service,
                &scope,
                &current,
                request.expected_log_index,
                expected_hash,
                proposal_hash,
                payload_hash,
            )
            .await?
            .ok_or_else(|| {
                Status::aborted("PersonalDB head changed before the append could commit")
            })
        }
    }
}

async fn replay_if_committed(
    service: &PersonalDbServiceImpl,
    scope: &GroupScope,
    current: &CommittedHeadV2,
    expected_index: u64,
    expected_hash: Sha256Digest,
    proposal_hash: Sha256Digest,
    payload_hash: Sha256Digest,
) -> Result<Option<PersonalDbCommit>, Status> {
    if current.state().log_index != expected_index.saturating_add(1)
        || current.state().log_index == 0
    {
        return Ok(None);
    }
    let certificate = service
        .objects
        .read(scope, &entry_certificate_path(current.state().log_index))
        .await?
        .bytes
        .ok_or_else(|| Status::data_loss("PersonalDB commit certificate is missing"))?;
    let decoded = CommitCertificateV2::decode_canonical(&certificate).map_err(protocol_status)?;
    let core = &decoded.unsigned().entry_core;
    if core.previous_entry_hash != expected_hash
        || core.client_proposal_hash != proposal_hash
        || core.changeset_payload_hash != payload_hash
    {
        return Ok(None);
    }
    current
        .verify_certificate(&decoded)
        .map_err(protocol_status)?;
    Ok(Some(PersonalDbCommit {
        commit_certificate: certificate,
        committed_head: current.encode_deterministic().map_err(protocol_status)?,
        replayed: true,
    }))
}

fn validate_request(request: &AppendPersonalDbEntryRequest) -> Result<(), Status> {
    validate_command_id(&request.command_id)?;
    if request.changeset.is_empty()
        || u64::try_from(request.changeset.len()).unwrap_or(u64::MAX) > MAX_SYNC_ENTRY_BYTES
    {
        return Err(Status::invalid_argument(format!(
            "changeset must contain 1..={MAX_SYNC_ENTRY_BYTES} bytes"
        )));
    }
    if request.signed_client_proposal.is_empty()
        || request.signed_voter_acknowledgements.is_empty()
        || request
            .signed_voter_acknowledgements
            .iter()
            .any(Vec::is_empty)
        || request.signed_proposal_admission.is_empty()
    {
        return Err(Status::invalid_argument(
            "append requires client proposal, voter and admission evidence",
        ));
    }
    Ok(())
}
