use std::time::Duration;

use anvil_api::v1::{
    GetPersonalDbSnapshotRequest, PersonalDbCanonicalFrame, PersonalDbSnapshot,
    RegisterPersonalDbSnapshotRequest,
};
use anvil_store::PlacementLogId;
use personaldb_protocol::{
    MAX_SYNC_CHUNK_BYTES, MAX_SYNC_PAGE_BYTES, PersonalDbSnapshotFrameV1, Sha256Digest,
    SignedSnapshotTargetManifestV1, SnapshotChunkV1, SnapshotEndV1, SnapshotHeaderV1,
    SnapshotTargetManifestV1,
};
use tokio_stream::StreamExt;
use tonic::{Request, Status};

use super::authorization::GroupPermission;
use crate::authentication::Caller;
use crate::distributed_list::OriginalBearer;
use crate::v05::{deadline_remaining, request_deadline};

use super::model::{
    GroupScope, protocol_status, snapshot_bytes_path, snapshot_manifest_path, storage_command_id,
    validate_command_id, validate_id,
};
use super::service::{PersonalDbFrameStream, PersonalDbServiceImpl, authenticated_caller};

pub(super) async fn register(
    service: &PersonalDbServiceImpl,
    request: Request<RegisterPersonalDbSnapshotRequest>,
) -> Result<PersonalDbSnapshot, Status> {
    let deadline = request_deadline(request.metadata(), service.request_timeout)?;
    let caller = authenticated_caller(&request)?;
    let bearer = OriginalBearer::from_metadata(request.metadata())?;
    let request = request.into_inner();
    let scope = prepare_registration(service, caller, bearer, &request).await?;
    service
        .require_permission(&scope, GroupPermission::Write, "snapshot registration")
        .await?;
    route_or_execute(service, scope, request, deadline_remaining(deadline)?).await
}

pub(super) async fn execute_routed_registration(
    service: &PersonalDbServiceImpl,
    caller: Caller,
    bearer: OriginalBearer,
    request: RegisterPersonalDbSnapshotRequest,
    fence: PlacementLogId,
) -> Result<PersonalDbSnapshot, Status> {
    let scope = prepare_registration(service, caller, bearer, &request).await?;
    service
        .require_permission(&scope, GroupPermission::Write, "snapshot registration")
        .await?;
    service.placement.require_local_primary(&scope, fence)?;
    execute_local(service, scope, request, fence).await
}

async fn prepare_registration(
    service: &PersonalDbServiceImpl,
    caller: Caller,
    bearer: OriginalBearer,
    request: &RegisterPersonalDbSnapshotRequest,
) -> Result<GroupScope, Status> {
    validate_command_id(&request.command_id)?;
    if request.manifest.is_empty() || request.snapshot.is_empty() {
        return Err(Status::invalid_argument(
            "snapshot manifest and snapshot bytes must not be empty",
        ));
    }
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
    request: RegisterPersonalDbSnapshotRequest,
    remaining: Duration,
) -> Result<PersonalDbSnapshot, Status> {
    let primary = service.placement.primary(&scope)?;
    if let Some(address) = primary.address {
        return service
            .peers
            .route_register_personaldb_snapshot(
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
    scope: GroupScope,
    request: RegisterPersonalDbSnapshotRequest,
    fence: PlacementLogId,
) -> Result<PersonalDbSnapshot, Status> {
    service.placement.require_local_primary(&scope, fence)?;
    let lock = service.lock_index(&scope);
    let _guard = service.locks[lock].lock().await;
    service.placement.require_local_primary(&scope, fence)?;
    let group = service.require_manifest(&scope).await?;
    let manifest =
        SnapshotTargetManifestV1::decode_canonical(&request.manifest).map_err(protocol_status)?;
    validate_snapshot_manifest(service, &scope, &group, &manifest, &request.snapshot).await?;
    let snapshot_id = manifest.snapshot_id.clone();
    let signed = manifest
        .sign(service.signers.snapshot())
        .and_then(|value| value.encode_deterministic())
        .map_err(protocol_status)?;
    service.placement.require_unchanged(fence)?;
    service
        .put_hidden_if_absent(
            &scope,
            &snapshot_bytes_path(&snapshot_id),
            request.snapshot,
            &request.command_id,
        )
        .await?;
    let replayed = match service
        .objects
        .put_if_absent(
            &scope,
            &snapshot_manifest_path(&snapshot_id),
            signed.clone(),
            storage_command_id(&scope, &request.command_id, "snapshot-manifest"),
        )
        .await?
    {
        super::storage::ConditionalWrite::Applied => false,
        super::storage::ConditionalWrite::ConditionFailed => {
            let current = service
                .objects
                .read(&scope, &snapshot_manifest_path(&snapshot_id))
                .await?
                .bytes
                .ok_or_else(|| Status::data_loss("snapshot manifest disappeared"))?;
            if current != signed {
                return Err(Status::already_exists(
                    "snapshot ID already names another snapshot",
                ));
            }
            true
        }
    };
    service.placement.require_unchanged(fence)?;
    Ok(PersonalDbSnapshot {
        signed_manifest: signed,
        replayed,
    })
}

async fn validate_snapshot_manifest(
    service: &PersonalDbServiceImpl,
    scope: &GroupScope,
    group: &super::model::GroupManifest,
    manifest: &SnapshotTargetManifestV1,
    bytes: &[u8],
) -> Result<(), Status> {
    validate_id("snapshot_id", &manifest.snapshot_id)?;
    if manifest.group_id != scope.group_id
        || manifest.committed_head.state().database_id != scope.database_id
        || manifest.schema_hash != group.schema_hash()
        || manifest.projection_definition_hash != group.projection_hash()
    {
        return Err(Status::failed_precondition(
            "snapshot manifest does not match its addressed group",
        ));
    }
    let (_, current_head, _) = service.load_head(scope).await?;
    if current_head.state() != manifest.committed_head.state()
        || current_head.commit_certificate_hash()
            != manifest.committed_head.commit_certificate_hash()
    {
        return Err(Status::failed_precondition(
            "snapshot must target the current committed group head",
        ));
    }
    if manifest.compressed_length != u64::try_from(bytes.len()).unwrap_or(u64::MAX)
        || manifest.compressed_sha256 != Sha256Digest::hash(bytes)
    {
        return Err(Status::invalid_argument(
            "snapshot bytes do not match the manifest length and digest",
        ));
    }
    Ok(())
}

pub(super) async fn get(
    service: &PersonalDbServiceImpl,
    request: Request<GetPersonalDbSnapshotRequest>,
) -> Result<PersonalDbFrameStream, Status> {
    let caller = authenticated_caller(&request)?;
    let bearer = OriginalBearer::from_metadata(request.metadata())?;
    let request = request.into_inner();
    validate_id("snapshot_id", &request.snapshot_id)?;
    validate_id("request_id", &request.request_id)?;
    let scope = service
        .request_scope(
            caller,
            bearer,
            &request.bucket,
            &request.database_id,
            &request.group_id,
        )
        .await?;
    service
        .require_permission(&scope, GroupPermission::Read, "snapshot read")
        .await?;
    service.require_manifest(&scope).await?;
    let signed_bytes = service
        .objects
        .read(&scope, &snapshot_manifest_path(&request.snapshot_id))
        .await?
        .bytes
        .ok_or_else(|| Status::not_found("PersonalDB snapshot does not exist"))?;
    let signed =
        SignedSnapshotTargetManifestV1::decode_canonical(&signed_bytes).map_err(protocol_status)?;
    let snapshot = service
        .objects
        .read(&scope, &snapshot_bytes_path(&request.snapshot_id))
        .await?
        .bytes
        .ok_or_else(|| Status::data_loss("PersonalDB snapshot bytes are missing"))?;
    if Sha256Digest::hash(&snapshot) != signed.manifest.compressed_sha256
        || u64::try_from(snapshot.len()).unwrap_or(u64::MAX) != signed.manifest.compressed_length
    {
        return Err(Status::data_loss(
            "PersonalDB snapshot bytes do not match the signed manifest",
        ));
    }
    let start = usize::try_from(request.start_offset)
        .map_err(|_| Status::out_of_range("snapshot start offset does not fit this node"))?;
    if start > snapshot.len() {
        return Err(Status::out_of_range(
            "snapshot start offset is beyond the object",
        ));
    }
    let maximum = if request.max_bytes == 0 {
        MAX_SYNC_PAGE_BYTES
    } else {
        request.max_bytes.min(MAX_SYNC_PAGE_BYTES)
    };
    let end = start
        .saturating_add(usize::try_from(maximum).unwrap_or(usize::MAX))
        .min(snapshot.len());
    let chunk_size = usize::try_from(signed.manifest.chunk_size)
        .unwrap_or(MAX_SYNC_CHUNK_BYTES)
        .clamp(1, MAX_SYNC_CHUNK_BYTES);
    let mut frames = Vec::new();
    frames.push(frame(PersonalDbSnapshotFrameV1::Header(Box::new(
        SnapshotHeaderV1 {
            request_id: request.request_id,
            signed_manifest: signed,
            start_offset: request.start_offset,
            end_offset_exclusive: u64::try_from(end).unwrap_or(u64::MAX),
            trust_bundle_version: super::model::TRUST_BUNDLE_VERSION,
        },
    )))?);
    let delivered = &snapshot[start..end];
    for (chunk_number, data) in delivered.chunks(chunk_size).enumerate() {
        let offset = start.saturating_add(chunk_number.saturating_mul(chunk_size));
        frames.push(frame(PersonalDbSnapshotFrameV1::Chunk(SnapshotChunkV1 {
            offset: u64::try_from(offset).unwrap_or(u64::MAX),
            data: data.to_vec(),
            chunk_sha256: Sha256Digest::hash(data),
        }))?);
    }
    frames.push(frame(PersonalDbSnapshotFrameV1::End(SnapshotEndV1 {
        delivered_length: u64::try_from(delivered.len()).unwrap_or(u64::MAX),
        delivered_sha256: Sha256Digest::hash(delivered),
        next_offset: u64::try_from(end).unwrap_or(u64::MAX),
        complete: end == snapshot.len(),
    }))?);
    Ok(Box::pin(tokio_stream::iter(frames).map(Ok)))
}

fn frame(value: PersonalDbSnapshotFrameV1) -> Result<PersonalDbCanonicalFrame, Status> {
    Ok(PersonalDbCanonicalFrame {
        value: value.encode_deterministic().map_err(protocol_status)?,
    })
}
