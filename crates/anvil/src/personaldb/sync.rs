use anvil_api::v1::{PersonalDbCanonicalFrame, PersonalDbCatchUpRequest};
use personaldb_protocol::{
    CommitCertificateV2, CommittedHeadV2, MAX_SYNC_CHUNK_BYTES, MAX_SYNC_PAGE_BYTES,
    MAX_SYNC_PAGE_ENTRIES, PersonalDbSyncFrameV1, Sha256Digest, SyncEndV1, SyncEntryChunkV1,
    SyncEntryEndV1, SyncEntryStartV1, SyncHeaderV1, UnsignedCommittedHeadV2,
};
use tokio_stream::StreamExt;
use tonic::{Request, Status};

use super::authorization::GroupPermission;
use crate::distributed_list::OriginalBearer;

use super::model::{
    digest, entry_certificate_path, entry_payload_path, protocol_status, validate_id,
};
use super::service::{PersonalDbFrameStream, PersonalDbServiceImpl, authenticated_caller};
use super::traffic::{CompletedPayload, record_payloads_when_stream_completes};

pub(super) async fn catch_up(
    service: &PersonalDbServiceImpl,
    request: Request<PersonalDbCatchUpRequest>,
) -> Result<PersonalDbFrameStream, Status> {
    let caller = authenticated_caller(&request)?;
    let bearer = OriginalBearer::from_metadata(request.metadata())?;
    let request = request.into_inner();
    validate_id("request_id", &request.request_id)?;
    validate_id("projection_profile_id", &request.projection_profile_id)?;
    let from_hash = digest("from_log_hash_sha256", &request.from_log_hash_sha256)?;
    let expected_schema = digest(
        "expected_schema_hash_sha256",
        &request.expected_schema_hash_sha256,
    )?;
    let expected_projection = request
        .expected_projection_definition_hash_sha256
        .as_deref()
        .map(|value| digest("expected_projection_definition_hash_sha256", value))
        .transpose()?;
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
        .require_permission(&scope, GroupPermission::Read, "catch-up read")
        .await?;
    let manifest = service.require_manifest(&scope).await?;
    if expected_schema != manifest.schema_hash()
        || expected_projection != manifest.projection_hash()
    {
        return Err(Status::failed_precondition(
            "catch-up schema or projection definition differs from the group",
        ));
    }
    let (_, advertised_head, _) = service.load_head(&scope).await?;
    if request.from_log_index > advertised_head.state().log_index {
        return Err(Status::failed_precondition(
            "catch-up predecessor is newer than the committed group head",
        ));
    }
    let mut resulting_head = head_at(service, &scope, request.from_log_index).await?;
    if resulting_head.state().log_hash != from_hash {
        return Err(Status::failed_precondition(
            "catch-up predecessor hash does not match its committed index",
        ));
    }

    let mut frames = Vec::new();
    frames.push(frame(PersonalDbSyncFrameV1::Header(Box::new(
        SyncHeaderV1 {
            request_id: request.request_id,
            projection_profile_id: request.projection_profile_id,
            group_id: scope.group_id.clone(),
            from_log_index: request.from_log_index,
            from_log_hash: from_hash,
            advertised_head: advertised_head.clone(),
            schema_hash: manifest.schema_hash(),
            projection_definition_hash: manifest.projection_hash(),
            trust_bundle_version: manifest.trust_bundle_version,
        },
    )))?);
    let max_entries = usize::try_from(if request.max_entries == 0 {
        MAX_SYNC_PAGE_ENTRIES
    } else {
        request.max_entries.min(MAX_SYNC_PAGE_ENTRIES)
    })
    .expect("u32 fits usize on supported targets");
    let max_bytes = if request.max_bytes == 0 {
        MAX_SYNC_PAGE_BYTES
    } else {
        request.max_bytes.min(MAX_SYNC_PAGE_BYTES)
    };
    let mut delivered_entries = 0_u32;
    let mut delivered_bytes = 0_u64;
    let mut completed_payloads = Vec::new();
    let mut index = request.from_log_index;
    while index < advertised_head.state().log_index
        && usize::try_from(delivered_entries).unwrap_or(usize::MAX) < max_entries
    {
        let next = index + 1;
        let certificate_bytes = service
            .objects
            .read(&scope, &entry_certificate_path(next))
            .await?
            .bytes
            .ok_or_else(|| Status::data_loss("catch-up commit certificate is missing"))?;
        let certificate =
            CommitCertificateV2::decode_canonical(&certificate_bytes).map_err(protocol_status)?;
        let payload = service
            .objects
            .read(&scope, &entry_payload_path(next))
            .await?
            .bytes
            .ok_or_else(|| Status::data_loss("catch-up changeset payload is missing"))?;
        let payload_length = u64::try_from(payload.len()).unwrap_or(u64::MAX);
        if delivered_bytes.saturating_add(payload_length) > max_bytes {
            break;
        }
        let core = &certificate.unsigned().entry_core;
        if core.log_index != next
            || core.database_id != scope.database_id
            || core.previous_entry_hash != resulting_head.state().log_hash
            || core.changeset_payload_hash != Sha256Digest::hash(&payload)
        {
            return Err(Status::data_loss(
                "catch-up entry is not linked to its predecessor",
            ));
        }
        let entry_id = format!("{}:{next}", scope.group_id);
        frames.push(frame(PersonalDbSyncFrameV1::EntryStart(Box::new(
            SyncEntryStartV1 {
                entry_id: entry_id.clone(),
                changeset_length: payload_length,
                changeset_sha256: core.changeset_payload_hash,
                commit_certificate: certificate.clone(),
            },
        )))?);
        for (chunk_number, bytes) in payload.chunks(MAX_SYNC_CHUNK_BYTES).enumerate() {
            frames.push(frame(PersonalDbSyncFrameV1::EntryChunk(
                SyncEntryChunkV1 {
                    entry_id: entry_id.clone(),
                    offset: u64::try_from(chunk_number.saturating_mul(MAX_SYNC_CHUNK_BYTES))
                        .unwrap_or(u64::MAX),
                    data: bytes.to_vec(),
                    chunk_sha256: Sha256Digest::hash(bytes),
                },
            ))?);
        }
        resulting_head = head_for_certificate(service, &scope.group_id, &certificate)?;
        frames.push(frame(PersonalDbSyncFrameV1::EntryEnd(Box::new(
            SyncEntryEndV1 {
                entry_id,
                delivered_length: payload_length,
                delivered_sha256: core.changeset_payload_hash,
                committed_head: resulting_head.clone(),
            },
        )))?);
        completed_payloads.push(CompletedPayload {
            path: scope
                .storage_key(&entry_payload_path(next))?
                .path()
                .to_owned(),
            bytes: payload_length,
        });
        delivered_entries += 1;
        delivered_bytes += payload_length;
        index = next;
    }
    frames.push(frame(PersonalDbSyncFrameV1::End(Box::new(SyncEndV1 {
        delivered_entry_count: delivered_entries,
        delivered_byte_count: delivered_bytes,
        resulting_head,
    })))?);
    let objects = service.objects.clone();
    let tenant_id = scope.tenant_id;
    let bucket_id = scope.bucket_id;
    Ok(record_payloads_when_stream_completes(
        Box::pin(tokio_stream::iter(frames).map(Ok)),
        completed_payloads,
        move |payload| {
            objects.record_gateway_egress(tenant_id, bucket_id, &payload.path, payload.bytes);
        },
    ))
}

async fn head_at(
    service: &PersonalDbServiceImpl,
    scope: &super::model::GroupScope,
    index: u64,
) -> Result<CommittedHeadV2, Status> {
    if index == 0 {
        let (_, current, _) = service.load_head(scope).await?;
        if current.state().log_index == 0 {
            return Ok(current);
        }
        let manifest = service.require_manifest(scope).await?;
        return UnsignedCommittedHeadV2 {
            state: personaldb_protocol::StateCommitmentV1 {
                database_id: scope.database_id.clone(),
                log_index: 0,
                log_hash: Sha256Digest::ZERO,
                database_state_root: Sha256Digest::ZERO,
                schema_hash: manifest.schema_hash(),
                projection_definition_hash: manifest.projection_hash(),
                group_kind: manifest.kind(),
            },
            commit_certificate_hash: Sha256Digest::ZERO,
            primary_server_id: super::model::primary_server_id(service.local_node),
            placement_epoch: current.placement_epoch(),
        }
        .sign(&scope.group_id, service.signers.witness())
        .map_err(protocol_status);
    }
    let bytes = service
        .objects
        .read(scope, &entry_certificate_path(index))
        .await?
        .bytes
        .ok_or_else(|| Status::data_loss("catch-up predecessor certificate is missing"))?;
    let certificate = CommitCertificateV2::decode_canonical(&bytes).map_err(protocol_status)?;
    head_for_certificate(service, &scope.group_id, &certificate)
}

fn head_for_certificate(
    service: &PersonalDbServiceImpl,
    group_id: &str,
    certificate: &CommitCertificateV2,
) -> Result<CommittedHeadV2, Status> {
    UnsignedCommittedHeadV2 {
        state: certificate.unsigned().resulting_state.clone(),
        commit_certificate_hash: certificate.certificate_hash().map_err(protocol_status)?,
        primary_server_id: certificate.unsigned().primary_server_id.clone(),
        placement_epoch: certificate.unsigned().entry_core.placement_epoch,
    }
    .sign(group_id, service.signers.witness())
    .map_err(protocol_status)
}

fn frame(value: PersonalDbSyncFrameV1) -> Result<PersonalDbCanonicalFrame, Status> {
    Ok(PersonalDbCanonicalFrame {
        value: value.encode_deterministic().map_err(protocol_status)?,
    })
}
