//! Bounded typed state-transfer RPC implementations used only during a
//! membership handoff. No method exposes raw RocksDB keys or values.

use std::io::{self, Read, Write};

use anvil_store::{
    AuthzRealmCursor, AuthzRealmSnapshotError, AuthzRealmTransferManifest, AuthzSchemaCatalogue,
    AuthzScope, AuthzStoreError, LogicalRecordCandidate, LogicalRecordCursor, LogicalRecordError,
    LogicalRecordExport, LogicalRecordId, ObjectRecordCursor, ObjectRecordExport,
    PayloadArtifactCursor, PayloadArtifactSnapshot, ReferenceDeltaBatch, SourceId, StorageTenantId,
};

use super::*;

const HANDOFF_PAGE_BYTES: u64 = 4 * 1024 * 1024;

pub(super) async fn read_object_path_snapshot(
    service: &DataPeerService,
    mut request: Request<wire::HandoffObjectPathSnapshotRequest>,
) -> Result<Response<wire::ObjectPathSnapshotResponse>, Status> {
    let peer = request.get_ref().peer.clone();
    let caller = service.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
    service.validate_handoff(
        caller,
        request.get_ref().handoff.as_ref(),
        HandoffTarget::AnyNode,
    )?;
    let request = request.into_inner();
    let store = service.store.clone();
    let snapshot = tokio::task::spawn_blocking(move || {
        store.export_object_path_record(request.tenant_id, request.bucket_id, &request.exact_path)
    })
    .await
    .map_err(join_status)?
    .map_err(map_object_snapshot_error)?;
    Ok(Response::new(wire::ObjectPathSnapshotResponse {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        snapshot_json: encode_object_snapshot(&snapshot)?,
    }))
}

pub(super) async fn repair_object_path_snapshot(
    service: &DataPeerService,
    mut request: Request<wire::RepairHandoffObjectPathSnapshotRequest>,
) -> Result<Response<wire::ObjectPathSnapshotApplied>, Status> {
    let peer = request.get_ref().peer.clone();
    let caller = service.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
    let handoff = request.get_ref().handoff.clone();
    service.validate_handoff(caller, handoff.as_ref(), HandoffTarget::JoiningNode)?;
    require_object_snapshot_bound(&request.get_ref().expected_snapshot_json)?;
    require_object_snapshot_bound(&request.get_ref().selected_snapshot_json)?;
    let expected: Option<ObjectPathSnapshot> =
        decode_typed(&request.get_ref().expected_snapshot_json)?;
    let selected: Option<ObjectPathSnapshot> =
        decode_typed(&request.get_ref().selected_snapshot_json)?;
    let request = request.into_inner();
    service.validate_handoff(caller, handoff.as_ref(), HandoffTarget::JoiningNode)?;
    let applied = service
        .store
        .repair_object_path_snapshot(
            request.tenant_id,
            request.bucket_id,
            &request.exact_path,
            expected.as_ref(),
            selected.as_ref(),
        )
        .await
        .map_err(map_object_snapshot_error)?;
    Ok(Response::new(wire::ObjectPathSnapshotApplied {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        present: applied.retained,
        version: applied.version.map_or(0, |version| version.0),
        replayed: applied.replayed,
    }))
}

pub(super) async fn source_journal_status(
    service: &DataPeerService,
    mut request: Request<wire::HandoffSourceJournalStatusRequest>,
) -> Result<Response<wire::SourceJournalStatus>, Status> {
    let peer = request.get_ref().peer.clone();
    let caller = service.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
    service.validate_handoff(
        caller,
        request.get_ref().handoff.as_ref(),
        HandoffTarget::AnyNode,
    )?;
    let store = service.store.clone();
    let status = tokio::task::spawn_blocking(move || store.local_watch_status())
        .await
        .map_err(join_status)?
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    Ok(Response::new(wire::SourceJournalStatus {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        source_id_json: encode_typed(&status.source_id)?,
        tail: status.tail,
        retention_floor: status.retention_floor,
        retained_entries: status.retained_entries,
        retained_bytes: status.retained_bytes,
    }))
}

pub(super) async fn complete_system_bootstrap(
    service: &DataPeerService,
    mut request: Request<wire::CompleteSystemBootstrapHandoffRequest>,
) -> Result<Response<wire::HandoffRecordApplied>, Status> {
    let peer = request.get_ref().peer.clone();
    let caller = service.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
    let handoff = request.get_ref().handoff.clone();
    service.validate_handoff(caller, handoff.as_ref(), HandoffTarget::JoiningNode)?;
    let store = service.store.clone();
    let replayed = tokio::task::spawn_blocking(move || store.complete_system_bootstrap_handoff())
        .await
        .map_err(join_status)?
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    applied_response(replayed)
}

pub(super) async fn read_source_journal(
    service: &DataPeerService,
    mut request: Request<wire::HandoffSourceJournalReadRequest>,
) -> Result<Response<wire::SourceJournalPage>, Status> {
    let peer = request.get_ref().peer.clone();
    let caller = service.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
    service.validate_handoff(
        caller,
        request.get_ref().handoff.as_ref(),
        HandoffTarget::AnyNode,
    )?;
    let after = request.get_ref().after_offset;
    let limit = usize::try_from(request.get_ref().limit)
        .unwrap_or(usize::MAX)
        .min(MAX_LOCAL_INVALIDATION_SCAN_RECORDS);
    let max_bytes = request.get_ref().max_bytes;
    source_journal::require_page_bound(max_bytes)?;
    let store = service.store.clone();
    let page = tokio::task::spawn_blocking(move || {
        store.scan_local_changes_bounded(after, limit, max_bytes)
    })
    .await
    .map_err(join_status)?
    .map_err(map_mutation_error)?;
    source_journal::encode_page_response(page)
}

pub(super) async fn reference_cursor(
    service: &DataPeerService,
    mut request: Request<wire::HandoffReferenceCursorRequest>,
) -> Result<Response<wire::ReferenceDeltaStatus>, Status> {
    let peer = request.get_ref().peer.clone();
    let caller = service.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
    service.validate_handoff(
        caller,
        request.get_ref().handoff.as_ref(),
        HandoffTarget::AnyNode,
    )?;
    let source: SourceId = decode_typed(&request.get_ref().source_id_json)?;
    let store = service.store.clone();
    let through = tokio::task::spawn_blocking(move || store.reference_delta_cursor(source))
        .await
        .map_err(join_status)?
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    Ok(Response::new(wire::ReferenceDeltaStatus {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        through,
    }))
}

pub(super) async fn advance_reference_cursor(
    service: &DataPeerService,
    mut request: Request<wire::HandoffReferenceCursorAdvanceRequest>,
) -> Result<Response<wire::ReferenceDeltaApplied>, Status> {
    let peer = request.get_ref().peer.clone();
    let caller = service.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
    let handoff = request.get_ref().handoff.clone();
    service.validate_handoff(caller, handoff.as_ref(), HandoffTarget::JoiningNode)?;
    let source: SourceId = decode_typed(&request.get_ref().source_id_json)?;
    let through = request.get_ref().through;
    let store = service.store.clone();
    let current = tokio::task::spawn_blocking(move || store.reference_delta_cursor(source))
        .await
        .map_err(join_status)?
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    if current > through {
        return Err(Status::data_loss(
            "JOINING reference cursor is ahead of the handoff tail",
        ));
    }
    service.validate_handoff(caller, handoff.as_ref(), HandoffTarget::JoiningNode)?;
    let applied = service
        .store
        .apply_reference_deltas(ReferenceDeltaBatch {
            source,
            after: current,
            through,
            deltas: Vec::new(),
        })
        .await
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    Ok(Response::new(wire::ReferenceDeltaApplied {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        through: applied.through,
        replayed: applied.replayed,
    }))
}

pub(super) async fn export_object_records(
    service: &DataPeerService,
    mut request: Request<wire::HandoffPageRequest>,
) -> Result<Response<wire::HandoffPage>, Status> {
    authorize_page(service, &mut request)?;
    let request = request.into_inner();
    validate_page_limits(&request)?;
    let cursor = decode_cursor::<ObjectRecordCursor>(&request.cursor_json)?;
    let store = service.store.clone();
    let page = tokio::task::spawn_blocking(move || {
        store.export_object_records(cursor.as_ref(), request.max_records, request.max_bytes)
    })
    .await
    .map_err(join_status)?
    .map_err(|error| Status::failed_precondition(error.to_string()))?;
    page_response(&page)
}

pub(super) async fn install_object_record(
    service: &DataPeerService,
    mut request: Request<wire::HandoffRecordRequest>,
) -> Result<Response<wire::HandoffRecordApplied>, Status> {
    let caller = authorize_record(service, &mut request, HandoffTarget::JoiningNode)?;
    let scope = request.get_ref().handoff.clone();
    require_typed_bound(&request.get_ref().record_json)?;
    let record: ObjectRecordExport = decode_typed(&request.get_ref().record_json)?;
    service.validate_handoff(caller, scope.as_ref(), HandoffTarget::JoiningNode)?;
    let applied = match &record {
        ObjectRecordExport::ExactPath(selected) => {
            let current = service
                .store
                .export_object_path_record(
                    selected.tenant_id,
                    selected.bucket_id,
                    &selected.exact_path,
                )
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            service
                .store
                .repair_object_path_snapshot(
                    selected.tenant_id,
                    selected.bucket_id,
                    &selected.exact_path,
                    current.as_ref(),
                    Some(selected),
                )
                .await
                .map_err(|error| Status::failed_precondition(error.to_string()))?
        }
        ObjectRecordExport::Receipt(_) => service
            .store
            .install_quorum_reconciled_object_record(&record)
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?,
    };
    applied_response(applied.replayed)
}

pub(super) async fn export_logical_records(
    service: &DataPeerService,
    mut request: Request<wire::HandoffPageRequest>,
) -> Result<Response<wire::HandoffPage>, Status> {
    authorize_page(service, &mut request)?;
    let request = request.into_inner();
    validate_page_limits(&request)?;
    let cursor = decode_cursor::<LogicalRecordCursor>(&request.cursor_json)?;
    let store = service.store.clone();
    let page = tokio::task::spawn_blocking(move || {
        store.export_logical_records(cursor.as_ref(), request.max_records, request.max_bytes)
    })
    .await
    .map_err(join_status)?
    .map_err(logical_status)?;
    page_response(&page)
}

pub(super) async fn install_logical_record(
    service: &DataPeerService,
    mut request: Request<wire::HandoffRecordRequest>,
) -> Result<Response<wire::HandoffRecordApplied>, Status> {
    let caller = authorize_record(service, &mut request, HandoffTarget::JoiningNode)?;
    let scope = request.get_ref().handoff.clone();
    require_typed_bound(&request.get_ref().record_json)?;
    let record: LogicalRecordExport = decode_typed(&request.get_ref().record_json)?;
    service.validate_handoff(caller, scope.as_ref(), HandoffTarget::JoiningNode)?;
    let store = service.store.clone();
    let applied = tokio::task::spawn_blocking(move || {
        store.repair_quorum_reconciled_logical_record(&record.id, Some(&record.candidate))
    })
    .await
    .map_err(join_status)?
    .map_err(logical_status)?;
    applied_response(applied.replayed)
}

pub(super) async fn read_logical_record(
    service: &DataPeerService,
    mut request: Request<wire::LogicalRecordRequest>,
) -> Result<Response<wire::LogicalRecordResponse>, Status> {
    authorize_logical(service, &mut request, HandoffTarget::AnyNode)?;
    require_typed_bound(&request.get_ref().id_json)?;
    let id: LogicalRecordId = decode_typed(&request.get_ref().id_json)?;
    let store = service.store.clone();
    let candidate = tokio::task::spawn_blocking(move || store.logical_record_candidate(&id))
        .await
        .map_err(join_status)?
        .map_err(logical_status)?;
    Ok(Response::new(wire::LogicalRecordResponse {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        present: candidate.is_some(),
        candidate_json: candidate
            .as_ref()
            .map(encode_typed)
            .transpose()?
            .unwrap_or_default(),
    }))
}

pub(super) async fn repair_logical_record(
    service: &DataPeerService,
    mut request: Request<wire::RepairLogicalRecordRequest>,
) -> Result<Response<wire::HandoffRecordApplied>, Status> {
    let caller = authorize_repair_logical(service, &mut request, HandoffTarget::JoiningNode)?;
    let scope = request.get_ref().handoff.clone();
    require_typed_bound(&request.get_ref().id_json)?;
    let id: LogicalRecordId = decode_typed(&request.get_ref().id_json)?;
    let candidate: Option<LogicalRecordCandidate> = match (
        request.get_ref().present,
        request.get_ref().candidate_json.is_empty(),
    ) {
        (true, false) => Some(decode_typed(&request.get_ref().candidate_json)?),
        (false, true) => None,
        _ => {
            return Err(Status::invalid_argument(
                "logical repair presence and candidate disagree",
            ));
        }
    };
    service.validate_handoff(caller, scope.as_ref(), HandoffTarget::JoiningNode)?;
    let store = service.store.clone();
    let applied = tokio::task::spawn_blocking(move || {
        store.repair_quorum_reconciled_logical_record(&id, candidate.as_ref())
    })
    .await
    .map_err(join_status)?
    .map_err(logical_status)?;
    applied_response(applied.replayed)
}

pub(super) async fn export_authz_realm_keys(
    service: &DataPeerService,
    mut request: Request<wire::HandoffPageRequest>,
) -> Result<Response<wire::HandoffPage>, Status> {
    authorize_page(service, &mut request)?;
    let request = request.into_inner();
    validate_page_limits(&request)?;
    let cursor = decode_cursor::<AuthzRealmCursor>(&request.cursor_json)?;
    let repository = service.store.authz();
    let page = tokio::task::spawn_blocking(move || {
        repository.export_authz_realm_keys(cursor.as_ref(), request.max_records, request.max_bytes)
    })
    .await
    .map_err(join_status)?
    .map_err(authz_status)?;
    page_response(&page)
}

pub(super) async fn read_authz_schema_catalogue(
    service: &DataPeerService,
    mut request: Request<wire::AuthzSchemaCatalogueRequest>,
) -> Result<Response<wire::AuthzSchemaCatalogueResponse>, Status> {
    let peer = request.get_ref().peer.clone();
    let caller = service.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
    service.validate_handoff(
        caller,
        request.get_ref().handoff.as_ref(),
        HandoffTarget::AnyNode,
    )?;
    let tenant = StorageTenantId::parse(&request.get_ref().storage_tenant)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let repository = service.store.authz();
    let catalogue =
        tokio::task::spawn_blocking(move || repository.export_authz_schema_catalogue(&tenant))
            .await
            .map_err(join_status)?
            .map_err(authz_store_status)?;
    let catalogue_json = catalogue
        .as_ref()
        .map(encode_typed)
        .transpose()?
        .unwrap_or_default();
    require_typed_bound(&catalogue_json)?;
    Ok(Response::new(wire::AuthzSchemaCatalogueResponse {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        present: catalogue.is_some(),
        catalogue_json,
    }))
}

pub(super) async fn repair_authz_schema_catalogue(
    service: &DataPeerService,
    mut request: Request<wire::RepairAuthzSchemaCatalogueRequest>,
) -> Result<Response<wire::HandoffRecordApplied>, Status> {
    let peer = request.get_ref().peer.clone();
    let caller = service.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
    let handoff = request.get_ref().handoff.clone();
    service.validate_handoff(caller, handoff.as_ref(), HandoffTarget::JoiningNode)?;
    require_typed_bound(&request.get_ref().catalogue_json)?;
    let tenant = StorageTenantId::parse(&request.get_ref().storage_tenant)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let catalogue: Option<AuthzSchemaCatalogue> = match (
        request.get_ref().present,
        request.get_ref().catalogue_json.is_empty(),
    ) {
        (true, false) => Some(decode_typed(&request.get_ref().catalogue_json)?),
        (false, true) => None,
        _ => {
            return Err(Status::invalid_argument(
                "schema catalogue presence and payload disagree",
            ));
        }
    };
    if catalogue
        .as_ref()
        .is_some_and(|catalogue| catalogue.storage_tenant != tenant)
    {
        return Err(Status::invalid_argument(
            "schema catalogue belongs to another tenant",
        ));
    }
    service.validate_handoff(caller, handoff.as_ref(), HandoffTarget::JoiningNode)?;
    let repository = service.store.authz();
    let replayed = tokio::task::spawn_blocking(move || {
        let current = repository.export_authz_schema_catalogue(&tenant)?;
        if current.as_ref() == catalogue.as_ref() {
            return Ok(true);
        }
        repository.install_quorum_reconciled_authz_schema_catalogue(&tenant, catalogue.as_ref())?;
        Ok::<bool, AuthzStoreError>(false)
    })
    .await
    .map_err(join_status)?
    .map_err(authz_store_status)?;
    applied_response(replayed)
}

pub(super) async fn read_authz_realm_manifest(
    service: &DataPeerService,
    mut request: Request<wire::AuthzRealmRequest>,
) -> Result<Response<wire::AuthzRealmManifest>, Status> {
    authorize_realm(service, &mut request, HandoffTarget::AnyNode)?;
    let scope: AuthzScope = decode_typed(&request.get_ref().scope_json)?;
    let repository = service.store.authz();
    let manifest = tokio::task::spawn_blocking(move || {
        repository.export_authz_realm_stream(&scope, io::sink())
    })
    .await
    .map_err(join_status)?
    .map_err(authz_status)?;
    Ok(Response::new(wire::AuthzRealmManifest {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        present: manifest.is_some(),
        manifest_json: manifest
            .as_ref()
            .map(encode_typed)
            .transpose()?
            .unwrap_or_default(),
    }))
}

pub(super) async fn repair_authz_realm_absence(
    service: &DataPeerService,
    mut request: Request<wire::AuthzRealmRequest>,
) -> Result<Response<wire::HandoffRecordApplied>, Status> {
    let caller = authorize_realm(service, &mut request, HandoffTarget::JoiningNode)?;
    let handoff = request.get_ref().handoff.clone();
    let scope: AuthzScope = decode_typed(&request.get_ref().scope_json)?;
    service.validate_handoff(caller, handoff.as_ref(), HandoffTarget::JoiningNode)?;
    let repository = service.store.authz();
    let applied = tokio::task::spawn_blocking(move || {
        repository.install_quorum_reconciled_authz_realm_candidate(&scope, None)
    })
    .await
    .map_err(join_status)?
    .map_err(authz_status)?;
    applied_response(applied.replayed)
}

pub(super) async fn get_authz_realm(
    service: &DataPeerService,
    mut request: Request<wire::AuthzRealmRequest>,
) -> Result<Response<AuthzRealmStream>, Status> {
    authorize_realm(service, &mut request, HandoffTarget::AnyNode)?;
    let scope: AuthzScope = decode_typed(&request.get_ref().scope_json)?;
    let repository = service.store.authz();
    let manifest_scope = scope.clone();
    let manifest = tokio::task::spawn_blocking(move || {
        repository.export_authz_realm_stream(&manifest_scope, io::sink())
    })
    .await
    .map_err(join_status)?
    .map_err(authz_status)?
    .ok_or_else(|| Status::not_found("authorization realm is absent"))?;
    let manifest_json = encode_typed(&manifest)?;
    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    sender
        .send(Ok(wire::AuthzRealmFrame {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            offset: 0,
            content: Vec::new(),
            end: false,
            manifest_json,
        }))
        .await
        .map_err(|_| Status::cancelled("authorization realm stream closed"))?;
    let repository = service.store.authz();
    tokio::task::spawn_blocking(move || {
        let mut writer = RealmFrameWriter::new(sender);
        let observed = repository.export_authz_realm_stream(&scope, &mut writer);
        match observed {
            Ok(Some(observed)) if observed == manifest => writer.finish(),
            Ok(Some(_)) => writer.fail(Status::unavailable(
                "authorization realm changed during handoff export",
            )),
            Ok(None) => writer.fail(Status::not_found(
                "authorization realm disappeared during handoff export",
            )),
            Err(error) => writer.fail(authz_status(error)),
        }
    });
    Ok(Response::new(Box::pin(
        tokio_stream::wrappers::ReceiverStream::new(receiver),
    )))
}

pub(super) async fn put_authz_realm(
    service: &DataPeerService,
    request: Request<Streaming<wire::AuthzRealmPutFrame>>,
) -> Result<Response<wire::HandoffRecordApplied>, Status> {
    let pin = request
        .extensions()
        .get::<PeerSpkiSha256>()
        .copied()
        .ok_or_else(|| Status::unauthenticated("peer mTLS identity is missing"))?;
    let idle = effective_timeout(request.metadata(), service.maximum_unary_time);
    let mut stream = request.into_inner();
    let first = next_realm_put_frame(&mut stream, idle).await?;
    let caller = service.authorize_context(first.peer.as_ref(), pin, PeerRpcKind::StateTransfer)?;
    service.validate_handoff(caller, first.handoff.as_ref(), HandoffTarget::JoiningNode)?;
    let handoff = first.handoff.clone();
    if first.offset != 0 || !first.content.is_empty() || first.end || first.manifest_json.is_empty()
    {
        return Err(Status::invalid_argument(
            "authorization handoff must start with one manifest-only frame",
        ));
    }
    let manifest: AuthzRealmTransferManifest = decode_typed(&first.manifest_json)?;
    let (sender, receiver) = tokio::sync::mpsc::channel::<Vec<u8>>(2);
    let repository = service.store.authz();
    let install = tokio::task::spawn_blocking(move || {
        repository.install_quorum_reconciled_authz_realm_stream(
            &manifest,
            BlockingFrameReader::new(receiver),
        )
    });
    let mut offset = 0_u64;
    loop {
        let frame = next_realm_put_frame(&mut stream, idle).await?;
        let frame_caller =
            service.authorize_context(frame.peer.as_ref(), pin, PeerRpcKind::StateTransfer)?;
        if frame.handoff != handoff {
            return Err(Status::invalid_argument(
                "authorization handoff scope changed within the stream",
            ));
        }
        service.validate_handoff(
            frame_caller,
            frame.handoff.as_ref(),
            HandoffTarget::JoiningNode,
        )?;
        if !frame.manifest_json.is_empty() {
            return Err(Status::invalid_argument(
                "authorization manifest may appear only in the first frame",
            ));
        }
        validate_stream_frame(offset, &frame.content, frame.offset, frame.end)?;
        if frame.end && !frame.content.is_empty() {
            return Err(Status::invalid_argument(
                "authorization stream must use an empty final frame",
            ));
        }
        offset = offset
            .checked_add(frame.content.len() as u64)
            .ok_or_else(|| Status::resource_exhausted("authorization stream is too large"))?;
        if !frame.content.is_empty() {
            sender
                .send(frame.content)
                .await
                .map_err(|_| Status::data_loss("authorization install stopped reading"))?;
        }
        if frame.end {
            service.validate_handoff(frame_caller, handoff.as_ref(), HandoffTarget::JoiningNode)?;
            drop(sender);
            let applied = install.await.map_err(join_status)?.map_err(authz_status)?;
            return applied_response(applied.replayed);
        }
    }
}

pub(super) async fn export_payload_artifacts(
    service: &DataPeerService,
    mut request: Request<wire::HandoffPageRequest>,
) -> Result<Response<wire::HandoffPage>, Status> {
    authorize_page(service, &mut request)?;
    let request = request.into_inner();
    if request.max_records == 0 {
        return Err(Status::invalid_argument("payload handoff page is empty"));
    }
    let cursor = decode_cursor::<PayloadArtifactCursor>(&request.cursor_json)?;
    let store = service.store.clone();
    let page = tokio::task::spawn_blocking(move || {
        store.export_payload_artifact_snapshots(cursor.as_ref(), request.max_records)
    })
    .await
    .map_err(join_status)?
    .map_err(map_mutation_error)?;
    page_response(&page)
}

pub(super) async fn install_payload_lifecycle(
    service: &DataPeerService,
    mut request: Request<wire::HandoffRecordRequest>,
) -> Result<Response<wire::HandoffRecordApplied>, Status> {
    let caller = authorize_record(service, &mut request, HandoffTarget::AnyNode)?;
    let scope = request.get_ref().handoff.clone();
    require_typed_bound(&request.get_ref().record_json)?;
    let artifact: PayloadArtifactSnapshot = decode_typed(&request.get_ref().record_json)?;
    service.validate_handoff(caller, scope.as_ref(), HandoffTarget::AnyNode)?;
    service
        .store
        .install_payload_artifact_lifecycle(&service.codec, &artifact)
        .await
        .map_err(map_mutation_error)?;
    applied_response(false)
}

fn authorize_page(
    service: &DataPeerService,
    request: &mut Request<wire::HandoffPageRequest>,
) -> Result<AuthenticatedPeer, Status> {
    let peer = request.get_ref().peer.clone();
    let caller = service.authorize(request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
    service.validate_handoff(
        caller,
        request.get_ref().handoff.as_ref(),
        HandoffTarget::AnyNode,
    )?;
    Ok(caller)
}

fn authorize_record(
    service: &DataPeerService,
    request: &mut Request<wire::HandoffRecordRequest>,
    target: HandoffTarget,
) -> Result<AuthenticatedPeer, Status> {
    let peer = request.get_ref().peer.clone();
    let caller = service.authorize(request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
    service.validate_handoff(caller, request.get_ref().handoff.as_ref(), target)?;
    Ok(caller)
}

fn authorize_realm(
    service: &DataPeerService,
    request: &mut Request<wire::AuthzRealmRequest>,
    target: HandoffTarget,
) -> Result<AuthenticatedPeer, Status> {
    let peer = request.get_ref().peer.clone();
    let caller = service.authorize(request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
    service.validate_handoff(caller, request.get_ref().handoff.as_ref(), target)?;
    require_typed_bound(&request.get_ref().scope_json)?;
    Ok(caller)
}

fn authorize_logical(
    service: &DataPeerService,
    request: &mut Request<wire::LogicalRecordRequest>,
    target: HandoffTarget,
) -> Result<AuthenticatedPeer, Status> {
    let peer = request.get_ref().peer.clone();
    let caller = service.authorize(request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
    service.validate_handoff(caller, request.get_ref().handoff.as_ref(), target)?;
    Ok(caller)
}

fn authorize_repair_logical(
    service: &DataPeerService,
    request: &mut Request<wire::RepairLogicalRecordRequest>,
    target: HandoffTarget,
) -> Result<AuthenticatedPeer, Status> {
    let peer = request.get_ref().peer.clone();
    let caller = service.authorize(request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
    service.validate_handoff(caller, request.get_ref().handoff.as_ref(), target)?;
    Ok(caller)
}

fn validate_page_limits(request: &wire::HandoffPageRequest) -> Result<(), Status> {
    if request.max_records == 0 || request.max_bytes == 0 || request.max_bytes > HANDOFF_PAGE_BYTES
    {
        return Err(Status::invalid_argument(format!(
            "handoff pages require non-zero limits and at most {HANDOFF_PAGE_BYTES} bytes"
        )));
    }
    require_typed_bound(&request.cursor_json)
}

fn decode_cursor<T: serde::de::DeserializeOwned>(encoded: &[u8]) -> Result<Option<T>, Status> {
    if encoded.is_empty() {
        Ok(None)
    } else {
        decode_typed(encoded).map(Some)
    }
}

fn page_response<T: serde::Serialize>(page: &T) -> Result<Response<wire::HandoffPage>, Status> {
    let page_json = encode_typed(page)?;
    require_typed_bound(&page_json)?;
    Ok(Response::new(wire::HandoffPage {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        page_json,
    }))
}

fn applied_response(replayed: bool) -> Result<Response<wire::HandoffRecordApplied>, Status> {
    Ok(Response::new(wire::HandoffRecordApplied {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        replayed,
    }))
}

fn logical_status(error: LogicalRecordError) -> Status {
    match error {
        LogicalRecordError::Storage(_) => Status::internal(error.to_string()),
        LogicalRecordError::Tampered => Status::data_loss(error.to_string()),
        _ => Status::failed_precondition(error.to_string()),
    }
}

fn authz_status(error: AuthzRealmSnapshotError) -> Status {
    match error {
        AuthzRealmSnapshotError::Store(_) => Status::internal(error.to_string()),
        AuthzRealmSnapshotError::TransferIntegrity(_) => Status::data_loss(error.to_string()),
        _ => Status::failed_precondition(error.to_string()),
    }
}

fn authz_store_status(error: AuthzStoreError) -> Status {
    match error {
        AuthzStoreError::Storage(_) => Status::internal(error.to_string()),
        AuthzStoreError::RevisionNotAvailable { .. } | AuthzStoreError::ReceiptCapacity => {
            Status::unavailable(error.to_string())
        }
        _ => Status::failed_precondition(error.to_string()),
    }
}

fn join_status(error: tokio::task::JoinError) -> Status {
    Status::internal(format!("handoff worker failed: {error}"))
}

async fn next_realm_put_frame(
    stream: &mut Streaming<wire::AuthzRealmPutFrame>,
    idle: Duration,
) -> Result<wire::AuthzRealmPutFrame, Status> {
    tokio::time::timeout(idle, stream.message())
        .await
        .map_err(|_| Status::deadline_exceeded("authorization handoff made no progress"))??
        .ok_or_else(|| Status::invalid_argument("authorization handoff ended without final frame"))
}

struct RealmFrameWriter {
    sender: tokio::sync::mpsc::Sender<Result<wire::AuthzRealmFrame, Status>>,
    offset: u64,
}

impl RealmFrameWriter {
    fn new(sender: tokio::sync::mpsc::Sender<Result<wire::AuthzRealmFrame, Status>>) -> Self {
        Self { sender, offset: 0 }
    }

    fn finish(self) {
        let _ = self.sender.blocking_send(Ok(wire::AuthzRealmFrame {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            offset: self.offset,
            content: Vec::new(),
            end: true,
            manifest_json: Vec::new(),
        }));
    }

    fn fail(self, status: Status) {
        let _ = self.sender.blocking_send(Err(status));
    }
}

impl Write for RealmFrameWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        for content in bytes.chunks(DATA_PEER_FRAME_BYTES) {
            self.sender
                .blocking_send(Ok(wire::AuthzRealmFrame {
                    schema_version: DATA_PEER_SCHEMA_VERSION,
                    offset: self.offset,
                    content: content.to_vec(),
                    end: false,
                    manifest_json: Vec::new(),
                }))
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "realm stream closed"))?;
            self.offset = self
                .offset
                .checked_add(content.len() as u64)
                .ok_or_else(|| io::Error::other("realm stream length overflow"))?;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct BlockingFrameReader {
    receiver: tokio::sync::mpsc::Receiver<Vec<u8>>,
    current: io::Cursor<Vec<u8>>,
}

impl BlockingFrameReader {
    fn new(receiver: tokio::sync::mpsc::Receiver<Vec<u8>>) -> Self {
        Self {
            receiver,
            current: io::Cursor::new(Vec::new()),
        }
    }
}

impl Read for BlockingFrameReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        loop {
            let read = self.current.read(output)?;
            if read != 0 {
                return Ok(read);
            }
            let Some(next) = self.receiver.blocking_recv() else {
                return Ok(0);
            };
            self.current = io::Cursor::new(next);
        }
    }
}
