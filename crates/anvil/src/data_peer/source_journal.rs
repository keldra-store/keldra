use super::*;

pub(super) async fn status(
    service: &DataPeerService,
    mut request: Request<wire::SourceJournalStatusRequest>,
) -> Result<Response<wire::SourceJournalStatus>, Status> {
    let peer = request.get_ref().peer.clone();
    service.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
    let metadata = request.metadata().clone();
    let store = service.store.clone();
    let status = service
        .bounded(&metadata, async move {
            tokio::task::spawn_blocking(move || store.local_watch_status())
                .await
                .map_err(|error| Status::internal(format!("join journal status: {error}")))?
                .map_err(|error| Status::failed_precondition(error.to_string()))
        })
        .await?;
    Ok(Response::new(wire::SourceJournalStatus {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        source_id_json: encode_typed(&status.source_id)?,
        tail: status.tail,
        settled_through: status.settled_through,
        retention_floor: status.retention_floor,
        retained_entries: status.retained_entries,
        retained_bytes: status.retained_bytes,
    }))
}

pub(super) async fn read(
    service: &DataPeerService,
    mut request: Request<wire::SourceJournalReadRequest>,
) -> Result<Response<wire::SourceJournalPage>, Status> {
    let peer = request.get_ref().peer.clone();
    service.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
    let after = request.get_ref().after_offset;
    let limit = usize::try_from(request.get_ref().limit)
        .unwrap_or(usize::MAX)
        .min(MAX_LOCAL_INVALIDATION_SCAN_RECORDS);
    let max_bytes = request.get_ref().max_bytes;
    require_page_bound(max_bytes)?;
    let metadata = request.metadata().clone();
    let store = service.store.clone();
    let page = service
        .bounded(&metadata, async move {
            tokio::task::spawn_blocking(move || {
                store.scan_local_changes_bounded(after, limit, max_bytes)
            })
            .await
            .map_err(|error| Status::internal(format!("join journal read: {error}")))?
            .map_err(map_mutation_error)
        })
        .await?;
    encode_page_response(page)
}

pub(super) fn encode_page_response(
    page: anvil_store::LocalChangePage,
) -> Result<Response<wire::SourceJournalPage>, Status> {
    let changes_json = encode_page(page.changes)?;
    let actual_bytes = changes_json.iter().try_fold(0_u64, |total, encoded| {
        total.checked_add(encoded.len() as u64)
    });
    if actual_bytes != Some(page.encoded_bytes) {
        return Err(Status::internal(
            "source journal page byte accounting is inconsistent",
        ));
    }
    let (oversize_offset, oversize_encoded_bytes) = page
        .oversize
        .map_or((0, 0), |oversize| (oversize.offset, oversize.encoded_bytes));
    Ok(Response::new(wire::SourceJournalPage {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        changes_json,
        encoded_bytes: page.encoded_bytes,
        oversize_offset,
        oversize_encoded_bytes,
        source_id_json: encode_typed(&page.source_id)?,
    }))
}

pub(super) fn require_page_bound(max_bytes: u64) -> Result<(), Status> {
    if max_bytes == 0 || max_bytes > MAX_TYPED_MUTATION_BYTES as u64 {
        return Err(Status::invalid_argument(
            "source journal byte limit is outside the private peer bound",
        ));
    }
    Ok(())
}
