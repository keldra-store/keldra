use super::*;

pub(super) async fn handle(
    state: &AppState,
    request: Request<PublishPendingMutationFinalisationRequest>,
) -> Result<Response<PublishPendingMutationFinalisationResponse>, Status> {
    ensure_internal_node_request(state, &request).await?;
    let source_node_id = request
        .get_ref()
        .header
        .as_ref()
        .expect("authenticated internal request has a header")
        .source_node_id
        .clone();
    let request = request.into_inner();
    validate_pending_mutation_finalisation_request(&request)?;
    core_store::CoreStore::validate_pending_mutation_finalisation_proposal_bytes(
        &request.finalisation_record,
        &source_node_id,
    )
    .map_err(core_store_internal_status)?;
    state
        .core_store
        .verify_internal_core_receipt_signature(
            &source_node_id,
            &request.payload_hash,
            &request.source_signature,
        )
        .map_err(core_store_internal_status)?;

    if let core_store::CoreMetaWriteRoute::Remote(target) = state
        .core_store
        .pending_mutation_finalisation_write_route()
        .await
        .map_err(core_store_internal_status)?
    {
        return Err(Status::unavailable(format!(
            "pending mutation finalisation owner is {}",
            target.node_id
        )));
    }

    let finalisation_record = state
        .core_store
        .publish_pending_mutation_finalisation_proposal_as_owner(
            &request.finalisation_record,
            &source_node_id,
        )
        .await
        .map_err(core_store_internal_status)?;
    Ok(Response::new(PublishPendingMutationFinalisationResponse {
        finalisation_record,
    }))
}
