//! Exact-transition mutation drain over the mandatory-mTLS data peer.

use super::*;

pub(super) async fn drain_mutations(
    service: &DataPeerService,
    mut request: Request<wire::MutationDrainRequest>,
) -> Result<Response<wire::MutationDrained>, Status> {
    let (caller, scope) = authorize_scope(service, &mut request)?;
    service.validate_handoff(caller, Some(&scope), HandoffTarget::ActiveOrJoiningNode)?;
    let identity = drain_identity(&scope);
    service.cutover_admission.close_now(identity)?;
    service
        .handoff
        .monitor_mutation_drain(service.cutover_admission.clone(), identity);
    service.cutover_admission.wait_until_drained().await?;
    Ok(Response::new(drained()))
}

pub(super) fn release_mutation_drain(
    service: &DataPeerService,
    mut request: Request<wire::MutationDrainRequest>,
) -> Result<Response<wire::MutationDrained>, Status> {
    let (caller, scope) = authorize_scope(service, &mut request)?;
    service
        .handoff
        .validate_mutation_drain_release(caller, &scope)?;
    service.cutover_admission.release(drain_identity(&scope));
    Ok(Response::new(drained()))
}

fn authorize_scope(
    service: &DataPeerService,
    request: &mut Request<wire::MutationDrainRequest>,
) -> Result<(AuthenticatedPeer, wire::HandoffScope), Status> {
    let context = request.get_ref().peer.clone();
    let caller = service.authorize(request, context.as_ref(), PeerRpcKind::StateTransfer)?;
    let scope = request
        .get_ref()
        .handoff
        .clone()
        .ok_or_else(|| Status::invalid_argument("handoff scope is required"))?;
    Ok((caller, scope))
}

fn drain_identity(scope: &wire::HandoffScope) -> crate::mutation_admission::DrainIdentity {
    crate::mutation_admission::DrainIdentity {
        joining_node_id: scope.joining_node_id,
        started_log_index: scope.started_log_index,
    }
}

fn drained() -> wire::MutationDrained {
    wire::MutationDrained {
        schema_version: DATA_PEER_SCHEMA_VERSION,
    }
}
