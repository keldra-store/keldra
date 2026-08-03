use super::*;

impl DataPeerService {
    pub(super) async fn apply_retained_version_delete_call(
        &self,
        mut request: Request<wire::TypedMutationRequest>,
    ) -> Result<Response<wire::RetainedVersionDeleteApplied>, Status> {
        let peer = request.get_ref().peer.clone();
        let peer = self.authorize(&mut request, peer.as_ref(), PeerRpcKind::DataPlane)?;
        require_typed_bound(&request.get_ref().mutation_json)?;
        let mutation: RetainedVersionDeleteMutation =
            decode_typed(&request.get_ref().mutation_json)?;
        let placement_fence = self
            .mutation_admission
            .retained_version_delete(peer, &mutation)?;
        let metadata = request.metadata().clone();
        let store = self.store.clone();
        let admission = self.mutation_admission.clone();
        let applied = self
            .bounded(&metadata, async move {
                admission.require_fence(placement_fence)?;
                let applied = store
                    .apply_retained_version_delete_replica(&mutation)
                    .await
                    .map_err(map_mutation_error)?;
                admission.require_fence(placement_fence)?;
                Ok(applied)
            })
            .await?;
        Ok(Response::new(wire::RetainedVersionDeleteApplied {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            outcome_json: encode_typed(&applied.outcome)?,
            replayed: applied.replayed,
        }))
    }
}
