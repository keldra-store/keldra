use super::*;

impl DataPeerService {
    pub(super) async fn apply_retained_version_delete_call(
        &self,
        mut request: Request<wire::TypedMutationRequest>,
    ) -> Result<Response<wire::RetainedVersionDeleteApplied>, Status> {
        let _permit = self.cutover_admission.enter_continuation()?;
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
                loop {
                    admission.require_fence(placement_fence)?;
                    match store.apply_retained_version_delete_replica(&mutation).await {
                        Ok(applied) => {
                            admission.require_fence(placement_fence)?;
                            return Ok(applied);
                        }
                        Err(
                            error @ (keldra_store::MutationError::ReceiptCapacity
                            | keldra_store::MutationError::SourceJournalCapacity),
                        ) => {
                            let capacity = match error {
                                keldra_store::MutationError::ReceiptCapacity => "receipt",
                                keldra_store::MutationError::SourceJournalCapacity => {
                                    "source_journal"
                                }
                                _ => unreachable!("capacity pattern was matched"),
                            };
                            let started = std::time::Instant::now();
                            tracing::info!(
                                monotonic_counter.keldra_peer_mutation_backpressure_waits_total =
                                    1_u64,
                                capacity,
                                "replica retained-version delete is waiting for bounded durable state"
                            );
                            store.wait_for_mutation_capacity().await;
                            tracing::info!(
                                histogram.keldra_peer_mutation_backpressure_wait_duration_seconds =
                                    started.elapsed().as_secs_f64(),
                                capacity,
                                "replica retained-version delete capacity wait completed"
                            );
                        }
                        Err(error) => return Err(map_mutation_error(error)),
                    }
                }
            })
            .await?;
        Ok(Response::new(wire::RetainedVersionDeleteApplied {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            outcome_json: encode_typed(&applied.outcome)?,
            replayed: applied.replayed,
        }))
    }
}
