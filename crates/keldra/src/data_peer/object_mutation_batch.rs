use super::*;

impl DataPeerService {
    pub(super) async fn apply_object_mutation_batch_call(
        &self,
        mut request: Request<wire::TypedMutationBatchRequest>,
    ) -> Result<Response<wire::ObjectMutationBatchApplied>, Status> {
        let _permit = self.cutover_admission.enter_continuation()?;
        let peer = request.get_ref().peer.clone();
        let peer = self.authorize(&mut request, peer.as_ref(), PeerRpcKind::DataPlane)?;
        let encoded = &request.get_ref().mutation_json;
        if encoded.is_empty() || encoded.len() > MAX_OBJECT_MUTATION_BATCH_ITEMS {
            return Err(Status::resource_exhausted(format!(
                "object mutation batch must contain 1..={MAX_OBJECT_MUTATION_BATCH_ITEMS} items"
            )));
        }
        let mut logical_bytes = 0_usize;
        let mut mutations = Vec::with_capacity(encoded.len());
        for value in encoded {
            require_typed_bound(value)?;
            logical_bytes = logical_bytes.checked_add(value.len()).ok_or_else(|| {
                Status::resource_exhausted("object mutation batch byte count overflow")
            })?;
            if logical_bytes > MAX_OBJECT_MUTATION_BATCH_BYTES {
                return Err(Status::resource_exhausted(format!(
                    "object mutation batch exceeds {MAX_OBJECT_MUTATION_BATCH_BYTES} bytes"
                )));
            }
            mutations.push(decode_typed::<ObjectMutation>(value)?);
        }
        let placement_fence = self
            .mutation_admission
            .object_mutation_batch(peer, &mutations)?;
        let metadata = request.metadata().clone();
        let store = self.store.clone();
        let admission = self.mutation_admission.clone();
        let applied = self
            .bounded(&metadata, async move {
                loop {
                    admission.require_fence(placement_fence)?;
                    match store.apply_object_mutation_replica_batch(&mutations).await {
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
                                monotonic_counter
                                    .keldra_peer_mutation_batch_backpressure_waits_total = 1_u64,
                                capacity,
                                operation_count = mutations.len(),
                                "replica mutation batch is waiting for bounded durable state"
                            );
                            store.wait_for_mutation_capacity().await;
                            tracing::info!(
                                histogram
                                    .keldra_peer_mutation_batch_backpressure_wait_duration_seconds =
                                    started.elapsed().as_secs_f64(),
                                capacity,
                                operation_count = mutations.len(),
                                "replica mutation batch capacity wait completed"
                            );
                        }
                        Err(error) => return Err(map_mutation_error(error)),
                    }
                }
            })
            .await?;
        Ok(Response::new(wire::ObjectMutationBatchApplied {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            outcomes: applied
                .into_iter()
                .map(|outcome| wire::ObjectMutationApplied {
                    schema_version: DATA_PEER_SCHEMA_VERSION,
                    version: outcome.version.0,
                    replayed: outcome.replayed,
                })
                .collect(),
        }))
    }
}
