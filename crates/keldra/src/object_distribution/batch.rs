//! Bounded distributed execution for public `BulkWrite` coordinator groups.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;

use keldra_store::{
    BatchOperation, CoordinatedObjectMutation, DefinitionMutationIntent, Durability,
    MutationReceipt, ObjectMutationGovernance,
};
use tonic::Status;

use super::{
    MutableRecordReplicaGroup, ObjectDistribution, complete_metadata, mutation_capacity_kind,
    mutation_status, operation_key, stage_distributed_put,
};
use crate::cluster_placement::ClusterPlacement;
use crate::mutation_admission::{MutationAdmission, MutationPermit};
use crate::payload_distribution::PreparedPayloadEvidence;

#[derive(Clone)]
struct BatchItem {
    index: usize,
    operation: BatchOperation,
    governance: ObjectMutationGovernance,
    definition_intent: Option<DefinitionMutationIntent>,
}

#[derive(Clone)]
struct PreparedBatchItem {
    item: BatchItem,
}

impl ObjectDistribution {
    pub(crate) async fn mutate_many(
        &self,
        operations: Vec<BatchOperation>,
    ) -> Vec<Result<MutationReceipt, Status>> {
        self.mutate_many_inner(
            operations
                .into_iter()
                .map(|operation| (operation, None, None))
                .collect(),
        )
        .await
    }

    pub(crate) async fn mutate_many_with_definition_intents(
        &self,
        operations: Vec<(BatchOperation, Option<DefinitionMutationIntent>)>,
    ) -> Vec<Result<MutationReceipt, Status>> {
        self.mutate_many_inner(
            operations
                .into_iter()
                .map(|(operation, intent)| (operation, intent, None))
                .collect(),
        )
        .await
    }

    pub(crate) async fn mutate_many_with_governance(
        &self,
        operations: Vec<(BatchOperation, ObjectMutationGovernance)>,
    ) -> Vec<Result<MutationReceipt, Status>> {
        self.mutate_many_inner(
            operations
                .into_iter()
                .map(|(operation, governance)| (operation, None, Some(governance)))
                .collect(),
        )
        .await
    }

    async fn mutate_many_inner(
        &self,
        operations: Vec<(
            BatchOperation,
            Option<DefinitionMutationIntent>,
            Option<ObjectMutationGovernance>,
        )>,
    ) -> Vec<Result<MutationReceipt, Status>> {
        if operations.is_empty() {
            return Vec::new();
        }
        let count = operations.len();
        let placement = match self.placement() {
            Ok(placement) => placement,
            Err(error) => return (0..count).map(|_| Err(error.clone())).collect(),
        };
        if placement.active_node_ids().len() == 1 {
            if operations
                .iter()
                .all(|(_, intent, governance)| intent.is_none() && governance.is_none())
            {
                let operations = operations
                    .into_iter()
                    .map(|(operation, _, _)| operation)
                    .collect();
                return match self.mutation_admission.enter() {
                    Ok(_permit) => self
                        .store
                        .bulk_write_with_backpressure(operations)
                        .await
                        .into_iter()
                        .map(|outcome| outcome.result.map_err(mutation_status))
                        .collect(),
                    Err(error) => (0..count).map(|_| Err(error.clone())).collect(),
                };
            }
            let mut outcomes = Vec::with_capacity(count);
            for (operation, intent, governance) in operations {
                let result = match (intent, governance) {
                    (Some(intent), Some(governance)) => {
                        self.mutate_with_governance_and_definition_intent(
                            operation,
                            governance,
                            Some(intent),
                        )
                        .await
                    }
                    (Some(intent), None) => {
                        self.mutate_with_definition_intent(operation, intent).await
                    }
                    (None, Some(governance)) => {
                        self.mutate_with_governance(operation, governance).await
                    }
                    (None, None) => self.mutate(operation).await,
                };
                outcomes.push(result);
            }
            return outcomes;
        }

        let mut governance_cache =
            BTreeMap::<(String, String), Result<ObjectMutationGovernance, Status>>::new();
        let mut grouped = BTreeMap::<Vec<u64>, Vec<BatchItem>>::new();
        let mut outcomes = vec![None; count];
        for (index, (operation, definition_intent, supplied_governance)) in
            operations.into_iter().enumerate()
        {
            let key = operation_key(&operation);
            let governance = match supplied_governance {
                Some(governance) => Ok(governance),
                None => governance_cache
                    .entry((key.tenant().to_owned(), key.bucket().to_owned()))
                    .or_insert_with(|| self.resolve_governance(key))
                    .clone(),
            };
            let governance = match governance {
                Ok(governance) => governance,
                Err(error) => {
                    outcomes[index] = Some(Err(error));
                    continue;
                }
            };
            let group = match self.replica_group_stable(
                &placement,
                governance.tenant_id,
                governance.bucket_id,
                key,
            ) {
                Ok(group) if group.coordinator() == self.local_node => group,
                Ok(group) => {
                    outcomes[index] = Some(Err(Status::failed_precondition(format!(
                        "object path is coordinated by node {}",
                        group.coordinator().0
                    ))));
                    continue;
                }
                Err(error) => {
                    outcomes[index] = Some(Err(error));
                    continue;
                }
            };
            let group_key = group.replicas().iter().map(|node| node.0).collect();
            grouped.entry(group_key).or_default().push(BatchItem {
                index,
                operation,
                governance,
                definition_intent,
            });
        }

        let mut tasks = tokio::task::JoinSet::new();
        for (_, items) in grouped {
            let distribution = self.clone();
            tasks.spawn(async move {
                let indices = items.iter().map(|item| item.index).collect::<Vec<_>>();
                let result = distribution.execute_mutation_group(items).await;
                (indices, result)
            });
        }
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok((_, group_outcomes)) => {
                    for (index, outcome) in group_outcomes {
                        outcomes[index] = Some(outcome);
                    }
                }
                Err(error) => {
                    tracing::error!(%error, "distributed mutation group task failed");
                }
            }
        }
        outcomes
            .into_iter()
            .map(|outcome| {
                outcome.unwrap_or_else(|| {
                    Err(Status::internal(
                        "distributed mutation group did not return an outcome",
                    ))
                })
            })
            .collect()
    }

    fn resolve_governance(
        &self,
        key: &keldra_store::ObjectKey,
    ) -> Result<ObjectMutationGovernance, Status> {
        let (tenant_id, bucket_id) = self
            .store
            .resolve_bucket_ids(key.tenant(), key.bucket())
            .map_err(mutation_status)?;
        Ok(ObjectMutationGovernance {
            tenant_id,
            bucket_id,
            versioning: self
                .store
                .bucket_versioning(key.tenant(), key.bucket())
                .map_err(mutation_status)?,
            policy: self
                .store
                .bucket_policy(key.tenant(), key.bucket())
                .map_err(mutation_status)?,
        })
    }

    async fn execute_mutation_group(
        &self,
        items: Vec<BatchItem>,
    ) -> Vec<(usize, Result<MutationReceipt, Status>)> {
        let mut outcomes = BTreeMap::new();
        let mut preparation = tokio::task::JoinSet::new();
        let group_indices = items.iter().map(|item| item.index).collect::<Vec<_>>();
        for item in items {
            let distribution = self.clone();
            preparation.spawn(async move {
                let index = item.index;
                let result = async {
                    let operation = match item.operation {
                        BatchOperation::Put(request) => {
                            stage_distributed_put(&distribution.store, request)
                                .await
                                .map(BatchOperation::Publish)
                        }
                        operation => Ok(operation),
                    }?;
                    Ok::<_, Status>(PreparedBatchItem {
                        item: BatchItem { operation, ..item },
                    })
                }
                .await;
                (index, result)
            });
        }
        let mut prepared = Vec::new();
        while let Some(joined) = preparation.join_next().await {
            match joined {
                Ok((_, Ok(item))) => prepared.push(item),
                Ok((index, Err(error))) => {
                    tracing::warn!(%error, "distributed bulk payload preparation failed");
                    outcomes.insert(index, Err(error));
                }
                Err(error) => {
                    tracing::error!(%error, "distributed bulk payload task failed");
                }
            }
        }
        for index in group_indices {
            if !prepared.iter().any(|item| item.item.index == index)
                && !outcomes.contains_key(&index)
            {
                outcomes.insert(
                    index,
                    Err(Status::internal(
                        "distributed bulk payload task did not return an outcome",
                    )),
                );
            }
        }
        prepared.sort_by_key(|item| item.item.index);
        if prepared.is_empty() {
            return outcomes.into_iter().collect();
        }

        loop {
            let pending = prepared
                .iter()
                .filter(|item| !outcomes.contains_key(&item.item.index))
                .cloned()
                .collect::<Vec<_>>();
            if pending.is_empty() {
                return outcomes.into_iter().collect();
            }
            let attempt = begin_fenced_mutation_attempt(&self.mutation_admission, || async {
                let placement = match self.placement()? {
                    placement if placement.active_node_ids().len() > 1 => placement,
                    _ => {
                        return Err(Status::unavailable(
                            "object placement changed while starting a distributed mutation batch",
                        ));
                    }
                };
                let group = self.current_mutation_group(&placement, &pending)?;
                let prepared_payloads = self
                    .prepare_mutation_group_payloads(&placement, &pending)
                    .await;
                let reconcilable = pending
                    .iter()
                    .zip(&prepared_payloads)
                    .filter(|(_, evidence)| evidence.is_ok())
                    .map(|(item, _)| item.clone())
                    .collect::<Vec<_>>();
                let context = if reconcilable.is_empty() {
                    None
                } else {
                    Some(
                        self.reconcile_mutation_group(&placement, &reconcilable)
                            .await?,
                    )
                };
                Ok((placement, group, prepared_payloads, context))
            })
            .await;
            let (permit, (placement, group, prepared_payloads, context)) = match attempt {
                Ok(attempt) => attempt,
                Err(error) => return failed_group_outcomes(outcomes, &pending, error),
            };
            let mut attempt_items = Vec::with_capacity(pending.len());
            let mut payload_evidence = Vec::with_capacity(pending.len());
            for (item, evidence) in pending.into_iter().zip(prepared_payloads) {
                match evidence {
                    Ok(evidence) => {
                        attempt_items.push(item);
                        payload_evidence.push(evidence);
                    }
                    Err(error) => {
                        tracing::warn!(%error, "distributed bulk payload preparation failed");
                        outcomes.insert(item.item.index, Err(error));
                    }
                }
            }
            if attempt_items.is_empty() {
                return outcomes.into_iter().collect();
            }
            let context = context.expect("a prepared payload has a reconciliation context");
            let completion = self.clone();
            let completion_placement = placement.clone();
            let completion_group = group.clone();
            let store_operations = attempt_items
                .iter()
                .map(|item| {
                    (
                        item.item.operation.clone(),
                        item.item.governance.clone(),
                        item.item.definition_intent,
                    )
                })
                .collect();
            let completed = complete_metadata(async move {
                let _permit = permit;
                let coordinated = completion
                    .store
                    .coordinate_distributed_mutation_batch(store_operations, context)
                    .await
                    .map_err(mutation_status)?;
                let durable = completion
                    .replicate_mutation_group_batch(
                        &completion_placement,
                        &completion_group,
                        &coordinated,
                    )
                    .await;
                Ok::<_, Status>((coordinated, durable))
            })
            .await;
            if let Err(error) = &completed
                && let Some(capacity) = mutation_capacity_kind(error)
            {
                self.wait_for_mutation_capacity(capacity).await;
                continue;
            }
            let (coordinated, durable) = match completed {
                Ok(value) => value,
                Err(error) => {
                    return failed_group_outcomes(outcomes, &attempt_items, error);
                }
            };
            for (((item, evidence), coordinated), durable) in attempt_items
                .iter()
                .zip(payload_evidence.iter())
                .zip(coordinated.iter())
                .zip(durable.into_iter())
            {
                let result = match (coordinated, durable) {
                    (Err(error), _) => Err(mutation_status(error.clone())),
                    (Ok(_), Err(error)) => Err(error),
                    (Ok(coordinated), Ok(())) => {
                        match (&item.item.operation, evidence.as_ref()) {
                            (BatchOperation::Publish(request), Some(evidence)) => {
                                match request.durability {
                                    Durability::Local => self.continue_payload_placement(
                                        self.local_node,
                                        request.blob.clone(),
                                    ),
                                    Durability::Replicated => {
                                        if let Err(error) = self
                                            .wait_for_replicated_reference(
                                                &placement,
                                                &request.blob,
                                                evidence,
                                                coordinated,
                                            )
                                            .await
                                        {
                                            outcomes.insert(item.item.index, Err(error));
                                            continue;
                                        }
                                    }
                                }
                            }
                            (BatchOperation::Delete(_), None) => {}
                            _ => {
                                outcomes.insert(
                                    item.item.index,
                                    Err(Status::internal(
                                        "distributed batch payload evidence is inconsistent",
                                    )),
                                );
                                continue;
                            }
                        }
                        Ok(coordinated.receipt.clone())
                    }
                };
                outcomes.insert(item.item.index, result);
            }
            if let Err(error) = self.placement().and_then(|current| {
                (current.fence() == placement.fence())
                    .then_some(())
                    .ok_or_else(|| {
                        Status::unavailable("object placement changed during grouped mutation")
                    })
            }) {
                for item in &attempt_items {
                    outcomes.insert(item.item.index, Err(error.clone()));
                }
            }
            return outcomes.into_iter().collect();
        }
    }

    fn current_mutation_group(
        &self,
        placement: &ClusterPlacement,
        prepared: &[PreparedBatchItem],
    ) -> Result<MutableRecordReplicaGroup, Status> {
        let mut expected = None;
        for item in prepared {
            let key = operation_key(&item.item.operation);
            let candidate = self.replica_group_stable(
                placement,
                item.item.governance.tenant_id,
                item.item.governance.bucket_id,
                key,
            )?;
            if candidate.coordinator() != self.local_node {
                return Err(Status::failed_precondition(format!(
                    "object path is coordinated by node {}",
                    candidate.coordinator().0
                )));
            }
            if expected
                .as_ref()
                .is_some_and(|current| current != &candidate)
            {
                return Err(Status::unavailable(
                    "object placement changed the metadata group during a mutation batch",
                ));
            }
            expected = Some(candidate);
        }
        expected.ok_or_else(|| Status::internal("distributed mutation batch is empty"))
    }

    async fn prepare_mutation_group_payloads(
        &self,
        placement: &ClusterPlacement,
        prepared: &[PreparedBatchItem],
    ) -> Vec<Result<Option<PreparedPayloadEvidence>, Status>> {
        let mut tasks = tokio::task::JoinSet::new();
        for item in prepared {
            let distribution = self.clone();
            let placement = placement.clone();
            let index = item.item.index;
            let operation = item.item.operation.clone();
            tasks.spawn(async move {
                let evidence: Result<Option<PreparedPayloadEvidence>, Status> = async {
                    match operation {
                        BatchOperation::Publish(request) => {
                            let evidence = distribution
                                .prepare_payload(
                                    &placement,
                                    distribution.local_node,
                                    &request.blob,
                                    request.durability,
                                )
                                .await?;
                            distribution
                                .payload
                                .verify_on_path_coordinator(
                                    &placement,
                                    &request.blob,
                                    request.durability,
                                    distribution.local_node,
                                    &evidence,
                                )
                                .await
                                .map_err(super::payload_status)?;
                            Ok(Some(evidence))
                        }
                        BatchOperation::Delete(_) => Ok(None),
                        BatchOperation::Put(_) => {
                            unreachable!("put was sealed before the attempt")
                        }
                    }
                }
                .await;
                (index, evidence)
            });
        }
        let mut evidence = BTreeMap::new();
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok((index, value)) => {
                    evidence.insert(index, value);
                }
                Err(error) => {
                    return prepared
                        .iter()
                        .map(|_| {
                            Err(Status::internal(format!(
                                "distributed bulk payload task failed: {error}"
                            )))
                        })
                        .collect();
                }
            }
        }
        prepared
            .iter()
            .map(|item| match evidence.remove(&item.item.index) {
                Some(result) => result,
                None => Err(Status::internal(
                    "distributed bulk payload task did not return an outcome",
                )),
            })
            .collect()
    }

    async fn reconcile_mutation_group(
        &self,
        placement: &ClusterPlacement,
        prepared: &[PreparedBatchItem],
    ) -> Result<keldra_store::ObjectMutationContext, Status> {
        let mut contexts = tokio::task::JoinSet::new();
        let mut unique_paths = BTreeSet::new();
        for item in prepared {
            let key = operation_key(&item.item.operation);
            unique_paths.insert((
                item.item.governance.tenant_id,
                item.item.governance.bucket_id,
                key.clone(),
            ));
        }
        for (tenant_id, bucket_id, key) in unique_paths {
            let distribution = self.clone();
            let fence = placement.fence();
            contexts.spawn(async move {
                distribution
                    .reconcile_before_mutation_stable(&key, tenant_id, bucket_id, fence)
                    .await
            });
        }
        let mut context = None;
        while let Some(joined) = contexts.join_next().await {
            match joined {
                Ok(Ok(candidate)) if context.is_none() => context = Some(candidate),
                Ok(Ok(candidate)) if context == Some(candidate) => {}
                Ok(Ok(_)) => {
                    return Err(Status::unavailable(
                        "serving authority changed during grouped mutation reconciliation",
                    ));
                }
                Ok(Err(error)) => return Err(error),
                Err(error) => {
                    return Err(Status::internal(format!(
                        "grouped mutation reconciliation task failed: {error}"
                    )));
                }
            }
        }
        context.ok_or_else(|| Status::internal("distributed mutation batch is empty"))
    }

    async fn replicate_mutation_group_batch(
        &self,
        placement: &ClusterPlacement,
        group: &MutableRecordReplicaGroup,
        coordinated: &[Result<CoordinatedObjectMutation, keldra_store::MutationError>],
    ) -> Vec<Result<(), Status>> {
        let mut results = vec![Ok(()); coordinated.len()];
        for (index, outcome) in coordinated.iter().enumerate() {
            if outcome
                .as_ref()
                .is_ok_and(|value| value.mutation.is_none() && !value.receipt.replayed)
            {
                results[index] = Err(Status::data_loss(
                    "coordinator omitted a non-replayed replica mutation",
                ));
            }
        }
        let indexed_mutations = coordinated
            .iter()
            .enumerate()
            .filter_map(|(index, outcome)| {
                outcome
                    .as_ref()
                    .ok()
                    .and_then(|value| value.mutation.clone())
                    .map(|mutation| (index, mutation))
            })
            .collect::<Vec<_>>();
        if indexed_mutations.is_empty() {
            return results;
        }
        let mutations = indexed_mutations
            .iter()
            .map(|(_, mutation)| mutation.clone())
            .collect::<Vec<_>>();
        let mut durable = vec![vec![self.local_node]; mutations.len()];
        let mut tasks = tokio::task::JoinSet::new();
        for node in group
            .replicas()
            .iter()
            .copied()
            .filter(|node| *node != self.local_node)
        {
            let Some(address) = placement.address(node).map(|address| address.0.clone()) else {
                tracing::warn!(node = node.0, "metadata replica has no peer address");
                continue;
            };
            let peers = self.peers.clone();
            let mutations = mutations.clone();
            tasks.spawn(async move {
                (
                    node,
                    peers
                        .apply_object_mutation_batch(node, &address, &mutations)
                        .await,
                )
            });
        }
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok((node, Ok(applied))) if applied.len() == mutations.len() => {
                    for (index, outcome) in applied.into_iter().enumerate() {
                        if outcome.version == mutations[index].version.id {
                            durable[index].push(node);
                        } else {
                            tracing::warn!(
                                node = node.0,
                                item = index,
                                "replica mutation batch returned another version"
                            );
                        }
                    }
                }
                Ok((node, Ok(_))) => tracing::warn!(
                    node = node.0,
                    "replica mutation batch returned an unexpected outcome count"
                ),
                Ok((node, Err(error))) => {
                    tracing::warn!(node = node.0, %error, "replica mutation batch failed")
                }
                Err(error) => tracing::warn!(%error, "replica mutation batch task failed"),
            }
        }

        let mut quorum_positions = Vec::new();
        for (mutation_index, (coordinated_index, mutation)) in indexed_mutations.iter().enumerate()
        {
            if group.is_acknowledged_by(&durable[mutation_index]) {
                quorum_positions.push((
                    mutation.stamp.source_id,
                    mutation.stamp.source_journal_position,
                ));
            } else {
                results[*coordinated_index] = Err(Status::unavailable(format!(
                    "object metadata reached {} of {} required replicas",
                    durable[mutation_index].len(),
                    group.required_acknowledgements()
                )));
            }
        }
        if let Some((source, _)) = quorum_positions.first().copied()
            && let Err(error) = self
                .store
                .settle_source_journal_positions_if_contiguous(
                    source,
                    &quorum_positions
                        .iter()
                        .filter(|(candidate, _)| *candidate == source)
                        .map(|(_, offset)| *offset)
                        .collect::<Vec<_>>(),
                )
                .await
        {
            tracing::warn!(
                source = ?source,
                count = quorum_positions.len(),
                %error,
                "metadata batch quorum succeeded but source settlement failed"
            );
        }
        results
    }
}

async fn begin_fenced_mutation_attempt<T, F, Fut>(
    admission: &MutationAdmission,
    prepare: F,
) -> Result<(MutationPermit, T), Status>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, Status>>,
{
    let permit = admission.enter()?;
    let attempt = prepare().await?;
    Ok((permit, attempt))
}

fn failed_group_outcomes(
    mut outcomes: BTreeMap<usize, Result<MutationReceipt, Status>>,
    prepared: &[PreparedBatchItem],
    error: Status,
) -> Vec<(usize, Result<MutationReceipt, Status>)> {
    for item in prepared {
        outcomes.insert(item.item.index, Err(error.clone()));
    }
    outcomes.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::mutation_admission::DrainIdentity;

    use super::*;

    fn cutover() -> DrainIdentity {
        DrainIdentity {
            joining_node_id: 9,
            started_log_index: 41,
        }
    }

    #[tokio::test]
    async fn cutover_closes_admission_before_attempt_state_is_observed() {
        let admission = MutationAdmission::new_closed(cutover());
        let observed = Arc::new(AtomicUsize::new(0));
        let closure_observed = observed.clone();

        let error = begin_fenced_mutation_attempt(&admission, move || async move {
            closure_observed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await
        .err()
        .expect("closed admission must reject the attempt");

        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert_eq!(observed.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_retried_attempt_reads_fresh_state_after_capacity_wait() {
        let admission = MutationAdmission::new();
        let generation = Arc::new(AtomicUsize::new(1));
        let calls = Arc::new(AtomicUsize::new(0));

        let first_generation = generation.clone();
        let first_calls = calls.clone();
        let (first_permit, first) = begin_fenced_mutation_attempt(&admission, move || async move {
            first_calls.fetch_add(1, Ordering::SeqCst);
            Ok(first_generation.load(Ordering::SeqCst))
        })
        .await
        .unwrap();
        assert_eq!(first, 1);
        drop(first_permit);

        generation.store(2, Ordering::SeqCst);
        let second_generation = generation.clone();
        let second_calls = calls.clone();
        let (_second_permit, second) =
            begin_fenced_mutation_attempt(&admission, move || async move {
                second_calls.fetch_add(1, Ordering::SeqCst);
                Ok(second_generation.load(Ordering::SeqCst))
            })
            .await
            .unwrap();

        assert_eq!(second, 2);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
