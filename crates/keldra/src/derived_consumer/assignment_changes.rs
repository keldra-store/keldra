//! Lossless, bounded assignment intake independent of derived work turns.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use keldra_store::{
    DefinitionAssignmentMutation, DefinitionKind, DerivedConsumerKind, PlacementLogId, Store,
};
use tonic::Status;

use super::assigned::AssignedBucketInventory;

type AssignmentKey = (u64, u64, u64);

pub(super) struct AssignmentChangeCollector {
    pending: Arc<Mutex<PendingAssignmentChanges>>,
    task: tokio::task::JoinHandle<()>,
}

pub(super) struct AssignmentChangeDrain {
    pub(super) replacement: Option<AssignedBucketInventory>,
    pub(super) mutations: Vec<DefinitionAssignmentMutation>,
}

#[derive(Default)]
struct PendingAssignmentChanges {
    replacement: Option<AssignedBucketInventory>,
    mutations: BTreeMap<AssignmentKey, DefinitionAssignmentMutation>,
    failure: Option<Status>,
}

impl AssignmentChangeCollector {
    pub(super) async fn start(
        kind: DerivedConsumerKind,
        store: Store,
        fence: PlacementLogId,
    ) -> Result<(AssignedBucketInventory, Self), Status> {
        let definition_kind = definition_kind(kind);
        let (inventory, mut receiver) = exact_snapshot(&store, definition_kind, fence).await?;
        let pending = Arc::new(Mutex::new(PendingAssignmentChanges::default()));
        let task_pending = pending.clone();
        let task = tokio::spawn(async move {
            let mut received_total = 0_u64;
            let mut coalesced_total = 0_u64;
            let mut pending_peak = 0_usize;
            let mut summary = tokio::time::interval(std::time::Duration::from_secs(10));
            summary.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            summary.tick().await;
            loop {
                let received = tokio::select! {
                    received = receiver.recv() => received,
                    _ = summary.tick() => {
                        let pending_count = task_pending
                            .lock()
                            .map(|pending| pending.mutations.len())
                            .unwrap_or(0);
                        tracing::info!(
                            consumer.kind = ?kind,
                            assignment.received_total = received_total,
                            assignment.coalesced_total = coalesced_total,
                            assignment.pending = pending_count,
                            assignment.pending_peak = pending_peak,
                            "derived assignment collector summary"
                        );
                        continue;
                    }
                };
                match received {
                    Ok(mutations) => {
                        let result = task_pending.lock().map(|mut pending| {
                            let before = pending.mutations.len();
                            let mut accepted = 0_usize;
                            for mutation in mutations {
                                if mutation.kind() == definition_kind {
                                    accepted += 1;
                                    insert_pending(&mut pending.mutations, mutation);
                                }
                            }
                            let after = pending.mutations.len();
                            received_total = received_total.saturating_add(accepted as u64);
                            coalesced_total = coalesced_total.saturating_add(
                                before.saturating_add(accepted).saturating_sub(after) as u64,
                            );
                            pending_peak = pending_peak.max(after);
                        });
                        if result.is_err() {
                            return;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            consumer.kind = ?kind,
                            skipped,
                            "derived assignment collector lagged; replacing from exact durable inventory"
                        );
                        match exact_snapshot(&store, definition_kind, fence).await {
                            Ok((replacement, exact_receiver)) => {
                                receiver = exact_receiver;
                                let Ok(mut pending) = task_pending.lock() else {
                                    return;
                                };
                                pending.replacement = Some(replacement);
                                pending.mutations.clear();
                            }
                            Err(error) => {
                                if let Ok(mut pending) = task_pending.lock() {
                                    pending.failure = Some(error);
                                }
                                return;
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        if let Ok(mut pending) = task_pending.lock() {
                            pending.failure = Some(Status::unavailable(
                                "derived assignment notifications closed",
                            ));
                        }
                        return;
                    }
                }
            }
        });
        Ok((inventory, Self { pending, task }))
    }

    pub(super) fn drain(&self) -> Result<AssignmentChangeDrain, Status> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| Status::internal("derived assignment collector lock is poisoned"))?;
        if let Some(error) = pending.failure.take() {
            return Err(error);
        }
        Ok(AssignmentChangeDrain {
            replacement: pending.replacement.take(),
            mutations: std::mem::take(&mut pending.mutations)
                .into_values()
                .collect(),
        })
    }
}

impl Drop for AssignmentChangeCollector {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn exact_snapshot(
    store: &Store,
    kind: DefinitionKind,
    fence: PlacementLogId,
) -> Result<
    (
        AssignedBucketInventory,
        tokio::sync::broadcast::Receiver<Vec<DefinitionAssignmentMutation>>,
    ),
    Status,
> {
    let store = store.clone();
    tokio::task::spawn_blocking(move || {
        let mut inventory = AssignedBucketInventory::new(kind, fence);
        let receiver = store.visit_definition_assignment_snapshot(kind, |assignment| {
            inventory.insert_scanned(assignment);
        })?;
        Ok::<_, keldra_store::DefinitionStateError>((inventory, receiver))
    })
    .await
    .map_err(|error| Status::internal(format!("derived assignment task failed: {error}")))?
    .map_err(|error| Status::internal(error.to_string()))
}

fn mutation_key(mutation: &DefinitionAssignmentMutation) -> AssignmentKey {
    match mutation {
        DefinitionAssignmentMutation::Upsert(value) => {
            (value.tenant_id, value.bucket_id, value.definition_id)
        }
        DefinitionAssignmentMutation::Delete(value) => {
            (value.tenant_id, value.bucket_id, value.definition_id)
        }
        DefinitionAssignmentMutation::Remove {
            tenant_id,
            bucket_id,
            definition_id,
            ..
        } => (*tenant_id, *bucket_id, *definition_id),
    }
}

fn insert_pending(
    pending: &mut BTreeMap<AssignmentKey, DefinitionAssignmentMutation>,
    mutation: DefinitionAssignmentMutation,
) {
    let key = mutation_key(&mutation);
    let incoming = (
        mutation.object_version(),
        mutation.observed_fence().term,
        mutation.observed_fence().index,
    );
    let retained_is_newer = pending.get(&key).is_some_and(|retained| {
        (
            retained.object_version(),
            retained.observed_fence().term,
            retained.observed_fence().index,
        ) > incoming
    });
    if !retained_is_newer {
        pending.insert(key, mutation);
    }
}

const fn definition_kind(kind: DerivedConsumerKind) -> DefinitionKind {
    match kind {
        DerivedConsumerKind::Index => DefinitionKind::Index,
        DerivedConsumerKind::Accounting => DefinitionKind::Accounting,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keldra_store::{DefinitionAssignment, VersionId};

    fn assignment(version: u64) -> DefinitionAssignmentMutation {
        DefinitionAssignmentMutation::Upsert(DefinitionAssignment {
            kind: DefinitionKind::Index,
            tenant_id: 1,
            bucket_id: 2,
            definition_id: 3,
            definition_path: "indexes/a".into(),
            object_version: VersionId(version),
            observed_fence: PlacementLogId { term: 4, index: 5 },
            rank: 0,
        })
    }

    #[test]
    fn pending_changes_coalesce_by_exact_definition_identity() {
        let mut pending = PendingAssignmentChanges::default();
        insert_pending(&mut pending.mutations, assignment(1));
        insert_pending(&mut pending.mutations, assignment(2));
        insert_pending(&mut pending.mutations, assignment(1));
        assert_eq!(pending.mutations.len(), 1);
        assert_eq!(
            pending.mutations.into_values().next().unwrap(),
            assignment(2)
        );
    }
}
