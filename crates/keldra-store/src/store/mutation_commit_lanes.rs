//! Stable conflict lanes for independently prepared object mutations.

use std::collections::BTreeSet;
use std::sync::Arc;

use tokio::sync::{
    Mutex, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, Semaphore,
};

use super::{DefinitionMutationIntent, PreparedOperation, receipt_key};

const CONFLICT_STRIPES_PER_COMMIT_LANE: usize = 64;

#[derive(Clone)]
pub(super) struct MutationCommitLanes {
    fence: Arc<RwLock<()>>,
    conflicts: Arc<Vec<Arc<Mutex<()>>>>,
    physical_slots: Arc<Semaphore>,
}

pub(super) struct MutationLaneGuard {
    _fence: OwnedRwLockReadGuard<()>,
    _conflicts: Vec<OwnedMutexGuard<()>>,
    _physical_slot: tokio::sync::OwnedSemaphorePermit,
}

pub(super) struct ExclusiveMutationGuard {
    _fence: OwnedRwLockWriteGuard<()>,
}

impl MutationCommitLanes {
    pub(super) fn new(commit_lanes: usize) -> Self {
        let conflict_count = commit_lanes
            .checked_mul(CONFLICT_STRIPES_PER_COMMIT_LANE)
            .expect("validated commit-lane count has bounded conflict stripes");
        let conflicts = (0..conflict_count)
            .map(|_| Arc::new(Mutex::new(())))
            .collect();
        Self {
            fence: Arc::new(RwLock::new(())),
            conflicts: Arc::new(conflicts),
            physical_slots: Arc::new(Semaphore::new(commit_lanes)),
        }
    }

    pub(super) async fn acquire(
        &self,
        resources: impl IntoIterator<Item = Vec<u8>>,
    ) -> MutationLaneGuard {
        let fence = self.fence.clone().read_owned().await;
        let stripes = resources
            .into_iter()
            .map(|resource| self.stripe(&resource))
            .collect::<BTreeSet<_>>();
        let mut conflicts = Vec::with_capacity(stripes.len());
        for stripe in stripes {
            conflicts.push(self.conflicts[stripe].clone().lock_owned().await);
        }
        let physical_slot = self
            .physical_slots
            .clone()
            .acquire_owned()
            .await
            .expect("mutation commit lane semaphore remains open");
        MutationLaneGuard {
            _fence: fence,
            _conflicts: conflicts,
            _physical_slot: physical_slot,
        }
    }

    pub(super) async fn acquire_exclusive(&self) -> ExclusiveMutationGuard {
        ExclusiveMutationGuard {
            _fence: self.fence.clone().write_owned().await,
        }
    }

    fn stripe(&self, resource: &[u8]) -> usize {
        let mut hash = 0xcbf29ce484222325_u64;
        for byte in resource {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        let count = u64::try_from(self.conflicts.len()).expect("conflict stripe count fits u64");
        usize::try_from(hash % count).expect("conflict stripe index fits usize")
    }
}

pub(super) fn conflict_resources(
    operation: &PreparedOperation,
    definition_intent: Option<DefinitionMutationIntent>,
) -> Vec<Vec<u8>> {
    let mut resources = operation
        .lock_paths()
        .into_iter()
        .map(|path| {
            tagged_resource(
                1,
                [
                    path.tenant.as_bytes(),
                    path.bucket.as_bytes(),
                    path.path.as_bytes(),
                ],
            )
        })
        .collect::<Vec<_>>();
    if let Some(command_id) = operation.command_id() {
        resources.push(tagged_resource(
            2,
            [receipt_key(operation.identity(), command_id).as_slice()],
        ));
    }
    if let Some(reference) = operation.payload_reference() {
        resources.push(tagged_resource(
            3,
            [
                reference.hash.as_slice(),
                reference.length.to_be_bytes().as_slice(),
            ],
        ));
    }
    if let Some(intent) = definition_intent {
        resources.push(tagged_resource(
            4,
            [
                operation.identity().encode().as_slice(),
                &[intent.kind as u8],
                intent.definition_id.to_be_bytes().as_slice(),
            ],
        ));
    }
    resources
}

fn tagged_resource<'a>(tag: u8, parts: impl IntoIterator<Item = &'a [u8]>) -> Vec<u8> {
    let mut resource = vec![tag];
    for part in parts {
        resource.extend_from_slice(
            &u64::try_from(part.len())
                .expect("mutation resource component length fits u64")
                .to_be_bytes(),
        );
        resource.extend_from_slice(part);
    }
    resource
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shared_resource_excludes_a_second_lane() {
        let lanes = MutationCommitLanes::new(4);
        let first = lanes.acquire([b"path:a".to_vec()]).await;
        let waiting = tokio::spawn({
            let lanes = lanes.clone();
            async move { lanes.acquire([b"path:a".to_vec()]).await }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(first);
        waiting.await.unwrap();
    }

    #[tokio::test]
    async fn exclusive_fence_waits_for_lane_and_blocks_new_lanes() {
        let lanes = MutationCommitLanes::new(2);
        let first = lanes.acquire([b"path:a".to_vec()]).await;
        let exclusive = tokio::spawn({
            let lanes = lanes.clone();
            async move { lanes.acquire_exclusive().await }
        });
        tokio::task::yield_now().await;
        assert!(!exclusive.is_finished());
        drop(first);
        let exclusive = exclusive.await.unwrap();
        let waiting = tokio::spawn({
            let lanes = lanes.clone();
            async move { lanes.acquire([b"path:b".to_vec()]).await }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(exclusive);
        waiting.await.unwrap();
    }
}
