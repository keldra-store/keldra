//! Disposable rank-zero assignment inventory for one derived consumer kind.

use std::collections::BTreeMap;

use anvil_store::{
    DefinitionAssignment, DefinitionAssignmentMutation, DefinitionKind, PlacementLogId, VersionId,
};

use super::DerivedDefinitionIdentity;

type BucketIdentity = (u64, u64);

/// Process-local assignments grouped by their sparse journal-route boundary.
///
/// The durable assignment column family remains recoverable truth. This map is
/// populated by one bounded startup/recovery scan and then maintained from the
/// store's existing assignment notifications. It never becomes an authority.
pub(super) struct AssignedBucketInventory {
    kind: DefinitionKind,
    fence: PlacementLogId,
    buckets: BTreeMap<BucketIdentity, BTreeMap<u64, DefinitionAssignment>>,
}

impl AssignedBucketInventory {
    pub(super) fn new(kind: DefinitionKind, fence: PlacementLogId) -> Self {
        Self {
            kind,
            fence,
            buckets: BTreeMap::new(),
        }
    }

    pub(super) fn insert_scanned(
        &mut self,
        assignment: DefinitionAssignment,
    ) -> Option<DerivedDefinitionIdentity> {
        self.apply_upsert(assignment)
    }

    /// Applies one already-committed disposable assignment notification.
    /// Returns the prior rank-zero identity when pending retention evidence for
    /// that exact definition version must be discarded.
    pub(super) fn apply(
        &mut self,
        mutation: DefinitionAssignmentMutation,
    ) -> Option<DerivedDefinitionIdentity> {
        if mutation.kind() != self.kind {
            return None;
        }
        match mutation {
            DefinitionAssignmentMutation::Upsert(assignment) => self.apply_upsert(assignment),
            DefinitionAssignmentMutation::Delete(deletion) => self.apply_remove(
                deletion.tenant_id,
                deletion.bucket_id,
                deletion.definition_id,
                deletion.object_version,
                deletion.observed_fence,
            ),
            DefinitionAssignmentMutation::Remove {
                tenant_id,
                bucket_id,
                definition_id,
                object_version,
                observed_fence,
                ..
            } => self.apply_remove(
                tenant_id,
                bucket_id,
                definition_id,
                object_version,
                observed_fence,
            ),
        }
    }

    pub(super) fn buckets(
        &self,
    ) -> impl Iterator<Item = (BucketIdentity, &BTreeMap<u64, DefinitionAssignment>)> {
        self.buckets
            .iter()
            .map(|(&identity, assignments)| (identity, assignments))
    }

    pub(super) fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    pub(super) fn definitions(
        &self,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Option<&BTreeMap<u64, DefinitionAssignment>> {
        self.buckets.get(&(tenant_id, bucket_id))
    }

    fn apply_upsert(
        &mut self,
        assignment: DefinitionAssignment,
    ) -> Option<DerivedDefinitionIdentity> {
        let bucket = (assignment.tenant_id, assignment.bucket_id);
        let definition_id = assignment.definition_id;
        let existing = self
            .buckets
            .get(&bucket)
            .and_then(|definitions| definitions.get(&definition_id));
        if existing == Some(&assignment) {
            return None;
        }
        if existing.is_some_and(|existing| is_newer(existing, &assignment)) {
            return None;
        }
        let removed = self.remove_current(bucket, definition_id);
        if assignment.kind == self.kind
            && assignment.observed_fence == self.fence
            && assignment.rank == 0
        {
            self.buckets
                .entry(bucket)
                .or_default()
                .insert(definition_id, assignment);
        }
        removed.map(|assignment| DerivedDefinitionIdentity::from_assignment(&assignment))
    }

    fn apply_remove(
        &mut self,
        tenant_id: u64,
        bucket_id: u64,
        definition_id: u64,
        object_version: VersionId,
        observed_fence: PlacementLogId,
    ) -> Option<DerivedDefinitionIdentity> {
        let bucket = (tenant_id, bucket_id);
        let existing = self
            .buckets
            .get(&bucket)
            .and_then(|definitions| definitions.get(&definition_id));
        if existing.is_some_and(|existing| {
            existing.object_version > object_version
                || (existing.object_version == object_version
                    && fence_key(existing.observed_fence) > fence_key(observed_fence))
        }) {
            return None;
        }
        self.remove_current(bucket, definition_id)
            .map(|assignment| DerivedDefinitionIdentity::from_assignment(&assignment))
    }

    fn remove_current(
        &mut self,
        bucket: BucketIdentity,
        definition_id: u64,
    ) -> Option<DefinitionAssignment> {
        let definitions = self.buckets.get_mut(&bucket)?;
        let removed = definitions.remove(&definition_id);
        if definitions.is_empty() {
            self.buckets.remove(&bucket);
        }
        removed
    }
}

fn is_newer(existing: &DefinitionAssignment, candidate: &DefinitionAssignment) -> bool {
    existing.object_version > candidate.object_version
        || (existing.object_version == candidate.object_version
            && fence_key(existing.observed_fence) > fence_key(candidate.observed_fence))
}

const fn fence_key(fence: PlacementLogId) -> (u64, u64) {
    (fence.term, fence.index)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assignment(bucket_id: u64, definition_id: u64, version: u64) -> DefinitionAssignment {
        DefinitionAssignment {
            kind: DefinitionKind::Index,
            tenant_id: 1,
            bucket_id,
            definition_id,
            definition_path: format!("_anvil/indexes/{definition_id}.json"),
            object_version: VersionId(version),
            observed_fence: PlacementLogId { term: 1, index: 2 },
            rank: 0,
        }
    }

    #[test]
    fn assignments_are_grouped_by_bucket_and_updated_without_a_rescan() {
        let fence = PlacementLogId { term: 1, index: 2 };
        let mut inventory = AssignedBucketInventory::new(DefinitionKind::Index, fence);
        inventory.insert_scanned(assignment(10, 1, 1));
        inventory.insert_scanned(assignment(10, 2, 1));
        inventory.insert_scanned(assignment(20, 3, 1));

        let grouped = inventory
            .buckets()
            .map(|(bucket, definitions)| (bucket, definitions.len()))
            .collect::<Vec<_>>();
        assert_eq!(grouped, [((1, 10), 2), ((1, 20), 1)]);

        let removed = inventory.apply(DefinitionAssignmentMutation::Remove {
            kind: DefinitionKind::Index,
            tenant_id: 1,
            bucket_id: 10,
            definition_id: 1,
            object_version: VersionId(1),
            observed_fence: fence,
        });
        assert_eq!(removed.unwrap().definition_id, 1);
        assert_eq!(inventory.buckets().next().unwrap().1.len(), 1);
    }

    #[test]
    fn an_older_notification_cannot_replace_a_newer_scanned_assignment() {
        let fence = PlacementLogId { term: 1, index: 2 };
        let mut inventory = AssignedBucketInventory::new(DefinitionKind::Index, fence);
        inventory.insert_scanned(assignment(10, 1, 2));
        assert_eq!(
            inventory.apply(DefinitionAssignmentMutation::Upsert(assignment(10, 1, 1))),
            None
        );
        assert_eq!(
            inventory.buckets().next().unwrap().1[&1].object_version,
            VersionId(2)
        );
    }
}
