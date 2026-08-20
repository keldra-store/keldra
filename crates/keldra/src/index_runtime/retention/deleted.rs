//! Bounded cleanup of one deleted format-v4 index namespace.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use keldra_store::{
    DefinitionKind, DefinitionLocator, DefinitionOperation, DeletedDefinitionCleanup, Store,
    VersionId,
};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::{IndexCurrentHead, IndexHeadScanScope};
use crate::index_runtime::coordination::definition_reference_matches;
use crate::logical_name_resolution::LogicalNameResolver;

use super::{
    IndexArtifactDelete, IndexArtifactRouter, IndexRetentionBudget, IndexRetentionSchedule,
    RetentionWork, UNREACHABLE_ARTIFACT_SAFETY_MILLIS, delete_command, now_unix_millis,
    retention_due_status,
};
use crate::index_runtime::scanner::{ClusterIndexScan, ClusterIndexScanner};

#[derive(Clone)]
pub(super) struct DeletedDefinitionRetention {
    store: Store,
    scanner: ClusterIndexScanner,
    reader: ClusterObjectReader,
    artifacts: IndexArtifactRouter,
    names: LogicalNameResolver,
    budget: IndexRetentionBudget,
    schedule: IndexRetentionSchedule,
    active: Arc<Mutex<Option<DeletedCleanupJob>>>,
}

impl DeletedDefinitionRetention {
    pub(super) fn new(
        store: Store,
        scanner: ClusterIndexScanner,
        reader: ClusterObjectReader,
        artifacts: IndexArtifactRouter,
        names: LogicalNameResolver,
        budget: IndexRetentionBudget,
        schedule: IndexRetentionSchedule,
    ) -> Self {
        Self {
            store,
            scanner,
            reader,
            artifacts,
            names,
            budget,
            schedule,
            active: Arc::new(Mutex::new(None)),
        }
    }

    pub(super) fn with_budget(mut self, budget: IndexRetentionBudget) -> Self {
        self.budget = budget;
        self
    }

    pub(super) fn with_schedule(mut self, schedule: IndexRetentionSchedule) -> Self {
        self.schedule = schedule;
        self
    }

    pub(super) fn has_active(&self) -> Result<bool, Status> {
        self.active
            .lock()
            .map_err(|_| Status::internal("deleted-index cleanup lock is poisoned"))
            .map(|active| active.is_some())
    }

    pub(super) fn oldest_due(&self) -> Result<Option<DeletedDefinitionCleanup>, Status> {
        self.store
            .oldest_deleted_definition_cleanup()
            .map_err(retention_due_status)
    }

    pub(super) async fn run_tick(&self) -> Result<u64, Status> {
        let mut active = self.take_active()?;
        if active.is_none() {
            let Some(due) = self.oldest_due()? else {
                return Ok(0);
            };
            if due.due_at_unix_millis > now_unix_millis()? {
                return Ok(0);
            }
            match self.load_job(due.clone()).await {
                Ok(job) => active = job,
                Err(error) => {
                    self.defer(&due)?;
                    return Err(error);
                }
            }
            if active.is_none() {
                return Ok(0);
            }
        }
        let mut active = active.expect("deleted-index cleanup job was loaded");
        if !self.due_matches(&active.due)? {
            return Ok(0);
        }
        if !self.artifacts.is_local_builder(
            active.due.tenant_id,
            active.due.bucket_id,
            active.due.index_id,
        )? {
            self.complete(&active.due)?;
            return Ok(0);
        }

        let mut work = RetentionWork::new(self.budget);
        let result = self.advance(&mut active, &mut work).await;
        match result {
            Ok(removed) if active.complete => {
                if let Some(due_at) = active.next_due_unix_millis {
                    let mut replacement = active.due.clone();
                    replacement.due_at_unix_millis = due_at;
                    self.store
                        .replace_deleted_definition_cleanup(&active.due, &replacement)
                        .map_err(retention_due_status)?;
                } else {
                    self.complete(&active.due)?;
                }
                self.record_tick(&active.due, &work, removed, false);
                Ok(removed)
            }
            Ok(removed) => {
                let due = active.due.clone();
                if self.due_matches(&active.due)? {
                    self.put_active(active)?;
                }
                self.record_tick(&due, &work, removed, false);
                Ok(removed)
            }
            Err(error) => {
                self.defer(&active.due)?;
                self.record_tick(&active.due, &work, 0, true);
                Err(error)
            }
        }
    }

    async fn load_job(
        &self,
        due: DeletedDefinitionCleanup,
    ) -> Result<Option<DeletedCleanupJob>, Status> {
        if !self.due_matches(&due)? {
            return Ok(None);
        }
        if !self
            .artifacts
            .is_local_builder(due.tenant_id, due.bucket_id, due.index_id)?
        {
            self.complete(&due)?;
            return Ok(None);
        }
        if self.deleted_definition_state(&due).await? == DeletedDefinitionState::ReusedNamespace {
            self.complete(&due)?;
            self.record_reused_namespace(&due);
            return Ok(None);
        }
        let (storage_tenant, bucket) = self
            .names
            .resolve_bucket_names(due.tenant_id, due.bucket_id)
            .await?;
        let scan = self.scanner.begin(IndexHeadScanScope {
            tenant_id: due.tenant_id,
            bucket_id: due.bucket_id,
            index_id: due.index_id,
        })?;
        Ok(Some(DeletedCleanupJob {
            due,
            storage_tenant,
            bucket,
            scan,
            pending: VecDeque::new(),
            next_due_unix_millis: None,
            complete: false,
        }))
    }

    async fn advance(
        &self,
        job: &mut DeletedCleanupJob,
        work: &mut RetentionWork,
    ) -> Result<u64, Status> {
        let mut removed = 0_u64;
        while work.has_room() && !job.complete {
            if let Some(candidate) = job.pending.pop_front() {
                let charge = candidate.path.len() as u64 + 64;
                if !work.can_charge(charge) {
                    job.pending.push_front(candidate);
                    break;
                }
                work.charge(charge);
                if let Some(due_at) = candidate.due_at_unix_millis {
                    job.next_due_unix_millis = Some(
                        job.next_due_unix_millis
                            .map_or(due_at, |current| current.min(due_at)),
                    );
                    continue;
                }
                if self.deleted_definition_state(&job.due).await?
                    == DeletedDefinitionState::ReusedNamespace
                {
                    job.pending.clear();
                    job.next_due_unix_millis = None;
                    job.complete = true;
                    self.record_reused_namespace(&job.due);
                    continue;
                }
                self.artifacts
                    .delete(IndexArtifactDelete {
                        storage_tenant: job.storage_tenant.clone(),
                        bucket: job.bucket.clone(),
                        tenant_id: job.due.tenant_id,
                        bucket_id: job.due.bucket_id,
                        index_id: job.due.index_id,
                        exact_path: candidate.path.clone(),
                        expected_version: candidate.version,
                        command_id: delete_command(
                            job.due.index_id,
                            candidate.version,
                            "deleted-definition",
                            &candidate.path,
                        ),
                        definition_intent: None,
                    })
                    .await?;
                removed = removed.saturating_add(1);
                continue;
            }

            let page = self.within(work, job.scan.next_page()).await?;
            let Some(heads) = page else {
                job.complete = true;
                continue;
            };
            let now = now_unix_millis()?;
            for head in heads {
                if let Some(candidate) = cleanup_candidate(head, now)? {
                    job.pending.push_back(candidate);
                }
            }
        }
        Ok(removed)
    }

    async fn deleted_definition_state(
        &self,
        due: &DeletedDefinitionCleanup,
    ) -> Result<DeletedDefinitionState, Status> {
        if !self.due_matches(due)? {
            return Err(Status::aborted(
                "deleted-index cleanup schedule changed before exact action",
            ));
        }
        if !self
            .artifacts
            .is_local_builder(due.tenant_id, due.bucket_id, due.index_id)?
        {
            return Err(Status::aborted(
                "deleted-index cleanup builder changed before exact action",
            ));
        }
        let locator = self
            .store
            .definition_locator(
                DefinitionKind::Index,
                due.tenant_id,
                due.bucket_id,
                &due.definition_path,
            )
            .map_err(|error| Status::unavailable(error.to_string()))?
            .ok_or_else(|| Status::unavailable("deleted index locator is unavailable"))?;
        let state = classify_deleted_definition_locator(&locator, due)?;
        if state != DeletedDefinitionState::ExactDelete {
            return Ok(state);
        }
        if !definition_reference_matches(
            &self.reader,
            due.tenant_id,
            due.bucket_id,
            &due.definition_path,
            due.definition_object_version,
            DefinitionOperation::Delete,
        )
        .await?
        {
            return Err(Status::unavailable(
                "deleted index tombstone is not yet exact-readable",
            ));
        }
        Ok(DeletedDefinitionState::ExactDelete)
    }

    async fn within<T>(
        &self,
        work: &RetentionWork,
        operation: impl std::future::Future<Output = Result<T, Status>>,
    ) -> Result<T, Status> {
        let remaining = work.remaining().ok_or_else(|| {
            Status::deadline_exceeded("deleted-index cleanup exhausted its tick time budget")
        })?;
        tokio::time::timeout(remaining, operation)
            .await
            .map_err(|_| Status::deadline_exceeded("deleted-index cleanup operation timed out"))?
    }

    fn take_active(&self) -> Result<Option<DeletedCleanupJob>, Status> {
        self.active
            .lock()
            .map_err(|_| Status::internal("deleted-index cleanup lock is poisoned"))
            .map(|mut active| active.take())
    }

    fn put_active(&self, job: DeletedCleanupJob) -> Result<(), Status> {
        let mut active = self
            .active
            .lock()
            .map_err(|_| Status::internal("deleted-index cleanup lock is poisoned"))?;
        *active = Some(job);
        Ok(())
    }

    fn due_matches(&self, due: &DeletedDefinitionCleanup) -> Result<bool, Status> {
        self.store
            .deleted_definition_cleanup_matches(due)
            .map_err(retention_due_status)
    }

    fn complete(&self, due: &DeletedDefinitionCleanup) -> Result<(), Status> {
        self.store
            .complete_deleted_definition_cleanup(due)
            .map(|_| ())
            .map_err(retention_due_status)
    }

    fn defer(&self, due: &DeletedDefinitionCleanup) -> Result<(), Status> {
        let retry_millis =
            u64::try_from(self.schedule.retry_interval.as_millis()).unwrap_or(u64::MAX);
        let mut replacement = due.clone();
        replacement.due_at_unix_millis = now_unix_millis()?.saturating_add(retry_millis);
        self.store
            .replace_deleted_definition_cleanup(due, &replacement)
            .map(|_| ())
            .map_err(retention_due_status)
    }

    fn record_tick(
        &self,
        due: &DeletedDefinitionCleanup,
        work: &RetentionWork,
        removed: u64,
        failed: bool,
    ) {
        tracing::debug!(
            index.id = due.index_id,
            cleanup.records = work.records as u64,
            cleanup.bytes = work.bytes,
            cleanup.failed = failed,
            monotonic_counter.keldra_index_deleted_artifacts_total = removed,
            monotonic_counter.keldra_index_deleted_cleanup_errors_total = u64::from(failed),
            "bounded deleted-index cleanup tick completed"
        );
    }

    fn record_reused_namespace(&self, due: &DeletedDefinitionCleanup) {
        tracing::debug!(
            index.id = due.index_id,
            definition.version = due.definition_object_version.0,
            monotonic_counter.keldra_index_deleted_cleanup_reused_namespace_total = 1_u64,
            "retired deleted-index cleanup because a newer definition reused its artifact namespace"
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeletedDefinitionState {
    ExactDelete,
    SupersededPath,
    ReusedNamespace,
}

fn classify_deleted_definition_locator(
    locator: &DefinitionLocator,
    due: &DeletedDefinitionCleanup,
) -> Result<DeletedDefinitionState, Status> {
    if locator.kind != DefinitionKind::Index
        || locator.tenant_id != due.tenant_id
        || locator.bucket_id != due.bucket_id
        || locator.path != due.definition_path
    {
        return Err(Status::data_loss(
            "deleted index locator identity is inconsistent",
        ));
    }
    if locator.object_version < due.definition_object_version {
        return Err(Status::unavailable(
            "deleted index locator has not reached the cleanup revision",
        ));
    }
    if locator.object_version == due.definition_object_version {
        if locator.operation == DefinitionOperation::Delete && locator.definition_id == due.index_id
        {
            return Ok(DeletedDefinitionState::ExactDelete);
        }
        return Err(Status::data_loss(
            "deleted index locator does not match its cleanup revision",
        ));
    }
    if locator.definition_id == due.index_id {
        Ok(DeletedDefinitionState::ReusedNamespace)
    } else {
        Ok(DeletedDefinitionState::SupersededPath)
    }
}

struct DeletedCleanupJob {
    due: DeletedDefinitionCleanup,
    storage_tenant: String,
    bucket: String,
    scan: ClusterIndexScan,
    pending: VecDeque<CleanupCandidate>,
    next_due_unix_millis: Option<u64>,
    complete: bool,
}

#[derive(Clone)]
struct CleanupCandidate {
    path: String,
    version: VersionId,
    due_at_unix_millis: Option<u64>,
}

fn cleanup_candidate(
    head: IndexCurrentHead,
    now_unix_millis: u64,
) -> Result<Option<CleanupCandidate>, Status> {
    if head.version.deleted {
        return Ok(None);
    }
    if head.version.blob.is_none() {
        return Err(Status::data_loss(
            "live deleted-index artifact has no blob reference",
        ));
    }
    let eligible_at = head
        .version
        .committed_at_unix_millis
        .saturating_add(UNREACHABLE_ARTIFACT_SAFETY_MILLIS);
    Ok(Some(CleanupCandidate {
        path: head.exact_path,
        version: head.version.id,
        due_at_unix_millis: (now_unix_millis < eligible_at).then_some(eligible_at),
    }))
}

#[cfg(test)]
mod tests {
    use keldra_store::{BlobRef, Head, Version};

    use super::*;

    fn head(committed_at_unix_millis: u64, deleted: bool) -> IndexCurrentHead {
        IndexCurrentHead {
            tenant_id: 1,
            bucket_id: 2,
            exact_path: "_keldra/indices/v4/3/current".into(),
            head: Head {
                version: VersionId(4),
                deleted,
                mutation_stamp: None,
            },
            version: Version {
                id: VersionId(4),
                blob: (!deleted).then_some(BlobRef {
                    hash: [5; 32],
                    length: 9,
                }),
                content_type: None,
                deleted,
                committed_at_unix_millis,
            },
            versions: Vec::new(),
        }
    }

    fn cleanup() -> DeletedDefinitionCleanup {
        DeletedDefinitionCleanup {
            tenant_id: 1,
            bucket_id: 2,
            index_id: 3,
            definition_path: "_keldra/indexes/by-path".into(),
            definition_object_version: VersionId(4),
            due_at_unix_millis: 5,
        }
    }

    fn locator() -> DefinitionLocator {
        DefinitionLocator {
            kind: DefinitionKind::Index,
            tenant_id: 1,
            bucket_id: 2,
            definition_id: 3,
            path: "_keldra/indexes/by-path".into(),
            object_version: VersionId(4),
            operation: DefinitionOperation::Delete,
        }
    }

    #[test]
    fn exact_delete_locator_allows_cleanup() {
        assert_eq!(
            classify_deleted_definition_locator(&locator(), &cleanup()).unwrap(),
            DeletedDefinitionState::ExactDelete
        );
    }

    #[test]
    fn recreated_definition_supersedes_old_cleanup() {
        let mut current = locator();
        current.definition_id = 9;
        current.object_version = VersionId(6);
        current.operation = DefinitionOperation::Upsert;
        assert_eq!(
            classify_deleted_definition_locator(&current, &cleanup()).unwrap(),
            DeletedDefinitionState::SupersededPath
        );
    }

    #[test]
    fn recreated_definition_reusing_index_id_retires_old_cleanup() {
        let mut current = locator();
        current.object_version = VersionId(6);
        current.operation = DefinitionOperation::Upsert;
        assert_eq!(
            classify_deleted_definition_locator(&current, &cleanup()).unwrap(),
            DeletedDefinitionState::ReusedNamespace
        );
    }

    #[test]
    fn stale_or_inconsistent_locators_still_fail_closed() {
        let mut stale = locator();
        stale.object_version = VersionId(3);
        assert_eq!(
            classify_deleted_definition_locator(&stale, &cleanup())
                .unwrap_err()
                .code(),
            tonic::Code::Unavailable
        );

        let mut inconsistent = locator();
        inconsistent.definition_id = 9;
        assert_eq!(
            classify_deleted_definition_locator(&inconsistent, &cleanup())
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );
    }

    #[test]
    fn young_artifact_is_rescheduled_at_the_exact_safety_age() {
        let candidate = cleanup_candidate(head(100, false), 200).unwrap().unwrap();
        assert_eq!(
            candidate.due_at_unix_millis,
            Some(100 + UNREACHABLE_ARTIFACT_SAFETY_MILLIS)
        );
    }

    #[test]
    fn eligible_artifact_and_tombstone_are_distinguished() {
        let now = UNREACHABLE_ARTIFACT_SAFETY_MILLIS + 101;
        assert_eq!(
            cleanup_candidate(head(100, false), now)
                .unwrap()
                .unwrap()
                .due_at_unix_millis,
            None
        );
        assert!(cleanup_candidate(head(100, true), now).unwrap().is_none());
    }
}
