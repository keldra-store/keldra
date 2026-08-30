//! Node-wide bounded retention of ordinary format-v4 index artifacts.

use std::collections::VecDeque;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use keldra_index::v4::{ArtifactPackReference, INDEX_COMPONENT_BYTES};
use keldra_store::{DefinitionKind, IndexCommitRetentionDue, ObjectKey, Store, VersionId};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::index_config::IndexRuntimeConfig;
use crate::index_service::{StoredIndexDefinition, definition_path};
use crate::logical_name_resolution::LogicalNameResolver;

use super::cache::IndexMergeScratchSpace;
use super::catalog::CatalogDefinition;
use super::committed_view::{
    CommitManifestReference, IndexCommitManifest, IndexCurrentPointer, LocatorPackOwnership,
    ReleasingManifestReference,
};
use super::coordination::load_definition_locator_object;
use super::publication::{
    IndexArtifactDelete, IndexArtifactRouter, IndexCurrentMutationGuard, artifact_path,
    manifest_path,
};
use super::publisher::{CommittedIndexView, IndexCommitPublisher};
use super::scanner::ClusterIndexScanner;

#[path = "retention/orphans.rs"]
mod orphans;
use orphans::IndexOrphanScrub;

#[path = "retention/deleted.rs"]
mod deleted;
#[path = "retention/scratch.rs"]
mod scratch;
use deleted::DeletedDefinitionRetention;
#[cfg(test)]
use scratch::RETENTION_COMMIT_SLOTS;
use scratch::{
    RetainedObjectCollector, RetainedObjectProof, RetainedObjectRecord, RetainedObjectSort,
};

const UNREACHABLE_ARTIFACT_SAFETY_MILLIS: u64 = 24 * 60 * 60 * 1_000;
const MAX_RETENTION_RECORD_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_RETENTION_TICK_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_RETENTION_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const RETAINED_MANIFEST_CLASS: u8 = 1;
const RETAINED_ARTIFACT_CLASS: u8 = 2;
const RELEASED_MANIFEST_CLASS: u8 = 3;
const RELEASED_ARTIFACT_CLASS: u8 = 4;

#[derive(Clone, Copy, Debug)]
pub(crate) struct IndexRetentionBudget {
    pub(crate) max_records: u32,
    pub(crate) max_bytes: u64,
    pub(crate) max_time: Duration,
}

impl IndexRetentionBudget {
    pub(crate) fn new(
        max_records: u32,
        max_bytes: u64,
        max_time: Duration,
    ) -> Result<Self, Status> {
        if max_records == 0 || max_bytes < MAX_RETENTION_RECORD_BYTES || max_time.is_zero() {
            return Err(Status::invalid_argument(
                "index retention budgets cannot represent one bounded artifact record",
            ));
        }
        Ok(Self {
            max_records,
            max_bytes,
            max_time,
        })
    }
}

impl Default for IndexRetentionBudget {
    fn default() -> Self {
        Self {
            max_records: 128,
            max_bytes: MAX_RETENTION_RECORD_BYTES,
            max_time: Duration::from_secs(1),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct IndexRetentionSchedule {
    pub(crate) tick_interval: Duration,
    pub(crate) retry_interval: Duration,
}

impl IndexRetentionSchedule {
    pub(crate) fn new(tick_interval: Duration, retry_interval: Duration) -> Result<Self, Status> {
        if tick_interval.is_zero() || retry_interval.is_zero() {
            return Err(Status::invalid_argument(
                "index retention scheduler intervals must be positive",
            ));
        }
        Ok(Self {
            tick_interval,
            retry_interval,
        })
    }
}

impl Default for IndexRetentionSchedule {
    fn default() -> Self {
        Self {
            tick_interval: DEFAULT_RETENTION_TICK_INTERVAL,
            retry_interval: DEFAULT_RETENTION_RETRY_INTERVAL,
        }
    }
}

#[derive(Clone)]
pub(crate) struct IndexCommitRetention {
    store: Store,
    reader: ClusterObjectReader,
    artifacts: IndexArtifactRouter,
    publisher: IndexCommitPublisher,
    scratch: IndexMergeScratchSpace,
    config: IndexRuntimeConfig,
    budget: IndexRetentionBudget,
    schedule: IndexRetentionSchedule,
    active: Arc<Mutex<Option<ActiveRetentionJob>>>,
    deleted: DeletedDefinitionRetention,
    orphans: IndexOrphanScrub,
    run_lock: Arc<tokio::sync::Mutex<()>>,
}

impl IndexCommitRetention {
    pub(crate) fn new(
        store: Store,
        scanner: ClusterIndexScanner,
        reader: ClusterObjectReader,
        artifacts: IndexArtifactRouter,
        publisher: IndexCommitPublisher,
        scratch: IndexMergeScratchSpace,
        names: LogicalNameResolver,
        config: IndexRuntimeConfig,
    ) -> Self {
        let budget = IndexRetentionBudget::default();
        let schedule = IndexRetentionSchedule::default();
        Self {
            orphans: IndexOrphanScrub::new(
                store.clone(),
                scanner.clone(),
                reader.clone(),
                artifacts.clone(),
                publisher.clone(),
                scratch.clone(),
            ),
            deleted: DeletedDefinitionRetention::new(
                store.clone(),
                scanner.clone(),
                reader.clone(),
                artifacts.clone(),
                names,
                budget,
                schedule,
            ),
            store,
            reader,
            artifacts,
            publisher,
            scratch,
            config,
            budget,
            schedule,
            active: Arc::new(Mutex::new(None)),
            run_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub(crate) fn with_budget(mut self, budget: IndexRetentionBudget) -> Self {
        self.budget = budget;
        self.deleted = self.deleted.with_budget(budget);
        self.orphans = self.orphans.with_budget(budget);
        self
    }

    pub(crate) fn with_schedule(mut self, schedule: IndexRetentionSchedule) -> Self {
        self.schedule = schedule;
        self.deleted = self.deleted.with_schedule(schedule);
        self
    }

    /// Durably schedule the published revision immediately. The current
    /// pointer remains the sole revision authority; this sparse record is
    /// only restart-safe evidence that bounded retention work is due.
    pub(crate) fn schedule(
        &self,
        definition: &CatalogDefinition,
        current: &CommittedIndexView,
    ) -> Result<(), Status> {
        let physical = definition.physical_stored();
        require_current_identity(&physical, current)?;
        let due = commit_due(
            &physical,
            definition.tenant_id,
            definition.bucket_id,
            definition.object_version,
            current.manifest.revision,
            now_unix_millis()?,
        )?;
        self.store
            .schedule_index_commit_retention(&due)
            .map_err(retention_due_status)?;
        self.orphans.schedule_if_absent(
            &physical,
            definition.tenant_id,
            definition.bucket_id,
            definition.object_version,
        )
    }

    pub(crate) fn unschedule(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        index_id: u64,
    ) -> Result<(), Status> {
        let mut active = self
            .active
            .lock()
            .map_err(|_| Status::internal("index retention active-job lock is poisoned"))?;
        if active.as_ref().is_some_and(|active| {
            active.due.tenant_id == tenant_id
                && active.due.bucket_id == bucket_id
                && active.due.index_id == index_id
        }) {
            *active = None;
        }
        self.store
            .cancel_index_commit_retention(tenant_id, bucket_id, index_id)
            .map_err(retention_due_status)?;
        self.store
            .cancel_index_orphan_scrub(tenant_id, bucket_id, index_id)
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(())
    }

    pub(crate) fn start_scheduler(&self) -> IndexRetentionTask {
        let retention = self.clone();
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(retention.schedule.tick_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Maintenance never delays serving or inventories artifacts at
            // startup. Definitions schedule work as builders load.
            interval.tick().await;
            loop {
                interval.tick().await;
                if let Err(error) = retention.run_tick().await {
                    tracing::debug!(%error, "bounded index retention tick will retry");
                }
            }
        });
        IndexRetentionTask { task }
    }

    async fn run_tick(&self) -> Result<u64, Status> {
        let _run = self.run_lock.lock().await;
        if self.deleted.has_active()? {
            return self.deleted.run_tick().await;
        }
        if self.has_active()? {
            return self.run_commit_tick().await;
        }
        let revision = self
            .store
            .oldest_index_commit_retention_due()
            .map_err(retention_due_status)?;
        let deleted = self.deleted.oldest_due()?;
        let orphan = self.orphans.oldest_due()?;
        let commit_due = revision.as_ref().map(|due| due.due_at_unix_millis);
        let deleted_due = deleted.as_ref().map(|due| due.due_at_unix_millis);
        let orphan_due = orphan.as_ref().map(|due| due.due_at_unix_millis);
        let oldest = [
            commit_due.map(|due| (due, 0_u8)),
            deleted_due.map(|due| (due, 1_u8)),
            orphan_due.map(|due| (due, 2_u8)),
        ]
        .into_iter()
        .flatten()
        .min();
        match oldest.map(|(_, kind)| kind) {
            None => Ok(0),
            Some(0) => self.run_commit_tick().await,
            Some(1) => self.deleted.run_tick().await,
            Some(2) => self.orphans.run_tick().await,
            Some(_) => unreachable!("maintenance kind is bounded"),
        }
    }

    async fn run_commit_tick(&self) -> Result<u64, Status> {
        let mut active = self.take_active()?;
        if active.is_none() {
            let Some(due) = self
                .store
                .oldest_index_commit_retention_due()
                .map_err(retention_due_status)?
            else {
                return Ok(0);
            };
            let now = now_unix_millis()?;
            if due.due_at_unix_millis > now {
                return Ok(0);
            }
            match self.load_due_job(due.clone()).await {
                Ok(Some(job)) => active = Some(ActiveRetentionJob { due, job }),
                Ok(None) => return Ok(0),
                Err(error) => {
                    self.defer_due(&due)?;
                    return Err(error);
                }
            }
        }
        let mut active = active.expect("due retention job was loaded");
        let index_id = active.due.index_id;
        let Some(latest) = self
            .store
            .index_commit_retention_due(
                active.due.tenant_id,
                active.due.bucket_id,
                active.due.index_id,
            )
            .map_err(retention_due_status)?
        else {
            return Ok(0);
        };
        if !retention_job_matches_schedule(
            &active.due,
            active.job.current.manifest.revision,
            &latest,
        ) {
            if active.job.release_cleanup_in_progress() {
                // The candidate set is fixed by the releasing roots already
                // discovered. A newer commit can only add protection, which
                // is revalidated under the current-mutation gate immediately
                // before every delete. Do not restart expensive proof work.
                active.due = latest;
            } else {
                let now = now_unix_millis()?;
                if latest.due_at_unix_millis > now {
                    active.due = latest;
                    self.put_active(active)?;
                    return Ok(0);
                }
                let Some(next_job) = self.load_due_job(latest.clone()).await? else {
                    return Ok(0);
                };
                active = ActiveRetentionJob {
                    due: latest,
                    job: next_job,
                };
            }
        }
        let mut work = RetentionWork::new(self.budget);
        let result = self
            .advance_job(&active.due, &mut active.job, &mut work)
            .await;
        match result {
            Ok(removed) => {
                if matches!(active.job.phase, RetentionPhase::Complete) {
                    if !self.finish_due(&mut active, &mut work).await? {
                        self.put_active(active)?;
                    }
                } else {
                    self.put_active(active)?;
                }
                let (backlog, oldest_millis) = self.backlog()?;
                tracing::debug!(
                    index.id = index_id,
                    gauge.keldra_index_retention_tick_records = work.records as u64,
                    gauge.keldra_index_retention_tick_bytes = work.bytes,
                    gauge.keldra_index_retention_backlog = backlog as u64,
                    gauge.keldra_index_retention_oldest_pending_millis = oldest_millis,
                    monotonic_counter.keldra_index_retention_artifacts_deleted_total = removed,
                    "bounded node-wide index retention tick completed"
                );
                Ok(removed)
            }
            Err(error) => {
                self.defer_due(&active.due)?;
                if let Some(latest) = self
                    .store
                    .index_commit_retention_due(
                        active.due.tenant_id,
                        active.due.bucket_id,
                        active.due.index_id,
                    )
                    .map_err(retention_due_status)?
                {
                    active.due = latest;
                }
                self.put_active(active)?;
                let (backlog, oldest_millis) = self.backlog()?;
                tracing::debug!(
                    index.id = index_id,
                    gauge.keldra_index_retention_backlog = backlog as u64,
                    gauge.keldra_index_retention_oldest_pending_millis = oldest_millis,
                    monotonic_counter.keldra_index_retention_errors_total = 1_u64,
                    %error,
                    "bounded index retention work failed"
                );
                Err(error)
            }
        }
    }

    fn take_active(&self) -> Result<Option<ActiveRetentionJob>, Status> {
        self.active
            .lock()
            .map_err(|_| Status::internal("index retention active-job lock is poisoned"))
            .map(|mut active| active.take())
    }

    fn has_active(&self) -> Result<bool, Status> {
        self.active
            .lock()
            .map_err(|_| Status::internal("index retention active-job lock is poisoned"))
            .map(|active| active.is_some())
    }

    fn put_active(&self, job: ActiveRetentionJob) -> Result<(), Status> {
        let mut active = self
            .active
            .lock()
            .map_err(|_| Status::internal("index retention active-job lock is poisoned"))?;
        *active = Some(job);
        Ok(())
    }

    async fn load_due_job(
        &self,
        due: IndexCommitRetentionDue,
    ) -> Result<Option<RetentionJob>, Status> {
        if !self
            .store
            .index_commit_retention_due_matches(&due)
            .map_err(retention_due_status)?
        {
            return Ok(None);
        }
        let Some(locator) = self
            .store
            .definition_locator(
                DefinitionKind::Index,
                due.tenant_id,
                due.bucket_id,
                &due.definition_path,
            )
            .map_err(|error| Status::unavailable(error.to_string()))?
        else {
            return Err(Status::failed_precondition(
                "scheduled index definition is no longer live",
            ));
        };
        let Some(object) = load_definition_locator_object(&self.reader, &locator).await? else {
            return Err(Status::unavailable(
                "scheduled index definition changed during exact read",
            ));
        };
        let definition = StoredIndexDefinition::decode(&object.bytes)?;
        if definition.index_id != locator.definition_id
            || definition_path(&definition.name)? != due.definition_path
        {
            return Err(Status::data_loss(
                "scheduled retention definition identity is inconsistent",
            ));
        }
        let catalog = CatalogDefinition::new(
            due.tenant_id,
            due.bucket_id,
            locator.object_version.0,
            definition,
        )?;
        if catalog.physical_index_id() != due.index_id {
            return Err(Status::data_loss(
                "scheduled retention physical identity is inconsistent",
            ));
        }
        let definition = catalog.physical_stored();
        let current = self
            .publisher
            .load_current(&definition, due.tenant_id, due.bucket_id)
            .await?
            .ok_or_else(|| Status::unavailable("scheduled index has no current committed view"))?;
        if current.manifest.definition_version != catalog.physical_definition_version() {
            return Err(Status::unavailable(
                "index definition publication has not reached its current revision",
            ));
        }
        if locator.object_version != due.definition_object_version
            || current.manifest.revision != due.commit_revision
        {
            let replacement = commit_due(
                &definition,
                due.tenant_id,
                due.bucket_id,
                locator.object_version.0,
                current.manifest.revision,
                now_unix_millis()?,
            )?;
            self.store
                .schedule_index_commit_retention(&replacement)
                .map_err(retention_due_status)?;
            return Ok(None);
        }
        RetentionJob::new(
            definition,
            due.tenant_id,
            due.bucket_id,
            locator.object_version,
            current,
        )
        .map(Some)
    }

    async fn finish_due(
        &self,
        active: &mut ActiveRetentionJob,
        work: &mut RetentionWork,
    ) -> Result<bool, Status> {
        if !active.job.roots_unlinked && !active.job.cleanup_roots.is_empty() {
            let completed = active.job.cleanup_roots.clone();
            active.job.current = self
                .within(
                    work,
                    self.publisher.finish_releasing(
                        &active.job.definition,
                        active.job.tenant_id,
                        active.job.bucket_id,
                        &completed,
                        active.job.definition_object_version,
                    ),
                )
                .await?;
            work.charge(INDEX_COMPONENT_BYTES as u64);
            active.job.roots_unlinked = true;
        }
        while active.job.roots_unlinked && work.has_room() {
            let Some(next) = active.job.cleanup_roots.last() else {
                break;
            };
            if !work.can_charge(next.manifest.blob.length) {
                break;
            }
            let Some(released) = active.job.cleanup_roots.pop() else {
                break;
            };
            let encoded_bytes = released.manifest.blob.length;
            let result = self
                .within(
                    work,
                    self.delete_unlinked_manifest_if_still_unlinked(&active.job, &released),
                )
                .await;
            work.charge(encoded_bytes);
            if let Err(error) = result {
                tracing::debug!(
                    index.id = active.job.definition.index_id,
                    manifest.path = released.manifest.path,
                    %error,
                    "unlinked releasing manifest will be reclaimed by orphan scrub"
                );
            }
        }
        if !active.job.cleanup_roots.is_empty() {
            return Ok(false);
        }
        let Some(scheduled) = self
            .store
            .index_commit_retention_due(
                active.due.tenant_id,
                active.due.bucket_id,
                active.due.index_id,
            )
            .map_err(retention_due_status)?
        else {
            return Ok(true);
        };
        let next_age_due = minimum_due(
            active.job.next_due_unix_millis,
            next_age_due(&active.job.current.pointer, self.config),
        );
        let due_at = completion_due(
            &active.job.current.pointer,
            next_age_due,
            now_unix_millis()?,
        );
        if let Some(due_at) = due_at {
            let replacement = commit_due(
                &active.job.definition,
                active.job.tenant_id,
                active.job.bucket_id,
                active.job.definition_object_version.0,
                active.job.current.manifest.revision,
                due_at,
            )?;
            self.store
                .replace_index_commit_retention_due(&scheduled, &replacement)
                .map_err(retention_due_status)?;
        } else {
            self.store
                .complete_index_commit_retention_due(&scheduled)
                .map_err(retention_due_status)?;
        }
        Ok(true)
    }

    fn defer_due(&self, due: &IndexCommitRetentionDue) -> Result<(), Status> {
        let retry_millis =
            u64::try_from(self.schedule.retry_interval.as_millis()).unwrap_or(u64::MAX);
        let mut replacement = due.clone();
        replacement.due_at_unix_millis = now_unix_millis()?.saturating_add(retry_millis);
        self.store
            .replace_index_commit_retention_due(due, &replacement)
            .map_err(retention_due_status)?;
        Ok(())
    }

    fn backlog(&self) -> Result<(usize, u64), Status> {
        let Some(oldest) = self
            .store
            .oldest_index_commit_retention_due()
            .map_err(retention_due_status)?
        else {
            return Ok((0, 0));
        };
        Ok((
            1,
            now_unix_millis()?.saturating_sub(oldest.due_at_unix_millis),
        ))
    }

    fn require_due(&self, expected: &IndexCommitRetentionDue) -> Result<(), Status> {
        if self
            .store
            .index_commit_retention_due_matches(expected)
            .map_err(retention_due_status)?
        {
            Ok(())
        } else {
            Err(Status::aborted(
                "index retention schedule changed before exact action",
            ))
        }
    }

    async fn advance_job(
        &self,
        due: &IndexCommitRetentionDue,
        job: &mut RetentionJob,
        work: &mut RetentionWork,
    ) -> Result<u64, Status> {
        let mut removed = 0_u64;
        while work.has_room() && !matches!(job.phase, RetentionPhase::Complete) {
            let phase = std::mem::replace(&mut job.phase, RetentionPhase::Complete);
            let phase_before = std::mem::discriminant(&phase);
            let records_before = work.records;
            let bytes_before = work.bytes;
            let advanced = match phase {
                RetentionPhase::Initialize => self.advance_initialize(due, job, work).await,
                RetentionPhase::Discover(discovery) => {
                    self.advance_discovery(job, discovery, work).await
                }
                RetentionPhase::Sort(sort) => self.advance_sort(job, sort, work).await,
                RetentionPhase::Sweep(sweep) => self.advance_sweep(job, sweep, work).await,
                RetentionPhase::Complete => Ok((RetentionPhase::Complete, 0)),
            };
            let (next, deleted) = match advanced {
                Ok(advanced) => advanced,
                Err(error) => {
                    job.phase = error.phase;
                    return Err(error.status);
                }
            };
            let phase_changed = phase_before != std::mem::discriminant(&next);
            job.phase = next;
            removed = removed.saturating_add(deleted);
            if !phase_changed && work.records == records_before && work.bytes == bytes_before {
                break;
            }
        }
        Ok(removed)
    }

    async fn advance_initialize(
        &self,
        due: &IndexCommitRetentionDue,
        job: &mut RetentionJob,
        work: &mut RetentionWork,
    ) -> RetentionStepResult {
        let selected = match self
            .publisher
            .metadata_retained(&job.current, SystemTime::now())
        {
            Ok(selected) => selected,
            Err(error) => {
                return Err(RetentionStepError::new(RetentionPhase::Initialize, error));
            }
        };
        if selected != job.current.pointer.retained {
            if let Err(error) = self.require_due(due) {
                return Err(RetentionStepError::new(RetentionPhase::Initialize, error));
            }
            let trimmed = match self
                .within(
                    work,
                    self.publisher.trim_retained(
                        &job.definition,
                        job.tenant_id,
                        job.bucket_id,
                        &job.current,
                        selected,
                        job.definition_object_version,
                    ),
                )
                .await
            {
                Ok(trimmed) => trimmed,
                Err(error) => {
                    return Err(RetentionStepError::new(RetentionPhase::Initialize, error));
                }
            };
            job.current = trimmed;
            work.charge(INDEX_COMPONENT_BYTES as u64);
        }
        let released = job.current.pointer.releasing.clone();
        job.cleanup_roots = released.clone();
        job.next_due_unix_millis = next_age_due(&job.current.pointer, self.config);
        let discovery = match self
            .within(
                work,
                RetentionDiscovery::new(&job.current, released, self.scratch.clone()),
            )
            .await
        {
            Ok(discovery) => discovery,
            Err(error) => {
                return Err(RetentionStepError::new(RetentionPhase::Initialize, error));
            }
        };
        Ok((RetentionPhase::Discover(discovery), 0))
    }

    async fn advance_sort(
        &self,
        job: &mut RetentionJob,
        mut sort: RetentionSort,
        work: &RetentionWork,
    ) -> RetentionStepResult {
        if sort.protected_proof.is_none() {
            match sort.protected.advance(work.deadline()).await {
                Ok(Some(proof)) => {
                    sort.protected_proof = Some(proof);
                    return Ok((RetentionPhase::Sort(sort), 0));
                }
                Ok(None) => return Ok((RetentionPhase::Sort(sort), 0)),
                Err(error) => {
                    return Err(RetentionStepError::new(RetentionPhase::Sort(sort), error));
                }
            }
        }
        match sort.released.advance(work.deadline()).await {
            Ok(Some(released)) => {
                job.cleanup_roots = sort.releasing_roots;
                Ok((
                    RetentionPhase::Sweep(RetentionSweep::new(
                        sort.protected_proof
                            .take()
                            .expect("protected proof completed before released proof"),
                        job.current.pointer.retained.len(),
                        released,
                    )),
                    0,
                ))
            }
            Ok(None) => Ok((RetentionPhase::Sort(sort), 0)),
            Err(error) => Err(RetentionStepError::new(RetentionPhase::Sort(sort), error)),
        }
    }

    async fn advance_discovery(
        &self,
        job: &RetentionJob,
        mut discovery: RetentionDiscovery,
        work: &mut RetentionWork,
    ) -> RetentionStepResult {
        if let Some(mut pending) = discovery.pending_manifests.pop_front() {
            if !work.can_charge(INDEX_COMPONENT_BYTES as u64) {
                discovery.pending_manifests.push_front(pending);
                return Ok((RetentionPhase::Discover(discovery), 0));
            }
            let loaded = match pending.loaded.take() {
                Some(manifest) => LoadedManifest {
                    encoded_bytes: pending.reference.blob.length,
                    manifest,
                },
                None => match self
                    .within(work, self.load_manifest(job, &pending.reference))
                    .await
                {
                    Ok(loaded) => loaded,
                    Err(error) => {
                        discovery.pending_manifests.push_front(pending);
                        return Err(RetentionStepError::new(
                            RetentionPhase::Discover(discovery),
                            error,
                        ));
                    }
                },
            };
            work.charge(loaded.encoded_bytes);
            if let Err(error) = discovery
                .protect_manifest(pending.rank, &pending.reference, &loaded.manifest)
                .await
            {
                pending.loaded = Some(loaded.manifest);
                discovery.pending_manifests.push_front(pending);
                return Err(RetentionStepError::new(
                    RetentionPhase::Discover(discovery),
                    error,
                ));
            }
            return Ok((RetentionPhase::Discover(discovery), 0));
        }

        if let Some(pending) = discovery.pending_released.pop_front() {
            if !work.can_charge(INDEX_COMPONENT_BYTES as u64) {
                discovery.pending_released.push_front(pending);
                return Ok((RetentionPhase::Discover(discovery), 0));
            }
            let loaded = match self
                .within(work, self.load_manifest(job, &pending.manifest))
                .await
            {
                Ok(loaded) => loaded,
                Err(error) => {
                    discovery.pending_released.push_front(pending);
                    return Err(RetentionStepError::new(
                        RetentionPhase::Discover(discovery),
                        error,
                    ));
                }
            };
            work.charge(loaded.encoded_bytes);
            if let Err(error) = discovery.collect_released(&pending, &loaded.manifest).await {
                discovery.pending_released.push_front(pending);
                return Err(RetentionStepError::new(
                    RetentionPhase::Discover(discovery),
                    error,
                ));
            }
            return Ok((RetentionPhase::Discover(discovery), 0));
        }

        tracing::debug!(
            index.id = job.definition.index_id,
            "format-v4 retained proof and exact released roots collected into bounded scratch"
        );
        if discovery.releasing_roots.is_empty() {
            return Ok((RetentionPhase::Complete, 0));
        }
        Ok((
            RetentionPhase::Sort(RetentionSort {
                protected: discovery.collector.into_sort(),
                protected_proof: None,
                released: discovery.released_collector.into_sort(),
                releasing_roots: discovery.releasing_roots,
            }),
            0,
        ))
    }

    async fn advance_sweep(
        &self,
        job: &mut RetentionJob,
        mut sweep: RetentionSweep,
        work: &mut RetentionWork,
    ) -> RetentionStepResult {
        if let Some(candidate) = sweep.pending.pop_front() {
            let bytes = candidate.path.len() as u64 + 64;
            if !work.can_charge(bytes) {
                sweep.pending.push_front(candidate);
                return Ok((RetentionPhase::Sweep(sweep), 0));
            }
            work.charge(bytes);
            job.next_due_unix_millis = minimum_due(job.next_due_unix_millis, candidate.due_at);
            if candidate.delete {
                let current_guard = match self
                    .within(
                        work,
                        self.artifacts
                            .acquire_current_mutation(job.definition.index_id),
                    )
                    .await
                {
                    Ok(guard) => guard,
                    Err(error) => {
                        sweep.pending.push_front(candidate);
                        return Err(RetentionStepError::new(RetentionPhase::Sweep(sweep), error));
                    }
                };
                let newest = match self
                    .within(
                        work,
                        self.publisher
                            .load_current(&job.definition, job.tenant_id, job.bucket_id),
                    )
                    .await
                {
                    Ok(Some(current)) => current,
                    Ok(None) => {
                        sweep.pending.push_front(candidate);
                        return Err(RetentionStepError::new(
                            RetentionPhase::Sweep(sweep),
                            Status::aborted("current pointer disappeared before retention delete"),
                        ));
                    }
                    Err(error) => {
                        sweep.pending.push_front(candidate);
                        return Err(RetentionStepError::new(RetentionPhase::Sweep(sweep), error));
                    }
                };
                match self
                    .within(work, self.candidate_is_live(job, &newest, &candidate))
                    .await
                {
                    Ok(true) => {
                        drop(current_guard);
                        return Ok((RetentionPhase::Sweep(sweep), 0));
                    }
                    Ok(false) => {}
                    Err(error) => {
                        drop(current_guard);
                        sweep.pending.push_front(candidate);
                        return Err(RetentionStepError::new(RetentionPhase::Sweep(sweep), error));
                    }
                }
                job.current = newest;
                if let Err(error) = self
                    .within(
                        work,
                        self.delete_exact(
                            job,
                            &current_guard,
                            &candidate.path,
                            candidate.version,
                            candidate.class,
                            candidate.length,
                        ),
                    )
                    .await
                {
                    drop(current_guard);
                    sweep.pending.push_front(candidate);
                    return Err(RetentionStepError::new(RetentionPhase::Sweep(sweep), error));
                }
                drop(current_guard);
                return Ok((RetentionPhase::Sweep(sweep), 1));
            }
            return Ok((RetentionPhase::Sweep(sweep), 0));
        }
        if sweep.next_released >= sweep.released.len() {
            return Ok((RetentionPhase::Complete, 0));
        }
        let record = match self
            .within(work, sweep.released.record(sweep.next_released))
            .await
        {
            Ok(record) => record,
            Err(error) => {
                return Err(RetentionStepError::new(RetentionPhase::Sweep(sweep), error));
            }
        };
        sweep.next_released = sweep.next_released.saturating_add(1);
        let released = match ReleasedObject::from_record(job.definition.index_id, record) {
            Ok(released) => released,
            Err(error) => {
                return Err(RetentionStepError::new(RetentionPhase::Sweep(sweep), error));
            }
        };
        match self
            .within(work, classify_released_object(&mut sweep, released))
            .await
        {
            Ok(candidate) => sweep.pending.push_back(candidate),
            Err(error) => {
                return Err(RetentionStepError::new(RetentionPhase::Sweep(sweep), error));
            }
        }
        Ok((RetentionPhase::Sweep(sweep), 0))
    }

    async fn load_manifest(
        &self,
        job: &RetentionJob,
        reference: &CommitManifestReference,
    ) -> Result<LoadedManifest, Status> {
        reference
            .validate(job.definition.index_id)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        let key = ObjectKey::new(
            &job.definition.tenant,
            &job.definition.bucket,
            &reference.path,
        )
        .map_err(|error| Status::internal(error.to_string()))?;
        let Some(mut opened) = self
            .reader
            .open_stable(
                &key,
                job.tenant_id,
                job.bucket_id,
                Some(reference.object_version),
            )
            .await?
        else {
            return Err(Status::data_loss(
                "retained format-v4 manifest object is absent",
            ));
        };
        if opened.version.id != reference.object_version
            || opened.version.deleted
            || opened.version.blob.as_ref() != Some(&reference.blob)
        {
            return Err(Status::data_loss(
                "retained format-v4 manifest differs from its exact reference",
            ));
        }
        let payload = opened
            .payload
            .take()
            .ok_or_else(|| Status::data_loss("retained format-v4 manifest has no payload"))?;
        let mut bytes = Vec::new();
        let maximum = reference.blob.length.checked_add(1).ok_or_else(|| {
            Status::resource_exhausted("retained format-v4 manifest length exceeds u64")
        })?;
        payload
            .take(maximum)
            .read_to_end(&mut bytes)
            .map_err(|error| Status::internal(format!("read format-v4 manifest: {error}")))?;
        if bytes.len() as u64 != reference.blob.length {
            return Err(Status::data_loss(
                "retained format-v4 manifest length differs from its verified object reference",
            ));
        }
        let manifest = IndexCommitManifest::decode(&bytes)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        validate_manifest_reference(reference, &manifest, job.definition.index_id)?;
        Ok(LoadedManifest {
            manifest,
            encoded_bytes: bytes.len() as u64,
        })
    }

    async fn delete_exact(
        &self,
        job: &RetentionJob,
        current_guard: &IndexCurrentMutationGuard,
        path: &str,
        version: VersionId,
        class: &'static str,
        encoded_bytes: u64,
    ) -> Result<(), Status> {
        let result = self
            .artifacts
            .delete_while_current_mutation_held(
                IndexArtifactDelete {
                    storage_tenant: job.definition.tenant.clone(),
                    bucket: job.definition.bucket.clone(),
                    tenant_id: job.tenant_id,
                    bucket_id: job.bucket_id,
                    index_id: job.definition.index_id,
                    exact_path: path.to_owned(),
                    expected_version: version,
                    command_id: delete_command(job.definition.index_id, version, class, path),
                    definition_intent: None,
                },
                current_guard,
            )
            .await;
        tracing::debug!(
            index.id = job.definition.index_id,
            tenant.id = job.tenant_id,
            bucket.id = job.bucket_id,
            cleanup.object_class = class,
            cleanup.outcome = if result.is_err() {
                "failed"
            } else {
                "completed"
            },
            monotonic_counter.keldra_index_releasing_object_deletions_total =
                u64::from(result.is_ok()),
            monotonic_counter.keldra_index_releasing_object_deletion_failures_total =
                u64::from(result.is_err()),
            monotonic_counter.keldra_index_releasing_object_deleted_bytes_total =
                if result.is_ok() { encoded_bytes } else { 0 },
            histogram.keldra_index_releasing_object_bytes = encoded_bytes,
            "exact released index object cleanup finished"
        );
        result.map(|_| ())
    }

    async fn delete_unlinked_manifest_if_still_unlinked(
        &self,
        job: &RetentionJob,
        released: &ReleasingManifestReference,
    ) -> Result<(), Status> {
        let current_guard = self
            .artifacts
            .acquire_current_mutation(job.definition.index_id)
            .await?;
        let Some(current) = self
            .publisher
            .load_current(&job.definition, job.tenant_id, job.bucket_id)
            .await?
        else {
            return Err(Status::aborted(
                "current pointer disappeared before unlinked manifest cleanup",
            ));
        };
        let reference = &released.manifest;
        if std::iter::once(&current.pointer.current)
            .chain(current.pointer.retained.iter())
            .chain(
                current
                    .pointer
                    .releasing
                    .iter()
                    .map(|candidate| &candidate.manifest),
            )
            .any(|candidate| candidate == reference)
        {
            return Err(Status::aborted(
                "manifest was re-adopted before unlinked cleanup",
            ));
        }
        self.artifacts
            .delete_while_current_mutation_held(
                IndexArtifactDelete {
                    storage_tenant: job.definition.tenant.clone(),
                    bucket: job.definition.bucket.clone(),
                    tenant_id: job.tenant_id,
                    bucket_id: job.bucket_id,
                    index_id: job.definition.index_id,
                    exact_path: reference.path.clone(),
                    expected_version: reference.object_version,
                    command_id: delete_command(
                        job.definition.index_id,
                        reference.object_version,
                        "manifest",
                        &reference.path,
                    ),
                    definition_intent: None,
                },
                &current_guard,
            )
            .await
            .map(|_| ())
    }

    /// Revalidate only this fixed released candidate against the newest
    /// serving authority. New unrelated artifacts cannot affect the decision;
    /// an identical version re-adopted by a newer commit is caught here.
    async fn candidate_is_live(
        &self,
        job: &RetentionJob,
        current: &CommittedIndexView,
        candidate: &SweepCandidate,
    ) -> Result<bool, Status> {
        if candidate.class == "manifest" {
            return Ok(std::iter::once(&current.pointer.current)
                .chain(current.pointer.retained.iter())
                .any(|reference| {
                    reference.path == candidate.path
                        && reference.object_version == candidate.version
                }));
        }
        for reference in
            std::iter::once(&current.pointer.current).chain(current.pointer.retained.iter())
        {
            let loaded = self.load_manifest(job, reference).await?;
            if manifest_contains_candidate(&loaded.manifest, candidate) {
                return Ok(true);
            }
        }
        if let Some(rebuild) = self
            .publisher
            .load_rebuild_root(&job.definition, job.tenant_id, job.bucket_id)
            .await?
            && manifest_contains_candidate(&rebuild.root.candidate, candidate)
        {
            return Ok(true);
        }
        Ok(false)
    }

    async fn within<T>(
        &self,
        work: &RetentionWork,
        operation: impl std::future::Future<Output = Result<T, Status>>,
    ) -> Result<T, Status> {
        let remaining = work.remaining().ok_or_else(|| {
            Status::deadline_exceeded("index retention exhausted its tick time budget")
        })?;
        tokio::time::timeout(remaining, operation)
            .await
            .map_err(|_| Status::deadline_exceeded("index retention operation timed out"))?
    }
}

pub(crate) struct IndexRetentionTask {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for IndexRetentionTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct ActiveRetentionJob {
    due: IndexCommitRetentionDue,
    job: RetentionJob,
}

struct RetentionJob {
    definition: StoredIndexDefinition,
    tenant_id: u64,
    bucket_id: u64,
    definition_object_version: VersionId,
    current: CommittedIndexView,
    next_due_unix_millis: Option<u64>,
    cleanup_roots: Vec<ReleasingManifestReference>,
    roots_unlinked: bool,
    phase: RetentionPhase,
}

impl RetentionJob {
    fn new(
        definition: StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        definition_object_version: VersionId,
        current: CommittedIndexView,
    ) -> Result<Self, Status> {
        Ok(Self {
            definition,
            tenant_id,
            bucket_id,
            definition_object_version,
            current,
            next_due_unix_millis: None,
            cleanup_roots: Vec::new(),
            roots_unlinked: false,
            phase: RetentionPhase::Initialize,
        })
    }

    fn release_cleanup_in_progress(&self) -> bool {
        release_cleanup_in_progress(&self.phase, &self.cleanup_roots)
    }
}

enum RetentionPhase {
    Initialize,
    Discover(RetentionDiscovery),
    Sort(RetentionSort),
    Sweep(RetentionSweep),
    Complete,
}

fn release_cleanup_in_progress(
    _phase: &RetentionPhase,
    roots: &[ReleasingManifestReference],
) -> bool {
    !roots.is_empty()
}

type RetentionStepResult = Result<(RetentionPhase, u64), RetentionStepError>;

struct RetentionStepError {
    phase: RetentionPhase,
    status: Status,
}

impl RetentionStepError {
    fn new(phase: RetentionPhase, status: Status) -> Self {
        Self { phase, status }
    }
}

struct RetentionDiscovery {
    index_id: u64,
    collector: RetainedObjectCollector,
    released_collector: RetainedObjectCollector,
    pending_manifests: VecDeque<RankedManifest>,
    pending_released: VecDeque<ReleasingManifestReference>,
    releasing_roots: Vec<ReleasingManifestReference>,
}

impl RetentionDiscovery {
    async fn new(
        current: &CommittedIndexView,
        released: Vec<ReleasingManifestReference>,
        scratch: IndexMergeScratchSpace,
    ) -> Result<Self, Status> {
        let mut pending_manifests = VecDeque::new();
        pending_manifests.push_back(RankedManifest {
            rank: 0,
            reference: current.pointer.current.clone(),
            loaded: Some(current.manifest.clone()),
        });
        pending_manifests.extend(current.pointer.retained.iter().cloned().enumerate().map(
            |(index, reference)| RankedManifest {
                rank: index + 1,
                reference,
                loaded: None,
            },
        ));
        let releasing_roots = released.clone();
        let collector = RetainedObjectCollector::new(scratch.clone()).await?;
        let released_collector = RetainedObjectCollector::new(scratch).await?;
        Ok(Self {
            index_id: current.manifest.index_id,
            collector,
            released_collector,
            pending_manifests,
            pending_released: released.into(),
            releasing_roots,
        })
    }

    async fn protect_manifest(
        &mut self,
        rank: usize,
        reference: &CommitManifestReference,
        manifest: &IndexCommitManifest,
    ) -> Result<(), Status> {
        validate_manifest_reference(reference, manifest, self.index_id)?;
        let mut records = vec![RetainedObjectRecord::new(
            RETAINED_MANIFEST_CLASS,
            reference.blob.hash,
            reference.object_version.0,
            reference.blob.length,
            rank,
        )?];
        for segment in &manifest.segments {
            for pack in &segment.packs {
                prepare_pack(self.index_id, rank, pack, &mut records)?;
            }
        }
        for locator in &manifest.locator_roots {
            if let LocatorPackOwnership::Standalone(packs) = &locator.pack_ownership {
                for pack in packs {
                    prepare_pack(self.index_id, rank, pack, &mut records)?;
                }
            }
        }
        self.collector.append(records).await?;
        Ok(())
    }

    async fn collect_released(
        &mut self,
        released: &ReleasingManifestReference,
        manifest: &IndexCommitManifest,
    ) -> Result<(), Status> {
        validate_manifest_reference(&released.manifest, manifest, self.index_id)?;
        let mut records = vec![RetainedObjectRecord::new(
            RELEASED_MANIFEST_CLASS,
            released.manifest.blob.hash,
            released.manifest.object_version.0,
            released.manifest.blob.length,
            0,
        )?];
        for pack in manifest
            .segments
            .iter()
            .flat_map(|segment| segment.packs.iter())
            .chain(manifest.locator_roots.iter().flat_map(
                |locator| match &locator.pack_ownership {
                    LocatorPackOwnership::Segment => [].as_slice(),
                    LocatorPackOwnership::Standalone(packs) => packs.as_slice(),
                },
            ))
        {
            pack.validate(self.index_id)
                .map_err(index_integrity_status)?;
            records.push(RetainedObjectRecord::new(
                RELEASED_ARTIFACT_CLASS,
                pack.object_content_hash,
                pack.object_version,
                pack.object_length,
                0,
            )?);
        }
        self.released_collector.append(records).await?;
        Ok(())
    }
}

struct RetentionSort {
    protected: RetainedObjectSort,
    protected_proof: Option<RetainedObjectProof>,
    released: RetainedObjectSort,
    releasing_roots: Vec<ReleasingManifestReference>,
}

struct ReleasedObject {
    path: String,
    version: VersionId,
    hash: [u8; 32],
    length: u64,
    class: &'static str,
}

impl ReleasedObject {
    fn from_record(index_id: u64, record: RetainedObjectRecord) -> Result<Self, Status> {
        let (class, hash, version, length) = record.parts();
        let (path, class) = match class {
            RELEASED_MANIFEST_CLASS => (manifest_path(index_id, hash), "manifest"),
            RELEASED_ARTIFACT_CLASS => (artifact_path(index_id, hash), "artifact"),
            _ => {
                return Err(Status::data_loss(
                    "released-object proof contains an invalid record class",
                ));
            }
        };
        Ok(Self {
            path,
            version: VersionId(version),
            hash,
            length,
            class,
        })
    }
}

fn prepare_pack(
    index_id: u64,
    rank: usize,
    pack: &ArtifactPackReference,
    records: &mut Vec<RetainedObjectRecord>,
) -> Result<(), Status> {
    pack.validate(index_id).map_err(index_integrity_status)?;
    records.push(RetainedObjectRecord::new(
        RETAINED_ARTIFACT_CLASS,
        pack.object_content_hash,
        pack.object_version,
        pack.object_length,
        rank,
    )?);
    Ok(())
}

struct RankedManifest {
    rank: usize,
    reference: CommitManifestReference,
    loaded: Option<IndexCommitManifest>,
}

struct LoadedManifest {
    manifest: IndexCommitManifest,
    encoded_bytes: u64,
}

struct RetentionSweep {
    proof: RetainedObjectProof,
    selected_max_rank: usize,
    released: RetainedObjectProof,
    next_released: u64,
    pending: VecDeque<SweepCandidate>,
}

impl RetentionSweep {
    fn new(
        proof: RetainedObjectProof,
        selected_max_rank: usize,
        released: RetainedObjectProof,
    ) -> Self {
        Self {
            proof,
            selected_max_rank,
            released,
            next_released: 0,
            pending: VecDeque::new(),
        }
    }
}

struct SweepCandidate {
    path: String,
    version: VersionId,
    class: &'static str,
    length: u64,
    delete: bool,
    due_at: Option<u64>,
}

async fn classify_released_object(
    sweep: &mut RetentionSweep,
    released: ReleasedObject,
) -> Result<SweepCandidate, Status> {
    let proof_class = if released.class == "manifest" {
        RETAINED_MANIFEST_CLASS
    } else {
        RETAINED_ARTIFACT_CLASS
    };
    let protected = sweep
        .proof
        .lookup(proof_class, released.hash, released.version.0)
        .await?;
    if let Some((bytes, _)) = protected
        && bytes != released.length
    {
        return Err(Status::data_loss(
            "retained object proof has a conflicting released-object length",
        ));
    }
    let protected = protected.is_some_and(|(_, rank)| rank <= sweep.selected_max_rank);
    Ok(SweepCandidate {
        path: released.path,
        version: released.version,
        class: released.class,
        length: released.length,
        // `releasing` is the crash-safe queue for roots deliberately removed
        // from the current/retained set. Once the exact retained proof says
        // this object version is unshared it is known garbage, not an
        // uncertain orphan, and can be reclaimed immediately. The 24-hour
        // safety age applies only to the independent namespace orphan scrub.
        // The manifest is the restart-safe description of its releasing
        // graph. Unlink the root from the current pointer before deleting the
        // manifest itself; a crash during pack cleanup can then resume.
        delete: released.class != "manifest" && !protected,
        due_at: None,
    })
}

fn manifest_contains_candidate(manifest: &IndexCommitManifest, candidate: &SweepCandidate) -> bool {
    manifest
        .segments
        .iter()
        .flat_map(|segment| segment.packs.iter())
        .chain(
            manifest
                .locator_roots
                .iter()
                .flat_map(|locator| match &locator.pack_ownership {
                    LocatorPackOwnership::Segment => [].as_slice(),
                    LocatorPackOwnership::Standalone(packs) => packs.as_slice(),
                }),
        )
        .any(|pack| {
            pack.object_version == candidate.version.0
                && artifact_path(manifest.index_id, pack.object_content_hash) == candidate.path
        })
}

struct RetentionWork {
    budget: IndexRetentionBudget,
    started: Instant,
    records: u32,
    bytes: u64,
}

impl RetentionWork {
    fn new(budget: IndexRetentionBudget) -> Self {
        Self {
            budget,
            started: Instant::now(),
            records: 0,
            bytes: 0,
        }
    }

    fn has_room(&self) -> bool {
        self.records < self.budget.max_records
            && self.bytes < self.budget.max_bytes
            && self.started.elapsed() < self.budget.max_time
    }

    fn can_charge(&self, bytes: u64) -> bool {
        self.has_room()
            && (self.records == 0 || self.bytes.saturating_add(bytes) <= self.budget.max_bytes)
    }

    fn charge(&mut self, bytes: u64) {
        self.records = self.records.saturating_add(1);
        self.bytes = self.bytes.saturating_add(bytes.min(self.budget.max_bytes));
    }

    fn remaining(&self) -> Option<Duration> {
        self.budget.max_time.checked_sub(self.started.elapsed())
    }

    fn deadline(&self) -> Instant {
        self.started + self.budget.max_time
    }
}

fn next_age_due(pointer: &IndexCurrentPointer, config: IndexRuntimeConfig) -> Option<u64> {
    let maximum_age_millis = config
        .max_commit_revision_age_hours()
        .saturating_mul(60 * 60 * 1_000);
    pointer
        .retained
        .iter()
        .map(|reference| {
            reference
                .published_at_unix_millis
                .saturating_add(maximum_age_millis)
                .saturating_add(1)
        })
        .min()
}

fn commit_due(
    definition: &StoredIndexDefinition,
    tenant_id: u64,
    bucket_id: u64,
    definition_object_version: u64,
    revision: u64,
    due_at_unix_millis: u64,
) -> Result<IndexCommitRetentionDue, Status> {
    let due = IndexCommitRetentionDue {
        tenant_id,
        bucket_id,
        index_id: definition.index_id,
        definition_path: definition_path(&definition.name)?,
        definition_object_version: VersionId(definition_object_version),
        commit_revision: revision,
        due_at_unix_millis,
    };
    due.validate().map_err(retention_due_status)?;
    Ok(due)
}

fn minimum_due(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn completion_due(
    pointer: &IndexCurrentPointer,
    next_age_due: Option<u64>,
    now_unix_millis: u64,
) -> Option<u64> {
    if pointer.releasing.is_empty() {
        next_age_due
    } else {
        Some(now_unix_millis)
    }
}

fn retention_job_matches_schedule(
    job_due: &IndexCommitRetentionDue,
    proof_revision: u64,
    scheduled: &IndexCommitRetentionDue,
) -> bool {
    job_due == scheduled && proof_revision == scheduled.commit_revision
}

fn validate_manifest_reference(
    reference: &CommitManifestReference,
    manifest: &IndexCommitManifest,
    index_id: u64,
) -> Result<(), Status> {
    reference
        .validate(index_id)
        .map_err(|error| Status::data_loss(error.to_string()))?;
    if manifest.index_id != index_id
        || manifest.revision != reference.revision
        || manifest.definition_version != reference.definition_version
        || manifest.schema_fingerprint != reference.schema_fingerprint
    {
        return Err(Status::data_loss(
            "format-v4 manifest identity differs from its current-pointer reference",
        ));
    }
    Ok(())
}

fn require_current_identity(
    definition: &StoredIndexDefinition,
    current: &CommittedIndexView,
) -> Result<(), Status> {
    current
        .pointer
        .validate()
        .map_err(|error| Status::data_loss(error.to_string()))?;
    if current.pointer.index_id != definition.index_id
        || current.manifest.index_id != definition.index_id
    {
        return Err(Status::data_loss(
            "current index pointer and manifest identity differ during retention",
        ));
    }
    validate_manifest_reference(
        &current.pointer.current,
        &current.manifest,
        definition.index_id,
    )
}

fn index_integrity_status(error: keldra_index::IndexError) -> Status {
    Status::data_loss(error.to_string())
}

fn retention_due_status(error: keldra_store::IndexRetentionDueError) -> Status {
    match error {
        keldra_store::IndexRetentionDueError::Malformed(message) => Status::data_loss(message),
        keldra_store::IndexRetentionDueError::Storage(message) => Status::unavailable(message),
    }
}

fn delete_command(index_id: u64, version: VersionId, class: &str, path: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(class.as_bytes());
    hasher.update(path.as_bytes());
    hasher.update(&version.0.to_be_bytes());
    format!(
        "index-v4-gc-{index_id}-{}",
        &hasher.finalize().to_hex().as_str()[..24]
    )
}

fn now_unix_millis() -> Result<u64, Status> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Status::internal("system clock predates the Unix epoch"))?
            .as_millis(),
    )
    .map_err(|_| Status::internal("system time exceeds u64 milliseconds"))
}

#[cfg(test)]
#[path = "retention/tests.rs"]
mod tests;
