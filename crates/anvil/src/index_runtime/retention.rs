//! Node-wide bounded retention of ordinary format-v4 index artifacts.

use std::collections::VecDeque;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anvil_index::v4::{ArtifactPackReference, INDEX_COMPONENT_BYTES};
use anvil_store::{DefinitionKind, IndexGenerationRetentionDue, ObjectKey, Store, VersionId};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::{IndexCurrentHead, IndexHeadScanScope};
use crate::index_config::IndexRuntimeConfig;
use crate::index_service::{StoredIndexDefinition, definition_path};
use crate::logical_name_resolution::LogicalNameResolver;

use super::cache::IndexMergeScratchSpace;
use super::coordination::load_definition_locator_object;
use super::generation::{
    IndexCurrentPointer, IndexGenerationManifest, LocatorPackOwnership, ManifestReference,
};
use super::publication::{
    IndexArtifactDelete, IndexArtifactRouter, artifact_hash_from_path, current_path,
    is_manifest_artifact_path, manifest_hash_from_path,
};
use super::publisher::{IndexGenerationPublisher, PublishedGeneration};
use super::scanner::{ClusterIndexScan, ClusterIndexScanner};

#[path = "retention/deleted.rs"]
mod deleted;
#[path = "retention/scratch.rs"]
mod scratch;
use deleted::DeletedDefinitionRetention;
use scratch::{
    RETENTION_GENERATION_SLOTS, RetainedObjectCollector, RetainedObjectProof, RetainedObjectRecord,
    RetainedObjectSort,
};

const UNREACHABLE_ARTIFACT_SAFETY_MILLIS: u64 = 24 * 60 * 60 * 1_000;
const PUBLIC_REQUEST_SAFETY_MILLIS: u64 = 30 * 1_000;
const MAX_RETENTION_RECORD_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_RETENTION_TICK_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_RETENTION_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const RETAINED_MANIFEST_CLASS: u8 = 1;
const RETAINED_ARTIFACT_CLASS: u8 = 2;

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
pub(crate) struct IndexGenerationRetention {
    store: Store,
    scanner: ClusterIndexScanner,
    reader: ClusterObjectReader,
    artifacts: IndexArtifactRouter,
    publisher: IndexGenerationPublisher,
    scratch: IndexMergeScratchSpace,
    config: IndexRuntimeConfig,
    budget: IndexRetentionBudget,
    schedule: IndexRetentionSchedule,
    active: Arc<Mutex<Option<ActiveRetentionJob>>>,
    deleted: DeletedDefinitionRetention,
    run_lock: Arc<tokio::sync::Mutex<()>>,
}

impl IndexGenerationRetention {
    pub(crate) fn new(
        store: Store,
        scanner: ClusterIndexScanner,
        reader: ClusterObjectReader,
        artifacts: IndexArtifactRouter,
        publisher: IndexGenerationPublisher,
        scratch: IndexMergeScratchSpace,
        names: LogicalNameResolver,
        config: IndexRuntimeConfig,
    ) -> Self {
        let budget = IndexRetentionBudget::default();
        let schedule = IndexRetentionSchedule::default();
        Self {
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
            scanner,
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
        self
    }

    pub(crate) fn with_schedule(mut self, schedule: IndexRetentionSchedule) -> Self {
        self.schedule = schedule;
        self.deleted = self.deleted.with_schedule(schedule);
        self
    }

    /// Durably schedule the published generation immediately. The current
    /// pointer remains the sole generation authority; this sparse record is
    /// only restart-safe evidence that bounded retention work is due.
    pub(crate) fn schedule(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        current: &PublishedGeneration,
    ) -> Result<(), Status> {
        require_current_identity(definition, current)?;
        let due = generation_due(
            definition,
            tenant_id,
            bucket_id,
            current.manifest.definition_version,
            current.manifest.generation,
            now_unix_millis()?,
        )?;
        self.store
            .schedule_index_generation_retention(&due)
            .map(|_| ())
            .map_err(retention_due_status)
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
            .cancel_index_generation_retention(tenant_id, bucket_id, index_id)
            .map_err(retention_due_status)?;
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
                    tracing::warn!(%error, "bounded index retention tick will retry");
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
            return self.run_generation_tick().await;
        }
        let generation = self
            .store
            .oldest_index_generation_retention_due()
            .map_err(retention_due_status)?;
        let deleted = self.deleted.oldest_due()?;
        match (generation, deleted) {
            (None, None) => Ok(0),
            (Some(_), None) => self.run_generation_tick().await,
            (None, Some(_)) => self.deleted.run_tick().await,
            (Some(generation), Some(deleted))
                if deleted.due_at_unix_millis <= generation.due_at_unix_millis =>
            {
                self.deleted.run_tick().await
            }
            (Some(_), Some(_)) => self.run_generation_tick().await,
        }
    }

    async fn run_generation_tick(&self) -> Result<u64, Status> {
        let mut active = self.take_active()?;
        if active.is_none() {
            let Some(due) = self
                .store
                .oldest_index_generation_retention_due()
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
        if !self
            .store
            .index_generation_retention_due_matches(&active.due)
            .map_err(retention_due_status)?
        {
            return Ok(0);
        }
        // The durable due record and ordinary current pointer are both fences,
        // not leases. Re-read the pointer on every bounded work quantum before
        // allowing another trim or exact artifact deletion.
        active.job.current_validated = false;
        let mut work = RetentionWork::new(self.budget);
        let result = self
            .advance_job(&active.due, &mut active.job, &mut work)
            .await;
        match result {
            Ok(removed) => {
                if matches!(active.job.phase, RetentionPhase::Complete) {
                    self.finish_due(&active)?;
                } else if self
                    .store
                    .index_generation_retention_due_matches(&active.due)
                    .map_err(retention_due_status)?
                {
                    self.put_active(active)?;
                }
                let (backlog, oldest_millis) = self.backlog()?;
                tracing::debug!(
                    index.id = index_id,
                    gauge.anvil_index_retention_tick_records = work.records as u64,
                    gauge.anvil_index_retention_tick_bytes = work.bytes,
                    gauge.anvil_index_retention_backlog = backlog as u64,
                    gauge.anvil_index_retention_oldest_pending_millis = oldest_millis,
                    monotonic_counter.anvil_index_retention_artifacts_deleted_total = removed,
                    "bounded node-wide index retention tick completed"
                );
                Ok(removed)
            }
            Err(error) => {
                self.defer_due(&active.due)?;
                let (backlog, oldest_millis) = self.backlog()?;
                tracing::warn!(
                    index.id = index_id,
                    gauge.anvil_index_retention_backlog = backlog as u64,
                    gauge.anvil_index_retention_oldest_pending_millis = oldest_millis,
                    monotonic_counter.anvil_index_retention_errors_total = 1_u64,
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
        due: IndexGenerationRetentionDue,
    ) -> Result<Option<RetentionJob>, Status> {
        if !self
            .store
            .index_generation_retention_due_matches(&due)
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
        if locator.definition_id != due.index_id {
            return Err(Status::data_loss(
                "scheduled index locator belongs to another index",
            ));
        }
        let Some(object) = load_definition_locator_object(&self.reader, &locator).await? else {
            return Err(Status::unavailable(
                "scheduled index definition changed during exact read",
            ));
        };
        let definition = StoredIndexDefinition::decode(&object.bytes)?;
        if definition.index_id != due.index_id
            || definition_path(&definition.name)? != due.definition_path
        {
            return Err(Status::data_loss(
                "scheduled retention definition identity is inconsistent",
            ));
        }
        let current = self
            .publisher
            .load_current(&definition, due.tenant_id, due.bucket_id)
            .await?
            .ok_or_else(|| Status::unavailable("scheduled index has no current generation"))?;
        if current.manifest.definition_version != locator.object_version.0 {
            return Err(Status::unavailable(
                "index definition publication has not reached its current revision",
            ));
        }
        if locator.object_version != due.definition_object_version
            || current.manifest.generation != due.generation
        {
            let replacement = generation_due(
                &definition,
                due.tenant_id,
                due.bucket_id,
                locator.object_version.0,
                current.manifest.generation,
                now_unix_millis()?,
            )?;
            self.store
                .schedule_index_generation_retention(&replacement)
                .map_err(retention_due_status)?;
            return Ok(None);
        }
        RetentionJob::new(definition, due.tenant_id, due.bucket_id, current).map(Some)
    }

    fn finish_due(&self, active: &ActiveRetentionJob) -> Result<(), Status> {
        if let Some(due_at) = active.job.next_due_unix_millis {
            let replacement = generation_due(
                &active.job.definition,
                active.job.tenant_id,
                active.job.bucket_id,
                active.job.current.manifest.definition_version,
                active.job.current.manifest.generation,
                due_at,
            )?;
            self.store
                .replace_index_generation_retention_due(&active.due, &replacement)
                .map_err(retention_due_status)?;
        } else {
            self.store
                .complete_index_generation_retention_due(&active.due)
                .map_err(retention_due_status)?;
        }
        Ok(())
    }

    fn defer_due(&self, due: &IndexGenerationRetentionDue) -> Result<(), Status> {
        let retry_millis =
            u64::try_from(self.schedule.retry_interval.as_millis()).unwrap_or(u64::MAX);
        let mut replacement = due.clone();
        replacement.due_at_unix_millis = now_unix_millis()?.saturating_add(retry_millis);
        self.store
            .replace_index_generation_retention_due(due, &replacement)
            .map_err(retention_due_status)?;
        Ok(())
    }

    fn backlog(&self) -> Result<(usize, u64), Status> {
        let Some(oldest) = self
            .store
            .oldest_index_generation_retention_due()
            .map_err(retention_due_status)?
        else {
            return Ok((0, 0));
        };
        Ok((
            1,
            now_unix_millis()?.saturating_sub(oldest.due_at_unix_millis),
        ))
    }

    fn require_due(&self, expected: &IndexGenerationRetentionDue) -> Result<(), Status> {
        if self
            .store
            .index_generation_retention_due_matches(expected)
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
        due: &IndexGenerationRetentionDue,
        job: &mut RetentionJob,
        work: &mut RetentionWork,
    ) -> Result<u64, Status> {
        if !job.current_validated {
            self.validate_scheduled_current(job, work).await?;
            job.current_validated = true;
        }
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
                RetentionPhase::Sort(sort) => self.advance_sort(sort, work).await,
                RetentionPhase::Trim(proof) => self.advance_trim(due, job, proof, work).await,
                RetentionPhase::Sweep(sweep) => self.advance_sweep(due, job, sweep, work).await,
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
        due: &IndexGenerationRetentionDue,
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
        job.next_due_unix_millis = next_age_due(&job.current.pointer, self.config);
        let discovery = match self
            .within(
                work,
                RetentionDiscovery::new(&job.current, self.scratch.clone()),
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
        mut sort: RetainedObjectSort,
        work: &RetentionWork,
    ) -> RetentionStepResult {
        match sort.advance(work.deadline()).await {
            Ok(Some(proof)) => Ok((RetentionPhase::Trim(proof), 0)),
            Ok(None) => Ok((RetentionPhase::Sort(sort), 0)),
            Err(error) => Err(RetentionStepError::new(RetentionPhase::Sort(sort), error)),
        }
    }

    async fn advance_trim(
        &self,
        due: &IndexGenerationRetentionDue,
        job: &mut RetentionJob,
        proof: RetainedObjectProof,
        work: &mut RetentionWork,
    ) -> RetentionStepResult {
        let retained = match select_byte_retained(
            &job.current.pointer,
            proof.contributions(),
            self.config.max_retained_generation_bytes(),
        ) {
            Ok(retained) => retained,
            Err(error) => {
                return Err(RetentionStepError::new(RetentionPhase::Trim(proof), error));
            }
        };
        if retained != job.current.pointer.retained {
            if let Err(error) = self.require_due(due) {
                return Err(RetentionStepError::new(RetentionPhase::Trim(proof), error));
            }
            let trimmed = match self
                .within(
                    work,
                    self.publisher.trim_retained(
                        &job.definition,
                        job.tenant_id,
                        job.bucket_id,
                        &job.current,
                        retained,
                    ),
                )
                .await
            {
                Ok(trimmed) => trimmed,
                Err(error) => {
                    return Err(RetentionStepError::new(RetentionPhase::Trim(proof), error));
                }
            };
            job.current = trimmed;
            work.charge(INDEX_COMPONENT_BYTES as u64);
        }
        job.next_due_unix_millis = minimum_due(
            job.next_due_unix_millis,
            next_age_due(&job.current.pointer, self.config),
        );
        let selected_max_rank = job.current.pointer.retained.len();
        Ok((
            RetentionPhase::Sweep(RetentionSweep::new(proof, selected_max_rank)),
            0,
        ))
    }

    async fn validate_scheduled_current(
        &self,
        job: &RetentionJob,
        work: &mut RetentionWork,
    ) -> Result<(), Status> {
        let maximum = INDEX_COMPONENT_BYTES as u64;
        if !work.can_charge(maximum) {
            return Err(Status::resource_exhausted(
                "index retention cannot validate the current pointer within this tick",
            ));
        }
        let path = current_path(job.definition.index_id);
        let key = ObjectKey::new(&job.definition.tenant, &job.definition.bucket, &path)
            .map_err(|error| Status::internal(error.to_string()))?;
        let Some(mut opened) = self
            .within(
                work,
                self.reader
                    .open_stable(&key, job.tenant_id, job.bucket_id, None),
            )
            .await?
        else {
            return Err(Status::aborted(
                "scheduled index current pointer is no longer live",
            ));
        };
        if opened.version.deleted || opened.version.id != job.current.current_object_version {
            return Err(Status::aborted(
                "scheduled index current pointer changed before retention",
            ));
        }
        let payload = opened
            .payload
            .take()
            .ok_or_else(|| Status::data_loss("live index current pointer has no payload"))?;
        let mut bytes = Vec::new();
        payload
            .take(maximum + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| Status::internal(format!("read index current pointer: {error}")))?;
        if bytes.len() as u64 > maximum {
            return Err(Status::resource_exhausted(
                "index current pointer exceeds the format-v4 component bound",
            ));
        }
        work.charge(bytes.len() as u64);
        let pointer = IndexCurrentPointer::decode(&bytes)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        if pointer != job.current.pointer {
            return Err(Status::aborted(
                "scheduled index generation is no longer current",
            ));
        }
        Ok(())
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

        tracing::debug!(
            index.id = job.definition.index_id,
            "format-v4 retained pack tables collected into bounded scratch"
        );
        Ok((RetentionPhase::Sort(discovery.collector.into_sort()), 0))
    }

    async fn advance_sweep(
        &self,
        due: &IndexGenerationRetentionDue,
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
                if let Err(error) = self.require_due(due) {
                    sweep.pending.push_front(candidate);
                    return Err(RetentionStepError::new(RetentionPhase::Sweep(sweep), error));
                }
                if let Err(error) = self
                    .within(
                        work,
                        self.delete_exact(job, &candidate.path, candidate.version, candidate.class),
                    )
                    .await
                {
                    sweep.pending.push_front(candidate);
                    return Err(RetentionStepError::new(RetentionPhase::Sweep(sweep), error));
                }
                return Ok((RetentionPhase::Sweep(sweep), 1));
            }
            return Ok((RetentionPhase::Sweep(sweep), 0));
        }
        if sweep.scan.is_none() {
            let scan = self.scanner.begin(IndexHeadScanScope {
                tenant_id: job.tenant_id,
                bucket_id: job.bucket_id,
                index_id: job.definition.index_id,
            });
            sweep.scan = match scan {
                Ok(scan) => Some(scan),
                Err(error) => {
                    return Err(RetentionStepError::new(RetentionPhase::Sweep(sweep), error));
                }
            };
        }
        let page = match self
            .within(
                work,
                sweep
                    .scan
                    .as_mut()
                    .expect("artifact scan exists")
                    .next_page(),
            )
            .await
        {
            Ok(page) => page,
            Err(error) => {
                return Err(RetentionStepError::new(RetentionPhase::Sweep(sweep), error));
            }
        };
        let Some(heads) = page else {
            return Ok((RetentionPhase::Complete, 0));
        };
        let now = match now_unix_millis() {
            Ok(now) => now,
            Err(error) => {
                return Err(RetentionStepError::new(RetentionPhase::Sweep(sweep), error));
            }
        };
        for head in heads {
            match self
                .within(work, classify_sweep_candidate(job, &mut sweep, head, now))
                .await
            {
                Ok(Some(candidate)) => sweep.pending.push_back(candidate),
                Ok(None) => {}
                Err(error) => {
                    return Err(RetentionStepError::new(RetentionPhase::Sweep(sweep), error));
                }
            }
        }
        Ok((RetentionPhase::Sweep(sweep), 0))
    }

    async fn load_manifest(
        &self,
        job: &RetentionJob,
        reference: &ManifestReference,
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
        let manifest = IndexGenerationManifest::decode(&bytes)
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
        path: &str,
        version: VersionId,
        class: &'static str,
    ) -> Result<(), Status> {
        self.artifacts
            .delete(IndexArtifactDelete {
                storage_tenant: job.definition.tenant.clone(),
                bucket: job.definition.bucket.clone(),
                tenant_id: job.tenant_id,
                bucket_id: job.bucket_id,
                index_id: job.definition.index_id,
                exact_path: path.to_owned(),
                expected_version: version,
                command_id: delete_command(job.definition.index_id, version, class, path),
                definition_intent: None,
            })
            .await?;
        Ok(())
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
    due: IndexGenerationRetentionDue,
    job: RetentionJob,
}

struct RetentionJob {
    definition: StoredIndexDefinition,
    tenant_id: u64,
    bucket_id: u64,
    current: PublishedGeneration,
    current_validated: bool,
    next_due_unix_millis: Option<u64>,
    phase: RetentionPhase,
}

impl RetentionJob {
    fn new(
        definition: StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        current: PublishedGeneration,
    ) -> Result<Self, Status> {
        Ok(Self {
            definition,
            tenant_id,
            bucket_id,
            current,
            current_validated: false,
            next_due_unix_millis: None,
            phase: RetentionPhase::Initialize,
        })
    }
}

enum RetentionPhase {
    Initialize,
    Discover(RetentionDiscovery),
    Sort(RetainedObjectSort),
    Trim(RetainedObjectProof),
    Sweep(RetentionSweep),
    Complete,
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
    pending_manifests: VecDeque<RankedManifest>,
}

impl RetentionDiscovery {
    async fn new(
        current: &PublishedGeneration,
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
        Ok(Self {
            index_id: current.manifest.index_id,
            collector: RetainedObjectCollector::new(scratch).await?,
            pending_manifests,
        })
    }

    async fn protect_manifest(
        &mut self,
        rank: usize,
        reference: &ManifestReference,
        manifest: &IndexGenerationManifest,
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
    reference: ManifestReference,
    loaded: Option<IndexGenerationManifest>,
}

struct LoadedManifest {
    manifest: IndexGenerationManifest,
    encoded_bytes: u64,
}

struct RetentionSweep {
    proof: RetainedObjectProof,
    selected_max_rank: usize,
    scan: Option<ClusterIndexScan>,
    pending: VecDeque<SweepCandidate>,
}

impl RetentionSweep {
    fn new(proof: RetainedObjectProof, selected_max_rank: usize) -> Self {
        Self {
            proof,
            selected_max_rank,
            scan: None,
            pending: VecDeque::new(),
        }
    }
}

struct SweepCandidate {
    path: String,
    version: VersionId,
    class: &'static str,
    delete: bool,
    due_at: Option<u64>,
}

async fn classify_sweep_candidate(
    job: &RetentionJob,
    sweep: &mut RetentionSweep,
    head: IndexCurrentHead,
    now: u64,
) -> Result<Option<SweepCandidate>, Status> {
    let index_id = job.definition.index_id;
    if head.version.deleted || head.version.blob.is_none() {
        return Ok(None);
    }
    let (class, safety_age, protected) =
        if head.exact_path == current_path(index_id) {
            (
                "current",
                PUBLIC_REQUEST_SAFETY_MILLIS,
                head.version.id == job.current.current_object_version
                    || (!head.head.deleted && head.version.id == head.head.version),
            )
        } else if is_manifest_artifact_path(index_id, &head.exact_path) {
            let blob =
                head.version.blob.as_ref().ok_or_else(|| {
                    Status::data_loss("live index manifest has no blob reference")
                })?;
            if manifest_hash_from_path(index_id, &head.exact_path) != Some(blob.hash) {
                return Err(Status::data_loss(
                    "index manifest path and object blob hash disagree",
                ));
            }
            let protected = sweep
                .proof
                .lookup(RETAINED_MANIFEST_CLASS, blob.hash, head.version.id.0)
                .await?;
            if let Some((bytes, _)) = protected
                && bytes != blob.length
            {
                return Err(Status::data_loss(
                    "retained manifest proof has a conflicting object length",
                ));
            }
            (
                "manifest",
                UNREACHABLE_ARTIFACT_SAFETY_MILLIS,
                protected.is_some_and(|(_, rank)| rank <= sweep.selected_max_rank),
            )
        } else if let Some(hash) = artifact_hash_from_path(index_id, &head.exact_path) {
            let blob =
                head.version.blob.as_ref().ok_or_else(|| {
                    Status::data_loss("live index artifact has no blob reference")
                })?;
            if blob.hash != hash {
                return Err(Status::data_loss(
                    "index artifact path and object blob hash disagree",
                ));
            }
            let protected = sweep
                .proof
                .lookup(RETAINED_ARTIFACT_CLASS, hash, head.version.id.0)
                .await?;
            if let Some((bytes, _)) = protected
                && bytes != blob.length
            {
                return Err(Status::data_loss(
                    "retained artifact proof has a conflicting object length",
                ));
            }
            (
                "artifact",
                UNREACHABLE_ARTIFACT_SAFETY_MILLIS,
                protected.is_some_and(|(_, rank)| rank <= sweep.selected_max_rank),
            )
        } else {
            return Ok(None);
        };
    let age = now.saturating_sub(head.version.committed_at_unix_millis);
    let due_at = (!protected && age < safety_age).then_some(
        head.version
            .committed_at_unix_millis
            .saturating_add(safety_age),
    );
    Ok(Some(SweepCandidate {
        path: head.exact_path.clone(),
        version: head.version.id,
        class,
        delete: age >= safety_age && !protected,
        due_at,
    }))
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

fn select_byte_retained(
    pointer: &IndexCurrentPointer,
    contributions: &[u64; RETENTION_GENERATION_SLOTS],
    maximum_bytes: u64,
) -> Result<Vec<ManifestReference>, Status> {
    let mut total = contributions[0];
    let mut retained = Vec::new();
    for (index, reference) in pointer.retained.iter().enumerate() {
        let rank = index + 1;
        let candidate = total.checked_add(contributions[rank]).ok_or_else(|| {
            Status::resource_exhausted("retained generation byte total overflowed")
        })?;
        if candidate > maximum_bytes {
            break;
        }
        total = candidate;
        retained.push(reference.clone());
    }
    Ok(retained)
}

fn next_age_due(pointer: &IndexCurrentPointer, config: IndexRuntimeConfig) -> Option<u64> {
    let maximum_age_millis = config
        .max_generation_age_hours()
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

fn generation_due(
    definition: &StoredIndexDefinition,
    tenant_id: u64,
    bucket_id: u64,
    definition_object_version: u64,
    generation: u64,
    due_at_unix_millis: u64,
) -> Result<IndexGenerationRetentionDue, Status> {
    let due = IndexGenerationRetentionDue {
        tenant_id,
        bucket_id,
        index_id: definition.index_id,
        definition_path: definition_path(&definition.name)?,
        definition_object_version: VersionId(definition_object_version),
        generation,
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

fn validate_manifest_reference(
    reference: &ManifestReference,
    manifest: &IndexGenerationManifest,
    index_id: u64,
) -> Result<(), Status> {
    reference
        .validate(index_id)
        .map_err(|error| Status::data_loss(error.to_string()))?;
    if manifest.index_id != index_id
        || manifest.generation != reference.generation
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
    current: &PublishedGeneration,
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

fn index_integrity_status(error: anvil_index::IndexError) -> Status {
    Status::data_loss(error.to_string())
}

fn retention_due_status(error: anvil_store::IndexRetentionDueError) -> Status {
    match error {
        anvil_store::IndexRetentionDueError::Malformed(message) => Status::data_loss(message),
        anvil_store::IndexRetentionDueError::Storage(message) => Status::unavailable(message),
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
