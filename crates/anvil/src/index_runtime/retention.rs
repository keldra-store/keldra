//! Node-wide bounded retention of ordinary format-2 index artifacts.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anvil_store::{ObjectKey, VersionId};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::{IndexCurrentHead, IndexHeadScanScope};
use crate::index_config::IndexRuntimeConfig;
use crate::index_service::StoredIndexDefinition;

use super::generation::{IndexCurrentPointer, IndexGenerationManifest, ManifestReference};
use super::publication::{
    IndexArtifactDelete, IndexArtifactRouter, current_path, is_manifest_artifact_path,
    run_hash_from_artifact_path,
};
use super::publisher::PublishedGeneration;
use super::scanner::{ClusterIndexScan, ClusterIndexScanner};

const UNREACHABLE_ARTIFACT_SAFETY_MILLIS: u64 = 24 * 60 * 60 * 1_000;
const PUBLIC_REQUEST_SAFETY_MILLIS: u64 = 30 * 1_000;
const MAX_RETENTION_RECORD_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_RETENTION_TICK_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_RETENTION_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const MAX_ACTIVE_RETENTION_JOBS: usize = 64;

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
    scanner: ClusterIndexScanner,
    reader: ClusterObjectReader,
    artifacts: IndexArtifactRouter,
    config: IndexRuntimeConfig,
    budget: IndexRetentionBudget,
    schedule: IndexRetentionSchedule,
    scheduler: Arc<Mutex<RetentionScheduler>>,
    run_lock: Arc<tokio::sync::Mutex<()>>,
}

impl IndexGenerationRetention {
    pub(crate) fn new(
        scanner: ClusterIndexScanner,
        reader: ClusterObjectReader,
        artifacts: IndexArtifactRouter,
        config: IndexRuntimeConfig,
    ) -> Self {
        Self {
            scanner,
            reader,
            artifacts,
            config,
            budget: IndexRetentionBudget::default(),
            schedule: IndexRetentionSchedule::default(),
            scheduler: Arc::new(Mutex::new(RetentionScheduler::default())),
            run_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub(crate) fn with_budget(mut self, budget: IndexRetentionBudget) -> Self {
        self.budget = budget;
        self
    }

    pub(crate) fn with_schedule(mut self, schedule: IndexRetentionSchedule) -> Self {
        self.schedule = schedule;
        self
    }

    /// Lease bounded retention work discovered by the durable assignment walk.
    /// Completed work is forgotten rather than retained as one idle record per
    /// definition; a later assignment walk leases it again.
    pub(crate) fn schedule(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        current: &PublishedGeneration,
    ) -> Result<(), Status> {
        require_current_identity(definition, current)?;
        let requested = RetentionIdentity::new(tenant_id, bucket_id, definition.index_id)?;
        self.scheduler
            .lock()
            .map_err(|_| Status::internal("index retention scheduler lock is poisoned"))?
            .register(
                requested,
                RetentionJob::new(definition.clone(), tenant_id, bucket_id, current.clone()),
            )
    }

    pub(crate) fn unschedule(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        index_id: u64,
    ) -> Result<(), Status> {
        let identity = RetentionIdentity::new(tenant_id, bucket_id, index_id)?;
        self.scheduler
            .lock()
            .map_err(|_| Status::internal("index retention scheduler lock is poisoned"))?
            .remove(identity);
        Ok(())
    }

    pub(crate) fn start_scheduler(&self) -> IndexRetentionTask {
        let retention = self.clone();
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(retention.schedule.tick_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Maintenance never delays serving or inventories artifacts at
            // startup. Definitions explicitly schedule work as builders load.
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
        let Some((identity, mut job)) = self
            .scheduler
            .lock()
            .map_err(|_| Status::internal("index retention scheduler lock is poisoned"))?
            .pop()
        else {
            return Ok(0);
        };
        let mut work = RetentionWork::new(self.budget);
        let result = self.advance_job(&mut job, &mut work).await;
        match result {
            Ok(removed) => {
                let (backlog, oldest_millis) = {
                    let mut scheduler = self.scheduler.lock().map_err(|_| {
                        Status::internal("index retention scheduler lock is poisoned")
                    })?;
                    if matches!(job.phase, RetentionPhase::Complete) {
                        scheduler.complete(identity, job.generation);
                    } else {
                        scheduler.requeue(identity, job);
                    }
                    scheduler.backlog()
                };
                tracing::debug!(
                    index.id = identity.index_id,
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
                let transient = matches!(
                    error.code(),
                    tonic::Code::Unavailable
                        | tonic::Code::DeadlineExceeded
                        | tonic::Code::Aborted
                        | tonic::Code::ResourceExhausted
                );
                let (backlog, oldest_millis) = {
                    let mut scheduler = self.scheduler.lock().map_err(|_| {
                        Status::internal("index retention scheduler lock is poisoned")
                    })?;
                    if transient {
                        scheduler.retry(identity, job, self.schedule.retry_interval);
                    } else {
                        scheduler.fail(identity, job.generation);
                    }
                    scheduler.backlog()
                };
                tracing::warn!(
                    index.id = identity.index_id,
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

    async fn advance_job(
        &self,
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
                RetentionPhase::Chain(chain) => self.advance_chain(job, chain, work).await,
                RetentionPhase::Delete(delete) => self.advance_delete(job, delete, work).await,
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
            // A scan page or phase cursor may advance without charging an
            // artifact record. Yield in that case as well as when the
            // remaining byte budget cannot admit the next bounded record;
            // otherwise an unchanged phase could spin for the rest of the
            // process lifetime.
            if !phase_changed && work.records == records_before && work.bytes == bytes_before {
                break;
            }
        }
        Ok(removed)
    }

    async fn validate_scheduled_current(
        &self,
        job: &RetentionJob,
        work: &mut RetentionWork,
    ) -> Result<(), Status> {
        if !work.can_charge(MAX_RETENTION_RECORD_BYTES) {
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
        let mut payload = opened
            .payload
            .take()
            .ok_or_else(|| Status::data_loss("live index current pointer has no payload"))?;
        let mut bytes = Vec::new();
        payload
            .take(MAX_RETENTION_RECORD_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| Status::internal(format!("read index current pointer: {error}")))?;
        if bytes.len() as u64 > MAX_RETENTION_RECORD_BYTES {
            return Err(Status::resource_exhausted(
                "index current pointer exceeds the retention record bound",
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

    async fn advance_chain(
        &self,
        job: &RetentionJob,
        mut chain: RetentionChain,
        work: &mut RetentionWork,
    ) -> RetentionStepResult {
        let Some(reference) = chain.previous.clone() else {
            return Ok((
                RetentionPhase::Sweep(RetentionSweep::new(chain.retained)),
                0,
            ));
        };
        if !work.can_charge(MAX_RETENTION_RECORD_BYTES) {
            return Ok((RetentionPhase::Chain(chain), 0));
        }
        if let Err(error) =
            validate_predecessor(&reference, chain.expected_below, job.definition.index_id)
        {
            return Err(RetentionStepError::new(RetentionPhase::Chain(chain), error));
        }
        let loaded = match self.within(work, self.load_manifest(job, &reference)).await {
            Ok(loaded) => loaded,
            Err(error) => {
                return Err(RetentionStepError::new(RetentionPhase::Chain(chain), error));
            }
        };
        let (manifest, encoded_bytes) = match loaded {
            LoadedPredecessor::Present(manifest, encoded_bytes) => (manifest, encoded_bytes),
            LoadedPredecessor::PreviouslyPruned => {
                work.charge(reference.path.len() as u64 + 32);
                return Ok((
                    RetentionPhase::Sweep(RetentionSweep::new(chain.retained)),
                    0,
                ));
            }
        };
        work.charge(encoded_bytes);
        let now = match now_unix_millis() {
            Ok(now) => now,
            Err(error) => {
                return Err(RetentionStepError::new(RetentionPhase::Chain(chain), error));
            }
        };
        let within_bounds = !chain.obsolete
            && retain_predecessor(
                chain.retained_count,
                chain.retained_bytes,
                &reference,
                &manifest,
                now,
                self.config,
            );
        chain.previous = manifest.previous.clone();
        chain.expected_below = manifest.generation;
        if within_bounds {
            chain.retained.insert(&reference.path, &manifest);
            chain.retained_count = chain.retained_count.saturating_add(1);
            chain.retained_bytes = match chain
                .retained_bytes
                .checked_add(manifest.authoritative_bytes)
            {
                Some(bytes) => bytes,
                None => {
                    return Err(RetentionStepError::new(
                        RetentionPhase::Chain(chain),
                        Status::resource_exhausted("retained index generation bytes overflow"),
                    ));
                }
            };
            chain.successor_published_at = reference.published_at_unix_millis;
            return Ok((RetentionPhase::Chain(chain), 0));
        }
        chain.obsolete = true;
        let safe = now.saturating_sub(chain.successor_published_at) >= PUBLIC_REQUEST_SAFETY_MILLIS;
        chain.successor_published_at = reference.published_at_unix_millis;
        if safe {
            Ok((
                RetentionPhase::Delete(RetentionDelete::new(chain, reference, manifest)),
                0,
            ))
        } else {
            Ok((RetentionPhase::Chain(chain), 0))
        }
    }

    async fn advance_delete(
        &self,
        job: &RetentionJob,
        mut delete: RetentionDelete,
        work: &mut RetentionWork,
    ) -> RetentionStepResult {
        if let Some(head) = delete.pending.pop_front() {
            let bytes = head.exact_path.len() as u64 + 64;
            if !work.can_charge(bytes) {
                delete.pending.push_front(head);
                return Ok((RetentionPhase::Delete(delete), 0));
            }
            work.charge(bytes);
            if !head.head.deleted && !head.version.deleted {
                if let Err(error) = self
                    .within(
                        work,
                        self.delete_exact(job, &head.exact_path, head.version.id, "run"),
                    )
                    .await
                {
                    delete.pending.push_front(head);
                    return Err(RetentionStepError::new(
                        RetentionPhase::Delete(delete),
                        error,
                    ));
                }
                return Ok((RetentionPhase::Delete(delete), 1));
            }
            return Ok((RetentionPhase::Delete(delete), 0));
        }
        while delete.run_index < delete.manifest.runs.len() {
            let run = &delete.manifest.runs[delete.run_index];
            if delete
                .chain
                .retained
                .run_hashes
                .contains(&run.root_blob.hash)
            {
                delete.run_index += 1;
                delete.scan = None;
                continue;
            }
            if delete.scan.is_none() {
                let scan = self.scanner.begin(IndexHeadScanScope::Run {
                    tenant_id: job.tenant_id,
                    bucket_id: job.bucket_id,
                    index_id: job.definition.index_id,
                    run_hash: run.root_blob.hash,
                });
                delete.scan = match scan {
                    Ok(scan) => Some(scan),
                    Err(error) => {
                        return Err(RetentionStepError::new(
                            RetentionPhase::Delete(delete),
                            error,
                        ));
                    }
                };
            }
            let page = match self
                .within(
                    work,
                    delete.scan.as_mut().expect("run scan exists").next_page(),
                )
                .await
            {
                Ok(page) => page,
                Err(error) => {
                    return Err(RetentionStepError::new(
                        RetentionPhase::Delete(delete),
                        error,
                    ));
                }
            };
            match page {
                Some(heads) => {
                    delete.pending.extend(heads);
                    return Ok((RetentionPhase::Delete(delete), 0));
                }
                None => {
                    delete.run_index += 1;
                    delete.scan = None;
                }
            }
        }
        let bytes = delete.reference.path.len() as u64 + 64;
        if !work.can_charge(bytes) {
            return Ok((RetentionPhase::Delete(delete), 0));
        }
        work.charge(bytes);
        if let Err(error) = self
            .within(
                work,
                self.delete_exact(
                    job,
                    &delete.reference.path,
                    delete.reference.object_version,
                    "manifest",
                ),
            )
            .await
        {
            return Err(RetentionStepError::new(
                RetentionPhase::Delete(delete),
                error,
            ));
        }
        Ok((RetentionPhase::Chain(delete.chain), 1))
    }

    async fn advance_sweep(
        &self,
        job: &RetentionJob,
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
            if candidate.delete {
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
            let scan = self.scanner.begin(IndexHeadScanScope::Artifacts {
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
            sweep
                .pending
                .extend(sweep_candidates(job, &sweep.retained, head, now));
        }
        Ok((RetentionPhase::Sweep(sweep), 0))
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

    async fn load_manifest(
        &self,
        job: &RetentionJob,
        reference: &ManifestReference,
    ) -> Result<LoadedPredecessor, Status> {
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
            let current = self
                .reader
                .head_stable(&key, job.tenant_id, job.bucket_id)
                .await?;
            return classify_absent_predecessor(reference.object_version, current.as_ref());
        };
        if opened.version.id != reference.object_version
            || opened.version.deleted
            || opened.version.blob.as_ref() != Some(&reference.blob)
        {
            return Err(Status::data_loss(
                "index predecessor object differs from its manifest reference",
            ));
        }
        let mut payload = opened
            .payload
            .take()
            .ok_or_else(|| Status::data_loss("live index predecessor manifest has no payload"))?;
        let mut bytes = Vec::new();
        payload
            .take(MAX_RETENTION_RECORD_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| {
                Status::internal(format!("read index predecessor manifest: {error}"))
            })?;
        if bytes.len() as u64 > MAX_RETENTION_RECORD_BYTES {
            return Err(Status::resource_exhausted(
                "index predecessor manifest exceeds the retention record bound",
            ));
        }
        let manifest = IndexGenerationManifest::decode(&bytes)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        if manifest.index_id != job.definition.index_id
            || manifest.generation != reference.generation
            || manifest.definition_version != reference.definition_version
        {
            return Err(Status::data_loss(
                "index predecessor manifest identity differs from its reference",
            ));
        }
        Ok(LoadedPredecessor::Present(manifest, bytes.len() as u64))
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
}

pub(crate) struct IndexRetentionTask {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for IndexRetentionTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RetentionIdentity {
    tenant_id: u64,
    bucket_id: u64,
    index_id: u64,
}

impl RetentionIdentity {
    fn new(tenant_id: u64, bucket_id: u64, index_id: u64) -> Result<Self, Status> {
        if tenant_id == 0 || bucket_id == 0 || index_id == 0 {
            return Err(Status::data_loss(
                "index retention identity contains a zero stable ID",
            ));
        }
        Ok(Self {
            tenant_id,
            bucket_id,
            index_id,
        })
    }
}

#[derive(Default)]
struct RetentionScheduler {
    jobs: BTreeMap<RetentionIdentity, RetentionJob>,
    running: BTreeMap<RetentionIdentity, u64>,
    ready: VecDeque<RetentionIdentity>,
    queued: BTreeSet<RetentionIdentity>,
    delayed: BTreeMap<RetentionIdentity, (Instant, u64)>,
    waiting: BTreeSet<RetentionIdentity>,
}

impl RetentionScheduler {
    fn register(&mut self, identity: RetentionIdentity, job: RetentionJob) -> Result<(), Status> {
        let known_generation = self
            .jobs
            .get(&identity)
            .map(|job| job.generation)
            .into_iter()
            .chain(self.running.get(&identity).copied())
            .max();
        if known_generation.is_some_and(|known| known >= job.generation) {
            return Ok(());
        }
        if known_generation.is_none() && self.active_len() >= MAX_ACTIVE_RETENTION_JOBS {
            return Err(Status::resource_exhausted(
                "node-wide active index retention lease limit reached",
            ));
        }
        self.waiting.remove(&identity);
        self.delayed.remove(&identity);
        self.jobs.insert(identity, job);
        self.queue_if_runnable(identity);
        Ok(())
    }

    fn pop(&mut self) -> Option<(RetentionIdentity, RetentionJob)> {
        let now = Instant::now();
        let due = self
            .delayed
            .iter()
            .filter_map(|(identity, (due, generation))| {
                (*due <= now).then_some((*identity, *generation))
            })
            .collect::<Vec<_>>();
        for (identity, generation) in due {
            self.delayed.remove(&identity);
            if self
                .jobs
                .get(&identity)
                .is_some_and(|job| job.generation == generation)
            {
                self.waiting.remove(&identity);
                self.queue_if_runnable(identity);
            }
        }
        while let Some(identity) = self.ready.pop_front() {
            self.queued.remove(&identity);
            if let Some(job) = self.jobs.remove(&identity) {
                self.running.insert(identity, job.generation);
                return Some((identity, job));
            }
        }
        None
    }

    fn requeue(&mut self, identity: RetentionIdentity, job: RetentionJob) {
        if self.running.remove(&identity) != Some(job.generation) {
            self.queue_if_runnable(identity);
            return;
        }
        self.waiting.remove(&identity);
        if self
            .jobs
            .get(&identity)
            .is_none_or(|replacement| replacement.generation <= job.generation)
        {
            self.jobs.insert(identity, job);
        }
        self.queue_if_runnable(identity);
    }

    fn complete(&mut self, identity: RetentionIdentity, generation: u64) {
        if self.running.remove(&identity) != Some(generation) {
            self.queue_if_runnable(identity);
            return;
        }
        self.queue_if_runnable(identity);
    }

    fn retry(&mut self, identity: RetentionIdentity, job: RetentionJob, _retry: Duration) {
        // Durable assignment rediscovery is the retry queue. Drop disposable
        // traversal state so one failing retention job cannot pin one of the
        // bounded node-wide leases.
        self.fail(identity, job.generation);
    }

    fn fail(&mut self, identity: RetentionIdentity, generation: u64) {
        if self.running.remove(&identity) != Some(generation) {
            self.queue_if_runnable(identity);
            return;
        }
        self.queued.remove(&identity);
        self.waiting.remove(&identity);
        self.queue_if_runnable(identity);
    }

    fn remove(&mut self, identity: RetentionIdentity) {
        self.jobs.remove(&identity);
        self.running.remove(&identity);
        self.queued.remove(&identity);
        self.ready.retain(|queued| *queued != identity);
        self.waiting.remove(&identity);
        self.delayed.remove(&identity);
    }

    fn queue_if_runnable(&mut self, identity: RetentionIdentity) {
        if self.jobs.contains_key(&identity)
            && !self.running.contains_key(&identity)
            && !self.waiting.contains(&identity)
            && self.queued.insert(identity)
        {
            self.ready.push_back(identity);
        }
    }

    fn active_len(&self) -> usize {
        self.running.len()
            + self
                .jobs
                .keys()
                .filter(|identity| !self.running.contains_key(identity))
                .count()
    }

    fn backlog(&self) -> (usize, u64) {
        let oldest = self
            .jobs
            .values()
            .map(|job| job.started.elapsed())
            .max()
            .unwrap_or_default();
        (
            self.active_len(),
            oldest.as_millis().min(u128::from(u64::MAX)) as u64,
        )
    }
}

struct RetentionJob {
    definition: StoredIndexDefinition,
    tenant_id: u64,
    bucket_id: u64,
    generation: u64,
    current: PublishedGeneration,
    current_validated: bool,
    started: Instant,
    phase: RetentionPhase,
}

impl RetentionJob {
    fn new(
        definition: StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        current: PublishedGeneration,
    ) -> Self {
        let mut retained = RetainedArtifacts::default();
        retained.insert(&current.pointer.manifest_path, &current.manifest);
        let generation = current.manifest.generation;
        let phase = RetentionPhase::Chain(RetentionChain {
            retained,
            retained_count: 1,
            retained_bytes: current.manifest.authoritative_bytes,
            obsolete: false,
            successor_published_at: current.pointer.published_at_unix_millis,
            previous: current.manifest.previous.clone(),
            expected_below: current.manifest.generation,
        });
        Self {
            definition,
            tenant_id,
            bucket_id,
            generation,
            current,
            current_validated: false,
            started: Instant::now(),
            phase,
        }
    }
}

enum RetentionPhase {
    Chain(RetentionChain),
    Delete(RetentionDelete),
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

struct RetentionChain {
    retained: RetainedArtifacts,
    retained_count: u32,
    retained_bytes: u64,
    obsolete: bool,
    successor_published_at: u64,
    previous: Option<ManifestReference>,
    expected_below: u64,
}

struct RetentionDelete {
    chain: RetentionChain,
    reference: ManifestReference,
    manifest: IndexGenerationManifest,
    run_index: usize,
    scan: Option<ClusterIndexScan>,
    pending: VecDeque<IndexCurrentHead>,
}

impl RetentionDelete {
    fn new(
        chain: RetentionChain,
        reference: ManifestReference,
        manifest: IndexGenerationManifest,
    ) -> Self {
        Self {
            chain,
            reference,
            manifest,
            run_index: 0,
            scan: None,
            pending: VecDeque::new(),
        }
    }
}

struct RetentionSweep {
    retained: RetainedArtifacts,
    scan: Option<ClusterIndexScan>,
    pending: VecDeque<SweepCandidate>,
}

impl RetentionSweep {
    fn new(retained: RetainedArtifacts) -> Self {
        Self {
            retained,
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
}

fn sweep_candidates(
    job: &RetentionJob,
    retained: &RetainedArtifacts,
    head: IndexCurrentHead,
    now: u64,
) -> Vec<SweepCandidate> {
    if head.exact_path == current_path(job.definition.index_id) {
        return if head.version.id != head.head.version
            && !head.version.deleted
            && head.version.blob.is_some()
        {
            vec![SweepCandidate {
                path: head.exact_path,
                version: head.version.id,
                class: "current",
                delete: true,
            }]
        } else {
            Vec::new()
        };
    }
    // Artifact pages carry one retained descriptor per record. Only the
    // current descriptor determines whether a non-current-pointer artifact is
    // presently reachable; older retained descriptors are handled by exact
    // version deletion when their generation becomes obsolete.
    if head.version.id != head.head.version {
        return Vec::new();
    }
    let age = now.saturating_sub(head.version.committed_at_unix_millis);
    let retained_path = if is_manifest_artifact_path(job.definition.index_id, &head.exact_path) {
        retained.manifest_paths.contains(&head.exact_path)
    } else if let Some(run_hash) =
        run_hash_from_artifact_path(job.definition.index_id, &head.exact_path)
    {
        retained.run_hashes.contains(&run_hash)
    } else {
        true
    };
    vec![SweepCandidate {
        path: head.exact_path,
        version: head.version.id,
        class: "unreachable",
        delete: !head.head.deleted
            && !head.version.deleted
            && age >= UNREACHABLE_ARTIFACT_SAFETY_MILLIS
            && !retained_path,
    }]
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
}

enum LoadedPredecessor {
    Present(IndexGenerationManifest, u64),
    PreviouslyPruned,
}

fn classify_absent_predecessor(
    referenced_version: VersionId,
    current: Option<&anvil_store::Version>,
) -> Result<LoadedPredecessor, Status> {
    match current {
        Some(version)
            if version.id > referenced_version && version.deleted && version.blob.is_none() =>
        {
            Ok(LoadedPredecessor::PreviouslyPruned)
        }
        Some(_) => Err(Status::data_loss(
            "index predecessor version is absent while its object path remains live",
        )),
        None => Err(Status::data_loss(
            "index predecessor version disappeared without a retention tombstone",
        )),
    }
}

#[derive(Clone, Default)]
struct RetainedArtifacts {
    manifest_paths: BTreeSet<String>,
    run_hashes: BTreeSet<[u8; 32]>,
}

impl RetainedArtifacts {
    fn insert(&mut self, path: &str, manifest: &IndexGenerationManifest) {
        self.manifest_paths.insert(path.to_owned());
        self.run_hashes
            .extend(manifest.runs.iter().map(|run| run.root_blob.hash));
    }
}

fn require_current_identity(
    definition: &StoredIndexDefinition,
    current: &PublishedGeneration,
) -> Result<(), Status> {
    if current.pointer.index_id != definition.index_id
        || current.manifest.index_id != definition.index_id
        || current.pointer.generation != current.manifest.generation
        || current.pointer.definition_version != current.manifest.definition_version
        || current.pointer.manifest_path
            != super::publication::manifest_path(
                definition.index_id,
                current.pointer.manifest_blob.hash,
            )
    {
        return Err(Status::data_loss(
            "current index pointer and manifest identity differ during retention",
        ));
    }
    Ok(())
}

fn validate_predecessor(
    reference: &ManifestReference,
    expected_below: u64,
    index_id: u64,
) -> Result<(), Status> {
    if reference.generation >= expected_below
        || reference.path != super::publication::manifest_path(index_id, reference.blob.hash)
    {
        return Err(Status::data_loss(
            "index predecessor chain is non-canonical or cyclic",
        ));
    }
    Ok(())
}

fn retain_predecessor(
    retained_count: u32,
    retained_bytes: u64,
    reference: &ManifestReference,
    manifest: &IndexGenerationManifest,
    now: u64,
    config: IndexRuntimeConfig,
) -> bool {
    let within_count = retained_count < config.max_retained_generations();
    let within_age = now.saturating_sub(reference.published_at_unix_millis)
        < config
            .max_generation_age_hours()
            .saturating_mul(60 * 60 * 1_000);
    let within_bytes = retained_bytes
        .checked_add(manifest.authoritative_bytes)
        .is_some_and(|total| total <= config.max_retained_generation_bytes());
    within_count && within_age && within_bytes
}

fn delete_command(index_id: u64, version: VersionId, class: &str, path: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(class.as_bytes());
    hasher.update(path.as_bytes());
    hasher.update(&version.0.to_be_bytes());
    format!(
        "index-v2-gc-{index_id}-{}",
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
mod tests {
    use anvil_api::v1::{CreateIndexRequest, IndexSpecification, PathIndexSpec};
    use anvil_store::{BlobRef, Head, PlacementLogId, SourceId, Version};
    use tonic::Code;

    use super::*;
    use crate::index_runtime::events::{AtomicProgramWatermark, IndexBarrier, IndexSourceCursor};
    use crate::index_runtime::generation::{IndexGenerationManifest, ManifestRun};

    fn config(count: u32, age: u64, bytes: u64) -> IndexRuntimeConfig {
        IndexRuntimeConfig::new(1, 1, 64 * 1024 * 1024, 1, count, age, bytes).unwrap()
    }

    fn manifest(generation: u64, bytes: u64, run_hash: [u8; 32]) -> IndexGenerationManifest {
        let barrier = IndexBarrier {
            fence: PlacementLogId { term: 1, index: 1 },
            atomic: AtomicProgramWatermark::new(None, None, 0),
            sources: [(
                anvil_consensus::NodeId(1),
                IndexSourceCursor {
                    source: SourceId {
                        node_id: 1,
                        source_epoch: [1; 32],
                    },
                    next_offset: 1,
                },
            )]
            .into_iter()
            .collect(),
        };
        IndexGenerationManifest::new(
            9,
            generation,
            1,
            anvil_index::IndexKind::Path,
            &barrier,
            vec![ManifestRun {
                sequence: generation,
                level: 0,
                root_path: super::super::publication::run_root_path(9, run_hash),
                root_blob: BlobRef {
                    hash: run_hash,
                    length: 10,
                },
                root_object_version: VersionId(generation),
                mutation_count: 1,
                live_document_count: 1,
                minimum_version: 1,
                maximum_version: 1,
                authoritative_bytes: bytes,
            }],
            None,
            1,
            0,
        )
        .unwrap()
    }

    fn retention_job(tenant_id: u64, bucket_id: u64, generation: u64) -> RetentionJob {
        let definition = StoredIndexDefinition::create(
            format!("tenant-{tenant_id}"),
            CreateIndexRequest {
                bucket: format!("bucket-{bucket_id}"),
                name: "path".to_owned(),
                path_prefix: String::new(),
                content_type: String::new(),
                specification: Some(IndexSpecification {
                    specification: Some(anvil_api::v1::index_specification::Specification::Path(
                        PathIndexSpec {},
                    )),
                }),
                command_id: format!("create-{tenant_id}-{bucket_id}"),
            },
            9,
        )
        .unwrap();
        let manifest = manifest(generation, 10, [generation as u8; 32]);
        let manifest_blob = BlobRef {
            hash: [generation.saturating_add(64) as u8; 32],
            length: 10,
        };
        let pointer = IndexCurrentPointer::new(
            &manifest,
            manifest_blob,
            VersionId(generation),
            UNIX_EPOCH + Duration::from_secs(1),
        )
        .unwrap();
        RetentionJob::new(
            definition,
            tenant_id,
            bucket_id,
            PublishedGeneration {
                pointer,
                current_object_version: VersionId(generation.saturating_add(100)),
                manifest,
            },
        )
    }

    #[test]
    fn first_count_age_or_byte_bound_stops_the_retained_prefix() {
        let candidate = manifest(2, 40, [2; 32]);
        let reference = ManifestReference {
            generation: 2,
            definition_version: 1,
            path: super::super::publication::manifest_path(9, [8; 32]),
            blob: BlobRef {
                hash: [8; 32],
                length: 10,
            },
            object_version: VersionId(2),
            published_at_unix_millis: 90_000_000,
        };
        assert!(!retain_predecessor(
            2,
            40,
            &reference,
            &candidate,
            100_000_000,
            config(2, 24, 100)
        ));
        assert!(!retain_predecessor(
            1,
            40,
            &reference,
            &candidate,
            200_000_001,
            config(3, 24, 100)
        ));
        assert!(!retain_predecessor(
            1,
            70,
            &reference,
            &candidate,
            100_000_000,
            config(3, 24, 100)
        ));
    }

    #[test]
    fn repeated_collection_stops_at_a_proven_pruned_predecessor() {
        let referenced = VersionId(41);
        let tombstone = Version {
            id: VersionId(42),
            blob: None,
            content_type: None,
            deleted: true,
            committed_at_unix_millis: 100,
        };
        for _ in 0..2 {
            assert!(matches!(
                classify_absent_predecessor(referenced, Some(&tombstone)),
                Ok(LoadedPredecessor::PreviouslyPruned)
            ));
        }
    }

    #[test]
    fn absent_live_or_unmarked_predecessors_fail_closed() {
        let referenced = VersionId(41);
        let live = Version {
            id: VersionId(42),
            blob: Some(BlobRef {
                hash: [4; 32],
                length: 10,
            }),
            content_type: None,
            deleted: false,
            committed_at_unix_millis: 100,
        };
        let stale_tombstone = Version {
            id: referenced,
            blob: None,
            content_type: None,
            deleted: true,
            committed_at_unix_millis: 100,
        };
        for current in [Some(&live), Some(&stale_tombstone), None] {
            let error = match classify_absent_predecessor(referenced, current) {
                Ok(_) => panic!("unproven predecessor pruning must fail closed"),
                Err(error) => error,
            };
            assert_eq!(error.code(), Code::DataLoss);
        }
    }

    #[test]
    fn retention_budget_rejects_a_limit_smaller_than_one_record() {
        assert!(
            IndexRetentionBudget::new(1, MAX_RETENTION_RECORD_BYTES - 1, Duration::from_secs(1))
                .is_err()
        );
        assert!(
            IndexRetentionBudget::new(1, MAX_RETENTION_RECORD_BYTES, Duration::from_secs(1))
                .is_ok()
        );
    }

    #[test]
    fn retention_schedule_rejects_zero_intervals() {
        assert!(IndexRetentionSchedule::new(Duration::ZERO, Duration::from_secs(1),).is_err());
        assert!(IndexRetentionSchedule::new(Duration::from_secs(1), Duration::ZERO,).is_err());
    }

    #[test]
    fn retention_scheduler_gives_each_identity_a_turn() {
        let mut scheduler = RetentionScheduler::default();
        let first = RetentionIdentity::new(1, 2, 9).unwrap();
        let second = RetentionIdentity::new(3, 4, 9).unwrap();
        scheduler.register(first, retention_job(1, 2, 1)).unwrap();
        scheduler.register(second, retention_job(3, 4, 1)).unwrap();

        let (selected, job) = scheduler.pop().unwrap();
        assert_eq!(selected, first);
        scheduler.requeue(selected, job);

        assert_eq!(scheduler.pop().unwrap().0, second);
    }

    #[test]
    fn transient_retention_failure_yields_a_lease_to_a_later_definition() {
        let mut scheduler = RetentionScheduler::default();
        for tenant_id in 1..=MAX_ACTIVE_RETENTION_JOBS as u64 {
            let identity = RetentionIdentity::new(tenant_id, 2, 9).unwrap();
            scheduler
                .register(identity, retention_job(tenant_id, 2, 3))
                .unwrap();
        }
        let later = RetentionIdentity::new(MAX_ACTIVE_RETENTION_JOBS as u64 + 1, 2, 9).unwrap();
        assert!(
            scheduler
                .register(
                    later,
                    retention_job(MAX_ACTIVE_RETENTION_JOBS as u64 + 1, 2, 3),
                )
                .is_err()
        );

        let (failing, job) = scheduler.pop().unwrap();
        scheduler.retry(failing, job, Duration::ZERO);

        assert_eq!(scheduler.active_len(), MAX_ACTIVE_RETENTION_JOBS - 1);
        scheduler
            .register(
                later,
                retention_job(MAX_ACTIVE_RETENTION_JOBS as u64 + 1, 2, 3),
            )
            .unwrap();
        assert_eq!(scheduler.active_len(), MAX_ACTIVE_RETENTION_JOBS);
    }

    #[test]
    fn completed_retention_job_releases_its_bounded_lease() {
        let mut scheduler = RetentionScheduler::default();
        let identity = RetentionIdentity::new(1, 2, 9).unwrap();
        scheduler
            .register(identity, retention_job(1, 2, 3))
            .unwrap();
        let (_, job) = scheduler.pop().unwrap();
        scheduler.complete(identity, job.generation);
        assert_eq!(scheduler.active_len(), 0);
        assert!(scheduler.pop().is_none());
    }

    #[test]
    fn artifact_sweep_consumes_one_retained_descriptor_at_a_time() {
        let job = retention_job(1, 2, 3);
        let path = current_path(9);
        let old = Version {
            id: VersionId(2),
            blob: Some(BlobRef {
                hash: [2; 32],
                length: 10,
            }),
            content_type: None,
            deleted: false,
            committed_at_unix_millis: 1,
        };
        let head = IndexCurrentHead {
            tenant_id: 1,
            bucket_id: 2,
            exact_path: path.clone(),
            head: Head {
                version: VersionId(3),
                deleted: false,
                mutation_stamp: None,
            },
            version: old.clone(),
            versions: vec![old.clone()],
        };

        let candidates = sweep_candidates(&job, &RetainedArtifacts::default(), head, u64::MAX);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].path, path);
        assert_eq!(candidates[0].version, old.id);
        assert!(candidates[0].delete);

        let noncurrent_artifact = IndexCurrentHead {
            tenant_id: 1,
            bucket_id: 2,
            exact_path: super::super::publication::manifest_path(9, [8; 32]),
            head: Head {
                version: VersionId(3),
                deleted: false,
                mutation_stamp: None,
            },
            version: old.clone(),
            versions: vec![old],
        };
        assert!(
            sweep_candidates(
                &job,
                &RetainedArtifacts::default(),
                noncurrent_artifact,
                u64::MAX,
            )
            .is_empty()
        );
    }
}
