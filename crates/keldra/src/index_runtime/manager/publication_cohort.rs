//! Bounded, cross-index collection for one physical publication stage.
//!
//! One scheduler instance represents one publication stage. It admits at most
//! one candidate per index until that candidate has either been discarded or
//! has received its independent physical-publication outcome. Incremental and
//! maintenance candidates use separate bounded queues and workers so a slow
//! maintenance batch cannot occupy incremental publication capacity.

use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tonic::Status;

use crate::index_runtime::events::{IndexBarrier, IndexEventJournal};
use crate::index_runtime::publication::{
    DerivedArtifactAdmission, GuardedIndexArtifactCohort, GuardedIndexArtifactPublish,
    IndexArtifactOutcome, IndexArtifactPublicationOutcome, IndexArtifactPublish,
    IndexArtifactRouter, MAX_INDEX_ARTIFACT_BATCH_BYTES, MAX_INDEX_ARTIFACT_BATCH_ITEMS,
};

use super::publication::IndexPublicationSlots;
use super::support::event_status;

const MAX_COLLECTION_DELAY: Duration = Duration::from_millis(5);
const MAX_QUEUED_CANDIDATES: usize = 256;
const MAX_INCREMENTAL_PHYSICAL_BATCHES: usize = 2;
const MAX_MAINTENANCE_PHYSICAL_BATCHES: usize = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PublicationCohortClass {
    Incremental,
    Maintenance,
}

impl PublicationCohortClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Incremental => "incremental",
            Self::Maintenance => "maintenance",
        }
    }
}

/// Bounds apply independently to the incremental and maintenance queues.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PublicationCohortBounds {
    pub(crate) max_queued_candidates: usize,
    pub(crate) max_batch_items: u64,
    pub(crate) max_batch_bytes: u64,
    pub(crate) max_collection_delay: Duration,
    pub(crate) max_incremental_batches: usize,
    pub(crate) max_maintenance_batches: usize,
}

impl PublicationCohortBounds {
    pub(crate) fn new(
        max_queued_candidates: usize,
        max_batch_items: u64,
        max_batch_bytes: u64,
        max_collection_delay: Duration,
        max_incremental_batches: usize,
        max_maintenance_batches: usize,
    ) -> Self {
        assert!(
            max_queued_candidates > 0,
            "cohort queue bound must be positive"
        );
        assert!(max_batch_items > 0, "cohort item bound must be positive");
        assert!(max_batch_bytes > 0, "cohort byte bound must be positive");
        assert!(
            max_incremental_batches > 0,
            "incremental physical batch concurrency must be positive"
        );
        assert!(
            max_maintenance_batches > 0,
            "maintenance physical batch concurrency must be positive"
        );
        assert!(
            max_collection_delay <= MAX_COLLECTION_DELAY,
            "cohort collection delay must not exceed 5ms"
        );
        Self {
            max_queued_candidates,
            max_batch_items,
            max_batch_bytes,
            max_collection_delay,
            max_incremental_batches,
            max_maintenance_batches,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum PublicationCohortError<E> {
    DuplicateIndex,
    EmptyCandidate,
    CandidateTooLarge,
    Closed,
    InvalidOutcomeCount,
    Publication(E),
}

impl<E: fmt::Display> fmt::Display for PublicationCohortError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateIndex => {
                formatter.write_str("an index already has a candidate in this publication stage")
            }
            Self::EmptyCandidate => formatter.write_str("publication candidate is empty"),
            Self::CandidateTooLarge => {
                formatter.write_str("publication candidate exceeds the physical batch bounds")
            }
            Self::Closed => formatter.write_str("publication cohort scheduler is closed"),
            Self::InvalidOutcomeCount => formatter.write_str(
                "physical publication outcome count differs from submitted candidate count",
            ),
            Self::Publication(error) => write!(formatter, "physical publication failed: {error}"),
        }
    }
}

/// A stage-specific, node-local batching boundary.
///
/// `K` identifies a physical cohort (for example a replica group and admission
/// class). `I` is the stable index identity. `P` owns everything that must live
/// through publication, including a current-mutation guard for guarded stages.
/// Concrete payloads contain durable `BlobRef` metadata, not blob contents, so
/// the bounded candidate count also bounds retained queue memory.
pub(crate) struct PublicationCohortScheduler<I: Eq + Hash, K, P, O, E> {
    incremental: QueueHandle<I, K, P, O, E>,
    maintenance: QueueHandle<I, K, P, O, E>,
    active: Arc<Mutex<HashSet<I>>>,
    bounds: PublicationCohortBounds,
}

impl<I: Eq + Hash, K, P, O, E> Clone for PublicationCohortScheduler<I, K, P, O, E> {
    fn clone(&self) -> Self {
        Self {
            incremental: self.incremental.clone(),
            maintenance: self.maintenance.clone(),
            active: self.active.clone(),
            bounds: self.bounds,
        }
    }
}

struct QueueHandle<I: Eq + Hash, K, P, O, E> {
    sender: mpsc::Sender<QueuedCandidate<I, K, P, O, E>>,
    capacity: Arc<Semaphore>,
}

impl<I: Eq + Hash, K, P, O, E> Clone for QueueHandle<I, K, P, O, E> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            capacity: self.capacity.clone(),
        }
    }
}

impl<I, K, P, O, E> PublicationCohortScheduler<I, K, P, O, E>
where
    I: Clone + Eq + Hash + Send + 'static,
    K: Clone + Eq + Send + 'static,
    P: Send + 'static,
    O: Send + 'static,
    E: Send + 'static,
{
    /// Starts independent incremental and maintenance workers. The dispatcher
    /// must preserve input order and return one independent outcome per input.
    pub(crate) fn start<F, Fut>(bounds: PublicationCohortBounds, dispatch: F) -> Self
    where
        F: Fn(PublicationCohortClass, K, Vec<P>) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Vec<Result<O, E>>> + Send + 'static,
    {
        Self::start_with_admission(bounds, |_class| async { Ok::<(), E>(()) }, dispatch)
    }

    /// Starts workers whose physical admission is acquired before an epoch is
    /// frozen. Candidates continue accumulating while the shared lane is busy.
    pub(crate) fn start_with_admission<A, Acquire, AcquireFuture, F, Fut>(
        bounds: PublicationCohortBounds,
        acquire: Acquire,
        dispatch: F,
    ) -> Self
    where
        A: Send + 'static,
        Acquire: Fn(PublicationCohortClass) -> AcquireFuture + Clone + Send + 'static,
        AcquireFuture: Future<Output = Result<A, E>> + Send + 'static,
        F: Fn(PublicationCohortClass, K, Vec<P>) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Vec<Result<O, E>>> + Send + 'static,
    {
        let active = Arc::new(Mutex::new(HashSet::new()));
        let incremental = start_queue(
            PublicationCohortClass::Incremental,
            bounds,
            acquire.clone(),
            dispatch.clone(),
        );
        let maintenance = start_queue(
            PublicationCohortClass::Maintenance,
            bounds,
            acquire,
            dispatch,
        );
        Self {
            incremental,
            maintenance,
            active,
            bounds,
        }
    }

    /// Submits one index candidate to this stage. Queue admission and the
    /// response channel are cancellation-safe: cancellation before dispatch
    /// discards the disposable queued candidate, while cancellation after
    /// dispatch does not interrupt the physical publication.
    pub(crate) async fn submit(
        &self,
        class: PublicationCohortClass,
        index: I,
        cohort: K,
        payload: P,
        logical_items: u64,
        logical_bytes: u64,
        largest_item_bytes: u64,
    ) -> Result<O, PublicationCohortError<E>> {
        if logical_items == 0 || logical_bytes == 0 || largest_item_bytes == 0 {
            return Err(PublicationCohortError::EmptyCandidate);
        }
        if largest_item_bytes > self.bounds.max_batch_bytes {
            return Err(PublicationCohortError::CandidateTooLarge);
        }
        let queue = match class {
            PublicationCohortClass::Incremental => &self.incremental,
            PublicationCohortClass::Maintenance => &self.maintenance,
        };
        let capacity = queue
            .capacity
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| PublicationCohortError::Closed)?;
        let active = ActiveIndex::reserve(self.active.clone(), index)
            .map_err(|()| PublicationCohortError::DuplicateIndex)?;
        let (completion, response) = oneshot::channel();
        queue
            .sender
            .send(QueuedCandidate {
                cohort,
                payload,
                logical_items,
                logical_bytes,
                queued_at: Instant::now(),
                completion,
                _capacity: capacity,
                _active: active,
            })
            .await
            .map_err(|_| PublicationCohortError::Closed)?;
        response
            .await
            .unwrap_or(Err(PublicationCohortError::Closed))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PhysicalPublicationCohort {
    storage_tenant: String,
    bucket: String,
    tenant_id: u64,
    bucket_id: u64,
    admission: DerivedArtifactAdmission,
}

type StableIndexIdentity = (u64, u64, u64);
type ImmutablePublicationScheduler = PublicationCohortScheduler<
    StableIndexIdentity,
    PhysicalPublicationCohort,
    Vec<IndexArtifactPublish>,
    Vec<IndexArtifactPublicationOutcome>,
    Status,
>;
type CurrentPublicationScheduler = PublicationCohortScheduler<
    StableIndexIdentity,
    GuardedIndexArtifactCohort,
    CurrentPublicationCandidate,
    IndexArtifactPublicationOutcome,
    Status,
>;

struct CurrentPublicationCandidate {
    barrier: IndexBarrier,
    guarded: GuardedIndexArtifactPublish,
}

/// The concrete node-local cohort boundary used by index publication.
///
/// Pack and manifest stages are deliberately separate: a manifest cannot be
/// encoded until every pack has an exact durable outcome. Current publication
/// is separate again because its payload owns the current-mutation guard.
#[derive(Clone)]
pub(crate) struct IndexPublicationCohorts {
    packs: ImmutablePublicationScheduler,
    manifests: ImmutablePublicationScheduler,
    currents: CurrentPublicationScheduler,
    current_router: IndexArtifactRouter,
}

impl IndexPublicationCohorts {
    pub(crate) fn new(
        router: IndexArtifactRouter,
        journal: Arc<IndexEventJournal>,
        slots: IndexPublicationSlots,
    ) -> Self {
        let bounds = PublicationCohortBounds::new(
            MAX_QUEUED_CANDIDATES,
            MAX_INDEX_ARTIFACT_BATCH_ITEMS as u64,
            MAX_INDEX_ARTIFACT_BATCH_BYTES,
            MAX_COLLECTION_DELAY,
            MAX_INCREMENTAL_PHYSICAL_BATCHES,
            MAX_MAINTENANCE_PHYSICAL_BATCHES,
        );
        Self {
            packs: immutable_scheduler(router.clone(), slots.clone(), bounds),
            manifests: immutable_scheduler(router.clone(), slots.clone(), bounds),
            currents: current_scheduler(router.clone(), journal, slots, bounds),
            current_router: router,
        }
    }

    /// Publishes all immutable packs for one index candidate. Each item keeps
    /// its independent outcome so the publisher can retry only transient
    /// failures while retaining successful content-addressed packs.
    pub(crate) async fn publish_packs(
        &self,
        requests: Vec<IndexArtifactPublish>,
        class: PublicationCohortClass,
    ) -> Result<Vec<IndexArtifactPublicationOutcome>, Status> {
        self.publish_immutable(&self.packs, requests, class).await
    }

    pub(crate) async fn publish_manifest(
        &self,
        request: IndexArtifactPublish,
        class: PublicationCohortClass,
    ) -> Result<IndexArtifactOutcome, Status> {
        let mut outcomes = self
            .publish_immutable(&self.manifests, vec![request], class)
            .await?;
        outcomes
            .pop()
            .ok_or_else(|| Status::internal("manifest cohort returned no outcome"))?
    }

    pub(crate) async fn publish_current(
        &self,
        guarded: GuardedIndexArtifactPublish,
        barrier: IndexBarrier,
        class: PublicationCohortClass,
    ) -> Result<IndexArtifactOutcome, Status> {
        let identity = request_identity(&guarded.request)?;
        let cohort = self
            .current_router
            .guarded_publication_cohort(&guarded.request)?;
        let bytes = guarded.request.blob.length;
        self.currents
            .submit(
                class,
                identity,
                cohort,
                CurrentPublicationCandidate { barrier, guarded },
                1,
                bytes,
                bytes,
            )
            .await
            .map_err(cohort_status)?
    }

    async fn publish_immutable(
        &self,
        scheduler: &ImmutablePublicationScheduler,
        requests: Vec<IndexArtifactPublish>,
        class: PublicationCohortClass,
    ) -> Result<Vec<IndexArtifactPublicationOutcome>, Status> {
        let first = requests
            .first()
            .ok_or_else(|| Status::invalid_argument("immutable publication candidate is empty"))?;
        let identity = request_identity(first)?;
        let cohort = physical_cohort(first);
        let logical_items = requests.len() as u64;
        let mut bytes = 0_u64;
        let mut largest = 0_u64;
        for request in &requests {
            if request_identity(request)? != identity || physical_cohort(request) != cohort {
                return Err(Status::invalid_argument(
                    "one immutable publication candidate must share its index and physical cohort",
                ));
            }
            bytes = bytes.saturating_add(request.blob.length);
            largest = largest.max(request.blob.length);
        }
        scheduler
            .submit(
                class,
                identity,
                cohort,
                requests,
                logical_items,
                bytes,
                largest,
            )
            .await
            .map_err(cohort_status)
    }
}

fn immutable_scheduler(
    router: IndexArtifactRouter,
    slots: IndexPublicationSlots,
    bounds: PublicationCohortBounds,
) -> ImmutablePublicationScheduler {
    PublicationCohortScheduler::start_with_admission(
        bounds,
        {
            let slots = slots.clone();
            move |class| {
                let slots = slots.clone();
                async move { acquire_physical_slot(&slots, class).await }
            }
        },
        move |_class, _cohort, candidates| {
            let router = router.clone();
            async move {
                let candidate_lengths = candidates.iter().map(Vec::len).collect::<Vec<_>>();
                let candidate_count = candidate_lengths.len();
                let requests = candidates.into_iter().flatten().collect::<Vec<_>>();
                match router.publish_immutable_cohort(requests).await {
                    Ok(outcomes) => partition_immutable_outcomes(outcomes, &candidate_lengths),
                    Err(error) => repeated_batch_error(candidate_count, error),
                }
            }
        },
    )
}

fn current_scheduler(
    router: IndexArtifactRouter,
    journal: Arc<IndexEventJournal>,
    slots: IndexPublicationSlots,
    bounds: PublicationCohortBounds,
) -> CurrentPublicationScheduler {
    PublicationCohortScheduler::start_with_admission(
        bounds,
        {
            let slots = slots.clone();
            move |class| {
                let slots = slots.clone();
                async move { acquire_physical_slot(&slots, class).await }
            }
        },
        move |_class, _cohort, candidates| {
            let router = router.clone();
            let journal = journal.clone();
            async move { publish_validated_currents(&router, &journal, candidates).await }
        },
    )
}

async fn publish_validated_currents(
    router: &IndexArtifactRouter,
    journal: &IndexEventJournal,
    candidates: Vec<CurrentPublicationCandidate>,
) -> Vec<Result<IndexArtifactPublicationOutcome, Status>> {
    let candidate_count = candidates.len();
    let barriers = candidates
        .iter()
        .map(|candidate| candidate.barrier.clone())
        .collect::<Vec<_>>();
    let validations = journal.validate_publication_barriers(&barriers).await;
    if validations.len() != candidate_count {
        return repeated_batch_error(
            candidate_count,
            Status::internal("current cohort barrier validation returned an invalid outcome count"),
        );
    }

    let mut outcomes = std::iter::repeat_with(|| None)
        .take(candidate_count)
        .collect::<Vec<_>>();
    let mut valid_indices = Vec::with_capacity(candidate_count);
    let mut guarded = Vec::with_capacity(candidate_count);
    for (index, (candidate, validation)) in candidates.into_iter().zip(validations).enumerate() {
        match validation {
            Ok(()) => {
                valid_indices.push(index);
                guarded.push(candidate.guarded);
            }
            Err(error) => outcomes[index] = Some(Ok(Err(event_status(error)))),
        }
    }

    if !guarded.is_empty() {
        let published = match router.publish_guarded_cohort(guarded).await {
            Ok(published) if published.len() == valid_indices.len() => published
                .into_iter()
                .map(|outcome| Ok(outcome))
                .collect::<Vec<_>>(),
            Ok(_) => repeated_batch_error(
                valid_indices.len(),
                Status::internal("guarded cohort returned an invalid outcome count"),
            ),
            Err(error) => repeated_batch_error(valid_indices.len(), error),
        };
        for (index, outcome) in valid_indices.into_iter().zip(published) {
            outcomes[index] = Some(outcome);
        }
    }

    outcomes
        .into_iter()
        .map(|outcome| {
            outcome.unwrap_or_else(|| {
                Err(Status::internal(
                    "current cohort publication left an unresolved outcome",
                ))
            })
        })
        .collect()
}

async fn acquire_physical_slot(
    slots: &IndexPublicationSlots,
    class: PublicationCohortClass,
) -> Result<tokio::sync::OwnedSemaphorePermit, Status> {
    match class {
        PublicationCohortClass::Incremental => slots.acquire_incremental().await,
        PublicationCohortClass::Maintenance => slots.acquire_maintenance().await,
    }
}

fn partition_immutable_outcomes(
    outcomes: Vec<IndexArtifactPublicationOutcome>,
    candidate_lengths: &[usize],
) -> Vec<Result<Vec<IndexArtifactPublicationOutcome>, Status>> {
    let candidate_count = candidate_lengths.len();
    if outcomes.len() != candidate_lengths.iter().sum::<usize>() {
        return repeated_batch_error(
            candidate_count,
            Status::internal("immutable cohort returned an invalid outcome count"),
        );
    }
    let mut outcomes = outcomes.into_iter();
    candidate_lengths
        .iter()
        .map(|length| Ok(outcomes.by_ref().take(*length).collect()))
        .collect()
}

fn repeated_batch_error<T>(count: usize, error: Status) -> Vec<Result<T, Status>> {
    (0..count)
        .map(|_| Err(Status::new(error.code(), error.message().to_owned())))
        .collect()
}

fn request_identity(request: &IndexArtifactPublish) -> Result<StableIndexIdentity, Status> {
    if request.tenant_id == 0 || request.bucket_id == 0 || request.index_id == 0 {
        return Err(Status::invalid_argument(
            "index publication identity must be non-zero",
        ));
    }
    Ok((request.tenant_id, request.bucket_id, request.index_id))
}

fn physical_cohort(request: &IndexArtifactPublish) -> PhysicalPublicationCohort {
    PhysicalPublicationCohort {
        storage_tenant: request.storage_tenant.clone(),
        bucket: request.bucket.clone(),
        tenant_id: request.tenant_id,
        bucket_id: request.bucket_id,
        admission: request.admission,
    }
}

fn cohort_status(error: PublicationCohortError<Status>) -> Status {
    match error {
        PublicationCohortError::DuplicateIndex => {
            Status::aborted("index already has an active candidate in this publication stage")
        }
        PublicationCohortError::EmptyCandidate => {
            Status::invalid_argument("publication candidate is empty")
        }
        PublicationCohortError::CandidateTooLarge => {
            Status::resource_exhausted("one publication item exceeds the physical batch bound")
        }
        PublicationCohortError::Closed => {
            Status::unavailable("publication cohort scheduler is closed")
        }
        PublicationCohortError::InvalidOutcomeCount => {
            Status::internal("publication cohort returned an invalid outcome count")
        }
        PublicationCohortError::Publication(error) => error,
    }
}

struct ActiveIndex<I: Eq + Hash> {
    active: Arc<Mutex<HashSet<I>>>,
    index: Option<I>,
}

impl<I: Clone + Eq + Hash> ActiveIndex<I> {
    fn reserve(active: Arc<Mutex<HashSet<I>>>, index: I) -> Result<Self, ()> {
        let mut entries = active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !entries.insert(index.clone()) {
            return Err(());
        }
        drop(entries);
        Ok(Self {
            active,
            index: Some(index),
        })
    }
}

impl<I: Eq + Hash> Drop for ActiveIndex<I> {
    fn drop(&mut self) {
        if let Some(index) = self.index.take() {
            self.active
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&index);
        }
    }
}

struct QueuedCandidate<I: Eq + Hash, K, P, O, E> {
    cohort: K,
    payload: P,
    logical_items: u64,
    logical_bytes: u64,
    queued_at: Instant,
    completion: oneshot::Sender<Result<O, PublicationCohortError<E>>>,
    _capacity: OwnedSemaphorePermit,
    _active: ActiveIndex<I>,
}

fn start_queue<I, K, P, O, E, A, Acquire, AcquireFuture, F, Fut>(
    class: PublicationCohortClass,
    bounds: PublicationCohortBounds,
    acquire: Acquire,
    dispatch: F,
) -> QueueHandle<I, K, P, O, E>
where
    I: Eq + Hash + Send + 'static,
    K: Clone + Eq + Send + 'static,
    P: Send + 'static,
    O: Send + 'static,
    E: Send + 'static,
    A: Send + 'static,
    Acquire: Fn(PublicationCohortClass) -> AcquireFuture + Clone + Send + 'static,
    AcquireFuture: Future<Output = Result<A, E>> + Send + 'static,
    F: Fn(PublicationCohortClass, K, Vec<P>) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Vec<Result<O, E>>> + Send + 'static,
{
    let (sender, receiver) = mpsc::channel(bounds.max_queued_candidates);
    tokio::spawn(run_queue(class, bounds, receiver, acquire, dispatch));
    QueueHandle {
        sender,
        capacity: Arc::new(Semaphore::new(bounds.max_queued_candidates)),
    }
}

async fn run_queue<I, K, P, O, E, A, Acquire, AcquireFuture, F, Fut>(
    class: PublicationCohortClass,
    bounds: PublicationCohortBounds,
    mut receiver: mpsc::Receiver<QueuedCandidate<I, K, P, O, E>>,
    acquire: Acquire,
    dispatch: F,
) where
    I: Eq + Hash + Send + 'static,
    K: Clone + Eq + Send + 'static,
    P: Send + 'static,
    O: Send + 'static,
    E: Send + 'static,
    A: Send + 'static,
    Acquire: Fn(PublicationCohortClass) -> AcquireFuture + Clone + Send + 'static,
    AcquireFuture: Future<Output = Result<A, E>> + Send + 'static,
    F: Fn(PublicationCohortClass, K, Vec<P>) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Vec<Result<O, E>>> + Send + 'static,
{
    let mut pending = std::collections::VecDeque::new();
    let max_inflight = match class {
        PublicationCohortClass::Incremental => bounds.max_incremental_batches,
        PublicationCohortClass::Maintenance => bounds.max_maintenance_batches,
    };
    let mut inflight = tokio::task::JoinSet::new();
    loop {
        while inflight.len() >= max_inflight {
            let _ = inflight.join_next().await;
        }
        let Some(mut first) = next_live(&mut pending, &mut receiver).await else {
            break;
        };
        let admission = match tokio::select! {
            result = acquire(class) => Some(result),
            _ = first.completion.closed() => None,
        } {
            None => {
                record_cancelled(class);
                continue;
            }
            Some(result) => match result {
                Ok(admission) => admission,
                Err(error) => {
                    complete_publication_error(first, error);
                    continue;
                }
            },
        };
        if first.completion.is_closed() {
            drop(admission);
            record_cancelled(class);
            continue;
        }
        let cohort = first.cohort.clone();
        let deadline =
            tokio::time::Instant::from_std(first.queued_at + bounds.max_collection_delay);
        let mut items = first.logical_items;
        let mut bytes = first.logical_bytes;
        let mut batch = vec![first];

        collect_pending(
            &cohort,
            bounds,
            &mut items,
            &mut bytes,
            &mut pending,
            &mut batch,
        );
        drain_ready(
            class,
            &cohort,
            bounds,
            &mut items,
            &mut bytes,
            &mut pending,
            &mut receiver,
            &mut batch,
        );
        while items < bounds.max_batch_items && bytes < bounds.max_batch_bytes {
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            let received = tokio::select! {
                candidate = receiver.recv() => candidate,
                _ = tokio::time::sleep_until(deadline) => None,
            };
            let Some(candidate) = received else {
                break;
            };
            if candidate.completion.is_closed() {
                record_cancelled(class);
                continue;
            }
            if candidate.cohort == cohort
                && fits(
                    bounds,
                    items,
                    bytes,
                    candidate.logical_items,
                    candidate.logical_bytes,
                )
            {
                items += candidate.logical_items;
                bytes += candidate.logical_bytes;
                batch.push(candidate);
            } else {
                pending.push_back(candidate);
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
        }

        batch.retain(|candidate| {
            let live = !candidate.completion.is_closed();
            if !live {
                record_cancelled(class);
            }
            live
        });
        if batch.is_empty() {
            drop(admission);
            continue;
        }
        let candidates = batch.len() as u64;
        let items = batch.iter().fold(0_u64, |total, candidate| {
            total.saturating_add(candidate.logical_items)
        });
        let bytes = batch.iter().fold(0_u64, |total, candidate| {
            total.saturating_add(candidate.logical_bytes)
        });
        let oldest_wait = batch
            .iter()
            .map(|candidate| candidate.queued_at.elapsed())
            .max()
            .unwrap_or_default();
        record_batch(class, bounds, candidates, items, bytes, oldest_wait);
        let dispatch = dispatch.clone();
        inflight.spawn(async move {
            dispatch_batch(class, cohort, batch, admission, dispatch).await;
        });
    }
    while inflight.join_next().await.is_some() {}
}

async fn dispatch_batch<I, K, P, O, E, A, F, Fut>(
    class: PublicationCohortClass,
    cohort: K,
    batch: Vec<QueuedCandidate<I, K, P, O, E>>,
    admission: A,
    dispatch: F,
) where
    I: Eq + Hash,
    F: Fn(PublicationCohortClass, K, Vec<P>) -> Fut,
    Fut: Future<Output = Vec<Result<O, E>>>,
{
    let mut completions = Vec::with_capacity(batch.len());
    let mut payloads = Vec::with_capacity(batch.len());
    for candidate in batch {
        completions.push((candidate.completion, candidate._capacity, candidate._active));
        payloads.push(candidate.payload);
    }
    let outcomes = dispatch(class, cohort, payloads).await;
    drop(admission);
    if outcomes.len() != completions.len() {
        for (completion, capacity, active) in completions {
            drop(active);
            drop(capacity);
            let _ = completion.send(Err(PublicationCohortError::InvalidOutcomeCount));
        }
        return;
    }
    for ((completion, capacity, active), outcome) in completions.into_iter().zip(outcomes) {
        // A caller may immediately submit the next candidate after observing
        // this outcome, so release stage identity and queue capacity first.
        drop(active);
        drop(capacity);
        let _ = completion.send(outcome.map_err(PublicationCohortError::Publication));
    }
}

fn complete_publication_error<I, K, P, O, E>(candidate: QueuedCandidate<I, K, P, O, E>, error: E)
where
    I: Eq + Hash,
{
    let QueuedCandidate {
        completion,
        _capacity: capacity,
        _active: active,
        ..
    } = candidate;
    drop(active);
    drop(capacity);
    let _ = completion.send(Err(PublicationCohortError::Publication(error)));
}

async fn next_live<I, K, P, O, E>(
    pending: &mut std::collections::VecDeque<QueuedCandidate<I, K, P, O, E>>,
    receiver: &mut mpsc::Receiver<QueuedCandidate<I, K, P, O, E>>,
) -> Option<QueuedCandidate<I, K, P, O, E>>
where
    I: Eq + Hash,
{
    loop {
        let candidate = match pending.pop_front() {
            Some(candidate) => Some(candidate),
            None => receiver.recv().await,
        }?;
        if candidate.completion.is_closed() {
            continue;
        }
        return Some(candidate);
    }
}

fn collect_pending<I, K, P, O, E>(
    cohort: &K,
    bounds: PublicationCohortBounds,
    items: &mut u64,
    bytes: &mut u64,
    pending: &mut std::collections::VecDeque<QueuedCandidate<I, K, P, O, E>>,
    batch: &mut Vec<QueuedCandidate<I, K, P, O, E>>,
) where
    I: Eq + Hash,
    K: Eq,
{
    let mut cursor = 0;
    while cursor < pending.len() {
        if pending[cursor].completion.is_closed() {
            pending.remove(cursor);
            continue;
        }
        let candidate = &pending[cursor];
        if &candidate.cohort == cohort
            && fits(
                bounds,
                *items,
                *bytes,
                candidate.logical_items,
                candidate.logical_bytes,
            )
        {
            let candidate = pending
                .remove(cursor)
                .expect("bounded pending cursor exists");
            *items += candidate.logical_items;
            *bytes += candidate.logical_bytes;
            batch.push(candidate);
        } else {
            cursor += 1;
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn drain_ready<I, K, P, O, E>(
    class: PublicationCohortClass,
    cohort: &K,
    bounds: PublicationCohortBounds,
    items: &mut u64,
    bytes: &mut u64,
    pending: &mut std::collections::VecDeque<QueuedCandidate<I, K, P, O, E>>,
    receiver: &mut mpsc::Receiver<QueuedCandidate<I, K, P, O, E>>,
    batch: &mut Vec<QueuedCandidate<I, K, P, O, E>>,
) where
    I: Eq + Hash,
    K: Eq,
{
    while *items < bounds.max_batch_items && *bytes < bounds.max_batch_bytes {
        let candidate = match receiver.try_recv() {
            Ok(candidate) => candidate,
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                break;
            }
        };
        if candidate.completion.is_closed() {
            record_cancelled(class);
        } else if &candidate.cohort == cohort
            && fits(
                bounds,
                *items,
                *bytes,
                candidate.logical_items,
                candidate.logical_bytes,
            )
        {
            *items += candidate.logical_items;
            *bytes += candidate.logical_bytes;
            batch.push(candidate);
        } else {
            pending.push_back(candidate);
        }
    }
}

fn fits(
    bounds: PublicationCohortBounds,
    items: u64,
    bytes: u64,
    candidate_items: u64,
    candidate_bytes: u64,
) -> bool {
    items.saturating_add(candidate_items) <= bounds.max_batch_items
        && bytes.saturating_add(candidate_bytes) <= bounds.max_batch_bytes
}

fn record_cancelled(class: PublicationCohortClass) {
    tracing::debug!(
        publication.class = class.as_str(),
        monotonic_counter.keldra_index_publication_cohort_cancelled_total = 1_u64,
        "cancelled index publication cohort candidate discarded"
    );
}

fn record_batch(
    class: PublicationCohortClass,
    bounds: PublicationCohortBounds,
    candidates: u64,
    items: u64,
    bytes: u64,
    oldest_wait: Duration,
) {
    tracing::debug!(
        publication.class = class.as_str(),
        publication.cohort_candidates = candidates,
        publication.cohort_items = items,
        publication.cohort_bytes = bytes,
        monotonic_counter.keldra_index_publication_cohort_logical_candidates_total = candidates,
        monotonic_counter.keldra_index_publication_cohort_logical_items_total = items,
        monotonic_counter.keldra_index_publication_cohort_physical_batches_total = 1_u64,
        histogram.keldra_index_publication_cohort_batch_candidates = candidates,
        histogram.keldra_index_publication_cohort_batch_items = items,
        histogram.keldra_index_publication_cohort_batch_bytes = bytes,
        histogram.keldra_index_publication_cohort_item_fill_ratio =
            (items as f64 / bounds.max_batch_items as f64).min(1.0),
        histogram.keldra_index_publication_cohort_byte_fill_ratio =
            (bytes as f64 / bounds.max_batch_bytes as f64).min(1.0),
        histogram.keldra_index_publication_cohort_wait_seconds = oldest_wait.as_secs_f64(),
        "index publication physical cohort dispatched"
    );
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use keldra_consensus::NodeId;
    use keldra_store::VersionId;
    use tokio::sync::{Mutex as AsyncMutex, Notify};

    use super::*;

    fn bounds(items: u64, bytes: u64) -> PublicationCohortBounds {
        PublicationCohortBounds::new(128, items, bytes, Duration::from_millis(5), 1, 1)
    }

    #[test]
    fn immutable_outcomes_are_partitioned_without_losing_partial_failures() {
        let outcomes = vec![
            Ok(IndexArtifactOutcome {
                version: VersionId(1),
                replayed: false,
            }),
            Err(Status::unavailable("retry pack")),
            Ok(IndexArtifactOutcome {
                version: VersionId(3),
                replayed: true,
            }),
        ];
        let partitioned = partition_immutable_outcomes(outcomes, &[2, 1]);
        assert_eq!(partitioned.len(), 2);
        let first = partitioned[0].as_ref().unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(first[0].as_ref().unwrap().version, VersionId(1));
        assert_eq!(
            first[1].as_ref().unwrap_err().code(),
            tonic::Code::Unavailable
        );
        let second = partitioned[1].as_ref().unwrap();
        assert_eq!(second[0].as_ref().unwrap().version, VersionId(3));
    }

    #[test]
    fn invalid_physical_outcome_count_fails_every_candidate() {
        let partitioned = partition_immutable_outcomes(Vec::new(), &[1, 1]);
        assert_eq!(partitioned.len(), 2);
        assert!(
            partitioned
                .iter()
                .all(|outcome| outcome.as_ref().unwrap_err().code() == tonic::Code::Internal)
        );
    }

    #[tokio::test]
    async fn coalesces_one_four_sixteen_and_sixty_four_candidates() {
        for candidate_count in [1_usize, 4, 16, 64] {
            let batches = Arc::new(AsyncMutex::new(Vec::new()));
            let observed = batches.clone();
            let blocker_entered = Arc::new(Notify::new());
            let release_blocker = Arc::new(Notify::new());
            let scheduler = PublicationCohortScheduler::start(bounds(64, 64), {
                let observed = observed.clone();
                let blocker_entered = blocker_entered.clone();
                let release_blocker = release_blocker.clone();
                move |_class, cohort: u8, payloads: Vec<usize>| {
                    let observed = observed.clone();
                    let blocker_entered = blocker_entered.clone();
                    let release_blocker = release_blocker.clone();
                    async move {
                        observed.lock().await.push((cohort, payloads.len()));
                        if cohort == 0 {
                            blocker_entered.notify_waiters();
                            release_blocker.notified().await;
                        }
                        payloads.into_iter().map(Ok::<_, ()>).collect()
                    }
                }
            });
            let blocker = tokio::spawn({
                let scheduler = scheduler.clone();
                async move {
                    scheduler
                        .submit(
                            PublicationCohortClass::Incremental,
                            usize::MAX,
                            0,
                            usize::MAX,
                            1,
                            1,
                            1,
                        )
                        .await
                }
            });
            blocker_entered.notified().await;
            let mut tasks = Vec::new();
            for index in 0..candidate_count {
                let scheduler = scheduler.clone();
                tasks.push(tokio::spawn(async move {
                    scheduler
                        .submit(
                            PublicationCohortClass::Incremental,
                            index,
                            7,
                            index,
                            1,
                            1,
                            1,
                        )
                        .await
                }));
            }
            while scheduler
                .active
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len()
                != candidate_count + 1
            {
                tokio::task::yield_now().await;
            }
            release_blocker.notify_waiters();
            assert_eq!(blocker.await.unwrap(), Ok(usize::MAX));
            for (index, task) in tasks.into_iter().enumerate() {
                assert_eq!(task.await.unwrap(), Ok(index));
            }
            assert_eq!(*batches.lock().await, vec![(0, 1), (7, candidate_count)]);
        }
    }

    #[tokio::test]
    async fn guarded_routing_tuple_is_the_current_stage_batch_boundary() {
        let blocker = GuardedIndexArtifactCohort::test_key(vec![NodeId(9)], vec![NodeId(9)]);
        let shared = GuardedIndexArtifactCohort::test_key(
            vec![NodeId(1), NodeId(2)],
            vec![NodeId(2), NodeId(3)],
        );
        let different = GuardedIndexArtifactCohort::test_key(
            vec![NodeId(1), NodeId(3)],
            vec![NodeId(2), NodeId(3)],
        );
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let batches = Arc::new(AsyncMutex::new(Vec::new()));
        let scheduler = PublicationCohortScheduler::start(bounds(8, 8), {
            let blocker = blocker.clone();
            let entered = entered.clone();
            let release = release.clone();
            let batches = batches.clone();
            move |_class, cohort: GuardedIndexArtifactCohort, payloads: Vec<u64>| {
                let blocker = blocker.clone();
                let entered = entered.clone();
                let release = release.clone();
                let batches = batches.clone();
                async move {
                    batches.lock().await.push((cohort.clone(), payloads.len()));
                    if cohort == blocker {
                        entered.notify_waiters();
                        release.notified().await;
                    }
                    payloads.into_iter().map(Ok::<_, ()>).collect()
                }
            }
        });
        let blocked = tokio::spawn({
            let scheduler = scheduler.clone();
            let blocker = blocker.clone();
            async move {
                scheduler
                    .submit(
                        PublicationCohortClass::Incremental,
                        99,
                        blocker,
                        99,
                        1,
                        1,
                        1,
                    )
                    .await
            }
        });
        entered.notified().await;
        let first = tokio::spawn({
            let scheduler = scheduler.clone();
            let shared = shared.clone();
            async move {
                scheduler
                    .submit(PublicationCohortClass::Incremental, 1, shared, 1, 1, 1, 1)
                    .await
            }
        });
        let second = tokio::spawn({
            let scheduler = scheduler.clone();
            let shared = shared.clone();
            async move {
                scheduler
                    .submit(PublicationCohortClass::Incremental, 2, shared, 2, 1, 1, 1)
                    .await
            }
        });
        let separate = tokio::spawn({
            let scheduler = scheduler.clone();
            let different = different.clone();
            async move {
                scheduler
                    .submit(
                        PublicationCohortClass::Incremental,
                        3,
                        different,
                        3,
                        1,
                        1,
                        1,
                    )
                    .await
            }
        });
        while scheduler
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
            != 4
        {
            tokio::task::yield_now().await;
        }
        release.notify_waiters();
        assert_eq!(blocked.await.unwrap(), Ok(99));
        assert_eq!(first.await.unwrap(), Ok(1));
        assert_eq!(second.await.unwrap(), Ok(2));
        assert_eq!(separate.await.unwrap(), Ok(3));

        let batches = batches.lock().await;
        assert!(
            batches
                .iter()
                .any(|(cohort, len)| cohort == &shared && *len == 2)
        );
        assert!(
            batches
                .iter()
                .any(|(cohort, len)| cohort == &different && *len == 1)
        );
    }

    #[tokio::test]
    async fn candidates_accumulating_behind_physical_admission_share_one_epoch() {
        let admission = Arc::new(Semaphore::new(0));
        let batches = Arc::new(AsyncMutex::new(Vec::new()));
        let scheduler = PublicationCohortScheduler::start_with_admission(
            bounds(8, 8),
            {
                let admission = admission.clone();
                move |_class| {
                    let admission = admission.clone();
                    async move { admission.acquire_owned().await.map_err(|_| ()) }
                }
            },
            {
                let batches = batches.clone();
                move |_class, _cohort: u8, payloads: Vec<u64>| {
                    let batches = batches.clone();
                    async move {
                        batches.lock().await.push(payloads.len());
                        payloads.into_iter().map(Ok::<_, ()>).collect()
                    }
                }
            },
        );
        let mut tasks = Vec::new();
        for index in 0..4_u64 {
            let scheduler = scheduler.clone();
            tasks.push(tokio::spawn(async move {
                scheduler
                    .submit(
                        PublicationCohortClass::Incremental,
                        index,
                        1,
                        index,
                        1,
                        1,
                        1,
                    )
                    .await
            }));
        }
        while scheduler
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
            != 4
        {
            tokio::task::yield_now().await;
        }
        assert!(batches.lock().await.is_empty());
        admission.add_permits(1);
        for (expected, task) in tasks.into_iter().enumerate() {
            assert_eq!(task.await.unwrap(), Ok(expected as u64));
        }
        assert_eq!(*batches.lock().await, vec![4]);
    }

    #[tokio::test]
    async fn respects_item_byte_and_physical_cohort_bounds() {
        let batches = Arc::new(AsyncMutex::new(Vec::new()));
        let observed = batches.clone();
        let scheduler = PublicationCohortScheduler::start(
            bounds(4, 10),
            move |_class, cohort: u8, payloads: Vec<usize>| {
                let observed = observed.clone();
                async move {
                    observed.lock().await.push((cohort, payloads.len()));
                    payloads.into_iter().map(Ok::<_, ()>).collect()
                }
            },
        );
        let mut tasks = Vec::new();
        for index in 0..10 {
            let scheduler = scheduler.clone();
            tasks.push(tokio::spawn(async move {
                scheduler
                    .submit(
                        PublicationCohortClass::Incremental,
                        index,
                        (index % 2) as u8,
                        index,
                        1,
                        3,
                        3,
                    )
                    .await
            }));
        }
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        {
            let batches = batches.lock().await;
            assert!(batches.iter().all(|(_, candidates)| *candidates <= 3));
            assert_eq!(batches.iter().map(|(_, count)| count).sum::<usize>(), 10);
            assert!(batches.iter().any(|(cohort, _)| *cohort == 0));
            assert!(batches.iter().any(|(cohort, _)| *cohort == 1));
        }

        // An aggregate logical candidate may exceed a physical batch bound
        // when each individual item is valid. It dispatches alone so the
        // concrete router can split its items without blocking later epochs.
        assert_eq!(
            scheduler
                .submit(PublicationCohortClass::Incremental, 20, 0, 20, 5, 12, 4,)
                .await,
            Ok(20)
        );
        assert_eq!(
            scheduler
                .submit(PublicationCohortClass::Incremental, 21, 0, 21, 1, 11, 11,)
                .await,
            Err(PublicationCohortError::CandidateTooLarge)
        );
        assert_eq!(
            scheduler
                .submit(PublicationCohortClass::Incremental, 22, 1, 22, 1, 3, 3,)
                .await,
            Ok(22)
        );
        let batches = batches.lock().await;
        assert_eq!(batches[batches.len() - 2..], [(0, 1), (1, 1)]);
    }

    #[tokio::test]
    async fn cancellation_releases_the_index_without_interrupting_dispatch() {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let dispatches = Arc::new(AtomicUsize::new(0));
        let scheduler = PublicationCohortScheduler::start(bounds(8, 8), {
            let entered = entered.clone();
            let release = release.clone();
            let dispatches = dispatches.clone();
            move |_class, _cohort: u8, payloads: Vec<u64>| {
                let entered = entered.clone();
                let release = release.clone();
                let dispatches = dispatches.clone();
                async move {
                    let ordinal = dispatches.fetch_add(1, Ordering::SeqCst);
                    if ordinal == 0 {
                        entered.notify_waiters();
                        release.notified().await;
                    }
                    payloads.into_iter().map(Ok::<_, ()>).collect()
                }
            }
        });
        let first = tokio::spawn({
            let scheduler = scheduler.clone();
            async move {
                scheduler
                    .submit(PublicationCohortClass::Incremental, 1, 0, 9, 1, 1, 1)
                    .await
            }
        });
        entered.notified().await;
        first.abort();
        assert_eq!(
            scheduler
                .submit(PublicationCohortClass::Maintenance, 1, 0, 10, 1, 1, 1)
                .await,
            Err(PublicationCohortError::DuplicateIndex)
        );
        release.notify_waiters();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match scheduler
                    .submit(PublicationCohortClass::Incremental, 1, 0, 11, 1, 1, 1)
                    .await
                {
                    Err(PublicationCohortError::DuplicateIndex) => tokio::task::yield_now().await,
                    outcome => break outcome,
                }
            }
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(dispatches.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cancelled_queued_candidate_is_discarded_and_releases_its_index() {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let dispatches = Arc::new(AtomicUsize::new(0));
        let scheduler = PublicationCohortScheduler::start(bounds(8, 8), {
            let entered = entered.clone();
            let release = release.clone();
            let dispatches = dispatches.clone();
            move |_class, _cohort: u8, payloads: Vec<u64>| {
                let entered = entered.clone();
                let release = release.clone();
                let dispatches = dispatches.clone();
                async move {
                    if dispatches.fetch_add(1, Ordering::SeqCst) == 0 {
                        entered.notify_waiters();
                        release.notified().await;
                    }
                    payloads.into_iter().map(Ok::<_, ()>).collect()
                }
            }
        });
        let blocker = tokio::spawn({
            let scheduler = scheduler.clone();
            async move {
                scheduler
                    .submit(PublicationCohortClass::Incremental, 1, 0, 1, 1, 1, 1)
                    .await
            }
        });
        entered.notified().await;
        let cancelled = tokio::spawn({
            let scheduler = scheduler.clone();
            async move {
                scheduler
                    .submit(PublicationCohortClass::Incremental, 2, 0, 2, 1, 1, 1)
                    .await
            }
        });
        while scheduler
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
            != 2
        {
            tokio::task::yield_now().await;
        }
        cancelled.abort();
        release.notify_waiters();
        assert_eq!(blocker.await.unwrap(), Ok(1));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !scheduler
                    .active
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .contains(&2)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            scheduler
                .submit(PublicationCohortClass::Incremental, 2, 0, 3, 1, 1, 1)
                .await,
            Ok(3)
        );
        assert_eq!(dispatches.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cancellation_during_collection_prevents_dispatch_and_drops_owned_state() {
        struct DropProbe(Arc<AtomicUsize>);

        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let admitted = Arc::new(Notify::new());
        let dispatches = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let scheduler = PublicationCohortScheduler::start_with_admission(
            PublicationCohortBounds::new(128, 8, 8, Duration::from_millis(5), 1, 1),
            {
                let admitted = admitted.clone();
                move |_class| {
                    admitted.notify_one();
                    async { Ok::<(), ()>(()) }
                }
            },
            {
                let dispatches = dispatches.clone();
                move |_class, _cohort: u8, payloads: Vec<DropProbe>| {
                    dispatches.fetch_add(1, Ordering::SeqCst);
                    async move { payloads.into_iter().map(|_| Ok::<_, ()>(())).collect() }
                }
            },
        );

        let admitted_wait = admitted.notified();
        let cancelled = tokio::spawn({
            let scheduler = scheduler.clone();
            let drops = drops.clone();
            async move {
                scheduler
                    .submit(
                        PublicationCohortClass::Incremental,
                        1_u64,
                        0,
                        DropProbe(drops),
                        1,
                        1,
                        1,
                    )
                    .await
            }
        });
        admitted_wait.await;
        // Admission is complete; let the worker enter its bounded collection
        // wait before cancelling the logical submitter.
        tokio::task::yield_now().await;
        cancelled.abort();
        let _ = cancelled.await;

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let active = scheduler
                    .active
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .contains(&1);
                if !active {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        assert_eq!(dispatches.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn maintenance_progresses_while_incremental_dispatch_is_blocked() {
        let incremental_entered = Arc::new(Notify::new());
        let release_incremental = Arc::new(Notify::new());
        let scheduler = PublicationCohortScheduler::start(bounds(8, 8), {
            let incremental_entered = incremental_entered.clone();
            let release_incremental = release_incremental.clone();
            move |class, _cohort: u8, payloads: Vec<u64>| {
                let incremental_entered = incremental_entered.clone();
                let release_incremental = release_incremental.clone();
                async move {
                    if class == PublicationCohortClass::Incremental {
                        incremental_entered.notify_waiters();
                        release_incremental.notified().await;
                    }
                    payloads.into_iter().map(Ok::<_, ()>).collect()
                }
            }
        });
        let incremental = tokio::spawn({
            let scheduler = scheduler.clone();
            async move {
                scheduler
                    .submit(PublicationCohortClass::Incremental, 1, 0, 1, 1, 1, 1)
                    .await
            }
        });
        incremental_entered.notified().await;
        let maintenance = tokio::time::timeout(
            Duration::from_millis(100),
            scheduler.submit(PublicationCohortClass::Maintenance, 2, 0, 2, 1, 1, 1),
        )
        .await
        .expect("maintenance queue must not wait behind incremental dispatch");
        assert_eq!(maintenance, Ok(2));
        release_incremental.notify_waiters();
        assert_eq!(incremental.await.unwrap(), Ok(1));
    }

    #[tokio::test]
    async fn physical_batch_concurrency_is_bounded_by_class() {
        let entered = Arc::new(tokio::sync::Barrier::new(3));
        let release = Arc::new(tokio::sync::Barrier::new(3));
        let bounds = PublicationCohortBounds::new(8, 8, 8, Duration::from_millis(5), 2, 1);
        let scheduler = PublicationCohortScheduler::start(bounds, {
            let entered = entered.clone();
            let release = release.clone();
            move |_class, _cohort: u8, payloads: Vec<u64>| {
                let entered = entered.clone();
                let release = release.clone();
                async move {
                    entered.wait().await;
                    release.wait().await;
                    payloads.into_iter().map(Ok::<_, ()>).collect()
                }
            }
        });
        let first = tokio::spawn({
            let scheduler = scheduler.clone();
            async move {
                scheduler
                    .submit(PublicationCohortClass::Incremental, 1, 1, 1, 1, 1, 1)
                    .await
            }
        });
        let second = tokio::spawn({
            let scheduler = scheduler.clone();
            async move {
                scheduler
                    .submit(PublicationCohortClass::Incremental, 2, 2, 2, 1, 1, 1)
                    .await
            }
        });
        tokio::time::timeout(Duration::from_millis(100), entered.wait())
            .await
            .expect("two incremental physical batches may run concurrently");
        release.wait().await;
        assert_eq!(first.await.unwrap(), Ok(1));
        assert_eq!(second.await.unwrap(), Ok(2));
    }

    #[tokio::test]
    async fn rejects_a_second_active_candidate_for_the_same_index() {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let scheduler = PublicationCohortScheduler::start(bounds(8, 8), {
            let entered = entered.clone();
            let release = release.clone();
            move |_class, _cohort: u8, payloads: Vec<u64>| {
                let entered = entered.clone();
                let release = release.clone();
                async move {
                    entered.notify_waiters();
                    release.notified().await;
                    payloads.into_iter().map(Ok::<_, ()>).collect()
                }
            }
        });
        let first = tokio::spawn({
            let scheduler = scheduler.clone();
            async move {
                scheduler
                    .submit(PublicationCohortClass::Incremental, 1, 0, 1, 1, 1, 1)
                    .await
            }
        });
        entered.notified().await;
        assert_eq!(
            scheduler
                .submit(PublicationCohortClass::Incremental, 1, 0, 2, 1, 1, 1)
                .await,
            Err(PublicationCohortError::DuplicateIndex)
        );
        release.notify_waiters();
        assert_eq!(first.await.unwrap(), Ok(1));
    }

    #[tokio::test]
    async fn completion_releases_identity_before_the_caller_can_retry() {
        let scheduler = PublicationCohortScheduler::start(
            bounds(8, 8),
            |_class, _cohort: u8, payloads: Vec<u64>| async move {
                payloads.into_iter().map(Ok::<_, ()>).collect()
            },
        );
        assert_eq!(
            scheduler
                .submit(PublicationCohortClass::Incremental, 1, 0, 1, 1, 1, 1)
                .await,
            Ok(1)
        );
        assert_eq!(
            scheduler
                .submit(PublicationCohortClass::Incremental, 1, 0, 2, 1, 1, 1)
                .await,
            Ok(2)
        );
    }

    #[tokio::test]
    async fn source_epoch_change_while_queued_prevents_current_dispatch() {
        let admission_waiting = Arc::new(Notify::new());
        let release_admission = Arc::new(Notify::new());
        let source_epoch = Arc::new(AtomicUsize::new(1));
        let routed = Arc::new(AtomicUsize::new(0));
        let scheduler = PublicationCohortScheduler::start_with_admission(
            bounds(8, 8),
            {
                let admission_waiting = admission_waiting.clone();
                let release_admission = release_admission.clone();
                move |_class| {
                    let admission_waiting = admission_waiting.clone();
                    let release_admission = release_admission.clone();
                    async move {
                        admission_waiting.notify_waiters();
                        release_admission.notified().await;
                        Ok::<(), Status>(())
                    }
                }
            },
            {
                let source_epoch = source_epoch.clone();
                let routed = routed.clone();
                move |_class, _cohort: u8, candidates: Vec<usize>| {
                    let source_epoch = source_epoch.clone();
                    let routed = routed.clone();
                    async move {
                        candidates
                            .into_iter()
                            .map(|expected_epoch| {
                                if source_epoch.load(Ordering::SeqCst) != expected_epoch {
                                    Err(Status::failed_precondition(
                                        "source epoch changed before current dispatch",
                                    ))
                                } else {
                                    routed.fetch_add(1, Ordering::SeqCst);
                                    Ok(expected_epoch)
                                }
                            })
                            .collect()
                    }
                }
            },
        );

        let waiting = admission_waiting.notified();
        let publication = tokio::spawn({
            let scheduler = scheduler.clone();
            async move {
                scheduler
                    .submit(PublicationCohortClass::Incremental, 1, 0, 1, 1, 1, 1)
                    .await
            }
        });
        waiting.await;
        source_epoch.store(2, Ordering::SeqCst);
        release_admission.notify_waiters();

        let error = publication.await.unwrap().unwrap_err();
        assert!(matches!(error, PublicationCohortError::Publication(_)));
        assert_eq!(routed.load(Ordering::SeqCst), 0);
    }
}
