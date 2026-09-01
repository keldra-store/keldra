//! Bounded baseline reads for one input-ordered object mutation batch.
//!
//! Snapshot-backed caches may be populated before the store commit fence, but
//! every exact raw observation must then be revalidated under that fence. The
//! cache is never authoritative: mutations still enter the existing pending
//! maps in input order and share the existing final `WriteBatch`.

use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};

use rocksdb::{DB, SnapshotWithThreadMode};

use super::object_alias_registry::decode_registry;
use super::receipt_codec::decode_stored_receipt;
use super::*;
use crate::ObjectAliasRegistry;

const PREFETCH_KEYS_PER_MULTI_GET: usize = 256;

type Cached<T> = Result<Option<T>, MutationError>;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ObservedMutationCell {
    cf_name: &'static str,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

/// Exact raw values selected by one coherent mutation-prefetch snapshot.
#[derive(Clone, Debug, Default)]
pub(super) struct MutationReadToken {
    cells: Vec<ObservedMutationCell>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum MutationConflictResource {
    Object(ObjectPath),
    Receipt(Vec<u8>),
    Blob(Vec<u8>),
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct MutationPreparationMetrics {
    pub(super) configured_lanes: usize,
    pub(super) effective_lanes: usize,
    pub(super) lane_jobs: usize,
    pub(super) components: usize,
    pub(super) largest_component_operations: usize,
    pub(super) peak_active_lanes: usize,
    pub(super) summed_lane_queue_wait: std::time::Duration,
    pub(super) summed_lane_service: std::time::Duration,
}

pub(super) struct MutationReadSpeculation {
    pub(super) cache: MutationReadCache,
    tokens: Vec<MutationReadToken>,
    pub(super) metrics: MutationPreparationMetrics,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct MutationReadRevalidation {
    pub(super) unchanged: bool,
    pub(super) checked_keys: usize,
}

enum MutationReadSource<'a> {
    Current,
    Snapshot(SnapshotWithThreadMode<'a, DB>),
}

/// A current or snapshot-backed RocksDB view for one complete bulk prefetch.
struct MutationReadView<'a> {
    store: &'a Store,
    source: MutationReadSource<'a>,
    observations: RefCell<BTreeMap<(&'static str, Vec<u8>), Option<Vec<u8>>>>,
}

impl<'a> MutationReadView<'a> {
    fn current(store: &'a Store) -> Self {
        Self {
            store,
            source: MutationReadSource::Current,
            observations: RefCell::new(BTreeMap::new()),
        }
    }

    fn snapshot(store: &'a Store) -> Self {
        Self {
            store,
            source: MutationReadSource::Snapshot(store.db.snapshot()),
            observations: RefCell::new(BTreeMap::new()),
        }
    }

    fn take_token(&self) -> MutationReadToken {
        MutationReadToken {
            cells: std::mem::take(&mut *self.observations.borrow_mut())
                .into_iter()
                .map(|((cf_name, key), value)| ObservedMutationCell {
                    cf_name,
                    key,
                    value,
                })
                .collect(),
        }
    }

    fn multi_get_raw(
        &self,
        cf_name: &'static str,
        keys: &BTreeSet<Vec<u8>>,
    ) -> Result<BTreeMap<Vec<u8>, Cached<Vec<u8>>>, MutationError> {
        let cf = self.store.cf(cf_name)?;
        let mut values = BTreeMap::new();
        let ordered = keys.iter().collect::<Vec<_>>();
        for keys in ordered.chunks(PREFETCH_KEYS_PER_MULTI_GET) {
            let fetched = match &self.source {
                MutationReadSource::Current => self
                    .store
                    .db
                    .multi_get_cf(keys.iter().map(|key| (cf, key.as_slice()))),
                MutationReadSource::Snapshot(snapshot) => {
                    snapshot.multi_get_cf(keys.iter().map(|key| (cf, key.as_slice())))
                }
            };
            if fetched.len() != keys.len() {
                return Err(MutationError::Storage(format!(
                    "{cf_name} bulk prefetch returned the wrong result count"
                )));
            }
            for (key, value) in keys.iter().zip(fetched) {
                let value = value
                    .map(|encoded| encoded.map(|bytes| bytes.to_vec()))
                    .map_err(storage_error)?;
                if matches!(&self.source, MutationReadSource::Snapshot(_)) {
                    self.observations
                        .borrow_mut()
                        .insert((cf_name, (*key).clone()), value.clone());
                }
                values.insert((*key).clone(), Ok(value));
            }
        }
        Ok(values)
    }
}

#[derive(Default)]
pub(super) struct MutationReadCache {
    heads: BTreeMap<Vec<u8>, Cached<Head>>,
    stored_versions: BTreeMap<Vec<u8>, Cached<StoredVersion>>,
    receipts: BTreeMap<Vec<u8>, Cached<StoredReceipt>>,
    blob_references: BTreeMap<Vec<u8>, Cached<BlobReferenceState>>,
    inline_payloads: BTreeMap<Vec<u8>, Cached<Vec<u8>>>,
    alias_registries: BTreeMap<Vec<u8>, Cached<ObjectAliasRegistry>>,
    policies: BTreeMap<Vec<u8>, Result<BucketPolicy, MutationError>>,
    versioning: BTreeMap<Vec<u8>, Result<ObjectVersioning, MutationError>>,
}

#[derive(Default)]
struct PrefetchMetrics {
    head_keys: u64,
    version_keys: u64,
    receipt_keys: u64,
    blob_reference_keys: u64,
    inline_payload_keys: u64,
    alias_registry_keys: u64,
    policy_keys: u64,
    versioning_keys: u64,
    head_seconds: f64,
    version_seconds: f64,
    receipt_seconds: f64,
    blob_reference_seconds: f64,
    inline_payload_seconds: f64,
    alias_registry_seconds: f64,
    policy_seconds: f64,
    versioning_seconds: f64,
}

impl MutationReadCache {
    pub(super) fn load(
        store: &Store,
        operations: &[&PreparedOperation],
    ) -> Result<Self, MutationError> {
        Self::load_from(&MutationReadView::current(store), operations, true)
    }

    pub(super) fn load_governed(
        store: &Store,
        operations: &[&PreparedOperation],
        governed_buckets: &BTreeSet<Vec<u8>>,
    ) -> Result<Self, MutationError> {
        validate_governed_coverage(operations, governed_buckets)?;
        Self::load_from(&MutationReadView::current(store), operations, false)
    }

    fn load_governed_snapshot(
        store: &Store,
        operations: &[&PreparedOperation],
        governed_buckets: &BTreeSet<Vec<u8>>,
    ) -> Result<(Self, MutationReadToken), MutationError> {
        validate_governed_coverage(operations, governed_buckets)?;
        let view = MutationReadView::snapshot(store);
        let cache = Self::load_from(&view, operations, false)?;
        Ok((cache, view.take_token()))
    }

    fn merge(&mut self, mut other: Self) {
        // Read-only namespaces such as validated governance may overlap across
        // component snapshots. Deterministic last-component selection is safe
        // only because every component's raw token is retained: differing
        // observations cannot both revalidate under the commit guard, so the
        // merged cache is never evaluated in that case.
        self.heads.append(&mut other.heads);
        self.stored_versions.append(&mut other.stored_versions);
        self.receipts.append(&mut other.receipts);
        self.blob_references.append(&mut other.blob_references);
        self.inline_payloads.append(&mut other.inline_payloads);
        self.alias_registries.append(&mut other.alias_registries);
        self.policies.append(&mut other.policies);
        self.versioning.append(&mut other.versioning);
    }

    fn load_from(
        view: &MutationReadView<'_>,
        operations: &[&PreparedOperation],
        load_bucket_settings: bool,
    ) -> Result<Self, MutationError> {
        let mut metrics = PrefetchMetrics::default();
        let head_keys = operations
            .iter()
            .map(|operation| operation.encoded_head_key())
            .collect::<BTreeSet<_>>();
        let receipt_keys = operations
            .iter()
            .filter_map(|operation| {
                operation
                    .command_id()
                    .map(|command_id| receipt_key(operation.identity(), command_id))
            })
            .collect::<BTreeSet<_>>();
        let bucket_keys = if load_bucket_settings {
            operations
                .iter()
                .map(|operation| operation.identity().encode().to_vec())
                .collect::<BTreeSet<_>>()
        } else {
            BTreeSet::new()
        };

        let (heads, elapsed) = multi_get_json::<Head>(view, CF_HEADS, &head_keys)?;
        metrics.head_keys = head_keys.len() as u64;
        metrics.head_seconds = elapsed;

        let started = std::time::Instant::now();
        let alias_registries = view
            .multi_get_raw(CF_OBJECT_ALIAS_REGISTRIES, &head_keys)?
            .into_iter()
            .map(|(key, cached)| {
                let decoded = cached
                    .and_then(|value| value.map(|encoded| decode_registry(&encoded)).transpose());
                (key, decoded)
            })
            .collect();
        metrics.alias_registry_keys = head_keys.len() as u64;
        metrics.alias_registry_seconds = started.elapsed().as_secs_f64();

        let mut version_key_by_head = BTreeMap::new();
        let mut version_keys = BTreeSet::new();
        for (head_key, cached) in &heads {
            if let Ok(Some(head)) = cached {
                let key = exact_version_key(head_key, head.version);
                version_keys.insert(key.clone());
                version_key_by_head.insert(head_key.clone(), key);
            }
        }
        let started = std::time::Instant::now();
        let versions_by_key = view
            .multi_get_raw(CF_VERSIONS, &version_keys)?
            .into_iter()
            .map(|(key, cached)| {
                let decoded = cached.and_then(|value| {
                    value
                        .map(|encoded| StoredVersion::decode(&encoded))
                        .transpose()
                });
                (key, decoded)
            })
            .collect::<BTreeMap<_, _>>();
        let elapsed = started.elapsed();
        metrics.version_keys = version_keys.len() as u64;
        metrics.version_seconds = elapsed.as_secs_f64();
        let stored_versions: BTreeMap<Vec<u8>, Cached<StoredVersion>> = version_key_by_head
            .into_iter()
            .map(|(head_key, version_key)| {
                let cached = versions_by_key.get(&version_key).cloned().ok_or_else(|| {
                    MutationError::Storage("bulk version prefetch omitted a requested key".into())
                });
                (head_key, cached.and_then(|value| value))
            })
            .collect();

        let started = std::time::Instant::now();
        let receipts = view
            .multi_get_raw(CF_RECEIPTS, &receipt_keys)?
            .into_iter()
            .map(|(key, cached)| {
                let decoded = cached.and_then(|value| {
                    value
                        .map(|encoded| decode_stored_receipt(&encoded))
                        .transpose()
                });
                (key, decoded)
            })
            .collect();
        let elapsed = started.elapsed().as_secs_f64();
        metrics.receipt_keys = receipt_keys.len() as u64;
        metrics.receipt_seconds = elapsed;

        let started = std::time::Instant::now();
        let policies = view
            .multi_get_raw(CF_POLICIES, &bucket_keys)?
            .into_iter()
            .map(|(key, cached)| {
                let decoded = decode_bucket_policy(&key, cached);
                (key, decoded)
            })
            .collect();
        metrics.policy_keys = bucket_keys.len() as u64;
        metrics.policy_seconds = started.elapsed().as_secs_f64();
        let started = std::time::Instant::now();
        let versioning = view
            .multi_get_raw(CF_BUCKET_OPTIONS, &bucket_keys)?
            .into_iter()
            .map(|(key, cached)| {
                let decoded = decode_bucket_versioning(&key, cached);
                (key, decoded)
            })
            .collect();
        metrics.versioning_keys = bucket_keys.len() as u64;
        metrics.versioning_seconds = started.elapsed().as_secs_f64();

        let mut blob_reference_keys = BTreeSet::new();
        let mut inline_payload_keys = BTreeSet::new();
        for operation in operations {
            if let Some(reference) = operation.payload_reference() {
                let key = blob_reference_key(reference);
                blob_reference_keys.insert(key.clone());
                if is_inline_payload_artifact(reference) {
                    inline_payload_keys.insert(complete_artifact_key(reference));
                }
            }
        }
        for cached in stored_versions.values() {
            if let Ok(Some(stored)) = cached
                && let Some(reference) = stored.version.blob.as_ref()
            {
                blob_reference_keys.insert(blob_reference_key(reference));
            }
        }

        let started = std::time::Instant::now();
        let blob_references = view
            .multi_get_raw(CF_BLOB_REFERENCES, &blob_reference_keys)?
            .into_iter()
            .map(|(key, cached)| {
                let decoded = cached.and_then(|value| {
                    value
                        .map(|encoded| decode_blob_reference_state(&encoded))
                        .transpose()
                });
                (key, decoded)
            })
            .collect();
        metrics.blob_reference_keys = blob_reference_keys.len() as u64;
        metrics.blob_reference_seconds = started.elapsed().as_secs_f64();

        let started = std::time::Instant::now();
        let inline_payloads = view.multi_get_raw(CF_PAYLOAD_ARTIFACTS, &inline_payload_keys)?;
        metrics.inline_payload_keys = inline_payload_keys.len() as u64;
        metrics.inline_payload_seconds = started.elapsed().as_secs_f64();
        metrics.emit();

        Ok(Self {
            heads,
            stored_versions,
            receipts,
            blob_references,
            inline_payloads,
            alias_registries,
            policies,
            versioning,
        })
    }

    pub(super) fn head(&self, key: &[u8]) -> Option<Cached<Head>> {
        self.heads.get(key).cloned()
    }

    pub(super) fn stored_version(&self, head_key: &[u8]) -> Option<Cached<StoredVersion>> {
        self.stored_versions.get(head_key).cloned()
    }

    pub(super) fn receipt(&self, key: &[u8]) -> Option<Cached<StoredReceipt>> {
        self.receipts.get(key).cloned()
    }

    pub(super) fn blob_reference(&self, reference: &BlobRef) -> Option<Cached<BlobReferenceState>> {
        self.blob_references
            .get(&blob_reference_key(reference))
            .cloned()
    }

    pub(super) fn blob_reference_by_key(&self, key: &[u8]) -> Option<Cached<BlobReferenceState>> {
        self.blob_references.get(key).cloned()
    }

    pub(super) fn inline_payload(&self, reference: &BlobRef) -> Option<Cached<Vec<u8>>> {
        self.inline_payloads
            .get(&complete_artifact_key(reference))
            .cloned()
    }

    pub(super) fn alias_registry(
        &self,
        head_key: &[u8],
        canonical_path: &str,
    ) -> Option<Cached<ObjectAliasRegistry>> {
        self.alias_registries.get(head_key).cloned().map(|cached| {
            cached.and_then(|registry| {
                if let Some(registry) = registry.as_ref() {
                    registry.validate(canonical_path)?;
                }
                Ok(registry)
            })
        })
    }

    pub(super) fn seed_bucket_settings(
        &self,
        policies: &mut BTreeMap<Vec<u8>, Result<BucketPolicy, MutationError>>,
        versioning: &mut BTreeMap<Vec<u8>, Result<ObjectVersioning, MutationError>>,
    ) {
        for (key, value) in &self.policies {
            policies.entry(key.clone()).or_insert_with(|| value.clone());
        }
        for (key, value) in &self.versioning {
            versioning
                .entry(key.clone())
                .or_insert_with(|| value.clone());
        }
    }
}

impl MutationReadSpeculation {
    /// Snapshot-prefetch independent conflict components on at most the
    /// configured number of blocking lanes. Components, rather than individual
    /// operations, own snapshots so every dependent read starts from one
    /// coherent RocksDB view. The lane-one path deliberately retains the
    /// original single-snapshot baseline.
    pub(super) async fn load(
        store: &Store,
        operations: &[Arc<PreparedOperation>],
        configured_lanes: usize,
        governed_buckets: &BTreeSet<Vec<u8>>,
    ) -> Result<Self, MutationError> {
        debug_assert!(configured_lanes > 0);
        validate_governed_coverage(
            &operations.iter().map(Arc::as_ref).collect::<Vec<_>>(),
            governed_buckets,
        )?;
        if operations.is_empty() {
            return Ok(Self {
                cache: MutationReadCache::default(),
                tokens: Vec::new(),
                metrics: MutationPreparationMetrics {
                    configured_lanes,
                    ..MutationPreparationMetrics::default()
                },
            });
        }

        let refs = operations.iter().map(Arc::as_ref).collect::<Vec<_>>();
        if configured_lanes == 1 {
            let service_started = std::time::Instant::now();
            let (cache, token) =
                MutationReadCache::load_governed_snapshot(store, &refs, governed_buckets)?;
            return Ok(Self {
                cache,
                tokens: vec![token],
                metrics: MutationPreparationMetrics {
                    configured_lanes,
                    effective_lanes: 1,
                    lane_jobs: 1,
                    components: 1,
                    largest_component_operations: operations.len(),
                    peak_active_lanes: 1,
                    summed_lane_service: service_started.elapsed(),
                    ..MutationPreparationMetrics::default()
                },
            });
        }
        let components = mutation_conflict_components(&refs);
        let effective_lanes = configured_lanes.min(components.len());
        let largest_component_operations = components.iter().map(Vec::len).max().unwrap_or(0);
        let assignments = assign_components_to_lanes(&components, effective_lanes);
        let owned = Arc::new(operations.to_vec());
        let store = store.clone();
        let governed_buckets = Arc::new(governed_buckets.clone());
        let active_lanes = Arc::new(AtomicUsize::new(0));
        let peak_active_lanes = Arc::new(AtomicUsize::new(0));
        let worker_active_lanes = Arc::clone(&active_lanes);
        let worker_peak_active_lanes = Arc::clone(&peak_active_lanes);
        let loaded = run_preparation_lanes(
            assignments,
            Arc::new(move |lane_components: Vec<Vec<usize>>| {
                let active = worker_active_lanes.fetch_add(1, Ordering::SeqCst) + 1;
                worker_peak_active_lanes.fetch_max(active, Ordering::SeqCst);
                let view = MutationReadView::snapshot(&store);
                let result = lane_components
                    .into_iter()
                    .map(|component| {
                        let refs = component
                            .iter()
                            .map(|index| owned[*index].as_ref())
                            .collect::<Vec<_>>();
                        validate_governed_coverage(&refs, &governed_buckets)?;
                        let cache = MutationReadCache::load_from(&view, &refs, false)?;
                        Ok::<_, MutationError>((cache, view.take_token()))
                    })
                    .collect::<Result<Vec<_>, _>>();
                worker_active_lanes.fetch_sub(1, Ordering::SeqCst);
                result
            }),
        )
        .await?;

        let summed_lane_queue_wait = loaded
            .iter()
            .fold(std::time::Duration::ZERO, |total, lane| {
                total.saturating_add(lane.queue_wait)
            });
        let summed_lane_service = loaded
            .iter()
            .fold(std::time::Duration::ZERO, |total, lane| {
                total.saturating_add(lane.service)
            });
        let mut cache = MutationReadCache::default();
        let mut tokens = Vec::with_capacity(components.len());
        for lane in loaded {
            for (component_cache, token) in lane.value {
                cache.merge(component_cache);
                tokens.push(token);
            }
        }
        Ok(Self {
            cache,
            tokens,
            metrics: MutationPreparationMetrics {
                configured_lanes,
                effective_lanes,
                lane_jobs: effective_lanes,
                components: components.len(),
                largest_component_operations,
                peak_active_lanes: peak_active_lanes.load(Ordering::SeqCst),
                summed_lane_queue_wait,
                summed_lane_service,
            },
        })
    }

    pub(super) fn revalidate(
        &self,
        store: &Store,
    ) -> Result<MutationReadRevalidation, MutationError> {
        let mut unchanged = true;
        let mut checked_keys = 0;
        let mut first_error = None;
        for token in &self.tokens {
            match token.revalidate(store) {
                Ok(result) => {
                    unchanged &= result.unchanged;
                    checked_keys += result.checked_keys;
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(MutationReadRevalidation {
                unchanged,
                checked_keys,
            })
        }
    }

    pub(super) fn token_len(&self) -> usize {
        self.tokens.iter().map(MutationReadToken::len).sum()
    }
}

fn validate_governed_coverage(
    operations: &[&PreparedOperation],
    governed_buckets: &BTreeSet<Vec<u8>>,
) -> Result<(), MutationError> {
    if operations
        .iter()
        .any(|operation| !governed_buckets.contains(&operation.identity().encode().to_vec()))
    {
        Err(MutationError::InvalidPolicy(
            "single-node speculative preparation lacks validated governance".into(),
        ))
    } else {
        Ok(())
    }
}

struct PreparationLaneResult<T> {
    value: T,
    queue_wait: std::time::Duration,
    service: std::time::Duration,
}

async fn run_preparation_lanes<T, F>(
    assignments: Vec<Vec<Vec<usize>>>,
    work: Arc<F>,
) -> Result<Vec<PreparationLaneResult<T>>, MutationError>
where
    T: Send + 'static,
    F: Fn(Vec<Vec<usize>>) -> Result<T, MutationError> + Send + Sync + 'static,
{
    let mut jobs = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let work = Arc::clone(&work);
        let queued_at = std::time::Instant::now();
        jobs.push(tokio::task::spawn_blocking(move || {
            let started = std::time::Instant::now();
            let queue_wait = started.duration_since(queued_at);
            let value = work(assignment)?;
            Ok(PreparationLaneResult {
                value,
                queue_wait,
                service: started.elapsed(),
            })
        }));
    }
    let mut results = Vec::with_capacity(jobs.len());
    let mut first_error = None;
    for job in jobs {
        match job.await {
            Ok(Ok(result)) => results.push(result),
            Ok(Err(error)) => {
                first_error.get_or_insert(error);
            }
            Err(error) => {
                first_error.get_or_insert_with(|| {
                    MutationError::Storage(format!("mutation preparation lane failed: {error}"))
                });
            }
        }
    }
    if let Some(error) = first_error {
        Err(error)
    } else {
        Ok(results)
    }
}

fn mutation_conflict_components(operations: &[&PreparedOperation]) -> Vec<Vec<usize>> {
    // This graph is the exact conservative boundary for Stage 2's read-only
    // prefetch. Snapshot-derived predecessor blobs, aliases and definition
    // transitions must extend/merge these components before a later stage is
    // allowed to plan mutations concurrently.
    let mut parents = (0..operations.len()).collect::<Vec<_>>();
    let mut owners = BTreeMap::<MutationConflictResource, usize>::new();
    for (index, operation) in operations.iter().enumerate() {
        for resource in mutation_conflict_resources(operation) {
            if let Some(previous) = owners.insert(resource, index) {
                union_components(&mut parents, previous, index);
            }
        }
    }
    let mut components = BTreeMap::<usize, Vec<usize>>::new();
    for index in 0..operations.len() {
        let root = find_component(&mut parents, index);
        components.entry(root).or_default().push(index);
    }
    let mut components = components.into_values().collect::<Vec<_>>();
    components.sort_by_key(|component| component[0]);
    components
}

fn mutation_conflict_resources(operation: &PreparedOperation) -> Vec<MutationConflictResource> {
    // A full ObjectPath covers the head/current-version/alias registry and any
    // definition locator staged for that target; Clone contributes its exact
    // source path too. Receipt and Blob use their real durable key encodings,
    // with the latter also covering the derived inline-artifact identity.
    // Validated governance is read-only under policy_gate and intentionally is
    // not an edge: joining a whole bucket would erase useful parallelism.
    let mut resources = operation
        .lock_paths()
        .into_iter()
        .map(MutationConflictResource::Object)
        .collect::<Vec<_>>();
    if let Some(command_id) = operation.command_id() {
        resources.push(MutationConflictResource::Receipt(receipt_key(
            operation.identity(),
            command_id,
        )));
    }
    if let Some(reference) = operation.payload_reference() {
        resources.push(MutationConflictResource::Blob(blob_reference_key(
            reference,
        )));
    }
    resources
}

fn find_component(parents: &mut [usize], index: usize) -> usize {
    if parents[index] != index {
        parents[index] = find_component(parents, parents[index]);
    }
    parents[index]
}

fn union_components(parents: &mut [usize], left: usize, right: usize) {
    let left = find_component(parents, left);
    let right = find_component(parents, right);
    let root = left.min(right);
    parents[left] = root;
    parents[right] = root;
}

fn assign_components_to_lanes(
    components: &[Vec<usize>],
    lane_count: usize,
) -> Vec<Vec<Vec<usize>>> {
    let mut lanes = vec![Vec::new(); lane_count];
    let mut loads = vec![0_usize; lane_count];
    for component in components {
        let lane = loads
            .iter()
            .enumerate()
            .min_by_key(|(lane, load)| (**load, *lane))
            .map(|(lane, _)| lane)
            .expect("non-empty lane set");
        loads[lane] += component.len();
        lanes[lane].push(component.clone());
    }
    lanes
}

impl MutationReadToken {
    pub(super) fn len(&self) -> usize {
        self.cells.len()
    }

    /// Re-read every observed cell under the caller's commit guard. A stale
    /// early cell does not short-circuit the remaining exact comparisons.
    pub(super) fn revalidate(
        &self,
        store: &Store,
    ) -> Result<MutationReadRevalidation, MutationError> {
        let mut unchanged = true;
        let mut checked_keys = 0;
        let mut start = 0;
        while start < self.cells.len() {
            let cf_name = self.cells[start].cf_name;
            let mut end = start + 1;
            while end < self.cells.len()
                && self.cells[end].cf_name == cf_name
                && end - start < PREFETCH_KEYS_PER_MULTI_GET
            {
                end += 1;
            }
            let cells = &self.cells[start..end];
            let cf = store.cf(cf_name)?;
            let fetched = store
                .db
                .multi_get_cf(cells.iter().map(|cell| (cf, cell.key.as_slice())));
            if fetched.len() != cells.len() {
                return Err(MutationError::Storage(format!(
                    "{cf_name} mutation token revalidation returned the wrong result count"
                )));
            }
            for (cell, value) in cells.iter().zip(fetched) {
                let value = value
                    .map(|encoded| encoded.map(|bytes| bytes.to_vec()))
                    .map_err(storage_error)?;
                checked_keys += 1;
                unchanged &= value == cell.value;
            }
            start = end;
        }
        Ok(MutationReadRevalidation {
            unchanged,
            checked_keys,
        })
    }
}

impl PrefetchMetrics {
    fn emit(self) {
        tracing::info!(
            monotonic_counter.keldra_store_bulk_prefetch_head_keys_total = self.head_keys,
            monotonic_counter.keldra_store_bulk_prefetch_version_keys_total = self.version_keys,
            monotonic_counter.keldra_store_bulk_prefetch_receipt_keys_total = self.receipt_keys,
            monotonic_counter.keldra_store_bulk_prefetch_blob_reference_keys_total =
                self.blob_reference_keys,
            monotonic_counter.keldra_store_bulk_prefetch_inline_payload_keys_total =
                self.inline_payload_keys,
            monotonic_counter.keldra_store_bulk_prefetch_alias_registry_keys_total =
                self.alias_registry_keys,
            monotonic_counter.keldra_store_bulk_prefetch_policy_keys_total = self.policy_keys,
            monotonic_counter.keldra_store_bulk_prefetch_versioning_keys_total =
                self.versioning_keys,
            histogram.keldra_store_bulk_prefetch_heads_duration_seconds = self.head_seconds,
            histogram.keldra_store_bulk_prefetch_versions_duration_seconds = self.version_seconds,
            histogram.keldra_store_bulk_prefetch_receipts_duration_seconds = self.receipt_seconds,
            histogram.keldra_store_bulk_prefetch_blob_references_duration_seconds =
                self.blob_reference_seconds,
            histogram.keldra_store_bulk_prefetch_inline_payloads_duration_seconds =
                self.inline_payload_seconds,
            histogram.keldra_store_bulk_prefetch_alias_registries_duration_seconds =
                self.alias_registry_seconds,
            histogram.keldra_store_bulk_prefetch_policies_duration_seconds = self.policy_seconds,
            histogram.keldra_store_bulk_prefetch_versioning_duration_seconds =
                self.versioning_seconds,
            "object storage bulk baseline prefetched"
        );
    }
}

fn decode_bucket_policy(
    key: &[u8],
    cached: Cached<Vec<u8>>,
) -> Result<BucketPolicy, MutationError> {
    let Some(encoded) = cached? else {
        return Ok(BucketPolicy::default());
    };
    let identity = BucketIdentity::decode(key).map_err(storage_error)?;
    let id = crate::LogicalRecordId::BucketPolicy {
        tenant_id: identity.tenant_id.0,
        bucket_id: identity.bucket_id.0,
    };
    match decode_current_value(&id, &encoded).map_err(storage_error)? {
        crate::LogicalRecordValue::BucketPolicy {
            tenant_id,
            bucket_id,
            policy,
        } if tenant_id == identity.tenant_id.0 && bucket_id == identity.bucket_id.0 => Ok(policy),
        _ => Err(MutationError::Storage(
            "bucket policy has the wrong logical type or identity".into(),
        )),
    }
}

fn decode_bucket_versioning(
    key: &[u8],
    cached: Cached<Vec<u8>>,
) -> Result<ObjectVersioning, MutationError> {
    let Some(encoded) = cached? else {
        return Ok(ObjectVersioning::default());
    };
    let identity = BucketIdentity::decode(key).map_err(storage_error)?;
    let id = crate::LogicalRecordId::BucketOptions {
        tenant_id: identity.tenant_id.0,
        bucket_id: identity.bucket_id.0,
    };
    match decode_current_value(&id, &encoded).map_err(storage_error)? {
        crate::LogicalRecordValue::BucketOptions {
            tenant_id,
            bucket_id,
            versioning,
        } if tenant_id == identity.tenant_id.0 && bucket_id == identity.bucket_id.0 => {
            Ok(versioning)
        }
        _ => Err(MutationError::Storage(
            "bucket options have the wrong logical type or identity".into(),
        )),
    }
}

fn multi_get_json<T>(
    view: &MutationReadView<'_>,
    cf_name: &'static str,
    keys: &BTreeSet<Vec<u8>>,
) -> Result<(BTreeMap<Vec<u8>, Cached<T>>, f64), MutationError>
where
    T: for<'de> Deserialize<'de>,
{
    let started = std::time::Instant::now();
    let values = view
        .multi_get_raw(cf_name, keys)?
        .into_iter()
        .map(|(key, cached)| {
            let decoded = cached.and_then(|value| {
                value
                    .map(|encoded| serde_json::from_slice(&encoded).map_err(storage_error))
                    .transpose()
            });
            (key, decoded)
        })
        .collect();
    Ok((values, started.elapsed().as_secs_f64()))
}

fn exact_version_key(head_key: &[u8], version: VersionId) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(head_key.len() + 1 + size_of::<u64>());
    encoded.extend_from_slice(head_key);
    encoded.push(0);
    encoded.extend_from_slice(&version.0.to_be_bytes());
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StoreOptions;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn store() -> (tempfile::TempDir, Store) {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        (temporary, store)
    }

    #[tokio::test]
    async fn exact_token_detects_absence_becoming_present() {
        let (_temporary, store) = store().await;
        let key = b"speculative-absent".to_vec();
        let token = MutationReadToken {
            cells: vec![ObservedMutationCell {
                cf_name: CF_METADATA,
                key: key.clone(),
                value: None,
            }],
        };
        let initial = token.revalidate(&store).unwrap();
        assert!(initial.unchanged);
        assert_eq!(initial.checked_keys, 1);

        store
            .db
            .put_cf(store.cf(CF_METADATA).unwrap(), key, b"present")
            .unwrap();
        let changed = token.revalidate(&store).unwrap();
        assert!(!changed.unchanged);
        assert_eq!(changed.checked_keys, 1);
    }

    #[tokio::test]
    async fn exact_token_detects_present_value_change_and_checks_every_key() {
        let (_temporary, store) = store().await;
        let first = b"speculative-first".to_vec();
        let second = b"speculative-second".to_vec();
        let metadata = store.cf(CF_METADATA).unwrap();
        store.db.put_cf(metadata, &first, b"one").unwrap();
        store.db.put_cf(metadata, &second, b"two").unwrap();
        let token = MutationReadToken {
            cells: vec![
                ObservedMutationCell {
                    cf_name: CF_METADATA,
                    key: first.clone(),
                    value: Some(b"one".to_vec()),
                },
                ObservedMutationCell {
                    cf_name: CF_METADATA,
                    key: second,
                    value: Some(b"two".to_vec()),
                },
            ],
        };

        store.db.put_cf(metadata, first, b"changed").unwrap();
        let changed = token.revalidate(&store).unwrap();
        assert!(!changed.unchanged);
        assert_eq!(changed.checked_keys, token.len());

        store.db.delete_cf(metadata, b"speculative-first").unwrap();
        let removed = token.revalidate(&store).unwrap();
        assert!(!removed.unchanged);
        assert_eq!(removed.checked_keys, token.len());
    }

    #[tokio::test]
    async fn snapshot_view_retains_the_value_selected_before_a_current_write() {
        let (_temporary, store) = store().await;
        let key = b"snapshot-cell".to_vec();
        let metadata = store.cf(CF_METADATA).unwrap();
        store.db.put_cf(metadata, &key, b"before").unwrap();
        let view = MutationReadView::snapshot(&store);
        let keys = BTreeSet::from([key.clone()]);
        let selected = view.multi_get_raw(CF_METADATA, &keys).unwrap();

        store.db.put_cf(metadata, &key, b"after").unwrap();
        assert_eq!(
            selected.get(&key).unwrap().as_ref().unwrap().as_deref(),
            Some(b"before".as_slice())
        );
        let revalidated = view.take_token().revalidate(&store).unwrap();
        assert!(!revalidated.unchanged);
        assert_eq!(revalidated.checked_keys, 1);
    }

    #[tokio::test]
    async fn conflicting_component_observations_cannot_be_accepted() {
        let (_temporary, store) = store().await;
        let key = b"component-overlap".to_vec();
        store
            .db
            .put_cf(store.cf(CF_METADATA).unwrap(), &key, b"current")
            .unwrap();
        let speculation = MutationReadSpeculation {
            cache: MutationReadCache::default(),
            tokens: vec![
                MutationReadToken {
                    cells: vec![ObservedMutationCell {
                        cf_name: CF_METADATA,
                        key: key.clone(),
                        value: Some(b"current".to_vec()),
                    }],
                },
                MutationReadToken {
                    cells: vec![ObservedMutationCell {
                        cf_name: CF_METADATA,
                        key,
                        value: Some(b"older".to_vec()),
                    }],
                },
            ],
            metrics: MutationPreparationMetrics::default(),
        };

        let result = speculation.revalidate(&store).unwrap();
        assert!(!result.unchanged);
        assert_eq!(result.checked_keys, 2);
    }

    fn publish(path: &str, command: &str, hash: u8) -> PreparedOperation {
        PreparedOperation::Publish {
            request: PublishRequest {
                key: ObjectKey::new("tenant", "bucket", path).unwrap(),
                blob: BlobRef {
                    hash: [hash; 32],
                    length: 10,
                },
                content_type: None,
                mode: PutMode::PutIfAbsent,
                command_id: Some(command.into()),
                durability: Durability::Local,
            },
            identity: BucketIdentity {
                tenant_id: TenantId(1),
                bucket_id: BucketId(2),
            },
            fingerprint: [0; 32],
        }
    }

    #[test]
    fn conflict_components_close_transitively_and_keep_independent_order() {
        let first = publish("a", "shared-command", 1);
        let second = publish("b", "shared-command", 2);
        let third = publish("c", "third", 2);
        let independent = publish("d", "fourth", 4);
        let operations = [&first, &second, &third, &independent];

        assert_eq!(
            mutation_conflict_components(&operations),
            vec![vec![0, 1, 2], vec![3]]
        );
        assert_eq!(
            assign_components_to_lanes(&mutation_conflict_components(&operations), 2),
            vec![vec![vec![0, 1, 2]], vec![vec![3]]]
        );
    }

    #[test]
    fn clone_source_and_writer_are_co_partitioned() {
        let writer = publish("source", "writer", 1);
        let clone = PreparedOperation::Clone {
            request: CloneRequest {
                source: ObjectKey::new("tenant", "bucket", "source").unwrap(),
                source_version: VersionId(7),
                destination: ObjectKey::new("tenant", "bucket", "destination").unwrap(),
                blob: BlobRef {
                    hash: [2; 32],
                    length: 10,
                },
                content_type: None,
                mode: PutMode::PutIfAbsent,
                command_id: Some("clone".into()),
                durability: Durability::Local,
            },
            identity: BucketIdentity {
                tenant_id: TenantId(1),
                bucket_id: BucketId(2),
            },
            fingerprint: [0; 32],
        };

        assert_eq!(
            mutation_conflict_components(&[&writer, &clone]),
            vec![vec![0, 1]]
        );
    }

    #[tokio::test]
    async fn governed_prefetch_fails_closed_without_complete_bucket_coverage() {
        let (_temporary, store) = store().await;
        let operation = Arc::new(publish("object", "command", 1));
        let error = MutationReadSpeculation::load(&store, &[operation], 1, &BTreeSet::new())
            .await
            .err()
            .unwrap();
        assert!(matches!(error, MutationError::InvalidPolicy(_)));
    }

    #[tokio::test]
    async fn preparation_lanes_enter_blocking_work_in_parallel_and_return_lane_order() {
        let barrier = Arc::new(Barrier::new(2));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let results = run_preparation_lanes(
            vec![vec![vec![0]], vec![vec![1]]],
            Arc::new({
                let barrier = Arc::clone(&barrier);
                let active = Arc::clone(&active);
                let peak = Arc::clone(&peak);
                move |components: Vec<Vec<usize>>| {
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(current, Ordering::SeqCst);
                    barrier.wait();
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(components[0][0])
                }
            }),
        )
        .await
        .unwrap();

        assert_eq!(peak.load(Ordering::SeqCst), 2);
        assert_eq!(
            results
                .into_iter()
                .map(|result| result.value)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
    }
}
