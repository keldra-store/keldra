//! Bounded source-node projection shared by assigned index assemblers.
//!
//! The source journal and exact object versions remain authoritative. This
//! process-local mapper caches only disposable definition-neutral JSON facts
//! and prepared definition projections. A cache miss deterministically reads
//! and projects the exact source again.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use keldra_index::IndexKind;
use keldra_index::v4::Schema;
use keldra_index::v4::build::MergeMutation;
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;

use super::catalog::{CatalogDefinition, CatalogIdentity};
use super::cpu::IndexCpuPool;
use super::json_projection::{ProjectedScalarPointers, project_scalar_pointers};
use super::source::{IndexBuildDiagnostics, IndexBuildObject, IndexSourceMutation};
use super::v4_projection::{
    project_mutation, project_typed_json_from_shared_scalars, projected_mutation_resident_bytes,
    source_matches_schema,
};
use super::working_memory::{IndexWorkingMemory, WorkingMemoryAccount, WorkingMemoryPermit};

const MAPPER_STRIPES: usize = 64;
const CACHE_ENTRY_FIXED_BYTES: usize = 256;
const OUTPUT_ENTRY_FIXED_BYTES: usize = 128;
const PLAN_POINTER_FIXED_BYTES: usize = 128;
const TELEMETRY_REPORT_INTERVAL: u64 = 16_384;

#[derive(Clone)]
pub(crate) struct SharedProjectionMapper {
    inner: Arc<MapperInner>,
}

struct MapperInner {
    reader: ClusterObjectReader,
    cpu: IndexCpuPool,
    definitions: Mutex<BTreeMap<CatalogIdentity, RegisteredDefinition>>,
    cache: Mutex<ProjectionCache>,
    stripes: [tokio::sync::Mutex<()>; MAPPER_STRIPES],
    cache_limit: usize,
    mapping_limit: usize,
    telemetry: MapperTelemetry,
    _memory: WorkingMemoryPermit,
}

#[derive(Default)]
struct MapperTelemetry {
    requests: AtomicU64,
    payload_parses: AtomicU64,
    selected_hits: AtomicU64,
    prepared_hits: AtomicU64,
    union_bypasses: AtomicU64,
}

#[derive(Clone)]
struct RegisteredDefinition {
    tenant_id: u64,
    bucket_id: u64,
    schema: Schema,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SourceProjectionKey {
    tenant_id: u64,
    bucket_id: u64,
    path: String,
    version: u64,
    committed_at_unix_millis: u64,
    content_type: Option<String>,
    content_hash: [u8; 32],
    content_length: u64,
    plan_hash: [u8; 32],
}

struct ProjectionCache {
    entries: HashMap<SourceProjectionKey, CachedProjection>,
    used_bytes: usize,
    clock: u64,
}

struct CachedProjection {
    selected: Option<Arc<ProjectedScalarPointers>>,
    outputs: HashMap<[u8; 32], CachedOutput>,
    resident_bytes: usize,
    last_used: u64,
}

struct CachedOutput {
    mutation: Arc<MergeMutation>,
    diagnostics: IndexBuildDiagnostics,
    resident_bytes: usize,
}

struct ProjectionPlan {
    key: SourceProjectionKey,
    pointers: Vec<String>,
    workspace_bytes: usize,
}

impl SharedProjectionMapper {
    pub(crate) async fn new(
        reader: ClusterObjectReader,
        cpu: IndexCpuPool,
        memory: IndexWorkingMemory,
        bytes: u64,
    ) -> Result<Self, Status> {
        let permit = memory
            .acquire_up_to(WorkingMemoryAccount::SharedProjection, bytes, bytes)
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let admitted = usize::try_from(permit.bytes()).map_err(|_| {
            Status::failed_precondition("shared projection memory exceeds this platform")
        })?;
        let mapping_limit = admitted / 2;
        let cache_limit = admitted.saturating_sub(mapping_limit);
        if mapping_limit == 0 || cache_limit == 0 {
            return Err(Status::failed_precondition(
                "shared projection memory cannot provide cache and mapping workspace",
            ));
        }
        Ok(Self {
            inner: Arc::new(MapperInner {
                reader,
                cpu,
                definitions: Mutex::new(BTreeMap::new()),
                cache: Mutex::new(ProjectionCache {
                    entries: HashMap::new(),
                    used_bytes: 0,
                    clock: 0,
                }),
                stripes: std::array::from_fn(|_| tokio::sync::Mutex::new(())),
                cache_limit,
                mapping_limit,
                telemetry: MapperTelemetry::default(),
                _memory: permit,
            }),
        })
    }

    pub(crate) fn upsert(&self, definition: &CatalogDefinition) -> Result<(), Status> {
        let mut definitions = self
            .inner
            .definitions
            .lock()
            .map_err(|_| Status::internal("shared projection definition lock is poisoned"))?;
        if definition.schema.kind == IndexKind::TypedJson {
            definitions.insert(
                definition.identity(),
                RegisteredDefinition {
                    tenant_id: definition.tenant_id,
                    bucket_id: definition.bucket_id,
                    schema: definition.schema.clone(),
                },
            );
        } else {
            definitions.remove(&definition.identity());
        }
        Ok(())
    }

    pub(crate) fn remove(&self, identity: CatalogIdentity) -> Result<(), Status> {
        self.inner
            .definitions
            .lock()
            .map_err(|_| Status::internal("shared projection definition lock is poisoned"))?
            .remove(&identity);
        Ok(())
    }

    pub(crate) async fn project(
        &self,
        definition: &CatalogDefinition,
        source: IndexSourceMutation,
        maximum_projection_bytes: usize,
    ) -> Result<(MergeMutation, IndexBuildDiagnostics), Status> {
        self.observe_request();
        if definition.schema.kind != IndexKind::TypedJson {
            return Err(Status::internal(
                "non-Typed-JSON source entered the shared scalar mapper",
            ));
        }
        let object = match &source {
            IndexSourceMutation::Upsert(object)
                if source_matches_schema(&definition.schema, object) =>
            {
                object
            }
            _ => {
                return project_typed_json_from_shared_scalars(
                    &definition.schema,
                    source,
                    None,
                    maximum_projection_bytes,
                )
                .map_err(index_status);
            }
        };
        let plan = match self.plan(definition, object) {
            Ok(plan) => plan,
            Err(status) if status.code() == tonic::Code::ResourceExhausted => {
                self.inner
                    .telemetry
                    .union_bypasses
                    .fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    projection.cache = "union_bypass",
                    monotonic_counter.keldra_index_shared_projection_union_bypasses_total = 1_u64,
                    "shared selector plan exceeded its workspace; using exact definition projection"
                );
                return self
                    .project_definition(definition, source, maximum_projection_bytes)
                    .await;
            }
            Err(status) => return Err(status),
        };
        if let Some(output) = self.cached_output(
            &plan.key,
            definition.schema_fingerprint,
            maximum_projection_bytes,
        )? {
            return Ok(output);
        }

        let stripe = cache_stripe(&plan.key);
        let _exclusive_source = self.inner.stripes[stripe].lock().await;
        if let Some(output) = self.cached_output(
            &plan.key,
            definition.schema_fingerprint,
            maximum_projection_bytes,
        )? {
            return Ok(output);
        }

        let selected = match self.cached_selected(&plan.key)? {
            Some(selected) => selected,
            None => match self.map_source(&plan, object).await {
                Ok(selected) => selected,
                // A union of many definitions can be larger than this
                // mapper's bounded workspace even though the requesting
                // definition still fits its own admitted builder lane. The
                // optimization must never introduce a new functional limit:
                // reopen the exact payload and use the released per-definition
                // projector for this source.
                Err(status) if status.code() == tonic::Code::ResourceExhausted => {
                    self.inner
                        .telemetry
                        .union_bypasses
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(
                        projection.cache = "union_bypass",
                        monotonic_counter.keldra_index_shared_projection_union_bypasses_total =
                            1_u64,
                        "shared selector union exceeded its workspace; using exact definition projection"
                    );
                    return self
                        .project_definition(definition, source, maximum_projection_bytes)
                        .await;
                }
                Err(status) => return Err(status),
            },
        };
        let schema = definition.schema.clone();
        let source_for_projection = source.clone();
        let selected_for_projection = selected.clone();
        let assembly_started = Instant::now();
        let projected = self
            .inner
            .cpu
            .submit(move || {
                project_typed_json_from_shared_scalars(
                    &schema,
                    source_for_projection,
                    selected_for_projection.as_deref(),
                    maximum_projection_bytes,
                )
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(index_status)?;
        let assembly_seconds = assembly_started.elapsed().as_secs_f64();
        self.cache_output(
            plan.key,
            definition.schema_fingerprint,
            selected,
            &projected,
        )?;
        tracing::debug!(
            histogram.keldra_index_shared_projection_assembly_duration_seconds = assembly_seconds,
            "index assembler consumed shared source facts"
        );
        Ok(projected)
    }

    async fn project_definition(
        &self,
        definition: &CatalogDefinition,
        source: IndexSourceMutation,
        maximum_projection_bytes: usize,
    ) -> Result<(MergeMutation, IndexBuildDiagnostics), Status> {
        let payload = match &source {
            IndexSourceMutation::Upsert(object)
                if source_matches_schema(&definition.schema, object) =>
            {
                let reference = keldra_store::BlobRef {
                    hash: object.content_hash,
                    length: object.content_length,
                };
                let payload = self.inner.reader.open_blob_payload(&reference).await?;
                tracing::debug!(
                    monotonic_counter.keldra_index_projection_payload_fetches_total = 1_u64,
                    monotonic_counter.keldra_index_projection_payload_bytes_total =
                        object.content_length,
                    projection.cache = "union_bypass",
                    "exact source payload fetched for definition projection"
                );
                Some(payload)
            }
            _ => None,
        };
        let schema = definition.schema.clone();
        self.inner
            .cpu
            .submit(move || {
                let mut payload = payload;
                project_mutation(
                    &schema,
                    source,
                    payload
                        .as_mut()
                        .map(|reader| reader as &mut dyn std::io::Read),
                    maximum_projection_bytes,
                )
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(index_status)
    }

    fn plan(
        &self,
        definition: &CatalogDefinition,
        object: &IndexBuildObject,
    ) -> Result<ProjectionPlan, Status> {
        let definitions = self
            .inner
            .definitions
            .lock()
            .map_err(|_| Status::internal("shared projection definition lock is poisoned"))?;
        let mut pointers = BTreeSet::new();
        let mut workspace_bytes = CACHE_ENTRY_FIXED_BYTES
            .checked_add(object.path.capacity())
            .and_then(|bytes| {
                bytes.checked_add(object.content_type.as_ref().map_or(0, String::capacity))
            })
            .ok_or_else(|| Status::resource_exhausted("shared projection plan size overflow"))?;
        for pointer in definitions
            .values()
            .filter(|registered| {
                registered.tenant_id == definition.tenant_id
                    && registered.bucket_id == definition.bucket_id
                    && source_matches_schema(&registered.schema, object)
            })
            .flat_map(|registered| {
                registered
                    .schema
                    .fields
                    .iter()
                    .map(|field| &field.source_selector)
            })
            .chain(
                definition
                    .schema
                    .fields
                    .iter()
                    .map(|field| &field.source_selector),
            )
        {
            if pointers.contains(pointer) {
                continue;
            }
            let pointer_workspace = PLAN_POINTER_FIXED_BYTES
                .checked_add(pointer.len())
                .and_then(|bytes| bytes.checked_mul(2))
                .ok_or_else(|| {
                    Status::resource_exhausted("shared projection plan size overflow")
                })?;
            workspace_bytes = workspace_bytes
                .checked_add(pointer_workspace)
                .ok_or_else(|| {
                    Status::resource_exhausted("shared projection plan size overflow")
                })?;
            if workspace_bytes >= self.inner.mapping_limit {
                return Err(Status::resource_exhausted(
                    "shared projection selector union exceeds its mapping workspace",
                ));
            }
            pointers.insert(pointer.clone());
        }
        let pointers = pointers.into_iter().collect::<Vec<_>>();
        let mut plan = blake3::Hasher::new();
        for pointer in &pointers {
            plan.update(&(pointer.len() as u64).to_be_bytes());
            plan.update(pointer.as_bytes());
        }
        Ok(ProjectionPlan {
            key: SourceProjectionKey {
                tenant_id: definition.tenant_id,
                bucket_id: definition.bucket_id,
                path: object.path.clone(),
                version: object.version,
                committed_at_unix_millis: object.committed_at_unix_millis,
                content_type: object.content_type.clone(),
                content_hash: object.content_hash,
                content_length: object.content_length,
                plan_hash: *plan.finalize().as_bytes(),
            },
            pointers,
            workspace_bytes,
        })
    }

    async fn map_source(
        &self,
        plan: &ProjectionPlan,
        object: &IndexBuildObject,
    ) -> Result<Option<Arc<ProjectedScalarPointers>>, Status> {
        let map_started = Instant::now();
        let reference = keldra_store::BlobRef {
            hash: object.content_hash,
            length: object.content_length,
        };
        let mut payload = self.inner.reader.open_blob_payload(&reference).await?;
        tracing::debug!(
            monotonic_counter.keldra_index_projection_payload_fetches_total = 1_u64,
            monotonic_counter.keldra_index_projection_payload_bytes_total = object.content_length,
            projection.cache = "miss",
            "exact source payload fetched by shared projection mapper"
        );
        let pointers = plan.pointers.clone();
        let mapping_limit = self
            .inner
            .mapping_limit
            .checked_sub(plan.workspace_bytes)
            .ok_or_else(|| {
                Status::resource_exhausted(
                    "shared projection selector union exhausts its mapping workspace",
                )
            })?;
        let mapped = self
            .inner
            .cpu
            .submit(move || project_scalar_pointers(&mut payload, &pointers, mapping_limit))
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(index_status)?
            .map(Arc::new);
        self.cache_selected(plan.key.clone(), mapped.clone())?;
        self.inner
            .telemetry
            .payload_parses
            .fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            projection.cache = "miss",
            projection.selected_pointers = plan.pointers.len() as u64,
            histogram.keldra_index_shared_projection_map_duration_seconds =
                map_started.elapsed().as_secs_f64(),
            monotonic_counter.keldra_index_shared_projection_payload_parses_total = 1_u64,
            "index source payload mapped once for assigned definitions"
        );
        Ok(mapped)
    }

    fn cached_output(
        &self,
        key: &SourceProjectionKey,
        schema: [u8; 32],
        maximum_projection_bytes: usize,
    ) -> Result<Option<(MergeMutation, IndexBuildDiagnostics)>, Status> {
        let mut cache = self.cache()?;
        let tick = cache.tick();
        let Some(entry) = cache.entries.get_mut(key) else {
            return Ok(None);
        };
        entry.last_used = tick;
        let Some(output) = entry.outputs.get(&schema) else {
            return Ok(None);
        };
        if output.resident_bytes > maximum_projection_bytes {
            return Ok(None);
        }
        tracing::debug!(
            projection.cache = "prepared_hit",
            monotonic_counter.keldra_index_shared_projection_prepared_hits_total = 1_u64,
            "index assembler reused one prepared source projection"
        );
        self.inner
            .telemetry
            .prepared_hits
            .fetch_add(1, Ordering::Relaxed);
        Ok(Some(((*output.mutation).clone(), output.diagnostics)))
    }

    fn cached_selected(
        &self,
        key: &SourceProjectionKey,
    ) -> Result<Option<Option<Arc<ProjectedScalarPointers>>>, Status> {
        let mut cache = self.cache()?;
        let tick = cache.tick();
        let Some(entry) = cache.entries.get_mut(key) else {
            return Ok(None);
        };
        entry.last_used = tick;
        tracing::debug!(
            projection.cache = "selected_hit",
            monotonic_counter.keldra_index_shared_projection_selected_hits_total = 1_u64,
            "index assembler reused definition-neutral selected values"
        );
        self.inner
            .telemetry
            .selected_hits
            .fetch_add(1, Ordering::Relaxed);
        Ok(Some(entry.selected.clone()))
    }

    fn cache_selected(
        &self,
        key: SourceProjectionKey,
        selected: Option<Arc<ProjectedScalarPointers>>,
    ) -> Result<(), Status> {
        let selected_bytes = selected
            .as_deref()
            .map_or(Ok(1usize), ProjectedScalarPointers::resident_bytes)
            .map_err(index_status)?;
        let resident = CACHE_ENTRY_FIXED_BYTES
            .checked_add(key.path.capacity())
            .and_then(|bytes| {
                bytes.checked_add(key.content_type.as_ref().map_or(0, String::capacity))
            })
            .and_then(|bytes| bytes.checked_add(selected_bytes))
            .ok_or_else(|| Status::resource_exhausted("shared projection cache size overflow"))?;
        if resident > self.inner.cache_limit {
            return Ok(());
        }
        let mut cache = self.cache()?;
        let tick = cache.tick();
        if cache.entries.contains_key(&key) {
            return Ok(());
        }
        cache.used_bytes = cache.used_bytes.saturating_add(resident);
        cache.entries.insert(
            key,
            CachedProjection {
                selected,
                outputs: HashMap::new(),
                resident_bytes: resident,
                last_used: tick,
            },
        );
        cache.evict_to(self.inner.cache_limit);
        Ok(())
    }

    fn cache_output(
        &self,
        key: SourceProjectionKey,
        schema: [u8; 32],
        selected: Option<Arc<ProjectedScalarPointers>>,
        projected: &(MergeMutation, IndexBuildDiagnostics),
    ) -> Result<(), Status> {
        self.cache_selected(key.clone(), selected)?;
        let mutation_bytes =
            projected_mutation_resident_bytes(&projected.0).map_err(index_status)?;
        let resident = OUTPUT_ENTRY_FIXED_BYTES
            .checked_add(mutation_bytes)
            .ok_or_else(|| Status::resource_exhausted("prepared projection size overflow"))?;
        if resident > self.inner.cache_limit {
            return Ok(());
        }
        let mut cache = self.cache()?;
        let tick = cache.tick();
        if !cache.make_room(self.inner.cache_limit, resident, Some(&key)) {
            return Ok(());
        }
        let Some(entry) = cache.entries.get_mut(&key) else {
            return Ok(());
        };
        entry.last_used = tick;
        if entry.outputs.contains_key(&schema) {
            return Ok(());
        }
        entry.outputs.insert(
            schema,
            CachedOutput {
                mutation: Arc::new(projected.0.clone()),
                diagnostics: projected.1,
                resident_bytes: mutation_bytes,
            },
        );
        entry.resident_bytes = entry.resident_bytes.saturating_add(resident);
        cache.used_bytes = cache.used_bytes.saturating_add(resident);
        cache.evict_to(self.inner.cache_limit);
        Ok(())
    }

    fn cache(&self) -> Result<std::sync::MutexGuard<'_, ProjectionCache>, Status> {
        self.inner
            .cache
            .lock()
            .map_err(|_| Status::internal("shared projection cache lock is poisoned"))
    }

    fn observe_request(&self) {
        let requests = self
            .inner
            .telemetry
            .requests
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        if requests % TELEMETRY_REPORT_INTERVAL != 0 {
            return;
        }
        let (cache_entries, cache_bytes) = self
            .inner
            .cache
            .lock()
            .map(|cache| (cache.entries.len() as u64, cache.used_bytes as u64))
            .unwrap_or((0, 0));
        tracing::info!(
            projection.requests = requests,
            projection.payload_parses = self.inner.telemetry.payload_parses.load(Ordering::Relaxed),
            projection.selected_hits = self.inner.telemetry.selected_hits.load(Ordering::Relaxed),
            projection.prepared_hits = self.inner.telemetry.prepared_hits.load(Ordering::Relaxed),
            projection.union_bypasses = self.inner.telemetry.union_bypasses.load(Ordering::Relaxed),
            projection.cache_entries = cache_entries,
            projection.cache_bytes = cache_bytes,
            "shared index projection cumulative summary"
        );
    }
}

impl ProjectionCache {
    fn tick(&mut self) -> u64 {
        self.clock = self.clock.wrapping_add(1);
        self.clock
    }

    fn evict_to(&mut self, limit: usize) {
        while self.used_bytes > limit {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone())
            else {
                self.used_bytes = 0;
                return;
            };
            if let Some(removed) = self.entries.remove(&oldest) {
                self.used_bytes = self.used_bytes.saturating_sub(removed.resident_bytes);
            }
        }
    }

    fn make_room(
        &mut self,
        limit: usize,
        additional: usize,
        preserve: Option<&SourceProjectionKey>,
    ) -> bool {
        while self.used_bytes.saturating_add(additional) > limit {
            let Some(oldest) = self
                .entries
                .iter()
                .filter(|(key, _)| preserve != Some(*key))
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone())
            else {
                return false;
            };
            if let Some(removed) = self.entries.remove(&oldest) {
                self.used_bytes = self.used_bytes.saturating_sub(removed.resident_bytes);
            }
        }
        true
    }
}

fn cache_stripe(key: &SourceProjectionKey) -> usize {
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hash);
    usize::try_from(hash.finish()).unwrap_or(0) % MAPPER_STRIPES
}

fn index_status(error: keldra_index::IndexError) -> Status {
    match error {
        keldra_index::IndexError::ResourceLimit { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        keldra_index::IndexError::Io(_) => Status::unavailable(error.to_string()),
        _ => Status::data_loss(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(ordinal: u64) -> SourceProjectionKey {
        SourceProjectionKey {
            tenant_id: 1,
            bucket_id: 2,
            path: format!("records/{ordinal}.json"),
            version: ordinal,
            committed_at_unix_millis: ordinal,
            content_type: Some("application/json".into()),
            content_hash: [ordinal as u8; 32],
            content_length: 72 * 1024,
            plan_hash: [0x55; 32],
        }
    }

    fn entry(bytes: usize, last_used: u64) -> CachedProjection {
        CachedProjection {
            selected: None,
            outputs: HashMap::new(),
            resident_bytes: bytes,
            last_used,
        }
    }

    #[test]
    fn cache_room_evicts_oldest_without_dropping_the_source_being_assembled() {
        let retained = key(1);
        let oldest = key(2);
        let newest = key(3);
        let mut cache = ProjectionCache {
            entries: HashMap::from([
                (retained.clone(), entry(30, 1)),
                (oldest.clone(), entry(30, 2)),
                (newest.clone(), entry(30, 3)),
            ]),
            used_bytes: 90,
            clock: 3,
        };

        assert!(cache.make_room(100, 35, Some(&retained)));
        assert!(cache.entries.contains_key(&retained));
        assert!(!cache.entries.contains_key(&oldest));
        assert!(cache.entries.contains_key(&newest));
        assert_eq!(cache.used_bytes, 60);
    }

    #[test]
    fn exact_source_and_plan_identity_are_part_of_the_cache_key() {
        let original = key(7);
        let mut changed_bytes = original.clone();
        changed_bytes.content_hash = [0xaa; 32];
        let mut changed_plan = original.clone();
        changed_plan.plan_hash = [0xbb; 32];

        assert_ne!(original, changed_bytes);
        assert_ne!(original, changed_plan);
    }
}
