//! Bounded handoff of committed payloads to the memory-first indexing path.
//!
//! This is a disposable acceleration cache, never an ordering or durability
//! authority. Journal replay names the exact committed version before a
//! consumer may take bytes from here. Overflow evicts FIFO and therefore
//! degrades to an exact payload read without delaying object ingestion.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::sync::{Arc, Mutex};

use keldra_store::{BatchOperation, MutationReceipt};

use super::catalog::PhysicalCatalogSnapshot;
use super::json_projection::{ProjectedScalarPointers, project_scalar_pointers};

const ENTRY_OVERHEAD_BYTES: usize = 256;

#[derive(Clone)]
pub(crate) struct HotProjectionIngress {
    inner: Arc<Mutex<HotState>>,
    maximum_bytes: usize,
    cpu: Arc<std::sync::OnceLock<super::cpu::IndexCpuPool>>,
    changes: tokio::sync::broadcast::Sender<()>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct HotKey {
    tenant_id: u64,
    bucket_id: u64,
    path: String,
    version: u64,
}

struct HotPayload {
    selected: ProjectedScalarPointers,
    charge: usize,
    sequence: u64,
    router_generation: [u8; 32],
}

#[derive(Default)]
struct HotState {
    payloads: BTreeMap<HotKey, HotPayload>,
    /// Exact sequence lookup prevents a consumed/replaced payload from
    /// scanning every buffered entry.  Ordered eviction remains logarithmic
    /// and no stale FIFO tombstones accumulate.
    fifo: BTreeMap<u64, HotKey>,
    used_bytes: usize,
    reserved_bytes: usize,
    router_bytes: usize,
    next_sequence: u64,
    compiled_routes: BTreeMap<(u64, u64), CompiledBucketRouter>,
    retired_selectors: Vec<RetiredSelectors>,
}

#[derive(Clone, Default)]
struct CompiledBucketRouter {
    generation: [u8; 32],
    root: PrefixNode,
    pointers: Arc<[String]>,
    charge: usize,
    selector_charge: usize,
}

struct BuildingBucketRouter {
    root: PrefixNode,
    pointers: BTreeSet<String>,
}

struct RetiredSelectors {
    pointers: Arc<[String]>,
    charge: usize,
}

#[derive(Clone, Default)]
struct PrefixNode {
    children: BTreeMap<u8, PrefixNode>,
    raw_prefix: RouteContentTypes,
    segment_boundary: RouteContentTypes,
}

#[derive(Clone, Default)]
struct RouteContentTypes {
    any: bool,
    exact: BTreeSet<String>,
}

impl BuildingBucketRouter {
    fn insert(&mut self, path_prefix: &str, content_type: Option<&str>) {
        let mut node = &mut self.root;
        for byte in path_prefix.bytes() {
            node = node.children.entry(byte).or_default();
        }
        let terminal = if path_prefix.is_empty() || path_prefix.ends_with('/') {
            &mut node.raw_prefix
        } else {
            &mut node.segment_boundary
        };
        terminal.insert(content_type);
    }
}

impl CompiledBucketRouter {
    fn matches_path(&self, path: &str) -> bool {
        let mut node = &self.root;
        let bytes = path.as_bytes();
        if node.raw_prefix.any
            || !node.raw_prefix.exact.is_empty()
            || (bytes.is_empty()
                && (node.segment_boundary.any || !node.segment_boundary.exact.is_empty()))
        {
            return true;
        }
        for (index, byte) in bytes.iter().copied().enumerate() {
            let Some(child) = node.children.get(&byte) else {
                return false;
            };
            node = child;
            if node.raw_prefix.any
                || !node.raw_prefix.exact.is_empty()
                || ((node.segment_boundary.any || !node.segment_boundary.exact.is_empty())
                    && (index + 1 == bytes.len() || bytes[index + 1] == b'/'))
            {
                return true;
            }
        }
        false
    }

    fn matches(&self, path: &str, content_type: Option<&str>) -> bool {
        let mut node = &self.root;
        let bytes = path.as_bytes();
        if node.raw_prefix.matches(content_type)
            || (bytes.is_empty() && node.segment_boundary.matches(content_type))
        {
            return true;
        }
        for (index, byte) in bytes.iter().copied().enumerate() {
            let Some(child) = node.children.get(&byte) else {
                return false;
            };
            node = child;
            if node.raw_prefix.matches(content_type)
                || (node.segment_boundary.matches(content_type)
                    && (index + 1 == bytes.len() || bytes[index + 1] == b'/'))
            {
                return true;
            }
        }
        false
    }
}

impl RouteContentTypes {
    fn insert(&mut self, content_type: Option<&str>) {
        match content_type {
            Some(content_type) => {
                self.exact.insert(content_type.to_owned());
            }
            None => self.any = true,
        }
    }

    fn matches(&self, content_type: Option<&str>) -> bool {
        self.any || content_type.is_some_and(|content_type| self.exact.contains(content_type))
    }
}

pub(crate) struct PendingHotProjection {
    tenant_id: u64,
    bucket_id: u64,
    path: String,
    bytes: Vec<u8>,
    charge: usize,
    state: Arc<Mutex<HotState>>,
    reservation_held: bool,
    project: bool,
    router_generation: [u8; 32],
    pointers: Arc<[String]>,
}

impl Drop for PendingHotProjection {
    fn drop(&mut self) {
        if self.reservation_held {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.reserved_bytes = state.reserved_bytes.saturating_sub(self.charge);
        }
    }
}

impl HotProjectionIngress {
    pub(crate) fn new(maximum_bytes: u64) -> Result<Self, &'static str> {
        let maximum_bytes = usize::try_from(maximum_bytes)
            .map_err(|_| "TypedJson hot-ingress memory exceeds this platform")?;
        if maximum_bytes == 0 {
            return Err("TypedJson hot-ingress memory must be positive");
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(HotState::default())),
            maximum_bytes,
            cpu: Arc::new(std::sync::OnceLock::new()),
            changes: tokio::sync::broadcast::channel(1_024).0,
        })
    }

    pub(crate) fn subscribe(&self) -> tokio::sync::broadcast::Receiver<()> {
        self.changes.subscribe()
    }

    pub(crate) fn install_cpu(&self, cpu: super::cpu::IndexCpuPool) -> Result<(), &'static str> {
        self.cpu
            .set(cpu)
            .map_err(|_| "TypedJson hot indexing CPU pool was installed more than once")
    }

    /// Compile one immutable physical-catalog generation. The published
    /// catalogue already owns every route and selector string; object writes
    /// only clone the resulting selector `Arc`.
    pub(crate) fn replace_compiled_catalog(&self, snapshot: Arc<PhysicalCatalogSnapshot>) -> bool {
        let estimated = estimate_router_bytes(&snapshot);
        {
            let mut state = self
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            reclaim_retired_selectors(&mut state);
            if total_bytes(&state).saturating_add(estimated) > self.maximum_bytes {
                disable_routes(&mut state);
                emit_router_resident(&state, self.maximum_bytes);
                return false;
            }
            state.reserved_bytes = state.reserved_bytes.saturating_add(estimated);
        }

        let mut building = BTreeMap::<(u64, u64), BuildingBucketRouter>::new();
        for recipe in snapshot.recipes.iter() {
            let bucket = building
                .entry((recipe.family.tenant_id, recipe.family.bucket_id))
                .or_insert_with(|| BuildingBucketRouter {
                    root: PrefixNode::default(),
                    pointers: BTreeSet::new(),
                });
            bucket.insert(&recipe.path_prefix, recipe.content_type.as_deref());
            bucket.pointers.extend(recipe.selectors.iter().cloned());
        }
        let mut compiled = BTreeMap::new();
        for (identity, bucket) in building {
            let pointers = Arc::<[String]>::from(bucket.pointers.into_iter().collect::<Vec<_>>());
            let selector_charge = selector_resident_bytes(&pointers);
            let charge = prefix_node_resident_bytes(&bucket.root)
                .saturating_add(selector_charge)
                .saturating_add(ENTRY_OVERHEAD_BYTES);
            compiled.insert(
                identity,
                CompiledBucketRouter {
                    generation: snapshot.identity,
                    root: bucket.root,
                    pointers,
                    charge,
                    selector_charge,
                },
            );
        }
        let compiled_charge = compiled.values().map(|router| router.charge).sum::<usize>();
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.reserved_bytes = state.reserved_bytes.saturating_sub(estimated);
        disable_routes(&mut state);
        reclaim_retired_selectors(&mut state);
        if total_bytes(&state).saturating_add(compiled_charge) > self.maximum_bytes {
            emit_router_resident(&state, self.maximum_bytes);
            return false;
        }
        state.compiled_routes = compiled;
        state.router_bytes = state.router_bytes.saturating_add(compiled_charge);
        emit_router_resident(&state, self.maximum_bytes);
        true
    }

    pub(crate) fn pending(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        operation: &BatchOperation,
    ) -> Option<PendingHotProjection> {
        let (key, content_type, bytes, match_content_type) = match operation {
            BatchOperation::Put(request) => (
                &request.key,
                request.content_type.as_deref(),
                Some(request.bytes.as_slice()),
                true,
            ),
            BatchOperation::Publish(request) => {
                (&request.key, request.content_type.as_deref(), None, true)
            }
            BatchOperation::Clone(request) => (
                &request.destination,
                request.content_type.as_deref(),
                None,
                true,
            ),
            BatchOperation::Delete(request) => (&request.key, None, None, false),
        };
        if key.path().split('/').any(|segment| segment == "_keldra") {
            return None;
        }
        let route = {
            let mut state = self
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            reclaim_retired_selectors(&mut state);
            let router = state.compiled_routes.get(&(tenant_id, bucket_id))?;
            let matches = if match_content_type {
                router.matches(key.path(), content_type)
            } else {
                router.matches_path(key.path())
            };
            matches.then(|| (router.generation, router.pointers.clone()))
        };
        let (router_generation, pointers) = route?;
        if pointers.is_empty() {
            return Some(self.replay_only_pending());
        }
        if bytes.is_none() {
            return Some(self.replay_only_pending());
        }
        let bytes = bytes.expect("put bytes were checked");
        tracing::debug!(
            pipeline.stage = "hot_payload",
            monotonic_counter.keldra_index_pipeline_hot_offered_rows_total = 1_u64,
            monotonic_counter.keldra_index_pipeline_hot_offered_bytes_total = bytes.len() as u64,
            "committed payload offered to the bounded indexing fast path"
        );
        // The map key and FIFO key each own the path. Account both retained
        // copies before cloning any payload bytes.
        let charge = ENTRY_OVERHEAD_BYTES
            .checked_add(key.path().len().checked_mul(2)?)?
            .checked_add(bytes.len())?;
        if charge > self.maximum_bytes {
            emit_rejected("entry_too_large", bytes.len() as u64);
            return Some(self.replay_only_pending());
        }
        {
            let mut state = self
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let available = self
                .maximum_bytes
                .saturating_sub(state.used_bytes)
                .saturating_sub(state.reserved_bytes)
                .saturating_sub(state.router_bytes);
            if charge > available {
                drop(state);
                emit_rejected("inflight_budget_full", bytes.len() as u64);
                return Some(self.replay_only_pending());
            }
            state.reserved_bytes = match state.reserved_bytes.checked_add(charge) {
                Some(bytes) => bytes,
                None => {
                    drop(state);
                    emit_rejected("size_overflow", bytes.len() as u64);
                    return Some(self.replay_only_pending());
                }
            };
        }
        Some(PendingHotProjection {
            tenant_id,
            bucket_id,
            path: key.path().to_owned(),
            bytes: bytes.to_vec(),
            charge,
            state: self.inner.clone(),
            reservation_held: true,
            project: true,
            router_generation,
            pointers,
        })
    }

    fn replay_only_pending(&self) -> PendingHotProjection {
        PendingHotProjection {
            tenant_id: 0,
            bucket_id: 0,
            path: String::new(),
            bytes: Vec::new(),
            charge: 0,
            state: self.inner.clone(),
            reservation_held: false,
            project: false,
            router_generation: [0; 32],
            pointers: Arc::from([]),
        }
    }

    pub(crate) fn admit_committed(
        &self,
        pending: Option<PendingHotProjection>,
        receipt: &MutationReceipt,
    ) {
        let Some(pending) = pending else {
            return;
        };
        let _ = self.changes.send(());
        if receipt.replayed || receipt.deleted {
            emit_replay_required("not_a_new_live_head", pending.bytes.len() as u64);
            return;
        }
        if !pending.project {
            emit_replay_required("hot_projection_unavailable", pending.bytes.len() as u64);
            return;
        }
        let payload_bytes = pending.bytes.len() as u64;
        let version = receipt.version.0;
        let ingress = self.clone();
        #[cfg(test)]
        {
            let selected = project_scalar_pointers(
                &mut Cursor::new(&pending.bytes),
                &pending.pointers,
                ingress.maximum_bytes,
            );
            match selected {
                Ok(Some(selected)) => ingress.admit_selected(pending, version, selected),
                Ok(None) => emit_replay_required("payload_not_selected", payload_bytes),
                Err(_) => emit_replay_required("preparation_failed", payload_bytes),
            }
            return;
        }
        #[cfg(not(test))]
        {
            let Some(cpu) = ingress.cpu.get().cloned() else {
                emit_replay_required("cpu_pool_unavailable", payload_bytes);
                return;
            };
            tokio::spawn(async move {
                let maximum = ingress.maximum_bytes;
                let selected = cpu
                    .submit(move || {
                        project_scalar_pointers(
                            &mut Cursor::new(&pending.bytes),
                            &pending.pointers,
                            maximum,
                        )
                        .map(|selected| (pending, selected))
                    })
                    .await;
                match selected {
                    Ok(Ok((pending, Some(selected)))) => {
                        ingress.admit_selected(pending, version, selected)
                    }
                    Ok(Ok((_pending, None))) => {
                        emit_replay_required("payload_not_selected", payload_bytes)
                    }
                    Ok(Err(error)) => {
                        tracing::debug!(%error, "hot projection preparation fell back to journal replay");
                        emit_replay_required("preparation_failed", payload_bytes);
                    }
                    Err(error) => {
                        tracing::debug!(%error, "hot projection CPU task fell back to journal replay");
                        emit_replay_required("cpu_failed", payload_bytes);
                    }
                }
            });
        }
    }

    fn admit_selected(
        &self,
        mut pending: PendingHotProjection,
        version: u64,
        selected: ProjectedScalarPointers,
    ) {
        let selected_bytes = match selected.resident_bytes() {
            Ok(bytes) => bytes,
            Err(_) => return,
        };
        let charge = match ENTRY_OVERHEAD_BYTES
            .checked_add(pending.path.len().saturating_mul(2))
            .and_then(|bytes| bytes.checked_add(selected_bytes))
        {
            Some(charge) if charge <= self.maximum_bytes => charge,
            _ => return,
        };
        let key = HotKey {
            tenant_id: pending.tenant_id,
            bucket_id: pending.bucket_id,
            path: std::mem::take(&mut pending.path),
            version,
        };
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reclaim_retired_selectors(&mut state);
        state.reserved_bytes = state.reserved_bytes.saturating_sub(pending.charge);
        pending.reservation_held = false;
        if state
            .compiled_routes
            .get(&(key.tenant_id, key.bucket_id))
            .map(|router| router.generation)
            != Some(pending.router_generation)
        {
            emit_replay_required("catalog_changed", selected_bytes as u64);
            return;
        }
        if let Some(previous) = state.payloads.remove(&key) {
            state.used_bytes = state.used_bytes.saturating_sub(previous.charge);
            state.fifo.remove(&previous.sequence);
        }
        while total_bytes(&state).saturating_add(charge) > self.maximum_bytes {
            let Some((sequence, evicted)) = state.fifo.pop_first() else {
                break;
            };
            if state
                .payloads
                .get(&evicted)
                .is_some_and(|payload| payload.sequence == sequence)
                && let Some(payload) = state.payloads.remove(&evicted)
            {
                super::v1_telemetry::V1PipelineTelemetry::add(
                    &super::v1_telemetry::global().hot_evictions,
                    1,
                );
                state.used_bytes = state.used_bytes.saturating_sub(payload.charge);
                tracing::debug!(
                    pipeline.stage = "hot_prepared",
                    monotonic_counter.keldra_index_pipeline_replay_required_rows_total = 1_u64,
                    monotonic_counter.keldra_index_pipeline_replay_required_bytes_total =
                        payload.charge as u64,
                    "TypedJson hot-ingress selection evicted for bounded memory"
                );
            }
        }
        if total_bytes(&state).saturating_add(charge) > self.maximum_bytes {
            drop(state);
            emit_replay_required("prepared_budget_full", selected_bytes as u64);
            return;
        }
        let Some(next_sequence) = state.next_sequence.checked_add(1) else {
            drop(state);
            emit_replay_required("sequence_exhausted", selected_bytes as u64);
            return;
        };
        state.next_sequence = next_sequence;
        let sequence = state.next_sequence;
        state.used_bytes = state.used_bytes.saturating_add(charge);
        super::v1_telemetry::V1PipelineTelemetry::set(
            &super::v1_telemetry::global().stage_resident_bytes,
            total_bytes(&state) as u64,
        );
        super::v1_telemetry::V1PipelineTelemetry::set(
            &super::v1_telemetry::global().stage_limit_bytes,
            self.maximum_bytes as u64,
        );
        state.fifo.insert(sequence, key.clone());
        state.payloads.insert(
            key,
            HotPayload {
                selected,
                charge,
                sequence,
                router_generation: pending.router_generation,
            },
        );
        tracing::debug!(
            pipeline.stage = "hot_prepared",
            gauge.keldra_index_pipeline_stage_resident_bytes = state.used_bytes as u64,
            gauge.keldra_index_pipeline_stage_limit_bytes = self.maximum_bytes as u64,
            monotonic_counter.keldra_index_pipeline_hot_admitted_rows_total = 1_u64,
            monotonic_counter.keldra_index_pipeline_hot_admitted_bytes_total =
                selected_bytes as u64,
            "committed compact selection admitted to TypedJson hot ingress"
        );
    }

    pub(crate) fn take_exact_selected(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
        version: u64,
    ) -> Option<ProjectedScalarPointers> {
        let key = HotKey {
            tenant_id,
            bucket_id,
            path: path.to_owned(),
            version,
        };
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reclaim_retired_selectors(&mut state);
        let Some(payload) = state.payloads.remove(&key) else {
            return None;
        };
        let current_generation = state
            .compiled_routes
            .get(&(tenant_id, bucket_id))
            .map(|router| router.generation);
        let router_compatible = current_generation == Some(payload.router_generation);
        state.used_bytes = state.used_bytes.saturating_sub(payload.charge);
        super::v1_telemetry::V1PipelineTelemetry::set(
            &super::v1_telemetry::global().stage_resident_bytes,
            total_bytes(&state) as u64,
        );
        state.fifo.remove(&payload.sequence);
        tracing::debug!(
            pipeline.stage = "hot_prepared",
            gauge.keldra_index_pipeline_stage_resident_bytes = state.used_bytes as u64,
            gauge.keldra_index_pipeline_stage_limit_bytes = self.maximum_bytes as u64,
            monotonic_counter.keldra_index_pipeline_hot_payload_rows_total = 1_u64,
            monotonic_counter.keldra_index_pipeline_hot_payload_bytes_total = payload.charge as u64,
            pipeline.router_compatible = router_compatible,
            "journal-ordered TypedJson projection consumed hot selection"
        );
        router_compatible.then_some(payload.selected)
    }

    #[cfg(test)]
    fn used_bytes(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .used_bytes
    }

    #[cfg(test)]
    fn reserved_bytes(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .reserved_bytes
    }

    #[cfg(test)]
    fn router_bytes(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .router_bytes
    }

    #[cfg(test)]
    fn fifo_len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fifo
            .len()
    }

    #[cfg(test)]
    pub(crate) fn activate_test_route(&self, tenant_id: u64, bucket_id: u64) {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut router = BuildingBucketRouter {
            root: PrefixNode::default(),
            pointers: BTreeSet::new(),
        };
        router.insert("", Some("application/json"));
        router.pointers.insert("/value".into());
        let pointers = Arc::from(router.pointers.into_iter().collect::<Vec<_>>());
        let selector_charge = selector_resident_bytes(&pointers);
        let charge = prefix_node_resident_bytes(&router.root)
            .saturating_add(selector_charge)
            .saturating_add(ENTRY_OVERHEAD_BYTES);
        state.router_bytes = state.router_bytes.saturating_add(charge);
        state.compiled_routes.insert(
            (tenant_id, bucket_id),
            CompiledBucketRouter {
                generation: [9; 32],
                root: router.root,
                pointers,
                charge,
                selector_charge,
            },
        );
    }
}

fn total_bytes(state: &HotState) -> usize {
    state
        .used_bytes
        .saturating_add(state.reserved_bytes)
        .saturating_add(state.router_bytes)
}

fn selector_resident_bytes(pointers: &[String]) -> usize {
    std::mem::size_of_val(pointers)
        .saturating_add(pointers.iter().map(String::capacity).sum::<usize>())
}

fn prefix_node_resident_bytes(node: &PrefixNode) -> usize {
    std::mem::size_of::<PrefixNode>()
        .saturating_add(
            node.raw_prefix
                .exact
                .iter()
                .chain(node.segment_boundary.exact.iter())
                .map(|value| std::mem::size_of::<String>().saturating_add(value.capacity()))
                .sum::<usize>(),
        )
        .saturating_add(
            node.children
                .values()
                .map(prefix_node_resident_bytes)
                .sum::<usize>(),
        )
}

fn estimate_router_bytes(snapshot: &PhysicalCatalogSnapshot) -> usize {
    snapshot.recipes.iter().fold(0, |bytes, recipe| {
        bytes
            .saturating_add(ENTRY_OVERHEAD_BYTES.saturating_mul(2))
            .saturating_add(
                recipe
                    .path_prefix
                    .len()
                    .saturating_mul(std::mem::size_of::<PrefixNode>()),
            )
            .saturating_add(recipe.content_type.as_ref().map_or(0, String::len))
            .saturating_add(selector_resident_bytes(&recipe.selectors))
    })
}

fn reclaim_retired_selectors(state: &mut HotState) {
    let mut retained = Vec::with_capacity(state.retired_selectors.len());
    for retired in std::mem::take(&mut state.retired_selectors) {
        if Arc::strong_count(&retired.pointers) == 1 {
            state.router_bytes = state.router_bytes.saturating_sub(retired.charge);
        } else {
            retained.push(retired);
        }
    }
    state.retired_selectors = retained;
}

fn disable_routes(state: &mut HotState) {
    let routes = std::mem::take(&mut state.compiled_routes);
    for (_, router) in routes {
        state.router_bytes = state.router_bytes.saturating_sub(router.charge);
        if Arc::strong_count(&router.pointers) > 1 {
            state.router_bytes = state.router_bytes.saturating_add(router.selector_charge);
            state.retired_selectors.push(RetiredSelectors {
                pointers: router.pointers,
                charge: router.selector_charge,
            });
        }
    }
}

fn emit_router_resident(state: &HotState, maximum_bytes: usize) {
    super::v1_telemetry::V1PipelineTelemetry::set(
        &super::v1_telemetry::global().stage_resident_bytes,
        total_bytes(state) as u64,
    );
    super::v1_telemetry::V1PipelineTelemetry::set(
        &super::v1_telemetry::global().stage_limit_bytes,
        maximum_bytes as u64,
    );
}

fn emit_rejected(reason: &'static str, payload_bytes: u64) {
    emit_replay_required(reason, payload_bytes);
}

fn emit_replay_required(reason: &'static str, payload_bytes: u64) {
    tracing::debug!(
        pipeline.reason = reason,
        pipeline.payload_bytes = payload_bytes,
        monotonic_counter.keldra_index_pipeline_replay_required_rows_total = 1_u64,
        monotonic_counter.keldra_index_pipeline_replay_required_bytes_total = payload_bytes,
        "committed object uses journal replay after hot-ingress rejection"
    );
}

#[cfg(test)]
mod tests {
    use keldra_store::{
        DeleteRequest, Durability, ObjectKey, Precondition, PutMode, PutRequest, VersionId,
    };

    use super::*;

    fn put(path: &str, bytes: usize) -> BatchOperation {
        let bytes =
            format!("{{\"value\":\"{}\"}}", "x".repeat(bytes.saturating_sub(12))).into_bytes();
        BatchOperation::Put(PutRequest {
            key: ObjectKey::new("tenant", "bucket", path).unwrap(),
            bytes,
            content_type: Some("application/json".into()),
            mode: PutMode::Put,
            command_id: Some(format!("put-{path}")),
            durability: Durability::Local,
        })
    }

    fn receipt(version: u64) -> MutationReceipt {
        MutationReceipt {
            command_id: Some(format!("v-{version}")),
            fingerprint: [version as u8; 32],
            version: VersionId(version),
            deleted: false,
            replayed: false,
            replay_guarantee_expires_at_unix_millis: 1,
        }
    }

    fn delete(path: &str) -> BatchOperation {
        BatchOperation::Delete(DeleteRequest {
            key: ObjectKey::new("tenant", "bucket", path).unwrap(),
            precondition: Precondition::Any,
            command_id: Some(format!("delete-{path}")),
            durability: Durability::Local,
        })
    }

    #[test]
    fn committed_exact_version_is_consumed_once() {
        let ingress = HotProjectionIngress::new(4_096).unwrap();
        ingress.activate_test_route(1, 2);
        let pending = ingress.pending(1, 2, &put("a", 100));
        ingress.admit_committed(pending, &receipt(3));
        assert!(ingress.take_exact_selected(1, 2, "a", 2).is_none());
        assert!(ingress.take_exact_selected(1, 2, "a", 3).is_some());
        assert!(ingress.take_exact_selected(1, 2, "a", 3).is_none());
        assert_eq!(ingress.used_bytes(), 0);
        assert_eq!(ingress.fifo_len(), 0);
    }

    #[test]
    fn in_flight_payloads_are_reserved_before_their_bytes_are_cloned() {
        let ingress = HotProjectionIngress::new(1_000).unwrap();
        ingress.activate_test_route(1, 2);
        let first = ingress.pending(1, 2, &put("a", 100)).unwrap();
        assert!(ingress.reserved_bytes() > 0);
        assert!(
            ingress
                .router_bytes()
                .saturating_add(ingress.reserved_bytes())
                <= 1_000
        );
        let replay_only = ingress.pending(1, 2, &put("b", 100)).unwrap();
        assert!(!replay_only.project);
        drop(first);
        assert_eq!(ingress.reserved_bytes(), 0);
        assert!(ingress.pending(1, 2, &put("b", 100)).unwrap().project);
    }

    #[test]
    fn full_budget_falls_back_without_blocking_ingestion() {
        let maximum_bytes = 4_096;
        let ingress = HotProjectionIngress::new(maximum_bytes).unwrap();
        ingress.activate_test_route(1, 2);
        for version in 1..=20 {
            let path = format!("item-{version}");
            let pending = ingress.pending(1, 2, &put(&path, 100));
            ingress.admit_committed(pending, &receipt(version));
        }
        assert!(ingress.take_exact_selected(1, 2, "item-1", 1).is_some());
        assert!(ingress.take_exact_selected(1, 2, "item-20", 20).is_none());
        let state = ingress
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(u64::try_from(total_bytes(&state)).unwrap() <= maximum_bytes);
    }

    #[test]
    fn replayed_receipt_is_not_admitted() {
        let ingress = HotProjectionIngress::new(4_096).unwrap();
        ingress.activate_test_route(1, 2);
        let pending = ingress.pending(1, 2, &put("a", 100));
        let mut replayed = receipt(3);
        replayed.replayed = true;
        ingress.admit_committed(pending, &replayed);
        assert!(ingress.take_exact_selected(1, 2, "a", 3).is_none());
    }

    #[tokio::test]
    async fn relevant_committed_delete_wakes_journal_reconciliation() {
        let ingress = HotProjectionIngress::new(4_096).unwrap();
        ingress.activate_test_route(1, 2);
        let mut changes = ingress.subscribe();
        let pending = ingress.pending(1, 2, &delete("a"));
        let mut deleted = receipt(3);
        deleted.deleted = true;
        ingress.admit_committed(pending, &deleted);
        changes.recv().await.unwrap();
        assert_eq!(ingress.reserved_bytes(), 0);
    }

    #[test]
    fn thousand_item_bulk_admission_stays_aligned_to_exact_committed_receipts() {
        let ingress = HotProjectionIngress::new(4 * 1024 * 1024).unwrap();
        ingress.activate_test_route(1, 2);
        let pending = (0..1_000)
            .map(|index| ingress.pending(1, 2, &put(&format!("objects/{index}"), 100)))
            .collect::<Vec<_>>();
        assert!(pending.iter().all(Option::is_some));

        for (index, pending) in pending.into_iter().enumerate() {
            let mut committed = receipt(10_000 + index as u64);
            // Replayed operations have no new journal mutation and must not be
            // duplicated by the hot path. A failed operation is represented by
            // dropping its reservation, exactly as the batch caller does when
            // no successful receipt is returned.
            if index == 111 {
                committed.replayed = true;
                ingress.admit_committed(pending, &committed);
            } else if index == 777 {
                drop(pending);
            } else {
                ingress.admit_committed(pending, &committed);
            }
        }

        for index in 0..1_000 {
            let payload = ingress.take_exact_selected(
                1,
                2,
                &format!("objects/{index}"),
                10_000 + index as u64,
            );
            assert_eq!(payload.is_some(), index != 111 && index != 777);
        }
        assert_eq!(ingress.used_bytes(), 0);
        assert_eq!(ingress.reserved_bytes(), 0);
        assert_eq!(ingress.fifo_len(), 0);
    }

    #[test]
    fn hot_ingress_swaps_only_the_compiled_physical_router() {
        let ingress = HotProjectionIngress::new(4_096).unwrap();
        ingress.activate_test_route(1, 2);
        let first = ingress.pending(1, 2, &put("objects/a", 100)).unwrap();
        let second = ingress.pending(1, 2, &put("objects/b", 100)).unwrap();
        assert!(Arc::ptr_eq(&first.pointers, &second.pointers));
        assert!(ingress.router_bytes() > 0);
    }

    #[test]
    fn compiled_router_obeys_segment_aware_public_prefix_semantics() {
        let mut router = BuildingBucketRouter {
            root: PrefixNode::default(),
            pointers: BTreeSet::new(),
        };
        router.insert("model", None);
        let router = CompiledBucketRouter {
            generation: [1; 32],
            root: router.root,
            pointers: Arc::from([]),
            charge: 0,
            selector_charge: 0,
        };
        assert!(router.matches("model", None));
        assert!(router.matches("model/weights", None));
        assert!(!router.matches("models/weights", None));

        let mut children = BuildingBucketRouter {
            root: PrefixNode::default(),
            pointers: BTreeSet::new(),
        };
        children.insert("model/", Some("application/json"));
        let children = CompiledBucketRouter {
            generation: [1; 32],
            root: children.root,
            pointers: Arc::from([]),
            charge: 0,
            selector_charge: 0,
        };
        assert!(!children.matches("model", Some("application/json")));
        assert!(children.matches("model/weights", Some("application/json")));
        assert!(!children.matches("model/weights", Some("text/plain")));
    }
}
