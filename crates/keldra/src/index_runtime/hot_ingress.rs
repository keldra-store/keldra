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

use super::json_projection::{ProjectedScalarPointers, project_scalar_pointers};

const ENTRY_OVERHEAD_BYTES: usize = 256;

#[derive(Clone)]
pub(crate) struct HotProjectionIngress {
    inner: Arc<Mutex<HotState>>,
    maximum_bytes: usize,
    cpu: Arc<std::sync::OnceLock<super::cpu::IndexCpuPool>>,
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
    next_sequence: u64,
    router_generations: BTreeMap<(u64, u64), [u8; 32]>,
    compiled_routes: BTreeMap<(u64, u64), CompiledBucketRouter>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct HotRoute {
    path_prefix: String,
    content_type: Option<String>,
}

#[derive(Clone)]
pub(crate) struct CompiledHotRoute {
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) path_prefix: String,
    pub(crate) content_type: Option<String>,
    pub(crate) pointers: Vec<String>,
}

#[derive(Clone, Default)]
struct CompiledBucketRouter {
    root: PrefixNode,
    pointers: BTreeSet<String>,
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

impl CompiledBucketRouter {
    fn insert(&mut self, route: &HotRoute) {
        let mut node = &mut self.root;
        for byte in route.path_prefix.bytes() {
            node = node.children.entry(byte).or_default();
        }
        let terminal = if route.path_prefix.is_empty() || route.path_prefix.ends_with('/') {
            &mut node.raw_prefix
        } else {
            &mut node.segment_boundary
        };
        terminal.insert(route.content_type.as_deref());
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
    router_generation: [u8; 32],
    pointers: Vec<String>,
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
        })
    }

    pub(crate) fn install_cpu(&self, cpu: super::cpu::IndexCpuPool) -> Result<(), &'static str> {
        self.cpu
            .set(cpu)
            .map_err(|_| "TypedJson hot indexing CPU pool was installed more than once")
    }

    pub(crate) fn replace_compiled_catalog(
        &self,
        generation: [u8; 32],
        routes: impl IntoIterator<Item = CompiledHotRoute>,
    ) {
        let mut compiled = BTreeMap::<(u64, u64), CompiledBucketRouter>::new();
        for route in routes {
            let bucket = compiled
                .entry((route.tenant_id, route.bucket_id))
                .or_default();
            bucket.insert(&HotRoute {
                path_prefix: route.path_prefix,
                content_type: route.content_type,
            });
            bucket.pointers.extend(route.pointers);
        }
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.compiled_routes = compiled;
        state.router_generations.clear();
        let buckets = state.compiled_routes.keys().copied().collect::<Vec<_>>();
        for bucket in buckets {
            state.router_generations.insert(bucket, generation);
        }
    }

    pub(crate) fn pending(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        operation: &BatchOperation,
    ) -> Option<PendingHotProjection> {
        let BatchOperation::Put(request) = operation else {
            return None;
        };
        if request
            .key
            .path()
            .split('/')
            .any(|segment| segment == "_keldra")
        {
            return None;
        }
        let route = {
            let state = self
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let interested = state
                .compiled_routes
                .get(&(tenant_id, bucket_id))
                .is_some_and(|router| {
                    router.matches(request.key.path(), request.content_type.as_deref())
                });
            interested.then(|| {
                (
                    state.router_generations[&(tenant_id, bucket_id)],
                    state.compiled_routes[&(tenant_id, bucket_id)]
                        .pointers
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>(),
                )
            })
        };
        let (router_generation, pointers) = route?;
        if pointers.is_empty() {
            return None;
        }
        tracing::debug!(
            pipeline.stage = "hot_payload",
            monotonic_counter.keldra_index_pipeline_hot_offered_rows_total = 1_u64,
            monotonic_counter.keldra_index_pipeline_hot_offered_bytes_total =
                request.bytes.len() as u64,
            "committed payload offered to the bounded indexing fast path"
        );
        // The map key and FIFO key each own the path. Account both retained
        // copies before cloning any payload bytes.
        let charge = ENTRY_OVERHEAD_BYTES
            .checked_add(request.key.path().len().checked_mul(2)?)?
            .checked_add(request.bytes.len())?;
        if charge > self.maximum_bytes {
            emit_rejected("entry_too_large", request.bytes.len() as u64);
            return None;
        }
        {
            let mut state = self
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let available = self
                .maximum_bytes
                .saturating_sub(state.used_bytes)
                .saturating_sub(state.reserved_bytes);
            if charge > available {
                drop(state);
                emit_rejected("inflight_budget_full", request.bytes.len() as u64);
                return None;
            }
            state.reserved_bytes = match state.reserved_bytes.checked_add(charge) {
                Some(bytes) => bytes,
                None => {
                    drop(state);
                    emit_rejected("size_overflow", request.bytes.len() as u64);
                    return None;
                }
            };
        }
        Some(PendingHotProjection {
            tenant_id,
            bucket_id,
            path: request.key.path().to_owned(),
            bytes: request.bytes.clone(),
            charge,
            state: self.inner.clone(),
            reservation_held: true,
            router_generation,
            pointers,
        })
    }

    pub(crate) fn admit_committed(
        &self,
        pending: Option<PendingHotProjection>,
        receipt: &MutationReceipt,
    ) {
        let Some(pending) = pending else {
            return;
        };
        if receipt.replayed || receipt.deleted {
            emit_replay_required("not_a_new_live_head", pending.bytes.len() as u64);
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
        state.reserved_bytes = state.reserved_bytes.saturating_sub(pending.charge);
        pending.reservation_held = false;
        if state
            .router_generations
            .get(&(key.tenant_id, key.bucket_id))
            .copied()
            != Some(pending.router_generation)
        {
            emit_replay_required("catalog_changed", selected_bytes as u64);
            return;
        }
        if let Some(previous) = state.payloads.remove(&key) {
            state.used_bytes = state.used_bytes.saturating_sub(previous.charge);
            state.fifo.remove(&previous.sequence);
        }
        while state.used_bytes.saturating_add(charge) > self.maximum_bytes {
            let Some((sequence, evicted)) = state.fifo.pop_first() else {
                break;
            };
            if state
                .payloads
                .get(&evicted)
                .is_some_and(|payload| payload.sequence == sequence)
                && let Some(payload) = state.payloads.remove(&evicted)
            {
                super::v6_telemetry::V6PipelineTelemetry::add(
                    &super::v6_telemetry::global().hot_evictions,
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
        let Some(next_sequence) = state.next_sequence.checked_add(1) else {
            drop(state);
            emit_replay_required("sequence_exhausted", selected_bytes as u64);
            return;
        };
        state.next_sequence = next_sequence;
        let sequence = state.next_sequence;
        state.used_bytes = state.used_bytes.saturating_add(charge);
        super::v6_telemetry::V6PipelineTelemetry::set(
            &super::v6_telemetry::global().stage_resident_bytes,
            state.used_bytes.saturating_add(state.reserved_bytes) as u64,
        );
        super::v6_telemetry::V6PipelineTelemetry::set(
            &super::v6_telemetry::global().stage_limit_bytes,
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
        let Some(payload) = state.payloads.remove(&key) else {
            return None;
        };
        let current_generation = state
            .router_generations
            .get(&(tenant_id, bucket_id))
            .copied();
        let router_compatible = current_generation == Some(payload.router_generation);
        state.used_bytes = state.used_bytes.saturating_sub(payload.charge);
        super::v6_telemetry::V6PipelineTelemetry::set(
            &super::v6_telemetry::global().stage_resident_bytes,
            state.used_bytes.saturating_add(state.reserved_bytes) as u64,
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
        let mut router = CompiledBucketRouter::default();
        router.insert(&HotRoute {
            path_prefix: String::new(),
            content_type: Some("application/json".into()),
        });
        router.pointers.insert("/value".into());
        state.compiled_routes.insert((tenant_id, bucket_id), router);
        state
            .router_generations
            .insert((tenant_id, bucket_id), [9; 32]);
    }
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
    use keldra_store::{Durability, ObjectKey, PutMode, PutRequest, VersionId};

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
        let ingress = HotProjectionIngress::new(700).unwrap();
        ingress.activate_test_route(1, 2);
        let first = ingress.pending(1, 2, &put("a", 100)).unwrap();
        assert!(ingress.reserved_bytes() > 0);
        assert!(ingress.pending(1, 2, &put("b", 100)).is_none());
        drop(first);
        assert_eq!(ingress.reserved_bytes(), 0);
        assert!(ingress.pending(1, 2, &put("b", 100)).is_some());
    }

    #[test]
    fn overflow_evicts_fifo_and_never_blocks_ingestion() {
        let ingress = HotProjectionIngress::new(800).unwrap();
        ingress.activate_test_route(1, 2);
        for (path, version) in [("a", 1), ("b", 2), ("c", 3)] {
            let pending = ingress.pending(1, 2, &put(path, 100));
            ingress.admit_committed(pending, &receipt(version));
        }
        assert!(ingress.take_exact_selected(1, 2, "a", 1).is_none());
        assert!(ingress.take_exact_selected(1, 2, "c", 3).is_some());
        assert!(ingress.used_bytes() <= 800);
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
        ingress.replace_compiled_catalog(
            [7; 32],
            [CompiledHotRoute {
                tenant_id: 1,
                bucket_id: 2,
                path_prefix: "objects/".into(),
                content_type: Some("application/json".into()),
                pointers: vec!["/value".into()],
            }],
        );
        let state = ingress
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(state.compiled_routes.len(), 1);
        assert_eq!(state.router_generations[&(1, 2)], [7; 32]);
    }

    #[test]
    fn compiled_router_obeys_segment_aware_public_prefix_semantics() {
        let mut router = CompiledBucketRouter::default();
        router.insert(&HotRoute {
            path_prefix: "model".into(),
            content_type: None,
        });
        assert!(router.matches("model", None));
        assert!(router.matches("model/weights", None));
        assert!(!router.matches("models/weights", None));

        let mut children = CompiledBucketRouter::default();
        children.insert(&HotRoute {
            path_prefix: "model/".into(),
            content_type: Some("application/json".into()),
        });
        assert!(!children.matches("model", Some("application/json")));
        assert!(children.matches("model/weights", Some("application/json")));
        assert!(!children.matches("model/weights", Some("text/plain")));
    }
}
