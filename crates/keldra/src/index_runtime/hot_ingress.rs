//! Bounded handoff of committed payloads to the memory-first indexing path.
//!
//! This is a disposable acceleration cache, never an ordering or durability
//! authority. Journal replay names the exact committed version before a
//! consumer may take bytes from here. Overflow evicts FIFO and therefore
//! degrades to an exact payload read without delaying object ingestion.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::sync::{Arc, Mutex};

use keldra_store::{BatchOperation, MutationReceipt, VersionId};

use super::catalog::PhysicalCatalogSnapshot;
use super::json_projection::{ProjectedScalarPointers, project_scalar_pointers};

const ENTRY_OVERHEAD_BYTES: usize = 256;

/// A deliberately conservative allowance for one independently allocated
/// ordered-map/set entry, including allocator metadata and unused node slots.
const TREE_ENTRY_OVERHEAD_BYTES: usize = 256;

#[derive(Clone)]
pub(crate) struct HotProjectionIngress {
    inner: Arc<Mutex<HotState>>,
    maximum_bytes: usize,
    cpu: Arc<std::sync::OnceLock<super::cpu::IndexCpuPool>>,
    changes: tokio::sync::broadcast::Sender<()>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct HotPathKey {
    tenant_id: u64,
    bucket_id: u64,
    path: Arc<str>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PendingTokenKey {
    path: HotPathKey,
    token: u64,
}

enum HotSlot {
    /// Raw committed bytes are reserved and queued for projection.
    Preparing {
        exact_version: VersionId,
        token: u64,
        router_generation: [u8; 32],
    },
    /// Exact selected fields can satisfy the journal-ordered consumer once.
    Ready {
        exact_version: VersionId,
        token: u64,
        router_generation: [u8; 32],
        selected: ProjectedScalarPointers,
        charge: usize,
    },
    /// This exact commit must use storage; retaining its version prevents an
    /// older queued callback from becoming the path's latest cache state.
    ReplayOnly {
        exact_version: VersionId,
        token: u64,
        router_generation: [u8; 32],
        charge: usize,
    },
}

impl HotSlot {
    const fn exact_version(&self) -> VersionId {
        match self {
            Self::Preparing { exact_version, .. }
            | Self::Ready { exact_version, .. }
            | Self::ReplayOnly { exact_version, .. } => *exact_version,
        }
    }

    const fn token(&self) -> u64 {
        match self {
            Self::Preparing { token, .. }
            | Self::Ready { token, .. }
            | Self::ReplayOnly { token, .. } => *token,
        }
    }

    const fn retained_charge(&self) -> usize {
        match self {
            Self::Preparing { .. } => 0,
            Self::Ready { charge, .. } | Self::ReplayOnly { charge, .. } => *charge,
        }
    }

    const fn router_generation(&self) -> [u8; 32] {
        match self {
            Self::Preparing {
                router_generation, ..
            }
            | Self::Ready {
                router_generation, ..
            }
            | Self::ReplayOnly {
                router_generation, ..
            } => *router_generation,
        }
    }
}

#[derive(Default)]
struct HotState {
    /// At most one committed version is retained for one logical object path.
    /// The source journal remains authoritative for both ordering and bytes.
    slots: BTreeMap<HotPathKey, HotSlot>,
    /// Exact token lookup prevents consumed or superseded path state from
    /// leaving FIFO tombstones. Ordered eviction remains logarithmic.
    fifo: BTreeMap<u64, HotPathKey>,
    /// A callback may publish only while its exact pre-commit token remains.
    /// Tokens are removed synchronously when a consumer or newer callback
    /// overtakes work whose committed version was not known at admission time.
    pending_tokens: BTreeMap<PendingTokenKey, [u8; 32]>,
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
    owned: Option<PendingOwned>,
    charge: usize,
    token: u64,
    state: Arc<Mutex<HotState>>,
    reservation_held: bool,
    project: bool,
    router_generation: [u8; 32],
    projection_limit: usize,
    maximum_bytes: usize,
}

struct PendingOwned {
    path: Arc<str>,
    bytes: Vec<u8>,
    pointers: Arc<[String]>,
}

struct RegisteredHotProjection {
    pending: PendingHotProjection,
    exact_version: VersionId,
    token: u64,
    router_generation: [u8; 32],
    cleanup_armed: bool,
}

impl Drop for PendingHotProjection {
    fn drop(&mut self) {
        if self.reservation_held {
            let key = self.key();
            let state_handle = self.state.clone();
            let mut state = state_handle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            cancel_exact_pending_token(&mut state, &key, self.token);
            drop(key);
            release_pending_reservation(&mut state, self);
            emit_hot_resident(&state, self.maximum_bytes);
        }
    }
}

impl PendingHotProjection {
    fn owned(&self) -> &PendingOwned {
        self.owned
            .as_ref()
            .expect("hot projection still owns its pending buffers")
    }

    fn path(&self) -> &str {
        &self.owned().path
    }

    fn bytes(&self) -> &[u8] {
        &self.owned().bytes
    }

    fn pointers(&self) -> &Arc<[String]> {
        &self.owned().pointers
    }

    fn key(&self) -> HotPathKey {
        HotPathKey {
            tenant_id: self.tenant_id,
            bucket_id: self.bucket_id,
            path: self.owned().path.clone(),
        }
    }
}

impl Drop for RegisteredHotProjection {
    fn drop(&mut self) {
        if !self.cleanup_armed {
            return;
        }
        let key = self.pending.key();
        let state_handle = self.pending.state.clone();
        let mut state = state_handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if preparing_matches(
            state.slots.get(&key),
            self.exact_version,
            self.token,
            self.router_generation,
        ) {
            remove_slot(&mut state, &key);
        }
        drop(key);
        release_pending_reservation(&mut state, &mut self.pending);
        emit_hot_resident(&state, self.pending.maximum_bytes);
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
            // A selector generation and every prepared selection derived from
            // it form one disposable view. Drop the view before sizing its
            // replacement; in-flight work remains charged by its reservation
            // and its token can no longer publish a stale completion.
            clear_incompatible_slots(&mut state, snapshot.identity);
            if total_bytes(&state).saturating_add(estimated) > self.maximum_bytes {
                disable_routes(&mut state);
                emit_hot_resident(&state, self.maximum_bytes);
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
            let charge = compiled_router_resident_bytes(&bucket.root, selector_charge);
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
        let compiled_charge = compiled
            .values()
            .fold(0_usize, |total, router| total.saturating_add(router.charge));
        debug_assert!(
            compiled_charge <= estimated,
            "physical-router sizing must conservatively reserve construction"
        );
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Admissions may have raced with router construction under the old
        // generation. Invalidate those tokens at the publication boundary.
        clear_incompatible_slots(&mut state, snapshot.identity);
        disable_routes(&mut state);
        reclaim_retired_selectors(&mut state);
        let additional_charge = compiled_charge.saturating_sub(estimated);
        if total_bytes(&state).saturating_add(additional_charge) > self.maximum_bytes {
            // Destroy the rejected tree while its construction reservation is
            // still held, then return that credit without exposing an
            // unaccounted resident tree to another admission.
            drop(compiled);
            state.reserved_bytes = state.reserved_bytes.saturating_sub(estimated);
            emit_hot_resident(&state, self.maximum_bytes);
            return false;
        }
        state.compiled_routes = compiled;
        state.router_bytes = state.router_bytes.saturating_add(compiled_charge);
        state.reserved_bytes = state.reserved_bytes.saturating_sub(estimated);
        emit_hot_resident(&state, self.maximum_bytes);
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
            if !router.matches_path(key.path()) {
                return None;
            }
            let project = match_content_type && router.matches(key.path(), content_type);
            Some((router.generation, router.pointers.clone(), project))
        };
        let (router_generation, pointers, project) = route?;
        if !project || pointers.is_empty() {
            return self.replay_only_pending(
                tenant_id,
                bucket_id,
                key.path(),
                router_generation,
                pointers,
            );
        }
        if bytes.is_none() {
            return self.replay_only_pending(
                tenant_id,
                bucket_id,
                key.path(),
                router_generation,
                pointers,
            );
        }
        let bytes = bytes.expect("put bytes were checked");
        tracing::debug!(
            pipeline.stage = "hot_payload",
            monotonic_counter.keldra_index_pipeline_hot_offered_rows_total = 1_u64,
            monotonic_counter.keldra_index_pipeline_hot_offered_bytes_total = bytes.len() as u64,
            "committed payload offered to the bounded indexing fast path"
        );
        // `project_scalar_pointers` charges construction to its selected-byte
        // limit, then remaps that result from synthetic names to JSON pointers.
        // Reserve both representations before cloning the source bytes. A hot
        // projection that expands beyond this source-sized speculative window
        // simply falls back to authoritative storage replay.
        let sizing = || {
            let projection_limit = ENTRY_OVERHEAD_BYTES
                .checked_add(bytes.len())?
                .checked_add(selector_resident_bytes(&pointers).checked_mul(4)?)?
                .checked_add(pointers.len().checked_mul(TREE_ENTRY_OVERHEAD_BYTES)?)?;
            let projection_workspace = projection_limit.checked_mul(2)?;
            let charge = pending_state_resident_bytes(key.path().len())
                .checked_add(bytes.len())?
                .checked_add(projection_workspace)?;
            Some((projection_limit, charge))
        };
        let Some((projection_limit, charge)) = sizing() else {
            emit_rejected("size_overflow", bytes.len() as u64);
            return self.replay_only_pending(
                tenant_id,
                bucket_id,
                key.path(),
                router_generation,
                pointers,
            );
        };
        if charge > self.maximum_bytes {
            emit_rejected("entry_too_large", bytes.len() as u64);
            return self.replay_only_pending(
                tenant_id,
                bucket_id,
                key.path(),
                router_generation,
                pointers,
            );
        }
        self.register_pending(
            tenant_id,
            bucket_id,
            key.path(),
            bytes,
            charge,
            true,
            router_generation,
            pointers.clone(),
            projection_limit,
        )
        .or_else(|| {
            self.replay_only_pending(
                tenant_id,
                bucket_id,
                key.path(),
                router_generation,
                pointers,
            )
        })
    }

    fn replay_only_pending(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
        router_generation: [u8; 32],
        pointers: Arc<[String]>,
    ) -> Option<PendingHotProjection> {
        self.register_pending(
            tenant_id,
            bucket_id,
            path,
            &[],
            pending_state_resident_bytes(path.len()),
            false,
            router_generation,
            pointers,
            0,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn register_pending(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
        bytes: &[u8],
        charge: usize,
        project: bool,
        router_generation: [u8; 32],
        pointers: Arc<[String]>,
        projection_limit: usize,
    ) -> Option<PendingHotProjection> {
        let (path, token) = {
            let mut state = self
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            reclaim_retired_selectors(&mut state);
            if current_router_generation_parts(&state, tenant_id, bucket_id)
                != Some(router_generation)
            {
                cancel_path_parts(&mut state, tenant_id, bucket_id, path);
                emit_hot_resident(&state, self.maximum_bytes);
                return None;
            }
            evict_retained_until_fits(&mut state, charge, self.maximum_bytes);
            if charge > self.maximum_bytes
                || total_bytes(&state).saturating_add(charge) > self.maximum_bytes
            {
                // This callback will have no token and therefore can never
                // publish. Cancel existing state now so it cannot outlive a
                // successful untracked mutation of this path.
                cancel_path_parts(&mut state, tenant_id, bucket_id, path);
                emit_hot_resident(&state, self.maximum_bytes);
                emit_rejected("inflight_budget_full", bytes.len() as u64);
                return None;
            }
            let Some(token) = allocate_token(&mut state) else {
                cancel_path_parts(&mut state, tenant_id, bucket_id, path);
                emit_hot_resident(&state, self.maximum_bytes);
                emit_rejected("sequence_exhausted", bytes.len() as u64);
                return None;
            };
            state.reserved_bytes = state
                .reserved_bytes
                .checked_add(charge)
                .expect("bounded hot reservation cannot overflow");
            // Allocate the shared path only after its credit is held. The token
            // map and returned handle share this one immutable allocation.
            let path = Arc::<str>::from(path);
            let hot_path = HotPathKey {
                tenant_id,
                bucket_id,
                path: path.clone(),
            };
            let replaced = state.pending_tokens.insert(
                PendingTokenKey {
                    path: hot_path,
                    token,
                },
                router_generation,
            );
            debug_assert!(replaced.is_none(), "hot tokens are globally unique");
            emit_hot_resident(&state, self.maximum_bytes);
            (path, token)
        };
        Some(PendingHotProjection {
            tenant_id,
            bucket_id,
            owned: Some(PendingOwned {
                path,
                bytes: bytes.to_vec(),
                pointers,
            }),
            charge,
            token,
            state: self.inner.clone(),
            reservation_held: true,
            project,
            router_generation,
            projection_limit,
            maximum_bytes: self.maximum_bytes,
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
        let payload_bytes = pending.bytes().len() as u64;
        if receipt.replayed {
            let _ = self.changes.send(());
            emit_replay_required("not_a_new_live_head", payload_bytes);
            return;
        }
        if receipt.deleted || !pending.project {
            let reason = if receipt.deleted {
                "not_a_live_head"
            } else {
                "hot_projection_unavailable"
            };
            self.install_replay_only(pending, receipt.version);
            // The committed version has synchronously superseded any older
            // path state before the journal consumer is woken.
            let _ = self.changes.send(());
            emit_replay_required(reason, payload_bytes);
            return;
        }
        let Some(registered) = self.register_preparing(pending, receipt.version) else {
            let _ = self.changes.send(());
            return;
        };
        let ingress = self.clone();
        #[cfg(not(test))]
        let cpu = ingress.cpu.get().cloned();
        #[cfg(not(test))]
        if cpu.is_none() {
            ingress.finish_replay_only(registered, "cpu_pool_unavailable", payload_bytes);
            let _ = ingress.changes.send(());
            return;
        }
        #[cfg(test)]
        {
            let selected = project_scalar_pointers(
                &mut Cursor::new(registered.pending.bytes()),
                registered.pending.pointers(),
                registered.pending.projection_limit,
            );
            match selected {
                Ok(Some(selected)) => ingress.finish_selected(registered, selected),
                Ok(None) => {
                    ingress.finish_replay_only(registered, "payload_not_selected", payload_bytes)
                }
                Err(_) => {
                    ingress.finish_replay_only(registered, "preparation_failed", payload_bytes)
                }
            }
            let _ = self.changes.send(());
            return;
        }
        #[cfg(not(test))]
        {
            let cpu = cpu.expect("hot projection CPU availability was checked");
            tokio::spawn(async move {
                let task_ingress = ingress.clone();
                let selected = cpu
                    .submit(move || {
                        if !task_ingress.preparation_is_current(&registered) {
                            return (registered, None);
                        }
                        let selected = project_scalar_pointers(
                            &mut Cursor::new(registered.pending.bytes()),
                            registered.pending.pointers(),
                            registered.pending.projection_limit,
                        );
                        (registered, Some(selected))
                    })
                    .await;
                match selected {
                    Ok((registered, None)) => ingress.discard_stale(registered),
                    Ok((registered, Some(Ok(Some(selected))))) => {
                        ingress.finish_selected(registered, selected)
                    }
                    Ok((registered, Some(Ok(None)))) => ingress.finish_replay_only(
                        registered,
                        "payload_not_selected",
                        payload_bytes,
                    ),
                    Ok((registered, Some(Err(error)))) => {
                        tracing::debug!(%error, "hot projection preparation fell back to journal replay");
                        ingress.finish_replay_only(registered, "preparation_failed", payload_bytes);
                    }
                    Err(error) => {
                        // The registered value is dropped by the CPU bridge on
                        // cancellation or panic, which atomically clears the
                        // matching Preparing token and its reservation.
                        tracing::debug!(%error, "hot projection CPU task fell back to journal replay");
                        emit_replay_required("cpu_failed", payload_bytes);
                    }
                }
            });
            // Preparing is visible and its CPU task is queued before a wake can
            // overtake it. A faster journal consumer still removes the exact
            // token and falls back to authoritative storage.
            let _ = self.changes.send(());
        }
    }

    fn register_preparing(
        &self,
        mut pending: PendingHotProjection,
        exact_version: VersionId,
    ) -> Option<RegisteredHotProjection> {
        let payload_bytes = pending.bytes().len() as u64;
        let key = pending.key();
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reclaim_retired_selectors(&mut state);
        let claimed =
            claim_pending_token(&mut state, &key, pending.token, pending.router_generation);
        // Commit order, not request-start order, is authoritative. Cancel every
        // sibling that was already pending; a later successful callback still
        // retires this slot even though its own publish token is gone.
        cancel_pending_tokens_for_path(&mut state, &key);
        let may_publish_version = supersede_older(&mut state, &key, exact_version);
        if !claimed {
            drop(key);
            release_pending_reservation(&mut state, &mut pending);
            emit_hot_resident(&state, self.maximum_bytes);
            drop(state);
            emit_replay_required("cancelled_before_commit_callback", payload_bytes);
            return None;
        }
        if !may_publish_version {
            drop(key);
            release_pending_reservation(&mut state, &mut pending);
            emit_hot_resident(&state, self.maximum_bytes);
            drop(state);
            emit_replay_required("stale_committed_version", payload_bytes);
            return None;
        }
        if current_router_generation(&state, &key) != Some(pending.router_generation) {
            drop(key);
            release_pending_reservation(&mut state, &mut pending);
            emit_hot_resident(&state, self.maximum_bytes);
            drop(state);
            emit_replay_required("catalog_changed", payload_bytes);
            return None;
        }
        let token = pending.token;
        state.slots.insert(
            key,
            HotSlot::Preparing {
                exact_version,
                token,
                router_generation: pending.router_generation,
            },
        );
        emit_hot_resident(&state, self.maximum_bytes);
        drop(state);
        Some(RegisteredHotProjection {
            router_generation: pending.router_generation,
            pending,
            exact_version,
            token,
            cleanup_armed: true,
        })
    }

    fn install_replay_only(&self, mut pending: PendingHotProjection, exact_version: VersionId) {
        let key = pending.key();
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reclaim_retired_selectors(&mut state);
        let claimed =
            claim_pending_token(&mut state, &key, pending.token, pending.router_generation);
        cancel_pending_tokens_for_path(&mut state, &key);
        let may_publish_version = supersede_older(&mut state, &key, exact_version);
        if !claimed || !may_publish_version {
            drop(key);
            release_pending_reservation(&mut state, &mut pending);
            emit_hot_resident(&state, self.maximum_bytes);
            return;
        }
        if current_router_generation(&state, &key) != Some(pending.router_generation) {
            drop(key);
            release_pending_reservation(&mut state, &mut pending);
            emit_hot_resident(&state, self.maximum_bytes);
            return;
        }
        let token = pending.token;
        let router_generation = pending.router_generation;
        // Destroy the raw pending buffers while their credit is still held,
        // then atomically transfer the path to bounded retained state.
        release_pending_reservation(&mut state, &mut pending);
        let inserted = insert_replay_only(
            &mut state,
            key,
            exact_version,
            token,
            router_generation,
            self.maximum_bytes,
        );
        debug_assert!(
            inserted,
            "a charged pending token reserves its replay barrier replacement"
        );
        emit_hot_resident(&state, self.maximum_bytes);
    }

    fn preparation_is_current(&self, registered: &RegisteredHotProjection) -> bool {
        let key = registered.pending.key();
        let state = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        preparing_matches(
            state.slots.get(&key),
            registered.exact_version,
            registered.token,
            registered.router_generation,
        )
    }

    fn discard_stale(&self, mut registered: RegisteredHotProjection) {
        let key = registered.pending.key();
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if preparing_matches(
            state.slots.get(&key),
            registered.exact_version,
            registered.token,
            registered.router_generation,
        ) {
            remove_slot(&mut state, &key);
        }
        drop(key);
        release_pending_reservation(&mut state, &mut registered.pending);
        registered.cleanup_armed = false;
        record_stale_preparation();
        emit_hot_resident(&state, self.maximum_bytes);
    }

    fn finish_replay_only(
        &self,
        mut registered: RegisteredHotProjection,
        reason: &'static str,
        payload_bytes: u64,
    ) {
        let key = registered.pending.key();
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reclaim_retired_selectors(&mut state);
        if !preparing_matches(
            state.slots.get(&key),
            registered.exact_version,
            registered.token,
            registered.router_generation,
        ) {
            registered.cleanup_armed = false;
            drop(key);
            release_pending_reservation(&mut state, &mut registered.pending);
            record_stale_preparation();
            emit_hot_resident(&state, self.maximum_bytes);
            drop(state);
            emit_replay_required("superseded_preparation", payload_bytes);
            return;
        }
        if current_router_generation(&state, &key) == Some(registered.router_generation) {
            let insert_key = key.clone();
            registered.cleanup_armed = false;
            release_pending_reservation(&mut state, &mut registered.pending);
            if !insert_replay_only(
                &mut state,
                insert_key,
                registered.exact_version,
                registered.token,
                registered.router_generation,
                self.maximum_bytes,
            ) {
                remove_slot(&mut state, &key);
            }
        } else {
            remove_slot(&mut state, &key);
            registered.cleanup_armed = false;
            drop(key);
            release_pending_reservation(&mut state, &mut registered.pending);
            emit_hot_resident(&state, self.maximum_bytes);
            drop(state);
            emit_replay_required(reason, payload_bytes);
            return;
        }
        drop(key);
        emit_hot_resident(&state, self.maximum_bytes);
        drop(state);
        emit_replay_required(reason, payload_bytes);
    }

    fn finish_selected(
        &self,
        mut registered: RegisteredHotProjection,
        selected: ProjectedScalarPointers,
    ) {
        let selected_bytes = match selected.resident_bytes() {
            Ok(bytes) => bytes,
            Err(_) => {
                let payload_bytes = registered.pending.bytes().len() as u64;
                drop(selected);
                self.finish_replay_only(registered, "prepared_size_unavailable", payload_bytes);
                return;
            }
        };
        let charge = ready_slot_resident_bytes(
            registered.pending.path().len(),
            registered.pending.pointers().len(),
            selected_bytes,
        );
        let Some(charge) = charge.filter(|charge| *charge <= self.maximum_bytes) else {
            drop(selected);
            self.finish_replay_only(
                registered,
                "prepared_entry_too_large",
                selected_bytes as u64,
            );
            return;
        };
        let key = registered.pending.key();
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reclaim_retired_selectors(&mut state);
        if !preparing_matches(
            state.slots.get(&key),
            registered.exact_version,
            registered.token,
            registered.router_generation,
        ) {
            registered.cleanup_armed = false;
            drop(key);
            drop(selected);
            release_pending_reservation(&mut state, &mut registered.pending);
            record_stale_preparation();
            emit_hot_resident(&state, self.maximum_bytes);
            return;
        }
        if current_router_generation(&state, &key) != Some(registered.router_generation) {
            remove_slot(&mut state, &key);
            registered.cleanup_armed = false;
            drop(key);
            drop(selected);
            release_pending_reservation(&mut state, &mut registered.pending);
            emit_hot_resident(&state, self.maximum_bytes);
            drop(state);
            emit_replay_required("catalog_changed", selected_bytes as u64);
            return;
        }
        // No other admission can observe the reserved-to-retained transition.
        // Only the positive difference needs additional room because the
        // selected allocation is already covered by this pending reservation.
        registered.cleanup_armed = false;
        let additional_charge = charge.saturating_sub(registered.pending.charge);
        evict_retained_until_fits(&mut state, additional_charge, self.maximum_bytes);
        if total_bytes(&state).saturating_add(additional_charge) > self.maximum_bytes {
            drop(selected);
            release_pending_reservation(&mut state, &mut registered.pending);
            if !insert_replay_only(
                &mut state,
                key.clone(),
                registered.exact_version,
                registered.token,
                registered.router_generation,
                self.maximum_bytes,
            ) {
                remove_slot(&mut state, &key);
            }
            drop(key);
            emit_hot_resident(&state, self.maximum_bytes);
            drop(state);
            emit_replay_required("prepared_budget_full", selected_bytes as u64);
            return;
        };
        state.used_bytes = state.used_bytes.saturating_add(charge);
        state.fifo.insert(registered.token, key.clone());
        state.slots.insert(
            key,
            HotSlot::Ready {
                exact_version: registered.exact_version,
                token: registered.token,
                router_generation: registered.router_generation,
                selected,
                charge,
            },
        );
        // The selected value is now retained and charged. Destroy the raw
        // pending buffers before returning their reservation to the budget.
        release_pending_reservation(&mut state, &mut registered.pending);
        super::v1_telemetry::V1PipelineTelemetry::add(
            &super::v1_telemetry::global().hot_admissions,
            1,
        );
        emit_hot_resident(&state, self.maximum_bytes);
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

    pub(crate) fn take_exact_selected_for_generation(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
        version: u64,
        router_generation: [u8; 32],
    ) -> Option<ProjectedScalarPointers> {
        let key = HotPathKey {
            tenant_id,
            bucket_id,
            path: Arc::from(path),
        };
        let exact_version = VersionId(version);
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reclaim_retired_selectors(&mut state);
        let cancelled_pending = cancel_pending_tokens_for_path(&mut state, &key);
        let Some(slot_version) = state.slots.get(&key).map(HotSlot::exact_version) else {
            if cancelled_pending != 0 {
                emit_hot_resident(&state, self.maximum_bytes);
            }
            return None;
        };
        if slot_version > exact_version {
            if cancelled_pending != 0 {
                emit_hot_resident(&state, self.maximum_bytes);
            }
            return None;
        }
        if slot_version == exact_version
            && state
                .slots
                .get(&key)
                .is_some_and(|slot| slot.router_generation() != router_generation)
        {
            if cancelled_pending != 0 {
                emit_hot_resident(&state, self.maximum_bytes);
            }
            return None;
        }
        let slot = remove_slot(&mut state, &key).expect("observed hot slot remained locked");
        emit_hot_resident(&state, self.maximum_bytes);
        if slot_version < exact_version {
            return None;
        }
        let HotSlot::Ready {
            selected, charge, ..
        } = slot
        else {
            return None;
        };
        tracing::debug!(
            pipeline.stage = "hot_prepared",
            gauge.keldra_index_pipeline_stage_resident_bytes = state.used_bytes as u64,
            gauge.keldra_index_pipeline_stage_limit_bytes = self.maximum_bytes as u64,
            monotonic_counter.keldra_index_pipeline_hot_payload_rows_total = 1_u64,
            monotonic_counter.keldra_index_pipeline_hot_payload_bytes_total = charge as u64,
            "journal-ordered TypedJson projection consumed hot selection"
        );
        Some(selected)
    }

    #[cfg(test)]
    pub(crate) fn take_exact_selected(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
        version: u64,
    ) -> Option<ProjectedScalarPointers> {
        let router_generation = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .compiled_routes
            .get(&(tenant_id, bucket_id))
            .map(|router| router.generation)?;
        self.take_exact_selected_for_generation(
            tenant_id,
            bucket_id,
            path,
            version,
            router_generation,
        )
    }

    pub(crate) fn discard_through(&self, tenant_id: u64, bucket_id: u64, path: &str, version: u64) {
        let key = HotPathKey {
            tenant_id,
            bucket_id,
            path: Arc::from(path),
        };
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let cancelled_pending = cancel_pending_tokens_for_path(&mut state, &key);
        let removed = if state
            .slots
            .get(&key)
            .is_some_and(|slot| slot.exact_version() <= VersionId(version))
        {
            remove_slot(&mut state, &key);
            true
        } else {
            false
        };
        if removed || cancelled_pending != 0 {
            emit_hot_resident(&state, self.maximum_bytes);
        }
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
    fn slot_len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .slots
            .len()
    }

    #[cfg(test)]
    fn pending_token_len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending_tokens
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
        let charge = compiled_router_resident_bytes(&router.root, selector_charge);
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

fn release_pending_reservation(state: &mut HotState, pending: &mut PendingHotProjection) {
    if pending.reservation_held {
        // The reservation covers the source buffer, selector handle, path, and
        // projection workspace. Destroy those owned values before making their
        // credit visible to another admission.
        drop(pending.owned.take());
        state.reserved_bytes = state.reserved_bytes.saturating_sub(pending.charge);
        pending.reservation_held = false;
    }
}

fn current_router_generation(state: &HotState, key: &HotPathKey) -> Option<[u8; 32]> {
    current_router_generation_parts(state, key.tenant_id, key.bucket_id)
}

fn current_router_generation_parts(
    state: &HotState,
    tenant_id: u64,
    bucket_id: u64,
) -> Option<[u8; 32]> {
    state
        .compiled_routes
        .get(&(tenant_id, bucket_id))
        .map(|router| router.generation)
}

fn claim_pending_token(
    state: &mut HotState,
    key: &HotPathKey,
    token: u64,
    router_generation: [u8; 32],
) -> bool {
    state
        .pending_tokens
        .remove(&PendingTokenKey {
            path: key.clone(),
            token,
        })
        .is_some_and(|generation| generation == router_generation)
}

fn cancel_exact_pending_token(state: &mut HotState, key: &HotPathKey, token: u64) {
    state.pending_tokens.remove(&PendingTokenKey {
        path: key.clone(),
        token,
    });
}

fn cancel_pending_tokens_for_path(state: &mut HotState, key: &HotPathKey) -> usize {
    let before = state.pending_tokens.len();
    state
        .pending_tokens
        .retain(|pending, _| pending.path != *key);
    before.saturating_sub(state.pending_tokens.len())
}

fn cancel_path_parts(state: &mut HotState, tenant_id: u64, bucket_id: u64, path: &str) {
    state.pending_tokens.retain(|pending, _| {
        pending.path.tenant_id != tenant_id
            || pending.path.bucket_id != bucket_id
            || pending.path.path.as_ref() != path
    });
    let slot = state
        .slots
        .keys()
        .find(|key| {
            key.tenant_id == tenant_id && key.bucket_id == bucket_id && key.path.as_ref() == path
        })
        .cloned();
    if let Some(key) = slot {
        remove_slot(state, &key);
    }
}

fn allocate_token(state: &mut HotState) -> Option<u64> {
    let token = state.next_sequence.checked_add(1)?;
    state.next_sequence = token;
    Some(token)
}

fn preparing_matches(
    slot: Option<&HotSlot>,
    exact_version: VersionId,
    token: u64,
    router_generation: [u8; 32],
) -> bool {
    matches!(
        slot,
        Some(HotSlot::Preparing {
            exact_version: current_version,
            token: current_token,
            router_generation: current_generation,
        }) if *current_version == exact_version
            && *current_token == token
            && *current_generation == router_generation
    )
}

/// Remove a strictly older version before publishing the path's next state.
/// Equal or newer versions win when commit callbacks arrive out of order.
fn supersede_older(state: &mut HotState, key: &HotPathKey, exact_version: VersionId) -> bool {
    match state.slots.get(key).map(HotSlot::exact_version) {
        Some(current) if current >= exact_version => false,
        Some(_) => {
            remove_slot(state, key);
            super::v1_telemetry::V1PipelineTelemetry::add(
                &super::v1_telemetry::global().hot_superseded,
                1,
            );
            tracing::debug!(
                pipeline.stage = "hot_prepared",
                monotonic_counter.keldra_index_pipeline_hot_superseded_rows_total = 1_u64,
                "newer committed object superseded hot-ingress path state"
            );
            true
        }
        None => true,
    }
}

fn remove_slot(state: &mut HotState, key: &HotPathKey) -> Option<HotSlot> {
    let slot = state.slots.remove(key)?;
    state.fifo.remove(&slot.token());
    state.used_bytes = state.used_bytes.saturating_sub(slot.retained_charge());
    Some(slot)
}

fn clear_incompatible_slots(state: &mut HotState, router_generation: [u8; 32]) {
    state
        .pending_tokens
        .retain(|_, generation| *generation == router_generation);
    let incompatible = state
        .slots
        .iter()
        .filter(|(_, slot)| slot.router_generation() != router_generation)
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    for key in incompatible {
        remove_slot(state, &key);
    }
}

fn insert_replay_only(
    state: &mut HotState,
    key: HotPathKey,
    exact_version: VersionId,
    token: u64,
    router_generation: [u8; 32],
    maximum_bytes: usize,
) -> bool {
    let Some(charge) = replay_slot_resident_bytes(key.path.len()) else {
        return false;
    };
    if charge > maximum_bytes {
        return false;
    }
    evict_retained_until_fits(state, charge, maximum_bytes);
    if total_bytes(state).saturating_add(charge) > maximum_bytes {
        return false;
    }
    remove_slot(state, &key);
    state.used_bytes = state.used_bytes.saturating_add(charge);
    state.fifo.insert(token, key.clone());
    state.slots.insert(
        key,
        HotSlot::ReplayOnly {
            exact_version,
            token,
            router_generation,
            charge,
        },
    );
    true
}

fn evict_retained_until_fits(state: &mut HotState, needed: usize, maximum_bytes: usize) {
    while total_bytes(state).saturating_add(needed) > maximum_bytes {
        let Some((token, key)) = state.fifo.pop_first() else {
            break;
        };
        let is_current = state
            .slots
            .get(&key)
            .is_some_and(|slot| slot.token() == token && slot.retained_charge() != 0);
        if !is_current {
            continue;
        }
        let Some(slot) = state.slots.remove(&key) else {
            continue;
        };
        let charge = slot.retained_charge();
        state.used_bytes = state.used_bytes.saturating_sub(charge);
        super::v1_telemetry::V1PipelineTelemetry::add(
            &super::v1_telemetry::global().hot_evictions,
            1,
        );
        tracing::debug!(
            pipeline.stage = "hot_prepared",
            monotonic_counter.keldra_index_pipeline_replay_required_rows_total = 1_u64,
            monotonic_counter.keldra_index_pipeline_replay_required_bytes_total = charge as u64,
            "TypedJson hot-ingress state evicted for bounded memory"
        );
    }
}

fn record_stale_preparation() {
    super::v1_telemetry::V1PipelineTelemetry::add(
        &super::v1_telemetry::global().hot_stale_preparations,
        1,
    );
    tracing::debug!(
        pipeline.stage = "hot_prepared",
        monotonic_counter.keldra_index_pipeline_hot_stale_preparations_total = 1_u64,
        "superseded hot-ingress preparation was discarded"
    );
}

fn path_allocation_bytes(path_bytes: usize) -> usize {
    path_bytes.saturating_add(2 * std::mem::size_of::<usize>())
}

fn retained_slot_state_resident_bytes(path_bytes: usize) -> Option<usize> {
    path_allocation_bytes(path_bytes)
        .checked_add(TREE_ENTRY_OVERHEAD_BYTES.checked_mul(2)?)?
        .checked_add(std::mem::size_of::<HotPathKey>())?
        .checked_add(std::mem::size_of::<HotSlot>())?
        .checked_add(std::mem::size_of::<u64>())?
        .checked_add(std::mem::size_of::<HotPathKey>())
}

fn pending_state_resident_bytes(path_bytes: usize) -> usize {
    let token_entry = TREE_ENTRY_OVERHEAD_BYTES
        .saturating_add(std::mem::size_of::<PendingTokenKey>())
        .saturating_add(32);
    let retained_entries = retained_slot_state_resident_bytes(0).unwrap_or(usize::MAX);
    path_allocation_bytes(path_bytes)
        .saturating_add(token_entry.max(retained_entries))
        .saturating_add(std::mem::size_of::<PendingHotProjection>())
}

fn replay_slot_resident_bytes(path_bytes: usize) -> Option<usize> {
    retained_slot_state_resident_bytes(path_bytes)
}

fn ready_slot_resident_bytes(
    path_bytes: usize,
    selected_fields: usize,
    selected_bytes: usize,
) -> Option<usize> {
    retained_slot_state_resident_bytes(path_bytes)?
        .checked_add(selected_bytes)?
        .checked_add(selected_fields.checked_mul(TREE_ENTRY_OVERHEAD_BYTES)?)
}

fn total_bytes(state: &HotState) -> usize {
    state
        .used_bytes
        .saturating_add(state.reserved_bytes)
        .saturating_add(state.router_bytes)
}

fn selector_resident_bytes(pointers: &[String]) -> usize {
    (2 * std::mem::size_of::<usize>())
        .saturating_add(std::mem::size_of_val(pointers))
        .saturating_add(pointers.iter().map(String::capacity).sum::<usize>())
}

fn prefix_node_resident_bytes(node: &PrefixNode) -> usize {
    std::mem::size_of::<PrefixNode>()
        .saturating_add(
            node.raw_prefix
                .exact
                .iter()
                .chain(node.segment_boundary.exact.iter())
                .map(|value| {
                    TREE_ENTRY_OVERHEAD_BYTES
                        .saturating_add(std::mem::size_of::<String>())
                        .saturating_add(value.capacity())
                })
                .sum::<usize>(),
        )
        .saturating_add(
            node.children
                .len()
                .saturating_mul(TREE_ENTRY_OVERHEAD_BYTES + std::mem::size_of::<u8>()),
        )
        .saturating_add(
            node.children
                .values()
                .map(prefix_node_resident_bytes)
                .sum::<usize>(),
        )
}

fn compiled_router_resident_bytes(root: &PrefixNode, selector_bytes: usize) -> usize {
    prefix_node_resident_bytes(root)
        .saturating_add(selector_bytes)
        .saturating_add(TREE_ENTRY_OVERHEAD_BYTES)
        .saturating_add(std::mem::size_of::<(u64, u64)>())
        .saturating_add(
            std::mem::size_of::<CompiledBucketRouter>()
                .saturating_sub(std::mem::size_of::<PrefixNode>()),
        )
}

fn estimate_router_bytes(snapshot: &PhysicalCatalogSnapshot) -> usize {
    snapshot.recipes.iter().fold(0, |bytes, recipe| {
        let path_nodes = recipe.path_prefix.len().saturating_add(1).saturating_mul(
            std::mem::size_of::<PrefixNode>()
                .saturating_add(TREE_ENTRY_OVERHEAD_BYTES)
                .saturating_add(std::mem::size_of::<u8>()),
        );
        let content_type = recipe.content_type.as_ref().map_or(0, |content_type| {
            TREE_ENTRY_OVERHEAD_BYTES
                .saturating_add(std::mem::size_of::<String>())
                .saturating_add(content_type.capacity())
        });
        let selector_tree = recipe
            .selectors
            .len()
            .saturating_mul(TREE_ENTRY_OVERHEAD_BYTES);
        bytes
            .saturating_add(TREE_ENTRY_OVERHEAD_BYTES.saturating_mul(2))
            .saturating_add(path_nodes)
            .saturating_add(content_type)
            .saturating_add(selector_resident_bytes(&recipe.selectors))
            .saturating_add(selector_tree)
    })
}

fn reclaim_retired_selectors(state: &mut HotState) {
    let mut reclaimed = 0_usize;
    state.retired_selectors.retain(|retired| {
        if Arc::strong_count(&retired.pointers) == 1 {
            reclaimed = reclaimed.saturating_add(retired.charge);
            false
        } else {
            true
        }
    });
    state.router_bytes = state.router_bytes.saturating_sub(reclaimed);
    // `retain` compacts in place. Release any now-unused backing allocation so
    // removed entries do not leave resident but uncharged vector capacity.
    state.retired_selectors.shrink_to_fit();
}

fn disable_routes(state: &mut HotState) {
    let routes = std::mem::take(&mut state.compiled_routes);
    for (_, router) in routes {
        state.router_bytes = state.router_bytes.saturating_sub(router.charge);
        if Arc::strong_count(&router.pointers) > 1 {
            let charge = router.selector_charge.saturating_add(
                ENTRY_OVERHEAD_BYTES.saturating_add(std::mem::size_of::<RetiredSelectors>()),
            );
            state.router_bytes = state.router_bytes.saturating_add(charge);
            state.retired_selectors.push(RetiredSelectors {
                pointers: router.pointers,
                charge,
            });
        }
    }
}

fn emit_hot_resident(state: &HotState, maximum_bytes: usize) {
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
#[path = "hot_ingress/tests.rs"]
mod tests;
