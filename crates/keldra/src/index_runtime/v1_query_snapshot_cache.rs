//! Bounded reuse of immutable, fully validated v1 query snapshots.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use keldra_index::IndexError;
use keldra_index::v1::{
    LogicalProjectionBinding, PinnedPartitionQueryRoot, QueryRecipeCatalogProof,
    QuerySnapshotIdentity, ValidatedQuerySnapshot, query_snapshot_identity,
};

use super::cursor::QueryPositionRoot;
use super::{PinnedRootVector, QUERY_SNAPSHOT_CACHE_BYTES};

pub(super) struct CachedRuntimeQuerySnapshot {
    pub(super) pinned: Arc<PinnedRootVector>,
    pub(super) snapshot: Arc<ValidatedQuerySnapshot>,
    resident_bytes: usize,
    stamp: AtomicU64,
    protected: AtomicBool,
}

struct QuerySnapshotCacheState {
    resident_bytes: usize,
    clock: u64,
    entry_count: usize,
    entries: HashMap<QuerySnapshotIdentity, Vec<Arc<CachedRuntimeQuerySnapshot>>>,
    cold_recency: VecDeque<(u64, Weak<CachedRuntimeQuerySnapshot>)>,
    protected_recency: VecDeque<(u64, Weak<CachedRuntimeQuerySnapshot>)>,
}

const RECENCY_RECORDS_PER_ENTRY: usize = 4;
const RECENCY_COMPACTION_SLACK: usize = 16;

impl Default for QuerySnapshotCacheState {
    fn default() -> Self {
        Self {
            resident_bytes: std::mem::size_of::<Self>().saturating_add(
                RECENCY_COMPACTION_SLACK
                    .saturating_mul(std::mem::size_of::<(u64, Weak<CachedRuntimeQuerySnapshot>)>()),
            ),
            clock: 0,
            entry_count: 0,
            entries: HashMap::new(),
            cold_recency: VecDeque::new(),
            protected_recency: VecDeque::new(),
        }
    }
}

#[derive(Clone)]
pub(super) struct V1QuerySnapshotCache {
    state: Arc<Mutex<QuerySnapshotCacheState>>,
    maximum_bytes: usize,
    #[cfg(test)]
    deep_size_computations: Arc<std::sync::atomic::AtomicUsize>,
}

impl Default for V1QuerySnapshotCache {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(QuerySnapshotCacheState::default())),
            maximum_bytes: QUERY_SNAPSHOT_CACHE_BYTES,
            #[cfg(test)]
            deep_size_computations: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }
}

impl V1QuerySnapshotCache {
    pub(super) fn get_for_pinned(
        &self,
        pinned: &PinnedRootVector,
        logical: &LogicalProjectionBinding,
        catalog_lineage: &[[u8; 32]],
        recipe_catalog_proofs: &[QueryRecipeCatalogProof],
    ) -> Result<Option<Arc<CachedRuntimeQuerySnapshot>>, IndexError> {
        let identity = query_snapshot_identity(pinned.cut, &pinned.roots)?;
        Ok(self.get_matching(
            identity,
            logical,
            catalog_lineage,
            recipe_catalog_proofs,
            |cached| cached.pinned.matches_generation(pinned),
            false,
        ))
    }

    pub(super) fn get_for_continuation(
        &self,
        identity: QuerySnapshotIdentity,
        logical: &LogicalProjectionBinding,
        catalog_lineage: &[[u8; 32]],
        recipe_catalog_proofs: &[QueryRecipeCatalogProof],
        roots: &[QueryPositionRoot],
    ) -> Option<Arc<CachedRuntimeQuerySnapshot>> {
        self.get_matching(
            identity,
            logical,
            catalog_lineage,
            recipe_catalog_proofs,
            |cached| cached.pinned.matches_position_roots(roots),
            true,
        )
    }

    fn get_matching(
        &self,
        identity: QuerySnapshotIdentity,
        logical: &LogicalProjectionBinding,
        catalog_lineage: &[[u8; 32]],
        recipe_catalog_proofs: &[QueryRecipeCatalogProof],
        additional_match: impl Fn(&CachedRuntimeQuerySnapshot) -> bool,
        protect: bool,
    ) -> Option<Arc<CachedRuntimeQuerySnapshot>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let found = state.entries.get(&identity).and_then(|entries| {
            entries
                .iter()
                .find(|entry| {
                    entry
                        .snapshot
                        .matches_binding(logical, catalog_lineage, recipe_catalog_proofs)
                        && additional_match(entry)
                })
                .cloned()
        })?;
        touch(&mut state, &found, protect);
        Some(found)
    }

    pub(super) fn insert(
        &self,
        pinned: PinnedRootVector,
        snapshot: Arc<ValidatedQuerySnapshot>,
        protect: bool,
    ) {
        let identity = snapshot.identity();
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(existing) = exact_entry(&state, identity, &pinned, &snapshot) {
                touch(&mut state, &existing, protect);
                return;
            }
        }
        // Snapshot accounting deliberately charges shared Arc run descriptors
        // to every entry. That is conservative and, critically, is evaluated
        // only on a true miss rather than walking the immutable snapshot on
        // every query-cache hit.
        #[cfg(test)]
        self.deep_size_computations.fetch_add(1, Ordering::Relaxed);
        let resident_bytes = snapshot
            .resident_bytes()
            .saturating_add(std::mem::size_of::<PinnedRootVector>())
            .saturating_add(std::mem::size_of::<CachedRuntimeQuerySnapshot>())
            .saturating_add(
                pinned
                    .roots
                    .capacity()
                    .saturating_mul(std::mem::size_of::<PinnedPartitionQueryRoot>()),
            )
            .saturating_add(
                pinned
                    .generation_hashes
                    .capacity()
                    .saturating_mul(std::mem::size_of::<[u8; 32]>()),
            )
            .saturating_add(pinned.directory.entries.capacity().saturating_mul(
                std::mem::size_of::<keldra_index::v1::ProjectionPartitionDirectoryEntry>(),
            ))
            .saturating_add(
                pinned
                    .directory
                    .entries
                    .iter()
                    .fold(0usize, |bytes, entry| {
                        bytes.saturating_add(entry.covered_predecessors.capacity().saturating_mul(
                            std::mem::size_of::<keldra_index::v1::ProjectionGenerationReference>(),
                        ))
                    }),
            )
            .saturating_add(std::mem::size_of::<QuerySnapshotIdentity>())
            .saturating_add(std::mem::size_of::<Vec<Arc<CachedRuntimeQuerySnapshot>>>())
            .saturating_add(std::mem::size_of::<Arc<CachedRuntimeQuerySnapshot>>())
            .saturating_add(
                RECENCY_RECORDS_PER_ENTRY
                    .saturating_mul(std::mem::size_of::<(u64, Weak<CachedRuntimeQuerySnapshot>)>()),
            )
            // Hash-table control bytes and allocator headers are not exposed;
            // charge a deliberately padded per-entry allowance.
            .saturating_add(64);
        if resident_bytes > self.maximum_bytes {
            return;
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = exact_entry(&state, identity, &pinned, &snapshot) {
            touch(&mut state, &existing, protect);
            return;
        }
        let cached = Arc::new(CachedRuntimeQuerySnapshot {
            pinned: Arc::new(pinned),
            snapshot,
            resident_bytes,
            stamp: AtomicU64::new(0),
            protected: AtomicBool::new(protect),
        });
        state.resident_bytes = state.resident_bytes.saturating_add(resident_bytes);
        state.entry_count = state.entry_count.saturating_add(1);
        state
            .entries
            .entry(identity)
            .or_default()
            .push(cached.clone());
        touch(&mut state, &cached, protect);
        while state.resident_bytes > self.maximum_bytes {
            let oldest = pop_current(&mut state.cold_recency, false);
            let oldest = match oldest {
                Some(oldest) => Some(oldest),
                None => pop_current(&mut state.protected_recency, true),
            };
            let Some(oldest) = oldest else {
                break;
            };
            let oldest_identity = oldest.snapshot.identity();
            let mut remove_bucket = false;
            if let Some(entries) = state.entries.get_mut(&oldest_identity) {
                entries.retain(|entry| !Arc::ptr_eq(entry, &oldest));
                remove_bucket = entries.is_empty();
            }
            if remove_bucket {
                state.entries.remove(&oldest_identity);
            }
            state.resident_bytes = state.resident_bytes.saturating_sub(oldest.resident_bytes);
            state.entry_count = state.entry_count.saturating_sub(1);
        }
        compact_recency_if_needed(&mut state);
    }

    #[cfg(test)]
    fn with_maximum_bytes(maximum_bytes: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(QuerySnapshotCacheState::default())),
            maximum_bytes,
            deep_size_computations: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }
}

fn exact_entry(
    state: &QuerySnapshotCacheState,
    identity: QuerySnapshotIdentity,
    pinned: &PinnedRootVector,
    snapshot: &ValidatedQuerySnapshot,
) -> Option<Arc<CachedRuntimeQuerySnapshot>> {
    state.entries.get(&identity).and_then(|entries| {
        entries
            .iter()
            .find(|entry| {
                entry.snapshot.has_same_binding(snapshot) && entry.pinned.matches_generation(pinned)
            })
            .cloned()
    })
}

fn touch(
    state: &mut QuerySnapshotCacheState,
    entry: &Arc<CachedRuntimeQuerySnapshot>,
    protect: bool,
) {
    if protect {
        entry.protected.store(true, Ordering::Relaxed);
    }
    state.clock = state.clock.saturating_add(1);
    let stamp = state.clock;
    entry.stamp.store(stamp, Ordering::Relaxed);
    let record = (stamp, Arc::downgrade(entry));
    if entry.protected.load(Ordering::Relaxed) {
        state.protected_recency.push_back(record);
    } else {
        state.cold_recency.push_back(record);
    }
    compact_recency_if_needed(state);
}

fn pop_current(
    recency: &mut VecDeque<(u64, Weak<CachedRuntimeQuerySnapshot>)>,
    protected: bool,
) -> Option<Arc<CachedRuntimeQuerySnapshot>> {
    while let Some((stamp, entry)) = recency.pop_front() {
        let Some(entry) = entry.upgrade() else {
            continue;
        };
        if entry.stamp.load(Ordering::Relaxed) == stamp
            && entry.protected.load(Ordering::Relaxed) == protected
        {
            return Some(entry);
        }
    }
    None
}

fn compact_recency_if_needed(state: &mut QuerySnapshotCacheState) {
    let maximum_records = state
        .entry_count
        .saturating_mul(RECENCY_RECORDS_PER_ENTRY)
        .saturating_add(RECENCY_COMPACTION_SLACK);
    if state
        .cold_recency
        .len()
        .saturating_add(state.protected_recency.len())
        <= maximum_records
    {
        return;
    }
    state.cold_recency.retain(|(stamp, entry)| {
        entry.upgrade().is_some_and(|entry| {
            entry.stamp.load(Ordering::Relaxed) == *stamp
                && !entry.protected.load(Ordering::Relaxed)
        })
    });
    state.protected_recency.retain(|(stamp, entry)| {
        entry.upgrade().is_some_and(|entry| {
            entry.stamp.load(Ordering::Relaxed) == *stamp && entry.protected.load(Ordering::Relaxed)
        })
    });
    state.cold_recency.shrink_to_fit();
    state.protected_recency.shrink_to_fit();
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    use bytes::Bytes;
    use keldra_index::v1::{
        AuthorizedQueryCandidate, CatalogOrdinalRange, ProjectionFamilyPartitionDirectory,
        ProjectionPartitionIdentity, ProjectionQueryRunDescriptor, ProjectionQueryStreamRoot,
        QueryAdmissionContext, QueryArtifactLoad, QueryArtifactLoader, QueryBlockCredits,
        QueryBlockKind, QueryBlockLimits, QueryCandidateAdmission, QueryCommonCut,
        QueryDocumentGate, QueryExecutionLimits, QueryMemoryPermit, QueryRecipeCatalogProof,
        QueryRootCutProof, QueryRunPage, QueryRunReference, RecipeIdentity, StableDocumentKey,
        TypedJsonQueryRequest, encode_document_gate, encode_projection_query_run,
        encode_query_block, encode_query_run_page, execute_typed_json_query,
    };

    use super::*;

    struct Permit;

    impl QueryMemoryPermit for Permit {
        fn admitted_bytes(&self) -> usize {
            1024 * 1024
        }
    }

    struct Loader {
        artifacts: HashMap<[u8; 32], Bytes>,
        loads: usize,
    }

    impl QueryArtifactLoader for Loader {
        fn load_query_artifact(
            &mut self,
            request: QueryArtifactLoad,
        ) -> impl std::future::Future<Output = Result<Bytes, IndexError>> + Send {
            self.loads += 1;
            let artifact = self.artifacts.get(&request.hash).cloned();
            async move { artifact.ok_or(IndexError::Integrity) }
        }
    }

    struct Admission {
        calls: usize,
    }

    impl QueryCandidateAdmission for Admission {
        fn admit_snapshot_current_authorized_batch(
            &mut self,
            contexts: Vec<QueryAdmissionContext>,
        ) -> impl std::future::Future<
            Output = Result<Vec<Option<AuthorizedQueryCandidate>>, IndexError>,
        > + Send {
            self.calls += 1;
            async move {
                Ok(contexts
                    .into_iter()
                    .map(|context| {
                        Some(AuthorizedQueryCandidate {
                            candidate: context.candidate,
                        })
                    })
                    .collect())
            }
        }
    }

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    fn ready<T>(future: impl std::future::Future<Output = T>) -> T {
        let waker = Waker::from(Arc::new(NoopWake));
        let mut context = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("in-memory future unexpectedly yielded"),
        }
    }

    fn credits() -> QueryBlockCredits {
        QueryBlockCredits::from_query_permit(Box::new(Permit)).unwrap()
    }

    #[test]
    fn exact_fresh_generation_is_reused_and_a_republication_misses() {
        let partition = ProjectionPartitionIdentity::new([1; 32], 1, [2; 32], 3, 4, 5).unwrap();
        let membership = RecipeIdentity::new([3; 32]).unwrap();
        let document = StableDocumentKey::from_bytes([9; 32]).unwrap();
        let mut encoding_credits = credits();
        let gate = encode_query_block(
            QueryBlockKind::Gate,
            membership,
            &[encode_document_gate(QueryDocumentGate {
                document,
                material_source_version: 1,
                current_source_version: 1,
                live: true,
                source_path: Some("objects/source.json".into()),
                canonical_source_path: None,
                result_path: Some("objects/result.json".into()),
                result_version: 1,
            })
            .unwrap()],
            QueryBlockLimits::default_for_memory(),
            &mut encoding_credits,
        )
        .unwrap();
        let descriptor = ProjectionQueryRunDescriptor {
            partition,
            physical_catalog_generation: [4; 32],
            sequence: 1,
            source_start_offset: 1,
            next_offset: 2,
            through_atomic_position: 20,
            blocks: vec![gate.descriptor.clone()],
        };
        let encoded = encode_projection_query_run(
            &descriptor,
            QueryBlockLimits::default_for_memory(),
            &mut encoding_credits,
        )
        .unwrap();
        let page = encode_query_run_page(QueryRunPage::Leaf(vec![QueryRunReference {
            hash: encoded.hash,
            encoded_bytes: encoded.bytes.len() as u64,
            sequence: 1,
            level: 0,
            source_start_offset: 1,
            next_offset: 2,
            through_atomic_position: 20,
        }]))
        .unwrap();
        let cut = QueryCommonCut {
            through_atomic_position: 20,
        };
        let root = ProjectionQueryStreamRoot {
            stream_root_hash: page.hash,
            stream_root_encoded_bytes: page.bytes.len() as u64,
            run_count: 1,
            first_sequence: 1,
            last_sequence: 1,
            source_start_offset: 1,
            next_offset: 2,
            through_atomic_position: 20,
        };
        let pin = PinnedPartitionQueryRoot {
            partition,
            physical_catalog_generation: [4; 32],
            root,
            cut_proof: QueryRootCutProof {
                common_cut: cut,
                selected_stream_root_hash: root.stream_root_hash,
                next_newer_through_atomic_position: None,
            },
            handoff_lineage_id: [5; 32],
        };
        let logical = LogicalProjectionBinding {
            logical_index_id: 1,
            logical_definition_version: 1,
            family_id: [1; 32],
            physical_catalog_generation: [4; 32],
            membership,
            fields: Vec::new(),
        };
        let proofs = vec![QueryRecipeCatalogProof {
            recipe: membership,
            accepted_ordinal_ranges: vec![CatalogOrdinalRange { first: 0, last: 0 }],
        }];
        let lineage = vec![[4; 32]];
        let request = TypedJsonQueryRequest {
            logical: logical.clone(),
            fields: Vec::new(),
            catalog_lineage: lineage.clone(),
            recipe_catalog_proofs: proofs.clone(),
            predicate: None,
            order: Vec::new(),
            facets: Vec::new(),
            aggregates: Vec::new(),
            resume_after_document: None,
            result_limit: 10,
        };
        let mut loader = Loader {
            artifacts: [
                (page.hash, Bytes::from(page.bytes)),
                (encoded.hash, Bytes::from(encoded.bytes)),
                (gate.descriptor.hash, Bytes::from(gate.bytes)),
            ]
            .into(),
            loads: 0,
        };
        let mut admission = Admission { calls: 0 };
        let mut query_credits = credits();
        let (_, snapshot) = ready(execute_typed_json_query(
            &mut loader,
            &mut admission,
            cut,
            &[pin],
            None,
            &request,
            QueryExecutionLimits::default_for_memory(),
            QueryBlockLimits::default_for_memory(),
            &mut query_credits,
        ))
        .unwrap();
        assert_eq!(loader.loads, 3);
        assert_eq!(admission.calls, 1);

        let pinned = PinnedRootVector {
            cut,
            roots: vec![pin],
            generation_hashes: vec![[7; 32]],
            directory: ProjectionFamilyPartitionDirectory {
                family_id: [1; 32],
                revision: 1,
                entries: Vec::new(),
            },
            directory_version: keldra_store::VersionId(1),
        };
        let cache = V1QuerySnapshotCache::default();
        super::super::cache_completed_snapshot(
            &cache,
            pinned.clone(),
            snapshot.clone(),
            false,
            false,
        );
        assert_eq!(cache.deep_size_computations.load(Ordering::Relaxed), 1);
        cache.insert(pinned.clone(), snapshot.clone(), false);
        assert_eq!(cache.deep_size_computations.load(Ordering::Relaxed), 1);

        let hit = cache
            .get_for_pinned(&pinned, &logical, &lineage, &proofs)
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&hit.snapshot, &snapshot));
        let mut repeat_credits = credits();
        let (_, repeated_snapshot) = ready(execute_typed_json_query(
            &mut loader,
            &mut admission,
            cut,
            &[pin],
            Some(hit.snapshot.clone()),
            &request,
            QueryExecutionLimits::default_for_memory(),
            QueryBlockLimits::default_for_memory(),
            &mut repeat_credits,
        ))
        .unwrap();
        assert!(Arc::ptr_eq(&repeated_snapshot, &snapshot));
        assert_eq!(loader.loads, 4);
        assert_eq!(admission.calls, 2);
        for _ in 0..100 {
            assert!(
                cache
                    .get_for_pinned(&pinned, &logical, &lineage, &proofs)
                    .unwrap()
                    .is_some()
            );
        }
        let cache_state = cache
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            cache_state
                .cold_recency
                .len()
                .saturating_add(cache_state.protected_recency.len())
                <= cache_state
                    .entry_count
                    .saturating_mul(RECENCY_RECORDS_PER_ENTRY)
                    .saturating_add(RECENCY_COMPACTION_SLACK)
        );
        drop(cache_state);

        let mut republished = pinned.clone();
        republished.generation_hashes[0] = [8; 32];
        assert!(
            cache
                .get_for_pinned(&republished, &logical, &lineage, &proofs)
                .unwrap()
                .is_none()
        );
        cache.insert(republished.clone(), snapshot.clone(), true);
        let original_cursor_roots = vec![QueryPositionRoot {
            generation_hash: [7; 32],
            next_newer_through_atomic_position: None,
        }];
        let republished_cursor_roots = vec![QueryPositionRoot {
            generation_hash: [8; 32],
            next_newer_through_atomic_position: None,
        }];
        assert_eq!(
            cache
                .get_for_continuation(
                    snapshot.identity(),
                    &logical,
                    &lineage,
                    &proofs,
                    &original_cursor_roots,
                )
                .unwrap()
                .pinned
                .generation_hashes
                .as_slice(),
            &[[7; 32]]
        );
        assert_eq!(
            cache
                .get_for_continuation(
                    snapshot.identity(),
                    &logical,
                    &lineage,
                    &proofs,
                    &republished_cursor_roots,
                )
                .unwrap()
                .pinned
                .generation_hashes
                .as_slice(),
            &[[8; 32]]
        );

        let mut different_binding = logical.clone();
        different_binding.logical_definition_version += 1;
        assert!(
            cache
                .get_for_pinned(&pinned, &different_binding, &lineage, &proofs)
                .unwrap()
                .is_none()
        );

        let entry_bytes = hit.resident_bytes;
        let base_bytes = QuerySnapshotCacheState::default().resident_bytes;
        let eviction = V1QuerySnapshotCache::with_maximum_bytes(
            base_bytes.saturating_add(entry_bytes.saturating_mul(2)),
        );
        eviction.insert(pinned.clone(), snapshot.clone(), true);
        eviction.insert(republished.clone(), snapshot.clone(), false);
        let mut newest = pinned.clone();
        newest.generation_hashes[0] = [9; 32];
        eviction.insert(newest.clone(), snapshot.clone(), false);

        assert!(
            eviction
                .get_for_pinned(&pinned, &logical, &lineage, &proofs)
                .unwrap()
                .is_some()
        );
        assert!(
            eviction
                .get_for_pinned(&republished, &logical, &lineage, &proofs)
                .unwrap()
                .is_none()
        );
        assert!(
            eviction
                .get_for_pinned(&newest, &logical, &lineage, &proofs)
                .unwrap()
                .is_some()
        );
    }
}
