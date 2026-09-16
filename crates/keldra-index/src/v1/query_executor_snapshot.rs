use std::sync::Arc;

use bytes::Bytes;

use crate::IndexError;
use crate::v1::{CatalogOrdinalRange, LogicalFieldBinding};

use super::{
    Budget, LogicalProjectionBinding, PinnedPartitionQueryRoot, ProjectionQueryRunDescriptor,
    ProjectionQueryStreamRoot, QueryArtifactKind, QueryArtifactLoad, QueryArtifactLoader,
    QueryBlockCredits, QueryBlockDescriptor, QueryBlockKind, QueryBlockLimits, QueryCommonCut,
    QueryPartitionExecutor, QueryPopulation, QueryRecipeCatalogProof, QueryRunChild, QueryRunPage,
    QueryRunReference, RecipeIdentity, TypedJsonQueryRequest, decode_projection_query_run,
    decode_query_run_page, page_summary, resource,
};

/// Content identity of one immutable, validated root-vector search snapshot.
///
/// Public continuations carry this identity beside their search-after key. A
/// continuation therefore reuses the exact descriptor/manifests admitted for
/// its first page rather than reconstructing a possibly newer root vector at
/// the same atomic cut.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct QuerySnapshotIdentity([u8; 32]);

impl QuerySnapshotIdentity {
    pub fn from_bytes(bytes: [u8; 32]) -> Result<Self, IndexError> {
        if bytes == [0; 32] {
            return Err(IndexError::InvalidQuery(
                "v1 query snapshot identity is zero".into(),
            ));
        }
        Ok(Self(bytes))
    }

    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PartitionView {
    pub(super) pin: PinnedPartitionQueryRoot,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct PartitionManifest {
    pub(super) view: PartitionView,
    pub(super) runs: Vec<Arc<ProjectionQueryRunDescriptor>>,
}

impl PartitionManifest {
    pub(super) fn matching_blocks(
        &self,
        kind: QueryBlockKind,
        recipe: RecipeIdentity,
    ) -> impl Iterator<Item = (usize, &QueryBlockDescriptor)> {
        self.runs
            .iter()
            .enumerate()
            .flat_map(move |(run_index, run)| {
                super::matching_run_blocks(run, kind, recipe)
                    .iter()
                    .map(move |block| (run_index, block))
            })
    }

    pub(super) fn find_run_block(
        &self,
        run: usize,
        hash: [u8; 32],
        kind: QueryBlockKind,
        recipe: RecipeIdentity,
    ) -> Result<&QueryBlockDescriptor, IndexError> {
        self.runs
            .get(run)
            .and_then(|run| {
                super::matching_run_blocks(run, kind, recipe)
                    .iter()
                    .find(|block| block.hash == hash)
            })
            .ok_or(IndexError::Integrity)
    }
}

struct QueryRunStream {
    stack: Vec<QueryRunChild>,
    pending: Vec<QueryRunReference>,
    emitted: u64,
    expected_runs: u64,
    previous_sequence: Option<u64>,
}

pub(super) async fn load_exact_pre_admitted<L: QueryArtifactLoader>(
    loader: &mut L,
    request: QueryArtifactLoad,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<Bytes, IndexError> {
    let kind = request.kind;
    let encoded_bytes = request.encoded_bytes;
    if encoded_bytes == 0 {
        return Err(IndexError::Integrity);
    }
    budget.load(kind, encoded_bytes)?;
    credits.reserve(encoded_bytes)?;
    let loaded = loader.load_query_artifact(request).await;
    let bytes = match loaded {
        Ok(bytes) => bytes,
        Err(error) => {
            credits.release(encoded_bytes)?;
            return Err(error);
        }
    };
    if bytes.len() != encoded_bytes {
        credits.release(encoded_bytes)?;
        return Err(IndexError::Integrity);
    }
    Ok(bytes)
}

impl QueryRunStream {
    fn new(root: ProjectionQueryStreamRoot) -> Self {
        let mut stack = Vec::new();
        if root.run_count != 0 {
            stack.push(QueryRunChild {
                hash: root.stream_root_hash,
                encoded_bytes: root.stream_root_encoded_bytes,
                run_count: root.run_count,
                first_sequence: root.first_sequence,
                last_sequence: root.last_sequence,
                source_start_offset: root.source_start_offset,
                next_offset: root.next_offset,
                through_atomic_position: root.through_atomic_position,
            });
        }
        Self {
            stack,
            pending: Vec::new(),
            emitted: 0,
            expected_runs: root.run_count,
            previous_sequence: None,
        }
    }

    async fn next<L: QueryArtifactLoader, X: QueryPartitionExecutor>(
        &mut self,
        loader: &mut L,
        executor: &X,
        credits: &mut QueryBlockCredits,
        budget: &mut Budget,
    ) -> Result<Option<QueryRunReference>, IndexError> {
        loop {
            if let Some(reference) = self.pending.pop() {
                if self
                    .previous_sequence
                    .is_some_and(|sequence| sequence <= reference.sequence)
                {
                    return Err(IndexError::Integrity);
                }
                self.previous_sequence = Some(reference.sequence);
                self.emitted = self
                    .emitted
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
                if self.emitted > self.expected_runs {
                    return Err(IndexError::Integrity);
                }
                return Ok(Some(reference));
            }
            let Some(expected) = self.stack.pop() else {
                if self.emitted != self.expected_runs {
                    return Err(IndexError::Integrity);
                }
                return Ok(None);
            };
            let encoded_bytes =
                usize::try_from(expected.encoded_bytes).map_err(|_| IndexError::Integrity)?;
            if encoded_bytes > budget.limits().maximum_page_bytes {
                return resource(encoded_bytes, budget.limits().maximum_page_bytes);
            }
            let bytes = load_exact_pre_admitted(
                loader,
                QueryArtifactLoad::direct(QueryArtifactKind::Page, expected.hash, encoded_bytes),
                credits,
                budget,
            )
            .await?;
            budget.reserve_heap(credits, bytes.len())?;
            let loaded_bytes = bytes.len();
            let page = executor
                .run_cpu(Box::new(move || decode_query_run_page(&bytes)))
                .await?;
            credits.release(loaded_bytes)?;
            if page_summary(expected.hash, &page, loaded_bytes)? != expected {
                return Err(IndexError::Integrity);
            }
            match page {
                QueryRunPage::Leaf(runs) => self.pending.extend(runs),
                QueryRunPage::Branch(children) => self.stack.extend(children),
            }
        }
    }
}

async fn load_next_descriptor<L: QueryArtifactLoader, X: QueryPartitionExecutor>(
    loader: &mut L,
    executor: &X,
    view: &PartitionView,
    catalog_lineage: &[[u8; 32]],
    recipe_catalog_proofs: &[QueryRecipeCatalogProof],
    stream: &mut QueryRunStream,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<Option<(Arc<ProjectionQueryRunDescriptor>, usize)>, IndexError> {
    let Some(reference) = stream.next(loader, executor, credits, budget).await? else {
        return Ok(None);
    };
    let encoded_bytes =
        usize::try_from(reference.encoded_bytes).map_err(|_| IndexError::Integrity)?;
    if encoded_bytes > block_limits.maximum_run_descriptor_bytes {
        return resource(encoded_bytes, block_limits.maximum_run_descriptor_bytes);
    }
    let request = QueryArtifactLoad::direct(QueryArtifactKind::Run, reference.hash, encoded_bytes);
    loop {
        if let Some(descriptor) = loader.cached_projection_query_run(request.clone())? {
            // Only descriptors produced by the validated decoder enter this
            // cache. Preserve logical load evidence, but do not repeat its full
            // structural validation on every query page.
            budget.load(request.kind, request.encoded_bytes)?;
            validate_loaded_descriptor(
                view,
                catalog_lineage,
                recipe_catalog_proofs,
                &reference,
                &descriptor,
            )?;
            return Ok(Some((descriptor, 0)));
        }
        let _leadership = match loader
            .coordinate_query_run_population(request.clone())
            .await?
        {
            QueryPopulation::Completed => continue,
            QueryPopulation::Lead(leadership) => leadership,
        };
        let populated = async {
            let bytes = load_exact_pre_admitted(loader, request.clone(), credits, budget).await?;
            let loaded_bytes = bytes.len();
            let mut decode_credits = credits.try_fork_query()?;
            let descriptor = executor
                .run_cpu(Box::new(move || {
                    Ok(Arc::new(decode_projection_query_run(
                        &bytes,
                        block_limits,
                        &mut decode_credits,
                    )?))
                }))
                .await?;
            credits.release(loaded_bytes)?;
            validate_loaded_descriptor(
                view,
                catalog_lineage,
                recipe_catalog_proofs,
                &reference,
                &descriptor,
            )?;
            loader.cache_projection_query_run(request.clone(), descriptor.clone());
            Ok(Some((descriptor, loaded_bytes)))
        }
        .await;
        return populated;
    }
}

fn validate_loaded_descriptor(
    view: &PartitionView,
    catalog_lineage: &[[u8; 32]],
    recipe_catalog_proofs: &[QueryRecipeCatalogProof],
    reference: &QueryRunReference,
    descriptor: &ProjectionQueryRunDescriptor,
) -> Result<(), IndexError> {
    if descriptor.partition != view.pin.partition
        || descriptor.sequence != reference.sequence
        || descriptor.source_start_offset != reference.source_start_offset
        || descriptor.next_offset != reference.next_offset
        || descriptor.through_atomic_position != reference.through_atomic_position
        || descriptor.through_atomic_position > view.pin.root.through_atomic_position
    {
        return Err(IndexError::Integrity);
    }
    for block in &descriptor.blocks {
        let proof = recipe_catalog_proofs
            .binary_search_by_key(&block.recipe, |proof| proof.recipe)
            .ok()
            .map(|index| &recipe_catalog_proofs[index])
            .ok_or(IndexError::Integrity)?;
        // The lineage is intentionally tiny (currently at most five clean
        // generations). A contiguous scan avoids a tree allocation and node
        // traversal on every fresh query snapshot.
        let ordinal = catalog_lineage
            .iter()
            .position(|generation| *generation == descriptor.physical_catalog_generation)
            .and_then(|ordinal| u32::try_from(ordinal).ok())
            .ok_or(IndexError::Integrity)?;
        if !proof.accepts_ordinal(ordinal) {
            return Err(IndexError::Integrity);
        }
    }
    Ok(())
}

pub(super) async fn load_partition_manifest<L: QueryArtifactLoader, X: QueryPartitionExecutor>(
    loader: &mut L,
    executor: &X,
    view: PartitionView,
    catalog_lineage: &[[u8; 32]],
    recipe_catalog_proofs: &[QueryRecipeCatalogProof],
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<(PartitionManifest, usize, usize), IndexError> {
    let mut stream = QueryRunStream::new(view.pin.root);
    let mut runs = Vec::new();
    let mut resident_bytes = 0usize;
    while let Some((run, run_bytes)) = load_next_descriptor(
        loader,
        executor,
        &view,
        catalog_lineage,
        recipe_catalog_proofs,
        &mut stream,
        block_limits,
        credits,
        budget,
    )
    .await?
    {
        resident_bytes = resident_bytes
            .checked_add(run_bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        runs.push(run);
    }
    let index_bytes = runs
        .capacity()
        .checked_mul(std::mem::size_of::<Arc<ProjectionQueryRunDescriptor>>())
        .ok_or(IndexError::OffsetOverflow)?;
    budget.reserve_heap(credits, index_bytes)?;
    Ok((
        PartitionManifest { view, runs },
        resident_bytes,
        index_bytes,
    ))
}

/// Immutable descriptor/manifests admitted for one exact published root
/// vector. The fields are private so query execution can rely on construction
/// having completed every structural and catalogue-coverage check.
#[derive(Debug, Eq, PartialEq)]
pub struct ValidatedQuerySnapshot {
    pub(super) memory_lease: crate::v1::SegmentMemoryLease,
    pub(super) identity: QuerySnapshotIdentity,
    pub(super) common_cut: QueryCommonCut,
    pub(super) pins: Vec<PinnedPartitionQueryRoot>,
    pub(super) logical: LogicalProjectionBinding,
    pub(super) catalog_lineage: Vec<[u8; 32]>,
    pub(super) recipe_catalog_proofs: Vec<QueryRecipeCatalogProof>,
    pub(super) manifests: Vec<PartitionManifest>,
}

impl ValidatedQuerySnapshot {
    pub fn has_memory_lease(&self) -> bool {
        self.memory_lease.is_attached()
    }
    pub fn attach_memory_lease(&self, lease: Arc<dyn Send + Sync + std::fmt::Debug>) -> bool {
        self.memory_lease.attach(lease)
    }
    pub fn runs(&self) -> impl Iterator<Item = &Arc<ProjectionQueryRunDescriptor>> {
        self.manifests
            .iter()
            .flat_map(|manifest| manifest.runs.iter())
    }
    pub const fn identity(&self) -> QuerySnapshotIdentity {
        self.identity
    }

    pub const fn common_cut(&self) -> QueryCommonCut {
        self.common_cut
    }

    pub fn pins(&self) -> &[PinnedPartitionQueryRoot] {
        &self.pins
    }

    pub fn matches_binding(
        &self,
        logical: &LogicalProjectionBinding,
        catalog_lineage: &[[u8; 32]],
        recipe_catalog_proofs: &[QueryRecipeCatalogProof],
    ) -> bool {
        &self.logical == logical
            && self.catalog_lineage == catalog_lineage
            && self.recipe_catalog_proofs == recipe_catalog_proofs
    }

    pub fn has_same_binding(&self, other: &Self) -> bool {
        self.logical == other.logical
            && self.catalog_lineage == other.catalog_lineage
            && self.recipe_catalog_proofs == other.recipe_catalog_proofs
    }

    /// Owned vectors and bindings only. Shared run descriptors, document
    /// tables and pack tables carry independent allocation-lifetime leases.
    pub fn resident_bytes(&self) -> usize {
        let fixed = std::mem::size_of::<Self>()
            .saturating_add(
                self.pins
                    .capacity()
                    .saturating_mul(std::mem::size_of::<PinnedPartitionQueryRoot>()),
            )
            .saturating_add(
                self.catalog_lineage
                    .capacity()
                    .saturating_mul(std::mem::size_of::<[u8; 32]>()),
            )
            .saturating_add(
                self.recipe_catalog_proofs
                    .capacity()
                    .saturating_mul(std::mem::size_of::<QueryRecipeCatalogProof>()),
            )
            .saturating_add(
                self.manifests
                    .capacity()
                    .saturating_mul(std::mem::size_of::<PartitionManifest>()),
            );
        let proofs = self
            .recipe_catalog_proofs
            .iter()
            .fold(0usize, |bytes, proof| {
                bytes.saturating_add(
                    proof
                        .accepted_ordinal_ranges
                        .capacity()
                        .saturating_mul(std::mem::size_of::<CatalogOrdinalRange>()),
                )
            });
        let logical = self.logical.fields.iter().fold(
            self.logical
                .fields
                .capacity()
                .saturating_mul(std::mem::size_of::<LogicalFieldBinding>()),
            |bytes, field| bytes.saturating_add(field.public_name.capacity()),
        );
        self.manifests.iter().fold(
            fixed.saturating_add(proofs).saturating_add(logical),
            |bytes, manifest| {
                bytes.saturating_add(
                    manifest
                        .runs
                        .capacity()
                        .saturating_mul(std::mem::size_of::<Arc<ProjectionQueryRunDescriptor>>()),
                )
            },
        )
    }

    pub(super) fn validate_for(
        &self,
        identity: QuerySnapshotIdentity,
        common_cut: QueryCommonCut,
        pins: &[PinnedPartitionQueryRoot],
        request: &TypedJsonQueryRequest,
    ) -> Result<(), IndexError> {
        if self.identity != identity
            || self.common_cut != common_cut
            || self.pins != pins
            || !self.matches_binding(
                &request.logical,
                &request.catalog_lineage,
                &request.recipe_catalog_proofs,
            )
            || self.manifests.len() != pins.len()
        {
            return Err(IndexError::InvalidQuery(
                "v1 query continuation does not match its validated snapshot".into(),
            ));
        }
        Ok(())
    }
}

/// Derives the signed-continuation identity for an exact ordered root vector.
pub fn query_snapshot_identity(
    common_cut: QueryCommonCut,
    pins: &[PinnedPartitionQueryRoot],
) -> Result<QuerySnapshotIdentity, IndexError> {
    if pins.is_empty()
        || pins
            .windows(2)
            .any(|pair| pair[0].partition >= pair[1].partition)
    {
        return Err(IndexError::InvalidQuery(
            "v1 query snapshot root vector is absent or non-canonical".into(),
        ));
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"keldra.index.v1.query-snapshot/v1\0");
    hasher.update(&common_cut.through_atomic_position.to_be_bytes());
    for pin in pins {
        pin.validate_at(common_cut)?;
        let partition = pin.partition;
        hasher.update(&partition.family_id);
        hasher.update(&partition.source_node.to_be_bytes());
        hasher.update(&partition.source_epoch);
        hasher.update(&partition.producer_node.to_be_bytes());
        hasher.update(&partition.placement_term.to_be_bytes());
        hasher.update(&partition.placement_index.to_be_bytes());
        hasher.update(&pin.physical_catalog_generation);
        let root = pin.root;
        hasher.update(&root.stream_root_hash);
        hasher.update(&root.stream_root_encoded_bytes.to_be_bytes());
        hasher.update(&root.run_count.to_be_bytes());
        hasher.update(&root.first_sequence.to_be_bytes());
        hasher.update(&root.last_sequence.to_be_bytes());
        hasher.update(&root.source_start_offset.to_be_bytes());
        hasher.update(&root.next_offset.to_be_bytes());
        hasher.update(&root.through_atomic_position.to_be_bytes());
        hasher.update(&pin.handoff_lineage_id);
        match pin.cut_proof.next_newer_through_atomic_position {
            Some(next) => {
                hasher.update(&[1]);
                hasher.update(&next.to_be_bytes());
            }
            None => {
                hasher.update(&[0]);
            }
        }
    }
    Ok(QuerySnapshotIdentity(*hasher.finalize().as_bytes()))
}
