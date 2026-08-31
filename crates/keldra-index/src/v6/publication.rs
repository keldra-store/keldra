use std::collections::BTreeSet;

use crate::IndexError;

use super::{
    ComponentStreamRoot, EncodedComponentStreamPage, EncodedProjectionGeneration,
    EncodedQueryBlock, EncodedQueryRunPage, PreparedQueryMutationBatch,
    ProjectionCatalogTransition, ProjectionCurrent, ProjectionGeneration,
    ProjectionGenerationReference, ProjectionPackCredits, ProjectionPartitionIdentity,
    ProjectionQueryRunArtifacts, QueryBlockCredits, QueryBlockLimits, QueryRunReference,
    SealedComponentDelta, SealedProjectionDeltaPack, append_component_stream,
    append_query_run_path_copy, decode_projection_generation, encode_projection_current,
    encode_projection_generation, pack_component_deltas, prepare_projection_query_run,
};

/// Complete immutable payload which must be durable before its generation is
/// installed. None of these artifacts is query-visible by itself.
#[derive(Debug)]
pub struct PreparedProjectionGeneration {
    pub packs: Vec<SealedProjectionDeltaPack>,
    pub stream_pages: Vec<EncodedComponentStreamPage>,
    pub generation: EncodedProjectionGeneration,
    /// Mutable value installed by exact-version CAS only after every immutable
    /// artifact and the generation record are durable.
    pub current: Vec<u8>,
    _pack_credits: ProjectionPackCredits,
}

/// One exact source/atomic-cut publication payload. The query credits remain
/// held until every immutable output and the final generation/current inputs
/// are dropped or handed to durable publication.
#[derive(Debug)]
pub struct PreparedAtomicProjectionGeneration {
    pub packs: Vec<SealedProjectionDeltaPack>,
    pub stream_pages: Vec<EncodedComponentStreamPage>,
    pub query_blocks: Vec<EncodedQueryBlock>,
    pub query_run: super::EncodedProjectionQueryRun,
    pub query_stream_pages: Vec<EncodedQueryRunPage>,
    pub generation: EncodedProjectionGeneration,
    pub current: Vec<u8>,
    _query_credits: QueryBlockCredits,
    _pack_credits: ProjectionPackCredits,
}

impl PreparedAtomicProjectionGeneration {
    pub fn payload_bytes(&self) -> Result<usize, IndexError> {
        self.packs
            .iter()
            .map(|pack| pack.bytes.len())
            .chain(self.stream_pages.iter().map(|page| page.bytes.len()))
            .chain(self.query_blocks.iter().map(|block| block.bytes.len()))
            .chain(self.query_stream_pages.iter().map(|page| page.bytes.len()))
            .chain(
                self.generation
                    .component_directory
                    .pages
                    .iter()
                    .map(|page| page.bytes.len()),
            )
            .chain([
                self.query_run.bytes.len(),
                self.generation.bytes.len(),
                self.current.len(),
            ])
            .try_fold(0usize, |total, bytes| {
                total.checked_add(bytes).ok_or(IndexError::OffsetOverflow)
            })
    }
}

impl PreparedProjectionGeneration {
    pub fn payload_bytes(&self) -> Result<usize, IndexError> {
        self.packs
            .iter()
            .map(|pack| pack.bytes.len())
            .chain(self.stream_pages.iter().map(|page| page.bytes.len()))
            .chain(
                self.generation
                    .component_directory
                    .pages
                    .iter()
                    .map(|page| page.bytes.len()),
            )
            .chain([self.generation.bytes.len(), self.current.len()])
            .try_fold(0_usize, |total, bytes| {
                total.checked_add(bytes).ok_or(IndexError::OffsetOverflow)
            })
    }
}

/// Prepare one all-or-none projection generation without performing I/O.
///
/// The caller publishes every returned immutable artifact, then atomically
/// installs `generation`. Failure before that install leaves only ordinary
/// content-addressed orphans. Existing stream pages are loaded by hash only
/// for the rightmost path of each changed component.
pub fn prepare_projection_generation(
    partition: ProjectionPartitionIdentity,
    physical_catalog_generation: [u8; 32],
    previous: Option<(&ProjectionGeneration, [u8; 32])>,
    source_start_offset: u64,
    next_offset: u64,
    through_atomic_position: u64,
    inherited_partitions: Vec<ProjectionGenerationReference>,
    deltas: Vec<SealedComponentDelta>,
    pack_credits: ProjectionPackCredits,
    load_stream_page: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
) -> Result<PreparedProjectionGeneration, IndexError> {
    prepare_projection_generation_inner(
        partition,
        physical_catalog_generation,
        previous,
        None,
        source_start_offset,
        next_offset,
        through_atomic_position,
        inherited_partitions,
        deltas,
        pack_credits,
        load_stream_page,
    )
}

#[allow(clippy::too_many_arguments)]
fn prepare_projection_generation_inner(
    partition: ProjectionPartitionIdentity,
    physical_catalog_generation: [u8; 32],
    previous: Option<(&ProjectionGeneration, [u8; 32])>,
    transition: Option<&ProjectionCatalogTransition>,
    source_start_offset: u64,
    next_offset: u64,
    through_atomic_position: u64,
    inherited_partitions: Vec<ProjectionGenerationReference>,
    deltas: Vec<SealedComponentDelta>,
    pack_credits: ProjectionPackCredits,
    mut load_stream_page: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
) -> Result<PreparedProjectionGeneration, IndexError> {
    validate_publication_cut(
        partition,
        physical_catalog_generation,
        previous,
        transition,
        source_start_offset,
        next_offset,
        through_atomic_position,
        &inherited_partitions,
    )?;
    let mut components = BTreeSet::new();
    for delta in &deltas {
        if !components.insert(delta.component) {
            return Err(IndexError::InvalidDefinition(
                "projection publication contains duplicate component deltas".into(),
            ));
        }
    }

    let charged_packs = pack_component_deltas(deltas, pack_credits)?;
    let (packs, pack_credits) = (charged_packs.packs, charged_packs.credits);
    let mut stream_pages = Vec::new();
    let mut replacements = Vec::new();
    for delta in packs.iter().flat_map(|pack| &pack.deltas) {
        let previous_root =
            if transition.is_none_or(|transition| transition.retains_component(delta.component)) {
                previous
                    .and_then(|(generation, _)| generation.root(delta.component))
                    .map(ComponentStreamRoot::from_component_root)
                    .transpose()?
            } else {
                None
            };
        let appended = append_component_stream(
            previous_root,
            |hash| load_stream_page(hash),
            delta,
            source_start_offset,
            next_offset,
            through_atomic_position,
        )?;
        replacements.push(appended.root.component_root()?);
        stream_pages.extend(appended.new_pages);
    }
    replacements.sort_by_key(|root| root.component);

    let generation = match previous {
        Some((generation, generation_hash)) => match transition {
            Some(transition) => generation.advance_catalog(
                generation_hash,
                physical_catalog_generation,
                next_offset,
                through_atomic_position,
                replacements,
                transition,
            )?,
            None => generation.advance(
                generation_hash,
                physical_catalog_generation,
                next_offset,
                through_atomic_position,
                replacements,
            )?,
        },
        None if inherited_partitions.is_empty() => ProjectionGeneration::initial(
            partition,
            physical_catalog_generation,
            next_offset,
            through_atomic_position,
            replacements,
        )?,
        None => ProjectionGeneration::initial_after_handoff(
            partition,
            physical_catalog_generation,
            next_offset,
            through_atomic_position,
            replacements,
            inherited_partitions,
        )?,
    };
    let encoded_generation = encode_projection_generation(&generation)?;
    let current = encode_projection_current(ProjectionCurrent::new(
        encoded_generation.hash,
        &generation,
    )?)?;
    let prepared = PreparedProjectionGeneration {
        packs,
        stream_pages,
        generation: encoded_generation,
        current,
        _pack_credits: pack_credits,
    };
    prepared.payload_bytes()?;
    Ok(prepared)
}

#[allow(clippy::too_many_arguments)]
fn validate_publication_cut(
    partition: ProjectionPartitionIdentity,
    physical_catalog_generation: [u8; 32],
    previous: Option<(&ProjectionGeneration, [u8; 32])>,
    transition: Option<&ProjectionCatalogTransition>,
    source_start_offset: u64,
    next_offset: u64,
    through_atomic_position: u64,
    inherited_partitions: &[ProjectionGenerationReference],
) -> Result<(), IndexError> {
    partition.validate()?;
    if physical_catalog_generation == [0; 32]
        || source_start_offset >= next_offset
        || previous.is_some_and(|(generation, _)| {
            generation.partition != partition || generation.next_offset != source_start_offset
        })
        || (previous.is_some() && !inherited_partitions.is_empty())
        || (transition.is_none()
            && previous.is_some_and(|(generation, _)| {
                generation.physical_catalog_generation != physical_catalog_generation
            }))
    {
        return Err(IndexError::InvalidDefinition(
            "projection publication family is invalid".into(),
        ));
    }
    if let (Some((generation, hash)), Some(transition)) = (previous, transition) {
        transition.validate_against(generation, hash, physical_catalog_generation)?;
    } else if transition.is_some() {
        return Err(IndexError::InvalidDefinition(
            "projection catalog transition has no exact predecessor".into(),
        ));
    }
    if previous.is_none() && !inherited_partitions.is_empty() {
        if inherited_partitions.iter().any(|reference| {
            reference.validate().is_err()
                || reference.partition.family_id != partition.family_id
                || reference.partition.source_node != partition.source_node
                || reference.partition.source_epoch != partition.source_epoch
                || reference.partition == partition
                || reference.physical_catalog_generation != physical_catalog_generation
        }) || inherited_partitions
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(IndexError::InvalidDefinition(
                "projection handoff lineage is invalid".into(),
            ));
        }
        let latest_inherited_offset = inherited_partitions
            .iter()
            .map(|reference| reference.next_offset)
            .max()
            .ok_or(IndexError::Integrity)?;
        if latest_inherited_offset != source_start_offset
            || inherited_partitions.iter().any(|reference| {
                reference.next_offset > source_start_offset
                    || reference.through_atomic_position > through_atomic_position
            })
        {
            return Err(IndexError::InvalidDefinition(
                "projection handoff does not begin at exact inherited coverage".into(),
            ));
        }
    }
    Ok(())
}

/// Atomically prepare component streams and the query-ready mini-run at one
/// source/atomic cut. The returned generation is the only visibility point and
/// references the newly appended query-run stream root.
#[allow(clippy::too_many_arguments)]
pub fn prepare_atomic_projection_generation(
    partition: ProjectionPartitionIdentity,
    physical_catalog_generation: [u8; 32],
    previous: Option<(&ProjectionGeneration, [u8; 32])>,
    source_start_offset: u64,
    next_offset: u64,
    through_atomic_position: u64,
    inherited_partitions: Vec<ProjectionGenerationReference>,
    deltas: Vec<SealedComponentDelta>,
    query_batch: PreparedQueryMutationBatch,
    query_limits: QueryBlockLimits,
    query_credits: QueryBlockCredits,
    pack_credits: ProjectionPackCredits,
    load_stream_page: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
    load_query_page: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
) -> Result<PreparedAtomicProjectionGeneration, IndexError> {
    prepare_atomic_projection_generation_inner(
        partition,
        physical_catalog_generation,
        previous,
        None,
        source_start_offset,
        next_offset,
        through_atomic_position,
        inherited_partitions,
        deltas,
        query_batch,
        query_limits,
        query_credits,
        pack_credits,
        load_stream_page,
        load_query_page,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn prepare_atomic_projection_catalog_transition(
    partition: ProjectionPartitionIdentity,
    physical_catalog_generation: [u8; 32],
    previous: (&ProjectionGeneration, [u8; 32]),
    transition: ProjectionCatalogTransition,
    source_start_offset: u64,
    next_offset: u64,
    through_atomic_position: u64,
    deltas: Vec<SealedComponentDelta>,
    query_batch: PreparedQueryMutationBatch,
    query_limits: QueryBlockLimits,
    query_credits: QueryBlockCredits,
    pack_credits: ProjectionPackCredits,
    load_stream_page: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
    load_query_page: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
) -> Result<PreparedAtomicProjectionGeneration, IndexError> {
    prepare_atomic_projection_generation_inner(
        partition,
        physical_catalog_generation,
        Some(previous),
        Some(&transition),
        source_start_offset,
        next_offset,
        through_atomic_position,
        Vec::new(),
        deltas,
        query_batch,
        query_limits,
        query_credits,
        pack_credits,
        load_stream_page,
        load_query_page,
    )
}

#[allow(clippy::too_many_arguments)]
fn prepare_atomic_projection_generation_inner(
    partition: ProjectionPartitionIdentity,
    physical_catalog_generation: [u8; 32],
    previous: Option<(&ProjectionGeneration, [u8; 32])>,
    transition: Option<&ProjectionCatalogTransition>,
    source_start_offset: u64,
    next_offset: u64,
    through_atomic_position: u64,
    inherited_partitions: Vec<ProjectionGenerationReference>,
    deltas: Vec<SealedComponentDelta>,
    query_batch: PreparedQueryMutationBatch,
    query_limits: QueryBlockLimits,
    query_credits: QueryBlockCredits,
    pack_credits: ProjectionPackCredits,
    mut load_stream_page: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
    mut load_query_page: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
) -> Result<PreparedAtomicProjectionGeneration, IndexError> {
    validate_publication_cut(
        partition,
        physical_catalog_generation,
        previous,
        transition,
        source_start_offset,
        next_offset,
        through_atomic_position,
        &inherited_partitions,
    )?;
    if previous
        .is_some_and(|(generation, _)| generation.through_atomic_position > through_atomic_position)
    {
        return Err(IndexError::InvalidDefinition(
            "atomic projection preparation cut is not contiguous".into(),
        ));
    }
    let sequence = match previous {
        Some((generation, _)) => generation
            .query_stream_root
            .last_sequence
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?,
        None => 1,
    };
    let charged_query = prepare_projection_query_run(
        partition,
        physical_catalog_generation,
        sequence,
        source_start_offset,
        next_offset,
        through_atomic_position,
        query_batch,
        query_limits,
        query_credits,
    )?;
    let (ProjectionQueryRunArtifacts { blocks, run }, query_credits) = charged_query.into_parts();
    let previous_query_root = match previous {
        Some((generation, _)) => generation.query_stream_root,
        None => super::ProjectionQueryStreamRoot::empty(
            partition,
            physical_catalog_generation,
            source_start_offset,
            0,
        )?,
    };
    let appended_query = append_query_run_path_copy(
        Some(previous_query_root),
        partition,
        physical_catalog_generation,
        QueryRunReference {
            hash: run.hash,
            sequence,
            level: 0,
            source_start_offset,
            next_offset,
            through_atomic_position,
        },
        |hash| load_query_page(hash),
    )?;
    let components = prepare_projection_generation_inner(
        partition,
        physical_catalog_generation,
        previous,
        transition,
        source_start_offset,
        next_offset,
        through_atomic_position,
        inherited_partitions,
        deltas,
        pack_credits,
        |hash| load_stream_page(hash),
    )?;
    let generation = decode_projection_generation(
        &components.generation.bytes,
        &components.generation.component_directory,
    )?
    .with_query_stream_root(appended_query.root)?;
    generation.validate()?;
    let encoded_generation = encode_projection_generation(&generation)?;
    let current = encode_projection_current(ProjectionCurrent::new(
        encoded_generation.hash,
        &generation,
    )?)?;
    let prepared = PreparedAtomicProjectionGeneration {
        packs: components.packs,
        stream_pages: components.stream_pages,
        query_blocks: blocks,
        query_run: run,
        query_stream_pages: appended_query.pages,
        generation: encoded_generation,
        current,
        _query_credits: query_credits,
        _pack_credits: components._pack_credits,
    };
    prepared.payload_bytes()?;
    Ok(prepared)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use crate::v6::{
        CanonicalRecipeState, ComponentIdentity, ComponentStreamDirectory, DocumentHead,
        IndexingMemoryCredits, IndexingMemoryLimits, IndexingMemoryStage, ProjectedDocumentState,
        ProjectionMutationBuffer, RecipeIdentity, StableDocumentKey,
        decode_component_delta_segment, decode_component_stream, decode_projection_generation,
        decode_projection_query_run, visit_query_runs_newest,
    };

    fn partition(family_id: [u8; 32]) -> ProjectionPartitionIdentity {
        ProjectionPartitionIdentity::new(family_id, 1, [2; 32], 1, 3, 4).unwrap()
    }

    fn sealed(component: ComponentIdentity, byte: u8) -> SealedComponentDelta {
        if component == ComponentIdentity::SourceRecords {
            let key =
                StableDocumentKey::derive([11; 32], format!("objects/{byte}").as_str(), 0).unwrap();
            return super::super::buffer::seal_component(
                component,
                BTreeMap::from([(key, Some(vec![byte]))]),
            )
            .unwrap();
        }
        let mut buffer = ProjectionMutationBuffer::new(16 * 1024).unwrap();
        let scope = [11; 32];
        let recipe = match component {
            ComponentIdentity::Membership(recipe)
            | ComponentIdentity::Field(recipe)
            | ComponentIdentity::Order(recipe) => recipe,
            ComponentIdentity::DocumentHead | ComponentIdentity::SourceRecords => {
                RecipeIdentity::new([12; 32]).unwrap()
            }
        };
        let canonical = CanonicalRecipeState::new(recipe, vec![1]).unwrap();
        let (memberships, fields) = if matches!(component, ComponentIdentity::Field(_)) {
            (Vec::new(), vec![canonical])
        } else {
            (vec![canonical], Vec::new())
        };
        let state = ProjectedDocumentState::new(
            scope,
            DocumentHead::new(
                scope,
                format!("objects/{byte}"),
                0,
                u64::from(byte),
                None,
                true,
            )
            .unwrap(),
            memberships,
            fields,
        )
        .unwrap();
        buffer.apply_state(&state, None).unwrap();
        buffer
            .seal()
            .unwrap()
            .into_iter()
            .find(|delta| delta.component == component)
            .unwrap()
    }

    fn page_store(prepared: &PreparedProjectionGeneration) -> BTreeMap<[u8; 32], Vec<u8>> {
        prepared
            .stream_pages
            .iter()
            .map(|page| (page.hash, page.bytes.clone()))
            .collect()
    }

    fn query_credits(bytes: usize) -> QueryBlockCredits {
        let memory = IndexingMemoryCredits::new(
            bytes,
            IndexingMemoryLimits {
                hot_payload_bytes: bytes,
                worker_scratch_bytes: bytes,
                prepared_rows_bytes: bytes,
                replay_input_bytes: bytes,
                projection_accumulator_bytes: bytes,
                seal_scratch_bytes: bytes,
                ordering_catalog_bytes: bytes,
            },
        )
        .unwrap();
        QueryBlockCredits::from_pipeline_permit(
            memory
                .acquire(IndexingMemoryStage::OrderingCatalog, bytes)
                .unwrap(),
        )
    }

    fn pack_credits(bytes: usize) -> ProjectionPackCredits {
        let memory = IndexingMemoryCredits::new(
            bytes,
            IndexingMemoryLimits {
                hot_payload_bytes: bytes,
                worker_scratch_bytes: bytes,
                prepared_rows_bytes: bytes,
                replay_input_bytes: bytes,
                projection_accumulator_bytes: bytes,
                seal_scratch_bytes: bytes,
                ordering_catalog_bytes: bytes,
            },
        )
        .unwrap();
        ProjectionPackCredits::from_pipeline_permit(
            memory
                .acquire(IndexingMemoryStage::SealScratch, bytes)
                .unwrap(),
        )
    }

    fn only_component_descriptor(
        prepared: &PreparedProjectionGeneration,
        component: ComponentIdentity,
    ) -> super::super::ComponentSegmentDescriptor {
        let generation = decode_projection_generation(
            &prepared.generation.bytes,
            &prepared.generation.component_directory,
        )
        .unwrap();
        let root =
            ComponentStreamRoot::from_component_root(generation.root(component).unwrap()).unwrap();
        decode_component_stream(&ComponentStreamDirectory {
            component,
            root_hash: root.root_hash,
            segment_count: root.segment_count,
            first_sequence: root.first_sequence,
            last_sequence: root.last_sequence,
            encoded_bytes: root.encoded_bytes,
            logical_bytes: root.logical_bytes,
            directory_bytes: root.directory_bytes,
            pages: prepared.stream_pages.clone(),
        })
        .unwrap()
        .remove(0)
    }

    #[test]
    fn initial_publication_contains_every_artifact_before_one_complete_generation() {
        let family = [7; 32];
        let prepared = prepare_projection_generation(
            partition(family),
            [6; 32],
            None,
            0,
            2,
            10,
            Vec::new(),
            vec![
                sealed(ComponentIdentity::DocumentHead, 1),
                sealed(ComponentIdentity::SourceRecords, 1),
            ],
            pack_credits(1024 * 1024),
            |_| Err(IndexError::Integrity),
        )
        .unwrap();

        assert_eq!(prepared.packs.len(), 1);
        assert_eq!(prepared.packs[0].deltas.len(), 2);
        assert_eq!(prepared.stream_pages.len(), 2);
        assert!(prepared.payload_bytes().unwrap() > prepared.packs[0].bytes.len());
        let generation = decode_projection_generation(
            &prepared.generation.bytes,
            &prepared.generation.component_directory,
        )
        .unwrap();
        assert_eq!(generation.partition.family_id, family);
        assert_eq!(generation.revision, 1);
        assert!(generation.root(ComponentIdentity::DocumentHead).is_some());
        assert!(generation.root(ComponentIdentity::SourceRecords).is_some());
        let current = crate::v6::decode_projection_current(&prepared.current).unwrap();
        current.validate_against(&generation).unwrap();
        assert_eq!(current.generation_hash, prepared.generation.hash);
    }

    #[test]
    fn advance_reopens_changed_streams_and_reuses_unchanged_roots() {
        let family = [8; 32];
        let initial = prepare_projection_generation(
            partition(family),
            [6; 32],
            None,
            0,
            2,
            10,
            Vec::new(),
            vec![
                sealed(ComponentIdentity::DocumentHead, 1),
                sealed(ComponentIdentity::SourceRecords, 1),
            ],
            pack_credits(1024 * 1024),
            |_| Err(IndexError::Integrity),
        )
        .unwrap();
        let previous = decode_projection_generation(
            &initial.generation.bytes,
            &initial.generation.component_directory,
        )
        .unwrap();
        let pages = page_store(&initial);
        let advanced = prepare_projection_generation(
            partition(family),
            [6; 32],
            Some((&previous, initial.generation.hash)),
            2,
            3,
            11,
            Vec::new(),
            vec![sealed(ComponentIdentity::DocumentHead, 2)],
            pack_credits(1024 * 1024),
            |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
        )
        .unwrap();
        let current = decode_projection_generation(
            &advanced.generation.bytes,
            &advanced.generation.component_directory,
        )
        .unwrap();

        assert_eq!(current.revision, 2);
        assert_eq!(
            current.previous_generation_hash,
            Some(initial.generation.hash)
        );
        assert_eq!(
            current.root(ComponentIdentity::SourceRecords),
            previous.root(ComponentIdentity::SourceRecords)
        );
        assert_eq!(
            current
                .root(ComponentIdentity::DocumentHead)
                .unwrap()
                .segment_count,
            2
        );
        assert_eq!(advanced.stream_pages.len(), 1);
    }

    #[test]
    fn duplicate_components_and_missing_previous_pages_fail_before_publication() {
        let family = [9; 32];
        let delta = sealed(ComponentIdentity::DocumentHead, 1);
        assert!(
            prepare_projection_generation(
                partition(family),
                [6; 32],
                None,
                0,
                2,
                10,
                Vec::new(),
                vec![delta.clone(), delta],
                pack_credits(1024 * 1024),
                |_| Err(IndexError::Integrity),
            )
            .is_err()
        );

        let initial = prepare_projection_generation(
            partition(family),
            [6; 32],
            None,
            0,
            2,
            10,
            Vec::new(),
            vec![
                sealed(ComponentIdentity::DocumentHead, 1),
                sealed(ComponentIdentity::SourceRecords, 1),
            ],
            pack_credits(1024 * 1024),
            |_| Err(IndexError::Integrity),
        )
        .unwrap();
        let previous = decode_projection_generation(
            &initial.generation.bytes,
            &initial.generation.component_directory,
        )
        .unwrap();
        assert!(
            prepare_projection_generation(
                partition(family),
                [6; 32],
                Some((&previous, initial.generation.hash)),
                2,
                3,
                11,
                Vec::new(),
                vec![sealed(ComponentIdentity::DocumentHead, 2)],
                pack_credits(1024 * 1024),
                |_| Err(IndexError::Integrity),
            )
            .is_err()
        );
    }

    #[test]
    fn empty_initial_projection_publishes_only_the_complete_generation() {
        let prepared = prepare_projection_generation(
            partition([12; 32]),
            [6; 32],
            None,
            0,
            2,
            10,
            Vec::new(),
            Vec::new(),
            pack_credits(1),
            |_| Err(IndexError::Integrity),
        )
        .unwrap();
        assert!(prepared.packs.is_empty());
        assert!(prepared.stream_pages.is_empty());
        assert_eq!(prepared.generation.component_directory.root_count, 0);
        assert!(prepared.generation.component_directory.pages.is_empty());
        let generation = decode_projection_generation(
            &prepared.generation.bytes,
            &prepared.generation.component_directory,
        )
        .unwrap();
        assert!(generation.roots.is_empty());
    }

    #[test]
    fn packed_delta_hashes_cover_exact_component_bytes() {
        let prepared = prepare_projection_generation(
            partition([10; 32]),
            [6; 32],
            None,
            0,
            2,
            10,
            Vec::new(),
            vec![
                sealed(ComponentIdentity::DocumentHead, 1),
                sealed(ComponentIdentity::SourceRecords, 1),
            ],
            pack_credits(1024 * 1024),
            |_| Err(IndexError::Integrity),
        )
        .unwrap();
        let components = prepared.packs[0]
            .deltas
            .iter()
            .map(|delta| {
                let start = delta.offset as usize;
                let end = start + delta.encoded_bytes as usize;
                let decoded =
                    decode_component_delta_segment(&prepared.packs[0].bytes[start..end]).unwrap();
                assert_eq!(
                    delta.segment_hash,
                    *blake3::hash(&prepared.packs[0].bytes[start..end]).as_bytes()
                );
                decoded.component
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            components,
            BTreeSet::from([
                ComponentIdentity::DocumentHead,
                ComponentIdentity::SourceRecords,
            ])
        );
    }

    #[test]
    fn initial_backfill_and_handoff_preserve_nonzero_source_start() {
        let family = [13; 32];
        let backfill = prepare_projection_generation(
            partition(family),
            [6; 32],
            None,
            41,
            42,
            50,
            Vec::new(),
            vec![sealed(ComponentIdentity::DocumentHead, 1)],
            pack_credits(1024 * 1024),
            |_| Err(IndexError::Integrity),
        )
        .unwrap();
        assert_eq!(
            only_component_descriptor(&backfill, ComponentIdentity::DocumentHead)
                .source_start_offset,
            41
        );

        let predecessor_partition =
            ProjectionPartitionIdentity::new(family, 1, [2; 32], 9, 2, 3).unwrap();
        let predecessor =
            ProjectionGeneration::initial(predecessor_partition, [6; 32], 41, 49, Vec::new())
                .unwrap()
                .reference([8; 32])
                .unwrap();
        let handoff = prepare_projection_generation(
            partition(family),
            [6; 32],
            None,
            41,
            42,
            50,
            vec![predecessor],
            vec![sealed(ComponentIdentity::DocumentHead, 2)],
            pack_credits(1024 * 1024),
            |_| Err(IndexError::Integrity),
        )
        .unwrap();
        assert_eq!(
            only_component_descriptor(&handoff, ComponentIdentity::DocumentHead)
                .source_start_offset,
            41
        );
        assert!(
            prepare_projection_generation(
                partition(family),
                [6; 32],
                None,
                40,
                42,
                50,
                vec![predecessor],
                vec![sealed(ComponentIdentity::DocumentHead, 3)],
                pack_credits(1024 * 1024),
                |_| Err(IndexError::Integrity),
            )
            .is_err()
        );
    }

    #[test]
    fn atomic_prepare_binds_components_query_run_generation_and_current_to_one_cut() {
        let family = [14; 32];
        let first = prepare_atomic_projection_generation(
            partition(family),
            [6; 32],
            None,
            0,
            2,
            10,
            Vec::new(),
            vec![sealed(ComponentIdentity::DocumentHead, 1)],
            PreparedQueryMutationBatch::default(),
            QueryBlockLimits::default_for_memory(),
            query_credits(1024 * 1024),
            pack_credits(1024 * 1024),
            |_| Err(IndexError::Integrity),
            |_| Err(IndexError::Integrity),
        )
        .unwrap();
        assert!(first.query_blocks.is_empty());
        assert_eq!(first.query_stream_pages.len(), 1);
        let mut descriptor_credits = query_credits(1024 * 1024);
        let query_descriptor = decode_projection_query_run(
            &first.query_run.bytes,
            QueryBlockLimits::default_for_memory(),
            &mut descriptor_credits,
        )
        .unwrap();
        assert!(query_descriptor.blocks.is_empty());
        assert_eq!(query_descriptor.source_start_offset, 0);
        assert_eq!(query_descriptor.next_offset, 2);
        assert_eq!(query_descriptor.through_atomic_position, 10);
        let generation = decode_projection_generation(
            &first.generation.bytes,
            &first.generation.component_directory,
        )
        .unwrap();
        assert_eq!(generation.next_offset, 2);
        assert_eq!(generation.through_atomic_position, 10);
        assert_eq!(generation.query_stream_root.run_count, 1);
        let current = crate::v6::decode_projection_current(&first.current).unwrap();
        current.validate_against(&generation).unwrap();
        let query_pages = first
            .query_stream_pages
            .iter()
            .map(|page| (page.hash, page.bytes.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut references = Vec::new();
        visit_query_runs_newest(
            generation.query_stream_root,
            |hash| query_pages.get(&hash).cloned().ok_or(IndexError::Integrity),
            &mut |reference| {
                references.push(reference);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(references.len(), 1);
        assert_eq!(references[0].hash, first.query_run.hash);
        assert_eq!(references[0].source_start_offset, 0);
        assert_eq!(references[0].next_offset, 2);
        assert_eq!(references[0].through_atomic_position, 10);

        assert!(
            prepare_atomic_projection_generation(
                partition(family),
                [6; 32],
                Some((&generation, first.generation.hash)),
                1,
                3,
                11,
                Vec::new(),
                Vec::new(),
                PreparedQueryMutationBatch::default(),
                QueryBlockLimits::default_for_memory(),
                query_credits(1024 * 1024),
                pack_credits(1024 * 1024),
                |_| Err(IndexError::Integrity),
                |_| Err(IndexError::Integrity),
            )
            .is_err()
        );
    }

    #[test]
    fn atomic_catalog_transition_keeps_only_explicit_retained_history() {
        let family = [15; 32];
        let retained = RecipeIdentity::new([12; 32]).unwrap();
        let first = prepare_atomic_projection_generation(
            partition(family),
            [6; 32],
            None,
            0,
            2,
            10,
            Vec::new(),
            vec![
                sealed(ComponentIdentity::DocumentHead, 1),
                sealed(ComponentIdentity::Field(retained), 1),
            ],
            PreparedQueryMutationBatch::default(),
            QueryBlockLimits::default_for_memory(),
            query_credits(1024 * 1024),
            pack_credits(1024 * 1024),
            |_| Err(IndexError::Integrity),
            |_| Err(IndexError::Integrity),
        )
        .unwrap();
        let previous = decode_projection_generation(
            &first.generation.bytes,
            &first.generation.component_directory,
        )
        .unwrap();
        let retained_root = previous.root(ComponentIdentity::Field(retained)).cloned();
        let stream_pages = first
            .stream_pages
            .iter()
            .map(|page| (page.hash, page.bytes.clone()))
            .collect::<BTreeMap<_, _>>();
        let query_pages = first
            .query_stream_pages
            .iter()
            .map(|page| (page.hash, page.bytes.clone()))
            .collect::<BTreeMap<_, _>>();
        let transition = ProjectionCatalogTransition {
            predecessor: previous.reference(first.generation.hash).unwrap(),
            retained_recipes: vec![retained],
        };
        let second = prepare_atomic_projection_catalog_transition(
            partition(family),
            [7; 32],
            (&previous, first.generation.hash),
            transition,
            2,
            3,
            11,
            Vec::new(),
            PreparedQueryMutationBatch::default(),
            QueryBlockLimits::default_for_memory(),
            query_credits(1024 * 1024),
            pack_credits(1),
            |hash| {
                stream_pages
                    .get(&hash)
                    .cloned()
                    .ok_or(IndexError::Integrity)
            },
            |hash| query_pages.get(&hash).cloned().ok_or(IndexError::Integrity),
        )
        .unwrap();
        let next = decode_projection_generation(
            &second.generation.bytes,
            &second.generation.component_directory,
        )
        .unwrap();
        assert_eq!(next.physical_catalog_generation, [7; 32]);
        assert_eq!(
            next.root(ComponentIdentity::Field(retained)).cloned(),
            retained_root
        );
        assert_eq!(next.query_stream_root.run_count, 2);
        assert_eq!(next.previous_generation_hash, Some(first.generation.hash));
    }
}
