use std::collections::BTreeSet;

use crate::IndexError;

use super::{
    ComponentStreamRoot, EncodedComponentStreamPage, EncodedProjectionGeneration,
    ProjectionBarrier, ProjectionCurrent, ProjectionGeneration, SealedComponentDelta,
    SealedProjectionDeltaPack, append_component_stream, encode_projection_current,
    encode_projection_generation, pack_component_deltas,
};

/// Complete immutable payload which must be durable before its generation is
/// installed. None of these artifacts is query-visible by itself.
#[derive(Debug, Eq, PartialEq)]
pub struct PreparedProjectionGeneration {
    pub packs: Vec<SealedProjectionDeltaPack>,
    pub stream_pages: Vec<EncodedComponentStreamPage>,
    pub generation: EncodedProjectionGeneration,
    /// Mutable value installed by exact-version CAS only after every immutable
    /// artifact and the generation record are durable.
    pub current: Vec<u8>,
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
    family_id: [u8; 32],
    previous: Option<(&ProjectionGeneration, [u8; 32])>,
    barrier: ProjectionBarrier,
    deltas: Vec<SealedComponentDelta>,
    mut load_stream_page: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
) -> Result<PreparedProjectionGeneration, IndexError> {
    if family_id == [0; 32]
        || previous.is_some_and(|(generation, _)| generation.family_id != family_id)
    {
        return Err(IndexError::InvalidDefinition(
            "projection publication family is invalid".into(),
        ));
    }
    let mut components = BTreeSet::new();
    for delta in &deltas {
        if !components.insert(delta.component) {
            return Err(IndexError::InvalidDefinition(
                "projection publication contains duplicate component deltas".into(),
            ));
        }
    }

    let packs = pack_component_deltas(deltas)?;
    let mut stream_pages = Vec::new();
    let mut replacements = Vec::new();
    for delta in packs.iter().flat_map(|pack| &pack.deltas) {
        let previous_root = previous
            .and_then(|(generation, _)| generation.root(delta.component))
            .map(ComponentStreamRoot::from_component_root)
            .transpose()?;
        let appended =
            append_component_stream(previous_root, |hash| load_stream_page(hash), delta)?;
        replacements.push(appended.root.component_root()?);
        stream_pages.extend(appended.new_pages);
    }
    replacements.sort_by_key(|root| root.component);

    let generation = match previous {
        Some((generation, generation_hash)) => {
            generation.advance(generation_hash, barrier, replacements)?
        }
        None => ProjectionGeneration::initial(family_id, barrier, replacements)?,
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
    };
    prepared.payload_bytes()?;
    Ok(prepared)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use crate::v5::{
        CanonicalRecipeState, ComponentIdentity, DocumentHead, ProjectedDocumentState,
        ProjectionMutationBuffer, RecipeIdentity, decode_component_delta_segment,
        decode_projection_generation,
    };

    fn barrier(next: u64) -> ProjectionBarrier {
        ProjectionBarrier::new(vec![(1, [1; 32], next)], None).unwrap()
    }

    fn sealed(component: ComponentIdentity, byte: u8) -> SealedComponentDelta {
        let mut buffer = ProjectionMutationBuffer::new(16 * 1024).unwrap();
        let scope = [11; 32];
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
            vec![
                CanonicalRecipeState::new(RecipeIdentity::new([12; 32]).unwrap(), vec![1]).unwrap(),
            ],
            Vec::new(),
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

    #[test]
    fn initial_publication_contains_every_artifact_before_one_complete_generation() {
        let family = [7; 32];
        let prepared = prepare_projection_generation(
            family,
            None,
            barrier(2),
            vec![
                sealed(ComponentIdentity::DocumentHead, 1),
                sealed(ComponentIdentity::ProjectedState, 1),
            ],
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
        assert_eq!(generation.family_id, family);
        assert_eq!(generation.revision, 1);
        assert!(generation.root(ComponentIdentity::DocumentHead).is_some());
        assert!(generation.root(ComponentIdentity::ProjectedState).is_some());
        let current = crate::v5::decode_projection_current(&prepared.current).unwrap();
        current.validate_against(&generation).unwrap();
        assert_eq!(current.generation_hash, prepared.generation.hash);
    }

    #[test]
    fn advance_reopens_changed_streams_and_reuses_unchanged_roots() {
        let family = [8; 32];
        let initial = prepare_projection_generation(
            family,
            None,
            barrier(2),
            vec![
                sealed(ComponentIdentity::DocumentHead, 1),
                sealed(ComponentIdentity::ProjectedState, 1),
            ],
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
            family,
            Some((&previous, initial.generation.hash)),
            barrier(3),
            vec![sealed(ComponentIdentity::DocumentHead, 2)],
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
            current.root(ComponentIdentity::ProjectedState),
            previous.root(ComponentIdentity::ProjectedState)
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
                family,
                None,
                barrier(2),
                vec![delta.clone(), delta],
                |_| Err(IndexError::Integrity),
            )
            .is_err()
        );

        let initial = prepare_projection_generation(
            family,
            None,
            barrier(2),
            vec![
                sealed(ComponentIdentity::DocumentHead, 1),
                sealed(ComponentIdentity::ProjectedState, 1),
            ],
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
                family,
                Some((&previous, initial.generation.hash)),
                barrier(3),
                vec![sealed(ComponentIdentity::DocumentHead, 2)],
                |_| Err(IndexError::Integrity),
            )
            .is_err()
        );
    }

    #[test]
    fn empty_initial_projection_publishes_only_the_complete_generation() {
        let prepared =
            prepare_projection_generation([12; 32], None, barrier(2), Vec::new(), |_| {
                Err(IndexError::Integrity)
            })
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
            [10; 32],
            None,
            barrier(2),
            vec![
                sealed(ComponentIdentity::DocumentHead, 1),
                sealed(ComponentIdentity::ProjectedState, 1),
            ],
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
                ComponentIdentity::ProjectedState,
            ])
        );
    }
}
