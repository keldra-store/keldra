use super::tests::{
    pack_credits, partition, prepare_atomic_projection_generation, query_credits, sealed,
};
use super::*;
use crate::v1::{
    ComponentIdentity, ComponentStreamDirectory, PreparedQueryMutationBatch, QueryBlockLimits,
    SealedComponentDelta, StableDocumentKey, decode_component_stream, decode_projection_current,
    decode_projection_generation, resolve_component_record_from_verified_artifacts,
};

fn key(byte: u8) -> StableDocumentKey {
    StableDocumentKey::from_bytes([byte; 32]).unwrap()
}
fn child(records: &[(u8, Option<&[u8]>)]) -> SealedComponentDelta {
    super::super::buffer::seal_component(
        ComponentIdentity::SourceRecords,
        records
            .iter()
            .map(|(byte, value)| (key(*byte), value.map(<[u8]>::to_vec)))
            .collect(),
    )
    .unwrap()
}
fn directory(
    generation: &ProjectionGeneration,
    pages: Vec<EncodedComponentStreamPage>,
) -> ComponentStreamDirectory {
    let root = ComponentStreamRoot::from_component_root(
        generation.root(ComponentIdentity::SourceRecords).unwrap(),
    )
    .unwrap();
    let mut available = pages
        .into_iter()
        .map(|page| (page.hash, page))
        .collect::<BTreeMap<_, _>>();
    let mut pending = vec![root.root_hash];
    let mut reachable = BTreeMap::new();
    while let Some(hash) = pending.pop() {
        if reachable.contains_key(&hash) {
            continue;
        }
        let page = available
            .remove(&hash)
            .expect("fixture contains every reachable page");
        pending
            .extend(crate::v1::component_stream_child_hashes(root.component, &page.bytes).unwrap());
        reachable.insert(hash, page);
    }
    ComponentStreamDirectory {
        component: root.component,
        root_hash: root.root_hash,
        segment_count: root.segment_count,
        first_sequence: root.first_sequence,
        last_sequence: root.last_sequence,
        encoded_bytes: root.encoded_bytes,
        logical_bytes: root.logical_bytes,
        directory_bytes: root.directory_bytes,
        pages: reachable.into_values().collect(),
    }
}

#[test]
fn split_children_publish_one_atomic_cut_then_successor_updates_and_tombstones_win() {
    let partition = partition([9; 32]);
    let initial = prepare_atomic_projection_generation(
        partition,
        [6; 32],
        None,
        0,
        2,
        10,
        Vec::new(),
        vec![
            sealed(ComponentIdentity::DocumentHead, 1),
            child(&[(1, Some(b"first"))]),
            child(&[(2, Some(b"second"))]),
        ],
        PreparedQueryMutationBatch::default(),
        QueryBlockLimits::default_for_memory(),
        query_credits(1024 * 1024),
        pack_credits(1024 * 1024),
        |_| -> Result<Vec<u8>, IndexError> {
            panic!("siblings must resolve their newly prepared pages without external reads")
        },
        |_| Err::<Vec<u8>, _>(IndexError::Integrity),
    )
    .unwrap();
    let generation = decode_projection_generation(
        &initial.generation.bytes,
        &initial.generation.component_directory,
    )
    .unwrap();
    let current = decode_projection_current(&initial.current).unwrap();
    current.validate_against(&generation).unwrap();
    assert_eq!(generation.roots.len(), 2);
    assert_eq!(
        generation
            .roots
            .iter()
            .filter(|root| root.component == ComponentIdentity::SourceRecords)
            .count(),
        1
    );
    assert_eq!(
        generation
            .roots
            .iter()
            .filter(|root| root.component == ComponentIdentity::DocumentHead)
            .count(),
        1
    );
    assert_eq!(current.next_offset, 2);
    assert_eq!(current.through_atomic_position, 10);
    assert_eq!(generation.query_stream_root.run_count, 1);
    let initial_directory = directory(&generation, initial.stream_pages.clone());
    let descriptors = decode_component_stream(&initial_directory).unwrap();
    assert_eq!(descriptors.len(), 2);
    for (index, descriptor) in descriptors.iter().enumerate() {
        assert_eq!(descriptor.sequence, index as u64 + 1);
        assert_eq!(descriptor.source_start_offset, 0);
        assert_eq!(descriptor.next_offset, 2);
        assert_eq!(descriptor.through_atomic_position, 10);
    }
    let mut artifacts = initial
        .packs
        .iter()
        .map(|pack| (pack.hash, pack.bytes.clone()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        resolve_component_record_from_verified_artifacts(&initial_directory, &artifacts, key(1))
            .unwrap(),
        Some(b"first".to_vec())
    );
    assert_eq!(
        resolve_component_record_from_verified_artifacts(&initial_directory, &artifacts, key(2))
            .unwrap(),
        Some(b"second".to_vec())
    );
    let stream_pages = initial
        .stream_pages
        .iter()
        .map(|page| (page.hash, page.bytes.to_vec()))
        .collect::<BTreeMap<_, _>>();
    let query_pages = initial
        .query_stream_pages
        .iter()
        .map(|page| (page.hash, page.bytes.to_vec()))
        .collect::<BTreeMap<_, _>>();
    let successor = prepare_atomic_projection_generation(
        partition,
        [6; 32],
        Some((&generation, initial.generation.hash)),
        2,
        3,
        11,
        Vec::new(),
        vec![child(&[(1, Some(b"updated"))]), child(&[(2, None)])],
        PreparedQueryMutationBatch::default(),
        QueryBlockLimits::default_for_memory(),
        query_credits(1024 * 1024),
        pack_credits(1024 * 1024),
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
        &successor.generation.bytes,
        &successor.generation.component_directory,
    )
    .unwrap();
    let next_current = decode_projection_current(&successor.current).unwrap();
    next_current.validate_against(&next).unwrap();
    assert_eq!(next_current.next_offset, 3);
    assert_eq!(next_current.through_atomic_position, 11);
    assert_eq!(next.roots.len(), 2);
    assert_eq!(
        next.roots
            .iter()
            .filter(|root| root.component == ComponentIdentity::SourceRecords)
            .count(),
        1
    );
    assert_eq!(
        next.root(ComponentIdentity::DocumentHead),
        generation.root(ComponentIdentity::DocumentHead)
    );
    assert_eq!(next.query_stream_root.run_count, 2);
    let mut all_pages = initial.stream_pages;
    all_pages.extend(successor.stream_pages);
    let next_directory = directory(&next, all_pages);
    let descriptors = decode_component_stream(&next_directory).unwrap();
    assert_eq!(descriptors.len(), 4);
    for (index, descriptor) in descriptors.iter().enumerate() {
        assert_eq!(descriptor.sequence, index as u64 + 1);
        assert_eq!(
            (
                descriptor.source_start_offset,
                descriptor.next_offset,
                descriptor.through_atomic_position
            ),
            if index < 2 { (0, 2, 10) } else { (2, 3, 11) }
        );
    }
    artifacts.extend(
        successor
            .packs
            .into_iter()
            .map(|pack| (pack.hash, pack.bytes)),
    );
    assert_eq!(
        resolve_component_record_from_verified_artifacts(&next_directory, &artifacts, key(1))
            .unwrap(),
        Some(b"updated".to_vec())
    );
    assert_eq!(
        resolve_component_record_from_verified_artifacts(&next_directory, &artifacts, key(2))
            .unwrap(),
        None
    );
}

#[test]
fn split_publication_rejects_duplicate_overlapping_and_reversed_child_ranges() {
    for children in [
        vec![child(&[(1, Some(b"a"))]), child(&[(1, Some(b"a"))])],
        vec![
            child(&[(1, Some(b"a")), (3, Some(b"c"))]),
            child(&[(2, Some(b"b"))]),
        ],
        vec![child(&[(2, Some(b"b"))]), child(&[(1, Some(b"a"))])],
    ] {
        let result = prepare_atomic_projection_generation(
            partition([9; 32]),
            [6; 32],
            None,
            0,
            2,
            10,
            Vec::new(),
            children,
            PreparedQueryMutationBatch::default(),
            QueryBlockLimits::default_for_memory(),
            query_credits(1024 * 1024),
            pack_credits(1024 * 1024),
            |_| -> Result<Vec<u8>, IndexError> {
                panic!("invalid siblings must fail before page reads")
            },
            |_| -> Result<Vec<u8>, IndexError> {
                panic!("invalid siblings must fail before query page reads")
            },
        );
        assert!(matches!(result, Err(IndexError::InvalidDefinition(_))));
    }
}
