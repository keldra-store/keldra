use super::*;
use crate::v1::pack::test_pack_credits;
fn key(byte: u8) -> StableDocumentKey {
    StableDocumentKey::from_bytes([byte; 32]).unwrap()
}

fn sealed(component: ComponentIdentity, records: &[(u8, Option<&[u8]>)]) -> SealedComponentDelta {
    seal_component(
        component,
        records
            .iter()
            .map(|(key_byte, value)| (key(*key_byte), value.map(<[u8]>::to_vec)))
            .collect(),
    )
    .unwrap()
}
fn packed(delta: SealedComponentDelta) -> (PackedComponentDelta, Vec<u8>) {
    let bytes = delta.bytes.len();
    let pack = pack_component_deltas(vec![delta], test_pack_credits(bytes))
        .unwrap()
        .packs
        .remove(0);
    (pack.deltas[0].clone(), pack.bytes)
}
fn run(sequence: u64) -> ComponentSegmentDescriptor {
    ComponentSegmentDescriptor {
        sequence,
        level: 0,
        minimum_key: key(1),
        maximum_key: key(u8::MAX),
        source_start_offset: sequence - 1,
        next_offset: sequence,
        through_atomic_position: sequence,
        pack_hash: *crate::profiled_blake3_hash!(&sequence.to_le_bytes()).as_bytes(),
        pack_offset: 0,
        encoded_bytes: 100,
        logical_bytes: 80,
        records: 1,
    }
}
fn reachable_pages(
    component: ComponentIdentity,
    hash: [u8; 32],
    pages: &BTreeMap<[u8; 32], Vec<u8>>,
    reachable: &mut Vec<EncodedComponentStreamPage>,
) {
    let bytes = pages.get(&hash).unwrap().clone();
    if let Page::Branch(children) = decode_page(component, &bytes).unwrap() {
        for child in children {
            reachable_pages(component, child.hash, pages, reachable);
        }
    }
    reachable.push(EncodedComponentStreamPage { hash, bytes });
}
#[test]
fn newest_delta_wins_and_tombstones_hide_older_values() {
    let component = ComponentIdentity::Field(RecipeIdentity::new([7; 32]).unwrap());
    let (first, first_pack) = packed(sealed(component, &[(1, Some(b"old")), (2, Some(b"kept"))]));
    let (second, second_pack) = packed(sealed(component, &[(1, Some(b"new")), (2, None)]));
    let one = append_component_delta(None, &first, 0, 1, 1).unwrap();
    let two = append_component_delta(Some(&one), &second, 1, 2, 2).unwrap();
    let artifacts = [
        (first.pack_hash, first_pack),
        (second.pack_hash, second_pack),
    ]
    .into_iter()
    .collect();
    assert_eq!(
        resolve_component_record_from_verified_artifacts(&two, &artifacts, key(1)).unwrap(),
        Some(b"new".to_vec())
    );
    assert_eq!(
        resolve_component_record_from_verified_artifacts(&two, &artifacts, key(2)).unwrap(),
        None
    );
    let newest = decode_component_stream(&two).unwrap().pop().unwrap();
    let newest_pack = artifacts.get(&newest.pack_hash).unwrap();
    assert_eq!(
        lookup_component_record_in_verified_pack(component, &newest, newest_pack, key(2)).unwrap(),
        ComponentRecordLookup::Tombstone
    );
    assert_eq!(
        lookup_component_record_in_verified_pack(component, &newest, newest_pack, key(3)).unwrap(),
        ComponentRecordLookup::Missing
    );
}

#[test]
fn compaction_preserves_the_exact_newest_view() {
    let component = ComponentIdentity::Membership(RecipeIdentity::new([8; 32]).unwrap());
    let (first, first_pack) = packed(sealed(component, &[(1, Some(b"one")), (2, Some(b"two"))]));
    let (second, second_pack) = packed(sealed(component, &[(1, None), (3, Some(b"three"))]));
    let one = append_component_delta(None, &first, 0, 1, 1).unwrap();
    let two = append_component_delta(Some(&one), &second, 1, 2, 2).unwrap();
    let artifacts = [
        (first.pack_hash, first_pack),
        (second.pack_hash, second_pack),
    ]
    .into_iter()
    .collect::<BTreeMap<_, _>>();
    let pages = two
        .pages
        .iter()
        .map(|page| (page.hash, page.bytes.clone()))
        .collect::<BTreeMap<_, _>>();
    let plan = select_component_compaction(
        two.root(),
        |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
        ComponentCompactionLimits {
            l0_trigger: 2,
            maximum_input_runs: 4,
            maximum_loaded_pack_bytes: 4096,
            maximum_output_run_bytes: 1024,
        },
    )
    .unwrap()
    .unwrap();
    let compacted = compact_component_runs(
        &plan,
        ComponentCompactionLimits {
            l0_trigger: 2,
            maximum_input_runs: 4,
            maximum_loaded_pack_bytes: 4096,
            maximum_output_run_bytes: 1024,
        },
        TombstoneCompactionPolicy::Retain,
        |hash| artifacts.get(&hash).cloned().ok_or(IndexError::Integrity),
    )
    .unwrap();
    assert_eq!(compacted.len(), 1);
    let (delta, bytes) = packed(compacted.into_iter().next().unwrap());
    let spliced = splice_compacted_component_runs(two.root(), &plan, &[delta.clone()], |hash| {
        pages.get(&hash).cloned().ok_or(IndexError::Integrity)
    })
    .unwrap();
    assert!(
        spliced.new_pages.len() <= 1,
        "one-leaf compaction rewrites one page"
    );
    let mut page_store = pages;
    page_store.extend(
        spliced
            .new_pages
            .iter()
            .map(|page| (page.hash, page.bytes.clone())),
    );
    let mut reachable = Vec::new();
    reachable_pages(
        component,
        spliced.root.root_hash,
        &page_store,
        &mut reachable,
    );
    let compacted = ComponentStreamDirectory {
        component,
        root_hash: spliced.root.root_hash,
        segment_count: spliced.root.segment_count,
        first_sequence: spliced.root.first_sequence,
        last_sequence: spliced.root.last_sequence,
        encoded_bytes: spliced.root.encoded_bytes,
        logical_bytes: spliced.root.logical_bytes,
        directory_bytes: spliced.root.directory_bytes,
        pages: reachable,
    };
    let compacted_artifacts = [(delta.pack_hash, bytes)].into_iter().collect();
    for stable_key in [key(1), key(2), key(3), key(4)] {
        assert_eq!(
            resolve_component_record_from_verified_artifacts(&two, &artifacts, stable_key).unwrap(),
            resolve_component_record_from_verified_artifacts(
                &compacted,
                &compacted_artifacts,
                stable_key,
            )
            .unwrap()
        );
    }
}

#[test]
fn splice_rewrites_only_the_affected_page_path_and_refuses_unrepresentable_output() {
    let component = ComponentIdentity::DocumentHead;
    let segments = (1_u64..=300).map(run).collect::<Vec<_>>();
    let directory = build_component_stream(component, &segments).unwrap();
    let plan = ComponentCompactionPlan {
        stream_root_hash: directory.root_hash,
        component,
        inputs: segments[..2].to_vec(),
        target_level: 1,
        covers_oldest_history: false,
        minimum_key: key(1),
        maximum_key: key(u8::MAX),
        source_start_offset: 0,
        next_offset: 2,
        through_atomic_position: 2,
    };
    let (delta, _) = packed(sealed(component, &[(1, Some(b"new"))]));
    let pages = directory
        .pages
        .iter()
        .map(|page| (page.hash, page.bytes.clone()))
        .collect::<BTreeMap<_, _>>();
    let spliced =
        splice_compacted_component_runs(directory.root(), &plan, &[delta.clone()], |hash| {
            pages.get(&hash).cloned().ok_or(IndexError::Integrity)
        })
        .unwrap();
    assert_eq!(spliced.root.segment_count, 299);
    assert_eq!(spliced.new_pages.len(), 2, "leaf plus its root path");
    assert!(spliced.new_pages.len() < directory.pages.len());
    let mut invalid_coverage = plan.clone();
    invalid_coverage.minimum_key = key(2);
    let error = splice_compacted_component_runs(
        directory.root(),
        &invalid_coverage,
        &[delta.clone()],
        |_| Err::<Vec<u8>, _>(IndexError::Integrity),
    )
    .unwrap_err();
    assert!(matches!(error, IndexError::IntegrityViolation(_)));
    let message = error.to_string();
    assert!(message.contains("minimum_key"));
    assert!(message.contains("component=DocumentHead"));
    assert!(message.contains("stream_root="));
    assert!(matches!(
        splice_compacted_component_runs(
            directory.root(),
            &plan,
            &[delta.clone(), delta.clone(), delta.clone()],
            |_| Err::<Vec<u8>, _>(IndexError::Integrity)
        ),
        Err(IndexError::ResourceLimit { .. })
    ));
    let mut wrong_root = directory.root();
    wrong_root.root_hash = [42; 32];
    assert_eq!(
        splice_compacted_component_runs(wrong_root, &plan, &[delta], |_| {
            Err::<Vec<u8>, _>(IndexError::Integrity)
        }),
        Err(IndexError::Integrity)
    );
}

#[test]
fn directory_fanout_bounds_pages_for_seventy_thousand_segments() {
    let segments = (1_u64..=70_000).map(run).collect::<Vec<_>>();
    let directory = build_component_stream(ComponentIdentity::DocumentHead, &segments).unwrap();
    assert_eq!(decode_component_stream(&directory).unwrap(), segments);
    assert!(
        directory
            .pages
            .iter()
            .all(|page| page.bytes.len() < 32 * 1024)
    );
    assert!(directory.pages.len() < 600);
    let pages = directory
        .pages
        .iter()
        .map(|page| (page.hash, page.bytes.clone()))
        .collect::<BTreeMap<_, _>>();
    let root_children = component_stream_child_hashes(
        directory.component,
        pages.get(&directory.root_hash).expect("root page"),
    )
    .unwrap();
    assert!(!root_children.is_empty());
    assert!(root_children.iter().all(|hash| pages.contains_key(hash)));
}

#[test]
fn compaction_uses_level_summaries_to_prune_the_page_tree() {
    let segments = (1_u64..=20_000).map(run).collect::<Vec<_>>();
    let directory = build_component_stream(ComponentIdentity::DocumentHead, &segments).unwrap();
    let pages = directory
        .pages
        .iter()
        .map(|page| (page.hash, page.bytes.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut loads = 0usize;
    let plan = select_component_compaction(
        directory.root(),
        |hash| {
            loads += 1;
            pages.get(&hash).cloned().ok_or(IndexError::Integrity)
        },
        ComponentCompactionLimits {
            l0_trigger: 4,
            maximum_input_runs: 8,
            maximum_loaded_pack_bytes: 4096,
            maximum_output_run_bytes: 1024,
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(plan.input_count(), 4);
    assert!(
        loads < pages.len() / 4,
        "loaded {loads}/{} pages",
        pages.len()
    );
}

#[test]
fn reverse_cursor_reaches_newest_segment_without_opening_all_pages() {
    let segments = (1_u64..=300).map(run).collect::<Vec<_>>();
    let directory = build_component_stream(ComponentIdentity::DocumentHead, &segments).unwrap();
    let pages = directory
        .pages
        .iter()
        .map(|page| (page.hash, page.bytes.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut cursor = ComponentStreamReverseCursor::new(directory.root()).unwrap();
    let mut page_loads = 0;
    let newest = loop {
        match cursor.next().unwrap() {
            ComponentStreamReverseStep::LoadPage { hash } => {
                page_loads += 1;
                cursor
                    .provide_page(hash, pages.get(&hash).unwrap())
                    .unwrap();
            }
            ComponentStreamReverseStep::Segment(descriptor) => break descriptor,
            ComponentStreamReverseStep::Complete => panic!("stream unexpectedly empty"),
        }
    };
    assert_eq!(newest.sequence, 300);
    assert_eq!(page_loads, 2, "only the root and newest leaf are opened");
    assert!(page_loads < pages.len());
}

#[test]
fn append_path_copies_only_the_logarithmic_right_spine() {
    let component = ComponentIdentity::DocumentHead;
    let segments = (1_u64..=65_536).map(run).collect::<Vec<_>>();
    let previous = build_component_stream(component, &segments).unwrap();
    let pages = previous
        .pages
        .iter()
        .map(|page| (page.hash, page.bytes.clone()))
        .collect::<BTreeMap<_, _>>();
    let persisted_root = previous.component_root().unwrap();
    let reopened_root = ComponentStreamRoot::from_component_root(&persisted_root).unwrap();
    assert_eq!(reopened_root, previous.root());
    let (delta, _) = packed(sealed(component, &[(1, Some(b"next"))]));

    let appended = append_component_stream(
        Some(reopened_root),
        |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
        &delta,
        65_536,
        65_537,
        65_537,
    )
    .unwrap();

    assert_eq!(appended.root.segment_count, 65_537);
    assert_eq!(appended.new_pages.len(), 3);
    assert!(appended.new_pages.len() < previous.pages.len() / 50);
    let mut page_store = pages;
    page_store.extend(
        appended
            .new_pages
            .iter()
            .map(|page| (page.hash, page.bytes.clone())),
    );
    let mut all_pages = Vec::new();
    reachable_pages(
        component,
        appended.root.root_hash,
        &page_store,
        &mut all_pages,
    );
    let complete = ComponentStreamDirectory {
        component,
        root_hash: appended.root.root_hash,
        segment_count: appended.root.segment_count,
        first_sequence: appended.root.first_sequence,
        last_sequence: appended.root.last_sequence,
        encoded_bytes: appended.root.encoded_bytes,
        logical_bytes: appended.root.logical_bytes,
        directory_bytes: appended.root.directory_bytes,
        pages: all_pages,
    };
    let decoded = decode_component_stream(&complete).unwrap();
    assert_eq!(decoded.len(), 65_537);
    assert_eq!(decoded.last().unwrap().pack_hash, delta.pack_hash);
}

#[test]
fn artifact_identity_selects_a_preverified_pack() {
    let component = ComponentIdentity::DocumentHead;
    let (segment, _) = packed(sealed(component, &[(1, Some(b"state"))]));
    let directory = append_component_delta(None, &segment, 0, 1, 1).unwrap();
    let artifacts = BTreeMap::new();
    assert!(matches!(
        resolve_component_record_from_verified_artifacts(&directory, &artifacts, key(1)),
        Err(IndexError::Integrity)
    ));
}
