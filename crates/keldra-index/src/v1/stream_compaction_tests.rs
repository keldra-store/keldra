use super::*;
use crate::v1::pack::test_pack_credits;
use std::collections::BTreeMap;

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
fn second_compaction_uses_the_full_overlapping_target_range() {
    let component = ComponentIdentity::DocumentHead;
    let limits = ComponentCompactionLimits {
        l0_trigger: 2,
        maximum_input_runs: 4,
        maximum_loaded_pack_bytes: 64 * 1024,
        maximum_output_run_bytes: 4 * 1024,
    };
    let mut pages = BTreeMap::new();
    let mut packs = BTreeMap::new();

    let (wide_first, wide_first_pack) = packed(sealed(
        component,
        &[(1, Some(b"wide-left")), (250, Some(b"wide-right"))],
    ));
    packs.insert(wide_first.pack_hash, wide_first_pack);
    let first = append_component_stream(
        None,
        |_| Err::<Vec<u8>, _>(IndexError::Integrity),
        &wide_first,
        0,
        1,
        1,
    )
    .unwrap();
    pages.extend(
        first
            .new_pages
            .iter()
            .map(|page| (page.hash, page.bytes.clone())),
    );

    let (wide_second, wide_second_pack) = packed(sealed(
        component,
        &[(1, Some(b"new-left")), (250, Some(b"new-right"))],
    ));
    packs.insert(wide_second.pack_hash, wide_second_pack);
    let second = append_component_stream(
        Some(first.root),
        |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
        &wide_second,
        1,
        2,
        2,
    )
    .unwrap();
    pages.extend(
        second
            .new_pages
            .iter()
            .map(|page| (page.hash, page.bytes.clone())),
    );

    let first_plan = select_component_compaction(
        second.root,
        |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
        limits,
    )
    .unwrap()
    .unwrap();
    let first_output = compact_component_runs(
        &first_plan,
        limits,
        TombstoneCompactionPolicy::Retain,
        |hash| packs.get(&hash).cloned().ok_or(IndexError::Integrity),
    )
    .unwrap();
    let (wide_l1, wide_l1_pack) = packed(first_output.into_iter().next().unwrap());
    packs.insert(wide_l1.pack_hash, wide_l1_pack);
    let first_splice = splice_compacted_component_runs(
        second.root,
        &first_plan,
        std::slice::from_ref(&wide_l1),
        |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
    )
    .unwrap();
    pages.extend(
        first_splice
            .new_pages
            .iter()
            .map(|page| (page.hash, page.bytes.clone())),
    );

    let (narrow_first, narrow_first_pack) =
        packed(sealed(component, &[(100, Some(b"narrow-one"))]));
    packs.insert(narrow_first.pack_hash, narrow_first_pack);
    let third = append_component_stream(
        Some(first_splice.root),
        |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
        &narrow_first,
        2,
        3,
        3,
    )
    .unwrap();
    pages.extend(
        third
            .new_pages
            .iter()
            .map(|page| (page.hash, page.bytes.clone())),
    );

    let (narrow_second, narrow_second_pack) =
        packed(sealed(component, &[(101, Some(b"narrow-two"))]));
    packs.insert(narrow_second.pack_hash, narrow_second_pack);
    let fourth = append_component_stream(
        Some(third.root),
        |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
        &narrow_second,
        3,
        4,
        4,
    )
    .unwrap();
    pages.extend(
        fourth
            .new_pages
            .iter()
            .map(|page| (page.hash, page.bytes.clone())),
    );

    let second_plan = select_component_compaction(
        fourth.root,
        |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
        limits,
    )
    .unwrap()
    .unwrap();
    assert_eq!(second_plan.input_count(), 3);
    assert!(second_plan.covers_oldest_history);
    assert_eq!(second_plan.minimum_key, key(1));
    assert_eq!(second_plan.maximum_key, key(250));
    let second_output = compact_component_runs(
        &second_plan,
        limits,
        TombstoneCompactionPolicy::Retain,
        |hash| packs.get(&hash).cloned().ok_or(IndexError::Integrity),
    )
    .unwrap();
    let packed_output = second_output
        .into_iter()
        .map(|delta| packed(delta).0)
        .collect::<Vec<_>>();
    let second_splice =
        splice_compacted_component_runs(fourth.root, &second_plan, &packed_output, |hash| {
            pages.get(&hash).cloned().ok_or(IndexError::Integrity)
        })
        .unwrap();
    pages.extend(
        second_splice
            .new_pages
            .iter()
            .map(|page| (page.hash, page.bytes.clone())),
    );
    let mut reachable = Vec::new();
    reachable_pages(
        component,
        second_splice.root.root_hash,
        &pages,
        &mut reachable,
    );
    let compacted = ComponentStreamDirectory {
        component,
        root_hash: second_splice.root.root_hash,
        segment_count: second_splice.root.segment_count,
        first_sequence: second_splice.root.first_sequence,
        last_sequence: second_splice.root.last_sequence,
        encoded_bytes: second_splice.root.encoded_bytes,
        logical_bytes: second_splice.root.logical_bytes,
        directory_bytes: second_splice.root.directory_bytes,
        pages: reachable,
    };
    let decoded = decode_component_stream(&compacted).unwrap();
    assert_eq!(decoded.len(), packed_output.len());
    assert!(decoded.iter().all(|run| run.level == 1));
}

#[test]
fn expanded_target_range_keeps_tombstones_when_older_history_overlaps_its_flank() {
    let component = ComponentIdentity::DocumentHead;
    let limits = ComponentCompactionLimits {
        l0_trigger: 2,
        maximum_input_runs: 4,
        maximum_loaded_pack_bytes: 64 * 1024,
        maximum_output_run_bytes: 4 * 1024,
    };
    let inputs = [
        (2, &[(1, Some(b"older-l2".as_slice()))][..]),
        (
            1,
            &[
                (1, Some(b"wide-left".as_slice())),
                (250, Some(b"wide-right".as_slice())),
            ][..],
        ),
        (0, &[(100, None)][..]),
        (0, &[(101, Some(b"narrow".as_slice()))][..]),
    ];
    let mut packs = BTreeMap::new();
    let mut segments = Vec::new();
    for (index, (level, records)) in inputs.into_iter().enumerate() {
        let (delta, pack) = packed(sealed(component, records));
        packs.insert(delta.pack_hash, pack);
        let sequence = index as u64 + 1;
        segments
            .push(descriptor(sequence, level, sequence - 1, sequence, sequence, &delta).unwrap());
    }
    let directory = build_component_stream(component, &segments).unwrap();
    let pages = directory
        .pages
        .iter()
        .map(|page| (page.hash, page.bytes.clone()))
        .collect::<BTreeMap<_, _>>();

    let plan = select_component_compaction(
        directory.root(),
        |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
        limits,
    )
    .unwrap()
    .unwrap();
    assert_eq!(plan.input_count(), 3);
    assert_eq!(plan.minimum_key, key(1));
    assert_eq!(plan.maximum_key, key(250));
    assert!(!plan.covers_oldest_history);
    assert!(matches!(
        compact_component_runs(
            &plan,
            limits,
            TombstoneCompactionPolicy::DropWhenOldestHistoryCovered,
            |_| -> Result<Vec<u8>, IndexError> {
                panic!("unsafe tombstone drop must fail before packs are loaded")
            },
        ),
        Err(IndexError::InvalidDefinition(_))
    ));
    assert!(
        compact_component_runs(&plan, limits, TombstoneCompactionPolicy::Retain, |hash| {
            packs.get(&hash).cloned().ok_or(IndexError::Integrity)
        },)
        .is_ok()
    );
}
