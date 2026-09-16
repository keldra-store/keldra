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

fn packed(delta: SealedComponentDelta) -> (PackedComponentDelta, ArtifactPackTable, Vec<u8>) {
    let bytes = delta.bytes.len();
    let pack = pack_component_deltas(vec![delta], test_pack_credits(bytes))
        .unwrap()
        .packs
        .remove(0);
    let table = ArtifactPackTable::new(vec![ArtifactPackReference {
        ordinal: pack.ordinal,
        canonical_path: format!("_keldra/index-projections/v1/test/packs/{}", pack.ordinal).into(),
        object_version: 1,
        hash: pack.hash,
        length: pack.bytes.len() as u64,
    }])
    .unwrap();
    (pack.deltas[0].clone(), table, pack.bytes)
}

fn pack_hash(delta: &PackedComponentDelta, table: &ArtifactPackTable) -> [u8; 32] {
    delta.locator.resolve(table).unwrap().hash
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
    reachable.push(EncodedComponentStreamPage {
        hash,
        bytes: bytes.into(),
    });
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

    let (wide_first, wide_first_table, wide_first_pack) = packed(sealed(
        component,
        &[(1, Some(b"wide-left")), (250, Some(b"wide-right"))],
    ));
    packs.insert(pack_hash(&wide_first, &wide_first_table), wide_first_pack);
    let first = append_component_stream(
        None,
        |_| Err::<Vec<u8>, _>(IndexError::Integrity),
        &wide_first,
        &wide_first_table,
        0,
        1,
        1,
    )
    .unwrap();
    pages.extend(
        first
            .new_pages
            .iter()
            .map(|page| (page.hash, page.bytes.to_vec())),
    );

    let (wide_second, wide_second_table, wide_second_pack) = packed(sealed(
        component,
        &[(1, Some(b"new-left")), (250, Some(b"new-right"))],
    ));
    packs.insert(
        pack_hash(&wide_second, &wide_second_table),
        wide_second_pack,
    );
    let second = append_component_stream(
        Some(first.root),
        |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
        &wide_second,
        &wide_second_table,
        1,
        2,
        2,
    )
    .unwrap();
    pages.extend(
        second
            .new_pages
            .iter()
            .map(|page| (page.hash, page.bytes.to_vec())),
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
        |reference| {
            packs
                .get(&reference.hash)
                .cloned()
                .ok_or(IndexError::Integrity)
        },
    )
    .unwrap();
    let (wide_l1, wide_l1_table, wide_l1_pack) = packed(first_output.into_iter().next().unwrap());
    packs.insert(pack_hash(&wide_l1, &wide_l1_table), wide_l1_pack);
    let first_splice = splice_compacted_component_runs(
        second.root,
        &first_plan,
        std::slice::from_ref(&wide_l1),
        &wide_l1_table,
        |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
    )
    .unwrap();
    pages.extend(
        first_splice
            .new_pages
            .iter()
            .map(|page| (page.hash, page.bytes.to_vec())),
    );

    let (narrow_first, narrow_first_table, narrow_first_pack) =
        packed(sealed(component, &[(100, Some(b"narrow-one"))]));
    packs.insert(
        pack_hash(&narrow_first, &narrow_first_table),
        narrow_first_pack,
    );
    let third = append_component_stream(
        Some(first_splice.root),
        |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
        &narrow_first,
        &narrow_first_table,
        2,
        3,
        3,
    )
    .unwrap();
    pages.extend(
        third
            .new_pages
            .iter()
            .map(|page| (page.hash, page.bytes.to_vec())),
    );

    let (narrow_second, narrow_second_table, narrow_second_pack) =
        packed(sealed(component, &[(101, Some(b"narrow-two"))]));
    packs.insert(
        pack_hash(&narrow_second, &narrow_second_table),
        narrow_second_pack,
    );
    let fourth = append_component_stream(
        Some(third.root),
        |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
        &narrow_second,
        &narrow_second_table,
        3,
        4,
        4,
    )
    .unwrap();
    pages.extend(
        fourth
            .new_pages
            .iter()
            .map(|page| (page.hash, page.bytes.to_vec())),
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
    let mut released_plan = second_plan.clone();
    released_plan.minimum_key = key(100);
    released_plan.maximum_key = key(101);
    let released_error = splice_compacted_component_runs(
        fourth.root,
        &released_plan,
        &[],
        &ArtifactPackTable::empty(),
        |_| Err::<Vec<u8>, _>(IndexError::Integrity),
    )
    .unwrap_err();
    assert!(matches!(released_error, IndexError::IntegrityViolation(_)));
    assert!(released_error.to_string().contains("minimum_key"));
    let second_output = compact_component_runs(
        &second_plan,
        limits,
        TombstoneCompactionPolicy::Retain,
        |reference| {
            packs
                .get(&reference.hash)
                .cloned()
                .ok_or(IndexError::Integrity)
        },
    )
    .unwrap();
    let output_bytes = second_output.iter().map(|delta| delta.bytes.len()).sum();
    let sealed_output =
        pack_component_deltas(second_output, test_pack_credits(output_bytes)).unwrap();
    let output_table = ArtifactPackTable::new(
        sealed_output
            .packs
            .iter()
            .map(|pack| ArtifactPackReference {
                ordinal: pack.ordinal,
                canonical_path: format!(
                    "_keldra/index-projections/v1/test/output-packs/{}",
                    pack.ordinal
                )
                .into(),
                object_version: u64::from(pack.ordinal) + 1,
                hash: pack.hash,
                length: pack.bytes.len() as u64,
            })
            .collect(),
    )
    .unwrap();
    let packed_output = sealed_output
        .packs
        .iter()
        .flat_map(|pack| pack.deltas.iter().cloned())
        .collect::<Vec<_>>();
    let (later, later_table, later_pack) =
        packed(sealed(component, &[(250, Some(b"later-append"))]));
    packs.insert(pack_hash(&later, &later_table), later_pack);
    let later_append = append_component_stream(
        Some(fourth.root),
        |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
        &later,
        &later_table,
        4,
        5,
        5,
    )
    .unwrap();
    pages.extend(
        later_append
            .new_pages
            .iter()
            .map(|page| (page.hash, page.bytes.to_vec())),
    );
    let rebased_splice = splice_compacted_component_runs(
        later_append.root,
        &second_plan,
        &packed_output,
        &output_table,
        |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
    )
    .unwrap();
    assert_eq!(
        rebased_splice.root.last_sequence,
        later_append.root.last_sequence
    );
    assert_eq!(
        rebased_splice.root.segment_count,
        fourth.root.segment_count - second_plan.input_count() as u64
            + packed_output.len() as u64
            + 1
    );
    let second_splice = splice_compacted_component_runs(
        fourth.root,
        &second_plan,
        &packed_output,
        &output_table,
        |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
    )
    .unwrap();
    pages.extend(
        second_splice
            .new_pages
            .iter()
            .map(|page| (page.hash, page.bytes.to_vec())),
    );
    assert!(matches!(
        splice_compacted_component_runs(
            second_splice.root,
            &second_plan,
            &packed_output,
            &output_table,
            |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
        ),
        Err(IndexError::StaleProposal)
    ));
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
        let (delta, table, pack) = packed(sealed(component, records));
        packs.insert(pack_hash(&delta, &table), pack);
        let sequence = index as u64 + 1;
        segments.push(
            descriptor(
                sequence,
                level,
                sequence - 1,
                sequence,
                sequence,
                &delta,
                &table,
            )
            .unwrap(),
        );
    }
    let directory = build_component_stream(component, &segments).unwrap();
    let pages = directory
        .pages
        .iter()
        .map(|page| (page.hash, page.bytes.to_vec()))
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
        compact_component_runs(
            &plan,
            limits,
            TombstoneCompactionPolicy::Retain,
            |reference| {
                packs
                    .get(&reference.hash)
                    .cloned()
                    .ok_or(IndexError::Integrity)
            },
        )
        .is_ok()
    );
}
