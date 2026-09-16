use super::*;

fn key(byte: u8) -> StableDocumentKey {
    StableDocumentKey::from_bytes([byte; 32]).unwrap()
}

fn seal_records(
    component: ComponentIdentity,
    records: BTreeMap<StableDocumentKey, Option<Vec<u8>>>,
) -> Result<Vec<SealedComponentDelta>, IndexError> {
    let mut buffer = ProjectionMutationBuffer::new(128 * 1024 * 1024).unwrap();
    buffer.used_bytes =
        records.values().map(accounted_record_bytes).sum::<usize>() + COMPONENT_ACCOUNTING_BYTES;
    buffer.components.insert(component, records);
    buffer.seal()
}

#[test]
fn initial_component_seal_splits_observed_thirty_five_megabyte_baseline_without_losing_records() {
    let component = ComponentIdentity::DocumentHead;
    let encoded = 35_517_290usize;
    let overhead = component_run_encoded_bytes(component, 3, 3 * 41).unwrap();
    let values = encoded - overhead;
    let sizes = [values / 3, values / 3, values - 2 * (values / 3)];
    let input = (1..=3)
        .map(|byte| (key(byte), Some(vec![byte; sizes[usize::from(byte - 1)]])))
        .collect();
    let children = seal_records(component, input).unwrap();
    assert_eq!(children.len(), 3);
    let mut decoded = Vec::new();
    for child in &children {
        assert!(child.bytes.len() <= super::super::ARTIFACT_PACK_MAX_BYTES);
        decoded.extend(decode_component_delta(&child.bytes).unwrap());
    }
    assert_eq!(decoded.len(), 3);
    for (index, record) in decoded.iter().enumerate() {
        assert_eq!(record.stable_key, key(index as u8 + 1));
        let value = record.replacement.as_ref().unwrap();
        assert_eq!(value.len(), sizes[index]);
        assert!(value.iter().all(|byte| *byte == index as u8 + 1));
    }
    assert!(
        children
            .windows(2)
            .all(|pair| pair[0].maximum_key < pair[1].minimum_key)
    );
}

#[test]
fn initial_seal_accepts_exact_cap_and_retains_tombstone_but_refuses_oversized_single_record() {
    let component = ComponentIdentity::Membership(RecipeIdentity::new([7; 32]).unwrap());
    let cap = super::super::ARTIFACT_PACK_MAX_BYTES;
    let value_bytes = cap - component_run_encoded_bytes(component, 1, 41).unwrap();
    let children = seal_records(
        component,
        BTreeMap::from([(key(1), Some(vec![1; value_bytes])), (key(2), None)]),
    )
    .unwrap();
    assert_eq!(children.len(), 2);
    assert_eq!(children[0].bytes.len(), cap);
    let tombstone = decode_component_delta(&children[1].bytes).unwrap();
    assert_eq!(tombstone[0].stable_key, key(2));
    assert!(tombstone[0].replacement.is_none());
    assert!(
        matches!(seal_records(component, BTreeMap::from([(key(1), Some(vec![1; value_bytes + 1]))])),
        Err(IndexError::ResourceLimit { needed, limit }) if needed == cap + 1 && limit == cap)
    );
}

#[test]
fn initial_seal_wire_sizing_includes_restart_boundaries_and_all_component_headers() {
    for component in [
        ComponentIdentity::DocumentHead,
        ComponentIdentity::SourceRecords,
        ComponentIdentity::Membership(RecipeIdentity::new([1; 32]).unwrap()),
        ComponentIdentity::Field(RecipeIdentity::new([2; 32]).unwrap()),
        ComponentIdentity::Order(RecipeIdentity::new([3; 32]).unwrap()),
    ] {
        for count in [63usize, 64, 65, 127, 128, 129] {
            let records = (1..=count)
                .map(|ordinal| {
                    (
                        key(ordinal as u8),
                        if ordinal % 2 == 0 {
                            None
                        } else {
                            Some(vec![4; 17])
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let body = records
                .values()
                .map(|value| 33 + value.as_ref().map_or(0, |value| 8 + value.len()))
                .sum();
            let children = seal_records(component, records).unwrap();
            assert_eq!(children.len(), 1);
            assert_eq!(
                children[0].bytes.len(),
                component_run_encoded_bytes(component, count, body).unwrap()
            );
            assert_eq!(
                decode_component_delta(&children[0].bytes).unwrap().len(),
                count
            );
        }
    }
}
