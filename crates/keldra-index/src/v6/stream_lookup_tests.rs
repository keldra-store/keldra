use super::*;
use crate::v6::pack::test_pack_credits;

fn key(byte: u8) -> StableDocumentKey {
    StableDocumentKey::from_bytes([byte; 32]).unwrap()
}

fn packed(
    component: ComponentIdentity,
    records: &[(u8, Option<&[u8]>)],
) -> (ComponentSegmentDescriptor, Vec<u8>) {
    let delta = seal_component(
        component,
        records
            .iter()
            .map(|(key_byte, value)| (key(*key_byte), value.map(<[u8]>::to_vec)))
            .collect(),
    )
    .unwrap();
    let bytes = delta.bytes.len();
    let pack = pack_component_deltas(vec![delta], test_pack_credits(bytes))
        .unwrap()
        .packs
        .remove(0);
    (
        descriptor(1, 0, 0, 1, 1, &pack.deltas[0]).unwrap(),
        pack.bytes,
    )
}

#[test]
fn lookup_distinguishes_value_tombstone_and_missing() {
    let component = ComponentIdentity::Field(RecipeIdentity::new([7; 32]).unwrap());
    let (descriptor, pack) = packed(component, &[(1, Some(b"value")), (2, None)]);

    assert_eq!(
        lookup_component_record_in_pack(component, &descriptor, &pack, key(1)).unwrap(),
        ComponentRecordLookup::Value(b"value".to_vec())
    );
    assert_eq!(
        lookup_component_record_in_pack(component, &descriptor, &pack, key(2)).unwrap(),
        ComponentRecordLookup::Tombstone
    );
    assert_eq!(
        lookup_component_record_in_pack(component, &descriptor, &pack, key(3)).unwrap(),
        ComponentRecordLookup::Missing
    );
}

#[test]
fn lookup_rejects_a_wrong_component_and_record_count() {
    let component = ComponentIdentity::DocumentHead;
    let (descriptor, pack) = packed(component, &[(1, Some(b"value"))]);
    assert!(matches!(
        lookup_component_record_in_pack(
            ComponentIdentity::SourceRecords,
            &descriptor,
            &pack,
            key(1),
        ),
        Err(IndexError::Integrity)
    ));

    let mut wrong_count = descriptor;
    wrong_count.records += 1;
    assert!(matches!(
        lookup_component_record_in_pack(component, &wrong_count, &pack, key(1)),
        Err(IndexError::Integrity)
    ));
}

#[test]
fn lookup_validates_records_after_an_early_match() {
    let component = ComponentIdentity::DocumentHead;
    let (mut descriptor, mut pack) =
        packed(component, &[(1, Some(b"first")), (2, Some(b"second"))]);
    let second_key = pack
        .windows(32)
        .rposition(|window| window == [2_u8; 32])
        .expect("the second record key must be encoded");
    pack[second_key..second_key + 32].copy_from_slice(&[1_u8; 32]);

    let payload_end = pack.len() - 32;
    let integrity = *blake3::hash(&pack[..payload_end]).as_bytes();
    pack[payload_end..].copy_from_slice(&integrity);
    let artifact_hash = *blake3::hash(&pack).as_bytes();
    descriptor.segment_hash = artifact_hash;
    descriptor.pack_hash = artifact_hash;

    assert!(matches!(
        lookup_component_record_in_pack(component, &descriptor, &pack, key(1)),
        Err(IndexError::UnsortedRecords)
    ));
}
