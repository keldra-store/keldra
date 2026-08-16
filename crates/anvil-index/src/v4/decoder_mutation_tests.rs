use std::fmt::Debug;
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::IndexError;

use super::{
    DocId, DocValueBlock, DocValueCell, DocumentIdentity, FieldId, IdentityBlock, LocatorEntry,
    LocatorValue, NormBlock, ObjectIdentity, PathLocatorBlock, PointBlock, PointEntry, PointValue,
    PositionEntry, PositionsBlock, PostingBlock, PostingReference, ScalarValue, SegmentStatistics,
    TERM_TYPE_STRING, TermDictionary, TermEntry, VectorBlock, canonical_term_key,
};

fn assert_hostile_count_is_bounded<T: Debug>(
    mut seed: Vec<u8>,
    count_offset: usize,
    decode: impl Fn(&[u8]) -> Result<T, IndexError>,
) {
    assert!(decode(&seed).is_ok(), "mutation seed must be canonical");
    seed[count_offset..count_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    let result = catch_unwind(AssertUnwindSafe(|| decode(&seed)));
    assert!(result.is_ok(), "hostile count must never panic");
    assert!(result.unwrap().is_err(), "hostile count must fail closed");
}

fn assert_truncations_fail_closed<T: Debug>(
    seed: &[u8],
    decode: impl Fn(&[u8]) -> Result<T, IndexError>,
) {
    for length in [0, 1, seed.len() / 2, seed.len().saturating_sub(1)] {
        let result = catch_unwind(AssertUnwindSafe(|| decode(&seed[..length])));
        assert!(result.is_ok(), "truncated payload must never panic");
        assert!(
            result.unwrap().is_err(),
            "truncated payload must fail closed"
        );
    }
}

#[test]
fn checked_component_decoders_reject_hostile_counts_without_panics() {
    let identity = IdentityBlock::new(
        DocId::new(0),
        vec![DocumentIdentity {
            source: ObjectIdentity {
                path: "source".into(),
                version: 1,
            },
            source_record: 0,
            result: None,
        }],
    )
    .unwrap()
    .encode_payload()
    .unwrap();
    assert_hostile_count_is_bounded(identity.clone(), 6, IdentityBlock::decode_payload);
    assert_truncations_fail_closed(&identity, IdentityBlock::decode_payload);

    let locator = PathLocatorBlock::new(vec![LocatorEntry {
        path: "source".into(),
        value: LocatorValue::Deleted {
            tombstone_version: 1,
        },
    }])
    .unwrap()
    .encode_payload()
    .unwrap();
    assert_hostile_count_is_bounded(locator.clone(), 2, PathLocatorBlock::decode_payload);
    assert_truncations_fail_closed(&locator, PathLocatorBlock::decode_payload);

    let term = canonical_term_key(FieldId::new(0), TERM_TYPE_STRING, &[0, b'a']).unwrap();
    let dictionary = TermDictionary::new(vec![TermEntry {
        term,
        postings: PostingReference {
            document_frequency: 1,
            total_term_frequency: 1,
            first_component_ordinal: 0,
            component_count: 1,
        },
    }])
    .unwrap()
    .encode_payload()
    .unwrap();
    assert_hostile_count_is_bounded(dictionary.clone(), 2, TermDictionary::decode_payload);
    assert_truncations_fail_closed(&dictionary, TermDictionary::decode_payload);

    let points = PointBlock::new(
        FieldId::new(0),
        vec![PointEntry {
            value: PointValue::Value(ScalarValue::Signed(1)),
            doc_id: DocId::new(0),
        }],
    )
    .unwrap()
    .encode_payload()
    .unwrap();
    assert_hostile_count_is_bounded(points.clone(), 6, PointBlock::decode_payload);
    assert_truncations_fail_closed(&points, PointBlock::decode_payload);

    let doc_values = DocValueBlock::new(
        FieldId::new(0),
        DocId::new(0),
        false,
        vec![DocValueCell::value(ScalarValue::Signed(1))],
    )
    .unwrap()
    .encode_payload()
    .unwrap();
    assert_hostile_count_is_bounded(doc_values.clone(), 10, DocValueBlock::decode_payload);
    assert_truncations_fail_closed(&doc_values, DocValueBlock::decode_payload);

    let positions = PositionsBlock::new(vec![PositionEntry {
        doc_id: DocId::new(0),
        positions: vec![1],
    }])
    .unwrap()
    .encode_payload()
    .unwrap();
    assert_hostile_count_is_bounded(positions.clone(), 2, PositionsBlock::decode_payload);
    assert_truncations_fail_closed(&positions, PositionsBlock::decode_payload);

    let norms = NormBlock::new(FieldId::new(0), DocId::new(0), vec![Some(1)])
        .unwrap()
        .encode_payload()
        .unwrap();
    assert_hostile_count_is_bounded(norms.clone(), 10, NormBlock::decode_payload);
    assert_truncations_fail_closed(&norms, NormBlock::decode_payload);

    let vectors = VectorBlock::new(
        FieldId::new(0),
        DocId::new(0),
        2,
        vec![Some(vec![1.0, 2.0])],
    )
    .unwrap()
    .encode_payload()
    .unwrap();
    assert_hostile_count_is_bounded(vectors.clone(), 10, VectorBlock::decode_payload);
    assert_truncations_fail_closed(&vectors, VectorBlock::decode_payload);

    let statistics = SegmentStatistics::new(1, 1, 0, None, Vec::new(), Vec::new())
        .unwrap()
        .encode_payload()
        .unwrap();
    assert_hostile_count_is_bounded(statistics.clone(), 27, SegmentStatistics::decode_payload);
    assert_truncations_fail_closed(&statistics, SegmentStatistics::decode_payload);

    let postings = PostingBlock::new(vec![DocId::new(1)], None)
        .unwrap()
        .encode_payload()
        .unwrap();
    assert_hostile_count_is_bounded(postings.clone(), 11, PostingBlock::decode_payload);
    assert_truncations_fail_closed(&postings, PostingBlock::decode_payload);
}
