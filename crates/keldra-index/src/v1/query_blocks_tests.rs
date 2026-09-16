use super::*;
use crate::typed_json::{Collation, FieldId, FieldType};
use crate::v1::{
    ArtifactPackReference, IndexingMemoryCredits, IndexingMemoryLimits, IndexingMemoryStage,
    QueryMemoryPermit, encode_doc_value,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn test_pack_table(length: u64) -> Arc<ArtifactPackTable> {
    Arc::new(
        ArtifactPackTable::new(vec![ArtifactPackReference {
            ordinal: 0,
            canonical_path: "_keldra/index-projections/v1/test/artifacts/packs/0".into(),
            object_version: 1,
            hash: [9; 32],
            length,
        }])
        .unwrap(),
    )
}

struct TestQueryPermit {
    bytes: usize,
    drops: Arc<AtomicUsize>,
}

impl QueryMemoryPermit for TestQueryPermit {
    fn admitted_bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for TestQueryPermit {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn query_credits_retain_and_release_the_runtime_admission() {
    let drops = Arc::new(AtomicUsize::new(0));
    let credits = QueryBlockCredits::from_query_permit(Box::new(TestQueryPermit {
        bytes: 4096,
        drops: drops.clone(),
    }))
    .unwrap();
    assert_eq!(credits.remaining(), 4096);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(credits);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

fn credits(bytes: usize) -> QueryBlockCredits {
    let limits = IndexingMemoryLimits {
        hot_payload_bytes: bytes,
        worker_scratch_bytes: bytes,
        prepared_rows_bytes: bytes,
        replay_input_bytes: bytes,
        projection_accumulator_bytes: bytes,
        seal_scratch_bytes: bytes,
        ordering_catalog_bytes: bytes,
    };
    let memory = IndexingMemoryCredits::new(bytes, limits).unwrap();
    let permit = memory
        .acquire(IndexingMemoryStage::OrderingCatalog, bytes)
        .unwrap();
    QueryBlockCredits::from_pipeline_permit(permit)
}

fn recipe() -> RecipeIdentity {
    RecipeIdentity::new([7; 32]).unwrap()
}
fn partition() -> ProjectionPartitionIdentity {
    ProjectionPartitionIdentity::new([1; 32], 2, [3; 32], 2, 4, 5).unwrap()
}
fn records() -> Vec<QueryBlockRecord> {
    vec![
        QueryBlockRecord {
            key: b"alpha".to_vec(),
            value: b"one".to_vec(),
        },
        QueryBlockRecord {
            key: b"beta".to_vec(),
            value: b"two".to_vec(),
        },
    ]
}

fn key(byte: u8) -> StableDocumentKey {
    StableDocumentKey::from_bytes([byte; 32]).unwrap()
}
fn document(byte: u8) -> StableDocumentKey {
    key(byte)
}

fn record_ref(record: &QueryBlockRecord) -> QueryBlockRecordRef<'_> {
    QueryBlockRecordRef {
        key: &record.key,
        value: &record.value,
        document: None,
    }
}

fn bound(encoded: &EncodedQueryBlock) -> QueryBlockDescriptor {
    QueryBlockDescriptor {
        kind: encoded.descriptor.kind,
        recipe: encoded.descriptor.recipe,
        minimum_key: encoded.descriptor.minimum_key.clone(),
        maximum_key: encoded.descriptor.maximum_key.clone(),
        hash: encoded.descriptor.hash,
        encoded_bytes: encoded.descriptor.encoded_bytes,
        records: encoded.descriptor.records,
        locator: ArtifactPackLocator {
            ordinal: 0,
            offset: 0,
            encoded_bytes: encoded.descriptor.encoded_bytes,
            logical_bytes: encoded.descriptor.encoded_bytes,
            checksum: encoded.descriptor.hash,
        },
        pack_table: test_pack_table(encoded.descriptor.encoded_bytes),
        documents: encoded.descriptor.documents.clone(),
    }
}

#[test]
fn dense_segment_postings_share_identity_table_and_advance_with_liveness() {
    let documents = Arc::new(
        SegmentDocumentTable::new_with_versions(
            [document(1), document(2), document(3)]
                .into_iter()
                .map(|document| (document, 7)),
        )
        .unwrap(),
    );
    let records = [1, 2, 3]
        .into_iter()
        .map(|id| {
            encode_posting(QueryPosting {
                document: document(id),
                material_source_version: 7,
                live: true,
                position_block_hash: None,
                positions: 0,
            })
            .unwrap()
        })
        .collect::<Vec<_>>();
    let limits = QueryBlockLimits::default_for_memory();
    let mut credits = credits(1024 * 1024);
    let encoded = encode_query_block_with_documents(
        QueryBlockKind::Posting,
        recipe(),
        &records,
        documents.clone(),
        limits,
        &mut credits,
    )
    .unwrap();
    // Stable identities exist once in the owning run, never in these postings.
    assert!(
        !encoded
            .bytes
            .windows(32)
            .any(|window| window == document(2).bytes())
    );
    let descriptor = bound(&encoded);
    let reader = SegmentReader::from_verified_content(
        &descriptor,
        Bytes::from(encoded.bytes),
        limits,
        &mut credits,
    )
    .unwrap();
    let mask = reader
        .live_documents_for_candidates(
            [
                SegmentDocumentId(0),
                SegmentDocumentId(1),
                SegmentDocumentId(2),
            ],
            |key, version| version == 7 && key != document(2),
        )
        .unwrap();
    let mut cursor = reader
        .posting_cursor()
        .unwrap()
        .with_live_documents(&mask)
        .unwrap();
    assert_eq!(
        cursor.next().unwrap().unwrap().document,
        SegmentDocumentId(0)
    );
    assert_eq!(
        cursor
            .advance(SegmentDocumentId(1))
            .unwrap()
            .unwrap()
            .document,
        SegmentDocumentId(2)
    );
    assert!(cursor.next().unwrap().is_none());
    let wrong = Arc::new(SegmentDocumentTable::new([document(9)]).unwrap());
    assert!(
        reader
            .posting_cursor()
            .unwrap()
            .with_live_documents(&SegmentLiveDocuments::all_live(wrong))
            .is_err()
    );
}

#[test]
fn compact_point_keys_round_trip_and_keep_cross_segment_object_identity() {
    let limits = QueryBlockLimits::default_for_memory();
    let mut credits = credits(1024 * 1024);
    let original = encode_point(&QueryPoint {
        value: ScalarValue::Signed(42),
        document: document(2),
        material_source_version: 8,
        live: true,
    })
    .unwrap();
    let encoded = encode_query_block(
        QueryBlockKind::Point,
        recipe(),
        &[original.clone()],
        limits,
        &mut credits,
    )
    .unwrap();
    let descriptor = bound(&encoded);
    let mut cursor =
        QueryBlockCursor::new(&descriptor, &encoded.bytes, limits, &mut credits).unwrap();
    let compact = cursor.next().unwrap().unwrap();
    assert_eq!(compact.canonical_key(), original.key);
    assert_eq!(decode_point(compact).unwrap().document, document(2));
    let mut wrong = descriptor.clone();
    wrong.documents = Arc::new(SegmentDocumentTable::new([document(3)]).unwrap());
    assert!(QueryBlockCursor::new(&wrong, &encoded.bytes, limits, &mut credits).is_err());
}

#[test]
fn segment_identity_binds_material_version_and_columns_decode_selected_rows() {
    let first = SegmentDocumentTable::new_with_versions([(key(1), 3)]).unwrap();
    let second = SegmentDocumentTable::new_with_versions([(key(1), 4)]).unwrap();
    assert_ne!(first.identity(), second.identity());
    assert!(SegmentDocumentTable::new_with_versions([(key(1), 3), (key(1), 4)]).is_err());
    let records = [
        encode_doc_value(
            &QueryDocValue {
                document: key(1),
                material_source_version: 3,
                value: Some(vec![ScalarValue::Signed(10)]),
            },
            QueryBlockLimits::default_for_memory(),
        )
        .unwrap(),
        encode_doc_value(
            &QueryDocValue {
                document: key(2),
                material_source_version: 3,
                value: Some(vec![ScalarValue::Signed(20)]),
            },
            QueryBlockLimits::default_for_memory(),
        )
        .unwrap(),
    ];
    let mut memory = credits(1024 * 1024);
    let limits = QueryBlockLimits::default_for_memory();
    let encoded = encode_query_block(
        QueryBlockKind::DocValue,
        recipe(),
        &records,
        limits,
        &mut memory,
    )
    .unwrap();
    let descriptor = bound(&encoded);
    let reader = SegmentReader::from_verified_content(
        &descriptor,
        Bytes::from(encoded.bytes),
        limits,
        &mut memory,
    )
    .unwrap();
    assert_eq!(
        reader
            .doc_value(SegmentDocumentId(1), limits)
            .unwrap()
            .unwrap()
            .value,
        Some(vec![ScalarValue::Signed(20)])
    );
    assert!(
        reader
            .doc_value(SegmentDocumentId(2), limits)
            .unwrap()
            .is_none()
    );
    let mask = reader
        .live_documents_for_candidates([SegmentDocumentId(1)], |_, version| version == 3)
        .unwrap();
    assert!(!mask.is_live(SegmentDocumentId(0)));
    assert!(mask.is_live(SegmentDocumentId(1)));
}

#[test]
fn typed_query_records_are_canonical_and_round_trip() {
    let term = QueryTermEntry {
        term: ScalarValue::String("rust".into()),
        posting_shards: vec![QueryPostingShard {
            posting_block_hash: [1; 32],
            posting_records: 2,
            minimum_document: key(1),
            maximum_document: key(2),
        }],
    };
    let term_record = encode_term_entry(&term).unwrap();
    assert_eq!(
        decode_term_entry(
            record_ref(&term_record),
            QueryBlockLimits::default_for_memory()
        )
        .unwrap(),
        term
    );

    let posting = QueryPosting {
        document: key(2),
        material_source_version: 3,
        live: true,
        position_block_hash: Some([4; 32]),
        positions: 2,
    };
    let posting_record = encode_posting(posting).unwrap();
    assert_eq!(
        decode_posting(record_ref(&posting_record)).unwrap(),
        posting
    );

    let point = QueryPoint {
        value: ScalarValue::String("rust\0index".into()),
        document: key(3),
        material_source_version: 4,
        live: true,
    };
    let point_record = encode_point(&point).unwrap();
    assert_eq!(decode_point(record_ref(&point_record)).unwrap(), point);

    let gate = QueryDocumentGate {
        document: key(5),
        material_source_version: 6,
        current_source_version: 8,
        live: false,
        source_path: Some("objects/5.json".into()),
        canonical_source_path: None,
        result_path: Some("results/5.json".into()),
        result_version: 8,
    };
    let gate_record = encode_document_gate(gate.clone()).unwrap();
    assert_eq!(
        decode_document_gate(record_ref(&gate_record)).unwrap(),
        gate
    );

    let positions = QueryPositions {
        document: key(6),
        positions: vec![0, 3, 8],
    };
    let position_record = encode_positions(&positions).unwrap();
    assert_eq!(
        decode_positions(
            record_ref(&position_record),
            QueryBlockLimits::default_for_memory()
        )
        .unwrap(),
        positions
    );
}

#[test]
fn cursor_is_lazy_integrity_checked_and_seekable() {
    let limits = QueryBlockLimits::default_for_memory();
    let mut credits = credits(DEFAULT_QUERY_BLOCK_BYTES);
    let encoded = encode_query_block(
        QueryBlockKind::TermDictionary,
        recipe(),
        &records(),
        limits,
        &mut credits,
    )
    .unwrap();
    let descriptor = bound(&encoded);
    let mut cursor =
        QueryBlockCursor::new(&descriptor, &encoded.bytes, limits, &mut credits).unwrap();
    assert_eq!(cursor.seek_to(b"beta").unwrap().unwrap().value, b"two");
    assert!(cursor.next().unwrap().is_none());
    let mut corrupt = encoded.bytes.clone();
    corrupt[0] ^= 1;
    assert!(QueryBlockCursor::new(&descriptor, &corrupt, limits, &mut credits).is_err());
}

#[test]
fn decoded_block_reuses_encoded_storage_and_releases_fill_credits() {
    let limits = QueryBlockLimits::default_for_memory();
    let mut credits = credits(2 * DEFAULT_QUERY_BLOCK_BYTES);
    let encoded = encode_query_block(
        QueryBlockKind::TermDictionary,
        recipe(),
        &records(),
        limits,
        &mut credits,
    )
    .unwrap();
    let descriptor = bound(&encoded);
    let before_fill = credits.remaining();
    let bytes = Bytes::from(encoded.bytes);
    let decoded =
        DecodedQueryBlock::from_verified_content(&descriptor, bytes.clone(), limits, &mut credits)
            .unwrap();

    assert_eq!(credits.remaining(), before_fill);
    assert_eq!(decoded.encoded_bytes(), bytes.len());
    assert_eq!(
        decoded
            .records_from(b"beta")
            .map(|record| record.key)
            .collect::<Vec<_>>(),
        vec![b"beta".as_slice()]
    );
    assert_eq!(
        decoded
            .records()
            .map(|record| (record.key, record.value))
            .collect::<Vec<_>>(),
        vec![
            (b"alpha".as_slice(), b"one".as_slice()),
            (b"beta".as_slice(), b"two".as_slice()),
        ]
    );
}

#[test]
fn restart_table_limits_exact_seek_to_one_block_tail() {
    let limits = QueryBlockLimits::default_for_memory();
    let records = (0u16..192)
        .map(|value| QueryBlockRecord {
            key: format!("term-{value:03}").into_bytes(),
            value: value.to_be_bytes().to_vec(),
        })
        .collect::<Vec<_>>();
    let mut credits = credits(DEFAULT_QUERY_BLOCK_BYTES);
    let encoded = encode_query_block(
        QueryBlockKind::TermDictionary,
        recipe(),
        &records,
        limits,
        &mut credits,
    )
    .unwrap();
    let descriptor = bound(&encoded);
    let mut cursor =
        QueryBlockCursor::new(&descriptor, &encoded.bytes, limits, &mut credits).unwrap();
    assert_eq!(
        cursor.seek_to(b"term-151").unwrap().unwrap().key,
        b"term-151"
    );
    assert!(cursor.record_index < 192);
    assert!(cursor.record_index > 128);
}

#[test]
fn ascending_seeks_continue_from_the_current_record() {
    let limits = QueryBlockLimits::default_for_memory();
    let records = (0u16..192)
        .map(|value| QueryBlockRecord {
            key: format!("term-{value:03}").into_bytes(),
            value: value.to_be_bytes().to_vec(),
        })
        .collect::<Vec<_>>();
    let mut credits = credits(DEFAULT_QUERY_BLOCK_BYTES);
    let encoded = encode_query_block(
        QueryBlockKind::TermDictionary,
        recipe(),
        &records,
        limits,
        &mut credits,
    )
    .unwrap();
    let descriptor = bound(&encoded);
    let mut cursor =
        QueryBlockCursor::new(&descriptor, &encoded.bytes, limits, &mut credits).unwrap();

    assert_eq!(
        cursor.seek_to(b"term-130").unwrap().unwrap().key,
        b"term-130"
    );
    let after_first = cursor.record_index;
    assert_eq!(
        cursor.seek_to(b"term-131").unwrap().unwrap().key,
        b"term-131"
    );
    assert_eq!(cursor.record_index, after_first + 1);
    assert_eq!(
        cursor.seek_to(b"term-150").unwrap().unwrap().key,
        b"term-150"
    );
    assert_eq!(cursor.record_index, after_first + 20);

    // Backward and repeated seeks retain the public random-access
    // behavior by falling back to the restart table.
    assert_eq!(
        cursor.seek_to(b"term-010").unwrap().unwrap().key,
        b"term-010"
    );
    assert_eq!(
        cursor.seek_to(b"term-010").unwrap().unwrap().key,
        b"term-010"
    );
}

#[test]
fn encoded_posting_merge_keeps_old_term_tombstones_and_suppresses_delete() {
    let limits = QueryBlockLimits::default_for_memory();
    let document = key(9);
    // Build with a shared reservation so both immutable source blocks and
    // their caller-loaded cursors are charged to one pipeline admission.
    let mut reservation = credits(DEFAULT_QUERY_BLOCK_BYTES);
    let old_live = encode_query_block(
        QueryBlockKind::Posting,
        recipe(),
        &[encode_posting(QueryPosting {
            document,
            material_source_version: 1,
            live: true,
            position_block_hash: None,
            positions: 0,
        })
        .unwrap()],
        limits,
        &mut reservation,
    )
    .unwrap();
    let old_term_removal = encode_query_block(
        QueryBlockKind::Posting,
        recipe(),
        &[encode_posting(QueryPosting {
            document,
            material_source_version: 2,
            live: false,
            position_block_hash: None,
            positions: 0,
        })
        .unwrap()],
        limits,
        &mut reservation,
    )
    .unwrap();
    let old_term_removal_descriptor = bound(&old_term_removal);
    let old_live_descriptor = bound(&old_live);
    let mut inputs = [
        QueryBlockCursor::new(
            &old_term_removal_descriptor,
            &old_term_removal.bytes,
            limits,
            &mut reservation,
        )
        .unwrap(),
        QueryBlockCursor::new(
            &old_live_descriptor,
            &old_live.bytes,
            limits,
            &mut reservation,
        )
        .unwrap(),
    ];
    let mut candidates = Vec::new();
    visit_live_postings(recipe(), &mut inputs, limits, &mut |posting| {
        candidates.push(posting.document);
        Ok(())
    })
    .unwrap();
    assert!(candidates.is_empty());

    let new_term = encode_query_block(
        QueryBlockKind::Posting,
        recipe(),
        &[encode_posting(QueryPosting {
            document,
            material_source_version: 2,
            live: true,
            position_block_hash: None,
            positions: 0,
        })
        .unwrap()],
        limits,
        &mut reservation,
    )
    .unwrap();
    let new_term_descriptor = bound(&new_term);
    let mut new_inputs = [QueryBlockCursor::new(
        &new_term_descriptor,
        &new_term.bytes,
        limits,
        &mut reservation,
    )
    .unwrap()];
    visit_live_postings(recipe(), &mut new_inputs, limits, &mut |posting| {
        candidates.push(posting.document);
        Ok(())
    })
    .unwrap();
    assert_eq!(candidates, vec![document]);
}

#[test]
fn credit_refusal_happens_before_output_allocation() {
    let limits = QueryBlockLimits::default_for_memory();
    let mut credits = credits(1);
    assert!(matches!(
        encode_query_block(
            QueryBlockKind::TermDictionary,
            recipe(),
            &records(),
            limits,
            &mut credits
        ),
        Err(IndexError::ResourceLimit { .. })
    ));
    assert_eq!(credits.remaining(), 1);
}

#[test]
fn multi_numeric_facet_and_aggregate_preserve_all_values_and_tombstone() {
    let field = FieldSchema {
        id: FieldId::new(0),
        name: "scores".into(),
        source_selector: "/scores".into(),
        field_type: FieldType::UnsignedInteger,
        cardinality: Cardinality::Multi,
        allow_missing: true,
        allow_null: false,
        collation: Collation::BinaryUtf8,
        capabilities: FieldCapabilities::FACET.union(FieldCapabilities::AGGREGATE),
        analyzer: None,
        date_format: None,
    };
    let state = TypedJsonFieldState::from_selected(
        &field,
        Some(vec![
            ScalarValue::Unsigned(9),
            ScalarValue::Unsigned(2),
            ScalarValue::Unsigned(2),
        ]),
    )
    .unwrap();
    let mut memory = credits(4096);
    let created =
        prepare_typed_json_field_delta(&field, key(7), 1, Some(&state), &mut memory).unwrap();
    assert_eq!(
        created.doc_value.unwrap().value,
        Some(vec![
            ScalarValue::Unsigned(2),
            ScalarValue::Unsigned(2),
            ScalarValue::Unsigned(9),
        ])
    );
    let mut memory = credits(4096);
    let deleted = prepare_typed_json_field_delta(&field, key(7), 2, None, &mut memory).unwrap();
    assert_eq!(deleted.doc_value.unwrap().value, None);
}

#[test]
fn multi_text_positions_preserve_array_order_duplicates_and_value_gap() {
    let field = FieldSchema {
        id: FieldId::new(0),
        name: "body".into(),
        source_selector: "/body".into(),
        field_type: crate::typed_json::FieldType::Text,
        cardinality: Cardinality::Multi,
        allow_missing: true,
        allow_null: false,
        collation: Collation::BinaryUtf8,
        capabilities: FieldCapabilities::FULL_TEXT,
        analyzer: Some(crate::typed_json::Analyzer::UnicodeAlphanumericLowercase),
        date_format: None,
    };
    let state = TypedJsonFieldState::from_selected(
        &field,
        Some(vec![
            ScalarValue::String("zeta alpha".into()),
            ScalarValue::String(String::new()),
            ScalarValue::String("alpha".into()),
            ScalarValue::String("alpha".into()),
        ]),
    )
    .unwrap();

    let terms = analyzed_terms(Some(&state)).unwrap();
    assert_eq!(terms["zeta"], vec![0]);
    // Empty values still advance the boundary: the later duplicate values
    // begin at positions 4 and 6 with the fixed one-position gap.
    assert_eq!(terms["alpha"], vec![1, 4, 6]);
}

#[test]
fn descriptor_is_exact_family_partition_catalog_and_cut_binding() {
    let limits = QueryBlockLimits::default_for_memory();
    let mut credits = credits(DEFAULT_QUERY_BLOCK_BYTES);
    let block = encode_query_block(
        QueryBlockKind::TermDictionary,
        recipe(),
        &records(),
        limits,
        &mut credits,
    )
    .unwrap();
    let pack_table = test_pack_table(block.bytes.len() as u64);
    let logical = block.descriptor;
    let descriptor = ProjectionQueryRunDescriptor {
        partition: partition(),
        physical_catalog_generation: [8; 32],
        sequence: 1,
        source_start_offset: 9,
        next_offset: 10,
        through_atomic_position: 11,
        pack_table: pack_table.clone(),
        memory_lease: SegmentMemoryLease::default(),
        blocks: vec![QueryBlockDescriptor {
            kind: logical.kind,
            recipe: logical.recipe,
            minimum_key: logical.minimum_key,
            maximum_key: logical.maximum_key,
            hash: logical.hash,
            encoded_bytes: logical.encoded_bytes,
            records: logical.records,
            locator: ArtifactPackLocator {
                ordinal: 0,
                offset: 0,
                encoded_bytes: logical.encoded_bytes,
                logical_bytes: logical.encoded_bytes,
                checksum: logical.hash,
            },
            pack_table,
            documents: logical.documents,
        }],
    };
    let encoded = encode_projection_query_run(&descriptor, limits, &mut credits).unwrap();
    let encoded_again = encode_projection_query_run(&descriptor, limits, &mut credits).unwrap();
    assert_eq!(encoded, encoded_again, "root codec must be deterministic");
    assert_eq!(
        decode_projection_query_run(&encoded.bytes, limits, &mut credits).unwrap(),
        descriptor
    );

    let path = descriptor.pack_table.entries()[0].canonical_path.as_bytes();
    let path_offset = encoded
        .bytes
        .windows(path.len())
        .position(|candidate| candidate == path)
        .unwrap();
    let mut corrupt_path = encoded.bytes.clone();
    corrupt_path[path_offset] = b'X';
    assert!(decode_projection_query_run(&corrupt_path, limits, &mut credits).is_err());

    for changed in ["path", "version", "hash", "length"] {
        let mut reference = descriptor.pack_table.entries()[0].clone();
        match changed {
            "path" => {
                reference.canonical_path =
                    "_keldra/index-projections/v1/test/artifacts/packs/changed".into();
            }
            "version" => reference.object_version += 1,
            "hash" => reference.hash = [8; 32],
            "length" => reference.length += 1,
            _ => unreachable!(),
        }
        let table = Arc::new(ArtifactPackTable::new(vec![reference]).unwrap());
        let mut changed_descriptor = descriptor.clone();
        changed_descriptor.pack_table = table.clone();
        for block in &mut changed_descriptor.blocks {
            block.pack_table = table.clone();
        }
        let changed_encoded =
            encode_projection_query_run(&changed_descriptor, limits, &mut credits).unwrap();
        assert_ne!(
            changed_encoded.hash, encoded.hash,
            "{changed} must be root-bound"
        );
    }
}

#[test]
fn run_block_count_is_bounded_by_descriptor_bytes_not_query_concurrency() {
    let limits = QueryBlockLimits {
        maximum_loaded_blocks: 1,
        ..QueryBlockLimits::default_for_memory()
    };
    let pack_table = test_pack_table(4097);
    let blocks = (0_u64..4097)
        .map(|ordinal| {
            let key = ordinal.to_be_bytes().to_vec();
            let mut hash = [0_u8; 32];
            hash[24..].copy_from_slice(&ordinal.saturating_add(1).to_be_bytes());
            QueryBlockDescriptor {
                kind: QueryBlockKind::TermDictionary,
                recipe: recipe(),
                minimum_key: key.clone(),
                maximum_key: key,
                hash,
                encoded_bytes: 1,
                records: 1,
                locator: ArtifactPackLocator {
                    ordinal: 0,
                    offset: ordinal,
                    encoded_bytes: 1,
                    logical_bytes: 1,
                    checksum: hash,
                },
                pack_table: pack_table.clone(),
                documents: Arc::new(SegmentDocumentTable::default()),
            }
        })
        .collect();
    let descriptor = ProjectionQueryRunDescriptor {
        partition: partition(),
        physical_catalog_generation: [8; 32],
        sequence: 1,
        source_start_offset: 9,
        next_offset: 10,
        through_atomic_position: 11,
        pack_table,
        blocks,
        memory_lease: SegmentMemoryLease::default(),
    };
    let mut encode_credits = credits(2 * 1024 * 1024);
    let encoded = encode_projection_query_run(&descriptor, limits, &mut encode_credits)
        .expect("persistent run cardinality is independent of loaded-block concurrency");
    let mut decode_credits = credits(2 * 1024 * 1024);
    assert_eq!(
        decode_projection_query_run(&encoded.bytes, limits, &mut decode_credits).unwrap(),
        descriptor
    );
}

#[test]
fn default_run_descriptor_limit_covers_full_publication_batches() {
    assert_eq!(
        QueryBlockLimits::default_for_memory().maximum_run_descriptor_bytes,
        64 * 1024 * 1024
    );
}

#[test]
fn self_built_run_identity_failure_names_the_exact_invariant() {
    let descriptor = ProjectionQueryRunDescriptor {
        partition: partition(),
        physical_catalog_generation: [8; 32],
        sequence: 0,
        source_start_offset: 9,
        next_offset: 10,
        through_atomic_position: 11,
        pack_table: Arc::new(ArtifactPackTable::new(Vec::new()).unwrap()),
        blocks: Vec::new(),
        memory_lease: SegmentMemoryLease::default(),
    };
    let error = descriptor
        .validate(QueryBlockLimits::default_for_memory())
        .unwrap_err();
    assert!(matches!(error, IndexError::IntegrityViolation(_)));
    let message = error.to_string();
    assert!(message.contains("projection query run sequence"));
    assert!(message.contains("partition="));
    assert!(message.contains("source_range=9..10"));
}
