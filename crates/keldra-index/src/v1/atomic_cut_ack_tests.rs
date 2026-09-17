use super::*;
use crate::v1::*;

fn credits() -> QueryBlockCredits {
    let bytes = 4 * 1024 * 1024;
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
            .acquire(IndexingMemoryStage::WorkerScratch, bytes)
            .unwrap(),
    )
}

fn partition() -> ProjectionPartitionIdentity {
    ProjectionPartitionIdentity::new([1; 32], 1, [2; 32], 1, 1, 1).unwrap()
}

#[test]
fn atomic_ack_preserves_live_gate_blocks_component_roots_and_old_cut() {
    let limits = QueryBlockLimits::default_for_memory();
    let mut admission = credits();
    let gate = QueryDocumentGate {
        document: StableDocumentKey::from_bytes([7; 32]).unwrap(),
        material_source_version: 11,
        current_source_version: 11,
        live: true,
        source_path: Some("objects/one".into()),
        canonical_source_path: None,
        result_path: Some("objects/one".into()),
        result_version: 11,
    };
    let record = encode_document_gate(gate.clone()).unwrap();
    let block = encode_query_block(
        QueryBlockKind::Gate,
        RecipeIdentity::new([8; 32]).unwrap(),
        &[record.clone()],
        limits,
        &mut admission,
    )
    .unwrap();
    let data = block.bytes.clone();
    let logical = block.descriptor;
    let packs = std::sync::Arc::new(
        ArtifactPackTable::new(vec![ArtifactPackReference {
            ordinal: 0,
            canonical_path: "_keldra/index-projections/v1/test.pack".into(),
            object_version: 11,
            hash: logical.hash,
            length: logical.encoded_bytes,
        }])
        .unwrap(),
    );
    let descriptor = ProjectionQueryRunDescriptor {
        partition: partition(),
        physical_catalog_generation: [3; 32],
        sequence: 1,
        source_start_offset: 1,
        next_offset: 2,
        through_atomic_position: 10,
        pack_table: packs.clone(),
        memory_lease: SegmentMemoryLease::default(),
        blocks: vec![QueryBlockDescriptor {
            kind: logical.kind,
            recipe: logical.recipe,
            minimum_key: logical.minimum_key,
            maximum_key: logical.maximum_key,
            hash: logical.hash,
            encoded_bytes: logical.encoded_bytes,
            records: logical.records,
            documents: logical.documents,
            pack_table: packs,
            locator: ArtifactPackLocator {
                ordinal: 0,
                offset: 0,
                encoded_bytes: logical.encoded_bytes,
                logical_bytes: logical.encoded_bytes,
                checksum: logical.hash,
            },
        }],
    };
    let run = encode_projection_query_run(&descriptor, limits, &mut admission).unwrap();
    let reference = QueryRunReference {
        hash: run.hash,
        encoded_bytes: run.bytes.len() as u64,
        sequence: 1,
        level: 2,
        source_start_offset: 1,
        next_offset: 2,
        through_atomic_position: 10,
    };
    // A persisted compacted run is encoded natively, not inserted through
    // the fresh-mini-run append API (which correctly requires level zero).
    let page = encode_query_run_page(QueryRunPage::Leaf(vec![reference])).unwrap();
    let stream = PreparedQueryRunAppend {
        root: ProjectionQueryStreamRoot {
            stream_root_hash: page.hash,
            stream_root_encoded_bytes: page.bytes.len() as u64,
            run_count: 1,
            first_sequence: 1,
            last_sequence: 1,
            source_start_offset: 1,
            next_offset: 2,
            through_atomic_position: 10,
        },
        pages: vec![page],
    };
    // Encoded component bytes include both data and directory bytes.
    let root =
        ComponentRoot::new(ComponentIdentity::DocumentHead, [9; 32], 1, 200, 100, 100).unwrap();
    let old = ProjectionGeneration::initial(partition(), [3; 32], 2, 10, vec![root])
        .unwrap()
        .with_query_stream_root(stream.root)
        .unwrap();
    let encoded_old = encode_projection_generation(&old).unwrap();
    let ack = prepare_atomic_cut_acknowledgement(
        &old,
        encoded_old.hash,
        20,
        Some((reference, &run.bytes)),
        limits,
        credits(),
        |hash| {
            stream
                .pages
                .iter()
                .find(|page| page.hash == hash)
                .map(|page| page.bytes.to_vec())
                .ok_or(IndexError::Integrity)
        },
    )
    .unwrap();
    let generation =
        decode_projection_generation(&ack.generation.bytes, &ack.generation.component_directory)
            .unwrap();
    assert_eq!(generation.next_offset, old.next_offset);
    assert_eq!(generation.through_atomic_position, 20);
    assert_eq!(generation.roots, old.roots);
    assert_eq!(generation.inherited_partitions, old.inherited_partitions);
    assert_eq!(generation.previous_generation_hash, Some(encoded_old.hash));
    assert_eq!(generation.revision, old.revision + 1);
    let updated = decode_projection_query_run(
        &ack.query_run.as_ref().unwrap().bytes,
        limits,
        &mut credits(),
    )
    .unwrap();
    assert_eq!(updated.blocks, descriptor.blocks);
    assert_eq!(updated.pack_table, descriptor.pack_table);
    assert_eq!(updated.sequence, descriptor.sequence);
    assert_eq!(updated.source_start_offset, descriptor.source_start_offset);
    assert_eq!(updated.next_offset, descriptor.next_offset);
    assert_eq!(updated.through_atomic_position, 20);
    assert_eq!(
        *crate::profiled_blake3_hash!(&data).as_bytes(),
        updated.blocks[0].hash
    );
    assert_eq!(
        decode_document_gate(QueryBlockRecordRef {
            key: &record.key,
            value: &record.value,
            document: None
        })
        .unwrap(),
        gate
    );
    // Old continuations retain their exact immutable root and original descriptor.
    old.query_stream_root.validate_at(2, 10).unwrap();
    let reopened_old =
        decode_projection_generation(&encoded_old.bytes, &encoded_old.component_directory).unwrap();
    assert_eq!(reopened_old, old);
    let old_descriptor = decode_projection_query_run(&run.bytes, limits, &mut credits()).unwrap();
    assert_eq!(old_descriptor.through_atomic_position, 10);
    let mut refs = Vec::new();
    visit_query_runs_newest(
        generation.query_stream_root,
        |hash| {
            ack.query_stream_pages
                .iter()
                .find(|page| page.hash == hash)
                .map(|page| page.bytes.to_vec())
                .ok_or(IndexError::Integrity)
        },
        &mut |reference| {
            refs.push(reference);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].level, 2);
    assert_eq!(refs[0].sequence, reference.sequence);
    assert_eq!(refs[0].through_atomic_position, 20);
}

#[test]
fn atomic_ack_empty_stream_emits_no_fake_run_and_rejects_backward_cut() {
    let old = ProjectionGeneration::initial(partition(), [3; 32], 7, 10, Vec::new()).unwrap();
    let encoded = encode_projection_generation(&old).unwrap();
    let ack = prepare_atomic_cut_acknowledgement(
        &old,
        encoded.hash,
        20,
        None,
        QueryBlockLimits::default_for_memory(),
        credits(),
        |_| Err::<Vec<u8>, _>(IndexError::Integrity),
    )
    .unwrap();
    assert!(ack.query_run.is_none());
    assert!(ack.query_stream_pages.is_empty());
    let reopened =
        decode_projection_generation(&ack.generation.bytes, &ack.generation.component_directory)
            .unwrap();
    reopened.query_stream_root.validate_at(7, 20).unwrap();
    assert_eq!(reopened.next_offset, 7);
    assert!(
        prepare_atomic_cut_acknowledgement(
            &old,
            encoded.hash,
            9,
            None,
            QueryBlockLimits::default_for_memory(),
            credits(),
            |_| Err::<Vec<u8>, _>(IndexError::Integrity)
        )
        .is_err()
    );
}
