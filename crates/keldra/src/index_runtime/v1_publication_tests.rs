use keldra_index::v1::{
    ArtifactPackReference, ArtifactPackTable, IndexingMemoryCredits, IndexingMemoryLimits,
    IndexingMemoryStage, PreparedQueryMembershipDelta, PreparedQueryMutationBatch,
    ProjectionPackCredits, QueryBlockCredits, QueryBlockLimits, QueryDocumentGate, RecipeIdentity,
    StableDocumentKey, pack_component_deltas,
    prepare_atomic_projection_generation as prepare_atomic_projection_generation_packed,
    prepare_projection_query_run,
};

use super::*;
use crate::index_runtime::publication::IndexArtifactOutcome;

#[tokio::test]
async fn cancelled_readback_wait_retains_the_required_verification_task() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let mut verification = Some(V1PostCasVerification::start({
        let entered = entered.clone();
        let release = release.clone();
        move || {
            let entered = entered.clone();
            let release = release.clone();
            async move {
                entered.notify_one();
                release.notified().await;
                Ok(())
            }
        }
    }));
    entered.notified().await;
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(1),
            finish_required_post_cas_verification(&mut verification),
        )
        .await
        .is_err()
    );
    assert!(verification.as_ref().unwrap().task.is_some());
    release.notify_one();
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        finish_required_post_cas_verification(&mut verification),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(verification.is_none());
}

fn partition() -> ProjectionPartitionIdentity {
    ProjectionPartitionIdentity::new([7; 32], 1, [8; 32], 2, 3, 4).unwrap()
}

#[test]
fn observed_source_progress_is_shared_between_clones_and_replaced() {
    let observations = ObservedSourceProgress::default();
    let clone = observations.clone();
    let first = partition();
    let second = ProjectionPartitionIdentity::new([17; 32], 11, [18; 32], 12, 13, 14).unwrap();

    observations.replace(&BTreeMap::from([(first, 21), (second, 34)]));
    assert_eq!(clone.get(first), Some(21));
    assert_eq!(clone.get(second), Some(34));

    clone.replace(&BTreeMap::from([(second, 55)]));
    assert_eq!(observations.get(first), None);
    assert_eq!(observations.get(second), Some(55));
}

fn query_credits() -> QueryBlockCredits {
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
            .acquire(IndexingMemoryStage::OrderingCatalog, bytes)
            .unwrap(),
    )
}

fn pack_credits() -> ProjectionPackCredits {
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
    ProjectionPackCredits::from_pipeline_permit(
        memory
            .acquire(IndexingMemoryStage::SealScratch, bytes)
            .unwrap(),
    )
}

#[allow(clippy::too_many_arguments)]
fn prepare_atomic_projection_generation<StreamPageBytes, QueryPageBytes>(
    partition: ProjectionPartitionIdentity,
    catalog: [u8; 32],
    previous: Option<(&keldra_index::v1::ProjectionGeneration, [u8; 32])>,
    source_start_offset: u64,
    next_offset: u64,
    through_atomic_position: u64,
    inherited: Vec<keldra_index::v1::ProjectionGenerationReference>,
    deltas: Vec<keldra_index::v1::SealedComponentDelta>,
    batch: PreparedQueryMutationBatch,
    limits: QueryBlockLimits,
    query_credits: QueryBlockCredits,
    component_credits: ProjectionPackCredits,
    load_stream_page: impl FnMut([u8; 32]) -> Result<StreamPageBytes, keldra_index::IndexError>,
    load_query_page: impl FnMut([u8; 32]) -> Result<QueryPageBytes, keldra_index::IndexError>,
) -> Result<PreparedAtomicProjectionGeneration, keldra_index::IndexError>
where
    StreamPageBytes: AsRef<[u8]>,
    QueryPageBytes: AsRef<[u8]>,
{
    let component_packs = pack_component_deltas(deltas, component_credits)?;
    let component_table = ArtifactPackTable::new(
        component_packs
            .packs
            .iter()
            .map(|pack| ArtifactPackReference {
                ordinal: pack.ordinal,
                canonical_path: format!(
                    "_keldra/index-projections/v1/test/component-packs/{}",
                    pack.ordinal
                )
                .into(),
                object_version: u64::from(pack.ordinal) + 1,
                hash: pack.hash,
                length: pack.bytes.len() as u64,
            })
            .collect(),
    )?;
    let sequence = previous.map_or(1, |(generation, _)| {
        generation.query_stream_root.last_sequence + 1
    });
    let query = prepare_projection_query_run(
        partition,
        catalog,
        sequence,
        source_start_offset,
        next_offset,
        through_atomic_position,
        batch,
        limits,
        query_credits,
    )?;
    let query_table = test_pack_table(query.packs())?;
    prepare_atomic_projection_generation_packed(
        partition,
        catalog,
        previous,
        source_start_offset,
        next_offset,
        through_atomic_position,
        inherited,
        component_packs,
        component_table,
        query,
        query_table,
        load_stream_page,
        load_query_page,
    )
}

fn test_pack_table(
    packs: &[keldra_index::v1::UnpublishedArtifactPack],
) -> Result<ArtifactPackTable, keldra_index::IndexError> {
    ArtifactPackTable::new(
        packs
            .iter()
            .map(|pack| ArtifactPackReference {
                ordinal: pack.ordinal,
                canonical_path: format!("_keldra/index-projections/v1/test/packs/{}", pack.ordinal)
                    .into(),
                object_version: u64::from(pack.ordinal) + 1,
                hash: pack.hash,
                length: pack.bytes.len() as u64,
            })
            .collect(),
    )
}

fn prepared(next_offset: u64, through_atomic_position: u64) -> PreparedAtomicProjectionGeneration {
    prepare_atomic_projection_generation(
        partition(),
        [9; 32],
        None,
        0,
        next_offset,
        through_atomic_position,
        Vec::new(),
        Vec::new(),
        PreparedQueryMutationBatch {
            membership: Some(PreparedQueryMembershipDelta {
                recipe: RecipeIdentity::new([10; 32]).unwrap(),
                gates: vec![QueryDocumentGate {
                    document: StableDocumentKey::derive([11; 32], "objects/one", 0).unwrap(),
                    material_source_version: 1,
                    current_source_version: 1,
                    live: true,
                    source_path: Some("objects/one".into()),
                    canonical_source_path: None,
                    result_path: Some("objects/one".into()),
                    result_version: 1,
                }],
            }),
            fields: Vec::new(),
        },
        QueryBlockLimits::default_for_memory(),
        query_credits(),
        pack_credits(),
        |_| Err::<Vec<u8>, _>(keldra_index::IndexError::Integrity),
        |_| Err::<Vec<u8>, _>(keldra_index::IndexError::Integrity),
    )
    .unwrap()
}

fn artifact_fingerprints(plan: &AtomicPublicationPlan) -> Vec<(String, [u8; 32], Vec<u8>)> {
    plan.immutable
        .iter()
        .map(|artifact| {
            (
                artifact.path.clone(),
                artifact.hash,
                artifact.bytes.to_vec(),
            )
        })
        .collect()
}

#[test]
fn publication_validation_charges_decoded_tables_to_existing_admission() {
    let (payload, mut publication) = prepared(1, 11).into_publication_parts();
    let credits = publication.query_validation_credits();
    let initial_used = credits.admitted_bytes() - credits.remaining();
    let decoded = keldra_index::v1::decode_projection_query_run(
        &payload.query_run.bytes,
        QueryBlockLimits::default_for_memory(),
        credits,
    )
    .unwrap();
    let temporary = credits.admitted_bytes() - credits.remaining() - initial_used;
    assert!(
        temporary > payload.query_run.bytes.len(),
        "decoded document tables require admission beyond encoded wire bytes"
    );
    drop(decoded);
    credits.release(temporary).unwrap();
    assert_eq!(credits.admitted_bytes() - credits.remaining(), initial_used);
}

#[test]
fn atomic_plan_contains_query_artifacts_and_generation_before_current_phase() {
    let plan = plan_atomic_publication(partition(), None, prepared(1, 11)).unwrap();
    let kinds = plan
        .immutable
        .iter()
        .map(|artifact| artifact.kind)
        .collect::<Vec<_>>();

    assert!(kinds.contains(&keldra_index::v1::ProjectionArtifactKind::QueryRunPack));
    assert!(kinds.contains(&keldra_index::v1::ProjectionArtifactKind::QueryRunStreamPage));
    assert!(kinds.contains(&keldra_index::v1::ProjectionArtifactKind::Generation));
    assert!(!kinds.contains(&keldra_index::v1::ProjectionArtifactKind::Current));
    assert_eq!(
        plan.immutable
            .iter()
            .filter(|artifact| {
                artifact.kind == keldra_index::v1::ProjectionArtifactKind::QueryRunPack
            })
            .count(),
        1,
        "query data packs are published before planning; the run descriptor remains in the atomic immutable set"
    );
    assert_eq!(plan.current.next_offset, 1);
}

#[test]
fn inline_staging_windows_enforce_every_store_bound() {
    let maximum_batch_bytes = usize::try_from(MAX_DERIVED_PROGRESS_INLINE_BATCH_BYTES).unwrap();
    assert!(inline_window_fits(0, 0, 0));
    assert!(inline_window_fits(
        MAX_DERIVED_PROGRESS_INLINE_BATCH_ITEMS - 1,
        0,
        PAYLOAD_ARTIFACT_CHUNK_BYTES,
    ));
    assert!(!inline_window_fits(
        MAX_DERIVED_PROGRESS_INLINE_BATCH_ITEMS,
        0,
        1,
    ));
    assert!(inline_window_fits(
        7,
        7 * PAYLOAD_ARTIFACT_CHUNK_BYTES,
        PAYLOAD_ARTIFACT_CHUNK_BYTES,
    ));
    assert!(!inline_window_fits(8, maximum_batch_bytes, 1));
    assert!(!inline_window_fits(0, 0, PAYLOAD_ARTIFACT_CHUNK_BYTES + 1));
}

#[test]
fn mixed_inline_and_chunked_artifacts_keep_stage_order() {
    let chunked = PAYLOAD_ARTIFACT_CHUNK_BYTES + 1;
    assert_eq!(
        immutable_stage_windows([1, 2, chunked, 3, chunked + 1, 4]).unwrap(),
        vec![
            ImmutableStageWindow::Inline { items: 2, bytes: 3 },
            ImmutableStageWindow::Unary { bytes: chunked },
            ImmutableStageWindow::Inline { items: 1, bytes: 3 },
            ImmutableStageWindow::Unary { bytes: chunked + 1 },
            ImmutableStageWindow::Inline { items: 1, bytes: 4 },
        ]
    );
}

#[test]
fn parallel_stage_work_preserves_window_and_artifact_order() {
    let lengths = [1, 2, PAYLOAD_ARTIFACT_CHUNK_BYTES + 1, 3];
    let artifacts = lengths
        .into_iter()
        .enumerate()
        .map(|(ordinal, length)| ArtifactBytes {
            path: format!("artifact-{ordinal}"),
            kind: keldra_index::v1::ProjectionArtifactKind::Pack,
            hash: [u8::try_from(ordinal).unwrap(); 32],
            bytes: vec![0; length].into(),
        })
        .collect();
    let work = immutable_stage_work(artifacts).unwrap();

    assert_eq!(
        work.iter().map(|(ordinal, _)| *ordinal).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        work.into_iter()
            .flat_map(|(_, (_, artifacts))| artifacts.into_iter().map(|artifact| artifact.path))
            .collect::<Vec<_>>(),
        vec!["artifact-0", "artifact-1", "artifact-2", "artifact-3"]
    );
}

#[test]
fn immutable_stage_work_moves_shared_bytes_without_copying() {
    let bytes = Bytes::from(vec![0x5a; 1_024]);
    let allocation = bytes.as_ptr();
    let artifacts = vec![ArtifactBytes {
        path: "shared-artifact".into(),
        kind: keldra_index::v1::ProjectionArtifactKind::Pack,
        hash: [7; 32],
        bytes,
    }];
    let work = immutable_stage_work(artifacts).unwrap();
    let staged = &work[0].1.1[0].bytes;
    assert_eq!(staged.as_ptr(), allocation);
}

#[test]
fn inline_artifacts_partition_at_byte_and_item_limits() {
    let maximum_batch_bytes = usize::try_from(MAX_DERIVED_PROGRESS_INLINE_BATCH_BYTES).unwrap();
    assert_eq!(
        immutable_stage_windows(std::iter::repeat_n(PAYLOAD_ARTIFACT_CHUNK_BYTES, 9)).unwrap(),
        vec![
            ImmutableStageWindow::Inline {
                items: 8,
                bytes: maximum_batch_bytes,
            },
            ImmutableStageWindow::Inline {
                items: 1,
                bytes: PAYLOAD_ARTIFACT_CHUNK_BYTES,
            },
        ]
    );
    assert_eq!(
        immutable_stage_windows(std::iter::repeat_n(
            1,
            MAX_DERIVED_PROGRESS_INLINE_BATCH_ITEMS + 1,
        ))
        .unwrap(),
        vec![
            ImmutableStageWindow::Inline {
                items: MAX_DERIVED_PROGRESS_INLINE_BATCH_ITEMS,
                bytes: MAX_DERIVED_PROGRESS_INLINE_BATCH_ITEMS,
            },
            ImmutableStageWindow::Inline { items: 1, bytes: 1 },
        ]
    );
}

#[test]
fn current_phase_requires_every_immutable_publication() {
    let successful = vec![
        Ok(IndexArtifactOutcome {
            version: VersionId(1),
            replayed: false,
        }),
        Ok(IndexArtifactOutcome {
            version: VersionId(2),
            replayed: true,
        }),
    ];
    assert!(require_all_immutable_publications(successful).is_ok());

    let failed = vec![
        Ok(IndexArtifactOutcome {
            version: VersionId(1),
            replayed: false,
        }),
        Err(Status::unavailable("injected immutable failure")),
    ];
    let mut current_attempted = false;
    if require_all_immutable_publications(failed).is_ok() {
        current_attempted = true;
    }

    assert!(!current_attempted);
}

#[test]
fn replay_builds_identical_content_paths_and_commands() {
    let first = plan_atomic_publication(partition(), None, prepared(1, 11)).unwrap();
    let replay = plan_atomic_publication(partition(), None, prepared(1, 11)).unwrap();
    assert_eq!(
        artifact_fingerprints(&first),
        artifact_fingerprints(&replay)
    );

    let blob = BlobRef {
        hash: *keldra_index::profiled_blake3_hash!(&first.current_bytes).as_bytes(),
        length: first.current_bytes.len() as u64,
    };
    let first_request = request(
        "tenant",
        "bucket",
        1,
        2,
        projection_routing_id(partition()),
        projection_current_path(partition()),
        blob.clone(),
        None,
    );
    let replay_request = request(
        "tenant",
        "bucket",
        1,
        2,
        projection_routing_id(partition()),
        projection_current_path(partition()),
        blob,
        None,
    );
    assert_eq!(first_request.command_id, replay_request.command_id);
}

#[test]
fn current_and_query_cut_cannot_be_crossed() {
    let mut crossed = prepared(1, 11);
    crossed.current = prepared(2, 12).current;
    let error = plan_atomic_publication(partition(), None, crossed).unwrap_err();
    assert_eq!(error.code(), tonic::Code::DataLoss);
}

#[test]
fn empty_source_cut_successor_preserves_native_live_documents_and_component_roots() {
    use keldra_index::v1::{
        DocumentHead, ProjectedDocumentState, ProjectionMutationBuffer, QueryBlockCursor,
        QueryBlockKind, decode_projection_current, decode_projection_generation,
        decode_projection_query_run, visit_live_gates, visit_query_runs_newest,
    };

    let recipe = RecipeIdentity::new([10; 32]).unwrap();
    let document = StableDocumentKey::derive([11; 32], "objects/one", 0).unwrap();
    let gate = QueryDocumentGate {
        document,
        material_source_version: 1,
        current_source_version: 1,
        live: true,
        source_path: Some("objects/one".into()),
        canonical_source_path: None,
        result_path: Some("objects/one".into()),
        result_version: 1,
    };
    let mut buffer = ProjectionMutationBuffer::new(16 * 1024).unwrap();
    buffer
        .apply_state(
            &ProjectedDocumentState::new(
                [11; 32],
                DocumentHead::new([11; 32], "objects/one".into(), 0, 1, None, true).unwrap(),
                Vec::new(),
                Vec::new(),
            )
            .unwrap(),
        )
        .unwrap();
    let limits = QueryBlockLimits::default_for_memory();
    let first = prepare_atomic_projection_generation(
        partition(),
        [9; 32],
        None,
        0,
        2,
        11,
        Vec::new(),
        buffer.seal().unwrap(),
        PreparedQueryMutationBatch {
            membership: Some(PreparedQueryMembershipDelta {
                recipe,
                gates: vec![gate.clone()],
            }),
            fields: Vec::new(),
        },
        limits,
        query_credits(),
        pack_credits(),
        |_| Err::<Vec<u8>, _>(keldra_index::IndexError::Integrity),
        |_| Err::<Vec<u8>, _>(keldra_index::IndexError::Integrity),
    )
    .unwrap();
    let previous = decode_projection_generation(
        &first.generation.bytes,
        &first.generation.component_directory,
    )
    .unwrap();
    assert!(!previous.roots.is_empty());
    let mut query_pages = first
        .query_stream_pages
        .iter()
        .map(|page| (page.hash, page.bytes.to_vec()))
        .collect::<BTreeMap<_, _>>();
    let stream_pages = first
        .stream_pages
        .iter()
        .map(|page| (page.hash, page.bytes.to_vec()))
        .collect::<BTreeMap<_, _>>();
    let next = prepare_atomic_projection_generation(
        partition(),
        [9; 32],
        Some((&previous, first.generation.hash)),
        2,
        5,
        11,
        Vec::new(),
        Vec::new(),
        PreparedQueryMutationBatch {
            membership: None,
            fields: Vec::new(),
        },
        limits,
        query_credits(),
        pack_credits(),
        |hash| {
            stream_pages
                .get(&hash)
                .cloned()
                .ok_or(keldra_index::IndexError::Integrity)
        },
        |hash| {
            query_pages
                .get(&hash)
                .cloned()
                .ok_or(keldra_index::IndexError::Integrity)
        },
    )
    .unwrap();
    let reopened =
        decode_projection_generation(&next.generation.bytes, &next.generation.component_directory)
            .unwrap();
    let current = decode_projection_current(&next.current).unwrap();
    current.validate_against(&reopened).unwrap();
    assert_eq!(reopened.partition, previous.partition);
    assert_eq!(reopened.partition, partition());
    assert_eq!(reopened.roots, previous.roots);
    assert_eq!(current.next_offset, 5);
    assert_eq!(current.through_atomic_position, 11);
    assert_eq!(
        reopened.previous_generation_hash,
        Some(first.generation.hash)
    );
    assert_eq!(reopened.query_stream_root.run_count, 2);
    query_pages.extend(
        next.query_stream_pages
            .iter()
            .map(|page| (page.hash, page.bytes.to_vec())),
    );
    let mut references = Vec::new();
    visit_query_runs_newest(
        reopened.query_stream_root,
        |hash| {
            query_pages
                .get(&hash)
                .cloned()
                .ok_or(keldra_index::IndexError::Integrity)
        },
        &mut |reference| {
            references.push(reference);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(references.len(), 2);
    assert_eq!(references[0].hash, next.query_run.hash);
    assert_eq!(references[0].source_start_offset, 2);
    assert_eq!(references[0].next_offset, 5);
    assert_eq!(references[1].hash, first.query_run.hash);
    let mut decode_credits = query_credits();
    let empty_run =
        decode_projection_query_run(&next.query_run.bytes, limits, &mut decode_credits).unwrap();
    assert!(empty_run.blocks.is_empty());
    let live_run =
        decode_projection_query_run(&first.query_run.bytes, limits, &mut decode_credits).unwrap();
    let mut cursors = live_run
        .blocks
        .iter()
        .filter(|block| block.kind == QueryBlockKind::Gate)
        .map(|block| {
            let pack = first
                .query_packs
                .iter()
                .find(|pack| pack.ordinal == block.locator.ordinal)
                .unwrap();
            let start = usize::try_from(block.locator.offset).unwrap();
            let end = start + usize::try_from(block.locator.encoded_bytes).unwrap();
            QueryBlockCursor::new(block, &pack.bytes[start..end], limits, &mut decode_credits)
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert!(!cursors.is_empty());
    let mut live = Vec::new();
    visit_live_gates(
        QueryBlockKind::Gate,
        recipe,
        &mut cursors,
        limits,
        &mut |gate| {
            live.push(gate);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(live, vec![gate]);
}

#[tokio::test]
async fn transient_post_cas_failure_retains_and_retries_mandatory_verification() {
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let verification_attempts = Arc::clone(&attempts);
    let mut verification = Some(V1PostCasVerification::start(move || {
        let attempt = verification_attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        async move {
            if attempt == 0 {
                Err(Status::unavailable("injected readback failure"))
            } else {
                Ok(())
            }
        }
    }));

    let error = finish_required_post_cas_verification(&mut verification)
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(verification.is_some());

    finish_required_post_cas_verification(&mut verification)
        .await
        .unwrap();
    assert!(verification.is_none());
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[tokio::test]
async fn integrity_post_cas_failure_clears_contained_verification() {
    let mut verification = Some(V1PostCasVerification::start(|| async {
        Err(Status::data_loss("injected exact readback mismatch"))
    }));

    let error = finish_required_post_cas_verification(&mut verification)
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::DataLoss);
    assert!(verification.is_none());
}

#[test]
fn idle_compaction_successor_preserves_cut_and_advances_generation() {
    let plan = plan_atomic_publication(partition(), None, prepared(1, 11)).unwrap();
    let loaded = LoadedV1ProjectionGeneration {
        current: plan.current,
        current_object_version: VersionId(41),
        generation: plan.generation,
    };
    let compacted = loaded.generation.clone();

    let successor =
        compaction_publication::compaction_successor_generation(&loaded, &compacted).unwrap();

    assert_eq!(successor.next_offset, loaded.generation.next_offset);
    assert_eq!(
        successor.through_atomic_position,
        loaded.generation.through_atomic_position
    );
    assert_eq!(successor.revision, loaded.generation.revision + 1);
    assert_eq!(
        successor.previous_generation_hash,
        Some(loaded.current.generation_hash)
    );
    assert_eq!(successor.query_stream_root, compacted.query_stream_root);
    let bound = compaction_publication::metadata_admission_bytes(&successor).unwrap();
    let encoded = keldra_index::v1::encode_projection_generation(&successor).unwrap();
    let retained_wire = encoded.bytes.capacity()
        + encoded
            .component_directory
            .pages
            .iter()
            .map(|page| page.bytes.capacity())
            .sum::<usize>()
        + keldra_index::v1::encode_projection_current(
            ProjectionCurrent::new(encoded.hash, &successor).unwrap(),
        )
        .unwrap()
        .capacity();
    assert!(retained_wire < bound);
    assert!(
        bound < 64 * 1024,
        "small maintenance must not reserve a configured pipeline fraction"
    );
}

#[test]
fn idle_compaction_rejects_a_changed_source_cut() {
    let plan = plan_atomic_publication(partition(), None, prepared(1, 11)).unwrap();
    let loaded = LoadedV1ProjectionGeneration {
        current: plan.current,
        current_object_version: VersionId(41),
        generation: plan.generation,
    };
    let mut compacted = loaded.generation.clone();
    compacted.next_offset += 1;

    let error =
        compaction_publication::compaction_successor_generation(&loaded, &compacted).unwrap_err();
    assert_eq!(error.code(), tonic::Code::DataLoss);
}
