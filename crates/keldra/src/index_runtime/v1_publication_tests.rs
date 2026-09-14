use keldra_index::v1::{
    CanonicalRecipeState, DocumentHead, IndexingMemoryCredits, IndexingMemoryLimits,
    IndexingMemoryStage, PreparedQueryMembershipDelta, PreparedQueryMutationBatch,
    ProjectedDocumentState, ProjectionMutationBuffer, ProjectionPackCredits, QueryBlockCredits,
    QueryBlockLimits, QueryDocumentGate, RecipeIdentity, StableDocumentKey, pack_component_deltas,
    prepare_atomic_projection_generation,
};

use super::*;
use crate::index_runtime::publication::IndexArtifactOutcome;

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
        .map(|artifact| (artifact.path.clone(), artifact.hash, artifact.bytes.clone()))
        .collect()
}

#[test]
fn prepared_component_deltas_become_exact_keyed_cache_updates() {
    let scope = [11; 32];
    let recipe = RecipeIdentity::new([12; 32]).unwrap();
    let state = ProjectedDocumentState::new(
        scope,
        DocumentHead::new(scope, "objects/one".into(), 0, 7, None, true).unwrap(),
        vec![CanonicalRecipeState::new(recipe, vec![1]).unwrap()],
        Vec::new(),
    )
    .unwrap();
    let stable_key = state.head.stable_key;
    let mut buffer = ProjectionMutationBuffer::new(1024 * 1024).unwrap();
    buffer
        .apply_source_states(scope, "objects/one", 7, vec![state], Vec::new())
        .unwrap();
    let (packs, _) = pack_component_deltas(buffer.seal().unwrap(), pack_credits())
        .unwrap()
        .into_parts();
    let updates = projection_state_updates(&packs).unwrap();

    assert!(updates.iter().any(|(key, value)| {
        *key == projection_state_record_key(ComponentIdentity::DocumentHead, stable_key)
            && value.is_some()
    }));
    assert!(updates.iter().any(|(key, value)| {
        *key == projection_state_record_key(ComponentIdentity::SourceRecords, stable_key)
            && value.is_some()
    }));
}

#[test]
fn projection_cache_partition_key_uses_only_stable_logical_identity() {
    assert_eq!(
        projection_state_partition_key(1, 2, partition()),
        projection_state_partition_key(
            1,
            2,
            ProjectionPartitionIdentity::new([7; 32], 1, [9; 32], 3, 30, 40).unwrap()
        )
    );
    assert_ne!(
        projection_state_partition_key(1, 2, partition()),
        projection_state_partition_key(
            1,
            2,
            ProjectionPartitionIdentity::new([6; 32], 1, [8; 32], 2, 3, 4).unwrap()
        )
    );
    assert_ne!(
        projection_state_partition_key(1, 2, partition()),
        projection_state_partition_key(
            1,
            2,
            ProjectionPartitionIdentity::new([7; 32], 2, [8; 32], 2, 3, 4).unwrap()
        )
    );
    assert_ne!(
        projection_state_partition_key(1, 2, partition()),
        projection_state_partition_key(2, 2, partition())
    );
    assert_ne!(
        projection_state_partition_key(1, 2, partition()),
        projection_state_partition_key(1, 3, partition())
    );
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
        2,
        "the gate block and its run descriptor must both be immutable"
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
            bytes: vec![0; length],
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
