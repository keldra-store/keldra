use keldra_index::v1::{
    IndexingMemoryCredits, IndexingMemoryLimits, IndexingMemoryStage, PreparedQueryMembershipDelta,
    PreparedQueryMutationBatch, ProjectionPackCredits, QueryBlockCredits, QueryBlockLimits,
    QueryDocumentGate, RecipeIdentity, StableDocumentKey, prepare_atomic_projection_generation,
};

use super::*;
use crate::index_runtime::publication::IndexArtifactOutcome;

fn partition() -> ProjectionPartitionIdentity {
    ProjectionPartitionIdentity::new([7; 32], 1, [8; 32], 2, 3, 4).unwrap()
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
