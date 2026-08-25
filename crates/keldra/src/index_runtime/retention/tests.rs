use std::collections::{BTreeMap, VecDeque};
use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, Instant, UNIX_EPOCH};

use keldra_consensus::NodeId;
use keldra_index::v4::{
    ArtifactDescriptor, ArtifactPackReference, ComponentKind, IndexKind, SegmentComponent,
    SegmentDescriptor, SegmentIdentity, artifact_path, manifest_path,
};
use keldra_store::{BlobRef, PlacementLogId, SourceId, VersionId};

use super::*;
use crate::index_runtime::cache::{
    IndexCache, IndexCacheConfig, IndexCacheError, IndexSegmentFetcher, IndexSegmentId,
};
use crate::index_runtime::committed_view::{
    CommitManifestReference, LocatorRoot, ReleasingManifestReference,
};
use crate::index_runtime::events::{AtomicProgramWatermark, IndexBarrier, IndexSourceCursor};

struct NoFetch;

#[tonic::async_trait]
impl IndexSegmentFetcher for NoFetch {
    async fn fetch(
        &self,
        _segment: IndexSegmentId,
    ) -> Result<Box<dyn Read + Send>, IndexCacheError> {
        panic!("retention pack-table tests never materialise cache objects")
    }
}

fn pack(index_id: u64, seed: u8) -> ArtifactPackReference {
    ArtifactPackReference::new(
        index_id,
        artifact_path(index_id, [seed; 32]),
        u64::from(seed) + 1,
        [seed; 32],
        4096,
    )
    .unwrap()
}

fn artifact(pack_ordinal: u32, offset: u64, seed: u8) -> ArtifactDescriptor {
    ArtifactDescriptor::new(
        9,
        pack_ordinal,
        offset,
        120,
        0,
        ComponentKind::ROUTING_NODE,
        1,
        [seed; 32],
    )
    .unwrap()
}

fn pack_table_manifest() -> (
    IndexCommitManifest,
    ArtifactPackReference,
    ArtifactPackReference,
    ArtifactPackReference,
) {
    let shared = pack(9, 1);
    let segment_only = pack(9, 2);
    let standalone_only = pack(9, 3);
    let identity = SegmentIdentity::new(9, 1, [9; 32], 10).unwrap();
    let segment = SegmentDescriptor::new(
        identity,
        1,
        1,
        vec![shared.clone(), shared.clone(), segment_only.clone()],
        vec![
            SegmentComponent {
                role: ComponentKind::IDENTITY_TABLE,
                field_id: None,
                ordinal: 0,
                artifact: artifact(0, 0, 11),
            },
            SegmentComponent {
                role: ComponentKind::LIVE_MASK,
                field_id: None,
                ordinal: 0,
                artifact: artifact(2, 0, 12),
            },
            SegmentComponent {
                role: ComponentKind::SCORING_STATISTICS,
                field_id: None,
                ordinal: 0,
                artifact: artifact(1, 120, 13),
            },
        ],
        360,
        0,
    )
    .unwrap();
    let detached_identity = SegmentIdentity::new(9, 1, [9; 32], 11).unwrap();
    let barrier = IndexBarrier {
        fence: PlacementLogId { term: 1, index: 1 },
        atomic: AtomicProgramWatermark::new(None, None, 0),
        sources: BTreeMap::from([(
            NodeId(1),
            IndexSourceCursor {
                source: SourceId {
                    node_id: 1,
                    source_epoch: [1; 32],
                },
                next_offset: 1,
            },
        )]),
    };
    let manifest = IndexCommitManifest::new(
        9,
        3,
        1,
        IndexKind::TypedJson,
        [9; 32],
        &barrier,
        Vec::new(),
        None,
        Vec::new(),
        vec![segment],
        vec![
            LocatorRoot {
                sequence: 1,
                identity,
                artifact: artifact(1, 240, 14),
                pack_ownership: LocatorPackOwnership::Segment,
                encoded_bytes: 120,
                logical_bytes: 0,
            },
            LocatorRoot {
                sequence: 2,
                identity: detached_identity,
                artifact: artifact(2, 0, 15),
                pack_ownership: LocatorPackOwnership::Standalone(vec![
                    shared.clone(),
                    shared.clone(),
                    standalone_only.clone(),
                ]),
                encoded_bytes: 120,
                logical_bytes: 0,
            },
        ],
        600,
        0,
    )
    .unwrap();
    (manifest, shared, segment_only, standalone_only)
}

fn reference(revision: u64, published_at: u64, seed: u8) -> CommitManifestReference {
    CommitManifestReference {
        revision,
        definition_version: 1,
        schema_fingerprint: [9; 32],
        path: manifest_path(9, [seed; 32]),
        blob: BlobRef {
            hash: [seed; 32],
            length: 120,
        },
        object_version: VersionId(revision + 10),
        published_at_unix_millis: published_at,
        retained_bytes: 1_024,
    }
}

fn pointer() -> IndexCurrentPointer {
    IndexCurrentPointer::new(
        9,
        reference(3, 300, 3),
        vec![reference(2, 200, 2), reference(1, 100, 1)],
        Vec::new(),
    )
    .unwrap()
}

#[test]
fn idle_age_revisit_uses_the_earliest_retained_expiry() {
    let config = IndexRuntimeConfig::default();
    let age = config.max_commit_revision_age_hours() * 60 * 60 * 1_000;
    assert_eq!(next_age_due(&pointer(), config), Some(100 + age + 1));
    assert_eq!(minimum_due(Some(9), Some(4)), Some(4));
    assert_eq!(minimum_due(None, Some(4)), Some(4));
}

#[test]
fn retained_object_proof_is_reused_only_for_its_exact_durable_schedule() {
    let scheduled = IndexCommitRetentionDue {
        tenant_id: 1,
        bucket_id: 2,
        index_id: 9,
        definition_path: "_keldra/indexes/search".into(),
        definition_object_version: VersionId(4),
        commit_revision: 7,
        due_at_unix_millis: 100,
    };
    assert!(retention_job_matches_schedule(&scheduled, 7, &scheduled));

    let mut newer = scheduled.clone();
    newer.commit_revision = 8;
    assert!(!retention_job_matches_schedule(&scheduled, 7, &newer));
    assert!(!retention_job_matches_schedule(&newer, 7, &newer));

    let mut rescheduled = scheduled.clone();
    rescheduled.due_at_unix_millis = 200;
    assert!(!retention_job_matches_schedule(&scheduled, 7, &rescheduled));
}

#[test]
fn retained_pack_records_reject_the_wrong_index_and_invalid_rank() {
    let mut records = Vec::new();
    assert!(prepare_pack(9, 2, &pack(9, 7), &mut records).is_ok());
    assert_eq!(records.len(), 1);

    assert!(prepare_pack(8, 2, &pack(9, 7), &mut records).is_err());
    assert!(prepare_pack(9, RETENTION_COMMIT_SLOTS, &pack(9, 7), &mut records,).is_err());
}

#[test]
fn retained_pack_records_preserve_exact_version_identity() {
    let current = pack(9, 7);
    let mut previous = current.clone();
    previous.object_version += 1;
    let mut records = Vec::new();
    prepare_pack(9, 0, &current, &mut records).unwrap();
    prepare_pack(9, 0, &previous, &mut records).unwrap();

    assert_eq!(records.len(), 2);
    assert_ne!(records[0], records[1]);
    assert_eq!(
        records[0],
        RetainedObjectRecord::new(
            RETAINED_ARTIFACT_CLASS,
            current.object_content_hash,
            current.object_version,
            current.object_length,
            0,
        )
        .unwrap()
    );
}

#[tokio::test]
async fn manifest_pack_tables_protect_and_deduplicate_exact_ordinary_objects() {
    let temporary = tempfile::tempdir().unwrap();
    let cache = IndexCache::new(
        temporary.path(),
        IndexCacheConfig::new(1024 * 1024, 1024 * 1024).unwrap(),
        Arc::new(NoFetch),
    )
    .unwrap();
    let (manifest, shared, segment_only, standalone_only) = pack_table_manifest();
    let encoded = manifest.encode().unwrap();
    let blob = BlobRef {
        hash: *blake3::hash(&encoded).as_bytes(),
        length: encoded.len() as u64,
    };
    let reference = CommitManifestReference::new(
        &manifest,
        blob.clone(),
        VersionId(40),
        UNIX_EPOCH + Duration::from_secs(1),
    )
    .unwrap();
    let mut discovery = RetentionDiscovery {
        index_id: 9,
        collector: RetainedObjectCollector::new(cache.merge_scratch())
            .await
            .unwrap(),
        released_collector: RetainedObjectCollector::new(cache.merge_scratch())
            .await
            .unwrap(),
        pending_manifests: VecDeque::new(),
        pending_released: VecDeque::new(),
        releasing_roots: Vec::new(),
    };
    discovery
        .protect_manifest(2, &reference, &manifest)
        .await
        .unwrap();
    let releasing =
        ReleasingManifestReference::new(reference.clone(), UNIX_EPOCH + Duration::from_secs(2))
            .unwrap();
    discovery
        .collect_released(&releasing, &manifest)
        .await
        .unwrap();
    let released_collector = std::mem::replace(
        &mut discovery.released_collector,
        RetainedObjectCollector::new(cache.merge_scratch())
            .await
            .unwrap(),
    );
    let mut released_sort = released_collector.into_sort();
    let mut released_proof = loop {
        if let Some(proof) = released_sort
            .advance(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
        {
            break proof;
        }
    };
    assert_eq!(released_proof.len(), 4);
    assert_eq!(
        released_proof
            .lookup(
                RELEASED_MANIFEST_CLASS,
                reference.blob.hash,
                reference.object_version.0,
            )
            .await
            .unwrap(),
        Some((reference.blob.length, 0)),
    );
    for pack in [&shared, &segment_only, &standalone_only] {
        assert_eq!(
            released_proof
                .lookup(
                    RELEASED_ARTIFACT_CLASS,
                    pack.object_content_hash,
                    pack.object_version,
                )
                .await
                .unwrap(),
            Some((pack.object_length, 0)),
        );
    }

    let mut sort = discovery.collector.into_sort();
    let mut proof = loop {
        if let Some(proof) = sort
            .advance(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
        {
            break proof;
        }
    };
    for protected in [&shared, &segment_only, &standalone_only] {
        assert_eq!(
            proof
                .lookup(
                    RETAINED_ARTIFACT_CLASS,
                    protected.object_content_hash,
                    protected.object_version,
                )
                .await
                .unwrap(),
            Some((protected.object_length, 2))
        );
    }
    assert_eq!(
        proof
            .lookup(
                RETAINED_MANIFEST_CLASS,
                reference.blob.hash,
                reference.object_version.0,
            )
            .await
            .unwrap(),
        Some((reference.blob.length, 2))
    );
    assert_eq!(
        proof.contributions()[2],
        reference
            .blob
            .length
            .checked_add(shared.object_length)
            .and_then(|bytes| bytes.checked_add(segment_only.object_length))
            .and_then(|bytes| bytes.checked_add(standalone_only.object_length))
            .unwrap()
    );

    let mut sweep = RetentionSweep::new(proof, 2, released_proof);
    let candidate = classify_released_object(
        &mut sweep,
        ReleasedObject {
            path: artifact_path(9, [99; 32]),
            version: VersionId(999),
            hash: [99; 32],
            length: 4096,
            class: "artifact",
        },
    )
    .await
    .unwrap();
    assert!(
        candidate.delete,
        "known unshared releasing objects are immediate"
    );
    assert_eq!(
        candidate.due_at, None,
        "orphan safety age is not releasing age"
    );
    let manifest_candidate = classify_released_object(
        &mut sweep,
        ReleasedObject {
            path: reference.path.clone(),
            version: VersionId(777),
            hash: [77; 32],
            length: reference.blob.length,
            class: "manifest",
        },
    )
    .await
    .unwrap();
    assert!(
        !manifest_candidate.delete,
        "releasing manifest must survive until its root is CAS-unlinked"
    );
    sweep.pending.push_back(candidate);
    let roots = vec![releasing.clone()];
    assert!(release_cleanup_in_progress(
        &RetentionPhase::Sweep(sweep),
        &roots,
    ));

    let discovery = RetentionDiscovery {
        index_id: 9,
        collector: RetainedObjectCollector::new(cache.merge_scratch())
            .await
            .unwrap(),
        released_collector: RetainedObjectCollector::new(cache.merge_scratch())
            .await
            .unwrap(),
        pending_manifests: VecDeque::new(),
        pending_released: VecDeque::from([releasing.clone()]),
        releasing_roots: vec![releasing.clone()],
    };
    assert!(release_cleanup_in_progress(
        &RetentionPhase::Discover(discovery),
        &roots,
    ));

    let sort_phase = RetentionPhase::Sort(RetentionSort {
        protected: RetainedObjectCollector::new(cache.merge_scratch())
            .await
            .unwrap()
            .into_sort(),
        protected_proof: None,
        released: RetainedObjectCollector::new(cache.merge_scratch())
            .await
            .unwrap()
            .into_sort(),
        releasing_roots: vec![releasing.clone()],
    });
    assert!(release_cleanup_in_progress(&sort_phase, &roots));
    assert!(release_cleanup_in_progress(
        &RetentionPhase::Complete,
        &roots,
    ));
    assert!(release_cleanup_in_progress(
        &RetentionPhase::Initialize,
        &roots,
    ));
    assert!(!release_cleanup_in_progress(&RetentionPhase::Complete, &[],));
}

#[test]
fn completion_prioritizes_new_releasing_roots_over_retained_age() {
    let mut current = pointer();
    assert_eq!(completion_due(&current, Some(900), 100), Some(900));
    current.releasing = vec![
        ReleasingManifestReference::new(reference(4, 400, 4), UNIX_EPOCH + Duration::from_secs(1))
            .unwrap(),
    ];
    assert_eq!(
        completion_due(&current, Some(900), 100),
        Some(100),
        "new releasing work must not wait for retained-age maintenance"
    );
}

#[test]
fn retention_budget_and_schedule_reject_unbounded_ticks() {
    assert!(
        IndexRetentionBudget::new(1, MAX_RETENTION_RECORD_BYTES - 1, Duration::from_secs(1))
            .is_err()
    );
    assert!(IndexRetentionSchedule::new(Duration::ZERO, Duration::from_secs(1)).is_err());
    assert!(IndexRetentionSchedule::new(Duration::from_secs(1), Duration::ZERO).is_err());
}
