use anvil_api::v1::{CreateIndexRequest, IndexSpecification, PathIndexSpec};
use anvil_store::{
    BlobRef, Head, MUTATION_STAMP_FORMAT, MutationStamp, ObjectHeadChange, ObjectHeadChangeKind,
    PlacementLogId, SourceId, Version, VersionId,
};

use crate::index_runtime::events::{AtomicProgramWatermark, IndexJournalChange, IndexSourceCursor};

use super::catch_up::journal_source_paths;
use super::*;

#[test]
fn publication_progress_is_narrow_and_compaction_stays_bounded() {
    assert_eq!(
        publication_admission(false),
        DerivedArtifactAdmission::PublicationProgress
    );
    assert_eq!(
        publication_admission(true),
        DerivedArtifactAdmission::Bounded
    );
    assert_eq!(compaction_admission(), DerivedArtifactAdmission::Bounded);
}

fn barrier(next_offset: u64) -> IndexBarrier {
    IndexBarrier {
        fence: PlacementLogId { term: 3, index: 7 },
        atomic: AtomicProgramWatermark::new(None, None, 0),
        sources: [(
            NodeId(1),
            IndexSourceCursor {
                source: SourceId {
                    node_id: 1,
                    source_epoch: [1; 32],
                },
                next_offset,
            },
        )]
        .into_iter()
        .collect(),
    }
}

fn journal_change(tenant_id: u64, bucket_id: u64, path: &str, offset: u64) -> IndexJournalChange {
    IndexJournalChange {
        node: NodeId(1),
        change: LocalChange::ObjectHead(ObjectHeadChange {
            offset,
            tenant_id,
            bucket_id,
            exact_path: path.to_owned(),
            path_version: VersionId(offset),
            kind: ObjectHeadChangeKind::Put,
            reference_deltas: Vec::new(),
            definition_transition: None,
            accounting_transition: None,
        }),
    }
}

fn journal_page(changes: Vec<IndexJournalChange>, next_offset: u64) -> IndexJournalPage {
    IndexJournalPage {
        changes,
        through: barrier(next_offset),
        encoded_bytes: 1,
    }
}

fn snapshot_head(path: &str, version: u64) -> IndexSourceSnapshotHead {
    IndexSourceSnapshotHead {
        tenant_id: 1,
        bucket_id: 2,
        exact_path: path.to_owned(),
        head: Head {
            version: VersionId(version),
            deleted: false,
            mutation_stamp: None,
        },
        version: Version {
            id: VersionId(version),
            blob: Some(BlobRef {
                hash: [version as u8; 32],
                length: 10,
            }),
            content_type: Some("application/json".to_owned()),
            deleted: false,
            committed_at_unix_millis: version,
        },
    }
}

#[test]
fn snapshot_frame_measurement_matches_the_streams_per_record_credit() {
    let frame = vec![snapshot_head("a.json", 1), snapshot_head("b.json", 2)];
    let encoded = frame
        .iter()
        .map(|head| serde_json::to_vec(head).unwrap().len() as u64)
        .sum::<u64>();
    let resident = std::mem::size_of::<Vec<IndexSourceSnapshotHead>>()
        + frame.capacity() * std::mem::size_of::<IndexSourceSnapshotHead>()
        + frame
            .iter()
            .map(|head| {
                head.exact_path.capacity()
                    + head
                        .version
                        .content_type
                        .as_ref()
                        .map_or(0, String::capacity)
            })
            .sum::<usize>();

    assert_eq!(
        rebuild::measure_snapshot_frame(&frame, frame.capacity()).unwrap(),
        rebuild::SnapshotFrameMeasure {
            encoded_bytes: encoded,
            resident_bytes: resident as u64,
        }
    );
    assert!(serde_json::to_vec(&frame).unwrap().len() as u64 > encoded);
}

fn definition(tenant_id: u64, bucket_id: u64, index_id: u64) -> CatalogDefinition {
    let stored = StoredIndexDefinition::create(
        format!("tenant-{tenant_id}"),
        CreateIndexRequest {
            bucket: format!("bucket-{bucket_id}"),
            name: format!("index-{index_id}"),
            path_prefix: String::new(),
            content_type: String::new(),
            specification: Some(IndexSpecification {
                specification: Some(anvil_api::v1::index_specification::Specification::Path(
                    PathIndexSpec {},
                )),
            }),
            command_id: format!("create-{tenant_id}-{bucket_id}-{index_id}"),
        },
        index_id,
    )
    .unwrap();
    CatalogDefinition::new(tenant_id, bucket_id, 1, stored).unwrap()
}

fn queue_definition(scheduler: &mut BuilderScheduler, definition: CatalogDefinition) {
    let identity = definition.identity();
    let job = BuilderJob::new(definition.clone()).unwrap();
    scheduler.entries.insert(
        identity,
        ScheduledBuilder {
            definition,
            job: Some(job),
            queued: false,
            wake_pending: false,
        },
    );
    scheduler.enqueue(identity);
}

fn queue_dirty_definition(scheduler: &mut BuilderScheduler, definition: CatalogDefinition) {
    let identity = definition.identity();
    let mut job = BuilderJob::new(definition.clone()).unwrap();
    let progress = BuilderProgress::start(job.telemetry_identity(), BuilderProgressPhase::CatchUp);
    job.phase = BuilderPhase::CatchUp(CatchUpWork {
        current: None,
        through: barrier(10),
        target: barrier(12),
        candidate: CandidateGeneration::rebuild(),
        changed: true,
        must_publish: true,
        maintenance: false,
        progress,
    });
    scheduler.entries.insert(
        identity,
        ScheduledBuilder {
            definition,
            job: Some(job),
            queued: false,
            wake_pending: false,
        },
    );
    scheduler.enqueue(identity);
}

#[test]
fn reserved_segment_matching_is_not_a_string_prefix_guess() {
    assert!(contains_reserved_segment("a/_anvil/meta.json"));
    assert!(!contains_reserved_segment("a/_anvilish/meta.json"));
}

#[test]
fn reserved_artifact_pages_have_no_generation_source_changes() {
    let page = journal_page(
        vec![
            journal_change(1, 2, "_anvil/indexes/v4/0000000000000009/current", 11),
            journal_change(
                1,
                2,
                "_anvil/indexes/v4/0000000000000009/manifests/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                12,
            ),
        ],
        13,
    );

    assert!(journal_source_paths(1, 2, "", &page).is_empty());
}

#[test]
fn irrelevant_source_progress_is_published_and_survives_reload() {
    let job = BuilderJob::new(definition(1, 2, 9)).unwrap();
    let mut work = CatchUpWork {
        current: None,
        through: barrier(10),
        target: barrier(13),
        candidate: CandidateGeneration::rebuild(),
        changed: false,
        must_publish: false,
        maintenance: false,
        progress: BuilderProgress::start(job.telemetry_identity(), BuilderProgressPhase::CatchUp),
    };
    let page = journal_page(vec![journal_change(1, 2, "outside/scope.json", 12)], 13);

    assert!(journal_source_paths(1, 2, "records/", &page).is_empty());
    record_source_page_progress(&mut work, &page);
    assert!(!work.changed);
    assert!(work.must_publish);
    assert_eq!(work.through, work.target);

    let manifest = super::super::generation::IndexGenerationManifest::new(
        9,
        2,
        1,
        anvil_index::v4::IndexKind::Path,
        job.definition.schema_fingerprint,
        &work.through,
        Vec::new(),
        work.candidate.segments,
        work.candidate.locator_roots,
        0,
        0,
    )
    .unwrap();
    let reloaded =
        super::super::generation::IndexGenerationManifest::decode(&manifest.encode().unwrap())
            .unwrap();
    assert_eq!(reloaded.barrier().unwrap(), barrier(13));
}

#[test]
fn same_kind_ready_queue_gives_the_next_definition_a_turn() {
    let mut scheduler = BuilderScheduler::default();
    let first = definition(1, 2, 9);
    let second = definition(3, 4, 9);
    let first_identity = first.identity();
    let second_identity = second.identity();
    queue_definition(&mut scheduler, first);
    queue_definition(&mut scheduler, second);

    let first_job = scheduler.pop_runnable().unwrap();
    assert_eq!(first_job.definition.identity(), first_identity);
    assert!(scheduler.pop_runnable().is_none());

    scheduler.running_kinds[kind_slot(IndexKind::Path)] = false;
    scheduler.entries.get_mut(&first_identity).unwrap().job = Some(first_job);
    scheduler.enqueue(first_identity);
    let next = scheduler.pop_runnable().unwrap();
    assert_eq!(next.definition.identity(), second_identity);
}

#[test]
fn parked_rebuild_limit_prioritizes_same_kind_catch_up() {
    let mut scheduler = BuilderScheduler::default();
    let parked_inspection = definition(1, 2, 9);
    let catch_up = definition(3, 4, 10);
    let catch_up_identity = catch_up.identity();
    queue_definition(&mut scheduler, parked_inspection);
    queue_dirty_definition(&mut scheduler, catch_up);
    scheduler.open_rebuilds[kind_slot(IndexKind::Path)] = MAX_OPEN_REBUILDS_PER_KIND;

    let selected = scheduler
        .pop_runnable()
        .expect("same-kind catch-up must run while parked rebuild admission is full");

    assert_eq!(selected.definition.identity(), catch_up_identity);
    assert!(matches!(selected.phase, BuilderPhase::CatchUp(_)));
    assert!(scheduler.pop_runnable().is_none());
}

#[test]
fn scheduler_identity_includes_tenant_and_bucket() {
    let mut scheduler = BuilderScheduler::default();
    queue_definition(&mut scheduler, definition(1, 2, 9));
    queue_definition(&mut scheduler, definition(3, 4, 9));
    assert_eq!(scheduler.entries.len(), 2);
}

#[test]
fn idle_builder_releases_its_bounded_lease() {
    let mut scheduler = BuilderScheduler::default();
    let definition = definition(1, 2, 9);
    let identity = definition.identity();
    queue_definition(&mut scheduler, definition);
    let job = scheduler.pop_runnable().unwrap();
    let metadata = WorkMetadata::from_job(&job);
    scheduler.complete_with(
        metadata,
        BuilderStep {
            job,
            disposition: BuilderDisposition::Idle,
            retention_current: None,
        },
        |_, _, _| true,
    );
    assert!(!scheduler.entries.contains_key(&identity));
    assert_eq!(scheduler.remaining_capacity(), MAX_ACTIVE_BUILDERS);
}

#[test]
fn same_definition_wake_during_active_work_reinspects_after_idle() {
    let mut scheduler = BuilderScheduler::default();
    let definition = definition(1, 2, 9);
    let identity = definition.identity();
    queue_dirty_definition(&mut scheduler, definition.clone());

    let mut job = scheduler.pop_runnable().unwrap();
    let metadata = WorkMetadata::from_job(&job);
    assert!(scheduler.entries[&identity].job.is_none());
    assert!(scheduler.record_same_definition_wake(&definition));
    assert!(scheduler.entries[&identity].wake_pending);

    job.phase = BuilderPhase::Inspect;
    scheduler.complete_with(
        metadata,
        BuilderStep {
            job,
            disposition: BuilderDisposition::Idle,
            retention_current: None,
        },
        |_, _, _| true,
    );

    assert!(!scheduler.entries[&identity].wake_pending);
    let next = scheduler
        .pop_runnable()
        .expect("the wake observed during active work must trigger reinspection");
    assert_eq!(next.definition.identity(), identity);
    assert!(matches!(next.phase, BuilderPhase::Inspect));
}

#[test]
fn successful_publish_yields_a_lease_to_a_later_assignment() {
    let mut scheduler = BuilderScheduler::default();
    for index_id in 1..=MAX_ACTIVE_BUILDERS as u64 {
        queue_dirty_definition(&mut scheduler, definition(1, 2, index_id));
    }
    let later = definition(3, 4, MAX_ACTIVE_BUILDERS as u64 + 1);
    let later_identity = later.identity();
    let catalog = IndexCatalog::default();
    catalog.upsert(later).unwrap();

    assert_eq!(scheduler.remaining_capacity(), 0);
    assert!(
        catalog
            .take(later_identity, scheduler.can_admit(later_identity))
            .unwrap()
            .is_none()
    );

    let mut published_job = scheduler.pop_runnable().unwrap();
    let metadata = WorkMetadata::from_job(&published_job);
    let (phase, disposition) = complete_publication(&mut published_job);
    published_job.phase = phase;
    scheduler.complete_with(
        metadata,
        BuilderStep {
            job: published_job,
            disposition,
            retention_current: None,
        },
        |_, _, _| true,
    );

    assert_eq!(scheduler.remaining_capacity(), 1);
    assert_eq!(scheduler.entries.len(), MAX_ACTIVE_BUILDERS - 1);
    assert!(
        scheduler
            .entries
            .values()
            .all(|entry| { entry.job.as_ref().is_some_and(BuilderJob::is_active) })
    );

    let admitted = catalog
        .take(later_identity, scheduler.can_admit(later_identity))
        .unwrap()
        .expect("later durable assignment should acquire the yielded lease");
    let CatalogChange::Upsert(later) = admitted else {
        panic!("later assignment unexpectedly became a removal");
    };
    queue_definition(&mut scheduler, later);

    assert_eq!(scheduler.remaining_capacity(), 0);
    assert!(scheduler.entries.contains_key(&later_identity));
}

#[test]
fn transient_failure_keeps_exact_work_until_its_delayed_retry() {
    let mut scheduler = BuilderScheduler::default();
    queue_dirty_definition(&mut scheduler, definition(1, 2, 9));

    let failed_job = scheduler.pop_runnable().unwrap();
    let failed_identity = failed_job.definition.identity();
    let metadata = WorkMetadata::from_job(&failed_job);
    scheduler.complete_with(
        metadata,
        BuilderStep {
            job: failed_job,
            disposition: BuilderDisposition::Retry(BUILDER_RETRY_INTERVAL),
            retention_current: None,
        },
        |_, _, _| true,
    );

    assert!(scheduler.entries.contains_key(&failed_identity));
    assert!(scheduler.pop_runnable().is_none());
    assert_eq!(
        scheduler.delayed[&failed_identity].1,
        metadata.definition_version
    );

    let retry_due = scheduler.delayed[&failed_identity].0;
    scheduler.promote_due(retry_due);
    let resumed = scheduler
        .pop_runnable()
        .expect("the preserved builder was not retried after its delay");
    assert_eq!(resumed.definition.identity(), failed_identity);
    let BuilderPhase::CatchUp(resumed) = resumed.phase else {
        panic!("the delayed retry discarded exact catch-up work");
    };
    assert_eq!(resumed.through, barrier(10));
    assert_eq!(resumed.target, barrier(12));
    assert!(resumed.changed);
    assert!(resumed.must_publish);
}

#[test]
fn lost_incremental_history_is_a_failed_precondition() {
    for error in [
        IndexEventError::CheckpointMismatch(NodeId(1)),
        IndexEventError::SourceEpochChanged(NodeId(1)),
        IndexEventError::SourceHistoryGap(NodeId(1)),
        IndexEventError::IncompleteSources,
    ] {
        assert_eq!(event_status(error).code(), tonic::Code::FailedPrecondition);
    }
}

#[test]
fn atomic_barrier_change_reinspects_instead_of_losing_the_wake() {
    let error = event_status(IndexEventError::BarrierChanged);
    assert_eq!(error.code(), tonic::Code::Aborted);
    assert_eq!(
        failure_recovery(BuilderFailurePhase::CatchUp, &error),
        BuilderFailureRecovery::Reinspect
    );
}

#[test]
fn malformed_source_evidence_fails_the_definition_closed() {
    let error = event_status(IndexEventError::NonContiguousSource(NodeId(1)));
    assert_eq!(error.code(), tonic::Code::DataLoss);
    assert_eq!(
        failure_recovery(BuilderFailurePhase::CatchUp, &error),
        BuilderFailureRecovery::FailClosed
    );
}

#[test]
fn transient_catch_up_and_publish_failures_preserve_exact_work() {
    let job = BuilderJob::new(definition(1, 2, 9)).unwrap();
    let catch_up = CatchUpWork {
        current: None,
        through: barrier(10),
        target: barrier(12),
        candidate: CandidateGeneration::rebuild(),
        changed: true,
        must_publish: true,
        maintenance: false,
        progress: BuilderProgress::start(job.telemetry_identity(), BuilderProgressPhase::CatchUp),
    };
    let catch_up_step = recover_builder_failure(
        job,
        BuilderFailurePhase::CatchUp,
        Some(BuilderPhase::CatchUp(catch_up)),
        Status::unavailable("temporary peer failure"),
    );
    assert!(matches!(
        catch_up_step.disposition,
        BuilderDisposition::Retry(_)
    ));
    let BuilderPhase::CatchUp(resumed) = catch_up_step.job.phase else {
        panic!("transient catch-up failure discarded its resumable phase");
    };
    assert_eq!(resumed.through, barrier(10));
    assert_eq!(resumed.target, barrier(12));
    assert!(resumed.changed);
    assert!(resumed.must_publish);

    let publish = PublishWork {
        current: None,
        barrier: barrier(12),
        candidate: CandidateGeneration::rebuild(),
        admission: DerivedArtifactAdmission::PublicationProgress,
    };
    let publish_step = recover_builder_failure(
        BuilderJob::new(definition(1, 2, 9)).unwrap(),
        BuilderFailurePhase::Publish,
        Some(BuilderPhase::Publish(publish)),
        Status::deadline_exceeded("temporary publication timeout"),
    );
    assert!(matches!(
        publish_step.disposition,
        BuilderDisposition::Retry(_)
    ));
    assert!(matches!(publish_step.job.phase, BuilderPhase::Publish(_)));
}

#[test]
fn head_advanced_past_a_fixed_target_reinspects_instead_of_retrying_forever() {
    let target = barrier(12);
    let head = Head {
        version: VersionId(13),
        deleted: false,
        mutation_stamp: Some(MutationStamp {
            format: MUTATION_STAMP_FORMAT,
            predecessor_version: Some(VersionId(12)),
            program_commit_cursor: None,
            mutation_fingerprint: [9; 32],
            active_placement_log_id: target.fence,
            serving_fence_term: 3,
            source_id: target.sources[&NodeId(1)].source,
            source_journal_position: 12,
        }),
    };

    let error = require_visible_head(&head, &target).unwrap_err();
    assert_eq!(error.code(), tonic::Code::Aborted);
    assert_eq!(
        failure_recovery(BuilderFailurePhase::CatchUp, &error),
        BuilderFailureRecovery::Reinspect
    );
}

#[test]
fn incompatible_incremental_history_fails_closed_without_opening_a_snapshot() {
    let job = BuilderJob::new(definition(1, 2, 9)).unwrap();
    let work = CatchUpWork {
        current: None,
        through: barrier(10),
        target: barrier(12),
        candidate: CandidateGeneration::rebuild(),
        changed: false,
        must_publish: false,
        maintenance: false,
        progress: BuilderProgress::start(job.telemetry_identity(), BuilderProgressPhase::CatchUp),
    };
    let step = recover_builder_failure(
        job,
        BuilderFailurePhase::CatchUp,
        Some(BuilderPhase::CatchUp(work)),
        Status::failed_precondition("source history gap"),
    );

    assert!(matches!(step.job.phase, BuilderPhase::Inspect));
    assert!(matches!(step.disposition, BuilderDisposition::Failed));
    assert_eq!(
        failure_recovery(
            BuilderFailurePhase::Inspect,
            &Status::failed_precondition("published barrier is no longer retained")
        ),
        BuilderFailureRecovery::FailClosed
    );
    assert_eq!(
        failure_recovery(
            BuilderFailurePhase::Rebuild,
            &Status::unavailable("terminal snapshot stream")
        ),
        BuilderFailureRecovery::Reinspect
    );
    assert_eq!(
        failure_recovery(
            BuilderFailurePhase::Publish,
            &Status::failed_precondition("definition changed before CAS")
        ),
        BuilderFailureRecovery::Reinspect
    );
}

#[test]
fn deterministic_failure_stays_quiet_until_a_new_definition_revision() {
    let mut scheduler = BuilderScheduler::default();
    let original = definition(1, 2, 9);
    let identity = original.identity();
    queue_definition(&mut scheduler, original);
    let job = scheduler.pop_runnable().unwrap();
    let metadata = WorkMetadata::from_job(&job);
    let failed = recover_builder_failure(
        job,
        BuilderFailurePhase::Inspect,
        Some(BuilderPhase::Inspect),
        Status::data_loss("corrupt manifest"),
    );
    scheduler.complete_with(metadata, failed, |_, _, _| true);

    assert!(scheduler.pop_runnable().is_none());
    assert!(!scheduler.entries.contains_key(&identity));

    let mut replacement = definition(1, 2, 9);
    replacement.object_version = 2;
    queue_definition(&mut scheduler, replacement);
    assert_eq!(
        scheduler.pop_runnable().unwrap().definition.object_version,
        2
    );
}

#[test]
fn resident_work_plan_charges_every_simultaneous_allocation() {
    let total = anvil_index::MIN_INDEX_KIND_MEMORY_BYTES as u64;
    let configured = SegmentMemoryPlan::new(total as usize).unwrap();
    let source_resident_bytes = 8 * 1024 * 1024;
    let plan = work_plan_for_limit(total, source_resident_bytes).unwrap();

    assert!(source_wire_limit(total) >= 64 * 1024);
    assert_eq!(plan.max_resident_bytes, configured.max_resident_bytes);
    assert!(plan.max_source_projection_bytes > 0);
    assert_eq!(
        plan.max_resident_bytes
            + FIXED_INDEX_SEAL_WORKSPACE_BYTES
            + plan.max_source_projection_bytes
            + source_resident_bytes as usize,
        plan.total_bytes
    );
    assert!(configured.max_resident_bytes > 1024 * 1024);
}
