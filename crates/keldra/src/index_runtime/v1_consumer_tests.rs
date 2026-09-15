use super::*;

#[test]
fn integrity_failure_halts_only_the_affected_partition() {
    assert!(halts_partition(&Status::data_loss(
        "broken compaction plan"
    )));
    assert!(!halts_partition(&Status::unavailable("retryable source")));
}

#[test]
fn overlapping_compaction_integrity_halts_once_and_exposes_durable_lag() {
    // The index regression constructs the released wide-L1 plus narrow-L0
    // self-built plan. This is its first server boundary: splice reports the
    // named integrity invariant and producer compaction maps it to DataLoss.
    let error =
        super::super::v1_compaction::index_status(keldra_index::IndexError::IntegrityViolation(
            "component compaction plan minimum_key; component=DocumentHead, stream_root=0102"
                .into(),
        ));
    assert_eq!(error.code(), tonic::Code::DataLoss);
    assert!(halts_partition(&error));

    let source = SourceId {
        node_id: 1,
        source_epoch: [4; 32],
    };
    let mut affected = PartitionEvidence::opened(source, [3; 32], [5; 32], 12);
    let mut peer = PartitionEvidence::opened(source, [6; 32], [7; 32], 20);
    let now = Instant::now();
    let caught_up = affected.observe_lag(12, false, false, STALL_AFTER, now);
    assert_eq!(caught_up.entries, 0);
    assert!(!caught_up.stalled);

    let mut affected_halted = false;
    let peer_halted = false;
    let mut affected_stage = ProducerStage::Compacting;
    let first = contain_integrity_failure(
        &mut affected_halted,
        &mut affected_stage,
        Some(&mut affected),
        &error,
    );
    assert!(first.newly_halted);
    assert_eq!(first.failed_stage, ProducerStage::Compacting);
    assert!(!writer_state_is_runnable(affected_halted));

    let second = contain_integrity_failure(
        &mut affected_halted,
        &mut affected_stage,
        Some(&mut affected),
        &error,
    );
    assert!(!second.newly_halted, "the same halt must not log twice");

    let source_advanced =
        affected.observe_lag(20, true, false, STALL_AFTER, now + Duration::from_millis(1));
    assert_eq!(affected.published_next, 12, "lag uses durable Current");
    assert_eq!(source_advanced.entries, 8);
    assert!(source_advanced.stalled);

    let peer_progress = peer.observe_lag(20, false, false, STALL_AFTER, now);
    assert!(writer_state_is_runnable(peer_halted));
    assert_eq!(peer_progress.entries, 0);
    assert!(!peer_progress.stalled);
    assert!(!peer.halted);
}

#[test]
fn an_assigned_partition_without_an_open_writer_is_not_runnable() {
    let writers = BTreeMap::<ProjectionPartitionIdentity, Writer>::new();

    assert!(!runnable_partition(&writers, &partition()));
}

#[test]
fn fresh_partition_lag_starts_at_the_journal_sentinel() {
    assert_eq!(routed_lag_start(0), 1);
    assert_eq!(routed_lag_start(17), 17);
}

#[test]
fn alias_head_mutation_preserves_exact_and_canonical_paths() {
    let mutation = head_mutation(
        ObjectHeadChange {
            offset: 8,
            tenant_id: 1,
            bucket_id: 2,
            exact_path: "aliases/reserved.json".into(),
            canonical_path: Some("objects/target.json".into()),
            path_version: VersionId(9),
            kind: ObjectHeadChangeKind::Delete,
            program_commit_cursor: None,
            reference_deltas: Vec::new(),
            accounting_transition: None,
            definition_transition: None,
        },
        0,
    );

    assert_eq!(mutation.path, "aliases/reserved.json");
    assert_eq!(
        mutation.canonical_path.as_deref(),
        Some("objects/target.json")
    );
    assert!(mutation.deleted);
    assert!(!mutation.predecessor_absent_at_window_start);
}

#[test]
fn only_direct_head_accounting_proves_predecessor_absence() {
    let direct = head_mutation(
        ObjectHeadChange {
            offset: 1,
            tenant_id: 1,
            bucket_id: 2,
            exact_path: "objects/new.json".into(),
            canonical_path: None,
            path_version: VersionId(1),
            kind: ObjectHeadChangeKind::Put,
            program_commit_cursor: None,
            reference_deltas: Vec::new(),
            accounting_transition: Some(keldra_store::AccountingHeadTransition::new(
                None,
                Some(10),
                0,
            )),
            definition_transition: None,
        },
        0,
    );
    assert!(direct.predecessor_absent_at_window_start);

    let replacement = head_mutation(
        ObjectHeadChange {
            offset: 2,
            path_version: VersionId(2),
            accounting_transition: Some(keldra_store::AccountingHeadTransition::new(
                Some(10),
                Some(11),
                10,
            )),
            ..ObjectHeadChange {
                offset: 1,
                tenant_id: 1,
                bucket_id: 2,
                exact_path: "objects/existing.json".into(),
                canonical_path: None,
                path_version: VersionId(1),
                kind: ObjectHeadChangeKind::Put,
                program_commit_cursor: None,
                reference_deltas: Vec::new(),
                accounting_transition: None,
                definition_transition: None,
            }
        },
        0,
    );
    assert!(!replacement.predecessor_absent_at_window_start);

    let alias = head_mutation(
        ObjectHeadChange {
            offset: 3,
            tenant_id: 1,
            bucket_id: 2,
            exact_path: "aliases/new.json".into(),
            canonical_path: Some("objects/new.json".into()),
            path_version: VersionId(3),
            kind: ObjectHeadChangeKind::Put,
            program_commit_cursor: None,
            reference_deltas: Vec::new(),
            accounting_transition: Some(keldra_store::AccountingHeadTransition::new(
                None,
                Some(10),
                0,
            )),
            definition_transition: None,
        },
        0,
    );
    assert!(!alias.predecessor_absent_at_window_start);
}

#[test]
fn finalized_atomic_mutations_keep_predecessor_fallback() {
    let source = SourceId {
        node_id: 1,
        source_epoch: [3; 32],
    };
    let (_, mutations) = dispatch_mutations(V1SourceDispatch::FinalizedAtomic(
        super::super::v1_journal_dispatch::V1FinalizedAtomicGroup {
            source,
            cursor: 7,
            mutations: vec![super::super::v1_atomic_dispatch::FinalizedAtomicMutation {
                cursor: 7,
                mutation: keldra_store::AtomicBatchMutation {
                    tenant_id: 1,
                    bucket_id: 2,
                    exact_path: "objects/atomic.json".into(),
                    canonical_path: None,
                    path_version: VersionId(4),
                    deleted: false,
                    source_id: source,
                    source_journal_position: 4,
                },
            }],
        },
    ))
    .unwrap();
    assert_eq!(mutations.len(), 1);
    assert!(!mutations[0].predecessor_absent_at_window_start);
}

fn partition() -> ProjectionPartitionIdentity {
    ProjectionPartitionIdentity {
        family_id: [3; 32],
        source_node: 1,
        source_epoch: [4; 32],
        producer_node: 1,
        placement_term: 1,
        placement_index: 1,
    }
}

fn loaded_current(
    physical_catalog_generation: [u8; 32],
    current_object_version: VersionId,
) -> LoadedV1ProjectionGeneration {
    let generation = keldra_index::v1::ProjectionGeneration::initial(
        partition(),
        physical_catalog_generation,
        12,
        12,
        Vec::new(),
    )
    .unwrap();
    let current = keldra_index::v1::ProjectionCurrent::new([9; 32], &generation).unwrap();
    LoadedV1ProjectionGeneration {
        current,
        current_object_version,
        generation,
    }
}

fn query_update(
    document: keldra_index::v1::StableDocumentKey,
    path: &str,
    version: u64,
) -> PreparedQueryMutationBatch {
    PreparedQueryMutationBatch {
        membership: Some(keldra_index::v1::PreparedQueryMembershipDelta {
            recipe: keldra_index::v1::RecipeIdentity::new([3; 32]).unwrap(),
            gates: vec![keldra_index::v1::QueryDocumentGate {
                document,
                material_source_version: version,
                current_source_version: version,
                live: true,
                source_path: Some(path.into()),
                canonical_source_path: None,
                result_path: Some(path.into()),
                result_version: version,
            }],
        }),
        fields: Vec::new(),
    }
}

fn query_field_update(
    document: keldra_index::v1::StableDocumentKey,
    version: u64,
) -> keldra_index::v1::PreparedQueryRecipeDelta {
    keldra_index::v1::PreparedQueryRecipeDelta {
        recipe: keldra_index::v1::RecipeIdentity::new([4; 32]).unwrap(),
        delta: keldra_index::v1::PreparedQueryFieldDelta {
            presence: keldra_index::v1::QueryDocumentGate {
                document,
                material_source_version: version,
                current_source_version: version,
                live: true,
                source_path: None,
                canonical_source_path: None,
                result_path: None,
                result_version: 0,
            },
            doc_value: None,
            terms: Vec::new(),
            points: Vec::new(),
        },
    }
}

fn mutation(path: &str, offset: u64) -> Mutation {
    Mutation {
        offset,
        ordinal: 0,
        tenant_id: 1,
        bucket_id: 2,
        path: path.into(),
        canonical_path: None,
        version: offset,
        deleted: false,
        predecessor_absent_at_window_start: false,
    }
}

#[test]
fn newest_wins_keeps_the_first_post_current_absence_evidence() {
    let mut created = mutation("objects/a", 1);
    created.predecessor_absent_at_window_start = true;
    let recreated = mutation("objects/a", 3);
    let (_, coalesced) = coalesce_units(
        0,
        vec![
            (0, vec![created]),
            (0, vec![mutation("objects/a", 2)]),
            (0, vec![recreated]),
        ],
    )
    .unwrap();
    assert_eq!(coalesced.len(), 1);
    assert_eq!(coalesced[0].offset, 3);
    assert!(coalesced[0].predecessor_absent_at_window_start);

    let mut existing = mutation("objects/b", 4);
    existing.predecessor_absent_at_window_start = false;
    let mut later_recreate = mutation("objects/b", 5);
    later_recreate.predecessor_absent_at_window_start = true;
    let (_, coalesced) =
        coalesce_units(0, vec![(0, vec![existing]), (0, vec![later_recreate])]).unwrap();
    assert!(!coalesced[0].predecessor_absent_at_window_start);
}

#[test]
fn queued_pages_keep_the_first_post_current_absence_evidence() {
    let mut latest = BTreeMap::new();
    let mut resident_bytes = 0;
    let mut operations = 0;
    let mut next = 0;
    let mut created = mutation("objects/a", 1);
    created.predecessor_absent_at_window_start = true;
    queue_mutation_window(
        &mut latest,
        &mut resident_bytes,
        &mut operations,
        &mut next,
        1024 * 1024,
        vec![created],
        2,
    )
    .unwrap();
    queue_mutation_window(
        &mut latest,
        &mut resident_bytes,
        &mut operations,
        &mut next,
        1024 * 1024,
        vec![mutation("objects/a", 2)],
        3,
    )
    .unwrap();
    assert!(latest["objects/a"].predecessor_absent_at_window_start);
}

#[test]
fn fresh_partition_publication_starts_at_the_zero_sentinel() {
    assert_eq!(publication_start(None), 0);
}

#[test]
fn stale_catalog_current_is_only_the_cas_guard_for_a_fresh_rebuild() {
    let current = loaded_current([7; 32], VersionId(41));

    let (resumed, replacement) = current_for_catalog(Some(current.clone()), [7; 32]);
    assert_eq!(
        resumed.unwrap().current_object_version,
        current.current_object_version
    );
    assert_eq!(replacement, None);

    let (resumed, replacement) = current_for_catalog(Some(current), [8; 32]);
    assert!(resumed.is_none());
    assert_eq!(replacement, Some(VersionId(41)));

    let (resumed, replacement) = current_for_catalog(None, [8; 32]);
    assert!(resumed.is_none());
    assert_eq!(replacement, None);
}

#[test]
fn empty_physical_family_writers_do_not_preallocate_query_capacity() {
    let bytes = 1_024;
    let credits = IndexingMemoryCredits::new(
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
    let limits = Limits {
        bytes,
        flush_bytes: 64,
        projection_batch_bytes: 256,
        flush_age: Duration::from_secs(1),
        flush_operations: 64,
        lsm_runs: 64,
        lsm_bytes: 1_024,
        parallelism: 1,
        worker_bytes: 64,
    };

    let writers = (0..5)
        .map(|_| empty_query_credits(&credits, limits).unwrap())
        .collect::<Vec<_>>();

    assert_eq!(writers.len(), 5);
    assert_eq!(
        credits.stage_used_bytes(IndexingMemoryStage::OrderingCatalog),
        5
    );
}

#[test]
fn projection_batch_headroom_scales_with_pipeline_memory() {
    let config = IndexRuntimeConfig::new(4)
        .unwrap()
        .with_pipeline_memory_bytes(4 * 1024 * 1024 * 1024)
        .unwrap();
    let limits = limits(config).unwrap();

    assert_eq!(limits.bytes, 2 * 1024 * 1024 * 1024);
    assert_eq!(limits.flush_bytes, 16 * 1024 * 1024);
    assert_eq!(limits.projection_batch_bytes, 512 * 1024 * 1024);
    assert_eq!(
        limits.flush_operations,
        IndexRuntimeConfig::DEFAULT_FLUSH_MAX_OPERATIONS
    );
}

#[test]
fn configured_flush_operation_bound_is_not_replaced_by_a_fixed_small_batch() {
    let config = IndexRuntimeConfig::new(4)
        .unwrap()
        .with_flush_boundaries(16 * 1024 * 1024, 1_000, 131_072)
        .unwrap();

    assert_eq!(limits(config).unwrap().flush_operations, 131_072);
}

#[test]
fn successful_progress_reschedules_only_an_unfinished_writer() {
    assert!(should_reschedule_after_advance(
        ProducerStage::JournalScan,
        10,
        11
    ));
    assert!(should_reschedule_after_advance(
        ProducerStage::Backfill,
        10,
        11
    ));
    assert!(!should_reschedule_after_advance(
        ProducerStage::CaughtUp,
        10,
        11
    ));
}

#[test]
fn no_progress_does_not_busy_reschedule_a_partition() {
    assert!(!should_reschedule_after_advance(
        ProducerStage::JournalScan,
        10,
        10
    ));
    assert!(!should_reschedule_after_advance(
        ProducerStage::Backfill,
        10,
        9
    ));
}

#[test]
fn rolling_preparation_refills_only_available_bounded_lanes() {
    assert_eq!(preparation_refill_size(10, 0, 4), 4);
    assert_eq!(preparation_refill_size(10, 2, 4), 2);
    assert_eq!(preparation_refill_size(10, 3, 4), 0);
    assert_eq!(preparation_refill_size(1, 0, 4), 1);
    assert_eq!(preparation_refill_size(0, 0, 4), 0);
    assert_eq!(preparation_refill_size(3, 0, 0), 1);
}

#[test]
fn journal_read_ahead_tracks_worker_capacity_without_becoming_unbounded() {
    assert_eq!(journal_read_ahead_pages(0), 2);
    assert_eq!(journal_read_ahead_pages(1), 2);
    assert_eq!(journal_read_ahead_pages(4), 8);
    assert_eq!(journal_read_ahead_pages(64), 32);
}

#[test]
fn only_the_latest_background_lag_observation_can_publish() {
    let epoch = AtomicU64::new(7);
    assert!(lag_observation_is_current(&epoch, 7));
    assert!(!lag_observation_is_current(&epoch, 6));
    epoch.store(8, Ordering::Release);
    assert!(!lag_observation_is_current(&epoch, 7));
    assert!(lag_observation_is_current(&epoch, 8));
}

#[test]
fn publication_chunks_preserve_atomic_offset_groups_and_contiguous_cuts() {
    let mutations = vec![
        mutation("objects/a", 1),
        mutation("objects/b", 2),
        Mutation {
            ordinal: 1,
            ..mutation("objects/c", 2)
        },
        mutation("objects/d", 3),
        mutation("objects/e", 4),
    ];

    let chunks = mutation_publication_chunks(mutations, 5, 2).unwrap();

    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].0.len(), 3);
    assert_eq!(chunks[0].1, 3);
    assert_eq!(chunks[1].0.len(), 2);
    assert_eq!(chunks[1].1, 5);
    assert!(chunks.iter().all(|(chunk, _)| {
        chunk
            .windows(2)
            .all(|pair| (pair[0].offset, pair[0].ordinal) < (pair[1].offset, pair[1].ordinal))
    }));
}

#[test]
fn one_large_atomic_group_remains_indivisible() {
    let mutations = (0..8)
        .map(|ordinal| Mutation {
            ordinal,
            ..mutation(&format!("objects/{ordinal}"), 7)
        })
        .collect();

    let chunks = mutation_publication_chunks(mutations, 8, 2).unwrap();

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].0.len(), 8);
    assert_eq!(chunks[0].1, 8);
}

#[test]
fn reserved_only_progress_does_not_request_an_empty_current_publication() {
    // A fresh partition needs its one baseline Current for activation,
    // even when the baseline contains no matching objects.
    assert!(has_publication_work(false, 0));

    // Once Current exists, filtered control events advance only the replay
    // cursor. They cannot generate another Current/source event alone.
    assert!(!has_publication_work(true, 0));

    // The first later matching mutation makes the complete contiguous
    // range, including the skipped control positions, publishable.
    assert!(has_publication_work(true, 1));
}

#[test]
fn repeated_hot_paths_coalesce_without_losing_the_safe_atomic_cut() {
    let units = (0..10_000_u64)
        .map(|offset| {
            (
                offset + 10,
                vec![mutation(
                    &format!("objects/{:03}", offset % 256),
                    offset + 1,
                )],
            )
        })
        .collect();
    let (through_atomic, output) = coalesce_units(7, units).unwrap();
    assert_eq!(through_atomic, 10_009);
    assert_eq!(output.len(), 256);
    assert!(
        output
            .windows(2)
            .all(|pair| pair[0].offset < pair[1].offset)
    );
    assert!(output.iter().all(|mutation| mutation.offset > 9_744));
}

#[test]
fn duplicate_path_inside_one_atomic_unit_still_fails_closed() {
    let error = coalesce_units(
        0,
        vec![(9, vec![mutation("objects/a", 3), mutation("objects/a", 3)])],
    )
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::DataLoss);
}

#[test]
fn publication_window_is_newest_wins_across_page_boundaries() {
    let mut latest = BTreeMap::new();
    let mut bytes = 0;
    let mut operations = 0;
    let mut next = 1;
    let mut changed = mutation("objects/a", 2);
    changed.version = 20;
    queue_mutation_window(
        &mut latest,
        &mut bytes,
        &mut operations,
        &mut next,
        usize::MAX,
        vec![changed],
        3,
    )
    .unwrap();
    let mut restored = mutation("objects/a", 3);
    restored.version = 10;
    queue_mutation_window(
        &mut latest,
        &mut bytes,
        &mut operations,
        &mut next,
        usize::MAX,
        vec![restored],
        4,
    )
    .unwrap();

    assert_eq!(latest.len(), 1);
    assert_eq!(latest["objects/a"].version, 10);
    assert_eq!(next, 4);
    assert_eq!(operations, 2);
}

#[test]
fn create_then_delete_remains_an_empty_latest_mutation_and_advances() {
    let mut latest = BTreeMap::new();
    let mut bytes = 0;
    let mut operations = 0;
    let mut next = 0;
    queue_mutation_window(
        &mut latest,
        &mut bytes,
        &mut operations,
        &mut next,
        usize::MAX,
        vec![mutation("objects/a", 1)],
        2,
    )
    .unwrap();
    let mut deleted = mutation("objects/a", 2);
    deleted.deleted = true;
    queue_mutation_window(
        &mut latest,
        &mut bytes,
        &mut operations,
        &mut next,
        usize::MAX,
        vec![deleted],
        3,
    )
    .unwrap();

    assert!(latest["objects/a"].deleted);
    assert_eq!(latest.len(), 1);
    assert_eq!(next, 3);
}

#[test]
fn ten_thousand_hot_updates_prepare_only_256_query_documents() {
    let scope = [9; 32];
    let mut latest = BTreeMap::new();
    let mut bytes = 0;
    let mut operations = 0;
    let mut next = 0;
    for offset in 1..=10_000_u64 {
        queue_mutation_window(
            &mut latest,
            &mut bytes,
            &mut operations,
            &mut next,
            usize::MAX,
            vec![mutation(&format!("objects/{:03}", offset % 256), offset)],
            offset + 1,
        )
        .unwrap();
    }
    assert_eq!(latest.len(), 256);
    assert_eq!(operations, 10_000);
    assert_eq!(next, 10_001);

    let mut pending = PreparedQueryMutationBatch::default();
    let mut preparations = 0;
    for mutation in latest.into_values() {
        let document =
            keldra_index::v1::StableDocumentKey::derive(scope, &mutation.path, 0).unwrap();
        merge_query(
            &mut pending,
            query_update(document, &mutation.path, mutation.version),
        )
        .unwrap();
        preparations += 1;
    }

    let membership = pending.membership.as_ref().unwrap();
    assert_eq!(membership.gates.len(), 256);
    assert_eq!(preparations, 256);
    assert!(membership.gates.iter().all(|gate| {
        let path = gate.source_path.as_deref().unwrap();
        let suffix = path.rsplit('/').next().unwrap().parse::<u64>().unwrap();
        gate.current_source_version == 10_000 - ((10_000 - suffix) % 256)
    }));

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
    let credits = QueryBlockCredits::from_pipeline_permit(
        memory
            .acquire(IndexingMemoryStage::OrderingCatalog, bytes)
            .unwrap(),
    );
    let artifacts = keldra_index::v1::prepare_projection_query_run(
        ProjectionPartitionIdentity::new([1; 32], 2, [3; 32], 2, 4, 5).unwrap(),
        [6; 32],
        1,
        1,
        10_001,
        10_000,
        pending,
        keldra_index::v1::QueryBlockLimits::default_for_memory(),
        credits,
    )
    .unwrap();
    assert!(!artifacts.packs().is_empty());
}

#[test]
fn repeated_unpublished_document_preparation_keeps_only_the_latest_query_delta() {
    let document = keldra_index::v1::StableDocumentKey::derive([9; 32], "objects/a", 0).unwrap();
    let mut pending = PreparedQueryMutationBatch::default();
    let mut first = query_update(document, "objects/a", 1);
    first.fields.push(query_field_update(document, 1));
    merge_query(&mut pending, first).unwrap();
    let mut second = query_update(document, "objects/a", 2);
    second.fields.push(query_field_update(document, 2));
    merge_query(&mut pending, second).unwrap();

    let membership = pending.membership.as_ref().unwrap();
    assert_eq!(membership.gates.len(), 1);
    assert_eq!(membership.gates[0].current_source_version, 2);
    assert_eq!(pending.fields.len(), 1);
    assert_eq!(pending.fields[0].delta.presence.current_source_version, 2);

    let bytes = 1024 * 1024;
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
    let credits = QueryBlockCredits::from_pipeline_permit(
        memory
            .acquire(IndexingMemoryStage::OrderingCatalog, bytes)
            .unwrap(),
    );
    let artifacts = keldra_index::v1::prepare_projection_query_run(
        partition(),
        [6; 32],
        1,
        1,
        3,
        2,
        pending,
        keldra_index::v1::QueryBlockLimits::default_for_memory(),
        credits,
    )
    .unwrap();
    assert!(!artifacts.packs().is_empty());
}

#[test]
fn replay_selection_holds_measured_metadata_not_construction_headroom() {
    let mutation = mutation("objects/a", 7);
    let selected = SelectedV1Source {
        source: IndexSourceMutation::Remove {
            identity: keldra_index::v1::ObjectIdentity {
                path: "objects/a".into(),
                version: 7,
            },
            canonical_path: None,
        },
        selected: None,
    };

    let retained = selected_mutation_resident_bytes(&mutation, &selected, &[]).unwrap();

    assert!(retained < 1024);
    assert!(retained >= std::mem::size_of::<SelectedMutation>());
}
