use super::*;
use keldra_index::v1::IndexingMemoryLimits;

fn retry_writer(credits: &IndexingMemoryCredits) -> Writer {
    use keldra_api::v1::*;
    let catalog = IndexCatalog::default();
    let definition = crate::index_service::StoredIndexDefinition::create(
        "tenant".into(),
        CreateIndexRequest {
            bucket: "bucket".into(),
            name: "retry-marker".into(),
            path_prefix: String::new(),
            content_type: String::new(),
            specification: Some(IndexSpecification {
                specification: Some(index_specification::Specification::TypedJson(
                    TypedJsonIndexSpec {
                        fields: vec![IndexField {
                            name: "value".into(),
                            json_pointer: "/value".into(),
                            cardinality: IndexFieldCardinality::Single as i32,
                            capabilities: vec![IndexFieldCapability::Exact as i32],
                            field_type: Some(index_field::FieldType::Keyword(KeywordIndexField {})),
                        }],
                        physical_order: Vec::new(),
                    },
                )),
            }),
            command_id: "retry-marker-create".into(),
            result_authorization: Some(IndexResultAuthorization {
                policy: Some(index_result_authorization::Policy::Application(
                    ApplicationIndexResultAuthorization {},
                )),
            }),
        },
        1,
    )
    .unwrap();
    catalog
        .upsert(super::super::catalog::CatalogDefinition::new(1, 2, 1, definition).unwrap())
        .unwrap();
    let recipe = catalog.physical_snapshot().unwrap().recipes[0].clone();
    let source = SourceId {
        node_id: 1,
        source_epoch: [4; 32],
    };
    let partition = partition();
    let scanned = IndexBarrier {
        fence: keldra_store::PlacementLogId { term: 1, index: 1 },
        atomic: super::super::events::AtomicProgramWatermark::new(None, None, 0),
        sources: BTreeMap::from([(
            NodeId(1),
            super::super::events::IndexSourceCursor {
                source,
                next_offset: 12,
            },
        )]),
    };
    Writer {
        current: Some(loaded_current(recipe.physical_generation, VersionId(90))),
        recipe,
        source,
        partition,
        catalog_rebuild_current_version: None,
        dispatcher: None,
        look_ahead: None,
        look_ahead_context: None,
        look_ahead_permitted: false,
        look_ahead_progress: None,
        look_ahead_prepared: BTreeMap::new(),
        scanned,
        accumulator: PartitionProjectionAccumulator::new(
            source_scope(source),
            partition,
            12,
            4096,
            credits.clone(),
        )
        .unwrap(),
        baseline: None,
        query: PreparedQueryMutationBatch::default(),
        query_credits: QueryBlockCredits::from_growable_pipeline_permit(
            credits
                .acquire(IndexingMemoryStage::OrderingCatalog, 1)
                .unwrap(),
            4096,
        )
        .unwrap(),
        query_input_credits: Vec::new(),
        sealing_progress: None,
        since: None,
        source_bytes: 0,
        pending_prepared_rows: 0,
        pending_prepared_bytes: 0,
        pending_projected_rows: 0,
        pending_projected_encoded_bytes: 0,
        through_atomic: 0,
        atomic_replay_target: None,
        pending_mutations: BTreeMap::new(),
        pending_mutation_bytes: 0,
        pending_operations: 0,
        pending_next: 12,
        skipped_proof_next: 12,
        pending_skipped_ack: false,
        pending_mutation_capacity: 4096,
        pending_mutation_permit: credits
            .acquire(IndexingMemoryStage::ReplayInput, 1)
            .unwrap(),
        background_compaction: None,
        post_cas_verification: None,
        pending_publication: None,
        halted_on_integrity_failure: false,
        stage: ProducerStage::Preparing,
    }
}

fn retry_credits() -> IndexingMemoryCredits {
    let bytes = 1024 * 1024;
    IndexingMemoryCredits::new(
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
    .unwrap()
}

#[test]
fn retry_discards_consumed_sealed_state_before_pending_publication_exists() {
    use keldra_index::v1::{DocumentHead, ProjectedDocumentState};
    let credits = retry_credits();
    let mut writer = retry_writer(&credits);
    let scope = source_scope(writer.source);
    let path = "objects/marker-12";
    let row = PreparedProjectionRow {
        source_offset: 12,
        mutation_ordinal: 0,
        source_path: path.into(),
        source_version: 12,
        projected_states: vec![
            ProjectedDocumentState::new(
                scope,
                DocumentHead::new(scope, path.into(), 0, 12, None, true).unwrap(),
                Vec::new(),
                Vec::new(),
            )
            .unwrap(),
        ],
    };
    let batch = PreparedProjectionBatchReservation::reserve(&credits, 4096)
        .unwrap()
        .finish(scope, 12, 13, vec![row])
        .unwrap();
    assert!(matches!(
        writer.accumulator.apply_batch(batch).unwrap(),
        ProjectionBatchAdmission::Applied { .. }
    ));
    assert!(writer.accumulator.buffered_bytes() > 0);
    let durable = writer.current.clone().unwrap();
    writer
        .scanned
        .sources
        .get_mut(&NodeId(1))
        .unwrap()
        .next_offset = 13;
    writer.pending_next = 13;
    writer.query = query_update(
        keldra_index::v1::StableDocumentKey::derive(scope, path, 0).unwrap(),
        path,
        12,
    );
    // These are the actual destructive consumption operations before flush
    // creates pending_publication; the failure must not keep this writer.
    let sealed = writer.accumulator.seal_and_reset().unwrap();
    let consumed_query = std::mem::take(&mut writer.query);
    assert_eq!(sealed.projection.checkpoint.next_offset, 13);
    assert_eq!(writer.accumulator.next_offset(), 13);
    assert_eq!(writer.accumulator.buffered_bytes(), 0);
    assert!(writer.pending_publication.is_none());
    drop(sealed);
    drop(consumed_query);
    let mut evidence = PartitionEvidenceMap::from([(
        partition(),
        PartitionEvidence::opened(writer.source, [3; 32], [5; 32], 12),
    )]);
    let mut writers = BTreeMap::new();
    assert!(outcome_handling::record_partition_outcome(
        partition(),
        writer,
        Err(Status::unavailable(
            "artifact staging failed before pending successor"
        )),
        &mut writers,
        &mut evidence
    ));
    assert!(!writers.contains_key(&partition()));
    assert_eq!(evidence[&partition()].published_next, 12);
    assert_eq!(credits.used_bytes(), 0);
    let mut reopened = retry_writer(&credits);
    reopened.current = Some(durable);
    assert_eq!(reopened.accumulator.next_offset(), 12);
    assert_eq!(reopened.pending_next, 12);
    assert_eq!(reopened.scanned.sources[&NodeId(1)].next_offset, 12);
}

#[test]
fn successful_advance_retains_writer_and_integrity_failure_retains_halted_writer() {
    let credits = retry_credits();
    let writer = retry_writer(&credits);
    let mut evidence = PartitionEvidenceMap::from([(
        partition(),
        PartitionEvidence::opened(writer.source, [3; 32], [5; 32], 12),
    )]);
    let mut writers = BTreeMap::new();
    assert!(!outcome_handling::record_partition_outcome(
        partition(),
        writer,
        Ok(()),
        &mut writers,
        &mut evidence
    ));
    assert!(writers.contains_key(&partition()));
    assert!(!writers[&partition()].halted_on_integrity_failure);
    assert_eq!(evidence[&partition()].published_next, 12);
    let mut writer = writers.remove(&partition()).unwrap();
    writer.stage = ProducerStage::Compacting;
    assert!(!outcome_handling::record_partition_outcome(
        partition(),
        writer,
        Err(Status::data_loss("compaction invariant failed")),
        &mut writers,
        &mut evidence
    ));
    assert!(writers[&partition()].halted_on_integrity_failure);
    assert!(evidence[&partition()].halted);
    assert_eq!(evidence[&partition()].published_next, 12);
}

#[test]
fn retry_discards_scanned_writer_and_replays_all_source_page_chunks() {
    let bytes = 1024 * 1024;
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
    let mut writer = retry_writer(&credits);
    let durable = writer.current.clone().unwrap();
    let source = writer.source;
    let dispatches = (12..15)
        .map(|offset| V1SourceDispatch::OrdinaryHead {
            source,
            head: ObjectHeadChange {
                offset,
                tenant_id: 1,
                bucket_id: 2,
                exact_path: format!("objects/marker-{offset}"),
                canonical_path: None,
                path_version: VersionId(offset),
                kind: ObjectHeadChangeKind::Put,
                program_commit_cursor: None,
                reference_deltas: Vec::new(),
                accounting_transition: None,
                definition_transition: None,
            },
        })
        .collect::<Vec<_>>();
    let (_, mutations) = prepare_page(&writer, dispatches.clone(), 15).unwrap();
    let mut chunks = publication_chunks(
        mutations,
        15,
        1,
        0,
        0,
        |mutation| mutation.offset,
        |mutation| mutation.atomic_group,
    )
    .unwrap();
    assert_eq!(chunks.len(), 3);
    // Production advances scanned before flushing/queuing these local chunks.
    writer
        .scanned
        .sources
        .get_mut(&NodeId(1))
        .unwrap()
        .next_offset = 15;
    let first = chunks.remove(0);
    queue_mutations(&mut writer, first.mutations, first.next).unwrap();
    assert_eq!(writer.pending_next, 13);
    assert_eq!(writer.pending_mutations.len(), 1);
    assert_eq!(writer.accumulator.next_offset(), 12);
    assert_eq!(
        chunks
            .iter()
            .flat_map(|chunk| &chunk.mutations)
            .map(|mutation| mutation.offset)
            .collect::<Vec<_>>(),
        vec![13, 14]
    );
    let mut evidence = PartitionEvidenceMap::from([(
        partition(),
        PartitionEvidence::opened(source, [3; 32], [5; 32], 12),
    )]);
    let mut writers = BTreeMap::new();
    assert!(outcome_handling::record_partition_outcome(
        partition(),
        writer,
        Err(Status::resource_exhausted(
            "preparation construction memory unavailable"
        )),
        &mut writers,
        &mut evidence
    ));
    assert!(
        !writers.contains_key(&partition()),
        "an advanced retry writer would skip unqueued marker chunks"
    );
    assert_eq!(evidence[&partition()].published_next, 12);
    assert_eq!(evidence[&partition()].identical_retries, 1);
    for stage in [
        IndexingMemoryStage::ReplayInput,
        IndexingMemoryStage::OrderingCatalog,
        IndexingMemoryStage::ProjectionAccumulator,
    ] {
        assert_eq!(
            credits.stage_used_bytes(stage),
            0,
            "discard releases speculative writer memory"
        );
    }
    // Reopening starts from durable Current, not the consumed page's scan cut.
    let mut reopened = retry_writer(&credits);
    reopened.current = Some(durable);
    let (_, replay) = prepare_page(&reopened, dispatches, 15).unwrap();
    assert_eq!(
        replay
            .iter()
            .map(|mutation| mutation.path.as_str())
            .collect::<Vec<_>>(),
        vec![
            "objects/marker-12",
            "objects/marker-13",
            "objects/marker-14"
        ]
    );
    assert_eq!(
        replay
            .iter()
            .map(|mutation| mutation.version)
            .collect::<Vec<_>>(),
        vec![12, 13, 14]
    );
    assert_eq!(reopened.scanned.sources[&NodeId(1)].next_offset, 12);
}

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
                selective_source_position: None,
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
                selective_source_position: None,
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
        atomic_group: None,
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
fn projection_batch_headroom_scales_while_advance_work_remains_bounded() {
    let config = IndexRuntimeConfig::new(4)
        .unwrap()
        .with_pipeline_memory_bytes(4 * 1024 * 1024 * 1024)
        .unwrap();
    let limits = limits(config).unwrap();

    assert_eq!(limits.bytes, 4 * 1024 * 1024 * 1024);
    assert_eq!(limits.flush_bytes, MAX_ADVANCE_SLICE_BYTES);
    assert_eq!(limits.projection_batch_bytes, 1024 * 1024 * 1024);
    assert_eq!(limits.flush_operations, 4_096);
}

#[test]
fn configured_flush_operation_bound_is_capped_for_visibility_latency() {
    let config = IndexRuntimeConfig::new(4)
        .unwrap()
        .with_flush_boundaries(16 * 1024 * 1024, 1_000, 131_072)
        .unwrap();

    assert_eq!(limits(config).unwrap().flush_operations, 4_096);
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
    assert_eq!(preparation_refill_size(1_000, 0, 4), 4);
    assert_eq!(preparation_refill_size(1_000, 2, 4), 2);
    assert_eq!(preparation_refill_size(1_000, 3, 4), 1);
    assert_eq!(preparation_refill_size(10, 0, 4), 1);
    assert_eq!(preparation_refill_size(1, 0, 4), 1);
    assert_eq!(preparation_refill_size(0, 0, 4), 0);
    assert_eq!(preparation_refill_size(3, 0, 0), 1);
    assert_eq!(preparation_refill_size(4_096, 0, 16), 16);
    assert_eq!(preparation_refill_size(4_096, 12, 16), 4);
    assert_eq!(preparation_refill_size(4_096, 16, 16), 0);
}

#[test]
fn preparation_jobs_are_substantial_bounded_chunks() {
    assert_eq!(preparation_chunk_size(0), 0);
    assert_eq!(preparation_chunk_size(1), 1);
    assert_eq!(preparation_chunk_size(256), 256);
    assert_eq!(preparation_chunk_size(10_000), 256);
}

#[test]
fn preparation_chunk_admits_one_bounded_batch_workspace_before_exact_retention() {
    let bytes = 128 * 1_024;
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

    let construction = acquire_preparation_construction(&credits, 256).unwrap();
    let expected = 256 * std::mem::size_of::<Mutation>();
    assert_eq!(construction.bytes(), expected);
    assert_eq!(
        credits.stage_used_bytes(IndexingMemoryStage::ReplayInput),
        expected,
        "descriptor-only loading does not reserve a configured worker workspace"
    );
    assert!(expected < 256 * 1_024);
    drop(construction);
    assert_eq!(
        credits.stage_used_bytes(IndexingMemoryStage::ReplayInput),
        0
    );
}

#[test]
fn journal_read_ahead_tracks_worker_capacity_without_becoming_unbounded() {
    assert_eq!(journal_read_ahead_pages(0), 2);
    assert_eq!(journal_read_ahead_pages(1), 2);
    assert_eq!(journal_read_ahead_pages(4), 8);
    assert_eq!(journal_read_ahead_pages(64), 32);
}

#[test]
fn producer_advance_slices_have_a_hard_operation_ceiling() {
    assert_eq!(bounded_advance_operations(u64::MAX), 4_096);
    assert_eq!(bounded_advance_operations(8_192), 4_096);
    assert_eq!(bounded_advance_operations(512), 512);
    assert_eq!(bounded_advance_operations(0), 1);
    assert_eq!(MAX_ADVANCE_SLICE_BYTES, 4 * 1024 * 1024);
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
fn publication_chunks_preserve_named_atomic_groups_and_contiguous_cuts() {
    let mutations = vec![
        mutation("objects/a", 1),
        Mutation {
            atomic_group: Some(9),
            ..mutation("objects/b", 2)
        },
        Mutation {
            ordinal: 1,
            atomic_group: Some(9),
            ..mutation("objects/c", 2)
        },
        mutation("objects/d", 3),
        mutation("objects/e", 4),
    ];

    let chunks = publication_chunks(
        mutations,
        5,
        2,
        9,
        9,
        |mutation| mutation.offset,
        |mutation| mutation.atomic_group,
    )
    .unwrap();

    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].mutations.len(), 3);
    assert_eq!(chunks[0].next, 3);
    assert_eq!(chunks[0].through_atomic, 9);
    assert_eq!(chunks[1].mutations.len(), 2);
    assert_eq!(chunks[1].next, 5);
    assert_eq!(chunks[1].through_atomic, 9);
    assert!(chunks.iter().all(|chunk| {
        chunk
            .mutations
            .windows(2)
            .all(|pair| (pair[0].offset, pair[0].ordinal) < (pair[1].offset, pair[1].ordinal))
    }));
}

#[test]
fn one_large_atomic_group_remains_indivisible() {
    let mutations = (0..8)
        .map(|ordinal| Mutation {
            ordinal,
            atomic_group: Some(77),
            ..mutation(&format!("objects/{ordinal}"), 7)
        })
        .collect();

    let chunks = publication_chunks(
        mutations,
        8,
        2,
        76,
        77,
        |mutation| mutation.offset,
        |mutation| mutation.atomic_group,
    )
    .unwrap();

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].mutations.len(), 8);
    assert_eq!(chunks[0].next, 8);
    assert_eq!(chunks[0].through_atomic, 77);
}

#[test]
fn distinct_source_offsets_in_one_atomic_group_cross_the_slice_together() {
    let mut mutations = (1..=4_095)
        .map(|offset| mutation(&format!("ordinary/{offset}"), offset))
        .collect::<Vec<_>>();
    mutations.push(Mutation {
        atomic_group: Some(91),
        ..mutation("atomic/a", 4_096)
    });
    mutations.push(Mutation {
        atomic_group: Some(91),
        ..mutation("atomic/b", 4_097)
    });
    mutations.push(mutation("ordinary/tail", 4_098));

    let chunks = publication_chunks(
        mutations,
        4_099,
        4_096,
        91,
        91,
        |mutation| mutation.offset,
        |mutation| mutation.atomic_group,
    )
    .unwrap();

    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].mutations.len(), 4_097);
    assert_eq!(chunks[0].next, 4_098);
    assert_eq!(chunks[0].through_atomic, 91);
    assert_eq!(chunks[1].mutations.len(), 1);
    assert_eq!(chunks[1].next, 4_099);
    assert_eq!(chunks[1].through_atomic, 91);
}

#[test]
fn atomic_chunk_is_not_visible_at_an_older_cross_partition_common_cut() {
    let mutations = vec![
        Mutation {
            atomic_group: Some(91),
            ..mutation("atomic/a", 1)
        },
        Mutation {
            ordinal: 1,
            atomic_group: Some(91),
            ..mutation("atomic/b", 1)
        },
        mutation("ordinary/tail", 2),
    ];
    let chunks = publication_chunks(
        mutations,
        3,
        2,
        90,
        90,
        |mutation| mutation.offset,
        |mutation| mutation.atomic_group,
    )
    .unwrap();

    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].through_atomic, 91);
    let other_partition_through_atomic = 90;
    let common_cut = chunks[0].through_atomic.min(other_partition_through_atomic);
    assert_eq!(common_cut, 90);
    assert!(chunks[0].through_atomic > common_cut);
}

#[test]
fn superseding_mutation_cannot_split_an_atomic_unit_across_generations() {
    let units = vec![
        (
            91,
            vec![
                Mutation {
                    atomic_group: Some(91),
                    ..mutation("objects/a", 1)
                },
                Mutation {
                    ordinal: 1,
                    atomic_group: Some(91),
                    ..mutation("objects/b", 1)
                },
            ],
        ),
        (0, vec![mutation("objects/a", 2), mutation("ordinary/c", 3)]),
    ];
    let (page_atomic, mutations) = coalesce_units(90, units).unwrap();
    assert_eq!(
        mutations
            .iter()
            .map(|mutation| mutation.path.as_str())
            .collect::<Vec<_>>(),
        vec!["objects/b", "objects/a", "ordinary/c"]
    );

    let chunks = publication_chunks(
        mutations,
        4,
        2,
        90,
        page_atomic,
        |mutation| mutation.offset,
        |mutation| mutation.atomic_group,
    )
    .unwrap();

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].through_atomic, 91);
    assert_eq!(chunks[0].next, 4);
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
fn reopening_one_partition_replays_other_sources_from_their_own_retained_floor() {
    let credits = retry_credits();
    let writer = retry_writer(&credits);
    let mut target = writer.scanned.clone();
    target.sources.get_mut(&NodeId(1)).unwrap().next_offset = 50;
    let other = super::super::events::IndexSourceCursor {
        source: SourceId {
            node_id: 2,
            source_epoch: [8; 32],
        },
        next_offset: 500,
    };
    target.sources.insert(NodeId(2), other);
    let mut retained = target.clone();
    retained.sources.get_mut(&NodeId(1)).unwrap().next_offset = 10;
    retained.sources.get_mut(&NodeId(2)).unwrap().next_offset = 401;
    let scanned = owned_source_scan_start(&target, &retained, writer.source, 12, true).unwrap();
    assert_eq!(scanned.sources[&NodeId(1)].next_offset, 12);
    assert_eq!(scanned.sources[&NodeId(2)].source, other.source);
    assert_eq!(scanned.sources[&NodeId(2)].next_offset, 401);
    assert!(owned_source_scan_start(&target, &retained, writer.source, 9, true).is_err());
    let fresh = owned_source_scan_start(&target, &retained, writer.source, 1, false).unwrap();
    assert_eq!(fresh.sources[&NodeId(1)].next_offset, 1);
    assert_eq!(fresh.sources[&NodeId(2)].next_offset, 401);
    let mut wrong_epoch = writer.source;
    wrong_epoch.source_epoch = [9; 32];
    assert!(owned_source_scan_start(&target, &retained, wrong_epoch, 12, true).is_err());
}

#[test]
fn atomic_replay_target_remains_fixed_while_writes_extend_the_live_tail() {
    let credits = retry_credits();
    let mut writer = retry_writer(&credits);
    writer.current = Some(loaded_current(
        writer.recipe.physical_generation,
        VersionId(3),
    ));
    let mut captured = writer.scanned.clone();
    captured.atomic = super::super::events::AtomicProgramWatermark::new(Some(20), Some(20), 0);
    atomic_progress::capture_replay_target(&mut writer, &captured, &credits).unwrap();
    let mut live = captured.clone();
    live.sources.get_mut(&NodeId(1)).unwrap().next_offset += 10_000;
    live.atomic = super::super::events::AtomicProgramWatermark::new(Some(30), Some(30), 0);
    atomic_progress::capture_replay_target(&mut writer, &live, &credits).unwrap();
    assert_eq!(writer.atomic_replay_target.as_ref().unwrap().0, captured);
}

#[test]
fn quiet_partition_atomic_ack_requires_complete_foreign_replay_not_max_observed_cursor() {
    let credits = retry_credits();
    let mut writer = retry_writer(&credits);
    writer.dispatcher = Some(V1OrderedSourceDispatcher::new(
        writer.scanned.fence,
        BTreeSet::from([writer.source]),
        credits
            .acquire(IndexingMemoryStage::ReplayInput, 1)
            .unwrap(),
        64 * 1024,
    ));
    writer.current = Some(loaded_current(
        writer.recipe.physical_generation,
        VersionId(3),
    ));
    writer.through_atomic = 12;
    let mut target = writer.scanned.clone();
    target.atomic = super::super::events::AtomicProgramWatermark::new(Some(20), Some(20), 0);
    let other = super::super::events::IndexSourceCursor {
        source: SourceId {
            node_id: 2,
            source_epoch: [8; 32],
        },
        next_offset: 402,
    };
    target.sources.insert(NodeId(2), other);
    let mut before_other = other;
    before_other.next_offset = 401;
    writer.scanned.sources.insert(NodeId(2), before_other);
    assert_eq!(
        atomic_progress::complete_atomic_replay(&writer, &target).unwrap(),
        None
    );
    let own_before = writer_processed_next(&writer);
    let progress_before = writer_dispatch_progress(&writer);
    writer.scanned.sources.insert(NodeId(2), other);
    assert_eq!(writer_processed_next(&writer), own_before);
    assert!(should_reschedule_after_advance(
        ProducerStage::JournalScan,
        progress_before,
        writer_dispatch_progress(&writer)
    ));
    assert_eq!(
        atomic_progress::complete_atomic_replay(&writer, &target).unwrap(),
        Some((20, 12))
    );
    writer.pending_prepared_rows = 1;
    assert_eq!(
        atomic_progress::complete_atomic_replay(&writer, &target).unwrap(),
        None
    );
    writer.pending_prepared_rows = 0;
    target.fence.index += 1;
    assert_eq!(
        atomic_progress::complete_atomic_replay(&writer, &target).unwrap(),
        None
    );
}

#[test]
fn admitted_empty_cut_advances_native_checkpoint_without_inventing_rows_or_components() {
    let credits = retry_credits();
    let mut writer = retry_writer(&credits);
    let before = writer.accumulator.next_offset();
    let limits = Limits {
        bytes: 1024 * 1024,
        flush_bytes: 64,
        projection_batch_bytes: 256,
        flush_age: Duration::from_secs(1),
        flush_operations: 64,
        lsm_runs: 64,
        lsm_bytes: 1024,
        parallelism: 1,
        worker_bytes: 64,
    };
    apply_rows(&mut writer, before + 5, Vec::new(), &credits, limits).unwrap();
    assert_eq!(writer.accumulator.next_offset(), before + 5);
    assert_eq!(writer.pending_prepared_rows, 0);
    assert_eq!(writer.pending_projected_rows, 0);
    assert!(
        writer.since.is_none(),
        "only an external proof can arm an empty publication"
    );
    let sealed = writer.accumulator.seal_and_reset().unwrap().into_parts().0;
    assert_eq!(sealed.checkpoint.next_offset, before + 5);
    assert!(sealed.deltas.is_empty());
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
        selection_memory: None,
    };

    let retained = selected_mutation_resident_bytes(&mutation, &selected).unwrap();

    assert!(retained < 1024);
    assert!(retained >= std::mem::size_of::<SelectedMutation>());
}

#[test]
fn completed_compaction_is_harvested_without_new_source_work() {
    assert!(should_harvest_background_compaction(true, 0, true, true));
    assert!(!should_harvest_background_compaction(true, 1, true, true));
    assert!(!should_harvest_background_compaction(true, 0, false, true));
    assert!(!should_harvest_background_compaction(true, 0, true, false));
}
