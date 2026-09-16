use super::*;
use crate::{
    BatchOperation, BlobRef, Durability, ObjectKey, ObjectMutationContext,
    ObjectMutationGovernance, PlacementLogId, PutMode, PutRequest, ReferenceDelta, StoreOptions,
    WatchCursor, WatchScope,
};

fn status(tail: u64, entries: u64, bytes: u64) -> WatchJournalStatus {
    WatchJournalStatus {
        source_id: crate::SourceId {
            node_id: 1,
            source_epoch: [7; 32],
        },
        tail,
        settled_through: tail,
        retention_floor: 0,
        retained_entries: entries,
        retained_bytes: bytes,
    }
}

#[test]
fn completion_encoding_is_fixed_and_rejects_range_mismatch() {
    let completion = LaneCompletion {
        ticket: 9,
        first_offset: 11,
        last_offset: 13,
        journal_entries: 3,
        journal_bytes: 707,
        receipt_entries: 2,
        receipt_bytes: 808,
        high_version: Some(VersionId(91)),
        reference_cursor_advanced: true,
        inline_reference_safe: true,
        visibility_settled: true,
    };
    assert_eq!(
        LaneCompletion::decode(&completion.encode()).unwrap(),
        completion
    );
    let mut malformed = completion.encode();
    malformed[25..33].copy_from_slice(&2_u64.to_be_bytes());
    assert!(LaneCompletion::decode(&malformed).is_err());
}

#[test]
fn reservation_assigns_monotonic_tickets_and_exact_source_ranges() {
    let initial = status(4, 4, 400);
    let mut runtime = LaneRuntime {
        next_ticket: 7,
        projected_ticket: 7,
        reserved_watch: initial,
        reserved_receipts: MutationReceiptStatus {
            entries: 2,
            bytes: 20,
        },
        projected_watch: initial,
        projected_receipts: MutationReceiptStatus {
            entries: 2,
            bytes: 20,
        },
        projected_high_version: Some(VersionId(10)),
        reserved_reference_cursor_safe: true,
        reserved_inline_reference_safe: true,
        completions: BTreeMap::new(),
        visibility_proofs: BTreeSet::new(),
        visibility_prefix_proof: None,
    };
    let completion = runtime
        .reserve(
            status(6, 6, 650),
            MutationReceiptStatus {
                entries: 3,
                bytes: 35,
            },
            Some(VersionId(12)),
        )
        .unwrap();
    assert_eq!(completion.ticket, 8);
    assert_eq!((completion.first_offset, completion.last_offset), (5, 6));
    assert_eq!(
        (completion.journal_entries, completion.journal_bytes),
        (2, 250)
    );
    assert_eq!(
        (completion.receipt_entries, completion.receipt_bytes),
        (1, 15)
    );
}

#[test]
fn reservations_preserve_a_contiguous_reference_safe_frontier() {
    let initial = status(0, 0, 0);
    let receipts = MutationReceiptStatus {
        entries: 0,
        bytes: 0,
    };
    let mut runtime = LaneRuntime {
        next_ticket: 0,
        projected_ticket: 0,
        reserved_watch: initial,
        reserved_receipts: receipts,
        projected_watch: initial,
        projected_receipts: receipts,
        projected_high_version: None,
        reserved_reference_cursor_safe: true,
        reserved_inline_reference_safe: true,
        completions: BTreeMap::new(),
        visibility_proofs: BTreeSet::new(),
        visibility_prefix_proof: None,
    };

    let first = runtime
        .reserve_with_reference_settlement(status(1, 1, 100), receipts, None, true, true, true)
        .unwrap();
    let second = runtime
        .reserve_with_reference_settlement(status(2, 2, 200), receipts, None, true, true, true)
        .unwrap();

    assert!(first.reference_cursor_advanced);
    assert!(first.inline_reference_safe);
    assert!(second.reference_cursor_advanced);
    assert!(second.inline_reference_safe);
}

#[test]
fn reservations_do_not_jump_an_unsettled_reference_frontier() {
    let initial = status(1, 1, 100);
    let receipts = MutationReceiptStatus {
        entries: 0,
        bytes: 0,
    };
    let mut runtime = LaneRuntime {
        next_ticket: 0,
        projected_ticket: 0,
        reserved_watch: initial,
        reserved_receipts: receipts,
        projected_watch: initial,
        projected_receipts: receipts,
        projected_high_version: None,
        reserved_reference_cursor_safe: false,
        reserved_inline_reference_safe: false,
        completions: BTreeMap::new(),
        visibility_proofs: BTreeSet::new(),
        visibility_prefix_proof: None,
    };

    let completion = runtime
        .reserve_with_reference_settlement(status(2, 2, 200), receipts, None, true, true, true)
        .unwrap();

    assert!(!completion.reference_cursor_advanced);
    assert!(!completion.inline_reference_safe);
}

#[test]
fn no_reference_artifact_keeps_cursor_safe_without_skipping_retention_consumers() {
    let initial = status(0, 0, 0);
    let receipts = MutationReceiptStatus {
        entries: 0,
        bytes: 0,
    };
    let mut runtime = LaneRuntime {
        next_ticket: 0,
        projected_ticket: 0,
        reserved_watch: initial,
        reserved_receipts: receipts,
        projected_watch: initial,
        projected_receipts: receipts,
        projected_high_version: None,
        reserved_reference_cursor_safe: true,
        reserved_inline_reference_safe: true,
        completions: BTreeMap::new(),
        visibility_proofs: BTreeSet::new(),
        visibility_prefix_proof: None,
    };

    let artifact = runtime
        .reserve_with_reference_settlement(status(1, 1, 100), receipts, None, true, false, true)
        .unwrap();
    let foreground = runtime
        .reserve_with_reference_settlement(status(2, 2, 200), receipts, None, true, true, true)
        .unwrap();

    assert!(artifact.reference_cursor_advanced);
    assert!(!artifact.inline_reference_safe);
    assert!(foreground.reference_cursor_advanced);
    assert!(!foreground.inline_reference_safe);
}

#[tokio::test]
async fn caught_up_cursors_rearm_conservative_reference_frontiers() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let mut runtime = store.mutation_commit_lanes.sequence().await;
    let runtime = runtime.as_mut().unwrap();
    runtime.reserved_reference_cursor_safe = false;
    runtime.reserved_inline_reference_safe = false;

    store
        .rearm_caught_up_reference_frontiers(runtime, 0)
        .unwrap();

    assert!(runtime.reserved_reference_cursor_safe);
    assert!(runtime.reserved_inline_reference_safe);
}

#[tokio::test]
async fn caught_up_rearm_rejects_a_cursor_beyond_reserved_authority() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let mut runtime = store.mutation_commit_lanes.sequence().await;
    let runtime = runtime.as_mut().unwrap();

    let error = store
        .rearm_caught_up_reference_frontiers(runtime, 1)
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("beyond reserved source-journal tail")
    );
}

#[tokio::test]
async fn out_of_order_completion_does_not_advance_across_live_ticket() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let (first, second) = {
        let mut runtime = store.mutation_commit_lanes.sequence().await;
        let runtime = runtime.as_mut().unwrap();
        let watch = runtime.reserved_watch;
        let receipts = runtime.reserved_receipts;
        (
            runtime.reserve(watch, receipts, None).unwrap(),
            runtime.reserve(watch, receipts, None).unwrap(),
        )
    };
    for completion in [first, second] {
        let mut batch = WriteBatch::default();
        store.stage_lane_completion(&mut batch, completion).unwrap();
        store.db.write(batch).unwrap();
    }

    let later_finish = tokio::spawn({
        let store = store.clone();
        async move { store.finish_lane_commit(second, true).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(
        !later_finish.is_finished(),
        "a later response must wait for the contiguous durable frontier"
    );
    let encoded = store
        .db
        .get_cf(store.cf(CF_METADATA).unwrap(), LANE_FRONTIER_KEY)
        .unwrap()
        .unwrap();
    assert_eq!(
        u64::from_be_bytes(encoded.as_slice().try_into().unwrap()),
        0
    );

    let first_metrics = store.finish_lane_commit(first, true).await.unwrap();
    let later_metrics = later_finish.await.unwrap().unwrap();
    assert_eq!(first_metrics.completion_reorder_depth, 0);
    assert_eq!(first_metrics.completion_ticket_lag, 1);
    assert_eq!(first_metrics.contiguous_projection_completions, 2);
    assert!(first_metrics.projection_write > Duration::ZERO);
    assert_eq!(later_metrics.completion_reorder_depth, 1);
    assert_eq!(later_metrics.completion_ticket_lag, 2);
    assert_eq!(later_metrics.contiguous_projection_completions, 0);
    assert!(later_metrics.ordered_frontier_wait > Duration::ZERO);
    let encoded = store
        .db
        .get_cf(store.cf(CF_METADATA).unwrap(), LANE_FRONTIER_KEY)
        .unwrap()
        .unwrap();
    assert_eq!(
        u64::from_be_bytes(encoded.as_slice().try_into().unwrap()),
        2
    );
}

#[tokio::test]
async fn out_of_order_reference_completion_does_not_cross_visibility_frontier() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let make_change = |offset| {
        LocalChange::object_head(
            offset,
            tenant_id,
            bucket_id,
            format!("objects/{offset}"),
            VersionId(offset),
            false,
            vec![ReferenceDelta {
                blob: BlobRef {
                    hash: [offset as u8; 32],
                    length: 8,
                },
                change: 1,
            }],
            None,
            None,
        )
    };
    let mut completions = Vec::new();
    for offset in [1, 2] {
        let change = make_change(offset);
        let encoded = encode_local_change(&change).unwrap();
        let completion = LaneCompletion {
            ticket: offset,
            first_offset: offset,
            last_offset: offset,
            journal_entries: 1,
            journal_bytes: invalidation_record_bytes(encoded.len()),
            receipt_entries: 0,
            receipt_bytes: 0,
            high_version: Some(VersionId(offset)),
            reference_cursor_advanced: true,
            inline_reference_safe: true,
            visibility_settled: true,
        };
        let mut batch = WriteBatch::default();
        batch.put_cf(
            store.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
            invalidation_key(offset),
            encoded,
        );
        store.stage_lane_completion(&mut batch, completion).unwrap();
        store.db.write(batch).unwrap();
        completions.push(completion);
    }

    let later_finish = tokio::spawn({
        let store = store.clone();
        let completion = completions[1];
        async move { store.finish_lane_commit(completion, true).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(
        !later_finish.is_finished(),
        "reference visibility must wait for the contiguous frontier"
    );
    assert_eq!(store.local_watch_status().unwrap().tail, 0);
    store
        .finish_lane_commit(completions[0], true)
        .await
        .unwrap();
    later_finish.await.unwrap().unwrap();

    let status = store.local_watch_status().unwrap();
    assert_eq!((status.tail, status.settled_through), (2, 2));
    let page = store
        .scan_watch_page(
            &WatchScope::new("tenant", "bucket", "").unwrap(),
            WatchCursor::new(0),
            8,
        )
        .await
        .unwrap();
    assert_eq!(page.invalidations.len(), 2);
    assert_eq!(page.checkpoint.offset(), 2);
}

#[tokio::test]
async fn quorum_visibility_proofs_batch_and_never_cross_a_gap() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let source = store.local_watch_status().unwrap().source_id;
    for offset in [1_u64, 2] {
        let change = LocalChange::sequence_gap(offset);
        let encoded = encode_local_change(&change).unwrap();
        let completion = {
            let mut runtime = store.mutation_commit_lanes.sequence().await;
            let runtime = runtime.as_mut().unwrap();
            let mut watch = runtime.reserved_watch;
            watch.tail = offset;
            watch.retained_entries += 1;
            watch.retained_bytes += invalidation_record_bytes(encoded.len());
            let receipts = runtime.reserved_receipts;
            runtime
                .reserve_with_reference_settlement(watch, receipts, None, false, false, false)
                .unwrap()
        };
        let mut batch = WriteBatch::default();
        batch.put_cf(
            store.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
            invalidation_key(offset),
            encoded,
        );
        store.stage_lane_completion(&mut batch, completion).unwrap();
        store.db.write(batch).unwrap();
        store.finish_lane_commit(completion, true).await.unwrap();
    }
    assert_eq!(store.local_watch_status().unwrap().settled_through, 0);

    assert_eq!(
        store
            .settle_source_journal_positions_if_contiguous(source, &[2])
            .await
            .unwrap(),
        None
    );
    assert_eq!(store.local_watch_status().unwrap().settled_through, 0);
    assert_eq!(
        store
            .settle_source_journal_positions_if_contiguous(source, &[1])
            .await
            .unwrap(),
        Some(2)
    );
    assert_eq!(store.local_watch_status().unwrap().settled_through, 2);
}

#[tokio::test]
async fn recovered_visibility_prefix_uses_one_bounded_projection_request() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let source = store.local_watch_status().unwrap().source_id;
    for offset in 1..=3 {
        let encoded = encode_local_change(&LocalChange::sequence_gap(offset)).unwrap();
        let completion = {
            let mut runtime = store.mutation_commit_lanes.sequence().await;
            let runtime = runtime.as_mut().unwrap();
            let mut reserved = runtime.reserved_watch;
            reserved.tail = offset;
            reserved.retained_entries += 1;
            reserved.retained_bytes += invalidation_record_bytes(encoded.len());
            let receipts = runtime.reserved_receipts;
            runtime
                .reserve_with_reference_settlement(reserved, receipts, None, false, false, false)
                .unwrap()
        };
        let mut batch = WriteBatch::default();
        batch.put_cf(
            store.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
            invalidation_key(offset),
            encoded,
        );
        store.stage_lane_completion(&mut batch, completion).unwrap();
        store.db.write(batch).unwrap();
        store.finish_lane_commit(completion, true).await.unwrap();
    }

    store
        .settle_lane_source_journal_through(source, 3)
        .await
        .unwrap();
    assert_eq!(store.local_watch_status().unwrap().settled_through, 3);
    let runtime = store.mutation_commit_lanes.sequence().await;
    assert!(runtime.as_ref().unwrap().visibility_proofs.is_empty());
    assert!(runtime.as_ref().unwrap().visibility_prefix_proof.is_none());
}

#[tokio::test]
async fn projection_failure_preserves_runtime_and_retry_advances_retention_authority() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let change = LocalChange::sequence_gap(1);
    let encoded = encode_local_change(&change).unwrap();
    let completion = {
        let mut runtime = store.mutation_commit_lanes.sequence().await;
        let runtime = runtime.as_mut().unwrap();
        let mut watch = runtime.reserved_watch;
        watch.tail = 1;
        watch.retained_entries = 1;
        watch.retained_bytes = invalidation_record_bytes(encoded.len());
        let receipts = runtime.reserved_receipts;
        runtime.reserve(watch, receipts, None).unwrap()
    };
    let mut batch = WriteBatch::default();
    batch.put_cf(
        store.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
        invalidation_key(1),
        encoded,
    );
    store.stage_lane_completion(&mut batch, completion).unwrap();
    store.db.write(batch).unwrap();

    store
        .mutation_commit_lanes
        .fail_next_projection
        .store(true, Ordering::Release);
    assert!(store.finish_lane_commit(completion, true).await.is_err());
    {
        let runtime = store.mutation_commit_lanes.sequence().await;
        let runtime = runtime.as_ref().unwrap();
        assert_eq!(runtime.projected_ticket, 0);
        assert!(runtime.completions.contains_key(&completion.ticket));
    }
    assert_eq!(store.local_watch_status().unwrap().tail, 0);
    assert!(
        store
            .db
            .get_cf(store.cf(CF_METADATA).unwrap(), completion.key())
            .unwrap()
            .is_some()
    );

    let retry = store.project_lane_completions().await.unwrap();
    assert_eq!(retry.completions, 1);
    assert!(retry.write > Duration::ZERO);
    assert_eq!(store.local_watch_status().unwrap().tail, 1);
    assert_eq!(
        store
            .source_journal_reference_safe_through
            .load(Ordering::Acquire),
        1
    );
    assert!(
        store
            .db
            .get_cf(store.cf(CF_METADATA).unwrap(), completion.key())
            .unwrap()
            .is_none()
    );
    assert!(store.prune_source_journal_for_capacity().await.unwrap());
    assert_eq!(store.local_watch_status().unwrap().retention_floor, 1);
}

#[tokio::test]
async fn completion_disambiguation_read_failure_retries_without_losing_the_ticket() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let completion = {
        let mut runtime = store.mutation_commit_lanes.sequence().await;
        let runtime = runtime.as_mut().unwrap();
        let watch = runtime.reserved_watch;
        let receipts = runtime.reserved_receipts;
        runtime.reserve(watch, receipts, None).unwrap()
    };
    let mut batch = WriteBatch::default();
    store.stage_lane_completion(&mut batch, completion).unwrap();
    store.db.write(batch).unwrap();
    store
        .mutation_commit_lanes
        .fail_next_completion_disambiguation
        .store(true, Ordering::Release);

    store
        .finish_lane_commit_cancellation_safe(completion, false)
        .await
        .unwrap();

    let runtime = store.mutation_commit_lanes.sequence().await;
    let runtime = runtime.as_ref().unwrap();
    assert_eq!(runtime.projected_ticket, completion.ticket);
    assert!(!runtime.completions.contains_key(&completion.ticket));
    assert!(
        store
            .db
            .get_cf(store.cf(CF_METADATA).unwrap(), completion.key())
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn cancelled_direct_caller_cannot_leave_a_durable_ticket_hole() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    store
        .mutation_commit_lanes
        .pause_next_cancellation_safe_settlement();
    let before_write = store.db.latest_sequence_number();
    let first_key = ObjectKey::new("tenant", "bucket", "objects/cancelled-direct").unwrap();
    let first = tokio::spawn({
        let store = store.clone();
        let key = first_key.clone();
        async move {
            store
                .put(PutRequest {
                    key,
                    bytes: b"first".to_vec(),
                    content_type: Some("application/octet-stream".into()),
                    mode: PutMode::PutIfAbsent,
                    command_id: Some("cancelled-direct-first".into()),
                    durability: Durability::Local,
                })
                .await
        }
    });
    store
        .mutation_commit_lanes
        .wait_for_cancellation_safe_settlement()
        .await;
    assert!(store.db.latest_sequence_number() > before_write);

    first.abort();
    let _ = first.await;
    store
        .mutation_commit_lanes
        .resume_cancellation_safe_settlement();

    let second_key = ObjectKey::new("tenant", "bucket", "objects/after-cancel").unwrap();
    tokio::time::timeout(
        Duration::from_secs(2),
        store.put(PutRequest {
            key: second_key.clone(),
            bytes: b"second".to_vec(),
            content_type: Some("application/octet-stream".into()),
            mode: PutMode::PutIfAbsent,
            command_id: Some("cancelled-direct-second".into()),
            durability: Durability::Local,
        }),
    )
    .await
    .expect("a later direct mutation settles without restarting the Store")
    .unwrap();

    assert!(store.get(&first_key).await.unwrap().is_some());
    assert!(store.get(&second_key).await.unwrap().is_some());
    let runtime = store.mutation_commit_lanes.sequence().await;
    let runtime = runtime.as_ref().unwrap();
    assert_eq!(runtime.projected_ticket, runtime.next_ticket);
    assert!(runtime.completions.is_empty());
}

#[tokio::test]
async fn projection_write_releases_sequence_and_merges_newer_runtime_state() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let first = {
        let mut runtime = store.mutation_commit_lanes.sequence().await;
        let runtime = runtime.as_mut().unwrap();
        let watch = runtime.reserved_watch;
        runtime
            .reserve(
                watch,
                MutationReceiptStatus {
                    entries: 1,
                    bytes: 10,
                },
                Some(VersionId(1)),
            )
            .unwrap()
    };
    let mut batch = WriteBatch::default();
    store.stage_lane_completion(&mut batch, first).unwrap();
    store.db.write(batch).unwrap();
    store
        .mutation_commit_lanes
        .pause_next_projection
        .store(true, Ordering::Release);
    let finishing_first = tokio::spawn({
        let store = store.clone();
        async move { store.finish_lane_commit(first, true).await }
    });
    store
        .mutation_commit_lanes
        .projection_write_completed
        .acquire()
        .await
        .unwrap()
        .forget();

    let second = {
        let mut runtime = store.mutation_commit_lanes.sequence().await;
        let runtime = runtime.as_mut().unwrap();
        let watch = runtime.reserved_watch;
        let completion = runtime
            .reserve(
                watch,
                MutationReceiptStatus {
                    entries: 2,
                    bytes: 30,
                },
                Some(VersionId(2)),
            )
            .unwrap();
        assert!(
            runtime
                .completions
                .insert(
                    completion.ticket,
                    LaneCompletionState::Committed(completion)
                )
                .is_none()
        );
        completion
    };
    let mut batch = WriteBatch::default();
    store.stage_lane_completion(&mut batch, second).unwrap();
    store.db.write(batch).unwrap();
    store
        .mutation_commit_lanes
        .projection_publish_continue
        .add_permits(1);
    finishing_first.await.unwrap().unwrap();

    {
        let runtime = store.mutation_commit_lanes.sequence().await;
        let runtime = runtime.as_ref().unwrap();
        assert_eq!((runtime.projected_ticket, runtime.next_ticket), (1, 2));
        assert_eq!(
            runtime.projected_receipts,
            MutationReceiptStatus {
                entries: 1,
                bytes: 10,
            }
        );
        assert_eq!(
            runtime.reserved_receipts,
            MutationReceiptStatus {
                entries: 2,
                bytes: 30,
            }
        );
        assert_eq!(
            runtime.completions.get(&second.ticket),
            Some(&LaneCompletionState::Committed(second))
        );
    }

    let projection = store.project_lane_completions().await.unwrap();
    assert_eq!(projection.completions, 1);
    let runtime = store.mutation_commit_lanes.sequence().await;
    let runtime = runtime.as_ref().unwrap();
    assert_eq!((runtime.projected_ticket, runtime.next_ticket), (2, 2));
    assert_eq!(runtime.projected_receipts, runtime.reserved_receipts);
    assert!(runtime.completions.is_empty());
}

#[tokio::test]
async fn durable_projection_ahead_of_runtime_does_not_reject_a_lane_reservation() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let governance = ObjectMutationGovernance {
        tenant_id,
        bucket_id,
        versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
        policy: store.bucket_policy("tenant", "bucket").unwrap(),
    };
    let change = LocalChange::sequence_gap(1);
    let encoded = encode_local_change(&change).unwrap();
    let (completion, source_id) = {
        let mut runtime = store.mutation_commit_lanes.sequence().await;
        let runtime = runtime.as_mut().unwrap();
        let mut watch = runtime.reserved_watch;
        let source_id = watch.source_id;
        watch.tail = 1;
        watch.retained_entries = 1;
        watch.retained_bytes = invalidation_record_bytes(encoded.len());
        let receipts = runtime.reserved_receipts;
        (runtime.reserve(watch, receipts, None).unwrap(), source_id)
    };
    let mut batch = WriteBatch::default();
    batch.put_cf(
        store.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
        invalidation_key(1),
        encoded,
    );
    store.stage_lane_completion(&mut batch, completion).unwrap();
    store.db.write(batch).unwrap();
    store
        .mutation_commit_lanes
        .pause_next_projection
        .store(true, Ordering::Release);
    let finishing_projection = tokio::spawn({
        let store = store.clone();
        async move { store.finish_lane_commit(completion, true).await }
    });
    store
        .mutation_commit_lanes
        .projection_write_completed
        .acquire()
        .await
        .unwrap()
        .forget();
    assert_eq!(store.reference_delta_cursor(source_id).unwrap(), 1);
    {
        let runtime = store.mutation_commit_lanes.sequence().await;
        assert_eq!(runtime.as_ref().unwrap().projected_watch.tail, 0);
    }

    let group = tokio::spawn({
        let store = store.clone();
        async move {
            store
                .coordinate_single_node_mutation_batch(
                    vec![(
                        BatchOperation::Put(PutRequest {
                            key: ObjectKey::new("tenant", "bucket", "objects/next").unwrap(),
                            bytes: b"next".to_vec(),
                            content_type: Some("application/octet-stream".into()),
                            mode: PutMode::PutIfAbsent,
                            command_id: Some("next-command".into()),
                            durability: Durability::Local,
                        }),
                        governance,
                        None,
                    )],
                    ObjectMutationContext {
                        active_placement_log_id: PlacementLogId { term: 1, index: 1 },
                        serving_fence_term: 1,
                    },
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let next_ticket = store
                .mutation_commit_lanes
                .sequence()
                .await
                .as_ref()
                .unwrap()
                .next_ticket;
            if next_ticket == 2 {
                break;
            }
            assert!(
                !group.is_finished(),
                "the lane group failed before reserving its source range"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the lane group reserves while volatile projection publication is paused");
    assert!(
        !group.is_finished(),
        "the lane group still waits for ordered projection acknowledgement"
    );

    store
        .mutation_commit_lanes
        .projection_publish_continue
        .add_permits(1);
    finishing_projection.await.unwrap().unwrap();
    let outcomes = group.await.unwrap().unwrap();
    assert_eq!(outcomes.len(), 1);
    assert!(outcomes[0].is_ok());
}

#[tokio::test]
async fn restart_closes_abandoned_source_gap_before_later_completion() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let later = LocalChange::sequence_gap(2);
    let encoded = encode_local_change(&later).unwrap();
    let completion = LaneCompletion {
        ticket: 2,
        first_offset: 2,
        last_offset: 2,
        journal_entries: 1,
        journal_bytes: invalidation_record_bytes(encoded.len()),
        receipt_entries: 0,
        receipt_bytes: 0,
        high_version: None,
        reference_cursor_advanced: true,
        inline_reference_safe: true,
        visibility_settled: true,
    };
    let mut batch = WriteBatch::default();
    batch.put_cf(
        store.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
        invalidation_key(2),
        encoded,
    );
    store.stage_lane_completion(&mut batch, completion).unwrap();
    store.db.write(batch).unwrap();
    drop(store);

    let recovered = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let status = recovered.local_watch_status().unwrap();
    assert_eq!((status.tail, status.settled_through), (2, 2));
    assert_eq!(
        recovered.reference_delta_cursor(status.source_id).unwrap(),
        2
    );
    let gap = recovered
        .db
        .get_cf(
            recovered.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
            invalidation_key(1),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        crate::watch::decode_local_change(&gap).unwrap(),
        LocalChange::sequence_gap(1)
    );
}

#[tokio::test]
async fn shared_resource_excludes_a_second_lane() {
    let lanes = MutationCommitLanes::new(4);
    let first = lanes.acquire([b"path:a".to_vec()]).await;
    let waiting = tokio::spawn({
        let lanes = lanes.clone();
        async move { lanes.acquire([b"path:a".to_vec()]).await }
    });
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());
    drop(first);
    waiting.await.unwrap();
}

#[tokio::test]
async fn direct_local_store_mutation_does_not_wait_for_an_unrelated_lane() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let held = store
        .mutation_commit_lanes
        .acquire([b"synthetic-unrelated-resource".to_vec()])
        .await;

    let receipt = tokio::time::timeout(
        Duration::from_secs(1),
        store.put(PutRequest {
            key: ObjectKey::new("tenant", "bucket", "objects/direct-local").unwrap(),
            bytes: b"direct-local".to_vec(),
            content_type: Some("application/octet-stream".into()),
            mode: PutMode::PutIfAbsent,
            command_id: Some("direct-local-command".into()),
            durability: Durability::Local,
        }),
    )
    .await
    .expect("an unrelated active lane must not impose an exclusive commit fence")
    .unwrap();
    assert!(!receipt.replayed);
    drop(held);
}

#[tokio::test]
async fn physical_slot_metrics_track_current_and_peak_utilization() {
    let lanes = MutationCommitLanes::new(2);
    let first = lanes.acquire([b"path:a".to_vec()]).await;
    assert_eq!(first.physical_slots_active_at_acquire(), 1);
    assert_eq!(first.physical_slots_peak_since_start_at_acquire(), 1);
    assert_eq!(first.physical_slot_count(), 2);

    let second = lanes.acquire([b"path:b".to_vec()]).await;
    assert_eq!(second.physical_slots_active_at_acquire(), 2);
    assert_eq!(second.physical_slots_peak_since_start_at_acquire(), 2);
    assert_eq!(first.physical_slots_active(), 2);
    assert_eq!(first.physical_slots_peak_since_start(), 2);
    drop(second);
    assert_eq!(first.physical_slots_active(), 1);
    drop(first);
    assert_eq!(lanes.physical_slots_active.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn physical_slot_can_be_released_without_releasing_conflict_guard() {
    let lanes = MutationCommitLanes::new(1);
    let mut first = lanes.acquire([b"path:a".to_vec()]).await;
    let conflicting = tokio::spawn({
        let lanes = lanes.clone();
        async move { lanes.acquire([b"path:a".to_vec()]).await }
    });
    let independent = tokio::spawn({
        let lanes = lanes.clone();
        async move { lanes.acquire([b"path:b".to_vec()]).await }
    });
    tokio::task::yield_now().await;
    assert!(!conflicting.is_finished());
    assert!(!independent.is_finished());

    first.release_physical_slot();
    let independent = tokio::time::timeout(Duration::from_secs(1), independent)
        .await
        .expect("an independent lane can use the released physical slot")
        .unwrap();
    assert!(
        !conflicting.is_finished(),
        "releasing the physical slot must retain the conflict guard"
    );

    drop(first);
    drop(independent);
    tokio::time::timeout(Duration::from_secs(1), conflicting)
        .await
        .expect("the conflict guard is released when the lane guard drops")
        .unwrap();
    assert_eq!(lanes.physical_slots_active.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn exclusive_fence_waits_for_lane_and_blocks_new_lanes() {
    let lanes = MutationCommitLanes::new(2);
    let first = lanes.acquire([b"path:a".to_vec()]).await;
    let exclusive = tokio::spawn({
        let lanes = lanes.clone();
        async move { lanes.acquire_exclusive().await }
    });
    tokio::task::yield_now().await;
    assert!(!exclusive.is_finished());
    drop(first);
    let exclusive = exclusive.await.unwrap();
    let waiting = tokio::spawn({
        let lanes = lanes.clone();
        async move { lanes.acquire([b"path:b".to_vec()]).await }
    });
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());
    drop(exclusive);
    waiting.await.unwrap();
}
