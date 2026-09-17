use super::*;
use crate::PlacementLogId;

fn request(path: &str, command: &str, blob: BlobRef) -> PublishRequest {
    PublishRequest {
        key: ObjectKey::new("tenant", "bucket", path).unwrap(),
        blob,
        content_type: Some("application/octet-stream".into()),
        mode: PutMode::PutIfAbsent,
        command_id: Some(command.into()),
        durability: Durability::Local,
    }
}

fn put_request(path: &str, command: &str, bytes: &[u8], durability: Durability) -> PutRequest {
    PutRequest {
        key: ObjectKey::new("tenant", "bucket", path).unwrap(),
        bytes: bytes.to_vec(),
        content_type: Some("application/octet-stream".into()),
        mode: PutMode::PutIfAbsent,
        command_id: Some(command.into()),
        durability,
    }
}

fn governed_put(
    path: &str,
    command: &str,
    bytes: &[u8],
    governance: ObjectMutationGovernance,
) -> (
    BatchOperation,
    ObjectMutationGovernance,
    Option<DefinitionMutationIntent>,
) {
    (
        BatchOperation::Put(put_request(path, command, bytes, Durability::Local)),
        governance,
        None,
    )
}

#[tokio::test]
async fn same_path_fifo_survives_the_max_group_boundary() {
    let temporary = tempfile::tempdir().unwrap();
    let config = SingleNodeGroupCommitConfig::new(
        1,
        5_000,
        64 * 1024 * 1024,
        64,
        8_000,
        128 * 1024 * 1024,
        std::time::Duration::from_secs(30),
    )
    .unwrap()
    .with_commit_lanes(2)
    .unwrap();
    let store =
        Store::open(StoreOptions::new(temporary.path(), 1).with_single_node_group_commit(config))
            .await
            .unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let governance = ObjectMutationGovernance {
        tenant_id,
        bucket_id,
        versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
        policy: store.bucket_policy("tenant", "bucket").unwrap(),
    };
    let context = ObjectMutationContext {
        active_placement_log_id: PlacementLogId { term: 1, index: 1 },
        serving_fence_term: 1,
    };
    store
        .single_node_group_commit
        .pause_next_group_registration();
    let first = tokio::spawn({
        let store = store.clone();
        let governance = governance.clone();
        async move {
            store
                .coordinate_single_node_mutation_batch(
                    vec![governed_put(
                        "objects/fifo",
                        "fifo-first",
                        b"first",
                        governance,
                    )],
                    context,
                )
                .await
        }
    });
    store
        .single_node_group_commit
        .wait_for_paused_group_registration()
        .await;
    // Model later execution work taking every bounded slot after the first
    // group has arrived but before it registers its complete conflict set. The
    // first group must reach this point without already holding a slot; under
    // the former ordering this reservation times out because the paused first
    // group consumed one permit before registration.
    let later_execution_capacity = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        store.single_node_group_commit.reserve_all_execution_slots(),
    )
    .await
    .expect("ordered conflict registration must precede execution-slot admission");
    let mut second = tokio::spawn({
        let store = store.clone();
        async move {
            store
                .coordinate_single_node_mutation_batch(
                    vec![governed_put(
                        "objects/fifo",
                        "fifo-second",
                        b"second",
                        governance,
                    )],
                    context,
                )
                .await
        }
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut second)
            .await
            .is_err(),
        "a later same-path group cannot pass the earlier unregistered group"
    );
    store.single_node_group_commit.resume_group_registration();
    drop(later_execution_capacity);

    assert!(matches!(first.await.unwrap().as_deref(), Ok([Ok(_)])));
    assert!(matches!(
        second.await.unwrap().as_deref(),
        Ok([Err(MutationError::PreconditionFailed { .. })])
    ));
    assert_eq!(
        store
            .get(&ObjectKey::new("tenant", "bucket", "objects/fifo").unwrap())
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"first"
    );
}

#[tokio::test]
async fn successor_waits_for_registration_without_holding_policy_guard() {
    let temporary = tempfile::tempdir().unwrap();
    let config = SingleNodeGroupCommitConfig::new(
        1,
        5_000,
        64 * 1024 * 1024,
        64,
        8_000,
        128 * 1024 * 1024,
        std::time::Duration::from_secs(30),
    )
    .unwrap()
    .with_commit_lanes(2)
    .unwrap();
    let store =
        Store::open(StoreOptions::new(temporary.path(), 1).with_single_node_group_commit(config))
            .await
            .unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let governance = ObjectMutationGovernance {
        tenant_id,
        bucket_id,
        versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
        policy: store.bucket_policy("tenant", "bucket").unwrap(),
    };
    let context = ObjectMutationContext {
        active_placement_log_id: PlacementLogId { term: 1, index: 1 },
        serving_fence_term: 1,
    };
    store
        .single_node_group_commit
        .pause_next_group_registration();
    let first = tokio::spawn({
        let store = store.clone();
        let governance = governance.clone();
        async move {
            store
                .coordinate_single_node_mutation_batch(
                    vec![governed_put(
                        "objects/guard-first",
                        "guard-first",
                        b"first",
                        governance,
                    )],
                    context,
                )
                .await
        }
    });
    store
        .single_node_group_commit
        .wait_for_paused_group_registration()
        .await;
    let second = tokio::spawn({
        let store = store.clone();
        async move {
            store
                .coordinate_single_node_mutation_batch(
                    vec![governed_put(
                        "objects/guard-second",
                        "guard-second",
                        b"second",
                        governance,
                    )],
                    context,
                )
                .await
        }
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        store
            .single_node_group_commit
            .wait_for_registration_predecessor(),
    )
    .await
    .expect("the successor reaches its predecessor handoff");

    let policy_writer = store
        .policy_gate
        .try_write()
        .expect("a registration successor must not hold the policy read guard");
    drop(policy_writer);
    store.single_node_group_commit.resume_group_registration();

    assert!(matches!(first.await.unwrap().as_deref(), Ok([Ok(_)])));
    assert!(matches!(second.await.unwrap().as_deref(), Ok([Ok(_)])));
}

#[tokio::test]
async fn distributed_requests_share_the_pre_lane_group_builder() {
    let temporary = tempfile::tempdir().unwrap();
    let config = SingleNodeGroupCommitConfig::new(
        5,
        5_000,
        64 * 1024 * 1024,
        64,
        8_000,
        128 * 1024 * 1024,
        std::time::Duration::from_secs(30),
    )
    .unwrap()
    .with_commit_lanes(4)
    .unwrap();
    let store =
        Store::open(StoreOptions::new(temporary.path(), 1).with_single_node_group_commit(config))
            .await
            .unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let governance = ObjectMutationGovernance {
        tenant_id,
        bucket_id,
        versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
        policy: store.bucket_policy("tenant", "bucket").unwrap(),
    };
    let context = ObjectMutationContext {
        active_placement_log_id: PlacementLogId { term: 1, index: 1 },
        serving_fence_term: 1,
    };
    let blobs = ["a", "b", "c", "d", "e"];
    let mut staged = Vec::new();
    for suffix in blobs {
        staged.push((suffix, store.stage_blob(suffix.as_bytes()).await.unwrap()));
    }
    let before = store.db.latest_sequence_number();
    let call = |suffix: &'static str, blob: BlobRef, governance: ObjectMutationGovernance| {
        let store = store.clone();
        async move {
            store
                .coordinate_distributed_publish_batch_with_governance(
                    vec![request(
                        &format!("objects/distributed-{suffix}"),
                        &format!("distributed-{suffix}"),
                        blob,
                    )],
                    governance,
                    context,
                )
                .await
        }
    };
    let mut staged = staged.into_iter();
    let (a_name, a_blob) = staged.next().unwrap();
    let (b_name, b_blob) = staged.next().unwrap();
    let (c_name, c_blob) = staged.next().unwrap();
    let (d_name, d_blob) = staged.next().unwrap();
    let (e_name, e_blob) = staged.next().unwrap();
    let (a, b, c, d, e) = tokio::join!(
        call(a_name, a_blob, governance.clone()),
        call(b_name, b_blob, governance.clone()),
        call(c_name, c_blob, governance.clone()),
        call(d_name, d_blob, governance.clone()),
        call(e_name, e_blob, governance),
    );

    for result in [a, b, c, d, e] {
        assert!(matches!(result.as_deref(), Ok([Ok(_)])));
    }
    assert_eq!(
        store
            .db
            .get_updates_since(before)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
            .len(),
        1
    );
    let status = store.local_watch_status().unwrap();
    assert!(status.settled_through < status.tail);
}

#[tokio::test]
async fn single_node_derived_publish_uses_inline_reference_lane_beyond_journal_limit() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreOptions::new(temporary.path(), 1)
            .with_watch_retention(WatchRetention::new(1, 1024 * 1024).unwrap()),
    )
    .await
    .unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let governance = ObjectMutationGovernance {
        tenant_id,
        bucket_id,
        versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
        policy: store.bucket_policy("tenant", "bucket").unwrap(),
    };
    let context = ObjectMutationContext {
        active_placement_log_id: PlacementLogId { term: 4, index: 2 },
        serving_fence_term: 4,
    };
    let initial = store
        .coordinate_single_node_mutation_batch(
            vec![governed_put(
                "objects/source",
                "source",
                b"source",
                governance.clone(),
            )],
            context,
        )
        .await
        .unwrap();
    assert!(matches!(initial.as_slice(), [Ok(_)]));

    let blob = store
        .stage_derived_progress_blob(b"immutable index progress")
        .await
        .unwrap();
    let legacy_mutex = store.commit_lock.lock().await;
    let batch = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        store.coordinate_single_node_derived_progress_publish_batch_with_governance(
            vec![request(
                "_keldra/index-projections/v1/component/current",
                "derived-current",
                blob.clone(),
            )],
            governance.clone(),
            context,
        ),
    )
    .await
    .expect("single-node derived publication must not wait for the process commit mutex")
    .unwrap();
    drop(legacy_mutex);
    assert_eq!(
        batch.source_journal_settlement,
        SourceJournalSettlement::CompletedByCoordinator
    );
    let coordinated = batch.outcomes.into_iter().next().unwrap().unwrap();
    assert!(!coordinated.receipt.replayed);
    assert!(coordinated.mutation.is_some());
    let reference_state = store.blob_reference_state(&blob).unwrap().unwrap();
    assert_eq!((reference_state.ref_count, reference_state.flags), (1, 0));
    let status = store.local_watch_status().unwrap();
    assert_eq!(status.settled_through, status.tail);
    assert_eq!(
        store.reference_delta_cursor(status.source_id).unwrap(),
        status.tail
    );
    assert!(
        store
            .source_journal_runtime_metrics()
            .unwrap()
            .progress_debt_entries()
            > 0
    );

    let bounded = store
        .coordinate_mutation_batch(
            vec![governed_put(
                "objects/bounded",
                "bounded",
                b"bounded",
                governance,
            )],
            context,
            CoordinatorBatchPayloadPreparation::SingleNode {
                source_journal_admission: SourceJournalAdmission::Bounded,
            },
        )
        .await;
    assert!(matches!(bounded, Err(MutationError::SourceJournalCapacity)));
}

async fn conflict_resources_for_put(
    store: &Store,
    governance: &ObjectMutationGovernance,
    path: &str,
    command: &str,
    bytes: &[u8],
) -> BTreeSet<Vec<u8>> {
    let identity = BucketIdentity {
        tenant_id: TenantId(governance.tenant_id),
        bucket_id: BucketId(governance.bucket_id),
    };
    let prepared = store
        .prepare_single_node_coordinated(
            BatchOperation::Put(put_request(path, command, bytes, Durability::Local)),
            identity,
        )
        .await
        .unwrap();
    store.mutation_commit_lanes.conflict_resources_for_test(
        super::super::mutation_commit_lanes::conflict_resources(&prepared, None),
    )
}

#[tokio::test]
async fn independent_lane_evaluation_retries_without_holding_sequence_authority() {
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
    let context = ObjectMutationContext {
        active_placement_log_id: PlacementLogId { term: 7, index: 9 },
        serving_fence_term: 7,
    };
    let first_path = "objects/paused";
    let first_resources =
        conflict_resources_for_put(&store, &governance, first_path, "paused-command", b"paused")
            .await;
    let mut second = None;
    for candidate in 0..256 {
        let path = format!("objects/independent-{candidate}");
        let command = format!("independent-command-{candidate}");
        let bytes = format!("independent-{candidate}").into_bytes();
        let resources =
            conflict_resources_for_put(&store, &governance, &path, &command, &bytes).await;
        if first_resources.is_disjoint(&resources) {
            second = Some((path, command, bytes));
            break;
        }
    }
    let (second_path, second_command, second_bytes) =
        second.expect("the exact conflict resource set has an independent candidate");
    let before = store.local_watch_status().unwrap().tail;

    store.mutation_commit_lanes.pause_next_lane_evaluation();
    let paused = tokio::spawn({
        let store = store.clone();
        let governance = governance.clone();
        async move {
            store
                .coordinate_mutation_batch(
                    vec![governed_put(
                        first_path,
                        "paused-command",
                        b"paused",
                        governance,
                    )],
                    context,
                    CoordinatorBatchPayloadPreparation::SingleNode {
                        source_journal_admission: SourceJournalAdmission::Bounded,
                    },
                )
                .await
        }
    });
    store
        .mutation_commit_lanes
        .wait_for_paused_lane_evaluation()
        .await;
    let sequence = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        store.mutation_commit_lanes.sequence(),
    )
    .await
    .expect("evaluation must not retain the sequence authority");
    drop(sequence);

    let independent = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        store.coordinate_mutation_batch(
            vec![governed_put(
                &second_path,
                &second_command,
                &second_bytes,
                governance.clone(),
            )],
            context,
            CoordinatorBatchPayloadPreparation::SingleNode {
                source_journal_admission: SourceJournalAdmission::Bounded,
            },
        ),
    )
    .await
    .expect("independent lane must commit while the first evaluation is paused")
    .unwrap();
    store.mutation_commit_lanes.resume_paused_lane_evaluation();
    let paused = paused.await.unwrap().unwrap();

    assert_eq!(independent.metrics.lane_authority_revalidation_retries, 0);
    assert_eq!(paused.metrics.lane_authority_revalidation_retries, 1);
    let independent_mutation = independent.outcomes[0]
        .as_ref()
        .unwrap()
        .mutation
        .as_ref()
        .unwrap();
    let paused_mutation = paused.outcomes[0]
        .as_ref()
        .unwrap()
        .mutation
        .as_ref()
        .unwrap();
    assert_eq!(
        independent_mutation.stamp.source_journal_position,
        before + 1
    );
    assert_eq!(paused_mutation.stamp.source_journal_position, before + 2);
    assert_eq!(store.local_watch_status().unwrap().tail, before + 2);

    let replay_tail = store.local_watch_status().unwrap().tail;
    let replays: [(&str, &str, &[u8]); 2] = [
        (first_path, "paused-command", b"paused".as_slice()),
        (&second_path, &second_command, second_bytes.as_slice()),
    ];
    for (path, command, bytes) in replays {
        let replay = store
            .coordinate_mutation_batch(
                vec![governed_put(path, command, bytes, governance.clone())],
                context,
                CoordinatorBatchPayloadPreparation::SingleNode {
                    source_journal_admission: SourceJournalAdmission::Bounded,
                },
            )
            .await
            .unwrap();
        assert!(replay.outcomes[0].as_ref().unwrap().receipt.replayed);
    }
    assert_eq!(store.local_watch_status().unwrap().tail, replay_tail);
}

#[tokio::test]
async fn failed_lane_evaluation_does_not_reserve_sequence_authority() {
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
    let context = ObjectMutationContext {
        active_placement_log_id: PlacementLogId { term: 8, index: 1 },
        serving_fence_term: 8,
    };
    store
        .coordinate_mutation_batch(
            vec![governed_put(
                "objects/existing",
                "create-existing",
                b"existing",
                governance.clone(),
            )],
            context,
            CoordinatorBatchPayloadPreparation::SingleNode {
                source_journal_admission: SourceJournalAdmission::Bounded,
            },
        )
        .await
        .unwrap();
    let before_watch = store.local_watch_status().unwrap();
    let before_ticket = store
        .mutation_commit_lanes
        .sequence()
        .await
        .as_ref()
        .unwrap()
        .next_ticket;

    let failed = store
        .coordinate_mutation_batch(
            vec![governed_put(
                "objects/existing",
                "must-not-reserve",
                b"replacement",
                governance,
            )],
            context,
            CoordinatorBatchPayloadPreparation::SingleNode {
                source_journal_admission: SourceJournalAdmission::Bounded,
            },
        )
        .await
        .unwrap();

    assert!(failed.outcomes[0].is_err());
    assert!(!failed.metrics.physical_commit);
    assert_eq!(store.local_watch_status().unwrap(), before_watch);
    assert_eq!(
        store
            .mutation_commit_lanes
            .sequence()
            .await
            .as_ref()
            .unwrap()
            .next_ticket,
        before_ticket
    );
}

#[tokio::test]
async fn single_node_inline_put_batch_has_one_physical_commit_and_readable_stamped_payloads() {
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
    let context = ObjectMutationContext {
        active_placement_log_id: PlacementLogId { term: 3, index: 7 },
        serving_fence_term: 3,
    };
    let payloads = [
        ("objects/0", b"zero".as_slice()),
        ("objects/1", b"one".as_slice()),
        ("objects/2", b"two".as_slice()),
    ];
    let operations = payloads
        .iter()
        .enumerate()
        .map(|(index, (path, bytes))| {
            (
                BatchOperation::Put(put_request(
                    path,
                    &format!("put-{index}"),
                    bytes,
                    Durability::Local,
                )),
                governance.clone(),
                None,
            )
        })
        .collect();
    let before = store.db.latest_sequence_number();

    let outcomes = store
        .coordinate_single_node_mutation_batch(operations, context)
        .await
        .unwrap();

    assert_eq!(outcomes.len(), payloads.len());
    let mut source = None;
    let mut source_positions = Vec::new();
    for (outcome, (path, bytes)) in outcomes.iter().zip(payloads) {
        let coordinated = outcome.as_ref().unwrap();
        let mutation = coordinated
            .mutation
            .as_ref()
            .expect("new coordinated put must carry its replica mutation");
        source.get_or_insert(mutation.stamp.source_id);
        assert_eq!(source, Some(mutation.stamp.source_id));
        source_positions.push(mutation.stamp.source_journal_position);
        assert!(
            store
                .read_reference_proof(
                    mutation.stamp.source_id,
                    mutation.stamp.source_journal_position,
                )
                .unwrap()
                .is_some()
        );
        assert_eq!(
            mutation.stamp.active_placement_log_id,
            context.active_placement_log_id
        );
        assert_eq!(
            mutation.stamp.serving_fence_term,
            context.serving_fence_term
        );
        let key = ObjectKey::new("tenant", "bucket", path).unwrap();
        let object = store
            .get(&key)
            .await
            .unwrap()
            .expect("committed inline payload must be readable");
        assert_eq!(object.bytes, bytes);
        let reference = mutation.version.blob.as_ref().unwrap();
        let reference_state = store.blob_reference_state(reference).unwrap().unwrap();
        assert_eq!((reference_state.ref_count, reference_state.flags), (1, 0));
        assert_eq!(
            store.head(&key).unwrap().unwrap().mutation_stamp,
            Some(mutation.stamp)
        );
    }
    let journal = store.local_watch_status().unwrap();
    assert_eq!(journal.settled_through, journal.tail);
    assert_eq!(
        store.reference_delta_cursor(journal.source_id).unwrap(),
        journal.tail
    );
    assert_eq!(source_positions.last().copied(), Some(journal.tail));
    assert_eq!(source, Some(journal.source_id));
    assert_eq!(
        store
            .db
            .get_updates_since(before)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
            .len(),
        1
    );

    let sequence_before_replicated = store.db.latest_sequence_number();
    let replicated = store
        .coordinate_single_node_mutation_batch(
            vec![(
                BatchOperation::Put(put_request(
                    "objects/replicated",
                    "put-replicated",
                    b"not locally satisfiable",
                    Durability::Replicated,
                )),
                governance,
                None,
            )],
            context,
        )
        .await
        .unwrap();
    assert!(matches!(
        replicated.as_slice(),
        [Err(MutationError::DurabilityUnavailable)]
    ));
    assert_eq!(
        store.db.latest_sequence_number(),
        sequence_before_replicated
    );
}

#[tokio::test]
async fn single_node_cache_is_loaded_after_legacy_exclusive_writer_finishes() {
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
    let context = ObjectMutationContext {
        active_placement_log_id: PlacementLogId { term: 1, index: 1 },
        serving_fence_term: 1,
    };
    let exclusive = store.mutation_commit_lanes.acquire_exclusive().await;
    let mutation = tokio::spawn({
        let store = store.clone();
        async move {
            store
                .coordinate_single_node_mutation_batch(
                    vec![(
                        BatchOperation::Put(put_request(
                            "objects/raced",
                            "put-raced",
                            b"lane value",
                            Durability::Local,
                        )),
                        governance,
                        None,
                    )],
                    context,
                )
                .await
        }
    });
    while store.mutation_commit_lanes.waiting_fence_readers() == 0 {
        tokio::task::yield_now().await;
    }

    let identity = BucketIdentity {
        tenant_id: TenantId(tenant_id),
        bucket_id: BucketId(bucket_id),
    };
    let head_key = identity.head_key("objects/raced");
    store
        .db
        .put_cf(
            store.cf(CF_HEADS).unwrap(),
            &head_key,
            encode_head(&Head {
                version: VersionId(u64::MAX),
                deleted: false,
                mutation_stamp: None,
            })
            .unwrap(),
        )
        .unwrap();
    drop(exclusive);

    let outcomes = mutation.await.unwrap().unwrap();
    assert!(outcomes[0].is_err());
    assert_eq!(
        store
            .head_by_storage_key(&head_key)
            .unwrap()
            .unwrap()
            .version,
        VersionId(u64::MAX)
    );
}

#[tokio::test]
async fn multiple_distributed_publishes_use_one_physical_metadata_batch() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let first = store.stage_blob(b"first pack").await.unwrap();
    let second = store.stage_blob(b"second pack").await.unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let governance = ObjectMutationGovernance {
        tenant_id,
        bucket_id,
        versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
        policy: store.bucket_policy("tenant", "bucket").unwrap(),
    };
    let before = store.db.latest_sequence_number();
    let outcomes = store
        .coordinate_distributed_publish_batch_with_governance(
            vec![
                request("packs/0", "pack-0", first),
                request("packs/1", "pack-1", second),
            ],
            governance,
            ObjectMutationContext {
                active_placement_log_id: PlacementLogId { term: 1, index: 1 },
                serving_fence_term: 1,
            },
        )
        .await
        .unwrap();

    assert_eq!(outcomes.len(), 2);
    assert!(outcomes.iter().all(std::result::Result::is_ok));
    assert!(
        store
            .head(&ObjectKey::new("tenant", "bucket", "packs/0").unwrap())
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .head(&ObjectKey::new("tenant", "bucket", "packs/1").unwrap())
            .unwrap()
            .is_some()
    );
    assert_eq!(
        store
            .db
            .get_updates_since(before)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn receipt_capacity_commits_only_the_successful_prefix_before_retry() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreOptions::new(temporary.path(), 1).with_mutation_receipt_retention(
            MutationReceiptRetention::new(60, 1, 1024 * 1024).unwrap(),
        ),
    )
    .await
    .unwrap();
    let first = store.stage_blob(b"first pack").await.unwrap();
    let second = store.stage_blob(b"second pack").await.unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let governance = ObjectMutationGovernance {
        tenant_id,
        bucket_id,
        versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
        policy: store.bucket_policy("tenant", "bucket").unwrap(),
    };
    let before = store.db.latest_sequence_number();

    let error = store
        .coordinate_distributed_publish_batch_with_governance(
            vec![
                request("packs/0", "pack-0", first),
                request("packs/1", "pack-1", second),
            ],
            governance,
            ObjectMutationContext {
                active_placement_log_id: PlacementLogId { term: 1, index: 1 },
                serving_fence_term: 1,
            },
        )
        .await
        .unwrap_err();

    assert_eq!(error, MutationError::ReceiptCapacity);
    assert!(
        store
            .head(&ObjectKey::new("tenant", "bucket", "packs/0").unwrap())
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .head(&ObjectKey::new("tenant", "bucket", "packs/1").unwrap())
            .unwrap()
            .is_none()
    );
    assert_eq!(store.mutation_receipt_status().unwrap().entries, 1);
    assert_eq!(
        store
            .db
            .get_updates_since(before)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn derived_origin_and_replica_export_identical_receipts_and_replay_exact_mutations() {
    for single_node in [false, true] {
        let origin_dir = tempfile::tempdir().unwrap();
        let replica_dir = tempfile::tempdir().unwrap();
        let origin = Store::open(StoreOptions::new(origin_dir.path(), 1))
            .await
            .unwrap();
        let replica = Store::open(StoreOptions::new(replica_dir.path(), 2))
            .await
            .unwrap();
        let (tenant_id, bucket_id) = origin.resolve_bucket_ids("tenant", "bucket").unwrap();
        let governance = ObjectMutationGovernance {
            tenant_id,
            bucket_id,
            versioning: origin.bucket_versioning("tenant", "bucket").unwrap(),
            policy: origin.bucket_policy("tenant", "bucket").unwrap(),
        };
        let context = ObjectMutationContext {
            active_placement_log_id: PlacementLogId { term: 1, index: 12 },
            serving_fence_term: 1,
        };
        let blob = origin
            .stage_derived_progress_blob(b"derived immutable")
            .await
            .unwrap();
        let original = request(
            "_keldra/index-projections/v1/test/artifacts/packs/one",
            "derived-original",
            blob,
        );
        let coordinate = |requests, governance| {
            let origin = origin.clone();
            async move {
                if single_node {
                    origin
                        .coordinate_single_node_derived_progress_publish_batch_with_governance(
                            requests, governance, context,
                        )
                        .await
                        .unwrap()
                        .outcomes
                } else {
                    origin
                        .coordinate_derived_progress_publish_batch_with_governance(
                            requests, governance, context,
                        )
                        .await
                        .unwrap()
                }
            }
        };
        let first = coordinate(vec![original.clone()], governance.clone())
            .await
            .remove(0)
            .unwrap();
        assert!(!first.receipt.replayed);
        let mutation = first.mutation.unwrap();
        assert_eq!(
            first.receipt.replay_guarantee_expires_at_unix_millis,
            mutation.receipt_expires_at_unix_millis
        );
        assert!(
            !replica
                .apply_object_mutation_replica(&mutation)
                .await
                .unwrap()
                .replayed
        );
        let exported_receipts = |store: &Store| {
            store
                .export_object_records(None, 100, 1024 * 1024)
                .unwrap()
                .records
                .into_iter()
                .filter_map(|record| match record {
                    crate::ObjectRecordExport::Receipt(value) => Some(value),
                    crate::ObjectRecordExport::ExactPath(_) => None,
                })
                .collect::<Vec<_>>()
        };
        // Exact ObjectMutation equality is the final handoff quorum contract;
        // no origin/replica stamp, expiry or predecessor normalization is used.
        assert_eq!(exported_receipts(&origin), vec![mutation.clone()]);
        assert_eq!(exported_receipts(&replica), vec![mutation.clone()]);
        let origin_status = origin.mutation_receipt_status().unwrap();
        let replica_status = replica.mutation_receipt_status().unwrap();
        let before_command_replay = origin.local_watch_status().unwrap();
        let replay = coordinate(vec![original.clone()], governance.clone())
            .await
            .remove(0)
            .unwrap();
        assert!(replay.receipt.replayed);
        assert_eq!(replay.mutation.as_ref(), Some(&mutation));
        assert_eq!(replay.receipt.version, mutation.version.id);
        assert!(
            replica
                .apply_object_mutation_replica(replay.mutation.as_ref().unwrap())
                .await
                .unwrap()
                .replayed
        );
        assert_eq!(origin.mutation_receipt_status().unwrap(), origin_status);
        assert_eq!(replica.mutation_receipt_status().unwrap(), replica_status);
        assert_eq!(
            origin.local_watch_status().unwrap().tail,
            before_command_replay.tail
        );
        let mut conflicting = original.clone();
        conflicting.blob = origin
            .stage_derived_progress_blob(b"conflicting bytes")
            .await
            .unwrap();
        let rejected = coordinate(vec![conflicting], governance.clone())
            .await
            .remove(0);
        assert_eq!(rejected.unwrap_err(), MutationError::IdempotencyConflict);
        assert_eq!(origin.mutation_receipt_status().unwrap(), origin_status);
        assert_eq!(
            origin.local_watch_status().unwrap().tail,
            before_command_replay.tail
        );
        assert_eq!(exported_receipts(&origin), vec![mutation.clone()]);
        // A new command for already-present identical immutable bytes keeps
        // the existing content-replay contract: no new stamped mutation exists.
        let mut fresh_command = original;
        fresh_command.command_id = Some("derived-fresh-command".into());
        let before_content_replay = origin.local_watch_status().unwrap();
        let content_replay = coordinate(vec![fresh_command], governance)
            .await
            .remove(0)
            .unwrap();
        assert!(content_replay.receipt.replayed);
        assert_eq!(content_replay.receipt.version, mutation.version.id);
        assert!(content_replay.mutation.is_none());
        assert_eq!(
            content_replay
                .receipt
                .replay_guarantee_expires_at_unix_millis,
            0
        );
        assert_eq!(
            origin.local_watch_status().unwrap().tail,
            before_content_replay.tail
        );
        assert_eq!(origin.mutation_receipt_status().unwrap(), origin_status);
        assert_eq!(exported_receipts(&origin), exported_receipts(&replica));
    }
}

#[tokio::test]
async fn derived_publication_retains_receipts_and_enforces_the_shared_capacity() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreOptions::new(temporary.path(), 1).with_mutation_receipt_retention(
            MutationReceiptRetention::new(60, 3, 1024 * 1024).unwrap(),
        ),
    )
    .await
    .unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let governance = ObjectMutationGovernance {
        tenant_id,
        bucket_id,
        versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
        policy: store.bucket_policy("tenant", "bucket").unwrap(),
    };
    let context = ObjectMutationContext {
        active_placement_log_id: PlacementLogId { term: 1, index: 1 },
        serving_fence_term: 1,
    };
    let ordinary = store
        .coordinate_single_node_mutation_batch(
            vec![governed_put(
                "objects/ordinary",
                "ordinary",
                b"ordinary",
                governance.clone(),
            )],
            context,
        )
        .await
        .unwrap();
    assert!(matches!(ordinary.as_slice(), [Ok(_)]));
    let full = store.mutation_receipt_status().unwrap();
    assert_eq!(full.entries, 1);

    let derived_blob = store.stage_derived_progress_blob(b"derived").await.unwrap();
    let derived_request = request("derived/immutable", "derived", derived_blob);
    let derived = store
        .coordinate_single_node_derived_progress_publish_batch_with_governance(
            vec![derived_request.clone()],
            governance.clone(),
            context,
        )
        .await
        .unwrap();
    assert!(matches!(derived.outcomes.as_slice(), [Ok(_)]));
    let derived_version = derived.outcomes[0].as_ref().unwrap().receipt.version;
    let after_derived = store.mutation_receipt_status().unwrap();
    assert_eq!(after_derived.entries, full.entries + 1);
    assert!(after_derived.bytes > full.bytes);

    let replay = store
        .coordinate_single_node_derived_progress_publish_batch_with_governance(
            vec![derived_request],
            governance.clone(),
            context,
        )
        .await
        .unwrap();
    assert!(replay.outcomes[0].as_ref().unwrap().receipt.replayed);
    assert_eq!(store.mutation_receipt_status().unwrap(), after_derived);

    let current_blob = store
        .stage_derived_progress_blob(b"derived current")
        .await
        .unwrap();
    let mut current_request = request("derived/immutable", "derived-current", current_blob);
    current_request.mode = PutMode::PutIfVersion(derived_version);
    let current = store
        .coordinate_single_node_derived_progress_publish_batch_with_governance(
            vec![current_request],
            governance.clone(),
            context,
        )
        .await
        .unwrap();
    assert!(matches!(current.outcomes.as_slice(), [Ok(_)]));
    let at_capacity = store.mutation_receipt_status().unwrap();
    assert_eq!(at_capacity.entries, 3);

    let rejected = store
        .coordinate_single_node_derived_progress_publish_batch_with_governance(
            vec![request(
                "derived/second",
                "derived-second",
                store.stage_derived_progress_blob(b"second").await.unwrap(),
            )],
            governance.clone(),
            context,
        )
        .await
        .unwrap_err();
    assert_eq!(rejected, MutationError::ReceiptCapacity);
    assert_eq!(store.mutation_receipt_status().unwrap(), at_capacity);

    let ordinary_blob = store.stage_blob(b"second ordinary").await.unwrap();
    let error = store
        .coordinate_distributed_publish_batch_with_governance(
            vec![request("objects/second", "second", ordinary_blob)],
            governance,
            context,
        )
        .await
        .unwrap_err();
    assert_eq!(error, MutationError::ReceiptCapacity);
    assert_eq!(store.mutation_receipt_status().unwrap(), at_capacity);
}

#[tokio::test]
async fn distributed_coordinator_does_not_use_the_process_commit_mutex() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let blob = store.stage_blob(b"cluster lane payload").await.unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let governance = ObjectMutationGovernance {
        tenant_id,
        bucket_id,
        versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
        policy: store.bucket_policy("tenant", "bucket").unwrap(),
    };
    let legacy_mutex = store.commit_lock.lock().await;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        store.coordinate_distributed_publish_batch_with_governance(
            vec![request("objects/cluster-lane", "cluster-lane", blob)],
            governance,
            ObjectMutationContext {
                active_placement_log_id: PlacementLogId { term: 4, index: 8 },
                serving_fence_term: 4,
            },
        ),
    )
    .await
    .expect("distributed coordination must not wait for the process commit mutex")
    .unwrap();
    drop(legacy_mutex);
    assert!(matches!(result.as_slice(), [Ok(_)]));
}
