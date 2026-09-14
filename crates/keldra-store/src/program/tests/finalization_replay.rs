use super::*;

#[tokio::test]
async fn unversioned_program_finalization_replays_after_released_predecessor_is_deleted() {
    let (_temporary, store, _program) = configured_store().await;
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let governance = ProgramGovernanceParticipant {
        tenant: "tenant".into(),
        bucket: "bucket".into(),
        tenant_id,
        bucket_id,
        policy: BucketPolicy {
            program_only_prefixes: vec!["managed".into()],
            ..Default::default()
        },
        versioning: ObjectVersioning::Unversioned,
    };
    let first_blob = store.stage_blob(b"first program payload").await.unwrap();
    let second_blob = store.stage_blob(b"second program payload").await.unwrap();
    let first_version = Version {
        id: store.clock.next().unwrap(),
        blob: Some(first_blob.clone()),
        content_type: Some("application/octet-stream".into()),
        deleted: false,
        committed_at_unix_millis: now_unix_millis().unwrap(),
        protected_link_descriptor: false,
    };
    let first_stage = ProgramPathStage {
        format: PROGRAM_PATH_STAGE_FORMAT,
        begin_cursor: 70,
        bundle_hash: PreparedBundleHash([0x51; 32]),
        program_hash: ProgramHash([0x52; 32]),
        authority: ProgramBundleAuthority::StoredProgram {
            program_path_hash: [0x53; 32],
            program_hash: [0x52; 32],
        },
        participant_manifest_hash: [0x54; 32],
        tenant_id,
        bucket_id,
        governance: governance.clone(),
        path: counter_path(),
        expected: ObservedHead::NeverExisted,
        previous_version: None,
        version: first_version.clone(),
    };
    let context = ObjectMutationContext {
        active_placement_log_id: PlacementLogId { term: 3, index: 7 },
        serving_fence_term: 3,
    };
    let first_reservation = commit_stage_reservation(&store, &first_stage, 71, context).await;
    let first = store
        .coordinate_program_path_finalization(first_stage, 71, context)
        .await
        .unwrap();
    store
        .release_program_participant(&first_reservation, Some(71))
        .await
        .unwrap();
    let replica_temporary = tempfile::tempdir().unwrap();
    let replica = Store::open(StoreOptions::new(replica_temporary.path(), 2))
        .await
        .unwrap();
    let first_replica_reservation =
        commit_stage_reservation(&replica, &first.mutation.stage, 71, context).await;
    assert!(
        !replica
            .apply_program_path_finalization_replica(&first.mutation, context)
            .await
            .unwrap()
            .replayed
    );
    replica
        .release_program_participant(&first_replica_reservation, Some(71))
        .await
        .unwrap();

    let first_position = first.mutation.stamp.source_journal_position;
    store
        .advance_source_journal_reference_safe_through(first_position)
        .await
        .unwrap();
    store
        .advance_source_journal_settled_through(first_position)
        .await
        .unwrap();
    while store.local_watch_status().unwrap().retention_floor < first_position {
        assert!(store.prune_source_journal_for_test().await.unwrap());
    }
    assert_eq!(
        store
            .version_metadata(&object_key(&counter_path()).unwrap(), first_version.id)
            .unwrap(),
        Some(first_version.clone()),
        "pruning the current source event releases but does not delete its descriptor"
    );

    let second_version = Version {
        id: store.clock.next().unwrap(),
        blob: Some(second_blob.clone()),
        content_type: Some("application/octet-stream".into()),
        deleted: false,
        committed_at_unix_millis: now_unix_millis().unwrap(),
        protected_link_descriptor: false,
    };
    let second_stage = ProgramPathStage {
        format: PROGRAM_PATH_STAGE_FORMAT,
        begin_cursor: 72,
        bundle_hash: PreparedBundleHash([0x61; 32]),
        program_hash: ProgramHash([0x62; 32]),
        authority: ProgramBundleAuthority::StoredProgram {
            program_path_hash: [0x63; 32],
            program_hash: [0x62; 32],
        },
        participant_manifest_hash: [0x64; 32],
        tenant_id,
        bucket_id,
        governance,
        path: counter_path(),
        expected: ObservedHead::Version {
            version: first_version.id.0.to_string(),
        },
        previous_version: Some(first_version.clone()),
        version: second_version,
    };
    commit_stage_reservation(&store, &second_stage, 73, context).await;
    let second = store
        .coordinate_program_path_finalization(second_stage.clone(), 73, context)
        .await
        .unwrap();
    assert_eq!(
        second.mutation.reference_deltas,
        [
            ReferenceDelta {
                blob: second_blob,
                change: 1,
            },
            ReferenceDelta {
                blob: first_blob,
                change: -1,
            },
        ]
    );
    assert_eq!(
        store
            .version_metadata(&object_key(&counter_path()).unwrap(), first_version.id)
            .unwrap(),
        None,
        "first application deletes the journal-released predecessor"
    );

    let coordinator_replay = store
        .coordinate_program_path_finalization(second_stage, 73, context)
        .await
        .unwrap();
    assert!(coordinator_replay.replayed);
    assert_eq!(coordinator_replay.mutation, second.mutation);

    commit_stage_reservation(&replica, &second.mutation.stage, 73, context).await;
    let replica_first_apply = replica
        .apply_program_path_finalization_replica(&second.mutation, context)
        .await
        .unwrap();
    assert!(!replica_first_apply.replayed);
    assert_eq!(
        replica_first_apply.version,
        second.mutation.stage.version.id
    );
    assert_eq!(
        replica
            .version_metadata_by_identity(
                BucketIdentity {
                    tenant_id: TenantId(tenant_id),
                    bucket_id: BucketId(bucket_id),
                },
                &object_key(&counter_path()).unwrap(),
                first_version.id,
            )
            .unwrap(),
        Some(first_version),
        "the replica keeps its pending descriptor for proof cleanup"
    );
    let replica_replay = replica
        .apply_program_path_finalization_replica(&second.mutation, context)
        .await
        .unwrap();
    assert!(replica_replay.replayed);
    assert_eq!(replica_replay.version, second.mutation.stage.version.id);
}
