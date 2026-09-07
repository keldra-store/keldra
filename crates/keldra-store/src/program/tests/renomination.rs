use super::*;

#[tokio::test]
async fn distributed_path_finalization_replays_after_executor_renomination() {
    let (_temporary, store, program) = configured_store().await;
    let engine = store.program_engine(&program).unwrap();
    let lease = engine
        .prepare(
            &InvocationContext::new("tenant").unwrap(),
            &invocation("distributed-renomination", ExpectedHead::Absent),
        )
        .await
        .unwrap();
    let mut prepared = store
        .prepare_distributed_program_bundle(
            program.hash,
            lease.bundle(),
            &BTreeMap::new(),
            &authoritative_governance(&store, ObjectVersioning::Unversioned),
        )
        .await
        .unwrap();
    prepared.attest_remote_durability("replicated").unwrap();
    let record = store.prepared_program_record(&prepared).await.unwrap();
    let stage =
        path_stage_from_prepared(&prepared, &record, record.writes().first().unwrap(), 40).unwrap();
    let old_context = mutation_context();
    let old_reservation = record
        .reservations(
            40,
            [0x71; 32],
            prepared.bundle.hash,
            1,
            old_context.serving_fence_term,
            old_context.active_placement_log_id,
        )
        .unwrap()
        .into_iter()
        .find(|reservation| matches!(reservation, ProgramReservation::Object(_)))
        .unwrap();
    store
        .reserve_program_participant(&old_reservation)
        .await
        .unwrap();
    store
        .commit_program_participant(&old_reservation, 42)
        .await
        .unwrap();
    let finalized = store
        .coordinate_program_path_finalization(stage.clone(), 42, old_context)
        .await
        .unwrap();

    let replica_temporary = tempfile::tempdir().unwrap();
    let replica = Store::open(StoreOptions::new(replica_temporary.path(), 2))
        .await
        .unwrap();
    replica
        .reserve_program_participant(&old_reservation)
        .await
        .unwrap();
    replica
        .commit_program_participant(&old_reservation, 42)
        .await
        .unwrap();
    assert!(
        !replica
            .apply_program_path_finalization_replica(&finalized.mutation, old_context)
            .await
            .unwrap()
            .replayed
    );

    let new_context = ObjectMutationContext {
        serving_fence_term: old_context.serving_fence_term + 1,
        ..old_context
    };
    let rebound_reservation = record
        .reservations(
            40,
            [0x71; 32],
            prepared.bundle.hash,
            2,
            new_context.serving_fence_term,
            new_context.active_placement_log_id,
        )
        .unwrap()
        .into_iter()
        .find(|reservation| matches!(reservation, ProgramReservation::Object(_)))
        .unwrap();
    for target in [&store, &replica] {
        target
            .reserve_program_participant(&rebound_reservation)
            .await
            .unwrap();
        target
            .commit_program_participant(&rebound_reservation, 42)
            .await
            .unwrap();
    }

    let coordinator_replay = store
        .coordinate_program_path_finalization(stage, 42, new_context)
        .await
        .unwrap();
    assert!(coordinator_replay.replayed);
    assert_eq!(coordinator_replay.mutation, finalized.mutation);
    assert_eq!(
        coordinator_replay.mutation.stamp.serving_fence_term, old_context.serving_fence_term,
        "the sealed mutation retains its original executor provenance"
    );
    let replica_replay = replica
        .apply_program_path_finalization_replica(&coordinator_replay.mutation, new_context)
        .await
        .unwrap();
    assert!(replica_replay.replayed);
    assert_eq!(replica_replay.version, finalized.mutation.stage.version.id);
}
