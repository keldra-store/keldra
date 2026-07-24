use super::*;

#[tokio::test]
async fn incremental_materialization_visits_one_event_with_a_large_backlog() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();

    append_authz_record_without_segment(&storage, &record(1, "add"))
        .await
        .unwrap();
    let initial = materialize_authz_derived_state_at_revision(&storage, 42, 1, 0)
        .await
        .unwrap();
    assert_eq!(initial.processed_revision, 1);
    assert_eq!(initial.source_rows_visited, 1);

    for revision in 2..=128 {
        append_authz_record_without_segment(
            &storage,
            &tuple(
                revision,
                "document",
                &format!("unrelated-{revision:04}"),
                "viewer",
                "user",
                "someone-else",
                "add",
            ),
        )
        .await
        .unwrap();
    }

    let next = materialize_authz_derived_state_at_revision(&storage, 42, 128, 0)
        .await
        .unwrap();
    assert_eq!(next.processed_revision, 2);
    assert_eq!(next.source_rows_visited, 1);
    assert!(
        authz_segment::existing_authz_tuple_segment_ref(&storage, 42, 2)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        authz_segment::existing_authz_tuple_segment_ref(&storage, 42, 3)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn incremental_materialization_never_bootstraps_by_scanning_history() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for revision in 1..=4 {
        append_authz_record_without_segment(&storage, &record(revision, "add"))
            .await
            .unwrap();
    }

    let error = materialize_authz_derived_state_at_revision(&storage, 42, 4, 0)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("AuthzMaterializationRepairRequired")
    );
}

#[tokio::test]
async fn direct_materialization_can_advance_through_an_existing_backlog() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();

    append_authz_record_without_segment(&storage, &record(1, "add"))
        .await
        .unwrap();
    materialize_authz_derived_state_at_revision(&storage, 42, 1, 0)
        .await
        .unwrap();
    for revision in 2..=8 {
        append_authz_record_without_segment(
            &storage,
            &tuple(
                revision,
                "document",
                &format!("backlog-{revision}"),
                "viewer",
                "user",
                "alice",
                "add",
            ),
        )
        .await
        .unwrap();
    }

    let outcome = materialize_authz_derived_state_through_revision(&storage, 42, 8, 0)
        .await
        .unwrap();
    assert_eq!(outcome.processed_revision, 8);
    for revision in 1..=8 {
        assert!(
            authz_segment::existing_authz_tuple_segment_ref(&storage, 42, revision)
                .await
                .unwrap()
                .is_some(),
            "direct materialization must publish revision {revision}"
        );
    }
}
