use super::*;
use crate::writer_segment_catalog::{
    read_writer_segment_catalog_record, write_writer_segment_catalog_record,
};

#[tokio::test]
async fn historical_point_lookup_ignores_retained_segments_before_its_checkpoint() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let target = tuple_record(1, "target", "alice");

    let first_ref =
        write_authz_tuple_checkpoint_segment(&storage, 7, &[target.clone()], None, 1, 1, 0)
            .await
            .unwrap();
    let first = read_writer_segment_catalog_record(
        &storage,
        AUTHZ_TUPLE_SEGMENT_CATALOG_FAMILY,
        &authz_tuple_segment_scope(7).unwrap(),
        1,
        &first_ref,
    )
    .await
    .unwrap()
    .unwrap();
    for generation in 2..AUTHZ_DELTA_CHECKPOINT_INTERVAL {
        let mut retained = first.clone();
        retained.generation = generation;
        retained.segment_ref = format!("retained-authz-segment-{generation:020}");
        write_writer_segment_catalog_record(&storage, &retained, &[])
            .await
            .unwrap();
    }

    write_authz_tuple_checkpoint_segment(
        &storage,
        7,
        &[target],
        None,
        AUTHZ_DELTA_CHECKPOINT_INTERVAL,
        1,
        0,
    )
    .await
    .unwrap();
    let mut previous =
        read_authz_tuple_segment_at_revision(&storage, 7, AUTHZ_DELTA_CHECKPOINT_INTERVAL)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(previous.header.generation, AUTHZ_DELTA_CHECKPOINT_INTERVAL);
    assert_eq!(previous.header.source_stream_cursor, 1);
    for revision in (AUTHZ_DELTA_CHECKPOINT_INTERVAL + 1)..=260 {
        write_authz_tuple_delta_segment(&storage, 7, &previous, &[], revision, 1, 0)
            .await
            .unwrap();
        previous = read_authz_tuple_segment_at_revision(&storage, 7, revision)
            .await
            .unwrap()
            .unwrap();
    }

    let outcome = lookup_materialized_tuple_at_revision(
        &storage, 7, "document", "target", "viewer", "user", "alice", "", 260,
    )
    .await
    .unwrap();
    assert_eq!(outcome.record.unwrap().object_id, "target");
    assert_eq!(outcome.stats.segments_opened, 5);
    assert!(outcome.stats.segments_opened <= AUTHZ_DELTA_CHECKPOINT_INTERVAL as usize);
    assert!(outcome.stats.table_rows_visited <= 5);

    let permission = resolve_materialized_permission_at_revision(
        &storage, 7, "document", "target", "viewer", "user", "alice", "", 260,
    )
    .await
    .unwrap();
    assert!(permission.allowed);
    assert_eq!(permission.stats.segments_opened, 5);
    assert!(permission.stats.segments_opened <= AUTHZ_DELTA_CHECKPOINT_INTERVAL as usize);
    assert!(permission.stats.table_rows_visited <= 5);
}

#[tokio::test]
async fn later_checkpoint_supersedes_an_earlier_chain_in_the_same_window() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let old = tuple_record(1, "old", "alice");
    write_authz_tuple_checkpoint_segment(&storage, 7, &[old.clone()], None, 1, 1, 0)
        .await
        .unwrap();
    let previous = read_authz_tuple_segment_at_revision(&storage, 7, 1)
        .await
        .unwrap()
        .unwrap();
    let removed = AuthzTupleRecord {
        object_id: "old".to_string(),
        ..record(2, "remove")
    };
    write_authz_tuple_delta_segment(&storage, 7, &previous, &[removed], 2, 2, 0)
        .await
        .unwrap();

    let current = tuple_record(3, "current", "alice");
    write_authz_tuple_checkpoint_segment(&storage, 7, &[current], None, 3, 3, 0)
        .await
        .unwrap();
    let merged = read_authz_tuple_segment_at_revision(&storage, 7, 3)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(merged.records.len(), 1);
    assert_eq!(merged.records[0].object_id, "current");

    let old = lookup_materialized_tuple_at_revision(
        &storage, 7, "document", "old", "viewer", "user", "alice", "", 3,
    )
    .await
    .unwrap();
    assert!(old.record.is_none());
    assert_eq!(old.stats.segments_opened, 3);
    let current = lookup_materialized_tuple_at_revision(
        &storage, 7, "document", "current", "viewer", "user", "alice", "", 3,
    )
    .await
    .unwrap();
    assert_eq!(current.record.unwrap().object_id, "current");
    assert_eq!(current.stats.segments_opened, 3);
}

#[tokio::test]
async fn checkpoint_rejects_a_regressed_source_cursor() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let current = tuple_record(1, "current", "alice");
    write_authz_tuple_checkpoint_segment(&storage, 7, &[current.clone()], None, 1, 2, 0)
        .await
        .unwrap();
    let previous = read_authz_tuple_segment_at_revision(&storage, 7, 1)
        .await
        .unwrap()
        .unwrap();

    let error =
        write_authz_tuple_checkpoint_segment(&storage, 7, &[current], Some(&previous), 2, 1, 0)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("does not advance"));
}
