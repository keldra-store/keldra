use super::super::local_root_publication_test_control::pause_publication;
use super::*;
use crate::{
    task_lease::{
        TaskLeaseAcquire, TaskLeaseOwner, acquire_task_lease, renew_task_lease,
        task_lease_fenced_precondition,
    },
    writer_segment_catalog::{
        WriterSegmentCatalogRecord, read_writer_segment_catalog_record,
        test_writer_segment_mutation_batch,
    },
};
use std::{sync::Arc, time::Duration};

const TASK_LEASE_KEY: &[u8] = b"task publication successor signing key";
const TASK_LEASE_TTL_NANOS: i64 = 60_000_000_000;

#[tokio::test]
async fn task_publication_successor_attempt_commits_after_stale_intent_terminalizes() {
    let temp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let store = Arc::new(CoreStore::new(storage.clone()).await.unwrap());
    let acquired_at = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    let stale_lease = acquire_task_lease(
        &storage,
        TaskLeaseAcquire {
            task_id: "writer-successor-attempt".to_string(),
            task_kind: "writer-segment-publication".to_string(),
            partition_family: "writer-segment".to_string(),
            partition_id: hex::encode([41; 32]),
            owner: TaskLeaseOwner::node_instance("node-a", "worker-a"),
            source_cursor: 1,
            now_nanos: acquired_at,
            ttl_nanos: TASK_LEASE_TTL_NANOS,
        },
        TASK_LEASE_KEY,
    )
    .await
    .unwrap();
    let stale_precondition =
        task_lease_fenced_precondition(&storage, &stale_lease, acquired_at, TASK_LEASE_KEY)
            .await
            .unwrap();
    let record = record();
    let stale_batch =
        test_writer_segment_mutation_batch(&record, &[stale_precondition.clone()]).unwrap();
    let identical_retry =
        test_writer_segment_mutation_batch(&record, &[stale_precondition]).unwrap();
    assert_eq!(stale_batch.transaction_id, identical_retry.transaction_id);

    let stale_attempt_id = stale_batch.transaction_id.clone();
    let pause = pause_publication(&stale_attempt_id);
    let commit_store = Arc::clone(&store);
    let mut stale_commit =
        tokio::spawn(async move { commit_store.commit_mutation_batch(stale_batch).await });
    tokio::select! {
        _ = pause.wait_until_reached() => {}
        result = &mut stale_commit => {
            panic!("stale publication finished before final linearization: {result:?}")
        }
        _ = tokio::time::sleep(Duration::from_secs(10)) => {
            panic!("stale publication did not reach final linearization")
        }
    }

    let renewed_at = acquired_at.checked_add(1).unwrap();
    let renewed_lease = renew_task_lease(
        &storage,
        &stale_lease,
        renewed_at,
        TASK_LEASE_TTL_NANOS,
        TASK_LEASE_KEY,
    )
    .await
    .unwrap();
    pause.release();
    let stale_receipt = stale_commit.await.unwrap().unwrap();
    assert_eq!(
        stale_receipt.state,
        CoreTransactionState::FinalisationFailed
    );
    let stale_error = stale_receipt
        .finalisation_error
        .as_deref()
        .expect("stale publication records its terminal failure");
    assert!(
        stale_error.contains("CoreMetaPublicationTerminal"),
        "unexpected stale publication error: {stale_error}"
    );
    let stale_intent = store
        .read_root_publication_intent(&stale_attempt_id)
        .unwrap()
        .expect("stale publication intent remains durable");
    assert!(stale_intent.ensure_pending().is_err());

    let fresh_precondition =
        task_lease_fenced_precondition(&storage, &renewed_lease, renewed_at, TASK_LEASE_KEY)
            .await
            .unwrap();
    let fresh_batch = test_writer_segment_mutation_batch(&record, &[fresh_precondition]).unwrap();
    let fresh_attempt_id = fresh_batch.transaction_id.clone();
    assert_ne!(stale_attempt_id, fresh_attempt_id);
    assert_eq!(
        stale_attempt_id.split_once(":attempt:").unwrap().0,
        fresh_attempt_id.split_once(":attempt:").unwrap().0,
        "renewal must preserve the stable logical publication identity"
    );

    let receipt = store.commit_mutation_batch(fresh_batch).await.unwrap();
    assert_eq!(receipt.state, CoreTransactionState::Committed);
    assert_eq!(receipt.transaction_id, fresh_attempt_id);
    assert_eq!(
        read_writer_segment_catalog_record(
            &storage,
            &record.family,
            &record.scope,
            record.generation,
            &record.segment_ref,
        )
        .await
        .unwrap(),
        Some(record)
    );
    assert!(
        store
            .read_root_publication_intent(&stale_attempt_id)
            .unwrap()
            .expect("stale terminal intent remains after successor commit")
            .ensure_pending()
            .is_err()
    );
}

fn record() -> WriterSegmentCatalogRecord {
    WriterSegmentCatalogRecord {
        family: "task-successor-writer".to_string(),
        scope: "tenant/7/index/task-successor".to_string(),
        segment_ref: "segment:1".to_string(),
        core_object_ref_target: "core-object-ref:task-successor-segment".to_string(),
        segment_hash: hex::encode([71; 32]),
        segment_length: 128,
        generation: 1,
        source_cursor: 1,
        created_at_unix_nanos: 1,
    }
}
