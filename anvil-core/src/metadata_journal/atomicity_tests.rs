use super::*;
use crate::core_store::{
    CoreBeginTransaction, CoreMutationPrecondition, CoreStore, CoreTransaction,
    CoreTransactionState, CoreTransactionUpdate,
};
use crate::persistence::ObjectWatchEvent;
use anyhow::anyhow;
use chrono::Utc;
use std::collections::{BTreeMap, BTreeSet};
use tempfile::tempdir;

const ZERO_HASH: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";

fn sample_bucket() -> Bucket {
    Bucket {
        id: 91,
        tenant_id: 37,
        name: "atomic-object-mutations".to_string(),
        region: "test-region".to_string(),
        created_at: Utc::now(),
        is_public_read: false,
    }
}

fn sample_object(id: i64, key: impl Into<String>) -> Object {
    Object {
        id,
        tenant_id: 37,
        bucket_id: 91,
        key: key.into(),
        kind: object_links::ObjectEntryKind::Blob,
        content_hash: format!("hash-{id}"),
        size: id.saturating_mul(10),
        etag: format!("etag-{id}"),
        content_type: Some("application/octet-stream".to_string()),
        version_id: uuid::Uuid::new_v4(),
        mutation_id: uuid::Uuid::new_v4(),
        index_policy_snapshot: "snapshot".to_string(),
        user_metadata_hash: "metadata-hash".to_string(),
        authz_revision: 1,
        record_hash: format!("record-{id}"),
        created_at: Utc::now(),
        deleted_at: None,
        storage_class: None,
        user_meta: None,
        shard_map: None,
        checksum: None,
        link: None,
    }
}

fn watch_event(bucket: &Bucket, object: &Object, event_type: &str) -> ObjectWatchEvent {
    ObjectWatchEvent {
        id: 0,
        tenant_id: bucket.tenant_id,
        bucket_id: bucket.id,
        bucket_name: bucket.name.clone(),
        key: object.key.clone(),
        event_type: event_type.to_string(),
        version_id: Some(object.version_id),
        mutation_id: object.mutation_id,
        payload_hash: object.content_hash.clone(),
        etag: Some(object.etag.clone()),
        size: object.size,
        is_delete_marker: object.deleted_at.is_some(),
        created_at: object.created_at,
    }
}

fn transaction_scope(bucket: &Bucket) -> String {
    hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id))
}

async fn begin_object_transaction(
    store: &CoreStore,
    bucket: &Bucket,
    transaction_id: &str,
    principal: &str,
) -> CoreTransaction {
    let scope = transaction_scope(bucket);
    store
        .begin_explicit_transaction(CoreBeginTransaction {
            idempotency_key: transaction_id.to_string(),
            root_anchor_key: scope.clone(),
            root_key_hash: CoreStore::root_key_hash_for_anchor(&scope),
            scope_partition: scope,
            ttl_ms: 60_000,
            purpose: "object mutation atomicity test".to_string(),
            principal: principal.to_string(),
            preconditions_hash: ZERO_HASH.to_string(),
        })
        .await
        .unwrap()
}

async fn assert_mutation_invisible(storage: &Storage, bucket: &Bucket, object: &Object) {
    assert!(
        read_current_object(storage, bucket, b"unused", &object.key)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        crate::watch_log::exact_object_watch_cursor(
            storage,
            bucket.tenant_id,
            bucket.id,
            object.version_id,
            object.mutation_id,
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(
        read_all_metadata_journal_records(storage, bucket)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        crate::watch_log::list_object_watch_event_page(
            storage,
            bucket.tenant_id,
            bucket.id,
            "",
            0,
            16,
        )
        .await
        .unwrap()
        .events
        .is_empty()
    );
}

#[tokio::test]
async fn implicit_object_mutation_commits_metadata_watch_cursor_and_current_row_together() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let object = sample_object(1, "objects/one.bin");

    append_object_mutation(&storage, &bucket, &object, ObjectJournalMutation::Put)
        .await
        .unwrap();

    let metadata = read_all_metadata_journal_records(&storage, &bucket)
        .await
        .unwrap();
    assert_eq!(metadata.len(), 1);
    let cursor = crate::watch_log::exact_object_watch_cursor(
        &storage,
        bucket.tenant_id,
        bucket.id,
        object.version_id,
        object.mutation_id,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(cursor, 1);
    assert_eq!(
        read_current_object(&storage, &bucket, b"unused", &object.key)
            .await
            .unwrap()
            .unwrap()
            .version_id,
        object.version_id
    );

    let transaction_id = format!("object-metadata:{}:put", object.mutation_id);
    let transaction = CoreStore::new(storage.clone())
        .await
        .unwrap()
        .read_transaction(&transaction_id)
        .await
        .unwrap()
        .unwrap();
    let stream_ids = transaction
        .visible_updates
        .iter()
        .filter_map(|update| match update {
            CoreTransactionUpdate::StreamAppend { stream_id, .. } => Some(stream_id.clone()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        stream_ids,
        BTreeSet::from([
            object_metadata_stream_id(bucket.tenant_id, bucket.id),
            crate::watch_log::object_watch_stream_id(bucket.tenant_id, bucket.id),
        ])
    );
    assert!(
        transaction
            .visible_updates
            .iter()
            .any(|update| matches!(update, CoreTransactionUpdate::CoreMetaPut { .. }))
    );
}

#[tokio::test]
async fn failed_mutation_precondition_exposes_none_of_the_object_mutation() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let object = sample_object(1, "objects/rejected.bin");
    let rejected = append_object_mutation_inner(
        &storage,
        &bucket,
        &object,
        ObjectJournalMutation::Put,
        0,
        Some(CoreMutationPrecondition::StreamHead {
            stream_id: object_metadata_stream_id(bucket.tenant_id, bucket.id),
            expected_last_sequence: 99,
            expected_last_event_hash: ZERO_HASH.to_string(),
        }),
        None,
        None,
    )
    .await
    .unwrap_err();
    assert!(rejected.to_string().contains("head mismatch"));
    assert_mutation_invisible(&storage, &bucket, &object).await;
}

#[tokio::test]
async fn cursor_conflict_cannot_commit_metadata_or_replace_the_current_object() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let object = sample_object(1, "objects/conflict.bin");
    append_object_mutation(&storage, &bucket, &object, ObjectJournalMutation::Put)
        .await
        .unwrap();

    let mut conflicting = object.clone();
    conflicting.content_hash = "conflicting-hash".to_string();
    conflicting.deleted_at = Some(Utc::now());
    let rejected = append_object_mutation(
        &storage,
        &bucket,
        &conflicting,
        ObjectJournalMutation::DeleteMarker,
    )
    .await
    .unwrap_err();
    assert!(rejected.to_string().contains("precondition failed"));

    assert_eq!(
        read_all_metadata_journal_records(&storage, &bucket)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        crate::watch_log::list_object_watch_event_page(
            &storage,
            bucket.tenant_id,
            bucket.id,
            "",
            0,
            16,
        )
        .await
        .unwrap()
        .events
        .len(),
        1
    );
    assert_eq!(
        read_current_object(&storage, &bucket, b"unused", &object.key)
            .await
            .unwrap()
            .unwrap()
            .content_hash,
        object.content_hash
    );
}

#[tokio::test]
async fn reused_mutation_identity_cannot_hide_a_different_object_payload() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let object = sample_object(1, "objects/idempotency-conflict.bin");
    append_object_mutation(&storage, &bucket, &object, ObjectJournalMutation::Put)
        .await
        .unwrap();

    let mut conflicting = object.clone();
    conflicting.content_hash = "different-content".to_string();
    conflicting.record_hash = "different-record".to_string();
    let rejected =
        append_object_mutation(&storage, &bucket, &conflicting, ObjectJournalMutation::Put)
            .await
            .unwrap_err();
    assert!(rejected.to_string().contains("idempotency identity"));
    assert_eq!(
        read_current_object(&storage, &bucket, b"unused", &object.key)
            .await
            .unwrap()
            .unwrap()
            .content_hash,
        object.content_hash
    );
    assert_eq!(
        crate::watch_log::list_object_watch_event_page(
            &storage,
            bucket.tenant_id,
            bucket.id,
            "",
            0,
            16,
        )
        .await
        .unwrap()
        .events
        .len(),
        1
    );
}

#[tokio::test]
async fn later_version_deletion_cannot_retarget_the_original_mutation_cursor() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let object = sample_object(1, "objects/deleted-version.bin");
    append_object_mutation(&storage, &bucket, &object, ObjectJournalMutation::Put)
        .await
        .unwrap();

    let mut deletion = object.clone();
    deletion.id = 2;
    deletion.mutation_id = uuid::Uuid::new_v4();
    deletion.deleted_at = Some(Utc::now());
    append_object_mutation(
        &storage,
        &bucket,
        &deletion,
        ObjectJournalMutation::DeleteVersion,
    )
    .await
    .unwrap();

    assert_eq!(
        crate::watch_log::exact_object_watch_cursor(
            &storage,
            bucket.tenant_id,
            bucket.id,
            object.version_id,
            object.mutation_id,
        )
        .await
        .unwrap(),
        Some(1)
    );
    assert_eq!(
        crate::watch_log::exact_object_watch_cursor(
            &storage,
            bucket.tenant_id,
            bucket.id,
            deletion.version_id,
            deletion.mutation_id,
        )
        .await
        .unwrap(),
        Some(2)
    );
    assert_eq!(
        crate::watch_log::latest_object_watch_cursor(
            &storage,
            bucket.tenant_id,
            bucket.id,
            object.version_id,
        )
        .await
        .unwrap(),
        Some(2)
    );
}

#[tokio::test]
async fn staged_object_mutation_stays_invisible_across_restart_and_rollback() {
    let temp = tempdir().unwrap();
    let bucket = sample_bucket();
    let object = sample_object(1, "objects/rolled-back.bin");
    let principal = "principal:object-rollback";
    let transaction_id = "object-rollback";
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let transaction = begin_object_transaction(&store, &bucket, transaction_id, principal).await;
    append_object_mutation_inner(
        &storage,
        &bucket,
        &object,
        ObjectJournalMutation::Put,
        0,
        None,
        Some(&transaction.transaction_id),
        Some(principal),
    )
    .await
    .unwrap();
    assert_mutation_invisible(&storage, &bucket, &object).await;
    drop(store);
    drop(storage);

    let reopened = Storage::new_at(temp.path()).await.unwrap();
    assert_mutation_invisible(&reopened, &bucket, &object).await;
    CoreStore::new(reopened.clone())
        .await
        .unwrap()
        .rollback_explicit_transaction(&transaction.transaction_id, principal, "test rollback")
        .await
        .unwrap();
    assert_mutation_invisible(&reopened, &bucket, &object).await;
}

#[tokio::test]
async fn explicit_commit_after_restart_makes_every_object_projection_visible_together() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let object = sample_object(1, "objects/committed.bin");
    let principal = "principal:object-commit";
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let transaction = begin_object_transaction(&store, &bucket, "object-commit", principal).await;
    append_object_mutation_inner(
        &storage,
        &bucket,
        &object,
        ObjectJournalMutation::Put,
        0,
        None,
        Some(&transaction.transaction_id),
        Some(principal),
    )
    .await
    .unwrap();
    assert_mutation_invisible(&storage, &bucket, &object).await;
    drop(store);
    drop(storage);

    let storage = Storage::new_at(temp.path()).await.unwrap();
    let committed = CoreStore::new(storage.clone())
        .await
        .unwrap()
        .commit_explicit_transaction(&transaction.transaction_id, principal)
        .await
        .unwrap();
    assert_eq!(committed.state, CoreTransactionState::Committed);
    assert_eq!(
        read_all_metadata_journal_records(&storage, &bucket)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        crate::watch_log::exact_object_watch_cursor(
            &storage,
            bucket.tenant_id,
            bucket.id,
            object.version_id,
            object.mutation_id,
        )
        .await
        .unwrap(),
        Some(1)
    );
    assert_eq!(
        read_current_object(&storage, &bucket, b"unused", &object.key)
            .await
            .unwrap()
            .unwrap()
            .version_id,
        object.version_id
    );
    crate::watch_log::committed_object_watch_receipt(
        &storage,
        &bucket,
        &object,
        &watch_event(&bucket, &object, "put"),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn explicit_transaction_commits_multiple_object_mutations_without_projection_collisions() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let first = sample_object(1, "objects/transaction-one.bin");
    let second = sample_object(2, "objects/transaction-two.bin");
    let principal = "principal:multi-object-commit";
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let transaction =
        begin_object_transaction(&store, &bucket, "multi-object-commit", principal).await;
    for object in [&first, &second] {
        append_object_mutation_inner(
            &storage,
            &bucket,
            object,
            ObjectJournalMutation::Put,
            0,
            None,
            Some(&transaction.transaction_id),
            Some(principal),
        )
        .await
        .unwrap();
    }

    store
        .commit_explicit_transaction(&transaction.transaction_id, principal)
        .await
        .unwrap();
    assert_eq!(
        read_all_metadata_journal_records(&storage, &bucket)
            .await
            .unwrap()
            .len(),
        2
    );
    for (object, expected_cursor) in [(&first, 1), (&second, 2)] {
        assert_eq!(
            crate::watch_log::exact_object_watch_cursor(
                &storage,
                bucket.tenant_id,
                bucket.id,
                object.version_id,
                object.mutation_id,
            )
            .await
            .unwrap(),
            Some(expected_cursor)
        );
        assert_eq!(
            read_current_object(&storage, &bucket, b"unused", &object.key)
                .await
                .unwrap()
                .unwrap()
                .version_id,
            object.version_id
        );
    }
}

#[tokio::test]
async fn concurrent_versions_keep_gap_free_watch_order_and_exact_cursor_identity() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let objects = (1..=24)
        .map(|id| sample_object(id, format!("objects/{id}.bin")))
        .collect::<Vec<_>>();
    let mut joins = tokio::task::JoinSet::new();
    for object in objects.clone() {
        let storage = storage.clone();
        let bucket = bucket.clone();
        joins.spawn(async move {
            append_object_mutation(&storage, &bucket, &object, ObjectJournalMutation::Put).await?;
            let cursor = crate::watch_log::exact_object_watch_cursor(
                &storage,
                bucket.tenant_id,
                bucket.id,
                object.version_id,
                object.mutation_id,
            )
            .await?
            .ok_or_else(|| anyhow!("committed object has no exact watch cursor"))?;
            Ok::<_, anyhow::Error>((object.mutation_id, cursor))
        });
    }

    let mut cursors = BTreeMap::new();
    while let Some(result) = joins.join_next().await {
        let (mutation_id, cursor) = result.unwrap().unwrap();
        cursors.insert(mutation_id, cursor);
    }
    assert_eq!(cursors.len(), objects.len());
    assert_eq!(
        cursors.values().copied().collect::<BTreeSet<_>>(),
        (1..=objects.len() as u128).collect::<BTreeSet<_>>()
    );

    let page = crate::watch_log::list_object_watch_event_page(
        &storage,
        bucket.tenant_id,
        bucket.id,
        "objects/",
        0,
        64,
    )
    .await
    .unwrap();
    assert!(!page.has_more);
    assert_eq!(page.events.len(), objects.len());
    let metadata_mutations = read_all_metadata_journal_records(&storage, &bucket)
        .await
        .unwrap()
        .into_iter()
        .map(|record| record.body.mutation_id)
        .collect::<BTreeSet<_>>();
    assert_eq!(metadata_mutations.len(), objects.len());
    let event_mutations = page
        .events
        .iter()
        .map(|event| event.mutation_id.to_string())
        .collect::<BTreeSet<_>>();
    assert_eq!(event_mutations, metadata_mutations);
    for event in page.events {
        assert!(metadata_mutations.contains(&event.mutation_id.to_string()));
        assert_eq!(
            cursors.get(&event.mutation_id),
            Some(&(u128::try_from(event.id).unwrap()))
        );
    }
}
