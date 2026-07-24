use super::*;
use crate::core_store::{
    CF_STREAM_RECORDS, CoreMetaStore, CoreMetaTuplePart, CoreStore, CoreTransactionUpdate,
    TABLE_OBJECT_WATCH_CURSOR_ROW, TABLE_STREAM_RECORD_INDEX_ROW, core_meta_tuple_key,
};
use chrono::Utc;
use std::collections::{BTreeMap, BTreeSet};
use tempfile::tempdir;

fn sample_bucket() -> Bucket {
    Bucket {
        id: 77,
        tenant_id: 12,
        name: "watch-bucket".to_string(),
        region: "test-region".to_string(),
        created_at: Utc::now(),
        is_public_read: false,
    }
}

fn sample_object(id: i64, key: &str) -> Object {
    Object {
        id,
        tenant_id: 12,
        bucket_id: 77,
        key: key.to_string(),
        kind: crate::object_links::ObjectEntryKind::Blob,
        content_hash: format!("hash-{id}"),
        size: 100 + id,
        etag: format!("etag-{id}"),
        content_type: Some("text/plain".to_string()),
        version_id: uuid::Uuid::new_v4(),
        mutation_id: uuid::Uuid::new_v4(),
        index_policy_snapshot: "snapshot".to_string(),
        user_metadata_hash: "metadata-hash".to_string(),
        authz_revision: 3,
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

fn sample_event(id: i64, bucket: &Bucket, object: &Object, event_type: &str) -> ObjectWatchEvent {
    ObjectWatchEvent {
        id,
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
        is_delete_marker: false,
        created_at: object.created_at,
    }
}

async fn append_object_mutation_and_watch(
    storage: &Storage,
    bucket: &Bucket,
    object: &Object,
    event: &ObjectWatchEvent,
) -> anyhow::Result<crate::core_store::StreamAppendReceipt> {
    crate::metadata_journal::append_object_mutation(
        storage,
        bucket,
        object,
        crate::metadata_journal::ObjectJournalMutation::Put,
    )
    .await?;
    committed_object_watch_receipt(storage, bucket, object, event).await
}

#[tokio::test]
async fn append_commits_watch_record_and_exact_cursor_projection_atomically() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let object = sample_object(1, "docs/a.txt");
    let event = sample_event(1, &bucket, &object, "put");
    let receipt = append_object_mutation_and_watch(&storage, &bucket, &object, &event)
        .await
        .unwrap();

    assert_eq!(receipt.sequence, 1);
    assert_eq!(
        exact_object_watch_cursor(
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
    let transaction = CoreStore::new(storage.clone())
        .await
        .unwrap()
        .read_transaction(&format!("object-metadata:{}:put", object.mutation_id))
        .await
        .unwrap()
        .unwrap();
    assert!(transaction.visible_updates.iter().any(|update| matches!(
        update,
        CoreTransactionUpdate::StreamAppend {
            visible_sequence: 1,
            ..
        }
    )));
    assert!(transaction.visible_updates.iter().any(|update| matches!(
        update,
        CoreTransactionUpdate::CoreMetaPut { table_id, .. }
            if *table_id == TABLE_OBJECT_WATCH_CURSOR_ROW
    )));
}

#[tokio::test]
async fn concurrent_versions_resolve_to_their_exact_committed_cursors() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let objects = (1..=24)
        .map(|id| sample_object(id, &format!("docs/{id}.txt")))
        .collect::<Vec<_>>();
    let mut joins = tokio::task::JoinSet::new();
    for object in objects.clone() {
        let storage = storage.clone();
        let bucket = bucket.clone();
        joins.spawn(async move {
            let event = sample_event(object.id, &bucket, &object, "put");
            let receipt =
                append_object_mutation_and_watch(&storage, &bucket, &object, &event).await?;
            Ok::<_, anyhow::Error>((object.version_id, object.mutation_id, receipt.sequence))
        });
    }

    let mut committed = BTreeMap::new();
    while let Some(result) = joins.join_next().await {
        let (version_id, mutation_id, sequence) = result.unwrap().unwrap();
        committed.insert((version_id, mutation_id), sequence);
    }
    assert_eq!(committed.len(), objects.len());
    assert_eq!(
        committed.values().copied().collect::<BTreeSet<_>>(),
        (1..=objects.len() as u64).collect::<BTreeSet<_>>()
    );
    for ((version_id, mutation_id), expected_sequence) in committed {
        assert_eq!(
            exact_object_watch_cursor(
                &storage,
                bucket.tenant_id,
                bucket.id,
                version_id,
                mutation_id,
            )
            .await
            .unwrap(),
            Some(u128::from(expected_sequence))
        );
    }
}

#[tokio::test]
async fn retrying_one_object_watch_publication_is_idempotent() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let object = sample_object(1, "devices/capability.json");
    let event = sample_event(1, &bucket, &object, "put");

    let first = append_object_mutation_and_watch(&storage, &bucket, &object, &event)
        .await
        .unwrap();
    let retried = committed_object_watch_receipt(&storage, &bucket, &object, &event)
        .await
        .unwrap();

    assert_eq!(first.sequence, retried.sequence);
    assert!(retried.idempotent_replay);
    assert_eq!(
        latest_object_watch_stream_cursor(&storage, bucket.tenant_id, bucket.id)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn watch_pages_are_bounded_gap_free_and_prefix_filtered() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    for (id, key) in [
        "docs/a.txt",
        "images/a.png",
        "docs/b.txt",
        "images/b.png",
        "docs/c.txt",
    ]
    .into_iter()
    .enumerate()
    {
        let object = sample_object(id as i64 + 1, key);
        append_object_mutation_and_watch(
            &storage,
            &bucket,
            &object,
            &sample_event(id as i64 + 1, &bucket, &object, "put"),
        )
        .await
        .unwrap();
    }

    let mut cursor = 0;
    let mut event_cursors = Vec::new();
    let mut page_cursors = Vec::new();
    loop {
        let page =
            list_object_watch_event_page(&storage, bucket.tenant_id, bucket.id, "docs/", cursor, 2)
                .await
                .unwrap();
        event_cursors.extend(page.events.iter().map(|event| event.id));
        page_cursors.push(page.next_cursor);
        if !page.has_more {
            break;
        }
        assert!(page.next_cursor > cursor);
        cursor = page.next_cursor;
    }

    assert_eq!(event_cursors, vec![1, 3, 5]);
    assert_eq!(page_cursors, vec![2, 4, 5]);
    assert!(
        list_object_watch_event_page(&storage, bucket.tenant_id, bucket.id, "", 0, 0,)
            .await
            .unwrap_err()
            .to_string()
            .contains("page limit")
    );
    assert!(
        list_object_watch_event_page(
            &storage,
            bucket.tenant_id,
            bucket.id,
            "",
            0,
            OBJECT_WATCH_PAGE_MAX + 1,
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("page limit")
    );
    assert!(
        list_object_watch_event_page(&storage, bucket.tenant_id, bucket.id, "", -1, 1,)
            .await
            .unwrap_err()
            .to_string()
            .contains("non-negative")
    );
}

#[tokio::test]
async fn exact_cursor_projection_does_not_depend_on_retained_watch_history() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let object = sample_object(1, "docs/a.txt");
    let event = sample_event(1, &bucket, &object, "put");
    let receipt = append_object_mutation_and_watch(&storage, &bucket, &object, &event)
        .await
        .unwrap();

    CoreMetaStore::open(storage.core_store_meta_path())
        .unwrap()
        .delete(
            CF_STREAM_RECORDS,
            TABLE_STREAM_RECORD_INDEX_ROW,
            &stream_record_tuple_key(&receipt.stream_id, receipt.sequence),
        )
        .unwrap();

    assert_eq!(
        exact_object_watch_cursor(
            &storage,
            bucket.tenant_id,
            bucket.id,
            object.version_id,
            object.mutation_id,
        )
        .await
        .unwrap(),
        Some(u128::from(receipt.sequence))
    );
}

fn stream_record_tuple_key(stream_id: &str, sequence: u64) -> Vec<u8> {
    let sequence = sequence.to_be_bytes();
    let mut parts = Vec::with_capacity(stream_id.len() + 2);
    parts.push(CoreMetaTuplePart::Raw(b"stream-record"));
    parts.extend(
        stream_id
            .as_bytes()
            .iter()
            .map(|byte| CoreMetaTuplePart::Raw(std::slice::from_ref(byte))),
    );
    parts.push(CoreMetaTuplePart::Raw(&sequence));
    core_meta_tuple_key(&parts).unwrap()
}
