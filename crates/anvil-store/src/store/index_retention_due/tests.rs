use super::*;
use crate::StoreOptions;

fn generation(
    tenant_id: u64,
    bucket_id: u64,
    index_id: u64,
    definition_version: u64,
    generation: u64,
    due_at: u64,
) -> IndexGenerationRetentionDue {
    IndexGenerationRetentionDue {
        tenant_id,
        bucket_id,
        index_id,
        definition_path: format!("_anvil/indexes/v4/definitions/index-{index_id}"),
        definition_object_version: VersionId(definition_version),
        generation,
        due_at_unix_millis: due_at,
    }
}

fn deleted(index_id: u64, definition_version: u64, due_at: u64) -> DeletedDefinitionCleanup {
    DeletedDefinitionCleanup {
        tenant_id: 1,
        bucket_id: 2,
        index_id,
        definition_path: format!("_anvil/indexes/v4/definitions/index-{index_id}"),
        definition_object_version: VersionId(definition_version),
        due_at_unix_millis: due_at,
    }
}

#[tokio::test]
async fn oldest_generation_is_selected_by_due_time_without_other_kind_interference() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let later = generation(1, 2, 10, 4, 1, 900);
    let earlier = generation(1, 3, 11, 5, 1, 100);
    store.schedule_index_generation_retention(&later).unwrap();
    store.schedule_index_generation_retention(&earlier).unwrap();
    store
        .schedule_deleted_definition_cleanup(&deleted(12, 6, 1))
        .unwrap();

    assert_eq!(
        store.oldest_index_generation_retention_due().unwrap(),
        Some(earlier)
    );
    assert_eq!(
        store.oldest_deleted_definition_cleanup().unwrap(),
        Some(deleted(12, 6, 1))
    );
}

#[tokio::test]
async fn newer_publication_replaces_one_identity_without_a_scan() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let first = generation(1, 2, 10, 4, 7, 500);
    let newer = generation(1, 2, 10, 4, 8, 600);
    let older = generation(1, 2, 10, 4, 6, 100);
    assert!(store.schedule_index_generation_retention(&first).unwrap());
    assert!(store.schedule_index_generation_retention(&newer).unwrap());
    assert!(!store.schedule_index_generation_retention(&older).unwrap());

    assert!(
        !store
            .index_generation_retention_due_matches(&first)
            .unwrap()
    );
    assert!(
        store
            .index_generation_retention_due_matches(&newer)
            .unwrap()
    );
    assert_eq!(
        store.oldest_index_generation_retention_due().unwrap(),
        Some(newer)
    );
}

#[tokio::test]
async fn durable_schedule_has_no_process_local_sixty_four_job_ceiling() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    for index_id in 1..=128 {
        assert!(
            store
                .schedule_index_generation_retention(&generation(1, 2, index_id, 4, 1, index_id,))
                .unwrap()
        );
    }
    assert_eq!(
        store
            .oldest_index_generation_retention_due()
            .unwrap()
            .unwrap()
            .index_id,
        1
    );
}

#[tokio::test]
async fn exact_completion_cannot_remove_a_replacement() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let first = generation(1, 2, 10, 4, 7, 500);
    let newer = generation(1, 2, 10, 4, 8, 600);
    store.schedule_index_generation_retention(&first).unwrap();
    store.schedule_index_generation_retention(&newer).unwrap();

    assert!(
        !store
            .complete_index_generation_retention_due(&first)
            .unwrap()
    );
    assert!(
        store
            .index_generation_retention_due_matches(&newer)
            .unwrap()
    );
    assert!(
        store
            .complete_index_generation_retention_due(&newer)
            .unwrap()
    );
    assert_eq!(store.oldest_index_generation_retention_due().unwrap(), None);
}

#[tokio::test]
async fn assignment_loss_cancels_only_generation_work() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let generation = generation(1, 2, 10, 4, 7, 500);
    store
        .schedule_index_generation_retention(&generation)
        .unwrap();
    assert!(store.cancel_index_generation_retention(1, 2, 10).unwrap());
    assert_eq!(store.oldest_index_generation_retention_due().unwrap(), None);

    let cleanup = deleted(10, 5, 600);
    store.schedule_deleted_definition_cleanup(&cleanup).unwrap();
    assert!(!store.cancel_index_generation_retention(1, 2, 10).unwrap());
    assert_eq!(
        store.oldest_deleted_definition_cleanup().unwrap(),
        Some(cleanup)
    );
}

#[tokio::test]
async fn exact_reschedule_preserves_work_and_moves_its_due_key() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let first = generation(1, 2, 10, 4, 7, 500);
    let delayed = generation(1, 2, 10, 4, 7, 900);
    store.schedule_index_generation_retention(&first).unwrap();

    assert!(
        store
            .replace_index_generation_retention_due(&first, &delayed)
            .unwrap()
    );
    assert!(
        !store
            .index_generation_retention_due_matches(&first)
            .unwrap()
    );
    assert!(
        store
            .index_generation_retention_due_matches(&delayed)
            .unwrap()
    );
}

#[tokio::test]
async fn due_record_survives_store_restart() {
    let temporary = tempfile::tempdir().unwrap();
    let options = StoreOptions::new(temporary.path(), 1);
    let expected = generation(1, 2, 10, 4, 7, 500);
    {
        let store = Store::open(options.clone()).await.unwrap();
        store
            .schedule_index_generation_retention(&expected)
            .unwrap();
    }
    let reopened = Store::open(options).await.unwrap();
    assert_eq!(
        reopened.oldest_index_generation_retention_due().unwrap(),
        Some(expected)
    );
}

#[tokio::test]
async fn deleted_definition_can_be_staged_with_its_callers_batch() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let cleanup = deleted(12, 6, 100);
    let _guard = store.due_lock().unwrap();
    let mut batch = WriteBatch::default();
    store
        .stage_deleted_definition_cleanup(&mut batch, &cleanup)
        .unwrap();
    store.write_due_batch(batch).unwrap();
    drop(_guard);

    assert_eq!(
        store.oldest_deleted_definition_cleanup().unwrap(),
        Some(cleanup)
    );
}
