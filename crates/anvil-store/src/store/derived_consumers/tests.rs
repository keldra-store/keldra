use super::*;
use crate::{
    BatchOperation, Durability, ObjectKey, PutMode, PutRequest, StoreOptions, WatchRetention,
};

fn fence(index: u64) -> PlacementLogId {
    PlacementLogId { term: 1, index }
}

fn checkpoint(
    source: SourceId,
    kind: DerivedConsumerKind,
    node: u16,
    next_offset: u64,
    observed_fence: PlacementLogId,
) -> DerivedConsumerCheckpoint {
    DerivedConsumerCheckpoint {
        consumer_kind: kind,
        source_id: source,
        consumer_node_id: node,
        next_offset,
        observed_fence,
    }
}

async fn put(store: &Store, command: &str) {
    let outcomes = store
        .bulk_write(vec![BatchOperation::Put(PutRequest {
            key: ObjectKey::new("tenant", "bucket", command).unwrap(),
            bytes: vec![1],
            content_type: None,
            mode: PutMode::PutIfAbsent,
            command_id: Some(command.into()),
            durability: Durability::Local,
        })])
        .await;
    outcomes[0].result.as_ref().unwrap();
}

#[tokio::test]
async fn both_consumer_kinds_and_every_active_node_constrain_the_safe_cut() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    for command in ["one", "two", "three"] {
        put(&store, command).await;
    }
    let source = store.local_watch_status().unwrap().source_id;
    let nodes = [1, 2, 3];
    store
        .ensure_derived_consumer_membership(fence(1), &nodes)
        .await
        .unwrap();

    for node in nodes {
        store
            .apply_derived_consumer_checkpoint(
                checkpoint(source, DerivedConsumerKind::Index, node, 4, fence(1)),
                &nodes,
            )
            .await
            .unwrap();
    }
    for (node, next) in [(1, 4), (2, 3), (3, 4)] {
        store
            .apply_derived_consumer_checkpoint(
                checkpoint(
                    source,
                    DerivedConsumerKind::Accounting,
                    node,
                    next,
                    fence(1),
                ),
                &nodes,
            )
            .await
            .unwrap();
    }
    let status = store.derived_consumer_status().unwrap().unwrap();
    assert_eq!(status.index_safe_through, 3);
    assert_eq!(status.accounting_safe_through, 2);
    assert_eq!(status.safe_through(), 2);
}

#[tokio::test]
async fn checkpoint_apply_is_idempotent_and_rejects_regression_or_future_progress() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    put(&store, "one").await;
    let source = store.local_watch_status().unwrap().source_id;
    let nodes = [1, 2];
    let value = checkpoint(source, DerivedConsumerKind::Index, 1, 2, fence(1));
    let first = store
        .apply_derived_consumer_checkpoint(value, &nodes)
        .await
        .unwrap();
    let replay = store
        .apply_derived_consumer_checkpoint(value, &nodes)
        .await
        .unwrap();
    assert_eq!(first, replay);
    assert_eq!(
        store
            .apply_derived_consumer_checkpoint(
                checkpoint(source, DerivedConsumerKind::Index, 1, 1, fence(1)),
                &nodes,
            )
            .await,
        Err(DerivedConsumerError::CheckpointRegression)
    );
    assert_eq!(
        store
            .apply_derived_consumer_checkpoint(
                checkpoint(source, DerivedConsumerKind::Index, 1, 3, fence(1)),
                &nodes,
            )
            .await,
        Err(DerivedConsumerError::CheckpointFuture)
    );
}

#[tokio::test]
async fn membership_and_both_checkpoint_kinds_survive_reopen() {
    let temporary = tempfile::tempdir().unwrap();
    let options = StoreOptions::new(temporary.path(), 1);
    let store = Store::open(options.clone()).await.unwrap();
    put(&store, "one").await;
    let source = store.local_watch_status().unwrap().source_id;
    let nodes = [1, 2];
    for kind in DerivedConsumerKind::ALL {
        store
            .apply_derived_consumer_checkpoint(checkpoint(source, kind, 1, 2, fence(1)), &nodes)
            .await
            .unwrap();
    }
    drop(store);

    let reopened = Store::open(options).await.unwrap();
    let status = reopened.derived_consumer_status().unwrap().unwrap();
    assert_eq!(status.source_id, source);
    assert_eq!(status.observed_fence, fence(1));
    assert_eq!(status.active_consumer_nodes, vec![1, 2]);
    for kind in DerivedConsumerKind::ALL {
        assert_eq!(
            reopened
                .derived_consumer_checkpoint(kind, 1)
                .unwrap()
                .unwrap()
                .next_offset,
            2
        );
        assert_eq!(
            reopened
                .derived_consumer_checkpoint(kind, 2)
                .unwrap()
                .unwrap()
                .next_offset,
            1
        );
    }
}

#[tokio::test]
async fn membership_cutover_fences_the_old_set_at_the_retained_floor() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    for command in ["one", "two"] {
        put(&store, command).await;
    }
    let source = store.local_watch_status().unwrap().source_id;
    for kind in DerivedConsumerKind::ALL {
        store
            .apply_derived_consumer_checkpoint(checkpoint(source, kind, 1, 3, fence(1)), &[1])
            .await
            .unwrap();
    }
    assert_eq!(
        store
            .derived_consumer_status()
            .unwrap()
            .unwrap()
            .safe_through(),
        2
    );

    store
        .ensure_derived_consumer_membership(fence(2), &[1, 2])
        .await
        .unwrap();
    let fenced = store.derived_consumer_status().unwrap().unwrap();
    assert_eq!(fenced.observed_fence, fence(2));
    assert_eq!(fenced.active_consumer_nodes, vec![1, 2]);
    assert_eq!(fenced.safe_through(), 0);
    assert_eq!(
        store
            .apply_derived_consumer_checkpoint(
                checkpoint(source, DerivedConsumerKind::Index, 1, 3, fence(1)),
                &[1],
            )
            .await,
        Err(DerivedConsumerError::FenceRegression)
    );
}

#[tokio::test]
async fn pruning_waits_for_settlement_references_indexes_and_accounting() {
    let temporary = tempfile::tempdir().unwrap();
    let options = StoreOptions::new(temporary.path(), 1)
        .with_watch_retention(WatchRetention::new(1, 1024 * 1024).unwrap());
    let store = Store::open(options).await.unwrap();
    let source = store.local_watch_status().unwrap().source_id;
    store
        .ensure_derived_consumer_membership(fence(1), &[1])
        .await
        .unwrap();
    put(&store, "one").await;
    assert_eq!(
        store
            .bulk_write(vec![BatchOperation::Put(PutRequest {
                key: ObjectKey::new("tenant", "bucket", "two").unwrap(),
                bytes: vec![2],
                content_type: None,
                mode: PutMode::PutIfAbsent,
                command_id: Some("two".into()),
                durability: Durability::Local,
            })])
            .await[0]
            .result,
        Err(crate::MutationError::SourceJournalCapacity)
    );
    store
        .advance_source_journal_reference_safe_through(1)
        .await
        .unwrap();
    store
        .apply_derived_consumer_checkpoint(
            checkpoint(source, DerivedConsumerKind::Index, 1, 2, fence(1)),
            &[1],
        )
        .await
        .unwrap();
    let metrics = store.source_journal_runtime_metrics().unwrap();
    assert_eq!(metrics.tail, 1);
    assert_eq!(metrics.settled_through, 1);
    assert_eq!(metrics.reference_safe_through, 1);
    assert_eq!(metrics.index_safe_through, 1);
    assert_eq!(metrics.accounting_safe_through, 0);
    assert_eq!(metrics.prune_safe_through(), 0);
    assert_eq!(metrics.retained_entries, 1);
    assert_eq!(metrics.max_entries, 1);
    assert_eq!(store.local_watch_status().unwrap().retention_floor, 0);
    store
        .apply_derived_consumer_checkpoint(
            checkpoint(source, DerivedConsumerKind::Accounting, 1, 2, fence(1)),
            &[1],
        )
        .await
        .unwrap();
    assert_eq!(store.local_watch_status().unwrap().retention_floor, 0);
    put(&store, "two-after-progress").await;
    assert_eq!(store.local_watch_status().unwrap().retention_floor, 1);
}
