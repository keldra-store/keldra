use keldra_store::{
    BlobRef, Head, MUTATION_STAMP_FORMAT, MutationStamp, SourceId, Store, StoreOptions, Version,
    VersionId,
};
use tonic::Code;

use super::*;

fn snapshot(version: u64, predecessor: Option<u64>, branch: u8) -> ObjectPathSnapshot {
    let id = VersionId(version);
    ObjectPathSnapshot {
        tenant_id: 11,
        bucket_id: 22,
        exact_path: "ledger/entry".into(),
        head: Head {
            version: id,
            deleted: false,
            mutation_stamp: Some(MutationStamp {
                format: MUTATION_STAMP_FORMAT,
                predecessor_version: predecessor.map(VersionId),
                program_commit_cursor: None,
                mutation_fingerprint: [branch; 32],
                active_placement_log_id: PlacementLogId {
                    term: 1,
                    index: version,
                },
                serving_fence_term: 1,
                source_id: SourceId {
                    node_id: u16::from(branch.max(1)),
                    source_epoch: [branch.max(1); 32],
                },
                source_journal_position: version,
            }),
        },
        versions: vec![Version {
            id,
            blob: Some(BlobRef {
                hash: [branch; 32],
                length: 1,
            }),
            content_type: None,
            deleted: false,
            committed_at_unix_millis: version,
            protected_link_descriptor: false,
        }],
        journal_pending_versions: Vec::new(),
        journal_released_versions: Vec::new(),
        definition_locator: None,
        alias_registry: None,
        alias_registry_transition: None,
    }
}

fn baseline() -> ObjectPathSnapshot {
    let mut baseline = snapshot(1, None, 1);
    baseline.head.mutation_stamp = None;
    baseline
}

fn current_snapshot(version: u64, predecessor: Option<u64>, branch: u8) -> CurrentObjectSnapshot {
    let snapshot = snapshot(version, predecessor, branch);
    CurrentObjectSnapshot {
        tenant_id: snapshot.tenant_id,
        bucket_id: snapshot.bucket_id,
        exact_path: snapshot.exact_path,
        head: snapshot.head,
        version: snapshot.versions.into_iter().next().unwrap(),
        alias_registry: None,
    }
}

async fn stores(count: usize) -> (tempfile::TempDir, Vec<Store>) {
    let root = tempfile::tempdir().unwrap();
    let mut stores = Vec::with_capacity(count);
    for node_id in 1..=count {
        stores.push(
            Store::open(StoreOptions::new(
                root.path().join(format!("node-{node_id}")),
                u16::try_from(node_id).unwrap(),
            ))
            .await
            .unwrap(),
        );
    }
    (root, stores)
}

async fn install(store: &Store, selected: Option<&ObjectPathSnapshot>) {
    let expected = read(store);
    store
        .repair_object_path_snapshot(11, 22, "ledger/entry", expected.as_ref(), selected)
        .await
        .unwrap();
}

fn read(store: &Store) -> Option<ObjectPathSnapshot> {
    store
        .export_object_path_record(11, 22, "ledger/entry")
        .unwrap()
}

async fn reconcile_stores(
    stores: &[Store],
    required: usize,
) -> Result<Option<ObjectPathSnapshot>, Status> {
    let observations = stores
        .iter()
        .enumerate()
        .map(|(index, store)| ReplicaObservation {
            node: NodeId(u64::try_from(index + 1).unwrap()),
            snapshot: read(store),
        })
        .collect::<Vec<_>>();
    let selected = select_quorum_snapshot(&observations, required, stores.len())?;
    for (store, observation) in stores.iter().zip(&observations) {
        if observation.snapshot != selected {
            install(store, selected.as_ref()).await;
        }
    }
    Ok(selected)
}

#[tokio::test]
async fn one_of_one_selects_the_complete_local_state() {
    let (_root, stores) = stores(1).await;
    let expected = baseline();
    install(&stores[0], Some(&expected)).await;

    assert_eq!(reconcile_stores(&stores, 1).await.unwrap(), Some(expected));
}

#[test]
fn current_only_selector_requires_the_same_quorum_without_history() {
    let selected = current_snapshot(9, Some(8), 9);
    let stale = current_snapshot(8, Some(7), 8);
    assert_eq!(
        select_current_object_snapshot_quorum(
            &[Some(selected.clone()), Some(stale), Some(selected.clone())],
            2,
            3,
        )
        .unwrap(),
        Some(selected)
    );
}

#[test]
fn current_observation_identity_rejects_each_cross_object_dimension_before_selection() {
    let expected = current_snapshot(9, Some(8), 9);
    validate_current_object_observation_identity(
        Some(&expected),
        expected.tenant_id,
        expected.bucket_id,
        &expected.exact_path,
    )
    .unwrap();
    validate_current_object_observation_identity(None, 11, 22, "ledger/entry").unwrap();

    let mut wrong_tenant = expected.clone();
    wrong_tenant.tenant_id += 1;
    let mut wrong_bucket = expected.clone();
    wrong_bucket.bucket_id += 1;
    let mut wrong_path = expected;
    wrong_path.exact_path = "ledger/other".into();

    for observation in [&wrong_tenant, &wrong_bucket, &wrong_path] {
        let error =
            validate_current_object_observation_identity(Some(observation), 11, 22, "ledger/entry")
                .unwrap_err();
        assert_eq!(error.code(), Code::DataLoss);
    }
}

#[test]
fn current_only_batch_selector_preserves_input_order_and_selects_each_exact_quorum() {
    let mut a_current = current_snapshot(9, Some(8), 9);
    a_current.exact_path = "docs/a".into();
    let mut a_old = current_snapshot(8, Some(7), 8);
    a_old.exact_path = "docs/a".into();
    let mut b_current = current_snapshot(12, Some(11), 12);
    b_current.exact_path = "docs/b".into();
    let mut b_old = current_snapshot(11, Some(10), 11);
    b_old.exact_path = "docs/b".into();
    let mut c_minority = current_snapshot(4, Some(3), 4);
    c_minority.exact_path = "docs/c".into();

    let selected = select_current_object_snapshot_batch_quorum(
        &[
            vec![Some(a_current.clone()), Some(b_old), None],
            vec![Some(a_current.clone()), Some(b_current.clone()), None],
            vec![Some(a_old), Some(b_current.clone()), Some(c_minority)],
        ],
        2,
        3,
        3,
    )
    .unwrap();

    assert_eq!(
        selected,
        vec![Some(a_current), Some(b_current), None],
        "each column must be quorum-selected without reordering the requested paths"
    );
}

#[test]
fn current_only_batch_selector_rejects_short_replica_batches() {
    let error = select_current_object_snapshot_batch_quorum(
        &[vec![Some(current_snapshot(1, None, 1))], Vec::new()],
        2,
        2,
        1,
    )
    .unwrap_err();
    assert_eq!(error.code(), Code::DataLoss);
}

#[test]
fn exact_version_batch_never_substitutes_a_newer_descriptor() {
    let version_n = current_snapshot(8, Some(7), 8).version;
    let version_n_plus_one = current_snapshot(9, Some(8), 9).version;
    let entries = vec![(0, "docs/a".into(), VersionId(8))];

    let error = select_exact_version_batch_quorum(
        &[
            vec![Some(version_n.clone())],
            vec![Some(version_n)],
            vec![Some(version_n_plus_one)],
        ],
        2,
        3,
        &entries,
    )
    .unwrap_err();
    assert_eq!(error.code(), Code::DataLoss);
}

#[test]
fn exact_version_batch_fails_closed_when_only_the_newer_head_remains() {
    let version_n_plus_one = current_snapshot(9, Some(8), 9).version;
    let entries = vec![(0, "docs/a".into(), VersionId(8))];
    let error = select_exact_version_batch_quorum(
        &[
            vec![Some(version_n_plus_one.clone())],
            vec![Some(version_n_plus_one)],
        ],
        2,
        2,
        &entries,
    )
    .unwrap_err();
    assert_eq!(error.code(), Code::DataLoss);
}

#[test]
fn current_only_two_of_two_accepts_one_direct_successor() {
    let predecessor = current_snapshot(8, Some(7), 8);
    let successor = current_snapshot(9, Some(8), 9);
    assert_eq!(
        select_current_object_snapshot_quorum(&[Some(predecessor), Some(successor.clone())], 2, 2,)
            .unwrap(),
        Some(successor)
    );
}

#[test]
fn guarded_read_rejects_an_unreplicated_coordinator_successor() {
    let expected = current_snapshot(1, None, 1);
    let coordinator_successor = current_snapshot(2, Some(1), 2);

    let error = select_guarded_current_object_snapshot_quorum(
        &[
            Some(coordinator_successor),
            Some(expected.clone()),
            Some(expected),
        ],
        VersionId(1),
        2,
        3,
    )
    .unwrap_err();

    assert_eq!(error.code(), Code::Unavailable);
}

#[test]
fn guarded_read_rejects_an_unreplicated_coordinator_delete() {
    let expected = current_snapshot(1, None, 1);
    let mut coordinator_delete = current_snapshot(2, Some(1), 2);
    coordinator_delete.head.deleted = true;
    coordinator_delete.version.deleted = true;
    coordinator_delete.version.blob = None;

    let error = select_guarded_current_object_snapshot_quorum(
        &[
            Some(coordinator_delete),
            Some(expected.clone()),
            Some(expected),
        ],
        VersionId(1),
        2,
        3,
    )
    .unwrap_err();

    assert_eq!(error.code(), Code::Unavailable);
}

#[test]
fn guarded_read_accepts_only_an_exact_expected_quorum_without_a_conflicting_candidate() {
    let expected = current_snapshot(1, None, 1);
    assert_eq!(
        select_guarded_current_object_snapshot_quorum(
            &[Some(expected.clone()), Some(expected.clone()), None],
            VersionId(1),
            2,
            3,
        )
        .unwrap(),
        expected
    );

    let successor = current_snapshot(2, Some(1), 2);
    assert_eq!(
        select_guarded_current_object_snapshot_quorum(
            &[Some(current_snapshot(1, None, 1)), Some(successor)],
            VersionId(2),
            2,
            2,
        )
        .unwrap_err()
        .code(),
        Code::Unavailable
    );
}

#[tokio::test]
async fn two_of_two_repairs_one_direct_predecessor() {
    let (_root, stores) = stores(2).await;
    let expected = snapshot(2, Some(1), 2);
    for store in &stores {
        install(store, Some(&expected)).await;
    }
    assert_eq!(reconcile_stores(&stores, 2).await.unwrap(), Some(expected));

    install(&stores[1], Some(&baseline())).await;
    let selected = reconcile_stores(&stores, 2).await.unwrap();
    assert_eq!(selected, read(&stores[0]));
    assert_eq!(read(&stores[0]), read(&stores[1]));
}

#[tokio::test]
async fn two_of_two_repairs_absence_to_the_first_stamped_version() {
    let (_root, stores) = stores(2).await;
    let first = snapshot(1, None, 1);
    install(&stores[0], Some(&first)).await;

    assert_eq!(
        reconcile_stores(&stores, 2).await.unwrap(),
        Some(first.clone())
    );
    assert!(
        stores
            .iter()
            .all(|store| read(store) == Some(first.clone()))
    );
}

#[tokio::test]
async fn two_of_three_repairs_stale_and_missing_replicas() {
    let (_root, stores) = stores(3).await;
    let expected = snapshot(2, Some(1), 2);
    install(&stores[0], Some(&expected)).await;
    install(&stores[1], Some(&expected)).await;
    install(&stores[2], Some(&baseline())).await;

    assert_eq!(
        reconcile_stores(&stores, 2).await.unwrap(),
        Some(expected.clone())
    );
    assert!(
        stores
            .iter()
            .all(|store| read(store) == Some(expected.clone()))
    );

    install(&stores[2], None).await;
    assert_eq!(
        reconcile_stores(&stores, 2).await.unwrap(),
        Some(expected.clone())
    );
    assert!(
        stores
            .iter()
            .all(|store| read(store) == Some(expected.clone()))
    );
}

#[tokio::test]
async fn higher_minority_cannot_beat_a_lower_exact_quorum() {
    let (_root, stores) = stores(3).await;
    let committed = baseline();
    let unacknowledged = snapshot(2, Some(1), 2);
    install(&stores[0], Some(&committed)).await;
    install(&stores[1], Some(&committed)).await;
    install(&stores[2], Some(&unacknowledged)).await;

    assert_eq!(
        reconcile_stores(&stores, 2).await.unwrap(),
        Some(committed.clone())
    );
    assert!(
        stores
            .iter()
            .all(|store| read(store) == Some(committed.clone()))
    );
}

#[tokio::test]
async fn exact_quorum_repairs_multi_gap_and_minority_sibling() {
    let (_root, stores) = stores(3).await;
    let winner = snapshot(3, Some(2), 3);
    install(&stores[0], Some(&winner)).await;
    install(&stores[1], Some(&winner)).await;
    install(&stores[2], Some(&baseline())).await;
    assert_eq!(
        reconcile_stores(&stores, 2).await.unwrap(),
        Some(winner.clone())
    );
    assert_eq!(read(&stores[2]), Some(winner));

    let winner = snapshot(4, Some(3), 4);
    let sibling = snapshot(5, Some(3), 5);
    install(&stores[0], Some(&winner)).await;
    install(&stores[1], Some(&winner)).await;
    install(&stores[2], Some(&sibling)).await;
    assert_eq!(
        reconcile_stores(&stores, 2).await.unwrap(),
        Some(winner.clone())
    );
    assert_eq!(read(&stores[2]), Some(winner));
}

#[tokio::test]
async fn sibling_or_lineage_gap_without_exact_quorum_fails_unavailable() {
    let (_root, stores) = stores(2).await;
    let sibling_a = snapshot(2, Some(1), 2);
    let sibling_b = snapshot(3, Some(1), 3);
    install(&stores[0], Some(&sibling_a)).await;
    install(&stores[1], Some(&sibling_b)).await;
    assert_eq!(
        reconcile_stores(&stores, 2).await.unwrap_err().code(),
        Code::Unavailable
    );

    let gap = snapshot(5, Some(4), 5);
    install(&stores[1], Some(&gap)).await;
    assert_eq!(
        reconcile_stores(&stores, 2).await.unwrap_err().code(),
        Code::Unavailable
    );
}

#[test]
fn direct_lineage_never_crosses_object_identity() {
    let predecessor = baseline();
    let mut unrelated = snapshot(2, Some(1), 2);
    unrelated.exact_path = "another/object".into();

    assert_eq!(
        select_object_snapshot_quorum(&[Some(predecessor), Some(unrelated)], 2, 2)
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
}

#[tokio::test]
async fn delayed_compare_and_repair_never_rolls_back_a_concurrent_commit() {
    let (_root, stores) = stores(1).await;
    let observed = baseline();
    let advanced = snapshot(2, Some(1), 2);
    install(&stores[0], Some(&observed)).await;
    stores[0]
        .repair_object_path_snapshot(11, 22, "ledger/entry", Some(&observed), Some(&advanced))
        .await
        .unwrap();

    assert_eq!(
        stores[0]
            .repair_object_path_snapshot(11, 22, "ledger/entry", Some(&observed), None,)
            .await
            .unwrap_err(),
        ObjectSnapshotError::RepairPreconditionFailed
    );
    assert_eq!(read(&stores[0]), Some(advanced));
}
