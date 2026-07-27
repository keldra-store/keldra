use std::time::Duration;

use crate::{
    AppState,
    config::Config,
    mvcc_product::ProductMutation,
    mvcc_transaction::{
        CertificationResult, DurabilityLevel, HierarchicalRangeStampScheme, LogicalKey,
        ReadConsistency, TransactionBundleBuilder,
    },
    object_materialisation::{ObjectMaterialisationJob, ObjectMaterialisationOperations},
    personaldb_signing::PersonalDbProtocolKeyring,
};

const PRINCIPAL: &str = "cross-feature-test";
const NOW: u64 = 1_000_000;

async fn state() -> (tempfile::TempDir, AppState) {
    let directory = tempfile::tempdir().unwrap();
    let config = Config {
        jwt_secret: "test-secret".into(),
        anvil_secret_encryption_key:
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
        public_api_addr: "127.0.0.1:0".into(),
        api_listen_addr: "127.0.0.1:0".into(),
        region: "local".into(),
        node_id: "cross-feature-node".into(),
        bootstrap_system_admin_subject_kind: "app".into(),
        bootstrap_system_admin_subject_id: "admin-principal".into(),
        storage_path: directory
            .path()
            .join("storage")
            .to_string_lossy()
            .into_owned(),
        ..Config::default()
    };
    let state = AppState::new(config, PersonalDbProtocolKeyring::disabled())
        .await
        .unwrap();
    (directory, state)
}

fn feature_key(table_id: u16, name: &str) -> LogicalKey {
    LogicalKey {
        table_id,
        application_key: format!("cross-feature/{name}").into_bytes(),
    }
}

fn feature_rows() -> Vec<(LogicalKey, Vec<u8>)> {
    [
        (0x7101, "object-metadata"),
        (0x7102, "append-journal"),
        (0x7103, "manifest-cas"),
        (0x7104, "boundary-schema"),
        (0x7105, "index-definition"),
    ]
    .into_iter()
    .map(|(table, name)| {
        (
            feature_key(table, name),
            format!("{name}-value").into_bytes(),
        )
    })
    .collect()
}

fn materialisation_job(cluster_id: &str, transaction_id: &str) -> Vec<u8> {
    ObjectMaterialisationJob {
        schema: ObjectMaterialisationJob::SCHEMA.into(),
        cluster_id: cluster_id.into(),
        transaction_id: transaction_id.into(),
        tenant_id: 1,
        bucket_id: 2,
        bucket_name: "bucket".into(),
        object_key: "object".into(),
        object_version_id: "version-1".into(),
        target_logical_identity: "tenant/1/bucket/2/object/object/version/version-1".into(),
        representation: serde_json::json!({"kind": "blob"}),
        content_hash: "content-hash".into(),
        payload_length: 3,
        frozen_object: serde_json::json!({
            "version_id": "version-1",
            "content_hash": "content-hash",
            "size": 3
        }),
        source_manifest_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .into(),
        content_type: None,
        user_metadata: serde_json::json!({}),
        index_policy_snapshot: serde_json::json!({}),
        originating_snapshot_version: 0,
        frozen_index_definitions: Vec::new(),
        authz_revision: 0,
        boundary_schema: None,
        boundary_schema_generation: 0,
        boundary_schema_hash: None,
        requested_operations: ObjectMaterialisationOperations {
            extract_boundaries: true,
            maintain_indexes: true,
        },
        requested_at_unix_ms: NOW,
    }
    .canonical_bytes()
    .unwrap()
}

fn transaction_outbox_records(
    state: &AppState,
    transaction_id: &str,
) -> Vec<crate::mvcc_store::OutboxRecord> {
    state
        .mvcc
        .runtime
        .local_store()
        .outbox_records_after(0, usize::MAX)
        .unwrap()
        .into_iter()
        .filter(|record| record.transaction_id == transaction_id)
        .collect()
}

fn materialisation_job_id(cluster_id: &str, transaction_id: &str) -> String {
    ObjectMaterialisationJob::decode(&materialisation_job(cluster_id, transaction_id))
        .unwrap()
        .job_id()
        .unwrap()
}

async fn begin(
    state: &AppState,
    idempotency_key: &str,
    now: u64,
) -> crate::mvcc_open_transactions::TransactionHandle {
    state
        .mvcc
        .open_transactions
        .begin(
            state.mvcc.runtime.as_ref(),
            state.mvcc.cluster_id(),
            PRINCIPAL,
            idempotency_key,
            Duration::from_secs(30),
            DurabilityLevel::Local,
            ReadConsistency::Linearized,
            now,
        )
        .await
        .unwrap()
}

fn stage_features(state: &AppState, transaction_id: &str, now: u64) {
    state
        .mvcc
        .stage_product_mutations(
            transaction_id,
            PRINCIPAL,
            feature_rows()
                .into_iter()
                .map(|(key, value)| ProductMutation::put(key, value))
                .collect(),
            now,
        )
        .unwrap();
    state
        .mvcc
        .open_transactions
        .add_stream_event(
            transaction_id,
            crate::mvcc_outbox::StreamOutboxEvent::new(
                7,
                "events",
                "partition-7",
                "object.committed",
                b"object-committed".to_vec(),
            )
            .unwrap(),
            now,
        )
        .unwrap();
    state
        .mvcc
        .open_transactions
        .add_job(
            transaction_id,
            materialisation_job(state.mvcc.cluster_id(), transaction_id),
            now,
        )
        .unwrap();
}

#[tokio::test]
async fn one_transaction_atomically_publishes_every_cross_feature_projection() {
    let (_directory, state) = state().await;
    let transaction = begin(&state, "successful-cross-feature", NOW).await;
    stage_features(&state, &transaction.transaction_id, NOW + 1);

    for (key, expected) in feature_rows() {
        assert_eq!(
            state
                .mvcc
                .read_transaction_value(&transaction.transaction_id, PRINCIPAL, &key)
                .unwrap(),
            Some(expected)
        );
        assert_eq!(state.mvcc.read_latest_value(&key).unwrap(), None);
    }

    let outcome = state
        .mvcc
        .open_transactions
        .commit(
            state.mvcc.runtime.as_ref(),
            &transaction.transaction_id,
            PRINCIPAL,
            NOW + 2,
        )
        .await
        .unwrap();
    let commit_version = match outcome.certification {
        CertificationResult::Committed { commit_version } => commit_version,
        other => panic!("cross-feature commit failed: {other:?}"),
    };

    for (key, expected) in feature_rows() {
        assert_eq!(state.mvcc.read_latest_value(&key).unwrap(), Some(expected));
    }
    let outbox = transaction_outbox_records(&state, &transaction.transaction_id);
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].commit_version, commit_version);
    let stream_event = crate::mvcc_outbox::StreamOutboxEvent::decode(&outbox[0].payload).unwrap();
    assert_eq!(stream_event.partition_id, 7);
    assert_eq!(stream_event.stream_id, "events");
    assert_eq!(stream_event.stream_partition, "partition-7");
    assert_eq!(stream_event.record_kind, "object.committed");
    assert_eq!(stream_event.payload, b"object-committed");
    let job_id = materialisation_job_id(state.mvcc.cluster_id(), &transaction.transaction_id);
    assert!(
        state
            .mvcc
            .runtime
            .local_store()
            .object_materialisation_record(&job_id)
            .unwrap()
            .is_some()
    );

    let replay = state
        .mvcc
        .open_transactions
        .commit(
            state.mvcc.runtime.as_ref(),
            &transaction.transaction_id,
            PRINCIPAL,
            NOW + 3,
        )
        .await
        .unwrap();
    assert_eq!(replay.certification, outcome.certification);
}

#[tokio::test]
async fn one_conflicting_feature_aborts_without_partial_visibility() {
    let (_directory, state) = state().await;
    let transaction = begin(&state, "conflicting-cross-feature", NOW).await;
    stage_features(&state, &transaction.transaction_id, NOW + 1);
    let conflict_key = feature_key(0x7103, "manifest-cas");
    state
        .mvcc
        .autocommit_product_mutations(
            "competing-writer",
            "manifest-winner",
            vec![ProductMutation::put(
                conflict_key.clone(),
                b"winner".to_vec(),
            )],
            DurabilityLevel::Local,
            NOW + 2,
        )
        .await
        .unwrap();

    let outcome = state
        .mvcc
        .open_transactions
        .commit(
            state.mvcc.runtime.as_ref(),
            &transaction.transaction_id,
            PRINCIPAL,
            NOW + 3,
        )
        .await
        .unwrap();
    assert!(matches!(
        outcome.certification,
        CertificationResult::Aborted { .. }
    ));
    for (key, _) in feature_rows() {
        let expected = (key == conflict_key).then(|| b"winner".to_vec());
        assert_eq!(state.mvcc.read_latest_value(&key).unwrap(), expected);
    }
    assert!(transaction_outbox_records(&state, &transaction.transaction_id).is_empty());
    let job_id = materialisation_job_id(state.mvcc.cluster_id(), &transaction.transaction_id);
    assert!(
        state
            .mvcc
            .runtime
            .local_store()
            .object_materialisation_record(&job_id)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn a_deleted_logical_key_can_be_recreated() {
    let (_directory, state) = state().await;
    let key = feature_key(0x7101, "recreated-after-delete");

    let create_version = state
        .mvcc
        .autocommit_product_mutations(
            PRINCIPAL,
            "create-before-delete",
            vec![ProductMutation::put(key.clone(), b"first".to_vec())],
            DurabilityLevel::Local,
            NOW,
        )
        .await
        .unwrap();
    let delete_version = state
        .mvcc
        .autocommit_product_mutations(
            PRINCIPAL,
            "delete-before-recreate",
            vec![ProductMutation::delete(key.clone())],
            DurabilityLevel::Local,
            NOW + 1,
        )
        .await
        .unwrap();
    assert_eq!(state.mvcc.read_latest_value(&key).unwrap(), None);

    let recreate_version = state
        .mvcc
        .autocommit_product_mutations_with_predicates(
            PRINCIPAL,
            "recreate-after-delete",
            vec![ProductMutation::put(key.clone(), b"second".to_vec())],
            vec![(key.clone(), crate::mvcc_transaction::PredicateKind::Absent)],
            DurabilityLevel::Local,
            NOW + 2,
        )
        .await
        .unwrap();

    assert!(create_version < delete_version);
    assert!(delete_version < recreate_version);
    assert_eq!(
        state.mvcc.read_latest_value(&key).unwrap(),
        Some(b"second".to_vec())
    );
}

#[tokio::test]
async fn reads_remain_at_the_begin_snapshot_and_foreign_cluster_staging_is_rejected() {
    let (_directory, state) = state().await;
    let key = feature_key(0x7101, "snapshot-object");
    state
        .mvcc
        .autocommit_product_mutations(
            PRINCIPAL,
            "snapshot-v1",
            vec![ProductMutation::put(key.clone(), b"v1".to_vec())],
            DurabilityLevel::Local,
            NOW,
        )
        .await
        .unwrap();
    let transaction = begin(&state, "fixed-snapshot", NOW + 1).await;
    state
        .mvcc
        .autocommit_product_mutations(
            "later-writer",
            "snapshot-v2",
            vec![ProductMutation::put(key.clone(), b"v2".to_vec())],
            DurabilityLevel::Local,
            NOW + 2,
        )
        .await
        .unwrap();

    assert_eq!(
        state
            .mvcc
            .read_transaction_value(&transaction.transaction_id, PRINCIPAL, &key)
            .unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(
        state.mvcc.read_latest_value(&key).unwrap(),
        Some(b"v2".to_vec())
    );
    let error = state
        .mvcc
        .open_transactions
        .put(
            &transaction.transaction_id,
            "another-cluster",
            feature_key(0x7102, "foreign"),
            b"forbidden".to_vec(),
            NOW + 3,
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("staged resource belongs to another cluster")
    );
}

#[test]
fn local_apply_watermark_rejects_backward_versions_without_changing_visibility() {
    let directory = tempfile::tempdir().unwrap();
    let store = crate::mvcc_store::LocalMvccStore::open(directory.path()).unwrap();
    let newer_key = feature_key(0x7101, "newer");
    let older_key = feature_key(0x7101, "older");
    let mut newer = TransactionBundleBuilder::new(
        "cluster",
        "newer-transaction",
        0,
        PRINCIPAL,
        HierarchicalRangeStampScheme::new(),
    );
    newer.put(newer_key.clone(), b"newer".to_vec());
    store
        .apply_certified_bundle(7, &newer.build().unwrap())
        .unwrap();

    let mut older = TransactionBundleBuilder::new(
        "cluster",
        "older-transaction",
        0,
        PRINCIPAL,
        HierarchicalRangeStampScheme::new(),
    );
    older.put(older_key.clone(), b"older".to_vec());
    assert!(
        store
            .apply_certified_bundle(6, &older.build().unwrap())
            .unwrap_err()
            .to_string()
            .contains("below applied version")
    );
    assert_eq!(store.applied_version().unwrap(), 7);
    assert_eq!(
        store.read_latest(&newer_key).unwrap().unwrap().value,
        b"newer"
    );
    assert!(store.read_latest(&older_key).unwrap().is_none());
}

#[tokio::test]
async fn successor_epoch_aborts_stale_atomic_publication_and_retry_is_stable() {
    let (_directory, state) = state().await;
    let lease_key = feature_key(0x7201, "task-lease");
    let proof_key = feature_key(0x7202, "derived-proof-head");
    let checkpoint_key = feature_key(0x7203, "watch-checkpoint");
    let lease_epoch_one = b"lease-epoch-1".to_vec();
    let lease_epoch_two = b"lease-epoch-2".to_vec();

    state
        .mvcc
        .autocommit_product_mutations(
            PRINCIPAL,
            "seed-lease-epoch",
            vec![ProductMutation::put(
                lease_key.clone(),
                lease_epoch_one.clone(),
            )],
            DurabilityLevel::Local,
            NOW,
        )
        .await
        .unwrap();

    let stale = begin(&state, "stale-proof-checkpoint-publication", NOW + 1).await;
    assert_eq!(
        state
            .mvcc
            .read_transaction_value(&stale.transaction_id, PRINCIPAL, &lease_key)
            .unwrap(),
        Some(lease_epoch_one.clone())
    );
    state
        .mvcc
        .stage_predicate(
            &stale.transaction_id,
            PRINCIPAL,
            lease_key.clone(),
            crate::mvcc_transaction::PredicateKind::ValueHash(
                *blake3::hash(&lease_epoch_one).as_bytes(),
            ),
            NOW + 1,
        )
        .unwrap();
    state
        .mvcc
        .stage_product_mutations(
            &stale.transaction_id,
            PRINCIPAL,
            vec![
                ProductMutation::put(proof_key.clone(), b"proof".to_vec()),
                ProductMutation::put(checkpoint_key.clone(), b"checkpoint".to_vec()),
            ],
            NOW + 1,
        )
        .unwrap();

    state
        .mvcc
        .autocommit_product_mutations_with_predicates(
            PRINCIPAL,
            "install-successor-lease-epoch",
            vec![ProductMutation::put(
                lease_key.clone(),
                lease_epoch_two.clone(),
            )],
            vec![(
                lease_key.clone(),
                crate::mvcc_transaction::PredicateKind::ValueHash(
                    *blake3::hash(&lease_epoch_one).as_bytes(),
                ),
            )],
            DurabilityLevel::Local,
            NOW + 2,
        )
        .await
        .unwrap();

    // The stale transaction retains its fixed snapshot even after takeover.
    assert_eq!(
        state
            .mvcc
            .read_transaction_value(&stale.transaction_id, PRINCIPAL, &lease_key)
            .unwrap(),
        Some(lease_epoch_one)
    );
    let outcome = state
        .mvcc
        .open_transactions
        .commit(
            state.mvcc.runtime.as_ref(),
            &stale.transaction_id,
            PRINCIPAL,
            NOW + 3,
        )
        .await
        .unwrap();
    assert!(matches!(
        outcome.certification,
        CertificationResult::Aborted { .. }
    ));
    assert_eq!(
        state.mvcc.read_latest_value(&lease_key).unwrap(),
        Some(lease_epoch_two)
    );
    assert_eq!(state.mvcc.read_latest_value(&proof_key).unwrap(), None);
    assert_eq!(state.mvcc.read_latest_value(&checkpoint_key).unwrap(), None);

    let retry = begin(&state, "stale-proof-checkpoint-publication", NOW + 4).await;
    assert_eq!(retry.transaction_id, stale.transaction_id);
    assert_eq!(
        state
            .mvcc
            .open_transactions
            .status(&retry.transaction_id, PRINCIPAL, NOW + 4)
            .unwrap()
            .state,
        "aborted"
    );
}
