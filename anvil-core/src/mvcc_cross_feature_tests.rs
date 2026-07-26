use std::time::Duration;

use crate::{
    AppState,
    config::Config,
    mvcc_product::ProductMutation,
    mvcc_transaction::{CertificationResult, DurabilityLevel, LogicalKey, ReadConsistency},
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

async fn begin(
    state: &AppState,
    idempotency_key: &str,
    now: u64,
) -> crate::mvcc_transaction::TransactionHandle {
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
    let outbox = state
        .mvcc
        .runtime
        .local_store()
        .outbox_records_after(0, 10)
        .unwrap();
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].commit_version, commit_version);
    assert_eq!(outbox[0].payload, b"object-committed");
    let job_id = ObjectMaterialisationJob::decode(&materialisation_job(
        state.mvcc.cluster_id(),
        &transaction.transaction_id,
    ))
    .unwrap()
    .job_id()
    .unwrap();
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
    assert!(
        state
            .mvcc
            .runtime
            .local_store()
            .outbox_records_after(0, 10)
            .unwrap()
            .is_empty()
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
