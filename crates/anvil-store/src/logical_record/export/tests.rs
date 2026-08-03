use tempfile::TempDir;

use super::*;
use crate::StoreOptions;

async fn open_store(root: &TempDir, node_id: u16) -> Store {
    Store::open(StoreOptions::new(root.path(), node_id))
        .await
        .unwrap()
}

fn put_raw(store: &Store, cf: &'static str, key: &[u8], value: &[u8]) {
    store
        .db
        .put_cf(store.db.cf_handle(cf).unwrap(), key, value)
        .unwrap();
}

fn context(version: u64) -> LogicalRecordMutationContext {
    LogicalRecordMutationContext {
        record_version: VersionId(version),
        active_placement_log_id: PlacementLogId { term: 3, index: 9 },
        serving_fence_term: 4,
    }
}

fn policy_value(prefix: &str) -> LogicalRecordValue {
    LogicalRecordValue::BucketPolicy {
        tenant_id: 7,
        bucket_id: 11,
        policy: BucketPolicy {
            immutable_prefixes: vec![prefix.to_owned()],
            program_only_prefixes: Vec::new(),
        },
    }
}

fn install_export_fixtures(store: &Store) {
    put_raw(
        store,
        CF_NAMES,
        &tenant_name_key("acme"),
        &7_u64.to_be_bytes(),
    );
    put_raw(
        store,
        CF_NAMES,
        &tenant_name_key("beta"),
        &8_u64.to_be_bytes(),
    );
    put_raw(
        store,
        CF_NAMES,
        &bucket_name_key(TenantId(7), "objects"),
        &11_u64.to_be_bytes(),
    );
    put_raw(store, CF_BUCKET_OPTIONS, &identity(7, 11).encode(), &[1]);
    put_raw(
        store,
        CF_CREDENTIALS,
        &application_key("worker"),
        br#"{"format_version":1,"app_id":"worker","client_id":"worker-client","storage_tenant":"acme"}"#,
    );
}

#[tokio::test]
async fn export_pages_are_bounded_deterministic_and_resume_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let store = open_store(&root, 1).await;
    install_export_fixtures(&store);

    let first = store
        .export_logical_records(None, 2, MAX_LOGICAL_RECORD_EXPORT_BYTES)
        .unwrap();
    assert_eq!(first.records.len(), 2);
    assert_eq!(
        first
            .records
            .iter()
            .map(|record| record.id.clone())
            .collect::<Vec<_>>(),
        vec![
            LogicalRecordId::TenantNameClaim {
                storage_tenant: StorageTenantId::parse("acme").unwrap(),
            },
            LogicalRecordId::TenantNameClaim {
                storage_tenant: StorageTenantId::parse("beta").unwrap(),
            },
        ]
    );
    let cursor = first.next_cursor.unwrap();
    let encoded_cursor = serde_json::to_string(&cursor).unwrap();
    let cursor: LogicalRecordCursor = serde_json::from_str(&encoded_cursor).unwrap();
    assert_eq!(
        LogicalRecordCursor::from_token(cursor.as_token()).unwrap(),
        cursor
    );
    drop(store);

    let reopened = open_store(&root, 1).await;
    let mut ids = Vec::new();
    let mut cursor = Some(cursor);
    loop {
        let page = reopened
            .export_logical_records(cursor.as_ref(), 2, MAX_LOGICAL_RECORD_EXPORT_BYTES)
            .unwrap();
        ids.extend(page.records.into_iter().map(|record| record.id));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(
        ids,
        vec![
            LogicalRecordId::BucketNameClaim {
                tenant_id: 7,
                bucket: "objects".into(),
            },
            LogicalRecordId::BucketOptions {
                tenant_id: 7,
                bucket_id: 11,
            },
            LogicalRecordId::Application {
                app_id: "worker".into(),
            },
        ]
    );
    assert_eq!(
        reopened.export_logical_records(None, 1, 1).unwrap_err(),
        LogicalRecordError::ExportRecordTooLarge {
            required_bytes: canonical_bytes(
                &reopened
                    .export_logical_records(None, 1, MAX_LOGICAL_RECORD_EXPORT_BYTES)
                    .unwrap()
                    .records[0]
            )
            .unwrap()
            .len() as u64,
        }
    );
    assert_eq!(
        LogicalRecordCursor::from_token("not-a-cursor").unwrap_err(),
        LogicalRecordError::InvalidCursor
    );
}

#[tokio::test]
async fn quorum_reconciled_snapshot_installs_baselines_and_current_envelopes_once() {
    let source_root = tempfile::tempdir().unwrap();
    let source = open_store(&source_root, 1).await;
    put_raw(
        &source,
        CF_NAMES,
        &tenant_name_key("acme"),
        &7_u64.to_be_bytes(),
    );
    put_raw(
        &source,
        CF_POLICIES,
        &identity(7, 11).encode(),
        br#"{"immutable_prefixes":[],"program_only_prefixes":[]}"#,
    );
    let policy_mutation = source
        .construct_logical_record_mutation(policy_value("ledger"), context(100))
        .unwrap();
    source
        .commit_logical_record_mutation(&policy_mutation)
        .unwrap();
    let exports = source
        .export_logical_records(
            None,
            MAX_LOGICAL_RECORD_EXPORT_RECORDS,
            MAX_LOGICAL_RECORD_EXPORT_BYTES,
        )
        .unwrap()
        .records;
    assert_eq!(exports.len(), 2);

    let destination_root = tempfile::tempdir().unwrap();
    let destination = open_store(&destination_root, 2).await;
    for record in &exports {
        assert!(
            !destination
                .install_quorum_reconciled_logical_record(record)
                .unwrap()
                .replayed
        );
        assert!(
            destination
                .install_quorum_reconciled_logical_record(record)
                .unwrap()
                .replayed
        );
        assert_eq!(
            destination.logical_record_candidate(&record.id).unwrap(),
            Some(record.candidate.clone())
        );
    }
    drop(destination);
    let reopened = open_store(&destination_root, 2).await;
    for record in &exports {
        assert_eq!(
            reopened.logical_record_candidate(&record.id).unwrap(),
            Some(record.candidate.clone())
        );
    }

    let baseline = exports
        .iter()
        .find(|record| matches!(record.candidate, LogicalRecordCandidate::Baseline { .. }))
        .unwrap();
    let mut tampered = baseline.clone();
    let LogicalRecordCandidate::Baseline { baseline_hash, .. } = &mut tampered.candidate else {
        unreachable!()
    };
    baseline_hash.0[0] ^= 1;
    let tamper_root = tempfile::tempdir().unwrap();
    let tamper_store = open_store(&tamper_root, 3).await;
    assert_eq!(
        tamper_store
            .install_quorum_reconciled_logical_record(&tampered)
            .unwrap_err(),
        LogicalRecordError::Tampered
    );

    let conflicting_root = tempfile::tempdir().unwrap();
    let conflicting = open_store(&conflicting_root, 3).await;
    let conflicting_mutation = conflicting
        .construct_logical_record_mutation(policy_value("other"), context(200))
        .unwrap();
    conflicting
        .commit_logical_record_mutation(&conflicting_mutation)
        .unwrap();
    let versioned = exports
        .iter()
        .find(|record| matches!(record.candidate, LogicalRecordCandidate::Versioned(_)))
        .unwrap();
    assert_eq!(
        conflicting
            .install_quorum_reconciled_logical_record(versioned)
            .unwrap_err(),
        LogicalRecordError::SnapshotConflict
    );
}
