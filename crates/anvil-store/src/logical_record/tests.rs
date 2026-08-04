use argon2::{Params, Version};
use tempfile::TempDir;

use super::*;
use crate::{LocalChange, StoreOptions};

async fn open_store(root: &TempDir, node_id: u16) -> Store {
    Store::open(StoreOptions::new(root.path(), node_id))
        .await
        .unwrap()
}

fn context(version: u64) -> LogicalRecordMutationContext {
    LogicalRecordMutationContext {
        record_version: VersionId(version),
        active_placement_log_id: PlacementLogId { term: 3, index: 9 },
        serving_fence_term: 4,
    }
}

fn policy_value(immutable: &[&str], program_only: &[&str]) -> LogicalRecordValue {
    LogicalRecordValue::BucketPolicy {
        tenant_id: 7,
        bucket_id: 11,
        policy: BucketPolicy {
            immutable_prefixes: immutable.iter().map(|value| (*value).to_owned()).collect(),
            program_only_prefixes: program_only
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
        },
    }
}

fn policy_id() -> LogicalRecordId {
    LogicalRecordId::BucketPolicy {
        tenant_id: 7,
        bucket_id: 11,
    }
}

fn put_raw(store: &Store, cf: &'static str, key: &[u8], value: &[u8]) {
    store
        .db
        .put_cf(store.db.cf_handle(cf).unwrap(), key, value)
        .unwrap();
}

#[tokio::test]
async fn released_raw_fixtures_have_frozen_baseline_evidence() {
    let root = tempfile::tempdir().unwrap();
    let store = open_store(&root, 1).await;
    let tenant = StorageTenantId::parse("acme").unwrap();
    let tenant_id = LogicalRecordId::TenantNameClaim {
        storage_tenant: tenant.clone(),
    };
    // Released 0.5.0 name claims are one big-endian u64.
    put_raw(
        &store,
        CF_NAMES,
        &tenant_name_key("acme"),
        &[0, 0, 0, 0, 0, 0, 0, 7],
    );
    let LogicalRecordCandidate::Baseline {
        typed_value,
        baseline_hash,
    } = store.logical_record_candidate(&tenant_id).unwrap().unwrap()
    else {
        panic!("released fixture must remain a baseline")
    };
    assert_eq!(
        typed_value,
        LogicalRecordValue::TenantNameClaim {
            storage_tenant: tenant,
            tenant_id: 7,
        }
    );
    assert_eq!(
        hex::encode(baseline_hash.0),
        "b0ac247bc876037d6ce5a14c221231341733f3fc38921af7d6a7d90f8a6f1d71"
    );

    let options_id = LogicalRecordId::BucketOptions {
        tenant_id: 7,
        bucket_id: 11,
    };
    // Released 0.5.0 bucket versioning is one byte; 1 means enabled.
    put_raw(&store, CF_BUCKET_OPTIONS, &identity(7, 11).encode(), &[1]);
    let LogicalRecordCandidate::Baseline {
        typed_value,
        baseline_hash,
    } = store
        .logical_record_candidate(&options_id)
        .unwrap()
        .unwrap()
    else {
        panic!("released fixture must remain a baseline")
    };
    assert_eq!(
        typed_value,
        LogicalRecordValue::BucketOptions {
            tenant_id: 7,
            bucket_id: 11,
            versioning: ObjectVersioning::Enabled,
        }
    );
    assert_eq!(
        hex::encode(baseline_hash.0),
        "f110871ff4fce0b9a0416b43266a3fbcbec1bb2958cae76db09361dee0500b11"
    );
}

#[tokio::test]
async fn baseline_mutation_constructs_without_commit_then_replays_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let store = open_store(&root, 1).await;
    let raw = br#"{"immutable_prefixes":[],"program_only_prefixes":[]}"#;
    put_raw(&store, CF_POLICIES, &identity(7, 11).encode(), raw);
    let before = store
        .logical_record_candidate(&policy_id())
        .unwrap()
        .unwrap();
    let mutation = store
        .construct_logical_record_mutation(policy_value(&["ledger"], &[]), context(100))
        .unwrap();
    assert!(matches!(
        mutation.predecessor,
        LogicalRecordPredecessor::BaselineHash(_)
    ));
    assert_eq!(
        store
            .logical_record_candidate(&policy_id())
            .unwrap()
            .unwrap(),
        before
    );
    assert_eq!(
        store.commit_logical_record_mutation(&mutation).unwrap(),
        LogicalRecordApplied {
            record_version: VersionId(100),
            replayed: false,
        }
    );
    assert!(
        store
            .apply_logical_record_mutation_replica(&mutation)
            .unwrap()
            .replayed
    );
    drop(store);

    let reopened = open_store(&root, 1).await;
    assert_eq!(
        reopened.logical_record_candidate(&policy_id()).unwrap(),
        Some(LogicalRecordCandidate::Versioned(mutation))
    );
}

#[tokio::test]
async fn journaled_logical_mutation_commits_one_typed_aggregate_change() {
    let root = tempfile::tempdir().unwrap();
    let store = open_store(&root, 1).await;
    let mutation = store
        .construct_logical_record_mutation(policy_value(&["ledger"], &[]), context(100))
        .unwrap();

    let applied = store
        .apply_logical_record_mutation_journaled(&mutation)
        .await
        .unwrap();
    assert!(!applied.replayed);
    let changes = store.scan_local_changes(0, 10).unwrap();
    let [LocalChange::AggregateChanged(change)] = changes.as_slice() else {
        panic!("expected one logical-record aggregate change")
    };
    assert_eq!(change.aggregate_kind, AggregateKind::LogicalRecord);
    assert_eq!(change.revision, 100);
    assert_eq!(
        serde_json::from_slice::<LogicalRecordId>(&change.aggregate_key).unwrap(),
        policy_id()
    );

    assert!(
        store
            .apply_logical_record_mutation_journaled(&mutation)
            .await
            .unwrap()
            .replayed
    );
    assert_eq!(store.scan_local_changes(0, 10).unwrap().len(), 1);
}

#[tokio::test]
async fn replica_rejects_gap_sibling_and_tampering() {
    let coordinator_root = tempfile::tempdir().unwrap();
    let coordinator = open_store(&coordinator_root, 1).await;
    let baseline = br#"{"immutable_prefixes":[],"program_only_prefixes":[]}"#;
    put_raw(
        &coordinator,
        CF_POLICIES,
        &identity(7, 11).encode(),
        baseline,
    );
    let first = coordinator
        .construct_logical_record_mutation(policy_value(&["ledger"], &[]), context(100))
        .unwrap();
    let sibling = coordinator
        .construct_logical_record_mutation(policy_value(&[], &["private"]), context(101))
        .unwrap();

    let empty_root = tempfile::tempdir().unwrap();
    let empty = open_store(&empty_root, 2).await;
    assert_eq!(
        empty
            .apply_logical_record_mutation_replica(&first)
            .unwrap_err(),
        LogicalRecordError::LineageGap
    );

    assert!(
        !coordinator
            .apply_logical_record_mutation_replica(&first)
            .unwrap()
            .replayed
    );
    assert_eq!(
        coordinator
            .apply_logical_record_mutation_replica(&sibling)
            .unwrap_err(),
        LogicalRecordError::Sibling
    );

    let mut tampered = first;
    let LogicalRecordValue::BucketPolicy { policy, .. } = &mut tampered.typed_value else {
        unreachable!()
    };
    policy.immutable_prefixes = vec!["different".into()];
    assert_eq!(
        tampered.validate().unwrap_err(),
        LogicalRecordError::Tampered
    );
}

#[tokio::test]
async fn write_once_claims_reject_replacement_and_resolve_enveloped_ids() {
    let root = tempfile::tempdir().unwrap();
    let store = open_store(&root, 1).await;
    let tenant = StorageTenantId::parse("acme").unwrap();
    let tenant_claim = LogicalRecordValue::TenantNameClaim {
        storage_tenant: tenant,
        tenant_id: 7,
    };
    let tenant_mutation = store
        .construct_logical_record_mutation(tenant_claim, context(100))
        .unwrap();
    store
        .commit_logical_record_mutation(&tenant_mutation)
        .unwrap();
    assert_eq!(
        store
            .construct_logical_record_mutation(
                LogicalRecordValue::TenantNameClaim {
                    storage_tenant: StorageTenantId::parse("acme").unwrap(),
                    tenant_id: 8,
                },
                context(101),
            )
            .unwrap_err(),
        LogicalRecordError::Immutable
    );

    let bucket_claim = LogicalRecordValue::BucketNameClaim {
        tenant_id: 7,
        bucket: "objects".into(),
        bucket_id: 11,
    };
    let bucket_mutation = store
        .construct_logical_record_mutation(bucket_claim, context(102))
        .unwrap();
    store
        .commit_logical_record_mutation(&bucket_mutation)
        .unwrap();
    assert_eq!(
        store.resolve_bucket_ids("acme", "objects").unwrap(),
        (7, 11)
    );
}

#[tokio::test]
async fn identical_released_write_once_baseline_can_be_stamped_once() {
    let root = tempfile::tempdir().unwrap();
    let store = open_store(&root, 1).await;
    put_raw(
        &store,
        CF_NAMES,
        &tenant_name_key("acme"),
        &7_u64.to_be_bytes(),
    );
    let claim = LogicalRecordValue::TenantNameClaim {
        storage_tenant: StorageTenantId::parse("acme").unwrap(),
        tenant_id: 7,
    };
    assert_eq!(
        store
            .construct_logical_record_mutation(
                LogicalRecordValue::TenantNameClaim {
                    storage_tenant: StorageTenantId::parse("acme").unwrap(),
                    tenant_id: 8,
                },
                context(99),
            )
            .unwrap_err(),
        LogicalRecordError::Immutable
    );
    let mutation = store
        .construct_logical_record_mutation(claim.clone(), context(100))
        .unwrap();
    assert!(matches!(
        mutation.predecessor,
        LogicalRecordPredecessor::BaselineHash(_)
    ));
    store.commit_logical_record_mutation(&mutation).unwrap();
    assert_eq!(
        store.logical_record_candidate(&claim.id()).unwrap(),
        Some(LogicalRecordCandidate::Versioned(mutation))
    );
}

#[test]
fn credential_debug_never_displays_verifier_material() {
    let credential = LogicalCredentialRecord {
        app_id: "worker".into(),
        client_id: "worker-client".into(),
        storage_tenant: StorageTenantId::parse("acme").unwrap(),
        active: true,
        verifier: StoredCredentialVerifier::Argon2id {
            version: Version::V0x13.into(),
            memory_cost_kib: Params::DEFAULT_M_COST,
            time_cost: Params::DEFAULT_T_COST,
            parallelism: Params::DEFAULT_P_COST,
            output_length: Params::DEFAULT_OUTPUT_LEN as u32,
            salt: [0xA5; 32],
            output: [0x5A; 32],
        },
        sigv4_secret: None,
    };
    let rendered = format!("{credential:?}");
    assert!(rendered.contains("[REDACTED]"));
    assert!(!rendered.contains("165"));
    assert!(!rendered.contains("90"));
}
