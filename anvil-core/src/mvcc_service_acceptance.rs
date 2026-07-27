//! Source-level acceptance guards for product-service migration to MVCC.
//!
//! Runtime/fault tests exercise the transaction engine itself. These guards
//! prevent registered product RPCs from silently regressing to the legacy
//! commit-then-follow-up-write shape while the wider E2E suite is stabilised.

#[test]
fn admin_product_mutations_use_internal_transactions_and_certified_consequences() {
    let source = include_str!("services/admin.rs");
    for operation in [
        "tenant-create",
        "application-create",
        "application-secret-rotate",
        "application-policy-grant",
        "application-policy-revoke",
        "bucket-create",
        "bucket-public-access",
    ] {
        assert!(
            source.contains(&format!(
                "begin_admin_product_transaction(self, &principal, context, \"{operation}\")"
            )),
            "admin operation {operation} is not routed through internal MVCC"
        );
    }
    assert!(source.contains("stage_bucket_defaults("));
    assert!(source.contains("stage_bucket_public_read_tuple("));
    assert!(source.contains("stage_delegated_action_tuple_batch_with_admin_audit("));
    assert!(source.contains("stage_admin_application_result("));
    assert!(source.contains("stage_or_verify_admin_policy_result("));
    assert!(source.contains("TenantLocatorFinalizationJob"));
    assert!(!source.contains("write_delegated_action_tuple_with_admin_audit("));
}

#[test]
fn admin_retry_keys_bind_changed_input() {
    let helpers = include_str!("services/admin/helpers.rs");
    assert!(helpers.contains("admin tenant idempotency key was already used for different input"));
    assert!(
        helpers.contains("admin application idempotency key was already used for different input")
    );
    assert!(helpers.contains("admin policy idempotency key was already used for different input"));
    assert!(helpers.contains("resolved_idempotency_result("));
}

#[test]
fn personaldb_effects_are_certified_or_durable_postcommit_work() {
    let source = include_str!("services/personaldb.rs");
    assert!(source.contains("stage_personaldb_row_owner_grants("));
    assert!(source.contains("PersonalDbPostCommitJob"));
    assert!(source.contains("add_job("));
    assert!(source.contains("PERSONALDB_PROJECTION_WRITEBACK_RESULT_NAMESPACE"));
    assert!(source.contains("resolved_idempotency_result("));
    assert!(!source.contains("materialize_personaldb_row_owner_grants("));
    assert!(!source.contains(".write_authz_tuple_batch(actor.tenant_id"));
}

#[test]
fn tenant_locator_obligation_is_retryable_and_only_removed_after_publication() {
    let worker = include_str!("persistence/tenant_locator_finalization.rs");
    let admin = include_str!("services/admin.rs");
    let compact_admin = admin.split_whitespace().collect::<String>();
    assert!(compact_admin.contains("locator_job.mutation()"));
    let publish = worker
        .find("write_mesh_tenant_locators(")
        .expect("worker must publish the mesh locator");
    let validate = worker
        .find("validate_assignment(&guard)")
        .expect("worker must fence completion");
    let tombstone = worker
        .find("ProductMutation { key, value: None }")
        .expect("worker must retire the completed obligation");
    assert!(publish < validate && validate < tombstone);
}

#[test]
fn tenant_locator_job_identity_and_payload_are_deterministic() {
    let tenant = crate::persistence::Tenant {
        id: 42,
        name: "tenant-a".to_string(),
    };
    let job = crate::tenant_locator_finalization_job::TenantLocatorFinalizationJob {
        cluster_id: "cluster-a".to_string(),
        transaction_id: "tx-a".to_string(),
        tenant,
        idempotency_key: "request-a".to_string(),
        home_region: "eu-west".to_string(),
    };
    let first = job.mutation().unwrap();
    let second = job.mutation().unwrap();
    assert_eq!(first, second);
    let decoded: crate::tenant_locator_finalization_job::TenantLocatorFinalizationJob =
        serde_json::from_slice(first.value.as_deref().unwrap()).unwrap();
    assert_eq!(decoded.transaction_id, "tx-a");
    assert_eq!(decoded.home_region, "eu-west");
}
