use anvil_authz::{ObjectId, RealmId, TupleSubject};
use rocksdb::IteratorMode;
use tempfile::TempDir;

use super::*;
use crate::store::CF_BUCKET_OPTIONS;
use crate::{AuthzConsistency, AuthzRevision, AuthzScope, SYSTEM_STORAGE_TENANT_ID, StoreOptions};

const SECRET: &str = "secret-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

async fn store() -> (TempDir, Store) {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(directory.path(), 7))
        .await
        .unwrap();
    (directory, store)
}

fn request(app_id: &str, client_id: &str) -> SystemBootstrapRequest {
    SystemBootstrapRequest {
        app_id: app_id.into(),
        client_id: client_id.into(),
        client_secret: SECRET.into(),
    }
}

fn app(app_id: &str) -> ObjectRef {
    ObjectRef::opaque(APP_NAMESPACE, app_id).unwrap()
}

fn tenant(value: &str) -> StorageTenantId {
    StorageTenantId::parse(value).unwrap()
}

fn provision_request(
    storage_tenant: &str,
    owner_app_id: &str,
    owner_client_id: &str,
    revision: u64,
) -> ProvisionTenantRequest {
    ProvisionTenantRequest {
        storage_tenant: tenant(storage_tenant),
        owner_app_id: owner_app_id.into(),
        owner_client_id: owner_client_id.into(),
        owner_client_secret: SECRET.into(),
        principal: app("bootstrap-app"),
        expected_authorization_revision: AuthzRevision(revision),
        expected_binding_generation: 1,
    }
}

#[test]
fn bootstrap_request_debug_redacts_the_secret() {
    let request = request("bootstrap-app", "bootstrap-client");
    let debug = format!("{request:?}");
    assert!(debug.contains("bootstrap-app"));
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains(SECRET));

    let application = ApplicationCredentialRequest {
        storage_tenant: tenant("acme"),
        app_id: "app".into(),
        client_id: "client".into(),
        client_secret: SECRET.into(),
    };
    assert!(!format!("{application:?}").contains(SECRET));
    let provision = provision_request("acme", "owner", "owner-client", 3);
    assert!(!format!("{provision:?}").contains(SECRET));
}

#[tokio::test]
async fn bootstrap_installs_one_complete_system_state_batch() {
    let (_directory, store) = store().await;
    assert_eq!(
        store.system_bootstrap_state().unwrap(),
        SystemBootstrapState::Missing
    );

    store
        .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
        .unwrap();

    assert_eq!(
        store.system_bootstrap_state().unwrap(),
        SystemBootstrapState::Complete {
            version: SYSTEM_BOOTSTRAP_VERSION,
        }
    );
    assert_eq!(
        store
            .authz()
            .tenant_revision(&StorageTenantId::system())
            .unwrap(),
        AuthzRevision(3)
    );
    let snapshot = store
        .authz()
        .realm_snapshot(&AuthzScope::system(), AuthzConsistency::Latest)
        .unwrap();
    assert_eq!(snapshot.binding.generation, 1);
    assert_eq!(snapshot.binding.authz_revision, AuthzRevision(2));
    assert_eq!(snapshot.binding.tuple_count, 1);
    assert_eq!(snapshot.tuples.len(), 1);
    let tuple = &snapshot.tuples[0];
    assert_eq!(tuple.relation, "bootstrap_admin");
    assert_eq!(tuple.object.namespace, SYSTEM_NAMESPACE);
    assert_eq!(tuple.object.id, ObjectId::Opaque("_anvil".into()));
    assert_eq!(
        tuple.subject,
        TupleSubject::Object(ObjectRef::opaque(APP_NAMESPACE, "bootstrap-app").unwrap())
    );
}

#[tokio::test]
async fn completed_handoff_marker_is_durable_and_idempotent() {
    let (_directory, store) = store().await;
    assert_eq!(
        store.system_bootstrap_state().unwrap(),
        SystemBootstrapState::Missing
    );
    assert!(!store.complete_system_bootstrap_handoff().unwrap());
    assert_eq!(
        store.system_bootstrap_state().unwrap(),
        SystemBootstrapState::Complete {
            version: SYSTEM_BOOTSTRAP_VERSION,
        }
    );
    assert!(store.complete_system_bootstrap_handoff().unwrap());
}

#[tokio::test]
async fn repeat_bootstrap_is_rejected_without_minting_another_administrator() {
    let (_directory, store) = store().await;
    store
        .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
        .unwrap();

    assert!(matches!(
        store.bootstrap_system(request("other-app", "other-client")),
        Err(SystemBootstrapError::AlreadyBootstrapped)
    ));
    assert!(store.credential("other-client").unwrap().is_none());
    assert_eq!(
        store
            .authz()
            .realm_snapshot(&AuthzScope::system(), AuthzConsistency::Latest)
            .unwrap()
            .tuples
            .len(),
        1
    );
}

#[tokio::test]
async fn credential_lookup_and_verification_return_the_stable_application() {
    let (_directory, store) = store().await;
    store
        .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
        .unwrap();

    let expected = ApplicationCredential {
        app_id: "bootstrap-app".into(),
        client_id: "bootstrap-client".into(),
        storage_tenant: StorageTenantId::system(),
        active: true,
    };
    assert_eq!(
        store.credential("bootstrap-client").unwrap(),
        Some(expected.clone())
    );
    assert_eq!(
        store.verify_credential("bootstrap-client", SECRET).unwrap(),
        Some(expected)
    );
    assert!(
        store
            .verify_credential("bootstrap-client", "wrong-secret-with-at-least-32-bytes")
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .verify_credential("missing-client", SECRET)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn tenant_provisioning_commits_owner_credential_marker_and_tuple_together() {
    let (_directory, store) = store().await;
    store
        .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
        .unwrap();

    let receipt = store
        .provision_tenant(provision_request("acme", "owner-app", "owner-client", 3))
        .unwrap();

    assert!(!receipt.replayed);
    assert_eq!(receipt.authorization_revision, AuthzRevision(4));
    assert_eq!(receipt.credential.storage_tenant, tenant("acme"));
    assert_eq!(receipt.credential.app_id, "owner-app");
    assert_eq!(
        store.verify_credential("owner-client", SECRET).unwrap(),
        Some(receipt.credential.clone())
    );
    let tenant_id = store.tenant_id_by_name("acme").unwrap().unwrap();
    let marker = store
        .credentials()
        .read_json::<StoredTenant>(CF_METADATA, &tenant_record_key(tenant_id))
        .unwrap()
        .unwrap();
    assert_eq!(marker.tenant_id, tenant_id);
    assert_eq!(marker.authorization_revision, AuthzRevision(4));
    let snapshot = store
        .authz()
        .realm_snapshot(&AuthzScope::system(), AuthzConsistency::Latest)
        .unwrap();
    assert!(snapshot.tuples.contains(&Tuple::new(
        tenant_resource(&tenant("acme")).unwrap(),
        "owner",
        app("owner-app"),
    )));

    let replay = store
        .provision_tenant(provision_request("acme", "owner-app", "owner-client", 4))
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.authorization_revision, AuthzRevision(4));
    assert_eq!(
        store
            .authz()
            .tenant_revision(&StorageTenantId::system())
            .unwrap(),
        AuthzRevision(4)
    );
}

#[tokio::test]
async fn distributed_tenant_plan_is_typed_and_has_no_storage_side_effects() {
    let (_directory, store) = store().await;
    store
        .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
        .unwrap();

    let prepared = store
        .prepare_tenant_provisioning(
            provision_request("acme", "owner-app", "owner-client", 3),
            700,
        )
        .unwrap();

    assert_eq!(prepared.tenant_id, 700);
    assert_eq!(prepared.logical_records.len(), 4);
    assert_eq!(prepared.grant.expected_revision, Some(AuthzRevision(3)));
    assert_eq!(
        prepared.grant.operation_id.as_deref(),
        Some("provision-tenant-700")
    );
    assert_eq!(
        store
            .authz()
            .tenant_revision(&StorageTenantId::system())
            .unwrap(),
        AuthzRevision(3)
    );
    for value in &prepared.logical_records {
        assert!(
            store
                .logical_record_candidate(&value.id())
                .unwrap()
                .is_none()
        );
    }
    let credential = prepared
        .logical_records
        .iter()
        .find_map(|value| match value {
            LogicalRecordValue::Credential(record) => Some(record),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        credential.verify_secret(SECRET).unwrap(),
        Some(prepared.credential)
    );
    assert!(credential.verify_secret("wrong-secret").unwrap().is_none());
}

#[tokio::test]
async fn protected_system_tenant_cannot_be_provisioned_as_an_external_tenant() {
    let (_directory, store) = store().await;
    store
        .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
        .unwrap();

    assert!(matches!(
        store.provision_tenant(provision_request(
            SYSTEM_STORAGE_TENANT_ID,
            "other-owner",
            "other-client",
            3,
        )),
        Err(CredentialRepositoryError::InvalidInput(message))
            if message.contains("protected system tenant")
    ));
    assert!(store.credential("other-client").unwrap().is_none());
}

#[tokio::test]
async fn retained_tenant_name_claim_prevents_reassignment_to_a_new_stable_tenant() {
    let (_directory, store) = store().await;
    store
        .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
        .unwrap();
    store
        .provision_tenant(provision_request("acme", "owner-app", "owner-client", 3))
        .unwrap();
    let original_id = store.tenant_id_by_name("acme").unwrap().unwrap();

    // Tenant release is not a 0.5.1 API. Model its only safe future storage
    // shape: the name claim remains mapped to the original stable ID even if
    // the live tenant record is absent or later replaced by a tombstone.
    store
        .db
        .delete_cf(
            store.cf(CF_METADATA).unwrap(),
            tenant_record_key(original_id),
        )
        .unwrap();

    assert!(matches!(
        store.provision_tenant(provision_request(
            "acme",
            "replacement-owner",
            "replacement-client",
            4,
        )),
        Err(CredentialRepositoryError::Storage(message))
            if message.contains("missing stable-ID record")
    ));
    assert_eq!(store.tenant_id_by_name("acme").unwrap(), Some(original_id));
    assert!(store.credential("replacement-client").unwrap().is_none());
}

#[tokio::test]
async fn failed_tenant_provisioning_leaves_no_partial_owner_state() {
    let (_directory, store) = store().await;
    store
        .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
        .unwrap();
    let mut invalid = provision_request("acme", "owner-app", "owner-client", 3);
    invalid.owner_client_secret = "too-short".into();

    assert!(matches!(
        store.provision_tenant(invalid),
        Err(CredentialRepositoryError::InvalidInput(_))
    ));
    assert!(store.credential("owner-client").unwrap().is_none());
    assert!(store.tenant_id_by_name("acme").unwrap().is_none());
    assert_eq!(
        store
            .authz()
            .realm_snapshot(&AuthzScope::system(), AuthzConsistency::Latest)
            .unwrap()
            .tuples
            .len(),
        1
    );
}

#[tokio::test]
async fn application_credentials_create_rotate_disable_and_replay_without_plaintext() {
    let (_directory, store) = store().await;
    store
        .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
        .unwrap();
    store
        .provision_tenant(provision_request("acme", "owner-app", "owner-client", 3))
        .unwrap();
    let application = ApplicationCredentialRequest {
        storage_tenant: tenant("acme"),
        app_id: "worker-app".into(),
        client_id: "worker-client".into(),
        client_secret: SECRET.into(),
    };

    let created = store
        .create_application(application.clone(), AuthzRevision(4))
        .unwrap();
    assert!(!created.replayed);
    assert!(
        store
            .create_application(application.clone(), AuthzRevision(4))
            .unwrap()
            .replayed
    );
    assert!(
        store
            .verify_credential("worker-client", SECRET)
            .unwrap()
            .is_some()
    );

    let replacement = "replacement-0123456789abcdef0123456789abcdef0123456789abcdef";
    let rotated = store
        .rotate_application_credential(
            ApplicationCredentialRequest {
                client_secret: replacement.into(),
                ..application.clone()
            },
            AuthzRevision(4),
        )
        .unwrap();
    assert!(!rotated.replayed);
    assert!(
        store
            .verify_credential("worker-client", SECRET)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .verify_credential("worker-client", replacement)
            .unwrap()
            .is_some()
    );

    let disabled = store
        .disable_application_credential(
            tenant("acme"),
            "worker-app".into(),
            "worker-client".into(),
            AuthzRevision(4),
        )
        .unwrap();
    assert!(!disabled.credential.active);
    assert!(
        store
            .verify_credential("worker-client", replacement)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .disable_application_credential(
                tenant("acme"),
                "worker-app".into(),
                "worker-client".into(),
                AuthzRevision(4),
            )
            .unwrap()
            .replayed
    );
}

#[tokio::test]
async fn application_ids_and_client_ids_are_globally_unique_authentication_subjects() {
    let (_directory, store) = store().await;
    store
        .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
        .unwrap();
    store
        .provision_tenant(provision_request("acme", "acme-owner", "acme-client", 3))
        .unwrap();
    store
        .provision_tenant(provision_request("other", "other-owner", "other-client", 4))
        .unwrap();

    let duplicate_app = store.create_application(
        ApplicationCredentialRequest {
            storage_tenant: tenant("other"),
            app_id: "acme-owner".into(),
            client_id: "different-client".into(),
            client_secret: SECRET.into(),
        },
        AuthzRevision(5),
    );
    assert!(matches!(
        duplicate_app,
        Err(CredentialRepositoryError::AlreadyExists(_))
    ));
    let duplicate_client = store.create_application(
        ApplicationCredentialRequest {
            storage_tenant: tenant("other"),
            app_id: "different-app".into(),
            client_id: "acme-client".into(),
            client_secret: SECRET.into(),
        },
        AuthzRevision(5),
    );
    assert!(matches!(
        duplicate_client,
        Err(CredentialRepositoryError::AlreadyExists(_))
    ));
}

#[tokio::test]
async fn bucket_creation_and_role_changes_are_system_realm_tuple_batches() {
    let (_directory, store) = store().await;
    store
        .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
        .unwrap();
    store
        .provision_tenant(provision_request("acme", "owner-app", "owner-client", 3))
        .unwrap();
    let bucket = CreateBucketRequest {
        storage_tenant: tenant("acme"),
        bucket: "objects".into(),
        owner: app("owner-app"),
        principal: app("owner-app"),
        expected_authorization_revision: AuthzRevision(4),
        expected_binding_generation: 1,
        versioning: ObjectVersioning::Enabled,
    };

    let created = store.create_bucket(bucket.clone()).unwrap();
    assert_eq!(created.authorization_revision, AuthzRevision(5));
    assert_eq!(created.versioning, ObjectVersioning::Enabled);
    assert_eq!(
        store.bucket_versioning("acme", "objects").unwrap(),
        ObjectVersioning::Enabled
    );
    let tenant_id = store.tenant_id_by_name("acme").unwrap().unwrap();
    let bucket_id = store
        .bucket_id_by_name(tenant_id, "objects")
        .unwrap()
        .unwrap();
    let identity = BucketIdentity {
        tenant_id,
        bucket_id,
    };
    assert_eq!(
        store
            .db
            .get_cf(store.cf(CF_NAMES).unwrap(), tenant_name_key("acme"))
            .unwrap()
            .unwrap()
            .as_ref(),
        tenant_id.0.to_be_bytes()
    );
    assert_eq!(
        store
            .db
            .get_cf(
                store.cf(CF_NAMES).unwrap(),
                bucket_name_key(tenant_id, "objects"),
            )
            .unwrap()
            .unwrap()
            .as_ref(),
        bucket_id.0.to_be_bytes()
    );
    let stored = store
        .credentials()
        .read_json::<StoredBucket>(CF_METADATA, &bucket_record_key(identity))
        .unwrap()
        .unwrap();
    assert_eq!(stored.tenant_id, tenant_id);
    assert_eq!(stored.bucket_id, bucket_id);
    assert!(
        store
            .db
            .get_cf(store.cf(CF_BUCKET_OPTIONS).unwrap(), identity.encode())
            .unwrap()
            .is_some()
    );
    assert!(!created.replayed);
    let replayed = store.create_bucket(bucket).unwrap();
    assert!(replayed.replayed);
    assert_eq!(replayed.versioning, ObjectVersioning::Enabled);
    let role = SetApplicationRoleRequest {
        storage_tenant: tenant("acme"),
        app_id: "owner-app".into(),
        target: ApplicationRoleTarget::Bucket {
            bucket: "objects".into(),
            role: BucketApplicationRole::Writer,
        },
        granted: true,
        principal: app("owner-app"),
        expected_authorization_revision: AuthzRevision(5),
        expected_binding_generation: 1,
    };
    let granted = store.set_application_role(role.clone()).unwrap();
    assert_eq!(granted.authorization_revision, AuthzRevision(6));
    assert!(!granted.replayed);
    let mut replay = role.clone();
    replay.expected_authorization_revision = AuthzRevision(6);
    assert!(store.set_application_role(replay).unwrap().replayed);
    let mut remove = role;
    remove.granted = false;
    remove.expected_authorization_revision = AuthzRevision(6);
    assert_eq!(
        store
            .set_application_role(remove)
            .unwrap()
            .authorization_revision,
        AuthzRevision(7)
    );
    let snapshot = store
        .authz()
        .realm_snapshot(&AuthzScope::system(), AuthzConsistency::Latest)
        .unwrap();
    assert!(snapshot.tuples.contains(&Tuple::new(
        bucket_resource(&tenant("acme"), "objects").unwrap(),
        "owner",
        app("owner-app"),
    )));
    assert!(!snapshot.tuples.contains(&Tuple::new(
        bucket_resource(&tenant("acme"), "objects").unwrap(),
        "writer",
        app("owner-app"),
    )));
}

#[tokio::test]
async fn distributed_bucket_plan_places_existence_before_its_grant() {
    let (_directory, store) = store().await;
    store
        .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
        .unwrap();
    let prepared = store
        .prepare_bucket_creation(
            CreateBucketRequest {
                storage_tenant: tenant("acme"),
                bucket: "objects".into(),
                owner: app("owner-app"),
                principal: app("owner-app"),
                expected_authorization_revision: AuthzRevision(9),
                expected_binding_generation: 1,
                versioning: ObjectVersioning::Enabled,
            },
            700,
            701,
        )
        .unwrap();

    assert_eq!(prepared.bucket_id, 701);
    assert_eq!(prepared.logical_records.len(), 3);
    assert_eq!(prepared.grant.expected_revision, Some(AuthzRevision(9)));
    assert_eq!(
        prepared.grant.operation_id.as_deref(),
        Some("create-bucket-700-701")
    );
    for value in &prepared.logical_records {
        assert!(
            store
                .logical_record_candidate(&value.id())
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn stale_authorization_revision_prevents_credential_mutation() {
    let (_directory, store) = store().await;
    store
        .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
        .unwrap();
    let candidate = ApplicationCredentialRequest {
        storage_tenant: StorageTenantId::system(),
        app_id: "system-worker".into(),
        client_id: "system-worker-client".into(),
        client_secret: SECRET.into(),
    };

    assert!(matches!(
        store.create_application(candidate, AuthzRevision(2)),
        Err(CredentialRepositoryError::Authorization(
            AuthzStoreError::RevisionConflict { .. }
        ))
    ));
    assert!(store.credential("system-worker-client").unwrap().is_none());
}

#[test]
fn every_typed_role_maps_to_one_declared_direct_system_relation() {
    let acme = tenant("acme");
    let cases = [
        (
            ApplicationRoleTarget::Tenant(TenantApplicationRole::Owner),
            "owner",
        ),
        (
            ApplicationRoleTarget::Tenant(TenantApplicationRole::Admin),
            "admin",
        ),
        (
            ApplicationRoleTarget::Tenant(TenantApplicationRole::Reader),
            "reader",
        ),
        (
            ApplicationRoleTarget::Tenant(TenantApplicationRole::ManageTenant),
            "manage_tenant_grant",
        ),
        (
            ApplicationRoleTarget::Tenant(TenantApplicationRole::ReadTenant),
            "read_tenant_grant",
        ),
        (
            ApplicationRoleTarget::Tenant(TenantApplicationRole::ManageBuckets),
            "manage_buckets_grant",
        ),
        (
            ApplicationRoleTarget::Tenant(TenantApplicationRole::ManageAuthz),
            "manage_authz_grant",
        ),
    ];
    for (target, expected) in cases {
        assert_eq!(role_tuple_parts(&acme, &target).unwrap().1, expected);
    }
    let bucket_roles = [
        (BucketApplicationRole::Owner, "owner"),
        (BucketApplicationRole::Admin, "admin"),
        (BucketApplicationRole::Reader, "reader"),
        (BucketApplicationRole::Writer, "writer"),
        (BucketApplicationRole::GetObject, "get_object_grant"),
        (BucketApplicationRole::PutObject, "put_object_grant"),
        (BucketApplicationRole::DeleteObject, "delete_object_grant"),
        (BucketApplicationRole::ManagePolicy, "manage_policy_grant"),
    ];
    for (role, expected) in bucket_roles {
        assert_eq!(
            role_tuple_parts(
                &acme,
                &ApplicationRoleTarget::Bucket {
                    bucket: "objects".into(),
                    role,
                },
            )
            .unwrap()
            .1,
            expected
        );
    }
    assert_eq!(
        role_tuple_parts(
            &StorageTenantId::system(),
            &ApplicationRoleTarget::System(SystemApplicationRole::Admin),
        )
        .unwrap()
        .1,
        "admin"
    );
}

#[tokio::test]
async fn plaintext_secret_is_not_persisted_in_credential_values() {
    let (_directory, store) = store().await;
    store
        .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
        .unwrap();

    let secret = SECRET.as_bytes();
    for name in crate::store::COLUMN_FAMILIES {
        let column_family = store.db.cf_handle(name).unwrap();
        for item in store.db.iterator_cf(column_family, IteratorMode::Start) {
            let (_key, value) = item.unwrap();
            assert!(!value.windows(secret.len()).any(|window| window == secret));
        }
    }
}

#[tokio::test]
async fn credential_record_persists_approved_argon2id_identity_and_costs() {
    let (_directory, store) = store().await;
    store
        .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
        .unwrap();

    let stored = store
        .credentials()
        .read_stored_credential("bootstrap-client")
        .unwrap()
        .unwrap();
    assert_eq!(stored.format_version, CREDENTIAL_FORMAT_VERSION);
    let StoredCredentialVerifier::Argon2id {
        version,
        memory_cost_kib,
        time_cost,
        parallelism,
        output_length,
        salt: _,
        output: _,
    } = &stored.verifier;
    assert_eq!(*version, u32::from(Version::V0x13));
    assert_eq!(*memory_cost_kib, Params::DEFAULT_M_COST);
    assert_eq!(*time_cost, Params::DEFAULT_T_COST);
    assert_eq!(*parallelism, Params::DEFAULT_P_COST);
    assert_eq!(*output_length, Params::DEFAULT_OUTPUT_LEN as u32);
    assert!(credential_matches(&stored.verifier, SECRET.as_bytes()).unwrap());
    assert!(
        !credential_matches(&stored.verifier, b"wrong-secret-with-at-least-32-bytes",).unwrap()
    );
}

#[test]
fn credential_verifier_uses_fresh_salts_and_rejects_unapproved_costs() {
    let first = new_credential_verifier(SECRET.as_bytes()).unwrap();
    let second = new_credential_verifier(SECRET.as_bytes()).unwrap();
    let (
        StoredCredentialVerifier::Argon2id {
            salt: first_salt, ..
        },
        StoredCredentialVerifier::Argon2id {
            salt: second_salt, ..
        },
    ) = (&first, &second);
    assert_ne!(first_salt, second_salt);

    let mut unsupported = first.clone();
    let StoredCredentialVerifier::Argon2id {
        memory_cost_kib, ..
    } = &mut unsupported;
    *memory_cost_kib = memory_cost_kib.saturating_add(1);
    assert!(matches!(
        validate_stored_credential_verifier(&unsupported),
        Err(CredentialRepositoryError::Storage(_))
    ));
}

#[tokio::test]
async fn invalid_input_leaves_every_bootstrap_state_absent() {
    let (_directory, store) = store().await;
    let mut invalid = request("bootstrap-app", "bootstrap-client");
    invalid.client_secret = "too-short".into();

    assert!(matches!(
        store.bootstrap_system(invalid),
        Err(SystemBootstrapError::InvalidInput(_))
    ));
    assert_eq!(
        store.system_bootstrap_state().unwrap(),
        SystemBootstrapState::Missing
    );
    assert!(store.credential("bootstrap-client").unwrap().is_none());
    assert_eq!(
        store
            .authz()
            .tenant_revision(&StorageTenantId::system())
            .unwrap(),
        AuthzRevision::ZERO
    );
    assert!(
        store
            .authz()
            .get_binding(&AuthzScope::system())
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn bootstrap_marker_survives_reopen() {
    let (directory, store) = store().await;
    store
        .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
        .unwrap();
    drop(store);

    let reopened = Store::open(StoreOptions::new(directory.path(), 7))
        .await
        .unwrap();
    assert_eq!(
        reopened.system_bootstrap_state().unwrap(),
        SystemBootstrapState::Complete {
            version: SYSTEM_BOOTSTRAP_VERSION,
        }
    );
    assert!(matches!(
        reopened.bootstrap_system(request("other-app", "other-client")),
        Err(SystemBootstrapError::AlreadyBootstrapped)
    ));
}

#[test]
fn schema_uses_the_protected_system_realm_id() {
    assert!(RealmId::system().is_system());
    assert!(
        system_schema()
            .namespaces
            .iter()
            .any(|namespace| namespace.name == SYSTEM_NAMESPACE)
    );
}
