use anvil_authz::{
    AllowedSubject, NamespaceDefinition, ObjectRef, RelationDefinition, RewriteRule,
};
use tempfile::TempDir;

use super::*;
use crate::StoreOptions;

fn tenant(value: &str) -> StorageTenantId {
    StorageTenantId::parse(value).unwrap()
}

fn scope(value: &str, realm: &str) -> AuthzScope {
    AuthzScope::new(tenant(value), RealmId::parse(realm).unwrap()).unwrap()
}

fn principal(value: &str) -> ObjectRef {
    ObjectRef::opaque("app", value).unwrap()
}

fn resource(value: &str) -> ObjectRef {
    ObjectRef::opaque("document", value).unwrap()
}

fn viewer_tuple(document: &str, user: &str) -> Tuple {
    Tuple::new(resource(document), "viewer", principal(user))
}

fn document_schema(reverse: bool) -> Schema {
    let mut relations = vec![
        RelationDefinition::direct("owner", [AllowedSubject::any_object("app")]),
        RelationDefinition::direct("viewer", [AllowedSubject::any_object("app")]),
        RelationDefinition::permission(
            "view",
            [
                RewriteRule::Inherit {
                    relation: "owner".into(),
                },
                RewriteRule::Inherit {
                    relation: "viewer".into(),
                },
            ],
        ),
    ];
    if reverse {
        relations.reverse();
        if let RelationKind::Permission { rules } = &mut relations[0].kind {
            rules.reverse();
        }
    }
    Schema::new([NamespaceDefinition::new("document", relations)])
}

fn system_schema() -> Schema {
    Schema::new([
        NamespaceDefinition::new(
            "storage_tenant",
            [RelationDefinition::direct(
                "owner",
                [AllowedSubject::any_object("app")],
            )],
        ),
        NamespaceDefinition::new(
            "authz_realm",
            [
                RelationDefinition::direct(
                    "parent_tenant",
                    [AllowedSubject::any_object("storage_tenant")],
                ),
                RelationDefinition::direct("owner", [AllowedSubject::any_object("app")]),
            ],
        ),
    ])
}

async fn store() -> (TempDir, Store) {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(directory.path(), 7))
        .await
        .unwrap();
    (directory, store)
}

fn publish(
    repository: &AuthzRepository,
    storage_tenant: StorageTenantId,
    id: &str,
    schema: Schema,
    expected_revision: AuthzRevision,
) -> PublishedSchema {
    repository
        .publish_schema(PublishSchemaRequest {
            storage_tenant,
            schema_id: SchemaId::parse(id).unwrap(),
            schema,
            expected_revision: Some(expected_revision),
        })
        .unwrap()
}

fn bind(
    repository: &AuthzRepository,
    scope: AuthzScope,
    schema_ref: SchemaRef,
    expected_revision: AuthzRevision,
) -> BoundRealm {
    repository
        .bind_schema(BindSchemaRequest {
            scope,
            schema_ref,
            expected_generation: Some(0),
            expected_revision: Some(expected_revision),
        })
        .unwrap()
}

#[tokio::test]
async fn schemas_are_canonical_immutable_and_same_content_replays() {
    let (_directory, store) = store().await;
    let repository = store.authz();
    let acme = tenant("acme");

    let first = publish(
        &repository,
        acme.clone(),
        "documents",
        document_schema(false),
        AuthzRevision::ZERO,
    );
    assert_eq!(first.schema_ref.schema_revision, 1);
    assert_eq!(first.authz_revision, AuthzRevision(1));
    assert!(!first.replayed);

    let replay = repository
        .publish_schema(PublishSchemaRequest {
            storage_tenant: acme.clone(),
            schema_id: SchemaId::parse("documents").unwrap(),
            schema: document_schema(true),
            // Replays are recognized before a stale revision CAS.
            expected_revision: Some(AuthzRevision::ZERO),
        })
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.schema_ref, first.schema_ref);
    assert_eq!(replay.authz_revision, AuthzRevision(1));
    assert_eq!(repository.tenant_revision(&acme).unwrap(), AuthzRevision(1));
    assert_eq!(
        repository
            .get_schema(&acme, &first.schema_ref)
            .unwrap()
            .unwrap(),
        canonical_schema(document_schema(false), AuthorizationLimits::default()).unwrap()
    );

    let mut changed = document_schema(false);
    changed.namespaces[0]
        .relations
        .push(RelationDefinition::direct(
            "editor",
            [AllowedSubject::any_object("app")],
        ));
    let second = publish(
        &repository,
        acme.clone(),
        "documents",
        changed,
        AuthzRevision(1),
    );
    assert_eq!(second.schema_ref.schema_revision, 2);
    assert_ne!(
        second.schema_ref.schema_digest,
        first.schema_ref.schema_digest
    );
    assert_eq!(repository.tenant_revision(&acme).unwrap(), AuthzRevision(2));
}

#[tokio::test]
async fn tuple_batches_are_atomic_replayable_and_principal_scoped() {
    let (directory, store) = store().await;
    let repository = store.authz();
    let acme = tenant("acme");
    let realm = scope("acme", "default");
    let published = publish(
        &repository,
        acme.clone(),
        "documents",
        document_schema(false),
        AuthzRevision::ZERO,
    );
    let bound = bind(
        &repository,
        realm.clone(),
        published.schema_ref,
        AuthzRevision(1),
    );
    assert_eq!(bound.binding.generation, 1);
    assert_eq!(repository.get_binding(&realm).unwrap(), Some(bound.binding));

    let invalid = repository.mutate_tuples(TupleBatchRequest {
        scope: realm.clone(),
        principal: principal("writer"),
        expected_revision: Some(AuthzRevision(2)),
        expected_binding_generation: 1,
        operation_id: Some("invalid".into()),
        mutations: vec![
            TupleMutation {
                kind: TupleMutationKind::Add,
                tuple: viewer_tuple("one", "alice"),
            },
            TupleMutation {
                kind: TupleMutationKind::Add,
                tuple: Tuple::new(resource("one"), "undeclared", principal("bob")),
            },
        ],
    });
    assert!(matches!(invalid, Err(AuthzStoreError::Authorization(_))));
    assert_eq!(repository.tenant_revision(&acme).unwrap(), AuthzRevision(2));
    assert!(
        repository
            .realm_snapshot(&realm, AuthzConsistency::Latest)
            .unwrap()
            .tuples
            .is_empty()
    );

    let original_request = TupleBatchRequest {
        scope: realm.clone(),
        principal: principal("writer-a"),
        expected_revision: Some(AuthzRevision(2)),
        expected_binding_generation: 1,
        operation_id: Some("operation-1".into()),
        mutations: vec![TupleMutation {
            kind: TupleMutationKind::Add,
            tuple: viewer_tuple("one", "alice"),
        }],
    };
    let applied = repository.mutate_tuples(original_request.clone()).unwrap();
    assert_eq!(applied.authz_revision, AuthzRevision(3));
    assert!(!applied.replayed);

    // Another authenticated principal may use the same operation spelling.
    let other = repository
        .mutate_tuples(TupleBatchRequest {
            scope: realm.clone(),
            principal: principal("writer-b"),
            expected_revision: Some(AuthzRevision(3)),
            expected_binding_generation: 1,
            operation_id: Some("operation-1".into()),
            mutations: vec![TupleMutation {
                kind: TupleMutationKind::Add,
                tuple: viewer_tuple("two", "bob"),
            }],
        })
        .unwrap();
    assert_eq!(other.authz_revision, AuthzRevision(4));

    // A lost response replays before the now-stale revision precondition.
    let replay = repository.mutate_tuples(original_request.clone()).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.authz_revision, AuthzRevision(3));
    assert_eq!(repository.tenant_revision(&acme).unwrap(), AuthzRevision(4));

    let mut mismatch = original_request;
    mismatch.mutations[0].tuple = viewer_tuple("changed", "alice");
    assert!(matches!(
        repository.mutate_tuples(mismatch),
        Err(AuthzStoreError::OperationMismatch)
    ));

    let check = AuthorizationCheck::new(principal("alice"), resource("one"), "view");
    let result = repository
        .batch_check(
            &realm,
            AuthzConsistency::AtLeast(AuthzRevision(4)),
            &[check],
        )
        .unwrap();
    assert_eq!(result.revision, AuthzRevision(4));
    assert_eq!(result.allowed, vec![true]);
    assert_eq!(
        repository
            .realm_snapshot(&realm, AuthzConsistency::Exact(AuthzRevision(4)))
            .unwrap()
            .revision,
        AuthzRevision(4)
    );
    assert!(matches!(
        repository.realm_snapshot(&realm, AuthzConsistency::Exact(AuthzRevision(3))),
        Err(AuthzStoreError::RevisionExpired { .. })
    ));
    assert!(matches!(
        repository.realm_snapshot(&realm, AuthzConsistency::AtLeast(AuthzRevision(5))),
        Err(AuthzStoreError::RevisionNotAvailable { .. })
    ));

    drop(repository);
    drop(store);
    let reopened = Store::open(StoreOptions::new(directory.path(), 7))
        .await
        .unwrap();
    let durable = reopened
        .authz()
        .realm_snapshot(&realm, AuthzConsistency::Latest)
        .unwrap();
    assert_eq!(durable.revision, AuthzRevision(4));
    assert_eq!(durable.tuples.len(), 2);
}

#[tokio::test]
async fn unexpired_tuple_receipts_are_never_evicted_to_admit_new_work() {
    let (_directory, store) = store().await;
    let mut limits = AuthzStoreLimits::default();
    limits.max_receipt_entries = 1;
    let repository = store.authz_with_limits(limits);
    let acme = tenant("acme");
    let realm = scope("acme", "default");
    let published = publish(
        &repository,
        acme.clone(),
        "documents",
        document_schema(false),
        AuthzRevision::ZERO,
    );
    bind(
        &repository,
        realm.clone(),
        published.schema_ref,
        AuthzRevision(1),
    );
    let first_request = TupleBatchRequest {
        scope: realm.clone(),
        principal: principal("writer"),
        expected_revision: Some(AuthzRevision(2)),
        expected_binding_generation: 1,
        operation_id: Some("first".into()),
        mutations: vec![TupleMutation {
            kind: TupleMutationKind::Add,
            tuple: viewer_tuple("one", "alice"),
        }],
    };
    let first = repository.mutate_tuples(first_request.clone()).unwrap();
    assert!(first.replay_guarantee_expires_at_unix_millis > current_unix_millis().unwrap());

    let second = repository.mutate_tuples(TupleBatchRequest {
        scope: realm.clone(),
        principal: principal("writer"),
        expected_revision: Some(AuthzRevision(3)),
        expected_binding_generation: 1,
        operation_id: Some("second".into()),
        mutations: vec![TupleMutation {
            kind: TupleMutationKind::Add,
            tuple: viewer_tuple("two", "bob"),
        }],
    });
    assert!(matches!(second, Err(AuthzStoreError::ReceiptCapacity)));
    assert_eq!(repository.tenant_revision(&acme).unwrap(), AuthzRevision(3));
    assert!(repository.mutate_tuples(first_request).unwrap().replayed);
}

#[tokio::test]
async fn an_expired_tuple_operation_id_may_be_used_as_new_and_is_rebounded() {
    let (_directory, store) = store().await;
    let repository = store.authz();
    let acme = tenant("acme");
    let realm = scope("acme", "default");
    let published = publish(
        &repository,
        acme,
        "documents",
        document_schema(false),
        AuthzRevision::ZERO,
    );
    bind(
        &repository,
        realm.clone(),
        published.schema_ref,
        AuthzRevision(1),
    );
    repository
        .mutate_tuples(TupleBatchRequest {
            scope: realm.clone(),
            principal: principal("writer"),
            expected_revision: Some(AuthzRevision(2)),
            expected_binding_generation: 1,
            operation_id: Some("reusable".into()),
            mutations: vec![TupleMutation {
                kind: TupleMutationKind::Add,
                tuple: viewer_tuple("one", "alice"),
            }],
        })
        .unwrap();

    let key = receipt_key(&tenant("acme"), &principal("writer"), "reusable").unwrap();
    let mut expired = repository
        .read_json::<StoredTupleReceipt>(CF_AUTHZ_RECEIPTS, &key)
        .unwrap()
        .unwrap();
    expired.created_at_unix_millis = 1;
    expired.expires_at_unix_millis = 2;
    expired.receipt.replay_guarantee_expires_at_unix_millis = 2;
    repository
        .db
        .put_cf(
            repository.cf(CF_AUTHZ_RECEIPTS).unwrap(),
            &key,
            encode_json(&expired).unwrap(),
        )
        .unwrap();

    let reused = repository
        .mutate_tuples(TupleBatchRequest {
            scope: realm,
            principal: principal("writer"),
            expected_revision: Some(AuthzRevision(3)),
            expected_binding_generation: 1,
            operation_id: Some("reusable".into()),
            mutations: vec![TupleMutation {
                kind: TupleMutationKind::Add,
                tuple: viewer_tuple("two", "bob"),
            }],
        })
        .unwrap();
    assert!(!reused.replayed);
    assert_eq!(reused.authz_revision, AuthzRevision(4));
    assert!(reused.replay_guarantee_expires_at_unix_millis > current_unix_millis().unwrap());
}

#[tokio::test]
async fn lost_tuple_response_replays_after_a_compatible_rebind() {
    let (_directory, store) = store().await;
    let repository = store.authz();
    let acme = tenant("acme");
    let realm = scope("acme", "default");
    let first = publish(
        &repository,
        acme.clone(),
        "documents",
        document_schema(false),
        AuthzRevision::ZERO,
    );
    bind(
        &repository,
        realm.clone(),
        first.schema_ref,
        AuthzRevision(1),
    );
    let original = TupleBatchRequest {
        scope: realm.clone(),
        principal: principal("writer"),
        expected_revision: Some(AuthzRevision(2)),
        expected_binding_generation: 1,
        operation_id: Some("lost-response".into()),
        mutations: vec![TupleMutation {
            kind: TupleMutationKind::Add,
            tuple: viewer_tuple("one", "alice"),
        }],
    };
    let applied = repository.mutate_tuples(original.clone()).unwrap();
    assert_eq!(applied.authz_revision, AuthzRevision(3));

    let mut compatible = document_schema(false);
    compatible.namespaces[0]
        .relations
        .push(RelationDefinition::direct(
            "editor",
            [AllowedSubject::any_object("app")],
        ));
    let second = publish(
        &repository,
        acme.clone(),
        "documents",
        compatible,
        AuthzRevision(3),
    );
    let rebound = repository
        .bind_schema(BindSchemaRequest {
            scope: realm,
            schema_ref: second.schema_ref,
            expected_generation: Some(1),
            expected_revision: Some(AuthzRevision(4)),
        })
        .unwrap();
    assert_eq!(rebound.binding.generation, 2);

    let mut retry = original;
    retry.expected_binding_generation = 2;
    let replay = repository.mutate_tuples(retry).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.authz_revision, AuthzRevision(3));
    assert_eq!(repository.tenant_revision(&acme).unwrap(), AuthzRevision(5));
}

#[tokio::test]
async fn rebinding_requires_generation_cas_and_exact_replay_is_idempotent() {
    let (_directory, store) = store().await;
    let repository = store.authz();
    let acme = tenant("acme");
    let realm = scope("acme", "default");
    let first = publish(
        &repository,
        acme.clone(),
        "documents",
        document_schema(false),
        AuthzRevision::ZERO,
    );
    let first_binding = bind(
        &repository,
        realm.clone(),
        first.schema_ref.clone(),
        AuthzRevision(1),
    );

    // A lost first-bind response is a harmless exact-reference replay.
    let replay = repository
        .bind_schema(BindSchemaRequest {
            scope: realm.clone(),
            schema_ref: first.schema_ref,
            expected_generation: Some(0),
            expected_revision: Some(AuthzRevision(1)),
        })
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.binding, first_binding.binding);

    let mut changed_schema = document_schema(false);
    changed_schema.namespaces[0]
        .relations
        .push(RelationDefinition::direct(
            "editor",
            [AllowedSubject::any_object("app")],
        ));
    let changed = publish(
        &repository,
        acme.clone(),
        "documents",
        changed_schema,
        AuthzRevision(2),
    );
    assert!(matches!(
        repository.bind_schema(BindSchemaRequest {
            scope: realm.clone(),
            schema_ref: changed.schema_ref.clone(),
            expected_generation: Some(0),
            expected_revision: Some(AuthzRevision(3)),
        }),
        Err(AuthzStoreError::BindingGenerationConflict { .. })
    ));
    let rebound = repository
        .bind_schema(BindSchemaRequest {
            scope: realm.clone(),
            schema_ref: changed.schema_ref,
            expected_generation: Some(1),
            expected_revision: Some(AuthzRevision(3)),
        })
        .unwrap();
    assert_eq!(rebound.binding.generation, 2);
    assert_eq!(rebound.binding.authz_revision, AuthzRevision(4));
    assert_eq!(
        repository.get_binding(&realm).unwrap(),
        Some(rebound.binding)
    );
}

#[tokio::test]
async fn rebind_rejects_a_schema_that_invalidates_current_tuples() {
    let (_directory, store) = store().await;
    let repository = store.authz();
    let acme = tenant("acme");
    let realm = scope("acme", "default");
    let first = publish(
        &repository,
        acme.clone(),
        "documents",
        document_schema(false),
        AuthzRevision::ZERO,
    );
    bind(
        &repository,
        realm.clone(),
        first.schema_ref,
        AuthzRevision(1),
    );
    repository
        .mutate_tuples(TupleBatchRequest {
            scope: realm.clone(),
            principal: principal("writer"),
            expected_revision: Some(AuthzRevision(2)),
            expected_binding_generation: 1,
            operation_id: Some("grant-viewer".into()),
            mutations: vec![TupleMutation {
                kind: TupleMutationKind::Add,
                tuple: viewer_tuple("one", "alice"),
            }],
        })
        .unwrap();

    let incompatible_schema = Schema::new([NamespaceDefinition::new(
        "document",
        [RelationDefinition::direct(
            "owner",
            [AllowedSubject::any_object("app")],
        )],
    )]);
    let incompatible = publish(
        &repository,
        acme.clone(),
        "documents",
        incompatible_schema,
        AuthzRevision(3),
    );
    assert!(matches!(
        repository.bind_schema(BindSchemaRequest {
            scope: realm.clone(),
            schema_ref: incompatible.schema_ref,
            expected_generation: Some(1),
            expected_revision: Some(AuthzRevision(4)),
        }),
        Err(AuthzStoreError::Authorization(_))
    ));
    assert_eq!(
        repository.get_binding(&realm).unwrap().unwrap().generation,
        1
    );
    assert_eq!(repository.tenant_revision(&acme).unwrap(), AuthzRevision(4));
}

#[tokio::test]
async fn first_binding_and_protected_owner_tuples_commit_all_or_nothing() {
    let (_directory, store) = store().await;
    let repository = store.authz();

    let system_scope = AuthzScope::system();
    let system_publication = publish(
        &repository,
        StorageTenantId::system(),
        "anvil-system",
        system_schema(),
        AuthzRevision::ZERO,
    );
    bind(
        &repository,
        system_scope.clone(),
        system_publication.schema_ref,
        AuthzRevision(1),
    );

    let acme = tenant("acme");
    let custom_scope = scope("acme", "relationships");
    let custom_publication = publish(
        &repository,
        acme.clone(),
        "documents",
        document_schema(false),
        AuthzRevision::ZERO,
    );
    let realm_resource = ObjectRef::opaque("authz_realm", "acme/relationships").unwrap();
    let parent = Tuple::new(
        realm_resource.clone(),
        "parent_tenant",
        ObjectRef::opaque("storage_tenant", "acme").unwrap(),
    );
    let owner = Tuple::new(realm_resource.clone(), "owner", principal("alice"));
    let binding_request = BindSchemaRequest {
        scope: custom_scope.clone(),
        schema_ref: custom_publication.schema_ref,
        expected_generation: Some(0),
        expected_revision: Some(AuthzRevision(1)),
    };

    let invalid = repository.bind_schema_with_protected_owner(
        binding_request.clone(),
        ProtectedRealmOwnership {
            principal: principal("alice"),
            expected_revision: AuthzRevision(999),
            expected_binding_generation: 1,
        },
    );
    assert!(matches!(
        invalid,
        Err(AuthzStoreError::RevisionConflict { .. })
    ));
    assert_eq!(repository.get_binding(&custom_scope).unwrap(), None);
    assert_eq!(repository.tenant_revision(&acme).unwrap(), AuthzRevision(1));
    assert_eq!(
        repository
            .tenant_revision(&StorageTenantId::system())
            .unwrap(),
        AuthzRevision(2)
    );

    let created = repository
        .bind_schema_with_protected_owner(
            binding_request.clone(),
            ProtectedRealmOwnership {
                principal: principal("alice"),
                expected_revision: AuthzRevision(2),
                expected_binding_generation: 1,
            },
        )
        .unwrap();
    assert_eq!(created.realm.binding.generation, 1);
    assert_eq!(created.realm.binding.authz_revision, AuthzRevision(2));
    assert_eq!(created.system_grant.authz_revision, AuthzRevision(3));
    assert_eq!(
        repository
            .realm_snapshot(&system_scope, AuthzConsistency::Latest)
            .unwrap()
            .tuples,
        vec![owner.clone(), parent.clone()]
    );

    assert!(matches!(
        repository.bind_schema_with_protected_owner(
            binding_request,
            ProtectedRealmOwnership {
                principal: principal("mallory"),
                expected_revision: AuthzRevision(3),
                expected_binding_generation: 1,
            },
        ),
        Err(AuthzStoreError::BindingGenerationConflict { .. })
    ));
    assert_eq!(
        repository
            .realm_snapshot(&system_scope, AuthzConsistency::Latest)
            .unwrap()
            .tuples,
        vec![owner, parent]
    );
    assert_eq!(
        repository
            .tenant_revision(&StorageTenantId::system())
            .unwrap(),
        AuthzRevision(3)
    );
}

#[test]
fn ids_and_current_only_exact_consistency_are_deliberately_narrow() {
    for invalid in ["", ".", "..", "a/b", "a:b", "a#b", "a\n"] {
        assert!(StorageTenantId::parse(invalid).is_err());
        assert!(SchemaId::parse(invalid).is_err());
    }
    let latest = AuthzConsistency::Latest;
    let at_least = AuthzConsistency::AtLeast(AuthzRevision(9));
    let exact = AuthzConsistency::Exact(AuthzRevision(9));
    assert_ne!(latest, at_least);
    assert_ne!(at_least, exact);
}

#[test]
fn external_storage_tenants_are_exact_lowercase_ascii_dns_labels() {
    for valid in ["a", "0", "acme", "acme-2"] {
        let tenant = StorageTenantId::parse(valid).unwrap();
        assert_eq!(tenant.as_str(), valid);
    }
    let boundary = "a".repeat(MAX_EXTERNAL_STORAGE_TENANT_BYTES);
    let parsed_boundary = StorageTenantId::parse(boundary.as_str()).unwrap();
    assert_eq!(parsed_boundary.as_str(), boundary.as_str());

    for invalid in [
        "",
        "Acme",
        "ACME",
        "-acme",
        "acme-",
        "acme_example",
        "acme.example",
        "acmé",
    ] {
        assert!(
            StorageTenantId::parse(invalid).is_err(),
            "{invalid:?} must be rejected rather than normalized"
        );
    }
    assert!(StorageTenantId::parse("a".repeat(MAX_EXTERNAL_STORAGE_TENANT_BYTES + 1)).is_err());
}

#[test]
fn protected_system_storage_tenant_remains_representable_but_reserved() {
    let parsed = StorageTenantId::parse(SYSTEM_STORAGE_TENANT_ID).unwrap();
    assert_eq!(parsed, StorageTenantId::system());
    assert!(parsed.is_system());

    let encoded = serde_json::to_vec(&parsed).unwrap();
    assert_eq!(
        serde_json::from_slice::<StorageTenantId>(&encoded).unwrap(),
        parsed
    );
}
