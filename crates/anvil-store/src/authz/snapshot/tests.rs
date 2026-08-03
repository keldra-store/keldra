use anvil_authz::{
    AllowedSubject, Authorization, AuthorizationLimits, MAX_NAMESPACE_BYTES, NamespaceDefinition,
    ObjectRef, RealmId, RelationDefinition, RewriteRule, Schema, Tuple,
};

use super::*;
use crate::{
    AuthzConsistency, AuthzRealmMutationContext, BindSchemaRequest, PlacementLogId,
    PublishSchemaRequest, SchemaId, SourceId, Store, StoreOptions, TupleBatchRequest,
    TupleMutation, TupleMutationKind,
};

fn tenant() -> StorageTenantId {
    StorageTenantId::parse("acme").unwrap()
}

fn scope(realm: &str) -> AuthzScope {
    AuthzScope::new(tenant(), RealmId::parse(realm).unwrap()).unwrap()
}

fn principal(name: &str) -> ObjectRef {
    ObjectRef::opaque("app", name).unwrap()
}

fn tuple(document: &str, user: &str) -> Tuple {
    Tuple::new(
        ObjectRef::opaque("document", document).unwrap(),
        "viewer",
        principal(user),
    )
}

fn schema() -> Schema {
    Schema::new([NamespaceDefinition::new(
        "document",
        [
            RelationDefinition::direct("viewer", [AllowedSubject::any_object("app")]),
            RelationDefinition::permission(
                "view",
                [RewriteRule::Inherit {
                    relation: "viewer".into(),
                }],
            ),
        ],
    )])
}

fn context(command: &str, position: u64) -> AuthzRealmMutationContext {
    AuthzRealmMutationContext {
        command_id: command.into(),
        active_placement_log_id: PlacementLogId { term: 4, index: 7 },
        serving_fence_term: 4,
        source_id: SourceId {
            node_id: 1,
            source_epoch: [1; 32],
        },
        source_journal_position: position,
    }
}

fn tuple_request(
    realm: &str,
    operation: &str,
    expected_revision: u64,
    values: &[(&str, &str)],
) -> TupleBatchRequest {
    TupleBatchRequest {
        scope: scope(realm),
        principal: principal("writer"),
        expected_revision: Some(AuthzRevision(expected_revision)),
        expected_binding_generation: 1,
        operation_id: Some(operation.into()),
        mutations: values
            .iter()
            .map(|(document, user)| TupleMutation {
                kind: TupleMutationKind::Add,
                tuple: tuple(document, user),
            })
            .collect(),
    }
}

fn populate(repository: &AuthzRepository) {
    let published = repository
        .publish_schema(PublishSchemaRequest {
            storage_tenant: tenant(),
            schema_id: SchemaId::parse("documents").unwrap(),
            schema: schema(),
            expected_revision: Some(AuthzRevision::ZERO),
        })
        .unwrap();
    repository
        .coordinate_bind_schema_mutation(
            BindSchemaRequest {
                scope: scope("documents"),
                schema_ref: published.schema_ref.clone(),
                expected_generation: Some(0),
                expected_revision: Some(AuthzRevision(1)),
            },
            context("bind-documents", 1),
        )
        .unwrap();
    repository
        .coordinate_tuple_mutation(
            tuple_request(
                "documents",
                "grant-documents",
                2,
                &[("one", "alice"), ("two", "bob")],
            ),
            context("grant-documents", 2),
        )
        .unwrap();
    repository
        .coordinate_bind_schema_mutation(
            BindSchemaRequest {
                scope: scope("reports"),
                schema_ref: published.schema_ref,
                expected_generation: Some(0),
                expected_revision: Some(AuthzRevision(3)),
            },
            context("bind-reports", 3),
        )
        .unwrap();
    repository
        .coordinate_tuple_mutation(
            tuple_request("reports", "grant-reports", 4, &[("three", "carol")]),
            context("grant-reports", 4),
        )
        .unwrap();
}

fn export_all(repository: &AuthzRepository, page_size: u32) -> Vec<AuthzRealmAggregate> {
    let mut cursor = None;
    let mut aggregates = Vec::new();
    loop {
        let page = repository
            .export_authz_realm_keys(cursor.as_ref(), page_size, MAX_AUTHZ_REALM_EXPORT_BYTES)
            .unwrap();
        assert!(!page.scopes.is_empty() || page.next_cursor.is_none());
        aggregates.extend(
            page.scopes
                .iter()
                .map(|scope| repository.export_authz_realm(scope).unwrap().unwrap()),
        );
        let Some(next) = page.next_cursor else {
            return aggregates;
        };
        cursor = Some(next);
    }
}

fn realm<'a>(aggregates: &'a [AuthzRealmAggregate], name: &str) -> &'a AuthzRealmAggregate {
    aggregates
        .iter()
        .find(|aggregate| aggregate.scope == scope(name))
        .unwrap()
}

#[test]
fn maximum_valid_tuple_set_requires_streamed_transfer() {
    const MAX_OPAQUE_ID_BYTES: usize = 4_096;
    let object_namespace = "o".repeat(MAX_NAMESPACE_BYTES);
    let subject_namespace = "s".repeat(MAX_NAMESPACE_BYTES);
    let relation = "r".repeat(MAX_NAMESPACE_BYTES);
    let subject = ObjectRef::opaque(&subject_namespace, "u".repeat(MAX_OPAQUE_ID_BYTES)).unwrap();
    let make_tuple = |index: u64| {
        let object_id = format!("{index:016x}{}", "x".repeat(MAX_OPAQUE_ID_BYTES - 16));
        Tuple::new(
            ObjectRef::opaque(&object_namespace, object_id).unwrap(),
            &relation,
            subject.clone(),
        )
    };
    let first = make_tuple(0);
    let last = make_tuple(AuthorizationLimits::default().max_tuples as u64 - 1);
    let schema = Schema::new([NamespaceDefinition::new(
        &object_namespace,
        [RelationDefinition::direct(
            &relation,
            [AllowedSubject::any_object(&subject_namespace)],
        )],
    )]);
    Authorization::new(
        RealmId::parse("sizing").unwrap(),
        schema,
        [first.clone(), last.clone()],
        AuthorizationLimits::default(),
    )
    .unwrap();

    let tuple_bytes = serde_json::to_vec(&first).unwrap().len() as u64;
    assert_eq!(serde_json::to_vec(&last).unwrap().len() as u64, tuple_bytes);
    let tuple_count = AuthorizationLimits::default().max_tuples as u64;
    let array_bytes = 2 + tuple_count * tuple_bytes + (tuple_count - 1);
    eprintln!("max tuple JSON bytes={tuple_bytes}, 65,536-tuple array bytes={array_bytes}");
    assert!(array_bytes > MAX_AUTHZ_REALM_EXPORT_BYTES);
}

#[tokio::test]
async fn complete_realms_page_install_replay_and_survive_restart() {
    let temporary = tempfile::tempdir().unwrap();
    let source = Store::open(StoreOptions::new(temporary.path().join("source"), 1))
        .await
        .unwrap();
    populate(&source.authz());
    let source_repository = source.authz();
    let aggregates = export_all(&source_repository, 1);
    assert_eq!(aggregates.len(), 2);
    assert_eq!(realm(&aggregates, "documents").tuples.len(), 2);
    assert_eq!(realm(&aggregates, "documents").receipts.len(), 1);
    assert_eq!(realm(&aggregates, "reports").receipts.len(), 1);
    assert_eq!(
        source_repository
            .export_authz_realm(&scope("documents"))
            .unwrap(),
        Some(realm(&aggregates, "documents").clone())
    );

    let target_path = temporary.path().join("target");
    let target = Store::open(StoreOptions::new(&target_path, 2))
        .await
        .unwrap();
    let target_repository = target.authz();
    for aggregate in &aggregates {
        let mut bytes = Vec::new();
        let manifest = source_repository
            .export_authz_realm_stream(&aggregate.scope, &mut bytes)
            .unwrap()
            .unwrap();
        assert_eq!(manifest.encoded_bytes, bytes.len() as u64);
        let applied = target_repository
            .install_quorum_reconciled_authz_realm_stream(&manifest, std::io::Cursor::new(bytes))
            .unwrap();
        assert!(!applied.replayed);
        assert_eq!(applied.retained_receipts, 1);
    }
    assert_eq!(target.local_watch_status().unwrap().tail, 0);
    assert_eq!(export_all(&target_repository, 1), aggregates);
    for aggregate in &aggregates {
        assert!(
            target_repository
                .install_quorum_reconciled_authz_realm(aggregate)
                .unwrap()
                .replayed
        );
    }
    assert_eq!(
        target_repository
            .realm_snapshot(&scope("documents"), AuthzConsistency::Latest)
            .unwrap(),
        source_repository
            .realm_snapshot(&scope("documents"), AuthzConsistency::Latest)
            .unwrap()
    );

    drop(target_repository);
    drop(target);
    let target = Store::open(StoreOptions::new(&target_path, 2))
        .await
        .unwrap();
    assert_eq!(export_all(&target.authz(), 2), aggregates);
}

fn directory_entries(path: &std::path::Path) -> Vec<std::ffi::OsString> {
    let mut entries = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    entries.sort();
    entries
}

fn hash_transfer(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(AUTHZ_REALM_TRANSFER_HASH_DOMAIN);
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

#[tokio::test]
async fn streamed_transfer_rejects_integrity_failures_and_never_leaves_a_spool() {
    let temporary = tempfile::tempdir().unwrap();
    let source = Store::open(StoreOptions::new(temporary.path().join("source"), 1))
        .await
        .unwrap();
    populate(&source.authz());
    let selected_scope = scope("documents");
    let mut canonical = Vec::new();
    let manifest = source
        .authz()
        .export_authz_realm_stream(&selected_scope, &mut canonical)
        .unwrap()
        .unwrap();

    let target_path = temporary.path().join("target");
    let target = Store::open(StoreOptions::new(&target_path, 2))
        .await
        .unwrap();
    let before = directory_entries(&target_path);

    let mut tampered = canonical.clone();
    let midpoint = tampered.len() / 2;
    tampered[midpoint] ^= 1;
    assert!(matches!(
        target.authz().install_quorum_reconciled_authz_realm_stream(
            &manifest,
            std::io::Cursor::new(tampered),
        ),
        Err(AuthzRealmSnapshotError::TransferIntegrity(_))
    ));
    assert_eq!(directory_entries(&target_path), before);
    assert!(
        target
            .authz()
            .export_authz_realm(&selected_scope)
            .unwrap()
            .is_none()
    );

    assert!(matches!(
        target.authz().install_quorum_reconciled_authz_realm_stream(
            &manifest,
            std::io::Cursor::new(&canonical[..canonical.len() - 1]),
        ),
        Err(AuthzRealmSnapshotError::TransferIntegrity(_))
    ));
    let mut trailing = canonical.clone();
    trailing.push(0);
    assert!(matches!(
        target.authz().install_quorum_reconciled_authz_realm_stream(
            &manifest,
            std::io::Cursor::new(trailing),
        ),
        Err(AuthzRealmSnapshotError::TransferIntegrity(_))
    ));

    let mut noncanonical = vec![b' '];
    noncanonical.extend_from_slice(&canonical);
    let mut noncanonical_manifest = manifest.clone();
    noncanonical_manifest.encoded_bytes = noncanonical.len() as u64;
    noncanonical_manifest.content_hash = hash_transfer(&noncanonical);
    assert!(matches!(
        target.authz().install_quorum_reconciled_authz_realm_stream(
            &noncanonical_manifest,
            std::io::Cursor::new(noncanonical),
        ),
        Err(AuthzRealmSnapshotError::TransferIntegrity(_))
    ));
    assert_eq!(directory_entries(&target_path), before);

    let applied = target
        .authz()
        .install_quorum_reconciled_authz_realm_stream(
            &manifest,
            std::io::Cursor::new(canonical.clone()),
        )
        .unwrap();
    assert!(!applied.replayed);
    assert_eq!(directory_entries(&target_path), before);
    assert!(
        target
            .authz()
            .install_quorum_reconciled_authz_realm_stream(
                &manifest,
                std::io::Cursor::new(canonical),
            )
            .unwrap()
            .replayed
    );
    assert_eq!(directory_entries(&target_path), before);
}

#[tokio::test]
async fn quorum_reconciled_install_replaces_stale_state_and_rejects_tampering() {
    let temporary = tempfile::tempdir().unwrap();
    let source = Store::open(StoreOptions::new(temporary.path().join("source"), 1))
        .await
        .unwrap();
    populate(&source.authz());
    let initial = source
        .authz()
        .export_authz_realm(&scope("documents"))
        .unwrap()
        .unwrap();
    let target = Store::open(StoreOptions::new(temporary.path().join("target"), 2))
        .await
        .unwrap();
    target
        .authz()
        .install_quorum_reconciled_authz_realm(&initial)
        .unwrap();

    source
        .authz()
        .coordinate_tuple_mutation(
            tuple_request("documents", "grant-newer", 5, &[("four", "dave")]),
            context("grant-newer", 5),
        )
        .unwrap();
    let newest_mutation = source
        .authz()
        .coordinate_tuple_mutation(
            tuple_request("documents", "grant-newest", 6, &[("five", "erin")]),
            context("grant-newest", 6),
        )
        .unwrap()
        .mutation
        .unwrap();
    let newer = source
        .authz()
        .export_authz_realm(&scope("documents"))
        .unwrap()
        .unwrap();
    assert!(matches!(
        target
            .authz()
            .apply_authz_realm_mutation_replica(&newest_mutation),
        Err(AuthzStoreError::RealmMutationLineageGap { .. })
    ));
    assert_ne!(
        target
            .authz()
            .export_authz_realm(&scope("documents"))
            .unwrap(),
        Some(newer.clone())
    );
    let applied = target
        .authz()
        .install_quorum_reconciled_authz_realm(&newer)
        .unwrap();
    assert!(!applied.replayed);
    assert_eq!(
        target
            .authz()
            .export_authz_realm(&scope("documents"))
            .unwrap(),
        Some(newer.clone())
    );
    assert!(matches!(
        target.authz().install_quorum_reconciled_authz_realm(&newer),
        Ok(AuthzRealmSnapshotApplied { replayed: true, .. })
    ));

    let mut unordered = initial.clone();
    unordered.tuples.reverse();
    assert!(matches!(
        unordered.validate(),
        Err(AuthzRealmSnapshotError::InvalidAggregate(_))
    ));
    let mut bad_stamp = initial.clone();
    bad_stamp
        .mutation_stamp
        .as_mut()
        .unwrap()
        .source_journal_position = 0;
    assert!(matches!(
        bad_stamp.validate(),
        Err(AuthzRealmSnapshotError::InvalidAggregate(_))
    ));
    let mut bad_receipt = initial;
    bad_receipt.receipts[0].input_fingerprint[0] ^= 1;
    assert!(matches!(
        bad_receipt.validate(),
        Err(AuthzRealmSnapshotError::InvalidAggregate(_))
    ));
}

#[tokio::test]
async fn quorum_reconciled_install_replaces_a_minority_sibling_and_absence() {
    let temporary = tempfile::tempdir().unwrap();
    let template = Store::open(StoreOptions::new(temporary.path().join("template"), 1))
        .await
        .unwrap();
    populate(&template.authz());
    let initial = template
        .authz()
        .export_authz_realm(&scope("documents"))
        .unwrap()
        .unwrap();

    let left = Store::open(StoreOptions::new(temporary.path().join("left"), 2))
        .await
        .unwrap();
    let right = Store::open(StoreOptions::new(temporary.path().join("right"), 3))
        .await
        .unwrap();
    for store in [&left, &right] {
        store
            .authz()
            .install_quorum_reconciled_authz_realm(&initial)
            .unwrap();
    }
    let left_mutation = left
        .authz()
        .coordinate_tuple_mutation(
            tuple_request("documents", "left-sibling", 3, &[("left", "alice")]),
            context("left-sibling", 7),
        )
        .unwrap()
        .mutation
        .unwrap();
    let right_mutation = right
        .authz()
        .coordinate_tuple_mutation(
            tuple_request("documents", "right-sibling", 3, &[("right", "bob")]),
            context("right-sibling", 8),
        )
        .unwrap()
        .mutation
        .unwrap();
    assert_eq!(left_mutation.revision(), right_mutation.revision());
    assert!(matches!(
        left.authz()
            .apply_authz_realm_mutation_replica(&right_mutation),
        Err(AuthzStoreError::RealmMutationSibling { .. })
    ));

    let winner = right
        .authz()
        .export_authz_realm(&scope("documents"))
        .unwrap()
        .unwrap();
    left.authz()
        .install_quorum_reconciled_authz_realm(&winner)
        .unwrap();
    assert_eq!(
        left.authz()
            .export_authz_realm(&scope("documents"))
            .unwrap(),
        Some(winner)
    );

    left.authz()
        .install_quorum_reconciled_authz_realm_candidate(&scope("documents"), None)
        .unwrap();
    assert!(
        left.authz()
            .export_authz_realm(&scope("documents"))
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn export_validates_cursor_limits_and_key_page_size() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    populate(&store.authz());
    assert!(matches!(
        AuthzRealmCursor::from_token("not-a-cursor"),
        Err(AuthzRealmSnapshotError::InvalidCursor)
    ));
    assert!(matches!(
        store
            .authz()
            .export_authz_realm_keys(None, 0, MAX_AUTHZ_REALM_EXPORT_BYTES),
        Err(AuthzRealmSnapshotError::InvalidExportLimit(_))
    ));
    assert!(matches!(
        store.authz().export_authz_realm_keys(None, 1, 1),
        Err(AuthzRealmSnapshotError::ExportKeyTooLarge { .. })
    ));
    assert!(
        store
            .authz()
            .export_authz_realm(&scope("absent"))
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn released_untyped_receipts_do_not_become_a_transfer_side_plane() {
    let temporary = tempfile::tempdir().unwrap();
    let source = Store::open(StoreOptions::new(temporary.path().join("source"), 1))
        .await
        .unwrap();
    let repository = source.authz();
    let published = repository
        .publish_schema(PublishSchemaRequest {
            storage_tenant: tenant(),
            schema_id: SchemaId::parse("documents").unwrap(),
            schema: schema(),
            expected_revision: Some(AuthzRevision::ZERO),
        })
        .unwrap();
    repository
        .bind_schema(BindSchemaRequest {
            scope: scope("legacy"),
            schema_ref: published.schema_ref,
            expected_generation: Some(0),
            expected_revision: Some(AuthzRevision(1)),
        })
        .unwrap();
    repository
        .mutate_tuples(tuple_request(
            "legacy",
            "legacy-receipt",
            2,
            &[("one", "alice")],
        ))
        .unwrap();
    let aggregate = repository
        .export_authz_realm(&scope("legacy"))
        .unwrap()
        .unwrap();
    assert!(aggregate.mutation_stamp.is_none());
    assert!(aggregate.receipts.is_empty());

    let target = Store::open(StoreOptions::new(temporary.path().join("target"), 2))
        .await
        .unwrap();
    target
        .authz()
        .install_quorum_reconciled_authz_realm(&aggregate)
        .unwrap();
    assert_eq!(
        target
            .authz()
            .realm_snapshot(&scope("legacy"), AuthzConsistency::Latest)
            .unwrap()
            .tuples,
        vec![tuple("one", "alice")]
    );
}
