use super::*;
use crate::anvil_api::{
    AuthzAllowedSubject, AuthzNamespaceSchema, AuthzRelationRule, AuthzRelationSchema,
    AuthzSchemaMemberKind, AuthzSubjectSelectorKind,
};
use crate::core_store::{
    CF_AUTHZ, CoreMetaStore, CoreMetaTuplePart, TABLE_AUTHZ_IDEMPOTENCY_RECEIPT_ROW,
    TABLE_AUTHZ_TUPLE_OBJECT_CURRENT_ROW, TABLE_AUTHZ_TUPLE_SUBJECT_CURRENT_ROW,
    core_meta_tuple_key,
};
use crate::partition_fence::{
    PartitionRecoveryAcquire, acquire_partition_recovery, force_expire_partition_owner_for_node,
    publish_partition_ready,
};
use chrono::Utc;
use tempfile::tempdir;

const PARTITION_OWNER_KEY: &[u8] = b"authorization tuple partition owner signing key";

fn any_subject(subject_kind: &str) -> AuthzAllowedSubject {
    AuthzAllowedSubject {
        selector_kind: AuthzSubjectSelectorKind::AnyCanonicalId as i32,
        subject_kind: subject_kind.to_string(),
        subject_id: String::new(),
    }
}

fn direct_relation(name: &str, subject_kinds: &[&str]) -> AuthzRelationSchema {
    AuthzRelationSchema {
        relation: name.to_string(),
        rules: Vec::new(),
        member_kind: AuthzSchemaMemberKind::DirectRelation as i32,
        allowed_subjects: subject_kinds.iter().map(|kind| any_subject(kind)).collect(),
    }
}

fn permission(name: &str, rules: Vec<AuthzRelationRule>) -> AuthzRelationSchema {
    AuthzRelationSchema {
        relation: name.to_string(),
        rules,
        member_kind: AuthzSchemaMemberKind::Permission as i32,
        allowed_subjects: Vec::new(),
    }
}

fn record(revision: i64, operation: &str) -> AuthzTupleRecord {
    AuthzTupleRecord {
        revision,
        revision_ordinal: 0,
        tenant_id: 42,
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        operation: operation.to_string(),
        written_by: "tester".to_string(),
        reason: "test".to_string(),
        mutation_id: uuid::Uuid::new_v4(),
        record_hash: hex::encode(hash32(format!("record-{revision}").as_bytes())),
        written_at: Utc::now(),
    }
}

fn tuple(
    revision: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    operation: &str,
) -> AuthzTupleRecord {
    AuthzTupleRecord {
            revision,
            revision_ordinal: 0,
            tenant_id: 42,
            namespace: namespace.to_string(),
            object_id: object_id.to_string(),
            relation: relation.to_string(),
            subject_kind: subject_kind.to_string(),
            subject_id: subject_id.to_string(),
            caveat_hash: String::new(),
            operation: operation.to_string(),
            written_by: "tester".to_string(),
            reason: "test".to_string(),
            mutation_id: uuid::Uuid::new_v4(),
            record_hash: hex::encode(hash32(
                format!(
                    "record-{revision}-{namespace}-{object_id}-{relation}-{subject_kind}-{subject_id}-{operation}"
                )
                .as_bytes(),
            )),
            written_at: Utc::now(),
    }
}

#[allow(clippy::too_many_arguments)]
fn tuple_with_caveat(
    revision: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
    operation: &str,
) -> AuthzTupleRecord {
    let mut record = tuple(
        revision,
        namespace,
        object_id,
        relation,
        subject_kind,
        subject_id,
        operation,
    );
    record.caveat_hash = caveat_hash.to_string();
    record.record_hash = authz_record_hash(AuthzRecordHashInput {
        revision: record.revision,
        revision_ordinal: record.revision_ordinal,
        tenant_id: record.tenant_id,
        namespace: &record.namespace,
        object_id: &record.object_id,
        relation: &record.relation,
        subject_kind: &record.subject_kind,
        subject_id: &record.subject_id,
        caveat_hash: &record.caveat_hash,
        operation: &record.operation,
        written_by: &record.written_by,
        reason: &record.reason,
    });
    record
}

async fn ready_authz_permit(
    storage: &Storage,
    tenant_id: i64,
    owner_node_id: &str,
) -> PartitionWritePermit {
    let request = PartitionRecoveryAcquire {
        partition_family: "authz_tuple".to_string(),
        partition_id: hex::encode(authz_partition_id(tenant_id)),
        owner_node_id: owner_node_id.to_string(),
        recovered_through_sequence: 0,
        recovered_manifest_hash: hex::encode([0; 32]),
        now_nanos: 100,
    };
    let recovering = acquire_partition_recovery(storage, request, PARTITION_OWNER_KEY)
        .await
        .unwrap();
    publish_partition_ready(
        storage,
        &recovering.partition_family,
        &recovering.partition_id,
        owner_node_id,
        recovering.fence_token,
        0,
        &hex::encode([3; 32]),
        200,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap()
    .write_permit()
    .unwrap()
}

async fn bind_default_document_schema(storage: &Storage, tenant_id: i64) -> i64 {
    let schema = crate::authz_realm_schema::put_schema_revision(
        storage,
        tenant_id,
        "test-authz",
        vec![AuthzNamespaceSchema {
            namespace: "document".to_string(),
            relations: vec![
                direct_relation("viewer", &["user"]),
                direct_relation("editor", &["user"]),
            ],
            schema_json: String::new(),
            schema_hash: String::new(),
            schema_version: 0,
            authz_revision: 0,
            applied_at: String::new(),
        }],
        "tester",
        "test schema",
    )
    .await
    .unwrap();
    crate::authz_realm_schema::bind_schema(
        storage,
        tenant_id,
        DEFAULT_AUTHZ_REALM_ID,
        schema.schema_ref,
        None,
        "tester",
        "bind test schema",
    )
    .await
    .unwrap()
    .authz_revision
    .try_into()
    .unwrap()
}

async fn append_authz_record_without_segment(
    storage: &Storage,
    record: &AuthzTupleRecord,
) -> Result<()> {
    test_append_authz_tuple_record_unfenced(storage, record).await
}

#[tokio::test]
async fn authz_journal_recovers_latest_exact_and_watch_ranges() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    test_append_authz_tuple_record_unfenced(&storage, &record(1, "add"))
        .await
        .unwrap();
    materialize_authz_tuple_segment_at_revision(&storage, 42, 1, 0)
        .await
        .unwrap();
    test_append_authz_tuple_record_unfenced(&storage, &record(2, "remove"))
        .await
        .unwrap();

    assert_eq!(latest_authz_revision(&storage, 42).await.unwrap(), 2);
    assert!(
        check_authz_tuple(
            &storage, 42, "document", "alpha", "viewer", "user", "alice", ""
        )
        .await
        .unwrap()
        .is_none()
    );
    assert_eq!(
        check_authz_tuple_at_revision(
            &storage, 42, "document", "alpha", "viewer", "user", "alice", "", 1
        )
        .await
        .unwrap()
        .unwrap()
        .operation,
        "add"
    );
    let watched = list_authz_tuple_log(&storage, 42, 0, "document", 10)
        .await
        .unwrap();
    assert_eq!(watched.len(), 2);
    assert_eq!(watched[1].revision, 2);
}

#[tokio::test]
async fn latest_authz_revision_uses_the_journal_head_not_a_tuple_scan() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let record = record(1, "add");
    test_append_authz_tuple_record_unfenced(&storage, &record)
        .await
        .unwrap();

    let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
    meta.delete(
        CF_AUTHZ,
        TABLE_AUTHZ_TUPLE_OBJECT_CURRENT_ROW,
        &projection::object_row_key(&record).unwrap(),
    )
    .unwrap();

    assert_eq!(latest_authz_revision(&storage, 42).await.unwrap(), 1);
}

#[tokio::test]
async fn tuple_writes_defer_segments_but_current_checks_use_current_rows() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let record = record(1, "add");
    test_append_authz_tuple_record_unfenced(&storage, &record)
        .await
        .unwrap();

    assert!(
        authz_segment::existing_authz_tuple_segment_ref(&storage, 42, 1)
            .await
            .unwrap()
            .is_none()
    );

    assert!(
        resolve_current_permission(
            &storage, 42, "document", "alpha", "viewer", "user", "alice", ""
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn missing_authz_segments_require_the_explicit_rebuild_path() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for revision in 1..=3 {
        let object_id = format!("object-{revision}");
        let record = tuple(
            revision, "document", &object_id, "viewer", "user", "alice", "add",
        );
        append_authz_record_without_segment(&storage, &record)
            .await
            .unwrap();
    }

    assert_eq!(latest_authz_revision(&storage, 42).await.unwrap(), 3);
    for revision in 1..=3 {
        assert!(
            authz_segment::existing_authz_tuple_segment_ref(&storage, 42, revision)
                .await
                .unwrap()
                .is_none()
        );
    }

    let unavailable = authz_segment::read_required_authz_tuple_segment_at_revision(&storage, 42, 3)
        .await
        .unwrap_err();
    assert!(unavailable.to_string().contains("AuthzRevisionUnavailable"));

    let incremental = materialize_authz_tuple_segment_at_revision(&storage, 42, 3, 0)
        .await
        .unwrap_err();
    assert!(
        incremental
            .to_string()
            .contains("AuthzMaterializationRepairRequired")
    );

    let outcome = rebuild_authz_materialization_at_revision(&storage, 42, 3, 0)
        .await
        .unwrap();
    assert_eq!(outcome.source_rows_visited, 3);
    let segment = authz_segment::read_required_authz_tuple_segment_at_revision(&storage, 42, 3)
        .await
        .unwrap()
        .expect("explicitly rebuilt authorization segment");
    assert_eq!(segment.header.generation, 3);
    assert_eq!(segment.records.len(), 3);
    assert!(
        authz_segment::existing_authz_tuple_segment_ref(&storage, 42, 1)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        authz_segment::existing_authz_tuple_segment_ref(&storage, 42, 2)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        authz_segment::existing_authz_tuple_segment_ref(&storage, 42, 3)
            .await
            .unwrap()
            .is_some()
    );
}

#[test]
fn caveat_hash_validation_accepts_empty_or_hex32_only() {
    validate_optional_caveat_hash("").unwrap();
    validate_optional_caveat_hash(&hex::encode([7; 32])).unwrap();
    validate_optional_caveat_hash("not-hex32").unwrap_err();
    validate_optional_caveat_hash(&hex::encode([7; 31])).unwrap_err();
}

#[tokio::test]
async fn authz_resolves_direct_and_nested_userset_tuples() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for record in [
        tuple(1, "group", "engineering", "member", "user", "alice", "add"),
        tuple(
            2,
            "folder",
            "platform",
            "viewer",
            "userset",
            "group/engineering#member",
            "add",
        ),
        tuple(
            3,
            "document",
            "alpha",
            "viewer",
            "userset",
            "folder/platform#viewer",
            "add",
        ),
    ] {
        test_append_authz_tuple_record_unfenced(&storage, &record)
            .await
            .unwrap();
    }

    assert!(
        resolve_permission_at_revision(
            &storage, 42, "document", "alpha", "viewer", "user", "alice", "", 3
        )
        .await
        .unwrap()
    );
    assert!(
        !resolve_permission_at_revision(
            &storage, 42, "document", "alpha", "viewer", "user", "bob", "", 3
        )
        .await
        .unwrap()
    );
    assert!(
        resolve_current_permission(
            &storage, 42, "document", "alpha", "viewer", "user", "alice", ""
        )
        .await
        .unwrap()
    );
    assert!(
        !resolve_current_permission(
            &storage, 42, "document", "alpha", "viewer", "user", "bob", ""
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn authz_userset_removal_and_cycles_do_not_grant_access() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let records = vec![
        tuple(1, "group", "engineering", "member", "user", "alice", "add"),
        tuple(
            2,
            "folder",
            "platform",
            "viewer",
            "userset",
            "group/engineering#member",
            "add",
        ),
        tuple(
            3,
            "document",
            "alpha",
            "viewer",
            "userset",
            "folder/platform#viewer",
            "add",
        ),
        tuple(
            4,
            "folder",
            "platform",
            "viewer",
            "userset",
            "group/engineering#member",
            "remove",
        ),
        tuple(
            5,
            "group",
            "a",
            "member",
            "userset",
            "group/b#member",
            "add",
        ),
        tuple(
            6,
            "group",
            "b",
            "member",
            "userset",
            "group/a#member",
            "add",
        ),
    ];
    for record in &records {
        test_append_authz_tuple_record_unfenced(&storage, &record)
            .await
            .unwrap();
    }
    let rebuilt = rebuild_authz_materialization_at_revision(&storage, 42, 3, 0)
        .await
        .unwrap();
    assert_eq!(rebuilt.processed_revision, 3);
    assert_eq!(rebuilt.generation, 3);
    materialize_authz_tuple_segment_at_revision(&storage, 42, 4, 0)
        .await
        .unwrap();

    assert!(
        resolve_permission_at_revision(
            &storage, 42, "document", "alpha", "viewer", "user", "alice", "", 3
        )
        .await
        .unwrap()
    );
    assert!(
        !resolve_current_permission(
            &storage, 42, "document", "alpha", "viewer", "user", "alice", ""
        )
        .await
        .unwrap()
    );
    assert!(
        !resolve_permission_at_revision(
            &storage, 42, "document", "alpha", "viewer", "user", "alice", "", 4
        )
        .await
        .unwrap()
    );
    assert!(
        !resolve_permission_at_revision(
            &storage, 42, "group", "a", "member", "user", "alice", "", 6
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn current_permission_resolution_does_not_visit_unrelated_tuples() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for record in [
        tuple(1, "group", "engineering", "member", "user", "alice", "add"),
        tuple(
            2,
            "folder",
            "platform",
            "viewer",
            "userset",
            "group/engineering#member",
            "add",
        ),
        tuple(
            3,
            "document",
            "alpha",
            "viewer",
            "userset",
            "folder/platform#viewer",
            "add",
        ),
    ] {
        test_append_authz_tuple_record_unfenced(&storage, &record)
            .await
            .unwrap();
    }

    let before = resolve_current_permission_with_stats(
        &storage, 42, "document", "alpha", "viewer", "user", "alice", "",
    )
    .await
    .unwrap();
    assert!(before.allowed);

    for ordinal in 0..256_i64 {
        test_append_authz_tuple_record_unfenced(
            &storage,
            &tuple(
                ordinal + 4,
                "document",
                &format!("unrelated-{ordinal:04}"),
                "viewer",
                "user",
                "someone-else",
                "add",
            ),
        )
        .await
        .unwrap();
    }

    let after = resolve_current_permission_with_stats(
        &storage, 42, "document", "alpha", "viewer", "user", "alice", "",
    )
    .await
    .unwrap();
    assert!(after.allowed);
    assert_eq!(
        after.stats.projection_rows_visited,
        before.stats.projection_rows_visited
    );
    assert_eq!(
        after.stats.graph_nodes_visited,
        before.stats.graph_nodes_visited
    );
    assert_eq!(
        after.stats.schema_point_reads,
        before.stats.schema_point_reads
    );
}

#[tokio::test]
async fn indexed_resolver_preserves_direct_userset_public_caveat_and_revision_semantics() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let caveat_hash = hex::encode([7_u8; 32]);
    let records = vec![
        tuple(1, "document", "direct", "viewer", "user", "alice", "add"),
        tuple(
            2,
            "document",
            "public",
            "viewer",
            crate::authz_schema_contract::PUBLIC_SUBJECT_KIND,
            crate::authz_schema_contract::PUBLIC_SUBJECT_ID,
            "add",
        ),
        tuple_with_caveat(
            3,
            "document",
            "conditional",
            "viewer",
            "user",
            "alice",
            &caveat_hash,
            "add",
        ),
        tuple(4, "group", "engineering", "member", "user", "alice", "add"),
        tuple(
            5,
            "document",
            "nested",
            "viewer",
            "userset",
            "group/engineering#member",
            "add",
        ),
    ];
    for record in &records {
        test_append_authz_tuple_record_unfenced(&storage, record)
            .await
            .unwrap();
    }
    authz_segment::write_authz_tuple_checkpoint_segment(&storage, 42, &records, None, 5, 5, 0)
        .await
        .unwrap();
    test_append_authz_tuple_record_unfenced(
        &storage,
        &tuple(6, "document", "direct", "viewer", "user", "alice", "remove"),
    )
    .await
    .unwrap();

    assert!(
        !resolve_current_permission(
            &storage, 42, "document", "direct", "viewer", "user", "alice", ""
        )
        .await
        .unwrap()
    );
    assert!(
        resolve_permission_at_revision(
            &storage, 42, "document", "direct", "viewer", "user", "alice", "", 5
        )
        .await
        .unwrap()
    );
    assert!(
        resolve_current_permission(
            &storage,
            42,
            "document",
            "public",
            "viewer",
            crate::authz_schema_contract::PUBLIC_SUBJECT_KIND,
            crate::authz_schema_contract::PUBLIC_SUBJECT_ID,
            "",
        )
        .await
        .unwrap()
    );
    assert!(
        resolve_current_permission(
            &storage,
            42,
            "document",
            "conditional",
            "viewer",
            "user",
            "alice",
            &caveat_hash,
        )
        .await
        .unwrap()
    );
    assert!(
        !resolve_current_permission(
            &storage,
            42,
            "document",
            "conditional",
            "viewer",
            "user",
            "alice",
            "",
        )
        .await
        .unwrap()
    );
    assert!(
        resolve_current_permission(
            &storage, 42, "document", "nested", "viewer", "user", "alice", ""
        )
        .await
        .unwrap()
    );
    let future = resolve_permission_at_revision(
        &storage, 42, "document", "direct", "viewer", "user", "alice", "", 7,
    )
    .await
    .unwrap_err();
    assert!(future.to_string().contains("AuthzRevisionUnavailable"));
}

#[tokio::test]
async fn authz_bound_schema_inherit_computed_and_tuple_to_userset_rules_are_enforced() {
    use crate::anvil_api::AuthzNamespaceSchema;
    use crate::authz_scope::encode_realm_namespace;

    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let realm_id = "realm_a";
    let document_ns = encode_realm_namespace(realm_id, "document");
    let folder_ns = encode_realm_namespace(realm_id, "folder");
    let tenant_ns = encode_realm_namespace(realm_id, "tenant");
    let group_ns = encode_realm_namespace(realm_id, "group");
    let schema = crate::authz_realm_schema::put_schema_revision(
        &storage,
        42,
        "workspace-authz",
        vec![
            AuthzNamespaceSchema {
                namespace: "document".to_string(),
                relations: vec![
                    permission(
                        "viewer",
                        vec![
                            AuthzRelationRule {
                                kind: "inherit".to_string(),
                                relation: "editor".to_string(),
                                tuple_relation: String::new(),
                                target_relation: String::new(),
                            },
                            AuthzRelationRule {
                                kind: "computed".to_string(),
                                relation: String::new(),
                                tuple_relation: "parent_folder".to_string(),
                                target_relation: "viewer".to_string(),
                            },
                            AuthzRelationRule {
                                kind: "tuple_to_userset".to_string(),
                                relation: String::new(),
                                tuple_relation: "shared_group".to_string(),
                                target_relation: "member".to_string(),
                            },
                        ],
                    ),
                    direct_relation("editor", &["user"]),
                    direct_relation("parent_folder", &["folder"]),
                    direct_relation("shared_group", &["group"]),
                ],
                schema_json: String::new(),
                schema_hash: String::new(),
                schema_version: 0,
                authz_revision: 0,
                applied_at: String::new(),
            },
            AuthzNamespaceSchema {
                namespace: "folder".to_string(),
                relations: vec![
                    permission(
                        "viewer",
                        vec![AuthzRelationRule {
                            kind: "computed".to_string(),
                            relation: String::new(),
                            tuple_relation: "parent_tenant".to_string(),
                            target_relation: "member".to_string(),
                        }],
                    ),
                    direct_relation("parent_tenant", &["tenant"]),
                ],
                schema_json: String::new(),
                schema_hash: String::new(),
                schema_version: 0,
                authz_revision: 0,
                applied_at: String::new(),
            },
            AuthzNamespaceSchema {
                namespace: "tenant".to_string(),
                relations: vec![direct_relation("member", &["user"])],
                schema_json: String::new(),
                schema_hash: String::new(),
                schema_version: 0,
                authz_revision: 0,
                applied_at: String::new(),
            },
            AuthzNamespaceSchema {
                namespace: "group".to_string(),
                relations: vec![direct_relation("member", &["user"])],
                schema_json: String::new(),
                schema_hash: String::new(),
                schema_version: 0,
                authz_revision: 0,
                applied_at: String::new(),
            },
        ],
        "tester",
        "test schema",
    )
    .await
    .unwrap();
    let binding = crate::authz_realm_schema::bind_schema(
        &storage,
        42,
        realm_id,
        schema.schema_ref,
        None,
        "tester",
        "bind schema",
    )
    .await
    .unwrap();
    let tuple_base_revision = i64::try_from(binding.authz_revision).unwrap();

    for record in [
        tuple(
            tuple_base_revision + 1,
            &document_ns,
            "alpha",
            "editor",
            "user",
            "direct-editor",
            "add",
        ),
        tuple(
            tuple_base_revision + 2,
            &tenant_ns,
            "acme",
            "member",
            "user",
            "tenant-member",
            "add",
        ),
        tuple(
            tuple_base_revision + 3,
            &folder_ns,
            "platform",
            "parent_tenant",
            "tenant",
            "acme",
            "add",
        ),
        tuple(
            tuple_base_revision + 4,
            &document_ns,
            "alpha",
            "parent_folder",
            "folder",
            "platform",
            "add",
        ),
        tuple(
            tuple_base_revision + 5,
            &group_ns,
            "engineering",
            "member",
            "user",
            "group-member",
            "add",
        ),
        tuple(
            tuple_base_revision + 6,
            &document_ns,
            "alpha",
            "shared_group",
            "group",
            "engineering",
            "add",
        ),
    ] {
        test_append_authz_tuple_record_unfenced(&storage, &record)
            .await
            .unwrap();
    }

    for subject in ["direct-editor", "tenant-member", "group-member"] {
        assert!(
            resolve_permission_at_revision(
                &storage,
                42,
                &document_ns,
                "alpha",
                "viewer",
                "user",
                subject,
                "",
                tuple_base_revision + 6,
            )
            .await
            .unwrap(),
            "{subject} should be granted by the bound schema"
        );
    }
    assert_eq!(
        list_current_authz_objects_page(
            &storage,
            42,
            &document_ns,
            "viewer",
            "user",
            "tenant-member",
            "",
            tuple_base_revision + 6,
            None,
            100,
        )
        .await
        .unwrap()
        .object_ids,
        vec!["alpha".to_string()]
    );
    assert_eq!(
        list_current_authz_subjects_page(
            &storage,
            42,
            &document_ns,
            "alpha",
            "viewer",
            Some("user"),
            tuple_base_revision + 6,
            None,
            100,
        )
        .await
        .unwrap()
        .subjects,
        vec![
            AuthzSubjectRef {
                subject_kind: "user".to_string(),
                subject_id: "direct-editor".to_string(),
                caveat_hash: String::new(),
            },
            AuthzSubjectRef {
                subject_kind: "user".to_string(),
                subject_id: "group-member".to_string(),
                caveat_hash: String::new(),
            },
            AuthzSubjectRef {
                subject_kind: "user".to_string(),
                subject_id: "tenant-member".to_string(),
                caveat_hash: String::new(),
            },
        ]
    );
}

#[tokio::test]
async fn authz_schema_writes_materialize_segment_schema_tables() {
    use crate::anvil_api::AuthzNamespaceSchema;
    use crate::authz_scope::encode_realm_namespace;

    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let schema = crate::authz_realm_schema::put_schema_revision(
        &storage,
        42,
        "workspace-authz",
        vec![AuthzNamespaceSchema {
            namespace: "document".to_string(),
            relations: vec![
                permission(
                    "viewer",
                    vec![AuthzRelationRule {
                        kind: "inherit".to_string(),
                        relation: "editor".to_string(),
                        tuple_relation: String::new(),
                        target_relation: String::new(),
                    }],
                ),
                direct_relation("editor", &["user"]),
            ],
            schema_json: String::new(),
            schema_hash: String::new(),
            schema_version: 0,
            authz_revision: 0,
            applied_at: String::new(),
        }],
        "tester",
        "put schema",
    )
    .await
    .unwrap();

    let segment = authz_segment::read_latest_authz_tuple_segment(&storage, 42)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(segment.header.generation, schema.authz_revision);
    assert!(segment.records.is_empty());
    assert!(segment.schema_descriptors.iter().any(|row| {
        row.realm_id.is_empty()
            && row.namespace == "document"
            && row.schema_id == "workspace-authz"
            && row.schema_revision == 1
    }));
    assert!(segment.relation_rules.iter().any(|row| {
        row.realm_id.is_empty()
            && row.namespace == "document"
            && row.relation == "viewer"
            && row.rule_kind == "inherit"
            && row.inherited_relation == "editor"
    }));

    let binding = crate::authz_realm_schema::bind_schema(
        &storage,
        42,
        "workspace-a",
        schema.schema_ref,
        None,
        "tester",
        "bind schema",
    )
    .await
    .unwrap();
    let bound_segment = authz_segment::read_latest_authz_tuple_segment(&storage, 42)
        .await
        .unwrap()
        .unwrap();
    let bound_namespace = encode_realm_namespace("workspace-a", "document");
    assert_eq!(
        bound_segment.header.generation,
        binding.authz_revision as u64
    );
    assert!(bound_segment.schema_descriptors.iter().any(|row| {
        row.realm_id == "workspace-a"
            && row.namespace == bound_namespace
            && row.schema_id == "workspace-authz"
            && row.binding_generation == 1
    }));
    assert!(bound_segment.relation_rules.iter().any(|row| {
        row.realm_id == "workspace-a"
            && row.namespace == bound_namespace
            && row.relation == "viewer"
            && row.rule_kind == "inherit"
            && row.inherited_relation == "editor"
    }));
    assert!(
        bound_segment
            .revision_checkpoints
            .iter()
            .any(|row| row.revision == binding.authz_revision as u64)
    );
}

#[tokio::test]
async fn authz_current_tuple_reads_filter_active_adds_only() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for record in [
        tuple(1, "document", "alpha", "viewer", "user", "alice", "add"),
        tuple(2, "document", "beta", "viewer", "user", "alice", "add"),
        tuple(3, "document", "beta", "viewer", "user", "alice", "remove"),
        tuple(4, "document", "alpha", "editor", "user", "bob", "add"),
    ] {
        test_append_authz_tuple_record_unfenced(&storage, &record)
            .await
            .unwrap();
    }

    let filter = AuthzTupleFilter {
        namespace: Some("document".to_string()),
        relation: Some("viewer".to_string()),
        subject_kind: Some("user".to_string()),
        subject_id: Some("alice".to_string()),
        caveat_hash: Some(String::new()),
        ..AuthzTupleFilter::default()
    };
    let active_viewers = page_current_authz_tuples(&storage, 42, &filter, 4, None, 100)
        .await
        .unwrap()
        .records;
    assert_eq!(active_viewers.len(), 1);
    assert_eq!(active_viewers[0].object_id, "alpha");

    assert_eq!(
        page_current_authz_tuples(&storage, 42, &filter, 2, None, 100)
            .await
            .unwrap_err(),
        AuthzProjectionPageError::RevisionMismatch {
            expected: 2,
            actual: 4,
        }
    );

    assert_eq!(
        list_current_authz_objects_page(
            &storage, 42, "document", "viewer", "user", "alice", "", 4, None, 100,
        )
        .await
        .unwrap()
        .object_ids,
        vec!["alpha".to_string()]
    );
    assert_eq!(
        list_current_authz_subjects_page(
            &storage,
            42,
            "document",
            "alpha",
            "editor",
            Some("user"),
            4,
            None,
            100,
        )
        .await
        .unwrap()
        .subjects,
        vec![AuthzSubjectRef {
            subject_kind: "user".to_string(),
            subject_id: "bob".to_string(),
            caveat_hash: String::new(),
        }]
    );
}

#[tokio::test]
async fn current_authz_projection_pages_in_object_order() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for (revision, object_id) in [(1, "alpha"), (2, "beta"), (3, "gamma")] {
        test_append_authz_tuple_record_unfenced(
            &storage,
            &tuple(
                revision, "document", object_id, "viewer", "user", "alice", "add",
            ),
        )
        .await
        .unwrap();
    }

    let filter = AuthzTupleFilter {
        realm_id: Some(DEFAULT_AUTHZ_REALM_ID.to_string()),
        namespace: Some("document".to_string()),
        ..AuthzTupleFilter::default()
    };
    let first = page_current_authz_tuples(&storage, 42, &filter, 3, None, 2)
        .await
        .unwrap();
    assert_eq!(
        first
            .records
            .iter()
            .map(|record| record.object_id.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "beta"]
    );
    let second =
        page_current_authz_tuples(&storage, 42, &filter, 3, first.next_tuple_key.as_deref(), 2)
            .await
            .unwrap();
    assert_eq!(
        second
            .records
            .iter()
            .map(|record| record.object_id.as_str())
            .collect::<Vec<_>>(),
        vec!["gamma"]
    );
    assert!(second.next_tuple_key.is_none());
}

#[tokio::test]
async fn current_authz_projection_uses_subject_order_for_subject_lookup() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for record in [
        tuple(1, "document", "alpha", "viewer", "user", "alice", "add"),
        tuple(2, "document", "beta", "viewer", "user", "bob", "add"),
        tuple(3, "document", "gamma", "editor", "user", "alice", "add"),
    ] {
        test_append_authz_tuple_record_unfenced(&storage, &record)
            .await
            .unwrap();
    }

    let page = page_current_authz_tuples(
        &storage,
        42,
        &AuthzTupleFilter {
            realm_id: Some(DEFAULT_AUTHZ_REALM_ID.to_string()),
            subject_kind: Some("user".to_string()),
            subject_id: Some("alice".to_string()),
            caveat_hash: Some(String::new()),
            ..AuthzTupleFilter::default()
        },
        3,
        None,
        10,
    )
    .await
    .unwrap();
    assert_eq!(
        page.records
            .iter()
            .map(|record| record.object_id.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "gamma"]
    );
    assert_eq!(page.candidates_visited, 2);
}

#[tokio::test]
async fn remove_deletes_both_active_authz_projections() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let added = record(1, "add");
    test_append_authz_tuple_record_unfenced(&storage, &added)
        .await
        .unwrap();
    let removed = record(2, "remove");
    test_append_authz_tuple_record_unfenced(&storage, &removed)
        .await
        .unwrap();

    let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
    assert!(
        meta.get(
            CF_AUTHZ,
            TABLE_AUTHZ_TUPLE_OBJECT_CURRENT_ROW,
            &projection::object_row_key(&removed).unwrap(),
        )
        .unwrap()
        .is_none()
    );
    assert!(
        meta.get(
            CF_AUTHZ,
            TABLE_AUTHZ_TUPLE_SUBJECT_CURRENT_ROW,
            &projection::subject_row_key(&removed).unwrap(),
        )
        .unwrap()
        .is_none()
    );
}

#[tokio::test]
async fn remove_deletes_old_projection_after_unrelated_authz_revisions() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let added = record(1, "add");
    test_append_authz_tuple_record_unfenced(&storage, &added)
        .await
        .unwrap();
    for revision in 2..=5 {
        test_append_authz_tuple_record_unfenced(
            &storage,
            &tuple(
                revision,
                "document",
                &format!("unrelated-{revision}"),
                "viewer",
                "user",
                "bob",
                "add",
            ),
        )
        .await
        .unwrap();
    }

    let removed = record(6, "remove");
    test_append_authz_tuple_record_unfenced(&storage, &removed)
        .await
        .unwrap();

    let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
    assert!(
        meta.get(
            CF_AUTHZ,
            TABLE_AUTHZ_TUPLE_OBJECT_CURRENT_ROW,
            &projection::object_row_key(&removed).unwrap(),
        )
        .unwrap()
        .is_none()
    );
    assert!(
        meta.get(
            CF_AUTHZ,
            TABLE_AUTHZ_TUPLE_SUBJECT_CURRENT_ROW,
            &projection::subject_row_key(&removed).unwrap(),
        )
        .unwrap()
        .is_none()
    );
}

#[tokio::test]
async fn current_authz_projection_rejects_stale_collection_revision() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    test_append_authz_tuple_record_unfenced(&storage, &record(1, "add"))
        .await
        .unwrap();
    test_append_authz_tuple_record_unfenced(
        &storage,
        &tuple(2, "document", "beta", "viewer", "user", "alice", "add"),
    )
    .await
    .unwrap();

    let error = page_current_authz_tuples(&storage, 42, &AuthzTupleFilter::default(), 1, None, 10)
        .await
        .unwrap_err();
    assert_eq!(
        error,
        AuthzProjectionPageError::RevisionMismatch {
            expected: 1,
            actual: 2,
        }
    );
}

#[tokio::test]
async fn sparse_authz_filter_stops_at_the_candidate_budget() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let candidate_budget = 17;
    for ordinal in 0..candidate_budget {
        test_append_authz_tuple_record_unfenced(
            &storage,
            &tuple(
                ordinal + 1,
                "document",
                &format!("a-{ordinal:02}"),
                "other",
                "user",
                "alice",
                "add",
            ),
        )
        .await
        .unwrap();
    }
    test_append_authz_tuple_record_unfenced(
        &storage,
        &tuple(
            candidate_budget + 1,
            "document",
            "z-target",
            "wanted",
            "user",
            "alice",
            "add",
        ),
    )
    .await
    .unwrap();

    let revision = candidate_budget + 1;
    let filter = AuthzTupleFilter {
        realm_id: Some(DEFAULT_AUTHZ_REALM_ID.to_string()),
        relation: Some("wanted".to_string()),
        ..AuthzTupleFilter::default()
    };
    let first = page_current_authz_tuples(&storage, 42, &filter, revision, None, 1)
        .await
        .unwrap();
    assert!(first.records.is_empty());
    assert_eq!(first.candidates_visited, candidate_budget as usize);
    assert!(first.next_tuple_key.is_some());

    let second = page_current_authz_tuples(
        &storage,
        42,
        &filter,
        revision,
        first.next_tuple_key.as_deref(),
        1,
    )
    .await
    .unwrap();
    assert_eq!(second.records[0].object_id, "z-target");
}

#[tokio::test]
async fn list_objects_page_bounds_sparse_zanzibar_resolution_and_continues() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for ordinal in 0..17 {
        test_append_authz_tuple_record_unfenced(
            &storage,
            &tuple(
                ordinal + 1,
                "document",
                &format!("a-{ordinal:02}"),
                "viewer",
                "user",
                "bob",
                "add",
            ),
        )
        .await
        .unwrap();
    }
    test_append_authz_tuple_record_unfenced(
        &storage,
        &tuple(18, "document", "z-target", "viewer", "user", "alice", "add"),
    )
    .await
    .unwrap();

    let first = list_current_authz_objects_page(
        &storage, 42, "document", "viewer", "user", "alice", "", 18, None, 1,
    )
    .await
    .unwrap();
    assert!(first.object_ids.is_empty());
    assert_eq!(first.tuple_rows_visited, 17);
    let continuation = first.next_object_id.expect("sparse page must continue");

    let second = list_current_authz_objects_page(
        &storage,
        42,
        "document",
        "viewer",
        "user",
        "alice",
        "",
        18,
        Some(&continuation),
        1,
    )
    .await
    .unwrap();
    assert_eq!(second.object_ids, vec!["z-target"]);
    assert!(second.next_object_id.is_none());
}

#[tokio::test]
async fn list_subjects_page_reads_only_the_requested_userset_graph() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for (revision, subject_id) in [(1, "alice"), (2, "bob"), (3, "carol")] {
        test_append_authz_tuple_record_unfenced(
            &storage,
            &tuple(
                revision, "document", "alpha", "viewer", "user", subject_id, "add",
            ),
        )
        .await
        .unwrap();
    }
    for ordinal in 0..64 {
        test_append_authz_tuple_record_unfenced(
            &storage,
            &tuple(
                ordinal + 4,
                "document",
                &format!("unrelated-{ordinal:02}"),
                "viewer",
                "user",
                "nobody",
                "add",
            ),
        )
        .await
        .unwrap();
    }

    let first = list_current_authz_subjects_page(
        &storage,
        42,
        "document",
        "alpha",
        "viewer",
        Some("user"),
        67,
        None,
        2,
    )
    .await
    .unwrap();
    assert_eq!(first.subjects.len(), 2);
    assert_eq!(first.tuple_rows_visited, 3);
    let continuation = first
        .next_subject_position
        .expect("subject page must continue");
    let second = list_current_authz_subjects_page(
        &storage,
        42,
        "document",
        "alpha",
        "viewer",
        Some("user"),
        67,
        Some(&continuation),
        2,
    )
    .await
    .unwrap();
    assert_eq!(second.subjects.len(), 1);
    assert_eq!(second.subjects[0].subject_id, "carol");
    assert!(second.next_subject_position.is_none());
}

#[tokio::test]
async fn authz_tuple_writes_materialize_userset_and_reverse_lookup_segments() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for record in [
        tuple(1, "group", "engineering", "member", "user", "alice", "add"),
        tuple(
            2,
            "folder",
            "platform",
            "viewer",
            "userset",
            "group/engineering#member",
            "add",
        ),
        tuple(
            3,
            "document",
            "alpha",
            "viewer",
            "userset",
            "folder/platform#viewer",
            "add",
        ),
    ] {
        test_append_authz_tuple_record_unfenced(&storage, &record)
            .await
            .unwrap();
    }

    rebuild_authz_materialization_at_revision(&storage, 42, 3, 0)
        .await
        .unwrap();

    let segment = authz_segment::read_latest_authz_tuple_segment(&storage, 42)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(segment.header.generation, 3);
    assert_eq!(segment.records.len(), 3);
    let checkpoint = segment
        .revision_checkpoints
        .last()
        .expect("latest revision checkpoint");
    assert_eq!(checkpoint.tuple_record_count, 3);
    assert_eq!(
        checkpoint.derived_userset_count,
        segment
            .userset_edges
            .iter()
            .filter(|row| row.source == "derived_userset")
            .count() as u64
    );
    assert_eq!(
        checkpoint.list_objects_count,
        segment.list_objects.len() as u64
    );
    assert_eq!(
        checkpoint.list_subjects_count,
        segment.list_subjects.len() as u64
    );
    assert!(segment.userset_edges.iter().any(|row| {
        row.namespace == "document"
            && row.object_id == "alpha"
            && row.relation == "viewer"
            && row.subject_kind == "user"
            && row.subject_id == "alice"
            && row.source == "derived_userset"
    }));
    assert!(segment.list_objects.iter().any(|row| {
        row.namespace == "document"
            && row.relation == "viewer"
            && row.subject_kind == "user"
            && row.subject_id == "alice"
            && row.object_id == "alpha"
    }));
    assert!(segment.list_subjects.iter().any(|row| {
        row.namespace == "document"
            && row.object_id == "alpha"
            && row.relation == "viewer"
            && row.subject_kind == "user"
            && row.subject_id == "alice"
    }));
}

#[tokio::test]
async fn authz_journal_permit_sets_payload_and_segment_fence() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let base_revision = bind_default_document_schema(&storage, 42).await;
    let initial_head = crate::authz_head::read(&storage, 42).await.unwrap().head;
    assert_eq!(initial_head.committed_revision, base_revision as u64);
    assert_eq!(initial_head.schema_revision, base_revision as u64);
    assert_eq!(initial_head.tuple_revision, 0);
    let permit = ready_authz_permit(&storage, 42, "node-a").await;

    append_authz_tuple_record_with_permit(
        &storage,
        &record(base_revision + 1, "add"),
        &permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap();

    let fences = read_authz_journal_payload_fences(&storage, 42)
        .await
        .unwrap();
    assert_eq!(fences, vec![permit.fence_token]);

    materialize_authz_tuple_segment_at_revision(
        &storage,
        42,
        u64::try_from(base_revision + 1).unwrap(),
        permit.fence_token,
    )
    .await
    .unwrap();

    let segment = authz_segment::read_latest_authz_tuple_segment(&storage, 42)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(segment.header.source_fence_token, permit.fence_token);
    let tuple_checkpoint = segment
        .revision_checkpoints
        .last()
        .expect("tuple revision checkpoint");
    assert_eq!(tuple_checkpoint.revision, (base_revision + 1) as u64);
    assert_eq!(tuple_checkpoint.source_fence_token, permit.fence_token);
}

#[tokio::test]
async fn authz_journal_rejects_stale_partition_permit() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let stale = ready_authz_permit(&storage, 42, "node-a").await;
    force_expire_partition_owner_for_node(
        &storage,
        &stale.partition_family,
        &stale.partition_id,
        "node-a",
        250,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap()
    .unwrap();
    let fresh = ready_authz_permit(&storage, 42, "node-b").await;
    assert!(fresh.fence_token > stale.fence_token);

    let rejected = append_authz_tuple_record_with_permit(
        &storage,
        &record(1, "add"),
        &stale,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap_err();
    assert!(rejected.to_string().contains("PartitionNotOwned"));

    append_authz_tuple_record_with_permit(&storage, &record(1, "add"), &fresh, PARTITION_OWNER_KEY)
        .await
        .unwrap();
}

#[tokio::test]
async fn authz_journal_batch_rejects_stale_partition_precondition() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let stale = ready_authz_permit(&storage, 42, "node-a").await;
    let stale_precondition = partition_write_precondition(&storage, &stale, PARTITION_OWNER_KEY)
        .await
        .unwrap();
    force_expire_partition_owner_for_node(
        &storage,
        &stale.partition_family,
        &stale.partition_id,
        "node-a",
        250,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap()
    .unwrap();
    let fresh = ready_authz_permit(&storage, 42, "node-b").await;
    assert!(fresh.fence_token > stale.fence_token);
    let head_snapshot = crate::authz_head::read(&storage, 42).await.unwrap();

    let rejected = append_authz_tuple_record_inner(
        &storage,
        &record(1, "add"),
        stale.fence_token,
        Some(stale_precondition),
        None,
        &head_snapshot,
    )
    .await
    .unwrap_err();
    assert!(
        rejected.to_string().contains("target mismatch")
            || rejected.to_string().contains("generation mismatch")
            || rejected.to_string().contains("precondition failed"),
        "unexpected error: {rejected:?}"
    );
}

#[tokio::test]
async fn authz_journal_rejects_wrong_partition_scope_before_write() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let valid = ready_authz_permit(&storage, 42, "node-a").await;

    let wrong_family = PartitionWritePermit {
        partition_family: "object_metadata".to_string(),
        partition_id: valid.partition_id.clone(),
        owner_node_id: valid.owner_node_id.clone(),
        fence_token: valid.fence_token,
    };
    let rejected = append_authz_tuple_record_with_permit(
        &storage,
        &record(1, "add"),
        &wrong_family,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap_err();
    assert!(
        rejected
            .to_string()
            .contains("does not target this authorization tuple partition")
    );

    let wrong_tenant_partition = PartitionWritePermit {
        partition_family: valid.partition_family.clone(),
        partition_id: hex::encode(authz_partition_id(43)),
        owner_node_id: valid.owner_node_id,
        fence_token: valid.fence_token,
    };
    let rejected = append_authz_tuple_record_with_permit(
        &storage,
        &record(1, "add"),
        &wrong_tenant_partition,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap_err();
    assert!(
        rejected
            .to_string()
            .contains("does not target this authorization tuple partition")
    );
    assert!(
        read_authz_journal_payload_fences(&storage, 42)
            .await
            .unwrap()
            .is_empty(),
        "wrong-scope internal authz writes must fail before stream creation"
    );
}

#[tokio::test]
pub(crate) async fn authz_write_with_permit_allocates_revision_under_fence() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let base_revision = bind_default_document_schema(&storage, 42).await;
    let permit = ready_authz_permit(&storage, 42, "node-a").await;

    let written = write_authz_tuple_with_permit(
        &storage,
        AuthzTupleWrite {
            tenant_id: 42,
            namespace: "document",
            object_id: "beta",
            relation: "editor",
            subject_kind: "user",
            subject_id: "bob",
            caveat_hash: "",
            operation: "add",
            written_by: "tester",
            reason: "test",
        },
        &permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap();
    assert_eq!(written.revision, base_revision + 1);
    let fences = read_authz_journal_payload_fences(&storage, 42)
        .await
        .unwrap();
    assert_eq!(fences[0], permit.fence_token);
}

mod conditional;
mod incremental_materialization;
mod performance;
