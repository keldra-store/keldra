use keldra_authz::{
    AllowedSubject, NamespaceDefinition, ObjectRef, RealmId, RelationDefinition, RewriteRule,
    Schema, Tuple,
};
use tempfile::TempDir;

use super::*;
use crate::{
    AggregateKind, AuthzConsistency, LocalChange, SchemaId, Store, StoreOptions, WatchRetention,
};

fn tenant() -> StorageTenantId {
    StorageTenantId::parse("acme").unwrap()
}

fn scope() -> AuthzScope {
    AuthzScope::new(tenant(), RealmId::parse("documents").unwrap()).unwrap()
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

fn context(command_id: &str, node_id: u16, position: u64) -> AuthzRealmMutationContext {
    AuthzRealmMutationContext {
        command_id: command_id.into(),
        active_placement_log_id: PlacementLogId { term: 3, index: 9 },
        serving_fence_term: 3,
        source_id: SourceId {
            node_id,
            source_epoch: [node_id as u8; 32],
        },
        source_journal_position: position,
    }
}

async fn stores() -> (TempDir, Store, Store) {
    let root = tempfile::tempdir().unwrap();
    let coordinator = Store::open(StoreOptions::new(root.path().join("coordinator"), 1))
        .await
        .unwrap();
    let replica = Store::open(StoreOptions::new(root.path().join("replica"), 2))
        .await
        .unwrap();
    (root, coordinator, replica)
}

fn publish(repository: &AuthzRepository) -> super::super::PublishedSchema {
    repository
        .publish_schema(super::super::PublishSchemaRequest {
            storage_tenant: tenant(),
            schema_id: SchemaId::parse("documents").unwrap(),
            schema: schema(),
            expected_revision: Some(AuthzRevision::ZERO),
        })
        .unwrap()
}

fn bind_request(schema_ref: super::super::SchemaRef) -> BindSchemaRequest {
    BindSchemaRequest {
        scope: scope(),
        schema_ref,
        expected_generation: Some(0),
        expected_revision: Some(AuthzRevision(1)),
    }
}

fn tuple_request(operation: &str, revision: u64, document: &str, user: &str) -> TupleBatchRequest {
    TupleBatchRequest {
        scope: scope(),
        principal: principal("writer"),
        expected_revision: Some(AuthzRevision(revision)),
        expected_binding_generation: 1,
        operation_id: Some(operation.into()),
        mutations: vec![TupleMutation {
            kind: TupleMutationKind::Add,
            tuple: tuple(document, user),
        }],
    }
}

#[tokio::test]
async fn complete_realm_mutations_apply_to_a_second_store_and_replay_exactly() {
    let (_root, coordinator, replica) = stores().await;
    let coordinator_repository = coordinator.authz();
    let replica_repository = replica.authz();
    let published = publish(&coordinator_repository);

    let coordinated_binding = coordinator_repository
        .coordinate_bind_schema_mutation(
            bind_request(published.schema_ref.clone()),
            context("bind-documents", 1, 1),
        )
        .unwrap();
    let binding_mutation = coordinated_binding.mutation.unwrap();
    assert_eq!(binding_mutation.stamp.predecessor_revision, None);
    assert_eq!(replica.local_watch_status().unwrap().tail, 0);
    let applied = replica_repository
        .apply_authz_realm_mutation_replica(&binding_mutation)
        .unwrap();
    assert!(!applied.replayed);
    assert_eq!(replica.local_watch_status().unwrap().tail, 0);
    assert_eq!(
        replica_repository
            .get_schema(&tenant(), &published.schema_ref)
            .unwrap(),
        Some(canonical_schema(schema(), AuthorizationLimits::default()).unwrap())
    );

    let request = tuple_request("grant-alice", 2, "one", "alice");
    let coordinated_tuple = coordinator_repository
        .coordinate_tuple_mutation(request.clone(), context("grant-alice", 1, 2))
        .unwrap();
    let tuple_mutation = coordinated_tuple.mutation.clone().unwrap();
    assert_eq!(
        tuple_mutation.stamp.predecessor_revision,
        Some(AuthzRevision(2))
    );
    let applied = replica_repository
        .apply_authz_realm_mutation_replica(&tuple_mutation)
        .unwrap();
    assert!(!applied.replayed);
    assert_eq!(replica.local_watch_status().unwrap().tail, 0);

    let coordinator_snapshot = coordinator_repository
        .realm_snapshot(&scope(), AuthzConsistency::Latest)
        .unwrap();
    let replica_snapshot = replica_repository
        .realm_snapshot(&scope(), AuthzConsistency::Latest)
        .unwrap();
    assert_eq!(replica_snapshot, coordinator_snapshot);
    assert_eq!(replica_snapshot.tuples, vec![tuple("one", "alice")]);

    let sequence = replica_repository.db.latest_sequence_number();
    let replay = replica_repository
        .apply_authz_realm_mutation_replica(&tuple_mutation)
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replica_repository.db.latest_sequence_number(), sequence);

    let coordinator_replay = coordinator_repository
        .coordinate_tuple_mutation(request, context("grant-alice", 1, 99))
        .unwrap();
    assert!(matches!(
        coordinator_replay.result,
        CoordinatedAuthzRealmResult::Tuples(TupleBatchReceipt { replayed: true, .. })
    ));
    assert_eq!(coordinator_replay.mutation, Some(tuple_mutation));
}

#[tokio::test]
async fn journaled_tuple_mutation_uses_the_actual_atomic_source_position() {
    let (_root, coordinator, _replica) = stores().await;
    let repository = coordinator.authz();
    let published = publish(&repository);
    repository
        .coordinate_bind_schema_mutation(
            bind_request(published.schema_ref),
            context("bind-documents", 1, 7),
        )
        .unwrap();
    let request = tuple_request("journaled-grant", 2, "one", "alice");
    let placement = PlacementLogId { term: 8, index: 21 };

    let coordinated = coordinator
        .coordinate_journaled_authz_tuple_mutation(41, request.clone(), placement, 8)
        .await
        .unwrap();
    let mutation = coordinated.mutation.as_ref().unwrap();
    let status = coordinator.local_watch_status().unwrap();
    assert_eq!(status.tail, 1);
    assert_eq!(mutation.stamp.source_id, status.source_id);
    assert_eq!(mutation.stamp.source_journal_position, 1);
    let changes = coordinator.scan_local_changes(0, 10).unwrap();
    let LocalChange::AggregateChanged(change) = &changes[0] else {
        panic!("expected a Zanzibar aggregate invalidation");
    };
    assert_eq!(change.aggregate_kind, AggregateKind::ZanzibarRealm);
    assert_eq!(change.revision, 3);
    let mut expected_key = 41_u64.to_be_bytes().to_vec();
    expected_key.extend_from_slice(&scope().handoff_order_key().unwrap());
    assert_eq!(change.aggregate_key, expected_key);

    let replay = coordinator
        .coordinate_journaled_authz_tuple_mutation(41, request, placement, 8)
        .await
        .unwrap();
    assert!(matches!(
        replay.result,
        CoordinatedAuthzRealmResult::Tuples(TupleBatchReceipt { replayed: true, .. })
    ));
    assert_eq!(coordinator.local_watch_status().unwrap().tail, 1);
}

#[tokio::test]
async fn journaled_authz_capacity_is_typed_and_retry_wakes_without_partial_binding() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreOptions::new(root.path(), 1)
            .with_watch_retention(WatchRetention::new(1, 1024 * 1024).unwrap()),
    )
    .await
    .unwrap();
    let placement = PlacementLogId { term: 8, index: 21 };
    let publish_request = super::super::PublishSchemaRequest {
        storage_tenant: tenant(),
        schema_id: SchemaId::parse("capacity").unwrap(),
        schema: schema(),
        expected_revision: Some(AuthzRevision::ZERO),
    };
    let published = store
        .coordinate_journaled_authz_schema_publication(41, publish_request.clone(), placement, 8)
        .await
        .unwrap();
    assert_eq!(store.local_watch_status().unwrap().tail, 1);
    let binding = bind_request(published.result.schema_ref.clone());

    assert!(matches!(
        store
            .coordinate_journaled_authz_schema_binding(41, binding.clone(), placement, 8)
            .await,
        Err(AuthzStoreError::SourceJournalCapacity)
    ));
    assert_eq!(store.local_watch_status().unwrap().tail, 1);
    assert!(store.authz().get_binding(&scope()).unwrap().is_none());

    let retry_store = store.clone();
    let mut waiting = tokio::spawn(async move {
        loop {
            match retry_store
                .coordinate_journaled_authz_schema_binding(41, binding.clone(), placement, 8)
                .await
            {
                Err(AuthzStoreError::SourceJournalCapacity) => {
                    retry_store.wait_for_mutation_capacity().await;
                }
                outcome => break outcome,
            }
        }
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut waiting)
            .await
            .is_err(),
        "the exact journal bound must hold the authorization mutation"
    );
    assert_eq!(store.local_watch_status().unwrap().tail, 1);
    assert!(store.authz().get_binding(&scope()).unwrap().is_none());

    let replay = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        store.coordinate_journaled_authz_schema_publication(41, publish_request, placement, 8),
    )
    .await
    .expect("capacity waiting must release the journal and authorization locks")
    .unwrap();
    assert!(replay.result.replayed);

    store
        .advance_source_journal_reference_safe_through(1)
        .await
        .unwrap();
    let bound = tokio::time::timeout(std::time::Duration::from_secs(2), waiting)
        .await
        .expect("the capacity notification must wake the authorization retry")
        .unwrap()
        .unwrap();
    assert!(matches!(
        bound.result,
        CoordinatedAuthzRealmResult::Bound(_)
    ));
    assert!(store.authz().get_binding(&scope()).unwrap().is_some());
    let status = store.local_watch_status().unwrap();
    assert_eq!(status.tail, 2);
    assert_eq!(status.retention_floor, 1);
    assert_eq!(status.retained_entries, 1);
}

#[tokio::test]
async fn replicas_reject_gaps_stale_mutations_siblings_and_tampering() {
    let (root, coordinator, replica) = stores().await;
    let coordinator_repository = coordinator.authz();
    let published = publish(&coordinator_repository);
    let bind = coordinator_repository
        .coordinate_bind_schema_mutation(
            bind_request(published.schema_ref),
            context("bind-documents", 1, 1),
        )
        .unwrap()
        .mutation
        .unwrap();
    let first = coordinator_repository
        .coordinate_tuple_mutation(
            tuple_request("grant-alice", 2, "one", "alice"),
            context("grant-alice", 1, 2),
        )
        .unwrap()
        .mutation
        .unwrap();
    let second = coordinator_repository
        .coordinate_tuple_mutation(
            tuple_request("grant-bob", 3, "two", "bob"),
            context("grant-bob", 1, 3),
        )
        .unwrap()
        .mutation
        .unwrap();

    let replica_repository = replica.authz();
    replica_repository
        .apply_authz_realm_mutation_replica(&bind)
        .unwrap();
    assert!(matches!(
        replica_repository.apply_authz_realm_mutation_replica(&second),
        Err(AuthzStoreError::RealmMutationLineageGap {
            current: Some(AuthzRevision(2)),
            predecessor: Some(AuthzRevision(3)),
        })
    ));
    replica_repository
        .apply_authz_realm_mutation_replica(&first)
        .unwrap();

    let mut sibling = first.clone();
    sibling.command_id = "grant-alice-sibling".into();
    sibling.input_fingerprint = [17; 32];
    sibling.set_computed_fingerprint();
    sibling.validate().unwrap();
    assert!(matches!(
        replica_repository.apply_authz_realm_mutation_replica(&sibling),
        Err(AuthzStoreError::RealmMutationSibling {
            predecessor: Some(AuthzRevision(2)),
        })
    ));

    replica_repository
        .apply_authz_realm_mutation_replica(&second)
        .unwrap();
    assert!(matches!(
        replica_repository.apply_authz_realm_mutation_replica(&bind),
        Err(AuthzStoreError::RealmMutationStale { .. })
    ));

    let mut tampered = second;
    tampered.input_fingerprint[0] ^= 1;
    assert!(matches!(
        tampered.validate(),
        Err(AuthzStoreError::InvalidRealmMutation(_))
    ));

    let empty = Store::open(StoreOptions::new(root.path().join("empty"), 3))
        .await
        .unwrap();
    assert!(matches!(
        empty.authz().apply_authz_realm_mutation_replica(&first),
        Err(AuthzStoreError::RealmMutationLineageGap {
            current: None,
            predecessor: Some(AuthzRevision(2)),
        })
    ));
}

#[test]
fn released_unstamped_binding_json_decodes_as_a_committed_baseline() {
    let binding = RealmBinding {
        scope: scope(),
        schema_ref: super::super::SchemaRef {
            schema_id: SchemaId::parse("documents").unwrap(),
            schema_revision: 1,
            schema_digest: super::super::SchemaDigest([4; 32]),
        },
        generation: 1,
        authz_revision: AuthzRevision(7),
        tuple_count: 0,
    };
    let released = serde_json::to_vec(&binding).unwrap();
    let decoded: StoredRealmBinding = serde_json::from_slice(&released).unwrap();
    assert_eq!(decoded.binding, binding);
    assert_eq!(decoded.mutation_stamp, None);
    assert_eq!(decoded.aggregate_revision, None);
}
