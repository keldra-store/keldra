use keldra_authz::{AllowedSubject, NamespaceDefinition, RelationDefinition, Schema};

use super::*;
use crate::{
    AuthzScope, BindSchemaRequest, PlacementLogId, SchemaId, SourceId, Store, StoreOptions,
};

fn tenant() -> StorageTenantId {
    StorageTenantId::parse("tenant").unwrap()
}

fn schema(namespace: &str) -> Schema {
    Schema::new([NamespaceDefinition::new(
        namespace,
        [RelationDefinition::direct(
            "owner",
            [AllowedSubject::any_object("application")],
        )],
    )])
}

fn request(id: &str, schema: Schema, expected: u64) -> PublishSchemaRequest {
    PublishSchemaRequest {
        storage_tenant: tenant(),
        schema_id: SchemaId::parse(id).unwrap(),
        schema,
        expected_revision: Some(AuthzRevision(expected)),
    }
}

fn context(command: &str, position: u64) -> AuthzRealmMutationContext {
    AuthzRealmMutationContext {
        command_id: command.into(),
        active_placement_log_id: PlacementLogId { term: 3, index: 8 },
        serving_fence_term: 3,
        source_id: SourceId {
            node_id: 1,
            source_epoch: [7; 32],
        },
        source_journal_position: position,
    }
}

async fn stores() -> (tempfile::TempDir, Store, Store) {
    let root = tempfile::tempdir().unwrap();
    let source = Store::open(StoreOptions::new(root.path().join("source"), 1))
        .await
        .unwrap();
    let replica = Store::open(StoreOptions::new(root.path().join("replica"), 2))
        .await
        .unwrap();
    (root, source, replica)
}

#[tokio::test]
async fn unbound_revisions_replicate_and_digest_replay_is_preserved() {
    let (_root, source, replica) = stores().await;
    let source = source.authz();
    let replica = replica.authz();

    let first = source
        .coordinate_schema_publication(
            request("documents", schema("document"), 0),
            context("publish-1", 1),
        )
        .unwrap();
    assert!(!first.result.replayed);
    let mutation = first.mutation.unwrap();
    let applied = replica.apply_schema_publication_replica(&mutation).unwrap();
    assert!(!applied.replayed);
    assert_eq!(
        replica
            .get_schema(&tenant(), &first.result.schema_ref)
            .unwrap(),
        Some(schema("document"))
    );

    let replay = replica
        .publish_schema(request("documents", schema("document"), 99))
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.schema_ref, first.result.schema_ref);
    assert!(
        replica
            .apply_schema_publication_replica(&mutation)
            .unwrap()
            .replayed
    );

    let second = source
        .coordinate_schema_publication(
            request("documents", schema("document_v2"), 1),
            context("publish-2", 2),
        )
        .unwrap();
    assert_eq!(second.result.schema_ref.schema_revision, 2);
    replica
        .apply_schema_publication_replica(second.mutation.as_ref().unwrap())
        .unwrap();
    assert_eq!(
        source.export_authz_schema_catalogue(&tenant()).unwrap(),
        replica.export_authz_schema_catalogue(&tenant()).unwrap()
    );
}

#[tokio::test]
async fn digest_retry_returns_the_exact_retained_publication_lineage() {
    let (_root, source, _replica) = stores().await;
    let source = source.authz();
    let request = request("documents", schema("document"), 0);
    let first = source
        .coordinate_schema_publication(request.clone(), context("publish-original", 1))
        .unwrap();
    let original = first.mutation.clone().unwrap();

    let retry = source
        .coordinate_schema_publication(request, context("different-retry-context", 99))
        .unwrap();
    assert!(retry.result.replayed);
    assert_eq!(retry.mutation, Some(original.clone()));

    let catalogue = source
        .export_authz_schema_catalogue(&tenant())
        .unwrap()
        .unwrap();
    assert_eq!(
        catalogue.schemas[0].publication_mutation.as_ref(),
        Some(&original)
    );
}

#[tokio::test]
async fn conflicting_schema_publication_replay_fails_closed() {
    let (_root, source, replica) = stores().await;
    let original = source
        .authz()
        .coordinate_schema_publication(
            request("documents", schema("document"), 0),
            context("publish-original", 1),
        )
        .unwrap()
        .mutation
        .unwrap();
    let replica = replica.authz();
    replica.apply_schema_publication_replica(&original).unwrap();

    let mut sibling = original;
    sibling.command_id = "publish-sibling".into();
    sibling.set_computed_fingerprint();
    sibling.validate().unwrap();
    assert!(matches!(
        replica.apply_schema_publication_replica(&sibling),
        Err(AuthzStoreError::RealmMutationConflict)
    ));
}

#[tokio::test]
async fn legacy_schema_revision_decodes_and_survives_catalogue_handoff() {
    let (_root, source, replica) = stores().await;
    let source = source.authz();
    let published = source
        .publish_schema(request("documents", schema("document"), 0))
        .unwrap();
    let raw = source
        .db
        .get_cf(
            source.cf(CF_AUTHZ_SCHEMAS).unwrap(),
            schema_revision_key(&tenant(), &published.schema_ref),
        )
        .unwrap()
        .unwrap();
    assert!(
        !String::from_utf8(raw)
            .unwrap()
            .contains("publication_mutation")
    );

    let catalogue = source
        .export_authz_schema_catalogue(&tenant())
        .unwrap()
        .unwrap();
    assert_eq!(catalogue.schemas[0].publication_mutation, None);
    let replica = replica.authz();
    replica
        .install_quorum_reconciled_authz_schema_catalogue(&tenant(), Some(&catalogue))
        .unwrap();
    assert_eq!(
        replica.export_authz_schema_catalogue(&tenant()).unwrap(),
        Some(catalogue)
    );
}

#[tokio::test]
async fn exact_catalogue_install_removes_minority_entries_and_rebuilds_latest_keys() {
    let (_root, source, replica) = stores().await;
    let source = source.authz();
    let replica = replica.authz();

    source
        .publish_schema(request("documents", schema("document"), 0))
        .unwrap();
    source
        .publish_schema(request("notes", schema("note"), 1))
        .unwrap();
    source
        .publish_schema(request("documents", schema("document_v2"), 2))
        .unwrap();
    replica
        .publish_schema(request("minority", schema("rogue"), 0))
        .unwrap();

    let winner = source
        .export_authz_schema_catalogue(&tenant())
        .unwrap()
        .unwrap();
    assert_eq!(winner.schemas.len(), 3);
    let applied = replica
        .install_quorum_reconciled_authz_schema_catalogue(&tenant(), Some(&winner))
        .unwrap();
    assert_eq!(applied.revision, AuthzRevision(3));
    assert_eq!(applied.schema_count, 3);
    assert_eq!(
        replica.export_authz_schema_catalogue(&tenant()).unwrap(),
        Some(winner)
    );

    let next = replica
        .publish_schema(request("documents", schema("document_v3"), 3))
        .unwrap();
    assert_eq!(next.schema_ref.schema_revision, 3);
    assert_eq!(next.authz_revision, AuthzRevision(4));
}

#[tokio::test]
async fn catalogue_revision_tracks_realm_changes_without_copying_realm_data() {
    let (_root, source, _replica) = stores().await;
    let repository = source.authz();
    let published = repository
        .publish_schema(request("documents", schema("document"), 0))
        .unwrap();
    let scope =
        AuthzScope::new(tenant(), keldra_authz::RealmId::parse("documents").unwrap()).unwrap();
    repository
        .bind_schema(BindSchemaRequest {
            scope,
            schema_ref: published.schema_ref,
            expected_generation: Some(0),
            expected_revision: Some(AuthzRevision(1)),
        })
        .unwrap();

    let catalogue = repository
        .export_authz_schema_catalogue(&tenant())
        .unwrap()
        .unwrap();
    assert_eq!(catalogue.authz_revision, AuthzRevision(2));
    assert_eq!(catalogue.schemas.len(), 1);
}

#[tokio::test]
async fn replica_rejects_a_publication_with_a_missing_predecessor() {
    let (_root, source, replica) = stores().await;
    let source = source.authz();
    let replica = replica.authz();
    source
        .publish_schema(request("first", schema("first"), 0))
        .unwrap();
    let second = source
        .coordinate_schema_publication(
            request("second", schema("second"), 1),
            context("publish-second", 2),
        )
        .unwrap()
        .mutation
        .unwrap();

    assert!(matches!(
        replica.apply_schema_publication_replica(&second),
        Err(AuthzStoreError::RealmMutationLineageGap { .. })
    ));
}

#[tokio::test]
async fn absent_quorum_winner_clears_only_the_tenant_catalogue() {
    let (_root, _source, replica) = stores().await;
    let replica = replica.authz();
    replica
        .publish_schema(request("documents", schema("document"), 0))
        .unwrap();
    let applied = replica
        .install_quorum_reconciled_authz_schema_catalogue(&tenant(), None)
        .unwrap();
    assert_eq!(applied.revision, AuthzRevision::ZERO);
    assert_eq!(applied.schema_count, 0);
    assert_eq!(
        replica.export_authz_schema_catalogue(&tenant()).unwrap(),
        None
    );
}
