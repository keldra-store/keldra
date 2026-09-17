use keldra_api::v1::{
    CreateIndexRequest, IndexField, IndexFieldCapability, IndexFieldCardinality,
    IndexSpecification, KeywordIndexField, TypedJsonIndexSpec, index_field,
};

use super::*;

fn replay_barrier(next: u64) -> super::super::events::IndexBarrier {
    use super::super::events::{AtomicProgramWatermark, IndexBarrier, IndexSourceCursor};
    use keldra_consensus::NodeId;
    use keldra_store::{PlacementLogId, SourceId};
    IndexBarrier {
        fence: PlacementLogId { term: 1, index: 12 },
        atomic: AtomicProgramWatermark::new(None, None, 0),
        sources: [(
            NodeId(1),
            IndexSourceCursor {
                source: SourceId {
                    node_id: 1,
                    source_epoch: [9; 32],
                },
                next_offset: next,
            },
        )]
        .into_iter()
        .collect(),
    }
}

#[tokio::test]
async fn persisted_catalog_checkpoint_does_not_certify_transient_empty_inventory() {
    use keldra_store::{DefinitionCheckpoint, DefinitionConsumerKind, Store, StoreOptions};
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(directory.path(), 1))
        .await
        .unwrap();
    let barrier = replay_barrier(52);
    let cursor = barrier.sources.values().next().unwrap();
    let checkpoint = DefinitionCheckpoint {
        consumer_kind: DefinitionConsumerKind::V1IndexCatalog,
        source_id: cursor.source,
        next_offset: cursor.next_offset,
        observed_fence: barrier.fence,
    };
    store
        .apply_definition_assignment_page(&[], &checkpoint)
        .unwrap();
    assert_eq!(
        store
            .definition_checkpoint(DefinitionConsumerKind::V1IndexCatalog, 1)
            .unwrap(),
        Some(checkpoint)
    );
    let catalog = IndexCatalog::default();
    assert!(catalog.physical_snapshot().unwrap().recipes.is_empty());
    // This is exactly the inventory accessor used by retention, not a
    // separately mirrored bootstrap boolean.
    assert!(catalog.retention_snapshot().unwrap().is_none());
    let generation = catalog.catalog_replay_generation().unwrap();
    assert!(
        catalog
            .complete_catalog_replay(generation, &barrier)
            .unwrap()
    );
    let ready = catalog.retention_snapshot().unwrap().unwrap();
    assert!(ready.physical.recipes.is_empty());
    assert_eq!(ready.barrier, barrier);
    catalog.begin_catalog_replay().unwrap();
    assert!(catalog.retention_snapshot().unwrap().is_none());
    // Existing disk state deliberately remains unchanged on a retry epoch.
    assert_eq!(
        store
            .definition_checkpoint(DefinitionConsumerKind::V1IndexCatalog, 1)
            .unwrap(),
        Some(checkpoint)
    );
}

#[test]
fn successful_logical_catalog_mutation_invalidates_snapshot_paired_replay_proof() {
    let catalog = IndexCatalog::default();
    catalog.upsert(definition(7, 9, 10)).unwrap();
    let generation = catalog.catalog_replay_generation().unwrap();
    let barrier = replay_barrier(47);
    assert!(
        catalog
            .complete_catalog_replay(generation, &barrier)
            .unwrap()
    );
    let old = catalog.retention_snapshot().unwrap().unwrap();
    catalog.upsert(definition(7, 9, 11)).unwrap();
    // Aliases change logical inventory without necessarily changing the
    // physical recipe snapshot. The mutable generation is the CAS token.
    assert!(Arc::ptr_eq(
        &old.physical,
        &catalog.physical_snapshot().unwrap()
    ));
    assert!(catalog.catalog_replay_generation().unwrap() > generation);
    assert!(catalog.retention_snapshot().unwrap().is_none());
    assert!(
        !catalog
            .complete_catalog_replay(generation, &replay_barrier(52))
            .unwrap()
    );
    assert!(catalog.retention_snapshot().unwrap().is_none());
    let new_generation = catalog.catalog_replay_generation().unwrap();
    assert!(
        catalog
            .complete_catalog_replay(new_generation, &replay_barrier(52))
            .unwrap()
    );
    let new = catalog.retention_snapshot().unwrap().unwrap();
    assert_eq!(new.generation, new_generation);
    assert_eq!(new.barrier, replay_barrier(52));
    // Retained old proof owners remain immutable and charged until dropped.
    assert_eq!(old.generation, generation);
    assert_eq!(old.barrier, barrier);
}

#[test]
fn catalog_replay_proof_rejects_unclear_captured_atomic_barrier_without_losing_old_pair() {
    use super::super::events::AtomicProgramWatermark;
    let catalog = IndexCatalog::default();
    let generation = catalog.catalog_replay_generation().unwrap();
    let completed = replay_barrier(47);
    assert!(
        catalog
            .complete_catalog_replay(generation, &completed)
            .unwrap()
    );
    let old = catalog.retention_snapshot().unwrap().unwrap();
    let mut pending = replay_barrier(52);
    pending.atomic = AtomicProgramWatermark::new(Some(15), Some(14), 1);
    assert_eq!(
        catalog
            .complete_catalog_replay(generation, &pending)
            .unwrap_err()
            .code(),
        tonic::Code::DataLoss
    );
    assert!(Arc::ptr_eq(
        &old,
        &catalog.retention_snapshot().unwrap().unwrap()
    ));
    // The API inspects only its captured completed barrier, not later live
    // authority, so a new program cannot invalidate an older completed pair.
    assert_eq!(old.barrier, completed);
}

fn definition(tenant_id: u64, bucket_id: u64, index_id: u64) -> CatalogDefinition {
    CatalogDefinition::new(
        tenant_id,
        bucket_id,
        1,
        StoredIndexDefinition::create(
            "tenant".into(),
            CreateIndexRequest {
                bucket: "bucket".into(),
                name: format!("index-{index_id}"),
                path_prefix: String::new(),
                content_type: String::new(),
                specification: Some(IndexSpecification {
                    specification: Some(
                        keldra_api::v1::index_specification::Specification::TypedJson(
                            TypedJsonIndexSpec {
                                fields: vec![keyword_field("value", "/value")],
                                physical_order: Vec::new(),
                            },
                        ),
                    ),
                }),
                command_id: format!("create-{index_id}"),
                result_authorization: Some(keldra_api::v1::IndexResultAuthorization {
                    policy: Some(
                        keldra_api::v1::index_result_authorization::Policy::Application(
                            keldra_api::v1::ApplicationIndexResultAuthorization {},
                        ),
                    ),
                }),
            },
            index_id,
        )
        .unwrap(),
    )
    .unwrap()
}

fn wide_typed_definition(index_id: u64) -> CatalogDefinition {
    let fields = (0..32)
        .map(|field| IndexField {
            name: format!("public_field_name_{field}"),
            json_pointer: format!("/payload/value_{field}"),
            cardinality: IndexFieldCardinality::Single as i32,
            capabilities: vec![IndexFieldCapability::Exact as i32],
            field_type: Some(index_field::FieldType::Keyword(KeywordIndexField {})),
        })
        .collect();
    typed_definition(index_id, fields)
}

fn typed_definition(index_id: u64, fields: Vec<IndexField>) -> CatalogDefinition {
    CatalogDefinition::new(
        1,
        2,
        1,
        StoredIndexDefinition::create(
            "tenant".into(),
            CreateIndexRequest {
                bucket: "bucket".into(),
                name: format!("wide-{index_id}"),
                path_prefix: "objects/".into(),
                content_type: "application/json".into(),
                specification: Some(IndexSpecification {
                    specification: Some(
                        keldra_api::v1::index_specification::Specification::TypedJson(
                            TypedJsonIndexSpec {
                                fields,
                                physical_order: Vec::new(),
                            },
                        ),
                    ),
                }),
                command_id: format!("create-{index_id}"),
                result_authorization: Some(keldra_api::v1::IndexResultAuthorization {
                    policy: Some(
                        keldra_api::v1::index_result_authorization::Policy::Application(
                            keldra_api::v1::ApplicationIndexResultAuthorization {},
                        ),
                    ),
                }),
            },
            index_id,
        )
        .unwrap(),
    )
    .unwrap()
}

fn keyword_field(name: &str, pointer: &str) -> IndexField {
    IndexField {
        name: name.into(),
        json_pointer: pointer.into(),
        cardinality: IndexFieldCardinality::Single as i32,
        capabilities: vec![IndexFieldCapability::Exact as i32],
        field_type: Some(index_field::FieldType::Keyword(KeywordIndexField {})),
    }
}

fn explicit_rebuild(
    mut definition: CatalogDefinition,
    version: u64,
    accepted_at: u64,
) -> CatalogDefinition {
    definition.stored = definition
        .stored
        .with_explicit_rebuild(accepted_at)
        .unwrap();
    definition.object_version = version;
    definition
}

#[test]
fn accepted_explicit_rebuild_changes_generation_and_notifies_existing_producer() {
    let catalog = IndexCatalog::default();
    let initial = definition(1, 2, 9);
    let family = initial.projection_family_identity();
    catalog.upsert(initial.clone()).unwrap();
    let before = catalog.physical_snapshot().unwrap();
    let original_generation = before.recipes[0].physical_generation;
    assert_eq!(
        original_generation,
        family_physical_generation(&before.recipes[0])
    );
    let mut notices = catalog.subscribe();

    catalog.upsert(explicit_rebuild(initial, 2, 1_000)).unwrap();

    let after = catalog.physical_snapshot().unwrap();
    assert_eq!(after.recipes.len(), 1);
    assert_eq!(after.recipes[0].family, family);
    assert_ne!(after.recipes[0].physical_generation, original_generation);
    assert!(notices.try_recv().unwrap().physical_changed);
    assert_eq!(before.recipes[0].physical_generation, original_generation);
    assert!(!Arc::ptr_eq(&before.recipes[0], &after.recipes[0]));
}

#[test]
fn explicit_rebuild_replay_and_restart_derive_the_same_shared_generation() {
    let first = explicit_rebuild(definition(1, 2, 9), 2, 1_000);
    let alias = explicit_rebuild(definition(1, 2, 10), 3, 1_000);
    let catalog = IndexCatalog::default();
    catalog.upsert(first.clone()).unwrap();
    catalog.upsert(alias.clone()).unwrap();
    let before = catalog.physical_snapshot().unwrap();
    let mut notices = catalog.subscribe();

    catalog.upsert(first.clone()).unwrap();
    catalog.upsert(alias.clone()).unwrap();

    assert!(notices.try_recv().is_err());
    assert!(Arc::ptr_eq(&before, &catalog.physical_snapshot().unwrap()));
    let restarted = IndexCatalog::default();
    restarted.upsert(alias).unwrap();
    restarted.upsert(first).unwrap();
    let after = restarted.physical_snapshot().unwrap();
    assert_eq!(after.recipes.len(), 1);
    assert_eq!(
        after.recipes[0].physical_generation,
        before.recipes[0].physical_generation
    );
}

#[test]
fn alias_rebuild_replaces_one_shared_family_and_each_alias_intent_matters() {
    let catalog = IndexCatalog::default();
    let first = definition(1, 2, 9);
    let alias = definition(1, 2, 10);
    catalog.upsert(first.clone()).unwrap();
    catalog.upsert(alias.clone()).unwrap();
    let original = catalog.physical_snapshot().unwrap().recipes[0].physical_generation;
    catalog.upsert(explicit_rebuild(first, 2, 1_000)).unwrap();
    let first_rebuild = catalog.physical_snapshot().unwrap().recipes[0].physical_generation;
    assert_ne!(first_rebuild, original);

    // Equal server timestamps on different stable aliases are different intent.
    catalog.upsert(explicit_rebuild(alias, 2, 1_000)).unwrap();
    let (_, _, bindings, recipes, _) = catalog.snapshot().unwrap();
    assert_eq!(bindings.len(), 2);
    assert_eq!(recipes.len(), 1);
    assert!(
        bindings
            .iter()
            .all(|binding| binding.family == recipes[0].family)
    );
    assert_ne!(recipes[0].physical_generation, first_rebuild);
    assert_ne!(recipes[0].physical_generation, original);
}

#[test]
fn normal_definition_versions_and_public_field_names_preserve_rebuild_intent() {
    let catalog = IndexCatalog::default();
    let first = explicit_rebuild(
        typed_definition(9, vec![keyword_field("old_name", "/value")]),
        2,
        1_000,
    );
    catalog.upsert(first.clone()).unwrap();
    let generation = catalog.physical_snapshot().unwrap().recipes[0].physical_generation;
    let mut new_version = first;
    new_version.object_version = 3;
    catalog.upsert(new_version).unwrap();
    assert_eq!(
        catalog.physical_snapshot().unwrap().recipes[0].physical_generation,
        generation
    );

    let renamed = explicit_rebuild(
        typed_definition(9, vec![keyword_field("new_name", "/value")]),
        4,
        1_000,
    );
    catalog.upsert(renamed).unwrap();
    assert_eq!(
        catalog.physical_snapshot().unwrap().recipes[0].physical_generation,
        generation
    );
    let mut later = explicit_rebuild(
        typed_definition(9, vec![keyword_field("new_name", "/value")]),
        5,
        3_601_000,
    );
    catalog.upsert(later.clone()).unwrap();
    assert_ne!(
        catalog.physical_snapshot().unwrap().recipes[0].physical_generation,
        generation
    );
    later.object_version = 6;
    let repeated = catalog.physical_snapshot().unwrap().recipes[0].physical_generation;
    catalog.upsert(later).unwrap();
    assert_eq!(
        catalog.physical_snapshot().unwrap().recipes[0].physical_generation,
        repeated
    );
}

#[test]
fn removing_rebuilt_alias_rederives_generation_without_a_hidden_watermark() {
    let catalog = IndexCatalog::default();
    let alias = definition(1, 2, 10);
    catalog.upsert(alias.clone()).unwrap();
    let original = catalog.physical_snapshot().unwrap().recipes[0].physical_generation;
    catalog
        .upsert(explicit_rebuild(definition(1, 2, 9), 2, 1_000))
        .unwrap();
    assert_ne!(
        catalog.physical_snapshot().unwrap().recipes[0].physical_generation,
        original
    );
    let mut notices = catalog.subscribe();
    // Exercise the synchronous removal used by catalog reconciliation too.
    catalog.remove(1, 2, 9).unwrap();
    assert!(notices.try_recv().unwrap().physical_changed);
    assert_eq!(
        catalog.physical_snapshot().unwrap().recipes[0].physical_generation,
        original
    );
    let restarted = IndexCatalog::default();
    restarted.upsert(alias).unwrap();
    assert_eq!(
        restarted.physical_snapshot().unwrap().recipes[0].physical_generation,
        original
    );
}

#[tokio::test]
async fn versioned_alias_delete_rederives_surviving_family_rebuild_generation() {
    let catalog = IndexCatalog::default();
    catalog.upsert(definition(1, 2, 10)).unwrap();
    let original = catalog.physical_snapshot().unwrap().recipes[0].physical_generation;
    catalog
        .upsert(explicit_rebuild(definition(1, 2, 9), 2, 1_000))
        .unwrap();
    catalog
        .delete_wait(
            CatalogIdentity {
                tenant_id: 1,
                bucket_id: 2,
                index_id: 9,
            },
            3,
        )
        .await
        .unwrap();
    assert_eq!(
        catalog.physical_snapshot().unwrap().recipes[0].physical_generation,
        original
    );
}

#[test]
fn family_rebuild_map_contains_and_charges_only_nonempty_intents() {
    let catalog = IndexCatalog::default();
    for index_id in 1..=128 {
        catalog.upsert(definition(1, 2, index_id)).unwrap();
    }
    {
        let state = catalog.inner.lock().unwrap();
        assert_eq!(state.recipes.len(), 1);
        assert!(
            state
                .recipes
                .values()
                .next()
                .unwrap()
                .rebuild_intents
                .is_empty()
        );
    }
    catalog
        .upsert(explicit_rebuild(definition(1, 2, 9), 2, 1_000))
        .unwrap();
    let state = catalog.inner.lock().unwrap();
    let recipe = state.recipes.values().next().unwrap();
    assert_eq!(recipe.rebuild_intents, BTreeMap::from([(9, 1_000)]));
    let charged = catalog_recipe_state_resident_bytes(recipe).unwrap();
    let mut without_intents = recipe.clone();
    without_intents.rebuild_intents.clear();
    assert_eq!(
        charged - catalog_recipe_state_resident_bytes(&without_intents).unwrap(),
        std::mem::size_of::<(u64, u64)>() + 64
    );
}

#[test]
fn rejected_rebuild_admission_restores_family_intents_binding_and_generation() {
    let catalog = IndexCatalog::default();
    catalog
        .upsert(explicit_rebuild(definition(1, 2, 9), 2, 1_000))
        .unwrap();
    catalog.upsert(definition(1, 2, 10)).unwrap();
    let replay_generation = catalog.catalog_replay_generation().unwrap();
    assert!(
        catalog
            .complete_catalog_replay(replay_generation, &replay_barrier(47))
            .unwrap()
    );
    let replay_before = catalog.retention_snapshot().unwrap().unwrap();
    let before = catalog.physical_snapshot().unwrap();
    let mut notices = catalog.subscribe();
    let resident_before = {
        let mut state = catalog.inner.lock().unwrap();
        let resident = total_resident_bytes(&state).unwrap();
        state.maximum_bytes = resident;
        resident
    };

    let error = catalog
        .upsert(explicit_rebuild(definition(1, 2, 10), 2, 2_000))
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert!(notices.try_recv().is_err());
    assert!(Arc::ptr_eq(&before, &catalog.physical_snapshot().unwrap()));
    assert!(Arc::ptr_eq(
        &replay_before,
        &catalog.retention_snapshot().unwrap().unwrap()
    ));
    assert_eq!(
        catalog.catalog_replay_generation().unwrap(),
        replay_generation
    );
    let state = catalog.inner.lock().unwrap();
    let recipe = state.recipes.values().next().unwrap();
    assert_eq!(recipe.rebuild_intents, BTreeMap::from([(9, 1_000)]));
    let alias = state
        .bindings
        .get(&CatalogIdentity {
            tenant_id: 1,
            bucket_id: 2,
            index_id: 10,
        })
        .unwrap();
    assert_eq!(alias.object_version, 1);
    assert_eq!(alias.explicit_rebuild_at_unix_millis, None);
    assert_eq!(total_resident_bytes(&state).unwrap(), resident_before);
}

#[test]
fn changes_update_active_catalog_without_an_admission_queue() {
    let catalog = IndexCatalog::default();
    let first = definition(1, 2, 9);
    let mut replacement = first.clone();
    replacement.object_version = 2;
    catalog.upsert(first).unwrap();
    catalog.upsert(replacement.clone()).unwrap();
    let (_, _, bindings, recipes, contracts) = catalog.snapshot().unwrap();
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].object_version, 2);
    assert_eq!(recipes.len(), 1);
    assert_eq!(contracts.len(), 1);
}

#[test]
fn active_catalog_does_not_have_a_logical_definition_capacity_gate() {
    let catalog = IndexCatalog::default();
    let first = definition(1, 2, 9);
    let second = definition(3, 4, 10);
    catalog.upsert(first.clone()).unwrap();
    catalog.upsert(second).unwrap();
    assert_eq!(catalog.snapshot().unwrap().2.len(), 2);
    catalog
        .remove(first.tenant_id, first.bucket_id, first.stored.index_id)
        .unwrap();
    assert_eq!(catalog.snapshot().unwrap().2.len(), 1);
}

#[test]
fn mutation_backup_retains_only_affected_recipe_allocations() {
    let catalog = IndexCatalog::default();
    let first = definition(1, 2, 9);
    catalog.upsert(first.clone()).unwrap();
    catalog.upsert(definition(3, 4, 10)).unwrap();
    let state = catalog.inner.lock().unwrap();
    let next = compact_binding(&first);
    let backup = CatalogMutationBackup::capture(&state, first.identity(), Some(&next));

    assert_eq!(state.recipes.len(), 2);
    assert_eq!(backup.recipes.len(), 1);
    assert_eq!(backup.query_contracts.len(), 1);
    assert!(Arc::ptr_eq(
        &state.recipes.get(&next.family).unwrap().physical,
        &backup.recipes[0].1.as_ref().unwrap().physical
    ));
}

#[test]
fn published_snapshot_shares_recipe_allocation_with_catalog_state() {
    let catalog = IndexCatalog::default();
    catalog.upsert(definition(1, 2, 9)).unwrap();
    let snapshot = catalog.physical_snapshot().unwrap();
    let state = catalog.inner.lock().unwrap();
    let recipe = state.recipes.values().next().unwrap();

    assert!(Arc::ptr_eq(&recipe.physical, &snapshot.recipes[0]));
}

#[test]
fn recipe_state_and_field_reference_charge_is_released_with_family() {
    let catalog = IndexCatalog::default();
    catalog.upsert(definition(1, 2, 9)).unwrap();
    {
        let state = catalog.inner.lock().unwrap();
        let recipe = state.recipes.values().next().unwrap();
        assert_eq!(recipe.field_references.len(), 1);
        assert!(catalog_recipe_state_resident_bytes(recipe).unwrap() > 0);
        assert!(state.resident_bytes > 0);
    }

    catalog.remove(1, 2, 9).unwrap();

    let state = catalog.inner.lock().unwrap();
    assert!(state.recipes.is_empty());
    assert!(state.bindings.is_empty());
    assert!(state.query_contracts.is_empty());
    assert_eq!(state.resident_bytes, 0);
}

#[tokio::test]
async fn definition_delete_is_version_monotonic() {
    let catalog = IndexCatalog::default();
    let definition = definition(1, 2, 9);
    let identity = definition.identity();
    catalog.upsert(definition).unwrap();
    catalog.delete_wait(identity, 2).await.unwrap();
    catalog.remove(1, 2, 9).unwrap();
    assert!(catalog.snapshot().unwrap().2.is_empty());
}

#[tokio::test]
async fn ordered_recreation_does_not_retain_a_tombstone() {
    let catalog = IndexCatalog::default();
    let original = definition(1, 2, 9);
    let identity = original.identity();
    catalog.upsert(original).unwrap();
    catalog.delete_wait(identity, 3).await.unwrap();
    assert!(catalog.snapshot().unwrap().2.is_empty());

    let mut recreated = definition(1, 2, 9);
    recreated.object_version = 4;
    catalog.upsert(recreated).unwrap();
    assert_eq!(catalog.snapshot().unwrap().2[0].object_version, 4);
}

#[test]
fn ordinary_definition_compiles_one_bound_typed_json_schema_and_fingerprint() {
    let definition = definition(1, 2, 9);
    assert_eq!(definition.schema.path_prefix, "");
    assert_eq!(definition.schema.fields[0].name, "value");
    assert_eq!(
        definition.schema_fingerprint,
        definition.schema.fingerprint().unwrap()
    );
    definition.validate().unwrap();
}

#[test]
fn physical_recipe_reuses_its_compiled_selector_catalogue() {
    let catalog = IndexCatalog::default();
    catalog.upsert(definition(1, 2, 9)).unwrap();
    let physical = catalog.physical_snapshot().unwrap();
    assert_eq!(
        physical.recipes[0].projection_plan.pointers(),
        ["/value".to_owned()]
    );
    let (_, recipe, contract) = catalog
        .resolve(CatalogIdentity {
            tenant_id: 1,
            bucket_id: 2,
            index_id: 9,
        })
        .unwrap()
        .unwrap();
    let query = recipe.query_schema(&contract).unwrap();
    assert_eq!(query.fields[0].name, "value");
    assert_eq!(query.fields[0].source_selector, "/value");
}

#[test]
fn ordinary_definition_semantic_update_compiles_a_new_fingerprint() {
    let original = definition(1, 2, 9);
    let mut updated = original.stored.clone();
    updated.path_prefix = "tenant/42/".into();
    let updated = CatalogDefinition::new(1, 2, 2, updated).unwrap();

    assert_ne!(original.schema_fingerprint, updated.schema_fingerprint);
    assert_eq!(updated.schema.path_prefix, "tenant/42/");
}

#[test]
fn equivalent_logical_definitions_share_one_physical_projection_identity() {
    let first = definition(1, 2, 9);
    let second = definition(1, 2, 10);
    assert_ne!(first.identity(), second.identity());
    assert_eq!(first.schema_fingerprint, second.schema_fingerprint);
    assert_eq!(first.physical_identity(), second.physical_identity());
    assert_eq!(
        first.projection_family_identity(),
        second.projection_family_identity()
    );
    assert_eq!(
        first.membership_recipe_identity(),
        second.membership_recipe_identity()
    );
    assert_eq!(
        first.field_recipe_identities(),
        second.field_recipe_identities()
    );

    let different_bucket = definition(1, 3, 11);
    assert_ne!(
        first.physical_identity(),
        different_bucket.physical_identity()
    );
    assert_ne!(
        first.projection_family_identity(),
        different_bucket.projection_family_identity()
    );

    let mut different_scope = second.stored.clone();
    different_scope.path_prefix = "other/".into();
    let different_scope = CatalogDefinition::new(1, 2, 2, different_scope).unwrap();
    assert_ne!(
        first.physical_identity(),
        different_scope.physical_identity()
    );
    assert_ne!(
        first.projection_family_identity(),
        different_scope.projection_family_identity()
    );
}

#[test]
fn physical_recipe_identity_never_crosses_tenant_or_bucket_authority() {
    let first = definition(1, 2, 9);
    let other_tenant = definition(3, 2, 10);
    let other_bucket = definition(1, 4, 11);
    assert_ne!(
        first.membership_recipe_identity(),
        other_tenant.membership_recipe_identity()
    );
    assert_ne!(
        first.membership_recipe_identity(),
        other_bucket.membership_recipe_identity()
    );
    assert_ne!(
        first.field_recipe_identities(),
        other_tenant.field_recipe_identities()
    );
}

#[test]
fn catalog_scale_collapses_two_hundred_fifty_thousand_equivalent_definitions() {
    let base = wide_typed_definition(1);
    let catalog = IndexCatalog::default();
    let mut first_physical_generation = None;
    for index_id in 1..=250_000_u64 {
        let mut logical = base.clone();
        logical.stored.index_id = index_id;
        logical.stored.name = format!("index-{index_id}");
        catalog.upsert(logical).unwrap();
        if index_id == 1 {
            first_physical_generation = Some(catalog.snapshot().unwrap().1);
        }
    }
    let (generation, physical_generation, logical, physical, contracts) =
        catalog.snapshot().unwrap();
    assert_eq!(generation, 250_001);
    assert_eq!(logical.len(), 250_000);
    assert_eq!(physical.len(), 1);
    assert_eq!(contracts.len(), 1);
    assert_eq!(physical[0].fields.len(), 32);
    assert_ne!(physical_generation, [0; 32]);
    assert_eq!(Some(physical_generation), first_physical_generation);
    // The active catalog retains compact logical bindings and one interned
    // schema. It must not approach the footprint of 250K cloned schemas.
    assert!(catalog.resident_bytes().unwrap() < 96 * 1024 * 1024);
}

#[test]
fn overlapping_field_subsets_compile_to_one_family_union() {
    let catalog = IndexCatalog::default();
    catalog
        .upsert(typed_definition(
            1,
            vec![keyword_field("a", "/a"), keyword_field("b", "/b")],
        ))
        .unwrap();
    let first_generation = catalog.snapshot().unwrap().1;
    catalog
        .upsert(typed_definition(
            2,
            vec![keyword_field("bee", "/b"), keyword_field("c", "/c")],
        ))
        .unwrap();
    let (_, second_generation, bindings, families, contracts) = catalog.snapshot().unwrap();
    assert_eq!(bindings.len(), 2);
    assert_eq!(families.len(), 1);
    assert_eq!(families[0].fields.len(), 3);
    assert_eq!(contracts.len(), 2);
    assert_ne!(first_generation, second_generation);
    let projection = families[0].projection_schema().unwrap();
    assert_eq!(projection.fields.len(), 3);
    assert!(
        projection
            .fields
            .iter()
            .enumerate()
            .all(|(ordinal, field)| field.id.get() == ordinal as u32)
    );

    // An equivalent alias changes only the compact logical/query binding,
    // never the physical family generation or recipe union.
    catalog
        .upsert(typed_definition(
            3,
            vec![keyword_field("aye", "/a"), keyword_field("bee", "/b")],
        ))
        .unwrap();
    let (_, alias_generation, _, families, _) = catalog.snapshot().unwrap();
    assert_eq!(alias_generation, second_generation);
    assert_eq!(families[0].fields.len(), 3);
}

#[test]
fn physical_snapshot_is_republished_only_when_the_family_recipe_changes() {
    let catalog = IndexCatalog::default();
    catalog.upsert(definition(1, 2, 9)).unwrap();
    let first = catalog.physical_snapshot().unwrap();

    // This logical alias references the same physical recipe. Workers keep
    // sharing the already-published immutable snapshot.
    catalog.upsert(definition(1, 2, 10)).unwrap();
    let alias = catalog.physical_snapshot().unwrap();
    assert!(Arc::ptr_eq(&first, &alias));
    assert!(Arc::ptr_eq(
        &first.recipes[0].projection_plan,
        &alias.recipes[0].projection_plan,
    ));

    catalog
        .upsert(typed_definition(11, vec![keyword_field("other", "/other")]))
        .unwrap();
    let changed = catalog.physical_snapshot().unwrap();
    assert!(!Arc::ptr_eq(&alias, &changed));
    assert!(changed.generation > alias.generation);
    assert_eq!(changed.recipes.len(), 2);
    let retained = changed
        .recipes
        .iter()
        .find(|recipe| recipe.family == alias.recipes[0].family)
        .unwrap();
    let added = changed
        .recipes
        .iter()
        .find(|recipe| recipe.family != alias.recipes[0].family)
        .unwrap();
    assert!(Arc::ptr_eq(
        &alias.recipes[0].projection_plan,
        &retained.projection_plan,
    ));
    assert!(!Arc::ptr_eq(
        &alias.recipes[0].projection_plan,
        &added.projection_plan,
    ));
}

#[test]
fn superseded_snapshot_keeps_replaced_recipe_charged() {
    let catalog = IndexCatalog::default();
    catalog
        .upsert(typed_definition(1, vec![keyword_field("a", "/a")]))
        .unwrap();
    let old_snapshot = catalog.physical_snapshot().unwrap();
    catalog
        .upsert(typed_definition(
            2,
            vec![keyword_field("a", "/a"), keyword_field("b", "/b")],
        ))
        .unwrap();

    let retained_bytes = catalog.resident_bytes().unwrap();
    drop(old_snapshot);
    assert!(catalog.resident_bytes().unwrap() < retained_bytes);
}

#[test]
fn extracted_projection_plan_keeps_superseded_recipe_charge_alive() {
    let catalog = IndexCatalog::default();
    let original = typed_definition(1, vec![keyword_field("a", "/a")]);
    catalog.upsert(original.clone()).unwrap();
    let held_plan = {
        let state = catalog.inner.lock().unwrap();
        Arc::clone(
            &state
                .recipes
                .values()
                .next()
                .unwrap()
                .physical
                .projection_plan,
        )
    };
    let mut replacement = original;
    replacement.object_version = 2;
    catalog.upsert(replacement).unwrap();
    let retained = catalog.resident_bytes().unwrap();

    drop(held_plan);

    assert!(catalog.resident_bytes().unwrap() < retained);
}

#[test]
fn tight_cap_same_schema_replacement_is_atomic_until_old_snapshot_drops() {
    let catalog = IndexCatalog::default();
    let original = typed_definition(1, vec![keyword_field("a", "/a")]);
    catalog.upsert(original.clone()).unwrap();
    let old_snapshot = catalog.physical_snapshot().unwrap();
    let (before_generation, before_resident) = {
        let mut state = catalog.inner.lock().unwrap();
        let values = (state.generation, total_resident_bytes(&state).unwrap());
        state.maximum_bytes = values.1;
        values
    };
    let mut replacement = original;
    replacement.object_version = 2;

    assert_eq!(
        catalog.upsert(replacement.clone()).unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    {
        let state = catalog.inner.lock().unwrap();
        assert_eq!(state.generation, before_generation);
        assert_eq!(total_resident_bytes(&state).unwrap(), before_resident);
        assert_eq!(state.bindings.len(), 1);
        assert_eq!(state.bindings.values().next().unwrap().object_version, 1);
        assert!(Arc::ptr_eq(&state.published_physical, &old_snapshot));
    }

    drop(old_snapshot);
    catalog.upsert(replacement).unwrap();
    let state = catalog.inner.lock().unwrap();
    assert_eq!(state.bindings.values().next().unwrap().object_version, 2);
    assert!(total_resident_bytes(&state).unwrap() <= state.maximum_bytes);
}

#[test]
fn tight_cap_field_union_does_not_hide_old_snapshot_duplication() {
    let first = typed_definition(1, vec![keyword_field("a", "/a")]);
    let second = typed_definition(2, vec![keyword_field("a", "/a"), keyword_field("b", "/b")]);
    let probe = IndexCatalog::default();
    probe.upsert(first.clone()).unwrap();
    probe.upsert(second.clone()).unwrap();
    let target_bytes = probe.resident_bytes().unwrap();

    let catalog = IndexCatalog::default();
    catalog.upsert(first).unwrap();
    let old_snapshot = catalog.physical_snapshot().unwrap();
    let (before_generation, before_resident) = {
        let mut state = catalog.inner.lock().unwrap();
        let values = (state.generation, total_resident_bytes(&state).unwrap());
        state.maximum_bytes = target_bytes;
        values
    };

    assert_eq!(
        catalog.upsert(second.clone()).unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    {
        let state = catalog.inner.lock().unwrap();
        assert_eq!(state.generation, before_generation);
        assert_eq!(total_resident_bytes(&state).unwrap(), before_resident);
        assert_eq!(state.bindings.len(), 1);
        assert_eq!(
            state.recipes.values().next().unwrap().physical.fields.len(),
            1
        );
        assert!(Arc::ptr_eq(&state.published_physical, &old_snapshot));
    }

    drop(old_snapshot);
    catalog.upsert(second).unwrap();
    let state = catalog.inner.lock().unwrap();
    assert_eq!(state.bindings.len(), 2);
    assert_eq!(
        state.recipes.values().next().unwrap().physical.fields.len(),
        2
    );
    assert!(total_resident_bytes(&state).unwrap() <= state.maximum_bytes);
}

#[test]
fn generation_overflow_rolls_back_every_catalog_surface() {
    let catalog = IndexCatalog::default();
    catalog
        .upsert(typed_definition(1, vec![keyword_field("a", "/a")]))
        .unwrap();
    let published = catalog.physical_snapshot().unwrap();
    let before_resident = catalog.resident_bytes().unwrap();
    catalog.inner.lock().unwrap().generation = u64::MAX;

    let error = catalog
        .upsert(typed_definition(2, vec![keyword_field("b", "/b")]))
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    let state = catalog.inner.lock().unwrap();
    assert_eq!(state.generation, u64::MAX);
    assert_eq!(total_resident_bytes(&state).unwrap(), before_resident);
    assert_eq!(state.bindings.len(), 1);
    assert_eq!(
        state.recipes.values().next().unwrap().physical.fields.len(),
        1
    );
    assert!(Arc::ptr_eq(&state.published_physical, &published));
}

#[test]
fn removed_snapshot_bundle_blocks_then_restores_catalog_admission() {
    let definition = typed_definition(1, vec![keyword_field("a", "/a")]);
    let fresh = IndexCatalog::default();
    fresh.upsert(definition.clone()).unwrap();
    let admission_bytes = fresh.resident_bytes().unwrap();

    let catalog = IndexCatalog::default();
    catalog.upsert(definition.clone()).unwrap();
    let old_snapshot = catalog.physical_snapshot().unwrap();
    catalog.remove(1, 2, 1).unwrap();
    {
        let mut state = catalog.inner.lock().unwrap();
        assert!(total_resident_bytes(&state).unwrap() > 0);
        state.maximum_bytes = admission_bytes;
    }

    assert_eq!(
        catalog.upsert(definition.clone()).unwrap_err().code(),
        tonic::Code::ResourceExhausted,
        "a live removed snapshot must continue consuming catalog capacity"
    );
    drop(old_snapshot);

    catalog.upsert(definition).unwrap();
    assert!(catalog.resident_bytes().unwrap() > 0);
}

#[test]
fn catalog_rejects_schema_or_fingerprint_detached_from_the_definition() {
    let definition = definition(1, 2, 9);
    let mut wrong_fingerprint = definition.clone();
    wrong_fingerprint.schema_fingerprint[0] ^= 1;
    assert_eq!(
        wrong_fingerprint.validate().unwrap_err().code(),
        tonic::Code::DataLoss
    );

    let mut wrong_schema = definition;
    wrong_schema.schema.path_prefix = "other/".into();
    wrong_schema.schema_fingerprint = wrong_schema.schema.fingerprint().unwrap();
    assert_eq!(
        wrong_schema.validate().unwrap_err().code(),
        tonic::Code::DataLoss
    );
}
