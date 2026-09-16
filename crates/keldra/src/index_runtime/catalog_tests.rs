use keldra_api::v1::{
    CreateIndexRequest, IndexField, IndexFieldCapability, IndexFieldCardinality,
    IndexSpecification, KeywordIndexField, TypedJsonIndexSpec, index_field,
};

use super::*;

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
