use std::collections::BTreeMap;

use crate::IndexError;
use crate::typed_json::FieldId;
use crate::v1::MAX_CATALOG_LINEAGE_GENERATIONS;

use super::{
    PinnedPartitionQueryRoot, QueryCommonCut, QueryExecutionLimits, QueryFieldBinding,
    QueryRecipeCatalogProof, RecipeIdentity, TypedJsonQueryRequest, count_predicate_nodes,
    resource,
};

pub(super) fn validate_request(
    common_cut: QueryCommonCut,
    pins: &[PinnedPartitionQueryRoot],
    request: &TypedJsonQueryRequest,
    limits: QueryExecutionLimits,
    validate_snapshot_structure: bool,
) -> Result<BTreeMap<FieldId, QueryFieldBinding>, IndexError> {
    if pins.is_empty() || pins.len() > limits.maximum_partitions {
        return resource(pins.len(), limits.maximum_partitions);
    }
    if let Some(predicate) = request.predicate.as_ref() {
        predicate.validate()?;
    }
    if request.result_limit == 0 || request.result_limit > limits.maximum_results {
        return resource(request.result_limit, limits.maximum_results);
    }
    if request.order.len() > limits.maximum_order_fields {
        return resource(request.order.len(), limits.maximum_order_fields);
    }
    if request.facets.len() > limits.maximum_facets {
        return resource(request.facets.len(), limits.maximum_facets);
    }
    if request.aggregates.len() > limits.maximum_aggregates {
        return resource(request.aggregates.len(), limits.maximum_aggregates);
    }
    if request.resume_after_document.is_some()
        && (!request.order.is_empty()
            || !request.facets.is_empty()
            || !request.aggregates.is_empty())
    {
        return Err(IndexError::InvalidQuery(
            "a stable document cursor cannot be combined with ordering, facets, or aggregates"
                .into(),
        ));
    }
    let mut nodes = 0usize;
    if let Some(predicate) = request.predicate.as_ref() {
        count_predicate_nodes(predicate, &mut nodes)?;
    }
    if nodes > limits.maximum_boolean_nodes {
        return resource(nodes, limits.maximum_boolean_nodes);
    }
    if request.logical.logical_index_id == 0 || request.logical.logical_definition_version == 0 {
        return Err(IndexError::InvalidQuery(
            "logical query binding identity is zero".into(),
        ));
    }
    if validate_snapshot_structure {
        for (ordinal, pin) in pins.iter().enumerate() {
            pin.validate_at(common_cut)?;
            if pin.partition.family_id != request.logical.family_id
                || pin.physical_catalog_generation != request.logical.physical_catalog_generation
                || pin.physical_catalog_generation == [0; 32]
                || ordinal
                    .checked_sub(1)
                    .is_some_and(|previous| pins[previous].partition >= pin.partition)
            {
                return Err(IndexError::InvalidQuery(
                    "v1 query roots are not one unique common-cut vector".into(),
                ));
            }
        }
    }
    if request.recipe_catalog_proofs.is_empty()
        || request
            .recipe_catalog_proofs
            .windows(2)
            .any(|pair| pair[0].recipe >= pair[1].recipe)
    {
        return Err(IndexError::InvalidQuery(
            "v1 query recipe catalog proofs are absent or non-canonical".into(),
        ));
    }
    let logical = request
        .logical
        .fields
        .iter()
        .map(|field| (field.public_field_id, field.recipe))
        .collect::<BTreeMap<_, _>>();
    if logical.len() != request.logical.fields.len() {
        return Err(IndexError::InvalidQuery(
            "logical query binding has duplicate public fields".into(),
        ));
    }
    let mut contracts = BTreeMap::new();
    for binding in &request.fields {
        binding.field.validate()?;
        if logical.get(&binding.field.id.get()) != Some(&binding.recipe)
            || contracts
                .insert(binding.field.id, binding.clone())
                .is_some()
        {
            return Err(IndexError::InvalidQuery(
                "v1 query field contract disagrees with its logical binding".into(),
            ));
        }
    }
    for recipe in std::iter::once(request.logical.membership)
        .chain(contracts.values().map(|binding| binding.recipe))
    {
        if recipe_proof(request, recipe).is_none() {
            return Err(IndexError::InvalidQuery(
                "v1 query recipe lacks a catalog-lineage proof".into(),
            ));
        }
    }
    if validate_snapshot_structure {
        if request.catalog_lineage.is_empty()
            || request.catalog_lineage.len() > MAX_CATALOG_LINEAGE_GENERATIONS
            || request.catalog_lineage.last() != Some(&request.logical.physical_catalog_generation)
            || request
                .catalog_lineage
                .iter()
                .any(|generation| *generation == [0; 32])
            || request
                .catalog_lineage
                .iter()
                .enumerate()
                .any(|(index, generation)| request.catalog_lineage[..index].contains(generation))
        {
            return Err(IndexError::InvalidQuery(
                "v1 query catalog lineage is invalid".into(),
            ));
        }
        let active_ordinal = u32::try_from(request.catalog_lineage.len() - 1)
            .map_err(|_| IndexError::OffsetOverflow)?;
        for proof in &request.recipe_catalog_proofs {
            proof.validate(request.catalog_lineage.len(), active_ordinal)?;
        }
    }
    Ok(contracts)
}

fn recipe_proof(
    request: &TypedJsonQueryRequest,
    recipe: RecipeIdentity,
) -> Option<&QueryRecipeCatalogProof> {
    request
        .recipe_catalog_proofs
        .binary_search_by_key(&recipe, |proof| proof.recipe)
        .ok()
        .map(|index| &request.recipe_catalog_proofs[index])
}
