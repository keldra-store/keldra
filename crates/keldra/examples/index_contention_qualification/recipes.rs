//! Mapping between logical index definitions and shared physical recipes.

use std::collections::BTreeSet;

pub(super) fn physical_recipe(position: usize, physical_recipe_count: usize) -> usize {
    position % physical_recipe_count
}

pub(super) fn qualification_definition_positions(
    definition_count: usize,
    physical_recipe_count: usize,
    maximum: usize,
) -> Vec<usize> {
    if definition_count <= maximum {
        return (0..definition_count).collect();
    }
    let mut positions = (0..physical_recipe_count).collect::<BTreeSet<_>>();
    let remaining = maximum - physical_recipe_count;
    if remaining == 0 {
        return positions.into_iter().collect();
    }
    if remaining == 1 {
        positions.insert(definition_count - 1);
        return positions.into_iter().collect();
    }
    for ordinal in 0..remaining {
        positions.insert(
            physical_recipe_count
                + ordinal.saturating_mul(definition_count - 1 - physical_recipe_count)
                    / (remaining - 1),
        );
    }
    positions.into_iter().collect()
}

pub(super) fn recipe_probe_pointer(recipe: usize) -> String {
    format!("/probes/{recipe:02}")
}
