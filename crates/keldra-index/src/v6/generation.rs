use std::collections::{BTreeMap, BTreeSet};

use crate::IndexError;
const INDEX_ROUTING_KEY_BYTES: usize = 16 * 1024;
pub const MAX_QUERY_RECIPE_CATALOG_PROOFS: usize = 65_536;
pub const MAX_RETAINED_CATALOG_GENERATIONS_PER_RECIPE: usize = 4_096;

use super::RecipeIdentity;

/// Recipe-scoped proof that immutable query material from the named catalog
/// generations has exactly the same physical semantics as the active recipe.
/// A proof is deliberately not a global catalog alias: changed recipes must
/// start at the active generation while unchanged siblings may retain history.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryRecipeCatalogProof {
    pub recipe: RecipeIdentity,
    pub accepted_catalog_generations: Vec<[u8; 32]>,
}

impl QueryRecipeCatalogProof {
    pub fn validate(&self, active: [u8; 32]) -> Result<(), IndexError> {
        if active == [0; 32]
            || self.accepted_catalog_generations.is_empty()
            || self.accepted_catalog_generations.len() > MAX_RETAINED_CATALOG_GENERATIONS_PER_RECIPE
            || self
                .accepted_catalog_generations
                .binary_search(&active)
                .is_err()
            || self
                .accepted_catalog_generations
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || self
                .accepted_catalog_generations
                .iter()
                .any(|generation| *generation == [0; 32])
        {
            return Err(IndexError::InvalidDefinition(
                "query recipe catalog proof is invalid".into(),
            ));
        }
        Ok(())
    }

    pub fn accepts(&self, generation: [u8; 32]) -> bool {
        self.accepted_catalog_generations
            .binary_search(&generation)
            .is_ok()
    }
}

/// Explicit one-partition transition between immutable physical catalogs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionCatalogTransition {
    pub predecessor: ProjectionGenerationReference,
    pub retained_recipes: Vec<RecipeIdentity>,
}

impl ProjectionCatalogTransition {
    pub fn retains_component(&self, component: ComponentIdentity) -> bool {
        match component {
            ComponentIdentity::DocumentHead | ComponentIdentity::SourceRecords => true,
            ComponentIdentity::Membership(recipe)
            | ComponentIdentity::Field(recipe)
            | ComponentIdentity::Order(recipe) => {
                self.retained_recipes.binary_search(&recipe).is_ok()
            }
        }
    }

    pub fn validate_against(
        &self,
        previous: &ProjectionGeneration,
        previous_hash: [u8; 32],
        next_catalog: [u8; 32],
    ) -> Result<(), IndexError> {
        self.predecessor.validate()?;
        if next_catalog == [0; 32]
            || next_catalog == previous.physical_catalog_generation
            || self.predecessor != previous.reference(previous_hash)?
            || self
                .retained_recipes
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
        {
            return Err(IndexError::InvalidDefinition(
                "projection catalog transition is invalid".into(),
            ));
        }
        Ok(())
    }

    /// Build the exact per-recipe catalog proof set for activation. Recipes
    /// absent from `active_recipes` disappear; newly added recipes accept only
    /// the new catalog; retained recipes extend their already validated
    /// lineage by exactly the new catalog generation.
    pub fn recipe_catalog_proofs(
        &self,
        previous_catalog: [u8; 32],
        previous_proofs: &[QueryRecipeCatalogProof],
        active_recipes: &[RecipeIdentity],
        next_catalog: [u8; 32],
    ) -> Result<Vec<QueryRecipeCatalogProof>, IndexError> {
        if previous_catalog == [0; 32]
            || next_catalog == [0; 32]
            || previous_catalog == next_catalog
            || active_recipes.len() > MAX_QUERY_RECIPE_CATALOG_PROOFS
            || active_recipes.windows(2).any(|pair| pair[0] >= pair[1])
            || previous_proofs
                .windows(2)
                .any(|pair| pair[0].recipe >= pair[1].recipe)
            || self
                .retained_recipes
                .iter()
                .any(|recipe| active_recipes.binary_search(recipe).is_err())
        {
            return Err(IndexError::InvalidDefinition(
                "projection catalog recipe coverage is invalid".into(),
            ));
        }
        for proof in previous_proofs {
            proof.validate(previous_catalog)?;
        }
        let previous = previous_proofs
            .iter()
            .map(|proof| (proof.recipe, proof))
            .collect::<BTreeMap<_, _>>();
        active_recipes
            .iter()
            .map(|recipe| {
                let mut accepted_catalog_generations =
                    if self.retained_recipes.binary_search(recipe).is_ok() {
                        previous
                            .get(recipe)
                            .ok_or_else(|| {
                                IndexError::InvalidDefinition(
                                    "retained recipe has no predecessor catalog proof".into(),
                                )
                            })?
                            .accepted_catalog_generations
                            .clone()
                    } else {
                        Vec::new()
                    };
                match accepted_catalog_generations.binary_search(&next_catalog) {
                    Ok(_) => {}
                    Err(position) => accepted_catalog_generations.insert(position, next_catalog),
                }
                let proof = QueryRecipeCatalogProof {
                    recipe: *recipe,
                    accepted_catalog_generations,
                };
                proof.validate(next_catalog)?;
                Ok(proof)
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ComponentIdentity {
    DocumentHead,
    SourceRecords,
    Membership(RecipeIdentity),
    Field(RecipeIdentity),
    Order(RecipeIdentity),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentRoot {
    pub component: ComponentIdentity,
    pub stream_root_hash: [u8; 32],
    pub segment_count: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub encoded_bytes: u64,
    pub logical_bytes: u64,
    pub directory_bytes: u64,
}

impl ComponentRoot {
    pub fn new(
        component: ComponentIdentity,
        stream_root_hash: [u8; 32],
        segment_count: u64,
        encoded_bytes: u64,
        logical_bytes: u64,
        directory_bytes: u64,
    ) -> Result<Self, IndexError> {
        let root = Self {
            component,
            stream_root_hash,
            segment_count,
            first_sequence: 1,
            last_sequence: segment_count,
            encoded_bytes,
            logical_bytes,
            directory_bytes,
        };
        root.validate()?;
        Ok(root)
    }

    pub fn with_sequences(
        component: ComponentIdentity,
        stream_root_hash: [u8; 32],
        segment_count: u64,
        first_sequence: u64,
        last_sequence: u64,
        encoded_bytes: u64,
        logical_bytes: u64,
        directory_bytes: u64,
    ) -> Result<Self, IndexError> {
        let root = Self {
            component,
            stream_root_hash,
            segment_count,
            first_sequence,
            last_sequence,
            encoded_bytes,
            logical_bytes,
            directory_bytes,
        };
        root.validate()?;
        Ok(root)
    }

    pub(super) fn validate(&self) -> Result<(), IndexError> {
        if self.stream_root_hash == [0; 32]
            || self.segment_count == 0
            || self.first_sequence == 0
            || self.first_sequence > self.last_sequence
            || self
                .last_sequence
                .checked_sub(self.first_sequence)
                .and_then(|span| span.checked_add(1))
                .is_none_or(|span| span < self.segment_count)
            || self.encoded_bytes <= self.directory_bytes
            || self.logical_bytes == 0
            || self.directory_bytes == 0
        {
            return Err(IndexError::InvalidDefinition(
                "projection component root is invalid".into(),
            ));
        }
        Ok(())
    }
}

/// Exact authority boundary for one independently written projection stream.
///
/// `source_node` and `source_epoch` name the immutable source incarnation.
/// `producer_node` is the current fenced owner that builds its projection.
/// A handoff changes producer/fence without inventing a new source.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProjectionPartitionIdentity {
    pub family_id: [u8; 32],
    pub source_node: u64,
    pub source_epoch: [u8; 32],
    pub producer_node: u64,
    pub placement_term: u64,
    pub placement_index: u64,
}

impl ProjectionPartitionIdentity {
    pub fn new(
        family_id: [u8; 32],
        source_node: u64,
        source_epoch: [u8; 32],
        producer_node: u64,
        placement_term: u64,
        placement_index: u64,
    ) -> Result<Self, IndexError> {
        let identity = Self {
            family_id,
            source_node,
            source_epoch,
            producer_node,
            placement_term,
            placement_index,
        };
        identity.validate()?;
        Ok(identity)
    }

    pub fn validate(&self) -> Result<(), IndexError> {
        if self.family_id == [0; 32]
            || self.source_node == 0
            || self.source_epoch == [0; 32]
            || self.producer_node == 0
            || self.placement_term == 0
            || self.placement_index == 0
        {
            return Err(IndexError::InvalidDefinition(
                "projection partition identity is invalid".into(),
            ));
        }
        Ok(())
    }
}

/// Atomic point-in-time root for one source partition's physical components.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionGeneration {
    pub partition: ProjectionPartitionIdentity,
    /// Compiled physical recipe/source-router generation. This may advance
    /// while the stable source-partition namespace and existing roots remain.
    pub physical_catalog_generation: [u8; 32],
    pub revision: u64,
    /// First source-local journal offset not represented by this generation.
    pub next_offset: u64,
    /// Highest globally finalized atomic position fully represented here.
    pub through_atomic_position: u64,
    /// Immutable query-run stream root pinned by this same atomic generation.
    /// Query execution never assembles a run from source/component records.
    pub query_stream_root: ProjectionQueryStreamRoot,
    pub roots: Vec<ComponentRoot>,
    /// Exact retired partition roots whose live material was inherited before
    /// the family directory stopped exposing them independently.
    pub inherited_partitions: Vec<ProjectionGenerationReference>,
    pub previous_generation_hash: Option<[u8; 32]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionQueryStreamRoot {
    pub stream_root_hash: [u8; 32],
    pub run_count: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub source_start_offset: u64,
    pub next_offset: u64,
    pub through_atomic_position: u64,
}

impl ProjectionQueryStreamRoot {
    pub fn empty(
        partition: ProjectionPartitionIdentity,
        physical_catalog_generation: [u8; 32],
        next_offset: u64,
        through_atomic_position: u64,
    ) -> Result<Self, IndexError> {
        partition.validate()?;
        if physical_catalog_generation == [0; 32] {
            return Err(IndexError::InvalidDefinition(
                "projection query stream catalog is zero".into(),
            ));
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"keldra.index.v6.empty-query-stream/v1\0");
        hasher.update(&partition.family_id);
        hasher.update(&partition.source_node.to_be_bytes());
        hasher.update(&partition.source_epoch);
        hasher.update(&partition.producer_node.to_be_bytes());
        hasher.update(&partition.placement_term.to_be_bytes());
        hasher.update(&partition.placement_index.to_be_bytes());
        hasher.update(&physical_catalog_generation);
        hasher.update(&next_offset.to_be_bytes());
        hasher.update(&through_atomic_position.to_be_bytes());
        Ok(Self {
            stream_root_hash: *hasher.finalize().as_bytes(),
            run_count: 0,
            first_sequence: 0,
            last_sequence: 0,
            source_start_offset: next_offset,
            next_offset,
            through_atomic_position,
        })
    }

    pub fn validate_at(
        &self,
        next_offset: u64,
        through_atomic_position: u64,
    ) -> Result<(), IndexError> {
        let empty = self.run_count == 0;
        if self.stream_root_hash == [0; 32]
            || self.next_offset != next_offset
            || self.through_atomic_position != through_atomic_position
            || (empty
                && (self.first_sequence != 0
                    || self.last_sequence != 0
                    || self.source_start_offset != next_offset))
            || (!empty
                && (self.first_sequence == 0
                    || self.first_sequence > self.last_sequence
                    || self.source_start_offset >= self.next_offset))
        {
            return Err(IndexError::InvalidDefinition(
                "projection query stream root is invalid at generation cut".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProjectionGenerationReference {
    pub partition: ProjectionPartitionIdentity,
    pub physical_catalog_generation: [u8; 32],
    pub generation_hash: [u8; 32],
    pub generation_revision: u64,
    pub next_offset: u64,
    pub through_atomic_position: u64,
}

impl ProjectionGenerationReference {
    pub fn validate(&self) -> Result<(), IndexError> {
        self.partition.validate()?;
        if self.physical_catalog_generation == [0; 32]
            || self.generation_hash == [0; 32]
            || self.generation_revision == 0
        {
            return Err(IndexError::InvalidDefinition(
                "projection generation reference is invalid".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionCurrent {
    pub partition: ProjectionPartitionIdentity,
    pub physical_catalog_generation: [u8; 32],
    pub generation_hash: [u8; 32],
    pub generation_revision: u64,
    /// First source-local journal offset not represented by the selected root.
    pub next_offset: u64,
    pub through_atomic_position: u64,
}

impl ProjectionCurrent {
    pub fn new(
        generation_hash: [u8; 32],
        generation: &ProjectionGeneration,
    ) -> Result<Self, IndexError> {
        generation.validate()?;
        if generation_hash == [0; 32] {
            return Err(IndexError::InvalidDefinition(
                "projection current generation hash is zero".into(),
            ));
        }
        Ok(Self {
            partition: generation.partition,
            physical_catalog_generation: generation.physical_catalog_generation,
            generation_hash,
            generation_revision: generation.revision,
            next_offset: generation.next_offset,
            through_atomic_position: generation.through_atomic_position,
        })
    }

    pub fn validate_against(&self, generation: &ProjectionGeneration) -> Result<(), IndexError> {
        generation.validate()?;
        if self.partition != generation.partition
            || self.physical_catalog_generation != generation.physical_catalog_generation
            || self.generation_hash == [0; 32]
            || self.generation_revision != generation.revision
            || self.next_offset != generation.next_offset
            || self.through_atomic_position != generation.through_atomic_position
        {
            return Err(IndexError::InvalidDefinition(
                "projection current does not name its exact partition generation".into(),
            ));
        }
        Ok(())
    }
}

impl ProjectionGeneration {
    pub fn advance_catalog(
        &self,
        previous_generation_hash: [u8; 32],
        physical_catalog_generation: [u8; 32],
        next_offset: u64,
        through_atomic_position: u64,
        replacements: Vec<ComponentRoot>,
        transition: &ProjectionCatalogTransition,
    ) -> Result<Self, IndexError> {
        transition.validate_against(self, previous_generation_hash, physical_catalog_generation)?;
        let replacement_components = replacements
            .iter()
            .map(|root| root.component)
            .collect::<BTreeSet<_>>();
        let mut next = self.advance(
            previous_generation_hash,
            physical_catalog_generation,
            next_offset,
            through_atomic_position,
            replacements,
        )?;
        next.roots.retain(|root| {
            replacement_components.contains(&root.component)
                || transition.retains_component(root.component)
        });
        next.validate()?;
        Ok(next)
    }

    pub fn initial(
        partition: ProjectionPartitionIdentity,
        physical_catalog_generation: [u8; 32],
        next_offset: u64,
        through_atomic_position: u64,
        roots: Vec<ComponentRoot>,
    ) -> Result<Self, IndexError> {
        let generation = Self {
            partition,
            physical_catalog_generation,
            revision: 1,
            next_offset,
            through_atomic_position,
            query_stream_root: ProjectionQueryStreamRoot::empty(
                partition,
                physical_catalog_generation,
                next_offset,
                through_atomic_position,
            )?,
            roots,
            inherited_partitions: Vec::new(),
            previous_generation_hash: None,
        };
        generation.validate()?;
        Ok(generation)
    }

    pub fn initial_after_handoff(
        partition: ProjectionPartitionIdentity,
        physical_catalog_generation: [u8; 32],
        next_offset: u64,
        through_atomic_position: u64,
        roots: Vec<ComponentRoot>,
        inherited_partitions: Vec<ProjectionGenerationReference>,
    ) -> Result<Self, IndexError> {
        let generation = Self {
            partition,
            physical_catalog_generation,
            revision: 1,
            next_offset,
            through_atomic_position,
            query_stream_root: ProjectionQueryStreamRoot::empty(
                partition,
                physical_catalog_generation,
                next_offset,
                through_atomic_position,
            )?,
            roots,
            inherited_partitions,
            previous_generation_hash: None,
        };
        generation.validate()?;
        Ok(generation)
    }

    pub fn reference(
        &self,
        generation_hash: [u8; 32],
    ) -> Result<ProjectionGenerationReference, IndexError> {
        self.validate()?;
        let reference = ProjectionGenerationReference {
            partition: self.partition,
            physical_catalog_generation: self.physical_catalog_generation,
            generation_hash,
            generation_revision: self.revision,
            next_offset: self.next_offset,
            through_atomic_position: self.through_atomic_position,
        };
        reference.validate()?;
        Ok(reference)
    }

    pub fn advance(
        &self,
        previous_generation_hash: [u8; 32],
        physical_catalog_generation: [u8; 32],
        next_offset: u64,
        through_atomic_position: u64,
        replacements: Vec<ComponentRoot>,
    ) -> Result<Self, IndexError> {
        self.validate()?;
        if previous_generation_hash == [0; 32]
            || physical_catalog_generation == [0; 32]
            || next_offset < self.next_offset
            || through_atomic_position < self.through_atomic_position
        {
            return Err(IndexError::InvalidDefinition(
                "projection partition generation cannot move behind its predecessor".into(),
            ));
        }
        let mut roots = self
            .roots
            .iter()
            .cloned()
            .map(|root| (root.component, root))
            .collect::<BTreeMap<_, _>>();
        let mut replaced = BTreeSet::new();
        for root in replacements {
            root.validate()?;
            if !replaced.insert(root.component) {
                return Err(IndexError::InvalidDefinition(
                    "one generation replaces a component more than once".into(),
                ));
            }
            roots.insert(root.component, root);
        }
        let generation = Self {
            partition: self.partition,
            physical_catalog_generation,
            revision: self
                .revision
                .checked_add(1)
                .ok_or(IndexError::OffsetOverflow)?,
            next_offset,
            through_atomic_position,
            query_stream_root: ProjectionQueryStreamRoot::empty(
                self.partition,
                physical_catalog_generation,
                next_offset,
                through_atomic_position,
            )?,
            roots: roots.into_values().collect(),
            inherited_partitions: self.inherited_partitions.clone(),
            previous_generation_hash: Some(previous_generation_hash),
        };
        generation.validate()?;
        Ok(generation)
    }

    pub fn root(&self, identity: ComponentIdentity) -> Option<&ComponentRoot> {
        self.roots
            .binary_search_by_key(&identity, |root| root.component)
            .ok()
            .map(|index| &self.roots[index])
    }

    /// Attach the immutable query stream assembled from query-ready mini-runs
    /// before encoding/publishing this generation.
    pub fn with_query_stream_root(
        mut self,
        root: ProjectionQueryStreamRoot,
    ) -> Result<Self, IndexError> {
        root.validate_at(self.next_offset, self.through_atomic_position)?;
        self.query_stream_root = root;
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<(), IndexError> {
        self.partition.validate()?;
        self.query_stream_root
            .validate_at(self.next_offset, self.through_atomic_position)?;
        if self.physical_catalog_generation == [0; 32]
            || self.revision == 0
            || self.previous_generation_hash == Some([0; 32])
            || self
                .roots
                .windows(2)
                .any(|pair| pair[0].component >= pair[1].component)
            || self.roots.iter().any(|root| root.validate().is_err())
            || self.inherited_partitions.iter().any(|reference| {
                reference.validate().is_err()
                    || reference.partition.family_id != self.partition.family_id
                    || reference.partition.source_node != self.partition.source_node
                    || reference.partition.source_epoch != self.partition.source_epoch
                    || reference.physical_catalog_generation != self.physical_catalog_generation
            })
            || self
                .inherited_partitions
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || self
                .inherited_partitions
                .iter()
                .any(|reference| reference.partition == self.partition)
            || (!self.roots.is_empty() && self.root(ComponentIdentity::DocumentHead).is_none())
        {
            return Err(IndexError::InvalidDefinition(
                "projection partition generation is incomplete or non-canonical".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalFieldBinding {
    pub public_field_id: u32,
    pub public_name: String,
    pub recipe: RecipeIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalProjectionBinding {
    pub logical_index_id: u64,
    pub logical_definition_version: u64,
    pub family_id: [u8; 32],
    pub physical_catalog_generation: [u8; 32],
    pub membership: RecipeIdentity,
    pub fields: Vec<LogicalFieldBinding>,
}

impl LogicalProjectionBinding {
    pub fn validate_against(&self, generation: &ProjectionGeneration) -> Result<(), IndexError> {
        generation.validate()?;
        if self.logical_index_id == 0
            || self.logical_definition_version == 0
            || self.family_id != generation.partition.family_id
            || self.physical_catalog_generation != generation.physical_catalog_generation
            || self.physical_catalog_generation == [0; 32]
            || self.fields.iter().any(|field| {
                field.public_name.is_empty()
                    || field.public_name.len() > INDEX_ROUTING_KEY_BYTES
                    || field.public_name.contains('\0')
            })
            || self
                .fields
                .windows(2)
                .any(|pair| pair[0].public_field_id >= pair[1].public_field_id)
            || generation
                .root(ComponentIdentity::Membership(self.membership))
                .is_none()
            || self.fields.iter().any(|field| {
                generation
                    .root(ComponentIdentity::Field(field.recipe))
                    .is_none()
            })
        {
            return Err(IndexError::InvalidDefinition(
                "logical projection binding is invalid or physically incomplete".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recipe(byte: u8) -> RecipeIdentity {
        RecipeIdentity::new([byte; 32]).unwrap()
    }
    fn partition() -> ProjectionPartitionIdentity {
        ProjectionPartitionIdentity::new([1; 32], 7, [2; 32], 7, 3, 4).unwrap()
    }
    fn root(component: ComponentIdentity, byte: u8) -> ComponentRoot {
        ComponentRoot::new(component, [byte; 32], 1, 100, 80, 20).unwrap()
    }
    fn initial() -> ProjectionGeneration {
        ProjectionGeneration::initial(
            partition(),
            [7; 32],
            8,
            7,
            vec![
                root(ComponentIdentity::DocumentHead, 2),
                root(ComponentIdentity::SourceRecords, 6),
                root(ComponentIdentity::Membership(recipe(3)), 3),
                root(ComponentIdentity::Field(recipe(4)), 4),
                root(ComponentIdentity::Field(recipe(5)), 5),
            ],
        )
        .unwrap()
    }

    #[test]
    fn replacing_one_field_reuses_every_other_component_root() {
        let old = initial();
        let new = old
            .advance(
                [9; 32],
                [7; 32],
                9,
                8,
                vec![root(ComponentIdentity::Field(recipe(4)), 8)],
            )
            .unwrap();
        assert_eq!(new.revision, 2);
        assert_eq!(
            new.root(ComponentIdentity::Field(recipe(5))),
            old.root(ComponentIdentity::Field(recipe(5)))
        );
        assert_ne!(
            new.root(ComponentIdentity::Field(recipe(4))),
            old.root(ComponentIdentity::Field(recipe(4)))
        );
    }

    #[test]
    fn catalog_transition_reuses_only_retained_recipes_and_builds_exact_proofs() {
        let old = initial();
        let transition = ProjectionCatalogTransition {
            predecessor: old.reference([9; 32]).unwrap(),
            retained_recipes: vec![recipe(4)],
        };
        let next = old
            .advance_catalog(
                [9; 32],
                [8; 32],
                9,
                8,
                vec![root(ComponentIdentity::Field(recipe(6)), 8)],
                &transition,
            )
            .unwrap();
        assert_eq!(
            next.root(ComponentIdentity::Field(recipe(4))),
            old.root(ComponentIdentity::Field(recipe(4)))
        );
        assert!(next.root(ComponentIdentity::Field(recipe(5))).is_none());
        assert!(
            next.root(ComponentIdentity::Membership(recipe(3)))
                .is_none()
        );
        assert!(next.root(ComponentIdentity::Field(recipe(6))).is_some());

        let proofs = transition
            .recipe_catalog_proofs(
                [7; 32],
                &[QueryRecipeCatalogProof {
                    recipe: recipe(4),
                    accepted_catalog_generations: vec![[6; 32], [7; 32]],
                }],
                &[recipe(4), recipe(6)],
                [8; 32],
            )
            .unwrap();
        assert_eq!(
            proofs,
            vec![
                QueryRecipeCatalogProof {
                    recipe: recipe(4),
                    accepted_catalog_generations: vec![[6; 32], [7; 32], [8; 32]],
                },
                QueryRecipeCatalogProof {
                    recipe: recipe(6),
                    accepted_catalog_generations: vec![[8; 32]],
                },
            ]
        );
    }

    #[test]
    fn local_checkpoint_cannot_regress_or_cross_partition_identity() {
        let old = initial();
        assert!(old.advance([9; 32], [7; 32], 7, 7, Vec::new()).is_err());
        assert!(old.advance([9; 32], [7; 32], 9, 6, Vec::new()).is_err());
        let mut crossed = old.clone();
        crossed.partition.source_node = 8;
        let current = ProjectionCurrent::new([7; 32], &old).unwrap();
        assert!(current.validate_against(&crossed).is_err());
    }

    #[test]
    fn handoff_lineage_cannot_cross_a_physical_catalog() {
        let old = initial();
        let mut predecessor = old.reference([8; 32]).unwrap();
        predecessor.physical_catalog_generation = [42; 32];
        assert!(
            ProjectionGeneration::initial_after_handoff(
                partition(),
                [7; 32],
                9,
                8,
                old.roots.clone(),
                vec![predecessor],
            )
            .is_err()
        );
    }

    #[test]
    fn handoff_preserves_source_identity_but_changes_producer_and_fence() {
        let old = initial();
        let predecessor = old.reference([8; 32]).unwrap();
        let successor = ProjectionPartitionIdentity::new([1; 32], 7, [2; 32], 9, 4, 1).unwrap();
        let next = ProjectionGeneration::initial_after_handoff(
            successor,
            [7; 32],
            8,
            7,
            old.roots,
            vec![predecessor],
        )
        .unwrap();
        assert_eq!(next.partition.source_node, 7);
        assert_eq!(next.partition.source_epoch, [2; 32]);
        assert_eq!(next.partition.producer_node, 9);

        let crossed = ProjectionPartitionIdentity::new([1; 32], 8, [2; 32], 9, 4, 1).unwrap();
        assert!(
            ProjectionGeneration::initial_after_handoff(
                crossed,
                [7; 32],
                8,
                7,
                next.roots,
                next.inherited_partitions,
            )
            .is_err()
        );
    }
}
