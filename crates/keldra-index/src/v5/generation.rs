use std::collections::{BTreeMap, BTreeSet};

use crate::IndexError;
use crate::v4::INDEX_ROUTING_KEY_BYTES;

use super::RecipeIdentity;

/// One independently replaceable stream in a physical projection generation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ComponentIdentity {
    DocumentHead,
    ProjectedState,
    /// Exact stable-key set produced by one source object. This makes delete
    /// and record-expansion shrink proportional to that source rather than a
    /// scan of the complete projected-state stream.
    SourceRecords,
    Membership(RecipeIdentity),
    Field(RecipeIdentity),
    Order(RecipeIdentity),
}

/// Content-addressed immutable root for one complete component stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentRoot {
    pub component: ComponentIdentity,
    pub stream_root_hash: [u8; 32],
    pub segment_count: u64,
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

/// Complete source position represented by one atomic generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionBarrier {
    /// Sorted unique `(source node, source epoch, first unrepresented journal
    /// offset)` tuples. An offset is never interpreted across journal epochs.
    pub source_offsets: Vec<(u64, [u8; 32], u64)>,
    pub atomic_through: Option<u64>,
}

impl ProjectionBarrier {
    pub fn new(
        source_offsets: Vec<(u64, [u8; 32], u64)>,
        atomic_through: Option<u64>,
    ) -> Result<Self, IndexError> {
        let barrier = Self {
            source_offsets,
            atomic_through,
        };
        barrier.validate()?;
        Ok(barrier)
    }

    fn validate(&self) -> Result<(), IndexError> {
        if self.source_offsets.is_empty()
            || self
                .source_offsets
                .iter()
                .any(|(node, epoch, offset)| *node == 0 || *epoch == [0; 32] || *offset == 0)
            || self
                .source_offsets
                .windows(2)
                .any(|pair| pair[0].0 >= pair[1].0)
            || self.atomic_through == Some(0)
        {
            return Err(IndexError::InvalidDefinition(
                "projection generation barrier is invalid".into(),
            ));
        }
        Ok(())
    }

    pub fn covers(&self, previous: &Self) -> bool {
        let current = self
            .source_offsets
            .iter()
            .copied()
            .map(|(node, epoch, offset)| (node, (epoch, offset)))
            .collect::<BTreeMap<_, _>>();
        previous.source_offsets.iter().all(|(node, epoch, offset)| {
            current
                .get(node)
                .is_some_and(|(current_epoch, current_offset)| {
                    current_epoch == epoch && current_offset >= offset
                })
        }) && match (previous.atomic_through, self.atomic_through) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(old), Some(new)) => new >= old,
        }
    }
}

/// Atomic point-in-time root for independently reusable physical components.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionGeneration {
    pub family_id: [u8; 32],
    pub revision: u64,
    pub barrier: ProjectionBarrier,
    pub roots: Vec<ComponentRoot>,
    pub previous_generation_hash: Option<[u8; 32]>,
}

/// Small mutable family pointer. Its exact generation record and immutable
/// directories remain content-addressed; only this value is replaced by CAS.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionCurrent {
    pub family_id: [u8; 32],
    pub generation_hash: [u8; 32],
    pub generation_revision: u64,
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
            family_id: generation.family_id,
            generation_hash,
            generation_revision: generation.revision,
        })
    }

    pub fn validate_against(&self, generation: &ProjectionGeneration) -> Result<(), IndexError> {
        generation.validate()?;
        if self.family_id != generation.family_id
            || self.generation_hash == [0; 32]
            || self.generation_revision != generation.revision
        {
            return Err(IndexError::InvalidDefinition(
                "projection current does not name its exact generation".into(),
            ));
        }
        Ok(())
    }
}

impl ProjectionGeneration {
    pub fn initial(
        family_id: [u8; 32],
        barrier: ProjectionBarrier,
        roots: Vec<ComponentRoot>,
    ) -> Result<Self, IndexError> {
        let generation = Self {
            family_id,
            revision: 1,
            barrier,
            roots,
            previous_generation_hash: None,
        };
        generation.validate()?;
        Ok(generation)
    }

    /// Install changed roots while retaining every unchanged component.
    ///
    /// The returned value is an in-memory representation of the single
    /// generation record which publication must install atomically.
    pub fn advance(
        &self,
        previous_generation_hash: [u8; 32],
        barrier: ProjectionBarrier,
        replacements: Vec<ComponentRoot>,
    ) -> Result<Self, IndexError> {
        self.validate()?;
        barrier.validate()?;
        if previous_generation_hash == [0; 32] || !barrier.covers(&self.barrier) {
            return Err(IndexError::InvalidDefinition(
                "projection generation cannot move behind its predecessor".into(),
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
            family_id: self.family_id,
            revision: self
                .revision
                .checked_add(1)
                .ok_or(IndexError::OffsetOverflow)?,
            barrier,
            roots: roots.into_values().collect(),
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

    pub fn validate(&self) -> Result<(), IndexError> {
        self.barrier.validate()?;
        if self.family_id == [0; 32]
            || self.revision == 0
            || self.previous_generation_hash == Some([0; 32])
            || self
                .roots
                .windows(2)
                .any(|pair| pair[0].component >= pair[1].component)
            || self.roots.iter().any(|root| root.validate().is_err())
            || (!self.roots.is_empty()
                && (self.root(ComponentIdentity::DocumentHead).is_none()
                    || self.root(ComponentIdentity::ProjectedState).is_none()))
        {
            return Err(IndexError::InvalidDefinition(
                "projection generation is incomplete or non-canonical".into(),
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

/// Authorized logical name/ID mapping onto one exact physical generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalProjectionBinding {
    pub logical_index_id: u64,
    pub logical_definition_version: u64,
    pub family_id: [u8; 32],
    /// First family generation containing every recipe named by this binding.
    /// Later generations remain valid without rewriting logical catalog state.
    pub ready_from_revision: u64,
    pub membership: RecipeIdentity,
    pub fields: Vec<LogicalFieldBinding>,
}

impl LogicalProjectionBinding {
    pub fn validate_against(&self, generation: &ProjectionGeneration) -> Result<(), IndexError> {
        generation.validate()?;
        if self.logical_index_id == 0
            || self.logical_definition_version == 0
            || self.family_id != generation.family_id
            || self.ready_from_revision == 0
            || self.ready_from_revision > generation.revision
            || generation
                .root(ComponentIdentity::Membership(self.membership))
                .is_none()
        {
            return Err(IndexError::InvalidDefinition(
                "logical projection binding has no complete physical generation".into(),
            ));
        }
        let mut ids = BTreeSet::new();
        let mut names = BTreeSet::new();
        for field in &self.fields {
            if field.public_name.is_empty()
                || field.public_name.len() > INDEX_ROUTING_KEY_BYTES
                || field.public_name.contains('\0')
                || !ids.insert(field.public_field_id)
                || !names.insert(field.public_name.as_str())
                || generation
                    .root(ComponentIdentity::Field(field.recipe))
                    .is_none()
            {
                return Err(IndexError::InvalidDefinition(
                    "logical field binding is invalid or physically incomplete".into(),
                ));
            }
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

    fn root(component: ComponentIdentity, byte: u8) -> ComponentRoot {
        ComponentRoot::new(component, [byte; 32], 1, 100, 80, 20).unwrap()
    }

    fn initial() -> ProjectionGeneration {
        ProjectionGeneration::initial(
            [1; 32],
            ProjectionBarrier::new(vec![(1, [1; 32], 8)], None).unwrap(),
            vec![
                root(ComponentIdentity::DocumentHead, 2),
                root(ComponentIdentity::ProjectedState, 6),
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
                ProjectionBarrier::new(vec![(1, [1; 32], 9)], None).unwrap(),
                vec![root(ComponentIdentity::Field(recipe(4)), 8)],
            )
            .unwrap();
        assert_eq!(new.revision, 2);
        assert_eq!(
            new.root(ComponentIdentity::Field(recipe(5))),
            old.root(ComponentIdentity::Field(recipe(5)))
        );
        assert_eq!(
            new.root(ComponentIdentity::Membership(recipe(3))),
            old.root(ComponentIdentity::Membership(recipe(3)))
        );
        assert_ne!(
            new.root(ComponentIdentity::Field(recipe(4))),
            old.root(ComponentIdentity::Field(recipe(4)))
        );
    }

    #[test]
    fn logical_subsets_and_aliases_bind_the_same_physical_roots() {
        let generation = initial();
        let first = LogicalProjectionBinding {
            logical_index_id: 10,
            logical_definition_version: 1,
            family_id: generation.family_id,
            ready_from_revision: 1,
            membership: recipe(3),
            fields: vec![LogicalFieldBinding {
                public_field_id: 0,
                public_name: "state".into(),
                recipe: recipe(4),
            }],
        };
        let second = LogicalProjectionBinding {
            logical_index_id: 11,
            logical_definition_version: 7,
            family_id: generation.family_id,
            ready_from_revision: 1,
            membership: recipe(3),
            fields: vec![
                LogicalFieldBinding {
                    public_field_id: 4,
                    public_name: "advisory_state".into(),
                    recipe: recipe(4),
                },
                LogicalFieldBinding {
                    public_field_id: 9,
                    public_name: "ecosystem".into(),
                    recipe: recipe(5),
                },
            ],
        };
        first.validate_against(&generation).unwrap();
        second.validate_against(&generation).unwrap();
        assert_eq!(first.fields[0].recipe, second.fields[0].recipe);
    }

    #[test]
    fn generation_barriers_cannot_regress() {
        let old = initial();
        assert!(
            old.advance(
                [9; 32],
                ProjectionBarrier::new(vec![(1, [1; 32], 7)], None).unwrap(),
                Vec::new(),
            )
            .is_err()
        );
    }

    #[test]
    fn empty_family_has_a_complete_barrier_without_fabricated_components() {
        let barrier = ProjectionBarrier::new(vec![(1, [1; 32], 8)], None).unwrap();
        let empty = ProjectionGeneration::initial([7; 32], barrier.clone(), Vec::new()).unwrap();
        assert!(empty.roots.is_empty());
        assert!(barrier.covers(&empty.barrier));

        let advanced = empty
            .advance(
                [9; 32],
                ProjectionBarrier::new(vec![(1, [1; 32], 9)], None).unwrap(),
                Vec::new(),
            )
            .unwrap();
        assert!(advanced.roots.is_empty());
    }
}
