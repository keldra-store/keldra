use std::collections::{BTreeMap, BTreeSet};

use super::{
    MAX_QUERY_RECIPE_CATALOG_PROOFS, MAX_RETAINED_CATALOG_GENERATIONS_PER_RECIPE,
    ProjectionGenerationReference, ProjectionPartitionIdentity, QueryRecipeCatalogProof,
};
use crate::IndexError;

const DIRECTORY_MAGIC: &[u8; 8] = b"K6FDIR01";
const ACTIVATION_MAGIC: &[u8; 8] = b"K6CACT01";
const FORMAT: u16 = 2;
const MAX_PARTITIONS: usize = 65_536;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionPartitionLifecycle {
    Active,
    Retiring {
        successor: ProjectionPartitionIdentity,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionPartitionDirectoryEntry {
    pub partition: ProjectionPartitionIdentity,
    pub lifecycle: ProjectionPartitionLifecycle,
    pub covered_predecessors: Vec<ProjectionGenerationReference>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionFamilyPartitionDirectory {
    pub family_id: [u8; 32],
    pub revision: u64,
    pub entries: Vec<ProjectionPartitionDirectoryEntry>,
}

impl ProjectionFamilyPartitionDirectory {
    pub fn validate(&self) -> Result<(), IndexError> {
        if self.family_id == [0; 32]
            || self.revision == 0
            || self.entries.len() > MAX_PARTITIONS
            || self
                .entries
                .windows(2)
                .any(|p| p[0].partition >= p[1].partition)
        {
            return invalid("projection family partition directory is invalid");
        }
        let identities = self
            .entries
            .iter()
            .map(|e| e.partition)
            .collect::<BTreeSet<_>>();
        let mut covered_identities = BTreeSet::new();
        for entry in &self.entries {
            entry.partition.validate()?;
            if entry.partition.family_id != self.family_id
                || entry.covered_predecessors.len() > MAX_PARTITIONS
                || entry.covered_predecessors.windows(2).any(|p| p[0] >= p[1])
            {
                return invalid("projection partition directory entry is invalid");
            }
            for covered in &entry.covered_predecessors {
                covered.validate()?;
                if covered.partition.family_id != self.family_id
                    || covered.partition == entry.partition
                    || covered.partition.source_node != entry.partition.source_node
                    || covered.partition.source_epoch != entry.partition.source_epoch
                    || !covered_identities.insert(covered.partition)
                {
                    return invalid("covered predecessor has an invalid catalog identity");
                }
            }
            if let ProjectionPartitionLifecycle::Retiring { successor } = entry.lifecycle {
                if successor == entry.partition
                    || successor.source_node != entry.partition.source_node
                    || successor.source_epoch != entry.partition.source_epoch
                    || (successor.placement_term, successor.placement_index)
                        <= (
                            entry.partition.placement_term,
                            entry.partition.placement_index,
                        )
                    || !identities.contains(&successor)
                {
                    return invalid("retiring projection partition has no successor");
                }
            }
        }
        Ok(())
    }

    pub fn complete_handoff(
        &self,
        retiring: &[ProjectionGenerationReference],
    ) -> Result<Self, IndexError> {
        self.validate()?;
        let retiring = retiring.iter().copied().collect::<BTreeSet<_>>();
        if retiring.is_empty() {
            return invalid("projection handoff has no retiring roots");
        }
        for reference in &retiring {
            let entry = self
                .entries
                .iter()
                .find(|e| e.partition == reference.partition)
                .ok_or_else(|| {
                    IndexError::InvalidDefinition("retiring root is not current".into())
                })?;
            let ProjectionPartitionLifecycle::Retiring { successor } = entry.lifecycle else {
                return invalid("projection handoff root is not retiring");
            };
            let successor = self
                .entries
                .iter()
                .find(|e| e.partition == successor)
                .ok_or(IndexError::Integrity)?;
            if successor.lifecycle != ProjectionPartitionLifecycle::Active
                || successor
                    .covered_predecessors
                    .binary_search(reference)
                    .is_err()
            {
                return invalid("projection successor does not cover retiring root");
            }
        }
        let mut next = self.clone();
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        next.entries.retain(|entry| {
            !retiring
                .iter()
                .any(|reference| reference.partition == entry.partition)
        });
        next.validate()?;
        Ok(next)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CatalogBaseline {
    pub source_node: u64,
    pub source_epoch: [u8; 32],
    pub next_offset: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionCatalogActivation {
    pub family_id: [u8; 32],
    pub physical_catalog_generation: [u8; 32],
    /// Logical definitions may advance independently while reusing the same
    /// `physical_catalog_generation` and partition roots.
    pub definition_catalog_hash: [u8; 32],
    pub recipe_catalog_hash: [u8; 32],
    pub baseline: Vec<CatalogBaseline>,
    pub required_partitions: Vec<ProjectionPartitionIdentity>,
    pub activated_coverage: Vec<ProjectionGenerationReference>,
    /// Per-recipe catalog lineage accepted by query readers. Changed/new
    /// recipes contain only the active generation; retained recipes explicitly
    /// name each older compatible generation.
    pub recipe_catalog_proofs: Vec<QueryRecipeCatalogProof>,
}

impl ProjectionCatalogActivation {
    pub fn validate(&self) -> Result<(), IndexError> {
        if self.family_id == [0; 32]
            || self.physical_catalog_generation == [0; 32]
            || self.definition_catalog_hash == [0; 32]
            || self.recipe_catalog_hash == [0; 32]
            || self.baseline.is_empty()
            || self.required_partitions.is_empty()
            || self.baseline.len() > MAX_PARTITIONS
            || self.required_partitions.len() > MAX_PARTITIONS
            || self.activated_coverage.len() != self.required_partitions.len()
            || self.recipe_catalog_proofs.len() > MAX_QUERY_RECIPE_CATALOG_PROOFS
            || self.baseline.windows(2).any(|p| p[0] >= p[1])
            || self.baseline.windows(2).any(|p| {
                (p[0].source_node, p[0].source_epoch) >= (p[1].source_node, p[1].source_epoch)
            })
            || self.required_partitions.windows(2).any(|p| p[0] >= p[1])
            || self.activated_coverage.windows(2).any(|p| p[0] >= p[1])
            || self
                .recipe_catalog_proofs
                .windows(2)
                .any(|p| p[0].recipe >= p[1].recipe)
        {
            return invalid("projection catalog activation is invalid");
        }
        for proof in &self.recipe_catalog_proofs {
            proof.validate(self.physical_catalog_generation)?;
        }
        if self
            .baseline
            .iter()
            .any(|baseline| baseline.source_node == 0 || baseline.source_epoch == [0; 32])
        {
            return invalid("projection catalog baseline identity is invalid");
        }
        let coverage = self
            .activated_coverage
            .iter()
            .map(|r| (r.partition, r))
            .collect::<BTreeMap<_, _>>();
        if coverage.len() != self.activated_coverage.len()
            || self.activated_coverage.iter().any(|reference| {
                reference.validate().is_err()
                    || reference.partition.family_id != self.family_id
                    || reference.physical_catalog_generation != self.physical_catalog_generation
            })
        {
            return invalid("projection activation coverage identity is invalid");
        }
        for partition in &self.required_partitions {
            partition.validate()?;
            let reference = coverage.get(partition).ok_or_else(|| {
                IndexError::InvalidDefinition("activation lacks required coverage".into())
            })?;
            let baseline = self
                .baseline
                .iter()
                .find(|b| {
                    b.source_node == partition.source_node
                        && b.source_epoch == partition.source_epoch
                })
                .ok_or_else(|| {
                    IndexError::InvalidDefinition("activation lacks source baseline".into())
                })?;
            if partition.family_id != self.family_id || reference.next_offset < baseline.next_offset
            {
                return invalid("projection activation does not cover baseline");
            }
        }
        Ok(())
    }
}

pub fn encode_projection_family_directory(
    v: &ProjectionFamilyPartitionDirectory,
) -> Result<Vec<u8>, IndexError> {
    v.validate()?;
    let mut out = header(DIRECTORY_MAGIC);
    out.extend_from_slice(&v.family_id);
    u64_(&mut out, v.revision);
    u32_(&mut out, v.entries.len() as u32);
    for e in &v.entries {
        partition(&mut out, e.partition);
        match e.lifecycle {
            ProjectionPartitionLifecycle::Active => out.push(0),
            ProjectionPartitionLifecycle::Retiring { successor } => {
                out.push(1);
                partition(&mut out, successor);
            }
        };
        u32_(&mut out, e.covered_predecessors.len() as u32);
        for r in &e.covered_predecessors {
            reference(&mut out, *r);
        }
    }
    integrity(&mut out);
    Ok(out)
}

pub fn decode_projection_family_directory(
    bytes: &[u8],
) -> Result<ProjectionFamilyPartitionDirectory, IndexError> {
    let mut d = Decoder::new(verified(bytes)?);
    d.header(DIRECTORY_MAGIC)?;
    let family_id = d.a32()?;
    let revision = d.u64()?;
    let count = d.count()?;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let partition = d.partition()?;
        let lifecycle = match d.byte()? {
            0 => ProjectionPartitionLifecycle::Active,
            1 => ProjectionPartitionLifecycle::Retiring {
                successor: d.partition()?,
            },
            _ => return Err(IndexError::Decode("partition lifecycle".into())),
        };
        let n = d.count()?;
        let mut covered_predecessors = Vec::with_capacity(n);
        for _ in 0..n {
            covered_predecessors.push(d.reference()?);
        }
        entries.push(ProjectionPartitionDirectoryEntry {
            partition,
            lifecycle,
            covered_predecessors,
        });
    }
    d.finish()?;
    let v = ProjectionFamilyPartitionDirectory {
        family_id,
        revision,
        entries,
    };
    v.validate()?;
    Ok(v)
}

pub fn encode_projection_catalog_activation(
    v: &ProjectionCatalogActivation,
) -> Result<Vec<u8>, IndexError> {
    v.validate()?;
    let mut out = header(ACTIVATION_MAGIC);
    out.extend_from_slice(&v.family_id);
    out.extend_from_slice(&v.physical_catalog_generation);
    out.extend_from_slice(&v.definition_catalog_hash);
    out.extend_from_slice(&v.recipe_catalog_hash);
    u32_(&mut out, v.baseline.len() as u32);
    for b in &v.baseline {
        u64_(&mut out, b.source_node);
        out.extend_from_slice(&b.source_epoch);
        u64_(&mut out, b.next_offset);
    }
    u32_(&mut out, v.required_partitions.len() as u32);
    for p in &v.required_partitions {
        partition(&mut out, *p);
    }
    u32_(&mut out, v.activated_coverage.len() as u32);
    for r in &v.activated_coverage {
        reference(&mut out, *r);
    }
    u32_(&mut out, v.recipe_catalog_proofs.len() as u32);
    for proof in &v.recipe_catalog_proofs {
        out.extend_from_slice(&proof.recipe.bytes());
        u32_(&mut out, proof.accepted_catalog_generations.len() as u32);
        for generation in &proof.accepted_catalog_generations {
            out.extend_from_slice(generation);
        }
    }
    integrity(&mut out);
    Ok(out)
}

pub fn decode_projection_catalog_activation(
    bytes: &[u8],
) -> Result<ProjectionCatalogActivation, IndexError> {
    let mut d = Decoder::new(verified(bytes)?);
    d.header(ACTIVATION_MAGIC)?;
    let family_id = d.a32()?;
    let physical_catalog_generation = d.a32()?;
    let definition_catalog_hash = d.a32()?;
    let recipe_catalog_hash = d.a32()?;
    let n = d.count()?;
    let mut baseline = Vec::with_capacity(n);
    for _ in 0..n {
        baseline.push(CatalogBaseline {
            source_node: d.u64()?,
            source_epoch: d.a32()?,
            next_offset: d.u64()?,
        });
    }
    let n = d.count()?;
    let mut required_partitions = Vec::with_capacity(n);
    for _ in 0..n {
        required_partitions.push(d.partition()?);
    }
    let n = d.count()?;
    let mut activated_coverage = Vec::with_capacity(n);
    for _ in 0..n {
        activated_coverage.push(d.reference()?);
    }
    let n = d.count()?;
    let mut recipe_catalog_proofs = Vec::with_capacity(n);
    for _ in 0..n {
        let recipe = crate::v6::RecipeIdentity::new(d.a32()?)?;
        let generations = d.count()?;
        if generations > MAX_RETAINED_CATALOG_GENERATIONS_PER_RECIPE {
            return invalid("query recipe catalog proof generation count exceeds limit");
        }
        let mut accepted_catalog_generations = Vec::with_capacity(generations);
        for _ in 0..generations {
            accepted_catalog_generations.push(d.a32()?);
        }
        recipe_catalog_proofs.push(QueryRecipeCatalogProof {
            recipe,
            accepted_catalog_generations,
        });
    }
    d.finish()?;
    let v = ProjectionCatalogActivation {
        family_id,
        physical_catalog_generation,
        definition_catalog_hash,
        recipe_catalog_hash,
        baseline,
        required_partitions,
        activated_coverage,
        recipe_catalog_proofs,
    };
    v.validate()?;
    Ok(v)
}

fn invalid<T>(message: &str) -> Result<T, IndexError> {
    Err(IndexError::InvalidDefinition(message.into()))
}
fn header(magic: &[u8; 8]) -> Vec<u8> {
    let mut v = magic.to_vec();
    v.extend_from_slice(&FORMAT.to_le_bytes());
    v
}
fn partition(out: &mut Vec<u8>, v: ProjectionPartitionIdentity) {
    out.extend_from_slice(&v.family_id);
    u64_(out, v.source_node);
    out.extend_from_slice(&v.source_epoch);
    u64_(out, v.producer_node);
    u64_(out, v.placement_term);
    u64_(out, v.placement_index);
}
fn reference(out: &mut Vec<u8>, v: ProjectionGenerationReference) {
    partition(out, v.partition);
    out.extend_from_slice(&v.physical_catalog_generation);
    out.extend_from_slice(&v.generation_hash);
    u64_(out, v.generation_revision);
    u64_(out, v.next_offset);
    u64_(out, v.through_atomic_position);
}
fn u32_(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes())
}
fn u64_(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes())
}
fn integrity(out: &mut Vec<u8>) {
    out.extend_from_slice(blake3::hash(out).as_bytes())
}
fn verified(bytes: &[u8]) -> Result<&[u8], IndexError> {
    let n = bytes
        .len()
        .checked_sub(32)
        .ok_or(IndexError::UnexpectedEof {
            expected: 32,
            actual: bytes.len() as u64,
        })?;
    let (p, h) = bytes.split_at(n);
    if blake3::hash(p).as_bytes() != h {
        return Err(IndexError::Integrity);
    }
    Ok(p)
}
struct Decoder<'a> {
    b: &'a [u8],
    o: usize,
}
impl<'a> Decoder<'a> {
    fn new(b: &'a [u8]) -> Self {
        Self { b, o: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], IndexError> {
        let e = self.o.checked_add(n).ok_or(IndexError::OffsetOverflow)?;
        let v = self.b.get(self.o..e).ok_or(IndexError::UnexpectedEof {
            expected: e as u64,
            actual: self.b.len() as u64,
        })?;
        self.o = e;
        Ok(v)
    }
    fn header(&mut self, m: &[u8; 8]) -> Result<(), IndexError> {
        if self.take(8)? != m || self.u16()? != FORMAT {
            return Err(IndexError::InvalidFormat("projection lifecycle format"));
        }
        Ok(())
    }
    fn byte(&mut self) -> Result<u8, IndexError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, IndexError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, IndexError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, IndexError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn a32(&mut self) -> Result<[u8; 32], IndexError> {
        Ok(self.take(32)?.try_into().unwrap())
    }
    fn count(&mut self) -> Result<usize, IndexError> {
        let n = self.u32()? as usize;
        if n > MAX_PARTITIONS {
            return Err(IndexError::InvalidFormat("projection lifecycle count"));
        }
        Ok(n)
    }
    fn partition(&mut self) -> Result<ProjectionPartitionIdentity, IndexError> {
        ProjectionPartitionIdentity::new(
            self.a32()?,
            self.u64()?,
            self.a32()?,
            self.u64()?,
            self.u64()?,
            self.u64()?,
        )
    }
    fn reference(&mut self) -> Result<ProjectionGenerationReference, IndexError> {
        let v = ProjectionGenerationReference {
            partition: self.partition()?,
            physical_catalog_generation: self.a32()?,
            generation_hash: self.a32()?,
            generation_revision: self.u64()?,
            next_offset: self.u64()?,
            through_atomic_position: self.u64()?,
        };
        v.validate()?;
        Ok(v)
    }
    fn finish(self) -> Result<(), IndexError> {
        if self.o == self.b.len() {
            Ok(())
        } else {
            Err(IndexError::Decode(
                "projection lifecycle trailing bytes".into(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn part(node: u64) -> ProjectionPartitionIdentity {
        ProjectionPartitionIdentity::new([1; 32], node, [node as u8; 32], node, 3, node).unwrap()
    }
    fn reference(node: u64) -> ProjectionGenerationReference {
        ProjectionGenerationReference {
            partition: part(node),
            physical_catalog_generation: [2; 32],
            generation_hash: [node as u8; 32],
            generation_revision: 1,
            next_offset: 20,
            through_atomic_position: 19,
        }
    }
    #[test]
    fn directory_keeps_retiring_root_until_successor_proves_coverage() {
        let old = reference(1);
        let mut new = reference(1);
        new.partition.producer_node = 2;
        new.partition.placement_term = 4;
        new.partition.placement_index = 1;
        new.generation_hash = [2; 32];
        let directory = ProjectionFamilyPartitionDirectory {
            family_id: [1; 32],
            revision: 1,
            entries: vec![
                ProjectionPartitionDirectoryEntry {
                    partition: old.partition,
                    lifecycle: ProjectionPartitionLifecycle::Retiring {
                        successor: new.partition,
                    },
                    covered_predecessors: vec![],
                },
                ProjectionPartitionDirectoryEntry {
                    partition: new.partition,
                    lifecycle: ProjectionPartitionLifecycle::Active,
                    covered_predecessors: vec![old],
                },
            ],
        };
        let encoded = encode_projection_family_directory(&directory).unwrap();
        assert_eq!(
            decode_projection_family_directory(&encoded).unwrap(),
            directory
        );
        let complete = directory.complete_handoff(&[old]).unwrap();
        assert_eq!(complete.entries.len(), 1);
        assert_eq!(complete.entries[0].partition, new.partition);
    }

    #[test]
    fn handoff_rejects_a_successor_for_another_source_incarnation() {
        let old = reference(1);
        let mut successor = reference(2);
        successor.partition.producer_node = 3;
        successor.partition.placement_term = 4;
        let directory = ProjectionFamilyPartitionDirectory {
            family_id: [1; 32],
            revision: 1,
            entries: vec![
                ProjectionPartitionDirectoryEntry {
                    partition: old.partition,
                    lifecycle: ProjectionPartitionLifecycle::Retiring {
                        successor: successor.partition,
                    },
                    covered_predecessors: Vec::new(),
                },
                ProjectionPartitionDirectoryEntry {
                    partition: successor.partition,
                    lifecycle: ProjectionPartitionLifecycle::Active,
                    covered_predecessors: vec![old],
                },
            ],
        };
        assert!(directory.validate().is_err());
    }

    #[test]
    fn handoff_allows_fence_refresh_on_the_same_producer_only_forward() {
        let old = reference(1);
        let mut successor = old;
        successor.partition.placement_term = 4;
        successor.generation_hash = [9; 32];
        let directory = ProjectionFamilyPartitionDirectory {
            family_id: [1; 32],
            revision: 1,
            entries: vec![
                ProjectionPartitionDirectoryEntry {
                    partition: old.partition,
                    lifecycle: ProjectionPartitionLifecycle::Retiring {
                        successor: successor.partition,
                    },
                    covered_predecessors: Vec::new(),
                },
                ProjectionPartitionDirectoryEntry {
                    partition: successor.partition,
                    lifecycle: ProjectionPartitionLifecycle::Active,
                    covered_predecessors: vec![old],
                },
            ],
        };
        assert!(directory.validate().is_ok());

        let mut backward = directory.clone();
        backward.entries[1].partition.placement_term = 2;
        assert!(backward.validate().is_err());
    }
    #[test]
    fn activation_requires_every_partition_to_cover_its_baseline() {
        let p = part(1);
        let r = reference(1);
        let activation = ProjectionCatalogActivation {
            family_id: [1; 32],
            physical_catalog_generation: [2; 32],
            definition_catalog_hash: [3; 32],
            recipe_catalog_hash: [4; 32],
            baseline: vec![CatalogBaseline {
                source_node: 1,
                source_epoch: [1; 32],
                next_offset: 19,
            }],
            required_partitions: vec![p],
            activated_coverage: vec![r],
            recipe_catalog_proofs: vec![QueryRecipeCatalogProof {
                recipe: crate::v6::RecipeIdentity::new([9; 32]).unwrap(),
                accepted_catalog_generations: vec![[2; 32]],
            }],
        };
        let encoded = encode_projection_catalog_activation(&activation).unwrap();
        assert_eq!(
            decode_projection_catalog_activation(&encoded).unwrap(),
            activation
        );
        let mut incomplete = activation;
        incomplete.baseline[0].next_offset = 21;
        assert!(incomplete.validate().is_err());
    }

    #[test]
    fn activation_round_trips_retained_and_new_recipe_catalog_proofs() {
        let activation = ProjectionCatalogActivation {
            family_id: [1; 32],
            physical_catalog_generation: [2; 32],
            definition_catalog_hash: [3; 32],
            recipe_catalog_hash: [4; 32],
            baseline: vec![CatalogBaseline {
                source_node: 1,
                source_epoch: [1; 32],
                next_offset: 19,
            }],
            required_partitions: vec![part(1)],
            activated_coverage: vec![reference(1)],
            recipe_catalog_proofs: vec![
                QueryRecipeCatalogProof {
                    recipe: crate::v6::RecipeIdentity::new([8; 32]).unwrap(),
                    accepted_catalog_generations: vec![[1; 32], [2; 32]],
                },
                QueryRecipeCatalogProof {
                    recipe: crate::v6::RecipeIdentity::new([9; 32]).unwrap(),
                    accepted_catalog_generations: vec![[2; 32]],
                },
            ],
        };
        let encoded = encode_projection_catalog_activation(&activation).unwrap();
        assert_eq!(
            decode_projection_catalog_activation(&encoded).unwrap(),
            activation
        );
    }

    #[test]
    fn ordinary_partition_flush_does_not_change_family_directory_bytes() {
        let partition = part(1);
        let directory = ProjectionFamilyPartitionDirectory {
            family_id: [1; 32],
            revision: 1,
            entries: vec![ProjectionPartitionDirectoryEntry {
                partition,
                lifecycle: ProjectionPartitionLifecycle::Active,
                covered_predecessors: Vec::new(),
            }],
        };
        let before = encode_projection_family_directory(&directory).unwrap();
        let mut newer_current = reference(1);
        newer_current.generation_revision = 99;
        newer_current.next_offset = 50_000;
        assert_eq!(
            encode_projection_family_directory(&directory).unwrap(),
            before
        );
        assert_eq!(directory.entries[0].partition, newer_current.partition);
    }

    #[test]
    fn lineage_and_activation_reject_cross_catalog_and_duplicate_evidence() {
        let old = reference(1);
        let mut cross = reference(3);
        cross.partition.family_id = [9; 32];
        let directory = ProjectionFamilyPartitionDirectory {
            family_id: [1; 32],
            revision: 1,
            entries: vec![ProjectionPartitionDirectoryEntry {
                partition: old.partition,
                lifecycle: ProjectionPartitionLifecycle::Active,
                covered_predecessors: vec![cross],
            }],
        };
        assert!(directory.validate().is_err());

        let mut duplicate_predecessor = old;
        duplicate_predecessor.generation_revision = 2;
        duplicate_predecessor.generation_hash = [8; 32];
        let duplicate_lineage = ProjectionFamilyPartitionDirectory {
            family_id: [1; 32],
            revision: 1,
            entries: vec![ProjectionPartitionDirectoryEntry {
                partition: part(2),
                lifecycle: ProjectionPartitionLifecycle::Active,
                covered_predecessors: vec![old, duplicate_predecessor],
            }],
        };
        assert!(duplicate_lineage.validate().is_err());

        let mut activation = ProjectionCatalogActivation {
            family_id: [1; 32],
            physical_catalog_generation: [2; 32],
            definition_catalog_hash: [3; 32],
            recipe_catalog_hash: [4; 32],
            baseline: vec![CatalogBaseline {
                source_node: 1,
                source_epoch: [1; 32],
                next_offset: 10,
            }],
            required_partitions: vec![old.partition],
            activated_coverage: vec![old],
            recipe_catalog_proofs: vec![QueryRecipeCatalogProof {
                recipe: crate::v6::RecipeIdentity::new([9; 32]).unwrap(),
                accepted_catalog_generations: vec![[2; 32]],
            }],
        };
        activation.baseline.push(CatalogBaseline {
            source_node: 1,
            source_epoch: [1; 32],
            next_offset: 11,
        });
        assert!(activation.validate().is_err());
    }
}
