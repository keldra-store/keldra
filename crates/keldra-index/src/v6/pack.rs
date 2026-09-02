use crate::IndexError;
const INDEX_ARTIFACT_PACK_BYTES: usize = 64 * 1024 * 1024;

use super::{
    ComponentIdentity, IndexingMemoryPermit, SealedComponentDelta, StableDocumentKey,
    decode_component_delta_segment,
};

#[cfg(test)]
pub(crate) fn test_pack_credits(bytes: usize) -> ProjectionPackCredits {
    use super::{IndexingMemoryCredits, IndexingMemoryLimits, IndexingMemoryStage};

    let memory = IndexingMemoryCredits::new(
        bytes,
        IndexingMemoryLimits {
            hot_payload_bytes: bytes,
            worker_scratch_bytes: bytes,
            prepared_rows_bytes: bytes,
            replay_input_bytes: bytes,
            projection_accumulator_bytes: bytes,
            seal_scratch_bytes: bytes,
            ordering_catalog_bytes: bytes,
        },
    )
    .unwrap();
    ProjectionPackCredits::from_pipeline_permit(
        memory
            .acquire(IndexingMemoryStage::SealScratch, bytes)
            .unwrap(),
    )
}

/// Destination admission retained for as long as integrated pack bytes live.
#[derive(Debug)]
pub struct ProjectionPackCredits {
    remaining: usize,
    _permit: IndexingMemoryPermit,
}

impl ProjectionPackCredits {
    pub fn from_pipeline_permit(permit: IndexingMemoryPermit) -> Self {
        Self {
            remaining: permit.bytes(),
            _permit: permit,
        }
    }

    pub const fn remaining(&self) -> usize {
        self.remaining
    }

    fn reserve(&mut self, bytes: usize) -> Result<(), IndexError> {
        if bytes > self.remaining {
            return Err(IndexError::ResourceLimit {
                needed: bytes,
                limit: self.remaining,
            });
        }
        self.remaining -= bytes;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ChargedProjectionDeltaPacks {
    pub packs: Vec<SealedProjectionDeltaPack>,
    pub(crate) credits: ProjectionPackCredits,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedComponentDelta {
    pub component: ComponentIdentity,
    pub pack_hash: [u8; 32],
    pub offset: u64,
    pub encoded_bytes: u64,
    pub logical_bytes: u64,
    pub records: u64,
    pub segment_hash: [u8; 32],
    pub minimum_key: StableDocumentKey,
    pub maximum_key: StableDocumentKey,
}

#[derive(Debug, Eq, PartialEq)]
pub struct SealedProjectionDeltaPack {
    pub hash: [u8; 32],
    pub bytes: Vec<u8>,
    pub deltas: Vec<PackedComponentDelta>,
}

/// Pack independently reusable deltas into bounded integrated payload values.
/// A delta is never split and an empty pack is never emitted.
pub fn pack_component_deltas(
    deltas: Vec<SealedComponentDelta>,
    mut credits: ProjectionPackCredits,
) -> Result<ChargedProjectionDeltaPacks, IndexError> {
    if deltas.is_empty() {
        return Ok(ChargedProjectionDeltaPacks {
            packs: Vec::new(),
            credits,
        });
    }
    let packed_bytes = deltas.iter().try_fold(0usize, |total, delta| {
        validate_delta(delta)?;
        total
            .checked_add(delta.bytes.len())
            .ok_or(IndexError::OffsetOverflow)
    })?;
    // Refuse before allocating/copying the first destination byte. The caller
    // still owns its sealed-output admission at this point, so this second
    // permit proves the transient coexistence also fits the hard node budget.
    credits.reserve(packed_bytes)?;
    let mut packs = Vec::new();
    let mut bytes = Vec::new();
    let mut staged = Vec::new();
    for delta in deltas {
        if delta.bytes.len() > INDEX_ARTIFACT_PACK_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: delta.bytes.len(),
                limit: INDEX_ARTIFACT_PACK_BYTES,
            });
        }
        if !bytes.is_empty()
            && bytes
                .len()
                .checked_add(delta.bytes.len())
                .is_none_or(|needed| needed > INDEX_ARTIFACT_PACK_BYTES)
        {
            packs.push(seal_pack(bytes, staged)?);
            bytes = Vec::new();
            staged = Vec::new();
        }
        let offset = bytes.len() as u64;
        bytes.extend_from_slice(&delta.bytes);
        staged.push((delta, offset));
    }
    if !bytes.is_empty() {
        packs.push(seal_pack(bytes, staged)?);
    }
    Ok(ChargedProjectionDeltaPacks { packs, credits })
}

fn seal_pack(
    bytes: Vec<u8>,
    staged: Vec<(SealedComponentDelta, u64)>,
) -> Result<SealedProjectionDeltaPack, IndexError> {
    if bytes.is_empty() || bytes.len() > INDEX_ARTIFACT_PACK_BYTES || staged.is_empty() {
        return Err(IndexError::InvalidDefinition(
            "projection delta pack is empty or unbounded".into(),
        ));
    }
    let hash = *blake3::hash(&bytes).as_bytes();
    let deltas = staged
        .into_iter()
        .map(|(delta, offset)| PackedComponentDelta {
            component: delta.component,
            pack_hash: hash,
            offset,
            encoded_bytes: delta.encoded_bytes,
            logical_bytes: delta.logical_bytes,
            records: delta.records,
            segment_hash: delta.hash,
            minimum_key: delta.minimum_key,
            maximum_key: delta.maximum_key,
        })
        .collect();
    Ok(SealedProjectionDeltaPack {
        hash,
        bytes,
        deltas,
    })
}

fn validate_delta(delta: &SealedComponentDelta) -> Result<(), IndexError> {
    let decoded = decode_component_delta_segment(&delta.bytes)?;
    if decoded.component != delta.component
        || delta.hash != *blake3::hash(&delta.bytes).as_bytes()
        || delta.encoded_bytes != delta.bytes.len() as u64
        || delta.records != decoded.records.len() as u64
    {
        return Err(IndexError::Integrity);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::v6::buffer::seal_component;
    use crate::v6::{RecipeIdentity, StableDocumentKey};

    fn delta(component: ComponentIdentity, byte: u8, bytes: usize) -> SealedComponentDelta {
        seal_component(
            component,
            BTreeMap::from([(
                StableDocumentKey::from_bytes([byte; 32]).unwrap(),
                Some(vec![byte; bytes]),
            )]),
        )
        .unwrap()
    }

    #[test]
    fn many_tiny_components_share_one_integrity_checked_pack() {
        let deltas: Vec<_> = (1_u8..=64)
            .map(|byte| {
                delta(
                    ComponentIdentity::Field(RecipeIdentity::new([byte; 32]).unwrap()),
                    byte,
                    8,
                )
            })
            .collect();
        let bytes = deltas.iter().map(|delta| delta.bytes.len()).sum();
        let packs = pack_component_deltas(deltas, test_pack_credits(bytes))
            .unwrap()
            .packs;
        assert_eq!(packs.len(), 1);
        assert_eq!(packs[0].deltas.len(), 64);
        assert_eq!(packs[0].hash, *blake3::hash(&packs[0].bytes).as_bytes());
        for delta in &packs[0].deltas {
            let start = delta.offset as usize;
            let end = start + delta.encoded_bytes as usize;
            assert_eq!(
                delta.segment_hash,
                *blake3::hash(&packs[0].bytes[start..end]).as_bytes()
            );
        }
    }

    #[test]
    fn packs_split_only_at_the_existing_byte_bound() {
        let component = ComponentIdentity::DocumentHead;
        let first = delta(component, 1, INDEX_ARTIFACT_PACK_BYTES / 2);
        let second = delta(component, 2, INDEX_ARTIFACT_PACK_BYTES / 2);
        let bytes = first.bytes.len() + second.bytes.len();
        let packs = pack_component_deltas(vec![first, second], test_pack_credits(bytes))
            .unwrap()
            .packs;
        assert_eq!(packs.len(), 2);
        assert!(
            packs
                .iter()
                .all(|pack| pack.bytes.len() <= INDEX_ARTIFACT_PACK_BYTES)
        );
    }

    #[test]
    fn destination_memory_is_refused_before_pack_copy() {
        let delta = delta(ComponentIdentity::DocumentHead, 1, 4 * 1024);
        let needed = delta.bytes.len();
        let error = pack_component_deltas(vec![delta], test_pack_credits(needed - 1)).unwrap_err();
        assert_eq!(
            error,
            IndexError::ResourceLimit {
                needed,
                limit: needed - 1,
            }
        );
    }
}
