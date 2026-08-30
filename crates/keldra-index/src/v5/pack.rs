use crate::IndexError;
use crate::v4::INDEX_ARTIFACT_PACK_BYTES;

use super::{ComponentIdentity, SealedComponentDelta, decode_component_delta_segment};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedComponentDelta {
    pub component: ComponentIdentity,
    pub pack_hash: [u8; 32],
    pub offset: u64,
    pub encoded_bytes: u64,
    pub logical_bytes: u64,
    pub records: u64,
    pub segment_hash: [u8; 32],
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
) -> Result<Vec<SealedProjectionDeltaPack>, IndexError> {
    if deltas.is_empty() {
        return Ok(Vec::new());
    }
    let mut packs = Vec::new();
    let mut bytes = Vec::new();
    let mut staged = Vec::new();
    for delta in deltas {
        validate_delta(&delta)?;
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
    Ok(packs)
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
            component: delta.root.component,
            pack_hash: hash,
            offset,
            encoded_bytes: delta.root.encoded_bytes,
            logical_bytes: delta.root.logical_bytes,
            records: delta.records,
            segment_hash: delta.root.artifact_hash,
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
    if decoded.component != delta.root.component
        || delta.root.artifact_hash != *blake3::hash(&delta.bytes).as_bytes()
        || delta.root.encoded_bytes != delta.bytes.len() as u64
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
    use crate::v5::buffer::seal_component;
    use crate::v5::{RecipeIdentity, StableDocumentKey};

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
        let deltas = (1_u8..=64)
            .map(|byte| {
                delta(
                    ComponentIdentity::Field(RecipeIdentity::new([byte; 32]).unwrap()),
                    byte,
                    8,
                )
            })
            .collect();
        let packs = pack_component_deltas(deltas).unwrap();
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
        let component = ComponentIdentity::ProjectedState;
        let first = delta(component, 1, INDEX_ARTIFACT_PACK_BYTES / 2);
        let second = delta(component, 2, INDEX_ARTIFACT_PACK_BYTES / 2);
        let packs = pack_component_deltas(vec![first, second]).unwrap();
        assert_eq!(packs.len(), 2);
        assert!(
            packs
                .iter()
                .all(|pack| pack.bytes.len() <= INDEX_ARTIFACT_PACK_BYTES)
        );
    }
}
