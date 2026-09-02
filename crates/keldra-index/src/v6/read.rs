//! Verified reads of v6 component runs embedded in integrated delta packs.

use super::{
    ComponentDeltaRecord, ComponentIdentity, ComponentSegmentDescriptor,
    decode_component_delta_segment,
};
use crate::IndexError;

/// Decode every canonical record selected by one component-run descriptor.
///
/// The descriptor selects an exact segment range within an integrated delta
/// pack. The complete pack hash, selected range, segment hash, component,
/// record count, and segment footer are all verified before records are
/// returned. The result is bounded by the descriptor's already bounded
/// encoded segment and is suitable only for a disposable read view.
pub fn decode_component_records_in_pack(
    component: ComponentIdentity,
    descriptor: &ComponentSegmentDescriptor,
    pack: &[u8],
) -> Result<Vec<ComponentDeltaRecord>, IndexError> {
    if *blake3::hash(pack).as_bytes() != descriptor.pack_hash {
        return Err(IndexError::Integrity);
    }
    let start = usize::try_from(descriptor.pack_offset).map_err(|_| IndexError::OffsetOverflow)?;
    let length =
        usize::try_from(descriptor.encoded_bytes).map_err(|_| IndexError::OffsetOverflow)?;
    let end = start
        .checked_add(length)
        .ok_or(IndexError::OffsetOverflow)?;
    let segment = pack.get(start..end).ok_or(IndexError::Integrity)?;
    if *blake3::hash(segment).as_bytes() != descriptor.segment_hash {
        return Err(IndexError::Integrity);
    }
    let decoded = decode_component_delta_segment(segment)?;
    if decoded.component != component || decoded.records.len() as u64 != descriptor.records {
        return Err(IndexError::Integrity);
    }
    Ok(decoded.records)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::v6::buffer::seal_component;
    use crate::v6::{
        ComponentIdentity, IndexingMemoryCredits, IndexingMemoryLimits, IndexingMemoryStage,
        ProjectionPackCredits, RecipeIdentity, StableDocumentKey, append_component_delta,
        decode_component_stream, pack_component_deltas,
    };

    fn pack_credits(bytes: usize) -> ProjectionPackCredits {
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

    #[test]
    fn verified_run_read_rejects_wrong_pack_and_preserves_key_order() {
        let component = ComponentIdentity::Field(RecipeIdentity::new([3; 32]).unwrap());
        let keys = [
            StableDocumentKey::from_bytes([1; 32]).unwrap(),
            StableDocumentKey::from_bytes([2; 32]).unwrap(),
        ];
        let delta = seal_component(
            component,
            BTreeMap::from([(keys[0], Some(b"one".to_vec())), (keys[1], None)]),
        )
        .unwrap();
        let bytes = delta.bytes.len();
        let pack = pack_component_deltas(vec![delta], pack_credits(bytes))
            .unwrap()
            .packs
            .remove(0);
        let run = append_component_delta(None, &pack.deltas[0], 0, 1, 1).unwrap();
        let descriptor = decode_component_stream(&run).unwrap().remove(0);
        let records =
            decode_component_records_in_pack(component, &descriptor, &pack.bytes).unwrap();
        assert_eq!(
            records
                .iter()
                .map(|record| record.stable_key)
                .collect::<Vec<_>>(),
            keys
        );
        let mut corrupt = pack.bytes.clone();
        corrupt[0] ^= 1;
        assert!(decode_component_records_in_pack(component, &descriptor, &corrupt).is_err());
    }
}
