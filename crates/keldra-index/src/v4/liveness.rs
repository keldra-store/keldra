use std::sync::Arc;

use crate::IndexError;

use super::codec::{Decoder, Encoder};
use super::model::DocId;

const LIVE_MASK_CODEC_VERSION: u16 = 1;
pub const LIVE_MASK_BLOCK_DOCS: u32 = 32 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveMaskBlock {
    pub first_doc_id: DocId,
    pub document_count: u32,
    bits: Arc<[u8]>,
}

impl LiveMaskBlock {
    fn new(first_doc_id: DocId, document_count: u32, bits: Arc<[u8]>) -> Result<Self, IndexError> {
        if document_count == 0 || document_count > LIVE_MASK_BLOCK_DOCS {
            return Err(IndexError::InvalidFormat("live-mask document count"));
        }
        let bytes =
            usize::try_from(document_count.div_ceil(8)).map_err(|_| IndexError::OffsetOverflow)?;
        if bits.len() != bytes {
            return Err(IndexError::InvalidFormat("live-mask byte count"));
        }
        let remainder = document_count % 8;
        if remainder != 0 && bits.last().is_some_and(|last| *last >> remainder != 0) {
            return Err(IndexError::InvalidFormat("live-mask padding bits"));
        }
        first_doc_id
            .get()
            .checked_add(document_count - 1)
            .ok_or(IndexError::OffsetOverflow)?;
        Ok(Self {
            first_doc_id,
            document_count,
            bits,
        })
    }

    pub(crate) fn all_live(first_doc_id: DocId, document_count: u32) -> Result<Self, IndexError> {
        if document_count == 0 || document_count > LIVE_MASK_BLOCK_DOCS {
            return Err(IndexError::InvalidDefinition(
                "all-live block document count is invalid".into(),
            ));
        }
        let mut bits = vec![0xff; document_count.div_ceil(8) as usize];
        if !document_count.is_multiple_of(8) {
            *bits.last_mut().expect("nonempty live block") &= (1 << (document_count % 8)) - 1;
        }
        Self::new(first_doc_id, document_count, bits.into())
    }

    pub fn is_live(&self, doc_id: DocId) -> Option<bool> {
        let offset = doc_id.get().checked_sub(self.first_doc_id.get())?;
        if offset >= self.document_count {
            return None;
        }
        Some(self.bits[offset as usize / 8] & (1 << (offset % 8)) != 0)
    }

    /// Return one replacement immutable block with `doc_id` cleared. A DocId
    /// outside this block is rejected rather than silently changing another
    /// range.
    pub fn clear(&self, doc_id: DocId) -> Result<Self, IndexError> {
        let offset = doc_id
            .get()
            .checked_sub(self.first_doc_id.get())
            .filter(|offset| *offset < self.document_count)
            .ok_or_else(|| {
                IndexError::InvalidDefinition(
                    "live-mask DocId is outside the selected block".into(),
                )
            })?;
        if self.bits[offset as usize / 8] & (1 << (offset % 8)) == 0 {
            return Ok(self.clone());
        }
        let mut bits = self.bits.to_vec();
        bits[offset as usize / 8] &= !(1 << (offset % 8));
        Self::new(self.first_doc_id, self.document_count, bits.into())
    }

    /// Return one replacement immutable block with each ordered DocId range
    /// cleared. The bitmap is copied at most once, irrespective of how many
    /// documents the ranges cover.
    pub fn clear_ranges<I>(&self, ranges: I) -> Result<(Self, u32), IndexError>
    where
        I: IntoIterator<Item = (DocId, u32)>,
    {
        let block_first = self.first_doc_id.get();
        let block_end = block_first
            .checked_add(self.document_count)
            .ok_or(IndexError::OffsetOverflow)?;
        let mut previous_end = block_first;
        let mut bits = None::<Vec<u8>>;
        let mut cleared = 0u32;
        for (first_doc_id, count) in ranges {
            let first = first_doc_id.get();
            let end = first.checked_add(count).ok_or(IndexError::OffsetOverflow)?;
            if count == 0 || first < block_first || end > block_end || first < previous_end {
                return Err(IndexError::InvalidDefinition(
                    "live-mask ranges must be non-empty, ordered, disjoint, and inside the selected block"
                        .into(),
                ));
            }
            previous_end = end;
            let bytes = bits.get_or_insert_with(|| self.bits.to_vec());
            let mut offset = first - block_first;
            let range_end = end - block_first;
            while offset < range_end && !offset.is_multiple_of(8) {
                clear_live_bit(bytes, offset, &mut cleared)?;
                offset += 1;
            }
            while offset.checked_add(8).is_some_and(|next| next <= range_end) {
                let byte = &mut bytes[offset as usize / 8];
                cleared = cleared
                    .checked_add(byte.count_ones())
                    .ok_or(IndexError::OffsetOverflow)?;
                *byte = 0;
                offset += 8;
            }
            while offset < range_end {
                clear_live_bit(bytes, offset, &mut cleared)?;
                offset += 1;
            }
        }
        let Some(bits) = bits else {
            return Ok((self.clone(), 0));
        };
        Ok((
            Self::new(self.first_doc_id, self.document_count, bits.into())?,
            cleared,
        ))
    }

    pub fn encode_payload(&self) -> Result<Vec<u8>, IndexError> {
        let mut out = Encoder::default();
        out.u16(LIVE_MASK_CODEC_VERSION);
        out.u32(self.first_doc_id.get());
        out.u32(self.document_count);
        out.bytes(&self.bits)?;
        Ok(out.finish())
    }

    pub fn decode_payload(bytes: &[u8]) -> Result<Self, IndexError> {
        let mut input = Decoder::new(bytes)?;
        if input.u16()? != LIVE_MASK_CODEC_VERSION {
            return Err(IndexError::InvalidFormat("live-mask codec version"));
        }
        let first = DocId::new(input.u32()?);
        let count = input.u32()?;
        let bits: Arc<[u8]> = input.owned_bytes()?.into();
        input.finish()?;
        Self::new(first, count, bits)
    }
}

fn clear_live_bit(bits: &mut [u8], offset: u32, cleared: &mut u32) -> Result<(), IndexError> {
    let byte = &mut bits[offset as usize / 8];
    let mask = 1 << (offset % 8);
    if *byte & mask != 0 {
        *byte &= !mask;
        *cleared = cleared.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveMask {
    document_count: u32,
    blocks: Vec<LiveMaskBlock>,
}

impl LiveMask {
    pub fn all_live(document_count: u32) -> Result<Self, IndexError> {
        let mut blocks = Vec::new();
        let mut first = 0u32;
        while first < document_count {
            let count = LIVE_MASK_BLOCK_DOCS.min(document_count - first);
            let mut bits = vec![0xff; count.div_ceil(8) as usize];
            if !count.is_multiple_of(8) {
                *bits.last_mut().expect("non-empty live block") &= (1 << (count % 8)) - 1;
            }
            blocks.push(LiveMaskBlock::new(DocId::new(first), count, bits.into())?);
            first = first.checked_add(count).ok_or(IndexError::OffsetOverflow)?;
        }
        Ok(Self {
            document_count,
            blocks,
        })
    }

    /// Reconstruct a persisted immutable mask. Blocks must cover exactly
    /// `0..document_count` once, in dense DocId order; gaps, overlap, trailing
    /// blocks, and an incomplete tail are rejected.
    pub fn from_blocks(
        document_count: u32,
        blocks: Vec<LiveMaskBlock>,
    ) -> Result<Self, IndexError> {
        let mut expected_first = 0u32;
        for block in &blocks {
            LiveMaskBlock::new(block.first_doc_id, block.document_count, block.bits.clone())?;
            if block.first_doc_id.get() != expected_first {
                return Err(IndexError::InvalidFormat(
                    "live-mask blocks are not dense and ordered",
                ));
            }
            expected_first = expected_first
                .checked_add(block.document_count)
                .ok_or(IndexError::OffsetOverflow)?;
            if expected_first > document_count {
                return Err(IndexError::InvalidFormat(
                    "live-mask blocks exceed the document count",
                ));
            }
        }
        if expected_first != document_count || (document_count == 0) != blocks.is_empty() {
            return Err(IndexError::InvalidFormat(
                "live-mask blocks do not cover the document count",
            ));
        }
        Ok(Self {
            document_count,
            blocks,
        })
    }

    pub fn document_count(&self) -> u32 {
        self.document_count
    }

    pub fn blocks(&self) -> &[LiveMaskBlock] {
        &self.blocks
    }

    pub fn is_live(&self, doc_id: DocId) -> bool {
        if doc_id.get() >= self.document_count {
            return false;
        }
        let block = doc_id.get() / LIVE_MASK_BLOCK_DOCS;
        self.blocks[block as usize].is_live(doc_id).unwrap_or(false)
    }

    /// Return a new immutable view while sharing every unchanged block.
    pub fn clear(&self, doc_id: DocId) -> Result<Self, IndexError> {
        if doc_id.get() >= self.document_count {
            return Err(IndexError::InvalidDefinition(
                "live-mask DocId is outside the segment".into(),
            ));
        }
        let block_index = (doc_id.get() / LIVE_MASK_BLOCK_DOCS) as usize;
        let block = &self.blocks[block_index];
        let mut blocks = self.blocks.clone();
        blocks[block_index] = block.clear(doc_id)?;
        Ok(Self {
            document_count: self.document_count,
            blocks,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clearing_one_doc_reuses_unchanged_blocks() {
        let mask = LiveMask::all_live(LIVE_MASK_BLOCK_DOCS + 2).unwrap();
        let updated = mask.clear(DocId::new(1)).unwrap();
        assert!(!updated.is_live(DocId::new(1)));
        assert!(updated.is_live(DocId::new(LIVE_MASK_BLOCK_DOCS)));
        assert!(Arc::ptr_eq(&mask.blocks[1].bits, &updated.blocks[1].bits));
        assert!(!Arc::ptr_eq(&mask.blocks[0].bits, &updated.blocks[0].bits));
        let decoded =
            LiveMaskBlock::decode_payload(&updated.blocks[0].encode_payload().unwrap()).unwrap();
        assert_eq!(decoded, updated.blocks[0]);
    }

    #[test]
    fn persisted_blocks_require_exact_dense_coverage() {
        let mask = LiveMask::all_live(LIVE_MASK_BLOCK_DOCS + 2).unwrap();
        let rebuilt = LiveMask::from_blocks(mask.document_count(), mask.blocks().to_vec()).unwrap();
        assert_eq!(rebuilt, mask);
        assert!(LiveMask::from_blocks(mask.document_count() + 1, mask.blocks().to_vec()).is_err());
        assert!(
            LiveMask::from_blocks(mask.document_count(), vec![mask.blocks()[1].clone()]).is_err()
        );
    }

    #[test]
    fn nonzero_padding_is_corruption() {
        assert!(LiveMaskBlock::new(DocId::MIN, 1, Arc::from([0x80])).is_err());
    }

    #[test]
    fn clearing_ranges_copies_once_and_counts_only_live_bits() {
        let block = LiveMaskBlock::all_live(DocId::new(10), 12).unwrap();
        let (cleared, count) = block
            .clear_ranges([(DocId::new(11), 3), (DocId::new(18), 2)])
            .unwrap();
        assert_eq!(count, 5);
        assert_eq!(cleared.is_live(DocId::new(10)), Some(true));
        assert_eq!(cleared.is_live(DocId::new(11)), Some(false));
        assert_eq!(cleared.is_live(DocId::new(13)), Some(false));
        assert_eq!(cleared.is_live(DocId::new(18)), Some(false));
        assert_eq!(cleared.is_live(DocId::new(20)), Some(true));

        let (unchanged, count) = cleared
            .clear_ranges([(DocId::new(11), 3), (DocId::new(18), 2)])
            .unwrap();
        assert_eq!(count, 0);
        assert_eq!(unchanged, cleared);
    }

    #[test]
    fn clearing_ranges_rejects_overlap_and_out_of_block_ranges() {
        let block = LiveMaskBlock::all_live(DocId::new(10), 12).unwrap();
        assert!(
            block
                .clear_ranges([(DocId::new(11), 3), (DocId::new(13), 2)])
                .is_err()
        );
        assert!(block.clear_ranges([(DocId::new(21), 2)]).is_err());
        assert!(block.clear_ranges([(DocId::new(10), 0)]).is_err());
    }
}
