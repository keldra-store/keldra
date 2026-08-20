use crate::IndexError;
use sux::bits::BitVec;
use sux::rank_sel::{Rank9, Select9};
use sux::traits::{NumBits, Rank, Select};

use super::codec::{COMPONENT_HEADER_BYTES, Decoder, Encoder};
use super::model::{DocId, INDEX_COMPONENT_BYTES};

const POSTING_CODEC_VERSION: u16 = 1;
const MAX_PAYLOAD_BYTES: usize = INDEX_COMPONENT_BYTES - COMPONENT_HEADER_BYTES;
pub const POSTING_SKIP_INTERVAL: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PostingCodec {
    GapVarint = 1,
    DenseBitmap = 2,
}

/// Corpus-independent inputs for a conservative block score bound. Query-time
/// BM25 statistics are deliberately not persisted because they change when a
/// new immutable generation is published.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PostingImpact {
    pub maximum_frequency: u32,
    pub minimum_field_length: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostingBlock {
    codec: PostingCodec,
    doc_ids: Vec<DocId>,
    frequencies: Option<Vec<u32>>,
    impact: Option<PostingImpact>,
}

impl PostingBlock {
    pub fn new(doc_ids: Vec<DocId>, impact: Option<PostingImpact>) -> Result<Self, IndexError> {
        Self::with_frequencies(doc_ids, None, impact)
    }

    pub fn with_frequencies(
        doc_ids: Vec<DocId>,
        frequencies: Option<Vec<u32>>,
        impact: Option<PostingImpact>,
    ) -> Result<Self, IndexError> {
        validate_doc_ids(&doc_ids)?;
        if frequencies
            .as_ref()
            .is_some_and(|values| values.len() != doc_ids.len() || values.contains(&0))
        {
            return Err(IndexError::InvalidDefinition(
                "posting frequencies must be non-zero and align with DocIds".into(),
            ));
        }
        let observed_maximum_frequency = frequencies
            .as_ref()
            .and_then(|values| values.iter().copied().max())
            .unwrap_or(1);
        if impact.is_some_and(|value| {
            value.maximum_frequency == 0 || value.maximum_frequency < observed_maximum_frequency
        }) {
            return Err(IndexError::InvalidDefinition(
                "posting block impact understates its maximum frequency".into(),
            ));
        }
        let span = doc_ids
            .last()
            .unwrap()
            .get()
            .checked_sub(doc_ids[0].get())
            .and_then(|value| value.checked_add(1))
            .ok_or(IndexError::OffsetOverflow)?;
        let codec = if u64::from(span) <= (doc_ids.len() as u64).saturating_mul(8) {
            PostingCodec::DenseBitmap
        } else {
            PostingCodec::GapVarint
        };
        let block = Self {
            codec,
            doc_ids,
            frequencies,
            impact,
        };
        let length = block.encode_payload()?.len();
        if length > MAX_PAYLOAD_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: length + COMPONENT_HEADER_BYTES,
                limit: INDEX_COMPONENT_BYTES,
            });
        }
        Ok(block)
    }

    pub fn codec(&self) -> PostingCodec {
        self.codec
    }

    pub fn doc_ids(&self) -> &[DocId] {
        &self.doc_ids
    }

    pub fn frequencies(&self) -> Option<&[u32]> {
        self.frequencies.as_deref()
    }

    pub fn impact(&self) -> Option<PostingImpact> {
        self.impact
    }

    pub fn first_doc_id(&self) -> DocId {
        self.doc_ids[0]
    }

    pub fn last_doc_id(&self) -> DocId {
        *self
            .doc_ids
            .last()
            .expect("validated non-empty posting block")
    }

    pub fn encode_payload(&self) -> Result<Vec<u8>, IndexError> {
        validate_doc_ids(&self.doc_ids)?;
        validate_impact(self.impact, self.frequencies.as_deref())?;
        let mut body = Vec::new();
        match self.codec {
            PostingCodec::GapVarint => {
                let mut previous = 0u32;
                for (index, doc_id) in self.doc_ids.iter().enumerate() {
                    let gap = if index == 0 {
                        doc_id.get()
                    } else {
                        doc_id
                            .get()
                            .checked_sub(previous)
                            .ok_or(IndexError::OffsetOverflow)?
                    };
                    encode_varint(gap, &mut body);
                    previous = doc_id.get();
                }
            }
            PostingCodec::DenseBitmap => {
                let first = self.first_doc_id().get();
                let span = self
                    .last_doc_id()
                    .get()
                    .checked_sub(first)
                    .and_then(|value| value.checked_add(1))
                    .ok_or(IndexError::OffsetOverflow)?;
                body.resize(span.div_ceil(8) as usize, 0);
                for doc_id in &self.doc_ids {
                    let offset = doc_id.get() - first;
                    body[offset as usize / 8] |= 1 << (offset % 8);
                }
            }
        }
        let mut out = Encoder::default();
        out.u16(POSTING_CODEC_VERSION);
        out.u8(self.codec as u8);
        out.u32(self.first_doc_id().get());
        out.u32(self.last_doc_id().get());
        out.usize_u32(self.doc_ids.len())?;
        match self.impact {
            Some(impact) => {
                out.bool(true);
                out.u32(impact.maximum_frequency);
                out.u32(impact.minimum_field_length);
            }
            None => out.bool(false),
        }
        out.bool(self.frequencies.is_some());
        if let Some(frequencies) = &self.frequencies {
            out.usize_u32(frequencies.len())?;
            for frequency in frequencies {
                out.u32(*frequency);
            }
        }
        out.u16(POSTING_SKIP_INTERVAL as u16);
        out.usize_u32(self.doc_ids.len().div_ceil(POSTING_SKIP_INTERVAL))?;
        for (index, doc_id) in self
            .doc_ids
            .iter()
            .enumerate()
            .step_by(POSTING_SKIP_INTERVAL)
        {
            out.u32(u32::try_from(index).map_err(|_| IndexError::OffsetOverflow)?);
            out.u32(doc_id.get());
        }
        out.bytes(&body)?;
        Ok(out.finish())
    }

    pub fn decode_payload(bytes: &[u8]) -> Result<Self, IndexError> {
        let payload = decode_posting_payload(bytes)?;
        let doc_ids = match payload.codec {
            PostingCodec::GapVarint => decode_gaps(payload.body, payload.count)?,
            PostingCodec::DenseBitmap => {
                decode_bitmap(payload.body, payload.first, payload.last, payload.count)?
            }
        };
        validate_doc_ids(&doc_ids)?;
        validate_posting_descriptor(&payload, |ordinal| doc_ids.get(ordinal).copied())?;
        Ok(Self {
            codec: payload.codec,
            doc_ids,
            frequencies: payload.frequencies,
            impact: payload.impact,
        })
    }
}

type DensePostingBits = Select9<Rank9<BitVec<Box<[u64]>>>>;

#[derive(Clone, Debug)]
struct DensePostingDocs {
    first: DocId,
    last: DocId,
    count: usize,
    bits: DensePostingBits,
}

impl DensePostingDocs {
    fn decode(bytes: &[u8], first: u32, last: u32, count: usize) -> Result<Self, IndexError> {
        let span = validate_bitmap_layout(bytes, first, last)?;
        if count == 0 {
            return Err(IndexError::InvalidFormat("posting descriptor or skips"));
        }
        let mut words = Vec::with_capacity(span.div_ceil(u64::BITS as usize));
        for chunk in bytes.chunks(std::mem::size_of::<u64>()) {
            let mut encoded = [0u8; std::mem::size_of::<u64>()];
            encoded[..chunk.len()].copy_from_slice(chunk);
            words.push(u64::from_le_bytes(encoded));
        }
        // SAFETY: `validate_bitmap_layout` proves that the encoded byte length
        // covers `span`; converting every byte into explicit little-endian u64
        // words preserves Keldra's portable least-significant-bit-first codec.
        let vector = unsafe { BitVec::from_raw_parts(words.into_boxed_slice(), span) };
        let rank = Rank9::new(vector);
        if rank.num_ones() != count {
            return Err(IndexError::InvalidFormat("posting bitmap population"));
        }
        let bits = Select9::new(rank);
        if bits.select(0) != Some(0)
            || count
                .checked_sub(1)
                .and_then(|ordinal| bits.select(ordinal))
                != Some(span - 1)
        {
            return Err(IndexError::InvalidFormat("posting descriptor or skips"));
        }
        Ok(Self {
            first: DocId::new(first),
            last: DocId::new(last),
            count,
            bits,
        })
    }

    fn doc_id(&self, ordinal: usize) -> Option<DocId> {
        self.bits.select(ordinal).and_then(|offset| {
            u32::try_from(offset)
                .ok()
                .and_then(|offset| self.first.get().checked_add(offset))
                .map(DocId::new)
        })
    }

    fn lower_bound_from(&self, start: usize, target: DocId) -> Option<(usize, DocId)> {
        if start >= self.count {
            return None;
        }
        let target_ordinal = if target <= self.first {
            0
        } else {
            let relative = usize::try_from(target.get().checked_sub(self.first.get())?).ok()?;
            if relative >= self.bits.len() {
                return None;
            }
            self.bits.rank(relative)
        };
        let ordinal = start.max(target_ordinal);
        self.doc_id(ordinal).map(|doc_id| (ordinal, doc_id))
    }
}

#[derive(Clone, Debug)]
enum DecodedPostingDocs {
    Gap(Vec<DocId>),
    Dense(DensePostingDocs),
}

/// Query-only posting representation. Dense blocks retain their succinct
/// bitmap instead of expanding every hit into a `DocId`; merge/build callers
/// continue to use `PostingBlock` and its stable vector API.
#[derive(Clone, Debug)]
pub(super) struct DecodedPostingBlock {
    docs: DecodedPostingDocs,
    frequencies: Option<Vec<u32>>,
    impact: Option<PostingImpact>,
}

impl DecodedPostingBlock {
    pub(super) fn decode_payload(bytes: &[u8]) -> Result<Self, IndexError> {
        let payload = decode_posting_payload(bytes)?;
        let docs = match payload.codec {
            PostingCodec::GapVarint => {
                let doc_ids = decode_gaps(payload.body, payload.count)?;
                validate_doc_ids(&doc_ids)?;
                DecodedPostingDocs::Gap(doc_ids)
            }
            PostingCodec::DenseBitmap => DecodedPostingDocs::Dense(DensePostingDocs::decode(
                payload.body,
                payload.first,
                payload.last,
                payload.count,
            )?),
        };
        validate_posting_descriptor(&payload, |ordinal| match &docs {
            DecodedPostingDocs::Gap(doc_ids) => doc_ids.get(ordinal).copied(),
            DecodedPostingDocs::Dense(dense) => dense.doc_id(ordinal),
        })?;
        Ok(Self {
            docs,
            frequencies: payload.frequencies,
            impact: payload.impact,
        })
    }

    pub(super) fn doc_id(&self, ordinal: usize) -> Option<DocId> {
        match &self.docs {
            DecodedPostingDocs::Gap(doc_ids) => doc_ids.get(ordinal).copied(),
            DecodedPostingDocs::Dense(dense) => dense.doc_id(ordinal),
        }
    }

    pub(super) fn last_doc_id(&self) -> DocId {
        match &self.docs {
            DecodedPostingDocs::Gap(doc_ids) => *doc_ids.last().expect("validated posting block"),
            DecodedPostingDocs::Dense(dense) => dense.last,
        }
    }

    pub(super) fn lower_bound_from(&self, start: usize, target: DocId) -> Option<(usize, DocId)> {
        match &self.docs {
            DecodedPostingDocs::Gap(doc_ids) => {
                let remaining = doc_ids.get(start..)?;
                let ordinal = start + remaining.partition_point(|value| *value < target);
                doc_ids
                    .get(ordinal)
                    .copied()
                    .map(|doc_id| (ordinal, doc_id))
            }
            DecodedPostingDocs::Dense(dense) => dense.lower_bound_from(start, target),
        }
    }

    pub(super) fn frequency(&self, ordinal: usize) -> Option<u32> {
        match &self.frequencies {
            Some(frequencies) => frequencies.get(ordinal).copied(),
            None if ordinal < self.len() => Some(1),
            None => None,
        }
    }

    pub(super) fn impact(&self) -> Option<PostingImpact> {
        self.impact
    }

    fn len(&self) -> usize {
        match &self.docs {
            DecodedPostingDocs::Gap(doc_ids) => doc_ids.len(),
            DecodedPostingDocs::Dense(dense) => dense.count,
        }
    }
}

struct DecodedPostingPayload<'a> {
    codec: PostingCodec,
    first: u32,
    last: u32,
    count: usize,
    impact: Option<PostingImpact>,
    frequencies: Option<Vec<u32>>,
    skips: Vec<(u32, u32)>,
    body: &'a [u8],
}

fn decode_posting_payload(bytes: &[u8]) -> Result<DecodedPostingPayload<'_>, IndexError> {
    let mut input = Decoder::new(bytes)?;
    if input.u16()? != POSTING_CODEC_VERSION {
        return Err(IndexError::InvalidFormat("posting codec version"));
    }
    let codec = match input.u8()? {
        1 => PostingCodec::GapVarint,
        2 => PostingCodec::DenseBitmap,
        _ => return Err(IndexError::InvalidFormat("posting codec tag")),
    };
    let first = input.u32()?;
    let last = input.u32()?;
    let count = usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
    if count == 0 {
        return Err(IndexError::InvalidFormat("posting descriptor or skips"));
    }
    let impact = if input.bool()? {
        Some(PostingImpact {
            maximum_frequency: input.u32()?,
            minimum_field_length: input.u32()?,
        })
    } else {
        None
    };
    let frequencies = if input.bool()? {
        let frequency_count =
            usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
        if frequency_count != count {
            return Err(IndexError::InvalidFormat("posting frequency count"));
        }
        input.claim(frequency_count.saturating_mul(4))?;
        let mut values = Vec::with_capacity(frequency_count);
        for _ in 0..frequency_count {
            let value = input.u32()?;
            if value == 0 {
                return Err(IndexError::InvalidFormat("zero posting frequency"));
            }
            values.push(value);
        }
        Some(values)
    } else {
        None
    };
    if input.u16()? as usize != POSTING_SKIP_INTERVAL {
        return Err(IndexError::InvalidFormat("posting skip interval"));
    }
    let skip_count = usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
    if skip_count != count.div_ceil(POSTING_SKIP_INTERVAL) {
        return Err(IndexError::InvalidFormat("posting skip count"));
    }
    input.claim(skip_count.saturating_mul(8))?;
    input.claim(count.saturating_mul(std::mem::size_of::<DocId>()))?;
    let mut skips = Vec::with_capacity(skip_count);
    for _ in 0..skip_count {
        skips.push((input.u32()?, input.u32()?));
    }
    let body = input.bytes()?;
    input.finish()?;
    let observed_maximum_frequency = frequencies
        .as_ref()
        .and_then(|values| values.iter().copied().max())
        .unwrap_or(1);
    if impact.is_some_and(|value| {
        value.maximum_frequency == 0 || value.maximum_frequency < observed_maximum_frequency
    }) {
        return Err(IndexError::InvalidFormat("posting impact bound"));
    }
    Ok(DecodedPostingPayload {
        codec,
        first,
        last,
        count,
        impact,
        frequencies,
        skips,
        body,
    })
}

fn validate_posting_descriptor(
    payload: &DecodedPostingPayload<'_>,
    doc_id: impl Fn(usize) -> Option<DocId>,
) -> Result<(), IndexError> {
    if payload.count == 0
        || doc_id(0).map(DocId::get) != Some(payload.first)
        || doc_id(payload.count - 1).map(DocId::get) != Some(payload.last)
        || payload
            .skips
            .iter()
            .enumerate()
            .any(|(ordinal, (index, doc))| {
                let expected = ordinal * POSTING_SKIP_INTERVAL;
                *index as usize != expected || doc_id(expected).map(DocId::get) != Some(*doc)
            })
    {
        return Err(IndexError::InvalidFormat("posting descriptor or skips"));
    }
    Ok(())
}

fn validate_doc_ids(doc_ids: &[DocId]) -> Result<(), IndexError> {
    if doc_ids.is_empty()
        || doc_ids
            .windows(2)
            .any(|pair| pair[0].get() >= pair[1].get())
    {
        return Err(IndexError::InvalidDefinition(
            "posting DocIds must be unique and ascending".into(),
        ));
    }
    Ok(())
}

fn validate_impact(
    impact: Option<PostingImpact>,
    frequencies: Option<&[u32]>,
) -> Result<(), IndexError> {
    let observed_maximum_frequency = frequencies
        .and_then(|values| values.iter().copied().max())
        .unwrap_or(1);
    if impact.is_some_and(|value| {
        value.maximum_frequency == 0 || value.maximum_frequency < observed_maximum_frequency
    }) {
        return Err(IndexError::InvalidDefinition(
            "posting block impact understates its maximum frequency".into(),
        ));
    }
    Ok(())
}

fn encode_varint(mut value: u32, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn decode_gaps(bytes: &[u8], count: usize) -> Result<Vec<DocId>, IndexError> {
    let mut output = Vec::with_capacity(count);
    let (mut cursor, mut previous) = (0usize, 0u32);
    for index in 0..count {
        let gap = decode_varint(bytes, &mut cursor)?;
        if index != 0 && gap == 0 {
            return Err(IndexError::InvalidFormat("zero posting gap"));
        }
        let value = if index == 0 {
            gap
        } else {
            previous
                .checked_add(gap)
                .ok_or(IndexError::OffsetOverflow)?
        };
        output.push(DocId::new(value));
        previous = value;
    }
    if cursor != bytes.len() {
        return Err(IndexError::InvalidFormat("trailing posting gaps"));
    }
    Ok(output)
}

fn decode_varint(bytes: &[u8], cursor: &mut usize) -> Result<u32, IndexError> {
    let mut value = 0u32;
    for shift in (0..35).step_by(7) {
        let byte = *bytes
            .get(*cursor)
            .ok_or(IndexError::InvalidFormat("truncated posting gap"))?;
        *cursor += 1;
        if shift == 28 && byte & 0xf0 != 0 {
            return Err(IndexError::InvalidFormat("posting gap overflow"));
        }
        value |= u32::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(IndexError::InvalidFormat("posting gap overflow"))
}

fn decode_bitmap(
    bytes: &[u8],
    first: u32,
    last: u32,
    count: usize,
) -> Result<Vec<DocId>, IndexError> {
    let span = validate_bitmap_layout(bytes, first, last)?;
    let mut output = Vec::with_capacity(count);
    for offset in 0..span {
        if bytes[offset / 8] & (1 << (offset % 8)) != 0 {
            output.push(DocId::new(
                first
                    .checked_add(u32::try_from(offset).map_err(|_| IndexError::OffsetOverflow)?)
                    .ok_or(IndexError::OffsetOverflow)?,
            ));
        }
    }
    if output.len() != count {
        return Err(IndexError::InvalidFormat("posting bitmap population"));
    }
    Ok(output)
}

fn validate_bitmap_layout(bytes: &[u8], first: u32, last: u32) -> Result<usize, IndexError> {
    let span = last
        .checked_sub(first)
        .and_then(|value| value.checked_add(1))
        .and_then(|value| usize::try_from(value).ok())
        .ok_or(IndexError::InvalidFormat("posting bitmap range"))?;
    if bytes.len() != span.div_ceil(8)
        || span % 8 != 0 && bytes.last().is_some_and(|byte| *byte >> (span % 8) != 0)
    {
        return Err(IndexError::InvalidFormat("posting bitmap length"));
    }
    Ok(span)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostingList {
    blocks: Vec<PostingBlock>,
    count: usize,
}

impl PostingList {
    pub fn new(blocks: Vec<PostingBlock>) -> Result<Self, IndexError> {
        if blocks.is_empty()
            || blocks
                .windows(2)
                .any(|pair| pair[0].last_doc_id().get() >= pair[1].first_doc_id().get())
        {
            return Err(IndexError::InvalidDefinition(
                "posting blocks must be non-empty and non-overlapping".into(),
            ));
        }
        let count = blocks.iter().try_fold(0usize, |sum, block| {
            sum.checked_add(block.doc_ids.len())
                .ok_or(IndexError::OffsetOverflow)
        })?;
        Ok(Self { blocks, count })
    }

    pub fn cursor(&self) -> PostingCursor<'_> {
        PostingCursor {
            list: self,
            block: 0,
            position: None,
            consumed: 0,
        }
    }
}

pub struct PostingCursor<'a> {
    list: &'a PostingList,
    block: usize,
    position: Option<usize>,
    consumed: usize,
}

impl PostingCursor<'_> {
    pub fn doc_id(&self) -> Option<DocId> {
        self.position.and_then(|position| {
            self.list
                .blocks
                .get(self.block)?
                .doc_ids
                .get(position)
                .copied()
        })
    }

    // This is the RFC's advanceable posting-cursor operation, not a general
    // collection iterator: callers also use `advance(target)` and `cost()`.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<DocId> {
        if self.block >= self.list.blocks.len() {
            return None;
        }
        let next = self.position.map_or(0, |position| position + 1);
        if next < self.list.blocks[self.block].doc_ids.len() {
            self.position = Some(next);
            self.consumed += 1;
            return self.doc_id();
        }
        self.block += usize::from(self.position.is_some());
        self.position = None;
        if self.block >= self.list.blocks.len() {
            return None;
        }
        self.next()
    }

    pub fn advance(&mut self, target: DocId) -> Option<DocId> {
        if self.doc_id().is_some_and(|current| current >= target) {
            return self.doc_id();
        }
        while self.block < self.list.blocks.len()
            && self.list.blocks[self.block].last_doc_id() < target
        {
            self.consumed += self.list.blocks[self.block]
                .doc_ids
                .len()
                .saturating_sub(self.position.map_or(0, |value| value + 1));
            self.block += 1;
            self.position = None;
        }
        let block = self.list.blocks.get(self.block)?;
        let start = self.position.map_or(0, |value| value + 1);
        let offset = block.doc_ids[start..].partition_point(|value| *value < target);
        self.position = Some(start + offset);
        self.consumed += offset + 1;
        self.doc_id()
    }

    pub fn cost(&self) -> usize {
        self.list.count.saturating_sub(self.consumed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_and_dense_codecs_round_trip() {
        for values in [
            vec![DocId::new(1), DocId::new(1000), DocId::new(50_000)],
            (100..300).map(DocId::new).collect::<Vec<_>>(),
        ] {
            let impact = PostingImpact {
                maximum_frequency: 1,
                minimum_field_length: 3,
            };
            let block = PostingBlock::new(values.clone(), Some(impact)).unwrap();
            let decoded = PostingBlock::decode_payload(&block.encode_payload().unwrap()).unwrap();
            assert_eq!(decoded.doc_ids(), values);
            assert_eq!(decoded.codec(), block.codec());
            let query =
                DecodedPostingBlock::decode_payload(&block.encode_payload().unwrap()).unwrap();
            assert_eq!(
                (0..values.len())
                    .filter_map(|ordinal| query.doc_id(ordinal))
                    .collect::<Vec<_>>(),
                values
            );
            assert_eq!(query.frequency(0), Some(1));
            assert_eq!(query.frequency(values.len()), None);
            assert_eq!(query.impact(), Some(impact));
        }
    }

    #[test]
    fn query_decoder_keeps_gap_vectors_and_dense_rank_select_bits() {
        let sparse_values = vec![DocId::new(1), DocId::new(1000), DocId::new(50_000)];
        let sparse = PostingBlock::with_frequencies(
            sparse_values.clone(),
            Some(vec![2, 3, 5]),
            Some(PostingImpact {
                maximum_frequency: 5,
                minimum_field_length: 2,
            }),
        )
        .unwrap();
        let decoded =
            DecodedPostingBlock::decode_payload(&sparse.encode_payload().unwrap()).unwrap();
        assert!(matches!(&decoded.docs, DecodedPostingDocs::Gap(_)));
        assert_eq!(decoded.doc_id(1), Some(DocId::new(1000)));
        assert_eq!(
            decoded.lower_bound_from(0, DocId::new(2)),
            Some((1, DocId::new(1000)))
        );
        assert_eq!(decoded.frequency(2), Some(5));

        let mut dense_values = (10..=26).map(DocId::new).collect::<Vec<_>>();
        dense_values.extend([DocId::new(74), DocId::new(75), DocId::new(139)]);
        let frequencies = (1..=dense_values.len() as u32).collect::<Vec<_>>();
        let dense =
            PostingBlock::with_frequencies(dense_values.clone(), Some(frequencies.clone()), None)
                .unwrap();
        assert_eq!(dense.codec(), PostingCodec::DenseBitmap);
        let decoded =
            DecodedPostingBlock::decode_payload(&dense.encode_payload().unwrap()).unwrap();
        assert!(matches!(&decoded.docs, DecodedPostingDocs::Dense(_)));
        for (ordinal, expected) in dense_values.iter().copied().enumerate() {
            assert_eq!(decoded.doc_id(ordinal), Some(expected));
            assert_eq!(decoded.frequency(ordinal), Some(frequencies[ordinal]));
        }
        assert_eq!(decoded.doc_id(dense_values.len()), None);
        assert_eq!(
            decoded.lower_bound_from(0, DocId::new(73)),
            Some((17, DocId::new(74)))
        );
        assert_eq!(
            decoded.lower_bound_from(0, DocId::new(74)),
            Some((17, DocId::new(74)))
        );
        assert_eq!(
            decoded.lower_bound_from(0, DocId::new(76)),
            Some((19, DocId::new(139)))
        );
        assert_eq!(
            decoded.lower_bound_from(18, DocId::new(10)),
            Some((18, DocId::new(75)))
        );
        assert_eq!(decoded.lower_bound_from(0, DocId::new(140)), None);
    }

    #[test]
    fn dense_rank_select_preserves_portable_little_endian_bit_order() {
        let dense = DensePostingDocs::decode(&[0x81], 10, 17, 2).unwrap();
        assert_eq!(dense.doc_id(0), Some(DocId::new(10)));
        assert_eq!(dense.doc_id(1), Some(DocId::new(17)));
    }

    #[test]
    fn dense_rank_select_rejects_population_boundary_and_padding_corruption() {
        assert!(matches!(
            DensePostingDocs::decode(&[0x01, 0x01], 5, 13, 3),
            Err(IndexError::InvalidFormat("posting bitmap population"))
        ));
        assert!(matches!(
            DensePostingDocs::decode(&[0x02, 0x01], 5, 13, 2),
            Err(IndexError::InvalidFormat("posting descriptor or skips"))
        ));
        assert!(matches!(
            DensePostingDocs::decode(&[0x03, 0x00], 5, 13, 2),
            Err(IndexError::InvalidFormat("posting descriptor or skips"))
        ));
        assert!(matches!(
            DensePostingDocs::decode(&[0x01, 0x03], 5, 13, 2),
            Err(IndexError::InvalidFormat("posting bitmap length"))
        ));
    }

    #[test]
    fn cursor_next_advance_and_cost_cross_blocks() {
        let list = PostingList::new(vec![
            PostingBlock::new(vec![DocId::new(1), DocId::new(7)], None).unwrap(),
            PostingBlock::new(vec![DocId::new(100), DocId::new(105)], None).unwrap(),
        ])
        .unwrap();
        let mut cursor = list.cursor();
        assert_eq!(cursor.cost(), 4);
        assert_eq!(cursor.next(), Some(DocId::new(1)));
        assert_eq!(cursor.advance(DocId::new(80)), Some(DocId::new(100)));
        assert_eq!(cursor.cost(), 1);
        assert_eq!(cursor.next(), Some(DocId::new(105)));
        assert_eq!(cursor.next(), None);
    }
}
