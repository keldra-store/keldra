//! Stable document identities with compact ordinals scoped to one path range.

use std::collections::VecDeque;

use crate::codec::{Decoder, Encoder, encode_component};
use crate::run::{ComponentTree, LeafCursor, RunView, find_leaf};
use crate::succinct::{decode_elias_fano_with_budget, encode_elias_fano};
use crate::{
    ComponentCodec, IndexBlockSink, IndexDirectoryRead, IndexError, IndexKind,
    MAX_INDEX_DECODED_BLOCK_BYTES,
};

use super::{ROWS_TAG, TypedPayload, ordinal_key};

pub(super) const ELIAS_FANO_DECODED_FIXED_BYTES: usize = 256;
pub(super) const ELIAS_FANO_DECODED_BYTES_PER_VALUE: usize = 3 * std::mem::size_of::<u64>();

// A compaction lane owns one deterministic path range. The range prefix makes
// its compact identities disjoint without counting every live document in all
// preceding ranges. Thirty-two bits on each side allow 4.29 billion records in
// one bounded source range and 4.29 billion ranges in one immutable run.
pub(super) const LOCAL_ORDINAL_BITS: u32 = 32;
const MAX_LOCAL_ORDINAL: u64 = (1u64 << LOCAL_ORDINAL_BITS) - 1;
const MAX_RANGE_ID: usize = u32::MAX as usize;

pub(super) fn range_ordinal_base(range_id: usize) -> Result<u64, IndexError> {
    if range_id > MAX_RANGE_ID {
        return Err(IndexError::ResourceLimit {
            needed: range_id,
            limit: MAX_RANGE_ID,
        });
    }
    Ok((range_id as u64) << LOCAL_ORDINAL_BITS)
}

pub(super) fn range_local_ordinal(base: u64, local: u64) -> Result<u64, IndexError> {
    if local > MAX_LOCAL_ORDINAL || base & MAX_LOCAL_ORDINAL != 0 {
        return Err(IndexError::OffsetOverflow);
    }
    base.checked_add(local).ok_or(IndexError::OffsetOverflow)
}

pub(super) fn ordinal_range_id(ordinal: u64) -> u32 {
    (ordinal >> LOCAL_ORDINAL_BITS) as u32
}

pub(super) fn ordinal_local(ordinal: u64) -> u64 {
    ordinal & MAX_LOCAL_ORDINAL
}

pub(super) fn compose_ordinal(range_id: u32, local: u64) -> Result<u64, IndexError> {
    let base = range_ordinal_base(range_id as usize)?;
    range_local_ordinal(base, local)
}

#[derive(Clone, Debug)]
pub(crate) struct TypedRow {
    pub(crate) ordinal: u64,
    pub(crate) payload: TypedPayload,
}

pub(crate) struct TypedComponentWriter {
    kind: IndexKind,
    level: u8,
    target_bytes: usize,
    estimated_bytes: usize,
    pub(super) decoded_resident_bytes: usize,
    pub(super) rows: Vec<TypedRow>,
    tree: crate::run::RoutingTreeBuilder,
}

impl TypedComponentWriter {
    pub(crate) fn new(kind: IndexKind, level: u8, target_bytes: usize) -> Self {
        Self {
            kind,
            level,
            target_bytes: target_bytes.max(256),
            estimated_bytes: 0,
            decoded_resident_bytes: 0,
            rows: Vec::new(),
            tree: crate::run::RoutingTreeBuilder::new(kind, ROWS_TAG),
        }
    }

    pub(crate) async fn push<S: IndexBlockSink>(
        &mut self,
        row: TypedRow,
        sink: &mut S,
    ) -> Result<(), IndexError> {
        let row_bytes = row.payload.encoded_bytes().saturating_add(16);
        let row_decoded_resident_bytes =
            std::mem::size_of::<TypedRow>().saturating_add(row.payload.decoded_resident_bytes());
        let single_row_decoded_bytes =
            row_decoded_resident_bytes.saturating_add(self.ordinal_decode_resident_bytes(1));
        if single_row_decoded_bytes > MAX_INDEX_DECODED_BLOCK_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: single_row_decoded_bytes,
                limit: MAX_INDEX_DECODED_BLOCK_BYTES,
            });
        }
        let next_decoded_resident_bytes = self
            .decoded_resident_bytes
            .saturating_add(row_decoded_resident_bytes)
            .saturating_add(self.ordinal_decode_resident_bytes(self.rows.len().saturating_add(1)));
        if !self.rows.is_empty()
            && (self.estimated_bytes.saturating_add(row_bytes) > self.target_bytes
                || next_decoded_resident_bytes > MAX_INDEX_DECODED_BLOCK_BYTES)
        {
            self.flush(sink).await?;
        }
        if self
            .rows
            .last()
            .is_some_and(|previous| previous.ordinal >= row.ordinal)
        {
            return Err(IndexError::UnsortedRecords);
        }
        self.estimated_bytes = self.estimated_bytes.saturating_add(row_bytes);
        self.decoded_resident_bytes = self
            .decoded_resident_bytes
            .saturating_add(row_decoded_resident_bytes);
        self.rows.push(row);
        Ok(())
    }

    pub(super) fn ordinal_decode_resident_bytes(&self, row_count: usize) -> usize {
        if self.level == 0 || row_count == 0 {
            0
        } else {
            ELIAS_FANO_DECODED_FIXED_BYTES
                .saturating_add(row_count.saturating_mul(ELIAS_FANO_DECODED_BYTES_PER_VALUE))
        }
    }

    async fn flush<S: IndexBlockSink>(&mut self, sink: &mut S) -> Result<(), IndexError> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.rows);
        self.estimated_bytes = 0;
        self.decoded_resident_bytes = 0;
        let codec = if self.level == 0 {
            ComponentCodec::FixedRows
        } else {
            ComponentCodec::PrefixEliasFano
        };
        let body = encode_typed_rows(&rows, codec)?;
        let bytes = encode_component(self.kind, ROWS_TAG, codec, body)?;
        self.tree
            .emit_leaf(
                crate::GeneratedBlock::new(
                    self.kind,
                    ROWS_TAG,
                    codec,
                    0,
                    ordinal_key(rows.first().unwrap().ordinal),
                    ordinal_key(rows.last().unwrap().ordinal),
                    rows.len() as u64,
                    bytes,
                )?,
                sink,
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn finish<S: IndexBlockSink>(
        mut self,
        sink: &mut S,
    ) -> Result<ComponentTree, IndexError> {
        self.flush(sink).await?;
        self.tree.finish(sink).await
    }
}

fn encode_typed_rows(rows: &[TypedRow], codec: ComponentCodec) -> Result<Vec<u8>, IndexError> {
    let mut output = Encoder::default();
    output.u32(rows.len())?;
    if codec == ComponentCodec::PrefixEliasFano {
        output.bytes(&encode_elias_fano(
            &rows.iter().map(|row| row.ordinal).collect::<Vec<_>>(),
        )?)?;
    }
    for row in rows {
        if codec == ComponentCodec::FixedRows {
            output.u64(row.ordinal);
        }
        row.payload.encode(&mut output)?;
    }
    Ok(output.finish())
}

pub(super) fn decode_typed_rows(
    bytes: &[u8],
    codec: ComponentCodec,
) -> Result<Vec<TypedRow>, IndexError> {
    let mut decoder = Decoder::new(bytes);
    let count = decoder.u32()? as usize;
    decoder.guard_count::<TypedRow>(count, 4)?;
    let ordinals = if codec == ComponentCodec::PrefixEliasFano {
        let budget = decoder.budget();
        let sequence = decode_elias_fano_with_budget(decoder.bytes()?, budget)?;
        if sequence.len() != count {
            return Err(IndexError::InvalidFormat("typed ordinal count"));
        }
        Some(sequence)
    } else if codec == ComponentCodec::FixedRows {
        None
    } else {
        return Err(IndexError::InvalidFormat("typed block codec"));
    };
    let mut rows = Vec::with_capacity(count);
    for index in 0..count {
        rows.push(TypedRow {
            ordinal: match &ordinals {
                Some(values) => values.get(index)?,
                None => decoder.u64()?,
            },
            payload: TypedPayload::decode(&mut decoder)?,
        });
    }
    decoder.finish()?;
    if rows.is_empty()
        || rows
            .windows(2)
            .any(|pair| pair[0].ordinal >= pair[1].ordinal)
    {
        return Err(IndexError::InvalidFormat("typed ordinal order"));
    }
    Ok(rows)
}

pub(super) async fn read_typed_block<D: IndexDirectoryRead>(
    directory: &D,
    descriptor: &crate::BlockDescriptor,
) -> Result<Vec<TypedRow>, IndexError> {
    let block = crate::run::read_leaf(directory, descriptor).await?;
    let descriptor = descriptor.clone();
    directory
        .run_query_cpu(move || {
            validate_typed_rows(
                decode_typed_rows(block.body(), descriptor.codec)?,
                &descriptor,
            )
        })
        .await
}

pub(super) fn validate_typed_rows(
    rows: Vec<TypedRow>,
    descriptor: &crate::BlockDescriptor,
) -> Result<Vec<TypedRow>, IndexError> {
    if rows.first().map(|row| ordinal_key(row.ordinal)) != Some(descriptor.minimum_key.clone())
        || rows.last().map(|row| ordinal_key(row.ordinal)) != Some(descriptor.maximum_key.clone())
        || rows.len() as u64 != descriptor.element_count
    {
        return Err(IndexError::InvalidFormat("typed block descriptor"));
    }
    Ok(rows)
}

pub(super) struct TypedCursor<'a, D> {
    directory: &'a D,
    leaves: LeafCursor<'a, D>,
    rows: VecDeque<TypedRow>,
}

impl<'a, D: IndexDirectoryRead> TypedCursor<'a, D> {
    pub(super) fn new(directory: &'a D, root: crate::BlockDescriptor) -> Self {
        Self {
            directory,
            leaves: LeafCursor::new(directory, root),
            rows: VecDeque::new(),
        }
    }

    pub(super) async fn next(&mut self) -> Result<Option<TypedRow>, IndexError> {
        loop {
            if let Some(row) = self.rows.pop_front() {
                return Ok(Some(row));
            }
            let Some(descriptor) = self.leaves.next().await? else {
                return Ok(None);
            };
            self.rows = read_typed_block(self.directory, &descriptor).await?.into();
        }
    }
}

pub(super) async fn typed_row<D: IndexDirectoryRead>(
    directory: &D,
    view: &RunView,
    ordinal: u64,
) -> Result<TypedRow, IndexError> {
    let root = view
        .component_optional(ROWS_TAG)
        .ok_or(IndexError::InvalidFormat("missing typed component"))?;
    let descriptor = find_leaf(directory, root, &ordinal_key(ordinal))
        .await?
        .ok_or(IndexError::InvalidFormat("missing typed ordinal"))?;
    let rows = read_typed_block(directory, &descriptor).await?;
    let index = rows
        .binary_search_by_key(&ordinal, |row| row.ordinal)
        .map_err(|_| IndexError::InvalidFormat("missing typed ordinal"))?;
    Ok(rows.into_iter().nth(index).unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_bases_do_not_depend_on_preceding_document_counts() {
        let second = range_ordinal_base(1).unwrap();
        assert_eq!(range_local_ordinal(second, 0).unwrap(), 1u64 << 32);
        assert_eq!(range_local_ordinal(second, 73).unwrap(), (1u64 << 32) + 73);
        assert_eq!(ordinal_range_id((1u64 << 32) + 73), 1);
        assert_eq!(ordinal_local((1u64 << 32) + 73), 73);

        let maximum = compose_ordinal(u32::MAX, u32::MAX as u64).unwrap();
        assert_eq!(maximum, u64::MAX);
        assert_eq!(ordinal_range_id(maximum), u32::MAX);
        assert_eq!(ordinal_local(maximum), u32::MAX as u64);
        assert!(range_local_ordinal(0, u32::MAX as u64 + 1).is_err());
        #[cfg(target_pointer_width = "64")]
        assert!(range_ordinal_base(u32::MAX as usize + 1).is_err());
    }
}
