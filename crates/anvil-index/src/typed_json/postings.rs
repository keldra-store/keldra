//! Compressed typed-value postings over range-local document identities.

use std::collections::VecDeque;

use crate::codec::{Decoder, Encoder, encode_component};
use crate::routed::{RoutedRow, prefix_successor};
use crate::run::{ComponentTree, LeafCursor, RoutingTreeBuilder};
use crate::succinct::{decode_elias_fano_with_budget, elias_fano_encoded_len, encode_elias_fano};
use crate::{
    ComponentCodec, GeneratedBlock, IndexBlockSink, IndexDirectoryRead, IndexError, IndexKind,
};

use super::KEYS_TAG;
use super::identity::{compose_ordinal, ordinal_local, ordinal_range_id};

/// Authoritative Typed JSON and Metadata Filter postings. Canonical field/value
/// keys are stored once per block, then each path-range gets one succinct
/// monotone list of local dictionary ordinals.
pub(crate) struct PostingComponentWriter {
    kind: IndexKind,
    target_bytes: usize,
    estimated_bytes: usize,
    rows: Vec<RoutedRow>,
    last_key: Option<Vec<u8>>,
    tree: RoutingTreeBuilder,
    emitted: bool,
}

impl PostingComponentWriter {
    pub(crate) fn new(kind: IndexKind, target_bytes: usize) -> Self {
        Self {
            kind,
            target_bytes: target_bytes.max(256),
            estimated_bytes: 0,
            rows: Vec::new(),
            last_key: None,
            tree: RoutingTreeBuilder::new(kind, KEYS_TAG),
            emitted: false,
        }
    }

    pub(crate) async fn push<S: IndexBlockSink>(
        &mut self,
        row: RoutedRow,
        sink: &mut S,
    ) -> Result<(), IndexError> {
        let key = row.key();
        if self
            .last_key
            .as_ref()
            .is_some_and(|previous| previous >= &key)
        {
            return Err(IndexError::UnsortedRecords);
        }
        // Charge every row as if it were its own primary and range group. The
        // actual codec groups both and therefore cannot exceed this bound.
        // This matters for high-cardinality fields: every singleton posting
        // carries Elias-Fano support metadata that the old 24-byte estimate
        // omitted, allowing an L0 leaf to cross the fixed block ceiling.
        let sequence_bytes = elias_fano_encoded_len(1, ordinal_local(row.ordinal))?;
        let row_bytes = row
            .primary
            .len()
            .checked_add(28)
            .and_then(|bytes| bytes.checked_add(sequence_bytes))
            .ok_or(IndexError::OffsetOverflow)?;
        if !self.rows.is_empty()
            && self.estimated_bytes.saturating_add(row_bytes) > self.target_bytes
        {
            self.flush(sink).await?;
        }
        self.estimated_bytes = self.estimated_bytes.saturating_add(row_bytes);
        self.last_key = Some(key);
        self.rows.push(row);
        Ok(())
    }

    async fn flush<S: IndexBlockSink>(&mut self, sink: &mut S) -> Result<(), IndexError> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.rows);
        self.estimated_bytes = 0;
        let minimum_key = rows.first().unwrap().key();
        let maximum_key = rows.last().unwrap().key();
        let body = encode_posting_rows(&rows)?;
        let bytes = encode_component(
            self.kind,
            KEYS_TAG,
            ComponentCodec::QuasiSuccinctPostings,
            body,
        )?;
        self.tree
            .emit_leaf(
                GeneratedBlock::new(
                    self.kind,
                    KEYS_TAG,
                    ComponentCodec::QuasiSuccinctPostings,
                    0,
                    minimum_key,
                    maximum_key,
                    rows.len() as u64,
                    bytes,
                )?,
                sink,
            )
            .await?;
        self.emitted = true;
        Ok(())
    }

    pub(crate) async fn finish<S: IndexBlockSink>(
        mut self,
        sink: &mut S,
    ) -> Result<Option<ComponentTree>, IndexError> {
        self.flush(sink).await?;
        if !self.emitted {
            return Ok(None);
        }
        self.tree.finish(sink).await.map(Some)
    }
}

fn encode_posting_rows(rows: &[RoutedRow]) -> Result<Vec<u8>, IndexError> {
    if rows.is_empty()
        || rows
            .windows(2)
            .any(|pair| pair[0].compare(&pair[1]).is_ge())
    {
        return Err(IndexError::UnsortedRecords);
    }
    let mut output = Encoder::default();
    output.u32(rows.len())?;
    let primary_count = rows
        .iter()
        .enumerate()
        .filter(|(index, row)| *index == 0 || rows[*index - 1].primary != row.primary)
        .count();
    output.u32(primary_count)?;
    let mut previous_primary = Vec::new();
    let mut primary_start = 0usize;
    while primary_start < rows.len() {
        let primary = &rows[primary_start].primary;
        let primary_end = rows[primary_start..]
            .iter()
            .position(|row| &row.primary != primary)
            .map_or(rows.len(), |offset| primary_start + offset);
        let shared = common_prefix(&previous_primary, primary);
        output.u32(shared)?;
        output.bytes(&primary[shared..])?;

        let primary_rows = &rows[primary_start..primary_end];
        let range_count = primary_rows
            .iter()
            .enumerate()
            .filter(|(index, row)| {
                *index == 0
                    || ordinal_range_id(primary_rows[*index - 1].ordinal)
                        != ordinal_range_id(row.ordinal)
            })
            .count();
        output.u32(range_count)?;
        let mut range_start = 0usize;
        while range_start < primary_rows.len() {
            let range_id = ordinal_range_id(primary_rows[range_start].ordinal);
            let range_end = primary_rows[range_start..]
                .iter()
                .position(|row| ordinal_range_id(row.ordinal) != range_id)
                .map_or(primary_rows.len(), |offset| range_start + offset);
            let range_rows = &primary_rows[range_start..range_end];
            output.raw_u32(range_id);
            output.u32(range_rows.len())?;
            output.bytes(&encode_elias_fano(
                &range_rows
                    .iter()
                    .map(|row| ordinal_local(row.ordinal))
                    .collect::<Vec<_>>(),
            )?)?;
            for row in range_rows {
                output.raw_u32(row.position);
            }
            range_start = range_end;
        }
        previous_primary = primary.clone();
        primary_start = primary_end;
    }
    Ok(output.finish())
}

fn decode_posting_rows(bytes: &[u8]) -> Result<Vec<RoutedRow>, IndexError> {
    let mut decoder = Decoder::new(bytes);
    let row_count = decoder.u32()? as usize;
    decoder.guard_count::<RoutedRow>(row_count, 1)?;
    let primary_count = decoder.u32()? as usize;
    decoder.guard_count::<Vec<u8>>(primary_count, 8)?;
    if row_count == 0 || primary_count == 0 || primary_count > row_count {
        return Err(IndexError::InvalidFormat("typed posting counts"));
    }
    let mut rows = Vec::with_capacity(row_count);
    let mut previous_primary = Vec::new();
    for primary_index in 0..primary_count {
        let shared = decoder.u32()? as usize;
        if shared > previous_primary.len() {
            return Err(IndexError::InvalidFormat("typed posting key prefix"));
        }
        let suffix = decoder.bytes()?;
        let mut primary = previous_primary[..shared].to_vec();
        primary.extend_from_slice(suffix);
        if primary.is_empty()
            || (primary_index > 0 && previous_primary >= primary)
            || primary.len() > crate::MAX_INDEX_ROUTING_KEY_BYTES.saturating_sub(12)
        {
            return Err(IndexError::InvalidFormat("typed posting key order"));
        }
        decoder.charge(primary.len())?;
        let range_count = decoder.u32()? as usize;
        decoder.guard_count::<u32>(range_count, 12)?;
        if range_count == 0 {
            return Err(IndexError::InvalidFormat("typed posting range count"));
        }
        let mut previous_range = None;
        for _ in 0..range_count {
            let range_id = decoder.u32()?;
            if previous_range.is_some_and(|previous| previous >= range_id) {
                return Err(IndexError::InvalidFormat("typed posting range order"));
            }
            let posting_count = decoder.u32()? as usize;
            decoder.guard_count::<u32>(posting_count, 0)?;
            if posting_count == 0 || rows.len().saturating_add(posting_count) > row_count {
                return Err(IndexError::InvalidFormat("typed posting list count"));
            }
            decoder.charge(
                primary
                    .len()
                    .checked_mul(posting_count)
                    .ok_or(IndexError::OffsetOverflow)?,
            )?;
            let budget = decoder.budget();
            let locals = decode_elias_fano_with_budget(decoder.bytes()?, budget)?;
            if locals.len() != posting_count {
                return Err(IndexError::InvalidFormat("typed posting ordinal count"));
            }
            let mut previous_local = None;
            for index in 0..posting_count {
                let local = locals.get(index)?;
                if previous_local.is_some_and(|previous| previous >= local) {
                    return Err(IndexError::InvalidFormat("typed posting ordinal order"));
                }
                rows.push(RoutedRow::new(
                    primary.clone(),
                    compose_ordinal(range_id, local)
                        .map_err(|_| IndexError::InvalidFormat("typed posting range identity"))?,
                    decoder.u32()?,
                )?);
                previous_local = Some(local);
            }
            previous_range = Some(range_id);
        }
        previous_primary = primary;
    }
    decoder.finish()?;
    if rows.len() != row_count
        || rows
            .windows(2)
            .any(|pair| pair[0].compare(&pair[1]).is_ge())
    {
        return Err(IndexError::InvalidFormat("typed posting row order"));
    }
    Ok(rows)
}

async fn read_posting_block<D: IndexDirectoryRead>(
    directory: &D,
    descriptor: &crate::BlockDescriptor,
) -> Result<Vec<RoutedRow>, IndexError> {
    if descriptor.codec != ComponentCodec::QuasiSuccinctPostings {
        return Err(IndexError::InvalidFormat("typed posting block codec"));
    }
    let block = crate::run::read_leaf(directory, descriptor).await?;
    let descriptor = descriptor.clone();
    directory
        .run_query_cpu(move || {
            let rows = decode_posting_rows(block.body())?;
            if rows.first().map(RoutedRow::key) != Some(descriptor.minimum_key.clone())
                || rows.last().map(RoutedRow::key) != Some(descriptor.maximum_key.clone())
                || rows.len() as u64 != descriptor.element_count
            {
                return Err(IndexError::InvalidFormat("typed posting block descriptor"));
            }
            Ok(rows)
        })
        .await
}

pub(super) struct PostingCursor<'a, D> {
    directory: &'a D,
    leaves: LeafCursor<'a, D>,
    rows: VecDeque<RoutedRow>,
    range: Option<crate::compaction::KeyRange>,
    reverse: bool,
}

impl<'a, D: IndexDirectoryRead> PostingCursor<'a, D> {
    pub(super) fn new(
        directory: &'a D,
        root: crate::BlockDescriptor,
        prefix: Option<Vec<u8>>,
    ) -> Self {
        let range = prefix.map(|lower| crate::compaction::KeyRange {
            upper: prefix_successor(&lower),
            lower: Some(lower),
        });
        let leaves = match &range {
            Some(range) => LeafCursor::in_range(directory, root, range.clone()),
            None => LeafCursor::new(directory, root),
        };
        Self {
            directory,
            leaves,
            rows: VecDeque::new(),
            range,
            reverse: false,
        }
    }

    pub(super) fn in_range(
        directory: &'a D,
        root: crate::BlockDescriptor,
        range: crate::compaction::KeyRange,
    ) -> Self {
        Self {
            directory,
            leaves: LeafCursor::in_range(directory, root, range.clone()),
            rows: VecDeque::new(),
            range: Some(range),
            reverse: false,
        }
    }

    pub(super) fn in_range_reverse(
        directory: &'a D,
        root: crate::BlockDescriptor,
        range: crate::compaction::KeyRange,
    ) -> Self {
        Self {
            directory,
            leaves: LeafCursor::in_range_reverse(directory, root, range.clone()),
            rows: VecDeque::new(),
            range: Some(range),
            reverse: true,
        }
    }

    pub(super) async fn next(&mut self) -> Result<Option<RoutedRow>, IndexError> {
        loop {
            while let Some(row) = if self.reverse {
                self.rows.pop_back()
            } else {
                self.rows.pop_front()
            } {
                if self
                    .range
                    .as_ref()
                    .is_none_or(|range| range.contains(&row.primary))
                {
                    return Ok(Some(row));
                }
            }
            let Some(descriptor) = self.leaves.next().await? else {
                return Ok(None);
            };
            self.rows = read_posting_block(self.directory, &descriptor)
                .await?
                .into();
        }
    }
}

/// A cheap upper bound for choosing between predicate posting ranges. Routing
/// descriptors are authoritative and already carry descendant row counts. A
/// boundary leaf may include adjacent keys, which deliberately overestimates
/// rather than decoding payload blocks during planning.
pub(super) async fn estimate_posting_ranges<D: IndexDirectoryRead>(
    directory: &D,
    root: crate::BlockDescriptor,
    ranges: &[crate::compaction::KeyRange],
) -> Result<u64, IndexError> {
    let mut count = 0u64;
    for range in ranges {
        let (covered, boundary) =
            crate::run::range_descriptor_coverage(directory, root.clone(), range).await?;
        let boundary = boundary.iter().try_fold(0u64, |count, descriptor| {
            count
                .checked_add(descriptor.element_count)
                .ok_or(IndexError::OffsetOverflow)
        })?;
        count = count
            .checked_add(covered)
            .and_then(|count| count.checked_add(boundary))
            .ok_or(IndexError::OffsetOverflow)?;
    }
    Ok(count)
}

/// Count one posting range exactly while decoding only leaves which straddle a
/// range boundary. Interior leaf counts come directly from their descriptors.
pub(super) async fn count_posting_range<D: IndexDirectoryRead>(
    directory: &D,
    root: crate::BlockDescriptor,
    range: crate::compaction::KeyRange,
) -> Result<u64, IndexError> {
    let (mut count, boundary) =
        crate::run::range_descriptor_coverage(directory, root, &range).await?;
    for descriptor in boundary {
        let rows = u64::try_from(
            read_posting_block(directory, &descriptor)
                .await?
                .into_iter()
                .filter(|row| range.contains(&row.primary))
                .count(),
        )
        .map_err(|_| IndexError::OffsetOverflow)?;
        count = count.checked_add(rows).ok_or(IndexError::OffsetOverflow)?;
    }
    Ok(count)
}

fn common_prefix(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right)
        .take_while(|(left, right)| left == right)
        .count()
}

#[cfg(test)]
mod tests {
    use crate::io::tests::MemoryBlockSink;

    use super::*;

    #[tokio::test]
    async fn postings_round_trip_range_local_identities_in_succinct_lists() {
        let primary = b"status\0\x03active\0\0".to_vec();
        for kind in [IndexKind::TypedJson, IndexKind::MetadataFilter] {
            let mut sink = MemoryBlockSink::default();
            let mut writer = PostingComponentWriter::new(kind, 64 * 1024);
            for range in 0..3u32 {
                for local in 0..2_000u64 {
                    writer
                        .push(
                            RoutedRow::new(
                                primary.clone(),
                                compose_ordinal(range, local * 2).unwrap(),
                                0,
                            )
                            .unwrap(),
                            &mut sink,
                        )
                        .await
                        .unwrap();
                }
            }
            let tree = writer.finish(&mut sink).await.unwrap().unwrap();
            let mut cursor = PostingCursor::new(&sink, tree.root, Some(primary.clone()));
            let mut rows = Vec::new();
            while let Some(row) = cursor.next().await.unwrap() {
                rows.push(row);
            }
            assert_eq!(rows.len(), 6_000);
            assert_eq!(ordinal_range_id(rows[2_000].ordinal), 1);
            assert_eq!(ordinal_local(rows[2_000].ordinal), 0);
            assert!(
                rows.windows(2)
                    .all(|pair| pair[0].compare(&pair[1]).is_lt())
            );
        }
    }

    #[tokio::test]
    async fn postings_cursor_honours_arbitrary_half_open_ranges() {
        let mut sink = MemoryBlockSink::default();
        let mut writer = PostingComponentWriter::new(IndexKind::TypedJson, 256);
        for (primary, ordinal) in [
            (b"a".to_vec(), 1),
            (b"b".to_vec(), 2),
            (b"c".to_vec(), 3),
            (b"d".to_vec(), 4),
        ] {
            writer
                .push(RoutedRow::new(primary, ordinal, 0).unwrap(), &mut sink)
                .await
                .unwrap();
        }
        let tree = writer.finish(&mut sink).await.unwrap().unwrap();
        let mut cursor = PostingCursor::in_range(
            &sink,
            tree.root,
            crate::compaction::KeyRange {
                lower: Some(b"b".to_vec()),
                upper: Some(b"d".to_vec()),
            },
        );
        let mut primaries = Vec::new();
        while let Some(row) = cursor.next().await.unwrap() {
            primaries.push(row.primary);
        }
        assert_eq!(primaries, [b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn repeated_canonical_keys_are_materially_smaller_than_fixed_routed_rows() {
        let primary = vec![b'x'; 64];
        let rows = (0..20_000u64)
            .map(|local| RoutedRow::new(primary.clone(), local, 0).unwrap())
            .collect::<Vec<_>>();
        let encoded = encode_posting_rows(&rows).unwrap();
        let fixed = rows.len() * (primary.len() + 12 + 4);
        assert!(encoded.len() < fixed / 4);
        assert_eq!(decode_posting_rows(&encoded).unwrap(), rows);
    }
}
