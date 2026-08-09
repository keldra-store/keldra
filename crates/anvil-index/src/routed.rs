//! Shared key-routed component used by engines whose payload rows are stored by
//! dense document ordinal.

use std::cmp::Ordering;
use std::collections::VecDeque;

use crate::codec::{Decoder, Encoder, encode_component};
use crate::run::{ComponentTree, LeafCursor, RoutingTreeBuilder, read_leaf};
use crate::{
    ComponentCodec, GeneratedBlock, IndexBlockSink, IndexDirectoryRead, IndexError, IndexKind,
    MAX_INDEX_ROUTING_KEY_BYTES,
};

const ROUTING_SUFFIX_BYTES: usize = 12;
/// Conservative charge for the row struct, vector capacity and allocator
/// bookkeeping while derived keys coexist with admitted source mutations.
pub(crate) const ROUTED_ROW_RESIDENT_OVERHEAD_BYTES: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RoutedRow {
    pub(crate) primary: Vec<u8>,
    pub(crate) ordinal: u64,
    pub(crate) position: u32,
}

impl RoutedRow {
    pub(crate) fn new(primary: Vec<u8>, ordinal: u64, position: u32) -> Result<Self, IndexError> {
        let row = Self {
            primary,
            ordinal,
            position,
        };
        row.validate()?;
        Ok(row)
    }

    pub(crate) fn with_ordinal(&self, ordinal: u64) -> Self {
        Self {
            primary: self.primary.clone(),
            ordinal,
            position: self.position,
        }
    }

    pub(crate) fn key(&self) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.primary.len() + ROUTING_SUFFIX_BYTES);
        key.extend_from_slice(&self.primary);
        key.extend_from_slice(&self.ordinal.to_be_bytes());
        key.extend_from_slice(&self.position.to_be_bytes());
        key
    }

    pub(crate) fn compare(&self, other: &Self) -> Ordering {
        self.primary
            .cmp(&other.primary)
            .then_with(|| self.ordinal.cmp(&other.ordinal))
            .then_with(|| self.position.cmp(&other.position))
    }

    fn validate(&self) -> Result<(), IndexError> {
        if self.primary.is_empty()
            || self.primary.len() > MAX_INDEX_ROUTING_KEY_BYTES.saturating_sub(ROUTING_SUFFIX_BYTES)
        {
            return Err(IndexError::InvalidDefinition(
                "routed index key exceeds the format limit".into(),
            ));
        }
        Ok(())
    }
}

pub(crate) struct RoutedComponentWriter {
    kind: IndexKind,
    tag: u8,
    level: u8,
    target_bytes: usize,
    estimated_bytes: usize,
    rows: Vec<RoutedRow>,
    tree: RoutingTreeBuilder,
    emitted: bool,
}

impl RoutedComponentWriter {
    pub(crate) fn new(kind: IndexKind, tag: u8, level: u8, target_bytes: usize) -> Self {
        Self {
            kind,
            tag,
            level,
            target_bytes: target_bytes.max(256),
            estimated_bytes: 0,
            rows: Vec::new(),
            tree: RoutingTreeBuilder::new(kind, tag),
            emitted: false,
        }
    }

    pub(crate) async fn push<S: IndexBlockSink>(
        &mut self,
        row: RoutedRow,
        sink: &mut S,
    ) -> Result<(), IndexError> {
        // Validate rows constructed by merge callers too.
        row.validate()?;
        if self
            .rows
            .last()
            .is_some_and(|previous| previous.compare(&row) != Ordering::Less)
        {
            return Err(IndexError::UnsortedRecords);
        }
        let row_bytes = row.primary.len().saturating_add(ROUTING_SUFFIX_BYTES + 8);
        if !self.rows.is_empty()
            && self.estimated_bytes.saturating_add(row_bytes) > self.target_bytes
        {
            self.flush(sink).await?;
        }
        self.estimated_bytes = self.estimated_bytes.saturating_add(row_bytes);
        self.rows.push(row);
        Ok(())
    }

    async fn flush<S: IndexBlockSink>(&mut self, sink: &mut S) -> Result<(), IndexError> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.rows);
        self.estimated_bytes = 0;
        let codec = if self.level == 0 {
            ComponentCodec::FixedRows
        } else {
            ComponentCodec::PrefixEliasFano
        };
        let first = rows.first().unwrap().key();
        let last = rows.last().unwrap().key();
        let body = encode_rows(&rows, codec)?;
        let bytes = encode_component(self.kind, self.tag, codec, body)?;
        self.tree
            .emit_leaf(
                GeneratedBlock::new(
                    self.kind,
                    self.tag,
                    codec,
                    0,
                    first,
                    last,
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

fn encode_rows(rows: &[RoutedRow], codec: ComponentCodec) -> Result<Vec<u8>, IndexError> {
    let mut output = Encoder::default();
    output.u32(rows.len())?;
    let mut previous = Vec::new();
    for row in rows {
        let key = row.key();
        match codec {
            ComponentCodec::FixedRows => output.bytes(&key)?,
            ComponentCodec::PrefixEliasFano => {
                let shared = common_prefix(&previous, &key);
                output.u32(shared)?;
                output.bytes(&key[shared..])?;
            }
            _ => return Err(IndexError::InvalidFormat("routed key block codec")),
        }
        previous = key;
    }
    Ok(output.finish())
}

fn decode_rows(bytes: &[u8], codec: ComponentCodec) -> Result<Vec<RoutedRow>, IndexError> {
    let mut decoder = Decoder::new(bytes);
    let count = decoder.u32()? as usize;
    let minimum_encoded = if codec == ComponentCodec::FixedRows {
        4
    } else {
        8
    };
    decoder.guard_count::<RoutedRow>(count, minimum_encoded)?;
    let mut previous = Vec::new();
    let mut charged_previous = 0usize;
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let key = match codec {
            ComponentCodec::FixedRows => decoder.bytes()?.to_vec(),
            ComponentCodec::PrefixEliasFano => {
                let shared = decoder.u32()? as usize;
                if shared > previous.len() {
                    return Err(IndexError::InvalidFormat("routed key prefix"));
                }
                let mut key = previous[..shared].to_vec();
                key.extend_from_slice(decoder.bytes()?);
                key
            }
            _ => return Err(IndexError::InvalidFormat("routed key block codec")),
        };
        if key.len() <= ROUTING_SUFFIX_BYTES || key.len() > MAX_INDEX_ROUTING_KEY_BYTES {
            return Err(IndexError::InvalidFormat("routed key length"));
        }
        let primary_end = key.len() - ROUTING_SUFFIX_BYTES;
        decoder.charge(primary_end)?;
        if key.len() > charged_previous {
            decoder.charge(key.len() - charged_previous)?;
            charged_previous = key.len();
        }
        let ordinal_end = primary_end + 8;
        let ordinal = u64::from_be_bytes(
            key[primary_end..ordinal_end]
                .try_into()
                .map_err(|_| IndexError::InvalidFormat("routed ordinal"))?,
        );
        let position = u32::from_be_bytes(
            key[ordinal_end..]
                .try_into()
                .map_err(|_| IndexError::InvalidFormat("routed position"))?,
        );
        rows.push(RoutedRow {
            primary: key[..primary_end].to_vec(),
            ordinal,
            position,
        });
        previous = key;
    }
    decoder.finish()?;
    if rows.is_empty()
        || rows
            .windows(2)
            .any(|pair| pair[0].compare(&pair[1]) != Ordering::Less)
    {
        return Err(IndexError::InvalidFormat("routed key order"));
    }
    Ok(rows)
}

async fn read_block<D: IndexDirectoryRead>(
    directory: &D,
    descriptor: &crate::BlockDescriptor,
) -> Result<Vec<RoutedRow>, IndexError> {
    let block = read_leaf(directory, descriptor).await?;
    let rows = decode_rows(block.body(), descriptor.codec)?;
    if rows.first().map(RoutedRow::key) != Some(descriptor.minimum_key.clone())
        || rows.last().map(RoutedRow::key) != Some(descriptor.maximum_key.clone())
        || rows.len() as u64 != descriptor.element_count
    {
        return Err(IndexError::InvalidFormat("routed key block descriptor"));
    }
    Ok(rows)
}

pub(crate) struct RoutedCursor<'a, D> {
    directory: &'a D,
    leaves: LeafCursor<'a, D>,
    rows: VecDeque<RoutedRow>,
    prefix: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
}

impl<'a, D: IndexDirectoryRead> RoutedCursor<'a, D> {
    pub(crate) fn new(
        directory: &'a D,
        root: crate::BlockDescriptor,
        prefix: Option<Vec<u8>>,
    ) -> Self {
        let upper = prefix.as_deref().and_then(prefix_successor);
        Self {
            directory,
            leaves: LeafCursor::new(directory, root),
            rows: VecDeque::new(),
            prefix,
            upper,
        }
    }

    pub(crate) async fn next(&mut self) -> Result<Option<RoutedRow>, IndexError> {
        loop {
            while let Some(row) = self.rows.pop_front() {
                if self
                    .prefix
                    .as_ref()
                    .is_none_or(|prefix| row.primary.starts_with(prefix))
                {
                    return Ok(Some(row));
                }
            }
            let Some(descriptor) = self.leaves.next().await? else {
                return Ok(None);
            };
            if let Some(prefix) = &self.prefix {
                if descriptor.maximum_key.as_slice() < prefix.as_slice()
                    || self
                        .upper
                        .as_ref()
                        .is_some_and(|upper| descriptor.minimum_key.as_slice() >= upper.as_slice())
                {
                    continue;
                }
            }
            self.rows = read_block(self.directory, &descriptor).await?.into();
        }
    }
}

pub(crate) fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut successor = prefix.to_vec();
    while let Some(byte) = successor.pop() {
        if byte != u8::MAX {
            successor.push(byte + 1);
            return Some(successor);
        }
    }
    None
}

fn common_prefix(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right)
        .take_while(|(left, right)| left == right)
        .count()
}
