use crate::IndexError;

use super::codec::{COMPONENT_HEADER_BYTES, Decoder, Encoder};
use super::model::{DocId, INDEX_COMPONENT_BYTES, INDEX_ROUTING_KEY_BYTES};

const LOCATOR_CODEC_VERSION: u16 = 1;
const MAX_PAYLOAD_BYTES: usize = INDEX_COMPONENT_BYTES - COMPONENT_HEADER_BYTES;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DocIdRange {
    pub segment_id: u64,
    pub first_doc_id: DocId,
    pub count: u32,
}

impl DocIdRange {
    fn validate(&self) -> Result<(), IndexError> {
        if self.segment_id == 0 || self.count == 0 {
            return Err(IndexError::InvalidDefinition(
                "locator DocId ranges require a segment and non-zero count".into(),
            ));
        }
        self.first_doc_id
            .get()
            .checked_add(self.count - 1)
            .ok_or(IndexError::OffsetOverflow)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocatorValue {
    Live {
        object_version: u64,
        ranges: Vec<DocIdRange>,
    },
    Deleted {
        tombstone_version: u64,
    },
}

impl LocatorValue {
    pub fn version(&self) -> u64 {
        match self {
            Self::Live { object_version, .. } => *object_version,
            Self::Deleted { tombstone_version } => *tombstone_version,
        }
    }

    fn validate(&self) -> Result<(), IndexError> {
        match self {
            Self::Live {
                object_version,
                ranges,
            } if *object_version != 0 && !ranges.is_empty() => {
                for range in ranges {
                    range.validate()?;
                }
                if ranges.windows(2).any(|pair| {
                    (pair[0].segment_id, pair[0].first_doc_id)
                        >= (pair[1].segment_id, pair[1].first_doc_id)
                        || pair[0].segment_id == pair[1].segment_id
                            && pair[0]
                                .first_doc_id
                                .get()
                                .checked_add(pair[0].count)
                                .is_none_or(|end| end > pair[1].first_doc_id.get())
                }) {
                    return Err(IndexError::InvalidDefinition(
                        "locator DocId ranges must be ordered and non-overlapping".into(),
                    ));
                }
                Ok(())
            }
            Self::Deleted { tombstone_version } if *tombstone_version != 0 => Ok(()),
            _ => Err(IndexError::InvalidDefinition(
                "path locator versions and live segment IDs must be non-zero".into(),
            )),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocatorEntry {
    pub path: String,
    pub value: LocatorValue,
}

impl LocatorEntry {
    fn validate(&self) -> Result<(), IndexError> {
        if self.path.is_empty()
            || self.path.len() > INDEX_ROUTING_KEY_BYTES
            || self.path.contains('\0')
        {
            return Err(IndexError::InvalidDefinition(
                "path locator key must be 1..=4096 bytes without NUL".into(),
            ));
        }
        self.value.validate()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PathLocatorBlock {
    entries: Vec<LocatorEntry>,
}

pub(crate) struct PathLocatorBlockBuilder {
    entries: Vec<LocatorEntry>,
    encoded_bytes: usize,
}

impl Default for PathLocatorBlockBuilder {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            encoded_bytes: 6,
        }
    }
}

impl PathLocatorBlockBuilder {
    pub(crate) fn push(&mut self, entry: LocatorEntry) -> Result<Option<LocatorEntry>, IndexError> {
        entry.validate()?;
        if self
            .entries
            .last()
            .is_some_and(|previous| previous.path >= entry.path)
        {
            return Err(IndexError::UnsortedRecords);
        }
        let row = locator_entry_encoded_bytes(&entry)?;
        if !self.entries.is_empty() && self.encoded_bytes.saturating_add(row) > MAX_PAYLOAD_BYTES {
            return Ok(Some(entry));
        }
        if self.encoded_bytes.saturating_add(row) > MAX_PAYLOAD_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: self.encoded_bytes.saturating_add(row) + COMPONENT_HEADER_BYTES,
                limit: INDEX_COMPONENT_BYTES,
            });
        }
        self.encoded_bytes += row;
        self.entries.push(entry);
        Ok(None)
    }

    pub(crate) fn finish(&mut self) -> Result<Option<PathLocatorBlock>, IndexError> {
        if self.entries.is_empty() {
            return Ok(None);
        }
        self.encoded_bytes = 6;
        PathLocatorBlock::new(std::mem::take(&mut self.entries)).map(Some)
    }
}

impl PathLocatorBlock {
    pub fn new(entries: Vec<LocatorEntry>) -> Result<Self, IndexError> {
        validate_entries(&entries)?;
        let block = Self { entries };
        let needed = block.encode_payload()?.len();
        if needed > MAX_PAYLOAD_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: needed + COMPONENT_HEADER_BYTES,
                limit: INDEX_COMPONENT_BYTES,
            });
        }
        Ok(block)
    }

    pub fn entries(&self) -> &[LocatorEntry] {
        &self.entries
    }

    pub(crate) fn into_entries(self) -> Vec<LocatorEntry> {
        self.entries
    }

    pub fn get(&self, path: &str) -> Option<&LocatorValue> {
        self.entries
            .binary_search_by(|entry| entry.path.as_str().cmp(path))
            .ok()
            .map(|index| &self.entries[index].value)
    }

    pub fn lower_bound(&self, path: &str) -> usize {
        self.entries
            .partition_point(|entry| entry.path.as_str() < path)
    }

    pub fn encode_payload(&self) -> Result<Vec<u8>, IndexError> {
        validate_entries(&self.entries)?;
        let mut out = Encoder::default();
        out.u16(LOCATOR_CODEC_VERSION);
        out.usize_u32(self.entries.len())?;
        for entry in &self.entries {
            out.string(&entry.path)?;
            match entry.value {
                LocatorValue::Live {
                    object_version,
                    ref ranges,
                } => {
                    out.u8(1);
                    out.u64(object_version);
                    out.usize_u32(ranges.len())?;
                    for range in ranges {
                        out.u64(range.segment_id);
                        out.u32(range.first_doc_id.get());
                        out.u32(range.count);
                    }
                }
                LocatorValue::Deleted { tombstone_version } => {
                    out.u8(2);
                    out.u64(tombstone_version);
                }
            }
        }
        Ok(out.finish())
    }

    pub fn decode_payload(bytes: &[u8]) -> Result<Self, IndexError> {
        let mut input = Decoder::new(bytes)?;
        if input.u16()? != LOCATOR_CODEC_VERSION {
            return Err(IndexError::InvalidFormat("path-locator codec version"));
        }
        let count = usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
        input.claim(
            count
                .checked_mul(std::mem::size_of::<LocatorEntry>())
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let path = input.string()?;
            let value = match input.u8()? {
                1 => LocatorValue::Live {
                    object_version: input.u64()?,
                    ranges: {
                        let count = usize::try_from(input.u32()?)
                            .map_err(|_| IndexError::OffsetOverflow)?;
                        input.claim(
                            count
                                .checked_mul(std::mem::size_of::<DocIdRange>())
                                .ok_or(IndexError::OffsetOverflow)?,
                        )?;
                        let mut ranges = Vec::with_capacity(count);
                        for _ in 0..count {
                            ranges.push(DocIdRange {
                                segment_id: input.u64()?,
                                first_doc_id: DocId::new(input.u32()?),
                                count: input.u32()?,
                            });
                        }
                        ranges
                    },
                },
                2 => LocatorValue::Deleted {
                    tombstone_version: input.u64()?,
                },
                _ => return Err(IndexError::InvalidFormat("path-locator value tag")),
            };
            entries.push(LocatorEntry { path, value });
        }
        input.finish()?;
        Self::new(entries)
    }

    pub fn split(entries: Vec<LocatorEntry>) -> Result<Vec<Self>, IndexError> {
        validate_entries(&entries)?;
        let mut blocks = Vec::new();
        let mut pending = Vec::new();
        let mut bytes = 6usize;
        for entry in entries {
            let row = locator_entry_encoded_bytes(&entry)?;
            if !pending.is_empty() && bytes.saturating_add(row) > MAX_PAYLOAD_BYTES {
                blocks.push(Self::new(std::mem::take(&mut pending))?);
                bytes = 6;
            }
            if bytes.saturating_add(row) > MAX_PAYLOAD_BYTES {
                return Err(IndexError::ResourceLimit {
                    needed: bytes.saturating_add(row) + COMPONENT_HEADER_BYTES,
                    limit: INDEX_COMPONENT_BYTES,
                });
            }
            bytes += row;
            pending.push(entry);
        }
        if !pending.is_empty() {
            blocks.push(Self::new(pending)?);
        }
        Ok(blocks)
    }
}

fn locator_entry_encoded_bytes(entry: &LocatorEntry) -> Result<usize, IndexError> {
    4usize
        .checked_add(entry.path.len())
        .and_then(|value| {
            let value_bytes = match entry.value {
                LocatorValue::Live { ref ranges, .. } => ranges
                    .len()
                    .checked_mul(16)
                    .and_then(|bytes| bytes.checked_add(1 + 8 + 4)),
                LocatorValue::Deleted { .. } => Some(1 + 8),
            }?;
            value.checked_add(value_bytes)
        })
        .ok_or(IndexError::OffsetOverflow)
}

fn validate_entries(entries: &[LocatorEntry]) -> Result<(), IndexError> {
    for entry in entries {
        entry.validate()?;
    }
    if entries.windows(2).any(|pair| pair[0].path >= pair[1].path) {
        return Err(IndexError::UnsortedRecords);
    }
    Ok(())
}

/// Fold one sorted locator delta over its sorted predecessor. The highest
/// object/tombstone version wins. At an equal live object version the newer
/// delta may replace DocId ranges after a segment merge; live/delete conflicts
/// at one version remain corruption.
pub fn merge_locator_entries(
    previous: &[LocatorEntry],
    delta: &[LocatorEntry],
) -> Result<Vec<LocatorEntry>, IndexError> {
    validate_entries(previous)?;
    validate_entries(delta)?;
    let mut merged = Vec::with_capacity(previous.len().saturating_add(delta.len()));
    let (mut left, mut right) = (0usize, 0usize);
    while left < previous.len() || right < delta.len() {
        let selected = match (previous.get(left), delta.get(right)) {
            (Some(old), Some(new)) if old.path < new.path => {
                left += 1;
                old
            }
            (Some(old), Some(new)) if old.path > new.path => {
                right += 1;
                new
            }
            (Some(old), Some(new)) => {
                left += 1;
                right += 1;
                match old.value.version().cmp(&new.value.version()) {
                    std::cmp::Ordering::Less => new,
                    std::cmp::Ordering::Greater => old,
                    std::cmp::Ordering::Equal if old == new => old,
                    // A segment merge reassigns DocIds without changing the
                    // source object version. `delta` is the newer locator
                    // sequence, so its live ranges replace the predecessor.
                    std::cmp::Ordering::Equal
                        if matches!(old.value, LocatorValue::Live { .. })
                            && matches!(new.value, LocatorValue::Live { .. }) =>
                    {
                        new
                    }
                    std::cmp::Ordering::Equal => {
                        return Err(IndexError::InvalidFormat(
                            "conflicting path-locator values at one version",
                        ));
                    }
                }
            }
            (Some(old), None) => {
                left += 1;
                old
            }
            (None, Some(new)) => {
                right += 1;
                new
            }
            (None, None) => break,
        };
        merged.push(selected.clone());
    }
    Ok(merged)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live(path: &str, version: u64, doc: u32) -> LocatorEntry {
        LocatorEntry {
            path: path.into(),
            value: LocatorValue::Live {
                object_version: version,
                ranges: vec![DocIdRange {
                    segment_id: 9,
                    first_doc_id: DocId::new(doc),
                    count: 1,
                }],
            },
        }
    }

    #[test]
    fn tombstone_prevents_stale_resurrection() {
        let previous = vec![live("a", 7, 0), live("b", 4, 1)];
        let delta = vec![
            LocatorEntry {
                path: "a".into(),
                value: LocatorValue::Deleted {
                    tombstone_version: 8,
                },
            },
            live("b", 3, 2),
        ];
        let merged = merge_locator_entries(&previous, &delta).unwrap();
        assert!(matches!(merged[0].value, LocatorValue::Deleted { .. }));
        assert_eq!(merged[1], previous[1]);
        let block = PathLocatorBlock::new(merged).unwrap();
        let decoded = PathLocatorBlock::decode_payload(&block.encode_payload().unwrap()).unwrap();
        assert_eq!(decoded, block);
    }

    #[test]
    fn same_version_with_different_value_is_corruption() {
        let old = vec![live("a", 7, 0)];
        let relocated = vec![live("a", 7, 1)];
        assert_eq!(merge_locator_entries(&old, &relocated).unwrap(), relocated);
        let deleted = vec![LocatorEntry {
            path: "a".into(),
            value: LocatorValue::Deleted {
                tombstone_version: 7,
            },
        }];
        assert!(merge_locator_entries(&old, &deleted).is_err());
    }
}
