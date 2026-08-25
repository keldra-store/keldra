//! Durable, non-serving progress root for one initial or explicit rebuild.

use std::collections::BTreeMap;

use keldra_atomic_program::MAX_OBJECT_PATH_BYTES;
use keldra_consensus::NodeId;
use keldra_store::{ObjectKey, PlacementLogId, SourceId};
use thiserror::Error;

use super::committed_view::IndexCommitManifest;
use super::events::{AtomicProgramWatermark, IndexBarrier, IndexSourceCursor};

const MAGIC: &[u8; 8] = b"ANVLRBL4";
const CODEC_VERSION: u16 = 1;
const MAX_SOURCES: usize = 1_024;
pub(crate) const MAX_REBUILD_ROOT_BYTES: usize = 64 * 1024 * 1024;

/// One exact-CAS checkpoint. `candidate` names only completed durable output;
/// active and frozen local materialization is intentionally absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DurableRebuildRoot {
    pub index_id: u64,
    pub definition_version: u64,
    pub attempt_id: u64,
    pub baseline: IndexBarrier,
    pub last_canonical_path: Option<String>,
    pub baseline_complete: bool,
    pub scanned_records: u64,
    pub scanned_bytes: u64,
    pub candidate: IndexCommitManifest,
}

impl DurableRebuildRoot {
    pub(crate) fn validate(&self) -> Result<(), RebuildRootError> {
        self.candidate
            .validate()
            .map_err(|error| RebuildRootError::Invalid(error.to_string()))?;
        if self.index_id == 0
            || self.definition_version == 0
            || self.attempt_id == 0
            || self.candidate.index_id != self.index_id
            || self.candidate.definition_version != self.definition_version
            || !self.baseline.atomic.is_clear()
            || self.baseline.fence.term == 0
            || self.baseline.fence.index == 0
            || self.baseline.sources.is_empty()
            || self.baseline.sources.len() > MAX_SOURCES
            || (self.scanned_records == 0) != self.last_canonical_path.is_none()
        {
            return Err(RebuildRootError::Invalid(
                "rebuild root identity or progress is invalid".into(),
            ));
        }
        if let Some(path) = self.last_canonical_path.as_deref()
            && (path.len() > MAX_OBJECT_PATH_BYTES || ObjectKey::new("r", "r", path).is_err())
        {
            return Err(RebuildRootError::Invalid(
                "rebuild progress path is not canonical".into(),
            ));
        }
        let candidate_barrier = self
            .candidate
            .barrier()
            .map_err(|error| RebuildRootError::Invalid(error.to_string()))?;
        if candidate_barrier.fence != self.baseline.fence
            || !candidate_barrier.atomic.covers(self.baseline.atomic)
            || self.baseline.sources.iter().any(|(node, baseline)| {
                candidate_barrier.sources.get(node).is_none_or(|candidate| {
                    candidate.source != baseline.source
                        || candidate.next_offset < baseline.next_offset
                })
            })
        {
            return Err(RebuildRootError::Invalid(
                "candidate checkpoint does not cover the rebuild baseline".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, RebuildRootError> {
        self.validate()?;
        let candidate = self
            .candidate
            .encode()
            .map_err(|error| RebuildRootError::Invalid(error.to_string()))?;
        let mut output = Vec::new();
        output.extend_from_slice(MAGIC);
        put_u16(&mut output, CODEC_VERSION);
        put_u16(&mut output, 0);
        put_u64(&mut output, self.index_id);
        put_u64(&mut output, self.definition_version);
        put_u64(&mut output, self.attempt_id);
        encode_barrier(&mut output, &self.baseline)?;
        match &self.last_canonical_path {
            Some(path) => {
                output.push(1);
                put_bytes(&mut output, path.as_bytes())?;
            }
            None => output.push(0),
        }
        output.push(u8::from(self.baseline_complete));
        put_u64(&mut output, self.scanned_records);
        put_u64(&mut output, self.scanned_bytes);
        put_bytes(&mut output, &candidate)?;
        if output.len() > MAX_REBUILD_ROOT_BYTES {
            return Err(RebuildRootError::SizeLimit);
        }
        Ok(output)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, RebuildRootError> {
        if bytes.len() > MAX_REBUILD_ROOT_BYTES {
            return Err(RebuildRootError::SizeLimit);
        }
        let mut input = Input::new(bytes);
        if input.take(8)? != MAGIC || input.u16()? != CODEC_VERSION || input.u16()? != 0 {
            return Err(RebuildRootError::Invalid("unsupported rebuild root".into()));
        }
        let index_id = input.u64()?;
        let definition_version = input.u64()?;
        let attempt_id = input.u64()?;
        let baseline = decode_barrier(&mut input)?;
        let last_canonical_path = match input.u8()? {
            0 => None,
            1 => Some(input.string(MAX_OBJECT_PATH_BYTES)?),
            _ => return Err(RebuildRootError::Invalid("invalid path option".into())),
        };
        let baseline_complete = match input.u8()? {
            0 => false,
            1 => true,
            _ => return Err(RebuildRootError::Invalid("invalid completion flag".into())),
        };
        let scanned_records = input.u64()?;
        let scanned_bytes = input.u64()?;
        let candidate = IndexCommitManifest::decode(input.bytes(MAX_REBUILD_ROOT_BYTES)?)
            .map_err(|error| RebuildRootError::Invalid(error.to_string()))?;
        input.finish()?;
        let value = Self {
            index_id,
            definition_version,
            attempt_id,
            baseline,
            last_canonical_path,
            baseline_complete,
            scanned_records,
            scanned_bytes,
            candidate,
        };
        value.validate()?;
        Ok(value)
    }
}

fn encode_barrier(output: &mut Vec<u8>, barrier: &IndexBarrier) -> Result<(), RebuildRootError> {
    put_u64(output, barrier.fence.term);
    put_u64(output, barrier.fence.index);
    match barrier.atomic.finalized_through() {
        Some(cursor) => {
            output.push(1);
            put_u64(output, cursor);
        }
        None => output.push(0),
    }
    put_u32(
        output,
        u32::try_from(barrier.sources.len()).map_err(|_| RebuildRootError::SizeLimit)?,
    );
    for (node, cursor) in &barrier.sources {
        put_u64(output, node.0);
        put_u16(output, cursor.source.node_id);
        output.extend_from_slice(&cursor.source.source_epoch);
        put_u64(output, cursor.next_offset);
    }
    Ok(())
}

fn decode_barrier(input: &mut Input<'_>) -> Result<IndexBarrier, RebuildRootError> {
    let fence = PlacementLogId {
        term: input.u64()?,
        index: input.u64()?,
    };
    let finalized = match input.u8()? {
        0 => None,
        1 => Some(input.u64()?),
        _ => return Err(RebuildRootError::Invalid("invalid atomic option".into())),
    };
    let count = usize::try_from(input.u32()?).map_err(|_| RebuildRootError::SizeLimit)?;
    if count > MAX_SOURCES || count > input.remaining() / 50 {
        return Err(RebuildRootError::Invalid("invalid source count".into()));
    }
    let mut sources = BTreeMap::new();
    for _ in 0..count {
        let node = NodeId(input.u64()?);
        let cursor = IndexSourceCursor {
            source: SourceId {
                node_id: input.u16()?,
                source_epoch: input.array_32()?,
            },
            next_offset: input.u64()?,
        };
        if sources.insert(node, cursor).is_some() {
            return Err(RebuildRootError::Invalid("duplicate source".into()));
        }
    }
    Ok(IndexBarrier {
        fence,
        atomic: AtomicProgramWatermark::new(finalized, finalized, 0),
        sources,
    })
}

fn put_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_le_bytes());
}
fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}
fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}
fn put_bytes(output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), RebuildRootError> {
    put_u64(
        output,
        u64::try_from(bytes.len()).map_err(|_| RebuildRootError::SizeLimit)?,
    );
    output.extend_from_slice(bytes);
    Ok(())
}

struct Input<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Input<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }
    fn take(&mut self, len: usize) -> Result<&'a [u8], RebuildRootError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(RebuildRootError::SizeLimit)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| RebuildRootError::Invalid("truncated rebuild root".into()))?;
        self.offset = end;
        Ok(value)
    }
    fn u8(&mut self) -> Result<u8, RebuildRootError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, RebuildRootError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, RebuildRootError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, RebuildRootError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn array_32(&mut self) -> Result<[u8; 32], RebuildRootError> {
        Ok(self.take(32)?.try_into().unwrap())
    }
    fn bytes(&mut self, max: usize) -> Result<&'a [u8], RebuildRootError> {
        let len = usize::try_from(self.u64()?).map_err(|_| RebuildRootError::SizeLimit)?;
        if len > max {
            return Err(RebuildRootError::SizeLimit);
        }
        self.take(len)
    }
    fn string(&mut self, max: usize) -> Result<String, RebuildRootError> {
        let bytes = self.bytes(max)?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| RebuildRootError::Invalid("rebuild path is not UTF-8".into()))
    }
    fn finish(&self) -> Result<(), RebuildRootError> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(RebuildRootError::Invalid(
                "trailing rebuild root bytes".into(),
            ))
        }
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub(crate) enum RebuildRootError {
    #[error("rebuild root exceeds its durable bound")]
    SizeLimit,
    #[error("invalid rebuild root: {0}")]
    Invalid(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use keldra_index::v4::IndexKind;

    fn barrier(next_offset: u64) -> IndexBarrier {
        IndexBarrier {
            fence: PlacementLogId { term: 2, index: 3 },
            atomic: AtomicProgramWatermark::new(Some(7), Some(7), 0),
            sources: BTreeMap::from([(
                NodeId(1),
                IndexSourceCursor {
                    source: SourceId {
                        node_id: 1,
                        source_epoch: [4; 32],
                    },
                    next_offset,
                },
            )]),
        }
    }

    fn root() -> DurableRebuildRoot {
        DurableRebuildRoot {
            index_id: 9,
            definition_version: 11,
            attempt_id: 13,
            baseline: barrier(20),
            last_canonical_path: Some("docs/b".into()),
            baseline_complete: false,
            scanned_records: 2,
            scanned_bytes: 200,
            candidate: IndexCommitManifest::new(
                9,
                13,
                11,
                IndexKind::Path,
                [5; 32],
                &barrier(20),
                Vec::new(),
                None,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                0,
                0,
            )
            .unwrap(),
        }
    }

    #[test]
    fn rebuild_root_round_trip_rejects_corruption_and_old_codec() {
        let value = root();
        let encoded = value.encode().unwrap();
        assert_eq!(DurableRebuildRoot::decode(&encoded).unwrap(), value);

        let mut old = encoded.clone();
        old[8..10].copy_from_slice(&0_u16.to_le_bytes());
        assert!(DurableRebuildRoot::decode(&old).is_err());

        let mut corrupt = encoded;
        corrupt.pop();
        assert!(DurableRebuildRoot::decode(&corrupt).is_err());
    }

    #[test]
    fn candidate_checkpoint_must_cover_original_baseline() {
        let mut value = root();
        value.candidate = IndexCommitManifest::new(
            9,
            13,
            11,
            IndexKind::Path,
            [5; 32],
            &barrier(19),
            Vec::new(),
            None,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            0,
            0,
        )
        .unwrap();
        assert!(value.validate().is_err());
    }
}
