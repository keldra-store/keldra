//! Durable sparse real-time overlay metadata.
//!
//! Overlay generations keep exact sparse visibility evidence separate from
//! immutable query artifacts. One artifact may cover a microbatch of positions;
//! its document gates still carry each record's exact sparse source position.

use crate::IndexError;

use super::{PreparedQueryMutationBatch, ProjectionPartitionIdentity, QueryRunReference};

const GENERATION_MAGIC: &[u8; 8] = b"K1RGEN01";
const CURRENT_MAGIC: &[u8; 8] = b"K1RCUR01";
const FORMAT: u16 = 1;
pub const MAX_REALTIME_OVERLAY_EVIDENCE: usize = 16_384;
pub const MAX_REALTIME_OVERLAY_RUNS: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RealtimeOverlayEvidence {
    pub source_position: u64,
    pub atomic_position: u64,
    /// Digest of the complete atomic publication membership. Non-atomic
    /// mutations use their exact source-event identity digest.
    pub atomic_unit_hash: Option<[u8; 32]>,
}

impl RealtimeOverlayEvidence {
    pub fn is_absorbed_by(self, base_next_offset: u64, base_atomic_position: u64) -> bool {
        self.source_position < base_next_offset && self.atomic_position <= base_atomic_position
    }

    fn validate(self) -> Result<(), IndexError> {
        if self.atomic_unit_hash == Some([0; 32])
            || (self.atomic_position == 0) != self.atomic_unit_hash.is_none()
        {
            return Err(IndexError::InvalidDefinition(
                "real-time overlay evidence does not bind one exact source event".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RealtimeOverlayRun {
    /// Exact sparse positions represented by this query artifact. These are
    /// independent of the descriptor's bounding source interval.
    pub source_positions: Vec<u64>,
    pub query_run: QueryRunReference,
}

impl RealtimeOverlayRun {
    fn validate(&self) -> Result<(), IndexError> {
        if self.source_positions.is_empty()
            || self.source_positions.len() > MAX_REALTIME_OVERLAY_EVIDENCE
            || self
                .source_positions
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || self.query_run.hash == [0; 32]
            || self.query_run.encoded_bytes == 0
            || self.query_run.level != 0
            || self.query_run.source_start_offset != self.source_positions[0]
            || self.query_run.next_offset
                != self
                    .source_positions
                    .last()
                    .copied()
                    .unwrap()
                    .saturating_add(1)
        {
            return Err(IndexError::InvalidDefinition(
                "real-time overlay query artifact is invalid".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RealtimeOverlayGeneration {
    pub partition: ProjectionPartitionIdentity,
    pub physical_catalog_generation: [u8; 32],
    pub revision: u64,
    pub evidence: Vec<RealtimeOverlayEvidence>,
    pub runs: Vec<RealtimeOverlayRun>,
    pub previous_generation_hash: Option<[u8; 32]>,
}

impl RealtimeOverlayGeneration {
    pub fn validate(&self) -> Result<(), IndexError> {
        self.partition.validate()?;
        if self.physical_catalog_generation == [0; 32]
            || self.revision == 0
            || self.evidence.len() > MAX_REALTIME_OVERLAY_EVIDENCE
            || self.runs.len() > MAX_REALTIME_OVERLAY_RUNS
            || self.previous_generation_hash == Some([0; 32])
            || self.evidence.windows(2).any(|pair| {
                (
                    pair[0].source_position,
                    pair[0].atomic_position,
                    pair[0].atomic_unit_hash,
                ) >= (
                    pair[1].source_position,
                    pair[1].atomic_position,
                    pair[1].atomic_unit_hash,
                )
            })
        {
            return Err(IndexError::InvalidDefinition(
                "real-time overlay generation is invalid or non-canonical".into(),
            ));
        }
        for evidence in &self.evidence {
            evidence.validate()?;
        }
        for run in &self.runs {
            run.validate()?;
            if run.source_positions.iter().any(|position| {
                self.evidence
                    .binary_search_by_key(position, |evidence| evidence.source_position)
                    .is_err()
            }) {
                return Err(IndexError::InvalidDefinition(
                    "real-time overlay artifact lacks sparse evidence".into(),
                ));
            }
        }
        Ok(())
    }

    pub fn unabsorbed_evidence(
        &self,
        base_next_offset: u64,
        base_atomic_position: u64,
    ) -> impl Iterator<Item = RealtimeOverlayEvidence> + '_ {
        let retained_by_partial_run = self
            .runs
            .iter()
            .filter(|run| {
                run.source_positions.iter().any(|position| {
                    self.evidence
                        .binary_search_by_key(position, |entry| entry.source_position)
                        .is_ok_and(|index| {
                            !self.evidence[index]
                                .is_absorbed_by(base_next_offset, base_atomic_position)
                        })
                })
            })
            .flat_map(|run| run.source_positions.iter().copied())
            .collect::<std::collections::BTreeSet<_>>();
        self.evidence.iter().copied().filter(move |evidence| {
            !evidence.is_absorbed_by(base_next_offset, base_atomic_position)
                || retained_by_partial_run.contains(&evidence.source_position)
        })
    }

    pub fn unabsorbed_runs(
        &self,
        base_next_offset: u64,
        base_atomic_position: u64,
    ) -> impl Iterator<Item = RealtimeOverlayRun> + '_ {
        self.runs
            .iter()
            .filter(move |run| {
                run.source_positions.iter().any(|position| {
                    self.evidence
                        .binary_search_by_key(position, |evidence| evidence.source_position)
                        .is_ok_and(|index| {
                            !self.evidence[index]
                                .is_absorbed_by(base_next_offset, base_atomic_position)
                        })
                })
            })
            .cloned()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RealtimeOverlayCurrent {
    pub partition: ProjectionPartitionIdentity,
    pub physical_catalog_generation: [u8; 32],
    pub generation_hash: [u8; 32],
    pub generation_revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedRealtimeOverlayGeneration {
    pub hash: [u8; 32],
    pub bytes: Vec<u8>,
}

/// Bind every liveness record in one prepared event to its exact sparse
/// source position before sealing it as an overlay run.
pub fn bind_realtime_source_position(
    batch: &mut PreparedQueryMutationBatch,
    source_position: u64,
) -> Result<(), IndexError> {
    let mut gates = batch
        .membership
        .iter_mut()
        .flat_map(|membership| membership.gates.iter_mut())
        .chain(
            batch
                .fields
                .iter_mut()
                .map(|field| &mut field.delta.presence),
        );
    for gate in &mut gates {
        if gate
            .selective_source_position
            .replace(source_position)
            .is_some()
        {
            return Err(IndexError::InvalidDefinition(
                "prepared query mutation already has sparse source evidence".into(),
            ));
        }
    }
    Ok(())
}

pub fn encode_realtime_overlay_generation(
    generation: &RealtimeOverlayGeneration,
) -> Result<EncodedRealtimeOverlayGeneration, IndexError> {
    generation.validate()?;
    let mut out = Vec::new();
    out.extend_from_slice(GENERATION_MAGIC);
    put_u16(&mut out, FORMAT);
    put_partition(&mut out, generation.partition);
    out.extend_from_slice(&generation.physical_catalog_generation);
    put_u64(&mut out, generation.revision);
    put_u32(
        &mut out,
        u32::try_from(generation.evidence.len()).map_err(|_| IndexError::OffsetOverflow)?,
    );
    for evidence in &generation.evidence {
        put_evidence(&mut out, *evidence);
    }
    put_u32(
        &mut out,
        u32::try_from(generation.runs.len()).map_err(|_| IndexError::OffsetOverflow)?,
    );
    for run in &generation.runs {
        put_run(&mut out, run);
    }
    put_optional_hash(&mut out, generation.previous_generation_hash);
    Ok(EncodedRealtimeOverlayGeneration {
        hash: *crate::profiled_blake3_hash!(&out).as_bytes(),
        bytes: out,
    })
}

pub fn decode_realtime_overlay_generation(
    bytes: &[u8],
) -> Result<RealtimeOverlayGeneration, IndexError> {
    let mut decoder = Decoder::new(bytes);
    decoder.expect(GENERATION_MAGIC)?;
    if decoder.u16()? != FORMAT {
        return Err(IndexError::InvalidFormat(
            "real-time overlay generation format",
        ));
    }
    let partition = decoder.partition()?;
    let physical_catalog_generation = decoder.array_32()?;
    let revision = decoder.u64()?;
    let evidence_count = decoder.u32()? as usize;
    if evidence_count > MAX_REALTIME_OVERLAY_EVIDENCE {
        return Err(IndexError::InvalidFormat(
            "real-time overlay evidence count",
        ));
    }
    let mut evidence = Vec::with_capacity(evidence_count);
    for _ in 0..evidence_count {
        evidence.push(decoder.evidence()?);
    }
    let count = decoder.u32()? as usize;
    if count > MAX_REALTIME_OVERLAY_RUNS {
        return Err(IndexError::InvalidFormat("real-time overlay run count"));
    }
    let mut runs = Vec::with_capacity(count);
    for _ in 0..count {
        runs.push(decoder.run()?);
    }
    let previous_generation_hash = decoder.optional_hash()?;
    decoder.finish()?;
    let generation = RealtimeOverlayGeneration {
        partition,
        physical_catalog_generation,
        revision,
        evidence,
        runs,
        previous_generation_hash,
    };
    generation.validate()?;
    Ok(generation)
}

pub fn encode_realtime_overlay_current(
    current: RealtimeOverlayCurrent,
) -> Result<Vec<u8>, IndexError> {
    current.partition.validate()?;
    if current.physical_catalog_generation == [0; 32]
        || current.generation_hash == [0; 32]
        || current.generation_revision == 0
    {
        return Err(IndexError::InvalidDefinition(
            "real-time overlay current is invalid".into(),
        ));
    }
    let mut out = Vec::new();
    out.extend_from_slice(CURRENT_MAGIC);
    put_u16(&mut out, FORMAT);
    put_partition(&mut out, current.partition);
    out.extend_from_slice(&current.physical_catalog_generation);
    out.extend_from_slice(&current.generation_hash);
    put_u64(&mut out, current.generation_revision);
    Ok(out)
}

pub fn decode_realtime_overlay_current(bytes: &[u8]) -> Result<RealtimeOverlayCurrent, IndexError> {
    let mut decoder = Decoder::new(bytes);
    decoder.expect(CURRENT_MAGIC)?;
    if decoder.u16()? != FORMAT {
        return Err(IndexError::InvalidFormat(
            "real-time overlay current format",
        ));
    }
    let current = RealtimeOverlayCurrent {
        partition: decoder.partition()?,
        physical_catalog_generation: decoder.array_32()?,
        generation_hash: decoder.array_32()?,
        generation_revision: decoder.u64()?,
    };
    decoder.finish()?;
    encode_realtime_overlay_current(current)?;
    Ok(current)
}

fn put_evidence(out: &mut Vec<u8>, evidence: RealtimeOverlayEvidence) {
    put_u64(out, evidence.source_position);
    put_u64(out, evidence.atomic_position);
    put_optional_hash(out, evidence.atomic_unit_hash);
}

fn put_run(out: &mut Vec<u8>, run: &RealtimeOverlayRun) {
    put_u32(out, run.source_positions.len() as u32);
    for position in &run.source_positions {
        put_u64(out, *position);
    }
    let query_run = run.query_run;
    out.extend_from_slice(&query_run.hash);
    put_u64(out, query_run.encoded_bytes);
    put_u64(out, query_run.sequence);
    out.push(query_run.level);
    put_u64(out, query_run.source_start_offset);
    put_u64(out, query_run.next_offset);
    put_u64(out, query_run.through_atomic_position);
}

fn put_partition(out: &mut Vec<u8>, value: ProjectionPartitionIdentity) {
    out.extend_from_slice(&value.family_id);
    put_u64(out, value.source_node);
    out.extend_from_slice(&value.source_epoch);
    put_u64(out, value.producer_node);
    put_u64(out, value.placement_term);
    put_u64(out, value.placement_index);
}

fn put_optional_hash(out: &mut Vec<u8>, value: Option<[u8; 32]>) {
    match value {
        Some(hash) => {
            out.push(1);
            out.extend_from_slice(&hash);
        }
        None => out.push(0),
    }
}
fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}
fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}
fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

struct Decoder<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }
    fn take(&mut self, length: usize) -> Result<&'a [u8], IndexError> {
        let end = self
            .at
            .checked_add(length)
            .ok_or(IndexError::OffsetOverflow)?;
        let value = self
            .bytes
            .get(self.at..end)
            .ok_or(IndexError::InvalidFormat("real-time overlay metadata"))?;
        self.at = end;
        Ok(value)
    }
    fn expect(&mut self, expected: &[u8]) -> Result<(), IndexError> {
        if self.take(expected.len())? == expected {
            Ok(())
        } else {
            Err(IndexError::InvalidFormat("real-time overlay magic"))
        }
    }
    fn byte(&mut self) -> Result<u8, IndexError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, IndexError> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, IndexError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, IndexError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn array_32(&mut self) -> Result<[u8; 32], IndexError> {
        Ok(self.take(32)?.try_into().unwrap())
    }
    fn partition(&mut self) -> Result<ProjectionPartitionIdentity, IndexError> {
        ProjectionPartitionIdentity::new(
            self.array_32()?,
            self.u64()?,
            self.array_32()?,
            self.u64()?,
            self.u64()?,
            self.u64()?,
        )
    }
    fn optional_hash(&mut self) -> Result<Option<[u8; 32]>, IndexError> {
        match self.byte()? {
            0 => Ok(None),
            1 => Ok(Some(self.array_32()?)),
            _ => Err(IndexError::InvalidFormat("real-time overlay predecessor")),
        }
    }
    fn evidence(&mut self) -> Result<RealtimeOverlayEvidence, IndexError> {
        Ok(RealtimeOverlayEvidence {
            source_position: self.u64()?,
            atomic_position: self.u64()?,
            atomic_unit_hash: self.optional_hash()?,
        })
    }
    fn run(&mut self) -> Result<RealtimeOverlayRun, IndexError> {
        let count = self.u32()? as usize;
        if count == 0 || count > MAX_REALTIME_OVERLAY_EVIDENCE {
            return Err(IndexError::InvalidFormat(
                "real-time overlay artifact positions",
            ));
        }
        let mut source_positions = Vec::with_capacity(count);
        for _ in 0..count {
            source_positions.push(self.u64()?);
        }
        Ok(RealtimeOverlayRun {
            source_positions,
            query_run: QueryRunReference {
                hash: self.array_32()?,
                encoded_bytes: self.u64()?,
                sequence: self.u64()?,
                level: self.byte()?,
                source_start_offset: self.u64()?,
                next_offset: self.u64()?,
                through_atomic_position: self.u64()?,
            },
        })
    }
    fn finish(self) -> Result<(), IndexError> {
        if self.at == self.bytes.len() {
            Ok(())
        } else {
            Err(IndexError::InvalidFormat(
                "real-time overlay trailing bytes",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partition() -> ProjectionPartitionIdentity {
        ProjectionPartitionIdentity::new([1; 32], 2, [3; 32], 4, 5, 6).unwrap()
    }
    fn evidence(position: u64) -> RealtimeOverlayEvidence {
        RealtimeOverlayEvidence {
            source_position: position,
            atomic_position: position + 10,
            atomic_unit_hash: Some([99; 32]),
        }
    }
    fn run(positions: &[u64]) -> RealtimeOverlayRun {
        RealtimeOverlayRun {
            source_positions: positions.to_vec(),
            query_run: QueryRunReference {
                hash: [positions[0] as u8; 32],
                encoded_bytes: 99,
                sequence: positions.last().copied().unwrap() + 1,
                level: 0,
                source_start_offset: positions[0],
                next_offset: positions.last().copied().unwrap() + 1,
                through_atomic_position: positions.last().copied().unwrap() + 10,
            },
        }
    }

    #[test]
    fn sparse_generation_round_trips_without_claiming_a_prefix() {
        let generation = RealtimeOverlayGeneration {
            partition: partition(),
            physical_catalog_generation: [8; 32],
            revision: 3,
            evidence: vec![evidence(4), evidence(19)],
            runs: vec![run(&[4, 19])],
            previous_generation_hash: Some([7; 32]),
        };
        let encoded = encode_realtime_overlay_generation(&generation).unwrap();
        assert_eq!(
            decode_realtime_overlay_generation(&encoded.bytes).unwrap(),
            generation
        );
        assert_eq!(
            generation.unabsorbed_evidence(10, 100).collect::<Vec<_>>(),
            vec![evidence(4), evidence(19)]
        );
        let retained = RealtimeOverlayGeneration {
            evidence: generation.unabsorbed_evidence(10, 100).collect(),
            runs: generation.unabsorbed_runs(10, 100).collect(),
            ..generation.clone()
        };
        retained.validate().unwrap();
    }

    #[test]
    fn absorption_requires_both_source_and_atomic_coverage() {
        assert!(!evidence(4).is_absorbed_by(5, 13));
        assert!(evidence(4).is_absorbed_by(5, 14));
    }

    #[test]
    fn no_match_is_durable_sparse_visibility_evidence() {
        let no_match = evidence(7);
        no_match.validate().unwrap();
        let generation = RealtimeOverlayGeneration {
            partition: partition(),
            physical_catalog_generation: [8; 32],
            revision: 1,
            evidence: vec![no_match],
            runs: vec![],
            previous_generation_hash: None,
        };
        let encoded = encode_realtime_overlay_generation(&generation).unwrap();
        assert_eq!(
            decode_realtime_overlay_generation(&encoded.bytes).unwrap(),
            generation
        );
    }

    #[test]
    fn more_than_4096_positions_share_one_microbatch_artifact() {
        let evidence = (1..=5_000).map(evidence).collect::<Vec<_>>();
        let positions = evidence
            .iter()
            .map(|entry| entry.source_position)
            .collect::<Vec<_>>();
        let generation = RealtimeOverlayGeneration {
            partition: partition(),
            physical_catalog_generation: [8; 32],
            revision: 1,
            evidence,
            runs: vec![run(&positions)],
            previous_generation_hash: None,
        };
        let encoded = encode_realtime_overlay_generation(&generation).unwrap();
        let decoded = decode_realtime_overlay_generation(&encoded.bytes).unwrap();
        assert_eq!(decoded.evidence.len(), 5_000);
        assert_eq!(decoded.runs.len(), 1);
    }
}
