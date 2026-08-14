//! Restart-disposable external sorting for exact retained object-version bytes.

use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};
use std::time::Instant;

use anvil_index::IndexError;
use anvil_index::v4::build::{MergeScratchFile, MergeScratchSpace};
use tonic::Status;

use crate::index_runtime::cache::{IndexMergeScratchFile, IndexMergeScratchSpace};

pub(super) const RETENTION_GENERATION_SLOTS: usize = 64;
const RECORD_BYTES: usize = 50;
const SORT_CHUNK_BYTES: usize = 16 * 1024 * 1024;
const SORT_CHUNK_RECORDS: usize = SORT_CHUNK_BYTES / RECORD_BYTES;
const CURSOR_BUFFER_BYTES: usize = 64 * 1024;
const CURSOR_BUFFER_RECORDS: usize = CURSOR_BUFFER_BYTES / RECORD_BYTES;
const MERGE_FAN_IN: usize = 32;
const MERGE_STEP_RECORDS: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RetainedObjectRecord {
    class: u8,
    digest: [u8; 32],
    version: u64,
    bytes: u64,
    generation_rank: u8,
}

impl RetainedObjectRecord {
    pub(super) fn new(
        class: u8,
        digest: [u8; 32],
        version: u64,
        bytes: u64,
        generation_rank: usize,
    ) -> Result<Self, Status> {
        if class == 0 || version == 0 || bytes == 0 || generation_rank >= RETENTION_GENERATION_SLOTS
        {
            return Err(Status::data_loss(
                "retained object-version scratch record is invalid",
            ));
        }
        Ok(Self {
            class,
            digest,
            version,
            bytes,
            generation_rank: generation_rank as u8,
        })
    }

    fn key(&self) -> (u8, [u8; 32], u64) {
        (self.class, self.digest, self.version)
    }

    fn encode(self, output: &mut Vec<u8>) {
        output.push(self.class);
        output.extend_from_slice(&self.digest);
        output.extend_from_slice(&self.version.to_be_bytes());
        output.extend_from_slice(&self.bytes.to_be_bytes());
        output.push(self.generation_rank);
    }

    fn decode(bytes: &[u8]) -> Result<Self, Status> {
        if bytes.len() != RECORD_BYTES {
            return Err(Status::data_loss(
                "retained object-version scratch record is truncated",
            ));
        }
        Self::new(
            bytes[0],
            bytes[1..33]
                .try_into()
                .map_err(|_| Status::data_loss("retention scratch digest is truncated"))?,
            u64::from_be_bytes(
                bytes[33..41]
                    .try_into()
                    .map_err(|_| Status::data_loss("retention scratch version is truncated"))?,
            ),
            u64::from_be_bytes(
                bytes[41..49]
                    .try_into()
                    .map_err(|_| Status::data_loss("retention scratch length is truncated"))?,
            ),
            usize::from(bytes[49]),
        )
    }
}

impl Ord for RetainedObjectRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key()
            .cmp(&other.key())
            .then(self.generation_rank.cmp(&other.generation_rank))
    }
}

impl PartialOrd for RetainedObjectRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(super) struct RetainedObjectCollector {
    scratch: IndexMergeScratchSpace,
    raw: IndexMergeScratchFile,
    records: u64,
}

impl RetainedObjectCollector {
    pub(super) async fn new(scratch: IndexMergeScratchSpace) -> Result<Self, Status> {
        let raw = scratch.create_file().await.map_err(scratch_status)?;
        Ok(Self {
            scratch,
            raw,
            records: 0,
        })
    }

    pub(super) async fn append(
        &mut self,
        records: impl IntoIterator<Item = RetainedObjectRecord>,
    ) -> Result<(), Status> {
        let mut bytes = Vec::new();
        let mut count = 0_u64;
        for record in records {
            record.encode(&mut bytes);
            count = count
                .checked_add(1)
                .ok_or_else(|| Status::resource_exhausted("retention record count overflowed"))?;
        }
        if bytes.is_empty() {
            return Ok(());
        }
        self.raw.append(bytes).await.map_err(scratch_status)?;
        self.records = self
            .records
            .checked_add(count)
            .ok_or_else(|| Status::resource_exhausted("retention record count overflowed"))?;
        Ok(())
    }

    pub(super) fn into_sort(self) -> RetainedObjectSort {
        RetainedObjectSort {
            scratch: self.scratch,
            phase: SortPhase::Chunks {
                raw: self.raw,
                records: self.records,
                next_record: 0,
                runs: Vec::new(),
            },
        }
    }
}

pub(super) struct RetainedObjectSort {
    scratch: IndexMergeScratchSpace,
    phase: SortPhase,
}

impl RetainedObjectSort {
    /// Advance at most one bounded sort chunk or one merge quantum.
    pub(super) async fn advance(
        &mut self,
        deadline: Instant,
    ) -> Result<Option<RetainedObjectProof>, Status> {
        loop {
            let phase = std::mem::replace(&mut self.phase, SortPhase::Complete);
            match phase {
                SortPhase::Chunks {
                    raw,
                    records,
                    mut next_record,
                    mut runs,
                } => {
                    if next_record < records {
                        let count = (records - next_record).min(SORT_CHUNK_RECORDS as u64) as usize;
                        let offset =
                            next_record
                                .checked_mul(RECORD_BYTES as u64)
                                .ok_or_else(|| {
                                    Status::resource_exhausted(
                                        "retention scratch offset overflowed",
                                    )
                                })?;
                        let encoded = raw
                            .read_exact_at(offset, count * RECORD_BYTES)
                            .await
                            .map_err(scratch_status)?;
                        let mut chunk = decode_records(&encoded)?;
                        chunk.sort_unstable();
                        canonicalize(&mut chunk)?;
                        let output = self.scratch.create_file().await.map_err(scratch_status)?;
                        output
                            .append(encode_records(&chunk))
                            .await
                            .map_err(scratch_status)?;
                        runs.push(SortedRun {
                            file: output,
                            records: chunk.len() as u64,
                        });
                        next_record += count as u64;
                        self.phase = SortPhase::Chunks {
                            raw,
                            records,
                            next_record,
                            runs,
                        };
                        return Ok(None);
                    }
                    self.phase = next_merge_phase(runs)?;
                }
                SortPhase::Merge(mut pass) => {
                    if let Some(active) = pass.active.as_mut() {
                        if let Some(run) = active.advance(deadline).await? {
                            pass.outputs.push(run);
                            pass.active = None;
                        }
                        self.phase = SortPhase::Merge(pass);
                        return Ok(None);
                    }
                    if !pass.inputs.is_empty() {
                        let take = pass.inputs.len().min(MERGE_FAN_IN);
                        let group = pass.inputs.drain(..take).collect::<Vec<_>>();
                        pass.active = Some(RunMerge::new(&self.scratch, group).await?);
                        self.phase = SortPhase::Merge(pass);
                        continue;
                    }
                    self.phase = next_merge_phase(pass.outputs)?;
                }
                SortPhase::Sum(mut sum) => {
                    if let Some(contributions) = sum.advance(deadline).await? {
                        self.phase = SortPhase::Complete;
                        return Ok(Some(RetainedObjectProof::new(
                            sum.run.clone(),
                            contributions,
                        )));
                    }
                    self.phase = SortPhase::Sum(sum);
                    return Ok(None);
                }
                SortPhase::Complete => {
                    self.phase = SortPhase::Complete;
                    return Err(Status::failed_precondition(
                        "retention byte proof was already consumed",
                    ));
                }
            }
        }
    }
}

enum SortPhase {
    Chunks {
        raw: IndexMergeScratchFile,
        records: u64,
        next_record: u64,
        runs: Vec<SortedRun>,
    },
    Merge(MergePass),
    Sum(ContributionSum),
    Complete,
}

fn next_merge_phase(runs: Vec<SortedRun>) -> Result<SortPhase, Status> {
    if runs.len() <= 1 {
        Ok(SortPhase::Sum(ContributionSum::new(runs)?))
    } else {
        Ok(SortPhase::Merge(MergePass {
            inputs: VecDeque::from(runs),
            outputs: Vec::new(),
            active: None,
        }))
    }
}

struct MergePass {
    inputs: VecDeque<SortedRun>,
    outputs: Vec<SortedRun>,
    active: Option<RunMerge>,
}

#[derive(Clone)]
struct SortedRun {
    file: IndexMergeScratchFile,
    records: u64,
}

struct RunCursor {
    run: SortedRun,
    next_record: u64,
    buffer: VecDeque<RetainedObjectRecord>,
}

impl RunCursor {
    fn new(run: SortedRun) -> Self {
        Self {
            run,
            next_record: 0,
            buffer: VecDeque::new(),
        }
    }

    async fn next(&mut self) -> Result<Option<RetainedObjectRecord>, Status> {
        if let Some(record) = self.buffer.pop_front() {
            return Ok(Some(record));
        }
        if self.next_record >= self.run.records {
            return Ok(None);
        }
        let count =
            (self.run.records - self.next_record).min(CURSOR_BUFFER_RECORDS as u64) as usize;
        let offset = self
            .next_record
            .checked_mul(RECORD_BYTES as u64)
            .ok_or_else(|| Status::resource_exhausted("retention run offset overflowed"))?;
        let bytes = self
            .run
            .file
            .read_exact_at(offset, count * RECORD_BYTES)
            .await
            .map_err(scratch_status)?;
        self.next_record += count as u64;
        self.buffer.extend(decode_records(&bytes)?);
        Ok(self.buffer.pop_front())
    }
}

struct MergeInputs {
    cursors: Vec<RunCursor>,
    heads: Vec<Option<RetainedObjectRecord>>,
}

impl MergeInputs {
    fn new(runs: Vec<SortedRun>) -> Result<Self, Status> {
        if runs.len() > MERGE_FAN_IN {
            return Err(Status::internal("retention merge fan-in exceeded"));
        }
        let cursors = runs.into_iter().map(RunCursor::new).collect::<Vec<_>>();
        let heads = vec![None; cursors.len()];
        Ok(Self { cursors, heads })
    }

    async fn pop_min(&mut self) -> Result<Option<RetainedObjectRecord>, Status> {
        for index in 0..self.cursors.len() {
            if self.heads[index].is_none() {
                self.heads[index] = self.cursors[index].next().await?;
            }
        }
        let Some(index) = self
            .heads
            .iter()
            .enumerate()
            .filter_map(|(index, value)| value.map(|value| (index, value)))
            .min_by_key(|(_, value)| *value)
            .map(|(index, _)| index)
        else {
            return Ok(None);
        };
        Ok(self.heads[index].take())
    }
}

struct RunMerge {
    inputs: MergeInputs,
    output: IndexMergeScratchFile,
    output_records: u64,
    output_buffer: Vec<u8>,
    pending: Option<RetainedObjectRecord>,
}

impl RunMerge {
    async fn new(scratch: &IndexMergeScratchSpace, runs: Vec<SortedRun>) -> Result<Self, Status> {
        Ok(Self {
            inputs: MergeInputs::new(runs)?,
            output: scratch.create_file().await.map_err(scratch_status)?,
            output_records: 0,
            output_buffer: Vec::with_capacity(CURSOR_BUFFER_BYTES),
            pending: None,
        })
    }

    async fn advance(&mut self, deadline: Instant) -> Result<Option<SortedRun>, Status> {
        let mut processed = 0_usize;
        while processed < MERGE_STEP_RECORDS && Instant::now() < deadline {
            let Some(record) = self.inputs.pop_min().await? else {
                if let Some(pending) = self.pending.take() {
                    self.emit(pending)?;
                }
                self.flush().await?;
                return Ok(Some(SortedRun {
                    file: self.output.clone(),
                    records: self.output_records,
                }));
            };
            let mut completed = None;
            merge_pending(&mut self.pending, record, |value| {
                completed = Some(value);
                Ok(())
            })?;
            if let Some(completed) = completed {
                self.emit(completed)?;
            }
            processed += 1;
            if self.output_buffer.len() >= CURSOR_BUFFER_BYTES {
                self.flush().await?;
            }
        }
        self.flush().await?;
        Ok(None)
    }

    fn emit(&mut self, record: RetainedObjectRecord) -> Result<(), Status> {
        record.encode(&mut self.output_buffer);
        self.output_records = self
            .output_records
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("retention run count overflowed"))?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Status> {
        if !self.output_buffer.is_empty() {
            self.output
                .append(std::mem::take(&mut self.output_buffer))
                .await
                .map_err(scratch_status)?;
        }
        Ok(())
    }
}

struct ContributionSum {
    run: SortedRun,
    inputs: MergeInputs,
    pending: Option<RetainedObjectRecord>,
    contributions: [u64; RETENTION_GENERATION_SLOTS],
}

impl ContributionSum {
    fn new(runs: Vec<SortedRun>) -> Result<Self, Status> {
        let [run] = runs.as_slice() else {
            return Err(Status::data_loss(
                "retention byte proof does not contain one canonical run",
            ));
        };
        let run = run.clone();
        Ok(Self {
            run,
            inputs: MergeInputs::new(runs)?,
            pending: None,
            contributions: [0; RETENTION_GENERATION_SLOTS],
        })
    }

    async fn advance(
        &mut self,
        deadline: Instant,
    ) -> Result<Option<[u64; RETENTION_GENERATION_SLOTS]>, Status> {
        let mut processed = 0_usize;
        while processed < MERGE_STEP_RECORDS && Instant::now() < deadline {
            let Some(record) = self.inputs.pop_min().await? else {
                if let Some(pending) = self.pending.take() {
                    add_contribution(&mut self.contributions, pending)?;
                }
                return Ok(Some(self.contributions));
            };
            let mut completed = None;
            merge_pending(&mut self.pending, record, |value| {
                completed = Some(value);
                Ok(())
            })?;
            if let Some(completed) = completed {
                add_contribution(&mut self.contributions, completed)?;
            }
            processed += 1;
        }
        Ok(None)
    }
}

pub(super) struct RetainedObjectProof {
    run: SortedRun,
    contributions: [u64; RETENTION_GENERATION_SLOTS],
    cache: BTreeMap<u64, Vec<RetainedObjectRecord>>,
    cache_order: VecDeque<u64>,
}

impl RetainedObjectProof {
    fn new(run: SortedRun, contributions: [u64; RETENTION_GENERATION_SLOTS]) -> Self {
        Self {
            run,
            contributions,
            cache: BTreeMap::new(),
            cache_order: VecDeque::new(),
        }
    }

    pub(super) fn contributions(&self) -> &[u64; RETENTION_GENERATION_SLOTS] {
        &self.contributions
    }

    pub(super) async fn lookup(
        &mut self,
        class: u8,
        digest: [u8; 32],
        version: u64,
    ) -> Result<Option<(u64, usize)>, Status> {
        let key = (class, digest, version);
        let mut low = 0_u64;
        let mut high = self.run.records;
        while low < high {
            let middle = low + (high - low) / 2;
            let record = self.record_at(middle).await?;
            match record.key().cmp(&key) {
                Ordering::Less => low = middle + 1,
                Ordering::Greater => high = middle,
                Ordering::Equal => {
                    return Ok(Some((record.bytes, usize::from(record.generation_rank))));
                }
            }
        }
        Ok(None)
    }

    async fn record_at(&mut self, record: u64) -> Result<RetainedObjectRecord, Status> {
        const CACHE_BLOCKS: usize = 64;
        let block = record / CURSOR_BUFFER_RECORDS as u64;
        if !self.cache.contains_key(&block) {
            let first = block * CURSOR_BUFFER_RECORDS as u64;
            let count = (self.run.records - first).min(CURSOR_BUFFER_RECORDS as u64) as usize;
            let offset = first
                .checked_mul(RECORD_BYTES as u64)
                .ok_or_else(|| Status::resource_exhausted("retention proof offset overflowed"))?;
            let bytes = self
                .run
                .file
                .read_exact_at(offset, count * RECORD_BYTES)
                .await
                .map_err(scratch_status)?;
            if self.cache.len() >= CACHE_BLOCKS
                && let Some(oldest) = self.cache_order.pop_front()
            {
                self.cache.remove(&oldest);
            }
            self.cache.insert(block, decode_records(&bytes)?);
            self.cache_order.push_back(block);
        }
        let within = usize::try_from(record % CURSOR_BUFFER_RECORDS as u64)
            .map_err(|_| Status::resource_exhausted("retention proof index exceeds platform"))?;
        self.cache
            .get(&block)
            .and_then(|records| records.get(within))
            .copied()
            .ok_or_else(|| Status::data_loss("retention proof block is truncated"))
    }
}

fn merge_pending(
    pending: &mut Option<RetainedObjectRecord>,
    record: RetainedObjectRecord,
    mut complete: impl FnMut(RetainedObjectRecord) -> Result<(), Status>,
) -> Result<(), Status> {
    match pending.as_mut() {
        Some(current) if current.key() == record.key() => {
            if current.bytes != record.bytes {
                return Err(Status::data_loss(
                    "one retained ordinary object version has conflicting lengths",
                ));
            }
            current.generation_rank = current.generation_rank.min(record.generation_rank);
        }
        Some(_) => {
            let previous = pending.replace(record).expect("pending record exists");
            complete(previous)?;
        }
        None => *pending = Some(record),
    }
    Ok(())
}

fn add_contribution(
    contributions: &mut [u64; RETENTION_GENERATION_SLOTS],
    record: RetainedObjectRecord,
) -> Result<(), Status> {
    let slot = usize::from(record.generation_rank);
    contributions[slot] = contributions[slot]
        .checked_add(record.bytes)
        .ok_or_else(|| Status::resource_exhausted("retained generation bytes overflowed"))?;
    Ok(())
}

fn canonicalize(records: &mut Vec<RetainedObjectRecord>) -> Result<(), Status> {
    let input = std::mem::take(records);
    let mut output = Vec::with_capacity(input.len());
    let mut pending = None;
    for record in input {
        merge_pending(&mut pending, record, |value| {
            output.push(value);
            Ok(())
        })?;
    }
    if let Some(pending) = pending {
        output.push(pending);
    }
    *records = output;
    Ok(())
}

fn decode_records(bytes: &[u8]) -> Result<Vec<RetainedObjectRecord>, Status> {
    if bytes.len() % RECORD_BYTES != 0 {
        return Err(Status::data_loss(
            "retention scratch file ends inside one record",
        ));
    }
    bytes
        .chunks_exact(RECORD_BYTES)
        .map(RetainedObjectRecord::decode)
        .collect()
}

fn encode_records(records: &[RetainedObjectRecord]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(records.len() * RECORD_BYTES);
    for record in records {
        record.encode(&mut bytes);
    }
    bytes
}

fn scratch_status(error: IndexError) -> Status {
    match error {
        IndexError::UnexpectedEof { .. } | IndexError::Integrity => {
            Status::data_loss(error.to_string())
        }
        IndexError::ResourceLimit { .. } | IndexError::OffsetOverflow => {
            Status::resource_exhausted(error.to_string())
        }
        _ => Status::unavailable(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(digest: u8, version: u64, bytes: u64, rank: usize) -> RetainedObjectRecord {
        RetainedObjectRecord::new(2, [digest; 32], version, bytes, rank).unwrap()
    }

    #[test]
    fn chunk_canonicalization_counts_one_object_version_at_its_newest_rank() {
        let mut records = vec![
            record(2, 9, 40, 3),
            record(1, 8, 20, 2),
            record(2, 9, 40, 1),
            record(1, 8, 20, 4),
        ];
        records.sort_unstable();
        canonicalize(&mut records).unwrap();
        assert_eq!(records, vec![record(1, 8, 20, 2), record(2, 9, 40, 1)]);
    }

    #[test]
    fn conflicting_lengths_for_one_exact_object_version_fail_closed() {
        let mut records = vec![record(1, 8, 20, 1), record(1, 8, 21, 2)];
        records.sort_unstable();
        assert_eq!(
            canonicalize(&mut records).unwrap_err().code(),
            tonic::Code::DataLoss
        );
    }

    #[test]
    fn scratch_records_round_trip_without_platform_endianness() {
        let record = record(7, 0x0102_0304_0506_0708, 0x1112_1314_1516_1718, 5);
        let encoded = encode_records(&[record]);
        assert_eq!(encoded.len(), RECORD_BYTES);
        assert_eq!(decode_records(&encoded).unwrap(), vec![record]);
    }
}
