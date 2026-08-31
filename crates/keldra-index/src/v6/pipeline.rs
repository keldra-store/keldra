use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::IndexError;

use super::{
    ProjectedDocumentState, ProjectionMutationBuffer, ProjectionPartitionIdentity,
    SealedComponentDelta,
};

/// Independently bounded memory owned by one indexing pipeline stage.
///
/// The runtime may execute stages concurrently, so each retained allocation is
/// charged to both its stage and the shared total. Refused admission is an
/// explicit instruction to discard speculative preparation and replay the
/// authoritative source journal later; it is not an indexing failure.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum IndexingMemoryStage {
    HotPayload,
    /// JSON parse, field extraction, tokenizer, and posting-worker transients.
    WorkerScratch,
    PreparedRows,
    ReplayInput,
    ProjectionAccumulator,
    SealScratch,
    OrderingCatalog,
}

impl IndexingMemoryStage {
    const COUNT: usize = 7;

    const fn index(self) -> usize {
        match self {
            Self::HotPayload => 0,
            Self::WorkerScratch => 1,
            Self::PreparedRows => 2,
            Self::ReplayInput => 3,
            Self::ProjectionAccumulator => 4,
            Self::SealScratch => 5,
            Self::OrderingCatalog => 6,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryAdmission {
    Admitted,
    ReplayRequired {
        needed_bytes: usize,
        available_bytes: usize,
    },
}

/// Exact byte credits for retained v6 pipeline allocations.
///
/// Stage limits prevent a full prepared-row queue from consuming the memory
/// required to seal and release it. The sum of stage limits may exceed the
/// total, allowing idle-stage capacity to be reused without weakening the
/// shared hard bound.
#[derive(Clone, Debug)]
pub struct IndexingMemoryCredits {
    inner: Arc<Mutex<IndexingMemoryCreditState>>,
}

#[derive(Debug)]
struct IndexingMemoryCreditState {
    total_limit_bytes: usize,
    stage_limit_bytes: [usize; IndexingMemoryStage::COUNT],
    used_bytes: usize,
    stage_used_bytes: [usize; IndexingMemoryStage::COUNT],
}

#[derive(Debug)]
pub struct IndexingMemoryPermit {
    credits: IndexingMemoryCredits,
    stage: IndexingMemoryStage,
    bytes: usize,
}

impl IndexingMemoryPermit {
    /// Exact admission held by this permit. Consumers may subdivide it, but
    /// cannot grow it without returning to `IndexingMemoryCredits`.
    pub(crate) const fn bytes(&self) -> usize {
        self.bytes
    }

    fn shrink_to(&mut self, bytes: usize) -> Result<(), IndexError> {
        if bytes > self.bytes {
            return Err(IndexError::InvalidDefinition(
                "indexing memory permit cannot grow without admission".into(),
            ));
        }
        let released = self.bytes - bytes;
        let mut state = self
            .credits
            .inner
            .lock()
            .expect("indexing memory credit lock poisoned");
        state.used_bytes -= released;
        state.stage_used_bytes[self.stage.index()] -= released;
        self.bytes = bytes;
        Ok(())
    }
}

impl Drop for IndexingMemoryPermit {
    fn drop(&mut self) {
        let mut state = self
            .credits
            .inner
            .lock()
            .expect("indexing memory credit lock poisoned");
        state.used_bytes -= self.bytes;
        state.stage_used_bytes[self.stage.index()] -= self.bytes;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexingMemoryLimits {
    pub hot_payload_bytes: usize,
    pub worker_scratch_bytes: usize,
    pub prepared_rows_bytes: usize,
    pub replay_input_bytes: usize,
    pub projection_accumulator_bytes: usize,
    pub seal_scratch_bytes: usize,
    pub ordering_catalog_bytes: usize,
}

impl IndexingMemoryLimits {
    fn as_array(self) -> [usize; IndexingMemoryStage::COUNT] {
        [
            self.hot_payload_bytes,
            self.worker_scratch_bytes,
            self.prepared_rows_bytes,
            self.replay_input_bytes,
            self.projection_accumulator_bytes,
            self.seal_scratch_bytes,
            self.ordering_catalog_bytes,
        ]
    }
}

impl IndexingMemoryCredits {
    pub fn new(total_limit_bytes: usize, limits: IndexingMemoryLimits) -> Result<Self, IndexError> {
        let stage_limit_bytes = limits.as_array();
        if total_limit_bytes == 0
            || stage_limit_bytes.contains(&0)
            || stage_limit_bytes
                .iter()
                .any(|limit| *limit > total_limit_bytes)
        {
            return Err(IndexError::InvalidDefinition(
                "indexing pipeline memory limits are invalid".into(),
            ));
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(IndexingMemoryCreditState {
                total_limit_bytes,
                stage_limit_bytes,
                used_bytes: 0,
                stage_used_bytes: [0; IndexingMemoryStage::COUNT],
            })),
        })
    }

    pub fn total_limit_bytes(&self) -> usize {
        self.inner
            .lock()
            .expect("indexing memory credit lock poisoned")
            .total_limit_bytes
    }

    pub fn used_bytes(&self) -> usize {
        self.inner
            .lock()
            .expect("indexing memory credit lock poisoned")
            .used_bytes
    }

    pub fn stage_used_bytes(&self, stage: IndexingMemoryStage) -> usize {
        self.inner
            .lock()
            .expect("indexing memory credit lock poisoned")
            .stage_used_bytes[stage.index()]
    }

    pub fn acquire(
        &self,
        stage: IndexingMemoryStage,
        bytes: usize,
    ) -> Result<IndexingMemoryPermit, MemoryAdmission> {
        let mut state = self
            .inner
            .lock()
            .expect("indexing memory credit lock poisoned");
        let stage_index = stage.index();
        let total_available = state.total_limit_bytes.saturating_sub(state.used_bytes);
        let stage_available = state.stage_limit_bytes[stage_index]
            .saturating_sub(state.stage_used_bytes[stage_index]);
        let available_bytes = total_available.min(stage_available);
        if bytes > available_bytes {
            return Err(MemoryAdmission::ReplayRequired {
                needed_bytes: bytes,
                available_bytes,
            });
        }
        state.used_bytes += bytes;
        state.stage_used_bytes[stage_index] += bytes;
        drop(state);
        Ok(IndexingMemoryPermit {
            credits: self.clone(),
            stage,
            bytes,
        })
    }

    /// Move retained bytes between stages without transiently double-charging
    /// the shared total. The source remains fully charged on failure.
    pub fn transfer(
        &self,
        permit: &mut IndexingMemoryPermit,
        destination: IndexingMemoryStage,
    ) -> Result<MemoryAdmission, IndexError> {
        if !Arc::ptr_eq(&self.inner, &permit.credits.inner) {
            return Err(IndexError::InvalidDefinition(
                "indexing memory permit belongs to another pool".into(),
            ));
        }
        let mut state = self
            .inner
            .lock()
            .expect("indexing memory credit lock poisoned");
        let source_index = permit.stage.index();
        let destination_index = destination.index();
        let available = state.stage_limit_bytes[destination_index]
            .saturating_sub(state.stage_used_bytes[destination_index]);
        if permit.bytes > available {
            return Ok(MemoryAdmission::ReplayRequired {
                needed_bytes: permit.bytes,
                available_bytes: available,
            });
        }
        state.stage_used_bytes[source_index] -= permit.bytes;
        state.stage_used_bytes[destination_index] += permit.bytes;
        permit.stage = destination;
        Ok(MemoryAdmission::Admitted)
    }
}

/// Compact, definition-neutral projection prepared while the source payload is
/// already resident on an ingest node. Full source bytes are deliberately not
/// retained here.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedProjectionRow {
    pub source_offset: u64,
    pub mutation_ordinal: u32,
    pub source_path: String,
    pub source_version: u64,
    pub projected_states: Vec<ProjectedDocumentState>,
}

impl PreparedProjectionRow {
    pub fn resident_bytes(&self) -> Result<usize, IndexError> {
        let mut bytes = std::mem::size_of::<Self>()
            .checked_add(self.source_path.capacity())
            .and_then(|bytes| {
                bytes.checked_add(
                    self.projected_states
                        .capacity()
                        .checked_mul(std::mem::size_of::<ProjectedDocumentState>())?,
                )
            })
            .ok_or(IndexError::OffsetOverflow)?;
        for state in &self.projected_states {
            bytes = bytes
                .checked_add(state.resident_bytes()?)
                .ok_or(IndexError::OffsetOverflow)?;
        }
        Ok(bytes)
    }

    fn validate(&self, source_scope: [u8; 32]) -> Result<(), IndexError> {
        if self.source_path.is_empty() || self.source_version == 0 {
            return Err(IndexError::InvalidDefinition(
                "prepared projection row has an invalid source identity".into(),
            ));
        }
        for (record, state) in self.projected_states.iter().enumerate() {
            state.validate()?;
            if state.source_scope != source_scope
                || state.head.source_path != self.source_path
                || state.head.source_version != self.source_version
                || state.head.source_record
                    != u32::try_from(record).map_err(|_| IndexError::OffsetOverflow)?
                || !state.head.live
            {
                return Err(IndexError::InvalidDefinition(
                    "prepared projection row is not one exact source version".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Evidence that the complete journal range `[first_offset, next_offset)` was
/// inspected. Relevant rows may be sparse or empty, and an atomic journal
/// record may contain several rows ordered by `mutation_ordinal`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedProjectionBatch {
    first_offset: u64,
    next_offset: u64,
    resident_bytes: usize,
    rows: Vec<PreparedProjectionRow>,
}

#[derive(Debug)]
pub struct ChargedPreparedProjectionBatch {
    batch: PreparedProjectionBatch,
    _permit: IndexingMemoryPermit,
}

#[derive(Debug)]
pub struct PreparedProjectionBatchReservation {
    permit: IndexingMemoryPermit,
}

#[derive(Debug, Eq, PartialEq)]
pub enum PreparedProjectionBatchError {
    Admission(MemoryAdmission),
    Invalid(IndexError),
}

impl PreparedProjectionBatchReservation {
    /// Reserve retained output and worker scratch before extraction invokes
    /// `build`, so refusal performs no JSON parse, token expansion, row
    /// allocation, or payload clone. Worker scratch is released as soon as
    /// extraction completes; the prepared-row reservation follows the batch.
    pub fn prepare(
        credits: &IndexingMemoryCredits,
        maximum_bytes: usize,
        maximum_worker_scratch_bytes: usize,
        source_scope: [u8; 32],
        first_offset: u64,
        next_offset: u64,
        build: impl FnOnce() -> Result<Vec<PreparedProjectionRow>, IndexError>,
    ) -> Result<ChargedPreparedProjectionBatch, PreparedProjectionBatchError> {
        let reservation = Self::reserve(credits, maximum_bytes)
            .map_err(PreparedProjectionBatchError::Admission)?;
        let worker_scratch = credits
            .acquire(
                IndexingMemoryStage::WorkerScratch,
                maximum_worker_scratch_bytes,
            )
            .map_err(PreparedProjectionBatchError::Admission)?;
        let rows = build().map_err(PreparedProjectionBatchError::Invalid)?;
        drop(worker_scratch);
        reservation
            .finish(source_scope, first_offset, next_offset, rows)
            .map_err(PreparedProjectionBatchError::Invalid)
    }

    pub fn reserve(
        credits: &IndexingMemoryCredits,
        maximum_bytes: usize,
    ) -> Result<Self, MemoryAdmission> {
        Ok(Self {
            permit: credits.acquire(IndexingMemoryStage::PreparedRows, maximum_bytes)?,
        })
    }

    pub fn finish(
        mut self,
        source_scope: [u8; 32],
        first_offset: u64,
        next_offset: u64,
        rows: Vec<PreparedProjectionRow>,
    ) -> Result<ChargedPreparedProjectionBatch, IndexError> {
        let batch = PreparedProjectionBatch::new(source_scope, first_offset, next_offset, rows)?;
        if batch.resident_bytes > self.permit.bytes {
            return Err(IndexError::ResourceLimit {
                needed: batch.resident_bytes,
                limit: self.permit.bytes,
            });
        }
        self.permit.shrink_to(batch.resident_bytes)?;
        Ok(ChargedPreparedProjectionBatch {
            batch,
            _permit: self.permit,
        })
    }
}

impl PreparedProjectionBatch {
    fn new(
        source_scope: [u8; 32],
        first_offset: u64,
        next_offset: u64,
        rows: Vec<PreparedProjectionRow>,
    ) -> Result<Self, IndexError> {
        if source_scope == [0; 32] || next_offset <= first_offset {
            return Err(IndexError::InvalidDefinition(
                "prepared projection batch has an invalid inspected range".into(),
            ));
        }
        let mut resident_bytes = std::mem::size_of::<Self>()
            .checked_add(
                rows.capacity()
                    .checked_mul(std::mem::size_of::<PreparedProjectionRow>())
                    .ok_or(IndexError::OffsetOverflow)?,
            )
            .ok_or(IndexError::OffsetOverflow)?;
        for row in &rows {
            if row.source_offset < first_offset || row.source_offset >= next_offset {
                return Err(IndexError::InvalidDefinition(
                    "prepared projection row is outside its inspected journal range".into(),
                ));
            }
            row.validate(source_scope)?;
            resident_bytes = resident_bytes
                .checked_add(row.resident_bytes()?)
                .ok_or(IndexError::OffsetOverflow)?;
        }
        if rows.windows(2).any(|pair| {
            (pair[0].source_offset, pair[0].mutation_ordinal)
                >= (pair[1].source_offset, pair[1].mutation_ordinal)
        }) {
            return Err(IndexError::InvalidDefinition(
                "prepared projection rows are not in unique mutation-unit order".into(),
            ));
        }
        Ok(Self {
            first_offset,
            next_offset,
            resident_bytes,
            rows,
        })
    }

    pub const fn first_offset(&self) -> u64 {
        self.first_offset
    }

    pub const fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub const fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }

    fn into_coalesced_rows(self) -> Vec<PreparedProjectionRow> {
        let mut rows = self.rows;
        // Move rows into path groups without cloning their owned path bytes.
        // Newest mutation first means ordinary deduplication retains exactly
        // the newest source mutation for each path.
        rows.sort_unstable_by(|left, right| {
            left.source_path.cmp(&right.source_path).then_with(|| {
                (right.source_offset, right.mutation_ordinal)
                    .cmp(&(left.source_offset, left.mutation_ordinal))
            })
        });
        rows.dedup_by(|left, right| left.source_path == right.source_path);
        rows.sort_unstable_by_key(|row| (row.source_offset, row.mutation_ordinal));
        rows
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartitionProjectionCheckpoint {
    pub partition: ProjectionPartitionIdentity,
    /// First journal offset not represented by the sealed output.
    pub next_offset: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectionBatchAdmission {
    Applied {
        source_rows: usize,
        coalesced_rows: usize,
        next_offset: u64,
    },
    ReplayRequired {
        from_offset: u64,
        needed_bytes: usize,
        available_bytes: usize,
    },
}

#[derive(Debug, Eq, PartialEq)]
pub struct SealedPartitionProjection {
    pub checkpoint: PartitionProjectionCheckpoint,
    pub deltas: Vec<SealedComponentDelta>,
}

#[derive(Debug)]
pub struct ChargedSealedPartitionProjection {
    pub projection: SealedPartitionProjection,
    _permit: IndexingMemoryPermit,
}

impl ChargedSealedPartitionProjection {
    /// Transfer sealed bytes and their exact admission together. The permit
    /// must remain live until any destination pack reservation is acquired
    /// and packing has finished.
    pub fn into_parts(self) -> (SealedPartitionProjection, IndexingMemoryPermit) {
        (self.projection, self._permit)
    }
}

/// Ordered, partition-local accumulator for prepared projection rows.
///
/// It owns no storage and advances no durable state. A runtime persists every
/// returned delta and then atomically installs `checkpoint`; until that point,
/// replay starts from the accumulator's preceding durable checkpoint.
pub struct PartitionProjectionAccumulator {
    source_scope: [u8; 32],
    partition: ProjectionPartitionIdentity,
    next_offset: u64,
    buffer_limit_bytes: usize,
    buffer: ProjectionMutationBuffer,
    credits: IndexingMemoryCredits,
    _buffer_permit: IndexingMemoryPermit,
}

impl PartitionProjectionAccumulator {
    pub fn new(
        source_scope: [u8; 32],
        partition: ProjectionPartitionIdentity,
        next_offset: u64,
        buffer_limit_bytes: usize,
        credits: IndexingMemoryCredits,
    ) -> Result<Self, IndexError> {
        partition.validate()?;
        if source_scope == [0; 32] {
            return Err(IndexError::InvalidDefinition(
                "projection accumulator has an invalid partition identity".into(),
            ));
        }
        let buffer_permit = credits
            .acquire(
                IndexingMemoryStage::ProjectionAccumulator,
                buffer_limit_bytes,
            )
            .map_err(|_| IndexError::ResourceLimit {
                needed: buffer_limit_bytes,
                limit: credits.total_limit_bytes(),
            })?;
        Ok(Self {
            source_scope,
            partition,
            next_offset,
            buffer_limit_bytes,
            buffer: ProjectionMutationBuffer::new(buffer_limit_bytes)?,
            credits,
            _buffer_permit: buffer_permit,
        })
    }

    pub const fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn buffered_bytes(&self) -> usize {
        self.buffer.used_bytes()
    }

    pub fn apply_batch(
        &mut self,
        charged: ChargedPreparedProjectionBatch,
        mut previous_by_path: BTreeMap<String, Vec<ProjectedDocumentState>>,
    ) -> Result<ProjectionBatchAdmission, IndexError> {
        let batch = charged.batch;
        if batch.first_offset != self.next_offset {
            return Err(IndexError::InvalidDefinition(
                "projection batch does not begin at the contiguous checkpoint".into(),
            ));
        }
        let source_rows = batch.rows.len();
        let next_offset = batch.next_offset;
        let rows = batch.into_coalesced_rows();
        let coalesced_rows = rows.len();
        let scratch_bytes = self
            .buffer_limit_bytes
            .checked_mul(2)
            .ok_or(IndexError::OffsetOverflow)?;
        let _scratch = match self
            .credits
            .acquire(IndexingMemoryStage::SealScratch, scratch_bytes)
        {
            Ok(permit) => permit,
            Err(MemoryAdmission::ReplayRequired {
                needed_bytes,
                available_bytes,
            }) => {
                return Ok(ProjectionBatchAdmission::ReplayRequired {
                    from_offset: self.next_offset,
                    needed_bytes,
                    available_bytes,
                });
            }
            Err(MemoryAdmission::Admitted) => unreachable!(),
        };
        let mut working = self.buffer.clone();
        let applied = rows.into_iter().try_for_each(|row| {
            let previous = previous_by_path
                .remove(&row.source_path)
                .unwrap_or_default();
            working.apply_source_states_in_place(
                self.source_scope,
                &row.source_path,
                row.source_version,
                row.projected_states,
                previous,
            )
        });
        match applied {
            Ok(()) => {
                self.buffer = working;
                self.next_offset = next_offset;
                Ok(ProjectionBatchAdmission::Applied {
                    source_rows,
                    coalesced_rows,
                    next_offset,
                })
            }
            Err(IndexError::ResourceLimit { needed, limit }) => {
                Ok(ProjectionBatchAdmission::ReplayRequired {
                    from_offset: self.next_offset,
                    needed_bytes: needed,
                    available_bytes: limit.saturating_sub(self.buffer.used_bytes()),
                })
            }
            Err(error) => Err(error),
        }
    }

    pub fn seal_and_reset(&mut self) -> Result<ChargedSealedPartitionProjection, IndexError> {
        let used = self.buffer.used_bytes();
        let permit = self
            .credits
            .acquire(IndexingMemoryStage::SealScratch, used)
            .map_err(|_| IndexError::ResourceLimit {
                needed: used,
                limit: self.credits.total_limit_bytes(),
            })?;
        let sealed = std::mem::replace(
            &mut self.buffer,
            ProjectionMutationBuffer::new(self.buffer_limit_bytes)?,
        )
        .seal()?;
        Ok(ChargedSealedPartitionProjection {
            projection: SealedPartitionProjection {
                checkpoint: PartitionProjectionCheckpoint {
                    partition: self.partition,
                    next_offset: self.next_offset,
                },
                deltas: sealed,
            },
            _permit: permit,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v6::{
        CanonicalRecipeState, ComponentIdentity, DocumentHead, RecipeIdentity,
        decode_component_delta_segment, decode_document_head,
    };

    fn state(path: &str, version: u64, value: &[u8]) -> ProjectedDocumentState {
        let scope = [9; 32];
        ProjectedDocumentState::new(
            scope,
            DocumentHead::new(scope, path.into(), 0, version, None, true).unwrap(),
            vec![
                CanonicalRecipeState::new(RecipeIdentity::new([1; 32]).unwrap(), vec![1]).unwrap(),
            ],
            vec![
                CanonicalRecipeState::new(RecipeIdentity::new([2; 32]).unwrap(), value.to_vec())
                    .unwrap(),
            ],
        )
        .unwrap()
    }

    fn row(offset: u64, path: &str, version: u64, value: &[u8]) -> PreparedProjectionRow {
        PreparedProjectionRow {
            source_offset: offset,
            mutation_ordinal: 0,
            source_path: path.into(),
            source_version: version,
            projected_states: vec![state(path, version, value)],
        }
    }

    fn credits(total: usize) -> IndexingMemoryCredits {
        IndexingMemoryCredits::new(
            total,
            IndexingMemoryLimits {
                hot_payload_bytes: total,
                worker_scratch_bytes: total,
                prepared_rows_bytes: total,
                replay_input_bytes: total,
                projection_accumulator_bytes: total,
                seal_scratch_bytes: total,
                ordering_catalog_bytes: total,
            },
        )
        .unwrap()
    }

    #[test]
    fn stage_transfer_never_transiently_overcommits_total() {
        let credits = credits(1024);
        let mut permit = credits
            .acquire(IndexingMemoryStage::HotPayload, 1024)
            .unwrap();
        assert_eq!(
            credits
                .transfer(&mut permit, IndexingMemoryStage::PreparedRows)
                .unwrap(),
            MemoryAdmission::Admitted
        );
        assert_eq!(credits.used_bytes(), 1024);
        assert_eq!(
            credits.stage_used_bytes(IndexingMemoryStage::PreparedRows),
            1024
        );
        assert!(matches!(
            credits
                .acquire(IndexingMemoryStage::SealScratch, 1)
                .unwrap_err(),
            MemoryAdmission::ReplayRequired {
                available_bytes: 0,
                ..
            }
        ));
    }

    fn partition() -> ProjectionPartitionIdentity {
        ProjectionPartitionIdentity::new([1; 32], 3, [4; 32], 3, 5, 6).unwrap()
    }

    fn charged(
        memory: &IndexingMemoryCredits,
        batch: PreparedProjectionBatch,
    ) -> ChargedPreparedProjectionBatch {
        let bytes = batch.resident_bytes;
        PreparedProjectionBatchReservation::reserve(memory, bytes)
            .unwrap()
            .finish([9; 32], batch.first_offset, batch.next_offset, batch.rows)
            .unwrap()
    }

    #[test]
    fn memory_refusal_is_an_explicit_replay_signal_and_changes_nothing() {
        let batch = PreparedProjectionBatch::new([9; 32], 7, 8, vec![row(7, "objects/a", 1, b"x")])
            .unwrap();
        let needed = batch.resident_bytes();
        let memory = IndexingMemoryCredits::new(
            4096,
            IndexingMemoryLimits {
                hot_payload_bytes: 4096,
                worker_scratch_bytes: 4096,
                prepared_rows_bytes: needed - 1,
                replay_input_bytes: 4096,
                projection_accumulator_bytes: 1024,
                seal_scratch_bytes: 2048,
                ordering_catalog_bytes: 4096,
            },
        )
        .unwrap();
        let accumulator =
            PartitionProjectionAccumulator::new([9; 32], partition(), 7, 1024, memory.clone())
                .unwrap();
        assert!(matches!(
            PreparedProjectionBatchReservation::reserve(&memory, needed).unwrap_err(),
            MemoryAdmission::ReplayRequired { .. }
        ));
        assert_eq!(accumulator.next_offset(), 7);
        assert_eq!(accumulator.buffered_bytes(), 0);
        assert_eq!(memory.used_bytes(), 1024);
        drop(accumulator);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn dropping_a_charged_batch_releases_its_exact_credits() {
        let memory = credits(16 * 1024);
        let batch =
            PreparedProjectionBatch::new([9; 32], 0, 1, vec![row(0, "objects/a", 1, b"value")])
                .unwrap();
        let bytes = batch.resident_bytes();
        let charged = charged(&memory, batch);
        assert_eq!(
            memory.stage_used_bytes(IndexingMemoryStage::PreparedRows),
            bytes
        );
        drop(charged);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn reservation_refusal_does_not_invoke_extraction_or_allocate_rows() {
        use std::cell::Cell;
        let memory = credits(1024);
        let _occupied = memory
            .acquire(IndexingMemoryStage::PreparedRows, 1024)
            .unwrap();
        let invoked = Cell::new(false);
        let result =
            PreparedProjectionBatchReservation::prepare(&memory, 1, 1, [9; 32], 0, 1, || {
                invoked.set(true);
                Ok(vec![row(0, "objects/a", 1, b"value")])
            });
        assert!(matches!(
            result,
            Err(PreparedProjectionBatchError::Admission(
                MemoryAdmission::ReplayRequired { .. }
            ))
        ));
        assert!(!invoked.get());
    }

    #[test]
    fn worker_scratch_is_admitted_before_extraction() {
        use std::cell::Cell;
        let memory = credits(4096);
        let _occupied = memory
            .acquire(IndexingMemoryStage::WorkerScratch, 4096)
            .unwrap();
        let invoked = Cell::new(false);
        let result =
            PreparedProjectionBatchReservation::prepare(&memory, 1, 1, [9; 32], 0, 1, || {
                invoked.set(true);
                Ok(vec![row(0, "objects/a", 1, b"value")])
            });
        assert!(matches!(
            result,
            Err(PreparedProjectionBatchError::Admission(
                MemoryAdmission::ReplayRequired { .. }
            ))
        ));
        assert!(!invoked.get());
        assert_eq!(
            memory.stage_used_bytes(IndexingMemoryStage::PreparedRows),
            0
        );
    }

    #[test]
    fn checkpoint_advances_only_across_a_contiguous_batch() {
        let memory = credits(256 * 1024);
        let mut accumulator = PartitionProjectionAccumulator::new(
            [9; 32],
            partition(),
            10,
            64 * 1024,
            memory.clone(),
        )
        .unwrap();
        let gap =
            PreparedProjectionBatch::new([9; 32], 11, 12, vec![row(11, "objects/a", 1, b"a")])
                .unwrap();
        assert!(
            accumulator
                .apply_batch(charged(&memory, gap), BTreeMap::new())
                .is_err()
        );
        assert_eq!(accumulator.next_offset(), 10);

        let contiguous = PreparedProjectionBatch::new(
            [9; 32],
            10,
            14,
            vec![row(10, "objects/a", 1, b"a"), row(13, "objects/b", 1, b"b")],
        )
        .unwrap();
        accumulator
            .apply_batch(charged(&memory, contiguous), BTreeMap::new())
            .unwrap();
        let sealed = accumulator.seal_and_reset().unwrap();
        assert_eq!(sealed.projection.checkpoint.next_offset, 14);
    }

    #[test]
    fn empty_relevant_page_advances_the_inspected_checkpoint() {
        let memory = credits(256 * 1024);
        let mut accumulator = PartitionProjectionAccumulator::new(
            [9; 32],
            partition(),
            20,
            64 * 1024,
            memory.clone(),
        )
        .unwrap();
        let batch = PreparedProjectionBatch::new([9; 32], 20, 36, Vec::new()).unwrap();
        assert!(matches!(
            accumulator
                .apply_batch(charged(&memory, batch), BTreeMap::new())
                .unwrap(),
            ProjectionBatchAdmission::Applied {
                source_rows: 0,
                coalesced_rows: 0,
                next_offset: 36
            }
        ));
        assert_eq!(accumulator.next_offset(), 36);
    }

    #[test]
    fn atomic_rows_share_an_offset_in_deterministic_ordinal_order() {
        let mut first = row(4, "objects/a", 1, b"a");
        first.mutation_ordinal = 0;
        let mut second = row(4, "objects/b", 1, b"b");
        second.mutation_ordinal = 1;
        let batch = PreparedProjectionBatch::new([9; 32], 4, 5, vec![first, second]).unwrap();
        assert_eq!(batch.next_offset(), 5);
        let mut reversed = row(4, "objects/c", 1, b"c");
        reversed.mutation_ordinal = 0;
        let mut duplicate = row(4, "objects/d", 1, b"d");
        duplicate.mutation_ordinal = 0;
        assert!(PreparedProjectionBatch::new([9; 32], 4, 5, vec![reversed, duplicate]).is_err());
    }

    #[test]
    fn repeated_path_coalesces_to_the_exact_newest_source_version() {
        let memory = credits(256 * 1024);
        let mut accumulator =
            PartitionProjectionAccumulator::new([9; 32], partition(), 0, 64 * 1024, memory.clone())
                .unwrap();
        let batch = PreparedProjectionBatch::new(
            [9; 32],
            0,
            3,
            vec![
                row(0, "objects/a", 4, b"same"),
                row(1, "objects/a", 5, b"same"),
                row(2, "objects/a", 6, b"same"),
            ],
        )
        .unwrap();
        assert!(matches!(
            accumulator
                .apply_batch(charged(&memory, batch), BTreeMap::new())
                .unwrap(),
            ProjectionBatchAdmission::Applied {
                source_rows: 3,
                coalesced_rows: 1,
                next_offset: 3
            }
        ));
        let sealed = accumulator.seal_and_reset().unwrap();
        let sealed = sealed.projection;
        let head = sealed
            .deltas
            .iter()
            .find(|delta| delta.component == ComponentIdentity::DocumentHead)
            .unwrap();
        let decoded = decode_component_delta_segment(&head.bytes).unwrap();
        let head = decode_document_head(
            [9; 32],
            decoded.records[0].stable_key,
            decoded.records[0].replacement.as_deref().unwrap(),
        )
        .unwrap();
        assert_eq!(head.source_version, 6);
    }

    #[test]
    fn projection_preserving_batch_writes_only_the_compact_head_delta() {
        let previous = state("objects/a", 7, b"stable");
        let memory = credits(256 * 1024);
        let mut accumulator =
            PartitionProjectionAccumulator::new([9; 32], partition(), 0, 64 * 1024, memory.clone())
                .unwrap();
        let batch =
            PreparedProjectionBatch::new([9; 32], 0, 1, vec![row(0, "objects/a", 8, b"stable")])
                .unwrap();
        accumulator
            .apply_batch(
                charged(&memory, batch),
                BTreeMap::from([("objects/a".into(), vec![previous])]),
            )
            .unwrap();
        let sealed = accumulator.seal_and_reset().unwrap();
        let sealed = sealed.projection;
        assert_eq!(
            sealed
                .deltas
                .iter()
                .map(|delta| delta.component)
                .collect::<Vec<_>>(),
            vec![ComponentIdentity::DocumentHead]
        );
        let head = &sealed.deltas[0];
        let decoded = decode_component_delta_segment(&head.bytes).unwrap();
        let head = decode_document_head(
            [9; 32],
            decoded.records[0].stable_key,
            decoded.records[0].replacement.as_deref().unwrap(),
        )
        .unwrap();
        assert_eq!(head.source_version, 8);
        assert_eq!(head.material_source_version, 7);
    }

    #[test]
    fn one_transaction_snapshot_is_taken_for_any_batch_row_count() {
        let memory = credits(512 * 1024);
        let mut accumulator = PartitionProjectionAccumulator::new(
            [9; 32],
            partition(),
            0,
            128 * 1024,
            memory.clone(),
        )
        .unwrap();
        let rows = (0..64)
            .map(|offset| row(offset, &format!("objects/{offset}"), 1, b"value"))
            .collect();
        let batch = PreparedProjectionBatch::new([9; 32], 0, 64, rows).unwrap();
        assert_eq!(accumulator.buffer.clone_count(), 0);
        accumulator
            .apply_batch(charged(&memory, batch), BTreeMap::new())
            .unwrap();
        assert_eq!(accumulator.buffer.clone_count(), 1);
    }

    #[test]
    fn sealed_output_owns_scratch_until_drop_and_accumulator_is_reusable() {
        let memory = credits(256 * 1024);
        let mut accumulator =
            PartitionProjectionAccumulator::new([9; 32], partition(), 0, 64 * 1024, memory.clone())
                .unwrap();
        let batch =
            PreparedProjectionBatch::new([9; 32], 0, 1, vec![row(0, "objects/a", 1, b"value")])
                .unwrap();
        accumulator
            .apply_batch(charged(&memory, batch), BTreeMap::new())
            .unwrap();
        let accumulator_charge = memory.used_bytes();
        let sealed = accumulator.seal_and_reset().unwrap();
        assert_eq!(sealed.projection.checkpoint.next_offset, 1);
        assert!(memory.used_bytes() > accumulator_charge);
        drop(sealed);
        assert_eq!(memory.used_bytes(), accumulator_charge);

        let empty = PreparedProjectionBatch::new([9; 32], 1, 5, Vec::new()).unwrap();
        accumulator
            .apply_batch(charged(&memory, empty), BTreeMap::new())
            .unwrap();
        assert_eq!(
            accumulator
                .seal_and_reset()
                .unwrap()
                .projection
                .checkpoint
                .next_offset,
            5
        );
    }
}
