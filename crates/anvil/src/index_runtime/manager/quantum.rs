//! Fair byte quanta shared by snapshot rebuild and journal catch-up work.

use tonic::Status;

use super::source_wire_limit;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SourceWorkBoundary {
    Continue,
    SealAndYield,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SourceWorkQuantum {
    pub(super) limit: u64,
    consumed: u64,
}

impl SourceWorkQuantum {
    /// Derive one source-wire quantum from a resident-memory budget.
    pub(super) fn from_budget_limit(budget_limit: u64) -> Self {
        Self::from_wire_limit(source_wire_limit(budget_limit))
    }

    /// Use a source-wire limit that the snapshot scanner already derived.
    pub(super) const fn from_wire_limit(wire_limit: u64) -> Self {
        Self {
            limit: wire_limit,
            consumed: 0,
        }
    }

    /// Journal pulls accept a changing byte credit, so they never cross the
    /// quantum before yielding.
    pub(super) fn remaining(self) -> Option<u64> {
        self.limit
            .checked_sub(self.consumed)
            .filter(|bytes| *bytes > 0)
    }

    pub(super) fn advance_page(&mut self, bytes: u64) -> Result<SourceWorkBoundary, Status> {
        self.consumed = self
            .consumed
            .checked_add(bytes)
            .filter(|consumed| *consumed <= self.limit)
            .ok_or_else(|| Status::data_loss("index journal page exceeded its work quantum"))?;
        Ok(self.boundary())
    }

    /// If the next complete change fits an empty quantum but not its remaining
    /// credit, seal this partial builder and retry that change after yielding.
    pub(super) fn defer_page_to_next_quantum(self, required_bytes: u64) -> bool {
        self.consumed > 0 && required_bytes <= self.limit
    }

    /// Snapshot frame credit is fixed when the stream opens. The caller yields
    /// immediately after crossing the limit, so prior work below one quantum
    /// plus one bounded frame is strictly below two quanta (32 MiB at the
    /// default maximum).
    pub(super) fn advance_frame(&mut self, bytes: u64) -> Result<SourceWorkBoundary, Status> {
        if bytes > self.limit {
            return Err(Status::data_loss(
                "index snapshot frame exceeded its configured wire limit",
            ));
        }
        self.consumed = self
            .consumed
            .checked_add(bytes)
            .ok_or_else(|| Status::resource_exhausted("index source work quantum overflow"))?;
        Ok(self.boundary())
    }

    const fn boundary(self) -> SourceWorkBoundary {
        if self.consumed >= self.limit {
            SourceWorkBoundary::SealAndYield
        } else {
            SourceWorkBoundary::Continue
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_runtime::manager::MAX_SOURCE_WIRE_BYTES;

    const KIND_BUDGET: u64 = 256 * 1024 * 1024;

    #[test]
    fn sequential_journal_pages_share_one_builder_quantum() {
        let mut quantum = SourceWorkQuantum::from_budget_limit(KIND_BUDGET);
        assert_eq!(quantum.limit, MAX_SOURCE_WIRE_BYTES);

        for _ in 0..31 {
            assert_eq!(
                quantum.advance_page(512 * 1024).unwrap(),
                SourceWorkBoundary::Continue
            );
        }
        assert_eq!(quantum.remaining(), Some(512 * 1024));
        assert_eq!(
            quantum.advance_page(512 * 1024).unwrap(),
            SourceWorkBoundary::SealAndYield
        );
        assert_eq!(quantum.remaining(), None);
    }

    #[test]
    fn journal_page_cannot_cross_the_exact_yield_boundary() {
        let mut quantum = SourceWorkQuantum::from_budget_limit(KIND_BUDGET);
        assert_eq!(
            quantum.advance_page(MAX_SOURCE_WIRE_BYTES - 1).unwrap(),
            SourceWorkBoundary::Continue
        );

        let error = quantum.advance_page(2).unwrap_err();
        assert_eq!(error.code(), tonic::Code::DataLoss);
    }

    #[test]
    fn a_complete_change_larger_than_remaining_credit_starts_the_next_quantum() {
        let mut quantum = SourceWorkQuantum::from_budget_limit(KIND_BUDGET);
        quantum.advance_page(MAX_SOURCE_WIRE_BYTES - 1).unwrap();

        assert!(quantum.defer_page_to_next_quantum(2));
        assert!(!quantum.defer_page_to_next_quantum(MAX_SOURCE_WIRE_BYTES + 1));
        assert!(!SourceWorkQuantum::from_budget_limit(KIND_BUDGET).defer_page_to_next_quantum(2));
    }

    #[test]
    fn snapshot_frames_yield_after_one_bounded_overshoot() {
        let mut quantum = SourceWorkQuantum::from_budget_limit(KIND_BUDGET);
        assert_eq!(
            quantum.advance_frame(MAX_SOURCE_WIRE_BYTES - 1).unwrap(),
            SourceWorkBoundary::Continue
        );

        assert_eq!(
            quantum.advance_frame(MAX_SOURCE_WIRE_BYTES).unwrap(),
            SourceWorkBoundary::SealAndYield
        );
        assert!(quantum.consumed < MAX_SOURCE_WIRE_BYTES * 2);
    }

    #[test]
    fn snapshot_frame_cannot_exceed_its_stream_bound() {
        let mut quantum = SourceWorkQuantum::from_budget_limit(KIND_BUDGET);
        let error = quantum
            .advance_frame(MAX_SOURCE_WIRE_BYTES + 1)
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::DataLoss);
    }

    #[test]
    fn snapshot_uses_its_already_derived_wire_limit() {
        const SNAPSHOT_SHARE_BYTES: u64 = 89_478_485;
        const DERIVED_WIRE_LIMIT: u64 = 1_746_111;
        const OBSERVED_VALID_FRAME_BYTES: u64 = 1_745_999;

        assert_eq!(
            super::super::snapshot_source_wire_limit(SNAPSHOT_SHARE_BYTES),
            DERIVED_WIRE_LIMIT
        );

        let mut observed = SourceWorkQuantum::from_wire_limit(DERIVED_WIRE_LIMIT);
        assert_eq!(observed.limit, DERIVED_WIRE_LIMIT);
        assert_eq!(
            observed.advance_frame(OBSERVED_VALID_FRAME_BYTES).unwrap(),
            SourceWorkBoundary::Continue
        );

        let mut exact = SourceWorkQuantum::from_wire_limit(DERIVED_WIRE_LIMIT);
        assert_eq!(
            exact.advance_frame(DERIVED_WIRE_LIMIT).unwrap(),
            SourceWorkBoundary::SealAndYield
        );

        let error = SourceWorkQuantum::from_wire_limit(DERIVED_WIRE_LIMIT)
            .advance_frame(DERIVED_WIRE_LIMIT + 1)
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::DataLoss);
    }
}
