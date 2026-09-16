//! Lifetime accounting for reusable scalar-only parser vectors.
use super::*;
use keldra_index::v1::{IndexingMemoryCredits, IndexingMemoryPermit, IndexingMemoryStage};

#[derive(Default)]
struct RetainedScratch {
    scratch: ScalarProjectionScratch,
    permit: Option<IndexingMemoryPermit>,
}

impl RetainedScratch {
    fn bytes(&self) -> usize {
        self.scratch
            .slots
            .capacity()
            .saturating_mul(std::mem::size_of::<SelectedValue>())
            .saturating_add(
                self.scratch
                    .candidates
                    .capacity()
                    .saturating_mul(std::mem::size_of::<usize>()),
            )
    }

    fn reset(&mut self) {
        // Release allocations before the permit that accounts for them.
        self.scratch = ScalarProjectionScratch::default();
        self.permit = None;
    }

    fn prepare_pool(&mut self, credits: &IndexingMemoryCredits) {
        if let Some(permit) = &mut self.permit {
            // Same-stage transfer is a cheap pool-identity check. A different
            // runtime/backend must not inherit another authority's buffers.
            if credits
                .transfer(permit, IndexingMemoryStage::WorkerScratch)
                .is_err()
            {
                self.reset();
            }
        }
    }

    fn retain(&mut self, credits: &IndexingMemoryCredits) {
        self.scratch.slots.clear();
        self.scratch.candidates.clear();
        let bytes = self.bytes();
        if bytes == 0 {
            self.reset();
            return;
        }
        let admitted = if let Some(permit) = &mut self.permit {
            if bytes >= permit.bytes() {
                permit.grow_to(bytes).is_ok()
            } else {
                permit.shrink_to(bytes).is_ok()
            }
        } else {
            match credits.acquire(IndexingMemoryStage::WorkerScratch, bytes) {
                Ok(permit) => {
                    self.permit = Some(permit);
                    true
                }
                Err(_) => false,
            }
        };
        if !admitted {
            self.reset();
        }
    }
}

pub(crate) fn project_compiled_scalar_pointers_accounted(
    source: &mut dyn Read,
    plan: Arc<CompiledScalarProjectionPlan>,
    maximum: usize,
    credits: &IndexingMemoryCredits,
) -> Result<Option<ProjectedScalarPointers>, IndexError> {
    thread_local! {
        static SCRATCH: RefCell<RetainedScratch> = RefCell::new(RetainedScratch::default());
    }
    // Never leave growing buffers in TLS while a CPU closure can unwind. The
    // submitter-owned active parse guard covers growth until retain completes;
    // unwinding drops this wrapper and its old retained lease together.
    let mut retained = SCRATCH.with(|scratch| std::mem::take(&mut *scratch.borrow_mut()));
    retained.prepare_pool(credits);
    let result =
        project_compiled_scalar_pointers_with_scratch(source, plan, maximum, &mut retained.scratch);
    retained.retain(credits);
    SCRATCH.with(|scratch| *scratch.borrow_mut() = retained);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credits(bytes: usize) -> IndexingMemoryCredits {
        IndexingMemoryCredits::new(
            bytes,
            keldra_index::v1::IndexingMemoryLimits {
                hot_payload_bytes: bytes,
                worker_scratch_bytes: bytes,
                prepared_rows_bytes: bytes,
                replay_input_bytes: bytes,
                projection_accumulator_bytes: bytes,
                seal_scratch_bytes: bytes,
                ordering_catalog_bytes: bytes,
            },
        )
        .unwrap()
    }

    #[test]
    fn retained_capacity_is_exact_and_optional_growth_refusal_frees_it() {
        let memory = credits(4096);
        let mut retained = RetainedScratch::default();
        retained.scratch.slots.reserve_exact(8);
        retained.scratch.candidates.reserve_exact(8);
        retained.retain(&memory);
        assert_eq!(memory.used_bytes(), retained.bytes());
        let retained_bytes = retained.bytes();
        let occupied = memory
            .acquire(IndexingMemoryStage::ReplayInput, 4096 - retained_bytes)
            .unwrap();
        retained.scratch.slots.reserve_exact(64);
        retained.retain(&memory);
        assert_eq!(retained.bytes(), 0);
        assert_eq!(memory.used_bytes(), occupied.bytes());
    }

    #[test]
    fn changed_pool_and_unwind_release_previous_capacity() {
        let old = credits(4096);
        let new = credits(4096);
        let mut retained = RetainedScratch::default();
        retained.scratch.slots.reserve_exact(8);
        retained.retain(&old);
        assert!(old.used_bytes() > 0);
        retained.prepare_pool(&new);
        assert_eq!(old.used_bytes(), 0);
        assert_eq!(retained.bytes(), 0);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            retained.scratch.candidates.reserve_exact(8);
            retained.retain(&new);
            let _owned = retained;
            panic!("simulated parser unwind");
        }));
        assert_eq!(new.used_bytes(), 0);
    }

    #[test]
    fn malformed_selection_retains_only_empty_charged_parser_vectors() {
        let memory = credits(16 * 1024);
        let active = memory
            .acquire(IndexingMemoryStage::WorkerScratch, 8 * 1024)
            .unwrap();
        let plan =
            CompiledScalarProjectionPlan::compile(Arc::from(vec!["/value".to_owned()])).unwrap();
        let mut retained = RetainedScratch::default();
        let result = project_compiled_scalar_pointers_with_scratch(
            &mut std::io::Cursor::new(br#"{"value":"partial""#),
            plan,
            4096,
            &mut retained.scratch,
        )
        .unwrap();
        assert!(result.is_none());
        retained.retain(&memory);
        assert!(retained.scratch.slots.is_empty());
        assert!(retained.scratch.candidates.is_empty());
        drop(active);
        assert_eq!(memory.used_bytes(), retained.bytes());
        retained.reset();
        assert_eq!(memory.used_bytes(), 0);
    }
}
