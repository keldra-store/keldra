//! Process-local proof pairing compiled inventory with complete journal replay.
use super::super::events::IndexBarrier;
use super::*;

/// A persisted checkpoint cannot certify a freshly empty in-memory catalog.
/// This owner is created only after this process completes baseline inventory,
/// contiguous all-source replay, and every existing durable catalog checkpoint.
pub(crate) struct CompletedCatalogReplay {
    pub(crate) generation: u64,
    pub(crate) physical: Arc<PhysicalCatalogSnapshot>,
    pub(crate) barrier: IndexBarrier,
    _permit: IndexingMemoryPermit,
}

impl IndexCatalog {
    pub(crate) fn begin_catalog_replay(&self) -> Result<(), Status> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| Status::internal("active index catalog lock is poisoned"))?;
        state.completed_replay = None;
        Ok(())
    }

    pub(crate) fn catalog_replay_generation(&self) -> Result<u64, Status> {
        let state = self
            .inner
            .lock()
            .map_err(|_| Status::internal("active index catalog lock is poisoned"))?;
        Ok(state.generation)
    }

    /// Refuse a mark if inventory changed while durable checkpoint I/O ran.
    /// The token is mutable catalog generation, not physical snapshot generation:
    /// logical alias changes may leave the physical snapshot untouched.
    pub(crate) fn complete_catalog_replay(
        &self,
        expected_generation: u64,
        barrier: &IndexBarrier,
    ) -> Result<bool, Status> {
        if !barrier.atomic.is_clear()
            || barrier.sources.is_empty()
            || barrier.sources.iter().any(|(node, cursor)| {
                node.0 != u64::from(cursor.source.node_id)
                    || cursor.next_offset == 0
                    || cursor.source.source_epoch == [0; 32]
            })
        {
            return Err(Status::data_loss(
                "completed catalog replay has an invalid source barrier",
            ));
        }
        let mut state = self
            .inner
            .lock()
            .map_err(|_| Status::internal("active index catalog lock is poisoned"))?;
        if state.generation != expected_generation {
            return Ok(false);
        }
        let bytes = barrier
            .sources
            .len()
            .checked_mul(512 + std::mem::size_of::<super::super::events::IndexSourceCursor>())
            .and_then(|bytes| {
                bytes.checked_add(std::mem::size_of::<CompletedCatalogReplay>() + 128)
            })
            .ok_or_else(|| Status::resource_exhausted("catalog replay proof memory overflow"))?;
        let permit = state
            .recipe_resident
            .credits
            .acquire(IndexingMemoryStage::OrderingCatalog, bytes)
            .map_err(|_| Status::resource_exhausted("catalog replay proof memory unavailable"))?;
        state.completed_replay = Some(Arc::new(CompletedCatalogReplay {
            generation: expected_generation,
            physical: state.published_physical.clone(),
            barrier: barrier.clone(),
            _permit: permit,
        }));
        Ok(true)
    }

    /// Clone one already charged owner, pairing the exact inventory and cut
    /// under the same lock instead of joining unrelated memory/disk snapshots.
    pub(crate) fn retention_snapshot(&self) -> Result<Option<Arc<CompletedCatalogReplay>>, Status> {
        let state = self
            .inner
            .lock()
            .map_err(|_| Status::internal("active index catalog lock is poisoned"))?;
        Ok(state.completed_replay.clone())
    }
}
