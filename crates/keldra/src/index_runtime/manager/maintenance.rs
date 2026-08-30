//! Bounded compaction admission and debt repayment.

use super::*;

pub(super) async fn compact_one_if_needed(
    job: &BuilderJob,
    candidate: &mut CandidateCommit,
    limits: DebtLimits,
    dependencies: &IndexBuilderDependencies,
) -> Result<bool, Status> {
    invalidation::materialize_pending_live_masks(&job.definition, candidate, dependencies).await?;
    emit_compaction_debt(
        job.kind,
        &candidate.segments,
        limits.maximum_segments,
        limits.maximum_bytes,
        "observed",
    );
    let Some(selection) = debt::select_before_locator_limit(
        &candidate.segments,
        candidate.locator_roots.len(),
        limits,
    ) else {
        let Some(locator_selection) = debt::select_locator_roots(&candidate.locator_roots, limits)
        else {
            return Ok(false);
        };
        tracing::info!(
            index.kind = ?job.kind,
            compaction.trigger = "locator_debt",
            compaction.input_roots = locator_selection.input_roots,
            gauge.keldra_index_locator_roots = candidate.locator_roots.len() as u64,
            monotonic_counter.keldra_index_locator_compaction_admission_stops_total = 1_u64,
            "index source work yielded to bounded locator compaction debt"
        );
        let budget = dependencies.budgets.for_kind(job.kind);
        let (_publication_slot, _permit) =
            acquire_compaction_memory(&dependencies.maintenance_work_slots, budget).await?;
        locator_debt::compact_oldest_prefix(
            &job.definition,
            job.kind,
            locator_selection,
            compaction_admission(),
            candidate,
            dependencies,
        )
        .await?;
        return Ok(true);
    };
    tracing::info!(
        index.kind = ?job.kind,
        compaction.trigger = "debt",
        monotonic_counter.keldra_index_compaction_admission_stops_total = 1_u64,
        "index source work yielded to bounded compaction debt"
    );
    let budget = dependencies.budgets.for_kind(job.kind);
    let (_publication_slot, permit) =
        acquire_compaction_memory(&dependencies.maintenance_work_slots, budget).await?;
    compact_tier(
        &job.definition,
        job.kind,
        selection,
        permit.bytes(),
        compaction_admission(),
        candidate,
        dependencies,
    )
    .await?;
    emit_compaction_debt(
        job.kind,
        &candidate.segments,
        limits.maximum_segments,
        limits.maximum_bytes,
        "repaid",
    );
    Ok(true)
}

pub(super) async fn acquire_maintenance_memory(
    slots: &IndexMaintenanceWorkSlots,
    budget: &super::super::budget::IndexMemoryBudget,
    minimum: u64,
    preferred: u64,
) -> Result<(tokio::sync::OwnedSemaphorePermit, IndexMemoryPermit), Status> {
    // Queue for the scarce maintenance lane before leasing construction
    // memory. Waiting maintenance must not pin bytes that incremental builders
    // can use while it is not runnable.
    let slot = slots.acquire().await?;
    let permit = budget
        .acquire_up_to(minimum, preferred)
        .await
        .map_err(budget_status)?;
    Ok((slot, permit))
}

pub(super) async fn acquire_compaction_memory(
    slots: &IndexMaintenanceWorkSlots,
    budget: &super::super::budget::IndexMemoryBudget,
) -> Result<(tokio::sync::OwnedSemaphorePermit, IndexMemoryPermit), Status> {
    // A compaction is a non-preemptible maintenance turn. Its four default
    // lanes already fit the kind fair share, so borrowing the entire idle
    // parent cannot accelerate it and can prevent a source writer from
    // obtaining its mandatory turn until the merge finishes.
    acquire_maintenance_memory(slots, budget, budget.limit(), budget.limit()).await
}
