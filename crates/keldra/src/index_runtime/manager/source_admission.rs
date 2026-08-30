//! Adaptive admission for decoded source pages.

use super::*;

pub(super) async fn retry_smaller_source_page(
    job: &BuilderJob,
    mut work: CatchUpWork,
    mut active: ActiveIncrementalBuffer,
    page: IndexJournalPage,
    error: Status,
    dependencies: &IndexBuilderDependencies,
) -> Result<(BuilderPhase, BuilderDisposition, Option<CommittedIndexView>), Status> {
    let Some(reduced) = reduced_source_wire_limit(active.maximum_page_bytes) else {
        return Err(error);
    };
    // The journal barrier has not advanced. Drop the oversized decoded page
    // before sealing any active segment, then retry the exact page with a
    // smaller wire bound on the next turn.
    drop(page);
    if !active.builder.is_empty() {
        freeze_builder(
            &job.definition,
            job.kind,
            &mut active.builder,
            &mut work.candidate,
            dependencies,
        )
        .await?;
    }
    active.maximum_page_bytes = reduced;
    active.quantum = SourceWorkQuantum::from_wire_limit(reduced);
    active.operations = 0;
    work.active = Some(active);
    Ok((BuilderPhase::CatchUp(work), BuilderDisposition::Ready, None))
}
