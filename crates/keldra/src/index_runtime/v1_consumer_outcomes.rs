//! Resolve an advance against durable Current, never retain partial retry state.

use super::*;

pub(super) fn record_partition_outcome(
    partition: ProjectionPartitionIdentity,
    writer: Writer,
    result: Result<(), Status>,
    writers: &mut BTreeMap<ProjectionPartitionIdentity, Writer>,
    evidence: &mut PartitionEvidenceMap,
) -> bool {
    match result {
        Ok(()) => {
            if let Some(state) = evidence.get_mut(&partition) {
                let published_next = writer
                    .current
                    .as_ref()
                    .map_or(0, |current| current.current.next_offset);
                state.observe_success(published_next, writer_processed_next(&writer), writer.stage);
            }
            writers.insert(partition, writer);
            false
        }
        Err(error) if halts_partition(&error) => {
            let mut writer = writer;
            let transition = contain_integrity_failure(
                &mut writer.halted_on_integrity_failure,
                &mut writer.stage,
                evidence.get_mut(&partition),
                &error,
            );
            if transition.newly_halted {
                tracing::error!(
                    %error,
                    ?partition,
                    family_id = ?writer.recipe.family.family_id,
                    failed_stage = transition.failed_stage.label(),
                    "v1 projection partition halted after integrity failure; unrelated partitions continue"
                );
            }
            writers.insert(partition, writer);
            false
        }
        Err(error) => {
            let state = evidence
                .get_mut(&partition)
                .expect("opened v1 partition has runtime evidence");
            state.stage = writer.stage;
            state.record_retry(writer.stage, &error);
            tracing::warn!(
                %error,
                ?partition,
                family_id = ?writer.recipe.family.family_id,
                failed_stage = writer.stage.label(),
                identical_retries = state.identical_retries,
                "v1 projection partition will replay from Current; unrelated partitions continue"
            );
            // An advance may already have consumed a journal page while
            // some of its publication chunks still exist only in locals.
            // It may also have consumed sealed buffers before a pending
            // publication was created. Neither the scanned cursor nor a
            // pending successor proves that all consumed rows survive an
            // error. Discard the entire speculative writer and reopen from
            // durable Current on the next reconcile. This also handles an
            // ambiguous successful CAS: reopening validates the generation
            // that actually became authoritative before replay continues.
            drop(writer);
            true
        }
    }
}
