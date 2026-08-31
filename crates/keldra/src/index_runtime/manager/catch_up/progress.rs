//! Exact journal accounting and source-barrier advancement.

use super::*;

pub(super) fn processed_journal_encoded_bytes(
    changes: &[IndexJournalChange],
) -> Result<u64, Status> {
    changes.iter().try_fold(0_u64, |total, change| {
        total
            .checked_add(index_journal_change_encoded_len(change).map_err(event_status)?)
            .ok_or_else(|| Status::resource_exhausted("processed journal bytes overflow"))
    })
}

pub(super) fn add_source_payload_bytes(
    initial: u64,
    schema: &Schema,
    sources: &[IndexSourceMutation],
) -> Result<u64, Status> {
    sources.iter().try_fold(initial, |total, source| {
        total
            .checked_add(source_payload_bytes_for(schema, source))
            .ok_or_else(|| Status::resource_exhausted("index source payload bytes overflow"))
    })
}

pub(super) fn barrier_after_changes(
    from: &IndexBarrier,
    entries: &[IndexJournalChange],
) -> Result<IndexBarrier, Status> {
    let mut through = from.clone();
    for entry in entries {
        let cursor = through
            .sources
            .get_mut(&entry.node)
            .ok_or_else(|| Status::data_loss("journal page names an unknown source node"))?;
        cursor.next_offset = entry
            .change
            .offset()
            .checked_add(1)
            .ok_or_else(|| Status::data_loss("journal change offset overflow"))?;
    }
    Ok(through)
}
