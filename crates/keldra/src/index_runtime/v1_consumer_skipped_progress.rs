//! Durable skipped-cut acknowledgement with bounded source-wide proof.
use super::*;

/// A routed empty page proves no family mutation, but hides ordinary writes
/// in other buckets. Inspect the complete owned-source skipped interval before
/// durably acknowledging it. Publication artifacts themselves cannot arm an
/// acknowledgement, preventing Current -> journal -> Current feedback.
pub(super) async fn acknowledge_external_skipped_positions(
    writer: &mut Writer,
    target: &IndexBarrier,
    journal: &IndexEventJournal,
    credits: &IndexingMemoryCredits,
    limits: Limits,
) -> Result<(), Status> {
    if writer.current.is_none()
        || !writer.pending_mutations.is_empty()
        || writer.pending_next <= writer.accumulator.next_offset()
    {
        return Ok(());
    }
    let next = writer.pending_next;
    let first = writer
        .skipped_proof_next
        .max(writer.accumulator.next_offset());
    let external = if writer.pending_prepared_rows != 0 || writer.pending_skipped_ack {
        true
    } else if first >= next {
        false
    } else {
        let max_page = u64::try_from(limits.flush_bytes.saturating_div(4).max(1))
            .unwrap_or(u64::MAX)
            .min(MAX_INDEX_EVENT_PAGE_BYTES)
            .max(1);
        let page_bytes = usize::try_from(max_page)
            .unwrap_or(usize::MAX)
            .checked_mul(JOURNAL_PAGE_RESIDENT_MULTIPLIER)
            .and_then(|bytes| {
                // Raw page records use the existing wire-to-resident bound.
                // Also admit the captured/working/returned barrier vectors,
                // which do not contribute to the page's wire byte count.
                target
                    .sources
                    .len()
                    .checked_mul(
                        512 + std::mem::size_of::<super::super::events::IndexSourceCursor>(),
                    )
                    .and_then(|sources| sources.checked_add(std::mem::size_of::<IndexBarrier>()))
                    .and_then(|barrier| barrier.checked_mul(4))
                    .and_then(|barriers| bytes.checked_add(barriers))
            })
            .ok_or_else(|| Status::resource_exhausted("v1 skipped-page memory overflow"))?;
        let _memory = credits
            .acquire(IndexingMemoryStage::ReplayInput, page_bytes)
            .map_err(|_| Status::resource_exhausted("v1 skipped-page memory unavailable"))?;
        skipped_interval_has_external_change(journal, writer.source, first, next, target, max_page)
            .await?
    };
    if external {
        apply_rows(writer, next, Vec::new(), credits, limits)?;
        writer.pending_skipped_ack = true;
        writer.since.get_or_insert_with(Instant::now);
    }
    writer.skipped_proof_next = writer.skipped_proof_next.max(next);
    Ok(())
}

pub(in crate::index_runtime) async fn skipped_interval_has_external_change(
    journal: &IndexEventJournal,
    source: SourceId,
    first: u64,
    next: u64,
    target: &IndexBarrier,
    maximum_page_bytes: u64,
) -> Result<bool, Status> {
    let node = NodeId(u64::from(source.node_id));
    let mut through = target.clone();
    let cursor = through
        .sources
        .get_mut(&node)
        .filter(|cursor| cursor.source == source)
        .ok_or_else(|| Status::unavailable("v1 skipped proof source is absent"))?;
    if first == 0 || first > next || next > cursor.next_offset {
        return Err(Status::data_loss(
            "v1 skipped proof exceeds its captured source cut",
        ));
    }
    cursor.next_offset = next;
    let mut from = through.clone();
    from.sources
        .get_mut(&node)
        .expect("validated source remains present")
        .next_offset = first;
    let mut external = false;
    while let Some(page) = journal
        .next_raw_page(&from, &through, maximum_page_bytes)
        .await
        .map_err(event_status)?
    {
        for change in &page.changes {
            if change.node != node || page.through.sources[&node].source != source {
                return Err(Status::data_loss(
                    "v1 skipped proof mixed source identities",
                ));
            }
            external |= super::super::events::is_index_source_change(&change.change);
        }
        from = page.through;
    }
    if from != through {
        return Err(Status::data_loss(
            "v1 skipped proof did not cover the complete interval",
        ));
    }
    Ok(external)
}

pub(super) fn owned_source_scan_start(
    target: &IndexBarrier,
    retained_start: &IndexBarrier,
    source: SourceId,
    next: u64,
    has_current: bool,
) -> Result<IndexBarrier, Status> {
    let mut scanned = retained_start.clone();
    if scanned.fence != target.fence || scanned.sources.len() != target.sources.len() {
        return Err(Status::unavailable("v1 retained replay placement changed"));
    }
    let cursor = scanned
        .sources
        .get_mut(&NodeId(u64::from(source.node_id)))
        .filter(|cursor| cursor.source == source)
        .ok_or_else(|| Status::unavailable("assigned v1 source is absent"))?;
    let captured = target
        .sources
        .get(&NodeId(u64::from(source.node_id)))
        .filter(|cursor| cursor.source == source)
        .ok_or_else(|| Status::unavailable("assigned v1 source is absent"))?;
    if next == 0 || next > captured.next_offset {
        return Err(Status::data_loss("v1 owned replay cut exceeds source"));
    }
    if has_current && next < cursor.next_offset {
        return Err(Status::failed_precondition(
            "v1 Current lost retained source history",
        ));
    }
    // Fresh baselines read authoritative heads before replacing this sentinel
    // with their captured cut; an existing Current must never skip lost history.
    cursor.next_offset = next;
    Ok(scanned)
}
