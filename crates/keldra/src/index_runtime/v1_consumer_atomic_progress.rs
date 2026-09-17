//! Complete captured all-source replay, not highest-observed-cursor inference.
use super::*;

pub(super) fn capture_replay_target(
    writer: &mut Writer,
    target: &IndexBarrier,
    credits: &IndexingMemoryCredits,
) -> Result<(), Status> {
    if writer.atomic_replay_target.is_none()
        && writer.current.as_ref().is_some_and(|current| {
            target.atomic.finalized_through().unwrap_or(0) > current.current.through_atomic_position
        })
    {
        let bytes = target
            .sources
            .len()
            .checked_mul(512 + std::mem::size_of::<super::super::events::IndexSourceCursor>())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<IndexBarrier>()))
            .and_then(|bytes| bytes.checked_mul(4))
            .ok_or_else(|| Status::resource_exhausted("v1 atomic replay target memory overflow"))?;
        let memory = credits
            .acquire(IndexingMemoryStage::ReplayInput, bytes)
            .map_err(|_| {
                Status::resource_exhausted("v1 atomic replay target memory unavailable")
            })?;
        writer.atomic_replay_target = Some((target.clone(), memory));
    }
    Ok(())
}

pub(super) async fn acknowledge_complete_atomic_cut(
    writer: &mut Writer,
    target: &IndexBarrier,
    journal: &IndexEventJournal,
    publisher: &V1ProjectionPublisher,
    credits: &IndexingMemoryCredits,
    limits: Limits,
) -> Result<bool, Status> {
    let Some((through, owned_next)) = complete_atomic_replay(writer, target)? else {
        return Ok(false);
    };
    journal
        .validate_publication_barrier(target)
        .await
        .map_err(event_status)?;
    let current = writer
        .current
        .as_ref()
        .expect("complete replay requires Current");
    if current.current.next_offset < owned_next {
        // Complete replay proves the skipped interval contains no unpublished
        // owned effects. This is a normal empty source cut, never a floor skip.
        apply_rows(writer, owned_next, Vec::new(), credits, limits)?;
        writer.through_atomic = through;
        writer.pending_skipped_ack = true;
        writer.since.get_or_insert_with(Instant::now);
        return Ok(true);
    }
    if current.current.next_offset != owned_next || writer.pending_skipped_ack {
        return Ok(false);
    }
    writer.pending_publication = Some(
        publisher
            .prepare_atomic_cut_publication(
                &writer.recipe.storage_tenant,
                &writer.recipe.bucket,
                writer.recipe.family.tenant_id,
                writer.recipe.family.bucket_id,
                writer.partition,
                current,
                through,
                credits,
            )
            .await?,
    );
    writer.through_atomic = through;
    Ok(true)
}

pub(super) fn complete_atomic_replay(
    writer: &Writer,
    target: &IndexBarrier,
) -> Result<Option<(u64, u64)>, Status> {
    let Some(current) = writer.current.as_ref() else {
        return Ok(None);
    };
    let through = target.atomic.finalized_through().unwrap_or(0);
    if through <= current.current.through_atomic_position
        || !target.atomic.is_clear()
        || writer.scanned.fence != target.fence
        || writer.scanned.sources != target.sources
        || !writer.pending_mutations.is_empty()
        || writer.pending_prepared_rows != 0
        || writer.pending_publication.is_some()
        || writer.look_ahead.is_some()
    {
        return Ok(None);
    }
    let owned_next = target
        .sources
        .get(&NodeId(u64::from(writer.source.node_id)))
        .filter(|cursor| cursor.source == writer.source)
        .ok_or_else(|| Status::unavailable("v1 atomic proof source is absent"))?
        .next_offset;
    let Some(dispatcher) = writer.dispatcher.as_ref() else {
        return Ok(None);
    };
    if dispatcher.checkpoint_limit(writer.source, owned_next) < owned_next
        || writer.pending_next < owned_next
    {
        return Ok(None);
    }
    Ok(Some((through, owned_next)))
}
