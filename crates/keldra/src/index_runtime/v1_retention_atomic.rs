//! Keep executor finalizers until every affected owner's durable view covers them.
use super::super::events::{IndexBarrier, MAX_INDEX_EVENT_PAGE_BYTES};
use super::*;
use keldra_store::{AtomicBatchPublished, LocalChange};

pub(super) async fn cap_before_uncovered_finalizer(
    journal: &IndexEventJournal,
    target: &IndexBarrier,
    source: IndexSourceCursor,
    candidate: u64,
    recipes: &[Arc<PhysicalCatalogRecipe>],
    projections: &V1ProjectionPublisher,
    credits: &IndexingMemoryCredits,
) -> Result<Option<u64>, Status> {
    if recipes.is_empty() {
        // No live index family can depend on an executor publication. The
        // durable catalog checkpoint already proves this empty inventory.
        return Ok(Some(candidate));
    }
    // Admit raw records and the captured/working/returned source vectors before
    // cloning them. This uses the same resident multiplier as producer replay.
    let barrier_bytes = target
        .sources
        .len()
        .checked_mul(512 + std::mem::size_of::<IndexSourceCursor>())
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<IndexBarrier>()))
        .and_then(|bytes| bytes.checked_mul(4));
    let bytes = usize::try_from(MAX_INDEX_EVENT_PAGE_BYTES)
        .ok()
        .and_then(|bytes| bytes.checked_mul(4))
        .and_then(|bytes| bytes.checked_add(barrier_bytes?))
        .ok_or_else(|| Status::resource_exhausted("v1 finalizer proof memory overflow"))?;
    let _memory = credits
        .acquire(IndexingMemoryStage::ReplayInput, bytes)
        .map_err(|_| Status::resource_exhausted("v1 finalizer proof memory unavailable"))?;
    let floors = journal
        .retained_replay_start(target)
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let node = NodeId(u64::from(source.source.node_id));
    let floor = floors
        .sources
        .get(&node)
        .filter(|cursor| cursor.source == source.source)
        .ok_or_else(|| Status::data_loss("v1 finalizer proof source incarnation changed"))?;
    if candidate > source.next_offset || candidate == 0 {
        return Err(Status::data_loss(
            "v1 finalizer proof cut exceeds source barrier",
        ));
    }
    if candidate <= floor.next_offset {
        return Ok(Some(candidate));
    }
    let mut through = target.clone();
    through
        .sources
        .get_mut(&node)
        .ok_or_else(|| Status::data_loss("v1 finalizer source absent"))?
        .next_offset = candidate;
    let mut from = through.clone();
    from.sources
        .get_mut(&node)
        .expect("validated source")
        .next_offset = floor.next_offset;
    while let Some(page) = journal
        .next_raw_page(&from, &through, MAX_INDEX_EVENT_PAGE_BYTES)
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?
    {
        for change in &page.changes {
            if change.node != node || page.through.sources[&node].source != source.source {
                return Err(Status::data_loss(
                    "v1 finalizer proof mixed source incarnations",
                ));
            }
            if let LocalChange::AtomicBatchPublished(batch) = &change.change {
                match covered(batch, target, recipes, projections).await? {
                    Some(true) => {}
                    Some(false) => return Ok(Some(batch.offset)),
                    None => return Ok(None),
                }
            }
        }
        from = page.through;
    }
    if from != through {
        return Err(Status::data_loss(
            "v1 finalizer proof has an incomplete source interval",
        ));
    }
    Ok(Some(candidate))
}

async fn covered(
    batch: &AtomicBatchPublished,
    target: &IndexBarrier,
    recipes: &[Arc<PhysicalCatalogRecipe>],
    projections: &V1ProjectionPublisher,
) -> Result<Option<bool>, Status> {
    batch.validate().map_err(Status::data_loss)?;
    for recipe in recipes {
        if !batch.mutations.iter().any(|mutation| {
            mutation.tenant_id == recipe.family.tenant_id
                && mutation.bucket_id == recipe.family.bucket_id
        }) {
            continue;
        }
        let Some((activation, _)) = projections
            .load_activation(
                &recipe.storage_tenant,
                &recipe.bucket,
                recipe.family.tenant_id,
                recipe.family.bucket_id,
                recipe.family.family_id,
                recipe.physical_generation,
            )
            .await?
        else {
            return Ok(None);
        };
        activation.validate().map_err(index_status)?;
        if activation.physical_catalog_generation != recipe.physical_generation {
            return Ok(None);
        }
        let Some((directory, _)) = projections
            .load_family_directory(
                &recipe.storage_tenant,
                &recipe.bucket,
                recipe.family.tenant_id,
                recipe.family.bucket_id,
                recipe.family.family_id,
            )
            .await?
        else {
            return Ok(None);
        };
        directory.validate().map_err(index_status)?;
        // Only compare positions from the SAME source incarnation. Handoff
        // successor offsets cannot numerically prove a predecessor's cut.
        for (index, mutation) in batch.mutations.iter().enumerate() {
            if mutation.tenant_id != recipe.family.tenant_id
                || mutation.bucket_id != recipe.family.bucket_id
            {
                continue;
            }
            if batch.mutations[..index].iter().any(|prior| {
                prior.tenant_id == mutation.tenant_id
                    && prior.bucket_id == mutation.bucket_id
                    && prior.source_id == mutation.source_id
            }) {
                continue;
            }
            let position = required_position(
                batch,
                mutation.tenant_id,
                mutation.bucket_id,
                mutation.source_id,
            )
            .expect("mutation exists");
            let Some(cursor) = target
                .sources
                .get(&NodeId(u64::from(mutation.source_id.node_id)))
                .filter(|cursor| cursor.source == mutation.source_id)
                .copied()
            else {
                return Ok(None);
            };
            if !directory.entries.iter().any(|entry| {
                entry.partition.source_node == u64::from(cursor.source.node_id)
                    && entry.partition.source_epoch == cursor.source.source_epoch
            }) {
                return Ok(None);
            }
            let Some((next, atomic)) =
                live_directory_cut_for_source(cursor, recipe, &directory, projections).await?
            else {
                return Ok(None);
            };
            if !cut_covers_finalizer(next, atomic, position, batch.cursor) {
                return Ok(Some(false));
            }
        }
    }
    Ok(Some(true))
}

fn cut_covers_finalizer(next: u64, through_atomic: u64, position: u64, cursor: u64) -> bool {
    next > position && through_atomic >= cursor
}

fn required_position(
    batch: &AtomicBatchPublished,
    tenant: u64,
    bucket: u64,
    source: keldra_store::SourceId,
) -> Option<u64> {
    batch
        .mutations
        .iter()
        .filter(|mutation| {
            mutation.tenant_id == tenant
                && mutation.bucket_id == bucket
                && mutation.source_id == source
        })
        .map(|mutation| mutation.source_journal_position)
        .max()
}

#[cfg(test)]
mod tests {
    use super::{cut_covers_finalizer, required_position};
    use keldra_store::{
        AtomicBatchMutation, AtomicBatchPublished, PreparedBundleHash, SourceId, VersionId,
    };

    #[test]
    fn finalizer_requires_both_owner_mutation_and_atomic_cursor() {
        assert!(!cut_covers_finalizer(43, 20, 43, 20));
        assert!(!cut_covers_finalizer(44, 19, 43, 20));
        assert!(cut_covers_finalizer(44, 20, 43, 20));
        assert!(cut_covers_finalizer(100, 21, 43, 20));
    }

    #[test]
    fn finalizer_dependencies_are_exact_bucket_source_incarnations() {
        let owner = SourceId {
            node_id: 1,
            source_epoch: [1; 32],
        };
        let successor = SourceId {
            source_epoch: [2; 32],
            ..owner
        };
        let other = SourceId {
            node_id: 2,
            ..owner
        };
        let mutation = |path: &str, bucket, source, position| AtomicBatchMutation {
            tenant_id: 7,
            bucket_id: bucket,
            exact_path: path.into(),
            canonical_path: None,
            path_version: VersionId(1),
            deleted: false,
            source_id: source,
            source_journal_position: position,
        };
        let mut batch = AtomicBatchPublished {
            offset: 90,
            cursor: 20,
            bundle_hash: PreparedBundleHash([1; 32]),
            mutations: vec![
                mutation("a", 8, owner, 43),
                mutation("b", 8, owner, 45),
                mutation("c", 8, successor, 100),
                mutation("d", 8, other, 200),
                mutation("e", 9, owner, 300),
            ],
        };
        batch.mutations.sort();
        batch.validate().unwrap();
        assert_eq!(required_position(&batch, 7, 8, owner), Some(45));
        assert_eq!(required_position(&batch, 7, 8, successor), Some(100));
        assert_eq!(required_position(&batch, 7, 8, other), Some(200));
        assert_eq!(required_position(&batch, 7, 9, owner), Some(300));
        assert_eq!(required_position(&batch, 6, 8, owner), None);
        // An arbitrarily advanced successor is not predecessor evidence.
        assert!(!cut_covers_finalizer(45, batch.cursor, 45, batch.cursor));
    }
}
