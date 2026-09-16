//! Background compaction ownership and bounded launch for the v1 producer.

use super::super::v1_compaction::{
    V1CompactionArtifacts, V1CompactionBase, V1CompactionPublication,
};
use super::*;

pub(super) struct BackgroundCompaction {
    pub(super) predecessor_generation: [u8; 32],
    task: Option<tokio::task::JoinHandle<Result<V1CompactionPublication, Status>>>,
}

impl BackgroundCompaction {
    pub(super) fn is_finished(&self) -> bool {
        self.task
            .as_ref()
            .is_none_or(tokio::task::JoinHandle::is_finished)
    }

    pub(super) async fn finish(mut self) -> Result<V1CompactionPublication, Status> {
        self.task
            .take()
            .expect("background compaction task exists")
            .await
            .map_err(|error| Status::internal(format!("v1 compaction task failed: {error}")))?
    }
}

impl Drop for BackgroundCompaction {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

pub(super) fn should_harvest_background_compaction(
    has_current: bool,
    prepared_source_rows: u64,
    mutation_window_empty: bool,
    compaction_finished: bool,
) -> bool {
    compaction_finished
        && mutation_window_empty
        && !has_publication_work(has_current, prepared_source_rows)
}

pub(super) async fn take_finished_background_compaction(
    writer: &mut Writer,
    publisher: &V1ProjectionPublisher,
) -> Result<Option<(V1CompactionBase, V1CompactionArtifacts)>, Status> {
    if !writer
        .background_compaction
        .as_ref()
        .is_some_and(BackgroundCompaction::is_finished)
    {
        return Ok(None);
    }
    let background = writer
        .background_compaction
        .take()
        .expect("finished v1 compaction exists");
    writer.stage = ProducerStage::Compacting;
    let predecessor_generation = background.predecessor_generation;
    let mut prepared = match background.finish().await {
        Ok(prepared) => prepared,
        Err(error) if error.code() != tonic::Code::DataLoss => {
            tracing::warn!(%error, "optional v1 background compaction failed");
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let current = writer
        .current
        .as_ref()
        .expect("compaction requires Current");
    match prepared
        .rebase_onto(
            publisher,
            &writer.recipe.storage_tenant,
            &writer.recipe.bucket,
            writer.recipe.family.tenant_id,
            writer.recipe.family.bucket_id,
            current,
        )
        .await
    {
        Ok(true) => {
            let (base, artifacts) = prepared.into_parts();
            if base.predecessor.roots == current.generation.roots
                && base.predecessor.query_stream_root == current.generation.query_stream_root
            {
                tracing::debug!(
                    ?predecessor_generation,
                    "no-op v1 compaction proposal discarded"
                );
                Ok(None)
            } else {
                Ok(Some((base, artifacts)))
            }
        }
        Ok(false) => {
            tracing::debug!(?predecessor_generation, "stale v1 compaction discarded");
            Ok(None)
        }
        Err(error) if error.code() != tonic::Code::DataLoss => {
            tracing::warn!(%error, "optional v1 compaction proposal discarded");
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

pub(super) async fn publish_finished_background_compaction(
    writer: &mut Writer,
    publisher: &V1ProjectionPublisher,
) -> Result<bool, Status> {
    let Some((base, artifacts)) = take_finished_background_compaction(writer, publisher).await?
    else {
        return Ok(false);
    };
    let current = writer
        .current
        .as_ref()
        .expect("compaction publication requires Current");
    writer.pending_publication = Some(publisher.prepare_compaction_publication(
        writer.partition,
        current,
        base,
        artifacts,
    )?);
    writer.stage = ProducerStage::Publishing;
    finish_pending_publication(writer, publisher).await?;
    Ok(true)
}

pub(super) fn ensure_background_compaction(
    writer: &mut Writer,
    publisher: &V1ProjectionPublisher,
    cpu: &super::super::cpu::IndexCpuPool,
    credits: &IndexingMemoryCredits,
    compaction_ready: &Arc<tokio::sync::Notify>,
    limits: Limits,
) -> Result<(), Status> {
    let Some(current) = writer.current.as_ref() else {
        return Ok(());
    };
    if writer.background_compaction.is_some() || !generation_needs_compaction(current, limits) {
        return Ok(());
    }
    let bytes = limits.bytes.saturating_div(8).max(1);
    let Ok(component_permit) = credits.acquire(IndexingMemoryStage::SealScratch, bytes) else {
        return Ok(());
    };
    let Ok(query_permit) = credits.acquire(IndexingMemoryStage::OrderingCatalog, bytes) else {
        return Ok(());
    };
    let Ok(preload_permit) = credits.acquire(IndexingMemoryStage::ReplayInput, bytes) else {
        return Ok(());
    };
    let Ok(raw_output_permit) = credits.acquire(IndexingMemoryStage::SealScratch, bytes) else {
        return Ok(());
    };
    let publisher = publisher.clone();
    let cpu = cpu.clone();
    let storage_tenant = writer.recipe.storage_tenant.clone();
    let bucket = writer.recipe.bucket.clone();
    let tenant_id = writer.recipe.family.tenant_id;
    let bucket_id = writer.recipe.family.bucket_id;
    let loaded = current.clone();
    let predecessor_generation = current.current.generation_hash;
    let partition = writer.partition;
    let maximum_runs = usize::try_from(limits.lsm_runs)
        .map_err(|_| Status::invalid_argument("v1 LSM run bound exceeds this platform"))?;
    let maximum_unmerged_bytes = usize::try_from(limits.lsm_bytes)
        .map_err(|_| Status::invalid_argument("v1 LSM byte bound exceeds this platform"))?;
    let compaction_ready = Arc::clone(compaction_ready);
    let task = tokio::spawn(async move {
        let result = publisher
            .prepare_compaction(
                &storage_tenant,
                &bucket,
                tenant_id,
                bucket_id,
                &loaded,
                &cpu,
                maximum_runs,
                maximum_unmerged_bytes,
                bytes,
                preload_permit,
                raw_output_permit,
                ProjectionPackCredits::from_pipeline_permit(component_permit),
                QueryBlockCredits::from_pipeline_permit(query_permit),
            )
            .await;
        let result = match result {
            Ok(compaction) => {
                match publisher
                    .publish_compaction_artifacts(
                        &storage_tenant,
                        &bucket,
                        tenant_id,
                        bucket_id,
                        partition,
                        compaction.artifacts(),
                    )
                    .await
                {
                    Ok(()) => Ok(compaction),
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(error),
        };
        compaction_ready.notify_one();
        result
    });
    writer.background_compaction = Some(BackgroundCompaction {
        predecessor_generation,
        task: Some(task),
    });
    Ok(())
}

fn generation_needs_compaction(current: &LoadedV1ProjectionGeneration, limits: Limits) -> bool {
    current.generation.query_stream_root.run_count >= limits.lsm_runs
        || current.generation.roots.iter().any(|root| {
            root.segment_count >= limits.lsm_runs || root.encoded_bytes >= limits.lsm_bytes
        })
}

pub(super) fn compaction_in_flight(writer: &Writer) -> bool {
    writer
        .background_compaction
        .as_ref()
        .is_some_and(|compaction| !compaction.is_finished())
}
