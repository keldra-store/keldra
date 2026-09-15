//! Background compaction ownership and bounded launch for the v1 producer.

use super::super::v1_compaction::V1CompactionPublication;
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
