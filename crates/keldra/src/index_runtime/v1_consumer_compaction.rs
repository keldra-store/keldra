//! Background compaction ownership and bounded launch for the v1 producer.

use super::super::v1_compaction::{
    V1CompactionArtifacts, V1CompactionBase, V1CompactionPublication,
};
use super::*;

pub(super) struct BackgroundCompaction {
    pub(super) predecessor_generation: [u8; 32],
    task: Option<tokio::task::JoinHandle<Result<V1CompactionPublication, Status>>>,
    ready: Arc<tokio::sync::Notify>,
}

impl BackgroundCompaction {
    pub(super) fn is_finished(&self) -> bool {
        self.task
            .as_ref()
            .is_none_or(tokio::task::JoinHandle::is_finished)
    }

    pub(super) async fn finish(mut self) -> Result<V1CompactionPublication, Status> {
        let result = self
            .task
            .as_mut()
            .expect("background compaction task exists")
            .await
            .map_err(|error| Status::internal(format!("v1 compaction task failed: {error}")))?;
        self.task.take();
        result
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
    let predecessor_generation = background.predecessor_generation;
    let ready = Arc::clone(&background.ready);
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
    if needs_background_rebase(
        prepared.expected_generation_hash(),
        current.current.generation_hash,
    ) {
        // Never load a newer stream tree while holding up source advancement.
        // Rebase and stage its generated pages under the same retained permits
        // in the background. Harvest is conditional on that exact generation;
        // a later source publication causes another background rebase, never
        // an overwrite of newly appended runs or a change in the source cut.
        let current = current.clone();
        let predecessor_generation = current.current.generation_hash;
        let publisher = publisher.clone();
        let storage_tenant = writer.recipe.storage_tenant.clone();
        let bucket = writer.recipe.bucket.clone();
        let tenant_id = writer.recipe.family.tenant_id;
        let bucket_id = writer.recipe.family.bucket_id;
        let partition = writer.partition;
        let task_ready = Arc::clone(&ready);
        let task = tokio::spawn(async move {
            let result = async {
                let Some(rebased) = prepared
                    .rebase_onto(
                        &publisher,
                        &storage_tenant,
                        &bucket,
                        tenant_id,
                        bucket_id,
                        &current,
                    )
                    .await?
                else {
                    return Err(Status::aborted(
                        "v1 compaction selected inputs were replaced",
                    ));
                };
                prepared = rebased;
                publisher
                    .publish_compaction_artifacts(
                        &storage_tenant,
                        &bucket,
                        tenant_id,
                        bucket_id,
                        partition,
                        prepared.artifacts(),
                    )
                    .await?;
                Ok(prepared)
            }
            .await;
            task_ready.notify_one();
            result
        });
        writer.background_compaction = Some(BackgroundCompaction {
            predecessor_generation,
            task: Some(task),
            ready,
        });
        return Ok(None);
    }
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

fn needs_background_rebase(prepared: [u8; 32], current: [u8; 32]) -> bool {
    prepared != current
}

#[cfg(test)]
mod background_tests {
    use super::*;

    #[test]
    fn harvest_requires_the_exact_generation_prepared_in_background() {
        assert!(!needs_background_rebase([1; 32], [1; 32]));
        assert!(needs_background_rebase([1; 32], [2; 32]));
    }

    #[tokio::test]
    async fn completed_background_integrity_failure_is_not_reclassified() {
        let task =
            tokio::spawn(async { Err(Status::data_loss("component stream root invariant")) });
        let background = BackgroundCompaction {
            predecessor_generation: [1; 32],
            task: Some(task),
            ready: Arc::new(tokio::sync::Notify::new()),
        };
        let error = background.finish().await.err().unwrap();
        assert_eq!(error.code(), tonic::Code::DataLoss);
        assert_eq!(error.message(), "component stream root invariant");
    }

    #[tokio::test]
    async fn cancelling_finish_keeps_the_owned_task_abort_guard() {
        let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _sender = sender;
            std::future::pending::<()>().await;
            Err(Status::aborted("unreachable"))
        });
        let background = BackgroundCompaction {
            predecessor_generation: [1; 32],
            task: Some(task),
            ready: Arc::new(tokio::sync::Notify::new()),
        };
        let mut completion = Box::pin(background.finish());
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut completion)
                .await
                .is_err()
        );
        drop(completion);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), receiver)
                .await
                .unwrap()
                .is_err()
        );
    }

    #[tokio::test]
    async fn dropping_partition_ownership_cancels_only_its_compaction_task() {
        let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let sender = sender;
            std::future::pending::<()>().await;
            drop(sender);
            Err(Status::aborted("unreachable"))
        });
        let background = BackgroundCompaction {
            predecessor_generation: [1; 32],
            task: Some(task),
            ready: Arc::new(tokio::sync::Notify::new()),
        };
        drop(background);
        assert!(receiver.await.is_err());
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
    let publication = match publisher.prepare_compaction_publication(
        writer.partition,
        current,
        base,
        artifacts,
    ) {
        Ok(publication) => publication,
        Err(error) if error.code() == tonic::Code::ResourceExhausted => return Ok(false),
        Err(error) => return Err(error),
    };
    writer.pending_publication = Some(publication);
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
    let credits = credits.clone();
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
    let ready = Arc::clone(&compaction_ready);
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
                &credits,
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
        ready,
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
