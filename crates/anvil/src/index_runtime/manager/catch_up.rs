//! Bounded exact-current journal catch-up.

use anvil_store::{CurrentObjectSnapshot, MAX_OBJECT_RECORD_EXPORT_RECORDS};
use rayon::prelude::*;

use super::rebuild::{
    PreparedProjection, ProjectionBatch, fetch_projection_sources, partition_projection_lanes,
    receive_ordered_lane_item,
};
use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn process_journal_page(
    definition: &CatalogDefinition,
    specification: &IndexSpecification,
    kind: IndexKind,
    target: &IndexBarrier,
    page: &IndexJournalPage,
    plan: SegmentMemoryPlan,
    builder: &mut EngineSegmentBuilder,
    candidate: &mut CandidateGeneration,
    dependencies: &IndexBuilderDependencies,
) -> Result<bool, Status> {
    let paths = journal_source_paths(
        definition.tenant_id,
        definition.bucket_id,
        &definition.stored.path_prefix,
        page,
    )
    .into_iter()
    .collect::<Vec<_>>();
    let changed = !paths.is_empty();

    // One journal page is already byte-bounded. Exact-current reads retain
    // that bound and additionally obey the store's bounded multi-get limit.
    for paths in paths.chunks(MAX_OBJECT_RECORD_EXPORT_RECORDS as usize) {
        let sources = load_target_sources(definition, paths, target, dependencies).await?;
        project_sources(
            definition,
            specification,
            kind,
            plan,
            sources,
            builder,
            candidate,
            dependencies,
        )
        .await?;
    }
    Ok(changed)
}

pub(super) fn journal_source_paths(
    tenant_id: u64,
    bucket_id: u64,
    path_prefix: &str,
    page: &IndexJournalPage,
) -> BTreeMap<String, u64> {
    let mut paths = BTreeMap::<String, u64>::new();
    for entry in &page.changes {
        let (change_tenant_id, change_bucket_id, path, version) = match &entry.change {
            LocalChange::ObjectHead(change) => (
                change.tenant_id,
                change.bucket_id,
                &change.exact_path,
                change.path_version.0,
            ),
            LocalChange::RetainedVersionDeleted(change) => (
                change.tenant_id,
                change.bucket_id,
                &change.exact_path,
                change
                    .resulting_head_version
                    .unwrap_or(change.deleted_version)
                    .0,
            ),
            LocalChange::AggregateChanged(_) | LocalChange::ContentLifecycleChanged(_) => continue,
            _ => continue,
        };
        if change_tenant_id == tenant_id
            && change_bucket_id == bucket_id
            && path_matches_prefix(path, path_prefix)
            && !contains_reserved_segment(path)
        {
            paths
                .entry(path.clone())
                .and_modify(|selected| *selected = (*selected).max(version))
                .or_insert(version);
        }
    }
    paths
}

async fn load_target_sources(
    definition: &CatalogDefinition,
    paths: &[(String, u64)],
    target: &IndexBarrier,
    dependencies: &IndexBuilderDependencies,
) -> Result<Vec<IndexSourceMutation>, Status> {
    let keys = paths
        .iter()
        .map(|(path, _)| {
            ObjectKey::new(&definition.stored.tenant, &definition.stored.bucket, path)
                .map_err(|error| Status::internal(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    // Catch-up is background scheduler work, not a public RPC. This is its
    // existing retry quantum: an unfinished atomic program yields and retries
    // rather than inheriting the unrelated public 30-second request deadline.
    let snapshots = dependencies
        .reader
        .current_head_snapshots_stable(
            &keys,
            definition.tenant_id,
            definition.bucket_id,
            BUILDER_RETRY_INTERVAL,
        )
        .await?;
    paths
        .iter()
        .zip(snapshots)
        .map(|((path, fallback_version), snapshot)| {
            target_source(definition, path, *fallback_version, target, snapshot)
        })
        .collect()
}

fn target_source(
    definition: &CatalogDefinition,
    path: &str,
    fallback_version: u64,
    target: &IndexBarrier,
    snapshot: Option<CurrentObjectSnapshot>,
) -> Result<IndexSourceMutation, Status> {
    let Some(snapshot) = snapshot else {
        return Ok(IndexSourceMutation::Remove(DocumentRef {
            path: path.to_owned(),
            version: fallback_version,
        }));
    };
    if snapshot.exact_path != path {
        return Err(Status::data_loss(
            "index current-head batch returned another exact path",
        ));
    }
    require_visible_head(&snapshot.head, target)?;
    let version = &snapshot.version;
    if version.deleted
        || !source_matches_definition(&definition.stored, path, version.content_type.as_deref())
    {
        return Ok(IndexSourceMutation::Remove(DocumentRef {
            path: path.to_owned(),
            version: version.id.0,
        }));
    }
    Ok(IndexSourceMutation::Upsert(build_object(path, version)?))
}

#[allow(clippy::too_many_arguments)]
async fn project_sources(
    definition: &CatalogDefinition,
    specification: &IndexSpecification,
    kind: IndexKind,
    plan: SegmentMemoryPlan,
    sources: Vec<IndexSourceMutation>,
    builder: &mut EngineSegmentBuilder,
    candidate: &mut CandidateGeneration,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    let configured_lanes = usize::try_from(dependencies.config.projection_max_lanes(kind))
        .map_err(|_| Status::resource_exhausted("projection lane limit exceeds platform"))?;
    let max_lanes = configured_lanes.min(dependencies.cpu.workers()).max(1);
    let projection_budget = plan.max_source_projection_bytes as u64;
    let mut batch = ProjectionBatch::new(projection_budget, max_lanes);
    for source in sources {
        let prepared = PreparedProjection::new(specification, source)?;
        if let Some(pending) = batch.try_push(prepared)? {
            let full = std::mem::replace(
                &mut batch,
                ProjectionBatch::new(projection_budget, max_lanes),
            );
            project_catch_up_batch(
                definition,
                specification,
                kind,
                plan,
                full,
                builder,
                candidate,
                dependencies,
            )
            .await?;
            if batch.try_push(pending)?.is_some() {
                return Err(Status::internal(
                    "catch-up projection source was rejected by an empty batch after admission",
                ));
            }
        }
    }
    if !batch.is_empty() {
        project_catch_up_batch(
            definition,
            specification,
            kind,
            plan,
            batch,
            builder,
            candidate,
            dependencies,
        )
        .await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn project_catch_up_batch(
    definition: &CatalogDefinition,
    specification: &IndexSpecification,
    kind: IndexKind,
    plan: SegmentMemoryPlan,
    batch: ProjectionBatch,
    builder: &mut EngineSegmentBuilder,
    candidate: &mut CandidateGeneration,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    let effective_lanes = batch.effective_lanes();
    let lane_limit = batch.lane_limit()?;
    let fetched = fetch_projection_sources(batch.sources, effective_lanes, dependencies).await?;
    let source_count = fetched.len();
    let lanes = partition_projection_lanes(fetched, effective_lanes);
    let mut senders = Vec::with_capacity(lanes.len());
    let mut receivers = Vec::with_capacity(lanes.len());
    for _ in 0..lanes.len() {
        let (sender, receiver) = tokio::sync::mpsc::channel::<ProjectedSource>(1);
        senders.push(sender);
        receivers.push(receiver);
    }
    let cpu = dependencies.cpu.clone();
    let projection_specification = specification.clone();
    let queued = Instant::now();
    let cpu_task = tokio::spawn(async move {
        cpu.install(move || {
            let cpu_started = Instant::now();
            let queue_seconds = cpu_started.saturating_duration_since(queued).as_secs_f64();
            let lane_cpu_seconds = lanes
                .into_par_iter()
                .zip(senders.into_par_iter())
                .map(|(lane, sender)| {
                    lane.into_iter().fold(0.0, |cpu_seconds, mut fetched| {
                        let started = Instant::now();
                        let reader = fetched
                            .payload
                            .as_mut()
                            .map(|payload| payload as &mut dyn std::io::Read);
                        let projected = project_mutation(
                            &projection_specification,
                            fetched.source,
                            reader,
                            lane_limit,
                        );
                        let cpu_seconds = cpu_seconds + started.elapsed().as_secs_f64();
                        let failed = projected.is_err();
                        if sender.blocking_send(projected).is_err() || failed {
                            return cpu_seconds;
                        }
                        cpu_seconds
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .sum();
            ProjectionExecution {
                queue_seconds,
                cpu_seconds: lane_cpu_seconds,
            }
        })
        .await
        .map_err(cpu_status)
    });

    let mut failure = None;
    for position in 0..source_count {
        let projected = match receive_ordered_lane_item(&mut receivers, position).await {
            Some(projected) => projected,
            None => {
                failure = Some(Status::internal(
                    "catch-up projection lane omitted a source",
                ));
                break;
            }
        };
        match projected {
            Ok((mutation, diagnostics)) => {
                candidate.diagnostics.add(diagnostics);
                if let Err(error) = push_or_flush(
                    definition,
                    specification,
                    kind,
                    plan,
                    builder,
                    mutation,
                    candidate,
                    dependencies,
                )
                .await
                {
                    failure = Some(error);
                    break;
                }
            }
            Err(error) => {
                failure = Some(index_status(error));
                break;
            }
        }
    }
    drop(receivers);
    cpu_task
        .await
        .map_err(|error| Status::internal(format!("catch-up projection task failed: {error}")))??;
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(())
}

type ProjectedSource = Result<(EngineMutation, IndexBuildDiagnostics), IndexError>;

#[cfg(test)]
mod tests {
    use super::*;

    fn prepared(index: usize, projection_bytes: u64, resident_bytes: u64) -> PreparedProjection {
        PreparedProjection {
            source: IndexSourceMutation::Remove(DocumentRef {
                path: format!("objects/{index}"),
                version: 1,
            }),
            projection_bytes,
            resident_bytes,
            needs_payload: false,
        }
    }

    #[test]
    fn catch_up_page_batch_keeps_all_sources_beyond_the_lane_count() {
        const BUDGET: u64 = 32 * 1024 * 1024;
        let mut batch = ProjectionBatch::new(BUDGET, 4);
        for index in 0..MAX_OBJECT_RECORD_EXPORT_RECORDS as usize {
            assert!(
                batch
                    .try_push(prepared(index, 1_024, 4_096))
                    .unwrap()
                    .is_none()
            );
        }
        assert_eq!(batch.sources.len(), 1_000);
        assert_eq!(batch.effective_lanes(), 4);
        let two_bounded_output_slots_per_lane = 2 * batch.effective_lanes() as u64;
        assert!(
            batch.resident_bytes
                + two_bounded_output_slots_per_lane * batch.lane_limit().unwrap() as u64
                <= BUDGET
        );
    }

    #[test]
    fn catch_up_page_batch_rejects_sources_before_exceeding_its_shared_bytes() {
        let mut batch = ProjectionBatch::new(40, 4);
        assert!(batch.try_push(prepared(0, 10, 10)).unwrap().is_none());
        assert!(batch.try_push(prepared(1, 10, 10)).unwrap().is_none());
        assert!(batch.try_push(prepared(2, 10, 10)).unwrap().is_some());
        assert_eq!(batch.sources.len(), 2);
        assert_eq!(batch.resident_bytes, 20);
        assert_eq!(batch.lane_limit().unwrap(), 10);
    }

    #[tokio::test]
    async fn catch_up_lanes_progress_concurrently_and_apply_in_source_order() {
        let lanes = partition_projection_lanes((0_u64..6).collect(), 3);
        let (progress_sender, mut progress_receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut receivers = Vec::new();
        let mut tasks = Vec::new();
        for lane in lanes {
            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            receivers.push(receiver);
            let progress_sender = progress_sender.clone();
            tasks.push(tokio::task::spawn_blocking(move || {
                for value in lane {
                    sender.blocking_send(value).unwrap();
                    progress_sender.send(value).unwrap();
                }
            }));
        }
        drop(progress_sender);

        let mut first_round = Vec::new();
        for _ in 0..3 {
            first_round.push(progress_receiver.recv().await.unwrap());
        }
        first_round.sort_unstable();
        assert_eq!(first_round, [0, 1, 2]);

        let mut delivered = Vec::new();
        for position in 0..6 {
            delivered.push(
                receive_ordered_lane_item(&mut receivers, position)
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(delivered, (0_u64..6).collect::<Vec<_>>());
        for task in tasks {
            task.await.unwrap();
        }
    }
}
