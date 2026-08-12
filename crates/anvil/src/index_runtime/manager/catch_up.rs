//! Bounded exact-current journal catch-up.

use anvil_store::{CurrentObjectSnapshot, MAX_OBJECT_RECORD_EXPORT_RECORDS};

use super::rebuild::{PreparedProjection, ProjectionBatch, execute_projection_batch};
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
    let mut pending = sources
        .into_iter()
        .map(|source| PreparedProjection::new(specification, source))
        .collect::<Result<VecDeque<_>, _>>()?;
    while !pending.is_empty() {
        let remaining = plan
            .max_resident_bytes
            .checked_sub(builder.resident_bytes())
            .ok_or_else(|| Status::internal("catch-up builder exceeded its resident budget"))?;
        if remaining == 0 && !builder.is_empty() {
            flush_catch_up_builder(
                definition,
                specification,
                kind,
                plan,
                builder,
                candidate,
                dependencies,
            )
            .await?;
            continue;
        }

        let mut batch = ProjectionBatch::new(projection_budget, remaining as u64, max_lanes);
        while let Some(next) = pending.front() {
            if !batch.can_accept(next)? {
                break;
            }
            let next = pending
                .pop_front()
                .ok_or_else(|| Status::internal("catch-up projection queue changed"))?;
            if batch.try_push(next)?.is_some() {
                return Err(Status::internal(
                    "catch-up projection admission changed after its bounded check",
                ));
            }
        }

        if batch.is_empty() {
            if !builder.is_empty() {
                flush_catch_up_builder(
                    definition,
                    specification,
                    kind,
                    plan,
                    builder,
                    candidate,
                    dependencies,
                )
                .await?;
                continue;
            }
            let source = pending
                .pop_front()
                .ok_or_else(|| Status::internal("catch-up projection queue is empty"))?;
            return match batch.try_push(source) {
                Err(error) => Err(error),
                Ok(_) => Err(Status::internal(
                    "catch-up projection admission rejected a source that fits",
                )),
            };
        }

        project_catch_up_batch(specification, batch, builder, candidate, dependencies).await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn project_catch_up_batch(
    specification: &IndexSpecification,
    batch: ProjectionBatch,
    builder: &mut EngineSegmentBuilder,
    candidate: &mut CandidateGeneration,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    let effective_lanes = batch.effective_lanes();
    let lane_limit = batch.lane_limit()?;
    let (projected, _) = execute_projection_batch(
        specification,
        batch.sources,
        effective_lanes,
        lane_limit,
        dependencies,
    )
    .await?;
    for projected in projected {
        match projected {
            Ok((mutation, diagnostics)) => {
                candidate.diagnostics.add(diagnostics);
                match builder.try_push(mutation).map_err(index_status)? {
                    EngineSegmentPush::Accepted => {}
                    EngineSegmentPush::Full(_) => {
                        return Err(Status::internal(
                            "catch-up projection exceeded its admitted builder capacity",
                        ));
                    }
                }
            }
            Err(error) => return Err(index_status(error)),
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn flush_catch_up_builder(
    definition: &CatalogDefinition,
    specification: &IndexSpecification,
    kind: IndexKind,
    plan: SegmentMemoryPlan,
    builder: &mut EngineSegmentBuilder,
    candidate: &mut CandidateGeneration,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    let replacement = EngineSegmentBuilder::new(specification, plan).map_err(index_status)?;
    let full = std::mem::replace(builder, replacement);
    flush_builder(definition, kind, full, candidate, dependencies).await
}

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
        let mut batch = ProjectionBatch::new(BUDGET, BUDGET, 4);
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
        assert!(
            batch.resident_bytes + batch.sources.len() as u64 * batch.lane_limit().unwrap() as u64
                <= BUDGET
        );
    }

    #[test]
    fn catch_up_page_batch_rejects_sources_before_exceeding_its_shared_bytes() {
        let mut batch = ProjectionBatch::new(40, 40, 4);
        assert!(batch.try_push(prepared(0, 10, 10)).unwrap().is_none());
        assert!(batch.try_push(prepared(1, 10, 10)).unwrap().is_none());
        assert!(batch.try_push(prepared(2, 10, 10)).unwrap().is_some());
        assert_eq!(batch.sources.len(), 2);
        assert_eq!(batch.resident_bytes, 20);
        assert_eq!(batch.lane_limit().unwrap(), 10);
    }

    #[test]
    fn catch_up_batch_cannot_outgrow_remaining_builder_capacity() {
        let mut batch = ProjectionBatch::new(100, 25, 4);
        assert!(batch.try_push(prepared(0, 10, 5)).unwrap().is_none());
        assert!(batch.try_push(prepared(1, 10, 5)).unwrap().is_none());
        assert!(batch.try_push(prepared(2, 10, 5)).unwrap().is_some());
        assert_eq!(batch.sources.len(), 2);
        assert!(batch.sources.len() as u64 * batch.lane_limit().unwrap() as u64 <= 25);
    }
}
