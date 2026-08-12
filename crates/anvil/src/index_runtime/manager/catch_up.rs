//! Bounded exact-current journal catch-up.

use anvil_store::{CurrentObjectSnapshot, MAX_OBJECT_RECORD_EXPORT_RECORDS};
use rayon::prelude::*;
use tracing::Instrument;

use super::*;
use crate::cluster_object_read::ClusterReadPayload;

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
    let mut pending = sources.into_iter().collect::<VecDeque<_>>();

    while !pending.is_empty() {
        let mut wave = ProjectionWave::new(plan.max_source_projection_bytes as u64, max_lanes);
        while let Some(source) = pending.pop_front() {
            let prepared = PreparedProjection::new(specification, source)?;
            match wave.try_push(prepared)? {
                Some(pending_source) => {
                    pending.push_front(pending_source.source);
                    break;
                }
                None if wave.is_full() => break,
                None => {}
            }
        }

        let lane_limit = wave.lane_limit()?;
        let projected = project_wave(specification, wave.sources, lane_limit, dependencies).await?;
        for (mutation, diagnostics) in projected {
            candidate.diagnostics.add(diagnostics);
            push_or_flush(
                definition,
                specification,
                kind,
                plan,
                builder,
                mutation,
                candidate,
                dependencies,
            )
            .await?;
        }
    }
    Ok(())
}

struct PreparedProjection {
    source: IndexSourceMutation,
    projection_bytes: u64,
    resident_bytes: u64,
    needs_payload: bool,
}

impl PreparedProjection {
    fn new(
        specification: &IndexSpecification,
        source: IndexSourceMutation,
    ) -> Result<Self, Status> {
        let projection_bytes =
            projection_admission_bytes(specification, &source).map_err(index_status)?;
        let needs_payload =
            source_needs_payload(specification) && matches!(source, IndexSourceMutation::Upsert(_));
        let payload_bytes = match &source {
            IndexSourceMutation::Upsert(object) if needs_payload => object.content_length,
            _ => 0,
        };
        let payload_reserve = if payload_bytes == 0 {
            0
        } else if payload_bytes <= anvil_store::SMALL_BLOB_MAX_BYTES as u64 {
            payload_bytes
                .checked_mul(2)
                .ok_or_else(|| Status::resource_exhausted("inline payload reserve overflow"))?
        } else {
            crate::payload_read::PAYLOAD_READ_FRAME_BYTES as u64
        };
        let resident_bytes = projection_bytes
            .checked_add(payload_reserve)
            .ok_or_else(|| Status::resource_exhausted("projection wave reserve overflow"))?;
        Ok(Self {
            source,
            projection_bytes,
            resident_bytes,
            needs_payload,
        })
    }
}

struct ProjectionWave {
    sources: Vec<PreparedProjection>,
    resident_bytes: u64,
    max_projection_bytes: u64,
    budget: u64,
    max_lanes: usize,
}

impl ProjectionWave {
    fn new(budget: u64, max_lanes: usize) -> Self {
        Self {
            sources: Vec::new(),
            resident_bytes: 0,
            max_projection_bytes: 0,
            budget,
            max_lanes: max_lanes.max(1),
        }
    }

    fn is_full(&self) -> bool {
        self.sources.len() == self.max_lanes
    }

    fn lane_limit(&self) -> Result<usize, Status> {
        let limit = self
            .layout(
                self.sources.len(),
                self.resident_bytes,
                self.max_projection_bytes,
            )
            .ok_or_else(|| Status::resource_exhausted("projection wave exceeds its byte budget"))?;
        usize::try_from(limit)
            .map_err(|_| Status::resource_exhausted("projection lane budget exceeds platform"))
    }

    fn try_push(
        &mut self,
        source: PreparedProjection,
    ) -> Result<Option<PreparedProjection>, Status> {
        if self.is_full() {
            return Ok(Some(source));
        }
        let count = self.sources.len().saturating_add(1);
        let resident = self
            .resident_bytes
            .checked_add(source.resident_bytes)
            .ok_or_else(|| Status::resource_exhausted("projection wave reserve overflow"))?;
        let maximum = self.max_projection_bytes.max(source.projection_bytes);
        if self.layout(count, resident, maximum).is_none() {
            if self.sources.is_empty() {
                return Err(Status::resource_exhausted(format!(
                    "one index source projection cannot fit the {} byte wave budget",
                    self.budget
                )));
            }
            return Ok(Some(source));
        }
        self.resident_bytes = resident;
        self.max_projection_bytes = maximum;
        self.sources.push(source);
        Ok(None)
    }

    fn layout(&self, count: usize, resident: u64, maximum: u64) -> Option<u64> {
        if count == 0 || count > self.max_lanes || resident >= self.budget {
            return None;
        }
        let output_slots = u64::try_from(count).ok()?;
        let lane_limit = self.budget.checked_sub(resident)? / output_slots;
        (lane_limit >= maximum && lane_limit > 0).then_some(lane_limit)
    }
}

type ProjectedSource = (EngineMutation, IndexBuildDiagnostics);

struct FetchedProjection {
    source: IndexSourceMutation,
    payload: Option<ClusterReadPayload>,
}

async fn project_wave(
    specification: &IndexSpecification,
    sources: Vec<PreparedProjection>,
    lane_limit: usize,
    dependencies: &IndexBuilderDependencies,
) -> Result<Vec<ProjectedSource>, Status> {
    let fetched = fetch_projection_wave(sources, dependencies).await?;
    let specification = specification.clone();
    let projected = dependencies
        .cpu
        .install(move || {
            fetched
                .into_par_iter()
                .map(|mut fetched| {
                    let reader = fetched
                        .payload
                        .as_mut()
                        .map(|payload| payload as &mut dyn std::io::Read);
                    project_mutation(&specification, fetched.source, reader, lane_limit)
                })
                .collect::<Vec<_>>()
        })
        .await
        .map_err(cpu_status)?;
    projected
        .into_iter()
        .map(|result| result.map_err(index_status))
        .collect()
}

async fn fetch_projection_wave(
    sources: Vec<PreparedProjection>,
    dependencies: &IndexBuilderDependencies,
) -> Result<Vec<FetchedProjection>, Status> {
    let source_count = sources.len();
    let mut results = std::iter::repeat_with(|| None)
        .take(source_count)
        .collect::<Vec<Option<Result<FetchedProjection, Status>>>>();
    let mut tasks = tokio::task::JoinSet::new();
    for (position, prepared) in sources.into_iter().enumerate() {
        if !prepared.needs_payload {
            results[position] = Some(Ok(FetchedProjection {
                source: prepared.source,
                payload: None,
            }));
            continue;
        }
        let reader = dependencies.reader.clone();
        let span = tracing::Span::current();
        tasks.spawn(
            async move {
                let reference = match &prepared.source {
                    IndexSourceMutation::Upsert(object) => anvil_store::BlobRef {
                        hash: object.content_hash,
                        length: object.content_length,
                    },
                    IndexSourceMutation::Remove(_) => {
                        return (
                            position,
                            Err(Status::internal(
                                "remove projection unexpectedly requested a payload",
                            )),
                        );
                    }
                };
                let payload = reader.open_blob_payload(&reference).await;
                (
                    position,
                    payload.map(|payload| FetchedProjection {
                        source: prepared.source,
                        payload: Some(payload),
                    }),
                )
            }
            .instrument(span),
        );
    }
    while let Some(joined) = tasks.join_next().await {
        let (position, projected) =
            joined.map_err(|error| Status::internal(format!("projection task failed: {error}")))?;
        let slot = results
            .get_mut(position)
            .ok_or_else(|| Status::internal("projection returned an invalid position"))?;
        if slot.replace(projected).is_some() {
            return Err(Status::internal("projection returned a duplicate position"));
        }
    }

    collect_ordered(results)
}

fn collect_ordered<T>(results: Vec<Option<Result<T, Status>>>) -> Result<Vec<T>, Status> {
    let mut ordered = Vec::with_capacity(results.len());
    for result in results {
        ordered.push(result.ok_or_else(|| Status::internal("projection omitted a source"))??);
    }
    Ok(ordered)
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
    fn projection_wave_never_exceeds_its_lane_limit() {
        let mut wave = ProjectionWave::new(1_000, 2);
        assert!(wave.try_push(prepared(0, 10, 10)).unwrap().is_none());
        assert!(wave.try_push(prepared(1, 10, 10)).unwrap().is_none());
        assert!(wave.try_push(prepared(2, 10, 10)).unwrap().is_some());
        assert_eq!(wave.sources.len(), 2);
        assert_eq!(wave.lane_limit().unwrap(), 490);
    }

    #[test]
    fn projection_wave_rejects_work_that_exceeds_its_shared_bytes() {
        let mut wave = ProjectionWave::new(40, 4);
        assert!(wave.try_push(prepared(0, 10, 10)).unwrap().is_none());
        assert!(wave.try_push(prepared(1, 10, 10)).unwrap().is_none());
        assert!(wave.try_push(prepared(2, 10, 10)).unwrap().is_some());
        assert_eq!(wave.sources.len(), 2);
        assert_eq!(wave.lane_limit().unwrap(), 10);
    }

    #[test]
    fn completed_parallel_results_are_applied_in_canonical_positions() {
        let mut completed = vec![None, None, None];
        completed[2] = Some(Ok("third"));
        completed[0] = Some(Ok("first"));
        completed[1] = Some(Ok("second"));

        assert_eq!(
            collect_ordered(completed).unwrap(),
            ["first", "second", "third"]
        );
    }
}
