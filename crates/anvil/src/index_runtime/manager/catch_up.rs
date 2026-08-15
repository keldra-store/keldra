//! Bounded exact-current journal catch-up.

use anvil_store::{CurrentObjectSnapshot, MAX_OBJECT_RECORD_EXPORT_RECORDS};

use crate::index_runtime::events::{IndexJournalChange, IndexSourceCursor};

use super::rebuild::{
    FetchedProjection, PreparedProjection, ProjectionBatch, fetch_projection_sources,
    partition_projection_lanes, receive_ordered_lane_item, run_projection_lanes,
};
use super::*;

pub(super) struct JournalPageWork {
    pub(super) changed: bool,
    pub(super) source_payload_bytes: u64,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn process_journal_page(
    definition: &CatalogDefinition,
    kind: IndexKind,
    target: &IndexBarrier,
    page: &IndexJournalPage,
    plan: SegmentMemoryPlan,
    builder: &mut NativeSegmentBuild,
    candidate: &mut CandidateGeneration,
    dependencies: &IndexBuilderDependencies,
) -> Result<JournalPageWork, Status> {
    let paths = journal_source_paths(
        definition.tenant_id,
        definition.bucket_id,
        &definition.stored.path_prefix,
        page,
    )
    .into_iter()
    .collect::<Vec<_>>();
    let changed = !paths.is_empty();
    let mut source_payload_bytes = 0_u64;

    // One journal page is already byte-bounded. Exact-current reads retain
    // that bound and additionally obey the store's bounded multi-get limit.
    for paths in paths.chunks(MAX_OBJECT_RECORD_EXPORT_RECORDS as usize) {
        let sources = load_target_sources(definition, paths, target, dependencies).await?;
        source_payload_bytes = sources
            .iter()
            .try_fold(source_payload_bytes, |total, source| {
                total
                    .checked_add(source_payload_bytes_for(&definition.schema, source))
                    .ok_or_else(|| {
                        Status::resource_exhausted("index source payload bytes overflow")
                    })
            })?;
        project_sources(
            definition,
            kind,
            plan,
            sources,
            builder,
            candidate,
            dependencies,
        )
        .await?;
    }
    Ok(JournalPageWork {
        changed,
        source_payload_bytes,
    })
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

/// Concrete heap-resident bytes retained by one decoded journal page while
/// its ordered source mutations are projected. Fixed-size fields live in the
/// vector/map node charges; every variable-capacity field is added explicitly.
pub(super) fn journal_page_resident_bytes(page: &IndexJournalPage) -> Result<u64, Status> {
    let mut bytes = std::mem::size_of::<IndexJournalPage>()
        .checked_add(
            page.changes
                .capacity()
                .checked_mul(std::mem::size_of::<IndexJournalChange>())
                .ok_or_else(|| Status::resource_exhausted("journal page resident overflow"))?,
        )
        .and_then(|bytes| {
            bytes.checked_add(page.through.sources.len().checked_mul(
                std::mem::size_of::<(NodeId, IndexSourceCursor)>()
                    + 3 * std::mem::size_of::<usize>(),
            )?)
        })
        .ok_or_else(|| Status::resource_exhausted("journal page resident overflow"))?;
    for entry in &page.changes {
        let dynamic = match &entry.change {
            LocalChange::ObjectHead(change) => change
                .exact_path
                .capacity()
                .checked_add(
                    change
                        .reference_deltas
                        .capacity()
                        .checked_mul(std::mem::size_of::<anvil_store::ReferenceDelta>())
                        .ok_or_else(|| {
                            Status::resource_exhausted("journal page resident overflow")
                        })?,
                )
                .and_then(|bytes| {
                    bytes.checked_add(
                        change
                            .definition_transition
                            .as_ref()
                            .map_or(0, |transition| transition.path.capacity()),
                    )
                }),
            LocalChange::RetainedVersionDeleted(change) => {
                change.exact_path.capacity().checked_add(
                    change
                        .reference_deltas
                        .capacity()
                        .checked_mul(std::mem::size_of::<anvil_store::ReferenceDelta>())
                        .ok_or_else(|| {
                            Status::resource_exhausted("journal page resident overflow")
                        })?,
                )
            }
            LocalChange::AggregateChanged(change) => Some(change.aggregate_key.capacity()),
            LocalChange::ContentLifecycleChanged(change) => {
                change.blob_identity.capacity().checked_add(
                    change
                        .reference_deltas
                        .capacity()
                        .checked_mul(std::mem::size_of::<anvil_store::ReferenceDelta>())
                        .ok_or_else(|| {
                            Status::resource_exhausted("journal page resident overflow")
                        })?,
                )
            }
            _ => Some(0),
        }
        .ok_or_else(|| Status::resource_exhausted("journal page resident overflow"))?;
        bytes = bytes
            .checked_add(dynamic)
            .ok_or_else(|| Status::resource_exhausted("journal page resident overflow"))?;
    }
    u64::try_from(bytes)
        .map_err(|_| Status::resource_exhausted("journal page resident exceeds u64"))
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
        return Ok(IndexSourceMutation::Remove(ObjectIdentity {
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
        return Ok(IndexSourceMutation::Remove(ObjectIdentity {
            path: path.to_owned(),
            version: version.id.0,
        }));
    }
    Ok(IndexSourceMutation::Upsert(build_object(path, version)?))
}

#[allow(clippy::too_many_arguments)]
async fn project_sources(
    definition: &CatalogDefinition,
    kind: IndexKind,
    plan: SegmentMemoryPlan,
    sources: Vec<IndexSourceMutation>,
    builder: &mut NativeSegmentBuild,
    candidate: &mut CandidateGeneration,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    let configured_lanes = usize::try_from(dependencies.config.projection_max_lanes(kind))
        .map_err(|_| Status::resource_exhausted("projection lane limit exceeds platform"))?;
    let max_lanes = configured_lanes.min(dependencies.cpu.workers()).max(1);
    let projection_budget = plan.max_source_projection_bytes as u64;
    let mut batch = ProjectionBatch::new(projection_budget, max_lanes);
    for source in sources {
        let prepared = PreparedProjection::new(&definition.schema, source)?;
        if let Some(pending) = batch.try_push(prepared)? {
            let full = std::mem::replace(
                &mut batch,
                ProjectionBatch::new(projection_budget, max_lanes),
            );
            project_catch_up_batch(
                definition,
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
    kind: IndexKind,
    plan: SegmentMemoryPlan,
    batch: ProjectionBatch,
    builder: &mut NativeSegmentBuild,
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
    let projection_schema = definition.schema.clone();
    let cpu_task = tokio::spawn(run_projection_lanes(
        cpu,
        lanes,
        senders,
        move |mut fetched: FetchedProjection| {
            let reader = fetched
                .payload
                .as_mut()
                .map(|payload| payload as &mut dyn std::io::Read);
            project_mutation(&projection_schema, fetched.source, reader, lane_limit)
        },
    ));

    let mut failure = None;
    let mut mutations = Vec::with_capacity(source_count);
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
                mutations.push(mutation);
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
    apply_incremental_mutations(
        definition,
        kind,
        plan,
        builder,
        mutations,
        candidate,
        dependencies,
    )
    .await
}

async fn apply_incremental_mutations(
    definition: &CatalogDefinition,
    kind: IndexKind,
    plan: SegmentMemoryPlan,
    builder: &mut NativeSegmentBuild,
    mutations: Vec<MergeMutation>,
    candidate: &mut CandidateGeneration,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    if mutations.windows(2).any(|pair| {
        mutation_identity(&pair[0]).path.as_str() >= mutation_identity(&pair[1]).path.as_str()
    }) {
        return Err(Status::data_loss(
            "catch-up projection did not preserve its sorted unique source order",
        ));
    }
    let mut pending = mutations;

    let conflicts_with_open_segment = pending.iter().any(|mutation| {
        let identity = mutation_identity(mutation);
        builder
            .writer
            .source_version(&identity.path)
            .is_some_and(|version| {
                !matches!(mutation, MergeMutation::Upsert(_)) || version != identity.version
            })
    });
    if conflicts_with_open_segment {
        flush_builder(definition, kind, builder, candidate, dependencies).await?;
    }
    pending.retain(|mutation| {
        let identity = mutation_identity(mutation);
        !matches!(mutation, MergeMutation::Upsert(_))
            || builder.writer.source_version(&identity.path) != Some(identity.version)
    });
    if pending.is_empty() {
        return Ok(());
    }

    let directory = ManifestArtifactDirectory::new(
        dependencies.cache.clone(),
        dependencies.reader.clone(),
        definition.stored.tenant.clone(),
        definition.stored.bucket.clone(),
        definition.tenant_id,
        definition.bucket_id,
        definition.stored.index_id,
    )
    .map_err(index_status)?;
    let roots = candidate.locator_stream_roots()?;
    let mutation_bytes = mutation_batch_resident_bytes(&pending, pending.capacity())?;
    let mut previous_by_ordinal = if roots.is_empty() {
        Vec::new()
    } else {
        // `pending` remains the sole owner of each path. The locator result is
        // ordinal-aligned, so neither request keys nor matched keys are cloned.
        let paths = pending
            .iter()
            .map(|mutation| mutation_identity(mutation).path.as_str())
            .collect::<Vec<_>>();
        let path_reference_bytes =
            borrowed_path_references_resident_bytes(&paths, paths.capacity())?;
        let result_budget = plan
            .max_source_projection_bytes
            .checked_sub(mutation_bytes)
            .and_then(|bytes| bytes.checked_sub(path_reference_bytes))
            .ok_or_else(|| {
                Status::resource_exhausted(
                    "catch-up mutations leave no bounded path-locator workspace",
                )
            })?;
        locate_path_values(&directory, &roots, &paths, result_budget)
            .await
            .map_err(index_status)?
    };
    let mut invalidations = BTreeMap::<u64, Vec<DocIdRange>>::new();
    let mut accepted = Vec::with_capacity(pending.len());
    for (ordinal, mutation) in pending.into_iter().enumerate() {
        let identity = mutation_identity(&mutation);
        let previous = previous_by_ordinal.get_mut(ordinal).and_then(Option::take);
        let Some(previous) = previous else {
            accepted.push(mutation);
            continue;
        };
        if previous.version() > identity.version {
            continue;
        }
        if previous.version() == identity.version {
            let idempotent = matches!(
                (&previous, &mutation),
                (LocatorValue::Live { .. }, MergeMutation::Upsert(_))
                    | (LocatorValue::Deleted { .. }, MergeMutation::Delete(_))
            );
            if idempotent {
                continue;
            }
            return Err(Status::data_loss(
                "format-v4 locator disagrees with a source mutation at the same version",
            ));
        }
        if let LocatorValue::Live { ranges, .. } = previous {
            for range in ranges {
                invalidations
                    .entry(range.segment_id)
                    .or_default()
                    .push(range);
            }
        }
        accepted.push(mutation);
    }

    if !invalidations.is_empty() {
        let routing_codec = definition
            .schema
            .codec_version(anvil_index::v4::ComponentKind::ROUTING_NODE)
            .map_err(index_status)?;
        let mut sink = dependencies.publisher.component_sink(
            &definition.stored,
            definition.tenant_id,
            definition.bucket_id,
            DerivedArtifactAdmission::PublicationProgress,
        );
        for (segment_id, mut ranges) in invalidations {
            let position = candidate
                .segments
                .iter()
                .position(|segment| segment.identity.segment_id == segment_id)
                .ok_or_else(|| {
                    Status::data_loss("format-v4 locator names a missing generation segment")
                })?;
            normalize_invalidation_ranges(segment_id, &mut ranges)?;
            let replacement = rewrite_segment_live_mask(
                &directory,
                &mut sink,
                &candidate.segments[position],
                routing_codec,
                &ranges,
            )
            .await
            .map_err(index_status)?;
            candidate.segments[position] = replacement;
        }
    }

    let mut tombstones = Vec::new();
    for mutation in accepted {
        match mutation {
            MergeMutation::Upsert(source) => {
                push_or_flush(definition, kind, builder, source, candidate, dependencies).await?;
            }
            MergeMutation::Delete(identity) => tombstones.push(LocatorEntry {
                path: identity.path,
                value: LocatorValue::Deleted {
                    tombstone_version: identity.version,
                },
            }),
        }
    }
    if !tombstones.is_empty() {
        let identity = SegmentIdentity::new(
            definition.stored.index_id,
            definition.object_version,
            definition.schema_fingerprint,
            dependencies
                .store
                .allocate_snowflake_id()
                .map_err(|error| Status::internal(format!("allocate locator ID: {error}")))?,
        )
        .map_err(index_status)?;
        let mut sink = dependencies.publisher.component_sink(
            &definition.stored,
            definition.tenant_id,
            definition.bucket_id,
            DerivedArtifactAdmission::PublicationProgress,
        );
        sink.begin_segment(identity, &[]).map_err(index_status)?;
        let published = publish_locator_delta(
            &mut sink,
            identity,
            definition
                .schema
                .codec_version(anvil_index::v4::ComponentKind::PATH_LOCATOR)
                .map_err(index_status)?,
            definition
                .schema
                .codec_version(anvil_index::v4::ComponentKind::ROUTING_NODE)
                .map_err(index_status)?,
            tombstones,
        )
        .await
        .map_err(index_status)?;
        let packs = sink
            .finalize_segment(identity)
            .await
            .map_err(index_status)?;
        let sequence = candidate.allocate_sequence()?;
        candidate.locator_roots.push(LocatorRoot {
            sequence,
            identity,
            artifact: published.root,
            pack_ownership: LocatorPackOwnership::Standalone(packs),
            encoded_bytes: published.encoded_bytes,
            logical_bytes: published.logical_bytes,
        });
        candidate.locator_roots.sort_by_key(|root| root.sequence);
    }
    Ok(())
}

fn normalize_invalidation_ranges(
    segment_id: u64,
    ranges: &mut Vec<DocIdRange>,
) -> Result<(), Status> {
    ranges.sort_by_key(|range| range.first_doc_id.get());
    let mut write = 0usize;
    for read in 0..ranges.len() {
        let current = ranges[read];
        if current.segment_id != segment_id || current.count == 0 {
            return Err(Status::data_loss(
                "path locator returned an invalid live DocId range",
            ));
        }
        let current_end = current
            .first_doc_id
            .get()
            .checked_add(current.count)
            .ok_or_else(|| Status::data_loss("locator DocId range overflow"))?;
        if write != 0 {
            let previous = &mut ranges[write - 1];
            let previous_end = previous
                .first_doc_id
                .get()
                .checked_add(previous.count)
                .ok_or_else(|| Status::data_loss("locator DocId range overflow"))?;
            if current.first_doc_id.get() < previous_end {
                return Err(Status::data_loss(
                    "path locator returned overlapping live DocId ranges",
                ));
            }
            if current.first_doc_id.get() == previous_end {
                previous.count = current_end
                    .checked_sub(previous.first_doc_id.get())
                    .ok_or_else(|| Status::data_loss("locator DocId range underflow"))?;
                continue;
            }
        }
        ranges[write] = current;
        write += 1;
    }
    ranges.truncate(write);
    Ok(())
}

fn mutation_batch_resident_bytes(
    mutations: &[MergeMutation],
    capacity: usize,
) -> Result<usize, Status> {
    let mut bytes = std::mem::size_of::<Vec<MergeMutation>>()
        .checked_add(
            capacity
                .checked_mul(std::mem::size_of::<MergeMutation>())
                .ok_or_else(|| Status::resource_exhausted("catch-up mutation reserve overflow"))?,
        )
        .ok_or_else(|| Status::resource_exhausted("catch-up mutation reserve overflow"))?;
    for mutation in mutations {
        let dynamic = match mutation {
            MergeMutation::Upsert(source) => source
                .resident_bytes()
                .map_err(index_status)?
                .checked_sub(std::mem::size_of::<NativeProjectedSource>())
                .ok_or_else(|| {
                    Status::internal("projected source resident measure omitted its fixed value")
                })?,
            MergeMutation::Delete(identity) => identity.path.capacity(),
        };
        bytes = bytes
            .checked_add(dynamic)
            .ok_or_else(|| Status::resource_exhausted("catch-up mutation reserve overflow"))?;
    }
    Ok(bytes)
}

fn borrowed_path_references_resident_bytes(
    paths: &[&str],
    capacity: usize,
) -> Result<usize, Status> {
    if capacity < paths.len() {
        return Err(Status::internal(
            "borrowed path reference capacity is smaller than its length",
        ));
    }
    std::mem::size_of::<Vec<&str>>()
        .checked_add(
            capacity
                .checked_mul(std::mem::size_of::<&str>())
                .ok_or_else(|| {
                    Status::resource_exhausted("borrowed path reference reserve overflow")
                })?,
        )
        .ok_or_else(|| Status::resource_exhausted("borrowed path reference reserve overflow"))
}

fn mutation_identity(mutation: &MergeMutation) -> &ObjectIdentity {
    match mutation {
        MergeMutation::Upsert(source) => &source.source_identity,
        MergeMutation::Delete(identity) => identity,
    }
}

type ProjectedSource = Result<(MergeMutation, IndexBuildDiagnostics), IndexError>;

#[cfg(test)]
mod tests {
    use super::*;

    fn prepared(index: usize, projection_bytes: u64, resident_bytes: u64) -> PreparedProjection {
        PreparedProjection {
            source: IndexSourceMutation::Remove(ObjectIdentity {
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

    #[test]
    fn invalidation_ranges_merge_adjacency_without_expanding_doc_ids() {
        let mut ranges = vec![
            DocIdRange {
                segment_id: 7,
                first_doc_id: anvil_index::v4::DocId::new(8),
                count: 2,
            },
            DocIdRange {
                segment_id: 7,
                first_doc_id: anvil_index::v4::DocId::new(2),
                count: 3,
            },
            DocIdRange {
                segment_id: 7,
                first_doc_id: anvil_index::v4::DocId::new(5),
                count: 3,
            },
        ];
        normalize_invalidation_ranges(7, &mut ranges).unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].first_doc_id.get(), 2);
        assert_eq!(ranges[0].count, 8);
    }

    #[test]
    fn invalidation_ranges_reject_overlap_as_locator_corruption() {
        let mut ranges = vec![
            DocIdRange {
                segment_id: 7,
                first_doc_id: anvil_index::v4::DocId::new(2),
                count: 4,
            },
            DocIdRange {
                segment_id: 7,
                first_doc_id: anvil_index::v4::DocId::new(5),
                count: 2,
            },
        ];
        assert!(normalize_invalidation_ranges(7, &mut ranges).is_err());
    }

    #[test]
    fn journal_page_resident_measure_charges_decoded_path_capacity() {
        let mut path = String::with_capacity(4 * 1024);
        path.push_str("objects/a");
        let page = IndexJournalPage {
            changes: vec![IndexJournalChange {
                node: NodeId(1),
                change: LocalChange::ObjectHead(anvil_store::ObjectHeadChange {
                    offset: 1,
                    tenant_id: 2,
                    bucket_id: 3,
                    exact_path: path,
                    path_version: VersionId(4),
                    kind: anvil_store::ObjectHeadChangeKind::Put,
                    reference_deltas: Vec::new(),
                    accounting_transition: None,
                    definition_transition: None,
                }),
            }],
            through: IndexBarrier {
                fence: anvil_store::PlacementLogId { term: 1, index: 1 },
                atomic: crate::index_runtime::events::AtomicProgramWatermark::new(None, None, 0),
                sources: BTreeMap::new(),
            },
            encoded_bytes: 1,
        };
        assert!(journal_page_resident_bytes(&page).unwrap() >= 4 * 1024);
    }

    #[test]
    fn catch_up_locator_workspace_charges_mutations_and_borrowed_references_exactly() {
        let mut path = String::with_capacity(4 * 1024);
        path.push_str("objects/a");
        let mut mutations = Vec::with_capacity(7);
        mutations.push(MergeMutation::Delete(ObjectIdentity { path, version: 1 }));
        let mutation_bytes = mutation_batch_resident_bytes(&mutations, mutations.capacity())
            .expect("mutation resident measure");
        assert_eq!(
            mutation_bytes,
            std::mem::size_of::<Vec<MergeMutation>>()
                + mutations.capacity() * std::mem::size_of::<MergeMutation>()
                + 4 * 1024
        );

        let mut references = Vec::with_capacity(5);
        references.push("objects/a");
        references.push("objects/b");
        assert_eq!(
            borrowed_path_references_resident_bytes(&references, references.capacity()).unwrap(),
            std::mem::size_of::<Vec<&str>>() + references.capacity() * std::mem::size_of::<&str>()
        );
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
