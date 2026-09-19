//! Bounded local-source consumer for selectively routed real-time mutations.

use super::*;

use keldra_index::v1::RealtimeOverlayEvidence;
use keldra_store::{JournalRoute, LocalChange, Store};

const REALTIME_RETRY: Duration = Duration::from_millis(250);

pub(super) struct RealtimeLaneTask {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for RealtimeLaneTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl RealtimeLaneTask {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn start(
        local_node: NodeId,
        decisions: DecisionRaft,
        catalog: IndexCatalog,
        store: Store,
        reader: ClusterObjectReader,
        extractor: V1ProjectionExtractor,
        publisher: V1ProjectionPublisher,
        credits: IndexingMemoryCredits,
        limits: Limits,
        cpu: super::super::cpu::IndexCpuPool,
    ) -> Self {
        let mut wake = store.subscribe_realtime_journal_changes();
        let task = tokio::spawn(async move {
            let mut route_cursor = None;
            loop {
                if let Err(error) = reconcile(
                    local_node,
                    &decisions,
                    &catalog,
                    &store,
                    &reader,
                    &extractor,
                    &publisher,
                    &credits,
                    limits,
                    &cpu,
                    &mut route_cursor,
                )
                .await
                {
                    tracing::warn!(%error, "real-time index lane will retry from durable overlay evidence");
                }
                tokio::select! {
                    _ = tokio::time::sleep(REALTIME_RETRY.min(limits.flush_age)) => {},
                    _ = wake.changed() => {},
                }
            }
        });
        Self { task }
    }
}

#[allow(clippy::too_many_arguments)]
async fn reconcile(
    local_node: NodeId,
    decisions: &DecisionRaft,
    catalog: &IndexCatalog,
    store: &Store,
    reader: &ClusterObjectReader,
    extractor: &V1ProjectionExtractor,
    publisher: &V1ProjectionPublisher,
    credits: &IndexingMemoryCredits,
    limits: Limits,
    cpu: &super::super::cpu::IndexCpuPool,
    route_cursor: &mut Option<JournalRoute>,
) -> Result<(), Status> {
    let placement = super::current_placement(decisions)?;
    let placement_fence = placement.fence();
    let status = tokio::task::spawn_blocking({
        let store = store.clone();
        move || store.local_watch_status()
    })
    .await
    .map_err(|error| Status::internal(format!("real-time status task failed: {error}")))?
    .map_err(|error| Status::unavailable(error.to_string()))?;
    if u64::from(status.source_id.node_id) != local_node.0 {
        return Err(Status::failed_precondition(
            "local real-time source node mismatch",
        ));
    }
    let snapshot = catalog.physical_snapshot()?;
    let pending_routes = route_page(store.clone(), status.source_id, *route_cursor).await?;
    *route_cursor = next_route_cursor(&pending_routes);
    let pending_buckets = pending_routes
        .iter()
        .filter_map(|route| match route {
            JournalRoute::Bucket {
                tenant_id,
                bucket_id,
            } => Some((*tenant_id, *bucket_id)),
            JournalRoute::Definition(_) => None,
        })
        .collect::<BTreeSet<_>>();
    for (tenant_id, bucket_id) in pending_buckets.iter().copied() {
        if !snapshot.recipes.iter().any(|recipe| {
            recipe.family.tenant_id == tenant_id && recipe.family.bucket_id == bucket_id
        }) {
            // The catalog snapshot is durable proof that this marker had no
            // active physical family. A future family rebuilds from authority.
            acknowledge_base(
                store.clone(),
                tenant_id,
                bucket_id,
                status.source_id,
                status.settled_through,
            )
            .await?;
        }
    }
    let mut bucket_recipes = BTreeMap::<(u64, u64), Vec<Arc<PhysicalCatalogRecipe>>>::new();
    for recipe in snapshot.recipes.iter() {
        if !pending_buckets.contains(&(recipe.family.tenant_id, recipe.family.bucket_id)) {
            continue;
        }
        bucket_recipes
            .entry((recipe.family.tenant_id, recipe.family.bucket_id))
            .or_default()
            .push(recipe.clone());
    }
    let mut pending = VecDeque::from(bucket_recipes.into_iter().collect::<Vec<_>>());
    let mut active = tokio::task::JoinSet::new();
    while !pending.is_empty() || !active.is_empty() {
        while active.len() < limits.parallelism.max(1) {
            let Some(((tenant_id, bucket_id), recipes)) = pending.pop_front() else {
                break;
            };
            let store = store.clone();
            let reader = reader.clone();
            let extractor = extractor.clone();
            let publisher = publisher.clone();
            let credits = credits.clone();
            let cpu = cpu.clone();
            active.spawn(async move {
                process_bucket(
                    local_node,
                    placement_fence,
                    store,
                    reader,
                    extractor,
                    publisher,
                    credits,
                    limits,
                    status,
                    cpu,
                    tenant_id,
                    bucket_id,
                    recipes,
                )
                .await
            });
        }
        if let Some(joined) = active.join_next().await {
            joined.map_err(|error| {
                Status::internal(format!("real-time bucket lane failed: {error}"))
            })??;
        }
    }
    Ok(())
}

struct RealtimeFamilyContext {
    recipe: Arc<PhysicalCatalogRecipe>,
    partition: ProjectionPartitionIdentity,
    base_covered: u64,
    after: u64,
}

#[allow(clippy::too_many_arguments)]
async fn process_bucket(
    local_node: NodeId,
    placement: keldra_store::PlacementLogId,
    store: Store,
    reader: ClusterObjectReader,
    extractor: V1ProjectionExtractor,
    publisher: V1ProjectionPublisher,
    credits: IndexingMemoryCredits,
    limits: Limits,
    status: keldra_store::WatchJournalStatus,
    cpu: super::super::cpu::IndexCpuPool,
    tenant_id: u64,
    bucket_id: u64,
    recipes: Vec<Arc<PhysicalCatalogRecipe>>,
) -> Result<(), Status> {
    let mut families = Vec::with_capacity(recipes.len());
    for recipe in recipes {
        let Some((directory, _)) = publisher
            .load_family_directory(
                &recipe.storage_tenant,
                &recipe.bucket,
                tenant_id,
                bucket_id,
                recipe.family.family_id,
            )
            .await?
        else {
            return Err(Status::unavailable(
                "real-time family directory is not ready",
            ));
        };
        let partition = directory
            .entries
            .into_iter()
            .map(|entry| entry.partition)
            .find(|partition| {
                partition.source_node == u64::from(status.source_id.node_id)
                    && partition.source_epoch == status.source_id.source_epoch
                    && partition.producer_node == local_node.0
                    && partition.placement_term == placement.term
                    && partition.placement_index == placement.index
            })
            .ok_or_else(|| {
                Status::unavailable("real-time family partition is not assigned locally")
            })?;
        let base = publisher
            .load_current(
                &recipe.storage_tenant,
                &recipe.bucket,
                tenant_id,
                bucket_id,
                partition,
            )
            .await?;
        let base_next = base.as_ref().map_or(0, |value| value.current.next_offset);
        publisher
            .reconcile_realtime_overlay_absorption(
                &recipe.storage_tenant,
                &recipe.bucket,
                tenant_id,
                bucket_id,
                partition,
                recipe.physical_generation,
            )
            .await?;
        let mut compaction_credits = super::empty_query_credits(&credits, limits)?;
        compaction_credits.enter_sealing().map_err(index_status)?;
        let _ = publisher
            .compact_realtime_overlay_runs(
                &cpu,
                &recipe.storage_tenant,
                &recipe.bucket,
                tenant_id,
                bucket_id,
                partition,
                recipe.physical_generation,
                compaction_credits,
            )
            .await?;
        let overlay = publisher
            .load_realtime_overlay(
                &recipe.storage_tenant,
                &recipe.bucket,
                tenant_id,
                bucket_id,
                partition,
            )
            .await?;
        let after = overlay
            .as_ref()
            .and_then(|overlay| {
                overlay
                    .generation
                    .evidence
                    .last()
                    .map(|entry| entry.source_position)
            })
            .unwrap_or_else(|| base_next.saturating_sub(1));
        families.push(RealtimeFamilyContext {
            recipe,
            partition,
            base_covered: base_next.saturating_sub(1),
            after,
        });
    }
    let covered = families
        .iter()
        .map(|family| family.base_covered)
        .min()
        .ok_or_else(|| Status::internal("active real-time bucket has no physical families"))?;
    let after =
        slowest_family_cursor(families.iter().map(|family| family.after)).unwrap_or(covered);
    if after < status.settled_through {
        let page = scan_page(
            store.clone(),
            tenant_id,
            bucket_id,
            status.source_id,
            after,
            status.settled_through,
            realtime_scan_limit(limits.flush_operations),
            u64::try_from(limits.flush_bytes).unwrap_or(u64::MAX),
        )
        .await?;
        if page.oversize.is_some() {
            return Err(Status::resource_exhausted(
                "real-time routed event exceeds lane byte bound",
            ));
        }
        if !page.changes.is_empty() {
            let mut evidence = Vec::with_capacity(page.changes.len());
            let mut mutations = Vec::new();
            for change in page.changes {
                collect_change(status.source_id, change, &mut evidence, &mut mutations)?;
            }
            // Families can diverge after a partial publication failure. Only
            // the slowest shard consumes this page; once caught up, all shards
            // share the next exact-read/selection batch again.
            let selected_recipes = families
                .iter()
                .filter(|family| family.after == after)
                .map(|family| family.recipe.clone())
                .collect::<Vec<_>>();
            let prepared = prepare::prepare_realtime_bucket_fanout(
                selected_recipes,
                mutations,
                &reader,
                &extractor,
                &credits,
                limits,
            )
            .await?;
            for (recipe, prepared) in prepared {
                let family = families
                    .iter()
                    .find(|family| family.recipe.family.family_id == recipe.family.family_id)
                    .ok_or_else(|| Status::internal("prepared unknown real-time family"))?;
                let (query, query_credits, _input_guard) = match prepared {
                    Some(prepared) => (
                        Some(prepared.query),
                        Some(prepared.query_credits),
                        prepared.input_credits,
                    ),
                    None => (None, None, Vec::new()),
                };
                publisher
                    .publish_realtime_overlay_batch(
                        &recipe.storage_tenant,
                        &recipe.bucket,
                        tenant_id,
                        bucket_id,
                        family.partition,
                        recipe.physical_generation,
                        evidence.clone(),
                        query,
                        query_credits,
                    )
                    .await?;
                let mut compaction_credits = super::empty_query_credits(&credits, limits)?;
                compaction_credits.enter_sealing().map_err(index_status)?;
                let _ = publisher
                    .compact_realtime_overlay_runs(
                        &cpu,
                        &recipe.storage_tenant,
                        &recipe.bucket,
                        tenant_id,
                        bucket_id,
                        family.partition,
                        recipe.physical_generation,
                        compaction_credits,
                    )
                    .await?;
            }
        }
    }
    acknowledge_base(store, tenant_id, bucket_id, status.source_id, covered).await
}

fn slowest_family_cursor(cursors: impl IntoIterator<Item = u64>) -> Option<u64> {
    cursors.into_iter().min()
}

fn realtime_scan_limit(flush_operations: u64) -> usize {
    usize::try_from(flush_operations)
        .unwrap_or(usize::MAX)
        .clamp(1, keldra_store::MAX_LOCAL_INVALIDATION_SCAN_RECORDS)
}

fn next_route_cursor(routes: &[JournalRoute]) -> Option<JournalRoute> {
    (routes.len() == 1024).then(|| *routes.last().expect("full route page is non-empty"))
}

fn collect_change(
    event_source: SourceId,
    change: LocalChange,
    evidence: &mut Vec<RealtimeOverlayEvidence>,
    mutations: &mut Vec<(SourceId, Mutation)>,
) -> Result<(), Status> {
    match change {
        LocalChange::ObjectHead(head) if head.program_commit_cursor.is_none() => {
            evidence.push(RealtimeOverlayEvidence {
                source_position: head.offset,
                atomic_position: 0,
                atomic_unit_hash: None,
            });
            mutations.push((event_source, super::head_mutation(head, 0)));
        }
        LocalChange::AtomicBatchPublished(batch) => {
            evidence.push(RealtimeOverlayEvidence {
                source_position: batch.offset,
                atomic_position: batch.cursor,
                atomic_unit_hash: Some(batch.bundle_hash.0),
            });
            for (ordinal, mutation) in batch.mutations.into_iter().enumerate() {
                mutations.push((
                    mutation.source_id,
                    Mutation {
                        // Every gate binds to the indivisible publication event,
                        // while stable document identity uses mutation.source_id.
                        offset: batch.offset,
                        ordinal: u32::try_from(ordinal).map_err(|_| {
                            Status::resource_exhausted("real-time atomic batch is too large")
                        })?,
                        tenant_id: mutation.tenant_id,
                        bucket_id: mutation.bucket_id,
                        path: mutation.exact_path,
                        canonical_path: mutation.canonical_path,
                        version: mutation.path_version.0,
                        deleted: mutation.deleted,
                        atomic_group: Some(batch.cursor),
                        predecessor_absent_at_window_start: false,
                    },
                ));
            }
        }
        other => {
            evidence.push(RealtimeOverlayEvidence {
                source_position: other.offset(),
                atomic_position: 0,
                atomic_unit_hash: None,
            });
        }
    }
    Ok(())
}

async fn scan_page(
    store: Store,
    tenant_id: u64,
    bucket_id: u64,
    source: SourceId,
    after: u64,
    target: u64,
    limit: usize,
    max_bytes: u64,
) -> Result<keldra_store::RoutedLocalChangePage, Status> {
    tokio::task::spawn_blocking(move || {
        store.scan_realtime_routed_local_changes(
            JournalRoute::Bucket {
                tenant_id,
                bucket_id,
            },
            source,
            after,
            target,
            limit,
            max_bytes,
        )
    })
    .await
    .map_err(|error| Status::internal(format!("real-time scan task failed: {error}")))?
    .map_err(|error| Status::unavailable(error.to_string()))
}

async fn acknowledge_base(
    store: Store,
    tenant_id: u64,
    bucket_id: u64,
    source: SourceId,
    through: u64,
) -> Result<(), Status> {
    tokio::task::spawn_blocking(move || {
        store.acknowledge_realtime_base_coverage(
            JournalRoute::Bucket {
                tenant_id,
                bucket_id,
            },
            source,
            through,
        )
    })
    .await
    .map_err(|error| Status::internal(format!("real-time ack task failed: {error}")))?
    .map(|_| ())
    .map_err(|error| Status::unavailable(error.to_string()))
}

async fn route_page(
    store: Store,
    source: SourceId,
    after: Option<JournalRoute>,
) -> Result<Vec<JournalRoute>, Status> {
    tokio::task::spawn_blocking(move || store.realtime_bucket_routes(source, after, 1024))
        .await
        .map_err(|error| {
            Status::internal(format!("real-time route enumeration task failed: {error}"))
        })?
        .map_err(|error| Status::unavailable(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{JournalRoute, next_route_cursor, realtime_scan_limit, slowest_family_cursor};

    #[test]
    fn partial_family_publication_replays_from_the_slowest_cursor() {
        assert_eq!(slowest_family_cursor([41, 37, 41]), Some(37));
        assert_eq!(slowest_family_cursor(std::iter::empty()), None);
    }

    #[test]
    fn route_registry_pagination_retains_only_one_bounded_page() {
        let route = JournalRoute::Bucket {
            tenant_id: 1,
            bucket_id: 2,
        };
        assert_eq!(next_route_cursor(&vec![route; 1024]), Some(route));
        assert_eq!(next_route_cursor(&vec![route; 1023]), None);
    }

    #[test]
    fn realtime_scan_respects_the_store_page_contract() {
        assert_eq!(realtime_scan_limit(0), 1);
        assert_eq!(realtime_scan_limit(17), 17);
        assert_eq!(
            realtime_scan_limit(u64::MAX),
            keldra_store::MAX_LOCAL_INVALIDATION_SCAN_RECORDS
        );
    }
}
