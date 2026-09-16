//! Format-v1 local query execution over one verified family root vector.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use keldra_api::v1::{
    IndexAggregateOperation, IndexAggregateResult, IndexFacetBucket, IndexFacetResult,
    IndexQueryHit, ObjectAddress,
};
use keldra_atomic_program::MAX_OBJECT_PATH_BYTES;
use keldra_consensus::DecisionRaft;
use keldra_index::IndexError;
use keldra_index::typed_json::{AggregateOperation, FieldSchema, FieldType, ScalarValue};
use keldra_index::v1::QuerySnapshotIdentity;
use keldra_index::v1::{
    AuthorizedQueryCandidate, LogicalFieldBinding, LogicalProjectionBinding,
    MAX_QUERY_CANDIDATE_ADMISSION_BATCH, MAX_QUERY_PARTITIONS, PinnedPartitionQueryRoot,
    ProjectionCatalogActivation, ProjectionFamilyPartitionDirectory, ProjectionGenerationHeader,
    ProjectionPartitionIdentity, QueryAdmissionContext, QueryBlockCredits, QueryCandidateAdmission,
    QueryCommonCut, QueryCpuJob, QueryExecutionLimits, QueryFieldBinding, QueryMemoryPermit,
    QueryPartitionExecutor, QueryPartitionJob, QueryPublicValueEncoder, QueryRootCutProof,
    RecipeIdentity, StableDocumentKey, TypedJsonQueryRequest, ValidatedQuerySnapshot,
    decode_projection_generation_header, execute_typed_json_query_with_cursor_and_executor,
    projection_generation_path, query_snapshot_identity, resolve_query_partition_results,
};
use keldra_store::PlacementLogId;
use tonic::Status;

use crate::cluster_peer::{LocalIndexQueryExecutor, LocalIndexQueryRequest};
use crate::cluster_placement::ClusterPlacement;
use crate::index_service::{
    CandidateVisibilityEvidence, ExecutedIndexQuery, IndexCandidateIdentity,
    IndexCandidateVisibility,
};

use super::catalog::{CatalogIdentity, IndexCatalog, PhysicalCatalogRecipe};
use super::cpu::IndexQueryScheduler;
use super::date::format_millis;
use super::query_budget::{IndexQueryMemoryBudget, IndexQueryMemoryPermit};
use super::v1_parallel::run_query_partitions_ordered;
use super::v1_publication::V1ProjectionPublisher;
use super::v1_query_compile::compile_v1_query;

#[path = "v1_query_cursor.rs"]
mod cursor;
#[path = "v1_query_freshness.rs"]
mod query_freshness;
#[path = "v1_query_snapshot_cache.rs"]
mod snapshot_cache;
use cursor::{
    QueryContinuation, QueryPosition, QueryPositionRoot, decode_query_position,
    encode_query_position, normalized_query_binding,
};
use query_freshness::freshness;
use snapshot_cache::V1QuerySnapshotCache;
#[path = "v1_query_artifact_loader.rs"]
mod artifact_loader;
use artifact_loader::RuntimeArtifactLoader;

const _: [(); MAX_OBJECT_PATH_BYTES] = [(); keldra_index::v1::MAX_QUERY_DOCUMENT_PATH_BYTES];

const CONTROL_OBJECT_MAX_BYTES: usize = 256 * 1024;
// The sustained D1 qualification retains slightly more than 8 MiB once
// authorized candidates and decoded run state overlap. Keep the initial lease
// at the next power-of-two bound: the default 512 MiB query share still admits
// all 32 public-query lanes without forcing every query through a failed pass.
const MIN_QUERY_MEMORY_BYTES: u64 = 16 * 1024 * 1024;
// This is a logical-work limit, not a conversion of the memory lease.
const MAX_QUERY_CANDIDATES: usize = 1_000_000;
const MAX_PREDECESSOR_GENERATION_LOADS: usize = 4_096;
#[cfg(test)]
const QUERY_SNAPSHOT_CACHE_BYTES: usize = 64 * 1024 * 1024;

impl QueryMemoryPermit for IndexQueryMemoryPermit {
    fn admitted_bytes(&self) -> usize {
        usize::try_from(self.charged_bytes()).unwrap_or(usize::MAX)
    }
}

#[derive(Clone)]
pub(crate) struct V1LocalIndexQueryExecutor {
    decisions: DecisionRaft,
    catalog: IndexCatalog,
    projections: V1ProjectionPublisher,
    memory: IndexQueryMemoryBudget,
    query_scheduler: IndexQueryScheduler,
    snapshots: V1QuerySnapshotCache,
}

impl V1LocalIndexQueryExecutor {
    pub(crate) fn new(
        decisions: DecisionRaft,
        catalog: IndexCatalog,
        projections: V1ProjectionPublisher,
        memory: IndexQueryMemoryBudget,
        query_scheduler: IndexQueryScheduler,
    ) -> Self {
        let snapshots =
            V1QuerySnapshotCache::with_artifact_cache(projections.immutable_cache().clone());
        Self {
            decisions,
            catalog,
            projections,
            memory,
            query_scheduler,
            snapshots,
        }
    }

    async fn execute(&self, request: LocalIndexQueryRequest) -> Result<ExecutedIndexQuery, Status> {
        require_request(&request)?;
        tracing::debug!("v1 query begins catalog resolution");
        let start_fence = self.placement_fence()?;
        let (logical, recipe, schema, activation) = self.resolve_catalog(&request).await?;
        tracing::debug!("v1 query resolved its active catalog");
        let compiled = compile_v1_query(&schema, &request.query).map_err(index_status)?;
        let query_binding = normalized_query_binding(&logical, &compiled).map_err(index_status)?;
        let natural_page = compiled.order.is_empty()
            && compiled.facets.is_empty()
            && compiled.aggregates.is_empty();
        let continuation = request
            .resume
            .as_ref()
            .map(|cursor| decode_query_position(&cursor.last_position))
            .transpose()?;
        if continuation
            .as_ref()
            .is_some_and(|position| position.query_binding != query_binding)
        {
            return Err(Status::invalid_argument(
                "v1 query cursor belongs to a different normalized query",
            ));
        }
        let resume_after_document =
            match continuation.as_ref().map(|position| &position.continuation) {
                Some(QueryContinuation::Natural(document)) if natural_page => Some(*document),
                Some(QueryContinuation::Explicit(_)) if !natural_page => None,
                Some(_) => {
                    return Err(Status::invalid_argument(
                        "v1 query cursor continuation kind differs from the query order",
                    ));
                }
                None => None,
            };
        let explicit_search_after = continuation.as_ref().and_then(|position| {
            if let QueryContinuation::Explicit(cursor) = &position.continuation {
                Some(cursor)
            } else {
                None
            }
        });
        // Subscribe before the first pin so a Current publication between the
        // read and the wait cannot be lost. The publisher is the authority for
        // root-vector progress; no catalogue/directory polling interval is
        // needed.
        let mut publication_changes = self.projections.subscribe();
        let cached = continuation.as_ref().and_then(|position| {
            self.snapshots.get_for_continuation(
                position.snapshot,
                &logical,
                &activation.catalog_lineage,
                &activation.recipe_catalog_proofs,
                &position.roots,
            )
        });
        let (pinned, mut validated_snapshot) = if let Some(cached) = cached {
            if cached.pinned.cut.through_atomic_position
                != request
                    .resume
                    .as_ref()
                    .expect("continuation exists")
                    .commit_revision
            {
                return Err(Status::invalid_argument(
                    "v1 query cursor snapshot revision is invalid",
                ));
            }
            ((*cached.pinned).clone(), Some(cached.snapshot.clone()))
        } else {
            let pinned = loop {
                let pinned = self
                    .pin_root_vector(&request, Arc::clone(&recipe), continuation.as_ref())
                    .await?;
                if request.resume.is_some()
                    || requirement_is_covered(&pinned, request.required_freshness.as_ref())
                {
                    break pinned;
                }
                if tokio::time::Instant::now() >= request.deadline {
                    return Err(Status::deadline_exceeded(
                        "no v1 root vector reached the required freshness checkpoint",
                    ));
                }
                tokio::select! {
                    _ = publication_changes.recv() => {}
                    () = tokio::time::sleep_until(request.deadline) => {
                        return Err(Status::deadline_exceeded(
                            "no v1 root vector reached the required freshness checkpoint",
                        ));
                    }
                }
            };
            if let Some(expected) = continuation.as_ref().map(|position| position.snapshot)
                && pinned.identity != expected
            {
                return Err(Status::failed_precondition(
                    "v1 query continuation snapshot is no longer retained",
                ));
            }
            (pinned, None)
        };
        if validated_snapshot.is_none() {
            validated_snapshot = self
                .snapshots
                .get_for_pinned(
                    &pinned,
                    &logical,
                    &activation.catalog_lineage,
                    &activation.recipe_catalog_proofs,
                )
                .map_err(index_status)?
                .map(|cached| cached.snapshot.clone());
        }
        tracing::debug!(
            query.partition_count = pinned.roots.len(),
            "v1 query pinned its common-cut root vector"
        );

        let maximum_memory = self.memory.maximum_bounded_lease(u64::MAX);
        let limits = execution_limits(request.limit, maximum_memory)?;
        let query = TypedJsonQueryRequest {
            logical,
            fields: schema
                .fields
                .iter()
                .zip(schema.recipe_fingerprints().map_err(index_status)?.fields)
                .map(|(field, recipe)| {
                    Ok(QueryFieldBinding {
                        field: field.clone(),
                        recipe: RecipeIdentity::new(recipe)?,
                    })
                })
                .collect::<Result<Vec<_>, IndexError>>()
                .map_err(index_status)?,
            catalog_lineage: activation.catalog_lineage.clone(),
            recipe_catalog_proofs: activation.recipe_catalog_proofs.clone(),
            predicate: compiled.predicate,
            order: compiled.order,
            facets: compiled.facets,
            aggregates: compiled.aggregates,
            // Natural order retains one lookahead. Explicit order uses the
            // bounded collector's qualifying-count evidence and exact tuple.
            resume_after_document,
            result_limit: if natural_page {
                request.limit.saturating_add(1)
            } else {
                request.limit
            },
        };
        let mut requested_memory = MIN_QUERY_MEMORY_BYTES.min(maximum_memory);
        let mut retries = 0usize;
        let (result, _credits) = loop {
            tracing::debug!(
                query.memory_bytes = requested_memory,
                query.memory_retry = retries,
                "v1 query begins working-memory admission"
            );
            let memory = self
                .memory
                .acquire_bounded(requested_memory, maximum_memory)
                .await
                .map_err(|error| Status::resource_exhausted(error.to_string()))?;
            tracing::debug!(
                query.memory_bytes = requested_memory,
                "v1 query acquired working-memory admission"
            );
            let mut credits =
                QueryBlockCredits::from_query_permit(Box::new(memory)).map_err(index_status)?;
            let mut loader = RuntimeArtifactLoader::new(
                self.projections.clone(),
                request.storage_tenant.clone(),
                request.definition.bucket.clone(),
                request.tenant_id,
                request.bucket_id,
                self.query_scheduler.clone(),
                request.deadline,
            );
            let mut admission = RuntimeCandidateAdmission {
                visibility: request.candidate_visibility.clone(),
                storage_tenant: request.storage_tenant.clone(),
                bucket: request.definition.bucket.clone(),
                authorization_revision: request.authorization_revision,
            };
            let attempt = execute_typed_json_query_with_cursor_and_executor(
                &mut loader,
                &mut admission,
                pinned.cut,
                &pinned.roots,
                validated_snapshot.clone(),
                &query,
                explicit_search_after,
                &RuntimePublicValueEncoder,
                limits,
                self.projections.query_block_limits(),
                &mut credits,
                &self.query_scheduler,
            )
            .await;
            let required_memory = credits.required_query_lease_bytes();

            match attempt {
                Ok(result) => break (result, credits),
                Err(error)
                    if matches!(&error, IndexError::ResourceLimit { .. })
                        && required_memory.is_some() =>
                {
                    drop(credits);
                    requested_memory = next_query_memory_lease(
                        requested_memory,
                        required_memory.expect("credit exhaustion recorded required bytes"),
                        maximum_memory,
                    )?;
                    retries += 1;
                    tracing::debug!(
                        query.memory_bytes = requested_memory,
                        query.memory_retry = retries,
                        "v1 query will retry with a larger exact memory lease"
                    );
                }
                Err(error) => {
                    drop(credits);
                    return Err(index_status(error));
                }
            }
        };
        let (result, snapshot, explicit_next) = result;
        tracing::debug!("v1 query completed artifact execution and candidate admission");

        self.verify_pin(&request, &recipe, &activation, &pinned)
            .await?;
        if self.placement_fence()? != start_fence {
            return Err(Status::unavailable(
                "index query placement changed during v1 execution",
            ));
        }
        let (page, next_position) = if natural_page {
            let (page, next) = page_candidates(result.candidates, request.limit);
            (page, next.map(QueryContinuation::Natural))
        } else {
            (
                result.candidates,
                explicit_next.map(QueryContinuation::Explicit),
            )
        };
        // A validated root-vector snapshot is useful independently of whether
        // this response needs a continuation. Ordinary one-page queries over
        // the same immutable generation must not reconstruct and revalidate
        // every descriptor. Fresh requests still pin the current root vector
        // above, so a newer generation must match both its root identity and
        // exact generation hashes before it can reuse this entry.
        cache_completed_snapshot(
            &self.snapshots,
            pinned.clone(),
            snapshot.clone(),
            request.resume.is_some(),
            next_position.is_some(),
        );
        let next_position = next_position
            .as_ref()
            .map(|cursor| {
                encode_query_position(snapshot.identity(), query_binding, cursor, &pinned)
            })
            .transpose()?;
        let hits = page
            .into_iter()
            .map(|candidate| IndexQueryHit {
                address: Some(ObjectAddress {
                    tenant: request.storage_tenant.clone(),
                    bucket: request.definition.bucket.clone(),
                    path: candidate.candidate.result_path,
                }),
                object_version: candidate.candidate.result_version,
                score: None,
            })
            .collect();
        let facet_results = result
            .facets
            .into_iter()
            .map(|result| facet_to_api(&schema.fields, result))
            .collect::<Result<Vec<_>, _>>()?;
        let aggregate_results = result
            .aggregates
            .into_iter()
            .map(|result| aggregate_to_api(&schema.fields, result))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ExecutedIndexQuery {
            hits,
            facet_results,
            aggregate_results,
            freshness: freshness(
                &request,
                &pinned,
                start_fence,
                result.through_atomic_position,
                |partition| self.projections.observed_source_next(*partition),
            )?,
            next_position,
        })
    }

    fn placement_fence(&self) -> Result<PlacementLogId, Status> {
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
        ClusterPlacement::from_applied(&state)
            .map(|placement| placement.fence())
            .map_err(|error| Status::unavailable(error.to_string()))
    }

    async fn resolve_catalog(
        &self,
        request: &LocalIndexQueryRequest,
    ) -> Result<
        (
            LogicalProjectionBinding,
            Arc<PhysicalCatalogRecipe>,
            keldra_index::typed_json::TypedJsonSchema,
            ProjectionCatalogActivation,
        ),
        Status,
    > {
        let identity = CatalogIdentity {
            tenant_id: request.tenant_id,
            bucket_id: request.bucket_id,
            index_id: request.definition.index_id,
        };
        let (binding, recipe, contract) = self
            .catalog
            .resolve(identity)?
            .ok_or_else(|| Status::unavailable("logical index is not active in the v1 catalog"))?;
        if binding.object_version != request.definition.version {
            return Err(Status::failed_precondition(
                "logical index catalog revision differs from the authorized definition",
            ));
        }
        let schema = recipe.query_schema(&contract)?;
        let membership = RecipeIdentity::new(recipe.membership_recipe).map_err(index_status)?;
        let fields = contract
            .public_fields
            .iter()
            .enumerate()
            .map(|(ordinal, (name, recipe))| {
                Ok(LogicalFieldBinding {
                    public_field_id: u32::try_from(ordinal).map_err(|_| {
                        IndexError::InvalidDefinition(
                            "logical field catalog exceeds field ID capacity".into(),
                        )
                    })?,
                    public_name: name.clone(),
                    recipe: RecipeIdentity::new(*recipe)?,
                })
            })
            .collect::<Result<Vec<_>, IndexError>>()
            .map_err(index_status)?;
        let logical = LogicalProjectionBinding {
            logical_index_id: request.definition.index_id,
            logical_definition_version: request.definition.version,
            family_id: recipe.family.family_id,
            physical_catalog_generation: recipe.physical_generation,
            membership,
            fields,
        };
        let activation = self
            .projections
            .load_activation(
                &request.storage_tenant,
                &request.definition.bucket,
                request.tenant_id,
                request.bucket_id,
                recipe.family.family_id,
                recipe.physical_generation,
            )
            .await?
            .ok_or_else(|| Status::unavailable("v1 physical catalog is not activated"))?
            .0;
        activation.validate().map_err(index_status)?;
        if activation.family_id != recipe.family.family_id
            || activation.physical_catalog_generation != recipe.physical_generation
        {
            return Err(Status::data_loss(
                "v1 activation does not match the authoritative physical catalog",
            ));
        }
        for recipe in std::iter::once(logical.membership)
            .chain(logical.fields.iter().map(|field| field.recipe))
        {
            if activation
                .recipe_catalog_proofs
                .binary_search_by_key(&recipe, |proof| proof.recipe)
                .is_err()
            {
                return Err(Status::data_loss(
                    "v1 activation lacks a logical query recipe proof",
                ));
            }
        }
        Ok((logical, recipe, schema, activation))
    }

    async fn pin_root_vector(
        &self,
        request: &LocalIndexQueryRequest,
        recipe: Arc<PhysicalCatalogRecipe>,
        continuation: Option<&QueryPosition>,
    ) -> Result<PinnedRootVector, Status> {
        let (directory, directory_version) = self
            .projections
            .load_family_directory(
                &request.storage_tenant,
                &request.definition.bucket,
                request.tenant_id,
                request.bucket_id,
                recipe.family.family_id,
            )
            .await?
            .ok_or_else(|| Status::unavailable("v1 family directory is not published"))?;
        directory.validate().map_err(index_status)?;
        if directory.family_id != recipe.family.family_id {
            return Err(Status::data_loss(
                "v1 family directory does not match the catalog binding",
            ));
        }
        if directory.entries.is_empty() {
            return Err(Status::unavailable("v1 family directory has no partitions"));
        }
        let newest = self
            .load_newest_partition_roots(request, &recipe, &directory)
            .await?;
        let requested_cut = request.resume.as_ref().map(|cursor| cursor.commit_revision);
        let cut = requested_cut.unwrap_or_else(|| {
            newest
                .iter()
                .map(|(_, _, generation)| generation.through_atomic_position)
                .min()
                .unwrap_or(0)
        });
        let common_cut = QueryCommonCut {
            through_atomic_position: cut,
        };
        let exact = if let Some(continuation) = continuation {
            if continuation.roots.len() != newest.len() {
                return Err(Status::failed_precondition(
                    "v1 query continuation root vector no longer matches the family directory",
                ));
            }
            let work = newest
                .into_iter()
                .zip(&continuation.roots)
                .map(|((partition, _, _), exact)| (partition, (partition, exact.clone())))
                .collect();
            let runtime = self.clone();
            let request = request.clone();
            let recipe = recipe.clone();
            let outcomes = run_query_partitions_ordered(
                &self.query_scheduler,
                work,
                move |(partition, exact)| {
                    let runtime = runtime.clone();
                    let request = request.clone();
                    let recipe = recipe.clone();
                    async move {
                        let header = runtime
                            .load_generation_header(
                                &request,
                                &recipe,
                                partition,
                                exact.generation_hash,
                            )
                            .await?;
                        if header.physical_catalog_generation != recipe.physical_generation
                            || header.through_atomic_position > common_cut.through_atomic_position
                        {
                            return Err(Status::failed_precondition(
                                "v1 query continuation generation no longer matches its pinned cut",
                            ));
                        }
                        Ok((
                            pinned_query_root(
                                &recipe,
                                partition,
                                header,
                                common_cut,
                                exact.next_newer_through_atomic_position,
                            ),
                            exact.generation_hash,
                        ))
                    }
                },
            )
            .await?;
            outcomes
                .into_iter()
                .map(|(_, outcome)| outcome)
                .collect::<Result<Vec<_>, Status>>()?
        } else {
            let predecessor_loads = PredecessorLoadBudget::new(MAX_PREDECESSOR_GENERATION_LOADS);
            let work = newest
                .into_iter()
                .map(|(partition, generation_hash, generation)| {
                    (partition, (partition, generation_hash, generation))
                })
                .collect();
            let runtime = self.clone();
            let request = request.clone();
            let recipe = recipe.clone();
            let outcomes = run_query_partitions_ordered(
                &self.query_scheduler,
                work,
                move |(partition, generation_hash, generation)| {
                    let runtime = runtime.clone();
                    let request = request.clone();
                    let recipe = recipe.clone();
                    let predecessor_loads = predecessor_loads.clone();
                    async move {
                        runtime
                            .select_root_at_cut(
                                &request,
                                &recipe,
                                partition,
                                generation_hash,
                                generation,
                                common_cut,
                                &predecessor_loads,
                            )
                            .await
                    }
                },
            )
            .await?;
            outcomes
                .into_iter()
                .map(|(_, outcome)| outcome)
                .collect::<Result<Vec<_>, Status>>()?
        };
        let (roots, generation_hashes): (Vec<PinnedPartitionQueryRoot>, Vec<[u8; 32]>) =
            exact.into_iter().unzip();
        let identity = query_snapshot_identity(common_cut, &roots).map_err(index_status)?;
        Ok(PinnedRootVector {
            memory_lease: keldra_index::v1::SegmentMemoryLease::default(),
            identity,
            cut: common_cut,
            roots,
            generation_hashes,
            directory,
            directory_version,
        })
    }

    async fn load_newest_partition_roots(
        &self,
        request: &LocalIndexQueryRequest,
        recipe: &PhysicalCatalogRecipe,
        directory: &ProjectionFamilyPartitionDirectory,
    ) -> Result<
        Vec<(
            ProjectionPartitionIdentity,
            [u8; 32],
            keldra_index::v1::ProjectionGeneration,
        )>,
        Status,
    > {
        let work =
            bounded_query_partition_work(directory, |entry| (entry.partition, entry.partition))?;
        let projections = self.projections.clone();
        let storage_tenant = request.storage_tenant.clone();
        let bucket = request.definition.bucket.clone();
        let tenant_id = request.tenant_id;
        let bucket_id = request.bucket_id;
        let physical_generation = recipe.physical_generation;
        let outcomes =
            run_query_partitions_ordered(&self.query_scheduler, work, move |partition| {
                let projections = projections.clone();
                let storage_tenant = storage_tenant.clone();
                let bucket = bucket.clone();
                async move {
                    let loaded = projections
                        .load_current(&storage_tenant, &bucket, tenant_id, bucket_id, partition)
                        .await?
                        .ok_or_else(|| Status::unavailable("v1 partition current is absent"))?;
                    if loaded.generation.partition != partition
                        || loaded.generation.physical_catalog_generation != physical_generation
                    {
                        return Err(Status::unavailable(
                            "v1 partition current does not match the active catalog generation",
                        ));
                    }
                    Ok((loaded.current.generation_hash, loaded.generation))
                }
            })
            .await?;
        outcomes
            .into_iter()
            .map(|(partition, outcome)| {
                outcome
                    .map(|(generation_hash, generation)| (partition, generation_hash, generation))
            })
            .collect()
    }

    async fn select_root_at_cut(
        &self,
        request: &LocalIndexQueryRequest,
        recipe: &PhysicalCatalogRecipe,
        partition: ProjectionPartitionIdentity,
        mut generation_hash: [u8; 32],
        generation: keldra_index::v1::ProjectionGeneration,
        cut: QueryCommonCut,
        predecessor_loads: &PredecessorLoadBudget,
    ) -> Result<(PinnedPartitionQueryRoot, [u8; 32]), Status> {
        let mut header = ProjectionGenerationHeader {
            partition: generation.partition,
            physical_catalog_generation: generation.physical_catalog_generation,
            revision: generation.revision,
            next_offset: generation.next_offset,
            through_atomic_position: generation.through_atomic_position,
            query_stream_root: generation.query_stream_root,
            inherited_partitions: generation.inherited_partitions,
            component_directory_root_hash: [1; 32],
            component_root_count: generation.roots.len() as u64,
            previous_generation_hash: generation.previous_generation_hash,
        };
        let mut next_newer = None;
        while header.through_atomic_position > cut.through_atomic_position {
            if tokio::time::Instant::now() >= request.deadline {
                return Err(Status::deadline_exceeded(
                    "v1 predecessor-root pinning exceeded the query deadline",
                ));
            }
            next_newer = Some(header.through_atomic_position);
            let hash = header.previous_generation_hash.ok_or_else(|| {
                Status::failed_precondition("requested v1 query cut is no longer retained")
            })?;
            predecessor_loads.try_admit()?;
            generation_hash = hash;
            let previous = self
                .load_generation_header(request, recipe, partition, hash)
                .await?;
            if previous.revision >= header.revision
                || previous.through_atomic_position > header.through_atomic_position
            {
                return Err(Status::data_loss(
                    "v1 predecessor generation lineage is not strictly ordered",
                ));
            }
            header = previous;
        }
        Ok((
            pinned_query_root(recipe, partition, header, cut, next_newer),
            generation_hash,
        ))
    }

    async fn load_generation_header(
        &self,
        request: &LocalIndexQueryRequest,
        recipe: &PhysicalCatalogRecipe,
        partition: ProjectionPartitionIdentity,
        hash: [u8; 32],
    ) -> Result<ProjectionGenerationHeader, Status> {
        let path = projection_generation_path(partition, hash);
        let bytes = self
            .projections
            .read_object(
                &request.storage_tenant,
                &request.definition.bucket,
                recipe.family.tenant_id,
                recipe.family.bucket_id,
                &path,
                Some(hash),
                CONTROL_OBJECT_MAX_BYTES,
            )
            .await?
            .ok_or_else(|| {
                Status::failed_precondition("requested v1 query generation is no longer retained")
            })?
            .0;
        let header = decode_projection_generation_header(&bytes).map_err(index_status)?;
        if header.partition != partition {
            return Err(Status::data_loss(
                "pinned v1 generation belongs to another partition",
            ));
        }
        Ok(header)
    }

    async fn verify_pin(
        &self,
        request: &LocalIndexQueryRequest,
        recipe: &PhysicalCatalogRecipe,
        activation: &ProjectionCatalogActivation,
        pinned: &PinnedRootVector,
    ) -> Result<(), Status> {
        let current_directory = self
            .projections
            .load_family_directory(
                &request.storage_tenant,
                &request.definition.bucket,
                request.tenant_id,
                request.bucket_id,
                recipe.family.family_id,
            )
            .await?;
        if current_directory.as_ref().map(|value| (&value.0, value.1))
            != Some((&pinned.directory, pinned.directory_version))
        {
            return Err(Status::unavailable(
                "v1 family directory changed during query execution",
            ));
        }
        let current_activation = self
            .projections
            .load_activation(
                &request.storage_tenant,
                &request.definition.bucket,
                request.tenant_id,
                request.bucket_id,
                recipe.family.family_id,
                recipe.physical_generation,
            )
            .await?;
        if current_activation.as_ref().map(|value| &value.0) != Some(activation) {
            return Err(Status::unavailable(
                "v1 catalog activation changed during query execution",
            ));
        }
        let identity = CatalogIdentity {
            tenant_id: request.tenant_id,
            bucket_id: request.bucket_id,
            index_id: request.definition.index_id,
        };
        if !self.catalog.is_current(
            identity,
            request.definition.version,
            recipe.family,
            recipe.physical_generation,
            recipe.membership_recipe,
        )? {
            return Err(Status::unavailable(
                "logical or physical v1 catalog binding changed during query execution",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
struct PredecessorLoadBudget {
    admitted: Arc<AtomicUsize>,
    maximum: usize,
}

impl PredecessorLoadBudget {
    fn new(maximum: usize) -> Self {
        Self {
            admitted: Arc::new(AtomicUsize::new(0)),
            maximum,
        }
    }

    fn try_admit(&self) -> Result<(), Status> {
        self.admitted
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < self.maximum).then_some(current + 1)
            })
            .map(|_| ())
            .map_err(|_| {
                Status::resource_exhausted(format!(
                    "v1 predecessor generation load budget exhausted (maximum {} per root-vector pin)",
                    self.maximum
                ))
            })
    }

    #[cfg(test)]
    fn admitted(&self) -> usize {
        self.admitted.load(Ordering::Acquire)
    }
}

#[tonic::async_trait]
impl LocalIndexQueryExecutor for V1LocalIndexQueryExecutor {
    async fn execute_local(
        &self,
        request: LocalIndexQueryRequest,
    ) -> Result<ExecutedIndexQuery, Status> {
        self.execute(request).await
    }
}

struct PinnedRootVector {
    memory_lease: keldra_index::v1::SegmentMemoryLease,
    identity: QuerySnapshotIdentity,
    cut: QueryCommonCut,
    roots: Vec<PinnedPartitionQueryRoot>,
    generation_hashes: Vec<[u8; 32]>,
    directory: ProjectionFamilyPartitionDirectory,
    directory_version: keldra_store::VersionId,
}

impl Clone for PinnedRootVector {
    fn clone(&self) -> Self {
        Self {
            memory_lease: keldra_index::v1::SegmentMemoryLease::default(),
            identity: self.identity,
            cut: self.cut,
            roots: self.roots.clone(),
            generation_hashes: self.generation_hashes.clone(),
            directory: self.directory.clone(),
            directory_version: self.directory_version,
        }
    }
}

impl PinnedRootVector {
    fn matches_generation(&self, other: &Self) -> bool {
        self.cut == other.cut
            && self.roots == other.roots
            && self.generation_hashes == other.generation_hashes
    }

    fn matches_position_roots(&self, roots: &[QueryPositionRoot]) -> bool {
        self.roots.len() == roots.len()
            && self.generation_hashes.len() == roots.len()
            && self
                .roots
                .iter()
                .zip(&self.generation_hashes)
                .zip(roots)
                .all(|((pinned, generation_hash), position)| {
                    generation_hash == &position.generation_hash
                        && pinned.cut_proof.next_newer_through_atomic_position
                            == position.next_newer_through_atomic_position
                })
    }
}

fn cache_completed_snapshot(
    cache: &V1QuerySnapshotCache,
    pinned: PinnedRootVector,
    snapshot: Arc<ValidatedQuerySnapshot>,
    resumed: bool,
    has_next: bool,
) {
    cache.insert(pinned, snapshot, resumed || has_next);
}

impl QueryPartitionExecutor for IndexQueryScheduler {
    fn maximum_parallelism(&self) -> usize {
        IndexQueryScheduler::maximum_parallelism(self)
    }

    async fn execute_ordered<K, O>(
        &self,
        jobs: Vec<(K, QueryPartitionJob<O>)>,
    ) -> Result<Vec<(K, O)>, IndexError>
    where
        K: Copy + Ord + Send + 'static,
        O: Send + 'static,
    {
        let outcomes = run_query_partitions_ordered(self, jobs, |job| job)
            .await
            .map_err(|error| IndexError::Io(error.to_string()))?;
        resolve_query_partition_results(outcomes)
    }

    async fn run_cpu<O>(&self, job: QueryCpuJob<O>) -> Result<O, IndexError>
    where
        O: Send + 'static,
    {
        IndexQueryScheduler::run_cpu(self, job).await
    }
}

struct RuntimeCandidateAdmission {
    visibility: Arc<dyn IndexCandidateVisibility>,
    storage_tenant: String,
    bucket: String,
    authorization_revision: u64,
}

impl QueryCandidateAdmission for RuntimeCandidateAdmission {
    fn admit_snapshot_current_authorized_batch(
        &mut self,
        contexts: Vec<QueryAdmissionContext>,
    ) -> impl std::future::Future<Output = Result<Vec<Option<AuthorizedQueryCandidate>>, IndexError>>
    + Send {
        async move {
            if contexts.is_empty() || contexts.len() > MAX_QUERY_CANDIDATE_ADMISSION_BATCH {
                return Err(IndexError::InvalidQuery(
                    "v1 candidate admission batch is empty or exceeds its bound".into(),
                ));
            }
            let mut output = Vec::with_capacity(contexts.len());
            let mut contexts = contexts.into_iter();
            loop {
                let batch = contexts
                    .by_ref()
                    .take(MAX_QUERY_CANDIDATE_ADMISSION_BATCH)
                    .collect::<Vec<_>>();
                if batch.is_empty() {
                    break;
                }
                let identities = batch
                    .iter()
                    .map(|context| IndexCandidateIdentity {
                        source_path: context.candidate.source_path.clone(),
                        authorization_source_path: context
                            .candidate
                            .canonical_source_path
                            .as_ref()
                            .unwrap_or(&context.candidate.source_path)
                            .clone(),
                        source_version: context.candidate.current_source_version,
                        result: IndexQueryHit {
                            address: Some(ObjectAddress {
                                tenant: self.storage_tenant.clone(),
                                bucket: self.bucket.clone(),
                                path: context.candidate.result_path.clone(),
                            }),
                            object_version: context.candidate.result_version,
                            score: None,
                        },
                    })
                    .collect::<Vec<_>>();
                let CandidateVisibilityEvidence {
                    visible,
                    authorization_revision,
                    ..
                } = self
                    .visibility
                    .evaluate(&identities)
                    .await
                    .map_err(|error| IndexError::Io(error.to_string()))?;
                if authorization_revision != self.authorization_revision
                    || visible.len() != batch.len()
                {
                    return Err(IndexError::Integrity);
                }
                output.extend(batch.into_iter().zip(visible).map(|(context, visible)| {
                    visible.then(|| AuthorizedQueryCandidate {
                        candidate: context.candidate,
                    })
                }));
            }
            Ok(output)
        }
    }
}

fn require_request(request: &LocalIndexQueryRequest) -> Result<(), Status> {
    if request.authorization_revision == 0
        || request.tenant_id == 0
        || request.bucket_id == 0
        || request.definition.index_id == 0
        || request.definition.version == 0
        || request.limit == 0
    {
        return Err(Status::invalid_argument(
            "local v1 query identity is invalid",
        ));
    }
    if let Some(cursor) = request.resume.as_ref() {
        if cursor.authorization_revision != request.authorization_revision
            || decode_query_position(&cursor.last_position).is_err()
        {
            return Err(Status::invalid_argument(
                "local v1 query cursor identity is invalid",
            ));
        }
    }
    Ok(())
}

fn next_query_memory_lease(current: u64, required: usize, maximum: u64) -> Result<u64, Status> {
    let required = u64::try_from(required).map_err(|_| {
        Status::resource_exhausted("v1 query memory requirement exceeds this platform")
    })?;
    if required > maximum {
        return Err(Status::resource_exhausted(
            "v1 query requires more than the bounded per-query memory maximum",
        ));
    }
    let geometric = current.checked_mul(2).unwrap_or(maximum).min(maximum);
    let next = geometric.max(required);
    if next <= current {
        return Err(Status::internal(
            "v1 query memory retry did not increase its exact lease",
        ));
    }
    Ok(next)
}

fn execution_limits(requested: usize, maximum_memory: u64) -> Result<QueryExecutionLimits, Status> {
    if requested > MAX_QUERY_CANDIDATES {
        return Err(Status::resource_exhausted(
            "v1 query result page exceeds the bounded candidate limit",
        ));
    }
    let maximum_memory = usize::try_from(maximum_memory)
        .map_err(|_| Status::resource_exhausted("v1 query memory maximum exceeds this platform"))?;
    Ok(QueryExecutionLimits {
        maximum_partitions: MAX_QUERY_PARTITIONS,
        maximum_loaded_bytes: maximum_memory,
        maximum_heap_bytes: maximum_memory,
        maximum_candidates: MAX_QUERY_CANDIDATES,
        maximum_results: MAX_QUERY_CANDIDATES,
        ..QueryExecutionLimits::default_for_memory()
    })
}

fn bounded_query_partition_work<T>(
    directory: &ProjectionFamilyPartitionDirectory,
    mut map: impl FnMut(&keldra_index::v1::ProjectionPartitionDirectoryEntry) -> T,
) -> Result<Vec<T>, Status> {
    if directory.entries.len() > MAX_QUERY_PARTITIONS {
        return Err(Status::resource_exhausted(
            "v1 query partition count exceeds the bounded query limit",
        ));
    }
    Ok(directory.entries.iter().map(&mut map).collect())
}

fn handoff_lineage(partition: ProjectionPartitionIdentity) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"keldra.index.v1.handoff-lineage/v1\0");
    hash.update(&partition.family_id);
    hash.update(&partition.source_node.to_be_bytes());
    hash.update(&partition.source_epoch);
    *hash.finalize().as_bytes()
}

fn pinned_query_root(
    recipe: &PhysicalCatalogRecipe,
    partition: ProjectionPartitionIdentity,
    header: ProjectionGenerationHeader,
    cut: QueryCommonCut,
    next_newer_through_atomic_position: Option<u64>,
) -> PinnedPartitionQueryRoot {
    PinnedPartitionQueryRoot {
        partition,
        physical_catalog_generation: recipe.physical_generation,
        root: header.query_stream_root,
        cut_proof: QueryRootCutProof {
            common_cut: cut,
            selected_stream_root_hash: header.query_stream_root.stream_root_hash,
            next_newer_through_atomic_position,
        },
        handoff_lineage_id: handoff_lineage(partition),
    }
}

fn requirement_is_covered(
    pinned: &PinnedRootVector,
    requirement: Option<&crate::index_service::IndexFreshnessRequirement>,
) -> bool {
    let Some(requirement) = requirement else {
        return true;
    };
    requirement
        .atomic_through
        .is_none_or(|required| pinned.cut.through_atomic_position >= required)
        && requirement.sources.iter().all(|required| {
            pinned.roots.iter().any(|root| {
                root.partition.source_node == required.node_id
                    && root.partition.source_epoch == required.source_epoch
                    && root.root.next_offset >= required.next_offset
            })
        })
}

fn page_candidates(
    candidates: Vec<AuthorizedQueryCandidate>,
    limit: usize,
) -> (Vec<AuthorizedQueryCandidate>, Option<StableDocumentKey>) {
    let has_more = candidates.len() > limit;
    let page = candidates.into_iter().take(limit).collect::<Vec<_>>();
    let next = has_more
        .then(|| page.last().map(|candidate| candidate.candidate.document))
        .flatten();
    (page, next)
}

fn facet_to_api(
    fields: &[FieldSchema],
    result: keldra_index::typed_json::FacetResult,
) -> Result<IndexFacetResult, Status> {
    let field = fields
        .get(result.field_id.get() as usize)
        .ok_or_else(|| Status::data_loss("v1 facet names an unknown field"))?;
    let buckets = result
        .buckets
        .into_iter()
        .map(|bucket| {
            Ok(IndexFacetBucket {
                value_json: scalar_json(field, &bucket.value)?,
                count: bucket.count,
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    Ok(IndexFacetResult {
        field: field.name.clone(),
        buckets,
    })
}

struct RuntimePublicValueEncoder;

impl QueryPublicValueEncoder for RuntimePublicValueEncoder {
    fn encode_public_value(
        &self,
        field: &FieldSchema,
        value: &ScalarValue,
    ) -> Result<Vec<u8>, IndexError> {
        scalar_json(field, value).map_err(|_| IndexError::Integrity)
    }
}

fn aggregate_to_api(
    fields: &[FieldSchema],
    result: keldra_index::typed_json::AggregateResult,
) -> Result<IndexAggregateResult, Status> {
    let field = fields
        .get(result.field_id.get() as usize)
        .ok_or_else(|| Status::data_loss("v1 aggregate names an unknown field"))?;
    let operation = match result.operation {
        AggregateOperation::Count => IndexAggregateOperation::Count,
        AggregateOperation::Minimum => IndexAggregateOperation::Minimum,
        AggregateOperation::Maximum => IndexAggregateOperation::Maximum,
        AggregateOperation::Sum => IndexAggregateOperation::Sum,
        AggregateOperation::Average => IndexAggregateOperation::Average,
    };
    Ok(IndexAggregateResult {
        field: field.name.clone(),
        operation: operation as i32,
        value_json: result
            .value
            .as_ref()
            .map(|value| scalar_json(field, value))
            .transpose()?,
        contributing_count: result.contributing_count,
    })
}

fn scalar_json(field: &FieldSchema, value: &ScalarValue) -> Result<Vec<u8>, Status> {
    if field.field_type == FieldType::Date && !matches!(value, ScalarValue::Null) {
        let ScalarValue::Signed(millis) = value else {
            return Err(Status::data_loss(
                "v1 Date result is not signed milliseconds",
            ));
        };
        let rendered = format_millis(
            *millis,
            &field
                .effective_date_format()
                .ok_or_else(|| Status::data_loss("v1 Date field has no format"))?,
        )
        .map_err(|error| Status::data_loss(format!("format v1 Date result: {error}")))?;
        return serde_json::to_vec(&rendered)
            .map_err(|error| Status::internal(format!("encode v1 Date result: {error}")));
    }
    let json = match value {
        ScalarValue::Null => serde_json::Value::Null,
        ScalarValue::Boolean(value) => serde_json::Value::Bool(*value),
        ScalarValue::Signed(value) => serde_json::Value::Number((*value).into()),
        ScalarValue::Unsigned(value) => serde_json::Value::Number((*value).into()),
        ScalarValue::Number(bits) => serde_json::Number::from_f64(f64::from_bits(*bits))
            .map(serde_json::Value::Number)
            .ok_or_else(|| Status::data_loss("v1 query returned a non-finite number"))?,
        ScalarValue::String(value) => serde_json::Value::String(value.clone()),
    };
    serde_json::to_vec(&json)
        .map_err(|error| Status::internal(format!("encode v1 computation result: {error}")))
}

fn index_status(error: IndexError) -> Status {
    match error {
        IndexError::InvalidQuery(message) | IndexError::InvalidDefinition(message) => {
            Status::invalid_argument(message)
        }
        IndexError::ResourceLimit { .. } | IndexError::OffsetOverflow => {
            Status::resource_exhausted(error.to_string())
        }
        IndexError::Io(message) => Status::unavailable(message),
        IndexError::AdmissionDenied(message) => Status::resource_exhausted(message),
        IndexError::DeadlineExceeded => Status::deadline_exceeded(error.to_string()),
        _ => Status::data_loss(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const MAX_QUERY_MEMORY_BYTES: u64 = 256 * 1024 * 1024;
    use keldra_index::v1::{ProjectionQueryStreamRoot, QueryAdmissionCandidate, StableDocumentKey};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn minimum_query_leases_retain_qualification_concurrency() {
        const QUALIFICATION_QUERY_MEMORY_BYTES: u64 = 512 * 1024 * 1024;
        const QUALIFICATION_MAX_IN_FLIGHT: u64 = 32;

        assert_eq!(MIN_QUERY_MEMORY_BYTES, 16 * 1024 * 1024);
        assert!(
            MIN_QUERY_MEMORY_BYTES * QUALIFICATION_MAX_IN_FLIGHT
                <= QUALIFICATION_QUERY_MEMORY_BYTES
        );
    }

    #[test]
    fn execution_limits_are_fixed_independently_of_adaptive_leases() {
        let limits = execution_limits(100, MAX_QUERY_MEMORY_BYTES).unwrap();

        assert_eq!(limits.maximum_partitions, MAX_QUERY_PARTITIONS);
        assert_eq!(limits.maximum_candidates, MAX_QUERY_CANDIDATES);
        assert_eq!(limits.maximum_results, MAX_QUERY_CANDIDATES);
        assert_eq!(limits.maximum_loaded_bytes, MAX_QUERY_MEMORY_BYTES as usize);
        assert_eq!(limits.maximum_heap_bytes, MAX_QUERY_MEMORY_BYTES as usize);

        let mut expanded = limits;
        expanded.maximum_partitions = MAX_QUERY_PARTITIONS + 1;
        assert!(expanded.validate().is_err());
    }

    #[test]
    fn requested_page_cannot_expand_bounded_logical_work() {
        assert!(execution_limits(MAX_QUERY_CANDIDATES, MAX_QUERY_MEMORY_BYTES).is_ok());
        assert!(execution_limits(MAX_QUERY_CANDIDATES + 1, MAX_QUERY_MEMORY_BYTES).is_err());
    }

    #[test]
    fn configured_query_memory_can_exceed_the_old_fixed_ceiling() {
        let configured = 2 * 1024 * 1024 * 1024;
        let budget = IndexQueryMemoryBudget::new(configured).unwrap();
        let maximum = budget.maximum_bounded_lease(u64::MAX);
        let limits = execution_limits(100, maximum).unwrap();
        assert_eq!(maximum, configured);
        assert_eq!(limits.maximum_loaded_bytes, configured as usize);
        assert_eq!(limits.maximum_heap_bytes, configured as usize);
    }

    #[test]
    fn oversized_partition_directory_is_rejected_before_work_is_created() {
        let mut directory = ProjectionFamilyPartitionDirectory {
            family_id: [1; 32],
            revision: 1,
            entries: Vec::with_capacity(MAX_QUERY_PARTITIONS + 1),
        };
        directory.entries.resize_with(MAX_QUERY_PARTITIONS + 1, || {
            keldra_index::v1::ProjectionPartitionDirectoryEntry {
                partition: partition(4, 5),
                lifecycle: keldra_index::v1::ProjectionPartitionLifecycle::Active,
                covered_predecessors: Vec::new(),
            }
        });
        let created = AtomicUsize::new(0);

        let result = bounded_query_partition_work(&directory, |_| {
            created.fetch_add(1, Ordering::Relaxed);
        });

        assert!(result.is_err());
        assert_eq!(created.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn predecessor_load_budget_is_shared_and_does_not_overrun() {
        let budget = PredecessorLoadBudget::new(3);
        let peer = budget.clone();

        budget.try_admit().unwrap();
        peer.try_admit().unwrap();
        budget.try_admit().unwrap();
        let error = peer.try_admit().unwrap_err();

        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert_eq!(budget.admitted(), 3);
        assert_eq!(peer.admitted(), 3);
    }

    #[test]
    fn adaptive_query_lease_grows_geometrically_or_to_required_bytes() {
        assert_eq!(
            next_query_memory_lease(
                16 * 1024 * 1024,
                16 * 1024 * 1024 + 1,
                MAX_QUERY_MEMORY_BYTES,
            )
            .unwrap(),
            32 * 1024 * 1024
        );
        assert_eq!(
            next_query_memory_lease(16 * 1024 * 1024, 40 * 1024 * 1024, MAX_QUERY_MEMORY_BYTES,)
                .unwrap(),
            40 * 1024 * 1024
        );
    }

    #[test]
    fn adaptive_query_growth_is_bounded_by_retry_count_and_memory_maximum() {
        let mut lease = MIN_QUERY_MEMORY_BYTES;
        for _ in 0..4 {
            lease = next_query_memory_lease(lease, (lease + 1) as usize, MAX_QUERY_MEMORY_BYTES)
                .unwrap();
        }
        assert_eq!(lease, MAX_QUERY_MEMORY_BYTES);
        assert_eq!(
            next_query_memory_lease(
                lease,
                (MAX_QUERY_MEMORY_BYTES + 1) as usize,
                MAX_QUERY_MEMORY_BYTES,
            )
            .unwrap_err()
            .code(),
            tonic::Code::ResourceExhausted
        );
    }

    #[test]
    fn adaptive_query_lease_respects_a_smaller_configured_ceiling() {
        let maximum = 8 * 1024 * 1024;
        assert_eq!(MIN_QUERY_MEMORY_BYTES.min(maximum), maximum);
        assert_eq!(
            next_query_memory_lease(maximum, maximum as usize + 1, maximum)
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
    }

    fn partition(producer: u64, placement_index: u64) -> ProjectionPartitionIdentity {
        ProjectionPartitionIdentity::new([1; 32], 7, [2; 32], producer, 3, placement_index).unwrap()
    }

    fn root(partition: ProjectionPartitionIdentity, next_offset: u64) -> PinnedPartitionQueryRoot {
        let root = ProjectionQueryStreamRoot {
            stream_root_hash: [partition.producer_node as u8; 32],
            stream_root_encoded_bytes: 1,
            run_count: 1,
            first_sequence: 1,
            last_sequence: 1,
            source_start_offset: 0,
            next_offset,
            through_atomic_position: 9,
        };
        PinnedPartitionQueryRoot {
            partition,
            physical_catalog_generation: [3; 32],
            root,
            cut_proof: QueryRootCutProof {
                common_cut: QueryCommonCut {
                    through_atomic_position: 9,
                },
                selected_stream_root_hash: root.stream_root_hash,
                next_newer_through_atomic_position: None,
            },
            handoff_lineage_id: handoff_lineage(partition),
        }
    }

    fn authorized(document: u8, path: &str) -> AuthorizedQueryCandidate {
        let candidate = QueryAdmissionCandidate {
            partition: partition(4, 5),
            handoff_lineage_id: [6; 32],
            covered_through_source_position: 8,
            document: StableDocumentKey::from_bytes([document; 32]).unwrap(),
            material_source_version: 10,
            current_source_version: 12,
            source_path: format!("sources/{path}"),
            canonical_source_path: None,
            result_path: format!("results/{path}"),
            result_version: 15,
        };
        AuthorizedQueryCandidate { candidate }
    }

    #[test]
    fn common_cut_freshness_accepts_successor_lineage_without_double_counting() {
        let predecessor = root(partition(4, 5), 8);
        let successor = root(partition(6, 7), 12);
        assert_eq!(predecessor.handoff_lineage_id, successor.handoff_lineage_id);
        let pinned = PinnedRootVector {
            memory_lease: keldra_index::v1::SegmentMemoryLease::default(),
            identity: QuerySnapshotIdentity::from_bytes([9; 32]).unwrap(),
            cut: QueryCommonCut {
                through_atomic_position: 9,
            },
            roots: vec![predecessor, successor],
            generation_hashes: vec![[7; 32], [8; 32]],
            directory: ProjectionFamilyPartitionDirectory {
                family_id: [1; 32],
                revision: 1,
                entries: Vec::new(),
            },
            directory_version: keldra_store::VersionId(1),
        };
        assert!(requirement_is_covered(
            &pinned,
            Some(&crate::index_service::IndexFreshnessRequirement {
                sources: vec![crate::index_service::RequiredIndexSourceCheckpoint {
                    node_id: 7,
                    source_epoch: [2; 32],
                    next_offset: 12,
                }],
                atomic_through: Some(9),
            })
        ));
    }

    #[test]
    fn natural_page_cursor_uses_the_exact_last_returned_document() {
        let values = vec![authorized(1, "a"), authorized(2, "b"), authorized(3, "c")];
        let snapshot = QuerySnapshotIdentity::from_bytes([9; 32]).unwrap();
        let pinned = PinnedRootVector {
            memory_lease: keldra_index::v1::SegmentMemoryLease::default(),
            identity: snapshot,
            cut: QueryCommonCut {
                through_atomic_position: 9,
            },
            roots: vec![root(partition(4, 5), 8)],
            generation_hashes: vec![[7; 32]],
            directory: ProjectionFamilyPartitionDirectory {
                family_id: [1; 32],
                revision: 1,
                entries: Vec::new(),
            },
            directory_version: keldra_store::VersionId(1),
        };
        let (page, next) = page_candidates(values, 1);
        assert_eq!(page[0].candidate.document.bytes(), [1; 32]);
        assert_eq!(next, Some(StableDocumentKey::from_bytes([1; 32]).unwrap()));
        let continuation = QueryContinuation::Natural(next.unwrap());
        let encoded = encode_query_position(snapshot, [6; 32], &continuation, &pinned).unwrap();
        assert_eq!(
            decode_query_position(&encoded).unwrap(),
            QueryPosition {
                snapshot,
                query_binding: [6; 32],
                continuation,
                roots: vec![QueryPositionRoot {
                    generation_hash: [7; 32],
                    next_newer_through_atomic_position: None,
                }],
            }
        );
    }

    #[test]
    fn local_request_accepts_a_genesis_cut_with_an_exact_position() {
        let snapshot = QuerySnapshotIdentity::from_bytes([9; 32]).unwrap();
        let mut genesis_root = root(partition(4, 5), 8);
        genesis_root.root.through_atomic_position = 0;
        genesis_root.cut_proof.common_cut.through_atomic_position = 0;
        let pinned = PinnedRootVector {
            memory_lease: keldra_index::v1::SegmentMemoryLease::default(),
            identity: snapshot,
            cut: QueryCommonCut {
                through_atomic_position: 0,
            },
            roots: vec![genesis_root],
            generation_hashes: vec![[7; 32]],
            directory: ProjectionFamilyPartitionDirectory {
                family_id: [1; 32],
                revision: 1,
                entries: Vec::new(),
            },
            directory_version: keldra_store::VersionId(1),
        };
        let continuation =
            QueryContinuation::Natural(StableDocumentKey::from_bytes([3; 32]).unwrap());
        let request = LocalIndexQueryRequest {
            storage_tenant: "tenant".into(),
            tenant_id: 7,
            bucket_id: 9,
            definition: keldra_api::v1::IndexDefinition {
                index_id: 11,
                version: 13,
                ..Default::default()
            },
            query: Default::default(),
            limit: 100,
            resume: Some(crate::index_service::IndexPageCursor {
                commit_revision: 0,
                last_position: encode_query_position(snapshot, [6; 32], &continuation, &pinned)
                    .unwrap(),
                authorization_revision: 19,
            }),
            candidate_visibility: Arc::new(Visibility),
            authorization_revision: 19,
            required_freshness: None,
            deadline: tokio::time::Instant::now(),
        };

        assert!(require_request(&request).is_ok());
    }

    #[test]
    fn query_position_preserves_exact_generations_and_equal_cut_proofs() {
        let snapshot = QuerySnapshotIdentity::from_bytes([9; 32]).unwrap();
        let mut first = root(partition(4, 5), 8);
        first.cut_proof.next_newer_through_atomic_position = Some(10);
        let second = root(partition(6, 7), 12);
        let pinned = PinnedRootVector {
            memory_lease: keldra_index::v1::SegmentMemoryLease::default(),
            identity: snapshot,
            cut: QueryCommonCut {
                through_atomic_position: 9,
            },
            roots: vec![first, second],
            generation_hashes: vec![[7; 32], [8; 32]],
            directory: ProjectionFamilyPartitionDirectory {
                family_id: [1; 32],
                revision: 1,
                entries: Vec::new(),
            },
            directory_version: keldra_store::VersionId(1),
        };
        let continuation =
            QueryContinuation::Explicit(keldra_index::v1::ExplicitQuerySearchAfter {
                values: vec![None, Some(ScalarValue::Signed(42))],
                document: StableDocumentKey::from_bytes([3; 32]).unwrap(),
            });

        let decoded = decode_query_position(
            &encode_query_position(snapshot, [6; 32], &continuation, &pinned).unwrap(),
        )
        .unwrap();

        assert_eq!(decoded.snapshot, snapshot);
        assert_eq!(decoded.query_binding, [6; 32]);
        assert_eq!(decoded.continuation, continuation);
        assert_eq!(
            decoded.roots,
            vec![
                QueryPositionRoot {
                    generation_hash: [7; 32],
                    next_newer_through_atomic_position: Some(10),
                },
                QueryPositionRoot {
                    generation_hash: [8; 32],
                    next_newer_through_atomic_position: None,
                },
            ]
        );
    }

    #[test]
    fn snapshot_reuse_rejects_a_different_generation_with_the_same_query_root() {
        let pinned = PinnedRootVector {
            memory_lease: keldra_index::v1::SegmentMemoryLease::default(),
            identity: QuerySnapshotIdentity::from_bytes([9; 32]).unwrap(),
            cut: QueryCommonCut {
                through_atomic_position: 9,
            },
            roots: vec![root(partition(4, 5), 8)],
            generation_hashes: vec![[7; 32]],
            directory: ProjectionFamilyPartitionDirectory {
                family_id: [1; 32],
                revision: 1,
                entries: Vec::new(),
            },
            directory_version: keldra_store::VersionId(1),
        };
        let mut republished = pinned.clone();
        republished.generation_hashes[0] = [8; 32];
        let original_position = vec![QueryPositionRoot {
            generation_hash: [7; 32],
            next_newer_through_atomic_position: None,
        }];

        assert!(pinned.matches_generation(&pinned));
        assert!(pinned.matches_position_roots(&original_position));
        assert!(!pinned.matches_generation(&republished));
        assert!(!republished.matches_position_roots(&original_position));
    }

    #[test]
    fn query_position_rejects_truncation_and_trailing_bytes() {
        let snapshot = QuerySnapshotIdentity::from_bytes([9; 32]).unwrap();
        let pinned = PinnedRootVector {
            memory_lease: keldra_index::v1::SegmentMemoryLease::default(),
            identity: snapshot,
            cut: QueryCommonCut {
                through_atomic_position: 9,
            },
            roots: vec![root(partition(4, 5), 8)],
            generation_hashes: vec![[7; 32]],
            directory: ProjectionFamilyPartitionDirectory {
                family_id: [1; 32],
                revision: 1,
                entries: Vec::new(),
            },
            directory_version: keldra_store::VersionId(1),
        };
        let continuation =
            QueryContinuation::Natural(StableDocumentKey::from_bytes([3; 32]).unwrap());
        let mut encoded = encode_query_position(snapshot, [6; 32], &continuation, &pinned).unwrap();
        assert!(decode_query_position(&encoded[..encoded.len() - 1]).is_err());
        encoded.push(0);
        assert!(decode_query_position(&encoded).is_err());
    }

    struct Visibility;

    #[tonic::async_trait]
    impl IndexCandidateVisibility for Visibility {
        async fn evaluate(
            &self,
            candidates: &[IndexCandidateIdentity],
        ) -> Result<CandidateVisibilityEvidence, Status> {
            assert_eq!(candidates.len(), 1);
            assert_eq!(candidates[0].source_path, "sources/a");
            assert_eq!(candidates[0].source_version, 12);
            assert_eq!(
                candidates[0].result.address.as_ref().unwrap().path,
                "results/a"
            );
            assert_eq!(candidates[0].result.object_version, 15);
            Ok(CandidateVisibilityEvidence {
                visible: vec![true],
                authorization_revision: 4,
                denied: 0,
            })
        }
    }

    #[tokio::test]
    async fn admission_uses_snapshot_current_source_and_result_versions() {
        let candidate = authorized(1, "a").candidate;
        let mut admission = RuntimeCandidateAdmission {
            visibility: Arc::new(Visibility),
            storage_tenant: "tenant".into(),
            bucket: "bucket".into(),
            authorization_revision: 4,
        };
        let admitted = admission
            .admit_snapshot_current_authorized_batch(vec![QueryAdmissionContext {
                logical_index_id: 1,
                logical_definition_version: 2,
                common_cut: QueryCommonCut {
                    through_atomic_position: 9,
                },
                candidate,
            }])
            .await
            .unwrap()
            .pop()
            .unwrap();
        let admitted = admitted.unwrap();
        assert_eq!(admitted.candidate.result_path, "results/a");
        assert_eq!(admitted.candidate.result_version, 15);
    }

    struct CanonicalAliasVisibility;

    #[tonic::async_trait]
    impl IndexCandidateVisibility for CanonicalAliasVisibility {
        async fn evaluate(
            &self,
            candidates: &[IndexCandidateIdentity],
        ) -> Result<CandidateVisibilityEvidence, Status> {
            assert_eq!(candidates.len(), 1);
            assert_eq!(candidates[0].source_path, "sources/alias");
            assert_eq!(
                candidates[0].authorization_source_path,
                "_keldra/reserved/target.json"
            );
            assert_eq!(
                candidates[0].result.address.as_ref().unwrap().path,
                "results/alias"
            );
            Ok(CandidateVisibilityEvidence {
                visible: vec![true],
                authorization_revision: 4,
                denied: 0,
            })
        }
    }

    #[tokio::test]
    async fn admission_authorizes_an_alias_by_its_canonical_reserved_target() {
        let mut candidate = authorized(1, "alias").candidate;
        candidate.canonical_source_path = Some("_keldra/reserved/target.json".into());
        let mut admission = RuntimeCandidateAdmission {
            visibility: Arc::new(CanonicalAliasVisibility),
            storage_tenant: "tenant".into(),
            bucket: "bucket".into(),
            authorization_revision: 4,
        };

        let admitted = admission
            .admit_snapshot_current_authorized_batch(vec![QueryAdmissionContext {
                logical_index_id: 1,
                logical_definition_version: 2,
                common_cut: QueryCommonCut {
                    through_atomic_position: 9,
                },
                candidate,
            }])
            .await
            .unwrap()
            .pop()
            .unwrap()
            .unwrap();

        assert_eq!(admitted.candidate.source_path, "sources/alias");
        assert_eq!(admitted.candidate.result_path, "results/alias");
    }

    struct BatchVisibility {
        calls: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl IndexCandidateVisibility for BatchVisibility {
        async fn evaluate(
            &self,
            candidates: &[IndexCandidateIdentity],
        ) -> Result<CandidateVisibilityEvidence, Status> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            assert_eq!(candidates.len(), 64);
            Ok(CandidateVisibilityEvidence {
                visible: (0..candidates.len()).map(|index| index % 3 != 1).collect(),
                authorization_revision: 4,
                denied: 21,
            })
        }
    }

    #[tokio::test]
    async fn admission_batches_sixty_four_candidates_once_and_preserves_denial_alignment() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut admission = RuntimeCandidateAdmission {
            visibility: Arc::new(BatchVisibility {
                calls: calls.clone(),
            }),
            storage_tenant: "tenant".into(),
            bucket: "bucket".into(),
            authorization_revision: 4,
        };
        let contexts = (0..64u8)
            .map(|index| QueryAdmissionContext {
                logical_index_id: 1,
                logical_definition_version: 2,
                common_cut: QueryCommonCut {
                    through_atomic_position: 9,
                },
                candidate: authorized(index + 1, &format!("candidate-{index}")).candidate,
            })
            .collect();
        let admitted = admission
            .admit_snapshot_current_authorized_batch(contexts)
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(admitted.len(), 64);
        for (index, candidate) in admitted.into_iter().enumerate() {
            if index % 3 == 1 {
                assert!(candidate.is_none());
            } else {
                assert_eq!(
                    candidate.unwrap().candidate.document.bytes(),
                    [(index + 1) as u8; 32]
                );
            }
        }
    }

    #[test]
    fn facet_conversion_preserves_the_core_canonical_order() {
        let field = FieldSchema {
            id: keldra_index::typed_json::FieldId::new(0),
            name: "number".into(),
            source_selector: "/number".into(),
            field_type: FieldType::SignedInteger,
            cardinality: keldra_index::typed_json::Cardinality::Single,
            allow_missing: true,
            allow_null: false,
            collation: keldra_index::typed_json::Collation::BinaryUtf8,
            capabilities: keldra_index::typed_json::FieldCapabilities::FACET,
            analyzer: None,
            date_format: None,
        };
        let result = keldra_index::typed_json::FacetResult {
            field_id: field.id,
            buckets: vec![
                keldra_index::typed_json::FacetBucket {
                    value: ScalarValue::Signed(2),
                    count: 1,
                },
                keldra_index::typed_json::FacetBucket {
                    value: ScalarValue::Signed(10),
                    count: 1,
                },
            ],
        };
        let public = facet_to_api(&[field], result).unwrap();
        assert_eq!(public.buckets.len(), 2);
        assert_eq!(public.buckets[0].value_json, b"2");
        assert_eq!(public.buckets[1].value_json, b"10");
    }
}
