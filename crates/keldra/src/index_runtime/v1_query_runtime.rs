//! Format-v1 local query execution over one verified family root vector.

use std::collections::BTreeMap;
use std::sync::Arc;

use keldra_api::v1::{
    IndexAggregateOperation, IndexAggregateResult, IndexFacetBucket, IndexFacetResult,
    IndexFreshness, IndexQueryHit, IndexSourceFreshness, ObjectAddress,
};
use keldra_atomic_program::MAX_OBJECT_PATH_BYTES;
use keldra_consensus::DecisionRaft;
use keldra_index::IndexError;
use keldra_index::typed_json::{AggregateOperation, FieldSchema, FieldType, ScalarValue};
use keldra_index::v1::{
    AuthorizedQueryCandidate, LogicalFieldBinding, LogicalProjectionBinding,
    MAX_QUERY_CANDIDATE_ADMISSION_BATCH, PinnedPartitionQueryRoot, ProjectionCatalogActivation,
    ProjectionFamilyPartitionDirectory, ProjectionGenerationHeader, ProjectionPartitionIdentity,
    QueryAdmissionContext, QueryArtifactLoad, QueryArtifactLoader, QueryBlockCredits,
    QueryBlockLimits, QueryCandidateAdmission, QueryCommonCut, QueryExecutionLimits,
    QueryFieldBinding, QueryMemoryPermit, QueryRootCutProof, RecipeIdentity, TypedJsonQueryRequest,
    decode_projection_generation_header, execute_typed_json_query, projection_generation_path,
};
use keldra_store::{BlobRef, PlacementLogId};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::{LocalIndexQueryExecutor, LocalIndexQueryRequest};
use crate::cluster_placement::ClusterPlacement;
use crate::index_service::{
    CandidateVisibilityEvidence, ExecutedIndexQuery, IndexCandidateIdentity,
    IndexCandidateVisibility,
};

use super::catalog::{CatalogIdentity, IndexCatalog, PhysicalCatalogRecipe};
use super::date::format_millis;
use super::query_budget::{IndexQueryMemoryBudget, IndexQueryMemoryPermit};
use super::v1_publication::V1ProjectionPublisher;
use super::v1_query_compile::compile_v1_query;

const _: [(); MAX_OBJECT_PATH_BYTES] = [(); keldra_index::v1::MAX_QUERY_DOCUMENT_PATH_BYTES];

const CONTROL_OBJECT_MAX_BYTES: usize = 256 * 1024;
// The sustained D1 qualification retains slightly more than 8 MiB once
// authorized candidates and decoded run state overlap. Keep the initial lease
// at the next power-of-two bound: the default 512 MiB query share still admits
// all 32 public-query lanes without forcing every query through a failed pass.
const MIN_QUERY_MEMORY_BYTES: u64 = 16 * 1024 * 1024;
const MAX_QUERY_MEMORY_BYTES: u64 = 256 * 1024 * 1024;
// This is a logical-work limit, not a conversion of the memory lease.
const MAX_QUERY_CANDIDATES: usize = 1_000_000;

impl QueryMemoryPermit for IndexQueryMemoryPermit {
    fn admitted_bytes(&self) -> usize {
        usize::try_from(self.charged_bytes()).unwrap_or(usize::MAX)
    }
}

#[derive(Clone)]
pub(crate) struct V1LocalIndexQueryExecutor {
    decisions: DecisionRaft,
    reader: ClusterObjectReader,
    catalog: IndexCatalog,
    projections: V1ProjectionPublisher,
    memory: IndexQueryMemoryBudget,
}

impl V1LocalIndexQueryExecutor {
    pub(crate) fn new(
        decisions: DecisionRaft,
        reader: ClusterObjectReader,
        catalog: IndexCatalog,
        projections: V1ProjectionPublisher,
        memory: IndexQueryMemoryBudget,
    ) -> Self {
        Self {
            decisions,
            reader,
            catalog,
            projections,
            memory,
        }
    }

    async fn execute(&self, request: LocalIndexQueryRequest) -> Result<ExecutedIndexQuery, Status> {
        require_request(&request)?;
        tracing::debug!("v1 query begins catalog resolution");
        let start_fence = self.placement_fence()?;
        let (logical, recipe, schema, activation) = self.resolve_catalog(&request).await?;
        tracing::debug!("v1 query resolved its active catalog");
        let compiled = compile_v1_query(&schema, &request.query).map_err(index_status)?;
        let facet_limits = compiled
            .facets
            .iter()
            .map(|facet| facet.limit)
            .collect::<Vec<_>>();
        // Subscribe before the first pin so a Current publication between the
        // read and the wait cannot be lost. The publisher is the authority for
        // root-vector progress; no catalogue/directory polling interval is
        // needed.
        let mut publication_changes = self.projections.subscribe();
        let pinned = loop {
            let pinned = self.pin_root_vector(&request, &recipe).await?;
            if requirement_is_covered(&pinned, request.required_freshness.as_ref()) {
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
        tracing::debug!(
            query.partition_count = pinned.roots.len(),
            "v1 query pinned its common-cut root vector"
        );

        let limits = execution_limits(pinned.roots.len(), request.limit)?;
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
            // Core scalar ordering is an implementation order, while the
            // public contract breaks equal-count facet buckets by their
            // canonical JSON bytes (including formatted Date strings). Keep
            // the bounded complete bucket set until that public conversion.
            facets: compiled
                .facets
                .into_iter()
                .map(|facet| keldra_index::typed_json::FacetRequest {
                    limit: u32::MAX,
                    ..facet
                })
                .collect(),
            aggregates: compiled.aggregates,
            // The stable document cursor is applied below. Core execution must
            // retain every bounded authorized candidate so continuation never
            // skips an order position hidden by an earlier truncation.
            result_limit: limits.maximum_results,
        };
        let maximum_memory = self.memory.maximum_bounded_lease(MAX_QUERY_MEMORY_BYTES);
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
                .acquire_bounded(requested_memory, MAX_QUERY_MEMORY_BYTES)
                .await
                .map_err(|error| Status::resource_exhausted(error.to_string()))?;
            tracing::debug!(
                query.memory_bytes = requested_memory,
                "v1 query acquired working-memory admission"
            );
            let mut credits =
                QueryBlockCredits::from_query_permit(Box::new(memory)).map_err(index_status)?;
            let mut loader = RuntimeArtifactLoader::new(self.projections.clone());
            let mut admission = RuntimeCandidateAdmission {
                visibility: request.candidate_visibility.clone(),
                storage_tenant: request.storage_tenant.clone(),
                bucket: request.definition.bucket.clone(),
                authorization_revision: request.authorization_revision,
            };
            let attempt = execute_typed_json_query(
                &mut loader,
                &mut admission,
                pinned.cut,
                &pinned.roots,
                &query,
                limits,
                QueryBlockLimits::default_for_memory(),
                &mut credits,
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
        tracing::debug!("v1 query completed artifact execution and candidate admission");

        self.verify_pin(&request, &recipe, &activation, &pinned)
            .await?;
        if self.placement_fence()? != start_fence {
            return Err(Status::unavailable(
                "index query placement changed during v1 execution",
            ));
        }
        let (page, next_position) =
            page_candidates(result.candidates, request.resume.as_ref(), request.limit)?;
        let hits = page
            .into_iter()
            .map(|candidate| IndexQueryHit {
                address: Some(ObjectAddress {
                    tenant: request.storage_tenant.clone(),
                    bucket: request.definition.bucket.clone(),
                    path: candidate.result_path,
                }),
                object_version: candidate.result_version,
                score: None,
            })
            .collect();
        let facet_results = result
            .facets
            .into_iter()
            .zip(facet_limits)
            .map(|(result, limit)| facet_to_api(&schema.fields, result, limit))
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
            PhysicalCatalogRecipe,
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
        recipe: &PhysicalCatalogRecipe,
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
        let mut newest = Vec::with_capacity(directory.entries.len());
        for entry in &directory.entries {
            let loaded = self
                .projections
                .load_current(
                    &request.storage_tenant,
                    &request.definition.bucket,
                    request.tenant_id,
                    request.bucket_id,
                    entry.partition,
                )
                .await?
                .ok_or_else(|| Status::unavailable("v1 partition current is absent"))?;
            if loaded.generation.partition != entry.partition
                || loaded.generation.physical_catalog_generation != recipe.physical_generation
            {
                return Err(Status::unavailable(
                    "v1 partition current does not match the active catalog generation",
                ));
            }
            newest.push((entry.partition, loaded.generation));
        }
        let requested_cut = request.resume.as_ref().map(|cursor| cursor.commit_revision);
        let cut = requested_cut.unwrap_or_else(|| {
            newest
                .iter()
                .map(|(_, generation)| generation.through_atomic_position)
                .min()
                .unwrap_or(0)
        });
        let common_cut = QueryCommonCut {
            through_atomic_position: cut,
        };
        let mut roots = Vec::with_capacity(newest.len());
        for (partition, generation) in newest {
            roots.push(
                self.select_root_at_cut(request, recipe, partition, generation, common_cut)
                    .await?,
            );
        }
        roots.sort_by_key(|root| root.partition);
        Ok(PinnedRootVector {
            cut: common_cut,
            roots,
            directory,
            directory_version,
        })
    }

    async fn select_root_at_cut(
        &self,
        request: &LocalIndexQueryRequest,
        recipe: &PhysicalCatalogRecipe,
        partition: ProjectionPartitionIdentity,
        generation: keldra_index::v1::ProjectionGeneration,
        cut: QueryCommonCut,
    ) -> Result<PinnedPartitionQueryRoot, Status> {
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
        Ok(PinnedPartitionQueryRoot {
            partition,
            physical_catalog_generation: recipe.physical_generation,
            root: header.query_stream_root,
            cut_proof: QueryRootCutProof {
                common_cut: cut,
                selected_stream_root_hash: header.query_stream_root.stream_root_hash,
                next_newer_through_atomic_position: next_newer,
            },
            handoff_lineage_id: handoff_lineage(partition),
        })
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
            .ok_or_else(|| Status::data_loss("pinned v1 generation is absent"))?
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
    cut: QueryCommonCut,
    roots: Vec<PinnedPartitionQueryRoot>,
    directory: ProjectionFamilyPartitionDirectory,
    directory_version: keldra_store::VersionId,
}

struct RuntimeArtifactLoader {
    projections: V1ProjectionPublisher,
}

impl RuntimeArtifactLoader {
    fn new(projections: V1ProjectionPublisher) -> Self {
        Self { projections }
    }
}

impl QueryArtifactLoader for RuntimeArtifactLoader {
    fn load_query_artifact(
        &mut self,
        request: QueryArtifactLoad,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, IndexError>> + Send {
        async move {
            let blob = BlobRef {
                hash: request.hash,
                length: request.encoded_bytes as u64,
            };
            let bytes = self
                .projections
                .read_blob_local_first(&blob, request.encoded_bytes)
                .await
                .map_err(|error| IndexError::Io(error.to_string()))?;
            Ok(bytes)
        }
    }
}

struct RuntimeCandidateAdmission {
    visibility: Arc<dyn IndexCandidateVisibility>,
    storage_tenant: String,
    bucket: String,
    authorization_revision: u64,
}

impl QueryCandidateAdmission for RuntimeCandidateAdmission {
    fn admit_exact_current_authorized_batch(
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
                    visible.then(|| {
                        let result_path = context.candidate.result_path.clone();
                        let result_version = context.candidate.result_version;
                        AuthorizedQueryCandidate {
                            candidate: context.candidate,
                            result_path,
                            result_version,
                        }
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
    if request.resume.as_ref().is_some_and(|cursor| {
        cursor.commit_revision == 0
            || cursor.authorization_revision != request.authorization_revision
            || cursor.last_position.len() != 32
    }) {
        return Err(Status::invalid_argument(
            "local v1 query cursor identity is invalid",
        ));
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
    let geometric = current
        .checked_mul(2)
        .unwrap_or(maximum)
        .min(maximum);
    let next = geometric.max(required);
    if next <= current {
        return Err(Status::internal(
            "v1 query memory retry did not increase its exact lease",
        ));
    }
    Ok(next)
}

fn execution_limits(partitions: usize, requested: usize) -> Result<QueryExecutionLimits, Status> {
    if requested > MAX_QUERY_CANDIDATES {
        return Err(Status::resource_exhausted(
            "v1 query result page exceeds the bounded candidate limit",
        ));
    }
    let maximum_memory = usize::try_from(MAX_QUERY_MEMORY_BYTES)
        .map_err(|_| Status::resource_exhausted("v1 query memory maximum exceeds this platform"))?;
    Ok(QueryExecutionLimits {
        maximum_partitions: partitions.max(1),
        maximum_loaded_bytes: maximum_memory,
        maximum_heap_bytes: maximum_memory,
        maximum_candidates: MAX_QUERY_CANDIDATES,
        maximum_results: MAX_QUERY_CANDIDATES,
        ..QueryExecutionLimits::default_for_memory()
    })
}

fn handoff_lineage(partition: ProjectionPartitionIdentity) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"keldra.index.v1.handoff-lineage/v1\0");
    hash.update(&partition.family_id);
    hash.update(&partition.source_node.to_be_bytes());
    hash.update(&partition.source_epoch);
    *hash.finalize().as_bytes()
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
    resume: Option<&crate::index_service::IndexPageCursor>,
    limit: usize,
) -> Result<(Vec<AuthorizedQueryCandidate>, Option<Vec<u8>>), Status> {
    let start = if let Some(resume) = resume {
        if resume.last_position.len() != 32 {
            return Err(Status::invalid_argument("v1 query cursor is invalid"));
        }
        candidates
            .iter()
            .position(|candidate| candidate.candidate.document.bytes() == resume.last_position[..])
            .map(|position| position + 1)
            .ok_or_else(|| {
                Status::failed_precondition("v1 query cursor position is no longer visible")
            })?
    } else {
        0
    };
    let end = start.saturating_add(limit).min(candidates.len());
    let has_more = end < candidates.len();
    let page = candidates
        .into_iter()
        .skip(start)
        .take(limit)
        .collect::<Vec<_>>();
    let next = has_more
        .then(|| {
            page.last()
                .map(|candidate| candidate.candidate.document.bytes().to_vec())
        })
        .flatten();
    Ok((page, next))
}

fn freshness(
    request: &LocalIndexQueryRequest,
    pinned: &PinnedRootVector,
    fence: PlacementLogId,
    atomic: u64,
) -> Result<IndexFreshness, Status> {
    let mut sources = BTreeMap::new();
    for root in &pinned.roots {
        sources
            .entry((root.partition.source_node, root.partition.source_epoch))
            .and_modify(|offset: &mut u64| *offset = (*offset).max(root.root.next_offset))
            .or_insert(root.root.next_offset);
    }
    Ok(IndexFreshness {
        commit_revision: atomic,
        published_at: None,
        sources: sources
            .into_iter()
            .map(
                |((node_id, source_epoch), indexed_next_offset)| IndexSourceFreshness {
                    node_id,
                    source_epoch: source_epoch.to_vec(),
                    indexed_next_offset,
                    observed_tail: None,
                    lag_hint: 0,
                },
            )
            .collect(),
        initial_build_complete: true,
        rebuilding: false,
        authorization_revision: request.authorization_revision,
        placement_term: fence.term,
        placement_index: fence.index,
        index_id: request.definition.index_id,
        definition_version: request.definition.version,
    })
}

fn facet_to_api(
    fields: &[FieldSchema],
    result: keldra_index::typed_json::FacetResult,
    limit: u32,
) -> Result<IndexFacetResult, Status> {
    let field = fields
        .get(result.field_id.get() as usize)
        .ok_or_else(|| Status::data_loss("v1 facet names an unknown field"))?;
    let mut buckets = result
        .buckets
        .into_iter()
        .map(|bucket| {
            Ok(IndexFacetBucket {
                value_json: scalar_json(field, &bucket.value)?,
                count: bucket.count,
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    buckets.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.value_json.cmp(&right.value_json))
    });
    buckets.truncate(limit as usize);
    Ok(IndexFacetResult {
        field: field.name.clone(),
        buckets,
    })
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
        _ => Status::data_loss(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let limits = execution_limits(3, 100).unwrap();

        assert_eq!(limits.maximum_candidates, MAX_QUERY_CANDIDATES);
        assert_eq!(limits.maximum_results, MAX_QUERY_CANDIDATES);
        assert_eq!(limits.maximum_loaded_bytes, MAX_QUERY_MEMORY_BYTES as usize);
        assert_eq!(limits.maximum_heap_bytes, MAX_QUERY_MEMORY_BYTES as usize);
    }

    #[test]
    fn requested_page_cannot_expand_bounded_logical_work() {
        assert!(execution_limits(1, MAX_QUERY_CANDIDATES).is_ok());
        assert!(execution_limits(1, MAX_QUERY_CANDIDATES + 1).is_err());
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
            next_query_memory_lease(
                16 * 1024 * 1024,
                40 * 1024 * 1024,
                MAX_QUERY_MEMORY_BYTES,
            )
            .unwrap(),
            40 * 1024 * 1024
        );
    }

    #[test]
    fn adaptive_query_growth_is_bounded_by_retry_count_and_memory_maximum() {
        let mut lease = MIN_QUERY_MEMORY_BYTES;
        for _ in 0..4 {
            lease = next_query_memory_lease(
                lease,
                (lease + 1) as usize,
                MAX_QUERY_MEMORY_BYTES,
            )
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
            result_path: format!("results/{path}"),
            result_version: 15,
        };
        AuthorizedQueryCandidate {
            candidate,
            result_path: format!("results/{path}"),
            result_version: 15,
        }
    }

    #[test]
    fn common_cut_freshness_accepts_successor_lineage_without_double_counting() {
        let predecessor = root(partition(4, 5), 8);
        let successor = root(partition(6, 7), 12);
        assert_eq!(predecessor.handoff_lineage_id, successor.handoff_lineage_id);
        let pinned = PinnedRootVector {
            cut: QueryCommonCut {
                through_atomic_position: 9,
            },
            roots: vec![predecessor, successor],
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
    fn result_cursor_resumes_after_the_exact_stable_document() {
        let values = vec![authorized(1, "a"), authorized(2, "b"), authorized(3, "c")];
        let resume = crate::index_service::IndexPageCursor {
            commit_revision: 9,
            last_position: vec![1; 32],
            authorization_revision: 4,
        };
        let (page, next) = page_candidates(values, Some(&resume), 1).unwrap();
        assert_eq!(page[0].candidate.document.bytes(), [2; 32]);
        assert_eq!(next, Some(vec![2; 32]));
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
                stale: 0,
            })
        }
    }

    #[tokio::test]
    async fn admission_uses_exact_current_source_and_result_not_material_version() {
        let candidate = authorized(1, "a").candidate;
        let mut admission = RuntimeCandidateAdmission {
            visibility: Arc::new(Visibility),
            storage_tenant: "tenant".into(),
            bucket: "bucket".into(),
            authorization_revision: 4,
        };
        let admitted = admission
            .admit_exact_current_authorized_batch(vec![QueryAdmissionContext {
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
        assert_eq!(admitted.result_path, "results/a");
        assert_eq!(admitted.result_version, 15);
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
                stale: 0,
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
            .admit_exact_current_authorized_batch(contexts)
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
    fn facet_limit_is_applied_after_public_canonical_byte_ordering() {
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
        let public = facet_to_api(&[field], result, 1).unwrap();
        assert_eq!(public.buckets.len(), 1);
        assert_eq!(public.buckets[0].value_json, b"10");
    }
}
