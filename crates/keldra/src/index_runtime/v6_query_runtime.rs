//! Format-v6 local query execution over one verified family root vector.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use keldra_api::v1::{
    IndexAggregateOperation, IndexAggregateResult, IndexFacetBucket, IndexFacetResult,
    IndexFreshness, IndexQueryHit, IndexSourceFreshness, ObjectAddress,
};
use keldra_atomic_program::MAX_OBJECT_PATH_BYTES;
use keldra_consensus::DecisionRaft;
use keldra_index::IndexError;
use keldra_index::typed_json::{AggregateOperation, FieldSchema, FieldType, ScalarValue};
use keldra_index::v6::{
    AuthorizedQueryCandidate, LogicalFieldBinding, LogicalProjectionBinding,
    MAX_QUERY_CANDIDATE_ADMISSION_BATCH, PinnedPartitionQueryRoot, ProjectionCatalogActivation,
    ProjectionFamilyPartitionDirectory, ProjectionGenerationHeader, ProjectionPartitionIdentity,
    QueryAdmissionContext, QueryArtifactKind, QueryArtifactLoad, QueryArtifactLoader,
    QueryBlockCredits, QueryBlockLimits, QueryCandidateAdmission, QueryCommonCut,
    QueryExecutionLimits, QueryFieldBinding, QueryMemoryPermit, QueryRootCutProof, RecipeIdentity,
    TypedJsonQueryRequest, decode_projection_generation_header, execute_typed_json_query,
    projection_generation_path, projection_query_run_pack_path,
    projection_query_run_stream_page_path,
};
use keldra_store::{BlobRef, ObjectKey, PlacementLogId};
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
use super::v6_publication::V6ProjectionPublisher;
use super::v6_query_compile::compile_v6_query;

const _: [(); MAX_OBJECT_PATH_BYTES] = [(); keldra_index::v6::MAX_QUERY_DOCUMENT_PATH_BYTES];

const CONTROL_OBJECT_MAX_BYTES: usize = 256 * 1024;
const MIN_QUERY_MEMORY_BYTES: u64 = 2 * 1024 * 1024;
const PREFERRED_QUERY_MEMORY_BYTES: u64 = 256 * 1024 * 1024;
const FRESHNESS_POLL: Duration = Duration::from_millis(25);

impl QueryMemoryPermit for IndexQueryMemoryPermit {
    fn admitted_bytes(&self) -> usize {
        usize::try_from(self.charged_bytes()).unwrap_or(usize::MAX)
    }
}

#[derive(Clone)]
pub(crate) struct V6LocalIndexQueryExecutor {
    decisions: DecisionRaft,
    reader: ClusterObjectReader,
    catalog: IndexCatalog,
    projections: V6ProjectionPublisher,
    memory: IndexQueryMemoryBudget,
}

impl V6LocalIndexQueryExecutor {
    pub(crate) fn new(
        decisions: DecisionRaft,
        reader: ClusterObjectReader,
        catalog: IndexCatalog,
        projections: V6ProjectionPublisher,
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
        tracing::debug!("v6 query begins catalog resolution");
        let start_fence = self.placement_fence()?;
        let (logical, recipe, schema, activation) = self.resolve_catalog(&request).await?;
        tracing::debug!("v6 query resolved its active catalog");
        let compiled = compile_v6_query(&schema, &request.query).map_err(index_status)?;
        let facet_limits = compiled
            .facets
            .iter()
            .map(|facet| facet.limit)
            .collect::<Vec<_>>();
        let mut pinned = loop {
            let pinned = self.pin_root_vector(&request, &recipe, &activation).await?;
            if requirement_is_covered(&pinned, request.required_freshness.as_ref()) {
                break pinned;
            }
            if tokio::time::Instant::now() >= request.deadline {
                return Err(Status::deadline_exceeded(
                    "no v6 root vector reached the required freshness checkpoint",
                ));
            }
            tokio::time::sleep_until(
                (tokio::time::Instant::now() + FRESHNESS_POLL).min(request.deadline),
            )
            .await;
        };
        tracing::debug!(
            query.partition_count = pinned.roots.len(),
            "v6 query pinned its common-cut root vector"
        );

        tracing::debug!("v6 query begins working-memory admission");
        let memory = self
            .memory
            .acquire_up_to(MIN_QUERY_MEMORY_BYTES, PREFERRED_QUERY_MEMORY_BYTES)
            .await
            .map_err(|error| Status::resource_exhausted(error.to_string()))?;
        tracing::debug!("v6 query acquired working-memory admission");
        let admitted = usize::try_from(memory.charged_bytes()).unwrap_or(usize::MAX);
        let mut credits =
            QueryBlockCredits::from_query_permit(Box::new(memory)).map_err(index_status)?;
        let limits = execution_limits(admitted, pinned.roots.len(), request.limit)?;
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
        let mut loader = RuntimeArtifactLoader::new(
            self.reader.clone(),
            self.projections.clone(),
            request.storage_tenant.clone(),
            request.definition.bucket.clone(),
            request.tenant_id,
            request.bucket_id,
            recipe.family.family_id,
        );
        let mut admission = RuntimeCandidateAdmission {
            visibility: request.candidate_visibility.clone(),
            storage_tenant: request.storage_tenant.clone(),
            bucket: request.definition.bucket.clone(),
            authorization_revision: request.authorization_revision,
        };
        let result = execute_typed_json_query(
            &mut loader,
            &mut admission,
            pinned.cut,
            &pinned.roots,
            &query,
            limits,
            QueryBlockLimits::default_for_memory(),
            &mut credits,
        )
        .await
        .map_err(index_status)?;
        tracing::debug!("v6 query completed artifact execution and candidate admission");

        self.verify_pin(&request, &recipe, &activation, &pinned)
            .await?;
        if self.placement_fence()? != start_fence {
            return Err(Status::unavailable(
                "index query placement changed during v6 execution",
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
        let specification = request
            .definition
            .specification
            .as_ref()
            .ok_or_else(|| Status::data_loss("index definition has no specification"))?;
        let schema = super::typed_json_schema::compile_typed_json_schema(
            &request.definition.path_prefix,
            (!request.definition.content_type.is_empty())
                .then_some(request.definition.content_type.as_str()),
            specification,
        )
        .map_err(index_status)?;
        let recipes = schema.recipe_fingerprints().map_err(index_status)?;
        let (_, _, bindings, physical, contracts) = self.catalog.snapshot()?;
        let identity = CatalogIdentity {
            tenant_id: request.tenant_id,
            bucket_id: request.bucket_id,
            index_id: request.definition.index_id,
        };
        let binding = bindings
            .into_iter()
            .find(|binding| binding.identity == identity)
            .ok_or_else(|| Status::unavailable("logical index is not active in the v6 catalog"))?;
        if binding.object_version != request.definition.version {
            return Err(Status::failed_precondition(
                "logical index catalog revision differs from the authorized definition",
            ));
        }
        let recipe = physical
            .into_iter()
            .find(|recipe| recipe.family == binding.family)
            .ok_or_else(|| Status::data_loss("logical index has no physical v6 family"))?;
        if recipe.membership_recipe != recipes.membership {
            return Err(Status::unavailable(
                "logical index physical catalog is changing",
            ));
        }
        let contract = contracts
            .into_iter()
            .find(|contract| contract.identity == binding.query_contract)
            .ok_or_else(|| Status::data_loss("logical index has no query contract"))?;
        let expected_fields = schema
            .fields
            .iter()
            .zip(&recipes.fields)
            .map(|(field, recipe)| (field.name.clone(), *recipe))
            .collect::<Vec<_>>();
        if contract.public_fields.as_ref() != &expected_fields {
            return Err(Status::data_loss(
                "logical index query contract differs from its definition",
            ));
        }
        let membership = RecipeIdentity::new(recipes.membership).map_err(index_status)?;
        let fields = schema
            .fields
            .iter()
            .zip(recipes.fields)
            .map(|(field, recipe)| {
                Ok(LogicalFieldBinding {
                    public_field_id: field.id.get(),
                    public_name: field.name.clone(),
                    recipe: RecipeIdentity::new(recipe)?,
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
            .ok_or_else(|| Status::unavailable("v6 physical catalog is not activated"))?
            .0;
        activation.validate().map_err(index_status)?;
        if activation.family_id != recipe.family.family_id
            || activation.physical_catalog_generation != recipe.physical_generation
        {
            return Err(Status::data_loss(
                "v6 activation does not match the authoritative physical catalog",
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
                    "v6 activation lacks a logical query recipe proof",
                ));
            }
        }
        Ok((logical, recipe, schema, activation))
    }

    async fn pin_root_vector(
        &self,
        request: &LocalIndexQueryRequest,
        recipe: &PhysicalCatalogRecipe,
        activation: &ProjectionCatalogActivation,
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
            .ok_or_else(|| Status::unavailable("v6 family directory is not published"))?;
        directory.validate().map_err(index_status)?;
        if directory.family_id != recipe.family.family_id {
            return Err(Status::data_loss(
                "v6 family directory does not match the catalog binding",
            ));
        }
        if directory.entries.is_empty() {
            return Err(Status::unavailable("v6 family directory has no partitions"));
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
                .ok_or_else(|| Status::unavailable("v6 partition current is absent"))?;
            if loaded.generation.partition != entry.partition
                || loaded.generation.physical_catalog_generation != recipe.physical_generation
            {
                return Err(Status::unavailable(
                    "v6 partition current does not match the active catalog generation",
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
        generation: keldra_index::v6::ProjectionGeneration,
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
                    "v6 predecessor-root pinning exceeded the query deadline",
                ));
            }
            next_newer = Some(header.through_atomic_position);
            let hash = header.previous_generation_hash.ok_or_else(|| {
                Status::failed_precondition("requested v6 query cut is no longer retained")
            })?;
            let previous = self
                .load_generation_header(request, recipe, partition, hash)
                .await?;
            if previous.revision >= header.revision
                || previous.through_atomic_position > header.through_atomic_position
            {
                return Err(Status::data_loss(
                    "v6 predecessor generation lineage is not strictly ordered",
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
            .ok_or_else(|| Status::data_loss("pinned v6 generation is absent"))?
            .0;
        let header = decode_projection_generation_header(&bytes).map_err(index_status)?;
        if header.partition != partition {
            return Err(Status::data_loss(
                "pinned v6 generation belongs to another partition",
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
                "v6 family directory changed during query execution",
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
                "v6 catalog activation changed during query execution",
            ));
        }
        let (_, _, bindings, physical, _) = self.catalog.snapshot()?;
        let identity = CatalogIdentity {
            tenant_id: request.tenant_id,
            bucket_id: request.bucket_id,
            index_id: request.definition.index_id,
        };
        let binding_is_current = bindings.into_iter().any(|binding| {
            binding.identity == identity
                && binding.object_version == request.definition.version
                && binding.family == recipe.family
        });
        let physical_is_current = physical.into_iter().any(|current| {
            current.family == recipe.family
                && current.physical_generation == recipe.physical_generation
                && current.membership_recipe == recipe.membership_recipe
        });
        if !binding_is_current || !physical_is_current {
            return Err(Status::unavailable(
                "logical or physical v6 catalog binding changed during query execution",
            ));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl LocalIndexQueryExecutor for V6LocalIndexQueryExecutor {
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
    reader: ClusterObjectReader,
    projections: V6ProjectionPublisher,
    storage_tenant: String,
    bucket: String,
    tenant_id: u64,
    bucket_id: u64,
    family_id: [u8; 32],
    admitted: BTreeMap<(QueryArtifactKind, [u8; 32]), BlobRef>,
}

impl RuntimeArtifactLoader {
    fn new(
        reader: ClusterObjectReader,
        projections: V6ProjectionPublisher,
        storage_tenant: String,
        bucket: String,
        tenant_id: u64,
        bucket_id: u64,
        family_id: [u8; 32],
    ) -> Self {
        Self {
            reader,
            projections,
            storage_tenant,
            bucket,
            tenant_id,
            bucket_id,
            family_id,
            admitted: BTreeMap::new(),
        }
    }

    fn path(&self, kind: QueryArtifactKind, hash: [u8; 32]) -> Result<String, IndexError> {
        let partition = ProjectionPartitionIdentity::new(self.family_id, 1, [1; 32], 1, 1, 1)?;
        Ok(match kind {
            QueryArtifactKind::Page => projection_query_run_stream_page_path(partition, hash),
            QueryArtifactKind::Run | QueryArtifactKind::Block => {
                projection_query_run_pack_path(partition, hash)
            }
        })
    }
}

impl QueryArtifactLoader for RuntimeArtifactLoader {
    fn query_artifact_size(
        &mut self,
        kind: QueryArtifactKind,
        hash: [u8; 32],
    ) -> impl std::future::Future<Output = Result<usize, IndexError>> + Send {
        async move {
            let path = self.path(kind, hash)?;
            let key = ObjectKey::new(&self.storage_tenant, &self.bucket, &path)
                .map_err(|error| IndexError::Io(error.to_string()))?;
            let version = self
                .reader
                .head_stable(&key, self.tenant_id, self.bucket_id)
                .await
                .map_err(|error| IndexError::Io(error.to_string()))?
                .ok_or(IndexError::Integrity)?;
            let blob = version.blob.ok_or(IndexError::Integrity)?;
            if version.deleted || blob.hash != hash {
                return Err(IndexError::Integrity);
            }
            let bytes = usize::try_from(blob.length).map_err(|_| IndexError::OffsetOverflow)?;
            self.admitted.insert((kind, hash), blob);
            Ok(bytes)
        }
    }

    fn load_query_artifact(
        &mut self,
        request: QueryArtifactLoad,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, IndexError>> + Send {
        async move {
            let blob = self
                .admitted
                .remove(&(request.kind, request.hash))
                .ok_or(IndexError::Integrity)?;
            if blob.length != request.encoded_bytes as u64 {
                return Err(IndexError::Integrity);
            }
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
                    "v6 candidate admission batch is empty or exceeds its bound".into(),
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
            "local v6 query identity is invalid",
        ));
    }
    if request.resume.as_ref().is_some_and(|cursor| {
        cursor.commit_revision == 0
            || cursor.authorization_revision != request.authorization_revision
            || cursor.last_position.len() != 32
    }) {
        return Err(Status::invalid_argument(
            "local v6 query cursor identity is invalid",
        ));
    }
    Ok(())
}

fn execution_limits(
    admitted: usize,
    partitions: usize,
    requested: usize,
) -> Result<QueryExecutionLimits, Status> {
    let maximum_candidates = admitted
        .checked_div(256)
        .unwrap_or(0)
        .max(requested)
        .min(1_000_000);
    if maximum_candidates < requested {
        return Err(Status::resource_exhausted(
            "v6 query memory cannot retain the requested result page",
        ));
    }
    Ok(QueryExecutionLimits {
        maximum_partitions: partitions.max(1),
        maximum_loaded_bytes: admitted,
        maximum_heap_bytes: admitted,
        maximum_candidates,
        maximum_results: maximum_candidates,
        ..QueryExecutionLimits::default_for_memory()
    })
}

fn handoff_lineage(partition: ProjectionPartitionIdentity) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"keldra.index.v6.handoff-lineage/v1\0");
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
            return Err(Status::invalid_argument("v6 query cursor is invalid"));
        }
        candidates
            .iter()
            .position(|candidate| candidate.candidate.document.bytes() == resume.last_position[..])
            .map(|position| position + 1)
            .ok_or_else(|| {
                Status::failed_precondition("v6 query cursor position is no longer visible")
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
        .ok_or_else(|| Status::data_loss("v6 facet names an unknown field"))?;
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
        .ok_or_else(|| Status::data_loss("v6 aggregate names an unknown field"))?;
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
                "v6 Date result is not signed milliseconds",
            ));
        };
        let rendered = format_millis(
            *millis,
            &field
                .effective_date_format()
                .ok_or_else(|| Status::data_loss("v6 Date field has no format"))?,
        )
        .map_err(|error| Status::data_loss(format!("format v6 Date result: {error}")))?;
        return serde_json::to_vec(&rendered)
            .map_err(|error| Status::internal(format!("encode v6 Date result: {error}")));
    }
    let json = match value {
        ScalarValue::Null => serde_json::Value::Null,
        ScalarValue::Boolean(value) => serde_json::Value::Bool(*value),
        ScalarValue::Signed(value) => serde_json::Value::Number((*value).into()),
        ScalarValue::Unsigned(value) => serde_json::Value::Number((*value).into()),
        ScalarValue::Number(bits) => serde_json::Number::from_f64(f64::from_bits(*bits))
            .map(serde_json::Value::Number)
            .ok_or_else(|| Status::data_loss("v6 query returned a non-finite number"))?,
        ScalarValue::String(value) => serde_json::Value::String(value.clone()),
    };
    serde_json::to_vec(&json)
        .map_err(|error| Status::internal(format!("encode v6 computation result: {error}")))
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
    use keldra_index::v6::{ProjectionQueryStreamRoot, QueryAdmissionCandidate, StableDocumentKey};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn partition(producer: u64, placement_index: u64) -> ProjectionPartitionIdentity {
        ProjectionPartitionIdentity::new([1; 32], 7, [2; 32], producer, 3, placement_index).unwrap()
    }

    fn root(partition: ProjectionPartitionIdentity, next_offset: u64) -> PinnedPartitionQueryRoot {
        let root = ProjectionQueryStreamRoot {
            stream_root_hash: [partition.producer_node as u8; 32],
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
