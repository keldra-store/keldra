//! Local execution against one pinned immutable v2 manifest.

use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use anvil_api::v1::{IndexFreshness, IndexQueryHit, IndexSourceFreshness, ObjectAddress};
use anvil_store::{BlobRef, ObjectKey};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::{LocalIndexQueryExecutor, LocalIndexQueryRequest};
use crate::index_service::ExecutedIndexQuery;

use super::cache::{IndexCache, IndexCacheError, IndexSegmentFetcher, IndexSegmentId};
use super::directory::ManifestIndexDirectory;
use super::engine::kind_for_specification;
use super::events::{IndexBarrier, IndexEventJournal};
use super::generation::{
    IndexCurrentPointer, IndexGenerationManifest, ManifestReference, ManifestRun,
};
use super::publication::{current_path, manifest_path};
use super::query::{IndexQueryPosition, execute_query};

#[derive(Clone)]
pub(crate) struct LocalGenerationQueryExecutor {
    reader: ClusterObjectReader,
    cache: IndexCache,
    events: Arc<IndexEventJournal>,
}

impl LocalGenerationQueryExecutor {
    pub(crate) fn new(
        reader: ClusterObjectReader,
        cache: IndexCache,
        events: Arc<IndexEventJournal>,
    ) -> Self {
        Self {
            reader,
            cache,
            events,
        }
    }

    async fn execute(&self, request: LocalIndexQueryRequest) -> Result<ExecutedIndexQuery, Status> {
        let started = std::time::Instant::now();
        let kind = request
            .definition
            .specification
            .as_ref()
            .and_then(|specification| kind_for_specification(specification).ok());
        let result = self.execute_inner(request).await;
        if let Some(kind) = kind {
            let returned = result
                .as_ref()
                .map_or(0_u64, |executed| executed.hits.len() as u64);
            tracing::info!(
                index.kind = ?kind,
                histogram.anvil_index_query_duration_seconds = started.elapsed().as_secs_f64(),
                histogram.anvil_index_query_returned_hits = returned,
                "local index query completed"
            );
        }
        result
    }

    async fn execute_inner(
        &self,
        request: LocalIndexQueryRequest,
    ) -> Result<ExecutedIndexQuery, Status> {
        // Query execution is replica-local. Freshness may use a barrier already
        // observed by background work, but never fans out source-status RPCs.
        let observed = query_observed_barrier(&self.events);
        let requested_generation = request.resume.as_ref().map(|resume| resume.generation);
        let Some(loaded) = self
            .load_generation(
                &request.storage_tenant,
                &request.definition.bucket,
                request.tenant_id,
                request.bucket_id,
                request.definition.index_id,
                requested_generation,
            )
            .await?
        else {
            return Ok(ExecutedIndexQuery {
                hits: Vec::new(),
                freshness: empty_freshness(
                    request.definition.index_id,
                    request.definition.version,
                    observed.as_ref(),
                ),
                next_position: None,
            });
        };

        if loaded.manifest.definition_version != request.definition.version {
            if requested_generation.is_some() {
                return Err(Status::failed_precondition(
                    "requested generation belongs to another index definition version",
                ));
            }
            return Ok(ExecutedIndexQuery {
                hits: Vec::new(),
                freshness: freshness(&loaded, observed.as_ref(), false)?,
                next_position: None,
            });
        }
        let specification = request
            .definition
            .specification
            .as_ref()
            .ok_or_else(|| Status::data_loss("index definition has no specification"))?;
        let expected_kind = kind_for_specification(specification).map_err(index_status)?;
        if expected_kind != loaded.manifest.kind {
            return Err(Status::data_loss(
                "index manifest kind differs from its definition",
            ));
        }
        let position = request
            .resume
            .as_ref()
            .map(|resume| {
                serde_json::from_slice::<IndexQueryPosition>(&resume.last_position)
                    .map_err(|_| Status::invalid_argument("index page position is malformed"))
            })
            .transpose()?
            .unwrap_or_default();
        let directories = loaded
            .runs
            .iter()
            .map(|run| ManifestIndexDirectory::open(self.cache.clone(), run))
            .collect::<Result<Vec<_>, _>>()
            .map_err(index_status)?;
        let page = execute_query(
            &directories,
            specification,
            &request.query,
            request.limit,
            position,
        )
        .await
        .map_err(index_status)?;
        let hits = page
            .hits
            .into_iter()
            .map(|hit| IndexQueryHit {
                address: hit.object_path.map(|path| ObjectAddress {
                    tenant: request.storage_tenant.clone(),
                    bucket: request.definition.bucket.clone(),
                    path,
                }),
                object_version: hit.object_version,
                score: hit.score,
                fields_json: hit.fields_json,
            })
            .collect();
        let next_position = page
            .next
            .map(|position| {
                serde_json::to_vec(&position)
                    .map_err(|error| Status::internal(format!("encode index position: {error}")))
            })
            .transpose()?;
        Ok(ExecutedIndexQuery {
            hits,
            freshness: freshness(&loaded, observed.as_ref(), true)?,
            next_position,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn load_generation(
        &self,
        tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        index_id: u64,
        exact_generation: Option<u64>,
    ) -> Result<Option<LoadedGeneration>, Status> {
        let key = ObjectKey::new(tenant, bucket, current_path(index_id))
            .map_err(|error| Status::internal(error.to_string()))?;
        let Some(opened) = self
            .reader
            .open_stable(&key, tenant_id, bucket_id, None)
            .await?
        else {
            return Ok(None);
        };
        if opened.version.deleted {
            return Ok(None);
        }
        let pointer_bytes = read_payload(opened.payload, "current index pointer")?;
        let pointer = IndexCurrentPointer::decode(&pointer_bytes)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        if pointer.index_id != index_id {
            return Err(Status::data_loss(
                "current index pointer belongs to another index",
            ));
        }
        let mut published_at = pointer.published_at_unix_millis;
        let mut manifest = self
            .read_manifest_blob(index_id, &pointer.manifest_path, &pointer.manifest_blob)
            .await?;
        let requested = exact_generation.unwrap_or(pointer.generation);
        if requested > pointer.generation {
            return Err(Status::failed_precondition(
                "requested index generation was never published",
            ));
        }
        while manifest.generation > requested {
            let previous = manifest.previous.as_ref().ok_or_else(|| {
                Status::failed_precondition("requested index generation is no longer retained")
            })?;
            published_at = previous.published_at_unix_millis;
            manifest = self.read_manifest_reference(index_id, previous).await?;
        }
        if manifest.generation != requested {
            return Err(Status::failed_precondition(
                "requested index generation is no longer retained",
            ));
        }

        // Engine query order is newest first; manifest persistence is ascending
        // so sequence validation and deterministic CAS bytes stay simple.
        let runs = manifest.runs.iter().rev().cloned().collect();
        Ok(Some(LoadedGeneration {
            manifest,
            runs,
            published_at_unix_millis: published_at,
        }))
    }

    async fn read_manifest_reference(
        &self,
        index_id: u64,
        reference: &ManifestReference,
    ) -> Result<IndexGenerationManifest, Status> {
        let manifest = self
            .read_manifest_blob(index_id, &reference.path, &reference.blob)
            .await?;
        if manifest.generation != reference.generation
            || manifest.definition_version != reference.definition_version
        {
            return Err(Status::data_loss(
                "index manifest predecessor identity differs from its reference",
            ));
        }
        Ok(manifest)
    }

    async fn read_manifest_blob(
        &self,
        index_id: u64,
        path: &str,
        blob: &BlobRef,
    ) -> Result<IndexGenerationManifest, Status> {
        if path != manifest_path(index_id, blob.hash) {
            return Err(Status::data_loss("index manifest path is not canonical"));
        }
        let bytes = self.reader.read_blob_bytes(blob).await?;
        let manifest = IndexGenerationManifest::decode(&bytes)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        if manifest.index_id != index_id {
            return Err(Status::data_loss("index manifest belongs to another index"));
        }
        Ok(manifest)
    }
}

#[tonic::async_trait]
impl LocalIndexQueryExecutor for LocalGenerationQueryExecutor {
    async fn execute_local(
        &self,
        request: LocalIndexQueryRequest,
    ) -> Result<ExecutedIndexQuery, Status> {
        self.execute(request).await
    }
}

#[derive(Clone)]
pub(crate) struct ClusterIndexSegmentFetcher {
    reader: ClusterObjectReader,
}

impl ClusterIndexSegmentFetcher {
    pub(crate) fn new(reader: ClusterObjectReader) -> Self {
        Self { reader }
    }
}

#[tonic::async_trait]
impl IndexSegmentFetcher for ClusterIndexSegmentFetcher {
    async fn fetch(&self, segment: IndexSegmentId) -> Result<Vec<u8>, IndexCacheError> {
        self.reader
            .read_blob_bytes(&BlobRef {
                hash: segment.blake3,
                length: segment.length,
            })
            .await
            .map_err(|error| IndexCacheError::Fetch(error.to_string()))
    }
}

struct LoadedGeneration {
    manifest: IndexGenerationManifest,
    /// Newest first, matching the engine's deterministic version tie-break.
    runs: Vec<ManifestRun>,
    published_at_unix_millis: u64,
}

fn read_payload(
    payload: Option<crate::cluster_object_read::ClusterReadPayload>,
    label: &str,
) -> Result<Vec<u8>, Status> {
    let Some(mut payload) = payload else {
        return Err(Status::data_loss(format!("{label} has no payload")));
    };
    let mut bytes = Vec::new();
    payload
        .read_to_end(&mut bytes)
        .map_err(|error| Status::internal(format!("read {label}: {error}")))?;
    Ok(bytes)
}

fn freshness(
    generation: &LoadedGeneration,
    observed: Option<&IndexBarrier>,
    initial_build_complete: bool,
) -> Result<IndexFreshness, Status> {
    let indexed = generation
        .manifest
        .sources
        .iter()
        .map(|source| (source.node_id, source))
        .collect::<std::collections::BTreeMap<_, _>>();
    let observed_sources = observed
        .map(|barrier| {
            barrier
                .sources
                .iter()
                .map(|(node, cursor)| (node.0, cursor))
                .collect::<std::collections::BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let mut node_ids = indexed.keys().copied().collect::<Vec<_>>();
    node_ids.extend(observed_sources.keys().copied());
    node_ids.sort_unstable();
    node_ids.dedup();
    let mut rebuilding =
        observed.is_some_and(|barrier| generation.manifest.placement_fence != barrier.fence);
    let sources = node_ids
        .into_iter()
        .map(
            |node_id| match (indexed.get(&node_id), observed_sources.get(&node_id)) {
                (Some(indexed), Some(observed)) if indexed.source == observed.source => {
                    IndexSourceFreshness {
                        node_id,
                        source_epoch: indexed.source.source_epoch.to_vec(),
                        indexed_next_offset: indexed.next_offset,
                        observed_tail: observed.next_offset.checked_sub(1),
                        lag_hint: observed.next_offset.saturating_sub(indexed.next_offset),
                    }
                }
                (Some(indexed), _) => {
                    rebuilding |= observed.is_some();
                    IndexSourceFreshness {
                        node_id,
                        source_epoch: indexed.source.source_epoch.to_vec(),
                        indexed_next_offset: indexed.next_offset,
                        observed_tail: None,
                        lag_hint: 0,
                    }
                }
                (None, Some(observed)) => {
                    rebuilding = true;
                    IndexSourceFreshness {
                        node_id,
                        source_epoch: observed.source.source_epoch.to_vec(),
                        indexed_next_offset: 0,
                        observed_tail: observed.next_offset.checked_sub(1),
                        lag_hint: observed.next_offset,
                    }
                }
                (None, None) => unreachable!("node ID came from one source map"),
            },
        )
        .collect();
    Ok(IndexFreshness {
        generation: generation.manifest.generation,
        published_at: Some(publication_time(generation.published_at_unix_millis)?.into()),
        sources,
        initial_build_complete,
        rebuilding,
        authorization_revision: 0,
        placement_term: generation.manifest.placement_fence.term,
        placement_index: generation.manifest.placement_fence.index,
        index_id: generation.manifest.index_id,
        definition_version: generation.manifest.definition_version,
    })
}

fn empty_freshness(
    index_id: u64,
    definition_version: u64,
    observed: Option<&IndexBarrier>,
) -> IndexFreshness {
    let sources = observed
        .into_iter()
        .flat_map(|barrier| &barrier.sources)
        .map(|(node, cursor)| IndexSourceFreshness {
            node_id: node.0,
            source_epoch: cursor.source.source_epoch.to_vec(),
            indexed_next_offset: 0,
            observed_tail: cursor.next_offset.checked_sub(1),
            lag_hint: cursor.next_offset,
        })
        .collect();
    IndexFreshness {
        generation: 0,
        published_at: None,
        sources,
        initial_build_complete: false,
        rebuilding: true,
        authorization_revision: 0,
        placement_term: 0,
        placement_index: 0,
        index_id,
        definition_version,
    }
}

fn query_observed_barrier(events: &IndexEventJournal) -> Option<IndexBarrier> {
    events.last_observed_barrier()
}

fn publication_time(unix_millis: u64) -> Result<std::time::SystemTime, Status> {
    std::time::UNIX_EPOCH
        .checked_add(Duration::from_millis(unix_millis))
        .ok_or_else(|| Status::data_loss("index publication timestamp exceeds the system clock"))
}

fn index_status(error: anvil_index::IndexError) -> Status {
    match error {
        anvil_index::IndexError::InvalidQuery(_) => Status::invalid_argument(error.to_string()),
        anvil_index::IndexError::ResourceLimit { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        anvil_index::IndexError::InvalidDefinition(_) => {
            Status::failed_precondition(error.to_string())
        }
        anvil_index::IndexError::Io(_) => Status::unavailable(error.to_string()),
        anvil_index::IndexError::Encode(_) => Status::internal(error.to_string()),
        _ => Status::data_loss(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct PanicAuthority;

    impl super::super::events::IndexEventAuthority for PanicAuthority {
        fn current(&self) -> Result<super::super::events::IndexEventPlacement, String> {
            panic!("query attempted to consult cluster event authority")
        }
    }

    struct PanicSources;

    #[tonic::async_trait]
    impl super::super::events::IndexEventSources for PanicSources {
        async fn status(
            &self,
            _source: &super::super::events::IndexSource,
        ) -> Result<anvil_store::WatchJournalStatus, super::super::events::IndexEventError>
        {
            panic!("query attempted a source status RPC")
        }

        async fn read_page(
            &self,
            _source: &super::super::events::IndexSource,
            _expected_source: anvil_store::SourceId,
            _after_offset: u64,
            _target_offset: u64,
            _tenant_id: u64,
            _bucket_id: u64,
            _limit: usize,
            _max_bytes: u64,
        ) -> Result<super::super::events::IndexSourcePage, super::super::events::IndexEventError>
        {
            panic!("query attempted a source journal RPC")
        }
    }

    #[test]
    fn millisecond_timestamp_is_exact() {
        let value = publication_time(1_234)
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        assert_eq!(value, Duration::from_millis(1_234));
    }

    #[test]
    fn query_freshness_never_polls_cluster_sources() {
        let events = IndexEventJournal::new(Arc::new(PanicAuthority), Arc::new(PanicSources));
        assert!(query_observed_barrier(&events).is_none());
    }

    #[test]
    fn query_failures_preserve_public_status_semantics() {
        assert_eq!(
            index_status(anvil_index::IndexError::InvalidQuery("bad query".into())).code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            index_status(anvil_index::IndexError::ResourceLimit {
                needed: 2,
                limit: 1,
            })
            .code(),
            tonic::Code::ResourceExhausted
        );
        assert_eq!(
            index_status(anvil_index::IndexError::Integrity).code(),
            tonic::Code::DataLoss
        );
        assert_eq!(
            index_status(anvil_index::IndexError::Encode("failed".into())).code(),
            tonic::Code::Internal
        );
    }
}
