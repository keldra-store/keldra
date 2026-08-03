//! Local execution against one immutable, ordinary-object-backed generation.

use std::io::Read;
use std::time::Duration;

use anvil_api::v1::{IndexFreshness, IndexQueryHit, IndexSourceFreshness, ObjectAddress};
use anvil_store::{BlobRef, ObjectKey};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::{LocalIndexQueryExecutor, LocalIndexQueryRequest};
use crate::index_service::ExecutedIndexQuery;

use super::cache::{IndexCache, IndexCacheError, IndexSegmentFetcher, IndexSegmentId};
use super::directory::ManifestIndexDirectory;
use super::events::IndexEventRouter;
use super::generation::{IndexCurrentPointer, IndexGenerationManifest};
use super::publication::{current_path, generation_manifest_path};
use super::query::{IndexQueryPosition, execute_query};

#[derive(Clone)]
pub(crate) struct LocalGenerationQueryExecutor {
    reader: ClusterObjectReader,
    cache: IndexCache,
    events: IndexEventRouter,
}

impl LocalGenerationQueryExecutor {
    pub(crate) fn new(
        reader: ClusterObjectReader,
        cache: IndexCache,
        events: IndexEventRouter,
    ) -> Self {
        Self {
            reader,
            cache,
            events,
        }
    }

    async fn execute(&self, request: LocalIndexQueryRequest) -> Result<ExecutedIndexQuery, Status> {
        let observed = self.events.current_barrier().await;
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
                    &observed,
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
                freshness: freshness(&loaded, &observed, false)?,
                next_position: None,
            });
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
        let specification = request
            .definition
            .specification
            .as_ref()
            .ok_or_else(|| Status::data_loss("index definition has no specification"))?;
        let directory = ManifestIndexDirectory::open(self.cache.clone(), &loaded.manifest)
            .map_err(index_status)?;
        let page = execute_query(
            &directory,
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
            freshness: freshness(&loaded, &observed, true)?,
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
        match exact_generation {
            Some(generation) => {
                let path = generation_manifest_path(index_id, generation);
                let key = ObjectKey::new(tenant, bucket, &path)
                    .map_err(|error| Status::internal(error.to_string()))?;
                let Some(opened) = self
                    .reader
                    .open_stable(&key, tenant_id, bucket_id, None)
                    .await?
                else {
                    return Err(Status::failed_precondition(
                        "requested index generation is no longer retained",
                    ));
                };
                if opened.version.deleted {
                    return Err(Status::failed_precondition(
                        "requested index generation is no longer retained",
                    ));
                }
                let bytes = read_payload(opened.payload, "index generation manifest")?;
                let manifest = IndexGenerationManifest::decode(&bytes)
                    .map_err(|error| Status::data_loss(error.to_string()))?;
                if manifest.index_id != index_id || manifest.generation != generation {
                    return Err(Status::data_loss(
                        "index generation manifest identity is invalid",
                    ));
                }
                Ok(Some(LoadedGeneration {
                    published_at_unix_millis: opened.version.committed_at_unix_millis,
                    manifest,
                }))
            }
            None => {
                let key = ObjectKey::new(tenant, bucket, &current_path(index_id))
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
                let manifest_bytes = self.reader.read_blob_bytes(&pointer.manifest_blob).await?;
                let manifest = IndexGenerationManifest::decode(&manifest_bytes)
                    .map_err(|error| Status::data_loss(error.to_string()))?;
                if manifest.index_id != index_id
                    || manifest.generation != pointer.generation
                    || manifest.definition_version != pointer.definition_version
                {
                    return Err(Status::data_loss(
                        "current pointer and generation manifest disagree",
                    ));
                }
                Ok(Some(LoadedGeneration {
                    published_at_unix_millis: pointer.published_at_unix_millis,
                    manifest,
                }))
            }
        }
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
    observed: &super::events::IndexBarrier,
    initial_build_complete: bool,
) -> Result<IndexFreshness, Status> {
    let indexed = generation
        .manifest
        .sources
        .iter()
        .map(|source| (source.node_id, source))
        .collect::<std::collections::BTreeMap<_, _>>();
    let observed_sources = observed
        .sources
        .iter()
        .map(|(node, cursor)| (node.0, cursor))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut node_ids = indexed.keys().copied().collect::<Vec<_>>();
    node_ids.extend(observed_sources.keys().copied());
    node_ids.sort_unstable();
    node_ids.dedup();
    let mut rebuilding = generation.manifest.placement_fence != observed.fence;
    let sources = node_ids
        .into_iter()
        .map(
            |node_id| match (indexed.get(&node_id), observed_sources.get(&node_id)) {
                (Some(indexed), Some(observed)) if indexed.source == observed.source => {
                    let lag_hint = observed.next_offset.saturating_sub(indexed.next_offset);
                    // The source cursor is cluster-wide, so this is an upper bound
                    // that may include unrelated objects and reserved artifacts.
                    // Ordinary lag is evidence, not proof that this index is being
                    // rebuilt; only a fence/source-vector mismatch sets that flag.
                    IndexSourceFreshness {
                        node_id,
                        source_epoch: indexed.source.source_epoch.to_vec(),
                        indexed_next_offset: indexed.next_offset,
                        observed_tail: observed.next_offset.checked_sub(1),
                        lag_hint,
                    }
                }
                (Some(indexed), _) => {
                    rebuilding = true;
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
    observed: &super::events::IndexBarrier,
) -> IndexFreshness {
    let sources = observed
        .sources
        .iter()
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

fn publication_time(unix_millis: u64) -> Result<std::time::SystemTime, Status> {
    std::time::UNIX_EPOCH
        .checked_add(Duration::from_millis(unix_millis))
        .ok_or_else(|| Status::data_loss("index publication timestamp exceeds the system clock"))
}

fn index_status(error: anvil_index::IndexError) -> Status {
    Status::failed_precondition(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn millisecond_timestamp_is_exact() {
        let value = publication_time(1_234)
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        assert_eq!(value, Duration::from_millis(1_234));
    }
}
