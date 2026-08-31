//! Bounded format-v6 LSM compaction at the partition Current boundary.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use keldra_index::v6::{
    COMPONENT_STREAM_DIRECTORY_FANOUT, ChargedProjectionDeltaPacks, ChargedQueryRunCompaction,
    ComponentCompactionLimits, ComponentCompactionPlan, ComponentStreamRoot,
    EncodedComponentStreamPage, ProjectionGeneration, ProjectionPackCredits, QUERY_RUN_PAGE_FANOUT,
    QueryBlockCredits, QueryBlockLimits, QueryRunCompactionLimits, QueryRunPage,
    TombstoneCompactionPolicy, compact_component_runs, compact_encoded_query_runs,
    component_stream_child_hashes, decode_query_run_page, pack_component_deltas,
    projection_pack_path, projection_query_run_pack_path, projection_query_run_stream_page_path,
    projection_stream_page_path, select_component_compaction, select_query_run_compaction,
    splice_compacted_component_runs,
};
use tonic::Status;

use super::v6_publication::{LoadedV6ProjectionGeneration, V6ProjectionPublisher};

const MAX_ARTIFACT_BYTES: usize = 64 * 1024 * 1024;
const MAX_PAGE_BYTES: usize = 32 * 1024;

pub(crate) struct V6ComponentCompaction {
    pub(crate) packs: ChargedProjectionDeltaPacks,
    pub(crate) pages: Vec<EncodedComponentStreamPage>,
}

pub(crate) struct V6CompactionPublication {
    base: V6CompactionBase,
    artifacts: V6CompactionArtifacts,
}

pub(crate) struct V6CompactionBase {
    pub(crate) predecessor: ProjectionGeneration,
    component_overlay: BTreeMap<[u8; 32], Vec<u8>>,
    query_overlay: BTreeMap<[u8; 32], Vec<u8>>,
}

pub(crate) struct V6CompactionArtifacts {
    pub(crate) component: Option<V6ComponentCompaction>,
    pub(crate) query: Option<ChargedQueryRunCompaction>,
}

impl V6CompactionPublication {
    pub(crate) fn into_parts(self) -> (V6CompactionBase, V6CompactionArtifacts) {
        (self.base, self.artifacts)
    }
}

impl V6CompactionBase {
    pub(crate) fn component_page(&self, hash: &[u8; 32]) -> Option<&Vec<u8>> {
        self.component_overlay.get(hash)
    }

    pub(crate) fn query_page(&self, hash: &[u8; 32]) -> Option<&Vec<u8>> {
        self.query_overlay.get(hash)
    }
}

impl V6ProjectionPublisher {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn prepare_compaction(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        loaded: &LoadedV6ProjectionGeneration,
        maximum_runs: usize,
        maximum_unmerged_bytes: usize,
        maximum_preload_bytes: usize,
        component_credits: ProjectionPackCredits,
        query_credits: QueryBlockCredits,
    ) -> Result<V6CompactionPublication, Status> {
        let partition = loaded.generation.partition;
        let component_fan_in = maximum_runs.min(COMPONENT_STREAM_DIRECTORY_FANOUT).max(2);
        let component_limits = ComponentCompactionLimits {
            l0_trigger: component_fan_in.min(8),
            maximum_input_runs: component_fan_in,
            maximum_loaded_pack_bytes: component_credits.remaining(),
            maximum_output_run_bytes: maximum_unmerged_bytes
                .min(component_credits.remaining())
                .max(1024),
        };
        let mut predecessor = loaded.generation.clone();
        let mut component_pages = BTreeMap::new();
        let mut selected = Vec::<(ComponentCompactionPlan, ComponentStreamRoot)>::new();
        let mut sealed = Vec::new();
        let mut resident = 0usize;

        for root in &loaded.generation.roots {
            if root.segment_count < maximum_runs as u64
                && root.encoded_bytes < maximum_unmerged_bytes as u64
            {
                continue;
            }
            let stream = ComponentStreamRoot::from_component_root(root).map_err(index_status)?;
            load_component_pages(
                self,
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                partition,
                stream,
                maximum_preload_bytes,
                &mut resident,
                &mut component_pages,
            )
            .await?;
            let plan = select_component_compaction(
                stream,
                |hash| {
                    component_pages
                        .get(&hash)
                        .cloned()
                        .ok_or(keldra_index::IndexError::Integrity)
                },
                component_limits,
            )
            .map_err(index_status)?;
            let Some(plan) = plan else { continue };
            let output = compact_component_runs(
                &plan,
                component_limits,
                TombstoneCompactionPolicy::Retain,
                |hash| {
                    blocking_artifact(
                        self,
                        storage_tenant,
                        bucket,
                        tenant_id,
                        bucket_id,
                        projection_pack_path(partition, hash),
                        hash,
                        MAX_ARTIFACT_BYTES,
                    )
                },
            )
            .map_err(index_status)?;
            sealed.extend(output);
            selected.push((plan, stream));
        }

        let component = if selected.is_empty() {
            drop(component_credits);
            None
        } else {
            let packs = pack_component_deltas(sealed, component_credits).map_err(index_status)?;
            let mut replacements = Vec::new();
            let mut pages = Vec::new();
            for (plan, stream) in selected {
                let output = packs
                    .packs
                    .iter()
                    .flat_map(|pack| &pack.deltas)
                    .filter(|delta| delta.component == plan.component())
                    .cloned()
                    .collect::<Vec<_>>();
                let spliced = splice_compacted_component_runs(stream, &plan, &output, |hash| {
                    component_pages
                        .get(&hash)
                        .cloned()
                        .ok_or(keldra_index::IndexError::Integrity)
                })
                .map_err(index_status)?;
                replacements.push(spliced.root.component_root().map_err(index_status)?);
                pages.extend(spliced.new_pages);
            }
            for replacement in replacements {
                let index = predecessor
                    .roots
                    .binary_search_by_key(&replacement.component, |root| root.component)
                    .map_err(|_| Status::data_loss("v6 compacted component is absent"))?;
                predecessor.roots[index] = replacement;
            }
            for page in &pages {
                component_pages.insert(page.hash, page.bytes.clone());
            }
            Some(V6ComponentCompaction { packs, pages })
        };

        let query = if loaded.generation.query_stream_root.run_count >= maximum_runs as u64 {
            let mut query_pages = BTreeMap::new();
            load_query_pages(
                self,
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                partition,
                loaded.generation.query_stream_root.stream_root_hash,
                maximum_preload_bytes,
                &mut query_pages,
            )
            .await?;
            let query_fan_in = maximum_runs.min(QUERY_RUN_PAGE_FANOUT).max(2);
            let limits = QueryRunCompactionLimits {
                level_trigger: query_fan_in.min(8),
                maximum_input_runs: query_fan_in,
            };
            let plan = select_query_run_compaction(
                loaded.generation.query_stream_root,
                |hash| {
                    query_pages
                        .get(&hash)
                        .cloned()
                        .ok_or(keldra_index::IndexError::Integrity)
                },
                limits,
            )
            .map_err(index_status)?
            .ok_or_else(|| {
                Status::resource_exhausted("v6 query LSM has no bounded compaction window")
            })?;
            let compacted = compact_encoded_query_runs(
                loaded.generation.query_stream_root,
                &plan,
                partition,
                loaded.generation.physical_catalog_generation,
                QueryBlockLimits::default_for_memory(),
                query_credits,
                |hash| {
                    blocking_artifact(
                        self,
                        storage_tenant,
                        bucket,
                        tenant_id,
                        bucket_id,
                        projection_query_run_pack_path(partition, hash),
                        hash,
                        MAX_ARTIFACT_BYTES,
                    )
                },
                |hash| {
                    blocking_artifact(
                        self,
                        storage_tenant,
                        bucket,
                        tenant_id,
                        bucket_id,
                        projection_query_run_pack_path(partition, hash),
                        hash,
                        MAX_ARTIFACT_BYTES,
                    )
                },
                |hash| {
                    query_pages
                        .get(&hash)
                        .cloned()
                        .ok_or(keldra_index::IndexError::Integrity)
                },
            )
            .map_err(index_status)?;
            predecessor.query_stream_root = compacted.splice().root;
            for page in &compacted.splice().pages {
                query_pages.insert(page.hash, page.bytes.clone());
            }
            Some((compacted, query_pages))
        } else {
            drop(query_credits);
            None
        };
        predecessor.validate().map_err(index_status)?;
        let (query, query_overlay) = query
            .map(|(compaction, pages)| (Some(compaction), pages))
            .unwrap_or_default();
        Ok(V6CompactionPublication {
            base: V6CompactionBase {
                predecessor,
                component_overlay: component_pages,
                query_overlay,
            },
            artifacts: V6CompactionArtifacts { component, query },
        })
    }
}

#[allow(clippy::too_many_arguments)]
async fn load_component_pages(
    publisher: &V6ProjectionPublisher,
    storage_tenant: &str,
    bucket: &str,
    tenant_id: u64,
    bucket_id: u64,
    partition: keldra_index::v6::ProjectionPartitionIdentity,
    root: ComponentStreamRoot,
    maximum_bytes: usize,
    resident: &mut usize,
    pages: &mut BTreeMap<[u8; 32], Vec<u8>>,
) -> Result<(), Status> {
    let mut pending = VecDeque::from([root.root_hash]);
    while let Some(hash) = pending.pop_front() {
        if pages.contains_key(&hash) {
            continue;
        }
        let bytes = read_artifact(
            publisher,
            storage_tenant,
            bucket,
            tenant_id,
            bucket_id,
            projection_stream_page_path(partition, hash),
            hash,
            MAX_PAGE_BYTES,
        )
        .await?;
        *resident = resident
            .checked_add(bytes.len())
            .filter(|bytes| *bytes <= maximum_bytes)
            .ok_or_else(|| {
                Status::resource_exhausted("v6 compaction page preload exceeds memory bound")
            })?;
        pending
            .extend(component_stream_child_hashes(root.component, &bytes).map_err(index_status)?);
        pages.insert(hash, bytes);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn load_query_pages(
    publisher: &V6ProjectionPublisher,
    storage_tenant: &str,
    bucket: &str,
    tenant_id: u64,
    bucket_id: u64,
    partition: keldra_index::v6::ProjectionPartitionIdentity,
    root: [u8; 32],
    maximum_bytes: usize,
    pages: &mut BTreeMap<[u8; 32], Vec<u8>>,
) -> Result<(), Status> {
    let mut pending = VecDeque::from([root]);
    let mut resident = 0usize;
    let mut visited = BTreeSet::new();
    while let Some(hash) = pending.pop_front() {
        if !visited.insert(hash) {
            continue;
        }
        let bytes = read_artifact(
            publisher,
            storage_tenant,
            bucket,
            tenant_id,
            bucket_id,
            projection_query_run_stream_page_path(partition, hash),
            hash,
            MAX_PAGE_BYTES,
        )
        .await?;
        resident = resident
            .checked_add(bytes.len())
            .filter(|bytes| *bytes <= maximum_bytes)
            .ok_or_else(|| {
                Status::resource_exhausted("v6 query compaction page preload exceeds memory bound")
            })?;
        if let QueryRunPage::Branch(children) =
            decode_query_run_page(&bytes).map_err(index_status)?
        {
            pending.extend(children.into_iter().map(|child| child.hash));
        }
        pages.insert(hash, bytes);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn read_artifact(
    publisher: &V6ProjectionPublisher,
    storage_tenant: &str,
    bucket: &str,
    tenant_id: u64,
    bucket_id: u64,
    path: String,
    hash: [u8; 32],
    maximum_bytes: usize,
) -> Result<Vec<u8>, Status> {
    publisher
        .read_object(
            storage_tenant,
            bucket,
            tenant_id,
            bucket_id,
            &path,
            Some(hash),
            maximum_bytes,
        )
        .await?
        .map(|(bytes, _)| bytes)
        .ok_or_else(|| Status::data_loss("v6 compaction artifact is absent"))
}

#[allow(clippy::too_many_arguments)]
fn blocking_artifact(
    publisher: &V6ProjectionPublisher,
    storage_tenant: &str,
    bucket: &str,
    tenant_id: u64,
    bucket_id: u64,
    path: String,
    hash: [u8; 32],
    maximum_bytes: usize,
) -> Result<Vec<u8>, keldra_index::IndexError> {
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(read_artifact(
            publisher,
            storage_tenant,
            bucket,
            tenant_id,
            bucket_id,
            path,
            hash,
            maximum_bytes,
        ))
    })
    .map_err(|error| keldra_index::IndexError::Io(error.to_string()))
}

fn index_status(error: keldra_index::IndexError) -> Status {
    match error {
        keldra_index::IndexError::ResourceLimit { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        _ => Status::data_loss(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keldra_index::v6::{
        PreparedQueryMutationBatch, ProjectionCurrent, ProjectionPartitionIdentity,
        ProjectionQueryStreamRoot, QueryMemoryPermit, QueryRunReference,
        append_query_run_path_copy, prepare_projection_query_run,
    };

    struct Permit(usize);

    impl QueryMemoryPermit for Permit {
        fn admitted_bytes(&self) -> usize {
            self.0
        }
    }

    fn credits() -> QueryBlockCredits {
        QueryBlockCredits::from_query_permit(Box::new(Permit(16 * 1024 * 1024))).unwrap()
    }

    fn partition() -> ProjectionPartitionIdentity {
        ProjectionPartitionIdentity {
            family_id: [3; 32],
            source_node: 1,
            source_epoch: [4; 32],
            producer_node: 1,
            placement_term: 1,
            placement_index: 1,
        }
    }

    #[test]
    fn more_than_sixty_four_empty_flushes_compact_and_keep_current_valid() {
        let partition = partition();
        let catalog = [5; 32];
        let mut root = ProjectionQueryStreamRoot::empty(partition, catalog, 0, 0).unwrap();
        let mut pages = BTreeMap::new();
        let mut runs = BTreeMap::new();
        for offset in 0..65_u64 {
            let charged = prepare_projection_query_run(
                partition,
                catalog,
                root.last_sequence + 1,
                offset,
                offset + 1,
                offset + 1,
                PreparedQueryMutationBatch::default(),
                QueryBlockLimits::default_for_memory(),
                credits(),
            )
            .unwrap();
            let artifacts = charged.artifacts().clone();
            let reference = QueryRunReference {
                hash: artifacts.run.hash,
                sequence: root.last_sequence + 1,
                level: 0,
                source_start_offset: offset,
                next_offset: offset + 1,
                through_atomic_position: offset + 1,
            };
            runs.insert(artifacts.run.hash, artifacts.run.bytes);
            let appended =
                append_query_run_path_copy(Some(root), partition, catalog, reference, |hash| {
                    pages
                        .get(&hash)
                        .cloned()
                        .ok_or(keldra_index::IndexError::Integrity)
                })
                .unwrap();
            root = appended.root;
            pages.extend(
                appended
                    .pages
                    .into_iter()
                    .map(|page| (page.hash, page.bytes)),
            );
        }
        assert_eq!(root.run_count, 65);
        let plan = select_query_run_compaction(
            root,
            |hash| {
                pages
                    .get(&hash)
                    .cloned()
                    .ok_or(keldra_index::IndexError::Integrity)
            },
            QueryRunCompactionLimits {
                level_trigger: 8,
                maximum_input_runs: 64,
            },
        )
        .unwrap()
        .unwrap();
        let compacted = compact_encoded_query_runs(
            root,
            &plan,
            partition,
            catalog,
            QueryBlockLimits::default_for_memory(),
            credits(),
            |hash| {
                runs.get(&hash)
                    .cloned()
                    .ok_or(keldra_index::IndexError::Integrity)
            },
            |_| Err(keldra_index::IndexError::Integrity),
            |hash| {
                pages
                    .get(&hash)
                    .cloned()
                    .ok_or(keldra_index::IndexError::Integrity)
            },
        )
        .unwrap();
        assert!(compacted.splice().root.run_count < 64);
        pages.extend(
            compacted
                .splice()
                .pages
                .iter()
                .map(|page| (page.hash, page.bytes.clone())),
        );
        let next = prepare_projection_query_run(
            partition,
            catalog,
            root.last_sequence + 1,
            65,
            66,
            66,
            PreparedQueryMutationBatch::default(),
            QueryBlockLimits::default_for_memory(),
            credits(),
        )
        .unwrap();
        let next_artifacts = next.artifacts().clone();
        let appended = append_query_run_path_copy(
            Some(compacted.splice().root),
            partition,
            catalog,
            QueryRunReference {
                hash: next_artifacts.run.hash,
                sequence: root.last_sequence + 1,
                level: 0,
                source_start_offset: 65,
                next_offset: 66,
                through_atomic_position: 66,
            },
            |hash| {
                pages
                    .get(&hash)
                    .cloned()
                    .ok_or(keldra_index::IndexError::Integrity)
            },
        )
        .unwrap();
        let generation = ProjectionGeneration::initial(partition, catalog, 66, 66, Vec::new())
            .unwrap()
            .with_query_stream_root(appended.root)
            .unwrap();
        let current = ProjectionCurrent::new([9; 32], &generation).unwrap();
        current.validate_against(&generation).unwrap();
        assert_eq!(current.next_offset, 66);
    }
}
