//! Bounded format-v1 LSM compaction at the partition Current boundary.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use keldra_index::v1::{
    ArtifactPackTable, COMPONENT_STREAM_DIRECTORY_FANOUT, ChargedProjectionDeltaPacks,
    ChargedQueryRunCompaction, ComponentCompactionLimits, ComponentCompactionPlan,
    ComponentStreamRoot, EncodedComponentStreamPage, IndexingMemoryPermit, PackedComponentDelta,
    ProjectionGeneration, ProjectionPackCredits, ProjectionQueryStreamRoot, QUERY_RUN_PAGE_FANOUT,
    QueryBlockCredits, QueryBlockLimits, QueryRunCompactionLimits, QueryRunCompactionPlan,
    QueryRunPage, SealedComponentDelta, TombstoneCompactionPolicy, compact_component_runs,
    component_stream_child_hashes, decode_query_run_page, pack_component_deltas,
    prepare_encoded_query_run_compaction, projection_query_run_pack_path,
    projection_query_run_stream_page_path, projection_stream_page_path,
    select_component_compaction, select_query_run_compaction, splice_compacted_component_runs,
};
use tonic::Status;

use super::cpu::IndexCpuPool;
use super::v1_parallel::run_bounded_ordered;
use super::v1_publication::{LoadedV1ProjectionGeneration, V1ProjectionPublisher};

const MAX_ARTIFACT_BYTES: usize = 64 * 1024 * 1024;
const MAX_PAGE_BYTES: usize = 32 * 1024;
const MAX_PARALLEL_COMPONENT_COMPACTIONS: usize = 4;

pub(crate) struct V1ComponentCompaction {
    pub(crate) packs: ChargedProjectionDeltaPacks,
    pub(crate) pages: Vec<EncodedComponentStreamPage>,
    proposals: Vec<V1ComponentCompactionProposal>,
}

struct V1ComponentCompactionProposal {
    plan: ComponentCompactionPlan,
    output: Vec<PackedComponentDelta>,
    pack_table: ArtifactPackTable,
}

pub(crate) struct V1CompactionPublication {
    base: V1CompactionBase,
    artifacts: V1CompactionArtifacts,
}

pub(crate) struct V1CompactionBase {
    pub(crate) predecessor: ProjectionGeneration,
    component_overlay: BTreeMap<[u8; 32], Bytes>,
    query_overlay: BTreeMap<[u8; 32], Bytes>,
    _preload_permit: IndexingMemoryPermit,
}

pub(crate) struct V1CompactionArtifacts {
    pub(crate) component: Option<V1ComponentCompaction>,
    pub(crate) query: Option<ChargedQueryRunCompaction>,
}

impl V1CompactionPublication {
    pub(crate) fn artifacts(&self) -> &V1CompactionArtifacts {
        &self.artifacts
    }

    pub(crate) fn into_parts(self) -> (V1CompactionBase, V1CompactionArtifacts) {
        (self.base, self.artifacts)
    }

    /// Rebase immutable compaction outputs over a newer generation. Appended
    /// runs are retained. A proposal is discarded only when one of its exact
    /// selected inputs has already been replaced by another compaction.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn rebase_onto(
        &mut self,
        publisher: &V1ProjectionPublisher,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        current: &LoadedV1ProjectionGeneration,
    ) -> Result<bool, Status> {
        if self.base.predecessor == current.generation {
            return Ok(true);
        }
        let maximum_bytes = self.base._preload_permit.bytes();
        let partition = current.generation.partition;
        let mut rebased = current.generation.clone();
        let mut resident = self
            .base
            .component_overlay
            .values()
            .chain(self.base.query_overlay.values())
            .try_fold(0usize, |total, bytes| total.checked_add(bytes.len()))
            .ok_or_else(|| Status::resource_exhausted("v1 compaction rebase bytes overflow"))?;
        if resident > maximum_bytes {
            return Err(Status::resource_exhausted(
                "v1 compaction rebase exceeds its retained memory admission",
            ));
        }
        if let Some(component) = self.artifacts.component.as_mut() {
            for proposal in &component.proposals {
                let index = rebased
                    .roots
                    .binary_search_by_key(&proposal.plan.component(), |root| root.component)
                    .map_err(|_| Status::data_loss("v1 compaction component disappeared"))?;
                let stream = ComponentStreamRoot::from_component_root(&rebased.roots[index])
                    .map_err(index_status)?;
                load_component_pages(
                    publisher,
                    storage_tenant,
                    bucket,
                    tenant_id,
                    bucket_id,
                    partition,
                    stream,
                    maximum_bytes,
                    &mut resident,
                    &mut self.base.component_overlay,
                )
                .await?;
                let spliced = match splice_compacted_component_runs(
                    stream,
                    &proposal.plan,
                    &proposal.output,
                    &proposal.pack_table,
                    |hash| {
                        self.base
                            .component_overlay
                            .get(&hash)
                            .cloned()
                            .ok_or(keldra_index::IndexError::Integrity)
                    },
                ) {
                    Ok(spliced) => spliced,
                    Err(keldra_index::IndexError::StaleProposal) => return Ok(false),
                    Err(error) => return Err(index_status(error)),
                };
                rebased.roots[index] = spliced.root.component_root().map_err(index_status)?;
                for page in spliced.new_pages {
                    retain_generated_page(
                        maximum_bytes,
                        &mut resident,
                        &mut self.base.component_overlay,
                        page.hash,
                        page.bytes.clone(),
                    )?;
                    component.pages.push(page);
                }
            }
        }
        if let Some(query) = self.artifacts.query.as_mut() {
            load_query_pages(
                publisher,
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                partition,
                rebased.query_stream_root.stream_root_hash,
                maximum_bytes,
                &mut resident,
                &mut self.base.query_overlay,
            )
            .await?;
            match query.rebase(rebased.query_stream_root, |hash| {
                self.base
                    .query_overlay
                    .get(&hash)
                    .cloned()
                    .ok_or(keldra_index::IndexError::Integrity)
            }) {
                Ok(()) => {}
                Err(keldra_index::IndexError::StaleProposal) => return Ok(false),
                Err(error) => return Err(index_status(error)),
            }
            rebased.query_stream_root = query.splice().root;
            for page in &query.splice().pages {
                retain_generated_page(
                    maximum_bytes,
                    &mut resident,
                    &mut self.base.query_overlay,
                    page.hash,
                    page.bytes.clone(),
                )?;
            }
        }
        rebased.validate().map_err(index_status)?;
        self.base.predecessor = rebased;
        Ok(true)
    }
}

impl V1CompactionBase {
    pub(crate) fn component_page(&self, hash: &[u8; 32]) -> Option<&Bytes> {
        self.component_overlay.get(hash)
    }

    pub(crate) fn query_page(&self, hash: &[u8; 32]) -> Option<&Bytes> {
        self.query_overlay.get(hash)
    }
}

impl V1ProjectionPublisher {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn prepare_compaction(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        loaded: &LoadedV1ProjectionGeneration,
        cpu: &IndexCpuPool,
        maximum_runs: usize,
        maximum_unmerged_bytes: usize,
        maximum_preload_bytes: usize,
        preload_permit: IndexingMemoryPermit,
        raw_output_permit: IndexingMemoryPermit,
        component_credits: ProjectionPackCredits,
        query_credits: QueryBlockCredits,
    ) -> Result<V1CompactionPublication, Status> {
        let compaction_started = Instant::now();
        if preload_permit.bytes() < maximum_preload_bytes {
            return Err(Status::resource_exhausted(
                "v1 compaction preload exceeds its admitted memory",
            ));
        }
        let partition = loaded.generation.partition;
        let component_fan_in = maximum_runs.min(COMPONENT_STREAM_DIRECTORY_FANOUT).max(2);
        let mut predecessor = loaded.generation.clone();
        let mut component_pages = BTreeMap::new();
        let mut eligible = Vec::new();
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
            eligible.push(stream);
        }

        // Component roots are independent immutable streams. Split both the
        // existing pack-input and output ceilings across a bounded number of
        // jobs, retaining the former aggregate ceilings rather than multiplying
        // either ceiling by the concurrency width.
        let eligible_component_count = eligible.len();
        let (compaction_parallelism, per_job_bytes) =
            component_compaction_schedule(eligible_component_count, component_credits.remaining())?;
        let component_limits = ComponentCompactionLimits {
            l0_trigger: component_fan_in.min(8),
            maximum_input_runs: component_fan_in,
            maximum_loaded_pack_bytes: per_job_bytes,
            maximum_output_run_bytes: maximum_unmerged_bytes.min(per_job_bytes).max(1024),
        };
        let mut component_pages = Arc::new(component_pages);
        let job_component_pages = Arc::clone(&component_pages);
        let serial_streams = eligible.clone();
        let work = eligible
            .into_iter()
            .enumerate()
            .map(|(ordinal, stream)| (ordinal, stream))
            .collect();
        let publisher = self.clone();
        let parallel_cpu = cpu.clone();
        let task_storage_tenant = storage_tenant.to_owned();
        let task_bucket = bucket.to_owned();
        let compacted = run_bounded_ordered(work, compaction_parallelism, move |stream| {
            let publisher = publisher.clone();
            let cpu = parallel_cpu.clone();
            let storage_tenant = task_storage_tenant.clone();
            let bucket = task_bucket.clone();
            let component_pages = Arc::clone(&job_component_pages);
            async move {
                let runtime = tokio::runtime::Handle::current();
                let result = cpu
                    .submit(move || {
                        compact_component_stream(
                            &runtime,
                            &publisher,
                            &storage_tenant,
                            &bucket,
                            tenant_id,
                            bucket_id,
                            stream,
                            &component_pages,
                            component_limits,
                        )
                    })
                    .await
                    .map_err(|error| Status::internal(error.to_string()))
                    .and_then(|result| result);
                (stream, result)
            }
        })
        .await?;
        let serial_component_limits = ComponentCompactionLimits {
            l0_trigger: component_fan_in.min(8),
            maximum_input_runs: component_fan_in,
            maximum_loaded_pack_bytes: component_credits.remaining(),
            maximum_output_run_bytes: maximum_unmerged_bytes
                .min(component_credits.remaining())
                .max(1024),
        };
        let mut selected = Vec::<(ComponentCompactionPlan, ComponentStreamRoot)>::new();
        let mut sealed = Vec::new();
        if let Some(error) = compacted.iter().find_map(|(_, (_, result))| {
            result
                .as_ref()
                .err()
                .filter(|error| error.code() != tonic::Code::ResourceExhausted)
        }) {
            return Err(error.clone());
        }
        let retry_serially = compaction_parallelism > 1
            && compacted.iter().any(|(_, (_, result))| {
                result
                    .as_ref()
                    .is_err_and(|error| error.code() == tonic::Code::ResourceExhausted)
            });
        if retry_serially {
            // Discard every fair-share result before retrying. The old full
            // budget must not overlap retained sibling output, or a fallback
            // intended to preserve progress would exceed the charged ceiling.
            drop(compacted);
            for stream in serial_streams {
                let runtime = tokio::runtime::Handle::current();
                let publisher = self.clone();
                let storage_tenant = storage_tenant.to_owned();
                let bucket = bucket.to_owned();
                let pages = Arc::clone(&component_pages);
                let compacted = cpu
                    .submit(move || {
                        compact_component_stream(
                            &runtime,
                            &publisher,
                            &storage_tenant,
                            &bucket,
                            tenant_id,
                            bucket_id,
                            stream,
                            &pages,
                            serial_component_limits,
                        )
                    })
                    .await
                    .map_err(|error| Status::internal(error.to_string()))??;
                if let Some((plan, output)) = compacted {
                    selected.push((plan, stream));
                    sealed.extend(output);
                }
            }
        } else {
            for (_, (stream, result)) in compacted {
                if let Some((plan, output)) = result? {
                    selected.push((plan, stream));
                    sealed.extend(output);
                }
            }
        }
        let raw_output_bytes = sealed.iter().try_fold(0_usize, |total, delta| {
            total
                .checked_add(delta.bytes.len())
                .ok_or_else(|| Status::resource_exhausted("v1 compacted output bytes overflow"))
        })?;
        if raw_output_bytes > raw_output_permit.bytes() {
            return Err(Status::resource_exhausted(
                "v1 compacted raw output exceeds its admitted memory",
            ));
        }

        let compacted_component_count = selected.len();
        let component = if selected.is_empty() {
            drop(component_credits);
            None
        } else {
            let packs = pack_component_deltas(sealed, component_credits).map_err(index_status)?;
            drop(raw_output_permit);
            let pack_table = self
                .publish_component_packs(
                    storage_tenant,
                    bucket,
                    tenant_id,
                    bucket_id,
                    partition,
                    &packs.packs,
                )
                .await?;
            let mut replacements = Vec::new();
            let mut proposals = Vec::new();
            let mut pages = Vec::new();
            for (plan, stream) in selected {
                let output = packs
                    .packs
                    .iter()
                    .flat_map(|pack| &pack.deltas)
                    .filter(|delta| delta.component == plan.component())
                    .cloned()
                    .collect::<Vec<_>>();
                let spliced =
                    splice_compacted_component_runs(stream, &plan, &output, &pack_table, |hash| {
                        component_pages
                            .get(&hash)
                            .cloned()
                            .ok_or(keldra_index::IndexError::Integrity)
                    })
                    .map_err(index_status)?;
                replacements.push(spliced.root.component_root().map_err(index_status)?);
                pages.extend(spliced.new_pages);
                proposals.push(V1ComponentCompactionProposal {
                    plan,
                    output,
                    pack_table: pack_table.clone(),
                });
            }
            for replacement in replacements {
                let index = predecessor
                    .roots
                    .binary_search_by_key(&replacement.component, |root| root.component)
                    .map_err(|_| Status::data_loss("v1 compacted component is absent"))?;
                predecessor.roots[index] = replacement;
            }
            for page in &pages {
                retain_generated_page(
                    maximum_preload_bytes,
                    &mut resident,
                    Arc::make_mut(&mut component_pages),
                    page.hash,
                    page.bytes.clone(),
                )?;
            }
            Some(V1ComponentCompaction {
                packs,
                pages,
                proposals,
            })
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
                &mut resident,
                &mut query_pages,
            )
            .await?;
            let block_limits = QueryBlockLimits::default_for_memory();
            let query_fan_in = maximum_runs
                .min(QUERY_RUN_PAGE_FANOUT)
                .min(block_limits.maximum_loaded_blocks)
                .max(2);
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
                Status::resource_exhausted("v1 query LSM has no bounded compaction window")
            })?;
            let runtime = tokio::runtime::Handle::current();
            let publisher = self.clone();
            let storage_tenant = storage_tenant.to_owned();
            let bucket = bucket.to_owned();
            let cpu_query_pages = query_pages.clone();
            let query_stream_root = loaded.generation.query_stream_root;
            let physical_catalog_generation = loaded.generation.physical_catalog_generation;
            let compacted = cpu
                .submit(move || {
                    compact_query_stream(
                        &runtime,
                        &publisher,
                        &storage_tenant,
                        &bucket,
                        tenant_id,
                        bucket_id,
                        query_stream_root,
                        plan,
                        partition,
                        physical_catalog_generation,
                        block_limits,
                        query_credits,
                        &cpu_query_pages,
                    )
                })
                .await
                .map_err(|error| Status::internal(error.to_string()))??;
            predecessor.query_stream_root = compacted.splice().root;
            for page in &compacted.splice().pages {
                retain_generated_page(
                    maximum_preload_bytes,
                    &mut resident,
                    &mut query_pages,
                    page.hash,
                    page.bytes.clone(),
                )?;
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
        tracing::info!(
            histogram.keldra_index_v1_compaction_duration_seconds =
                compaction_started.elapsed().as_secs_f64(),
            eligible_component_streams = eligible_component_count,
            compacted_component_streams = compacted_component_count,
            component_parallelism = compaction_parallelism,
            query_compacted = query.is_some(),
            "keldra_index_v1_compaction"
        );
        Ok(V1CompactionPublication {
            base: V1CompactionBase {
                predecessor,
                component_overlay: Arc::try_unwrap(component_pages)
                    .unwrap_or_else(|pages| (*pages).clone()),
                query_overlay,
                _preload_permit: preload_permit,
            },
            artifacts: V1CompactionArtifacts { component, query },
        })
    }
}

/// Retain one newly encoded page under the same exact admission as loaded
/// pages. The page owner and overlay share one `Bytes` allocation; duplicate
/// content hashes therefore neither allocate nor consume admission twice.
fn retain_generated_page(
    maximum_bytes: usize,
    resident: &mut usize,
    overlay: &mut BTreeMap<[u8; 32], Bytes>,
    hash: [u8; 32],
    bytes: Bytes,
) -> Result<(), Status> {
    if let Some(existing) = overlay.get(&hash) {
        if existing != &bytes {
            return Err(Status::data_loss(
                "v1 compaction page hash names conflicting encoded bytes",
            ));
        }
        return Ok(());
    }
    let retained = resident
        .checked_add(bytes.len())
        .ok_or_else(|| Status::resource_exhausted("v1 compaction retained bytes overflow"))?;
    if retained > maximum_bytes {
        return Err(Status::resource_exhausted(
            "v1 compaction generated pages exceed retained memory admission",
        ));
    }
    overlay.insert(hash, bytes);
    *resident = retained;
    Ok(())
}

fn component_compaction_schedule(
    eligible: usize,
    available_bytes: usize,
) -> Result<(usize, usize), Status> {
    if eligible != 0 && available_bytes < 1024 {
        return Err(Status::resource_exhausted(
            "v1 component compaction has less than its minimum admitted memory",
        ));
    }
    let maximum_memory_jobs = available_bytes.saturating_div(1024).max(1);
    let parallelism = eligible
        .min(MAX_PARALLEL_COMPONENT_COMPACTIONS)
        .min(maximum_memory_jobs)
        .max(1);
    let per_job_bytes = available_bytes.saturating_div(parallelism);
    Ok((parallelism, per_job_bytes))
}

#[allow(clippy::too_many_arguments)]
fn compact_component_stream(
    runtime: &tokio::runtime::Handle,
    publisher: &V1ProjectionPublisher,
    storage_tenant: &str,
    bucket: &str,
    tenant_id: u64,
    bucket_id: u64,
    stream: ComponentStreamRoot,
    component_pages: &BTreeMap<[u8; 32], Bytes>,
    limits: ComponentCompactionLimits,
) -> Result<Option<(ComponentCompactionPlan, Vec<SealedComponentDelta>)>, Status> {
    let read_failure = Arc::new(Mutex::new(None));
    let plan = select_component_compaction(
        stream,
        |hash| {
            component_pages
                .get(&hash)
                .cloned()
                .ok_or(keldra_index::IndexError::Integrity)
        },
        limits,
    )
    .map_err(index_status)?;
    let Some(plan) = plan else {
        return Ok(None);
    };
    let output = compact_component_runs(
        &plan,
        limits,
        TombstoneCompactionPolicy::Retain,
        |reference| {
            blocking_pack(
                runtime,
                &read_failure,
                publisher,
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                reference,
            )
        },
    );
    let output = match output {
        Ok(output) => output,
        Err(error) => {
            return Err(
                recorded_artifact_failure(&read_failure).unwrap_or_else(|| index_status(error))
            );
        }
    };
    Ok(Some((plan, output)))
}

#[allow(clippy::too_many_arguments)]
fn compact_query_stream(
    runtime: &tokio::runtime::Handle,
    publisher: &V1ProjectionPublisher,
    storage_tenant: &str,
    bucket: &str,
    tenant_id: u64,
    bucket_id: u64,
    previous: ProjectionQueryStreamRoot,
    plan: QueryRunCompactionPlan,
    partition: keldra_index::v1::ProjectionPartitionIdentity,
    physical_catalog_generation: [u8; 32],
    limits: QueryBlockLimits,
    credits: QueryBlockCredits,
    query_pages: &BTreeMap<[u8; 32], Bytes>,
) -> Result<ChargedQueryRunCompaction, Status> {
    let read_failure = Arc::new(Mutex::new(None));
    let prepared = prepare_encoded_query_run_compaction(
        previous,
        &plan,
        partition,
        physical_catalog_generation,
        limits,
        credits,
        |hash| {
            blocking_artifact(
                runtime,
                &read_failure,
                publisher,
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                projection_query_run_pack_path(partition, hash),
                hash,
                MAX_ARTIFACT_BYTES,
            )
        },
        |descriptor| {
            blocking_query_block(
                runtime,
                &read_failure,
                publisher,
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                descriptor,
            )
        },
    );
    let prepared = match prepared {
        Ok(prepared) => prepared,
        Err(error) => {
            return Err(
                recorded_artifact_failure(&read_failure).unwrap_or_else(|| index_status(error))
            );
        }
    };
    let pack_table = runtime.block_on(publisher.publish_query_packs(
        storage_tenant,
        bucket,
        tenant_id,
        bucket_id,
        partition,
        prepared.packs(),
    ))?;
    prepared
        .finalize(pack_table, |hash| {
            query_pages
                .get(&hash)
                .cloned()
                .ok_or(keldra_index::IndexError::Integrity)
        })
        .map_err(index_status)
}

#[allow(clippy::too_many_arguments)]
async fn load_component_pages(
    publisher: &V1ProjectionPublisher,
    storage_tenant: &str,
    bucket: &str,
    tenant_id: u64,
    bucket_id: u64,
    partition: keldra_index::v1::ProjectionPartitionIdentity,
    root: ComponentStreamRoot,
    maximum_bytes: usize,
    resident: &mut usize,
    pages: &mut BTreeMap<[u8; 32], Bytes>,
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
        charge_preload(
            resident,
            bytes.len(),
            maximum_bytes,
            "v1 compaction page preload exceeds memory bound",
        )?;
        pending
            .extend(component_stream_child_hashes(root.component, &bytes).map_err(index_status)?);
        pages.insert(hash, Bytes::from(bytes));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn load_query_pages(
    publisher: &V1ProjectionPublisher,
    storage_tenant: &str,
    bucket: &str,
    tenant_id: u64,
    bucket_id: u64,
    partition: keldra_index::v1::ProjectionPartitionIdentity,
    root: [u8; 32],
    maximum_bytes: usize,
    resident: &mut usize,
    pages: &mut BTreeMap<[u8; 32], Bytes>,
) -> Result<(), Status> {
    let mut pending = VecDeque::from([root]);
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
        charge_preload(
            resident,
            bytes.len(),
            maximum_bytes,
            "v1 query compaction page preload exceeds memory bound",
        )?;
        if let QueryRunPage::Branch(children) =
            decode_query_run_page(&bytes).map_err(index_status)?
        {
            pending.extend(children.into_iter().map(|child| child.hash));
        }
        pages.insert(hash, Bytes::from(bytes));
    }
    Ok(())
}

fn charge_preload(
    resident: &mut usize,
    bytes: usize,
    maximum_bytes: usize,
    message: &'static str,
) -> Result<(), Status> {
    *resident = resident
        .checked_add(bytes)
        .filter(|resident| *resident <= maximum_bytes)
        .ok_or_else(|| Status::resource_exhausted(message))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn read_artifact(
    publisher: &V1ProjectionPublisher,
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
        .ok_or_else(|| Status::data_loss("v1 compaction artifact is absent"))
}

#[allow(clippy::too_many_arguments)]
fn blocking_artifact(
    runtime: &tokio::runtime::Handle,
    read_failure: &Arc<Mutex<Option<Status>>>,
    publisher: &V1ProjectionPublisher,
    storage_tenant: &str,
    bucket: &str,
    tenant_id: u64,
    bucket_id: u64,
    path: String,
    hash: [u8; 32],
    maximum_bytes: usize,
) -> Result<Vec<u8>, keldra_index::IndexError> {
    runtime
        .block_on(read_artifact(
            publisher,
            storage_tenant,
            bucket,
            tenant_id,
            bucket_id,
            path,
            hash,
            maximum_bytes,
        ))
        .map_err(|error| {
            *read_failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error.clone());
            keldra_index::IndexError::Io(error.to_string())
        })
}

#[allow(clippy::too_many_arguments)]
fn blocking_pack(
    runtime: &tokio::runtime::Handle,
    read_failure: &Arc<Mutex<Option<Status>>>,
    publisher: &V1ProjectionPublisher,
    storage_tenant: &str,
    bucket: &str,
    tenant_id: u64,
    bucket_id: u64,
    reference: &keldra_index::v1::ArtifactPackReference,
) -> Result<Vec<u8>, keldra_index::IndexError> {
    runtime
        .block_on(publisher.read_exact_artifact_pack(
            storage_tenant,
            bucket,
            tenant_id,
            bucket_id,
            reference,
        ))
        .map(|bytes| bytes.to_vec())
        .map_err(|error| {
            *read_failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error.clone());
            keldra_index::IndexError::Io(error.to_string())
        })
}

#[allow(clippy::too_many_arguments)]
fn blocking_query_block(
    runtime: &tokio::runtime::Handle,
    read_failure: &Arc<Mutex<Option<Status>>>,
    publisher: &V1ProjectionPublisher,
    storage_tenant: &str,
    bucket: &str,
    tenant_id: u64,
    bucket_id: u64,
    descriptor: &keldra_index::v1::QueryBlockDescriptor,
) -> Result<Vec<u8>, keldra_index::IndexError> {
    let pack = descriptor.pack_reference()?;
    let bytes = blocking_pack(
        runtime,
        read_failure,
        publisher,
        storage_tenant,
        bucket,
        tenant_id,
        bucket_id,
        pack,
    )?;
    let range = descriptor.locator.range()?;
    let block = bytes
        .get(range)
        .ok_or(keldra_index::IndexError::Integrity)?;
    if *keldra_index::profiled_blake3_hash!(block).as_bytes() != descriptor.hash {
        return Err(keldra_index::IndexError::Integrity);
    }
    Ok(block.to_vec())
}

fn recorded_artifact_failure(read_failure: &Arc<Mutex<Option<Status>>>) -> Option<Status> {
    read_failure
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
}

pub(super) fn index_status(error: keldra_index::IndexError) -> Status {
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
    use keldra_index::v1::{
        ArtifactPackTable, PreparedQueryMutationBatch, ProjectionCurrent,
        ProjectionPartitionIdentity, ProjectionQueryStreamRoot, QueryMemoryPermit,
        QueryRunReference, append_query_run_path_copy, prepare_projection_query_run,
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

    #[test]
    fn component_compaction_schedule_is_bounded_and_preserves_single_job_budget() {
        assert_eq!(
            component_compaction_schedule(1, 64 * 1024).unwrap(),
            (1, 64 * 1024)
        );
        assert_eq!(
            component_compaction_schedule(8, 64 * 1024).unwrap(),
            (4, 16 * 1024)
        );
        assert_eq!(
            component_compaction_schedule(4, 2 * 1024).unwrap(),
            (2, 1024)
        );

        let (parallelism, per_job_bytes) = component_compaction_schedule(8, 64 * 1024).unwrap();
        assert!(parallelism <= MAX_PARALLEL_COMPONENT_COMPACTIONS);
        assert!(parallelism * per_job_bytes <= 64 * 1024);
    }

    #[test]
    fn component_compaction_schedule_never_manufactures_memory() {
        let error = component_compaction_schedule(1, 1023).unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert_eq!(component_compaction_schedule(0, 0).unwrap(), (1, 0));
    }

    #[test]
    fn recorded_artifact_status_preserves_retryability() {
        let failure = Arc::new(Mutex::new(Some(Status::unavailable("retry"))));
        let recovered = recorded_artifact_failure(&failure).unwrap();
        assert_eq!(recovered.code(), tonic::Code::Unavailable);
        assert!(recorded_artifact_failure(&failure).is_none());
    }

    #[test]
    fn preload_charge_is_shared_across_component_and_query_phases() {
        let mut resident = 0;
        charge_preload(&mut resident, 600, 1024, "component").unwrap();
        let error = charge_preload(&mut resident, 425, 1024, "query").unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert_eq!(resident, 600);
    }

    #[test]
    fn generated_page_is_shared_and_charged_once() {
        let mut resident = 7;
        let mut overlay = BTreeMap::new();
        let page = Bytes::from_static(b"generated page");
        retain_generated_page(64, &mut resident, &mut overlay, [8; 32], page.clone()).unwrap();
        assert_eq!(resident, 7 + page.len());
        assert_eq!(overlay[&[8; 32]].as_ptr(), page.as_ptr());

        retain_generated_page(64, &mut resident, &mut overlay, [8; 32], page).unwrap();
        assert_eq!(resident, 7 + b"generated page".len());
    }

    #[test]
    fn generated_page_cannot_exceed_retained_admission() {
        let mut resident = 7;
        let mut overlay = BTreeMap::new();
        let error = retain_generated_page(
            8,
            &mut resident,
            &mut overlay,
            [8; 32],
            Bytes::from_static(b"two bytes"),
        )
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert_eq!(resident, 7);
        assert!(overlay.is_empty());
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
            .unwrap()
            .finalize(ArtifactPackTable::empty())
            .unwrap();
            let artifacts = charged.artifacts().clone();
            let reference = QueryRunReference {
                hash: artifacts.run.hash,
                encoded_bytes: u64::try_from(artifacts.run.bytes.len())
                    .expect("query run length fits u64"),
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
                maximum_input_runs: QueryBlockLimits::default_for_memory().maximum_loaded_blocks,
            },
        )
        .unwrap()
        .unwrap();
        let compacted = prepare_encoded_query_run_compaction(
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
            |_| Err::<Vec<u8>, _>(keldra_index::IndexError::Integrity),
        )
        .unwrap()
        .finalize(ArtifactPackTable::empty(), |hash| {
            pages
                .get(&hash)
                .cloned()
                .ok_or(keldra_index::IndexError::Integrity)
        })
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
        .unwrap()
        .finalize(ArtifactPackTable::empty())
        .unwrap();
        let next_artifacts = next.artifacts().clone();
        let appended = append_query_run_path_copy(
            Some(compacted.splice().root),
            partition,
            catalog,
            QueryRunReference {
                hash: next_artifacts.run.hash,
                encoded_bytes: u64::try_from(next_artifacts.run.bytes.len())
                    .expect("query run length fits u64"),
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
