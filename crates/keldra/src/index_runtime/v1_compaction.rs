//! Bounded format-v1 LSM compaction at the partition Current boundary.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use keldra_index::v1::{
    ArtifactPackTable, COMPONENT_STREAM_DIRECTORY_FANOUT, ChargedProjectionDeltaPacks,
    ChargedQueryRunCompaction, ComponentCompactionLimits, ComponentCompactionPlan,
    ComponentStreamRoot, EncodedComponentStreamPage, IndexingMemoryPermit, PackedComponentDelta,
    ProjectionGeneration, ProjectionPackCredits, ProjectionQueryStreamRoot, QUERY_RUN_PAGE_FANOUT,
    QueryBlockCredits, QueryBlockLimits, QueryRunCompactionLimits, QueryRunCompactionPlan,
    SealedComponentDelta, TombstoneCompactionPolicy, compact_component_runs, pack_component_deltas,
    prepare_encoded_query_run_compaction, projection_query_run_pack_path,
    projection_query_run_stream_page_path, projection_stream_page_path,
    select_component_compaction, select_query_run_compaction, splice_compacted_component_runs,
};
use tonic::Status;

use super::cpu::IndexCpuPool;
use super::v1_parallel::run_bounded_ordered;
use super::v1_publication::{LoadedV1ProjectionGeneration, V1ProjectionPublisher};

const MAX_ARTIFACT_BYTES: usize = 64 * 1024 * 1024;

fn component_output_run_limit(maximum_unmerged_bytes: usize) -> usize {
    // A merge can use the shared working-memory budget, but each indivisible
    // encoded child must fit the existing physical pack format's hard bound.
    maximum_unmerged_bytes
        .min(keldra_index::v1::ARTIFACT_PACK_MAX_BYTES)
        .max(1024)
}
const MAX_PAGE_BYTES: usize = 32 * 1024;
const MAX_PARALLEL_COMPONENT_COMPACTIONS: usize = 4;

#[path = "v1_compaction_memory.rs"]
mod memory;
use memory::{CompactionTreeAccess, admission_error};

pub(crate) struct V1ComponentCompaction {
    pub(crate) packs: ChargedProjectionDeltaPacks,
    pub(crate) pages: Vec<EncodedComponentStreamPage>,
    proposals: Vec<V1ComponentCompactionProposal>,
    _metadata_permits: Vec<IndexingMemoryPermit>,
}

struct V1ComponentCompactionProposal {
    plan: ComponentCompactionPlan,
    output: Vec<PackedComponentDelta>,
    pack_table: ArtifactPackTable,
}

pub(crate) struct V1CompactionPublication {
    expected_generation_hash: [u8; 32],
    base: V1CompactionBase,
    artifacts: V1CompactionArtifacts,
}

pub(crate) struct V1CompactionBase {
    pub(crate) predecessor: ProjectionGeneration,
    component_overlay: BTreeMap<[u8; 32], Bytes>,
    query_overlay: BTreeMap<[u8; 32], Bytes>,
    access: CompactionTreeAccess,
    cpu: IndexCpuPool,
}

pub(crate) struct V1CompactionArtifacts {
    pub(crate) component: Option<V1ComponentCompaction>,
    pub(crate) query: Option<ChargedQueryRunCompaction>,
}

impl V1CompactionPublication {
    pub(crate) fn expected_generation_hash(&self) -> [u8; 32] {
        self.expected_generation_hash
    }

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
        mut self,
        publisher: &V1ProjectionPublisher,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        current: &LoadedV1ProjectionGeneration,
    ) -> Result<Option<Self>, Status> {
        let cpu = self.base.cpu.clone();
        let current = current.clone();
        // Every retained allocation travels into the owned native closure;
        // cancellation cannot uncharge queued or still-running CPU work.
        let _ = (publisher, storage_tenant, bucket, tenant_id, bucket_id);
        cpu.submit(move || {
            if self.rebase_native(&current)? {
                Ok(Some(self))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
    }

    fn rebase_native(&mut self, current: &LoadedV1ProjectionGeneration) -> Result<bool, Status> {
        if self.expected_generation_hash == current.current.generation_hash {
            return Ok(true);
        }
        let mut rebased = current.generation.clone();
        let mut component_overlay = BTreeMap::new();
        let mut query_overlay = BTreeMap::new();
        if let Some(component) = self.artifacts.component.as_mut() {
            let mut pages = Vec::new();
            for proposal in &component.proposals {
                let index = rebased
                    .roots
                    .binary_search_by_key(&proposal.plan.component(), |root| root.component)
                    .map_err(|_| Status::data_loss("compaction component disappeared"))?;
                let stream = ComponentStreamRoot::from_component_root(&rebased.roots[index])
                    .map_err(index_status)?;
                let access = self.base.access.clone().with_output_reservation()?;
                let mut spliced = match splice_compacted_component_runs(
                    stream,
                    &proposal.plan,
                    &proposal.output,
                    &proposal.pack_table,
                    |hash| access.page(hash, false),
                ) {
                    Ok(spliced) => spliced,
                    Err(keldra_index::IndexError::StaleProposal) => return Ok(false),
                    Err(error) => return Err(index_status(error)),
                };
                access.seal_output(spliced.new_pages.iter().map(|page| page.bytes.len()))?;
                access.retain_component_pages(&mut spliced.new_pages);
                rebased.roots[index] = spliced.root.component_root().map_err(index_status)?;
                for page in &spliced.new_pages {
                    component_overlay.insert(page.hash, page.bytes.clone());
                }
                pages.extend(spliced.new_pages);
            }
            component.pages = pages;
        }
        if let Some(query) = self.artifacts.query.as_mut() {
            let access = self.base.access.clone().with_output_reservation()?;
            match query.rebase(rebased.query_stream_root, |hash| access.page(hash, true)) {
                Ok(()) => {}
                Err(keldra_index::IndexError::StaleProposal) => return Ok(false),
                Err(error) => return Err(index_status(error)),
            }
            access.seal_output(query.splice().pages.iter().map(|page| page.bytes.len()))?;
            query.retain_splice_page_owner(access.output_owner());
            rebased.query_stream_root = query.splice().root;
            for page in &query.splice().pages {
                query_overlay.insert(page.hash, page.bytes.clone());
            }
        }
        rebased.validate().map_err(index_status)?;
        // Only pages produced by this exact splice remain. Read authority is
        // the immutable object tree; previous-generation overlays are dropped.
        self.base.predecessor = rebased;
        self.base.component_overlay = component_overlay;
        self.base.query_overlay = query_overlay;
        self.expected_generation_hash = current.current.generation_hash;
        Ok(true)
    }
}

impl V1CompactionBase {
    pub(crate) fn reserve_publication_metadata(
        &self,
        bytes: usize,
    ) -> Result<IndexingMemoryPermit, Status> {
        self.access
            .credits
            .acquire(keldra_index::v1::IndexingMemoryStage::WorkerScratch, bytes)
            .map_err(|error| index_status(admission_error(error)))
    }
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
        credits: &keldra_index::v1::IndexingMemoryCredits,
    ) -> Result<V1CompactionPublication, Status> {
        let started = Instant::now();
        let partition = loaded.generation.partition;
        let access = CompactionTreeAccess {
            publisher: self.clone(),
            storage_tenant: storage_tenant.to_owned(),
            bucket: bucket.to_owned(),
            tenant_id,
            bucket_id,
            partition,
            credits: credits.clone(),
            runtime: tokio::runtime::Handle::current(),
            output: None,
        };
        let component_fan_in = maximum_runs.min(COMPONENT_STREAM_DIRECTORY_FANOUT).max(2);
        let component_limits = ComponentCompactionLimits {
            l0_trigger: component_fan_in.min(8),
            maximum_input_runs: component_fan_in,
            maximum_loaded_pack_bytes: credits.total_limit_bytes(),
            maximum_output_run_bytes: component_output_run_limit(maximum_unmerged_bytes),
        };
        let eligible = loaded
            .generation
            .roots
            .iter()
            .filter(|root| {
                root.segment_count >= maximum_runs as u64
                    || root.encoded_bytes >= maximum_unmerged_bytes as u64
            })
            .map(|root| ComponentStreamRoot::from_component_root(root).map_err(index_status))
            .collect::<Result<Vec<_>, _>>()?;
        let width = eligible
            .len()
            .min(MAX_PARALLEL_COMPONENT_COMPACTIONS)
            .max(1);
        let work = eligible.into_iter().enumerate().collect();
        let job_access = access.clone();
        let job_cpu = cpu.clone();
        let outputs = run_bounded_ordered(work, width, move |stream| {
            let access = job_access.clone();
            let cpu = job_cpu.clone();
            async move {
                cpu.submit(move || {
                    compact_component_stream_accounted(&access, stream, component_limits)
                })
                .await
                .map_err(|error| Status::internal(error.to_string()))?
            }
        })
        .await?;
        let mut plans = Vec::new();
        let mut sealed = Vec::new();
        let mut raw_owners = Vec::new();
        for (_, output) in outputs {
            let output = match output {
                Ok(output) => output,
                // Publish independently admitted siblings instead of dropping
                // useful work when their concurrent neighbour lacked scratch.
                Err(error) if error.code() == tonic::Code::ResourceExhausted => continue,
                Err(error) => return Err(error),
            };
            if let Some((plan, stream, output, permit)) = output {
                plans.push((plan, stream));
                sealed.extend(output);
                raw_owners.push(permit);
            }
        }
        let mut predecessor = loaded.generation.clone();
        let mut component_overlay = BTreeMap::new();
        let mut query_overlay = BTreeMap::new();
        let component = if plans.is_empty() {
            None
        } else {
            let packed_bytes = sealed
                .iter()
                .try_fold(0usize, |total, delta| total.checked_add(delta.bytes.len()))
                .ok_or_else(|| Status::resource_exhausted("compaction output size overflow"))?;
            // Destination and raw output coexist during packing.
            let permit = credits
                .acquire(
                    keldra_index::v1::IndexingMemoryStage::WorkerScratch,
                    packed_bytes,
                )
                .map_err(|error| index_status(admission_error(error)))?;
            let packs =
                pack_component_deltas(sealed, ProjectionPackCredits::from_pipeline_permit(permit))
                    .map_err(index_status)?;
            let table = self
                .publish_component_packs(
                    storage_tenant,
                    bucket,
                    tenant_id,
                    bucket_id,
                    partition,
                    &packs.packs,
                )
                .await?;
            let mut proposals = Vec::new();
            let mut pages = Vec::new();
            for (plan, stream) in plans {
                let output = packs
                    .packs
                    .iter()
                    .flat_map(|pack| &pack.deltas)
                    .filter(|delta| delta.component == plan.component())
                    .cloned()
                    .collect::<Vec<_>>();
                let splice_plan = plan.clone();
                let splice_output = output.clone();
                let splice_table = table.clone();
                let splice_access = access.clone();
                let (spliced, _owner) = cpu
                    .submit(move || {
                        let access = splice_access.with_output_reservation()?;
                        let mut spliced = splice_compacted_component_runs(
                            stream,
                            &splice_plan,
                            &splice_output,
                            &splice_table,
                            |hash| access.page(hash, false),
                        )
                        .map_err(index_status)?;
                        access
                            .seal_output(spliced.new_pages.iter().map(|page| page.bytes.len()))?;
                        access.retain_component_pages(&mut spliced.new_pages);
                        Ok::<_, Status>((spliced, access.output_owner()))
                    })
                    .await
                    .map_err(|error| Status::internal(error.to_string()))??;
                let index = predecessor
                    .roots
                    .binary_search_by_key(&plan.component(), |root| root.component)
                    .map_err(|_| Status::data_loss("compacted component disappeared"))?;
                predecessor.roots[index] = spliced.root.component_root().map_err(index_status)?;
                for page in &spliced.new_pages {
                    component_overlay.insert(page.hash, page.bytes.clone());
                }
                pages.extend(spliced.new_pages);
                proposals.push(V1ComponentCompactionProposal {
                    plan,
                    output,
                    pack_table: table.clone(),
                });
            }
            let metadata_bytes = proposals
                .iter()
                .try_fold(
                    proposals.capacity() * std::mem::size_of::<V1ComponentCompactionProposal>(),
                    |total, proposal| {
                        total
                            .checked_add(component_plan_resident_bytes(&proposal.plan).ok()?)
                            .and_then(|value| {
                                value.checked_add(
                                    proposal.output.capacity()
                                        * std::mem::size_of::<PackedComponentDelta>(),
                                )
                            })
                    },
                )
                .and_then(|value| value.checked_add(table.resident_bytes()))
                .and_then(|value| {
                    value.checked_add(
                        packs.packs.capacity()
                            * std::mem::size_of::<keldra_index::v1::SealedProjectionDeltaPack>(),
                    )
                })
                .and_then(|value| {
                    value.checked_add(
                        packs
                            .packs
                            .iter()
                            .map(|pack| {
                                pack.deltas.capacity() * std::mem::size_of::<PackedComponentDelta>()
                            })
                            .sum::<usize>(),
                    )
                })
                .ok_or_else(|| Status::resource_exhausted("merge metadata size overflow"))?;
            // Raw encoded outputs have been consumed. Retain their original
            // accounted owner only for the proposal/pack metadata now alive.
            let mut remaining = metadata_bytes;
            for owner in &mut raw_owners {
                let charge = remaining.min(owner.bytes());
                owner.shrink_to(charge).map_err(index_status)?;
                remaining -= charge;
            }
            if remaining != 0 {
                return Err(Status::data_loss(
                    "merge metadata exceeded construction admission",
                ));
            }
            Some(V1ComponentCompaction {
                packs,
                pages,
                proposals,
                _metadata_permits: raw_owners,
            })
        };
        let query = if loaded.generation.query_stream_root.run_count >= maximum_runs as u64 {
            let query_access = access.clone();
            let previous = loaded.generation.query_stream_root;
            let physical_generation = loaded.generation.physical_catalog_generation;
            let compacted = cpu
                .submit(move || {
                    let limits = QueryBlockLimits::default_for_memory();
                    let fan_in = maximum_runs
                        .min(QUERY_RUN_PAGE_FANOUT)
                        .min(limits.maximum_loaded_blocks)
                        .max(2);
                    let candidate_bytes = fan_in
                        .checked_mul(2 * std::mem::size_of::<keldra_index::v1::QueryRunReference>())
                        .ok_or_else(|| {
                            Status::resource_exhausted("query merge candidate bytes overflow")
                        })?;
                    let permit = query_access
                        .credits
                        .acquire(
                            keldra_index::v1::IndexingMemoryStage::OrderingCatalog,
                            candidate_bytes,
                        )
                        .map_err(|error| index_status(admission_error(error)))?;
                    let plan = select_query_run_compaction(
                        previous,
                        |hash| query_access.page(hash, true),
                        QueryRunCompactionLimits {
                            level_trigger: fan_in.min(8),
                            maximum_input_runs: fan_in,
                        },
                    )
                    .map_err(index_status)?
                    .ok_or_else(|| Status::resource_exhausted("no bounded query merge window"))?;
                    let mut query_credits = QueryBlockCredits::from_growable_pipeline_permit(
                        permit,
                        query_access.credits.total_limit_bytes(),
                    )
                    .map_err(index_status)?;
                    query_credits
                        .reserve(
                            plan.inputs_newest_first().len()
                                * 2
                                * std::mem::size_of::<keldra_index::v1::QueryRunReference>(),
                        )
                        .map_err(index_status)?;
                    let access = query_access.with_output_reservation()?;
                    let mut compacted = compact_query_stream(
                        &access.runtime,
                        &access.publisher,
                        &access.storage_tenant,
                        &access.bucket,
                        access.tenant_id,
                        access.bucket_id,
                        previous,
                        plan,
                        partition,
                        physical_generation,
                        limits,
                        query_credits,
                        &access,
                    )?;
                    access.seal_output(
                        compacted.splice().pages.iter().map(|page| page.bytes.len()),
                    )?;
                    compacted.retain_splice_page_owner(access.output_owner());
                    Ok::<_, Status>((compacted, access.output_owner()))
                })
                .await
                .map_err(|error| Status::internal(error.to_string()))?;
            match compacted {
                Ok((compacted, owner)) => {
                    let _owner = owner;
                    predecessor.query_stream_root = compacted.splice().root;
                    for page in &compacted.splice().pages {
                        query_overlay.insert(page.hash, page.bytes.clone());
                    }
                    Some(compacted)
                }
                Err(error) if error.code() == tonic::Code::ResourceExhausted => None,
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        if component.is_none() && query.is_none() {
            return Err(Status::resource_exhausted(
                "no merge window admitted by shared working memory",
            ));
        }
        predecessor.validate().map_err(index_status)?;
        tracing::info!(
            histogram.keldra_index_v1_compaction_duration_seconds = started.elapsed().as_secs_f64(),
            component_parallelism = width,
            query_compacted = query.is_some(),
            "keldra_index_v1_compaction"
        );
        Ok(V1CompactionPublication {
            expected_generation_hash: loaded.current.generation_hash,
            base: V1CompactionBase {
                predecessor,
                component_overlay,
                query_overlay,
                access,
                cpu: cpu.clone(),
            },
            artifacts: V1CompactionArtifacts { component, query },
        })
    }
}

fn compact_component_stream_accounted(
    access: &CompactionTreeAccess,
    stream: ComponentStreamRoot,
    limits: ComponentCompactionLimits,
) -> Result<
    Option<(
        ComponentCompactionPlan,
        ComponentStreamRoot,
        Vec<SealedComponentDelta>,
        IndexingMemoryPermit,
    )>,
    Status,
> {
    // Selected descriptors and their single-pack table allocations survive
    // the traversal stack. Admit the bounded candidate collection before the
    // selector allocates it; never let escaped table Arcs outlive accounting.
    let path_bytes = keldra_index::v1::projection_pack_path(access.partition, [0; 32]).len();
    let candidate_bytes = limits
        .maximum_input_runs
        .checked_mul(2)
        .and_then(|count| {
            count.checked_mul(
                std::mem::size_of::<keldra_index::v1::ComponentSegmentDescriptor>()
                    + std::mem::size_of::<keldra_index::v1::ArtifactPackReference>()
                    + path_bytes
                    + 32,
            )
        })
        .ok_or_else(|| Status::resource_exhausted("merge candidate metadata bound overflow"))?;
    let mut permit = access
        .credits
        .acquire(
            keldra_index::v1::IndexingMemoryStage::WorkerScratch,
            candidate_bytes,
        )
        .map_err(|error| index_status(admission_error(error)))?;
    let plan = select_component_compaction(stream, |hash| access.page(hash, false), limits)
        .map_err(index_status)?;
    let Some(plan) = plan else {
        return Ok(None);
    };
    let mut identities = BTreeSet::new();
    let mut pack_bytes = 0usize;
    let mut record_bytes = 0usize;
    let mut records = 0usize;
    for run in plan.input_runs() {
        let reference = run.pack_reference().map_err(index_status)?;
        if identities.insert((reference.hash, reference.object_version, reference.length)) {
            pack_bytes = pack_bytes
                .checked_add(usize::try_from(reference.length).map_err(|_| {
                    Status::resource_exhausted("merge pack length exceeds platform")
                })?)
                .ok_or_else(|| Status::resource_exhausted("merge pack size overflow"))?;
        }
        record_bytes =
            record_bytes
                .checked_add(usize::try_from(run.locator.encoded_bytes).map_err(|_| {
                    Status::resource_exhausted("merge record length exceeds platform")
                })?)
                .ok_or_else(|| Status::resource_exhausted("merge record size overflow"))?;
        records =
            records
                .checked_add(usize::try_from(run.records).map_err(|_| {
                    Status::resource_exhausted("merge record count exceeds platform")
                })?)
                .ok_or_else(|| Status::resource_exhausted("merge record count overflow"))?;
    }
    // Input Bytes and the component cursor's Vec copy can coexist. Resident
    // replacement-map records retain their separate 160-byte workspace charge;
    // each record additionally bounds a worst-case standalone encoded header
    // and restart entry. This is selected-input-sized construction headroom.
    let needed = pack_bytes
        .checked_mul(2)
        .and_then(|value| value.checked_add(record_bytes.checked_mul(2)?))
        .and_then(|value| value.checked_add(records.checked_mul(160 + 119 + 16)?))
        .and_then(|value| value.checked_add(component_plan_resident_bytes(&plan).ok()?))
        .ok_or_else(|| Status::resource_exhausted("merge construction bound overflow"))?;
    permit.grow_to(needed).map_err(|error| {
        tracing::debug!(
            counter.keldra_index_v1_compaction_admission_refused_total = 1_u64,
            "keldra_index_v1_compaction_admission"
        );
        index_status(admission_error(error))
    })?;
    tracing::debug!(
        histogram.keldra_index_v1_compaction_selected_input_bytes = pack_bytes as f64,
        histogram.keldra_index_v1_compaction_selected_fan_in = plan.input_count() as f64,
        "keldra_index_v1_compaction_selected"
    );
    let failures = Arc::new(Mutex::new(None));
    let output = compact_component_runs(
        &plan,
        limits,
        TombstoneCompactionPolicy::Retain,
        |reference| {
            blocking_pack(
                &access.runtime,
                &failures,
                &access.publisher,
                &access.storage_tenant,
                &access.bucket,
                access.tenant_id,
                access.bucket_id,
                reference,
            )
        },
    );
    let output = output.map_err(|error| {
        recorded_artifact_failure(&failures).unwrap_or_else(|| index_status(error))
    })?;
    let retained = output
        .iter()
        .try_fold(
            output.capacity() * std::mem::size_of::<SealedComponentDelta>(),
            |total, delta| total.checked_add(delta.bytes.capacity()),
        )
        .and_then(|value| value.checked_add(component_plan_resident_bytes(&plan).ok()?))
        .ok_or_else(|| Status::resource_exhausted("merge retained output size overflow"))?;
    if retained > permit.bytes() {
        return Err(Status::data_loss(
            "component merge exceeded its proven construction bound",
        ));
    }
    permit.shrink_to(retained).map_err(index_status)?;
    Ok(Some((plan, stream, output, permit)))
}

fn component_plan_resident_bytes(plan: &ComponentCompactionPlan) -> Result<usize, Status> {
    let mut tables = BTreeSet::new();
    plan.input_runs().iter().try_fold(
        plan.input_count()
            * 2
            * std::mem::size_of::<keldra_index::v1::ComponentSegmentDescriptor>(),
        |total, run| {
            let heap = if tables.insert(run.pack_table.entries().as_ptr() as usize) {
                run.pack_table
                    .resident_bytes()
                    .saturating_sub(std::mem::size_of::<ArtifactPackTable>())
            } else {
                0
            };
            total
                .checked_add(heap)
                .ok_or_else(|| Status::resource_exhausted("merge plan metadata size overflow"))
        },
    )
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
    access: &CompactionTreeAccess,
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
        .finalize(pack_table, |hash| access.page(hash, true))
        .map_err(index_status)
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
    let bytes = runtime
        .block_on(
            publisher.read_exact_artifact_pack_range(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                pack,
                descriptor.locator.offset,
                descriptor.locator.encoded_bytes,
                usize::try_from(descriptor.encoded_bytes)
                    .map_err(|_| keldra_index::IndexError::OffsetOverflow)?,
                descriptor.hash,
                None,
            ),
        )
        .map_err(|error| {
            *read_failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error.clone());
            keldra_index::IndexError::Io(error.to_string())
        })?;
    if *keldra_index::profiled_blake3_hash!(&bytes).as_bytes() != descriptor.hash {
        return Err(keldra_index::IndexError::Integrity);
    }
    Ok(bytes.to_vec())
}

fn recorded_artifact_failure(read_failure: &Arc<Mutex<Option<Status>>>) -> Option<Status> {
    read_failure
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
}

pub(super) fn index_status(error: keldra_index::IndexError) -> Status {
    match error {
        keldra_index::IndexError::DeadlineExceeded => Status::deadline_exceeded(error.to_string()),
        keldra_index::IndexError::AdmissionDenied(message) => Status::resource_exhausted(message),
        keldra_index::IndexError::ResourceLimit { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        keldra_index::IndexError::Io(message) => Status::unavailable(message),
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
    fn component_output_limit_matches_indivisible_physical_child_not_total_memory() {
        let cap = keldra_index::v1::ARTIFACT_PACK_MAX_BYTES;
        assert_eq!(component_output_run_limit(1024 * 1024 * 1024), cap);
        assert_eq!(component_output_run_limit(MAX_ARTIFACT_BYTES), cap);
        assert_eq!(component_output_run_limit(4 * 1024 * 1024), 4 * 1024 * 1024);
        assert_eq!(component_output_run_limit(1), 1024);
    }

    #[test]
    fn recorded_artifact_status_preserves_retryability() {
        let failure = Arc::new(Mutex::new(Some(Status::unavailable("retry"))));
        let recovered = recorded_artifact_failure(&failure).unwrap();
        assert_eq!(recovered.code(), tonic::Code::Unavailable);
        assert!(recorded_artifact_failure(&failure).is_none());
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
