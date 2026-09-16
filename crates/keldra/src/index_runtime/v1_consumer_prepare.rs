//! Bounded preparation of one format-v1 producer mutation window.

use super::*;

const PREPARATION_CHUNK_MUTATIONS: usize = 256;

pub(super) type PreparedChunks = BTreeMap<
    usize,
    (
        Vec<(Mutation, u64, Arc<IndexingMemoryPermit>)>,
        Vec<V1PreparationSlot>,
    ),
>;

pub(super) struct PreparedWindow {
    pub(super) chunks: PreparedChunks,
    mutation_count: usize,
    next_chunk_ordinal: usize,
    exact_duration: Duration,
    selection_duration: Duration,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn prepare_window(
    recipe: Arc<PhysicalCatalogRecipe>,
    source: SourceId,
    physical_catalog_identity: [u8; 32],
    mutations: Vec<Mutation>,
    reader: &ClusterObjectReader,
    extractor: &V1ProjectionExtractor,
    credits: &IndexingMemoryCredits,
    limits: Limits,
) -> Result<PreparedWindow, Status> {
    let telemetry = super::super::v1_telemetry::global();
    let prepare_timer =
        super::super::v1_telemetry::V1PipelineTelemetry::start_phase(&telemetry.prepare_nanos);
    let mutation_count = mutations.len();
    let scope = source_scope(source);
    let mut prepared_chunks = BTreeMap::new();
    let mut next_chunk_ordinal = 0usize;
    let mut exact_duration = Duration::ZERO;
    let mut selected_inputs = VecDeque::from(mutations);
    let mut jobs = tokio::task::JoinSet::new();
    let mut first_failure = None;
    let selection_started = Instant::now();
    while !selected_inputs.is_empty() || !jobs.is_empty() {
        let refill = preparation_refill_size(selected_inputs.len(), jobs.len(), limits.parallelism);
        for _ in 0..refill {
            let chunk_len = preparation_chunk_size(selected_inputs.len());
            let construction = acquire_preparation_construction(credits, chunk_len)?;
            let chunk = (0..chunk_len)
                .filter_map(|_| selected_inputs.pop_front())
                .collect::<Vec<_>>();
            // Descriptor reads charge only this bounded metadata vector.
            // Streaming selection separately admits source-derived scratch and
            // transfers into measured retained input before CPU preparation.
            let extractor = extractor.clone();
            let recipe = recipe.clone();
            let credits = credits.clone();
            let reader = reader.clone();
            let chunk_ordinal = next_chunk_ordinal;
            next_chunk_ordinal = next_chunk_ordinal.saturating_add(1);
            jobs.spawn(async move {
                let _construction = Arc::new(construction);
                let mut metadata = Vec::with_capacity(chunk.len());
                let mut prepare_inputs = Vec::with_capacity(chunk.len());
                let mut exact_duration = Duration::ZERO;
                let exact_timer = super::super::v1_telemetry::V1PipelineTelemetry::start_phase(
                    &telemetry.exact_source_read_nanos,
                );
                let exact_requests = chunk
                    .iter()
                    .map(|mutation| ExactMutationRequest {
                        path: &mutation.path,
                        canonical_path: mutation.canonical_path.as_deref(),
                        version: mutation.version,
                        deleted: mutation.deleted,
                    })
                    .collect::<Vec<_>>();
                let sources = load_exact_mutations(&reader, &recipe, &exact_requests, 1).await?;
                exact_duration = exact_duration.saturating_add(exact_timer.elapsed());
                drop(exact_timer);

                // Replacement segments invalidate old material through pinned
                // document liveness. Preparation needs only the exact new
                // source; it never reconstructs historical field components.
                for (mutation, source) in chunk.into_iter().zip(sources) {
                    let value = select_mutation(
                        extractor.clone(),
                        recipe.clone(),
                        physical_catalog_identity,
                        mutation,
                        source,
                        &credits,
                    )
                    .await?;
                    let Some(value) = value else {
                        continue;
                    };
                    let SelectedMutation {
                        mutation,
                        selected,
                        source_bytes,
                        _input,
                    } = value;
                    let _input = Arc::new(_input);
                    metadata.push((mutation, source_bytes, _input.clone()));
                    prepare_inputs.push(V1PreparationSlot {
                        selected,
                        credits: empty_query_credits(&credits, limits)?,
                        prepared: None,
                        source_memory: Some(_input),
                        workspace_memory: Some(_construction.clone()),
                    });
                }
                let mut prepared = extractor
                    .prepare_batch_owned(scope, recipe, prepare_inputs)
                    .await?;
                for slot in &mut prepared {
                    slot.workspace_memory = None;
                    drop(slot.source_memory.take());
                }
                Ok::<_, Status>((chunk_ordinal, (metadata, prepared), exact_duration))
            });
        }
        if !selected_inputs.is_empty()
            && preparation_refill_size(selected_inputs.len(), jobs.len(), limits.parallelism) > 0
        {
            continue;
        }
        if let Some(joined) = jobs.join_next().await {
            match joined {
                Ok(Ok((ordinal, values, chunk_exact))) => {
                    exact_duration = exact_duration.saturating_add(chunk_exact);
                    let replaced = prepared_chunks.insert(ordinal, values);
                    debug_assert!(replaced.is_none(), "preparation chunk ordinal is unique");
                }
                Ok(Err(error)) => {
                    first_failure.get_or_insert(error);
                    selected_inputs.clear();
                }
                Err(error) => {
                    first_failure.get_or_insert_with(|| {
                        Status::internal(format!("v1 selection task failed: {error}"))
                    });
                    selected_inputs.clear();
                }
            }
        }
    }
    if let Some(error) = first_failure {
        return Err(error);
    }
    let selection_duration = selection_started.elapsed();
    drop(prepare_timer);
    Ok(PreparedWindow {
        chunks: prepared_chunks,
        mutation_count,
        next_chunk_ordinal,
        exact_duration,
        selection_duration,
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn prepare_lane(
    writer: &mut Writer,
    physical_catalog_identity: [u8; 32],
    mutations: Vec<Mutation>,
    safe_next: u64,
    reader: &ClusterObjectReader,
    extractor: &V1ProjectionExtractor,
    publisher: &V1ProjectionPublisher,
    credits: &IndexingMemoryCredits,
    limits: Limits,
) -> Result<(), Status> {
    let started = Instant::now();
    let mut remaining = Vec::new();
    let mut cached_metadata = Vec::new();
    let mut cached_slots = Vec::new();
    for mutation in mutations {
        if let Some((metadata, slot)) = writer
            .look_ahead_prepared
            .remove(&(mutation.offset, mutation.ordinal))
        {
            if metadata.0.path != mutation.path || metadata.0.version != mutation.version {
                return Err(Status::data_loss("look-ahead source identity changed"));
            }
            cached_metadata.push(metadata);
            cached_slots.push(slot);
        } else {
            remaining.push(mutation);
        }
    }
    // Coalescing may supersede speculative rows. Drop only rows within this
    // consumed cut; later indivisible publication chunks retain their slots.
    writer
        .look_ahead_prepared
        .retain(|(offset, _), _| *offset >= safe_next);
    let PreparedWindow {
        mut chunks,
        mut mutation_count,
        mut next_chunk_ordinal,
        exact_duration,
        selection_duration,
    } = prepare_window(
        writer.recipe.clone(),
        writer.source,
        physical_catalog_identity,
        remaining,
        reader,
        extractor,
        credits,
        limits,
    )
    .await?;
    if !cached_slots.is_empty() {
        tracing::debug!(
            counter.keldra_index_v1_look_ahead_reused_mutations = cached_slots.len(),
            "reused exact source preparation from staging overlap"
        );
        mutation_count += cached_slots.len();
        chunks.insert(next_chunk_ordinal, (cached_metadata, cached_slots));
        next_chunk_ordinal += 1;
    }
    let prepared_chunks = chunks;
    reserve_sealing_progress(
        writer,
        prepared_chunks
            .values()
            .flat_map(|(_, slots)| slots.iter())
            .filter_map(|slot| slot.prepared.as_ref().map(|prepared| &prepared.query)),
        prepared_chunks
            .values()
            .flat_map(|(metadata, slots)| metadata.iter().zip(slots))
            .filter_map(|((mutation, _, _), slot)| {
                slot.prepared
                    .as_ref()
                    .map(|prepared| (mutation.path.as_str(), prepared.current.as_slice()))
            }),
        credits,
        publisher.query_block_limits(),
    )?;
    writer.look_ahead_progress = None;
    let prepared_count: usize = prepared_chunks
        .values()
        .map(|(_, values)| values.len())
        .sum();
    let fold_bytes = prepared_count
        .checked_mul(std::mem::size_of::<(
            (Mutation, u64, Arc<IndexingMemoryPermit>),
            V1PreparationSlot,
        )>())
        .ok_or_else(|| Status::resource_exhausted("prepared source fold admission overflow"))?;
    let _fold_memory = credits
        .acquire(IndexingMemoryStage::PreparedRows, fold_bytes.max(1))
        .map_err(|_| Status::resource_exhausted("prepared source fold memory unavailable"))?;
    let mut prepared_values = prepared_chunks
        .into_values()
        .flat_map(|(metadata, slots)| metadata.into_iter().zip(slots))
        .collect::<Vec<_>>();
    order_prepared_values(&mut prepared_values);
    let mut rows = Vec::with_capacity(prepared_count);
    let merge_started = Instant::now();
    for ((mutation, source_bytes, _input), mut slot) in prepared_values {
        let prepared = slot
            .prepared
            .take()
            .ok_or_else(|| Status::internal("v1 CPU batch omitted prepared slot"))?;
        merge_query(&mut writer.query, prepared.query)?;
        writer.query_input_credits.push(slot.credits);
        writer.source_bytes = writer.source_bytes.saturating_add(source_bytes);
        rows.push(PreparedProjectionRow {
            source_offset: mutation.offset,
            mutation_ordinal: mutation.ordinal,
            source_path: mutation.path,
            source_version: mutation.version,
            projected_states: prepared.current,
        });
    }
    let result = apply_rows(writer, safe_next, rows, credits, limits);
    let prepare_duration = started.elapsed();
    tracing::debug!(
        histogram.keldra_index_v1_prepare_duration_seconds = prepare_duration.as_secs_f64(),
        histogram.keldra_index_v1_exact_source_read_duration_seconds = exact_duration.as_secs_f64(),
        histogram.keldra_index_v1_selection_duration_seconds = selection_duration.as_secs_f64(),
        histogram.keldra_index_v1_merge_duration_seconds = merge_started.elapsed().as_secs_f64(),
        counter.keldra_index_v1_prepare_mutations = mutation_count,
        counter.keldra_index_v1_prepare_chunks = next_chunk_ordinal,
        gauge.keldra_index_v1_prepare_parallelism = limits.parallelism,
        "v1 producer prepared one mutation window"
    );
    result
}

fn order_prepared_values(
    values: &mut [(
        (Mutation, u64, Arc<IndexingMemoryPermit>),
        V1PreparationSlot,
    )],
) {
    values.sort_unstable_by_key(|((mutation, _, _), _)| (mutation.offset, mutation.ordinal));
}

#[cfg(test)]
mod order_tests {
    use super::*;
    use keldra_index::v1::{
        DocumentHead, IndexingMemoryLimits, ObjectIdentity, PreparedQueryMembershipDelta,
        PreparedTypedJsonDocument, ProjectedDocumentState, QueryDocumentGate, RecipeIdentity,
    };
    #[test]
    fn cached_and_fresh_preparation_fold_globally_in_source_order_with_exact_newest_gate() {
        let bytes = 65536;
        let memory = IndexingMemoryCredits::new(
            bytes,
            IndexingMemoryLimits {
                hot_payload_bytes: bytes,
                worker_scratch_bytes: bytes,
                prepared_rows_bytes: bytes,
                replay_input_bytes: bytes,
                projection_accumulator_bytes: bytes,
                seal_scratch_bytes: bytes,
                ordering_catalog_bytes: bytes,
            },
        )
        .unwrap();
        let scope = [9; 32];
        let recipe = RecipeIdentity::new([7; 32]).unwrap();
        let make = |path: &str, offset: u64| {
            let head = DocumentHead::new(scope, path.into(), 0, offset, None, true).unwrap();
            let gate = QueryDocumentGate {
                document: head.stable_key,
                material_source_version: offset,
                current_source_version: offset,
                live: true,
                source_path: Some(path.into()),
                canonical_source_path: None,
                result_path: Some(path.into()),
                result_version: offset,
            };
            let state = ProjectedDocumentState::new(scope, head, Vec::new(), Vec::new()).unwrap();
            let mutation = Mutation {
                offset,
                ordinal: 0,
                tenant_id: 1,
                bucket_id: 2,
                path: path.into(),
                canonical_path: None,
                version: offset,
                deleted: false,
                atomic_group: None,
                predecessor_absent_at_window_start: false,
            };
            let input = Arc::new(memory.acquire(IndexingMemoryStage::ReplayInput, 1).unwrap());
            let slot = V1PreparationSlot {
                selected: SelectedV1Source {
                    source: IndexSourceMutation::Remove {
                        identity: ObjectIdentity {
                            path: path.into(),
                            version: offset,
                        },
                        canonical_path: None,
                    },
                    selected: None,
                    selection_memory: None,
                },
                credits: QueryBlockCredits::from_pipeline_permit(
                    memory
                        .acquire(IndexingMemoryStage::OrderingCatalog, 1)
                        .unwrap(),
                ),
                prepared: Some(PreparedTypedJsonDocument {
                    current: vec![state],
                    query: PreparedQueryMutationBatch {
                        membership: Some(PreparedQueryMembershipDelta {
                            recipe,
                            gates: vec![gate],
                        }),
                        fields: Vec::new(),
                    },
                }),
                source_memory: None,
                workspace_memory: None,
            };
            ((mutation, 0, input), slot)
        };
        // Fresh preparation straddles a cached source position and includes a
        // later replacement for the same object; chunk ordering cannot solve it.
        let mut values = vec![
            make("objects/b", 21),
            make("objects/a", 22),
            make("objects/a", 20),
        ];
        order_prepared_values(&mut values);
        assert_eq!(
            values
                .iter()
                .map(|((mutation, _, _), _)| mutation.offset)
                .collect::<Vec<_>>(),
            vec![20, 21, 22]
        );
        let mut query = PreparedQueryMutationBatch::default();
        let mut rows = Vec::new();
        for ((mutation, _, _), mut slot) in values {
            let prepared = slot.prepared.take().unwrap();
            merge_query(&mut query, prepared.query).unwrap();
            rows.push(PreparedProjectionRow {
                source_offset: mutation.offset,
                mutation_ordinal: mutation.ordinal,
                source_path: mutation.path,
                source_version: mutation.version,
                projected_states: prepared.current,
            });
        }
        let gates = &query.membership.unwrap().gates;
        assert_eq!(gates.len(), 2);
        assert_eq!(
            gates
                .iter()
                .find(|gate| gate.source_path.as_deref() == Some("objects/a"))
                .unwrap()
                .material_source_version,
            22
        );
        let partition = ProjectionPartitionIdentity::new([1; 32], 2, [3; 32], 2, 4, 5).unwrap();
        let mut accumulator =
            PartitionProjectionAccumulator::new(scope, partition, 20, 4096, memory.clone())
                .unwrap();
        let batch = PreparedProjectionBatchReservation::reserve(&memory, 4096)
            .unwrap()
            .finish(scope, 20, 23, rows)
            .unwrap();
        assert!(matches!(
            accumulator.apply_batch(batch).unwrap(),
            ProjectionBatchAdmission::Applied { .. }
        ));
        assert_eq!(accumulator.next_offset(), 23);
        let sealed = accumulator.seal_and_reset().unwrap();
        assert_eq!(sealed.into_parts().0.checkpoint.next_offset, 23);
    }
}

pub(super) fn acquire_preparation_construction(
    credits: &IndexingMemoryCredits,
    chunk_len: usize,
) -> Result<IndexingMemoryPermit, Status> {
    let chunk_descriptors = chunk_len
        .checked_mul(std::mem::size_of::<Mutation>())
        .ok_or_else(|| Status::resource_exhausted("v1 replay chunk size overflow"))?;
    let construction_bytes = chunk_descriptors.max(1);
    credits
        .acquire(IndexingMemoryStage::ReplayInput, construction_bytes)
        .map_err(|_| Status::resource_exhausted("v1 replay chunk memory unavailable"))
}

pub(super) fn preparation_chunk_size(pending: usize) -> usize {
    pending.min(PREPARATION_CHUNK_MUTATIONS)
}

pub(super) fn preparation_refill_size(
    pending: usize,
    active: usize,
    maximum_parallelism: usize,
) -> usize {
    let maximum_parallelism = maximum_parallelism.max(1);
    if pending == 0 {
        return 0;
    }
    pending
        .div_ceil(PREPARATION_CHUNK_MUTATIONS)
        .min(maximum_parallelism.saturating_sub(active))
}

async fn select_mutation(
    extractor: V1ProjectionExtractor,
    recipe: Arc<PhysicalCatalogRecipe>,
    physical_catalog_identity: [u8; 32],
    mutation: Mutation,
    mut source: IndexSourceMutation,
    credits: &IndexingMemoryCredits,
) -> Result<Option<SelectedMutation>, Status> {
    let source_bytes = match &source {
        IndexSourceMutation::Upsert(object) => object.content_length,
        IndexSourceMutation::Remove { .. } => 0,
    };
    let content_type = match &source {
        IndexSourceMutation::Upsert(object) => object.content_type.as_deref(),
        IndexSourceMutation::Remove { .. } => None,
    };
    let mut matched = matching_recipes(
        std::slice::from_ref(&recipe),
        mutation.tenant_id,
        mutation.bucket_id,
        &mutation.path,
        content_type,
    );
    if matched.is_empty() {
        // A delete or content-type transition can stop matching this recipe
        // before extraction. Retire any exact-or-older hot state so a cached
        // projection cannot outlive the journal mutation that made it obsolete.
        extractor.discard_hot_through(
            mutation.tenant_id,
            mutation.bucket_id,
            &mutation.path,
            mutation.version,
        );
        // The source remains in this family's journal even when a content-type
        // transition stops matching its projection. Publish a newer deletion
        // gate, otherwise old indexed material would remain live forever.
        source = IndexSourceMutation::Remove {
            identity: keldra_index::v1::ObjectIdentity {
                path: mutation.path.clone(),
                version: mutation.version,
            },
            canonical_path: mutation.canonical_path.clone(),
        };
        matched.push(recipe.clone());
    }
    let mut selected = extractor
        .select(
            mutation.tenant_id,
            mutation.bucket_id,
            source,
            &matched,
            physical_catalog_identity,
            credits,
        )
        .await?;
    let retained_bytes = selected_mutation_resident_bytes(&mutation, &selected)?;
    let input = credits
        .acquire(IndexingMemoryStage::ReplayInput, retained_bytes.max(1))
        .map_err(|_| Status::resource_exhausted("v1 replay input memory unavailable"))?;
    drop(selected.selection_memory.take());
    Ok(Some(SelectedMutation {
        mutation,
        selected,
        source_bytes,
        _input: input,
    }))
}

pub(super) fn selected_mutation_resident_bytes(
    mutation: &Mutation,
    selected: &SelectedV1Source,
) -> Result<usize, Status> {
    let mut bytes = selected_mutation_base_resident_bytes(mutation, &selected.source)?;
    if let Some(projection) = &selected.selected {
        bytes = bytes
            .checked_add(projection.resident_bytes().map_err(index_status)?)
            .ok_or_else(|| Status::resource_exhausted("v1 replay selection size overflow"))?;
    }
    Ok(bytes)
}

fn selected_mutation_base_resident_bytes(
    mutation: &Mutation,
    source: &IndexSourceMutation,
) -> Result<usize, Status> {
    let mut bytes = std::mem::size_of::<SelectedMutation>()
        .checked_add(mutation.path.capacity())
        .and_then(|bytes| {
            bytes.checked_add(
                mutation
                    .canonical_path
                    .as_ref()
                    .map_or(0, |path| path.capacity()),
            )
        })
        .ok_or_else(|| Status::resource_exhausted("v1 replay selection size overflow"))?;
    bytes = bytes
        .checked_add(
            match source {
                IndexSourceMutation::Upsert(object) => std::mem::size_of::<IndexBuildObject>()
                    .checked_add(object.path.capacity())
                    .and_then(|value| {
                        value.checked_add(
                            object
                                .canonical_path
                                .as_ref()
                                .map_or(0, |path| path.capacity()),
                        )
                    })
                    .and_then(|value| {
                        value.checked_add(
                            object
                                .content_type
                                .as_ref()
                                .map_or(0, |content_type| content_type.capacity()),
                        )
                    }),
                IndexSourceMutation::Remove {
                    identity,
                    canonical_path,
                } => std::mem::size_of_val(identity)
                    .checked_add(identity.path.capacity())
                    .and_then(|value| {
                        value.checked_add(canonical_path.as_ref().map_or(0, |path| path.capacity()))
                    }),
            }
            .ok_or_else(|| Status::resource_exhausted("v1 replay selection size overflow"))?,
        )
        .ok_or_else(|| Status::resource_exhausted("v1 replay selection size overflow"))?;
    Ok(bytes)
}

pub(super) fn apply_rows(
    writer: &mut Writer,
    next: u64,
    mut rows: Vec<PreparedProjectionRow>,
    credits: &IndexingMemoryCredits,
    _limits: Limits,
) -> Result<(), Status> {
    let first = writer.accumulator.next_offset();
    if next <= first {
        return Ok(());
    }
    rows.sort_by_key(|row| (row.source_offset, row.mutation_ordinal));
    let admitted_rows = rows.iter().try_fold(
        std::mem::size_of::<keldra_index::v1::PreparedProjectionBatch>()
            + rows.capacity() * std::mem::size_of::<PreparedProjectionRow>(),
        |bytes, row| {
            bytes
                .checked_add(row.resident_bytes().map_err(index_status)?)
                .ok_or_else(|| Status::resource_exhausted("v1 row admission overflow"))
        },
    )?;
    let reservation = PreparedProjectionBatchReservation::reserve(credits, admitted_rows.max(1))
        .map_err(|_| Status::resource_exhausted("v1 prepared-row memory unavailable"))?;
    let batch = reservation
        .finish(source_scope(writer.source), first, next, rows)
        .map_err(index_status)?;
    let prepared_bytes = u64::try_from(batch.resident_bytes())
        .map_err(|_| Status::resource_exhausted("v1 prepared-row bytes exceed telemetry"))?;
    match writer
        .accumulator
        .apply_batch(batch)
        .map_err(index_status)?
    {
        ProjectionBatchAdmission::Applied {
            source_rows,
            coalesced_rows,
            ..
        } => {
            let source_rows = u64::try_from(source_rows)
                .map_err(|_| Status::resource_exhausted("v1 prepared rows exceed telemetry"))?;
            writer.pending_prepared_rows = writer.pending_prepared_rows.saturating_add(source_rows);
            writer.pending_prepared_bytes = writer.pending_prepared_bytes.saturating_add(
                super::super::v1_telemetry::V1PipelineTelemetry::indexed_prepared_bytes(
                    source_rows,
                    prepared_bytes,
                ),
            );
            writer.pending_projected_rows = writer.pending_projected_rows.saturating_add(
                u64::try_from(coalesced_rows).map_err(|_| {
                    Status::resource_exhausted("v1 projected rows exceed telemetry")
                })?,
            );
            // Cursor-only batches are expected for reserved projection and
            // catalog objects. They advance the in-memory contiguous cut, but
            // must not arm publication: an empty Current creates another
            // reserved source event and otherwise feeds itself forever. The
            // preceding Current remains the conservative durable retention
            // proof. A later matching mutation publishes one range spanning
            // every skipped control event; restart safely replays the retained
            // gap from that preceding Current.
            if source_rows != 0 {
                writer.since.get_or_insert_with(Instant::now);
            }
            Ok(())
        }
        ProjectionBatchAdmission::ReplayRequired { .. } => {
            Err(Status::resource_exhausted("v1 accumulator requires replay"))
        }
    }
}
