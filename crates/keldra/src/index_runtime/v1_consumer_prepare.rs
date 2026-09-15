//! Bounded preparation of one format-v1 producer mutation window.

use super::*;

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
    let telemetry = super::super::v1_telemetry::global();
    let prepare_timer =
        super::super::v1_telemetry::V1PipelineTelemetry::start_phase(&telemetry.prepare_nanos);
    let mutation_count = mutations.len();
    let current = writer.current.clone();
    let recipe = writer.recipe.clone();
    let scope = source_scope(writer.source);
    let input_bytes = limits.worker_bytes;
    let mut prepared_values = Vec::new();
    let exact_requests = mutations
        .iter()
        .map(|mutation| ExactMutationRequest {
            path: &mutation.path,
            canonical_path: mutation.canonical_path.as_deref(),
            version: mutation.version,
            deleted: mutation.deleted,
        })
        .collect::<Vec<_>>();
    let exact_timer = super::super::v1_telemetry::V1PipelineTelemetry::start_phase(
        &telemetry.exact_source_read_nanos,
    );
    let sources =
        load_exact_mutations(reader, &recipe, &exact_requests, limits.parallelism).await?;
    let exact_duration = exact_timer.elapsed();
    drop(exact_timer);
    let mut selected_inputs =
        VecDeque::from(mutations.into_iter().zip(sources).collect::<Vec<_>>());
    let mut jobs = tokio::task::JoinSet::new();
    let mut first_failure = None;
    let mut predecessor_duration = Duration::ZERO;
    let mut predecessor_batches = 0_u64;
    let selection_started = Instant::now();
    while !selected_inputs.is_empty() || !jobs.is_empty() {
        let refill = preparation_refill_size(selected_inputs.len(), jobs.len(), limits.parallelism);
        if refill > 0 {
            let chunk = (0..refill)
                .filter_map(|_| selected_inputs.pop_front())
                .collect::<Vec<_>>();
            let inputs = (0..chunk.len())
                .map(|_| {
                    credits
                        .acquire(IndexingMemoryStage::ReplayInput, input_bytes)
                        .map_err(|_| {
                            Status::resource_exhausted("v1 replay input memory unavailable")
                        })
                })
                .collect::<Result<Vec<_>, Status>>()?;
            let predecessor_timer = super::super::v1_telemetry::V1PipelineTelemetry::start_phase(
                &telemetry.predecessor_read_nanos,
            );
            let previous = match &current {
                Some(current) => {
                    let matched_indices = chunk
                        .iter()
                        .enumerate()
                        .filter_map(|(index, (mutation, source))| {
                            let content_type = match source {
                                IndexSourceMutation::Upsert(object) => {
                                    object.content_type.as_deref()
                                }
                                IndexSourceMutation::Remove { .. } => None,
                            };
                            (!mutation.predecessor_absent_at_window_start
                                && !matching_recipes(
                                    std::slice::from_ref(&recipe),
                                    mutation.tenant_id,
                                    mutation.bucket_id,
                                    &mutation.path,
                                    content_type,
                                )
                                .is_empty())
                            .then_some(index)
                        })
                        .collect::<Vec<_>>();
                    let source_paths = matched_indices
                        .iter()
                        .map(|index| chunk[*index].0.path.as_str())
                        .collect::<Vec<_>>();
                    let matched_previous = publisher
                        .load_source_states_batch(
                            &recipe.storage_tenant,
                            &recipe.bucket,
                            recipe.family.tenant_id,
                            recipe.family.bucket_id,
                            current,
                            scope,
                            &source_paths,
                            limits.parallelism,
                        )
                        .await?;
                    let mut previous = std::iter::repeat_with(Vec::new)
                        .take(chunk.len())
                        .collect::<Vec<_>>();
                    for (index, states) in matched_indices.into_iter().zip(matched_previous) {
                        previous[index] = states;
                    }
                    previous
                }
                None => std::iter::repeat_with(Vec::new).take(chunk.len()).collect(),
            };
            predecessor_duration = predecessor_duration.saturating_add(predecessor_timer.elapsed());
            drop(predecessor_timer);
            predecessor_batches = predecessor_batches.saturating_add(1);
            for (((mutation, source), previous), input) in
                chunk.into_iter().zip(previous).zip(inputs)
            {
                let extractor = extractor.clone();
                let prepare_extractor = extractor.clone();
                let recipe = recipe.clone();
                let credits = credits.clone();
                jobs.spawn(async move {
                    let Some(value) = select_mutation(
                        extractor,
                        recipe.clone(),
                        physical_catalog_identity,
                        mutation,
                        source,
                        previous,
                        input,
                    )
                    .await?
                    else {
                        return Ok(None);
                    };
                    let SelectedMutation {
                        mutation,
                        selected,
                        previous,
                        source_bytes,
                        _input,
                    } = value;
                    let query_credits = empty_query_credits(&credits, limits)?;
                    let (selected, previous, prepared, query_credits) = prepare_extractor
                        .prepare_owned(scope, selected, recipe, previous, query_credits)
                        .await?;
                    Ok::<_, Status>(Some((
                        SelectedMutation {
                            mutation,
                            selected,
                            previous,
                            source_bytes,
                            _input,
                        },
                        prepared,
                        query_credits,
                    )))
                });
            }
        }
        if let Some(joined) = jobs.join_next().await {
            match joined {
                Ok(Ok(Some(value))) => prepared_values.push(value),
                Ok(Ok(None)) => {}
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
    prepared_values.sort_by_key(|(value, _, _)| (value.mutation.offset, value.mutation.ordinal));
    let mut rows = Vec::with_capacity(prepared_values.len());
    let mut previous = BTreeMap::new();
    let merge_started = Instant::now();
    for (value, prepared, query_credits) in prepared_values {
        let mutation = value.mutation;
        merge_query(&mut writer.query, prepared.query)?;
        writer.query_input_credits.push(query_credits);
        previous.insert(mutation.path.clone(), value.previous);
        writer.source_bytes = writer.source_bytes.saturating_add(value.source_bytes);
        rows.push(PreparedProjectionRow {
            source_offset: mutation.offset,
            mutation_ordinal: mutation.ordinal,
            source_path: mutation.path,
            source_version: mutation.version,
            projected_states: prepared.current,
        });
    }
    let result = apply_rows(writer, safe_next, rows, previous, credits, limits);
    let prepare_duration = prepare_timer.elapsed();
    drop(prepare_timer);
    tracing::debug!(
        histogram.keldra_index_v1_prepare_duration_seconds = prepare_duration.as_secs_f64(),
        histogram.keldra_index_v1_exact_source_read_duration_seconds = exact_duration.as_secs_f64(),
        histogram.keldra_index_v1_predecessor_read_duration_seconds =
            predecessor_duration.as_secs_f64(),
        histogram.keldra_index_v1_selection_duration_seconds = selection_duration.as_secs_f64(),
        histogram.keldra_index_v1_merge_duration_seconds = merge_started.elapsed().as_secs_f64(),
        counter.keldra_index_v1_prepare_mutations = mutation_count,
        counter.keldra_index_v1_predecessor_batches = predecessor_batches,
        gauge.keldra_index_v1_prepare_parallelism = limits.parallelism,
        "v1 producer prepared one mutation window"
    );
    result
}

pub(super) fn preparation_refill_size(
    pending: usize,
    active: usize,
    maximum_parallelism: usize,
) -> usize {
    let maximum_parallelism = maximum_parallelism.max(1);
    if pending == 0 || active > maximum_parallelism.saturating_div(2) {
        return 0;
    }
    pending.min(maximum_parallelism.saturating_sub(active))
}

async fn select_mutation(
    extractor: V1ProjectionExtractor,
    recipe: PhysicalCatalogRecipe,
    physical_catalog_identity: [u8; 32],
    mutation: Mutation,
    source: IndexSourceMutation,
    previous: Vec<keldra_index::v1::ProjectedDocumentState>,
    mut input: keldra_index::v1::IndexingMemoryPermit,
) -> Result<Option<SelectedMutation>, Status> {
    let source_bytes = match &source {
        IndexSourceMutation::Upsert(object) => object.content_length,
        IndexSourceMutation::Remove { .. } => 0,
    };
    let content_type = match &source {
        IndexSourceMutation::Upsert(object) => object.content_type.as_deref(),
        IndexSourceMutation::Remove { .. } => None,
    };
    let matched = matching_recipes(
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
        return Ok(None);
    }
    let selected = extractor
        .select(
            mutation.tenant_id,
            mutation.bucket_id,
            source,
            &matched,
            physical_catalog_identity,
        )
        .await?;
    let retained_bytes = selected_mutation_resident_bytes(&mutation, &selected, &previous)?;
    if retained_bytes > input.bytes() {
        return Err(Status::resource_exhausted(format!(
            "v1 replay selection requires {retained_bytes} bytes but its construction bound is {}",
            input.bytes()
        )));
    }
    input
        .shrink_to(retained_bytes.max(1))
        .map_err(index_status)?;
    Ok(Some(SelectedMutation {
        mutation,
        selected,
        previous,
        source_bytes,
        _input: input,
    }))
}

pub(super) fn selected_mutation_resident_bytes(
    mutation: &Mutation,
    selected: &SelectedV1Source,
    previous: &[keldra_index::v1::ProjectedDocumentState],
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
            match &selected.source {
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
    if let Some(projection) = &selected.selected {
        bytes = bytes
            .checked_add(projection.resident_bytes().map_err(index_status)?)
            .ok_or_else(|| Status::resource_exhausted("v1 replay selection size overflow"))?;
    }
    bytes = bytes
        .checked_add(
            previous
                .len()
                .checked_mul(std::mem::size_of::<keldra_index::v1::ProjectedDocumentState>())
                .ok_or_else(|| Status::resource_exhausted("v1 replay selection size overflow"))?,
        )
        .ok_or_else(|| Status::resource_exhausted("v1 replay selection size overflow"))?;
    for state in previous {
        bytes = bytes
            .checked_add(state.resident_bytes().map_err(index_status)?)
            .ok_or_else(|| Status::resource_exhausted("v1 replay selection size overflow"))?;
    }
    Ok(bytes)
}

pub(super) fn apply_rows(
    writer: &mut Writer,
    next: u64,
    mut rows: Vec<PreparedProjectionRow>,
    previous: BTreeMap<String, Vec<keldra_index::v1::ProjectedDocumentState>>,
    credits: &IndexingMemoryCredits,
    limits: Limits,
) -> Result<(), Status> {
    let first = writer.accumulator.next_offset();
    if next <= first {
        return Ok(());
    }
    rows.sort_by_key(|row| (row.source_offset, row.mutation_ordinal));
    let reservation =
        PreparedProjectionBatchReservation::reserve(credits, limits.projection_batch_bytes)
            .map_err(|_| Status::resource_exhausted("v1 prepared-row memory unavailable"))?;
    let batch = reservation
        .finish(source_scope(writer.source), first, next, rows)
        .map_err(index_status)?;
    let prepared_bytes = u64::try_from(batch.resident_bytes())
        .map_err(|_| Status::resource_exhausted("v1 prepared-row bytes exceed telemetry"))?;
    match writer
        .accumulator
        .apply_batch(batch, previous)
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
