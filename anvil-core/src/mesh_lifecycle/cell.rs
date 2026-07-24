use super::*;

#[cfg(test)]
pub async fn register_cell(
    storage: &Storage,
    input: RegisterCellDescriptor,
) -> LifecycleResult<CellDescriptor> {
    register_cell_inner(storage, input, None).await
}

pub async fn register_cell_with_control(
    storage: &Storage,
    input: RegisterCellDescriptor,
    authority: LifecycleControlWriteAuthority<'_>,
) -> LifecycleResult<CellDescriptor> {
    register_cell_inner(storage, input, Some(authority)).await
}

async fn register_cell_inner(
    storage: &Storage,
    input: RegisterCellDescriptor,
    authority: Option<LifecycleControlWriteAuthority<'_>>,
) -> LifecycleResult<CellDescriptor> {
    require_identifier(&input.mesh_id, "mesh id")?;
    require_identifier(&input.region, "region")?;
    require_identifier(&input.cell_id, "cell id")?;
    require_identifier(&input.failure_domain, "cell failure domain")?;

    let mut state = read_state(storage).await?;
    if !state.regions.contains_key(&input.region) {
        return Err(LifecycleError::NotFound {
            resource_kind: "region",
            resource_id: input.region.clone(),
        });
    }
    let key = cell_key(&input.region, &input.cell_id)?;
    if state.cells.contains_key(&key) {
        return Err(LifecycleError::AlreadyExists {
            resource_kind: "cell",
            resource_id: input.cell_id,
        });
    }

    let now = timestamp_now();
    let descriptor = CellDescriptor {
        schema: CELL_DESCRIPTOR_SCHEMA.to_string(),
        mesh_id: input.mesh_id,
        region: input.region,
        cell_id: input.cell_id,
        state: LifecycleState::Joining,
        placement_weight: input.placement_weight,
        failure_domain: input.failure_domain,
        created_at: now.clone(),
        updated_at: now,
        generation: 1,
    };
    state.cells.insert(key, descriptor.clone());
    let record_key = cell_record_key(&descriptor.region, &descriptor.cell_id)?;
    let control = authority
        .map(|authority| {
            topology_mutation::fenced_control_mutation(
                CELL_DESCRIPTOR_STREAM_FAMILY,
                record_key,
                "create",
                None,
                descriptor.generation,
                &descriptor.mesh_id,
                &descriptor,
                authority,
            )
        })
        .transpose()?;
    topology_mutation::commit_topology_mutation(
        storage,
        record_proto::encode_cell_projection_row(&descriptor)?,
        control,
    )
    .await?;
    Ok(descriptor)
}

pub async fn put_cell_in_transaction(
    storage: &Storage,
    input: RegisterCellDescriptor,
    target: Option<LifecycleState>,
    transaction_id: &str,
    principal: &str,
) -> LifecycleResult<CellDescriptor> {
    require_identifier(&input.mesh_id, "mesh id")?;
    require_identifier(&input.region, "region")?;
    require_identifier(&input.cell_id, "cell id")?;
    require_identifier(&input.failure_domain, "cell failure domain")?;

    let mut state = read_state_for_transaction(storage, transaction_id, principal).await?;
    let transaction_timestamp =
        lifecycle_transaction_timestamp(storage, transaction_id, principal).await?;
    if !state.regions.contains_key(&input.region) {
        return Err(LifecycleError::NotFound {
            resource_kind: "region",
            resource_id: input.region.clone(),
        });
    }
    let key = cell_key(&input.region, &input.cell_id)?;
    let existing_generation = state
        .cells
        .get(&key)
        .map(|descriptor| descriptor.generation);
    let mut descriptor = if let Some(existing) = state.cells.get(&key).cloned() {
        if existing.failure_domain != input.failure_domain {
            return Err(LifecycleError::InvalidArgument(format!(
                "cell {}/{} already exists with failure domain {}",
                existing.region, existing.cell_id, existing.failure_domain
            )));
        }
        existing
    } else {
        let now = transaction_timestamp.clone();
        CellDescriptor {
            schema: CELL_DESCRIPTOR_SCHEMA.to_string(),
            mesh_id: input.mesh_id,
            region: input.region,
            cell_id: input.cell_id,
            state: LifecycleState::Joining,
            placement_weight: input.placement_weight,
            failure_domain: input.failure_domain,
            created_at: now.clone(),
            updated_at: now,
            generation: 1,
        }
    };

    if let Some(target) = target
        && descriptor.state != target
    {
        validate_region_transition(descriptor.state, target).map_err(|_| {
            LifecycleError::LifecycleTransitionDenied {
                resource_kind: "cell",
                resource_id: descriptor.cell_id.clone(),
                from: descriptor.state,
                to: target,
            }
        })?;
        descriptor.state = target;
        descriptor.updated_at = transaction_timestamp;
        descriptor.generation = descriptor.generation.saturating_add(1);
    }

    if existing_generation.is_some() && descriptor == state.cells[&key] {
        return Ok(descriptor);
    }

    let key = cell_key(&descriptor.region, &descriptor.cell_id)?;
    state.cells.insert(key, descriptor.clone());
    let record_key = cell_record_key(&descriptor.region, &descriptor.cell_id)?;
    let control = topology_mutation::transactional_control_mutation(
        CELL_DESCRIPTOR_STREAM_FAMILY,
        record_key,
        if existing_generation.is_some() {
            "upsert"
        } else {
            "create"
        },
        existing_generation,
        descriptor.generation,
        &descriptor.mesh_id,
        &descriptor,
        principal,
    )?;
    topology_mutation::stage_topology_mutation_in_transaction(
        storage,
        record_proto::encode_cell_projection_row(&descriptor)?,
        control,
        transaction_id,
        principal,
    )
    .await?;
    Ok(descriptor)
}

#[cfg(test)]
pub async fn transition_cell(
    storage: &Storage,
    region: &str,
    cell_id: &str,
    expected_generation: u64,
    target: LifecycleState,
) -> LifecycleResult<CellDescriptor> {
    transition_cell_inner(storage, region, cell_id, expected_generation, target, None).await
}

pub async fn transition_cell_with_control(
    storage: &Storage,
    region: &str,
    cell_id: &str,
    expected_generation: u64,
    target: LifecycleState,
    authority: LifecycleControlWriteAuthority<'_>,
) -> LifecycleResult<CellDescriptor> {
    transition_cell_inner(
        storage,
        region,
        cell_id,
        expected_generation,
        target,
        Some(authority),
    )
    .await
}

async fn transition_cell_inner(
    storage: &Storage,
    region: &str,
    cell_id: &str,
    expected_generation: u64,
    target: LifecycleState,
    authority: Option<LifecycleControlWriteAuthority<'_>>,
) -> LifecycleResult<CellDescriptor> {
    let key = cell_key(region, cell_id)?;
    let mut state = read_state(storage).await?;
    let descriptor = state
        .cells
        .get_mut(&key)
        .ok_or_else(|| LifecycleError::NotFound {
            resource_kind: "cell",
            resource_id: cell_id.to_string(),
        })?;
    ensure_generation("cell", cell_id, descriptor.generation, expected_generation)?;
    validate_region_transition(descriptor.state, target).map_err(|_| {
        LifecycleError::LifecycleTransitionDenied {
            resource_kind: "cell",
            resource_id: cell_id.to_string(),
            from: descriptor.state,
            to: target,
        }
    })?;
    descriptor.state = target;
    descriptor.updated_at = timestamp_now();
    descriptor.generation = descriptor.generation.saturating_add(1);
    let out = descriptor.clone();
    let record_key = cell_record_key(&out.region, &out.cell_id)?;
    let control = authority
        .map(|authority| {
            topology_mutation::fenced_control_mutation(
                CELL_DESCRIPTOR_STREAM_FAMILY,
                record_key,
                "upsert",
                Some(expected_generation),
                out.generation,
                &out.mesh_id,
                &out,
                authority,
            )
        })
        .transpose()?;
    topology_mutation::commit_topology_mutation(
        storage,
        record_proto::encode_cell_projection_row(&out)?,
        control,
    )
    .await?;
    Ok(out)
}

pub async fn list_cells(
    storage: &Storage,
    region_filter: Option<&str>,
) -> LifecycleResult<Vec<CellDescriptor>> {
    if let Some(region) = region_filter.filter(|region| !region.is_empty()) {
        require_identifier(region, "region")?;
    }
    let cells = read_state(storage)
        .await?
        .cells
        .into_values()
        .filter(|cell| {
            region_filter.is_none_or(|region| region.is_empty() || cell.region == region)
        })
        .collect();
    Ok(cells)
}
