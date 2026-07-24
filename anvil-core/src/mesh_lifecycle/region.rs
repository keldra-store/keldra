use super::*;

#[cfg(test)]
pub async fn create_region(
    storage: &Storage,
    input: CreateRegionDescriptor,
) -> LifecycleResult<RegionDescriptor> {
    create_region_inner(storage, input, None).await
}

pub async fn create_region_with_control(
    storage: &Storage,
    input: CreateRegionDescriptor,
    authority: LifecycleControlWriteAuthority<'_>,
) -> LifecycleResult<RegionDescriptor> {
    create_region_inner(storage, input, Some(authority)).await
}

async fn create_region_inner(
    storage: &Storage,
    input: CreateRegionDescriptor,
    authority: Option<LifecycleControlWriteAuthority<'_>>,
) -> LifecycleResult<RegionDescriptor> {
    require_identifier(&input.mesh_id, "mesh id")?;
    require_identifier(&input.region, "region")?;
    require_nonempty(&input.virtual_host_suffix, "virtual host suffix")?;
    if let Some(default_cell) = &input.default_cell {
        require_identifier(default_cell, "default cell")?;
    }

    let mut state = read_state(storage).await?;
    if state.regions.contains_key(&input.region) {
        return Err(LifecycleError::AlreadyExists {
            resource_kind: "region",
            resource_id: input.region,
        });
    }

    let now = timestamp_now();
    let descriptor = RegionDescriptor {
        schema: REGION_DESCRIPTOR_SCHEMA.to_string(),
        mesh_id: input.mesh_id,
        region: input.region.clone(),
        state: LifecycleState::Joining,
        public_base_url: input.public_base_url,
        virtual_host_suffix: input.virtual_host_suffix,
        placement_weight: input.placement_weight,
        default_cell: input.default_cell,
        created_at: now.clone(),
        updated_at: now,
        generation: 1,
    };
    state
        .regions
        .insert(descriptor.region.clone(), descriptor.clone());
    let control = authority
        .map(|authority| {
            topology_mutation::fenced_control_mutation(
                REGION_DESCRIPTOR_STREAM_FAMILY,
                descriptor.region.clone(),
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
        record_proto::encode_region_projection_row(&descriptor)?,
        control,
    )
    .await?;
    Ok(descriptor)
}

pub async fn put_region_in_transaction(
    storage: &Storage,
    input: CreateRegionDescriptor,
    target: Option<LifecycleState>,
    transaction_id: &str,
    principal: &str,
) -> LifecycleResult<RegionDescriptor> {
    require_identifier(&input.mesh_id, "mesh id")?;
    require_identifier(&input.region, "region")?;
    require_nonempty(&input.virtual_host_suffix, "virtual host suffix")?;
    if let Some(default_cell) = &input.default_cell {
        require_identifier(default_cell, "default cell")?;
    }

    let mut state = read_state_for_transaction(storage, transaction_id, principal).await?;
    let transaction_timestamp =
        lifecycle_transaction_timestamp(storage, transaction_id, principal).await?;
    let existing_generation = state
        .regions
        .get(&input.region)
        .map(|descriptor| descriptor.generation);
    let existed = existing_generation.is_some();
    let mut descriptor = if let Some(existing) = state.regions.get(&input.region).cloned() {
        if !input.public_base_url.is_empty() && existing.public_base_url != input.public_base_url {
            return Err(LifecycleError::InvalidArgument(format!(
                "region {} already exists with endpoint {}",
                existing.region, existing.public_base_url
            )));
        }
        existing
    } else {
        require_nonempty(&input.public_base_url, "public base url")?;
        let now = transaction_timestamp.clone();
        RegionDescriptor {
            schema: REGION_DESCRIPTOR_SCHEMA.to_string(),
            mesh_id: input.mesh_id,
            region: input.region.clone(),
            state: LifecycleState::Joining,
            public_base_url: input.public_base_url,
            virtual_host_suffix: input.virtual_host_suffix,
            placement_weight: input.placement_weight,
            default_cell: input.default_cell,
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
                resource_kind: "region",
                resource_id: descriptor.region.clone(),
                from: descriptor.state,
                to: target,
            }
        })?;
        ensure_region_drain_completion_is_supported(storage, &descriptor.region, target).await?;
        descriptor.state = target;
        descriptor.updated_at = transaction_timestamp;
        descriptor.generation = descriptor.generation.saturating_add(1);
    }

    if existed && descriptor == state.regions[&input.region] {
        return Ok(descriptor);
    }

    state
        .regions
        .insert(descriptor.region.clone(), descriptor.clone());
    let operation = if existed { "upsert" } else { "create" };
    let expected_generation = existing_generation;
    let control = topology_mutation::transactional_control_mutation(
        REGION_DESCRIPTOR_STREAM_FAMILY,
        descriptor.region.clone(),
        operation,
        expected_generation,
        descriptor.generation,
        &descriptor.mesh_id,
        &descriptor,
        principal,
    )?;
    topology_mutation::stage_topology_mutation_in_transaction(
        storage,
        record_proto::encode_region_projection_row(&descriptor)?,
        control,
        transaction_id,
        principal,
    )
    .await?;
    Ok(descriptor)
}

#[cfg(test)]
pub async fn transition_region(
    storage: &Storage,
    region: &str,
    expected_generation: u64,
    target: LifecycleState,
) -> LifecycleResult<RegionDescriptor> {
    transition_region_inner(storage, region, expected_generation, target, None).await
}

pub async fn transition_region_with_control(
    storage: &Storage,
    region: &str,
    expected_generation: u64,
    target: LifecycleState,
    authority: LifecycleControlWriteAuthority<'_>,
) -> LifecycleResult<RegionDescriptor> {
    transition_region_inner(
        storage,
        region,
        expected_generation,
        target,
        Some(authority),
    )
    .await
}

async fn transition_region_inner(
    storage: &Storage,
    region: &str,
    expected_generation: u64,
    target: LifecycleState,
    authority: Option<LifecycleControlWriteAuthority<'_>>,
) -> LifecycleResult<RegionDescriptor> {
    require_identifier(region, "region")?;
    let mut state = read_state(storage).await?;
    {
        let descriptor = state
            .regions
            .get(region)
            .ok_or_else(|| LifecycleError::NotFound {
                resource_kind: "region",
                resource_id: region.to_string(),
            })?;
        ensure_generation("region", region, descriptor.generation, expected_generation)?;
        validate_region_transition(descriptor.state, target).map_err(|_| {
            LifecycleError::LifecycleTransitionDenied {
                resource_kind: "region",
                resource_id: region.to_string(),
                from: descriptor.state,
                to: target,
            }
        })?;
    }
    ensure_region_drain_completion_is_supported(storage, region, target).await?;
    let descriptor = state
        .regions
        .get_mut(region)
        .ok_or_else(|| LifecycleError::NotFound {
            resource_kind: "region",
            resource_id: region.to_string(),
        })?;
    descriptor.state = target;
    descriptor.updated_at = timestamp_now();
    descriptor.generation = descriptor.generation.saturating_add(1);
    let out = descriptor.clone();
    let control = authority
        .map(|authority| {
            topology_mutation::fenced_control_mutation(
                REGION_DESCRIPTOR_STREAM_FAMILY,
                out.region.clone(),
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
        record_proto::encode_region_projection_row(&out)?,
        control,
    )
    .await?;
    Ok(out)
}

pub fn parse_activation_checkpoint_json(input: &str) -> LifecycleResult<ActivationCheckpoint> {
    require_nonempty(input, "activation checkpoint")?;
    serde_json::from_str(input).map_err(|err| {
        LifecycleError::InvalidArgument(format!("activation checkpoint JSON is invalid: {err}"))
    })
}

pub async fn activate_region(
    storage: &Storage,
    region: &str,
    expected_generation: u64,
    checkpoint: &ActivationCheckpoint,
) -> LifecycleResult<RegionDescriptor> {
    activate_region_inner(storage, region, expected_generation, checkpoint, None).await
}

pub async fn activate_region_with_control(
    storage: &Storage,
    region: &str,
    expected_generation: u64,
    checkpoint: &ActivationCheckpoint,
    authority: LifecycleControlWriteAuthority<'_>,
) -> LifecycleResult<RegionDescriptor> {
    activate_region_inner(
        storage,
        region,
        expected_generation,
        checkpoint,
        Some(authority),
    )
    .await
}

async fn activate_region_inner(
    storage: &Storage,
    region: &str,
    expected_generation: u64,
    checkpoint: &ActivationCheckpoint,
    authority: Option<LifecycleControlWriteAuthority<'_>>,
) -> LifecycleResult<RegionDescriptor> {
    require_identifier(region, "region")?;

    let mut state = read_state(storage).await?;
    let current = state
        .regions
        .get(region)
        .ok_or_else(|| LifecycleError::NotFound {
            resource_kind: "region",
            resource_id: region.to_string(),
        })?;
    ensure_generation("region", region, current.generation, expected_generation)?;
    validate_region_transition(current.state, LifecycleState::Active).map_err(|_| {
        LifecycleError::LifecycleTransitionDenied {
            resource_kind: "region",
            resource_id: region.to_string(),
            from: current.state,
            to: LifecycleState::Active,
        }
    })?;
    validate_activation_checkpoint_header(checkpoint, &current.mesh_id, region)?;
    validate_activation_checkpoint_streams(storage, checkpoint).await?;
    ensure_region_activation_dependencies(&state, region)?;

    let descriptor = state
        .regions
        .get_mut(region)
        .ok_or_else(|| LifecycleError::NotFound {
            resource_kind: "region",
            resource_id: region.to_string(),
        })?;
    descriptor.state = LifecycleState::Active;
    descriptor.updated_at = timestamp_now();
    descriptor.generation = descriptor.generation.saturating_add(1);
    let out = descriptor.clone();
    let control = authority
        .map(|authority| {
            topology_mutation::fenced_control_mutation(
                REGION_DESCRIPTOR_STREAM_FAMILY,
                out.region.clone(),
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
        record_proto::encode_region_projection_row(&out)?,
        control,
    )
    .await?;
    Ok(out)
}

pub async fn list_regions(storage: &Storage) -> LifecycleResult<Vec<RegionDescriptor>> {
    Ok(read_state(storage).await?.regions.into_values().collect())
}

pub async fn ensure_region_accepts_new_writes(
    storage: &Storage,
    region: &str,
) -> LifecycleResult<()> {
    require_identifier(region, "region")?;
    let state = read_state(storage).await?;
    ensure_region_accepts_new_writes_in_state(&state, region)
}

pub async fn ensure_new_writable_placement(
    storage: &Storage,
    region: &str,
    cell_id: &str,
    node_id: &str,
) -> LifecycleResult<()> {
    require_identifier(region, "region")?;
    require_identifier(cell_id, "cell id")?;
    require_identifier(node_id, "node id")?;

    let state = read_state(storage).await?;
    ensure_region_accepts_new_writes_in_state(&state, region)?;
    ensure_cell_accepts_new_writes_in_state(&state, region, cell_id)?;
    ensure_node_accepts_new_writes_in_state(&state, region, cell_id, node_id)?;
    Ok(())
}
