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

    let mut state =
        topology_mutation::read_topology_mutation_state(storage, authority.as_ref()).await?;
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
    _storage: &Storage,
    _input: CreateRegionDescriptor,
    _target: Option<LifecycleState>,
    _transaction_id: &str,
    _principal: &str,
) -> LifecycleResult<RegionDescriptor> {
    Err(topology_transaction_error("region"))
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
    let mut state =
        topology_mutation::read_topology_mutation_state(storage, authority.as_ref()).await?;
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
    ensure_region_drain_completion_is_supported(
        storage,
        authority.as_ref().map(|authority| authority.mvcc),
        region,
        target,
    )
    .await?;
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

    let mut state =
        topology_mutation::read_topology_mutation_state(storage, authority.as_ref()).await?;
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
    let checkpoint_store = crate::mesh_control_stream::MeshCheckpointStore::new(storage);
    validate_activation_checkpoint_streams(storage, &checkpoint_store, checkpoint).await?;
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

pub fn ensure_region_accepts_new_writes_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    region: &str,
) -> LifecycleResult<()> {
    require_identifier(region, "region")?;
    let state = read_lifecycle_state_projection_mvcc(mvcc)?;
    if !state.regions.contains_key(region) {
        return Err(LifecycleError::NotFound {
            resource_kind: "region",
            resource_id: region.to_string(),
        });
    }
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

pub fn ensure_new_writable_placement_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    region: &str,
    cell_id: &str,
    node_id: &str,
) -> LifecycleResult<()> {
    require_identifier(region, "region")?;
    require_identifier(cell_id, "cell id")?;
    require_identifier(node_id, "node id")?;

    let state = read_lifecycle_state_projection_mvcc(mvcc)?;
    if !state.regions.contains_key(region) {
        return Err(LifecycleError::NotFound {
            resource_kind: "region",
            resource_id: region.to_string(),
        });
    }
    let expected_cell_key = cell_key(region, cell_id)?;
    if !state.cells.contains_key(&expected_cell_key) {
        return Err(LifecycleError::NotFound {
            resource_kind: "cell",
            resource_id: format!("{region}/{cell_id}"),
        });
    }
    if !state.nodes.contains_key(node_id) {
        return Err(LifecycleError::NotFound {
            resource_kind: "node",
            resource_id: node_id.to_string(),
        });
    }

    ensure_region_accepts_new_writes_in_state(&state, region)?;
    ensure_cell_accepts_new_writes_in_state(&state, region, cell_id)?;
    ensure_node_accepts_new_writes_in_state(&state, region, cell_id, node_id)?;
    Ok(())
}
