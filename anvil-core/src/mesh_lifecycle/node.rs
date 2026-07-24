use super::*;

#[cfg(test)]
pub async fn register_node(
    storage: &Storage,
    input: RegisterNodeDescriptor,
) -> LifecycleResult<NodeDescriptor> {
    register_node_inner(storage, input, None).await
}

pub async fn register_node_with_control(
    storage: &Storage,
    input: RegisterNodeDescriptor,
    authority: LifecycleControlWriteAuthority<'_>,
) -> LifecycleResult<NodeDescriptor> {
    register_node_inner(storage, input, Some(authority)).await
}

async fn register_node_inner(
    storage: &Storage,
    input: RegisterNodeDescriptor,
    authority: Option<LifecycleControlWriteAuthority<'_>>,
) -> LifecycleResult<NodeDescriptor> {
    require_identifier(&input.mesh_id, "mesh id")?;
    require_identifier(&input.node_id, "node id")?;
    require_identifier(&input.region, "region")?;
    require_identifier(&input.cell_id, "cell id")?;
    if input.receipt_signing_public_key.is_empty() {
        return Err(LifecycleError::InvalidArgument(
            "receipt signing public key must not be empty".to_string(),
        ));
    }
    crate::node_signing::NodeVerifyingKey::from_bytes(&input.receipt_signing_public_key).map_err(
        |err| {
            LifecycleError::InvalidArgument(format!("receipt signing public key is invalid: {err}"))
        },
    )?;
    require_nonempty(&input.public_api_addr, "public api addr")?;
    if input.capabilities.is_empty() {
        return Err(LifecycleError::InvalidArgument(
            "node capabilities must not be empty".to_string(),
        ));
    }
    let capacity_json_hash = capacity_json_hash(&input.capacity_json)?;

    let mut state = read_state(storage).await?;
    if !state.regions.contains_key(&input.region) {
        return Err(LifecycleError::NotFound {
            resource_kind: "region",
            resource_id: input.region,
        });
    }
    let cell_key = cell_key(&input.region, &input.cell_id)?;
    if !state.cells.contains_key(&cell_key) {
        return Err(LifecycleError::NotFound {
            resource_kind: "cell",
            resource_id: input.cell_id.clone(),
        });
    }
    if state.nodes.contains_key(&input.node_id) {
        return Err(LifecycleError::AlreadyExists {
            resource_kind: "node",
            resource_id: input.node_id,
        });
    }

    let now = timestamp_now();
    let descriptor = NodeDescriptor {
        schema: NODE_DESCRIPTOR_SCHEMA.to_string(),
        mesh_id: input.mesh_id,
        node_id: input.node_id.clone(),
        region: input.region,
        cell_id: input.cell_id,
        receipt_signing_public_key: input.receipt_signing_public_key,
        public_api_addr: input.public_api_addr,
        capabilities: input.capabilities,
        capacity_json_hash,
        state: LifecycleState::Joining,
        drain: None,
        last_heartbeat_at: None,
        created_at: now.clone(),
        updated_at: now,
        generation: 1,
    };
    state
        .nodes
        .insert(descriptor.node_id.clone(), descriptor.clone());
    let record_key = node_record_key(&descriptor.region, &descriptor.cell_id, &descriptor.node_id)?;
    let control = authority
        .map(|authority| {
            topology_mutation::fenced_control_mutation(
                NODE_DESCRIPTOR_STREAM_FAMILY,
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
        record_proto::encode_node_projection_row(&descriptor)?,
        control,
    )
    .await?;
    Ok(descriptor)
}

pub async fn put_node_in_transaction(
    storage: &Storage,
    input: RegisterNodeDescriptor,
    target: Option<LifecycleState>,
    transaction_id: &str,
    principal: &str,
) -> LifecycleResult<NodeDescriptor> {
    require_identifier(&input.mesh_id, "mesh id")?;
    require_identifier(&input.node_id, "node id")?;
    require_identifier(&input.region, "region")?;
    require_identifier(&input.cell_id, "cell id")?;
    if input.receipt_signing_public_key.is_empty() {
        return Err(LifecycleError::InvalidArgument(
            "receipt signing public key must not be empty".to_string(),
        ));
    }
    crate::node_signing::NodeVerifyingKey::from_bytes(&input.receipt_signing_public_key).map_err(
        |err| {
            LifecycleError::InvalidArgument(format!("receipt signing public key is invalid: {err}"))
        },
    )?;
    require_nonempty(&input.public_api_addr, "public api addr")?;
    if input.capabilities.is_empty() {
        return Err(LifecycleError::InvalidArgument(
            "node capabilities must not be empty".to_string(),
        ));
    }
    let capacity_json_hash = capacity_json_hash(&input.capacity_json)?;

    let mut state = read_state_for_transaction(storage, transaction_id, principal).await?;
    let transaction_timestamp =
        lifecycle_transaction_timestamp(storage, transaction_id, principal).await?;
    if !state.regions.contains_key(&input.region) {
        return Err(LifecycleError::NotFound {
            resource_kind: "region",
            resource_id: input.region,
        });
    }
    let cell_key = cell_key(&input.region, &input.cell_id)?;
    if !state.cells.contains_key(&cell_key) {
        return Err(LifecycleError::NotFound {
            resource_kind: "cell",
            resource_id: input.cell_id,
        });
    }
    let existing_generation = state
        .nodes
        .get(&input.node_id)
        .map(|descriptor| descriptor.generation);
    let mut descriptor = if let Some(existing) = state.nodes.get(&input.node_id).cloned() {
        if existing.region != input.region
            || existing.cell_id != input.cell_id
            || existing.receipt_signing_public_key != input.receipt_signing_public_key
            || existing.public_api_addr != input.public_api_addr
            || existing.capabilities != input.capabilities
            || existing.capacity_json_hash != capacity_json_hash
        {
            return Err(LifecycleError::InvalidArgument(format!(
                "node {} already exists with different immutable descriptor fields",
                existing.node_id
            )));
        }
        existing
    } else {
        let now = transaction_timestamp.clone();
        NodeDescriptor {
            schema: NODE_DESCRIPTOR_SCHEMA.to_string(),
            mesh_id: input.mesh_id,
            node_id: input.node_id.clone(),
            region: input.region,
            cell_id: input.cell_id,
            receipt_signing_public_key: input.receipt_signing_public_key,
            public_api_addr: input.public_api_addr,
            capabilities: input.capabilities,
            capacity_json_hash,
            state: LifecycleState::Joining,
            drain: None,
            last_heartbeat_at: None,
            created_at: now.clone(),
            updated_at: now,
            generation: 1,
        }
    };

    if let Some(target) = target
        && descriptor.state != target
    {
        if target == LifecycleState::Active {
            ensure_node_placement_is_active(&state, &descriptor)?;
        }
        validate_node_transition(descriptor.state, target).map_err(|_| {
            LifecycleError::LifecycleTransitionDenied {
                resource_kind: "node",
                resource_id: descriptor.node_id.clone(),
                from: descriptor.state,
                to: target,
            }
        })?;
        descriptor.state = target;
        descriptor.drain = None;
        descriptor.updated_at = transaction_timestamp;
        descriptor.generation = descriptor.generation.saturating_add(1);
    }

    if existing_generation.is_some() && descriptor == state.nodes[&input.node_id] {
        return Ok(descriptor);
    }

    state
        .nodes
        .insert(descriptor.node_id.clone(), descriptor.clone());
    let record_key = node_record_key(&descriptor.region, &descriptor.cell_id, &descriptor.node_id)?;
    let control = topology_mutation::transactional_control_mutation(
        NODE_DESCRIPTOR_STREAM_FAMILY,
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
        record_proto::encode_node_projection_row(&descriptor)?,
        control,
        transaction_id,
        principal,
    )
    .await?;
    Ok(descriptor)
}

#[cfg(test)]
pub async fn transition_node(
    storage: &Storage,
    node_id: &str,
    expected_generation: u64,
    target: LifecycleState,
    drain: Option<NodeDrainDescriptor>,
) -> LifecycleResult<NodeDescriptor> {
    transition_node_inner(storage, node_id, expected_generation, target, drain, None).await
}

pub async fn transition_node_with_control(
    storage: &Storage,
    node_id: &str,
    expected_generation: u64,
    target: LifecycleState,
    drain: Option<NodeDrainDescriptor>,
    authority: LifecycleControlWriteAuthority<'_>,
) -> LifecycleResult<NodeDescriptor> {
    transition_node_inner(
        storage,
        node_id,
        expected_generation,
        target,
        drain,
        Some(authority),
    )
    .await
}

async fn transition_node_inner(
    storage: &Storage,
    node_id: &str,
    expected_generation: u64,
    target: LifecycleState,
    drain: Option<NodeDrainDescriptor>,
    authority: Option<LifecycleControlWriteAuthority<'_>>,
) -> LifecycleResult<NodeDescriptor> {
    require_identifier(node_id, "node id")?;
    let mut state = read_state(storage).await?;
    let current = state
        .nodes
        .get(node_id)
        .ok_or_else(|| LifecycleError::NotFound {
            resource_kind: "node",
            resource_id: node_id.to_string(),
        })?;
    ensure_generation("node", node_id, current.generation, expected_generation)?;
    if target == LifecycleState::Active {
        ensure_node_placement_is_active(&state, current)?;
    }
    validate_node_transition(current.state, target).map_err(|_| {
        LifecycleError::LifecycleTransitionDenied {
            resource_kind: "node",
            resource_id: node_id.to_string(),
            from: current.state,
            to: target,
        }
    })?;

    let descriptor = state
        .nodes
        .get_mut(node_id)
        .ok_or_else(|| LifecycleError::NotFound {
            resource_kind: "node",
            resource_id: node_id.to_string(),
        })?;
    descriptor.state = target;
    descriptor.drain = if target == LifecycleState::Draining {
        drain
    } else {
        None
    };
    descriptor.updated_at = timestamp_now();
    descriptor.generation = descriptor.generation.saturating_add(1);
    let out = descriptor.clone();
    let record_key = node_record_key(&out.region, &out.cell_id, &out.node_id)?;
    let control = authority
        .map(|authority| {
            topology_mutation::fenced_control_mutation(
                NODE_DESCRIPTOR_STREAM_FAMILY,
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
        record_proto::encode_node_projection_row(&out)?,
        control,
    )
    .await?;
    Ok(out)
}

pub async fn list_nodes(
    storage: &Storage,
    region_filter: Option<&str>,
    cell_filter: Option<&str>,
) -> LifecycleResult<Vec<NodeDescriptor>> {
    let store = CoreStore::new(storage.clone()).await?;
    list_nodes_with_core_store(storage, &store, region_filter, cell_filter).await
}

pub async fn list_nodes_with_core_store(
    storage: &Storage,
    store: &CoreStore,
    region_filter: Option<&str>,
    cell_filter: Option<&str>,
) -> LifecycleResult<Vec<NodeDescriptor>> {
    if let Some(region) = region_filter.filter(|region| !region.is_empty()) {
        require_identifier(region, "region")?;
    }
    if let Some(cell_id) = cell_filter.filter(|cell_id| !cell_id.is_empty()) {
        require_identifier(cell_id, "cell id")?;
    }
    let nodes = read_state_with_core_store(storage, store)
        .await?
        .nodes
        .into_values()
        .filter(|node| {
            region_filter.is_none_or(|region| region.is_empty() || node.region == region)
        })
        .filter(|node| cell_filter.is_none_or(|cell| cell.is_empty() || node.cell_id == cell))
        .collect();
    Ok(nodes)
}

pub fn list_node_projections_with_core_store(
    store: &CoreStore,
    region_filter: Option<&str>,
    cell_filter: Option<&str>,
) -> LifecycleResult<Vec<NodeDescriptor>> {
    if let Some(region) = region_filter.filter(|region| !region.is_empty()) {
        require_identifier(region, "region")?;
    }
    if let Some(cell_id) = cell_filter.filter(|cell| !cell.is_empty()) {
        require_identifier(cell_id, "cell id")?;
    }
    let nodes = read_lifecycle_state_projection_with_core_store(store)?
        .nodes
        .into_values()
        .filter(|node| {
            region_filter.is_none_or(|region| region.is_empty() || node.region == region)
        })
        .filter(|node| cell_filter.is_none_or(|cell| cell.is_empty() || node.cell_id == cell))
        .collect();
    Ok(nodes)
}
