use super::record_proto;
use super::*;
use crate::core_store::{
    CF_MESH, CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaStore, TABLE_MESH_NODE_ROW,
    TABLE_MESH_PARTITION_ROW, core_meta_committed_row_common, replace_core_meta_row_common,
};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BootstrapMeshLifecycleProjection {
    pub regions: Vec<CreateRegionDescriptor>,
    pub cells: Vec<RegisterCellDescriptor>,
    pub nodes: Vec<RegisterNodeDescriptor>,
}

pub fn install_bootstrap_lifecycle_projection(
    storage: &Storage,
    store: &CoreStore,
    input: BootstrapMeshLifecycleProjection,
) -> LifecycleResult<MeshLifecycleState> {
    let existing = read_lifecycle_state_projection_with_core_store(store)?;
    if !existing.regions.is_empty() || !existing.cells.is_empty() || !existing.nodes.is_empty() {
        portable_snapshot::validate_complete_topology_state(
            &existing,
            existing.canonical_topology_activation.as_ref(),
        )?;
        ensure_bootstrap_input_matches(&existing, &input)?;
        return Ok(existing);
    }
    let state = bootstrap_lifecycle_state(input)?;
    install_bootstrap_lifecycle_projection_state(storage, store, &state)?;
    Ok(state)
}

fn ensure_bootstrap_input_matches(
    existing: &MeshLifecycleState,
    input: &BootstrapMeshLifecycleProjection,
) -> LifecycleResult<()> {
    let regions_match = existing.regions.len() == input.regions.len()
        && input.regions.iter().all(|candidate| {
            existing
                .regions
                .get(&candidate.region)
                .is_some_and(|current| {
                    current.mesh_id == candidate.mesh_id
                        && current.state == LifecycleState::Active
                        && current.public_base_url == candidate.public_base_url
                        && current.virtual_host_suffix == candidate.virtual_host_suffix
                        && current.placement_weight == candidate.placement_weight
                        && current.default_cell == candidate.default_cell
                        && current.generation == 1
                })
        });
    let cells_match = existing.cells.len() == input.cells.len()
        && input.cells.iter().all(|candidate| {
            cell_key(&candidate.region, &candidate.cell_id)
                .ok()
                .and_then(|key| existing.cells.get(&key))
                .is_some_and(|current| {
                    current.mesh_id == candidate.mesh_id
                        && current.state == LifecycleState::Active
                        && current.placement_weight == candidate.placement_weight
                        && current.failure_domain == candidate.failure_domain
                        && current.generation == 1
                })
        });
    let nodes_match = existing.nodes.len() == input.nodes.len()
        && input.nodes.iter().all(|candidate| {
            existing
                .nodes
                .get(&candidate.node_id)
                .is_some_and(|current| {
                    current.mesh_id == candidate.mesh_id
                        && current.region == candidate.region
                        && current.cell_id == candidate.cell_id
                        && current.receipt_signing_public_key
                            == candidate.receipt_signing_public_key
                        && current.public_api_addr == candidate.public_api_addr
                        && current.capabilities == candidate.capabilities
                        && capacity_json_hash(&candidate.capacity_json)
                            .is_ok_and(|hash| current.capacity_json_hash == hash)
                        && current.state == LifecycleState::Active
                        && current.drain.is_none()
                        && current.generation == 1
                })
        });
    if regions_match && cells_match && nodes_match {
        return Ok(());
    }
    Err(LifecycleError::InvalidArgument(
        "bootstrap topology differs from the already installed canonical topology".to_string(),
    ))
}

fn bootstrap_lifecycle_state(
    input: BootstrapMeshLifecycleProjection,
) -> LifecycleResult<MeshLifecycleState> {
    let now = timestamp_now();
    let mut state = MeshLifecycleState::default();

    for input in input.regions {
        require_identifier(&input.mesh_id, "bootstrap mesh id")?;
        require_identifier(&input.region, "bootstrap region")?;
        require_nonempty(&input.virtual_host_suffix, "bootstrap virtual host suffix")?;
        if let Some(default_cell) = &input.default_cell {
            require_identifier(default_cell, "bootstrap default cell")?;
        }
        if state.regions.contains_key(&input.region) {
            return Err(LifecycleError::AlreadyExists {
                resource_kind: "region",
                resource_id: input.region,
            });
        }
        let descriptor = RegionDescriptor {
            schema: REGION_DESCRIPTOR_SCHEMA.to_string(),
            mesh_id: input.mesh_id,
            region: input.region.clone(),
            state: LifecycleState::Active,
            public_base_url: input.public_base_url,
            virtual_host_suffix: input.virtual_host_suffix,
            placement_weight: input.placement_weight,
            default_cell: input.default_cell,
            created_at: now.clone(),
            updated_at: now.clone(),
            generation: 1,
        };
        state.regions.insert(descriptor.region.clone(), descriptor);
    }

    for input in input.cells {
        require_identifier(&input.mesh_id, "bootstrap mesh id")?;
        require_identifier(&input.region, "bootstrap cell region")?;
        require_identifier(&input.cell_id, "bootstrap cell id")?;
        require_identifier(&input.failure_domain, "bootstrap cell failure domain")?;
        if !state.regions.contains_key(&input.region) {
            return Err(LifecycleError::NotFound {
                resource_kind: "region",
                resource_id: input.region,
            });
        }
        let key = cell_key(&input.region, &input.cell_id)?;
        if state.cells.contains_key(&key) {
            return Err(LifecycleError::AlreadyExists {
                resource_kind: "cell",
                resource_id: key,
            });
        }
        let descriptor = CellDescriptor {
            schema: CELL_DESCRIPTOR_SCHEMA.to_string(),
            mesh_id: input.mesh_id,
            region: input.region,
            cell_id: input.cell_id,
            state: LifecycleState::Active,
            placement_weight: input.placement_weight,
            failure_domain: input.failure_domain,
            created_at: now.clone(),
            updated_at: now.clone(),
            generation: 1,
        };
        state.cells.insert(key, descriptor);
    }

    for input in input.nodes {
        require_identifier(&input.mesh_id, "bootstrap mesh id")?;
        require_identifier(&input.node_id, "bootstrap node id")?;
        require_identifier(&input.region, "bootstrap node region")?;
        require_identifier(&input.cell_id, "bootstrap node cell id")?;
        if input.receipt_signing_public_key.is_empty() {
            return Err(LifecycleError::InvalidArgument(
                "bootstrap node receipt signing public key must not be empty".to_string(),
            ));
        }
        crate::node_signing::NodeVerifyingKey::from_bytes(&input.receipt_signing_public_key)
            .map_err(|err| {
                LifecycleError::InvalidArgument(format!(
                    "bootstrap node receipt signing public key is invalid: {err}"
                ))
            })?;
        require_nonempty(&input.public_api_addr, "bootstrap node public api addr")?;
        if input.capabilities.is_empty() {
            return Err(LifecycleError::InvalidArgument(
                "bootstrap node capabilities must not be empty".to_string(),
            ));
        }
        if !state.regions.contains_key(&input.region) {
            return Err(LifecycleError::NotFound {
                resource_kind: "region",
                resource_id: input.region,
            });
        }
        let key = cell_key(&input.region, &input.cell_id)?;
        if !state.cells.contains_key(&key) {
            return Err(LifecycleError::NotFound {
                resource_kind: "cell",
                resource_id: input.cell_id,
            });
        }
        if state.nodes.contains_key(&input.node_id) {
            return Err(LifecycleError::AlreadyExists {
                resource_kind: "node",
                resource_id: input.node_id,
            });
        }
        let descriptor = NodeDescriptor {
            schema: NODE_DESCRIPTOR_SCHEMA.to_string(),
            mesh_id: input.mesh_id,
            node_id: input.node_id.clone(),
            region: input.region,
            cell_id: input.cell_id,
            receipt_signing_public_key: input.receipt_signing_public_key,
            public_api_addr: input.public_api_addr,
            capabilities: input.capabilities,
            capacity_json_hash: capacity_json_hash(&input.capacity_json)?,
            state: LifecycleState::Active,
            drain: None,
            last_heartbeat_at: None,
            created_at: now.clone(),
            updated_at: now.clone(),
            generation: 1,
        };
        state.nodes.insert(descriptor.node_id.clone(), descriptor);
    }

    let activated_at_unix_nanos = Utc::now()
        .timestamp_nanos_opt()
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| {
            LifecycleError::InvalidArgument(
                "canonical topology activation time is outside the supported range".to_string(),
            )
        })?;
    state.canonical_topology_activation = topology_activation::build_canonical_topology_activation(
        &state,
        None,
        activated_at_unix_nanos,
    )?;
    state.topology_head = Some(LifecycleTopologyHead {
        schema: LIFECYCLE_TOPOLOGY_HEAD_SCHEMA.to_string(),
        mesh_id: topology_activation::canonical_mesh_id(&state)?,
        topology_hash: topology_activation::topology_state_hash(&state)?,
        generation: 1,
    });

    Ok(state)
}

fn install_bootstrap_lifecycle_projection_state(
    storage: &Storage,
    store: &CoreStore,
    state: &MeshLifecycleState,
) -> LifecycleResult<()> {
    portable_snapshot::validate_complete_topology_state(
        state,
        canonical_topology_activation_with_core_store(store)?.as_ref(),
    )?;
    ensure_canonical_topology_activation_is_preserved(
        canonical_topology_activation_with_core_store(store)?.as_ref(),
        state.canonical_topology_activation.as_ref(),
    )?;
    let rows = encode_lifecycle_projection_rows(state)?;
    let mut desired_keys = BTreeSet::new();
    let mut tuple_keys = Vec::with_capacity(rows.len());
    let mut table_ids = Vec::with_capacity(rows.len());
    let mut payloads = Vec::with_capacity(rows.len());

    for row in rows {
        let table_id = lifecycle_projection_table_id(row.kind)?;
        let tuple_key = lifecycle_projection_row_key(row.kind, &row.record_key)?;
        desired_keys.insert((table_id, tuple_key.clone()));
        table_ids.push(table_id);
        tuple_keys.push(tuple_key);
        // Genesis precedes root publication and quorum, so its canonical rows
        // are immediately visible bootstrap state rather than unpublished
        // generation-one mutations. The same bytes are copied to every peer.
        payloads.push(replace_core_meta_row_common(
            &row.payload,
            &core_meta_committed_row_common("mesh/bootstrap", "", 0, "", 0),
        )?);
    }

    let prefix = lifecycle_projection_row_prefix()?;
    let mut delete_rows = Vec::new();
    for table_id in [TABLE_MESH_PARTITION_ROW, TABLE_MESH_NODE_ROW] {
        for row in scan_lifecycle_projection_rows(store, table_id, &prefix)? {
            let projection = record_proto::decode_lifecycle_projection_row(&row.payload)?;
            let (kind, record_key) = lifecycle_projection_descriptor_key(&projection)?;
            let tuple_key = lifecycle_projection_row_key(kind, &record_key)?;
            if !desired_keys.contains(&(table_id, tuple_key.clone())) {
                delete_rows.push((table_id, tuple_key));
            }
        }
    }

    let mut ops = Vec::with_capacity(payloads.len() + delete_rows.len());
    for index in 0..payloads.len() {
        ops.push(CoreMetaBatchOp {
            cf: CF_MESH,
            table_id: table_ids[index],
            tuple_key: &tuple_keys[index],
            common: None,
            kind: CoreMetaBatchOpKind::Put(&payloads[index]),
        });
    }
    for (table_id, tuple_key) in &delete_rows {
        ops.push(CoreMetaBatchOp {
            cf: CF_MESH,
            table_id: *table_id,
            tuple_key,
            common: None,
            kind: CoreMetaBatchOpKind::Delete,
        });
    }
    if ops.is_empty() {
        return Ok(());
    }
    CoreMetaStore::open(storage.core_store_meta_path())?.write_local_committed_batch(&ops)?;
    Ok(())
}
