use super::*;
use crate::core_store::CoreMetaEncodedOwnedRow;

pub(crate) fn validate_portable_lifecycle_topology_snapshot(
    store: &CoreStore,
    rows: &[CoreMetaEncodedOwnedRow],
) -> LifecycleResult<()> {
    let prefix = lifecycle_projection_row_prefix()?;
    let mut state = MeshLifecycleState::default();
    let mut topology_rows = 0_usize;
    let mut activation_rows = 0_usize;
    let mut head_rows = 0_usize;
    let mut logical_rows = BTreeSet::new();

    for row in rows {
        if row.delete_marker || row.cf != CF_MESH {
            continue;
        }
        let (table_id, tuple_key, payload) = store.decode_coremeta_encoded_owned_row(row)?;
        if !tuple_key.starts_with(&prefix) {
            continue;
        }
        if !is_lifecycle_projection_table(table_id) {
            return Err(LifecycleError::InvalidArgument(
                "portable lifecycle projection is stored in an unsupported CoreMeta table"
                    .to_string(),
            ));
        }

        let projection = record_proto::decode_lifecycle_projection_row(&payload)?;
        let (kind, record_key) = lifecycle_projection_descriptor_key(&projection)?;
        ensure_lifecycle_projection_table(table_id, kind)?;
        let expected_tuple_key = lifecycle_projection_row_key(kind, &record_key)?;
        if tuple_key != expected_tuple_key {
            return Err(LifecycleError::InvalidArgument(format!(
                "portable lifecycle projection {kind}/{record_key} is stored under the wrong physical key"
            )));
        }
        if !logical_rows.insert((kind, record_key)) {
            return Err(LifecycleError::InvalidArgument(
                "portable lifecycle snapshot contains duplicate logical projection rows"
                    .to_string(),
            ));
        }
        match projection {
            record_proto::LifecycleProjectionDescriptor::Region(_)
            | record_proto::LifecycleProjectionDescriptor::Cell(_)
            | record_proto::LifecycleProjectionDescriptor::Node(_) => {
                topology_rows = topology_rows.saturating_add(1);
            }
            record_proto::LifecycleProjectionDescriptor::TopologyActivation(_) => {
                activation_rows = activation_rows.saturating_add(1);
            }
            record_proto::LifecycleProjectionDescriptor::TopologyHead(_) => {
                head_rows = head_rows.saturating_add(1);
            }
            record_proto::LifecycleProjectionDescriptor::HostAlias(_)
            | record_proto::LifecycleProjectionDescriptor::BucketDrainException(_) => {}
        }
        apply_lifecycle_projection_row(&mut state, table_id, &payload)?;
    }

    if activation_rows > 1 || head_rows > 1 {
        return Err(LifecycleError::InvalidArgument(
            "portable lifecycle snapshot must contain at most one activation and one topology head"
                .to_string(),
        ));
    }
    let existing_activation = canonical_topology_activation_with_core_store(store)?;
    if topology_rows == 0 {
        ensure_canonical_topology_activation_is_preserved(existing_activation.as_ref(), None)?;
        return Ok(());
    }
    validate_complete_topology_state(&state, existing_activation.as_ref())
}

pub(super) fn validate_complete_topology_state(
    state: &MeshLifecycleState,
    existing_activation: Option<&CanonicalTopologyActivation>,
) -> LifecycleResult<()> {
    let mesh_id = topology_activation::canonical_mesh_id(state)?;
    validate_topology_relationships(state, &mesh_id)?;

    let head = state.topology_head.as_ref().ok_or_else(|| {
        LifecycleError::InvalidArgument(
            "portable lifecycle topology is missing its durable topology head".to_string(),
        )
    })?;
    record_proto::validate_topology_head(head)?;
    if head.mesh_id != mesh_id
        || head.topology_hash != topology_activation::topology_state_hash(state)?
    {
        return Err(LifecycleError::InvalidArgument(
            "portable lifecycle topology head does not match the complete topology snapshot"
                .to_string(),
        ));
    }

    ensure_canonical_topology_activation_is_preserved(
        existing_activation,
        state.canonical_topology_activation.as_ref(),
    )?;
    let cohort = topology_activation::canonical_metadata_cohort(state)?;
    let activation = state.canonical_topology_activation.as_ref();
    if cohort.len() >= 3 && activation.is_none() {
        return Err(LifecycleError::InvalidArgument(
            "portable canonical topology is missing activation evidence".to_string(),
        ));
    }
    let Some(activation) = activation else {
        return Ok(());
    };
    topology_activation::validate_activation_against_topology_head(state, activation, head)?;
    if activation.pre_activation_topology_head_generation == 0
        && activation.pre_activation_topology_head_hash
            != topology_activation::empty_topology_hash()?
    {
        return Err(LifecycleError::InvalidArgument(
            "genesis activation does not reference the empty pre-activation topology head"
                .to_string(),
        ));
    }
    if head.generation
        == activation
            .pre_activation_topology_head_generation
            .saturating_add(1)
        && activation.metadata_node_ids != cohort
    {
        return Err(LifecycleError::InvalidArgument(
            "canonical activation contributor set does not match its activation topology"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_topology_relationships(
    state: &MeshLifecycleState,
    mesh_id: &str,
) -> LifecycleResult<()> {
    for region in state.regions.values() {
        if region.mesh_id != mesh_id {
            return Err(LifecycleError::InvalidArgument(format!(
                "region {} belongs to a different mesh",
                region.region
            )));
        }
        if let Some(default_cell) = region.default_cell.as_ref()
            && !state
                .cells
                .contains_key(&cell_key(&region.region, default_cell)?)
        {
            return Err(LifecycleError::InvalidArgument(format!(
                "region {} references missing default cell {default_cell}",
                region.region
            )));
        }
    }
    for cell in state.cells.values() {
        let region = state.regions.get(&cell.region).ok_or_else(|| {
            LifecycleError::InvalidArgument(format!(
                "cell {}/{} references a missing region",
                cell.region, cell.cell_id
            ))
        })?;
        if cell.mesh_id != mesh_id || region.mesh_id != mesh_id {
            return Err(LifecycleError::InvalidArgument(format!(
                "cell {}/{} belongs to a different mesh",
                cell.region, cell.cell_id
            )));
        }
    }
    for node in state.nodes.values() {
        let cell = state
            .cells
            .get(&cell_key(&node.region, &node.cell_id)?)
            .ok_or_else(|| {
                LifecycleError::InvalidArgument(format!(
                    "node {} references a missing cell",
                    node.node_id
                ))
            })?;
        if node.mesh_id != mesh_id || cell.mesh_id != mesh_id {
            return Err(LifecycleError::InvalidArgument(format!(
                "node {} belongs to a different mesh",
                node.node_id
            )));
        }
    }
    Ok(())
}
