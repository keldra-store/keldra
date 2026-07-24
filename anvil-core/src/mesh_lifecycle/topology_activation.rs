use super::*;

pub const CANONICAL_TOPOLOGY_ACTIVATION_SCHEMA: &str =
    "anvil.mesh.canonical_topology_activation.v1";
pub const CANONICAL_METADATA_QUORUM_PROFILE: &str = "equal-peer-r3-q2";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CanonicalTopologyActivation {
    pub schema: String,
    pub mesh_id: String,
    pub pre_activation_topology_head_generation: u64,
    pub pre_activation_topology_head_hash: String,
    pub topology_hash: String,
    pub metadata_node_ids: Vec<String>,
    pub quorum_profile: String,
    pub activated_at_unix_nanos: u64,
    pub generation: u64,
    pub payload_hash: String,
}

pub(super) fn build_canonical_topology_activation(
    state: &MeshLifecycleState,
    pre_activation_head: Option<&LifecycleTopologyHead>,
    activated_at_unix_nanos: u64,
) -> LifecycleResult<Option<CanonicalTopologyActivation>> {
    let metadata_node_ids = canonical_metadata_cohort(state)?;
    if metadata_node_ids.len() < 3 {
        return Ok(None);
    }
    if activated_at_unix_nanos == 0 {
        return Err(LifecycleError::InvalidArgument(
            "canonical topology activation time must be nonzero".to_string(),
        ));
    }

    let (pre_activation_topology_head_generation, pre_activation_topology_head_hash) =
        match pre_activation_head {
            Some(head) => {
                record_proto::validate_topology_head(head)?;
                (head.generation, head.topology_hash.clone())
            }
            None => (0, empty_topology_hash()?),
        };
    let mut activation = CanonicalTopologyActivation {
        schema: CANONICAL_TOPOLOGY_ACTIVATION_SCHEMA.to_string(),
        mesh_id: canonical_mesh_id(state)?,
        pre_activation_topology_head_generation,
        pre_activation_topology_head_hash,
        topology_hash: topology_state_hash(state)?,
        metadata_node_ids,
        quorum_profile: CANONICAL_METADATA_QUORUM_PROFILE.to_string(),
        activated_at_unix_nanos,
        generation: 1,
        payload_hash: String::new(),
    };
    activation.payload_hash = activation_payload_hash(&activation)?;
    validate_canonical_topology_activation(&activation)?;
    Ok(Some(activation))
}

pub(super) fn validate_canonical_topology_activation_for_state(
    state: &MeshLifecycleState,
    activation: &CanonicalTopologyActivation,
) -> LifecycleResult<()> {
    validate_canonical_topology_activation(activation)?;
    let mesh_id = canonical_mesh_id(state)?;
    if activation.mesh_id != mesh_id {
        return Err(LifecycleError::InvalidArgument(format!(
            "canonical topology activation mesh {} does not match topology mesh {mesh_id}",
            activation.mesh_id
        )));
    }
    let mut failure_domains = BTreeSet::new();
    for node_id in &activation.metadata_node_ids {
        let node = state.nodes.get(node_id).ok_or_else(|| {
            LifecycleError::InvalidArgument(format!(
                "canonical topology activation references missing node {node_id}"
            ))
        })?;
        if node.mesh_id != mesh_id || !node.capabilities.contains(&NodeCapability::Metadata) {
            return Err(LifecycleError::InvalidArgument(format!(
                "canonical topology activation node {node_id} is not a metadata node in mesh {mesh_id}"
            )));
        }
        let cell = state
            .cells
            .get(&cell_key(&node.region, &node.cell_id)?)
            .ok_or_else(|| {
                LifecycleError::InvalidArgument(format!(
                    "canonical topology activation node {node_id} references a missing cell"
                ))
            })?;
        failure_domains.insert((cell.region.clone(), cell.failure_domain.clone()));
    }
    if failure_domains.len() < 3 {
        return Err(LifecycleError::InvalidArgument(
            "canonical topology activation requires three distinct failure domains".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn validate_activation_against_topology_head(
    state: &MeshLifecycleState,
    activation: &CanonicalTopologyActivation,
    head: &LifecycleTopologyHead,
) -> LifecycleResult<()> {
    validate_canonical_topology_activation_for_state(state, activation)?;
    record_proto::validate_topology_head(head)?;
    if head.mesh_id != activation.mesh_id {
        return Err(LifecycleError::InvalidArgument(
            "canonical topology activation and topology head mesh ids differ".to_string(),
        ));
    }
    let minimum_generation = activation
        .pre_activation_topology_head_generation
        .checked_add(1)
        .ok_or_else(|| {
            LifecycleError::InvalidArgument(
                "canonical topology activation generation overflow".to_string(),
            )
        })?;
    if head.generation < minimum_generation {
        return Err(LifecycleError::InvalidArgument(
            "topology head predates canonical topology activation".to_string(),
        ));
    }
    if head.generation == minimum_generation && head.topology_hash != activation.topology_hash {
        return Err(LifecycleError::InvalidArgument(
            "first post-activation topology head does not match activation topology hash"
                .to_string(),
        ));
    }
    Ok(())
}

pub(super) fn validate_canonical_topology_activation(
    activation: &CanonicalTopologyActivation,
) -> LifecycleResult<()> {
    if activation.schema != CANONICAL_TOPOLOGY_ACTIVATION_SCHEMA {
        return Err(LifecycleError::InvalidArgument(format!(
            "canonical topology activation schema must be {CANONICAL_TOPOLOGY_ACTIVATION_SCHEMA}"
        )));
    }
    require_identifier(&activation.mesh_id, "canonical topology activation mesh id")?;
    if activation.quorum_profile != CANONICAL_METADATA_QUORUM_PROFILE {
        return Err(LifecycleError::InvalidArgument(format!(
            "canonical topology activation quorum profile must be {CANONICAL_METADATA_QUORUM_PROFILE}"
        )));
    }
    validate_sha256(
        &activation.pre_activation_topology_head_hash,
        "canonical pre-activation topology-head hash",
    )?;
    if activation.pre_activation_topology_head_generation == 0
        && activation.pre_activation_topology_head_hash != empty_topology_hash()?
    {
        return Err(LifecycleError::InvalidArgument(
            "genesis activation must reference the empty pre-activation topology head".to_string(),
        ));
    }
    validate_sha256(&activation.topology_hash, "canonical topology hash")?;
    validate_sha256(
        &activation.payload_hash,
        "canonical activation payload hash",
    )?;
    if activation.activated_at_unix_nanos == 0 || activation.generation != 1 {
        return Err(LifecycleError::InvalidArgument(
            "canonical topology activation must be immutable generation one with a nonzero time"
                .to_string(),
        ));
    }
    if activation.metadata_node_ids.len() != 3 {
        return Err(LifecycleError::InvalidArgument(
            "canonical topology activation requires exactly three metadata nodes for R3/Q2"
                .to_string(),
        ));
    }
    let mut canonical = activation.metadata_node_ids.clone();
    for node_id in &canonical {
        require_identifier(node_id, "canonical topology metadata node id")?;
        if is_synthetic_control_node_id(node_id) {
            return Err(LifecycleError::InvalidArgument(
                "canonical topology activation cannot name a synthetic control node".to_string(),
            ));
        }
    }
    canonical.sort();
    canonical.dedup();
    if canonical != activation.metadata_node_ids {
        return Err(LifecycleError::InvalidArgument(
            "canonical topology metadata node ids must be sorted and unique".to_string(),
        ));
    }
    if activation.payload_hash != activation_payload_hash(activation)? {
        return Err(LifecycleError::InvalidArgument(
            "canonical topology activation payload hash mismatch".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn is_synthetic_control_node_id(node_id: &str) -> bool {
    node_id.starts_with("local-control-node-")
}

pub(super) fn canonical_mesh_id(state: &MeshLifecycleState) -> LifecycleResult<String> {
    let mut mesh_ids = state
        .regions
        .values()
        .map(|region| region.mesh_id.as_str())
        .chain(state.cells.values().map(|cell| cell.mesh_id.as_str()))
        .chain(state.nodes.values().map(|node| node.mesh_id.as_str()))
        .collect::<BTreeSet<_>>();
    if mesh_ids.len() != 1 {
        return Err(LifecycleError::InvalidArgument(
            "canonical topology must contain exactly one mesh identity".to_string(),
        ));
    }
    let mesh_id = mesh_ids
        .pop_first()
        .expect("one canonical mesh identity was checked")
        .to_string();
    require_identifier(&mesh_id, "canonical topology mesh id")?;
    Ok(mesh_id)
}

pub(super) fn topology_state_hash(state: &MeshLifecycleState) -> LifecycleResult<String> {
    #[derive(Serialize)]
    struct CanonicalTopology<'a> {
        regions: &'a BTreeMap<String, RegionDescriptor>,
        cells: &'a BTreeMap<String, CellDescriptor>,
        nodes: &'a BTreeMap<String, NodeDescriptor>,
    }

    let encoded = serde_json::to_vec(&CanonicalTopology {
        regions: &state.regions,
        cells: &state.cells,
        nodes: &state.nodes,
    })?;
    let mut scoped = b"anvil.mesh.lifecycle_topology.v1\0".to_vec();
    scoped.extend_from_slice(&encoded);
    Ok(format!("sha256:{}", crate::core_store::sha256_hex(&scoped)))
}

pub(super) fn empty_topology_hash() -> LifecycleResult<String> {
    topology_state_hash(&MeshLifecycleState::default())
}

pub(super) fn canonical_metadata_cohort(
    state: &MeshLifecycleState,
) -> LifecycleResult<Vec<String>> {
    let mut candidates = state
        .nodes
        .values()
        .filter(|node| {
            node.state == LifecycleState::Active
                && node.capabilities.contains(&NodeCapability::Metadata)
                && !node.public_api_addr.trim().is_empty()
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    let mut failure_domains = BTreeSet::new();
    let mut cohort = Vec::with_capacity(3);
    for node in candidates {
        let Some(cell) = state.cells.get(&cell_key(&node.region, &node.cell_id)?) else {
            continue;
        };
        let failure_domain = (cell.region.clone(), cell.failure_domain.clone());
        if failure_domains.insert(failure_domain) {
            cohort.push(node.node_id.clone());
        }
        if cohort.len() == 3 {
            break;
        }
    }
    cohort.sort();
    Ok(cohort)
}

fn activation_payload_hash(activation: &CanonicalTopologyActivation) -> LifecycleResult<String> {
    #[derive(Serialize)]
    struct ActivationPayload<'a> {
        schema: &'a str,
        mesh_id: &'a str,
        pre_activation_topology_head_generation: u64,
        pre_activation_topology_head_hash: &'a str,
        topology_hash: &'a str,
        metadata_node_ids: &'a [String],
        quorum_profile: &'a str,
        activated_at_unix_nanos: u64,
        generation: u64,
    }

    let encoded = serde_json::to_vec(&ActivationPayload {
        schema: &activation.schema,
        mesh_id: &activation.mesh_id,
        pre_activation_topology_head_generation: activation.pre_activation_topology_head_generation,
        pre_activation_topology_head_hash: &activation.pre_activation_topology_head_hash,
        topology_hash: &activation.topology_hash,
        metadata_node_ids: &activation.metadata_node_ids,
        quorum_profile: &activation.quorum_profile,
        activated_at_unix_nanos: activation.activated_at_unix_nanos,
        generation: activation.generation,
    })?;
    let mut scoped = b"anvil.mesh.canonical_topology_activation.payload.v1\0".to_vec();
    scoped.extend_from_slice(&encoded);
    Ok(format!("sha256:{}", crate::core_store::sha256_hex(&scoped)))
}

fn validate_sha256(value: &str, field: &str) -> LifecycleResult<()> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(LifecycleError::InvalidArgument(format!(
            "{field} must use sha256"
        )));
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(LifecycleError::InvalidArgument(format!(
            "{field} must contain 32 bytes of hexadecimal digest"
        )));
    }
    Ok(())
}

pub(super) fn validate_sha256_for_topology(value: &str, field: &str) -> LifecycleResult<()> {
    validate_sha256(value, field)
}
