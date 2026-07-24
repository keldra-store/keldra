use super::*;
use crate::core_store::{
    core_meta_committed_row_common, core_meta_root_key_hash, decode_deterministic_proto,
    encode_deterministic_proto,
};
use prost::Message;
use serde::Serialize;
use std::collections::BTreeMap;

pub(crate) trait LifecycleControlPayload: Serialize {
    fn encode_lifecycle_control_payload(&self, stream_family: &str) -> LifecycleResult<Vec<u8>>;
}

pub(super) const LIFECYCLE_PROJECTION_ROW_SCHEMA: &str =
    "anvil.coremeta.mesh_lifecycle_projection.v1";
pub(super) const LIFECYCLE_PROJECTION_ROW_PREFIX: &str = "mesh-lifecycle-projection";
pub(super) const LIFECYCLE_PROJECTION_REGION_KIND: &str = "region";
pub(super) const LIFECYCLE_PROJECTION_CELL_KIND: &str = "cell";
pub(super) const LIFECYCLE_PROJECTION_NODE_KIND: &str = "node";
pub(super) const LIFECYCLE_PROJECTION_HOST_ALIAS_KIND: &str = "host_alias";
pub(super) const LIFECYCLE_PROJECTION_BUCKET_DRAIN_EXCEPTION_KIND: &str = "bucket_drain_exception";
pub(super) const LIFECYCLE_PROJECTION_TOPOLOGY_ACTIVATION_KIND: &str = "topology_activation";
pub(super) const LIFECYCLE_PROJECTION_TOPOLOGY_HEAD_KIND: &str = "topology_head";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EncodedLifecycleProjectionRow {
    pub kind: &'static str,
    pub record_key: String,
    pub payload: Vec<u8>,
}

pub(super) enum LifecycleProjectionDescriptor {
    Region(RegionDescriptor),
    Cell(CellDescriptor),
    Node(NodeDescriptor),
    HostAlias(HostAliasDescriptor),
    BucketDrainException(BucketDrainExceptionDescriptor),
    TopologyActivation(CanonicalTopologyActivation),
    TopologyHead(LifecycleTopologyHead),
}

#[derive(Clone, PartialEq, Message)]
struct MeshLifecycleStateProto {
    #[prost(message, repeated, tag = "1")]
    regions: Vec<RegionDescriptorProto>,
    #[prost(message, repeated, tag = "2")]
    cells: Vec<CellDescriptorProto>,
    #[prost(message, repeated, tag = "3")]
    nodes: Vec<NodeDescriptorProto>,
    #[prost(message, repeated, tag = "4")]
    host_aliases: Vec<HostAliasDescriptorProto>,
    #[prost(message, repeated, tag = "5")]
    bucket_drain_exceptions: Vec<BucketDrainExceptionDescriptorProto>,
    #[prost(message, optional, tag = "6")]
    canonical_topology_activation: Option<CanonicalTopologyActivationProto>,
    #[prost(message, optional, tag = "7")]
    topology_head: Option<LifecycleTopologyHeadProto>,
}

#[derive(Clone, PartialEq, Message)]
struct MeshLifecycleProjectionRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    kind: String,
    #[prost(string, tag = "4")]
    record_key: String,
    #[prost(uint64, tag = "5")]
    generation: u64,
    #[prost(bytes, tag = "6")]
    descriptor_payload_proto: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct RegionDescriptorProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    mesh_id: String,
    #[prost(string, tag = "3")]
    region: String,
    #[prost(uint32, tag = "4")]
    state: u32,
    #[prost(string, tag = "5")]
    public_base_url: String,
    #[prost(string, tag = "6")]
    virtual_host_suffix: String,
    #[prost(uint32, tag = "7")]
    placement_weight: u32,
    #[prost(string, optional, tag = "8")]
    default_cell: Option<String>,
    #[prost(string, tag = "9")]
    created_at: String,
    #[prost(string, tag = "10")]
    updated_at: String,
    #[prost(uint64, tag = "11")]
    generation: u64,
}

#[derive(Clone, PartialEq, Message)]
struct CellDescriptorProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    mesh_id: String,
    #[prost(string, tag = "3")]
    region: String,
    #[prost(string, tag = "4")]
    cell_id: String,
    #[prost(uint32, tag = "5")]
    state: u32,
    #[prost(uint32, tag = "6")]
    placement_weight: u32,
    #[prost(string, tag = "7")]
    created_at: String,
    #[prost(string, tag = "8")]
    updated_at: String,
    #[prost(uint64, tag = "9")]
    generation: u64,
    #[prost(string, tag = "10")]
    failure_domain: String,
}

#[derive(Clone, PartialEq, Message)]
struct NodeDrainDescriptorProto {
    #[prost(string, tag = "1")]
    started_at: String,
    #[prost(uint64, tag = "2")]
    graceful_timeout_ms: u64,
    #[prost(bool, tag = "3")]
    force_after_timeout: bool,
}

#[derive(Clone, PartialEq, Message)]
struct NodeDescriptorProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    mesh_id: String,
    #[prost(string, tag = "3")]
    node_id: String,
    #[prost(string, tag = "4")]
    region: String,
    #[prost(string, tag = "5")]
    cell_id: String,
    #[prost(bytes, tag = "16")]
    receipt_signing_public_key: Vec<u8>,
    #[prost(string, tag = "7")]
    public_api_addr: String,
    #[prost(uint32, repeated, tag = "9")]
    capabilities: Vec<u32>,
    #[prost(uint32, tag = "10")]
    state: u32,
    #[prost(message, optional, tag = "11")]
    drain: Option<NodeDrainDescriptorProto>,
    #[prost(string, optional, tag = "12")]
    last_heartbeat_at: Option<String>,
    #[prost(string, tag = "13")]
    created_at: String,
    #[prost(string, tag = "14")]
    updated_at: String,
    #[prost(uint64, tag = "15")]
    generation: u64,
    #[prost(string, tag = "17")]
    capacity_json_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct HostAliasDescriptorProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    hostname: String,
    #[prost(string, tag = "3")]
    tenant_id: String,
    #[prost(string, tag = "4")]
    bucket_name: String,
    #[prost(string, tag = "5")]
    region: String,
    #[prost(string, tag = "6")]
    prefix: String,
    #[prost(uint32, tag = "7")]
    state: u32,
    #[prost(string, tag = "8")]
    created_at: String,
    #[prost(string, tag = "9")]
    updated_at: String,
    #[prost(uint64, tag = "10")]
    generation: u64,
}

#[derive(Clone, PartialEq, Message)]
struct BucketDrainExceptionDescriptorProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    tenant_id: String,
    #[prost(string, tag = "3")]
    bucket_name: String,
    #[prost(string, tag = "4")]
    region: String,
    #[prost(uint32, tag = "5")]
    disposition: u32,
    #[prost(string, tag = "6")]
    reason: String,
    #[prost(string, optional, tag = "7")]
    expires_at: Option<String>,
    #[prost(uint64, tag = "8")]
    generation: u64,
}

#[derive(Clone, PartialEq, Message)]
struct CanonicalTopologyActivationProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    mesh_id: String,
    #[prost(uint64, tag = "3")]
    pre_activation_topology_head_generation: u64,
    #[prost(string, tag = "4")]
    pre_activation_topology_head_hash: String,
    #[prost(string, tag = "5")]
    topology_hash: String,
    #[prost(string, repeated, tag = "6")]
    metadata_node_ids: Vec<String>,
    #[prost(string, tag = "7")]
    quorum_profile: String,
    #[prost(uint64, tag = "8")]
    activated_at_unix_nanos: u64,
    #[prost(uint64, tag = "9")]
    generation: u64,
    #[prost(string, tag = "10")]
    payload_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct LifecycleTopologyHeadProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    mesh_id: String,
    #[prost(string, tag = "3")]
    topology_hash: String,
    #[prost(uint64, tag = "4")]
    generation: u64,
}

pub(super) enum LifecycleControlDescriptor {
    Region(RegionDescriptor),
    Cell(CellDescriptor),
    Node(NodeDescriptor),
}

pub(super) fn encode_lifecycle_state(state: &MeshLifecycleState) -> LifecycleResult<Vec<u8>> {
    Ok(encode_deterministic_proto(&MeshLifecycleStateProto {
        regions: state.regions.values().map(region_to_proto).collect(),
        cells: state.cells.values().map(cell_to_proto).collect(),
        nodes: state.nodes.values().map(node_to_proto).collect(),
        host_aliases: state
            .host_aliases
            .values()
            .map(host_alias_to_proto)
            .collect(),
        bucket_drain_exceptions: state
            .bucket_drain_exceptions
            .values()
            .map(bucket_drain_exception_to_proto)
            .collect(),
        canonical_topology_activation: state
            .canonical_topology_activation
            .as_ref()
            .map(topology_activation_to_proto),
        topology_head: state.topology_head.as_ref().map(topology_head_to_proto),
    }))
}

pub(super) fn decode_lifecycle_state(bytes: &[u8]) -> LifecycleResult<MeshLifecycleState> {
    let proto: MeshLifecycleStateProto = decode_deterministic(bytes, "mesh lifecycle state")?;
    let mut state = MeshLifecycleState::default();
    for region in proto.regions {
        let descriptor = region_from_proto(region)?;
        insert_unique(
            &mut state.regions,
            descriptor.region.clone(),
            descriptor,
            "region",
        )?;
    }
    for cell in proto.cells {
        let descriptor = cell_from_proto(cell)?;
        let key = cell_key(&descriptor.region, &descriptor.cell_id)?;
        insert_unique(&mut state.cells, key, descriptor, "cell")?;
    }
    for node in proto.nodes {
        let descriptor = node_from_proto(node)?;
        insert_unique(
            &mut state.nodes,
            descriptor.node_id.clone(),
            descriptor,
            "node",
        )?;
    }
    for alias in proto.host_aliases {
        let descriptor = host_alias_from_proto(alias)?;
        insert_unique(
            &mut state.host_aliases,
            descriptor.hostname.clone(),
            descriptor,
            "host alias",
        )?;
    }
    for exception in proto.bucket_drain_exceptions {
        let descriptor = bucket_drain_exception_from_proto(exception)?;
        let key = bucket_drain_exception_key(
            &descriptor.region,
            &descriptor.tenant_id,
            &descriptor.bucket_name,
        );
        insert_unique(
            &mut state.bucket_drain_exceptions,
            key,
            descriptor,
            "bucket drain exception",
        )?;
    }
    state.canonical_topology_activation = proto
        .canonical_topology_activation
        .map(topology_activation_from_proto)
        .transpose()?;
    state.topology_head = proto
        .topology_head
        .map(topology_head_from_proto)
        .transpose()?;
    Ok(state)
}

pub(super) fn encode_region_projection_row(
    descriptor: &RegionDescriptor,
) -> LifecycleResult<EncodedLifecycleProjectionRow> {
    encode_projection_row(
        LIFECYCLE_PROJECTION_REGION_KIND,
        descriptor.region.clone(),
        descriptor.generation,
        encode_deterministic_proto(&region_to_proto(descriptor)),
    )
}

pub(super) fn encode_cell_projection_row(
    descriptor: &CellDescriptor,
) -> LifecycleResult<EncodedLifecycleProjectionRow> {
    let record_key = cell_key(&descriptor.region, &descriptor.cell_id)?;
    encode_projection_row(
        LIFECYCLE_PROJECTION_CELL_KIND,
        record_key,
        descriptor.generation,
        encode_deterministic_proto(&cell_to_proto(descriptor)),
    )
}

pub(super) fn encode_node_projection_row(
    descriptor: &NodeDescriptor,
) -> LifecycleResult<EncodedLifecycleProjectionRow> {
    let record_key = node_record_key(&descriptor.region, &descriptor.cell_id, &descriptor.node_id)?;
    encode_projection_row(
        LIFECYCLE_PROJECTION_NODE_KIND,
        record_key,
        descriptor.generation,
        encode_deterministic_proto(&node_to_proto(descriptor)),
    )
}

pub(super) fn encode_host_alias_projection_row(
    descriptor: &HostAliasDescriptor,
) -> LifecycleResult<EncodedLifecycleProjectionRow> {
    encode_projection_row(
        LIFECYCLE_PROJECTION_HOST_ALIAS_KIND,
        descriptor.hostname.clone(),
        descriptor.generation,
        encode_deterministic_proto(&host_alias_to_proto(descriptor)),
    )
}

pub(super) fn encode_bucket_drain_exception_projection_row(
    descriptor: &BucketDrainExceptionDescriptor,
) -> LifecycleResult<EncodedLifecycleProjectionRow> {
    let record_key = bucket_drain_exception_key(
        &descriptor.region,
        &descriptor.tenant_id,
        &descriptor.bucket_name,
    );
    encode_projection_row(
        LIFECYCLE_PROJECTION_BUCKET_DRAIN_EXCEPTION_KIND,
        record_key,
        descriptor.generation,
        encode_deterministic_proto(&bucket_drain_exception_to_proto(descriptor)),
    )
}

pub(super) fn encode_topology_activation_projection_row(
    activation: &CanonicalTopologyActivation,
) -> LifecycleResult<EncodedLifecycleProjectionRow> {
    topology_activation::validate_canonical_topology_activation(activation)?;
    encode_projection_row(
        LIFECYCLE_PROJECTION_TOPOLOGY_ACTIVATION_KIND,
        activation.mesh_id.clone(),
        activation.generation,
        encode_deterministic_proto(&topology_activation_to_proto(activation)),
    )
}

pub(super) fn encode_topology_head_projection_row(
    head: &LifecycleTopologyHead,
) -> LifecycleResult<EncodedLifecycleProjectionRow> {
    validate_topology_head(head)?;
    encode_projection_row(
        LIFECYCLE_PROJECTION_TOPOLOGY_HEAD_KIND,
        head.mesh_id.clone(),
        head.generation,
        encode_deterministic_proto(&topology_head_to_proto(head)),
    )
}

pub(super) fn decode_lifecycle_projection_row(
    bytes: &[u8],
) -> LifecycleResult<LifecycleProjectionDescriptor> {
    let row: MeshLifecycleProjectionRowProto =
        decode_deterministic(bytes, "mesh lifecycle projection row")?;
    row.common.as_ref().ok_or_else(|| {
        LifecycleError::InvalidArgument(
            "mesh lifecycle projection row is missing CoreMeta common".to_string(),
        )
    })?;
    ensure_schema(
        &row.schema,
        LIFECYCLE_PROJECTION_ROW_SCHEMA,
        "mesh lifecycle projection row",
    )?;
    match row.kind.as_str() {
        LIFECYCLE_PROJECTION_REGION_KIND => {
            let descriptor: RegionDescriptor = region_from_proto(decode_deterministic(
                &row.descriptor_payload_proto,
                "region projection payload",
            )?)?;
            ensure_projection_row_scope(
                &row,
                LIFECYCLE_PROJECTION_REGION_KIND,
                &descriptor.region,
                descriptor.generation,
            )?;
            Ok(LifecycleProjectionDescriptor::Region(descriptor))
        }
        LIFECYCLE_PROJECTION_CELL_KIND => {
            let descriptor: CellDescriptor = cell_from_proto(decode_deterministic(
                &row.descriptor_payload_proto,
                "cell projection payload",
            )?)?;
            let record_key = cell_key(&descriptor.region, &descriptor.cell_id)?;
            ensure_projection_row_scope(
                &row,
                LIFECYCLE_PROJECTION_CELL_KIND,
                &record_key,
                descriptor.generation,
            )?;
            Ok(LifecycleProjectionDescriptor::Cell(descriptor))
        }
        LIFECYCLE_PROJECTION_NODE_KIND => {
            let descriptor: NodeDescriptor = node_from_proto(decode_deterministic(
                &row.descriptor_payload_proto,
                "node projection payload",
            )?)?;
            let record_key =
                node_record_key(&descriptor.region, &descriptor.cell_id, &descriptor.node_id)?;
            ensure_projection_row_scope(
                &row,
                LIFECYCLE_PROJECTION_NODE_KIND,
                &record_key,
                descriptor.generation,
            )?;
            Ok(LifecycleProjectionDescriptor::Node(descriptor))
        }
        LIFECYCLE_PROJECTION_HOST_ALIAS_KIND => {
            let descriptor: HostAliasDescriptor = host_alias_from_proto(decode_deterministic(
                &row.descriptor_payload_proto,
                "host alias projection payload",
            )?)?;
            ensure_projection_row_scope(
                &row,
                LIFECYCLE_PROJECTION_HOST_ALIAS_KIND,
                &descriptor.hostname,
                descriptor.generation,
            )?;
            Ok(LifecycleProjectionDescriptor::HostAlias(descriptor))
        }
        LIFECYCLE_PROJECTION_BUCKET_DRAIN_EXCEPTION_KIND => {
            let descriptor: BucketDrainExceptionDescriptor =
                bucket_drain_exception_from_proto(decode_deterministic(
                    &row.descriptor_payload_proto,
                    "bucket drain exception projection payload",
                )?)?;
            let record_key = bucket_drain_exception_key(
                &descriptor.region,
                &descriptor.tenant_id,
                &descriptor.bucket_name,
            );
            ensure_projection_row_scope(
                &row,
                LIFECYCLE_PROJECTION_BUCKET_DRAIN_EXCEPTION_KIND,
                &record_key,
                descriptor.generation,
            )?;
            Ok(LifecycleProjectionDescriptor::BucketDrainException(
                descriptor,
            ))
        }
        LIFECYCLE_PROJECTION_TOPOLOGY_ACTIVATION_KIND => {
            let activation = topology_activation_from_proto(decode_deterministic(
                &row.descriptor_payload_proto,
                "canonical topology activation projection payload",
            )?)?;
            ensure_projection_row_scope(
                &row,
                LIFECYCLE_PROJECTION_TOPOLOGY_ACTIVATION_KIND,
                &activation.mesh_id,
                activation.generation,
            )?;
            Ok(LifecycleProjectionDescriptor::TopologyActivation(
                activation,
            ))
        }
        LIFECYCLE_PROJECTION_TOPOLOGY_HEAD_KIND => {
            let head = topology_head_from_proto(decode_deterministic(
                &row.descriptor_payload_proto,
                "lifecycle topology head projection payload",
            )?)?;
            ensure_projection_row_scope(
                &row,
                LIFECYCLE_PROJECTION_TOPOLOGY_HEAD_KIND,
                &head.mesh_id,
                head.generation,
            )?;
            Ok(LifecycleProjectionDescriptor::TopologyHead(head))
        }
        _ => Err(LifecycleError::InvalidArgument(format!(
            "unknown mesh lifecycle projection row kind {}",
            row.kind
        ))),
    }
}

fn encode_projection_row(
    kind: &'static str,
    record_key: String,
    generation: u64,
    descriptor_payload_proto: Vec<u8>,
) -> LifecycleResult<EncodedLifecycleProjectionRow> {
    let root_anchor_key = projection_root_anchor_key(kind, &record_key);
    let row = MeshLifecycleProjectionRowProto {
        common: Some(core_meta_committed_row_common(
            "mesh",
            core_meta_root_key_hash(&root_anchor_key),
            generation,
            format!("mesh-lifecycle:{kind}:{generation}"),
            0,
        )),
        schema: LIFECYCLE_PROJECTION_ROW_SCHEMA.to_string(),
        kind: kind.to_string(),
        record_key: record_key.clone(),
        generation,
        descriptor_payload_proto,
    };
    Ok(EncodedLifecycleProjectionRow {
        kind,
        record_key,
        payload: encode_deterministic_proto(&row),
    })
}

pub(super) fn projection_root_anchor_key(kind: &str, record_key: &str) -> String {
    if matches!(
        kind,
        LIFECYCLE_PROJECTION_REGION_KIND
            | LIFECYCLE_PROJECTION_CELL_KIND
            | LIFECYCLE_PROJECTION_NODE_KIND
            | LIFECYCLE_PROJECTION_TOPOLOGY_ACTIVATION_KIND
            | LIFECYCLE_PROJECTION_TOPOLOGY_HEAD_KIND
    ) {
        LIFECYCLE_TOPOLOGY_ROOT_ANCHOR_KEY.to_string()
    } else {
        format!("mesh/lifecycle/{kind}/{record_key}")
    }
}

fn ensure_projection_row_scope(
    row: &MeshLifecycleProjectionRowProto,
    kind: &'static str,
    record_key: &str,
    generation: u64,
) -> LifecycleResult<()> {
    if row.kind != kind || row.record_key != record_key || row.generation != generation {
        return Err(LifecycleError::InvalidArgument(format!(
            "mesh lifecycle projection row scope mismatch: expected {kind}/{record_key}/{generation}, got {}/{}/{}",
            row.kind, row.record_key, row.generation
        )));
    }
    Ok(())
}

pub(crate) fn encode_lifecycle_control_payload<T>(
    payload: &T,
    stream_family: &str,
) -> LifecycleResult<Vec<u8>>
where
    T: LifecycleControlPayload,
{
    payload.encode_lifecycle_control_payload(stream_family)
}

pub(super) fn decode_lifecycle_control_payload(
    stream_family: &str,
    payload_proto: &[u8],
) -> LifecycleResult<LifecycleControlDescriptor> {
    match stream_family {
        REGION_DESCRIPTOR_STREAM_FAMILY => {
            Ok(LifecycleControlDescriptor::Region(region_from_proto(
                decode_deterministic(payload_proto, "region control payload")?,
            )?))
        }
        CELL_DESCRIPTOR_STREAM_FAMILY => Ok(LifecycleControlDescriptor::Cell(cell_from_proto(
            decode_deterministic(payload_proto, "cell control payload")?,
        )?)),
        NODE_DESCRIPTOR_STREAM_FAMILY => Ok(LifecycleControlDescriptor::Node(node_from_proto(
            decode_deterministic(payload_proto, "node control payload")?,
        )?)),
        _ => Err(LifecycleError::InvalidArgument(format!(
            "unknown lifecycle control stream family {stream_family}"
        ))),
    }
}

pub(crate) fn control_payload_operator_json(
    stream_family: &str,
    expected_record_key: &str,
    payload_proto: &[u8],
) -> LifecycleResult<Vec<u8>> {
    match decode_lifecycle_control_payload(stream_family, payload_proto)? {
        LifecycleControlDescriptor::Region(descriptor) => {
            ensure_control_record_key(expected_record_key, &descriptor.region)?;
            Ok(serde_json::to_vec(&descriptor)?)
        }
        LifecycleControlDescriptor::Cell(descriptor) => {
            let key = cell_record_key(&descriptor.region, &descriptor.cell_id)?;
            ensure_control_record_key(expected_record_key, &key)?;
            Ok(serde_json::to_vec(&descriptor)?)
        }
        LifecycleControlDescriptor::Node(descriptor) => {
            let key =
                node_record_key(&descriptor.region, &descriptor.cell_id, &descriptor.node_id)?;
            ensure_control_record_key(expected_record_key, &key)?;
            Ok(serde_json::to_vec(&descriptor)?)
        }
    }
}

impl LifecycleControlPayload for RegionDescriptor {
    fn encode_lifecycle_control_payload(&self, stream_family: &str) -> LifecycleResult<Vec<u8>> {
        ensure_stream_family(stream_family, REGION_DESCRIPTOR_STREAM_FAMILY)?;
        Ok(encode_deterministic_proto(&region_to_proto(self)))
    }
}

impl LifecycleControlPayload for CellDescriptor {
    fn encode_lifecycle_control_payload(&self, stream_family: &str) -> LifecycleResult<Vec<u8>> {
        ensure_stream_family(stream_family, CELL_DESCRIPTOR_STREAM_FAMILY)?;
        Ok(encode_deterministic_proto(&cell_to_proto(self)))
    }
}

impl LifecycleControlPayload for NodeDescriptor {
    fn encode_lifecycle_control_payload(&self, stream_family: &str) -> LifecycleResult<Vec<u8>> {
        ensure_stream_family(stream_family, NODE_DESCRIPTOR_STREAM_FAMILY)?;
        Ok(encode_deterministic_proto(&node_to_proto(self)))
    }
}

fn region_to_proto(descriptor: &RegionDescriptor) -> RegionDescriptorProto {
    RegionDescriptorProto {
        schema: descriptor.schema.clone(),
        mesh_id: descriptor.mesh_id.clone(),
        region: descriptor.region.clone(),
        state: lifecycle_state_to_proto(descriptor.state),
        public_base_url: descriptor.public_base_url.clone(),
        virtual_host_suffix: descriptor.virtual_host_suffix.clone(),
        placement_weight: descriptor.placement_weight,
        default_cell: descriptor.default_cell.clone(),
        created_at: descriptor.created_at.clone(),
        updated_at: descriptor.updated_at.clone(),
        generation: descriptor.generation,
    }
}

fn region_from_proto(proto: RegionDescriptorProto) -> LifecycleResult<RegionDescriptor> {
    let descriptor = RegionDescriptor {
        schema: proto.schema,
        mesh_id: proto.mesh_id,
        region: proto.region,
        state: lifecycle_state_from_proto(proto.state)?,
        public_base_url: proto.public_base_url,
        virtual_host_suffix: proto.virtual_host_suffix,
        placement_weight: proto.placement_weight,
        default_cell: proto.default_cell,
        created_at: proto.created_at,
        updated_at: proto.updated_at,
        generation: proto.generation,
    };
    ensure_schema(
        &descriptor.schema,
        REGION_DESCRIPTOR_SCHEMA,
        "region descriptor",
    )?;
    require_identifier(&descriptor.mesh_id, "region mesh id")?;
    require_identifier(&descriptor.region, "region")?;
    Ok(descriptor)
}

fn cell_to_proto(descriptor: &CellDescriptor) -> CellDescriptorProto {
    CellDescriptorProto {
        schema: descriptor.schema.clone(),
        mesh_id: descriptor.mesh_id.clone(),
        region: descriptor.region.clone(),
        cell_id: descriptor.cell_id.clone(),
        state: lifecycle_state_to_proto(descriptor.state),
        placement_weight: descriptor.placement_weight,
        created_at: descriptor.created_at.clone(),
        updated_at: descriptor.updated_at.clone(),
        generation: descriptor.generation,
        failure_domain: descriptor.failure_domain.clone(),
    }
}

fn cell_from_proto(proto: CellDescriptorProto) -> LifecycleResult<CellDescriptor> {
    let descriptor = CellDescriptor {
        schema: proto.schema,
        mesh_id: proto.mesh_id,
        region: proto.region,
        cell_id: proto.cell_id,
        state: lifecycle_state_from_proto(proto.state)?,
        placement_weight: proto.placement_weight,
        failure_domain: proto.failure_domain,
        created_at: proto.created_at,
        updated_at: proto.updated_at,
        generation: proto.generation,
    };
    ensure_schema(
        &descriptor.schema,
        CELL_DESCRIPTOR_SCHEMA,
        "cell descriptor",
    )?;
    require_identifier(&descriptor.mesh_id, "cell mesh id")?;
    require_identifier(&descriptor.region, "cell region")?;
    require_identifier(&descriptor.cell_id, "cell id")?;
    require_identifier(&descriptor.failure_domain, "cell failure domain")?;
    Ok(descriptor)
}

fn node_to_proto(descriptor: &NodeDescriptor) -> NodeDescriptorProto {
    NodeDescriptorProto {
        schema: descriptor.schema.clone(),
        mesh_id: descriptor.mesh_id.clone(),
        node_id: descriptor.node_id.clone(),
        region: descriptor.region.clone(),
        cell_id: descriptor.cell_id.clone(),
        receipt_signing_public_key: descriptor.receipt_signing_public_key.clone(),
        public_api_addr: descriptor.public_api_addr.clone(),
        capabilities: descriptor
            .capabilities
            .iter()
            .copied()
            .map(node_capability_to_proto)
            .collect(),
        state: lifecycle_state_to_proto(descriptor.state),
        drain: descriptor.drain.as_ref().map(node_drain_to_proto),
        last_heartbeat_at: descriptor.last_heartbeat_at.clone(),
        created_at: descriptor.created_at.clone(),
        updated_at: descriptor.updated_at.clone(),
        generation: descriptor.generation,
        capacity_json_hash: descriptor.capacity_json_hash.clone(),
    }
}

fn node_from_proto(proto: NodeDescriptorProto) -> LifecycleResult<NodeDescriptor> {
    let descriptor = NodeDescriptor {
        schema: proto.schema,
        mesh_id: proto.mesh_id,
        node_id: proto.node_id,
        region: proto.region,
        cell_id: proto.cell_id,
        receipt_signing_public_key: proto.receipt_signing_public_key,
        public_api_addr: proto.public_api_addr,
        capabilities: proto
            .capabilities
            .into_iter()
            .map(node_capability_from_proto)
            .collect::<LifecycleResult<Vec<_>>>()?,
        capacity_json_hash: proto.capacity_json_hash,
        state: lifecycle_state_from_proto(proto.state)?,
        drain: proto.drain.map(node_drain_from_proto).transpose()?,
        last_heartbeat_at: proto.last_heartbeat_at,
        created_at: proto.created_at,
        updated_at: proto.updated_at,
        generation: proto.generation,
    };
    ensure_schema(
        &descriptor.schema,
        NODE_DESCRIPTOR_SCHEMA,
        "node descriptor",
    )?;
    require_identifier(&descriptor.mesh_id, "node mesh id")?;
    require_identifier(&descriptor.node_id, "node id")?;
    require_identifier(&descriptor.region, "node region")?;
    require_identifier(&descriptor.cell_id, "node cell id")?;
    if descriptor.receipt_signing_public_key.is_empty() {
        return Err(LifecycleError::InvalidArgument(
            "node receipt signing public key must not be empty".to_string(),
        ));
    }
    crate::node_signing::NodeVerifyingKey::from_bytes(&descriptor.receipt_signing_public_key)
        .map_err(|err| {
            LifecycleError::InvalidArgument(format!(
                "node receipt signing public key is invalid: {err}"
            ))
        })?;
    if descriptor.capabilities.is_empty() {
        return Err(LifecycleError::InvalidArgument(
            "node capabilities must not be empty".to_string(),
        ));
    }
    require_nonempty(&descriptor.capacity_json_hash, "node capacity JSON hash")?;
    Ok(descriptor)
}

fn node_drain_to_proto(drain: &NodeDrainDescriptor) -> NodeDrainDescriptorProto {
    NodeDrainDescriptorProto {
        started_at: drain.started_at.clone(),
        graceful_timeout_ms: drain.graceful_timeout_ms,
        force_after_timeout: drain.force_after_timeout,
    }
}

fn node_drain_from_proto(proto: NodeDrainDescriptorProto) -> LifecycleResult<NodeDrainDescriptor> {
    require_nonempty(&proto.started_at, "node drain started_at")?;
    Ok(NodeDrainDescriptor {
        started_at: proto.started_at,
        graceful_timeout_ms: proto.graceful_timeout_ms,
        force_after_timeout: proto.force_after_timeout,
    })
}

fn host_alias_to_proto(descriptor: &HostAliasDescriptor) -> HostAliasDescriptorProto {
    HostAliasDescriptorProto {
        schema: descriptor.schema.clone(),
        hostname: descriptor.hostname.clone(),
        tenant_id: descriptor.tenant_id.clone(),
        bucket_name: descriptor.bucket_name.clone(),
        region: descriptor.region.clone(),
        prefix: descriptor.prefix.clone(),
        state: host_alias_state_to_proto(descriptor.state),
        created_at: descriptor.created_at.clone(),
        updated_at: descriptor.updated_at.clone(),
        generation: descriptor.generation,
    }
}

fn host_alias_from_proto(proto: HostAliasDescriptorProto) -> LifecycleResult<HostAliasDescriptor> {
    let descriptor = HostAliasDescriptor {
        schema: proto.schema,
        hostname: routing::normalize_alias_hostname(&proto.hostname)
            .map_err(|err| LifecycleError::InvalidArgument(err.to_string()))?,
        tenant_id: proto.tenant_id,
        bucket_name: proto.bucket_name,
        region: proto.region,
        prefix: proto.prefix,
        state: host_alias_state_from_proto(proto.state)?,
        created_at: proto.created_at,
        updated_at: proto.updated_at,
        generation: proto.generation,
    };
    ensure_schema(
        &descriptor.schema,
        routing::HOST_ALIAS_DESCRIPTOR_SCHEMA,
        "host alias descriptor",
    )?;
    Ok(descriptor)
}

fn bucket_drain_exception_to_proto(
    descriptor: &BucketDrainExceptionDescriptor,
) -> BucketDrainExceptionDescriptorProto {
    BucketDrainExceptionDescriptorProto {
        schema: descriptor.schema.clone(),
        tenant_id: descriptor.tenant_id.clone(),
        bucket_name: descriptor.bucket_name.clone(),
        region: descriptor.region.clone(),
        disposition: bucket_drain_disposition_to_proto(descriptor.disposition),
        reason: descriptor.reason.clone(),
        expires_at: descriptor.expires_at.clone(),
        generation: descriptor.generation,
    }
}

fn bucket_drain_exception_from_proto(
    proto: BucketDrainExceptionDescriptorProto,
) -> LifecycleResult<BucketDrainExceptionDescriptor> {
    let descriptor = BucketDrainExceptionDescriptor {
        schema: proto.schema,
        tenant_id: proto.tenant_id,
        bucket_name: proto.bucket_name,
        region: proto.region,
        disposition: bucket_drain_disposition_from_proto(proto.disposition)?,
        reason: proto.reason,
        expires_at: proto.expires_at,
        generation: proto.generation,
    };
    ensure_schema(
        &descriptor.schema,
        BUCKET_DRAIN_EXCEPTION_SCHEMA,
        "bucket drain exception descriptor",
    )?;
    require_identifier(&descriptor.tenant_id, "bucket drain exception tenant id")?;
    require_identifier(
        &descriptor.bucket_name,
        "bucket drain exception bucket name",
    )?;
    require_identifier(&descriptor.region, "bucket drain exception region")?;
    require_nonempty(&descriptor.reason, "bucket drain exception reason")?;
    Ok(descriptor)
}

fn topology_activation_to_proto(
    activation: &CanonicalTopologyActivation,
) -> CanonicalTopologyActivationProto {
    CanonicalTopologyActivationProto {
        schema: activation.schema.clone(),
        mesh_id: activation.mesh_id.clone(),
        pre_activation_topology_head_generation: activation.pre_activation_topology_head_generation,
        pre_activation_topology_head_hash: activation.pre_activation_topology_head_hash.clone(),
        topology_hash: activation.topology_hash.clone(),
        metadata_node_ids: activation.metadata_node_ids.clone(),
        quorum_profile: activation.quorum_profile.clone(),
        activated_at_unix_nanos: activation.activated_at_unix_nanos,
        generation: activation.generation,
        payload_hash: activation.payload_hash.clone(),
    }
}

fn topology_activation_from_proto(
    proto: CanonicalTopologyActivationProto,
) -> LifecycleResult<CanonicalTopologyActivation> {
    let activation = CanonicalTopologyActivation {
        schema: proto.schema,
        mesh_id: proto.mesh_id,
        pre_activation_topology_head_generation: proto.pre_activation_topology_head_generation,
        pre_activation_topology_head_hash: proto.pre_activation_topology_head_hash,
        topology_hash: proto.topology_hash,
        metadata_node_ids: proto.metadata_node_ids,
        quorum_profile: proto.quorum_profile,
        activated_at_unix_nanos: proto.activated_at_unix_nanos,
        generation: proto.generation,
        payload_hash: proto.payload_hash,
    };
    topology_activation::validate_canonical_topology_activation(&activation)?;
    Ok(activation)
}

fn topology_head_to_proto(head: &LifecycleTopologyHead) -> LifecycleTopologyHeadProto {
    LifecycleTopologyHeadProto {
        schema: head.schema.clone(),
        mesh_id: head.mesh_id.clone(),
        topology_hash: head.topology_hash.clone(),
        generation: head.generation,
    }
}

fn topology_head_from_proto(
    proto: LifecycleTopologyHeadProto,
) -> LifecycleResult<LifecycleTopologyHead> {
    let head = LifecycleTopologyHead {
        schema: proto.schema,
        mesh_id: proto.mesh_id,
        topology_hash: proto.topology_hash,
        generation: proto.generation,
    };
    validate_topology_head(&head)?;
    Ok(head)
}

pub(super) fn validate_topology_head(head: &LifecycleTopologyHead) -> LifecycleResult<()> {
    ensure_schema(
        &head.schema,
        LIFECYCLE_TOPOLOGY_HEAD_SCHEMA,
        "lifecycle topology head",
    )?;
    require_identifier(&head.mesh_id, "lifecycle topology head mesh id")?;
    if head.generation == 0 {
        return Err(LifecycleError::InvalidArgument(
            "lifecycle topology head generation must be nonzero".to_string(),
        ));
    }
    topology_activation::validate_sha256_for_topology(
        &head.topology_hash,
        "lifecycle topology head hash",
    )
}

fn decode_deterministic<M>(bytes: &[u8], context: &'static str) -> LifecycleResult<M>
where
    M: Message + Default,
{
    decode_deterministic_proto(bytes, context)
        .map_err(|err| LifecycleError::InvalidArgument(err.to_string()))
}

fn ensure_schema(
    actual: &str,
    expected: &'static str,
    context: &'static str,
) -> LifecycleResult<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(LifecycleError::InvalidArgument(format!(
            "{context} schema must be {expected}, got {actual}"
        )))
    }
}

fn ensure_stream_family(actual: &str, expected: &'static str) -> LifecycleResult<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(LifecycleError::InvalidArgument(format!(
            "lifecycle control payload for {expected} cannot be written to {actual}"
        )))
    }
}

fn ensure_control_record_key(expected: &str, actual: &str) -> LifecycleResult<()> {
    if expected == actual {
        Ok(())
    } else {
        Err(LifecycleError::InvalidArgument(format!(
            "lifecycle control record key mismatch: expected {expected}, got {actual}"
        )))
    }
}

fn insert_unique<T>(
    map: &mut BTreeMap<String, T>,
    key: String,
    value: T,
    kind: &'static str,
) -> LifecycleResult<()> {
    if map.insert(key.clone(), value).is_some() {
        return Err(LifecycleError::InvalidArgument(format!(
            "duplicate {kind} record {key} in lifecycle protobuf"
        )));
    }
    Ok(())
}

fn lifecycle_state_to_proto(state: LifecycleState) -> u32 {
    match state {
        LifecycleState::Joining => 1,
        LifecycleState::Active => 2,
        LifecycleState::ReadOnly => 3,
        LifecycleState::Draining => 4,
        LifecycleState::Drained => 5,
        LifecycleState::DrainedWithExceptions => 6,
        LifecycleState::Offline => 7,
        LifecycleState::Removed => 8,
    }
}

fn lifecycle_state_from_proto(value: u32) -> LifecycleResult<LifecycleState> {
    match value {
        1 => Ok(LifecycleState::Joining),
        2 => Ok(LifecycleState::Active),
        3 => Ok(LifecycleState::ReadOnly),
        4 => Ok(LifecycleState::Draining),
        5 => Ok(LifecycleState::Drained),
        6 => Ok(LifecycleState::DrainedWithExceptions),
        7 => Ok(LifecycleState::Offline),
        8 => Ok(LifecycleState::Removed),
        _ => Err(invalid_enum("lifecycle state", value)),
    }
}

fn node_capability_to_proto(capability: NodeCapability) -> u32 {
    match capability {
        NodeCapability::Object => 1,
        NodeCapability::Index => 2,
        NodeCapability::PersonalDb => 3,
        NodeCapability::Metadata => 4,
        NodeCapability::Gateway => 5,
        NodeCapability::Admin => 6,
    }
}

fn node_capability_from_proto(value: u32) -> LifecycleResult<NodeCapability> {
    match value {
        1 => Ok(NodeCapability::Object),
        2 => Ok(NodeCapability::Index),
        3 => Ok(NodeCapability::PersonalDb),
        4 => Ok(NodeCapability::Metadata),
        5 => Ok(NodeCapability::Gateway),
        6 => Ok(NodeCapability::Admin),
        _ => Err(invalid_enum("node capability", value)),
    }
}

fn bucket_drain_disposition_to_proto(disposition: BucketDrainDisposition) -> u32 {
    match disposition {
        BucketDrainDisposition::BlockUntilEmpty => 1,
        BucketDrainDisposition::RemainProxyOnly => 2,
        BucketDrainDisposition::ReadOnlyUntilRemoved => 3,
        BucketDrainDisposition::DeleteAfterRetention => 4,
    }
}

fn bucket_drain_disposition_from_proto(value: u32) -> LifecycleResult<BucketDrainDisposition> {
    match value {
        1 => Ok(BucketDrainDisposition::BlockUntilEmpty),
        2 => Ok(BucketDrainDisposition::RemainProxyOnly),
        3 => Ok(BucketDrainDisposition::ReadOnlyUntilRemoved),
        4 => Ok(BucketDrainDisposition::DeleteAfterRetention),
        _ => Err(invalid_enum("bucket drain disposition", value)),
    }
}

fn host_alias_state_to_proto(state: HostAliasState) -> u32 {
    match state {
        HostAliasState::PendingVerification => 1,
        HostAliasState::Active => 2,
        HostAliasState::Suspended => 3,
        HostAliasState::Deleted => 4,
    }
}

fn host_alias_state_from_proto(value: u32) -> LifecycleResult<HostAliasState> {
    match value {
        1 => Ok(HostAliasState::PendingVerification),
        2 => Ok(HostAliasState::Active),
        3 => Ok(HostAliasState::Suspended),
        4 => Ok(HostAliasState::Deleted),
        _ => Err(invalid_enum("host alias state", value)),
    }
}

fn invalid_enum(field: &'static str, value: u32) -> LifecycleError {
    LifecycleError::InvalidArgument(format!("invalid {field} protobuf enum value {value}"))
}
