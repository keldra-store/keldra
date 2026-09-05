use std::time::Duration;

use keldra_consensus::{CapabilityRange, DecisionRaft, NodeDescriptor, NodeId, StateMachine};

pub(crate) const BASELINE_PEER_PROTOCOL_VERSION: u16 = 1;
pub(crate) const BASELINE_STORAGE_FORMAT_VERSION: u16 = 1;
pub(crate) const GENERALIZED_ATOMIC_PEER_PROTOCOL_VERSION: u16 = 2;
pub(crate) const GENERALIZED_ATOMIC_STORAGE_FORMAT_VERSION: u16 = 2;

/// Protocols understood by this binary. Fresh clusters select the current
/// version; Raft's selected capability remains authoritative thereafter.
pub(crate) const PEER_PROTOCOL_CAPABILITY: CapabilityRange = CapabilityRange {
    min: BASELINE_PEER_PROTOCOL_VERSION,
    max: GENERALIZED_ATOMIC_PEER_PROTOCOL_VERSION,
};

/// Storage formats understood by this binary. Version 2 includes durable path
/// and governance reservations and is selected when a fresh cluster starts.
pub(crate) const STORAGE_FORMAT_CAPABILITY: CapabilityRange = CapabilityRange {
    min: BASELINE_STORAGE_FORMAT_VERSION,
    max: GENERALIZED_ATOMIC_STORAGE_FORMAT_VERSION,
};

pub(crate) const fn range_contains(range: CapabilityRange, version: u16) -> bool {
    range.min <= version && version <= range.max
}

pub(crate) fn descriptor_supports_selected(
    state: &StateMachine,
    descriptor: &NodeDescriptor,
) -> bool {
    range_contains(
        descriptor.supported_protocol,
        state.cluster_control().active_protocol_version(),
    ) && range_contains(
        descriptor.supported_storage_format,
        state.cluster_control().active_storage_format(),
    )
}

pub(crate) fn binary_supports_selected(state: &StateMachine) -> bool {
    range_contains(
        PEER_PROTOCOL_CAPABILITY,
        state.cluster_control().active_protocol_version(),
    ) && range_contains(
        STORAGE_FORMAT_CAPABILITY,
        state.cluster_control().active_storage_format(),
    )
}

pub(crate) fn generalized_atomic_paths_active(state: &StateMachine) -> bool {
    state.cluster_control().active_protocol_version() >= GENERALIZED_ATOMIC_PEER_PROTOCOL_VERSION
        && state.cluster_control().active_storage_format()
            >= GENERALIZED_ATOMIC_STORAGE_FORMAT_VERSION
}

pub(crate) struct CapabilityAdvertisementTask(tokio::task::JoinHandle<()>);

impl CapabilityAdvertisementTask {
    pub(crate) fn start(
        decisions: DecisionRaft,
        transport: crate::cluster_peer::ClusterPeerTransport,
        local_node: NodeId,
    ) -> Self {
        Self(tokio::spawn(async move {
            loop {
                match advertise_once(&decisions, &transport, local_node).await {
                    Ok(true) => return,
                    Ok(false) => {}
                    Err(error) => tracing::warn!(
                        node.id = local_node.0,
                        error = %error,
                        "local capability attestation will retry"
                    ),
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }))
    }
}

impl Drop for CapabilityAdvertisementTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn advertise_once(
    decisions: &DecisionRaft,
    transport: &crate::cluster_peer::ClusterPeerTransport,
    local_node: NodeId,
) -> anyhow::Result<bool> {
    let state = decisions.state()?;
    anyhow::ensure!(
        binary_supports_selected(&state),
        "running binary does not support the cluster's selected peer protocol or storage format"
    );
    let descriptor = state
        .cluster_control()
        .nodes()
        .get(&local_node)
        .ok_or_else(|| anyhow::anyhow!("local node has no committed descriptor"))?;
    if range_contains(descriptor.supported_protocol, PEER_PROTOCOL_CAPABILITY.min)
        && range_contains(descriptor.supported_protocol, PEER_PROTOCOL_CAPABILITY.max)
        && range_contains(
            descriptor.supported_storage_format,
            STORAGE_FORMAT_CAPABILITY.min,
        )
        && range_contains(
            descriptor.supported_storage_format,
            STORAGE_FORMAT_CAPABILITY.max,
        )
    {
        return Ok(true);
    }
    let expected_protocol = descriptor.supported_protocol;
    let expected_storage = descriptor.supported_storage_format;
    let replacement_protocol = union(expected_protocol, PEER_PROTOCOL_CAPABILITY);
    let replacement_storage = union(expected_storage, STORAGE_FORMAT_CAPABILITY);
    let leader = decisions
        .current_leader()
        .map(NodeId)
        .ok_or_else(|| anyhow::anyhow!("decision leader is unknown"))?;
    let address = state
        .cluster_control()
        .nodes()
        .get(&leader)
        .ok_or_else(|| anyhow::anyhow!("decision leader has no committed descriptor"))?
        .peer_address
        .0
        .clone();
    let (protocol, storage) = transport
        .update_local_capabilities(
            leader,
            &address,
            expected_protocol,
            expected_storage,
            replacement_protocol,
            replacement_storage,
        )
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    Ok(protocol == replacement_protocol && storage == replacement_storage)
}

const fn union(left: CapabilityRange, right: CapabilityRange) -> CapabilityRange {
    CapabilityRange {
        min: if left.min < right.min {
            left.min
        } else {
            right.min
        },
        max: if left.max > right.max {
            left.max
        } else {
            right.max
        },
    }
}
