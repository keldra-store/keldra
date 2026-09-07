use keldra_consensus::{CapabilityRange, NodeDescriptor, StateMachine};

pub(crate) const PEER_PROTOCOL_VERSION: u16 = 1;
pub(crate) const STORAGE_FORMAT_VERSION: u16 = 1;

/// Protocols understood by this binary. Fresh clusters select the current
/// version; Raft's selected capability remains authoritative thereafter.
pub(crate) const PEER_PROTOCOL_CAPABILITY: CapabilityRange = CapabilityRange {
    min: PEER_PROTOCOL_VERSION,
    max: PEER_PROTOCOL_VERSION,
};

/// The single storage format understood by this fresh-volume binary.
pub(crate) const STORAGE_FORMAT_CAPABILITY: CapabilityRange = CapabilityRange {
    min: STORAGE_FORMAT_VERSION,
    max: STORAGE_FORMAT_VERSION,
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
