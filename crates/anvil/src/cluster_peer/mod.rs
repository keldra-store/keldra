//! Typed ordinary cluster operations on the mandatory-mTLS peer listener.
//!
//! This lane is deliberately separate from ownership handoff. It carries one
//! applied placement fence on every request and exposes logical operations,
//! never RocksDB column-family keys or values.

mod admin_transport;
mod admission;
mod authz;
mod authz_transport;
mod control;
mod logical_names;
mod programs;
mod public_authz;
mod public_authz_transport;
mod routing;
mod storage;
mod transport;

use std::sync::Arc;
use std::time::Duration;

use anvil_consensus::{CommittedPeerPinProvider, DecisionRaft, NodeId};
use anvil_store::Store;

use crate::distributed_list::AuthoritativeListAuthorizer;
use crate::logical_name_resolution::LateBoundLogicalNameResolution;

pub(crate) mod wire {
    tonic::include_proto!("anvil.cluster_peer.v1");
}

pub(crate) use authz::LateBoundFreshAuthorization;
pub(crate) use control::LateBoundDistributedControl;
pub(crate) use public_authz::{RoutedAuthzHandler, RoutedAuthzHandlers};
pub(crate) use routing::{RoutedCall, RoutedPublicHandler, RoutedPublicHandlers};
pub(crate) use transport::ClusterPeerTransport;

pub(crate) const CLUSTER_PEER_SCHEMA_VERSION: u32 = 1;
const MAX_CLUSTER_PEER_MESSAGE_BYTES: usize = 64 * 1024 * 1024 + 64 * 1024;
const MAX_CLUSTER_OPERATION_TIME: Duration = Duration::from_secs(30);
const MAX_TYPED_JSON_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct ClusterPeerService {
    local_node: NodeId,
    store: Store,
    decisions: DecisionRaft,
    pins: Arc<dyn CommittedPeerPinProvider>,
    list_authorizer: Arc<dyn AuthoritativeListAuthorizer>,
    fresh_authorization: LateBoundFreshAuthorization,
    distributed_control: LateBoundDistributedControl,
    name_resolution: LateBoundLogicalNameResolution,
    routed: RoutedPublicHandlers,
    routed_authz: RoutedAuthzHandlers,
}

pub(crate) type ClusterPeerServer =
    wire::cluster_peer_server::ClusterPeerServer<ClusterPeerService>;

impl ClusterPeerService {
    pub(crate) fn new(
        local_node: NodeId,
        store: Store,
        decisions: DecisionRaft,
        pins: Arc<dyn CommittedPeerPinProvider>,
        list_authorizer: Arc<dyn AuthoritativeListAuthorizer>,
        fresh_authorization: LateBoundFreshAuthorization,
        distributed_control: LateBoundDistributedControl,
        name_resolution: LateBoundLogicalNameResolution,
        routed: RoutedPublicHandlers,
        routed_authz: RoutedAuthzHandlers,
    ) -> Self {
        Self {
            local_node,
            store,
            decisions,
            pins,
            list_authorizer,
            fresh_authorization,
            distributed_control,
            name_resolution,
            routed,
            routed_authz,
        }
    }

    pub(crate) fn into_server(self) -> ClusterPeerServer {
        ClusterPeerServer::new(self)
            .max_decoding_message_size(MAX_CLUSTER_PEER_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_CLUSTER_PEER_MESSAGE_BYTES)
    }
}

fn encode_json<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, tonic::Status> {
    let encoded = serde_json::to_vec(value)
        .map_err(|error| tonic::Status::internal(format!("encode typed peer value: {error}")))?;
    if encoded.len() > MAX_TYPED_JSON_BYTES {
        return Err(tonic::Status::resource_exhausted(
            "typed peer value exceeds the protocol limit",
        ));
    }
    Ok(encoded)
}

fn decode_json<T: serde::de::DeserializeOwned>(encoded: &[u8]) -> Result<T, tonic::Status> {
    if encoded.is_empty() || encoded.len() > MAX_TYPED_JSON_BYTES {
        return Err(tonic::Status::invalid_argument(
            "typed peer value is empty or exceeds the protocol limit",
        ));
    }
    serde_json::from_slice(encoded).map_err(|error| {
        tonic::Status::invalid_argument(format!("decode typed peer value: {error}"))
    })
}

fn require_response_schema(schema: u32) -> Result<(), tonic::Status> {
    if schema == CLUSTER_PEER_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(tonic::Status::failed_precondition(format!(
            "unsupported cluster-peer response schema {schema}"
        )))
    }
}

#[cfg(test)]
mod tests;
