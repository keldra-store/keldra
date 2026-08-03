//! Bounded typed mutable-record transfer for one ADD transition.

use std::collections::BTreeSet;

use anvil_store::{LocalChange, StorageTenantId};
use tonic::Status;

use super::HandoffTopology;
use crate::data_peer::DataPeerTransport;

mod authz;
mod logical;
mod object;

pub(super) async fn transfer_all(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
) -> Result<(), Status> {
    object::transfer(topology, peers).await?;
    logical::transfer(topology, peers).await?;
    authz::transfer(topology, peers).await
}

pub(super) async fn replay_object_paths(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
    changes: &[LocalChange],
) -> Result<(), Status> {
    let mut paths = BTreeSet::new();
    for change in changes {
        match change {
            LocalChange::ObjectHead(change) => {
                paths.insert((
                    change.tenant_id,
                    change.bucket_id,
                    change.exact_path.clone(),
                ));
            }
            LocalChange::RetainedVersionDeleted(change) => {
                paths.insert((
                    change.tenant_id,
                    change.bucket_id,
                    change.exact_path.clone(),
                ));
            }
            _ => {
                return Err(Status::failed_precondition(
                    "source journal contains an unsupported change kind",
                ));
            }
        }
    }
    for (tenant_id, bucket_id, exact_path) in paths {
        object::reconcile_path(topology, peers, tenant_id, bucket_id, &exact_path).await?;
    }
    Ok(())
}

pub(super) fn quorum(replica_count: usize) -> Result<usize, Status> {
    match replica_count {
        1 => Ok(1),
        2 => Ok(2),
        3 => Ok(2),
        _ => Err(Status::failed_precondition(
            "mutable handoff replica group is invalid",
        )),
    }
}

pub(super) fn object_placement_key(tenant_id: u64, bucket_id: u64, path: &str) -> Vec<u8> {
    let mut key = tenant_id.to_be_bytes().to_vec();
    key.extend_from_slice(&bucket_id.to_be_bytes());
    key.extend_from_slice(path.as_bytes());
    key
}

pub(super) fn tenant_name_placement_key(storage_tenant: &StorageTenantId) -> Vec<u8> {
    storage_tenant.as_str().as_bytes().to_vec()
}
