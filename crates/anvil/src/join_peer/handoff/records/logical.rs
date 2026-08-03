use std::collections::BTreeMap;

use anvil_consensus::NodeId;
use anvil_store::{
    LogicalRecordCandidate, LogicalRecordCursor, LogicalRecordExport, LogicalRecordId,
    LogicalRecordValue, StorageTenantId,
};
use tonic::Status;

use super::{quorum, tenant_name_placement_key};
use crate::data_peer::DataPeerTransport;
use crate::join_peer::handoff::HandoffTopology;
use crate::join_peer::handoff::merge::{MergeSource, next_key};
use crate::logical_record_distribution::highest_valid_candidate;
use crate::placement::PlacementKind;

pub(super) async fn transfer(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
) -> Result<(), Status> {
    let mut sources = topology
        .discovery_endpoints()
        .cloned()
        .map(MergeSource::<LogicalRecordExport, LogicalRecordCursor>::new)
        .collect::<Vec<_>>();
    loop {
        refill(&mut sources, peers).await?;
        let Some(key) = next_key(&sources) else {
            return Ok(());
        };
        let mut observed = BTreeMap::new();
        for source in &mut sources {
            if let Some(record) = source.take_if(&key) {
                observed.insert(source.node_id(), record);
            }
        }
        transfer_identity(topology, peers, observed).await?;
    }
}

async fn refill(
    sources: &mut [MergeSource<LogicalRecordExport, LogicalRecordCursor>],
    peers: &DataPeerTransport,
) -> Result<(), Status> {
    for source in sources {
        if !source.needs_page() {
            continue;
        }
        let node = source.node_id();
        let address = source.address().to_owned();
        let cursor = source.cursor().cloned();
        let page = peers
            .export_logical_records(node, &address, cursor.as_ref())
            .await?;
        source.install_page(page.records, page.next_cursor, |record| {
            record
                .handoff_order_key()
                .map_err(|error| Status::data_loss(error.to_string()))
        })?;
    }
    Ok(())
}

async fn transfer_identity(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
    observed: BTreeMap<NodeId, LogicalRecordExport>,
) -> Result<(), Status> {
    let id = observed
        .values()
        .next()
        .map(|record| record.id.clone())
        .ok_or_else(|| Status::data_loss("logical handoff identity has no observation"))?;
    if observed.values().any(|record| record.id != id) {
        return Err(Status::data_loss(
            "logical handoff order key identifies contradictory records",
        ));
    }
    if matches!(id, LogicalRecordId::TenantSchema { .. }) {
        return Err(Status::failed_precondition(
            "tenant-wide Zanzibar schema catalogue handoff is not installed",
        ));
    }
    let (kind, placement_key) = placement(&id)?;
    let old = topology.old_replicas(kind, &placement_key);
    let candidates = old
        .iter()
        .map(|node| observed.get(node).map(|record| record.candidate.clone()))
        .collect::<Vec<_>>();
    let selected = select(&candidates, quorum(old.len())?)?;
    if !topology
        .new_replicas(kind, &placement_key)
        .contains(&topology.joining().node_id)
    {
        return Ok(());
    }
    repair_joiner(topology, peers, &id, selected.as_ref()).await
}

pub(super) async fn resolve_tenant_id(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
    storage_tenant: &StorageTenantId,
) -> Result<u64, Status> {
    let id = LogicalRecordId::TenantNameClaim {
        storage_tenant: storage_tenant.clone(),
    };
    let key = tenant_name_placement_key(storage_tenant);
    let old = topology.old_replicas(PlacementKind::TenantNameClaim, &key);
    let mut candidates = Vec::with_capacity(old.len());
    for node in &old {
        let address = topology
            .address(*node)
            .ok_or_else(|| Status::data_loss("tenant-name replica has no peer address"))?;
        candidates.push(peers.read_logical_record(*node, address, &id).await?);
    }
    let selected = select(&candidates, quorum(old.len())?)?.ok_or_else(|| {
        Status::failed_precondition(format!(
            "authorization tenant {storage_tenant} has no stable tenant-name claim"
        ))
    })?;
    match selected.typed_value() {
        LogicalRecordValue::TenantNameClaim {
            storage_tenant: selected_name,
            tenant_id,
        } if selected_name == storage_tenant && *tenant_id != 0 => Ok(*tenant_id),
        _ => Err(Status::data_loss(
            "tenant-name record resolved to another logical value",
        )),
    }
}

async fn repair_joiner(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
    id: &LogicalRecordId,
    selected: Option<&LogicalRecordCandidate>,
) -> Result<(), Status> {
    let joining = topology.joining();
    let current = peers
        .read_logical_record(joining.node_id, &joining.address, id)
        .await?;
    if current.as_ref() != selected {
        peers
            .repair_logical_record(joining.node_id, &joining.address, id, selected)
            .await?;
    }
    Ok(())
}

fn select(
    candidates: &[Option<LogicalRecordCandidate>],
    required: usize,
) -> Result<Option<LogicalRecordCandidate>, Status> {
    let observed = candidates.iter().collect::<Vec<_>>();
    highest_valid_candidate(&observed, required)
}

fn placement(id: &LogicalRecordId) -> Result<(PlacementKind, Vec<u8>), Status> {
    match id {
        LogicalRecordId::TenantNameClaim { storage_tenant } => Ok((
            PlacementKind::TenantNameClaim,
            tenant_name_placement_key(storage_tenant),
        )),
        LogicalRecordId::TenantRecord { tenant_id } => Ok((
            PlacementKind::TenantOrBucketRecord,
            tenant_id.to_be_bytes().to_vec(),
        )),
        LogicalRecordId::BucketRecord {
            tenant_id,
            bucket_id,
        }
        | LogicalRecordId::BucketOptions {
            tenant_id,
            bucket_id,
        }
        | LogicalRecordId::BucketPolicy {
            tenant_id,
            bucket_id,
        } => {
            let mut key = tenant_id.to_be_bytes().to_vec();
            key.extend_from_slice(&bucket_id.to_be_bytes());
            Ok((PlacementKind::TenantOrBucketRecord, key))
        }
        LogicalRecordId::BucketNameClaim { tenant_id, bucket } => {
            let mut key = tenant_id.to_be_bytes().to_vec();
            key.extend_from_slice(bucket.as_bytes());
            Ok((PlacementKind::TenantOrBucketRecord, key))
        }
        LogicalRecordId::Application { app_id } => {
            Ok((PlacementKind::Credential, app_id.as_bytes().to_vec()))
        }
        LogicalRecordId::Credential { client_id } => {
            Ok((PlacementKind::Credential, client_id.as_bytes().to_vec()))
        }
        LogicalRecordId::TenantSchema { .. } => Err(Status::failed_precondition(
            "TenantSchema belongs to the tenant-wide Zanzibar catalogue",
        )),
    }
}
