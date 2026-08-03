use std::collections::BTreeMap;

use anvil_consensus::NodeId;
use anvil_store::{
    AuthzSchemaCatalogue, LogicalRecordCandidate, LogicalRecordCursor, LogicalRecordExport,
    LogicalRecordId, LogicalRecordValue, StorageTenantId,
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
    let mut transferred_catalogue = None;
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
        transfer_identity(topology, peers, observed, &mut transferred_catalogue).await?;
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
    transferred_catalogue: &mut Option<StorageTenantId>,
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
    if let LogicalRecordId::TenantSchema { storage_tenant, .. } = &id {
        if transferred_catalogue.as_ref() != Some(storage_tenant) {
            transfer_schema_catalogue(topology, peers, storage_tenant).await?;
            *transferred_catalogue = Some(storage_tenant.clone());
        }
        return Ok(());
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

async fn transfer_schema_catalogue(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
    storage_tenant: &StorageTenantId,
) -> Result<(), Status> {
    let tenant_id = resolve_tenant_id(topology, peers, storage_tenant).await?;
    let key = tenant_id.to_be_bytes();
    let old = topology.old_replicas(PlacementKind::ZanzibarRealm, &key);
    let mut candidates = Vec::with_capacity(old.len());
    for node in &old {
        let address = topology
            .address(*node)
            .ok_or_else(|| Status::data_loss("schema catalogue replica has no peer address"))?;
        let candidate = peers
            .read_authz_schema_catalogue(*node, address, storage_tenant)
            .await?;
        if let Some(catalogue) = candidate.as_ref() {
            catalogue
                .validate()
                .map_err(|error| Status::data_loss(error.to_string()))?;
            if catalogue.storage_tenant != *storage_tenant {
                return Err(Status::data_loss(
                    "schema catalogue replica returned another tenant",
                ));
            }
        }
        candidates.push(candidate);
    }
    let selected = select_catalogue(&candidates, quorum(old.len())?)?;
    if !topology
        .new_replicas(PlacementKind::ZanzibarRealm, &key)
        .contains(&topology.joining().node_id)
    {
        return Ok(());
    }

    let joining = topology.joining();
    let current = peers
        .read_authz_schema_catalogue(joining.node_id, &joining.address, storage_tenant)
        .await?;
    if current != selected {
        peers
            .repair_authz_schema_catalogue(
                joining.node_id,
                &joining.address,
                storage_tenant,
                selected.as_ref(),
            )
            .await?;
    }
    Ok(())
}

fn select_catalogue(
    candidates: &[Option<AuthzSchemaCatalogue>],
    required: usize,
) -> Result<Option<AuthzSchemaCatalogue>, Status> {
    exact_quorum(candidates, required).ok_or_else(|| {
        Status::unavailable("authorization schema catalogue has no exact read quorum")
    })
}

fn exact_quorum<T: Clone + Eq>(candidates: &[Option<T>], required: usize) -> Option<Option<T>> {
    if required == 0 {
        return None;
    }
    candidates.iter().find_map(|candidate| {
        (candidates
            .iter()
            .filter(|other| *other == candidate)
            .count()
            >= required)
            .then(|| candidate.clone())
    })
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

#[cfg(test)]
mod tests {
    use super::exact_quorum;

    #[test]
    fn exact_catalogue_quorum_selects_present_candidate() {
        assert_eq!(
            exact_quorum(&[Some(7_u8), Some(7), Some(9)], 2),
            Some(Some(7))
        );
    }

    #[test]
    fn exact_catalogue_quorum_can_select_absence() {
        assert_eq!(exact_quorum(&[None, Some(7_u8), None], 2), Some(None));
    }

    #[test]
    fn exact_catalogue_quorum_fails_closed_without_agreement() {
        assert_eq!(exact_quorum(&[Some(7_u8), Some(8), None], 2), None);
    }
}
