use std::collections::BTreeMap;

use keldra_consensus::NodeId;
use keldra_store::{ObjectMutation, ObjectPathSnapshot, ObjectRecordCursor, ObjectRecordExport};
use tonic::Status;

use super::{object_placement_key, quorum};
use crate::data_peer::DataPeerTransport;
use crate::join_peer::handoff::HandoffTopology;
use crate::join_peer::handoff::merge::{MergeSource, next_key};
use crate::object_distribution::select_object_snapshot_quorum;
use crate::placement::PlacementKind;

#[derive(Clone, Debug, Eq, PartialEq)]
enum Identity {
    Path {
        tenant_id: u64,
        bucket_id: u64,
        exact_path: String,
    },
    Receipt {
        tenant_id: u64,
        bucket_id: u64,
        exact_path: String,
        command_id: String,
    },
}

pub(super) async fn transfer(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
) -> Result<(), Status> {
    let mut sources = topology
        .discovery_endpoints()
        .cloned()
        .map(MergeSource::<ObjectRecordExport, ObjectRecordCursor>::new)
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
    sources: &mut [MergeSource<ObjectRecordExport, ObjectRecordCursor>],
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
            .export_object_records(node, &address, cursor.as_ref())
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
    observed: BTreeMap<NodeId, ObjectRecordExport>,
) -> Result<(), Status> {
    let selected_identity = observed
        .values()
        .next()
        .map(identity)
        .ok_or_else(|| Status::data_loss("object handoff identity has no observation"))?;
    if observed
        .values()
        .any(|record| identity(record) != selected_identity)
    {
        return Err(Status::data_loss(
            "object handoff order key identifies contradictory records",
        ));
    }
    let (tenant_id, bucket_id, exact_path) = selected_identity.parts();
    let placement_key = object_placement_key(tenant_id, bucket_id, exact_path);
    let old = topology.old_replicas(PlacementKind::Object, &placement_key);
    if !topology
        .new_replicas(PlacementKind::Object, &placement_key)
        .contains(&topology.joining().node_id)
    {
        return Ok(());
    }
    match selected_identity {
        Identity::Path { .. } => {
            let candidates = old
                .iter()
                .map(|node| match observed.get(node) {
                    Some(ObjectRecordExport::ExactPath(snapshot)) => Ok(Some(snapshot.clone())),
                    Some(ObjectRecordExport::Receipt(_)) => Err(Status::data_loss(
                        "object path identity resolved to a receipt",
                    )),
                    None => Ok(None),
                })
                .collect::<Result<Vec<_>, _>>()?;
            let selected =
                select_object_snapshot_quorum(&candidates, quorum(old.len())?, old.len())?;
            repair_joiner_path(
                topology,
                peers,
                tenant_id,
                bucket_id,
                exact_path,
                selected.as_ref(),
            )
            .await
        }
        Identity::Receipt { .. } => {
            let candidates = old
                .iter()
                .map(|node| match observed.get(node) {
                    Some(ObjectRecordExport::Receipt(receipt)) => Ok(Some(receipt.clone())),
                    Some(ObjectRecordExport::ExactPath(_)) => Err(Status::data_loss(
                        "object receipt identity resolved to a path",
                    )),
                    None => Ok(None),
                })
                .collect::<Result<Vec<_>, _>>()?;
            if let Some(receipt) = select_receipt_quorum(&candidates, quorum(old.len())?)? {
                peers
                    .install_object_record(
                        topology.joining().node_id,
                        &topology.joining().address,
                        &ObjectRecordExport::Receipt(receipt),
                    )
                    .await?;
            }
            Ok(())
        }
    }
}

pub(super) async fn reconcile_path(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
    tenant_id: u64,
    bucket_id: u64,
    exact_path: &str,
) -> Result<(), Status> {
    let placement_key = object_placement_key(tenant_id, bucket_id, exact_path);
    let old = topology.old_replicas(PlacementKind::Object, &placement_key);
    if !topology
        .new_replicas(PlacementKind::Object, &placement_key)
        .contains(&topology.joining().node_id)
    {
        return Ok(());
    }
    let mut candidates = Vec::with_capacity(old.len());
    for node in &old {
        let address = topology
            .address(*node)
            .ok_or_else(|| Status::data_loss("object replica has no peer address"))?;
        candidates.push(
            peers
                .read_handoff_object_path_snapshot(*node, address, tenant_id, bucket_id, exact_path)
                .await?,
        );
    }
    let selected = select_object_snapshot_quorum(&candidates, quorum(old.len())?, old.len())?;
    repair_joiner_path(
        topology,
        peers,
        tenant_id,
        bucket_id,
        exact_path,
        selected.as_ref(),
    )
    .await
}

async fn repair_joiner_path(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
    tenant_id: u64,
    bucket_id: u64,
    exact_path: &str,
    selected: Option<&ObjectPathSnapshot>,
) -> Result<(), Status> {
    let joining = topology.joining();
    let current = peers
        .read_handoff_object_path_snapshot(
            joining.node_id,
            &joining.address,
            tenant_id,
            bucket_id,
            exact_path,
        )
        .await?;
    if current.as_ref() != selected {
        peers
            .repair_handoff_object_path_snapshot(
                joining.node_id,
                &joining.address,
                tenant_id,
                bucket_id,
                exact_path,
                current.as_ref(),
                selected,
            )
            .await?;
    }
    Ok(())
}

fn select_receipt_quorum(
    observed: &[Option<ObjectMutation>],
    required: usize,
) -> Result<Option<ObjectMutation>, Status> {
    for candidate in observed {
        if observed.iter().filter(|other| *other == candidate).count() >= required {
            return Ok(candidate.clone());
        }
    }
    Err(Status::unavailable(
        "object receipt has no exact old-placement quorum",
    ))
}

fn identity(record: &ObjectRecordExport) -> Identity {
    match record {
        ObjectRecordExport::ExactPath(record) => Identity::Path {
            tenant_id: record.tenant_id,
            bucket_id: record.bucket_id,
            exact_path: record.exact_path.clone(),
        },
        ObjectRecordExport::Receipt(record) => Identity::Receipt {
            tenant_id: record.tenant_id,
            bucket_id: record.bucket_id,
            exact_path: record.exact_path.clone(),
            command_id: record.command_id.clone(),
        },
    }
}

impl Identity {
    fn parts(&self) -> (u64, u64, &str) {
        match self {
            Self::Path {
                tenant_id,
                bucket_id,
                exact_path,
            }
            | Self::Receipt {
                tenant_id,
                bucket_id,
                exact_path,
                ..
            } => (*tenant_id, *bucket_id, exact_path),
        }
    }
}
