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
            let selected = select_handoff_path_quorum(&candidates, quorum(old.len())?, old.len())?;
            if joining_path_matches(&observed, topology.joining().node_id, selected.as_ref()) {
                return Ok(());
            }
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
                if joining_receipt_matches(&observed, topology.joining().node_id, &receipt) {
                    return Ok(());
                }
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

fn joining_path_matches(
    observed: &BTreeMap<NodeId, ObjectRecordExport>,
    joining: NodeId,
    selected: Option<&ObjectPathSnapshot>,
) -> bool {
    match (observed.get(&joining), selected) {
        (Some(ObjectRecordExport::ExactPath(current)), Some(selected)) => current == selected,
        _ => false,
    }
}

fn joining_receipt_matches(
    observed: &BTreeMap<NodeId, ObjectRecordExport>,
    joining: NodeId,
    selected: &ObjectMutation,
) -> bool {
    matches!(
        observed.get(&joining),
        Some(ObjectRecordExport::Receipt(current)) if current == selected
    )
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
    let selected = select_handoff_path_quorum(&candidates, quorum(old.len())?, old.len())?;
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

/// Selects complete object authority without mistaking replica-local journal
/// cleanup progress for divergent object state. A JOINING replica receives no
/// old source-journal work to retire, and it cannot serve mutations before the
/// final payload/cursor handoff. Journal-managed descriptors are therefore
/// installed as released. User-retained versions and every other snapshot
/// field remain exact quorum authority.
fn select_handoff_path_quorum(
    observed: &[Option<ObjectPathSnapshot>],
    required: usize,
    replica_count: usize,
) -> Result<Option<ObjectPathSnapshot>, Status> {
    let normalized = observed
        .iter()
        .cloned()
        .map(|snapshot| snapshot.map(release_handoff_retention).transpose())
        .collect::<Result<Vec<_>, _>>()?;
    select_object_snapshot_quorum(&normalized, required, replica_count)
}

fn release_handoff_retention(
    mut snapshot: ObjectPathSnapshot,
) -> Result<ObjectPathSnapshot, Status> {
    snapshot
        .validate()
        .map_err(|error| Status::data_loss(error.to_string()))?;
    snapshot
        .journal_released_versions
        .append(&mut snapshot.journal_pending_versions);
    snapshot.journal_released_versions.sort_unstable();
    snapshot
        .validate()
        .map_err(|error| Status::data_loss(error.to_string()))?;
    Ok(snapshot)
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

#[cfg(test)]
mod tests {
    use keldra_store::{
        BlobRef, Head, LEGACY_OBJECT_MUTATION_FORMAT, MUTATION_STAMP_FORMAT, MutationStamp,
        PlacementLogId, SourceId, Version, VersionId,
    };
    use tonic::Code;

    use super::*;

    fn snapshot() -> ObjectPathSnapshot {
        let version = VersionId(7);
        ObjectPathSnapshot {
            tenant_id: 11,
            bucket_id: 22,
            exact_path: "objects/entry".into(),
            head: Head {
                version,
                deleted: false,
                mutation_stamp: None,
            },
            versions: vec![Version {
                id: version,
                blob: Some(BlobRef {
                    hash: [1; 32],
                    length: 1,
                }),
                content_type: None,
                deleted: false,
                committed_at_unix_millis: 1,
                protected_link_descriptor: false,
            }],
            journal_pending_versions: vec![version],
            journal_released_versions: Vec::new(),
            definition_locator: None,
            alias_registry: None,
            alias_registry_transition: None,
        }
    }

    fn receipt() -> ObjectMutation {
        ObjectMutation {
            format: LEGACY_OBJECT_MUTATION_FORMAT,
            tenant_id: 11,
            bucket_id: 22,
            exact_path: "objects/entry".into(),
            command_id: "command-1".into(),
            input_fingerprint: [1; 32],
            version: snapshot().versions.remove(0),
            receipt_expires_at_unix_millis: 2,
            stamp: MutationStamp {
                format: MUTATION_STAMP_FORMAT,
                predecessor_version: None,
                program_commit_cursor: None,
                mutation_fingerprint: [2; 32],
                active_placement_log_id: PlacementLogId { term: 1, index: 2 },
                serving_fence_term: 1,
                source_id: SourceId {
                    node_id: 1,
                    source_epoch: [3; 32],
                },
                source_journal_position: 4,
            },
            reference_deltas: Vec::new(),
            accounting_transition: None,
            definition_transition: None,
            alias_snapshot: None,
        }
    }

    #[test]
    fn matching_joiner_path_observation_needs_no_live_read() {
        let joining = NodeId(3);
        let selected = release_handoff_retention(snapshot()).unwrap();
        let mut observed =
            BTreeMap::from([(joining, ObjectRecordExport::ExactPath(selected.clone()))]);

        assert!(joining_path_matches(&observed, joining, Some(&selected)));
        observed.insert(joining, ObjectRecordExport::ExactPath(snapshot()));
        assert!(!joining_path_matches(&observed, joining, Some(&selected)));
        assert!(!joining_path_matches(&BTreeMap::new(), joining, None));
    }

    #[test]
    fn matching_joiner_receipt_observation_needs_no_reinstall() {
        let joining = NodeId(3);
        let selected = receipt();
        let mut observed =
            BTreeMap::from([(joining, ObjectRecordExport::Receipt(selected.clone()))]);

        assert!(joining_receipt_matches(&observed, joining, &selected));
        let mut different = selected.clone();
        different.input_fingerprint = [9; 32];
        observed.insert(joining, ObjectRecordExport::Receipt(different));
        assert!(!joining_receipt_matches(&observed, joining, &selected));
        assert!(!joining_receipt_matches(
            &BTreeMap::new(),
            joining,
            &selected
        ));
    }

    #[test]
    fn handoff_equates_pending_and_released_journal_retention() {
        let pending = snapshot();
        let mut released = pending.clone();
        released.journal_pending_versions.clear();
        released
            .journal_released_versions
            .push(released.head.version);

        assert_eq!(
            select_handoff_path_quorum(&[Some(pending), Some(released.clone())], 2, 2).unwrap(),
            Some(released)
        );
    }

    #[test]
    fn handoff_does_not_hide_authoritative_or_user_retention_divergence() {
        let pending = snapshot();
        let mut conflicting = pending.clone();
        conflicting.versions[0].blob.as_mut().unwrap().hash = [2; 32];
        assert_eq!(
            select_handoff_path_quorum(&[Some(pending.clone()), Some(conflicting)], 2, 2)
                .unwrap_err()
                .code(),
            Code::Unavailable
        );

        let mut user_retained = pending.clone();
        user_retained.journal_pending_versions.clear();
        assert_eq!(
            select_handoff_path_quorum(&[Some(pending), Some(user_retained)], 2, 2)
                .unwrap_err()
                .code(),
            Code::Unavailable
        );
    }
}
