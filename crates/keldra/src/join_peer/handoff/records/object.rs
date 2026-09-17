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

#[derive(Clone, Copy)]
pub(super) enum ObjectTransferPhase {
    Precopy,
    Authoritative,
}

#[derive(Debug, PartialEq, Eq)]
enum ReceiptTransferSelection {
    Exact(ObjectMutation),
    Absent,
    Deferred,
}

#[derive(Debug, PartialEq, Eq)]
enum PathTransferSelection {
    Selected(Option<ObjectPathSnapshot>),
    Deferred,
}

pub(super) async fn precopy(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
) -> Result<(), Status> {
    transfer(topology, peers, ObjectTransferPhase::Precopy).await
}

pub(super) async fn transfer_authoritatively(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
) -> Result<(), Status> {
    transfer(topology, peers, ObjectTransferPhase::Authoritative).await
}

async fn transfer(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
    transfer_phase: ObjectTransferPhase,
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
        transfer_identity(topology, peers, observed, transfer_phase).await?;
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
    transfer_phase: ObjectTransferPhase,
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
            let PathTransferSelection::Selected(selected) = select_path_for_transfer(
                &candidates,
                quorum(old.len())?,
                old.len(),
                transfer_phase,
            )?
            else {
                return Ok(());
            };
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
            if let ReceiptTransferSelection::Exact(receipt) =
                select_receipt_for_transfer(&candidates, quorum(old.len())?, transfer_phase)?
            {
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
    phase: ObjectTransferPhase,
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
    let PathTransferSelection::Selected(selected) =
        select_path_for_transfer(&candidates, quorum(old.len())?, old.len(), phase)?
    else {
        return Ok(());
    };
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

fn select_receipt_for_transfer(
    observed: &[Option<ObjectMutation>],
    required: usize,
    phase: ObjectTransferPhase,
) -> Result<ReceiptTransferSelection, Status> {
    match select_receipt_quorum(observed, required) {
        Ok(Some(receipt)) => Ok(ReceiptTransferSelection::Exact(receipt)),
        Ok(None) => Ok(ReceiptTransferSelection::Absent),
        Err(error)
            if matches!(phase, ObjectTransferPhase::Precopy)
                && error.code() == tonic::Code::Unavailable =>
        {
            // Pre-copy pages are independent observations while writes
            // continue, not one absence/quorum proof. Install nothing; the
            // mandatory fresh scan after all drains must prove exact quorum.
            Ok(ReceiptTransferSelection::Deferred)
        }
        Err(error) => Err(error),
    }
}

fn select_path_for_transfer(
    observed: &[Option<ObjectPathSnapshot>],
    required: usize,
    replica_count: usize,
    phase: ObjectTransferPhase,
) -> Result<PathTransferSelection, Status> {
    match select_handoff_path_quorum(observed, required, replica_count) {
        Ok(selected) => Ok(PathTransferSelection::Selected(selected)),
        Err(error)
            if matches!(phase, ObjectTransferPhase::Precopy)
                && error.code() == tonic::Code::Unavailable =>
        {
            // Independent observations while origins still admit mutations do
            // not establish authority. Install nothing; the drained final
            // scan and replay must select from fresh authoritative snapshots.
            Ok(PathTransferSelection::Deferred)
        }
        Err(error) => Err(error),
    }
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
        BlobRef, Head, MUTATION_STAMP_FORMAT, MutationStamp, OBJECT_MUTATION_FORMAT,
        ObjectVersioning, PlacementLogId, SourceId, Version, VersionId,
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
            format: OBJECT_MUTATION_FORMAT,
            tenant_id: 11,
            bucket_id: 22,
            versioning: ObjectVersioning::Unversioned,
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
    fn receipt_precopy_defers_missing_and_conflicting_quorum_but_final_transfer_rejects() {
        let exact = receipt();
        let mut conflicting = exact.clone();
        conflicting.input_fingerprint = [9; 32];
        for observed in [
            vec![None, Some(exact.clone())],
            vec![Some(exact.clone()), Some(conflicting)],
        ] {
            assert_eq!(
                select_receipt_for_transfer(&observed, 2, ObjectTransferPhase::Precopy).unwrap(),
                ReceiptTransferSelection::Deferred,
                "inconclusive preparation must install no receipt"
            );
            assert_eq!(
                select_receipt_for_transfer(&observed, 2, ObjectTransferPhase::Authoritative)
                    .unwrap_err()
                    .code(),
                Code::Unavailable,
                "the drained transfer must still require exact old-owner quorum"
            );
        }
    }

    #[test]
    fn receipt_precopy_and_final_transfer_preserve_exact_quorum_and_absence() {
        let exact = receipt();
        for phase in [
            ObjectTransferPhase::Precopy,
            ObjectTransferPhase::Authoritative,
        ] {
            assert_eq!(
                select_receipt_for_transfer(&[Some(exact.clone()), Some(exact.clone())], 2, phase)
                    .unwrap(),
                ReceiptTransferSelection::Exact(exact.clone()),
                "normal exact receipts remain eligible for installation"
            );
            assert_eq!(
                select_receipt_for_transfer(&[None, None], 2, phase).unwrap(),
                ReceiptTransferSelection::Absent
            );
        }
    }

    #[test]
    fn independently_captured_receipt_pages_defer_until_fresh_final_scan() {
        use crate::join_peer::handoff::HandoffEndpoint;

        let mut exact = receipt();
        exact.stamp.mutation_fingerprint = exact.computed_fingerprint();
        exact.validate().unwrap();
        let record = ObjectRecordExport::Receipt(exact.clone());
        let key = record.handoff_order_key().unwrap();
        let endpoint = |id| HandoffEndpoint {
            node_id: NodeId(id),
            address: "unused".into(),
        };
        // Owner 1's page was captured before the concurrent receipt commit;
        // owner 2's independently captured page sees it after exact replication.
        let mut old_pages = [
            MergeSource::<ObjectRecordExport, ObjectRecordCursor>::new(endpoint(1)),
            MergeSource::<ObjectRecordExport, ObjectRecordCursor>::new(endpoint(2)),
        ];
        old_pages[0]
            .install_page(Vec::new(), None, |record| {
                record
                    .handoff_order_key()
                    .map_err(|error| Status::data_loss(error.to_string()))
            })
            .unwrap();
        old_pages[1]
            .install_page(vec![record.clone()], None, |record| {
                record
                    .handoff_order_key()
                    .map_err(|error| Status::data_loss(error.to_string()))
            })
            .unwrap();
        assert_eq!(next_key(&old_pages), Some(key.clone()));
        let candidates = old_pages
            .iter_mut()
            .map(|source| match source.take_if(&key) {
                Some(ObjectRecordExport::Receipt(receipt)) => Some(receipt),
                None => None,
                Some(ObjectRecordExport::ExactPath(_)) => {
                    panic!("receipt key cannot resolve a path")
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(
            select_receipt_for_transfer(&candidates, 2, ObjectTransferPhase::Precopy).unwrap(),
            ReceiptTransferSelection::Deferred
        );
        // The final drained pass starts fresh page streams. Both snapshots
        // contain the exact immutable receipt; no singleton was installed.
        assert_eq!(
            select_receipt_for_transfer(
                &[Some(exact.clone()), Some(exact.clone())],
                2,
                ObjectTransferPhase::Authoritative
            )
            .unwrap(),
            ReceiptTransferSelection::Exact(exact)
        );
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
    fn independently_captured_path_pages_defer_only_before_the_authoritative_scan() {
        use crate::join_peer::handoff::HandoffEndpoint;

        let older = snapshot();
        let mut mutation = receipt();
        mutation.version.id = VersionId(9);
        mutation.stamp.predecessor_version = Some(VersionId(8));
        mutation.stamp.source_journal_position = 9;
        mutation.stamp.mutation_fingerprint = mutation.computed_fingerprint();
        mutation.validate().unwrap();
        let mut newer = older.clone();
        newer.head.version = mutation.version.id;
        newer.head.mutation_stamp = Some(mutation.stamp);
        newer.versions = vec![mutation.version];
        newer.journal_pending_versions = vec![VersionId(9)];
        older.validate().unwrap();
        newer.validate().unwrap();
        let records = [
            ObjectRecordExport::ExactPath(older.clone()),
            ObjectRecordExport::ExactPath(newer.clone()),
        ];
        let key = records[0].handoff_order_key().unwrap();
        assert_eq!(records[1].handoff_order_key().unwrap(), key);
        let mut pages = [
            MergeSource::<ObjectRecordExport, ObjectRecordCursor>::new(HandoffEndpoint {
                node_id: NodeId(1),
                address: "unused".into(),
            }),
            MergeSource::<ObjectRecordExport, ObjectRecordCursor>::new(HandoffEndpoint {
                node_id: NodeId(2),
                address: "unused".into(),
            }),
        ];
        for (page, record) in pages.iter_mut().zip(records) {
            page.install_page(vec![record], None, |record| {
                record
                    .handoff_order_key()
                    .map_err(|error| Status::data_loss(error.to_string()))
            })
            .unwrap();
        }
        let candidates = pages
            .iter_mut()
            .map(|page| match page.take_if(&key) {
                Some(ObjectRecordExport::ExactPath(snapshot)) => Some(snapshot),
                _ => panic!("path identity must resolve to its independently captured snapshot"),
            })
            .collect::<Vec<_>>();
        // Version 9 is a valid successor of 8, not of the old page's 7. The
        // optional scan/replay cannot infer an authority from these pages.
        assert_eq!(
            select_path_for_transfer(&candidates, 2, 2, ObjectTransferPhase::Precopy).unwrap(),
            PathTransferSelection::Deferred
        );
        assert_eq!(
            select_path_for_transfer(&candidates, 2, 2, ObjectTransferPhase::Authoritative)
                .unwrap_err()
                .code(),
            Code::Unavailable
        );
        assert_eq!(
            select_handoff_path_quorum(&candidates, 2, 2)
                .unwrap_err()
                .code(),
            Code::Unavailable
        );
        let fresh = vec![Some(newer.clone()), Some(newer.clone())];
        let exact =
            PathTransferSelection::Selected(Some(release_handoff_retention(newer).unwrap()));
        assert_eq!(
            select_path_for_transfer(&fresh, 2, 2, ObjectTransferPhase::Precopy).unwrap(),
            exact
        );
        assert_eq!(
            select_path_for_transfer(&fresh, 2, 2, ObjectTransferPhase::Authoritative).unwrap(),
            exact
        );
    }

    #[test]
    fn path_transfer_defers_missing_quorum_but_never_malformed_snapshots() {
        let valid = snapshot();
        let missing = [Some(valid.clone()), None];
        assert_eq!(
            select_path_for_transfer(&missing, 2, 2, ObjectTransferPhase::Precopy).unwrap(),
            PathTransferSelection::Deferred
        );
        assert_eq!(
            select_path_for_transfer(&missing, 2, 2, ObjectTransferPhase::Authoritative)
                .unwrap_err()
                .code(),
            Code::Unavailable
        );
        let mut malformed = valid;
        malformed.versions.clear();
        for phase in [
            ObjectTransferPhase::Precopy,
            ObjectTransferPhase::Authoritative,
        ] {
            assert_eq!(
                select_path_for_transfer(
                    &[Some(malformed.clone()), Some(malformed.clone())],
                    2,
                    2,
                    phase
                )
                .unwrap_err()
                .code(),
                Code::DataLoss
            );
            assert_eq!(
                select_path_for_transfer(&[None, None], 2, 2, phase).unwrap(),
                PathTransferSelection::Selected(None)
            );
        }
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
