use std::collections::BTreeMap;

use anvil_consensus::NodeId;
use anvil_store::{AuthzRealmCursor, AuthzScope};
use tonic::Status;

use super::{logical, quorum};
use crate::authz_distribution::{AuthzRealmReplicaCandidate, exact_quorum_candidate};
use crate::data_peer::DataPeerTransport;
use crate::join_peer::handoff::HandoffTopology;
use crate::join_peer::handoff::merge::{MergeSource, next_key};
use crate::placement::PlacementKind;

pub(super) async fn transfer(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
) -> Result<(), Status> {
    let mut sources = topology
        .discovery_endpoints()
        .cloned()
        .map(MergeSource::<AuthzScope, AuthzRealmCursor>::new)
        .collect::<Vec<_>>();
    loop {
        refill(&mut sources, peers).await?;
        let Some(key) = next_key(&sources) else {
            return Ok(());
        };
        let mut observed = BTreeMap::new();
        for source in &mut sources {
            if let Some(scope) = source.take_if(&key) {
                observed.insert(source.node_id(), scope);
            }
        }
        transfer_scope(topology, peers, observed).await?;
    }
}

async fn refill(
    sources: &mut [MergeSource<AuthzScope, AuthzRealmCursor>],
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
            .export_authz_realm_keys(node, &address, cursor.as_ref())
            .await?;
        source.install_page(page.scopes, page.next_cursor, |scope| {
            scope
                .handoff_order_key()
                .map_err(|error| Status::data_loss(error.to_string()))
        })?;
    }
    Ok(())
}

async fn transfer_scope(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
    observed: BTreeMap<NodeId, AuthzScope>,
) -> Result<(), Status> {
    let scope = observed
        .values()
        .next()
        .cloned()
        .ok_or_else(|| Status::data_loss("authorization handoff scope has no observation"))?;
    if observed.values().any(|candidate| candidate != &scope) {
        return Err(Status::data_loss(
            "authorization handoff order key identifies contradictory realms",
        ));
    }
    let tenant_id = logical::resolve_tenant_id(topology, peers, &scope.storage_tenant).await?;
    let key = tenant_id.to_be_bytes();
    let old = topology.old_replicas(PlacementKind::ZanzibarRealm, &key);
    let new = topology.new_replicas(PlacementKind::ZanzibarRealm, &key);
    let mut candidates = Vec::with_capacity(old.len());
    for node in &old {
        let address = topology
            .address(*node)
            .ok_or_else(|| Status::data_loss("authorization replica has no peer address"))?;
        candidates.push(read_candidate(peers, *node, address, &scope).await?);
    }
    let observed_candidates = candidates.iter().collect::<Vec<_>>();
    let selected = exact_quorum_candidate(&observed_candidates, quorum(old.len())?)?;
    if !new.contains(&topology.joining().node_id) {
        return Ok(());
    }

    let joining = topology.joining();
    let current = read_candidate(peers, joining.node_id, &joining.address, &scope).await?;
    if current == selected {
        return Ok(());
    }
    let Some(selected) = selected else {
        peers
            .repair_authz_realm_absence(joining.node_id, &joining.address, &scope)
            .await?;
        return Ok(());
    };
    let source = old
        .iter()
        .zip(&candidates)
        .find_map(|(node, candidate)| (candidate.as_ref() == Some(&selected)).then_some(*node))
        .ok_or_else(|| Status::data_loss("authorization quorum winner has no source"))?;
    let source_address = topology
        .address(source)
        .ok_or_else(|| Status::data_loss("authorization source has no peer address"))?;
    peers
        .copy_authz_realm(
            source,
            source_address,
            joining.node_id,
            &joining.address,
            &scope,
            &selected.manifest,
        )
        .await?;
    Ok(())
}

async fn read_candidate(
    peers: &DataPeerTransport,
    node: NodeId,
    address: &str,
    scope: &AuthzScope,
) -> Result<Option<AuthzRealmReplicaCandidate>, Status> {
    peers
        .authz_realm_manifest(node, address, scope)
        .await?
        .map(AuthzRealmReplicaCandidate::from_manifest)
        .transpose()
}
