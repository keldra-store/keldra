//! Complete-record quorum coordination for mutable non-object metadata.
//!
//! Raft contributes only the current ACTIVE membership and serving fence.
//! Exact logical records remain typed RocksDB values replicated to the first
//! three weighted-HRW ranks; no record ownership is persisted in Raft.

use std::sync::Arc;

use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::{
    LogicalRecordApplied, LogicalRecordCandidate, LogicalRecordError, LogicalRecordExport,
    LogicalRecordId, LogicalRecordMutation, LogicalRecordMutationContext, LogicalRecordPredecessor,
    LogicalRecordSnapshotApplied, LogicalRecordValue, Store, VersionId,
};
use tonic::Status;

use crate::cluster_placement::ClusterPlacement;
use crate::mutable_record_replica_group::MutableRecordReplicaGroup;
use crate::placement::PlacementKind;
use crate::serving_fence::ServingAuthority;

/// Typed private transport seam. Peers expose logical records, never raw
/// column-family keys or values.
#[tonic::async_trait]
pub(crate) trait LogicalRecordReplicaTransport: Send + Sync + 'static {
    async fn read_candidate(
        &self,
        target: NodeId,
        address: &str,
        id: &LogicalRecordId,
    ) -> Result<Option<LogicalRecordCandidate>, Status>;

    async fn repair_candidate(
        &self,
        target: NodeId,
        address: &str,
        id: &LogicalRecordId,
        candidate: Option<&LogicalRecordCandidate>,
    ) -> Result<LogicalRecordSnapshotApplied, Status>;

    async fn apply_mutation(
        &self,
        target: NodeId,
        address: &str,
        mutation: &LogicalRecordMutation,
    ) -> Result<LogicalRecordApplied, Status>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReplicaEndpoint {
    node_id: NodeId,
    address: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LogicalRecordRoute {
    group: MutableRecordReplicaGroup,
    endpoints: Vec<ReplicaEndpoint>,
    active_placement_log_id: anvil_store::PlacementLogId,
    serving_fence_term: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LogicalRecordReadTarget {
    pub(crate) node_id: NodeId,
    pub(crate) address: String,
    pub(crate) placement_fence: anvil_store::PlacementLogId,
}

#[derive(Clone)]
struct LogicalRecordDistributionCore {
    local_node: NodeId,
    store: Store,
    peers: Arc<dyn LogicalRecordReplicaTransport>,
    /// Routing provides one coordinator per record. A single local gate is
    /// sufficient to serialize every reconcile/mutate sequence handled by
    /// this process without a per-record lock registry.
    coordinator_serial: Arc<tokio::sync::Mutex<()>>,
    mutation_admission: crate::mutation_admission::MutationAdmission,
}

impl LogicalRecordDistributionCore {
    async fn coordinate<F>(
        &self,
        route: &LogicalRecordRoute,
        typed_value: LogicalRecordValue,
        mut require_current_fence: F,
    ) -> Result<LogicalRecordApplied, Status>
    where
        F: FnMut() -> Result<(), Status> + Send,
    {
        let _serial = self.coordinator_serial.lock().await;
        let _permit = self.mutation_admission.enter()?;
        require_current_fence()?;
        let id = typed_value.id();
        let current = self.reconcile(route, &id).await?;
        require_current_fence()?;
        if let Some(LogicalRecordCandidate::Versioned(existing)) = current
            && existing.typed_value == typed_value
        {
            return Ok(LogicalRecordApplied {
                record_version: existing.record_version,
                replayed: true,
            });
        }

        let record_version = self
            .store
            .allocate_logical_record_version()
            .map_err(logical_record_status)?;
        let mutation = self
            .store
            .construct_logical_record_mutation(
                typed_value,
                LogicalRecordMutationContext {
                    record_version,
                    active_placement_log_id: route.active_placement_log_id,
                    serving_fence_term: route.serving_fence_term,
                },
            )
            .map_err(logical_record_status)?;
        let applied = self.replicate(route, &mutation).await?;
        require_current_fence()?;
        Ok(applied)
    }

    async fn reconcile(
        &self,
        route: &LogicalRecordRoute,
        id: &LogicalRecordId,
    ) -> Result<Option<LogicalRecordCandidate>, Status> {
        let local = self
            .store
            .logical_record_candidate(id)
            .map_err(logical_record_status)?;
        let mut observations = vec![(self.local_node, Ok(local))];
        let mut tasks = tokio::task::JoinSet::new();
        for endpoint in route
            .endpoints
            .iter()
            .filter(|endpoint| endpoint.node_id != self.local_node)
            .cloned()
        {
            let peers = self.peers.clone();
            let id = id.clone();
            tasks.spawn(async move {
                let result = peers
                    .read_candidate(endpoint.node_id, &endpoint.address, &id)
                    .await;
                (endpoint.node_id, result)
            });
        }
        while let Some(joined) = tasks.join_next().await {
            observations.push(joined.map_err(|error| {
                Status::internal(format!("logical-record read task failed: {error}"))
            })?);
        }

        let successful = observations
            .iter()
            .filter_map(|(_, result)| result.as_ref().ok())
            .collect::<Vec<_>>();
        if successful.len() < route.group.required_acknowledgements() {
            return Err(Status::unavailable(
                "logical record did not reach its read quorum",
            ));
        }
        for candidate in successful.iter().filter_map(|candidate| candidate.as_ref()) {
            LogicalRecordExport {
                id: id.clone(),
                candidate: candidate.clone(),
            }
            .validate()
            .map_err(logical_record_status)?;
        }
        let winner = highest_valid_candidate(&successful, route.group.required_acknowledgements())?;
        let mut durable = observations
            .iter()
            .filter_map(|(node, result)| (result.as_ref().ok() == Some(&winner)).then_some(*node))
            .collect::<Vec<_>>();

        if observations
            .iter()
            .find(|(node, _)| *node == self.local_node)
            .and_then(|(_, result)| result.as_ref().ok())
            != Some(&winner)
        {
            self.store
                .repair_quorum_reconciled_logical_record(id, winner.as_ref())
                .map_err(logical_record_status)?;
            durable.push(self.local_node);
        }
        let installed = self
            .store
            .logical_record_candidate(id)
            .map_err(logical_record_status)?;
        if installed != winner {
            return Err(Status::data_loss(
                "local logical-record repair did not install the quorum winner",
            ));
        }

        let mut repairs = tokio::task::JoinSet::new();
        for endpoint in route
            .endpoints
            .iter()
            .filter(|endpoint| endpoint.node_id != self.local_node)
            .cloned()
        {
            let observed = observations
                .iter()
                .find(|(node, _)| *node == endpoint.node_id)
                .and_then(|(_, result)| result.as_ref().ok());
            if observed == Some(&winner) {
                continue;
            }
            let peers = self.peers.clone();
            let id = id.clone();
            let winner = winner.clone();
            repairs.spawn(async move {
                let result = peers
                    .repair_candidate(endpoint.node_id, &endpoint.address, &id, winner.as_ref())
                    .await;
                (endpoint.node_id, result)
            });
        }
        while let Some(joined) = repairs.join_next().await {
            let (node_id, result) = joined.map_err(|error| {
                Status::internal(format!("logical-record repair task failed: {error}"))
            })?;
            match result {
                Ok(applied) if applied.record_version == candidate_version(winner.as_ref()) => {
                    durable.push(node_id);
                }
                Ok(_) => {
                    tracing::warn!(
                        node_id = node_id.0,
                        "logical-record minority repair returned another version"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        node_id = node_id.0,
                        %error,
                        "logical-record minority repair did not complete"
                    );
                }
            }
        }
        if !route.group.is_acknowledged_by(&durable) {
            return Err(Status::unavailable(format!(
                "logical-record recovery reached {} of {} required replicas",
                durable.len(),
                route.group.required_acknowledgements()
            )));
        }
        Ok(winner)
    }

    async fn replicate(
        &self,
        route: &LogicalRecordRoute,
        mutation: &LogicalRecordMutation,
    ) -> Result<LogicalRecordApplied, Status> {
        let local = self
            .store
            .apply_logical_record_mutation_journaled(mutation)
            .await
            .map_err(logical_record_status)?;
        if local.record_version != mutation.record_version {
            return Err(Status::data_loss(
                "local logical-record replica returned another version",
            ));
        }
        let mut durable = vec![self.local_node];
        let mut tasks = tokio::task::JoinSet::new();
        for endpoint in route
            .endpoints
            .iter()
            .filter(|endpoint| endpoint.node_id != self.local_node)
            .cloned()
        {
            let peers = self.peers.clone();
            let mutation = mutation.clone();
            tasks.spawn(async move {
                let result = peers
                    .apply_mutation(endpoint.node_id, &endpoint.address, &mutation)
                    .await;
                (endpoint.node_id, result)
            });
        }
        while let Some(joined) = tasks.join_next().await {
            let (node_id, result) = joined.map_err(|error| {
                Status::internal(format!("logical-record replica task failed: {error}"))
            })?;
            if matches!(result, Ok(applied) if applied.record_version == mutation.record_version) {
                durable.push(node_id);
            }
        }
        if !route.group.is_acknowledged_by(&durable) {
            return Err(Status::unavailable(format!(
                "logical record reached {} of {} required replicas",
                durable.len(),
                route.group.required_acknowledgements()
            )));
        }
        Ok(local)
    }
}

/// Production wrapper that derives every route from the currently applied
/// Raft membership and accepts work only while the exact serving fence remains
/// current.
#[derive(Clone)]
pub(crate) struct LogicalRecordDistribution {
    local_node: NodeId,
    decisions: DecisionRaft,
    serving: ServingAuthority,
    core: LogicalRecordDistributionCore,
}

impl LogicalRecordDistribution {
    pub(crate) fn new(
        local_node: NodeId,
        store: Store,
        decisions: DecisionRaft,
        serving: ServingAuthority,
        peers: Arc<dyn LogicalRecordReplicaTransport>,
        mutation_admission: crate::mutation_admission::MutationAdmission,
    ) -> Self {
        Self {
            local_node,
            decisions,
            serving,
            core: LogicalRecordDistributionCore {
                local_node,
                store,
                peers,
                coordinator_serial: Arc::new(tokio::sync::Mutex::new(())),
                mutation_admission,
            },
        }
    }

    pub(crate) async fn mutate(
        &self,
        typed_value: LogicalRecordValue,
    ) -> Result<LogicalRecordApplied, Status> {
        let id = typed_value.id();
        let route = self.route(&id)?;
        if route.group.coordinator() != self.local_node {
            return Err(Status::failed_precondition(format!(
                "logical record is coordinated by node {}",
                route.group.coordinator().0
            )));
        }
        self.core
            .coordinate(&route, typed_value, || {
                self.require_current_route(&id, &route)
            })
            .await
    }

    /// Reconcile one complete logical record at its current HRW coordinator.
    /// No replica-local or cached value is exposed through this boundary.
    pub(crate) async fn read(
        &self,
        id: &LogicalRecordId,
    ) -> Result<Option<LogicalRecordValue>, Status> {
        let _serial = self.core.coordinator_serial.lock().await;
        let route = self.route(id)?;
        if route.group.coordinator() != self.local_node {
            return Err(Status::failed_precondition(format!(
                "logical record is coordinated by node {}",
                route.group.coordinator().0
            )));
        }
        self.require_current_route(id, &route)?;
        let candidate = self.core.reconcile(&route, id).await?;
        self.require_current_route(id, &route)?;
        Ok(candidate.map(|candidate| candidate.typed_value().clone()))
    }

    pub(crate) fn read_target(
        &self,
        id: &LogicalRecordId,
    ) -> Result<Option<LogicalRecordReadTarget>, Status> {
        let route = self.route(id)?;
        let coordinator = route.group.coordinator();
        if coordinator == self.local_node {
            return Ok(None);
        }
        let endpoint = route
            .endpoints
            .iter()
            .find(|endpoint| endpoint.node_id == coordinator)
            .ok_or_else(|| Status::unavailable("logical-record coordinator has no endpoint"))?;
        Ok(Some(LogicalRecordReadTarget {
            node_id: coordinator,
            address: endpoint.address.clone(),
            placement_fence: route.active_placement_log_id,
        }))
    }

    pub(crate) fn require_read_target(
        &self,
        id: &LogicalRecordId,
        expected: &LogicalRecordReadTarget,
    ) -> Result<(), Status> {
        if self.read_target(id)?.as_ref() == Some(expected) {
            Ok(())
        } else {
            Err(Status::unavailable(
                "logical-record placement changed during name resolution",
            ))
        }
    }

    fn require_current_route(
        &self,
        id: &LogicalRecordId,
        expected: &LogicalRecordRoute,
    ) -> Result<(), Status> {
        let current = self.route(id)?;
        if current != *expected || current.group.coordinator() != self.local_node {
            return Err(Status::unavailable(
                "logical-record placement or serving fence changed during coordination",
            ));
        }
        Ok(())
    }

    fn route(&self, id: &LogicalRecordId) -> Result<LogicalRecordRoute, Status> {
        // The local lookup validates every typed identity before it can be
        // hashed or sent to a peer.
        self.core
            .store
            .logical_record_candidate(id)
            .map_err(logical_record_status)?;
        let serving = self.serving.mutation_context()?;
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
        let placement = ClusterPlacement::from_applied(&state)
            .map_err(|error| Status::unavailable(error.to_string()))?;
        if placement.fence() != serving.active_placement_log_id {
            return Err(Status::unavailable(
                "serving lease does not cover the applied placement",
            ));
        }
        let (kind, key) = placement_key(id)?;
        let group = MutableRecordReplicaGroup::select(
            kind,
            placement.cluster_id(),
            &key,
            placement.placement_nodes(),
        )
        .ok_or_else(|| Status::unavailable("cluster has no logical-record replica"))?;
        let endpoints = group
            .replicas()
            .iter()
            .map(|node_id| {
                let address = placement.address(*node_id).ok_or_else(|| {
                    Status::unavailable(format!(
                        "ACTIVE logical-record node {} has no peer address",
                        node_id.0
                    ))
                })?;
                Ok(ReplicaEndpoint {
                    node_id: *node_id,
                    address: address.0.clone(),
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        Ok(LogicalRecordRoute {
            group,
            endpoints,
            active_placement_log_id: serving.active_placement_log_id,
            serving_fence_term: serving.serving_fence_term,
        })
    }
}

fn placement_key(id: &LogicalRecordId) -> Result<(PlacementKind, Vec<u8>), Status> {
    match id {
        LogicalRecordId::TenantNameClaim { storage_tenant } => Ok((
            PlacementKind::TenantNameClaim,
            storage_tenant.as_str().as_bytes().to_vec(),
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
            let mut key = Vec::with_capacity(16);
            key.extend_from_slice(&tenant_id.to_be_bytes());
            key.extend_from_slice(&bucket_id.to_be_bytes());
            Ok((PlacementKind::TenantOrBucketRecord, key))
        }
        LogicalRecordId::Application { app_id } => {
            Ok((PlacementKind::Credential, app_id.as_bytes().to_vec()))
        }
        LogicalRecordId::Credential { client_id } => {
            Ok((PlacementKind::Credential, client_id.as_bytes().to_vec()))
        }
        LogicalRecordId::BucketNameClaim { tenant_id, bucket } => {
            let mut key = Vec::with_capacity(8 + bucket.len());
            key.extend_from_slice(&tenant_id.to_be_bytes());
            key.extend_from_slice(bucket.as_bytes());
            Ok((PlacementKind::TenantOrBucketRecord, key))
        }
        LogicalRecordId::TenantSchema { .. } => Err(Status::unimplemented(
            "tenant schemas belong to the tenant-wide Zanzibar replica group",
        )),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CandidateIdentity {
    Absent,
    Baseline(anvil_store::BaselineHash),
    Version(VersionId),
}

#[derive(Clone)]
struct CandidateEvidence {
    candidate: Option<LogicalRecordCandidate>,
    identity: CandidateIdentity,
    predecessor: Option<CandidateIdentity>,
    observations: usize,
}

/// Reconcile the current values read from a replica quorum.
///
/// One exact quorum is sufficient evidence for a current state, but is not an
/// excuse to discard a directly linked successor whose response may have been
/// lost. Without a quorum-proven anchor, divergent states must form one
/// predecessor-linked chain. An anchor may repair lower stale states, while a
/// higher candidate still needs its predecessor link. Siblings are resolvable
/// only when exactly one sibling itself has quorum evidence; missing links and
/// ambiguous branches fail closed.
pub(crate) fn highest_valid_candidate(
    observed: &[&Option<LogicalRecordCandidate>],
    required: usize,
) -> Result<Option<LogicalRecordCandidate>, Status> {
    if required == 0 || observed.len() < required {
        return Err(Status::unavailable(
            "logical record did not provide its required read quorum",
        ));
    }

    let mut evidence = Vec::<CandidateEvidence>::new();
    for candidate in observed {
        if let Some(existing) = evidence
            .iter_mut()
            .find(|existing| &existing.candidate == *candidate)
        {
            existing.observations += 1;
            continue;
        }
        evidence.push(CandidateEvidence {
            candidate: (**candidate).clone(),
            identity: candidate_identity(candidate),
            predecessor: candidate_predecessor(candidate),
            observations: 1,
        });
    }

    for (index, candidate) in evidence.iter().enumerate() {
        if evidence[..index].iter().any(|earlier| {
            earlier.identity == candidate.identity && earlier.candidate != candidate.candidate
        }) {
            return Err(Status::unavailable(
                "one logical-record identity has contradictory canonical values",
            ));
        }
    }

    let mut discarded = vec![false; evidence.len()];
    let mut handled_predecessors = Vec::<CandidateIdentity>::new();
    for candidate in &evidence {
        let Some(predecessor) = candidate.predecessor else {
            continue;
        };
        if handled_predecessors.contains(&predecessor) {
            continue;
        }
        handled_predecessors.push(predecessor);
        let siblings = evidence
            .iter()
            .enumerate()
            .filter_map(|(index, candidate)| {
                (candidate.predecessor == Some(predecessor)).then_some(index)
            })
            .collect::<Vec<_>>();
        if siblings.len() <= 1 {
            continue;
        }
        let proven = siblings
            .iter()
            .copied()
            .filter(|index| evidence[*index].observations >= required)
            .collect::<Vec<_>>();
        if proven.len() != 1 {
            return Err(Status::unavailable(
                "logical record has contradictory siblings without one quorum-proven successor",
            ));
        }
        for sibling in siblings {
            if sibling != proven[0] {
                discarded[sibling] = true;
            }
        }
    }

    let retained = (0..evidence.len())
        .filter(|index| !discarded[*index])
        .collect::<Vec<_>>();
    let proven = retained
        .iter()
        .copied()
        .filter(|index| evidence[*index].observations >= required)
        .collect::<Vec<_>>();
    if proven.len() > 1 {
        return Err(Status::unavailable(
            "logical record has contradictory quorum-proven states",
        ));
    }
    if let Some(anchor) = proven.first().copied() {
        return select_from_quorum_candidate(&evidence, &retained, anchor);
    }
    select_linked_candidate_chain(&evidence, &retained)
}

fn select_from_quorum_candidate(
    evidence: &[CandidateEvidence],
    retained: &[usize],
    anchor: usize,
) -> Result<Option<LogicalRecordCandidate>, Status> {
    let mut selected = vec![anchor];
    let mut current = anchor;
    loop {
        let children = retained
            .iter()
            .copied()
            .filter(|candidate| {
                evidence[*candidate].predecessor == Some(evidence[current].identity)
            })
            .collect::<Vec<_>>();
        match children.as_slice() {
            [] => break,
            [child] => {
                current = *child;
                selected.push(*child);
            }
            _ => {
                return Err(Status::unavailable(
                    "logical record has an unresolved predecessor branch",
                ));
            }
        }
    }
    for candidate in retained
        .iter()
        .copied()
        .filter(|candidate| !selected.contains(candidate))
    {
        if !candidate_is_older(evidence[candidate].identity, evidence[anchor].identity) {
            return Err(Status::unavailable(
                "logical record has a higher state with missing predecessor evidence",
            ));
        }
    }
    Ok(evidence[current].candidate.clone())
}

fn select_linked_candidate_chain(
    evidence: &[CandidateEvidence],
    retained: &[usize],
) -> Result<Option<LogicalRecordCandidate>, Status> {
    let roots = retained
        .iter()
        .copied()
        .filter(|index| {
            evidence[*index].predecessor.is_none_or(|predecessor| {
                !retained
                    .iter()
                    .any(|parent| evidence[*parent].identity == predecessor)
            })
        })
        .collect::<Vec<_>>();
    if roots.len() != 1 {
        return Err(Status::unavailable(
            "logical record has missing predecessor evidence or disconnected states",
        ));
    }

    let mut current = roots[0];
    let mut visited = 1_usize;
    loop {
        let children = retained
            .iter()
            .copied()
            .filter(|candidate| {
                evidence[*candidate].predecessor == Some(evidence[current].identity)
            })
            .collect::<Vec<_>>();
        match children.as_slice() {
            [] => break,
            [child] => {
                current = *child;
                visited += 1;
            }
            _ => {
                return Err(Status::unavailable(
                    "logical record has an unresolved predecessor branch",
                ));
            }
        }
    }
    if visited != retained.len() {
        return Err(Status::unavailable(
            "logical record has missing predecessor evidence or disconnected states",
        ));
    }
    Ok(evidence[current].candidate.clone())
}

fn candidate_is_older(candidate: CandidateIdentity, anchor: CandidateIdentity) -> bool {
    match (candidate, anchor) {
        (CandidateIdentity::Absent, CandidateIdentity::Baseline(_))
        | (
            CandidateIdentity::Absent | CandidateIdentity::Baseline(_),
            CandidateIdentity::Version(_),
        ) => true,
        (CandidateIdentity::Version(candidate), CandidateIdentity::Version(anchor)) => {
            candidate < anchor
        }
        _ => false,
    }
}

fn candidate_identity(candidate: &Option<LogicalRecordCandidate>) -> CandidateIdentity {
    match candidate {
        None => CandidateIdentity::Absent,
        Some(LogicalRecordCandidate::Baseline { baseline_hash, .. }) => {
            CandidateIdentity::Baseline(*baseline_hash)
        }
        Some(LogicalRecordCandidate::Versioned(mutation)) => {
            CandidateIdentity::Version(mutation.record_version)
        }
    }
}

fn candidate_predecessor(candidate: &Option<LogicalRecordCandidate>) -> Option<CandidateIdentity> {
    let Some(LogicalRecordCandidate::Versioned(mutation)) = candidate else {
        return None;
    };
    Some(match mutation.predecessor {
        LogicalRecordPredecessor::Absent => CandidateIdentity::Absent,
        LogicalRecordPredecessor::BaselineHash(hash) => CandidateIdentity::Baseline(hash),
        LogicalRecordPredecessor::VersionId(version) => CandidateIdentity::Version(version),
    })
}

fn candidate_version(candidate: Option<&LogicalRecordCandidate>) -> Option<VersionId> {
    match candidate {
        Some(LogicalRecordCandidate::Versioned(mutation)) => Some(mutation.record_version),
        None | Some(LogicalRecordCandidate::Baseline { .. }) => None,
    }
}

fn logical_record_status(error: LogicalRecordError) -> Status {
    match error {
        LogicalRecordError::Storage(_) => Status::internal(error.to_string()),
        LogicalRecordError::Tampered => Status::data_loss(error.to_string()),
        LogicalRecordError::LineageGap
        | LogicalRecordError::Sibling
        | LogicalRecordError::SnapshotConflict => Status::unavailable(error.to_string()),
        _ => Status::failed_precondition(error.to_string()),
    }
}

#[cfg(test)]
mod tests;
