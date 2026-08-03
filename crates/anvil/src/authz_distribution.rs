//! Tenant-wide Zanzibar coordination over complete realm replicas.
//!
//! Every realm belonging to one stable tenant ID uses the same weighted-HRW
//! replica group. Raft contributes only ACTIVE membership and the serving
//! fence; no realm, revision counter, or ownership decision is stored in it.

use std::sync::Arc;

use anvil_authz::AuthorizationCheck;
use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::{
    AuthzConsistency, AuthzRealmAggregate, AuthzRealmMutation, AuthzRealmMutationContext,
    AuthzRealmSnapshotApplied, AuthzRealmTransferManifest, AuthzRepository, AuthzRevision,
    AuthzScope, AuthzStoreError, BindSchemaRequest, CoordinatedAuthzRealmMutation,
    ReplicaAuthzRealmMutationApplied, TupleBatchRequest,
};
use tonic::Status;

use crate::cluster_placement::ClusterPlacement;
use crate::mutable_record_replica_group::MutableRecordReplicaGroup;
use crate::placement::PlacementKind;
use crate::serving_fence::ServingAuthority;

/// Exact identity and lineage summary for one complete streamed aggregate.
/// The manifest hash covers the canonical aggregate bytes, including the
/// stamp; the copied summary lets a failed quorum distinguish siblings/gaps
/// without accepting either one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AuthzRealmReplicaCandidate {
    pub(crate) manifest: AuthzRealmTransferManifest,
    pub(crate) predecessor_revision: Option<AuthzRevision>,
    pub(crate) mutation_fingerprint: Option<[u8; 32]>,
}

impl AuthzRealmReplicaCandidate {
    pub(crate) fn from_aggregate(
        aggregate: &AuthzRealmAggregate,
        manifest: AuthzRealmTransferManifest,
    ) -> Result<Self, Status> {
        if manifest.scope != aggregate.scope || manifest.revision != aggregate.revision {
            return Err(Status::data_loss(
                "authorization realm manifest disagrees with its aggregate",
            ));
        }
        Ok(Self {
            manifest,
            predecessor_revision: aggregate
                .mutation_stamp
                .and_then(|stamp| stamp.predecessor_revision),
            mutation_fingerprint: aggregate
                .mutation_stamp
                .map(|stamp| stamp.mutation_fingerprint),
        })
    }

    fn validate_for(&self, scope: &AuthzScope) -> Result<(), Status> {
        if self.manifest.scope != *scope || self.manifest.revision == AuthzRevision::ZERO {
            return Err(Status::data_loss(
                "authorization replica returned another realm or a zero revision",
            ));
        }
        match (self.predecessor_revision, self.mutation_fingerprint) {
            (None, None) => Ok(()),
            (predecessor, Some(fingerprint))
                if fingerprint != [0; 32]
                    && predecessor.is_none_or(|revision| {
                        revision != AuthzRevision::ZERO && revision < self.manifest.revision
                    }) =>
            {
                Ok(())
            }
            _ => Err(Status::data_loss(
                "authorization replica returned inconsistent realm lineage",
            )),
        }
    }
}

/// Typed private transport seam. Implementations stream complete aggregates
/// between the named source and target and invoke only the storage kernel's
/// explicitly quorum-reconciled install boundary.
#[tonic::async_trait]
pub(crate) trait AuthzReplicaTransport: Send + Sync + 'static {
    async fn apply_realm_mutation(
        &self,
        target: NodeId,
        address: &str,
        mutation: &AuthzRealmMutation,
    ) -> Result<ReplicaAuthzRealmMutationApplied, Status>;

    async fn read_realm_candidate(
        &self,
        target: NodeId,
        address: &str,
        scope: &AuthzScope,
    ) -> Result<Option<AuthzRealmReplicaCandidate>, Status>;

    async fn install_realm_candidate(
        &self,
        target: NodeId,
        address: &str,
        source: Option<(NodeId, String)>,
        scope: &AuthzScope,
        winner: Option<&AuthzRealmReplicaCandidate>,
    ) -> Result<AuthzRealmSnapshotApplied, Status>;
}

#[derive(Clone, Debug)]
struct ReplicaEndpoint {
    node_id: NodeId,
    address: String,
}

#[derive(Clone, Debug)]
struct TenantReplicaSet {
    group: MutableRecordReplicaGroup,
    endpoints: Vec<ReplicaEndpoint>,
}

impl TenantReplicaSet {
    fn from_placement(placement: &ClusterPlacement, tenant_id: u64) -> Result<Self, Status> {
        if tenant_id == 0 {
            return Err(Status::failed_precondition(
                "stable authorization tenant ID must be non-zero",
            ));
        }
        let group = MutableRecordReplicaGroup::select(
            PlacementKind::ZanzibarRealm,
            placement.cluster_id(),
            &tenant_id.to_be_bytes(),
            placement.placement_nodes(),
        )
        .ok_or_else(|| Status::unavailable("cluster has no authorization replica"))?;
        let endpoints = group
            .replicas()
            .iter()
            .map(|node_id| {
                let address = placement.address(*node_id).ok_or_else(|| {
                    Status::unavailable(format!(
                        "ACTIVE authorization node {} has no peer address",
                        node_id.0
                    ))
                })?;
                Ok(ReplicaEndpoint {
                    node_id: *node_id,
                    address: address.0.clone(),
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        Ok(Self { group, endpoints })
    }
}

#[derive(Clone)]
struct AuthzDistributionCore {
    local_node: NodeId,
    repository: AuthzRepository,
    peers: Arc<dyn AuthzReplicaTransport>,
    /// Routing and fencing provide one active tenant-group coordinator. This
    /// single local gate keeps its reconcile/repair/mutate sequences ordered
    /// without a per-tenant registry or distributed lock.
    coordinator_serial: Arc<tokio::sync::Mutex<()>>,
}

impl AuthzDistributionCore {
    async fn replicate(
        &self,
        replicas: &TenantReplicaSet,
        scope: &AuthzScope,
        coordinated: &CoordinatedAuthzRealmMutation,
    ) -> Result<(), Status> {
        let Some(mutation) = coordinated.mutation.as_ref() else {
            self.reconcile(replicas, scope).await?;
            return Ok(());
        };
        let mut durable = vec![self.local_node];
        let mut tasks = tokio::task::JoinSet::new();
        for endpoint in replicas
            .endpoints
            .iter()
            .filter(|endpoint| endpoint.node_id != self.local_node)
            .cloned()
        {
            let peers = self.peers.clone();
            let mutation = mutation.clone();
            tasks.spawn(async move {
                let result = peers
                    .apply_realm_mutation(endpoint.node_id, &endpoint.address, &mutation)
                    .await;
                (endpoint.node_id, result)
            });
        }
        while let Some(joined) = tasks.join_next().await {
            let (node_id, result) = joined.map_err(|error| {
                Status::internal(format!("authorization peer task failed: {error}"))
            })?;
            if matches!(result, Ok(applied) if applied.revision == mutation.revision()) {
                durable.push(node_id);
            }
        }
        if replicas.group.is_acknowledged_by(&durable) {
            Ok(())
        } else {
            Err(Status::unavailable(format!(
                "authorization mutation reached {} of {} required replicas",
                durable.len(),
                replicas.group.required_acknowledgements()
            )))
        }
    }

    async fn reconcile(
        &self,
        replicas: &TenantReplicaSet,
        scope: &AuthzScope,
    ) -> Result<Option<AuthzRealmReplicaCandidate>, Status> {
        let mut tasks = tokio::task::JoinSet::new();
        for endpoint in replicas.endpoints.iter().cloned() {
            let peers = self.peers.clone();
            let scope = scope.clone();
            tasks.spawn(async move {
                let result = peers
                    .read_realm_candidate(endpoint.node_id, &endpoint.address, &scope)
                    .await;
                (endpoint, result)
            });
        }
        let mut observations = Vec::with_capacity(replicas.endpoints.len());
        while let Some(joined) = tasks.join_next().await {
            observations.push(joined.map_err(|error| {
                Status::internal(format!("authorization read task failed: {error}"))
            })?);
        }
        let successful = observations
            .iter()
            .filter_map(|(_, result)| result.as_ref().ok())
            .collect::<Vec<_>>();
        if successful.len() < replicas.group.required_acknowledgements() {
            return Err(Status::unavailable(
                "authorization realm did not reach its read quorum",
            ));
        }
        for candidate in successful.iter().filter_map(|candidate| candidate.as_ref()) {
            candidate.validate_for(scope)?;
        }
        let winner =
            exact_quorum_candidate(&successful, replicas.group.required_acknowledgements())?;
        let source = winner.as_ref().and_then(|winner| {
            observations.iter().find_map(|(endpoint, observed)| {
                (observed.as_ref().ok().and_then(Option::as_ref) == Some(winner))
                    .then(|| (endpoint.node_id, endpoint.address.clone()))
            })
        });

        self.require_local_winner(&observations, source.clone(), scope, winner.as_ref())
            .await?;
        for (endpoint, observed) in observations {
            if endpoint.node_id == self.local_node || observed.as_ref().ok() == Some(&winner) {
                continue;
            }
            if let Err(error) = self
                .peers
                .install_realm_candidate(
                    endpoint.node_id,
                    &endpoint.address,
                    source.clone(),
                    scope,
                    winner.as_ref(),
                )
                .await
            {
                tracing::warn!(
                    node_id = endpoint.node_id.0,
                    %error,
                    "authorization minority read repair did not complete"
                );
            }
        }
        Ok(winner)
    }

    async fn require_local_winner(
        &self,
        observations: &[(
            ReplicaEndpoint,
            Result<Option<AuthzRealmReplicaCandidate>, Status>,
        )],
        source: Option<(NodeId, String)>,
        scope: &AuthzScope,
        winner: Option<&AuthzRealmReplicaCandidate>,
    ) -> Result<(), Status> {
        let local = observations
            .iter()
            .find(|(endpoint, _)| endpoint.node_id == self.local_node)
            .ok_or_else(|| Status::internal("local authorization replica is not selected"))?;
        if local.1.as_ref().ok() == Some(&winner.cloned()) {
            return Ok(());
        }
        self.peers
            .install_realm_candidate(self.local_node, &local.0.address, source, scope, winner)
            .await?;
        let installed = self
            .peers
            .read_realm_candidate(self.local_node, &local.0.address, scope)
            .await?;
        if installed.as_ref() != winner {
            return Err(Status::data_loss(
                "local authorization read repair did not install the quorum winner",
            ));
        }
        Ok(())
    }

    async fn fresh_check(
        &self,
        replicas: &TenantReplicaSet,
        scope: AuthzScope,
        consistency: AuthzConsistency,
        check: AuthorizationCheck,
    ) -> Result<(bool, AuthzRevision), Status> {
        if self.reconcile(replicas, &scope).await?.is_none() {
            return Err(Status::failed_precondition(
                "authorization realm has no schema binding",
            ));
        }
        let repository = self.repository.clone();
        tokio::task::spawn_blocking(move || repository.check(&scope, consistency, &check))
            .await
            .map_err(|error| Status::internal(format!("authorization worker failed: {error}")))?
            .map_err(authz_status)
    }
}

/// Production placement/fence wrapper. Public service routing is deliberately
/// not installed until system-tenant identity and protected-owner semantics
/// are resolved.
#[derive(Clone)]
pub(crate) struct ZanzibarDistribution {
    local_node: NodeId,
    decisions: DecisionRaft,
    serving: ServingAuthority,
    core: AuthzDistributionCore,
}

impl ZanzibarDistribution {
    pub(crate) fn new(
        local_node: NodeId,
        repository: AuthzRepository,
        decisions: DecisionRaft,
        serving: ServingAuthority,
        peers: Arc<dyn AuthzReplicaTransport>,
    ) -> Self {
        Self {
            local_node,
            decisions,
            serving,
            core: AuthzDistributionCore {
                local_node,
                repository,
                peers,
                coordinator_serial: Arc::new(tokio::sync::Mutex::new(())),
            },
        }
    }

    pub(crate) async fn bind_schema(
        &self,
        stable_tenant_id: u64,
        request: BindSchemaRequest,
        context: AuthzRealmMutationContext,
    ) -> Result<CoordinatedAuthzRealmMutation, Status> {
        let _serial = self.core.coordinator_serial.lock().await;
        let mut replicas = self.require_coordinator(stable_tenant_id)?;
        self.require_context(&context, replicas.group.coordinator())?;
        let scope = request.scope.clone();
        self.core.reconcile(&replicas, &scope).await?;
        replicas = self.require_coordinator(stable_tenant_id)?;
        self.require_context(&context, replicas.group.coordinator())?;
        let repository = self.core.repository.clone();
        let coordinated = tokio::task::spawn_blocking(move || {
            repository.coordinate_bind_schema_mutation(request, context)
        })
        .await
        .map_err(|error| Status::internal(format!("authorization worker failed: {error}")))?
        .map_err(authz_status)?;
        self.core.replicate(&replicas, &scope, &coordinated).await?;
        Ok(coordinated)
    }

    pub(crate) async fn mutate_tuples(
        &self,
        stable_tenant_id: u64,
        request: TupleBatchRequest,
        context: AuthzRealmMutationContext,
    ) -> Result<CoordinatedAuthzRealmMutation, Status> {
        let _serial = self.core.coordinator_serial.lock().await;
        let mut replicas = self.require_coordinator(stable_tenant_id)?;
        self.require_context(&context, replicas.group.coordinator())?;
        let scope = request.scope.clone();
        self.core.reconcile(&replicas, &scope).await?;
        replicas = self.require_coordinator(stable_tenant_id)?;
        self.require_context(&context, replicas.group.coordinator())?;
        let repository = self.core.repository.clone();
        let coordinated = tokio::task::spawn_blocking(move || {
            repository.coordinate_tuple_mutation(request, context)
        })
        .await
        .map_err(|error| Status::internal(format!("authorization worker failed: {error}")))?
        .map_err(authz_status)?;
        self.core.replicate(&replicas, &scope, &coordinated).await?;
        Ok(coordinated)
    }

    pub(crate) async fn fresh_check(
        &self,
        stable_tenant_id: u64,
        scope: AuthzScope,
        consistency: AuthzConsistency,
        check: AuthorizationCheck,
    ) -> Result<(bool, AuthzRevision), Status> {
        let _serial = self.core.coordinator_serial.lock().await;
        let replicas = self.require_coordinator(stable_tenant_id)?;
        self.serving.mutation_context()?;
        self.core
            .fresh_check(&replicas, scope, consistency, check)
            .await
    }

    fn require_coordinator(&self, stable_tenant_id: u64) -> Result<TenantReplicaSet, Status> {
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
        let placement = ClusterPlacement::from_applied(&state)
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let replicas = TenantReplicaSet::from_placement(&placement, stable_tenant_id)?;
        if replicas.group.coordinator() != self.local_node {
            return Err(Status::failed_precondition(format!(
                "authorization tenant is coordinated by node {}",
                replicas.group.coordinator().0
            )));
        }
        Ok(replicas)
    }

    fn require_context(
        &self,
        context: &AuthzRealmMutationContext,
        coordinator: NodeId,
    ) -> Result<(), Status> {
        let current = self.serving.mutation_context()?;
        if coordinator != self.local_node
            || context.active_placement_log_id != current.active_placement_log_id
            || context.serving_fence_term != current.serving_fence_term
            || u64::from(context.source_id.node_id) != self.local_node.0
        {
            return Err(Status::unavailable(
                "authorization mutation context does not match the current coordinator fence",
            ));
        }
        Ok(())
    }
}

fn exact_quorum_candidate(
    observed: &[&Option<AuthzRealmReplicaCandidate>],
    required: usize,
) -> Result<Option<AuthzRealmReplicaCandidate>, Status> {
    for candidate in observed {
        if observed.iter().filter(|other| *other == candidate).count() >= required {
            return Ok((*candidate).clone());
        }
    }
    let present = observed
        .iter()
        .filter_map(|candidate| candidate.as_ref())
        .collect::<Vec<_>>();
    let sibling = present.iter().enumerate().any(|(index, left)| {
        present[index + 1..].iter().any(|right| {
            left.manifest.revision == right.manifest.revision
                && left.mutation_fingerprint != right.mutation_fingerprint
        })
    });
    let reason = if sibling { "sibling" } else { "lineage gap" };
    Err(Status::unavailable(format!(
        "authorization realm has no exact read quorum ({reason})"
    )))
}

fn authz_status(error: AuthzStoreError) -> Status {
    match error {
        AuthzStoreError::ReceiptCapacity | AuthzStoreError::RevisionNotAvailable { .. } => {
            Status::unavailable(error.to_string())
        }
        AuthzStoreError::Storage(_) => Status::internal(error.to_string()),
        _ => Status::failed_precondition(error.to_string()),
    }
}

#[cfg(test)]
mod tests;
