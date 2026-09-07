//! Tenant-wide Zanzibar coordination over complete realm replicas.
//!
//! Every realm belonging to one stable tenant ID uses the same weighted-HRW
//! replica group. Raft contributes only ACTIVE membership and the serving
//! fence; no realm, revision counter, or ownership decision is stored in it.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;

use keldra_authz::{AuthorizationCheck, Schema};
use keldra_consensus::{DecisionRaft, NodeId};
use keldra_store::{
    AUTHZ_REALM_STATE_FORMAT, AUTHZ_REALM_TRANSFER_MANIFEST_FORMAT, AuthzConsistency,
    AuthzRealmMutation, AuthzRealmSnapshotApplied, AuthzRealmState, AuthzRealmTransferManifest,
    AuthzRepository, AuthzRevision, AuthzSchemaPublicationMutation, AuthzScope, AuthzStoreError,
    BindSchemaRequest, CoordinatedAuthzRealmMutation, CoordinatedAuthzSchemaPublication,
    PlacementLogId, PublishSchemaRequest, ReplicaAuthzRealmMutationApplied,
    ReplicaAuthzSchemaPublicationApplied, SchemaRef, StorageTenantId, Store, TupleBatchRequest,
};
use tonic::Status;

use crate::cluster_placement::ClusterPlacement;
use crate::mutable_record_replica_group::MutableRecordReplicaGroup;
use crate::placement::PlacementKind;
use crate::serving_fence::ServingAuthority;

const AUTHZ_COORDINATOR_LANES: usize = 64;

/// One private, stable copy of a realm stream while it is crossing a peer
/// boundary. The file is removed after the network producer releases it.
pub(crate) struct AuthzTransferSpool {
    file: Option<File>,
    path: PathBuf,
}

impl AuthzTransferSpool {
    pub(crate) fn new() -> io::Result<Self> {
        let directory = std::env::temp_dir();
        for _ in 0..4 {
            let path = directory.join(format!(
                "keldra-authz-transfer-{}-{}",
                std::process::id(),
                uuid::Uuid::new_v4()
            ));
            let mut options = OpenOptions::new();
            options.read(true).write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            match options.open(&path) {
                Ok(file) => {
                    return Ok(Self {
                        file: Some(file),
                        path,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not allocate a unique authorization transfer spool",
        ))
    }

    pub(crate) fn rewind(&mut self) -> io::Result<()> {
        self.file_mut().seek(SeekFrom::Start(0)).map(|_| ())
    }

    fn file_mut(&mut self) -> &mut File {
        self.file
            .as_mut()
            .expect("authorization transfer spool file is present until drop")
    }
}

impl Read for AuthzTransferSpool {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.file_mut().read(buffer)
    }
}

impl Write for AuthzTransferSpool {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.file_mut().write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file_mut().flush()
    }
}

impl Drop for AuthzTransferSpool {
    fn drop(&mut self) {
        drop(self.file.take());
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Exact O(1) identity and lineage summary for one realm replica. Full-stream
/// transfer evidence is attached only after bytes cross a peer boundary.
#[derive(Clone, Debug)]
pub(crate) struct AuthzRealmReplicaCandidate {
    pub(crate) state: AuthzRealmState,
    transfer_manifest: Option<AuthzRealmTransferManifest>,
}

impl PartialEq for AuthzRealmReplicaCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.state == other.state
    }
}

impl Eq for AuthzRealmReplicaCandidate {}

impl AuthzRealmReplicaCandidate {
    #[cfg(test)]
    pub(crate) fn from_aggregate(
        aggregate: &keldra_store::AuthzRealmAggregate,
        manifest: AuthzRealmTransferManifest,
    ) -> Result<Self, Status> {
        let state = AuthzRealmState {
            format: AUTHZ_REALM_STATE_FORMAT,
            scope: aggregate.scope.clone(),
            revision: aggregate.revision,
            predecessor_revision: aggregate
                .mutation_stamp
                .and_then(|stamp| stamp.predecessor_revision),
            mutation_fingerprint: aggregate
                .mutation_stamp
                .map(|stamp| stamp.mutation_fingerprint),
            schema_ref: aggregate.binding.schema_ref.clone(),
            binding_generation: aggregate.binding.generation,
            tuple_count: aggregate.binding.tuple_count,
        };
        let candidate = Self::from_manifest(manifest)?;
        if candidate.state != state {
            return Err(Status::data_loss(
                "authorization realm manifest disagrees with its aggregate",
            ));
        }
        candidate.validate_for(&aggregate.scope)?;
        Ok(candidate)
    }

    pub(crate) fn from_state(state: AuthzRealmState) -> Result<Self, Status> {
        let candidate = Self {
            state,
            transfer_manifest: None,
        };
        let scope = candidate.state.scope.clone();
        candidate.validate_for(&scope)?;
        Ok(candidate)
    }

    pub(crate) fn from_manifest(manifest: AuthzRealmTransferManifest) -> Result<Self, Status> {
        if manifest.format != AUTHZ_REALM_TRANSFER_MANIFEST_FORMAT || manifest.encoded_bytes == 0 {
            return Err(Status::data_loss(
                "authorization transfer manifest has an invalid v1 envelope",
            ));
        }
        let candidate = Self {
            state: manifest.state(),
            transfer_manifest: Some(manifest),
        };
        let scope = candidate.state.scope.clone();
        candidate.validate_for(&scope)?;
        Ok(candidate)
    }

    pub(crate) fn transfer_manifest(&self) -> Result<&AuthzRealmTransferManifest, Status> {
        self.transfer_manifest.as_ref().ok_or_else(|| {
            Status::failed_precondition("authorization candidate has no transfer manifest")
        })
    }

    pub(crate) fn validate_for(&self, scope: &AuthzScope) -> Result<(), Status> {
        if self.state.format != AUTHZ_REALM_STATE_FORMAT
            || self.state.scope != *scope
            || self.state.revision == AuthzRevision::ZERO
            || self.state.binding_generation == 0
            || self.state.schema_ref.schema_revision == 0
        {
            return Err(Status::data_loss(
                "authorization replica returned another realm or a zero revision",
            ));
        }
        match (
            self.state.predecessor_revision,
            self.state.mutation_fingerprint,
        ) {
            (None, None) => Ok(()),
            (predecessor, Some(fingerprint))
                if fingerprint != [0; 32]
                    && predecessor.is_none_or(|revision| {
                        revision != AuthzRevision::ZERO && revision < self.state.revision
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
    async fn apply_schema_publication(
        &self,
        target: NodeId,
        address: &str,
        stable_tenant_id: u64,
        mutation: &AuthzSchemaPublicationMutation,
    ) -> Result<ReplicaAuthzSchemaPublicationApplied, Status>;

    async fn has_schema_publication(
        &self,
        target: NodeId,
        address: &str,
        stable_tenant_id: u64,
        query: &AuthzSchemaReplicaQuery,
    ) -> Result<bool, Status>;

    async fn apply_realm_mutation(
        &self,
        target: NodeId,
        address: &str,
        stable_tenant_id: u64,
        mutation: &AuthzRealmMutation,
    ) -> Result<ReplicaAuthzRealmMutationApplied, Status>;

    async fn read_realm_candidate(
        &self,
        target: NodeId,
        address: &str,
        stable_tenant_id: u64,
        scope: &AuthzScope,
    ) -> Result<Option<AuthzRealmReplicaCandidate>, Status>;

    async fn install_realm_candidate(
        &self,
        target: NodeId,
        address: &str,
        stable_tenant_id: u64,
        source: Option<(NodeId, String)>,
        scope: &AuthzScope,
        winner: Option<&AuthzRealmReplicaCandidate>,
    ) -> Result<AuthzRealmSnapshotApplied, Status>;
}

/// Exact read-only proof used when an apply response or the original client
/// response was lost. It transfers no catalogue and performs no repair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AuthzSchemaReplicaQuery {
    pub(crate) storage_tenant: StorageTenantId,
    pub(crate) schema_ref: SchemaRef,
    pub(crate) schema: Schema,
    pub(crate) published_at_revision: AuthzRevision,
    pub(crate) publication_mutation: Option<AuthzSchemaPublicationMutation>,
}

#[derive(Clone, Debug)]
struct ReplicaEndpoint {
    node_id: NodeId,
    address: String,
}

#[derive(Clone, Debug)]
struct TenantReplicaSet {
    stable_tenant_id: u64,
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
        Ok(Self {
            stable_tenant_id: tenant_id,
            group,
            endpoints,
        })
    }
}

#[derive(Clone)]
struct AuthzDistributionCore {
    local_node: NodeId,
    repository: AuthzRepository,
    peers: Arc<dyn AuthzReplicaTransport>,
    /// Fixed lanes retain the required tenant-wide schema ordering and
    /// per-realm ordering without a growing lock registry.
    coordinator_lanes: Arc<[tokio::sync::RwLock<()>; AUTHZ_COORDINATOR_LANES]>,
}

impl AuthzDistributionCore {
    fn tenant_lane(&self, stable_tenant_id: u64) -> &tokio::sync::RwLock<()> {
        &self.coordinator_lanes[lane_index(stable_tenant_id)]
    }

    fn realm_lane(&self, stable_tenant_id: u64, scope: &AuthzScope) -> &tokio::sync::RwLock<()> {
        let mut hash = stable_tenant_id ^ 0x517c_c1b7_2722_0a95;
        hash = hash_lane_component(hash, scope.storage_tenant.as_str().as_bytes());
        hash = hash_lane_component(hash, scope.realm.as_str().as_bytes());
        &self.coordinator_lanes[lane_index(hash)]
    }
}

fn hash_lane_component(mut hash: u64, bytes: &[u8]) -> u64 {
    hash ^= bytes.len() as u64;
    hash = hash.wrapping_mul(0x100_0000_01b3);
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

fn lane_index(mut hash: u64) -> usize {
    hash = (hash ^ (hash >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash = (hash ^ (hash >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^= hash >> 31;
    (hash as usize) % AUTHZ_COORDINATOR_LANES
}

impl AuthzDistributionCore {
    async fn replicate_schema_publication(
        &self,
        replicas: &TenantReplicaSet,
        storage_tenant: &StorageTenantId,
        coordinated: &CoordinatedAuthzSchemaPublication,
    ) -> Result<(), Status> {
        let schema = self
            .repository
            .get_schema(storage_tenant, &coordinated.result.schema_ref)
            .map_err(authz_status)?
            .ok_or_else(|| Status::data_loss("coordinated schema revision is missing locally"))?;
        let query = AuthzSchemaReplicaQuery {
            storage_tenant: storage_tenant.clone(),
            schema_ref: coordinated.result.schema_ref.clone(),
            schema,
            published_at_revision: coordinated.result.authz_revision,
            publication_mutation: coordinated.mutation.clone(),
        };
        if self
            .repository
            .tenant_revision(storage_tenant)
            .map_err(authz_status)?
            < query.published_at_revision
        {
            return Err(Status::data_loss(
                "coordinated schema revision exceeds the local tenant revision",
            ));
        }
        if let Some(mutation) = coordinated.mutation.as_ref()
            && (mutation.storage_tenant != *storage_tenant
                || mutation.schema_ref != query.schema_ref
                || mutation.schema != query.schema
                || mutation.revision() != query.published_at_revision)
        {
            return Err(Status::data_loss(
                "coordinated schema mutation disagrees with its local result",
            ));
        }
        if coordinated.mutation.is_none() && !coordinated.result.replayed {
            return Err(Status::data_loss(
                "new schema publication omitted its typed mutation",
            ));
        }

        let mut durable = vec![self.local_node];
        if let Some(mutation) = coordinated.mutation.as_ref() {
            let mut tasks = tokio::task::JoinSet::new();
            let stable_tenant_id = replicas.stable_tenant_id;
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
                        .apply_schema_publication(
                            endpoint.node_id,
                            &endpoint.address,
                            stable_tenant_id,
                            &mutation,
                        )
                        .await;
                    (endpoint.node_id, result)
                });
            }
            while let Some(joined) = tasks.join_next().await {
                let (node_id, result) = joined.map_err(|error| {
                    Status::internal(format!("authorization schema peer task failed: {error}"))
                })?;
                if matches!(result, Ok(applied) if applied.revision == query.published_at_revision)
                {
                    durable.push(node_id);
                }
            }
        }
        if replicas.group.is_acknowledged_by(&durable) {
            return Ok(());
        }

        // An apply may have committed even when its response was lost. A
        // digest replay has no new mutation, so it uses the same exact proof.
        // This proves durability only; it never copies or repairs a catalogue.
        let mut tasks = tokio::task::JoinSet::new();
        let stable_tenant_id = replicas.stable_tenant_id;
        for endpoint in replicas
            .endpoints
            .iter()
            .filter(|endpoint| !durable.contains(&endpoint.node_id))
            .cloned()
        {
            let peers = self.peers.clone();
            let query = query.clone();
            tasks.spawn(async move {
                let result = peers
                    .has_schema_publication(
                        endpoint.node_id,
                        &endpoint.address,
                        stable_tenant_id,
                        &query,
                    )
                    .await;
                (endpoint.node_id, result)
            });
        }
        while let Some(joined) = tasks.join_next().await {
            let (node_id, result) = joined.map_err(|error| {
                Status::internal(format!("authorization schema proof task failed: {error}"))
            })?;
            if matches!(result, Ok(true)) {
                durable.push(node_id);
            }
        }
        if replicas.group.is_acknowledged_by(&durable) {
            Ok(())
        } else {
            Err(Status::unavailable(format!(
                "authorization schema publication reached {} of {} required replicas",
                durable.len(),
                replicas.group.required_acknowledgements()
            )))
        }
    }

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
        let stable_tenant_id = replicas.stable_tenant_id;
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
                    .apply_realm_mutation(
                        endpoint.node_id,
                        &endpoint.address,
                        stable_tenant_id,
                        &mutation,
                    )
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
        let stable_tenant_id = replicas.stable_tenant_id;
        for endpoint in replicas.endpoints.iter().cloned() {
            let peers = self.peers.clone();
            let scope = scope.clone();
            tasks.spawn(async move {
                let result = peers
                    .read_realm_candidate(
                        endpoint.node_id,
                        &endpoint.address,
                        stable_tenant_id,
                        &scope,
                    )
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

        self.require_local_winner(
            &observations,
            replicas.stable_tenant_id,
            source.clone(),
            scope,
            winner.as_ref(),
        )
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
                    replicas.stable_tenant_id,
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
        stable_tenant_id: u64,
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
            .install_realm_candidate(
                self.local_node,
                &local.0.address,
                stable_tenant_id,
                source,
                scope,
                winner,
            )
            .await?;
        let installed = self
            .peers
            .read_realm_candidate(self.local_node, &local.0.address, stable_tenant_id, scope)
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
        let (allowed, revision) = self
            .fresh_checks(replicas, scope, consistency, vec![check])
            .await?;
        Ok((allowed[0], revision))
    }

    async fn fresh_checks(
        &self,
        replicas: &TenantReplicaSet,
        scope: AuthzScope,
        consistency: AuthzConsistency,
        checks: Vec<AuthorizationCheck>,
    ) -> Result<(Vec<bool>, AuthzRevision), Status> {
        if self.reconcile(replicas, &scope).await?.is_none() {
            return Err(Status::failed_precondition(
                "authorization realm has no schema binding",
            ));
        }
        let repository = self.repository.clone();
        let result = tokio::task::spawn_blocking(move || {
            repository.batch_check(&scope, consistency, &checks)
        })
        .await
        .map_err(|error| Status::internal(format!("authorization worker failed: {error}")))?
        .map_err(authz_status)?;
        Ok((result.allowed, result.revision))
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
    mutation_admission: crate::mutation_admission::MutationAdmission,
}

impl ZanzibarDistribution {
    pub(crate) fn new(
        local_node: NodeId,
        repository: AuthzRepository,
        decisions: DecisionRaft,
        serving: ServingAuthority,
        peers: Arc<dyn AuthzReplicaTransport>,
        mutation_admission: crate::mutation_admission::MutationAdmission,
    ) -> Self {
        Self {
            local_node,
            decisions,
            serving,
            core: AuthzDistributionCore {
                local_node,
                repository,
                peers,
                coordinator_lanes: Arc::new(std::array::from_fn(|_| tokio::sync::RwLock::new(()))),
            },
            mutation_admission,
        }
    }

    pub(crate) async fn bind_schema_journaled(
        &self,
        stable_tenant_id: u64,
        store: &Store,
        request: BindSchemaRequest,
    ) -> Result<CoordinatedAuthzRealmMutation, Status> {
        loop {
            let serial = self
                .core
                .realm_lane(stable_tenant_id, &request.scope)
                .write()
                .await;
            let permit = self.mutation_admission.enter()?;
            let mut replicas = self.require_coordinator(stable_tenant_id)?;
            let serving = self.serving.mutation_context()?;
            let scope = request.scope.clone();
            self.core.reconcile(&replicas, &scope).await?;
            replicas = self.require_coordinator(stable_tenant_id)?;
            let current = self.serving.mutation_context()?;
            if current != serving {
                return Err(Status::unavailable(
                    "authorization serving fence changed during reconciliation",
                ));
            }
            let coordinated = match store
                .coordinate_journaled_authz_schema_binding(
                    stable_tenant_id,
                    request.clone(),
                    serving.active_placement_log_id,
                    serving.serving_fence_term,
                )
                .await
            {
                Ok(coordinated) => coordinated,
                Err(AuthzStoreError::SourceJournalCapacity) => {
                    drop(permit);
                    drop(serial);
                    store.wait_for_mutation_capacity().await;
                    continue;
                }
                Err(error) => return Err(authz_status(error)),
            };
            self.core.replicate(&replicas, &scope, &coordinated).await?;
            return Ok(coordinated);
        }
    }

    pub(crate) async fn publish_schema_journaled(
        &self,
        stable_tenant_id: u64,
        store: &Store,
        request: PublishSchemaRequest,
    ) -> Result<CoordinatedAuthzSchemaPublication, Status> {
        loop {
            let serial = self.core.tenant_lane(stable_tenant_id).write().await;
            let permit = self.mutation_admission.enter()?;
            let replicas = self.require_coordinator(stable_tenant_id)?;
            let serving = self.serving.mutation_context()?;
            let storage_tenant = request.storage_tenant.clone();
            let coordinated = match store
                .coordinate_journaled_authz_schema_publication(
                    stable_tenant_id,
                    request.clone(),
                    serving.active_placement_log_id,
                    serving.serving_fence_term,
                )
                .await
            {
                Ok(coordinated) => coordinated,
                Err(AuthzStoreError::SourceJournalCapacity) => {
                    drop(permit);
                    drop(serial);
                    store.wait_for_mutation_capacity().await;
                    continue;
                }
                Err(error) => return Err(authz_status(error)),
            };
            self.core
                .replicate_schema_publication(&replicas, &storage_tenant, &coordinated)
                .await?;
            return Ok(coordinated);
        }
    }

    pub(crate) async fn reconcile_realm(
        &self,
        stable_tenant_id: u64,
        scope: &AuthzScope,
    ) -> Result<(), Status> {
        let _serial = self.core.realm_lane(stable_tenant_id, scope).write().await;
        let replicas = self.require_coordinator(stable_tenant_id)?;
        self.serving.mutation_context()?;
        if self.core.reconcile(&replicas, scope).await?.is_none() {
            return Err(Status::failed_precondition(
                "authorization realm has no schema binding",
            ));
        }
        Ok(())
    }

    pub(crate) fn repository(&self) -> &AuthzRepository {
        &self.core.repository
    }

    /// Coordinates a tuple mutation through the storage kernel boundary that
    /// commits its AggregateChanged record in the same RocksDB WriteBatch.
    pub(crate) async fn mutate_tuples_journaled(
        &self,
        stable_tenant_id: u64,
        store: &Store,
        request: TupleBatchRequest,
    ) -> Result<CoordinatedAuthzRealmMutation, Status> {
        self.mutate_tuples_journaled_inner(stable_tenant_id, store, request, false)
            .await
    }

    /// Replay a trusted adapter's reconstructed tuple request against the
    /// retained original CAS before entering the ordinary mutation path.
    pub(crate) async fn mutate_tuples_journaled_restoring_retained_precondition(
        &self,
        stable_tenant_id: u64,
        store: &Store,
        request: TupleBatchRequest,
    ) -> Result<CoordinatedAuthzRealmMutation, Status> {
        self.mutate_tuples_journaled_inner(stable_tenant_id, store, request, true)
            .await
    }

    async fn mutate_tuples_journaled_inner(
        &self,
        stable_tenant_id: u64,
        store: &Store,
        request: TupleBatchRequest,
        restore_retained_precondition: bool,
    ) -> Result<CoordinatedAuthzRealmMutation, Status> {
        loop {
            let serial = self
                .core
                .realm_lane(stable_tenant_id, &request.scope)
                .write()
                .await;
            let permit = self.mutation_admission.enter()?;
            let mut replicas = self.require_coordinator(stable_tenant_id)?;
            let serving = self.serving.mutation_context()?;
            let scope = request.scope.clone();
            self.core.reconcile(&replicas, &scope).await?;
            replicas = self.require_coordinator(stable_tenant_id)?;
            let current = self.serving.mutation_context()?;
            if current != serving {
                return Err(Status::unavailable(
                    "authorization serving fence changed during reconciliation",
                ));
            }
            let request = if restore_retained_precondition {
                self.core
                    .repository
                    .restore_retained_tuple_replay_precondition(request.clone())
                    .map_err(authz_status)?
            } else {
                request.clone()
            };
            let coordinated = match store
                .coordinate_journaled_authz_tuple_mutation(
                    stable_tenant_id,
                    request,
                    serving.active_placement_log_id,
                    serving.serving_fence_term,
                )
                .await
            {
                Ok(coordinated) => coordinated,
                Err(AuthzStoreError::SourceJournalCapacity) => {
                    drop(permit);
                    drop(serial);
                    store.wait_for_mutation_capacity().await;
                    continue;
                }
                Err(error) => return Err(authz_status(error)),
            };
            self.core.replicate(&replicas, &scope, &coordinated).await?;
            return Ok(coordinated);
        }
    }

    pub(crate) async fn fresh_check(
        &self,
        stable_tenant_id: u64,
        scope: AuthzScope,
        consistency: AuthzConsistency,
        check: AuthorizationCheck,
    ) -> Result<(bool, AuthzRevision), Status> {
        self.fresh_check_with_generation(stable_tenant_id, scope, consistency, check)
            .await
            .map(|(allowed, revision, _)| (allowed, revision))
    }

    pub(crate) async fn fresh_check_with_generation(
        &self,
        stable_tenant_id: u64,
        scope: AuthzScope,
        consistency: AuthzConsistency,
        check: AuthorizationCheck,
    ) -> Result<(bool, AuthzRevision, u64), Status> {
        let _serial = self.core.realm_lane(stable_tenant_id, &scope).read().await;
        let (replicas, placement_fence) = self.require_read_replica(stable_tenant_id)?;
        let checked_scope = scope.clone();
        let (allowed, revision) = self
            .core
            .fresh_check(&replicas, scope, consistency, check)
            .await?;
        let binding = self
            .core
            .repository
            .get_binding(&checked_scope)
            .map_err(authz_status)?
            .ok_or_else(|| Status::failed_precondition("authorization realm has no binding"))?;
        self.require_unchanged_fresh_context(stable_tenant_id, &replicas.group, placement_fence)?;
        Ok((allowed, revision, binding.generation))
    }

    pub(crate) async fn fresh_checks_with_generation(
        &self,
        stable_tenant_id: u64,
        scope: AuthzScope,
        consistency: AuthzConsistency,
        checks: Vec<AuthorizationCheck>,
    ) -> Result<(Vec<bool>, AuthzRevision, u64), Status> {
        let _serial = self.core.realm_lane(stable_tenant_id, &scope).read().await;
        let (replicas, placement_fence) = self.require_read_replica(stable_tenant_id)?;
        let checked_scope = scope.clone();
        let (allowed, revision) = self
            .core
            .fresh_checks(&replicas, scope, consistency, checks)
            .await?;
        let binding = self
            .core
            .repository
            .get_binding(&checked_scope)
            .map_err(authz_status)?
            .ok_or_else(|| Status::failed_precondition("authorization realm has no binding"))?;
        self.require_unchanged_fresh_context(stable_tenant_id, &replicas.group, placement_fence)?;
        Ok((allowed, revision, binding.generation))
    }

    fn require_unchanged_fresh_context(
        &self,
        stable_tenant_id: u64,
        original_group: &MutableRecordReplicaGroup,
        original_fence: PlacementLogId,
    ) -> Result<(), Status> {
        let (current_replicas, current_fence) = self
            .require_read_replica(stable_tenant_id)
            .map_err(|_| Status::unavailable("authorization replica changed during fresh check"))?;
        if current_replicas.group != *original_group || current_fence != original_fence {
            return Err(Status::unavailable(
                "authorization placement fence or replica group changed during fresh check",
            ));
        }
        Ok(())
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

    /// Fresh checks are read-only, but still reconcile the complete realm from
    /// its exact replica quorum before evaluation. Any selected replica may do
    /// that work so loss of rank zero does not make a healthy quorum
    /// unavailable. Mutation coordination remains rank-zero-only.
    fn require_read_replica(
        &self,
        stable_tenant_id: u64,
    ) -> Result<(TenantReplicaSet, PlacementLogId), Status> {
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
        let placement = ClusterPlacement::from_applied(&state)
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let replicas = TenantReplicaSet::from_placement(&placement, stable_tenant_id)?;
        if !replicas.group.replicas().contains(&self.local_node) {
            return Err(Status::failed_precondition(
                "fresh authorization check did not reach a selected tenant replica",
            ));
        }
        Ok((replicas, placement.fence()))
    }
}

pub(crate) fn exact_quorum_candidate(
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
            left.state.revision == right.state.revision
                && left.state.mutation_fingerprint != right.state.mutation_fingerprint
        })
    });
    let reason = if sibling { "sibling" } else { "lineage gap" };
    Err(Status::unavailable(format!(
        "authorization realm has no exact read quorum ({reason})"
    )))
}

fn authz_status(error: AuthzStoreError) -> Status {
    match error {
        AuthzStoreError::ReceiptCapacity
        | AuthzStoreError::SourceJournalCapacity
        | AuthzStoreError::RevisionNotAvailable { .. } => Status::unavailable(error.to_string()),
        AuthzStoreError::Storage(_) => Status::internal(error.to_string()),
        _ => Status::failed_precondition(error.to_string()),
    }
}

#[cfg(test)]
mod tests;
