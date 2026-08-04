//! Fenced cluster-wide `ListObjects` collection and page merge.
//!
//! Every ACTIVE source scans one local RocksDB snapshot, filters it through the
//! same weighted-HRW placement, and returns one bounded page. The ingress then
//! validates every source identity and fence before a bounded lexical merge.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anvil_atomic_program::MAX_OBJECT_PATH_BYTES;
use anvil_authz::{ObjectRef, RealmId};
use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::{
    ListObjectsPage, MAX_LIST_OBJECTS, ObjectKey, PlacementLogId, StorageTenantId, Store,
};
use thiserror::Error;
use tonic::{Status, metadata::MetadataMap};

use crate::authentication::{Caller, JwtManager};
use crate::authorization::{ObjectPermission, SystemAuthorizer};
use crate::cluster_placement::ClusterPlacement;
use crate::placement::PlacementKind;
use crate::serving_fence::ServingAuthority;

/// Original request authority for a distributed object read, deliberately
/// separate from every Debug or protobuf-derived listing type. Private mTLS
/// transports carry either the signed bearer or the fixed anonymous marker in
/// gRPC metadata rather than serializing a caller or an authorization result.
#[derive(Clone)]
pub(crate) struct OriginalBearer(Arc<str>);

impl OriginalBearer {
    pub(crate) fn from_metadata(metadata: &MetadataMap) -> Result<Self, Status> {
        let mut values = metadata.get_all("authorization").iter();
        let value = values
            .next()
            .ok_or_else(|| Status::unauthenticated("a bearer token is required"))?;
        if values.next().is_some() {
            return Err(Status::unauthenticated(
                "exactly one bearer token is required",
            ));
        }
        let value = value
            .to_str()
            .map_err(|_| Status::unauthenticated("the bearer token is malformed"))?;
        let token = value
            .strip_prefix("Bearer ")
            .filter(|token| !token.is_empty())
            .ok_or_else(|| Status::unauthenticated("the bearer token is malformed"))?;
        Ok(Self(Arc::from(token)))
    }

    /// Private mTLS routing marker for the public ingress identity. It is not
    /// accepted by the public JWT interceptor and carries no authority of its
    /// own; every source still evaluates Zanzibar for `app:_anvil/anonymous`.
    pub(crate) fn anonymous() -> Self {
        Self(Arc::from(anvil_authz::ANONYMOUS_SUBJECT_ID))
    }

    pub(crate) fn from_signed_token(token: impl Into<Arc<str>>) -> Self {
        Self(token.into())
    }

    pub(crate) fn signed_token(&self) -> &str {
        &self.0
    }

    pub(crate) fn is_anonymous(&self) -> bool {
        self.0.as_ref() == anvil_authz::ANONYMOUS_SUBJECT_ID
    }
}

/// One stable-ID listing request sent to every member of one exact placement.
///
/// Mutable tenant and bucket names are resolved once at ingress. Peer requests
/// carry those canonical names for independent authorization, the bound stable
/// IDs used for the head scan, and the exact committed placement fence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LocalListQuery {
    placement_fence: PlacementLogId,
    tenant: String,
    bucket: String,
    tenant_id: u64,
    bucket_id: u64,
    prefix: String,
    start_after: Option<String>,
    limit: usize,
    include_index_definitions: bool,
    include_personaldb_manifests: bool,
}

impl LocalListQuery {
    pub(crate) fn new(
        placement_fence: PlacementLogId,
        tenant: impl Into<String>,
        bucket: impl Into<String>,
        tenant_id: u64,
        bucket_id: u64,
        prefix: impl Into<String>,
        start_after: Option<String>,
        limit: usize,
    ) -> Result<Self, Status> {
        let tenant = tenant.into();
        let bucket = bucket.into();
        let prefix = prefix.into();
        if placement_fence.term == 0 || placement_fence.index == 0 {
            return Err(Status::failed_precondition(
                "active placement fence is unavailable",
            ));
        }
        if tenant_id == 0 || bucket_id == 0 {
            return Err(Status::invalid_argument(
                "stable tenant and bucket IDs must be non-zero",
            ));
        }
        let validation_path = start_after.as_deref().unwrap_or("_list");
        ObjectKey::new(&tenant, &bucket, validation_path)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if prefix.len() > MAX_OBJECT_PATH_BYTES {
            return Err(Status::invalid_argument(format!(
                "list prefix exceeds {MAX_OBJECT_PATH_BYTES} UTF-8 bytes"
            )));
        }
        if start_after
            .as_ref()
            .is_some_and(|cursor| cursor.is_empty() || cursor.len() > MAX_OBJECT_PATH_BYTES)
        {
            return Err(Status::invalid_argument(
                "exclusive list cursor is empty or too long",
            ));
        }
        if !(1..=MAX_LIST_OBJECTS).contains(&limit) {
            return Err(Status::invalid_argument(format!(
                "list limit must be within 1..={MAX_LIST_OBJECTS}"
            )));
        }
        Ok(Self {
            placement_fence,
            tenant,
            bucket,
            tenant_id,
            bucket_id,
            prefix,
            start_after,
            limit,
            include_index_definitions: false,
            include_personaldb_manifests: false,
        })
    }

    pub(crate) const fn placement_fence(&self) -> PlacementLogId {
        self.placement_fence
    }

    pub(crate) fn tenant(&self) -> &str {
        &self.tenant
    }

    pub(crate) fn bucket(&self) -> &str {
        &self.bucket
    }

    pub(crate) const fn tenant_id(&self) -> u64 {
        self.tenant_id
    }

    pub(crate) const fn bucket_id(&self) -> u64 {
        self.bucket_id
    }

    pub(crate) fn prefix(&self) -> &str {
        &self.prefix
    }

    pub(crate) fn start_after(&self) -> Option<&str> {
        self.start_after.as_deref()
    }

    pub(crate) const fn limit(&self) -> usize {
        self.limit
    }

    pub(crate) const fn includes_index_definitions(&self) -> bool {
        self.include_index_definitions
    }

    pub(crate) const fn includes_personaldb_manifests(&self) -> bool {
        self.include_personaldb_manifests
    }

    pub(crate) fn for_index_definitions(mut self) -> Result<Self, Status> {
        let suffix = self
            .prefix
            .strip_prefix("_anvil/indexes/definitions/")
            .ok_or_else(|| {
                Status::invalid_argument(
                    "index-definition listing requires its exact reserved prefix",
                )
            })?;
        if suffix.contains('/')
            || self.start_after.as_ref().is_some_and(|path| {
                crate::index_runtime::publication::index_definition_name(path).is_none()
            })
        {
            return Err(Status::invalid_argument(
                "index-definition listing cannot enumerate another reserved path",
            ));
        }
        self.include_index_definitions = true;
        Ok(self)
    }

    pub(crate) fn for_personaldb_manifests(mut self) -> Result<Self, Status> {
        if self.prefix != crate::personaldb::MANIFEST_ROOT_PREFIX
            || self
                .start_after
                .as_ref()
                .is_some_and(|path| crate::personaldb::parse_manifest_object_path(path).is_err())
        {
            return Err(Status::invalid_argument(
                "PersonalDB listing is restricted to published group manifests",
            ));
        }
        self.include_personaldb_manifests = true;
        Ok(self)
    }
}

/// Destination-side authorization for a listing source.
///
/// An implementation may evaluate locally only when it can prove that its
/// system-realm replica is current for the exact placement. A later N>3
/// adapter may instead call the HRW Zanzibar coordinator. Neither form may
/// accept a serialized caller or an ingress allow/deny decision.
#[tonic::async_trait]
pub(crate) trait AuthoritativeListAuthorizer: Send + Sync + 'static {
    async fn authorize(
        &self,
        bearer: &OriginalBearer,
        query: &LocalListQuery,
    ) -> Result<(), Status>;
}

/// Fail-closed startup bridge for the private listener.
///
/// The mandatory-mTLS listener must accept join traffic before the serving
/// fence and authoritative Zanzibar coordinator are ready. Listing requests
/// therefore fail immediately until that already-approved authorizer is
/// installed; no request is queued and no authorization result is cached.
#[derive(Clone, Default)]
pub(crate) struct LateBoundListAuthorizer {
    inner: Arc<OnceLock<Arc<dyn AuthoritativeListAuthorizer>>>,
}

impl LateBoundListAuthorizer {
    pub(crate) fn install(
        &self,
        authorizer: Arc<dyn AuthoritativeListAuthorizer>,
    ) -> Result<(), Arc<dyn AuthoritativeListAuthorizer>> {
        self.inner.set(authorizer)
    }
}

#[tonic::async_trait]
impl AuthoritativeListAuthorizer for LateBoundListAuthorizer {
    async fn authorize(
        &self,
        bearer: &OriginalBearer,
        query: &LocalListQuery,
    ) -> Result<(), Status> {
        let authorizer = self
            .inner
            .get()
            .cloned()
            .ok_or_else(|| Status::unavailable("list authorization is not ready"))?;
        authorizer.authorize(bearer, query).await
    }
}

/// The resource-bearing permission sent to the distributed Zanzibar owner.
/// Expected stable IDs bind the authorized mutable names to the head-key range
/// and prevent a confused-deputy request from pairing one grant with another
/// bucket's physical IDs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ListAuthorizationPermission {
    BucketObjectsGet {
        bucket: String,
        expected_tenant_id: u64,
        expected_bucket_id: u64,
    },
}

/// Transport-neutral boundary owned by Zanzibar distribution, not listing.
/// Its production adapter chooses the canonical HRW realm coordinator and
/// returns only a fresh allow/deny (or an availability error).
#[tonic::async_trait]
pub(crate) trait TenantAuthorizationCoordinator: Send + Sync + 'static {
    async fn allows(
        &self,
        storage_tenant: StorageTenantId,
        realm: RealmId,
        subject: ObjectRef,
        permission: ListAuthorizationPermission,
        placement_fence: PlacementLogId,
    ) -> Result<bool, Status>;
}

/// Source-side verifier. It reconstructs identity from the original JWT and
/// delegates only the Zanzibar check to the authoritative realm coordinator.
#[derive(Clone)]
pub(crate) struct CoordinatedListAuthorizer {
    tokens: JwtManager,
    coordinator: Arc<dyn TenantAuthorizationCoordinator>,
}

impl CoordinatedListAuthorizer {
    pub(crate) fn new(
        tokens: JwtManager,
        coordinator: Arc<dyn TenantAuthorizationCoordinator>,
    ) -> Self {
        Self {
            tokens,
            coordinator,
        }
    }
}

#[tonic::async_trait]
impl AuthoritativeListAuthorizer for CoordinatedListAuthorizer {
    async fn authorize(
        &self,
        bearer: &OriginalBearer,
        query: &LocalListQuery,
    ) -> Result<(), Status> {
        let caller = if bearer.is_anonymous() {
            Caller::from_anonymous(
                StorageTenantId::parse(query.tenant())
                    .map_err(|error| Status::invalid_argument(error.to_string()))?,
            )
        } else {
            self.tokens
                .verify(bearer.signed_token())
                .map_err(|_| Status::unauthenticated("the bearer token is invalid or expired"))?
        };
        if caller.storage_tenant().as_str() != query.tenant() {
            return Err(Status::permission_denied(
                "object list does not belong to the authenticated tenant",
            ));
        }
        // This reserved scan is consumed only by the in-process PersonalDB
        // service. That service performs a fresh per-group Zanzibar check
        // before returning any descriptor to the caller. Requiring a
        // bucket-wide grant here would incorrectly hide exact group roles.
        if query.includes_personaldb_manifests() {
            return Ok(());
        }
        let allowed = self
            .coordinator
            .allows(
                caller.storage_tenant().clone(),
                RealmId::system(),
                caller.subject().clone(),
                ListAuthorizationPermission::BucketObjectsGet {
                    bucket: query.bucket().to_owned(),
                    expected_tenant_id: query.tenant_id(),
                    expected_bucket_id: query.bucket_id(),
                },
                query.placement_fence(),
            )
            .await?;
        if allowed {
            Ok(())
        } else {
            Err(Status::permission_denied(
                "bucket-wide object read is required for listing",
            ))
        }
    }
}

trait ListFenceAuthority: Send + Sync + 'static {
    fn require_fence(&self, expected: PlacementLogId) -> Result<(), Status>;
}

impl ListFenceAuthority for ServingAuthority {
    fn require_fence(&self, expected: PlacementLogId) -> Result<(), Status> {
        let current = self.mutation_context()?.active_placement_log_id;
        if current == expected {
            Ok(())
        } else {
            Err(Status::unavailable(
                "authorization serving fence changed during listing",
            ))
        }
    }
}

/// Focused local coordinator used only when the Zanzibar distribution layer
/// has already proved this node owns a current system-realm replica.
#[derive(Clone)]
pub(crate) struct LocalListAuthorizationCoordinator {
    store: Store,
    system: SystemAuthorizer,
    fence: Arc<dyn ListFenceAuthority>,
}

impl LocalListAuthorizationCoordinator {
    pub(crate) fn new(store: Store, fence: ServingAuthority) -> Self {
        Self {
            system: SystemAuthorizer::new(store.authz()),
            store,
            fence: Arc::new(fence),
        }
    }

    #[cfg(test)]
    fn with_test_fence(store: Store, fence: Arc<dyn ListFenceAuthority>) -> Self {
        Self {
            system: SystemAuthorizer::new(store.authz()),
            store,
            fence,
        }
    }
}

#[tonic::async_trait]
impl TenantAuthorizationCoordinator for LocalListAuthorizationCoordinator {
    async fn allows(
        &self,
        storage_tenant: StorageTenantId,
        realm: RealmId,
        subject: ObjectRef,
        permission: ListAuthorizationPermission,
        placement_fence: PlacementLogId,
    ) -> Result<bool, Status> {
        if realm != RealmId::system() {
            return Err(Status::invalid_argument(
                "list authorization requires the protected system realm",
            ));
        }
        self.fence.require_fence(placement_fence)?;
        let ListAuthorizationPermission::BucketObjectsGet {
            bucket,
            expected_tenant_id,
            expected_bucket_id,
        } = permission;
        let store = self.store.clone();
        let system = self.system.clone();
        let tenant = storage_tenant.as_str().to_owned();
        tokio::task::spawn_blocking(move || {
            let resolved = store
                .resolve_bucket_ids(&tenant, &bucket)
                .map_err(|_| Status::unavailable("authoritative bucket identity is unavailable"))?;
            if resolved != (expected_tenant_id, expected_bucket_id) {
                return Err(Status::unavailable(
                    "bucket identity changed while authorizing the list",
                ));
            }
            let authorization = system
                .load()
                .map_err(|_| Status::unavailable("authoritative Zanzibar state is unavailable"))?;
            let allowed = authorization
                .allows_bucket_objects(&subject, &tenant, &bucket, ObjectPermission::Get)
                .map_err(crate::authz_api::authz_status)?;
            Ok(allowed)
        })
        .await
        .map_err(|error| Status::internal(format!("list authorization worker failed: {error}")))?
    }
}

/// Explicit fail-closed placeholder for a node that cannot prove a current
/// local system-realm replica. It is replaced only by the separately approved
/// HRW authorization-coordinator adapter.
pub(crate) struct UnavailableTenantAuthorizationCoordinator;

#[tonic::async_trait]
impl TenantAuthorizationCoordinator for UnavailableTenantAuthorizationCoordinator {
    async fn allows(
        &self,
        _storage_tenant: StorageTenantId,
        _realm: RealmId,
        _subject: ObjectRef,
        _permission: ListAuthorizationPermission,
        _placement_fence: PlacementLogId,
    ) -> Result<bool, Status> {
        Err(Status::unavailable(
            "authoritative Zanzibar coordinator adapter is unavailable",
        ))
    }
}

/// One peer result, including the identity and fence that cannot be inferred
/// safely from arrival order or the connection target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OwnedListPage {
    source_node: NodeId,
    placement_fence: PlacementLogId,
    page: ListObjectsPage,
}

impl OwnedListPage {
    pub(crate) fn new(
        source_node: NodeId,
        placement_fence: PlacementLogId,
        page: ListObjectsPage,
    ) -> Self {
        Self {
            source_node,
            placement_fence,
            page,
        }
    }

    pub(crate) const fn source_node(&self) -> NodeId {
        self.source_node
    }

    pub(crate) const fn placement_fence(&self) -> PlacementLogId {
        self.placement_fence
    }

    pub(crate) fn page(&self) -> &ListObjectsPage {
        &self.page
    }

    pub(crate) fn into_page(self) -> ListObjectsPage {
        self.page
    }
}

/// Mandatory-mTLS transport boundary for one bounded source-local page.
///
/// The concrete peer client is added beside the private peer protobuf. Tests
/// use the same boundary against independent stores without adding a second
/// listing implementation.
#[tonic::async_trait]
pub(crate) trait ClusterListPeers: Send + Sync + 'static {
    async fn list_local_page(
        &self,
        target: NodeId,
        address: &str,
        bearer: OriginalBearer,
        query: LocalListQuery,
    ) -> Result<OwnedListPage, Status>;
}

trait ListPlacementView: Send + Sync + 'static {
    fn fence(&self) -> PlacementLogId;
    fn active_node_ids(&self) -> Vec<NodeId>;
    fn address(&self, node_id: NodeId) -> Option<String>;
    fn object_owner(&self, tenant_id: u64, bucket_id: u64, path: &str) -> Option<NodeId>;
}

impl ListPlacementView for ClusterPlacement {
    fn fence(&self) -> PlacementLogId {
        self.fence()
    }

    fn active_node_ids(&self) -> Vec<NodeId> {
        self.active_node_ids()
    }

    fn address(&self, node_id: NodeId) -> Option<String> {
        self.address(node_id).map(|address| address.0.clone())
    }

    fn object_owner(&self, tenant_id: u64, bucket_id: u64, path: &str) -> Option<NodeId> {
        self.rank(
            PlacementKind::Object,
            &object_placement_key(tenant_id, bucket_id, path),
        )
        .into_iter()
        .next()
    }
}

/// Cluster-wide listing hook consumed by the public object service.
#[derive(Clone)]
pub(crate) struct DistributedObjectLister {
    local_node: NodeId,
    store: Store,
    decisions: DecisionRaft,
    peers: Arc<dyn ClusterListPeers>,
    authorization: Arc<dyn AuthoritativeListAuthorizer>,
}

impl DistributedObjectLister {
    pub(crate) fn new(
        local_node: NodeId,
        store: Store,
        decisions: DecisionRaft,
        peers: Arc<dyn ClusterListPeers>,
        authorization: Arc<dyn AuthoritativeListAuthorizer>,
    ) -> Self {
        Self {
            local_node,
            store,
            decisions,
            peers,
            authorization,
        }
    }

    /// Resolve no mutable names and retain no cross-page snapshot. The caller
    /// supplies stable IDs and each invocation captures a fresh placement.
    pub(crate) async fn list_objects(
        &self,
        bearer: OriginalBearer,
        tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ListObjectsPage, Status> {
        let placement = self.placement()?;
        let query = LocalListQuery::new(
            placement.fence(),
            tenant,
            bucket,
            tenant_id,
            bucket_id,
            prefix,
            start_after.map(str::to_owned),
            limit,
        )?;
        self.list_query(bearer, placement, query).await
    }

    /// Lists immutable index definitions only. The same bucket-level Zanzibar
    /// check and cluster merge apply; public object listing cannot set this.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn list_index_definitions(
        &self,
        bearer: OriginalBearer,
        tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ListObjectsPage, Status> {
        let placement = self.placement()?;
        let query = LocalListQuery::new(
            placement.fence(),
            tenant,
            bucket,
            tenant_id,
            bucket_id,
            prefix,
            start_after.map(str::to_owned),
            limit,
        )?
        .for_index_definitions()?;
        self.list_query(bearer, placement, query).await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn list_personaldb_manifests(
        &self,
        bearer: OriginalBearer,
        tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ListObjectsPage, Status> {
        let placement = self.placement()?;
        let query = LocalListQuery::new(
            placement.fence(),
            tenant,
            bucket,
            tenant_id,
            bucket_id,
            crate::personaldb::MANIFEST_ROOT_PREFIX,
            start_after.map(str::to_owned),
            limit,
        )?
        .for_personaldb_manifests()?;
        self.list_query(bearer, placement, query).await
    }

    async fn list_query(
        &self,
        bearer: OriginalBearer,
        placement: ClusterPlacement,
        query: LocalListQuery,
    ) -> Result<ListObjectsPage, Status> {
        loop {
            let page = gather_cluster_page(
                self.local_node,
                self.store.clone(),
                Arc::new(placement.clone()),
                self.peers.clone(),
                self.authorization.clone(),
                bearer.clone(),
                query.clone(),
            )
            .await?;

            // A cutover after one source responded must not turn an old ownership
            // set into a successful page.
            if self.placement()?.fence() != page.placement_fence {
                return Err(Status::unavailable(
                    "active placement changed while collecting the list page",
                ));
            }
            if crate::programs::atomic_tail_is_clear(&self.decisions)? {
                return Ok(page.page);
            }
            crate::programs::wait_for_atomic_tail(&self.decisions, Duration::from_secs(30)).await?;
        }
    }

    fn placement(&self) -> Result<ClusterPlacement, Status> {
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
        ClusterPlacement::from_applied(&state)
            .map_err(|error| Status::unavailable(error.to_string()))
    }
}

#[derive(Debug)]
struct ClusterPage {
    placement_fence: PlacementLogId,
    page: ListObjectsPage,
}

async fn gather_cluster_page(
    local_node: NodeId,
    store: Store,
    placement: Arc<dyn ListPlacementView>,
    peers: Arc<dyn ClusterListPeers>,
    authorization: Arc<dyn AuthoritativeListAuthorizer>,
    bearer: OriginalBearer,
    query: LocalListQuery,
) -> Result<ClusterPage, Status> {
    if query.placement_fence() != placement.fence() {
        return Err(Status::unavailable(
            "list request does not match the active placement fence",
        ));
    }
    let active = placement.active_node_ids();
    if !active.contains(&local_node) {
        return Err(Status::unavailable(
            "the ingress node is not ACTIVE in this placement",
        ));
    }

    let mut tasks = tokio::task::JoinSet::new();
    for source in active.iter().copied() {
        let query = query.clone();
        if source == local_node {
            let store = store.clone();
            let placement = placement.clone();
            let authorization = authorization.clone();
            let bearer = bearer.clone();
            tasks.spawn(async move {
                authorization.authorize(&bearer, &query).await?;
                tokio::task::spawn_blocking(move || {
                    local_owned_page(&store, source, placement.as_ref(), &query)
                })
                .await
                .map_err(|error| Status::internal(format!("local list worker failed: {error}")))?
            });
        } else {
            let address = placement.address(source).ok_or_else(|| {
                Status::unavailable(format!("ACTIVE node {} has no peer address", source.0))
            })?;
            let peers = peers.clone();
            let bearer = bearer.clone();
            tasks
                .spawn(async move { peers.list_local_page(source, &address, bearer, query).await });
        }
    }

    let mut sources = Vec::with_capacity(active.len());
    while let Some(joined) = tasks.join_next().await {
        let owned = joined
            .map_err(|error| Status::internal(format!("list source task failed: {error}")))??;
        if !active.contains(&owned.source_node()) {
            return Err(Status::unavailable(format!(
                "non-ACTIVE node {} returned a list page",
                owned.source_node().0
            )));
        }
        if owned.placement_fence() != query.placement_fence() {
            return Err(Status::unavailable(format!(
                "ACTIVE node {} returned a stale list fence",
                owned.source_node().0
            )));
        }
        if owned.page().paths.iter().any(|path| {
            path.len() > MAX_OBJECT_PATH_BYTES
                || !path.starts_with(query.prefix())
                || !path_is_allowed_for_query(&query, path)
        }) {
            return Err(Status::unavailable(format!(
                "ACTIVE node {} returned a path outside the requested public prefix",
                owned.source_node().0
            )));
        }
        sources.push(ActiveListSource::page(
            owned.source_node(),
            owned.into_page(),
        ));
    }

    let page = merge_active_list_pages(&active, query.start_after(), query.limit(), sources)
        .map_err(|error| Status::unavailable(error.to_string()))?;
    Ok(ClusterPage {
        placement_fence: query.placement_fence(),
        page,
    })
}

fn path_is_allowed_for_query(query: &LocalListQuery, path: &str) -> bool {
    if query.includes_index_definitions() {
        crate::index_runtime::publication::index_definition_name(path).is_some()
    } else if query.includes_personaldb_manifests() {
        crate::personaldb::parse_manifest_object_path(path).is_ok()
    } else {
        !path.split('/').any(|segment| segment == "_anvil")
    }
}

/// Produce one bounded page from one RocksDB snapshot and one exact placement.
/// This is the only storage implementation used by both the local fast path
/// and the private peer service.
fn local_owned_page(
    store: &Store,
    local_node: NodeId,
    placement: &dyn ListPlacementView,
    query: &LocalListQuery,
) -> Result<OwnedListPage, Status> {
    if placement.fence() != query.placement_fence() {
        return Err(Status::unavailable(
            "list source has another active placement fence",
        ));
    }
    if !placement.active_node_ids().contains(&local_node) {
        return Err(Status::unavailable("list source is not ACTIVE"));
    }
    let page = if query.includes_index_definitions() {
        store.list_local_owned_index_definitions(
            query.tenant_id(),
            query.bucket_id(),
            query.prefix(),
            query.start_after(),
            query.limit(),
            |tenant_id, bucket_id, path| {
                placement.object_owner(tenant_id, bucket_id, path) == Some(local_node)
            },
        )
    } else if query.includes_personaldb_manifests() {
        store.list_local_owned_personaldb_manifests(
            query.tenant_id(),
            query.bucket_id(),
            query.prefix(),
            query.start_after(),
            query.limit(),
            |tenant_id, bucket_id, path| {
                placement.object_owner(tenant_id, bucket_id, path) == Some(local_node)
            },
        )
    } else {
        store.list_local_owned_objects(
            query.tenant_id(),
            query.bucket_id(),
            query.prefix(),
            query.start_after(),
            query.limit(),
            |tenant_id, bucket_id, path| {
                placement.object_owner(tenant_id, bucket_id, path) == Some(local_node)
            },
        )
    }
    .map_err(|error| Status::failed_precondition(error.to_string()))?;
    Ok(OwnedListPage::new(local_node, placement.fence(), page))
}

fn object_placement_key(tenant_id: u64, bucket_id: u64, path: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(16 + path.len());
    key.extend_from_slice(&tenant_id.to_be_bytes());
    key.extend_from_slice(&bucket_id.to_be_bytes());
    key.extend_from_slice(path.as_bytes());
    key
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ActiveListSource {
    node_id: NodeId,
    outcome: ActiveListOutcome,
}

impl ActiveListSource {
    pub(crate) fn page(node_id: NodeId, page: ListObjectsPage) -> Self {
        Self {
            node_id,
            outcome: ActiveListOutcome::Page(page),
        }
    }

    pub(crate) fn unavailable(node_id: NodeId) -> Self {
        Self {
            node_id,
            outcome: ActiveListOutcome::Unavailable,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ActiveListOutcome {
    Page(ListObjectsPage),
    Unavailable,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum InvalidSourcePage {
    #[error("source returned more than 1000 paths")]
    TooManyPaths,
    #[error("source paths are not strictly byte-lexical and duplicate-free")]
    NotStrictlySorted,
    #[error("source returned a path at or before the exclusive start_after cursor")]
    BeforeCursor,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub(crate) enum DistributedListError {
    #[error("list limit {requested} is outside 1..=1000")]
    InvalidLimit { requested: usize },
    #[error("ACTIVE membership repeats node {node_id:?}")]
    DuplicateActiveMember { node_id: NodeId },
    #[error("received a list result from non-ACTIVE node {node_id:?}")]
    UnexpectedSource { node_id: NodeId },
    #[error("received more than one list result from ACTIVE node {node_id:?}")]
    DuplicateSource { node_id: NodeId },
    #[error("ACTIVE node {node_id:?} supplied no list result")]
    MissingSource { node_id: NodeId },
    #[error("ACTIVE node {node_id:?} could not produce its list page")]
    SourceUnavailable { node_id: NodeId },
    #[error("ACTIVE node {node_id:?} supplied an invalid list page: {violation}")]
    InvalidSource {
        node_id: NodeId,
        violation: InvalidSourcePage,
    },
    #[error("path {path:?} was claimed by contradictory owners {first:?} and {second:?}")]
    ContradictoryOwners {
        path: String,
        first: NodeId,
        second: NodeId,
    },
}

/// Merge one exact source result per ACTIVE member into one stateless page.
///
/// Every source page is a fresh read-committed observation for this request.
/// The function retains no state between calls, so a continuation deliberately
/// observes commits made after the previous page.
pub(crate) fn merge_active_list_pages(
    active_members: &[NodeId],
    start_after: Option<&str>,
    limit: usize,
    sources: Vec<ActiveListSource>,
) -> Result<ListObjectsPage, DistributedListError> {
    if !(1..=MAX_LIST_OBJECTS).contains(&limit) {
        return Err(DistributedListError::InvalidLimit { requested: limit });
    }

    let mut active = HashSet::with_capacity(active_members.len());
    for node_id in active_members {
        if !active.insert(*node_id) {
            return Err(DistributedListError::DuplicateActiveMember { node_id: *node_id });
        }
    }

    let mut received = HashMap::with_capacity(sources.len());
    for source in sources {
        if !active.contains(&source.node_id) {
            return Err(DistributedListError::UnexpectedSource {
                node_id: source.node_id,
            });
        }
        if received.insert(source.node_id, source.outcome).is_some() {
            return Err(DistributedListError::DuplicateSource {
                node_id: source.node_id,
            });
        }
    }

    let mut pages = Vec::with_capacity(active_members.len());
    for node_id in active_members {
        let outcome = received
            .remove(node_id)
            .ok_or(DistributedListError::MissingSource { node_id: *node_id })?;
        let ActiveListOutcome::Page(page) = outcome else {
            return Err(DistributedListError::SourceUnavailable { node_id: *node_id });
        };
        validate_source_page(*node_id, &page, start_after)?;
        pages.push((*node_id, page));
    }

    reject_contradictory_owners(&pages)?;
    Ok(merge_valid_pages(&pages, limit))
}

fn validate_source_page(
    node_id: NodeId,
    page: &ListObjectsPage,
    start_after: Option<&str>,
) -> Result<(), DistributedListError> {
    let violation = if page.paths.len() > MAX_LIST_OBJECTS {
        Some(InvalidSourcePage::TooManyPaths)
    } else if page
        .paths
        .windows(2)
        .any(|pair| pair[0].as_bytes() >= pair[1].as_bytes())
    {
        Some(InvalidSourcePage::NotStrictlySorted)
    } else if start_after.is_some_and(|cursor| {
        page.paths
            .iter()
            .any(|path| path.as_bytes() <= cursor.as_bytes())
    }) {
        Some(InvalidSourcePage::BeforeCursor)
    } else {
        None
    };
    match violation {
        Some(violation) => Err(DistributedListError::InvalidSource { node_id, violation }),
        None => Ok(()),
    }
}

fn reject_contradictory_owners(
    pages: &[(NodeId, ListObjectsPage)],
) -> Result<(), DistributedListError> {
    let path_count = pages.iter().map(|(_, page)| page.paths.len()).sum();
    let mut owners = HashMap::<&str, NodeId>::with_capacity(path_count);
    for (node_id, page) in pages {
        for path in &page.paths {
            if let Some(first) = owners.insert(path.as_str(), *node_id) {
                return Err(DistributedListError::ContradictoryOwners {
                    path: path.clone(),
                    first,
                    second: *node_id,
                });
            }
        }
    }
    Ok(())
}

fn merge_valid_pages(pages: &[(NodeId, ListObjectsPage)], limit: usize) -> ListObjectsPage {
    let mut consumed = vec![0_usize; pages.len()];
    let mut heap = BinaryHeap::with_capacity(pages.len());
    for (source, (_, page)) in pages.iter().enumerate() {
        if let Some(path) = page.paths.first() {
            heap.push(MergeCandidate {
                path: path.as_str(),
                source,
            });
        }
    }

    let mut paths = Vec::with_capacity(limit);
    while paths.len() < limit {
        let Some(candidate) = heap.pop() else {
            break;
        };
        paths.push(candidate.path.to_owned());
        consumed[candidate.source] += 1;
        let next = consumed[candidate.source];
        if let Some(path) = pages[candidate.source].1.paths.get(next) {
            heap.push(MergeCandidate {
                path: path.as_str(),
                source: candidate.source,
            });
        }
    }

    let has_more = pages
        .iter()
        .enumerate()
        .any(|(source, (_, page))| page.has_more || consumed[source] < page.paths.len());
    ListObjectsPage { paths, has_more }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MergeCandidate<'a> {
    path: &'a str,
    source: usize,
}

impl Ord for MergeCandidate<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .path
            .as_bytes()
            .cmp(self.path.as_bytes())
            .then_with(|| other.source.cmp(&self.source))
    }
}

impl PartialOrd for MergeCandidate<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::num::NonZeroU32;
    use std::path::Path;
    use std::sync::Mutex;

    use anvil_authz::{ObjectRef, Tuple};
    use anvil_consensus::{ClusterId, PeerAddress};
    use anvil_store::{
        AuthzRevision, CreateBucketRequest, DeleteRequest, Durability, ObjectKey, ObjectVersioning,
        Precondition, ProvisionTenantRequest, PutMode, PutRequest, StorageTenantId, StoreOptions,
        SystemBootstrapRequest, TupleBatchRequest, TupleMutation, TupleMutationKind,
    };

    use crate::placement::{PlacementNode, rank_nodes};

    use super::*;

    fn node(id: u64) -> NodeId {
        NodeId(id)
    }

    fn page(id: u64, paths: &[&str], has_more: bool) -> ActiveListSource {
        ActiveListSource::page(
            node(id),
            ListObjectsPage {
                paths: paths.iter().map(|path| (*path).to_owned()).collect(),
                has_more,
            },
        )
    }

    struct AllowList;

    #[tonic::async_trait]
    impl AuthoritativeListAuthorizer for AllowList {
        async fn authorize(
            &self,
            _bearer: &OriginalBearer,
            _query: &LocalListQuery,
        ) -> Result<(), Status> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct CaptureListAuthorization {
        seen: Mutex<Vec<(StorageTenantId, ObjectRef)>>,
    }

    #[tonic::async_trait]
    impl TenantAuthorizationCoordinator for CaptureListAuthorization {
        async fn allows(
            &self,
            storage_tenant: StorageTenantId,
            _realm: RealmId,
            subject: ObjectRef,
            _permission: ListAuthorizationPermission,
            _placement_fence: PlacementLogId,
        ) -> Result<bool, Status> {
            self.seen.lock().unwrap().push((storage_tenant, subject));
            Ok(true)
        }
    }

    #[tokio::test]
    async fn late_bound_authorizer_fails_closed_then_installs_once() {
        let authorizer = LateBoundListAuthorizer::default();
        let bearer = OriginalBearer::from_signed_token("signed.jwt");
        let query = LocalListQuery::new(
            PlacementLogId { term: 1, index: 2 },
            "tenant",
            "bucket",
            3,
            4,
            "",
            None,
            10,
        )
        .unwrap();

        assert_eq!(
            authorizer
                .authorize(&bearer, &query)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unavailable
        );
        assert!(authorizer.install(Arc::new(AllowList)).is_ok());
        authorizer.authorize(&bearer, &query).await.unwrap();
        assert!(authorizer.install(Arc::new(AllowList)).is_err());
    }

    #[test]
    fn original_bearer_is_separate_and_requires_one_canonical_header() {
        let mut metadata = MetadataMap::new();
        metadata.insert("authorization", "Bearer signed.jwt.value".parse().unwrap());
        let bearer = OriginalBearer::from_metadata(&metadata).unwrap();
        assert_eq!(bearer.signed_token(), "signed.jwt.value");

        metadata.append("authorization", "Bearer another.jwt".parse().unwrap());
        let duplicate = match OriginalBearer::from_metadata(&metadata) {
            Ok(_) => panic!("duplicate bearer metadata must be rejected"),
            Err(error) => error,
        };
        assert_eq!(duplicate.code(), tonic::Code::Unauthenticated);
        let mut malformed = MetadataMap::new();
        malformed.insert("authorization", "signed.jwt.value".parse().unwrap());
        let malformed = match OriginalBearer::from_metadata(&malformed) {
            Ok(_) => panic!("a non-Bearer authorization scheme must be rejected"),
            Err(error) => error,
        };
        assert_eq!(malformed.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn anonymous_list_still_runs_the_source_side_zanzibar_check() {
        let captured = Arc::new(CaptureListAuthorization::default());
        let authorizer = CoordinatedListAuthorizer::new(
            JwtManager::new(b"anonymous-list-secret-0123456789abcdef").unwrap(),
            captured.clone(),
        );
        let query = LocalListQuery::new(
            PlacementLogId { term: 1, index: 2 },
            "tenant",
            "bucket",
            3,
            4,
            "",
            None,
            10,
        )
        .unwrap();

        let bearer = OriginalBearer::anonymous();
        assert!(bearer.is_anonymous());
        authorizer.authorize(&bearer, &query).await.unwrap();

        assert_eq!(
            captured.seen.lock().unwrap().as_slice(),
            &[(
                StorageTenantId::parse("tenant").unwrap(),
                ObjectRef::anonymous(),
            )]
        );
    }

    #[test]
    fn merges_every_active_source_in_global_byte_lexical_order() {
        let merged = merge_active_list_pages(
            &[node(1), node(2), node(3)],
            Some("a"),
            10,
            vec![
                page(3, &["d", "z"], false),
                page(1, &["b", "f"], false),
                page(2, &["c", "g"], false),
            ],
        )
        .unwrap();

        assert_eq!(merged.paths, ["b", "c", "d", "f", "g", "z"]);
        assert!(!merged.has_more);
    }

    #[test]
    fn missing_active_source_makes_the_page_unavailable() {
        assert_eq!(
            merge_active_list_pages(&[node(1), node(2)], None, 10, vec![page(1, &["a"], false)],),
            Err(DistributedListError::MissingSource { node_id: node(2) })
        );
    }

    #[test]
    fn explicit_source_error_makes_the_page_unavailable() {
        assert_eq!(
            merge_active_list_pages(
                &[node(1), node(2)],
                None,
                10,
                vec![
                    page(1, &["a"], false),
                    ActiveListSource::unavailable(node(2)),
                ],
            ),
            Err(DistributedListError::SourceUnavailable { node_id: node(2) })
        );
    }

    #[test]
    fn identical_path_from_two_sources_is_never_deduplicated() {
        assert_eq!(
            merge_active_list_pages(
                &[node(4), node(9)],
                None,
                10,
                vec![
                    page(4, &["same/path"], false),
                    page(9, &["same/path"], false)
                ],
            ),
            Err(DistributedListError::ContradictoryOwners {
                path: "same/path".into(),
                first: node(4),
                second: node(9),
            })
        );
    }

    #[test]
    fn source_output_must_be_sorted_unique_and_after_the_cursor() {
        assert_eq!(
            merge_active_list_pages(&[node(1)], None, 10, vec![page(1, &["b", "a"], false)],),
            Err(DistributedListError::InvalidSource {
                node_id: node(1),
                violation: InvalidSourcePage::NotStrictlySorted,
            })
        );
        assert_eq!(
            merge_active_list_pages(&[node(1)], Some("a"), 10, vec![page(1, &["a", "b"], false)],),
            Err(DistributedListError::InvalidSource {
                node_id: node(1),
                violation: InvalidSourcePage::BeforeCursor,
            })
        );
    }

    #[test]
    fn unconsumed_items_or_a_source_tail_set_has_more() {
        let unconsumed = merge_active_list_pages(
            &[node(1), node(2)],
            None,
            1,
            vec![page(1, &["a"], false), page(2, &["b"], false)],
        )
        .unwrap();
        assert_eq!(unconsumed.paths, ["a"]);
        assert!(unconsumed.has_more);

        let source_tail = merge_active_list_pages(
            &[node(1), node(2)],
            None,
            10,
            vec![page(1, &["a"], false), page(2, &[], true)],
        )
        .unwrap();
        assert_eq!(source_tail.paths, ["a"]);
        assert!(source_tail.has_more);
    }

    #[test]
    fn continuations_pass_two_thousand_without_an_arbitrary_total_cap() {
        let active = [node(1), node(2)];
        let mut start_after = None::<String>;
        let mut all = Vec::new();
        let mut page_lengths = Vec::new();

        loop {
            let sources = active
                .iter()
                .map(|owner| generated_source_page(*owner, start_after.as_deref()))
                .collect();
            let merged =
                merge_active_list_pages(&active, start_after.as_deref(), MAX_LIST_OBJECTS, sources)
                    .unwrap();
            page_lengths.push(merged.paths.len());
            all.extend(merged.paths.iter().cloned());
            if !merged.has_more {
                break;
            }
            start_after = merged.paths.last().cloned();
        }

        assert_eq!(page_lengths, [1_000, 1_000, 5]);
        assert_eq!(all.len(), 2_005);
        assert_eq!(all.first().map(String::as_str), Some("item/0000"));
        assert_eq!(all.last().map(String::as_str), Some("item/2004"));
        assert!(all.windows(2).all(|pair| pair[0] < pair[1]));
    }

    fn generated_source_page(owner: NodeId, start_after: Option<&str>) -> ActiveListSource {
        let mut current = (0..2_005)
            .filter(|index| node(1 + (*index as u64 % 2)) == owner)
            .map(|index| format!("item/{index:04}"))
            .filter(|path| start_after.is_none_or(|cursor| path.as_str() > cursor));
        let paths = current.by_ref().take(MAX_LIST_OBJECTS).collect::<Vec<_>>();
        let has_more = current.next().is_some();
        ActiveListSource::page(owner, ListObjectsPage { paths, has_more })
    }

    #[test]
    fn every_page_is_a_fresh_read_committed_merge() {
        let active = [node(1), node(2)];
        let first = merge_active_list_pages(
            &active,
            None,
            1,
            vec![page(1, &["a"], false), page(2, &["c"], false)],
        )
        .unwrap();
        assert_eq!(first.paths, ["a"]);
        assert!(first.has_more);

        // Between requests, `b` commits and `c` is deleted. The continuation
        // uses only its exclusive path cursor and the new source observations.
        let second = merge_active_list_pages(
            &active,
            Some("a"),
            10,
            vec![page(1, &["b"], false), page(2, &[], false)],
        )
        .unwrap();
        assert_eq!(second.paths, ["b"]);
        assert!(!second.has_more);
    }

    #[derive(Clone)]
    struct FixedPlacement {
        cluster_id: ClusterId,
        fence: PlacementLogId,
        nodes: Vec<PlacementNode>,
        addresses: BTreeMap<NodeId, PeerAddress>,
    }

    impl FixedPlacement {
        fn three_nodes() -> Self {
            let node_ids = [node(1), node(2), node(3)];
            Self {
                cluster_id: ClusterId(*b"list-cluster-tst"),
                fence: PlacementLogId { term: 4, index: 9 },
                nodes: node_ids
                    .into_iter()
                    .map(|node_id| PlacementNode::new(node_id, NonZeroU32::new(1_000_000).unwrap()))
                    .collect(),
                addresses: node_ids
                    .into_iter()
                    .map(|node_id| (node_id, PeerAddress(format!("node-{}:50052", node_id.0))))
                    .collect(),
            }
        }
    }

    impl ListPlacementView for FixedPlacement {
        fn fence(&self) -> PlacementLogId {
            self.fence
        }

        fn active_node_ids(&self) -> Vec<NodeId> {
            self.nodes
                .iter()
                .map(|candidate| candidate.node_id())
                .collect()
        }

        fn address(&self, node_id: NodeId) -> Option<String> {
            self.addresses
                .get(&node_id)
                .map(|address| address.0.clone())
        }

        fn object_owner(&self, tenant_id: u64, bucket_id: u64, path: &str) -> Option<NodeId> {
            rank_nodes(
                PlacementKind::Object,
                self.cluster_id,
                &object_placement_key(tenant_id, bucket_id, path),
                &self.nodes,
            )
            .first()
            .map(|candidate| candidate.node_id())
        }
    }

    impl ListFenceAuthority for FixedPlacement {
        fn require_fence(&self, expected: PlacementLogId) -> Result<(), Status> {
            if self.fence == expected {
                Ok(())
            } else {
                Err(Status::unavailable("test serving fence is stale"))
            }
        }
    }

    #[derive(Clone, Copy)]
    enum PeerBehavior {
        Normal,
        Fail(NodeId),
        StaleFence(NodeId),
    }

    struct InProcessListPeers {
        stores: BTreeMap<NodeId, Store>,
        placement: Arc<FixedPlacement>,
        authorizers: BTreeMap<NodeId, Arc<dyn AuthoritativeListAuthorizer>>,
        behavior: PeerBehavior,
        calls: Mutex<Vec<NodeId>>,
    }

    impl InProcessListPeers {
        fn new(
            stores: BTreeMap<NodeId, Store>,
            placement: Arc<FixedPlacement>,
            tokens: JwtManager,
            behavior: PeerBehavior,
        ) -> Self {
            let authorizers = stores
                .iter()
                .map(|(node_id, store)| {
                    let fence: Arc<dyn ListFenceAuthority> = placement.clone();
                    let coordinator: Arc<dyn TenantAuthorizationCoordinator> = Arc::new(
                        LocalListAuthorizationCoordinator::with_test_fence(store.clone(), fence),
                    );
                    let authorizer: Arc<dyn AuthoritativeListAuthorizer> =
                        Arc::new(CoordinatedListAuthorizer::new(tokens.clone(), coordinator));
                    (*node_id, authorizer)
                })
                .collect();
            Self {
                stores,
                placement,
                authorizers,
                behavior,
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    #[tonic::async_trait]
    impl ClusterListPeers for InProcessListPeers {
        async fn list_local_page(
            &self,
            target: NodeId,
            address: &str,
            bearer: OriginalBearer,
            query: LocalListQuery,
        ) -> Result<OwnedListPage, Status> {
            self.calls.lock().unwrap().push(target);
            assert_eq!(
                self.placement.address(target).as_deref(),
                Some(address),
                "aggregator must use the fenced ACTIVE peer address"
            );
            if matches!(self.behavior, PeerBehavior::Fail(node_id) if node_id == target) {
                return Err(Status::unavailable("injected peer failure"));
            }
            self.authorizers[&target].authorize(&bearer, &query).await?;
            let store = self.stores.get(&target).unwrap();
            let mut page = local_owned_page(store, target, self.placement.as_ref(), &query)?;
            if matches!(self.behavior, PeerBehavior::StaleFence(node_id) if node_id == target) {
                page.placement_fence.index -= 1;
            }
            Ok(page)
        }
    }

    #[tokio::test]
    async fn separate_replica_stores_emit_one_owner_across_stateless_pages() {
        let (directories, stores, tenant_id, bucket_id, tokens, bearer) = replicated_stores().await;
        let placement = Arc::new(FixedPlacement::three_nodes());
        let peers = Arc::new(InProcessListPeers::new(
            stores.clone(),
            placement.clone(),
            tokens,
            PeerBehavior::Normal,
        ));
        let ingress_authorizer = peers.authorizers[&node(1)].clone();
        let mut start_after = None::<String>;
        let mut listed = Vec::new();

        loop {
            let query = LocalListQuery::new(
                placement.fence(),
                "tenant",
                "bucket",
                tenant_id,
                bucket_id,
                "",
                start_after.clone(),
                3,
            )
            .unwrap();
            let page = gather_cluster_page(
                node(1),
                stores[&node(1)].clone(),
                placement.clone(),
                peers.clone(),
                ingress_authorizer.clone(),
                bearer.clone(),
                query,
            )
            .await
            .unwrap()
            .page;
            listed.extend(page.paths.iter().cloned());
            if !page.has_more {
                break;
            }
            start_after = page.paths.last().cloned();
        }

        assert_eq!(
            listed,
            [
                "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf"
            ]
        );
        let calls = peers.calls.lock().unwrap();
        assert_eq!(calls.len(), 6);
        assert_eq!(calls.iter().filter(|source| **source == node(2)).count(), 3);
        assert_eq!(calls.iter().filter(|source| **source == node(3)).count(), 3);
        drop(calls);
        drop(peers);
        drop(stores);
        drop(directories);
    }

    #[tokio::test]
    async fn any_peer_failure_or_stale_fence_fails_the_whole_page() {
        let (directories, stores, tenant_id, bucket_id, tokens, bearer) = replicated_stores().await;
        let placement = Arc::new(FixedPlacement::three_nodes());
        for behavior in [
            PeerBehavior::Fail(node(2)),
            PeerBehavior::StaleFence(node(3)),
        ] {
            let peers = Arc::new(InProcessListPeers::new(
                stores.clone(),
                placement.clone(),
                tokens.clone(),
                behavior,
            ));
            let query = LocalListQuery::new(
                placement.fence(),
                "tenant",
                "bucket",
                tenant_id,
                bucket_id,
                "",
                None,
                10,
            )
            .unwrap();
            let error = gather_cluster_page(
                node(1),
                stores[&node(1)].clone(),
                placement.clone(),
                peers.clone(),
                peers.authorizers[&node(1)].clone(),
                bearer.clone(),
                query,
            )
            .await
            .unwrap_err();
            assert_eq!(error.code(), tonic::Code::Unavailable);
        }
        drop(stores);
        drop(directories);
    }

    #[tokio::test]
    async fn every_source_rechecks_jwt_current_grant_and_fence() {
        let (directories, stores, tenant_id, bucket_id, tokens, bearer) = replicated_stores().await;
        let placement = Arc::new(FixedPlacement::three_nodes());
        let fence: Arc<dyn ListFenceAuthority> = placement.clone();
        let coordinator: Arc<dyn TenantAuthorizationCoordinator> = Arc::new(
            LocalListAuthorizationCoordinator::with_test_fence(stores[&node(2)].clone(), fence),
        );
        let authorizer = CoordinatedListAuthorizer::new(tokens.clone(), coordinator);
        let query = LocalListQuery::new(
            placement.fence(),
            "tenant",
            "bucket",
            tenant_id,
            bucket_id,
            "",
            None,
            10,
        )
        .unwrap();
        authorizer.authorize(&bearer, &query).await.unwrap();

        let wrong_identity = LocalListQuery::new(
            placement.fence(),
            "tenant",
            "bucket",
            tenant_id,
            bucket_id + 1,
            "",
            None,
            10,
        )
        .unwrap();
        assert_eq!(
            authorizer
                .authorize(&bearer, &wrong_identity)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unavailable
        );

        let invalid = OriginalBearer::from_signed_token("not-a-signed-jwt");
        assert_eq!(
            authorizer
                .authorize(&invalid, &query)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        let denied = OriginalBearer::from_signed_token(
            tokens
                .mint(
                    StorageTenantId::parse("tenant").unwrap(),
                    "unprivileged-app",
                )
                .unwrap(),
        );
        assert_eq!(
            authorizer
                .authorize(&denied, &query)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );

        let store = &stores[&node(2)];
        let repository = store.authz();
        let current = SystemAuthorizer::new(repository.clone()).load().unwrap();
        let owner = ObjectRef::opaque("app", "owner-app").unwrap();
        repository
            .mutate_tuples(TupleBatchRequest {
                scope: anvil_store::AuthzScope::system(),
                principal: owner.clone(),
                expected_revision: Some(current.revision),
                expected_binding_generation: current.binding_generation,
                operation_id: Some("revoke-list-owner".into()),
                mutations: vec![TupleMutation {
                    kind: TupleMutationKind::Remove,
                    tuple: Tuple::new(
                        crate::authorization::bucket_resource("tenant", "bucket").unwrap(),
                        "owner",
                        owner,
                    ),
                }],
            })
            .unwrap();
        assert_eq!(
            authorizer
                .authorize(&bearer, &query)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );

        let mut stale = (*placement).clone();
        stale.fence.index -= 1;
        let stale_coordinator: Arc<dyn TenantAuthorizationCoordinator> = Arc::new(
            LocalListAuthorizationCoordinator::with_test_fence(store.clone(), Arc::new(stale)),
        );
        let stale_authorizer = CoordinatedListAuthorizer::new(tokens, stale_coordinator);
        assert_eq!(
            stale_authorizer
                .authorize(&bearer, &query)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unavailable
        );
        drop(stores);
        drop(directories);
    }

    async fn replicated_stores() -> (
        Vec<tempfile::TempDir>,
        BTreeMap<NodeId, Store>,
        u64,
        u64,
        JwtManager,
        OriginalBearer,
    ) {
        let template = tempfile::tempdir().unwrap();
        let template_store = Store::open(StoreOptions::new(template.path(), 17))
            .await
            .unwrap();
        template_store
            .bootstrap_system(SystemBootstrapRequest {
                app_id: "bootstrap-app".into(),
                client_id: "bootstrap-client".into(),
                client_secret: "bootstrap-secret-0123456789abcdef0123456789abcdef".into(),
            })
            .unwrap();
        let tenant = StorageTenantId::parse("tenant").unwrap();
        let owner = ObjectRef::opaque("app", "owner-app").unwrap();
        template_store
            .provision_tenant(ProvisionTenantRequest {
                storage_tenant: tenant.clone(),
                owner_app_id: "owner-app".into(),
                owner_client_id: "owner-client".into(),
                owner_client_secret: "owner-secret-0123456789abcdef0123456789abcdef".into(),
                principal: ObjectRef::opaque("app", "bootstrap-app").unwrap(),
                expected_authorization_revision: AuthzRevision(3),
                expected_binding_generation: 1,
            })
            .unwrap();
        template_store
            .create_bucket(CreateBucketRequest {
                storage_tenant: tenant.clone(),
                bucket: "bucket".into(),
                owner: owner.clone(),
                principal: owner,
                expected_authorization_revision: AuthzRevision(4),
                expected_binding_generation: 1,
                versioning: ObjectVersioning::Unversioned,
            })
            .unwrap();
        let tokens = JwtManager::new(b"list-jwt-secret-0123456789abcdef0123456789abcdef").unwrap();
        let bearer = OriginalBearer::from_signed_token(tokens.mint(tenant, "owner-app").unwrap());
        for (index, path) in [
            "golf",
            "alpha",
            "charlie",
            "bravo",
            "echo",
            "delta",
            "foxtrot",
            "gone",
            "_anvil",
            "folder/_anvil/meta.json",
        ]
        .into_iter()
        .enumerate()
        {
            template_store
                .put(PutRequest {
                    key: ObjectKey::new("tenant", "bucket", path).unwrap(),
                    bytes: path.as_bytes().to_vec(),
                    content_type: None,
                    mode: PutMode::PutIfAbsent,
                    command_id: Some(format!("seed-{index}")),
                    durability: Durability::Local,
                })
                .await
                .unwrap();
        }
        template_store
            .delete(DeleteRequest {
                key: ObjectKey::new("tenant", "bucket", "gone").unwrap(),
                precondition: Precondition::Any,
                command_id: Some("seed-delete".into()),
                durability: Durability::Local,
            })
            .await
            .unwrap();
        let (tenant_id, bucket_id) = template_store
            .resolve_bucket_ids("tenant", "bucket")
            .unwrap();
        drop(template_store);

        let mut directories = Vec::new();
        let mut stores = BTreeMap::new();
        for node_id in [node(1), node(2), node(3)] {
            let directory = tempfile::tempdir().unwrap();
            copy_tree(template.path(), directory.path());
            let store = Store::open(StoreOptions::new(directory.path(), 17))
                .await
                .unwrap();
            assert_eq!(
                store.resolve_bucket_ids("tenant", "bucket").unwrap(),
                (tenant_id, bucket_id)
            );
            directories.push(directory);
            stores.insert(node_id, store);
        }
        directories.push(template);
        (directories, stores, tenant_id, bucket_id, tokens, bearer)
    }

    fn copy_tree(source: &Path, destination: &Path) {
        for entry in fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let target = destination.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                fs::create_dir(&target).unwrap();
                copy_tree(&entry.path(), &target);
            } else {
                fs::copy(entry.path(), target).unwrap();
            }
        }
    }
}
