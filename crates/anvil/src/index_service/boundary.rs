//! Narrow boundaries between the public index API and the clustered runtime.
//!
//! The service owns request validation, ordinary-object lifecycle calls,
//! Zanzibar filtering, and opaque page tokens. Implementations behind these
//! traits own only clustered definition discovery and generation execution.

use std::sync::Arc;
use std::time::Duration;

use anvil_api::v1::{IndexDefinition, IndexFreshness, IndexQuery, IndexQueryHit};
use anvil_store::ObjectKey;
use tonic::Status;
use tonic::metadata::MetadataMap;

use crate::authentication::Caller;
use crate::authorization::ObjectPermission;
use crate::distributed_list::OriginalBearer;

/// Authorized context retained when the public service calls into a query
/// replica. The original signed token or fixed anonymous marker, rather than a
/// serialized `Caller`, crosses the mandatory-mTLS listener. A remote node
/// reconstructs identity and evaluates Zanzibar independently.
#[derive(Clone)]
pub(crate) struct IndexRequestContext {
    caller: Caller,
    bearer: OriginalBearer,
    metadata: MetadataMap,
    deadline: tokio::time::Instant,
}

impl IndexRequestContext {
    pub(crate) fn new(
        caller: Caller,
        bearer: OriginalBearer,
        metadata: MetadataMap,
        deadline: tokio::time::Instant,
    ) -> Self {
        Self {
            caller,
            bearer,
            metadata,
            deadline,
        }
    }

    pub(crate) fn caller(&self) -> &Caller {
        &self.caller
    }

    pub(crate) fn routed_bearer(&self) -> &str {
        self.bearer.signed_token()
    }

    pub(crate) fn bearer(&self) -> OriginalBearer {
        self.bearer.clone()
    }

    pub(crate) fn metadata(&self) -> &MetadataMap {
        &self.metadata
    }

    pub(crate) fn remaining(&self) -> Result<std::time::Duration, Status> {
        crate::v05::deadline_remaining(self.deadline)
    }
}

/// One definition name returned by a scoped ordinary-object prefix listing.
/// The public service exact-reads and authorizes the ordinary object before
/// returning any definition content.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ListedIndexDefinition {
    pub(crate) name: String,
}

#[derive(Clone)]
pub(crate) struct IndexDefinitionScan {
    pub(crate) bearer: OriginalBearer,
    pub(crate) tenant: String,
    pub(crate) bucket: String,
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) start_after_name: Option<String>,
    pub(crate) limit: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexDefinitionScanPage {
    pub(crate) definitions: Vec<ListedIndexDefinition>,
    pub(crate) has_more: bool,
}

#[tonic::async_trait]
pub(crate) trait IndexDefinitionLister: Send + Sync + 'static {
    async fn scan(&self, request: IndexDefinitionScan) -> Result<IndexDefinitionScanPage, Status>;
}

/// Evidence from one fresh, exact-revision Zanzibar evaluation. The service
/// uses the revision both in response freshness and in the next-page token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexAuthorizationEvidence {
    pub(crate) allowed: Vec<bool>,
    pub(crate) revision: u64,
}

#[tonic::async_trait]
pub(crate) trait IndexAuthorization: Send + Sync + 'static {
    async fn allows_objects_with_evidence(
        &self,
        caller: &Caller,
        requests: &[(ObjectKey, ObjectPermission)],
    ) -> Result<IndexAuthorizationEvidence, Status>;
}

#[tonic::async_trait]
pub(crate) trait IndexDefinitionReader: Send + Sync + 'static {
    async fn current_snapshot(
        &self,
        key: &ObjectKey,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Option<anvil_store::ObjectPathSnapshot>, Status>;
}

#[tonic::async_trait]
pub(crate) trait IndexLiveVersionReader: Send + Sync + 'static {
    async fn current_snapshots(
        &self,
        keys: &[ObjectKey],
        tenant_id: u64,
        bucket_id: u64,
        budget: Duration,
    ) -> Result<Vec<Option<anvil_store::CurrentObjectSnapshot>>, Status>;
}

#[tonic::async_trait]
impl IndexLiveVersionReader for crate::cluster_object_read::ClusterObjectReader {
    async fn current_snapshots(
        &self,
        keys: &[ObjectKey],
        tenant_id: u64,
        bucket_id: u64,
        budget: Duration,
    ) -> Result<Vec<Option<anvil_store::CurrentObjectSnapshot>>, Status> {
        self.current_head_snapshots_stable(keys, tenant_id, bucket_id, budget)
            .await
    }
}

#[tonic::async_trait]
impl IndexDefinitionReader for crate::cluster_object_read::ClusterObjectReader {
    async fn current_snapshot(
        &self,
        key: &ObjectKey,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Option<anvil_store::ObjectPathSnapshot>, Status> {
        self.current_snapshot_stable(key, tenant_id, bucket_id)
            .await
    }
}

/// Immutable values to which every opaque query page token is bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IndexPageTokenBinding {
    pub(crate) index_id: u64,
    pub(crate) definition_version: u64,
    pub(crate) query_hash: [u8; 32],
}

/// Mutable cursor evidence carried by a valid page token. A continuation is
/// always pinned to one immutable generation and one Zanzibar revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexPageCursor {
    pub(crate) generation: u64,
    pub(crate) last_position: Vec<u8>,
    pub(crate) authorization_revision: u64,
}

/// Opaque token codec. Its production implementation is backed by the same
/// JWT key as other Anvil capabilities and must bind the complete `Caller`.
pub(crate) trait IndexPageTokenCodec: Send + Sync + 'static {
    fn decode(
        &self,
        caller: &Caller,
        token: &[u8],
        expected: IndexPageTokenBinding,
    ) -> Result<IndexPageCursor, Status>;

    fn encode(
        &self,
        caller: &Caller,
        binding: IndexPageTokenBinding,
        cursor: &IndexPageCursor,
    ) -> Result<Vec<u8>, Status>;
}

#[derive(Clone)]
pub(crate) struct ExecuteIndexQuery {
    pub(crate) context: IndexRequestContext,
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) definition: IndexDefinition,
    pub(crate) query: IndexQuery,
    pub(crate) limit: usize,
    /// `None` selects the latest published generation. A continuation supplies
    /// the exact immutable generation and engine-specific last position.
    pub(crate) resume: Option<IndexPageCursor>,
}

#[derive(Clone, Debug)]
pub(crate) struct ExecutedIndexQuery {
    pub(crate) hits: Vec<IndexQueryHit>,
    pub(crate) freshness: IndexFreshness,
    /// Engine-specific stable position. A local engine position is private
    /// until authorization-aware pagination replaces it with the position
    /// following a returned, Zanzibar-authorized hit.
    pub(crate) next_position: Option<Vec<u8>>,
}

#[tonic::async_trait]
pub(crate) trait IndexQueryExecutor: Send + Sync + 'static {
    async fn execute(&self, request: ExecuteIndexQuery) -> Result<ExecutedIndexQuery, Status>;
}

#[derive(Clone)]
pub(crate) struct IndexServiceDependencies {
    pub(crate) definitions: Arc<dyn IndexDefinitionLister>,
    pub(crate) queries: Arc<dyn IndexQueryExecutor>,
    pub(crate) authorization: Arc<dyn IndexAuthorization>,
    pub(crate) page_tokens: Arc<dyn IndexPageTokenCodec>,
    pub(crate) definition_reader: Arc<dyn IndexDefinitionReader>,
    pub(crate) live_versions: Arc<dyn IndexLiveVersionReader>,
}
