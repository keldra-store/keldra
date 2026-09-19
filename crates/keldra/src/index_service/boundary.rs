//! Narrow boundaries between the public index API and the clustered runtime.
//!
//! The service owns request validation, definition admission, ordinary-object
//! lifecycle calls, and opaque page tokens. Local revision execution owns
//! mandatory candidate Zanzibar checks over snapshot-current gates through the supplied
//! visibility boundary; it cannot return a page through optional post-filtering.

use std::sync::Arc;

use keldra_api::v1::{
    IndexAggregateResult, IndexDefinition, IndexFacetResult, IndexFreshness, IndexQuery,
    IndexQueryHit,
};
use keldra_authz::ObjectRef;
use keldra_store::{AuthzScope, ObjectKey, SchemaRef};
use serde::{Deserialize, Serialize};
use tonic::Status;
use tonic::metadata::MetadataMap;

use crate::authentication::{Caller, PluginObjectScope};
use crate::authorization::ObjectPermission;
use crate::distributed_list::OriginalBearer;

use super::candidate_visibility::IndexCandidateVisibility;

/// Authorized context retained when the public service calls into a query
/// replica. The original signed token or fixed anonymous marker, rather than a
/// serialized `Caller`, crosses the mandatory-mTLS listener. A remote node
/// reconstructs identity and evaluates Zanzibar independently.
#[derive(Clone)]
pub(crate) struct IndexRequestContext {
    caller: Caller,
    bearer: OriginalBearer,
    metadata: MetadataMap,
    plugin_scope: Option<PluginObjectScope>,
    deadline: tokio::time::Instant,
}

impl IndexRequestContext {
    pub(crate) fn new(
        caller: Caller,
        bearer: OriginalBearer,
        metadata: MetadataMap,
        plugin_scope: Option<PluginObjectScope>,
        deadline: tokio::time::Instant,
    ) -> Self {
        Self {
            caller,
            bearer,
            metadata,
            plugin_scope,
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

    pub(crate) fn plugin_scope(&self) -> Option<&PluginObjectScope> {
        self.plugin_scope.as_ref()
    }

    pub(crate) fn remaining(&self) -> Result<std::time::Duration, Status> {
        crate::object_service::deadline_remaining(self.deadline)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RequiredIndexSourceCheckpoint {
    pub(crate) node_id: u64,
    pub(crate) source_epoch: [u8; 32],
    pub(crate) next_offset: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexFreshnessRequirement {
    pub(crate) sources: Vec<RequiredIndexSourceCheckpoint>,
    pub(crate) atomic_through: Option<u64>,
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

/// Definition-owned result policy. Keeping this as a validated domain value
/// prevents public and routed query paths from interpreting protobuf defaults
/// differently.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum IndexResultAuthorizationPolicy {
    Application,
    Realm(RealmIndexResultAuthorizationPolicy),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum RealmIndexAuthorizationTarget {
    CanonicalSourcePath,
    ResultPath,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct RealmIndexResultAuthorizationPolicy {
    pub(crate) realm: String,
    pub(crate) resource_namespace: String,
    pub(crate) relation: String,
    pub(crate) target: RealmIndexAuthorizationTarget,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct IndexRealmAuthorizationEvidence {
    pub(crate) revision: u64,
    pub(crate) binding_generation: u64,
    pub(crate) schema_ref: SchemaRef,
}

/// Complete immutable Zanzibar evidence for one query. The protected system
/// revision always exists; custom-realm evidence exists only for definitions
/// which opt into end-user filtering.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexQueryAuthorizationEvidence {
    pub(crate) system_revision: u64,
    pub(crate) result: Option<IndexRealmAuthorizationEvidence>,
}

#[tonic::async_trait]
pub(crate) trait IndexAuthorization: Send + Sync + 'static {
    async fn allows_objects_with_evidence(
        &self,
        caller: &Caller,
        requests: &[(ObjectKey, ObjectPermission)],
    ) -> Result<IndexAuthorizationEvidence, Status>;

    /// Evaluate protected application/object access at exactly the revision
    /// established when the query was admitted. Production implementations
    /// must read that immutable revision rather than sampling `Latest` and
    /// comparing afterward. The default keeps test doubles source-compatible
    /// while still failing closed if their evidence moved.
    async fn allows_objects_at_revision_with_evidence(
        &self,
        caller: &Caller,
        requests: &[(ObjectKey, ObjectPermission)],
        required_revision: u64,
    ) -> Result<IndexAuthorizationEvidence, Status> {
        let evidence = self.allows_objects_with_evidence(caller, requests).await?;
        if required_revision == 0 || evidence.revision != required_revision {
            return Err(Status::failed_precondition(
                "protected authorization revision changed during index execution",
            ));
        }
        Ok(evidence)
    }

    /// Admit the definition and, for a custom policy, prove the application is
    /// allowed to evaluate that realm before pinning its current authority.
    async fn admit_query(
        &self,
        caller: &Caller,
        _stable_tenant_id: u64,
        definition: &ObjectKey,
        policy: &IndexResultAuthorizationPolicy,
        authorization_subject: Option<&ObjectRef>,
    ) -> Result<IndexQueryAuthorizationEvidence, Status> {
        let evidence = self
            .allows_objects_with_evidence(caller, &[(definition.clone(), ObjectPermission::Get)])
            .await?;
        if evidence.revision == 0 || evidence.allowed.as_slice() != [true] {
            return if evidence.allowed.as_slice() == [false] {
                Err(Status::permission_denied(
                    "index definition query is not authorized",
                ))
            } else {
                Err(Status::data_loss(
                    "index definition authorization returned invalid evidence",
                ))
            };
        }
        if !matches!(policy, IndexResultAuthorizationPolicy::Application)
            || authorization_subject.is_some()
        {
            return Err(Status::failed_precondition(
                "custom-realm index authorization is not installed",
            ));
        }
        Ok(IndexQueryAuthorizationEvidence {
            system_revision: evidence.revision,
            result: None,
        })
    }

    /// Evaluate custom-realm candidates at exactly the admitted revision and
    /// binding. Implementations must fail rather than silently moving to a
    /// newer schema or revision.
    async fn allows_realm_results_with_evidence(
        &self,
        _caller: &Caller,
        _stable_tenant_id: u64,
        _scope: &AuthzScope,
        _authorization_subject: &ObjectRef,
        _relation: &str,
        _resources: &[ObjectRef],
        _required_system_revision: u64,
        _required: &IndexRealmAuthorizationEvidence,
    ) -> Result<IndexAuthorizationEvidence, Status> {
        Err(Status::failed_precondition(
            "custom-realm index authorization is not installed",
        ))
    }
}

#[tonic::async_trait]
pub(crate) trait IndexDefinitionReader: Send + Sync + 'static {
    async fn current_snapshot(
        &self,
        key: &ObjectKey,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Option<keldra_store::ObjectPathSnapshot>, Status>;
}

#[tonic::async_trait]
impl IndexDefinitionReader for crate::cluster_object_read::ClusterObjectReader {
    async fn current_snapshot(
        &self,
        key: &ObjectKey,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Option<keldra_store::ObjectPathSnapshot>, Status> {
        self.current_snapshot_stable(key, tenant_id, bucket_id)
            .await
    }
}

/// Immutable values to which every opaque query page token is bound.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexPageTokenBinding {
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) index_id: u64,
    pub(crate) definition_version: u64,
    pub(crate) query_hash: [u8; 32],
    pub(crate) result_authorization: IndexResultAuthorizationPolicy,
    pub(crate) authorization_subject: Option<ObjectRef>,
    pub(crate) result_authorization_evidence: Option<IndexRealmAuthorizationEvidence>,
}

/// Mutable engine cursor carried by a valid page token. The surrounding token
/// binding carries the caller, result policy, end-user subject, and both
/// Zanzibar authorities so engine code cannot omit those security operands.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexPageCursor {
    pub(crate) commit_revision: u64,
    pub(crate) last_position: Vec<u8>,
    pub(crate) authorization_revision: u64,
}

/// Opaque token codec. Its production implementation is backed by the same
/// JWT key as other Keldra capabilities and must bind the complete `Caller`.
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

/// One caller-bound, exact committed mutation whose sparse real-time index
/// visibility must be present in the snapshot selected for a query.
///
/// This is deliberately not a contiguous freshness checkpoint. A query can
/// satisfy it from either an exact sparse overlay publication or a base root
/// which has subsequently absorbed the named source position.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexVisibilityRequirement {
    pub(crate) source_node_id: u16,
    pub(crate) source_epoch: [u8; 32],
    pub(crate) source_journal_position: u64,
    pub(crate) source_journal_through_position: u64,
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) exact_path: Option<String>,
    pub(crate) version: Option<u64>,
    pub(crate) program_commit_cursor: Option<u64>,
    pub(crate) atomic_unit_hash: Option<[u8; 32]>,
    pub(crate) active_placement_term: u64,
    pub(crate) active_placement_index: u64,
    pub(crate) expires_at_unix_millis: u64,
}

pub(crate) trait IndexVisibilityTokenCodec: Send + Sync + 'static {
    fn decode(
        &self,
        caller: &Caller,
        token: &[u8],
        expected_tenant_id: u64,
        expected_bucket_id: u64,
    ) -> Result<IndexVisibilityRequirement, Status>;
}

#[derive(Clone)]
pub(crate) struct ExecuteIndexQuery {
    pub(crate) context: IndexRequestContext,
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) definition: IndexDefinition,
    pub(crate) query: IndexQuery,
    pub(crate) limit: usize,
    /// Mandatory candidate-level Zanzibar and exact-current boundary. A local
    /// executor must invoke it while collecting/refilling, before admitting an
    /// arbitrary-order candidate to top-K state.
    pub(crate) candidate_visibility: Arc<dyn IndexCandidateVisibility>,
    /// Zanzibar revision established by the one definition-admission check for
    /// this execution. It also binds empty results and any continuation token.
    pub(crate) authorization_revision: u64,
    pub(crate) result_authorization: Option<IndexRealmAuthorizationEvidence>,
    pub(crate) authorization_subject: Option<ObjectRef>,
    /// `None` selects the latest published revision. A continuation supplies
    /// the exact immutable revision and engine-specific last position.
    pub(crate) resume: Option<IndexPageCursor>,
    pub(crate) required_freshness: Option<IndexFreshnessRequirement>,
    pub(crate) required_visibility: Vec<IndexVisibilityRequirement>,
}

#[derive(Clone, Debug)]
pub(crate) struct ExecutedIndexQuery {
    pub(crate) hits: Vec<IndexQueryHit>,
    pub(crate) facet_results: Vec<IndexFacetResult>,
    pub(crate) aggregate_results: Vec<IndexAggregateResult>,
    pub(crate) freshness: IndexFreshness,
    /// Engine-specific stable position following a returned, authorized and
    /// exact-current hit. Candidate-private positions never cross this boundary.
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
    pub(crate) visibility_tokens: Arc<dyn IndexVisibilityTokenCodec>,
    pub(crate) definition_reader: Arc<dyn IndexDefinitionReader>,
}
