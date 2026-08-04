use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anvil_api::v1::personal_db_service_server::PersonalDbService;
use anvil_api::v1::{
    AppendPersonalDbEntryRequest, ChangePersonalDbGroupRoleRequest, CreatePersonalDbGroupRequest,
    DescribePersonalDbGroupRequest, GetPersonalDbSnapshotRequest, ListPersonalDbGroupsRequest,
    ListPersonalDbGroupsResponse, MaterializePersonalDbProjectionRequest, PersonalDbCanonicalFrame,
    PersonalDbCatchUpRequest, PersonalDbCommit, PersonalDbGroup, PersonalDbGroupRole,
    PersonalDbGroupRoleChange, PersonalDbMaterialization, PersonalDbSnapshot,
    RegisterPersonalDbSnapshotRequest,
};
use anvil_authz::{AuthorizationCheck, ObjectRef, Tuple};
use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::{
    CoordinatedAuthzRealmResult, PlacementLogId, Store, TupleBatchRequest, TupleMutation,
    TupleMutationKind,
};
use personaldb_protocol::{
    CommittedHeadV2, DatabaseGroupKind, ProjectionDefinitionModeV1, ProjectionDefinitionV1,
    Sha256Digest, StateCommitmentV1, UnsignedCommittedHeadV2, UnsignedGroupDescriptorV1,
};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::authentication::{Caller, JwtManager};
use crate::authoritative_system::{AuthoritativeSystemAuthorization, FreshAuthorizationResult};
use crate::authorization::ObjectPermission;
use crate::authz_distribution::ZanzibarDistribution;
use crate::cluster_peer::ClusterPeerTransport;
use crate::cluster_placement::ClusterPlacement;
use crate::distributed_control_plane::DistributedControlPlane;
use crate::distributed_list::{DistributedObjectLister, OriginalBearer};
use crate::logical_name_resolution::LogicalNameResolver;
use crate::mutable_record_replica_group::MutableRecordReplicaGroup;
use crate::placement::PlacementKind;
use crate::v05::{deadline_remaining, request_deadline};

use super::authorization::{
    GroupPermission, ensure_realm, group_resource, realm_scope, role_relation,
};
use super::model::{
    GroupManifest, GroupScope, StoredGroupKind, TRUST_BUNDLE_VERSION, digest, manifest_path,
    parse_kind, parse_manifest_object_path, parse_scope_ids, projection_definition_path,
    storage_command_id, validate_command_id,
};
use super::placement::PersonalDbPlacement;
use super::routing::{
    ApplyPersonalDbRoleCall, RoutedPersonalDbCall, RoutedPersonalDbHandler,
    RoutedPersonalDbRequest, RoutedPersonalDbResponse,
};
use super::signing::PersonalDbSigners;
use super::storage::{ConditionalWrite, PersonalDbObjects};

const LIST_DEFAULT_LIMIT: usize = 100;
const LIST_MAX_LIMIT: usize = 100;
const LIST_SCAN_LIMIT: usize = 300;

pub(crate) type PersonalDbFrameStream =
    Pin<Box<dyn Stream<Item = Result<PersonalDbCanonicalFrame, Status>> + Send>>;

#[derive(Clone)]
pub(crate) struct PersonalDbServiceImpl {
    pub(super) local_node: NodeId,
    pub(super) decisions: DecisionRaft,
    pub(super) tokens: JwtManager,
    pub(super) names: LogicalNameResolver,
    pub(super) authorization: AuthoritativeSystemAuthorization,
    pub(super) zanzibar: Arc<ZanzibarDistribution>,
    pub(super) store: Store,
    pub(super) control: Arc<DistributedControlPlane>,
    pub(super) peers: ClusterPeerTransport,
    pub(super) objects: PersonalDbObjects,
    pub(super) lister: DistributedObjectLister,
    pub(super) placement: PersonalDbPlacement,
    pub(super) signers: PersonalDbSigners,
    pub(super) locks: Arc<[tokio::sync::Mutex<()>; 64]>,
    pub(super) request_timeout: Duration,
}

impl PersonalDbServiceImpl {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        local_node: NodeId,
        decisions: DecisionRaft,
        tokens: JwtManager,
        names: LogicalNameResolver,
        authorization: AuthoritativeSystemAuthorization,
        zanzibar: Arc<ZanzibarDistribution>,
        store: Store,
        control: Arc<DistributedControlPlane>,
        peers: ClusterPeerTransport,
        objects: PersonalDbObjects,
        lister: DistributedObjectLister,
        request_timeout: Duration,
    ) -> Result<Self, Status> {
        let signers = PersonalDbSigners::derive(&tokens)?;
        Ok(Self {
            local_node,
            placement: PersonalDbPlacement::new(local_node, decisions.clone()),
            decisions,
            tokens,
            names,
            authorization,
            zanzibar,
            store,
            control,
            peers,
            objects,
            lister,
            signers,
            locks: Arc::new(std::array::from_fn(|_| tokio::sync::Mutex::new(()))),
            request_timeout,
        })
    }

    pub(crate) fn routed_handler(&self) -> Arc<dyn RoutedPersonalDbHandler> {
        Arc::new(self.clone())
    }

    pub(super) async fn request_scope(
        &self,
        caller: Caller,
        bearer: OriginalBearer,
        bucket: &str,
        database_id: &str,
        group_id: &str,
    ) -> Result<GroupScope, Status> {
        let (bucket, database_id, group_id) = parse_scope_ids(bucket, database_id, group_id)?;
        let tenant = caller.storage_tenant().as_str().to_owned();
        let (tenant_id, bucket_id) = self.names.resolve_bucket_ids(&tenant, &bucket).await?;
        Ok(GroupScope {
            tenant,
            bucket,
            tenant_id,
            bucket_id,
            database_id,
            group_id,
            caller,
            bearer,
        })
    }

    pub(super) async fn load_manifest(
        &self,
        scope: &GroupScope,
    ) -> Result<Option<GroupManifest>, Status> {
        let stored = self.objects.read(scope, manifest_path()).await?;
        let Some(bytes) = stored.bytes else {
            return Ok(None);
        };
        let manifest: GroupManifest = serde_json::from_slice(&bytes)
            .map_err(|_| Status::data_loss("PersonalDB group manifest is malformed"))?;
        manifest.validate_for(scope)?;
        Ok(Some(manifest))
    }

    pub(super) async fn require_manifest(
        &self,
        scope: &GroupScope,
    ) -> Result<GroupManifest, Status> {
        self.load_manifest(scope)
            .await?
            .ok_or_else(|| Status::not_found("PersonalDB group does not exist"))
    }

    pub(super) async fn load_head(
        &self,
        scope: &GroupScope,
    ) -> Result<(u64, CommittedHeadV2, Vec<u8>), Status> {
        let stored = self.objects.read(scope, super::model::head_path()).await?;
        let version = stored
            .version
            .ok_or_else(|| Status::data_loss("PersonalDB committed head has no object version"))?;
        let bytes = stored
            .bytes
            .ok_or_else(|| Status::data_loss("PersonalDB committed head is missing"))?;
        let head = CommittedHeadV2::decode_canonical(&bytes).map_err(protocol_data_loss)?;
        Ok((version, head, bytes))
    }

    pub(super) async fn descriptor(
        &self,
        scope: &GroupScope,
        manifest: &GroupManifest,
        replayed: bool,
    ) -> Result<PersonalDbGroup, Status> {
        let (_, head, _) = self.load_head(scope).await?;
        let descriptor = UnsignedGroupDescriptorV1 {
            group_id: scope.group_id.clone(),
            database_id: scope.database_id.clone(),
            group_kind: manifest.kind(),
            schema_hash: manifest.schema_hash(),
            projection_definition_hash: manifest.projection_hash(),
            committed_head: head,
            trust_bundle_version: manifest.trust_bundle_version,
        }
        .sign(self.signers.group_control())
        .and_then(|value| value.encode_deterministic())
        .map_err(protocol_data_loss)?;
        Ok(PersonalDbGroup {
            descriptor,
            trust_records_json: self.signers.trust_records_json(),
            replayed,
        })
    }

    pub(super) fn lock_index(&self, scope: &GroupScope) -> usize {
        usize::from(blake3::hash(&scope.placement_key()).as_bytes()[0]) % self.locks.len()
    }

    async fn create_local(
        &self,
        scope: GroupScope,
        request: CreatePersonalDbGroupRequest,
        fence: PlacementLogId,
    ) -> Result<PersonalDbGroup, Status> {
        self.placement.require_local_primary(&scope, fence)?;
        let lock = self.lock_index(&scope);
        let _guard = self.locks[lock].lock().await;
        self.placement.require_local_primary(&scope, fence)?;

        let kind = parse_kind(request.kind)?;
        let schema_hash = digest("schema_hash_sha256", &request.schema_hash_sha256)?;
        let projection = projection_definition(&scope, kind, request.mirror_projection)?;
        if let Some(existing) = self.load_manifest(&scope).await? {
            if existing.group_kind != kind
                || existing.schema_hash() != schema_hash
                || existing.projection_hash()
                    != projection
                        .as_ref()
                        .map(ProjectionDefinitionV1::canonical_sha256)
                        .transpose()
                        .map_err(protocol_data_loss)?
            {
                return Err(Status::already_exists(
                    "PersonalDB group exists with another immutable definition",
                ));
            }
            return self.descriptor(&scope, &existing, true).await;
        }

        if let Some(definition) = &projection {
            let source = self
                .request_scope(
                    scope.caller.clone(),
                    scope.bearer.clone(),
                    &definition.source_bucket,
                    &definition.source_database_id.0,
                    &definition.source_group_id,
                )
                .await?;
            self.require_permission(&source, GroupPermission::Read, "projection source read")
                .await?;
            let source_manifest = self.require_manifest(&source).await?;
            if source_manifest.kind() != DatabaseGroupKind::Source {
                return Err(Status::failed_precondition(
                    "a mirror projection source must be a source group",
                ));
            }
            if source_manifest.schema_hash() != schema_hash {
                return Err(Status::failed_precondition(
                    "a mirror projection must use its source group's schema hash",
                ));
            }
        }

        let projection_hash = projection
            .as_ref()
            .map(ProjectionDefinitionV1::canonical_sha256)
            .transpose()
            .map_err(protocol_data_loss)?;
        let state = StateCommitmentV1 {
            database_id: scope.database_id.clone(),
            log_index: 0,
            log_hash: Sha256Digest::ZERO,
            database_state_root: Sha256Digest::ZERO,
            schema_hash,
            projection_definition_hash: projection_hash,
            group_kind: kind.into(),
        };
        let genesis = UnsignedCommittedHeadV2 {
            state,
            commit_certificate_hash: Sha256Digest::ZERO,
            primary_server_id: super::model::primary_server_id(self.local_node),
            placement_epoch: fence.index,
        }
        .sign(&scope.group_id, self.signers.witness())
        .and_then(|head| head.encode_deterministic())
        .map_err(protocol_data_loss)?;
        self.put_hidden_if_absent(
            &scope,
            super::model::head_path(),
            genesis,
            &request.command_id,
        )
        .await?;
        if let Some(definition) = &projection {
            self.put_hidden_if_absent(
                &scope,
                projection_definition_path(),
                definition
                    .encode_deterministic()
                    .map_err(protocol_data_loss)?,
                &request.command_id,
            )
            .await?;
        }

        let creator_app = scope
            .caller
            .authenticated_app_id()
            .map_err(|_| Status::unauthenticated("PersonalDB requires an application identity"))?
            .to_owned();
        self.apply_role_from_primary(
            &scope,
            ChangePersonalDbGroupRoleRequest {
                bucket: scope.bucket.clone(),
                database_id: scope.database_id.0.clone(),
                group_id: scope.group_id.clone(),
                app_id: creator_app,
                role: PersonalDbGroupRole::Manager as i32,
                command_id: request.command_id.clone(),
            },
            true,
            true,
            fence,
        )
        .await?;

        let manifest = GroupManifest {
            storage_format_version: super::model::STORAGE_FORMAT_VERSION,
            database_id: scope.database_id.0.clone(),
            group_id: scope.group_id.clone(),
            group_kind: kind,
            schema_hash_sha256: schema_hash.into_bytes(),
            projection_definition_hash_sha256: projection_hash.map(Sha256Digest::into_bytes),
            trust_bundle_version: TRUST_BUNDLE_VERSION,
        };
        let bytes = serde_json::to_vec(&manifest)
            .map_err(|_| Status::internal("PersonalDB manifest could not be encoded"))?;
        match self
            .objects
            .put_if_absent(
                &scope,
                manifest_path(),
                bytes,
                storage_command_id(&scope, &request.command_id, "publish-manifest"),
            )
            .await?
        {
            ConditionalWrite::Applied => {}
            ConditionalWrite::ConditionFailed => {
                let existing = self.require_manifest(&scope).await?;
                if existing != manifest {
                    return Err(Status::already_exists(
                        "PersonalDB group publication conflicts with another definition",
                    ));
                }
            }
        }
        self.placement.require_unchanged(fence)?;
        self.descriptor(&scope, &manifest, false).await
    }

    pub(super) async fn put_hidden_if_absent(
        &self,
        scope: &GroupScope,
        suffix: &str,
        expected: Vec<u8>,
        command_id: &str,
    ) -> Result<(), Status> {
        match self
            .objects
            .put_if_absent(
                scope,
                suffix,
                expected.clone(),
                storage_command_id(scope, command_id, suffix),
            )
            .await?
        {
            ConditionalWrite::Applied => Ok(()),
            ConditionalWrite::ConditionFailed => {
                let current = self.objects.read(scope, suffix).await?;
                if current.bytes.as_deref() == Some(expected.as_slice()) {
                    Ok(())
                } else {
                    Err(Status::already_exists(
                        "PersonalDB group preparation conflicts with another definition",
                    ))
                }
            }
        }
    }

    pub(super) async fn require_permission(
        &self,
        scope: &GroupScope,
        permission: GroupPermission,
        operation: &'static str,
    ) -> Result<FreshAuthorizationResult, Status> {
        let evidence = self.group_evidence(scope, &[permission]).await?;
        if evidence.allowed == [true]
            || self
                .bucket_allows_group_permission(scope, permission)
                .await?
        {
            Ok(evidence)
        } else {
            Err(Status::permission_denied(format!(
                "PersonalDB {operation} is not authorized"
            )))
        }
    }

    async fn require_bucket_put(&self, scope: &GroupScope) -> Result<(), Status> {
        let allowed = self
            .authorization
            .allows_objects(
                &scope.caller,
                &[(scope.virtual_key()?, ObjectPermission::Put)],
            )
            .await?;
        if allowed == [true] {
            Ok(())
        } else {
            Err(Status::permission_denied(
                "PersonalDB group creation is not authorized",
            ))
        }
    }

    async fn require_any_role(&self, scope: &GroupScope) -> Result<(), Status> {
        let group = self
            .group_evidence(
                scope,
                &[
                    GroupPermission::Read,
                    GroupPermission::Write,
                    GroupPermission::Materialize,
                    GroupPermission::Manage,
                ],
            )
            .await?;
        if group.allowed.into_iter().any(|allowed| allowed) {
            return Ok(());
        }
        let key = scope.virtual_key()?;
        let requests = [
            (key.clone(), ObjectPermission::Get),
            (key.clone(), ObjectPermission::Put),
            (key, ObjectPermission::Delete),
        ];
        let allowed = self
            .authorization
            .allows_objects(&scope.caller, &requests)
            .await?;
        if allowed.into_iter().any(|allowed| allowed)
            || self
                .authorization
                .allows_bucket_policy(&scope.caller, &scope.tenant, &scope.bucket)
                .await?
        {
            Ok(())
        } else {
            Err(Status::not_found("PersonalDB group does not exist"))
        }
    }

    async fn require_manager(
        &self,
        scope: &GroupScope,
    ) -> Result<FreshAuthorizationResult, Status> {
        let exact = self
            .group_evidence(scope, &[GroupPermission::Manage])
            .await?;
        if exact.allowed == [true] {
            return Ok(exact);
        }
        let bucket = self
            .authorization
            .allows_bucket_policy_with_evidence(&scope.caller, &scope.tenant, &scope.bucket)
            .await?;
        if bucket.allowed == [true] {
            Ok(exact)
        } else {
            Err(Status::permission_denied(
                "PersonalDB group role management is not authorized",
            ))
        }
    }

    async fn group_evidence(
        &self,
        scope: &GroupScope,
        permissions: &[GroupPermission],
    ) -> Result<FreshAuthorizationResult, Status> {
        let object = group_resource(scope)?;
        let checks = permissions
            .iter()
            .map(|permission| {
                AuthorizationCheck::new(
                    scope.caller.subject().clone(),
                    object.clone(),
                    permission.relation(),
                )
            })
            .collect();
        self.authorization
            .fresh_tenant_checks(
                scope.tenant_id,
                realm_scope(scope.caller.storage_tenant())?,
                checks,
            )
            .await
    }

    async fn bucket_allows_group_permission(
        &self,
        scope: &GroupScope,
        permission: GroupPermission,
    ) -> Result<bool, Status> {
        if permission == GroupPermission::Manage {
            return self
                .authorization
                .allows_bucket_policy(&scope.caller, &scope.tenant, &scope.bucket)
                .await;
        }
        let object_permission = match permission {
            GroupPermission::Read => ObjectPermission::Get,
            GroupPermission::Write => ObjectPermission::Put,
            GroupPermission::Materialize => ObjectPermission::Delete,
            GroupPermission::Manage => unreachable!(),
        };
        self.authorization
            .allows_objects(&scope.caller, &[(scope.virtual_key()?, object_permission)])
            .await
            .map(|allowed| allowed == [true])
    }

    async fn apply_role_from_primary(
        &self,
        scope: &GroupScope,
        request: ChangePersonalDbGroupRoleRequest,
        granted: bool,
        creator_owner: bool,
        fence: PlacementLogId,
    ) -> Result<PersonalDbGroupRoleChange, Status> {
        let executor = self.role_target(scope.tenant_id)?;
        if executor.node_id == self.local_node {
            return self
                .apply_role_local(ApplyPersonalDbRoleCall {
                    bearer: Arc::from(scope.bearer.signed_token()),
                    source_node: self.local_node,
                    placement_fence: fence,
                    tenant_id: scope.tenant_id,
                    bucket_id: scope.bucket_id,
                    request,
                    granted,
                    creator_owner,
                })
                .await;
        }
        self.peers
            .apply_personaldb_role(
                executor.node_id,
                executor.address.as_deref().ok_or_else(|| {
                    Status::unavailable("PersonalDB role executor has no peer address")
                })?,
                scope.bearer.signed_token(),
                scope.tenant_id,
                scope.bucket_id,
                request,
                granted,
                creator_owner,
                self.request_timeout,
            )
            .await
    }

    async fn apply_role_local(
        &self,
        call: ApplyPersonalDbRoleCall,
    ) -> Result<PersonalDbGroupRoleChange, Status> {
        let caller = self
            .tokens
            .verify(&call.bearer)
            .map_err(|_| Status::unauthenticated("the routed bearer token is invalid"))?;
        let bearer = OriginalBearer::from_signed_token(call.bearer.clone());
        let scope = self
            .request_scope(
                caller,
                bearer,
                &call.request.bucket,
                &call.request.database_id,
                &call.request.group_id,
            )
            .await?;
        if (scope.tenant_id, scope.bucket_id) != (call.tenant_id, call.bucket_id) {
            return Err(Status::failed_precondition(
                "PersonalDB role request stable IDs no longer match mutable names",
            ));
        }
        let primary = self.placement.primary(&scope)?;
        if primary.node_id != call.source_node || primary.fence != call.placement_fence {
            return Err(Status::permission_denied(
                "PersonalDB role request did not originate at the group primary",
            ));
        }
        validate_command_id(&call.request.command_id)?;
        let role = parse_role(call.request.role)?;
        ensure_realm(
            &self.zanzibar,
            &self.store,
            scope.tenant_id,
            scope.caller.storage_tenant(),
        )
        .await?;
        let evidence = if call.creator_owner {
            if !call.granted
                || role != PersonalDbGroupRole::Manager
                || scope.caller.authenticated_app_id().ok() != Some(call.request.app_id.as_str())
            {
                return Err(Status::permission_denied(
                    "PersonalDB creator ownership request is invalid",
                ));
            }
            self.require_bucket_put(&scope).await?;
            self.group_evidence(&scope, &[GroupPermission::Manage])
                .await?
        } else {
            self.require_manifest(&scope).await?;
            self.require_manager(&scope).await?
        };
        self.control
            .require_personaldb_application(scope.caller.storage_tenant(), &call.request.app_id)
            .await?;
        let subject = ObjectRef::opaque("app", call.request.app_id.clone())
            .map_err(crate::authz_api::authz_status)?;
        let tuple = Tuple::new(group_resource(&scope)?, role_relation(role), subject);
        let operation_id = role_operation_id(&scope, &call.request, call.granted);
        let result = self
            .zanzibar
            .mutate_tuples_journaled(
                scope.tenant_id,
                &self.store,
                TupleBatchRequest {
                    scope: realm_scope(scope.caller.storage_tenant())?,
                    principal: scope.caller.subject().clone(),
                    expected_revision: Some(evidence.revision),
                    expected_binding_generation: evidence.binding_generation,
                    operation_id: Some(operation_id),
                    mutations: vec![TupleMutation {
                        kind: if call.granted {
                            TupleMutationKind::Add
                        } else {
                            TupleMutationKind::Remove
                        },
                        tuple,
                    }],
                },
            )
            .await?;
        let receipt = match result.result {
            CoordinatedAuthzRealmResult::Tuples(receipt) => receipt,
            CoordinatedAuthzRealmResult::Bound(_) => {
                return Err(Status::internal(
                    "PersonalDB role mutation returned a realm-binding result",
                ));
            }
        };
        self.placement.require_unchanged(call.placement_fence)?;
        Ok(PersonalDbGroupRoleChange {
            authorization_revision: receipt.authz_revision.0,
            replayed: receipt.replayed,
        })
    }

    fn role_target(&self, tenant_id: u64) -> Result<RoleTarget, Status> {
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("cluster placement state is unavailable"))?;
        let placement = ClusterPlacement::from_applied(&state)
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let group = MutableRecordReplicaGroup::select(
            PlacementKind::ZanzibarRealm,
            placement.cluster_id(),
            &tenant_id.to_be_bytes(),
            placement.placement_nodes(),
        )
        .ok_or_else(|| Status::unavailable("cluster has no authorization replica"))?;
        let coordinator = group.coordinator();
        Ok(RoleTarget {
            node_id: coordinator,
            address: (coordinator != self.local_node)
                .then(|| placement.address(coordinator).map(|value| value.0.clone()))
                .flatten(),
        })
    }

    async fn route_or_create(
        &self,
        scope: GroupScope,
        request: CreatePersonalDbGroupRequest,
        remaining: Duration,
    ) -> Result<PersonalDbGroup, Status> {
        let primary = self.placement.primary(&scope)?;
        if let Some(address) = primary.address {
            return self
                .peers
                .route_create_personaldb_group(
                    primary.node_id,
                    &address,
                    scope.bearer.signed_token(),
                    request,
                    remaining,
                )
                .await;
        }
        self.create_local(scope, request, primary.fence).await
    }

    async fn route_or_change_role(
        &self,
        scope: GroupScope,
        request: ChangePersonalDbGroupRoleRequest,
        granted: bool,
        remaining: Duration,
    ) -> Result<PersonalDbGroupRoleChange, Status> {
        let primary = self.placement.primary(&scope)?;
        if let Some(address) = primary.address {
            return self
                .peers
                .route_change_personaldb_group_role(
                    primary.node_id,
                    &address,
                    scope.bearer.signed_token(),
                    request,
                    granted,
                    remaining,
                )
                .await;
        }
        self.placement
            .require_local_primary(&scope, primary.fence)?;
        self.require_manifest(&scope).await?;
        self.require_manager(&scope).await?;
        self.apply_role_from_primary(&scope, request, granted, false, primary.fence)
            .await
    }
}

#[tonic::async_trait]
impl RoutedPersonalDbHandler for PersonalDbServiceImpl {
    async fn execute(
        &self,
        call: RoutedPersonalDbCall,
    ) -> Result<RoutedPersonalDbResponse, Status> {
        let caller = self
            .tokens
            .verify(call.bearer())
            .map_err(|_| Status::unauthenticated("the routed bearer token is invalid"))?;
        let bearer = OriginalBearer::from_signed_token(Arc::<str>::from(call.bearer()));
        let fence = call.placement_fence();
        match call.into_request() {
            RoutedPersonalDbRequest::Create(request) => {
                validate_command_id(&request.command_id)?;
                let scope = self
                    .request_scope(
                        caller,
                        bearer,
                        &request.bucket,
                        &request.database_id,
                        &request.group_id,
                    )
                    .await?;
                self.require_bucket_put(&scope).await?;
                self.placement.require_local_primary(&scope, fence)?;
                self.create_local(scope, request, fence)
                    .await
                    .map(RoutedPersonalDbResponse::Group)
            }
            RoutedPersonalDbRequest::ChangeRole { request, granted } => {
                validate_command_id(&request.command_id)?;
                let scope = self
                    .request_scope(
                        caller,
                        bearer,
                        &request.bucket,
                        &request.database_id,
                        &request.group_id,
                    )
                    .await?;
                self.placement.require_local_primary(&scope, fence)?;
                self.require_manifest(&scope).await?;
                self.require_manager(&scope).await?;
                self.apply_role_from_primary(&scope, request, granted, false, fence)
                    .await
                    .map(RoutedPersonalDbResponse::RoleChange)
            }
            RoutedPersonalDbRequest::Append(request) => {
                super::commit::execute_routed_append(self, caller, bearer, request, fence)
                    .await
                    .map(RoutedPersonalDbResponse::Commit)
            }
            RoutedPersonalDbRequest::Materialize(request) => {
                super::projection::execute_routed_materialization(
                    self, caller, bearer, request, fence,
                )
                .await
                .map(RoutedPersonalDbResponse::Materialization)
            }
            RoutedPersonalDbRequest::RegisterSnapshot(request) => {
                super::snapshot::execute_routed_registration(self, caller, bearer, request, fence)
                    .await
                    .map(RoutedPersonalDbResponse::Snapshot)
            }
        }
    }

    async fn apply_role(
        &self,
        call: ApplyPersonalDbRoleCall,
    ) -> Result<PersonalDbGroupRoleChange, Status> {
        self.apply_role_local(call).await
    }
}

#[tonic::async_trait]
impl PersonalDbService for PersonalDbServiceImpl {
    async fn create_group(
        &self,
        request: Request<CreatePersonalDbGroupRequest>,
    ) -> Result<Response<PersonalDbGroup>, Status> {
        let deadline = request_deadline(request.metadata(), self.request_timeout)?;
        let caller = authenticated_caller(&request)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let request = request.into_inner();
        validate_command_id(&request.command_id)?;
        let scope = self
            .request_scope(
                caller,
                bearer,
                &request.bucket,
                &request.database_id,
                &request.group_id,
            )
            .await?;
        self.require_bucket_put(&scope).await?;
        self.route_or_create(scope, request, deadline_remaining(deadline)?)
            .await
            .map(Response::new)
    }

    async fn describe_group(
        &self,
        request: Request<DescribePersonalDbGroupRequest>,
    ) -> Result<Response<PersonalDbGroup>, Status> {
        let caller = authenticated_caller(&request)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let request = request.into_inner();
        let scope = self
            .request_scope(
                caller,
                bearer,
                &request.bucket,
                &request.database_id,
                &request.group_id,
            )
            .await?;
        self.require_any_role(&scope).await?;
        let manifest = self.require_manifest(&scope).await?;
        self.descriptor(&scope, &manifest, false)
            .await
            .map(Response::new)
    }

    async fn list_groups(
        &self,
        request: Request<ListPersonalDbGroupsRequest>,
    ) -> Result<Response<ListPersonalDbGroupsResponse>, Status> {
        let caller = authenticated_caller(&request)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let request = request.into_inner();
        super::model::validate_id("bucket", &request.bucket)?;
        let tenant = caller.storage_tenant().as_str().to_owned();
        let (tenant_id, bucket_id) = self
            .names
            .resolve_bucket_ids(&tenant, &request.bucket)
            .await?;
        let limit = if request.limit == 0 {
            LIST_DEFAULT_LIMIT
        } else {
            usize::try_from(request.limit)
                .unwrap_or(usize::MAX)
                .min(LIST_MAX_LIMIT)
        };
        let mut scan_after = decode_page_token(&request.page_token)?;
        let bucket_manager = self
            .authorization
            .allows_bucket_policy(&caller, &tenant, &request.bucket)
            .await?;
        let mut groups = Vec::with_capacity(limit);
        let mut progress = AuthorizedPageProgress::new(limit);
        let next_page_token = 'scan: loop {
            let page = self
                .lister
                .list_personaldb_manifests(
                    bearer.clone(),
                    &tenant,
                    &request.bucket,
                    tenant_id,
                    bucket_id,
                    scan_after.as_deref(),
                    LIST_SCAN_LIMIT,
                )
                .await?;
            if page.paths.is_empty() {
                if page.has_more {
                    return Err(Status::data_loss(
                        "PersonalDB manifest listing made no forward progress",
                    ));
                }
                break 'scan String::new();
            }

            let mut scopes = Vec::with_capacity(page.paths.len());
            for path in &page.paths {
                let (database_id, group_id) = parse_manifest_object_path(path)?;
                scopes.push(GroupScope {
                    tenant: tenant.clone(),
                    bucket: request.bucket.clone(),
                    tenant_id,
                    bucket_id,
                    database_id,
                    group_id,
                    caller: caller.clone(),
                    bearer: bearer.clone(),
                });
            }
            let visible = if bucket_manager {
                vec![true; scopes.len()]
            } else {
                let mut group_checks = Vec::with_capacity(scopes.len() * 4);
                let mut object_checks = Vec::with_capacity(scopes.len() * 3);
                for scope in &scopes {
                    let group = group_resource(scope)?;
                    group_checks.extend([
                        AuthorizationCheck::new(
                            caller.subject().clone(),
                            group.clone(),
                            GroupPermission::Read.relation(),
                        ),
                        AuthorizationCheck::new(
                            caller.subject().clone(),
                            group.clone(),
                            GroupPermission::Write.relation(),
                        ),
                        AuthorizationCheck::new(
                            caller.subject().clone(),
                            group.clone(),
                            GroupPermission::Materialize.relation(),
                        ),
                        AuthorizationCheck::new(
                            caller.subject().clone(),
                            group,
                            GroupPermission::Manage.relation(),
                        ),
                    ]);
                    let key = scope.virtual_key()?;
                    object_checks.extend([
                        (key.clone(), ObjectPermission::Get),
                        (key.clone(), ObjectPermission::Put),
                        (key, ObjectPermission::Delete),
                    ]);
                }
                let group_allowed = self
                    .authorization
                    .fresh_tenant_checks(
                        tenant_id,
                        realm_scope(caller.storage_tenant())?,
                        group_checks,
                    )
                    .await?
                    .allowed;
                let object_allowed = self
                    .authorization
                    .allows_objects(&caller, &object_checks)
                    .await?;
                group_allowed
                    .chunks_exact(4)
                    .zip(object_allowed.chunks_exact(3))
                    .map(|(group_roles, object_roles)| {
                        group_roles
                            .iter()
                            .chain(object_roles)
                            .any(|allowed| *allowed)
                    })
                    .collect()
            };

            let mut candidates = page.paths.iter().zip(scopes).zip(visible).peekable();
            while let Some(((path, scope), allowed)) = candidates.next() {
                if allowed {
                    let manifest = self.require_manifest(&scope).await?;
                    groups.push(self.descriptor(&scope, &manifest, false).await?);
                    if progress.accept(path) {
                        let source_has_more = candidates.peek().is_some() || page.has_more;
                        break 'scan progress.continuation(source_has_more);
                    }
                }
            }
            if !page.has_more {
                break 'scan String::new();
            }
            let next_scan_after = page
                .paths
                .last()
                .expect("non-empty page checked above")
                .clone();
            if scan_after.as_ref() == Some(&next_scan_after) {
                return Err(Status::data_loss(
                    "PersonalDB manifest listing did not advance its cursor",
                ));
            }
            scan_after = Some(next_scan_after);
        };
        Ok(Response::new(ListPersonalDbGroupsResponse {
            groups,
            next_page_token,
        }))
    }

    async fn grant_group_role(
        &self,
        request: Request<ChangePersonalDbGroupRoleRequest>,
    ) -> Result<Response<PersonalDbGroupRoleChange>, Status> {
        self.change_role(request, true).await.map(Response::new)
    }

    async fn revoke_group_role(
        &self,
        request: Request<ChangePersonalDbGroupRoleRequest>,
    ) -> Result<Response<PersonalDbGroupRoleChange>, Status> {
        self.change_role(request, false).await.map(Response::new)
    }

    async fn append_entry(
        &self,
        request: Request<AppendPersonalDbEntryRequest>,
    ) -> Result<Response<PersonalDbCommit>, Status> {
        super::commit::append(self, request)
            .await
            .map(Response::new)
    }

    async fn materialize_projection(
        &self,
        request: Request<MaterializePersonalDbProjectionRequest>,
    ) -> Result<Response<PersonalDbMaterialization>, Status> {
        super::projection::materialize(self, request)
            .await
            .map(Response::new)
    }

    type CatchUpStream = PersonalDbFrameStream;

    async fn catch_up(
        &self,
        request: Request<PersonalDbCatchUpRequest>,
    ) -> Result<Response<Self::CatchUpStream>, Status> {
        super::sync::catch_up(self, request)
            .await
            .map(Response::new)
    }

    async fn register_snapshot(
        &self,
        request: Request<RegisterPersonalDbSnapshotRequest>,
    ) -> Result<Response<PersonalDbSnapshot>, Status> {
        super::snapshot::register(self, request)
            .await
            .map(Response::new)
    }

    type GetSnapshotStream = PersonalDbFrameStream;

    async fn get_snapshot(
        &self,
        request: Request<GetPersonalDbSnapshotRequest>,
    ) -> Result<Response<Self::GetSnapshotStream>, Status> {
        super::snapshot::get(self, request).await.map(Response::new)
    }
}

impl PersonalDbServiceImpl {
    async fn change_role(
        &self,
        request: Request<ChangePersonalDbGroupRoleRequest>,
        granted: bool,
    ) -> Result<PersonalDbGroupRoleChange, Status> {
        let deadline = request_deadline(request.metadata(), self.request_timeout)?;
        let caller = authenticated_caller(&request)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let request = request.into_inner();
        validate_command_id(&request.command_id)?;
        parse_role(request.role)?;
        let scope = self
            .request_scope(
                caller,
                bearer,
                &request.bucket,
                &request.database_id,
                &request.group_id,
            )
            .await?;
        self.require_manifest(&scope).await?;
        self.require_manager(&scope).await?;
        self.route_or_change_role(scope, request, granted, deadline_remaining(deadline)?)
            .await
    }
}

pub(super) fn authenticated_caller<T>(request: &Request<T>) -> Result<Caller, Status> {
    request
        .extensions()
        .get::<Caller>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("a bearer token is required"))
}

fn projection_definition(
    scope: &GroupScope,
    kind: StoredGroupKind,
    definition: Option<anvil_api::v1::PersonalDbMirrorProjectionDefinition>,
) -> Result<Option<ProjectionDefinitionV1>, Status> {
    match (kind, definition) {
        (StoredGroupKind::Projection, Some(definition)) => {
            let (_, source_database_id, source_group_id) = parse_scope_ids(
                &definition.source_bucket,
                &definition.source_database_id,
                &definition.source_group_id,
            )?;
            Ok(Some(ProjectionDefinitionV1 {
                projection_database_id: scope.database_id.clone(),
                projection_group_id: scope.group_id.clone(),
                source_database_id,
                source_group_id,
                source_bucket: definition.source_bucket,
                mode: ProjectionDefinitionModeV1::Mirror,
            }))
        }
        (StoredGroupKind::Projection, None) => Err(Status::invalid_argument(
            "projection groups require a mirror projection definition",
        )),
        (_, Some(_)) => Err(Status::invalid_argument(
            "only projection groups may contain a projection definition",
        )),
        (_, None) => Ok(None),
    }
}

fn parse_role(value: i32) -> Result<PersonalDbGroupRole, Status> {
    match PersonalDbGroupRole::try_from(value)
        .map_err(|_| Status::invalid_argument("PersonalDB group role is not recognized"))?
    {
        PersonalDbGroupRole::Unspecified => Err(Status::invalid_argument(
            "PersonalDB group role must be specified",
        )),
        role => Ok(role),
    }
}

fn role_operation_id(
    scope: &GroupScope,
    request: &ChangePersonalDbGroupRoleRequest,
    granted: bool,
) -> String {
    let mut hasher = blake3::Hasher::new_derive_key("anvil.personaldb/role-operation/v1");
    hasher.update(&scope.placement_key());
    hasher.update(request.app_id.as_bytes());
    hasher.update(&request.role.to_be_bytes());
    hasher.update(&[u8::from(granted)]);
    hasher.update(request.command_id.as_bytes());
    format!("personaldb-role:{}", hasher.finalize().to_hex())
}

fn encode_page_token(path: &str) -> String {
    hex::encode(path.as_bytes())
}

struct AuthorizedPageProgress {
    limit: usize,
    accepted: usize,
    last_authorized_path: Option<String>,
}

impl AuthorizedPageProgress {
    fn new(limit: usize) -> Self {
        debug_assert!(limit > 0);
        Self {
            limit,
            accepted: 0,
            last_authorized_path: None,
        }
    }

    fn accept(&mut self, path: &str) -> bool {
        self.accepted += 1;
        self.last_authorized_path = Some(path.to_owned());
        self.accepted == self.limit
    }

    fn continuation(&self, source_has_more: bool) -> String {
        if self.accepted != self.limit || !source_has_more {
            return String::new();
        }
        self.last_authorized_path
            .as_deref()
            .map(encode_page_token)
            .unwrap_or_default()
    }
}

fn decode_page_token(token: &str) -> Result<Option<String>, Status> {
    if token.is_empty() {
        return Ok(None);
    }
    let bytes = hex::decode(token)
        .map_err(|_| Status::invalid_argument("PersonalDB page token is malformed"))?;
    let path = String::from_utf8(bytes)
        .map_err(|_| Status::invalid_argument("PersonalDB page token is malformed"))?;
    parse_manifest_object_path(&path)
        .map_err(|_| Status::invalid_argument("PersonalDB page token is malformed"))?;
    Ok(Some(path))
}

fn protocol_data_loss(error: impl std::fmt::Display) -> Status {
    Status::data_loss(format!("invalid stored PersonalDB evidence: {error}"))
}

struct RoleTarget {
    node_id: NodeId,
    address: Option<String>,
}

#[cfg(test)]
mod pagination_tests {
    use super::*;

    const FIRST_VISIBLE: &str =
        "_anvil/personaldb/v1/64617461626173652d31/67726f75702d31/manifest.json";
    const SECOND_VISIBLE: &str =
        "_anvil/personaldb/v1/64617461626173652d32/67726f75702d32/manifest.json";

    #[test]
    fn continuation_is_derived_only_from_the_last_authorized_result() {
        let mut progress = AuthorizedPageProgress::new(2);
        assert!(!progress.accept(FIRST_VISIBLE));

        // Arbitrarily many unauthorized manifest paths may be scanned here;
        // none is passed to `accept`, so none can become public pagination
        // metadata. The next authorized result remains the only cursor source.
        assert!(progress.accept(SECOND_VISIBLE));

        let token = progress.continuation(true);
        assert_eq!(
            decode_page_token(&token).unwrap().as_deref(),
            Some(SECOND_VISIBLE)
        );
    }

    #[test]
    fn incomplete_or_exhausted_authorized_pages_have_no_continuation() {
        let mut incomplete = AuthorizedPageProgress::new(2);
        assert!(!incomplete.accept(FIRST_VISIBLE));
        assert!(incomplete.continuation(true).is_empty());

        let mut exhausted = AuthorizedPageProgress::new(1);
        assert!(exhausted.accept(FIRST_VISIBLE));
        assert!(exhausted.continuation(false).is_empty());
    }
}
