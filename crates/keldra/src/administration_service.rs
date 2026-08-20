//! Typed administration backed by the protected Zanzibar system realm.

use std::path::PathBuf;
use std::sync::Arc;

use keldra_api::v1 as api;
use keldra_api::v1::administration_service_server::AdministrationService;
use keldra_consensus::{
    ApplyError, ApplyResult, CLUSTER_CONTROL_COMMAND_VERSION, CapabilityRange, Command,
    DecisionRaft, DecisionRaftError, MAX_PEER_ADDRESS_BYTES, MembershipTransitionKind,
    NodeDescriptor, NodeId, NodeState, PeerAddress, StateMachine,
};
use keldra_store::{
    ApplicationCredentialRequest, ApplicationRoleTarget, AuthzStoreError, BucketApplicationRole,
    CreateBucketRequest, CredentialMutationReceipt, CredentialRepositoryError,
    ObjectVersioning as StoreObjectVersioning, ProvisionTenantRequest, SetApplicationRoleRequest,
    SetBucketPublicReadRequest, StorageTenantId, Store, SystemApplicationRole,
    TenantApplicationRole,
};
use tonic::{Request, Response, Status};

use crate::authentication::Caller;
use crate::authorization::{StorageTenantPermission, SystemAuthorizer};
use crate::distributed_control_plane::DistributedControlPlane;
use crate::distributed_list::OriginalBearer;
use crate::join_bundle::{self, JoinBundle, JoinBundleError, JoinSeed};

const PEER_PROTOCOL_VERSION: u16 = 1;
const STORAGE_FORMAT_VERSION: u16 = 1;

#[derive(Clone)]
pub(crate) struct AdministrationServiceImpl {
    store: Store,
    system_authorizer: SystemAuthorizer,
    decisions: DecisionRaft,
    join_bundle_directory: PathBuf,
    distributed: Option<Arc<DistributedControlPlane>>,
}

impl AdministrationServiceImpl {
    pub(crate) fn new(
        store: Store,
        decisions: DecisionRaft,
        join_bundle_directory: PathBuf,
    ) -> Self {
        Self {
            system_authorizer: SystemAuthorizer::new(store.authz()),
            store,
            decisions,
            join_bundle_directory,
            distributed: None,
        }
    }

    pub(crate) fn with_distributed(mut self, distributed: Arc<DistributedControlPlane>) -> Self {
        self.distributed = Some(distributed);
        self
    }
}

#[tonic::async_trait]
impl AdministrationService for AdministrationServiceImpl {
    async fn prepare_node(
        &self,
        request: Request<api::PrepareNodeRequest>,
    ) -> Result<Response<api::PrepareNodeResponse>, Status> {
        let caller = caller(&request)?;
        let request = request.into_inner();
        let node_id = NodeId(u64::from(request.node_id));
        let peer_address = parse_peer_address(request.peer_address)?;
        if !(1..=1_023).contains(&node_id.0) {
            return Err(Status::invalid_argument(
                "node_id must be between 1 and 1023",
            ));
        }
        if request.storage_weight_millionths == 0 {
            return Err(Status::invalid_argument(
                "storage_weight_millionths must be positive",
            ));
        }

        if let Some(distributed) = self.distributed.as_ref() {
            distributed.authorize_node_preparation(&caller).await?;
        } else {
            let authorizer = self.system_authorizer.clone();
            run(move || {
                let system = authorizer.load().map_err(authz_status)?;
                require_allowed(
                    system
                        .allows_manage_system(caller.subject())
                        .map_err(authz_evaluation_status)?,
                    "node preparation is not authorized",
                )
            })
            .await?;
        }

        // The bundle is deliberately a node-local operator handoff file. Prove
        // this node can commit before creating private material that a follower
        // could only strand on its own disk.
        self.decisions
            .confirm_leadership()
            .await
            .map_err(decision_status)?;
        let state = self.decisions.state().map_err(decision_status)?;
        let cluster_id = state.cluster_id().ok_or_else(|| {
            Status::failed_precondition("cluster identity has not been initialized")
        })?;
        reconcile_committed_refresh(
            &self.join_bundle_directory,
            &state,
            node_id,
            &peer_address,
            request.storage_weight_millionths,
        )?;
        let is_raft_member = self
            .decisions
            .committed_voter_ids()
            .map_err(decision_status)?
            .contains(&node_id)
            || self
                .decisions
                .committed_learner_ids()
                .map_err(decision_status)?
                .contains(&node_id);
        let existing = preflight_node_preparation(
            &state,
            node_id,
            &peer_address,
            request.storage_weight_millionths,
            is_raft_member,
        )?;
        let seeds = active_join_seeds(&state)?;
        let (path, bundle) = match existing {
            None => {
                let directory = self.join_bundle_directory.clone();
                let bundle_address = peer_address.clone();
                let storage_weight_millionths = request.storage_weight_millionths;
                let (path, bundle) = run(move || {
                    join_bundle::create_or_load(
                        &directory,
                        cluster_id,
                        node_id,
                        bundle_address,
                        storage_weight_millionths,
                        seeds,
                    )
                    .map_err(join_bundle_status)
                })
                .await?;
                let descriptor = joining_descriptor(&bundle)?;
                let committed = self
                    .decisions
                    .submit(Command::BeginAddNode {
                        format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                        descriptor,
                    })
                    .await
                    .map_err(decision_status)?;
                match committed.result {
                    ApplyResult::MembershipTransitionBegun(transition)
                        if transition.kind == MembershipTransitionKind::Add
                            && transition.node_id == node_id => {}
                    result => {
                        return Err(Status::internal(format!(
                            "node preparation returned an unexpected result: {result:?}"
                        )));
                    }
                }
                (path, bundle)
            }
            Some(existing) => {
                self.refresh_joining_preparation(
                    cluster_id,
                    node_id,
                    peer_address.clone(),
                    request.storage_weight_millionths,
                    seeds,
                    existing,
                )
                .await?
            }
        };

        let path = path.to_str().ok_or_else(|| {
            Status::internal("join bundle path cannot be represented in the public response")
        })?;
        Ok(Response::new(api::PrepareNodeResponse {
            join_bundle_path: path.to_owned(),
            cluster_id: cluster_id.0.to_vec(),
            node_id: node_id.0,
            peer_spki_sha256: bundle
                .peer_spki_sha256()
                .map_err(join_bundle_status)?
                .0
                .to_vec(),
        }))
    }

    async fn provision_tenant(
        &self,
        request: Request<api::ProvisionTenantRequest>,
    ) -> Result<Response<api::ProvisionTenantResponse>, Status> {
        let caller = caller(&request)?;
        if let Some(distributed) = self.distributed.as_ref() {
            let bearer = OriginalBearer::from_metadata(request.metadata())?;
            let response = distributed
                .provision_tenant(caller, bearer, request.into_inner())
                .await?;
            return Ok(Response::new(response));
        }
        let request = request.into_inner();
        let storage_tenant =
            StorageTenantId::parse(request.storage_tenant).map_err(authz_status)?;
        let store = self.store.clone();
        let authorizer = self.system_authorizer.clone();
        let receipt = run(move || {
            let system = authorizer.load().map_err(authz_status)?;
            require_allowed(
                system
                    .allows_manage_system(caller.subject())
                    .map_err(authz_evaluation_status)?,
                "tenant provisioning is not authorized",
            )?;
            store
                .provision_tenant(ProvisionTenantRequest {
                    storage_tenant,
                    owner_app_id: request.owner_app_id,
                    owner_client_id: request.owner_client_id,
                    owner_client_secret: request.owner_client_secret,
                    principal: caller.subject().clone(),
                    expected_authorization_revision: system.revision,
                    expected_binding_generation: system.binding_generation,
                })
                .map_err(credential_store_status)
        })
        .await?;
        Ok(Response::new(api::ProvisionTenantResponse {
            credential: Some(credential_to_api(receipt.credential, receipt.replayed)),
            authorization_revision: receipt.authorization_revision.0,
            replayed: receipt.replayed,
        }))
    }

    async fn create_application(
        &self,
        request: Request<api::CreateApplicationRequest>,
    ) -> Result<Response<api::ApplicationCredential>, Status> {
        let caller = caller(&request)?;
        if let Some(distributed) = self.distributed.as_ref() {
            let bearer = OriginalBearer::from_metadata(request.metadata())?;
            let response = distributed
                .create_application(caller, bearer, request.into_inner())
                .await?;
            return Ok(Response::new(response));
        }
        let request = request.into_inner();
        let store = self.store.clone();
        let authorizer = self.system_authorizer.clone();
        let receipt = run(move || {
            let system = authorizer.load().map_err(authz_status)?;
            require_manage_tenant_or_system(&system, &caller)?;
            store
                .create_application(
                    ApplicationCredentialRequest {
                        storage_tenant: caller.storage_tenant().clone(),
                        app_id: request.app_id,
                        client_id: request.client_id,
                        client_secret: request.client_secret,
                    },
                    system.revision,
                )
                .map_err(credential_store_status)
        })
        .await?;
        Ok(Response::new(credential_to_api(
            receipt.credential,
            receipt.replayed,
        )))
    }

    async fn rotate_application_credential(
        &self,
        request: Request<api::RotateApplicationCredentialRequest>,
    ) -> Result<Response<api::ApplicationCredential>, Status> {
        let caller = caller(&request)?;
        if let Some(distributed) = self.distributed.as_ref() {
            let bearer = OriginalBearer::from_metadata(request.metadata())?;
            let response = distributed
                .rotate_application_credential(caller, bearer, request.into_inner())
                .await?;
            return Ok(Response::new(response));
        }
        let request = request.into_inner();
        let store = self.store.clone();
        let authorizer = self.system_authorizer.clone();
        let receipt = run(move || {
            let system = authorizer.load().map_err(authz_status)?;
            require_manage_tenant_or_system(&system, &caller)?;
            store
                .rotate_application_credential(
                    ApplicationCredentialRequest {
                        storage_tenant: caller.storage_tenant().clone(),
                        app_id: request.app_id,
                        client_id: request.client_id,
                        client_secret: request.client_secret,
                    },
                    system.revision,
                )
                .map_err(credential_store_status)
        })
        .await?;
        Ok(Response::new(credential_to_api(
            receipt.credential,
            receipt.replayed,
        )))
    }

    async fn recover_application_credential(
        &self,
        request: Request<api::RecoverApplicationCredentialRequest>,
    ) -> Result<Response<api::ApplicationCredential>, Status> {
        let caller = caller(&request)?;
        if let Some(distributed) = self.distributed.as_ref() {
            let bearer = OriginalBearer::from_metadata(request.metadata())?;
            let response = distributed
                .recover_application_credential(caller, bearer, request.into_inner())
                .await?;
            return Ok(Response::new(response));
        }
        let request = request.into_inner();
        let store = self.store.clone();
        let authorizer = self.system_authorizer.clone();
        let receipt = run(move || {
            let system = authorizer.load().map_err(authz_status)?;
            require_credential_recovery(&system, &caller)?;
            let storage_tenant = StorageTenantId::parse(request.storage_tenant)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            store
                .rotate_application_credential(
                    ApplicationCredentialRequest {
                        storage_tenant,
                        app_id: request.app_id,
                        client_id: request.client_id,
                        client_secret: request.client_secret,
                    },
                    system.revision,
                )
                .map_err(credential_store_status)
        })
        .await?;
        Ok(Response::new(credential_to_api(
            receipt.credential,
            receipt.replayed,
        )))
    }

    async fn disable_application_credential(
        &self,
        request: Request<api::DisableApplicationCredentialRequest>,
    ) -> Result<Response<api::ApplicationCredentialState>, Status> {
        let caller = caller(&request)?;
        if let Some(distributed) = self.distributed.as_ref() {
            let bearer = OriginalBearer::from_metadata(request.metadata())?;
            let response = distributed
                .disable_application_credential(caller, bearer, request.into_inner())
                .await?;
            return Ok(Response::new(response));
        }
        let request = request.into_inner();
        let store = self.store.clone();
        let authorizer = self.system_authorizer.clone();
        let receipt = run(move || {
            let system = authorizer.load().map_err(authz_status)?;
            require_manage_tenant_or_system(&system, &caller)?;
            store
                .disable_application_credential(
                    caller.storage_tenant().clone(),
                    request.app_id,
                    request.client_id,
                    system.revision,
                )
                .map_err(credential_store_status)
        })
        .await?;
        Ok(Response::new(credential_state_to_api(receipt)))
    }

    async fn create_bucket(
        &self,
        request: Request<api::CreateBucketRequest>,
    ) -> Result<Response<api::CreateBucketResponse>, Status> {
        let caller = caller(&request)?;
        if caller.storage_tenant().is_system() {
            return Err(Status::invalid_argument(
                "buckets cannot be created in the protected system tenant",
            ));
        }
        if let Some(distributed) = self.distributed.as_ref() {
            let bearer = OriginalBearer::from_metadata(request.metadata())?;
            let response = distributed
                .create_bucket(caller, bearer, request.into_inner())
                .await?;
            return Ok(Response::new(response));
        }
        let request = request.into_inner();
        let versioning = versioning_from_api(request.versioning)?;
        let store = self.store.clone();
        let authorizer = self.system_authorizer.clone();
        let receipt = run(move || {
            let system = authorizer.load().map_err(authz_status)?;
            require_allowed(
                system
                    .allows_storage_tenant(
                        caller.subject(),
                        caller.storage_tenant().as_str(),
                        StorageTenantPermission::ManageBuckets,
                    )
                    .map_err(authz_evaluation_status)?,
                "bucket creation is not authorized",
            )?;
            store
                .create_bucket(CreateBucketRequest {
                    storage_tenant: caller.storage_tenant().clone(),
                    bucket: request.bucket,
                    versioning,
                    owner: caller.subject().clone(),
                    principal: caller.subject().clone(),
                    expected_authorization_revision: system.revision,
                    expected_binding_generation: system.binding_generation,
                })
                .map_err(credential_store_status)
        })
        .await?;
        Ok(Response::new(api::CreateBucketResponse {
            storage_tenant: receipt.storage_tenant.to_string(),
            bucket: receipt.bucket,
            authorization_revision: receipt.authorization_revision.0,
            replayed: receipt.replayed,
            versioning: versioning_to_api(receipt.versioning) as i32,
        }))
    }

    async fn set_bucket_versioning(
        &self,
        request: Request<api::SetBucketVersioningRequest>,
    ) -> Result<Response<api::SetBucketVersioningResponse>, Status> {
        let caller = caller(&request)?;
        if caller.storage_tenant().is_system() {
            return Err(Status::invalid_argument(
                "buckets cannot exist in the protected system tenant",
            ));
        }
        if let Some(distributed) = self.distributed.as_ref() {
            let bearer = OriginalBearer::from_metadata(request.metadata())?;
            let response = distributed
                .set_bucket_versioning(caller, bearer, request.into_inner())
                .await?;
            return Ok(Response::new(response));
        }
        let request = request.into_inner();
        let requested = versioning_from_api(request.versioning)?;
        if requested != StoreObjectVersioning::Enabled {
            return Err(Status::invalid_argument(
                "SetBucketVersioning accepts only ENABLED; bucket versioning cannot be disabled",
            ));
        }
        let storage_tenant = caller.storage_tenant().clone();
        let bucket = request.bucket;
        let store = self.store.clone();
        let authorizer = self.system_authorizer.clone();
        let response_tenant = storage_tenant.clone();
        let response_bucket = bucket.clone();
        let authorization_tenant = storage_tenant.clone();
        let authorization_bucket = bucket.clone();
        run(move || {
            let system = authorizer.load().map_err(authz_status)?;
            require_allowed(
                system
                    .allows_bucket_policy(
                        caller.subject(),
                        authorization_tenant.as_str(),
                        &authorization_bucket,
                    )
                    .map_err(authz_evaluation_status)?,
                "bucket versioning management is not authorized",
            )
        })
        .await?;
        let changed = store
            .enable_bucket_versioning(storage_tenant.as_str(), &bucket)
            .await
            .map_err(|_| Status::internal("bucket versioning metadata could not be updated"))?;
        Ok(Response::new(api::SetBucketVersioningResponse {
            storage_tenant: response_tenant.to_string(),
            bucket: response_bucket,
            versioning: api::ObjectVersioning::Enabled as i32,
            changed,
        }))
    }

    async fn set_bucket_public_read(
        &self,
        request: Request<api::SetBucketPublicReadRequest>,
    ) -> Result<Response<api::SetBucketPublicReadResponse>, Status> {
        let caller = caller(&request)?;
        if caller.storage_tenant().is_system() {
            return Err(Status::invalid_argument(
                "buckets cannot exist in the protected system tenant",
            ));
        }
        if let Some(distributed) = self.distributed.as_ref() {
            let bearer = OriginalBearer::from_metadata(request.metadata())?;
            let response = distributed
                .set_bucket_public_read(caller, bearer, request.into_inner())
                .await?;
            return Ok(Response::new(response));
        }
        let request = request.into_inner();
        let storage_tenant = caller.storage_tenant().clone();
        let bucket = request.bucket;
        let enabled = request.enabled;
        let store = self.store.clone();
        let authorizer = self.system_authorizer.clone();
        let receipt = run(move || {
            let system = authorizer.load().map_err(authz_status)?;
            require_allowed(
                system
                    .allows_bucket_policy(caller.subject(), storage_tenant.as_str(), &bucket)
                    .map_err(authz_evaluation_status)?,
                "public bucket policy management is not authorized",
            )?;
            let tenant = storage_tenant.clone();
            let response_bucket = bucket.clone();
            store
                .set_bucket_public_read(SetBucketPublicReadRequest {
                    storage_tenant,
                    bucket,
                    enabled,
                    principal: caller.subject().clone(),
                    expected_authorization_revision: system.revision,
                    expected_binding_generation: system.binding_generation,
                })
                .map(|receipt| (tenant, response_bucket, receipt))
                .map_err(credential_store_status)
        })
        .await?;
        Ok(Response::new(api::SetBucketPublicReadResponse {
            storage_tenant: receipt.0.to_string(),
            bucket: receipt.1,
            enabled,
            authorization_revision: receipt.2.authorization_revision.0,
            replayed: receipt.2.replayed,
        }))
    }

    async fn grant_application_role(
        &self,
        request: Request<api::ApplicationRoleRequest>,
    ) -> Result<Response<api::ApplicationRoleResponse>, Status> {
        self.change_application_role(request, true).await
    }

    async fn revoke_application_role(
        &self,
        request: Request<api::ApplicationRoleRequest>,
    ) -> Result<Response<api::ApplicationRoleResponse>, Status> {
        self.change_application_role(request, false).await
    }
}

impl AdministrationServiceImpl {
    async fn change_application_role(
        &self,
        request: Request<api::ApplicationRoleRequest>,
        granted: bool,
    ) -> Result<Response<api::ApplicationRoleResponse>, Status> {
        let caller = caller(&request)?;
        if let Some(distributed) = self.distributed.as_ref() {
            let bearer = OriginalBearer::from_metadata(request.metadata())?;
            let response = distributed
                .change_application_role(caller, bearer, request.into_inner(), granted)
                .await?;
            return Ok(Response::new(response));
        }
        let request = request.into_inner();
        let target = role_target_from_api(request.target)?;
        let store = self.store.clone();
        let authorizer = self.system_authorizer.clone();
        let receipt = run(move || {
            let system = authorizer.load().map_err(authz_status)?;
            require_role_management(&system, &caller, &target)?;
            store
                .set_application_role(SetApplicationRoleRequest {
                    storage_tenant: caller.storage_tenant().clone(),
                    app_id: request.app_id,
                    target,
                    granted,
                    principal: caller.subject().clone(),
                    expected_authorization_revision: system.revision,
                    expected_binding_generation: system.binding_generation,
                })
                .map_err(credential_store_status)
        })
        .await?;
        Ok(Response::new(api::ApplicationRoleResponse {
            authorization_revision: receipt.authorization_revision.0,
            replayed: receipt.replayed,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    async fn refresh_joining_preparation(
        &self,
        cluster_id: keldra_consensus::ClusterId,
        node_id: NodeId,
        peer_address: PeerAddress,
        storage_weight_millionths: u32,
        seeds: Vec<JoinSeed>,
        existing: NodeDescriptor,
    ) -> Result<(PathBuf, JoinBundle), Status> {
        let current = match join_bundle::load_for_request(
            &self.join_bundle_directory,
            cluster_id,
            node_id,
            &peer_address,
            storage_weight_millionths,
        ) {
            Ok(current) => Some(current),
            // The operator is explicitly told to copy and delete this file.
            // The committed descriptor retains the exact old public pair
            // needed to fence one newly generated private preparation.
            Err(JoinBundleError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(join_bundle_status(error)),
        };
        if let Some((_, current_bundle)) = &current
            && joining_descriptor(current_bundle)? != existing
        {
            return Err(Status::failed_precondition(
                "the retained JOINING descriptor differs from the prepared bundle",
            ));
        }
        let mut prepared = match join_bundle::load_refresh(&self.join_bundle_directory, node_id) {
            Ok(prepared) => Some(prepared),
            Err(JoinBundleError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(join_bundle_status(error)),
        };
        if prepared.is_none()
            && current
                .as_ref()
                .is_some_and(|(_, bundle)| bundle.seeds() == seeds.as_slice())
        {
            return Ok(current.expect("current bundle was checked above"));
        }
        if prepared
            .as_ref()
            .is_some_and(|(_, bundle)| bundle.seeds() != seeds.as_slice())
        {
            // This candidate was never committed and the ACTIVE seed view has
            // moved again. It is safe to replace only because reconciliation
            // above proved its pin/capability pair is not committed.
            join_bundle::discard_refresh(&self.join_bundle_directory, node_id)
                .map_err(join_bundle_status)?;
            prepared = None;
        }
        let (_, replacement) = match prepared {
            Some(prepared) => prepared,
            None => join_bundle::prepare_refresh(
                &self.join_bundle_directory,
                cluster_id,
                node_id,
                peer_address,
                storage_weight_millionths,
                seeds,
            )
            .map_err(join_bundle_status)?,
        };
        let replacement_descriptor = joining_descriptor(&replacement)?;
        let transition = self
            .decisions
            .state()
            .map_err(decision_status)?
            .cluster_control()
            .transition()
            .cloned()
            .ok_or_else(|| {
                Status::failed_precondition("JOINING descriptor has no ADD transition")
            })?;
        let committed = self
            .decisions
            .submit(Command::RefreshJoiningNodePreparation {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                node_id,
                started_log_index: transition.started_log_index,
                expected_peer_spki_sha256: existing.current_peer_spki_sha256,
                expected_join_capability_hash: existing.join_capability_hash.ok_or_else(|| {
                    Status::failed_precondition("JOINING descriptor has no join capability")
                })?,
                replacement_peer_spki_sha256: replacement_descriptor.current_peer_spki_sha256,
                replacement_join_capability_hash: replacement_descriptor
                    .join_capability_hash
                    .ok_or_else(|| {
                        Status::failed_precondition("replacement has no join capability")
                    })?,
            })
            .await;
        let committed = match committed {
            Ok(committed) => committed,
            Err(
                error @ DecisionRaftError::Rejected(ApplyError::JoiningNodeAlreadyRaftMember {
                    ..
                }),
            ) => {
                join_bundle::discard_refresh(&self.join_bundle_directory, node_id)
                    .map_err(join_bundle_status)?;
                return Err(decision_status(error));
            }
            Err(error) => return Err(decision_status(error)),
        };
        match committed.result {
            ApplyResult::JoiningNodePreparationRefreshed(descriptor)
                if descriptor == replacement_descriptor => {}
            result => {
                return Err(Status::internal(format!(
                    "node preparation refresh returned an unexpected result: {result:?}"
                )));
            }
        }
        join_bundle::install_refresh(&self.join_bundle_directory, node_id, &replacement)
            .map_err(join_bundle_status)
    }
}

fn parse_peer_address(value: String) -> Result<PeerAddress, Status> {
    if value.is_empty()
        || value.len() > MAX_PEER_ADDRESS_BYTES
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(Status::invalid_argument(
            "peer_address must contain 1 to 255 non-whitespace, non-control UTF-8 bytes",
        ));
    }
    Ok(PeerAddress(value))
}

fn preflight_node_preparation(
    state: &StateMachine,
    node_id: NodeId,
    peer_address: &PeerAddress,
    storage_weight_millionths: u32,
    is_raft_member: bool,
) -> Result<Option<NodeDescriptor>, Status> {
    let cluster = state.cluster_control();
    if let Some(existing) = cluster.nodes().get(&node_id) {
        if existing.state != NodeState::Joining {
            return Err(Status::failed_precondition("node ID is already active"));
        }
        if is_raft_member {
            return Err(Status::failed_precondition(
                "JOINING node is already in committed Raft membership",
            ));
        }
        let transition = cluster.transition().ok_or_else(|| {
            Status::failed_precondition("JOINING descriptor has no ADD transition")
        })?;
        if transition.kind != MembershipTransitionKind::Add || transition.node_id != node_id {
            return Err(Status::failed_precondition(
                "another cluster membership transition is in progress",
            ));
        }
        if &existing.peer_address != peer_address
            || existing.storage_weight_millionths != storage_weight_millionths
            || existing.supported_protocol
                != (CapabilityRange {
                    min: PEER_PROTOCOL_VERSION,
                    max: PEER_PROTOCOL_VERSION,
                })
            || existing.supported_storage_format
                != (CapabilityRange {
                    min: STORAGE_FORMAT_VERSION,
                    max: STORAGE_FORMAT_VERSION,
                })
            || existing.overlap_peer_spki_sha256.is_some()
            || existing.join_capability_hash.is_none()
        {
            return Err(Status::failed_precondition(
                "the retained JOINING descriptor differs from the request",
            ));
        }
        return Ok(Some(existing.clone()));
    }
    if cluster.used_node_ids().contains(node_id) {
        return Err(Status::already_exists(
            "node ID was previously admitted and cannot be reused",
        ));
    }
    if cluster.transition().is_some() {
        return Err(Status::failed_precondition(
            "another cluster membership transition is in progress",
        ));
    }
    if cluster
        .nodes()
        .values()
        .any(|descriptor| &descriptor.peer_address == peer_address)
    {
        return Err(Status::already_exists("peer address is already admitted"));
    }
    Ok(None)
}

/// Finish a refresh whose Raft descriptor was committed before the process
/// could rename the already-fsynced private bundle into place. The one
/// deterministic temporary file is not a second state plane: only an exact
/// committed public pin/capability pair authorizes its installation.
fn reconcile_committed_refresh(
    directory: &std::path::Path,
    state: &StateMachine,
    node_id: NodeId,
    peer_address: &PeerAddress,
    storage_weight_millionths: u32,
) -> Result<(), Status> {
    let (_, prepared) = match join_bundle::load_refresh(directory, node_id) {
        Ok(prepared) => prepared,
        Err(JoinBundleError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(());
        }
        Err(error) => return Err(join_bundle_status(error)),
    };
    let cluster_id = state
        .cluster_id()
        .ok_or_else(|| Status::failed_precondition("cluster identity has not been initialized"))?;
    prepared
        .ensure_request(cluster_id, node_id, peer_address, storage_weight_millionths)
        .map_err(join_bundle_status)?;
    let prepared_descriptor = joining_descriptor(&prepared)?;
    let Some(committed) = state.cluster_control().nodes().get(&node_id) else {
        // No Raft command can refer to this preparation. Remove the bounded
        // orphan before an initial preparation reuses the node ID.
        return join_bundle::discard_refresh(directory, node_id).map_err(join_bundle_status);
    };
    let stable_fields_match = committed.node_id == prepared_descriptor.node_id
        && committed.peer_address == prepared_descriptor.peer_address
        && committed.storage_weight_millionths == prepared_descriptor.storage_weight_millionths
        && committed.supported_protocol == prepared_descriptor.supported_protocol
        && committed.supported_storage_format == prepared_descriptor.supported_storage_format;
    if !stable_fields_match {
        return Err(Status::failed_precondition(
            "prepared refresh differs from the committed node identity",
        ));
    }
    if committed.current_peer_spki_sha256 != prepared_descriptor.current_peer_spki_sha256 {
        // The replacement was fsynced but its Raft command did not commit.
        // The normal retry path either proposes it or replaces it when the
        // current ACTIVE seed descriptors have changed again.
        return Ok(());
    }
    let capability_matches = match committed.state {
        NodeState::Joining => {
            committed.join_capability_hash == prepared_descriptor.join_capability_hash
        }
        // Activation consumes the join capability but retains the peer pin.
        NodeState::Active => committed.join_capability_hash.is_none(),
    };
    if !capability_matches {
        return Err(Status::failed_precondition(
            "prepared refresh capability differs from committed state",
        ));
    }
    join_bundle::install_refresh(directory, node_id, &prepared)
        .map(|_| ())
        .map_err(join_bundle_status)
}

fn active_join_seeds(state: &StateMachine) -> Result<Vec<JoinSeed>, Status> {
    let seeds = state
        .cluster_control()
        .nodes()
        .values()
        .filter(|descriptor| descriptor.state == NodeState::Active)
        .map(|descriptor| JoinSeed {
            node_id: descriptor.node_id,
            peer_address: descriptor.peer_address.clone(),
            current_peer_spki_sha256: descriptor.current_peer_spki_sha256,
            overlap_peer_spki_sha256: descriptor.overlap_peer_spki_sha256,
        })
        .collect::<Vec<_>>();
    if seeds.is_empty() {
        return Err(Status::failed_precondition(
            "cluster has no ACTIVE seed node",
        ));
    }
    Ok(seeds)
}

fn joining_descriptor(bundle: &JoinBundle) -> Result<NodeDescriptor, Status> {
    Ok(NodeDescriptor {
        node_id: bundle.node_id,
        peer_address: bundle.peer_address.clone(),
        storage_weight_millionths: bundle.storage_weight_millionths,
        state: NodeState::Joining,
        current_peer_spki_sha256: bundle.peer_spki_sha256().map_err(join_bundle_status)?,
        overlap_peer_spki_sha256: None,
        join_capability_hash: Some(bundle.capability_hash()),
        supported_protocol: CapabilityRange {
            min: PEER_PROTOCOL_VERSION,
            max: PEER_PROTOCOL_VERSION,
        },
        supported_storage_format: CapabilityRange {
            min: STORAGE_FORMAT_VERSION,
            max: STORAGE_FORMAT_VERSION,
        },
    })
}

fn require_manage_tenant_or_system(
    system: &crate::authorization::SystemAuthorization,
    caller: &Caller,
) -> Result<(), Status> {
    let allowed = if caller.storage_tenant().is_system() {
        system
            .allows_manage_system(caller.subject())
            .map_err(authz_evaluation_status)?
    } else {
        system
            .allows_storage_tenant(
                caller.subject(),
                caller.storage_tenant().as_str(),
                StorageTenantPermission::ManageTenant,
            )
            .map_err(authz_evaluation_status)?
    };
    require_allowed(
        allowed,
        "application credential management is not authorized",
    )
}

fn require_credential_recovery(
    system: &crate::authorization::SystemAuthorization,
    caller: &Caller,
) -> Result<(), Status> {
    let allowed = caller.storage_tenant().is_system()
        && system
            .allows_manage_system(caller.subject())
            .map_err(authz_evaluation_status)?;
    require_allowed(allowed, "application credential recovery is not authorized")
}

fn require_role_management(
    system: &crate::authorization::SystemAuthorization,
    caller: &Caller,
    target: &ApplicationRoleTarget,
) -> Result<(), Status> {
    let allowed = match target {
        ApplicationRoleTarget::System(_) => {
            if !caller.storage_tenant().is_system() {
                false
            } else {
                system
                    .allows_manage_system(caller.subject())
                    .map_err(authz_evaluation_status)?
            }
        }
        ApplicationRoleTarget::Tenant(_) => {
            if caller.storage_tenant().is_system() {
                false
            } else {
                system
                    .allows_storage_tenant(
                        caller.subject(),
                        caller.storage_tenant().as_str(),
                        StorageTenantPermission::ManageTenant,
                    )
                    .map_err(authz_evaluation_status)?
            }
        }
        ApplicationRoleTarget::Bucket { bucket, .. } => {
            if caller.storage_tenant().is_system() {
                false
            } else {
                system
                    .allows_bucket_policy(
                        caller.subject(),
                        caller.storage_tenant().as_str(),
                        bucket,
                    )
                    .map_err(authz_evaluation_status)?
            }
        }
    };
    require_allowed(allowed, "application role management is not authorized")
}

pub(crate) fn role_target_from_api(
    target: Option<api::application_role_request::Target>,
) -> Result<ApplicationRoleTarget, Status> {
    match target.ok_or_else(|| Status::invalid_argument("role target is required"))? {
        api::application_role_request::Target::System(target) => {
            let role = api::SystemApplicationRole::try_from(target.role)
                .map_err(|_| Status::invalid_argument("system application role is invalid"))?;
            match role {
                api::SystemApplicationRole::Admin => {
                    Ok(ApplicationRoleTarget::System(SystemApplicationRole::Admin))
                }
                api::SystemApplicationRole::Unspecified => Err(Status::invalid_argument(
                    "system application role must be specified",
                )),
            }
        }
        api::application_role_request::Target::Tenant(target) => {
            let role = api::TenantApplicationRole::try_from(target.role)
                .map_err(|_| Status::invalid_argument("tenant application role is invalid"))?;
            let role = match role {
                api::TenantApplicationRole::Owner => TenantApplicationRole::Owner,
                api::TenantApplicationRole::Admin => TenantApplicationRole::Admin,
                api::TenantApplicationRole::Reader => TenantApplicationRole::Reader,
                api::TenantApplicationRole::ManageTenant => TenantApplicationRole::ManageTenant,
                api::TenantApplicationRole::ReadTenant => TenantApplicationRole::ReadTenant,
                api::TenantApplicationRole::ManageBuckets => TenantApplicationRole::ManageBuckets,
                api::TenantApplicationRole::ManageAuthz => TenantApplicationRole::ManageAuthz,
                api::TenantApplicationRole::Unspecified => {
                    return Err(Status::invalid_argument(
                        "tenant application role must be specified",
                    ));
                }
            };
            Ok(ApplicationRoleTarget::Tenant(role))
        }
        api::application_role_request::Target::Bucket(target) => {
            let role = api::BucketApplicationRole::try_from(target.role)
                .map_err(|_| Status::invalid_argument("bucket application role is invalid"))?;
            let role = match role {
                api::BucketApplicationRole::Owner => BucketApplicationRole::Owner,
                api::BucketApplicationRole::Admin => BucketApplicationRole::Admin,
                api::BucketApplicationRole::Reader => BucketApplicationRole::Reader,
                api::BucketApplicationRole::Writer => BucketApplicationRole::Writer,
                api::BucketApplicationRole::GetObject => BucketApplicationRole::GetObject,
                api::BucketApplicationRole::PutObject => BucketApplicationRole::PutObject,
                api::BucketApplicationRole::DeleteObject => BucketApplicationRole::DeleteObject,
                api::BucketApplicationRole::ManagePolicy => BucketApplicationRole::ManagePolicy,
                api::BucketApplicationRole::Unspecified => {
                    return Err(Status::invalid_argument(
                        "bucket application role must be specified",
                    ));
                }
            };
            Ok(ApplicationRoleTarget::Bucket {
                bucket: target.bucket,
                role,
            })
        }
    }
}

fn credential_to_api(
    credential: keldra_store::ApplicationCredential,
    replayed: bool,
) -> api::ApplicationCredential {
    api::ApplicationCredential {
        storage_tenant: credential.storage_tenant.to_string(),
        app_id: credential.app_id,
        client_id: credential.client_id,
        active: credential.active,
        replayed,
    }
}

fn credential_state_to_api(receipt: CredentialMutationReceipt) -> api::ApplicationCredentialState {
    api::ApplicationCredentialState {
        storage_tenant: receipt.credential.storage_tenant.to_string(),
        app_id: receipt.credential.app_id,
        client_id: receipt.credential.client_id,
        active: receipt.credential.active,
        replayed: receipt.replayed,
    }
}

fn versioning_from_api(value: i32) -> Result<StoreObjectVersioning, Status> {
    match api::ObjectVersioning::try_from(value) {
        Ok(api::ObjectVersioning::Unversioned) => Ok(StoreObjectVersioning::Unversioned),
        Ok(api::ObjectVersioning::Enabled) => Ok(StoreObjectVersioning::Enabled),
        Err(_) => Err(Status::invalid_argument(
            "object versioning mode is unknown",
        )),
    }
}

fn versioning_to_api(value: StoreObjectVersioning) -> api::ObjectVersioning {
    match value {
        StoreObjectVersioning::Unversioned => api::ObjectVersioning::Unversioned,
        StoreObjectVersioning::Enabled => api::ObjectVersioning::Enabled,
    }
}

fn caller<T>(request: &Request<T>) -> Result<Caller, Status> {
    request
        .extensions()
        .get::<Caller>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("authenticated caller identity is missing"))
}

fn require_allowed(allowed: bool, message: &'static str) -> Result<(), Status> {
    if allowed {
        Ok(())
    } else {
        Err(Status::permission_denied(message))
    }
}

async fn run<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, Status> + Send + 'static,
) -> Result<T, Status> {
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|_| Status::internal("administration task failed"))?
}

fn credential_store_status(error: CredentialRepositoryError) -> Status {
    match error {
        CredentialRepositoryError::InvalidInput(message) => Status::invalid_argument(message),
        CredentialRepositoryError::AlreadyExists(message) => Status::already_exists(message),
        CredentialRepositoryError::NotFound(message) => Status::not_found(message),
        CredentialRepositoryError::Conflict(message) => Status::failed_precondition(message),
        CredentialRepositoryError::AlreadyBootstrapped => {
            Status::failed_precondition("system bootstrap has already completed")
        }
        CredentialRepositoryError::Authorization(error) => authz_status(error),
        CredentialRepositoryError::Entropy(_) | CredentialRepositoryError::Storage(_) => {
            Status::internal("credential or provisioning storage failed")
        }
    }
}

fn decision_status(error: DecisionRaftError) -> Status {
    match error {
        DecisionRaftError::ForwardToLeader { .. } | DecisionRaftError::Unavailable(_) => {
            Status::unavailable(error.to_string())
        }
        DecisionRaftError::LeaderTimeout | DecisionRaftError::SnapshotTimeout => {
            Status::deadline_exceeded(error.to_string())
        }
        DecisionRaftError::Rejected(
            ApplyError::NodeIdAlreadyUsed { .. }
            | ApplyError::PeerAddressAlreadyUsed
            | ApplyError::PeerSpkiAlreadyUsed
            | ApplyError::JoinCapabilityAlreadyUsed,
        ) => Status::already_exists(error.to_string()),
        DecisionRaftError::Rejected(
            ApplyError::InvalidNodeId
            | ApplyError::InvalidPeerAddress
            | ApplyError::InvalidStorageWeight
            | ApplyError::InvalidCapabilityRange { .. }
            | ApplyError::InvalidPeerSpki
            | ApplyError::InvalidJoinCapabilityHash,
        ) => Status::invalid_argument(error.to_string()),
        DecisionRaftError::Rejected(_) => Status::failed_precondition(error.to_string()),
        DecisionRaftError::InvalidNodeId | DecisionRaftError::Configuration(_) => {
            Status::invalid_argument(error.to_string())
        }
        DecisionRaftError::Storage(_) | DecisionRaftError::StatePoisoned => {
            Status::internal("cluster decision state could not be updated")
        }
    }
}

fn join_bundle_status(error: JoinBundleError) -> Status {
    match error {
        JoinBundleError::Conflict(message) => Status::failed_precondition(message),
        JoinBundleError::AlreadyExists => {
            Status::already_exists("join bundle path is already in use")
        }
        JoinBundleError::Invalid(message) => Status::failed_precondition(message),
        JoinBundleError::UnsupportedFormat(_) => {
            Status::failed_precondition("existing join bundle format is unsupported")
        }
        #[cfg(not(unix))]
        JoinBundleError::UnsupportedPlatform => {
            Status::failed_precondition("join bundle creation requires a Unix host")
        }
        JoinBundleError::Io(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Status::failed_precondition("prepared join bundle no longer exists")
        }
        JoinBundleError::Io(_) => Status::internal("join bundle could not be persisted"),
    }
}

fn authz_status(error: AuthzStoreError) -> Status {
    let message = error.to_string();
    match error {
        AuthzStoreError::InvalidInput(message) => Status::invalid_argument(message),
        AuthzStoreError::RevisionConflict { .. }
        | AuthzStoreError::BindingGenerationConflict { .. } => {
            Status::aborted("authorization state changed; retry the request")
        }
        AuthzStoreError::MissingBinding(_, _) | AuthzStoreError::SchemaNotFound(_, _) => {
            Status::failed_precondition(message)
        }
        AuthzStoreError::RevisionNotAvailable { .. } => Status::unavailable(message),
        AuthzStoreError::RevisionExpired { .. } => Status::failed_precondition(message),
        AuthzStoreError::ReceiptCapacity | AuthzStoreError::SourceJournalCapacity => {
            Status::resource_exhausted(message)
        }
        AuthzStoreError::OperationMismatch => Status::already_exists(message),
        AuthzStoreError::RealmMutationLineageGap { .. }
        | AuthzStoreError::RealmMutationStale { .. }
        | AuthzStoreError::RealmMutationSibling { .. }
        | AuthzStoreError::RealmMutationConflict => {
            Status::unavailable("authorization realm replica is not current")
        }
        AuthzStoreError::InvalidRealmMutation(_) => {
            Status::internal("authorization replication input was invalid")
        }
        AuthzStoreError::Authorization(_) | AuthzStoreError::Storage(_) => {
            Status::internal("authorization state could not be evaluated")
        }
    }
}

fn authz_evaluation_status(_error: keldra_authz::AuthorizationError) -> Status {
    Status::internal("authorization state could not be evaluated")
}

#[cfg(test)]
#[path = "administration_service/tests.rs"]
mod tests;
