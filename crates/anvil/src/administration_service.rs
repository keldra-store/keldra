//! Typed administration backed by the protected Zanzibar system realm.

use std::path::PathBuf;

use anvil_api::v1 as api;
use anvil_api::v1::administration_service_server::AdministrationService;
use anvil_consensus::{
    ApplyError, ApplyResult, CLUSTER_CONTROL_COMMAND_VERSION, CapabilityRange, Command,
    DecisionRaft, DecisionRaftError, MAX_PEER_ADDRESS_BYTES, MembershipTransitionKind,
    NodeDescriptor, NodeId, NodeState, PeerAddress, StateMachine,
};
use anvil_store::{
    ApplicationCredentialRequest, ApplicationRoleTarget, AuthzStoreError, BucketApplicationRole,
    CreateBucketRequest, CredentialMutationReceipt, CredentialRepositoryError,
    ObjectVersioning as StoreObjectVersioning, ProvisionTenantRequest, SetApplicationRoleRequest,
    StorageTenantId, Store, SystemApplicationRole, TenantApplicationRole,
};
use tonic::{Request, Response, Status};

use crate::authentication::Caller;
use crate::authorization::{StorageTenantPermission, SystemAuthorizer};
use crate::join_bundle::{self, JoinBundle, JoinBundleError, JoinSeed};

const PEER_PROTOCOL_VERSION: u16 = 1;
const STORAGE_FORMAT_VERSION: u16 = 1;

#[derive(Clone)]
pub(crate) struct AdministrationServiceImpl {
    store: Store,
    system_authorizer: SystemAuthorizer,
    decisions: DecisionRaft,
    join_bundle_directory: PathBuf,
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
        }
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

    async fn disable_application_credential(
        &self,
        request: Request<api::DisableApplicationCredentialRequest>,
    ) -> Result<Response<api::ApplicationCredentialState>, Status> {
        let caller = caller(&request)?;
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
        cluster_id: anvil_consensus::ClusterId,
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

fn role_target_from_api(
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
    credential: anvil_store::ApplicationCredential,
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
        AuthzStoreError::ReceiptCapacity => Status::resource_exhausted(message),
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

fn authz_evaluation_status(_error: anvil_authz::AuthorizationError) -> Status {
    Status::internal("authorization state could not be evaluated")
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;

    use anvil_api::v1::administration_service_server::AdministrationService;
    use anvil_consensus::{
        CapabilityRange, ClusterId, Command, CommittedPeerPins, JoinCapabilityHash, NodeDescriptor,
        NodeState, PeerSpkiSha256,
    };
    use anvil_store::{StoreOptions, SystemBootstrapRequest};

    use super::*;

    const SECRET: &str = "secret-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    async fn service() -> (tempfile::TempDir, Store, AdministrationServiceImpl) {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(directory.path(), 1))
            .await
            .unwrap();
        store
            .bootstrap_system(SystemBootstrapRequest {
                app_id: "bootstrap-app".into(),
                client_id: "bootstrap-client".into(),
                client_secret: SECRET.into(),
            })
            .unwrap();
        let decisions = DecisionRaft::open(directory.path().join("decisions"), 1, 16, 64 * 1024)
            .await
            .unwrap();
        decisions.ensure_one_node().await.unwrap();
        decisions
            .wait_for_leader(Duration::from_secs(5))
            .await
            .unwrap();
        decisions
            .submit(Command::InitializeCluster {
                cluster_id: ClusterId([12; 16]),
            })
            .await
            .unwrap();
        let admitted = decisions
            .submit(Command::BeginAddNode {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                descriptor: NodeDescriptor {
                    node_id: NodeId(1),
                    peer_address: PeerAddress("anvil-local://1".into()),
                    storage_weight_millionths: 1_000_000,
                    state: NodeState::Joining,
                    current_peer_spki_sha256: PeerSpkiSha256([1; 32]),
                    overlap_peer_spki_sha256: None,
                    join_capability_hash: Some(JoinCapabilityHash([1; 32])),
                    supported_protocol: CapabilityRange { min: 1, max: 1 },
                    supported_storage_format: CapabilityRange { min: 1, max: 1 },
                },
            })
            .await
            .unwrap();
        for _ in 0..2 {
            decisions
                .submit(Command::CompleteMembershipTransition {
                    format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                    started_log_index: admitted.log_index,
                })
                .await
                .unwrap();
        }
        let service = AdministrationServiceImpl::new(
            store.clone(),
            decisions,
            directory.path().to_path_buf(),
        );
        (directory, store, service)
    }

    fn authenticated<T>(tenant: StorageTenantId, app_id: &str, body: T) -> Request<T> {
        let mut request = Request::new(body);
        request
            .extensions_mut()
            .insert(Caller::from_authenticated_application(tenant, app_id).unwrap());
        request
    }

    fn prepare_request() -> api::PrepareNodeRequest {
        api::PrepareNodeRequest {
            node_id: 2,
            peer_address: "127.0.0.1:50062".into(),
            storage_weight_millionths: 500_000,
        }
    }

    #[tokio::test]
    async fn prepare_node_is_authorized_private_redacted_and_exactly_retryable() {
        let (_directory, _store, service) = service().await;
        let first = service
            .prepare_node(authenticated(
                StorageTenantId::system(),
                "bootstrap-app",
                prepare_request(),
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(first.cluster_id, vec![12; 16]);
        assert_eq!(first.node_id, 2);
        assert_eq!(first.peer_spki_sha256.len(), 32);
        let path = PathBuf::from(&first.join_bundle_path);
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o7777,
            0o600
        );
        let encoded = std::fs::read(&path).unwrap();
        let bundle = join_bundle::load(&path).unwrap();
        let state = service.decisions.state().unwrap();
        let descriptor = &state.cluster_control().nodes()[&NodeId(2)];
        assert_eq!(descriptor.state, NodeState::Joining);
        assert_eq!(
            descriptor.current_peer_spki_sha256.0.to_vec(),
            first.peer_spki_sha256
        );
        assert_eq!(
            descriptor.join_capability_hash,
            Some(bundle.capability_hash())
        );
        assert_ne!(
            descriptor.join_capability_hash.unwrap().0,
            bundle.capability()
        );
        assert_eq!(
            state.cluster_control().transition().unwrap().node_id,
            NodeId(2)
        );

        let retry = service
            .prepare_node(authenticated(
                StorageTenantId::system(),
                "bootstrap-app",
                prepare_request(),
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(retry, first);
        assert_eq!(std::fs::read(&path).unwrap(), encoded);

        let mut changed = prepare_request();
        changed.storage_weight_millionths += 1;
        let rejected = service
            .prepare_node(authenticated(
                StorageTenantId::system(),
                "bootstrap-app",
                changed,
            ))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::FailedPrecondition);
        assert_eq!(std::fs::read(path).unwrap(), encoded);
    }

    #[tokio::test]
    async fn stale_joining_preparation_refreshes_only_unused_material_and_seed_view() {
        let (directory, _store, service) = service().await;
        let first = service
            .prepare_node(authenticated(
                StorageTenantId::system(),
                "bootstrap-app",
                prepare_request(),
            ))
            .await
            .unwrap()
            .into_inner();
        let first_path = PathBuf::from(&first.join_bundle_path);
        let first_bundle = join_bundle::load(&first_path).unwrap();
        // This is the documented operator handoff: copy the bundle to the new
        // host, then delete the generated server-side file.
        std::fs::remove_file(&first_path).unwrap();
        std::fs::File::open(directory.path())
            .unwrap()
            .sync_all()
            .unwrap();
        let before = service.decisions.state().unwrap();
        let original_descriptor = before.cluster_control().nodes()[&NodeId(2)].clone();
        let original_transition = before.cluster_control().transition().cloned().unwrap();

        let new_seed_pin = PeerSpkiSha256([99; 32]);
        service
            .decisions
            .submit(Command::StagePeerSpkiOverlap {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                node_id: NodeId(1),
                expected_current: PeerSpkiSha256([1; 32]),
                overlap: new_seed_pin,
            })
            .await
            .unwrap();
        let fresh_state = service.decisions.state().unwrap();
        let fresh_seeds = active_join_seeds(&fresh_state).unwrap();

        // Simulate a crash after fsyncing the candidate but before proposing
        // the one replacement command. The API must reuse these exact bytes.
        let (_, prepared) = join_bundle::prepare_refresh(
            directory.path(),
            ClusterId([12; 16]),
            NodeId(2),
            PeerAddress("127.0.0.1:50062".into()),
            500_000,
            fresh_seeds.clone(),
        )
        .unwrap();
        let prepared_pin = prepared.peer_spki_sha256().unwrap();
        let prepared_capability = prepared.capability_hash();

        let refreshed = service
            .prepare_node(authenticated(
                StorageTenantId::system(),
                "bootstrap-app",
                prepare_request(),
            ))
            .await
            .unwrap()
            .into_inner();
        let refreshed_bundle = join_bundle::load(&first_path).unwrap();
        assert_eq!(refreshed_bundle, prepared);
        assert_eq!(refreshed_bundle.seeds(), fresh_seeds.as_slice());
        assert!(!join_bundle::refresh_path(directory.path(), NodeId(2)).exists());
        assert_ne!(refreshed.peer_spki_sha256, first.peer_spki_sha256);
        assert_ne!(
            refreshed_bundle.capability_hash(),
            first_bundle.capability_hash()
        );

        let after = service.decisions.state().unwrap();
        let replacement_descriptor = &after.cluster_control().nodes()[&NodeId(2)];
        let mut expected = original_descriptor.clone();
        expected.current_peer_spki_sha256 = prepared_pin;
        expected.join_capability_hash = Some(prepared_capability);
        assert_eq!(replacement_descriptor, &expected);
        assert_eq!(
            after.cluster_control().transition(),
            Some(&original_transition),
            "refresh must preserve the original ADD transition identity"
        );
        assert!(
            !CommittedPeerPins {
                current: replacement_descriptor.current_peer_spki_sha256,
                overlap: replacement_descriptor.overlap_peer_spki_sha256,
            }
            .contains(original_descriptor.current_peer_spki_sha256)
        );

        let encoded = std::fs::read(&first_path).unwrap();
        let exact_retry = service
            .prepare_node(authenticated(
                StorageTenantId::system(),
                "bootstrap-app",
                prepare_request(),
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(exact_retry, refreshed);
        assert_eq!(std::fs::read(first_path).unwrap(), encoded);
    }

    #[tokio::test]
    async fn committed_refresh_is_installed_after_restart_style_retry() {
        let (directory, _store, service) = service().await;
        let first = service
            .prepare_node(authenticated(
                StorageTenantId::system(),
                "bootstrap-app",
                prepare_request(),
            ))
            .await
            .unwrap()
            .into_inner();
        let final_path = PathBuf::from(&first.join_bundle_path);
        let old_bytes = std::fs::read(&final_path).unwrap();
        service
            .decisions
            .submit(Command::StagePeerSpkiOverlap {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                node_id: NodeId(1),
                expected_current: PeerSpkiSha256([1; 32]),
                overlap: PeerSpkiSha256([98; 32]),
            })
            .await
            .unwrap();
        let state = service.decisions.state().unwrap();
        let existing = state.cluster_control().nodes()[&NodeId(2)].clone();
        let transition = state.cluster_control().transition().cloned().unwrap();
        let (_, prepared) = join_bundle::prepare_refresh(
            directory.path(),
            ClusterId([12; 16]),
            NodeId(2),
            PeerAddress("127.0.0.1:50062".into()),
            500_000,
            active_join_seeds(&state).unwrap(),
        )
        .unwrap();
        let replacement = joining_descriptor(&prepared).unwrap();
        service
            .decisions
            .submit(Command::RefreshJoiningNodePreparation {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                node_id: NodeId(2),
                started_log_index: transition.started_log_index,
                expected_peer_spki_sha256: existing.current_peer_spki_sha256,
                expected_join_capability_hash: existing.join_capability_hash.unwrap(),
                replacement_peer_spki_sha256: replacement.current_peer_spki_sha256,
                replacement_join_capability_hash: replacement.join_capability_hash.unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(std::fs::read(&final_path).unwrap(), old_bytes);
        assert!(join_bundle::refresh_path(directory.path(), NodeId(2)).exists());

        let retried = service
            .prepare_node(authenticated(
                StorageTenantId::system(),
                "bootstrap-app",
                prepare_request(),
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            retried.peer_spki_sha256,
            replacement.current_peer_spki_sha256.0.to_vec()
        );
        assert_eq!(join_bundle::load(&final_path).unwrap(), prepared);
        assert!(!join_bundle::refresh_path(directory.path(), NodeId(2)).exists());
        assert_eq!(
            std::fs::metadata(final_path).unwrap().permissions().mode() & 0o7777,
            0o600
        );
    }

    #[tokio::test]
    async fn prepare_node_requires_manage_system_before_creating_private_material() {
        let (directory, _store, service) = service().await;
        let missing = service
            .prepare_node(Request::new(prepare_request()))
            .await
            .unwrap_err();
        assert_eq!(missing.code(), tonic::Code::Unauthenticated);

        let denied = service
            .prepare_node(authenticated(
                StorageTenantId::system(),
                "not-an-admin",
                prepare_request(),
            ))
            .await
            .unwrap_err();
        assert_eq!(denied.code(), tonic::Code::PermissionDenied);
        assert!(!join_bundle::generated_path(directory.path(), NodeId(2)).exists());
        let state = service.decisions.state().unwrap();
        assert!(!state.cluster_control().nodes().contains_key(&NodeId(2)));
        assert!(state.cluster_control().transition().is_none());
    }

    #[tokio::test]
    async fn bootstrap_admin_provisions_tenant_and_owner_can_manage_its_resources() {
        let (_directory, store, service) = service().await;
        let provisioned = service
            .provision_tenant(authenticated(
                StorageTenantId::system(),
                "bootstrap-app",
                api::ProvisionTenantRequest {
                    storage_tenant: "acme".into(),
                    owner_app_id: "acme-owner".into(),
                    owner_client_id: "acme-owner-client".into(),
                    owner_client_secret: SECRET.into(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(provisioned.authorization_revision, 4);
        assert_eq!(provisioned.credential.unwrap().storage_tenant, "acme");

        let acme = StorageTenantId::parse("acme").unwrap();
        let worker = service
            .create_application(authenticated(
                acme.clone(),
                "acme-owner",
                api::CreateApplicationRequest {
                    app_id: "worker".into(),
                    client_id: "worker-client".into(),
                    client_secret: SECRET.into(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(worker.app_id, "worker");
        assert!(worker.active);
        let bucket = service
            .create_bucket(authenticated(
                acme.clone(),
                "acme-owner",
                api::CreateBucketRequest {
                    bucket: "objects".into(),
                    versioning: api::ObjectVersioning::Unversioned as i32,
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(bucket.authorization_revision, 5);
        assert_eq!(bucket.versioning, api::ObjectVersioning::Unversioned as i32);
        let unauthorized = service
            .set_bucket_versioning(authenticated(
                acme.clone(),
                "worker",
                api::SetBucketVersioningRequest {
                    bucket: "objects".into(),
                    versioning: api::ObjectVersioning::Enabled as i32,
                },
            ))
            .await
            .unwrap_err();
        assert_eq!(unauthorized.code(), tonic::Code::PermissionDenied);
        let versioning = service
            .set_bucket_versioning(authenticated(
                acme.clone(),
                "acme-owner",
                api::SetBucketVersioningRequest {
                    bucket: "objects".into(),
                    versioning: api::ObjectVersioning::Enabled as i32,
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(versioning.changed);
        assert_eq!(versioning.storage_tenant, "acme");
        assert_eq!(versioning.bucket, "objects");
        assert_eq!(versioning.versioning, api::ObjectVersioning::Enabled as i32);
        let replay = service
            .set_bucket_versioning(authenticated(
                acme.clone(),
                "acme-owner",
                api::SetBucketVersioningRequest {
                    bucket: "objects".into(),
                    versioning: api::ObjectVersioning::Enabled as i32,
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(!replay.changed);
        let disable = service
            .set_bucket_versioning(authenticated(
                acme.clone(),
                "acme-owner",
                api::SetBucketVersioningRequest {
                    bucket: "objects".into(),
                    versioning: api::ObjectVersioning::Unversioned as i32,
                },
            ))
            .await
            .unwrap_err();
        assert_eq!(disable.code(), tonic::Code::InvalidArgument);
        let role = service
            .grant_application_role(authenticated(
                acme,
                "acme-owner",
                api::ApplicationRoleRequest {
                    app_id: "worker".into(),
                    target: Some(api::application_role_request::Target::Bucket(
                        api::BucketApplicationRoleTarget {
                            bucket: "objects".into(),
                            role: api::BucketApplicationRole::Writer.into(),
                        },
                    )),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(role.authorization_revision, 6);
        let revoked = service
            .revoke_application_role(authenticated(
                StorageTenantId::parse("acme").unwrap(),
                "acme-owner",
                api::ApplicationRoleRequest {
                    app_id: "worker".into(),
                    target: Some(api::application_role_request::Target::Bucket(
                        api::BucketApplicationRoleTarget {
                            bucket: "objects".into(),
                            role: api::BucketApplicationRole::Writer.into(),
                        },
                    )),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(revoked.authorization_revision, 7);
        assert!(!revoked.replayed);
        assert!(
            store
                .application(&StorageTenantId::parse("acme").unwrap(), "worker")
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn tenant_provisioning_rejects_noncanonical_spelling_without_claiming_an_alias() {
        let (_directory, _store, service) = service().await;
        let rejected = service
            .provision_tenant(authenticated(
                StorageTenantId::system(),
                "bootstrap-app",
                api::ProvisionTenantRequest {
                    storage_tenant: "Acme".into(),
                    owner_app_id: "owner".into(),
                    owner_client_id: "owner-client".into(),
                    owner_client_secret: SECRET.into(),
                },
            ))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::InvalidArgument);

        let canonical = service
            .provision_tenant(authenticated(
                StorageTenantId::system(),
                "bootstrap-app",
                api::ProvisionTenantRequest {
                    storage_tenant: "acme".into(),
                    owner_app_id: "owner".into(),
                    owner_client_id: "owner-client".into(),
                    owner_client_secret: SECRET.into(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(canonical.credential.unwrap().storage_tenant, "acme");
        assert!(!canonical.replayed);
    }

    #[tokio::test]
    async fn requests_without_identity_and_unprivileged_apps_fail_closed() {
        let (_directory, _store, service) = service().await;
        let missing = service
            .provision_tenant(Request::new(api::ProvisionTenantRequest {
                storage_tenant: "acme".into(),
                owner_app_id: "owner".into(),
                owner_client_id: "owner-client".into(),
                owner_client_secret: SECRET.into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(missing.code(), tonic::Code::Unauthenticated);

        let create_application = service
            .create_application(Request::new(api::CreateApplicationRequest::default()))
            .await
            .unwrap_err();
        let rotate = service
            .rotate_application_credential(Request::new(
                api::RotateApplicationCredentialRequest::default(),
            ))
            .await
            .unwrap_err();
        let disable = service
            .disable_application_credential(Request::new(
                api::DisableApplicationCredentialRequest::default(),
            ))
            .await
            .unwrap_err();
        let bucket = service
            .create_bucket(Request::new(api::CreateBucketRequest::default()))
            .await
            .unwrap_err();
        let versioning = service
            .set_bucket_versioning(Request::new(api::SetBucketVersioningRequest::default()))
            .await
            .unwrap_err();
        let grant = service
            .grant_application_role(Request::new(api::ApplicationRoleRequest::default()))
            .await
            .unwrap_err();
        let revoke = service
            .revoke_application_role(Request::new(api::ApplicationRoleRequest::default()))
            .await
            .unwrap_err();
        for status in [
            create_application,
            rotate,
            disable,
            bucket,
            versioning,
            grant,
            revoke,
        ] {
            assert_eq!(status.code(), tonic::Code::Unauthenticated);
        }

        let denied = service
            .provision_tenant(authenticated(
                StorageTenantId::system(),
                "not-an-admin",
                api::ProvisionTenantRequest {
                    storage_tenant: "acme".into(),
                    owner_app_id: "owner".into(),
                    owner_client_id: "owner-client".into(),
                    owner_client_secret: SECRET.into(),
                },
            ))
            .await
            .unwrap_err();
        assert_eq!(denied.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn application_rotation_disable_and_system_role_assignment_are_typed_and_authorized() {
        let (_directory, store, service) = service().await;
        let system_app = service
            .create_application(authenticated(
                StorageTenantId::system(),
                "bootstrap-app",
                api::CreateApplicationRequest {
                    app_id: "system-admin".into(),
                    client_id: "system-admin-client".into(),
                    client_secret: SECRET.into(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(system_app.storage_tenant, "_anvil");

        let role = service
            .grant_application_role(authenticated(
                StorageTenantId::system(),
                "bootstrap-app",
                api::ApplicationRoleRequest {
                    app_id: "system-admin".into(),
                    target: Some(api::application_role_request::Target::System(
                        api::SystemApplicationRoleTarget {
                            role: api::SystemApplicationRole::Admin.into(),
                        },
                    )),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(role.authorization_revision, 4);

        service
            .provision_tenant(authenticated(
                StorageTenantId::system(),
                "system-admin",
                api::ProvisionTenantRequest {
                    storage_tenant: "acme".into(),
                    owner_app_id: "acme-owner".into(),
                    owner_client_id: "acme-owner-client".into(),
                    owner_client_secret: SECRET.into(),
                },
            ))
            .await
            .unwrap();
        let acme = StorageTenantId::parse("acme").unwrap();
        service
            .create_application(authenticated(
                acme.clone(),
                "acme-owner",
                api::CreateApplicationRequest {
                    app_id: "worker".into(),
                    client_id: "worker-client".into(),
                    client_secret: SECRET.into(),
                },
            ))
            .await
            .unwrap();

        let replacement = "replacement-0123456789abcdef0123456789abcdef0123456789abcdef";
        let rotated = service
            .rotate_application_credential(authenticated(
                acme.clone(),
                "acme-owner",
                api::RotateApplicationCredentialRequest {
                    app_id: "worker".into(),
                    client_id: "worker-client".into(),
                    client_secret: replacement.into(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(rotated.active);
        assert!(
            store
                .verify_credential("worker-client", replacement)
                .unwrap()
                .is_some()
        );
        let disabled = service
            .disable_application_credential(authenticated(
                acme.clone(),
                "acme-owner",
                api::DisableApplicationCredentialRequest {
                    app_id: "worker".into(),
                    client_id: "worker-client".into(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(!disabled.active);
        assert!(
            store
                .verify_credential("worker-client", replacement)
                .unwrap()
                .is_none()
        );

        let denied = service
            .grant_application_role(authenticated(
                acme,
                "acme-owner",
                api::ApplicationRoleRequest {
                    app_id: "system-admin".into(),
                    target: Some(api::application_role_request::Target::System(
                        api::SystemApplicationRoleTarget {
                            role: api::SystemApplicationRole::Admin.into(),
                        },
                    )),
                },
            ))
            .await
            .unwrap_err();
        assert_eq!(denied.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn unspecified_and_unknown_roles_are_rejected_before_storage() {
        for role in [0, 999] {
            let error = role_target_from_api(Some(api::application_role_request::Target::System(
                api::SystemApplicationRoleTarget { role },
            )))
            .unwrap_err();
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
        }
    }
}
