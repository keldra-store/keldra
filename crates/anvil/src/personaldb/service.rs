use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anvil_api::v1::personal_db_service_server::PersonalDbService;
use anvil_api::v1::{
    PersonalDbExchangeRequest, PersonalDbExchangeResponse, PersonalDbGrantLeaderLeaseRequest,
    PersonalDbGrantLeaderLeaseResponse, PersonalDbRenewLeaderLeaseRequest,
    PersonalDbRenewLeaderLeaseResponse, PersonalDbWitnessCommitRequest,
    PersonalDbWitnessCommitResponse,
};
use personaldb_core::{DatabaseId, LeaderLease, ProposedLogEntry, ReplicaId, VoterAck};
use personaldb_server::{
    AuthContext, Authorizer, ClientMessage, JsonTransportCodec, ResourceRef, ServerActionKind,
    ServerError, ServerErrorCode, SessionContext, SessionId, TransportCodec, TransportDelivery,
    TransportKind, TransportLimits, WireFrame,
};
use tonic::{Request, Response, Status};

use crate::authentication::{Caller, JwtManager};
use crate::cluster_peer::ClusterPeerTransport;
use crate::cluster_peer::{RoutedCall, RoutedPersonalDbHandler};
use crate::distributed_list::{DistributedObjectLister, OriginalBearer};
use crate::logical_name_resolution::LogicalNameResolver;
use crate::serving_fence::ServingAuthority;
use crate::v05::{ObjectServiceImpl, deadline_remaining, request_deadline, run_request_until};

use super::instances::{PersonalDbInstance, PersonalDbInstances};
use super::placement::{HrwPrimaryResolver, PersonalDbPrimary};
use super::scope::{PersonalDbScopeLease, PersonalDbStorageId, PersonalDbStorageScope};

const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CANONICAL_JSON_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct PersonalDbServiceImpl {
    local_node: anvil_consensus::NodeId,
    resolver: HrwPrimaryResolver,
    instances: PersonalDbInstances,
    names: LogicalNameResolver,
    peers: ClusterPeerTransport,
    tokens: JwtManager,
    codec: JsonTransportCodec,
    limits: TransportLimits,
    request_timeout: Duration,
}

impl PersonalDbServiceImpl {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        local_node: anvil_consensus::NodeId,
        resolver: HrwPrimaryResolver,
        serving: ServingAuthority,
        object_service: ObjectServiceImpl,
        lister: DistributedObjectLister,
        names: LogicalNameResolver,
        authorization: crate::authoritative_system::AuthoritativeSystemAuthorization,
        peers: ClusterPeerTransport,
        tokens: JwtManager,
        request_timeout: Duration,
    ) -> anyhow::Result<Self> {
        let instances = PersonalDbInstances::new(
            local_node,
            resolver.clone(),
            serving,
            object_service,
            lister,
            authorization,
            &tokens,
        )?;
        Ok(Self {
            local_node,
            resolver,
            instances,
            names,
            peers,
            tokens,
            codec: JsonTransportCodec,
            limits: TransportLimits::default(),
            request_timeout,
        })
    }

    pub(crate) async fn routed_exchange(
        &self,
        bearer: &str,
        request: PersonalDbExchangeRequest,
    ) -> Result<PersonalDbExchangeResponse, Status> {
        let caller = self
            .tokens
            .verify(bearer)
            .map_err(|_| Status::unauthenticated("the bearer token is invalid or expired"))?;
        let original = OriginalBearer::from_signed_token_for_peer(bearer);
        self.exchange_local(caller, original, request).await
    }

    async fn exchange_local(
        &self,
        caller: Caller,
        bearer: OriginalBearer,
        request: PersonalDbExchangeRequest,
    ) -> Result<PersonalDbExchangeResponse, Status> {
        let frame = self
            .codec
            .decode_frame(&request.frame_json, &self.limits)
            .map_err(server_status)?;
        let database_id = frame.database_group.clone();
        let storage = self
            .storage_scope(&caller, &bearer, &request.bucket)
            .await?;
        let instance = self
            .instances
            .get(storage.storage_id())
            .map_err(server_status)?;
        let _scope = if let Some(database_id) = database_id.as_ref() {
            Some(
                instance
                    .scopes
                    .enter(database_id, storage.clone())
                    .map_err(object_store_status)?,
            )
        } else {
            None
        };
        let context = session_context(&caller, &frame, &request.bucket);
        let deliveries = instance
            .runtime
            .exchange(context, frame, &self.codec, &self.limits)
            .await
            .map_err(server_status)?;
        let mut frames = Vec::new();
        for delivery in deliveries {
            let frame = match delivery {
                TransportDelivery::Respond(frame)
                | TransportDelivery::Stream { frame }
                | TransportDelivery::SendTo { frame, .. }
                | TransportDelivery::Broadcast { frame, .. }
                | TransportDelivery::Forward { frame, .. }
                | TransportDelivery::Redirect { frame, .. } => Some(frame),
                TransportDelivery::Schedule(_) | TransportDelivery::Noop => None,
            };
            if let Some(frame) = frame {
                frames.push(
                    self.codec
                        .encode_frame(&frame, &self.limits)
                        .map_err(server_status)?,
                );
            }
        }
        Ok(PersonalDbExchangeResponse { frame_json: frames })
    }

    async fn storage_id(
        &self,
        caller: &Caller,
        bucket: &str,
    ) -> Result<PersonalDbStorageId, Status> {
        let tenant = caller.storage_tenant().as_str();
        let (tenant_id, bucket_id) = self.names.resolve_bucket_ids(tenant, bucket).await?;
        Ok(PersonalDbStorageId::new(tenant_id, bucket_id))
    }

    async fn storage_scope(
        &self,
        caller: &Caller,
        bearer: &OriginalBearer,
        bucket: &str,
    ) -> Result<PersonalDbStorageScope, Status> {
        let tenant = caller.storage_tenant().as_str().to_owned();
        let storage = self.storage_id(caller, bucket).await?;
        Ok(PersonalDbStorageScope {
            tenant,
            bucket: bucket.to_owned(),
            tenant_id: storage.tenant_id,
            bucket_id: storage.bucket_id,
            caller: caller.clone(),
            bearer: bearer.clone(),
        })
    }

    async fn primary_for(
        &self,
        caller: &Caller,
        bucket: &str,
        database_id: DatabaseId,
    ) -> Result<PersonalDbPrimary, Status> {
        let storage = self.storage_id(caller, bucket).await?;
        self.resolver
            .current(&storage.group(database_id))
            .map_err(server_status)
    }

    fn database_id(
        &self,
        request: &PersonalDbExchangeRequest,
    ) -> Result<Option<DatabaseId>, Status> {
        self.codec
            .decode_frame(&request.frame_json, &self.limits)
            .map(|frame| frame.database_group)
            .map_err(server_status)
    }

    async fn preauthorize_remote(
        &self,
        caller: &Caller,
        bearer: &OriginalBearer,
        request: &PersonalDbExchangeRequest,
        database_id: &DatabaseId,
    ) -> Result<(), Status> {
        let storage = self.storage_scope(caller, bearer, &request.bucket).await?;
        let instance = self
            .instances
            .get(storage.storage_id())
            .map_err(server_status)?;
        let _scope = instance
            .scopes
            .enter(database_id, storage)
            .map_err(object_store_status)?;
        let frame = self
            .codec
            .decode_frame(&request.frame_json, &self.limits)
            .map_err(server_status)?;
        let message = self
            .codec
            .decode_client_message(&frame)
            .map_err(server_status)?;
        let decision = instance
            .authorizer
            .authorize(
                &session_context(caller, &frame, &request.bucket).auth,
                message_action(&message),
                ResourceRef::DatabaseGroup(database_id.clone()),
            )
            .await
            .map_err(server_status)?;
        if decision.allowed {
            Ok(())
        } else {
            Err(Status::permission_denied(
                decision
                    .reason
                    .unwrap_or_else(|| "PersonalDB permission denied".into()),
            ))
        }
    }

    async fn authorize_authority_scope(
        &self,
        caller: &Caller,
        bearer: &OriginalBearer,
        bucket: &str,
        database_id: &DatabaseId,
    ) -> Result<(Arc<PersonalDbInstance>, PersonalDbScopeLease), Status> {
        let storage = self.storage_scope(caller, bearer, bucket).await?;
        let instance = self
            .instances
            .get(storage.storage_id())
            .map_err(server_status)?;
        let scope = instance
            .scopes
            .enter(database_id, storage)
            .map_err(object_store_status)?;
        let decision = instance
            .authorizer
            .authorize(
                &auth_context(caller),
                ServerActionKind::SubmitWriteProposal,
                ResourceRef::DatabaseGroup(database_id.clone()),
            )
            .await
            .map_err(server_status)?;
        if decision.allowed {
            Ok((instance, scope))
        } else {
            Err(Status::permission_denied(decision.reason.unwrap_or_else(
                || "PersonalDB authority permission denied".into(),
            )))
        }
    }

    async fn grant_leader_lease_local(
        &self,
        caller: Caller,
        bearer: OriginalBearer,
        request: PersonalDbGrantLeaderLeaseRequest,
    ) -> Result<PersonalDbGrantLeaderLeaseResponse, Status> {
        let database_id: DatabaseId = decode_canonical_json(&request.database_id_json)?;
        let leader_replica: ReplicaId = decode_canonical_json(&request.leader_replica_json)?;
        let duration = lease_duration(request.duration_millis)?;
        let (instance, _scope) = self
            .authorize_authority_scope(&caller, &bearer, &request.bucket, &database_id)
            .await?;
        let lease = instance
            .runtime
            .grant_leader_lease(&database_id, leader_replica, duration)
            .await
            .map_err(server_status)?;
        Ok(PersonalDbGrantLeaderLeaseResponse {
            leader_lease_json: encode_canonical_json(&lease)?,
        })
    }

    async fn renew_leader_lease_local(
        &self,
        caller: Caller,
        bearer: OriginalBearer,
        request: PersonalDbRenewLeaderLeaseRequest,
    ) -> Result<PersonalDbRenewLeaderLeaseResponse, Status> {
        let lease: LeaderLease = decode_canonical_json(&request.leader_lease_json)?;
        let duration = lease_duration(request.duration_millis)?;
        let (instance, _scope) = self
            .authorize_authority_scope(&caller, &bearer, &request.bucket, &lease.database_id)
            .await?;
        let renewed = instance
            .runtime
            .renew_leader_lease(&lease.database_id, &lease, duration)
            .await
            .map_err(server_status)?;
        Ok(PersonalDbRenewLeaderLeaseResponse {
            leader_lease_json: encode_canonical_json(&renewed)?,
        })
    }

    async fn witness_commit_local(
        &self,
        caller: Caller,
        bearer: OriginalBearer,
        request: PersonalDbWitnessCommitRequest,
    ) -> Result<PersonalDbWitnessCommitResponse, Status> {
        let proposed: ProposedLogEntry = decode_canonical_json(&request.proposed_log_entry_json)?;
        let voter_acks = request
            .voter_ack_json
            .iter()
            .map(|encoded| decode_canonical_json::<VoterAck>(encoded))
            .collect::<Result<Vec<_>, _>>()?;
        let (instance, _scope) = self
            .authorize_authority_scope(&caller, &bearer, &request.bucket, &proposed.database_id)
            .await?;
        let committed = instance
            .runtime
            .witness_commit(auth_context(&caller), proposed, voter_acks)
            .await
            .map_err(server_status)?;
        Ok(PersonalDbWitnessCommitResponse {
            committed_entry_json: encode_canonical_json(&committed)?,
        })
    }
}

#[tonic::async_trait]
impl PersonalDbService for PersonalDbServiceImpl {
    async fn exchange(
        &self,
        request: Request<PersonalDbExchangeRequest>,
    ) -> Result<Response<PersonalDbExchangeResponse>, Status> {
        let deadline = request_deadline(request.metadata(), self.request_timeout)?;
        run_request_until(
            deadline,
            async {
                let caller = request
                    .extensions()
                    .get::<Caller>()
                    .cloned()
                    .ok_or_else(|| Status::unauthenticated("authenticated caller is required"))?;
                let bearer = OriginalBearer::from_metadata(request.metadata())?;
                let value = request.into_inner();
                if let Some(database_id) = self.database_id(&value)? {
                    let primary = self
                        .primary_for(&caller, &value.bucket, database_id)
                        .await?;
                    if primary.node_id != self.local_node {
                        self.preauthorize_remote(
                            &caller,
                            &bearer,
                            &value,
                            &primary.assignment.database_id,
                        )
                        .await?;
                        return self
                            .peers
                            .route_personaldb_exchange(
                                primary.node_id,
                                &primary.peer_address,
                                bearer.signed_token(),
                                value,
                                deadline_remaining(deadline)?,
                            )
                            .await
                            .map(Response::new);
                    }
                }
                self.exchange_local(caller, bearer, value)
                    .await
                    .map(Response::new)
            },
            "PersonalDB request deadline exceeded",
        )
        .await
    }

    async fn grant_leader_lease(
        &self,
        request: Request<PersonalDbGrantLeaderLeaseRequest>,
    ) -> Result<Response<PersonalDbGrantLeaderLeaseResponse>, Status> {
        let deadline = request_deadline(request.metadata(), self.request_timeout)?;
        run_request_until(
            deadline,
            async {
                let caller = authenticated_caller(&request)?;
                let bearer = OriginalBearer::from_metadata(request.metadata())?;
                let value = request.into_inner();
                let database_id: DatabaseId = decode_canonical_json(&value.database_id_json)?;
                let primary = self
                    .primary_for(&caller, &value.bucket, database_id.clone())
                    .await?;
                if primary.node_id != self.local_node {
                    let (_instance, _scope) = self
                        .authorize_authority_scope(&caller, &bearer, &value.bucket, &database_id)
                        .await?;
                    return self
                        .peers
                        .route_personaldb_grant_leader_lease(
                            primary.node_id,
                            &primary.peer_address,
                            bearer.signed_token(),
                            value,
                            deadline_remaining(deadline)?,
                        )
                        .await
                        .map(Response::new);
                }
                self.grant_leader_lease_local(caller, bearer, value)
                    .await
                    .map(Response::new)
            },
            "PersonalDB request deadline exceeded",
        )
        .await
    }

    async fn renew_leader_lease(
        &self,
        request: Request<PersonalDbRenewLeaderLeaseRequest>,
    ) -> Result<Response<PersonalDbRenewLeaderLeaseResponse>, Status> {
        let deadline = request_deadline(request.metadata(), self.request_timeout)?;
        run_request_until(
            deadline,
            async {
                let caller = authenticated_caller(&request)?;
                let bearer = OriginalBearer::from_metadata(request.metadata())?;
                let value = request.into_inner();
                let lease: LeaderLease = decode_canonical_json(&value.leader_lease_json)?;
                let primary = self
                    .primary_for(&caller, &value.bucket, lease.database_id.clone())
                    .await?;
                if primary.node_id != self.local_node {
                    let (_instance, _scope) = self
                        .authorize_authority_scope(
                            &caller,
                            &bearer,
                            &value.bucket,
                            &lease.database_id,
                        )
                        .await?;
                    return self
                        .peers
                        .route_personaldb_renew_leader_lease(
                            primary.node_id,
                            &primary.peer_address,
                            bearer.signed_token(),
                            value,
                            deadline_remaining(deadline)?,
                        )
                        .await
                        .map(Response::new);
                }
                self.renew_leader_lease_local(caller, bearer, value)
                    .await
                    .map(Response::new)
            },
            "PersonalDB request deadline exceeded",
        )
        .await
    }

    async fn witness_commit(
        &self,
        request: Request<PersonalDbWitnessCommitRequest>,
    ) -> Result<Response<PersonalDbWitnessCommitResponse>, Status> {
        let deadline = request_deadline(request.metadata(), self.request_timeout)?;
        run_request_until(
            deadline,
            async {
                let caller = authenticated_caller(&request)?;
                let bearer = OriginalBearer::from_metadata(request.metadata())?;
                let value = request.into_inner();
                let proposed: ProposedLogEntry =
                    decode_canonical_json(&value.proposed_log_entry_json)?;
                let primary = self
                    .primary_for(&caller, &value.bucket, proposed.database_id.clone())
                    .await?;
                if primary.node_id != self.local_node {
                    let (_instance, _scope) = self
                        .authorize_authority_scope(
                            &caller,
                            &bearer,
                            &value.bucket,
                            &proposed.database_id,
                        )
                        .await?;
                    return self
                        .peers
                        .route_personaldb_witness_commit(
                            primary.node_id,
                            &primary.peer_address,
                            bearer.signed_token(),
                            value,
                            deadline_remaining(deadline)?,
                        )
                        .await
                        .map(Response::new);
                }
                self.witness_commit_local(caller, bearer, value)
                    .await
                    .map(Response::new)
            },
            "PersonalDB request deadline exceeded",
        )
        .await
    }
}

#[tonic::async_trait]
impl RoutedPersonalDbHandler for PersonalDbServiceImpl {
    async fn exchange(
        &self,
        call: RoutedCall<PersonalDbExchangeRequest>,
    ) -> Result<PersonalDbExchangeResponse, Status> {
        let bearer = call.bearer().to_owned();
        self.routed_exchange(&bearer, call.into_request()).await
    }

    async fn grant_leader_lease(
        &self,
        call: RoutedCall<PersonalDbGrantLeaderLeaseRequest>,
    ) -> Result<PersonalDbGrantLeaderLeaseResponse, Status> {
        let bearer = call.bearer().to_owned();
        let caller = verify_routed_caller(&self.tokens, &bearer)?;
        self.grant_leader_lease_local(
            caller,
            OriginalBearer::from_signed_token_for_peer(bearer.as_str()),
            call.into_request(),
        )
        .await
    }

    async fn renew_leader_lease(
        &self,
        call: RoutedCall<PersonalDbRenewLeaderLeaseRequest>,
    ) -> Result<PersonalDbRenewLeaderLeaseResponse, Status> {
        let bearer = call.bearer().to_owned();
        let caller = verify_routed_caller(&self.tokens, &bearer)?;
        self.renew_leader_lease_local(
            caller,
            OriginalBearer::from_signed_token_for_peer(bearer.as_str()),
            call.into_request(),
        )
        .await
    }

    async fn witness_commit(
        &self,
        call: RoutedCall<PersonalDbWitnessCommitRequest>,
    ) -> Result<PersonalDbWitnessCommitResponse, Status> {
        let bearer = call.bearer().to_owned();
        let caller = verify_routed_caller(&self.tokens, &bearer)?;
        self.witness_commit_local(
            caller,
            OriginalBearer::from_signed_token_for_peer(bearer.as_str()),
            call.into_request(),
        )
        .await
    }
}

fn message_action(message: &ClientMessage) -> ServerActionKind {
    match message {
        ClientMessage::Hello(_) | ClientMessage::Ping(_) => ServerActionKind::Ping,
        ClientMessage::OpenGroup(_) | ClientMessage::JoinGroup { .. } => {
            ServerActionKind::OpenOrJoinDatabaseGroup
        }
        ClientMessage::ResolvePrimary(_) | ClientMessage::Route { .. } => {
            ServerActionKind::ResolveRoute
        }
        ClientMessage::GetGroupPolicy(_) => ServerActionKind::ReadOrMutateGroupPolicy,
        ClientMessage::SubmitWriteProposal(_)
        | ClientMessage::SubmitLeaderChangeset(_)
        | ClientMessage::SubmitQuorumAck(_)
        | ClientMessage::GetOperationStatus(_)
        | ClientMessage::CancelOperation(_) => ServerActionKind::SubmitWriteProposal,
        ClientMessage::GetSnapshot(_)
        | ClientMessage::RegisterSnapshot(_)
        | ClientMessage::LoadSnapshot { .. } => ServerActionKind::ServeSnapshot,
        ClientMessage::AttachCheck(_) => ServerActionKind::AttachDatabaseGroup,
        ClientMessage::Subscribe(_) | ClientMessage::Unsubscribe(_) | ClientMessage::CatchUp(_) => {
            ServerActionKind::ServeCatchUp
        }
    }
}

fn session_context(caller: &Caller, frame: &WireFrame, bucket: &str) -> SessionContext {
    SessionContext {
        session_id: SessionId::new(format!("grpc:{}", frame.message_id.0)),
        replica_id: None,
        auth: auth_context(caller),
        remote_addr: None,
        transport: TransportKind::Grpc,
        protocol_version: frame.protocol_version,
        metadata: serde_json::json!({
            "schema_version": 1,
            "kind": "anvil_personaldb_transport",
            "data": { "bucket": bucket }
        }),
    }
}

fn auth_context(caller: &Caller) -> AuthContext {
    AuthContext {
        principal_id: format!("{:?}", caller.subject()),
        tenant_id: Some(caller.storage_tenant().as_str().to_owned()),
        claims_json: serde_json::json!({
            "schema_version": 1,
            "kind": "anvil_authenticated_session",
            "data": {}
        }),
    }
}

fn authenticated_caller<T>(request: &Request<T>) -> Result<Caller, Status> {
    request
        .extensions()
        .get::<Caller>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("authenticated caller is required"))
}

fn verify_routed_caller(tokens: &JwtManager, bearer: &str) -> Result<Caller, Status> {
    tokens
        .verify(bearer)
        .map_err(|_| Status::unauthenticated("the bearer token is invalid or expired"))
}

fn decode_canonical_json<T: serde::de::DeserializeOwned>(encoded: &[u8]) -> Result<T, Status> {
    if encoded.is_empty() || encoded.len() > MAX_CANONICAL_JSON_BYTES {
        return Err(Status::invalid_argument(
            "canonical PersonalDB JSON is empty or exceeds 16 MiB",
        ));
    }
    serde_json::from_slice(encoded)
        .map_err(|error| Status::invalid_argument(format!("invalid PersonalDB JSON: {error}")))
}

fn encode_canonical_json<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, Status> {
    let encoded = serde_json::to_vec(value)
        .map_err(|error| Status::internal(format!("encode PersonalDB JSON: {error}")))?;
    if encoded.len() > MAX_CANONICAL_JSON_BYTES {
        return Err(Status::resource_exhausted(
            "canonical PersonalDB JSON exceeds 16 MiB",
        ));
    }
    Ok(encoded)
}

fn lease_duration(duration_millis: u64) -> Result<Duration, Status> {
    if duration_millis == 0 {
        return Err(Status::invalid_argument(
            "PersonalDB leader lease duration must be non-zero",
        ));
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Status::unavailable("system clock predates the Unix epoch"))?
        .as_millis();
    let duration = u128::from(duration_millis);
    let request_margin = EXCHANGE_TIMEOUT.as_millis();
    if now
        .checked_add(duration)
        .and_then(|expiry| expiry.checked_add(request_margin))
        .is_none_or(|expiry| expiry > u128::from(u64::MAX))
    {
        return Err(Status::invalid_argument(
            "PersonalDB leader lease duration overflows its Unix-millisecond expiry",
        ));
    }
    Ok(Duration::from_millis(duration_millis))
}

fn server_status(error: ServerError) -> Status {
    match error.code() {
        ServerErrorCode::Authorization => Status::permission_denied(error.to_string()),
        ServerErrorCode::AuthenticationRequired => Status::unauthenticated(error.to_string()),
        ServerErrorCode::MessageTooLarge | ServerErrorCode::Backpressure => {
            Status::resource_exhausted(error.to_string())
        }
        ServerErrorCode::ProtocolDecode
        | ServerErrorCode::ProtocolCorrelation
        | ServerErrorCode::ProtocolVersion
        | ServerErrorCode::UnsupportedProtocolMessage => {
            Status::invalid_argument(error.to_string())
        }
        _ if error.code().is_retryable() => Status::unavailable(error.to_string()),
        _ => Status::failed_precondition(error.to_string()),
    }
}

fn object_store_status(error: personaldb_server_core::ObjectStoreError) -> Status {
    Status::unavailable(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_authority_values_round_trip_without_a_second_schema() {
        let database = DatabaseId::new("group");
        let encoded = encode_canonical_json(&database).unwrap();
        assert_eq!(
            decode_canonical_json::<DatabaseId>(&encoded).unwrap(),
            database
        );
    }

    #[test]
    fn lease_duration_rejects_zero_and_expiry_overflow() {
        assert_eq!(
            lease_duration(0).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            lease_duration(u64::MAX).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(lease_duration(30_000).unwrap(), Duration::from_secs(30));
    }
}
