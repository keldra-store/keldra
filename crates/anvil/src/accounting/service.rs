//! Zanzibar-authorized public accounting lifecycle and query RPCs.

use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use anvil_api::v1::accounting_service_server::AccountingService;
use anvil_api::v1::{
    AccountingDefinition, AccountingFreshness, AccountingSnapshot, DisableAccountingRequest,
    DisableAccountingResponse, EnableAccountingRequest, GetAccountingRequest,
};
use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::ObjectKey;
use tonic::{Request, Response, Status};

use crate::authentication::{Caller, JwtManager};
use crate::authoritative_system::AuthoritativeSystemAuthorization;
use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::{
    AccountingTrafficFlush, ClusterPeerTransport, RoutedAccountingHandler, RoutedCall,
};
use crate::cluster_placement::ClusterPlacement;
use crate::distributed_list::OriginalBearer;
use crate::index_runtime::placement::{IndexIdentity, IndexPlacement};
use crate::index_service::validate_command_id;
use crate::logical_name_resolution::LogicalNameResolver;
use crate::v05::{deadline_remaining, request_deadline, run_request_until};

use super::manager::{read_rollup, read_traffic_source};
use super::{
    AccountingCatalog, AccountingPublisher, LoadedAccountingDefinition, StoredAccountingDefinition,
    StoredTrafficSource, definition_path, derive_accounting_id, validate_prefix,
};

#[derive(Clone)]
pub(crate) struct AccountingServiceImpl {
    local_node: NodeId,
    decisions: DecisionRaft,
    tokens: JwtManager,
    names: LogicalNameResolver,
    authorization: AuthoritativeSystemAuthorization,
    peers: ClusterPeerTransport,
    reader: ClusterObjectReader,
    publisher: AccountingPublisher,
    catalog: AccountingCatalog,
    request_timeout: Duration,
}

impl AccountingServiceImpl {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        local_node: NodeId,
        decisions: DecisionRaft,
        tokens: JwtManager,
        names: LogicalNameResolver,
        authorization: AuthoritativeSystemAuthorization,
        peers: ClusterPeerTransport,
        reader: ClusterObjectReader,
        publisher: AccountingPublisher,
        catalog: AccountingCatalog,
        request_timeout: Duration,
    ) -> Self {
        Self {
            local_node,
            decisions,
            tokens,
            names,
            authorization,
            peers,
            reader,
            publisher,
            catalog,
            request_timeout,
        }
    }

    pub(crate) fn routed_handler(&self) -> Arc<dyn RoutedAccountingHandler> {
        Arc::new(self.clone())
    }

    async fn authorize_and_resolve(
        &self,
        caller: &Caller,
        bucket: &str,
    ) -> Result<(u64, u64), Status> {
        if bucket.is_empty() {
            return Err(Status::invalid_argument("accounting bucket is required"));
        }
        let tenant = caller.storage_tenant().as_str();
        if !self
            .authorization
            .allows_bucket_policy(caller, tenant, bucket)
            .await?
        {
            return Err(Status::permission_denied(
                "bucket accounting requires manage_policy",
            ));
        }
        self.names.resolve_bucket_ids(tenant, bucket).await
    }

    fn placement(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        accounting_id: u64,
    ) -> Result<(ClusterPlacement, IndexPlacement), Status> {
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
        let placement = ClusterPlacement::from_applied(&state)
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let identity = IndexIdentity::new(tenant_id, bucket_id, accounting_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let assignment = IndexPlacement::derive(identity, &placement)
            .map_err(|error| Status::unavailable(error.to_string()))?;
        Ok((placement, assignment))
    }

    fn require_local_builder(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        accounting_id: u64,
        expected_fence: Option<anvil_store::PlacementLogId>,
    ) -> Result<(), Status> {
        let (placement, assignment) = self.placement(tenant_id, bucket_id, accounting_id)?;
        if assignment.builder() != self.local_node {
            return Err(Status::failed_precondition(
                "accounting request did not reach its current weighted-HRW worker",
            ));
        }
        if expected_fence.is_some_and(|expected| expected != placement.fence()) {
            return Err(Status::unavailable(
                "accounting placement changed during routing",
            ));
        }
        Ok(())
    }

    async fn enable_local(
        &self,
        caller: &Caller,
        request: EnableAccountingRequest,
        ids: (u64, u64),
        expected_fence: Option<anvil_store::PlacementLogId>,
    ) -> Result<AccountingDefinition, Status> {
        validate_prefix(&request.path_prefix)?;
        validate_command_id(&request.command_id)?;
        let tenant = caller.storage_tenant().as_str().to_owned();
        let stored = StoredAccountingDefinition::create(
            tenant,
            request.bucket,
            request.path_prefix,
            ids.0,
            ids.1,
        )?;
        self.require_local_builder(ids.0, ids.1, stored.accounting_id, expected_fence)?;
        let outcome = self
            .publisher
            .publish_definition(&stored, ids.0, ids.1, None, request.command_id)
            .await?;
        stored.to_api(outcome.version)
    }

    async fn disable_local(
        &self,
        caller: &Caller,
        request: DisableAccountingRequest,
        ids: (u64, u64),
        expected_fence: Option<anvil_store::PlacementLogId>,
    ) -> Result<DisableAccountingResponse, Status> {
        validate_prefix(&request.path_prefix)?;
        validate_command_id(&request.command_id)?;
        if request.expected_version == 0 {
            return Err(Status::invalid_argument(
                "accounting expected version must be non-zero",
            ));
        }
        let accounting_id = derive_accounting_id(ids.0, ids.1, &request.path_prefix);
        self.require_local_builder(ids.0, ids.1, accounting_id, expected_fence)?;
        let definition = self
            .load_definition(caller, &request.bucket, &request.path_prefix, ids)
            .await?;
        if definition.version.0 != request.expected_version {
            return Err(Status::failed_precondition(
                "accounting definition version does not match expected_version",
            ));
        }
        let outcome = self
            .publisher
            .delete_definition(
                &definition.stored,
                ids.0,
                ids.1,
                definition.version,
                request.command_id,
            )
            .await?;
        Ok(DisableAccountingResponse {
            disabled: true,
            tombstone_version: outcome.version.0,
            replayed: outcome.replayed,
        })
    }

    async fn get_local(
        &self,
        caller: &Caller,
        request: GetAccountingRequest,
        ids: (u64, u64),
        expected_fence: Option<anvil_store::PlacementLogId>,
    ) -> Result<AccountingSnapshot, Status> {
        validate_prefix(&request.path_prefix)?;
        let accounting_id = derive_accounting_id(ids.0, ids.1, &request.path_prefix);
        self.require_local_builder(ids.0, ids.1, accounting_id, expected_fence)?;
        let definition = self
            .load_definition(caller, &request.bucket, &request.path_prefix, ids)
            .await?;
        match read_rollup(&definition, &self.reader).await? {
            Some((_, rollup)) if rollup.definition_version == definition.version.0 => {
                rollup.to_api(&definition.stored, definition.version)
            }
            _ => Ok(AccountingSnapshot {
                definition: Some(definition.stored.to_api(definition.version)?),
                logical_stored_bytes: 0,
                object_count: 0,
                accepted_inbound_bytes: 0,
                served_outbound_bytes: 0,
                freshness: Some(AccountingFreshness {
                    refreshed_at: Some(std::time::SystemTime::now().into()),
                    sources: Vec::new(),
                    complete: false,
                }),
            }),
        }
    }

    async fn load_definition(
        &self,
        caller: &Caller,
        bucket: &str,
        path_prefix: &str,
        ids: (u64, u64),
    ) -> Result<LoadedAccountingDefinition, Status> {
        let accounting_id = derive_accounting_id(ids.0, ids.1, path_prefix);
        let path = definition_path(accounting_id)?;
        let key = ObjectKey::new(caller.storage_tenant().as_str(), bucket, &path)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let Some(opened) = self.reader.open_stable(&key, ids.0, ids.1, None).await? else {
            return Err(Status::not_found("accounting definition does not exist"));
        };
        if opened.version.deleted {
            return Err(Status::not_found("accounting definition does not exist"));
        }
        let mut payload = opened.payload.ok_or_else(|| {
            Status::data_loss("live accounting definition has no readable payload")
        })?;
        let mut bytes = Vec::new();
        payload
            .read_to_end(&mut bytes)
            .map_err(|error| Status::internal(format!("read accounting definition: {error}")))?;
        let stored = StoredAccountingDefinition::decode(&bytes)?;
        if stored.accounting_id != accounting_id
            || stored.storage_tenant != caller.storage_tenant().as_str()
            || stored.bucket != bucket
            || stored.path_prefix != path_prefix
        {
            return Err(Status::data_loss(
                "accounting definition identity does not match its address",
            ));
        }
        Ok(LoadedAccountingDefinition {
            tenant_id: ids.0,
            bucket_id: ids.1,
            version: opened.version.id,
            stored,
        })
    }

    async fn routed_identity<T>(
        &self,
        call: &RoutedCall<T>,
        bucket: &str,
    ) -> Result<(Caller, (u64, u64)), Status> {
        let caller = self.tokens.verify(call.bearer()).map_err(|_| {
            Status::unauthenticated("the routed accounting bearer is invalid or expired")
        })?;
        let ids = self.authorize_and_resolve(&caller, bucket).await?;
        Ok((caller, ids))
    }
}

#[tonic::async_trait]
impl AccountingService for AccountingServiceImpl {
    async fn enable_accounting(
        &self,
        request: Request<EnableAccountingRequest>,
    ) -> Result<Response<AccountingDefinition>, Status> {
        let deadline = request_deadline(request.metadata(), self.request_timeout)?;
        run_request_until(
            deadline,
            async {
                let caller = request
                    .extensions()
                    .get::<Caller>()
                    .cloned()
                    .ok_or_else(|| {
                        Status::unauthenticated("authenticated caller identity is missing")
                    })?;
                let bearer = OriginalBearer::from_metadata(request.metadata())?;
                let value = request.into_inner();
                validate_prefix(&value.path_prefix)?;
                validate_command_id(&value.command_id)?;
                let ids = self.authorize_and_resolve(&caller, &value.bucket).await?;
                let id = derive_accounting_id(ids.0, ids.1, &value.path_prefix);
                let (placement, assignment) = self.placement(ids.0, ids.1, id)?;
                if assignment.builder() == self.local_node {
                    self.enable_local(&caller, value, ids, Some(placement.fence()))
                        .await
                } else {
                    let address = placement.address(assignment.builder()).ok_or_else(|| {
                        Status::unavailable("accounting worker has no peer address")
                    })?;
                    self.peers
                        .route_enable_accounting(
                            assignment.builder(),
                            &address.0,
                            bearer.signed_token(),
                            value,
                            deadline_remaining(deadline)?,
                        )
                        .await
                }
                .map(Response::new)
            },
            "accounting request deadline exceeded",
        )
        .await
    }

    async fn disable_accounting(
        &self,
        request: Request<DisableAccountingRequest>,
    ) -> Result<Response<DisableAccountingResponse>, Status> {
        let deadline = request_deadline(request.metadata(), self.request_timeout)?;
        run_request_until(
            deadline,
            async {
                let caller = request
                    .extensions()
                    .get::<Caller>()
                    .cloned()
                    .ok_or_else(|| {
                        Status::unauthenticated("authenticated caller identity is missing")
                    })?;
                let bearer = OriginalBearer::from_metadata(request.metadata())?;
                let value = request.into_inner();
                validate_prefix(&value.path_prefix)?;
                validate_command_id(&value.command_id)?;
                let ids = self.authorize_and_resolve(&caller, &value.bucket).await?;
                let id = derive_accounting_id(ids.0, ids.1, &value.path_prefix);
                let (placement, assignment) = self.placement(ids.0, ids.1, id)?;
                if assignment.builder() == self.local_node {
                    self.disable_local(&caller, value, ids, Some(placement.fence()))
                        .await
                } else {
                    let address = placement.address(assignment.builder()).ok_or_else(|| {
                        Status::unavailable("accounting worker has no peer address")
                    })?;
                    self.peers
                        .route_disable_accounting(
                            assignment.builder(),
                            &address.0,
                            bearer.signed_token(),
                            value,
                            deadline_remaining(deadline)?,
                        )
                        .await
                }
                .map(Response::new)
            },
            "accounting request deadline exceeded",
        )
        .await
    }

    async fn get_accounting(
        &self,
        request: Request<GetAccountingRequest>,
    ) -> Result<Response<AccountingSnapshot>, Status> {
        let deadline = request_deadline(request.metadata(), self.request_timeout)?;
        run_request_until(
            deadline,
            async {
                let caller = request
                    .extensions()
                    .get::<Caller>()
                    .cloned()
                    .ok_or_else(|| {
                        Status::unauthenticated("authenticated caller identity is missing")
                    })?;
                let bearer = OriginalBearer::from_metadata(request.metadata())?;
                let value = request.into_inner();
                validate_prefix(&value.path_prefix)?;
                let ids = self.authorize_and_resolve(&caller, &value.bucket).await?;
                let id = derive_accounting_id(ids.0, ids.1, &value.path_prefix);
                let (placement, assignment) = self.placement(ids.0, ids.1, id)?;
                if assignment.builder() == self.local_node {
                    self.get_local(&caller, value, ids, Some(placement.fence()))
                        .await
                } else {
                    let address = placement.address(assignment.builder()).ok_or_else(|| {
                        Status::unavailable("accounting worker has no peer address")
                    })?;
                    self.peers
                        .route_get_accounting(
                            assignment.builder(),
                            &address.0,
                            bearer.signed_token(),
                            value,
                            deadline_remaining(deadline)?,
                        )
                        .await
                }
                .map(Response::new)
            },
            "accounting request deadline exceeded",
        )
        .await
    }
}

#[tonic::async_trait]
impl RoutedAccountingHandler for AccountingServiceImpl {
    async fn enable(
        &self,
        call: RoutedCall<EnableAccountingRequest>,
    ) -> Result<AccountingDefinition, Status> {
        let (caller, ids) = self.routed_identity(&call, &call.request().bucket).await?;
        let fence = call.placement_fence();
        self.enable_local(&caller, call.into_request(), ids, Some(fence))
            .await
    }

    async fn disable(
        &self,
        call: RoutedCall<DisableAccountingRequest>,
    ) -> Result<DisableAccountingResponse, Status> {
        let (caller, ids) = self.routed_identity(&call, &call.request().bucket).await?;
        let fence = call.placement_fence();
        self.disable_local(&caller, call.into_request(), ids, Some(fence))
            .await
    }

    async fn get(
        &self,
        call: RoutedCall<GetAccountingRequest>,
    ) -> Result<AccountingSnapshot, Status> {
        let (caller, ids) = self.routed_identity(&call, &call.request().bucket).await?;
        let fence = call.placement_fence();
        self.get_local(&caller, call.into_request(), ids, Some(fence))
            .await
    }

    async fn flush(
        &self,
        _source: NodeId,
        placement: ClusterPlacement,
        value: AccountingTrafficFlush,
    ) -> Result<bool, Status> {
        let definition = self
            .catalog
            .get(value.accounting_id)?
            .ok_or_else(|| Status::not_found("accounting definition is not active"))?;
        self.require_local_builder(
            definition.tenant_id,
            definition.bucket_id,
            value.accounting_id,
            Some(placement.fence()),
        )?;
        let existing = read_traffic_source(&definition, value.source_node, &self.reader).await?;
        let expected_version = if existing.is_some() {
            let path = super::outbound_source_path(value.accounting_id, value.source_node.0)?;
            let key = ObjectKey::new(
                &definition.stored.storage_tenant,
                &definition.stored.bucket,
                &path,
            )
            .map_err(|error| Status::data_loss(error.to_string()))?;
            self.reader
                .open_stable(&key, definition.tenant_id, definition.bucket_id, None)
                .await?
                .map(|opened| opened.version.id)
        } else {
            None
        };
        let same_generation = existing
            .as_ref()
            .filter(|source| source.definition_version == definition.version.0);
        let source = StoredTrafficSource::new(
            value.accounting_id,
            definition.version.0,
            value.source_node.0,
            same_generation
                .map_or(0, |source| source.accepted_inbound_bytes)
                .saturating_add(value.accepted_inbound_bytes),
            same_generation
                .map_or(0, |source| source.served_outbound_bytes)
                .saturating_add(value.served_outbound_bytes),
        )?;
        let outcome = self
            .publisher
            .publish_outbound_source(
                &definition.stored,
                definition.tenant_id,
                definition.bucket_id,
                &source,
                expected_version,
                value.flush_id,
            )
            .await?;
        Ok(outcome.replayed)
    }
}
