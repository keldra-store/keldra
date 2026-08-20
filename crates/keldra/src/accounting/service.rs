//! Zanzibar-authorized public accounting lifecycle and query RPCs.

use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use keldra_api::v1::accounting_service_server::AccountingService;
use keldra_api::v1::{
    AccountingDefinition, AccountingFreshness, AccountingSnapshot, DisableAccountingRequest,
    DisableAccountingResponse, EnableAccountingRequest, GetAccountingRequest,
};
use keldra_consensus::{DecisionRaft, NodeId};
use keldra_store::{DefinitionKind, ObjectKey, Store};
use tonic::{Request, Response, Status};

use crate::authentication::{Caller, JwtManager};
use crate::authoritative_system::AuthoritativeSystemAuthorization;
use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::{
    AccountingTrafficBatch, AccountingTrafficFlush, ClusterPeerTransport, RoutedAccountingHandler,
    RoutedCall,
};
use crate::cluster_placement::ClusterPlacement;
use crate::distributed_list::OriginalBearer;
use crate::index_runtime::placement::{IndexIdentity, IndexPlacement};
use crate::index_service::validate_command_id;
use crate::logical_name_resolution::LogicalNameResolver;
use crate::v05::{deadline_remaining, request_deadline, run_request_until};

use super::manager::{read_rollup, read_traffic_source_versioned};
use super::{
    AccountingMatcher, AccountingPublisher, LoadedAccountingDefinition, StoredAccountingDefinition,
    StoredTrafficSource, definition_path, derive_accounting_id, matcher_node, validate_prefix,
};

#[derive(Clone)]
pub(crate) struct AccountingServiceImpl {
    local_node: NodeId,
    decisions: DecisionRaft,
    tokens: JwtManager,
    names: LogicalNameResolver,
    authorization: AuthoritativeSystemAuthorization,
    peers: ClusterPeerTransport,
    store: Store,
    reader: ClusterObjectReader,
    publisher: AccountingPublisher,
    matcher: AccountingMatcher,
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
        store: Store,
        reader: ClusterObjectReader,
        publisher: AccountingPublisher,
        matcher: AccountingMatcher,
        request_timeout: Duration,
    ) -> Self {
        Self {
            local_node,
            decisions,
            tokens,
            names,
            authorization,
            peers,
            store,
            reader,
            publisher,
            matcher,
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
        expected_fence: Option<keldra_store::PlacementLogId>,
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

    fn require_local_matcher(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        expected_fence: keldra_store::PlacementLogId,
    ) -> Result<(), Status> {
        let placement = self.current_placement()?;
        if placement.fence() != expected_fence {
            return Err(Status::unavailable(
                "accounting placement changed during matcher routing",
            ));
        }
        require_matcher_target(
            self.local_node,
            matcher_node(&placement, tenant_id, bucket_id)?,
        )
    }

    fn require_matcher_sender(
        &self,
        source: NodeId,
        tenant_id: u64,
        bucket_id: u64,
        expected_fence: keldra_store::PlacementLogId,
    ) -> Result<(), Status> {
        let placement = self.current_placement()?;
        if placement.fence() != expected_fence {
            return Err(Status::unavailable(
                "accounting placement changed during traffic publication",
            ));
        }
        if matcher_node(&placement, tenant_id, bucket_id)? != source {
            return Err(Status::permission_denied(
                "accounting traffic flush caller is not the current bucket matcher",
            ));
        }
        Ok(())
    }

    fn current_placement(&self) -> Result<ClusterPlacement, Status> {
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
        ClusterPlacement::from_applied(&state)
            .map_err(|error| Status::unavailable(error.to_string()))
    }

    async fn enable_local(
        &self,
        caller: &Caller,
        request: EnableAccountingRequest,
        ids: (u64, u64),
        expected_fence: Option<keldra_store::PlacementLogId>,
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
        expected_fence: Option<keldra_store::PlacementLogId>,
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
        expected_fence: Option<keldra_store::PlacementLogId>,
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

impl AccountingServiceImpl {
    async fn load_assigned_definition(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        accounting_id: u64,
    ) -> Result<LoadedAccountingDefinition, Status> {
        let store = self.store.clone();
        let assignment = tokio::task::spawn_blocking(move || {
            store.definition_assignment(
                DefinitionKind::Accounting,
                tenant_id,
                bucket_id,
                accounting_id,
            )
        })
        .await
        .map_err(|error| Status::internal(format!("join accounting assignment read: {error}")))?
        .map_err(|error| Status::internal(format!("read accounting assignment: {error}")))?
        .ok_or_else(|| Status::not_found("accounting definition is not assigned here"))?;
        super::runtime::load_assignment(self.local_node, &self.decisions, &self.reader, &assignment)
            .await?
            .ok_or_else(|| Status::not_found("accounting definition is not active here"))
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
        source: NodeId,
        placement: ClusterPlacement,
        value: AccountingTrafficFlush,
    ) -> Result<bool, Status> {
        self.require_matcher_sender(source, value.tenant_id, value.bucket_id, placement.fence())?;
        if !placement.active_node_ids().contains(&value.source_node) {
            return Err(Status::invalid_argument(
                "accounting traffic origin is not an ACTIVE cluster node",
            ));
        }
        let definition = self
            .load_assigned_definition(value.tenant_id, value.bucket_id, value.accounting_id)
            .await?;
        self.require_local_builder(
            definition.tenant_id,
            definition.bucket_id,
            value.accounting_id,
            Some(placement.fence()),
        )?;
        let existing =
            read_traffic_source_versioned(&definition, value.source_node, &self.reader).await?;
        let expected_version = existing.as_ref().map(|(version, _)| *version);
        let source = match prepare_traffic_source(
            existing.as_ref().map(|(_, source)| source),
            definition.version.0,
            &value,
        )? {
            TrafficSourceFlush::Replay => return Ok(true),
            TrafficSourceFlush::Publish(source) => source,
        };
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

    async fn match_traffic(
        &self,
        source: NodeId,
        placement: ClusterPlacement,
        value: AccountingTrafficBatch,
    ) -> Result<(), Status> {
        if source != value.source_node {
            return Err(Status::permission_denied(
                "accounting traffic origin differs from the authenticated ingress node",
            ));
        }
        self.require_local_matcher(value.tenant_id, value.bucket_id, placement.fence())?;
        let matched = self.matcher.match_batch(&value).await?;
        for matched in matched.iter() {
            let definition = matched.definition.as_ref();
            let identity = IndexIdentity::new(
                definition.tenant_id,
                definition.bucket_id,
                definition.stored.accounting_id,
            )
            .map_err(|error| Status::data_loss(error.to_string()))?;
            let assignment = IndexPlacement::derive(identity, &placement)
                .map_err(|error| Status::unavailable(error.to_string()))?;
            let flush = AccountingTrafficFlush {
                tenant_id: definition.tenant_id,
                bucket_id: definition.bucket_id,
                accounting_id: definition.stored.accounting_id,
                source_node: value.source_node,
                accepted_inbound_bytes: matched.accepted_inbound_bytes,
                served_outbound_bytes: matched.served_outbound_bytes,
                flush_id: format!(
                    "accounting-traffic-{}-{}-{}-{}",
                    value.source_node.0,
                    hex::encode(&value.source_epoch[..8]),
                    value.sequence,
                    definition.stored.accounting_id
                ),
            };
            if assignment.builder() == self.local_node {
                self.flush(self.local_node, placement.clone(), flush)
                    .await?;
            } else {
                let address = placement
                    .address(assignment.builder())
                    .ok_or_else(|| Status::unavailable("accounting worker has no peer address"))?;
                self.peers
                    .flush_accounting_traffic(assignment.builder(), &address.0, &flush)
                    .await?;
            }
        }
        self.require_local_matcher(value.tenant_id, value.bucket_id, placement.fence())
    }

    async fn invalidate_matcher_bucket(
        &self,
        source: NodeId,
        placement: ClusterPlacement,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<(), Status> {
        if !placement.active_node_ids().contains(&source) {
            return Err(Status::permission_denied(
                "accounting matcher invalidation caller is not ACTIVE",
            ));
        }
        self.require_local_matcher(tenant_id, bucket_id, placement.fence())?;
        self.matcher.invalidate_bucket(tenant_id, bucket_id).await?;
        self.require_local_matcher(tenant_id, bucket_id, placement.fence())
    }

    async fn clear_matcher_cache(
        &self,
        source: NodeId,
        placement: ClusterPlacement,
    ) -> Result<(), Status> {
        let current = self.current_placement()?;
        if current.fence() != placement.fence()
            || !placement.active_node_ids().contains(&source)
            || !placement.active_node_ids().contains(&self.local_node)
        {
            return Err(Status::permission_denied(
                "accounting matcher cache clear requires current ACTIVE peers",
            ));
        }
        self.matcher.clear().await;
        if self.current_placement()?.fence() != placement.fence() {
            return Err(Status::unavailable(
                "accounting placement changed during matcher cache clear",
            ));
        }
        Ok(())
    }
}

enum TrafficSourceFlush {
    Replay,
    Publish(StoredTrafficSource),
}

fn require_matcher_target(local_node: NodeId, matcher: NodeId) -> Result<(), Status> {
    if local_node != matcher {
        return Err(Status::failed_precondition(
            "accounting traffic did not reach the current bucket matcher",
        ));
    }
    Ok(())
}

fn prepare_traffic_source(
    existing: Option<&StoredTrafficSource>,
    definition_version: u64,
    value: &AccountingTrafficFlush,
) -> Result<TrafficSourceFlush, Status> {
    let same_generation = existing.filter(|source| {
        source.accounting_id == value.accounting_id
            && source.definition_version == definition_version
            && source.node_id == value.source_node.0
    });
    if same_generation.is_some_and(|source| source.last_flush_id == value.flush_id) {
        return Ok(TrafficSourceFlush::Replay);
    }
    Ok(TrafficSourceFlush::Publish(StoredTrafficSource::new(
        value.accounting_id,
        definition_version,
        value.source_node.0,
        same_generation
            .map_or(0, |source| source.accepted_inbound_bytes)
            .saturating_add(value.accepted_inbound_bytes),
        same_generation
            .map_or(0, |source| source.served_outbound_bytes)
            .saturating_add(value.served_outbound_bytes),
        value.flush_id.clone(),
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flush(accounting_id: u64, flush_id: &str) -> AccountingTrafficFlush {
        AccountingTrafficFlush {
            tenant_id: 11,
            bucket_id: 12,
            accounting_id,
            source_node: NodeId(7),
            accepted_inbound_bytes: 90,
            served_outbound_bytes: 40,
            flush_id: flush_id.into(),
        }
    }

    fn published(value: TrafficSourceFlush) -> StoredTrafficSource {
        match value {
            TrafficSourceFlush::Replay => panic!("expected a source publication"),
            TrafficSourceFlush::Publish(source) => source,
        }
    }

    #[test]
    fn lost_publish_response_replays_without_adding_the_batch_twice() {
        let value = flush(31, "accounting-traffic-7-epoch-1-31");
        let first = published(prepare_traffic_source(None, 4, &value).unwrap());
        assert_eq!(first.accepted_inbound_bytes, 90);
        assert_eq!(first.served_outbound_bytes, 40);

        assert!(matches!(
            prepare_traffic_source(Some(&first), 4, &value).unwrap(),
            TrafficSourceFlush::Replay
        ));
        assert_eq!(first.accepted_inbound_bytes, 90);
        assert_eq!(first.served_outbound_bytes, 40);
    }

    #[test]
    fn partial_multi_definition_retry_replays_only_the_completed_definition() {
        let first_value = flush(31, "accounting-traffic-7-epoch-1-31");
        let second_value = flush(32, "accounting-traffic-7-epoch-1-32");
        let first_source = published(prepare_traffic_source(None, 4, &first_value).unwrap());

        assert!(matches!(
            prepare_traffic_source(Some(&first_source), 4, &first_value).unwrap(),
            TrafficSourceFlush::Replay
        ));
        let second_source = published(prepare_traffic_source(None, 8, &second_value).unwrap());
        assert_eq!(second_source.accounting_id, 32);
        assert_eq!(second_source.accepted_inbound_bytes, 90);
        assert_eq!(second_source.served_outbound_bytes, 40);
    }

    #[test]
    fn matcher_placement_rejects_every_node_except_the_current_hrw_target() {
        assert!(require_matcher_target(NodeId(7), NodeId(7)).is_ok());
        assert_eq!(
            require_matcher_target(NodeId(8), NodeId(7))
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }
}
