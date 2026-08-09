use std::time::{Duration, Instant};

use anvil_store::{
    AuthzRevision, AuthzSchemaPublicationMutation, AuthzStoreError, ListObjectsPage,
    LogicalRecordCandidate, LogicalRecordError, LogicalRecordExport, LogicalRecordId,
    LogicalRecordMutation, ObjectKey, PlacementLogId, ReferenceProof, SchemaRef, SourceId,
    StorageTenantId, VersionId, WatchJournalStatus,
};
use serde::{Deserialize, Serialize};
use tonic::{Request, Response, Status};

use super::{CLUSTER_PEER_SCHEMA_VERSION, ClusterPeerService, decode_json, encode_json, wire};
use crate::distributed_list::{LocalListQuery, OriginalBearer, OwnedListPage};
use crate::distributed_watch::{DistributedWatchScope, filter_public_changes};
use crate::mutable_record_replica_group::MutableRecordReplicaGroup;
use crate::placement::PlacementKind;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SchemaReplicaQueryWire {
    storage_tenant: StorageTenantId,
    schema_ref: SchemaRef,
    schema: anvil_authz::Schema,
    published_at_revision: AuthzRevision,
    publication_mutation: Option<AuthzSchemaPublicationMutation>,
}

#[tonic::async_trait]
impl wire::cluster_peer_server::ClusterPeer for ClusterPeerService {
    type ReadRealmAggregateStream = super::authz::RealmAggregateStream;
    type ScanIndexSourceSnapshotStream = super::index_snapshot::IndexSourceSnapshotRpcStream;

    async fn publish_index_artifact(
        &self,
        request: Request<wire::PublishIndexArtifactRequest>,
    ) -> Result<Response<wire::IndexArtifactPublished>, Status> {
        self.publish_index_artifact_call(request).await
    }

    async fn delete_index_artifact(
        &self,
        request: Request<wire::DeleteIndexArtifactRequest>,
    ) -> Result<Response<wire::IndexArtifactDeleted>, Status> {
        self.delete_index_artifact_call(request).await
    }

    async fn scan_index_heads(
        &self,
        request: Request<wire::ScanIndexHeadsRequest>,
    ) -> Result<Response<wire::IndexHeadScanPage>, Status> {
        self.scan_index_heads_call(request).await
    }

    async fn scan_index_source_snapshot(
        &self,
        request: Request<tonic::Streaming<wire::IndexSourceSnapshotRequest>>,
    ) -> Result<Response<Self::ScanIndexSourceSnapshotStream>, Status> {
        self.scan_index_source_snapshot_call(request).await
    }

    async fn route_index_query(
        &self,
        request: Request<wire::RouteIndexQueryRequest>,
    ) -> Result<Response<wire::RoutedIndexQueryResponse>, Status> {
        self.route_index_query_call(request).await
    }

    async fn route_create_personal_db_group(
        &self,
        request: Request<wire::RouteCreatePersonalDbGroupRequest>,
    ) -> Result<Response<anvil_api::v1::PersonalDbGroup>, Status> {
        self.route_create_personaldb_group_call(request).await
    }

    async fn route_change_personal_db_group_role(
        &self,
        request: Request<wire::RouteChangePersonalDbGroupRoleRequest>,
    ) -> Result<Response<anvil_api::v1::PersonalDbGroupRoleChange>, Status> {
        self.route_change_personaldb_group_role_call(request).await
    }

    async fn route_append_personal_db_entry(
        &self,
        request: Request<wire::RouteAppendPersonalDbEntryRequest>,
    ) -> Result<Response<anvil_api::v1::PersonalDbCommit>, Status> {
        self.route_append_personaldb_entry_call(request).await
    }

    async fn route_materialize_personal_db_projection(
        &self,
        request: Request<wire::RouteMaterializePersonalDbProjectionRequest>,
    ) -> Result<Response<anvil_api::v1::PersonalDbMaterialization>, Status> {
        self.route_materialize_personaldb_projection_call(request)
            .await
    }

    async fn route_register_personal_db_snapshot(
        &self,
        request: Request<wire::RouteRegisterPersonalDbSnapshotRequest>,
    ) -> Result<Response<anvil_api::v1::PersonalDbSnapshot>, Status> {
        self.route_register_personaldb_snapshot_call(request).await
    }

    async fn apply_personal_db_role(
        &self,
        request: Request<wire::ApplyPersonalDbRoleRequest>,
    ) -> Result<Response<anvil_api::v1::PersonalDbGroupRoleChange>, Status> {
        self.apply_personaldb_role_call(request).await
    }

    async fn route_enable_accounting(
        &self,
        request: Request<wire::RouteEnableAccountingRequest>,
    ) -> Result<Response<anvil_api::v1::AccountingDefinition>, Status> {
        self.route_enable_accounting_call(request).await
    }

    async fn route_disable_accounting(
        &self,
        request: Request<wire::RouteDisableAccountingRequest>,
    ) -> Result<Response<anvil_api::v1::DisableAccountingResponse>, Status> {
        self.route_disable_accounting_call(request).await
    }

    async fn route_get_accounting(
        &self,
        request: Request<wire::RouteGetAccountingRequest>,
    ) -> Result<Response<anvil_api::v1::AccountingSnapshot>, Status> {
        self.route_get_accounting_call(request).await
    }

    async fn flush_accounting_traffic(
        &self,
        request: Request<wire::FlushAccountingTrafficRequest>,
    ) -> Result<Response<wire::FlushAccountingTrafficResponse>, Status> {
        self.flush_accounting_traffic_call(request).await
    }

    async fn resolve_tenant_name(
        &self,
        request: Request<wire::ResolveTenantNameRequest>,
    ) -> Result<Response<wire::ResolveTenantNameResult>, Status> {
        self.resolve_tenant_name_call(request).await
    }

    async fn resolve_bucket_name(
        &self,
        request: Request<wire::ResolveBucketNameRequest>,
    ) -> Result<Response<wire::ResolveBucketNameResult>, Status> {
        self.resolve_bucket_name_call(request).await
    }

    async fn read_logical_record(
        &self,
        request: Request<wire::LogicalRecordReadRequest>,
    ) -> Result<Response<wire::LogicalRecordCandidate>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let id: LogicalRecordId = decode_json(&request.get_ref().id_json)?;
        require_logical_replica(
            &admitted.placement,
            admitted.authenticated.node_id,
            self.local_node,
            &id,
        )?;
        let store = self.store.clone();
        let candidate = bounded_blocking(admitted.timeout, move || {
            store.logical_record_candidate(&id).map_err(logical_status)
        })
        .await?;
        Ok(Response::new(wire_candidate(candidate)?))
    }

    async fn repair_logical_record(
        &self,
        request: Request<wire::LogicalRecordRepairRequest>,
    ) -> Result<Response<wire::LogicalRecordSnapshotApplied>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let id: LogicalRecordId = decode_json(&request.get_ref().id_json)?;
        require_logical_replica(
            &admitted.placement,
            admitted.authenticated.node_id,
            self.local_node,
            &id,
        )?;
        let selected: Option<LogicalRecordCandidate> =
            decode_optional(request.get_ref().present, &request.get_ref().candidate_json)?;
        if let Some(candidate) = selected.as_ref() {
            LogicalRecordExport {
                id: id.clone(),
                candidate: candidate.clone(),
            }
            .validate()
            .map_err(logical_status)?;
        }
        let store = self.store.clone();
        let applied = bounded_blocking(admitted.timeout, move || {
            store
                .repair_quorum_reconciled_logical_record(&id, selected.as_ref())
                .map_err(logical_status)
        })
        .await?;
        Ok(Response::new(wire::LogicalRecordSnapshotApplied {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            present: applied.record_version.is_some(),
            record_version: applied.record_version.unwrap_or(VersionId(0)).0,
            replayed: applied.replayed,
        }))
    }

    async fn apply_logical_record(
        &self,
        request: Request<wire::LogicalRecordMutationRequest>,
    ) -> Result<Response<wire::LogicalRecordApplied>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let mutation: LogicalRecordMutation = decode_json(&request.get_ref().mutation_json)?;
        let id = mutation.typed_value.id();
        require_logical_replica(
            &admitted.placement,
            admitted.authenticated.node_id,
            self.local_node,
            &id,
        )?;
        if mutation.active_placement_log_id != admitted.placement.fence() {
            return Err(Status::unavailable(
                "logical mutation does not carry the admitted placement fence",
            ));
        }
        let applied = tokio::time::timeout(
            admitted.timeout,
            self.store
                .apply_logical_record_mutation_journaled(&mutation),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("logical mutation deadline exceeded"))?
        .map_err(logical_status)?;
        Ok(Response::new(wire::LogicalRecordApplied {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            record_version: applied.record_version.0,
            replayed: applied.replayed,
        }))
    }

    async fn apply_schema_publication(
        &self,
        request: Request<wire::SchemaPublicationApplyRequest>,
    ) -> Result<Response<wire::SchemaPublicationApplied>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let stable_tenant_id = request.get_ref().stable_tenant_id;
        let mutation: AuthzSchemaPublicationMutation =
            decode_json(&request.get_ref().mutation_json)?;
        require_schema_replica(
            &admitted.placement,
            admitted.authenticated.node_id,
            self.local_node,
            stable_tenant_id,
            &mutation.storage_tenant,
        )?;
        if mutation.stamp.active_placement_log_id != admitted.placement.fence()
            || u64::from(mutation.stamp.source_id.node_id) != admitted.authenticated.node_id.0
        {
            return Err(Status::unavailable(
                "schema mutation does not carry its coordinator placement fence",
            ));
        }
        let repository = self.store.authz();
        let applied = bounded_blocking(admitted.timeout, move || {
            repository
                .apply_schema_publication_replica(&mutation)
                .map_err(authz_status)
        })
        .await?;
        Ok(Response::new(wire::SchemaPublicationApplied {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            revision: applied.revision.0,
            replayed: applied.replayed,
        }))
    }

    async fn has_schema_publication(
        &self,
        request: Request<wire::SchemaPublicationQueryRequest>,
    ) -> Result<Response<wire::Exists>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let stable_tenant_id = request.get_ref().stable_tenant_id;
        let query: SchemaReplicaQueryWire = decode_json(&request.get_ref().query_json)?;
        require_schema_replica(
            &admitted.placement,
            admitted.authenticated.node_id,
            self.local_node,
            stable_tenant_id,
            &query.storage_tenant,
        )?;
        let repository = self.store.authz();
        let exists = bounded_blocking(admitted.timeout, move || {
            let matches = repository
                .get_schema(&query.storage_tenant, &query.schema_ref)
                .map_err(authz_status)?
                .as_ref()
                == Some(&query.schema)
                && repository
                    .tenant_revision(&query.storage_tenant)
                    .map_err(authz_status)?
                    >= query.published_at_revision;
            if !matches {
                return Ok(false);
            }
            let Some(expected) = query.publication_mutation.as_ref() else {
                return Ok(true);
            };
            Ok(repository
                .export_authz_schema_catalogue(&query.storage_tenant)
                .map_err(authz_status)?
                .into_iter()
                .flat_map(|catalogue| catalogue.schemas)
                .find(|revision| revision.schema_ref == query.schema_ref)
                .and_then(|revision| revision.publication_mutation)
                .as_ref()
                == Some(expected))
        })
        .await?;
        Ok(Response::new(wire::Exists {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            exists,
        }))
    }

    async fn fresh_authorization_checks(
        &self,
        request: Request<wire::FreshAuthorizationChecksRequest>,
    ) -> Result<Response<wire::FreshAuthorizationChecksResult>, Status> {
        self.fresh_authorization_checks_call(request).await
    }

    async fn apply_realm_mutation(
        &self,
        request: Request<wire::RealmMutationApplyRequest>,
    ) -> Result<Response<wire::RealmMutationApplied>, Status> {
        self.apply_realm_mutation_call(request).await
    }

    async fn read_realm_candidate(
        &self,
        request: Request<wire::RealmCandidateReadRequest>,
    ) -> Result<Response<wire::RealmCandidate>, Status> {
        self.read_realm_candidate_call(request).await
    }

    async fn read_realm_aggregate(
        &self,
        request: Request<wire::RealmAggregateReadRequest>,
    ) -> Result<Response<Self::ReadRealmAggregateStream>, Status> {
        self.read_realm_aggregate_call(request).await
    }

    async fn install_realm_candidate(
        &self,
        request: Request<tonic::Streaming<wire::RealmCandidateInstallFrame>>,
    ) -> Result<Response<wire::RealmCandidateInstalled>, Status> {
        self.install_realm_candidate_call(request).await
    }

    async fn fresh_authorization_check(
        &self,
        request: Request<wire::FreshAuthorizationCheckRequest>,
    ) -> Result<Response<wire::FreshAuthorizationCheckResult>, Status> {
        self.fresh_authorization_check_call(request).await
    }

    async fn read_reference_proof(
        &self,
        request: Request<wire::ReferenceProofReadRequest>,
    ) -> Result<Response<wire::ReferenceProofReadResponse>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let source: SourceId = decode_json(&request.get_ref().source_id_json)?;
        let offset = request.get_ref().source_offset;
        if source.node_id == 0 || offset == 0 {
            return Err(Status::invalid_argument(
                "reference proof source and offset must be non-zero",
            ));
        }
        require_object_replica(
            &admitted.placement,
            self.local_node,
            request.get_ref().tenant_id,
            request.get_ref().bucket_id,
            &request.get_ref().exact_path,
        )?;
        let store = self.store.clone();
        let proof = bounded_blocking(admitted.timeout, move || {
            store
                .read_reference_proof(source, offset)
                .map_err(|error| Status::internal(error.to_string()))
        })
        .await?;
        if proof.as_ref().is_some_and(|proof| {
            !proof_matches_path(
                proof,
                source,
                offset,
                request.get_ref().tenant_id,
                request.get_ref().bucket_id,
                &request.get_ref().exact_path,
            )
        }) {
            return Err(Status::data_loss(
                "stored reference proof does not match its requested path coordinates",
            ));
        }
        Ok(Response::new(wire::ReferenceProofReadResponse {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            present: proof.is_some(),
            proof_json: proof
                .as_ref()
                .map(encode_json)
                .transpose()?
                .unwrap_or_default(),
        }))
    }

    async fn list_local_objects(
        &self,
        request: Request<wire::LocalListRequest>,
    ) -> Result<Response<wire::LocalListPage>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let raw = request.get_ref();
        let limit = usize::try_from(raw.limit)
            .map_err(|_| Status::invalid_argument("list limit does not fit this node"))?;
        let mut query = LocalListQuery::new(
            admitted.placement.fence(),
            raw.tenant.clone(),
            raw.bucket.clone(),
            raw.tenant_id,
            raw.bucket_id,
            raw.prefix.clone(),
            raw.start_after.clone(),
            limit,
        )?;
        if raw.include_index_definitions {
            query = query.for_index_definitions()?;
        }
        if raw.include_personaldb_manifests {
            if raw.include_index_definitions {
                return Err(Status::invalid_argument(
                    "a local list request may select only one reserved scope",
                ));
            }
            query = query.for_personaldb_manifests()?;
        }
        self.list_authorizer.authorize(&bearer, &query).await?;
        let deadline = Instant::now()
            .checked_add(admitted.timeout)
            .ok_or_else(|| Status::invalid_argument("list deadline overflowed"))?;
        loop {
            let placement = admitted.placement.clone();
            let local_node = self.local_node;
            let store = self.store.clone();
            let query = query.clone();
            let owned = bounded_blocking(visibility_remaining(deadline)?, move || {
                let page = if query.includes_index_definitions() {
                    store.list_local_owned_index_definitions(
                        query.tenant_id(),
                        query.bucket_id(),
                        query.prefix(),
                        query.start_after(),
                        query.limit(),
                        |tenant_id, bucket_id, path| {
                            object_coordinator(&placement, tenant_id, bucket_id, path)
                                == Some(local_node)
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
                            object_coordinator(&placement, tenant_id, bucket_id, path)
                                == Some(local_node)
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
                            object_coordinator(&placement, tenant_id, bucket_id, path)
                                == Some(local_node)
                        },
                    )
                }
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
                Ok(OwnedListPage::new(local_node, placement.fence(), page))
            })
            .await?;
            if crate::programs::atomic_tail_is_clear(&self.decisions)? {
                return Ok(Response::new(wire::LocalListPage {
                    schema_version: CLUSTER_PEER_SCHEMA_VERSION,
                    source_node_id: owned.source_node().0,
                    placement_term: owned.placement_fence().term,
                    placement_index: owned.placement_fence().index,
                    paths: owned.page().paths.clone(),
                    has_more: owned.page().has_more,
                }));
            }
            crate::programs::wait_for_atomic_tail(&self.decisions, visibility_remaining(deadline)?)
                .await?;
        }
    }

    async fn get_watch_status(
        &self,
        request: Request<wire::WatchStatusRequest>,
    ) -> Result<Response<wire::WatchStatus>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let scope: DistributedWatchScope = decode_json(&request.get_ref().scope_json)?;
        self.list_authorizer
            .authorize(
                &bearer,
                &watch_authorization_query(admitted.placement.fence(), &scope)?,
            )
            .await?;
        let deadline = Instant::now()
            .checked_add(admitted.timeout)
            .ok_or_else(|| Status::invalid_argument("watch deadline overflowed"))?;
        loop {
            let store = self.store.clone();
            let status = bounded_blocking(visibility_remaining(deadline)?, move || {
                store
                    .local_watch_status()
                    .map_err(|error| Status::internal(error.to_string()))
            })
            .await?;
            require_local_source(self.local_node, &status)?;
            if crate::programs::atomic_tail_is_clear(&self.decisions)? {
                return Ok(Response::new(wire_watch_status(
                    self.local_node,
                    admitted.placement.fence(),
                    &status,
                )?));
            }
            crate::programs::wait_for_atomic_tail(&self.decisions, visibility_remaining(deadline)?)
                .await?;
        }
    }

    async fn read_watch_page(
        &self,
        request: Request<wire::WatchPageRequest>,
    ) -> Result<Response<wire::WatchPage>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let expected_source: SourceId = decode_json(&request.get_ref().expected_source_json)?;
        let scope: DistributedWatchScope = decode_json(&request.get_ref().scope_json)?;
        self.list_authorizer
            .authorize(
                &bearer,
                &watch_authorization_query(admitted.placement.fence(), &scope)?,
            )
            .await?;
        let next_offset = request.get_ref().next_offset;
        let max_records = usize::try_from(request.get_ref().max_records)
            .map_err(|_| Status::invalid_argument("watch page limit does not fit this node"))?
            .min(anvil_store::MAX_LOCAL_INVALIDATION_SCAN_RECORDS);
        if max_records == 0 {
            return Err(Status::invalid_argument(
                "watch page limit must be positive",
            ));
        }
        let deadline = Instant::now()
            .checked_add(admitted.timeout)
            .ok_or_else(|| Status::invalid_argument("watch deadline overflowed"))?;
        loop {
            let store = self.store.clone();
            let (status, changes, returned_next) =
                bounded_blocking(visibility_remaining(deadline)?, move || {
                    let status = store
                        .local_watch_status()
                        .map_err(|error| Status::internal(error.to_string()))?;
                    if status.source_id != expected_source {
                        return Err(Status::out_of_range("RESUME_EXPIRED"));
                    }
                    let floor_next = status
                        .retention_floor
                        .checked_add(1)
                        .ok_or_else(|| Status::data_loss("watch retention floor cannot advance"))?;
                    let tail_next = status
                        .tail
                        .checked_add(1)
                        .ok_or_else(|| Status::data_loss("watch tail cannot advance"))?;
                    if next_offset < floor_next || next_offset > tail_next {
                        return Err(Status::out_of_range("RESUME_EXPIRED"));
                    }
                    let changes = store
                        .scan_local_changes(next_offset - 1, max_records)
                        .map_err(|error| Status::internal(error.to_string()))?;
                    let returned_next = changes
                        .last()
                        .map_or(next_offset, |change| change.offset().saturating_add(1));
                    Ok((status, changes, returned_next))
                })
                .await?;
            require_local_source(self.local_node, &status)?;
            if crate::programs::atomic_tail_is_clear(&self.decisions)? {
                let heads = filter_public_changes(&scope, changes);
                return Ok(Response::new(wire::WatchPage {
                    schema_version: CLUSTER_PEER_SCHEMA_VERSION,
                    source_node_id: self.local_node.0,
                    placement_term: admitted.placement.fence().term,
                    placement_index: admitted.placement.fence().index,
                    source_id_json: encode_json(&status.source_id)?,
                    tail: status.tail,
                    retention_floor: status.retention_floor,
                    retained_entries: status.retained_entries,
                    retained_bytes: status.retained_bytes,
                    next_offset: returned_next,
                    object_heads_json: heads.iter().map(encode_json).collect::<Result<_, _>>()?,
                }));
            }
            crate::programs::wait_for_atomic_tail(&self.decisions, visibility_remaining(deadline)?)
                .await?;
        }
    }

    async fn route_put_end(
        &self,
        request: Request<wire::RoutePutEndRequest>,
    ) -> Result<Response<anvil_api::v1::MutationReceipt>, Status> {
        self.route_put_end_call(request).await
    }

    async fn route_delete(
        &self,
        request: Request<wire::RouteDeleteRequest>,
    ) -> Result<Response<anvil_api::v1::MutationReceipt>, Status> {
        self.route_delete_call(request).await
    }

    async fn route_delete_if_version(
        &self,
        request: Request<wire::RouteDeleteIfVersionRequest>,
    ) -> Result<Response<anvil_api::v1::MutationReceipt>, Status> {
        self.route_delete_if_version_call(request).await
    }

    async fn route_bulk_write(
        &self,
        request: Request<wire::RouteBulkWriteRequest>,
    ) -> Result<Response<anvil_api::v1::BulkWriteResponse>, Status> {
        self.route_bulk_write_call(request).await
    }

    async fn route_internal_delete_if_version(
        &self,
        request: Request<wire::RouteDeleteIfVersionRequest>,
    ) -> Result<Response<anvil_api::v1::MutationReceipt>, Status> {
        self.route_internal_delete_if_version_call(request).await
    }

    async fn route_internal_put_end(
        &self,
        request: Request<wire::RoutePutEndRequest>,
    ) -> Result<Response<anvil_api::v1::MutationReceipt>, Status> {
        self.route_internal_put_end_call(request).await
    }

    async fn route_internal_bulk_write(
        &self,
        request: Request<wire::RouteBulkWriteRequest>,
    ) -> Result<Response<anvil_api::v1::BulkWriteResponse>, Status> {
        self.route_internal_bulk_write_call(request).await
    }

    async fn route_authz_put_schema(
        &self,
        request: Request<wire::RouteAuthzPutSchemaRequest>,
    ) -> Result<Response<anvil_api::v1::PutSchemaResponse>, Status> {
        self.route_authz_put_schema_call(request).await
    }

    async fn route_authz_bind_schema(
        &self,
        request: Request<wire::RouteAuthzBindSchemaRequest>,
    ) -> Result<Response<anvil_api::v1::BindSchemaResponse>, Status> {
        self.route_authz_bind_schema_call(request).await
    }

    async fn route_authz_get_binding(
        &self,
        request: Request<wire::RouteAuthzGetBindingRequest>,
    ) -> Result<Response<anvil_api::v1::GetBindingResponse>, Status> {
        self.route_authz_get_binding_call(request).await
    }

    async fn route_authz_get_schema(
        &self,
        request: Request<wire::RouteAuthzGetSchemaRequest>,
    ) -> Result<Response<anvil_api::v1::GetSchemaResponse>, Status> {
        self.route_authz_get_schema_call(request).await
    }

    async fn route_authz_mutate_tuples(
        &self,
        request: Request<wire::RouteAuthzMutateTuplesRequest>,
    ) -> Result<Response<anvil_api::v1::MutateTuplesResponse>, Status> {
        self.route_authz_mutate_tuples_call(request).await
    }

    async fn route_authz_read_tuples(
        &self,
        request: Request<wire::RouteAuthzReadTuplesRequest>,
    ) -> Result<Response<anvil_api::v1::ReadTuplesResponse>, Status> {
        self.route_authz_read_tuples_call(request).await
    }

    async fn route_authz_check_permission(
        &self,
        request: Request<wire::RouteAuthzCheckPermissionRequest>,
    ) -> Result<Response<anvil_api::v1::CheckPermissionResponse>, Status> {
        self.route_authz_check_permission_call(request).await
    }

    async fn route_authz_check_permissions(
        &self,
        request: Request<wire::RouteAuthzCheckPermissionsRequest>,
    ) -> Result<Response<anvil_api::v1::CheckPermissionsResponse>, Status> {
        self.route_authz_check_permissions_call(request).await
    }

    async fn route_set_bucket_policy(
        &self,
        request: Request<wire::RouteSetBucketPolicyRequest>,
    ) -> Result<Response<anvil_api::v1::BucketPolicy>, Status> {
        self.route_set_bucket_policy_call(request).await
    }

    async fn route_delete_version(
        &self,
        request: Request<wire::RouteDeleteVersionRequest>,
    ) -> Result<Response<anvil_api::v1::DeleteVersionResponse>, Status> {
        self.route_delete_version_call(request).await
    }

    async fn route_provision_tenant(
        &self,
        request: Request<wire::RouteProvisionTenantRequest>,
    ) -> Result<Response<anvil_api::v1::ProvisionTenantResponse>, Status> {
        self.route_provision_tenant_call(request).await
    }

    async fn route_create_bucket(
        &self,
        request: Request<wire::RouteCreateBucketRequest>,
    ) -> Result<Response<anvil_api::v1::CreateBucketResponse>, Status> {
        self.route_create_bucket_call(request).await
    }

    async fn route_credential_exchange(
        &self,
        request: Request<wire::RouteCredentialExchangeRequest>,
    ) -> Result<Response<anvil_api::v1::AccessToken>, Status> {
        self.route_credential_exchange_call(request).await
    }

    async fn route_admin_create_application(
        &self,
        request: Request<wire::RouteAdminCreateApplicationRequest>,
    ) -> Result<Response<anvil_api::v1::ApplicationCredential>, Status> {
        self.route_admin_create_application_call(request).await
    }

    async fn route_admin_rotate_credential(
        &self,
        request: Request<wire::RouteAdminRotateCredentialRequest>,
    ) -> Result<Response<anvil_api::v1::ApplicationCredential>, Status> {
        self.route_admin_rotate_credential_call(request).await
    }

    async fn route_admin_disable_credential(
        &self,
        request: Request<wire::RouteAdminDisableCredentialRequest>,
    ) -> Result<Response<anvil_api::v1::ApplicationCredentialState>, Status> {
        self.route_admin_disable_credential_call(request).await
    }

    async fn route_admin_set_bucket_versioning(
        &self,
        request: Request<wire::RouteAdminSetBucketVersioningRequest>,
    ) -> Result<Response<anvil_api::v1::SetBucketVersioningResponse>, Status> {
        self.route_admin_set_bucket_versioning_call(request).await
    }

    async fn route_admin_set_bucket_public_read(
        &self,
        request: Request<wire::RouteAdminSetBucketPublicReadRequest>,
    ) -> Result<Response<anvil_api::v1::SetBucketPublicReadResponse>, Status> {
        self.route_admin_set_bucket_public_read_call(request).await
    }

    async fn route_admin_change_application_role(
        &self,
        request: Request<wire::RouteAdminChangeApplicationRoleRequest>,
    ) -> Result<Response<anvil_api::v1::ApplicationRoleResponse>, Status> {
        self.route_admin_change_application_role_call(request).await
    }

    async fn route_invoke_program(
        &self,
        request: Request<wire::RouteInvokeProgramRequest>,
    ) -> Result<Response<anvil_api::v1::InvokeProgramResponse>, Status> {
        self.route_invoke_program_call(request).await
    }

    async fn stage_program_path(
        &self,
        request: Request<wire::ProgramStagePathRequest>,
    ) -> Result<Response<wire::ProgramStagePathResponse>, Status> {
        self.stage_program_path_call(request).await
    }

    async fn coordinate_program_path_finalization(
        &self,
        request: Request<wire::ProgramCoordinatePathFinalizationRequest>,
    ) -> Result<Response<wire::ProgramCoordinatedPathFinalization>, Status> {
        self.coordinate_program_path_finalization_call(request)
            .await
    }

    async fn apply_program_path_finalization(
        &self,
        request: Request<wire::ProgramApplyPathFinalizationRequest>,
    ) -> Result<Response<wire::ProgramPathFinalizationApplied>, Status> {
        self.apply_program_path_finalization_call(request).await
    }

    async fn coordinate_logical_record(
        &self,
        request: Request<wire::CoordinateLogicalRecordRequest>,
    ) -> Result<Response<wire::CoordinateLogicalRecordResponse>, Status> {
        self.coordinate_logical_record_call(request).await
    }

    async fn read_coordinated_logical_record(
        &self,
        request: Request<wire::CoordinatedLogicalReadRequest>,
    ) -> Result<Response<wire::CoordinatedLogicalReadResponse>, Status> {
        self.read_coordinated_logical_record_call(request).await
    }

    async fn coordinate_system_grant(
        &self,
        request: Request<wire::CoordinateSystemGrantRequest>,
    ) -> Result<Response<wire::CoordinateSystemGrantResponse>, Status> {
        self.coordinate_system_grant_call(request).await
    }
}

fn watch_authorization_query(
    fence: PlacementLogId,
    scope: &DistributedWatchScope,
) -> Result<LocalListQuery, Status> {
    LocalListQuery::new(
        fence,
        scope.tenant(),
        scope.bucket(),
        scope.tenant_id(),
        scope.bucket_id(),
        scope.prefix(),
        None,
        1,
    )
}

fn require_logical_replica(
    placement: &crate::cluster_placement::ClusterPlacement,
    source: anvil_consensus::NodeId,
    local: anvil_consensus::NodeId,
    id: &LogicalRecordId,
) -> Result<(), Status> {
    let (kind, key) = logical_placement_key(id)?;
    let group = MutableRecordReplicaGroup::select(
        kind,
        placement.cluster_id(),
        &key,
        placement.placement_nodes(),
    )
    .ok_or_else(|| Status::unavailable("cluster has no logical-record replica"))?;
    if group.coordinator() != source || !group.replicas().contains(&local) {
        return Err(Status::failed_precondition(
            "logical record is not routed from its coordinator to this replica",
        ));
    }
    Ok(())
}

fn require_schema_replica(
    placement: &crate::cluster_placement::ClusterPlacement,
    source: anvil_consensus::NodeId,
    local: anvil_consensus::NodeId,
    stable_tenant_id: u64,
    storage_tenant: &StorageTenantId,
) -> Result<(), Status> {
    if stable_tenant_id == 0 {
        return Err(Status::failed_precondition(
            "schema publication stable tenant identity must be non-zero",
        ));
    }
    let group = MutableRecordReplicaGroup::select(
        PlacementKind::ZanzibarRealm,
        placement.cluster_id(),
        &stable_tenant_id.to_be_bytes(),
        placement.placement_nodes(),
    )
    .ok_or_else(|| Status::unavailable("cluster has no Zanzibar replica"))?;
    if group.coordinator() != source || !group.replicas().contains(&local) {
        return Err(Status::failed_precondition(
            "schema publication is not routed from its coordinator to this replica",
        ));
    }
    if storage_tenant.as_str().is_empty() {
        return Err(Status::invalid_argument(
            "schema publication storage tenant must not be empty",
        ));
    }
    Ok(())
}

fn require_object_replica(
    placement: &crate::cluster_placement::ClusterPlacement,
    local: anvil_consensus::NodeId,
    tenant_id: u64,
    bucket_id: u64,
    path: &str,
) -> Result<(), Status> {
    if tenant_id == 0 || bucket_id == 0 {
        return Err(Status::invalid_argument(
            "reference proof stable IDs must be non-zero",
        ));
    }
    ObjectKey::new("peer", "proof", path)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let key = object_placement_key(tenant_id, bucket_id, path);
    let group = MutableRecordReplicaGroup::select(
        PlacementKind::Object,
        placement.cluster_id(),
        &key,
        placement.placement_nodes(),
    )
    .ok_or_else(|| Status::unavailable("cluster has no object metadata replica"))?;
    if !group.replicas().contains(&local) {
        return Err(Status::failed_precondition(
            "reference proof request is not addressed to a current metadata replica",
        ));
    }
    Ok(())
}

pub(super) fn object_coordinator(
    placement: &crate::cluster_placement::ClusterPlacement,
    tenant_id: u64,
    bucket_id: u64,
    path: &str,
) -> Option<anvil_consensus::NodeId> {
    placement
        .rank(
            PlacementKind::Object,
            &object_placement_key(tenant_id, bucket_id, path),
        )
        .into_iter()
        .next()
}

fn object_placement_key(tenant_id: u64, bucket_id: u64, path: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(16 + path.len());
    key.extend_from_slice(&tenant_id.to_be_bytes());
    key.extend_from_slice(&bucket_id.to_be_bytes());
    key.extend_from_slice(path.as_bytes());
    key
}

fn logical_placement_key(id: &LogicalRecordId) -> Result<(PlacementKind, Vec<u8>), Status> {
    match id {
        LogicalRecordId::TenantNameClaim { storage_tenant } => Ok((
            PlacementKind::TenantNameClaim,
            storage_tenant.as_str().as_bytes().to_vec(),
        )),
        LogicalRecordId::TenantRecord { tenant_id } => Ok((
            PlacementKind::TenantOrBucketRecord,
            tenant_id.to_be_bytes().to_vec(),
        )),
        LogicalRecordId::BucketRecord {
            tenant_id,
            bucket_id,
        }
        | LogicalRecordId::BucketOptions {
            tenant_id,
            bucket_id,
        }
        | LogicalRecordId::BucketPolicy {
            tenant_id,
            bucket_id,
        } => {
            let mut key = Vec::with_capacity(16);
            key.extend_from_slice(&tenant_id.to_be_bytes());
            key.extend_from_slice(&bucket_id.to_be_bytes());
            Ok((PlacementKind::TenantOrBucketRecord, key))
        }
        LogicalRecordId::Application { app_id } => {
            Ok((PlacementKind::Credential, app_id.as_bytes().to_vec()))
        }
        LogicalRecordId::Credential { client_id } => {
            Ok((PlacementKind::Credential, client_id.as_bytes().to_vec()))
        }
        LogicalRecordId::BucketNameClaim { tenant_id, bucket } => {
            let mut key = Vec::with_capacity(8 + bucket.len());
            key.extend_from_slice(&tenant_id.to_be_bytes());
            key.extend_from_slice(bucket.as_bytes());
            Ok((PlacementKind::TenantOrBucketRecord, key))
        }
        LogicalRecordId::TenantSchema { .. } => Err(Status::failed_precondition(
            "tenant schemas use the tenant-wide Zanzibar protocol",
        )),
    }
}

fn proof_matches_path(
    proof: &ReferenceProof,
    source: SourceId,
    offset: u64,
    tenant_id: u64,
    bucket_id: u64,
    exact_path: &str,
) -> bool {
    if proof.source_id != source || proof.offset() != offset {
        return false;
    }
    match &proof.change {
        anvil_store::LocalChange::ObjectHead(change) => {
            change.tenant_id == tenant_id
                && change.bucket_id == bucket_id
                && change.exact_path == exact_path
        }
        anvil_store::LocalChange::RetainedVersionDeleted(change) => {
            change.tenant_id == tenant_id
                && change.bucket_id == bucket_id
                && change.exact_path == exact_path
        }
        _ => false,
    }
}

fn require_local_source(
    local_node: anvil_consensus::NodeId,
    status: &WatchJournalStatus,
) -> Result<(), Status> {
    let expected = u16::try_from(local_node.0)
        .map_err(|_| Status::data_loss("local node ID cannot identify a source journal"))?;
    if status.source_id.node_id != expected
        || status.retention_floor > status.tail
        || status.retained_entries != status.tail - status.retention_floor
    {
        return Err(Status::data_loss(
            "local source-journal identity or retention metadata is inconsistent",
        ));
    }
    Ok(())
}

fn wire_watch_status(
    local_node: anvil_consensus::NodeId,
    fence: PlacementLogId,
    status: &WatchJournalStatus,
) -> Result<wire::WatchStatus, Status> {
    Ok(wire::WatchStatus {
        schema_version: CLUSTER_PEER_SCHEMA_VERSION,
        source_node_id: local_node.0,
        placement_term: fence.term,
        placement_index: fence.index,
        source_id_json: encode_json(&status.source_id)?,
        tail: status.tail,
        retention_floor: status.retention_floor,
        retained_entries: status.retained_entries,
        retained_bytes: status.retained_bytes,
    })
}

fn wire_candidate(
    candidate: Option<LogicalRecordCandidate>,
) -> Result<wire::LogicalRecordCandidate, Status> {
    Ok(wire::LogicalRecordCandidate {
        schema_version: CLUSTER_PEER_SCHEMA_VERSION,
        present: candidate.is_some(),
        candidate_json: candidate
            .as_ref()
            .map(encode_json)
            .transpose()?
            .unwrap_or_default(),
    })
}

fn decode_optional<T: serde::de::DeserializeOwned>(
    present: bool,
    encoded: &[u8],
) -> Result<Option<T>, Status> {
    match (present, encoded.is_empty()) {
        (false, true) => Ok(None),
        (true, false) => decode_json(encoded).map(Some),
        _ => Err(Status::invalid_argument(
            "typed optional presence flag and value disagree",
        )),
    }
}

fn visibility_remaining(deadline: Instant) -> Result<Duration, Status> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(Status::deadline_exceeded(
            "atomic-program visibility deadline exceeded",
        ))
    } else {
        Ok(remaining)
    }
}

pub(super) async fn bounded_blocking<T: Send + 'static>(
    timeout: std::time::Duration,
    operation: impl FnOnce() -> Result<T, Status> + Send + 'static,
) -> Result<T, Status> {
    tokio::time::timeout(timeout, tokio::task::spawn_blocking(operation))
        .await
        .map_err(|_| Status::deadline_exceeded("cluster operation deadline exceeded"))?
        .map_err(|error| Status::internal(format!("cluster storage worker failed: {error}")))?
}

fn logical_status(error: LogicalRecordError) -> Status {
    match error {
        LogicalRecordError::Storage(_) => Status::internal(error.to_string()),
        LogicalRecordError::Stale
        | LogicalRecordError::Sibling
        | LogicalRecordError::LineageGap => Status::aborted(error.to_string()),
        _ => Status::failed_precondition(error.to_string()),
    }
}

pub(super) fn authz_status(error: AuthzStoreError) -> Status {
    match error {
        AuthzStoreError::Storage(_) => Status::internal(error.to_string()),
        AuthzStoreError::RevisionNotAvailable { .. } | AuthzStoreError::ReceiptCapacity => {
            Status::unavailable(error.to_string())
        }
        _ => Status::failed_precondition(error.to_string()),
    }
}

pub(super) fn schema_query_json(
    query: &crate::authz_distribution::AuthzSchemaReplicaQuery,
) -> Result<Vec<u8>, Status> {
    encode_json(&SchemaReplicaQueryWire {
        storage_tenant: query.storage_tenant.clone(),
        schema_ref: query.schema_ref.clone(),
        schema: query.schema.clone(),
        published_at_revision: query.published_at_revision,
        publication_mutation: query.publication_mutation.clone(),
    })
}

pub(super) fn list_page_from_wire(page: wire::LocalListPage) -> Result<OwnedListPage, Status> {
    if page.schema_version != CLUSTER_PEER_SCHEMA_VERSION {
        return Err(Status::failed_precondition(
            "unsupported cluster-peer list response schema",
        ));
    }
    Ok(OwnedListPage::new(
        anvil_consensus::NodeId(page.source_node_id),
        PlacementLogId {
            term: page.placement_term,
            index: page.placement_index,
        },
        ListObjectsPage {
            paths: page.paths,
            has_more: page.has_more,
        },
    ))
}
