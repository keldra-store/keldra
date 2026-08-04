use std::time::Duration;

use anvil_api::v1::{
    AccountingDefinition, AccountingSnapshot, BucketPolicy as ApiBucketPolicy, BulkWriteRequest,
    BulkWriteResponse, DeleteIfVersionRequest, DeleteRequest, DeleteVersionRequest,
    DeleteVersionResponse, DisableAccountingRequest, DisableAccountingResponse,
    EnableAccountingRequest, GetAccountingRequest, MutationReceipt, PutToken,
    SetBucketPolicyRequest,
};
use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::{
    AuthzSchemaPublicationMutation, LogicalRecordApplied, LogicalRecordCandidate, LogicalRecordId,
    LogicalRecordMutation, LogicalRecordSnapshotApplied, LogicalRecordValue, ObjectHeadChange,
    PlacementLogId, ProgramPathMutation, ProgramPathStage, ReferenceProof,
    ReplicaAuthzSchemaPublicationApplied, SourceId, TupleBatchReceipt, TupleBatchRequest,
    VersionId, WatchJournalStatus,
};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Request, Status};

use super::storage::{list_page_from_wire, schema_query_json};
use super::{
    CLUSTER_PEER_SCHEMA_VERSION, MAX_CLUSTER_OPERATION_TIME, MAX_CLUSTER_PEER_MESSAGE_BYTES,
    decode_json, encode_json, require_response_schema, wire,
};
use crate::authz_distribution::AuthzSchemaReplicaQuery;
use crate::cluster_placement::ClusterPlacement;
use crate::data_peer::DataPeerTransport;
use crate::distributed_list::{ClusterListPeers, LocalListQuery, OriginalBearer, OwnedListPage};
use crate::distributed_watch::{
    ClusterWatchSources, DistributedWatchScope, WatchSourceError, WatchSourcePage,
    WatchSourceQuery, WatchSourceStatus,
};
use crate::index_runtime::publication::{
    IndexArtifactDelete, IndexArtifactOutcome, IndexArtifactPublish,
};
use crate::logical_record_distribution::LogicalRecordReplicaTransport;
use crate::reference_delivery::{ReferenceProofPeers, ReferenceProofRead};

/// Cached mandatory-mTLS transport for the typed ordinary cluster lane.
#[derive(Clone)]
pub(crate) struct ClusterPeerTransport {
    data: DataPeerTransport,
    decisions: DecisionRaft,
}

impl ClusterPeerTransport {
    pub(crate) async fn route_enable_accounting(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: EnableAccountingRequest,
        remaining: Duration,
    ) -> Result<AccountingDefinition, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteEnableAccountingRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_enable_accounting(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_disable_accounting(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: DisableAccountingRequest,
        remaining: Duration,
    ) -> Result<DisableAccountingResponse, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteDisableAccountingRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_disable_accounting(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_get_accounting(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: GetAccountingRequest,
        remaining: Duration,
    ) -> Result<AccountingSnapshot, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteGetAccountingRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_get_accounting(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn flush_accounting_traffic(
        &self,
        target: NodeId,
        address: &str,
        value: &super::AccountingTrafficFlush,
    ) -> Result<bool, Status> {
        let placement = self.placement()?;
        let response = self
            .client(target, address)?
            .flush_accounting_traffic(wire::FlushAccountingTrafficRequest {
                peer: Some(self.context(placement.fence(), 0, MAX_CLUSTER_OPERATION_TIME)?),
                accounting_id: value.accounting_id,
                source_node_id: value.source_node.0,
                accepted_inbound_bytes: value.accepted_inbound_bytes,
                served_outbound_bytes: value.served_outbound_bytes,
                flush_id: value.flush_id.clone(),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(response.replayed)
    }

    pub(crate) fn new(data: DataPeerTransport, decisions: DecisionRaft) -> Self {
        Self { data, decisions }
    }

    pub(crate) async fn publish_index_artifact(
        &self,
        target: NodeId,
        address: &str,
        request: &IndexArtifactPublish,
    ) -> Result<IndexArtifactOutcome, Status> {
        let placement = self.placement()?;
        let response = self
            .client(target, address)?
            .publish_index_artifact(wire::PublishIndexArtifactRequest {
                peer: Some(self.context(placement.fence(), 0, MAX_CLUSTER_OPERATION_TIME)?),
                storage_tenant: request.storage_tenant.clone(),
                bucket: request.bucket.clone(),
                tenant_id: request.tenant_id,
                bucket_id: request.bucket_id,
                index_id: request.index_id,
                exact_path: request.exact_path.clone(),
                blob_blake3: request.blob.hash.to_vec(),
                blob_length: request.blob.length,
                expected_version: request.expected_version.map(|version| version.0),
                command_id: request.command_id.clone(),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        nonzero_artifact_outcome(response.version, response.replayed)
    }

    pub(crate) async fn delete_index_artifact(
        &self,
        target: NodeId,
        address: &str,
        request: &IndexArtifactDelete,
    ) -> Result<IndexArtifactOutcome, Status> {
        let placement = self.placement()?;
        let response = self
            .client(target, address)?
            .delete_index_artifact(wire::DeleteIndexArtifactRequest {
                peer: Some(self.context(placement.fence(), 0, MAX_CLUSTER_OPERATION_TIME)?),
                storage_tenant: request.storage_tenant.clone(),
                bucket: request.bucket.clone(),
                tenant_id: request.tenant_id,
                bucket_id: request.bucket_id,
                index_id: request.index_id,
                exact_path: request.exact_path.clone(),
                expected_version: request.expected_version.0,
                command_id: request.command_id.clone(),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        nonzero_artifact_outcome(response.version, response.replayed)
    }

    pub(crate) async fn scan_index_heads(
        &self,
        target: NodeId,
        address: &str,
        scope: super::IndexHeadScanScope,
        cursor: Option<&anvil_store::ObjectRecordCursor>,
    ) -> Result<super::IndexHeadScanPage, Status> {
        let placement = self.placement()?;
        let fence = placement.fence();
        let scope = match scope {
            super::IndexHeadScanScope::Definitions => {
                wire::scan_index_heads_request::Scope::Definitions(wire::AllIndexDefinitionHeads {})
            }
            super::IndexHeadScanScope::AccountingDefinitions => {
                wire::scan_index_heads_request::Scope::AccountingDefinitions(
                    wire::AllAccountingDefinitionHeads {},
                )
            }
            super::IndexHeadScanScope::Generation {
                tenant_id,
                bucket_id,
                index_id,
            } => wire::scan_index_heads_request::Scope::Generation(wire::IndexGenerationHeads {
                tenant_id,
                bucket_id,
                index_id,
            }),
            super::IndexHeadScanScope::SourceObjects {
                tenant_id,
                bucket_id,
                path_prefix,
            } => {
                wire::scan_index_heads_request::Scope::SourceObjects(wire::IndexSourceObjectHeads {
                    tenant_id,
                    bucket_id,
                    path_prefix,
                })
            }
            super::IndexHeadScanScope::AccountingSourceObjects {
                tenant_id,
                bucket_id,
                path_prefix,
            } => wire::scan_index_heads_request::Scope::AccountingSourceObjects(
                wire::AccountingSourceObjectHeads {
                    tenant_id,
                    bucket_id,
                    path_prefix,
                },
            ),
        };
        let response = self
            .client(target, address)?
            .scan_index_heads(wire::ScanIndexHeadsRequest {
                peer: Some(self.context(fence, 0, MAX_CLUSTER_OPERATION_TIME)?),
                cursor: cursor.map(|cursor| cursor.as_token().to_owned()),
                scope: Some(scope),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        if response.source_node_id != target.0
            || response.placement_term != fence.term
            || response.placement_index != fence.index
        {
            return Err(Status::data_loss(
                "index head scan source or placement fence differs from the request",
            ));
        }
        let source_epoch: [u8; 32] = response
            .source_epoch
            .try_into()
            .map_err(|_| Status::data_loss("index head scan source epoch has the wrong length"))?;
        let source_node = u16::try_from(response.source_node_id)
            .map_err(|_| Status::data_loss("index head scan source node exceeds u16"))?;
        let heads = response
            .heads_json
            .iter()
            .map(|encoded| decode_json::<super::IndexCurrentHead>(encoded))
            .collect::<Result<Vec<_>, _>>()?;
        for head in &heads {
            validate_index_head(head)?;
        }
        let next_cursor = response
            .next_cursor
            .map(anvil_store::ObjectRecordCursor::from_token)
            .transpose()
            .map_err(|error| Status::data_loss(error.to_string()))?;
        Ok(super::IndexHeadScanPage {
            source: SourceId {
                node_id: source_node,
                source_epoch,
            },
            placement_fence: fence,
            heads,
            next_cursor,
        })
    }

    pub(crate) async fn apply_schema_publication(
        &self,
        target: NodeId,
        address: &str,
        stable_tenant_id: u64,
        mutation: &AuthzSchemaPublicationMutation,
    ) -> Result<ReplicaAuthzSchemaPublicationApplied, Status> {
        let fence = mutation.stamp.active_placement_log_id;
        let response = self
            .client(target, address)?
            .apply_schema_publication(wire::SchemaPublicationApplyRequest {
                peer: Some(self.context(fence, 0, MAX_CLUSTER_OPERATION_TIME)?),
                stable_tenant_id,
                mutation_json: encode_json(mutation)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(ReplicaAuthzSchemaPublicationApplied {
            revision: anvil_store::AuthzRevision(response.revision),
            replayed: response.replayed,
        })
    }

    pub(crate) async fn has_schema_publication(
        &self,
        target: NodeId,
        address: &str,
        stable_tenant_id: u64,
        query: &AuthzSchemaReplicaQuery,
    ) -> Result<bool, Status> {
        let placement = self.placement()?;
        let response = self
            .client(target, address)?
            .has_schema_publication(wire::SchemaPublicationQueryRequest {
                peer: Some(self.context(placement.fence(), 0, MAX_CLUSTER_OPERATION_TIME)?),
                stable_tenant_id,
                query_json: schema_query_json(query)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(response.exists)
    }

    pub(crate) async fn route_put_end(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        request: PutToken,
        remaining: Duration,
    ) -> Result<MutationReceipt, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RoutePutEndRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(request),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_put_end(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_internal_put_end(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: PutToken,
        remaining: Duration,
    ) -> Result<MutationReceipt, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RoutePutEndRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_internal_put_end(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_delete(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: DeleteRequest,
        remaining: Duration,
    ) -> Result<MutationReceipt, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteDeleteRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_delete(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_delete_if_version(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: DeleteIfVersionRequest,
        remaining: Duration,
    ) -> Result<MutationReceipt, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteDeleteIfVersionRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_delete_if_version(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_bulk_write(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: BulkWriteRequest,
        remaining: Duration,
    ) -> Result<BulkWriteResponse, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteBulkWriteRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_bulk_write(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_internal_delete_if_version(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: DeleteIfVersionRequest,
        remaining: Duration,
    ) -> Result<MutationReceipt, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteDeleteIfVersionRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_internal_delete_if_version(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_internal_bulk_write(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: BulkWriteRequest,
        remaining: Duration,
    ) -> Result<BulkWriteResponse, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteBulkWriteRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_internal_bulk_write(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_set_bucket_policy(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: SetBucketPolicyRequest,
        remaining: Duration,
    ) -> Result<ApiBucketPolicy, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteSetBucketPolicyRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_set_bucket_policy(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_delete_version(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: DeleteVersionRequest,
        remaining: Duration,
    ) -> Result<DeleteVersionResponse, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteDeleteVersionRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_delete_version(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_provision_tenant(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: anvil_api::v1::ProvisionTenantRequest,
        remaining: Duration,
    ) -> Result<anvil_api::v1::ProvisionTenantResponse, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteProvisionTenantRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_provision_tenant(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_create_bucket(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: anvil_api::v1::CreateBucketRequest,
        remaining: Duration,
    ) -> Result<anvil_api::v1::CreateBucketResponse, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteCreateBucketRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_create_bucket(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_credential_exchange(
        &self,
        target: NodeId,
        address: &str,
        value: anvil_api::v1::ExchangeClientCredentialsRequest,
        remaining: Duration,
    ) -> Result<anvil_api::v1::AccessToken, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteCredentialExchangeRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        request.set_timeout(remaining.min(MAX_CLUSTER_OPERATION_TIME));
        Ok(self
            .client(target, address)?
            .route_credential_exchange(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_invoke_program(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: anvil_api::v1::InvokeProgramRequest,
        remaining: Duration,
    ) -> Result<anvil_api::v1::InvokeProgramResponse, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteInvokeProgramRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_invoke_program(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn stage_program_path(
        &self,
        target: NodeId,
        address: &str,
        executor_nomination_log_index: u64,
        stage: &ProgramPathStage,
        remaining: Duration,
    ) -> Result<anvil_store::BlobRef, Status> {
        let fence = self.placement()?.fence();
        let response = self
            .client(target, address)?
            .stage_program_path(wire::ProgramStagePathRequest {
                peer: Some(self.context(fence, 0, remaining)?),
                executor_nomination_log_index,
                stage_json: encode_json(stage)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        let hash: [u8; 32] = response
            .stage_blob_hash
            .try_into()
            .map_err(|_| Status::data_loss("program stage hash has the wrong length"))?;
        Ok(anvil_store::BlobRef {
            hash,
            length: response.stage_blob_length,
        })
    }

    pub(crate) async fn coordinate_program_path_finalization(
        &self,
        target: NodeId,
        address: &str,
        executor_nomination_log_index: u64,
        commit_cursor: u64,
        stage: &ProgramPathStage,
        remaining: Duration,
    ) -> Result<ProgramPathMutation, Status> {
        let fence = self.placement()?.fence();
        let response = self
            .client(target, address)?
            .coordinate_program_path_finalization(wire::ProgramCoordinatePathFinalizationRequest {
                peer: Some(self.context(fence, 0, remaining)?),
                executor_nomination_log_index,
                commit_cursor,
                stage_json: encode_json(stage)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        decode_json(&response.mutation_json)
    }

    pub(crate) async fn apply_program_path_finalization(
        &self,
        target: NodeId,
        address: &str,
        executor_nomination_log_index: u64,
        mutation: &ProgramPathMutation,
        remaining: Duration,
    ) -> Result<anvil_store::ReplicaProgramPathApplied, Status> {
        let fence = self.placement()?.fence();
        let response = self
            .client(target, address)?
            .apply_program_path_finalization(wire::ProgramApplyPathFinalizationRequest {
                peer: Some(self.context(fence, 0, remaining)?),
                executor_nomination_log_index,
                mutation_json: encode_json(mutation)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(anvil_store::ReplicaProgramPathApplied {
            version: VersionId(response.version),
            replayed: response.replayed,
        })
    }

    pub(crate) async fn coordinate_logical_record(
        &self,
        target: NodeId,
        address: &str,
        value: &LogicalRecordValue,
        remaining: Duration,
    ) -> Result<(), Status> {
        let fence = self.placement()?.fence();
        let response = self
            .client(target, address)?
            .coordinate_logical_record(wire::CoordinateLogicalRecordRequest {
                peer: Some(self.context(fence, 0, remaining)?),
                typed_value_json: encode_json(value)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)
    }

    pub(crate) async fn read_coordinated_logical_record(
        &self,
        target: NodeId,
        address: &str,
        id: &LogicalRecordId,
        remaining: Duration,
    ) -> Result<Option<LogicalRecordValue>, Status> {
        let fence = self.placement()?.fence();
        let response = self
            .client(target, address)?
            .read_coordinated_logical_record(wire::CoordinatedLogicalReadRequest {
                peer: Some(self.context(fence, 0, remaining)?),
                id_json: encode_json(id)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        decode_optional(response.present, &response.typed_value_json)
    }

    pub(crate) async fn coordinate_system_grant(
        &self,
        target: NodeId,
        address: &str,
        value: &TupleBatchRequest,
        remaining: Duration,
    ) -> Result<TupleBatchReceipt, Status> {
        let fence = self.placement()?.fence();
        let response = self
            .client(target, address)?
            .coordinate_system_grant(wire::CoordinateSystemGrantRequest {
                peer: Some(self.context(fence, 0, remaining)?),
                tuple_batch_json: encode_json(value)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        decode_json(&response.receipt_json)
    }

    pub(super) fn placement(&self) -> Result<ClusterPlacement, Status> {
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
        ClusterPlacement::from_applied(&state)
            .map_err(|error| Status::unavailable(error.to_string()))
    }

    pub(super) fn context(
        &self,
        expected: PlacementLogId,
        hop_count: u32,
        remaining: Duration,
    ) -> Result<wire::PeerContext, Status> {
        let placement = self.placement()?;
        if placement.fence() != expected {
            return Err(Status::unavailable(
                "active placement differs from the operation fence",
            ));
        }
        let millis = remaining.min(MAX_CLUSTER_OPERATION_TIME).as_millis().max(1);
        let remaining_deadline_millis = u32::try_from(millis)
            .map_err(|_| Status::invalid_argument("cluster deadline is too large"))?;
        let (cluster_id, source_node_id) = self.data.peer_identity();
        if cluster_id != placement.cluster_id() {
            return Err(Status::unavailable(
                "peer transport identity differs from the active cluster",
            ));
        }
        Ok(wire::PeerContext {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            cluster_id: cluster_id.into_bytes().to_vec(),
            source_node_id: source_node_id.0,
            placement_term: expected.term,
            placement_index: expected.index,
            hop_count,
            remaining_deadline_millis,
        })
    }

    pub(super) fn client(
        &self,
        target: NodeId,
        address: &str,
    ) -> Result<wire::cluster_peer_client::ClusterPeerClient<Channel>, Status> {
        Ok(
            wire::cluster_peer_client::ClusterPeerClient::new(self.data.channel(target, address)?)
                .max_decoding_message_size(MAX_CLUSTER_PEER_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_CLUSTER_PEER_MESSAGE_BYTES),
        )
    }
}

#[tonic::async_trait]
impl LogicalRecordReplicaTransport for ClusterPeerTransport {
    async fn read_candidate(
        &self,
        target: NodeId,
        address: &str,
        id: &LogicalRecordId,
    ) -> Result<Option<LogicalRecordCandidate>, Status> {
        let placement = self.placement()?;
        let response = self
            .client(target, address)?
            .read_logical_record(wire::LogicalRecordReadRequest {
                peer: Some(self.context(placement.fence(), 0, MAX_CLUSTER_OPERATION_TIME)?),
                id_json: encode_json(id)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        decode_optional(response.present, &response.candidate_json)
    }

    async fn repair_candidate(
        &self,
        target: NodeId,
        address: &str,
        id: &LogicalRecordId,
        candidate: Option<&LogicalRecordCandidate>,
    ) -> Result<LogicalRecordSnapshotApplied, Status> {
        let placement = self.placement()?;
        let response = self
            .client(target, address)?
            .repair_logical_record(wire::LogicalRecordRepairRequest {
                peer: Some(self.context(placement.fence(), 0, MAX_CLUSTER_OPERATION_TIME)?),
                id_json: encode_json(id)?,
                present: candidate.is_some(),
                candidate_json: candidate.map(encode_json).transpose()?.unwrap_or_default(),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(LogicalRecordSnapshotApplied {
            record_version: response
                .present
                .then_some(VersionId(response.record_version)),
            replayed: response.replayed,
        })
    }

    async fn apply_mutation(
        &self,
        target: NodeId,
        address: &str,
        mutation: &LogicalRecordMutation,
    ) -> Result<LogicalRecordApplied, Status> {
        let response = self
            .client(target, address)?
            .apply_logical_record(wire::LogicalRecordMutationRequest {
                peer: Some(self.context(
                    mutation.active_placement_log_id,
                    0,
                    MAX_CLUSTER_OPERATION_TIME,
                )?),
                mutation_json: encode_json(mutation)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(LogicalRecordApplied {
            record_version: VersionId(response.record_version),
            replayed: response.replayed,
        })
    }
}

#[tonic::async_trait]
impl ReferenceProofPeers for ClusterPeerTransport {
    async fn read_reference_proof(
        &self,
        node: NodeId,
        address: &str,
        request: ReferenceProofRead,
    ) -> Result<Option<ReferenceProof>, String> {
        self.read_reference_proof_inner(node, address, request)
            .await
            .map_err(|error| error.to_string())
    }
}

impl ClusterPeerTransport {
    async fn read_reference_proof_inner(
        &self,
        node: NodeId,
        address: &str,
        request: ReferenceProofRead,
    ) -> Result<Option<ReferenceProof>, Status> {
        let response = self
            .client(node, address)?
            .read_reference_proof(wire::ReferenceProofReadRequest {
                peer: Some(self.context(request.placement_fence, 0, MAX_CLUSTER_OPERATION_TIME)?),
                source_id_json: encode_json(&request.source)?,
                source_offset: request.offset,
                tenant_id: request.tenant_id,
                bucket_id: request.bucket_id,
                exact_path: request.exact_path,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        decode_optional(response.present, &response.proof_json)
    }
}

#[tonic::async_trait]
impl ClusterListPeers for ClusterPeerTransport {
    async fn list_local_page(
        &self,
        target: NodeId,
        address: &str,
        bearer: OriginalBearer,
        query: LocalListQuery,
    ) -> Result<OwnedListPage, Status> {
        let mut request = Request::new(wire::LocalListRequest {
            peer: Some(self.context(query.placement_fence(), 0, MAX_CLUSTER_OPERATION_TIME)?),
            tenant: query.tenant().to_owned(),
            bucket: query.bucket().to_owned(),
            tenant_id: query.tenant_id(),
            bucket_id: query.bucket_id(),
            prefix: query.prefix().to_owned(),
            start_after: query.start_after().map(str::to_owned),
            limit: u32::try_from(query.limit())
                .map_err(|_| Status::invalid_argument("list limit exceeds u32"))?,
            include_index_definitions: query.includes_index_definitions(),
            include_personaldb_manifests: query.includes_personaldb_manifests(),
        });
        add_bearer_and_timeout(
            &mut request,
            bearer.signed_token(),
            MAX_CLUSTER_OPERATION_TIME,
        )?;
        let page = self
            .client(target, address)?
            .list_local_objects(request)
            .await?
            .into_inner();
        list_page_from_wire(page)
    }
}

#[tonic::async_trait]
impl ClusterWatchSources for ClusterPeerTransport {
    async fn status(
        &self,
        target: NodeId,
        address: &str,
        membership_revision: PlacementLogId,
        bearer: OriginalBearer,
        scope: DistributedWatchScope,
    ) -> Result<WatchSourceStatus, WatchSourceError> {
        let result = async {
            let mut request = Request::new(wire::WatchStatusRequest {
                peer: Some(self.context(membership_revision, 0, MAX_CLUSTER_OPERATION_TIME)?),
                scope_json: encode_json(&scope)?,
            });
            add_bearer_and_timeout(
                &mut request,
                bearer.signed_token(),
                MAX_CLUSTER_OPERATION_TIME,
            )?;
            let response = self
                .client(target, address)?
                .get_watch_status(request)
                .await?
                .into_inner();
            require_response_schema(response.schema_version)?;
            Ok(WatchSourceStatus {
                source_node: NodeId(response.source_node_id),
                membership_revision: PlacementLogId {
                    term: response.placement_term,
                    index: response.placement_index,
                },
                status: parse_watch_status(
                    &response.source_id_json,
                    response.tail,
                    response.retention_floor,
                    response.retained_entries,
                    response.retained_bytes,
                )?,
            })
        }
        .await;
        result.map_err(watch_error)
    }

    async fn read_page(
        &self,
        target: NodeId,
        address: &str,
        bearer: OriginalBearer,
        query: WatchSourceQuery,
    ) -> Result<WatchSourcePage, WatchSourceError> {
        let result = async {
            let mut request = Request::new(wire::WatchPageRequest {
                peer: Some(self.context(
                    query.membership_revision,
                    0,
                    MAX_CLUSTER_OPERATION_TIME,
                )?),
                expected_source_json: encode_json(&query.expected_source)?,
                scope_json: encode_json(&query.scope)?,
                next_offset: query.next_offset,
                max_records: u32::try_from(query.max_records)
                    .map_err(|_| Status::invalid_argument("watch page limit exceeds u32"))?,
            });
            add_bearer_and_timeout(
                &mut request,
                bearer.signed_token(),
                MAX_CLUSTER_OPERATION_TIME,
            )?;
            let response = self
                .client(target, address)?
                .read_watch_page(request)
                .await?
                .into_inner();
            require_response_schema(response.schema_version)?;
            let status = parse_watch_status(
                &response.source_id_json,
                response.tail,
                response.retention_floor,
                response.retained_entries,
                response.retained_bytes,
            )?;
            let object_heads = response
                .object_heads_json
                .iter()
                .map(|encoded| decode_json::<ObjectHeadChange>(encoded))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(WatchSourcePage {
                source_node: NodeId(response.source_node_id),
                membership_revision: PlacementLogId {
                    term: response.placement_term,
                    index: response.placement_index,
                },
                status,
                next_offset: response.next_offset,
                object_heads,
            })
        }
        .await;
        result.map_err(watch_error)
    }
}

fn parse_watch_status(
    encoded_source: &[u8],
    tail: u64,
    retention_floor: u64,
    retained_entries: u64,
    retained_bytes: u64,
) -> Result<WatchJournalStatus, Status> {
    Ok(WatchJournalStatus {
        source_id: decode_json::<SourceId>(encoded_source)?,
        tail,
        retention_floor,
        retained_entries,
        retained_bytes,
    })
}

fn decode_optional<T: serde::de::DeserializeOwned>(
    present: bool,
    encoded: &[u8],
) -> Result<Option<T>, Status> {
    match (present, encoded.is_empty()) {
        (false, true) => Ok(None),
        (true, false) => decode_json(encoded).map(Some),
        _ => Err(Status::data_loss(
            "typed peer response presence flag and value disagree",
        )),
    }
}

fn nonzero_artifact_outcome(version: u64, replayed: bool) -> Result<IndexArtifactOutcome, Status> {
    if version == 0 {
        return Err(Status::data_loss(
            "index artifact mutation returned a zero version",
        ));
    }
    Ok(IndexArtifactOutcome {
        version: VersionId(version),
        replayed,
    })
}

fn validate_index_head(head: &super::IndexCurrentHead) -> Result<(), Status> {
    if head.tenant_id == 0
        || head.bucket_id == 0
        || head.exact_path.is_empty()
        || head.head.version.0 == 0
        || head.head.version != head.version.id
        || head.head.deleted != head.version.deleted
    {
        return Err(Status::data_loss(
            "index head scan returned an invalid current-head snapshot",
        ));
    }
    Ok(())
}

pub(super) fn add_bearer_and_timeout<T>(
    request: &mut Request<T>,
    bearer: &str,
    timeout: Duration,
) -> Result<(), Status> {
    let value = MetadataValue::try_from(format!("Bearer {bearer}"))
        .map_err(|_| Status::invalid_argument("bearer token cannot be represented as metadata"))?;
    request.metadata_mut().insert("authorization", value);
    request.set_timeout(timeout.min(MAX_CLUSTER_OPERATION_TIME));
    Ok(())
}

fn watch_error(status: Status) -> WatchSourceError {
    if status.code() == tonic::Code::OutOfRange && status.message() == "RESUME_EXPIRED" {
        WatchSourceError::ResumeExpired
    } else {
        WatchSourceError::Unavailable(status.to_string())
    }
}
