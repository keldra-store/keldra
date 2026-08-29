use std::time::Duration;

use keldra_api::v1::{
    AccountingDefinition, AccountingSnapshot, BucketPolicy as ApiBucketPolicy, BulkWriteRequest,
    BulkWriteResponse, CloneObjectRequest, DeleteIfVersionRequest, DeleteRequest,
    DeleteVersionRequest, DeleteVersionResponse, DisableAccountingRequest,
    DisableAccountingResponse, EnableAccountingRequest, GetAccountingRequest, LinkObjectRequest,
    MutationReceipt, PutToken, SetBucketPolicyRequest, UnlinkObjectRequest,
};
use keldra_consensus::{CapabilityRange, DecisionRaft, NodeId};
use keldra_store::{
    AuthzSchemaPublicationMutation, DefinitionKind, DefinitionMutationIntent, LogicalRecordApplied,
    LogicalRecordCandidate, LogicalRecordId, LogicalRecordMutation, LogicalRecordSnapshotApplied,
    LogicalRecordValue, ObjectHeadChange, PlacementLogId, ProgramAliasRegistryMutation,
    ProgramAliasRegistryStage, ProgramPathMutation, ProgramPathStage, ProgramReservation,
    ReferenceProof, ReplicaAuthzSchemaPublicationApplied, SourceId, TupleBatchReceipt,
    TupleBatchRequest, VersionId, WatchJournalStatus,
};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Request, Status};

use super::storage::{list_page_from_wire, schema_query_json};
use super::{
    CLUSTER_PEER_SCHEMA_VERSION, MAX_CLUSTER_OPERATION_TIME, MAX_CLUSTER_PEER_MESSAGE_BYTES,
    MAX_INDEX_SOURCE_SNAPSHOT_TIME, decode_json, encode_json, require_response_schema, wire,
};
use crate::authentication::Caller;
use crate::authz_distribution::AuthzSchemaReplicaQuery;
use crate::cluster_placement::ClusterPlacement;
use crate::data_peer::DataPeerTransport;
use crate::distributed_list::{ClusterListPeers, LocalListQuery, OriginalBearer, OwnedListPage};
use crate::distributed_watch::{
    ClusterWatchSources, DistributedWatchScope, WatchSourceError, WatchSourcePage,
    WatchSourceQuery, WatchSourceStatus,
};
use crate::index_runtime::publication::{
    IndexArtifactDelete, IndexArtifactOutcome, IndexArtifactPublicationOutcome,
    IndexArtifactPublish,
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
    pub(crate) async fn update_local_capabilities(
        &self,
        leader: NodeId,
        address: &str,
        expected_protocol: CapabilityRange,
        expected_storage: CapabilityRange,
        replacement_protocol: CapabilityRange,
        replacement_storage: CapabilityRange,
    ) -> Result<(CapabilityRange, CapabilityRange), Status> {
        let placement = self.placement()?;
        let response = self
            .client(leader, address)?
            .update_local_capabilities(wire::UpdateLocalCapabilitiesRequest {
                peer: Some(self.context(placement.fence(), 0, MAX_CLUSTER_OPERATION_TIME)?),
                expected_protocol_min: u32::from(expected_protocol.min),
                expected_protocol_max: u32::from(expected_protocol.max),
                expected_storage_min: u32::from(expected_storage.min),
                expected_storage_max: u32::from(expected_storage.max),
                replacement_protocol_min: u32::from(replacement_protocol.min),
                replacement_protocol_max: u32::from(replacement_protocol.max),
                replacement_storage_min: u32::from(replacement_storage.min),
                replacement_storage_max: u32::from(replacement_storage.max),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        if response.node_id != self.data.peer_identity().1.0 {
            return Err(Status::data_loss(
                "capability attestation returned another node",
            ));
        }
        Ok((
            wire_capability_range(response.protocol_min, response.protocol_max)?,
            wire_capability_range(response.storage_min, response.storage_max)?,
        ))
    }

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
                tenant_id: value.tenant_id,
                bucket_id: value.bucket_id,
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

    pub(crate) async fn match_accounting_traffic(
        &self,
        target: NodeId,
        address: &str,
        value: &super::AccountingTrafficBatch,
    ) -> Result<(), Status> {
        if value.source_node != self.data.peer_identity().1 {
            return Err(Status::invalid_argument(
                "accounting traffic batch source is not the local node",
            ));
        }
        let placement = self.placement()?;
        let response = self
            .client(target, address)?
            .match_accounting_traffic(wire::MatchAccountingTrafficRequest {
                peer: Some(self.context(placement.fence(), 0, MAX_CLUSTER_OPERATION_TIME)?),
                source_node_id: value.source_node.0,
                source_epoch: value.source_epoch.to_vec(),
                sequence: value.sequence,
                tenant_id: value.tenant_id,
                bucket_id: value.bucket_id,
                entries: value
                    .entries
                    .iter()
                    .map(|entry| wire::AccountingTrafficEntry {
                        exact_path: entry.exact_path.clone(),
                        accepted_inbound_bytes: entry.accepted_inbound_bytes,
                        served_outbound_bytes: entry.served_outbound_bytes,
                    })
                    .collect(),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)
    }

    pub(crate) async fn invalidate_accounting_matcher_bucket(
        &self,
        target: NodeId,
        address: &str,
        tenant_id: u64,
        bucket_id: u64,
        expected_fence: PlacementLogId,
    ) -> Result<(), Status> {
        let placement = self.placement()?;
        if placement.fence() != expected_fence {
            return Err(Status::unavailable(
                "accounting matcher invalidation placement changed",
            ));
        }
        let response = self
            .client(target, address)?
            .invalidate_accounting_matcher_bucket(wire::InvalidateAccountingMatcherBucketRequest {
                peer: Some(self.context(expected_fence, 0, MAX_CLUSTER_OPERATION_TIME)?),
                tenant_id,
                bucket_id,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)
    }

    pub(crate) async fn clear_accounting_matcher_cache(
        &self,
        target: NodeId,
        address: &str,
        expected_fence: PlacementLogId,
    ) -> Result<(), Status> {
        let placement = self.placement()?;
        if placement.fence() != expected_fence {
            return Err(Status::unavailable(
                "accounting matcher cache-clear placement changed",
            ));
        }
        let response = self
            .client(target, address)?
            .clear_accounting_matcher_cache(wire::ClearAccountingMatcherCacheRequest {
                peer: Some(self.context(expected_fence, 0, MAX_CLUSTER_OPERATION_TIME)?),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)
    }

    pub(crate) fn new(data: DataPeerTransport, decisions: DecisionRaft) -> Self {
        Self { data, decisions }
    }

    pub(crate) async fn publish_index_artifact(
        &self,
        target: NodeId,
        address: &str,
        expected_fence: PlacementLogId,
        request: &IndexArtifactPublish,
    ) -> Result<IndexArtifactOutcome, Status> {
        let response = self
            .client(target, address)?
            .publish_index_artifact(wire_index_artifact_publish(
                request,
                Some(self.context(expected_fence, 0, MAX_CLUSTER_OPERATION_TIME)?),
            ))
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        nonzero_artifact_outcome(response.version, response.replayed)
    }

    pub(crate) async fn publish_index_artifacts(
        &self,
        target: NodeId,
        address: &str,
        expected_fence: PlacementLogId,
        requests: &[IndexArtifactPublish],
    ) -> Result<Vec<IndexArtifactPublicationOutcome>, Status> {
        let response = self
            .client(target, address)?
            .publish_index_artifacts(wire::PublishIndexArtifactsRequest {
                peer: Some(self.context(expected_fence, 0, MAX_CLUSTER_OPERATION_TIME)?),
                publications: requests
                    .iter()
                    .map(|request| wire_index_artifact_publish(request, None))
                    .collect(),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        decode_indexed_artifact_outcomes(response.outcomes, requests.len())
    }

    pub(crate) async fn publish_guarded_index_artifacts(
        &self,
        target: NodeId,
        address: &str,
        expected_fence: PlacementLogId,
        requests: &[IndexArtifactPublish],
    ) -> Result<Vec<IndexArtifactPublicationOutcome>, Status> {
        let response = self
            .client(target, address)?
            .publish_guarded_index_artifacts(wire::PublishGuardedIndexArtifactsRequest {
                peer: Some(self.context(expected_fence, 0, MAX_CLUSTER_OPERATION_TIME)?),
                publications: requests
                    .iter()
                    .map(|request| wire_index_artifact_publish(request, None))
                    .collect(),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        decode_indexed_artifact_outcomes(response.outcomes, requests.len())
    }

    pub(crate) async fn commit_guarded_index_artifact(
        &self,
        target: NodeId,
        address: &str,
        expected_fence: PlacementLogId,
        builder: NodeId,
        request: &IndexArtifactPublish,
    ) -> Result<IndexArtifactOutcome, Status> {
        let response = self
            .client(target, address)?
            .commit_guarded_index_artifact(wire::CommitGuardedIndexArtifactRequest {
                peer: Some(self.context(expected_fence, 0, MAX_CLUSTER_OPERATION_TIME)?),
                builder_node_id: builder.0,
                publication: Some(wire_index_artifact_publish(request, None)),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        nonzero_artifact_outcome(response.version, response.replayed)
    }

    pub(crate) async fn commit_guarded_index_artifacts(
        &self,
        target: NodeId,
        address: &str,
        expected_fence: PlacementLogId,
        builder: NodeId,
        requests: &[IndexArtifactPublish],
    ) -> Result<Vec<IndexArtifactPublicationOutcome>, Status> {
        let response = self
            .client(target, address)?
            .commit_guarded_index_artifacts(wire::CommitGuardedIndexArtifactsRequest {
                peer: Some(self.context(expected_fence, 0, MAX_CLUSTER_OPERATION_TIME)?),
                builder_node_id: builder.0,
                publications: requests
                    .iter()
                    .map(|request| wire_index_artifact_publish(request, None))
                    .collect(),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        decode_indexed_artifact_outcomes(response.outcomes, requests.len())
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
                definition_kind: routed_definition_kind(request.definition_intent),
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
        cursor: Option<&keldra_store::ObjectRecordCursor>,
    ) -> Result<super::IndexHeadScanPage, Status> {
        let placement = self.placement()?;
        let fence = placement.fence();
        let artifacts = wire::IndexArtifactHeads {
            tenant_id: scope.tenant_id,
            bucket_id: scope.bucket_id,
            index_id: scope.index_id,
        };
        let response = self
            .client(target, address)?
            .scan_index_heads(wire::ScanIndexHeadsRequest {
                peer: Some(self.context(fence, 0, MAX_CLUSTER_OPERATION_TIME)?),
                cursor: cursor.map(|cursor| cursor.as_token().to_owned()),
                artifacts: Some(artifacts),
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
            .map(keldra_store::ObjectRecordCursor::from_token)
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

    pub(crate) async fn scan_index_source_snapshot(
        &self,
        target: NodeId,
        address: &str,
        tenant_id: u64,
        bucket_id: u64,
        path_prefix: String,
        resume_after_path: Option<String>,
        max_frame_bytes: u64,
    ) -> Result<super::IndexSourceSnapshot, Status> {
        super::index_snapshot::require_snapshot_frame_bound(max_frame_bytes)?;
        let placement = self.placement()?;
        let fence = placement.fence();
        let (requests, receiver) = tokio::sync::mpsc::channel(1);
        requests
            .send(wire::IndexSourceSnapshotRequest {
                command: Some(wire::index_source_snapshot_request::Command::Begin(
                    wire::IndexSourceSnapshotBegin {
                        peer: Some(self.context_with_timeout_limit(
                            fence,
                            0,
                            MAX_INDEX_SOURCE_SNAPSHOT_TIME,
                            MAX_INDEX_SOURCE_SNAPSHOT_TIME,
                        )?),
                        tenant_id,
                        bucket_id,
                        path_prefix,
                        max_frame_bytes,
                        resume_after_path,
                    },
                )),
            })
            .await
            .map_err(|_| Status::unavailable("index snapshot request stream closed"))?;
        let request = Request::new(tokio_stream::wrappers::ReceiverStream::new(receiver));
        let mut stream = self
            .client(target, address)?
            .scan_index_source_snapshot(request)
            .await?
            .into_inner();
        let response = tokio::time::timeout(MAX_INDEX_SOURCE_SNAPSHOT_TIME, stream.message())
            .await
            .map_err(|_| Status::deadline_exceeded("index source snapshot deadline exceeded"))??
            .ok_or_else(|| {
                Status::data_loss("index source snapshot returned no acknowledgement")
            })?;
        let begun = match response.event {
            Some(wire::index_source_snapshot_response::Event::Begun(begun)) => begun,
            Some(wire::index_source_snapshot_response::Event::Frame(_)) | None => {
                return Err(Status::data_loss(
                    "index source snapshot did not begin with an acknowledgement",
                ));
            }
        };
        super::index_snapshot::open_client_snapshot(
            target,
            self.decisions.clone(),
            fence,
            requests,
            stream,
            begun,
            MAX_INDEX_SOURCE_SNAPSHOT_TIME,
        )
    }

    pub(crate) async fn scan_retained_source_snapshot(
        &self,
        target: NodeId,
        address: &str,
        tenant_id: u64,
        bucket_id: u64,
        path_prefix: String,
        max_frame_bytes: u64,
    ) -> Result<super::RetainedSourceSnapshot, Status> {
        super::index_snapshot::require_snapshot_frame_bound(max_frame_bytes)?;
        let placement = self.placement()?;
        let fence = placement.fence();
        let (requests, receiver) = tokio::sync::mpsc::channel(1);
        requests
            .send(wire::RetainedSourceSnapshotRequest {
                command: Some(wire::retained_source_snapshot_request::Command::Begin(
                    wire::RetainedSourceSnapshotBegin {
                        peer: Some(self.context_with_timeout_limit(
                            fence,
                            0,
                            MAX_INDEX_SOURCE_SNAPSHOT_TIME,
                            MAX_INDEX_SOURCE_SNAPSHOT_TIME,
                        )?),
                        tenant_id,
                        bucket_id,
                        path_prefix,
                        max_frame_bytes,
                    },
                )),
            })
            .await
            .map_err(|_| Status::unavailable("retained snapshot request stream closed"))?;
        let request = Request::new(tokio_stream::wrappers::ReceiverStream::new(receiver));
        let mut stream = self
            .client(target, address)?
            .scan_retained_source_snapshot(request)
            .await?
            .into_inner();
        let response = tokio::time::timeout(MAX_INDEX_SOURCE_SNAPSHOT_TIME, stream.message())
            .await
            .map_err(|_| Status::deadline_exceeded("retained source snapshot deadline exceeded"))??
            .ok_or_else(|| {
                Status::data_loss("retained source snapshot returned no acknowledgement")
            })?;
        let begun = match response.event {
            Some(wire::retained_source_snapshot_response::Event::Begun(begun)) => begun,
            Some(wire::retained_source_snapshot_response::Event::Frame(_)) | None => {
                return Err(Status::data_loss(
                    "retained source snapshot did not begin with an acknowledgement",
                ));
            }
        };
        super::index_snapshot::open_client_retained_snapshot(
            target,
            self.decisions.clone(),
            fence,
            requests,
            stream,
            begun,
            MAX_INDEX_SOURCE_SNAPSHOT_TIME,
        )
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
            revision: keldra_store::AuthzRevision(response.revision),
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
        atomic_executor_replay_checked: bool,
        remaining: Duration,
    ) -> Result<MutationReceipt, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteDeleteRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
            atomic_executor_replay_checked,
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
        atomic_executor_replay_checked: bool,
        remaining: Duration,
    ) -> Result<MutationReceipt, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteDeleteIfVersionRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
            atomic_executor_replay_checked,
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_delete_if_version(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_clone_object(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: CloneObjectRequest,
        remaining: Duration,
    ) -> Result<MutationReceipt, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteCloneObjectRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_clone_object(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_link_object(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: LinkObjectRequest,
        remaining: Duration,
    ) -> Result<MutationReceipt, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteLinkObjectRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_link_object(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_unlink_object(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: UnlinkObjectRequest,
        remaining: Duration,
    ) -> Result<MutationReceipt, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteUnlinkObjectRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_unlink_object(request)
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
            definition_intents: Vec::new(),
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
        atomic_executor_replay_checked: bool,
        remaining: Duration,
    ) -> Result<MutationReceipt, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteDeleteIfVersionRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
            atomic_executor_replay_checked,
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
        definition_intents: Vec<(usize, DefinitionMutationIntent)>,
        remaining: Duration,
    ) -> Result<BulkWriteResponse, Status> {
        let fence = self.placement()?.fence();
        let definition_intents = definition_intents
            .into_iter()
            .map(|(operation_index, intent)| {
                let operation_index = u32::try_from(operation_index).map_err(|_| {
                    Status::invalid_argument("definition intent operation index is too large")
                })?;
                let kind = match intent.kind {
                    DefinitionKind::Index => wire::RoutedDefinitionKind::Index,
                    DefinitionKind::Accounting => wire::RoutedDefinitionKind::Accounting,
                };
                Ok(wire::RoutedDefinitionMutationIntent {
                    operation_index,
                    kind: kind as i32,
                    definition_id: intent.definition_id,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let mut request = Request::new(wire::RouteBulkWriteRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
            definition_intents,
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
        original_alias: Option<keldra_api::v1::ObjectAddress>,
        remaining: Duration,
    ) -> Result<DeleteVersionResponse, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteDeleteVersionRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
            original_alias,
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
        value: keldra_api::v1::ProvisionTenantRequest,
        remaining: Duration,
    ) -> Result<keldra_api::v1::ProvisionTenantResponse, Status> {
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
        value: keldra_api::v1::CreateBucketRequest,
        remaining: Duration,
    ) -> Result<keldra_api::v1::CreateBucketResponse, Status> {
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
        value: keldra_api::v1::ExchangeClientCredentialsRequest,
        remaining: Duration,
    ) -> Result<keldra_api::v1::AccessToken, Status> {
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
        value: keldra_api::v1::InvokeProgramRequest,
        remaining: Duration,
    ) -> Result<keldra_api::v1::InvokeProgramResponse, Status> {
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

    pub(crate) async fn route_built_in_replay_batch(
        &self,
        target: NodeId,
        address: &str,
        executor_nomination_log_index: u64,
        lookups: &[crate::programs::BuiltInReplayLookup],
        remaining: Duration,
    ) -> Result<Vec<Result<Option<crate::programs::InvokedProgramResult>, Status>>, Status> {
        if lookups.len() > keldra_store::MAX_ATOMIC_BATCH_MUTATIONS {
            return Err(Status::resource_exhausted(
                "built-in replay batch exceeds the atomic mutation bound",
            ));
        }
        let lookup_count = lookups.len();
        let expected_indices = lookups
            .iter()
            .map(|lookup| lookup.original_index)
            .collect::<Vec<_>>();
        if expected_indices.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(Status::invalid_argument(
                "built-in replay original indices must be strictly increasing",
            ));
        }
        let fence = self.placement()?.fence();
        let lookups = lookups
            .iter()
            .map(|lookup| {
                Ok(wire::BuiltInReplayLookup {
                    original_index: lookup.original_index,
                    authority_kind: u32::from(lookup.authority_kind),
                    contract_version: u32::from(lookup.contract_version),
                    invocation_id: lookup.invocation_id.to_vec(),
                    input_fingerprint: lookup.input_fingerprint.to_vec(),
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let mut request = Request::new(wire::RouteBuiltInReplayBatchRequest {
            peer: Some(self.context(fence, 0, remaining)?),
            executor_nomination_log_index,
            lookups,
        });
        request.set_timeout(remaining.min(MAX_CLUSTER_OPERATION_TIME));
        let response = self
            .client(target, address)?
            .route_built_in_replay_batch(request)
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        if response.outcomes.len() != lookup_count {
            return Err(Status::data_loss(
                "built-in replay batch response cardinality differs from its request",
            ));
        }
        response
            .outcomes
            .into_iter()
            .zip(expected_indices)
            .map(|(outcome, expected_index)| {
                if outcome.original_index != expected_index {
                    return Err(Status::data_loss(
                        "built-in replay batch response order is malformed",
                    ));
                }
                if outcome.error_code != 0 {
                    Ok(Err(Status::new(
                        tonic::Code::from_i32(outcome.error_code),
                        outcome.error_message,
                    )))
                } else if outcome.result_json.is_empty() {
                    Ok(Ok(None))
                } else {
                    decode_json(&outcome.result_json).map(Some).map(Ok)
                }
            })
            .collect()
    }

    pub(crate) async fn stage_program_path(
        &self,
        target: NodeId,
        address: &str,
        executor_nomination_log_index: u64,
        stage: &ProgramPathStage,
        remaining: Duration,
    ) -> Result<keldra_store::BlobRef, Status> {
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
        Ok(keldra_store::BlobRef {
            hash,
            length: response.stage_blob_length,
        })
    }

    pub(crate) async fn reserve_program_participant(
        &self,
        target: NodeId,
        address: &str,
        executor_nomination_log_index: u64,
        reservation: &ProgramReservation,
        remaining: Duration,
    ) -> Result<(), Status> {
        let fence = self.placement()?.fence();
        let response = self
            .client(target, address)?
            .reserve_program_participant(wire::ProgramReserveParticipantRequest {
                peer: Some(self.context(fence, 0, remaining)?),
                executor_nomination_log_index,
                reservation_json: encode_json(reservation)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)
    }

    pub(crate) async fn commit_program_participant(
        &self,
        target: NodeId,
        address: &str,
        executor_nomination_log_index: u64,
        commit_cursor: u64,
        reservation: &ProgramReservation,
        remaining: Duration,
    ) -> Result<(), Status> {
        let fence = self.placement()?.fence();
        let response = self
            .client(target, address)?
            .commit_program_participant(wire::ProgramCommitParticipantRequest {
                peer: Some(self.context(fence, 0, remaining)?),
                executor_nomination_log_index,
                commit_cursor,
                reservation_json: encode_json(reservation)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)
    }

    pub(crate) async fn release_program_participant(
        &self,
        target: NodeId,
        address: &str,
        executor_nomination_log_index: u64,
        finalized_commit_cursor: Option<u64>,
        reservation: &ProgramReservation,
        remaining: Duration,
    ) -> Result<(), Status> {
        let fence = self.placement()?.fence();
        let response = self
            .client(target, address)?
            .release_program_participant(wire::ProgramReleaseParticipantRequest {
                peer: Some(self.context(fence, 0, remaining)?),
                executor_nomination_log_index,
                reservation_json: encode_json(reservation)?,
                finalized_commit_cursor: finalized_commit_cursor.unwrap_or(0),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)
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
    ) -> Result<keldra_store::ReplicaProgramPathApplied, Status> {
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
        Ok(keldra_store::ReplicaProgramPathApplied {
            version: VersionId(response.version),
            replayed: response.replayed,
        })
    }

    pub(crate) async fn coordinate_program_alias_registry_finalization(
        &self,
        target: NodeId,
        address: &str,
        executor_nomination_log_index: u64,
        commit_cursor: u64,
        stage: &ProgramAliasRegistryStage,
        remaining: Duration,
    ) -> Result<ProgramAliasRegistryMutation, Status> {
        let fence = self.placement()?.fence();
        let response = self
            .client(target, address)?
            .coordinate_program_alias_registry_finalization(
                wire::ProgramCoordinateAliasRegistryFinalizationRequest {
                    peer: Some(self.context(fence, 0, remaining)?),
                    executor_nomination_log_index,
                    commit_cursor,
                    stage_json: encode_json(stage)?,
                },
            )
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        decode_json(&response.mutation_json)
    }

    pub(crate) async fn apply_program_alias_registry_finalization(
        &self,
        target: NodeId,
        address: &str,
        executor_nomination_log_index: u64,
        mutation: &ProgramAliasRegistryMutation,
        remaining: Duration,
    ) -> Result<bool, Status> {
        let fence = self.placement()?.fence();
        let response = self
            .client(target, address)?
            .apply_program_alias_registry_finalization(
                wire::ProgramApplyAliasRegistryFinalizationRequest {
                    peer: Some(self.context(fence, 0, remaining)?),
                    executor_nomination_log_index,
                    mutation_json: encode_json(mutation)?,
                },
            )
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(response.changed)
    }

    pub(crate) async fn read_program_alias_registry(
        &self,
        target: NodeId,
        address: &str,
        executor_nomination_log_index: u64,
        tenant_id: u64,
        bucket_id: u64,
        canonical_path: &str,
        remaining: Duration,
    ) -> Result<Option<keldra_store::ObjectAliasRegistry>, Status> {
        let fence = self.placement()?.fence();
        let response = self
            .client(target, address)?
            .read_program_alias_registry(wire::ProgramReadAliasRegistryRequest {
                peer: Some(self.context(fence, 0, remaining)?),
                executor_nomination_log_index,
                tenant_id,
                bucket_id,
                canonical_path: canonical_path.to_owned(),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        response
            .registry_json
            .as_deref()
            .map(decode_json)
            .transpose()
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
        self.context_with_timeout_limit(expected, hop_count, remaining, MAX_CLUSTER_OPERATION_TIME)
    }

    pub(super) fn context_with_timeout_limit(
        &self,
        expected: PlacementLogId,
        hop_count: u32,
        remaining: Duration,
        max_timeout: Duration,
    ) -> Result<wire::PeerContext, Status> {
        let placement = self.placement()?;
        if placement.fence() != expected {
            return Err(Status::unavailable(
                "active placement differs from the operation fence",
            ));
        }
        let millis = remaining.min(max_timeout).as_millis().max(1);
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

pub(super) fn wire_index_artifact_publish(
    request: &IndexArtifactPublish,
    peer: Option<wire::PeerContext>,
) -> wire::PublishIndexArtifactRequest {
    wire::PublishIndexArtifactRequest {
        peer,
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
        definition_kind: routed_definition_kind(request.definition_intent),
        guarded_definition_kind: routed_kind(
            request.definition_guard.as_ref().map(|guard| guard.kind),
        ),
        guarded_definition_path: request
            .definition_guard
            .as_ref()
            .map_or_else(String::new, |guard| guard.exact_path.clone()),
        guarded_definition_version: request
            .definition_guard
            .as_ref()
            .map_or(0, |guard| guard.expected_version.0),
        publication_progress: request.admission.is_publication_progress(),
    }
}

fn routed_definition_kind(intent: Option<DefinitionMutationIntent>) -> i32 {
    routed_kind(intent.map(|intent| intent.kind))
}

fn routed_kind(kind: Option<DefinitionKind>) -> i32 {
    match kind {
        Some(DefinitionKind::Index) => wire::RoutedDefinitionKind::Index as i32,
        Some(DefinitionKind::Accounting) => wire::RoutedDefinitionKind::Accounting as i32,
        None => wire::RoutedDefinitionKind::Unspecified as i32,
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
        caller: Caller,
        scope: DistributedWatchScope,
    ) -> Result<WatchSourceStatus, WatchSourceError> {
        let result = async {
            let application_id = caller
                .authenticated_app_id()
                .map_err(|error| Status::unauthenticated(error.to_string()))?
                .to_owned();
            let mut request = Request::new(wire::WatchStatusRequest {
                peer: Some(self.context(membership_revision, 0, MAX_CLUSTER_OPERATION_TIME)?),
                scope_json: encode_json(&scope)?,
                storage_tenant: caller.storage_tenant().as_str().to_owned(),
                application_id,
            });
            request.set_timeout(MAX_CLUSTER_OPERATION_TIME);
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
                    response.settled_through,
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
        caller: Caller,
        query: WatchSourceQuery,
    ) -> Result<WatchSourcePage, WatchSourceError> {
        let result = async {
            let application_id = caller
                .authenticated_app_id()
                .map_err(|error| Status::unauthenticated(error.to_string()))?
                .to_owned();
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
                storage_tenant: caller.storage_tenant().as_str().to_owned(),
                application_id,
            });
            request.set_timeout(MAX_CLUSTER_OPERATION_TIME);
            let response = self
                .client(target, address)?
                .read_watch_page(request)
                .await?
                .into_inner();
            require_response_schema(response.schema_version)?;
            let status = parse_watch_status(
                &response.source_id_json,
                response.tail,
                response.settled_through,
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
    settled_through: u64,
    retention_floor: u64,
    retained_entries: u64,
    retained_bytes: u64,
) -> Result<WatchJournalStatus, Status> {
    Ok(WatchJournalStatus {
        source_id: decode_json::<SourceId>(encoded_source)?,
        tail,
        settled_through,
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

pub(super) fn decode_indexed_artifact_outcomes(
    encoded: Vec<wire::IndexedIndexArtifactPublicationOutcome>,
    expected: usize,
) -> Result<Vec<IndexArtifactPublicationOutcome>, Status> {
    if encoded.len() != expected {
        return Err(Status::data_loss(
            "grouped index artifact outcome count differs from its request",
        ));
    }
    let mut outcomes = vec![None; expected];
    for encoded in encoded {
        require_response_schema(encoded.schema_version)?;
        let index = usize::try_from(encoded.request_index)
            .map_err(|_| Status::data_loss("index artifact outcome index does not fit usize"))?;
        let slot = outcomes.get_mut(index).ok_or_else(|| {
            Status::data_loss("index artifact outcome index is outside the request")
        })?;
        if slot.is_some() {
            return Err(Status::data_loss(
                "index artifact outcome index was returned more than once",
            ));
        }
        let outcome = match encoded.result.ok_or_else(|| {
            Status::data_loss("index artifact outcome has no published or failed result")
        })? {
            wire::indexed_index_artifact_publication_outcome::Result::Published(published) => {
                require_response_schema(published.schema_version)?;
                nonzero_artifact_outcome(published.version, published.replayed)
            }
            wire::indexed_index_artifact_publication_outcome::Result::Failed(failed) => {
                require_response_schema(failed.schema_version)?;
                Err(Status::new(
                    tonic::Code::from_i32(failed.code),
                    failed.message,
                ))
            }
        };
        *slot = Some(outcome);
    }
    outcomes
        .into_iter()
        .map(|outcome| {
            outcome.ok_or_else(|| Status::data_loss("index artifact outcome is missing"))
        })
        .collect()
}

fn validate_index_head(head: &super::IndexCurrentHead) -> Result<(), Status> {
    if head.tenant_id == 0
        || head.bucket_id == 0
        || head.exact_path.is_empty()
        || head.head.version.0 == 0
        || head.version.id.0 == 0
        || head.version.id > head.head.version
        || head.versions.len() != 1
        || head.versions.first() != Some(&head.version)
        || (head.head.version == head.version.id && head.head.deleted != head.version.deleted)
        || head.version.deleted != head.version.blob.is_none()
    {
        return Err(Status::data_loss(
            "index head scan returned an invalid retained descriptor",
        ));
    }
    Ok(())
}

fn wire_capability_range(min: u32, max: u32) -> Result<CapabilityRange, Status> {
    let min =
        u16::try_from(min).map_err(|_| Status::data_loss("capability minimum exceeds u16"))?;
    let max =
        u16::try_from(max).map_err(|_| Status::data_loss("capability maximum exceeds u16"))?;
    if min == 0 || min > max {
        return Err(Status::data_loss(
            "capability response contains an invalid range",
        ));
    }
    Ok(CapabilityRange { min, max })
}

pub(super) fn add_bearer_and_timeout<T>(
    request: &mut Request<T>,
    bearer: &str,
    timeout: Duration,
) -> Result<(), Status> {
    add_bearer_and_timeout_with_limit(request, bearer, timeout, MAX_CLUSTER_OPERATION_TIME)
}

pub(super) fn add_bearer_and_timeout_with_limit<T>(
    request: &mut Request<T>,
    bearer: &str,
    timeout: Duration,
    maximum: Duration,
) -> Result<(), Status> {
    let value = MetadataValue::try_from(format!("Bearer {bearer}"))
        .map_err(|_| Status::invalid_argument("bearer token cannot be represented as metadata"))?;
    request.metadata_mut().insert("authorization", value);
    request.set_timeout(timeout.min(maximum));
    Ok(())
}

fn watch_error(status: Status) -> WatchSourceError {
    if status.code() == tonic::Code::OutOfRange && status.message() == "RESUME_EXPIRED" {
        WatchSourceError::ResumeExpired
    } else if matches!(
        status.code(),
        tonic::Code::PermissionDenied | tonic::Code::Unauthenticated
    ) {
        WatchSourceError::AccessRevoked
    } else {
        WatchSourceError::Unavailable(status.to_string())
    }
}
