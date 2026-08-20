use keldra_authz::{AuthorizationCheck, ObjectRef, RealmId};
use keldra_consensus::NodeId;
use keldra_store::{
    AuthzConsistency, AuthzRealmMutation, AuthzRealmSnapshotApplied, AuthzRevision,
    AuthzSchemaPublicationMutation, AuthzScope, PlacementLogId, ReplicaAuthzRealmMutationApplied,
    ReplicaAuthzSchemaPublicationApplied, StorageTenantId,
};
use tonic::Status;

use super::{
    ClusterPeerTransport, MAX_CLUSTER_OPERATION_TIME, decode_json, encode_json,
    require_response_schema, wire,
};
use crate::authoritative_system::{FreshAuthorizationResult, StableBucketAuthorization};
use crate::authorization::SYSTEM_STABLE_TENANT_ID;
use crate::authz_distribution::{
    AuthzRealmReplicaCandidate, AuthzReplicaTransport, AuthzSchemaReplicaQuery,
};
use crate::distributed_list::{ListAuthorizationPermission, TenantAuthorizationCoordinator};
use crate::mutable_record_replica_group::MutableRecordReplicaGroup;
use crate::placement::PlacementKind;

#[tonic::async_trait]
impl AuthzReplicaTransport for ClusterPeerTransport {
    async fn apply_schema_publication(
        &self,
        target: NodeId,
        address: &str,
        stable_tenant_id: u64,
        mutation: &AuthzSchemaPublicationMutation,
    ) -> Result<ReplicaAuthzSchemaPublicationApplied, Status> {
        ClusterPeerTransport::apply_schema_publication(
            self,
            target,
            address,
            stable_tenant_id,
            mutation,
        )
        .await
    }

    async fn has_schema_publication(
        &self,
        target: NodeId,
        address: &str,
        stable_tenant_id: u64,
        query: &AuthzSchemaReplicaQuery,
    ) -> Result<bool, Status> {
        ClusterPeerTransport::has_schema_publication(self, target, address, stable_tenant_id, query)
            .await
    }

    async fn apply_realm_mutation(
        &self,
        target: NodeId,
        address: &str,
        stable_tenant_id: u64,
        mutation: &AuthzRealmMutation,
    ) -> Result<ReplicaAuthzRealmMutationApplied, Status> {
        let response = self
            .client(target, address)?
            .apply_realm_mutation(wire::RealmMutationApplyRequest {
                peer: Some(self.context(
                    mutation.stamp.active_placement_log_id,
                    0,
                    MAX_CLUSTER_OPERATION_TIME,
                )?),
                stable_tenant_id,
                mutation_json: encode_json(mutation)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(ReplicaAuthzRealmMutationApplied {
            revision: AuthzRevision(response.revision),
            replayed: response.replayed,
        })
    }

    async fn read_realm_candidate(
        &self,
        target: NodeId,
        address: &str,
        stable_tenant_id: u64,
        scope: &AuthzScope,
    ) -> Result<Option<AuthzRealmReplicaCandidate>, Status> {
        let placement = self.placement()?;
        let response = self
            .client(target, address)?
            .read_realm_candidate(wire::RealmCandidateReadRequest {
                peer: Some(self.context(placement.fence(), 0, MAX_CLUSTER_OPERATION_TIME)?),
                stable_tenant_id,
                scope_json: encode_json(scope)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        let manifest = decode_optional(response.present, &response.manifest_json)?;
        manifest
            .map(AuthzRealmReplicaCandidate::from_manifest)
            .transpose()
    }

    async fn install_realm_candidate(
        &self,
        target: NodeId,
        address: &str,
        stable_tenant_id: u64,
        source: Option<(NodeId, String)>,
        scope: &AuthzScope,
        winner: Option<&AuthzRealmReplicaCandidate>,
    ) -> Result<AuthzRealmSnapshotApplied, Status> {
        let placement = self.placement()?;
        let context = self.context(placement.fence(), 0, MAX_CLUSTER_OPERATION_TIME)?;
        let (sender, receiver) = tokio::sync::mpsc::channel(2);

        let producer = if let Some(winner) = winner {
            winner.validate_for(scope)?;
            let (source_node, source_address) = source
                .ok_or_else(|| Status::internal("present realm winner has no source replica"))?;
            let mut source_stream = self
                .client(source_node, &source_address)?
                .read_realm_aggregate(wire::RealmAggregateReadRequest {
                    peer: Some(context.clone()),
                    stable_tenant_id,
                    scope_json: encode_json(scope)?,
                })
                .await?
                .into_inner();
            let first = next_source_frame(&mut source_stream).await?;
            require_response_schema(first.schema_version)?;
            let observed =
                AuthzRealmReplicaCandidate::from_manifest(decode_json(&first.manifest_json)?)?;
            if observed != *winner || first.offset != 0 || !first.content.is_empty() || first.end {
                return Err(Status::data_loss(
                    "authorization source did not stream the selected quorum candidate",
                ));
            }
            sender
                .send(wire::RealmCandidateInstallFrame {
                    peer: Some(context),
                    stable_tenant_id,
                    scope_json: encode_json(scope)?,
                    present: true,
                    manifest_json: encode_json(&winner.manifest)?,
                    offset: 0,
                    content: Vec::new(),
                    end: false,
                })
                .await
                .map_err(|_| Status::cancelled("realm install stream closed"))?;
            let expected_bytes = winner.manifest.encoded_bytes;
            Some(tokio::spawn(async move {
                forward_source_stream(source_stream, sender, expected_bytes).await
            }))
        } else {
            sender
                .send(wire::RealmCandidateInstallFrame {
                    peer: Some(context),
                    stable_tenant_id,
                    scope_json: encode_json(scope)?,
                    present: false,
                    manifest_json: Vec::new(),
                    offset: 0,
                    content: Vec::new(),
                    end: true,
                })
                .await
                .map_err(|_| Status::cancelled("realm install stream closed"))?;
            drop(sender);
            None
        };

        let response = self
            .client(target, address)?
            .install_realm_candidate(tokio_stream::wrappers::ReceiverStream::new(receiver))
            .await;
        if let Some(producer) = producer {
            producer.await.map_err(|error| {
                Status::internal(format!("realm forwarding task failed: {error}"))
            })??;
        }
        let response = response?.into_inner();
        require_response_schema(response.schema_version)?;
        Ok(AuthzRealmSnapshotApplied {
            revision: AuthzRevision(response.revision),
            replayed: response.replayed,
            retained_receipts: usize::try_from(response.retained_receipts).map_err(|_| {
                Status::resource_exhausted("retained receipt count does not fit this node")
            })?,
        })
    }
}

#[tonic::async_trait]
impl TenantAuthorizationCoordinator for ClusterPeerTransport {
    async fn allows(
        &self,
        storage_tenant: StorageTenantId,
        realm: RealmId,
        subject: ObjectRef,
        permission: ListAuthorizationPermission,
        placement_fence: PlacementLogId,
    ) -> Result<bool, Status> {
        if realm != RealmId::system() {
            return Err(Status::invalid_argument(
                "list authorization requires the protected system realm",
            ));
        }
        let placement = self.placement()?;
        if placement.fence() != placement_fence {
            return Err(Status::unavailable(
                "authorization placement changed during the request",
            ));
        }
        let ListAuthorizationPermission::BucketObjectsGet {
            bucket,
            expected_tenant_id,
            expected_bucket_id,
        } = permission;
        let group = MutableRecordReplicaGroup::select(
            PlacementKind::ZanzibarRealm,
            placement.cluster_id(),
            &SYSTEM_STABLE_TENANT_ID.to_be_bytes(),
            placement.placement_nodes(),
        )
        .ok_or_else(|| Status::unavailable("cluster has no Zanzibar replica"))?;
        let target = group.coordinator();
        let address = placement.address(target).ok_or_else(|| {
            Status::unavailable("Zanzibar coordinator has no current peer address")
        })?;
        let tenant = storage_tenant.as_str().to_owned();
        let object = crate::authorization::bucket_resource(&tenant, &bucket)
            .map_err(crate::authz_api::authz_status)?;
        let check = AuthorizationCheck::new(subject, object, "get_object");
        let (allowed, _, _) = self
            .fresh_authorization_check(
                target,
                &address.0,
                SYSTEM_STABLE_TENANT_ID,
                &AuthzScope::system(),
                AuthzConsistency::Latest,
                &check,
                Some(wire::StableBucketBinding {
                    storage_tenant: tenant,
                    bucket,
                    expected_tenant_id,
                    expected_bucket_id,
                }),
                placement_fence,
            )
            .await?;
        Ok(allowed)
    }
}

impl ClusterPeerTransport {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn fresh_authorization_check(
        &self,
        target: NodeId,
        address: &str,
        stable_tenant_id: u64,
        scope: &AuthzScope,
        consistency: AuthzConsistency,
        check: &AuthorizationCheck,
        stable_bucket: Option<wire::StableBucketBinding>,
        placement_fence: PlacementLogId,
    ) -> Result<(bool, AuthzRevision, u64), Status> {
        let (mode, revision) = consistency_to_wire(consistency);
        let response = self
            .client(target, address)?
            .fresh_authorization_check(wire::FreshAuthorizationCheckRequest {
                peer: Some(self.context(placement_fence, 0, MAX_CLUSTER_OPERATION_TIME)?),
                stable_tenant_id,
                scope_json: encode_json(scope)?,
                check_json: encode_json(check)?,
                consistency: mode as i32,
                consistency_revision: revision,
                stable_bucket,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok((
            response.allowed,
            AuthzRevision(response.revision),
            response.binding_generation,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn fresh_authorization_checks(
        &self,
        target: NodeId,
        address: &str,
        stable_tenant_id: u64,
        scope: &AuthzScope,
        consistency: AuthzConsistency,
        checks: &[AuthorizationCheck],
        stable_buckets: &[StableBucketAuthorization],
        placement_fence: PlacementLogId,
    ) -> Result<FreshAuthorizationResult, Status> {
        if checks.is_empty() || checks.len() > 1_000 {
            return Err(Status::invalid_argument(
                "fresh authorization batch must contain 1..=1000 checks",
            ));
        }
        let (mode, revision) = consistency_to_wire(consistency);
        let response = self
            .client(target, address)?
            .fresh_authorization_checks(wire::FreshAuthorizationChecksRequest {
                peer: Some(self.context(placement_fence, 0, MAX_CLUSTER_OPERATION_TIME)?),
                stable_tenant_id,
                scope_json: encode_json(scope)?,
                checks_json: checks.iter().map(encode_json).collect::<Result<_, _>>()?,
                consistency: mode as i32,
                consistency_revision: revision,
                stable_buckets: stable_buckets
                    .iter()
                    .map(|binding| wire::StableBucketBinding {
                        storage_tenant: binding.storage_tenant.clone(),
                        bucket: binding.bucket.clone(),
                        expected_tenant_id: binding.expected_tenant_id,
                        expected_bucket_id: binding.expected_bucket_id,
                    })
                    .collect(),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        if response.allowed.len() != checks.len() {
            return Err(Status::data_loss(
                "fresh authorization result count differs from its request",
            ));
        }
        Ok(FreshAuthorizationResult {
            allowed: response.allowed,
            revision: AuthzRevision(response.revision),
            binding_generation: response.binding_generation,
        })
    }
}

async fn next_source_frame(
    stream: &mut tonic::Streaming<wire::RealmAggregateFrame>,
) -> Result<wire::RealmAggregateFrame, Status> {
    tokio::time::timeout(MAX_CLUSTER_OPERATION_TIME, stream.message())
        .await
        .map_err(|_| Status::deadline_exceeded("authorization source stream stalled"))??
        .ok_or_else(|| Status::data_loss("authorization source stream ended early"))
}

async fn forward_source_stream(
    mut source: tonic::Streaming<wire::RealmAggregateFrame>,
    sender: tokio::sync::mpsc::Sender<wire::RealmCandidateInstallFrame>,
    expected_bytes: u64,
) -> Result<(), Status> {
    let mut offset = 0_u64;
    loop {
        let frame = next_source_frame(&mut source).await?;
        require_response_schema(frame.schema_version)?;
        if !frame.manifest_json.is_empty()
            || frame.offset != offset
            || frame.content.len() > 64 * 1024
            || (frame.end && !frame.content.is_empty())
            || (!frame.end && frame.content.is_empty())
        {
            return Err(Status::data_loss(
                "authorization source stream is not contiguous or canonical",
            ));
        }
        if frame.end {
            if offset != expected_bytes {
                return Err(Status::data_loss(
                    "authorization source stream length differs from its manifest",
                ));
            }
        } else {
            offset = offset
                .checked_add(frame.content.len() as u64)
                .ok_or_else(|| Status::resource_exhausted("realm stream length overflow"))?;
        }
        sender
            .send(wire::RealmCandidateInstallFrame {
                peer: None,
                stable_tenant_id: 0,
                scope_json: Vec::new(),
                present: false,
                manifest_json: Vec::new(),
                offset: frame.offset,
                content: frame.content,
                end: frame.end,
            })
            .await
            .map_err(|_| Status::cancelled("authorization destination stream closed"))?;
        if frame.end {
            return Ok(());
        }
    }
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

fn consistency_to_wire(consistency: AuthzConsistency) -> (wire::AuthorizationConsistencyMode, u64) {
    match consistency {
        AuthzConsistency::Latest => (
            wire::AuthorizationConsistencyMode::AuthorizationConsistencyLatest,
            0,
        ),
        AuthzConsistency::AtLeast(revision) => (
            wire::AuthorizationConsistencyMode::AuthorizationConsistencyAtLeast,
            revision.0,
        ),
        AuthzConsistency::Exact(revision) => (
            wire::AuthorizationConsistencyMode::AuthorizationConsistencyExact,
            revision.0,
        ),
    }
}
