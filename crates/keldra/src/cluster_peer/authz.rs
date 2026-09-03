use std::io::{self, Read, Write};
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use keldra_authz::AuthorizationCheck;
use keldra_consensus::{NodeId, PeerSpkiSha256};
use keldra_store::{
    AuthzConsistency, AuthzRealmMutation, AuthzRealmSnapshotError, AuthzRealmTransferManifest,
    AuthzRevision, AuthzScope, AuthzStoreError, PlacementLogId,
};
use tonic::{Request, Response, Status, Streaming};

use super::{
    CLUSTER_PEER_SCHEMA_VERSION, ClusterPeerService, MAX_CLUSTER_OPERATION_TIME, decode_json,
    encode_json, wire,
};
use crate::authz_distribution::{AuthzRealmReplicaCandidate, ZanzibarDistribution};
use crate::logical_name_resolution::LogicalNameResolution;
use crate::mutable_record_replica_group::MutableRecordReplicaGroup;
use crate::placement::PlacementKind;

const REALM_FRAME_BYTES: usize = 64 * 1024;

pub(super) type RealmAggregateStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<wire::RealmAggregateFrame, Status>> + Send>>;

#[tonic::async_trait]
pub(crate) trait FreshAuthorizationHandler: Send + Sync + 'static {
    async fn fresh_check(
        &self,
        stable_tenant_id: u64,
        scope: AuthzScope,
        consistency: AuthzConsistency,
        check: AuthorizationCheck,
    ) -> Result<(bool, AuthzRevision, u64), Status>;

    async fn fresh_checks(
        &self,
        stable_tenant_id: u64,
        scope: AuthzScope,
        consistency: AuthzConsistency,
        checks: Vec<AuthorizationCheck>,
    ) -> Result<(Vec<bool>, AuthzRevision, u64), Status> {
        if checks.is_empty() {
            return Err(Status::invalid_argument(
                "authorization check batch must not be empty",
            ));
        }
        let mut allowed = Vec::with_capacity(checks.len());
        let mut pinned = None;
        let mut generation = None;
        for check in checks {
            let requested = pinned.map_or(consistency, AuthzConsistency::Exact);
            let (result, revision, observed_generation) = self
                .fresh_check(stable_tenant_id, scope.clone(), requested, check)
                .await?;
            if pinned.is_some_and(|pinned| pinned != revision)
                || generation.is_some_and(|generation| generation != observed_generation)
            {
                return Err(Status::unavailable(
                    "authorization revision changed during batch evaluation",
                ));
            }
            pinned = Some(revision);
            generation = Some(observed_generation);
            allowed.push(result);
        }
        Ok((
            allowed,
            pinned.expect("non-empty checks establish a revision"),
            generation.expect("non-empty checks establish a generation"),
        ))
    }
}

#[tonic::async_trait]
impl FreshAuthorizationHandler for ZanzibarDistribution {
    async fn fresh_check(
        &self,
        stable_tenant_id: u64,
        scope: AuthzScope,
        consistency: AuthzConsistency,
        check: AuthorizationCheck,
    ) -> Result<(bool, AuthzRevision, u64), Status> {
        ZanzibarDistribution::fresh_check_with_generation(
            self,
            stable_tenant_id,
            scope,
            consistency,
            check,
        )
        .await
    }

    async fn fresh_checks(
        &self,
        stable_tenant_id: u64,
        scope: AuthzScope,
        consistency: AuthzConsistency,
        checks: Vec<AuthorizationCheck>,
    ) -> Result<(Vec<bool>, AuthzRevision, u64), Status> {
        ZanzibarDistribution::fresh_checks_with_generation(
            self,
            stable_tenant_id,
            scope,
            consistency,
            checks,
        )
        .await
    }
}

/// Breaks the listener/startup cycle without weakening fail-closed checks.
/// The peer listener can start before join, while checks remain unavailable
/// until the current Zanzibar coordinator has been installed.
#[derive(Clone, Default)]
pub(crate) struct LateBoundFreshAuthorization {
    inner: Arc<OnceLock<Arc<dyn FreshAuthorizationHandler>>>,
}

impl LateBoundFreshAuthorization {
    pub(crate) fn install(
        &self,
        handler: Arc<dyn FreshAuthorizationHandler>,
    ) -> Result<(), Arc<dyn FreshAuthorizationHandler>> {
        self.inner.set(handler)
    }

    async fn check(
        &self,
        stable_tenant_id: u64,
        scope: AuthzScope,
        consistency: AuthzConsistency,
        check: AuthorizationCheck,
    ) -> Result<(bool, AuthzRevision, u64), Status> {
        let handler = self
            .inner
            .get()
            .cloned()
            .ok_or_else(|| Status::unavailable("fresh Zanzibar authorization is not ready"))?;
        handler
            .fresh_check(stable_tenant_id, scope, consistency, check)
            .await
    }

    async fn check_many(
        &self,
        stable_tenant_id: u64,
        scope: AuthzScope,
        consistency: AuthzConsistency,
        checks: Vec<AuthorizationCheck>,
    ) -> Result<(Vec<bool>, AuthzRevision, u64), Status> {
        let handler = self
            .inner
            .get()
            .cloned()
            .ok_or_else(|| Status::unavailable("fresh Zanzibar authorization is not ready"))?;
        handler
            .fresh_checks(stable_tenant_id, scope, consistency, checks)
            .await
    }
}

impl ClusterPeerService {
    pub(super) async fn apply_realm_mutation_call(
        &self,
        request: Request<wire::RealmMutationApplyRequest>,
    ) -> Result<Response<wire::RealmMutationApplied>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let stable_tenant_id = request.get_ref().stable_tenant_id;
        let mutation: AuthzRealmMutation = decode_json(&request.get_ref().mutation_json)?;
        require_realm_replica(
            &admitted.placement,
            admitted.authenticated.node_id,
            self.local_node,
            stable_tenant_id,
            &mutation.scope,
        )?;
        require_mutation_fence(
            mutation.stamp.active_placement_log_id,
            mutation.stamp.source_id.node_id,
            &admitted,
        )?;
        let repository = self.store.authz();
        let applied = super::storage::bounded_blocking(admitted.timeout, move || {
            repository
                .apply_authz_realm_mutation_replica(&mutation)
                .map_err(super::storage::authz_status)
        })
        .await?;
        Ok(Response::new(wire::RealmMutationApplied {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            revision: applied.revision.0,
            replayed: applied.replayed,
        }))
    }

    pub(super) async fn read_realm_candidate_call(
        &self,
        request: Request<wire::RealmCandidateReadRequest>,
    ) -> Result<Response<wire::RealmCandidate>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let stable_tenant_id = request.get_ref().stable_tenant_id;
        let scope: AuthzScope = decode_json(&request.get_ref().scope_json)?;
        require_realm_reconciliation_peer(
            &admitted.placement,
            admitted.authenticated.node_id,
            self.local_node,
            stable_tenant_id,
            &scope,
        )?;
        let repository = self.store.authz();
        let candidate = super::storage::bounded_blocking(admitted.timeout, move || {
            read_candidate(&repository, &scope)
        })
        .await?;
        Ok(Response::new(wire::RealmCandidate {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            present: candidate.is_some(),
            manifest_json: candidate
                .as_ref()
                .map(|candidate| encode_json(&candidate.manifest))
                .transpose()?
                .unwrap_or_default(),
        }))
    }

    pub(super) async fn read_realm_aggregate_call(
        &self,
        request: Request<wire::RealmAggregateReadRequest>,
    ) -> Result<Response<RealmAggregateStream>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let stable_tenant_id = request.get_ref().stable_tenant_id;
        let scope: AuthzScope = decode_json(&request.get_ref().scope_json)?;
        require_realm_reconciliation_peer(
            &admitted.placement,
            admitted.authenticated.node_id,
            self.local_node,
            stable_tenant_id,
            &scope,
        )?;
        let repository = self.store.authz();
        let manifest_scope = scope.clone();
        let manifest = super::storage::bounded_blocking(admitted.timeout, move || {
            repository
                .export_authz_realm_stream(&manifest_scope, io::sink())
                .map_err(snapshot_status)?
                .ok_or_else(|| Status::not_found("authorization realm is absent"))
        })
        .await?;
        AuthzRealmReplicaCandidate::from_manifest(manifest.clone())?.validate_for(&scope)?;

        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        sender
            .send(Ok(wire::RealmAggregateFrame {
                schema_version: CLUSTER_PEER_SCHEMA_VERSION,
                offset: 0,
                content: Vec::new(),
                end: false,
                manifest_json: encode_json(&manifest)?,
            }))
            .await
            .map_err(|_| Status::cancelled("authorization realm stream closed"))?;
        let repository = self.store.authz();
        tokio::task::spawn_blocking(move || {
            let mut writer = RealmFrameWriter::new(sender);
            match repository.export_authz_realm_stream(&scope, &mut writer) {
                Ok(Some(observed)) if observed == manifest => writer.finish(),
                Ok(Some(_)) => writer.fail(Status::unavailable(
                    "authorization realm changed during aggregate export",
                )),
                Ok(None) => writer.fail(Status::not_found(
                    "authorization realm disappeared during aggregate export",
                )),
                Err(error) => writer.fail(snapshot_status(error)),
            }
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(receiver),
        )))
    }

    pub(super) async fn install_realm_candidate_call(
        &self,
        request: Request<Streaming<wire::RealmCandidateInstallFrame>>,
    ) -> Result<Response<wire::RealmCandidateInstalled>, Status> {
        let started = tokio::time::Instant::now();
        let pin = request
            .extensions()
            .get::<PeerSpkiSha256>()
            .copied()
            .ok_or_else(|| Status::unauthenticated("peer mTLS identity is missing"))?;
        let mut stream = request.into_inner();
        let first = next_install_frame(&mut stream, started + MAX_CLUSTER_OPERATION_TIME).await?;
        let admitted = self.admit_pin(
            pin,
            first.peer.as_ref().ok_or_else(|| {
                Status::invalid_argument("realm install header requires peer context")
            })?,
            0,
        )?;
        let deadline = started + admitted.timeout;
        let scope: AuthzScope = decode_json(&first.scope_json)?;
        require_realm_reconciliation_peer(
            &admitted.placement,
            admitted.authenticated.node_id,
            self.local_node,
            first.stable_tenant_id,
            &scope,
        )?;

        if !first.present {
            require_absent_header(&first)?;
            let repository = self.store.authz();
            let applied = super::storage::bounded_blocking(admitted.timeout, move || {
                repository
                    .install_quorum_reconciled_authz_realm_candidate(&scope, None)
                    .map_err(snapshot_status)
            })
            .await?;
            return installed_response(applied);
        }
        if first.offset != 0 || !first.content.is_empty() || first.end {
            return Err(Status::invalid_argument(
                "realm install must start with one manifest-only header",
            ));
        }
        let manifest: AuthzRealmTransferManifest = decode_json(&first.manifest_json)?;
        let selected = AuthzRealmReplicaCandidate::from_manifest(manifest.clone())?;
        selected.validate_for(&scope)?;

        let (sender, receiver) = tokio::sync::mpsc::channel::<Vec<u8>>(2);
        let repository = self.store.authz();
        let install = tokio::task::spawn_blocking(move || {
            repository
                .install_quorum_reconciled_authz_realm_stream(
                    &manifest,
                    BlockingFrameReader::new(receiver),
                )
                .map_err(snapshot_status)
        });
        let mut offset = 0_u64;
        loop {
            let frame = next_install_frame(&mut stream, deadline).await?;
            require_content_frame(&frame, offset)?;
            if frame.end {
                break;
            }
            offset = offset
                .checked_add(frame.content.len() as u64)
                .ok_or_else(|| Status::resource_exhausted("realm stream length overflow"))?;
            sender
                .send(frame.content)
                .await
                .map_err(|_| Status::cancelled("realm install worker closed"))?;
        }
        drop(sender);
        let applied = tokio::time::timeout_at(deadline, install)
            .await
            .map_err(|_| Status::deadline_exceeded("realm install deadline exceeded"))?
            .map_err(|error| Status::internal(format!("realm install worker failed: {error}")))??;
        installed_response(applied)
    }

    pub(super) async fn fresh_authorization_check_call(
        &self,
        request: Request<wire::FreshAuthorizationCheckRequest>,
    ) -> Result<Response<wire::FreshAuthorizationCheckResult>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let deadline = tokio::time::Instant::now() + admitted.timeout;
        let raw = request.get_ref();
        let stable_tenant_id = raw.stable_tenant_id;
        require_realm_read_replica(&admitted.placement, self.local_node, stable_tenant_id)?;
        let scope: AuthzScope = decode_json(&raw.scope_json)?;
        scope
            .handoff_order_key()
            .map_err(super::storage::authz_status)?;
        let check: AuthorizationCheck = decode_json(&raw.check_json)?;
        let consistency = consistency_from_wire(raw.consistency, raw.consistency_revision)?;
        if let Some(binding) = raw.stable_bucket.clone() {
            if binding.expected_tenant_id == 0
                || binding.expected_bucket_id == 0
                || binding.storage_tenant.is_empty()
                || binding.bucket.is_empty()
            {
                return Err(Status::invalid_argument(
                    "stable bucket authorization binding is invalid",
                ));
            }
            let tenant = keldra_store::StorageTenantId::parse(&binding.storage_tenant)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            let (resolved_tenant, resolved_bucket) = tokio::time::timeout_at(deadline, async {
                let tenant_id = self.name_resolution.resolve_tenant_id(&tenant).await?;
                let bucket_id = self
                    .name_resolution
                    .resolve_bucket_id(binding.expected_tenant_id, &binding.bucket)
                    .await?;
                Ok::<_, Status>((tenant_id, bucket_id))
            })
            .await
            .map_err(|_| Status::deadline_exceeded("authorization deadline exceeded"))??;
            if resolved_tenant != Some(binding.expected_tenant_id)
                || resolved_bucket != Some(binding.expected_bucket_id)
            {
                return Err(Status::unavailable(
                    "bucket identity changed while authorizing the request",
                ));
            }
        }
        let (allowed, revision, binding_generation) = tokio::time::timeout_at(
            deadline,
            self.fresh_authorization
                .check(stable_tenant_id, scope, consistency, check),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("authorization deadline exceeded"))??;
        Ok(Response::new(wire::FreshAuthorizationCheckResult {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            allowed,
            revision: revision.0,
            binding_generation,
        }))
    }

    pub(super) async fn fresh_authorization_checks_call(
        &self,
        request: Request<wire::FreshAuthorizationChecksRequest>,
    ) -> Result<Response<wire::FreshAuthorizationChecksResult>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let deadline = tokio::time::Instant::now() + admitted.timeout;
        let raw = request.get_ref();
        let stable_tenant_id = raw.stable_tenant_id;
        require_realm_read_replica(&admitted.placement, self.local_node, stable_tenant_id)?;
        if raw.checks_json.is_empty() || raw.checks_json.len() > 1_000 {
            return Err(Status::resource_exhausted(
                "fresh authorization batch must contain 1..=1000 checks",
            ));
        }
        if raw.stable_buckets.len() > 1_000 {
            return Err(Status::resource_exhausted(
                "fresh authorization batch exceeds 1000 stable bucket bindings",
            ));
        }
        let scope: AuthzScope = decode_json(&raw.scope_json)?;
        scope
            .handoff_order_key()
            .map_err(super::storage::authz_status)?;
        let checks = raw
            .checks_json
            .iter()
            .map(|encoded| decode_json(encoded))
            .collect::<Result<Vec<AuthorizationCheck>, _>>()?;
        let consistency = consistency_from_wire(raw.consistency, raw.consistency_revision)?;
        verify_stable_bucket_bindings(
            &self.name_resolution,
            stable_tenant_id,
            raw.stable_buckets.clone(),
            deadline,
        )
        .await?;
        let (allowed, revision, binding_generation) = tokio::time::timeout_at(
            deadline,
            self.fresh_authorization
                .check_many(stable_tenant_id, scope, consistency, checks),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("authorization deadline exceeded"))??;
        Ok(Response::new(wire::FreshAuthorizationChecksResult {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            allowed,
            revision: revision.0,
            binding_generation,
        }))
    }
}

async fn verify_stable_bucket_bindings(
    names: &crate::logical_name_resolution::LateBoundLogicalNameResolution,
    _stable_tenant_id: u64,
    bindings: Vec<wire::StableBucketBinding>,
    deadline: tokio::time::Instant,
) -> Result<(), Status> {
    for binding in bindings {
        if binding.expected_tenant_id == 0
            || binding.expected_bucket_id == 0
            || binding.storage_tenant.is_empty()
            || binding.bucket.is_empty()
        {
            return Err(Status::invalid_argument(
                "stable bucket authorization binding is invalid",
            ));
        }
        let tenant = keldra_store::StorageTenantId::parse(&binding.storage_tenant)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let (tenant_id, bucket_id) = tokio::time::timeout_at(deadline, async {
            let tenant_id = names.resolve_tenant_id(&tenant).await?;
            let bucket_id = names
                .resolve_bucket_id(binding.expected_tenant_id, &binding.bucket)
                .await?;
            Ok::<_, Status>((tenant_id, bucket_id))
        })
        .await
        .map_err(|_| Status::deadline_exceeded("authorization deadline exceeded"))??;
        if tenant_id != Some(binding.expected_tenant_id)
            || bucket_id != Some(binding.expected_bucket_id)
        {
            return Err(Status::unavailable(
                "bucket identity changed while authorizing the request",
            ));
        }
    }
    Ok(())
}

fn require_realm_replica(
    placement: &crate::cluster_placement::ClusterPlacement,
    source: NodeId,
    local: NodeId,
    stable_tenant_id: u64,
    scope: &AuthzScope,
) -> Result<(), Status> {
    let group = realm_group(placement, stable_tenant_id)?;
    scope
        .handoff_order_key()
        .map_err(super::storage::authz_status)?;
    if group.coordinator() != source || !group.replicas().contains(&local) {
        return Err(Status::failed_precondition(
            "authorization realm is not routed from its coordinator to this replica",
        ));
    }
    Ok(())
}

fn require_realm_reconciliation_peer(
    placement: &crate::cluster_placement::ClusterPlacement,
    source: NodeId,
    local: NodeId,
    stable_tenant_id: u64,
    scope: &AuthzScope,
) -> Result<(), Status> {
    let group = realm_group(placement, stable_tenant_id)?;
    scope
        .handoff_order_key()
        .map_err(super::storage::authz_status)?;
    require_realm_reconciliation_members(&group, source, local)
}

fn require_realm_reconciliation_members(
    group: &MutableRecordReplicaGroup,
    source: NodeId,
    local: NodeId,
) -> Result<(), Status> {
    if !group.replicas().contains(&source) || !group.replicas().contains(&local) {
        return Err(Status::failed_precondition(
            "authorization realm reconciliation must stay within its selected replicas",
        ));
    }
    Ok(())
}

fn require_realm_read_replica(
    placement: &crate::cluster_placement::ClusterPlacement,
    local: NodeId,
    stable_tenant_id: u64,
) -> Result<(), Status> {
    if !realm_group(placement, stable_tenant_id)?
        .replicas()
        .contains(&local)
    {
        return Err(Status::failed_precondition(
            "fresh authorization check is not addressed to a selected tenant replica",
        ));
    }
    Ok(())
}

fn realm_group(
    placement: &crate::cluster_placement::ClusterPlacement,
    stable_tenant_id: u64,
) -> Result<MutableRecordReplicaGroup, Status> {
    if stable_tenant_id == 0 {
        return Err(Status::invalid_argument(
            "stable authorization tenant ID must be non-zero",
        ));
    }
    MutableRecordReplicaGroup::select(
        PlacementKind::ZanzibarRealm,
        placement.cluster_id(),
        &stable_tenant_id.to_be_bytes(),
        placement.placement_nodes(),
    )
    .ok_or_else(|| Status::unavailable("cluster has no Zanzibar replica"))
}

fn require_mutation_fence(
    fence: PlacementLogId,
    source_node_id: u16,
    admitted: &super::admission::AdmittedPeer,
) -> Result<(), Status> {
    if fence != admitted.placement.fence()
        || u64::from(source_node_id) != admitted.authenticated.node_id.0
    {
        return Err(Status::unavailable(
            "authorization mutation does not carry its coordinator placement fence",
        ));
    }
    Ok(())
}

fn read_candidate(
    repository: &keldra_store::AuthzRepository,
    scope: &AuthzScope,
) -> Result<Option<AuthzRealmReplicaCandidate>, Status> {
    let manifest = repository
        .export_authz_realm_stream(scope, io::sink())
        .map_err(snapshot_status)?;
    manifest
        .map(AuthzRealmReplicaCandidate::from_manifest)
        .transpose()
}

fn require_absent_header(first: &wire::RealmCandidateInstallFrame) -> Result<(), Status> {
    if !first.manifest_json.is_empty()
        || first.offset != 0
        || !first.content.is_empty()
        || !first.end
    {
        return Err(Status::invalid_argument(
            "absent realm install must be one terminal header",
        ));
    }
    Ok(())
}

fn require_content_frame(
    frame: &wire::RealmCandidateInstallFrame,
    expected_offset: u64,
) -> Result<(), Status> {
    if frame.peer.is_some()
        || frame.stable_tenant_id != 0
        || !frame.scope_json.is_empty()
        || frame.present
        || !frame.manifest_json.is_empty()
        || frame.offset != expected_offset
        || frame.content.len() > REALM_FRAME_BYTES
        || (frame.end && !frame.content.is_empty())
        || (!frame.end && frame.content.is_empty())
    {
        return Err(Status::invalid_argument(
            "realm install content frames are not contiguous or canonical",
        ));
    }
    Ok(())
}

async fn next_install_frame(
    stream: &mut Streaming<wire::RealmCandidateInstallFrame>,
    deadline: tokio::time::Instant,
) -> Result<wire::RealmCandidateInstallFrame, Status> {
    tokio::time::timeout_at(deadline, stream.message())
        .await
        .map_err(|_| Status::deadline_exceeded("authorization realm stream deadline exceeded"))??
        .ok_or_else(|| Status::invalid_argument("authorization realm stream ended early"))
}

fn consistency_from_wire(mode: i32, revision: u64) -> Result<AuthzConsistency, Status> {
    match wire::AuthorizationConsistencyMode::try_from(mode) {
        Ok(wire::AuthorizationConsistencyMode::AuthorizationConsistencyLatest) if revision == 0 => {
            Ok(AuthzConsistency::Latest)
        }
        Ok(wire::AuthorizationConsistencyMode::AuthorizationConsistencyAtLeast)
            if revision != 0 =>
        {
            Ok(AuthzConsistency::AtLeast(AuthzRevision(revision)))
        }
        Ok(wire::AuthorizationConsistencyMode::AuthorizationConsistencyExact) if revision != 0 => {
            Ok(AuthzConsistency::Exact(AuthzRevision(revision)))
        }
        _ => Err(Status::invalid_argument(
            "authorization consistency mode and revision disagree",
        )),
    }
}

fn installed_response(
    applied: keldra_store::AuthzRealmSnapshotApplied,
) -> Result<Response<wire::RealmCandidateInstalled>, Status> {
    Ok(Response::new(wire::RealmCandidateInstalled {
        schema_version: CLUSTER_PEER_SCHEMA_VERSION,
        revision: applied.revision.0,
        replayed: applied.replayed,
        retained_receipts: u64::try_from(applied.retained_receipts)
            .map_err(|_| Status::resource_exhausted("retained receipt count exceeds u64"))?,
    }))
}

fn snapshot_status(error: AuthzRealmSnapshotError) -> Status {
    match error {
        AuthzRealmSnapshotError::Store(AuthzStoreError::Storage(_)) => {
            Status::internal(error.to_string())
        }
        AuthzRealmSnapshotError::TransferIntegrity(_) => Status::data_loss(error.to_string()),
        AuthzRealmSnapshotError::SnapshotConflict => Status::aborted(error.to_string()),
        _ => Status::failed_precondition(error.to_string()),
    }
}

struct RealmFrameWriter {
    sender: tokio::sync::mpsc::Sender<Result<wire::RealmAggregateFrame, Status>>,
    offset: u64,
}

impl RealmFrameWriter {
    fn new(sender: tokio::sync::mpsc::Sender<Result<wire::RealmAggregateFrame, Status>>) -> Self {
        Self { sender, offset: 0 }
    }

    fn finish(self) {
        let _ = self.sender.blocking_send(Ok(wire::RealmAggregateFrame {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            offset: self.offset,
            content: Vec::new(),
            end: true,
            manifest_json: Vec::new(),
        }));
    }

    fn fail(self, status: Status) {
        let _ = self.sender.blocking_send(Err(status));
    }
}

impl Write for RealmFrameWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        for content in bytes.chunks(REALM_FRAME_BYTES) {
            self.sender
                .blocking_send(Ok(wire::RealmAggregateFrame {
                    schema_version: CLUSTER_PEER_SCHEMA_VERSION,
                    offset: self.offset,
                    content: content.to_vec(),
                    end: false,
                    manifest_json: Vec::new(),
                }))
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "realm stream closed"))?;
            self.offset = self
                .offset
                .checked_add(content.len() as u64)
                .ok_or_else(|| io::Error::other("realm stream length overflow"))?;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct BlockingFrameReader {
    receiver: tokio::sync::mpsc::Receiver<Vec<u8>>,
    current: io::Cursor<Vec<u8>>,
}

impl BlockingFrameReader {
    fn new(receiver: tokio::sync::mpsc::Receiver<Vec<u8>>) -> Self {
        Self {
            receiver,
            current: io::Cursor::new(Vec::new()),
        }
    }
}

impl Read for BlockingFrameReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        loop {
            let read = self.current.read(output)?;
            if read != 0 {
                return Ok(read);
            }
            let Some(next) = self.receiver.blocking_recv() else {
                return Ok(0);
            };
            self.current = io::Cursor::new(next);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use keldra_consensus::ClusterId;

    use super::*;
    use crate::placement::PlacementNode;

    #[test]
    fn consistency_wire_requires_a_revision_only_for_bounded_modes() {
        assert_eq!(
            consistency_from_wire(
                wire::AuthorizationConsistencyMode::AuthorizationConsistencyLatest as i32,
                0,
            )
            .unwrap(),
            AuthzConsistency::Latest
        );
        assert_eq!(
            consistency_from_wire(
                wire::AuthorizationConsistencyMode::AuthorizationConsistencyExact as i32,
                7,
            )
            .unwrap(),
            AuthzConsistency::Exact(AuthzRevision(7))
        );
        assert!(
            consistency_from_wire(
                wire::AuthorizationConsistencyMode::AuthorizationConsistencyLatest as i32,
                7,
            )
            .is_err()
        );
        assert!(
            consistency_from_wire(
                wire::AuthorizationConsistencyMode::AuthorizationConsistencyAtLeast as i32,
                0,
            )
            .is_err()
        );
    }

    #[test]
    fn absent_install_is_one_terminal_header() {
        let valid = wire::RealmCandidateInstallFrame {
            end: true,
            ..Default::default()
        };
        assert!(require_absent_header(&valid).is_ok());

        let mut invalid = valid;
        invalid.content = vec![1];
        assert_eq!(
            require_absent_header(&invalid).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn realm_content_frames_are_contiguous_and_bounded() {
        let valid = wire::RealmCandidateInstallFrame {
            offset: 9,
            content: vec![1; REALM_FRAME_BYTES],
            ..Default::default()
        };
        assert!(require_content_frame(&valid, 9).is_ok());

        let mut wrong_offset = valid.clone();
        wrong_offset.offset = 8;
        assert!(require_content_frame(&wrong_offset, 9).is_err());

        let mut oversized = valid;
        oversized.content.push(1);
        assert!(require_content_frame(&oversized, 9).is_err());
    }

    #[test]
    fn reconciliation_reads_and_repairs_accept_any_selected_peer_only() {
        let nodes = [NodeId(1), NodeId(2), NodeId(3)]
            .into_iter()
            .map(|node_id| PlacementNode::new(node_id, NonZeroU32::new(1_000_000).unwrap()))
            .collect::<Vec<_>>();
        let group = MutableRecordReplicaGroup::select(
            PlacementKind::ZanzibarRealm,
            ClusterId([7; 16]),
            &1_u64.to_be_bytes(),
            &nodes,
        )
        .unwrap();
        let replicas = group.replicas();

        assert!(require_realm_reconciliation_members(&group, replicas[1], replicas[2]).is_ok());
        assert_eq!(
            require_realm_reconciliation_members(&group, NodeId(99), replicas[0])
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            require_realm_reconciliation_members(&group, replicas[0], NodeId(99))
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }
}
