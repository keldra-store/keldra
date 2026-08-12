//! Ephemeral mutation admission used only during membership cutover.
//!
//! The gate is process-local and deliberately has no durable state. An ADD
//! leader closes every old ACTIVE node over the mandatory-mTLS peer channel,
//! waits for admitted work to finish, commits the new placement, and then
//! explicitly releases the drain handles. Failed or cancelled handoffs remain
//! closed until the next authoritative retry or the Raft transition monitor
//! releases them.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use anvil_consensus::{DecisionRaft, MembershipTransitionKind, NodeState};
use tonic::Status;
use tonic::body::Body;
use tonic::codegen::Service;
use tonic::codegen::http::{Request, Response};
use tonic::server::NamedService;

const CLOSED_MESSAGE: &str = "mutable traffic is paused for membership cutover";

#[derive(Clone, Default)]
pub(crate) struct MutationAdmission {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    state: Mutex<State>,
    drained: tokio::sync::Notify,
}

#[derive(Default)]
struct State {
    drain: Option<DrainIdentity>,
    active: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DrainIdentity {
    pub(crate) joining_node_id: u64,
    pub(crate) started_log_index: u64,
}

pub(crate) struct MutationPermit {
    inner: Arc<Inner>,
}

pub(crate) struct MutationDrain {
    inner: Arc<Inner>,
    identity: DrainIdentity,
}

impl MutationDrain {
    pub(crate) fn release(self) {
        MutationAdmission {
            inner: self.inner.clone(),
        }
        .release(self.identity);
    }
}

impl MutationAdmission {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn new_closed(identity: DrainIdentity) -> Self {
        let admission = Self::new();
        admission.inner.state.lock().expect("new gate lock").drain = Some(identity);
        admission
    }

    pub(crate) fn enter(&self) -> Result<MutationPermit, Status> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| Status::internal("mutation-admission lock is poisoned"))?;
        if state.drain.is_some() {
            return Err(Status::unavailable(CLOSED_MESSAGE));
        }
        state.active = state
            .active
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("active mutation count overflow"))?;
        Ok(MutationPermit {
            inner: self.inner.clone(),
        })
    }

    /// Counts authenticated peer work that completes an origin mutation which
    /// was admitted before a membership drain began.
    ///
    /// Origin admission is closed during cutover, but rejecting its downstream
    /// replica apply would strand a partially written record. Continuations
    /// therefore remain admissible and are included in the same active count,
    /// so the drain cannot finish until they have also completed.
    pub(crate) fn enter_continuation(&self) -> Result<MutationPermit, Status> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| Status::internal("mutation-admission lock is poisoned"))?;
        state.active = state
            .active
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("active mutation count overflow"))?;
        Ok(MutationPermit {
            inner: self.inner.clone(),
        })
    }

    pub(crate) async fn drain(&self, identity: DrainIdentity) -> Result<MutationDrain, Status> {
        self.begin_close(identity)?;
        let drain = MutationDrain {
            inner: self.inner.clone(),
            identity,
        };
        self.wait_until_drained().await?;
        Ok(drain)
    }

    pub(crate) async fn close(&self, identity: DrainIdentity) -> Result<(), Status> {
        self.close_now(identity)?;
        self.wait_until_drained().await
    }

    pub(crate) fn close_now(&self, identity: DrainIdentity) -> Result<(), Status> {
        self.begin_close(identity)
    }

    fn begin_close(&self, identity: DrainIdentity) -> Result<(), Status> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| Status::internal("mutation-admission lock is poisoned"))?;
        if state.drain.is_some_and(|current| current != identity) {
            return Err(Status::failed_precondition(
                "another membership cutover already owns mutation admission",
            ));
        }
        state.drain = Some(identity);
        Ok(())
    }

    pub(crate) async fn wait_until_drained(&self) -> Result<(), Status> {
        loop {
            let notified = self.inner.drained.notified();
            if self
                .inner
                .state
                .lock()
                .map_err(|_| Status::internal("mutation-admission lock is poisoned"))?
                .active
                == 0
            {
                return Ok(());
            }
            notified.await;
        }
    }

    pub(crate) fn release(&self, identity: DrainIdentity) {
        if let Ok(mut state) = self.inner.state.lock()
            && state.drain == Some(identity)
        {
            state.drain = None;
        }
    }

    pub(crate) fn drain_identity(&self) -> Option<DrainIdentity> {
        self.inner.state.lock().ok()?.drain
    }

    pub(crate) fn monitor_add(&self, decisions: DecisionRaft, identity: DrainIdentity) {
        let admission = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let Ok(state) = decisions.state() else {
                    continue;
                };
                let still_pre_activation =
                    state
                        .cluster_control()
                        .transition()
                        .is_some_and(|transition| {
                            transition.kind == MembershipTransitionKind::Add
                                && transition.node_id.0 == identity.joining_node_id
                                && transition.started_log_index == identity.started_log_index
                                && state
                                    .cluster_control()
                                    .nodes()
                                    .get(&transition.node_id)
                                    .is_some_and(|descriptor| {
                                        descriptor.state == NodeState::Joining
                                    })
                        });
                if !still_pre_activation {
                    admission.release(identity);
                    return;
                }
            }
        });
    }
}

impl Drop for MutationPermit {
    fn drop(&mut self) {
        let Ok(mut state) = self.inner.state.lock() else {
            return;
        };
        debug_assert!(state.active > 0);
        state.active = state.active.saturating_sub(1);
        if state.active == 0 {
            self.inner.drained.notify_waiters();
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum AdmissionSurface {
    Public,
    ClusterPeer,
}

#[derive(Clone)]
pub(crate) struct MutationAdmissionService<S> {
    inner: S,
    admission: MutationAdmission,
    surface: AdmissionSurface,
}

impl<S> MutationAdmissionService<S> {
    pub(crate) fn new(inner: S, admission: MutationAdmission, surface: AdmissionSurface) -> Self {
        Self {
            inner,
            admission,
            surface,
        }
    }
}

impl<S> NamedService for MutationAdmissionService<S>
where
    S: NamedService,
{
    const NAME: &'static str = S::NAME;
}

type BoxResponseFuture<E> = Pin<Box<dyn Future<Output = Result<Response<Body>, E>> + Send>>;

impl<S, B> Service<Request<Body>> for MutationAdmissionService<S>
where
    S: Service<Request<Body>, Response = Response<B>> + Clone + Send + 'static,
    B: http_body_util::BodyExt<Data = tonic::codegen::Bytes> + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
    S::Error: Send + 'static,
    S::Future: Send + 'static,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = BoxResponseFuture<S::Error>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let is_mutation = match self.surface {
            AdmissionSurface::Public => is_public_mutation(request.uri().path()),
            AdmissionSurface::ClusterPeer => is_cluster_peer_mutation(request.uri().path()),
        };
        if !is_mutation {
            let future = self.inner.call(request);
            return Box::pin(async move { future.await.map(|response| response.map(Body::new)) });
        }
        let permit = match self.surface {
            AdmissionSurface::Public => self.admission.enter(),
            AdmissionSurface::ClusterPeer => self.admission.enter_continuation(),
        };
        let permit = match permit {
            Ok(permit) => permit,
            Err(status) => return Box::pin(async move { Ok(grpc_error(status)) }),
        };
        let future = self.inner.call(request);
        Box::pin(async move {
            let result = future.await.map(|response| response.map(Body::new));
            drop(permit);
            result
        })
    }
}

fn grpc_error(status: Status) -> Response<Body> {
    status.into_http()
}

fn method(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or_default()
}

fn is_public_mutation(path: &str) -> bool {
    matches!(
        method(path),
        "PutEnd"
            | "Delete"
            | "DeleteIfVersion"
            | "DeleteVersion"
            | "BulkWrite"
            | "SetBucketPolicy"
            | "InvokeProgram"
            | "PutSchema"
            | "BindSchema"
            | "MutateTuples"
            | "CreateIndex"
            | "UpdateIndex"
            | "RebuildIndex"
            | "DeleteIndex"
            | "EnableAccounting"
            | "DisableAccounting"
            | "PrepareNode"
            | "ProvisionTenant"
            | "CreateApplication"
            | "RotateApplicationCredential"
            | "DisableApplicationCredential"
            | "CreateBucket"
            | "SetBucketVersioning"
            | "SetBucketPublicRead"
            | "GrantApplicationRole"
            | "RevokeApplicationRole"
            | "CreateGroup"
            | "GrantGroupRole"
            | "RevokeGroupRole"
            | "AppendEntry"
            | "MaterializeProjection"
            | "RegisterSnapshot"
    )
}

fn is_cluster_peer_mutation(path: &str) -> bool {
    matches!(
        method(path),
        "RepairLogicalRecord"
            | "ApplyLogicalRecord"
            | "ApplySchemaPublication"
            | "ApplyRealmMutation"
            | "InstallRealmCandidate"
            | "RoutePutEnd"
            | "RouteDelete"
            | "RouteDeleteIfVersion"
            | "RouteBulkWrite"
            | "RouteInternalPutEnd"
            | "RouteInternalDeleteIfVersion"
            | "RouteInternalBulkWrite"
            | "RouteProvisionTenant"
            | "RouteCreateBucket"
            | "RouteAdminCreateApplication"
            | "RouteAdminRotateCredential"
            | "RouteAdminDisableCredential"
            | "RouteAdminSetBucketVersioning"
            | "RouteAdminSetBucketPublicRead"
            | "RouteAdminChangeApplicationRole"
            | "RouteInvokeProgram"
            | "RouteAuthzPutSchema"
            | "RouteAuthzBindSchema"
            | "RouteAuthzMutateTuples"
            | "RouteSetBucketPolicy"
            | "RouteDeleteVersion"
            | "StageProgramPath"
            | "CoordinateProgramPathFinalization"
            | "ApplyProgramPathFinalization"
            | "PublishIndexArtifact"
            | "DeleteIndexArtifact"
            | "RouteCreatePersonalDbGroup"
            | "RouteChangePersonalDbGroupRole"
            | "RouteAppendPersonalDbEntry"
            | "RouteMaterializePersonalDbProjection"
            | "RouteRegisterPersonalDbSnapshot"
            | "ApplyPersonalDbRole"
            | "RouteEnableAccounting"
            | "RouteDisableAccounting"
            | "FlushAccountingTraffic"
            | "ApplyDerivedConsumerCheckpoint"
            | "CoordinateLogicalRecord"
            | "CoordinateSystemGrant"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn close_rejects_new_work_and_waits_for_admitted_work() {
        let gate = MutationAdmission::new();
        let first = gate.enter().unwrap();
        let draining = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.drain(identity()).await.unwrap() })
        };
        tokio::task::yield_now().await;
        assert_eq!(
            gate.enter().err().expect("closed gate").code(),
            tonic::Code::Unavailable
        );
        assert!(!draining.is_finished());
        drop(first);
        let drain = draining.await.unwrap();
        assert_eq!(
            gate.enter().err().expect("closed gate").code(),
            tonic::Code::Unavailable
        );
        drain.release();
        drop(gate.enter().unwrap());
    }

    #[tokio::test]
    async fn cancelled_drain_remains_closed_until_authoritative_release() {
        let gate = MutationAdmission::new();
        let first = gate.enter().unwrap();
        let draining = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.drain(identity()).await })
        };
        tokio::task::yield_now().await;
        draining.abort();
        assert!(matches!(draining.await, Err(error) if error.is_cancelled()));
        drop(first);
        assert_eq!(
            gate.enter().err().expect("closed gate").code(),
            tonic::Code::Unavailable
        );
        gate.release(identity());
        drop(gate.enter().unwrap());
    }

    #[tokio::test]
    async fn stale_drain_drop_cannot_reopen_a_newer_cutover() {
        let gate = MutationAdmission::new();
        let old = gate.drain(identity()).await.unwrap();
        let newer = DrainIdentity {
            joining_node_id: 3,
            started_log_index: 29,
        };
        gate.inner.state.lock().unwrap().drain = Some(newer);
        drop(old);
        assert_eq!(gate.inner.state.lock().unwrap().drain, Some(newer));
        assert_eq!(
            gate.enter().err().expect("closed gate").code(),
            tonic::Code::Unavailable
        );
    }

    #[tokio::test]
    async fn cancelled_remote_close_remains_closed() {
        let gate = MutationAdmission::new();
        let first = gate.enter().unwrap();
        let closing = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.close(identity()).await })
        };
        tokio::task::yield_now().await;
        closing.abort();
        assert!(closing.await.unwrap_err().is_cancelled());
        drop(first);
        assert_eq!(
            gate.enter().err().expect("closed gate").code(),
            tonic::Code::Unavailable
        );
        gate.release(identity());
        drop(gate.enter().unwrap());
    }

    #[tokio::test]
    async fn same_cutover_can_have_multiple_drain_waiters() {
        let gate = MutationAdmission::new();
        let first = gate.enter().unwrap();
        gate.close_now(identity()).unwrap();
        let one = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.wait_until_drained().await })
        };
        let two = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.wait_until_drained().await })
        };
        tokio::task::yield_now().await;
        drop(first);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            one.await.unwrap().unwrap();
            two.await.unwrap().unwrap();
        })
        .await
        .expect("all same-cutover drain waiters should wake");
    }

    #[tokio::test]
    async fn closed_gate_counts_peer_continuations_until_they_finish() {
        let gate = MutationAdmission::new();
        gate.close_now(identity()).unwrap();
        assert_eq!(
            gate.enter().err().expect("closed origin gate").code(),
            tonic::Code::Unavailable
        );

        let continuation = gate.enter_continuation().unwrap();
        let drained = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.wait_until_drained().await })
        };
        tokio::task::yield_now().await;
        assert!(!drained.is_finished());

        drop(continuation);
        tokio::time::timeout(std::time::Duration::from_secs(1), drained)
            .await
            .expect("continuation should release the drain")
            .unwrap()
            .unwrap();
    }

    fn identity() -> DrainIdentity {
        DrainIdentity {
            joining_node_id: 2,
            started_log_index: 17,
        }
    }

    #[test]
    fn current_mutating_surfaces_are_classified() {
        assert!(is_public_mutation("/anvil.v1.ObjectService/PutEnd"));
        assert!(is_public_mutation("/anvil.v1.ObjectService/Delete"));
        assert!(is_public_mutation(
            "/anvil.v1.ObjectService/DeleteIfVersion"
        ));
        assert!(is_public_mutation("/anvil.v1.ObjectService/DeleteVersion"));
        assert!(is_public_mutation("/anvil.v1.ObjectService/BulkWrite"));
        assert!(is_public_mutation("/anvil.v1.AuthzService/MutateTuples"));
        assert!(is_public_mutation("/anvil.v1.IndexService/RebuildIndex"));
        assert!(!is_public_mutation("/anvil.v1.ObjectService/GetObject"));
        assert!(is_cluster_peer_mutation(
            "/anvil.cluster_peer.v1.ClusterPeer/RoutePutEnd"
        ));
        assert!(is_cluster_peer_mutation(
            "/anvil.cluster_peer.v1.ClusterPeer/RouteInternalBulkWrite"
        ));
        assert!(is_cluster_peer_mutation(
            "/anvil.cluster_peer.v1.ClusterPeer/RouteDeleteVersion"
        ));
        assert!(is_cluster_peer_mutation(
            "/anvil.cluster_peer.v1.ClusterPeer/RouteProvisionTenant"
        ));
        assert!(!is_cluster_peer_mutation(
            "/anvil.cluster_peer.v1.ClusterPeer/ReadLogicalRecord"
        ));
    }
}
