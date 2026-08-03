//! Runtime renewal and public-request enforcement for the node-wide serving
//! fence. Grant validation and `CLOCK_BOOTTIME` arithmetic live in
//! `anvil-consensus`; this module only follows the applied leader/placement and
//! keeps one transient grant fresh.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use anvil_consensus::{
    ApplyResult, ClusterId, Command, DecisionRaft, NodeId, NodeState, PeerNode,
    SERVING_LEASE_RENEW_INTERVAL, ServingLeaseState, StateMachine, TonicPeerTransport,
};
use anvil_store::{ObjectMutationContext, PlacementLogId};
use anyhow::{Context, Result, bail};
use tonic::{Request, Status};

const READY_POLL_INTERVAL: Duration = Duration::from_millis(25);

#[derive(Clone)]
pub(crate) struct ServingAuthority {
    cluster_id: ClusterId,
    decisions: DecisionRaft,
    state: Arc<RwLock<ServingLeaseState>>,
}

pub(crate) struct ServingFenceRuntime {
    authority: ServingAuthority,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl ServingAuthority {
    fn new(cluster_id: ClusterId, decisions: DecisionRaft, state: ServingLeaseState) -> Self {
        Self {
            cluster_id,
            decisions,
            state: Arc::new(RwLock::new(state)),
        }
    }

    pub(crate) fn require<T>(&self, request: Request<T>) -> Result<Request<T>, Status> {
        if self.has_valid_lease() {
            Ok(request)
        } else {
            Err(Status::unavailable(
                "this node does not hold a current serving fence",
            ))
        }
    }

    pub(crate) fn has_valid_lease(&self) -> bool {
        self.mutation_context().is_ok()
    }

    /// Capture the exact placement and leader term authorizing a mutable
    /// coordinator operation. Callers carry this immutable value through the
    /// typed mutation; a later renewal cannot silently change its fence.
    pub(crate) fn mutation_context(&self) -> Result<ObjectMutationContext, Status> {
        let Ok(applied) = self.decisions.state() else {
            return Err(Status::unavailable(
                "the applied cluster state is unavailable",
            ));
        };
        if applied.cluster_id() != Some(self.cluster_id) {
            return Err(Status::unavailable(
                "the applied cluster identity does not match the serving authority",
            ));
        }
        let Some(placement) = applied.cluster_control().active_placement_log_id() else {
            return Err(Status::unavailable(
                "the active placement fence is unavailable",
            ));
        };
        let lease = {
            let mut state = self
                .state
                .write()
                .map_err(|_| Status::internal("serving-fence state lock is poisoned"))?;
            state.set_active_placement(placement);
            state.valid_lease().ok_or_else(|| {
                Status::unavailable("this node does not hold a current serving fence")
            })?
        };
        Ok(ObjectMutationContext {
            active_placement_log_id: PlacementLogId {
                term: placement.leader_id.term,
                index: placement.index,
            },
            serving_fence_term: lease.raft_term(),
        })
    }
}

impl ServingFenceRuntime {
    pub(crate) async fn start(
        decisions: DecisionRaft,
        transport: TonicPeerTransport,
        ready_timeout: Duration,
    ) -> Result<Self> {
        anyhow::ensure!(
            !ready_timeout.is_zero(),
            "serving-fence readiness timeout must be non-zero"
        );
        let state = decisions.state().context("read initial serving state")?;
        let cluster_id = state
            .cluster_id()
            .context("cluster identity is unavailable for serving")?;
        let placement = state
            .cluster_control()
            .active_placement_log_id()
            .context("active placement is unavailable for serving")?;
        let authority = ServingAuthority::new(
            cluster_id,
            decisions.clone(),
            ServingLeaseState::new(cluster_id, placement),
        );
        let renewal_authority = authority.clone();
        let renewal_decisions = decisions.clone();
        let task = tokio::spawn(async move {
            renewal_loop(renewal_decisions, transport, renewal_authority).await;
        });
        let mut runtime = Self {
            authority,
            task: Some(task),
        };
        if let Err(error) = runtime.wait_until_ready(ready_timeout).await {
            runtime.abort();
            return Err(error);
        }
        Ok(runtime)
    }

    pub(crate) fn authority(&self) -> ServingAuthority {
        self.authority.clone()
    }

    pub(crate) async fn shutdown(mut self) {
        let Some(task) = self.task.take() else {
            return;
        };
        task.abort();
        let _ = task.await;
    }

    async fn wait_until_ready(&self, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.authority.has_valid_lease() {
                return Ok(());
            }
            if self.task.as_ref().is_none_or(|task| task.is_finished()) {
                bail!("serving-fence renewal task stopped before readiness");
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("timed out waiting for the initial serving fence");
            }
            tokio::time::sleep(READY_POLL_INTERVAL).await;
        }
    }

    fn abort(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl Drop for ServingFenceRuntime {
    fn drop(&mut self) {
        self.abort();
    }
}

async fn renewal_loop(
    decisions: DecisionRaft,
    transport: TonicPeerTransport,
    authority: ServingAuthority,
) {
    let mut interval = tokio::time::interval(SERVING_LEASE_RENEW_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        if let Err(error) = renew_once(&decisions, &transport, &authority).await {
            tracing::debug!(%error, "serving-fence renewal did not produce a grant");
        }
    }
}

async fn renew_once(
    decisions: &DecisionRaft,
    transport: &TonicPeerTransport,
    authority: &ServingAuthority,
) -> Result<()> {
    let state = decisions.state().context("read applied serving state")?;
    let cluster_id = state
        .cluster_id()
        .context("applied state has no cluster identity")?;
    anyhow::ensure!(
        cluster_id == authority.cluster_id,
        "applied cluster identity changed while renewing the serving fence"
    );
    let placement = state
        .cluster_control()
        .active_placement_log_id()
        .context("applied state has no active placement fence")?;
    let leader = decisions
        .current_leader()
        .context("Raft has no current leader for serving-fence renewal")?;
    let descriptor = state
        .cluster_control()
        .nodes()
        .get(&NodeId(leader))
        .context("current Raft leader has no committed node descriptor")?;
    anyhow::ensure!(
        descriptor.state == NodeState::Active,
        "current Raft leader is not ACTIVE"
    );
    repair_executor_nomination(decisions, &state, NodeId(leader)).await?;
    let peer = PeerNode::new(descriptor.peer_address.0.clone());
    let pending = {
        let mut local = authority
            .state
            .write()
            .map_err(|_| anyhow::anyhow!("serving-fence state lock is poisoned"))?;
        local.set_active_placement(placement);
        local
            .begin_request()
            .context("capture serving-fence request start")?
    };
    let grant = transport
        .request_serving_lease(leader, &peer, pending.request())
        .await
        .context("request serving grant from current leader")?;
    authority
        .state
        .write()
        .map_err(|_| anyhow::anyhow!("serving-fence state lock is poisoned"))?
        .accept_grant(pending, grant)
        .context("accept current leader serving grant")?;
    Ok(())
}

/// Only the current ACTIVE leader repairs a missing or ineligible executor.
/// NominateExecutor is the existing durable fence; no separate election state
/// or timer is introduced.
async fn repair_executor_nomination(
    decisions: &DecisionRaft,
    state: &StateMachine,
    leader: NodeId,
) -> Result<()> {
    if executor_nominee(state, leader, NodeId(decisions.node_id())) != Some(leader) {
        return Ok(());
    }
    let applied = decisions
        .submit(Command::NominateExecutor { executor: leader })
        .await
        .context("repair atomic executor nomination")?;
    anyhow::ensure!(
        matches!(
            applied.result,
            ApplyResult::ExecutorNominated(nomination) if nomination.executor == leader
        ),
        "atomic executor nomination returned an unexpected result"
    );
    tracing::info!(
        node_id = leader.0,
        monotonic_counter.anvil_atomic_program_executor_nominations_total = 1_u64,
        "ACTIVE Raft leader repaired the atomic executor nomination"
    );
    Ok(())
}

fn executor_nominee(state: &StateMachine, leader: NodeId, local: NodeId) -> Option<NodeId> {
    if local != leader
        || state
            .cluster_control()
            .nodes()
            .get(&leader)
            .is_none_or(|descriptor| descriptor.state != NodeState::Active)
    {
        return None;
    }
    let current_is_active = state.executor().is_some_and(|nomination| {
        state
            .cluster_control()
            .nodes()
            .get(&nomination.executor)
            .is_some_and(|descriptor| descriptor.state == NodeState::Active)
    });
    (!current_is_active).then_some(leader)
}

#[cfg(test)]
mod tests {
    use anvil_consensus::{
        CLUSTER_CONTROL_COMMAND_VERSION, CapabilityRange, Command, JoinCapabilityHash,
        NodeDescriptor, PeerAddress, PeerSpkiSha256, ServingLeaseGrant,
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn public_authority_fails_closed_and_tracks_exact_placement() {
        let directory = tempfile::tempdir().unwrap();
        let raft = DecisionRaft::open(directory.path(), 1, 8, 64 * 1024)
            .await
            .unwrap();
        raft.ensure_one_node().await.unwrap();
        raft.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        let cluster_id = ClusterId([5; 16]);
        raft.submit(Command::InitializeCluster { cluster_id })
            .await
            .unwrap();
        let descriptor = NodeDescriptor {
            node_id: NodeId(1),
            peer_address: PeerAddress("anvil-local://1".into()),
            storage_weight_millionths: 1_000_000,
            state: NodeState::Joining,
            current_peer_spki_sha256: PeerSpkiSha256([1; 32]),
            overlap_peer_spki_sha256: None,
            join_capability_hash: Some(JoinCapabilityHash([2; 32])),
            supported_protocol: CapabilityRange { min: 1, max: 1 },
            supported_storage_format: CapabilityRange { min: 1, max: 1 },
        };
        let begun = raft
            .submit(Command::BeginAddNode {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                descriptor,
            })
            .await
            .unwrap();
        raft.submit(Command::CompleteMembershipTransition {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            started_log_index: begun.log_index,
        })
        .await
        .unwrap();
        raft.apply_fixed_voters_for_transition(begun.log_index)
            .await
            .unwrap();
        raft.submit(Command::CompleteMembershipTransition {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            started_log_index: begun.log_index,
        })
        .await
        .unwrap();
        let first = raft
            .state()
            .unwrap()
            .cluster_control()
            .active_placement_log_id()
            .unwrap();
        assert_eq!(
            executor_nominee(&raft.state().unwrap(), NodeId(1), NodeId(1)),
            Some(NodeId(1))
        );
        assert_eq!(
            executor_nominee(&raft.state().unwrap(), NodeId(1), NodeId(2)),
            None
        );
        raft.submit(Command::NominateExecutor {
            executor: NodeId(1),
        })
        .await
        .unwrap();
        assert_eq!(
            executor_nominee(&raft.state().unwrap(), NodeId(1), NodeId(1)),
            None
        );
        let state = ServingLeaseState::new(cluster_id, first);
        let authority = ServingAuthority::new(cluster_id, raft.clone(), state);
        assert_eq!(
            authority.require(Request::new(())).unwrap_err().code(),
            tonic::Code::Unavailable
        );

        let mut state = authority.state.write().unwrap();
        let pending = state.begin_request().unwrap();
        state
            .accept_grant(
                pending,
                ServingLeaseGrant {
                    cluster_id,
                    raft_term: 4,
                    active_placement_log_id: first,
                    maximum_local_lifetime: Duration::from_secs(1),
                },
            )
            .unwrap();
        drop(state);
        assert!(authority.require(Request::new(())).is_ok());
        assert_eq!(
            authority.mutation_context().unwrap(),
            ObjectMutationContext {
                active_placement_log_id: PlacementLogId {
                    term: first.leader_id.term,
                    index: first.index,
                },
                serving_fence_term: 4,
            }
        );

        let begun = raft
            .submit(Command::BeginReweightNode {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                node_id: NodeId(1),
                storage_weight_millionths: 2_000_000,
            })
            .await
            .unwrap();
        raft.submit(Command::CompleteMembershipTransition {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            started_log_index: begun.log_index,
        })
        .await
        .unwrap();
        let second = raft
            .state()
            .unwrap()
            .cluster_control()
            .active_placement_log_id()
            .unwrap();
        assert_ne!(first, second);
        assert!(!authority.has_valid_lease());
        raft.shutdown().await.unwrap();
    }
}
