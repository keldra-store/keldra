//! Transient node-wide serving leases for one exact committed placement.
//!
//! Leases never enter Raft. The leader reuses only a fresh linearizable quorum
//! proof, and each recipient measures expiry from its own request-start time.

use std::sync::Arc;
use std::time::Duration;

use openraft::LogId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ClusterId, DecisionRaft, DecisionRaftError, LeaderQuorumProof};

/// Fixed maximum authority granted by one leader response.
pub const SERVING_LEASE_MAX_LIFETIME: Duration = Duration::from_secs(2);

/// Normal interval between transient renewal requests.
pub const SERVING_LEASE_RENEW_INTERVAL: Duration = Duration::from_millis(500);

/// Membership cutover delay, including one second beyond lease expiry.
pub const SERVING_LEASE_CUTOVER_WAIT: Duration = Duration::from_secs(3);

/// The applied placement for which a recipient asks the leader for authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServingLeaseRequest {
    pub cluster_id: ClusterId,
    pub active_placement_log_id: LogId<u64>,
}

/// A transient leader grant. It is not a durable record or a Raft command.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServingLeaseGrant {
    pub cluster_id: ClusterId,
    pub raft_term: u64,
    pub active_placement_log_id: LogId<u64>,
    pub maximum_local_lifetime: Duration,
}

/// A request paired with the recipient's local pre-send timestamp.
///
/// Call [`ServingLeaseState::begin_request`] immediately before passing
/// [`Self::request`] to the peer transport. Any transport delay consumes the
/// grant's local lifetime.
#[derive(Debug, PartialEq, Eq)]
pub struct PendingServingLeaseRequest {
    request: ServingLeaseRequest,
    requested_at: BootTimeInstant,
}

impl PendingServingLeaseRequest {
    pub fn request(&self) -> ServingLeaseRequest {
        self.request
    }
}

/// One accepted, process-local serving lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServingLease {
    cluster_id: ClusterId,
    raft_term: u64,
    active_placement_log_id: LogId<u64>,
    expires_at: BootTimeInstant,
}

impl ServingLease {
    pub fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    pub fn raft_term(&self) -> u64 {
        self.raft_term
    }

    pub fn active_placement_log_id(&self) -> LogId<u64> {
        self.active_placement_log_id
    }

    pub fn remaining_lifetime(&self) -> Option<Duration> {
        let now = BootTimeInstant::now().ok()?;
        self.expires_at.0.checked_sub(now.0)
    }
}

/// Recipient-side authority state for one stable cluster identity.
#[derive(Clone, Debug)]
pub struct ServingLeaseState {
    cluster_id: ClusterId,
    active_placement_log_id: LogId<u64>,
    highest_raft_term: u64,
    current: Option<ServingLease>,
}

/// Leader-local cutover guard shared by every serving-grant RPC handler.
///
/// It is transient and bounded to one observed leader/placement identity. A
/// restart, leadership term change, or placement change conservatively starts
/// the fixed cutover wait again.
#[derive(Clone, Default)]
pub struct ServingLeaseIssuer {
    cutover: Arc<tokio::sync::Mutex<ServingLeaseCutover>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ServingLeaseIssuerIdentity {
    cluster_id: ClusterId,
    raft_term: u64,
    active_placement_log_id: LogId<u64>,
}

#[derive(Clone, Copy, Debug, Default)]
struct ServingLeaseCutover {
    observed: Option<(ServingLeaseIssuerIdentity, BootTimeInstant)>,
}

impl ServingLeaseState {
    pub fn new(cluster_id: ClusterId, active_placement_log_id: LogId<u64>) -> Self {
        Self {
            cluster_id,
            active_placement_log_id,
            highest_raft_term: active_placement_log_id.leader_id.term,
            current: None,
        }
    }

    /// Capture `CLOCK_BOOTTIME` immediately before a renewal is sent.
    pub fn begin_request(&self) -> Result<PendingServingLeaseRequest, ServingLeaseError> {
        Ok(PendingServingLeaseRequest {
            request: ServingLeaseRequest {
                cluster_id: self.cluster_id,
                active_placement_log_id: self.active_placement_log_id,
            },
            requested_at: BootTimeInstant::now()?,
        })
    }

    /// Invalidate old authority immediately after applying a new placement.
    pub fn set_active_placement(&mut self, active_placement_log_id: LogId<u64>) {
        if self.active_placement_log_id != active_placement_log_id {
            self.active_placement_log_id = active_placement_log_id;
            self.highest_raft_term = self
                .highest_raft_term
                .max(active_placement_log_id.leader_id.term);
            self.current = None;
        }
    }

    pub fn active_placement_log_id(&self) -> LogId<u64> {
        self.active_placement_log_id
    }

    pub fn highest_raft_term(&self) -> u64 {
        self.highest_raft_term
    }

    /// Validate and install a leader response, consuming its pending request.
    pub fn accept_grant(
        &mut self,
        pending: PendingServingLeaseRequest,
        grant: ServingLeaseGrant,
    ) -> Result<ServingLease, ServingLeaseError> {
        let expected_request = ServingLeaseRequest {
            cluster_id: self.cluster_id,
            active_placement_log_id: self.active_placement_log_id,
        };
        if pending.request != expected_request {
            return Err(ServingLeaseError::RequestSuperseded);
        }
        validate_identity(expected_request, grant)?;
        if grant.maximum_local_lifetime > SERVING_LEASE_MAX_LIFETIME {
            return Err(ServingLeaseError::GrantLifetimeTooLong {
                granted: grant.maximum_local_lifetime,
            });
        }
        if grant.raft_term < self.highest_raft_term {
            return Err(ServingLeaseError::RaftTermRegressed {
                highest: self.highest_raft_term,
                received: grant.raft_term,
            });
        }

        if self.highest_raft_term != grant.raft_term {
            self.highest_raft_term = grant.raft_term;
            self.current = None;
        }

        let expires_at = pending
            .requested_at
            .checked_add(grant.maximum_local_lifetime)
            .ok_or(ServingLeaseError::GrantArrivedAfterExpiry)?;
        if BootTimeInstant::now()? >= expires_at {
            return Err(ServingLeaseError::GrantArrivedAfterExpiry);
        }

        let candidate = ServingLease {
            cluster_id: grant.cluster_id,
            raft_term: grant.raft_term,
            active_placement_log_id: grant.active_placement_log_id,
            expires_at,
        };
        let accepted = match self.current {
            Some(current) if current.expires_at >= candidate.expires_at => current,
            _ => candidate,
        };
        self.current = Some(accepted);
        Ok(accepted)
    }

    /// Return current authority only while the exact local placement matches.
    pub fn valid_lease(&self) -> Option<ServingLease> {
        let lease = self.current?;
        let now = BootTimeInstant::now().ok()?;
        (lease.cluster_id == self.cluster_id
            && lease.active_placement_log_id == self.active_placement_log_id
            && now < lease.expires_at)
            .then_some(lease)
    }

    pub fn has_valid_lease(&self) -> bool {
        self.valid_lease().is_some()
    }
}

impl ServingLeaseIssuer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Obtain a quorum-backed grant and withhold it until the fixed cutover
    /// wait for this exact leader term and placement has elapsed.
    pub async fn grant(
        &self,
        raft: &DecisionRaft,
        request: ServingLeaseRequest,
    ) -> Result<ServingLeaseGrant, ServingLeaseError> {
        let grant = raft.grant_serving_lease(request).await?;
        let now = BootTimeInstant::now()?;
        let identity = ServingLeaseIssuerIdentity {
            cluster_id: grant.cluster_id,
            raft_term: grant.raft_term,
            active_placement_log_id: grant.active_placement_log_id,
        };
        if !self.cutover.lock().await.permits(identity, now)? {
            return Err(ServingLeaseError::CutoverInProgress);
        }
        Ok(grant)
    }
}

impl ServingLeaseCutover {
    fn permits(
        &mut self,
        identity: ServingLeaseIssuerIdentity,
        now: BootTimeInstant,
    ) -> Result<bool, ServingLeaseError> {
        match self.observed {
            Some((observed, ready_at)) if observed == identity => Ok(now >= ready_at),
            _ => {
                let ready_at = now
                    .checked_add(SERVING_LEASE_CUTOVER_WAIT)
                    .ok_or(ServingLeaseError::ClockRangeExceeded)?;
                self.observed = Some((identity, ready_at));
                Ok(false)
            }
        }
    }
}

impl DecisionRaft {
    /// Issue one transient grant from a fresh cached-or-renewed quorum proof.
    pub async fn grant_serving_lease(
        &self,
        request: ServingLeaseRequest,
    ) -> Result<ServingLeaseGrant, ServingLeaseError> {
        let proof = self.confirm_leadership().await?;
        let state = self.state()?;
        let cluster_id = state
            .cluster_id()
            .ok_or(ServingLeaseError::ClusterNotInitialized)?;
        let active_placement_log_id = state
            .cluster_control()
            .active_placement_log_id()
            .ok_or(ServingLeaseError::ActivePlacementUnavailable)?;
        validate_request(
            request,
            ServingLeaseRequest {
                cluster_id,
                active_placement_log_id,
            },
        )?;
        grant_from_proof(proof, cluster_id, active_placement_log_id)
    }
}

fn grant_from_proof(
    proof: LeaderQuorumProof,
    cluster_id: ClusterId,
    active_placement_log_id: LogId<u64>,
) -> Result<ServingLeaseGrant, ServingLeaseError> {
    if !proof.is_fresh() {
        return Err(ServingLeaseError::LeaderQuorumProofStale);
    }
    Ok(ServingLeaseGrant {
        cluster_id,
        raft_term: proof.raft_term,
        active_placement_log_id,
        maximum_local_lifetime: SERVING_LEASE_MAX_LIFETIME,
    })
}

fn validate_request(
    received: ServingLeaseRequest,
    expected: ServingLeaseRequest,
) -> Result<(), ServingLeaseError> {
    if received.cluster_id != expected.cluster_id {
        return Err(ServingLeaseError::ClusterMismatch {
            expected: expected.cluster_id,
            received: received.cluster_id,
        });
    }
    if received.active_placement_log_id != expected.active_placement_log_id {
        return Err(ServingLeaseError::ActivePlacementMismatch {
            expected: expected.active_placement_log_id,
            received: received.active_placement_log_id,
        });
    }
    Ok(())
}

fn validate_identity(
    expected: ServingLeaseRequest,
    grant: ServingLeaseGrant,
) -> Result<(), ServingLeaseError> {
    validate_request(
        ServingLeaseRequest {
            cluster_id: grant.cluster_id,
            active_placement_log_id: grant.active_placement_log_id,
        },
        expected,
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct BootTimeInstant(Duration);

impl BootTimeInstant {
    fn now() -> Result<Self, ServingLeaseError> {
        crate::raft::boot_time_now()
            .map(Self)
            .map_err(ServingLeaseError::Consensus)
    }

    fn checked_add(self, duration: Duration) -> Option<Self> {
        self.0.checked_add(duration).map(Self)
    }
}

#[derive(Debug, Error)]
pub enum ServingLeaseError {
    #[error(transparent)]
    Consensus(#[from] DecisionRaftError),
    #[error("the Raft state has no cluster identity")]
    ClusterNotInitialized,
    #[error("the Raft state has no active placement fence")]
    ActivePlacementUnavailable,
    #[error("serving lease cluster mismatch: expected {expected:?}, received {received:?}")]
    ClusterMismatch {
        expected: ClusterId,
        received: ClusterId,
    },
    #[error("serving lease placement mismatch: expected {expected:?}, received {received:?}")]
    ActivePlacementMismatch {
        expected: LogId<u64>,
        received: LogId<u64>,
    },
    #[error("serving lease grant exceeds the fixed two-second maximum: {granted:?}")]
    GrantLifetimeTooLong { granted: Duration },
    #[error("serving lease Raft term regressed from {highest} to {received}")]
    RaftTermRegressed { highest: u64, received: u64 },
    #[error("the leader quorum proof is older than 500 milliseconds")]
    LeaderQuorumProofStale,
    #[error("the serving lease response arrived after its local expiry")]
    GrantArrivedAfterExpiry,
    #[error("the serving lease request names an older applied placement")]
    RequestSuperseded,
    #[error("the fixed serving lease cutover wait is still in progress")]
    CutoverInProgress,
    #[error("serving lease boot-clock range is exhausted")]
    ClockRangeExceeded,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_quorum_proof_cannot_emit_a_grant() {
        let directory = tempfile::tempdir().unwrap();
        let raft = DecisionRaft::open(directory.path(), 1, 4, 64 * 1024)
            .await
            .unwrap();
        raft.ensure_one_node().await.unwrap();
        raft.wait_for_leader(Duration::from_secs(5)).await.unwrap();
        let proof = raft.confirm_leadership().await.unwrap();
        tokio::time::sleep(crate::LEADER_QUORUM_PROOF_MAX_AGE + Duration::from_millis(25)).await;

        assert!(matches!(
            grant_from_proof(
                proof,
                ClusterId([1; 16]),
                LogId::new(openraft::CommittedLeaderId::new(1, 1), 3),
            ),
            Err(ServingLeaseError::LeaderQuorumProofStale)
        ));
        raft.shutdown().await.unwrap();
    }

    #[test]
    fn issuer_waits_for_each_new_term_or_placement_exactly_once() {
        let cluster_id = ClusterId([4; 16]);
        let first = ServingLeaseIssuerIdentity {
            cluster_id,
            raft_term: 3,
            active_placement_log_id: LogId::new(openraft::CommittedLeaderId::new(2, 1), 7),
        };
        let second_term = ServingLeaseIssuerIdentity {
            raft_term: 4,
            ..first
        };
        let second_placement = ServingLeaseIssuerIdentity {
            active_placement_log_id: LogId::new(openraft::CommittedLeaderId::new(4, 1), 8),
            ..second_term
        };
        let start = BootTimeInstant(Duration::from_secs(10));
        let mut cutover = ServingLeaseCutover::default();

        assert!(!cutover.permits(first, start).unwrap());
        assert!(
            !cutover
                .permits(
                    first,
                    BootTimeInstant(start.0 + SERVING_LEASE_CUTOVER_WAIT - Duration::from_nanos(1))
                )
                .unwrap()
        );
        assert!(
            cutover
                .permits(first, BootTimeInstant(start.0 + SERVING_LEASE_CUTOVER_WAIT))
                .unwrap()
        );
        assert!(
            !cutover
                .permits(
                    second_term,
                    BootTimeInstant(start.0 + SERVING_LEASE_CUTOVER_WAIT)
                )
                .unwrap()
        );
        assert!(
            !cutover
                .permits(
                    second_placement,
                    BootTimeInstant(start.0 + SERVING_LEASE_CUTOVER_WAIT * 2),
                )
                .unwrap()
        );
    }
}
