use std::time::Duration;

use keldra_consensus::{
    CLUSTER_CONTROL_COMMAND_VERSION, CapabilityRange, ClusterId, Command, DecisionRaft,
    JoinCapabilityHash, NodeDescriptor, NodeId, NodeState, PeerAddress, PeerSpkiSha256,
    SERVING_LEASE_CUTOVER_WAIT, SERVING_LEASE_MAX_LIFETIME, SERVING_LEASE_RENEW_INTERVAL,
    ServingLeaseError, ServingLeaseGrant, ServingLeaseRequest, ServingLeaseState,
};
use openraft::{CommittedLeaderId, LogId};

fn log_id(term: u64, index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(term, 1), index)
}

fn grant(
    cluster_id: ClusterId,
    raft_term: u64,
    active_placement_log_id: LogId<u64>,
    maximum_local_lifetime: Duration,
) -> ServingLeaseGrant {
    ServingLeaseGrant {
        cluster_id,
        raft_term,
        active_placement_log_id,
        maximum_local_lifetime,
    }
}

fn joining_descriptor() -> NodeDescriptor {
    NodeDescriptor {
        node_id: NodeId(1),
        peer_address: PeerAddress("keldra-local://1".into()),
        storage_weight_millionths: 1_000_000,
        state: NodeState::Joining,
        current_peer_spki_sha256: PeerSpkiSha256([1; 32]),
        overlap_peer_spki_sha256: None,
        join_capability_hash: Some(JoinCapabilityHash([2; 32])),
        supported_protocol: CapabilityRange { min: 1, max: 1 },
        supported_storage_format: CapabilityRange { min: 1, max: 1 },
    }
}

#[test]
fn serving_timing_constants_are_fixed_by_the_rfc() {
    assert_eq!(SERVING_LEASE_MAX_LIFETIME, Duration::from_secs(2));
    assert_eq!(SERVING_LEASE_RENEW_INTERVAL, Duration::from_millis(500));
    assert_eq!(SERVING_LEASE_CUTOVER_WAIT, Duration::from_secs(3));
}

#[test]
fn matching_grant_installs_only_for_its_exact_placement_and_term() {
    let cluster_id = ClusterId([7; 16]);
    let placement = log_id(3, 11);
    let mut state = ServingLeaseState::new(cluster_id, placement);
    let pending = state.begin_request().unwrap();
    let lease = state
        .accept_grant(
            pending,
            grant(cluster_id, 5, placement, Duration::from_millis(100)),
        )
        .unwrap();

    assert_eq!(lease.cluster_id(), cluster_id);
    assert_eq!(lease.raft_term(), 5);
    assert_eq!(lease.active_placement_log_id(), placement);
    assert!(lease.remaining_lifetime().is_some());
    assert!(state.has_valid_lease());
    assert_eq!(state.highest_raft_term(), 5);

    state.set_active_placement(log_id(4, 12));
    assert!(!state.has_valid_lease());
}

#[test]
fn recipient_rejects_identity_mismatch_overlong_grants_and_term_regression() {
    let cluster_id = ClusterId([8; 16]);
    let placement = log_id(6, 20);
    let mut state = ServingLeaseState::new(cluster_id, placement);

    let pending = state.begin_request().unwrap();
    assert!(matches!(
        state.accept_grant(
            pending,
            grant(ClusterId([9; 16]), 7, placement, SERVING_LEASE_MAX_LIFETIME,),
        ),
        Err(ServingLeaseError::ClusterMismatch { .. })
    ));

    let pending = state.begin_request().unwrap();
    assert!(matches!(
        state.accept_grant(
            pending,
            grant(cluster_id, 7, log_id(6, 21), SERVING_LEASE_MAX_LIFETIME),
        ),
        Err(ServingLeaseError::ActivePlacementMismatch { .. })
    ));

    let pending = state.begin_request().unwrap();
    assert!(matches!(
        state.accept_grant(
            pending,
            grant(
                cluster_id,
                7,
                placement,
                SERVING_LEASE_MAX_LIFETIME + Duration::from_nanos(1),
            ),
        ),
        Err(ServingLeaseError::GrantLifetimeTooLong { .. })
    ));

    let pending = state.begin_request().unwrap();
    state
        .accept_grant(
            pending,
            grant(cluster_id, 7, placement, Duration::from_millis(100)),
        )
        .unwrap();
    let pending = state.begin_request().unwrap();
    assert!(matches!(
        state.accept_grant(
            pending,
            grant(cluster_id, 6, placement, Duration::from_millis(100)),
        ),
        Err(ServingLeaseError::RaftTermRegressed {
            highest: 7,
            received: 6,
        })
    ));
}

#[test]
fn response_delay_consumes_lifetime_from_the_pre_send_timestamp() {
    let cluster_id = ClusterId([10; 16]);
    let placement = log_id(2, 4);
    let mut state = ServingLeaseState::new(cluster_id, placement);
    let pending = state.begin_request().unwrap();
    std::thread::sleep(Duration::from_millis(5));

    assert!(matches!(
        state.accept_grant(
            pending,
            grant(cluster_id, 3, placement, Duration::from_millis(1)),
        ),
        Err(ServingLeaseError::GrantArrivedAfterExpiry)
    ));
    assert!(!state.has_valid_lease());
    assert_eq!(state.highest_raft_term(), 3);
}

#[test]
fn response_to_a_superseded_placement_request_is_rejected() {
    let cluster_id = ClusterId([11; 16]);
    let first = log_id(4, 7);
    let second = log_id(5, 8);
    let mut state = ServingLeaseState::new(cluster_id, first);
    let pending = state.begin_request().unwrap();
    state.set_active_placement(second);

    assert!(matches!(
        state.accept_grant(
            pending,
            grant(cluster_id, 5, first, SERVING_LEASE_MAX_LIFETIME),
        ),
        Err(ServingLeaseError::RequestSuperseded)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_grants_only_its_applied_cluster_and_exact_placement() {
    let directory = tempfile::tempdir().unwrap();
    let raft = DecisionRaft::open(directory.path(), 1, 4, 64 * 1024)
        .await
        .unwrap();
    raft.ensure_one_node().await.unwrap();
    raft.wait_for_leader(Duration::from_secs(5)).await.unwrap();
    let cluster_id = ClusterId([12; 16]);
    raft.submit(Command::InitializeCluster { cluster_id })
        .await
        .unwrap();
    let add = raft
        .submit(Command::BeginAddNode {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            descriptor: joining_descriptor(),
        })
        .await
        .unwrap();
    raft.submit(Command::CompleteMembershipTransition {
        format_version: CLUSTER_CONTROL_COMMAND_VERSION,
        started_log_index: add.log_index,
    })
    .await
    .unwrap();
    let placement = raft
        .state()
        .unwrap()
        .cluster_control()
        .active_placement_log_id()
        .unwrap();
    let request = ServingLeaseRequest {
        cluster_id,
        active_placement_log_id: placement,
    };
    let issued = raft.grant_serving_lease(request).await.unwrap();
    assert_eq!(issued.cluster_id, cluster_id);
    assert_eq!(issued.active_placement_log_id, placement);
    assert_eq!(issued.maximum_local_lifetime, SERVING_LEASE_MAX_LIFETIME);

    assert!(matches!(
        raft.grant_serving_lease(ServingLeaseRequest {
            cluster_id: ClusterId([13; 16]),
            active_placement_log_id: placement,
        })
        .await,
        Err(ServingLeaseError::ClusterMismatch { .. })
    ));
    assert!(matches!(
        raft.grant_serving_lease(ServingLeaseRequest {
            cluster_id,
            active_placement_log_id: log_id(placement.leader_id.term, placement.index + 1),
        })
        .await,
        Err(ServingLeaseError::ActivePlacementMismatch { .. })
    ));

    assert!(!matches!(
        raft.grant_serving_lease(request).await,
        Err(ServingLeaseError::Consensus(_))
    ));
    raft.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_refuses_to_grant_before_cluster_and_placement_exist() {
    let directory = tempfile::tempdir().unwrap();
    let raft = DecisionRaft::open(directory.path(), 1, 4, 64 * 1024)
        .await
        .unwrap();
    raft.ensure_one_node().await.unwrap();
    raft.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    assert!(matches!(
        raft.grant_serving_lease(ServingLeaseRequest {
            cluster_id: ClusterId([1; 16]),
            active_placement_log_id: log_id(1, 1),
        })
        .await,
        Err(ServingLeaseError::ClusterNotInitialized)
    ));
    raft.shutdown().await.unwrap();
}
