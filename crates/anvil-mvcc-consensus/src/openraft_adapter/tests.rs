use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use openraft::{CommittedLeaderId, Membership, RaftTypeConfig, storage::RaftLogStorage};
use tempfile::TempDir;

use super::*;
use crate::{
    BundleHash, DurabilityLevel, NodeId, NodeIncarnation, TransactionId,
    storage::fail_sync_write_at,
};

struct NoRemoteFactory;

impl ConsensusRpcFactory for NoRemoteFactory {
    fn client(&self, _target: NodeId, _node: &ConsensusNode) -> Box<dyn ConsensusRpcClient> {
        panic!("single-node runtime must not create a remote client")
    }
}

#[test]
fn application_payload_is_the_compact_certification_command() {
    fn assert_data_type<C: RaftTypeConfig<D = ConsensusCommand>>() {}
    fn assert_response_type<C: RaftTypeConfig<R = RaftApplyResult>>() {}
    assert_data_type::<AnvilRaftConfig>();
    assert_response_type::<AnvilRaftConfig>();
}

#[test]
fn control_entries_do_not_advance_the_product_bundle_catch_up_target() {
    let mut decisions = BTreeMap::from([(
        CommitVersion(7),
        Some(CommittedBundleDecision {
            cluster_id_hash: [1; 32],
            bundle_hash: BundleHash([2; 32]),
            bundle_length: 9,
            durability: DurabilityLevel::Quorum,
            durable_holders: Vec::new(),
        }),
    )]);
    decisions.insert(CommitVersion(8), None);
    decisions.insert(CommitVersion(9), None);

    assert_eq!(
        latest_committed_bundle_version(&decisions),
        CommitVersion(7)
    );
}

#[test]
fn production_timing_allows_networked_linearized_read_confirmation() {
    let config = production_raft_config("cluster-a".into());

    assert_eq!(
        (
            config.heartbeat_interval,
            config.election_timeout_min,
            config.election_timeout_max,
        ),
        (500, 1_500, 3_000)
    );
    assert!(config.election_timeout_min >= config.heartbeat_interval * 3);
    assert_eq!(config.max_payload_entries, 4);
    config.validate().expect("production Raft timing is valid");
}

#[test]
fn concrete_types_implement_openraft_v2_storage_contracts() {
    fn assert_log<T: RaftLogStorage<AnvilRaftConfig>>() {}
    fn assert_state<T: RaftStateMachine<AnvilRaftConfig>>() {}
    assert_log::<OpenRaftLogStore>();
    assert_state::<OpenRaftStateMachine>();

    let directory = TempDir::new().unwrap();
    let (log, state) = stores(RocksRaftStore::open(directory.path(), 0).unwrap(), [1; 32]).unwrap();
    drop((log, state));
}

#[test]
fn bundle_holders_must_intersect_every_regular_election_quorum() {
    let membership = StoredMembership::new(
        None,
        Membership::new(
            vec![BTreeSet::from([1, 2, 3, 4, 5])],
            BTreeSet::from([1, 2, 3, 4, 5, 9]),
        ),
    );
    assert!(holders_intersect_every_election_quorum(
        &membership,
        &BTreeSet::from([1, 2, 3]),
    ));
    assert!(!holders_intersect_every_election_quorum(
        &membership,
        &BTreeSet::from([1, 2, 9]),
    ));
}

#[test]
fn joint_membership_uses_voters_and_never_counts_arbitrary_learners() {
    let membership = StoredMembership::new(
        None,
        Membership::new(
            vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([3, 4, 5])],
            BTreeSet::from([1, 2, 3, 4, 5, 9, 10]),
        ),
    );
    assert!(holders_intersect_every_election_quorum(
        &membership,
        &BTreeSet::from([1, 2, 9]),
    ));
    assert!(!holders_intersect_every_election_quorum(
        &membership,
        &BTreeSet::from([1, 9, 10]),
    ));
}

#[tokio::test]
async fn upgrades_prevent_false_loss_and_incarnation_replacement_is_detected() {
    let directory = TempDir::new().unwrap();
    let store = RocksRaftStore::open(directory.path(), 0).unwrap();
    let (_, mut machine) = stores(store.clone(), [1; 32]).unwrap();
    let leader = CommittedLeaderId::new(1, 1);
    let holder = NodeIncarnation {
        node_id: NodeId(11),
        incarnation: 1,
    };
    let survivor = NodeIncarnation {
        node_id: NodeId(22),
        incarnation: 1,
    };
    machine
        .apply([
            Entry {
                log_id: LogId::new(leader, 1),
                payload: EntryPayload::Membership(Membership::new(
                    vec![BTreeSet::from([1, 2])],
                    (),
                )),
            },
            Entry {
                log_id: LogId::new(leader, 2),
                payload: EntryPayload::Normal(ConsensusCommand::InstallNode {
                    cluster_id_hash: [1; 32],
                    node: holder,
                    raft_node_id: NodeId(1),
                    failure_domain: "zone-a".into(),
                }),
            },
            Entry {
                log_id: LogId::new(leader, 3),
                payload: EntryPayload::Normal(ConsensusCommand::InstallNode {
                    cluster_id_hash: [1; 32],
                    node: survivor,
                    raft_node_id: NodeId(2),
                    failure_domain: "zone-b".into(),
                }),
            },
            Entry {
                log_id: LogId::new(leader, 4),
                payload: EntryPayload::Normal(ConsensusCommand::SetDurabilityPolicy {
                    cluster_id_hash: [1; 32],
                    generation: 1,
                    bundle_quorum_holders: 1,
                    tolerated_failure_domains: 0,
                }),
            },
        ])
        .await
        .unwrap();
    let mut command = test_command(7);
    command.durability = DurabilityLevel::Local;
    command.durable_holders = vec![holder];
    machine
        .apply([Entry {
            log_id: LogId::new(leader, 5),
            payload: EntryPayload::Normal(ConsensusCommand::Certify(command)),
        }])
        .await
        .unwrap();
    assert!(
        machine
            .store
            .read_state_value::<MachineState>(KEY_OPENRAFT_STATE)
            .unwrap()
            .unwrap()
            .local_durability_violations
            .is_empty()
    );

    machine
        .apply([
            Entry {
                log_id: LogId::new(leader, 6),
                payload: EntryPayload::Normal(ConsensusCommand::UpgradeDurability {
                    cluster_id_hash: [1; 32],
                    commit_version: CommitVersion(5),
                    bundle_hash: BundleHash([7; 32]),
                    durability: DurabilityLevel::Quorum,
                    durable_holders: vec![holder, survivor],
                }),
            },
            Entry {
                log_id: LogId::new(leader, 7),
                payload: EntryPayload::Membership(Membership::new(
                    vec![BTreeSet::from([2])],
                    BTreeSet::from([1, 2]),
                )),
            },
            Entry {
                log_id: LogId::new(leader, 8),
                payload: EntryPayload::Normal(ConsensusCommand::RemoveNode {
                    cluster_id_hash: [1; 32],
                    node: holder,
                }),
            },
        ])
        .await
        .unwrap();
    assert!(
        machine
            .store
            .read_state_value::<MachineState>(KEY_OPENRAFT_STATE)
            .unwrap()
            .unwrap()
            .local_durability_violations
            .is_empty()
    );

    let mut second = test_command(8);
    second.durability = DurabilityLevel::Local;
    second.durable_holders = vec![survivor];
    machine
        .apply([
            Entry {
                log_id: LogId::new(leader, 9),
                payload: EntryPayload::Normal(ConsensusCommand::Certify(second)),
            },
            Entry {
                log_id: LogId::new(leader, 10),
                payload: EntryPayload::Normal(ConsensusCommand::InstallNode {
                    cluster_id_hash: [1; 32],
                    node: NodeIncarnation {
                        node_id: survivor.node_id,
                        incarnation: 2,
                    },
                    raft_node_id: NodeId(2),
                    failure_domain: "zone-b".into(),
                }),
            },
        ])
        .await
        .unwrap();
    let state = machine
        .store
        .read_state_value::<MachineState>(KEY_OPENRAFT_STATE)
        .unwrap()
        .unwrap();
    assert_eq!(
        state
            .decisions
            .get(&CommitVersion(5))
            .and_then(Option::as_ref)
            .unwrap()
            .durability,
        DurabilityLevel::Quorum
    );
    let violations = state.local_durability_violations;
    assert_eq!(violations.len(), 1);
    assert_eq!(
        violations.get(&CommitVersion(9)),
        Some(&crate::LocalDurabilityViolation {
            commit_version: CommitVersion(9),
            bundle_hash: BundleHash([8; 32]),
            lost_holder: survivor,
            detected_at_log_index: 10,
        })
    );
}

#[test]
fn persisted_state_rejects_restart_under_another_cluster() {
    let directory = TempDir::new().unwrap();
    let store = RocksRaftStore::open(directory.path(), 7).unwrap();
    stores(store.clone(), [1; 32]).unwrap();

    let error = match stores(store, [2; 32]) {
        Ok(_) => panic!("cross-cluster restart was accepted"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("persisted Raft state belongs to another cluster")
    );
}

#[test]
fn restart_recovery_fault_is_retryable_without_changing_persisted_state() {
    let directory = TempDir::new().unwrap();
    let store = RocksRaftStore::open(directory.path(), 7).unwrap();
    stores(store.clone(), [1; 32]).unwrap();
    let before: MachineState = store.read_state_value(KEY_OPENRAFT_STATE).unwrap().unwrap();

    fail_next_restart_recovery();
    let error = match stores(store.clone(), [1; 32]) {
        Ok(_) => panic!("injected restart recovery unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("RestartRecovery"));

    stores(store.clone(), [1; 32]).unwrap();
    let after: MachineState = store.read_state_value(KEY_OPENRAFT_STATE).unwrap().unwrap();
    assert_eq!(after.last_applied_log_id, before.last_applied_log_id);
    assert_eq!(after.membership, before.membership);
    assert_eq!(
        after.certification.last_applied(),
        before.certification.last_applied()
    );
    assert_eq!(
        after.control.topology_epoch(),
        before.control.topology_epoch()
    );
}

#[test]
fn log_flushed_completion_follows_durable_success_and_reports_failed_write() {
    let directory = TempDir::new().unwrap();
    let store = RocksRaftStore::open(directory.path(), 7).unwrap();
    let log_store = OpenRaftLogStore {
        store: store.clone(),
    };

    let successful_completion = Arc::new(Mutex::new(None));
    let observed = successful_completion.clone();
    let callback_store = store.clone();
    log_store
        .append_durable_with_completion(&[(0, vec![1, 2, 3])], move |result| {
            assert_eq!(callback_store.get_log(0).unwrap(), Some(vec![1, 2, 3]));
            assert_eq!(callback_store.last_log_index().unwrap(), Some(0));
            *observed.lock().unwrap() = Some(result);
        })
        .unwrap();
    assert!(
        successful_completion
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .is_ok()
    );

    fail_sync_write_at(1);
    let failed_completion = Arc::new(Mutex::new(None));
    let observed = failed_completion.clone();
    let error = log_store
        .append_durable_with_completion(&[(1, vec![4, 5, 6])], move |result| {
            *observed.lock().unwrap() = Some(result);
        })
        .unwrap_err();
    assert!(error.to_string().contains("injected"));
    assert!(failed_completion.lock().unwrap().take().unwrap().is_err());
    assert_eq!(store.get_log(1).unwrap(), None);
    assert_eq!(store.last_log_index().unwrap(), Some(0));
}

#[tokio::test]
async fn failed_state_machine_write_preserves_state_and_last_applied_atomically() {
    let directory = TempDir::new().unwrap();
    let store = RocksRaftStore::open(directory.path(), 0).unwrap();
    let (_, mut machine) = stores(store.clone(), [3; 32]).unwrap();

    fail_sync_write_at(1);
    let owner = NodeIncarnation {
        node_id: NodeId(9),
        incarnation: 2,
    };
    let error = machine
        .apply([Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
            payload: EntryPayload::Normal(ConsensusCommand::InstallNode {
                cluster_id_hash: [3; 32],
                node: owner,
                raft_node_id: NodeId(8),
                failure_domain: "zone-a".into(),
            }),
        }])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("injected"));

    let persisted: MachineState = store.read_state_value(KEY_OPENRAFT_STATE).unwrap().unwrap();
    assert_eq!(persisted.last_applied_log_id, None);
    assert_eq!(persisted.control.node_incarnation(owner.node_id), None);
    assert!(persisted.decisions.is_empty());
}

#[tokio::test]
async fn snapshot_from_another_cluster_is_rejected() {
    let source_directory = TempDir::new().unwrap();
    let source_store = RocksRaftStore::open(source_directory.path(), 1).unwrap();
    stores(source_store.clone(), [2; 32]).unwrap();
    let mut builder = OpenRaftSnapshotBuilder {
        store: source_store,
        cluster_id_hash: [2; 32],
    };
    let snapshot = builder.build_snapshot().await.unwrap();

    let target_directory = TempDir::new().unwrap();
    let (_, mut target) = stores(
        RocksRaftStore::open(target_directory.path(), 1).unwrap(),
        [1; 32],
    )
    .unwrap();
    let error = target
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("configured cluster"));
}

#[tokio::test]
async fn control_state_survives_snapshot_install_atomically() {
    let source_directory = TempDir::new().unwrap();
    let source_store = RocksRaftStore::open(source_directory.path(), 1).unwrap();
    let (_, mut source) = stores(source_store.clone(), [4; 32]).unwrap();
    let owner = NodeIncarnation {
        node_id: NodeId(8),
        incarnation: 3,
    };
    let commands = [
        ConsensusCommand::InstallNode {
            cluster_id_hash: [4; 32],
            node: owner,
            raft_node_id: NodeId(8),
            failure_domain: "zone-a".into(),
        },
        ConsensusCommand::AssignPartition {
            cluster_id_hash: [4; 32],
            partition_id: 12,
            owner,
            epoch: 7,
        },
        ConsensusCommand::SetDurabilityPolicy {
            cluster_id_hash: [4; 32],
            generation: 5,
            bundle_quorum_holders: 3,
            tolerated_failure_domains: 1,
        },
        ConsensusCommand::AdvanceGcWatermark {
            cluster_id_hash: [4; 32],
            watermark: CommitVersion(4),
        },
    ];
    source
        .apply(
            commands
                .into_iter()
                .enumerate()
                .map(|(offset, command)| Entry {
                    log_id: LogId::new(CommittedLeaderId::new(1, 1), offset as u64 + 1),
                    payload: EntryPayload::Normal(command),
                }),
        )
        .await
        .unwrap();
    let mut builder = source.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await.unwrap();

    let target_directory = TempDir::new().unwrap();
    let target_store = RocksRaftStore::open(target_directory.path(), 1).unwrap();
    let (_, mut target) = stores(target_store.clone(), [4; 32]).unwrap();
    target
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    let restored: MachineState = target_store
        .read_state_value(KEY_OPENRAFT_STATE)
        .unwrap()
        .unwrap();
    assert_eq!(restored.control.node_incarnation(NodeId(8)), Some(3));
    assert_eq!(restored.control.partition(12).unwrap().epoch, 7);
    assert_eq!(restored.control.durability_policy().generation, 5);
    assert_eq!(restored.control.gc_safety_watermark(), CommitVersion(4));
}

#[tokio::test]
async fn gc_watermark_cannot_jump_beyond_its_consensus_position() {
    let directory = TempDir::new().unwrap();
    let store = RocksRaftStore::open(directory.path(), 0).unwrap();
    let (_, mut machine) = stores(store.clone(), [1; 32]).unwrap();
    let responses = machine
        .apply([Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), 7),
            payload: EntryPayload::Normal(ConsensusCommand::AdvanceGcWatermark {
                cluster_id_hash: [1; 32],
                watermark: CommitVersion(8),
            }),
        }])
        .await
        .unwrap();

    assert!(matches!(
        responses.as_slice(),
        [RaftApplyResult::Rejected(reason)] if reason.contains("consensus position")
    ));
    let state: MachineState = store.read_state_value(KEY_OPENRAFT_STATE).unwrap().unwrap();
    assert_eq!(state.control.gc_safety_watermark(), CommitVersion(0));
}

#[tokio::test]
async fn duplicate_transaction_advances_raft_applied_id_without_republishing_bundle() {
    let directory = TempDir::new().unwrap();
    let store = RocksRaftStore::open(directory.path(), 0).unwrap();
    let (_, mut machine) = stores(store.clone(), [1; 32]).unwrap();
    let command = CertifyTransaction {
        cluster_id_hash: [1; 32],
        transaction_id: TransactionId([1; 16]),
        principal_hash: [2; 32],
        snapshot_version: CommitVersion(0),
        point_observations: vec![],
        range_observations: vec![],
        predicates: vec![],
        assignment_predicates: vec![],
        written_point_keys: vec![],
        written_points: vec![],
        advanced_range_stamps: vec![],
        bundle_hash: BundleHash([2; 32]),
        bundle_length: 1,
        durability: DurabilityLevel::Local,
        durable_holders: vec![NodeIncarnation {
            node_id: NodeId(1),
            incarnation: 1,
        }],
    };
    machine
        .apply([
            Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
                payload: EntryPayload::Normal(ConsensusCommand::InstallNode {
                    cluster_id_hash: [1; 32],
                    node: NodeIncarnation {
                        node_id: NodeId(1),
                        incarnation: 1,
                    },
                    raft_node_id: NodeId(1),
                    failure_domain: "zone-a".into(),
                }),
            },
            Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), 2),
                payload: EntryPayload::Normal(ConsensusCommand::SetDurabilityPolicy {
                    cluster_id_hash: [1; 32],
                    generation: 1,
                    bundle_quorum_holders: 1,
                    tolerated_failure_domains: 0,
                }),
            },
        ])
        .await
        .unwrap();
    let log_id_1 = LogId::new(CommittedLeaderId::new(1, 1), 3);
    let log_id_9 = LogId::new(CommittedLeaderId::new(1, 1), 9);
    let first = machine
        .apply([Entry {
            log_id: log_id_1,
            payload: EntryPayload::Normal(ConsensusCommand::Certify(command.clone())),
        }])
        .await
        .unwrap();
    machine
        .apply([Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), 4),
            payload: EntryPayload::Normal(ConsensusCommand::RemoveNode {
                cluster_id_hash: [1; 32],
                node: NodeIncarnation {
                    node_id: NodeId(1),
                    incarnation: 1,
                },
            }),
        }])
        .await
        .unwrap();
    let retry = machine
        .apply([Entry {
            log_id: log_id_9,
            payload: EntryPayload::Normal(ConsensusCommand::Certify(command)),
        }])
        .await
        .unwrap();
    assert_eq!(first, retry);
    assert!(matches!(
        retry.as_slice(),
        [RaftApplyResult::Certification(
            CertificationResult::Committed {
                commit_version: CommitVersion(3),
                ..
            }
        )]
    ));
    assert_eq!(machine.applied_state().await.unwrap().0, Some(log_id_9));
    let state: MachineState = store.read_state_value(KEY_OPENRAFT_STATE).unwrap().unwrap();
    assert!(matches!(
        state.decisions.get(&CommitVersion(3)),
        Some(Some(decision))
            if decision.bundle_hash == BundleHash([2; 32])
                && decision.bundle_length == 1
    ));
    assert_eq!(state.decisions.get(&CommitVersion(9)), Some(&None));
}

#[tokio::test]
async fn unrelated_partition_topology_change_preserves_assignment_predicate() {
    let directory = TempDir::new().unwrap();
    let store = RocksRaftStore::open(directory.path(), 0).unwrap();
    let (_, mut machine) = stores(store.clone(), [1; 32]).unwrap();
    let owner = NodeIncarnation {
        node_id: NodeId(1),
        incarnation: 1,
    };
    machine
        .apply([
            Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
                payload: EntryPayload::Normal(ConsensusCommand::InstallNode {
                    cluster_id_hash: [1; 32],
                    node: owner,
                    raft_node_id: NodeId(1),
                    failure_domain: "zone-a".into(),
                }),
            },
            Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), 2),
                payload: EntryPayload::Normal(ConsensusCommand::SetDurabilityPolicy {
                    cluster_id_hash: [1; 32],
                    generation: 1,
                    bundle_quorum_holders: 1,
                    tolerated_failure_domains: 0,
                }),
            },
            Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), 3),
                payload: EntryPayload::Normal(ConsensusCommand::AssignPartition {
                    cluster_id_hash: [1; 32],
                    partition_id: 7,
                    owner,
                    epoch: 1,
                }),
            },
            Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), 4),
                payload: EntryPayload::Normal(ConsensusCommand::AssignPartition {
                    cluster_id_hash: [1; 32],
                    partition_id: 8,
                    owner,
                    epoch: 1,
                }),
            },
        ])
        .await
        .unwrap();
    let before: MachineState = store.read_state_value(KEY_OPENRAFT_STATE).unwrap().unwrap();
    assert_eq!(before.control.topology_epoch(), 4);
    assert_eq!(
        before.control.partition(7),
        Some(&crate::PartitionAssignment { owner, epoch: 1 })
    );

    let result = machine
        .apply([Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), 5),
            payload: EntryPayload::Normal(ConsensusCommand::Certify(CertifyTransaction {
                cluster_id_hash: [1; 32],
                transaction_id: TransactionId([11; 16]),
                principal_hash: [2; 32],
                snapshot_version: CommitVersion(0),
                point_observations: vec![],
                range_observations: vec![],
                predicates: vec![],
                assignment_predicates: vec![crate::AssignmentPredicate {
                    partition_id: 7,
                    owner,
                    assignment_epoch: 1,
                    topology_epoch: 3,
                }],
                written_point_keys: vec![],
                written_points: vec![],
                advanced_range_stamps: vec![],
                bundle_hash: BundleHash([11; 32]),
                bundle_length: 1,
                durability: DurabilityLevel::Local,
                durable_holders: vec![owner],
            })),
        }])
        .await
        .unwrap();

    assert!(matches!(
        result.as_slice(),
        [RaftApplyResult::Certification(
            CertificationResult::Committed {
                commit_version: CommitVersion(5),
                ..
            }
        )]
    ));
}

#[tokio::test]
async fn stale_assignment_predicate_is_a_stable_abort_not_a_rejected_raft_entry() {
    let directory = TempDir::new().unwrap();
    let store = RocksRaftStore::open(directory.path(), 0).unwrap();
    let (_, mut machine) = stores(store.clone(), [1; 32]).unwrap();
    let owner = NodeIncarnation {
        node_id: NodeId(1),
        incarnation: 1,
    };
    machine
        .apply([
            Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
                payload: EntryPayload::Normal(ConsensusCommand::InstallNode {
                    cluster_id_hash: [1; 32],
                    node: owner,
                    raft_node_id: NodeId(1),
                    failure_domain: "zone-a".into(),
                }),
            },
            Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), 2),
                payload: EntryPayload::Normal(ConsensusCommand::SetDurabilityPolicy {
                    cluster_id_hash: [1; 32],
                    generation: 1,
                    bundle_quorum_holders: 1,
                    tolerated_failure_domains: 0,
                }),
            },
            Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), 3),
                payload: EntryPayload::Normal(ConsensusCommand::AssignPartition {
                    cluster_id_hash: [1; 32],
                    partition_id: 7,
                    owner,
                    epoch: 1,
                }),
            },
            Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), 4),
                payload: EntryPayload::Normal(ConsensusCommand::AssignPartition {
                    cluster_id_hash: [1; 32],
                    partition_id: 7,
                    owner,
                    epoch: 2,
                }),
            },
        ])
        .await
        .unwrap();
    let command = CertifyTransaction {
        cluster_id_hash: [1; 32],
        transaction_id: TransactionId([9; 16]),
        principal_hash: [2; 32],
        snapshot_version: CommitVersion(0),
        point_observations: vec![],
        range_observations: vec![],
        predicates: vec![],
        assignment_predicates: vec![crate::AssignmentPredicate {
            partition_id: 7,
            owner,
            assignment_epoch: 1,
            topology_epoch: 3,
        }],
        written_point_keys: vec![],
        written_points: vec![],
        advanced_range_stamps: vec![],
        bundle_hash: BundleHash([8; 32]),
        bundle_length: 1,
        durability: DurabilityLevel::Local,
        durable_holders: vec![owner],
    };

    let first = machine
        .apply([Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), 5),
            payload: EntryPayload::Normal(ConsensusCommand::Certify(command.clone())),
        }])
        .await
        .unwrap();
    assert!(matches!(
        first.as_slice(),
        [RaftApplyResult::Certification(
            CertificationResult::Aborted {
                at_version: CommitVersion(5),
                reason: crate::CertificationAbort::AssignmentConflict {
                    partition_id: 7,
                    expected_epoch: CommitVersion(1),
                    actual_epoch: Some(CommitVersion(2)),
                    ..
                },
                ..
            }
        )]
    ));

    let retry = machine
        .apply([Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), 6),
            payload: EntryPayload::Normal(ConsensusCommand::Certify(command.clone())),
        }])
        .await
        .unwrap();
    assert_eq!(retry, first);
    let state: MachineState = store.read_state_value(KEY_OPENRAFT_STATE).unwrap().unwrap();
    assert_eq!(state.decisions.get(&CommitVersion(5)), Some(&None));
    assert_eq!(state.decisions.get(&CommitVersion(6)), Some(&None));

    let mut malformed = command;
    malformed.transaction_id = TransactionId([10; 16]);
    malformed.bundle_hash = BundleHash([10; 32]);
    malformed.assignment_predicates[0].topology_epoch = 0;
    let malformed_result = machine
        .apply([Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), 7),
            payload: EntryPayload::Normal(ConsensusCommand::Certify(malformed)),
        }])
        .await
        .unwrap();
    assert!(matches!(
        malformed_result.as_slice(),
        [RaftApplyResult::Certification(
            CertificationResult::Aborted {
                at_version: CommitVersion(7),
                reason: crate::CertificationAbort::InvalidCommand(reason),
                ..
            }
        )] if reason.contains("non-zero exact authority")
    ));
}

#[tokio::test]
async fn single_node_runtime_initializes_certifies_and_linearizes() {
    let directory = TempDir::new().unwrap();
    let runtime = OpenRaftConsensus::new(
        NodeId(1),
        RocksRaftStore::open(directory.path(), 0).unwrap(),
        [1; 32],
        "test-cluster",
        Arc::new(NoRemoteFactory),
    )
    .await
    .unwrap();
    runtime
        .initialize(BTreeMap::from([(
            NodeId(1),
            ConsensusNode {
                address: "in-process".into(),
            },
        )]))
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while runtime.raft.metrics().borrow().current_leader != Some(1) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "single node did not elect itself"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(matches!(
        runtime
            .install_node(
                [1; 32],
                NodeIncarnation {
                    node_id: NodeId(1),
                    incarnation: 1,
                },
                NodeId(1),
                "zone-a".into(),
            )
            .await
            .unwrap(),
        ControlApplyResult::NodeInstalled(_)
    ));
    assert!(matches!(
        runtime
            .set_durability_policy([1; 32], 1, 1, 0)
            .await
            .unwrap(),
        ControlApplyResult::DurabilityPolicySet(_)
    ));
    let command = test_command(8);
    let (first, concurrent_retry) = tokio::join!(
        runtime.certify(command.clone()),
        runtime.certify(command.clone())
    );
    let first = first.unwrap();
    assert_eq!(concurrent_retry.unwrap(), first);
    let committed = match &first {
        CertificationResult::Committed { commit_version, .. } => *commit_version,
        other => panic!("unexpected result: {other:?}"),
    };
    assert_eq!(
        runtime.observed_commit_version(),
        committed,
        "concurrent retry must not allocate another Raft position"
    );
    let outcome = runtime
        .linearized_transaction_outcome(command.transaction_id)
        .await
        .unwrap()
        .expect("certified transaction must have a retained outcome");
    assert_eq!(outcome.principal_hash, command.principal_hash);
    assert_eq!(outcome.snapshot_version, command.snapshot_version);
    assert_eq!(outcome.durability, command.durability);
    assert_eq!(outcome.result, first);

    let mut mismatched = command;
    mismatched.bundle_hash = BundleHash([9; 32]);
    let cursor_before_mismatch = runtime.observed_commit_version();
    assert!(matches!(
        runtime.certify(mismatched).await,
        Err(ConsensusError::Rejected(reason))
            if reason == CertificationError::TransactionIdentityMismatch.to_string()
    ));
    assert_eq!(
        runtime.observed_commit_version(),
        cursor_before_mismatch,
        "bundle identity mismatch must be rejected before Raft"
    );
    assert_eq!(
        runtime.linearized_read_barrier().await.unwrap(),
        runtime.observed_commit_version()
    );
    assert!(runtime.observed_commit_version() >= committed);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn automatic_snapshot_purges_logs_and_restart_keeps_certification_state() {
    let directory = TempDir::new().unwrap();
    let store = RocksRaftStore::open(directory.path(), 0).unwrap();
    let runtime = OpenRaftConsensus::new_with_config(
        NodeId(1),
        store.clone(),
        [1; 32],
        openraft::Config {
            cluster_name: "snapshot-purge-test".into(),
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(4),
            max_in_snapshot_log_to_keep: 1,
            replication_lag_threshold: 20,
            purge_batch_size: 1,
            ..Default::default()
        },
        Arc::new(NoRemoteFactory),
    )
    .await
    .unwrap();
    runtime
        .initialize(BTreeMap::from([(
            NodeId(1),
            ConsensusNode {
                address: "in-process".into(),
            },
        )]))
        .await
        .unwrap();
    wait_for_single_node_leader(&runtime).await;
    runtime
        .install_node(
            [1; 32],
            NodeIncarnation {
                node_id: NodeId(1),
                incarnation: 1,
            },
            NodeId(1),
            "zone-a".into(),
        )
        .await
        .unwrap();
    runtime
        .set_durability_policy([1; 32], 1, 1, 0)
        .await
        .unwrap();

    let mut retained = None;
    for id in 20..36 {
        let result = runtime.certify(test_command(id)).await.unwrap();
        if id == 25 {
            retained = Some((id, result));
        }
    }
    let (retained_id, retained_result) = retained.unwrap();
    let retained_version = match &retained_result {
        CertificationResult::Committed { commit_version, .. } => *commit_version,
        other => panic!("unexpected result: {other:?}"),
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if store
            .last_purged_index()
            .unwrap()
            .is_some_and(|purged| purged >= retained_version.0)
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "OpenRaft did not snapshot and purge the covered log"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        store.get_log(0).unwrap().is_none(),
        "purged entries must be absent from RocksDB"
    );
    runtime.shutdown().await.unwrap();

    let restarted = OpenRaftConsensus::new(
        NodeId(1),
        store,
        [1; 32],
        "snapshot-purge-test",
        Arc::new(NoRemoteFactory),
    )
    .await
    .unwrap();
    wait_for_single_node_leader(&restarted).await;
    let cursor_before_retry = restarted.observed_commit_version();
    assert_eq!(
        restarted.certify(test_command(retained_id)).await.unwrap(),
        retained_result,
        "certification retry state must survive snapshot-backed log purge"
    );
    assert_eq!(
        restarted.observed_commit_version(),
        cursor_before_retry,
        "restart retry must not allocate another Raft position"
    );
    restarted.shutdown().await.unwrap();
}

async fn wait_for_single_node_leader(runtime: &OpenRaftConsensus) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while runtime.raft.metrics().borrow().current_leader != Some(1) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "single node did not elect itself"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn test_command(id: u8) -> CertifyTransaction {
    CertifyTransaction {
        cluster_id_hash: [1; 32],
        transaction_id: TransactionId([id; 16]),
        principal_hash: [2; 32],
        snapshot_version: CommitVersion(0),
        point_observations: vec![],
        range_observations: vec![],
        predicates: vec![],
        assignment_predicates: vec![],
        written_point_keys: vec![],
        written_points: vec![],
        advanced_range_stamps: vec![],
        bundle_hash: BundleHash([id; 32]),
        bundle_length: 1,
        durability: DurabilityLevel::Local,
        durable_holders: vec![NodeIncarnation {
            node_id: NodeId(1),
            incarnation: 1,
        }],
    }
}
