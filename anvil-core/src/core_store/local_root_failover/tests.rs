use super::*;

fn replica(node_id: &str) -> LocalShardPlacement {
    LocalShardPlacement {
        node_id: node_id.to_string(),
        region_id: "r1".to_string(),
        cell_id: format!("cell-{node_id}"),
        failure_domain: format!("cell-{node_id}"),
        region_weight: 100,
        cell_weight: 100,
        public_api_addr: format!("http://{node_id}"),
        is_local: false,
    }
}

fn scope<'a>() -> RootFailoverVoteScope<'a> {
    RootFailoverVoteScope {
        root_key_hash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        current_generation: 7,
        current_root_hash: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        register_cohort_hash: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        failed_owner_node_id: "node-a",
        candidate_owner_node_id: "node-b",
        previous_owner_fence: 4,
        voter_node_id: "node-c",
    }
}

fn owner_failure_scope() -> RootOwnerFailureScope {
    RootOwnerFailureScope {
        register_ownership_set_hash:
            "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_string(),
        failed_owner_node_id: "node-a".to_string(),
        candidate_owner_node_id: "node-b".to_string(),
    }
}

fn owner_probe_evidence(
    probe_state: OwnerProbeState,
    failure_evidence: RootOwnerFailureEvidence,
) -> RootFailoverVoteEvidence {
    RootFailoverVoteEvidence::OwnerProbe {
        probe_state,
        failure_evidence,
    }
}

fn structurally_sign_vote(vote: &mut RootFailoverVoteRowProto) {
    vote.signed_payload_hash = root_failover_vote_payload_hash(vote);
    vote.voter_signature = vec![1];
}

#[test]
fn equal_peers_choose_one_deterministic_root_failover_candidate() {
    let mut replicas = vec![replica("node-c"), replica("node-a"), replica("node-b")];
    let first = failover_candidate("node-a", &replicas).unwrap().to_string();
    replicas.reverse();
    let second = failover_candidate("node-a", &replicas).unwrap();
    assert_eq!(first, second);
    assert_ne!(first, "node-a");
}

#[test]
fn canonical_settlement_retains_cohort_owner_or_uses_rendezvous_successor() {
    let mut replicas = vec![replica("node-c"), replica("node-a"), replica("node-b")];
    assert_eq!(
        canonical_settlement_owner_from_replicas("node-b", &replicas),
        Some("node-b")
    );

    let retired_owner = "local-control-node-1";
    let expected = failover_candidate(retired_owner, &replicas)
        .expect("canonical R3 cohort has a successor")
        .to_string();
    assert_eq!(
        canonical_settlement_owner_from_replicas(retired_owner, &replicas),
        Some(expected.as_str())
    );
    replicas.reverse();
    assert_eq!(
        canonical_settlement_owner_from_replicas(retired_owner, &replicas),
        Some(expected.as_str())
    );
}

#[test]
fn one_failed_owner_has_one_successor_for_every_root() {
    let replicas = vec![replica("node-a"), replica("node-b"), replica("node-c")];
    let successor = failover_candidate("node-a", &replicas).unwrap();
    assert_eq!(
        root_write_target_node("node-a", OwnerProbeState::Failed, &replicas),
        Some(successor)
    );
    assert_eq!(
        root_write_target_node("node-a", OwnerProbeState::Healthy, &replicas),
        Some("node-a")
    );
}

#[test]
fn newer_owner_observation_does_not_count_as_liveness_failure() {
    let observation = OwnerProbeObservation::Advanced {
        owner_node_id: "node-b".to_string(),
        generation: 8,
    };
    assert_eq!(observation.state(), OwnerProbeState::Healthy);
    assert_eq!(
        OwnerProbeObservation::Failed.state(),
        OwnerProbeState::Failed
    );
}

#[test]
fn voter_rejects_failover_observation_behind_its_current_head() {
    let current_head = CoreInternalRootAnchorRead {
        root_key_hash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .to_string(),
        generation: 8,
        root_anchor_record: Vec::new(),
        root_anchor_hash: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            .to_string(),
    };
    assert!(!failover_observation_matches_current_head(
        7,
        &current_head.root_anchor_hash,
        &current_head,
    ));
    assert!(failover_observation_matches_current_head(
        8,
        &current_head.root_anchor_hash,
        &current_head,
    ));
}

#[test]
fn failed_owner_does_not_block_surviving_register_voters() {
    let replicas = vec![replica("node-a"), replica("node-b"), replica("node-c")];
    let voters = failover_voter_replicas("node-a", &replicas);
    assert_eq!(
        voters
            .iter()
            .map(|replica| replica.node_id.as_str())
            .collect::<Vec<_>>(),
        vec!["node-b", "node-c"]
    );
}

#[test]
fn ordinary_owner_failover_still_requires_three_probes_spanning_timeout() {
    let mut tracker = RootOwnerFailureTracker::default();
    let first_evidence = tracker.observe(owner_failure_scope(), OwnerProbeState::Failed, 10);
    let first = next_failover_vote(
        None,
        scope(),
        owner_probe_evidence(OwnerProbeState::Failed, first_evidence),
        10,
    )
    .unwrap();
    assert_eq!(first.decision, "suspect");
    assert_eq!(first.reason_code, ROOT_FAILOVER_REASON_OWNER_UNREACHABLE);
    let second_at = 10 + ROOT_FAILOVER_TIMEOUT.as_nanos() as u64;
    let second_evidence =
        tracker.observe(owner_failure_scope(), OwnerProbeState::Failed, second_at);
    let second = next_failover_vote(
        Some(&first),
        scope(),
        owner_probe_evidence(OwnerProbeState::Failed, second_evidence),
        second_at,
    )
    .unwrap();
    assert_eq!(second.decision, "suspect");
    let third_evidence = tracker.observe(owner_failure_scope(), OwnerProbeState::Failed, second_at);
    let third = next_failover_vote(
        Some(&second),
        scope(),
        owner_probe_evidence(OwnerProbeState::Failed, third_evidence),
        second_at,
    )
    .unwrap();
    assert_eq!(third.decision, "grant");
    assert_eq!(third.reason_code, ROOT_FAILOVER_REASON_OWNER_UNREACHABLE);

    let mut signed = third.clone();
    structurally_sign_vote(&mut signed);
    validate_root_failover_vote(&signed).unwrap();

    let mut premature = signed;
    premature.last_probe_unix_nanos =
        premature.first_failed_probe_unix_nanos + ROOT_FAILOVER_TIMEOUT.as_nanos() as u64 - 1;
    structurally_sign_vote(&mut premature);
    assert!(validate_root_failover_vote(&premature).is_err());
}

#[test]
fn pre_activation_synthetic_owner_retirement_grants_without_probes() {
    let activated_at = 100;
    assert!(synthetic_control_owner_retired_by_activation(
        Some(activated_at),
        "local-control-node-1",
        activated_at - 1,
    ));
    assert!(!synthetic_control_owner_retired_by_activation(
        None,
        "local-control-node-1",
        activated_at - 1,
    ));
    assert!(!synthetic_control_owner_retired_by_activation(
        Some(activated_at),
        "node-a",
        activated_at - 1,
    ));
    assert!(!synthetic_control_owner_retired_by_activation(
        Some(activated_at),
        "local-control-node-1",
        activated_at,
    ));

    let mut synthetic_scope = scope();
    synthetic_scope.failed_owner_node_id = "local-control-node-1";
    let mut vote = next_failover_vote(
        None,
        synthetic_scope,
        RootFailoverVoteEvidence::SyntheticOwnerRetiredByCanonicalActivation,
        activated_at + 1,
    )
    .unwrap();
    assert_eq!(vote.decision, "grant");
    assert_eq!(
        vote.reason_code,
        ROOT_FAILOVER_REASON_SYNTHETIC_OWNER_RETIRED
    );
    assert_eq!(vote.failed_probe_count, 0);
    assert_eq!(vote.first_failed_probe_unix_nanos, 0);
    assert_eq!(vote.last_probe_unix_nanos, 0);
    assert!(!confirmed_owner_unreachable_evidence(&vote));

    structurally_sign_vote(&mut vote);
    validate_root_failover_vote(&vote).unwrap();

    vote.reason_code = ROOT_FAILOVER_REASON_OWNER_UNREACHABLE.to_string();
    structurally_sign_vote(&mut vote);
    assert!(validate_root_failover_vote(&vote).is_err());
}

#[test]
fn healthy_probe_resets_failover_suspicion() {
    let mut tracker = RootOwnerFailureTracker::default();
    let suspect_evidence = tracker.observe(owner_failure_scope(), OwnerProbeState::Failed, 10);
    let suspect = next_failover_vote(
        None,
        scope(),
        owner_probe_evidence(OwnerProbeState::Failed, suspect_evidence),
        10,
    )
    .unwrap();
    let healthy_evidence = tracker.observe(owner_failure_scope(), OwnerProbeState::Healthy, 20);
    let healthy = next_failover_vote(
        Some(&suspect),
        scope(),
        owner_probe_evidence(OwnerProbeState::Healthy, healthy_evidence),
        20,
    )
    .unwrap();
    assert_eq!(healthy.decision, "reject");
    assert_eq!(healthy.failed_probe_count, 0);
    let retried_evidence = tracker.observe(owner_failure_scope(), OwnerProbeState::Failed, 30);
    let retried = next_failover_vote(
        Some(&healthy),
        scope(),
        owner_probe_evidence(OwnerProbeState::Failed, retried_evidence),
        30,
    )
    .unwrap();
    assert_eq!(retried.failed_probe_count, 1);
}

#[test]
fn one_owner_failure_observation_applies_to_every_root_in_the_ownership_set() {
    let mut tracker = RootOwnerFailureTracker::default();
    let first_at = 10;
    tracker.observe(owner_failure_scope(), OwnerProbeState::Failed, first_at);
    let grant_at = first_at + ROOT_FAILOVER_TIMEOUT.as_nanos() as u64;
    tracker.observe(owner_failure_scope(), OwnerProbeState::Failed, grant_at);
    let evidence = tracker.observe(owner_failure_scope(), OwnerProbeState::Failed, grant_at);
    let second_root_scope = RootFailoverVoteScope {
        root_key_hash: "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        current_generation: 11,
        current_root_hash: "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        ..scope()
    };

    let vote = next_failover_vote(
        None,
        second_root_scope,
        owner_probe_evidence(OwnerProbeState::Failed, evidence),
        grant_at,
    )
    .unwrap();

    assert_eq!(vote.decision, "grant");
    assert_eq!(vote.failed_probe_count, ROOT_FAILOVER_PROBE_COUNT);
    assert_eq!(vote.first_failed_probe_unix_nanos, first_at);
}

#[test]
fn confirmed_owner_failure_is_reused_with_one_background_refresh() {
    let mut tracker = RootOwnerFailureTracker::default();
    let scope = owner_failure_scope();
    let first_at = 10;
    let grant_at = first_at + ROOT_FAILOVER_TIMEOUT.as_nanos() as u64;
    tracker.observe(scope.clone(), OwnerProbeState::Failed, first_at);
    tracker.observe(scope.clone(), OwnerProbeState::Failed, grant_at);
    tracker.observe(scope.clone(), OwnerProbeState::Failed, grant_at);

    let (evidence, refresh_started) = tracker
        .confirmed_evidence(&scope, grant_at + 1)
        .expect("confirmed failure evidence remains fresh");
    assert_eq!(evidence.failed_probe_count, ROOT_FAILOVER_PROBE_COUNT);
    assert!(refresh_started);
    assert!(
        !tracker
            .confirmed_evidence(&scope, grant_at + 2)
            .expect("confirmed evidence remains reusable")
            .1,
        "only one refresh probe may run for an ownership set"
    );
}

#[test]
fn confirmed_owner_failure_expires_or_is_cleared_by_health() {
    let mut tracker = RootOwnerFailureTracker::default();
    let scope = owner_failure_scope();
    let first_at = 10;
    let grant_at = first_at + ROOT_FAILOVER_TIMEOUT.as_nanos() as u64;
    tracker.observe(scope.clone(), OwnerProbeState::Failed, first_at);
    tracker.observe(scope.clone(), OwnerProbeState::Failed, grant_at);
    tracker.observe(scope.clone(), OwnerProbeState::Failed, grant_at);

    assert!(
        tracker
            .confirmed_evidence(
                &scope,
                grant_at + ROOT_FAILOVER_CONFIRMED_EVIDENCE_TTL.as_nanos() as u64 + 1,
            )
            .is_none(),
        "stale failure evidence must not grant another root failover"
    );

    tracker
        .confirmed_evidence(&scope, grant_at + 1)
        .expect("fresh evidence starts a refresh");
    tracker.finish_refresh(scope.clone(), OwnerProbeState::Healthy, grant_at + 2);
    assert!(tracker.confirmed_evidence(&scope, grant_at + 3).is_none());
}
