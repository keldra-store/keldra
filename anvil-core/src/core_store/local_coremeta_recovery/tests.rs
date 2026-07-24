use super::*;

#[tokio::test]
async fn readiness_waiter_blocks_until_recovery_is_ready() {
    let directory = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(directory.path()).await.unwrap())
        .await
        .unwrap();
    store.set_coremeta_recovery_required(true);

    assert!(
        tokio::time::timeout(
            Duration::from_millis(25),
            store.wait_for_coremeta_recovery_ready(),
        )
        .await
        .is_err()
    );

    store.set_coremeta_recovery_required(false);
    tokio::time::timeout(
        Duration::from_secs(1),
        store.wait_for_coremeta_recovery_ready(),
    )
    .await
    .expect("readiness waiter did not observe the ready transition");
}

#[tokio::test]
async fn marking_recovery_unready_wakes_the_steady_state_loop() {
    let directory = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(directory.path()).await.unwrap())
        .await
        .unwrap();
    store.set_coremeta_recovery_required(true);

    let waiting_store = store.clone();
    let waiter = tokio::spawn(async move {
        waiting_store
            .wait_for_coremeta_recovery_wake(Duration::from_secs(60))
            .await;
    });
    tokio::task::yield_now().await;
    store.mark_coremeta_recovery_unready();

    tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("recovery loop did not wake after readiness was revoked")
        .unwrap();
    assert!(!store.coremeta_recovery_ready());
}

#[tokio::test]
async fn stale_publication_retry_preserves_existing_unready_state() {
    let directory = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(directory.path()).await.unwrap())
        .await
        .unwrap();
    store.set_coremeta_recovery_required(true);
    store.finish_stale_recovery_publication_retry("foreground race".to_string());

    assert!(!store.coremeta_recovery_ready());
    assert!(!store.coremeta_recovery_snapshot().ready);
}

#[tokio::test]
async fn stale_publication_retry_does_not_revoke_admitted_readiness() {
    let directory = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(directory.path()).await.unwrap())
        .await
        .unwrap();
    store.set_coremeta_recovery_required(true);
    let mut round = RecoveryRound {
        readiness_epoch: store.begin_coremeta_recovery_round(),
        root_directory_complete: true,
        canonical_settlement_complete: true,
        physical_register_quorum_complete: true,
        pending_mutations_complete: true,
        ..RecoveryRound::default()
    };
    round.reachable_peers.insert("node-a".into());
    assert!(store.finish_coremeta_recovery_round(&round, true, None));

    store.finish_stale_recovery_publication_retry("foreground race".to_string());

    assert!(store.coremeta_recovery_ready());
    assert!(store.coremeta_recovery_snapshot().ready);
}

#[tokio::test]
async fn local_store_ignores_distributed_repair_readiness_signals() {
    let directory = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(directory.path()).await.unwrap())
        .await
        .unwrap();

    store.mark_coremeta_recovery_unready();

    assert!(store.coremeta_recovery_ready());
}

#[tokio::test]
async fn incoming_generation_gap_coalesces_a_supervised_repair_target() {
    let directory = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(directory.path()).await.unwrap())
        .await
        .unwrap();
    store.set_coremeta_recovery_required(true);
    let root_key_hash = root_key_hash("test/recovery-target");

    assert!(
        !store
            .incoming_root_publication_is_ready(&root_key_hash, 3)
            .unwrap()
    );
    let first_epoch = store.begin_coremeta_recovery_round();
    assert_eq!(
        store.coremeta_root_repair_targets().get(&root_key_hash),
        Some(&3)
    );

    store.request_coremeta_root_repair(&root_key_hash, 2);
    assert_eq!(store.begin_coremeta_recovery_round(), first_epoch);
    store.request_coremeta_root_repair(&root_key_hash, 5);
    assert!(store.begin_coremeta_recovery_round() > first_epoch);
    assert_eq!(
        store.coremeta_root_repair_targets().get(&root_key_hash),
        Some(&5)
    );

    store.complete_coremeta_root_repair(&root_key_hash, 4);
    assert!(
        store
            .coremeta_root_repair_targets()
            .contains_key(&root_key_hash)
    );
    store.complete_coremeta_root_repair(&root_key_hash, 5);
    assert!(
        !store
            .coremeta_root_repair_targets()
            .contains_key(&root_key_hash)
    );
}

#[tokio::test]
async fn repair_signal_prevents_a_stale_round_from_restoring_readiness() {
    let directory = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(directory.path()).await.unwrap())
        .await
        .unwrap();
    store.set_coremeta_recovery_required(true);
    let mut round = RecoveryRound {
        readiness_epoch: store.begin_coremeta_recovery_round(),
        root_directory_complete: true,
        canonical_settlement_complete: true,
        physical_register_quorum_complete: true,
        pending_mutations_complete: true,
        ..RecoveryRound::default()
    };
    round.reachable_peers.insert("node-a".into());

    store.request_coremeta_root_repair(&root_key_hash("test/stale-round"), 1);

    assert!(!store.finish_coremeta_recovery_round(&round, true, None));
    assert!(!store.coremeta_recovery_ready());
    assert!(!store.coremeta_recovery_snapshot().ready);
}

#[test]
fn recovery_backoff_is_bounded() {
    let mut delay = RECOVERY_INITIAL_BACKOFF;
    for _ in 0..16 {
        delay = next_recovery_backoff(delay);
    }
    assert_eq!(delay, RECOVERY_MAX_BACKOFF);
}

#[test]
fn stale_foreground_publication_race_is_retryable_without_reopening_startup_barrier() {
    let stale: anyhow::Error = CoreStoreCommitError::RootChangedBeforeDurableStaging {
        root_key_hash: format!("sha256:{}", "a".repeat(64)),
        expected_generation: 3,
        expected_hash: format!("sha256:{}", "b".repeat(64)),
        actual_generation: 4,
        actual_hash: format!("sha256:{}", "c".repeat(64)),
    }
    .into();
    assert!(is_stale_recovery_publication(&stale));
    assert!(!is_stale_recovery_publication(&anyhow!(
        "corrupt recovery generation"
    )));
}

#[test]
fn recovery_sources_prefer_highest_generation_then_stable_node_id() {
    let mut sources = [
        RecoverySource {
            peer: RecoveryPeer {
                node_id: "node-b".into(),
                public_api_addr: "b".into(),
            },
            final_generation: 8,
            retention_floor_generation: 1,
        },
        RecoverySource {
            peer: RecoveryPeer {
                node_id: "node-a".into(),
                public_api_addr: "a".into(),
            },
            final_generation: 8,
            retention_floor_generation: 1,
        },
        RecoverySource {
            peer: RecoveryPeer {
                node_id: "node-c".into(),
                public_api_addr: "c".into(),
            },
            final_generation: 7,
            retention_floor_generation: 1,
        },
    ];
    sources.sort_by(|left, right| {
        right
            .final_generation
            .cmp(&left.final_generation)
            .then_with(|| left.peer.node_id.cmp(&right.peer.node_id))
    });
    assert_eq!(sources[0].peer.node_id, "node-a");
    assert_eq!(sources[1].peer.node_id, "node-b");
}

#[test]
fn recovery_readiness_requires_peer_convergence_and_no_pending_group() {
    let mut round = RecoveryRound::default();
    assert!(!recovery_round_is_ready(&round));

    round.reachable_peers.insert("node-a".into());
    assert!(!recovery_round_is_ready(&round));

    round.root_directory_complete = true;
    round.canonical_settlement_complete = true;
    round.physical_register_quorum_complete = true;
    round.pending_mutations_complete = true;
    assert!(recovery_round_is_ready(&round));

    round.lagging_roots.insert("root-a".into());
    assert!(!recovery_round_is_ready(&round));
    round.lagging_roots.clear();

    round.pending_bundles.insert(
        b"bundle-a".to_vec(),
        CoreMetaRecoveryPublicationBundle {
            transaction_id: "transaction-a".into(),
            publisher_node_id: "node-a".into(),
            scopes: vec![("root-a".into(), 1)],
            coordinator_scope: ("root-a".into(), 1),
            guard_context_hash: None,
            transaction_expires_at_unix_nanos: 0,
            guard_visible_update_count: 0,
            guard_precondition_count: 0,
        },
    );
    assert!(!recovery_round_is_ready(&round));
}

#[test]
fn admitted_recovery_remains_serviceable_during_foreground_publication() {
    let mut round = RecoveryRound {
        root_directory_complete: true,
        canonical_settlement_complete: true,
        physical_register_quorum_complete: true,
        pending_mutations_complete: false,
        ..RecoveryRound::default()
    };
    round.reachable_peers.insert("node-a".into());
    round
        .unresolved_publication_intents
        .insert("foreground-transaction".into());

    assert!(!recovery_round_is_ready(&round));
    assert!(recovery_round_is_serviceable(&round));
}

#[test]
fn admitted_recovery_stays_ready_while_unrelated_roots_converge() {
    let mut round = RecoveryRound {
        root_directory_complete: true,
        canonical_settlement_complete: true,
        physical_register_quorum_complete: true,
        pending_mutations_complete: true,
        ..RecoveryRound::default()
    };
    round.reachable_peers.insert("node-a".into());
    round.lagging_roots.insert("root-a".into());
    round.pending_bundles.insert(
        b"bundle-a".to_vec(),
        CoreMetaRecoveryPublicationBundle {
            transaction_id: "transaction-a".into(),
            publisher_node_id: "node-a".into(),
            scopes: vec![("root-a".into(), 1)],
            coordinator_scope: ("root-a".into(), 1),
            guard_context_hash: None,
            transaction_expires_at_unix_nanos: 0,
            guard_visible_update_count: 0,
            guard_precondition_count: 0,
        },
    );

    assert!(!recovery_round_is_serviceable(&round));
    assert!(recovery_round_preserves_admitted_readiness(&round));
}

#[test]
fn pending_mutations_wait_for_canonical_history_settlement() {
    let mut round = RecoveryRound {
        root_directory_complete: true,
        canonical_settlement_complete: true,
        physical_register_quorum_complete: true,
        pending_mutations_complete: false,
        ..RecoveryRound::default()
    };
    round.reachable_peers.insert("node-a".into());
    assert!(recovery_round_can_replay_pending_mutations(&round));

    round.lagging_roots.insert("stream-root".into());
    assert!(!recovery_round_can_replay_pending_mutations(&round));
    round.lagging_roots.clear();

    round
        .unresolved_publication_intents
        .insert("publication-a".into());
    assert!(!recovery_round_can_replay_pending_mutations(&round));
}

#[test]
fn root_directory_quorum_settle_requires_current_complete_and_quorum() {
    let state = StdMutex::new(RootDirectoryScanState {
        peers_with_complete_pass: BTreeSet::from(["node-a".into(), "node-b".into()]),
        ..RootDirectoryScanState::default()
    });
    let authoritative = BTreeSet::from(["node-a".into(), "node-b".into(), "node-c".into()]);
    let mut reachable = BTreeSet::from(["node-b".into(), "node-c".into()]);
    assert!(!root_directory_quorum_is_settled(
        &state,
        &reachable,
        &authoritative,
        2,
    ));

    reachable.insert("node-a".into());
    assert!(root_directory_quorum_is_settled(
        &state,
        &reachable,
        &authoritative,
        2,
    ));

    reachable.remove("node-c");
    assert!(!root_directory_quorum_is_settled(
        &state,
        &reachable,
        &authoritative,
        3,
    ));
}

#[test]
fn root_directory_quorum_ignores_non_register_peers() {
    let state = StdMutex::new(RootDirectoryScanState {
        peers_with_complete_pass: BTreeSet::from([
            "register-a".into(),
            "cache-d".into(),
            "cache-e".into(),
        ]),
        ..RootDirectoryScanState::default()
    });
    let authoritative = BTreeSet::from([
        "register-a".into(),
        "register-b".into(),
        "register-c".into(),
    ]);
    let reachable = BTreeSet::from(["register-a".into(), "cache-d".into(), "cache-e".into()]);
    assert!(!root_directory_quorum_is_settled(
        &state,
        &reachable,
        &authoritative,
        2,
    ));
}

#[test]
fn local_replica_plus_one_complete_remote_satisfies_r3q2_discovery() {
    assert_eq!(remote_recovery_acknowledgements(2, true), 1);
    assert_eq!(remote_recovery_acknowledgements(2, false), 2);
    let state = StdMutex::new(RootDirectoryScanState {
        peers_with_complete_pass: BTreeSet::from(["node-b".into()]),
        ..RootDirectoryScanState::default()
    });
    let authoritative = BTreeSet::from(["node-a".into(), "node-b".into(), "node-c".into()]);
    let reachable = BTreeSet::from(["node-b".into()]);
    assert!(root_directory_quorum_is_settled(
        &state,
        &reachable,
        &authoritative,
        remote_recovery_acknowledgements(2, true),
    ));
    assert!(!root_directory_quorum_is_settled(
        &state,
        &reachable,
        &authoritative,
        remote_recovery_acknowledgements(2, false),
    ));
}

#[test]
fn root_directory_heads_only_require_inventory_when_a_peer_is_ahead() {
    let heads = BTreeMap::from([
        (
            "node-a".to_string(),
            RootDirectoryEntry {
                root_key_hash:
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                root_generation: 7,
                root_anchor_hash:
                    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
            },
        ),
        (
            "node-b".to_string(),
            RootDirectoryEntry {
                root_key_hash:
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                root_generation: 9,
                root_anchor_hash:
                    "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".into(),
            },
        ),
    ]);
    assert_eq!(highest_remote_root_generation(Some(&heads)), 9);
    assert!(remote_root_needs_inventory(8, Some(&heads)));
    assert!(!remote_root_needs_inventory(9, Some(&heads)));
    assert!(!remote_root_needs_inventory(10, Some(&heads)));
    assert_eq!(highest_remote_root_generation(Some(&BTreeMap::new())), 0);
    assert_eq!(highest_remote_root_generation(None), 0);
}

#[test]
fn a_new_root_directory_pass_preserves_the_last_complete_snapshot() {
    let first = RootDirectoryEntry {
        root_key_hash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .into(),
        root_generation: 1,
        root_anchor_hash: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            .into(),
    };
    let second = RootDirectoryEntry {
        root_key_hash: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
            .into(),
        root_generation: 2,
        root_anchor_hash: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
            .into(),
    };
    let mut state = RootDirectoryScanState::default();
    state.record_page(
        "node-a",
        "",
        &RootDirectoryPage {
            entries: vec![first.clone()],
            next_root_key_hash: first.root_key_hash.clone(),
            directory_complete: false,
            page_hash: String::new(),
            encoded_bytes: 1,
        },
    );
    assert!(!state.peers_with_complete_pass.contains("node-a"));
    state.record_page(
        "node-a",
        &first.root_key_hash,
        &RootDirectoryPage {
            entries: vec![second],
            next_root_key_hash: String::new(),
            directory_complete: true,
            page_hash: String::new(),
            encoded_bytes: 1,
        },
    );
    assert!(state.peers_with_complete_pass.contains("node-a"));
    assert_eq!(state.peer_entries["node-a"].len(), 2);

    state.record_page(
        "node-a",
        "",
        &RootDirectoryPage {
            entries: vec![first.clone()],
            next_root_key_hash: first.root_key_hash,
            directory_complete: false,
            page_hash: String::new(),
            encoded_bytes: 1,
        },
    );
    assert!(state.peers_with_complete_pass.contains("node-a"));
    assert_eq!(
        state.peer_entries["node-a"].len(),
        2,
        "an incomplete refresh must not expose a partial directory"
    );
    assert_eq!(state.pending_peer_entries["node-a"].len(), 1);
}

#[test]
fn a_completed_root_directory_refresh_replaces_the_prior_snapshot_atomically() {
    let stale = RootDirectoryEntry {
        root_key_hash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .into(),
        root_generation: 1,
        root_anchor_hash: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            .into(),
    };
    let current = RootDirectoryEntry {
        root_key_hash: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
            .into(),
        root_generation: 2,
        root_anchor_hash: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
            .into(),
    };
    let mut state = RootDirectoryScanState {
        peers_with_complete_pass: BTreeSet::from(["node-a".into()]),
        peer_entries: BTreeMap::from([(
            "node-a".into(),
            BTreeMap::from([(stale.root_key_hash.clone(), stale)]),
        )]),
        ..RootDirectoryScanState::default()
    };

    state.record_page(
        "node-a",
        "",
        &RootDirectoryPage {
            entries: vec![current.clone()],
            next_root_key_hash: String::new(),
            directory_complete: true,
            page_hash: String::new(),
            encoded_bytes: 1,
        },
    );

    assert!(state.peers_with_complete_pass.contains("node-a"));
    assert_eq!(
        state.peer_entries["node-a"].keys().collect::<Vec<_>>(),
        vec![&current.root_key_hash]
    );
    assert!(!state.pending_peer_entries.contains_key("node-a"));
}

#[test]
fn first_generation_agreement_starts_without_an_invalid_zero_cursor() {
    assert_eq!(inventory_cursor_before(0), None);
    assert_eq!(inventory_cursor_before(1), None);
    assert_eq!(
        inventory_cursor_before(2),
        Some(CoreMetaInventoryCursor { generation: 1 })
    );
}
