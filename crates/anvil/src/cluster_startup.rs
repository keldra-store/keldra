use anvil_consensus::{
    ApplyResult, CLUSTER_CONTROL_COMMAND_VERSION, ClusterId, Command, DecisionRaft,
    ErasureCodeProfile, JwtSigningKeyFingerprint, NodeId, SYSTEM_BOOTSTRAP_VERSION,
    SystemBootstrapState as ConsensusBootstrapState,
};
use anvil_store::{ErasureProfile, Store, SystemBootstrapState as LocalBootstrapState};
use anyhow::{Context, Result, bail};
use uuid::Uuid;

use crate::bootstrap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BootstrapAction {
    Ready,
    CommitExistingLocalState,
    CreateLocalStateThenCommit,
}

/// Ensure the one-node genesis group has exactly one stable cluster identity.
///
/// Seed discovery and joining deliberately do not use this helper: they must
/// learn the existing identity from the admitted cluster.
pub(crate) async fn ensure_genesis_identity(decisions: &DecisionRaft) -> Result<ClusterId> {
    if let Some(cluster_id) = decisions.state()?.cluster_id() {
        return Ok(cluster_id);
    }

    let requested = ClusterId(*Uuid::new_v4().as_bytes());
    let committed = decisions
        .submit(Command::InitializeCluster {
            cluster_id: requested,
        })
        .await
        .context("commit genesis cluster identity")?;
    match committed.result {
        ApplyResult::ClusterInitialized { cluster_id } if cluster_id == requested => Ok(cluster_id),
        ApplyResult::ClusterInitialized { cluster_id } => {
            bail!(
                "cluster identity changed while genesis was being initialized: requested {:?}, committed {:?}",
                requested,
                cluster_id
            )
        }
        result => bail!("cluster identity command returned unexpected result {result:?}"),
    }
}

/// Bind the startup-selected immutable erasure geometry to this cluster.
///
/// Genesis and an in-place 0.5.0 upgrade bind an absent value exactly once.
/// Every later restart must present the already committed profile.
pub(crate) async fn ensure_erasure_code_profile(
    decisions: &DecisionRaft,
    requested: ErasureProfile,
) -> Result<()> {
    let requested = ErasureCodeProfile {
        data_shards: requested.data_shards(),
        parity_shards: requested.parity_shards(),
        stripe_unit: requested.stripe_unit(),
    };
    if let Some(committed) = decisions.state()?.cluster_control().erasure_code_profile() {
        anyhow::ensure!(
            committed == requested,
            "configured erasure-code profile {}+{} with {}-byte stripes does not match the committed cluster profile {}+{} with {}-byte stripes",
            requested.data_shards,
            requested.parity_shards,
            requested.stripe_unit,
            committed.data_shards,
            committed.parity_shards,
            committed.stripe_unit,
        );
        return Ok(());
    }

    let committed = decisions
        .submit(Command::BindErasureCodeProfile {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            profile: requested,
        })
        .await
        .context("commit immutable cluster erasure-code profile")?;
    match committed.result {
        ApplyResult::ErasureCodeProfileBound(profile) if profile == requested => Ok(()),
        result => bail!("erasure-code profile command returned unexpected result {result:?}"),
    }
}

/// Bind the operator-selected JWT material to this cluster without retaining
/// the secret. An absent value is the released-0.5.0 migration case; every
/// later startup must match the first committed fingerprint.
pub(crate) async fn ensure_jwt_signing_key_fingerprint(
    decisions: &DecisionRaft,
    requested: JwtSigningKeyFingerprint,
) -> Result<()> {
    if let Some(committed) = decisions
        .state()?
        .cluster_control()
        .jwt_signing_key_fingerprint()
    {
        anyhow::ensure!(
            committed == requested,
            "configured JWT signing key does not match the committed cluster fingerprint"
        );
        return Ok(());
    }

    let committed = decisions
        .submit(Command::BindJwtSigningKeyFingerprint {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            fingerprint: requested,
        })
        .await
        .context("commit immutable JWT signing-key fingerprint")?;
    match committed.result {
        ApplyResult::JwtSigningKeyFingerprintBound(fingerprint) if fingerprint == requested => {
            Ok(())
        }
        result => {
            bail!("JWT signing-key fingerprint command returned unexpected result {result:?}")
        }
    }
}

/// Commit the bounded Raft bootstrap marker after protected store records are
/// durable. Repeated startup reads the retained state and adds no new log item.
pub(crate) async fn complete_system_bootstrap(
    decisions: &DecisionRaft,
    local_node: NodeId,
) -> Result<()> {
    let state = decisions.state()?;
    match state.system_bootstrap() {
        ConsensusBootstrapState::Missing => {}
        ConsensusBootstrapState::Complete {
            version: SYSTEM_BOOTSTRAP_VERSION,
            ..
        } => return Ok(()),
        ConsensusBootstrapState::Complete { version, .. } => {
            bail!("Raft system-bootstrap version {version} is unsupported")
        }
    }
    let nomination = state
        .executor()
        .context("system bootstrap requires a nominated atomic executor")?;
    if nomination.executor != local_node {
        bail!(
            "system bootstrap belongs to nominated executor {}, not local node {}",
            nomination.executor.0,
            local_node.0
        );
    }

    let committed = decisions
        .submit(Command::CompleteSystemBootstrap {
            executor: local_node,
            nomination_log_index: nomination.nomination_log_index,
            bootstrap_version: SYSTEM_BOOTSTRAP_VERSION,
        })
        .await
        .context("commit system-bootstrap completion")?;
    match committed.result {
        ApplyResult::SystemBootstrapCompleted(ConsensusBootstrapState::Complete {
            version: SYSTEM_BOOTSTRAP_VERSION,
            ..
        }) => Ok(()),
        result => bail!("system-bootstrap command returned unexpected result {result:?}"),
    }
}

/// Reconcile the durable local protected identity with the retained Raft
/// completion marker before any public service is exposed.
///
/// The local records are always made durable first. Raft records only that
/// those records safely exist; it never contains credentials or Zanzibar data.
pub(crate) async fn reconcile_system_bootstrap(
    store: &Store,
    decisions: &DecisionRaft,
    local_node: NodeId,
    data_dir: &std::path::Path,
    run_system_bootstrap: bool,
    configured_output: Option<&std::path::Path>,
) -> Result<()> {
    let local = read_local_bootstrap_state(store).await?;
    let raft = decisions.state()?.system_bootstrap();
    match bootstrap_action(local, raft, run_system_bootstrap)? {
        BootstrapAction::Ready => Ok(()),
        BootstrapAction::CommitExistingLocalState => {
            complete_system_bootstrap(decisions, local_node).await
        }
        BootstrapAction::CreateLocalStateThenCommit => {
            bootstrap::enforce(store, data_dir, true, configured_output).await?;
            let durable = read_local_bootstrap_state(store).await?;
            anyhow::ensure!(
                durable
                    == (LocalBootstrapState::Complete {
                        version: anvil_store::SYSTEM_BOOTSTRAP_VERSION,
                    }),
                "local system bootstrap did not leave a durable completion marker"
            );
            complete_system_bootstrap(decisions, local_node).await
        }
    }
}

async fn read_local_bootstrap_state(store: &Store) -> Result<LocalBootstrapState> {
    let state_store = store.clone();
    tokio::task::spawn_blocking(move || state_store.system_bootstrap_state())
        .await
        .context("join system bootstrap marker read")?
        .context("read system bootstrap marker")
}

fn bootstrap_action(
    local: LocalBootstrapState,
    raft: ConsensusBootstrapState,
    run_system_bootstrap: bool,
) -> Result<BootstrapAction> {
    let local_complete = match local {
        LocalBootstrapState::Missing => false,
        LocalBootstrapState::Complete {
            version: anvil_store::SYSTEM_BOOTSTRAP_VERSION,
        } => true,
        LocalBootstrapState::Complete { version } => {
            bail!("local system-bootstrap version {version} is unsupported")
        }
    };
    let raft_complete = match raft {
        ConsensusBootstrapState::Missing => false,
        ConsensusBootstrapState::Complete {
            version: SYSTEM_BOOTSTRAP_VERSION,
            ..
        } => true,
        ConsensusBootstrapState::Complete { version, .. } => {
            bail!("Raft system-bootstrap version {version} is unsupported")
        }
    };

    match (local_complete, raft_complete, run_system_bootstrap) {
        (true, true, _) => Ok(BootstrapAction::Ready),
        (true, false, _) => Ok(BootstrapAction::CommitExistingLocalState),
        (false, true, _) => bail!(
            "Raft records a completed system bootstrap, but the protected local identity is missing"
        ),
        (false, false, true) => Ok(BootstrapAction::CreateLocalStateThenCommit),
        (false, false, false) => bail!(
            "system bootstrap has not completed; start this node once with --run-system-bootstrap"
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use anvil_store::{StoreOptions, SystemBootstrapRequest};

    use super::*;
    use crate::programs::ProgramCoordinator;

    const LOCAL_COMPLETE: LocalBootstrapState = LocalBootstrapState::Complete {
        version: anvil_store::SYSTEM_BOOTSTRAP_VERSION,
    };

    fn raft_complete(committed_log_index: u64) -> ConsensusBootstrapState {
        ConsensusBootstrapState::Complete {
            version: SYSTEM_BOOTSTRAP_VERSION,
            committed_log_index,
        }
    }

    #[test]
    fn bootstrap_state_matrix_is_fail_closed() {
        assert_eq!(
            bootstrap_action(LOCAL_COMPLETE, raft_complete(7), false).unwrap(),
            BootstrapAction::Ready
        );
        assert_eq!(
            bootstrap_action(LOCAL_COMPLETE, ConsensusBootstrapState::Missing, false).unwrap(),
            BootstrapAction::CommitExistingLocalState
        );
        assert_eq!(
            bootstrap_action(
                LocalBootstrapState::Missing,
                ConsensusBootstrapState::Missing,
                true
            )
            .unwrap(),
            BootstrapAction::CreateLocalStateThenCommit
        );

        assert_eq!(
            bootstrap_action(LOCAL_COMPLETE, raft_complete(7), true).unwrap(),
            BootstrapAction::Ready
        );
        assert_eq!(
            bootstrap_action(LOCAL_COMPLETE, ConsensusBootstrapState::Missing, true).unwrap(),
            BootstrapAction::CommitExistingLocalState
        );
        for requested in [false, true] {
            let error = bootstrap_action(LocalBootstrapState::Missing, raft_complete(7), requested)
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("protected local identity is missing")
            );
        }
        let error = bootstrap_action(
            LocalBootstrapState::Missing,
            ConsensusBootstrapState::Missing,
            false,
        )
        .unwrap_err();
        assert!(error.to_string().contains("--run-system-bootstrap"));
    }

    #[tokio::test]
    async fn erasure_profile_is_bound_once_and_restart_configuration_must_match() {
        let temporary = tempfile::tempdir().unwrap();
        let decisions = DecisionRaft::open(temporary.path().join("decisions"), 1, 16, 64 * 1024)
            .await
            .unwrap();
        decisions.ensure_one_node().await.unwrap();
        decisions
            .wait_for_leader(Duration::from_secs(10))
            .await
            .unwrap();
        ensure_genesis_identity(&decisions).await.unwrap();

        let profile = ErasureProfile::default();
        ensure_erasure_code_profile(&decisions, profile)
            .await
            .unwrap();
        ensure_erasure_code_profile(&decisions, profile)
            .await
            .unwrap();
        assert_eq!(
            decisions
                .state()
                .unwrap()
                .cluster_control()
                .erasure_code_profile(),
            Some(ErasureCodeProfile {
                data_shards: 2,
                parity_shards: 1,
                stripe_unit: 16 * 1024,
            })
        );

        let mismatch = ErasureProfile::new(4, 2, 16 * 1024).unwrap();
        let error = ensure_erasure_code_profile(&decisions, mismatch)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("does not match the committed"));
        decisions.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn legacy_absent_jwt_fingerprint_is_bound_and_restarts_fail_closed() {
        let temporary = tempfile::tempdir().unwrap();
        let expected = JwtSigningKeyFingerprint([21; 32]);
        let mismatch = JwtSigningKeyFingerprint([22; 32]);
        let decisions = DecisionRaft::open(temporary.path().join("decisions"), 1, 16, 64 * 1024)
            .await
            .unwrap();
        decisions.ensure_one_node().await.unwrap();
        decisions
            .wait_for_leader(Duration::from_secs(10))
            .await
            .unwrap();
        ensure_genesis_identity(&decisions).await.unwrap();
        assert_eq!(
            decisions
                .state()
                .unwrap()
                .cluster_control()
                .jwt_signing_key_fingerprint(),
            None,
            "released 0.5.0 state has no JWT fingerprint"
        );
        ensure_jwt_signing_key_fingerprint(&decisions, expected)
            .await
            .unwrap();
        decisions.shutdown().await.unwrap();
        drop(decisions);

        let decisions = DecisionRaft::open(temporary.path().join("decisions"), 1, 16, 64 * 1024)
            .await
            .unwrap();
        decisions.ensure_one_node().await.unwrap();
        decisions
            .wait_for_leader(Duration::from_secs(10))
            .await
            .unwrap();
        ensure_jwt_signing_key_fingerprint(&decisions, expected)
            .await
            .unwrap();
        let error = ensure_jwt_signing_key_fingerprint(&decisions, mismatch)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("does not match the committed"));
        assert_eq!(
            decisions
                .state()
                .unwrap()
                .cluster_control()
                .jwt_signing_key_fingerprint(),
            Some(expected)
        );
        decisions.shutdown().await.unwrap();
    }

    async fn open_runtime(root: &Path) -> (Store, DecisionRaft, ProgramCoordinator) {
        let store = Store::open(StoreOptions::new(root, 1)).await.unwrap();
        let decisions = DecisionRaft::open(root.join("decisions"), 1, 16, 64 * 1024)
            .await
            .unwrap();
        decisions.ensure_one_node().await.unwrap();
        decisions
            .wait_for_leader(Duration::from_secs(10))
            .await
            .unwrap();
        ensure_genesis_identity(&decisions).await.unwrap();
        let programs = ProgramCoordinator::start(store.clone(), decisions.clone(), NodeId(1))
            .await
            .unwrap();
        (store, decisions, programs)
    }

    async fn stop_runtime(store: Store, decisions: DecisionRaft, programs: ProgramCoordinator) {
        drop(programs);
        decisions.shutdown().await.unwrap();
        drop(decisions);
        drop(store);
    }

    #[tokio::test]
    async fn existing_local_marker_is_migrated_once_and_restart_is_idempotent() {
        let temporary = tempfile::tempdir().unwrap();
        let initial_store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        initial_store
            .bootstrap_system(SystemBootstrapRequest {
                app_id: "existing-bootstrap-app".into(),
                client_id: "existing-bootstrap-client".into(),
                client_secret: "existing-bootstrap-secret-with-at-least-32-bytes".into(),
            })
            .unwrap();
        drop(initial_store);

        let credential_output = temporary.path().join("must-not-be-created.json");
        let (store, decisions, programs) = open_runtime(temporary.path()).await;
        reconcile_system_bootstrap(
            &store,
            &decisions,
            NodeId(1),
            temporary.path(),
            false,
            Some(&credential_output),
        )
        .await
        .unwrap();
        let first_completion = decisions
            .state()
            .unwrap()
            .system_bootstrap()
            .committed_log_index()
            .unwrap();
        let first_cluster_id = decisions.state().unwrap().cluster_id().unwrap();
        assert!(!credential_output.exists());
        stop_runtime(store, decisions, programs).await;

        let (store, decisions, programs) = open_runtime(temporary.path()).await;
        reconcile_system_bootstrap(
            &store,
            &decisions,
            NodeId(1),
            temporary.path(),
            false,
            Some(&credential_output),
        )
        .await
        .unwrap();
        assert_eq!(
            decisions
                .state()
                .unwrap()
                .system_bootstrap()
                .committed_log_index(),
            Some(first_completion)
        );
        assert_eq!(
            decisions.state().unwrap().cluster_id(),
            Some(first_cluster_id)
        );
        assert!(!credential_output.exists());
        stop_runtime(store, decisions, programs).await;
    }

    #[tokio::test]
    async fn raft_completion_never_recreates_a_missing_protected_identity() {
        let temporary = tempfile::tempdir().unwrap();
        let credential_output = temporary.path().join("must-not-be-created.json");
        let (store, decisions, programs) = open_runtime(temporary.path()).await;
        complete_system_bootstrap(&decisions, NodeId(1))
            .await
            .unwrap();

        let error = reconcile_system_bootstrap(
            &store,
            &decisions,
            NodeId(1),
            temporary.path(),
            true,
            Some(&credential_output),
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("protected local identity is missing")
        );
        assert_eq!(
            store.system_bootstrap_state().unwrap(),
            LocalBootstrapState::Missing
        );
        assert!(!credential_output.exists());
        stop_runtime(store, decisions, programs).await;
    }
}
