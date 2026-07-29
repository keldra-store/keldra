#![recursion_limit = "512"]

use crate::auth::JwtManager;
use crate::config::Config;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::warn;

pub(crate) fn emit_test_timing(label: impl AsRef<str>, elapsed: Duration) {
    let label = label.as_ref();
    if std::env::var("ANVIL_TEST_TIMINGS").is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    }) {
        eprintln!("[timing] {label}={elapsed:?}");
    }
    if crate::perf::enabled() {
        crate::perf::record_duration("anvil_internal_span", &[("span", label)], elapsed);
    }
}

// The modules we've created
pub mod access_control;
pub mod anvil_personaldb_sqlite_changeset;
pub mod auth;
pub mod authz_derived_lag_watch;
mod authz_head;
pub mod authz_journal;
pub mod authz_namespace_watch;
pub mod authz_realm_schema;
pub mod authz_repair;
pub mod authz_schema;
pub mod authz_schema_contract;
pub mod authz_scope;
pub mod authz_segment;
pub mod authz_userset_index;
pub mod bucket_journal;
pub mod bucket_locator_finalization_job;
pub mod bucket_manager;
pub mod bundle_replication;
#[cfg(feature = "test-cluster-transport-faults")]
pub mod cluster_transport_fault;
pub mod config;
pub mod control_journal;
pub mod core_store;
pub mod crypto;
pub mod derived_index_catchup;
pub mod derived_index_proof;
pub mod diagnostic_store;
#[cfg(test)]
mod direct_mutation_contract;
pub mod directory_repair;
pub mod discovery;
pub mod embedding_provider;
pub mod error_codes;
pub mod formats;
pub mod full_text_segment;
pub mod gateway_store;
pub mod git_pack;
pub mod git_source_index;
pub mod git_source_manifest;
pub mod git_source_postcommit_job;
pub mod git_source_query;
pub mod git_source_watch;
pub mod hf_ingestion_postcommit_job;
pub mod hf_journal;
pub mod index_builder;
pub mod index_coremeta;
pub mod index_diagnostic_journal;
pub mod index_finalization_job;
pub mod index_journal;
pub mod index_partition_watch;
pub mod index_repair;
pub mod local_object_store;
pub mod manifest_journal;
pub mod media_extraction;
pub mod mesh_control_segment;
pub mod mesh_control_stream;
pub mod mesh_directory;
pub mod mesh_lifecycle;
pub mod metadata_journal;
pub mod middleware;
pub mod model_journal;
pub mod multipart_journal;
pub mod mvcc_apply_worker;
pub mod mvcc_assignment_reconciler;
pub mod mvcc_bootstrap;
pub mod mvcc_consensus_adapter;
pub mod mvcc_control_plane;
#[cfg(test)]
mod mvcc_crash_restart_acceptance;
#[cfg(test)]
mod mvcc_cross_feature_tests;
pub mod mvcc_fault_injection;
pub mod mvcc_gc;
pub mod mvcc_gc_coordinator;
pub mod mvcc_local_durability_upgrade;
pub mod mvcc_node_runtime;
#[cfg(test)]
mod mvcc_observability_contract;
pub mod mvcc_open_transactions;
pub mod mvcc_outbox;
pub(crate) mod mvcc_physical_payload;
#[cfg(test)]
mod mvcc_process_crash_acceptance;
pub mod mvcc_product;
#[cfg(test)]
mod mvcc_service_acceptance;
pub mod mvcc_shard_repair;
pub mod mvcc_store;
#[cfg(test)]
mod mvcc_three_node_fault_tests;
pub mod mvcc_transaction;
pub mod mvcc_worker_authority;
pub mod native_idempotency;
pub mod node_identity;
pub mod node_signing;
pub mod object_link_finalization_job;
pub mod object_links;
pub mod object_manager;
pub mod object_materialisation;
pub mod object_materialisation_runner;
pub mod object_shard_manifest;
pub mod observability;
pub mod partition_fence;
pub mod perf;
pub mod perf_baseline;
pub mod permissions;
pub mod persistence;
pub mod personaldb_catchup;
pub mod personaldb_commit_store;
pub mod personaldb_control;
pub mod personaldb_coremeta;
pub mod personaldb_envelope;
pub mod personaldb_heads;
pub mod personaldb_postcommit_job;
pub mod personaldb_projection;
pub mod personaldb_projection_builder;
pub mod personaldb_projection_snapshot;
pub mod personaldb_projection_writeback;
pub mod personaldb_proposal_admission;
pub mod personaldb_repair;
pub mod personaldb_row_index;
pub mod personaldb_schema;
pub mod personaldb_segment;
pub mod personaldb_signing;
pub mod personaldb_signing_object;
pub mod personaldb_signing_store;
pub mod personaldb_snapshot_builder;
pub mod personaldb_snapshot_store;
pub mod personaldb_submit;
pub mod personaldb_watch;
pub mod query_planner;
pub mod registry_segment;
pub mod repair_finding;
pub mod replication;
pub mod replication_client;
pub mod routing;
pub mod search_query;
pub mod services;
pub mod shard_placement;
pub mod shard_store;
pub mod sharding;
pub mod storage;
pub mod streaming_erasure;
pub mod system_realm;
pub(crate) mod task_execution_guard;
pub mod task_journal;
pub mod task_lease;
pub mod tasks;
pub mod tenant_audit;
pub mod tenant_locator_finalization_job;
pub mod typed_field_segment;
pub mod validation;
pub mod vector_hnsw;
pub mod vector_segment;
pub mod watch_checkpoint;
pub mod watch_log;
pub mod watch_resume;
pub mod worker;
pub mod writer_segment_catalog;
pub mod writer_segment_range;

#[cfg(test)]
pub(crate) mod test_support;

// The gRPC code generated by tonic-build
pub mod admin_audit;
pub mod append_journal;
pub mod anvil_api {
    tonic::include_proto!("anvil");
}

// Our application state, which will hold the persistence layer, storage engine, etc.
#[derive(Clone, Debug)]
pub struct AppState {
    pub persistence: persistence::Persistence,
    pub storage: storage::Storage,
    pub core_store: core_store::CoreStore,
    pub sharder: sharding::ShardManager,
    pub jwt_manager: Arc<JwtManager>,
    pub region: String,
    pub bucket_manager: bucket_manager::BucketManager,
    pub object_manager: object_manager::ObjectManager,
    pub config: Arc<Config>,
    pub secret_keyring: Arc<crypto::EncryptionKeyring>,
    pub personaldb_signing_key_store: Arc<personaldb_signing_store::PersonalDbSigningKeyStore>,
    pub personaldb_protocol_keyring: Arc<personaldb_signing::PersonalDbProtocolKeyring>,
    pub personaldb_commit_locks: Arc<Mutex<HashMap<String, Weak<Mutex<()>>>>>,
    pub native_mutation_locks: Arc<Mutex<HashMap<String, Weak<Mutex<()>>>>>,
    pub observability: observability::Observability,
    pub mvcc: Arc<mvcc_bootstrap::MvccSubsystem>,
}

fn bootstrap_system_realm_before_coremeta_recovery(
    mvcc_peer_count: usize,
    marker_exists: bool,
) -> bool {
    mvcc_peer_count == 1 || !marker_exists
}

impl AppState {
    pub async fn new(
        config: Config,
        personaldb_protocol_keyring: personaldb_signing::PersonalDbProtocolKeyring,
    ) -> Result<Self> {
        let config = config.with_persisted_identity().await?;
        let secret_keyring = Arc::new(config.secret_keyring()?);
        let has_personaldb_keyring_override = personaldb_protocol_keyring.is_enabled()
            || !personaldb_protocol_keyring.trust_store().is_empty();
        let partition_signing_key = hex::decode(&config.anvil_secret_encryption_key)?;
        let arc_config = Arc::new(config);
        let distributed_coremeta_recovery = arc_config.requires_distributed_coremeta_recovery();
        let jwt_manager = Arc::new(JwtManager::new(arc_config.jwt_secret.clone()));
        let storage = storage::Storage::new_at(&arc_config.storage_path).await?;
        let core_store = core_store::CoreStore::new_with_pipeline_keyring_and_identity(
            storage.clone(),
            arc_config.core_pipeline_keyring()?,
            core_store::CoreStoreNodeIdentity {
                mesh_id: arc_config.mesh_id.clone(),
                node_id: arc_config.node_id.clone(),
                region_id: arc_config.region.clone(),
                cell_id: arc_config.cell_id.clone(),
                public_api_addr: arc_config.public_api_addr.clone(),
                internal_bearer_token: (!arc_config.corestore_internal_bearer_token.is_empty())
                    .then(|| arc_config.corestore_internal_bearer_token.clone()),
            },
            if distributed_coremeta_recovery {
                // Raft/MVCC remains the cluster startup authority. CoreStore
                // must nevertheless defer replay of a same-disk pending
                // mutation until its local CoreMeta history has caught up to
                // the physical root-register quorum; replaying it here can
                // otherwise publish from a stale root before this process can
                // serve or consume recovery RPCs.
                core_store::CoreStoreStartupRecovery::Distributed
            } else {
                core_store::CoreStoreStartupRecovery::Immediate
            },
        )
        .await?;
        let mvcc = Arc::new(
            mvcc_bootstrap::MvccSubsystem::bootstrap(&arc_config, core_store.core_meta_database())
                .await
                .context("bootstrap mandatory MVCC subsystem")?,
        );
        let personaldb_signing_key_store =
            Arc::new(personaldb_signing_store::PersonalDbSigningKeyStore::new(
                storage.clone(),
                mvcc.clone(),
                secret_keyring.clone(),
            ));
        let personaldb_protocol_keyring = if has_personaldb_keyring_override {
            personaldb_protocol_keyring
        } else {
            match personaldb_signing_key_store.load_protocol_keyring() {
                Ok(keyring) => keyring,
                Err(error) => {
                    warn!(
                        error = %error,
                        "PersonalDB signing keys are unavailable; PersonalDB signing operations will fail closed"
                    );
                    personaldb_signing::PersonalDbProtocolKeyring::disabled()
                }
            }
        };
        let personaldb_protocol_keyring = Arc::new(personaldb_protocol_keyring);
        let persistence = persistence::Persistence::new(&arc_config)?;
        persistence
            .install_mvcc(mvcc.clone())
            .context("install MVCC transaction staging in persistence")?;
        if !arc_config.region.is_empty()
            && arc_config.mvcc_bootstrap_membership
            && !distributed_coremeta_recovery
        {
            // A standalone node owns its local region bootstrap. Distributed
            // regions are installed through the admin topology bootstrap and
            // recovered through CoreMeta; re-creating one on every process
            // start can observe a prepared-but-not-yet-recovered fence row.
            persistence
                .create_region(&arc_config.region)
                .await
                .context("bootstrap standalone region")?;
        }
        let sharder = sharding::ShardManager::new();
        let personaldb_commit_locks = Arc::new(Mutex::new(HashMap::new()));
        let native_mutation_locks = Arc::new(Mutex::new(HashMap::new()));
        let observability = observability::Observability::default();

        let bucket_manager =
            bucket_manager::BucketManager::new(persistence.clone(), storage.clone());
        let object_manager = object_manager::ObjectManager::new(
            persistence.clone(),
            storage.clone(),
            core_store.clone(),
            arc_config.region.clone(),
            arc_config.cross_region_routing_policy,
            partition_signing_key,
            observability.clone(),
            services::object::configured_default_durability(&arc_config.mvcc_default_durability)
                .map_err(|status| anyhow::anyhow!(status.to_string()))?,
        );
        object_manager
            .install_mvcc(mvcc.clone())
            .context("install MVCC object runtime")?;
        let system_realm_marker_exists = system_realm::bootstrap_marker_exists_in_runtime(
            mvcc.runtime.as_ref(),
            &system_realm::normalized_mesh_id(&arc_config.mesh_id),
        )?;
        if bootstrap_system_realm_before_coremeta_recovery(
            mvcc.peers.len(),
            system_realm_marker_exists,
        ) {
            // A fresh multi-node cluster has no active physical topology from
            // which CoreMeta recovery can select peers. Its admin topology RPC
            // is itself protected by the first system-realm grants, so the
            // first-ever bootstrap must retain the synthetic local-control
            // path. A restart with an existing marker defers schema upgrade
            // until distributed root history has reconciled in `anvil`.
            if mvcc.peers.len() > 1 {
                let bootstrap_config = arc_config.clone();
                let bootstrap_persistence = persistence.clone();
                let bootstrap_storage = storage.clone();
                let bootstrap_keyring = secret_keyring.clone();
                tokio::spawn(async move {
                    loop {
                        if !bootstrap_persistence
                            .mvcc()
                            .is_ok_and(|mvcc| mvcc.consensus.is_leader())
                        {
                            tokio::time::sleep(Duration::from_millis(250)).await;
                            continue;
                        }
                        match system_realm::ensure_bootstrapped(
                            &bootstrap_config,
                            &bootstrap_persistence,
                            &bootstrap_storage,
                            bootstrap_keyring.as_ref(),
                        )
                        .await
                        {
                            Ok(()) => return,
                            Err(error) => {
                                tracing::debug!(
                                    error = %format!("{error:#}"),
                                    "system realm bootstrap is waiting for its Raft assignment"
                                );
                                tokio::time::sleep(Duration::from_millis(250)).await;
                            }
                        }
                    }
                });
            } else {
                system_realm::ensure_bootstrapped(
                    &arc_config,
                    &persistence,
                    &storage,
                    secret_keyring.as_ref(),
                )
                .await
                .context("bootstrap system realm")?;
            }
        }
        if !distributed_coremeta_recovery {
            mvcc.start_background_work(core_store.clone(), observability.clone())
                .context("start MVCC background workers")?;
        }

        Ok(Self {
            persistence,
            storage,
            core_store,
            sharder,
            jwt_manager,
            region: arc_config.region.clone(),
            bucket_manager,
            object_manager,
            config: arc_config,
            secret_keyring,
            personaldb_signing_key_store,
            personaldb_protocol_keyring,
            personaldb_commit_locks,
            native_mutation_locks,
            observability,
            mvcc,
        })
    }

    pub async fn ensure_system_realm_bootstrapped(&self) -> Result<()> {
        system_realm::ensure_bootstrapped(
            &self.config,
            &self.persistence,
            &self.storage,
            self.secret_keyring.as_ref(),
        )
        .await
        .context("bootstrap system realm")
    }

    pub fn system_realm_is_bootstrapped(&self) -> Result<bool> {
        system_realm::bootstrap_marker_exists_in_runtime(
            self.mvcc.runtime.as_ref(),
            &system_realm::normalized_mesh_id(&self.config.mesh_id),
        )
    }
}

#[cfg(test)]
mod app_state_tests {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    #[test]
    fn only_first_ever_distributed_system_realm_bootstraps_before_coremeta_recovery() {
        assert!(bootstrap_system_realm_before_coremeta_recovery(3, false));
        assert!(!bootstrap_system_realm_before_coremeta_recovery(3, true));
        assert!(bootstrap_system_realm_before_coremeta_recovery(1, true));
    }

    #[tokio::test]
    async fn starts_without_personaldb_signing_keys() {
        let directory = tempfile::tempdir().unwrap();
        let config = Config {
            jwt_secret: "test-secret".to_string(),
            anvil_secret_encryption_key:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            public_api_addr: "127.0.0.1:0".to_string(),
            api_listen_addr: "127.0.0.1:0".to_string(),
            region: "local".to_string(),
            bootstrap_system_admin_subject_kind: "app".to_string(),
            bootstrap_system_admin_subject_id: "admin-principal".to_string(),
            storage_path: directory
                .path()
                .join("storage")
                .to_string_lossy()
                .into_owned(),
            ..Config::default()
        };

        let state = AppState::new(
            config,
            personaldb_signing::PersonalDbProtocolKeyring::disabled(),
        )
        .await
        .unwrap();

        assert!(!state.personaldb_protocol_keyring.is_enabled());
        assert!(
            state
                .personaldb_signing_key_store
                .list_public_records()
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn mvcc_object_job_publishes_all_frozen_index_kinds_before_completion() {
        let directory = tempfile::tempdir().unwrap();
        let config = Config {
            jwt_secret: "test-secret".to_string(),
            anvil_secret_encryption_key:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            public_api_addr: "127.0.0.1:0".to_string(),
            api_listen_addr: "127.0.0.1:0".to_string(),
            region: "local".to_string(),
            node_id: "node-a".to_string(),
            bootstrap_system_admin_subject_kind: "app".to_string(),
            bootstrap_system_admin_subject_id: "admin-principal".to_string(),
            allow_test_only_embedding_provider: true,
            storage_path: directory
                .path()
                .join("storage")
                .to_string_lossy()
                .into_owned(),
            ..Config::default()
        };
        let state = AppState::new(
            config.clone(),
            personaldb_signing::PersonalDbProtocolKeyring::disabled(),
        )
        .await
        .unwrap();
        crate::test_support::install_active_test_topology(&state.persistence, &config)
            .await
            .unwrap();
        state.persistence.create_region("local").await.unwrap();
        let tenant = state
            .persistence
            .create_tenant("mvcc-index-test", "mvcc-index-test")
            .await
            .unwrap();
        let bucket = state
            .persistence
            .create_bucket(tenant.id, "documents", "local")
            .await
            .unwrap();
        let claims = auth::Claims {
            sub: "test-app".to_string(),
            exp: usize::MAX,
            tenant_id: tenant.id,
            jti: None,
        };
        access_control::grant_storage_tenant_owner(
            &state.persistence,
            tenant.id,
            &claims.sub,
            "test",
            "materialisation e2e",
        )
        .await
        .unwrap();
        access_control::grant_bucket_defaults(
            &state.persistence,
            &bucket,
            &claims.sub,
            "test",
            "materialisation e2e",
        )
        .await
        .unwrap();
        let definitions = [
            persistence::IndexDefinitionMutation::Create {
                name: "typed".into(),
                kind: "typed_json".into(),
                selector: serde_json::Value::Null,
                extractor: serde_json::Value::Null,
                authorization_mode: "inherit_object".into(),
                build_policy: serde_json::json!({
                    "source_kind": "object_current",
                    "fields": [{"name": "title", "extractor": "/title"}],
                }),
            },
            persistence::IndexDefinitionMutation::Create {
                name: "text".into(),
                kind: "full_text".into(),
                selector: serde_json::Value::Null,
                extractor: serde_json::json!({
                    "fields": [{"source": "object_body_utf8"}],
                }),
                authorization_mode: "inherit_object".into(),
                build_policy: serde_json::json!({}),
            },
            persistence::IndexDefinitionMutation::Create {
                name: "vector".into(),
                kind: "vector".into(),
                selector: serde_json::Value::Null,
                extractor: serde_json::json!({"kind": "object_body_utf8"}),
                authorization_mode: "inherit_object".into(),
                build_policy: serde_json::json!({
                    "schema": formats::vector::VECTOR_INDEX_SCHEMA,
                    "source": {"kind": "object_current", "prefix": ""},
                    "extractor": {"kind": "object_body_utf8"},
                    "embedding": {
                        "provider": "test_only",
                        "model": "test",
                        "dimension": 4,
                        "modality": "text",
                        "normalisation": "unit_l2",
                        "chunking": {"strategy": "whole_object"}
                    },
                    "ann": {"algorithm": "hnsw", "metric": "cosine"}
                }),
            },
        ];
        for definition in definitions {
            let outcome = state
                .persistence
                .apply_index_definition_mutation(&bucket, &definition, None, None)
                .await
                .unwrap();
            assert!(matches!(
                outcome,
                persistence::IndexDefinitionMutationOutcome::Published { .. }
            ));
        }

        let principal = object_manager::transaction_principal_from_claims(&claims);
        let handle = state
            .mvcc
            .open_transactions
            .begin(
                state.mvcc.runtime.as_ref(),
                state.mvcc.cluster_id().to_string(),
                &principal,
                "materialisation-e2e",
                Duration::from_secs(60),
                mvcc_transaction::DurabilityLevel::Local,
                mvcc_transaction::ReadConsistency::LocalSnapshot,
                now_ms(),
            )
            .await
            .unwrap();
        let object = state
            .object_manager
            .put_object(
                &claims,
                &bucket.name,
                "document.json",
                futures_util::stream::iter(vec![Ok(
                    br#"{"title":"MVCC materialisation document"}"#.to_vec(),
                )]),
                object_manager::ObjectWriteOptions {
                    content_type: Some("application/json".into()),
                    transaction_id: Some(handle.transaction_id.clone()),
                    transaction_principal: Some(principal.clone()),
                    visibility: object_manager::ObjectWriteVisibility::strict(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let commit = state
            .mvcc
            .open_transactions
            .commit(
                state.mvcc.runtime.as_ref(),
                &handle.transaction_id,
                &principal,
                now_ms(),
            )
            .await
            .unwrap();
        assert!(matches!(
            commit.certification,
            mvcc_transaction::CertificationResult::Committed { .. }
        ));

        let target = format!(
            "tenant/{}/bucket/{}/object/{}/version/{}",
            tenant.id, bucket.id, object.key, object.version_id
        );
        let status_key = object_materialisation::materialisation_status_key(&target).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if let Some(row) = state
                    .mvcc
                    .runtime
                    .local_store()
                    .read_latest(&status_key)
                    .unwrap()
                {
                    let result: object_materialisation::ObjectMaterialisationResult =
                        serde_json::from_slice(&row.value).unwrap();
                    assert_eq!(result.canonical_bytes().unwrap(), row.value);
                    if result.state == object_materialisation::ObjectMaterialisationState::Complete
                    {
                        break result;
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            let pending = state
                .mvcc
                .runtime
                .local_store()
                .read_latest(&status_key)
                .unwrap()
                .map(|row| {
                    serde_json::from_slice::<object_materialisation::ObjectMaterialisationResult>(
                        &row.value,
                    )
                    .unwrap()
                });
            let record = pending.as_ref().and_then(|result| {
                state
                    .mvcc
                    .runtime
                    .local_store()
                    .object_materialisation_record(&result.job_id)
                    .unwrap()
            });
            panic!("materialisation timed out; pending={pending:?}; queue_record={record:?}")
        });
        assert_eq!(
            result.state,
            object_materialisation::ObjectMaterialisationState::Complete
        );
        let outcomes = result.index_marker["outcomes"].as_array().unwrap();
        assert_eq!(outcomes.len(), 3);
        assert!(outcomes.iter().all(|outcome| {
            outcome["segment_hashes"]
                .as_array()
                .is_some_and(|hashes| !hashes.is_empty())
        }));

        let unsupported = persistence::IndexDefinitionMutation::Create {
            name: "unsupported".into(),
            kind: "future_index_kind".into(),
            selector: serde_json::Value::Null,
            extractor: serde_json::Value::Null,
            authorization_mode: "inherit_object".into(),
            build_policy: serde_json::json!({}),
        };
        assert!(matches!(
            state
                .persistence
                .apply_index_definition_mutation(&bucket, &unsupported, None, None)
                .await
                .unwrap(),
            persistence::IndexDefinitionMutationOutcome::Published { .. }
        ));
        let unsupported_handle = state
            .mvcc
            .open_transactions
            .begin(
                state.mvcc.runtime.as_ref(),
                state.mvcc.cluster_id().to_string(),
                &principal,
                "materialisation-unsupported",
                Duration::from_secs(60),
                mvcc_transaction::DurabilityLevel::Local,
                mvcc_transaction::ReadConsistency::LocalSnapshot,
                now_ms(),
            )
            .await
            .unwrap();
        let unsupported_object = state
            .object_manager
            .put_object(
                &claims,
                &bucket.name,
                "unsupported.json",
                futures_util::stream::iter(vec![Ok(br#"{"title":"pending"}"#.to_vec())]),
                object_manager::ObjectWriteOptions {
                    content_type: Some("application/json".into()),
                    transaction_id: Some(unsupported_handle.transaction_id.clone()),
                    transaction_principal: Some(principal.clone()),
                    visibility: object_manager::ObjectWriteVisibility::strict(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        state
            .mvcc
            .open_transactions
            .commit(
                state.mvcc.runtime.as_ref(),
                &unsupported_handle.transaction_id,
                &principal,
                now_ms(),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let unsupported_target = format!(
            "tenant/{}/bucket/{}/object/{}/version/{}",
            tenant.id, bucket.id, unsupported_object.key, unsupported_object.version_id
        );
        let unsupported_status =
            object_materialisation::materialisation_status_key(&unsupported_target).unwrap();
        let unsupported_row = state
            .mvcc
            .runtime
            .local_store()
            .read_latest(&unsupported_status)
            .unwrap()
            .expect("unsupported job retains its pending status");
        let unsupported_result: object_materialisation::ObjectMaterialisationResult =
            serde_json::from_slice(&unsupported_row.value).unwrap();
        assert_eq!(
            unsupported_result.state,
            object_materialisation::ObjectMaterialisationState::Pending
        );
        assert!(
            state
                .mvcc
                .runtime
                .local_store()
                .has_incomplete_object_materialisations()
                .unwrap()
        );
    }

    #[tokio::test]
    async fn distributed_startup_leaves_region_creation_to_topology_bootstrap() {
        let directory = tempfile::tempdir().unwrap();
        let config = Config {
            jwt_secret: "test-secret".to_string(),
            anvil_secret_encryption_key:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            public_api_addr: "127.0.0.1:0".to_string(),
            api_listen_addr: "127.0.0.1:0".to_string(),
            region: "distributed-region".to_string(),
            node_id: "node-a".to_string(),
            bootstrap_system_admin_subject_kind: "app".to_string(),
            bootstrap_system_admin_subject_id: "admin-principal".to_string(),
            mvcc_peers_json: serde_json::to_string(&[
                serde_json::json!({
                    "cluster_id": "default",
                    "raft_node_id": 1,
                    "node_id": "node-a",
                    "incarnation": 1,
                    "endpoint": "http://127.0.0.1:50051",
                    "failure_domain": "zone-a",
                    "voter": true,
                }),
                serde_json::json!({
                    "cluster_id": "default",
                    "raft_node_id": 2,
                    "node_id": "node-b",
                    "incarnation": 1,
                    "endpoint": "http://127.0.0.1:50052",
                    "failure_domain": "zone-b",
                    "voter": true,
                }),
            ])
            .unwrap(),
            mvcc_bootstrap_membership: false,
            allow_test_only_insecure_mvcc_transport: true,
            storage_path: directory
                .path()
                .join("storage")
                .to_string_lossy()
                .into_owned(),
            ..Config::default()
        };

        let state = AppState::new(
            config,
            personaldb_signing::PersonalDbProtocolKeyring::disabled(),
        )
        .await
        .unwrap();

        assert!(state.persistence.list_regions().await.unwrap().is_empty());
    }

    #[test]
    fn distributed_mvcc_quorum_certifies_replicates_and_applies_in_order() {
        std::thread::Builder::new()
            .name("distributed-mvcc-e2e-test".to_string())
            .stack_size(16 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(6)
                    .thread_stack_size(16 * 1024 * 1024)
                    .build()
                    .unwrap()
                    .block_on(
                        distributed_mvcc_quorum_certifies_replicates_and_applies_in_order_body(),
                    )
            })
            .unwrap()
            .join()
            .unwrap();
    }

    async fn distributed_mvcc_quorum_certifies_replicates_and_applies_in_order_body() {
        use crate::anvil_api::{
            BoundaryDimension, BoundarySource, PutBoundarySchemaRequest,
            consensus_transport_server::ConsensusTransportServer,
            object_service_server::ObjectService as _,
            replication_service_server::ReplicationServiceServer,
        };
        use crate::bundle_replication::BundleTargetStream as _;
        use crate::mvcc_transaction::{DurabilityLevel, LogicalKey, ReadConsistency};
        use anvil_mvcc_consensus::Consensus as _;
        use sha2::Digest as _;
        use tokio::net::TcpListener;
        use tokio_stream::wrappers::TcpListenerStream;
        use tonic::transport::Server;

        let directories = [
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        ];
        let mut listeners = vec![
            Some(TcpListener::bind("127.0.0.1:0").await.unwrap()),
            Some(TcpListener::bind("127.0.0.1:0").await.unwrap()),
            Some(TcpListener::bind("127.0.0.1:0").await.unwrap()),
        ];
        let endpoints = listeners
            .iter()
            .map(|listener| {
                format!(
                    "http://{}",
                    listener.as_ref().unwrap().local_addr().unwrap()
                )
            })
            .collect::<Vec<_>>();
        let peers = endpoints
            .iter()
            .enumerate()
            .map(|(index, endpoint)| {
                serde_json::json!({
                    "cluster_id": "distributed-e2e",
                    "raft_node_id": index + 1,
                    "node_id": format!("node-{}", index + 1),
                    "incarnation": 1,
                    "endpoint": endpoint,
                    "failure_domain": format!("zone-{}", index + 1),
                    "voter": true,
                })
            })
            .collect::<Vec<_>>();
        let peers_json = serde_json::to_string(&peers).unwrap();
        let mut configs = Vec::new();
        for (index, directory) in directories.iter().enumerate() {
            configs.push(Config {
                jwt_secret: "test-secret".into(),
                anvil_secret_encryption_key:
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                public_api_addr: "127.0.0.1:0".into(),
                api_listen_addr: "127.0.0.1:0".into(),
                region: "distributed".into(),
                node_id: format!("node-{}", index + 1),
                bootstrap_system_admin_subject_kind: "app".into(),
                bootstrap_system_admin_subject_id: "admin-principal".into(),
                allow_test_only_embedding_provider: true,
                bootstrap_node_ids: vec!["node-1".into(), "node-2".into(), "node-3".into()],
                storage_path: directory
                    .path()
                    .join("storage")
                    .to_string_lossy()
                    .into_owned(),
                mvcc_cluster_id: "distributed-e2e".into(),
                mvcc_raft_node_id: index as u64 + 1,
                mvcc_node_incarnation: 1,
                mvcc_failure_domain: format!("zone-{}", index + 1),
                mvcc_peers_json: peers_json.clone(),
                mvcc_bootstrap_membership: index == 0,
                mvcc_bundle_quorum_holders: 2,
                mvcc_prepared_bundle_gc_grace_ms: 86_400_000,
                mvcc_tolerated_failure_domains: 1,
                mvcc_rpc_timeout_ms: 5_000,
                ..Config::default()
            });
        }
        // Followers must be serving before the bootstrap node initializes the
        // three-voter membership. AppState initialization on node 1 waits for
        // that membership to elect it and install the initial control state.
        let mut states = (0..3).map(|_| None).collect::<Vec<_>>();
        let mut servers = (0..3).map(|_| None).collect::<Vec<_>>();
        for index in [1_usize, 2, 0] {
            let state = AppState::new(
                configs[index].clone(),
                personaldb_signing::PersonalDbProtocolKeyring::disabled(),
            )
            .await
            .unwrap();
            let consensus = state.mvcc.consensus_service.clone();
            let replication = state.mvcc.replication_service.clone();
            let listener = listeners[index].take().unwrap();
            servers[index] = Some(tokio::spawn(async move {
                Server::builder()
                    .add_service(ConsensusTransportServer::new(consensus))
                    .add_service(ReplicationServiceServer::new(replication))
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
                    .unwrap();
            }));
            states[index] = Some(state);
        }
        let states = states.into_iter().map(Option::unwrap).collect::<Vec<_>>();
        let mut servers = servers.into_iter().map(Option::unwrap).collect::<Vec<_>>();
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if states[0]
                    .mvcc
                    .consensus
                    .linearized_read_barrier()
                    .await
                    .is_ok()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("node one becomes leader");
        crate::test_support::install_active_test_topology(&states[0].persistence, &configs[0])
            .await
            .unwrap();

        let principal = "distributed-e2e-principal";
        let key = LogicalKey {
            table_id: 0x7f01,
            application_key: b"ordered/quorum".to_vec(),
        };
        let handle = states[0]
            .mvcc
            .open_transactions
            .begin(
                states[0].mvcc.runtime.as_ref(),
                "distributed-e2e",
                principal,
                "quorum-write",
                Duration::from_secs(30),
                DurabilityLevel::Quorum,
                ReadConsistency::Linearized,
                now_ms(),
            )
            .await
            .unwrap();
        states[0]
            .mvcc
            .open_transactions
            .put(
                &handle.transaction_id,
                "distributed-e2e",
                key.clone(),
                b"replicated-value".to_vec(),
                now_ms(),
            )
            .unwrap();
        let outcome = states[0]
            .mvcc
            .open_transactions
            .commit(
                states[0].mvcc.runtime.as_ref(),
                &handle.transaction_id,
                principal,
                now_ms(),
            )
            .await
            .unwrap();
        assert!(matches!(
            outcome.certification,
            mvcc_transaction::CertificationResult::Committed { .. }
        ));
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if states[1]
                    .mvcc
                    .runtime
                    .local_store()
                    .read_latest(&key)
                    .unwrap()
                    .is_some_and(|row| row.value == b"replicated-value")
                    && states[2]
                        .mvcc
                        .runtime
                        .local_store()
                        .read_latest(&key)
                        .unwrap()
                        .is_some_and(|row| row.value == b"replicated-value")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("followers apply the certified bundle in order");

        states[0]
            .persistence
            .create_region("distributed")
            .await
            .unwrap();
        let tenant = states[0]
            .persistence
            .create_tenant("distributed-e2e", "distributed-e2e")
            .await
            .unwrap();
        let bucket = states[0]
            .persistence
            .create_bucket(tenant.id, "materialised", "distributed")
            .await
            .unwrap();
        let claims = auth::Claims {
            sub: "distributed-test-app".into(),
            exp: usize::MAX,
            tenant_id: tenant.id,
            jti: None,
        };
        access_control::grant_storage_tenant_owner(
            &states[0].persistence,
            tenant.id,
            &claims.sub,
            "test",
            "distributed materialisation",
        )
        .await
        .unwrap();
        access_control::grant_bucket_defaults(
            &states[0].persistence,
            &bucket,
            &claims.sub,
            "test",
            "distributed materialisation",
        )
        .await
        .unwrap();
        for definition in [
            persistence::IndexDefinitionMutation::Create {
                name: "typed".into(),
                kind: "typed_json".into(),
                selector: serde_json::Value::Null,
                extractor: serde_json::Value::Null,
                authorization_mode: "inherit_object".into(),
                build_policy: serde_json::json!({
                    "source_kind": "object_current",
                    "fields": [{"name": "title", "extractor": "/title"}],
                }),
            },
            persistence::IndexDefinitionMutation::Create {
                name: "text".into(),
                kind: "full_text".into(),
                selector: serde_json::Value::Null,
                extractor: serde_json::json!({
                    "fields": [{"source": "object_body_utf8"}],
                }),
                authorization_mode: "inherit_object".into(),
                build_policy: serde_json::json!({}),
            },
            persistence::IndexDefinitionMutation::Create {
                name: "vector".into(),
                kind: "vector".into(),
                selector: serde_json::Value::Null,
                extractor: serde_json::json!({"kind": "object_body_utf8"}),
                authorization_mode: "inherit_object".into(),
                build_policy: serde_json::json!({
                    "schema": formats::vector::VECTOR_INDEX_SCHEMA,
                    "source": {"kind": "object_current", "prefix": ""},
                    "extractor": {"kind": "object_body_utf8"},
                    "embedding": {
                        "provider": "test_only",
                        "model": "test",
                        "dimension": 4,
                        "modality": "text",
                        "normalisation": "unit_l2",
                        "chunking": {"strategy": "whole_object"}
                    },
                    "ann": {"algorithm": "hnsw", "metric": "cosine"}
                }),
            },
        ] {
            states[0]
                .persistence
                .apply_index_definition_mutation(&bucket, &definition, None, None)
                .await
                .unwrap();
        }
        let mut boundary_schema_request = tonic::Request::new(PutBoundarySchemaRequest {
            bucket_name: bucket.name.clone(),
            expected_generation: None,
            dimensions: vec![BoundaryDimension {
                name: "partition".into(),
                source: Some(BoundarySource {
                    kind: "user_metadata_json_pointer".into(),
                    value: "/partition".into(),
                    max_body_bytes: 0,
                }),
                value_type: "string".into(),
                categories: vec!["storage_partition".into()],
                required: true,
                cardinality: "low".into(),
                max_values_per_block: 1,
                placement_affinity: "prefer_colocate".into(),
                compaction_scope: "require_same_value".into(),
                shared_ranges_allowed: false,
                shared_record_kinds: Vec::new(),
                deprecated: false,
            }],
            mutation_id: "distributed-boundary-schema".into(),
            transaction_id: None,
        });
        boundary_schema_request
            .extensions_mut()
            .insert(claims.clone());
        let boundary_schema = states[0]
            .put_boundary_schema(boundary_schema_request)
            .await
            .unwrap()
            .into_inner()
            .schema
            .expect("public boundary-schema write returns the committed schema");
        assert_eq!(boundary_schema.bucket_name, bucket.name);
        assert_eq!(boundary_schema.generation, 1);
        assert_eq!(boundary_schema.dimensions.len(), 1);

        let erasure_key = LogicalKey {
            table_id: 0x7f01,
            application_key: b"ordered/erasure".to_vec(),
        };
        let erasure_handle = states[0]
            .mvcc
            .open_transactions
            .begin(
                states[0].mvcc.runtime.as_ref(),
                "distributed-e2e",
                principal,
                "erasure-write",
                Duration::from_secs(30),
                DurabilityLevel::Erasure,
                ReadConsistency::Linearized,
                now_ms(),
            )
            .await
            .unwrap();
        let payload = br#"{"title":"distributed erasure payload"}"#;
        let materialised_object = states[0]
            .object_manager
            .put_object(
                &claims,
                &bucket.name,
                "distributed.json",
                futures_util::stream::iter(vec![Ok(payload.to_vec())]),
                object_manager::ObjectWriteOptions {
                    content_type: Some("application/json".into()),
                    user_metadata: Some(serde_json::json!({"partition": "alpha"})),
                    transaction_id: Some(erasure_handle.transaction_id.clone()),
                    transaction_principal: Some(principal.into()),
                    visibility: object_manager::ObjectWriteVisibility::strict(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let materialised_shard_map = materialised_object.shard_map.clone().unwrap();
        assert_eq!(
            materialised_shard_map["schema"],
            "anvil.mvcc.object_shard_manifest.v1"
        );
        let manifest: object_shard_manifest::PhysicalObjectShardManifest =
            serde_json::from_value(materialised_shard_map["manifest"].clone()).unwrap();
        assert_eq!(manifest.data_shards, 2);
        assert_eq!(manifest.parity_shards, 1);
        assert_eq!(manifest.placements.len(), 3);
        states[0]
            .mvcc
            .open_transactions
            .put(
                &erasure_handle.transaction_id,
                "distributed-e2e",
                erasure_key.clone(),
                b"erasure-certified".to_vec(),
                now_ms(),
            )
            .unwrap();
        let erasure_outcome = states[0]
            .mvcc
            .open_transactions
            .commit(
                states[0].mvcc.runtime.as_ref(),
                &erasure_handle.transaction_id,
                principal,
                now_ms(),
            )
            .await
            .unwrap();
        assert!(matches!(
            erasure_outcome.certification,
            mvcc_transaction::CertificationResult::Committed { .. }
        ));
        let materialisation_target = format!(
            "tenant/{}/bucket/{}/object/{}/version/{}",
            tenant.id, bucket.id, materialised_object.key, materialised_object.version_id
        );
        let materialisation_key =
            object_materialisation::materialisation_status_key(&materialisation_target).unwrap();
        let materialisation = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some(row) = states[0]
                    .mvcc
                    .runtime
                    .local_store()
                    .read_latest(&materialisation_key)
                    .unwrap()
                {
                    let result: object_materialisation::ObjectMaterialisationResult =
                        serde_json::from_slice(&row.value).unwrap();
                    if result.state == object_materialisation::ObjectMaterialisationState::Complete
                    {
                        break result;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            let pending = states[0]
                .mvcc
                .runtime
                .local_store()
                .read_latest(&materialisation_key)
                .unwrap()
                .map(|row| {
                    serde_json::from_slice::<object_materialisation::ObjectMaterialisationResult>(
                        &row.value,
                    )
                    .unwrap()
                });
            let records = states
                .iter()
                .map(|state| {
                    pending.as_ref().and_then(|result| {
                        state
                            .mvcc
                            .runtime
                            .local_store()
                            .object_materialisation_record(&result.job_id)
                            .unwrap()
                    })
                })
                .collect::<Vec<_>>();
            let control = states
                .iter()
                .map(|state| state.mvcc.consensus.applied_control_snapshot().unwrap())
                .collect::<Vec<_>>();
            panic!(
                "distributed materialisation timed out; pending={pending:?}; \
                 queue_records={records:?}; control={control:?}"
            )
        });
        assert_eq!(
            materialisation.index_marker["outcomes"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
        assert!(
            materialisation
                .derived_boundaries
                .as_array()
                .is_some_and(|values| {
                    values
                        .iter()
                        .any(|value| value["name"] == "partition" && value["value"] == "alpha")
                })
        );
        servers[2].abort();
        let _ = (&mut servers[2]).await;
        let reconstructed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        manifest
            .read_range_chunks(
                &states[0].mvcc.replication_client,
                0,
                manifest.object_length,
                {
                    let reconstructed = reconstructed.clone();
                    move |chunk| {
                        let reconstructed = reconstructed.clone();
                        async move {
                            reconstructed.lock().unwrap().extend_from_slice(&chunk);
                            Ok(())
                        }
                    }
                },
            )
            .await
            .unwrap();
        assert_eq!(&*reconstructed.lock().unwrap(), payload);
        let restarted_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let restarted_address = restarted_listener.local_addr().unwrap();
        let consensus = states[2].mvcc.consensus_service.clone();
        let replication = states[2].mvcc.replication_service.clone();
        servers[2] = tokio::spawn(async move {
            Server::builder()
                .add_service(ConsensusTransportServer::new(consensus))
                .add_service(ReplicationServiceServer::new(replication))
                .serve_with_incoming(TcpListenerStream::new(restarted_listener))
                .await
                .unwrap();
        });
        let restarted_endpoint = format!("http://{restarted_address}");
        states[0]
            .mvcc
            .replication_client
            .replace_peer_endpoint(
                "distributed-e2e",
                &mvcc_transaction::NodeIncarnation {
                    node_id: "node-3".into(),
                    incarnation: 1,
                },
                restarted_endpoint.clone(),
            )
            .await
            .unwrap();
        states[0]
            .mvcc
            .consensus
            .change_membership(
                [
                    anvil_mvcc_consensus::NodeId(1),
                    anvil_mvcc_consensus::NodeId(2),
                ]
                .into_iter()
                .collect(),
                false,
            )
            .await
            .unwrap();
        states[0]
            .mvcc
            .consensus
            .add_learner(
                anvil_mvcc_consensus::NodeId(3),
                anvil_mvcc_consensus::ConsensusNode {
                    address: restarted_endpoint,
                },
                true,
            )
            .await
            .unwrap();
        states[0]
            .mvcc
            .consensus
            .change_membership(
                [
                    anvil_mvcc_consensus::NodeId(1),
                    anvil_mvcc_consensus::NodeId(2),
                    anvil_mvcc_consensus::NodeId(3),
                ]
                .into_iter()
                .collect(),
                false,
            )
            .await
            .unwrap();
        let reconnect_key = LogicalKey {
            table_id: 0x7f01,
            application_key: b"ordered/reconnect".to_vec(),
        };
        let reconnect_handle = states[0]
            .mvcc
            .open_transactions
            .begin(
                states[0].mvcc.runtime.as_ref(),
                "distributed-e2e",
                principal,
                "reconnect-write",
                Duration::from_secs(30),
                DurabilityLevel::Quorum,
                ReadConsistency::Linearized,
                now_ms(),
            )
            .await
            .unwrap();
        states[0]
            .mvcc
            .open_transactions
            .put(
                &reconnect_handle.transaction_id,
                "distributed-e2e",
                reconnect_key.clone(),
                b"after-reconnect".to_vec(),
                now_ms(),
            )
            .unwrap();
        states[0]
            .mvcc
            .open_transactions
            .commit(
                states[0].mvcc.runtime.as_ref(),
                &reconnect_handle.transaction_id,
                principal,
                now_ms(),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if states[2]
                    .mvcc
                    .runtime
                    .local_store()
                    .read_latest(&reconnect_key)
                    .unwrap()
                    .is_some_and(|row| row.value == b"after-reconnect")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("reconnected follower applies the next certified bundle");
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if states[1]
                    .mvcc
                    .runtime
                    .local_store()
                    .read_latest(&erasure_key)
                    .unwrap()
                    .is_some_and(|row| row.value == b"erasure-certified")
                    && states[2]
                        .mvcc
                        .runtime
                        .local_store()
                        .read_latest(&erasure_key)
                        .unwrap()
                        .is_some_and(|row| row.value == b"erasure-certified")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("followers apply erasure-certified transaction");

        let foreign_target = bundle_replication::BundleTarget {
            cluster_id: "foreign-cluster".into(),
            node: mvcc_transaction::NodeIncarnation {
                node_id: "node-2".into(),
                incarnation: 1,
            },
            failure_domain: "zone-2".into(),
            voter: true,
        };
        let foreign_error = states[0]
            .mvcc
            .replication_client
            .send_bundle(
                &foreign_target,
                &mvcc_transaction::BundleIdentity {
                    hash: format!(
                        "sha256:{}",
                        hex::encode(sha2::Sha256::digest(b"foreign-bundle"))
                    ),
                    length: 14,
                },
                b"foreign-bundle",
            )
            .await
            .unwrap_err();
        assert!(foreign_error.to_string().contains("cross-cluster"));

        for server in servers {
            server.abort();
        }
    }
}
