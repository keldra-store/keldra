use clap::Parser;

use crate::routing::CrossRegionRoutingPolicy;
use anyhow::Result;

/// A distributed storage and compute system.
#[derive(Parser, Debug, Clone, PartialEq, Eq)]
#[command(version, about, long_about = None)]
pub struct Config {
    /// The secret key used for signing JWTs.
    #[arg(long, env)]
    pub jwt_secret: String,

    /// Active hex-encoded 32-byte key used for server-side secret encryption.
    #[arg(long, env)]
    pub anvil_secret_encryption_key: String,

    /// Identifier written into new encrypted secret envelopes.
    #[arg(long, env, default_value = "primary")]
    pub anvil_secret_encryption_key_id: String,

    /// Comma-delimited previous secret encryption keys as `key_id:hex`.
    #[arg(long, env, default_value = "")]
    pub anvil_secret_encryption_previous_keys: String,

    /// Bearer token used by this node when it calls another node's internal
    /// CoreStore services. Empty disables remote internal writes; a multi-node
    /// placement will fail rather than silently degrading to local-only storage.
    #[arg(long, env, default_value = "")]
    pub corestore_internal_bearer_token: String,

    /// The publicly reachable gRPC address for this node.
    #[arg(long, env)]
    pub public_api_addr: String,

    /// The address to bind the main gRPC service to.
    #[arg(long, env, default_value = "0.0.0.0:50051")]
    pub api_listen_addr: String,

    /// The address to bind the administrative gRPC service to.
    #[arg(long, env, default_value = "127.0.0.1:50052")]
    pub admin_listen_addr: String,

    /// Explicitly allow binding the private admin plane to a non-loopback address.
    #[arg(long, env, default_value_t = false)]
    pub allow_public_admin_listener: bool,

    /// Stable mesh identifier for administrative and lifecycle records.
    #[arg(long, env, default_value = "default")]
    pub mesh_id: String,

    /// First-boot system-realm admin app name. Used only when the system realm is absent.
    #[arg(long, env, default_value = "")]
    pub bootstrap_system_admin_app_name: String,

    /// File path where the first-boot admin app credential JSON is written.
    #[arg(long, env, default_value = "")]
    pub bootstrap_system_admin_credential_output_path: String,

    /// Existing first-boot admin subject kind. Used instead of creating an app.
    #[arg(long, env, default_value = "")]
    pub bootstrap_system_admin_subject_kind: String,

    /// Existing first-boot admin subject id. Used instead of creating an app.
    #[arg(long, env, default_value = "")]
    pub bootstrap_system_admin_subject_id: String,

    /// Node principals admitted during mesh genesis. These principals receive
    /// only the system-realm relation required for authenticated node RPCs.
    #[arg(long, env, use_value_delimiter = true, value_delimiter = ',')]
    pub bootstrap_node_ids: Vec<String>,

    /// The current region this node is operating in.
    #[arg(long, env)]
    pub region: String,

    /// The current cell this node is operating in.
    #[arg(long, env, default_value = "default")]
    pub cell_id: String,

    /// Region host suffix advertised for virtual-host routing.
    #[arg(long, env, default_value = "")]
    pub public_region_base_domain: String,

    /// Trusted proxy source IPs or CIDR ranges allowed to supply forwarded request metadata.
    #[arg(long, env, use_value_delimiter = true, value_delimiter = ',')]
    pub trusted_proxy_source_ranges: Vec<String>,

    /// Policy for requests whose bucket locator is owned by another region.
    #[arg(long, env, default_value_t = CrossRegionRoutingPolicy::RedirectPreferred)]
    pub cross_region_routing_policy: CrossRegionRoutingPolicy,

    /// Stable node id. When supplied for a new volume, it becomes the persisted
    /// identity; subsequent starts must supply the same value or omit it.
    #[arg(long, env, default_value = "")]
    pub node_id: String,

    /// Directory used for Anvil-owned object bytes, metadata journals, indexes, and manifests.
    #[arg(long, env, default_value = "anvil-data")]
    pub storage_path: String,

    /// PersonalDB entries committed after the latest snapshot before building another snapshot.
    #[arg(long, env, default_value_t = 1024)]
    pub personaldb_snapshot_entry_threshold: u64,

    /// PersonalDB changeset payload bytes committed after the latest snapshot before building another snapshot.
    #[arg(long, env, default_value_t = 64 * 1024 * 1024)]
    pub personaldb_snapshot_payload_bytes_threshold: u64,

    /// Enables the deterministic vector embedding provider used only by test harnesses.
    #[arg(skip)]
    pub allow_test_only_embedding_provider: bool,

    /// JSON object defining named production vector embedding providers.
    #[arg(long, env, default_value = "")]
    pub vector_embedding_providers_json: String,

    /// Uncompacted object metadata journal frames allowed before scheduling compaction.
    #[arg(long, env, default_value_t = 4096)]
    pub object_metadata_compaction_frame_threshold: u64,

    /// Uncompacted object metadata journal bytes allowed before scheduling compaction.
    #[arg(long, env, default_value_t = 64 * 1024 * 1024)]
    pub object_metadata_compaction_bytes_threshold: u64,

    /// Run the in-process background worker loop for tasks such as compaction and index builds.
    #[arg(long, env, default_value_t = true)]
    pub run_background_worker: bool,

    /// Maximum number of background tasks executed concurrently by this node.
    #[arg(
        long,
        env,
        default_value_t = 4,
        value_parser = parse_positive_usize
    )]
    pub background_worker_concurrency: usize,

    /// Seconds that an in-process background task lease remains valid without renewal.
    #[arg(long, env, default_value_t = 300)]
    pub task_lease_ttl_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            jwt_secret: String::new(),
            anvil_secret_encryption_key: String::new(),
            anvil_secret_encryption_key_id: "primary".to_string(),
            anvil_secret_encryption_previous_keys: String::new(),
            corestore_internal_bearer_token: String::new(),
            public_api_addr: String::new(),
            api_listen_addr: "0.0.0.0:50051".to_string(),
            admin_listen_addr: "127.0.0.1:50052".to_string(),
            allow_public_admin_listener: false,
            mesh_id: "default".to_string(),
            bootstrap_system_admin_app_name: String::new(),
            bootstrap_system_admin_credential_output_path: String::new(),
            bootstrap_system_admin_subject_kind: String::new(),
            bootstrap_system_admin_subject_id: String::new(),
            bootstrap_node_ids: Vec::new(),
            region: String::new(),
            cell_id: "default".to_string(),
            public_region_base_domain: String::new(),
            trusted_proxy_source_ranges: Vec::new(),
            cross_region_routing_policy: CrossRegionRoutingPolicy::RedirectPreferred,
            node_id: String::new(),
            storage_path: "anvil-data".to_string(),
            personaldb_snapshot_entry_threshold: 1024,
            personaldb_snapshot_payload_bytes_threshold: 64 * 1024 * 1024,
            allow_test_only_embedding_provider: false,
            vector_embedding_providers_json: String::new(),
            object_metadata_compaction_frame_threshold: 4096,
            object_metadata_compaction_bytes_threshold: 64 * 1024 * 1024,
            run_background_worker: true,
            background_worker_concurrency: 4,
            task_lease_ttl_secs: 300,
        }
    }
}

fn parse_positive_usize(value: &str) -> std::result::Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| "value must be a positive integer".to_string())?;
    if parsed == 0 {
        return Err("value must be greater than zero".into());
    }
    Ok(parsed)
}

impl Config {
    #[allow(unused)]
    pub fn from_ref(args: &Self) -> Self {
        let mut me = Self::default();
        args.clone_into(&mut me);
        me
    }

    /// Whether this node participates in a multi-node CoreMeta topology.
    ///
    /// Distributed topology is installed through the administrative bootstrap
    /// path before data-plane readiness. A node restart must not try to mutate
    /// that topology while constructing its local application state.
    pub fn requires_distributed_coremeta_recovery(&self) -> bool {
        self.bootstrap_node_ids.len() > 1
    }

    pub fn secret_keyring(&self) -> Result<crate::crypto::EncryptionKeyring> {
        let active_key_id = self.active_encryption_key_id();
        crate::crypto::EncryptionKeyring::from_hex_config(
            active_key_id,
            &self.anvil_secret_encryption_key,
            &self.anvil_secret_encryption_previous_keys,
        )
    }

    pub fn core_pipeline_keyring(&self) -> Result<crate::core_store::CorePipelineKeyring> {
        let active_key_id = self.active_encryption_key_id();
        crate::core_store::CorePipelineKeyring::from_hex_config(
            active_key_id,
            &self.anvil_secret_encryption_key,
            &self.anvil_secret_encryption_previous_keys,
        )
    }

    fn active_encryption_key_id(&self) -> &str {
        if self.anvil_secret_encryption_key_id.trim().is_empty() {
            "primary"
        } else {
            &self.anvil_secret_encryption_key_id
        }
    }

    pub fn validate_admin_listener_bind(&self) -> Result<()> {
        let addr: std::net::SocketAddr = self.admin_listen_addr.parse()?;
        if !self.allow_public_admin_listener && !addr.ip().is_loopback() {
            anyhow::bail!(
                "ADMIN_LISTEN_ADDR={} is not loopback; set ALLOW_PUBLIC_ADMIN_LISTENER=true only when the admin port is protected by private networking",
                self.admin_listen_addr
            );
        }
        Ok(())
    }

    pub async fn with_persisted_identity(mut self) -> Result<Self> {
        let requested_node_id = (!self.node_id.trim().is_empty()).then_some(self.node_id.as_str());
        let identity =
            crate::node_identity::load_or_create_node_identity_with_core_store_configuration(
                &self.storage_path,
                requested_node_id,
                self.core_pipeline_keyring()?,
                crate::core_store::CoreStoreNodeIdentity {
                    mesh_id: self.mesh_id.clone(),
                    node_id: String::new(),
                    region_id: self.region.clone(),
                    cell_id: self.cell_id.clone(),
                    public_api_addr: self.public_api_addr.clone(),
                    internal_bearer_token: (!self.corestore_internal_bearer_token.is_empty())
                        .then(|| self.corestore_internal_bearer_token.clone()),
                },
            )
            .await?;
        self.node_id = identity.node_id;

        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use tempfile::tempdir;

    fn required_args() -> [&'static str; 9] {
        [
            "anvil",
            "--jwt-secret",
            "test-secret",
            "--anvil-secret-encryption-key",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--public-api-addr",
            "test-node",
            "--region",
            "us-east-1",
        ]
    }

    #[test]
    fn cross_region_routing_policy_defaults_to_redirect_preferred() {
        let config = Config::try_parse_from(required_args()).unwrap();

        assert_eq!(
            config.cross_region_routing_policy,
            CrossRegionRoutingPolicy::RedirectPreferred
        );
    }

    #[test]
    fn cross_region_routing_policy_parses_configured_value() {
        let mut args = required_args().to_vec();
        args.extend(["--cross-region-routing-policy", "local_only"]);
        let config = Config::try_parse_from(args).unwrap();

        assert_eq!(
            config.cross_region_routing_policy,
            CrossRegionRoutingPolicy::LocalOnly
        );
    }

    #[test]
    fn background_worker_concurrency_defaults_and_parses() {
        let default = Config::try_parse_from(required_args()).unwrap();
        assert_eq!(default.background_worker_concurrency, 4);

        let mut args = required_args().to_vec();
        args.extend(["--background-worker-concurrency", "1"]);
        let configured = Config::try_parse_from(args).unwrap();
        assert_eq!(configured.background_worker_concurrency, 1);

        let mut invalid_args = required_args().to_vec();
        invalid_args.extend(["--background-worker-concurrency", "0"]);
        assert!(Config::try_parse_from(invalid_args).is_err());
    }

    #[test]
    fn rust_default_matches_cli_defaults() {
        let mut cli_default = Config::try_parse_from(required_args()).unwrap();
        cli_default.jwt_secret.clear();
        cli_default.anvil_secret_encryption_key.clear();
        cli_default.public_api_addr.clear();
        cli_default.region.clear();

        assert_eq!(Config::default(), cli_default);
    }

    #[test]
    fn distributed_coremeta_is_derived_from_committed_topology_bootstrap_configuration() {
        assert!(!Config::default().requires_distributed_coremeta_recovery());
        assert!(
            Config {
                bootstrap_node_ids: vec!["node-a".into(), "node-b".into()],
                ..Config::default()
            }
            .requires_distributed_coremeta_recovery()
        );
    }

    #[test]
    fn production_config_has_no_personaldb_signer_process_or_private_key_input() {
        let command = Config::command();
        let exposed_inputs = command
            .get_arguments()
            .map(|argument| {
                let mut input = argument.get_id().as_str().to_string();
                if let Some(long) = argument.get_long() {
                    input.push(' ');
                    input.push_str(long);
                }
                if let Some(env) = argument.get_env() {
                    input.push(' ');
                    input.push_str(&env.to_string_lossy());
                }
                input.to_ascii_lowercase()
            })
            .collect::<Vec<_>>();

        for forbidden in [
            "personaldb_protocol_signing_manifest",
            "personaldb-protocol-signing-manifest",
            "personaldb_private_key",
            "personaldb-private-key",
            "private_key_pkcs8",
            "private-key-pkcs8",
            "personaldb_in_process",
            "personaldb-in-process",
            "personaldb_signer_socket",
            "personaldb-signer-socket",
        ] {
            assert!(
                exposed_inputs
                    .iter()
                    .all(|input| !input.contains(forbidden)),
                "production coordinator config unexpectedly exposes {forbidden}"
            );
        }

        for forbidden_option in [
            "--personaldb-private-key-pkcs8-path",
            "--personaldb-in-process-signer",
        ] {
            let mut args = required_args().to_vec();
            args.extend([forbidden_option, "forbidden"]);
            assert!(
                Config::try_parse_from(args).is_err(),
                "production coordinator accepted {forbidden_option}"
            );
        }
    }

    #[tokio::test]
    async fn persisted_identity_is_coremeta_owned_and_reloads() {
        let temp = tempdir().unwrap();
        let storage_path = temp.path().join("storage");
        let pipeline_key = "00".repeat(32);
        let config = Config {
            storage_path: storage_path.to_string_lossy().into_owned(),
            anvil_secret_encryption_key: pipeline_key.clone(),
            ..Config::default()
        };

        let first = config.with_persisted_identity().await.unwrap();
        let restarted = Config {
            storage_path: storage_path.to_string_lossy().into_owned(),
            anvil_secret_encryption_key: pipeline_key,
            ..Config::default()
        }
        .with_persisted_identity()
        .await
        .unwrap();

        assert_eq!(first.node_id, restarted.node_id);
        assert!(!storage_path.join("node-id").exists());
        assert!(
            storage_path
                .join("corestore")
                .join("meta")
                .join("rocksdb")
                .exists()
        );
    }

    #[test]
    fn admin_listener_rejects_public_bind_without_explicit_opt_in() {
        for addr in ["0.0.0.0:50052", "[::]:50052", "192.168.1.10:50052"] {
            let config = Config {
                admin_listen_addr: addr.to_string(),
                allow_public_admin_listener: false,
                ..Config::default()
            };

            assert!(config.validate_admin_listener_bind().is_err(), "{addr}");
        }
    }

    #[test]
    fn admin_listener_allows_loopback_or_explicit_public_opt_in() {
        for addr in ["127.0.0.1:50052", "[::1]:50052"] {
            let config = Config {
                admin_listen_addr: addr.to_string(),
                allow_public_admin_listener: false,
                ..Config::default()
            };

            config.validate_admin_listener_bind().unwrap();
        }

        let config = Config {
            admin_listen_addr: "0.0.0.0:50052".to_string(),
            allow_public_admin_listener: true,
            ..Config::default()
        };
        config.validate_admin_listener_bind().unwrap();
    }
}
