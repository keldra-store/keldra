use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use keldra::authentication::{JwtManager, RateLimitConfig, load_token_signing_key};
use keldra::observability::{Observability, ObservabilityConfig};
use keldra::{
    ExplicitAuthoritativePaths, IndexRuntimeConfig, PluginGatewayConfig, ServerConfig,
    StoragePaths, serve,
};

#[derive(Debug, Parser)]
#[command(name = "keldra-server", version, about = "Keldra object server")]
struct Arguments {
    #[arg(long, env = "KELDRA_LISTEN", default_value = "127.0.0.1:50051")]
    listen: SocketAddr,

    #[arg(long, env = "KELDRA_PEER_LISTEN", default_value = "127.0.0.1:50052")]
    peer_listen: SocketAddr,

    #[arg(long, env = "KELDRA_PEER_ADVERTISE")]
    peer_advertise: Option<String>,

    /// Consume one operator-copied mode-0600 bundle to join an existing cluster.
    #[arg(long, env = "KELDRA_JOIN_BUNDLE")]
    join_bundle: Option<PathBuf>,

    #[arg(long, env = "KELDRA_DATA_DIR", default_value = "keldra-data")]
    data_dir: PathBuf,

    /// Durable node identity, certificates, and Raft state. Pinned at initialization.
    #[arg(long, env = "KELDRA_STATE_DIR")]
    state_dir: Option<PathBuf>,

    /// Durable RocksDB SST and manifest directory. Pinned at initialization.
    #[arg(long, env = "KELDRA_METADATA_DIR")]
    metadata_dir: Option<PathBuf>,

    /// Durable RocksDB write-ahead-log directory. Pinned at initialization.
    #[arg(long, env = "KELDRA_METADATA_WAL_DIR")]
    metadata_wal_dir: Option<PathBuf>,

    /// Durable canonical blob and erasure-shard directory. Pinned at initialization.
    #[arg(long, env = "KELDRA_PAYLOAD_DIR")]
    payload_dir: Option<PathBuf>,

    /// Restart-disposable index construction scratch directory.
    #[arg(long, env = "KELDRA_SCRATCH_DIR")]
    scratch_dir: Option<PathBuf>,

    /// Restart-disposable index and gateway cache directory.
    #[arg(long, env = "KELDRA_CACHE_DIR")]
    cache_dir: Option<PathBuf>,

    /// Aggregate bytes admitted across active unfinished uploads.
    #[arg(long, env = "KELDRA_PENDING_UPLOAD_MAX_BYTES")]
    pending_upload_max_bytes: Option<NonZeroU64>,

    #[arg(long, env = "KELDRA_RUN_SYSTEM_BOOTSTRAP", default_value_t = false)]
    run_system_bootstrap: bool,

    #[arg(
        long,
        env = "KELDRA_SYSTEM_BOOTSTRAP_CREDENTIAL_OUTPUT",
        requires = "run_system_bootstrap"
    )]
    system_bootstrap_credential_output: Option<PathBuf>,

    #[arg(long, env = "KELDRA_NODE_ID", default_value_t = 1)]
    node_id: u16,

    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    otlp_endpoint: Option<String>,

    #[arg(
        long,
        env = "KELDRA_MAX_ATOMIC_COMMIT_ENTRIES",
        default_value_t = 4_096
    )]
    max_atomic_commit_entries: u32,

    #[arg(
        long,
        env = "KELDRA_MAX_ATOMIC_COMMIT_BYTES",
        default_value_t = 16 * 1024 * 1024_u64
    )]
    max_atomic_commit_bytes: u64,

    #[arg(
        long,
        env = "KELDRA_ATOMIC_PROGRAM_TIMEOUT_SECONDS",
        default_value = "30"
    )]
    atomic_program_timeout_seconds: NonZeroU64,

    /// Maximum wall time for one BulkWrite RPC; shorter client deadlines still win.
    #[arg(long, env = "KELDRA_BULK_WRITE_TIMEOUT_SECONDS", default_value = "600")]
    bulk_write_timeout_seconds: NonZeroU64,

    /// Maximum wall time for one QueryIndex RPC; shorter client deadlines still win.
    #[arg(
        long,
        env = "KELDRA_INDEX_QUERY_TIMEOUT_SECONDS",
        default_value = "300"
    )]
    index_query_timeout_seconds: NonZeroU64,

    #[arg(long, env = "KELDRA_TOKEN_SIGNING_KEY_FILE")]
    token_signing_key_file: PathBuf,

    /// DNS suffix used by HTTP plugins: <bucket>.<tenant>.<domain>.
    #[arg(long, env = "KELDRA_PUBLIC_BASE_DOMAIN")]
    public_base_domain: Option<String>,

    /// Scheme advertised by plugin authentication challenges.
    #[arg(long, env = "KELDRA_PUBLIC_SCHEME", default_value = "https")]
    public_scheme: String,

    /// Installed HTTP plugin origin in name@version=http://host:port form.
    #[arg(
        long = "http-plugin",
        env = "KELDRA_HTTP_PLUGINS",
        value_delimiter = ','
    )]
    http_plugins: Vec<String>,

    #[arg(
        long,
        env = "KELDRA_RATE_LIMIT_GLOBAL_PER_SECOND",
        default_value = "10000"
    )]
    rate_limit_global_per_second: NonZeroU32,

    #[arg(long, env = "KELDRA_RATE_LIMIT_GLOBAL_BURST", default_value = "10000")]
    rate_limit_global_burst: NonZeroU32,

    #[arg(
        long,
        env = "KELDRA_RATE_LIMIT_AUTHENTICATED_PER_SECOND",
        default_value = "1000"
    )]
    rate_limit_authenticated_per_second: NonZeroU32,

    #[arg(
        long,
        env = "KELDRA_RATE_LIMIT_AUTHENTICATED_BURST",
        default_value = "1000"
    )]
    rate_limit_authenticated_burst: NonZeroU32,

    #[arg(
        long,
        env = "KELDRA_RATE_LIMIT_CREDENTIAL_GLOBAL_PER_MINUTE",
        default_value = "100"
    )]
    rate_limit_credential_global_per_minute: NonZeroU32,

    #[arg(
        long,
        env = "KELDRA_RATE_LIMIT_CREDENTIAL_GLOBAL_BURST",
        default_value = "20"
    )]
    rate_limit_credential_global_burst: NonZeroU32,

    #[arg(
        long,
        env = "KELDRA_RATE_LIMIT_CREDENTIAL_CLIENT_PER_MINUTE",
        default_value = "10"
    )]
    rate_limit_credential_client_per_minute: NonZeroU32,

    #[arg(
        long,
        env = "KELDRA_RATE_LIMIT_CREDENTIAL_CLIENT_BURST",
        default_value = "3"
    )]
    rate_limit_credential_client_burst: NonZeroU32,

    #[arg(
        long,
        env = "KELDRA_RATE_LIMIT_KEYED_CLEANUP_INTERVAL",
        default_value = "1024"
    )]
    rate_limit_keyed_cleanup_interval: NonZeroU64,

    /// Bounded CPU workers in the partition-owned v6 index pipeline (default: 4).
    #[arg(
        long,
        env = "KELDRA_INDEXING_CORES",
        default_value_t = IndexRuntimeConfig::DEFAULT_INDEXING_CORES
    )]
    indexing_cores: u32,

    /// Query working-memory fair share (default: 512 MiB).
    #[arg(
        long,
        env = "KELDRA_INDEX_QUERY_MEMORY_BYTES",
        default_value_t = IndexRuntimeConfig::DEFAULT_QUERY_MEMORY_BYTES
    )]
    index_query_memory_bytes: u64,

    /// TypedJson memory-first indexing budget. Absent uses 256 MiB per indexing core.
    #[arg(long, env = "KELDRA_INDEX_PIPELINE_MEMORY_BYTES")]
    index_pipeline_memory_bytes: Option<u64>,

    /// Hard aggregate heap ceiling shared by queries and the v6 pipeline.
    /// Absent uses their checked sum.
    #[arg(long, env = "KELDRA_INDEX_WORKING_MEMORY_BYTES")]
    index_working_memory_bytes: Option<u64>,

    /// Maximum v6 LSM runs retained in one level before compaction (default: 64).
    #[arg(
        long,
        env = "KELDRA_INDEX_LSM_MAX_RUNS_PER_LEVEL",
        default_value_t = IndexRuntimeConfig::DEFAULT_LSM_MAX_RUNS_PER_LEVEL
    )]
    index_lsm_max_runs_per_level: u32,

    /// Maximum v6 LSM unmerged bytes in one level (default: 1 GiB).
    #[arg(
        long,
        env = "KELDRA_INDEX_LSM_MAX_UNMERGED_BYTES_PER_LEVEL",
        default_value_t = IndexRuntimeConfig::DEFAULT_LSM_MAX_UNMERGED_BYTES_PER_LEVEL
    )]
    index_lsm_max_unmerged_bytes_per_level: u64,

    /// Accounted v6 active-partition RAM which freezes a non-empty segment (default: 16 MiB).
    #[arg(
        long = "index-flush-bytes",
        env = "KELDRA_INDEX_FLUSH_BYTES",
        default_value_t = IndexRuntimeConfig::DEFAULT_FLUSH_BYTES
    )]
    index_segment_flush_bytes: u64,

    /// Maximum age in milliseconds of a mutation in a non-empty active segment (default: 1000).
    #[arg(
        long = "index-flush-max-age-millis",
        env = "KELDRA_INDEX_FLUSH_MAX_AGE_MILLIS",
        default_value_t = IndexRuntimeConfig::DEFAULT_FLUSH_MAX_AGE_MILLIS
    )]
    index_segment_flush_max_age_millis: u64,

    /// Maximum complete mutation units accumulated in one segment (default: 65536).
    #[arg(
        long = "index-flush-max-operations",
        env = "KELDRA_INDEX_FLUSH_MAX_OPERATIONS",
        default_value_t = IndexRuntimeConfig::DEFAULT_FLUSH_MAX_OPERATIONS
    )]
    index_segment_flush_max_operations: u64,

    #[arg(long, env = "KELDRA_MAX_BLOB_BYTES", default_value_t = 16 * 1024 * 1024 * 1024_u64)]
    max_blob_bytes: u64,

    #[arg(
        long,
        env = "KELDRA_MAX_TOTAL_WAL_BYTES",
        default_value_t = keldra_store::DEFAULT_MAX_TOTAL_WAL_BYTES
    )]
    max_total_wal_bytes: u64,

    #[arg(
        long,
        env = "KELDRA_ERASURE_DATA_SHARDS",
        default_value_t = keldra_store::DEFAULT_ERASURE_DATA_SHARDS
    )]
    erasure_data_shards: u16,

    #[arg(
        long,
        env = "KELDRA_ERASURE_PARITY_SHARDS",
        default_value_t = keldra_store::DEFAULT_ERASURE_PARITY_SHARDS
    )]
    erasure_parity_shards: u16,

    #[arg(
        long,
        env = "KELDRA_ERASURE_STRIPE_UNIT_BYTES",
        default_value_t = keldra_store::DEFAULT_ERASURE_STRIPE_UNIT_BYTES
    )]
    erasure_stripe_unit_bytes: u32,

    #[arg(
        long,
        env = "KELDRA_AWAITING_PUBLISH_TTL_SECONDS",
        default_value_t = keldra_store::DEFAULT_AWAITING_PUBLISH_TTL_SECONDS
    )]
    awaiting_publish_ttl_seconds: u64,

    #[arg(
        long,
        env = "KELDRA_MUTATION_RECEIPT_RETENTION_SECONDS",
        default_value_t = keldra_store::DEFAULT_MUTATION_RECEIPT_RETENTION_SECONDS
    )]
    mutation_receipt_retention_seconds: u64,

    #[arg(
        long,
        env = "KELDRA_MAX_MUTATION_RECEIPT_ENTRIES",
        default_value_t = keldra_store::DEFAULT_MUTATION_RECEIPT_MAX_ENTRIES
    )]
    max_mutation_receipt_entries: u64,

    #[arg(
        long,
        env = "KELDRA_MAX_MUTATION_RECEIPT_BYTES",
        default_value_t = keldra_store::DEFAULT_MUTATION_RECEIPT_MAX_BYTES
    )]
    max_mutation_receipt_bytes: u64,

    /// Maximum retained entries in each node's ordered source journal.
    #[arg(
        long,
        env = "KELDRA_SOURCE_JOURNAL_MAX_ENTRIES",
        default_value_t = keldra_store::DEFAULT_WATCH_MAX_ENTRIES
    )]
    source_journal_max_entries: u64,

    /// Maximum retained logical bytes in each node's ordered source journal.
    #[arg(
        long,
        env = "KELDRA_SOURCE_JOURNAL_MAX_BYTES",
        default_value_t = keldra_store::DEFAULT_WATCH_MAX_BYTES
    )]
    source_journal_max_bytes: u64,
}

impl Arguments {
    fn storage_paths(&self) -> (StoragePaths, ExplicitAuthoritativePaths) {
        let explicit = ExplicitAuthoritativePaths {
            state: self.state_dir.is_some(),
            metadata: self.metadata_dir.is_some(),
            metadata_wal: self.metadata_wal_dir.is_some(),
            payload: self.payload_dir.is_some(),
        };
        let pending_upload_max = self
            .pending_upload_max_bytes
            .map(NonZeroU64::get)
            .unwrap_or(self.max_blob_bytes);
        let mut paths = StoragePaths::under(&self.data_dir, pending_upload_max);
        if let Some(path) = &self.state_dir {
            paths.state = path.clone();
        }
        if let Some(path) = &self.metadata_dir {
            paths.metadata = path.clone();
        }
        paths.metadata_wal = self
            .metadata_wal_dir
            .clone()
            .unwrap_or_else(|| paths.metadata.clone());
        if let Some(path) = &self.payload_dir {
            paths.payload = path.clone();
        }
        paths.scratch = self
            .scratch_dir
            .clone()
            .unwrap_or_else(|| self.data_dir.join("index-scratch"));
        paths.cache = self
            .cache_dir
            .clone()
            .unwrap_or_else(|| self.data_dir.join("cache"));
        (paths, explicit)
    }

    fn erasure_profile(&self) -> Result<keldra_store::ErasureProfile> {
        keldra_store::ErasureProfile::new(
            self.erasure_data_shards,
            self.erasure_parity_shards,
            self.erasure_stripe_unit_bytes,
        )
        .context("validate erasure-code profile")
    }

    fn index_runtime_config(&self) -> Result<IndexRuntimeConfig> {
        let mut config = IndexRuntimeConfig::new(self.indexing_cores)
            .context("validate v6 index runtime configuration")?
            .with_query_memory_bytes(self.index_query_memory_bytes)
            .and_then(|config| {
                config.with_flush_boundaries(
                    self.index_segment_flush_bytes,
                    self.index_segment_flush_max_age_millis,
                    self.index_segment_flush_max_operations,
                )
            })
            .and_then(|config| {
                config.with_lsm_limits(
                    self.index_lsm_max_runs_per_level,
                    self.index_lsm_max_unmerged_bytes_per_level,
                )
            })
            .context("validate v6 index runtime configuration")?;
        if let Some(bytes) = self.index_pipeline_memory_bytes {
            config = config
                .with_pipeline_memory_bytes(bytes)
                .context("validate v6 indexing pipeline memory configuration")?;
        }
        if let Some(bytes) = self.index_working_memory_bytes {
            config = config
                .with_working_memory_bytes(bytes)
                .context("validate v6 aggregate index working-memory configuration")?;
        }
        config
            .working_memory_bytes()
            .context("validate v6 aggregate index working-memory configuration")?;
        Ok(config)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = Arguments::parse();
    let (storage, explicit_authoritative_paths) = arguments.storage_paths();
    let erasure_profile = arguments.erasure_profile()?;
    let index_runtime = arguments.index_runtime_config()?;
    let plugin_gateway = PluginGatewayConfig::new(
        arguments.public_base_domain.clone(),
        Some(arguments.public_scheme.clone()),
        arguments.http_plugins.clone(),
    )?;
    let signing_key =
        load_token_signing_key(&arguments.token_signing_key_file).with_context(|| {
            format!(
                "load token signing key from {}",
                arguments.token_signing_key_file.display()
            )
        })?;
    let token_manager = JwtManager::new(signing_key).context("configure access tokens")?;
    let observability = Observability::init(ObservabilityConfig {
        node_id: arguments.node_id,
        otlp_endpoint: arguments.otlp_endpoint,
    })?;
    let server_result = serve(ServerConfig {
        listen: arguments.listen,
        peer_listen: arguments.peer_listen,
        peer_advertise: arguments.peer_advertise,
        join_bundle: arguments.join_bundle,
        storage,
        explicit_authoritative_paths,
        run_system_bootstrap: arguments.run_system_bootstrap,
        system_bootstrap_credential_output: arguments.system_bootstrap_credential_output,
        node_id: arguments.node_id,
        max_atomic_commit_entries: arguments.max_atomic_commit_entries,
        max_atomic_commit_bytes: arguments.max_atomic_commit_bytes,
        atomic_program_timeout: std::time::Duration::from_secs(
            arguments.atomic_program_timeout_seconds.get(),
        ),
        bulk_write_timeout: std::time::Duration::from_secs(
            arguments.bulk_write_timeout_seconds.get(),
        ),
        index_query_timeout: std::time::Duration::from_secs(
            arguments.index_query_timeout_seconds.get(),
        ),
        token_manager,
        rate_limits: RateLimitConfig {
            global_per_second: arguments.rate_limit_global_per_second,
            global_burst: arguments.rate_limit_global_burst,
            authenticated_per_second: arguments.rate_limit_authenticated_per_second,
            authenticated_burst: arguments.rate_limit_authenticated_burst,
            credential_global_per_minute: arguments.rate_limit_credential_global_per_minute,
            credential_global_burst: arguments.rate_limit_credential_global_burst,
            credential_client_per_minute: arguments.rate_limit_credential_client_per_minute,
            credential_client_burst: arguments.rate_limit_credential_client_burst,
            keyed_cleanup_interval: arguments.rate_limit_keyed_cleanup_interval,
        },
        index_runtime,
        plugin_gateway,
        max_blob_bytes: arguments.max_blob_bytes,
        max_total_wal_bytes: arguments.max_total_wal_bytes,
        erasure_profile,
        awaiting_publish_ttl_seconds: arguments.awaiting_publish_ttl_seconds,
        mutation_receipt_retention_seconds: arguments.mutation_receipt_retention_seconds,
        max_mutation_receipt_entries: arguments.max_mutation_receipt_entries,
        max_mutation_receipt_bytes: arguments.max_mutation_receipt_bytes,
        source_journal_max_entries: arguments.source_journal_max_entries,
        source_journal_max_bytes: arguments.source_journal_max_bytes,
    })
    .await;
    let observability_result = observability.shutdown().await;
    match (server_result, observability_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(server_error), Ok(())) => Err(server_error),
        (Ok(()), Err(observability_error)) => Err(observability_error),
        (Err(server_error), Err(observability_error)) => {
            tracing::error!(
                error = %observability_error,
                "OpenTelemetry shutdown failed after the server stopped"
            );
            Err(server_error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory as _;

    fn parse(extra: &[&str]) -> Arguments {
        let mut arguments = vec![
            "keldra-server",
            "--token-signing-key-file",
            "test-signing-key",
        ];
        arguments.extend_from_slice(extra);
        Arguments::try_parse_from(arguments).unwrap()
    }

    #[test]
    fn erasure_profile_defaults_to_two_plus_one() {
        let arguments = parse(&[]);
        let profile = arguments.erasure_profile().unwrap();
        assert_eq!(profile, keldra_store::ErasureProfile::default());
        assert_eq!(
            arguments.peer_listen,
            "127.0.0.1:50052".parse::<SocketAddr>().unwrap()
        );
        assert!(arguments.peer_advertise.is_none());
    }

    #[test]
    fn storage_paths_default_under_the_data_directory() {
        let arguments = parse(&[
            "--data-dir",
            "/var/lib/keldra",
            "--max-blob-bytes",
            "1048576",
        ]);
        let (paths, explicit) = arguments.storage_paths();

        assert_eq!(paths, StoragePaths::under("/var/lib/keldra", 1_048_576));
        assert_eq!(explicit, ExplicitAuthoritativePaths::default());
    }

    #[test]
    fn storage_paths_accept_independent_authoritative_and_disposable_roots() {
        let arguments = parse(&[
            "--data-dir",
            "/fallback",
            "--state-dir",
            "/state",
            "--metadata-dir",
            "/metadata",
            "--metadata-wal-dir",
            "/wal",
            "--payload-dir",
            "/payload",
            "--scratch-dir",
            "/scratch",
            "--cache-dir",
            "/cache",
            "--pending-upload-max-bytes",
            "2097152",
        ]);
        let (paths, explicit) = arguments.storage_paths();

        assert_eq!(paths.state, PathBuf::from("/state"));
        assert_eq!(paths.metadata, PathBuf::from("/metadata"));
        assert_eq!(paths.metadata_wal, PathBuf::from("/wal"));
        assert_eq!(paths.payload, PathBuf::from("/payload"));
        assert_eq!(paths.scratch, PathBuf::from("/scratch"));
        assert_eq!(paths.cache, PathBuf::from("/cache"));
        assert_eq!(paths.pending_upload_max_bytes, 2_097_152);
        assert_eq!(
            explicit,
            ExplicitAuthoritativePaths {
                state: true,
                metadata: true,
                metadata_wal: true,
                payload: true,
            }
        );
    }

    #[test]
    fn metadata_wal_defaults_to_the_effective_metadata_root() {
        let arguments = parse(&["--metadata-dir", "/metadata"]);
        let (paths, explicit) = arguments.storage_paths();

        assert_eq!(paths.metadata_wal, PathBuf::from("/metadata"));
        assert!(explicit.metadata);
        assert!(!explicit.metadata_wal);
    }

    #[test]
    fn peer_listener_and_advertised_address_are_explicit_startup_options() {
        let arguments = parse(&[
            "--peer-listen",
            "0.0.0.0:60052",
            "--peer-advertise",
            "keldra-1.internal:60052",
        ]);
        assert_eq!(
            arguments.peer_listen,
            "0.0.0.0:60052".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            arguments.peer_advertise.as_deref(),
            Some("keldra-1.internal:60052")
        );
    }

    #[test]
    fn erasure_profile_accepts_a_valid_startup_override() {
        let profile = parse(&[
            "--erasure-data-shards",
            "4",
            "--erasure-parity-shards",
            "2",
            "--erasure-stripe-unit-bytes",
            "32768",
        ])
        .erasure_profile()
        .unwrap();
        assert_eq!(profile.data_shards(), 4);
        assert_eq!(profile.parity_shards(), 2);
        assert_eq!(profile.stripe_unit(), 32 * 1024);
    }

    #[test]
    fn erasure_profile_rejects_invalid_startup_geometry() {
        for extra in [
            vec!["--erasure-data-shards", "0"],
            vec!["--erasure-parity-shards", "0"],
            vec![
                "--erasure-data-shards",
                "255",
                "--erasure-parity-shards",
                "2",
            ],
            vec!["--erasure-stripe-unit-bytes", "0"],
        ] {
            let error = parse(&extra).erasure_profile().unwrap_err();
            assert!(error.to_string().contains("validate erasure-code profile"));
        }
    }

    #[test]
    fn request_timeout_classes_are_independent() {
        let defaults = parse(&[]);
        assert_eq!(defaults.atomic_program_timeout_seconds.get(), 30);
        assert_eq!(defaults.bulk_write_timeout_seconds.get(), 600);
        assert_eq!(defaults.index_query_timeout_seconds.get(), 300);

        let configured = parse(&[
            "--atomic-program-timeout-seconds",
            "12",
            "--bulk-write-timeout-seconds",
            "480",
            "--index-query-timeout-seconds",
            "600",
        ]);
        assert_eq!(configured.atomic_program_timeout_seconds.get(), 12);
        assert_eq!(configured.bulk_write_timeout_seconds.get(), 480);
        assert_eq!(configured.index_query_timeout_seconds.get(), 600);
    }

    #[test]
    fn index_runtime_defaults_are_wired_to_startup_configuration() {
        assert_eq!(
            parse(&[]).index_runtime_config().unwrap(),
            IndexRuntimeConfig::default()
        );
    }

    #[test]
    fn index_runtime_accepts_the_complete_v6_operator_matrix() {
        let config = parse(&[
            "--indexing-cores",
            "6",
            "--index-pipeline-memory-bytes",
            "1610612736",
            "--index-query-memory-bytes",
            "268435456",
            "--index-working-memory-bytes",
            "1879048192",
            "--index-lsm-max-runs-per-level",
            "12",
            "--index-lsm-max-unmerged-bytes-per-level",
            "10485760",
            "--index-flush-bytes",
            "12582912",
            "--index-flush-max-age-millis",
            "750",
            "--index-flush-max-operations",
            "32768",
        ])
        .index_runtime_config()
        .unwrap();
        assert_eq!(config.indexing_cores(), 6);
        assert_eq!(config.pipeline_memory_bytes(), 1_610_612_736);
        assert_eq!(config.query_memory_bytes(), 268_435_456);
        assert_eq!(config.working_memory_bytes().unwrap(), 1_879_048_192);
        assert_eq!(config.lsm_max_runs_per_level(), 12);
        assert_eq!(config.lsm_max_unmerged_bytes_per_level(), 10_485_760);
        assert_eq!(config.flush_bytes(), 12_582_912);
        assert_eq!(config.flush_max_age().as_millis(), 750);
        assert_eq!(config.flush_max_operations(), 32_768);
    }

    #[test]
    fn index_runtime_rejects_zero_and_out_of_range_v6_limits() {
        for extra in [
            vec!["--indexing-cores", "0"],
            vec!["--index-pipeline-memory-bytes", "0"],
            vec!["--index-query-memory-bytes", "0"],
            vec!["--index-working-memory-bytes", "0"],
            vec!["--index-lsm-max-runs-per-level", "0"],
            vec!["--index-lsm-max-unmerged-bytes-per-level", "0"],
            vec!["--index-flush-bytes", "0"],
            vec!["--index-flush-max-age-millis", "0"],
            vec!["--index-flush-max-operations", "0"],
        ] {
            assert!(parse(&extra).index_runtime_config().is_err(), "{extra:?}");
        }
    }

    #[test]
    fn help_exposes_only_the_v6_index_controls() {
        let help = Arguments::command().render_long_help().to_string();
        for live in [
            "--indexing-cores",
            "--index-pipeline-memory-bytes",
            "--index-query-memory-bytes",
            "--index-working-memory-bytes",
            "--index-lsm-max-runs-per-level",
            "--index-lsm-max-unmerged-bytes-per-level",
            "--index-flush-bytes",
            "--index-flush-max-age-millis",
            "--index-flush-max-operations",
        ] {
            assert!(help.contains(live), "help omitted {live}");
        }
    }
}
