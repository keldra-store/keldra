use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;

use anvil::authentication::{JwtManager, RateLimitConfig, load_token_signing_key};
use anvil::observability::{Observability, ObservabilityConfig};
use anvil::{IndexRuntimeConfig, ServerConfig, serve};
use anvil_index::IndexKind;
use anyhow::{Context, Result};
use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "anvil-server", version, about = "Anvil 0.7 object server")]
struct Arguments {
    #[arg(long, env = "ANVIL_LISTEN", default_value = "127.0.0.1:50051")]
    listen: SocketAddr,

    #[arg(long, env = "ANVIL_PEER_LISTEN", default_value = "127.0.0.1:50052")]
    peer_listen: SocketAddr,

    #[arg(long, env = "ANVIL_PEER_ADVERTISE")]
    peer_advertise: Option<String>,

    /// Consume one operator-copied mode-0600 bundle to join an existing cluster.
    #[arg(long, env = "ANVIL_JOIN_BUNDLE")]
    join_bundle: Option<PathBuf>,

    #[arg(long, env = "ANVIL_DATA_DIR", default_value = "anvil-data")]
    data_dir: PathBuf,

    #[arg(long, env = "ANVIL_RUN_SYSTEM_BOOTSTRAP", default_value_t = false)]
    run_system_bootstrap: bool,

    #[arg(
        long,
        env = "ANVIL_SYSTEM_BOOTSTRAP_CREDENTIAL_OUTPUT",
        requires = "run_system_bootstrap"
    )]
    system_bootstrap_credential_output: Option<PathBuf>,

    #[arg(long, env = "ANVIL_NODE_ID", default_value_t = 1)]
    node_id: u16,

    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    otlp_endpoint: Option<String>,

    #[arg(long, env = "ANVIL_MAX_ATOMIC_COMMIT_ENTRIES", default_value_t = 4_096)]
    max_atomic_commit_entries: u32,

    #[arg(
        long,
        env = "ANVIL_MAX_ATOMIC_COMMIT_BYTES",
        default_value_t = 16 * 1024 * 1024_u64
    )]
    max_atomic_commit_bytes: u64,

    #[arg(
        long,
        env = "ANVIL_ATOMIC_PROGRAM_TIMEOUT_SECONDS",
        default_value = "30"
    )]
    atomic_program_timeout_seconds: NonZeroU64,

    #[arg(long, env = "ANVIL_TOKEN_SIGNING_KEY_FILE")]
    token_signing_key_file: PathBuf,

    #[arg(
        long,
        env = "ANVIL_RATE_LIMIT_GLOBAL_PER_SECOND",
        default_value = "10000"
    )]
    rate_limit_global_per_second: NonZeroU32,

    #[arg(long, env = "ANVIL_RATE_LIMIT_GLOBAL_BURST", default_value = "10000")]
    rate_limit_global_burst: NonZeroU32,

    #[arg(
        long,
        env = "ANVIL_RATE_LIMIT_AUTHENTICATED_PER_SECOND",
        default_value = "1000"
    )]
    rate_limit_authenticated_per_second: NonZeroU32,

    #[arg(
        long,
        env = "ANVIL_RATE_LIMIT_AUTHENTICATED_BURST",
        default_value = "1000"
    )]
    rate_limit_authenticated_burst: NonZeroU32,

    #[arg(
        long,
        env = "ANVIL_RATE_LIMIT_CREDENTIAL_GLOBAL_PER_MINUTE",
        default_value = "100"
    )]
    rate_limit_credential_global_per_minute: NonZeroU32,

    #[arg(
        long,
        env = "ANVIL_RATE_LIMIT_CREDENTIAL_GLOBAL_BURST",
        default_value = "20"
    )]
    rate_limit_credential_global_burst: NonZeroU32,

    #[arg(
        long,
        env = "ANVIL_RATE_LIMIT_CREDENTIAL_CLIENT_PER_MINUTE",
        default_value = "10"
    )]
    rate_limit_credential_client_per_minute: NonZeroU32,

    #[arg(
        long,
        env = "ANVIL_RATE_LIMIT_CREDENTIAL_CLIENT_BURST",
        default_value = "3"
    )]
    rate_limit_credential_client_burst: NonZeroU32,

    #[arg(
        long,
        env = "ANVIL_RATE_LIMIT_KEYED_CLEANUP_INTERVAL",
        default_value = "1024"
    )]
    rate_limit_keyed_cleanup_interval: NonZeroU64,

    /// Shared disposable index disk-cache budget in bytes (default: 10 GiB).
    #[arg(
        long,
        env = "ANVIL_INDEX_DISK_CACHE_BYTES",
        default_value_t = IndexRuntimeConfig::DEFAULT_DISK_CACHE_BYTES
    )]
    index_disk_cache_bytes: u64,

    /// Percentage of node memory bounding concurrent index block materialization (default: 10).
    #[arg(
        long,
        env = "ANVIL_INDEX_MEMORY_PERCENT",
        default_value_t = IndexRuntimeConfig::DEFAULT_MEMORY_PERCENT
    )]
    index_memory_percent: u8,

    /// Hard aggregate build/compaction heap budget for each index kind (default: 256 MiB).
    #[arg(
        long,
        env = "ANVIL_INDEX_BUILDER_MEMORY_BYTES_PER_KIND",
        default_value_t = IndexRuntimeConfig::DEFAULT_BUILDER_MEMORY_BYTES_PER_KIND
    )]
    index_builder_memory_bytes_per_kind: u64,

    /// Path builder-memory override; absent uses the common per-kind fallback.
    #[arg(long, env = "ANVIL_INDEX_PATH_BUILDER_MEMORY_BYTES")]
    index_path_builder_memory_bytes: Option<u64>,

    /// Maximum parallel Path compaction lanes (default: 4).
    #[arg(
        long,
        env = "ANVIL_INDEX_PATH_COMPACTION_MAX_LANES",
        default_value_t = IndexRuntimeConfig::DEFAULT_COMPACTION_MAX_LANES
    )]
    index_path_compaction_max_lanes: u32,

    /// Metadata-filter builder-memory override; absent uses the common fallback.
    #[arg(long, env = "ANVIL_INDEX_METADATA_FILTER_BUILDER_MEMORY_BYTES")]
    index_metadata_filter_builder_memory_bytes: Option<u64>,

    /// Maximum parallel Metadata-filter compaction lanes (default: 4).
    #[arg(
        long,
        env = "ANVIL_INDEX_METADATA_FILTER_COMPACTION_MAX_LANES",
        default_value_t = IndexRuntimeConfig::DEFAULT_COMPACTION_MAX_LANES
    )]
    index_metadata_filter_compaction_max_lanes: u32,

    /// Typed-JSON builder-memory override; absent uses the common fallback.
    #[arg(long, env = "ANVIL_INDEX_TYPED_JSON_BUILDER_MEMORY_BYTES")]
    index_typed_json_builder_memory_bytes: Option<u64>,

    /// Maximum parallel Typed-JSON compaction lanes (default: 4).
    #[arg(
        long,
        env = "ANVIL_INDEX_TYPED_JSON_COMPACTION_MAX_LANES",
        default_value_t = IndexRuntimeConfig::DEFAULT_COMPACTION_MAX_LANES
    )]
    index_typed_json_compaction_max_lanes: u32,

    /// Full-text builder-memory override; absent uses the common fallback.
    #[arg(long, env = "ANVIL_INDEX_FULL_TEXT_BUILDER_MEMORY_BYTES")]
    index_full_text_builder_memory_bytes: Option<u64>,

    /// Maximum parallel Full-text compaction lanes (default: 4).
    #[arg(
        long,
        env = "ANVIL_INDEX_FULL_TEXT_COMPACTION_MAX_LANES",
        default_value_t = IndexRuntimeConfig::DEFAULT_COMPACTION_MAX_LANES
    )]
    index_full_text_compaction_max_lanes: u32,

    /// Vector builder-memory override; absent uses the common fallback.
    #[arg(long, env = "ANVIL_INDEX_VECTOR_BUILDER_MEMORY_BYTES")]
    index_vector_builder_memory_bytes: Option<u64>,

    /// Maximum parallel Vector compaction lanes (default: 4).
    #[arg(
        long,
        env = "ANVIL_INDEX_VECTOR_COMPACTION_MAX_LANES",
        default_value_t = IndexRuntimeConfig::DEFAULT_COMPACTION_MAX_LANES
    )]
    index_vector_compaction_max_lanes: u32,

    /// Hybrid builder-memory override; absent uses the common fallback.
    #[arg(long, env = "ANVIL_INDEX_HYBRID_BUILDER_MEMORY_BYTES")]
    index_hybrid_builder_memory_bytes: Option<u64>,

    /// Maximum parallel Hybrid compaction lanes (default: 4).
    #[arg(
        long,
        env = "ANVIL_INDEX_HYBRID_COMPACTION_MAX_LANES",
        default_value_t = IndexRuntimeConfig::DEFAULT_COMPACTION_MAX_LANES
    )]
    index_hybrid_compaction_max_lanes: u32,

    /// Git-source builder-memory override; absent uses the common fallback.
    #[arg(long, env = "ANVIL_INDEX_GIT_SOURCE_BUILDER_MEMORY_BYTES")]
    index_git_source_builder_memory_bytes: Option<u64>,

    /// Maximum parallel Git-source compaction lanes (default: 4).
    #[arg(
        long,
        env = "ANVIL_INDEX_GIT_SOURCE_COMPACTION_MAX_LANES",
        default_value_t = IndexRuntimeConfig::DEFAULT_COMPACTION_MAX_LANES
    )]
    index_git_source_compaction_max_lanes: u32,

    /// Tensor builder-memory override; absent uses the common fallback.
    #[arg(long, env = "ANVIL_INDEX_TENSOR_BUILDER_MEMORY_BYTES")]
    index_tensor_builder_memory_bytes: Option<u64>,

    /// Maximum parallel Tensor compaction lanes (default: 4).
    #[arg(
        long,
        env = "ANVIL_INDEX_TENSOR_COMPACTION_MAX_LANES",
        default_value_t = IndexRuntimeConfig::DEFAULT_COMPACTION_MAX_LANES
    )]
    index_tensor_compaction_max_lanes: u32,

    /// Threads in Anvil's process-owned index CPU pool (default: 4).
    #[arg(
        long,
        env = "ANVIL_INDEX_RAYON_WORKERS",
        default_value_t = IndexRuntimeConfig::DEFAULT_RAYON_WORKERS
    )]
    index_rayon_workers: u32,

    /// Maximum generations retained per index, including current (default: 3).
    #[arg(
        long,
        env = "ANVIL_INDEX_MAX_RETAINED_GENERATIONS",
        default_value_t = IndexRuntimeConfig::DEFAULT_MAX_RETAINED_GENERATIONS
    )]
    index_max_retained_generations: u32,

    /// Maximum age of an obsolete index generation in hours (default: 24).
    #[arg(
        long,
        env = "ANVIL_INDEX_MAX_GENERATION_AGE_HOURS",
        default_value_t = IndexRuntimeConfig::DEFAULT_MAX_GENERATION_AGE_HOURS
    )]
    index_max_generation_age_hours: u64,

    /// Maximum authoritative bytes retained across all generations per index (default: 50 GiB).
    #[arg(
        long,
        env = "ANVIL_INDEX_MAX_RETAINED_GENERATION_BYTES",
        default_value_t = IndexRuntimeConfig::DEFAULT_MAX_RETAINED_GENERATION_BYTES
    )]
    index_max_retained_generation_bytes: u64,

    #[arg(long, env = "ANVIL_MAX_BLOB_BYTES", default_value_t = 16 * 1024 * 1024 * 1024_u64)]
    max_blob_bytes: u64,

    #[arg(
        long,
        env = "ANVIL_ERASURE_DATA_SHARDS",
        default_value_t = anvil_store::DEFAULT_ERASURE_DATA_SHARDS
    )]
    erasure_data_shards: u16,

    #[arg(
        long,
        env = "ANVIL_ERASURE_PARITY_SHARDS",
        default_value_t = anvil_store::DEFAULT_ERASURE_PARITY_SHARDS
    )]
    erasure_parity_shards: u16,

    #[arg(
        long,
        env = "ANVIL_ERASURE_STRIPE_UNIT_BYTES",
        default_value_t = anvil_store::DEFAULT_ERASURE_STRIPE_UNIT_BYTES
    )]
    erasure_stripe_unit_bytes: u32,

    #[arg(
        long,
        env = "ANVIL_AWAITING_PUBLISH_TTL_SECONDS",
        default_value_t = anvil_store::DEFAULT_AWAITING_PUBLISH_TTL_SECONDS
    )]
    awaiting_publish_ttl_seconds: u64,

    #[arg(
        long,
        env = "ANVIL_MUTATION_RECEIPT_RETENTION_SECONDS",
        default_value_t = anvil_store::DEFAULT_MUTATION_RECEIPT_RETENTION_SECONDS
    )]
    mutation_receipt_retention_seconds: u64,

    #[arg(
        long,
        env = "ANVIL_MAX_MUTATION_RECEIPT_ENTRIES",
        default_value_t = anvil_store::DEFAULT_MUTATION_RECEIPT_MAX_ENTRIES
    )]
    max_mutation_receipt_entries: u64,

    #[arg(
        long,
        env = "ANVIL_MAX_MUTATION_RECEIPT_BYTES",
        default_value_t = anvil_store::DEFAULT_MUTATION_RECEIPT_MAX_BYTES
    )]
    max_mutation_receipt_bytes: u64,

    #[arg(
        long,
        env = "ANVIL_WATCH_MAX_ENTRIES",
        default_value_t = anvil_store::DEFAULT_WATCH_MAX_ENTRIES
    )]
    watch_max_entries: u64,

    #[arg(
        long,
        env = "ANVIL_WATCH_MAX_BYTES",
        default_value_t = anvil_store::DEFAULT_WATCH_MAX_BYTES
    )]
    watch_max_bytes: u64,
}

impl Arguments {
    fn erasure_profile(&self) -> Result<anvil_store::ErasureProfile> {
        anvil_store::ErasureProfile::new(
            self.erasure_data_shards,
            self.erasure_parity_shards,
            self.erasure_stripe_unit_bytes,
        )
        .context("validate erasure-code profile")
    }

    fn index_runtime_config(&self) -> Result<IndexRuntimeConfig> {
        let mut config = IndexRuntimeConfig::new(
            self.index_disk_cache_bytes,
            self.index_memory_percent,
            self.index_builder_memory_bytes_per_kind,
            self.index_rayon_workers,
            self.index_max_retained_generations,
            self.index_max_generation_age_hours,
            self.index_max_retained_generation_bytes,
        )
        .context("validate index runtime configuration")?;
        for (kind, memory, lanes) in [
            (
                IndexKind::Path,
                self.index_path_builder_memory_bytes,
                self.index_path_compaction_max_lanes,
            ),
            (
                IndexKind::MetadataFilter,
                self.index_metadata_filter_builder_memory_bytes,
                self.index_metadata_filter_compaction_max_lanes,
            ),
            (
                IndexKind::TypedJson,
                self.index_typed_json_builder_memory_bytes,
                self.index_typed_json_compaction_max_lanes,
            ),
            (
                IndexKind::FullText,
                self.index_full_text_builder_memory_bytes,
                self.index_full_text_compaction_max_lanes,
            ),
            (
                IndexKind::Vector,
                self.index_vector_builder_memory_bytes,
                self.index_vector_compaction_max_lanes,
            ),
            (
                IndexKind::Hybrid,
                self.index_hybrid_builder_memory_bytes,
                self.index_hybrid_compaction_max_lanes,
            ),
            (
                IndexKind::GitSource,
                self.index_git_source_builder_memory_bytes,
                self.index_git_source_compaction_max_lanes,
            ),
            (
                IndexKind::Tensor,
                self.index_tensor_builder_memory_bytes,
                self.index_tensor_compaction_max_lanes,
            ),
        ] {
            config = config
                .with_kind_limits(
                    kind,
                    memory.unwrap_or(self.index_builder_memory_bytes_per_kind),
                    lanes,
                )
                .context("validate index runtime configuration")?;
        }
        Ok(config)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = Arguments::parse();
    let erasure_profile = arguments.erasure_profile()?;
    let index_runtime = arguments.index_runtime_config()?;
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
        data_dir: arguments.data_dir,
        run_system_bootstrap: arguments.run_system_bootstrap,
        system_bootstrap_credential_output: arguments.system_bootstrap_credential_output,
        node_id: arguments.node_id,
        max_atomic_commit_entries: arguments.max_atomic_commit_entries,
        max_atomic_commit_bytes: arguments.max_atomic_commit_bytes,
        atomic_program_timeout: std::time::Duration::from_secs(
            arguments.atomic_program_timeout_seconds.get(),
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
        max_blob_bytes: arguments.max_blob_bytes,
        erasure_profile,
        awaiting_publish_ttl_seconds: arguments.awaiting_publish_ttl_seconds,
        mutation_receipt_retention_seconds: arguments.mutation_receipt_retention_seconds,
        max_mutation_receipt_entries: arguments.max_mutation_receipt_entries,
        max_mutation_receipt_bytes: arguments.max_mutation_receipt_bytes,
        watch_max_entries: arguments.watch_max_entries,
        watch_max_bytes: arguments.watch_max_bytes,
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
            "anvil-server",
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
        assert_eq!(profile, anvil_store::ErasureProfile::default());
        assert_eq!(
            arguments.peer_listen,
            "127.0.0.1:50052".parse::<SocketAddr>().unwrap()
        );
        assert!(arguments.peer_advertise.is_none());
    }

    #[test]
    fn peer_listener_and_advertised_address_are_explicit_startup_options() {
        let arguments = parse(&[
            "--peer-listen",
            "0.0.0.0:60052",
            "--peer-advertise",
            "anvil-1.internal:60052",
        ]);
        assert_eq!(
            arguments.peer_listen,
            "0.0.0.0:60052".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            arguments.peer_advertise.as_deref(),
            Some("anvil-1.internal:60052")
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
    fn index_runtime_defaults_are_wired_to_startup_configuration() {
        assert_eq!(
            parse(&[]).index_runtime_config().unwrap(),
            IndexRuntimeConfig::default()
        );
    }

    #[test]
    fn index_runtime_accepts_explicit_operator_limits() {
        let config = parse(&[
            "--index-disk-cache-bytes",
            "1048576",
            "--index-memory-percent",
            "25",
            "--index-builder-memory-bytes-per-kind",
            "33554432",
            "--index-path-builder-memory-bytes",
            "67108864",
            "--index-path-compaction-max-lanes",
            "3",
            "--index-rayon-workers",
            "6",
            "--index-max-retained-generations",
            "7",
            "--index-max-generation-age-hours",
            "48",
            "--index-max-retained-generation-bytes",
            "2097152",
        ])
        .index_runtime_config()
        .unwrap();
        assert_eq!(config.disk_cache_bytes(), 1_048_576);
        assert_eq!(config.memory_percent(), 25);
        assert_eq!(config.builder_memory_bytes_per_kind(), 33_554_432);
        assert_eq!(config.builder_memory_bytes(IndexKind::Path), 67_108_864);
        assert_eq!(config.compaction_max_lanes(IndexKind::Path), 3);
        assert_eq!(
            config.builder_memory_bytes(IndexKind::TypedJson),
            33_554_432
        );
        assert_eq!(config.compaction_max_lanes(IndexKind::TypedJson), 4);
        assert_eq!(config.rayon_workers(), 6);
        assert_eq!(config.max_retained_generations(), 7);
        assert_eq!(config.max_generation_age_hours(), 48);
        assert_eq!(config.max_retained_generation_bytes(), 2_097_152);
    }

    #[test]
    fn index_runtime_rejects_zero_and_out_of_range_limits() {
        for extra in [
            vec!["--index-disk-cache-bytes", "0"],
            vec!["--index-memory-percent", "0"],
            vec!["--index-memory-percent", "101"],
            vec!["--index-builder-memory-bytes-per-kind", "0"],
            vec!["--index-vector-builder-memory-bytes", "0"],
            vec!["--index-vector-compaction-max-lanes", "0"],
            vec!["--index-rayon-workers", "0"],
            vec!["--index-max-retained-generations", "0"],
            vec!["--index-max-generation-age-hours", "0"],
            vec!["--index-max-retained-generation-bytes", "0"],
        ] {
            let error = parse(&extra).index_runtime_config().unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("validate index runtime configuration")
            );
        }
    }

    #[test]
    fn index_runtime_defaults_and_meaning_are_visible_in_help() {
        let help = Arguments::command().render_long_help().to_string();
        for expected in [
            "--index-disk-cache-bytes",
            "default: 10 GiB",
            "--index-memory-percent",
            "default: 10",
            "--index-builder-memory-bytes-per-kind",
            "default: 256 MiB",
            "--index-path-builder-memory-bytes",
            "--index-path-compaction-max-lanes",
            "--index-tensor-builder-memory-bytes",
            "--index-tensor-compaction-max-lanes",
            "--index-rayon-workers",
            "default: 4",
            "--index-max-retained-generations",
            "including current",
            "--index-max-generation-age-hours",
            "default: 24",
            "--index-max-retained-generation-bytes",
            "default: 50 GiB",
        ] {
            assert!(
                help.contains(expected),
                "help omitted `{expected}`:\n{help}"
            );
        }
    }
}
