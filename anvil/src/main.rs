use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;

use anvil::authentication::{JwtManager, RateLimitConfig, load_token_signing_key};
use anvil::observability::{Observability, ObservabilityConfig};
use anvil::{ServerConfig, serve};
use anyhow::{Context, Result};
use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "anvil-server", version, about = "Anvil 0.5 object server")]
struct Arguments {
    #[arg(long, env = "ANVIL_LISTEN", default_value = "127.0.0.1:50051")]
    listen: SocketAddr,

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

    #[arg(long, env = "ANVIL_MAX_BLOB_BYTES", default_value_t = 16 * 1024 * 1024 * 1024_u64)]
    max_blob_bytes: u64,

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

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = Arguments::parse();
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
        max_blob_bytes: arguments.max_blob_bytes,
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
