use std::net::SocketAddr;
use std::path::PathBuf;

use anvil::{ServerConfig, serve};
use anyhow::Result;
use clap::Parser;
use tracing_subscriber::EnvFilter;

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
        env = "ANVIL_API_TOKEN",
        hide_env_values = true,
        default_value = ""
    )]
    api_token: String,

    #[arg(long, env = "ANVIL_INSECURE_NO_AUTH", default_value_t = false)]
    insecure_no_auth: bool,

    #[arg(long, env = "ANVIL_MAX_BLOB_BYTES", default_value_t = 16 * 1024 * 1024 * 1024_u64)]
    max_blob_bytes: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let arguments = Arguments::parse();
    serve(ServerConfig {
        listen: arguments.listen,
        data_dir: arguments.data_dir,
        run_system_bootstrap: arguments.run_system_bootstrap,
        system_bootstrap_credential_output: arguments.system_bootstrap_credential_output,
        node_id: arguments.node_id,
        max_atomic_commit_entries: arguments.max_atomic_commit_entries,
        max_atomic_commit_bytes: arguments.max_atomic_commit_bytes,
        api_token: arguments.api_token,
        insecure_no_auth: arguments.insecure_no_auth,
        max_blob_bytes: arguments.max_blob_bytes,
    })
    .await
}
