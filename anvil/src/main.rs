#![recursion_limit = "512"]

use anvil::run;
use clap::Parser;
use std::net::SocketAddr;
use tracing::info;

use anvil::config::Config;

const ANVIL_TOKIO_WORKER_STACK_BYTES: usize = 8 * 1024 * 1024;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(ANVIL_TOKIO_WORKER_STACK_BYTES)
        .build()?
        .block_on(async_main())
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let mut config = Config::parse();
    // The process-backed integration fixture needs real child processes and
    // independent listeners, but deliberately uses loopback h2c rather than
    // provisioning certificates. Keep this escape hatch out of release
    // builds: production peer transport remains TLS-only.
    #[cfg(debug_assertions)]
    if std::env::var_os("ANVIL_TEST_ALLOW_INSECURE_MVCC_TRANSPORT").is_some() {
        config.allow_test_only_insecure_mvcc_transport = true;
    }
    config.validate_admin_listener_bind()?;

    let addr = config
        .api_listen_addr
        .parse::<SocketAddr>()
        .expect("Invalid gRPC bind address");
    let admin_addr = config
        .admin_listen_addr
        .parse::<SocketAddr>()
        .expect("Invalid admin gRPC bind address");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let admin_listener = tokio::net::TcpListener::bind(admin_addr).await?;

    info!("Anvil server (gRPC & S3) listening on {}", addr);
    info!("Anvil admin server (gRPC) listening on {}", admin_addr);

    run(listener, admin_listener, config).await?;
    Ok(())
}
