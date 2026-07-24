#![allow(dead_code)]
#![recursion_limit = "512"]

#[path = "../cli/admin.rs"]
mod admin_cli;
#[path = "../config.rs"]
mod config;
#[path = "../context.rs"]
mod context;

use crate::context::Context;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[clap(author, version, about = "Network administrative client for Anvil", long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
    #[clap(long, global = true)]
    profile: Option<String>,
    #[clap(long, global = true)]
    config: Option<String>,
    #[clap(long, global = true, env = "ANVIL_ADMIN_ENDPOINT")]
    host: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate local secret material for server configuration
    Key {
        #[clap(subcommand)]
        command: KeyCommands,
    },
    #[clap(flatten)]
    Admin(admin_cli::AdminCommands),
}

#[derive(Subcommand)]
enum KeyCommands {
    /// Generate a new ANVIL_SECRET_ENCRYPTION_KEY value
    GenerateSecretEncryptionKey,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match &cli.command {
        Commands::Key {
            command: KeyCommands::GenerateSecretEncryptionKey,
        } => {
            println!("{}", anvil::crypto::generate_key_hex());
            eprintln!(
                "Generated one ANVIL_SECRET_ENCRYPTION_KEY. Create it once per Anvil storage cluster, store it in a secret manager, never commit it, and keep it securely. Losing it makes encrypted secrets unrecoverable. If it leaks, configure a new active key, keep the old key in ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS, run admin secret-encryption-key rotate, then remove the old key after verification."
            );
        }
        Commands::Admin(command) => {
            let ctx = Context::admin(cli.profile, cli.config, cli.host)?;
            admin_cli::handle_admin_command(command, &ctx).await?;
        }
    }
    Ok(())
}
