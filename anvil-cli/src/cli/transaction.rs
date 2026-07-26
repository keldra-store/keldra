use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::transaction_service_client::TransactionServiceClient;
use clap::{Subcommand, ValueEnum};

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum TransactionReadConsistency {
    LocalSnapshot,
    Linearized,
}

impl TransactionReadConsistency {
    fn as_proto(self) -> i32 {
        match self {
            Self::LocalSnapshot => api::MvccReadConsistency::LocalSnapshot as i32,
            Self::Linearized => api::MvccReadConsistency::Linearized as i32,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum TransactionDurability {
    Local,
    Quorum,
    Erasure,
}

impl TransactionDurability {
    fn as_proto(self) -> i32 {
        match self {
            Self::Local => api::MvccDurability::Local as i32,
            Self::Quorum => api::MvccDurability::Quorum as i32,
            Self::Erasure => api::MvccDurability::Erasure as i32,
        }
    }
}

#[derive(Subcommand)]
pub enum TransactionCommands {
    /// Begin an explicit cluster-scoped transaction.
    Begin {
        #[clap(long)]
        cluster_id: String,
        #[clap(long, default_value_t = 60_000)]
        ttl_ms: u64,
        #[clap(long, value_enum, default_value = "linearized")]
        read_consistency: TransactionReadConsistency,
        #[clap(long, value_enum, default_value = "quorum")]
        durability: TransactionDurability,
        #[clap(long)]
        idempotency_key: Option<String>,
    },
    /// Commit an explicit transaction.
    Commit {
        transaction_id: String,
        #[clap(long)]
        cluster_id: String,
    },
    /// Roll back an explicit transaction.
    Rollback {
        transaction_id: String,
        #[clap(long)]
        cluster_id: String,
        #[clap(long, default_value = "cli-request")]
        reason: String,
    },
    /// Read explicit transaction status.
    Get {
        transaction_id: String,
        #[clap(long)]
        cluster_id: String,
    },
}

pub async fn handle_transaction_command(
    command: &TransactionCommands,
    ctx: &Context,
) -> anyhow::Result<()> {
    let mut client = TransactionServiceClient::connect(ctx.profile.host.clone()).await?;
    let token = ctx.get_bearer_token().await?;
    match command {
        TransactionCommands::Begin {
            cluster_id,
            ttl_ms,
            read_consistency,
            durability,
            idempotency_key,
        } => {
            let mut request = tonic::Request::new(api::BeginTransactionRequest {
                idempotency_key: idempotency_key
                    .clone()
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
                ttl_ms: *ttl_ms,
                read_consistency: read_consistency.as_proto(),
                cluster_id: cluster_id.clone(),
                durability: durability.as_proto(),
            });
            attach_bearer(&mut request, &token);
            let response = client.begin_transaction(request).await?.into_inner();
            println!("request_id={}", response.request_id);
            println!("transaction_id={}", response.transaction_id);
            println!("cluster_id={}", response.cluster_id);
            println!("state={}", response.state);
            println!("snapshot_version={}", response.snapshot_version);
            println!("expires_at_unix_ms={}", response.expires_at_unix_ms);
            println!(
                "durability={:?}",
                api::MvccDurability::try_from(response.durability)
            );
        }
        TransactionCommands::Commit {
            transaction_id,
            cluster_id,
        } => {
            let mut request = tonic::Request::new(api::CommitTransactionRequest {
                transaction_id: transaction_id.clone(),
                cluster_id: cluster_id.clone(),
            });
            attach_bearer(&mut request, &token);
            let response = client.commit_transaction(request).await?.into_inner();
            println!("request_id={}", response.request_id);
            println!("mutation_id={}", response.mutation_id);
            println!("state={:?}", api::WriteState::try_from(response.state));
            println!("idempotency_outcome={}", response.idempotency_outcome);
        }
        TransactionCommands::Rollback {
            transaction_id,
            cluster_id,
            reason,
        } => {
            let mut request = tonic::Request::new(api::RollbackTransactionRequest {
                transaction_id: transaction_id.clone(),
                reason: reason.clone(),
                cluster_id: cluster_id.clone(),
            });
            attach_bearer(&mut request, &token);
            let response = client.rollback_transaction(request).await?.into_inner();
            println!("request_id={}", response.request_id);
            println!("transaction_id={}", response.transaction_id);
            println!("state={}", response.state);
        }
        TransactionCommands::Get {
            transaction_id,
            cluster_id,
        } => {
            let mut request = tonic::Request::new(api::GetTransactionRequest {
                transaction_id: transaction_id.clone(),
                cluster_id: cluster_id.clone(),
            });
            attach_bearer(&mut request, &token);
            let response = client.get_transaction(request).await?.into_inner();
            println!("transaction_id={}", response.transaction_id);
            println!("cluster_id={}", response.cluster_id);
            println!("state={}", response.state);
            println!("snapshot_version={}", response.snapshot_version);
            println!("expires_at_unix_ms={}", response.expires_at_unix_ms);
            println!(
                "durability={:?}",
                api::MvccDurability::try_from(response.durability)
            );
            if let Some(commit_version) = response.commit_version {
                println!("commit_version={commit_version}");
            }
            if let Some(error) = response.error {
                println!("error={} {}", error.code, error.message);
            }
        }
    }
    Ok(())
}

fn attach_bearer<T>(request: &mut tonic::Request<T>, token: &str) {
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
}
