use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::coordination_service_client::CoordinationServiceClient;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum LeaseCommands {
    Acquire {
        task_id: String,
        task_kind: String,
        partition_family: String,
        partition_id: String,
        #[clap(long, default_value = "")]
        owner_label: String,
        #[clap(long, default_value_t = 0)]
        source_cursor_low: u64,
        #[clap(long, default_value_t = 0)]
        source_cursor_high: u64,
        #[clap(long, default_value_t = 30_000_000_000_i64)]
        ttl_nanos: i64,
    },
    Checkpoint {
        task_id: String,
        fence_token: u64,
        checkpoint_cursor_low: u64,
        checkpoint_cursor_high: u64,
    },
    Commit {
        task_id: String,
        fence_token: u64,
        committed_cursor_low: u64,
        committed_cursor_high: u64,
    },
    Read {
        task_id: String,
    },
    ForceRelease {
        task_id: String,
    },
}

pub async fn handle_lease_command(command: &LeaseCommands, ctx: &Context) -> anyhow::Result<()> {
    let mut client = CoordinationServiceClient::connect(ctx.profile.host.clone()).await?;
    let token = ctx.get_bearer_token().await?;
    match command {
        LeaseCommands::Acquire {
            task_id,
            task_kind,
            partition_family,
            partition_id,
            owner_label,
            source_cursor_low,
            source_cursor_high,
            ttl_nanos,
        } => {
            let mut request = tonic::Request::new(api::AcquireTaskLeaseRequest {
                task_id: task_id.clone(),
                task_kind: task_kind.clone(),
                partition_family: partition_family.clone(),
                partition_id: partition_id.clone(),
                owner_label: owner_label.clone(),
                source_cursor_low: *source_cursor_low,
                source_cursor_high: *source_cursor_high,
                requested_ttl_nanos: *ttl_nanos,
            });
            add_auth(&mut request, &token);
            print_lease(client.acquire_task_lease(request).await?.into_inner().lease);
        }
        LeaseCommands::Checkpoint {
            task_id,
            fence_token,
            checkpoint_cursor_low,
            checkpoint_cursor_high,
        } => {
            let lease = read_current_task_lease(&mut client, task_id, *fence_token, &token).await?;
            let mut request = tonic::Request::new(api::CheckpointTaskLeaseRequest {
                task_id: task_id.clone(),
                fence_token: lease.fence_token,
                checkpoint_cursor_low: *checkpoint_cursor_low,
                checkpoint_cursor_high: *checkpoint_cursor_high,
                expected_root_generation: lease.root_generation,
                expected_lease_epoch: lease.lease_epoch,
                expected_expires_at_nanos: lease.expires_at_nanos,
                expected_lease_hash: lease.lease_hash,
            });
            add_auth(&mut request, &token);
            print_lease(
                client
                    .checkpoint_task_lease(request)
                    .await?
                    .into_inner()
                    .lease,
            );
        }
        LeaseCommands::Commit {
            task_id,
            fence_token,
            committed_cursor_low,
            committed_cursor_high,
        } => {
            let lease = read_current_task_lease(&mut client, task_id, *fence_token, &token).await?;
            let mut request = tonic::Request::new(api::CommitTaskLeaseRequest {
                task_id: task_id.clone(),
                fence_token: lease.fence_token,
                committed_cursor_low: *committed_cursor_low,
                committed_cursor_high: *committed_cursor_high,
                expected_root_generation: lease.root_generation,
                expected_lease_epoch: lease.lease_epoch,
                expected_expires_at_nanos: lease.expires_at_nanos,
                expected_lease_hash: lease.lease_hash,
            });
            add_auth(&mut request, &token);
            let response = client.commit_task_lease(request).await?.into_inner();
            println!("committed={}", response.committed);
            print_lease(response.previous_lease);
        }
        LeaseCommands::Read { task_id } => {
            let mut request = tonic::Request::new(api::ReadTaskLeaseRequest {
                task_id: task_id.clone(),
            });
            add_auth(&mut request, &token);
            let response = client.read_task_lease(request).await?.into_inner();
            println!("found={}", response.found);
            print_lease(response.lease);
        }
        LeaseCommands::ForceRelease { task_id } => {
            let mut request = tonic::Request::new(api::ForceReleaseTaskLeaseRequest {
                task_id: task_id.clone(),
            });
            add_auth(&mut request, &token);
            let response = client.force_release_task_lease(request).await?.into_inner();
            println!("released={}", response.released);
            print_lease(response.previous_lease);
        }
    }
    Ok(())
}

async fn read_current_task_lease(
    client: &mut CoordinationServiceClient<tonic::transport::Channel>,
    task_id: &str,
    expected_fence_token: u64,
    token: &str,
) -> anyhow::Result<api::TaskLease> {
    let mut request = tonic::Request::new(api::ReadTaskLeaseRequest {
        task_id: task_id.to_string(),
    });
    add_auth(&mut request, token);
    let response = client.read_task_lease(request).await?.into_inner();
    if !response.found {
        anyhow::bail!("task lease {task_id:?} does not exist");
    }
    let lease = response
        .lease
        .ok_or_else(|| anyhow::anyhow!("task lease {task_id:?} response omitted the lease"))?;
    if lease.fence_token != expected_fence_token {
        anyhow::bail!(
            "task lease {task_id:?} has fence token {}, not expected fence token {expected_fence_token}",
            lease.fence_token
        );
    }
    Ok(lease)
}

fn print_lease(lease: Option<api::TaskLease>) {
    if let Some(lease) = lease {
        println!(
            "{}\tfence={}\towner={}\tcheckpoint={}:{}",
            lease.task_id,
            lease.fence_token,
            lease.owner_principal_id,
            lease.checkpoint_cursor_low,
            lease.checkpoint_cursor_high
        );
    }
}

fn add_auth<T>(request: &mut tonic::Request<T>, token: &str) {
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
}
