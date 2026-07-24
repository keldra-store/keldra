use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum HostAliasCommands {
    /// Create a pending host alias for an owned bucket.
    Create {
        hostname: String,
        bucket_name: String,
        #[clap(long)]
        region: Option<String>,
        #[clap(long, default_value = "")]
        prefix: String,
        #[clap(long)]
        transaction_id: Option<String>,
    },
    /// Read host alias metadata.
    Read { hostname: String },
    /// Verify a pending host alias with the observed DNS/domain challenge value.
    Verify {
        hostname: String,
        observed_challenge: String,
        #[clap(long)]
        expected_generation: u64,
        #[clap(long)]
        transaction_id: Option<String>,
    },
    /// List host aliases visible to the current tenant application.
    List {
        #[clap(long)]
        region: Option<String>,
        #[clap(long, default_value_t = 100)]
        limit: u32,
    },
    /// Delete a tenant-owned host alias.
    Delete {
        hostname: String,
        #[clap(long)]
        expected_generation: u64,
        #[clap(long)]
        transaction_id: Option<String>,
    },
}

pub async fn handle_host_alias_command(
    command: &HostAliasCommands,
    ctx: &Context,
) -> anyhow::Result<()> {
    let mut client = ObjectServiceClient::connect(ctx.profile.host.clone()).await?;
    let token = ctx.get_bearer_token().await?;

    match command {
        HostAliasCommands::Create {
            hostname,
            bucket_name,
            region,
            prefix,
            transaction_id,
        } => {
            let mut request = tonic::Request::new(api::CreateHostAliasRequest {
                context: Some(public_context(
                    "host-alias-create",
                    0,
                    transaction_id.clone(),
                )),
                hostname: hostname.clone(),
                tenant_id: String::new(),
                bucket_name: bucket_name.clone(),
                region: region.clone().unwrap_or_default(),
                prefix: prefix.clone(),
            });
            attach_auth(&mut request, &token)?;
            print_host_alias(
                client
                    .create_host_alias(request)
                    .await?
                    .into_inner()
                    .host_alias,
            );
        }
        HostAliasCommands::Read { hostname } => {
            let mut request = tonic::Request::new(api::ReadHostAliasRequest {
                request_id: format!("host-alias-read-{}", uuid::Uuid::new_v4()),
                hostname: hostname.clone(),
            });
            attach_auth(&mut request, &token)?;
            print_host_alias(
                client
                    .read_host_alias(request)
                    .await?
                    .into_inner()
                    .host_alias,
            );
        }
        HostAliasCommands::Verify {
            hostname,
            observed_challenge,
            expected_generation,
            transaction_id,
        } => {
            let mut request = tonic::Request::new(api::VerifyHostAliasRequest {
                context: Some(public_context(
                    "host-alias-verify",
                    *expected_generation,
                    transaction_id.clone(),
                )),
                hostname: hostname.clone(),
                observed_challenge: observed_challenge.clone(),
            });
            attach_auth(&mut request, &token)?;
            print_host_alias(
                client
                    .verify_host_alias(request)
                    .await?
                    .into_inner()
                    .host_alias,
            );
        }
        HostAliasCommands::List { region, limit } => {
            let mut request = tonic::Request::new(api::ListHostAliasesRequest {
                region: region.clone().unwrap_or_default(),
                page: Some(api::PageRequest {
                    page_token: String::new(),
                    page_size: *limit,
                }),
            });
            attach_auth(&mut request, &token)?;
            let response = client.list_host_aliases(request).await?.into_inner();
            for alias in response.host_aliases {
                println!(
                    "{}\t{}\t{}\t{}\t{}",
                    alias.hostname,
                    alias.bucket_name,
                    alias.region,
                    host_alias_state_name(alias.state),
                    alias.generation
                );
            }
        }
        HostAliasCommands::Delete {
            hostname,
            expected_generation,
            transaction_id,
        } => {
            let mut request = tonic::Request::new(api::DeleteHostAliasRequest {
                context: Some(public_context(
                    "host-alias-delete",
                    *expected_generation,
                    transaction_id.clone(),
                )),
                hostname: hostname.clone(),
            });
            attach_auth(&mut request, &token)?;
            let response = client.delete_host_alias(request).await?.into_inner();
            println!(
                "Deleted {} at generation {}",
                response.resource_id, response.generation
            );
        }
    }

    Ok(())
}

fn public_context(
    tag: &str,
    expected_generation: u64,
    transaction_id: Option<String>,
) -> api::PublicMutationContext {
    api::PublicMutationContext {
        request_id: format!("{tag}-{}", uuid::Uuid::new_v4()),
        idempotency_key: uuid::Uuid::new_v4().to_string(),
        expected_generation,
        transaction_id,
        saga_operation: None,
        saga_compensation_operation: None,
    }
}

fn attach_auth<T>(request: &mut tonic::Request<T>, token: &str) -> anyhow::Result<()> {
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse()?);
    Ok(())
}

fn print_host_alias(alias: Option<api::HostAliasDescriptor>) {
    if let Some(alias) = alias {
        println!(
            "{} -> {}/{} ({}, generation {})",
            alias.hostname,
            alias.bucket_name,
            alias.prefix,
            host_alias_state_name(alias.state),
            alias.generation
        );
        if !alias.verification_challenge.is_empty() {
            println!("verification_challenge={}", alias.verification_challenge);
        }
    }
}

fn host_alias_state_name(value: i32) -> &'static str {
    match api::HostAliasState::try_from(value) {
        Ok(api::HostAliasState::PendingVerification) => "pending_verification",
        Ok(api::HostAliasState::Active) => "active",
        Ok(api::HostAliasState::Suspended) => "suspended",
        Ok(api::HostAliasState::Deleted) => "deleted",
        _ => "unspecified",
    }
}
