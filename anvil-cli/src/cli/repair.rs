use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::repair_service_client::RepairServiceClient;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum RepairCommands {
    Run {
        #[clap(subcommand)]
        target: RepairTarget,
    },
    Findings {
        scope_kind: String,
        scope_id: String,
        #[clap(long, default_value_t = 100)]
        page_size: u32,
        #[clap(long, default_value = "")]
        page_token: String,
    },
}

#[derive(Subcommand)]
pub enum RepairTarget {
    Index {
        bucket: String,
        index: String,
        #[clap(long, action = clap::ArgAction::SetTrue)]
        rebuild: bool,
    },
    Directory {
        bucket: String,
        #[clap(long, action = clap::ArgAction::SetTrue)]
        rebuild: bool,
    },
    AuthzDerived {
        derived_index_id: String,
        #[clap(long, action = clap::ArgAction::SetTrue)]
        rebuild: bool,
    },
    PersonalDb {
        database_id: String,
    },
}

pub async fn handle_repair_command(command: &RepairCommands, ctx: &Context) -> anyhow::Result<()> {
    let mut client = RepairServiceClient::connect(ctx.profile.host.clone()).await?;
    let token = ctx.get_bearer_token().await?;
    match command {
        RepairCommands::Run { target } => match target {
            RepairTarget::Index {
                bucket,
                index,
                rebuild,
            } => {
                let mut request = tonic::Request::new(api::RepairIndexRequest {
                    bucket_name: bucket.clone(),
                    index_name: index.clone(),
                    rebuild: *rebuild,
                });
                add_auth(&mut request, &token);
                let response = client.repair_index(request).await?.into_inner();
                println!(
                    "{}\t{}\t{}",
                    response.status, response.bucket_name, response.index_name
                );
            }
            RepairTarget::Directory { bucket, rebuild } => {
                let mut request = tonic::Request::new(api::RepairDirectoryIndexRequest {
                    bucket_name: bucket.clone(),
                    rebuild: *rebuild,
                });
                add_auth(&mut request, &token);
                let response = client.repair_directory_index(request).await?.into_inner();
                println!(
                    "{}\t{}\t{}",
                    response.status, response.bucket_name, response.reason
                );
            }
            RepairTarget::AuthzDerived {
                derived_index_id,
                rebuild,
            } => {
                let mut request = tonic::Request::new(api::RepairAuthzDerivedIndexRequest {
                    derived_index_id: derived_index_id.clone(),
                    rebuild: *rebuild,
                });
                add_auth(&mut request, &token);
                let response = client
                    .repair_authz_derived_index(request)
                    .await?
                    .into_inner();
                println!("{}\t{}", response.status, response.derived_index_id);
            }
            RepairTarget::PersonalDb { database_id } => {
                let mut request = tonic::Request::new(api::RepairPersonalDbLogChainRequest {
                    database_id: database_id.clone(),
                });
                add_auth(&mut request, &token);
                let response = client
                    .repair_personal_db_log_chain(request)
                    .await?
                    .into_inner();
                println!(
                    "{}\t{}\t{}",
                    response.status, response.tenant_id, response.database_id
                );
            }
        },
        RepairCommands::Findings {
            scope_kind,
            scope_id,
            page_size,
            page_token,
        } => {
            let mut request = tonic::Request::new(api::ListRepairFindingsRequest {
                scope_kind: scope_kind.clone(),
                scope_id: scope_id.clone(),
                page: Some(api::PageRequest {
                    page_size: *page_size,
                    page_token: page_token.clone(),
                }),
            });
            add_auth(&mut request, &token);
            let response = client.list_repair_findings(request).await?.into_inner();
            for finding in response.findings {
                println!(
                    "{}\t{}\t{}\t{}",
                    finding.finding_id, finding.severity, finding.status, finding.message
                );
            }
            if let Some(page) = response
                .page
                .filter(|page| !page.next_page_token.is_empty())
            {
                println!("next_page_token={}", page.next_page_token);
            }
        }
    }
    Ok(())
}

fn add_auth<T>(request: &mut tonic::Request<T>, token: &str) {
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
}
