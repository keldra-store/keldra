use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::personal_db_service_client::PersonalDbServiceClient;
use clap::{Args, Subcommand};
use tokio_stream::StreamExt;

#[derive(Subcommand)]
pub enum PersonalDbCommands {
    Group {
        #[clap(subcommand)]
        command: GroupCommands,
    },
    Projection {
        #[clap(subcommand)]
        command: ProjectionCommands,
    },
    Changeset {
        #[clap(subcommand)]
        command: ChangesetCommands,
    },
    CatchUp(CatchUpArgs),
    Watch {
        database_id: String,
        #[clap(long, default_value_t = 0)]
        after_cursor_low: u64,
        #[clap(long, default_value_t = 0)]
        after_cursor_high: u64,
    },
}

#[derive(Subcommand)]
pub enum GroupCommands {
    Create {
        database_id: String,
        schema_hash: String,
        genesis_hash: String,
        #[clap(long, default_value = "")]
        schema_sql: String,
    },
    Read {
        database_id: String,
    },
}

#[derive(Subcommand)]
pub enum ProjectionCommands {
    Create {
        database_id: String,
        projection_definition_json: String,
    },
    Read {
        database_id: String,
        projection_id: String,
    },
}

#[derive(Subcommand)]
pub enum ChangesetCommands {
    Submit(SubmitChangesetArgs),
}

#[derive(Args)]
pub struct SubmitChangesetArgs {
    database_id: String,
    payload_file: String,
    #[clap(long, default_value_t = 0)]
    base_log_index: u64,
    #[clap(long, default_value = "")]
    base_log_hash: String,
    #[clap(long, default_value_t = 0)]
    client_log_epoch: u64,
    #[clap(long, default_value_t = 0)]
    membership_epoch: u64,
    #[clap(long, default_value_t = 0)]
    policy_epoch: u64,
    #[clap(long, default_value = "cli")]
    leader_replica_id: String,
}

#[derive(Args)]
pub struct CatchUpArgs {
    database_id: String,
    #[clap(long, default_value = "cli")]
    replica_id: String,
    #[clap(long, default_value_t = 0)]
    have_log_index: u64,
    #[clap(long, default_value = "")]
    have_log_hash: String,
    #[clap(long, default_value_t = 100)]
    max_entries: u32,
}

pub async fn handle_personaldb_command(
    command: &PersonalDbCommands,
    ctx: &Context,
) -> anyhow::Result<()> {
    let mut client = PersonalDbServiceClient::connect(ctx.profile.host.clone()).await?;
    let token = ctx.get_bearer_token().await?;
    let claims = crate::cli::object::decode_native_token_claims(&token)?;
    match command {
        PersonalDbCommands::Group {
            command:
                GroupCommands::Create {
                    database_id,
                    schema_hash,
                    genesis_hash,
                    schema_sql,
                },
        } => {
            let mut request = tonic::Request::new(api::CreatePersonalDbGroupRequest {
                database_id: database_id.clone(),
                schema_hash: schema_hash.clone(),
                genesis_hash: genesis_hash.clone(),
                schema_sql: schema_sql.clone(),
                options: None,
            });
            add_auth(&mut request, &token);
            print_group(client.create_personal_db_group(request).await?.into_inner());
        }
        PersonalDbCommands::Group {
            command: GroupCommands::Read { database_id },
        } => {
            let mut request = tonic::Request::new(api::GetPersonalDbGroupRequest {
                tenant_id: claims.tenant_id,
                database_id: database_id.clone(),
            });
            add_auth(&mut request, &token);
            print_group(client.get_personal_db_group(request).await?.into_inner());
        }
        PersonalDbCommands::Projection {
            command:
                ProjectionCommands::Create {
                    database_id,
                    projection_definition_json,
                },
        } => {
            let mut request = tonic::Request::new(api::CreatePersonalDbProjectionRequest {
                tenant_id: claims.tenant_id,
                database_id: database_id.clone(),
                projection_definition_json: projection_definition_json.clone(),
                options: None,
            });
            add_auth(&mut request, &token);
            println!(
                "{}",
                client
                    .create_personal_db_projection(request)
                    .await?
                    .into_inner()
                    .projection_definition_json
            );
        }
        PersonalDbCommands::Projection {
            command:
                ProjectionCommands::Read {
                    database_id,
                    projection_id,
                },
        } => {
            let mut request = tonic::Request::new(api::GetPersonalDbProjectionRequest {
                tenant_id: claims.tenant_id,
                database_id: database_id.clone(),
                projection_id: projection_id.clone(),
            });
            add_auth(&mut request, &token);
            println!(
                "{}",
                client
                    .get_personal_db_projection(request)
                    .await?
                    .into_inner()
                    .projection_definition_json
            );
        }
        PersonalDbCommands::Changeset {
            command: ChangesetCommands::Submit(args),
        } => {
            let payload = tokio::fs::read(&args.payload_file).await?;
            let mut request = tonic::Request::new(api::SubmitPersonalDbChangesetRequest {
                tenant_id: claims.tenant_id,
                database_id: args.database_id.clone(),
                principal: claims.sub.clone(),
                session_token: String::new(),
                request_id: format!("personaldb-submit-{}", uuid::Uuid::new_v4()),
                idempotency_key: uuid::Uuid::new_v4().to_string(),
                base_log_index: args.base_log_index,
                base_log_hash: args.base_log_hash.clone(),
                client_log_epoch: args.client_log_epoch,
                membership_epoch: args.membership_epoch,
                policy_epoch: args.policy_epoch,
                leader_replica_id: args.leader_replica_id.clone(),
                voter_acks: Vec::new(),
                changeset_payload_hash: blake3::hash(&payload).to_hex().to_string(),
                changeset_bytes: payload,
                client_debug_metadata_json: String::new(),
                options: None,
            });
            add_auth(&mut request, &token);
            let response = client
                .submit_personal_db_changeset(request)
                .await?
                .into_inner();
            println!(
                "log_index={} log_hash={}",
                response.log_index, response.log_hash
            );
        }
        PersonalDbCommands::CatchUp(args) => {
            let mut request = tonic::Request::new(api::PersonalDbCatchUpRequest {
                tenant_id: claims.tenant_id,
                database_id: args.database_id.clone(),
                principal: claims.sub.clone(),
                replica_id: args.replica_id.clone(),
                have_log_index: args.have_log_index,
                have_log_hash: args.have_log_hash.clone(),
                max_entries: args.max_entries,
            });
            add_auth(&mut request, &token);
            let response = client.catch_up_personal_db(request).await?.into_inner();
            println!(
                "entries={} has_more={} snapshot_required={}",
                response.entries.len(),
                response.has_more,
                response.snapshot_required
            );
        }
        PersonalDbCommands::Watch {
            database_id,
            after_cursor_low,
            after_cursor_high,
        } => {
            let mut request = tonic::Request::new(api::WatchPersonalDbGroupRequest {
                tenant_id: claims.tenant_id,
                database_id: database_id.clone(),
                after_cursor_low: *after_cursor_low,
                after_cursor_high: *after_cursor_high,
            });
            add_auth(&mut request, &token);
            let mut stream = client.watch_personal_db_group(request).await?.into_inner();
            while let Some(item) = stream.next().await {
                let item = item?;
                println!(
                    "{}:{}\t{}",
                    item.cursor_low, item.cursor_high, item.event_type
                );
            }
        }
    }
    Ok(())
}

fn print_group(response: api::PersonalDbGroupResponse) {
    if let Some(manifest) = response.manifest {
        println!(
            "{}\t{}\t{}",
            manifest.tenant_id, manifest.database_id, manifest.schema_hash
        );
    }
}

fn add_auth<T>(request: &mut tonic::Request<T>, token: &str) {
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
}
