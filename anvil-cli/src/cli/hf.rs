use crate::context::Context;
use anvil::anvil_api::{
    self as api, hf_ingestion_service_client::HfIngestionServiceClient,
    hugging_face_key_service_client::HuggingFaceKeyServiceClient,
};
use clap::Subcommand;

#[derive(Subcommand)]
pub enum HfCommands {
    /// Manage keys
    Key {
        #[clap(subcommand)]
        command: HfKeyCommands,
    },
    /// Manage ingestions
    Ingest {
        #[clap(subcommand)]
        command: HfIngestCommands,
    },
}

#[derive(Subcommand)]
pub enum HfKeyCommands {
    /// Add a named key
    Add {
        #[clap(long)]
        name: String,
        #[clap(long)]
        token: String,
        #[clap(long)]
        note: Option<String>,
    },
    /// List keys
    Ls {
        #[clap(long, default_value_t = 100)]
        page_size: u32,
        #[clap(long, default_value = "")]
        page_token: String,
    },
    /// Remove a key
    Rm {
        #[clap(long)]
        name: String,
    },
}

#[derive(Subcommand)]
pub enum HfIngestCommands {
    /// Start an ingestion
    Start {
        #[clap(long)]
        key: String,
        #[clap(long)]
        repo: String,
        #[clap(long)]
        revision: Option<String>,
        #[clap(long)]
        bucket: String,
        #[clap(long)]
        target_region: String,
        #[clap(long)]
        prefix: Option<String>,
        #[clap(long)]
        include: Vec<String>,
        #[clap(long)]
        exclude: Vec<String>,
    },
    /// Get status
    Status {
        #[clap(long)]
        id: String,
    },
    /// Cancel an ingestion
    Cancel {
        #[clap(long)]
        id: String,
    },
}

pub async fn handle_hf_command(command: &HfCommands, ctx: &Context) -> anyhow::Result<()> {
    let token = ctx.get_bearer_token().await?;

    match command {
        HfCommands::Key { command } => {
            let mut client: HuggingFaceKeyServiceClient<tonic::transport::Channel> =
                HuggingFaceKeyServiceClient::connect(ctx.profile.host.clone()).await?;
            match command {
                HfKeyCommands::Add { name, token, note } => {
                    let mut request = tonic::Request::new(api::CreateHfKeyRequest {
                        name: name.clone(),
                        token: token.clone(),
                        note: note.clone().unwrap_or_default(),
                    });
                    request.metadata_mut().insert(
                        "authorization",
                        format!("Bearer {}", ctx.get_bearer_token().await?)
                            .parse()
                            .unwrap(),
                    );
                    let resp = client.create_key(request).await?;
                    println!("created key: {}", resp.into_inner().name);
                }
                HfKeyCommands::Ls {
                    page_size,
                    page_token,
                } => {
                    let mut request = tonic::Request::new(api::ListHfKeysRequest {
                        page: Some(api::PageRequest {
                            page_size: *page_size,
                            page_token: page_token.clone(),
                        }),
                    });
                    request.metadata_mut().insert(
                        "authorization",
                        format!("Bearer {}", token).parse().unwrap(),
                    );
                    let response = client.list_keys(request).await?.into_inner();
                    for k in response.keys {
                        println!("{}\t{}", k.name, k.updated_at);
                    }
                    if let Some(page) = response
                        .page
                        .filter(|page| !page.next_page_token.is_empty())
                    {
                        println!("next_page_token={}", page.next_page_token);
                    }
                }
                HfKeyCommands::Rm { name } => {
                    let mut request =
                        tonic::Request::new(api::DeleteHfKeyRequest { name: name.clone() });
                    request.metadata_mut().insert(
                        "authorization",
                        format!("Bearer {}", token).parse().unwrap(),
                    );
                    client.delete_key(request).await?;
                    println!("deleted key: {}", name);
                }
            }
        }
        HfCommands::Ingest { command } => {
            let mut client: HfIngestionServiceClient<tonic::transport::Channel> =
                HfIngestionServiceClient::connect(ctx.profile.host.clone()).await?;
            match command {
                HfIngestCommands::Start {
                    key,
                    repo,
                    revision,
                    bucket,
                    target_region,
                    prefix,
                    include,
                    exclude,
                } => {
                    let mut request = tonic::Request::new(api::StartHfIngestionRequest {
                        key_name: key.clone(),
                        repo: repo.clone(),
                        revision: revision.clone().unwrap_or_default(),
                        target_bucket: bucket.clone(),
                        target_prefix: prefix.clone().unwrap_or_else(|| repo.clone()),
                        include_globs: include.clone(),
                        exclude_globs: exclude.clone(),
                        target_region: target_region.clone(),
                    });
                    request.metadata_mut().insert(
                        "authorization",
                        format!("Bearer {}", token).parse().unwrap(),
                    );
                    let resp = client.start_ingestion(request).await?;
                    println!("ingestion id: {}", resp.into_inner().ingestion_id);
                }
                HfIngestCommands::Status { id } => {
                    let mut request = tonic::Request::new(api::GetHfIngestionStatusRequest {
                        ingestion_id: id.clone(),
                    });
                    request.metadata_mut().insert(
                        "authorization",
                        format!("Bearer {}", token).parse().unwrap(),
                    );
                    let resp = client.get_ingestion_status(request).await?;
                    let s = resp.into_inner();
                    println!(
                        "state={} queued={} downloading={} stored={} failed={} error={}",
                        s.state, s.queued, s.downloading, s.stored, s.failed, s.error
                    );
                }
                HfIngestCommands::Cancel { id } => {
                    let mut request = tonic::Request::new(api::CancelHfIngestionRequest {
                        ingestion_id: id.clone(),
                    });
                    request.metadata_mut().insert(
                        "authorization",
                        format!("Bearer {}", token).parse().unwrap(),
                    );
                    client.cancel_ingestion(request).await?;
                    println!("canceled: {}", id);
                }
            }
        }
    }

    Ok(())
}
