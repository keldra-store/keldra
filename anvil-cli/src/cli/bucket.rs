use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum BucketCommands {
    /// Create a new bucket
    Create {
        name: String,
        region: String,
        #[clap(long)]
        transaction_id: Option<String>,
    },
    /// Remove a bucket
    Rm {
        name: String,
        #[clap(long)]
        transaction_id: Option<String>,
    },
    /// List buckets
    Ls {
        #[clap(long, default_value_t = 100)]
        page_size: u32,
        #[clap(long, default_value = "")]
        page_token: String,
    },
    /// Set public access for a bucket
    SetPublic {
        name: String,
        #[clap(long, action = clap::ArgAction::Set)]
        allow: bool,
        #[clap(long)]
        transaction_id: Option<String>,
    },
}

pub async fn handle_bucket_command(command: &BucketCommands, ctx: &Context) -> anyhow::Result<()> {
    let mut client = BucketServiceClient::connect(ctx.profile.host.clone()).await?;
    let token = ctx.get_bearer_token().await?;

    match command {
        BucketCommands::Create {
            name,
            region,
            transaction_id,
        } => {
            let mut request = tonic::Request::new(api::CreateBucketRequest {
                bucket_name: name.clone(),
                region: region.clone(),
                options: write_options(transaction_id),
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            client.create_bucket(request).await?;
            println!("Bucket {} created", name);
        }
        BucketCommands::Rm {
            name,
            transaction_id,
        } => {
            let mut request = tonic::Request::new(api::DeleteBucketRequest {
                bucket_name: name.clone(),
                options: write_options(transaction_id),
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            client.delete_bucket(request).await?;
            println!("Bucket {} deleted", name);
        }
        BucketCommands::Ls {
            page_size,
            page_token,
        } => {
            let mut request = tonic::Request::new(api::ListBucketsRequest {
                page: Some(api::PageRequest {
                    page_size: *page_size,
                    page_token: page_token.clone(),
                }),
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            let response = client.list_buckets(request).await?.into_inner();
            for bucket in response.buckets {
                println!("{}\t{}", bucket.name, bucket.creation_date);
            }
            if let Some(page) = response
                .page
                .filter(|page| !page.next_page_token.is_empty())
            {
                println!("next_page_token={}", page.next_page_token);
            }
        }
        BucketCommands::SetPublic {
            name,
            allow,
            transaction_id,
        } => {
            let mut request = tonic::Request::new(api::PutBucketPolicyRequest {
                bucket_name: name.clone(),
                policy_json: format!("{{\"is_public_read\": {}}}", allow),
                options: write_options(transaction_id),
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            client.put_bucket_policy(request).await?;
            println!("Public access for bucket {} set to {}", name, allow);
        }
    }
    Ok(())
}

fn write_options(transaction_id: &Option<String>) -> Option<api::WriteOptions> {
    transaction_id
        .as_ref()
        .map(|transaction_id| api::WriteOptions {
            idempotency_key: uuid::Uuid::new_v4().to_string(),
            consistency: api::ConsistencyMode::Committed as i32,
            wait_for_finalization: false,
            preconditions: Vec::new(),
            boundary_values: Vec::new(),
            execution: Some(api::write_options::Execution::TransactionId(
                transaction_id.clone(),
            )),
        })
}
