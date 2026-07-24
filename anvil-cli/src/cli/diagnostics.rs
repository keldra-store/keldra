use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::index_service_client::IndexServiceClient;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum DiagnosticsCommands {
    List {
        bucket: String,
        index: String,
        #[clap(long, default_value_t = 100)]
        page_size: u32,
        #[clap(long, default_value = "")]
        page_token: String,
        #[clap(long, default_value = "")]
        severity: String,
    },
}

pub async fn handle_diagnostics_command(
    command: &DiagnosticsCommands,
    ctx: &Context,
) -> anyhow::Result<()> {
    let mut client = IndexServiceClient::connect(ctx.profile.host.clone()).await?;
    let token = ctx.get_bearer_token().await?;
    match command {
        DiagnosticsCommands::List {
            bucket,
            index,
            page_size,
            page_token,
            severity,
        } => {
            let mut request = tonic::Request::new(api::ListIndexDiagnosticsRequest {
                bucket_name: bucket.clone(),
                index_name: index.clone(),
                severity: severity.clone(),
                page: Some(api::PageRequest {
                    page_size: *page_size,
                    page_token: page_token.clone(),
                }),
            });
            request
                .metadata_mut()
                .insert("authorization", format!("Bearer {token}").parse().unwrap());
            let response = client.list_index_diagnostics(request).await?.into_inner();
            for diagnostic in response.diagnostics {
                println!(
                    "{}\t{}\t{}\t{}",
                    diagnostic.cursor, diagnostic.severity, diagnostic.code, diagnostic.message
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
