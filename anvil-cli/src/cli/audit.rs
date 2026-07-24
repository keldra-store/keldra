use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::audit_service_client::AuditServiceClient;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum AuditCommands {
    List {
        #[clap(long, default_value = "")]
        principal: String,
        #[clap(long, default_value = "")]
        resource: String,
        #[clap(long, default_value = "")]
        action: String,
        #[clap(long, default_value_t = 100)]
        page_size: u32,
        #[clap(long, default_value = "")]
        page_token: String,
    },
}

pub async fn handle_audit_command(command: &AuditCommands, ctx: &Context) -> anyhow::Result<()> {
    let mut client = AuditServiceClient::connect(ctx.profile.host.clone()).await?;
    let token = ctx.get_bearer_token().await?;
    match command {
        AuditCommands::List {
            principal,
            resource,
            action,
            page_size,
            page_token,
        } => {
            let mut request = tonic::Request::new(api::ListAuditEventsRequest {
                request_id: format!("audit-list-{}", uuid::Uuid::new_v4()),
                principal_id: principal.clone(),
                resource_id: resource.clone(),
                action: action.clone(),
                page: Some(api::PageRequest {
                    page_token: page_token.clone(),
                    page_size: *page_size,
                }),
            });
            request
                .metadata_mut()
                .insert("authorization", format!("Bearer {token}").parse().unwrap());
            let response = client.list_tenant_audit_events(request).await?.into_inner();
            for event in response.events {
                println!(
                    "{}\t{}\t{}\t{}\t{}",
                    event.created_at,
                    event.principal_id,
                    event.action,
                    event.resource_id,
                    event.audit_event_id
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
