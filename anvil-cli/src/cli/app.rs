use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::auth_service_client::AuthServiceClient;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum AppCommands {
    /// Create an application credential in the authenticated tenant.
    Create { app_name: String },
    /// Rotate an application secret in the authenticated tenant.
    RotateSecret { app_name: String },
    /// Delete an application credential in the authenticated tenant.
    Delete { app_name: String },
    /// List application credentials in the authenticated tenant.
    List {
        #[clap(long, default_value_t = 100)]
        page_size: u32,
        #[clap(long, default_value = "")]
        page_token: String,
    },
}

pub async fn handle_app_command(command: &AppCommands, ctx: &Context) -> anyhow::Result<()> {
    let mut client = AuthServiceClient::connect(ctx.profile.host.clone()).await?;
    let token = ctx.get_bearer_token().await?;

    match command {
        AppCommands::Create { app_name } => {
            let mut request = tonic::Request::new(api::CreateApplicationCredentialRequest {
                app_name: app_name.clone(),
                request_id: format!("app-create-{}", uuid::Uuid::new_v4()),
                idempotency_key: uuid::Uuid::new_v4().to_string(),
            });
            add_auth(&mut request, &token);
            let response = client
                .create_application_credential(request)
                .await?
                .into_inner();
            println!(
                "app_id={}\napp_name={}\nclient_id={}\nclient_secret={}",
                response.app_id, response.app_name, response.client_id, response.client_secret
            );
        }
        AppCommands::RotateSecret { app_name } => {
            let mut request = tonic::Request::new(api::RotateApplicationCredentialSecretRequest {
                app_name: app_name.clone(),
                request_id: format!("app-rotate-{}", uuid::Uuid::new_v4()),
                idempotency_key: uuid::Uuid::new_v4().to_string(),
            });
            add_auth(&mut request, &token);
            let response = client
                .rotate_application_credential_secret(request)
                .await?
                .into_inner();
            println!(
                "app_id={}\napp_name={}\nclient_id={}\nclient_secret={}",
                response.app_id, response.app_name, response.client_id, response.client_secret
            );
        }
        AppCommands::Delete { app_name } => {
            let mut request = tonic::Request::new(api::DeleteApplicationCredentialRequest {
                app_name: app_name.clone(),
                request_id: format!("app-delete-{}", uuid::Uuid::new_v4()),
                idempotency_key: uuid::Uuid::new_v4().to_string(),
            });
            add_auth(&mut request, &token);
            let response = client
                .delete_application_credential(request)
                .await?
                .into_inner();
            println!("deleted app_id={}", response.app_id);
        }
        AppCommands::List {
            page_size,
            page_token,
        } => {
            let mut request = tonic::Request::new(api::ListApplicationsRequest {
                page: Some(api::PageRequest {
                    page_size: *page_size,
                    page_token: page_token.clone(),
                }),
            });
            add_auth(&mut request, &token);
            let response = client.list_applications(request).await?.into_inner();
            for app in response.applications {
                println!("{}\t{}\t{}", app.app_id, app.app_name, app.client_id);
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
