use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::auth_service_client::AuthServiceClient;
use clap::Subcommand;
use tokio::time::{Duration, timeout};
use tonic::transport::Endpoint;

#[derive(Subcommand)]
pub enum AuthCommands {
    /// Get a new access token
    GetToken {
        #[clap(long)]
        client_id: Option<String>,
        #[clap(long)]
        client_secret: Option<String>,
    },
    /// Grant a permission to another app
    Grant {
        app: String,
        action: String,
        resource: String,
    },
    /// Revoke a permission from an app
    Revoke {
        app: String,
        action: String,
        resource: String,
    },
    /// List grants for an app in the authenticated tenant
    ListGrants {
        app: String,
        #[clap(long, default_value_t = 100)]
        page_size: u32,
        #[clap(long, default_value = "")]
        page_token: String,
    },
}

pub async fn handle_auth_command(command: &AuthCommands, ctx: &Context) -> anyhow::Result<()> {
    let endpoint = Endpoint::from_shared(ctx.profile.host.clone())?
        .connect_timeout(Duration::from_secs(5))
        .tcp_nodelay(true);
    let channel = endpoint.connect().await?;
    let mut client = AuthServiceClient::new(channel);

    match command {
        AuthCommands::GetToken {
            client_id,
            client_secret,
        } => {
            let (id, secret) = match (client_id.as_ref(), client_secret.as_ref()) {
                (Some(id), Some(secret)) => (id.clone(), secret.clone()),
                _ => (
                    ctx.profile.client_id.clone(),
                    ctx.profile.client_secret.clone(),
                ),
            };

            // Build channel on current runtime and perform unary call with a timeout
            let endpoint = Endpoint::from_shared(ctx.profile.host.clone())?
                .connect_timeout(Duration::from_secs(5))
                .tcp_nodelay(true);
            let channel = endpoint.connect().await?;
            let mut c = AuthServiceClient::new(channel);
            let resp = timeout(
                Duration::from_secs(5),
                c.get_access_token(api::GetAccessTokenRequest {
                    client_id: id,
                    client_secret: secret,
                }),
            )
            .await
            .map_err(|_| anyhow::anyhow!("get-token request timed out"))??;
            let token = resp.into_inner().access_token;
            // Explicitly drop client before printing/exiting to tear down h2 cleanly
            drop(c);
            println!("{}", token);
        }
        AuthCommands::Grant {
            app,
            action,
            resource,
        } => {
            let token = ctx.get_bearer_token().await?;
            let mut request = tonic::Request::new(api::GrantAccessRequest {
                grantee_app_id: app.clone(),
                action: normalise_delegated_action(action, resource)?,
                resource: resource.clone(),
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            client.grant_access(request).await?;
            println!("Permission granted.");
        }
        AuthCommands::Revoke {
            app,
            action,
            resource,
        } => {
            let token = ctx.get_bearer_token().await?;
            let mut request = tonic::Request::new(api::RevokeAccessRequest {
                grantee_app_id: app.clone(),
                action: normalise_delegated_action(action, resource)?,
                resource: resource.clone(),
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            client.revoke_access(request).await?;
            println!("Permission revoked.");
        }
        AuthCommands::ListGrants {
            app,
            page_size,
            page_token,
        } => {
            let token = ctx.get_bearer_token().await?;
            let mut request = tonic::Request::new(api::ListAccessGrantsRequest {
                app: app.clone(),
                page: Some(api::PageRequest {
                    page_size: *page_size,
                    page_token: page_token.clone(),
                }),
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            let response = client.list_access_grants(request).await?.into_inner();
            for grant in response.grants {
                println!("{}\t{}\t{}", grant.app_name, grant.action, grant.resource);
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

fn normalise_delegated_action(action: &str, resource: &str) -> anyhow::Result<String> {
    let action = action.trim();
    if action.contains(':') {
        return Ok(action.to_string());
    }

    let family = if resource.starts_with("bucket:") {
        "bucket"
    } else if resource.starts_with("object:") {
        "object"
    } else if resource.starts_with("index:") {
        "index"
    } else if resource.starts_with("personaldb:") {
        "personaldb"
    } else if resource.starts_with("authz:") {
        "authz"
    } else if resource.starts_with("app:") || resource.starts_with("tenant:") {
        "app"
    } else {
        anyhow::bail!(
            "delegated action must be fully qualified, for example object:read or bucket:read"
        );
    };

    Ok(format!("{family}:{action}"))
}

#[cfg(test)]
mod tests {
    use super::normalise_delegated_action;

    #[test]
    fn normalise_delegated_action_keeps_qualified_actions() {
        assert_eq!(
            normalise_delegated_action("object:read", "bucket:docs").unwrap(),
            "object:read"
        );
    }

    #[test]
    fn normalise_delegated_action_qualifies_resource_family() {
        assert_eq!(
            normalise_delegated_action("read", "bucket:docs").unwrap(),
            "bucket:read"
        );
        assert_eq!(
            normalise_delegated_action("read", "object:docs/report.pdf").unwrap(),
            "object:read"
        );
    }
}
