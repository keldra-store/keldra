use super::common::{AdminClient, print_rpc_response, request_id_or_cli, with_auth};
use anvil::anvil_api as api;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum StorageClassCommands {
    /// List storage classes exposed by the operator
    List {
        #[clap(long)]
        request_id: Option<String>,
        #[clap(long, action = clap::ArgAction::SetTrue)]
        include_operator_only: bool,
        #[clap(long, default_value_t = 100)]
        page_size: u32,
        #[clap(long, default_value = "")]
        page_token: String,
    },
    /// Show one storage class profile
    Get {
        #[clap(long)]
        request_id: Option<String>,
        #[clap(long)]
        class_id: String,
    },
}

pub(super) async fn handle_storage_class_command(
    command: &StorageClassCommands,
    client: &mut AdminClient,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        StorageClassCommands::List {
            request_id,
            include_operator_only,
            page_size,
            page_token,
        } => {
            let request_id = request_id_or_cli(request_id);
            print_rpc_response(
                "storage_classes",
                None,
                Some(&request_id),
                client.list_storage_classes(with_auth(
                    api::ListStorageClassesRequest {
                        request_id: request_id.clone(),
                        include_operator_only: *include_operator_only,
                        page: Some(api::PageRequest {
                            page_size: *page_size,
                            page_token: page_token.clone(),
                        }),
                    },
                    token,
                )?),
            )
            .await?;
        }
        StorageClassCommands::Get {
            request_id,
            class_id,
        } => {
            let request_id = request_id_or_cli(request_id);
            print_rpc_response(
                "storage_class",
                None,
                Some(&request_id),
                client.get_storage_class(with_auth(
                    api::GetStorageClassRequest {
                        request_id: request_id.clone(),
                        class_id: class_id.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
    }
    Ok(())
}
