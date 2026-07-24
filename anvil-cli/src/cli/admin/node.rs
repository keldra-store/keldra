use super::common::{AdminClient, MutationOptions, PageOptions, print_rpc_response, with_auth};
use anvil::anvil_api as api;
use base64::Engine;
use clap::{Subcommand, ValueEnum};

#[derive(Subcommand)]
pub enum NodeCommands {
    /// Read this server's registration descriptor from its private admin API
    DescribeLocal,
    /// Register a node descriptor
    Register {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        node_id: String,
        #[clap(long)]
        region: String,
        #[clap(long)]
        cell_id: String,
        #[clap(long)]
        receipt_signing_public_key_b64: String,
        #[clap(long)]
        public_api_addr: String,
        #[clap(long = "capability", value_delimiter = ',', required = true)]
        capabilities: Vec<NodeCapabilityArg>,
        #[clap(long, default_value = "{}")]
        capacity_json: String,
    },
    /// Activate a joining or drained node
    Activate {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        node_id: String,
    },
    /// Drain an active node
    Drain {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        node_id: String,
        #[clap(long)]
        graceful_timeout_ms: u64,
        #[clap(long, action = clap::ArgAction::SetTrue)]
        force_after_timeout: bool,
    },
    /// Force an active or draining node offline
    ForceOffline {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        node_id: String,
    },
    /// Remove a drained node
    Remove {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        node_id: String,
    },
    /// List node descriptors
    List {
        #[clap(long)]
        region: Option<String>,
        #[clap(long)]
        cell_id: Option<String>,
        #[clap(flatten)]
        page: PageOptions,
    },
}
#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum NodeCapabilityArg {
    Object,
    Index,
    #[value(alias = "personal-db", alias = "personal_db")]
    Personaldb,
    Metadata,
    Gateway,
    Admin,
}
impl NodeCapabilityArg {
    pub(super) fn to_proto(self) -> i32 {
        match self {
            Self::Object => 1,
            Self::Index => 2,
            Self::Personaldb => 3,
            Self::Metadata => 4,
            Self::Gateway => 5,
            Self::Admin => 6,
        }
    }
}

pub(super) async fn handle_node_command(
    command: &NodeCommands,
    client: &mut AdminClient,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        NodeCommands::DescribeLocal => {
            print_rpc_response(
                "node",
                None,
                None,
                client.get_local_node_descriptor(with_auth(
                    api::GetLocalNodeDescriptorRequest {},
                    token,
                )?),
            )
            .await?;
        }
        NodeCommands::Register {
            context,
            node_id,
            region,
            cell_id,
            receipt_signing_public_key_b64,
            public_api_addr,
            capabilities,
            capacity_json,
        } => {
            let admin_context = context.to_create_context()?;
            let receipt_signing_public_key = base64::engine::general_purpose::STANDARD
                .decode(receipt_signing_public_key_b64)
                .or_else(|_| {
                    base64::engine::general_purpose::URL_SAFE_NO_PAD
                        .decode(receipt_signing_public_key_b64)
                })?;
            print_rpc_response(
                "node",
                Some(&admin_context),
                None,
                client.register_node(with_auth(
                    api::RegisterNodeRequest {
                        context: Some(admin_context.clone()),
                        node_id: node_id.clone(),
                        region: region.clone(),
                        cell_id: cell_id.clone(),
                        public_api_addr: public_api_addr.clone(),
                        capabilities: capabilities
                            .iter()
                            .map(|capability| capability.to_proto())
                            .collect(),
                        receipt_signing_public_key,
                        capacity_json: capacity_json.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        NodeCommands::Activate { context, node_id } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "node",
                Some(&admin_context),
                None,
                client.activate_node(with_auth(
                    api::ActivateNodeRequest {
                        context: Some(admin_context.clone()),
                        node_id: node_id.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        NodeCommands::Drain {
            context,
            node_id,
            graceful_timeout_ms,
            force_after_timeout,
        } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "node",
                Some(&admin_context),
                None,
                client.drain_node(with_auth(
                    api::DrainNodeRequest {
                        context: Some(admin_context.clone()),
                        node_id: node_id.clone(),
                        graceful_timeout_ms: *graceful_timeout_ms,
                        force_after_timeout: *force_after_timeout,
                    },
                    token,
                )?),
            )
            .await?;
        }
        NodeCommands::ForceOffline { context, node_id } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "node",
                Some(&admin_context),
                None,
                client.force_offline_node(with_auth(
                    api::ForceOfflineNodeRequest {
                        context: Some(admin_context.clone()),
                        node_id: node_id.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        NodeCommands::Remove { context, node_id } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "node",
                Some(&admin_context),
                None,
                client.remove_node(with_auth(
                    api::RemoveNodeRequest {
                        context: Some(admin_context.clone()),
                        node_id: node_id.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        NodeCommands::List {
            region,
            cell_id,
            page,
        } => {
            print_rpc_response(
                "nodes",
                None,
                None,
                client.list_nodes(with_auth(
                    api::ListNodesRequest {
                        region: region.clone().unwrap_or_default(),
                        cell_id: cell_id.clone().unwrap_or_default(),
                        page: page.to_page_request(),
                    },
                    token,
                )?),
            )
            .await?;
        }
    }

    Ok(())
}
