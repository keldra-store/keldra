use super::common::{print_rpc_response, request_id_or_cli, with_auth};
use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::admin_service_client::AdminServiceClient;
use anvil::anvil_api::mesh_control_service_client::MeshControlServiceClient;
use anyhow::Result;
use base64::Engine;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum MeshCommands {
    /// Install this fresh node as an authoritative active single-node topology.
    BootstrapLocal {
        /// Failure-domain label for the local cell.
        #[clap(long)]
        failure_domain: Option<String>,
    },
    /// Upsert a mesh region through the admin control plane.
    PutRegion {
        #[clap(long)]
        region_id: String,
        #[clap(long)]
        endpoint: String,
        #[clap(long, default_value = "joining")]
        state: String,
    },
    /// Upsert a cell inside a mesh region.
    PutCell {
        #[clap(long)]
        region_id: String,
        #[clap(long)]
        cell_id: String,
        #[clap(long)]
        failure_domain: String,
        #[clap(long, default_value = "joining")]
        state: String,
    },
    /// Upsert a node descriptor inside a cell.
    PutNode {
        #[clap(long)]
        node_id: String,
        #[clap(long)]
        region_id: String,
        #[clap(long)]
        cell_id: String,
        #[clap(long)]
        advertise_addr: String,
        #[clap(long)]
        receipt_signing_public_key_b64: String,
        #[clap(long = "capability")]
        capabilities: Vec<String>,
        #[clap(long, default_value = "joining")]
        state: String,
        #[clap(long, default_value = "{}")]
        capacity_json: String,
    },
    /// Start draining a node.
    DrainNode {
        #[clap(long)]
        node_id: String,
        #[clap(long, default_value_t = 300_000)]
        graceful_timeout_ms: u64,
        #[clap(long, action = clap::ArgAction::SetTrue)]
        force_after_timeout: bool,
    },
    /// Start draining a cell.
    DrainCell {
        #[clap(long)]
        region: String,
        #[clap(long)]
        cell_id: String,
    },
    /// Move a bucket's home region in the mesh routing directory.
    MoveBucket {
        #[clap(long)]
        tenant_id: String,
        #[clap(long)]
        bucket_name: String,
        #[clap(long)]
        target_region_id: String,
    },
    /// Print the current partition map rows known to the control plane.
    PartitionMap {
        #[clap(long)]
        scope: Option<String>,
        #[clap(long)]
        request_id: Option<String>,
    },
}

pub(super) async fn handle_mesh_command(
    command: &MeshCommands,
    ctx: &Context,
    token: &str,
) -> Result<()> {
    let mut client = MeshControlServiceClient::connect(ctx.profile.host.clone()).await?;
    match command {
        MeshCommands::BootstrapLocal { failure_domain } => {
            let mut admin_client = AdminServiceClient::connect(ctx.profile.host.clone()).await?;
            let descriptor = admin_client
                .get_local_node_descriptor(with_auth(api::GetLocalNodeDescriptorRequest {}, token)?)
                .await?
                .into_inner()
                .node
                .ok_or_else(|| anyhow::anyhow!("local node descriptor response was empty"))?;
            let failure_domain = failure_domain
                .clone()
                .unwrap_or_else(|| descriptor.cell_id.clone());
            print_rpc_response(
                "mesh_bootstrap",
                None,
                None,
                client.bootstrap_mesh_topology(with_auth(
                    api::BootstrapMeshTopologyRequest {
                        regions: vec![api::PutRegionRequest {
                            region_id: descriptor.region.clone(),
                            endpoint: descriptor.public_api_addr.clone(),
                            state: "active".to_string(),
                            options: None,
                        }],
                        cells: vec![api::PutCellRequest {
                            region_id: descriptor.region.clone(),
                            cell_id: descriptor.cell_id.clone(),
                            failure_domain,
                            state: "active".to_string(),
                            options: None,
                        }],
                        nodes: vec![api::PutNodeRequest {
                            node_id: descriptor.node_id,
                            region_id: descriptor.region,
                            cell_id: descriptor.cell_id,
                            advertise_addr: descriptor.public_api_addr,
                            state: "active".to_string(),
                            capacity_json: "{}".to_string(),
                            options: None,
                            receipt_signing_public_key: descriptor.receipt_signing_public_key,
                            capabilities: descriptor
                                .capabilities
                                .into_iter()
                                .filter_map(|value| api::NodeCapability::try_from(value).ok())
                                .filter_map(capability_name)
                                .map(str::to_string)
                                .collect(),
                        }],
                        canonical_coremeta_rows: Vec::new(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        MeshCommands::PutRegion {
            region_id,
            endpoint,
            state,
        } => {
            print_rpc_response(
                "mesh_region",
                None,
                None,
                client.put_region(with_auth(
                    api::PutRegionRequest {
                        region_id: region_id.clone(),
                        endpoint: endpoint.clone(),
                        state: state.clone(),
                        options: Some(default_write_options()),
                    },
                    token,
                )?),
            )
            .await?;
        }
        MeshCommands::PutCell {
            region_id,
            cell_id,
            failure_domain,
            state,
        } => {
            print_rpc_response(
                "mesh_cell",
                None,
                None,
                client.put_cell(with_auth(
                    api::PutCellRequest {
                        region_id: region_id.clone(),
                        cell_id: cell_id.clone(),
                        failure_domain: failure_domain.clone(),
                        state: state.clone(),
                        options: Some(default_write_options()),
                    },
                    token,
                )?),
            )
            .await?;
        }
        MeshCommands::PutNode {
            node_id,
            region_id,
            cell_id,
            advertise_addr,
            receipt_signing_public_key_b64,
            capabilities,
            state,
            capacity_json,
        } => {
            let receipt_signing_public_key = base64::engine::general_purpose::STANDARD
                .decode(receipt_signing_public_key_b64)
                .or_else(|_| {
                    base64::engine::general_purpose::URL_SAFE_NO_PAD
                        .decode(receipt_signing_public_key_b64)
                })?;
            print_rpc_response(
                "mesh_node",
                None,
                None,
                client.put_node(with_auth(
                    api::PutNodeRequest {
                        node_id: node_id.clone(),
                        region_id: region_id.clone(),
                        cell_id: cell_id.clone(),
                        advertise_addr: advertise_addr.clone(),
                        state: state.clone(),
                        capacity_json: capacity_json.clone(),
                        options: Some(default_write_options()),
                        receipt_signing_public_key,
                        capabilities: capabilities.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        MeshCommands::DrainNode {
            node_id,
            graceful_timeout_ms,
            force_after_timeout,
        } => {
            print_rpc_response(
                "mesh_node",
                None,
                None,
                client.drain_node(with_auth(
                    api::DrainNodeRequest {
                        context: None,
                        node_id: node_id.clone(),
                        graceful_timeout_ms: *graceful_timeout_ms,
                        force_after_timeout: *force_after_timeout,
                    },
                    token,
                )?),
            )
            .await?;
        }
        MeshCommands::DrainCell { region, cell_id } => {
            print_rpc_response(
                "mesh_cell",
                None,
                None,
                client.drain_cell(with_auth(
                    api::DrainCellRequest {
                        context: None,
                        region: region.clone(),
                        cell_id: cell_id.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        MeshCommands::MoveBucket {
            tenant_id,
            bucket_name,
            target_region_id,
        } => {
            print_rpc_response(
                "mesh_bucket",
                None,
                None,
                client.move_bucket(with_auth(
                    api::MoveBucketRequest {
                        bucket_name: bucket_name.clone(),
                        target_region_id: target_region_id.clone(),
                        options: Some(default_write_options()),
                        tenant_id: tenant_id.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        MeshCommands::PartitionMap { scope, request_id } => {
            let request_id = request_id_or_cli(request_id);
            print_rpc_response(
                "mesh_partition_map",
                None,
                Some(&request_id),
                client.get_partition_map(with_auth(
                    api::GetPartitionMapRequest {
                        scope: scope.clone().unwrap_or_default(),
                        page: None,
                    },
                    token,
                )?),
            )
            .await?;
        }
    }
    Ok(())
}

fn default_write_options() -> api::WriteOptions {
    api::WriteOptions {
        idempotency_key: uuid::Uuid::new_v4().to_string(),
        consistency: api::ConsistencyMode::Finalised as i32,
        wait_for_finalization: true,
        preconditions: Vec::new(),
        boundary_values: Vec::new(),
        execution: None,
    }
}

fn capability_name(capability: api::NodeCapability) -> Option<&'static str> {
    match capability {
        api::NodeCapability::Object => Some("object"),
        api::NodeCapability::Index => Some("index"),
        api::NodeCapability::Personaldb => Some("personaldb"),
        api::NodeCapability::Metadata => Some("metadata"),
        api::NodeCapability::Gateway => Some("gateway"),
        api::NodeCapability::Admin => Some("admin"),
        api::NodeCapability::Unspecified => None,
    }
}
