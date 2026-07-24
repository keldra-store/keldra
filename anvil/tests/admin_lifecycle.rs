#![recursion_limit = "256"]

use anvil::anvil_api::admin_service_client::AdminServiceClient;
use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::*;
use anvil_test_utils::{personaldb_test_protocol_keyring, wait_for_port};
use std::time::Duration;
use tempfile::TempDir;
use tokio::task::JoinHandle;
use tonic::Code;

struct AdminNode {
    public_url: String,
    admin_url: String,
    state: anvil::AppState,
    handle: JoinHandle<()>,
    _temp: TempDir,
}

impl Drop for AdminNode {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn spawn_admin_node() -> AdminNode {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("node-a");
    let public_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let admin_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public_addr = public_listener.local_addr().unwrap();
    let admin_addr = admin_listener.local_addr().unwrap();

    let config = anvil::config::Config {
        jwt_secret: "test-secret".to_string(),
        anvil_secret_encryption_key:
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        public_api_addr: format!("http://{public_addr}"),
        api_listen_addr: public_addr.to_string(),
        admin_listen_addr: admin_addr.to_string(),
        mesh_id: "mesh-test".to_string(),
        bootstrap_system_admin_subject_kind: "app".to_string(),
        bootstrap_system_admin_subject_id: "admin-principal".to_string(),
        region: "eu-west-1".to_string(),
        cell_id: "cell-a".to_string(),
        public_region_base_domain: "eu-west-1.anvil-storage.test".to_string(),
        storage_path: storage_path.to_string_lossy().into_owned(),
        personaldb_snapshot_entry_threshold: 1024,
        personaldb_snapshot_payload_bytes_threshold: 64 * 1024 * 1024,
        // These tests exercise synchronous administrative lifecycle APIs.
        // Background task execution is covered separately and would leave
        // detached workers competing across the nodes created by this binary.
        run_background_worker: false,
        ..anvil::config::Config::default()
    };

    let state = anvil::AppState::new(config, personaldb_test_protocol_keyring())
        .await
        .unwrap();
    let state_for_handle = state.clone();
    let handle = tokio::spawn(async move {
        anvil::start_node_with_admin_listener(
            public_listener,
            Some(admin_listener),
            state_for_handle,
        )
        .await
        .unwrap();
    });

    assert!(wait_for_port(public_addr, Duration::from_secs(5)).await);
    assert!(wait_for_port(admin_addr, Duration::from_secs(5)).await);

    AdminNode {
        public_url: format!("http://{public_addr}"),
        admin_url: format!("http://{admin_addr}"),
        state,
        handle,
        _temp: temp,
    }
}

fn admin_token(node: &AdminNode) -> String {
    node.state
        .jwt_manager
        .mint_token("admin-principal".to_string(), 0)
        .unwrap()
}

fn non_admin_token(node: &AdminNode) -> String {
    node.state
        .jwt_manager
        .mint_token("object-principal".to_string(), 0)
        .unwrap()
}

fn with_auth<T>(mut request: tonic::Request<T>, token: &str) -> tonic::Request<T> {
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

fn context(label: &str, expected_generation: u64) -> AdminRequestContext {
    AdminRequestContext {
        request_id: format!("req-{label}"),
        idempotency_key: format!("idem-{label}"),
        audit_reason: format!("test {label}"),
        expected_generation,
    }
}

fn test_receipt_signing_public_key() -> Vec<u8> {
    anvil::node_signing::NodeSigningKeypair::generate()
        .unwrap()
        .public_key_bytes()
        .to_vec()
}

async fn activation_checkpoint_json_from_existing_streams(
    node: &AdminNode,
    region: &str,
) -> String {
    let mut required_streams = Vec::new();
    let stream_families = anvil::mesh_directory::RoutingRecordFamily::all()
        .into_iter()
        .map(|family| family.stream_family())
        .chain(anvil::mesh_lifecycle::lifecycle_control_stream_families().into_iter());
    for stream_family in stream_families {
        let partitions = anvil::mesh_control_stream::list_control_stream_partitions_page(
            &node.state.storage,
            stream_family,
            None,
            1_024,
        )
        .await
        .unwrap();
        assert!(partitions.next_stream_id.is_none());
        for partition in partitions.partitions {
            let cursor = anvil::mesh_control_stream::control_stream_append_cursor(
                &node.state.storage,
                stream_family,
                &partition,
            )
            .await
            .unwrap();
            let log = anvil::mesh_control_stream::read_control_stream_page(
                &node.state.storage,
                stream_family,
                &partition,
                cursor.sequence.get().saturating_sub(2),
                1,
            )
            .await
            .unwrap();
            let record = log.records.last().unwrap();
            anvil::mesh_control_stream::write_control_checkpoint(
                &node.state.storage,
                &anvil::mesh_control_stream::ControlCheckpointRecord::new(
                    "mesh-test",
                    region,
                    stream_family,
                    &partition,
                    record.metadata.sequence,
                    record.metadata.record_digest.clone(),
                    "2026-07-02T00:00:00Z",
                ),
            )
            .await
            .unwrap();
            required_streams.push(serde_json::json!({
                "stream_family": stream_family,
                "partition": partition,
                "sequence": record.metadata.sequence.get(),
                "digest": record.metadata.record_digest.as_str(),
            }));
        }
    }
    serde_json::json!({
        "schema": anvil::mesh_lifecycle::ACTIVATION_CHECKPOINT_SCHEMA,
        "mesh_id": "mesh-test",
        "region": region,
        "created_at": "2026-07-02T00:00:00Z",
        "required_streams": required_streams,
    })
    .to_string()
}

fn missing_activation_checkpoint_json(mesh_id: &str, region: &str) -> String {
    serde_json::json!({
        "schema": anvil::mesh_lifecycle::ACTIVATION_CHECKPOINT_SCHEMA,
        "mesh_id": mesh_id,
        "region": region,
        "created_at": "2026-07-02T00:00:00Z",
        "required_streams": [
            {
                "stream_family": "bucket_locator",
                "partition": "0a7f",
                "sequence": 1,
                "digest": "blake3:0000000000000000000000000000000000000000000000000000000000000000"
            }
        ]
    })
    .to_string()
}

async fn prepare_active_region_dependencies(
    client: &mut AdminServiceClient<tonic::transport::Channel>,
    token: &str,
    label: &str,
    region: &str,
    cell_id: &str,
    node_id: &str,
) {
    let cell = client
        .register_cell(with_auth(
            tonic::Request::new(RegisterCellRequest {
                context: Some(context(&format!("{label}-register-cell"), 0)),
                region: region.to_string(),
                cell_id: cell_id.to_string(),
                placement_weight: 100,
                failure_domain: "rack-a".to_string(),
            }),
            token,
        ))
        .await
        .unwrap()
        .into_inner()
        .cell
        .unwrap();
    let cell = client
        .activate_cell(with_auth(
            tonic::Request::new(ActivateCellRequest {
                context: Some(context(&format!("{label}-activate-cell"), cell.generation)),
                region: region.to_string(),
                cell_id: cell_id.to_string(),
            }),
            token,
        ))
        .await
        .unwrap()
        .into_inner()
        .cell
        .unwrap();
    assert_eq!(cell.state, 2);

    let node = client
        .register_node(with_auth(
            tonic::Request::new(RegisterNodeRequest {
                context: Some(context(&format!("{label}-register-node"), 0)),
                node_id: node_id.to_string(),
                region: region.to_string(),
                cell_id: cell_id.to_string(),
                receipt_signing_public_key: test_receipt_signing_public_key(),
                public_api_addr: "http://127.0.0.1:50051".to_string(),
                capabilities: vec![1, 6],
                capacity_json: "{}".to_string(),
            }),
            token,
        ))
        .await
        .unwrap()
        .into_inner()
        .node
        .unwrap();
    let node = client
        .activate_node(with_auth(
            tonic::Request::new(ActivateNodeRequest {
                context: Some(context(&format!("{label}-activate-node"), node.generation)),
                node_id: node_id.to_string(),
            }),
            token,
        ))
        .await
        .unwrap()
        .into_inner()
        .node
        .unwrap();
    assert_eq!(node.state, 2);
}

#[path = "admin_lifecycle/admin_auth.rs"]
mod admin_auth;
#[path = "admin_lifecycle/aliases_audit.rs"]
mod aliases_audit;
#[path = "admin_lifecycle/personaldb_signing_keys.rs"]
mod personaldb_signing_keys;
#[path = "admin_lifecycle/region_lifecycle.rs"]
mod region_lifecycle;
#[path = "admin_lifecycle/tenant_routing.rs"]
mod tenant_routing;
