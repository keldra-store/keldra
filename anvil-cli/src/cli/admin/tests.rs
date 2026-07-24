use super::bucket::BucketPublicAccessCommands;
use super::common::{MutationOptions, with_auth};
use super::node::NodeCapabilityArg;
use super::personaldb_signing_key::{
    PersonalDbSigningImportStatusArg, PersonalDbSigningPurposeArg,
    PersonalDbSigningTerminalStatusArg, derive_public_key, read_private_key_file,
};
use super::region::BucketDrainOverrideArg;
use super::repair::RepairKindArg;
use super::*;
use anvil::anvil_api as api;
use anvil::anvil_api::admin_service_client::AdminServiceClient;
use anvil_test_utils::personaldb_test_protocol_keyring;
use base64::Engine;
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

#[derive(Parser)]
struct TestAdminCli {
    #[clap(subcommand)]
    command: AdminCommands,
}

struct AdminCliNode {
    admin_url: String,
    state: anvil::AppState,
    handle: JoinHandle<()>,
    _temp: TempDir,
}

impl Drop for AdminCliNode {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn spawn_admin_cli_node() -> AdminCliNode {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("cli-node");
    let public_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let admin_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public_addr = public_listener.local_addr().unwrap();
    let admin_addr = admin_listener.local_addr().unwrap();

    let config = anvil::config::Config {
        jwt_secret: "cli-test-secret".to_string(),
        anvil_secret_encryption_key:
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        public_api_addr: format!("http://{public_addr}"),
        api_listen_addr: public_addr.to_string(),
        admin_listen_addr: admin_addr.to_string(),
        mesh_id: "mesh-cli-test".to_string(),
        bootstrap_system_admin_subject_kind: "app".to_string(),
        bootstrap_system_admin_subject_id: "cli-admin-principal".to_string(),
        region: "eu-west-1".to_string(),
        cell_id: "cell-a".to_string(),
        public_region_base_domain: "eu-west-1.anvil-storage.test".to_string(),
        storage_path: storage_path.to_string_lossy().into_owned(),
        personaldb_snapshot_entry_threshold: 1024,
        personaldb_snapshot_payload_bytes_threshold: 64 * 1024 * 1024,
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

    wait_for_tcp_port(admin_addr, Duration::from_secs(5)).await;

    AdminCliNode {
        admin_url: format!("http://{admin_addr}"),
        state,
        handle,
        _temp: temp,
    }
}

async fn wait_for_tcp_port(addr: SocketAddr, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for admin listener on {addr}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn mutation_options(label: &str, expected_generation: u64) -> MutationOptions {
    MutationOptions {
        request_id: Some(format!("req-{label}")),
        idempotency_key: Some(format!("idem-{label}")),
        audit_reason: format!("test {label}"),
        expected_generation: Some(expected_generation),
    }
}

fn admin_token(node: &AdminCliNode) -> String {
    node.state
        .jwt_manager
        .mint_token("cli-admin-principal".to_string(), 0)
        .unwrap()
}

async fn write_activation_checkpoint_from_existing_streams(
    node: &AdminCliNode,
    file_name: &str,
) -> PathBuf {
    let path = node._temp.path().join(file_name);
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
                    "mesh-cli-test",
                    "eu-west-1",
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
    std::fs::write(
        &path,
        serde_json::json!({
            "schema": anvil::mesh_lifecycle::ACTIVATION_CHECKPOINT_SCHEMA,
            "mesh_id": "mesh-cli-test",
            "region": "eu-west-1",
            "created_at": "2026-07-02T00:00:00Z",
            "required_streams": required_streams
        })
        .to_string(),
    )
    .unwrap();
    path
}

#[test]
fn mutation_options_generate_optional_ids() {
    let context = MutationOptions {
        request_id: None,
        idempotency_key: None,
        audit_reason: "planned maintenance".to_string(),
        expected_generation: Some(42),
    }
    .to_action_context();

    assert!(context.request_id.starts_with("cli-"));
    assert!(!context.idempotency_key.is_empty());
    assert_eq!(context.audit_reason, "planned maintenance");
    assert_eq!(context.expected_generation, 42);
}

#[test]
fn mutation_context_helpers_enforce_generation_contract() {
    let create = MutationOptions {
        request_id: Some("req-create".to_string()),
        idempotency_key: Some("idem-create".to_string()),
        audit_reason: "create resource".to_string(),
        expected_generation: None,
    };
    assert_eq!(create.to_create_context().unwrap().expected_generation, 0);
    assert!(create.to_update_context().is_err());

    let update = MutationOptions {
        request_id: Some("req-update".to_string()),
        idempotency_key: Some("idem-update".to_string()),
        audit_reason: "update resource".to_string(),
        expected_generation: Some(7),
    };
    assert_eq!(update.to_update_context().unwrap().expected_generation, 7);
    assert!(update.to_create_context().is_err());
}

#[test]
fn mutation_parse_requires_explicit_audit_reason() {
    let result = TestAdminCli::try_parse_from([
        "admin",
        "region",
        "create",
        "--expected-generation",
        "0",
        "--region",
        "eu-west-1",
        "--public-base-url",
        "https://eu-west-1.example.test",
        "--virtual-host-suffix",
        "eu-west-1.example.test",
    ]);
    let Err(error) = result else {
        panic!("expected audit_reason parse failure");
    };

    assert_eq!(
        error.kind(),
        clap::error::ErrorKind::MissingRequiredArgument
    );
}

#[test]
fn mutation_parse_allows_generated_request_ids() {
    let cli = TestAdminCli::try_parse_from([
        "admin",
        "region",
        "create",
        "--audit-reason",
        "bootstrap region",
        "--expected-generation",
        "0",
        "--region",
        "eu-west-1",
        "--public-base-url",
        "https://eu-west-1.example.test",
        "--virtual-host-suffix",
        "eu-west-1.example.test",
    ])
    .unwrap();

    let AdminCommands::Region {
        command: RegionCommands::Create {
            context, region, ..
        },
    } = cli.command
    else {
        panic!("expected region create command");
    };

    assert!(context.request_id.is_none());
    assert!(context.idempotency_key.is_none());
    assert_eq!(context.audit_reason, "bootstrap region");
    assert_eq!(context.expected_generation, Some(0));
    assert_eq!(region, "eu-west-1");
}

#[test]
fn personaldb_signing_key_commands_parse_protocol_fields() {
    let import = TestAdminCli::try_parse_from([
        "admin",
        "personal-db-signing-key",
        "import",
        "--audit-reason",
        "install witness generation",
        "--expected-generation",
        "0",
        "--private-key-pkcs8",
        "/secure/witness.pk8",
        "--key-generation",
        "3",
        "--purpose",
        "witness",
        "--database-scope",
        "database-a",
        "--database-scope",
        "database-b",
        "--group-scope",
        "group-a",
        "--valid-from-log-index",
        "100",
        "--valid-until-log-index",
        "200",
        "--status",
        "retiring",
    ])
    .unwrap();

    let AdminCommands::PersonalDbSigningKey {
        command:
            PersonalDbSigningKeyCommands::Import {
                context,
                private_key_pkcs8,
                key_generation,
                purpose,
                database_scopes,
                group_scopes,
                valid_from_log_index,
                valid_until_log_index,
                status,
            },
    } = import.command
    else {
        panic!("expected PersonalDB signing key import command");
    };
    assert_eq!(context.expected_generation, Some(0));
    assert_eq!(private_key_pkcs8, PathBuf::from("/secure/witness.pk8"));
    assert_eq!(key_generation, 3);
    assert!(matches!(purpose, PersonalDbSigningPurposeArg::Witness));
    assert_eq!(database_scopes, ["database-a", "database-b"]);
    assert_eq!(group_scopes, ["group-a"]);
    assert_eq!(valid_from_log_index, 100);
    assert_eq!(valid_until_log_index, Some(200));
    assert!(matches!(status, PersonalDbSigningImportStatusArg::Retiring));

    let set_status = TestAdminCli::try_parse_from([
        "admin",
        "personal-db-signing-key",
        "set-status",
        "--audit-reason",
        "retire generation",
        "--expected-generation",
        "7",
        "--key-id",
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "--status",
        "revoked-future",
        "--valid-until-log-index",
        "250",
    ])
    .unwrap();

    let AdminCommands::PersonalDbSigningKey {
        command:
            PersonalDbSigningKeyCommands::SetStatus {
                context,
                key_id,
                status,
                valid_until_log_index,
            },
    } = set_status.command
    else {
        panic!("expected PersonalDB signing key set-status command");
    };
    assert_eq!(context.to_update_context().unwrap().expected_generation, 7);
    assert_eq!(
        key_id,
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
    assert!(matches!(
        status,
        PersonalDbSigningTerminalStatusArg::RevokedFuture
    ));
    assert_eq!(valid_until_log_index, 250);

    let unsupported_purpose = TestAdminCli::try_parse_from([
        "admin",
        "personal-db-signing-key",
        "import",
        "--audit-reason",
        "reject unsupported purpose",
        "--private-key-pkcs8",
        "/secure/source-proposer.pk8",
        "--key-generation",
        "1",
        "--purpose",
        "source-proposer",
    ]);
    let Err(unsupported_purpose) = unsupported_purpose else {
        panic!("expected unsupported PersonalDB signing purpose to be rejected");
    };
    assert_eq!(
        unsupported_purpose.kind(),
        clap::error::ErrorKind::InvalidValue
    );
}

#[test]
fn personaldb_signing_key_derives_expected_raw_public_key() {
    let private_key = base64::engine::general_purpose::STANDARD
        .decode("MC4CAQAwBQYDK2VwBCIEIDMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMz")
        .unwrap();
    let expected_public_key = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode("F8t5-ytBIPKx7GXkGY1uCLKOgT_rAeSkAIObheGAgM4")
        .unwrap();

    let public_key = derive_public_key(
        &private_key,
        4,
        PersonalDbSigningPurposeArg::Witness,
        &["database-a".to_string()],
        &["group-a".to_string()],
        0,
        None,
        PersonalDbSigningImportStatusArg::Active,
    )
    .unwrap();

    assert_eq!(public_key, expected_public_key);
}

#[tokio::test]
async fn personaldb_signing_key_file_is_regular_private_and_bounded() {
    let temp = tempfile::tempdir().unwrap();
    let key_path = temp.path().join("witness.pk8");
    std::fs::write(&key_path, [1_u8, 2, 3]).unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    assert_eq!(read_private_key_file(&key_path).await.unwrap(), [1, 2, 3]);

    let oversized_path = temp.path().join("oversized.pk8");
    std::fs::write(&oversized_path, vec![0_u8; 16 * 1024 + 1]).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&oversized_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    assert!(
        read_private_key_file(&oversized_path)
            .await
            .unwrap_err()
            .to_string()
            .contains("exceeds the 16384 byte limit")
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::{PermissionsExt, symlink};

        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o640)).unwrap();
        assert!(
            read_private_key_file(&key_path)
                .await
                .unwrap_err()
                .to_string()
                .contains("must not grant group or other permissions")
        );

        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let symlink_path = temp.path().join("witness-link.pk8");
        symlink(&key_path, &symlink_path).unwrap();
        assert!(
            read_private_key_file(&symlink_path)
                .await
                .unwrap_err()
                .to_string()
                .contains("must not be a symbolic link")
        );
    }
}

#[test]
fn bucket_override_parses_reason_with_colons() {
    let override_arg: BucketDrainOverrideArg =
        "tenant-a:photos:read-only-until-removed:incident:123"
            .parse()
            .unwrap();

    let proto = override_arg.to_proto();
    assert_eq!(proto.tenant_id, "tenant-a");
    assert_eq!(proto.bucket_name, "photos");
    assert_eq!(proto.disposition, 3);
    assert_eq!(proto.reason, "incident:123");
}

#[test]
fn node_capabilities_map_to_proto_values() {
    assert_eq!(NodeCapabilityArg::Object.to_proto(), 1);
    assert_eq!(NodeCapabilityArg::Index.to_proto(), 2);
    assert_eq!(NodeCapabilityArg::Personaldb.to_proto(), 3);
    assert_eq!(NodeCapabilityArg::Metadata.to_proto(), 4);
    assert_eq!(NodeCapabilityArg::Gateway.to_proto(), 5);
    assert_eq!(NodeCapabilityArg::Admin.to_proto(), 6);
}

#[test]
fn host_alias_activate_requires_expected_generation_and_reason() {
    let cli = TestAdminCli::try_parse_from([
        "admin",
        "host-alias",
        "activate",
        "--audit-reason",
        "dns verified",
        "--expected-generation",
        "7",
        "--hostname",
        "cdn.example.com",
    ])
    .unwrap();

    let AdminCommands::HostAlias {
        command: HostAliasCommands::Activate { context, hostname },
    } = cli.command
    else {
        panic!("expected host-alias activate command");
    };

    assert_eq!(context.audit_reason, "dns verified");
    assert_eq!(context.expected_generation, Some(7));
    assert_eq!(hostname, "cdn.example.com");
}

#[test]
fn missing_lifecycle_commands_parse_with_mutation_context() {
    let region_cli = TestAdminCli::try_parse_from([
        "admin",
        "region",
        "set-read-only",
        "--audit-reason",
        "maintenance window",
        "--expected-generation",
        "11",
        "--region",
        "eu-west-1",
    ])
    .unwrap();
    let AdminCommands::Region {
        command: RegionCommands::SetReadOnly { context, region },
    } = region_cli.command
    else {
        panic!("expected region set-read-only command");
    };
    assert_eq!(context.audit_reason, "maintenance window");
    assert_eq!(context.expected_generation, Some(11));
    assert_eq!(region, "eu-west-1");

    let node_cli = TestAdminCli::try_parse_from([
        "admin",
        "node",
        "force-offline",
        "--audit-reason",
        "lost heartbeat",
        "--expected-generation",
        "12",
        "--node-id",
        "node-a",
    ])
    .unwrap();
    let AdminCommands::Node {
        command: NodeCommands::ForceOffline { context, node_id },
    } = node_cli.command
    else {
        panic!("expected node force-offline command");
    };
    assert_eq!(context.audit_reason, "lost heartbeat");
    assert_eq!(context.expected_generation, Some(12));
    assert_eq!(node_id, "node-a");
}

#[test]
fn routing_commands_parse_family_and_mutation_context() {
    let list_cli = TestAdminCli::try_parse_from([
        "admin",
        "routing",
        "list",
        "--family",
        "bucket-locator",
        "--page-size",
        "25",
    ])
    .unwrap();
    let AdminCommands::Routing {
        command: RoutingCommands::List { family, page },
    } = list_cli.command
    else {
        panic!("expected routing list command");
    };
    assert_eq!(family.unwrap().to_proto(), 3);
    assert_eq!(page.page_size, Some(25));

    let repair_cli = TestAdminCli::try_parse_from([
        "admin",
        "routing",
        "repair",
        "--audit-reason",
        "rebuild missing locator",
        "--expected-generation",
        "1",
        "--family",
        "tenant-name",
        "--record-key",
        "acme",
    ])
    .unwrap();
    let AdminCommands::Routing {
        command:
            RoutingCommands::Repair {
                context,
                family,
                record_key,
            },
    } = repair_cli.command
    else {
        panic!("expected routing repair command");
    };
    assert_eq!(context.audit_reason, "rebuild missing locator");
    assert_eq!(context.expected_generation, Some(1));
    assert_eq!(family.to_proto(), 1);
    assert_eq!(record_key, "acme");
}

#[test]
fn repair_diagnostics_and_audit_commands_parse() {
    let repair_cli = TestAdminCli::try_parse_from([
        "admin",
        "repair",
        "run",
        "--audit-reason",
        "verify directory",
        "--expected-generation",
        "0",
        "--repair-kind",
        "directory-index",
        "--tenant-id",
        "acme",
        "--bucket-name",
        "releases",
        "--rebuild",
    ])
    .unwrap();
    let AdminCommands::Repair {
        command:
            RepairCommands::Run {
                context,
                repair_kind,
                tenant_id,
                bucket_name,
                rebuild,
                ..
            },
    } = repair_cli.command
    else {
        panic!("expected repair run command");
    };
    assert_eq!(context.audit_reason, "verify directory");
    assert_eq!(context.expected_generation, Some(0));
    assert_eq!(repair_kind.to_proto(), 2);
    assert_eq!(tenant_id, "acme");
    assert_eq!(bucket_name.as_deref(), Some("releases"));
    assert!(rebuild);

    let diagnostics_cli = TestAdminCli::try_parse_from([
        "admin",
        "diagnostics",
        "list",
        "--request-id",
        "req-diag",
        "--source",
        "index",
        "--tenant-id",
        "acme",
        "--bucket-name",
        "releases",
        "--severity",
        "warning",
        "--page-size",
        "10",
    ])
    .unwrap();
    let AdminCommands::Diagnostics {
        command:
            DiagnosticsCommands::List {
                request_id,
                source,
                tenant_id,
                bucket_name,
                severity,
                page,
                ..
            },
    } = diagnostics_cli.command
    else {
        panic!("expected diagnostics list command");
    };
    assert_eq!(request_id.as_deref(), Some("req-diag"));
    assert_eq!(source.as_deref(), Some("index"));
    assert_eq!(tenant_id.as_deref(), Some("acme"));
    assert_eq!(bucket_name.as_deref(), Some("releases"));
    assert_eq!(severity.as_deref(), Some("warning"));
    assert_eq!(page.page_size, Some(10));

    let audit_cli = TestAdminCli::try_parse_from([
        "admin",
        "audit",
        "list",
        "--request-id",
        "req-audit",
        "--principal-id",
        "admin-a",
        "--resource-id",
        "bucket/releases",
        "--action",
        "run_repair",
    ])
    .unwrap();
    let AdminCommands::Audit {
        command:
            AuditCommands::List {
                request_id,
                principal_id,
                resource_id,
                action,
                ..
            },
    } = audit_cli.command
    else {
        panic!("expected audit list command");
    };
    assert_eq!(request_id.as_deref(), Some("req-audit"));
    assert_eq!(principal_id.as_deref(), Some("admin-a"));
    assert_eq!(resource_id.as_deref(), Some("bucket/releases"));
    assert_eq!(action.as_deref(), Some("run_repair"));
}

#[test]
fn tenant_app_and_bucket_admin_commands_parse() {
    let tenant_cli = TestAdminCli::try_parse_from([
        "admin",
        "tenant",
        "create",
        "--audit-reason",
        "create tenant",
        "--expected-generation",
        "0",
        "--name",
        "acme",
        "--home-region",
        "eu-west-1",
    ])
    .unwrap();
    let AdminCommands::Tenant {
        command:
            TenantCommands::Create {
                context,
                name,
                home_region,
            },
    } = tenant_cli.command
    else {
        panic!("expected tenant create command");
    };
    assert_eq!(context.audit_reason, "create tenant");
    assert_eq!(name, "acme");
    assert_eq!(home_region, "eu-west-1");

    let app_cli = TestAdminCli::try_parse_from([
        "admin",
        "app",
        "rotate-secret",
        "--audit-reason",
        "rotate app",
        "--expected-generation",
        "1",
        "--tenant-id",
        "acme",
        "--app-name",
        "publisher",
    ])
    .unwrap();
    let AdminCommands::App {
        command:
            AppCommands::RotateSecret {
                context,
                tenant_id,
                app_name,
            },
    } = app_cli.command
    else {
        panic!("expected app rotate-secret command");
    };
    assert_eq!(context.expected_generation, Some(1));
    assert_eq!(tenant_id, "acme");
    assert_eq!(app_name, "publisher");

    let bucket_cli = TestAdminCli::try_parse_from([
        "admin",
        "bucket",
        "public-access",
        "set",
        "--audit-reason",
        "publish bucket",
        "--expected-generation",
        "1",
        "--tenant-id",
        "acme",
        "--bucket-name",
        "releases",
        "--allow",
        "true",
    ])
    .unwrap();
    let AdminCommands::Bucket {
        command:
            BucketCommands::PublicAccess {
                command:
                    BucketPublicAccessCommands::Set {
                        context,
                        tenant_id,
                        bucket_name,
                        allow,
                    },
            },
    } = bucket_cli.command
    else {
        panic!("expected bucket public-access set command");
    };
    assert_eq!(context.audit_reason, "publish bucket");
    assert_eq!(tenant_id, "acme");
    assert_eq!(bucket_name, "releases");
    assert!(allow);
}

#[tokio::test]
async fn admin_repair_diagnostics_and_audit_handlers_return_structured_responses() {
    let node = spawn_admin_cli_node().await;
    let token = admin_token(&node);
    let mut client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();

    client
        .create_tenant(
            with_auth(
                api::CreateTenantRequest {
                    context: Some(
                        mutation_options("admin-diag-tenant", 0)
                            .to_create_context()
                            .unwrap(),
                    ),
                    name: "acme".to_string(),
                    home_region: "eu-west-1".to_string(),
                },
                &token,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    client
        .create_bucket_admin(
            with_auth(
                api::CreateBucketAdminRequest {
                    context: Some(
                        mutation_options("admin-diag-bucket", 0)
                            .to_create_context()
                            .unwrap(),
                    ),
                    tenant_id: "acme".to_string(),
                    bucket_name: "releases".to_string(),
                    region: "eu-west-1".to_string(),
                },
                &token,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let repair = client
        .run_repair(
            with_auth(
                api::RunRepairRequest {
                    context: Some(
                        mutation_options("admin-directory-repair", 0).to_action_context(),
                    ),
                    repair_kind: RepairKindArg::DirectoryIndex.to_proto(),
                    tenant_id: "acme".to_string(),
                    bucket_name: "releases".to_string(),
                    index_name: String::new(),
                    derived_index_id: String::new(),
                    database_id: String::new(),
                    rebuild: false,
                },
                &token,
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .into_inner();
    assert_eq!(repair.request_id, "req-admin-directory-repair");
    assert_eq!(repair.status, "empty_source");
    assert_eq!(repair.scope_kind, "bucket");
    assert!(repair.findings.is_empty());
    assert!(repair.audit_event_id.contains("req-admin-directory-repair"));

    let diagnostics = client
        .list_diagnostics(
            with_auth(
                api::ListDiagnosticsRequest {
                    request_id: "req-admin-diagnostics".to_string(),
                    source: "index".to_string(),
                    tenant_id: "acme".to_string(),
                    bucket_name: "releases".to_string(),
                    index_name: String::new(),
                    severity: String::new(),
                    page: Some(api::PageRequest {
                        page_token: String::new(),
                        page_size: 5,
                    }),
                },
                &token,
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .into_inner();
    assert_eq!(diagnostics.request_id, "req-admin-diagnostics");
    assert_eq!(diagnostics.data_source, "index_diagnostic_journal");
    assert!(diagnostics.diagnostics.is_empty());
    assert!(diagnostics.page.unwrap().next_page_token.is_empty());

    let audit = client
        .list_audit_events(
            with_auth(
                api::ListAuditEventsRequest {
                    request_id: "req-admin-audit".to_string(),
                    principal_id: String::new(),
                    resource_id: String::new(),
                    action: "admin.repair.run".to_string(),
                    page: Some(api::PageRequest {
                        page_token: String::new(),
                        page_size: 5,
                    }),
                },
                &token,
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .into_inner();
    assert_eq!(audit.request_id, "req-admin-audit");
    assert_eq!(audit.data_source, "admin_audit_log");
    assert_eq!(audit.events.len(), 1);
    assert_eq!(audit.events[0].request_id, "req-admin-directory-repair");
    assert_eq!(audit.events[0].action, "admin.repair.run");
    assert!(audit.page.unwrap().next_page_token.is_empty());
}

#[tokio::test]
async fn missing_lifecycle_cli_handlers_call_admin_service_and_persist_state() {
    let node = spawn_admin_cli_node().await;
    let token = admin_token(&node);
    let mut client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();

    handle_region_command(
        &RegionCommands::Create {
            context: mutation_options("cli-create-region", 0),
            region: "eu-west-1".to_string(),
            public_base_url: "https://eu-west-1.anvil-storage.test".to_string(),
            virtual_host_suffix: "eu-west-1.anvil-storage.test".to_string(),
            placement_weight: 100,
            default_cell: Some("cell-a".to_string()),
        },
        &mut client,
        &token,
    )
    .await
    .unwrap();

    handle_cell_command(
        &CellCommands::Register {
            context: mutation_options("cli-register-cell", 0),
            region: "eu-west-1".to_string(),
            cell_id: "cell-a".to_string(),
            placement_weight: 100,
            failure_domain: "rack-a".to_string(),
        },
        &mut client,
        &token,
    )
    .await
    .unwrap();

    let cell = node
        .state
        .persistence
        .list_cell_descriptors(Some("eu-west-1"))
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    handle_cell_command(
        &CellCommands::Activate {
            context: mutation_options("cli-activate-cell", cell.generation),
            region: "eu-west-1".to_string(),
            cell_id: "cell-a".to_string(),
        },
        &mut client,
        &token,
    )
    .await
    .unwrap();

    handle_node_command(
        &NodeCommands::Register {
            context: mutation_options("cli-register-node", 0),
            node_id: "node-a".to_string(),
            region: "eu-west-1".to_string(),
            cell_id: "cell-a".to_string(),
            public_api_addr: "http://127.0.0.1:50051".to_string(),
            capabilities: vec![NodeCapabilityArg::Object, NodeCapabilityArg::Admin],
            receipt_signing_public_key_b64: base64::engine::general_purpose::STANDARD
                .encode(node.state.core_store.local_receipt_signing_public_key()),
            capacity_json: "{}".to_string(),
        },
        &mut client,
        &token,
    )
    .await
    .unwrap();

    let registered_node = node
        .state
        .persistence
        .list_node_descriptors(Some("eu-west-1"), Some("cell-a"))
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    handle_node_command(
        &NodeCommands::Activate {
            context: mutation_options("cli-activate-node", registered_node.generation),
            node_id: "node-a".to_string(),
        },
        &mut client,
        &token,
    )
    .await
    .unwrap();

    let region = node
        .state
        .persistence
        .list_region_descriptors()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let activation_checkpoint =
        write_activation_checkpoint_from_existing_streams(&node, "activate-region.json").await;
    handle_region_command(
        &RegionCommands::Activate {
            context: mutation_options("cli-activate-region", region.generation),
            region: "eu-west-1".to_string(),
            activation_checkpoint: activation_checkpoint.clone(),
        },
        &mut client,
        &token,
    )
    .await
    .unwrap();

    let active_region = node
        .state
        .persistence
        .list_region_descriptors()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    handle_region_command(
        &RegionCommands::SetReadOnly {
            context: mutation_options("cli-set-region-read-only", active_region.generation),
            region: "eu-west-1".to_string(),
        },
        &mut client,
        &token,
    )
    .await
    .unwrap();

    let read_only_region = node
        .state
        .persistence
        .list_region_descriptors()
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(
        read_only_region.state,
        anvil::mesh_lifecycle::LifecycleState::ReadOnly
    );

    handle_region_command(
        &RegionCommands::Activate {
            context: mutation_options(
                "cli-reactivate-read-only-region",
                read_only_region.generation,
            ),
            region: "eu-west-1".to_string(),
            activation_checkpoint,
        },
        &mut client,
        &token,
    )
    .await
    .unwrap();

    let active_node = node
        .state
        .persistence
        .list_node_descriptors(Some("eu-west-1"), Some("cell-a"))
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    handle_node_command(
        &NodeCommands::ForceOffline {
            context: mutation_options("cli-force-offline-node", active_node.generation),
            node_id: "node-a".to_string(),
        },
        &mut client,
        &token,
    )
    .await
    .unwrap();

    let offline_node = node
        .state
        .persistence
        .list_node_descriptors(Some("eu-west-1"), Some("cell-a"))
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(
        offline_node.state,
        anvil::mesh_lifecycle::LifecycleState::Offline
    );
}
