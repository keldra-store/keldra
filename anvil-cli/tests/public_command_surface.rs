#![recursion_limit = "256"]

use std::process::{Command, Output};
use std::time::Duration;

use anvil::anvil_api::{
    AcquireTaskLeaseRequest, CheckpointTaskLeaseRequest, CommitTaskLeaseRequest,
    ReadTaskLeaseRequest, coordination_service_client::CoordinationServiceClient,
};
use anvil_test_utils::{TestCluster, authenticated_request};
use tempfile::{TempDir, tempdir};

fn assert_anvil_help(args: &[&str], expected: &[&str]) {
    let output = Command::new(env!("CARGO_BIN_EXE_anvil"))
        .args(args)
        .arg("--help")
        .output()
        .expect("run anvil help");
    assert!(
        output.status.success(),
        "anvil {:?} --help failed\nstdout:\n{}\nstderr:\n{}",
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    for item in expected {
        assert!(
            stdout.contains(item),
            "help for {:?} missing {item}\n{stdout}",
            args
        );
    }
}

async fn run_anvil(config_dir: &TempDir, args: &[&str]) -> Output {
    let config_path = config_dir.path().join("config.toml");
    let mut all_args = vec![
        "--config".to_string(),
        config_path.to_string_lossy().into_owned(),
    ];
    all_args.extend(args.iter().map(|arg| arg.to_string()));
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_anvil"))
        .args(&all_args)
        .output()
        .await
        .expect("run anvil");
    if !output.status.success() {
        panic!(
            "anvil {} failed\nstdout:\n{}\nstderr:\n{}",
            all_args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    output
}

async fn run_anvil_with_token(config_dir: &TempDir, token: &str, args: &[&str]) -> Output {
    let config_path = config_dir.path().join("config.toml");
    let mut all_args = vec![
        "--config".to_string(),
        config_path.to_string_lossy().into_owned(),
    ];
    all_args.extend(args.iter().map(|arg| arg.to_string()));
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_anvil"))
        .env("ANVIL_AUTH_TOKEN", token)
        .args(&all_args)
        .output()
        .await
        .expect("run anvil");
    if !output.status.success() {
        panic!(
            "anvil {} failed\nstdout:\n{}\nstderr:\n{}",
            all_args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    output
}

async fn run_anvil_eventually(
    config_dir: &TempDir,
    args: &[&str],
    expected_stdout: &[&str],
    timeout: Duration,
) -> Output {
    let start = std::time::Instant::now();
    loop {
        let config_path = config_dir.path().join("config.toml");
        let mut all_args = vec![
            "--config".to_string(),
            config_path.to_string_lossy().into_owned(),
        ];
        all_args.extend(args.iter().map(|arg| arg.to_string()));
        let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_anvil"))
            .args(&all_args)
            .output()
            .await
            .expect("run anvil");
        let contains_expected_stdout = {
            let stdout = String::from_utf8_lossy(&output.stdout);
            expected_stdout
                .iter()
                .all(|expected| stdout.contains(expected))
        };
        if output.status.success() && contains_expected_stdout {
            return output;
        }
        if start.elapsed() >= timeout {
            panic!(
                "anvil {} did not succeed within {:?}\nstdout:\n{}\nstderr:\n{}",
                all_args.join(" "),
                timeout,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn run_anvil_expect_failure(config_dir: &TempDir, args: &[&str]) -> Output {
    let config_path = config_dir.path().join("config.toml");
    let mut all_args = vec![
        "--config".to_string(),
        config_path.to_string_lossy().into_owned(),
    ];
    all_args.extend(args.iter().map(|arg| arg.to_string()));
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_anvil"))
        .args(&all_args)
        .output()
        .await
        .expect("run anvil");
    assert!(
        !output.status.success(),
        "anvil {} unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        all_args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

async fn start_six_node_public_cluster() -> TestCluster {
    // The production ec-4-2 profile requires six distinct node failure domains.
    let mut cluster = TestCluster::new(&[
        "test-region-1",
        "test-region-1",
        "test-region-1",
        "test-region-1",
        "test-region-1",
        "test-region-1",
    ])
    .await;
    cluster.start_and_converge(Duration::from_secs(10)).await;
    cluster
}

async fn start_cluster_for_public_cli() -> (TestCluster, TempDir) {
    let cluster = start_six_node_public_cluster().await;
    let config_dir = tempdir().unwrap();
    let app_name = format!("public-cli-{}", uuid::Uuid::new_v4().simple());
    let (client_id, client_secret) = cluster
        .create_application_with_storage_tenant_owner("default", &app_name)
        .await;
    run_anvil(
        &config_dir,
        &[
            "static-config",
            "--name",
            "default",
            "--host",
            &cluster.grpc_addrs[0],
            "--client-id",
            &client_id,
            "--client-secret",
            &client_secret,
            "--default",
        ],
    )
    .await;
    (cluster, config_dir)
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn parse_stream_id(output: &Output) -> String {
    stdout(output)
        .split_whitespace()
        .find_map(|part| part.strip_prefix("stream_id="))
        .expect("stream_id in output")
        .to_string()
}

fn parse_link_generation(output: &Output) -> String {
    let text = stdout(output);
    let marker = "generation ";
    let start = text.find(marker).expect("link generation in output") + marker.len();
    text[start..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect()
}

fn parse_host_alias_generation(output: &Output) -> String {
    let text = stdout(output);
    let marker = "generation ";
    let start = text.find(marker).expect("host alias generation in output") + marker.len();
    text[start..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect()
}

fn parse_host_alias_challenge(output: &Output) -> String {
    stdout(output)
        .lines()
        .find_map(|line| line.strip_prefix("verification_challenge="))
        .expect("verification challenge in output")
        .to_string()
}

fn parse_fence(output: &Output) -> String {
    stdout(output)
        .split_whitespace()
        .find_map(|part| part.strip_prefix("fence="))
        .expect("fence token in output")
        .to_string()
}

#[test]
fn public_cli_link_lifecycle_e2e() {
    assert_anvil_help(
        &["object", "link"],
        &["create", "update", "delete", "read", "list"],
    );
}

#[test]
fn public_cli_index_lifecycle_and_query_e2e() {
    assert_anvil_help(
        &["index"],
        &[
            "create",
            "update",
            "disable",
            "drop",
            "list",
            "query",
            "diagnostics",
        ],
    );
}

#[test]
fn public_cli_watch_prefix_e2e() {
    assert_anvil_help(
        &["watch"],
        &[
            "prefix",
            "index-definition",
            "index-partition",
            "authz",
            "personaldb",
        ],
    );
}

#[test]
fn public_cli_personaldb_submit_and_catchup_e2e() {
    assert_anvil_help(
        &["personaldb"],
        &["group", "projection", "changeset", "catch-up", "watch"],
    );
}

#[test]
fn public_cli_append_stream_lifecycle_e2e() {
    assert_anvil_help(
        &["stream"],
        &["create", "append", "read", "tail", "seal-segment"],
    );
}

#[test]
fn public_cli_coordination_lease_fence_e2e() {
    assert_anvil_help(
        &["lease"],
        &["acquire", "checkpoint", "commit", "read", "force-release"],
    );
}

#[test]
fn public_cli_authz_schema_tuple_check_e2e() {
    assert_anvil_help(
        &["authz"],
        &[
            "schema",
            "tuple",
            "check",
            "list-objects",
            "list-subjects",
            "watch",
        ],
    );
}

#[test]
fn public_cli_host_alias_verification_e2e() {
    assert_anvil_help(
        &["host-alias"],
        &["create", "verify", "read", "list", "delete"],
    );
}

#[test]
fn admin_cli_rejects_public_port_e2e() {
    let output = Command::new(env!("CARGO_BIN_EXE_anvil-admin"))
        .args(["--host", "http://127.0.0.1:1", "node", "list"])
        .output()
        .expect("run anvil-admin against closed public-like port");
    assert!(!output.status.success());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
async fn public_named_task_lease_mutations_are_cluster_endpoint_agnostic_e2e() {
    let cluster = start_six_node_public_cluster().await;
    let token = cluster.token.clone();
    let task_id = format!("public-lease-{}", uuid::Uuid::new_v4().simple());

    let mut node_a = CoordinationServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut node_b = CoordinationServiceClient::connect(cluster.grpc_addrs[3].clone())
        .await
        .unwrap();
    let mut node_c = CoordinationServiceClient::connect(cluster.grpc_addrs[4].clone())
        .await
        .unwrap();
    let mut node_d = CoordinationServiceClient::connect(cluster.grpc_addrs[5].clone())
        .await
        .unwrap();

    let acquired = node_a
        .acquire_task_lease(authenticated_request(
            tonic::Request::new(AcquireTaskLeaseRequest {
                task_id: task_id.clone(),
                task_kind: "public-endpoint-regression".to_string(),
                partition_family: "bucket".to_string(),
                partition_id: format!("{:064x}", 1_u8),
                owner_label: "public-client".to_string(),
                source_cursor_low: 0,
                source_cursor_high: 0,
                requested_ttl_nanos: 30_000_000_000,
                options: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .lease
        .expect("acquired lease");

    let checkpointed = node_b
        .checkpoint_task_lease(authenticated_request(
            tonic::Request::new(CheckpointTaskLeaseRequest {
                task_id: task_id.clone(),
                fence_token: acquired.fence_token,
                checkpoint_cursor_low: 1,
                checkpoint_cursor_high: 0,
                expected_root_generation: acquired.root_generation,
                expected_lease_epoch: acquired.lease_epoch,
                expected_expires_at_nanos: acquired.expires_at_nanos,
                expected_lease_hash: acquired.lease_hash,
                options: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .lease
        .expect("checkpointed lease");
    assert_eq!(checkpointed.checkpoint_cursor_low, 1);

    let committed = node_c
        .commit_task_lease(authenticated_request(
            tonic::Request::new(CommitTaskLeaseRequest {
                task_id: task_id.clone(),
                fence_token: checkpointed.fence_token,
                committed_cursor_low: 2,
                committed_cursor_high: 0,
                expected_root_generation: checkpointed.root_generation,
                expected_lease_epoch: checkpointed.lease_epoch,
                expected_expires_at_nanos: checkpointed.expires_at_nanos,
                expected_lease_hash: checkpointed.lease_hash,
                options: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(committed.committed);
    assert_eq!(
        committed
            .previous_lease
            .expect("committed lease")
            .checkpoint_cursor_low,
        2
    );

    let read = node_d
        .read_task_lease(authenticated_request(
            tonic::Request::new(ReadTaskLeaseRequest { task_id }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!read.found);
    assert!(read.lease.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
async fn tenant_tutorial_commands_run_without_admin_port_e2e() {
    let (_cluster, config_dir) = start_cluster_for_public_cli().await;
    let bucket = format!("public-cli-{}", uuid::Uuid::new_v4().simple());
    run_anvil(&config_dir, &["bucket", "create", &bucket, "test-region-1"]).await;

    let temp = tempdir().unwrap();
    let v1 = temp.path().join("v1.txt");
    let v2 = temp.path().join("v2.txt");
    std::fs::write(&v1, "version one").unwrap();
    std::fs::write(&v2, "version two").unwrap();
    let obj_v1 = format!("s3://{bucket}/app-v1.txt");
    let obj_v2 = format!("s3://{bucket}/app-v2.txt");
    run_anvil(
        &config_dir,
        &["object", "put", v1.to_str().unwrap(), &obj_v1],
    )
    .await;
    run_anvil(
        &config_dir,
        &["object", "put", v2.to_str().unwrap(), &obj_v2],
    )
    .await;

    let latest = format!("s3://{bucket}/latest.txt");
    let created = run_anvil(&config_dir, &["object", "link", "create", &latest, &obj_v1]).await;
    assert!(stdout(&created).contains("latest.txt -> app-v1.txt"));
    let gen1 = parse_link_generation(&created);
    let listed = run_anvil(
        &config_dir,
        &["object", "link", "list", &format!("s3://{bucket}/")],
    )
    .await;
    assert!(stdout(&listed).contains("latest.txt -> app-v1.txt"));
    let updated = run_anvil(
        &config_dir,
        &[
            "object",
            "link",
            "update",
            &latest,
            &obj_v2,
            "--expected-generation",
            &gen1,
        ],
    )
    .await;
    assert!(stdout(&updated).contains("latest.txt -> app-v2.txt"));
    let gen2 = parse_link_generation(&updated);
    run_anvil(
        &config_dir,
        &[
            "object",
            "link",
            "delete",
            &latest,
            "--expected-generation",
            &gen2,
        ],
    )
    .await;

    run_anvil(
        &config_dir,
        &["index", "create", &bucket, "by-path", "path"],
    )
    .await;
    let indexes = run_anvil(&config_dir, &["index", "list", &bucket]).await;
    assert!(stdout(&indexes).contains("by-path"));
    let query = run_anvil_eventually(
        &config_dir,
        &[
            "index",
            "query",
            &bucket,
            "by-path",
            "--path-prefix",
            "app-",
            "--limit",
            "2",
        ],
        &["app-v1.txt", "app-v2.txt"],
        Duration::from_secs(30),
    )
    .await;
    let query_output = stdout(&query);
    assert!(query_output.contains("app-v1.txt"), "{query_output}");
    assert!(query_output.contains("app-v2.txt"), "{query_output}");
    run_anvil(
        &config_dir,
        &[
            "diagnostics",
            "list",
            &bucket,
            "by-path",
            "--page-size",
            "5",
        ],
    )
    .await;
    run_anvil(
        &config_dir,
        &["repair", "run", "directory", &bucket, "--rebuild"],
    )
    .await;

    let stream = run_anvil(&config_dir, &["stream", "create", &bucket, "events/app"]).await;
    let stream_id = parse_stream_id(&stream);
    run_anvil(
        &config_dir,
        &[
            "stream",
            "append",
            &bucket,
            "events/app",
            &stream_id,
            "event-one",
        ],
    )
    .await;
    let stream_read = run_anvil(
        &config_dir,
        &[
            "stream",
            "read",
            &bucket,
            "events/app",
            &stream_id,
            "--include-payload",
        ],
    )
    .await;
    assert!(stdout(&stream_read).contains("event-one"));
    run_anvil(
        &config_dir,
        &["stream", "seal-segment", &bucket, "events/app", &stream_id],
    )
    .await;

    let task_id = format!("cli-task-{}", uuid::Uuid::new_v4().simple());
    let lease_token = stdout(&run_anvil(&config_dir, &["auth", "get-token"]).await)
        .trim()
        .to_string();
    let lease_partition = format!("{:064x}", 1_u8);
    let lease = run_anvil_with_token(
        &config_dir,
        &lease_token,
        &[
            "lease",
            "acquire",
            &task_id,
            "tutorial",
            "bucket",
            &lease_partition,
        ],
    )
    .await;
    let fence = parse_fence(&lease);
    run_anvil_with_token(
        &config_dir,
        &lease_token,
        &["lease", "checkpoint", &task_id, &fence, "1", "1"],
    )
    .await;
    run_anvil_with_token(
        &config_dir,
        &lease_token,
        &["lease", "commit", &task_id, &fence, "2", "2"],
    )
    .await;

    let app_name = format!("tenant-app-{}", uuid::Uuid::new_v4().simple());
    let created_app = run_anvil(&config_dir, &["app", "create", &app_name]).await;
    assert!(stdout(&created_app).contains(&app_name));
    run_anvil(
        &config_dir,
        &["auth", "grant", &app_name, "bucket:read", &bucket],
    )
    .await;
    let grants = run_anvil(&config_dir, &["auth", "list-grants", &app_name]).await;
    assert!(stdout(&grants).contains(&app_name));
    run_anvil(
        &config_dir,
        &["auth", "revoke", &app_name, "bucket:read", &bucket],
    )
    .await;
    run_anvil(&config_dir, &["app", "rotate-secret", &app_name]).await;

    let host = format!("{}.example.test", uuid::Uuid::new_v4().simple());
    let alias = run_anvil(
        &config_dir,
        &[
            "host-alias",
            "create",
            &host,
            &bucket,
            "--region",
            "test-region-1",
        ],
    )
    .await;
    let challenge = parse_host_alias_challenge(&alias);
    let alias_generation = parse_host_alias_generation(&alias);
    run_anvil(
        &config_dir,
        &[
            "host-alias",
            "verify",
            &host,
            &challenge,
            "--expected-generation",
            &alias_generation,
        ],
    )
    .await;
    let alias_list = run_anvil(&config_dir, &["host-alias", "list"]).await;
    assert!(stdout(&alias_list).contains(&host));

    let audit = run_anvil(&config_dir, &["audit", "list", "--page-size", "20"]).await;
    assert!(stdout(&audit).contains("object_link") || stdout(&audit).contains("host_alias"));

    let bad_admin = run_anvil_expect_failure(&config_dir, &["admin", "node", "list"]).await;
    assert!(
        String::from_utf8_lossy(&bad_admin.stderr).contains("unrecognized subcommand")
            || String::from_utf8_lossy(&bad_admin.stderr).contains("error")
    );
}
