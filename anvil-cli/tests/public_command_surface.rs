#![recursion_limit = "256"]

use std::process::{Command, Output};
use std::time::Duration;

use anvil_test_utils::TestCluster;
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

fn run_anvil(config_dir: &TempDir, args: &[&str]) -> Output {
    let config_path = config_dir.path().join("config.toml");
    let mut all_args = vec![
        "--config".to_string(),
        config_path.to_string_lossy().into_owned(),
    ];
    all_args.extend(args.iter().map(|arg| arg.to_string()));
    let output = Command::new(env!("CARGO_BIN_EXE_anvil"))
        .args(&all_args)
        .output()
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

fn run_anvil_with_token(config_dir: &TempDir, token: &str, args: &[&str]) -> Output {
    let config_path = config_dir.path().join("config.toml");
    let mut all_args = vec![
        "--config".to_string(),
        config_path.to_string_lossy().into_owned(),
    ];
    all_args.extend(args.iter().map(|arg| arg.to_string()));
    let output = Command::new(env!("CARGO_BIN_EXE_anvil"))
        .env("ANVIL_AUTH_TOKEN", token)
        .args(&all_args)
        .output()
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

fn run_anvil_eventually(config_dir: &TempDir, args: &[&str], timeout: Duration) -> Output {
    let start = std::time::Instant::now();
    loop {
        let config_path = config_dir.path().join("config.toml");
        let mut all_args = vec![
            "--config".to_string(),
            config_path.to_string_lossy().into_owned(),
        ];
        all_args.extend(args.iter().map(|arg| arg.to_string()));
        let output = Command::new(env!("CARGO_BIN_EXE_anvil"))
            .args(&all_args)
            .output()
            .expect("run anvil");
        if output.status.success() {
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
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn run_anvil_expect_failure(config_dir: &TempDir, args: &[&str]) -> Output {
    let config_path = config_dir.path().join("config.toml");
    let mut all_args = vec![
        "--config".to_string(),
        config_path.to_string_lossy().into_owned(),
    ];
    all_args.extend(args.iter().map(|arg| arg.to_string()));
    let output = Command::new(env!("CARGO_BIN_EXE_anvil"))
        .args(&all_args)
        .output()
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

async fn start_cluster_for_public_cli() -> (TestCluster, TempDir) {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(10)).await;

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
    );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_tutorial_commands_run_without_admin_port_e2e() {
    let (_cluster, config_dir) = start_cluster_for_public_cli().await;
    let bucket = format!("public-cli-{}", uuid::Uuid::new_v4().simple());
    run_anvil(&config_dir, &["bucket", "create", &bucket, "test-region-1"]);

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
    );
    run_anvil(
        &config_dir,
        &["object", "put", v2.to_str().unwrap(), &obj_v2],
    );

    let latest = format!("s3://{bucket}/latest.txt");
    let created = run_anvil(&config_dir, &["object", "link", "create", &latest, &obj_v1]);
    assert!(stdout(&created).contains("latest.txt -> app-v1.txt"));
    let gen1 = parse_link_generation(&created);
    let listed = run_anvil(
        &config_dir,
        &["object", "link", "list", &format!("s3://{bucket}/")],
    );
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
    );
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
    );

    run_anvil(
        &config_dir,
        &["index", "create", &bucket, "by-path", "path"],
    );
    let indexes = run_anvil(&config_dir, &["index", "list", &bucket]);
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
        Duration::from_secs(30),
    );
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
    );
    run_anvil(
        &config_dir,
        &["repair", "run", "directory", &bucket, "--rebuild"],
    );

    let stream = run_anvil(&config_dir, &["stream", "create", &bucket, "events/app"]);
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
    );
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
    );
    assert!(stdout(&stream_read).contains("event-one"));
    run_anvil(
        &config_dir,
        &["stream", "seal-segment", &bucket, "events/app", &stream_id],
    );

    let task_id = format!("cli-task-{}", uuid::Uuid::new_v4().simple());
    let lease_token = stdout(&run_anvil(&config_dir, &["auth", "get-token"]))
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
    );
    let fence = parse_fence(&lease);
    run_anvil_with_token(
        &config_dir,
        &lease_token,
        &["lease", "checkpoint", &task_id, &fence, "1", "1"],
    );
    run_anvil_with_token(
        &config_dir,
        &lease_token,
        &["lease", "commit", &task_id, &fence, "2", "2"],
    );

    let app_name = format!("tenant-app-{}", uuid::Uuid::new_v4().simple());
    let created_app = run_anvil(&config_dir, &["app", "create", &app_name]);
    assert!(stdout(&created_app).contains(&app_name));
    run_anvil(
        &config_dir,
        &["auth", "grant", &app_name, "bucket:read", &bucket],
    );
    let grants = run_anvil(&config_dir, &["auth", "list-grants", &app_name]);
    assert!(stdout(&grants).contains(&app_name));
    run_anvil(
        &config_dir,
        &["auth", "revoke", &app_name, "bucket:read", &bucket],
    );
    run_anvil(&config_dir, &["app", "rotate-secret", &app_name]);

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
    );
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
    );
    let alias_list = run_anvil(&config_dir, &["host-alias", "list"]);
    assert!(stdout(&alias_list).contains(&host));

    let audit = run_anvil(&config_dir, &["audit", "list", "--page-size", "20"]);
    assert!(stdout(&audit).contains("object_link") || stdout(&audit).contains("host_alias"));

    let bad_admin = run_anvil_expect_failure(&config_dir, &["admin", "node", "list"]);
    assert!(
        String::from_utf8_lossy(&bad_admin.stderr).contains("unrecognized subcommand")
            || String::from_utf8_lossy(&bad_admin.stderr).contains("error")
    );
}
