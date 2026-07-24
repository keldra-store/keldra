use std::path::PathBuf;
use std::time::{Duration, Instant};

use super::docker_image::configured_docker_image;

#[derive(Debug, Clone)]
pub(super) struct DockerHostPorts {
    pub api_ports: Vec<u16>,
    pub admin_ports: Vec<u16>,
}

pub(super) struct DockerStartupCleanupGuard {
    compose_file: PathBuf,
    project_name: String,
    compose_env: Vec<(String, String)>,
    armed: bool,
}

impl DockerStartupCleanupGuard {
    pub(super) fn new(
        compose_file: PathBuf,
        project_name: String,
        compose_env: Vec<(String, String)>,
    ) -> Self {
        Self {
            compose_file,
            project_name,
            compose_env,
            armed: true,
        }
    }

    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for DockerStartupCleanupGuard {
    fn drop(&mut self) {
        if self.armed {
            docker_compose_with_env(
                &self.compose_file,
                &self.project_name,
                &["down", "-v", "--remove-orphans"],
                &self.compose_env,
            );
        }
    }
}

pub(super) fn docker_command_with_env(extra_env: &[(String, String)]) -> std::process::Command {
    let mut command = command_with_docker_env("docker");
    // Docker Desktop can deadlock concurrent container starts for the shared
    // six-node cluster. Serialising Compose's control-plane operations does not
    // serialise the cluster or any test workload once the nodes are running.
    command.env("COMPOSE_PARALLEL_LIMIT", "1");
    for node in 1..=docker_node_count() {
        command.env(
            format!("ANVIL_TEST_NODE{node}_TOKEN"),
            mint_docker_system_admin_token(&format!("anvil-test-node-{node}")),
        );
    }
    for (key, value) in extra_env {
        command.env(key, value);
    }
    command
}

pub(super) fn docker_node_service(node: u8) -> String {
    assert!(
        (1..=6).contains(&node),
        "unsupported Docker test node {node}"
    );
    format!("anvil{node}")
}

pub(super) fn reserve_docker_host_ports(count: usize) -> Vec<u16> {
    (0..count)
        .map(|_| {
            let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
                .expect("reserve Docker test host port");
            let port = listener.local_addr().expect("reserved port address").port();
            drop(listener);
            port
        })
        .collect()
}

pub(super) fn docker_shared_project_ports(
    project_name: &str,
    compose_env: &mut Vec<(String, String)>,
) -> DockerHostPorts {
    let from_env = docker_ports_from_env();
    let ports = from_env.unwrap_or_else(|| docker_project_port_file(project_name));
    for (index, port) in ports.api_ports.iter().enumerate() {
        upsert_compose_env(
            compose_env,
            format!("ANVIL_TEST_API{}_PORT", index + 1),
            port.to_string(),
        );
    }
    for (index, port) in ports.admin_ports.iter().enumerate() {
        upsert_compose_env(
            compose_env,
            format!("ANVIL_TEST_ADMIN{}_PORT", index + 1),
            port.to_string(),
        );
    }
    ports
}

fn docker_ports_from_env() -> Option<DockerHostPorts> {
    let api_ports = read_numbered_ports_from_env("ANVIL_TEST_API")?;
    let admin_ports = read_numbered_ports_from_env("ANVIL_TEST_ADMIN")?;
    Some(DockerHostPorts {
        api_ports,
        admin_ports,
    })
}

fn read_numbered_ports_from_env(prefix: &str) -> Option<Vec<u16>> {
    let mut ports = Vec::with_capacity(docker_node_count() as usize);
    for node in 1..=docker_node_count() {
        let value = std::env::var(format!("{prefix}{node}_PORT")).ok()?;
        ports.push(value.parse::<u16>().ok()?);
    }
    Some(ports)
}

pub(super) fn docker_test_port_allocation_lock() -> DockerPortAllocationLock {
    let dir = std::env::temp_dir().join("anvil-test-cluster-locks");
    std::fs::create_dir_all(&dir).expect("create Docker test port state dir");
    DockerPortAllocationLock::acquire(&dir)
}

fn docker_project_port_file(project_name: &str) -> DockerHostPorts {
    let dir = std::env::temp_dir().join("anvil-test-cluster-locks");
    std::fs::create_dir_all(&dir).expect("create Docker test port state dir");
    let port_file = dir.join(format!("{}.ports", sanitize_project_filename(project_name)));
    if let Some(ports) = read_project_port_file(&port_file) {
        return ports;
    }
    let reserved = reserve_docker_host_ports((docker_node_count() as usize) * 2);
    let split_at = docker_node_count() as usize;
    let ports = DockerHostPorts {
        api_ports: reserved[..split_at].to_vec(),
        admin_ports: reserved[split_at..].to_vec(),
    };
    write_project_port_file(&port_file, &ports);
    ports
}

fn read_project_port_file(path: &std::path::Path) -> Option<DockerHostPorts> {
    let raw = std::fs::read_to_string(path).ok()?;
    let mut api_ports = Vec::with_capacity(docker_node_count() as usize);
    let mut admin_ports = Vec::with_capacity(docker_node_count() as usize);
    for node in 1..=docker_node_count() {
        api_ports.push(read_port_line(&raw, &format!("ANVIL_TEST_API{node}_PORT"))?);
        admin_ports.push(read_port_line(
            &raw,
            &format!("ANVIL_TEST_ADMIN{node}_PORT"),
        )?);
    }
    Some(DockerHostPorts {
        api_ports,
        admin_ports,
    })
}

fn read_port_line(raw: &str, key: &str) -> Option<u16> {
    raw.lines()
        .find_map(|line| line.strip_prefix(&format!("{key}=")))
        .and_then(|value| value.trim().parse::<u16>().ok())
}

fn write_project_port_file(path: &std::path::Path, ports: &DockerHostPorts) {
    let mut raw = String::new();
    for (index, port) in ports.api_ports.iter().enumerate() {
        raw.push_str(&format!("ANVIL_TEST_API{}_PORT={port}\n", index + 1));
    }
    for (index, port) in ports.admin_ports.iter().enumerate() {
        raw.push_str(&format!("ANVIL_TEST_ADMIN{}_PORT={port}\n", index + 1));
    }
    std::fs::write(path, raw).expect("write Docker test project port state");
}

fn upsert_compose_env(compose_env: &mut Vec<(String, String)>, key: String, value: String) {
    if let Some((_, existing)) = compose_env
        .iter_mut()
        .find(|(existing_key, _)| existing_key == &key)
    {
        *existing = value;
    } else {
        compose_env.push((key, value));
    }
}

fn sanitize_project_filename(project_name: &str) -> String {
    let mut sanitized = project_name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        sanitized = "anvil-shared-test".to_string();
    }
    sanitized
}

pub(super) struct DockerPortAllocationLock {
    path: PathBuf,
}

impl DockerPortAllocationLock {
    fn acquire(dir: &std::path::Path) -> Self {
        let path = dir.join("docker-port-allocation.lock");
        let start = Instant::now();
        loop {
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(mut file) => {
                    use std::io::Write;
                    let _ = writeln!(file, "pid={}", std::process::id());
                    return Self { path };
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    if port_lock_owner_is_dead(&path) || start.elapsed() > Duration::from_secs(300)
                    {
                        let _ = std::fs::remove_file(&path);
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(error) => panic!("acquire Docker test port allocation lock {path:?}: {error}"),
            }
        }
    }
}

fn port_lock_owner_is_dead(path: &std::path::Path) -> bool {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return true;
    };
    let Some(pid) = raw
        .lines()
        .find_map(|line| line.strip_prefix("pid="))
        .and_then(|value| value.trim().parse::<u32>().ok())
    else {
        return true;
    };

    #[cfg(unix)]
    {
        !std::process::Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

impl Drop for DockerPortAllocationLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub(super) fn docker_jwt_secret() -> String {
    std::env::var("ANVIL_DOCKER_TEST_JWT_SECRET")
        .unwrap_or_else(|_| "docker-test-secret".to_string())
}

pub(super) fn mint_docker_system_admin_token(app_id: &str) -> String {
    use jsonwebtoken::{EncodingKey, Header, encode};

    encode(
        &Header::default(),
        &anvil_core::auth::Claims {
            sub: app_id.to_string(),
            exp: 4_102_444_800,
            tenant_id: anvil_core::system_realm::SYSTEM_STORAGE_TENANT_ID,
            jti: Some(format!("docker-test-{app_id}")),
        },
        &EncodingKey::from_secret(docker_jwt_secret().as_bytes()),
    )
    .expect("mint deterministic Docker system token")
}

pub(super) fn add_docker_admin_bearer<T>(request: &mut tonic::Request<T>, token: &str) {
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}")
            .parse()
            .expect("admin bearer metadata is valid"),
    );
}

pub(super) fn docker_test_region() -> String {
    std::env::var("ANVIL_DOCKER_TEST_REGION").unwrap_or_else(|_| "test-region-1".to_string())
}

pub(super) fn docker_node_count() -> u8 {
    std::env::var("ANVIL_DOCKER_TEST_NODE_COUNT")
        .ok()
        .and_then(|value| value.parse::<u8>().ok())
        .filter(|value| (3..=6).contains(value))
        .unwrap_or(6)
}

pub(super) fn docker_compose_project_name() -> String {
    std::env::var("ANVIL_DOCKER_TEST_PROJECT").unwrap_or_else(|_| "anvil-shared-test".to_string())
}

pub(super) fn docker_compose_file() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../anvil/tests/docker-compose.test.yml")
        .canonicalize()
        .expect("canonicalize Docker test compose file")
}

pub(super) fn command_with_docker_env(cmd: &str) -> std::process::Command {
    let mut command = std::process::Command::new(cmd);
    if cmd == "docker" {
        command.env("ANVIL_IMAGE", configured_docker_image());
        command.env("ANVIL_DOCKER_TEST_REGION", docker_test_region());
        command.env(
            "ANVIL_DOCKER_TEST_NODE_COUNT",
            docker_node_count().to_string(),
        );
    }
    command
}

pub(super) fn docker_compose_with_env(
    compose_file: &std::path::Path,
    project_name: &str,
    args: &[&str],
    extra_env: &[(String, String)],
) {
    let mut command_args = vec![
        "compose",
        "-p",
        project_name,
        "-f",
        compose_file.to_str().expect("compose path is utf-8"),
    ];
    command_args.extend_from_slice(args);
    let output = docker_command_with_env(extra_env)
        .args(command_args)
        .output()
        .expect("failed to run docker compose command");
    assert!(
        output.status.success(),
        "docker compose command failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

pub(super) fn docker_compose_create_then_start(
    compose_file: &std::path::Path,
    project_name: &str,
    compose_env: &[(String, String)],
) {
    let expected_image = compose_env
        .iter()
        .find_map(|(key, value)| (key == "ANVIL_IMAGE").then_some(value.as_str()))
        .expect("Docker test compose env includes ANVIL_IMAGE");
    if docker_project_needs_recreate(project_name, expected_image, compose_env) {
        docker_compose_with_env(
            compose_file,
            project_name,
            &["down", "-v", "--remove-orphans"],
            compose_env,
        );
    }
    docker_compose_with_env(
        compose_file,
        project_name,
        &["create", "--remove-orphans"],
        compose_env,
    );
    for node in 1..=docker_node_count() {
        docker_container_command(project_name, node, "start");
    }
}

pub(super) fn docker_project_needs_recreate(
    project_name: &str,
    expected_image: &str,
    compose_env: &[(String, String)],
) -> bool {
    let expected_image_id = docker_image_id(expected_image);
    let mut seen_nodes = 0_u8;
    for node in 1..=docker_node_count() {
        let node_ids = docker_service_container_ids(project_name, &docker_node_service(node));
        match node_ids.as_slice() {
            [] => continue,
            [node_id] => {
                seen_nodes += 1;
                if docker_container_image_id(node_id) != expected_image_id {
                    return true;
                }
                if !docker_container_publishes_port(
                    node_id,
                    "50051/tcp",
                    &docker_host_port_with_env(node, compose_env),
                ) || !docker_container_publishes_port(
                    node_id,
                    "50052/tcp",
                    &docker_host_admin_port_with_env(node, compose_env),
                ) {
                    return true;
                }
            }
            _ => return true,
        }
    }
    seen_nodes != 0 && seen_nodes != docker_node_count()
}

fn docker_service_container_ids(project_name: &str, service: &str) -> Vec<String> {
    let output = command_with_docker_env("docker")
        .args([
            "ps",
            "-aq",
            "--filter",
            &format!("label=com.docker.compose.project={project_name}"),
            "--filter",
            &format!("label=com.docker.compose.service={service}"),
        ])
        .output()
        .expect("failed to inspect Docker test project");
    if !output.status.success() {
        return Vec::new();
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|id| !id.is_empty())
        .map(str::to_string)
        .collect()
}

pub(super) fn docker_image_id(image: &str) -> Option<String> {
    let output = command_with_docker_env("docker")
        .args(["image", "inspect", "--format", "{{.Id}}", image])
        .output()
        .expect("failed to inspect Docker test image");
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(super) fn docker_container_image_id(container_id: &str) -> Option<String> {
    let output = command_with_docker_env("docker")
        .args(["inspect", "--format", "{{.Image}}", container_id])
        .output()
        .expect("failed to inspect Docker test container image");
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(super) fn docker_container_publishes_port(
    container_id: &str,
    container_port: &str,
    host_port: &str,
) -> bool {
    let output = command_with_docker_env("docker")
        .args(["port", container_id, container_port])
        .output()
        .expect("failed to inspect Docker test container port mapping");
    output.status.success()
        && String::from_utf8_lossy(&output.stdout).lines().any(|line| {
            line.rsplit_once(':')
                .is_some_and(|(_, port)| port == host_port)
        })
}

pub(super) fn docker_container_command(project_name: &str, node: u8, operation: &str) {
    let container_id = docker_container_id(project_name, node);
    if operation == "start" && docker_container_is_running(&container_id) {
        return;
    }
    let mut last_failure = String::new();
    for _ in 0..3 {
        let mut child = command_with_docker_env("docker")
            .args([operation, &container_id])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("failed to control Docker test node");
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match child.try_wait().expect("poll Docker test node command") {
                Some(status) if status.success() => return,
                Some(status) => {
                    let output = child
                        .wait_with_output()
                        .expect("collect failed Docker test node command");
                    last_failure = format!(
                        "status={status}, stderr={}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                    break;
                }
                None if Instant::now() >= deadline => {
                    child.kill().expect("kill stalled Docker test node command");
                    let _ = child.wait();
                    last_failure = "Docker Engine start/stop request stalled for 10s".to_string();
                    break;
                }
                None => std::thread::sleep(Duration::from_millis(25)),
            }
        }
        if operation == "start" && docker_container_is_running(&container_id) {
            return;
        }
    }
    panic!("docker {operation} failed for node {node}: {last_failure}");
}

pub(super) fn docker_container_id(project_name: &str, node: u8) -> String {
    let service = docker_node_service(node);
    let output = command_with_docker_env("docker")
        .args([
            "ps",
            "-aq",
            "--filter",
            &format!("label=com.docker.compose.project={project_name}"),
            "--filter",
            &format!("label=com.docker.compose.service={service}"),
        ])
        .output()
        .expect("failed to locate Docker test node");
    assert!(
        output.status.success(),
        "failed to locate Docker test node {node}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let ids = String::from_utf8(output.stdout).expect("Docker container id is utf-8");
    let ids = ids
        .lines()
        .filter(|id| !id.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    assert_eq!(
        ids.len(),
        1,
        "expected one container for Docker test node {node}"
    );
    ids.into_iter().next().unwrap()
}

pub(super) fn docker_container_volume_name(
    project_name: &str,
    node: u8,
    destination: &str,
) -> String {
    let container_id = docker_container_id(project_name, node);
    let output = command_with_docker_env("docker")
        .args([
            "inspect",
            "--format",
            "{{range .Mounts}}{{println .Name \"|\" .Destination}}{{end}}",
            &container_id,
        ])
        .output()
        .expect("inspect Docker test node mounts");
    assert!(
        output.status.success(),
        "inspect Docker test node {node} mounts: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split_once('|'))
        .find_map(|(name, mounted_at)| {
            (mounted_at.trim() == destination).then(|| name.trim().to_string())
        })
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| panic!("Docker test node {node} has no volume at {destination}"))
}

pub(super) fn docker_clear_node_block_shards(
    project_name: &str,
    compose_env: &[(String, String)],
    node: u8,
) {
    let volume = docker_container_volume_name(project_name, node, "/var/lib/anvil");
    let image = compose_env
        .iter()
        .find_map(|(key, value)| (key == "ANVIL_IMAGE").then_some(value.as_str()))
        .expect("Docker test compose env includes ANVIL_IMAGE");
    docker_command_expect_success(
        &[
            "run",
            "--rm",
            "--entrypoint",
            "sh",
            "--volume",
            &format!("{volume}:/var/lib/anvil"),
            image,
            "-c",
            "rm -rf /var/lib/anvil/corestore/blocks/local-cache/*",
        ],
        &format!("clear Docker test node {node} block shards"),
    );
}

pub(super) fn docker_remove_node_block_shards(
    project_name: &str,
    compose_env: &[(String, String)],
    node: u8,
    paths: &[String],
) {
    assert!(
        !paths.is_empty(),
        "remove Docker block shards requires at least one path"
    );
    let volume = docker_container_volume_name(project_name, node, "/var/lib/anvil");
    let image = compose_env
        .iter()
        .find_map(|(key, value)| (key == "ANVIL_IMAGE").then_some(value.as_str()))
        .expect("Docker test compose env includes ANVIL_IMAGE");
    let mut args = vec![
        "run".to_string(),
        "--rm".to_string(),
        "--entrypoint".to_string(),
        "rm".to_string(),
        "--volume".to_string(),
        format!("{volume}:/var/lib/anvil"),
        image.to_string(),
        "-f".to_string(),
        "--".to_string(),
    ];
    args.extend(paths.iter().cloned());
    let borrowed = args.iter().map(String::as_str).collect::<Vec<_>>();
    docker_command_expect_success(
        &borrowed,
        &format!("remove exact Docker test node {node} block shards"),
    );
}

pub(super) fn docker_compose_network_name(project_name: &str) -> String {
    let output = command_with_docker_env("docker")
        .args([
            "network",
            "ls",
            "--format",
            "{{.Name}}",
            "--filter",
            &format!("label=com.docker.compose.project={project_name}"),
            "--filter",
            "label=com.docker.compose.network=anvilnet",
        ])
        .output()
        .expect("locate Docker Compose test network");
    assert!(
        output.status.success(),
        "locate Docker Compose test network: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let network_ids = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    assert_eq!(
        network_ids.len(),
        1,
        "expected one Docker Compose test network for {project_name}"
    );
    network_ids.into_iter().next().unwrap()
}

pub(super) fn docker_network_container_ipv4(network: &str, container_id: &str) -> String {
    let template = format!(
        "{{{{with index .NetworkSettings.Networks \"{network}\"}}}}{{{{.IPAddress}}}}{{{{end}}}}"
    );
    let output = command_with_docker_env("docker")
        .args(["inspect", "--format", &template, container_id])
        .output()
        .expect("inspect Docker peer network address");
    assert!(
        output.status.success(),
        "inspect Docker peer address for {container_id} on {network}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let address = String::from_utf8_lossy(&output.stdout).trim().to_string();
    address
        .parse::<std::net::Ipv4Addr>()
        .unwrap_or_else(|error| panic!("invalid Docker peer IPv4 address {address:?}: {error}"));
    address
}

pub(super) fn docker_set_unreachable_peer_routes(
    container_id: &str,
    peer_addresses: &[String],
    blocked: bool,
) {
    let mut addresses = peer_addresses
        .iter()
        .map(|address| {
            address
                .parse::<std::net::Ipv4Addr>()
                .unwrap_or_else(|error| {
                    panic!("invalid Docker peer IPv4 address {address}: {error}")
                })
        })
        .collect::<Vec<_>>();
    addresses.sort_unstable();
    addresses.dedup();
    let script = addresses
        .iter()
        .map(|address| {
            if blocked {
                format!("ip route replace unreachable {address}/32")
            } else {
                format!("ip route del unreachable {address}/32 2>/dev/null || true")
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    docker_command_expect_success(
        &["exec", "--user", "0", container_id, "sh", "-ceu", &script],
        &format!(
            "{} Docker peer routes for {container_id}",
            if blocked { "block" } else { "restore" }
        ),
    );
}

fn docker_command_expect_success(args: &[&str], operation: &str) {
    let output = command_with_docker_env("docker")
        .args(args)
        .output()
        .unwrap_or_else(|error| panic!("{operation}: {error}"));
    assert!(
        output.status.success(),
        "{operation}\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

pub(super) fn docker_container_is_running(container_id: &str) -> bool {
    command_with_docker_env("docker")
        .args(["inspect", "--format", "{{.State.Running}}", container_id])
        .output()
        .map(|output| output.status.success() && output.stdout == b"true\n")
        .unwrap_or(false)
}

pub(super) fn docker_compose_output_with_env(
    compose_file: &std::path::Path,
    project_name: &str,
    args: &[&str],
    extra_env: &[(String, String)],
) -> std::process::Output {
    let mut command_args = vec![
        "compose",
        "-p",
        project_name,
        "-f",
        compose_file.to_str().expect("compose path is utf-8"),
    ];
    command_args.extend_from_slice(args);
    docker_command_with_env(extra_env)
        .args(command_args)
        .output()
        .expect("failed to run docker compose command")
}

pub(super) fn docker_host_port(node: u8) -> String {
    let (var, default) = match node {
        1 => ("ANVIL_TEST_API1_PORT", "55051"),
        2 => ("ANVIL_TEST_API2_PORT", "55052"),
        3 => ("ANVIL_TEST_API3_PORT", "55053"),
        4 => ("ANVIL_TEST_API4_PORT", "55054"),
        5 => ("ANVIL_TEST_API5_PORT", "55055"),
        6 => ("ANVIL_TEST_API6_PORT", "55056"),
        7 => ("ANVIL_TEST_API7_PORT", "55057"),
        8 => ("ANVIL_TEST_API8_PORT", "55058"),
        9 => ("ANVIL_TEST_API9_PORT", "55059"),
        _ => panic!("unsupported Docker test node {node}"),
    };
    std::env::var(var).unwrap_or_else(|_| default.to_string())
}

fn docker_host_port_with_env(node: u8, compose_env: &[(String, String)]) -> String {
    let key = format!("ANVIL_TEST_API{node}_PORT");
    compose_env
        .iter()
        .find_map(|(existing_key, value)| (existing_key == &key).then_some(value.clone()))
        .unwrap_or_else(|| docker_host_port(node))
}

pub(super) fn docker_host_api_url(node: u8) -> String {
    format!("http://127.0.0.1:{}", docker_host_port(node))
}

pub(super) fn docker_host_admin_port(node: u8) -> String {
    let (var, default) = match node {
        1 => ("ANVIL_TEST_ADMIN1_PORT", "56051"),
        2 => ("ANVIL_TEST_ADMIN2_PORT", "56052"),
        3 => ("ANVIL_TEST_ADMIN3_PORT", "56053"),
        4 => ("ANVIL_TEST_ADMIN4_PORT", "56054"),
        5 => ("ANVIL_TEST_ADMIN5_PORT", "56055"),
        6 => ("ANVIL_TEST_ADMIN6_PORT", "56056"),
        7 => ("ANVIL_TEST_ADMIN7_PORT", "56057"),
        8 => ("ANVIL_TEST_ADMIN8_PORT", "56058"),
        9 => ("ANVIL_TEST_ADMIN9_PORT", "56059"),
        _ => panic!("unsupported Docker test node {node}"),
    };
    std::env::var(var).unwrap_or_else(|_| default.to_string())
}

fn docker_host_admin_port_with_env(node: u8, compose_env: &[(String, String)]) -> String {
    let key = format!("ANVIL_TEST_ADMIN{node}_PORT");
    compose_env
        .iter()
        .find_map(|(existing_key, value)| (existing_key == &key).then_some(value.clone()))
        .unwrap_or_else(|| docker_host_admin_port(node))
}

pub(super) fn docker_host_admin_url(node: u8) -> String {
    format!("http://127.0.0.1:{}", docker_host_admin_port(node))
}
