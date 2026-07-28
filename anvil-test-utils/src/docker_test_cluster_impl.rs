use super::*;

impl DockerTestCluster {
    pub(super) fn start_shared() -> Arc<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build Docker shared-cluster startup runtime");
        runtime.block_on(async {
            let _guard = TestClusterStartupLock::acquire("docker-shared-cluster").await;
            let cluster = Self::start_or_reuse().await;
            Arc::new(cluster)
        })
    }

    async fn start_or_reuse() -> Self {
        let _port_guard = docker_test_port_allocation_lock();
        let docker_image = require_docker_image();
        let compose_file = docker_compose_file();
        let project_name = docker_compose_project_name();
        let mut compose_env = vec![("ANVIL_IMAGE".to_string(), docker_image)];
        let ports = docker_shared_project_ports(&project_name, &mut compose_env);
        docker_compose_create_then_start(&compose_file, &project_name, &compose_env);

        let grpc_addrs = ports
            .api_ports
            .iter()
            .map(|port| format!("http://127.0.0.1:{port}"))
            .collect::<Vec<_>>();

        let admin_addrs = ports
            .admin_ports
            .iter()
            .map(|port| format!("http://127.0.0.1:{port}"))
            .collect::<Vec<_>>();

        let admin_token = mint_docker_system_admin_token("docker-system-admin");
        let wait_start = Instant::now();
        for addr in &admin_addrs {
            if !wait_for_docker_admin_ready(addr, &admin_token, Duration::from_secs(120)).await {
                dump_docker_cluster_diagnostics(&compose_file, &project_name, &compose_env);
                panic!("Docker Anvil admin endpoint did not become ready: {addr}");
            }
        }
        emit_test_timing(
            "docker_shared_cluster admin_ports_ready",
            wait_start.elapsed(),
        );
        ensure_docker_topology(&admin_addrs, &admin_token, &docker_test_region()).await;

        // Distributed CoreMeta readiness depends on the lifecycle projection
        // installed through the pre-readiness admin plane.
        let wait_start = Instant::now();
        for addr in &grpc_addrs {
            assert!(
                wait_for_http_ready(addr, Duration::from_secs(90)).await,
                "Docker Anvil test endpoint did not become ready: {addr}"
            );
        }
        emit_test_timing("docker_shared_cluster ports_ready", wait_start.elapsed());

        Self {
            project_name,
            compose_file,
            grpc_addrs,
            admin_addrs,
            region: docker_test_region(),
            public_region_host: format!("{}.anvil-storage.test", docker_test_region()),
            admin_token,
            compose_env,
            deferred_topologies: Mutex::new(std::collections::BTreeMap::new()),
            cleanup_on_drop: false,
            _cluster_permit: None,
        }
    }

    pub(super) async fn start_isolated(
        label: &str,
        region: &str,
        deferred_ordinal: Option<u8>,
        cluster_permit: OwnedSemaphorePermit,
    ) -> Self {
        let _port_guard = docker_test_port_allocation_lock();
        let docker_image = require_docker_image();
        let compose_file = docker_compose_file();
        let project_name = format!("anvil-test-{}-{}", label, uuid::Uuid::new_v4().simple());
        let ports = reserve_docker_host_ports(12);
        let (api_ports, admin_ports) = ports.split_at(6);
        let mut compose_env = vec![
            ("ANVIL_IMAGE".to_string(), docker_image),
            ("ANVIL_DOCKER_TEST_REGION".to_string(), region.to_string()),
            ("ANVIL_DOCKER_TEST_NODE_COUNT".to_string(), "6".to_string()),
        ];
        for (index, port) in api_ports.iter().enumerate() {
            compose_env.push((
                format!("ANVIL_TEST_API{}_PORT", index + 1),
                port.to_string(),
            ));
        }
        for (index, port) in admin_ports.iter().enumerate() {
            compose_env.push((
                format!("ANVIL_TEST_ADMIN{}_PORT", index + 1),
                port.to_string(),
            ));
        }
        let mut startup_cleanup = DockerStartupCleanupGuard::new(
            compose_file.clone(),
            project_name.clone(),
            compose_env.clone(),
        );
        docker_compose_create_then_start(&compose_file, &project_name, &compose_env);

        let grpc_addrs = api_ports
            .iter()
            .map(|port| format!("http://127.0.0.1:{port}"))
            .collect::<Vec<_>>();
        let admin_addrs = admin_ports
            .iter()
            .map(|port| format!("http://127.0.0.1:{port}"))
            .collect::<Vec<_>>();
        let admin_token = mint_docker_system_admin_token("docker-system-admin");
        let wait_start = Instant::now();
        for addr in &admin_addrs {
            if !wait_for_docker_admin_ready(addr, &admin_token, Duration::from_secs(120)).await {
                dump_docker_cluster_diagnostics(&compose_file, &project_name, &compose_env);
                panic!("isolated Docker Anvil admin endpoint did not become ready: {addr}");
            }
        }
        emit_test_timing(
            "docker_isolated_cluster admin_ports_ready",
            wait_start.elapsed(),
        );
        let deferred_topology = if let Some(ordinal) = deferred_ordinal {
            Some((
                ordinal,
                prepare_docker_topology_with_deferred_peer(
                    &admin_addrs,
                    &admin_token,
                    region,
                    ordinal,
                )
                .await,
            ))
        } else {
            ensure_docker_topology(&admin_addrs, &admin_token, region).await;
            None
        };

        // Distributed CoreMeta readiness depends on the lifecycle projection
        // installed through the pre-readiness admin plane.
        let wait_start = Instant::now();
        for (index, addr) in grpc_addrs.iter().enumerate() {
            if deferred_ordinal == Some(u8::try_from(index + 1).unwrap()) {
                continue;
            }
            if !wait_for_http_ready(addr, Duration::from_secs(90)).await {
                dump_docker_cluster_diagnostics(&compose_file, &project_name, &compose_env);
                panic!(
                    "isolated Docker Anvil peer {} endpoint did not become ready: {addr}",
                    index + 1
                );
            }
        }
        emit_test_timing("docker_isolated_cluster ports_ready", wait_start.elapsed());

        let deferred_topologies = deferred_topology.into_iter().collect();
        let cluster = Self {
            project_name,
            compose_file,
            grpc_addrs,
            admin_addrs,
            region: region.to_string(),
            public_region_host: format!("{region}.anvil-storage.test"),
            admin_token,
            compose_env,
            deferred_topologies: Mutex::new(deferred_topologies),
            cleanup_on_drop: true,
            _cluster_permit: Some(cluster_permit),
        };
        startup_cleanup.disarm();
        cluster
    }

    pub fn admin_token(&self) -> &str {
        &self.admin_token
    }

    pub fn grpc_addr_for_test(&self, _label: &str) -> String {
        // Admin-created credentials are currently anchored through anvil1 in
        // the Docker harness. Use the same public endpoint for data-plane
        // requests so ordinary API tests do not depend on cross-node control
        // projection timing. Dedicated distributed tests still stop/restart
        // and inspect individual nodes explicitly.
        self.grpc_addrs[0].clone()
    }

    pub async fn create_tenant(&self, tenant_name: &str) -> i64 {
        let started_at = Instant::now();
        let mut client = connect_docker_admin(&self.admin_addrs[0]).await;
        let mut request = tonic::Request::new(anvil::anvil_api::CreateTenantRequest {
            context: Some(test_admin_context(
                &format!("create-tenant-{tenant_name}"),
                0,
            )),
            name: tenant_name.to_string(),
            home_region: self.region.clone(),
        });
        add_docker_admin_bearer(&mut request, &self.admin_token);
        let tenant_id = client
            .create_tenant(request)
            .await
            .expect("Docker admin CreateTenant")
            .into_inner()
            .tenant
            .expect("tenant create response includes tenant")
            .tenant_id
            .parse::<i64>()
            .expect("tenant id should be numeric");
        emit_test_timing("docker_actor create_tenant", started_at.elapsed());
        tenant_id
    }

    pub async fn create_application_with_id(
        &self,
        tenant_id: i64,
        app_name: &str,
    ) -> (String, String, String) {
        let started_at = Instant::now();
        let mut client = connect_docker_admin(&self.admin_addrs[0]).await;
        let mut request = tonic::Request::new(anvil::anvil_api::CreateApplicationRequest {
            context: Some(test_admin_context(&format!("create-app-{app_name}"), 0)),
            tenant_id: tenant_id.to_string(),
            app_name: app_name.to_string(),
        });
        add_docker_admin_bearer(&mut request, &self.admin_token);
        let response = client
            .create_application(request)
            .await
            .expect("Docker admin CreateApplication")
            .into_inner();
        emit_test_timing("docker_actor create_application", started_at.elapsed());
        (response.app_id, response.client_id, response.client_secret)
    }

    pub async fn grant_application_policy(
        &self,
        tenant_id: i64,
        app_name: &str,
        action: &str,
        resource: &str,
    ) {
        let started_at = Instant::now();
        let mut last_error = None;
        for attempt in 1..=5 {
            let mut client = connect_docker_admin(&self.admin_addrs[0]).await;
            let mut request =
                tonic::Request::new(anvil::anvil_api::GrantApplicationPolicyRequest {
                    context: Some(test_admin_context(
                        &format!("grant-{app_name}-{action}-{attempt}"),
                        0,
                    )),
                    tenant_id: tenant_id.to_string(),
                    app_name: app_name.to_string(),
                    action: action.to_string(),
                    resource: resource.to_string(),
                });
            add_docker_admin_bearer(&mut request, &self.admin_token);
            match client.grant_application_policy(request).await {
                Ok(_) => {
                    emit_test_timing(
                        format!("docker_actor grant_application_policy action={action}"),
                        started_at.elapsed(),
                    );
                    return;
                }
                Err(error) => {
                    last_error = Some(error);
                    tokio::time::sleep(Duration::from_millis(100 * attempt)).await;
                }
            }
        }
        panic!(
            "Docker admin GrantApplicationPolicy failed after retries: {:?}",
            last_error
        );
    }

    pub async fn grant_application_policies(
        &self,
        tenant_id: i64,
        app_name: &str,
        policies: &[(String, String)],
    ) {
        let started_at = Instant::now();
        let mut last_error = None;
        for attempt in 1..=5 {
            let mut client = connect_docker_admin(&self.admin_addrs[0]).await;
            let mut request = tonic::Request::new(anvil::anvil_api::ApplicationPoliciesRequest {
                context: Some(test_admin_context(
                    &format!("grant-batch-{app_name}-{attempt}"),
                    0,
                )),
                tenant_id: tenant_id.to_string(),
                app_name: app_name.to_string(),
                policies: policies
                    .iter()
                    .map(
                        |(action, resource)| anvil::anvil_api::ApplicationPolicyMutation {
                            action: action.clone(),
                            resource: resource.clone(),
                        },
                    )
                    .collect(),
            });
            add_docker_admin_bearer(&mut request, &self.admin_token);
            match client.grant_application_policies(request).await {
                Ok(_) => {
                    emit_test_timing(
                        format!(
                            "docker_actor grant_application_policies count={}",
                            policies.len()
                        ),
                        started_at.elapsed(),
                    );
                    return;
                }
                Err(error) => {
                    last_error = Some(error);
                    tokio::time::sleep(Duration::from_millis(100 * attempt)).await;
                }
            }
        }
        panic!(
            "Docker admin GrantApplicationPolicies failed after retries: {:?}",
            last_error
        );
    }

    pub async fn create_storage_actor(&self, label: &str) -> DockerTestStorageActor {
        let total_started_at = Instant::now();
        let label = compact_resource_label(label, 18);
        let tenant_name = unique_test_name(&format!("{label}-tenant"));
        let tenant_id = self.create_tenant(&tenant_name).await;
        let app_name = unique_test_name(&format!("{label}-app"));
        let (app_id, client_id, client_secret) =
            self.create_application_with_id(tenant_id, &app_name).await;
        let tenant_resource = format!("tenant:{tenant_id}");
        let mut policies = vec![("tenant:manage".to_string(), tenant_resource)];
        policies.extend(
            DOCKER_AUTHZ_BOOTSTRAP_ACTIONS
                .iter()
                .map(|action| ((*action).to_string(), "default".to_string())),
        );
        self.grant_application_policies(tenant_id, &app_name, &policies)
            .await;
        let grpc_addr = self.grpc_addrs[0].clone();
        let token_started_at = Instant::now();
        let token = get_access_token_for_test(&grpc_addr, &client_id, &client_secret).await;
        emit_test_timing("docker_actor get_access_token", token_started_at.elapsed());
        emit_test_timing("docker_actor total", total_started_at.elapsed());
        DockerTestStorageActor {
            tenant_id,
            tenant_name: Some(tenant_name),
            app_id,
            app_name,
            client_id,
            client_secret,
            token,
            grpc_addr,
            region: self.region.clone(),
        }
    }

    pub async fn create_actor_in_tenant(
        &self,
        tenant_id: i64,
        label: &str,
        grants: &[(&str, &str)],
    ) -> DockerTestStorageActor {
        let label = compact_resource_label(label, 18);
        let app_name = unique_test_name(&format!("{label}-app"));
        let (app_id, client_id, client_secret) =
            self.create_application_with_id(tenant_id, &app_name).await;
        if !grants.is_empty() {
            let policies = grants
                .iter()
                .map(|(action, resource)| ((*action).to_string(), (*resource).to_string()))
                .collect::<Vec<_>>();
            self.grant_application_policies(tenant_id, &app_name, &policies)
                .await;
        }
        let grpc_addr = self.grpc_addrs[0].clone();
        let token = get_access_token_for_test(&grpc_addr, &client_id, &client_secret).await;
        DockerTestStorageActor {
            tenant_id,
            tenant_name: None,
            app_id,
            app_name,
            client_id,
            client_secret,
            token,
            grpc_addr,
            region: self.region.clone(),
        }
    }

    pub async fn stop_node(&self, node: u8) {
        let project_name = self.project_name.clone();
        tokio::task::spawn_blocking(move || {
            docker_container_command(&project_name, node, "stop");
        })
        .await
        .expect("Docker stop node command panicked");
    }

    pub async fn start_node(&self, node: u8) {
        let project_name = self.project_name.clone();
        let addr = self.grpc_addrs[(node - 1) as usize].clone();
        let admin_addr = self.admin_addrs[(node - 1) as usize].clone();
        let admin_token = self.admin_token.clone();
        tokio::task::spawn_blocking(move || {
            docker_container_command(&project_name, node, "start");
        })
        .await
        .expect("Docker start node command panicked");
        assert!(
            wait_for_http_ready(&addr, Duration::from_secs(90)).await,
            "Docker Anvil test endpoint did not become ready after restart: {addr}"
        );
        assert!(
            wait_for_docker_admin_ready(&admin_addr, &admin_token, Duration::from_secs(90)).await,
            "Docker Anvil admin endpoint did not become ready after restart: {admin_addr}"
        );
    }

    pub async fn exec_node_output(&self, node: u8, args: &[&str]) -> std::process::Output {
        let service = docker_node_service(node);
        let compose_file = self.compose_file.clone();
        let project_name = self.project_name.clone();
        let compose_env = self.compose_env.clone();
        let args = args.iter().map(|arg| arg.to_string()).collect::<Vec<_>>();
        tokio::task::spawn_blocking(move || {
            let mut command_args = vec!["exec".to_string(), "-T".to_string(), service];
            command_args.extend(args);
            let borrowed = command_args.iter().map(String::as_str).collect::<Vec<_>>();
            docker_compose_output_with_env(&compose_file, &project_name, &borrowed, &compose_env)
        })
        .await
        .expect("Docker exec node command panicked")
    }

    pub fn s3_client(&self, actor: &DockerTestStorageActor) -> S3Client {
        let credentials =
            Credentials::new(&actor.client_id, &actor.client_secret, None, None, "static");
        let config = aws_sdk_s3::Config::builder()
            .credentials_provider(credentials)
            .region(aws_sdk_s3::config::Region::new(self.region.clone()))
            .endpoint_url(&actor.grpc_addr)
            .force_path_style(true)
            .behavior_version(BehaviorVersion::latest())
            .build();
        S3Client::from_conf(config)
    }
}
