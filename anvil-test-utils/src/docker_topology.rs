use super::*;

async fn local_node_descriptors(
    admin_addrs: &[String],
    admin_token: &str,
) -> Vec<anvil::anvil_api::NodeDescriptor> {
    let mut descriptors = Vec::with_capacity(admin_addrs.len());
    for admin_addr in admin_addrs {
        let mut client = connect_docker_admin(admin_addr).await;
        let mut request = tonic::Request::new(anvil::anvil_api::GetLocalNodeDescriptorRequest {});
        add_docker_admin_bearer(&mut request, admin_token);
        descriptors.push(
            client
                .get_local_node_descriptor(request)
                .await
                .expect("Docker admin GetLocalNodeDescriptor")
                .into_inner()
                .node
                .expect("local node descriptor response includes node"),
        );
    }
    descriptors
}

pub(super) async fn connect_docker_mesh_control(addr: &str) -> MeshControlServiceClient<Channel> {
    let mut last_error = None;
    for attempt in 1..=20 {
        match MeshControlServiceClient::connect(addr.to_string()).await {
            Ok(client) => return client,
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(100 * attempt)).await;
            }
        }
    }
    panic!("connect Docker mesh-control endpoint {addr}: {last_error:?}");
}

fn put_node_request(
    descriptor: &anvil::anvil_api::NodeDescriptor,
) -> anvil::anvil_api::PutNodeRequest {
    anvil::anvil_api::PutNodeRequest {
        node_id: descriptor.node_id.clone(),
        region_id: descriptor.region.clone(),
        cell_id: descriptor.cell_id.clone(),
        advertise_addr: descriptor.public_api_addr.clone(),
        state: "active".to_string(),
        capacity_json: "{}".to_string(),
        options: None,
        receipt_signing_public_key: descriptor.receipt_signing_public_key.clone(),
        capabilities: vec![
            "object".to_string(),
            "index".to_string(),
            "personaldb".to_string(),
            "metadata".to_string(),
            "gateway".to_string(),
            "admin".to_string(),
        ],
    }
}

fn bootstrap_topology_request(
    region: &str,
    descriptors: &[anvil::anvil_api::NodeDescriptor],
) -> anvil::anvil_api::BootstrapMeshTopologyRequest {
    let mut cells = BTreeSet::new();
    let cells = descriptors
        .iter()
        .filter_map(|descriptor| {
            cells
                .insert(descriptor.cell_id.clone())
                .then(|| anvil::anvil_api::PutCellRequest {
                    region_id: region.to_string(),
                    cell_id: descriptor.cell_id.clone(),
                    failure_domain: descriptor.cell_id.clone(),
                    state: "active".to_string(),
                    options: None,
                })
        })
        .collect();

    anvil::anvil_api::BootstrapMeshTopologyRequest {
        regions: vec![anvil::anvil_api::PutRegionRequest {
            region_id: region.to_string(),
            endpoint: format!("http://{region}.anvil-storage.test"),
            state: "active".to_string(),
            options: None,
        }],
        cells,
        nodes: descriptors.iter().map(put_node_request).collect(),
        canonical_coremeta_rows: Vec::new(),
    }
}

pub(super) async fn ensure_docker_topology(
    admin_addrs: &[String],
    admin_token: &str,
    region: &str,
) {
    let deferred = prepare_docker_topology(admin_addrs, admin_token, region, None).await;
    debug_assert!(deferred.is_none());
}

pub(super) async fn prepare_docker_topology_with_deferred_peer(
    admin_addrs: &[String],
    admin_token: &str,
    region: &str,
    deferred_ordinal: u8,
) -> anvil::anvil_api::BootstrapMeshTopologyRequest {
    assert!(
        (2..=admin_addrs.len()).contains(&usize::from(deferred_ordinal)),
        "deferred Docker peer must be a non-seed topology participant"
    );
    prepare_docker_topology(admin_addrs, admin_token, region, Some(deferred_ordinal))
        .await
        .expect("fresh Docker topology produced a deferred peer admission request")
}

async fn prepare_docker_topology(
    admin_addrs: &[String],
    admin_token: &str,
    region: &str,
    deferred_ordinal: Option<u8>,
) -> Option<anvil::anvil_api::BootstrapMeshTopologyRequest> {
    let descriptors = local_node_descriptors(admin_addrs, admin_token).await;
    if std::env::var_os("ANVIL_TEST_TIMINGS").is_some() {
        eprintln!(
            "[timing] docker_topology local_nodes={:?}",
            descriptors
                .iter()
                .map(|descriptor| (&descriptor.node_id, &descriptor.public_api_addr))
                .collect::<Vec<_>>()
        );
    }
    let topology = bootstrap_topology_request(region, &descriptors);

    // Genesis is installed directly into each node's local CoreMeta before any
    // node participates in quorum. Every node therefore starts normal writes
    // with the same active topology instead of independently replaying the
    // lifecycle API and producing divergent root histories.
    let mut seed_client = connect_docker_mesh_control(&admin_addrs[0]).await;
    let mut seed_request = tonic::Request::new(topology.clone());
    add_docker_admin_bearer(&mut seed_request, admin_token);
    let seed = seed_client
        .bootstrap_mesh_topology(seed_request)
        .await
        .expect("Docker seed BootstrapMeshTopology")
        .into_inner();
    if seed.already_initialised {
        assert!(
            deferred_ordinal.is_none(),
            "cannot defer a peer after Docker topology is already initialised"
        );
        return None;
    }
    assert!(
        !seed.canonical_coremeta_rows.is_empty(),
        "Docker seed topology bootstrap must return a canonical CoreMeta snapshot"
    );

    let mut deferred_request = None;
    for (index, admin_addr) in admin_addrs.iter().enumerate().skip(1) {
        let ordinal = u8::try_from(index + 1).expect("Docker peer ordinal fits in u8");
        let mut join = topology.clone();
        join.canonical_coremeta_rows = seed.canonical_coremeta_rows.clone();
        if deferred_ordinal == Some(ordinal) {
            deferred_request = Some(join);
            continue;
        }
        let mut client = connect_docker_mesh_control(admin_addr).await;
        let mut request = tonic::Request::new(join);
        add_docker_admin_bearer(&mut request, admin_token);
        let response = client
            .bootstrap_mesh_topology(request)
            .await
            .expect("Docker joining BootstrapMeshTopology")
            .into_inner();
        assert!(
            !response.already_initialised,
            "fresh Docker joining node unexpectedly had an existing mesh topology"
        );
    }
    deferred_request
}
