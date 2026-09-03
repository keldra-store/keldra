#[tokio::test]
async fn joining_ingress_proxies_the_page_to_every_active_source() {
    let (directories, stores, tenant_id, bucket_id, tokens, bearer) = replicated_stores().await;
    let placement = Arc::new(FixedPlacement::three_nodes());
    let peers = Arc::new(InProcessListPeers::new(
        stores.clone(),
        placement.clone(),
        tokens,
        PeerBehavior::Normal,
    ));
    let query = LocalListQuery::new(
        placement.fence(),
        "tenant",
        "bucket",
        tenant_id,
        bucket_id,
        "",
        None,
        10,
    )
    .unwrap();

    let page = gather_cluster_page(
        node(4),
        stores[&node(1)].clone(),
        placement.clone(),
        peers.clone(),
        peers.authorizers[&node(1)].clone(),
        bearer,
        query,
    )
    .await
    .unwrap()
    .page;

    assert_eq!(
        page.paths,
        [
            "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf"
        ]
    );
    assert_eq!(
        peers.calls.lock().unwrap().as_slice(),
        &[node(1), node(2), node(3)]
    );
    drop(peers);
    drop(stores);
    drop(directories);
}
