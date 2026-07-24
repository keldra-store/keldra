use super::*;

// This test stays in-process because it reads bucket state through
// cluster.states.persistence and directly toggles the generated public tuple.
#[tokio::test]
async fn test_set_public_access_and_get() {
    let cluster = shared_default_test_cluster().await;
    let route_tenant_id = cluster.states[0]
        .persistence
        .get_tenant_by_name("default")
        .await
        .unwrap()
        .unwrap()
        .id
        .to_string();
    let route_tenant_i64 = route_tenant_id.parse().unwrap();

    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();

    let token = cluster.token.clone();
    let principal_id = default_test_app_id(&cluster).await;
    let bucket_name = unique_test_name("public-access");
    let object_key = "public-object".to_string();

    let mut create_bucket_req = tonic::Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),

        options: None,
    });
    create_bucket_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let bucket_id = bucket_client
        .create_bucket(create_bucket_req)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    // Put an object
    let metadata = ObjectMetadata {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        mutation_context: Some(native_mutation_context(
            route_tenant_i64,
            bucket_id,
            &principal_id,
            "object-metadata",
        )),
        content_type: None,
        user_metadata_json: String::new(),
        storage_class: None,
    };
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                metadata,
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                b"public data".to_vec(),
            )),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client.put_object(put_req).await.unwrap();

    // Set bucket to public
    let mut public_req = Request::new(SetPublicAccessRequest {
        bucket: bucket_name.clone(),
        allow_public_read: true,
    });
    public_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    auth_client.set_public_access(public_req).await.unwrap();

    // Get object without auth
    let get_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,
        range: None,

        ..Default::default()
    });
    let mut get_req = get_req;
    get_req
        .metadata_mut()
        .insert("x-anvil-tenant-id", route_tenant_id.parse().unwrap());
    let _res = object_client.get_object(get_req).await.unwrap();

    // The bucket flag is not an authorisation bypass. Public reads must still
    // resolve through the generated Zanzibar tuple for the public principal.
    let public_bucket = cluster.states[0]
        .persistence
        .get_bucket_by_name(1, &bucket_name)
        .await
        .unwrap()
        .unwrap();
    assert!(public_bucket.is_public_read);
    anvil::access_control::write_bucket_public_read_tuple(
        &cluster.states[0].persistence,
        &public_bucket,
        false,
        "test",
        "prove public flag alone does not grant reads",
    )
    .await
    .unwrap();
    let mut tuple_removed_get = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,
        range: None,

        ..Default::default()
    });
    tuple_removed_get
        .metadata_mut()
        .insert("x-anvil-tenant-id", route_tenant_id.parse().unwrap());
    let tuple_removed = object_client.get_object(tuple_removed_get).await;
    assert_eq!(
        tuple_removed.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );

    anvil::access_control::write_bucket_public_read_tuple(
        &cluster.states[0].persistence,
        &public_bucket,
        true,
        "test",
        "restore public tuple for service flow",
    )
    .await
    .unwrap();
    let mut restored_public_get = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,
        range: None,

        ..Default::default()
    });
    restored_public_get
        .metadata_mut()
        .insert("x-anvil-tenant-id", route_tenant_id.parse().unwrap());
    object_client.get_object(restored_public_get).await.unwrap();

    // Set bucket to private
    let mut private_req = Request::new(SetPublicAccessRequest {
        bucket: bucket_name.clone(),
        allow_public_read: false,
    });
    private_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    auth_client.set_public_access(private_req).await.unwrap();

    // Get object without auth should now fail
    let get_req_2 = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,
        range: None,

        ..Default::default()
    });
    let mut get_req_2 = get_req_2;
    get_req_2
        .metadata_mut()
        .insert("x-anvil-tenant-id", route_tenant_id.parse().unwrap());
    let res_2 = object_client.get_object(get_req_2).await;
    assert!(res_2.is_err());
}

// This test stays in-process because it starts without a new token and restarts
// the cluster to verify rotated secrets survive local persistence.
#[tokio::test]
async fn test_reset_app_secret() {
    let mut cluster = isolated_test_cluster(
        "uses no-new-token startup and restarts the cluster after rotating a secret",
        &["eu-west-1"],
    )
    .await;
    cluster
        .start_and_converge_no_new_token(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT, false)
        .await;

    let app_name = unique_test_name("app-reset");

    // 1. Create an app and get original credentials
    let (client_id, original_secret) = create_app(&cluster, &app_name).await;

    // Grant it permissions and rotate the secret through the network admin API.
    grant_policy(&cluster, &app_name, "bucket:list", "buckets").await;
    let (_client_id, new_secret) = cluster
        .rotate_application_secret("default", &app_name)
        .await;

    // 3. Verify the secret has changed
    assert_ne!(original_secret, new_secret);

    // 4. Restart the cluster to ensure it picks up the new secret, clearing any cache.
    cluster.restart(Duration::from_secs(10)).await;

    // 5. Verify the NEW secret works against the restarted node
    let s3_client_new = cluster
        .get_s3_client("eu-west-1", &client_id, &new_secret)
        .await;
    match s3_client_new.list_buckets().send().await {
        Ok(_list_bucket_output) => {}
        Err(e) => {
            panic!("List buckets failed with the new secret: {:?}", e);
        }
    }

    // 6. Verify the OLD secret fails
    let s3_client_old = cluster
        .get_s3_client("eu-west-1", &client_id, &original_secret)
        .await;
    let list_buckets_old = s3_client_old.list_buckets().send().await;
    assert!(
        list_buckets_old.is_err(),
        "List buckets should fail with the old secret"
    );
}

#[tokio::test]
async fn test_service_set_public_access() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "service-public-access").await;

    let mut auth_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();

    let token = actor.token.clone();
    let principal_id = actor.app_id.clone();
    let bucket_name = unique_test_name("cli-public");
    let object_key = "cli-public-object".to_string();

    // 1. Create a bucket and upload an object to it.
    let mut create_bucket_req = tonic::Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: actor.region.clone(),

        options: None,
    });
    create_bucket_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let bucket_id = bucket_client
        .create_bucket(create_bucket_req)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let metadata = ObjectMetadata {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        mutation_context: Some(native_mutation_context(
            actor.tenant_id,
            bucket_id,
            &principal_id,
            "object-metadata",
        )),
        content_type: None,
        user_metadata_json: String::new(),
        storage_class: None,
    };
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                metadata,
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                b"public data from cli test".to_vec(),
            )),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client.put_object(put_req).await.unwrap();

    // 2. Verify the object is NOT public yet.
    let object_url = format!(
        "{}/{}/{}/{}",
        actor.grpc_addr,
        actor
            .tenant_name
            .as_deref()
            .expect("new Docker storage actor has a routeable tenant name"),
        bucket_name,
        object_key
    );
    let http_client = reqwest::Client::new();
    let resp_before = http_client
        .get(&object_url)
        .header("host", &cluster.public_region_host)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp_before.status(),
        403,
        "Object should be private initially"
    );

    // 3. Make the bucket public through the running service. The direct
    // storage-admin binary is an offline tool and correctly refuses to race an
    // active server's ownership fences.
    let mut set_public_req = tonic::Request::new(SetPublicAccessRequest {
        bucket: bucket_name.clone(),
        allow_public_read: true,
    });
    set_public_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    auth_client.set_public_access(set_public_req).await.unwrap();

    // 4. Verify the object IS public now, polling briefly for cache consistency.
    let mut resp_after = None;
    for _ in 0..20 {
        let resp = http_client
            .get(&object_url)
            .header("host", &cluster.public_region_host)
            .send()
            .await
            .unwrap();
        if resp.status() == 200 {
            resp_after = Some(resp);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let resp_after =
        resp_after.expect("Object should be public after CLI command, but never became public");

    assert_eq!(
        resp_after.status(),
        200,
        "Object should be public after CLI command"
    );
    let body = resp_after.text().await.unwrap();
    assert_eq!(body, "public data from cli test");
}
