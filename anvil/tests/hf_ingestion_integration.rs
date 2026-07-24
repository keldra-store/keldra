#![recursion_limit = "256"]

use anvil_test_utils::*;
use std::time::Duration;

fn authorized<T>(mut request: tonic::Request<T>, token: &str) -> tonic::Request<T> {
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

#[tokio::test]
async fn hf_keys_are_tenant_scoped() {
    let cluster = shared_docker_test_cluster().await;
    let first = create_docker_storage_test_actor(&cluster, "hf-key-first").await;
    let second = create_docker_storage_test_actor(&cluster, "hf-key-second").await;
    let key_name = unique_test_name("shared-hf-key");

    let mut client =
        anvil::anvil_api::hugging_face_key_service_client::HuggingFaceKeyServiceClient::connect(
            first.grpc_addr.clone(),
        )
        .await
        .unwrap();
    for (actor, note) in [(&first, "first tenant"), (&second, "second tenant")] {
        client
            .create_key(authorized(
                tonic::Request::new(anvil::anvil_api::CreateHfKeyRequest {
                    name: key_name.clone(),
                    token: format!("secret-{}", actor.tenant_id),
                    note: note.to_string(),
                }),
                &actor.token,
            ))
            .await
            .unwrap();
    }

    let first_keys = client
        .list_keys(authorized(
            tonic::Request::new(anvil::anvil_api::ListHfKeysRequest { page: None }),
            &first.token,
        ))
        .await
        .unwrap()
        .into_inner()
        .keys;
    let second_keys = client
        .list_keys(authorized(
            tonic::Request::new(anvil::anvil_api::ListHfKeysRequest { page: None }),
            &second.token,
        ))
        .await
        .unwrap()
        .into_inner()
        .keys;
    assert_eq!(
        first_keys
            .iter()
            .find(|key| key.name == key_name)
            .unwrap()
            .note,
        "first tenant"
    );
    assert_eq!(
        second_keys
            .iter()
            .find(|key| key.name == key_name)
            .unwrap()
            .note,
        "second tenant"
    );

    client
        .delete_key(authorized(
            tonic::Request::new(anvil::anvil_api::DeleteHfKeyRequest {
                name: key_name.clone(),
            }),
            &first.token,
        ))
        .await
        .unwrap();
    let second_keys = client
        .list_keys(authorized(
            tonic::Request::new(anvil::anvil_api::ListHfKeysRequest { page: None }),
            &second.token,
        ))
        .await
        .unwrap()
        .into_inner()
        .keys;
    assert!(second_keys.iter().any(|key| key.name == key_name));
}

#[tokio::test]
async fn hf_ingestion_single_file_integration() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "hf-ingestion").await;

    let token = actor.token.clone();

    // Create a bucket via gRPC
    let mut bucket_client = anvil::anvil_api::bucket_service_client::BucketServiceClient::connect(
        actor.grpc_addr.clone(),
    )
    .await
    .unwrap();
    let bucket_name = unique_test_name("models");
    let mut req = tonic::Request::new(anvil::anvil_api::CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: actor.region.clone(),

        options: None,
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(req).await.unwrap();

    // Create HF key with empty token (public repo)
    let mut key_client =
        anvil::anvil_api::hugging_face_key_service_client::HuggingFaceKeyServiceClient::connect(
            actor.grpc_addr.clone(),
        )
        .await
        .unwrap();
    let key_name = unique_test_name("hf-key");
    let mut kreq = tonic::Request::new(anvil::anvil_api::CreateHfKeyRequest {
        name: key_name.clone(),
        token: "".into(),
        note: "".into(),
    });
    kreq.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    key_client.create_key(kreq).await.unwrap();

    // Start ingestion for public config.json
    let mut ing_client =
        anvil::anvil_api::hf_ingestion_service_client::HfIngestionServiceClient::connect(
            actor.grpc_addr.clone(),
        )
        .await
        .unwrap();
    let mut sreq = tonic::Request::new(anvil::anvil_api::StartHfIngestionRequest {
        key_name,
        repo: "openai/gpt-oss-20b".into(),
        revision: "main".into(),
        target_bucket: bucket_name.clone(),
        target_region: actor.region.clone(),
        target_prefix: "gpt-oss-20b".into(),
        include_globs: vec!["config.json".into()],
        exclude_globs: vec![],
    });
    sreq.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let ing_id = ing_client
        .start_ingestion(sreq)
        .await
        .unwrap()
        .into_inner()
        .ingestion_id;

    // Poll status to completion
    let start = std::time::Instant::now();
    let mut attempts = 0_u64;
    loop {
        attempts += 1;
        if start.elapsed() > Duration::from_secs(120) {
            emit_test_timing(
                format!("hf_ingestion_status timeout attempts={attempts}"),
                start.elapsed(),
            );
            panic!("timeout waiting for ingestion");
        }
        let mut streq = tonic::Request::new(anvil::anvil_api::GetHfIngestionStatusRequest {
            ingestion_id: ing_id.clone(),
        });
        streq.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        let st = ing_client
            .get_ingestion_status(streq)
            .await
            .unwrap()
            .into_inner();
        if st.state == "completed" {
            emit_test_timing(
                format!("hf_ingestion_status completed attempts={attempts}"),
                start.elapsed(),
            );
            break;
        }
        if st.state == "failed" {
            emit_test_timing(
                format!("hf_ingestion_status failed attempts={attempts}"),
                start.elapsed(),
            );
            panic!("ingestion failed: {}", st.error);
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // Verify object is not public initially
    let http_base = actor.grpc_addr.trim_end_matches('/');
    let tenant_route = actor
        .tenant_name
        .as_deref()
        .expect("Docker storage actor includes tenant route name");
    let url = format!("{http_base}/{tenant_route}/{bucket_name}/gpt-oss-20b/config.json");
    let http_client = reqwest::Client::new();
    let resp_before = http_client
        .get(&url)
        .header(reqwest::header::HOST, &cluster.public_region_host)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp_before.status(),
        403,
        "Object should be private initially"
    );

    // Make the bucket public
    let mut auth_client =
        anvil::anvil_api::auth_service_client::AuthServiceClient::connect(actor.grpc_addr.clone())
            .await
            .unwrap();
    let mut req = tonic::Request::new(anvil::anvil_api::SetPublicAccessRequest {
        bucket: bucket_name.clone(),
        allow_public_read: true,
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    auth_client.set_public_access(req).await.unwrap();

    // Verify object is now public
    let mut resp_after = None;
    for _ in 0..5 {
        let resp = http_client
            .get(&url)
            .header(reqwest::header::HOST, &cluster.public_region_host)
            .send()
            .await
            .unwrap();
        if resp.status() == 200 {
            resp_after = Some(resp);
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let resp_after = resp_after.expect("Object should be public after setting policy");
    assert_eq!(
        resp_after.status(),
        200,
        "Object should be public after setting policy"
    );
    let txt = resp_after.text().await.unwrap();
    let v: serde_json::Value = serde_json::from_str(&txt).unwrap();
    assert!(v.is_object());
}

#[tokio::test]
async fn hf_ingestion_permission_denied() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "hf-ingestion-denied").await;

    let ok_token = actor.token.clone();

    // Create bucket
    let mut bucket_client = anvil::anvil_api::bucket_service_client::BucketServiceClient::connect(
        actor.grpc_addr.clone(),
    )
    .await
    .unwrap();
    let bucket_name = unique_test_name("models-denied");
    let mut req = tonic::Request::new(anvil::anvil_api::CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: actor.region.clone(),

        options: None,
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", ok_token).parse().unwrap(),
    );
    let _ = bucket_client.create_bucket(req).await;

    // Create key with auth ok
    let mut key_client =
        anvil::anvil_api::hugging_face_key_service_client::HuggingFaceKeyServiceClient::connect(
            actor.grpc_addr.clone(),
        )
        .await
        .unwrap();
    let key_name = unique_test_name("pd-test");
    let mut kreq = tonic::Request::new(anvil::anvil_api::CreateHfKeyRequest {
        name: key_name.clone(),
        token: "".into(),
        note: "".into(),
    });
    kreq.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", ok_token).parse().unwrap(),
    );
    key_client.create_key(kreq).await.unwrap();

    // Start ingestion with a token that lacks required scopes -> PermissionDenied
    let mut ing_client =
        anvil::anvil_api::hf_ingestion_service_client::HfIngestionServiceClient::connect(
            actor.grpc_addr.clone(),
        )
        .await
        .unwrap();
    let mut sreq = tonic::Request::new(anvil::anvil_api::StartHfIngestionRequest {
        key_name,
        repo: "openai/gpt-oss-20b".into(),
        revision: "main".into(),
        target_bucket: bucket_name,
        target_region: actor.region.clone(),
        target_prefix: "gpt-oss-20b".into(),
        include_globs: vec!["config.json".into()],
        exclude_globs: vec![],
    });
    // Create a same-tenant app with no HF ingestion grant.
    let limited_actor = cluster
        .create_actor_in_tenant(actor.tenant_id, "hf-ingestion-limited", &[])
        .await;
    sreq.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", limited_actor.token).parse().unwrap(),
    );
    let err = ing_client.start_ingestion(sreq).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}
