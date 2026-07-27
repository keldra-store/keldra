#![recursion_limit = "256"]

use std::time::{Duration, Instant};

use anvil_test_utils::{
    create_docker_storage_test_actor, emit_test_timing, shared_docker_test_cluster,
    unique_test_name,
};

fn authorized<T>(mut request: tonic::Request<T>, token: &str) -> tonic::Request<T> {
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

async fn get_public_text_with_retry(url: &str, host: &str, timeout: Duration) -> String {
    let start = Instant::now();
    let http = reqwest::Client::new();
    let mut last_error = "no attempts completed".to_string();
    loop {
        if start.elapsed() > timeout {
            panic!("timed out fetching {url}: {last_error}");
        }

        match http
            .get(url)
            .header(reqwest::header::HOST, host)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => match response.text().await {
                Ok(text) => return text,
                Err(error) => {
                    last_error = format!("failed to read response body: {error}");
                }
            },
            Ok(response) => {
                last_error = format!("unexpected HTTP status {}", response.status());
            }
            Err(error) => {
                last_error = format!("request failed: {error:?}");
            }
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_ingestion(
    client: &mut anvil::anvil_api::hf_ingestion_service_client::HfIngestionServiceClient<
        tonic::transport::Channel,
    >,
    token: &str,
    ingestion_id: &str,
    label: &str,
) {
    let start = Instant::now();
    let mut attempts = 0_u64;
    loop {
        attempts += 1;
        if start.elapsed() > Duration::from_secs(90) {
            emit_test_timing(
                format!("docker_hf_ingestion_status {label} timeout attempts={attempts}"),
                start.elapsed(),
            );
            panic!("timeout waiting for ingestion {label}");
        }
        let status = client
            .get_ingestion_status(authorized(
                tonic::Request::new(anvil::anvil_api::GetHfIngestionStatusRequest {
                    ingestion_id: ingestion_id.to_string(),
                }),
                token,
            ))
            .await
            .unwrap()
            .into_inner();
        if status.state == "completed" {
            emit_test_timing(
                format!("docker_hf_ingestion_status {label} completed attempts={attempts}"),
                start.elapsed(),
            );
            break;
        }
        if status.state == "failed" {
            emit_test_timing(
                format!("docker_hf_ingestion_status {label} failed attempts={attempts}"),
                start.elapsed(),
            );
            panic!("ingestion {label} failed: {}", status.error);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test]
async fn hf_ingestion_config_json() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "hf-e2e").await;
    let token = actor.token.clone();
    let bucket_name = unique_test_name("models");
    let key_name = unique_test_name("hf-key");

    let mut bucket_client = anvil::anvil_api::bucket_service_client::BucketServiceClient::connect(
        actor.grpc_addr.clone(),
    )
    .await
    .unwrap();
    bucket_client
        .create_bucket(authorized(
            tonic::Request::new(anvil::anvil_api::CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: actor.region.clone(),
                options: None,
            }),
            &token,
        ))
        .await
        .expect("create HF E2E bucket");

    let mut auth_client =
        anvil::anvil_api::auth_service_client::AuthServiceClient::connect(actor.grpc_addr.clone())
            .await
            .unwrap();
    auth_client
        .set_public_access(authorized(
            tonic::Request::new(anvil::anvil_api::SetPublicAccessRequest {
                context: None,
                bucket: bucket_name.clone(),
                allow_public_read: true,
            }),
            &token,
        ))
        .await
        .expect("make HF E2E bucket public");

    let mut key_client =
        anvil::anvil_api::hugging_face_key_service_client::HuggingFaceKeyServiceClient::connect(
            actor.grpc_addr.clone(),
        )
        .await
        .unwrap();
    key_client
        .create_key(authorized(
            tonic::Request::new(anvil::anvil_api::CreateHfKeyRequest {
                name: key_name.clone(),
                token: String::new(),
                note: String::new(),
                options: None,
            }),
            &token,
        ))
        .await
        .expect("create HF key");

    let mut ing_client =
        anvil::anvil_api::hf_ingestion_service_client::HfIngestionServiceClient::connect(
            actor.grpc_addr.clone(),
        )
        .await
        .unwrap();
    let ingestion_id = ing_client
        .start_ingestion(authorized(
            tonic::Request::new(anvil::anvil_api::StartHfIngestionRequest {
                key_name: key_name.clone(),
                repo: "openai/gpt-oss-20b".into(),
                revision: "main".into(),
                target_bucket: bucket_name.clone(),
                target_region: actor.region.clone(),
                target_prefix: "gpt-oss-20b".into(),
                include_globs: vec!["config.json".into()],
                exclude_globs: vec![],
                options: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .ingestion_id;
    wait_for_ingestion(&mut ing_client, &token, &ingestion_id, "config").await;

    let http_base = actor.grpc_addr.trim_end_matches('/');
    let tenant_route = actor
        .tenant_name
        .as_deref()
        .expect("Docker storage actor includes tenant route name");
    let config_url = format!("{http_base}/{tenant_route}/{bucket_name}/gpt-oss-20b/config.json");
    let txt = get_public_text_with_retry(
        &config_url,
        &cluster.public_region_host,
        Duration::from_secs(60),
    )
    .await;
    let v: serde_json::Value = serde_json::from_str(&txt).unwrap();
    assert!(v.is_object());

    let index_url =
        format!("{http_base}/{tenant_route}/{bucket_name}/gpt-oss-20b/anvil-index.json");
    let index_txt = get_public_text_with_retry(
        &index_url,
        &cluster.public_region_host,
        Duration::from_secs(60),
    )
    .await;
    let index_v: serde_json::Value = serde_json::from_str(&index_txt).unwrap();

    assert_eq!(index_v["meta"]["source_repo"], "openai/gpt-oss-20b");
    assert_eq!(index_v["meta"]["revision"], "main");
    assert_eq!(index_v["meta"]["total_files"], 1);
    assert!(index_v["meta"]["total_bytes"].is_number());
    assert!(index_v["meta"]["generated_at"].is_string());

    let files_map = index_v["files"].as_object().unwrap();
    assert_eq!(files_map.len(), 1);
    assert!(files_map.contains_key("config.json"));

    let config_json_entry = files_map["config.json"].as_object().unwrap();
    assert!(config_json_entry["size"].is_number());
    assert!(config_json_entry["etag"].is_string());
    assert!(config_json_entry["last_modified"].is_string());
    assert_eq!(
        config_json_entry["size"].as_i64().unwrap(),
        txt.len() as i64
    );

    let second_ingestion_id = ing_client
        .start_ingestion(authorized(
            tonic::Request::new(anvil::anvil_api::StartHfIngestionRequest {
                key_name,
                repo: "openai/gpt-oss-20b".into(),
                revision: "main".into(),
                target_bucket: bucket_name,
                target_region: actor.region.clone(),
                target_prefix: "gpt-oss-20b".into(),
                include_globs: vec!["README.md".into()],
                exclude_globs: vec![],
                options: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .ingestion_id;
    wait_for_ingestion(&mut ing_client, &token, &second_ingestion_id, "readme").await;

    let index_txt_2 = get_public_text_with_retry(
        &index_url,
        &cluster.public_region_host,
        Duration::from_secs(60),
    )
    .await;
    let index_v_2: serde_json::Value = serde_json::from_str(&index_txt_2).unwrap();
    assert_eq!(
        index_v_2["meta"]["total_files"], 2,
        "Index should contain 2 files after merge (found: {:?})",
        index_v_2["files"]
    );

    let files_map_2 = index_v_2["files"].as_object().unwrap();
    assert!(files_map_2.contains_key("config.json"));
    assert!(files_map_2.contains_key("README.md"));
}
