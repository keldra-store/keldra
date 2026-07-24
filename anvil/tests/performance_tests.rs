#![recursion_limit = "256"]

use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::index_service_client::IndexServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{
    self, CheckPermissionRequest, CreateBucketRequest, CreateIndexRequest, GetAccessTokenRequest,
    GetObjectRequest, IndexKind, ListAuthzObjectsRequest, ListObjectsRequest,
    NativeMutationContext, ObjectMetadata, PutObjectRequest, QueryIndexRequest, QueryIndexResponse,
    WriteAuthzTupleRequest,
};
use anvil_core::core_store::{
    AcquireFence, AppendStreamRecord, CF_INLINE_PAYLOADS, CoreMetaStore, CoreMetaTuplePart,
    CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition, CoreStore, GetBlob,
    PutBlob, ReadStream, ReleaseFence, TABLE_INLINE_PAYLOAD_ROW, core_meta_committed_row_common,
    core_meta_root_key_hash, core_meta_tuple_key, encode_core_meta_inline_payload_row,
};
use anvil_core::perf_baseline::{BaselineManifest, BaselineRunSummary, BaselineScenarioSummary};
use anvil_core::storage::Storage;
use anvil_test_utils::{
    ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT, emit_test_timing, isolated_test_cluster,
    shared_docker_test_cluster, unique_test_name,
};
use serde::Serialize;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tonic::Request;

fn perf_enabled() -> bool {
    std::env::var_os("ANVIL_RUN_PERF_TESTS").is_some()
}

fn docker_latency_enabled() -> bool {
    std::env::var_os("ANVIL_RUN_DOCKER_LATENCY").is_some()
}

fn authorized<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("valid token"),
    );
    request
}

fn native_mutation_context(bucket_id: i64, tag: &str) -> NativeMutationContext {
    let nonce = uuid::Uuid::new_v4();
    NativeMutationContext {
        tenant_id: 1,
        bucket_id,
        principal: "2".to_string(),
        request_id: format!("{tag}-{nonce}-request"),
        precondition: "none".to_string(),
        authz_zookie_optional: String::new(),
        idempotency_key: format!("{tag}-{nonce}-idempotency"),
        transaction_id: None,
        saga_operation: None,
        saga_compensation_operation: None,
        write_visibility: None,
    }
}

fn perf_coremeta_tuple_key(name: &str) -> Vec<u8> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("perf"),
        CoreMetaTuplePart::Utf8(name),
    ])
    .unwrap()
}

fn perf_coremeta_payload(name: &str, generation: u64) -> Vec<u8> {
    encode_core_meta_inline_payload_row(
        format!("perf-coremeta-{name}-{generation}").as_bytes(),
        core_meta_committed_row_common(
            "perf",
            core_meta_root_key_hash(&format!("perf/{name}")),
            generation,
            format!("perf-coremeta-{name}-{generation}"),
            generation,
        ),
    )
    .unwrap()
}

#[derive(Debug, Serialize)]
struct PerfSample {
    name: String,
    duration_ms: f64,
}

#[derive(Debug, Default)]
struct PerfReport {
    samples: Vec<PerfSample>,
    scenarios: Vec<BaselineScenarioSummary>,
    started_at: Option<Instant>,
}

impl PerfReport {
    async fn measure<F, Fut, T>(&mut self, name: &str, operation: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let started_at = Instant::now();
        let result = operation().await;
        let elapsed = started_at.elapsed();
        emit_test_timing(format!("perf.{name}"), elapsed);
        anvil::perf::record_duration("anvil_perf_case", &[("case", name)], elapsed);
        self.samples.push(PerfSample {
            name: name.to_string(),
            duration_ms: elapsed.as_secs_f64() * 1000.0,
        });
        self.scenarios
            .push(BaselineScenarioSummary::single_sample(name, elapsed));
        result
    }

    async fn write(&self) {
        let run_dir = std::env::var("ANVIL_PERF_RUN_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| workspace_perf_path(""));
        tokio::fs::create_dir_all(&run_dir).await.unwrap();

        let manifest = BaselineManifest::release_default();
        manifest
            .write_json_file(run_dir.join("baseline-manifest.json"))
            .unwrap();
        let elapsed = self
            .started_at
            .map(|started| started.elapsed())
            .unwrap_or_default();
        let summary = BaselineRunSummary::smoke(
            "cargo test -p anvil --test performance_tests -- --ignored-or-env-gated",
            &manifest,
            std::env::var("GITHUB_SHA")
                .or_else(|_| std::env::var("ANVIL_GIT_COMMIT"))
                .unwrap_or_else(|_| "local".to_string()),
            std::env::var("ANVIL_MACHINE_CLASS").unwrap_or_else(|_| "local-dev".to_string()),
            elapsed,
            self.scenarios.clone(),
        )
        .unwrap();
        tokio::fs::write(
            run_dir.join("performance-summary.json"),
            serde_json::to_vec_pretty(&summary).unwrap(),
        )
        .await
        .unwrap();
        tokio::fs::write(
            run_dir.join("release-gate-step.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema": "anvil.perf.release_gate_step.v1",
                "pass": summary.pass,
                "dataset_id": summary.dataset_id,
                "manifest_hash": summary.manifest_hash,
                "scenario_count": summary.scenarios.len()
            }))
            .unwrap(),
        )
        .await
        .unwrap();
        tokio::fs::write(
            run_dir.join("slow-spans.json"),
            serde_json::to_vec_pretty(&summary.slowest_traced_spans).unwrap(),
        )
        .await
        .unwrap();
        let mut line_protocol = String::new();
        for sample in &self.samples {
            line_protocol.push_str(&format!(
                "anvil_perf_case,case={} duration_ms={} {}\n",
                sample.name.replace(' ', "\\ "),
                sample.duration_ms,
                chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
            ));
        }
        tokio::fs::write(run_dir.join("anvil.line"), line_protocol)
            .await
            .unwrap();
        anvil::perf::flush().await;
        eprintln!("[perf] wrote {}", run_dir.display());
    }
}

#[tokio::test]
async fn performance_docker_end_user_flow() {
    if !docker_latency_enabled() {
        eprintln!("skipping performance_docker_end_user_flow; set ANVIL_RUN_DOCKER_LATENCY=1");
        return;
    }

    let mut report = PerfReport {
        started_at: Some(Instant::now()),
        ..PerfReport::default()
    };
    let cluster = shared_docker_test_cluster().await;
    let tenant_name = unique_test_name("perf-tenant");
    let tenant_id = report
        .measure("docker_create_tenant", || async {
            cluster.create_tenant(&tenant_name).await
        })
        .await;
    let app_name = unique_test_name("perf-app");
    let (app_id, client_id, client_secret) = report
        .measure("docker_create_application", || async {
            cluster
                .create_application_with_id(tenant_id, &app_name)
                .await
        })
        .await;
    let tenant_resource = format!("tenant:{tenant_id}");
    let mut policies = vec![("tenant:manage".to_string(), tenant_resource)];
    policies.extend(
        [
            "authz:tuple_write",
            "authz:tuple_read",
            "authz:check",
            "authz:watch",
            "authz:schema_read",
            "authz:schema_write",
        ]
        .into_iter()
        .map(|action| (action.to_string(), "default".to_string())),
    );
    report
        .measure("docker_grant_seven_policies", || async {
            cluster
                .grant_application_policies(tenant_id, &app_name, &policies)
                .await
        })
        .await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = report
        .measure("docker_get_access_token", || async {
            AuthServiceClient::connect(grpc_addr.clone())
                .await
                .unwrap()
                .get_access_token(GetAccessTokenRequest {
                    client_id: client_id.clone(),
                    client_secret: client_secret.clone(),
                })
                .await
                .unwrap()
                .into_inner()
                .access_token
        })
        .await;
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut auth_client = AuthServiceClient::connect(grpc_addr).await.unwrap();
    let bucket_name = unique_test_name("perf-bucket");
    let bucket_id = report
        .measure("docker_create_bucket", || async {
            bucket_client
                .create_bucket(authorized(
                    CreateBucketRequest {
                        bucket_name: bucket_name.clone(),
                        region: cluster.region.clone(),
                        options: None,
                    },
                    &token,
                ))
                .await
                .unwrap()
                .into_inner()
                .bucket_id
        })
        .await;

    let object_key = "latency/sample.txt".to_string();
    let payload = b"anvil docker latency sample".to_vec();
    report
        .measure("docker_put_27_byte_object", || async {
            let context = NativeMutationContext {
                tenant_id,
                bucket_id,
                principal: app_id.clone(),
                request_id: unique_test_name("perf-put-request"),
                precondition: "none".to_string(),
                authz_zookie_optional: String::new(),
                idempotency_key: unique_test_name("perf-put-idempotency"),
                transaction_id: None,
                saga_operation: None,
                saga_compensation_operation: None,
                write_visibility: None,
            };
            let metadata = PutObjectRequest {
                data: Some(anvil_api::put_object_request::Data::Metadata(
                    ObjectMetadata {
                        bucket_name: bucket_name.clone(),
                        object_key: object_key.clone(),
                        mutation_context: Some(context),
                        content_type: Some("text/plain".to_string()),
                        user_metadata_json: String::new(),
                        storage_class: None,
                    },
                )),
            };
            let chunk = PutObjectRequest {
                data: Some(anvil_api::put_object_request::Data::Chunk(payload.clone())),
            };
            object_client
                .put_object(authorized(
                    tokio_stream::iter(vec![metadata, chunk]),
                    &token,
                ))
                .await
                .unwrap();
        })
        .await;

    report
        .measure("docker_get_27_byte_object", || async {
            let mut stream = object_client
                .get_object(authorized(
                    GetObjectRequest {
                        bucket_name: bucket_name.clone(),
                        object_key: object_key.clone(),
                        ..Default::default()
                    },
                    &token,
                ))
                .await
                .unwrap()
                .into_inner();
            let mut downloaded = Vec::new();
            while let Some(message) = stream.message().await.unwrap() {
                if let Some(anvil_api::get_object_response::Data::Chunk(bytes)) = message.data {
                    downloaded.extend(bytes);
                }
            }
            assert_eq!(downloaded, payload);
        })
        .await;

    report
        .measure("docker_write_authz_tuple", || async {
            auth_client
                .write_authz_tuple(authorized(
                    WriteAuthzTupleRequest {
                        namespace: "document".to_string(),
                        object_id: "latency-object".to_string(),
                        relation: "viewer".to_string(),
                        subject_kind: "user".to_string(),
                        subject_id: "latency-user".to_string(),
                        caveat_hash: String::new(),
                        operation: "add".to_string(),
                        reason: "latency measurement".to_string(),
                        scope: None,
                    },
                    &token,
                ))
                .await
                .unwrap();
        })
        .await;
    report
        .measure("docker_check_permission", || async {
            let response = auth_client
                .check_permission(authorized(
                    CheckPermissionRequest {
                        namespace: "document".to_string(),
                        object_id: "latency-object".to_string(),
                        relation: "viewer".to_string(),
                        subject_kind: "user".to_string(),
                        subject_id: "latency-user".to_string(),
                        caveat_hash: String::new(),
                        consistency: "latest".to_string(),
                        zookie: String::new(),
                        scope: None,
                    },
                    &token,
                ))
                .await
                .unwrap()
                .into_inner();
            assert!(response.allowed);
        })
        .await;
    report
        .measure("docker_list_authz_objects", || async {
            let response = auth_client
                .list_authz_objects(authorized(
                    ListAuthzObjectsRequest {
                        namespace: "document".to_string(),
                        relation: "viewer".to_string(),
                        subject_kind: "user".to_string(),
                        subject_id: "latency-user".to_string(),
                        caveat_hash: String::new(),
                        consistency: "latest".to_string(),
                        zookie: String::new(),
                        page_size: 100,
                        page_token: String::new(),
                        scope: None,
                    },
                    &token,
                ))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(response.object_ids, vec!["latency-object"]);
        })
        .await;
    for name in ["docker_list_objects_cold", "docker_list_objects_warm"] {
        report
            .measure(name, || async {
                let response = object_client
                    .list_objects(authorized(
                        ListObjectsRequest {
                            bucket_name: bucket_name.clone(),
                            prefix: String::new(),
                            delimiter: String::new(),
                            start_after: String::new(),
                            max_keys: 100,
                            ..Default::default()
                        },
                        &token,
                    ))
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(response.objects.len(), 1);
            })
            .await;
    }

    eprintln!(
        "[perf-table] {}",
        serde_json::to_string_pretty(&report.samples).unwrap()
    );
    report.write().await;
}

fn workspace_perf_path(file_name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("target")
        .join("anvil")
        .join("perf")
        .join(file_name)
}

async fn put_json_object(
    object_client: &mut ObjectServiceClient<tonic::transport::Channel>,
    token: &str,
    bucket_id: i64,
    bucket_name: &str,
    key: &str,
    value: serde_json::Value,
    tag: &str,
) {
    let metadata = PutObjectRequest {
        data: Some(anvil_api::put_object_request::Data::Metadata(
            ObjectMetadata {
                bucket_name: bucket_name.to_string(),
                object_key: key.to_string(),
                mutation_context: Some(native_mutation_context(bucket_id, tag)),
                content_type: Some("application/json".to_string()),
                user_metadata_json: String::new(),
                storage_class: None,
            },
        )),
    };
    let chunk = PutObjectRequest {
        data: Some(anvil_api::put_object_request::Data::Chunk(
            serde_json::to_vec(&value).unwrap(),
        )),
    };
    object_client
        .put_object(authorized(tokio_stream::iter(vec![metadata, chunk]), token))
        .await
        .unwrap();
}

async fn query_index_until_hits(
    index_client: &mut IndexServiceClient<tonic::transport::Channel>,
    token: &str,
    request: QueryIndexRequest,
    expected_hits: usize,
    timeout: Duration,
) -> QueryIndexResponse {
    let started_at = Instant::now();
    let deadline = tokio::time::Instant::now() + timeout;
    let mut attempts = 0_u64;
    let mut last = None;
    while tokio::time::Instant::now() < deadline {
        attempts += 1;
        let response = match index_client
            .query_index(authorized(request.clone(), token))
            .await
        {
            Ok(response) => response.into_inner(),
            Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                tokio::time::sleep(Duration::from_millis(150)).await;
                continue;
            }
            Err(status) => panic!("query failed while waiting for index: {status:?}"),
        };
        if response.hits.len() == expected_hits && response.is_caught_up {
            let elapsed = started_at.elapsed();
            emit_test_timing(
                format!("perf.query_index_until_hits expected={expected_hits} attempts={attempts}"),
                elapsed,
            );
            anvil::perf::record_duration(
                "anvil_perf_case",
                &[("case", "query_index_until_hits")],
                elapsed,
            );
            return response;
        }
        last = Some(response);
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    last.unwrap_or_else(|| panic!("query did not execute before timeout"))
}

#[tokio::test]
async fn performance_native_api_smoke() {
    if !perf_enabled() {
        eprintln!("skipping performance_native_api_smoke; set ANVIL_RUN_PERF_TESTS=1");
        return;
    }

    let mut report = PerfReport::default();
    report.started_at = Some(Instant::now());

    let method_suite_started_at = Instant::now();
    {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();
        let payload = vec![42_u8; 64 * 1024];

        let object_ref = report
            .measure("corestore_put_blob_64k", || async {
                store
                    .put_blob(PutBlob {
                        logical_name: "perf/blob/64k".to_string(),
                        bytes: payload.clone(),
                        boundary_values: Vec::new(),
                        region_id: "perf-region-1".to_string(),
                        mutation_id: unique_test_name("perf-put-blob"),
                    })
                    .await
                    .unwrap()
            })
            .await;

        let fetched = report
            .measure("corestore_get_blob_64k", || async {
                store
                    .get_blob(GetBlob {
                        object_ref: object_ref.clone(),
                    })
                    .await
                    .unwrap()
            })
            .await;
        assert_eq!(fetched, payload);

        report
            .measure("corestore_append_stream_20", || async {
                for idx in 0..20 {
                    store
                        .append_stream(AppendStreamRecord {
                            stream_id: "perf/stream/events".to_string(),
                            partition_id: "perf".to_string(),
                            record_kind: "perf.event".to_string(),
                            payload: serde_json::to_vec(&serde_json::json!({
                                "idx": idx,
                                "payload": "method-level event"
                            }))
                            .unwrap(),
                            content_type: Some("application/json".to_string()),
                            user_metadata_json: "{}".to_string(),
                            fence: None,
                            transaction_id: None,
                            idempotency_key: Some(format!("perf-stream-{idx}")),
                        })
                        .await
                        .unwrap();
                }
            })
            .await;

        let stream_records = report
            .measure("corestore_read_stream_20", || async {
                store
                    .read_stream(ReadStream {
                        stream_id: "perf/stream/events".to_string(),
                        after_sequence: 0,
                        limit: 100,
                    })
                    .await
                    .unwrap()
            })
            .await;
        assert_eq!(stream_records.len(), 20);

        let coremeta_key = perf_coremeta_tuple_key("current");
        let coremeta_payload = perf_coremeta_payload("current", 1);
        let receipt = report
            .measure("corestore_coremeta_row_create", || async {
                store
                    .commit_mutation_batch(CoreMutationBatch {
                        transaction_id: unique_test_name("perf-coremeta"),
                        scope_partition: "perf".to_string(),
                        committed_by_principal: "perf-principal".to_string(),
                        root_publications: vec![],
                        preconditions: vec![CoreMutationPrecondition::CoreMetaRow {
                            cf: CF_INLINE_PAYLOADS.to_string(),
                            table_id: TABLE_INLINE_PAYLOAD_ROW,
                            tuple_key: coremeta_key.clone(),
                            expected_payload_hash: None,
                            require_absent: true,
                            require_present: false,
                        }],
                        operations: vec![CoreMutationOperation::CoreMetaPut {
                            partition_id: "perf".to_string(),
                            cf: CF_INLINE_PAYLOADS.to_string(),
                            table_id: TABLE_INLINE_PAYLOAD_ROW,
                            tuple_key: coremeta_key.clone(),
                            payload: coremeta_payload.clone(),
                        }],
                    })
                    .await
                    .unwrap()
            })
            .await;
        assert_eq!(receipt.visible_updates.len(), 1);
        assert!(
            CoreMetaStore::open(storage.core_store_meta_path())
                .unwrap()
                .get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &coremeta_key)
                .unwrap()
                .is_some()
        );

        let permit = report
            .measure("corestore_acquire_fence", || async {
                store
                    .acquire_fence(AcquireFence {
                        fence_name: "perf/fence/work".to_string(),
                        authenticated_principal: "perf-principal".to_string(),
                        ttl_ms: 30_000,
                    })
                    .await
                    .unwrap()
            })
            .await;

        let batch_coremeta_key = perf_coremeta_tuple_key("batch");
        let batch_coremeta_payload = perf_coremeta_payload("batch", 1);
        report
            .measure("corestore_mutation_batch_coremeta_and_stream", || async {
                store
                    .commit_mutation_batch(CoreMutationBatch {
                        transaction_id: unique_test_name("perf-batch"),
                        scope_partition: "perf".to_string(),
                        committed_by_principal: "perf-principal".to_string(),
                        root_publications: vec![],
                        preconditions: vec![CoreMutationPrecondition::CoreMetaRow {
                            cf: CF_INLINE_PAYLOADS.to_string(),
                            table_id: TABLE_INLINE_PAYLOAD_ROW,
                            tuple_key: batch_coremeta_key.clone(),
                            expected_payload_hash: None,
                            require_absent: true,
                            require_present: false,
                        }],
                        operations: vec![
                            CoreMutationOperation::CoreMetaPut {
                                partition_id: "perf".to_string(),
                                cf: CF_INLINE_PAYLOADS.to_string(),
                                table_id: TABLE_INLINE_PAYLOAD_ROW,
                                tuple_key: batch_coremeta_key.clone(),
                                payload: batch_coremeta_payload.clone(),
                            },
                            CoreMutationOperation::StreamAppend {
                                partition_id: "perf".to_string(),
                                stream_id: "perf/stream/batched".to_string(),
                                record_kind: "perf.batch".to_string(),
                                payload: br#"{"batched":true}"#.to_vec(),
                                idempotency_key: Some("perf-batch-stream-0".to_string()),
                            },
                        ],
                    })
                    .await
                    .unwrap()
            })
            .await;

        report
            .measure("corestore_release_fence", || async {
                store
                    .release_fence(ReleaseFence {
                        fence_name: permit.fence_name.clone(),
                        authenticated_principal: permit.owner_principal.clone(),
                        fence_token: permit.fence_token,
                    })
                    .await
                    .unwrap();
            })
            .await;
    }
    let method_suite_elapsed = method_suite_started_at.elapsed();
    emit_test_timing("perf.corestore_method_suite", method_suite_elapsed);
    anvil::perf::record_duration(
        "anvil_perf_case",
        &[("case", "corestore_method_suite")],
        method_suite_elapsed,
    );
    report.samples.push(PerfSample {
        name: "corestore_method_suite".to_string(),
        duration_ms: method_suite_elapsed.as_secs_f64() * 1000.0,
    });

    let mut cluster = report
        .measure("cluster_start_single_node", || async {
            let mut cluster = isolated_test_cluster(
                "performance benchmark measures single-node cluster startup and aborts nodes",
                &["perf-region-1"],
            )
            .await;
            cluster
                .start_and_converge(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT)
                .await;
            cluster
        })
        .await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = unique_test_name("perf-bucket");
    let bucket_id = report
        .measure("grpc_create_bucket", || async {
            bucket_client
                .create_bucket(authorized(
                    CreateBucketRequest {
                        bucket_name: bucket_name.clone(),
                        region: "perf-region-1".to_string(),

                        options: None,
                    },
                    &token,
                ))
                .await
                .unwrap()
                .into_inner()
                .bucket_id
        })
        .await;

    report
        .measure("grpc_put_json_objects_10", || async {
            for i in 0..10 {
                put_json_object(
                    &mut object_client,
                    &token,
                    bucket_id,
                    &bucket_name,
                    &format!("queue/item-{i:02}.json"),
                    serde_json::json!({"state": {
                        "queue_name": "outbound",
                        "state": if i % 2 == 0 {"pending"} else {"failed"},
                        "available_at": format!("2026-07-03T10:{i:02}:00Z"),
                        "priority": i,
                        "item_id": format!("item-{i:02}")
                    }}),
                    &format!("perf-put-{i}"),
                )
                .await;
            }
        })
        .await;

    report
        .measure("grpc_get_object", || async {
            let response = object_client
                .get_object(authorized(
                    GetObjectRequest {
                        bucket_name: bucket_name.clone(),
                        object_key: "queue/item-00.json".to_string(),
                        version_id: None,
                        range: None,

                        ..Default::default()
                    },
                    &token,
                ))
                .await
                .unwrap();
            let chunks = response.into_inner().message().await.unwrap();
            assert!(chunks.is_some());
        })
        .await;

    report
        .measure("grpc_list_objects", || async {
            let response = object_client
                .list_objects(authorized(
                    ListObjectsRequest {
                        bucket_name: bucket_name.clone(),
                        prefix: "queue/".to_string(),
                        delimiter: String::new(),
                        start_after: String::new(),
                        max_keys: 50,

                        ..Default::default()
                    },
                    &token,
                ))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(response.objects.len(), 10);
        })
        .await;

    report
        .measure("grpc_create_typed_json_index", || async {
            index_client
                .create_index(authorized(
                    CreateIndexRequest {
                        bucket_name: bucket_name.clone(),
                        name: "due-work".to_string(),
                        kind: IndexKind::TypedJson as i32,
                        selector_json: serde_json::json!({"prefix": "queue/"}).to_string(),
                        extractor_json: serde_json::json!({}).to_string(),
                        authorization_mode: "inherit_object".to_string(),
                        build_policy_json: serde_json::json!({
                            "source_kind": "object_current",
                            "fields": [
                                {"name": "queue_name", "extractor": "/state/queue_name", "required": true},
                                {"name": "state", "extractor": "/state/state", "required": true},
                                {"name": "available_at", "extractor": "/state/available_at", "required": true},
                                {"name": "priority", "extractor": "/state/priority", "required": true},
                                {"name": "item_id", "extractor": "/state/item_id", "required": true}
                            ],
                            "default_order": [
                                {"field": "available_at", "direction": "asc"},
                                {"field": "priority", "direction": "desc"},
                                {"field": "item_id", "direction": "asc"}
                            ]
                        })
                        .to_string(),

                        options: None,},
                    &token,
                ))
                .await
                .unwrap();
        })
        .await;

    let first_page = report
        .measure("grpc_query_typed_json_caught_up", || async {
            query_index_until_hits(
                &mut index_client,
                &token,
                QueryIndexRequest {
                    bucket_name: bucket_name.clone(),
                    index_name: "due-work".to_string(),
                    query_text: String::new(),
                    query_vector: vec![],
                    limit: 3,
                    phrase: false,
                    path_prefix: "queue/".to_string(),
                    metadata_filters_json: String::new(),
                    boundary_predicates_json: String::new(),
                    typed_predicates_json: serde_json::json!([
                        {"field": "queue_name", "op": "eq", "value": "outbound"},
                        {"field": "state", "op": "in", "values": ["pending", "failed"]},
                        {"field": "available_at", "op": "lte", "value": "2026-07-03T10:09:00Z"}
                    ])
                    .to_string(),
                    typed_order_json: String::new(),
                    page_token: String::new(),
                    require_caught_up_to_watch_cursor: String::new(),
                    lag_timeout_ms: 0,
                },
                3,
                Duration::from_secs(10),
            )
            .await
        })
        .await;
    assert_eq!(first_page.hits.len(), 3);

    report.write().await;
    for node in cluster.nodes.drain(..) {
        node.abort();
    }
}
