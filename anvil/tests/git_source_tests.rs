#![recursion_limit = "256"]

use anvil::anvil_api::git_source_service_client::GitSourceServiceClient;
use anvil::anvil_api::transaction_service_client::TransactionServiceClient;
use anvil::anvil_api::{
    BeginTransactionRequest, CommitTransactionRequest, GetGitBlobByPathRequest,
    GetGitObjectRequest, GitPackMetadata, ListGitTreeRequest, MvccDurability, MvccReadConsistency,
    PutGitPackRequest, WatchGitSourceRequest, WriteOptions, WriteState, put_git_pack_request,
    write_options,
};
use anvil::formats::git::{GitHashAlgorithm, GitSourceRecord};
use anvil::git_source_index::{GitSourceIndexWrite, write_git_source_index};
use anvil::git_source_watch::{GitSourceWatchPayload, append_git_source_watch_record};
use anvil_test_utils::{
    ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT, TestCluster, isolated_test_cluster,
    shared_default_test_cluster, unique_test_name,
};
use flate2::{Compression, write::ZlibEncoder};
use futures_util::StreamExt;
use sha1::{Digest, Sha1};
use std::io::Write;
use std::time::Duration;
use tonic::Request;

#[tokio::test]
// Internal-only: seeds Git source watch records directly through cluster MVCC.
async fn test_git_source_watch_streams_snapshot_and_new_events() {
    let cluster = shared_default_test_cluster().await;
    let repository_id = unique_test_name("repo-alpha");

    append_git_source_watch_record(
        &cluster.states[0].mvcc,
        1,
        &repository_id,
        [1; 16],
        5,
        git_watch_payload(&repository_id, 1),
    )
    .await
    .unwrap();

    let mut client = GitSourceServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut watch_req = Request::new(WatchGitSourceRequest {
        repository_id: repository_id.clone(),
        after_cursor_low: 0,
        after_cursor_high: 0,
    });
    watch_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", cluster.token).parse().unwrap(),
    );
    let mut stream = client
        .watch_git_source(watch_req)
        .await
        .unwrap()
        .into_inner();

    let snapshot = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.cursor_low, 1);
    assert_eq!(snapshot.cursor_high, 0);
    assert_eq!(snapshot.repository_id, repository_id);
    assert_eq!(snapshot.event_type, "index_published");
    assert_eq!(snapshot.generation, 1);
    assert_eq!(snapshot.source_hash, hex::encode([1; 32]));
    assert_eq!(
        snapshot.pack_object_version_id,
        "00000000-0000-0000-0000-000000000001"
    );
    assert_eq!(snapshot.authz_revision, 5);
    let envelope = snapshot
        .envelope
        .as_ref()
        .expect("git source watch envelope");
    assert_eq!(envelope.watch_stream_id, "git_source");
    assert_eq!(envelope.partition_family, "git_source");
    assert_eq!(envelope.cursor_low, snapshot.cursor_low);
    assert_eq!(envelope.index_generation, snapshot.generation);
    assert_eq!(envelope.authz_revision, snapshot.authz_revision);
    assert_eq!(envelope.record_kind, "git_source");
    assert!(!envelope.payload_hash.is_empty());

    append_git_source_watch_record(
        &cluster.states[0].mvcc,
        1,
        &repository_id,
        [2; 16],
        6,
        git_watch_payload(&repository_id, 2),
    )
    .await
    .unwrap();
    let live = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(live.cursor_low, 2);
    assert_eq!(live.generation, 2);
    assert_eq!(live.authz_revision, 6);
}

#[tokio::test]
// Internal-only: writes the Git source index directly and mints a custom JWT.
async fn test_git_source_query_apis_use_latest_index_and_enforce_read_authz() {
    let cluster = shared_default_test_cluster().await;
    let repository_id = unique_test_name("repo-alpha");

    write_git_source_index(
        &cluster.states[0].storage,
        &cluster.states[0].mvcc,
        GitSourceIndexWrite {
            tenant_id: 1,
            repository_id: &repository_id,
            generation: 1,
            source_hash: [7; 32],
            hash_algorithm: GitHashAlgorithm::Sha1,
            records: &[
                git_record(&repository_id, 1, 10, "src/lib.rs", 100, 44),
                git_record(&repository_id, 1, 11, "src/main.rs", 200, 55),
                git_record(&repository_id, 1, 12, "README.md", 300, 66),
                git_record(&repository_id, 2, 13, "src/lib.rs", 400, 77),
            ],
        },
    )
    .await
    .unwrap();

    let mut client = GitSourceServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let blob = client
        .get_git_blob_by_path(authorized(
            GetGitBlobByPathRequest {
                repository_id: repository_id.clone(),
                commit_id: hex::encode([1_u8; 20]),
                tree_path: "/src/lib.rs".to_string(),
            },
            &cluster.token,
        ))
        .await
        .unwrap()
        .into_inner()
        .location
        .expect("blob location");
    assert_eq!(blob.object_id, hex::encode([10_u8; 20]));
    assert_eq!(blob.tree_path, "src/lib.rs");
    assert_eq!(blob.blob_start, 100);
    assert_eq!(blob.blob_len, 44);
    assert_eq!(
        blob.pack_object_version_id,
        "0a0a0a0a-0a0a-0a0a-0a0a-0a0a0a0a0a0a"
    );

    let tree = client
        .list_git_tree(authorized(
            ListGitTreeRequest {
                repository_id: repository_id.clone(),
                commit_id: hex::encode([1_u8; 20]),
                prefix: "src".to_string(),
                page: Some(anvil::anvil_api::PageRequest {
                    page_size: 10,
                    page_token: String::new(),
                }),
            },
            &cluster.token,
        ))
        .await
        .unwrap()
        .into_inner()
        .entries;
    assert_eq!(
        tree.iter()
            .map(|entry| entry.tree_path.as_str())
            .collect::<Vec<_>>(),
        vec!["src/lib.rs", "src/main.rs"]
    );

    let object_locations = client
        .get_git_object(authorized(
            GetGitObjectRequest {
                repository_id: repository_id.clone(),
                object_id: hex::encode([10_u8; 20]),
                page: None,
            },
            &cluster.token,
        ))
        .await
        .unwrap()
        .into_inner()
        .locations;
    assert_eq!(object_locations.len(), 1);
    assert_eq!(object_locations[0].commit_id, hex::encode([1_u8; 20]));

    let read_denied_token = cluster.states[0]
        .jwt_manager
        .mint_token("watch-only".to_string(), 1)
        .unwrap();
    let denied = client
        .get_git_object(authorized(
            GetGitObjectRequest {
                repository_id: repository_id.clone(),
                object_id: hex::encode([10_u8; 20]),
                page: None,
            },
            &read_denied_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
// Internal-only: verifies durable Git pack ingest and object readback on the
// single-node topology supported by 0.4.0. Git query/index freshness is a
// separate, explicitly unqualified capability in this release.
async fn test_put_git_pack_stores_normal_object_and_is_s3_readable() {
    let mut cluster = isolated_test_cluster(
        "git pack ingest on the 0.4.0 single-node topology",
        &["test-region-1"],
    )
    .await;
    cluster
        .start_and_converge(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT)
        .await;
    let repository_id = unique_test_name("repo-alpha");
    let bucket_name = unique_test_name("git-source-packs");
    cluster.create_bucket(&bucket_name, "test-region-1").await;

    let pack = minimal_git_pack();
    let mut client = GitSourceServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut request = Request::new(tokio_stream::iter(vec![
        PutGitPackRequest {
            data: Some(put_git_pack_request::Data::Metadata(GitPackMetadata {
                repository_id: repository_id.clone(),
                bucket_name: bucket_name.clone(),
                options: None,
            })),
        },
        PutGitPackRequest {
            data: Some(put_git_pack_request::Data::Chunk(pack.clone())),
        },
    ]));
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", cluster.token).parse().unwrap(),
    );
    let response = client.put_git_pack(request).await.unwrap().into_inner();
    assert_eq!(response.repository_id, repository_id);
    assert_eq!(response.bucket_name, bucket_name);
    assert!(
        response
            .object_key
            .starts_with(&format!("git-source/{repository_id}/packs/"))
    );
    assert_eq!(response.generation, 1);
    assert_eq!(
        response.source_hash,
        blake3::hash(&pack).to_hex().to_string()
    );
    assert_eq!(response.record_count, 1);
    assert_eq!(response.write_state, WriteState::Committed as i32);
    assert_eq!(response.watch_cursor_low, 0);
    assert_eq!(response.watch_cursor_high, 0);

    let s3 = cluster
        .get_s3_client("test-region-1", "test-app", "test-secret")
        .await;
    let got = s3
        .get_object()
        .bucket(&bucket_name)
        .key(&response.object_key)
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();
    assert_eq!(got.as_ref(), pack.as_slice());
}

fn authorized<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("valid token"),
    );
    request
}

async fn begin_git_transaction(
    cluster: &TestCluster,
    transactions: &mut TransactionServiceClient<tonic::transport::Channel>,
    label: &str,
) -> String {
    transactions
        .begin_transaction(authorized(
            BeginTransactionRequest {
                idempotency_key: format!("{label}-{}", uuid::Uuid::new_v4()),
                ttl_ms: 30_000,
                read_consistency: MvccReadConsistency::Linearized as i32,
                cluster_id: cluster.states[0].mvcc.cluster_id().to_string(),
                durability: MvccDurability::Quorum as i32,
            },
            &cluster.token,
        ))
        .await
        .unwrap()
        .into_inner()
        .transaction_id
}

fn git_transaction_options(transaction_id: &str) -> WriteOptions {
    WriteOptions {
        idempotency_key: uuid::Uuid::new_v4().to_string(),
        consistency: 0,
        wait_for_finalization: false,
        preconditions: Vec::new(),
        boundary_values: Vec::new(),
        execution: Some(write_options::Execution::TransactionId(
            transaction_id.to_string(),
        )),
    }
}

async fn stage_git_pack(
    client: &mut GitSourceServiceClient<tonic::transport::Channel>,
    token: &str,
    repository_id: &str,
    bucket_name: &str,
    transaction_id: &str,
    pack: Vec<u8>,
) -> anvil::anvil_api::PutGitPackResponse {
    let mut request = Request::new(tokio_stream::iter(vec![
        PutGitPackRequest {
            data: Some(put_git_pack_request::Data::Metadata(GitPackMetadata {
                repository_id: repository_id.to_string(),
                bucket_name: bucket_name.to_string(),
                options: Some(git_transaction_options(transaction_id)),
            })),
        },
        PutGitPackRequest {
            data: Some(put_git_pack_request::Data::Chunk(pack)),
        },
    ]));
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    client.put_git_pack(request).await.unwrap().into_inner()
}

async fn put_git_pack_implicitly(
    client: &mut GitSourceServiceClient<tonic::transport::Channel>,
    token: &str,
    repository_id: &str,
    bucket_name: &str,
    idempotency_key: &str,
    pack: Vec<u8>,
) -> anvil::anvil_api::PutGitPackResponse {
    let mut request = Request::new(tokio_stream::iter(vec![
        PutGitPackRequest {
            data: Some(put_git_pack_request::Data::Metadata(GitPackMetadata {
                repository_id: repository_id.to_string(),
                bucket_name: bucket_name.to_string(),
                options: Some(WriteOptions {
                    idempotency_key: idempotency_key.to_string(),
                    consistency: 0,
                    wait_for_finalization: false,
                    preconditions: Vec::new(),
                    boundary_values: Vec::new(),
                    execution: None,
                }),
            })),
        },
        PutGitPackRequest {
            data: Some(put_git_pack_request::Data::Chunk(pack)),
        },
    ]));
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    client.put_git_pack(request).await.unwrap().into_inner()
}

#[tokio::test]
async fn implicit_git_pack_retry_reconstructs_the_committed_outcome() {
    let mut cluster = isolated_test_cluster("git-source-implicit-retry", &["test-region-1"]).await;
    cluster
        .start_and_converge(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT)
        .await;
    let repository_id = unique_test_name("repo-retry");
    let bucket_name = unique_test_name("git-retry-packs");
    cluster.create_bucket(&bucket_name, "test-region-1").await;
    let pack = minimal_git_pack();
    let idempotency_key = uuid::Uuid::new_v4().to_string();
    let mut client = GitSourceServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();

    let first = put_git_pack_implicitly(
        &mut client,
        &cluster.token,
        &repository_id,
        &bucket_name,
        &idempotency_key,
        pack.clone(),
    )
    .await;
    let retry = put_git_pack_implicitly(
        &mut client,
        &cluster.token,
        &repository_id,
        &bucket_name,
        &idempotency_key,
        pack,
    )
    .await;

    assert_eq!(retry, first);
    assert_eq!(retry.write_state, WriteState::Committed as i32);
}

async fn commit_git_transaction(
    cluster: &TestCluster,
    transactions: &mut TransactionServiceClient<tonic::transport::Channel>,
    transaction_id: String,
) -> Result<(), tonic::Status> {
    transactions
        .commit_transaction(authorized(
            CommitTransactionRequest {
                transaction_id,
                cluster_id: cluster.states[0].mvcc.cluster_id().to_string(),
            },
            &cluster.token,
        ))
        .await
        .map(|_| ())
}

#[tokio::test]
async fn git_packs_and_manifests_commit_atomically_across_repositories() {
    let mut cluster =
        isolated_test_cluster("git-source-transaction-success", &["test-region-1"]).await;
    cluster
        .start_and_converge(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT)
        .await;
    let endpoint = cluster.grpc_addrs[0].clone();
    let bucket_name = unique_test_name("git-transaction-bucket");
    cluster.create_bucket(&bucket_name, "test-region-1").await;
    let mut git = GitSourceServiceClient::connect(endpoint.clone())
        .await
        .unwrap();
    let mut transactions = TransactionServiceClient::connect(endpoint).await.unwrap();
    let transaction_id = begin_git_transaction(&cluster, &mut transactions, "git-success").await;
    let first_repo = unique_test_name("git-first");
    let second_repo = unique_test_name("git-second");

    for repository_id in [&first_repo, &second_repo] {
        let staged = stage_git_pack(
            &mut git,
            &cluster.token,
            repository_id,
            &bucket_name,
            &transaction_id,
            minimal_git_pack(),
        )
        .await;
        assert_eq!(staged.write_state, WriteState::Staged as i32);
        assert!(
            anvil::git_source_manifest::read_git_source_repository_manifest(
                &cluster.states[0].mvcc,
                1,
                repository_id,
            )
            .await
            .unwrap()
            .is_none()
        );
    }

    commit_git_transaction(&cluster, &mut transactions, transaction_id)
        .await
        .unwrap();
    for repository_id in [&first_repo, &second_repo] {
        let manifest = anvil::git_source_manifest::read_git_source_repository_manifest(
            &cluster.states[0].mvcc,
            1,
            repository_id,
        )
        .await
        .unwrap()
        .expect("committed GitSource manifest");
        assert_eq!(manifest.generation, 1);
        assert_eq!(manifest.bucket_name, bucket_name);
    }
}

#[tokio::test]
async fn git_repository_conflict_aborts_every_manifest_and_pack_in_losing_transaction() {
    let mut cluster =
        isolated_test_cluster("git-source-transaction-conflict", &["test-region-1"]).await;
    cluster
        .start_and_converge(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT)
        .await;
    let endpoint = cluster.grpc_addrs[0].clone();
    let bucket_name = unique_test_name("git-conflict-bucket");
    cluster.create_bucket(&bucket_name, "test-region-1").await;
    let mut git = GitSourceServiceClient::connect(endpoint.clone())
        .await
        .unwrap();
    let mut transactions = TransactionServiceClient::connect(endpoint).await.unwrap();
    let first = begin_git_transaction(&cluster, &mut transactions, "git-first").await;
    let second = begin_git_transaction(&cluster, &mut transactions, "git-second").await;
    let shared_repo = unique_test_name("git-shared");
    let losing_repo = unique_test_name("git-losing");

    stage_git_pack(
        &mut git,
        &cluster.token,
        &shared_repo,
        &bucket_name,
        &first,
        minimal_git_pack(),
    )
    .await;
    for repository_id in [&shared_repo, &losing_repo] {
        stage_git_pack(
            &mut git,
            &cluster.token,
            repository_id,
            &bucket_name,
            &second,
            minimal_git_pack(),
        )
        .await;
    }
    commit_git_transaction(&cluster, &mut transactions, first)
        .await
        .unwrap();
    let conflict = commit_git_transaction(&cluster, &mut transactions, second)
        .await
        .unwrap_err();
    assert_eq!(conflict.code(), tonic::Code::Aborted);
    assert!(
        anvil::git_source_manifest::read_git_source_repository_manifest(
            &cluster.states[0].mvcc,
            1,
            &shared_repo,
        )
        .await
        .unwrap()
        .is_some()
    );
    assert!(
        anvil::git_source_manifest::read_git_source_repository_manifest(
            &cluster.states[0].mvcc,
            1,
            &losing_repo,
        )
        .await
        .unwrap()
        .is_none()
    );
}

#[derive(Debug, Clone, Copy)]
enum TestGitKind {
    Commit,
    Tree,
    Blob,
}

impl TestGitKind {
    fn name(self) -> &'static str {
        match self {
            Self::Commit => "commit",
            Self::Tree => "tree",
            Self::Blob => "blob",
        }
    }

    fn pack_kind(self) -> u8 {
        match self {
            Self::Commit => 1,
            Self::Tree => 2,
            Self::Blob => 3,
        }
    }
}

fn minimal_git_pack() -> Vec<u8> {
    let (_commit_id, pack) = minimal_git_pack_with_commit();
    pack
}

fn minimal_git_pack_with_commit() -> (Vec<u8>, Vec<u8>) {
    let blob = b"hello\n".to_vec();
    let blob_id = test_git_object_id(TestGitKind::Blob, &blob);
    let mut tree = Vec::new();
    tree.extend_from_slice(b"100644 README.md\0");
    tree.extend_from_slice(&blob_id);
    let tree_id = test_git_object_id(TestGitKind::Tree, &tree);
    let commit = format!(
        "tree {}\nauthor A <a@example.test> 0 +0000\ncommitter A <a@example.test> 0 +0000\n\ninitial\n",
        hex::encode(&tree_id)
    )
    .into_bytes();
    let commit_id = test_git_object_id(TestGitKind::Commit, &commit);
    let objects = vec![
        (TestGitKind::Commit, commit),
        (TestGitKind::Tree, tree),
        (TestGitKind::Blob, blob),
    ];
    let mut pack = Vec::new();
    pack.extend_from_slice(b"PACK");
    pack.extend_from_slice(&2_u32.to_be_bytes());
    pack.extend_from_slice(&(objects.len() as u32).to_be_bytes());
    for (kind, data) in objects {
        write_test_pack_object(&mut pack, kind, &data);
    }
    let mut hasher = Sha1::new();
    hasher.update(&pack);
    pack.extend_from_slice(&hasher.finalize());
    (commit_id, pack)
}

fn write_test_pack_object(pack: &mut Vec<u8>, kind: TestGitKind, data: &[u8]) {
    let mut size = data.len() as u64;
    let mut first = (kind.pack_kind() << 4) | ((size as u8) & 0x0f);
    size >>= 4;
    if size != 0 {
        first |= 0x80;
    }
    pack.push(first);
    while size != 0 {
        let mut byte = (size as u8) & 0x7f;
        size >>= 7;
        if size != 0 {
            byte |= 0x80;
        }
        pack.push(byte);
    }
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data).unwrap();
    pack.extend_from_slice(&encoder.finish().unwrap());
}

fn test_git_object_id(kind: TestGitKind, data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha1::new();
    hasher.update(format!("{} {}\0", kind.name(), data.len()).as_bytes());
    hasher.update(data);
    hasher.finalize().to_vec()
}

fn git_record(
    repository_id: &str,
    commit: u8,
    object: u8,
    path: &str,
    start: u64,
    len: u64,
) -> GitSourceRecord {
    GitSourceRecord::new(
        GitHashAlgorithm::Sha1,
        repository_id.as_bytes().to_vec(),
        vec![commit; 20],
        vec![object; 20],
        path.as_bytes().to_vec(),
        start,
        len,
        [object; 16],
    )
    .unwrap()
}

fn git_watch_payload(repository_id: &str, generation: u64) -> GitSourceWatchPayload {
    GitSourceWatchPayload {
        repository_id: repository_id.to_string(),
        event_type: "index_published".to_string(),
        generation,
        source_hash: hex::encode([generation as u8; 32]),
        index_path: format!(
            "_anvil/git/tenants/tenant-1/repositories/{repository_id}/indexes/generation-{generation:020}-source.angit"
        ),
        pack_object_version_id: Some("00000000-0000-0000-0000-000000000001".to_string()),
        emitted_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    }
}
