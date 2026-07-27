#![recursion_limit = "512"]

use anvil::{
    mvcc_shard_repair::{
        MissingShardTarget, ShardMaintenanceKind, ShardRepairJob, ShardRepairState,
        resolve_manifest_at_snapshot,
    },
    mvcc_transaction::{CertificationResult, DurabilityLevel, LogicalKey, ReadConsistency},
    object_shard_manifest::PhysicalObjectShardManifest,
    shard_placement::{DistributedIngest, ShardPlacementPolicy},
    streaming_erasure::ErasureProfile,
};
use anvil_test_utils::mvcc_cluster::RealMvccCluster;
use tonic::Request;

async fn bootstrap_actor_on_every_node(
    cluster: &RealMvccCluster,
    bucket_name: &str,
) -> anvil_test_utils::mvcc_cluster::PublicActor {
    let actor = cluster
        .bootstrap_public_actor(0, bucket_name)
        .await
        .unwrap();
    for node in 1..3 {
        let state = cluster.state(node);
        state
            .persistence
            .create_region("e2e-region")
            .await
            .unwrap();
        let tenant = state
            .persistence
            .create_tenant("e2e-tenant", "e2e-tenant-key")
            .await
            .unwrap();
        let bucket = state
            .persistence
            .create_bucket(tenant.id, bucket_name, "e2e-region")
            .await
            .unwrap();
        assert_eq!(tenant.id, actor.tenant_id);
        assert_eq!(bucket.id, actor.bucket_id);
    }
    actor
}

async fn public_object_transaction(
    cluster: &RealMvccCluster,
    node: usize,
    actor: &anvil_test_utils::mvcc_cluster::PublicActor,
    id: &str,
    object_key: &str,
    payload: &[u8],
    durability: anvil::anvil_api::MvccDurability,
) -> anvil::anvil_api::WriteResponse {
    let endpoint = cluster.public_endpoint(node).to_string();
    let cluster_id = cluster.state(node).mvcc.cluster_id().to_string();
    let mut transactions =
        anvil::anvil_api::transaction_service_client::TransactionServiceClient::connect(
            endpoint.clone(),
        )
        .await
        .unwrap();
    let transaction = transactions
        .begin_transaction(authorized(
            anvil::anvil_api::BeginTransactionRequest {
                idempotency_key: format!("{id}-begin"),
                ttl_ms: 30_000,
                read_consistency: anvil::anvil_api::MvccReadConsistency::Linearized as i32,
                cluster_id: cluster_id.clone(),
                durability: durability as i32,
            },
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(transaction.durability, durability as i32);
    let mut objects =
        anvil::anvil_api::object_service_client::ObjectServiceClient::connect(endpoint)
            .await
            .unwrap();
    let staged = objects
        .mutation_batch(authorized(
            anvil::anvil_api::MutationBatchRequest {
                bucket_name: actor.bucket_name.clone(),
                mutation_context: Some(anvil::anvil_api::NativeMutationContext {
                    tenant_id: actor.tenant_id,
                    bucket_id: actor.bucket_id,
                    principal: actor.principal.clone(),
                    request_id: format!("{id}-write"),
                    precondition: String::new(),
                    authz_zookie_optional: String::new(),
                    idempotency_key: format!("{id}-write"),
                    transaction_id: Some(transaction.transaction_id.clone()),
                    write_visibility: None,
                }),
                precondition: None,
                operations: vec![anvil::anvil_api::MutationBatchOperation {
                    op: Some(anvil::anvil_api::mutation_batch_operation::Op::PutObject(
                        anvil::anvil_api::MutationBatchPutObject {
                            object_key: object_key.to_string(),
                            payload: payload.to_vec(),
                            content_type: Some("application/octet-stream".into()),
                            user_metadata_json: "{}".into(),
                            storage_class: None,
                        },
                    )),
                }],
            },
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(staged.write_state, anvil::anvil_api::WriteState::Staged as i32);
    transactions
        .commit_transaction(authorized(
            anvil::anvil_api::CommitTransactionRequest {
                transaction_id: transaction.transaction_id,
                cluster_id,
            },
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner()
}

async fn read_public_object(
    cluster: &RealMvccCluster,
    node: usize,
    actor: &anvil_test_utils::mvcc_cluster::PublicActor,
    object_key: &str,
) -> Result<Vec<u8>, tonic::Status> {
    let mut objects =
        anvil::anvil_api::object_service_client::ObjectServiceClient::connect(
            cluster.public_endpoint(node).to_string(),
        )
        .await
        .unwrap();
    let mut response = objects
        .get_object(authorized(
            anvil::anvil_api::GetObjectRequest {
                bucket_name: actor.bucket_name.clone(),
                object_key: object_key.to_string(),
                version_id: None,
                range: None,
                consistency: Some(anvil::anvil_api::ReadConsistency {
                    mode: Some(anvil::anvil_api::read_consistency::Mode::Latest(true)),
                }),
            },
            &actor.token,
        ))
        .await?
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(frame) = response.message().await? {
        if let Some(anvil::anvil_api::get_object_response::Data::Chunk(chunk)) = frame.data {
            bytes.extend(chunk);
        }
    }
    Ok(bytes)
}

async fn wait_for_public_object(
    cluster: &RealMvccCluster,
    node: usize,
    actor: &anvil_test_utils::mvcc_cluster::PublicActor,
    object_key: &str,
    expected: &[u8],
) {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            if read_public_object(cluster, node, actor, object_key)
                .await
                .is_ok_and(|bytes| bytes == expected)
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("object becomes readable from surviving node");
}

fn authorized<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn public_local_object_returns_locally_then_promotes_and_survives_holder_loss() {
    let mut cluster = RealMvccCluster::start().await.unwrap();
    let coordinator = cluster.wait_for_any_leader(&[0, 1, 2]).await.unwrap();
    let actor = bootstrap_actor_on_every_node(&cluster, "local-durability-e2e").await;
    let payload = vec![0x51_u8; 384 * 1024];

    let object_key = "durability/local".to_string();
    let response = public_object_transaction(
        &cluster,
        coordinator,
        &actor,
        "local-durability",
        &object_key,
        &payload,
        anvil::anvil_api::MvccDurability::Local,
    )
    .await;
    assert_eq!(response.state, anvil::anvil_api::WriteState::Committed as i32);
    assert_eq!(
        read_public_object(&cluster, coordinator, &actor, &object_key)
            .await
            .unwrap(),
        payload,
        "local durability returns with a readable local representation"
    );
    let endpoint = cluster.public_endpoint(coordinator).to_string();
    let mut objects =
        anvil::anvil_api::object_service_client::ObjectServiceClient::connect(endpoint)
            .await
            .unwrap();
    let automatic = objects
        .get_object_durability_promotion(authorized(
            anvil::anvil_api::GetObjectDurabilityPromotionRequest {
                bucket_name: actor.bucket_name.clone(),
                object_key: object_key.clone(),
                version_id: None,
            },
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!automatic.promotion_id.is_empty());
    assert_eq!(
        automatic.target_durability,
        anvil::anvil_api::MvccDurability::Erasure as i32
    );
    assert!(matches!(
        automatic.state.as_str(),
        "pending" | "running" | "complete"
    ));

    let explicit = objects
        .promote_object_durability(authorized(
            anvil::anvil_api::PromoteObjectDurabilityRequest {
                bucket_name: actor.bucket_name.clone(),
                object_key: object_key.clone(),
                version_id: None,
                target_durability: anvil::anvil_api::MvccDurability::Erasure as i32,
                idempotency_key: "explicit-local-promotion".into(),
            },
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(explicit.promotion_id, automatic.promotion_id);

    let status_node = (coordinator + 1) % 3;
    let mut remote_status =
        anvil::anvil_api::object_service_client::ObjectServiceClient::connect(
            cluster.public_endpoint(status_node).to_string(),
        )
        .await
        .unwrap();
    let initial_remote_status =
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            loop {
                if let Ok(response) = remote_status
                    .get_object_durability_promotion(authorized(
                        anvil::anvil_api::GetObjectDurabilityPromotionRequest {
                            bucket_name: actor.bucket_name.clone(),
                            object_key: object_key.clone(),
                            version_id: None,
                        },
                        &actor.token,
                    ))
                    .await
                {
                    return response.into_inner();
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("non-holder can query promotion status from replicated MVCC state");
    assert!(
        matches!(
            initial_remote_status.state.as_str(),
            "pending" | "running" | "complete"
        ),
        "non-holder returned an invalid promotion state"
    );
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let status = remote_status
                .get_object_durability_promotion(authorized(
                    anvil::anvil_api::GetObjectDurabilityPromotionRequest {
                        bucket_name: actor.bucket_name.clone(),
                        object_key: object_key.clone(),
                        version_id: None,
                    },
                    &actor.token,
                ))
                .await;
            if status.is_ok_and(|response| response.into_inner().state == "complete") {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("automatic/explicit local durability promotion completes");

    cluster.partition(coordinator);
    cluster
        .state(coordinator)
        .mvcc
        .consensus
        .shutdown()
        .await
        .unwrap();
    let survivors = [0, 1, 2]
        .into_iter()
        .filter(|node| *node != coordinator)
        .collect::<Vec<_>>();
    let survivor = cluster.wait_for_any_leader(&survivors).await.unwrap();
    let mut survivor_objects =
        anvil::anvil_api::object_service_client::ObjectServiceClient::connect(
            cluster.public_endpoint(survivor).to_string(),
        )
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let status = survivor_objects
                .get_object_durability_promotion(authorized(
                    anvil::anvil_api::GetObjectDurabilityPromotionRequest {
                        bucket_name: actor.bucket_name.clone(),
                        object_key: object_key.clone(),
                        version_id: None,
                    },
                    &actor.token,
                ))
                .await;
            if status.is_ok_and(|response| response.into_inner().state == "complete") {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("surviving node reports replicated promotion completion");
    wait_for_public_object(&cluster, survivor, &actor, &object_key, &payload).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn public_quorum_object_is_readable_after_one_node_loss() {
    let mut cluster = RealMvccCluster::start().await.unwrap();
    let coordinator = cluster.wait_for_any_leader(&[0, 1, 2]).await.unwrap();
    let actor = bootstrap_actor_on_every_node(&cluster, "quorum-durability-e2e").await;
    let payload = vec![0x62_u8; 384 * 1024];
    let response = public_object_transaction(
        &cluster,
        coordinator,
        &actor,
        "quorum-durability",
        "durability/quorum",
        &payload,
        anvil::anvil_api::MvccDurability::Quorum,
    )
    .await;
    assert_eq!(response.state, anvil::anvil_api::WriteState::Committed as i32);

    cluster.partition(coordinator);
    cluster
        .state(coordinator)
        .mvcc
        .consensus
        .shutdown()
        .await
        .unwrap();
    let survivors = [0, 1, 2]
        .into_iter()
        .filter(|node| *node != coordinator)
        .collect::<Vec<_>>();
    let survivor = cluster.wait_for_any_leader(&survivors).await.unwrap();
    wait_for_public_object(
        &cluster,
        survivor,
        &actor,
        "durability/quorum",
        &payload,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn public_erasure_object_is_readable_after_one_node_loss() {
    let mut cluster = RealMvccCluster::start().await.unwrap();
    let coordinator = cluster.wait_for_any_leader(&[0, 1, 2]).await.unwrap();
    let actor = bootstrap_actor_on_every_node(&cluster, "erasure-durability-e2e").await;
    let payload = vec![0x73_u8; 384 * 1024];
    let response = public_object_transaction(
        &cluster,
        coordinator,
        &actor,
        "erasure-durability",
        "durability/erasure",
        &payload,
        anvil::anvil_api::MvccDurability::Erasure,
    )
    .await;
    assert_eq!(response.state, anvil::anvil_api::WriteState::Committed as i32);

    cluster.partition(coordinator);
    cluster
        .state(coordinator)
        .mvcc
        .consensus
        .shutdown()
        .await
        .unwrap();
    let survivors = [0, 1, 2]
        .into_iter()
        .filter(|node| *node != coordinator)
        .collect::<Vec<_>>();
    let survivor = cluster.wait_for_any_leader(&survivors).await.unwrap();
    wait_for_public_object(
        &cluster,
        survivor,
        &actor,
        "durability/erasure",
        &payload,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn real_cluster_quorum_commit_is_readable_after_node_restart() {
    let mut cluster = RealMvccCluster::start().await.unwrap();
    let leader = cluster.wait_for_any_leader(&[0, 1, 2]).await.unwrap();
    let actor = cluster
        .bootstrap_public_actor(leader, "fixture-public-bucket")
        .await
        .unwrap();
    let endpoint = cluster.public_endpoint(leader).to_string();
    let cluster_id = cluster.state(leader).mvcc.cluster_id().to_string();
    let mut transactions =
        anvil::anvil_api::transaction_service_client::TransactionServiceClient::connect(
            endpoint.clone(),
        )
        .await
        .unwrap();
    let foreign = transactions
        .begin_transaction(authorized(
            anvil::anvil_api::BeginTransactionRequest {
                idempotency_key: "fixture-foreign-cluster".into(),
                ttl_ms: 30_000,
                read_consistency: anvil::anvil_api::MvccReadConsistency::Linearized as i32,
                cluster_id: "different-cluster".into(),
                durability: anvil::anvil_api::MvccDurability::Quorum as i32,
            },
            &actor.token,
        ))
        .await
        .unwrap_err();
    assert_eq!(foreign.code(), tonic::Code::FailedPrecondition);
    let transaction = transactions
        .begin_transaction(authorized(
            anvil::anvil_api::BeginTransactionRequest {
                idempotency_key: "fixture-public-transaction".into(),
                ttl_ms: 30_000,
                read_consistency: anvil::anvil_api::MvccReadConsistency::Linearized as i32,
                cluster_id: cluster_id.clone(),
                durability: anvil::anvil_api::MvccDurability::Quorum as i32,
            },
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner();
    let mut objects =
        anvil::anvil_api::object_service_client::ObjectServiceClient::connect(endpoint.clone())
            .await
            .unwrap();
    objects
        .mutation_batch(authorized(
            anvil::anvil_api::MutationBatchRequest {
                bucket_name: actor.bucket_name.clone(),
                mutation_context: Some(anvil::anvil_api::NativeMutationContext {
                    tenant_id: actor.tenant_id,
                    bucket_id: actor.bucket_id,
                    principal: actor.principal.clone(),
                    request_id: "fixture-public-write".into(),
                    precondition: String::new(),
                    authz_zookie_optional: String::new(),
                    idempotency_key: "fixture-public-write".into(),
                    transaction_id: Some(transaction.transaction_id.clone()),
                    write_visibility: None,
                }),
                precondition: None,
                operations: vec![anvil::anvil_api::MutationBatchOperation {
                    op: Some(anvil::anvil_api::mutation_batch_operation::Op::PutObject(
                        anvil::anvil_api::MutationBatchPutObject {
                            object_key: "fixture/smoke".into(),
                            payload: b"value".to_vec(),
                            content_type: Some("application/octet-stream".into()),
                            user_metadata_json: "{}".into(),
                            storage_class: None,
                        },
                    )),
                }],
            },
            &actor.token,
        ))
        .await
        .unwrap();
    transactions
        .commit_transaction(authorized(
            anvil::anvil_api::CommitTransactionRequest {
                transaction_id: transaction.transaction_id,
                cluster_id,
            },
            &actor.token,
        ))
        .await
        .unwrap();

    cluster.restart_node(leader).await.unwrap();
    let mut objects = anvil::anvil_api::object_service_client::ObjectServiceClient::connect(
        cluster.public_endpoint(leader).to_string(),
    )
    .await
    .unwrap();
    let mut response = objects
        .get_object(authorized(
            anvil::anvil_api::GetObjectRequest {
                bucket_name: actor.bucket_name,
                object_key: "fixture/smoke".into(),
                version_id: None,
                range: None,
                consistency: Some(anvil::anvil_api::ReadConsistency {
                    mode: Some(anvil::anvil_api::read_consistency::Mode::Latest(true)),
                }),
            },
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(frame) = response.message().await.unwrap() {
        if let Some(anvil::anvil_api::get_object_response::Data::Chunk(chunk)) = frame.data {
            bytes.extend(chunk);
        }
    }
    assert_eq!(bytes, b"value");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn real_cluster_elects_a_new_leader_and_catches_up_crashed_leader() {
    let mut cluster = RealMvccCluster::start().await.unwrap();
    let original_leader = cluster.wait_for_any_leader(&[0, 1, 2]).await.unwrap();
    let survivors = [0, 1, 2]
        .into_iter()
        .filter(|node| *node != original_leader)
        .collect::<Vec<_>>();

    cluster.partition(original_leader);
    cluster
        .state(original_leader)
        .mvcc
        .consensus
        .shutdown()
        .await
        .unwrap();
    let replacement_leader = cluster.wait_for_any_leader(&survivors).await.unwrap();
    let key = LogicalKey {
        table_id: 2,
        application_key: b"fixture/leader-recovery".to_vec(),
    };
    let outcome = cluster
        .commit(
            replacement_leader,
            "fixture-leader-recovery",
            key.clone(),
            b"committed-with-one-node-down".to_vec(),
        )
        .await
        .unwrap();
    let commit_version = match outcome.certification {
        CertificationResult::Committed { commit_version } => commit_version,
        CertificationResult::Aborted { reason } => {
            panic!("replacement leader transaction aborted: {reason:?}")
        }
    };

    cluster.restart_node(original_leader).await.unwrap();
    cluster
        .wait_for_applied_version(original_leader, commit_version)
        .await
        .unwrap();
    let row = cluster
        .state(original_leader)
        .mvcc
        .runtime
        .read_at(&key, commit_version)
        .unwrap()
        .expect("restarted former leader catches up the committed bundle");
    assert_eq!(row.value, b"committed-with-one-node-down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn real_cluster_reconstructs_a_deleted_shard_and_publishes_repaired_placement() {
    let cluster = RealMvccCluster::start().await.unwrap();
    let leader = cluster.wait_for_any_leader(&[0, 1, 2]).await.unwrap();
    let mvcc = &cluster.state(leader).mvcc;
    let (candidates, tolerated_failure_domains, _) = mvcc.live_shard_placement().unwrap();
    let profile = ErasureProfile {
        data_shards: 2,
        parity_shards: 1,
        shard_bytes: 64 * 1024,
    };
    let policy = ShardPlacementPolicy {
        tolerated_failure_domains,
    };
    let object_identity = uuid::Uuid::new_v4();
    let plan = policy
        .plan(object_identity, 1, profile, &candidates)
        .unwrap();
    let payload = vec![37_u8; 512 * 1024];
    let mut reader = payload.as_slice();
    let ingest = DistributedIngest::encode(
        &mvcc.replication_client,
        &plan,
        policy,
        profile,
        DurabilityLevel::Erasure,
        &mut reader,
        object_identity,
        None,
        1,
    )
    .await
    .unwrap();
    let manifest = PhysicalObjectShardManifest::from_ingest(
        mvcc.cluster_id(),
        object_identity,
        1,
        profile.data_shards,
        profile.parity_shards,
        profile.shard_bytes,
        &ingest,
    )
    .unwrap();
    let lost = manifest.placements[0].clone();
    let lost_node = cluster.node_index(&lost.node_id).unwrap();
    let lost_path = cluster.replication_transfer_path(lost_node, lost.transfer_id);
    assert!(lost_path.is_file());
    cluster
        .remove_replication_transfer(lost_node, lost.transfer_id)
        .unwrap();
    assert!(
        !lost_path.exists(),
        "the selected durable shard was removed"
    );

    let principal = "e2e-repair-producer";
    let handle = mvcc
        .open_transactions
        .begin(
            mvcc.runtime.as_ref(),
            mvcc.cluster_id().to_string(),
            principal,
            format!("repair-loss-{object_identity}"),
            std::time::Duration::from_secs(30),
            DurabilityLevel::Quorum,
            ReadConsistency::Linearized,
            10,
        )
        .await
        .unwrap();
    let target = plan.targets_by_ordinal[usize::from(lost.shard_ordinal)].clone();
    let job = ShardRepairJob {
        schema: ShardRepairJob::SCHEMA.to_string(),
        cluster_id: mvcc.cluster_id().to_string(),
        transaction_id: handle.transaction_id.clone(),
        kind: ShardMaintenanceKind::Repair,
        target_logical_identity: format!(
            "cluster/{}/object/{}",
            mvcc.cluster_id(),
            manifest.object_hash
        ),
        source_manifest: manifest.clone(),
        source_manifest_hash: hex::encode(
            blake3::hash(&manifest.canonical_bytes().unwrap()).as_bytes(),
        ),
        missing: vec![MissingShardTarget {
            stripe_ordinal: lost.stripe_ordinal,
            shard_ordinal: lost.shard_ordinal,
            target,
        }],
        retiring: Vec::new(),
        originating_snapshot_version: handle.snapshot_version,
        requested_at_unix_ms: 10,
    };
    let job_id = job.job_id().unwrap();
    mvcc.open_transactions
        .add_job(&handle.transaction_id, job.canonical_bytes().unwrap(), 11)
        .unwrap();
    let committed = mvcc
        .open_transactions
        .commit(mvcc.runtime.as_ref(), &handle.transaction_id, principal, 12)
        .await
        .unwrap();
    assert!(matches!(
        committed.certification,
        CertificationResult::Committed { .. }
    ));

    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let completed = (0..3).any(|node| {
                cluster
                    .state(node)
                    .mvcc
                    .runtime
                    .local_store()
                    .shard_repair_record(&job_id)
                    .ok()
                    .flatten()
                    .is_some_and(|record| record.state == ShardRepairState::Complete)
            });
            if completed && lost_path.is_file() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("durable shard repair completed");

    let snapshot = mvcc.runtime.local_store().readable_version().unwrap();
    let repaired =
        resolve_manifest_at_snapshot(mvcc.runtime.local_store(), &manifest, snapshot).unwrap();
    assert!(repaired.placements.iter().any(|placement| {
        placement.stripe_ordinal == lost.stripe_ordinal
            && placement.shard_ordinal == lost.shard_ordinal
            && placement.node_id == lost.node_id
            && placement.node_incarnation == lost.node_incarnation
    }));
    let reconstructed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    repaired
        .read_range_chunks(&mvcc.replication_client, 0, repaired.object_length, {
            let reconstructed = reconstructed.clone();
            move |chunk| {
                let reconstructed = reconstructed.clone();
                async move {
                    reconstructed.lock().unwrap().extend_from_slice(&chunk);
                    Ok(())
                }
            }
        })
        .await
        .unwrap();
    assert_eq!(*reconstructed.lock().unwrap(), payload);
}
