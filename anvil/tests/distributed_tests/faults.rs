use std::time::Duration;

use anvil_test_utils::{GrpcLostResponseProxy, isolated_docker_test_cluster};

use super::common::*;

#[tokio::test]
async fn docker_root_owner_failover_fences_stale_owner() {
    let cluster = isolated_docker_test_cluster("root-owner-failover", "test-region-1").await;
    let fixture = create_fixture(&cluster, "root-owner-failover").await;
    let peer_one = cluster.equal_peer(1);
    let peer_two = cluster.equal_peer(2);

    let baseline = put_and_observe(
        &peer_one.grpc_addr,
        &fixture,
        "owner-baseline",
        b"peer 1 established the initial root ownership epoch",
        &MutationIdentity::unique("owner-baseline"),
    )
    .await;
    cluster
        .wait_for_metadata_replica_convergence(
            &fixture.actor,
            &fixture.bucket_name,
            "owner-baseline",
            &baseline,
            DISTRIBUTED_WAIT,
        )
        .await;

    let partition = cluster.partition_equal_peers(&[1], &[2, 3, 4, 5, 6]).await;
    let failover = put_and_observe(
        &peer_two.grpc_addr,
        &fixture,
        "new-owner-write",
        b"peer 2 obtained a higher owner fence",
        &MutationIdentity::unique("new-owner-write"),
    )
    .await;
    let stale_identity = MutationIdentity::unique("stale-owner-write");
    let stale_failure = put_object_at(
        &peer_one.grpc_addr,
        &fixture,
        "stale-owner-write",
        b"must not publish from the isolated stale owner",
        &stale_identity,
    )
    .await
    .expect_err("isolated stale owner must not publish");
    assert_retryable_closed_failure(&stale_failure, "stale owner publication");
    partition.heal().await;

    cluster
        .wait_for_metadata_replica_convergence(
            &fixture.actor,
            &fixture.bucket_name,
            "new-owner-write",
            &failover,
            DISTRIBUTED_WAIT,
        )
        .await;
    cluster
        .wait_for_object_absent(
            &fixture.actor,
            &fixture.bucket_name,
            "stale-owner-write",
            &[1, 2, 3],
            DISTRIBUTED_WAIT,
        )
        .await;

    let recovered = put_and_observe(
        &peer_one.grpc_addr,
        &fixture,
        "stale-owner-write",
        b"must not publish from the isolated stale owner",
        &stale_identity,
    )
    .await;
    cluster
        .wait_for_metadata_replica_convergence(
            &fixture.actor,
            &fixture.bucket_name,
            "stale-owner-write",
            &recovered,
            DISTRIBUTED_WAIT,
        )
        .await;
    let versions = list_object_versions_at(&peer_one.grpc_addr, &fixture, "stale-owner-write")
        .await
        .expect("list stale-owner retry versions");
    assert_eq!(
        versions
            .versions
            .iter()
            .filter(|version| version.key == "stale-owner-write")
            .count(),
        1,
        "stale owner retry publishes exactly once after obtaining a current fence"
    );
}

#[tokio::test]
async fn docker_partition_heal_converges_without_split_brain() {
    let cluster = isolated_docker_test_cluster("partition-heal", "test-region-1").await;
    let fixture = create_fixture(&cluster, "partition-heal").await;
    let majority = cluster.equal_peer(1);
    let minority = cluster.equal_peer(3);
    let partition = cluster.partition_equal_peers(&[1, 2, 4, 5, 6], &[3]).await;

    let majority_key = "majority-commit";
    let majority_observation = put_and_observe(
        &majority.grpc_addr,
        &fixture,
        majority_key,
        b"the R3Q2 majority is the only side allowed to commit",
        &MutationIdentity::unique(majority_key),
    )
    .await;
    let minority_key = "minority-write";
    let minority_identity = MutationIdentity::unique(minority_key);
    let minority_failure = put_object_at(
        &minority.grpc_addr,
        &fixture,
        minority_key,
        b"must remain invisible",
        &minority_identity,
    )
    .await
    .expect_err("isolated R3 minority must not commit");
    assert_retryable_closed_failure(&minority_failure, "partition minority write");
    partition.heal().await;

    cluster
        .wait_for_metadata_replica_convergence(
            &fixture.actor,
            &fixture.bucket_name,
            majority_key,
            &majority_observation,
            DISTRIBUTED_WAIT,
        )
        .await;
    cluster
        .wait_for_object_absent(
            &fixture.actor,
            &fixture.bucket_name,
            minority_key,
            &[1, 2, 3],
            DISTRIBUTED_WAIT,
        )
        .await;

    let post_heal_key = "post-heal-commit";
    let post_heal = put_and_observe(
        &minority.grpc_addr,
        &fixture,
        post_heal_key,
        b"healed peer rejoins the one monotonic history",
        &MutationIdentity::unique(post_heal_key),
    )
    .await;
    cluster
        .wait_for_metadata_replica_convergence(
            &fixture.actor,
            &fixture.bucket_name,
            post_heal_key,
            &post_heal,
            DISTRIBUTED_WAIT,
        )
        .await;
}

#[tokio::test]
async fn docker_lost_ack_retry_commits_once() {
    let cluster = isolated_docker_test_cluster("lost-ack", "test-region-1").await;
    let fixture = create_fixture(&cluster, "lost-ack").await;
    let peer_one = cluster.equal_peer(1);
    let peer_two = cluster.equal_peer(2);
    let object_key = "lost-ack-object";
    let content = b"the first commit succeeds but its response is dropped";
    let identity = MutationIdentity::unique(object_key);
    let mut proxy = GrpcLostResponseProxy::start(&peer_one.grpc_addr).await;

    let first_failure = put_object_at(proxy.endpoint(), &fixture, object_key, content, &identity)
        .await
        .expect_err("the lost-response proxy returned an acknowledgement");
    proxy
        .wait_until_response_dropped(Duration::from_secs(15))
        .await
        .unwrap_or_else(|proxy_error| {
            panic!(
                "lost-response proxy did not reach the response DATA boundary: {proxy_error}; client failure: {first_failure:?}"
            )
        });

    let retry = put_object_at(
        &peer_two.grpc_addr,
        &fixture,
        object_key,
        content,
        &identity,
    )
    .await
    .expect("lost-ack retry through another equal peer");
    let observation =
        anvil_test_utils::DockerObjectObservation::from_put_response(&retry, content.len());
    cluster
        .wait_for_metadata_replica_convergence(
            &fixture.actor,
            &fixture.bucket_name,
            object_key,
            &observation,
            DISTRIBUTED_WAIT,
        )
        .await;

    let versions = list_object_versions_at(&peer_two.grpc_addr, &fixture, object_key)
        .await
        .expect("list versions after lost-ack retry");
    assert_eq!(
        versions
            .versions
            .iter()
            .filter(|version| version.key == object_key)
            .count(),
        1,
        "lost acknowledgement retry created exactly one object version"
    );
}
