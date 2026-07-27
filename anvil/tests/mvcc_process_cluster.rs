#![recursion_limit = "512"]

use std::{io::Write, path::PathBuf, time::Duration};

use anvil::anvil_api::{MvccReadConsistency, WriteState};
use anvil_test_utils::{GrpcLostResponseProxy, mvcc_process_cluster::ProcessMvccCluster};
use flate2::{Compression, write::ZlibEncoder};
use rusqlite::{Connection, session::Session};
use sha1::{Digest, Sha1};

const PERSONALDB_PROCESS_SCHEMA: &str = "CREATE TABLE items(
    id INTEGER PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    payload BLOB
);";

fn personaldb_insert_changeset() -> Vec<u8> {
    let database = Connection::open_in_memory().unwrap();
    database.execute_batch(PERSONALDB_PROCESS_SCHEMA).unwrap();
    let mut session = Session::new(&database).unwrap();
    session.attach::<&str>(None).unwrap();
    database
        .execute(
            "INSERT INTO items (id, name, payload) VALUES (1, 'process-crash', x'010203')",
            [],
        )
        .unwrap();
    let mut output = Vec::new();
    session.changeset_strm(&mut output).unwrap();
    assert!(!output.is_empty());
    output
}

fn process_git_pack() -> (String, Vec<u8>) {
    fn object_id(kind: &str, data: &[u8]) -> Vec<u8> {
        let mut hasher = Sha1::new();
        hasher.update(format!("{kind} {}\0", data.len()).as_bytes());
        hasher.update(data);
        hasher.finalize().to_vec()
    }
    fn append(pack: &mut Vec<u8>, kind: u8, data: &[u8]) {
        let mut size = data.len() as u64;
        let mut first = (kind << 4) | ((size as u8) & 0x0f);
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

    let blob = b"process crash recovery\n".to_vec();
    let blob_id = object_id("blob", &blob);
    let mut tree = Vec::new();
    tree.extend_from_slice(b"100644 README.md\0");
    tree.extend_from_slice(&blob_id);
    let tree_id = object_id("tree", &tree);
    let commit = format!(
        "tree {}\nauthor A <a@example.test> 0 +0000\ncommitter A <a@example.test> 0 +0000\n\nprocess\n",
        hex::encode(tree_id)
    )
    .into_bytes();
    let commit_id = object_id("commit", &commit);
    let objects = [(1_u8, commit), (2_u8, tree), (3_u8, blob)];
    let mut pack = Vec::new();
    pack.extend_from_slice(b"PACK");
    pack.extend_from_slice(&2_u32.to_be_bytes());
    pack.extend_from_slice(&(objects.len() as u32).to_be_bytes());
    for (kind, data) in objects {
        append(&mut pack, kind, &data);
    }
    let checksum = Sha1::digest(&pack);
    pack.extend_from_slice(&checksum);
    (hex::encode(commit_id), pack)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_commit_survives_lost_response_and_coordinator_sigkill() {
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_anvil-server"));
    let mut cluster = ProcessMvccCluster::start(binary).await.unwrap();
    let coordinator = cluster.wait_for_leader(&[0, 1, 2]).await.unwrap();
    let transaction = cluster
        .begin_transaction(coordinator, MvccReadConsistency::Linearized)
        .await
        .unwrap();

    let mut proxy = GrpcLostResponseProxy::start(&cluster.public_endpoint(coordinator)).await;
    let commit =
        cluster.commit_transaction(proxy.endpoint().to_string(), transaction.transaction_id);
    let (commit_result, dropped) = tokio::join!(
        commit,
        proxy.wait_until_response_dropped(Duration::from_secs(10))
    );
    assert!(
        commit_result.is_err(),
        "the commit acknowledgement must be lost"
    );
    dropped.unwrap();

    // The proxy only drops after the server has produced its unary response,
    // so this is the real after-proposal boundary.
    cluster.sigkill(coordinator).await.unwrap();
    let survivors = [0, 1, 2]
        .into_iter()
        .filter(|node| *node != coordinator)
        .collect::<Vec<_>>();
    let survivor = cluster.wait_for_leader(&survivors).await.unwrap();
    let survivor_snapshot = cluster
        .begin_transaction(survivor, MvccReadConsistency::Linearized)
        .await
        .unwrap();
    assert!(
        survivor_snapshot.snapshot_version > transaction.snapshot_version,
        "the surviving quorum must retain the acknowledged proposal"
    );

    cluster.restart(coordinator).await.unwrap();
    let restarted_snapshot = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Ok(snapshot) = cluster
                .begin_transaction(coordinator, MvccReadConsistency::LocalSnapshot)
                .await
            {
                if snapshot.snapshot_version >= survivor_snapshot.snapshot_version {
                    return snapshot;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("restarted coordinator catches up from its original RocksDB directory");
    assert_eq!(restarted_snapshot.state, "open");

    let follow_up = cluster
        .commit_transaction(
            cluster.public_endpoint(survivor),
            survivor_snapshot.transaction_id,
        )
        .await
        .unwrap();
    assert_eq!(follow_up.state, WriteState::Committed as i32);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_index_finalization_survives_two_coordinator_crashes_without_commit_retry() {
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_anvil-server"));
    let mut cluster = ProcessMvccCluster::start(binary).await.unwrap();
    let coordinator = cluster.wait_for_leader(&[0, 1, 2]).await.unwrap();
    let bucket_name = format!("index-finalization-{}", uuid::Uuid::new_v4().simple());
    let index_name = "committed-path";
    let bucket_id = cluster
        .create_bucket(coordinator, &bucket_name)
        .await
        .unwrap();
    let transaction = cluster
        .begin_transaction(coordinator, MvccReadConsistency::Linearized)
        .await
        .unwrap();
    let staged = cluster
        .stage_path_index(
            coordinator,
            &bucket_name,
            index_name,
            &transaction.transaction_id,
        )
        .await
        .unwrap();
    assert_eq!(staged.name, index_name);
    assert!(
        cluster
            .list_indexes(coordinator, &bucket_name)
            .await
            .unwrap()
            .is_empty(),
        "the staged definition must not publish before certification"
    );
    assert!(
        !cluster
            .index_creator_is_owner(coordinator, bucket_id, index_name)
            .await
            .unwrap(),
        "creator grants must not escape before certification"
    );
    assert_eq!(
        cluster
            .index_creator_owner_tuple_count(coordinator, bucket_id, index_name)
            .await
            .unwrap(),
        0
    );
    assert!(
        cluster
            .query_path_index(coordinator, &bucket_name, index_name)
            .await
            .is_err(),
        "no derived index build may exist before certification"
    );

    cluster
        .arm_hard_crash(coordinator, "IndexFinalizationBeforeExecute")
        .unwrap();
    let committed = cluster
        .commit_transaction(
            cluster.public_endpoint(coordinator),
            transaction.transaction_id,
        )
        .await
        .unwrap();
    assert_eq!(committed.state, WriteState::Committed as i32);
    cluster
        .wait_for_hard_crash(coordinator, Duration::from_secs(45))
        .await
        .unwrap();

    let survivors = [0, 1, 2]
        .into_iter()
        .filter(|node| *node != coordinator)
        .collect::<Vec<_>>();
    let survivor = cluster.wait_for_leader(&survivors).await.unwrap();
    let published = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Ok(indexes) = cluster.list_indexes(survivor, &bucket_name).await
                && indexes.iter().any(|index| {
                    index.index_id == staged.index_id && index.version == staged.version
                })
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        published.is_ok(),
        "the certified definition must survive the coordinator crash"
    );

    // Crash after the replay-safe effects but before marking the durable job
    // complete. The next same-disk restart must execute the same immutable job
    // again without duplicating the definition, grant, or build.
    cluster
        .arm_hard_crash(coordinator, "IndexFinalizationAfterExecute")
        .unwrap();
    cluster.restart(coordinator).await.unwrap();
    cluster
        .wait_for_hard_crash(coordinator, Duration::from_secs(45))
        .await
        .unwrap();
    cluster.restart(coordinator).await.unwrap();

    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let indexes = cluster
                .list_indexes(coordinator, &bucket_name)
                .await
                .unwrap_or_default();
            let exact = indexes
                .iter()
                .filter(|index| {
                    index.index_id == staged.index_id && index.version == staged.version
                })
                .count();
            let owner = cluster
                .index_creator_is_owner(coordinator, bucket_id, index_name)
                .await
                .unwrap_or(false);
            let owner_tuple_count = cluster
                .index_creator_owner_tuple_count(coordinator, bucket_id, index_name)
                .await
                .unwrap_or_default();
            let queryable = cluster
                .query_path_index(coordinator, &bucket_name, index_name)
                .await
                .is_ok();
            if exact == 1 && owner && owner_tuple_count == 1 && queryable {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("same-disk replay completes creator grant and exact-version build");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_personaldb_submit_survives_two_coordinator_crashes_without_client_retry() {
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_anvil-server"));
    let mut cluster = ProcessMvccCluster::start(binary).await.unwrap();
    let coordinator = cluster.wait_for_leader(&[0, 1, 2]).await.unwrap();
    let database_id = format!("personaldb-crash-{}", uuid::Uuid::new_v4().simple());
    cluster
        .create_personaldb_group(coordinator, &database_id, PERSONALDB_PROCESS_SCHEMA)
        .await
        .unwrap();
    let genesis_hash = hex::encode(anvil::formats::hash32(
        format!("genesis:{database_id}").as_bytes(),
    ));
    let transaction = cluster
        .begin_transaction(coordinator, MvccReadConsistency::Linearized)
        .await
        .unwrap();
    let staged = cluster
        .stage_personaldb_submit(
            coordinator,
            &database_id,
            &genesis_hash,
            personaldb_insert_changeset(),
            &transaction.transaction_id,
        )
        .await
        .unwrap();
    assert_eq!(staged.write_state, WriteState::Staged as i32);
    assert_eq!(
        cluster
            .get_personaldb_group(coordinator, &database_id)
            .await
            .unwrap()
            .committed_head
            .unwrap()
            .log_index,
        0,
        "the staged PersonalDB head must remain invisible before certification"
    );
    assert_eq!(
        cluster
            .personaldb_row_owner_tuple_count(coordinator, &database_id, "items", "1")
            .await
            .unwrap(),
        0,
        "postcommit grants must not escape before certification"
    );

    cluster
        .arm_hard_crash(coordinator, "PersonalDbPostCommitBeforeEffects")
        .unwrap();
    let committed = cluster
        .commit_transaction(
            cluster.public_endpoint(coordinator),
            transaction.transaction_id,
        )
        .await
        .unwrap();
    assert_eq!(committed.state, WriteState::Committed as i32);
    cluster
        .wait_for_hard_crash(coordinator, Duration::from_secs(45))
        .await
        .unwrap();

    let survivors = [0, 1, 2]
        .into_iter()
        .filter(|node| *node != coordinator)
        .collect::<Vec<_>>();
    let survivor = cluster.wait_for_leader(&survivors).await.unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if cluster
                .get_personaldb_group(survivor, &database_id)
                .await
                .ok()
                .and_then(|group| group.committed_head)
                .is_some_and(|head| head.log_index == 1)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the certified PersonalDB head survives the coordinator crash");

    // Replay the immutable job and crash after its idempotent effects but
    // before durable completion. A second same-disk restart must converge
    // without the client resubmitting either Submit or Commit.
    cluster
        .arm_hard_crash(coordinator, "PersonalDbPostCommitAfterEffects")
        .unwrap();
    cluster.restart(coordinator).await.unwrap();
    cluster
        .wait_for_hard_crash(coordinator, Duration::from_secs(45))
        .await
        .unwrap();
    cluster.restart(coordinator).await.unwrap();

    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let head_is_committed = cluster
                .get_personaldb_group(coordinator, &database_id)
                .await
                .ok()
                .and_then(|group| group.committed_head)
                .is_some_and(|head| head.log_index == 1);
            let owner_grants = cluster
                .personaldb_row_owner_tuple_count(coordinator, &database_id, "items", "1")
                .await
                .unwrap_or_default();
            if head_is_committed && owner_grants == 3 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("same-disk replay completes the exact PersonalDB row-owner grants");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_bucket_locator_survives_two_coordinator_crashes_without_client_retry() {
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_anvil-server"));
    let mut cluster = ProcessMvccCluster::start(binary).await.unwrap();
    let coordinator = cluster.wait_for_leader(&[0, 1, 2]).await.unwrap();
    let bucket_name = format!("bucket-locator-crash-{}", uuid::Uuid::new_v4().simple());
    let transaction = cluster
        .begin_transaction(coordinator, MvccReadConsistency::Linearized)
        .await
        .unwrap();
    cluster
        .stage_bucket_create(coordinator, &bucket_name, &transaction.transaction_id)
        .await
        .unwrap();
    assert_eq!(
        cluster
            .bucket_locator_record_count(coordinator, &bucket_name)
            .await
            .unwrap(),
        0,
        "a staged bucket must not publish its mesh locator"
    );

    cluster
        .arm_hard_crash(coordinator, "BucketLocatorFinalizationBeforeEffects")
        .unwrap();
    let committed = cluster
        .commit_transaction(
            cluster.public_endpoint(coordinator),
            transaction.transaction_id,
        )
        .await
        .unwrap();
    assert_eq!(committed.state, WriteState::Committed as i32);
    cluster
        .wait_for_hard_crash(coordinator, Duration::from_secs(45))
        .await
        .unwrap();

    cluster
        .arm_hard_crash(coordinator, "BucketLocatorFinalizationAfterEffects")
        .unwrap();
    cluster.restart(coordinator).await.unwrap();
    cluster
        .wait_for_hard_crash(coordinator, Duration::from_secs(45))
        .await
        .unwrap();
    cluster.restart(coordinator).await.unwrap();

    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if cluster
                .bucket_locator_record_count(coordinator, &bucket_name)
                .await
                .unwrap_or_default()
                == 1
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("same-disk replay publishes exactly one bucket locator");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_git_pack_survives_two_postcommit_crashes_without_client_retry_or_duplicate_watch()
 {
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_anvil-server"));
    let mut cluster = ProcessMvccCluster::start(binary).await.unwrap();
    let coordinator = cluster.wait_for_leader(&[0, 1, 2]).await.unwrap();
    cluster
        .bootstrap_object_placement(coordinator)
        .await
        .unwrap();
    let bucket_name = format!("git-crash-{}", uuid::Uuid::new_v4().simple());
    cluster
        .create_bucket(coordinator, &bucket_name)
        .await
        .unwrap();
    let repository_id = format!("repo-crash-{}", uuid::Uuid::new_v4().simple());
    let (commit_id, pack) = process_git_pack();
    let transaction = cluster
        .begin_transaction(coordinator, MvccReadConsistency::Linearized)
        .await
        .unwrap();
    let staged = cluster
        .stage_git_pack(
            coordinator,
            &repository_id,
            &bucket_name,
            pack,
            &transaction.transaction_id,
        )
        .await
        .unwrap();
    assert_eq!(staged.write_state, WriteState::Staged as i32);
    assert!(
        !cluster
            .object_exists(coordinator, &bucket_name, &staged.object_key)
            .await
            .unwrap(),
        "staged Git pack object must remain invisible before certification"
    );

    cluster
        .arm_hard_crash(coordinator, "GitSourcePostCommitBeforeEffects")
        .unwrap();
    let committed = cluster
        .commit_transaction(
            cluster.public_endpoint(coordinator),
            transaction.transaction_id,
        )
        .await
        .unwrap();
    assert_eq!(committed.state, WriteState::Committed as i32);
    cluster
        .wait_for_hard_crash(coordinator, Duration::from_secs(45))
        .await
        .unwrap();

    let survivors = [0, 1, 2]
        .into_iter()
        .filter(|node| *node != coordinator)
        .collect::<Vec<_>>();
    let survivor = cluster.wait_for_leader(&survivors).await.unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if cluster
                .object_exists(survivor, &bucket_name, &staged.object_key)
                .await
                .unwrap_or(false)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("certified Git pack object survives the coordinator crash");

    cluster
        .arm_hard_crash(coordinator, "GitSourcePostCommitAfterEffects")
        .unwrap();
    cluster.restart(coordinator).await.unwrap();
    cluster
        .wait_for_hard_crash(coordinator, Duration::from_secs(45))
        .await
        .unwrap();
    cluster.restart(coordinator).await.unwrap();

    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let events = cluster
                .git_source_watch_events(coordinator, &repository_id, Duration::from_millis(250))
                .await
                .unwrap_or_default();
            if events.len() == 1
                && events[0].generation == staged.generation
                && events[0].source_hash == staged.source_hash
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("same-disk replay materializes exactly one GitSource watch event");
    let blob = cluster
        .get_git_blob_by_path(coordinator, &repository_id, &commit_id, "README.md")
        .await
        .expect("same-disk replay materializes the GitSource index");
    assert_eq!(blob.pack_object_version_id, staged.version_id);
    let events = cluster
        .git_source_watch_events(coordinator, &repository_id, Duration::from_millis(750))
        .await
        .unwrap();
    assert_eq!(
        events.len(),
        1,
        "post-effect crash recovery must not duplicate GitSource watch publication"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_object_batch_recovers_after_leader_crashes_before_local_batch_write() {
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_anvil-server"));
    let mut cluster = ProcessMvccCluster::start(binary).await.unwrap();
    let leader = cluster.wait_for_leader(&[0, 1, 2]).await.unwrap();
    let bucket_name = format!("process-crash-{}", uuid::Uuid::new_v4().simple());
    let bucket_id = cluster.create_bucket(leader, &bucket_name).await.unwrap();
    let transaction = cluster
        .begin_transaction(leader, MvccReadConsistency::Linearized)
        .await
        .unwrap();
    let keys = ["crash-batch/one.bin", "crash-batch/two.bin"];
    let staged = cluster
        .stage_object_puts(
            leader,
            &bucket_name,
            bucket_id,
            &transaction.transaction_id,
            &[(keys[0], b"one"), (keys[1], b"two")],
        )
        .await
        .unwrap();
    assert_eq!(staged.write_state, WriteState::Staged as i32);
    for key in keys {
        assert!(
            !cluster
                .object_exists(leader, &bucket_name, key)
                .await
                .unwrap()
        );
    }

    cluster.arm_hard_crash(leader, "MvccBatchWrite").unwrap();
    let commit = cluster.commit_transaction(
        cluster.public_endpoint(leader),
        transaction.transaction_id.clone(),
    );
    let (commit_result, crash_result) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(10), commit),
        cluster.wait_for_hard_crash(leader, Duration::from_secs(10)),
    );
    crash_result.unwrap();
    assert!(
        !matches!(commit_result, Ok(Ok(_))),
        "a process abort before its local RocksDB batch must not return success"
    );

    let survivors = [0, 1, 2]
        .into_iter()
        .filter(|node| *node != leader)
        .collect::<Vec<_>>();
    let survivor = cluster.wait_for_leader(&survivors).await.unwrap();
    let stable = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Ok(status) = cluster
                .get_transaction(survivor, &transaction.transaction_id)
                .await
            {
                if status.state == "committed" {
                    return status;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("surviving quorum resolves the commit to one stable outcome");
    assert_eq!(stable.state, "committed");
    let retried = cluster
        .commit_transaction(
            cluster.public_endpoint(survivor),
            transaction.transaction_id.clone(),
        )
        .await
        .unwrap();
    assert_eq!(retried.state, WriteState::Committed as i32);
    for key in keys {
        assert!(
            cluster
                .object_exists(survivor, &bucket_name, key)
                .await
                .unwrap()
        );
    }

    cluster.restart(leader).await.unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let status = cluster
                .get_transaction(leader, &transaction.transaction_id)
                .await;
            let both_visible = matches!(
                cluster.object_exists(leader, &bucket_name, keys[0]).await,
                Ok(true)
            ) && matches!(
                cluster.object_exists(leader, &bucket_name, keys[1]).await,
                Ok(true)
            );
            if status.is_ok_and(|status| status.state == "committed") && both_visible {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("restarted original disk converges without partial object visibility");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_object_batch_retries_after_crash_before_prepared_bundle_sync() {
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_anvil-server"));
    let mut cluster = ProcessMvccCluster::start(binary).await.unwrap();
    let coordinator = cluster.wait_for_leader(&[0, 1, 2]).await.unwrap();
    let bucket_name = format!("process-prepare-{}", uuid::Uuid::new_v4().simple());
    let bucket_id = cluster
        .create_bucket(coordinator, &bucket_name)
        .await
        .unwrap();
    let transaction = cluster
        .begin_transaction(coordinator, MvccReadConsistency::Linearized)
        .await
        .unwrap();
    let keys = ["prepare-crash/one.bin", "prepare-crash/two.bin"];
    let staged = cluster
        .stage_object_puts(
            coordinator,
            &bucket_name,
            bucket_id,
            &transaction.transaction_id,
            &[(keys[0], b"one"), (keys[1], b"two")],
        )
        .await
        .unwrap();
    assert_eq!(staged.write_state, WriteState::Staged as i32);

    cluster
        .arm_hard_crash(coordinator, "PreparedBundleWrite")
        .unwrap();
    let commit = cluster.commit_transaction(
        cluster.public_endpoint(coordinator),
        transaction.transaction_id.clone(),
    );
    let (commit_result, crash_result) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(10), commit),
        cluster.wait_for_hard_crash(coordinator, Duration::from_secs(10)),
    );
    crash_result.unwrap();
    assert!(
        !matches!(commit_result, Ok(Ok(_))),
        "crashing before prepared-bundle sync must not return durability or commit success"
    );

    let survivors = [0, 1, 2]
        .into_iter()
        .filter(|node| *node != coordinator)
        .collect::<Vec<_>>();
    let survivor = cluster.wait_for_leader(&survivors).await.unwrap();
    for key in keys {
        assert!(
            !cluster
                .object_exists(survivor, &bucket_name, key)
                .await
                .unwrap(),
            "an unprepared bundle must not have any survivor-visible object"
        );
    }

    cluster.restart(coordinator).await.unwrap();
    let retry = cluster
        .commit_transaction(
            cluster.public_endpoint(coordinator),
            transaction.transaction_id.clone(),
        )
        .await
        .unwrap();
    assert_eq!(retry.state, WriteState::Committed as i32);
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let all_visible = matches!(
                cluster.object_exists(survivor, &bucket_name, keys[0]).await,
                Ok(true)
            ) && matches!(
                cluster.object_exists(survivor, &bucket_name, keys[1]).await,
                Ok(true)
            );
            if all_visible {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("retried durable bundle converges atomically after same-disk restart");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_object_batch_retries_after_crash_before_raft_wal_append() {
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_anvil-server"));
    let mut cluster = ProcessMvccCluster::start(binary).await.unwrap();
    let coordinator = cluster.wait_for_leader(&[0, 1, 2]).await.unwrap();
    let bucket_name = format!("process-raft-wal-{}", uuid::Uuid::new_v4().simple());
    let bucket_id = cluster
        .create_bucket(coordinator, &bucket_name)
        .await
        .unwrap();
    let transaction = cluster
        .begin_transaction(coordinator, MvccReadConsistency::Linearized)
        .await
        .unwrap();
    let keys = ["raft-wal-crash/one.bin", "raft-wal-crash/two.bin"];
    let staged = cluster
        .stage_object_puts(
            coordinator,
            &bucket_name,
            bucket_id,
            &transaction.transaction_id,
            &[(keys[0], b"one"), (keys[1], b"two")],
        )
        .await
        .unwrap();
    assert_eq!(staged.write_state, WriteState::Staged as i32);

    cluster.arm_hard_crash(coordinator, "RaftLogWrite").unwrap();
    let commit = cluster.commit_transaction(
        cluster.public_endpoint(coordinator),
        transaction.transaction_id.clone(),
    );
    let (commit_result, crash_result) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(10), commit),
        cluster.wait_for_hard_crash(coordinator, Duration::from_secs(10)),
    );
    crash_result.unwrap();
    assert!(
        !matches!(commit_result, Ok(Ok(_))),
        "crashing before the local Raft WAL append must not return commit success"
    );

    let survivors = [0, 1, 2]
        .into_iter()
        .filter(|node| *node != coordinator)
        .collect::<Vec<_>>();
    let survivor = cluster.wait_for_leader(&survivors).await.unwrap();
    for key in keys {
        assert!(
            !cluster
                .object_exists(survivor, &bucket_name, key)
                .await
                .unwrap(),
            "a proposal absent from the leader WAL must not leak partial visibility"
        );
    }

    cluster.restart(coordinator).await.unwrap();
    let retry = cluster
        .commit_transaction(
            cluster.public_endpoint(coordinator),
            transaction.transaction_id.clone(),
        )
        .await
        .unwrap();
    assert_eq!(retry.state, WriteState::Committed as i32);
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let all_visible = matches!(
                cluster.object_exists(survivor, &bucket_name, keys[0]).await,
                Ok(true)
            ) && matches!(
                cluster.object_exists(survivor, &bucket_name, keys[1]).await,
                Ok(true)
            );
            if all_visible {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("retried transaction converges atomically after Raft WAL crash recovery");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_erasure_object_retries_after_remote_shard_crash_before_sync() {
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_anvil-server"));
    let mut cluster = ProcessMvccCluster::start(binary).await.unwrap();
    let coordinator = cluster.wait_for_leader(&[0, 1, 2]).await.unwrap();
    cluster
        .bootstrap_object_placement(coordinator)
        .await
        .unwrap();
    let shard_target = (0..3).find(|node| *node != coordinator).unwrap();
    let bucket_name = format!("process-shard-{}", uuid::Uuid::new_v4().simple());
    let bucket_id = cluster
        .create_bucket(coordinator, &bucket_name)
        .await
        .unwrap();
    let transaction = cluster
        .begin_transaction_with_durability(
            coordinator,
            MvccReadConsistency::Linearized,
            anvil::anvil_api::MvccDurability::Erasure,
        )
        .await
        .unwrap();
    let object_key = "shard-crash/reconstruct.bin";
    let payload = vec![0x73_u8; 384 * 1024];

    cluster.arm_hard_crash(shard_target, "ShardWrite").unwrap();
    let stage = cluster.stage_object_puts(
        coordinator,
        &bucket_name,
        bucket_id,
        &transaction.transaction_id,
        &[(object_key, payload.as_slice())],
    );
    let (stage_result, crash_result) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(10), stage),
        cluster.wait_for_hard_crash(shard_target, Duration::from_secs(10)),
    );
    crash_result.unwrap();
    assert!(
        !matches!(stage_result, Ok(Ok(_))),
        "a shard target abort before sync must not produce a durable staging ACK"
    );
    assert!(
        !cluster
            .object_exists(coordinator, &bucket_name, object_key)
            .await
            .unwrap(),
        "failed erasure ingest must not become visible or commit"
    );

    cluster.restart(shard_target).await.unwrap();
    let staged = cluster
        .stage_object_puts(
            coordinator,
            &bucket_name,
            bucket_id,
            &transaction.transaction_id,
            &[(object_key, payload.as_slice())],
        )
        .await
        .unwrap();
    assert_eq!(staged.write_state, WriteState::Staged as i32);
    let committed = cluster
        .commit_transaction(
            cluster.public_endpoint(coordinator),
            transaction.transaction_id,
        )
        .await
        .unwrap();
    assert_eq!(committed.state, WriteState::Committed as i32);

    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if cluster
                .read_object(coordinator, &bucket_name, object_key)
                .await
                .is_ok_and(|bytes| bytes == payload)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("retried erasure object reconstructs exactly after shard target restart");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killed_node_is_replaced_by_higher_incarnation_and_catches_up() {
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_anvil-server"));
    let mut cluster = ProcessMvccCluster::start(binary).await.unwrap();
    let old_leader = cluster.wait_for_leader(&[0, 1, 2]).await.unwrap();
    let replaced = (0..3).find(|node| *node != old_leader).unwrap();
    cluster.sigkill(replaced).await.unwrap();

    let survivors = [0, 1, 2]
        .into_iter()
        .filter(|node| *node != replaced)
        .collect::<Vec<_>>();
    let leader = cluster.wait_for_leader(&survivors).await.unwrap();
    cluster.spawn_replacement(replaced, 2).await.unwrap();
    cluster
        .apply_replacement(leader, replaced, true)
        .await
        .unwrap();
    for survivor in survivors.iter().copied().filter(|node| *node != leader) {
        cluster
            .apply_replacement(survivor, replaced, false)
            .await
            .unwrap();
    }

    let before = cluster
        .begin_transaction(leader, MvccReadConsistency::Linearized)
        .await
        .unwrap();
    let committed = cluster
        .commit_transaction(cluster.public_endpoint(leader), before.transaction_id)
        .await
        .unwrap();
    assert_eq!(committed.state, WriteState::Committed as i32);
    let replacement = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Ok(snapshot) = cluster
                .begin_transaction(replaced, MvccReadConsistency::LocalSnapshot)
                .await
            {
                if snapshot.snapshot_version > before.snapshot_version {
                    return snapshot;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("higher incarnation catches up and serves its applied snapshot");
    assert_eq!(replacement.state, "open");

    let obsolete_endpoint = cluster.spawn_obsolete_incarnation(replaced).await.unwrap();
    let obsolete_local = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(snapshot) = cluster
                .begin_transaction_at(
                    obsolete_endpoint.clone(),
                    MvccReadConsistency::LocalSnapshot,
                )
                .await
            {
                return snapshot;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("obsolete process starts from its retired disk");
    assert!(obsolete_local.snapshot_version <= replacement.snapshot_version);
    let obsolete_attempt = tokio::time::timeout(
        Duration::from_secs(5),
        cluster.begin_transaction_at(obsolete_endpoint, MvccReadConsistency::Linearized),
    )
    .await;
    assert!(
        !matches!(obsolete_attempt, Ok(Ok(_))),
        "obsolete incarnation must not regain linearized consensus participation"
    );

    let healthy = cluster
        .begin_transaction(leader, MvccReadConsistency::Linearized)
        .await
        .unwrap();
    let healthy_commit = cluster
        .commit_transaction(cluster.public_endpoint(leader), healthy.transaction_id)
        .await
        .unwrap();
    assert_eq!(healthy_commit.state, WriteState::Committed as i32);
}
