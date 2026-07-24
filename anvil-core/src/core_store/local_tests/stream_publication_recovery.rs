use super::super::local_root_publication_test_control::fail_publication_once;
use super::*;

fn direct_append(
    stream_id: &str,
    partition_id: &str,
    payload: &[u8],
    idempotency_key: &str,
) -> AppendStreamRecord {
    AppendStreamRecord {
        stream_id: stream_id.to_string(),
        partition_id: partition_id.to_string(),
        record_kind: "test.event".to_string(),
        payload: payload.to_vec(),
        content_type: Some("application/octet-stream".to_string()),
        user_metadata_json: "{}".to_string(),
        fence: None,
        transaction_id: None,
        idempotency_key: Some(idempotency_key.to_string()),
    }
}

async fn assert_distinct_append_recovers_unfinished_generation(
    stream_id: &str,
    partition_id: &str,
) {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
        .await
        .unwrap();
    let initial_sequence = store.stream_head_sequence(stream_id).await.unwrap();
    let first_sequence = initial_sequence.checked_add(1).unwrap();
    let second_sequence = first_sequence.checked_add(1).unwrap();
    let first_transaction_id =
        super::super::local_roots_layout::direct_stream_publication_transaction_id(
            stream_id,
            first_sequence,
            first_sequence,
        );
    let _stream_guard = store.acquire_named_lock("stream", stream_id).await.unwrap();

    fail_publication_once(&first_transaction_id);
    let error = store
        .append_stream_unlocked(direct_append(
            stream_id,
            partition_id,
            b"first",
            "first-event",
        ))
        .await
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("injected CoreMeta publication failure"),
        "unexpected append error: {error:#}"
    );
    assert!(
        store
            .read_root_publication_intent(&first_transaction_id)
            .unwrap()
            .is_some(),
        "the failed append must leave a durable publication intent"
    );
    assert_eq!(
        store.stream_head_sequence(stream_id).await.unwrap(),
        initial_sequence
    );

    let second = store
        .append_stream_unlocked(direct_append(
            stream_id,
            partition_id,
            b"second",
            "second-event",
        ))
        .await
        .unwrap();
    assert_eq!(second.receipt.sequence, second_sequence);
    assert!(!second.receipt.idempotent_replay);
    assert!(
        store
            .read_root_publication_intent(&first_transaction_id)
            .unwrap()
            .is_none(),
        "foreground recovery must clear the first publication intent"
    );

    let records = if stream_id == CORE_TRANSACTION_STREAM_ID {
        store.read_direct_stream_records(stream_id).await.unwrap()
    } else {
        store
            .read_stream(ReadStream {
                stream_id: stream_id.to_string(),
                after_sequence: initial_sequence,
                limit: 2,
            })
            .await
            .unwrap()
    };
    let recovered = records
        .iter()
        .find(|record| record.sequence == first_sequence)
        .expect("recovered first record");
    let appended = records
        .iter()
        .find(|record| record.sequence == second_sequence)
        .expect("new second record");
    assert_eq!(recovered.payload, b"first");
    assert_eq!(appended.payload, b"second");
}

#[tokio::test]
async fn distinct_append_recovers_the_publishers_unfinished_stream_generation() {
    assert_distinct_append_recovers_unfinished_generation(
        "test/direct-publication-recovery",
        "tenant:test/bucket:stream-recovery",
    )
    .await;
}

#[tokio::test]
async fn distinct_append_recovers_the_publishers_unfinished_control_generation() {
    assert_distinct_append_recovers_unfinished_generation(
        CORE_TRANSACTION_STREAM_ID,
        "system/core-control",
    )
    .await;
}

#[tokio::test]
async fn exact_retry_recovers_and_replays_the_unfinished_stream_generation() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
        .await
        .unwrap();
    let stream_id = "test/direct-publication-exact-retry";
    let transaction_id =
        super::super::local_roots_layout::direct_stream_publication_transaction_id(stream_id, 1, 1);
    let input = direct_append(
        stream_id,
        "tenant:test/bucket:stream-recovery",
        b"exact",
        "exact-event",
    );
    let _stream_guard = store.acquire_named_lock("stream", stream_id).await.unwrap();

    fail_publication_once(&transaction_id);
    store
        .append_stream_unlocked(input.clone())
        .await
        .unwrap_err();
    let replay = store.append_stream_unlocked(input).await.unwrap();

    assert_eq!(replay.receipt.sequence, 1);
    assert!(replay.receipt.idempotent_replay);
    assert_eq!(store.stream_head_sequence(stream_id).await.unwrap(), 1);
    assert!(
        store
            .read_root_publication_intent(&transaction_id)
            .unwrap()
            .is_none()
    );
}

async fn stage_failed_direct_append(
    store: &CoreStore,
    stream_id: &str,
    payload: &[u8],
    idempotency_key: &str,
) -> RootPublicationIntent {
    let transaction_id =
        super::super::local_roots_layout::direct_stream_publication_transaction_id(stream_id, 1, 1);
    let _stream_guard = store.acquire_named_lock("stream", stream_id).await.unwrap();
    fail_publication_once(&transaction_id);
    let error = store
        .append_stream_unlocked(direct_append(
            stream_id,
            "tenant:test/bucket:committed-candidate-replacement",
            payload,
            idempotency_key,
        ))
        .await
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("injected CoreMeta publication failure"),
        "unexpected append error: {error:#}"
    );
    store
        .read_root_publication_intent(&transaction_id)
        .unwrap()
        .expect("failed direct append publication intent")
}

#[tokio::test]
async fn committed_direct_stream_candidate_replaces_a_pending_loser() {
    let losing_tmp = tempfile::tempdir().unwrap();
    let losing_store = CoreStore::new(Storage::new_at(losing_tmp.path()).await.unwrap())
        .await
        .unwrap();
    let winning_tmp = tempfile::tempdir().unwrap();
    let winning_store = CoreStore::new(Storage::new_at(winning_tmp.path()).await.unwrap())
        .await
        .unwrap();
    let stream_id = "test/committed-direct-stream-candidate-replacement";

    let losing_intent =
        stage_failed_direct_append(&losing_store, stream_id, b"loser", "loser").await;
    let winning_intent =
        stage_failed_direct_append(&winning_store, stream_id, b"winner", "winner").await;
    assert_ne!(losing_intent.plan_hash, winning_intent.plan_hash);
    losing_store
        .mark_root_publication_intent_terminal(&losing_intent, "TransactionExpired")
        .unwrap();

    let publication_bundle = winning_store
        .encode_coremeta_recovery_publication_bundle(&winning_intent)
        .unwrap();
    let rows_by_root = winning_intent
        .roots
        .iter()
        .map(|root| {
            (
                root.publication.descriptor.root_key_hash(),
                root.rows.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let outcomes = winning_store
        .root_publication_outcomes(&winning_intent)
        .unwrap();
    let anchors = winning_store
        .publication_anchors(&winning_intent, &outcomes)
        .unwrap();
    let committed_anchors = anchors
        .iter()
        .map(|anchor| {
            (
                (anchor.root_key_hash.clone(), anchor.root_generation),
                encode_root_anchor_record(anchor).unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();

    losing_store
        .stage_committed_replica_root_publication_intent(
            &publication_bundle,
            &rows_by_root,
            &committed_anchors,
        )
        .unwrap();

    let stored = losing_store
        .read_root_publication_intent(&winning_intent.transaction_id)
        .unwrap()
        .expect("committed candidate publication intent");
    stored.ensure_pending().unwrap();
    assert!(publication_intent_retry_matches(&stored, &winning_intent).unwrap());
    assert_ne!(stored.plan_hash, losing_intent.plan_hash);
}

#[tokio::test]
async fn committed_same_plan_replaces_stale_recorded_outcomes() {
    let winning_tmp = tempfile::tempdir().unwrap();
    let winning_store = CoreStore::new(Storage::new_at(winning_tmp.path()).await.unwrap())
        .await
        .unwrap();
    let alternate_tmp = tempfile::tempdir().unwrap();
    let alternate_store = CoreStore::new(Storage::new_at(alternate_tmp.path()).await.unwrap())
        .await
        .unwrap();
    let recovering_tmp = tempfile::tempdir().unwrap();
    let recovering_store = CoreStore::new(Storage::new_at(recovering_tmp.path()).await.unwrap())
        .await
        .unwrap();
    let stream_id = "test/committed-same-plan-outcome-replacement";

    let winning_intent =
        stage_failed_direct_append(&winning_store, stream_id, b"winner", "winner").await;
    let alternate_intent =
        stage_failed_direct_append(&alternate_store, stream_id, b"alternate", "alternate").await;
    let mut stale_intent = winning_intent.clone();
    stale_intent.roots[0].certificate_hash = alternate_intent.roots[0].certificate_hash.clone();
    assert_ne!(
        stale_intent.roots[0].certificate_hash,
        winning_intent.roots[0].certificate_hash
    );
    recovering_store
        .persist_root_publication_intent(&stale_intent)
        .unwrap();
    recovering_store
        .mark_root_publication_intent_terminal(&stale_intent, "TransactionExpired")
        .unwrap();

    let publication_bundle = winning_store
        .encode_coremeta_recovery_publication_bundle(&winning_intent)
        .unwrap();
    let rows_by_root = winning_intent
        .roots
        .iter()
        .map(|root| {
            (
                root.publication.descriptor.root_key_hash(),
                root.rows.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let outcomes = winning_store
        .root_publication_outcomes(&winning_intent)
        .unwrap();
    let anchors = winning_store
        .publication_anchors(&winning_intent, &outcomes)
        .unwrap();
    let committed_anchors = anchors
        .iter()
        .map(|anchor| {
            (
                (anchor.root_key_hash.clone(), anchor.root_generation),
                encode_root_anchor_record(anchor).unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();

    recovering_store
        .stage_committed_replica_root_publication_intent(
            &publication_bundle,
            &rows_by_root,
            &committed_anchors,
        )
        .unwrap();

    let stored = recovering_store
        .read_root_publication_intent(&winning_intent.transaction_id)
        .unwrap()
        .expect("quorum-selected publication intent");
    stored.ensure_pending().unwrap();
    assert!(stored.no_outcomes_recorded());
    assert!(publication_intent_retry_matches(&stored, &winning_intent).unwrap());
}

#[tokio::test]
async fn committed_same_plan_reopens_a_terminal_intent_without_outcomes() {
    let winning_tmp = tempfile::tempdir().unwrap();
    let winning_store = CoreStore::new(Storage::new_at(winning_tmp.path()).await.unwrap())
        .await
        .unwrap();
    let recovering_tmp = tempfile::tempdir().unwrap();
    let recovering_store = CoreStore::new(Storage::new_at(recovering_tmp.path()).await.unwrap())
        .await
        .unwrap();
    let stream_id = "test/committed-terminal-same-plan-replacement";

    let winning_intent =
        stage_failed_direct_append(&winning_store, stream_id, b"winner", "winner").await;
    let publication_bundle = winning_store
        .encode_coremeta_recovery_publication_bundle(&winning_intent)
        .unwrap();
    let rows_by_root = winning_intent
        .roots
        .iter()
        .map(|root| {
            (
                root.publication.descriptor.root_key_hash(),
                root.rows.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let outcomes = winning_store
        .root_publication_outcomes(&winning_intent)
        .unwrap();
    let anchors = winning_store
        .publication_anchors(&winning_intent, &outcomes)
        .unwrap();
    let committed_anchors = anchors
        .iter()
        .map(|anchor| {
            (
                (anchor.root_key_hash.clone(), anchor.root_generation),
                encode_root_anchor_record(anchor).unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();

    recovering_store
        .stage_replica_root_publication_intent(&publication_bundle, &rows_by_root)
        .unwrap();
    let staged = recovering_store
        .read_root_publication_intent(&winning_intent.transaction_id)
        .unwrap()
        .expect("same-plan intent without outcomes");
    recovering_store
        .mark_root_publication_intent_terminal(&staged, "TransactionExpired")
        .unwrap();

    recovering_store
        .stage_committed_replica_root_publication_intent(
            &publication_bundle,
            &rows_by_root,
            &committed_anchors,
        )
        .unwrap();
    let stored = recovering_store
        .read_root_publication_intent(&winning_intent.transaction_id)
        .unwrap()
        .expect("committed same-plan intent");
    stored.ensure_pending().unwrap();
    assert!(stored.no_outcomes_recorded());
    assert!(publication_intent_retry_matches(&stored, &winning_intent).unwrap());
}
