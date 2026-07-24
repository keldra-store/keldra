use super::*;

#[tokio::test]
async fn authz_tuple_write_latency_with_retained_history_perf() {
    if std::env::var_os("ANVIL_RUN_AUTHZ_PERF").is_none() {
        return;
    }
    let retained: usize = std::env::var("ANVIL_AUTHZ_PERF_SEED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(200);
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let measure_materialization = std::env::var_os("ANVIL_AUTHZ_PERF_MATERIALIZE").is_some();

    let seed_started = std::time::Instant::now();
    for revision in 1..=retained {
        append_authz_record_without_segment(
            &storage,
            &tuple(
                revision as i64,
                "document",
                &format!("seed-{revision:06}"),
                "viewer",
                "user",
                "alice",
                "add",
            ),
        )
        .await
        .unwrap();
        if measure_materialization && revision == 1 {
            materialize_authz_derived_state_at_revision(&storage, 42, 1, 0)
                .await
                .unwrap();
        }
    }
    let seed_elapsed = seed_started.elapsed();

    let write_started = std::time::Instant::now();
    test_append_authz_tuple_record_unfenced(
        &storage,
        &tuple(
            retained as i64 + 1,
            "document",
            "measured",
            "viewer",
            "user",
            "alice",
            "add",
        ),
    )
    .await
    .unwrap();
    let write_elapsed = write_started.elapsed();

    if measure_materialization {
        let materialize_started = std::time::Instant::now();
        let fence = latest_authz_journal_fence_token(&storage, 42)
            .await
            .unwrap();
        let outcome =
            materialize_authz_derived_state_at_revision(&storage, 42, retained as u64 + 1, fence)
                .await
                .unwrap();
        assert_eq!(outcome.source_rows_visited, 1);
        eprintln!(
            "[authz-perf] materialize_ms={} processed_revision={}",
            materialize_started.elapsed().as_millis(),
            outcome.processed_revision,
        );
    }

    let mut check_elapsed_ms = Vec::new();
    for _ in 0..10 {
        let check_started = std::time::Instant::now();
        let allowed = resolve_permission_at_revision(
            &storage,
            42,
            "document",
            "measured",
            "viewer",
            "user",
            "alice",
            "",
            retained as i64 + 1,
        )
        .await
        .unwrap();
        check_elapsed_ms.push(check_started.elapsed().as_millis());
        assert!(allowed);
    }

    eprintln!(
        "[authz-perf] retained={retained} seed_ms={} measured_write_ms={} check_ms={:?}",
        seed_elapsed.as_millis(),
        write_elapsed.as_millis(),
        check_elapsed_ms
    );
}
