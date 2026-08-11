use crate::compaction::COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES;
use crate::compaction::test_support::TokioExecutor;

#[tokio::test]
async fn parallel_ranges_are_deterministic_and_query_equivalent() {
    let old_mutations = (0..96)
        .map(|index| {
            upsert(
                &format!("/{:02x}/docs/{index:04}", index % 32),
                1,
                &format!("{} shared old{index:04}", term_for(index),),
            )
        })
        .collect::<Vec<_>>();
    let (old_sink, old_run) = build(old_mutations, 0, 96).await;
    let old = directory(&old_sink, old_run);
    let new_mutations = (0..48)
        .map(|index| {
            let path = format!("/{:02x}/docs/{index:04}", index % 32);
            if index % 7 == 0 {
                IndexMutation::Remove(DocumentRef { path, version: 2 })
            } else {
                upsert(
                    &path,
                    2,
                    &format!("{} shared updated{index:04}", term_for(index + 11)),
                )
            }
        })
        .collect::<Vec<_>>();
    let (new_sink, new_run) = build(new_mutations, 0, 96).await;
    let runs = [directory(&new_sink, new_run), old];

    let mut serial_sink = MemoryBlockSink::default();
    let serial = FullTextEngine::merge_with_target(&runs, 1, 96, &mut serial_sink)
        .await
        .unwrap();
    let expected_mutations = serial.descriptor().mutation_count;
    let progress = crate::compaction::CompactionProgress::default();
    let mut parallel_sink = MemoryBlockSink::default();
    let parallel = FullTextEngine::merge_parallel_with_target(
        &runs,
        1,
        96,
        &mut parallel_sink,
        crate::compaction::CompactionParallelism::new(
            4,
            COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES,
        )
        .unwrap(),
        progress.clone(),
        TokioExecutor::default(),
    )
    .await
    .unwrap();

    let mut repeated_sink = MemoryBlockSink::default();
    let repeated = FullTextEngine::merge_parallel_with_target(
        &runs,
        1,
        96,
        &mut repeated_sink,
        crate::compaction::CompactionParallelism::new(
            4,
            COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES,
        )
        .unwrap(),
        crate::compaction::CompactionProgress::default(),
        TokioExecutor::default(),
    )
    .await
    .unwrap();

    assert_eq!(parallel, repeated);
    assert_eq!(parallel_sink.len(), repeated_sink.len());
    assert_eq!(
        parallel.descriptor().mutation_count,
        serial.descriptor().mutation_count
    );
    assert_eq!(
        parallel.descriptor().live_document_count,
        serial.descriptor().live_document_count
    );
    assert_eq!(
        parallel.descriptor().minimum_version,
        serial.descriptor().minimum_version
    );
    assert_eq!(
        parallel.descriptor().maximum_version,
        serial.descriptor().maximum_version
    );

    let serial = [directory(&serial_sink, serial)];
    let parallel = [directory(&parallel_sink, parallel)];
    let fields = Vec::new();
    for text in ["shared", "alpha", "zulu", "updated0047"] {
        let query = FullTextQuery {
            text,
            fields: &fields,
            phrase: false,
            match_all_terms: false,
            limit: 128,
        };
        assert_eq!(
            FullTextEngine::query(&parallel, query.clone())
                .await
                .unwrap(),
            FullTextEngine::query(&serial, query).await.unwrap(),
        );
    }
    let snapshot = progress.snapshot();
    // Four count stripes, four range-local path/document writers, and four
    // independent term/posting writers all ran through the bounded executor.
    assert_eq!(snapshot.ranges_total, 12);
    assert_eq!(snapshot.ranges_completed, snapshot.ranges_total);
    assert_eq!(snapshot.active_lanes, 0);
    assert_eq!(snapshot.waiting_lanes, 0);
    assert_eq!(snapshot.output_records, expected_mutations);
}

#[tokio::test]
async fn parallel_cpu_failure_closes_and_joins_all_ranges() {
    let (sink, run) = build(
        [
            upsert("/00/a", 1, "alpha shared"),
            upsert("/ff/z", 1, "zulu shared"),
        ],
        0,
        64,
    )
    .await;
    let runs = [directory(&sink, run)];
    let progress = crate::compaction::CompactionProgress::default();
    let mut output = MemoryBlockSink::default();
    let error = FullTextEngine::merge_parallel_with_target(
        &runs,
        1,
        64,
        &mut output,
        crate::compaction::CompactionParallelism::new(
            4,
            COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES,
        )
        .unwrap(),
        progress.clone(),
        TokioExecutor::failing_cpu(),
    )
    .await
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("injected compaction CPU failure")
    );
    assert_eq!(progress.snapshot().active_lanes, 0);
    assert_eq!(progress.snapshot().waiting_lanes, 0);
}

fn term_for(index: usize) -> &'static str {
    const TERMS: [&str; 16] = [
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india",
        "juliet", "kilo", "lima", "mike", "november", "yankee", "zulu",
    ];
    TERMS[index % TERMS.len()]
}
