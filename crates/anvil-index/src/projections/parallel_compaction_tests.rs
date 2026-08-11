use crate::compaction::COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES;
use crate::compaction::test_support::TokioExecutor;

fn tensor_document(path: &str, version: u64, index: usize) -> IndexMutation<TensorDocument> {
    IndexMutation::Upsert(TensorDocument {
        document: DocumentRef {
            path: path.into(),
            version,
        },
        records: vec![TensorRecord {
            model_id: format!("model-{:02x}", index % 24),
            tensor_name: format!("weight-{index:04}"),
            source_path: format!("/data/{index:04}"),
            source_version: version,
            offset: index as u64,
            length: 4,
            dtype: "F32".into(),
            shape: vec![1],
        }],
    })
}

async fn tensor_run(
    mutations: impl IntoIterator<Item = IndexMutation<TensorDocument>>,
    level: u8,
    target: usize,
) -> (MemoryBlockSink, SealedRun) {
    let mut builder =
        TensorSegmentBuilder::new(SegmentBuildOptions::for_level(64 * 1024, level).unwrap())
            .unwrap();
    for mutation in mutations {
        assert!(matches!(
            builder.try_push(mutation).unwrap(),
            SegmentPush::Accepted
        ));
    }
    let mut sink = MemoryBlockSink::default();
    let run = builder
        .seal_with_target(&mut sink, target)
        .await
        .unwrap()
        .unwrap();
    (sink, run)
}

#[tokio::test]
async fn git_parallel_ranges_are_semantically_equivalent_and_deterministic() {
    let old_mutations = (0..96)
        .map(|index| {
            git_document(
                &format!("/{:02x}/source/{index:04}", index % 32),
                1,
                &format!("src/{index:04}.rs"),
                &format!("object-{index:04}"),
            )
        })
        .collect::<Vec<_>>();
    let (old_sink, old_run) = git_run(old_mutations, 0, 96).await;
    let old = directory(&old_sink, old_run);
    let new_mutations = (0..48)
        .map(|index| {
            let path = format!("/{:02x}/source/{index:04}", index % 32);
            if index % 7 == 0 {
                IndexMutation::Remove(DocumentRef { path, version: 2 })
            } else {
                git_document(
                    &path,
                    2,
                    &format!("src/{index:04}.rs"),
                    &format!("updated-{index:04}"),
                )
            }
        })
        .collect::<Vec<_>>();
    let (new_sink, new_run) = git_run(new_mutations, 0, 96).await;
    let runs = [directory(&new_sink, new_run), old];

    let mut serial_sink = MemoryBlockSink::default();
    let serial =
        merge_projection::<_, _, GitPayload>(&runs, IndexKind::GitSource, 1, 96, &mut serial_sink)
            .await
            .unwrap();
    let progress = crate::compaction::CompactionProgress::default();
    let mut parallel_sink = MemoryBlockSink::default();
    let parallel = parallel_compaction::merge_projection_parallel::<_, _, GitPayload, _>(
        &runs,
        IndexKind::GitSource,
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

    let mut repeat_sink = MemoryBlockSink::default();
    let repeat = parallel_compaction::merge_projection_parallel::<_, _, GitPayload, _>(
        &runs,
        IndexKind::GitSource,
        1,
        96,
        &mut repeat_sink,
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

    assert_eq!(parallel, repeat);
    assert_eq!(parallel_sink.len(), repeat_sink.len());
    let mutation_count = parallel.descriptor().mutation_count;
    assert_eq!(
        (
            parallel.descriptor().mutation_count,
            parallel.descriptor().live_document_count,
            parallel.descriptor().minimum_version,
            parallel.descriptor().maximum_version,
        ),
        (
            serial.descriptor().mutation_count,
            serial.descriptor().live_document_count,
            serial.descriptor().minimum_version,
            serial.descriptor().maximum_version,
        )
    );
    let serial_runs = [directory(&serial_sink, serial)];
    let parallel_runs = [directory(&parallel_sink, parallel)];
    for index in 0..96 {
        let tree_path = format!("src/{index:04}.rs");
        assert_eq!(
            GitSourceEngine::get_by_path(&parallel_runs, "repo", "abc", &tree_path)
                .await
                .unwrap(),
            GitSourceEngine::get_by_path(&serial_runs, "repo", "abc", &tree_path)
                .await
                .unwrap()
        );
        for object_id in [format!("object-{index:04}"), format!("updated-{index:04}")] {
            assert_eq!(
                GitSourceEngine::get_object(&parallel_runs, "repo", &object_id, 2)
                    .await
                    .unwrap(),
                GitSourceEngine::get_object(&serial_runs, "repo", &object_id, 2)
                    .await
                    .unwrap()
            );
        }
    }
    let snapshot = progress.snapshot();
    assert!(snapshot.ranges_total > 4);
    assert_eq!(snapshot.ranges_completed, snapshot.ranges_total);
    assert_eq!(snapshot.active_lanes, 0);
    assert_eq!(snapshot.waiting_lanes, 0);
    assert_eq!(snapshot.output_records, mutation_count);
}

#[tokio::test]
async fn tensor_parallel_ranges_are_semantically_equivalent_and_deterministic() {
    let old_mutations = (0..80)
        .map(|index| {
            tensor_document(
                &format!("/{:02x}/manifest/{index:04}", index % 24),
                1,
                index,
            )
        })
        .collect::<Vec<_>>();
    let (old_sink, old_run) = tensor_run(old_mutations, 0, 96).await;
    let old = directory(&old_sink, old_run);
    let new_mutations = (0..40)
        .map(|index| {
            let path = format!("/{:02x}/manifest/{index:04}", index % 24);
            if index % 5 == 0 {
                IndexMutation::Remove(DocumentRef { path, version: 2 })
            } else {
                tensor_document(&path, 2, index)
            }
        })
        .collect::<Vec<_>>();
    let (new_sink, new_run) = tensor_run(new_mutations, 0, 96).await;
    let runs = [directory(&new_sink, new_run), old];

    let mut serial_sink = MemoryBlockSink::default();
    let serial =
        merge_projection::<_, _, TensorPayload>(&runs, IndexKind::Tensor, 1, 96, &mut serial_sink)
            .await
            .unwrap();
    let progress = crate::compaction::CompactionProgress::default();
    let mut parallel_sink = MemoryBlockSink::default();
    let parallel = parallel_compaction::merge_projection_parallel::<_, _, TensorPayload, _>(
        &runs,
        IndexKind::Tensor,
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

    let mut repeat_sink = MemoryBlockSink::default();
    let repeat = parallel_compaction::merge_projection_parallel::<_, _, TensorPayload, _>(
        &runs,
        IndexKind::Tensor,
        1,
        96,
        &mut repeat_sink,
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

    assert_eq!(parallel, repeat);
    assert_eq!(parallel_sink.len(), repeat_sink.len());
    let mutation_count = parallel.descriptor().mutation_count;
    assert_eq!(
        (
            parallel.descriptor().mutation_count,
            parallel.descriptor().live_document_count,
            parallel.descriptor().minimum_version,
            parallel.descriptor().maximum_version,
        ),
        (
            serial.descriptor().mutation_count,
            serial.descriptor().live_document_count,
            serial.descriptor().minimum_version,
            serial.descriptor().maximum_version,
        )
    );
    let serial_runs = [directory(&serial_sink, serial)];
    let parallel_runs = [directory(&parallel_sink, parallel)];
    for index in 0..80 {
        let model_id = format!("model-{:02x}", index % 24);
        let tensor_name = format!("weight-{index:04}");
        assert_eq!(
            TensorProjectionEngine::get(&parallel_runs, &model_id, &tensor_name)
                .await
                .unwrap(),
            TensorProjectionEngine::get(&serial_runs, &model_id, &tensor_name)
                .await
                .unwrap()
        );
    }
    let snapshot = progress.snapshot();
    assert!(snapshot.ranges_total > 3);
    assert_eq!(snapshot.ranges_completed, snapshot.ranges_total);
    assert_eq!(snapshot.active_lanes, 0);
    assert_eq!(snapshot.waiting_lanes, 0);
    assert_eq!(snapshot.output_records, mutation_count);
}

#[tokio::test]
async fn projection_parallel_cpu_failure_closes_and_joins_all_ranges() {
    let (sink, run) = git_run(
        [
            git_document("/00/a", 1, "src/a.rs", "a"),
            git_document("/ff/z", 1, "src/z.rs", "z"),
        ],
        0,
        64,
    )
    .await;
    let runs = [directory(&sink, run)];
    let progress = crate::compaction::CompactionProgress::default();
    let mut output = MemoryBlockSink::default();
    let error = parallel_compaction::merge_projection_parallel::<_, _, GitPayload, _>(
        &runs,
        IndexKind::GitSource,
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
