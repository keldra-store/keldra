use crate::compaction::COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES;
use crate::compaction::test_support::TokioExecutor;

#[derive(Clone)]
struct RejectInputKeyReads {
    inner: MemoryDirectory,
}

impl IndexDirectoryRead for RejectInputKeyReads {
    type File = crate::io::tests::MemoryFile;

    async fn open_root(&self) -> Result<Self::File, IndexError> {
        self.inner.open_root().await
    }

    async fn open_block(
        &self,
        descriptor: &crate::BlockDescriptor,
    ) -> Result<Self::File, IndexError> {
        if descriptor.component_tag == KEYS_TAG {
            return Err(IndexError::Io(
                "compaction reread an input typed-key component".into(),
            ));
        }
        self.inner.open_block(descriptor).await
    }
}

#[derive(Clone, Default)]
struct RejectStagedPathReads {
    inner: MemoryBlockSink,
}

impl IndexBlockSink for RejectStagedPathReads {
    async fn emit(
        &mut self,
        block: crate::GeneratedBlock,
    ) -> Result<crate::BlockDescriptor, IndexError> {
        self.inner.emit(block).await
    }
}

impl IndexDirectoryRead for RejectStagedPathReads {
    type File = crate::io::tests::MemoryFile;

    async fn open_root(&self) -> Result<Self::File, IndexError> {
        self.inner.open_root().await
    }

    async fn open_block(
        &self,
        descriptor: &crate::BlockDescriptor,
    ) -> Result<Self::File, IndexError> {
        if descriptor.component_tag == PATH_CHANGES_TAG {
            return Err(IndexError::Io(
                "compaction point-read its staged typed path component".into(),
            ));
        }
        self.inner.open_block(descriptor).await
    }
}

fn metadata_upsert(
    path: &str,
    version: u64,
    status: &str,
    amount: f64,
) -> IndexMutation<MetadataDocument> {
    IndexMutation::Upsert(MetadataDocument {
        document: DocumentRef {
            path: path.into(),
            version,
        },
        fields: selected(vec![ScalarValue::String(status.into())], amount),
    })
}

async fn build_metadata_run(
    mutations: impl IntoIterator<Item = IndexMutation<MetadataDocument>>,
    level: u8,
    target: usize,
) -> (MemoryBlockSink, SealedRun) {
    let mut builder = MetadataSegmentBuilder::new(
        definition(),
        SegmentBuildOptions::for_level(64 * 1024, level).unwrap(),
    )
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
async fn typed_parallel_ranges_are_semantically_equivalent_and_deterministic() {
    let old_mutations = (0..96)
        .map(|index| {
            upsert(
                &format!("/{:02x}/typed/{index:04}", index % 32),
                1,
                if index % 2 == 0 { "active" } else { "idle" },
                index as f64,
            )
        })
        .collect::<Vec<_>>();
    let (old_sink, old_run) = build_run(old_mutations, 0, 96).await;
    let old = directory(&old_sink, old_run);
    let new_mutations = (0..48)
        .map(|index| {
            let path = format!("/{:02x}/typed/{index:04}", index % 32);
            if index % 7 == 0 {
                IndexMutation::Remove(DocumentRef { path, version: 2 })
            } else {
                upsert(&path, 2, "updated", (index * 3) as f64)
            }
        })
        .collect::<Vec<_>>();
    let (new_sink, new_run) = build_run(new_mutations, 0, 96).await;
    let runs = [directory(&new_sink, new_run), old];

    let one_lane_progress = crate::compaction::CompactionProgress::default();
    let mut one_lane_sink = MemoryBlockSink::default();
    let one_lane = parallel_compaction::merge_typed_parallel(
        &runs,
        IndexKind::TypedJson,
        1,
        96,
        &mut one_lane_sink,
        crate::compaction::CompactionParallelism::serial(),
        one_lane_progress.clone(),
        TokioExecutor::default(),
    )
    .await
    .unwrap();
    let progress = crate::compaction::CompactionProgress::default();
    let mut parallel_sink = MemoryBlockSink::default();
    let parallel = parallel_compaction::merge_typed_parallel(
        &runs,
        IndexKind::TypedJson,
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
    let repeat = parallel_compaction::merge_typed_parallel(
        &runs,
        IndexKind::TypedJson,
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
            one_lane.descriptor().mutation_count,
            one_lane.descriptor().live_document_count,
            one_lane.descriptor().minimum_version,
            one_lane.descriptor().maximum_version,
        )
    );
    let one_lane = directory(&one_lane_sink, one_lane);
    let parallel = directory(&parallel_sink, parallel);
    let parallel_view = open_run(&parallel, IndexKind::TypedJson).await.unwrap();
    let mut paths = PathRunCursor::new(
        &parallel,
        parallel_view.component(PATH_CHANGES_TAG).unwrap().clone(),
    );
    let mut local_by_range = BTreeMap::<u32, Vec<u64>>::new();
    while let Some(change) = paths.next().await.unwrap() {
        if let Some(ordinal) = change.document_ordinal {
            local_by_range
                .entry(identity::ordinal_range_id(ordinal))
                .or_default()
                .push(identity::ordinal_local(ordinal));
        }
    }
    assert!(local_by_range.len() > 1);
    assert!(local_by_range.values().all(|locals| {
        locals
            .iter()
            .copied()
            .eq(0..u64::try_from(locals.len()).unwrap())
    }));
    let mut query = exists_query();
    query.limit = 200;
    assert_eq!(
        TypedJsonEngine::query(&[parallel], &definition(), &query)
            .await
            .unwrap(),
        TypedJsonEngine::query(&[one_lane], &definition(), &query)
            .await
            .unwrap()
    );
    assert_eq!(one_lane_progress.snapshot().effective_lanes, 1);
    let snapshot = progress.snapshot();
    assert!(snapshot.ranges_total > 3);
    assert!(snapshot.effective_lanes > 1);
    assert_eq!(snapshot.ranges_completed, snapshot.ranges_total);
    assert_eq!(snapshot.active_lanes, 0);
    assert_eq!(snapshot.waiting_lanes, 0);
    assert_eq!(snapshot.output_records, mutation_count);
}

#[tokio::test]
async fn metadata_parallel_ranges_are_semantically_equivalent_and_deterministic() {
    let old_mutations = (0..80)
        .map(|index| {
            metadata_upsert(
                &format!("/{:02x}/metadata/{index:04}", index % 24),
                1,
                if index % 2 == 0 { "ready" } else { "waiting" },
                index as f64,
            )
        })
        .collect::<Vec<_>>();
    let (old_sink, old_run) = build_metadata_run(old_mutations, 0, 96).await;
    let old = directory(&old_sink, old_run);
    let new_mutations = (0..40)
        .map(|index| {
            let path = format!("/{:02x}/metadata/{index:04}", index % 24);
            if index % 5 == 0 {
                IndexMutation::Remove(DocumentRef { path, version: 2 })
            } else {
                metadata_upsert(&path, 2, "complete", (index * 5) as f64)
            }
        })
        .collect::<Vec<_>>();
    let (new_sink, new_run) = build_metadata_run(new_mutations, 0, 96).await;
    let runs = [directory(&new_sink, new_run), old];

    let one_lane_progress = crate::compaction::CompactionProgress::default();
    let mut one_lane_sink = MemoryBlockSink::default();
    let one_lane = parallel_compaction::merge_typed_parallel(
        &runs,
        IndexKind::MetadataFilter,
        1,
        96,
        &mut one_lane_sink,
        crate::compaction::CompactionParallelism::serial(),
        one_lane_progress.clone(),
        TokioExecutor::default(),
    )
    .await
    .unwrap();
    let progress = crate::compaction::CompactionProgress::default();
    let mut parallel_sink = MemoryBlockSink::default();
    let parallel = parallel_compaction::merge_typed_parallel(
        &runs,
        IndexKind::MetadataFilter,
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
    let repeat = parallel_compaction::merge_typed_parallel(
        &runs,
        IndexKind::MetadataFilter,
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
            one_lane.descriptor().mutation_count,
            one_lane.descriptor().live_document_count,
            one_lane.descriptor().minimum_version,
            one_lane.descriptor().maximum_version,
        )
    );
    let one_lane = directory(&one_lane_sink, one_lane);
    let parallel = directory(&parallel_sink, parallel);
    let mut query = exists_query();
    query.limit = 200;
    assert_eq!(
        MetadataFilterEngine::query(&[parallel], &definition(), &query)
            .await
            .unwrap(),
        MetadataFilterEngine::query(&[one_lane], &definition(), &query)
            .await
            .unwrap()
    );
    assert_eq!(one_lane_progress.snapshot().effective_lanes, 1);
    let snapshot = progress.snapshot();
    assert!(snapshot.ranges_total > 3);
    assert!(snapshot.effective_lanes > 1);
    assert_eq!(snapshot.ranges_completed, snapshot.ranges_total);
    assert_eq!(snapshot.active_lanes, 0);
    assert_eq!(snapshot.waiting_lanes, 0);
    assert_eq!(snapshot.output_records, mutation_count);
}

#[tokio::test]
async fn typed_routed_rebuild_uses_selected_rows_without_rereading_old_keys_or_new_paths() {
    let (old_sink, old_run) = build_run(
        [upsert("/a", 1, "old-a", 1.0), upsert("/b", 1, "old-b", 2.0)],
        0,
        64,
    )
    .await;
    let (new_sink, new_run) = build_run(
        [
            upsert("/a", 2, "new-a", 3.0),
            IndexMutation::Remove(DocumentRef {
                path: "/b".into(),
                version: 2,
            }),
        ],
        0,
        64,
    )
    .await;
    let runs = [
        RejectInputKeyReads {
            inner: directory(&new_sink, new_run),
        },
        RejectInputKeyReads {
            inner: directory(&old_sink, old_run),
        },
    ];
    let mut output = RejectStagedPathReads::default();
    let merged = parallel_compaction::merge_typed_parallel(
        &runs,
        IndexKind::TypedJson,
        1,
        64,
        &mut output,
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
    let compacted = [output.inner.directory_with_root(merged.into_root())];
    let hits = TypedJsonEngine::query(&compacted, &definition(), &exists_query())
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].document.path, "/a");
    assert_eq!(hits[0].document.version, 2);
    assert_eq!(
        hits[0].fields["status"],
        [ScalarValue::String("new-a".into())]
    );
}

#[tokio::test]
async fn metadata_routed_rebuild_uses_selected_rows_without_rereading_old_keys_or_new_paths() {
    let (old_sink, old_run) = build_metadata_run(
        [
            metadata_upsert("/a", 1, "old-a", 1.0),
            metadata_upsert("/b", 1, "old-b", 2.0),
        ],
        0,
        64,
    )
    .await;
    let (new_sink, new_run) = build_metadata_run(
        [
            metadata_upsert("/a", 2, "new-a", 3.0),
            IndexMutation::Remove(DocumentRef {
                path: "/b".into(),
                version: 2,
            }),
        ],
        0,
        64,
    )
    .await;
    let runs = [
        RejectInputKeyReads {
            inner: directory(&new_sink, new_run),
        },
        RejectInputKeyReads {
            inner: directory(&old_sink, old_run),
        },
    ];
    let mut output = RejectStagedPathReads::default();
    let merged = parallel_compaction::merge_typed_parallel(
        &runs,
        IndexKind::MetadataFilter,
        1,
        64,
        &mut output,
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
    let compacted = [output.inner.directory_with_root(merged.into_root())];
    let hits = MetadataFilterEngine::query(&compacted, &definition(), &exists_query())
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].document.path, "/a");
    assert_eq!(hits[0].document.version, 2);
    assert_eq!(
        hits[0].fields["status"],
        [ScalarValue::String("new-a".into())]
    );
}

#[tokio::test]
async fn typed_parallel_cpu_failure_closes_and_joins_all_ranges() {
    let (sink, run) = build_run(
        [
            upsert("/00/a", 1, "active", 1.0),
            upsert("/ff/z", 1, "active", 2.0),
        ],
        0,
        64,
    )
    .await;
    let runs = [directory(&sink, run)];
    let progress = crate::compaction::CompactionProgress::default();
    let mut output = MemoryBlockSink::default();
    let error = parallel_compaction::merge_typed_parallel(
        &runs,
        IndexKind::TypedJson,
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
