use crate::compaction::COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES;
use crate::compaction::test_support::TokioExecutor;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

#[derive(Clone, Default)]
struct CountingOutputSink {
    inner: MemoryBlockSink,
    path_opens: Arc<AtomicUsize>,
}

impl IndexBlockSink for CountingOutputSink {
    async fn emit(&mut self, block: GeneratedBlock) -> Result<BlockDescriptor, IndexError> {
        self.inner.emit(block).await
    }
}

impl IndexDirectoryRead for CountingOutputSink {
    type File = <MemoryBlockSink as IndexDirectoryRead>::File;

    async fn open_root(&self) -> Result<Self::File, IndexError> {
        self.inner.open_root().await
    }

    async fn open_block(&self, descriptor: &BlockDescriptor) -> Result<Self::File, IndexError> {
        if descriptor.component_tag == PATH_CHANGES_TAG {
            self.path_opens.fetch_add(1, AtomicOrdering::Relaxed);
        }
        self.inner.open_block(descriptor).await
    }
}

#[derive(Default)]
struct ComponentOpenCounts {
    documents: AtomicUsize,
    paths: AtomicUsize,
    postings: AtomicUsize,
}

#[derive(Clone)]
struct CountingDirectory {
    inner: MemoryDirectory,
    counts: Arc<ComponentOpenCounts>,
}

impl CountingDirectory {
    fn new(inner: MemoryDirectory) -> Self {
        Self {
            inner,
            counts: Arc::default(),
        }
    }

    fn snapshot(&self) -> (usize, usize, usize) {
        (
            self.counts.documents.load(AtomicOrdering::Relaxed),
            self.counts.paths.load(AtomicOrdering::Relaxed),
            self.counts.postings.load(AtomicOrdering::Relaxed),
        )
    }
}

impl IndexDirectoryRead for CountingDirectory {
    type File = <MemoryDirectory as IndexDirectoryRead>::File;

    async fn open_root(&self) -> Result<Self::File, IndexError> {
        self.inner.open_root().await
    }

    async fn open_block(&self, descriptor: &BlockDescriptor) -> Result<Self::File, IndexError> {
        match descriptor.component_tag {
            crate::segment::DOCUMENTS_TAG => {
                self.counts.documents.fetch_add(1, AtomicOrdering::Relaxed);
            }
            PATH_CHANGES_TAG => {
                self.counts.paths.fetch_add(1, AtomicOrdering::Relaxed);
            }
            FULL_TEXT_POSTINGS_TAG => {
                self.counts.postings.fetch_add(1, AtomicOrdering::Relaxed);
            }
            _ => {}
        }
        self.inner.open_block(descriptor).await
    }
}

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

    let one_lane_progress = crate::compaction::CompactionProgress::default();
    let mut one_lane_sink = MemoryBlockSink::default();
    let one_lane = FullTextEngine::merge_parallel_with_target(
        &runs,
        1,
        96,
        &mut one_lane_sink,
        crate::compaction::CompactionParallelism::serial(),
        one_lane_progress.clone(),
        TokioExecutor::default(),
    )
    .await
    .unwrap();
    let expected_mutations = one_lane.descriptor().mutation_count;
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
        one_lane.descriptor().mutation_count
    );
    assert_eq!(
        parallel.descriptor().live_document_count,
        one_lane.descriptor().live_document_count
    );
    assert_eq!(
        parallel.descriptor().minimum_version,
        one_lane.descriptor().minimum_version
    );
    assert_eq!(
        parallel.descriptor().maximum_version,
        one_lane.descriptor().maximum_version
    );

    let one_lane = [directory(&one_lane_sink, one_lane)];
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
            FullTextEngine::query(&one_lane, query).await.unwrap(),
        );
    }
    assert_eq!(one_lane_progress.snapshot().effective_lanes, 1);
    let snapshot = progress.snapshot();
    // Four common path/document writers, four bounded posting-selection
    // lanes, and four final posting-key merge lanes shared the executor.
    assert_eq!(snapshot.ranges_total, 12);
    assert!(snapshot.effective_lanes > 1);
    assert_eq!(snapshot.ranges_completed, snapshot.ranges_total);
    assert_eq!(snapshot.active_lanes, 0);
    assert_eq!(snapshot.waiting_lanes, 0);
    assert_eq!(snapshot.output_records, expected_mutations);
}

#[tokio::test]
async fn document_and_path_reads_do_not_scale_with_posting_count() {
    let sparse = (0..16)
        .map(|index| upsert(&format!("/docs/{index:04}"), 1, "base"))
        .collect::<Vec<_>>();
    let dense_text = (0..24)
        .map(|term| format!("term{term:02}"))
        .collect::<Vec<_>>()
        .join(" ");
    let dense = (0..16)
        .map(|index| {
            upsert(
                &format!("/docs/{index:04}"),
                1,
                &format!("base {dense_text}"),
            )
        })
        .collect::<Vec<_>>();

    let sparse_counts = compact_counted(sparse).await;
    let dense_counts = compact_counted(dense).await;
    assert!(dense_counts.2 > sparse_counts.2);
    assert_eq!(dense_counts.0, sparse_counts.0);
    assert_eq!(dense_counts.1, sparse_counts.1);
    assert_eq!(dense_counts.3, sparse_counts.3);
}

async fn compact_counted(
    mutations: Vec<IndexMutation<FullTextDocument>>,
) -> (usize, usize, usize, usize) {
    let (sink, run) = build(mutations, 0, 96).await;
    let input = CountingDirectory::new(directory(&sink, run));
    let mut output = CountingOutputSink::default();
    FullTextEngine::merge_parallel_with_target(
        std::slice::from_ref(&input),
        1,
        96,
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
    let (documents, paths, postings) = input.snapshot();
    (
        documents,
        paths,
        postings,
        output.path_opens.load(AtomicOrdering::Relaxed),
    )
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
