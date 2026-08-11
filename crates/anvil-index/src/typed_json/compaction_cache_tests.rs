use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::io::tests::{MemoryBlockSink, MemoryDirectory};
use crate::{IndexDirectoryRead, IndexError, IndexKind, IndexMutation};

use super::*;

#[derive(Clone)]
struct CountingDirectory {
    inner: MemoryDirectory,
    block_opens: Arc<AtomicUsize>,
}

impl CountingDirectory {
    fn new(inner: MemoryDirectory) -> Self {
        Self::with_counter(inner, Arc::new(AtomicUsize::new(0)))
    }

    fn with_counter(inner: MemoryDirectory, block_opens: Arc<AtomicUsize>) -> Self {
        Self { inner, block_opens }
    }

    fn block_opens(&self) -> usize {
        self.block_opens.load(Ordering::Relaxed)
    }

    fn reset_block_opens(&self) {
        self.block_opens.store(0, Ordering::Relaxed);
    }
}

async fn counted_run(
    path: &str,
    value: u64,
    block_opens: Arc<AtomicUsize>,
) -> (CountingDirectory, RunView) {
    let definition = TypedJsonDefinition {
        fields: vec![TypedField {
            name: "status".into(),
            json_pointer: "/status".into(),
        }],
    };
    let mut builder = TypedJsonSegmentBuilder::new(
        definition,
        SegmentBuildOptions::for_level(64 * 1024, 0).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        builder.try_push(upsert(path, value)).unwrap(),
        SegmentPush::Accepted
    ));
    let mut sink = MemoryBlockSink::default();
    let run = builder
        .seal_with_target(&mut sink, 1_024)
        .await
        .unwrap()
        .unwrap();
    let directory =
        CountingDirectory::with_counter(sink.directory_with_root(run.into_root()), block_opens);
    let view = open_run(&directory, IndexKind::TypedJson).await.unwrap();
    (directory, view)
}

async fn counted_paths_run(
    paths: &[String],
    block_opens: Arc<AtomicUsize>,
) -> (CountingDirectory, RunView) {
    let definition = TypedJsonDefinition {
        fields: vec![TypedField {
            name: "status".into(),
            json_pointer: "/status".into(),
        }],
    };
    let mut builder = TypedJsonSegmentBuilder::new(
        definition,
        SegmentBuildOptions::for_level(64 * 1024, 0).unwrap(),
    )
    .unwrap();
    for (value, path) in paths.iter().enumerate() {
        assert!(matches!(
            builder.try_push(upsert(path, value as u64)).unwrap(),
            SegmentPush::Accepted
        ));
    }
    let mut sink = MemoryBlockSink::default();
    let run = builder
        .seal_with_target(&mut sink, 1_024)
        .await
        .unwrap()
        .unwrap();
    let directory =
        CountingDirectory::with_counter(sink.directory_with_root(run.into_root()), block_opens);
    let view = open_run(&directory, IndexKind::TypedJson).await.unwrap();
    (directory, view)
}

impl IndexDirectoryRead for CountingDirectory {
    type File = <MemoryDirectory as IndexDirectoryRead>::File;

    async fn open_root(&self) -> Result<Self::File, IndexError> {
        self.inner.open_root().await
    }

    async fn open_block(
        &self,
        descriptor: &crate::BlockDescriptor,
    ) -> Result<Self::File, IndexError> {
        self.block_opens.fetch_add(1, Ordering::Relaxed);
        self.inner.open_block(descriptor).await
    }
}

fn upsert(path: &str, ordinal: u64) -> IndexMutation<TypedJsonDocument> {
    IndexMutation::Upsert(TypedJsonDocument {
        document: DocumentRef {
            path: path.into(),
            version: 1,
        },
        fields: BTreeMap::from([("status".into(), vec![ScalarValue::Number(ordinal as f64)])]),
    })
}

#[tokio::test]
async fn repeated_point_reads_reuse_resolved_decoded_leaves() {
    let definition = TypedJsonDefinition {
        fields: vec![TypedField {
            name: "status".into(),
            json_pointer: "/status".into(),
        }],
    };
    let mut builder = TypedJsonSegmentBuilder::new(
        definition,
        SegmentBuildOptions::for_level(64 * 1024, 0).unwrap(),
    )
    .unwrap();
    for mutation in [upsert("/a", 0), upsert("/b", 1), upsert("/c", 2)] {
        assert!(matches!(
            builder.try_push(mutation).unwrap(),
            SegmentPush::Accepted
        ));
    }
    let mut sink = MemoryBlockSink::default();
    let run = builder
        .seal_with_target(&mut sink, 1_024)
        .await
        .unwrap()
        .unwrap();
    let directory = CountingDirectory::new(sink.directory_with_root(run.into_root()));
    let view = open_run(&directory, IndexKind::TypedJson).await.unwrap();
    let mut cache = CompactionPointCache::default();

    assert_eq!(
        cache.document(&directory, &view, 0).await.unwrap().path,
        "/a"
    );
    let document_opens = directory.block_opens();
    assert_eq!(
        cache.document(&directory, &view, 1).await.unwrap().path,
        "/b"
    );
    assert_eq!(directory.block_opens(), document_opens);

    assert_eq!(cache.typed(&directory, &view, 0).await.unwrap().ordinal, 0);
    let typed_opens = directory.block_opens();
    assert_eq!(cache.typed(&directory, &view, 1).await.unwrap().ordinal, 1);
    assert_eq!(directory.block_opens(), typed_opens);
    assert!(matches!(
        cache.typed(&directory, &view, 0).await,
        Err(IndexError::InvalidFormat("typed ordinal already consumed"))
    ));
    assert_eq!(directory.block_opens(), typed_opens);

    let path_root = view.component(PATH_CHANGES_TAG).unwrap();
    assert_eq!(
        cache
            .path(&directory, path_root, "/a")
            .await
            .unwrap()
            .unwrap()
            .document
            .path,
        "/a"
    );
    let path_opens = directory.block_opens();
    assert_eq!(
        cache
            .path(&directory, path_root, "/b")
            .await
            .unwrap()
            .unwrap()
            .document
            .path,
        "/b"
    );
    assert_eq!(directory.block_opens(), path_opens);
}

#[tokio::test]
async fn source_payload_cache_reserves_two_moved_payload_slots() {
    let block_opens = Arc::new(AtomicUsize::new(0));
    let mut directories = Vec::new();
    let mut views = Vec::new();
    for (path, value) in [("/a", 0), ("/b", 1), ("/c", 2), ("/d", 3)] {
        let (directory, view) = counted_run(path, value, block_opens.clone()).await;
        directories.push(directory);
        views.push(view);
    }

    let mut source = CompactionPointCache::default();
    for (directory, view) in directories.iter().zip(&views) {
        source.document(directory, view, 0).await.unwrap();
        source.typed(directory, view, 0).await.unwrap();
    }
    assert_eq!(source.cached_leaf_count(), 6);
}

#[tokio::test]
async fn routed_input_and_output_caches_match_the_charged_working_set() {
    let input_opens = Arc::new(AtomicUsize::new(0));
    let mut directories = Vec::new();
    let mut views = Vec::new();
    for (path, value) in [("/a", 0), ("/b", 1), ("/c", 2), ("/d", 3)] {
        let (directory, view) = counted_run(path, value, input_opens.clone()).await;
        directories.push(directory);
        views.push(view);
    }
    let (fifth_directory, fifth_view) = counted_run("/e", 4, input_opens.clone()).await;
    let mut input = CompactionPointCache::input_documents();

    for (directory, view) in directories.iter().zip(&views) {
        input.document(directory, view, 0).await.unwrap();
    }
    let first_input_sequence_opens = input_opens.load(Ordering::Relaxed);

    for (directory, view) in directories.iter().zip(&views) {
        input.document(directory, view, 0).await.unwrap();
    }
    assert_eq!(
        input_opens.load(Ordering::Relaxed),
        first_input_sequence_opens,
        "four routed input document leaves remain hot"
    );

    input
        .document(&fifth_directory, &fifth_view, 0)
        .await
        .unwrap();
    assert_eq!(input.cached_leaf_count(), 4);

    let candidate_paths = (0..24)
        .map(|index| format!("/output/{index:03}/{}", "x".repeat(384)))
        .collect::<Vec<_>>();
    let output_opens = Arc::new(AtomicUsize::new(0));
    let (output_directory, output_view) = counted_paths_run(&candidate_paths, output_opens).await;
    let output_path_root = output_view.component(PATH_CHANGES_TAG).unwrap();
    let mut leaf_hashes = BTreeSet::new();
    let mut paths_in_distinct_leaves = Vec::new();
    for path in &candidate_paths {
        let descriptor =
            crate::run::find_leaf(&output_directory, output_path_root, path.as_bytes())
                .await
                .unwrap()
                .unwrap();
        if leaf_hashes.insert(descriptor.hash) {
            paths_in_distinct_leaves.push(path.as_str());
        }
        if paths_in_distinct_leaves.len() == 5 {
            break;
        }
    }
    assert_eq!(paths_in_distinct_leaves.len(), 5);
    output_directory.reset_block_opens();

    let mut output = CompactionPointCache::staged_output_paths();
    for path in &paths_in_distinct_leaves[..4] {
        output
            .path(&output_directory, output_path_root, path)
            .await
            .unwrap()
            .unwrap();
    }
    let first_output_sequence_opens = output_directory.block_opens();
    for path in &paths_in_distinct_leaves[..4] {
        output
            .path(&output_directory, output_path_root, path)
            .await
            .unwrap()
            .unwrap();
    }
    assert_eq!(
        output_directory.block_opens(),
        first_output_sequence_opens,
        "four non-sequential staged-output path leaves remain hot"
    );
    output
        .path(
            &output_directory,
            output_path_root,
            paths_in_distinct_leaves[4],
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.cached_leaf_count(), 4);
}
