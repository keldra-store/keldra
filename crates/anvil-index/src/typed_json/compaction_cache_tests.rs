use std::collections::BTreeMap;
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
async fn four_input_documents_and_output_path_remain_hot_together() {
    let block_opens = Arc::new(AtomicUsize::new(0));
    let mut directories = Vec::new();
    let mut views = Vec::new();
    for (path, value) in [("/a", 0), ("/b", 1), ("/c", 2), ("/d", 3)] {
        let (directory, view) = counted_run(path, value, block_opens.clone()).await;
        directories.push(directory);
        views.push(view);
    }
    let (sixth_directory, sixth_view) = counted_run("/e", 4, block_opens.clone()).await;
    let output_path_root = views[0].component(PATH_CHANGES_TAG).unwrap();
    let mut cache = CompactionPointCache::default();

    for (directory, view) in directories.iter().zip(&views) {
        cache.document(directory, view, 0).await.unwrap();
    }
    cache
        .path(&directories[0], output_path_root, "/a")
        .await
        .unwrap()
        .unwrap();
    let first_sequence_opens = block_opens.load(Ordering::Relaxed);

    for (directory, view) in directories.iter().zip(&views) {
        cache.document(directory, view, 0).await.unwrap();
    }
    cache
        .path(&directories[0], output_path_root, "/a")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        block_opens.load(Ordering::Relaxed),
        first_sequence_opens,
        "d0,d1,d2,d3,p must remain in the five-leaf point cache"
    );

    cache
        .document(&sixth_directory, &sixth_view, 0)
        .await
        .unwrap();
    assert_eq!(cache.cached_leaf_count(), 5);
}
