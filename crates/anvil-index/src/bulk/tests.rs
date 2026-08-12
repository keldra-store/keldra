use std::collections::BTreeMap;

use crate::compaction::test_support::TokioExecutor;
use crate::full_text::{FullTextDocument, FullTextEngine, FullTextQuery};
use crate::hybrid::{HybridDefinition, HybridDocument};
use crate::io::tests::{MemoryBlockSink, MemoryDirectory};
use crate::ordered::{PathDocument, PathEngine, PathQuery};
use crate::projections::{GitSourceDocument, GitSourceRecord, TensorDocument, TensorRecord};
use crate::run::open_run;
use crate::typed_json::{
    MetadataDocument, ScalarValue, TypedField, TypedJsonDefinition, TypedJsonDocument,
};
use crate::vector::{VectorDefinition, VectorDocument, VectorMetric};
use crate::{DocumentRef, IndexKind, IndexMutation, SealedRun};

use super::*;

const SORT_CHUNK: usize = 64 * 1024;

#[test]
fn range_ordinals_use_the_full_32_by_32_identity_space() {
    let maximum_base = range_ordinal_base(u32::MAX as u64).unwrap();
    assert_eq!(
        range_local_ordinal(maximum_base, u32::MAX as u64).unwrap(),
        u64::MAX
    );
    assert!(range_ordinal_base(u32::MAX as u64 + 1).is_err());
    assert!(range_local_ordinal(0, u32::MAX as u64 + 1).is_err());
}

fn document(path: &str, version: u64) -> DocumentRef {
    DocumentRef {
        path: path.into(),
        version,
    }
}

fn directory(sealed: SealedRun, sink: MemoryBlockSink) -> MemoryDirectory {
    assert_eq!(sealed.descriptor().level, BULK_OUTPUT_LEVEL);
    sink.directory_with_root(sealed.into_root())
}

async fn validate(kind: IndexKind, sealed: SealedRun, sink: MemoryBlockSink) {
    let directory = directory(sealed, sink);
    open_run(&directory, kind).await.unwrap();
}

#[tokio::test]
async fn every_public_kind_builds_one_direct_l1_base() {
    let options = BulkBuildOptions::new(SORT_CHUNK, 4).unwrap();
    let executor = TokioExecutor::default();

    let sink = MemoryBlockSink::default();
    let mut path = PathBulkBuilder::new(sink);
    path.push(IndexMutation::Upsert(PathDocument {
        document: document("a", 1),
    }))
    .await
    .unwrap();
    path.finish_range().await.unwrap();
    path.push(IndexMutation::Upsert(PathDocument {
        document: document("b", 2),
    }))
    .await
    .unwrap();
    let (sealed, sink) = path.finish().await.unwrap();
    let path_directory = directory(sealed.unwrap(), sink);
    let hits = PathEngine::query(
        &[path_directory],
        PathQuery {
            prefix: "",
            after_path: None,
            limit: 10,
        },
    )
    .await
    .unwrap();
    assert_eq!(hits, vec![document("a", 1), document("b", 2)]);

    let typed_definition = TypedJsonDefinition {
        fields: vec![TypedField {
            name: "state".into(),
            json_pointer: "/state".into(),
        }],
    };
    let fields = BTreeMap::from([("state".into(), vec![ScalarValue::String("ready".into())])]);

    let sink = MemoryBlockSink::default();
    let mut metadata =
        MetadataBulkBuilder::new(typed_definition.clone(), sink, executor.clone(), options)
            .unwrap();
    metadata
        .push(IndexMutation::Upsert(MetadataDocument {
            document: document("a", 1),
            fields: fields.clone(),
        }))
        .await
        .unwrap();
    let (sealed, sink) = metadata.finish().await.unwrap();
    validate(IndexKind::MetadataFilter, sealed.unwrap(), sink).await;

    let sink = MemoryBlockSink::default();
    let mut typed =
        TypedJsonBulkBuilder::new(typed_definition, sink, executor.clone(), options).unwrap();
    typed
        .push(IndexMutation::Upsert(TypedJsonDocument {
            document: document("a", 1),
            fields,
        }))
        .await
        .unwrap();
    let (sealed, sink) = typed.finish().await.unwrap();
    validate(IndexKind::TypedJson, sealed.unwrap(), sink).await;

    let sink = MemoryBlockSink::default();
    let mut full_text = FullTextBulkBuilder::new(sink, executor.clone(), options).unwrap();
    full_text
        .push(IndexMutation::Upsert(FullTextDocument {
            document: document("a", 1),
            fields: BTreeMap::from([("body".into(), "bounded index build".into())]),
        }))
        .await
        .unwrap();
    let (sealed, sink) = full_text.finish().await.unwrap();
    validate(IndexKind::FullText, sealed.unwrap(), sink).await;

    let vector_definition = VectorDefinition {
        dimension: 2,
        metric: VectorMetric::Cosine,
    };
    let sink = MemoryBlockSink::default();
    let mut vector = VectorBulkBuilder::new(vector_definition.clone(), sink).unwrap();
    vector
        .push(IndexMutation::Upsert(VectorDocument {
            document: document("a", 1),
            values: vec![1.0, 0.0],
        }))
        .await
        .unwrap();
    let (sealed, sink) = vector.finish().await.unwrap();
    validate(IndexKind::Vector, sealed.unwrap(), sink).await;

    let sink = MemoryBlockSink::default();
    let mut hybrid = HybridBulkBuilder::new(
        HybridDefinition {
            vector: vector_definition,
            text_weight: 1.0,
            vector_weight: 1.0,
        },
        sink,
        executor.clone(),
        options,
    )
    .unwrap();
    hybrid
        .push(IndexMutation::Upsert(HybridDocument {
            document: document("a", 1),
            text_fields: BTreeMap::from([("body".into(), "hybrid record".into())]),
            vector: vec![1.0, 0.0],
        }))
        .await
        .unwrap();
    let (sealed, sink) = hybrid.finish().await.unwrap();
    validate(IndexKind::Hybrid, sealed.unwrap(), sink).await;

    let sink = MemoryBlockSink::default();
    let mut git = GitSourceBulkBuilder::new(sink, executor.clone(), options).unwrap();
    git.push(IndexMutation::Upsert(GitSourceDocument {
        document: document("a", 1),
        records: vec![GitSourceRecord {
            repository_id: "repo".into(),
            commit_id: "commit".into(),
            tree_path: "src/lib.rs".into(),
            object_id: "object".into(),
            pack_path: "packs/1".into(),
            pack_version: 1,
            offset: 0,
            length: 10,
        }],
    }))
    .await
    .unwrap();
    let (sealed, sink) = git.finish().await.unwrap();
    validate(IndexKind::GitSource, sealed.unwrap(), sink).await;

    let sink = MemoryBlockSink::default();
    let mut tensor = TensorBulkBuilder::new(sink, executor, options).unwrap();
    tensor
        .push(IndexMutation::Upsert(TensorDocument {
            document: document("a", 1),
            records: vec![TensorRecord {
                model_id: "model".into(),
                tensor_name: "weight".into(),
                source_path: "model.bin".into(),
                source_version: 1,
                offset: 0,
                length: 8,
                dtype: "f32".into(),
                shape: vec![2],
            }],
        }))
        .await
        .unwrap();
    let (sealed, sink) = tensor.finish().await.unwrap();
    validate(IndexKind::Tensor, sealed.unwrap(), sink).await;
}

#[tokio::test]
async fn canonical_source_order_is_required_across_ranges() {
    let sink = MemoryBlockSink::default();
    let mut builder = PathBulkBuilder::new(sink);
    builder
        .push(IndexMutation::Upsert(PathDocument {
            document: document("b", 1),
        }))
        .await
        .unwrap();
    builder.finish_range().await.unwrap();
    assert!(
        builder
            .push(IndexMutation::Upsert(PathDocument {
                document: document("a", 2),
            }))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn large_single_text_field_streams_through_a_tiny_sort_chunk() {
    let sink = MemoryBlockSink::default();
    let options = BulkBuildOptions::new(8 * 1024, 4).unwrap();
    let mut builder = FullTextBulkBuilder::new(sink, TokioExecutor::default(), options).unwrap();
    let repetitions = 10_000;
    builder
        .push(IndexMutation::Upsert(FullTextDocument {
            document: document("large", 1),
            fields: BTreeMap::from([("body".into(), "repeat ".repeat(repetitions))]),
        }))
        .await
        .unwrap();
    let (sealed, sink) = builder.finish().await.unwrap();
    let directory = directory(sealed.unwrap(), sink);
    let fields = vec!["body".to_owned()];
    let hits = FullTextEngine::query(
        &[directory],
        FullTextQuery {
            text: "repeat",
            fields: &fields,
            phrase: false,
            match_all_terms: true,
            limit: 10,
        },
    )
    .await
    .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].document, document("large", 1));
}
