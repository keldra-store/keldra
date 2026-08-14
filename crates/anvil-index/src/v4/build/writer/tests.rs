use super::*;
use crate::FIXED_INDEX_SEAL_WORKSPACE_BYTES;
use crate::v4::build::{ExactMemorySink, ProjectedTerm, ProjectedVector};
use crate::v4::{
    Analyzer, Cardinality, Collation, ComponentVersion, FieldCapabilities, FieldSchema, FieldType,
    IndexKind, ObjectIdentity, VectorMetric, VectorNormalization,
};

const TEST_TOTAL_BYTES: usize = 64 * 1024 * 1024;

fn version(component_kind: ComponentKind) -> ComponentVersion {
    ComponentVersion {
        component_kind,
        codec_version: if component_kind == ComponentKind::IDENTITY_TABLE {
            2
        } else {
            1
        },
    }
}

fn text_schema() -> Schema {
    Schema {
        kind: IndexKind::FullText,
        path_prefix: "/objects".into(),
        content_type_scope: Some("application/json".into()),
        fields: vec![FieldSchema {
            id: FieldId::new(0),
            name: "body".into(),
            source_selector: "/body".into(),
            field_type: FieldType::Text,
            cardinality: Cardinality::Single,
            allow_missing: true,
            allow_null: false,
            collation: Collation::BinaryUtf8,
            capabilities: FieldCapabilities::FULL_TEXT,
            analyzer: Some(Analyzer::UnicodeAlphanumericLowercase),
            components: FieldComponents::TERMS
                .union(FieldComponents::POSITIONS)
                .union(FieldComponents::NORMS),
        }],
        semantics: IndexSemantics::FullText {
            analyzer: Analyzer::UnicodeAlphanumericLowercase,
            bm25_k1: 1.2,
            bm25_b: 0.75,
        },
        physical_order: Vec::new(),
        component_versions: vec![
            version(ComponentKind::ROUTING_NODE),
            version(ComponentKind::IDENTITY_TABLE),
            version(ComponentKind::LIVE_MASK),
            version(ComponentKind::PATH_LOCATOR),
            version(ComponentKind::TERM_DICTIONARY),
            version(ComponentKind::POSTINGS),
            version(ComponentKind::POSITIONS),
            version(ComponentKind::NORMS),
            version(ComponentKind::SCORING_STATISTICS),
        ],
    }
}

fn limits() -> BuildLimits {
    BuildLimits::with_resident_limits(
        TEST_TOTAL_BYTES,
        TEST_TOTAL_BYTES - FIXED_INDEX_SEAL_WORKSPACE_BYTES,
        FIXED_INDEX_SEAL_WORKSPACE_BYTES,
    )
    .unwrap()
}

fn identity(schema: &Schema) -> SegmentIdentity {
    SegmentIdentity::new(7, 1, schema.fingerprint().unwrap(), 11).unwrap()
}

fn source(path: String, records: Vec<ProjectedRecord>) -> ProjectedSource {
    ProjectedSource {
        source_identity: ObjectIdentity { path, version: 1 },
        records,
    }
}

fn text_record(order: &str, term: String) -> ProjectedRecord {
    ProjectedRecord {
        result_identity: None,
        order_key: order.as_bytes().to_vec(),
        terms: vec![ProjectedTerm {
            field_id: FieldId::new(0),
            term_type: crate::v4::TERM_TYPE_TEXT,
            term: term.clone().into_bytes(),
            frequency: 1,
            positions: vec![1],
        }],
        points: Vec::new(),
        doc_values: Vec::new(),
        vectors: Vec::new(),
        field_lengths: vec![(FieldId::new(0), 1)],
    }
}

#[tokio::test]
async fn build_is_deterministic_with_source_identity_retained_once() {
    let schema = text_schema();
    let long_path = "p".repeat(4096);
    let left = source(
        long_path,
        vec![
            text_record("b", "fixed".into()),
            text_record("a", "active".into()),
        ],
    );
    let right = source("short".into(), vec![text_record("c", "pending".into())]);
    let build = |ordered: Vec<ProjectedSource>| {
        let schema = schema.clone();
        async move {
            let mut writer = NativeSegmentWriter::new(identity(&schema), schema, limits()).unwrap();
            for source in ordered {
                assert_eq!(writer.push_source(source).unwrap(), SourcePush::Accepted);
            }
            let mut sink = ExactMemorySink::new();
            let built = writer.seal(&mut sink).await.unwrap();
            (built, sink)
        }
    };
    let (forward, forward_sink) = build(vec![left.clone(), right.clone()]).await;
    let (reverse, reverse_sink) = build(vec![right, left]).await;
    assert_eq!(forward.descriptor, reverse.descriptor);
    assert_eq!(forward.locator, reverse.locator);
    assert_eq!(forward_sink.objects(), reverse_sink.objects());
}

#[test]
fn source_admission_charges_flat_document_and_term_references_before_allocation() {
    let schema = text_schema();
    let maximum = 512 * 1024;
    let total = maximum + FIXED_INDEX_SEAL_WORKSPACE_BYTES;
    let limits =
        BuildLimits::with_resident_limits(total, maximum, FIXED_INDEX_SEAL_WORKSPACE_BYTES)
            .unwrap();
    let mut writer = NativeSegmentWriter::new(identity(&schema), schema, limits).unwrap();
    let make = |path: &str| {
        source(
            path.into(),
            (0..64)
                .map(|ordinal| {
                    text_record(&format!("{ordinal:04}"), format!("{ordinal:04}-{}", "x".repeat(4096)))
                })
                .collect(),
        )
    };
    assert_eq!(writer.push_source(make("a")).unwrap(), SourcePush::Accepted);
    let before_sources = writer.source_count();
    let before_bytes = writer.buffered_source_bytes();
    assert!(matches!(
        writer.push_source(make("b")).unwrap(),
        SourcePush::Full(_)
    ));
    assert_eq!(writer.source_count(), before_sources);
    assert_eq!(writer.buffered_source_bytes(), before_bytes);
    assert!(before_bytes <= maximum);
}

#[test]
fn statistics_limit_is_enforced_before_writer_buffers_are_allocated() {
    let mut schema = text_schema();
    schema.kind = IndexKind::TypedJson;
    schema.semantics = IndexSemantics::TypedJson;
    schema.physical_order.clear();
    schema.fields = (0..6_000)
        .map(|ordinal| FieldSchema {
            id: FieldId::new(ordinal),
            name: format!("field-{ordinal}"),
            source_selector: format!("/field-{ordinal}"),
            field_type: FieldType::SignedInteger,
            cardinality: Cardinality::Single,
            allow_missing: true,
            allow_null: false,
            collation: Collation::BinaryUtf8,
            capabilities: FieldCapabilities::EXACT
                .union(FieldCapabilities::RANGE)
                .union(FieldCapabilities::ORDER)
                .union(FieldCapabilities::FACET)
                .union(FieldCapabilities::AGGREGATE),
            analyzer: None,
            components: FieldComponents::POINTS.union(FieldComponents::DOC_VALUES),
        })
        .collect();
    // The oversized schema cannot produce its own fingerprint by design. A
    // syntactically valid identity lets construction reach that admission
    // check without weakening the non-zero identity invariant.
    let segment_identity = SegmentIdentity::new(7, 1, [1; 32], 11).unwrap();
    let error = match NativeSegmentWriter::new(segment_identity, schema, limits()) {
        Ok(_) => panic!("oversized statistics schema must fail writer construction"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        IndexError::ResourceLimit { needed, limit }
            if needed > limit && limit == crate::v4::INDEX_COMPONENT_BYTES
    ));
}

#[test]
fn writer_preallocates_the_schema_charged_assembly_vectors() {
    let schema = text_schema();
    let shape = schema.segment_shape().unwrap();
    let assembly = SegmentAssembly::new(&schema).unwrap();
    assert_eq!(
        assembly.components.capacity(),
        charged_vec_capacity(0, shape.component_count).unwrap()
    );
    assert_eq!(
        assembly.component_statistics.capacity(),
        charged_vec_capacity(0, shape.component_statistics_count).unwrap()
    );
    let charge = WriterCharge::for_schema(&schema).unwrap();
    let field_capacity = charged_vec_capacity(0, shape.field_count).unwrap();
    let component_capacity = charged_vec_capacity(0, shape.component_count).unwrap();
    let statistics_capacity = charged_vec_capacity(0, shape.component_statistics_count).unwrap();
    let fields = field_capacity * std::mem::size_of::<crate::v4::FieldStatistics>();
    let seen = field_capacity * std::mem::size_of::<u32>();
    let assembly = component_capacity * std::mem::size_of::<SegmentComponent>()
        + statistics_capacity * std::mem::size_of::<ComponentStatistics>();
    assert_eq!(charge.peak_bytes().unwrap(), fields + seen.max(assembly));
}

#[test]
fn source_path_index_uses_owned_sources_and_rejects_duplicates() {
    let schema = text_schema();
    let mut writer = NativeSegmentWriter::new(identity(&schema), schema, limits()).unwrap();
    assert_eq!(
        writer
            .push_source(source(
                "z/path".into(),
                vec![text_record("z", "z".into())]
            ))
            .unwrap(),
        SourcePush::Accepted
    );
    assert_eq!(
        writer
            .push_source(source(
                "a/path".into(),
                vec![text_record("a", "a".into())]
            ))
            .unwrap(),
        SourcePush::Accepted
    );
    assert_eq!(writer.source_version("a/path"), Some(1));
    assert_eq!(writer.source_version("z/path"), Some(1));
    assert_eq!(writer.source_version("missing"), None);
    assert!(
        writer
            .push_source(source(
                "a/path".into(),
                vec![text_record("duplicate", "duplicate".into())]
            ))
            .is_err()
    );
}

#[tokio::test]
async fn high_cardinality_positions_and_large_values_seal_in_bounded_blocks() {
    let schema = text_schema();
    let records = (0..2048)
        .map(|ordinal| {
            let mut record = text_record(
                &format!("{ordinal:08}"),
                format!("term-{ordinal:08}-{}", "v".repeat(4096)),
            );
            record.terms[0].frequency = 16;
            record.terms[0].positions = (0..16).collect();
            record
        })
        .collect();
    let mut writer = NativeSegmentWriter::new(identity(&schema), schema, limits()).unwrap();
    assert_eq!(
        writer.push_source(source("bulk".into(), records)).unwrap(),
        SourcePush::Accepted
    );
    let mut sink = ExactMemorySink::new();
    let built = writer.seal(&mut sink).await.unwrap();
    assert_eq!(built.descriptor.document_count, 2048);
    assert!(
        built
            .descriptor
            .components
            .iter()
            .any(|component| component.role == ComponentKind::POSITIONS)
    );
}

#[tokio::test]
async fn high_dimension_vectors_stream_one_component_block_at_a_time() {
    let mut schema = text_schema();
    schema.kind = IndexKind::Vector;
    schema.semantics = IndexSemantics::Vector {
        dimensions: 1024,
        metric: VectorMetric::Cosine,
        normalization: VectorNormalization::L2,
    };
    schema.fields[0].field_type = FieldType::Vector;
    schema.fields[0].capabilities = FieldCapabilities::empty();
    schema.fields[0].analyzer = None;
    schema.fields[0].components = FieldComponents::VECTOR;
    schema.component_versions = vec![
        version(ComponentKind::ROUTING_NODE),
        version(ComponentKind::IDENTITY_TABLE),
        version(ComponentKind::LIVE_MASK),
        version(ComponentKind::PATH_LOCATOR),
        version(ComponentKind::VECTORS),
        version(ComponentKind::SCORING_STATISTICS),
    ];
    let records = (0..512)
        .map(|ordinal| ProjectedRecord {
            result_identity: None,
            order_key: format!("{ordinal:08}").into_bytes(),
            terms: Vec::new(),
            points: Vec::new(),
            doc_values: Vec::new(),
            vectors: vec![ProjectedVector {
                field_id: FieldId::new(0),
                values: vec![1.0 / 32.0; 1024],
            }],
            field_lengths: Vec::new(),
        })
        .collect();
    let mut writer = NativeSegmentWriter::new(identity(&schema), schema, limits()).unwrap();
    assert_eq!(
        writer
            .push_source(source("vectors".into(), records))
            .unwrap(),
        SourcePush::Accepted
    );
    let mut sink = ExactMemorySink::new();
    let built = writer.seal(&mut sink).await.unwrap();
    assert_eq!(built.descriptor.document_count, 512);
}
