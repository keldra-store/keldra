//! Canonical compilation of the public index definition into format-v4 schema.

use std::collections::BTreeSet;

use anvil_api::v1::index_specification::Specification;
use anvil_api::v1::{
    FullTextIndexSpec, HybridIndexSpec, IndexOrderDirection, IndexSpecification,
    MetadataFilterIndexSpec, TypedJsonIndexSpec, VectorIndexSpec, VectorMetric as ApiVectorMetric,
};
use anvil_index::IndexError;
use anvil_index::v4::{
    Analyzer, Cardinality, Collation, ComponentKind, ComponentVersion, FieldComponents, FieldId,
    FieldSchema, IndexKind, IndexSemantics, OrderDirection, OrderField,
    STORED_FIELDS_COMPONENT_CODEC_VERSION, ScalarDomain, Schema, VectorMetric, VectorNormalization,
};

const COMPONENT_CODEC_VERSION: u16 = 1;
const IDENTITY_COMPONENT_CODEC_VERSION: u16 = 2;
const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;

const TERMS_AND_COLUMN: FieldComponents =
    FieldComponents::TERMS.union(FieldComponents::FAST_COLUMN);
const TERMS_COLUMN_AND_STORED: FieldComponents = TERMS_AND_COLUMN.union(FieldComponents::STORED);
const FULL_TEXT_COMPONENTS: FieldComponents = FieldComponents::TERMS
    .union(FieldComponents::POSITIONS)
    .union(FieldComponents::NORMS)
    .union(FieldComponents::STORED);

/// Compile one already-authorized public definition into its complete,
/// deterministic format-v4 schema. Field IDs are definition-local and dense;
/// no registry or observed source value participates.
pub(crate) fn compile_schema(
    path_prefix: &str,
    content_type_scope: Option<&str>,
    specification: &IndexSpecification,
) -> Result<Schema, IndexError> {
    let (kind, fields, semantics, physical_order) = match specification.specification.as_ref() {
        Some(Specification::Path(_)) => path_schema()?,
        Some(Specification::MetadataFilter(specification)) => metadata_schema(specification)?,
        Some(Specification::TypedJson(specification)) => typed_json_schema(specification)?,
        Some(Specification::FullText(specification)) => full_text_schema(specification)?,
        Some(Specification::Vector(specification)) => vector_schema(specification)?,
        Some(Specification::Hybrid(specification)) => hybrid_schema(specification)?,
        Some(Specification::GitSource(specification)) => git_schema(&specification.repository_id)?,
        Some(Specification::Tensor(specification)) => tensor_schema(&specification.model_id)?,
        None => {
            return Err(IndexError::InvalidDefinition(
                "index specification is required".into(),
            ));
        }
    };
    let schema = Schema {
        kind,
        path_prefix: path_prefix.to_owned(),
        content_type_scope: content_type_scope.map(str::to_owned),
        component_versions: component_versions(&fields),
        fields,
        semantics,
        physical_order,
    };
    schema.validate()?;
    Ok(schema)
}

type SchemaParts = (IndexKind, Vec<FieldSchema>, IndexSemantics, Vec<OrderField>);

fn path_schema() -> Result<SchemaParts, IndexError> {
    Ok((
        IndexKind::Path,
        vec![field(
            0,
            "path",
            "@object/path",
            ScalarDomain::STRING,
            Cardinality::Single,
            false,
            false,
            FieldComponents::TERMS,
        )?],
        IndexSemantics::Path,
        Vec::new(),
    ))
}

fn metadata_schema(specification: &MetadataFilterIndexSpec) -> Result<SchemaParts, IndexError> {
    let fields = specification
        .fields
        .iter()
        .enumerate()
        .map(|(ordinal, name)| {
            let (domain, allow_null) = metadata_domain(name)?;
            field(
                ordinal,
                name,
                &format!("@head/{name}"),
                domain,
                Cardinality::Single,
                false,
                allow_null,
                TERMS_COLUMN_AND_STORED,
            )
        })
        .collect::<Result<Vec<_>, IndexError>>()?;
    Ok((
        IndexKind::MetadataFilter,
        fields,
        IndexSemantics::MetadataFilter,
        Vec::new(),
    ))
}

fn metadata_domain(name: &str) -> Result<(ScalarDomain, bool), IndexError> {
    match name {
        "path" | "content_hash" => Ok((ScalarDomain::STRING, false)),
        "content_type" => Ok((ScalarDomain::STRING.union(ScalarDomain::NULL), true)),
        "version" | "content_length" | "committed_at_unix_millis" => {
            Ok((ScalarDomain::UNSIGNED, false))
        }
        _ => Err(IndexError::InvalidDefinition(
            "metadata index contains an unsupported object-head field".into(),
        )),
    }
}

fn typed_json_schema(specification: &TypedJsonIndexSpec) -> Result<SchemaParts, IndexError> {
    let fields = specification
        .fields
        .iter()
        .enumerate()
        .map(|(ordinal, value)| {
            field(
                ordinal,
                &value.name,
                &value.json_pointer,
                ScalarDomain::ALL_JSON,
                if value.multi_valued {
                    Cardinality::Multi
                } else {
                    Cardinality::Single
                },
                true,
                true,
                TERMS_COLUMN_AND_STORED,
            )
        })
        .collect::<Result<Vec<_>, IndexError>>()?;
    let physical_order = specification
        .physical_order
        .iter()
        .map(|order| {
            let ordinal = fields
                .iter()
                .position(|field| field.name == order.field)
                .ok_or_else(|| {
                    IndexError::InvalidDefinition(
                        "physical order names an unknown typed JSON field".into(),
                    )
                })?;
            let direction = match IndexOrderDirection::try_from(order.direction)
                .map_err(|_| IndexError::InvalidDefinition("unknown order direction".into()))?
            {
                IndexOrderDirection::Ascending => OrderDirection::Ascending,
                IndexOrderDirection::Descending => OrderDirection::Descending,
            };
            Ok(OrderField {
                field_id: FieldId::new(
                    u32::try_from(ordinal).map_err(|_| IndexError::OffsetOverflow)?,
                ),
                direction,
            })
        })
        .collect::<Result<Vec<_>, IndexError>>()?;
    Ok((
        IndexKind::TypedJson,
        fields,
        IndexSemantics::TypedJson,
        physical_order,
    ))
}

fn full_text_schema(specification: &FullTextIndexSpec) -> Result<SchemaParts, IndexError> {
    let fields = text_fields(specification)?;
    Ok((
        IndexKind::FullText,
        fields,
        IndexSemantics::FullText {
            analyzer: Analyzer::UnicodeAlphanumericLowercase,
            bm25_k1: BM25_K1,
            bm25_b: BM25_B,
        },
        Vec::new(),
    ))
}

fn text_fields(specification: &FullTextIndexSpec) -> Result<Vec<FieldSchema>, IndexError> {
    specification
        .fields
        .iter()
        .enumerate()
        .map(|(ordinal, value)| {
            field(
                ordinal,
                &value.name,
                &value.json_pointer,
                ScalarDomain::STRING,
                Cardinality::Single,
                true,
                false,
                FULL_TEXT_COMPONENTS,
            )
        })
        .collect()
}

fn vector_schema(specification: &VectorIndexSpec) -> Result<SchemaParts, IndexError> {
    Ok((
        IndexKind::Vector,
        vec![vector_field(0, "vector", specification)?],
        vector_semantics(specification)?,
        Vec::new(),
    ))
}

fn vector_field(
    ordinal: usize,
    name: &str,
    specification: &VectorIndexSpec,
) -> Result<FieldSchema, IndexError> {
    field(
        ordinal,
        name,
        &specification.json_pointer,
        ScalarDomain::NUMBER,
        Cardinality::Multi,
        false,
        false,
        FieldComponents::VECTOR,
    )
}

fn vector_semantics(specification: &VectorIndexSpec) -> Result<IndexSemantics, IndexError> {
    let metric = match ApiVectorMetric::try_from(specification.metric)
        .map_err(|_| IndexError::InvalidDefinition("unknown vector metric".into()))?
    {
        ApiVectorMetric::Cosine => VectorMetric::Cosine,
        ApiVectorMetric::Dot => VectorMetric::DotProduct,
        ApiVectorMetric::Euclidean => VectorMetric::Euclidean,
    };
    Ok(IndexSemantics::Vector {
        dimensions: specification.dimensions,
        metric,
        normalization: if specification.normalize {
            VectorNormalization::L2
        } else {
            VectorNormalization::None
        },
    })
}

fn hybrid_schema(specification: &HybridIndexSpec) -> Result<SchemaParts, IndexError> {
    let text = specification
        .full_text
        .as_ref()
        .ok_or_else(|| IndexError::InvalidDefinition("hybrid full-text spec is required".into()))?;
    let vector = specification
        .vector
        .as_ref()
        .ok_or_else(|| IndexError::InvalidDefinition("hybrid vector spec is required".into()))?;
    let mut fields = text_fields(text)?;
    let vector_name = unique_internal_name("@vector", &fields);
    fields.push(vector_field(fields.len(), &vector_name, vector)?);
    let IndexSemantics::Vector {
        dimensions,
        metric,
        normalization,
    } = vector_semantics(vector)?
    else {
        unreachable!();
    };
    Ok((
        IndexKind::Hybrid,
        fields,
        IndexSemantics::Hybrid {
            analyzer: Analyzer::UnicodeAlphanumericLowercase,
            bm25_k1: BM25_K1,
            bm25_b: BM25_B,
            dimensions,
            metric,
            normalization,
            lexical_weight: effective_weight(specification.full_text_weight),
            vector_weight: effective_weight(specification.vector_weight),
        },
        Vec::new(),
    ))
}

fn unique_internal_name(base: &str, fields: &[FieldSchema]) -> String {
    let mut name = base.to_owned();
    while fields.iter().any(|field| field.name == name) {
        name.push('@');
    }
    name
}

fn effective_weight(value: f32) -> f64 {
    if value == 0.0 { 1.0 } else { f64::from(value) }
}

fn git_schema(repository_scope: &str) -> Result<SchemaParts, IndexError> {
    let definitions = [
        (
            "repository_id",
            ScalarDomain::STRING,
            TERMS_COLUMN_AND_STORED,
        ),
        ("commit_id", ScalarDomain::STRING, TERMS_COLUMN_AND_STORED),
        ("tree_path", ScalarDomain::STRING, TERMS_COLUMN_AND_STORED),
        ("object_id", ScalarDomain::STRING, TERMS_COLUMN_AND_STORED),
        (
            "pack_path",
            ScalarDomain::STRING,
            FieldComponents::FAST_COLUMN.union(FieldComponents::STORED),
        ),
        (
            "pack_version",
            ScalarDomain::UNSIGNED,
            FieldComponents::FAST_COLUMN.union(FieldComponents::STORED),
        ),
        (
            "offset",
            ScalarDomain::UNSIGNED,
            FieldComponents::FAST_COLUMN.union(FieldComponents::STORED),
        ),
        (
            "length",
            ScalarDomain::UNSIGNED,
            FieldComponents::FAST_COLUMN.union(FieldComponents::STORED),
        ),
    ];
    let fields = fixed_fields("@git", &definitions)?;
    Ok((
        IndexKind::GitSource,
        fields,
        IndexSemantics::GitSource {
            repository_scope: repository_scope.to_owned(),
        },
        Vec::new(),
    ))
}

fn tensor_schema(model_scope: &str) -> Result<SchemaParts, IndexError> {
    let definitions = [
        ("model_id", ScalarDomain::STRING, TERMS_COLUMN_AND_STORED),
        ("tensor_name", ScalarDomain::STRING, TERMS_COLUMN_AND_STORED),
        (
            "source_path",
            ScalarDomain::STRING,
            FieldComponents::FAST_COLUMN.union(FieldComponents::STORED),
        ),
        (
            "source_version",
            ScalarDomain::UNSIGNED,
            FieldComponents::FAST_COLUMN.union(FieldComponents::STORED),
        ),
        (
            "offset",
            ScalarDomain::UNSIGNED,
            FieldComponents::FAST_COLUMN.union(FieldComponents::STORED),
        ),
        (
            "length",
            ScalarDomain::UNSIGNED,
            FieldComponents::FAST_COLUMN.union(FieldComponents::STORED),
        ),
        (
            "dtype",
            ScalarDomain::STRING,
            FieldComponents::FAST_COLUMN.union(FieldComponents::STORED),
        ),
    ];
    let mut fields = fixed_fields("@tensor", &definitions)?;
    fields.push(field(
        fields.len(),
        "shape",
        "@tensor/shape",
        ScalarDomain::UNSIGNED,
        Cardinality::Multi,
        false,
        false,
        FieldComponents::FAST_COLUMN.union(FieldComponents::STORED),
    )?);
    Ok((
        IndexKind::Tensor,
        fields,
        IndexSemantics::Tensor {
            model_scope: model_scope.to_owned(),
        },
        Vec::new(),
    ))
}

fn fixed_fields(
    selector_prefix: &str,
    definitions: &[(&str, ScalarDomain, FieldComponents)],
) -> Result<Vec<FieldSchema>, IndexError> {
    definitions
        .iter()
        .enumerate()
        .map(|(ordinal, (name, domain, components))| {
            field(
                ordinal,
                name,
                &format!("{selector_prefix}/{name}"),
                *domain,
                Cardinality::Single,
                false,
                false,
                *components,
            )
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn field(
    ordinal: usize,
    name: &str,
    source_selector: &str,
    domain: ScalarDomain,
    cardinality: Cardinality,
    allow_missing: bool,
    allow_null: bool,
    components: FieldComponents,
) -> Result<FieldSchema, IndexError> {
    Ok(FieldSchema {
        id: FieldId::new(u32::try_from(ordinal).map_err(|_| IndexError::OffsetOverflow)?),
        name: name.to_owned(),
        source_selector: source_selector.to_owned(),
        domain,
        cardinality,
        allow_missing,
        allow_null,
        collation: Collation::BinaryUtf8,
        components,
    })
}

fn component_versions(fields: &[FieldSchema]) -> Vec<ComponentVersion> {
    let mut kinds = BTreeSet::from([
        ComponentKind::SEGMENT_ROOT,
        ComponentKind::ROUTING_NODE,
        ComponentKind::IDENTITY_TABLE,
        ComponentKind::LIVE_MASK,
        ComponentKind::PATH_LOCATOR,
        ComponentKind::SCORING_STATISTICS,
        ComponentKind::GENERATION_MANIFEST,
    ]);
    for field in fields {
        if field.components.contains(FieldComponents::TERMS) {
            kinds.insert(ComponentKind::TERM_DICTIONARY);
            kinds.insert(ComponentKind::POSTINGS);
        }
        if field.components.contains(FieldComponents::FAST_COLUMN) {
            kinds.insert(ComponentKind::FAST_COLUMN);
        }
        if field.components.contains(FieldComponents::STORED) {
            kinds.insert(ComponentKind::STORED_FIELDS);
        }
        if field.components.contains(FieldComponents::POSITIONS) {
            kinds.insert(ComponentKind::POSITIONS);
        }
        if field.components.contains(FieldComponents::NORMS) {
            kinds.insert(ComponentKind::NORMS);
        }
        if field.components.contains(FieldComponents::VECTOR) {
            kinds.insert(ComponentKind::VECTORS);
        }
    }
    kinds
        .into_iter()
        .map(|component_kind| ComponentVersion {
            component_kind,
            codec_version: if component_kind == ComponentKind::IDENTITY_TABLE {
                IDENTITY_COMPONENT_CODEC_VERSION
            } else if component_kind == ComponentKind::STORED_FIELDS {
                STORED_FIELDS_COMPONENT_CODEC_VERSION
            } else {
                COMPONENT_CODEC_VERSION
            },
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use anvil_api::v1::index_specification::Specification as Spec;
    use anvil_api::v1::{
        FullTextField, GitSourceIndexSpec, IndexField, IndexOrder, MetadataFilterIndexSpec,
        PathIndexSpec, TensorIndexSpec, TypedJsonIndexSpec, VectorIndexSpec,
    };

    use super::*;

    fn spec(value: Spec) -> IndexSpecification {
        IndexSpecification {
            specification: Some(value),
        }
    }

    fn text() -> FullTextIndexSpec {
        FullTextIndexSpec {
            fields: vec![FullTextField {
                name: "body".into(),
                json_pointer: "/body".into(),
            }],
        }
    }

    fn vector() -> VectorIndexSpec {
        VectorIndexSpec {
            json_pointer: "/embedding".into(),
            dimensions: 3,
            metric: ApiVectorMetric::Cosine as i32,
            normalize: true,
        }
    }

    fn all_kinds() -> Vec<(&'static str, IndexSpecification, IndexKind)> {
        vec![
            ("path", spec(Spec::Path(PathIndexSpec {})), IndexKind::Path),
            (
                "metadata",
                spec(Spec::MetadataFilter(MetadataFilterIndexSpec {
                    fields: vec!["content_type".into(), "content_length".into()],
                })),
                IndexKind::MetadataFilter,
            ),
            (
                "typed",
                spec(Spec::TypedJson(TypedJsonIndexSpec {
                    fields: vec![
                        IndexField {
                            name: "modified".into(),
                            json_pointer: "/modified".into(),
                            multi_valued: false,
                        },
                        IndexField {
                            name: "ecosystems".into(),
                            json_pointer: "/ecosystems".into(),
                            multi_valued: true,
                        },
                    ],
                    physical_order: vec![IndexOrder {
                        field: "modified".into(),
                        direction: IndexOrderDirection::Descending as i32,
                    }],
                })),
                IndexKind::TypedJson,
            ),
            (
                "full-text",
                spec(Spec::FullText(text())),
                IndexKind::FullText,
            ),
            ("vector", spec(Spec::Vector(vector())), IndexKind::Vector),
            (
                "hybrid",
                spec(Spec::Hybrid(HybridIndexSpec {
                    full_text: Some(text()),
                    vector: Some(vector()),
                    full_text_weight: 0.25,
                    vector_weight: 0.75,
                })),
                IndexKind::Hybrid,
            ),
            (
                "git",
                spec(Spec::GitSource(GitSourceIndexSpec {
                    repository_id: "repo".into(),
                })),
                IndexKind::GitSource,
            ),
            (
                "tensor",
                spec(Spec::Tensor(TensorIndexSpec {
                    model_id: "model".into(),
                })),
                IndexKind::Tensor,
            ),
        ]
    }

    #[test]
    fn every_public_kind_compiles_to_dense_valid_canonical_schema() {
        let mut fingerprints = BTreeMap::new();
        for (name, specification, expected_kind) in all_kinds() {
            let schema =
                compile_schema("tenant/42/", Some("application/json"), &specification).unwrap();
            assert_eq!(schema.kind, expected_kind, "{name}");
            assert_eq!(schema.path_prefix, "tenant/42/");
            assert_eq!(
                schema.content_type_scope.as_deref(),
                Some("application/json")
            );
            assert_eq!(
                schema
                    .fields
                    .iter()
                    .map(|field| field.id.get())
                    .collect::<Vec<_>>(),
                (0..schema.fields.len() as u32).collect::<Vec<_>>(),
                "{name}"
            );
            assert!(schema.validate().is_ok(), "{name}");
            assert!(
                schema
                    .component_versions
                    .windows(2)
                    .all(|pair| { pair[0].component_kind < pair[1].component_kind })
            );
            assert!(
                schema.component_versions.iter().all(|version| {
                    version.codec_version
                        == if version.component_kind == ComponentKind::IDENTITY_TABLE {
                            IDENTITY_COMPONENT_CODEC_VERSION
                        } else if version.component_kind == ComponentKind::STORED_FIELDS {
                            STORED_FIELDS_COMPONENT_CODEC_VERSION
                        } else {
                            COMPONENT_CODEC_VERSION
                        }
                }),
                "{name}"
            );
            assert_eq!(
                schema.fingerprint().unwrap(),
                compile_schema("tenant/42/", Some("application/json"), &specification)
                    .unwrap()
                    .fingerprint()
                    .unwrap(),
                "{name}"
            );
            assert!(
                fingerprints
                    .insert(schema.fingerprint().unwrap(), name)
                    .is_none(),
                "{name} fingerprint collided"
            );
        }
    }

    #[test]
    fn typed_cardinality_and_physical_order_are_exact_schema_semantics() {
        let (_, specification, _) = all_kinds()
            .into_iter()
            .find(|(name, _, _)| *name == "typed")
            .unwrap();
        let schema = compile_schema("", None, &specification).unwrap();
        assert_eq!(schema.fields[0].cardinality, Cardinality::Single);
        assert_eq!(schema.fields[1].cardinality, Cardinality::Multi);
        assert_eq!(
            schema.physical_order,
            vec![OrderField {
                field_id: FieldId::new(0),
                direction: OrderDirection::Descending,
            }]
        );

        let mut cardinality = specification.clone();
        let Some(Spec::TypedJson(value)) = cardinality.specification.as_mut() else {
            unreachable!();
        };
        value.fields[1].multi_valued = false;
        assert_ne!(
            schema.fingerprint().unwrap(),
            compile_schema("", None, &cardinality)
                .unwrap()
                .fingerprint()
                .unwrap()
        );

        let mut order = specification;
        let Some(Spec::TypedJson(value)) = order.specification.as_mut() else {
            unreachable!();
        };
        value.physical_order[0].direction = IndexOrderDirection::Ascending as i32;
        assert_ne!(
            schema.fingerprint().unwrap(),
            compile_schema("", None, &order)
                .unwrap()
                .fingerprint()
                .unwrap()
        );
    }

    #[test]
    fn every_result_affecting_scope_and_kind_semantic_changes_the_fingerprint() {
        for (name, specification, _) in all_kinds() {
            let base = compile_schema("scope/", Some("application/json"), &specification)
                .unwrap()
                .fingerprint()
                .unwrap();
            assert_ne!(
                base,
                compile_schema("other/", Some("application/json"), &specification)
                    .unwrap()
                    .fingerprint()
                    .unwrap(),
                "{name} path scope"
            );
            assert_ne!(
                base,
                compile_schema("scope/", Some("application/cbor"), &specification)
                    .unwrap()
                    .fingerprint()
                    .unwrap(),
                "{name} content scope"
            );

            let changed = semantic_variant(&specification);
            assert_ne!(
                base,
                compile_schema("scope/", Some("application/json"), &changed)
                    .unwrap()
                    .fingerprint()
                    .unwrap(),
                "{name} kind semantic"
            );
        }
    }

    fn semantic_variant(specification: &IndexSpecification) -> IndexSpecification {
        let mut changed = specification.clone();
        match changed.specification.as_mut().unwrap() {
            Spec::Path(_) => {
                return spec(Spec::MetadataFilter(MetadataFilterIndexSpec {
                    fields: vec!["path".into()],
                }));
            }
            Spec::MetadataFilter(value) => value.fields.push("version".into()),
            Spec::TypedJson(value) => value.fields[0].json_pointer = "/updated".into(),
            Spec::FullText(value) => value.fields[0].json_pointer = "/title".into(),
            Spec::Vector(value) => value.dimensions += 1,
            Spec::Hybrid(value) => value.vector_weight = 0.5,
            Spec::GitSource(value) => value.repository_id.push_str("-other"),
            Spec::Tensor(value) => value.model_id.push_str("-other"),
        }
        changed
    }

    #[test]
    fn component_catalogue_is_complete_for_each_declared_field_capability() {
        for (name, specification, _) in all_kinds() {
            let schema = compile_schema("", None, &specification).unwrap();
            let kinds = schema
                .component_versions
                .iter()
                .map(|version| version.component_kind)
                .collect::<BTreeSet<_>>();
            for common in [
                ComponentKind::SEGMENT_ROOT,
                ComponentKind::ROUTING_NODE,
                ComponentKind::IDENTITY_TABLE,
                ComponentKind::LIVE_MASK,
                ComponentKind::PATH_LOCATOR,
                ComponentKind::SCORING_STATISTICS,
                ComponentKind::GENERATION_MANIFEST,
            ] {
                assert!(kinds.contains(&common), "{name}");
            }
            for field in &schema.fields {
                for (flag, required) in [
                    (FieldComponents::TERMS, ComponentKind::TERM_DICTIONARY),
                    (FieldComponents::TERMS, ComponentKind::POSTINGS),
                    (FieldComponents::FAST_COLUMN, ComponentKind::FAST_COLUMN),
                    (FieldComponents::STORED, ComponentKind::STORED_FIELDS),
                    (FieldComponents::POSITIONS, ComponentKind::POSITIONS),
                    (FieldComponents::NORMS, ComponentKind::NORMS),
                    (FieldComponents::VECTOR, ComponentKind::VECTORS),
                ] {
                    if field.components.contains(flag) {
                        assert!(kinds.contains(&required), "{name}");
                    }
                }
            }
        }
    }

    #[test]
    fn fixed_and_declared_field_catalogues_have_exact_v4_capabilities() {
        let schemas = all_kinds()
            .into_iter()
            .map(|(name, specification, _)| {
                (name, compile_schema("", None, &specification).unwrap())
            })
            .collect::<BTreeMap<_, _>>();

        assert_field(
            &schemas["path"].fields[0],
            0,
            "path",
            "@object/path",
            ScalarDomain::STRING,
            Cardinality::Single,
            FieldComponents::TERMS,
        );

        let metadata = &schemas["metadata"].fields;
        assert_field(
            &metadata[0],
            0,
            "content_type",
            "@head/content_type",
            ScalarDomain::STRING.union(ScalarDomain::NULL),
            Cardinality::Single,
            TERMS_COLUMN_AND_STORED,
        );
        assert!(metadata[0].allow_null);
        assert_field(
            &metadata[1],
            1,
            "content_length",
            "@head/content_length",
            ScalarDomain::UNSIGNED,
            Cardinality::Single,
            TERMS_COLUMN_AND_STORED,
        );

        let typed = &schemas["typed"].fields;
        assert_field(
            &typed[0],
            0,
            "modified",
            "/modified",
            ScalarDomain::ALL_JSON,
            Cardinality::Single,
            TERMS_COLUMN_AND_STORED,
        );
        assert_field(
            &typed[1],
            1,
            "ecosystems",
            "/ecosystems",
            ScalarDomain::ALL_JSON,
            Cardinality::Multi,
            TERMS_COLUMN_AND_STORED,
        );
        assert!(
            typed
                .iter()
                .all(|field| field.allow_missing && field.allow_null)
        );

        assert_field(
            &schemas["full-text"].fields[0],
            0,
            "body",
            "/body",
            ScalarDomain::STRING,
            Cardinality::Single,
            FULL_TEXT_COMPONENTS,
        );
        assert_field(
            &schemas["vector"].fields[0],
            0,
            "vector",
            "/embedding",
            ScalarDomain::NUMBER,
            Cardinality::Multi,
            FieldComponents::VECTOR,
        );

        let hybrid = &schemas["hybrid"].fields;
        assert_field(
            &hybrid[0],
            0,
            "body",
            "/body",
            ScalarDomain::STRING,
            Cardinality::Single,
            FULL_TEXT_COMPONENTS,
        );
        assert_field(
            &hybrid[1],
            1,
            "@vector",
            "/embedding",
            ScalarDomain::NUMBER,
            Cardinality::Multi,
            FieldComponents::VECTOR,
        );

        assert_fixed_names(
            &schemas["git"].fields,
            &[
                "repository_id",
                "commit_id",
                "tree_path",
                "object_id",
                "pack_path",
                "pack_version",
                "offset",
                "length",
            ],
        );
        assert!(
            schemas["git"].fields[..4]
                .iter()
                .all(|field| field.components.contains(FieldComponents::TERMS))
        );
        assert!(
            schemas["git"].fields[4..]
                .iter()
                .all(|field| !field.components.contains(FieldComponents::TERMS))
        );
        assert_fixed_names(
            &schemas["tensor"].fields,
            &[
                "model_id",
                "tensor_name",
                "source_path",
                "source_version",
                "offset",
                "length",
                "dtype",
                "shape",
            ],
        );
        assert_eq!(schemas["tensor"].fields[7].cardinality, Cardinality::Multi);
        assert!(
            schemas["tensor"].fields[..2]
                .iter()
                .all(|field| field.components.contains(FieldComponents::TERMS))
        );
        assert!(
            schemas["tensor"].fields[2..]
                .iter()
                .all(|field| !field.components.contains(FieldComponents::TERMS))
        );
        for schema_name in ["git", "tensor"] {
            assert!(
                schemas[schema_name]
                    .fields
                    .iter()
                    .all(|field| field.components.contains(FieldComponents::STORED)),
                "{schema_name}"
            );
        }
    }

    fn assert_field(
        field: &FieldSchema,
        id: u32,
        name: &str,
        selector: &str,
        domain: ScalarDomain,
        cardinality: Cardinality,
        components: FieldComponents,
    ) {
        assert_eq!(field.id, FieldId::new(id));
        assert_eq!(field.name, name);
        assert_eq!(field.source_selector, selector);
        assert_eq!(field.domain, domain);
        assert_eq!(field.cardinality, cardinality);
        assert_eq!(field.collation, Collation::BinaryUtf8);
        assert_eq!(field.components, components);
    }

    fn assert_fixed_names(fields: &[FieldSchema], names: &[&str]) {
        assert_eq!(
            fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            names
        );
        assert!(
            fields
                .iter()
                .all(|field| !field.allow_missing && !field.allow_null)
        );
    }
}
