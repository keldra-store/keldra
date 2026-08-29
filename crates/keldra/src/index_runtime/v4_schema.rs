//! Canonical compilation of the public index definition into format-v4 schema.

use std::collections::BTreeSet;

use keldra_api::v1::index_field::FieldType as ApiFieldType;
use keldra_api::v1::index_specification::Specification;
use keldra_api::v1::{
    FullTextIndexSpec, HybridIndexSpec, IndexFieldCapability, IndexFieldCardinality,
    IndexOrderDirection, IndexSpecification, MetadataFilterIndexSpec, TextAnalyzer,
    TypedJsonIndexSpec, VectorIndexSpec, VectorMetric as ApiVectorMetric,
};
use keldra_index::IndexError;
use keldra_index::v4::{
    Analyzer, Cardinality, Collation, ComponentKind, ComponentVersion, DateFormat,
    FieldCapabilities, FieldComponents, FieldId, FieldSchema, FieldType, IndexKind, IndexSemantics,
    OrderDirection, OrderField, Schema, VectorMetric, VectorNormalization,
};

use super::date::validate_format;

const COMPONENT_CODEC_VERSION: u16 = 1;
const IDENTITY_COMPONENT_CODEC_VERSION: u16 = 2;
const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;

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
            FieldType::Keyword,
            Cardinality::Single,
            false,
            false,
            FieldCapabilities::EXACT.union(FieldCapabilities::PREFIX),
            None,
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
            let (field_type, capabilities, allow_null) = metadata_field(name)?;
            field(
                ordinal,
                name,
                &format!("@head/{name}"),
                field_type,
                Cardinality::Single,
                false,
                allow_null,
                capabilities,
                None,
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

fn metadata_field(name: &str) -> Result<(FieldType, FieldCapabilities, bool), IndexError> {
    match name {
        "path" => Ok((
            FieldType::Keyword,
            FieldCapabilities::EXACT
                .union(FieldCapabilities::PREFIX)
                .union(FieldCapabilities::RANGE),
            false,
        )),
        "content_hash" => Ok((FieldType::Keyword, FieldCapabilities::EXACT, false)),
        "content_type" => Ok((
            FieldType::Keyword,
            FieldCapabilities::EXACT.union(FieldCapabilities::PREFIX),
            true,
        )),
        "version" | "content_length" | "committed_at_unix_millis" => Ok((
            FieldType::UnsignedInteger,
            FieldCapabilities::EXACT.union(FieldCapabilities::RANGE),
            false,
        )),
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
            let (field_type, analyzer) = api_field_type(value)?;
            let mut field = field(
                ordinal,
                &value.name,
                &value.json_pointer,
                field_type,
                api_cardinality(value.cardinality)?,
                true,
                true,
                api_capabilities(&value.capabilities)?,
                analyzer,
            )?;
            if let Some(ApiFieldType::Date(date)) = value.field_type.as_ref()
                && !date.strftime_pattern.is_empty()
            {
                field.date_format = Some(DateFormat::Strftime(date.strftime_pattern.clone()));
            }
            if let Some(format) = field.date_format.as_ref() {
                validate_format(format).map_err(|error| {
                    IndexError::InvalidDefinition(format!("invalid Date format: {error}"))
                })?;
            }
            Ok(field)
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

fn api_field_type(
    field: &keldra_api::v1::IndexField,
) -> Result<(FieldType, Option<Analyzer>), IndexError> {
    Ok(match field.field_type.as_ref() {
        Some(ApiFieldType::Boolean(_)) => (FieldType::Boolean, None),
        Some(ApiFieldType::SignedInteger(_)) => (FieldType::SignedInteger, None),
        Some(ApiFieldType::UnsignedInteger(_)) => (FieldType::UnsignedInteger, None),
        Some(ApiFieldType::Float(_)) => (FieldType::Float, None),
        Some(ApiFieldType::Keyword(_)) => (FieldType::Keyword, None),
        Some(ApiFieldType::Text(text)) => {
            let analyzer = match TextAnalyzer::try_from(text.analyzer).map_err(|_| {
                IndexError::InvalidDefinition("unknown Typed JSON text analyzer".into())
            })? {
                TextAnalyzer::UnicodeAlphanumericLowercase => {
                    Analyzer::UnicodeAlphanumericLowercase
                }
            };
            (FieldType::Text, Some(analyzer))
        }
        Some(ApiFieldType::Date(_)) => (FieldType::Date, None),
        None => {
            return Err(IndexError::InvalidDefinition(
                "Typed JSON field type is required".into(),
            ));
        }
    })
}

fn api_cardinality(value: i32) -> Result<Cardinality, IndexError> {
    Ok(
        match IndexFieldCardinality::try_from(value)
            .map_err(|_| IndexError::InvalidDefinition("unknown field cardinality".into()))?
        {
            IndexFieldCardinality::Single => Cardinality::Single,
            IndexFieldCardinality::Multi => Cardinality::Multi,
        },
    )
}

fn api_capabilities(values: &[i32]) -> Result<FieldCapabilities, IndexError> {
    if values.is_empty() {
        return Err(IndexError::InvalidDefinition(
            "field capabilities are required".into(),
        ));
    }
    let mut capabilities = FieldCapabilities::empty();
    let mut seen = BTreeSet::new();
    for encoded in values {
        let capability = IndexFieldCapability::try_from(*encoded)
            .map_err(|_| IndexError::InvalidDefinition("unknown field capability".into()))?;
        if !seen.insert(*encoded) {
            return Err(IndexError::InvalidDefinition(
                "field capabilities must be unique".into(),
            ));
        }
        capabilities = capabilities.union(match capability {
            IndexFieldCapability::Exact => FieldCapabilities::EXACT,
            IndexFieldCapability::Prefix => FieldCapabilities::PREFIX,
            IndexFieldCapability::Range => FieldCapabilities::RANGE,
            IndexFieldCapability::Order => FieldCapabilities::ORDER,
            IndexFieldCapability::Facet => FieldCapabilities::FACET,
            IndexFieldCapability::Aggregate => FieldCapabilities::AGGREGATE,
            IndexFieldCapability::FullText => FieldCapabilities::FULL_TEXT,
        });
    }
    Ok(capabilities)
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
                FieldType::Text,
                Cardinality::Single,
                true,
                false,
                FieldCapabilities::FULL_TEXT,
                Some(Analyzer::UnicodeAlphanumericLowercase),
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
        FieldType::Vector,
        Cardinality::Multi,
        false,
        false,
        FieldCapabilities::empty(),
        None,
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
        ("repository_id", FieldCapabilities::EXACT),
        ("commit_id", FieldCapabilities::EXACT),
        (
            "tree_path",
            FieldCapabilities::EXACT
                .union(FieldCapabilities::PREFIX)
                .union(FieldCapabilities::ORDER),
        ),
        ("object_id", FieldCapabilities::EXACT),
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
        ("model_id", FieldCapabilities::EXACT),
        (
            "tensor_name",
            FieldCapabilities::EXACT.union(FieldCapabilities::ORDER),
        ),
    ];
    let fields = fixed_fields("@tensor", &definitions)?;
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
    definitions: &[(&str, FieldCapabilities)],
) -> Result<Vec<FieldSchema>, IndexError> {
    definitions
        .iter()
        .enumerate()
        .map(|(ordinal, (name, capabilities))| {
            field(
                ordinal,
                name,
                &format!("{selector_prefix}/{name}"),
                FieldType::Keyword,
                Cardinality::Single,
                false,
                false,
                *capabilities,
                None,
            )
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn field(
    ordinal: usize,
    name: &str,
    source_selector: &str,
    field_type: FieldType,
    cardinality: Cardinality,
    allow_missing: bool,
    allow_null: bool,
    capabilities: FieldCapabilities,
    analyzer: Option<Analyzer>,
) -> Result<FieldSchema, IndexError> {
    let mut value = FieldSchema {
        id: FieldId::new(u32::try_from(ordinal).map_err(|_| IndexError::OffsetOverflow)?),
        name: name.to_owned(),
        source_selector: source_selector.to_owned(),
        field_type,
        cardinality,
        allow_missing,
        allow_null,
        collation: Collation::BinaryUtf8,
        capabilities,
        analyzer,
        date_format: (field_type == FieldType::Date).then_some(DateFormat::Iso8601),
        components: FieldComponents::TERMS,
    };
    value.components = value.compiled_components()?;
    Ok(value)
}

fn component_versions(fields: &[FieldSchema]) -> Vec<ComponentVersion> {
    let mut kinds = BTreeSet::from([
        ComponentKind::SEGMENT_ROOT,
        ComponentKind::ROUTING_NODE,
        ComponentKind::IDENTITY_TABLE,
        ComponentKind::LIVE_MASK,
        ComponentKind::PATH_LOCATOR,
        ComponentKind::SCORING_STATISTICS,
    ]);
    for field in fields {
        if field.components.contains(FieldComponents::TERMS) {
            kinds.insert(ComponentKind::TERM_DICTIONARY);
            kinds.insert(ComponentKind::POSTINGS);
        }
        if field.components.contains(FieldComponents::POINTS) {
            kinds.insert(ComponentKind::POINTS);
        }
        if field.components.contains(FieldComponents::DOC_VALUES) {
            kinds.insert(ComponentKind::DOC_VALUES);
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
            } else {
                COMPONENT_CODEC_VERSION
            },
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use keldra_api::v1::index_specification::Specification as Spec;
    use keldra_api::v1::{
        DateIndexField, FullTextField, GitSourceIndexSpec, IndexField, IndexOrder,
        KeywordIndexField, MetadataFilterIndexSpec, PathIndexSpec, SignedIntegerIndexField,
        TensorIndexSpec, TypedJsonIndexSpec, VectorIndexSpec,
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
                            cardinality: IndexFieldCardinality::Single as i32,
                            capabilities: vec![
                                IndexFieldCapability::Range as i32,
                                IndexFieldCapability::Order as i32,
                            ],
                            field_type: Some(ApiFieldType::SignedInteger(
                                SignedIntegerIndexField {},
                            )),
                        },
                        IndexField {
                            name: "ecosystems".into(),
                            json_pointer: "/ecosystems".into(),
                            cardinality: IndexFieldCardinality::Multi as i32,
                            capabilities: vec![
                                IndexFieldCapability::Exact as i32,
                                IndexFieldCapability::Facet as i32,
                            ],
                            field_type: Some(ApiFieldType::Keyword(KeywordIndexField {})),
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
        value.fields[1].cardinality = IndexFieldCardinality::Single as i32;
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
    fn typed_date_format_is_preserved_as_schema_semantics() {
        let specification = spec(Spec::TypedJson(TypedJsonIndexSpec {
            fields: vec![IndexField {
                name: "published".into(),
                json_pointer: "/published".into(),
                cardinality: IndexFieldCardinality::Single as i32,
                capabilities: vec![
                    IndexFieldCapability::Exact as i32,
                    IndexFieldCapability::Range as i32,
                    IndexFieldCapability::Order as i32,
                    IndexFieldCapability::Facet as i32,
                ],
                field_type: Some(ApiFieldType::Date(DateIndexField {
                    strftime_pattern: String::new(),
                })),
            }],
            physical_order: vec![IndexOrder {
                field: "published".into(),
                direction: IndexOrderDirection::Ascending as i32,
            }],
        }));
        let iso = compile_schema("", None, &specification).unwrap();
        assert_eq!(iso.fields[0].field_type, FieldType::Date);
        assert_eq!(iso.fields[0].date_format, Some(DateFormat::Iso8601));
        assert!(iso.validate().is_ok());

        let mut custom_specification = specification;
        let Some(Spec::TypedJson(custom)) = custom_specification.specification.as_mut() else {
            unreachable!();
        };
        let Some(ApiFieldType::Date(date)) = custom.fields[0].field_type.as_mut() else {
            unreachable!();
        };
        date.strftime_pattern = "%Y-%m-%d".into();
        let custom = compile_schema("", None, &custom_specification).unwrap();
        assert_eq!(
            custom.fields[0].date_format,
            Some(DateFormat::Strftime("%Y-%m-%d".into()))
        );
        assert_ne!(iso.fingerprint().unwrap(), custom.fingerprint().unwrap());
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
            ] {
                assert!(kinds.contains(&common), "{name}");
            }
            for field in &schema.fields {
                for (flag, required) in [
                    (FieldComponents::TERMS, ComponentKind::TERM_DICTIONARY),
                    (FieldComponents::TERMS, ComponentKind::POSTINGS),
                    (FieldComponents::POINTS, ComponentKind::POINTS),
                    (FieldComponents::DOC_VALUES, ComponentKind::DOC_VALUES),
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
    fn typed_capabilities_compile_only_required_native_components() {
        let (_, specification, _) = all_kinds()
            .into_iter()
            .find(|(name, _, _)| *name == "typed")
            .unwrap();
        let schema = compile_schema("", None, &specification).unwrap();
        let modified = &schema.fields[0];
        assert_eq!(modified.field_type, FieldType::SignedInteger);
        assert_eq!(
            modified.capabilities,
            FieldCapabilities::RANGE.union(FieldCapabilities::ORDER)
        );
        assert!(modified.components.contains(FieldComponents::POINTS));
        assert!(modified.components.contains(FieldComponents::DOC_VALUES));
        assert!(!modified.components.contains(FieldComponents::TERMS));

        let ecosystems = &schema.fields[1];
        assert_eq!(ecosystems.field_type, FieldType::Keyword);
        assert_eq!(
            ecosystems.capabilities,
            FieldCapabilities::EXACT.union(FieldCapabilities::FACET)
        );
        assert!(ecosystems.components.contains(FieldComponents::TERMS));
        assert!(ecosystems.components.contains(FieldComponents::DOC_VALUES));
        assert!(!ecosystems.components.contains(FieldComponents::POINTS));
    }
}
