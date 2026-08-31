//! Canonical compilation of the public TypedJson definition contract.
//!
//! This module deliberately owns no segment-layout detail.  It converts the
//! wire definition into the storage-neutral schema that both the v6 projector
//! and eventual query materializer consume.

use std::collections::BTreeSet;

use keldra_api::v1::index_field::FieldType as ApiFieldType;
use keldra_api::v1::{
    IndexFieldCapability, IndexFieldCardinality, IndexOrderDirection, IndexSpecification,
    TextAnalyzer,
};
use keldra_index::IndexError;
use keldra_index::typed_json::{
    Analyzer, Cardinality, Collation, DateFormat, FieldCapabilities, FieldId, FieldSchema,
    FieldType, OrderDirection, OrderField, TypedJsonSchema,
};

use super::date::validate_format;

/// Compile the only public index kind supported by the partition-owned v6
/// runtime.  Other kinds have already been rejected at API admission.
pub(crate) fn compile_typed_json_schema(
    path_prefix: &str,
    content_type_scope: Option<&str>,
    specification: &IndexSpecification,
) -> Result<TypedJsonSchema, IndexError> {
    let Some(keldra_api::v1::index_specification::Specification::TypedJson(specification)) =
        specification.specification.as_ref()
    else {
        return Err(IndexError::InvalidDefinition(
            "only TypedJson definitions are supported".into(),
        ));
    };
    let fields = specification
        .fields
        .iter()
        .enumerate()
        .map(|(ordinal, value)| {
            let (field_type, analyzer) = api_field_type(value)?;
            let mut field = FieldSchema {
                id: FieldId::new(u32::try_from(ordinal).map_err(|_| IndexError::OffsetOverflow)?),
                name: value.name.clone(),
                source_selector: value.json_pointer.clone(),
                field_type,
                cardinality: api_cardinality(value.cardinality)?,
                allow_missing: true,
                allow_null: true,
                collation: Collation::BinaryUtf8,
                capabilities: api_capabilities(&value.capabilities)?,
                analyzer,
                // Omitted is canonically ISO 8601 in the neutral contract;
                // retaining no redundant spelling keeps recipe identity
                // stable across clients that omit the default.
                date_format: None,
            };
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
                        "physical order names an unknown TypedJson field".into(),
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
    TypedJsonSchema {
        path_prefix: path_prefix.to_owned(),
        content_type_scope: content_type_scope.map(str::to_owned),
        fields,
        physical_order,
    }
    .canonicalize_physical_fields()
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
                IndexError::InvalidDefinition("unknown TypedJson text analyzer".into())
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
                "TypedJson field type is required".into(),
            ));
        }
    })
}

fn api_cardinality(value: i32) -> Result<Cardinality, IndexError> {
    match IndexFieldCardinality::try_from(value)
        .map_err(|_| IndexError::InvalidDefinition("unknown field cardinality".into()))?
    {
        IndexFieldCardinality::Single => Ok(Cardinality::Single),
        IndexFieldCardinality::Multi => Ok(Cardinality::Multi),
    }
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
