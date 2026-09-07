//! Storage-neutral validation for the public Typed JSON protobuf contract.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display, Formatter};

use crate::v1::index_field::FieldType;
use crate::v1::{
    IndexField, IndexFieldCapability, IndexFieldCardinality, IndexOrderDirection, TextAnalyzer,
    TypedJsonIndexSpec,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedJsonValidationError(&'static str);

impl Display for TypedJsonValidationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for TypedJsonValidationError {}

fn invalid(message: &'static str) -> TypedJsonValidationError {
    TypedJsonValidationError(message)
}

/// Validate the complete storage-neutral protobuf shape shared by clients and
/// servers. Execution engines may additionally compile engine-specific values
/// such as date patterns, but must call this authority first.
pub fn validate_typed_json_specification(
    specification: &TypedJsonIndexSpec,
) -> Result<(), TypedJsonValidationError> {
    if specification.fields.is_empty() {
        return Err(invalid("typed JSON index needs at least one field"));
    }
    let mut fields = BTreeMap::new();
    for field in &specification.fields {
        validate_field(field)?;
        if fields.insert(field.name.as_str(), field).is_some() {
            return Err(invalid("typed JSON field names must be unique"));
        }
    }

    let mut ordered = BTreeSet::new();
    for order in &specification.physical_order {
        require_text(&order.field, "physical-order field is required")?;
        if !ordered.insert(order.field.as_str()) {
            return Err(invalid("physical-order field names must be unique"));
        }
        let field = fields.get(order.field.as_str()).ok_or_else(|| {
            invalid("physical order names a field outside the typed JSON definition")
        })?;
        if field.cardinality != IndexFieldCardinality::Single as i32 {
            return Err(invalid(
                "physical order requires single-valued typed JSON fields",
            ));
        }
        if !field
            .capabilities
            .contains(&(IndexFieldCapability::Order as i32))
        {
            return Err(invalid(
                "physical order requires the typed JSON field to declare ORDER",
            ));
        }
        IndexOrderDirection::try_from(order.direction)
            .map_err(|_| invalid("physical order direction is unknown"))?;
    }
    Ok(())
}

fn validate_field(field: &IndexField) -> Result<(), TypedJsonValidationError> {
    require_text(&field.name, "index field name is required")?;
    if !field.json_pointer.is_empty()
        && (!field.json_pointer.starts_with('/') || field.json_pointer.contains('\0'))
    {
        return Err(invalid("JSON pointer must be empty or begin with '/'"));
    }
    let cardinality = IndexFieldCardinality::try_from(field.cardinality)
        .map_err(|_| invalid("typed JSON field cardinality is unknown"))?;
    let field_type = field
        .field_type
        .as_ref()
        .ok_or_else(|| invalid("typed JSON field type is required"))?;
    if let FieldType::Text(text) = field_type {
        TextAnalyzer::try_from(text.analyzer)
            .map_err(|_| invalid("typed JSON text analyzer is unknown"))?;
    }
    if field.capabilities.is_empty() {
        return Err(invalid("typed JSON field needs at least one capability"));
    }
    let mut capabilities = BTreeSet::new();
    for encoded in &field.capabilities {
        let capability = IndexFieldCapability::try_from(*encoded)
            .map_err(|_| invalid("typed JSON field capability is unknown"))?;
        if !capabilities.insert(capability) {
            return Err(invalid("typed JSON field capabilities must be unique"));
        }
        if !capability_allowed(field_type, capability) {
            return Err(invalid(
                "typed JSON field capability is invalid for its field type",
            ));
        }
    }
    if cardinality == IndexFieldCardinality::Multi
        && capabilities.contains(&IndexFieldCapability::Order)
    {
        return Err(invalid(
            "multi-valued typed JSON fields cannot declare ORDER",
        ));
    }
    Ok(())
}

fn capability_allowed(field_type: &FieldType, capability: IndexFieldCapability) -> bool {
    match field_type {
        FieldType::Boolean(_) => matches!(
            capability,
            IndexFieldCapability::Exact | IndexFieldCapability::Facet
        ),
        FieldType::SignedInteger(_) | FieldType::UnsignedInteger(_) | FieldType::Float(_) => {
            matches!(
                capability,
                IndexFieldCapability::Exact
                    | IndexFieldCapability::Range
                    | IndexFieldCapability::Order
                    | IndexFieldCapability::Facet
                    | IndexFieldCapability::Aggregate
            )
        }
        FieldType::Keyword(_) => matches!(
            capability,
            IndexFieldCapability::Exact
                | IndexFieldCapability::Prefix
                | IndexFieldCapability::Range
                | IndexFieldCapability::Order
                | IndexFieldCapability::Facet
        ),
        FieldType::Text(_) => capability == IndexFieldCapability::FullText,
        FieldType::Date(_) => matches!(
            capability,
            IndexFieldCapability::Exact
                | IndexFieldCapability::Range
                | IndexFieldCapability::Order
                | IndexFieldCapability::Facet
        ),
    }
}

fn require_text(value: &str, message: &'static str) -> Result<(), TypedJsonValidationError> {
    if value.is_empty() || value.contains('\0') {
        Err(invalid(message))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::v1::index_field::FieldType;
    use crate::v1::{
        IndexField, IndexFieldCapability, IndexFieldCardinality, KeywordIndexField,
        TypedJsonIndexSpec,
    };

    use super::validate_typed_json_specification;

    #[test]
    fn rejects_duplicate_fields_and_unorderable_physical_order() {
        let field = IndexField {
            name: "id".into(),
            json_pointer: "/id".into(),
            cardinality: IndexFieldCardinality::Single as i32,
            capabilities: vec![IndexFieldCapability::Exact as i32],
            field_type: Some(FieldType::Keyword(KeywordIndexField {})),
        };
        let mut specification = TypedJsonIndexSpec {
            fields: vec![field.clone(), field],
            physical_order: Vec::new(),
        };
        assert!(validate_typed_json_specification(&specification).is_err());
        specification.fields.pop();
        specification.physical_order.push(crate::v1::IndexOrder {
            field: "id".into(),
            direction: crate::v1::IndexOrderDirection::Ascending as i32,
        });
        assert!(validate_typed_json_specification(&specification).is_err());
    }
}
