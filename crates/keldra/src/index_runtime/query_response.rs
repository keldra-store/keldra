use keldra_api::v1::{IndexFacetBucket, IndexFacetResult};
use keldra_index::v4::{FacetResult, FieldSchema, FieldType, ScalarValue};
use tonic::Status;

use super::date::format_millis;

pub(crate) fn facet_result_to_api(
    fields: &[FieldSchema],
    result: FacetResult,
) -> Result<IndexFacetResult, Status> {
    let field = fields
        .get(result.field_id.get() as usize)
        .ok_or_else(|| Status::data_loss("native query result names an unknown field"))?;
    let mut buckets = result
        .buckets
        .into_iter()
        .map(|bucket| {
            Ok(IndexFacetBucket {
                value_json: facet_scalar_json(field, &bucket.value)?,
                count: bucket.count,
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    buckets.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.value_json.cmp(&right.value_json))
    });
    Ok(IndexFacetResult {
        field: field.name.clone(),
        buckets,
    })
}

fn facet_scalar_json(field: &FieldSchema, value: &ScalarValue) -> Result<Vec<u8>, Status> {
    if field.field_type != FieldType::Date {
        return scalar_json(value);
    }
    if matches!(value, ScalarValue::Null) {
        return scalar_json(value);
    }
    let ScalarValue::Signed(millis) = value else {
        return Err(Status::data_loss(
            "native Date facet is not signed milliseconds",
        ));
    };
    let format = field
        .date_format
        .as_ref()
        .ok_or_else(|| Status::data_loss("native Date facet field has no format"))?;
    let encoded = format_millis(*millis, format)
        .map_err(|error| Status::data_loss(format!("format Date facet: {error}")))?;
    serde_json::to_vec(&encoded)
        .map_err(|error| Status::internal(format!("encode Date facet result: {error}")))
}

pub(crate) fn scalar_json(value: &ScalarValue) -> Result<Vec<u8>, Status> {
    let value = match value {
        ScalarValue::Null => serde_json::Value::Null,
        ScalarValue::Boolean(value) => serde_json::Value::Bool(*value),
        ScalarValue::Signed(value) => serde_json::Value::Number((*value).into()),
        ScalarValue::Unsigned(value) => serde_json::Value::Number((*value).into()),
        ScalarValue::Number(bits) => serde_json::Number::from_f64(f64::from_bits(*bits))
            .map(serde_json::Value::Number)
            .ok_or_else(|| Status::data_loss("native query returned a non-finite number"))?,
        ScalarValue::String(value) => serde_json::Value::String(value.clone()),
    };
    serde_json::to_vec(&value)
        .map_err(|error| Status::internal(format!("encode index computation result: {error}")))
}

#[cfg(test)]
mod tests {
    use keldra_index::v4::{
        Analyzer, Cardinality, Collation, DateFormat, FacetBucket, FieldCapabilities,
        FieldComponents, FieldId,
    };

    use super::*;

    fn field(field_type: FieldType, date_format: Option<DateFormat>) -> FieldSchema {
        FieldSchema {
            id: FieldId::new(0),
            name: "published".into(),
            source_selector: "/published".into(),
            field_type,
            cardinality: Cardinality::Single,
            allow_missing: true,
            allow_null: true,
            collation: Collation::BinaryUtf8,
            capabilities: FieldCapabilities::FACET,
            analyzer: None::<Analyzer>,
            date_format,
            components: FieldComponents::DOC_VALUES,
        }
    }

    #[test]
    fn date_facets_render_the_fields_configured_format() {
        let fields = [field(
            FieldType::Date,
            Some(DateFormat::Strftime("%d/%m/%Y".into())),
        )];
        let result = facet_result_to_api(
            &fields,
            FacetResult {
                field_id: FieldId::new(0),
                buckets: vec![FacetBucket {
                    value: ScalarValue::Signed(86_400_000),
                    count: 2,
                }],
            },
        )
        .unwrap();
        assert_eq!(result.field, "published");
        assert_eq!(result.buckets[0].value_json, br#""02/01/1970""#);
    }

    #[test]
    fn facet_ties_use_canonical_json_byte_order() {
        let fields = [field(FieldType::UnsignedInteger, None)];
        let result = facet_result_to_api(
            &fields,
            FacetResult {
                field_id: FieldId::new(0),
                buckets: vec![
                    FacetBucket {
                        value: ScalarValue::Unsigned(2),
                        count: 1,
                    },
                    FacetBucket {
                        value: ScalarValue::Unsigned(10),
                        count: 1,
                    },
                ],
            },
        )
        .unwrap();
        assert_eq!(result.buckets[0].value_json, b"10");
        assert_eq!(result.buckets[1].value_json, b"2");
    }
}
