//! One-pass, definition-neutral source selection and v6 document preparation.

use std::collections::BTreeSet;
use std::io::Read;
use std::sync::Arc;
use std::time::Instant;

use keldra_index::v6::{
    PreparedTypedJsonDocument, ProjectedDocumentState, QueryBlockCredits, RecipeIdentity,
    TypedJsonDocumentInput, TypedJsonSelectedField, prepare_typed_json_document,
};
use keldra_index::{
    IndexError,
    typed_json::{FieldSchema, FieldType, ScalarValue},
};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;

use super::catalog::PhysicalCatalogRecipe;
use super::cpu::IndexCpuPool;
use super::date::parse_millis;
use super::hot_ingress::HotProjectionIngress;
use super::json_projection::{ProjectedScalarPointers, project_scalar_pointers};
use super::source::{IndexBuildObject, IndexSourceMutation};

#[derive(Clone)]
pub(crate) struct V6ProjectionExtractor {
    reader: ClusterObjectReader,
    cpu: IndexCpuPool,
    hot: HotProjectionIngress,
    maximum_projection_bytes: usize,
}

pub(crate) struct SelectedV6Source {
    pub(crate) source: IndexSourceMutation,
    pub(crate) selected: Option<ProjectedScalarPointers>,
}

impl V6ProjectionExtractor {
    pub(crate) fn new(
        reader: ClusterObjectReader,
        cpu: IndexCpuPool,
        hot: HotProjectionIngress,
        maximum_projection_bytes: usize,
    ) -> Self {
        Self {
            reader,
            cpu,
            hot,
            maximum_projection_bytes,
        }
    }

    pub(crate) async fn select(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        source: IndexSourceMutation,
        recipes: &[PhysicalCatalogRecipe],
    ) -> Result<SelectedV6Source, Status> {
        let Some(object) = (match &source {
            IndexSourceMutation::Upsert(object) => Some(object.clone()),
            IndexSourceMutation::Remove(_) => None,
        }) else {
            return Ok(SelectedV6Source {
                source,
                selected: None,
            });
        };
        let mut pointers = BTreeSet::new();
        for recipe in recipes {
            let schema = recipe.projection_schema()?;
            pointers.extend(
                schema
                    .fields
                    .iter()
                    .map(|field| field.source_selector.clone()),
            );
        }
        if pointers.is_empty() {
            return Ok(SelectedV6Source {
                source,
                selected: None,
            });
        }
        if let Some(selected) =
            self.hot
                .take_exact_selected(tenant_id, bucket_id, &object.path, object.version)
        {
            super::v6_telemetry::V6PipelineTelemetry::add(
                &super::v6_telemetry::global().hot_prepared_hits,
                1,
            );
            super::v6_telemetry::V6PipelineTelemetry::add(
                &super::v6_telemetry::global().selected_bytes,
                selected.resident_bytes().map_err(index_status)? as u64,
            );
            return Ok(SelectedV6Source {
                source,
                selected: Some(selected),
            });
        }
        super::v6_telemetry::V6PipelineTelemetry::add(&super::v6_telemetry::global().hot_misses, 1);
        let mut payload = self.open_payload(&object).await?;
        let pointers = Arc::new(pointers.into_iter().collect::<Vec<_>>());
        let maximum = self.maximum_projection_bytes;
        let queued_at = Instant::now();
        let (selected, cpu, wait) = self
            .cpu
            .submit(move || {
                let started = Instant::now();
                let wait = started.saturating_duration_since(queued_at);
                let selected = project_scalar_pointers(&mut payload, &pointers, maximum)?;
                Ok::<_, keldra_index::IndexError>((selected, started.elapsed(), wait))
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(index_status)?;
        let telemetry = super::v6_telemetry::global();
        super::v6_telemetry::V6PipelineTelemetry::add(
            &telemetry.payload_parsed_bytes,
            object.content_length,
        );
        if let Some(selected) = &selected {
            super::v6_telemetry::V6PipelineTelemetry::add(
                &telemetry.selected_bytes,
                selected.resident_bytes().map_err(index_status)? as u64,
            );
        }
        super::v6_telemetry::V6PipelineTelemetry::add(
            &telemetry.stage_cpu_nanos,
            cpu.as_nanos().min(u128::from(u64::MAX)) as u64,
        );
        super::v6_telemetry::V6PipelineTelemetry::add(
            &telemetry.stage_queue_wait_nanos,
            wait.as_nanos().min(u128::from(u64::MAX)) as u64,
        );
        Ok(SelectedV6Source { source, selected })
    }

    pub(crate) fn prepare(
        source_scope: [u8; 32],
        selected: &SelectedV6Source,
        recipe: &PhysicalCatalogRecipe,
        previous: Vec<ProjectedDocumentState>,
        credits: &mut QueryBlockCredits,
    ) -> Result<PreparedTypedJsonDocument, Status> {
        let (path, version, result, live) = match &selected.source {
            IndexSourceMutation::Upsert(object) => (
                object.path.clone(),
                object.version,
                Some(object.identity()),
                true,
            ),
            IndexSourceMutation::Remove(identity) => {
                (identity.path.clone(), identity.version, None, false)
            }
        };
        let fields = recipe
            .fields
            .iter()
            .map(|(identity, field)| {
                Ok::<_, Status>(TypedJsonSelectedField {
                    recipe: RecipeIdentity::new(*identity).map_err(index_status)?,
                    field: (**field).clone(),
                    selected: if live {
                        selected
                            .selected
                            .as_ref()
                            .and_then(|selected| selected.get(&field.source_selector))
                            .map(|selected| normalize_selected_values(field, &selected.values))
                            .transpose()
                            .map_err(index_status)?
                    } else {
                        None
                    },
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        prepare_typed_json_document(
            TypedJsonDocumentInput {
                source_scope,
                source_path: path,
                source_version: version,
                result,
                live,
                membership_recipe: RecipeIdentity::new(recipe.membership_recipe)
                    .map_err(index_status)?,
                fields,
            },
            previous,
            credits,
        )
        .map_err(index_status)
    }

    async fn open_payload(
        &self,
        object: &IndexBuildObject,
    ) -> Result<Box<dyn Read + Send>, Status> {
        let blob = keldra_store::BlobRef {
            hash: object.content_hash,
            length: object.content_length,
        };
        Ok(Box::new(self.reader.open_blob_payload(&blob).await?))
    }
}

/// Bind definition-neutral JSON number tags to one declared field type.
///
/// JSON has no distinct positive signed-integer spelling: `100` is selected as
/// `Unsigned(100)` even when a definition declares a signed field. Likewise an
/// integral JSON spelling may legally feed a Float field when it is exactly
/// representable. This is the schema-local normalization boundary retained by
/// the former v4 projector; it performs no lossy numeric coercion.
fn normalize_selected_values(
    field: &FieldSchema,
    selected: &[ScalarValue],
) -> Result<Vec<ScalarValue>, IndexError> {
    selected
        .iter()
        .cloned()
        .map(|value| normalize_selected_value(field, value))
        .collect()
}

fn normalize_selected_value(
    field: &FieldSchema,
    value: ScalarValue,
) -> Result<ScalarValue, IndexError> {
    if value == ScalarValue::Null {
        return Ok(value);
    }
    let invalid = || {
        IndexError::Decode(format!(
            "Typed JSON field `{}` contains a value outside its declared type",
            field.name
        ))
    };
    Ok(match (field.field_type, value) {
        (FieldType::Boolean, ScalarValue::Boolean(value)) => ScalarValue::Boolean(value),
        (FieldType::SignedInteger, ScalarValue::Signed(value)) => ScalarValue::Signed(value),
        (FieldType::SignedInteger, ScalarValue::Unsigned(value)) => {
            ScalarValue::Signed(i64::try_from(value).map_err(|_| invalid())?)
        }
        (FieldType::UnsignedInteger, ScalarValue::Unsigned(value)) => ScalarValue::Unsigned(value),
        (FieldType::UnsignedInteger, ScalarValue::Signed(0)) => ScalarValue::Unsigned(0),
        (FieldType::Float, ScalarValue::Number(bits)) => ScalarValue::Number(bits),
        (FieldType::Float, ScalarValue::Signed(value)) => {
            ScalarValue::exact_number_from_i64(value).ok_or_else(invalid)?
        }
        (FieldType::Float, ScalarValue::Unsigned(value)) => {
            ScalarValue::exact_number_from_u64(value).ok_or_else(invalid)?
        }
        (FieldType::Date, ScalarValue::String(value)) => ScalarValue::Signed(
            parse_millis(
                &value,
                &field.effective_date_format().ok_or_else(|| {
                    IndexError::InvalidDefinition("Date field has no format".into())
                })?,
            )
            .map_err(|_| invalid())?,
        ),
        (FieldType::Keyword | FieldType::Text, ScalarValue::String(value)) => {
            ScalarValue::String(value)
        }
        _ => return Err(invalid()),
    })
}

fn index_status(error: keldra_index::IndexError) -> Status {
    match error {
        keldra_index::IndexError::ResourceLimit { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        _ => Status::data_loss(error.to_string()),
    }
}

pub(crate) fn matching_recipes(
    recipes: &[PhysicalCatalogRecipe],
    tenant_id: u64,
    bucket_id: u64,
    path: &str,
    content_type: Option<&str>,
) -> Vec<PhysicalCatalogRecipe> {
    recipes
        .iter()
        .filter(|recipe| {
            recipe.family.tenant_id == tenant_id
                && recipe.family.bucket_id == bucket_id
                && crate::index_service::path_matches_prefix(path, &recipe.path_prefix)
                && recipe
                    .content_type
                    .as_deref()
                    .is_none_or(|expected| Some(expected) == content_type)
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use keldra_index::typed_json::{
        Analyzer, Cardinality, Collation, DateFormat, FieldCapabilities, FieldId,
    };

    use super::*;

    fn field(field_type: FieldType) -> FieldSchema {
        FieldSchema {
            id: FieldId::new(0),
            name: "value".into(),
            source_selector: "/value".into(),
            field_type,
            cardinality: Cardinality::Single,
            allow_missing: true,
            allow_null: true,
            collation: Collation::BinaryUtf8,
            capabilities: match field_type {
                FieldType::Text => FieldCapabilities::FULL_TEXT,
                _ => FieldCapabilities::EXACT,
            },
            analyzer: (field_type == FieldType::Text)
                .then_some(Analyzer::UnicodeAlphanumericLowercase),
            date_format: (field_type == FieldType::Date).then_some(DateFormat::Iso8601),
        }
    }

    #[test]
    fn definition_neutral_numbers_bind_to_declared_numeric_types() {
        assert_eq!(
            normalize_selected_values(
                &field(FieldType::SignedInteger),
                &[ScalarValue::Unsigned(7)]
            )
            .unwrap(),
            [ScalarValue::Signed(7)]
        );
        assert_eq!(
            normalize_selected_values(&field(FieldType::Float), &[ScalarValue::Unsigned(7)])
                .unwrap(),
            [ScalarValue::number(7.0).unwrap()]
        );
        assert!(
            normalize_selected_values(
                &field(FieldType::SignedInteger),
                &[ScalarValue::Unsigned(u64::MAX)]
            )
            .is_err()
        );
    }

    #[test]
    fn selected_date_strings_bind_to_epoch_milliseconds() {
        assert_eq!(
            normalize_selected_values(
                &field(FieldType::Date),
                &[ScalarValue::String("1970-01-02".into())]
            )
            .unwrap(),
            [ScalarValue::Signed(86_400_000)]
        );
    }
}
