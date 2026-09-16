//! One-pass, definition-neutral source selection and v1 document preparation.

use std::collections::BTreeSet;
use std::io::Read;
use std::sync::Arc;
use std::time::Instant;

use keldra_index::v1::{
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
use super::json_projection::{
    CompiledScalarProjectionPlan, ProjectedScalarPointers, project_compiled_scalar_pointers,
};
use super::source::{IndexBuildObject, IndexSourceMutation};

#[derive(Clone)]
pub(crate) struct V1ProjectionExtractor {
    reader: ClusterObjectReader,
    cpu: IndexCpuPool,
    hot: HotProjectionIngress,
    maximum_projection_bytes: usize,
}

pub(crate) struct SelectedV1Source {
    pub(crate) source: IndexSourceMutation,
    pub(crate) selected: Option<ProjectedScalarPointers>,
}

impl V1ProjectionExtractor {
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
        recipes: &[Arc<PhysicalCatalogRecipe>],
        physical_catalog_identity: [u8; 32],
    ) -> Result<SelectedV1Source, Status> {
        let object = match source {
            IndexSourceMutation::Upsert(object) => object,
            IndexSourceMutation::Remove {
                identity,
                canonical_path,
            } => {
                self.hot
                    .discard_through(tenant_id, bucket_id, &identity.path, identity.version);
                return Ok(SelectedV1Source {
                    source: IndexSourceMutation::Remove {
                        identity,
                        canonical_path,
                    },
                    selected: None,
                });
            }
        };
        let projection_plan = if recipes.len() == 1 {
            Arc::clone(&recipes[0].projection_plan)
        } else {
            let pointers = Arc::from(
                recipes
                    .iter()
                    .flat_map(|recipe| recipe.projection_plan.pointers().iter().cloned())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>(),
            );
            CompiledScalarProjectionPlan::compile(pointers).map_err(index_status)?
        };
        if projection_plan.pointers().is_empty() {
            self.discard_hot_through(tenant_id, bucket_id, &object.path, object.version);
            return Ok(SelectedV1Source {
                source: IndexSourceMutation::Upsert(object),
                selected: None,
            });
        }
        if let Some(selected) = self
            .hot
            .take_exact_selected_for_generation_wait(
                tenant_id,
                bucket_id,
                &object.path,
                object.version,
                physical_catalog_identity,
            )
            .await
        {
            super::v1_telemetry::V1PipelineTelemetry::add(
                &super::v1_telemetry::global().hot_prepared_hits,
                1,
            );
            super::v1_telemetry::V1PipelineTelemetry::add(
                &super::v1_telemetry::global().selected_bytes,
                selected.resident_bytes().map_err(index_status)? as u64,
            );
            return Ok(SelectedV1Source {
                source: IndexSourceMutation::Upsert(object),
                selected: Some(selected),
            });
        }
        super::v1_telemetry::V1PipelineTelemetry::add(&super::v1_telemetry::global().hot_misses, 1);
        let mut payload = self.open_payload(&object).await?;
        let maximum = self.maximum_projection_bytes;
        let queued_at = Instant::now();
        let (selected, cpu, wait) = self
            .cpu
            .submit(move || {
                let started = Instant::now();
                let wait = started.saturating_duration_since(queued_at);
                let selected =
                    project_compiled_scalar_pointers(&mut payload, projection_plan, maximum)?;
                Ok::<_, keldra_index::IndexError>((selected, started.elapsed(), wait))
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(|error| object_index_status(&object.path, error))?;
        let telemetry = super::v1_telemetry::global();
        super::v1_telemetry::V1PipelineTelemetry::add(
            &telemetry.payload_parsed_bytes,
            object.content_length,
        );
        if let Some(selected) = &selected {
            super::v1_telemetry::V1PipelineTelemetry::add(
                &telemetry.selected_bytes,
                selected.resident_bytes().map_err(index_status)? as u64,
            );
        }
        super::v1_telemetry::V1PipelineTelemetry::add(
            &telemetry.stage_cpu_nanos,
            cpu.as_nanos().min(u128::from(u64::MAX)) as u64,
        );
        super::v1_telemetry::V1PipelineTelemetry::add(
            &telemetry.stage_queue_wait_nanos,
            wait.as_nanos().min(u128::from(u64::MAX)) as u64,
        );
        Ok(SelectedV1Source {
            source: IndexSourceMutation::Upsert(object),
            selected,
        })
    }

    pub(crate) fn discard_hot_through(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
        version: u64,
    ) {
        self.hot
            .discard_through(tenant_id, bucket_id, path, version);
    }

    pub(crate) fn prepare(
        source_scope: [u8; 32],
        selected: &SelectedV1Source,
        recipe: &PhysicalCatalogRecipe,
        previous: &[ProjectedDocumentState],
        credits: &mut QueryBlockCredits,
    ) -> Result<PreparedTypedJsonDocument, Status> {
        let (path, canonical_source_path, version, result, live) = match &selected.source {
            IndexSourceMutation::Upsert(object) => (
                object.path.clone(),
                object.canonical_path.clone(),
                object.version,
                Some(object.identity()),
                true,
            ),
            IndexSourceMutation::Remove {
                identity,
                canonical_path,
            } => (
                identity.path.clone(),
                canonical_path.clone(),
                identity.version,
                None,
                false,
            ),
        };
        let diagnostic_path = path.clone();
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
                            .map_err(|error| object_index_status(&diagnostic_path, error))?
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
                canonical_source_path,
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
        .map_err(|error| object_index_status(&diagnostic_path, error))
    }

    /// Prepare a substantial ordered chunk in one CPU-pool job. The caller
    /// keeps chunks bounded by both mutation count and retained credits, so a
    /// worker can reuse its stack and allocator capacity without creating one
    /// scheduler job for every document.
    pub(crate) async fn prepare_batch_owned(
        &self,
        source_scope: [u8; 32],
        recipe: Arc<PhysicalCatalogRecipe>,
        inputs: Vec<(
            SelectedV1Source,
            Vec<ProjectedDocumentState>,
            QueryBlockCredits,
        )>,
    ) -> Result<
        Vec<(
            SelectedV1Source,
            Vec<ProjectedDocumentState>,
            PreparedTypedJsonDocument,
            QueryBlockCredits,
        )>,
        Status,
    > {
        self.cpu
            .submit(move || {
                let mut output = Vec::with_capacity(inputs.len());
                for (selected, previous, mut credits) in inputs {
                    let prepared =
                        Self::prepare(source_scope, &selected, &recipe, &previous, &mut credits)?;
                    output.push((selected, previous, prepared, credits));
                }
                Ok(output)
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))?
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
/// the projection pipeline; it performs no lossy numeric coercion.
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
    let diagnostic_value = diagnostic_scalar_value(&value);
    let invalid = |reason: &str| {
        IndexError::Decode(format!(
            "Typed JSON field `{}` refused value {} for declared type {:?}: {reason}",
            field.name, diagnostic_value, field.field_type
        ))
    };
    Ok(match (field.field_type, value) {
        (FieldType::Boolean, ScalarValue::Boolean(value)) => ScalarValue::Boolean(value),
        (FieldType::SignedInteger, ScalarValue::Signed(value)) => ScalarValue::Signed(value),
        (FieldType::SignedInteger, ScalarValue::Unsigned(value)) => ScalarValue::Signed(
            i64::try_from(value)
                .map_err(|_| invalid("unsigned integer exceeds the signed range"))?,
        ),
        (FieldType::UnsignedInteger, ScalarValue::Unsigned(value)) => ScalarValue::Unsigned(value),
        (FieldType::UnsignedInteger, ScalarValue::Signed(0)) => ScalarValue::Unsigned(0),
        (FieldType::Float, ScalarValue::Number(bits)) => ScalarValue::Number(bits),
        (FieldType::Float, ScalarValue::Signed(value)) => ScalarValue::exact_number_from_i64(value)
            .ok_or_else(|| invalid("integer cannot be represented exactly as a float"))?,
        (FieldType::Float, ScalarValue::Unsigned(value)) => {
            ScalarValue::exact_number_from_u64(value)
                .ok_or_else(|| invalid("integer cannot be represented exactly as a float"))?
        }
        (FieldType::Date, ScalarValue::String(value)) => ScalarValue::Signed(
            parse_millis(
                &value,
                &field.effective_date_format().ok_or_else(|| {
                    IndexError::InvalidDefinition("Date field has no format".into())
                })?,
            )
            .map_err(|error| invalid(&error.to_string()))?,
        ),
        (FieldType::Keyword | FieldType::Text, ScalarValue::String(value)) => {
            ScalarValue::String(value)
        }
        _ => return Err(invalid("value does not match the declared field type")),
    })
}

const MAX_DIAGNOSTIC_VALUE_CHARS: usize = 256;

fn diagnostic_scalar_value(value: &ScalarValue) -> String {
    let rendered = match value {
        ScalarValue::Null => "null".to_owned(),
        ScalarValue::Boolean(value) => value.to_string(),
        ScalarValue::Signed(value) => value.to_string(),
        ScalarValue::Number(bits) => f64::from_bits(*bits).to_string(),
        ScalarValue::Unsigned(value) => value.to_string(),
        ScalarValue::String(value) => serde_json::to_string(value)
            .unwrap_or_else(|_| "<string could not be rendered>".to_owned()),
    };
    if rendered.chars().count() <= MAX_DIAGNOSTIC_VALUE_CHARS {
        return rendered;
    }
    let mut truncated = rendered
        .chars()
        .take(MAX_DIAGNOSTIC_VALUE_CHARS)
        .collect::<String>();
    truncated.push_str("…");
    truncated
}

fn index_status(error: keldra_index::IndexError) -> Status {
    match error {
        keldra_index::IndexError::ResourceLimit { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        _ => Status::data_loss(error.to_string()),
    }
}

fn object_index_status(path: &str, error: keldra_index::IndexError) -> Status {
    let resource_limit = matches!(error, keldra_index::IndexError::ResourceLimit { .. });
    let message = format!("object {path:?} cannot be indexed: {error}");
    if resource_limit {
        Status::resource_exhausted(message)
    } else {
        Status::data_loss(message)
    }
}

pub(crate) fn matching_recipes(
    recipes: &[Arc<PhysicalCatalogRecipe>],
    tenant_id: u64,
    bucket_id: u64,
    path: &str,
    content_type: Option<&str>,
) -> Vec<Arc<PhysicalCatalogRecipe>> {
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
                &[
                    ScalarValue::String("1970-01-02".into()),
                    ScalarValue::String("2026-09-12T16:55:26.353907Z".into()),
                ]
            )
            .unwrap(),
            [
                ScalarValue::Signed(86_400_000),
                ScalarValue::Signed(1_789_232_126_353),
            ]
        );
    }

    #[test]
    fn refused_scalar_names_the_field_value_type_and_reason() {
        let error = normalize_selected_values(
            &field(FieldType::Date),
            &[ScalarValue::String("not-a-date".into())],
        )
        .unwrap_err();
        let status = object_index_status("content/example.json", error);
        let error = status.message();

        assert!(error.contains("object \"content/example.json\""), "{error}");
        assert!(error.contains("field `value`"), "{error}");
        assert!(error.contains("\"not-a-date\""), "{error}");
        assert!(error.contains("declared type Date"), "{error}");
        assert!(
            error.contains("date value does not match its field format"),
            "{error}"
        );
    }
}
