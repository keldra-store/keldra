use keldra_api::v1::index_field::FieldType;
use keldra_api::v1::index_specification::Specification;
use keldra_api::v1::{
    CreateIndexRequest, IndexDefinition, IndexField, IndexFieldCapability, IndexFieldCardinality,
    IndexKind, IndexOrderDirection, IndexSpecification, TextAnalyzer, TypedJsonIndexSpec,
    UpdateIndexRequest,
};
use keldra_atomic_program::MAX_OBJECT_PATH_BYTES;
use keldra_index::typed_json::DateFormat;
use keldra_store::INDEX_DEFINITION_PREFIX;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use tonic::Status;

use crate::index_runtime::date::validate_format;
use crate::index_runtime::typed_json_schema::compile_typed_json_schema;

const STORED_DEFINITION_FORMAT: u16 = 4;
const MAX_INDEX_NAME_BYTES: usize = 128;
const MAX_CONTENT_TYPE_BYTES: usize = 512;
const MAX_COMMAND_ID_BYTES: usize = 256;
const INDEX_ID_CONTEXT: &[u8] = b"keldra.index/id/v1";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredIndexDefinition {
    format: u16,
    pub index_id: u64,
    pub tenant: String,
    pub bucket: String,
    pub name: String,
    pub path_prefix: String,
    pub content_type: Option<String>,
    specification_protobuf: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_explicit_rebuild_at_unix_millis: Option<u64>,
}

impl StoredIndexDefinition {
    pub(crate) fn with_index_id(&self, index_id: u64) -> Self {
        debug_assert_ne!(index_id, 0);
        let mut physical = self.clone();
        physical.index_id = index_id;
        physical
    }

    pub(crate) fn create(
        tenant: String,
        request: CreateIndexRequest,
        index_id: u64,
    ) -> Result<Self, Status> {
        if index_id == 0 {
            return Err(Status::internal("allocated index ID is zero"));
        }
        let specification = validate_create_definition(&request)?;
        Ok(Self {
            format: STORED_DEFINITION_FORMAT,
            index_id,
            tenant,
            bucket: request.bucket,
            name: request.name,
            path_prefix: request.path_prefix,
            content_type: optional_content_type(request.content_type)?,
            specification_protobuf: specification.encode_to_vec(),
            last_explicit_rebuild_at_unix_millis: None,
        })
    }

    pub(crate) fn updated(&self, request: UpdateIndexRequest) -> Result<Self, Status> {
        let specification = validate_update_definition(&request)?;
        if request.bucket != self.bucket || request.name != self.name {
            return Err(Status::invalid_argument(
                "index bucket and name are immutable",
            ));
        }
        Ok(Self {
            format: self.format,
            index_id: self.index_id,
            tenant: self.tenant.clone(),
            bucket: self.bucket.clone(),
            name: self.name.clone(),
            path_prefix: request.path_prefix,
            content_type: optional_content_type(request.content_type)?,
            specification_protobuf: specification.encode_to_vec(),
            last_explicit_rebuild_at_unix_millis: self.last_explicit_rebuild_at_unix_millis,
        })
    }

    pub(crate) fn with_explicit_rebuild(
        &self,
        accepted_at_unix_millis: u64,
    ) -> Result<Self, Status> {
        validate_explicit_rebuild(accepted_at_unix_millis)?;
        let mut updated = self.clone();
        updated.last_explicit_rebuild_at_unix_millis = Some(accepted_at_unix_millis);
        Ok(updated)
    }

    pub(crate) fn last_explicit_rebuild_at_unix_millis(&self) -> Option<u64> {
        self.last_explicit_rebuild_at_unix_millis
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, Status> {
        serde_json::to_vec(self)
            .map_err(|error| Status::internal(format!("encode index definition: {error}")))
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Status> {
        let stored: Self = serde_json::from_slice(bytes)
            .map_err(|_| Status::data_loss("index definition is not valid canonical JSON"))?;
        if stored.format != STORED_DEFINITION_FORMAT
            || stored.index_id == 0
            || stored.tenant.is_empty()
            || stored.bucket.is_empty()
        {
            return Err(Status::data_loss(
                "index definition has invalid identity fields",
            ));
        }
        validate_name(&stored.name).map_err(|_| Status::data_loss("invalid stored index name"))?;
        validate_path_prefix(&stored.path_prefix)
            .map_err(|_| Status::data_loss("invalid stored index path prefix"))?;
        if stored
            .content_type
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > MAX_CONTENT_TYPE_BYTES)
        {
            return Err(Status::data_loss("stored index content type is invalid"));
        }
        let specification = stored.specification()?;
        validate_specification(&specification)
            .map_err(|_| Status::data_loss("stored index specification is invalid"))?;
        compile_typed_json_schema(
            &stored.path_prefix,
            stored.content_type.as_deref(),
            &specification,
        )
        .map_err(|_| Status::data_loss("stored index schema is invalid"))?;
        if let Some(accepted_at_unix_millis) = stored.last_explicit_rebuild_at_unix_millis {
            validate_explicit_rebuild(accepted_at_unix_millis)
                .map_err(|_| Status::data_loss("stored explicit index rebuild is invalid"))?;
        }
        Ok(stored)
    }

    pub(crate) fn specification(&self) -> Result<IndexSpecification, Status> {
        IndexSpecification::decode(self.specification_protobuf.as_slice())
            .map_err(|_| Status::data_loss("stored index specification cannot be decoded"))
    }

    pub(crate) fn to_api(&self, object_version: u64) -> Result<IndexDefinition, Status> {
        let specification = self.specification()?;
        let kind = kind_for(&specification)?;
        Ok(IndexDefinition {
            index_id: self.index_id,
            bucket: self.bucket.clone(),
            name: self.name.clone(),
            path_prefix: self.path_prefix.clone(),
            content_type: self.content_type.clone().unwrap_or_default(),
            kind: kind as i32,
            specification: Some(specification),
            version: object_version,
        })
    }
}

pub(crate) fn definition_path(name: &str) -> Result<String, Status> {
    validate_name(name)?;
    Ok(format!("{INDEX_DEFINITION_PREFIX}{name}"))
}

pub(crate) fn definition_name(path: &str) -> Option<&str> {
    let name = path.strip_prefix(INDEX_DEFINITION_PREFIX)?;
    validate_name(name).ok()?;
    Some(name)
}

/// Match an ordinary object path against the public segment-aware prefix
/// contract. A trailing slash selects children; a prefix without one selects
/// the exact path and its children, never a neighbouring segment.
pub(crate) fn path_matches_prefix(path: &str, prefix: &str) -> bool {
    if prefix.is_empty() {
        return true;
    }
    if prefix.ends_with('/') {
        return path.starts_with(prefix);
    }
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|remainder| remainder.starts_with('/'))
}

/// Derive one stable opaque identity for every retry of the same create
/// command without adding a managed cluster counter.
pub(crate) fn derive_index_id(
    tenant_id: u64,
    bucket_id: u64,
    name: &str,
    command_id: &str,
) -> Result<u64, Status> {
    if tenant_id == 0 || bucket_id == 0 {
        return Err(Status::failed_precondition(
            "stable tenant and bucket IDs are required",
        ));
    }
    validate_name(name)?;
    validate_command_id(command_id)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(INDEX_ID_CONTEXT);
    hasher.update(&tenant_id.to_be_bytes());
    hasher.update(&bucket_id.to_be_bytes());
    hasher.update(&(name.len() as u64).to_be_bytes());
    hasher.update(name.as_bytes());
    hasher.update(command_id.as_bytes());
    let mut encoded = [0_u8; 8];
    encoded.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
    let identity = u64::from_be_bytes(encoded);
    Ok(if identity == 0 { 1 } else { identity })
}

pub(crate) fn validate_create_definition(
    request: &CreateIndexRequest,
) -> Result<IndexSpecification, Status> {
    validate_bucket(&request.bucket)?;
    validate_name(&request.name)?;
    validate_path_prefix(&request.path_prefix)?;
    let content_type = optional_content_type(request.content_type.clone())?;
    validate_command_id(&request.command_id)?;
    let specification = request
        .specification
        .clone()
        .ok_or_else(|| Status::invalid_argument("index specification is required"))?;
    validate_specification(&specification)?;
    compile_typed_json_schema(
        &request.path_prefix,
        content_type.as_deref(),
        &specification,
    )
    .map_err(|error| Status::invalid_argument(error.to_string()))?;
    Ok(specification)
}

pub(crate) fn validate_update_definition(
    request: &UpdateIndexRequest,
) -> Result<IndexSpecification, Status> {
    if request.expected_version == 0 {
        return Err(Status::invalid_argument(
            "expected index definition version must be non-zero",
        ));
    }
    validate_bucket(&request.bucket)?;
    validate_name(&request.name)?;
    validate_path_prefix(&request.path_prefix)?;
    let content_type = optional_content_type(request.content_type.clone())?;
    validate_command_id(&request.command_id)?;
    let specification = request
        .specification
        .clone()
        .ok_or_else(|| Status::invalid_argument("index specification is required"))?;
    validate_specification(&specification)?;
    compile_typed_json_schema(
        &request.path_prefix,
        content_type.as_deref(),
        &specification,
    )
    .map_err(|error| Status::invalid_argument(error.to_string()))?;
    Ok(specification)
}

fn validate_specification(specification: &IndexSpecification) -> Result<(), Status> {
    match specification.specification.as_ref() {
        Some(Specification::TypedJson(specification)) => {
            validate_typed_json_specification(specification)
        }
        Some(
            Specification::Path(_)
            | Specification::MetadataFilter(_)
            | Specification::FullText(_)
            | Specification::Vector(_)
            | Specification::Hybrid(_)
            | Specification::GitSource(_)
            | Specification::Tensor(_),
        ) => Err(Status::unimplemented(
            "the partition-owned indexing architecture currently supports TypedJson only",
        )),
        None => Err(Status::invalid_argument("index specification is required")),
    }
}

fn kind_for(specification: &IndexSpecification) -> Result<IndexKind, Status> {
    match specification.specification.as_ref() {
        Some(Specification::TypedJson(_)) => Ok(IndexKind::TypedJson),
        Some(_) => Err(Status::data_loss(
            "stored index definition uses a removed non-TypedJson index kind",
        )),
        None => Err(Status::data_loss("stored index specification is empty")),
    }
}

fn validate_fields(fields: &[IndexField]) -> Result<(), Status> {
    if fields.is_empty() {
        return Err(Status::invalid_argument(
            "typed JSON index needs at least one field",
        ));
    }
    let mut names = BTreeSet::new();
    for field in fields {
        validate_field_parts(&field.name, &field.json_pointer)?;
        if !names.insert(field.name.as_str()) {
            return Err(Status::invalid_argument(
                "typed JSON field names must be unique",
            ));
        }
        validate_typed_json_field(field)?;
    }
    Ok(())
}

fn validate_typed_json_field(field: &IndexField) -> Result<(), Status> {
    let cardinality = IndexFieldCardinality::try_from(field.cardinality)
        .map_err(|_| Status::invalid_argument("typed JSON field cardinality is unknown"))?;
    let field_type = field
        .field_type
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("typed JSON field type is required"))?;
    if let FieldType::Text(text) = field_type {
        TextAnalyzer::try_from(text.analyzer)
            .map_err(|_| Status::invalid_argument("typed JSON text analyzer is unknown"))?;
    }
    if let FieldType::Date(date) = field_type {
        let format = if date.strftime_pattern.is_empty() {
            DateFormat::Iso8601
        } else {
            DateFormat::Strftime(date.strftime_pattern.clone())
        };
        validate_format(&format)
            .map_err(|error| Status::invalid_argument(format!("invalid Date format: {error}")))?;
    }

    if field.capabilities.is_empty() {
        return Err(Status::invalid_argument(
            "typed JSON field needs at least one capability",
        ));
    }
    let mut capabilities = BTreeSet::new();
    for encoded in &field.capabilities {
        let capability = IndexFieldCapability::try_from(*encoded)
            .map_err(|_| Status::invalid_argument("typed JSON field capability is unknown"))?;
        if !capabilities.insert(capability) {
            return Err(Status::invalid_argument(
                "typed JSON field capabilities must be unique",
            ));
        }
        if !capability_allowed(field_type, capability) {
            return Err(Status::invalid_argument(format!(
                "typed JSON field capability {capability:?} is invalid for its field type"
            )));
        }
    }
    if cardinality == IndexFieldCardinality::Multi
        && capabilities.contains(&IndexFieldCapability::Order)
    {
        return Err(Status::invalid_argument(
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

fn validate_typed_json_specification(specification: &TypedJsonIndexSpec) -> Result<(), Status> {
    validate_fields(&specification.fields)?;

    let fields = specification
        .fields
        .iter()
        .map(|field| {
            (
                field.name.as_str(),
                (field.cardinality, field.capabilities.as_slice()),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut ordered = BTreeSet::new();
    for order in &specification.physical_order {
        require_text(&order.field, "physical-order field")?;
        if !ordered.insert(order.field.as_str()) {
            return Err(Status::invalid_argument(
                "physical-order field names must be unique",
            ));
        }
        let Some((cardinality, capabilities)) = fields.get(order.field.as_str()) else {
            return Err(Status::invalid_argument(
                "physical order names a field outside the typed JSON definition",
            ));
        };
        if IndexFieldCardinality::try_from(*cardinality)
            .is_ok_and(|value| value == IndexFieldCardinality::Multi)
        {
            return Err(Status::invalid_argument(
                "physical order requires single-valued typed JSON fields",
            ));
        }
        if !capabilities
            .iter()
            .any(|value| *value == IndexFieldCapability::Order as i32)
        {
            return Err(Status::invalid_argument(
                "physical order requires the typed JSON field to declare ORDER",
            ));
        }
        IndexOrderDirection::try_from(order.direction)
            .map_err(|_| Status::invalid_argument("physical order direction is unknown"))?;
    }
    Ok(())
}

fn validate_field_parts(name: &str, pointer: &str) -> Result<(), Status> {
    require_text(name, "index field name")?;
    validate_json_pointer(pointer)
}

fn validate_json_pointer(pointer: &str) -> Result<(), Status> {
    if pointer.is_empty() || (pointer.starts_with('/') && !pointer.contains('\0')) {
        Ok(())
    } else {
        Err(Status::invalid_argument(
            "JSON pointer must be empty or begin with '/'",
        ))
    }
}

fn validate_bucket(bucket: &str) -> Result<(), Status> {
    require_text(bucket, "bucket")
}

fn validate_name(name: &str) -> Result<(), Status> {
    if name.is_empty()
        || name.len() > MAX_INDEX_NAME_BYTES
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        || name == "."
        || name == ".."
    {
        return Err(Status::invalid_argument(
            "index name must be 1..=128 ASCII letters, digits, '.', '-' or '_'",
        ));
    }
    Ok(())
}

fn validate_path_prefix(prefix: &str) -> Result<(), Status> {
    if prefix.len() > MAX_OBJECT_PATH_BYTES || prefix.contains('\0') {
        Err(Status::invalid_argument("index path prefix is invalid"))
    } else {
        Ok(())
    }
}

fn optional_content_type(value: String) -> Result<Option<String>, Status> {
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() > MAX_CONTENT_TYPE_BYTES || value.contains('\0') {
        return Err(Status::invalid_argument("index content type is invalid"));
    }
    Ok(Some(value))
}

fn require_unique_nonempty(values: &[String], label: &str) -> Result<(), Status> {
    if values.is_empty() {
        return Err(Status::invalid_argument(format!(
            "index needs at least one {label}"
        )));
    }
    let mut unique = BTreeSet::new();
    for value in values {
        require_text(value, label)?;
        if !unique.insert(value.as_str()) {
            return Err(Status::invalid_argument(format!(
                "duplicate {label} `{value}`"
            )));
        }
    }
    Ok(())
}

fn require_text(value: &str, label: &str) -> Result<(), Status> {
    if value.is_empty() || value.contains('\0') {
        Err(Status::invalid_argument(format!(
            "{label} must be non-empty and contain no NUL"
        )))
    } else {
        Ok(())
    }
}

pub(crate) fn validate_command_id(value: &str) -> Result<(), Status> {
    if value.is_empty() || value.len() > MAX_COMMAND_ID_BYTES || value.contains('\0') {
        Err(Status::invalid_argument(
            "command_id must contain 1 to 256 bytes and no NUL",
        ))
    } else {
        Ok(())
    }
}

fn validate_explicit_rebuild(accepted_at_unix_millis: u64) -> Result<(), Status> {
    if accepted_at_unix_millis == 0 {
        return Err(Status::invalid_argument(
            "explicit rebuild timestamp must be non-zero",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use keldra_api::v1::{
        DateIndexField, IndexOrder, KeywordIndexField, SignedIntegerIndexField, TensorIndexSpec,
        TypedJsonIndexSpec, index_specification,
    };

    use super::*;

    fn request() -> CreateIndexRequest {
        CreateIndexRequest {
            bucket: "objects".into(),
            name: "by-json".into(),
            path_prefix: "tenant/123/".into(),
            content_type: String::new(),
            specification: Some(IndexSpecification {
                specification: Some(index_specification::Specification::TypedJson(
                    TypedJsonIndexSpec {
                        fields: vec![IndexField {
                            name: "version".into(),
                            json_pointer: "/version".into(),
                            cardinality: IndexFieldCardinality::Single as i32,
                            capabilities: vec![
                                IndexFieldCapability::Exact as i32,
                                IndexFieldCapability::Range as i32,
                            ],
                            field_type: Some(FieldType::UnsignedInteger(
                                keldra_api::v1::UnsignedIntegerIndexField {},
                            )),
                        }],
                        physical_order: Vec::new(),
                    },
                )),
            }),
            command_id: "create-index".into(),
        }
    }

    #[test]
    fn stored_definition_round_trips_without_mutable_names_in_placement_identity() {
        let stored = StoredIndexDefinition::create("tenant".into(), request(), 44).unwrap();
        let decoded = StoredIndexDefinition::decode(&stored.encode().unwrap()).unwrap();
        assert_eq!(decoded, stored);
        assert_eq!(
            decoded.specification().unwrap().specification.unwrap(),
            stored.specification().unwrap().specification.unwrap()
        );
        assert_eq!(
            definition_path("by-json").unwrap(),
            "_keldra/indices/v4/definitions/by-json"
        );
        assert_eq!(
            derive_index_id(7, 9, "by-json", "create-index").unwrap(),
            derive_index_id(7, 9, "by-json", "create-index").unwrap()
        );
    }

    #[test]
    fn explicit_rebuild_state_round_trips_and_survives_semantic_updates() {
        let stored = StoredIndexDefinition::create("tenant".into(), request(), 44)
            .unwrap()
            .with_explicit_rebuild(1_000)
            .unwrap();
        let decoded = StoredIndexDefinition::decode(&stored.encode().unwrap()).unwrap();
        assert_eq!(decoded, stored);
        assert_eq!(decoded.last_explicit_rebuild_at_unix_millis(), Some(1_000));

        let create = request();
        let updated = decoded
            .updated(UpdateIndexRequest {
                bucket: create.bucket,
                name: create.name,
                path_prefix: "tenant/456/".into(),
                content_type: create.content_type,
                specification: create.specification,
                expected_version: 8,
                command_id: "update-index".into(),
            })
            .unwrap();
        assert_eq!(updated.last_explicit_rebuild_at_unix_millis(), Some(1_000));
    }

    #[test]
    fn invalid_stored_explicit_rebuild_state_is_rejected() {
        let stored = StoredIndexDefinition::create("tenant".into(), request(), 44).unwrap();
        let mut encoded = serde_json::to_value(stored).unwrap();
        encoded["last_explicit_rebuild_at_unix_millis"] = serde_json::json!(0);
        let error =
            StoredIndexDefinition::decode(&serde_json::to_vec(&encoded).unwrap()).unwrap_err();
        assert_eq!(error.code(), tonic::Code::DataLoss);
    }

    #[test]
    fn format_three_definition_is_not_a_compatibility_input() {
        let stored = StoredIndexDefinition::create("tenant".into(), request(), 44).unwrap();
        let mut encoded = serde_json::to_value(stored).unwrap();
        encoded["format"] = serde_json::json!(3);
        let error =
            StoredIndexDefinition::decode(&serde_json::to_vec(&encoded).unwrap()).unwrap_err();
        assert_eq!(error.code(), tonic::Code::DataLoss);
    }

    fn typed_json_request() -> CreateIndexRequest {
        let mut request = request();
        request.specification = Some(IndexSpecification {
            specification: Some(index_specification::Specification::TypedJson(
                TypedJsonIndexSpec {
                    fields: vec![
                        IndexField {
                            name: "modified_at".into(),
                            json_pointer: "/modified_at".into(),
                            cardinality: IndexFieldCardinality::Single as i32,
                            capabilities: vec![
                                IndexFieldCapability::Range as i32,
                                IndexFieldCapability::Order as i32,
                            ],
                            field_type: Some(FieldType::SignedInteger(SignedIntegerIndexField {})),
                        },
                        IndexField {
                            name: "ecosystems".into(),
                            json_pointer: "/ecosystems".into(),
                            cardinality: IndexFieldCardinality::Multi as i32,
                            capabilities: vec![
                                IndexFieldCapability::Exact as i32,
                                IndexFieldCapability::Facet as i32,
                            ],
                            field_type: Some(FieldType::Keyword(KeywordIndexField {})),
                        },
                    ],
                    physical_order: vec![IndexOrder {
                        field: "modified_at".into(),
                        direction: IndexOrderDirection::Descending as i32,
                    }],
                },
            )),
        });
        request
    }

    #[test]
    fn typed_json_cardinality_and_physical_order_round_trip() {
        let request = typed_json_request();
        let stored = StoredIndexDefinition::create("tenant".into(), request.clone(), 44).unwrap();
        let decoded = StoredIndexDefinition::decode(&stored.encode().unwrap()).unwrap();
        assert_eq!(
            decoded.to_api(7).unwrap().specification,
            request.specification
        );
    }

    #[test]
    fn definition_admission_rejects_a_schema_whose_statistics_cannot_fit() {
        let mut request = request();
        request.specification = Some(IndexSpecification {
            specification: Some(index_specification::Specification::TypedJson(
                TypedJsonIndexSpec {
                    fields: (0..65_537)
                        .map(|ordinal| IndexField {
                            name: format!("field-{ordinal}"),
                            json_pointer: format!("/field-{ordinal}"),
                            cardinality: IndexFieldCardinality::Single as i32,
                            capabilities: vec![IndexFieldCapability::Exact as i32],
                            field_type: Some(FieldType::Keyword(KeywordIndexField {})),
                        })
                        .collect(),
                    physical_order: Vec::new(),
                },
            )),
        });

        assert_eq!(
            validate_create_definition(&request).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn typed_json_rejects_missing_types_and_invalid_or_duplicate_capabilities() {
        let mut missing_type = typed_json_request();
        let Some(IndexSpecification {
            specification: Some(index_specification::Specification::TypedJson(specification)),
        }) = missing_type.specification.as_mut()
        else {
            unreachable!();
        };
        specification.fields[0].field_type = None;
        assert_eq!(
            validate_create_definition(&missing_type)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );

        let mut invalid = typed_json_request();
        let Some(IndexSpecification {
            specification: Some(index_specification::Specification::TypedJson(specification)),
        }) = invalid.specification.as_mut()
        else {
            unreachable!();
        };
        specification.fields[1]
            .capabilities
            .push(IndexFieldCapability::FullText as i32);
        assert_eq!(
            validate_create_definition(&invalid).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );

        let mut duplicate = typed_json_request();
        let Some(IndexSpecification {
            specification: Some(index_specification::Specification::TypedJson(specification)),
        }) = duplicate.specification.as_mut()
        else {
            unreachable!();
        };
        specification.fields[0]
            .capabilities
            .push(IndexFieldCapability::Order as i32);
        assert_eq!(
            validate_create_definition(&duplicate).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn typed_date_accepts_only_date_capabilities() {
        let mut request = typed_json_request();
        let Some(IndexSpecification {
            specification: Some(index_specification::Specification::TypedJson(specification)),
        }) = request.specification.as_mut()
        else {
            unreachable!();
        };
        specification.fields[0].field_type = Some(FieldType::Date(DateIndexField {
            strftime_pattern: String::new(),
        }));
        specification.fields[0].capabilities = vec![
            IndexFieldCapability::Exact as i32,
            IndexFieldCapability::Range as i32,
            IndexFieldCapability::Order as i32,
            IndexFieldCapability::Facet as i32,
        ];
        assert!(validate_typed_json_field(&specification.fields[0]).is_ok());

        specification.fields[0]
            .capabilities
            .push(IndexFieldCapability::Aggregate as i32);
        assert_eq!(
            validate_typed_json_field(&specification.fields[0])
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );

        specification.fields[0].capabilities.pop();
        let Some(FieldType::Date(date)) = specification.fields[0].field_type.as_mut() else {
            unreachable!();
        };
        date.strftime_pattern = "%Y-%B-%d".into();
        assert_eq!(
            validate_typed_json_field(&specification.fields[0])
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn physical_order_requires_unique_known_single_valued_fields_and_valid_directions() {
        let mut unknown = typed_json_request();
        let Some(IndexSpecification {
            specification: Some(index_specification::Specification::TypedJson(specification)),
        }) = unknown.specification.as_mut()
        else {
            unreachable!();
        };
        specification.physical_order[0].field = "unknown".into();
        assert_eq!(
            validate_create_definition(&unknown).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );

        let mut repeated = typed_json_request();
        let Some(IndexSpecification {
            specification: Some(index_specification::Specification::TypedJson(specification)),
        }) = repeated.specification.as_mut()
        else {
            unreachable!();
        };
        specification
            .physical_order
            .push(specification.physical_order[0].clone());
        assert_eq!(
            validate_create_definition(&repeated).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );

        let mut multi_valued = typed_json_request();
        let Some(IndexSpecification {
            specification: Some(index_specification::Specification::TypedJson(specification)),
        }) = multi_valued.specification.as_mut()
        else {
            unreachable!();
        };
        specification.physical_order[0].field = "ecosystems".into();
        assert_eq!(
            validate_create_definition(&multi_valued)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );

        let mut bad_direction = typed_json_request();
        let Some(IndexSpecification {
            specification: Some(index_specification::Specification::TypedJson(specification)),
        }) = bad_direction.specification.as_mut()
        else {
            unreachable!();
        };
        specification.physical_order[0].direction = 99;
        assert_eq!(
            validate_create_definition(&bad_direction)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn names_cannot_escape_the_reserved_definition_prefix() {
        let canonical = definition_path("safe-name").unwrap();
        assert_eq!(definition_name(&canonical), Some("safe-name"));
        for invalid in ["", "../other", "a/b", "name\0"] {
            assert!(definition_path(invalid).is_err(), "{invalid:?}");
        }
        for invalid_path in [
            "_keldra/indices/v3/definitions/safe-name",
            "_keldra/indices/v4/definitions/a/b",
            "_keldra/indices/v4/definitions/..",
        ] {
            assert_eq!(definition_name(invalid_path), None, "{invalid_path:?}");
        }
    }

    #[test]
    fn path_prefix_matching_respects_segment_boundaries() {
        assert!(path_matches_prefix("anything/here", ""));
        assert!(path_matches_prefix("model", "model"));
        assert!(path_matches_prefix("model/weights", "model"));
        assert!(path_matches_prefix("model/weights", "model/"));
        assert!(!path_matches_prefix("models/weights", "model"));
        assert!(!path_matches_prefix("model", "model/"));
    }

    #[test]
    fn removed_index_kinds_are_rejected_at_definition_admission() {
        let mut request = request();
        request.specification = Some(IndexSpecification {
            specification: Some(index_specification::Specification::Tensor(
                TensorIndexSpec {
                    model_id: "encoder-v1".into(),
                },
            )),
        });
        assert_eq!(
            StoredIndexDefinition::create("tenant".into(), request.clone(), 91)
                .unwrap_err()
                .code(),
            tonic::Code::Unimplemented
        );

        let mut invalid = request;
        invalid.specification = Some(IndexSpecification {
            specification: Some(index_specification::Specification::Tensor(
                TensorIndexSpec {
                    model_id: String::new(),
                },
            )),
        });
        assert_eq!(
            validate_create_definition(&invalid).unwrap_err().code(),
            tonic::Code::Unimplemented
        );
    }
}
