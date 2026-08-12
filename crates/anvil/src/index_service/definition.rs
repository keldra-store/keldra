use anvil_api::v1::index_specification::Specification;
use anvil_api::v1::{
    CreateIndexRequest, IndexDefinition, IndexField, IndexKind, IndexSpecification,
    UpdateIndexRequest,
};
use anvil_atomic_program::MAX_OBJECT_PATH_BYTES;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use tonic::Status;

const STORED_DEFINITION_FORMAT: u16 = 3;
const DEFINITION_PREFIX: &str = "_anvil/indexes/v3/definitions/";
const MAX_INDEX_NAME_BYTES: usize = 128;
const MAX_CONTENT_TYPE_BYTES: usize = 512;
const MAX_COMMAND_ID_BYTES: usize = 256;
const INDEX_ID_CONTEXT: &[u8] = b"anvil.index/id/v1";

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
}

impl StoredIndexDefinition {
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
        })
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
    Ok(format!("{DEFINITION_PREFIX}{name}"))
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
    optional_content_type(request.content_type.clone())?;
    validate_command_id(&request.command_id)?;
    let specification = request
        .specification
        .clone()
        .ok_or_else(|| Status::invalid_argument("index specification is required"))?;
    validate_specification(&specification)?;
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
    optional_content_type(request.content_type.clone())?;
    validate_command_id(&request.command_id)?;
    let specification = request
        .specification
        .clone()
        .ok_or_else(|| Status::invalid_argument("index specification is required"))?;
    validate_specification(&specification)?;
    Ok(specification)
}

fn validate_specification(specification: &IndexSpecification) -> Result<(), Status> {
    match specification.specification.as_ref() {
        Some(Specification::Path(_)) => Ok(()),
        Some(Specification::MetadataFilter(specification)) => {
            const ALLOWED: &[&str] = &[
                "path",
                "version",
                "content_type",
                "content_length",
                "content_hash",
                "committed_at_unix_millis",
            ];
            require_unique_nonempty(&specification.fields, "metadata field")?;
            if specification
                .fields
                .iter()
                .any(|field| !ALLOWED.contains(&field.as_str()))
            {
                return Err(Status::invalid_argument(
                    "metadata index contains an unsupported object-head field",
                ));
            }
            Ok(())
        }
        Some(Specification::TypedJson(specification)) => validate_fields(&specification.fields),
        Some(Specification::FullText(specification)) => {
            if specification.fields.is_empty() {
                return Err(Status::invalid_argument(
                    "full-text index needs at least one field",
                ));
            }
            let mut names = BTreeSet::new();
            for field in &specification.fields {
                validate_field_parts(&field.name, &field.json_pointer)?;
                if !names.insert(field.name.as_str()) {
                    return Err(Status::invalid_argument(
                        "full-text field names must be unique",
                    ));
                }
            }
            Ok(())
        }
        Some(Specification::Vector(specification)) => {
            if specification.dimensions == 0 {
                return Err(Status::invalid_argument(
                    "vector dimensions must be non-zero",
                ));
            }
            validate_json_pointer(&specification.json_pointer)
        }
        Some(Specification::Hybrid(specification)) => {
            let full_text = specification
                .full_text
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("hybrid full-text spec is required"))?;
            let vector = specification
                .vector
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("hybrid vector spec is required"))?;
            validate_specification(&IndexSpecification {
                specification: Some(Specification::FullText(full_text.clone())),
            })?;
            validate_specification(&IndexSpecification {
                specification: Some(Specification::Vector(vector.clone())),
            })?;
            let text_weight = specification.full_text_weight;
            let vector_weight = specification.vector_weight;
            if !text_weight.is_finite()
                || !vector_weight.is_finite()
                || text_weight < 0.0
                || vector_weight < 0.0
                || (text_weight == 0.0) != (vector_weight == 0.0)
            {
                return Err(Status::invalid_argument(
                    "hybrid weights must both be zero (equal) or finite and positive",
                ));
            }
            Ok(())
        }
        Some(Specification::GitSource(specification)) => {
            require_text(&specification.repository_id, "Git repository ID")
        }
        Some(Specification::Tensor(specification)) => {
            require_text(&specification.model_id, "tensor model ID")
        }
        None => Err(Status::invalid_argument("index specification is required")),
    }
}

fn kind_for(specification: &IndexSpecification) -> Result<IndexKind, Status> {
    Ok(match specification.specification.as_ref() {
        Some(Specification::Path(_)) => IndexKind::Path,
        Some(Specification::MetadataFilter(_)) => IndexKind::MetadataFilter,
        Some(Specification::TypedJson(_)) => IndexKind::TypedJson,
        Some(Specification::FullText(_)) => IndexKind::FullText,
        Some(Specification::Vector(_)) => IndexKind::Vector,
        Some(Specification::Hybrid(_)) => IndexKind::Hybrid,
        Some(Specification::GitSource(_)) => IndexKind::GitSource,
        Some(Specification::Tensor(_)) => IndexKind::Tensor,
        None => return Err(Status::data_loss("stored index specification is empty")),
    })
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

#[cfg(test)]
mod tests {
    use anvil_api::v1::{PathIndexSpec, TensorIndexSpec, index_specification};

    use super::*;

    fn request() -> CreateIndexRequest {
        CreateIndexRequest {
            bucket: "objects".into(),
            name: "by-path".into(),
            path_prefix: "tenant/123/".into(),
            content_type: String::new(),
            specification: Some(IndexSpecification {
                specification: Some(index_specification::Specification::Path(PathIndexSpec {})),
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
            definition_path("by-path").unwrap(),
            "_anvil/indexes/v3/definitions/by-path"
        );
        assert_eq!(
            derive_index_id(7, 9, "by-path", "create-index").unwrap(),
            derive_index_id(7, 9, "by-path", "create-index").unwrap()
        );
    }

    #[test]
    fn format_one_definition_is_not_a_compatibility_input() {
        let stored = StoredIndexDefinition::create("tenant".into(), request(), 44).unwrap();
        let mut encoded = serde_json::to_value(stored).unwrap();
        encoded["format"] = serde_json::json!(1);
        let error =
            StoredIndexDefinition::decode(&serde_json::to_vec(&encoded).unwrap()).unwrap_err();
        assert_eq!(error.code(), tonic::Code::DataLoss);
    }

    #[test]
    fn names_cannot_escape_the_reserved_definition_prefix() {
        for invalid in ["", "../other", "a/b", "name\0"] {
            assert!(definition_path(invalid).is_err(), "{invalid:?}");
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
    fn tensor_definition_requires_and_preserves_its_model_identity() {
        let mut request = request();
        request.specification = Some(IndexSpecification {
            specification: Some(index_specification::Specification::Tensor(
                TensorIndexSpec {
                    model_id: "encoder-v1".into(),
                },
            )),
        });
        let stored = StoredIndexDefinition::create("tenant".into(), request.clone(), 91).unwrap();
        let api = stored.to_api(4).unwrap();

        assert_eq!(api.kind, IndexKind::Tensor as i32);
        assert_eq!(api.specification, request.specification);

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
            tonic::Code::InvalidArgument
        );
    }
}
