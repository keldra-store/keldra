//! Typed JSON and object-metadata indexes.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::key::push_component;
use crate::{
    DocumentRef, IndexArtifacts, IndexDirectoryRead, IndexError, PagedMap, PagedMapBuilder,
};

const TYPED_POSTINGS_FILE: &str = "typed-json/postings.map";
const TYPED_ROWS_FILE: &str = "typed-json/rows.map";
const METADATA_POSTINGS_FILE: &str = "metadata/postings.map";
const METADATA_ROWS_FILE: &str = "metadata/rows.map";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Number(f64),
    String(String),
}

impl ScalarValue {
    pub fn from_json(value: &serde_json::Value) -> Option<Self> {
        match value {
            serde_json::Value::Null => Some(Self::Null),
            serde_json::Value::Bool(value) => Some(Self::Boolean(*value)),
            serde_json::Value::Number(value) => value.as_f64().map(Self::Number),
            serde_json::Value::String(value) => Some(Self::String(value.clone())),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => None,
        }
    }

    fn compare(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Null, Self::Null) => Some(Ordering::Equal),
            (Self::Boolean(left), Self::Boolean(right)) => Some(left.cmp(right)),
            (Self::Number(left), Self::Number(right)) => left.partial_cmp(right),
            (Self::String(left), Self::String(right)) => Some(left.cmp(right)),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypedField {
    pub name: String,
    pub json_pointer: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypedJsonDefinition {
    pub fields: Vec<TypedField>,
}

impl TypedJsonDefinition {
    pub fn validate(&self) -> Result<(), IndexError> {
        if self.fields.is_empty() {
            return Err(IndexError::InvalidDefinition(
                "typed JSON index needs at least one field".into(),
            ));
        }
        let mut names = BTreeSet::new();
        for field in &self.fields {
            if field.name.is_empty()
                || field.name.contains('\0')
                || (!field.json_pointer.is_empty() && !field.json_pointer.starts_with('/'))
            {
                return Err(IndexError::InvalidDefinition(format!(
                    "invalid typed JSON field `{}`",
                    field.name
                )));
            }
            if !names.insert(&field.name) {
                return Err(IndexError::InvalidDefinition(format!(
                    "duplicate typed JSON field `{}`",
                    field.name
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TypedJsonDocument {
    pub document: DocumentRef,
    pub value: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TypedPredicate {
    Exists {
        field: String,
    },
    Equal {
        field: String,
        value: ScalarValue,
    },
    In {
        field: String,
        values: Vec<ScalarValue>,
    },
    LessThan {
        field: String,
        value: ScalarValue,
    },
    LessThanOrEqual {
        field: String,
        value: ScalarValue,
    },
    GreaterThan {
        field: String,
        value: ScalarValue,
    },
    GreaterThanOrEqual {
        field: String,
        value: ScalarValue,
    },
    Prefix {
        field: String,
        prefix: String,
    },
}

impl TypedPredicate {
    fn field(&self) -> &str {
        match self {
            Self::Exists { field }
            | Self::Equal { field, .. }
            | Self::In { field, .. }
            | Self::LessThan { field, .. }
            | Self::LessThanOrEqual { field, .. }
            | Self::GreaterThan { field, .. }
            | Self::GreaterThanOrEqual { field, .. }
            | Self::Prefix { field, .. } => field,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypedOrder {
    pub field: String,
    pub descending: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TypedQuery {
    pub predicates: Vec<TypedPredicate>,
    pub order: Vec<TypedOrder>,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TypedHit {
    pub document: DocumentRef,
    pub fields: BTreeMap<String, Vec<ScalarValue>>,
}

pub struct TypedJsonEngine;

impl TypedJsonEngine {
    pub fn build(
        definition: &TypedJsonDefinition,
        documents: impl IntoIterator<Item = TypedJsonDocument>,
    ) -> Result<IndexArtifacts, IndexError> {
        definition.validate()?;
        build_typed(definition, documents, TYPED_POSTINGS_FILE, TYPED_ROWS_FILE)
    }

    pub async fn query<D: IndexDirectoryRead>(
        directory: &D,
        definition: &TypedJsonDefinition,
        query: &TypedQuery,
    ) -> Result<Vec<TypedHit>, IndexError> {
        query_typed(
            directory,
            definition,
            query,
            TYPED_POSTINGS_FILE,
            TYPED_ROWS_FILE,
        )
        .await
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetadataDocument {
    pub document: DocumentRef,
    pub metadata: serde_json::Map<String, serde_json::Value>,
}

pub struct MetadataFilterEngine;

impl MetadataFilterEngine {
    pub fn build(
        definition: &TypedJsonDefinition,
        documents: impl IntoIterator<Item = MetadataDocument>,
    ) -> Result<IndexArtifacts, IndexError> {
        definition.validate()?;
        build_typed(
            definition,
            documents.into_iter().map(|document| TypedJsonDocument {
                document: document.document,
                value: serde_json::Value::Object(document.metadata),
            }),
            METADATA_POSTINGS_FILE,
            METADATA_ROWS_FILE,
        )
    }

    pub fn build_fields(
        fields: impl IntoIterator<Item = String>,
        documents: impl IntoIterator<Item = MetadataDocument>,
    ) -> Result<(TypedJsonDefinition, IndexArtifacts), IndexError> {
        let definition = TypedJsonDefinition {
            fields: fields
                .into_iter()
                .map(|name| TypedField {
                    json_pointer: format!("/{}", escape_json_pointer(&name)),
                    name,
                })
                .collect(),
        };
        definition.validate()?;
        let artifacts = Self::build(&definition, documents)?;
        Ok((definition, artifacts))
    }

    pub async fn query<D: IndexDirectoryRead>(
        directory: &D,
        definition: &TypedJsonDefinition,
        query: &TypedQuery,
    ) -> Result<Vec<TypedHit>, IndexError> {
        query_typed(
            directory,
            definition,
            query,
            METADATA_POSTINGS_FILE,
            METADATA_ROWS_FILE,
        )
        .await
    }
}

fn build_typed(
    definition: &TypedJsonDefinition,
    documents: impl IntoIterator<Item = TypedJsonDocument>,
    postings_file: &str,
    rows_file: &str,
) -> Result<IndexArtifacts, IndexError> {
    let mut postings = PagedMapBuilder::default();
    let mut rows = PagedMapBuilder::default();
    for document in documents {
        if document.document.path.is_empty() || document.document.path.contains('\0') {
            return Err(IndexError::InvalidDefinition(
                "typed JSON document path must be non-empty and contain no NUL".into(),
            ));
        }
        let mut extracted = BTreeMap::<String, Vec<ScalarValue>>::new();
        for field in &definition.fields {
            let Some(value) = document.value.pointer(&field.json_pointer) else {
                continue;
            };
            let values = extract_scalars(value);
            if values.is_empty() {
                continue;
            }
            let mut encoded_seen = BTreeSet::new();
            for value in &values {
                let encoded = encode_scalar(value)?;
                if !encoded_seen.insert(encoded.clone()) {
                    continue;
                }
                let mut key = field_prefix(&field.name)?;
                key.extend_from_slice(&encoded);
                key.extend_from_slice(document.document.path.as_bytes());
                postings.insert(key, encode_json(&document.document)?)?;
            }
            extracted.insert(field.name.clone(), values);
        }
        rows.insert(
            document.document.path.as_bytes().to_vec(),
            encode_json(&TypedHit {
                document: document.document,
                fields: extracted,
            })?,
        )?;
    }
    let mut artifacts = IndexArtifacts::default();
    artifacts.insert(postings_file, postings.finish()?)?;
    artifacts.insert(rows_file, rows.finish()?)?;
    Ok(artifacts)
}

async fn query_typed<D: IndexDirectoryRead>(
    directory: &D,
    definition: &TypedJsonDefinition,
    query: &TypedQuery,
    postings_file: &str,
    rows_file: &str,
) -> Result<Vec<TypedHit>, IndexError> {
    definition.validate()?;
    let field_names = definition
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<BTreeSet<_>>();
    for field in query
        .predicates
        .iter()
        .map(TypedPredicate::field)
        .chain(query.order.iter().map(|order| order.field.as_str()))
    {
        if !field_names.contains(field) {
            return Err(IndexError::InvalidQuery(format!(
                "field `{field}` is not part of this index"
            )));
        }
    }
    if query.limit == 0 {
        return Ok(Vec::new());
    }
    let postings = PagedMap::open(directory.open_file(postings_file).await?).await?;
    let rows = PagedMap::open(directory.open_file(rows_file).await?).await?;
    let candidates = if query.predicates.is_empty() {
        rows.scan_all()
            .await?
            .into_iter()
            .map(|(_, value)| decode_json::<TypedHit>(&value).map(|hit| hit.document))
            .collect::<Result<BTreeSet<_>, _>>()?
    } else {
        let mut intersection: Option<BTreeSet<DocumentRef>> = None;
        for predicate in &query.predicates {
            let matching = predicate_matches(&postings, predicate).await?;
            intersection = Some(match intersection {
                None => matching,
                Some(current) => current.intersection(&matching).cloned().collect(),
            });
            if intersection.as_ref().is_some_and(BTreeSet::is_empty) {
                return Ok(Vec::new());
            }
        }
        intersection.unwrap_or_default()
    };
    let mut hits = Vec::with_capacity(candidates.len().min(query.limit));
    for document in candidates {
        let Some(value) = rows.get(document.path.as_bytes()).await? else {
            return Err(IndexError::InvalidFormat(
                "posting references missing typed row",
            ));
        };
        let hit: TypedHit = decode_json(&value)?;
        if hit.document == document {
            hits.push(hit);
        }
    }
    hits.sort_by(|left, right| compare_hits(left, right, &query.order));
    hits.truncate(query.limit);
    Ok(hits)
}

async fn predicate_matches<F: crate::IndexFileRead>(
    postings: &PagedMap<F>,
    predicate: &TypedPredicate,
) -> Result<BTreeSet<DocumentRef>, IndexError> {
    let field = field_prefix(predicate.field())?;
    let records = match predicate {
        TypedPredicate::Equal { value, .. } => {
            let mut prefix = field.clone();
            prefix.extend_from_slice(&encode_scalar(value)?);
            postings.scan_prefix(&prefix, None, usize::MAX).await?
        }
        TypedPredicate::In { values, .. } => {
            let mut records = Vec::new();
            for value in values {
                let mut prefix = field.clone();
                prefix.extend_from_slice(&encode_scalar(value)?);
                records.extend(postings.scan_prefix(&prefix, None, usize::MAX).await?);
            }
            records
        }
        TypedPredicate::Exists { .. }
        | TypedPredicate::LessThan { .. }
        | TypedPredicate::LessThanOrEqual { .. }
        | TypedPredicate::GreaterThan { .. }
        | TypedPredicate::GreaterThanOrEqual { .. }
        | TypedPredicate::Prefix { .. } => postings.scan_prefix(&field, None, usize::MAX).await?,
    };
    let mut matches = BTreeSet::new();
    for (key, value) in records {
        let (scalar, _) = decode_posting_scalar(&key[field.len()..])?;
        if predicate_accepts(predicate, &scalar) {
            matches.insert(decode_json(&value)?);
        }
    }
    Ok(matches)
}

fn predicate_accepts(predicate: &TypedPredicate, actual: &ScalarValue) -> bool {
    match predicate {
        TypedPredicate::Exists { .. } => true,
        TypedPredicate::Equal { value, .. } => actual == value,
        TypedPredicate::In { values, .. } => values.contains(actual),
        TypedPredicate::LessThan { value, .. } => actual.compare(value) == Some(Ordering::Less),
        TypedPredicate::LessThanOrEqual { value, .. } => {
            matches!(
                actual.compare(value),
                Some(Ordering::Less | Ordering::Equal)
            )
        }
        TypedPredicate::GreaterThan { value, .. } => {
            actual.compare(value) == Some(Ordering::Greater)
        }
        TypedPredicate::GreaterThanOrEqual { value, .. } => {
            matches!(
                actual.compare(value),
                Some(Ordering::Greater | Ordering::Equal)
            )
        }
        TypedPredicate::Prefix { prefix, .. } => {
            matches!(actual, ScalarValue::String(value) if value.starts_with(prefix))
        }
    }
}

fn compare_hits(left: &TypedHit, right: &TypedHit, order: &[TypedOrder]) -> Ordering {
    for specification in order {
        let left_value = left
            .fields
            .get(&specification.field)
            .and_then(|values| values.first());
        let right_value = right
            .fields
            .get(&specification.field)
            .and_then(|values| values.first());
        let ordering = match (left_value, right_value) {
            (Some(left), Some(right)) => left.compare(right).unwrap_or(Ordering::Equal),
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
        };
        let ordering = if specification.descending {
            ordering.reverse()
        } else {
            ordering
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.document.cmp(&right.document)
}

fn extract_scalars(value: &serde_json::Value) -> Vec<ScalarValue> {
    match value {
        serde_json::Value::Array(values) => {
            values.iter().filter_map(ScalarValue::from_json).collect()
        }
        value => ScalarValue::from_json(value).into_iter().collect(),
    }
}

fn field_prefix(field: &str) -> Result<Vec<u8>, IndexError> {
    let mut prefix = Vec::new();
    push_component(&mut prefix, field.as_bytes())?;
    Ok(prefix)
}

fn encode_scalar(value: &ScalarValue) -> Result<Vec<u8>, IndexError> {
    let mut output = Vec::new();
    match value {
        ScalarValue::Null => output.push(0),
        ScalarValue::Boolean(value) => {
            output.push(1);
            output.push(u8::from(*value));
        }
        ScalarValue::Number(value) => {
            if !value.is_finite() {
                return Err(IndexError::InvalidDefinition(
                    "typed JSON numbers must be finite".into(),
                ));
            }
            output.push(2);
            // JSON equality treats negative and positive zero as the same
            // number, so they must share one posting key.
            let bits = if *value == 0.0 {
                0.0f64.to_bits()
            } else {
                value.to_bits()
            };
            let sortable = if bits >> 63 == 1 {
                !bits
            } else {
                bits ^ (1 << 63)
            };
            output.extend_from_slice(&sortable.to_be_bytes());
        }
        ScalarValue::String(value) => {
            output.push(3);
            for byte in value.as_bytes() {
                if *byte == 0 {
                    output.extend_from_slice(&[0, 0xff]);
                } else {
                    output.push(*byte);
                }
            }
            output.extend_from_slice(&[0, 0]);
        }
    }
    Ok(output)
}

fn decode_posting_scalar(bytes: &[u8]) -> Result<(ScalarValue, usize), IndexError> {
    let Some(tag) = bytes.first() else {
        return Err(IndexError::InvalidFormat("missing typed scalar tag"));
    };
    match tag {
        0 => Ok((ScalarValue::Null, 1)),
        1 if bytes.len() >= 2 && bytes[1] <= 1 => Ok((ScalarValue::Boolean(bytes[1] == 1), 2)),
        2 if bytes.len() >= 9 => {
            let sortable = u64::from_be_bytes(bytes[1..9].try_into().unwrap());
            let bits = if sortable >> 63 == 1 {
                sortable ^ (1 << 63)
            } else {
                !sortable
            };
            let value = f64::from_bits(bits);
            if !value.is_finite() {
                return Err(IndexError::InvalidFormat("non-finite typed number"));
            }
            Ok((ScalarValue::Number(value), 9))
        }
        3 => {
            let mut value = Vec::new();
            let mut cursor = 1;
            loop {
                let Some(byte) = bytes.get(cursor).copied() else {
                    return Err(IndexError::InvalidFormat("unterminated typed string"));
                };
                cursor += 1;
                if byte != 0 {
                    value.push(byte);
                    continue;
                }
                let Some(escaped) = bytes.get(cursor).copied() else {
                    return Err(IndexError::InvalidFormat(
                        "unterminated typed string escape",
                    ));
                };
                cursor += 1;
                match escaped {
                    0 => break,
                    0xff => value.push(0),
                    _ => return Err(IndexError::InvalidFormat("invalid typed string escape")),
                }
            }
            Ok((
                ScalarValue::String(
                    String::from_utf8(value)
                        .map_err(|_| IndexError::InvalidFormat("typed string UTF-8"))?,
                ),
                cursor,
            ))
        }
        _ => Err(IndexError::InvalidFormat("invalid typed scalar")),
    }
}

fn encode_json<T: Serialize>(value: &T) -> Result<Vec<u8>, IndexError> {
    serde_json::to_vec(value).map_err(|error| IndexError::Encode(error.to_string()))
}

fn decode_json<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, IndexError> {
    serde_json::from_slice(bytes).map_err(|error| IndexError::Decode(error.to_string()))
}

fn escape_json_pointer(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::io::tests::MemoryDirectory;

    use super::*;

    fn directory(artifacts: IndexArtifacts) -> MemoryDirectory {
        MemoryDirectory::new(artifacts.into_files().map(|file| (file.name, file.bytes)))
    }

    fn definition() -> TypedJsonDefinition {
        TypedJsonDefinition {
            fields: vec![
                TypedField {
                    name: "status".into(),
                    json_pointer: "/status".into(),
                },
                TypedField {
                    name: "amount".into(),
                    json_pointer: "/amount".into(),
                },
                TypedField {
                    name: "tags".into(),
                    json_pointer: "/tags".into(),
                },
            ],
        }
    }

    #[tokio::test]
    async fn typed_query_intersects_array_and_range_postings() {
        let definition = definition();
        let artifacts = TypedJsonEngine::build(
            &definition,
            [
                TypedJsonDocument {
                    document: DocumentRef {
                        path: "/a".into(),
                        version: 1,
                    },
                    value: json!({"status":"open","amount":50,"tags":["urgent","red"]}),
                },
                TypedJsonDocument {
                    document: DocumentRef {
                        path: "/b".into(),
                        version: 2,
                    },
                    value: json!({"status":"open","amount":20,"tags":["blue"]}),
                },
                TypedJsonDocument {
                    document: DocumentRef {
                        path: "/c".into(),
                        version: 3,
                    },
                    value: json!({"status":"closed","amount":100,"tags":["urgent"]}),
                },
            ],
        )
        .unwrap();
        let hits = TypedJsonEngine::query(
            &directory(artifacts),
            &definition,
            &TypedQuery {
                predicates: vec![
                    TypedPredicate::Equal {
                        field: "tags".into(),
                        value: ScalarValue::String("urgent".into()),
                    },
                    TypedPredicate::GreaterThan {
                        field: "amount".into(),
                        value: ScalarValue::Number(40.0),
                    },
                ],
                order: vec![TypedOrder {
                    field: "amount".into(),
                    descending: true,
                }],
                limit: 10,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            hits.iter()
                .map(|hit| hit.document.path.as_str())
                .collect::<Vec<_>>(),
            ["/c", "/a"]
        );
    }

    #[tokio::test]
    async fn metadata_keys_with_slashes_use_json_pointer_escaping() {
        let mut metadata = serde_json::Map::new();
        metadata.insert("media/type".into(), json!("audio"));
        let (definition, artifacts) = MetadataFilterEngine::build_fields(
            ["media/type".into()],
            [MetadataDocument {
                document: DocumentRef {
                    path: "/song".into(),
                    version: 4,
                },
                metadata,
            }],
        )
        .unwrap();
        let hits = MetadataFilterEngine::query(
            &directory(artifacts),
            &definition,
            &TypedQuery {
                predicates: vec![TypedPredicate::Equal {
                    field: "media/type".into(),
                    value: ScalarValue::String("audio".into()),
                }],
                order: vec![],
                limit: 10,
            },
        )
        .await
        .unwrap();
        assert_eq!(hits[0].document.path, "/song");
    }
}
