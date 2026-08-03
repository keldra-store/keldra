use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexKind {
    Path,
    MetadataFilter,
    TypedJson,
    FullText,
    Vector,
    Hybrid,
    PersonalDbRowMetadata,
    GitSource,
    Tensor,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DocumentRef {
    pub path: String,
    pub version: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryHit {
    pub document: DocumentRef,
    pub score: Option<f32>,
}
