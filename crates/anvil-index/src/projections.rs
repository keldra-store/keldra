//! Ordered projection formats used by Git, model tensors and Hugging Face
//! manifests.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::key::{component_prefix, composite_key};
use crate::{IndexArtifacts, IndexDirectoryRead, IndexError, PagedMap, PagedMapBuilder};

const GIT_PATH_FILE: &str = "git/by-path.map";
const GIT_OBJECT_FILE: &str = "git/by-object.map";
const TENSOR_FILE: &str = "tensor/by-name.map";
const HF_FILE: &str = "hugging-face/by-filename.map";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitSourceRecord {
    pub repository_id: String,
    pub commit_id: String,
    pub tree_path: String,
    pub object_id: String,
    pub pack_path: String,
    pub pack_version: u64,
    pub offset: u64,
    pub length: u64,
}

pub struct GitSourceEngine;

impl GitSourceEngine {
    pub fn build(
        records: impl IntoIterator<Item = GitSourceRecord>,
    ) -> Result<IndexArtifacts, IndexError> {
        let mut by_path = PagedMapBuilder::default();
        let mut by_object = BTreeMap::<Vec<u8>, Vec<GitSourceRecord>>::new();
        for record in records {
            validate_text("repository ID", &record.repository_id)?;
            validate_text("commit ID", &record.commit_id)?;
            validate_text("Git tree path", &record.tree_path)?;
            validate_text("Git object ID", &record.object_id)?;
            let mut path_key =
                component_prefix(&[record.repository_id.as_bytes(), record.commit_id.as_bytes()])?;
            path_key.extend_from_slice(record.tree_path.as_bytes());
            by_path.insert(path_key, encode(&record)?)?;
            let object_key =
                composite_key(&[record.repository_id.as_bytes(), record.object_id.as_bytes()])?;
            by_object.entry(object_key).or_default().push(record);
        }
        let mut object_map = PagedMapBuilder::default();
        for (key, mut locations) in by_object {
            locations.sort_by(|left, right| {
                (&left.commit_id, &left.tree_path).cmp(&(&right.commit_id, &right.tree_path))
            });
            object_map.insert(key, encode(&locations)?)?;
        }
        let mut artifacts = IndexArtifacts::default();
        artifacts.insert(GIT_PATH_FILE, by_path.finish()?)?;
        artifacts.insert(GIT_OBJECT_FILE, object_map.finish()?)?;
        Ok(artifacts)
    }

    pub async fn get_by_path<D: IndexDirectoryRead>(
        directory: &D,
        repository_id: &str,
        commit_id: &str,
        tree_path: &str,
    ) -> Result<Option<GitSourceRecord>, IndexError> {
        let map = PagedMap::open(directory.open_file(GIT_PATH_FILE).await?).await?;
        let mut key = component_prefix(&[repository_id.as_bytes(), commit_id.as_bytes()])?;
        key.extend_from_slice(tree_path.as_bytes());
        map.get(&key).await?.map(|bytes| decode(&bytes)).transpose()
    }

    pub async fn list_tree<D: IndexDirectoryRead>(
        directory: &D,
        repository_id: &str,
        commit_id: &str,
        tree_prefix: &str,
        after_path: Option<&str>,
        limit: usize,
    ) -> Result<Vec<GitSourceRecord>, IndexError> {
        let map = PagedMap::open(directory.open_file(GIT_PATH_FILE).await?).await?;
        let base = component_prefix(&[repository_id.as_bytes(), commit_id.as_bytes()])?;
        let mut prefix = base.clone();
        prefix.extend_from_slice(tree_prefix.as_bytes());
        let after = after_path.map(|after| {
            let mut key = base;
            key.extend_from_slice(after.as_bytes());
            key
        });
        map.scan_prefix(&prefix, after.as_deref(), limit)
            .await?
            .into_iter()
            .map(|(_, bytes)| decode(&bytes))
            .collect()
    }

    pub async fn get_object<D: IndexDirectoryRead>(
        directory: &D,
        repository_id: &str,
        object_id: &str,
    ) -> Result<Vec<GitSourceRecord>, IndexError> {
        let map = PagedMap::open(directory.open_file(GIT_OBJECT_FILE).await?).await?;
        let key = composite_key(&[repository_id.as_bytes(), object_id.as_bytes()])?;
        map.get(&key)
            .await?
            .map(|bytes| decode(&bytes))
            .transpose()
            .map(Option::unwrap_or_default)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TensorRecord {
    pub model_id: String,
    pub tensor_name: String,
    pub source_path: String,
    pub source_version: u64,
    pub offset: u64,
    pub length: u64,
    pub dtype: String,
    pub shape: Vec<u64>,
}

pub struct TensorProjectionEngine;

impl TensorProjectionEngine {
    pub fn build(
        tensors: impl IntoIterator<Item = TensorRecord>,
    ) -> Result<IndexArtifacts, IndexError> {
        build_projection(TENSOR_FILE, tensors, |tensor| {
            composite_key(&[tensor.model_id.as_bytes(), tensor.tensor_name.as_bytes()])
        })
    }

    pub async fn get<D: IndexDirectoryRead>(
        directory: &D,
        model_id: &str,
        tensor_name: &str,
    ) -> Result<Option<TensorRecord>, IndexError> {
        projection_get(
            directory,
            TENSOR_FILE,
            &composite_key(&[model_id.as_bytes(), tensor_name.as_bytes()])?,
        )
        .await
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HuggingFaceManifestRecord {
    pub repository_id: String,
    pub revision: String,
    pub filename: String,
    pub object_path: String,
    pub object_version: u64,
    pub content_hash: String,
    pub length: u64,
    pub media_type: Option<String>,
}

pub struct HuggingFaceManifestEngine;

impl HuggingFaceManifestEngine {
    pub fn build(
        records: impl IntoIterator<Item = HuggingFaceManifestRecord>,
    ) -> Result<IndexArtifacts, IndexError> {
        build_projection(HF_FILE, records, |record| {
            composite_key(&[
                record.repository_id.as_bytes(),
                record.revision.as_bytes(),
                record.filename.as_bytes(),
            ])
        })
    }

    pub async fn get<D: IndexDirectoryRead>(
        directory: &D,
        repository_id: &str,
        revision: &str,
        filename: &str,
    ) -> Result<Option<HuggingFaceManifestRecord>, IndexError> {
        projection_get(
            directory,
            HF_FILE,
            &composite_key(&[
                repository_id.as_bytes(),
                revision.as_bytes(),
                filename.as_bytes(),
            ])?,
        )
        .await
    }
}

fn build_projection<T: Serialize>(
    file_name: &str,
    records: impl IntoIterator<Item = T>,
    key: impl Fn(&T) -> Result<Vec<u8>, IndexError>,
) -> Result<IndexArtifacts, IndexError> {
    let mut map = PagedMapBuilder::default();
    for record in records {
        map.insert(key(&record)?, encode(&record)?)?;
    }
    let mut artifacts = IndexArtifacts::default();
    artifacts.insert(file_name, map.finish()?)?;
    Ok(artifacts)
}

async fn projection_get<D: IndexDirectoryRead, T: DeserializeOwned>(
    directory: &D,
    file_name: &str,
    key: &[u8],
) -> Result<Option<T>, IndexError> {
    let map = PagedMap::open(directory.open_file(file_name).await?).await?;
    map.get(key).await?.map(|bytes| decode(&bytes)).transpose()
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, IndexError> {
    serde_json::to_vec(value).map_err(|error| IndexError::Encode(error.to_string()))
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, IndexError> {
    serde_json::from_slice(bytes).map_err(|error| IndexError::Decode(error.to_string()))
}

fn validate_text(label: &str, value: &str) -> Result<(), IndexError> {
    if value.is_empty() || value.contains('\0') {
        return Err(IndexError::InvalidDefinition(format!(
            "{label} must be non-empty and contain no NUL"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::io::tests::MemoryDirectory;

    use super::*;

    fn directory(artifacts: IndexArtifacts) -> MemoryDirectory {
        MemoryDirectory::new(artifacts.into_files().map(|file| (file.name, file.bytes)))
    }

    #[tokio::test]
    async fn git_supports_exact_object_and_prefix_tree_queries() {
        let records = [
            GitSourceRecord {
                repository_id: "repo".into(),
                commit_id: "abc".into(),
                tree_path: "src/lib.rs".into(),
                object_id: "111".into(),
                pack_path: "/packs/1".into(),
                pack_version: 1,
                offset: 10,
                length: 20,
            },
            GitSourceRecord {
                repository_id: "repo".into(),
                commit_id: "abc".into(),
                tree_path: "src/main.rs".into(),
                object_id: "222".into(),
                pack_path: "/packs/1".into(),
                pack_version: 1,
                offset: 30,
                length: 40,
            },
        ];
        let directory = directory(GitSourceEngine::build(records).unwrap());
        assert_eq!(
            GitSourceEngine::get_by_path(&directory, "repo", "abc", "src/lib.rs")
                .await
                .unwrap()
                .unwrap()
                .object_id,
            "111"
        );
        assert_eq!(
            GitSourceEngine::list_tree(&directory, "repo", "abc", "src/", None, 10)
                .await
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            GitSourceEngine::get_object(&directory, "repo", "222")
                .await
                .unwrap()[0]
                .tree_path,
            "src/main.rs"
        );
    }

    #[tokio::test]
    async fn internal_tensor_projection_is_queryable() {
        let tensor = TensorRecord {
            model_id: "model".into(),
            tensor_name: "encoder.weight".into(),
            source_path: "/model.safetensors".into(),
            source_version: 8,
            offset: 64,
            length: 4096,
            dtype: "F32".into(),
            shape: vec![32, 32],
        };
        let tensors = directory(TensorProjectionEngine::build([tensor.clone()]).unwrap());
        assert_eq!(
            TensorProjectionEngine::get(&tensors, "model", "encoder.weight")
                .await
                .unwrap(),
            Some(tensor)
        );
    }
}
