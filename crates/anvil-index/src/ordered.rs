//! Ordered path indexes.

use serde::{Deserialize, Serialize};

use crate::{
    DocumentRef, IndexArtifacts, IndexDirectoryRead, IndexError, PagedMap, PagedMapBuilder,
};

pub const PATH_FILE: &str = "path/entries.map";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathDocument {
    pub path: String,
    pub version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathQuery<'a> {
    pub prefix: &'a str,
    pub after_path: Option<&'a str>,
    pub limit: usize,
}

pub struct PathEngine;

impl PathEngine {
    pub fn build(
        documents: impl IntoIterator<Item = PathDocument>,
    ) -> Result<IndexArtifacts, IndexError> {
        let mut map = PagedMapBuilder::default();
        for document in documents {
            validate_path(&document.path)?;
            map.insert(
                document.path.as_bytes().to_vec(),
                document.version.to_le_bytes().to_vec(),
            )?;
        }
        let mut artifacts = IndexArtifacts::default();
        artifacts.insert(PATH_FILE, map.finish()?)?;
        Ok(artifacts)
    }

    pub async fn query<D: IndexDirectoryRead>(
        directory: &D,
        query: PathQuery<'_>,
    ) -> Result<Vec<DocumentRef>, IndexError> {
        validate_prefix(query.prefix)?;
        if let Some(after) = query.after_path {
            validate_path(after)?;
        }
        let map = PagedMap::open(directory.open_file(PATH_FILE).await?).await?;
        map.scan_prefix(
            query.prefix.as_bytes(),
            query.after_path.map(str::as_bytes),
            query.limit,
        )
        .await?
        .into_iter()
        .map(|(path, version)| {
            if version.len() != 8 {
                return Err(IndexError::InvalidFormat("path index version"));
            }
            Ok(DocumentRef {
                path: String::from_utf8(path)
                    .map_err(|_| IndexError::InvalidFormat("path index UTF-8"))?,
                version: u64::from_le_bytes(version.try_into().unwrap()),
            })
        })
        .collect()
    }
}

fn validate_path(path: &str) -> Result<(), IndexError> {
    if path.is_empty() || path.contains('\0') {
        return Err(IndexError::InvalidDefinition(
            "indexed object path must be non-empty and contain no NUL".into(),
        ));
    }
    Ok(())
}

fn validate_prefix(prefix: &str) -> Result<(), IndexError> {
    if prefix.contains('\0') {
        return Err(IndexError::InvalidQuery(
            "path prefix must contain no NUL".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::io::tests::MemoryDirectory;

    use super::*;

    #[tokio::test]
    async fn path_query_is_complete_and_stably_paginated() {
        let artifacts = PathEngine::build([
            PathDocument {
                path: "tenant/1/a".into(),
                version: 3,
            },
            PathDocument {
                path: "tenant/1/b".into(),
                version: 5,
            },
            PathDocument {
                path: "tenant/2/a".into(),
                version: 7,
            },
        ])
        .unwrap();
        let directory =
            MemoryDirectory::new(artifacts.into_files().map(|file| (file.name, file.bytes)));
        let first = PathEngine::query(
            &directory,
            PathQuery {
                prefix: "tenant/1/",
                after_path: None,
                limit: 1,
            },
        )
        .await
        .unwrap();
        assert_eq!(first[0].path, "tenant/1/a");
        let second = PathEngine::query(
            &directory,
            PathQuery {
                prefix: "tenant/1/",
                after_path: Some(&first[0].path),
                limit: 20,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            second,
            [DocumentRef {
                path: "tenant/1/b".into(),
                version: 5
            }]
        );
    }
}
