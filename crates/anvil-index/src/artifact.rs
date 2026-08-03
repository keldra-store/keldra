use std::collections::BTreeMap;

use crate::IndexError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeneratedFile {
    pub name: String,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexArtifacts {
    files: BTreeMap<String, Vec<u8>>,
}

impl IndexArtifacts {
    pub fn insert(&mut self, name: impl Into<String>, bytes: Vec<u8>) -> Result<(), IndexError> {
        let name = name.into();
        if name.is_empty() || name.starts_with('/') || name.contains("..") || name.contains('\0') {
            return Err(IndexError::InvalidDefinition(
                "index artifact name must be a relative canonical name".into(),
            ));
        }
        if self.files.insert(name.clone(), bytes).is_some() {
            return Err(IndexError::InvalidDefinition(format!(
                "duplicate generated file `{name}`"
            )));
        }
        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<&[u8]> {
        self.files.get(name).map(Vec::as_slice)
    }

    pub fn iter(&self) -> impl Iterator<Item = GeneratedFile> + '_ {
        self.files.iter().map(|(name, bytes)| GeneratedFile {
            name: name.clone(),
            bytes: bytes.clone(),
        })
    }

    pub fn into_files(self) -> impl Iterator<Item = GeneratedFile> {
        self.files
            .into_iter()
            .map(|(name, bytes)| GeneratedFile { name, bytes })
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    pub fn len(&self) -> usize {
        self.files.len()
    }
}
