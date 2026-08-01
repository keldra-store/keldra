use anvil_atomic_program::{
    MAX_OBJECT_BUCKET_BYTES, MAX_OBJECT_PATH_BYTES, MAX_OBJECT_TENANT_BYTES,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ObjectKey {
    tenant: String,
    bucket: String,
    path: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ObjectKeyError {
    #[error("{0} must not be empty")]
    Empty(&'static str),
    #[error("{0} is too long")]
    TooLong(&'static str),
    #[error("tenant and bucket must be canonical components")]
    NonCanonicalComponent,
    #[error("path must contain canonical non-empty relative segments")]
    NonCanonicalPath,
    #[error("object key components must not contain NUL")]
    Nul,
}

impl ObjectKey {
    pub fn new(
        tenant: impl Into<String>,
        bucket: impl Into<String>,
        path: impl Into<String>,
    ) -> Result<Self, ObjectKeyError> {
        let key = Self {
            tenant: tenant.into(),
            bucket: bucket.into(),
            path: path.into(),
        };
        key.validate()?;
        Ok(key)
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut encoded =
            Vec::with_capacity(2 + self.tenant.len() + 2 + self.bucket.len() + 4 + self.path.len());
        push_u16_string(&mut encoded, &self.tenant);
        push_u16_string(&mut encoded, &self.bucket);
        encoded.extend_from_slice(&(self.path.len() as u32).to_be_bytes());
        encoded.extend_from_slice(self.path.as_bytes());
        encoded
    }

    pub(crate) fn bucket_key(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(4 + self.tenant.len() + self.bucket.len());
        push_u16_string(&mut encoded, &self.tenant);
        push_u16_string(&mut encoded, &self.bucket);
        encoded
    }

    fn validate(&self) -> Result<(), ObjectKeyError> {
        for (name, value) in [
            ("tenant", &self.tenant),
            ("bucket", &self.bucket),
            ("path", &self.path),
        ] {
            if value.is_empty() {
                return Err(ObjectKeyError::Empty(name));
            }
            if value.contains('\0') {
                return Err(ObjectKeyError::Nul);
            }
        }
        if self.tenant.len() > MAX_OBJECT_TENANT_BYTES {
            return Err(ObjectKeyError::TooLong("tenant"));
        }
        if self.bucket.len() > MAX_OBJECT_BUCKET_BYTES {
            return Err(ObjectKeyError::TooLong("bucket"));
        }
        if self.path.len() > MAX_OBJECT_PATH_BYTES {
            return Err(ObjectKeyError::TooLong("path"));
        }
        if [&self.tenant, &self.bucket]
            .iter()
            .any(|component| component.contains('/') || component.chars().any(char::is_control))
        {
            return Err(ObjectKeyError::NonCanonicalComponent);
        }
        if self.path.starts_with('/')
            || self.path.ends_with('/')
            || self
                .path
                .split('/')
                .any(|segment| segment.is_empty() || matches!(segment, "." | ".."))
            || self.path.chars().any(char::is_control)
        {
            return Err(ObjectKeyError::NonCanonicalPath);
        }
        Ok(())
    }
}

fn push_u16_string(output: &mut Vec<u8>, value: &str) {
    output.extend_from_slice(&(value.len() as u16).to_be_bytes());
    output.extend_from_slice(value.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoding_is_unambiguous() {
        let left = ObjectKey::new("a", "bc", "d/e").unwrap();
        let right = ObjectKey::new("ab", "c", "d/e").unwrap();
        assert_ne!(left.encode(), right.encode());
    }

    #[test]
    fn rejects_filesystem_style_paths() {
        assert_eq!(
            ObjectKey::new("t", "b", "/a").unwrap_err(),
            ObjectKeyError::NonCanonicalPath
        );
        assert_eq!(
            ObjectKey::new("t", "b", "a//b").unwrap_err(),
            ObjectKeyError::NonCanonicalPath
        );
        assert_eq!(
            ObjectKey::new("t", "b", "a/../b").unwrap_err(),
            ObjectKeyError::NonCanonicalPath
        );
    }

    #[test]
    fn tenant_and_bucket_are_canonical_authorization_components() {
        assert_eq!(
            ObjectKey::new("other/tenant", "b", "a").unwrap_err(),
            ObjectKeyError::NonCanonicalComponent
        );
        assert_eq!(
            ObjectKey::new("t", "bad\nbucket", "a").unwrap_err(),
            ObjectKeyError::NonCanonicalComponent
        );
        assert_eq!(
            ObjectKey::new("t".repeat(MAX_OBJECT_TENANT_BYTES + 1), "b", "a").unwrap_err(),
            ObjectKeyError::TooLong("tenant")
        );
        assert_eq!(
            ObjectKey::new("t", "b".repeat(MAX_OBJECT_BUCKET_BYTES + 1), "a").unwrap_err(),
            ObjectKeyError::TooLong("bucket")
        );
        assert_eq!(
            ObjectKey::new("t", "b", "a".repeat(MAX_OBJECT_PATH_BYTES + 1)).unwrap_err(),
            ObjectKeyError::TooLong("path")
        );
    }
}
