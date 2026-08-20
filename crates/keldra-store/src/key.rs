use keldra_atomic_program::{
    MAX_OBJECT_BUCKET_BYTES, MAX_OBJECT_PATH_BYTES, MAX_OBJECT_TENANT_BYTES,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::authz::StorageTenantId;

/// Persisted object-key encoding. The first byte of every identity-derived key
/// makes later, explicit format migrations possible without guessing how an
/// existing key was encoded.
pub(crate) const STORAGE_KEY_FORMAT_VERSION: u8 = 0x01;
pub(crate) const TENANT_NAME_TYPE: u8 = 0x01;
pub(crate) const BUCKET_NAME_TYPE: u8 = 0x02;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) struct TenantId(pub(crate) u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) struct BucketId(pub(crate) u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BucketIdentity {
    pub(crate) tenant_id: TenantId,
    pub(crate) bucket_id: BucketId,
}

impl BucketIdentity {
    pub(crate) const ENCODED_BYTES: usize = 1 + size_of::<u64>() + size_of::<u64>();

    pub(crate) fn encode(self) -> [u8; Self::ENCODED_BYTES] {
        let mut encoded = [0_u8; Self::ENCODED_BYTES];
        encoded[0] = STORAGE_KEY_FORMAT_VERSION;
        encoded[1..9].copy_from_slice(&self.tenant_id.0.to_be_bytes());
        encoded[9..17].copy_from_slice(&self.bucket_id.0.to_be_bytes());
        encoded
    }

    pub(crate) fn decode(encoded: &[u8]) -> Result<Self, ObjectKeyError> {
        let encoded: &[u8; Self::ENCODED_BYTES] = encoded
            .try_into()
            .map_err(|_| ObjectKeyError::MalformedStorageKey)?;
        if encoded[0] != STORAGE_KEY_FORMAT_VERSION {
            return Err(ObjectKeyError::MalformedStorageKey);
        }
        let tenant_id = u64::from_be_bytes(
            encoded[1..9]
                .try_into()
                .map_err(|_| ObjectKeyError::MalformedStorageKey)?,
        );
        let bucket_id = u64::from_be_bytes(
            encoded[9..17]
                .try_into()
                .map_err(|_| ObjectKeyError::MalformedStorageKey)?,
        );
        Ok(Self {
            tenant_id: TenantId(tenant_id),
            bucket_id: BucketId(bucket_id),
        })
    }

    pub(crate) fn head_key(self, path: &str) -> Vec<u8> {
        let prefix = self.encode();
        let mut encoded = Vec::with_capacity(prefix.len() + path.len());
        encoded.extend_from_slice(&prefix);
        encoded.extend_from_slice(path.as_bytes());
        encoded
    }

    pub(crate) fn decode_head_path<'a>(self, encoded: &'a [u8]) -> Result<&'a str, ObjectKeyError> {
        let prefix = self.encode();
        let path = encoded
            .strip_prefix(&prefix)
            .ok_or(ObjectKeyError::MalformedStorageKey)?;
        std::str::from_utf8(path).map_err(|_| ObjectKeyError::MalformedStorageKey)
    }
}

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
    #[error("persisted object key is malformed")]
    MalformedStorageKey,
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
        if StorageTenantId::parse(self.tenant.as_str()).is_err()
            || self.bucket.contains('/')
            || self.bucket.chars().any(char::is_control)
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

pub(crate) fn contains_reserved_keldra_segment(path: &str) -> bool {
    path.split('/').any(|segment| segment == "_keldra")
}

/// Permanent name claim. Its value is the first stable tenant ID assigned to
/// `name`; tenant release must retain this key rather than make it assignable.
pub(crate) fn tenant_name_key(name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + name.len());
    key.extend_from_slice(&[STORAGE_KEY_FORMAT_VERSION, TENANT_NAME_TYPE]);
    key.extend_from_slice(name.as_bytes());
    key
}

pub(crate) fn bucket_name_key(tenant_id: TenantId, name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + size_of::<u64>() + name.len());
    key.extend_from_slice(&[STORAGE_KEY_FORMAT_VERSION, BUCKET_NAME_TYPE]);
    key.extend_from_slice(&tenant_id.0.to_be_bytes());
    key.extend_from_slice(name.as_bytes());
    key
}

pub(crate) fn encode_identity_value(id: u64) -> [u8; size_of::<u64>()] {
    id.to_be_bytes()
}

pub(crate) fn decode_identity_value(encoded: &[u8]) -> Result<u64, ObjectKeyError> {
    let encoded: [u8; size_of::<u64>()] = encoded
        .try_into()
        .map_err(|_| ObjectKeyError::MalformedStorageKey)?;
    Ok(u64::from_be_bytes(encoded))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_object_key_contains_only_format_ids_and_raw_path() {
        let identity = BucketIdentity {
            tenant_id: TenantId(0x0102_0304_0506_0708),
            bucket_id: BucketId(0x1112_1314_1516_1718),
        };
        assert_eq!(
            BucketIdentity::decode(&identity.encode()).unwrap(),
            identity
        );
        let encoded = identity.head_key("d/e");
        assert_eq!(
            encoded,
            [
                vec![STORAGE_KEY_FORMAT_VERSION],
                0x0102_0304_0506_0708_u64.to_be_bytes().to_vec(),
                0x1112_1314_1516_1718_u64.to_be_bytes().to_vec(),
                b"d/e".to_vec(),
            ]
            .concat()
        );
        assert_eq!(identity.decode_head_path(&encoded).unwrap(), "d/e");
    }

    #[test]
    fn bucket_identity_decode_rejects_other_formats_and_lengths() {
        let identity = BucketIdentity {
            tenant_id: TenantId(7),
            bucket_id: BucketId(11),
        };
        let mut other_format = identity.encode();
        other_format[0] = STORAGE_KEY_FORMAT_VERSION + 1;
        assert_eq!(
            BucketIdentity::decode(&other_format),
            Err(ObjectKeyError::MalformedStorageKey)
        );
        assert_eq!(
            BucketIdentity::decode(&identity.encode()[..16]),
            Err(ObjectKeyError::MalformedStorageKey)
        );
    }

    #[test]
    fn name_keys_are_versioned_typed_and_bucket_names_are_tenant_scoped() {
        assert_eq!(
            tenant_name_key("acme"),
            [vec![0x01, 0x01], b"acme".to_vec()].concat()
        );
        assert_eq!(
            bucket_name_key(TenantId(7), "objects"),
            [
                vec![0x01, 0x02],
                7_u64.to_be_bytes().to_vec(),
                b"objects".to_vec(),
            ]
            .concat()
        );
        assert_ne!(
            bucket_name_key(TenantId(7), "objects"),
            bucket_name_key(TenantId(8), "objects")
        );
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
    fn reserved_keldra_paths_match_exact_segments_only() {
        assert!(contains_reserved_keldra_segment("_keldra"));
        assert!(contains_reserved_keldra_segment("a/_keldra/meta.json"));
        assert!(!contains_reserved_keldra_segment("_keldraish"));
    }

    #[test]
    fn tenant_and_bucket_are_canonical_authorization_components() {
        assert_eq!(
            ObjectKey::new("other/tenant", "b", "a").unwrap_err(),
            ObjectKeyError::NonCanonicalComponent
        );
        for tenant in ["Acme", "-acme", "acme-", "acme_example", "acmé"] {
            assert_eq!(
                ObjectKey::new(tenant, "b", "a").unwrap_err(),
                ObjectKeyError::NonCanonicalComponent
            );
        }
        assert!(ObjectKey::new("acme-2", "b", "a").is_ok());
        assert!(ObjectKey::new(crate::SYSTEM_STORAGE_TENANT_ID, "b", "a").is_ok());
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
