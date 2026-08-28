use thiserror::Error;

use crate::key::contains_reserved_keldra_segment;
use crate::{Durability, ObjectKey, VersionId};

pub const OBJECT_LINK_CONTENT_TYPE: &str = "application/vnd.keldra.object-link.v1";
pub const MAX_INBOUND_OBJECT_LINKS: usize = 1_024;

const DESCRIPTOR_MAGIC: &[u8] = b"KELDRA_OBJECT_LINK\0\x01";

pub fn is_object_link_content_type(content_type: &str) -> bool {
    content_type == OBJECT_LINK_CONTENT_TYPE
}

pub fn object_link_command_fingerprint(
    link: &ObjectKey,
    target: Option<&ObjectKey>,
    durability: Durability,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key("keldra.object-link-command/v1");
    hash_key(&mut hasher, link);
    match target {
        Some(target) => {
            hasher.update(&[1]);
            hash_key(&mut hasher, target);
        }
        None => {
            hasher.update(&[0]);
        }
    }
    hasher.update(&[match durability {
        Durability::Local => 0,
        Durability::Replicated => 1,
    }]);
    *hasher.finalize().as_bytes()
}

fn hash_key(hasher: &mut blake3::Hasher, key: &ObjectKey) {
    for component in [key.tenant(), key.bucket(), key.path()] {
        hasher.update(&(component.len() as u64).to_be_bytes());
        hasher.update(component.as_bytes());
    }
}

/// Persisted value at a public link path. The target is always an ordinary,
/// canonical path in the same tenant and bucket as the descriptor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectLinkDescriptor {
    target_path: String,
}

impl ObjectLinkDescriptor {
    pub fn new(target_path: impl Into<String>) -> Result<Self, ObjectLinkError> {
        let target_path = target_path.into();
        validate_ordinary_path(&target_path, "target")?;
        Ok(Self { target_path })
    }

    pub fn target_path(&self) -> &str {
        &self.target_path
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(DESCRIPTOR_MAGIC.len() + 4 + self.target_path.len());
        encoded.extend_from_slice(DESCRIPTOR_MAGIC);
        push_string(&mut encoded, &self.target_path);
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, ObjectLinkError> {
        let mut input = encoded
            .strip_prefix(DESCRIPTOR_MAGIC)
            .ok_or(ObjectLinkError::MalformedDescriptor)?;
        let target_path = take_string(&mut input).ok_or(ObjectLinkError::MalformedDescriptor)?;
        if !input.is_empty() {
            return Err(ObjectLinkError::MalformedDescriptor);
        }
        Self::new(target_path)
    }
}

/// Snapshot needed to revalidate transparent resolution before a mutation.
/// The descriptor version, not merely its decoded target, prevents an unlink
/// and relink race from changing the meaning of an in-flight request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedObjectLink {
    pub link: ObjectKey,
    pub descriptor_version: VersionId,
    pub target: ObjectKey,
}

pub fn resolve_descriptor(
    link: ObjectKey,
    descriptor_version: VersionId,
    descriptor: &ObjectLinkDescriptor,
) -> Result<ResolvedObjectLink, ObjectLinkError> {
    validate_ordinary_path(link.path(), "link")?;
    let target = ObjectKey::new(link.tenant(), link.bucket(), descriptor.target_path())
        .map_err(|error| ObjectLinkError::InvalidPath(error.to_string()))?;
    if target == link {
        return Err(ObjectLinkError::SelfLink);
    }
    Ok(ResolvedObjectLink {
        link,
        descriptor_version,
        target,
    })
}

fn validate_ordinary_path(path: &str, role: &'static str) -> Result<(), ObjectLinkError> {
    ObjectKey::new("validation", "validation", path)
        .map_err(|error| ObjectLinkError::InvalidPath(error.to_string()))?;
    if contains_reserved_keldra_segment(path) {
        return Err(ObjectLinkError::ReservedPath(role));
    }
    Ok(())
}

fn push_string(output: &mut Vec<u8>, value: &str) {
    output.extend_from_slice(&(value.len() as u32).to_be_bytes());
    output.extend_from_slice(value.as_bytes());
}

fn take_u32(input: &mut &[u8]) -> Option<u32> {
    let (prefix, rest) = input.split_at_checked(4)?;
    *input = rest;
    Some(u32::from_be_bytes(
        prefix.try_into().expect("four-byte prefix"),
    ))
}

fn take_string(input: &mut &[u8]) -> Option<String> {
    let length = take_u32(input)? as usize;
    let (value, rest) = input.split_at_checked(length)?;
    *input = rest;
    Some(std::str::from_utf8(value).ok()?.to_owned())
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ObjectLinkError {
    #[error("invalid object-link path: {0}")]
    InvalidPath(String),
    #[error("{0} path is in Keldra's reserved namespace")]
    ReservedPath(&'static str),
    #[error("an object link cannot target itself")]
    SelfLink,
    #[error("object-link descriptor is malformed or uses an unsupported format")]
    MalformedDescriptor,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_has_one_canonical_encoding() {
        let descriptor = ObjectLinkDescriptor::new("target/a").unwrap();
        assert_eq!(
            ObjectLinkDescriptor::decode(&descriptor.encode()).unwrap(),
            descriptor
        );
        let mut trailing = descriptor.encode();
        trailing.push(0);
        assert_eq!(
            ObjectLinkDescriptor::decode(&trailing),
            Err(ObjectLinkError::MalformedDescriptor)
        );
    }

    #[test]
    fn links_cannot_target_links_by_encoding_reserved_or_self_paths() {
        assert!(matches!(
            ObjectLinkDescriptor::new("_keldra/object-links/x"),
            Err(ObjectLinkError::ReservedPath("target"))
        ));
        let link = ObjectKey::new("tenant", "bucket", "same").unwrap();
        let descriptor = ObjectLinkDescriptor::new("same").unwrap();
        assert_eq!(
            resolve_descriptor(link, VersionId(4), &descriptor),
            Err(ObjectLinkError::SelfLink)
        );
    }

    #[test]
    fn protected_descriptor_content_type_is_recognized_exactly() {
        assert!(is_object_link_content_type(OBJECT_LINK_CONTENT_TYPE));
        assert!(!is_object_link_content_type(
            "application/vnd.keldra.object-link.v1+json"
        ));
    }

    #[test]
    fn command_fingerprint_binds_operation_target_and_durability() {
        let link = ObjectKey::new("tenant", "bucket", "alias").unwrap();
        let target = ObjectKey::new("tenant", "bucket", "target").unwrap();
        let create = object_link_command_fingerprint(&link, Some(&target), Durability::Local);
        assert_eq!(
            create,
            object_link_command_fingerprint(&link, Some(&target), Durability::Local)
        );
        assert_ne!(
            create,
            object_link_command_fingerprint(&link, None, Durability::Local)
        );
        assert_ne!(
            create,
            object_link_command_fingerprint(&link, Some(&target), Durability::Replicated)
        );
    }
}
