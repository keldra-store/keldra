use super::*;

pub(super) fn put_fingerprint(
    encoded_head_key: &[u8],
    mode: PutMode,
    content_type: Option<&str>,
    durability: Durability,
    blob: &BlobRef,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"keldra.put.v1");
    hasher.update(encoded_head_key);
    hash_put_mode(&mut hasher, mode);
    hash_optional_string(&mut hasher, content_type);
    hash_durability(&mut hasher, durability);
    hasher.update(&blob.hash);
    hasher.update(&blob.length.to_be_bytes());
    *hasher.finalize().as_bytes()
}

pub(super) fn delete_fingerprint(request: &DeleteRequest, identity: BucketIdentity) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"keldra.delete.v1");
    hasher.update(&identity.head_key(request.key.path()));
    hash_precondition(&mut hasher, request.precondition);
    hash_durability(&mut hasher, request.durability);
    *hasher.finalize().as_bytes()
}

pub(super) fn publish_fingerprint(request: &PublishRequest, identity: BucketIdentity) -> [u8; 32] {
    // Publish is an internal staging detail for a streamed Put. Its canonical
    // idempotency identity must therefore be identical to an inline/bulk Put
    // with the same logical input.
    put_fingerprint(
        &identity.head_key(request.key.path()),
        request.mode,
        request.content_type.as_deref(),
        request.durability,
        &request.blob,
    )
}

pub(super) fn validate_clone_request(request: &CloneRequest) -> Result<(), MutationError> {
    validate_command_id(request.command_id.as_deref())?;
    if request.source_version.0 == 0
        || request.source.tenant() != request.destination.tenant()
        || request.source.bucket() != request.destination.bucket()
        || request.source == request.destination
        || matches!(request.mode, PutMode::PutImmutable)
    {
        return Err(MutationError::InvalidObjectMutation(
            "clone requires a non-zero exact source version, distinct same-bucket destination, and ordinary put operation"
                .into(),
        ));
    }
    Ok(())
}

pub(super) fn clone_fingerprint(request: &CloneRequest, identity: BucketIdentity) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"keldra.clone-object.v1");
    hasher.update(&identity.head_key(request.source.path()));
    hasher.update(&request.source_version.0.to_be_bytes());
    hasher.update(&identity.head_key(request.destination.path()));
    hash_put_mode(&mut hasher, request.mode);
    hash_optional_string(&mut hasher, request.content_type.as_deref());
    hash_durability(&mut hasher, request.durability);
    hasher.update(&request.blob.hash);
    hasher.update(&request.blob.length.to_be_bytes());
    *hasher.finalize().as_bytes()
}

fn hash_put_mode(hasher: &mut blake3::Hasher, mode: PutMode) {
    match mode {
        PutMode::Put => hasher.update(&[0]),
        PutMode::PutIfAbsent => hasher.update(&[1]),
        PutMode::PutIfVersion(version) => {
            hasher.update(&[2]);
            hasher.update(&version.0.to_be_bytes())
        }
        PutMode::PutImmutable => hasher.update(&[3]),
    };
}

fn hash_precondition(hasher: &mut blake3::Hasher, precondition: Precondition) {
    match precondition {
        Precondition::Any => hasher.update(&[0]),
        Precondition::Absent => hasher.update(&[1]),
        Precondition::Version(version) => {
            hasher.update(&[2]);
            hasher.update(&version.0.to_be_bytes())
        }
    };
}

fn hash_optional_string(hasher: &mut blake3::Hasher, value: Option<&str>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            hash_string(hasher, value);
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn hash_durability(hasher: &mut blake3::Hasher, durability: Durability) {
    hasher.update(&[match durability {
        Durability::Local => 0,
        Durability::Replicated => 1,
    }]);
}

pub(super) fn require_local_durability(durability: Durability) -> Result<(), MutationError> {
    match durability {
        Durability::Local => Ok(()),
        Durability::Replicated => Err(MutationError::DurabilityUnavailable),
    }
}

pub(super) fn hash_string(hasher: &mut blake3::Hasher, value: &str) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}
