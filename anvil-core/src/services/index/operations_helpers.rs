use super::*;

pub(super) fn authoritative_boundary_generation_hash(
    bucket_key: &str,
    payload: Option<&[u8]>,
) -> Result<String, Status> {
    let Some(bytes) = payload else {
        return Ok("none:0".to_string());
    };
    let schema = crate::core_store::CoreStore::decode_boundary_schema_from_mvcc(bytes)
        .map_err(|error| Status::internal(error.to_string()))?;
    if schema.bucket != bucket_key {
        return Err(Status::data_loss(
            "authoritative boundary schema scope does not match its MVCC key",
        ));
    }
    use sha2::Digest;
    Ok(format!(
        "generation:{}:sha256:{:x}",
        schema.generation,
        sha2::Sha256::digest(bytes)
    ))
}

impl AppState {
    pub(super) fn object_key_from_vector_source_id(
        bucket: &crate::persistence::Bucket,
        source_id_binary: &[u8],
    ) -> Result<String, Status> {
        let source = crate::core_store::SourceId::decode_binary(source_id_binary)
            .map_err(|e| Status::internal(format!("Invalid vector SourceId: {e}")))?;
        if source.kind != crate::core_store::SourceKind::ObjectCurrent {
            return Err(Status::internal("Vector SourceId is not an object source"));
        }
        let expected_prefix = format!("{}/{}/", bucket.tenant_id, bucket.name);
        source
            .resource_id
            .strip_prefix(&expected_prefix)
            .filter(|object_key| !object_key.is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| Status::internal("Vector SourceId does not match query bucket"))
    }
}

#[cfg(test)]
mod authoritative_read_tests {
    use super::*;

    #[test]
    fn boundary_generation_hash_is_derived_from_authoritative_mvcc_payload() {
        let schema = crate::core_store::CoreBoundarySchema {
            schema: crate::core_store::CORE_BOUNDARY_SCHEMA_SCHEMA.to_string(),
            bucket: "7/releases".to_string(),
            generation: 12,
            dimensions: Vec::new(),
            created_at: "2026-07-27T00:00:00Z".to_string(),
        };
        let payload =
            crate::core_store::CoreStore::encode_boundary_schema_for_mvcc(&schema).unwrap();
        let hash = authoritative_boundary_generation_hash(&schema.bucket, Some(&payload)).unwrap();
        assert!(hash.starts_with("generation:12:sha256:"));
        assert_eq!(
            authoritative_boundary_generation_hash(&schema.bucket, None).unwrap(),
            "none:0"
        );
        assert!(authoritative_boundary_generation_hash("8/releases", Some(&payload)).is_err());
    }
}
