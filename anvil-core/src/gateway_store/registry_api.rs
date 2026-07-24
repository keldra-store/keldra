use super::*;

pub const REGISTRY_BLOB_REPOSITORY: &str = "registry-blobs";
const REGISTRY_MANIFEST_MEDIA_TYPE: &str = "application/vnd.anvil.registry.manifest+json";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayPackageVersionRecord {
    pub registry_kind: String,
    pub namespace: String,
    pub package_name: String,
    pub version: String,
    pub manifest_ref: String,
    pub generation: u64,
}

#[allow(clippy::too_many_arguments)]
pub async fn put_registry_blob(
    storage: &Storage,
    tenant_id: i64,
    registry_kind: &str,
    namespace: &str,
    digest: &str,
    media_type: &str,
    bytes: &[u8],
    principal: &str,
    transaction_id: Option<&str>,
) -> Result<GatewayBlobRecord> {
    ensure_registry_repository(
        storage,
        tenant_id,
        registry_kind,
        namespace,
        REGISTRY_BLOB_REPOSITORY,
        principal,
        transaction_id,
    )
    .await?;
    if let Some(transaction_id) = transaction_id {
        return put_gateway_blob_in_transaction(
            storage,
            tenant_id,
            registry_kind,
            namespace,
            REGISTRY_BLOB_REPOSITORY,
            digest,
            media_type,
            bytes,
            principal,
            transaction_id,
        )
        .await;
    }
    put_gateway_blob(
        storage,
        tenant_id,
        registry_kind,
        namespace,
        REGISTRY_BLOB_REPOSITORY,
        digest,
        media_type,
        bytes,
        principal,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn put_package_version(
    storage: &Storage,
    tenant_id: i64,
    registry_kind: &str,
    namespace: &str,
    package_name: &str,
    version: &str,
    manifest_json: &str,
    blob_digests: &[String],
    principal: &str,
    expected_generation: Option<u64>,
    transaction_id: Option<&str>,
) -> Result<GatewayTagUpdateReceipt> {
    serde_json::from_str::<serde_json::Value>(manifest_json)
        .map_err(|err| anyhow!("registry manifest_json is not valid JSON: {err}"))?;
    ensure_registry_repository(
        storage,
        tenant_id,
        registry_kind,
        namespace,
        package_name,
        principal,
        transaction_id,
    )
    .await?;
    for digest in blob_digests {
        validate_gateway_digest(digest)?;
        if !registry_blob_exists_for_transaction(
            storage,
            tenant_id,
            registry_kind,
            namespace,
            REGISTRY_BLOB_REPOSITORY,
            digest,
            principal,
            transaction_id,
        )
        .await?
        {
            bail!("registry package version references missing blob {digest}");
        }
    }
    let manifest_digest = format!("sha256:{}", sha256_hex(manifest_json.as_bytes()));
    put_registry_blob(
        storage,
        tenant_id,
        registry_kind,
        namespace,
        &manifest_digest,
        REGISTRY_MANIFEST_MEDIA_TYPE,
        manifest_json.as_bytes(),
        principal,
        transaction_id,
    )
    .await?;
    if let Some(transaction_id) = transaction_id {
        return update_gateway_tag_in_transaction(
            storage,
            tenant_id,
            registry_kind,
            namespace,
            package_name,
            version,
            &manifest_digest,
            principal,
            expected_generation,
            transaction_id,
        )
        .await;
    }
    update_gateway_tag(
        storage,
        tenant_id,
        registry_kind,
        namespace,
        package_name,
        version,
        &manifest_digest,
        principal,
        expected_generation,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn put_registry_ref(
    storage: &Storage,
    tenant_id: i64,
    registry_kind: &str,
    namespace: &str,
    package_name: &str,
    ref_name: &str,
    target_version: &str,
    principal: &str,
    expected_generation: Option<u64>,
    transaction_id: Option<&str>,
) -> Result<GatewayTagUpdateReceipt> {
    let Some((target, _)) = read_gateway_tag_for_transaction(
        storage,
        tenant_id,
        registry_kind,
        namespace,
        package_name,
        target_version,
        principal,
        transaction_id,
    )
    .await?
    else {
        bail!("registry target version not found");
    };
    if let Some(transaction_id) = transaction_id {
        return update_gateway_tag_in_transaction(
            storage,
            tenant_id,
            registry_kind,
            namespace,
            package_name,
            ref_name,
            &target.target_digest,
            principal,
            expected_generation,
            transaction_id,
        )
        .await;
    }
    update_gateway_tag(
        storage,
        tenant_id,
        registry_kind,
        namespace,
        package_name,
        ref_name,
        &target.target_digest,
        principal,
        expected_generation,
    )
    .await
}

pub async fn get_package_version(
    storage: &Storage,
    tenant_id: i64,
    registry_kind: &str,
    namespace: &str,
    package_name: &str,
    version: &str,
) -> Result<Option<GatewayPackageVersionRecord>> {
    let Some((tag, stored_handle)) = read_gateway_tag(
        storage,
        tenant_id,
        registry_kind,
        namespace,
        package_name,
        version,
    )
    .await?
    else {
        return Ok(None);
    };
    Ok(Some(package_version_from_tag(
        tag,
        stored_handle.generation,
    )))
}

pub async fn list_package_versions(
    storage: &Storage,
    tenant_id: i64,
    registry_kind: &str,
    namespace: &str,
    package_name: &str,
    limit: usize,
    page_token: &str,
) -> Result<(Vec<GatewayPackageVersionRecord>, Option<String>)> {
    let registry_kind = normalize_gateway_identifier(registry_kind, "registry kind")?;
    let namespace = normalize_gateway_identifier(namespace, "namespace")?;
    let package_name = normalize_gateway_identifier(package_name, "package name")?;
    let mut after_tuple_key = if page_token.is_empty() {
        None
    } else {
        Some(hex::decode(page_token).map_err(|_| anyhow!("registry page_token is invalid"))?)
    };
    let effective_limit = limit.clamp(1, 1000);
    let mut versions = Vec::with_capacity(effective_limit);
    loop {
        let page = list_record_rows::<GatewayTagRecord>(
            storage,
            GATEWAY_ROW_TAG,
            after_tuple_key.as_deref(),
            1,
        )
        .await?;
        for row in page.records {
            if row.record.tenant_id == tenant_id
                && row.record.gateway == registry_kind
                && row.record.registry_instance_id == namespace
                && row.record.repository == package_name
            {
                versions.push(package_version_from_tag(row.record, row.generation));
            }
        }
        after_tuple_key = page.next_tuple_key;
        if versions.len() == effective_limit || after_tuple_key.is_none() {
            let next = after_tuple_key.as_deref().map(hex::encode);
            return Ok((versions, next));
        }
    }
}

async fn ensure_registry_repository(
    storage: &Storage,
    tenant_id: i64,
    registry_kind: &str,
    namespace: &str,
    repository: &str,
    principal: &str,
    transaction_id: Option<&str>,
) -> Result<()> {
    if read_gateway_repository(storage, tenant_id, registry_kind, namespace, repository)
        .await?
        .is_some()
    {
        return Ok(());
    }
    if let Some(transaction_id) = transaction_id {
        create_gateway_repository_in_transaction(
            storage,
            tenant_id,
            registry_kind,
            namespace,
            repository,
            principal,
            transaction_id,
        )
        .await?;
    } else {
        create_gateway_repository(
            storage,
            tenant_id,
            registry_kind,
            namespace,
            repository,
            principal,
        )
        .await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn create_gateway_repository_in_transaction(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    created_by_principal: &str,
    transaction_id: &str,
) -> Result<GatewayRepositoryRecord> {
    validate_tenant(tenant_id)?;
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let registry_instance_id = normalize_gateway_identifier(registry_instance_id, "registry")?;
    let repository = normalize_gateway_identifier(repository, "repository")?;
    let created_by_principal = normalize_gateway_identifier(created_by_principal, "principal")?;
    let key = GatewayRepositoryKey::new(tenant_id, &gateway, &registry_instance_id, &repository)?;
    let transaction_principal = format!("tenant/{tenant_id}/principal/{created_by_principal}");
    if let Some(existing) = read_record_row_in_transaction::<GatewayRepositoryRecord>(
        storage,
        GATEWAY_ROW_REPOSITORY,
        &key.ref_name(),
        transaction_id,
        &transaction_principal,
    )
    .await?
    {
        validate_repository_record(&existing.record, &key)?;
        return Ok(existing.record);
    }
    let mut record = GatewayRepositoryRecord {
        schema: GATEWAY_REPOSITORY_SCHEMA.to_string(),
        tenant_id,
        gateway,
        registry_instance_id,
        repository,
        created_at: now_rfc3339(),
        created_by_principal,
        record_hash: String::new(),
    };
    record.record_hash = hash_record(&record)?;
    put_record_row_in_transaction(
        storage,
        GATEWAY_ROW_REPOSITORY,
        &key.ref_name(),
        &record,
        true,
        None,
        transaction_id,
        &transaction_principal,
    )
    .await?;
    Ok(record)
}

#[allow(clippy::too_many_arguments)]
async fn put_gateway_blob_in_transaction(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    digest: &str,
    media_type: &str,
    bytes: &[u8],
    created_by_principal: &str,
    transaction_id: &str,
) -> Result<GatewayBlobRecord> {
    validate_tenant(tenant_id)?;
    validate_gateway_digest(digest)?;
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let registry_instance_id = normalize_gateway_identifier(registry_instance_id, "registry")?;
    let repository = normalize_gateway_identifier(repository, "repository")?;
    let created_by_principal = normalize_gateway_identifier(created_by_principal, "principal")?;
    let transaction_principal = format!("tenant/{tenant_id}/principal/{created_by_principal}");
    validate_media_type(media_type)?;
    let actual_digest = format!("sha256:{}", sha256_hex(bytes));
    if actual_digest != digest {
        bail!("gateway blob digest mismatch: expected {digest}, got {actual_digest}");
    }
    let ref_name = gateway_blob_ref_name(
        tenant_id,
        &gateway,
        &registry_instance_id,
        &repository,
        digest,
    )?;
    if let Some(existing) =
        read_record_row::<GatewayBlobRecord>(storage, GATEWAY_ROW_BLOB, &ref_name).await?
    {
        validate_blob_record(
            &existing.record,
            tenant_id,
            &gateway,
            &registry_instance_id,
            &repository,
            digest,
        )?;
        return Ok(existing.record);
    }
    if let Some(existing) = read_record_row_in_transaction::<GatewayBlobRecord>(
        storage,
        GATEWAY_ROW_BLOB,
        &ref_name,
        transaction_id,
        &transaction_principal,
    )
    .await?
    {
        validate_blob_record(
            &existing.record,
            tenant_id,
            &gateway,
            &registry_instance_id,
            &repository,
            digest,
        )?;
        return Ok(existing.record);
    }

    let store = CoreStore::new(storage.clone()).await?;
    let payload_write = write_gateway_logical_file_with_locator(
        &store,
        WriterFamily::Registry.as_str(),
        1,
        ref_name.clone(),
        bytes.to_vec(),
        format!("gateway-blob:{tenant_id}:{gateway}:{registry_instance_id}:{repository}:{digest}"),
    )
    .await?;
    let object_ref = core_object_ref_from_logical_file_write(&payload_write);
    let mut record = GatewayBlobRecord {
        schema: GATEWAY_BLOB_SCHEMA.to_string(),
        tenant_id,
        gateway,
        registry_instance_id,
        repository,
        digest: digest.to_string(),
        media_type: media_type.to_string(),
        size_bytes: bytes.len() as u64,
        object_ref,
        created_at: now_rfc3339(),
        created_by_principal,
        record_hash: String::new(),
    };
    record.record_hash = hash_record(&record)?;
    put_record_row_in_transaction(
        storage,
        GATEWAY_ROW_BLOB,
        &ref_name,
        &record,
        true,
        None,
        transaction_id,
        &transaction_principal,
    )
    .await?;
    Ok(record)
}

#[allow(clippy::too_many_arguments)]
async fn update_gateway_tag_in_transaction(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    tag: &str,
    target_digest: &str,
    updated_by_principal: &str,
    expected_generation: Option<u64>,
    transaction_id: &str,
) -> Result<GatewayTagUpdateReceipt> {
    validate_gateway_digest(target_digest)?;
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let registry_instance_id = normalize_gateway_identifier(registry_instance_id, "registry")?;
    let repository = normalize_gateway_identifier(repository, "repository")?;
    let tag = normalize_gateway_identifier(tag, "tag")?;
    let updated_by_principal = normalize_gateway_identifier(updated_by_principal, "principal")?;
    let mut record = GatewayTagRecord {
        schema: GATEWAY_TAG_SCHEMA.to_string(),
        tenant_id,
        gateway,
        registry_instance_id,
        repository,
        tag,
        target_digest: target_digest.to_string(),
        updated_at: now_rfc3339(),
        updated_by_principal,
        record_hash: String::new(),
    };
    record.record_hash = hash_record(&record)?;
    let ref_name = gateway_tag_ref_name(&record)?;
    if !registry_blob_exists_for_transaction(
        storage,
        tenant_id,
        &record.gateway,
        &record.registry_instance_id,
        REGISTRY_BLOB_REPOSITORY,
        &record.target_digest,
        &record.updated_by_principal,
        Some(transaction_id),
    )
    .await?
    {
        bail!("registry tag target blob is missing");
    }
    let row = put_record_row_in_transaction(
        storage,
        GATEWAY_ROW_TAG,
        &ref_name,
        &record,
        false,
        expected_generation,
        transaction_id,
        &format!(
            "tenant/{tenant_id}/principal/{}",
            record.updated_by_principal
        ),
    )
    .await?;
    Ok(GatewayTagUpdateReceipt {
        record,
        generation: row.generation,
    })
}

async fn registry_blob_locator_exists(
    storage: &Storage,
    tenant_id: i64,
    registry_kind: &str,
    namespace: &str,
    digest: &str,
) -> Result<bool> {
    Ok(coremeta::read_registry_blob_locator_row(
        storage,
        tenant_id,
        registry_kind,
        namespace,
        digest,
    )
    .await?
    .is_some())
}

#[allow(clippy::too_many_arguments)]
async fn registry_blob_exists_for_transaction(
    storage: &Storage,
    tenant_id: i64,
    registry_kind: &str,
    namespace: &str,
    repository: &str,
    digest: &str,
    principal: &str,
    transaction_id: Option<&str>,
) -> Result<bool> {
    if registry_blob_locator_exists(storage, tenant_id, registry_kind, namespace, digest).await? {
        return Ok(true);
    }
    let Some(transaction_id) = transaction_id else {
        return Ok(false);
    };
    let registry_kind = normalize_gateway_identifier(registry_kind, "registry kind")?;
    let namespace = normalize_gateway_identifier(namespace, "namespace")?;
    let repository = normalize_gateway_identifier(repository, "repository")?;
    let principal = normalize_gateway_identifier(principal, "principal")?;
    let ref_name =
        gateway_blob_ref_name(tenant_id, &registry_kind, &namespace, &repository, digest)?;
    let transaction_principal = format!("tenant/{tenant_id}/principal/{principal}");
    let Some(row) = read_record_row_in_transaction::<GatewayBlobRecord>(
        storage,
        GATEWAY_ROW_BLOB,
        &ref_name,
        transaction_id,
        &transaction_principal,
    )
    .await?
    else {
        return Ok(false);
    };
    validate_blob_record(
        &row.record,
        tenant_id,
        &registry_kind,
        &namespace,
        &repository,
        digest,
    )?;
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
async fn read_gateway_tag_for_transaction(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    tag: &str,
    principal: &str,
    transaction_id: Option<&str>,
) -> Result<Option<(GatewayTagRecord, GatewayStoredHandle)>> {
    if let Some(committed) = read_gateway_tag(
        storage,
        tenant_id,
        gateway,
        registry_instance_id,
        repository,
        tag,
    )
    .await?
    {
        return Ok(Some(committed));
    }
    let Some(transaction_id) = transaction_id else {
        return Ok(None);
    };
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let registry_instance_id = normalize_gateway_identifier(registry_instance_id, "registry")?;
    let repository = normalize_gateway_identifier(repository, "repository")?;
    let tag = normalize_gateway_identifier(tag, "tag")?;
    let principal = normalize_gateway_identifier(principal, "principal")?;
    let ref_name = gateway_tag_ref_name_parts(
        tenant_id,
        &gateway,
        &registry_instance_id,
        &repository,
        &tag,
    )?;
    let transaction_principal = format!("tenant/{tenant_id}/principal/{principal}");
    let Some(row) = read_record_row_in_transaction::<GatewayTagRecord>(
        storage,
        GATEWAY_ROW_TAG,
        &ref_name,
        transaction_id,
        &transaction_principal,
    )
    .await?
    else {
        return Ok(None);
    };
    validate_tag_record(
        &row.record,
        tenant_id,
        &gateway,
        &registry_instance_id,
        &repository,
        &tag,
    )?;
    let stored_handle = row.stored_handle();
    Ok(Some((row.record, stored_handle)))
}

fn package_version_from_tag(tag: GatewayTagRecord, generation: u64) -> GatewayPackageVersionRecord {
    GatewayPackageVersionRecord {
        registry_kind: tag.gateway,
        namespace: tag.registry_instance_id,
        package_name: tag.repository,
        version: tag.tag,
        manifest_ref: tag.target_digest,
        generation,
    }
}
