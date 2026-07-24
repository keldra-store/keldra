use super::*;

pub(super) const GATEWAY_MOUNT_MAX_HOSTS: usize = 64;
pub(super) const GATEWAY_MOUNT_MAX_PATH_PREFIXES: usize = 64;
pub(super) const GATEWAY_MOUNT_MAX_PATH_BYTES: usize = 4_096;

pub(super) fn gateway_upload_ref_name(record: &GatewayUploadSessionRecord) -> Result<String> {
    validate_tenant(record.tenant_id)?;
    gateway_upload_ref_name_parts(
        record.tenant_id,
        &record.gateway,
        &record.registry_instance_id,
        &record.repository,
        &record.upload_id,
    )
}

pub(super) fn gateway_upload_ref_name_parts(
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    upload_id: &str,
) -> Result<String> {
    validate_tenant(tenant_id)?;
    Ok(format!(
        "gateway_upload_session:tenant:{tenant_id}:gateway:{gateway}:registry:{registry_instance_id}:repository:{repository}:upload:{upload_id}"
    ))
}

pub(super) fn gateway_upload_idempotency_ref_name(
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    idempotency_key_hash: &str,
) -> Result<String> {
    validate_tenant(tenant_id)?;
    validate_gateway_digest(idempotency_key_hash)?;
    Ok(format!(
        "gateway_upload_idempotency:tenant:{tenant_id}:gateway:{gateway}:registry:{registry_instance_id}:repository:{repository}:idempotency:{idempotency_key_hash}"
    ))
}

pub(super) fn gateway_credential_ref_name(record: &GatewayCredentialRecord) -> Result<String> {
    validate_tenant(record.tenant_id)?;
    gateway_credential_ref_name_parts(record.tenant_id, &record.gateway, &record.credential_id)
}

pub(super) fn gateway_credential_ref_name_parts(
    tenant_id: i64,
    gateway: &str,
    credential_id: &str,
) -> Result<String> {
    validate_tenant(tenant_id)?;
    Ok(format!(
        "gateway_credential:tenant:{tenant_id}:gateway:{gateway}:credential:{credential_id}"
    ))
}

pub(super) fn gateway_mount_ref_name(record: &GatewayMountRecord) -> Result<String> {
    gateway_mount_ref_name_parts(&record.mount_id)
}

pub(super) fn gateway_mount_ref_name_parts(mount_id: &str) -> Result<String> {
    let mount_id = normalize_gateway_identifier(mount_id, "mount id")?;
    Ok(format!("gateway_mount:mount:{mount_id}"))
}

pub(super) fn gateway_partition_id(
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
) -> String {
    format!(
        "gateway:tenant:{tenant_id}:gateway:{gateway}:registry:{registry_instance_id}:repository:{repository}"
    )
}

pub(super) fn gateway_audit_partition_id(
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
) -> Result<String> {
    validate_tenant(tenant_id)?;
    Ok(format!(
        "gateway_audit:tenant:{tenant_id}:gateway:{gateway}:registry:{registry_instance_id}"
    ))
}

pub(super) fn gateway_audit_stream_id(
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
) -> Result<String> {
    validate_tenant(tenant_id)?;
    Ok(format!(
        "gateway_audit:tenant:{tenant_id}:gateway:{gateway}:registry:{registry_instance_id}"
    ))
}

pub(super) fn validate_repository_record(
    record: &GatewayRepositoryRecord,
    key: &GatewayRepositoryKey,
) -> Result<()> {
    if record.schema != GATEWAY_REPOSITORY_SCHEMA
        || record.tenant_id != key.tenant_id
        || record.gateway != key.gateway
        || record.registry_instance_id != key.registry_instance_id
        || record.repository != key.repository
    {
        bail!("gateway repository record scope mismatch");
    }
    validate_hash(record, &record.record_hash)
}

pub(super) fn validate_blob_record(
    record: &GatewayBlobRecord,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    digest: &str,
) -> Result<()> {
    if record.schema != GATEWAY_BLOB_SCHEMA
        || record.tenant_id != tenant_id
        || record.gateway != gateway
        || record.registry_instance_id != registry_instance_id
        || record.repository != repository
        || record.digest != digest
    {
        bail!("gateway blob record scope mismatch");
    }
    validate_gateway_digest(&record.digest)?;
    validate_media_type(&record.media_type)?;
    validate_hash(record, &record.record_hash)
}

pub(super) fn validate_tag_record(
    record: &GatewayTagRecord,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    tag: &str,
) -> Result<()> {
    if record.schema != GATEWAY_TAG_SCHEMA
        || record.tenant_id != tenant_id
        || record.gateway != gateway
        || record.registry_instance_id != registry_instance_id
        || record.repository != repository
        || record.tag != tag
    {
        bail!("gateway tag record scope mismatch");
    }
    validate_gateway_digest(&record.target_digest)?;
    validate_hash(record, &record.record_hash)
}

pub(super) fn validate_upload_session_record(
    record: &GatewayUploadSessionRecord,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    upload_id: &str,
) -> Result<()> {
    if record.schema != GATEWAY_UPLOAD_SESSION_SCHEMA
        || record.tenant_id != tenant_id
        || record.gateway != gateway
        || record.registry_instance_id != registry_instance_id
        || record.repository != repository
        || record.upload_id != upload_id
    {
        bail!("gateway upload session record scope mismatch");
    }
    if let Some(digest) = record.expected_digest.as_deref() {
        validate_gateway_digest(digest)?;
    }
    if let Some(digest) = record.committed_digest.as_deref() {
        validate_gateway_digest(digest)?;
    }
    validate_gateway_digest(&record.idempotency_key_hash)?;
    let mut next_offset = 0_u64;
    for part in &record.staged_parts {
        if part.schema != "anvil.gateway.upload_part.v1" || part.session_id != record.upload_id {
            bail!("gateway upload part scope mismatch");
        }
        normalize_gateway_identifier(&part.part_id, "part id")?;
        validate_gateway_digest(&part.payload_hash)?;
        validate_gateway_digest(&part.idempotency_key_hash)?;
        if part.offset != next_offset {
            bail!("gateway upload part offsets must be contiguous");
        }
        next_offset = next_offset
            .checked_add(part.length)
            .ok_or_else(|| anyhow!("gateway upload part offset overflow"))?;
    }
    if next_offset != record.received_bytes {
        bail!("gateway upload session received_bytes mismatch");
    }
    DateTime::parse_from_rfc3339(&record.started_at)
        .map_err(|error| anyhow!("gateway upload session started_at is invalid: {error}"))?;
    DateTime::parse_from_rfc3339(&record.expires_at)
        .map_err(|error| anyhow!("gateway upload session expires_at is invalid: {error}"))?;
    if let Some(completed_at) = record.completed_at.as_deref() {
        DateTime::parse_from_rfc3339(completed_at)
            .map_err(|error| anyhow!("gateway upload session completed_at is invalid: {error}"))?;
    }
    validate_hash(record, &record.record_hash)
}

pub(super) fn is_upload_session_expired(record: &GatewayUploadSessionRecord) -> Result<bool> {
    if matches!(
        record.state,
        GatewayUploadSessionState::Committed
            | GatewayUploadSessionState::Aborted
            | GatewayUploadSessionState::Expired
    ) {
        return Ok(false);
    }
    let expires_at = DateTime::parse_from_rfc3339(&record.expires_at)
        .map_err(|error| anyhow!("gateway upload session expires_at is invalid: {error}"))?
        .with_timezone(&Utc);
    Ok(expires_at <= Utc::now())
}

pub(super) fn validate_credential_record_shape(record: &GatewayCredentialRecord) -> Result<()> {
    if record.schema != GATEWAY_CREDENTIAL_SCHEMA {
        bail!("gateway credential record schema mismatch");
    }
    validate_tenant(record.tenant_id)?;
    normalize_gateway_identifier(&record.credential_id, "credential id")?;
    normalize_gateway_identifier(&record.gateway, "gateway")?;
    normalize_gateway_identifier(&record.subject_principal, "principal")?;
    if record.secret_hash.is_empty() || record.secret_hash.contains(char::is_control) {
        bail!("gateway credential secret_hash must be non-empty and safe");
    }
    if !record.record_hash.is_empty() {
        validate_hash(record, &record.record_hash)?;
    }
    Ok(())
}

pub(super) fn validate_mount_record_shape(record: &GatewayMountRecord) -> Result<()> {
    if record.schema != GATEWAY_MOUNT_SCHEMA {
        bail!("gateway mount record schema mismatch");
    }
    normalize_gateway_identifier(&record.mount_id, "mount id")?;
    normalize_gateway_identifier(&record.gateway, "gateway")?;
    validate_gateway_slug(&record.mesh_id, "mesh id")?;
    validate_gateway_slug(&record.region, "region")?;
    validate_gateway_slug(&record.anvil_storage_tenant_id, "anvil storage tenant id")?;
    validate_gateway_slug(
        &record.authz_scope.anvil_storage_tenant_id,
        "authz tenant id",
    )?;
    validate_gateway_slug(&record.authz_scope.authz_realm_id, "authz realm id")?;
    if record.authz_scope.anvil_storage_tenant_id != record.anvil_storage_tenant_id {
        bail!("gateway mount authz scope tenant mismatch");
    }
    validate_gateway_slug(&record.tenant_id, "tenant id")?;
    normalize_gateway_identifier(&record.registry_instance_id, "registry")?;
    normalize_gateway_identifier(&record.default_bucket, "default bucket")?;
    validate_gateway_repository_prefix(&record.repository_prefix)?;
    if record.generation == 0 {
        bail!("gateway mount generation must be positive");
    }
    if record.hosts.is_empty() && record.path_prefixes.is_empty() {
        bail!("gateway mount must define at least one host or path prefix");
    }
    if record.hosts.len() > GATEWAY_MOUNT_MAX_HOSTS {
        bail!("gateway mount has too many host aliases");
    }
    if record.path_prefixes.len() > GATEWAY_MOUNT_MAX_PATH_PREFIXES {
        bail!("gateway mount has too many path prefixes");
    }
    let mut unique_hosts = std::collections::BTreeSet::new();
    for host in &record.hosts {
        if normalize_gateway_host(host)? != *host {
            bail!("gateway mount host must be canonical");
        }
        if !unique_hosts.insert(host) {
            bail!("gateway mount host aliases must be unique");
        }
    }
    let mut unique_prefixes = std::collections::BTreeSet::new();
    for prefix in &record.path_prefixes {
        validate_gateway_path_prefix(prefix)?;
        if prefix != "/" && !prefix.ends_with('/') {
            bail!("gateway mount path prefixes must end with /");
        }
        if !unique_prefixes.insert(prefix) {
            bail!("gateway mount path prefixes must be unique");
        }
    }
    if !record.record_hash.is_empty() {
        validate_hash(record, &record.record_hash)?;
    }
    Ok(())
}

pub(super) fn normalise_gateway_audit_record(record: &mut GatewayAuditRecord) -> Result<()> {
    record.schema = GATEWAY_AUDIT_SCHEMA.to_string();
    validate_tenant(record.tenant_id)?;
    record.gateway = normalize_gateway_identifier(&record.gateway, "gateway")?;
    record.registry_instance_id =
        normalize_gateway_identifier(&record.registry_instance_id, "registry")?;
    record.operation = normalize_gateway_identifier(&record.operation, "operation")?;
    record.repository = normalize_gateway_identifier(&record.repository, "repository")?;
    record.package = record
        .package
        .as_deref()
        .map(|package| normalize_gateway_identifier(package, "package"))
        .transpose()?;
    record.version_or_reference = record
        .version_or_reference
        .as_deref()
        .map(|reference| normalize_gateway_identifier(reference, "version or reference"))
        .transpose()?;
    record.subject_principal =
        normalize_gateway_identifier(&record.subject_principal, "principal")?;
    record.credential_id = record
        .credential_id
        .as_deref()
        .map(|credential_id| normalize_gateway_identifier(credential_id, "credential id"))
        .transpose()?;
    record.request_id = normalize_gateway_identifier(&record.request_id, "request id")?;
    record.result = normalize_gateway_identifier(&record.result, "result")?;
    Ok(())
}

pub(super) fn validate_gateway_audit_record(record: &GatewayAuditRecord) -> Result<()> {
    if record.schema != GATEWAY_AUDIT_SCHEMA {
        bail!("gateway audit record schema mismatch");
    }
    validate_tenant(record.tenant_id)?;
    normalize_gateway_identifier(&record.gateway, "gateway")?;
    normalize_gateway_identifier(&record.registry_instance_id, "registry")?;
    normalize_gateway_identifier(&record.operation, "operation")?;
    normalize_gateway_identifier(&record.repository, "repository")?;
    if let Some(package) = record.package.as_deref() {
        normalize_gateway_identifier(package, "package")?;
    }
    if let Some(reference) = record.version_or_reference.as_deref() {
        normalize_gateway_identifier(reference, "version or reference")?;
    }
    if let Some(digest) = record.digest.as_deref() {
        validate_gateway_digest(digest)?;
    }
    normalize_gateway_identifier(&record.subject_principal, "principal")?;
    if let Some(credential_id) = record.credential_id.as_deref() {
        normalize_gateway_identifier(credential_id, "credential id")?;
    }
    normalize_gateway_identifier(&record.request_id, "request id")?;
    normalize_gateway_identifier(&record.result, "result")?;
    DateTime::parse_from_rfc3339(&record.created_at)
        .map_err(|error| anyhow!("gateway audit created_at is invalid: {error}"))?;
    validate_hash(record, &record.record_hash)
}

pub(super) fn normalize_gateway_actions(actions: &[String]) -> Result<Vec<String>> {
    let mut normalized = Vec::with_capacity(actions.len());
    for action in actions {
        let action = normalize_gateway_identifier(action, "action")?;
        if !normalized.iter().any(|existing| existing == &action) {
            normalized.push(action);
        }
    }
    normalized.sort();
    Ok(normalized)
}

pub(super) fn validate_gateway_token_claim_shape(claims: &GatewayAccessTokenClaims) -> Result<()> {
    validate_tenant(claims.tenant_id)?;
    normalize_gateway_identifier(&claims.gateway, "gateway")?;
    normalize_gateway_identifier(&claims.registry_instance_id, "registry")?;
    normalize_gateway_identifier(&claims.repository, "repository")?;
    normalize_gateway_actions(&claims.actions)?;
    if claims.actions.is_empty() {
        bail!("gateway token must contain at least one action");
    }
    normalize_gateway_identifier(&claims.subject_principal, "principal")?;
    normalize_gateway_identifier(&claims.credential_id, "credential id")?;
    if claims.credential_generation == 0 {
        bail!("gateway token credential generation must be nonzero");
    }
    if claims.iat <= 0 || claims.exp == 0 {
        bail!("gateway token time fields are invalid");
    }
    normalize_gateway_identifier(&claims.jti, "token id")?;
    Ok(())
}

pub(super) fn validate_gateway_token_requirement(
    requirement: &GatewayTokenRequirement,
) -> Result<()> {
    validate_tenant(requirement.tenant_id)?;
    normalize_gateway_identifier(&requirement.gateway, "gateway")?;
    normalize_gateway_identifier(&requirement.registry_instance_id, "registry")?;
    normalize_gateway_identifier(&requirement.repository, "repository")?;
    normalize_gateway_identifier(&requirement.action, "action")?;
    Ok(())
}

pub(super) fn validate_gateway_secret(secret: &str) -> Result<()> {
    if secret.is_empty() || secret.contains('\0') || secret.chars().any(char::is_control) {
        bail!("gateway secret is invalid");
    }
    Ok(())
}

pub(super) fn verify_gateway_credential_secret(
    secret_hash: &str,
    presented_secret: &str,
) -> Result<()> {
    validate_gateway_secret(presented_secret)?;
    if secret_hash.starts_with("$argon2") {
        let parsed = PasswordHash::new(secret_hash)
            .map_err(|_| anyhow!("gateway credential secret hash is invalid"))?;
        Argon2::default()
            .verify_password(presented_secret.as_bytes(), &parsed)
            .map_err(|_| anyhow!("gateway credential secret mismatch"))?;
        return Ok(());
    }
    if let Some(expected) = secret_hash.strip_prefix("sha256:") {
        let actual = sha256_hex(presented_secret.as_bytes());
        if expected.len() == actual.len()
            && constant_time_eq::constant_time_eq(expected.as_bytes(), actual.as_bytes())
        {
            return Ok(());
        }
    }
    bail!("gateway credential secret mismatch")
}

pub(super) fn validate_gateway_slug(value: &str, label: &str) -> Result<()> {
    if value.is_empty() || value.len() > 255 {
        bail!("gateway {label} length is invalid");
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        bail!("gateway {label} contains an unsafe character");
    }
    Ok(())
}

pub(super) fn validate_gateway_repository_prefix(value: &str) -> Result<()> {
    if value.is_empty() {
        return Ok(());
    }
    normalize_gateway_identifier(value, "repository prefix")?;
    Ok(())
}

pub(super) fn normalize_gateway_host(input: &str) -> Result<String> {
    let host = input.trim_end_matches('.').to_ascii_lowercase();
    if host.is_empty() || host.len() > 253 {
        bail!("gateway host length is invalid");
    }
    if host.contains("..") || host.starts_with('.') || host.ends_with('.') {
        bail!("gateway host has an empty label");
    }
    for label in host.split('.') {
        if label.is_empty() || label.len() > 63 {
            bail!("gateway host label length is invalid");
        }
        if label.starts_with('-') || label.ends_with('-') {
            bail!("gateway host label hyphen placement is invalid");
        }
        if !label
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            bail!("gateway host contains an unsafe character");
        }
    }
    Ok(host)
}

pub(super) fn normalize_gateway_path(input: &str) -> Result<String> {
    if !input.starts_with('/') {
        bail!("gateway path must start with /");
    }
    validate_gateway_path_prefix(input)?;
    Ok(input.to_string())
}

pub(super) fn validate_gateway_path_prefix(value: &str) -> Result<()> {
    if !value.starts_with('/') {
        bail!("gateway path prefix must start with /");
    }
    if value.len() > GATEWAY_MOUNT_MAX_PATH_BYTES {
        bail!("gateway path prefix is too long");
    }
    if value.contains('\\') || value.contains('%') || value.contains(char::is_control) {
        bail!("gateway path prefix contains an unsafe character");
    }
    for segment in value.split('/') {
        if matches!(segment, "." | "..") {
            bail!("gateway path prefix contains an unsafe segment");
        }
    }
    Ok(())
}

pub(super) fn validate_tenant(tenant_id: i64) -> Result<()> {
    if tenant_id <= 0 {
        bail!("gateway tenant id must be positive");
    }
    Ok(())
}

pub(super) fn validate_media_type(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 255
        || value.contains(char::is_control)
        || value.contains('/') && value.split('/').any(str::is_empty)
    {
        bail!("gateway media type is invalid");
    }
    Ok(())
}

pub(super) fn validate_hash<T: GatewayRecordCodec>(record: &T, actual: &str) -> Result<()> {
    let expected = hash_record(record)?;
    if expected != actual {
        bail!("gateway record hash mismatch");
    }
    Ok(())
}

pub(super) fn hash_record<T: GatewayRecordCodec>(record: &T) -> Result<String> {
    hash_gateway_record(record)
}

pub(super) fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

pub(super) fn idempotency_hash(value: &str) -> Result<String> {
    if value.is_empty() || value.contains(char::is_control) {
        bail!("gateway idempotency key is invalid");
    }
    Ok(format!("sha256:{}", sha256_hex(value.as_bytes())))
}

pub(super) fn encode_core_object_ref_target(object_ref: &CoreObjectRef) -> Result<String> {
    crate::core_store::encode_core_object_ref_target(object_ref)
}

pub(super) fn decode_core_object_ref_target(target: &str) -> Result<CoreObjectRef> {
    crate::core_store::decode_core_object_ref_target(target)
}
