use crate::{
    core_store::{
        AppendStreamRecord, AuthzScopeRef, CF_REGISTRY, CoreLogicalFileWrite, CoreMetaBatchOp,
        CoreMetaBatchOpKind, CoreMetaRootPublication, CoreMetaTuplePart, CoreMutationBatch,
        CoreMutationOperation, CoreMutationPrecondition, CoreObjectRef, CorePipelinePolicy,
        CoreStore, CoreTraceContext, GetBlob, ReadStream, StreamAppendReceipt, StreamRecord,
        TABLE_GATEWAY_METADATA_ROW, TABLE_GATEWAY_MOUNT_ROUTE_ROW, WriteLogicalFileRequest,
        core_meta_committed_row_common, core_meta_payload_digest, core_meta_root_key_hash,
        core_meta_tuple_key, core_object_ref_from_logical_file_write, decode_deterministic_proto,
        encode_deterministic_proto,
    },
    formats::{
        hash32,
        writer::{WriterFamily, canonical_logical_file_id},
    },
    storage::Storage,
};
use anyhow::{Result, anyhow, bail};
use argon2::Argon2;
use argon2::password_hash::{
    PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng,
};
use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use unicode_normalization::UnicodeNormalization;
use uuid::Uuid;

const GATEWAY_REPOSITORY_SCHEMA: &str = "anvil.gateway.repository.v1";
const GATEWAY_BLOB_SCHEMA: &str = "anvil.gateway.blob.v1";
const GATEWAY_TAG_SCHEMA: &str = "anvil.gateway.tag.v1";
const GATEWAY_UPLOAD_SESSION_SCHEMA: &str = "anvil.gateway.upload_session.v1";
const GATEWAY_CREDENTIAL_SCHEMA: &str = "anvil.gateway.credential.v1";
const GATEWAY_MOUNT_SCHEMA: &str = "anvil.gateway.mount.v1";
const GATEWAY_AUDIT_SCHEMA: &str = "anvil.gateway.audit.v1";
const GATEWAY_ACCESS_TOKEN_KIND: &str = "anvil.gateway.access_token.v1";
const GATEWAY_METADATA_ROW_SCHEMA: &str = "anvil.gateway.coremeta_record.v1";
const GATEWAY_ROW_REPOSITORY: &str = "repository";
const GATEWAY_ROW_BLOB: &str = "blob";
const GATEWAY_ROW_TAG: &str = "tag";
const GATEWAY_ROW_UPLOAD_SESSION: &str = "upload_session";
const GATEWAY_ROW_UPLOAD_IDEMPOTENCY: &str = "upload_idempotency";
const GATEWAY_ROW_CREDENTIAL: &str = "credential";
const GATEWAY_ROW_MOUNT: &str = "mount";
pub const GATEWAY_CREDENTIAL_CACHE_TTL_SECONDS: i64 = 60;
pub const GATEWAY_ACCESS_TOKEN_MAX_TTL_SECONDS: i64 = 900;
const REGIONAL_GATEWAY_SUFFIX: &str = ".anvil-storage.com";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayRepositoryRecord {
    pub schema: String,
    pub tenant_id: i64,
    pub gateway: String,
    pub registry_instance_id: String,
    pub repository: String,
    pub created_at: String,
    pub created_by_principal: String,
    pub record_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayBlobRecord {
    pub schema: String,
    pub tenant_id: i64,
    pub gateway: String,
    pub registry_instance_id: String,
    pub repository: String,
    pub digest: String,
    pub media_type: String,
    pub size_bytes: u64,
    pub object_ref: CoreObjectRef,
    pub created_at: String,
    pub created_by_principal: String,
    pub record_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayTagRecord {
    pub schema: String,
    pub tenant_id: i64,
    pub gateway: String,
    pub registry_instance_id: String,
    pub repository: String,
    pub tag: String,
    pub target_digest: String,
    pub updated_at: String,
    pub updated_by_principal: String,
    pub record_hash: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GatewayUploadSessionState {
    Open,
    Receiving,
    Finalising,
    Committed,
    Aborted,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayUploadPartRecord {
    pub schema: String,
    pub session_id: String,
    pub part_id: String,
    pub offset: u64,
    pub length: u64,
    pub payload_hash: String,
    pub idempotency_key_hash: String,
    pub core_object_ref: CoreObjectRef,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayUploadSessionRecord {
    pub schema: String,
    pub tenant_id: i64,
    pub gateway: String,
    pub registry_instance_id: String,
    pub repository: String,
    pub upload_id: String,
    pub idempotency_key_hash: String,
    pub state: GatewayUploadSessionState,
    pub expected_digest: Option<String>,
    pub received_bytes: u64,
    pub staged_parts: Vec<GatewayUploadPartRecord>,
    pub started_at: String,
    pub expires_at: String,
    pub completed_at: Option<String>,
    pub started_by_principal: String,
    pub committed_digest: Option<String>,
    pub record_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayCredentialRecord {
    pub schema: String,
    pub tenant_id: i64,
    pub credential_id: String,
    pub gateway: String,
    pub subject_principal: String,
    pub secret_hash: String,
    pub created_at: String,
    pub revoked_at: Option<String>,
    pub record_hash: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GatewayMountState {
    Active,
    Disabled,
    Draining,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayMountRecord {
    pub schema: String,
    pub mount_id: String,
    pub gateway: String,
    pub hosts: Vec<String>,
    pub path_prefixes: Vec<String>,
    pub mesh_id: String,
    pub region: String,
    pub anvil_storage_tenant_id: String,
    pub authz_scope: AuthzScopeRef,
    pub tenant_id: String,
    pub registry_instance_id: String,
    pub default_bucket: String,
    pub repository_prefix: String,
    pub state: GatewayMountState,
    pub generation: u64,
    pub record_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayMountMatchKind {
    ExactHostAlias,
    VirtualHostRegional,
    PathStyleRegional,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayMountResolution {
    pub record: GatewayMountRecord,
    pub row_generation: u64,
    pub matched_host: String,
    pub matched_path_prefix: String,
    pub match_kind: GatewayMountMatchKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayTagUpdateReceipt {
    pub record: GatewayTagRecord,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayUploadSessionReceipt {
    pub record: GatewayUploadSessionRecord,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayAuditRecord {
    pub schema: String,
    pub tenant_id: i64,
    pub gateway: String,
    pub registry_instance_id: String,
    pub operation: String,
    pub repository: String,
    pub package: Option<String>,
    pub version_or_reference: Option<String>,
    pub digest: Option<String>,
    pub subject_principal: String,
    pub credential_id: Option<String>,
    pub request_id: String,
    pub result: String,
    pub created_at: String,
    pub record_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayAuditAppendReceipt {
    pub record: GatewayAuditRecord,
    pub stream: StreamAppendReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayAuditStreamRecord {
    pub audit: GatewayAuditRecord,
    pub stream: StreamRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayAuditPage {
    pub records: Vec<GatewayAuditStreamRecord>,
    pub next_sequence: u64,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayAccessTokenClaims {
    pub token_kind: String,
    pub tenant_id: i64,
    pub gateway: String,
    pub registry_instance_id: String,
    pub repository: String,
    pub actions: Vec<String>,
    pub subject_principal: String,
    pub credential_id: String,
    pub credential_generation: u64,
    pub iat: i64,
    pub exp: usize,
    pub jti: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayAccessToken {
    pub access_token: String,
    pub token_type: String,
    pub expires_in_seconds: i64,
    pub claims: GatewayAccessTokenClaims,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayTokenRequirement {
    pub tenant_id: i64,
    pub gateway: String,
    pub registry_instance_id: String,
    pub repository: String,
    pub action: String,
}

pub fn normalize_gateway_identifier(input: &str, label: &str) -> Result<String> {
    let value = input.nfc().collect::<String>();
    if value.is_empty() {
        bail!("gateway {label} must not be empty");
    }
    if value.len() > 255 {
        bail!("gateway {label} exceeds 255 bytes");
    }
    if value != input {
        bail!("gateway {label} must already be Unicode NFC normalised");
    }
    if value.contains('%') {
        bail!("gateway {label} must not contain percent-encoded bytes");
    }
    if value.contains('\\') || value.contains('\0') || value.chars().any(char::is_control) {
        bail!("gateway {label} contains an unsafe character");
    }
    for segment in value.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            bail!("gateway {label} contains an unsafe path segment");
        }
        let lower = segment.to_ascii_lowercase();
        if matches!(
            lower.as_str(),
            "_anvil"
                | "corestore"
                | "admission"
                | "_system"
                | "_authz"
                | "_credentials"
                | "_gateway"
                | "_internal"
                | ".well-known"
        ) {
            bail!("gateway {label} uses a reserved name");
        }
    }
    Ok(value)
}

pub fn validate_gateway_digest(digest: &str) -> Result<()> {
    let Some(hex) = digest.strip_prefix("sha256:") else {
        bail!("gateway digest must use sha256:<hex>");
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("gateway digest must contain a 32 byte sha256 hex value");
    }
    Ok(())
}

pub async fn create_gateway_repository(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    created_by_principal: &str,
) -> Result<GatewayRepositoryRecord> {
    validate_tenant(tenant_id)?;
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let registry_instance_id = normalize_gateway_identifier(registry_instance_id, "registry")?;
    let repository = normalize_gateway_identifier(repository, "repository")?;
    let created_by_principal = normalize_gateway_identifier(created_by_principal, "principal")?;
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
    put_record_row(
        storage,
        GATEWAY_ROW_REPOSITORY,
        &gateway_repository_ref_name(&record)?,
        &record,
        true,
        None,
    )
    .await?;
    Ok(record)
}

pub async fn read_gateway_repository(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
) -> Result<Option<GatewayRepositoryRecord>> {
    let key = GatewayRepositoryKey::new(tenant_id, gateway, registry_instance_id, repository)?;
    let Some(row) = read_record_row::<GatewayRepositoryRecord>(
        storage,
        GATEWAY_ROW_REPOSITORY,
        &key.ref_name(),
    )
    .await?
    else {
        return Ok(None);
    };
    validate_repository_record(&row.record, &key)?;
    Ok(Some(row.record))
}

#[allow(clippy::too_many_arguments)]
pub async fn put_gateway_blob(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    digest: &str,
    media_type: &str,
    bytes: &[u8],
    created_by_principal: &str,
) -> Result<GatewayBlobRecord> {
    validate_tenant(tenant_id)?;
    validate_gateway_digest(digest)?;
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let registry_instance_id = normalize_gateway_identifier(registry_instance_id, "registry")?;
    let repository = normalize_gateway_identifier(repository, "repository")?;
    let created_by_principal = normalize_gateway_identifier(created_by_principal, "principal")?;
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
    coremeta::write_registry_blob_locator_row(storage, &record, &payload_write.locator).await?;
    put_record_row(storage, GATEWAY_ROW_BLOB, &ref_name, &record, true, None).await?;
    Ok(record)
}

pub async fn read_gateway_blob(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    digest: &str,
) -> Result<Option<(GatewayBlobRecord, Vec<u8>)>> {
    validate_gateway_digest(digest)?;
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let registry_instance_id = normalize_gateway_identifier(registry_instance_id, "registry")?;
    let repository = normalize_gateway_identifier(repository, "repository")?;
    let ref_name = gateway_blob_ref_name(
        tenant_id,
        &gateway,
        &registry_instance_id,
        &repository,
        digest,
    )?;
    let Some(row) =
        read_record_row::<GatewayBlobRecord>(storage, GATEWAY_ROW_BLOB, &ref_name).await?
    else {
        return Ok(None);
    };
    let record = row.record;
    validate_blob_record(
        &record,
        tenant_id,
        &gateway,
        &registry_instance_id,
        &repository,
        digest,
    )?;
    let bytes = CoreStore::new(storage.clone())
        .await?
        .get_blob(GetBlob {
            object_ref: record.object_ref.clone(),
        })
        .await?;
    let actual_digest = format!("sha256:{}", sha256_hex(&bytes));
    if actual_digest != record.digest || bytes.len() as u64 != record.size_bytes {
        bail!("gateway blob payload does not match its record");
    }
    Ok(Some((record, bytes)))
}

#[allow(clippy::too_many_arguments)]
pub async fn update_gateway_tag(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    tag: &str,
    target_digest: &str,
    updated_by_principal: &str,
    expected_generation: Option<u64>,
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
    let blob = coremeta::read_registry_blob_locator_row(
        storage,
        tenant_id,
        &record.gateway,
        &record.registry_instance_id,
        &record.target_digest,
    )
    .await?
    .ok_or_else(|| anyhow!("registry tag target blob is missing CoreMeta locator row"))?;
    let row = put_record_row(
        storage,
        GATEWAY_ROW_TAG,
        &ref_name,
        &record,
        false,
        expected_generation,
    )
    .await?;
    coremeta::write_registry_version_row_for_tag(storage, &record, &blob, row.generation).await?;
    Ok(GatewayTagUpdateReceipt {
        record,
        generation: row.generation,
    })
}

pub async fn read_gateway_tag(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    tag: &str,
) -> Result<Option<(GatewayTagRecord, GatewayStoredHandle)>> {
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let registry_instance_id = normalize_gateway_identifier(registry_instance_id, "registry")?;
    let repository = normalize_gateway_identifier(repository, "repository")?;
    let tag = normalize_gateway_identifier(tag, "tag")?;
    let ref_name = gateway_tag_ref_name_parts(
        tenant_id,
        &gateway,
        &registry_instance_id,
        &repository,
        &tag,
    )?;
    let Some(row) =
        read_record_row::<GatewayTagRecord>(storage, GATEWAY_ROW_TAG, &ref_name).await?
    else {
        return Ok(None);
    };
    let stored_handle = row.stored_handle();
    let record = row.record;
    validate_tag_record(
        &record,
        tenant_id,
        &gateway,
        &registry_instance_id,
        &repository,
        &tag,
    )?;
    Ok(Some((record, stored_handle)))
}

pub async fn create_gateway_upload_session(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    expected_digest: Option<&str>,
    started_by_principal: &str,
    idempotency_key: &str,
    ttl_seconds: i64,
) -> Result<GatewayUploadSessionRecord> {
    validate_tenant(tenant_id)?;
    if let Some(digest) = expected_digest {
        validate_gateway_digest(digest)?;
    }
    if ttl_seconds <= 0 {
        bail!("gateway upload session ttl must be positive");
    }
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let registry_instance_id = normalize_gateway_identifier(registry_instance_id, "registry")?;
    let repository = normalize_gateway_identifier(repository, "repository")?;
    let started_by_principal = normalize_gateway_identifier(started_by_principal, "principal")?;
    let idempotency_key_hash = idempotency_hash(idempotency_key)?;
    let idempotency_ref_name = gateway_upload_idempotency_ref_name(
        tenant_id,
        &gateway,
        &registry_instance_id,
        &repository,
        &idempotency_key_hash,
    )?;
    if let Some(existing) = read_record_row::<GatewayUploadSessionRecord>(
        storage,
        GATEWAY_ROW_UPLOAD_IDEMPOTENCY,
        &idempotency_ref_name,
    )
    .await?
    {
        validate_upload_session_record(
            &existing.record,
            tenant_id,
            &gateway,
            &registry_instance_id,
            &repository,
            &existing.record.upload_id,
        )?;
        if existing.record.expected_digest.as_deref() != expected_digest {
            bail!("gateway upload session idempotency target mismatch");
        }
        return Ok(existing.record);
    }

    let upload_id = Uuid::new_v4().simple().to_string();
    let now = Utc::now();
    let mut record = GatewayUploadSessionRecord {
        schema: GATEWAY_UPLOAD_SESSION_SCHEMA.to_string(),
        tenant_id,
        gateway,
        registry_instance_id,
        repository,
        upload_id,
        idempotency_key_hash,
        state: GatewayUploadSessionState::Open,
        expected_digest: expected_digest.map(str::to_string),
        received_bytes: 0,
        staged_parts: Vec::new(),
        started_at: now.to_rfc3339(),
        expires_at: (now + Duration::seconds(ttl_seconds)).to_rfc3339(),
        completed_at: None,
        started_by_principal,
        committed_digest: None,
        record_hash: String::new(),
    };
    record.record_hash = hash_record(&record)?;
    let session_handle_name = gateway_upload_ref_name(&record)?;
    if let Err(error) = put_upload_session_start_rows(
        storage,
        &session_handle_name,
        &idempotency_ref_name,
        &record,
    )
    .await
    {
        if let Some(existing) = read_record_row::<GatewayUploadSessionRecord>(
            storage,
            GATEWAY_ROW_UPLOAD_IDEMPOTENCY,
            &idempotency_ref_name,
        )
        .await?
        {
            if existing.record.expected_digest.as_deref() == expected_digest {
                return Ok(existing.record);
            }
        }
        return Err(error);
    }
    Ok(record)
}

#[allow(clippy::too_many_arguments)]
pub async fn read_gateway_upload_session(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    upload_id: &str,
) -> Result<Option<(GatewayUploadSessionRecord, GatewayStoredHandle)>> {
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let registry_instance_id = normalize_gateway_identifier(registry_instance_id, "registry")?;
    let repository = normalize_gateway_identifier(repository, "repository")?;
    let upload_id = normalize_gateway_identifier(upload_id, "upload id")?;
    let ref_name = gateway_upload_ref_name_parts(
        tenant_id,
        &gateway,
        &registry_instance_id,
        &repository,
        &upload_id,
    )?;
    let Some(row) = read_record_row::<GatewayUploadSessionRecord>(
        storage,
        GATEWAY_ROW_UPLOAD_SESSION,
        &ref_name,
    )
    .await?
    else {
        return Ok(None);
    };
    let stored_handle = row.stored_handle();
    let record = row.record;
    validate_upload_session_record(
        &record,
        tenant_id,
        &gateway,
        &registry_instance_id,
        &repository,
        &upload_id,
    )?;
    Ok(Some((record, stored_handle)))
}

#[allow(clippy::too_many_arguments)]
pub async fn abort_gateway_upload_session(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    upload_id: &str,
) -> Result<GatewayUploadSessionReceipt> {
    let Some((mut record, stored_handle)) = read_gateway_upload_session(
        storage,
        tenant_id,
        gateway,
        registry_instance_id,
        repository,
        upload_id,
    )
    .await?
    else {
        bail!("gateway upload session not found");
    };
    match record.state {
        GatewayUploadSessionState::Open | GatewayUploadSessionState::Receiving => {
            record.state = GatewayUploadSessionState::Aborted;
            record.record_hash.clear();
            record.record_hash = hash_record(&record)?;
            let row = put_record_row(
                storage,
                GATEWAY_ROW_UPLOAD_SESSION,
                &gateway_upload_ref_name(&record)?,
                &record,
                false,
                Some(stored_handle.generation),
            )
            .await?;
            Ok(GatewayUploadSessionReceipt {
                record,
                generation: row.generation,
            })
        }
        GatewayUploadSessionState::Aborted => Ok(GatewayUploadSessionReceipt {
            record,
            generation: stored_handle.generation,
        }),
        GatewayUploadSessionState::Committed
        | GatewayUploadSessionState::Expired
        | GatewayUploadSessionState::Finalising => {
            bail!("gateway upload session is not abortable")
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn expire_gateway_upload_session(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    upload_id: &str,
) -> Result<GatewayUploadSessionReceipt> {
    let Some((mut record, stored_handle)) = read_gateway_upload_session(
        storage,
        tenant_id,
        gateway,
        registry_instance_id,
        repository,
        upload_id,
    )
    .await?
    else {
        bail!("gateway upload session not found");
    };
    match record.state {
        GatewayUploadSessionState::Expired => Ok(GatewayUploadSessionReceipt {
            record,
            generation: stored_handle.generation,
        }),
        GatewayUploadSessionState::Open | GatewayUploadSessionState::Receiving
            if is_upload_session_expired(&record)? =>
        {
            record.state = GatewayUploadSessionState::Expired;
            record.record_hash.clear();
            record.record_hash = hash_record(&record)?;
            let row = put_record_row(
                storage,
                GATEWAY_ROW_UPLOAD_SESSION,
                &gateway_upload_ref_name(&record)?,
                &record,
                false,
                Some(stored_handle.generation),
            )
            .await?;
            Ok(GatewayUploadSessionReceipt {
                record,
                generation: row.generation,
            })
        }
        GatewayUploadSessionState::Open | GatewayUploadSessionState::Receiving => {
            bail!("gateway upload session has not expired")
        }
        GatewayUploadSessionState::Committed
        | GatewayUploadSessionState::Aborted
        | GatewayUploadSessionState::Finalising => {
            bail!("gateway upload session is not expirable")
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn append_gateway_upload_part(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    upload_id: &str,
    part_id: &str,
    offset: u64,
    bytes: &[u8],
    idempotency_key: &str,
) -> Result<GatewayUploadSessionReceipt> {
    let Some((mut record, stored_handle)) = read_gateway_upload_session(
        storage,
        tenant_id,
        gateway,
        registry_instance_id,
        repository,
        upload_id,
    )
    .await?
    else {
        bail!("gateway upload session not found");
    };
    if is_upload_session_expired(&record)? {
        record.state = GatewayUploadSessionState::Expired;
        record.record_hash.clear();
        record.record_hash = hash_record(&record)?;
        let row = put_record_row(
            storage,
            GATEWAY_ROW_UPLOAD_SESSION,
            &gateway_upload_ref_name(&record)?,
            &record,
            false,
            Some(stored_handle.generation),
        )
        .await?;
        return Ok(GatewayUploadSessionReceipt {
            record,
            generation: row.generation,
        });
    }
    if !matches!(
        record.state,
        GatewayUploadSessionState::Open | GatewayUploadSessionState::Receiving
    ) {
        bail!("gateway upload session is not appendable");
    }
    let part_id = normalize_gateway_identifier(part_id, "part id")?;
    if bytes.is_empty() {
        bail!("gateway upload part must not be empty");
    }
    let idempotency_key_hash = format!("sha256:{}", sha256_hex(idempotency_key.as_bytes()));
    let payload_hash = format!("sha256:{}", sha256_hex(bytes));
    if let Some(existing) = record
        .staged_parts
        .iter()
        .find(|part| part.idempotency_key_hash == idempotency_key_hash)
    {
        if existing.part_id == part_id
            && existing.offset == offset
            && existing.length == bytes.len() as u64
            && existing.payload_hash == payload_hash
        {
            return Ok(GatewayUploadSessionReceipt {
                record,
                generation: stored_handle.generation,
            });
        }
        bail!("gateway upload part idempotency conflict");
    }
    if record
        .staged_parts
        .iter()
        .any(|part| part.part_id == part_id)
    {
        bail!("gateway upload part id already exists");
    }
    if offset != record.received_bytes {
        bail!(
            "gateway upload part offset must equal current received_bytes {}",
            record.received_bytes
        );
    }

    let store = CoreStore::new(storage.clone()).await?;
    let object_ref = write_gateway_logical_file(
        &store,
        WriterFamily::Registry.as_str(),
        record.staged_parts.len() as u64 + 1,
        format!(
            "gateway_upload_part:tenant:{tenant_id}:gateway:{}:registry:{}:repository:{}:upload:{}:part:{part_id}",
            record.gateway, record.registry_instance_id, record.repository, record.upload_id
        ),
        bytes.to_vec(),
        format!(
            "gateway-upload-part:{tenant_id}:{}:{}:{}:{}:{part_id}:{idempotency_key_hash}",
            record.gateway, record.registry_instance_id, record.repository, record.upload_id
        ),
    )
    .await?;
    record.staged_parts.push(GatewayUploadPartRecord {
        schema: "anvil.gateway.upload_part.v1".to_string(),
        session_id: record.upload_id.clone(),
        part_id,
        offset,
        length: bytes.len() as u64,
        payload_hash,
        idempotency_key_hash,
        core_object_ref: object_ref,
    });
    record.received_bytes = record
        .received_bytes
        .checked_add(bytes.len() as u64)
        .ok_or_else(|| anyhow!("gateway upload received_bytes overflow"))?;
    record.state = GatewayUploadSessionState::Receiving;
    record.record_hash.clear();
    record.record_hash = hash_record(&record)?;
    let row = put_record_row(
        storage,
        GATEWAY_ROW_UPLOAD_SESSION,
        &gateway_upload_ref_name(&record)?,
        &record,
        false,
        Some(stored_handle.generation),
    )
    .await?;
    Ok(GatewayUploadSessionReceipt {
        record,
        generation: row.generation,
    })
}

#[allow(clippy::too_many_arguments)]
pub async fn finalise_gateway_upload_session(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    upload_id: &str,
    expected_digest: Option<&str>,
    media_type: &str,
    committed_by_principal: &str,
) -> Result<GatewayBlobRecord> {
    let Some((session, session_handle)) = read_gateway_upload_session(
        storage,
        tenant_id,
        gateway,
        registry_instance_id,
        repository,
        upload_id,
    )
    .await?
    else {
        bail!("gateway upload session not found");
    };
    if is_upload_session_expired(&session)? {
        bail!("gateway upload session has expired");
    }
    if session.state == GatewayUploadSessionState::Committed {
        let Some(digest) = session.committed_digest.as_deref() else {
            bail!("committed gateway upload session is missing committed digest");
        };
        let Some((record, _bytes)) = read_gateway_blob(
            storage,
            tenant_id,
            &session.gateway,
            &session.registry_instance_id,
            &session.repository,
            digest,
        )
        .await?
        else {
            bail!("committed gateway upload blob is missing");
        };
        return Ok(record);
    }
    if !matches!(session.state, GatewayUploadSessionState::Receiving) {
        bail!("gateway upload session is not finalisable");
    }
    let target_digest = expected_digest
        .map(str::to_string)
        .or_else(|| session.expected_digest.clone())
        .ok_or_else(|| anyhow!("gateway upload finalisation requires an expected digest"))?;
    validate_gateway_digest(&target_digest)?;
    validate_media_type(media_type)?;
    let committed_by_principal = normalize_gateway_identifier(committed_by_principal, "principal")?;

    let store = CoreStore::new(storage.clone()).await?;
    let mut ordered_parts = session.staged_parts.clone();
    ordered_parts.sort_by_key(|part| part.offset);
    let mut payload = Vec::with_capacity(session.received_bytes as usize);
    let mut next_offset = 0_u64;
    for part in &ordered_parts {
        if part.offset != next_offset {
            bail!(
                "gateway upload finalisation found a gap before part {}",
                part.part_id
            );
        }
        let bytes = store
            .get_blob(GetBlob {
                object_ref: part.core_object_ref.clone(),
            })
            .await?;
        if bytes.len() as u64 != part.length
            || format!("sha256:{}", sha256_hex(&bytes)) != part.payload_hash
        {
            bail!("gateway upload part payload does not match its receipt");
        }
        next_offset = next_offset
            .checked_add(part.length)
            .ok_or_else(|| anyhow!("gateway upload finalisation length overflow"))?;
        payload.extend_from_slice(&bytes);
    }
    if next_offset != session.received_bytes {
        bail!("gateway upload finalisation length mismatch");
    }
    let actual_digest = format!("sha256:{}", sha256_hex(&payload));
    if actual_digest != target_digest {
        bail!("gateway upload digest mismatch: expected {target_digest}, got {actual_digest}");
    }

    let gateway = session.gateway.clone();
    let registry_instance_id = session.registry_instance_id.clone();
    let repository = session.repository.clone();
    let blob_ref_name = gateway_blob_ref_name(
        tenant_id,
        &gateway,
        &registry_instance_id,
        &repository,
        &target_digest,
    )?;
    if let Some(existing) =
        read_record_row::<GatewayBlobRecord>(storage, GATEWAY_ROW_BLOB, &blob_ref_name).await?
    {
        validate_blob_record(
            &existing.record,
            tenant_id,
            &gateway,
            &registry_instance_id,
            &repository,
            &target_digest,
        )?;
        return commit_upload_session_record(
            storage,
            session,
            session_handle,
            &target_digest,
            None,
        )
        .await
        .map(|_| existing.record);
    }

    let payload_write = write_gateway_logical_file_with_locator(
        &store,
        WriterFamily::Registry.as_str(),
        1,
        blob_ref_name.clone(),
        payload,
        format!(
            "gateway-upload-finalise:{tenant_id}:{gateway}:{registry_instance_id}:{repository}:{target_digest}"
        ),
    )
    .await?;
    let payload_ref = core_object_ref_from_logical_file_write(&payload_write);
    let mut blob_record = GatewayBlobRecord {
        schema: GATEWAY_BLOB_SCHEMA.to_string(),
        tenant_id,
        gateway,
        registry_instance_id,
        repository,
        digest: target_digest.clone(),
        media_type: media_type.to_string(),
        size_bytes: session.received_bytes,
        object_ref: payload_ref,
        created_at: now_rfc3339(),
        created_by_principal: committed_by_principal,
        record_hash: String::new(),
    };
    blob_record.record_hash = hash_record(&blob_record)?;
    coremeta::write_registry_blob_locator_row(storage, &blob_record, &payload_write.locator)
        .await?;
    commit_upload_session_record(
        storage,
        session,
        session_handle,
        &target_digest,
        Some((blob_ref_name, blob_record.clone())),
    )
    .await?;
    Ok(blob_record)
}

pub async fn put_gateway_credential_record(
    storage: &Storage,
    mut record: GatewayCredentialRecord,
    expected_generation: Option<u64>,
) -> Result<u64> {
    record.record_hash.clear();
    validate_credential_record_shape(&record)?;
    record.record_hash = hash_record(&record)?;
    let ref_name = gateway_credential_ref_name(&record)?;
    let row = put_record_row(
        storage,
        GATEWAY_ROW_CREDENTIAL,
        &ref_name,
        &record,
        false,
        expected_generation,
    )
    .await?;
    Ok(row.generation)
}

pub async fn read_gateway_credential_record(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    credential_id: &str,
) -> Result<Option<(GatewayCredentialRecord, GatewayStoredHandle)>> {
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let credential_id = normalize_gateway_identifier(credential_id, "credential id")?;
    let ref_name = gateway_credential_ref_name_parts(tenant_id, &gateway, &credential_id)?;
    let Some(row) =
        read_record_row::<GatewayCredentialRecord>(storage, GATEWAY_ROW_CREDENTIAL, &ref_name)
            .await?
    else {
        return Ok(None);
    };
    let stored_handle = row.stored_handle();
    let record = row.record;
    if record.tenant_id != tenant_id
        || record.gateway != gateway
        || record.credential_id != credential_id
    {
        bail!("gateway credential record scope mismatch");
    }
    validate_credential_record_shape(&record)?;
    Ok(Some((record, stored_handle)))
}

pub async fn revoke_gateway_credential_record(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    credential_id: &str,
    expected_generation: u64,
) -> Result<u64> {
    let Some((mut record, _stored_handle)) =
        read_gateway_credential_record(storage, tenant_id, gateway, credential_id).await?
    else {
        bail!("gateway credential record not found");
    };
    if record.revoked_at.is_none() {
        record.revoked_at = Some(now_rfc3339());
    }
    put_gateway_credential_record(storage, record, Some(expected_generation)).await
}

pub fn hash_gateway_credential_secret(secret: &str) -> Result<String> {
    validate_gateway_secret(secret)?;
    let salt = SaltString::generate(&mut OsRng);
    let hash = Argon2::default()
        .hash_password(secret.as_bytes(), &salt)
        .map_err(|error| anyhow!("failed to hash gateway credential secret: {error}"))?;
    Ok(hash.to_string())
}

#[allow(clippy::too_many_arguments)]
pub async fn issue_gateway_access_token(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    repository: &str,
    credential_id: &str,
    presented_secret: &str,
    requested_actions: &[String],
    requested_ttl_seconds: i64,
    signing_secret: &str,
) -> Result<GatewayAccessToken> {
    validate_tenant(tenant_id)?;
    validate_gateway_secret(signing_secret)?;
    validate_gateway_secret(presented_secret)?;
    if requested_ttl_seconds <= 0 {
        bail!("gateway token ttl must be positive");
    }
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let registry_instance_id = normalize_gateway_identifier(registry_instance_id, "registry")?;
    let repository = normalize_gateway_identifier(repository, "repository")?;
    let credential_id = normalize_gateway_identifier(credential_id, "credential id")?;
    let actions = normalize_gateway_actions(requested_actions)?;
    if actions.is_empty() {
        bail!("gateway token requires at least one action");
    }
    let Some((credential, stored_handle)) =
        read_gateway_credential_record(storage, tenant_id, &gateway, &credential_id).await?
    else {
        bail!("gateway credential not found");
    };
    if credential.revoked_at.is_some() {
        bail!("gateway credential has been revoked");
    }
    verify_gateway_credential_secret(&credential.secret_hash, presented_secret)?;

    let now = Utc::now().timestamp();
    let ttl = requested_ttl_seconds.min(GATEWAY_ACCESS_TOKEN_MAX_TTL_SECONDS);
    let claims = GatewayAccessTokenClaims {
        token_kind: GATEWAY_ACCESS_TOKEN_KIND.to_string(),
        tenant_id,
        gateway,
        registry_instance_id,
        repository,
        actions,
        subject_principal: credential.subject_principal,
        credential_id,
        credential_generation: stored_handle.generation,
        iat: now,
        exp: (now + ttl) as usize,
        jti: Uuid::new_v4().to_string(),
    };
    let access_token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(signing_secret.as_bytes()),
    )?;
    Ok(GatewayAccessToken {
        access_token,
        token_type: "Bearer".to_string(),
        expires_in_seconds: ttl,
        claims,
    })
}

pub async fn validate_gateway_access_token(
    storage: &Storage,
    token: &str,
    signing_secret: &str,
    requirement: Option<&GatewayTokenRequirement>,
) -> Result<GatewayAccessTokenClaims> {
    validate_gateway_secret(signing_secret)?;
    let claims = decode::<GatewayAccessTokenClaims>(
        token,
        &DecodingKey::from_secret(signing_secret.as_bytes()),
        &Validation::default(),
    )?
    .claims;
    if claims.token_kind != GATEWAY_ACCESS_TOKEN_KIND {
        bail!("gateway token kind mismatch");
    }
    validate_gateway_token_claim_shape(&claims)?;
    if let Some(requirement) = requirement {
        validate_gateway_token_requirement(requirement)?;
        if claims.tenant_id != requirement.tenant_id
            || claims.gateway != requirement.gateway
            || claims.registry_instance_id != requirement.registry_instance_id
            || claims.repository != requirement.repository
            || !claims
                .actions
                .iter()
                .any(|action| action == &requirement.action)
        {
            bail!("gateway token does not satisfy route requirement");
        }
    }

    let Some((credential, stored_handle)) = read_gateway_credential_record(
        storage,
        claims.tenant_id,
        &claims.gateway,
        &claims.credential_id,
    )
    .await?
    else {
        bail!("gateway credential not found");
    };
    if stored_handle.generation != claims.credential_generation
        || credential.subject_principal != claims.subject_principal
    {
        bail!("gateway credential changed after token issue");
    }
    if credential.revoked_at.is_some() {
        bail!("gateway credential has been revoked");
    }
    Ok(claims)
}

pub async fn read_gateway_mount_record(
    storage: &Storage,
    mount_id: &str,
) -> Result<Option<(GatewayMountRecord, GatewayStoredHandle)>> {
    let mount_id = normalize_gateway_identifier(mount_id, "mount id")?;
    let ref_name = gateway_mount_ref_name_parts(&mount_id)?;
    let Some(row) =
        read_record_row::<GatewayMountRecord>(storage, GATEWAY_ROW_MOUNT, &ref_name).await?
    else {
        return Ok(None);
    };
    let stored_handle = row.stored_handle();
    let record = row.record;
    if record.mount_id != mount_id {
        bail!("gateway mount record scope mismatch");
    }
    validate_mount_record_shape(&record)?;
    Ok(Some((record, stored_handle)))
}

#[allow(clippy::too_many_arguments)]
pub async fn append_gateway_audit_record(
    storage: &Storage,
    mut record: GatewayAuditRecord,
    idempotency_key: Option<&str>,
) -> Result<GatewayAuditAppendReceipt> {
    record.record_hash.clear();
    normalise_gateway_audit_record(&mut record)?;
    let stream_id = gateway_audit_stream_id(
        record.tenant_id,
        &record.gateway,
        &record.registry_instance_id,
    )?;
    let partition_id = gateway_audit_partition_id(
        record.tenant_id,
        &record.gateway,
        &record.registry_instance_id,
    )?;
    let store = CoreStore::new(storage.clone()).await?;
    if let Some(idempotency_key) = idempotency_key {
        if let Some(stream) = store
            .read_stream_record_by_idempotency_key(&stream_id, idempotency_key)
            .await?
        {
            let existing: GatewayAuditRecord = decode_gateway_record(&stream.payload)?;
            validate_gateway_audit_record(&existing)?;
            if record.created_at.is_empty() {
                record.created_at = existing.created_at.clone();
            }
            record.record_hash = hash_record(&record)?;
            validate_gateway_audit_record(&record)?;
            if existing != record {
                bail!("gateway audit idempotency conflict");
            }
            return Ok(GatewayAuditAppendReceipt {
                record: existing,
                stream: StreamAppendReceipt {
                    stream_id: stream.stream_id,
                    sequence: stream.sequence,
                    cursor: stream.cursor,
                    event_hash: stream.event_hash,
                    idempotent_replay: true,
                },
            });
        }
    }
    if record.created_at.is_empty() {
        record.created_at = now_rfc3339();
    }
    record.record_hash = hash_record(&record)?;
    validate_gateway_audit_record(&record)?;

    let stream = store
        .append_stream(AppendStreamRecord {
            stream_id,
            partition_id,
            record_kind: GATEWAY_AUDIT_SCHEMA.to_string(),
            payload: encode_gateway_record(&record)?,
            content_type: None,
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: idempotency_key.map(str::to_string),
        })
        .await?;
    Ok(GatewayAuditAppendReceipt { record, stream })
}

pub async fn read_gateway_audit_page(
    storage: &Storage,
    tenant_id: i64,
    gateway: &str,
    registry_instance_id: &str,
    after_sequence: u64,
    limit: usize,
) -> Result<GatewayAuditPage> {
    validate_tenant(tenant_id)?;
    let gateway = normalize_gateway_identifier(gateway, "gateway")?;
    let registry_instance_id = normalize_gateway_identifier(registry_instance_id, "registry")?;
    let page = CoreStore::new(storage.clone())
        .await?
        .read_stream_page(ReadStream {
            stream_id: gateway_audit_stream_id(tenant_id, &gateway, &registry_instance_id)?,
            after_sequence,
            limit,
        })
        .await?;
    let mut audited = Vec::with_capacity(page.records.len());
    for stream in page.records {
        if stream.record_kind != GATEWAY_AUDIT_SCHEMA {
            bail!("gateway audit stream contains unexpected record kind");
        }
        let audit: GatewayAuditRecord = decode_gateway_record(&stream.payload)?;
        if audit.tenant_id != tenant_id
            || audit.gateway != gateway
            || audit.registry_instance_id != registry_instance_id
        {
            bail!("gateway audit record scope mismatch");
        }
        validate_gateway_audit_record(&audit)?;
        audited.push(GatewayAuditStreamRecord { audit, stream });
    }
    Ok(GatewayAuditPage {
        records: audited,
        next_sequence: page.next_sequence,
        has_more: page.has_more,
    })
}

async fn commit_upload_session_record(
    storage: &Storage,
    mut session: GatewayUploadSessionRecord,
    session_handle: GatewayStoredHandle,
    committed_digest: &str,
    blob_record: Option<(String, GatewayBlobRecord)>,
) -> Result<GatewayUploadSessionReceipt> {
    session.state = GatewayUploadSessionState::Committed;
    session.committed_digest = Some(committed_digest.to_string());
    session.completed_at = Some(now_rfc3339());
    session.record_hash.clear();
    session.record_hash = hash_record(&session)?;
    let session_handle_name = gateway_upload_ref_name(&session)?;
    if let Some((blob_key, blob_record)) = blob_record {
        put_record_row(
            storage,
            GATEWAY_ROW_BLOB,
            &blob_key,
            &blob_record,
            true,
            None,
        )
        .await?;
    }
    let row = put_record_row(
        storage,
        GATEWAY_ROW_UPLOAD_SESSION,
        &session_handle_name,
        &session,
        false,
        Some(session_handle.generation),
    )
    .await?;
    Ok(GatewayUploadSessionReceipt {
        record: session,
        generation: row.generation,
    })
}

async fn write_gateway_logical_file(
    store: &CoreStore,
    writer_family: &str,
    generation: u64,
    logical_file_id: String,
    source: Vec<u8>,
    mutation_id: String,
) -> Result<CoreObjectRef> {
    let write = write_gateway_logical_file_with_locator(
        store,
        writer_family,
        generation,
        logical_file_id,
        source,
        mutation_id,
    )
    .await?;
    Ok(core_object_ref_from_logical_file_write(&write))
}

async fn write_gateway_logical_file_with_locator(
    store: &CoreStore,
    writer_family: &str,
    generation: u64,
    logical_file_id: String,
    source: Vec<u8>,
    mutation_id: String,
) -> Result<CoreLogicalFileWrite> {
    let family = WriterFamily::from_name(writer_family)
        .ok_or_else(|| anyhow!("unsupported gateway writer family {writer_family}"))?;
    let logical_file_id =
        canonical_logical_file_id(family, generation, &logical_file_id, &hash32(&source));
    store
        .write_logical_file_with_locator(WriteLogicalFileRequest {
            writer_family: writer_family.to_string(),
            generation,
            logical_file_id,
            source,
            range_hints: Vec::new(),
            pipeline_policy: CorePipelinePolicy::default(),
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id,
            region_id: "local".to_string(),
        })
        .await
}

mod coremeta;
mod helpers;
mod keys;
mod metadata_rows;
mod mount_routes;
mod record_codec;
mod registry_api;
use helpers::*;
use keys::*;
use metadata_rows::*;
pub(crate) use metadata_rows::{GatewayStoredHandle, materialize_committed_gateway_transaction};
pub use mount_routes::{put_gateway_mount_record, resolve_gateway_mount};
use record_codec::*;
pub use registry_api::*;

#[cfg(test)]
mod tests;
