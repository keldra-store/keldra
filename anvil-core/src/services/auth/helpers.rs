use super::*;
use base64::Engine;

pub(super) fn authz_resource(namespace: &str, object_id: &str, relation: &str) -> String {
    format!("{}/{}#{}", namespace, object_id, relation)
}

pub(super) fn authz_filter_resource(namespace: &str, object_id: &str, relation: &str) -> String {
    match (
        namespace.is_empty(),
        object_id.is_empty(),
        relation.is_empty(),
    ) {
        (true, _, _) => "*".to_string(),
        (false, true, true) => namespace.to_string(),
        (false, true, false) => format!("{namespace}/*#{relation}"),
        (false, false, true) => format!("{namespace}/{object_id}#*"),
        (false, false, false) => authz_resource(namespace, object_id, relation),
    }
}

pub(super) fn validate_storage_tenant(
    claims: &auth::Claims,
    anvil_storage_tenant_id: &str,
) -> Result<(), Status> {
    if anvil_storage_tenant_id.is_empty() || anvil_storage_tenant_id == claims.tenant_id.to_string()
    {
        Ok(())
    } else {
        Err(Status::permission_denied(
            "authz scope storage tenant does not match authenticated tenant",
        ))
    }
}

pub(super) fn resolve_authz_scope(
    claims: &auth::Claims,
    scope: Option<&AuthzScope>,
) -> Result<AuthzScope, Status> {
    let mut resolved = scope.cloned().unwrap_or_else(|| AuthzScope {
        anvil_storage_tenant_id: claims.tenant_id.to_string(),
        authz_realm_id: DEFAULT_AUTHZ_REALM_ID.to_string(),
    });
    if resolved.anvil_storage_tenant_id.is_empty() {
        resolved.anvil_storage_tenant_id = claims.tenant_id.to_string();
    }
    validate_storage_tenant(claims, &resolved.anvil_storage_tenant_id)?;
    validate_authz_realm_id(&resolved.authz_realm_id)?;
    Ok(resolved)
}

pub(super) fn resolve_batch_scope(
    claims: &auth::Claims,
    request_scope: Option<&AuthzScope>,
    mutations: &[AuthzTupleMutation],
) -> Result<AuthzScope, Status> {
    let scope = resolve_authz_scope(
        claims,
        request_scope.or_else(|| {
            mutations
                .first()
                .and_then(|mutation| mutation.scope.as_ref())
        }),
    )?;
    for mutation in mutations {
        if let Some(mutation_scope) = mutation.scope.as_ref() {
            let mutation_scope = resolve_authz_scope(claims, Some(mutation_scope))?;
            if mutation_scope != scope {
                return Err(Status::invalid_argument(
                    "authz tuple batch must target one authz scope",
                ));
            }
        }
    }
    Ok(scope)
}

pub(super) fn record_belongs_to_realm(
    record: &crate::persistence::AuthzTupleRecord,
    realm_id: &str,
) -> bool {
    decode_realm_namespace(realm_id, &record.namespace).is_some()
}

pub(super) fn filter_records_for_realm(
    records: Vec<crate::persistence::AuthzTupleRecord>,
    realm_id: &str,
) -> Vec<crate::persistence::AuthzTupleRecord> {
    records
        .into_iter()
        .filter(|record| record_belongs_to_realm(record, realm_id))
        .collect()
}

pub(super) fn validate_tuple_field(name: &str, value: &str) -> Result<(), Status> {
    if value.is_empty() {
        return Err(Status::invalid_argument(format!(
            "{name} must not be empty"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(Status::invalid_argument(format!(
            "{name} must not contain control characters"
        )));
    }
    Ok(())
}

pub(super) fn validate_optional_tuple_field(name: &str, value: &str) -> Result<(), Status> {
    if value.is_empty() {
        return Ok(());
    }
    validate_tuple_field(name, value)
}

pub(super) fn validate_tuple_component(name: &str, value: &str) -> Result<(), Status> {
    validate_tuple_field(name, value)?;
    if value == "." || value == ".." || value.contains('/') {
        return Err(Status::invalid_argument(format!(
            "{name} must be a safe authz component"
        )));
    }
    Ok(())
}

pub(super) fn validate_authz_realm_id(value: &str) -> Result<(), Status> {
    if is_reserved_authz_realm_id(value) {
        return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
    }
    validate_tuple_component("authz_realm_id", value)
}

pub(super) fn is_reserved_authz_realm_id(value: &str) -> bool {
    matches!(value, "_anvil" | "_system" | "system")
        || value.starts_with("_anvil/")
        || value.starts_with("_system/")
}

pub(super) fn validate_public_authz_namespace(value: &str) -> Result<(), Status> {
    validate_tuple_component("namespace", value)?;
    if is_reserved_authz_namespace(value) {
        return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
    }
    Ok(())
}

pub(super) fn validate_optional_public_authz_namespace(value: &str) -> Result<(), Status> {
    if value.is_empty() {
        return Ok(());
    }
    validate_public_authz_namespace(value)
}

pub(super) fn is_reserved_authz_namespace(value: &str) -> bool {
    matches!(value, "_anvil" | "_system" | "system" | "anvil_mesh")
        || value.starts_with("_anvil/")
        || value.starts_with("_system/")
}

pub(super) fn validate_optional_tuple_component(name: &str, value: &str) -> Result<(), Status> {
    if value.is_empty() {
        return Ok(());
    }
    validate_tuple_component(name, value)
}

pub(super) fn validate_caveat_hash(value: &str) -> Result<(), Status> {
    authz_journal::validate_optional_caveat_hash(value)
        .map_err(|err| Status::invalid_argument(err.to_string()))
}

pub(super) fn validate_watch_component(name: &str, value: &str) -> Result<(), Status> {
    validate_tuple_field(name, value)?;
    if value == "." || value == ".." || value.contains('/') {
        return Err(Status::invalid_argument(format!(
            "{name} must be a safe path component"
        )));
    }
    Ok(())
}

pub(super) async fn write_authz_tuple_record(
    state: &AppState,
    claims: &auth::Claims,
    req: AuthzTupleMutation,
) -> Result<crate::persistence::AuthzTupleRecord, Status> {
    let operation = validate_authz_tuple_mutation(state, claims, &req).await?;
    let scope = resolve_authz_scope(claims, req.scope.as_ref())?;
    let record = state
        .persistence
        .write_authz_tuple(
            claims.tenant_id,
            &encode_realm_namespace(&scope.authz_realm_id, &req.namespace),
            &req.object_id,
            &req.relation,
            &req.subject_kind,
            &encode_userset_subject_realm(
                &scope.authz_realm_id,
                &req.subject_kind,
                &req.subject_id,
            ),
            &req.caveat_hash,
            operation,
            &claims.sub,
            &req.reason,
        )
        .await
        .map_err(authz_tuple_write_status)?;
    Ok(record)
}

pub(super) async fn validate_authz_tuple_mutation<'a>(
    state: &AppState,
    claims: &auth::Claims,
    req: &'a AuthzTupleMutation,
) -> Result<&'a str, Status> {
    let operation = validate_authz_tuple_mutation_shape(req)?;
    let scope = resolve_authz_scope(claims, req.scope.as_ref())?;
    crate::access_control::require_action(
        &state.storage,
        &state.persistence,
        claims,
        AnvilAction::AuthzTupleWrite,
        &scope.authz_realm_id,
    )
    .await?;
    Ok(operation)
}

pub(super) fn validate_authz_tuple_mutation_shape(
    req: &AuthzTupleMutation,
) -> Result<&str, Status> {
    validate_public_authz_namespace(&req.namespace)?;
    validate_tuple_field("object_id", &req.object_id)?;
    validate_tuple_component("relation", &req.relation)?;
    validate_tuple_component("subject_kind", &req.subject_kind)?;
    validate_tuple_field("subject_id", &req.subject_id)?;
    validate_caveat_hash(&req.caveat_hash)?;
    let operation = match req.operation.as_str() {
        "add" | "remove" => req.operation.as_str(),
        _ => return Err(Status::invalid_argument("operation must be add or remove")),
    };
    if req.subject_kind == "userset" {
        let subject = parse_userset_subject(&req.subject_id).ok_or_else(|| {
            Status::invalid_argument(
                "userset subject_id must use namespace/object_id#relation format",
            )
        })?;
        validate_public_authz_namespace(subject.namespace)?;
        validate_tuple_field("userset object_id", subject.object_id)?;
        validate_tuple_component("userset relation", subject.relation)?;
    }
    Ok(operation)
}

pub(super) fn validate_authz_batch_operation_id(operation_id: Option<&str>) -> Result<(), Status> {
    let Some(operation_id) = operation_id else {
        return Ok(());
    };
    authz_journal::validate_authz_batch_operation_id(operation_id)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

pub(super) fn optional_expected_authz_revision(
    expected_revision: Option<u64>,
) -> Result<Option<i64>, Status> {
    expected_revision
        .map(|revision| {
            i64::try_from(revision)
                .map_err(|_| Status::invalid_argument("expected_revision exceeds supported range"))
        })
        .transpose()
}

pub(super) fn authz_tuple_batch_write_status(error: anyhow::Error) -> Status {
    if let Some(contract) = error.chain().find_map(|cause| {
        cause.downcast_ref::<crate::authz_schema_contract::AuthzSchemaContractError>()
    }) {
        return Status::invalid_argument(contract.to_string());
    }
    let conflict = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<crate::persistence::AuthzTupleBatchWriteError>());
    match conflict {
        Some(crate::persistence::AuthzTupleBatchWriteError::OperationConflict) => {
            Status::aborted("AuthzOperationConflict")
        }
        Some(crate::persistence::AuthzTupleBatchWriteError::RevisionConflict {
            expected,
            actual,
        }) => Status::aborted(format!(
            "AuthzRevisionConflict: expected {expected}, current {actual}"
        )),
        Some(crate::persistence::AuthzTupleBatchWriteError::SchemaBindingChanged) => {
            Status::aborted("AuthzSchemaBindingChanged")
        }
        None => Status::internal(format!("{error:#}")),
    }
}

pub(super) fn authz_tuple_write_status(error: anyhow::Error) -> Status {
    authz_tuple_batch_write_status(error)
}

pub(super) async fn check_permission_response(
    state: &AppState,
    claims: &auth::Claims,
    req: CheckPermissionRequest,
) -> Result<CheckPermissionResponse, Status> {
    validate_public_authz_namespace(&req.namespace)?;
    validate_tuple_field("object_id", &req.object_id)?;
    validate_tuple_component("relation", &req.relation)?;
    validate_tuple_component("subject_kind", &req.subject_kind)?;
    validate_tuple_field("subject_id", &req.subject_id)?;
    validate_caveat_hash(&req.caveat_hash)?;
    let scope = resolve_authz_scope(claims, req.scope.as_ref())?;
    crate::access_control::require_action(
        &state.storage,
        &state.persistence,
        claims,
        AnvilAction::AuthzCheck,
        &scope.authz_realm_id,
    )
    .await?;
    let consistency = AuthzConsistency::from_request(&req.consistency, &req.zookie)?;
    let response_revision =
        resolve_authz_response_revision(&state.storage, claims.tenant_id, consistency).await?;
    let allowed = authz_journal::resolve_permission_at_revision(
        &state.storage,
        claims.tenant_id,
        &encode_realm_namespace(&scope.authz_realm_id, &req.namespace),
        &req.object_id,
        &req.relation,
        &req.subject_kind,
        &req.subject_id,
        &req.caveat_hash,
        response_revision,
    )
    .await
    .map_err(crate::services::authz_status::consistency_status)?;

    Ok(CheckPermissionResponse {
        allowed,
        revision: revision_to_u64(response_revision)?,
        zookie: zookie(response_revision),
        explanation_ref: if allowed {
            "tuple_or_userset_match".to_string()
        } else {
            "no_current_tuple_or_userset".to_string()
        },
    })
}

pub(super) async fn resolve_authz_response_revision(
    storage: &crate::storage::Storage,
    tenant_id: i64,
    consistency: AuthzConsistency,
) -> Result<i64, Status> {
    let latest_revision = authz_journal::latest_authz_revision(storage, tenant_id)
        .await
        .map_err(|e| Status::internal(format!("{e:#}")))?;
    if let Some(required_revision) = consistency.required_revision()
        && latest_revision < required_revision
    {
        return Err(Status::failed_precondition("AuthzRevisionUnavailable"));
    }

    Ok(match consistency {
        AuthzConsistency::Exact(revision) => revision,
        AuthzConsistency::Latest | AuthzConsistency::AtLeast(_) => latest_revision,
    })
}

pub(super) async fn require_current_authz_list_revision(
    storage: &crate::storage::Storage,
    tenant_id: i64,
    requested_revision: i64,
) -> Result<(), Status> {
    let current_revision = authz_journal::latest_authz_revision(storage, tenant_id)
        .await
        .map_err(|error| Status::internal(format!("{error:#}")))?;
    if current_revision != requested_revision {
        return Err(Status::failed_precondition(format!(
            "AuthzRevisionUnavailable: historical authz list reads are not supported; current revision is {current_revision}, requested {requested_revision}"
        )));
    }
    Ok(())
}

pub(super) fn authz_projection_page_status(
    error: authz_journal::AuthzProjectionPageError,
) -> Status {
    let message = error.to_string();
    match error {
        authz_journal::AuthzProjectionPageError::RevisionMismatch { .. } => {
            Status::failed_precondition(message)
        }
        authz_journal::AuthzProjectionPageError::InvalidPageSize => {
            Status::invalid_argument(message)
        }
        authz_journal::AuthzProjectionPageError::Internal(_) => Status::internal(message),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AuthzConsistency {
    Latest,
    AtLeast(i64),
    Exact(i64),
}

impl AuthzConsistency {
    pub(super) fn from_request(consistency: &str, zookie: &str) -> Result<Self, Status> {
        match consistency {
            "" | "latest" => Ok(Self::Latest),
            "at_least" => Ok(Self::AtLeast(parse_authz_zookie(zookie)?)),
            "exact" => Ok(Self::Exact(parse_authz_zookie(zookie)?)),
            _ => Err(Status::invalid_argument(
                "consistency must be latest, at_least, exact, or empty",
            )),
        }
    }

    pub(super) fn required_revision(self) -> Option<i64> {
        match self {
            Self::Latest => None,
            Self::AtLeast(revision) | Self::Exact(revision) => Some(revision),
        }
    }
}

pub(super) fn parse_authz_zookie(value: &str) -> Result<i64, Status> {
    let Some(revision) = value.strip_prefix("authz:") else {
        return Err(Status::invalid_argument(
            "zookie must use authz:<revision> format",
        ));
    };
    let revision = revision
        .parse::<i64>()
        .map_err(|_| Status::invalid_argument("zookie revision must be an integer"))?;
    if revision < 0 {
        return Err(Status::invalid_argument(
            "zookie revision must not be negative",
        ));
    }
    Ok(revision)
}

pub(super) fn revision_to_u64(revision: i64) -> Result<u64, Status> {
    u64::try_from(revision).map_err(|_| Status::internal("Invalid authz revision"))
}

pub(super) fn u64_to_i64(revision: u64) -> Result<i64, Status> {
    i64::try_from(revision).map_err(|_| Status::internal("Invalid authz revision"))
}

pub(super) fn zookie(revision: i64) -> String {
    format!("authz:{}", revision.max(0))
}

pub(super) fn schema_ref_response(record: &authz_realm_schema::StoredSchemaRef) -> AuthzSchemaRef {
    AuthzSchemaRef {
        schema_id: record.schema_id.clone(),
        schema_revision: record.schema_revision,
        schema_digest: record.schema_digest.clone(),
    }
}

pub(super) fn write_authz_tuple_response(
    record: &crate::persistence::AuthzTupleRecord,
) -> Result<WriteAuthzTupleResponse, Status> {
    Ok(WriteAuthzTupleResponse {
        revision: revision_to_u64(record.revision)?,
        zookie: zookie(record.revision),
        record_hash: record.record_hash.clone(),
    })
}

pub(super) fn write_authz_tuple_batch_response(
    records: &[crate::persistence::AuthzTupleRecord],
) -> Result<WriteAuthzTuplesResponse, Status> {
    let revision = records
        .iter()
        .map(|record| record.revision)
        .max()
        .unwrap_or(0);
    Ok(WriteAuthzTuplesResponse {
        results: records
            .iter()
            .map(write_authz_tuple_response)
            .collect::<Result<Vec<_>, _>>()?,
        revision: revision_to_u64(revision)?,
        zookie: zookie(revision),
    })
}

pub(super) fn authz_tuple_response_for_realm(
    record: &crate::persistence::AuthzTupleRecord,
    realm_id: &str,
) -> Result<AuthzTuple, Status> {
    let namespace = decode_realm_namespace(realm_id, &record.namespace)
        .ok_or_else(|| Status::internal("authz tuple namespace is outside requested realm"))?;
    Ok(AuthzTuple {
        namespace: namespace.to_string(),
        object_id: record.object_id.clone(),
        relation: record.relation.clone(),
        subject_kind: record.subject_kind.clone(),
        subject_id: decode_userset_subject_realm(
            realm_id,
            &record.subject_kind,
            &record.subject_id,
        ),
        caveat_hash: record.caveat_hash.clone(),
        revision: revision_to_u64(record.revision)?,
        zookie: zookie(record.revision),
    })
}

pub(super) fn optional_filter_value(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

pub(super) fn optional_str(value: &str) -> Option<&str> {
    if value.is_empty() { None } else { Some(value) }
}

const AUTHZ_PAGE_TOKEN_VERSION: u8 = 1;
const AUTHZ_PAGE_TOKEN_TTL_SECONDS: u64 = 15 * 60;
const MAX_AUTHZ_PAGE_TOKEN_ENCODED_BYTES: usize = 128 * 1024;
pub(super) const MAX_AUTHZ_PAGE_POSITION_BYTES: usize = u16::MAX as usize;
const MAX_AUTHZ_PAGE_POSITION_TEXT_BYTES: usize = ((MAX_AUTHZ_PAGE_POSITION_BYTES + 2) / 3) * 4;

pub(super) struct AuthzPageBinding<'a> {
    pub tenant_id: i64,
    pub principal_id: &'a str,
    pub revision: i64,
    pub filter_hash: &'a str,
    pub page_size: usize,
}

#[derive(Debug, Clone)]
pub(super) struct AuthzPageToken {
    version: u8,
    tenant_id: i64,
    pub(super) revision: i64,
    filter_hash: String,
    principal_hash: String,
    page_size: usize,
    pub(super) position: String,
    expires_at_unix_seconds: u64,
    signature: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct AuthzPageTokenProto {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(int64, tag = "3")]
    revision: i64,
    #[prost(string, tag = "4")]
    filter_hash: String,
    #[prost(string, tag = "5")]
    principal_hash: String,
    #[prost(uint32, tag = "6")]
    page_size: u32,
    #[prost(string, tag = "7")]
    position: String,
    #[prost(uint64, tag = "8")]
    expires_at_unix_seconds: u64,
    #[prost(string, tag = "9")]
    signature: String,
}

pub(super) fn parse_authz_page_token(
    value: &str,
    binding: &AuthzPageBinding<'_>,
    signing_key: &[u8],
) -> Result<Option<AuthzPageToken>, Status> {
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() > MAX_AUTHZ_PAGE_TOKEN_ENCODED_BYTES {
        return Err(Status::invalid_argument("Invalid authz page token"));
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| Status::invalid_argument("Invalid authz page token"))?;
    let token = authz_page_token_from_proto(
        crate::core_store::decode_deterministic_proto::<AuthzPageTokenProto>(
            &bytes,
            "authz page token",
        )
        .map_err(|_| Status::invalid_argument("Invalid authz page token"))?,
    )?;
    if token.version != AUTHZ_PAGE_TOKEN_VERSION
        || token.tenant_id != binding.tenant_id
        || token.revision != binding.revision
        || token.filter_hash != binding.filter_hash
        || token.principal_hash != authz_page_principal_hash(binding.principal_id)
        || token.page_size != binding.page_size
        || token.position.is_empty()
        || token.position.len() > MAX_AUTHZ_PAGE_POSITION_TEXT_BYTES
        || token.expires_at_unix_seconds < authz_page_now()?
    {
        return Err(Status::invalid_argument(
            "Authz page token does not match this request",
        ));
    }
    let expected = sign_authz_page_token(&token, signing_key)?;
    if !constant_time_eq::constant_time_eq(token.signature.as_bytes(), expected.as_bytes()) {
        return Err(Status::invalid_argument("Invalid authz page token"));
    }
    Ok(Some(token))
}

pub(super) fn authz_page_token_revision(value: &str) -> Result<Option<i64>, Status> {
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() > MAX_AUTHZ_PAGE_TOKEN_ENCODED_BYTES {
        return Err(Status::invalid_argument("Invalid authz page token"));
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| Status::invalid_argument("Invalid authz page token"))?;
    let proto = crate::core_store::decode_deterministic_proto::<AuthzPageTokenProto>(
        &bytes,
        "authz page token",
    )
    .map_err(|_| Status::invalid_argument("Invalid authz page token"))?;
    Ok(Some(proto.revision))
}

pub(super) fn encode_authz_page_token(
    binding: &AuthzPageBinding<'_>,
    position: &str,
    signing_key: &[u8],
) -> Result<String, Status> {
    if position.is_empty() || position.len() > MAX_AUTHZ_PAGE_POSITION_TEXT_BYTES {
        return Err(Status::internal(
            "authz page position is outside supported bounds",
        ));
    }
    let mut token = AuthzPageToken {
        version: AUTHZ_PAGE_TOKEN_VERSION,
        tenant_id: binding.tenant_id,
        revision: binding.revision,
        filter_hash: binding.filter_hash.to_string(),
        principal_hash: authz_page_principal_hash(binding.principal_id),
        page_size: binding.page_size,
        position: position.to_string(),
        expires_at_unix_seconds: authz_page_now()?.saturating_add(AUTHZ_PAGE_TOKEN_TTL_SECONDS),
        signature: String::new(),
    };
    token.signature = sign_authz_page_token(&token, signing_key)?;
    let bytes = crate::core_store::encode_deterministic_proto(&authz_page_token_to_proto(&token)?);
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

pub(super) fn encode_authz_page_position(position: &[u8]) -> Result<String, Status> {
    if position.is_empty() || position.len() > MAX_AUTHZ_PAGE_POSITION_BYTES {
        return Err(Status::internal(
            "authz binary page position is outside supported bounds",
        ));
    }
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(position))
}

pub(super) fn decode_authz_page_position(
    position: Option<&str>,
) -> Result<Option<Vec<u8>>, Status> {
    position
        .map(|position| {
            if position.is_empty() || position.len() > MAX_AUTHZ_PAGE_POSITION_TEXT_BYTES {
                return Err(Status::invalid_argument("Invalid authz page token"));
            }
            let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(position)
                .map_err(|_| Status::invalid_argument("Invalid authz page token"))?;
            if decoded.is_empty() || decoded.len() > MAX_AUTHZ_PAGE_POSITION_BYTES {
                return Err(Status::invalid_argument("Invalid authz page token"));
            }
            Ok(decoded)
        })
        .transpose()
}

fn authz_page_token_to_proto(token: &AuthzPageToken) -> Result<AuthzPageTokenProto, Status> {
    Ok(AuthzPageTokenProto {
        version: u32::from(token.version),
        tenant_id: token.tenant_id,
        revision: token.revision,
        filter_hash: token.filter_hash.clone(),
        principal_hash: token.principal_hash.clone(),
        page_size: u32::try_from(token.page_size)
            .map_err(|_| Status::invalid_argument("Invalid authz page token"))?,
        position: token.position.clone(),
        expires_at_unix_seconds: token.expires_at_unix_seconds,
        signature: token.signature.clone(),
    })
}

fn authz_page_token_from_proto(proto: AuthzPageTokenProto) -> Result<AuthzPageToken, Status> {
    Ok(AuthzPageToken {
        version: u8::try_from(proto.version)
            .map_err(|_| Status::invalid_argument("Invalid authz page token"))?,
        tenant_id: proto.tenant_id,
        revision: proto.revision,
        filter_hash: proto.filter_hash,
        principal_hash: proto.principal_hash,
        page_size: usize::try_from(proto.page_size)
            .map_err(|_| Status::invalid_argument("Invalid authz page token"))?,
        position: proto.position,
        expires_at_unix_seconds: proto.expires_at_unix_seconds,
        signature: proto.signature,
    })
}

fn sign_authz_page_token(token: &AuthzPageToken, signing_key: &[u8]) -> Result<String, Status> {
    let mut mac = Hmac::<Sha256>::new_from_slice(signing_key)
        .map_err(|_| Status::internal("Invalid authz page token signing key"))?;
    mac.update(b"authz-page-token-v1");
    mac.update(&[token.version]);
    mac.update(&token.tenant_id.to_le_bytes());
    mac.update(&token.revision.to_le_bytes());
    for part in [
        token.filter_hash.as_bytes(),
        token.principal_hash.as_bytes(),
        token.position.as_bytes(),
    ] {
        mac.update(&(part.len() as u64).to_le_bytes());
        mac.update(part);
    }
    mac.update(&(token.page_size as u64).to_le_bytes());
    mac.update(&token.expires_at_unix_seconds.to_le_bytes());
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}

pub(super) fn authz_page_filter_hash(kind: &str, values: &[&str]) -> String {
    let mut input = Vec::new();
    input.extend_from_slice(&(kind.len() as u64).to_le_bytes());
    input.extend_from_slice(kind.as_bytes());
    for value in values {
        input.extend_from_slice(&(value.len() as u64).to_le_bytes());
        input.extend_from_slice(value.as_bytes());
    }
    hex::encode(hash32(&input))
}

pub(super) fn normalize_page_size(value: u32) -> Result<usize, Status> {
    if value == 0 {
        return Ok(100);
    }
    let value = usize::try_from(value)
        .map_err(|_| Status::invalid_argument("page_size exceeds supported range"))?;
    if value > crate::services::collection_cursor::MAX_PAGE_SIZE {
        return Err(Status::invalid_argument("page_size must not exceed 1000"));
    }
    Ok(value)
}

fn authz_page_principal_hash(principal_id: &str) -> String {
    let mut input = Vec::with_capacity(principal_id.len() + 8);
    input.extend_from_slice(&(principal_id.len() as u64).to_le_bytes());
    input.extend_from_slice(principal_id.as_bytes());
    hex::encode(hash32(&input))
}

fn authz_page_now() -> Result<u64, Status> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| Status::internal("system clock predates Unix epoch"))
}

pub(super) fn authz_tuple_log_response(
    record: &crate::persistence::AuthzTupleRecord,
) -> WatchAuthzTupleLogResponse {
    let revision = revision_to_u64(record.revision).unwrap_or_default();
    let written_at = record.written_at.to_string();
    WatchAuthzTupleLogResponse {
        revision,
        namespace: record.namespace.clone(),
        object_id: record.object_id.clone(),
        relation: record.relation.clone(),
        subject_kind: record.subject_kind.clone(),
        subject_id: record.subject_id.clone(),
        caveat_hash: record.caveat_hash.clone(),
        operation: record.operation.clone(),
        written_by: record.written_by.clone(),
        reason: record.reason.clone(),
        record_hash: record.record_hash.clone(),
        written_at: written_at.clone(),
        envelope: Some(watch_envelope::envelope(WatchEnvelopeParts {
            watch_stream_id: "authz_tuple_log",
            partition_family: "authz_tuple",
            partition_id: record.namespace.clone(),
            cursor: revision.into(),
            mutation_id: record.mutation_id.to_string(),
            record_kind: "authz_tuple".to_string(),
            object_ref: format!(
                "{}:{}#{}",
                record.namespace, record.object_id, record.relation
            ),
            authz_revision: revision,
            index_generation: 0,
            personaldb_log_index: 0,
            payload_hash: record.record_hash.clone(),
            emitted_at: written_at,
        })),
    }
}

pub(super) fn authz_tuple_log_response_for_realm(
    record: &crate::persistence::AuthzTupleRecord,
    realm_id: &str,
) -> WatchAuthzTupleLogResponse {
    let mut response = authz_tuple_log_response(record);
    if let Some(namespace) = decode_realm_namespace(realm_id, &response.namespace) {
        response.namespace = namespace.to_string();
    }
    response.subject_id =
        decode_userset_subject_realm(realm_id, &response.subject_kind, &response.subject_id);
    response
}

pub(super) fn authz_namespace_watch_response(
    event: authz_namespace_watch::AuthzNamespaceWatchEvent,
) -> WatchAuthzNamespaceResponse {
    let (cursor_low, cursor_high) = split_u128(event.cursor);
    let payload = event.payload;
    let emitted_at = payload.emitted_at.clone();
    let namespace = payload.namespace.clone();
    let payload_hash = watch_envelope::payload_hash(&payload);
    WatchAuthzNamespaceResponse {
        cursor_low,
        cursor_high,
        namespace: namespace.clone(),
        event_type: payload.event_type,
        authz_revision: event.authz_revision,
        schema_hash: payload.schema_hash,
        invalidates_derived_usersets: payload.invalidates_derived_usersets,
        emitted_at: emitted_at.clone(),
        envelope: Some(watch_envelope::envelope(WatchEnvelopeParts {
            watch_stream_id: "authz_namespace",
            partition_family: "authz_namespace",
            partition_id: namespace.clone(),
            cursor: event.cursor,
            mutation_id: watch_envelope::uuid_from_bytes(event.mutation_id),
            record_kind: "authz_namespace".to_string(),
            object_ref: namespace,
            authz_revision: event.authz_revision,
            index_generation: 0,
            personaldb_log_index: 0,
            payload_hash,
            emitted_at,
        })),
    }
}

pub(super) fn authz_derived_lag_watch_response(
    event: authz_derived_lag_watch::AuthzDerivedLagWatchEvent,
) -> WatchAuthzDerivedLagResponse {
    let (cursor_low, cursor_high) = split_u128(event.cursor);
    let (source_cursor_low, source_cursor_high) = split_u128(event.payload.source_cursor);
    let revision_lag = event.payload.revision_lag();
    let payload = event.payload;
    let emitted_at = payload.emitted_at.clone();
    let derived_index_id = payload.derived_index_id.clone();
    let generation = payload.generation;
    let latest_revision = payload.latest_revision;
    let payload_hash = watch_envelope::payload_hash(&payload);
    WatchAuthzDerivedLagResponse {
        cursor_low,
        cursor_high,
        derived_index_id: derived_index_id.clone(),
        derived_index_kind: payload.derived_index_kind,
        processed_revision: payload.processed_revision,
        latest_revision,
        revision_lag,
        source_cursor_low,
        source_cursor_high,
        source_manifest_hash: payload.source_manifest_hash,
        generation,
        authz_revision: event.authz_revision,
        emitted_at: emitted_at.clone(),
        envelope: Some(watch_envelope::envelope(WatchEnvelopeParts {
            watch_stream_id: "authz_derived_lag",
            partition_family: "authz_derived_lag",
            partition_id: derived_index_id.clone(),
            cursor: event.cursor,
            mutation_id: watch_envelope::uuid_from_bytes(event.mutation_id),
            record_kind: "authz_derived_lag".to_string(),
            object_ref: derived_index_id,
            authz_revision: event.authz_revision,
            index_generation: generation,
            personaldb_log_index: 0,
            payload_hash,
            emitted_at,
        })),
    }
}

pub(super) fn split_u128(value: u128) -> (u64, u64) {
    (value as u64, (value >> 64) as u64)
}

pub(super) fn join_u128(low: u64, high: u64) -> u128 {
    u128::from(low) | (u128::from(high) << 64)
}

pub(super) fn mutation_id_from_record_hash(record_hash: &str) -> [u8; 16] {
    let mut mutation_id = [0; 16];
    if let Ok(bytes) = hex::decode(record_hash) {
        let len = bytes.len().min(mutation_id.len());
        mutation_id[..len].copy_from_slice(&bytes[..len]);
    }
    mutation_id
}

pub(super) async fn app_in_claims_tenant(
    state: &AppState,
    tenant_id: i64,
    app_name: &str,
) -> Result<crate::persistence::App, Status> {
    state
        .persistence
        .get_app_by_tenant_name(tenant_id, app_name)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found("Grantee app not found"))
}

pub(super) fn validate_public_delegation_resource(
    claims: &auth::Claims,
    resource: &str,
) -> Result<(), Status> {
    let resource = resource.trim();
    if resource.is_empty() {
        return Err(Status::invalid_argument("resource is required"));
    }
    if resource == "*"
        || resource.starts_with("system:")
        || resource == "anvil_mesh"
        || resource.starts_with("anvil_mesh:")
        || resource.starts_with("anvil_mesh/")
        || resource == "_system"
        || resource.starts_with("_system/")
        || resource == "_anvil"
        || resource.starts_with("_anvil/")
        || resource.contains("/_anvil/")
        || resource.contains("/_system/")
    {
        return Err(Status::permission_denied(
            "Public policy delegation cannot grant system, reserved, or wildcard authority",
        ));
    }
    if let Some(rest) = resource.strip_prefix("tenant:") {
        let tenant_id = rest.split(['/', ':']).next().unwrap_or_default();
        if tenant_id != claims.tenant_id.to_string() {
            return Err(Status::permission_denied(
                "Public policy delegation cannot grant cross-tenant authority",
            ));
        }
    }
    if let Some(rest) = resource.strip_prefix("tenant-") {
        let tenant_id = rest.split(['/', ':']).next().unwrap_or_default();
        if tenant_id != claims.tenant_id.to_string() {
            return Err(Status::permission_denied(
                "Public policy delegation cannot grant cross-tenant authority",
            ));
        }
    }
    Ok(())
}

pub(super) async fn require_app_management_permission(
    state: &AppState,
    claims: &auth::Claims,
    action: AnvilAction,
) -> Result<(), Status> {
    crate::access_control::require_action(
        &state.storage,
        &state.persistence,
        claims,
        action,
        &format!("tenant:{}", claims.tenant_id),
    )
    .await?;
    Ok(())
}

pub(super) fn validate_public_app_request(
    app_name: &str,
    request_id: &str,
    idempotency_key: &str,
) -> Result<(), Status> {
    if app_name.trim().is_empty() {
        return Err(Status::invalid_argument("app_name is required"));
    }
    if request_id.trim().is_empty() {
        return Err(Status::invalid_argument("request_id is required"));
    }
    if idempotency_key.trim().is_empty() {
        return Err(Status::invalid_argument("idempotency_key is required"));
    }
    Ok(())
}
