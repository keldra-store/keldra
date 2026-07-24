use crate::anvil_api::audit_service_server::AuditService;
use crate::anvil_api::*;
use crate::{AppState, access_control, auth, permissions::AnvilAction, tenant_audit};
use base64::Engine;
use hmac::Mac;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl AuditService for AppState {
    async fn list_tenant_audit_events(
        &self,
        request: Request<ListAuditEventsRequest>,
    ) -> Result<Response<AuditEventsResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::AppRead,
            "tenant",
        )
        .await?;
        let limit = crate::services::collection_cursor::page_size(req.page.as_ref())?;
        let filter = tenant_audit::TenantAuditEventFilter {
            principal_id: none_if_empty(&req.principal_id),
            resource_id: none_if_empty(&req.resource_id),
            action: none_if_empty(&req.action),
        };
        let revision =
            tenant_audit::tenant_audit_collection_revision(&self.storage, claims.tenant_id)
                .await
                .map_err(|err| Status::internal(err.to_string()))?;
        let cursor = decode_tenant_audit_cursor(
            req.page.as_ref(),
            &claims,
            &req,
            limit,
            &revision,
            &self.config.anvil_secret_encryption_key.as_bytes(),
        )?;
        let after_cursor = cursor
            .as_deref()
            .map(hex::decode)
            .transpose()
            .map_err(|_| Status::invalid_argument("Invalid tenant audit cursor"))?;
        let page = tenant_audit::list_tenant_audit_event_page_after(
            &self.storage,
            claims.tenant_id,
            filter,
            after_cursor.as_deref(),
            limit,
        )
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
        if page.revision != revision {
            return Err(Status::aborted(
                "Tenant audit collection changed while reading this page",
            ));
        }
        let next_cursor = page.next_cursor.map_or(Ok(String::new()), |cursor| {
            encode_tenant_audit_cursor(
                &hex::encode(cursor),
                &claims,
                &req,
                limit,
                &revision,
                &self.config.anvil_secret_encryption_key.as_bytes(),
            )
        })?;
        Ok(Response::new(AuditEventsResponse {
            request_id: req.request_id,
            page: Some(PageResponse {
                next_page_token: next_cursor,
            }),
            events: page.events.into_iter().map(audit_event_to_proto).collect(),
            data_source: "tenant_audit_log".to_string(),
        }))
    }
}

pub(crate) async fn record_tenant_audit_event(
    state: &AppState,
    claims: &auth::Claims,
    request_id: &str,
    resource_id: impl Into<String>,
    action: impl Into<String>,
    details: serde_json::Value,
) -> Result<String, Status> {
    let action = action.into();
    let audit_event_id = format!(
        "tenant-audit:{}:{}:{}",
        claims.tenant_id,
        request_id,
        uuid::Uuid::new_v4().simple()
    );
    let event = tenant_audit::TenantAuditEvent {
        schema: tenant_audit::TENANT_AUDIT_EVENT_SCHEMA.to_string(),
        audit_event_id: audit_event_id.clone(),
        request_id: request_id.to_string(),
        tenant_id: claims.tenant_id,
        principal_id: claims.sub.clone(),
        resource_id: resource_id.into(),
        action,
        created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        details_json: serde_json::to_string(&details)
            .map_err(|_| Status::internal("Failed to encode tenant audit details"))?,
    };
    tenant_audit::append_tenant_audit_event(&state.storage, &event)
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
    Ok(audit_event_id)
}

fn none_if_empty(value: &str) -> Option<&str> {
    let value = value.trim();
    if value.is_empty() { None } else { Some(value) }
}

fn audit_event_to_proto(event: tenant_audit::TenantAuditEvent) -> AuditEventRecord {
    AuditEventRecord {
        audit_event_id: event.audit_event_id,
        request_id: event.request_id,
        principal_id: event.principal_id,
        resource_id: event.resource_id,
        action: event.action,
        audit_reason: String::new(),
        created_at: event.created_at,
        details_json: event.details_json,
    }
}

fn decode_tenant_audit_cursor(
    page: Option<&PageRequest>,
    claims: &auth::Claims,
    req: &ListAuditEventsRequest,
    limit: usize,
    revision: &str,
    key: &[u8],
) -> Result<Option<String>, Status> {
    let Some(cursor) = page
        .map(|page| page.page_token.trim())
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| Status::invalid_argument("Invalid tenant audit cursor"))?;
    let token = tenant_audit_cursor_from_proto(
        crate::core_store::decode_deterministic_proto::<TenantAuditCursorProto>(
            &bytes,
            "tenant audit cursor",
        )
        .map_err(|_| Status::invalid_argument("Invalid tenant audit cursor"))?,
    )?;
    if token.version != 1 {
        return Err(Status::invalid_argument("Invalid tenant audit cursor"));
    }
    let expected_signature = sign_cursor(&token.without_signature(), key)?;
    if !constant_time_eq::constant_time_eq(
        token.signature.as_bytes(),
        expected_signature.as_bytes(),
    ) {
        return Err(Status::invalid_argument("Invalid tenant audit cursor"));
    }
    let expected = cursor_claims("", claims, req, limit, revision);
    if token.scope != expected.scope
        || token.tenant_id != expected.tenant_id
        || token.principal_hash != expected.principal_hash
        || token.filter_hash != expected.filter_hash
        || token.limit != expected.limit
        || token.revision != expected.revision
    {
        return Err(Status::invalid_argument(
            "Tenant audit cursor does not match this request",
        ));
    }
    Ok(Some(token.position))
}

fn encode_tenant_audit_cursor(
    position: &str,
    claims: &auth::Claims,
    req: &ListAuditEventsRequest,
    limit: usize,
    revision: &str,
    key: &[u8],
) -> Result<String, Status> {
    let mut token = cursor_claims(position, claims, req, limit, revision);
    token.signature = sign_cursor(&token.without_signature(), key)?;
    let bytes =
        crate::core_store::encode_deterministic_proto(&tenant_audit_cursor_to_proto(&token));
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

#[derive(Debug, Clone)]
struct TenantAuditCursor {
    version: u8,
    scope: String,
    position: String,
    tenant_id: i64,
    principal_hash: String,
    filter_hash: String,
    limit: u32,
    revision: String,
    signature: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct TenantAuditCursorProto {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(string, tag = "2")]
    scope: String,
    #[prost(string, tag = "3")]
    position: String,
    #[prost(int64, tag = "4")]
    tenant_id: i64,
    #[prost(string, tag = "5")]
    principal_hash: String,
    #[prost(string, tag = "6")]
    filter_hash: String,
    #[prost(uint32, tag = "7")]
    limit: u32,
    #[prost(string, tag = "8")]
    revision: String,
    #[prost(string, tag = "9")]
    signature: String,
}

impl TenantAuditCursor {
    fn without_signature(&self) -> TenantAuditCursorToSign<'_> {
        TenantAuditCursorToSign {
            version: self.version,
            scope: &self.scope,
            position: &self.position,
            tenant_id: self.tenant_id,
            principal_hash: &self.principal_hash,
            filter_hash: &self.filter_hash,
            limit: self.limit,
            revision: &self.revision,
        }
    }
}

fn tenant_audit_cursor_to_proto(token: &TenantAuditCursor) -> TenantAuditCursorProto {
    TenantAuditCursorProto {
        version: u32::from(token.version),
        scope: token.scope.clone(),
        position: token.position.clone(),
        tenant_id: token.tenant_id,
        principal_hash: token.principal_hash.clone(),
        filter_hash: token.filter_hash.clone(),
        limit: token.limit,
        revision: token.revision.clone(),
        signature: token.signature.clone(),
    }
}

fn tenant_audit_cursor_from_proto(
    proto: TenantAuditCursorProto,
) -> Result<TenantAuditCursor, Status> {
    Ok(TenantAuditCursor {
        version: u8::try_from(proto.version)
            .map_err(|_| Status::invalid_argument("Invalid tenant audit cursor"))?,
        scope: proto.scope,
        position: proto.position,
        tenant_id: proto.tenant_id,
        principal_hash: proto.principal_hash,
        filter_hash: proto.filter_hash,
        limit: proto.limit,
        revision: proto.revision,
        signature: proto.signature,
    })
}

#[derive(Debug, Clone, Copy)]
struct TenantAuditCursorToSign<'a> {
    version: u8,
    scope: &'a str,
    position: &'a str,
    tenant_id: i64,
    principal_hash: &'a str,
    filter_hash: &'a str,
    limit: u32,
    revision: &'a str,
}

fn cursor_claims(
    position: &str,
    claims: &auth::Claims,
    req: &ListAuditEventsRequest,
    limit: usize,
    revision: &str,
) -> TenantAuditCursor {
    TenantAuditCursor {
        version: 1,
        scope: "tenant_audit.list.v1".to_string(),
        position: position.to_string(),
        tenant_id: claims.tenant_id,
        principal_hash: hash_parts(&[&claims.sub, &claims.tenant_id.to_string()]),
        filter_hash: hash_parts(&[&req.principal_id, &req.resource_id, &req.action]),
        limit: u32::try_from(limit).unwrap_or(u32::MAX),
        revision: revision.to_string(),
        signature: String::new(),
    }
}

fn sign_cursor(claims: &TenantAuditCursorToSign<'_>, key: &[u8]) -> Result<String, Status> {
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(key)
        .map_err(|_| Status::internal("Invalid tenant audit cursor signing key"))?;
    mac.update(b"anvil-tenant-audit-cursor-v1");
    mac.update(&[claims.version]);
    update_mac_part(&mut mac, claims.scope.as_bytes());
    update_mac_part(&mut mac, claims.position.as_bytes());
    mac.update(&claims.tenant_id.to_le_bytes());
    update_mac_part(&mut mac, claims.principal_hash.as_bytes());
    update_mac_part(&mut mac, claims.filter_hash.as_bytes());
    mac.update(&claims.limit.to_le_bytes());
    update_mac_part(&mut mac, claims.revision.as_bytes());
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}

fn hash_parts(parts: &[&str]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil-tenant-audit-cursor-hash-v1");
    for part in parts {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    hex::encode(hasher.finalize().as_bytes())
}

fn update_mac_part(mac: &mut hmac::Hmac<sha2::Sha256>, value: &[u8]) {
    mac.update(&(value.len() as u64).to_le_bytes());
    mac.update(value);
}
