use super::*;

pub(super) fn parse_user_metadata_json(value: &str) -> Result<Option<serde_json::Value>, Status> {
    if value.trim().is_empty() {
        return Ok(None);
    }
    let parsed: serde_json::Value = serde_json::from_str(value)
        .map_err(|e| Status::invalid_argument(format!("Invalid user_metadata_json: {e}")))?;
    if !parsed.is_object() {
        return Err(Status::invalid_argument(
            "user_metadata_json must be a JSON object",
        ));
    }
    Ok(Some(parsed))
}

pub(super) fn json_object_string(value: Option<&serde_json::Value>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "{}".to_string())
}

pub(super) fn append_stream_record_info(
    record: crate::object_manager::AppendStreamRecordRead,
) -> AppendStreamRecordInfo {
    AppendStreamRecordInfo {
        record_sequence: record.record_sequence,
        payload_hash: record.payload_hash,
        payload_size: record.payload_size,
        created_at: record.created_at.to_rfc3339(),
        content_type: record.content_type.unwrap_or_default(),
        user_metadata_json: json_object_string(record.user_metadata.as_ref()),
        payload: record.payload.unwrap_or_default(),
        authenticated_principal: record.authenticated_principal,
    }
}

pub(super) async fn latest_authz_revision(state: &AppState, tenant_id: i64) -> Result<u64, Status> {
    let revision = authz_journal::latest_authz_revision(&state.storage, tenant_id)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
    u64::try_from(revision).map_err(|_| Status::internal("Invalid authz revision"))
}

pub(super) fn parse_optional_version_id(value: Option<&str>) -> Result<Option<uuid::Uuid>, Status> {
    value
        .filter(|value| !value.is_empty())
        .map(uuid::Uuid::parse_str)
        .transpose()
        .map_err(|_| Status::invalid_argument("Invalid version_id"))
}

pub(super) fn object_read_consistency(
    value: Option<&ReadConsistency>,
) -> Result<crate::object_manager::ObjectReadConsistency, Status> {
    let Some(value) = value else {
        return Ok(crate::object_manager::ObjectReadConsistency::Latest);
    };
    let Some(mode) = value.mode.as_ref() else {
        return Ok(crate::object_manager::ObjectReadConsistency::Latest);
    };
    match mode {
        crate::anvil_api::read_consistency::Mode::Latest(_) => {
            Ok(crate::object_manager::ObjectReadConsistency::Latest)
        }
        crate::anvil_api::read_consistency::Mode::AtRootGeneration(generation) => {
            Ok(crate::object_manager::ObjectReadConsistency::AtRootGeneration(*generation))
        }
        crate::anvil_api::read_consistency::Mode::AtAuthzRevision(revision) => {
            let revision = revision
                .parse::<i64>()
                .map_err(|_| Status::invalid_argument("Invalid authz revision"))?;
            Ok(crate::object_manager::ObjectReadConsistency::AtAuthzRevision(revision))
        }
    }
}

pub(super) async fn object_watch_cursor(
    state: &AppState,
    object: &crate::persistence::Object,
) -> Result<u64, Status> {
    let cursor = watch_log::latest_object_watch_cursor(
        &state.mvcc,
        object.tenant_id,
        object.bucket_id,
        object.version_id,
    )
    .map_err(|e| Status::internal(e.to_string()))?
    .ok_or_else(|| Status::internal("Object mutation watch event not found"))?;
    u64::try_from(cursor).map_err(|_| Status::internal("Invalid object watch cursor"))
}

pub(super) fn object_authz_revision(object: &crate::persistence::Object) -> Result<u64, Status> {
    u64::try_from(object.authz_revision).map_err(|_| Status::internal("Invalid authz revision"))
}
