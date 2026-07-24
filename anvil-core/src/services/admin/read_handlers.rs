use super::*;

pub(super) async fn list_diagnostics(
    state: &AppState,
    request: Request<ListDiagnosticsRequest>,
) -> Result<Response<DiagnosticsResponse>, Status> {
    let principal = require_admin(&request, state, SystemAdminRelation::ViewDiagnostics).await?;
    let req = request.into_inner();
    let request_id = require_request_id(&req.request_id)?.to_string();
    let page = req.page.as_ref();
    let limit = page_limit(page)?;
    let source = req.source.trim();

    if !req.severity.trim().is_empty() {
        validate_diagnostic_severity(&req.severity)?;
    }

    let index_scope =
        if source.is_empty() || source == "index" || source == "index_diagnostic_journal" {
            if req.tenant_id.trim().is_empty() || req.bucket_name.trim().is_empty() {
                if source == "index" || source == "index_diagnostic_journal" {
                    return Err(Status::invalid_argument(
                        "tenant_id and bucket_name are required for index diagnostics",
                    ));
                }
                None
            } else {
                let tenant_id = resolve_tenant_id(state, &req.tenant_id).await?;
                let bucket = state
                    .persistence
                    .get_bucket_by_name(tenant_id, &req.bucket_name)
                    .await
                    .map_err(|err| Status::internal(err.to_string()))?
                    .ok_or_else(|| Status::not_found("Bucket not found"))?;
                Some((tenant_id, bucket.id))
            }
        } else {
            None
        };

    let mut diagnostics = Vec::new();

    if source.is_empty() || source == "mesh" || source == "mesh_lifecycle" {
        diagnostics.extend(mesh_lifecycle_diagnostics(state).await?);
    }

    if source.is_empty() || source == "mesh" || source == "mesh_routing_projection" {
        diagnostics.extend(mesh_routing_projection_diagnostics(state).await?);
    }

    if !req.severity.trim().is_empty() {
        diagnostics.retain(|diagnostic| diagnostic.severity == req.severity);
    }
    let mut positions = diagnostics
        .iter()
        .map(|diagnostic| (diagnostic_position(diagnostic), diagnostic.cursor))
        .collect::<Vec<_>>();
    if let Some((tenant_id, bucket_id)) = index_scope {
        let revision = crate::index_diagnostic_journal::index_diagnostic_revision(
            &state.storage,
            tenant_id,
            bucket_id,
        )
        .await
        .map_err(|error| Status::internal(error.to_string()))?;
        positions.push((
            "index_diagnostic_journal:head".to_string(),
            revision
                .parse::<u64>()
                .map_err(|_| Status::internal("Invalid index diagnostic revision"))?,
        ));
    }
    positions.sort_by(|left, right| left.0.cmp(&right.0));
    let revision = admin_cursor::collection_revision(
        positions
            .iter()
            .map(|(position, cursor)| (position.as_str(), *cursor)),
    );
    let filters = [
        ("source", source),
        ("tenant_id", req.tenant_id.trim()),
        ("bucket_name", req.bucket_name.trim()),
        ("index_name", req.index_name.trim()),
        ("severity", req.severity.trim()),
    ];
    let binding = AdminCursorBinding {
        scope: "admin.list_diagnostics.v1",
        filters: &filters,
        principal: &principal,
        limit,
        revision: &revision,
        sort: "source.cursor.id.asc",
    };
    let cursor =
        admin_cursor::decode_page_cursor(page, &binding, state.config.jwt_secret.as_bytes())?;
    if let Some((tenant_id, bucket_id)) = index_scope {
        let after_cursor = cursor
            .as_deref()
            .map(index_diagnostic_admin_cursor)
            .transpose()?
            .flatten()
            .unwrap_or_default();
        let cursor_is_after_index = cursor.as_deref().is_some_and(|position| {
            !position.starts_with("index_diagnostic_journal:")
                && position > "index_diagnostic_journal:"
        });
        if !cursor_is_after_index {
            let query_limit = i32::try_from(limit + 1)
                .map_err(|_| Status::invalid_argument("page size exceeds supported range"))?;
            diagnostics.extend(
                state
                    .persistence
                    .list_index_diagnostics(
                        tenant_id,
                        bucket_id,
                        &req.index_name,
                        &req.severity,
                        after_cursor,
                        query_limit,
                    )
                    .await
                    .map_err(|err| Status::internal(err.to_string()))?
                    .into_iter()
                    .map(index_diagnostic_to_admin_record)
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
    }
    diagnostics.sort_by(|left, right| diagnostic_position(left).cmp(&diagnostic_position(right)));
    let mut diagnostics = diagnostics
        .into_iter()
        .filter(|diagnostic| {
            cursor
                .as_deref()
                .is_none_or(|cursor| diagnostic_position(diagnostic).as_str() > cursor)
        })
        .take(limit + 1)
        .collect::<Vec<_>>();
    let has_more = diagnostics.len() > limit;
    if has_more {
        diagnostics.truncate(limit);
    }
    let next_cursor = if has_more {
        diagnostics.last().map_or(Ok(String::new()), |diagnostic| {
            admin_cursor::encode_next_cursor(
                &diagnostic_position(diagnostic),
                &binding,
                state.config.jwt_secret.as_bytes(),
            )
        })?
    } else {
        String::new()
    };

    Ok(Response::new(DiagnosticsResponse {
        request_id,
        page: Some(PageResponse {
            next_page_token: next_cursor,
        }),
        diagnostics,
        data_source: if source.is_empty() {
            "combined".to_string()
        } else if source == "index" {
            "index_diagnostic_journal".to_string()
        } else if source == "mesh" {
            "mesh".to_string()
        } else {
            source.to_string()
        },
    }))
}

fn index_diagnostic_admin_cursor(position: &str) -> Result<Option<i64>, Status> {
    let Some(rest) = position.strip_prefix("index_diagnostic_journal:") else {
        return Ok(None);
    };
    let cursor = rest
        .split(':')
        .next()
        .ok_or_else(|| Status::invalid_argument("Invalid index diagnostic cursor"))?
        .parse::<u64>()
        .map_err(|_| Status::invalid_argument("Invalid index diagnostic cursor"))?;
    i64::try_from(cursor)
        .map(Some)
        .map_err(|_| Status::invalid_argument("Invalid index diagnostic cursor"))
}

pub(super) async fn list_audit_events(
    state: &AppState,
    request: Request<ListAuditEventsRequest>,
) -> Result<Response<AuditEventsResponse>, Status> {
    let principal = require_admin(&request, state, SystemAdminRelation::ViewAuditLog).await?;
    let req = request.into_inner();
    let request_id = require_request_id(&req.request_id)?.to_string();
    let page = req.page.as_ref();
    let limit = page_limit(page)?;
    let revision = admin_audit::audit_collection_revision(&state.storage)
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
    let filters = [
        ("principal_id", req.principal_id.as_str()),
        ("resource_id", req.resource_id.as_str()),
        ("action", req.action.as_str()),
    ];
    let binding = AdminCursorBinding {
        scope: "admin.list_audit_events.v1",
        filters: &filters,
        principal: &principal,
        limit,
        revision: &revision,
        sort: "created_at.audit_event_id.asc",
    };
    let cursor =
        admin_cursor::decode_page_cursor(page, &binding, state.config.jwt_secret.as_bytes())?;
    let after_cursor = cursor
        .as_deref()
        .map(hex::decode)
        .transpose()
        .map_err(|_| Status::invalid_argument("Invalid admin audit cursor"))?;
    let page = admin_audit::list_audit_event_page_after(
        &state.storage,
        AuditEventFilter {
            principal_id: none_if_empty(&req.principal_id),
            resource_id: none_if_empty(&req.resource_id),
            action: none_if_empty(&req.action),
        },
        after_cursor.as_deref(),
        limit,
    )
    .await
    .map_err(|err| Status::internal(err.to_string()))?;
    if page.revision != revision {
        return Err(Status::aborted(
            "Admin audit collection changed while reading this page",
        ));
    }
    let next_cursor = page.next_cursor.map_or(Ok(String::new()), |cursor| {
        admin_cursor::encode_next_cursor(
            &hex::encode(cursor),
            &binding,
            state.config.jwt_secret.as_bytes(),
        )
    })?;

    Ok(Response::new(AuditEventsResponse {
        request_id,
        page: Some(PageResponse {
            next_page_token: next_cursor,
        }),
        events: page.events.into_iter().map(audit_event_to_proto).collect(),
        data_source: "admin_audit_log".to_string(),
    }))
}

pub(super) async fn list_storage_classes(
    state: &AppState,
    request: Request<ListStorageClassesRequest>,
) -> Result<Response<ListStorageClassesResponse>, Status> {
    let principal = require_admin(&request, state, SystemAdminRelation::ViewSystem).await?;
    let req = request.into_inner();
    let request_id = require_request_id(&req.request_id)?.to_string();
    let catalog = state.core_store.storage_class_catalog();
    let storage_classes = state
        .core_store
        .list_storage_classes()
        .into_iter()
        .filter(|class| req.include_operator_only || class.tenant_selectable)
        .map(|class| storage_class_to_proto(&class, &catalog.default_class_id))
        .collect::<Vec<_>>();
    let include_operator_only = req.include_operator_only.to_string();
    let filters = [("include_operator_only", include_operator_only.as_str())];
    let principal_scope = format!(
        "admin:{}/tenant:{}",
        principal.principal_id, principal.tenant_id
    );
    let (storage_classes, page) = crate::services::collection_cursor::paginate(
        storage_classes,
        req.page.as_ref(),
        "anvil.AdminService/ListStorageClasses",
        &filters,
        &principal_scope,
        "class_id.asc",
        state.config.jwt_secret.as_bytes(),
        |class| class.class_id.as_str(),
        |class| {
            crate::services::collection_cursor::content_generation(&[
                class.description.as_bytes(),
                class.metadata_profile_id.as_bytes(),
                class.byte_profile_id.as_bytes(),
            ])
        },
    )?;
    Ok(Response::new(ListStorageClassesResponse {
        request_id,
        storage_classes,
        default_class_id: catalog.default_class_id.clone(),
        page: Some(page),
    }))
}

pub(super) async fn get_storage_class(
    state: &AppState,
    request: Request<GetStorageClassRequest>,
) -> Result<Response<StorageClassResponse>, Status> {
    let _principal = require_admin(&request, state, SystemAdminRelation::ViewSystem).await?;
    let req = request.into_inner();
    let request_id = require_request_id(&req.request_id)?.to_string();
    let class = state
        .core_store
        .get_storage_class(&req.class_id)
        .map_err(|err| Status::not_found(err.to_string()))?;
    Ok(Response::new(StorageClassResponse {
        request_id,
        storage_class: Some(storage_class_to_proto(
            &class,
            &state.core_store.storage_class_catalog().default_class_id,
        )),
    }))
}
