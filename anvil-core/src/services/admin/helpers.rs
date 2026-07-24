use super::*;

pub(super) async fn require_admin<T>(
    request: &Request<T>,
    state: &AppState,
    capability: SystemAdminRelation,
) -> Result<AdminPrincipal, Status> {
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .ok_or_else(|| Status::unauthenticated("Missing authenticated admin principal"))?;
    let allowed = crate::system_realm::check_admin_relation(
        &state.storage,
        &state.config.mesh_id,
        claims,
        capability,
    )
    .await
    .map_err(|err| Status::internal(err.to_string()))?;
    if !allowed {
        return Err(Status::permission_denied(format!(
            "Missing system realm admin relation {}",
            capability.as_str()
        )));
    }
    let mut principal = AdminPrincipal::from(claims);
    principal.checked_relation = Some(capability);
    principal.checked_object = Some(format!(
        "{}:{}",
        crate::system_realm::system_namespace(),
        crate::system_realm::system_mesh_object_id(&state.config.mesh_id)
    ));
    Ok(principal)
}

pub(super) fn require_mutation_context(
    context: Option<&AdminRequestContext>,
    create: bool,
) -> Result<&AdminRequestContext, Status> {
    let context = context.ok_or_else(|| Status::invalid_argument("Missing admin context"))?;
    if context.request_id.trim().is_empty() {
        return Err(Status::invalid_argument("Admin request_id is required"));
    }
    if context.idempotency_key.trim().is_empty() {
        return Err(Status::invalid_argument(
            "Admin idempotency_key is required",
        ));
    }
    if context.audit_reason.trim().is_empty() {
        return Err(Status::invalid_argument("Admin audit_reason is required"));
    }
    if create && context.expected_generation != 0 {
        return Err(Status::invalid_argument(
            "Create requests must use expected_generation = 0",
        ));
    }
    if !create && context.expected_generation == 0 {
        return Err(Status::invalid_argument(
            "Update requests must include expected_generation",
        ));
    }
    Ok(context)
}

pub(super) fn require_admin_action_context(
    context: Option<&AdminRequestContext>,
) -> Result<&AdminRequestContext, Status> {
    let context = context.ok_or_else(|| Status::invalid_argument("Missing admin context"))?;
    require_request_id(&context.request_id)?;
    if context.idempotency_key.trim().is_empty() {
        return Err(Status::invalid_argument(
            "Admin idempotency_key is required",
        ));
    }
    if context.audit_reason.trim().is_empty() {
        return Err(Status::invalid_argument("Admin audit_reason is required"));
    }
    Ok(context)
}

pub(super) fn require_request_id(request_id: &str) -> Result<&str, Status> {
    let request_id = request_id.trim();
    if request_id.is_empty() {
        return Err(Status::invalid_argument("Admin request_id is required"));
    }
    Ok(request_id)
}

pub(super) async fn run_index_repair(
    state: &AppState,
    request_id: &str,
    audit_event_id: &str,
    req: &RunRepairRequest,
) -> Result<RepairTaskResponse, Status> {
    let tenant_id = resolve_tenant_id(state, &req.tenant_id).await?;
    require_nonempty_admin_field(&req.bucket_name, "bucket_name")?;
    require_nonempty_admin_field(&req.index_name, "index_name")?;
    let bucket = state
        .persistence
        .get_bucket_by_name(tenant_id, &req.bucket_name)
        .await
        .map_err(|err| Status::internal(err.to_string()))?
        .ok_or_else(|| Status::not_found("Bucket not found"))?;
    let report = state
        .persistence
        .repair_index_from_base_journal(tenant_id, &bucket.name, &req.index_name, req.rebuild)
        .await
        .map_err(|err| Status::failed_precondition(err.to_string()))?;
    let (source_cursor_low, source_cursor_high) = split_u128_admin(report.source_cursor);
    let findings = report
        .finding
        .as_ref()
        .map(repair_finding_to_admin_proto)
        .transpose()?
        .into_iter()
        .collect::<Vec<_>>();
    let repair_task_id = findings
        .first()
        .map(|finding| finding.repair_task_id.clone())
        .unwrap_or_default();

    Ok(RepairTaskResponse {
        request_id: request_id.to_string(),
        repair_task_id,
        status: index_repair::status_name(&report.status).to_string(),
        scope_kind: "index".to_string(),
        scope_id: format!(
            "tenant-{tenant_id}-bucket-{}-index-{}",
            bucket.id, report.index_name
        ),
        findings,
        audit_event_id: audit_event_id.to_string(),
        details_json: json!({
            "repair_kind": "index",
            "bucket_name": report.bucket_name,
            "index_name": report.index_name,
            "index_storage_id": report.index_storage_id,
            "source_cursor_low": source_cursor_low,
            "source_cursor_high": source_cursor_high,
            "reason": index_repair::status_reason(&report.status),
            "rebuilt": report.build.is_some(),
        })
        .to_string(),
    })
}

pub(super) async fn run_directory_index_repair(
    state: &AppState,
    request_id: &str,
    audit_event_id: &str,
    req: &RunRepairRequest,
) -> Result<RepairTaskResponse, Status> {
    let tenant_id = resolve_tenant_id(state, &req.tenant_id).await?;
    require_nonempty_admin_field(&req.bucket_name, "bucket_name")?;
    let bucket = state
        .persistence
        .get_bucket_by_name(tenant_id, &req.bucket_name)
        .await
        .map_err(|err| Status::internal(err.to_string()))?
        .ok_or_else(|| Status::not_found("Bucket not found"))?;
    let report = state
        .persistence
        .repair_directory_index(tenant_id, &bucket.name, req.rebuild)
        .await
        .map_err(|err| Status::failed_precondition(err.to_string()))?;
    let (source_cursor_low, source_cursor_high) = split_u128_admin(report.source_cursor);
    let findings = report
        .finding
        .as_ref()
        .map(repair_finding_to_admin_proto)
        .transpose()?
        .into_iter()
        .collect::<Vec<_>>();
    let repair_task_id = findings
        .first()
        .map(|finding| finding.repair_task_id.clone())
        .unwrap_or_default();
    let actual = report.actual.as_ref();

    Ok(RepairTaskResponse {
        request_id: request_id.to_string(),
        repair_task_id,
        status: directory_repair::status_name(&report.status).to_string(),
        scope_kind: "bucket".to_string(),
        scope_id: format!("tenant-{tenant_id}-bucket-{}", bucket.id),
        findings,
        audit_event_id: audit_event_id.to_string(),
        details_json: json!({
            "repair_kind": "directory_index",
            "bucket_name": report.bucket_name,
            "source_cursor_low": source_cursor_low,
            "source_cursor_high": source_cursor_high,
            "expected_entry_count": report.expected.entry_count,
            "actual_entry_count": actual.map(|snapshot| snapshot.entry_count).unwrap_or_default(),
            "expected_snapshot_hash": report.expected.snapshot_hash,
            "actual_snapshot_hash": actual.map(|snapshot| snapshot.snapshot_hash.clone()).unwrap_or_default(),
            "reason": directory_repair::status_reason(&report.status),
            "rebuilt_manifest_hash": report
                .rebuilt
                .as_ref()
                .map(|rebuilt| rebuilt.manifest_hash.clone())
                .unwrap_or_default(),
        })
        .to_string(),
    })
}

pub(super) async fn run_authz_derived_index_repair(
    state: &AppState,
    request_id: &str,
    audit_event_id: &str,
    req: &RunRepairRequest,
) -> Result<RepairTaskResponse, Status> {
    let tenant_id = resolve_tenant_id(state, &req.tenant_id).await?;
    require_nonempty_admin_field(&req.derived_index_id, "derived_index_id")?;
    let report = state
        .persistence
        .repair_authz_derived_userset_index(tenant_id, &req.derived_index_id, req.rebuild)
        .await
        .map_err(|err| Status::failed_precondition(err.to_string()))?;
    let findings = report
        .finding
        .as_ref()
        .map(repair_finding_to_admin_proto)
        .transpose()?
        .into_iter()
        .collect::<Vec<_>>();
    let repair_task_id = findings
        .first()
        .map(|finding| finding.repair_task_id.clone())
        .unwrap_or_default();

    Ok(RepairTaskResponse {
        request_id: request_id.to_string(),
        repair_task_id,
        status: authz_repair::status_name(&report.status).to_string(),
        scope_kind: "authz_derived_index".to_string(),
        scope_id: format!("tenant-{tenant_id}-authz-{}", report.derived_index_id),
        findings,
        audit_event_id: audit_event_id.to_string(),
        details_json: json!({
            "repair_kind": "authz_derived_index",
            "derived_index_id": report.derived_index_id,
            "processed_revision": report.processed_revision,
            "latest_revision": report.latest_revision,
            "source_records_hash": report.source_records_hash,
            "reason": authz_repair::status_reason(&report.status),
            "rebuilt": report.rebuilt_index.is_some(),
        })
        .to_string(),
    })
}

pub(super) async fn run_personaldb_log_chain_repair(
    state: &AppState,
    request_id: &str,
    audit_event_id: &str,
    req: &RunRepairRequest,
) -> Result<RepairTaskResponse, Status> {
    let tenant_id = resolve_tenant_id(state, &req.tenant_id).await?;
    require_nonempty_admin_field(&req.database_id, "database_id")?;
    let report = state
        .persistence
        .repair_personaldb_log_chain(
            tenant_id,
            &req.database_id,
            state.personaldb_protocol_keyring.trust_store(),
        )
        .await
        .map_err(|err| Status::failed_precondition(err.to_string()))?;
    let findings = report
        .finding
        .as_ref()
        .map(repair_finding_to_admin_proto)
        .transpose()?
        .into_iter()
        .collect::<Vec<_>>();
    let repair_task_id = findings
        .first()
        .map(|finding| finding.repair_task_id.clone())
        .unwrap_or_default();

    Ok(RepairTaskResponse {
        request_id: request_id.to_string(),
        repair_task_id,
        status: personaldb_repair::status_name(&report.status).to_string(),
        scope_kind: "personaldb".to_string(),
        scope_id: format!("tenant-{tenant_id}-database-{}", report.database_id),
        findings,
        audit_event_id: audit_event_id.to_string(),
        details_json: json!({
            "repair_kind": "personaldb_log_chain",
            "database_id": report.database_id,
            "committed_log_index": report.committed_log_index,
            "verified_log_index": report.verified_log_index,
            "committed_log_hash": report.committed_log_hash,
            "reason": personaldb_repair::status_reason(&report.status),
        })
        .to_string(),
    })
}

pub(super) async fn run_mesh_routing_projection_repair(
    state: &AppState,
    request_id: &str,
    audit_event_id: &str,
) -> Result<RepairTaskResponse, Status> {
    let diagnostics = state
        .persistence
        .diagnose_mesh_routing_projection(None)
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
    let mut repaired_records = Vec::new();
    let mut skipped_records = Vec::new();
    let mut findings = Vec::new();
    let repair_task_id = format!("mesh-routing-projection-repair-{audit_event_id}");

    for (index, diagnostic) in diagnostics.into_iter().enumerate() {
        if !diagnostic.repair_safe
            || diagnostic.proposed_action != "repair_routing_record_from_control_stream"
            || diagnostic.record_key.trim().is_empty()
        {
            let repair_result = json!({
                "applied_action": "skipped",
                "code": diagnostic.code,
                "reason": "diagnostic is not safe for automatic routing projection repair",
            });
            let evidence = mesh_routing_projection_evidence(&diagnostic, Some(repair_result));
            skipped_records.push(evidence.clone());
            findings.push(mesh_routing_projection_repair_finding_record(
                &repair_task_id,
                index,
                &diagnostic,
                "RequiresOperatorReview",
                "ManualReview",
                &evidence,
            ));
            continue;
        }
        let Some(family) =
            mesh_directory::RoutingRecordFamily::from_stream_family(&diagnostic.stream_family)
        else {
            let repair_result = json!({
                "applied_action": "skipped",
                "code": diagnostic.code,
                "reason": "unknown routing record stream family",
            });
            let evidence = mesh_routing_projection_evidence(&diagnostic, Some(repair_result));
            skipped_records.push(evidence.clone());
            findings.push(mesh_routing_projection_repair_finding_record(
                &repair_task_id,
                index,
                &diagnostic,
                "RequiresOperatorReview",
                "ManualReview",
                &evidence,
            ));
            continue;
        };
        match state
            .persistence
            .repair_mesh_routing_record(family, &diagnostic.record_key)
            .await
        {
            Ok(record) => {
                let repair_result = json!({
                    "applied_action": "repair_routing_record_from_control_stream",
                    "descriptor_key": record.descriptor_key,
                    "generation": record.generation,
                });
                let evidence = mesh_routing_projection_evidence(&diagnostic, Some(repair_result));
                repaired_records.push(evidence.clone());
                findings.push(mesh_routing_projection_repair_finding_record(
                    &repair_task_id,
                    index,
                    &diagnostic,
                    "RebuiltDerivedIndex",
                    "RebuildDerivedIndex",
                    &evidence,
                ));
            }
            Err(err) => {
                let repair_result = json!({
                    "applied_action": "skipped",
                    "code": diagnostic.code,
                    "reason": err.to_string(),
                });
                let evidence = mesh_routing_projection_evidence(&diagnostic, Some(repair_result));
                skipped_records.push(evidence.clone());
                findings.push(mesh_routing_projection_repair_finding_record(
                    &repair_task_id,
                    index,
                    &diagnostic,
                    "RequiresOperatorReview",
                    "ManualReview",
                    &evidence,
                ));
            }
        }
    }

    let status = if skipped_records.is_empty() {
        "completed"
    } else if repaired_records.is_empty() {
        "failed"
    } else {
        "completed_with_warnings"
    };

    Ok(RepairTaskResponse {
        request_id: request_id.to_string(),
        repair_task_id,
        status: status.to_string(),
        scope_kind: "mesh_routing_projection".to_string(),
        scope_id: state.config.mesh_id.clone(),
        findings,
        audit_event_id: audit_event_id.to_string(),
        details_json: json!({
            "repair_kind": "mesh_routing_projection",
            "repaired_count": repaired_records.len(),
            "skipped_count": skipped_records.len(),
            "repaired_records": repaired_records,
            "skipped_records": skipped_records,
        })
        .to_string(),
    })
}

pub(super) fn require_nonempty_admin_field(value: &str, field: &'static str) -> Result<(), Status> {
    if value.trim().is_empty() {
        return Err(Status::invalid_argument(format!("{field} is required")));
    }
    Ok(())
}

pub(super) fn validate_diagnostic_severity(value: &str) -> Result<(), Status> {
    match value {
        "info" | "warning" | "error" => Ok(()),
        _ => Err(Status::invalid_argument("Invalid diagnostic severity")),
    }
}

pub(super) fn index_diagnostic_to_admin_record(
    diagnostic: persistence::IndexDiagnostic,
) -> Result<DiagnosticRecord, Status> {
    let cursor =
        u64::try_from(diagnostic.id).map_err(|_| Status::internal("Invalid diagnostic cursor"))?;
    Ok(DiagnosticRecord {
        diagnostic_id: format!("index-diagnostic-{cursor}"),
        scope_kind: "index".to_string(),
        scope_id: diagnostic
            .index_id
            .map(|index_id| {
                format!(
                    "tenant-{}-bucket-{}-index-{}",
                    diagnostic.tenant_id, diagnostic.bucket_id, index_id
                )
            })
            .unwrap_or_else(|| {
                format!(
                    "tenant-{}-bucket-{}-index-{}",
                    diagnostic.tenant_id, diagnostic.bucket_id, diagnostic.index_name
                )
            }),
        source: "index_diagnostic_journal".to_string(),
        severity: diagnostic.severity,
        code: diagnostic.code,
        message: diagnostic.message,
        object_key: diagnostic.object_key,
        version_id: diagnostic
            .version_id
            .map(|version_id| version_id.to_string())
            .unwrap_or_default(),
        details_json: diagnostic.details.to_string(),
        created_at_nanos: diagnostic
            .created_at
            .timestamp_nanos_opt()
            .ok_or_else(|| Status::internal("Invalid diagnostic timestamp"))?,
        cursor,
    })
}

pub(super) async fn mesh_routing_projection_diagnostics(
    state: &AppState,
) -> Result<Vec<DiagnosticRecord>, Status> {
    state
        .persistence
        .diagnose_mesh_routing_projection(None)
        .await
        .map_err(|err| Status::internal(err.to_string()))?
        .into_iter()
        .enumerate()
        .map(|(index, diagnostic)| {
            mesh_routing_projection_diagnostic_to_admin_record(
                u64::try_from(index + 1)
                    .map_err(|_| Status::internal("Too many mesh diagnostics"))?,
                diagnostic,
            )
        })
        .collect()
}

pub(super) fn mesh_routing_projection_diagnostic_to_admin_record(
    cursor: u64,
    diagnostic: mesh_control_stream::ControlProjectionDiagnostic,
) -> Result<DiagnosticRecord, Status> {
    let details = mesh_routing_projection_evidence(&diagnostic, None);
    let scope_id = format!(
        "{}/{}/{}",
        diagnostic.stream_family, diagnostic.partition, diagnostic.record_key
    );
    Ok(DiagnosticRecord {
        diagnostic_id: format!("mesh-routing-projection-{cursor}"),
        scope_kind: "routing_record".to_string(),
        scope_id,
        source: "mesh_routing_projection".to_string(),
        severity: diagnostic.severity.to_string(),
        code: diagnostic.code.to_string(),
        message: diagnostic.message,
        object_key: String::new(),
        version_id: String::new(),
        details_json: details.to_string(),
        created_at_nanos: Utc::now().timestamp_nanos_opt().unwrap_or_default(),
        cursor,
    })
}

pub(super) fn mesh_routing_projection_evidence(
    diagnostic: &mesh_control_stream::ControlProjectionDiagnostic,
    repair_result: Option<serde_json::Value>,
) -> serde_json::Value {
    let family = mesh_directory::RoutingRecordFamily::from_stream_family(&diagnostic.stream_family);
    let descriptor_key = family
        .filter(|_| !diagnostic.record_key.trim().is_empty())
        .and_then(|family| {
            mesh_directory::routing_record_descriptor_key_for_key(family, &diagnostic.record_key)
                .ok()
        });
    let expected_partition = family
        .filter(|_| !diagnostic.record_key.trim().is_empty())
        .and_then(|family| {
            mesh_directory::routing_record_partition_for_key(family, &diagnostic.record_key).ok()
        });

    json!({
        "stream_family": &diagnostic.stream_family,
        "partition": &diagnostic.partition,
        "expected_partition": expected_partition,
        "record_key": &diagnostic.record_key,
        "descriptor_key": descriptor_key,
        "stream_sequence": diagnostic.stream_sequence,
        "stream_generation": diagnostic.stream_generation,
        "stream_digest": diagnostic.stream_digest.as_deref(),
        "projection_generation": diagnostic.projection_generation,
        "projection_digest": diagnostic.projection_digest.as_deref(),
        "repair_safe": diagnostic.repair_safe,
        "proposed_action": diagnostic.proposed_action,
        "repair_result": repair_result,
    })
}

pub(super) fn mesh_routing_projection_repair_finding_record(
    repair_task_id: &str,
    index: usize,
    diagnostic: &mesh_control_stream::ControlProjectionDiagnostic,
    status: &str,
    proposed_action: &str,
    evidence: &serde_json::Value,
) -> RepairFindingRecord {
    let subject_id = format!(
        "{}/{}/{}",
        diagnostic.stream_family, diagnostic.partition, diagnostic.record_key
    );
    let evidence_json = evidence.to_string();
    RepairFindingRecord {
        finding_id: format!("{repair_task_id}-finding-{:04}", index + 1),
        scope_kind: "mesh_routing_projection".to_string(),
        scope_id: subject_id.clone(),
        repair_task_id: repair_task_id.to_string(),
        lease_fence_token: 0,
        severity: diagnostic.severity.to_string(),
        status: status.to_string(),
        code: diagnostic.code.to_string(),
        message: diagnostic.message.clone(),
        subjects: vec![RepairSubjectRecord {
            subject_kind: "mesh_control_stream_record".to_string(),
            subject_id,
            generation: diagnostic.stream_generation.unwrap_or_default(),
            has_generation: diagnostic.stream_generation.is_some(),
            cursor_low: diagnostic.stream_sequence.unwrap_or_default(),
            cursor_high: 0,
            has_cursor: diagnostic.stream_sequence.is_some(),
            expected_hash: diagnostic.stream_digest.clone().unwrap_or_default(),
            actual_hash: diagnostic.projection_digest.clone().unwrap_or_default(),
        }],
        proposed_action: proposed_action.to_string(),
        evidence_json,
        created_at_nanos: Utc::now().timestamp_nanos_opt().unwrap_or_default(),
        finding_hash: hex::encode(blake3::hash(evidence.to_string().as_bytes()).as_bytes()),
    }
}

pub(super) fn diagnostic_position(diagnostic: &DiagnosticRecord) -> String {
    format!(
        "{}:{:020}:{}",
        diagnostic.source, diagnostic.cursor, diagnostic.diagnostic_id
    )
}

pub(super) async fn mesh_lifecycle_diagnostics(
    state: &AppState,
) -> Result<Vec<DiagnosticRecord>, Status> {
    let mut diagnostics = Vec::new();
    let mut cursor = 1_u64;
    let mut push = |scope_kind: &str,
                    scope_id: String,
                    severity: &str,
                    code: &str,
                    message: String,
                    details: serde_json::Value| {
        diagnostics.push(DiagnosticRecord {
            diagnostic_id: format!("mesh-diagnostic-{cursor}"),
            scope_kind: scope_kind.to_string(),
            scope_id,
            source: "mesh_lifecycle".to_string(),
            severity: severity.to_string(),
            code: code.to_string(),
            message,
            object_key: String::new(),
            version_id: String::new(),
            details_json: details.to_string(),
            created_at_nanos: Utc::now().timestamp_nanos_opt().unwrap_or_default(),
            cursor,
        });
        cursor = cursor.saturating_add(1);
    };

    for region in state
        .persistence
        .list_region_descriptors()
        .await
        .map_err(lifecycle_status)?
    {
        if region.state != CoreLifecycleState::Active {
            push(
                "region",
                region.region.clone(),
                match region.state {
                    CoreLifecycleState::Draining
                    | CoreLifecycleState::Drained
                    | CoreLifecycleState::DrainedWithExceptions
                    | CoreLifecycleState::Offline
                    | CoreLifecycleState::Removed => "warning",
                    _ => "info",
                },
                "mesh_region_not_active",
                format!(
                    "region {} is {:?}; new writable placement is disabled",
                    region.region, region.state
                ),
                json!({
                    "generation": region.generation,
                    "state": format!("{:?}", region.state),
                    "default_cell": region.default_cell,
                }),
            );
        }
    }

    for cell in state
        .persistence
        .list_cell_descriptors(None)
        .await
        .map_err(lifecycle_status)?
    {
        if cell.state != CoreLifecycleState::Active {
            push(
                "cell",
                format!("{}/{}", cell.region, cell.cell_id),
                "info",
                "mesh_cell_not_active",
                format!(
                    "cell {}/{} is {:?}; node activation and placement are disabled",
                    cell.region, cell.cell_id, cell.state
                ),
                json!({
                    "generation": cell.generation,
                    "state": format!("{:?}", cell.state),
                    "placement_weight": cell.placement_weight,
                    "failure_domain": cell.failure_domain,
                }),
            );
        }
    }

    for node in state
        .persistence
        .list_node_descriptors(None, None)
        .await
        .map_err(lifecycle_status)?
    {
        if node.state != CoreLifecycleState::Active {
            let runtime_ownership_blockers = match node.state {
                CoreLifecycleState::Draining
                | CoreLifecycleState::Drained
                | CoreLifecycleState::Offline
                | CoreLifecycleState::Removed => state
                    .persistence
                    .node_runtime_ownership_blockers(&node.node_id)
                    .await
                    .map_err(|err| Status::internal(err.to_string()))?,
                _ => Vec::new(),
            };
            let proposed_action = if runtime_ownership_blockers.is_empty() {
                "no_runtime_ownership_repair_needed"
            } else {
                "force_offline_node_to_expire_runtime_ownership"
            };
            push(
                "node",
                node.node_id.clone(),
                match node.state {
                    CoreLifecycleState::Draining
                    | CoreLifecycleState::Drained
                    | CoreLifecycleState::Offline
                    | CoreLifecycleState::Removed => "warning",
                    _ => "info",
                },
                "mesh_node_not_active",
                format!(
                    "node {} is {:?}; new ownership should not be assigned",
                    node.node_id, node.state
                ),
                json!({
                    "generation": node.generation,
                    "state": format!("{:?}", node.state),
                    "region": node.region,
                    "cell_id": node.cell_id,
                    "capacity_json_hash": node.capacity_json_hash,
                    "drain": node.drain,
                    "runtime_ownership_blocker_count": runtime_ownership_blockers.len(),
                    "runtime_ownership_blockers": runtime_ownership_blockers,
                    "ownership_repair": {
                        "node_id": node.node_id,
                        "owner_region": node.region,
                        "owner_cell": node.cell_id,
                        "proposed_action": proposed_action,
                    },
                }),
            );
        }
    }

    let routing_record_count = state
        .persistence
        .list_mesh_routing_records(None)
        .await
        .map_err(|err| Status::internal(err.to_string()))?
        .len();
    push(
        "mesh",
        state.config.mesh_id.clone(),
        "info",
        "mesh_routing_projection_summary",
        format!("mesh routing projection has {routing_record_count} records"),
        json!({ "routing_record_count": routing_record_count }),
    );

    Ok(diagnostics)
}

pub(super) fn repair_finding_to_admin_proto(
    finding: &RepairFinding,
) -> Result<RepairFindingRecord, Status> {
    Ok(RepairFindingRecord {
        finding_id: finding.finding_id.clone(),
        scope_kind: finding.scope_kind.clone(),
        scope_id: finding.scope_id.clone(),
        repair_task_id: finding.repair_task_id.clone(),
        lease_fence_token: finding.lease_fence_token,
        severity: format!("{:?}", finding.severity),
        status: format!("{:?}", finding.status),
        code: finding.code.clone(),
        message: finding.message.clone(),
        subjects: finding
            .subjects
            .iter()
            .map(repair_subject_to_admin_proto)
            .collect(),
        proposed_action: format!("{:?}", finding.proposed_action),
        evidence_json: serde_json::to_string(&finding.evidence).unwrap_or_default(),
        created_at_nanos: finding.created_at_nanos,
        finding_hash: finding.finding_hash.clone().unwrap_or_default(),
    })
}

pub(super) fn repair_subject_to_admin_proto(subject: &RepairSubjectRef) -> RepairSubjectRecord {
    let (cursor_low, cursor_high) = subject.cursor.map(split_u128_admin).unwrap_or((0, 0));
    RepairSubjectRecord {
        subject_kind: subject.subject_kind.clone(),
        subject_id: subject.subject_id.clone(),
        generation: subject.generation.unwrap_or_default(),
        has_generation: subject.generation.is_some(),
        cursor_low,
        cursor_high,
        has_cursor: subject.cursor.is_some(),
        expected_hash: subject.expected_hash.clone().unwrap_or_default(),
        actual_hash: subject.actual_hash.clone().unwrap_or_default(),
    }
}

pub(super) fn split_u128_admin(value: u128) -> (u64, u64) {
    (value as u64, (value >> 64) as u64)
}

pub(super) fn audit_event_id(principal: &AdminPrincipal, context: &AdminRequestContext) -> String {
    format!("audit:{}:{}", principal.principal_id, context.request_id)
}

pub(super) fn audit_event_id_with_suffix(
    principal: &AdminPrincipal,
    context: &AdminRequestContext,
    suffix: &str,
) -> String {
    format!(
        "audit:{}:{}:{}",
        principal.principal_id,
        context.request_id,
        safe_audit_id_suffix(suffix)
    )
}

pub(super) async fn record_admin_audit_event(
    state: &AppState,
    principal: &AdminPrincipal,
    context: &AdminRequestContext,
    action: &str,
    resource_id: &str,
    details: serde_json::Value,
) -> Result<String, Status> {
    let audit_event_id = audit_event_id(principal, context);
    let event = AdminAuditEvent {
        schema: admin_audit::ADMIN_AUDIT_EVENT_SCHEMA.to_string(),
        audit_event_id: audit_event_id.clone(),
        request_id: context.request_id.clone(),
        principal_id: principal.principal_id.clone(),
        resource_id: resource_id.to_string(),
        action: action.to_string(),
        audit_reason: context.audit_reason.clone(),
        created_at: Utc::now().to_rfc3339(),
        details_json: admin_audit_details_json(principal, context, details)?,
    };
    admin_audit::append_audit_event(&state.storage, &event)
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
    Ok(audit_event_id)
}

pub(super) async fn record_admin_audit_event_with_suffix(
    state: &AppState,
    principal: &AdminPrincipal,
    context: &AdminRequestContext,
    action: &str,
    resource_id: &str,
    details: serde_json::Value,
    suffix: &str,
) -> Result<String, Status> {
    let audit_event_id = audit_event_id_with_suffix(principal, context, suffix);
    let event = AdminAuditEvent {
        schema: admin_audit::ADMIN_AUDIT_EVENT_SCHEMA.to_string(),
        audit_event_id: audit_event_id.clone(),
        request_id: context.request_id.clone(),
        principal_id: principal.principal_id.clone(),
        resource_id: resource_id.to_string(),
        action: action.to_string(),
        audit_reason: context.audit_reason.clone(),
        created_at: Utc::now().to_rfc3339(),
        details_json: admin_audit_details_json(principal, context, details)?,
    };
    admin_audit::append_audit_event(&state.storage, &event)
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
    Ok(audit_event_id)
}

pub(super) fn safe_audit_id_suffix(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect()
}

pub(super) fn admin_audit_details_json(
    principal: &AdminPrincipal,
    context: &AdminRequestContext,
    details: serde_json::Value,
) -> Result<String, Status> {
    let relation = principal
        .checked_relation
        .ok_or_else(|| Status::internal("Admin audit missing Zanzibar relation"))?;
    let object = principal
        .checked_object
        .as_ref()
        .ok_or_else(|| Status::internal("Admin audit missing Zanzibar object"))?;
    let mut details = match details {
        serde_json::Value::Object(map) => map,
        other => {
            let mut map = serde_json::Map::new();
            map.insert("details".to_string(), other);
            map
        }
    };
    details.insert(
        "idempotency_key".to_string(),
        json!(context.idempotency_key.clone()),
    );
    details.insert(
        "expected_generation".to_string(),
        json!(context.expected_generation),
    );
    details.insert("authorised_relation".to_string(), json!(relation.as_str()));
    details.insert("authorised_object".to_string(), json!(object));
    serde_json::to_string(&serde_json::Value::Object(details))
        .map_err(|_| Status::internal("Failed to encode admin audit details"))
}

pub(super) fn bucket_resource_id(tenant_id: i64, bucket_name: &str) -> String {
    format!("tenant:{tenant_id}:bucket:{bucket_name}")
}

pub(super) fn app_resource_id(tenant_id: i64, app_name: &str) -> String {
    format!("tenant:{tenant_id}:app:{app_name}")
}

pub(super) fn validate_policy_parts(action: &str, resource: &str) -> Result<(), Status> {
    let action = action.trim();
    let resource = resource.trim();
    if action.is_empty() {
        return Err(Status::invalid_argument("policy action is required"));
    }
    if resource.is_empty() {
        return Err(Status::invalid_argument("policy resource is required"));
    }
    if action == "*" || action.ends_with(":*") || resource == "*" {
        return Err(Status::permission_denied(
            "Admin policy grants must name explicit Zanzibar-backed actions and resources",
        ));
    }
    Ok(())
}

pub(super) fn parse_application_policy_batch(
    policies: &[ApplicationPolicyMutation],
) -> Result<Vec<(crate::permissions::AnvilAction, String)>, Status> {
    if policies.is_empty() {
        return Err(Status::invalid_argument(
            "At least one application policy is required",
        ));
    }
    if policies.len() > 256 {
        return Err(Status::invalid_argument(
            "Application policy batches are limited to 256 entries",
        ));
    }

    let mut seen = std::collections::BTreeSet::new();
    let mut parsed = Vec::with_capacity(policies.len());
    for policy in policies {
        validate_policy_parts(&policy.action, &policy.resource)?;
        if !seen.insert((policy.action.clone(), policy.resource.clone())) {
            return Err(Status::invalid_argument(
                "Application policy batches must not contain duplicates",
            ));
        }
        let action = policy
            .action
            .parse::<crate::permissions::AnvilAction>()
            .map_err(|_| Status::invalid_argument("Invalid delegated action"))?;
        parsed.push((action, policy.resource.clone()));
    }
    Ok(parsed)
}

pub(super) async fn mutate_application_policy_batch(
    state: &AppState,
    request: Request<ApplicationPoliciesRequest>,
    operation: &'static str,
    audit_action: &'static str,
    reason: &'static str,
) -> Result<Response<ApplicationPoliciesResponse>, Status> {
    let principal = require_admin(&request, state, SystemAdminRelation::ManagePolicies).await?;
    let req = request.into_inner();
    let context = require_admin_action_context(req.context.as_ref())?;
    let tenant_id = resolve_tenant_id(state, &req.tenant_id).await?;
    let app = resolve_tenant_app(state, tenant_id, &req.app_name).await?;
    let parsed = parse_application_policy_batch(&req.policies)?;
    crate::access_control::write_delegated_action_tuple_batch(
        &state.storage,
        &state.persistence,
        tenant_id,
        &app.id.to_string(),
        &parsed,
        operation,
        &principal.principal_id,
        reason,
    )
    .await?;
    let policy_details = req
        .policies
        .iter()
        .map(|policy| {
            json!({
                "action": policy.action,
                "resource": policy.resource,
            })
        })
        .collect::<Vec<_>>();
    let audit_event_id = record_admin_audit_event(
        state,
        &principal,
        context,
        audit_action,
        &app_resource_id(tenant_id, &app.name),
        json!({
            "resource_kind": "application_policy_batch",
            "tenant_id": tenant_id,
            "app_id": app.id,
            "app_name": &app.name,
            "client_id": &app.client_id,
            "policies": policy_details,
        }),
    )
    .await?;
    Ok(Response::new(ApplicationPoliciesResponse {
        request_id: context.request_id.clone(),
        tenant_id: tenant_id.to_string(),
        app_name: app.name,
        policies: req.policies,
        audit_event_id,
    }))
}

#[derive(Default)]
pub(super) struct SecretEncryptionRotationStats {
    pub(super) app_secrets_examined: u64,
    pub(super) app_secrets_rotated: u64,
    pub(super) hf_keys_examined: u64,
    pub(super) hf_keys_rotated: u64,
    pub(super) already_active: u64,
}

pub(super) async fn rotate_application_secret_envelopes(
    state: &AppState,
    dry_run: bool,
    stats: &mut SecretEncryptionRotationStats,
) -> Result<(), Status> {
    const ROTATION_PAGE_SIZE: usize = 256;
    let mut tenant_cursor = None;
    loop {
        let revision = state
            .persistence
            .current_control_collection_revision()
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        let tenant_page = state
            .persistence
            .page_tenants(&revision, tenant_cursor.as_deref(), ROTATION_PAGE_SIZE)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        for tenant in tenant_page.tenants {
            let mut app_cursor = None;
            loop {
                let revision = state
                    .persistence
                    .current_control_collection_revision()
                    .await
                    .map_err(|err| Status::internal(err.to_string()))?;
                let app_page = state
                    .persistence
                    .page_apps_for_tenant(
                        tenant.id,
                        &revision,
                        app_cursor.as_deref(),
                        ROTATION_PAGE_SIZE,
                    )
                    .await
                    .map_err(|err| Status::internal(err.to_string()))?;
                for app in app_page.apps {
                    let Some(details) = state
                        .persistence
                        .get_app_by_client_id(&app.client_id)
                        .await
                        .map_err(|err| Status::internal(err.to_string()))?
                    else {
                        continue;
                    };
                    stats.app_secrets_examined += 1;
                    match state
                        .secret_keyring
                        .reencrypt_if_needed(&details.client_secret_encrypted)
                        .map_err(|err| Status::internal(err.to_string()))?
                    {
                        Some(rotated) => {
                            stats.app_secrets_rotated += 1;
                            if !dry_run {
                                state
                                    .persistence
                                    .update_app_secret(details.id, &rotated)
                                    .await
                                    .map_err(|err| Status::internal(err.to_string()))?;
                            }
                        }
                        None => stats.already_active += 1,
                    }
                }
                let Some(next) = app_page.next_tuple_key else {
                    break;
                };
                if app_cursor.as_ref() == Some(&next) {
                    return Err(Status::internal(
                        "application secret rotation cursor did not advance",
                    ));
                }
                app_cursor = Some(next);
            }
        }
        let Some(next) = tenant_page.next_tuple_key else {
            break;
        };
        if tenant_cursor.as_ref() == Some(&next) {
            return Err(Status::internal(
                "application secret tenant cursor did not advance",
            ));
        }
        tenant_cursor = Some(next);
    }
    Ok(())
}

pub(super) async fn rotate_hf_secret_envelopes(
    state: &AppState,
    dry_run: bool,
    stats: &mut SecretEncryptionRotationStats,
) -> Result<(), Status> {
    let mut cursor = None;
    loop {
        let page = state
            .persistence
            .hf_list_encrypted_key_page(cursor.as_deref(), 256)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        for key in page.keys {
            stats.hf_keys_examined += 1;
            match state
                .secret_keyring
                .reencrypt_if_needed(&key.token_encrypted)
                .map_err(|err| Status::internal(err.to_string()))?
            {
                Some(rotated) => {
                    stats.hf_keys_rotated += 1;
                    if !dry_run {
                        state
                            .persistence
                            .hf_update_key_encrypted(key.id, &rotated)
                            .await
                            .map_err(|err| Status::internal(err.to_string()))?;
                    }
                }
                None => stats.already_active += 1,
            }
        }
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    Ok(())
}

pub(super) fn cell_resource_id(region: &str, cell_id: &str) -> String {
    format!("region:{region}:cell:{cell_id}")
}

pub(super) fn host_alias_audit_details(host_alias: &CoreHostAliasDescriptor) -> serde_json::Value {
    json!({
        "resource_kind": "host_alias",
        "hostname": &host_alias.hostname,
        "tenant_id": &host_alias.tenant_id,
        "bucket_name": &host_alias.bucket_name,
        "region": &host_alias.region,
        "prefix": &host_alias.prefix,
        "state": host_alias.state,
        "generation": host_alias.generation,
    })
}

pub(super) fn region_audit_details(region: &mesh_lifecycle::RegionDescriptor) -> serde_json::Value {
    json!({
        "resource_kind": "region",
        "mesh_id": &region.mesh_id,
        "region": &region.region,
        "state": region.state,
        "public_base_url": &region.public_base_url,
        "virtual_host_suffix": &region.virtual_host_suffix,
        "placement_weight": region.placement_weight,
        "default_cell": &region.default_cell,
        "generation": region.generation,
    })
}

pub(super) fn cell_audit_details(cell: &mesh_lifecycle::CellDescriptor) -> serde_json::Value {
    json!({
        "resource_kind": "cell",
        "mesh_id": &cell.mesh_id,
        "region": &cell.region,
        "cell_id": &cell.cell_id,
        "state": cell.state,
        "placement_weight": cell.placement_weight,
        "failure_domain": &cell.failure_domain,
        "generation": cell.generation,
    })
}

pub(super) fn node_audit_details(node: &mesh_lifecycle::NodeDescriptor) -> serde_json::Value {
    json!({
        "resource_kind": "node",
        "mesh_id": &node.mesh_id,
        "node_id": &node.node_id,
        "region": &node.region,
        "cell_id": &node.cell_id,
        "public_api_addr": &node.public_api_addr,
        "capabilities": &node.capabilities,
        "capacity_json_hash": &node.capacity_json_hash,
        "state": node.state,
        "drain": &node.drain,
        "generation": node.generation,
    })
}

pub(super) fn add_audit_detail(
    details: &mut serde_json::Value,
    key: &str,
    value: serde_json::Value,
) {
    if let serde_json::Value::Object(map) = details {
        map.insert(key.to_string(), value);
    }
}

pub(super) fn bucket_drain_overrides_details(
    overrides: &[BucketDrainOverride],
) -> Vec<serde_json::Value> {
    overrides
        .iter()
        .map(|override_| {
            json!({
                "tenant_id": &override_.tenant_id,
                "bucket_name": &override_.bucket_name,
                "disposition": region_drain_disposition_name(override_.disposition),
                "disposition_code": override_.disposition,
                "reason": &override_.reason,
                "expires_at": none_if_empty(&override_.expires_at),
            })
        })
        .collect()
}

pub(super) fn bucket_drain_override_from_proto(
    override_: &BucketDrainOverride,
) -> Result<persistence::RegionDrainBucketOverride, Status> {
    let disposition = region_drain_disposition_from_proto(override_.disposition, false)?;
    if override_.tenant_id.trim().is_empty() {
        return Err(Status::invalid_argument(
            "bucket drain override tenant_id is required",
        ));
    }
    if override_.bucket_name.trim().is_empty() {
        return Err(Status::invalid_argument(
            "bucket drain override bucket_name is required",
        ));
    }
    if override_.reason.trim().is_empty() && disposition.allows_drained_exception() {
        return Err(Status::invalid_argument(
            "bucket drain override reason is required for drain exceptions",
        ));
    }
    Ok(persistence::RegionDrainBucketOverride {
        tenant_id: override_.tenant_id.trim().to_string(),
        bucket_name: override_.bucket_name.trim().to_string(),
        disposition,
        reason: override_.reason.trim().to_string(),
        expires_at: none_if_empty(&override_.expires_at).map(str::to_string),
    })
}

pub(super) fn region_drain_disposition_from_proto(
    value: i32,
    unspecified_defaults_to_block: bool,
) -> Result<mesh_lifecycle::BucketDrainDisposition, Status> {
    match value {
        0 if unspecified_defaults_to_block => {
            Ok(mesh_lifecycle::BucketDrainDisposition::BlockUntilEmpty)
        }
        1 => Ok(mesh_lifecycle::BucketDrainDisposition::BlockUntilEmpty),
        2 => Ok(mesh_lifecycle::BucketDrainDisposition::RemainProxyOnly),
        3 => Ok(mesh_lifecycle::BucketDrainDisposition::ReadOnlyUntilRemoved),
        4 => Ok(mesh_lifecycle::BucketDrainDisposition::DeleteAfterRetention),
        _ => Err(Status::invalid_argument(format!(
            "unsupported region drain disposition code {value}"
        ))),
    }
}

pub(super) fn region_drain_plan_details(
    report: &persistence::RegionDrainPlanReport,
) -> serde_json::Value {
    json!({
        "region": &report.region,
        "decisions": report.decisions.iter().map(|decision| {
            json!({
                "tenant_id": &decision.tenant_id,
                "bucket_name": &decision.bucket_name,
                "disposition": decision.disposition.as_str(),
                "reason": &decision.reason,
                "expires_at": decision.expires_at.as_deref(),
                "status_before": format!("{:?}", decision.status_before),
                "status_after": format!("{:?}", decision.status_after),
                "bucket_locator_generation_before": decision.bucket_locator_generation_before,
                "bucket_locator_generation_after": decision.bucket_locator_generation_after,
                "exception_written": decision.exception_written,
                "locator_updated": decision.locator_updated,
            })
        }).collect::<Vec<_>>()
    })
}

pub(super) fn region_drain_disposition_name(value: i32) -> &'static str {
    match value {
        1 => "block_until_empty",
        2 => "remain_proxy_only",
        3 => "read_only_until_removed",
        4 => "delete_after_retention",
        _ => "unspecified",
    }
}

pub(super) fn audit_event_to_proto(event: AdminAuditEvent) -> AuditEventRecord {
    AuditEventRecord {
        audit_event_id: event.audit_event_id,
        request_id: event.request_id,
        principal_id: event.principal_id,
        resource_id: event.resource_id,
        action: event.action,
        audit_reason: event.audit_reason,
        created_at: event.created_at,
        details_json: event.details_json,
    }
}

pub(super) fn generated_client_id() -> String {
    format!("app_{}", uuid::Uuid::new_v4().simple())
}

pub(super) fn generated_client_secret() -> String {
    format!("secret_{}", uuid::Uuid::new_v4().simple())
}

pub(super) fn encrypt_admin_client_secret(
    state: &AppState,
    client_secret: &str,
) -> Result<Vec<u8>, Status> {
    state
        .secret_keyring
        .encrypt(client_secret.as_bytes())
        .map_err(|err| Status::internal(err.to_string()))
}

pub(super) fn bucket_to_proto(bucket: Bucket) -> crate::anvil_api::Bucket {
    crate::anvil_api::Bucket {
        name: bucket.name,
        creation_date: bucket
            .created_at
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        region: bucket.region,
        is_public_read: bucket.is_public_read,
        deleted: false,
        bucket_id: bucket.id,
    }
}

pub(super) async fn resolve_tenant_id(state: &AppState, tenant_ref: &str) -> Result<i64, Status> {
    let tenant_ref = tenant_ref.trim();
    if tenant_ref.is_empty() {
        return Err(Status::invalid_argument("tenant_id is required"));
    }
    if let Ok(tenant_id) = tenant_ref.parse::<i64>() {
        return Ok(tenant_id);
    }
    state
        .persistence
        .get_tenant_by_name(tenant_ref)
        .await
        .map_err(|err| Status::internal(err.to_string()))?
        .map(|tenant| tenant.id)
        .ok_or_else(|| Status::not_found("Tenant not found"))
}

pub(super) async fn resolve_tenant_app(
    state: &AppState,
    tenant_id: i64,
    app_name: &str,
) -> Result<persistence::App, Status> {
    let app_name = app_name.trim();
    if app_name.is_empty() {
        return Err(Status::invalid_argument("app_name is required"));
    }
    state
        .persistence
        .get_app_by_tenant_name(tenant_id, app_name)
        .await
        .map_err(|err| Status::internal(err.to_string()))?
        .ok_or_else(|| Status::not_found("Application not found"))
}

pub(super) async fn routing_config_for_region(
    state: &AppState,
    region_name: &str,
) -> Result<RoutingConfig, Status> {
    let region_name = region_name.trim();
    if region_name.is_empty() {
        return Err(Status::invalid_argument("region is required"));
    }
    let region = state
        .persistence
        .list_region_descriptors()
        .await
        .map_err(lifecycle_status)?
        .into_iter()
        .find(|region| region.region == region_name)
        .ok_or_else(|| Status::not_found("Region not found"))?;
    let base_domain = base_domain_from_region_suffix(&region.region, &region.virtual_host_suffix)?;
    RoutingConfig::new(base_domain).map_err(|err| Status::invalid_argument(err.to_string()))
}

pub(super) fn base_domain_from_region_suffix(
    region: &str,
    virtual_host_suffix: &str,
) -> Result<String, Status> {
    let suffix = routing::normalize_alias_hostname(virtual_host_suffix)
        .map_err(|err| Status::invalid_argument(err.to_string()))?;
    let region_prefix = format!(
        "{}.",
        region.trim().trim_end_matches('.').to_ascii_lowercase()
    );
    Ok(suffix
        .strip_prefix(&region_prefix)
        .unwrap_or(&suffix)
        .to_string())
}

pub(super) fn page_limit(page: Option<&PageRequest>) -> Result<usize, Status> {
    crate::services::collection_cursor::page_size(page)
}

pub(super) fn lifecycle_status(err: LifecycleError) -> Status {
    match err {
        LifecycleError::InvalidArgument(message) => Status::invalid_argument(message),
        LifecycleError::AlreadyExists { .. } => Status::already_exists(err.to_string()),
        LifecycleError::NotFound { .. } => Status::not_found(err.to_string()),
        LifecycleError::GenerationConflict { .. } => Status::aborted(err.to_string()),
        LifecycleError::LifecycleTransitionDenied { .. }
        | LifecycleError::ActivationCheckpointNotReached { .. } => {
            Status::failed_precondition(err.to_string())
        }
        LifecycleError::Io(_) | LifecycleError::Json(_) | LifecycleError::Other(_) => {
            Status::internal(err.to_string())
        }
    }
}

pub(super) fn node_capability_from_proto(value: i32) -> Result<CoreNodeCapability, Status> {
    match value {
        1 => Ok(CoreNodeCapability::Object),
        2 => Ok(CoreNodeCapability::Index),
        3 => Ok(CoreNodeCapability::PersonalDb),
        4 => Ok(CoreNodeCapability::Metadata),
        5 => Ok(CoreNodeCapability::Gateway),
        6 => Ok(CoreNodeCapability::Admin),
        _ => Err(Status::invalid_argument("Invalid node capability")),
    }
}

pub(super) fn node_capability_to_proto(value: CoreNodeCapability) -> i32 {
    match value {
        CoreNodeCapability::Object => 1,
        CoreNodeCapability::Index => 2,
        CoreNodeCapability::PersonalDb => 3,
        CoreNodeCapability::Metadata => 4,
        CoreNodeCapability::Gateway => 5,
        CoreNodeCapability::Admin => 6,
    }
}

pub(super) fn lifecycle_state_to_proto(value: CoreLifecycleState) -> i32 {
    match value {
        CoreLifecycleState::Joining => 1,
        CoreLifecycleState::Active => 2,
        CoreLifecycleState::ReadOnly => 3,
        CoreLifecycleState::Draining => 4,
        CoreLifecycleState::Drained => 5,
        CoreLifecycleState::DrainedWithExceptions => 6,
        CoreLifecycleState::Offline => 7,
        CoreLifecycleState::Removed => 8,
    }
}

pub(super) fn routing_record_family_from_proto(
    value: i32,
) -> Result<Option<mesh_directory::RoutingRecordFamily>, Status> {
    match value {
        0 => Ok(None),
        1 => Ok(Some(mesh_directory::RoutingRecordFamily::TenantName)),
        2 => Ok(Some(mesh_directory::RoutingRecordFamily::TenantLocator)),
        3 => Ok(Some(mesh_directory::RoutingRecordFamily::BucketLocator)),
        4 => Ok(Some(mesh_directory::RoutingRecordFamily::HostAlias)),
        _ => Err(Status::invalid_argument("Invalid routing record family")),
    }
}

pub(super) fn routing_record_family_to_proto(value: mesh_directory::RoutingRecordFamily) -> i32 {
    match value {
        mesh_directory::RoutingRecordFamily::TenantName => 1,
        mesh_directory::RoutingRecordFamily::TenantLocator => 2,
        mesh_directory::RoutingRecordFamily::BucketLocator => 3,
        mesh_directory::RoutingRecordFamily::HostAlias => 4,
    }
}

pub(super) fn routing_record_descriptor_to_proto(
    value: mesh_directory::RoutingRecordDescriptor,
) -> RoutingRecordDescriptor {
    RoutingRecordDescriptor {
        family: routing_record_family_to_proto(value.family),
        record_key: value.record_key,
        partition: value.partition,
        descriptor_key: value.descriptor_key,
        generation: value.generation,
        payload_json: value.payload_json,
    }
}

pub(super) fn host_alias_state_to_proto(value: CoreHostAliasState) -> i32 {
    match value {
        CoreHostAliasState::PendingVerification => 1,
        CoreHostAliasState::Active => 2,
        CoreHostAliasState::Suspended => 3,
        CoreHostAliasState::Deleted => 4,
    }
}

pub(super) fn host_alias_descriptor_to_proto(
    value: CoreHostAliasDescriptor,
) -> crate::anvil_api::HostAliasDescriptor {
    let verification_challenge = host_alias_verification_challenge(&value);
    crate::anvil_api::HostAliasDescriptor {
        schema: value.schema,
        hostname: value.hostname,
        tenant_id: value.tenant_id,
        bucket_name: value.bucket_name,
        region: value.region,
        prefix: value.prefix,
        state: host_alias_state_to_proto(value.state),
        created_at: value.created_at,
        updated_at: value.updated_at,
        generation: value.generation,
        verification_challenge,
    }
}

pub(super) fn host_alias_verification_challenge(value: &CoreHostAliasDescriptor) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(value.hostname.as_bytes());
    hasher.update(b"\0");
    hasher.update(value.tenant_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(value.bucket_name.as_bytes());
    hasher.update(b"\0");
    hasher.update(value.region.as_bytes());
    hasher.update(b"\0");
    hasher.update(value.prefix.as_bytes());
    format!("anvil-host-alias={}", hasher.finalize().to_hex())
}

pub(super) fn node_descriptor_to_proto(value: mesh_lifecycle::NodeDescriptor) -> NodeDescriptor {
    NodeDescriptor {
        schema: value.schema,
        mesh_id: value.mesh_id,
        node_id: value.node_id,
        region: value.region,
        cell_id: value.cell_id,
        receipt_signing_public_key: value.receipt_signing_public_key,
        public_api_addr: value.public_api_addr,
        capabilities: value
            .capabilities
            .into_iter()
            .map(node_capability_to_proto)
            .collect(),
        capacity_json_hash: value.capacity_json_hash,
        state: lifecycle_state_to_proto(value.state),
        drain: value.drain.map(node_drain_descriptor_to_proto),
        last_heartbeat_at: value.last_heartbeat_at.unwrap_or_default(),
        created_at: value.created_at,
        updated_at: value.updated_at,
        generation: value.generation,
    }
}

pub(super) fn node_drain_descriptor_to_proto(
    value: NodeDrainDescriptor,
) -> crate::anvil_api::NodeDrainDescriptor {
    crate::anvil_api::NodeDrainDescriptor {
        started_at: value.started_at,
        graceful_timeout_ms: value.graceful_timeout_ms,
        force_after_timeout: value.force_after_timeout,
    }
}

pub(super) fn region_descriptor_to_proto(
    value: mesh_lifecycle::RegionDescriptor,
) -> RegionDescriptor {
    RegionDescriptor {
        schema: value.schema,
        mesh_id: value.mesh_id,
        region: value.region,
        state: lifecycle_state_to_proto(value.state),
        public_base_url: value.public_base_url,
        virtual_host_suffix: value.virtual_host_suffix,
        placement_weight: value.placement_weight,
        default_cell: value.default_cell.unwrap_or_default(),
        created_at: value.created_at,
        updated_at: value.updated_at,
        generation: value.generation,
    }
}

pub(super) fn cell_descriptor_to_proto(value: mesh_lifecycle::CellDescriptor) -> CellDescriptor {
    CellDescriptor {
        schema: value.schema,
        mesh_id: value.mesh_id,
        region: value.region,
        cell_id: value.cell_id,
        state: lifecycle_state_to_proto(value.state),
        placement_weight: value.placement_weight,
        failure_domain: value.failure_domain,
        created_at: value.created_at,
        updated_at: value.updated_at,
        generation: value.generation,
    }
}

pub(super) fn empty_to_none(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

pub(super) fn none_if_empty(value: &str) -> Option<&str> {
    if value.is_empty() { None } else { Some(value) }
}

pub(super) fn parse_personaldb_key_status(
    value: &str,
    allow_active: bool,
) -> Result<PublicKeyStatus, Status> {
    let status = match value {
        "active" if allow_active => PublicKeyStatus::Active,
        "retiring" => PublicKeyStatus::Retiring,
        "revoked_future" => PublicKeyStatus::RevokedFuture,
        "compromised" => PublicKeyStatus::Compromised,
        "active" => {
            return Err(Status::invalid_argument(
                "A non-active PersonalDB signing key cannot be reactivated",
            ));
        }
        _ => {
            return Err(Status::invalid_argument(
                "PersonalDB signing key status must be active, retiring, revoked_future, or compromised",
            ));
        }
    };
    Ok(status)
}

pub(super) fn personaldb_signing_key_to_proto(
    record: PersonalDbSigningKeyPublicRecord,
) -> PersonalDbSigningKeyRecord {
    PersonalDbSigningKeyRecord {
        key_id: record.trust_record.key_id.to_string(),
        key_generation: record.trust_record.key_generation.get(),
        purpose: record.trust_record.purpose.as_str().to_string(),
        database_scopes: record
            .trust_record
            .database_scopes
            .iter()
            .map(|database_id| database_id.0.clone())
            .collect(),
        group_scopes: record.trust_record.group_scopes.clone(),
        valid_from_log_index: record.trust_record.valid_from_log_index,
        valid_until_log_index: record.trust_record.valid_until_log_index,
        status: record.trust_record.status.as_str().to_string(),
        public_key: record.trust_record.public_key.as_bytes().to_vec(),
        created_at_unix_nanos: record.created_at_unix_nanos,
        updated_at_unix_nanos: record.updated_at_unix_nanos,
        created_by: record.created_audit.actor_id,
        updated_by: record.updated_audit.actor_id,
        record_revision: record.record_revision,
    }
}

pub(super) fn storage_class_to_proto(
    class: &crate::core_store::CoreStorageClass,
    default_class_id: &str,
) -> StorageClassDescriptor {
    StorageClassDescriptor {
        class_id: class.class_id.clone(),
        description: class.description.clone(),
        metadata_profile_id: class.metadata_profile.profile_id.clone(),
        metadata_replica_count: u32::from(class.metadata_profile.replica_count),
        metadata_prepare_quorum: u32::from(class.metadata_profile.prepare_quorum),
        metadata_certificate_persist_quorum: u32::from(
            class.metadata_profile.certificate_persist_quorum,
        ),
        metadata_fsync_mode: class.metadata_profile.fsync_mode.clone(),
        byte_profile_id: class.byte_profile.profile_id.clone(),
        byte_codec_id: class.byte_profile.codec_id.clone(),
        data_shards: u32::from(class.byte_profile.data_shards),
        parity_shards: u32::from(class.byte_profile.parity_shards),
        read_quorum: u32::from(class.byte_profile.read_quorum),
        write_publish_threshold: u32::from(class.byte_profile.write_publish_threshold),
        target_block_bytes: class.byte_profile.target_block_bytes,
        max_shard_bytes: class.byte_profile.max_shard_bytes,
        compression: class.byte_profile.compression.clone(),
        encryption: class.byte_profile.encryption.clone(),
        inline_payload_enabled: class.inline_payload_policy.enabled,
        max_inline_payload_bytes: class.inline_payload_policy.max_raw_payload_bytes,
        absolute_inline_record_max_bytes: class
            .inline_payload_policy
            .absolute_encoded_record_max_bytes,
        min_cell_spread: u32::from(class.min_cell_spread),
        tenant_selectable: class.tenant_selectable,
        is_default: class.class_id == default_class_id,
    }
}
