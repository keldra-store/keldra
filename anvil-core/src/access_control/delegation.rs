use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegatedSystemRelation {
    pub namespace: String,
    pub object_id: String,
    pub relation: String,
}

fn normalize_delegation_resource(tenant_id: i64, resource: &str) -> Result<String, Status> {
    let resource = resource.trim();
    if resource.is_empty() {
        return Err(Status::invalid_argument("resource is required"));
    }

    let tenant_exact = format!("tenant:{tenant_id}");
    if resource == tenant_exact {
        return Ok(String::new());
    }
    let tenant_colon = format!("tenant:{tenant_id}:");
    if let Some(rest) = resource.strip_prefix(&tenant_colon) {
        return Ok(rest.to_string());
    }
    if resource.starts_with("tenant:") {
        return Err(Status::permission_denied(
            "cross-tenant delegation is not allowed",
        ));
    }

    if let Some(rest) = resource.strip_prefix("tenant-") {
        if let Some((candidate, suffix)) = rest.split_once('/') {
            if !candidate.is_empty() && candidate.bytes().all(|byte| byte.is_ascii_digit()) {
                if candidate == tenant_id.to_string() {
                    return Ok(suffix.to_string());
                }
                return Err(Status::permission_denied(
                    "cross-tenant delegation is not allowed",
                ));
            }
        }
    }

    Ok(resource.to_string())
}

async fn read_bucket_for_tenant(
    persistence: &Persistence,
    tenant_id: i64,
    bucket_name: &str,
) -> Result<Bucket, Status> {
    bucket_journal::read_current_bucket_mvcc(
        persistence
            .mvcc()
            .map_err(|error| Status::internal(error.to_string()))?,
        tenant_id,
        bucket_name,
    )
    .map_err(|error| Status::internal(error.to_string()))?
    .ok_or_else(|| Status::not_found("Bucket not found"))
}

pub async fn delegated_relation_for_action(
    storage: &Storage,
    persistence: &Persistence,
    tenant_id: i64,
    action: AnvilAction,
    resource: &str,
) -> Result<DelegatedSystemRelation, Status> {
    let resource = normalize_delegation_resource(tenant_id, resource)?;
    match action {
        AnvilAction::MeshManage
        | AnvilAction::MeshRead
        | AnvilAction::RepairRun
        | AnvilAction::RepairRead
        | AnvilAction::InternalProxyObject
        | AnvilAction::IndexWatch
        | AnvilAction::GitSourceWrite
        | AnvilAction::GitSourceRead
        | AnvilAction::GitSourceWatch => {
            return Err(Status::permission_denied(
                "This action cannot be delegated through tenant access grants",
            ));
        }

        AnvilAction::TenantManage => Ok(DelegatedSystemRelation {
            namespace: system_realm_namespace(SYSTEM_STORAGE_TENANT_NAMESPACE),
            object_id: storage_tenant_object_id(tenant_id),
            relation: "manage_tenant".to_string(),
        }),
        AnvilAction::BucketCreate => Ok(DelegatedSystemRelation {
            namespace: system_realm_namespace(SYSTEM_STORAGE_TENANT_NAMESPACE),
            object_id: storage_tenant_object_id(tenant_id),
            relation: "create_bucket".to_string(),
        }),
        AnvilAction::BucketList | AnvilAction::BucketWatch => Ok(DelegatedSystemRelation {
            namespace: system_realm_namespace(SYSTEM_STORAGE_TENANT_NAMESPACE),
            object_id: storage_tenant_object_id(tenant_id),
            relation: "list_buckets".to_string(),
        }),
        AnvilAction::BucketRead | AnvilAction::BucketWrite | AnvilAction::BucketDelete => {
            let bucket = read_bucket_for_tenant(persistence, tenant_id, &resource).await?;
            Ok(DelegatedSystemRelation {
                namespace: system_realm_namespace(SYSTEM_BUCKET_NAMESPACE),
                object_id: bucket_object_id(&bucket),
                relation: match action {
                    AnvilAction::BucketRead => "list_objects",
                    AnvilAction::BucketWrite | AnvilAction::BucketDelete => "manage_bucket",
                    _ => unreachable!(),
                }
                .to_string(),
            })
        }

        AnvilAction::ObjectList => {
            let (bucket_name, _) = split_bucket_key(&resource);
            let bucket = read_bucket_for_tenant(persistence, tenant_id, bucket_name).await?;
            Ok(DelegatedSystemRelation {
                namespace: system_realm_namespace(SYSTEM_BUCKET_NAMESPACE),
                object_id: bucket_object_id(&bucket),
                relation: "list_objects".to_string(),
            })
        }
        AnvilAction::ObjectRead | AnvilAction::ObjectWrite | AnvilAction::ObjectDelete => {
            let (bucket_name, key) = split_bucket_key(&resource);
            let bucket = read_bucket_for_tenant(persistence, tenant_id, bucket_name).await?;
            if let Some(key) = key {
                Ok(DelegatedSystemRelation {
                    namespace: system_realm_namespace(SYSTEM_OBJECT_NAMESPACE),
                    object_id: object_object_id(&bucket, key),
                    relation: match action {
                        AnvilAction::ObjectRead => "get",
                        AnvilAction::ObjectWrite => "put",
                        AnvilAction::ObjectDelete => "delete",
                        _ => unreachable!(),
                    }
                    .to_string(),
                })
            } else {
                Ok(DelegatedSystemRelation {
                    namespace: system_realm_namespace(SYSTEM_BUCKET_NAMESPACE),
                    object_id: bucket_object_id(&bucket),
                    relation: match action {
                        AnvilAction::ObjectRead => "get_object",
                        AnvilAction::ObjectWrite => "put_object",
                        AnvilAction::ObjectDelete => "delete_object",
                        _ => unreachable!(),
                    }
                    .to_string(),
                })
            }
        }

        AnvilAction::IndexCreate
        | AnvilAction::IndexUpdate
        | AnvilAction::IndexDelete
        | AnvilAction::IndexRead => {
            let (bucket_name, index_name) = split_bucket_key(&resource);
            let bucket = read_bucket_for_tenant(persistence, tenant_id, bucket_name).await?;
            if let Some(index_name) = index_name {
                Ok(DelegatedSystemRelation {
                    namespace: system_realm_namespace(SYSTEM_INDEX_NAMESPACE),
                    object_id: index_object_id(&bucket, index_name),
                    relation: match action {
                        AnvilAction::IndexCreate
                        | AnvilAction::IndexUpdate
                        | AnvilAction::IndexDelete => "define",
                        AnvilAction::IndexRead => "query",
                        _ => unreachable!(),
                    }
                    .to_string(),
                })
            } else {
                Ok(DelegatedSystemRelation {
                    namespace: system_realm_namespace(SYSTEM_BUCKET_NAMESPACE),
                    object_id: bucket_object_id(&bucket),
                    relation: match action {
                        AnvilAction::IndexCreate
                        | AnvilAction::IndexUpdate
                        | AnvilAction::IndexDelete => "manage_indexes",
                        AnvilAction::IndexRead => "query_indexes",
                        _ => unreachable!(),
                    }
                    .to_string(),
                })
            }
        }

        AnvilAction::StreamCreate => {
            let (bucket_name, _) = split_bucket_key(&resource);
            let bucket = read_bucket_for_tenant(persistence, tenant_id, bucket_name).await?;
            Ok(DelegatedSystemRelation {
                namespace: system_realm_namespace(SYSTEM_BUCKET_NAMESPACE),
                object_id: bucket_object_id(&bucket),
                relation: "put_object".to_string(),
            })
        }
        AnvilAction::StreamAppend | AnvilAction::StreamRead | AnvilAction::StreamSealSegment => {
            let (bucket_name, stream_key) = split_bucket_key(&resource);
            let stream_key = stream_key.ok_or_else(|| {
                Status::invalid_argument("stream delegation resource must be bucket/stream")
            })?;
            let bucket = read_bucket_for_tenant(persistence, tenant_id, bucket_name).await?;
            Ok(DelegatedSystemRelation {
                namespace: system_realm_namespace(SYSTEM_STREAM_NAMESPACE),
                object_id: stream_object_id(&bucket, stream_key),
                relation: match action {
                    AnvilAction::StreamAppend => "append",
                    AnvilAction::StreamRead => "read",
                    AnvilAction::StreamSealSegment => "seal_segment",
                    _ => unreachable!(),
                }
                .to_string(),
            })
        }

        AnvilAction::AppCreate
        | AnvilAction::AppRead
        | AnvilAction::AppRotateSecret
        | AnvilAction::AppDelete
        | AnvilAction::PolicyRead
        | AnvilAction::PolicyGrant
        | AnvilAction::PolicyRevoke
        | AnvilAction::HfKeyCreate
        | AnvilAction::HfKeyRead
        | AnvilAction::HfKeyDelete
        | AnvilAction::HfKeyList
        | AnvilAction::HfIngestionCreate
        | AnvilAction::HfIngestionRead
        | AnvilAction::HfIngestionDelete => Ok(DelegatedSystemRelation {
            namespace: system_realm_namespace(SYSTEM_STORAGE_TENANT_NAMESPACE),
            object_id: storage_tenant_object_id(tenant_id),
            relation: if matches!(
                action,
                AnvilAction::HfIngestionRead
                    | AnvilAction::HfKeyRead
                    | AnvilAction::HfKeyList
                    | AnvilAction::AppRead
            ) {
                "read_tenant"
            } else if matches!(action, AnvilAction::PolicyRead) {
                "read_access_grants"
            } else if matches!(action, AnvilAction::PolicyGrant) {
                "grant_access"
            } else if matches!(action, AnvilAction::PolicyRevoke) {
                "revoke_access"
            } else {
                "manage_tenant"
            }
            .to_string(),
        }),

        AnvilAction::AuthzTupleWrite
        | AnvilAction::AuthzTupleRead
        | AnvilAction::AuthzCheck
        | AnvilAction::AuthzWatch
        | AnvilAction::AuthzSchemaRead
        | AnvilAction::AuthzSchemaWrite => {
            let relation = match action {
                AnvilAction::AuthzTupleWrite => "tuple_writer",
                AnvilAction::AuthzCheck => "checker",
                AnvilAction::AuthzTupleRead
                | AnvilAction::AuthzWatch
                | AnvilAction::AuthzSchemaRead => "auditor",
                AnvilAction::AuthzSchemaWrite => "schema_admin",
                _ => unreachable!(),
            };
            Ok(DelegatedSystemRelation {
                namespace: system_realm_namespace(SYSTEM_AUTHZ_REALM_NAMESPACE),
                object_id: authz_realm_object_id(tenant_id, &resource),
                relation: relation.to_string(),
            })
        }

        AnvilAction::PersonalDbCreate => Ok(DelegatedSystemRelation {
            namespace: system_realm_namespace(SYSTEM_STORAGE_TENANT_NAMESPACE),
            object_id: storage_tenant_object_id(tenant_id),
            relation: "manage_tenant".to_string(),
        }),
        AnvilAction::PersonalDbRead | AnvilAction::PersonalDbWatch => Ok(DelegatedSystemRelation {
            namespace: system_realm_namespace(SYSTEM_PERSONALDB_GROUP_NAMESPACE),
            object_id: personaldb_group_object_id(tenant_id, &resource),
            relation: if matches!(action, AnvilAction::PersonalDbWatch) {
                "watch"
            } else {
                "get_snapshot"
            }
            .to_string(),
        }),
        AnvilAction::PersonalDbCommit
        | AnvilAction::PersonalDbInsert
        | AnvilAction::PersonalDbUpdate
        | AnvilAction::PersonalDbDelete => Ok(DelegatedSystemRelation {
            namespace: system_realm_namespace(SYSTEM_PERSONALDB_GROUP_NAMESPACE),
            object_id: personaldb_group_object_id(tenant_id, &resource),
            relation: "apply_changeset".to_string(),
        }),

        AnvilAction::CoordinationLeaseRead
        | AnvilAction::CoordinationLeaseWrite
        | AnvilAction::CoordinationLeaseAdmin => Ok(DelegatedSystemRelation {
            namespace: system_realm_namespace(SYSTEM_STORAGE_TENANT_NAMESPACE),
            object_id: storage_tenant_object_id(tenant_id),
            relation: match action {
                AnvilAction::CoordinationLeaseRead => "lease_read",
                AnvilAction::CoordinationLeaseWrite => "lease_write",
                AnvilAction::CoordinationLeaseAdmin => "lease_admin",
                _ => unreachable!(),
            }
            .to_string(),
        }),

        AnvilAction::RegistryBlobWrite
        | AnvilAction::RegistryVersionWrite
        | AnvilAction::RegistryRefWrite => Ok(DelegatedSystemRelation {
            namespace: system_realm_namespace(SYSTEM_REGISTRY_NAMESPACE),
            object_id: registry_namespace_object_id(
                tenant_id,
                registry_namespace_resource(&resource),
            ),
            relation: "publish".to_string(),
        }),
        AnvilAction::RegistryRead | AnvilAction::RegistryList => Ok(DelegatedSystemRelation {
            namespace: system_realm_namespace(SYSTEM_REGISTRY_NAMESPACE),
            object_id: registry_namespace_object_id(
                tenant_id,
                registry_namespace_resource(&resource),
            ),
            relation: "read".to_string(),
        }),
    }
}

pub async fn write_delegated_action_tuple(
    storage: &Storage,
    persistence: &Persistence,
    tenant_id: i64,
    grantee_principal_id: &str,
    action: AnvilAction,
    resource: &str,
    operation: &str,
    written_by: &str,
    reason: &str,
    audit_event: &crate::tenant_audit::TenantAuditEvent,
) -> Result<(), Status> {
    write_delegated_action_tuple_with_audit(
        storage,
        persistence,
        tenant_id,
        grantee_principal_id,
        action,
        resource,
        operation,
        written_by,
        reason,
        None,
        Some(audit_event),
    )
    .await
}

pub async fn write_delegated_action_tuple_with_admin_audit(
    storage: &Storage,
    persistence: &Persistence,
    tenant_id: i64,
    grantee_principal_id: &str,
    action: AnvilAction,
    resource: &str,
    operation: &str,
    written_by: &str,
    reason: &str,
    audit_event: &crate::admin_audit::AdminAuditEvent,
) -> Result<(), Status> {
    write_delegated_action_tuple_with_audit(
        storage,
        persistence,
        tenant_id,
        grantee_principal_id,
        action,
        resource,
        operation,
        written_by,
        reason,
        Some(audit_event),
        None,
    )
    .await
}

async fn write_delegated_action_tuple_with_audit(
    storage: &Storage,
    persistence: &Persistence,
    tenant_id: i64,
    grantee_principal_id: &str,
    action: AnvilAction,
    resource: &str,
    operation: &str,
    written_by: &str,
    reason: &str,
    admin_audit_event: Option<&crate::admin_audit::AdminAuditEvent>,
    tenant_audit_event: Option<&crate::tenant_audit::TenantAuditEvent>,
) -> Result<(), Status> {
    let relation =
        delegated_relation_for_action(storage, persistence, tenant_id, action.clone(), resource)
            .await?;
    let assignment_relation = delegated_assignment_relation(&action, &relation);
    let records = persistence
        .write_authz_tuple_batch_with_admin_audit(
            SYSTEM_STORAGE_TENANT_ID,
            vec![AuthzTupleBatchMutation {
                namespace: relation.namespace,
                object_id: relation.object_id,
                relation: assignment_relation,
                subject_kind: APP_SUBJECT_KIND.to_string(),
                subject_id: grantee_principal_id.to_string(),
                caveat_hash: String::new(),
                operation: operation.to_string(),
                reason: reason.to_string(),
            }],
            written_by,
            admin_audit_event,
            tenant_audit_event,
        )
        .await
        .map_err(authz_tuple_write_status)?;
    persistence
        .materialize_authz_through_revision(
            SYSTEM_STORAGE_TENANT_ID,
            records
                .first()
                .ok_or_else(|| Status::internal("missing authz record"))?
                .revision,
        )
        .await
        .map_err(authz_tuple_write_status)?;
    Ok(())
}

pub async fn stage_delegated_action_tuple(
    storage: &Storage,
    persistence: &Persistence,
    tenant_id: i64,
    grantee_principal_id: &str,
    action: AnvilAction,
    resource: &str,
    operation: &str,
    written_by: &str,
    reason: &str,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<(), Status> {
    stage_delegated_action_tuple_with_tenant_audit(
        storage,
        persistence,
        tenant_id,
        grantee_principal_id,
        action,
        resource,
        operation,
        written_by,
        reason,
        transaction_id,
        transaction_principal,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn stage_delegated_action_tuple_with_tenant_audit(
    storage: &Storage,
    persistence: &Persistence,
    tenant_id: i64,
    grantee_principal_id: &str,
    action: AnvilAction,
    resource: &str,
    operation: &str,
    written_by: &str,
    reason: &str,
    transaction_id: &str,
    transaction_principal: &str,
    audit_event: Option<&crate::tenant_audit::TenantAuditEvent>,
) -> Result<(), Status> {
    let relation =
        delegated_relation_for_action(storage, persistence, tenant_id, action.clone(), resource)
            .await?;
    let assignment_relation = delegated_assignment_relation(&action, &relation);
    persistence
        .stage_authz_tuple_batch_with_tenant_audit(
            SYSTEM_STORAGE_TENANT_ID,
            vec![AuthzTupleBatchMutation {
                namespace: relation.namespace,
                object_id: relation.object_id,
                relation: assignment_relation,
                subject_kind: APP_SUBJECT_KIND.to_string(),
                subject_id: grantee_principal_id.to_string(),
                caveat_hash: String::new(),
                operation: operation.to_string(),
                reason: reason.to_string(),
            }],
            written_by,
            transaction_id,
            transaction_principal,
            None,
            audit_event,
        )
        .await
        .map_err(authz_tuple_write_status)?;
    Ok(())
}

pub async fn write_delegated_action_tuple_batch(
    storage: &Storage,
    persistence: &Persistence,
    tenant_id: i64,
    grantee_principal_id: &str,
    policies: &[(AnvilAction, String)],
    operation: &str,
    written_by: &str,
    reason: &str,
    audit_event: &crate::admin_audit::AdminAuditEvent,
) -> Result<(), Status> {
    if policies.is_empty() {
        return Err(Status::invalid_argument(
            "At least one application policy is required",
        ));
    }
    if !matches!(operation, "add" | "remove") {
        return Err(Status::invalid_argument(
            "Application policy operation must be add or remove",
        ));
    }

    let mut mutations = Vec::with_capacity(policies.len());
    for (action, resource) in policies {
        let relation = delegated_relation_for_action(
            storage,
            persistence,
            tenant_id,
            action.clone(),
            resource,
        )
        .await?;
        let assignment_relation = delegated_assignment_relation(action, &relation);
        mutations.push(AuthzTupleBatchMutation {
            namespace: relation.namespace,
            object_id: relation.object_id,
            relation: assignment_relation,
            subject_kind: APP_SUBJECT_KIND.to_string(),
            subject_id: grantee_principal_id.to_string(),
            caveat_hash: String::new(),
            operation: operation.to_string(),
            reason: reason.to_string(),
        });
    }

    let records = persistence
        .write_authz_tuple_batch_with_admin_audit(
            SYSTEM_STORAGE_TENANT_ID,
            mutations,
            written_by,
            Some(audit_event),
            None,
        )
        .await
        .map_err(authz_tuple_write_status)?;
    let revision = records
        .iter()
        .map(|record| record.revision)
        .max()
        .ok_or_else(|| Status::internal("application policy batch produced no records"))?;
    persistence
        .materialize_authz_through_revision(SYSTEM_STORAGE_TENANT_ID, revision)
        .await
        .map_err(authz_tuple_write_status)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn stage_delegated_action_tuple_batch_with_admin_audit(
    storage: &Storage,
    persistence: &Persistence,
    tenant_id: i64,
    grantee_principal_id: &str,
    policies: &[(AnvilAction, String)],
    operation: &str,
    written_by: &str,
    reason: &str,
    audit_event: &crate::admin_audit::AdminAuditEvent,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<(), Status> {
    if policies.is_empty() {
        return Err(Status::invalid_argument(
            "At least one application policy is required",
        ));
    }
    if !matches!(operation, "add" | "remove") {
        return Err(Status::invalid_argument(
            "Application policy operation must be add or remove",
        ));
    }
    let mut mutations = Vec::with_capacity(policies.len());
    for (action, resource) in policies {
        let relation = delegated_relation_for_action(
            storage,
            persistence,
            tenant_id,
            action.clone(),
            resource,
        )
        .await?;
        let assignment_relation = delegated_assignment_relation(action, &relation);
        mutations.push(AuthzTupleBatchMutation {
            namespace: relation.namespace,
            object_id: relation.object_id,
            relation: assignment_relation,
            subject_kind: APP_SUBJECT_KIND.to_string(),
            subject_id: grantee_principal_id.to_string(),
            caveat_hash: String::new(),
            operation: operation.to_string(),
            reason: reason.to_string(),
        });
    }
    persistence
        .stage_authz_tuple_batch_with_admin_audit(
            SYSTEM_STORAGE_TENANT_ID,
            mutations,
            written_by,
            transaction_id,
            transaction_principal,
            None,
            Some(audit_event),
        )
        .await
        .map_err(authz_tuple_write_status)?;
    Ok(())
}

pub(super) fn delegated_assignment_relation(
    action: &AnvilAction,
    relation: &DelegatedSystemRelation,
) -> String {
    // Authz actions intentionally map to assignable realm roles. Other actions
    // map to computed permissions whose generated direct edge uses `_grant`.
    if matches!(
        action,
        AnvilAction::AuthzTupleWrite
            | AnvilAction::AuthzTupleRead
            | AnvilAction::AuthzCheck
            | AnvilAction::AuthzWatch
            | AnvilAction::AuthzSchemaRead
            | AnvilAction::AuthzSchemaWrite
    ) {
        relation.relation.clone()
    } else {
        format!("{}_grant", relation.relation)
    }
}

fn authz_tuple_write_status(error: anyhow::Error) -> Status {
    if let Some(contract) = error.chain().find_map(|cause| {
        cause.downcast_ref::<crate::authz_schema_contract::AuthzSchemaContractError>()
    }) {
        Status::invalid_argument(contract.to_string())
    } else {
        Status::internal(error.to_string())
    }
}

pub async fn system_realm_relationship_allows(
    storage: &Storage,
    mvcc: &MvccSubsystem,
    claims: &auth::Claims,
    namespace: &str,
    object_id: &str,
    relation: &str,
    authz_revision: Option<i64>,
) -> Result<bool> {
    let namespace = system_realm_namespace(namespace);
    match authz_revision {
        Some(revision) => {
            authz_journal::resolve_permission_at_revision(
                storage,
                mvcc,
                SYSTEM_STORAGE_TENANT_ID,
                &namespace,
                object_id,
                relation,
                APP_SUBJECT_KIND,
                &claims.sub,
                "",
                revision,
            )
            .await
        }
        None => {
            authz_journal::resolve_current_permission(
                storage,
                mvcc,
                SYSTEM_STORAGE_TENANT_ID,
                &namespace,
                object_id,
                relation,
                APP_SUBJECT_KIND,
                &claims.sub,
                "",
            )
            .await
        }
    }
}

pub async fn require_system_realm_permission(
    storage: &Storage,
    mvcc: &MvccSubsystem,
    claims: &auth::Claims,
    namespace: &str,
    object_id: &str,
    relation: &str,
) -> Result<(), Status> {
    if system_realm_relationship_allows(storage, mvcc, claims, namespace, object_id, relation, None)
        .await
        .map_err(|error| Status::internal(error.to_string()))?
    {
        Ok(())
    } else {
        Err(Status::permission_denied("Permission denied"))
    }
}

pub async fn require_storage_tenant_permission(
    storage: &Storage,
    mvcc: &MvccSubsystem,
    claims: &auth::Claims,
    relation: &str,
) -> Result<(), Status> {
    require_system_realm_permission(
        storage,
        mvcc,
        claims,
        SYSTEM_STORAGE_TENANT_NAMESPACE,
        &storage_tenant_object_id(claims.tenant_id),
        relation,
    )
    .await
}

pub async fn require_bucket_permission(
    storage: &Storage,
    mvcc: &MvccSubsystem,
    claims: &auth::Claims,
    bucket: &Bucket,
    relation: &str,
) -> Result<(), Status> {
    require_system_realm_permission(
        storage,
        mvcc,
        claims,
        SYSTEM_BUCKET_NAMESPACE,
        &bucket_object_id(bucket),
        relation,
    )
    .await
}

pub async fn require_object_permission(
    storage: &Storage,
    mvcc: &MvccSubsystem,
    claims: &auth::Claims,
    bucket: &Bucket,
    object_key: &str,
    relation: &str,
) -> Result<(), Status> {
    if system_realm_relationship_allows(
        storage,
        mvcc,
        claims,
        SYSTEM_OBJECT_NAMESPACE,
        &object_object_id(bucket, object_key),
        relation,
        None,
    )
    .await
    .map_err(|error| Status::internal(error.to_string()))?
    {
        return Ok(());
    }

    let bucket_relation = match relation {
        "get" => "get_object",
        "put" => "put_object",
        "delete" => "delete_object",
        "link" => "manage_links",
        other => other,
    };
    require_bucket_permission(storage, mvcc, claims, bucket, bucket_relation).await
}

pub async fn require_index_permission(
    storage: &Storage,
    mvcc: &MvccSubsystem,
    claims: &auth::Claims,
    bucket: &Bucket,
    index_name_or_id: &str,
    relation: &str,
) -> Result<(), Status> {
    if system_realm_relationship_allows(
        storage,
        mvcc,
        claims,
        SYSTEM_INDEX_NAMESPACE,
        &index_object_id(bucket, index_name_or_id),
        relation,
        None,
    )
    .await
    .map_err(|error| Status::internal(error.to_string()))?
    {
        return Ok(());
    }

    let bucket_relation = match relation {
        "define" | "repair" => "manage_indexes",
        "query" => "query_indexes",
        other => other,
    };
    require_bucket_permission(storage, mvcc, claims, bucket, bucket_relation).await
}

pub async fn principal_has_any_system_realm_relation(
    _storage: &Storage,
    mvcc: &MvccSubsystem,
    principal_id: &str,
) -> Result<bool> {
    let revision = authz_journal::latest_authz_revision(mvcc, SYSTEM_STORAGE_TENANT_ID)?;
    let page = authz_journal::page_current_authz_tuples(
        mvcc,
        SYSTEM_STORAGE_TENANT_ID,
        &authz_journal::AuthzTupleFilter {
            subject_kind: Some(APP_SUBJECT_KIND.to_string()),
            subject_id: Some(principal_id.to_string()),
            caveat_hash: Some(String::new()),
            ..authz_journal::AuthzTupleFilter::default()
        },
        revision,
        None,
        1,
    )
    .await
    .map_err(|error| anyhow::anyhow!(error))?;
    Ok(!page.records.is_empty())
}

pub async fn grant_storage_tenant_owner(
    persistence: &Persistence,
    tenant_id: i64,
    principal_id: &str,
    written_by: &str,
    reason: &str,
) -> Result<()> {
    let tenant_object_id = storage_tenant_object_id(tenant_id);
    let default_authz_realm_object_id = authz_realm_object_id(tenant_id, DEFAULT_AUTHZ_REALM_ID);
    persistence
        .write_authz_tuple_batch(
            SYSTEM_STORAGE_TENANT_ID,
            vec![
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_STORAGE_TENANT_NAMESPACE),
                    object_id: tenant_object_id.clone(),
                    relation: "owner".to_string(),
                    subject_kind: APP_SUBJECT_KIND.to_string(),
                    subject_id: principal_id.to_string(),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_AUTHZ_REALM_NAMESPACE),
                    object_id: default_authz_realm_object_id.clone(),
                    relation: "parent_tenant".to_string(),
                    subject_kind: SYSTEM_STORAGE_TENANT_NAMESPACE.to_string(),
                    subject_id: tenant_object_id.clone(),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_AUTHZ_REALM_NAMESPACE),
                    object_id: default_authz_realm_object_id,
                    relation: "owner".to_string(),
                    subject_kind: APP_SUBJECT_KIND.to_string(),
                    subject_id: principal_id.to_string(),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
            ],
            written_by,
        )
        .await?;
    Ok(())
}

pub async fn grant_bucket_defaults(
    persistence: &Persistence,
    bucket: &Bucket,
    principal_id: &str,
    written_by: &str,
    reason: &str,
) -> Result<()> {
    let bucket_id = bucket_object_id(bucket);
    persistence
        .write_authz_tuple_batch(
            SYSTEM_STORAGE_TENANT_ID,
            vec![
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_BUCKET_NAMESPACE),
                    object_id: bucket_id.clone(),
                    relation: "parent_tenant".to_string(),
                    subject_kind: SYSTEM_STORAGE_TENANT_NAMESPACE.to_string(),
                    subject_id: storage_tenant_object_id(bucket.tenant_id),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_BUCKET_NAMESPACE),
                    object_id: bucket_id,
                    relation: "owner".to_string(),
                    subject_kind: APP_SUBJECT_KIND.to_string(),
                    subject_id: principal_id.to_string(),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
            ],
            written_by,
        )
        .await?;
    Ok(())
}

pub async fn stage_bucket_defaults(
    persistence: &Persistence,
    bucket: &Bucket,
    principal_id: &str,
    written_by: &str,
    reason: &str,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<()> {
    let bucket_id = bucket_object_id(bucket);
    persistence
        .stage_authz_tuple_batch(
            SYSTEM_STORAGE_TENANT_ID,
            vec![
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_BUCKET_NAMESPACE),
                    object_id: bucket_id.clone(),
                    relation: "parent_tenant".to_string(),
                    subject_kind: SYSTEM_STORAGE_TENANT_NAMESPACE.to_string(),
                    subject_id: storage_tenant_object_id(bucket.tenant_id),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_BUCKET_NAMESPACE),
                    object_id: bucket_id,
                    relation: "owner".to_string(),
                    subject_kind: APP_SUBJECT_KIND.to_string(),
                    subject_id: principal_id.to_string(),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
            ],
            written_by,
            transaction_id,
            transaction_principal,
            None,
        )
        .await?;
    Ok(())
}

pub async fn write_bucket_public_read_tuple(
    persistence: &Persistence,
    bucket: &Bucket,
    is_public_read: bool,
    written_by: &str,
    reason: &str,
) -> Result<()> {
    persistence
        .write_authz_tuple(
            SYSTEM_STORAGE_TENANT_ID,
            &system_realm_namespace(SYSTEM_BUCKET_NAMESPACE),
            &bucket_object_id(bucket),
            "reader",
            APP_SUBJECT_KIND,
            PUBLIC_APP_PRINCIPAL_ID,
            "",
            if is_public_read { "add" } else { "remove" },
            written_by,
            reason,
        )
        .await?;
    Ok(())
}

pub async fn stage_bucket_public_read_tuple(
    persistence: &Persistence,
    bucket: &Bucket,
    is_public_read: bool,
    written_by: &str,
    reason: &str,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<()> {
    persistence
        .stage_authz_tuple_batch(
            SYSTEM_STORAGE_TENANT_ID,
            vec![AuthzTupleBatchMutation {
                namespace: system_realm_namespace(SYSTEM_BUCKET_NAMESPACE),
                object_id: bucket_object_id(bucket),
                relation: "reader".to_string(),
                subject_kind: APP_SUBJECT_KIND.to_string(),
                subject_id: PUBLIC_APP_PRINCIPAL_ID.to_string(),
                caveat_hash: String::new(),
                operation: if is_public_read { "add" } else { "remove" }.to_string(),
                reason: reason.to_string(),
            }],
            written_by,
            transaction_id,
            transaction_principal,
            None,
        )
        .await?;
    Ok(())
}

pub async fn grant_index_defaults(
    persistence: &Persistence,
    bucket: &Bucket,
    index_name_or_id: &str,
    principal_id: &str,
    written_by: &str,
    reason: &str,
) -> Result<()> {
    persistence
        .write_authz_tuple_batch(
            SYSTEM_STORAGE_TENANT_ID,
            vec![
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_INDEX_NAMESPACE),
                    object_id: index_object_id(bucket, index_name_or_id),
                    relation: "parent_bucket".to_string(),
                    subject_kind: SYSTEM_BUCKET_NAMESPACE.to_string(),
                    subject_id: bucket_object_id(bucket),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_INDEX_NAMESPACE),
                    object_id: index_object_id(bucket, index_name_or_id),
                    relation: "owner".to_string(),
                    subject_kind: APP_SUBJECT_KIND.to_string(),
                    subject_id: principal_id.to_string(),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
            ],
            written_by,
        )
        .await?;
    Ok(())
}

pub async fn stage_index_defaults(
    persistence: &Persistence,
    bucket: &Bucket,
    index_name_or_id: &str,
    principal_id: &str,
    written_by: &str,
    reason: &str,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<()> {
    persistence
        .stage_authz_tuple_batch(
            SYSTEM_STORAGE_TENANT_ID,
            vec![
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_INDEX_NAMESPACE),
                    object_id: index_object_id(bucket, index_name_or_id),
                    relation: "parent_bucket".to_string(),
                    subject_kind: SYSTEM_BUCKET_NAMESPACE.to_string(),
                    subject_id: bucket_object_id(bucket),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_INDEX_NAMESPACE),
                    object_id: index_object_id(bucket, index_name_or_id),
                    relation: "owner".to_string(),
                    subject_kind: APP_SUBJECT_KIND.to_string(),
                    subject_id: principal_id.to_string(),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
            ],
            written_by,
            transaction_id,
            transaction_principal,
            None,
        )
        .await?;
    Ok(())
}

pub async fn grant_personaldb_group_defaults(
    persistence: &Persistence,
    tenant_id: i64,
    group_id: &str,
    principal_id: &str,
    written_by: &str,
    reason: &str,
) -> Result<()> {
    let object_id = personaldb_group_object_id(tenant_id, group_id);
    persistence
        .write_authz_tuple_batch(
            SYSTEM_STORAGE_TENANT_ID,
            vec![
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_PERSONALDB_GROUP_NAMESPACE),
                    object_id: object_id.clone(),
                    relation: "parent_tenant".to_string(),
                    subject_kind: SYSTEM_STORAGE_TENANT_NAMESPACE.to_string(),
                    subject_id: storage_tenant_object_id(tenant_id),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_PERSONALDB_GROUP_NAMESPACE),
                    object_id,
                    relation: "owner".to_string(),
                    subject_kind: APP_SUBJECT_KIND.to_string(),
                    subject_id: principal_id.to_string(),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
            ],
            written_by,
        )
        .await?;
    Ok(())
}

pub async fn grant_object_defaults(
    persistence: &Persistence,
    bucket: &Bucket,
    object_key: &str,
    reason: &str,
) -> Result<()> {
    persistence
        .write_authz_tuple(
            SYSTEM_STORAGE_TENANT_ID,
            &system_realm_namespace(SYSTEM_OBJECT_NAMESPACE),
            &object_object_id(bucket, object_key),
            "parent_bucket",
            SYSTEM_BUCKET_NAMESPACE,
            &bucket_object_id(bucket),
            "",
            "add",
            "system",
            reason,
        )
        .await?;
    Ok(())
}

pub async fn grant_object_defaults_batch<'a>(
    persistence: &Persistence,
    objects: impl IntoIterator<Item = (&'a Bucket, &'a str)>,
    reason: &str,
) -> Result<()> {
    let mutations = objects
        .into_iter()
        .map(|(bucket, object_key)| object_parent_bucket_mutation(bucket, object_key, reason))
        .collect::<Vec<_>>();
    if mutations.is_empty() {
        return Ok(());
    }
    persistence
        .write_authz_tuple_batch(SYSTEM_STORAGE_TENANT_ID, mutations, "system")
        .await?;
    Ok(())
}

pub(super) fn object_parent_bucket_mutation(
    bucket: &Bucket,
    object_key: &str,
    reason: &str,
) -> AuthzTupleBatchMutation {
    AuthzTupleBatchMutation {
        namespace: system_realm_namespace(SYSTEM_OBJECT_NAMESPACE),
        object_id: object_object_id(bucket, object_key),
        relation: "parent_bucket".to_string(),
        subject_kind: SYSTEM_BUCKET_NAMESPACE.to_string(),
        subject_id: bucket_object_id(bucket),
        caveat_hash: String::new(),
        operation: "add".to_string(),
        reason: reason.to_string(),
    }
}

pub async fn grant_stream_defaults(
    persistence: &Persistence,
    bucket: &Bucket,
    stream_key: &str,
    principal_id: &str,
    written_by: &str,
    reason: &str,
) -> Result<()> {
    let object_id = stream_object_id(bucket, stream_key);
    persistence
        .write_authz_tuple_batch(
            SYSTEM_STORAGE_TENANT_ID,
            vec![
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_STREAM_NAMESPACE),
                    object_id: object_id.clone(),
                    relation: "parent_bucket".to_string(),
                    subject_kind: SYSTEM_BUCKET_NAMESPACE.to_string(),
                    subject_id: bucket_object_id(bucket),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_STREAM_NAMESPACE),
                    object_id,
                    relation: "owner".to_string(),
                    subject_kind: APP_SUBJECT_KIND.to_string(),
                    subject_id: principal_id.to_string(),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
            ],
            written_by,
        )
        .await?;
    Ok(())
}

pub async fn stage_stream_defaults(
    persistence: &Persistence,
    bucket: &Bucket,
    stream_key: &str,
    principal_id: &str,
    written_by: &str,
    reason: &str,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<()> {
    let object_id = stream_object_id(bucket, stream_key);
    persistence
        .stage_authz_tuple_batch(
            SYSTEM_STORAGE_TENANT_ID,
            vec![
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_STREAM_NAMESPACE),
                    object_id: object_id.clone(),
                    relation: "parent_bucket".to_string(),
                    subject_kind: SYSTEM_BUCKET_NAMESPACE.to_string(),
                    subject_id: bucket_object_id(bucket),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_STREAM_NAMESPACE),
                    object_id,
                    relation: "owner".to_string(),
                    subject_kind: APP_SUBJECT_KIND.to_string(),
                    subject_id: principal_id.to_string(),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
            ],
            written_by,
            transaction_id,
            transaction_principal,
            None,
        )
        .await?;
    Ok(())
}

pub async fn grant_registry_namespace_defaults(
    persistence: &Persistence,
    tenant_id: i64,
    namespace: &str,
    principal_id: &str,
    written_by: &str,
    reason: &str,
) -> Result<()> {
    let object_id = registry_namespace_object_id(tenant_id, namespace);
    persistence
        .write_authz_tuple_batch(
            SYSTEM_STORAGE_TENANT_ID,
            vec![
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_REGISTRY_NAMESPACE),
                    object_id: object_id.clone(),
                    relation: "parent_tenant".to_string(),
                    subject_kind: SYSTEM_STORAGE_TENANT_NAMESPACE.to_string(),
                    subject_id: storage_tenant_object_id(tenant_id),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_REGISTRY_NAMESPACE),
                    object_id,
                    relation: "owner".to_string(),
                    subject_kind: APP_SUBJECT_KIND.to_string(),
                    subject_id: principal_id.to_string(),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
            ],
            written_by,
        )
        .await?;
    Ok(())
}

pub async fn grant_region_defaults(
    persistence: &Persistence,
    region: &str,
    written_by: &str,
    reason: &str,
) -> Result<()> {
    persistence
        .write_authz_tuple(
            SYSTEM_STORAGE_TENANT_ID,
            &system_realm_namespace(SYSTEM_REGION_NAMESPACE),
            &region_object_id(region),
            "system",
            crate::system_realm::SYSTEM_NAMESPACE,
            crate::system_realm::SYSTEM_OBJECT_ID,
            "",
            "add",
            written_by,
            reason,
        )
        .await?;
    Ok(())
}

pub async fn grant_cell_defaults(
    persistence: &Persistence,
    region: &str,
    cell_id: &str,
    written_by: &str,
    reason: &str,
) -> Result<()> {
    persistence
        .write_authz_tuple(
            SYSTEM_STORAGE_TENANT_ID,
            &system_realm_namespace(SYSTEM_CELL_NAMESPACE),
            &cell_object_id(region, cell_id),
            "parent_region",
            SYSTEM_REGION_NAMESPACE,
            &region_object_id(region),
            "",
            "add",
            written_by,
            reason,
        )
        .await?;
    Ok(())
}

pub async fn grant_node_defaults(
    persistence: &Persistence,
    region: &str,
    cell_id: &str,
    node_id: &str,
    written_by: &str,
    reason: &str,
) -> Result<()> {
    persistence
        .write_authz_tuple(
            SYSTEM_STORAGE_TENANT_ID,
            &system_realm_namespace(SYSTEM_NODE_NAMESPACE),
            &node_object_id(region, cell_id, node_id),
            "parent_cell",
            SYSTEM_CELL_NAMESPACE,
            &cell_object_id(region, cell_id),
            "",
            "add",
            written_by,
            reason,
        )
        .await?;
    Ok(())
}

pub async fn grant_internal_node_system_access(
    persistence: &Persistence,
    node_id: &str,
    written_by: &str,
    reason: &str,
) -> Result<()> {
    persistence
        .write_authz_tuple(
            SYSTEM_STORAGE_TENANT_ID,
            &system_realm_namespace(SYSTEM_NAMESPACE),
            SYSTEM_OBJECT_ID,
            "manage_nodes_grant",
            APP_SUBJECT_KIND,
            node_id,
            "",
            "add",
            written_by,
            reason,
        )
        .await?;
    Ok(())
}

pub async fn grant_node_defaults_batch(
    persistence: &Persistence,
    nodes: &[(String, String, String)],
    written_by: &str,
    reason: &str,
) -> Result<()> {
    let mut mutations = Vec::with_capacity(nodes.len() * 2);
    for (region, cell_id, node_id) in nodes {
        mutations.push(AuthzTupleBatchMutation {
            namespace: system_realm_namespace(SYSTEM_NODE_NAMESPACE),
            object_id: node_object_id(region, cell_id, node_id),
            relation: "parent_cell".to_string(),
            subject_kind: SYSTEM_CELL_NAMESPACE.to_string(),
            subject_id: cell_object_id(region, cell_id),
            caveat_hash: String::new(),
            operation: "add".to_string(),
            reason: reason.to_string(),
        });
        mutations.push(AuthzTupleBatchMutation {
            namespace: system_realm_namespace(SYSTEM_NAMESPACE),
            object_id: SYSTEM_OBJECT_ID.to_string(),
            relation: "manage_nodes_grant".to_string(),
            subject_kind: APP_SUBJECT_KIND.to_string(),
            subject_id: node_id.clone(),
            caveat_hash: String::new(),
            operation: "add".to_string(),
            reason: reason.to_string(),
        });
    }
    if !mutations.is_empty() {
        persistence
            .write_authz_tuple_batch(SYSTEM_STORAGE_TENANT_ID, mutations, written_by)
            .await?;
    }
    Ok(())
}

pub async fn grant_authz_realm_defaults(
    persistence: &Persistence,
    tenant_id: i64,
    realm_id: &str,
    principal_id: &str,
    written_by: &str,
    reason: &str,
) -> Result<()> {
    let object_id = authz_realm_object_id(tenant_id, realm_id);
    persistence
        .write_authz_tuple_batch(
            SYSTEM_STORAGE_TENANT_ID,
            vec![
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_AUTHZ_REALM_NAMESPACE),
                    object_id: object_id.clone(),
                    relation: "parent_tenant".to_string(),
                    subject_kind: SYSTEM_STORAGE_TENANT_NAMESPACE.to_string(),
                    subject_id: storage_tenant_object_id(tenant_id),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_AUTHZ_REALM_NAMESPACE),
                    object_id,
                    relation: "owner".to_string(),
                    subject_kind: APP_SUBJECT_KIND.to_string(),
                    subject_id: principal_id.to_string(),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
            ],
            written_by,
        )
        .await?;
    Ok(())
}

pub async fn stage_authz_realm_defaults(
    persistence: &Persistence,
    tenant_id: i64,
    realm_id: &str,
    principal_id: &str,
    written_by: &str,
    reason: &str,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<()> {
    let object_id = authz_realm_object_id(tenant_id, realm_id);
    persistence
        .stage_authz_tuple_batch(
            SYSTEM_STORAGE_TENANT_ID,
            vec![
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_AUTHZ_REALM_NAMESPACE),
                    object_id: object_id.clone(),
                    relation: "parent_tenant".to_string(),
                    subject_kind: SYSTEM_STORAGE_TENANT_NAMESPACE.to_string(),
                    subject_id: storage_tenant_object_id(tenant_id),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
                AuthzTupleBatchMutation {
                    namespace: system_realm_namespace(SYSTEM_AUTHZ_REALM_NAMESPACE),
                    object_id,
                    relation: "owner".to_string(),
                    subject_kind: APP_SUBJECT_KIND.to_string(),
                    subject_id: principal_id.to_string(),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: reason.to_string(),
                },
            ],
            written_by,
            transaction_id,
            transaction_principal,
            None,
        )
        .await?;
    Ok(())
}
