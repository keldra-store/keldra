use crate::{
    auth, authz_journal,
    authz_scope::{DEFAULT_AUTHZ_REALM_ID, encode_realm_namespace},
    bucket_journal,
    permissions::AnvilAction,
    persistence::{AuthzTupleBatchMutation, Bucket, Persistence},
    storage::Storage,
    system_realm::{
        SYSTEM_AUTHZ_REALM_NAMESPACE, SYSTEM_BUCKET_NAMESPACE, SYSTEM_CELL_NAMESPACE,
        SYSTEM_INDEX_NAMESPACE, SYSTEM_NAMESPACE, SYSTEM_NODE_NAMESPACE, SYSTEM_OBJECT_ID,
        SYSTEM_OBJECT_NAMESPACE, SYSTEM_PERSONALDB_GROUP_NAMESPACE, SYSTEM_REALM_ID,
        SYSTEM_REGION_NAMESPACE, SYSTEM_REGISTRY_NAMESPACE, SYSTEM_STORAGE_TENANT_ID,
        SYSTEM_STORAGE_TENANT_NAMESPACE, SYSTEM_STREAM_NAMESPACE,
    },
};
use anyhow::Result;
use tonic::Status;

pub const APP_SUBJECT_KIND: &str = "app";
pub const USERSET_SUBJECT_KIND: &str = "userset";
pub const PUBLIC_APP_PRINCIPAL_ID: &str = crate::authz_schema_contract::PUBLIC_SUBJECT_ID;

pub fn public_read_claims(tenant_id: i64) -> auth::Claims {
    auth::Claims {
        sub: PUBLIC_APP_PRINCIPAL_ID.to_string(),
        exp: usize::MAX,
        tenant_id,
        jti: None,
    }
}

pub fn system_realm_namespace(namespace: &str) -> String {
    encode_realm_namespace(SYSTEM_REALM_ID, namespace)
}

fn split_bucket_key(resource: &str) -> (&str, Option<&str>) {
    let resource = resource.trim_end_matches('/');
    match resource.split_once('/') {
        Some((bucket, key)) if !bucket.is_empty() && !key.is_empty() => (bucket, Some(key)),
        _ => (resource, None),
    }
}

fn registry_namespace_resource(resource: &str) -> &str {
    let mut parts = resource.split('/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some("registry"), Some(kind), Some(namespace))
            if !kind.is_empty() && !namespace.is_empty() =>
        {
            &resource[..("registry/".len() + kind.len() + 1 + namespace.len())]
        }
        _ => resource,
    }
}

fn authz_runtime_relation_for_action(action: AnvilAction, resource: &str) -> Option<&'static str> {
    match action {
        AnvilAction::AuthzTupleWrite => Some("write_tuples"),
        AnvilAction::AuthzTupleRead | AnvilAction::AuthzWatch => Some("list"),
        AnvilAction::AuthzCheck => Some("check"),
        AnvilAction::AuthzSchemaRead if resource.starts_with("schema:") => None,
        AnvilAction::AuthzSchemaRead => Some("list"),
        AnvilAction::AuthzSchemaWrite if resource.starts_with("schema:") => None,
        AnvilAction::AuthzSchemaWrite => Some("put_schema"),
        _ => None,
    }
}

async fn read_claims_bucket(
    storage: &Storage,
    claims: &auth::Claims,
    bucket_name: &str,
) -> Result<Bucket, Status> {
    bucket_journal::read_current_bucket(storage, claims.tenant_id, bucket_name)
        .await
        .map_err(|error| Status::internal(error.to_string()))?
        .ok_or_else(|| Status::not_found("Bucket not found"))
}

pub async fn action_allows(
    storage: &Storage,
    _persistence: &Persistence,
    claims: &auth::Claims,
    action: AnvilAction,
    resource: &str,
) -> Result<bool, Status> {
    let result = match action {
        AnvilAction::TenantManage => {
            system_realm_relationship_allows(
                storage,
                claims,
                SYSTEM_STORAGE_TENANT_NAMESPACE,
                &storage_tenant_object_id(claims.tenant_id),
                "manage_tenant",
                None,
            )
            .await
        }
        AnvilAction::BucketCreate => {
            system_realm_relationship_allows(
                storage,
                claims,
                SYSTEM_STORAGE_TENANT_NAMESPACE,
                &storage_tenant_object_id(claims.tenant_id),
                "create_bucket",
                None,
            )
            .await
        }
        AnvilAction::BucketList | AnvilAction::BucketWatch => {
            system_realm_relationship_allows(
                storage,
                claims,
                SYSTEM_STORAGE_TENANT_NAMESPACE,
                &storage_tenant_object_id(claims.tenant_id),
                "list_buckets",
                None,
            )
            .await
        }
        AnvilAction::BucketRead | AnvilAction::BucketWrite | AnvilAction::BucketDelete => {
            let bucket = read_claims_bucket(storage, claims, resource).await?;
            let relation = match action {
                AnvilAction::BucketRead => "list_objects",
                AnvilAction::BucketWrite | AnvilAction::BucketDelete => "manage_bucket",
                _ => unreachable!(),
            };
            system_realm_relationship_allows(
                storage,
                claims,
                SYSTEM_BUCKET_NAMESPACE,
                &bucket_object_id(&bucket),
                relation,
                None,
            )
            .await
        }

        AnvilAction::ObjectList => {
            let (bucket_name, _) = split_bucket_key(resource);
            let bucket = read_claims_bucket(storage, claims, bucket_name).await?;
            system_realm_relationship_allows(
                storage,
                claims,
                SYSTEM_BUCKET_NAMESPACE,
                &bucket_object_id(&bucket),
                "list_objects",
                None,
            )
            .await
        }
        AnvilAction::ObjectRead | AnvilAction::ObjectWrite | AnvilAction::ObjectDelete => {
            let (bucket_name, key) = split_bucket_key(resource);
            let bucket = read_claims_bucket(storage, claims, bucket_name).await?;
            let relation = match action {
                AnvilAction::ObjectRead => "get",
                AnvilAction::ObjectWrite => "put",
                AnvilAction::ObjectDelete => "delete",
                _ => unreachable!(),
            };
            if let Some(key) = key {
                return Ok(system_realm_relationship_allows(
                    storage,
                    claims,
                    SYSTEM_OBJECT_NAMESPACE,
                    &object_object_id(&bucket, key),
                    relation,
                    None,
                )
                .await
                .map_err(|error| Status::internal(error.to_string()))?
                    || {
                        let bucket_relation = match action {
                            AnvilAction::ObjectRead => "get_object",
                            AnvilAction::ObjectWrite => "put_object",
                            AnvilAction::ObjectDelete => "delete_object",
                            _ => unreachable!(),
                        };
                        system_realm_relationship_allows(
                            storage,
                            claims,
                            SYSTEM_BUCKET_NAMESPACE,
                            &bucket_object_id(&bucket),
                            bucket_relation,
                            None,
                        )
                        .await
                        .map_err(|error| Status::internal(error.to_string()))?
                    });
            }
            let bucket_relation = match action {
                AnvilAction::ObjectRead => "get_object",
                AnvilAction::ObjectWrite => "put_object",
                AnvilAction::ObjectDelete => "delete_object",
                _ => unreachable!(),
            };
            system_realm_relationship_allows(
                storage,
                claims,
                SYSTEM_BUCKET_NAMESPACE,
                &bucket_object_id(&bucket),
                bucket_relation,
                None,
            )
            .await
        }

        AnvilAction::StreamCreate => {
            let (bucket_name, _) = split_bucket_key(resource);
            let bucket = read_claims_bucket(storage, claims, bucket_name).await?;
            system_realm_relationship_allows(
                storage,
                claims,
                SYSTEM_BUCKET_NAMESPACE,
                &bucket_object_id(&bucket),
                "put_object",
                None,
            )
            .await
        }
        AnvilAction::StreamAppend | AnvilAction::StreamRead | AnvilAction::StreamSealSegment => {
            let (bucket_name, stream_key) = split_bucket_key(resource);
            let stream_key = stream_key.ok_or_else(|| {
                Status::invalid_argument("stream action resource must be bucket/stream")
            })?;
            let bucket = read_claims_bucket(storage, claims, bucket_name).await?;
            let relation = match action {
                AnvilAction::StreamAppend => "append",
                AnvilAction::StreamRead => "read",
                AnvilAction::StreamSealSegment => "seal_segment",
                _ => unreachable!(),
            };
            system_realm_relationship_allows(
                storage,
                claims,
                SYSTEM_STREAM_NAMESPACE,
                &stream_object_id(&bucket, stream_key),
                relation,
                None,
            )
            .await
        }

        AnvilAction::IndexCreate
        | AnvilAction::IndexUpdate
        | AnvilAction::IndexDelete
        | AnvilAction::IndexRead
        | AnvilAction::IndexWatch => {
            let (bucket_name, index_name) = split_bucket_key(resource);
            let bucket = read_claims_bucket(storage, claims, bucket_name).await?;
            let relation = match action {
                AnvilAction::IndexCreate | AnvilAction::IndexUpdate | AnvilAction::IndexDelete => {
                    "define"
                }
                AnvilAction::IndexRead | AnvilAction::IndexWatch => "query",
                _ => unreachable!(),
            };
            if let Some(index_name) = index_name {
                return Ok(system_realm_relationship_allows(
                    storage,
                    claims,
                    SYSTEM_INDEX_NAMESPACE,
                    &index_object_id(&bucket, index_name),
                    relation,
                    None,
                )
                .await
                .map_err(|error| Status::internal(error.to_string()))?
                    || {
                        let bucket_relation = match action {
                            AnvilAction::IndexCreate
                            | AnvilAction::IndexUpdate
                            | AnvilAction::IndexDelete => "manage_indexes",
                            AnvilAction::IndexRead | AnvilAction::IndexWatch => "query_indexes",
                            _ => unreachable!(),
                        };
                        system_realm_relationship_allows(
                            storage,
                            claims,
                            SYSTEM_BUCKET_NAMESPACE,
                            &bucket_object_id(&bucket),
                            bucket_relation,
                            None,
                        )
                        .await
                        .map_err(|error| Status::internal(error.to_string()))?
                    });
            }
            let bucket_relation = match action {
                AnvilAction::IndexCreate | AnvilAction::IndexUpdate | AnvilAction::IndexDelete => {
                    "manage_indexes"
                }
                AnvilAction::IndexRead | AnvilAction::IndexWatch => "query_indexes",
                _ => unreachable!(),
            };
            system_realm_relationship_allows(
                storage,
                claims,
                SYSTEM_BUCKET_NAMESPACE,
                &bucket_object_id(&bucket),
                bucket_relation,
                None,
            )
            .await
        }

        AnvilAction::AuthzTupleWrite
        | AnvilAction::AuthzSchemaWrite
        | AnvilAction::AuthzTupleRead
        | AnvilAction::AuthzCheck
        | AnvilAction::AuthzWatch
        | AnvilAction::AuthzSchemaRead => {
            if let Some(relation) = authz_runtime_relation_for_action(action.clone(), resource) {
                system_realm_relationship_allows(
                    storage,
                    claims,
                    SYSTEM_AUTHZ_REALM_NAMESPACE,
                    &authz_realm_object_id(claims.tenant_id, resource),
                    relation,
                    None,
                )
                .await
            } else {
                let tenant_relation = if matches!(action, AnvilAction::AuthzSchemaRead) {
                    "read_tenant"
                } else {
                    "manage_tenant"
                };
                system_realm_relationship_allows(
                    storage,
                    claims,
                    SYSTEM_STORAGE_TENANT_NAMESPACE,
                    &storage_tenant_object_id(claims.tenant_id),
                    tenant_relation,
                    None,
                )
                .await
            }
        }

        AnvilAction::PolicyRead | AnvilAction::PolicyGrant | AnvilAction::PolicyRevoke => {
            let relation = match action {
                AnvilAction::PolicyRead => "read_access_grants",
                AnvilAction::PolicyGrant => "grant_access",
                AnvilAction::PolicyRevoke => "revoke_access",
                _ => unreachable!(),
            };
            system_realm_relationship_allows(
                storage,
                claims,
                SYSTEM_STORAGE_TENANT_NAMESPACE,
                &storage_tenant_object_id(claims.tenant_id),
                relation,
                None,
            )
            .await
        }

        AnvilAction::AppCreate
        | AnvilAction::AppRotateSecret
        | AnvilAction::AppDelete
        | AnvilAction::HfKeyCreate
        | AnvilAction::HfKeyDelete
        | AnvilAction::HfIngestionCreate
        | AnvilAction::HfIngestionDelete
        | AnvilAction::GitSourceWrite => {
            system_realm_relationship_allows(
                storage,
                claims,
                SYSTEM_STORAGE_TENANT_NAMESPACE,
                &storage_tenant_object_id(claims.tenant_id),
                "manage_tenant",
                None,
            )
            .await
        }
        AnvilAction::AppRead
        | AnvilAction::HfKeyRead
        | AnvilAction::HfKeyList
        | AnvilAction::HfIngestionRead
        | AnvilAction::GitSourceRead
        | AnvilAction::GitSourceWatch => {
            system_realm_relationship_allows(
                storage,
                claims,
                SYSTEM_STORAGE_TENANT_NAMESPACE,
                &storage_tenant_object_id(claims.tenant_id),
                "read_tenant",
                None,
            )
            .await
        }

        AnvilAction::PersonalDbCreate => {
            system_realm_relationship_allows(
                storage,
                claims,
                SYSTEM_STORAGE_TENANT_NAMESPACE,
                &storage_tenant_object_id(claims.tenant_id),
                "manage_tenant",
                None,
            )
            .await
        }
        AnvilAction::PersonalDbRead | AnvilAction::PersonalDbWatch => {
            system_realm_relationship_allows(
                storage,
                claims,
                SYSTEM_PERSONALDB_GROUP_NAMESPACE,
                &personaldb_group_object_id(claims.tenant_id, resource),
                if matches!(action, AnvilAction::PersonalDbWatch) {
                    "watch"
                } else {
                    "get_snapshot"
                },
                None,
            )
            .await
        }
        AnvilAction::PersonalDbCommit
        | AnvilAction::PersonalDbInsert
        | AnvilAction::PersonalDbUpdate
        | AnvilAction::PersonalDbDelete => {
            system_realm_relationship_allows(
                storage,
                claims,
                SYSTEM_PERSONALDB_GROUP_NAMESPACE,
                &personaldb_group_object_id(claims.tenant_id, resource),
                "apply_changeset",
                None,
            )
            .await
        }

        AnvilAction::RegistryBlobWrite
        | AnvilAction::RegistryVersionWrite
        | AnvilAction::RegistryRefWrite => Ok(system_realm_relationship_allows(
            storage,
            claims,
            SYSTEM_REGISTRY_NAMESPACE,
            &registry_namespace_object_id(claims.tenant_id, registry_namespace_resource(resource)),
            "publish",
            None,
        )
        .await
        .map_err(|error| Status::internal(error.to_string()))?
            || system_realm_relationship_allows(
                storage,
                claims,
                SYSTEM_STORAGE_TENANT_NAMESPACE,
                &storage_tenant_object_id(claims.tenant_id),
                "manage_tenant",
                None,
            )
            .await
            .map_err(|error| Status::internal(error.to_string()))?),
        AnvilAction::RegistryRead | AnvilAction::RegistryList => {
            Ok(system_realm_relationship_allows(
                storage,
                claims,
                SYSTEM_REGISTRY_NAMESPACE,
                &registry_namespace_object_id(
                    claims.tenant_id,
                    registry_namespace_resource(resource),
                ),
                "read",
                None,
            )
            .await
            .map_err(|error| Status::internal(error.to_string()))?
                || system_realm_relationship_allows(
                    storage,
                    claims,
                    SYSTEM_STORAGE_TENANT_NAMESPACE,
                    &storage_tenant_object_id(claims.tenant_id),
                    "read_tenant",
                    None,
                )
                .await
                .map_err(|error| Status::internal(error.to_string()))?)
        }

        AnvilAction::MeshManage | AnvilAction::InternalProxyObject => {
            crate::system_realm::check_admin_relation(
                storage,
                "default",
                claims,
                crate::system_realm::SystemAdminRelation::ManageSystem,
            )
            .await
        }
        AnvilAction::MeshRead => {
            crate::system_realm::check_admin_relation(
                storage,
                "default",
                claims,
                crate::system_realm::SystemAdminRelation::ViewSystem,
            )
            .await
        }
        AnvilAction::RepairRun | AnvilAction::RepairRead => {
            let relation = if matches!(action, AnvilAction::RepairRun) {
                "manage_tenant"
            } else {
                "read_tenant"
            };
            system_realm_relationship_allows(
                storage,
                claims,
                SYSTEM_STORAGE_TENANT_NAMESPACE,
                &storage_tenant_object_id(claims.tenant_id),
                relation,
                None,
            )
            .await
        }
        AnvilAction::CoordinationLeaseRead
        | AnvilAction::CoordinationLeaseWrite
        | AnvilAction::CoordinationLeaseAdmin => {
            let relation = match action {
                AnvilAction::CoordinationLeaseRead => "lease_read",
                AnvilAction::CoordinationLeaseWrite => "lease_write",
                AnvilAction::CoordinationLeaseAdmin => "lease_admin",
                _ => unreachable!(),
            };
            system_realm_relationship_allows(
                storage,
                claims,
                SYSTEM_STORAGE_TENANT_NAMESPACE,
                &storage_tenant_object_id(claims.tenant_id),
                relation,
                None,
            )
            .await
        }
    }
    .map_err(|error| Status::internal(error.to_string()))?;
    Ok(result)
}

pub async fn require_action(
    storage: &Storage,
    persistence: &Persistence,
    claims: &auth::Claims,
    action: AnvilAction,
    resource: &str,
) -> Result<(), Status> {
    if action_allows(storage, persistence, claims, action, resource).await? {
        Ok(())
    } else {
        Err(Status::permission_denied("Permission denied"))
    }
}

pub fn storage_tenant_object_id(tenant_id: i64) -> String {
    tenant_id.to_string()
}

pub fn bucket_object_id(bucket: &Bucket) -> String {
    bucket.id.to_string()
}

pub fn object_object_id(bucket: &Bucket, object_key: &str) -> String {
    format!("{}/{}", bucket.id, object_key)
}

pub fn stream_object_id(bucket: &Bucket, stream_key: &str) -> String {
    format!("{}/{}", bucket.id, stream_key)
}

pub fn index_object_id(bucket: &Bucket, index_name_or_id: &str) -> String {
    format!("{}/{}", bucket.id, index_name_or_id)
}

pub fn authz_realm_object_id(tenant_id: i64, realm_id: &str) -> String {
    format!("{tenant_id}/{realm_id}")
}

pub fn registry_namespace_object_id(tenant_id: i64, namespace: &str) -> String {
    format!("{tenant_id}/{namespace}")
}

pub fn personaldb_group_object_id(tenant_id: i64, group_id: &str) -> String {
    format!("{tenant_id}/{group_id}")
}

pub fn region_object_id(region: &str) -> String {
    region.to_string()
}

pub fn cell_object_id(region: &str, cell_id: &str) -> String {
    format!("{region}/{cell_id}")
}

pub fn node_object_id(region: &str, cell_id: &str, node_id: &str) -> String {
    format!("{region}/{cell_id}/{node_id}")
}

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
    storage: &Storage,
    tenant_id: i64,
    bucket_name: &str,
) -> Result<Bucket, Status> {
    bucket_journal::read_current_bucket(storage, tenant_id, bucket_name)
        .await
        .map_err(|error| Status::internal(error.to_string()))?
        .ok_or_else(|| Status::not_found("Bucket not found"))
}

pub async fn delegated_relation_for_action(
    storage: &Storage,
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
        | AnvilAction::InternalProxyObject => {
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
            let bucket = read_bucket_for_tenant(storage, tenant_id, &resource).await?;
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
            let bucket = read_bucket_for_tenant(storage, tenant_id, bucket_name).await?;
            Ok(DelegatedSystemRelation {
                namespace: system_realm_namespace(SYSTEM_BUCKET_NAMESPACE),
                object_id: bucket_object_id(&bucket),
                relation: "list_objects".to_string(),
            })
        }
        AnvilAction::ObjectRead | AnvilAction::ObjectWrite | AnvilAction::ObjectDelete => {
            let (bucket_name, key) = split_bucket_key(&resource);
            let bucket = read_bucket_for_tenant(storage, tenant_id, bucket_name).await?;
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
        | AnvilAction::IndexRead
        | AnvilAction::IndexWatch => {
            let (bucket_name, index_name) = split_bucket_key(&resource);
            let bucket = read_bucket_for_tenant(storage, tenant_id, bucket_name).await?;
            if let Some(index_name) = index_name {
                Ok(DelegatedSystemRelation {
                    namespace: system_realm_namespace(SYSTEM_INDEX_NAMESPACE),
                    object_id: index_object_id(&bucket, index_name),
                    relation: match action {
                        AnvilAction::IndexCreate
                        | AnvilAction::IndexUpdate
                        | AnvilAction::IndexDelete => "define",
                        AnvilAction::IndexRead | AnvilAction::IndexWatch => "query",
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
                        AnvilAction::IndexRead | AnvilAction::IndexWatch => "query_indexes",
                        _ => unreachable!(),
                    }
                    .to_string(),
                })
            }
        }

        AnvilAction::StreamCreate => {
            let (bucket_name, _) = split_bucket_key(&resource);
            let bucket = read_bucket_for_tenant(storage, tenant_id, bucket_name).await?;
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
            let bucket = read_bucket_for_tenant(storage, tenant_id, bucket_name).await?;
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
        | AnvilAction::HfIngestionDelete
        | AnvilAction::GitSourceWrite
        | AnvilAction::GitSourceRead
        | AnvilAction::GitSourceWatch => Ok(DelegatedSystemRelation {
            namespace: system_realm_namespace(SYSTEM_STORAGE_TENANT_NAMESPACE),
            object_id: storage_tenant_object_id(tenant_id),
            relation: if matches!(
                action,
                AnvilAction::GitSourceRead
                    | AnvilAction::GitSourceWatch
                    | AnvilAction::HfIngestionRead
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
) -> Result<(), Status> {
    let relation =
        delegated_relation_for_action(storage, tenant_id, action.clone(), resource).await?;
    let assignment_relation = delegated_assignment_relation(&action, &relation);
    let record = persistence
        .write_authz_tuple(
            SYSTEM_STORAGE_TENANT_ID,
            &relation.namespace,
            &relation.object_id,
            &assignment_relation,
            APP_SUBJECT_KIND,
            grantee_principal_id,
            "",
            operation,
            written_by,
            reason,
        )
        .await
        .map_err(authz_tuple_write_status)?;
    persistence
        .materialize_authz_through_revision(SYSTEM_STORAGE_TENANT_ID, record.revision)
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
        let relation =
            delegated_relation_for_action(storage, tenant_id, action.clone(), resource).await?;
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
        .write_authz_tuple_batch(SYSTEM_STORAGE_TENANT_ID, mutations, written_by)
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

fn delegated_assignment_relation(
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
    claims: &auth::Claims,
    namespace: &str,
    object_id: &str,
    relation: &str,
) -> Result<(), Status> {
    if system_realm_relationship_allows(storage, claims, namespace, object_id, relation, None)
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
    claims: &auth::Claims,
    relation: &str,
) -> Result<(), Status> {
    require_system_realm_permission(
        storage,
        claims,
        SYSTEM_STORAGE_TENANT_NAMESPACE,
        &storage_tenant_object_id(claims.tenant_id),
        relation,
    )
    .await
}

pub async fn require_bucket_permission(
    storage: &Storage,
    claims: &auth::Claims,
    bucket: &Bucket,
    relation: &str,
) -> Result<(), Status> {
    require_system_realm_permission(
        storage,
        claims,
        SYSTEM_BUCKET_NAMESPACE,
        &bucket_object_id(bucket),
        relation,
    )
    .await
}

pub async fn require_object_permission(
    storage: &Storage,
    claims: &auth::Claims,
    bucket: &Bucket,
    object_key: &str,
    relation: &str,
) -> Result<(), Status> {
    if system_realm_relationship_allows(
        storage,
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
    require_bucket_permission(storage, claims, bucket, bucket_relation).await
}

pub async fn require_index_permission(
    storage: &Storage,
    claims: &auth::Claims,
    bucket: &Bucket,
    index_name_or_id: &str,
    relation: &str,
) -> Result<(), Status> {
    if system_realm_relationship_allows(
        storage,
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
    require_bucket_permission(storage, claims, bucket, bucket_relation).await
}

pub async fn principal_has_any_system_realm_relation(
    storage: &Storage,
    principal_id: &str,
) -> Result<bool> {
    let revision = authz_journal::latest_authz_revision(storage, SYSTEM_STORAGE_TENANT_ID).await?;
    let page = authz_journal::page_current_authz_tuples(
        storage,
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

fn object_parent_bucket_mutation(
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

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::{
        DelegatedSystemRelation, SYSTEM_BUCKET_NAMESPACE, USERSET_SUBJECT_KIND,
        delegated_assignment_relation, object_parent_bucket_mutation, split_bucket_key,
    };
    use crate::{permissions::AnvilAction, persistence::Bucket};

    #[test]
    fn split_bucket_key_treats_empty_prefix_as_bucket_scope() {
        assert_eq!(split_bucket_key("photos"), ("photos", None));
        assert_eq!(split_bucket_key("photos/"), ("photos", None));
        assert_eq!(split_bucket_key("photos///"), ("photos", None));
        assert_eq!(
            split_bucket_key("photos/2026/report.txt"),
            ("photos", Some("2026/report.txt"))
        );
    }

    #[test]
    fn batched_object_defaults_reference_the_parent_bucket_directly() {
        let bucket = Bucket {
            id: 17,
            tenant_id: 9,
            name: "workspace".to_string(),
            region: "test-region".to_string(),
            created_at: Utc::now(),
            is_public_read: false,
        };

        let mutation = object_parent_bucket_mutation(&bucket, "devices/capability.json", "test");

        assert_eq!(mutation.relation, "parent_bucket");
        assert_eq!(mutation.subject_kind, SYSTEM_BUCKET_NAMESPACE);
        assert_eq!(mutation.subject_id, "17");
        assert_ne!(mutation.subject_kind, USERSET_SUBJECT_KIND);
    }

    #[test]
    fn delegated_authz_roles_are_assigned_directly() {
        let relation = DelegatedSystemRelation {
            namespace: "system/authz_realm".to_string(),
            object_id: "7/default".to_string(),
            relation: "tuple_writer".to_string(),
        };

        assert_eq!(
            delegated_assignment_relation(&AnvilAction::AuthzTupleWrite, &relation),
            "tuple_writer"
        );
    }

    #[test]
    fn delegated_permissions_use_their_generated_grant_relation() {
        let relation = DelegatedSystemRelation {
            namespace: "system/storage_tenant".to_string(),
            object_id: "7".to_string(),
            relation: "manage_tenant".to_string(),
        };

        assert_eq!(
            delegated_assignment_relation(&AnvilAction::TenantManage, &relation),
            "manage_tenant_grant"
        );
    }
}
