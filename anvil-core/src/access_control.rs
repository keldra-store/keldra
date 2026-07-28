use crate::{
    auth, authz_journal,
    authz_scope::{DEFAULT_AUTHZ_REALM_ID, encode_realm_namespace},
    bucket_journal,
    mvcc_bootstrap::MvccSubsystem,
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
    persistence: &Persistence,
    claims: &auth::Claims,
    bucket_name: &str,
) -> Result<Bucket, Status> {
    bucket_journal::read_current_bucket_mvcc(
        persistence
            .mvcc()
            .map_err(|error| Status::internal(error.to_string()))?,
        claims.tenant_id,
        bucket_name,
    )
    .map_err(|error| Status::internal(error.to_string()))?
    .ok_or_else(|| Status::not_found("Bucket not found"))
}

pub async fn action_allows(
    storage: &Storage,
    persistence: &Persistence,
    claims: &auth::Claims,
    action: AnvilAction,
    resource: &str,
) -> Result<bool, Status> {
    let result = match action {
        AnvilAction::TenantManage => {
            system_realm_relationship_allows(
                storage,
                persistence
                    .mvcc()
                    .map_err(|error| Status::internal(error.to_string()))?,
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
                persistence
                    .mvcc()
                    .map_err(|error| Status::internal(error.to_string()))?,
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
                persistence
                    .mvcc()
                    .map_err(|error| Status::internal(error.to_string()))?,
                claims,
                SYSTEM_STORAGE_TENANT_NAMESPACE,
                &storage_tenant_object_id(claims.tenant_id),
                "list_buckets",
                None,
            )
            .await
        }
        AnvilAction::BucketRead | AnvilAction::BucketWrite | AnvilAction::BucketDelete => {
            let bucket = read_claims_bucket(persistence, claims, resource).await?;
            let relation = match action {
                AnvilAction::BucketRead => "list_objects",
                AnvilAction::BucketWrite | AnvilAction::BucketDelete => "manage_bucket",
                _ => unreachable!(),
            };
            system_realm_relationship_allows(
                storage,
                persistence
                    .mvcc()
                    .map_err(|error| Status::internal(error.to_string()))?,
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
            let bucket = read_claims_bucket(persistence, claims, bucket_name).await?;
            system_realm_relationship_allows(
                storage,
                persistence
                    .mvcc()
                    .map_err(|error| Status::internal(error.to_string()))?,
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
            let bucket = read_claims_bucket(persistence, claims, bucket_name).await?;
            let relation = match action {
                AnvilAction::ObjectRead => "get",
                AnvilAction::ObjectWrite => "put",
                AnvilAction::ObjectDelete => "delete",
                _ => unreachable!(),
            };
            if let Some(key) = key {
                return Ok(system_realm_relationship_allows(
                    storage,
                    persistence
                        .mvcc()
                        .map_err(|error| Status::internal(error.to_string()))?,
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
                            persistence
                                .mvcc()
                                .map_err(|error| Status::internal(error.to_string()))?,
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
                persistence
                    .mvcc()
                    .map_err(|error| Status::internal(error.to_string()))?,
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
            let bucket = read_claims_bucket(persistence, claims, bucket_name).await?;
            system_realm_relationship_allows(
                storage,
                persistence
                    .mvcc()
                    .map_err(|error| Status::internal(error.to_string()))?,
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
            let bucket = read_claims_bucket(persistence, claims, bucket_name).await?;
            let relation = match action {
                AnvilAction::StreamAppend => "append",
                AnvilAction::StreamRead => "read",
                AnvilAction::StreamSealSegment => "seal_segment",
                _ => unreachable!(),
            };
            system_realm_relationship_allows(
                storage,
                persistence
                    .mvcc()
                    .map_err(|error| Status::internal(error.to_string()))?,
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
            let bucket = read_claims_bucket(persistence, claims, bucket_name).await?;
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
                    persistence
                        .mvcc()
                        .map_err(|error| Status::internal(error.to_string()))?,
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
                            persistence
                                .mvcc()
                                .map_err(|error| Status::internal(error.to_string()))?,
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
                persistence
                    .mvcc()
                    .map_err(|error| Status::internal(error.to_string()))?,
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
                    persistence
                        .mvcc()
                        .map_err(|error| Status::internal(error.to_string()))?,
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
                    persistence
                        .mvcc()
                        .map_err(|error| Status::internal(error.to_string()))?,
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
                persistence
                    .mvcc()
                    .map_err(|error| Status::internal(error.to_string()))?,
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
                persistence
                    .mvcc()
                    .map_err(|error| Status::internal(error.to_string()))?,
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
                persistence
                    .mvcc()
                    .map_err(|error| Status::internal(error.to_string()))?,
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
                persistence
                    .mvcc()
                    .map_err(|error| Status::internal(error.to_string()))?,
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
                persistence
                    .mvcc()
                    .map_err(|error| Status::internal(error.to_string()))?,
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
                persistence
                    .mvcc()
                    .map_err(|error| Status::internal(error.to_string()))?,
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
            persistence
                .mvcc()
                .map_err(|error| Status::internal(error.to_string()))?,
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
                persistence
                    .mvcc()
                    .map_err(|error| Status::internal(error.to_string()))?,
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
                persistence
                    .mvcc()
                    .map_err(|error| Status::internal(error.to_string()))?,
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
                    persistence
                        .mvcc()
                        .map_err(|error| Status::internal(error.to_string()))?,
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
                persistence
                    .mvcc()
                    .map_err(|error| Status::internal(error.to_string()))?,
                "default",
                claims,
                crate::system_realm::SystemAdminRelation::ManageSystem,
            )
            .await
        }
        AnvilAction::MeshRead => {
            crate::system_realm::check_admin_relation(
                storage,
                persistence
                    .mvcc()
                    .map_err(|error| Status::internal(error.to_string()))?,
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
                persistence
                    .mvcc()
                    .map_err(|error| Status::internal(error.to_string()))?,
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
                persistence
                    .mvcc()
                    .map_err(|error| Status::internal(error.to_string()))?,
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

mod delegation;
pub use delegation::*;
use delegation::{delegated_assignment_relation, object_parent_bucket_mutation};
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
