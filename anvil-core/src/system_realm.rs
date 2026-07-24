use crate::{
    anvil_api::{
        AuthzAllowedSubject, AuthzNamespaceSchema, AuthzRelationRule, AuthzRelationSchema,
        AuthzSchemaMemberKind, AuthzSubjectSelectorKind,
    },
    auth, authz_journal, authz_realm_schema,
    authz_scope::encode_realm_namespace,
    config::Config,
    core_store::{
        AcquireFence, CF_MESH, CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaTuplePart, CoreStore,
        TABLE_SYSTEM_BOOTSTRAP_MARKER_ROW, commit_coremeta_batch_for_storage,
        core_meta_committed_row_common, core_meta_root_key_hash, core_meta_tuple_key,
        encode_deterministic_proto,
    },
    crypto::EncryptionKeyring,
    formats::unix_nanos_from_rfc3339,
    mesh_lifecycle::{LifecycleState, NodeCapability},
    persistence::{App, AuthzTupleBatchMutation, Persistence},
    storage::Storage,
};
use anyhow::{Context, Result, anyhow};
use prost::Message;
use serde::Serialize;
use std::path::Path;

pub const SYSTEM_STORAGE_TENANT_ID: i64 = 0;
pub const SYSTEM_REALM_ID: &str = "_anvil/system";
pub const SYSTEM_SCHEMA_ID: &str = "anvil-system";
pub const SYSTEM_NAMESPACE: &str = "system";
pub const SYSTEM_OBJECT_ID: &str = "_anvil";
pub const SYSTEM_STORAGE_TENANT_NAMESPACE: &str = "storage_tenant";
pub const SYSTEM_BUCKET_NAMESPACE: &str = "bucket";
pub const SYSTEM_OBJECT_NAMESPACE: &str = "object";
pub const SYSTEM_STREAM_NAMESPACE: &str = "stream";
pub const SYSTEM_INDEX_NAMESPACE: &str = "index";
pub const SYSTEM_AUTHZ_REALM_NAMESPACE: &str = "authz_realm";
pub const SYSTEM_REGISTRY_NAMESPACE: &str = "registry_namespace";
pub const SYSTEM_PERSONALDB_GROUP_NAMESPACE: &str = "personaldb_group";
pub const SYSTEM_REGION_NAMESPACE: &str = "region";
pub const SYSTEM_CELL_NAMESPACE: &str = "cell";
pub const SYSTEM_NODE_NAMESPACE: &str = "node";
pub const SYSTEM_PARTITION_NAMESPACE: &str = "partition";
pub const SYSTEM_ADMIN_SUBJECT_KIND_APP: &str = "app";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemAdminRelation {
    BootstrapAdmin,
    Admin,
    ManageSystem,
    ViewSystem,
    ManageAdminPrincipals,
    RotateSecretKeys,
    ManageTenants,
    ManageApps,
    ManagePolicies,
    ManageSecretEncryptionKeys,
    ManagePersonalDbSigningKeys,
    ManageBuckets,
    ManageNodes,
    ManageCells,
    ManageRegions,
    ManagePartitions,
    ManageRouting,
    ManageHostAliases,
    ManageLinks,
    RunRepair,
    ViewDiagnostics,
    ViewAuditLog,
}

impl SystemAdminRelation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BootstrapAdmin => "bootstrap_admin",
            Self::Admin => "admin",
            Self::ManageSystem => "manage_system",
            Self::ViewSystem => "view_system",
            Self::ManageAdminPrincipals => "manage_admin_principals",
            Self::RotateSecretKeys | Self::ManageSecretEncryptionKeys => "rotate_secret_keys",
            Self::ManagePersonalDbSigningKeys => "manage_personaldb_signing_keys",
            Self::ManageTenants => "manage_tenants",
            Self::ManageApps => "manage_apps",
            Self::ManagePolicies => "manage_policies",
            Self::ManageBuckets => "manage_buckets",
            Self::ManageNodes => "manage_nodes",
            Self::ManageCells => "manage_cells",
            Self::ManageRegions => "manage_regions",
            Self::ManagePartitions | Self::ManageRouting => "manage_partitions",
            Self::ManageHostAliases => "manage_host_aliases",
            Self::ManageLinks => "manage_links",
            Self::RunRepair => "run_repair",
            Self::ViewDiagnostics => "view_diagnostics",
            Self::ViewAuditLog => "view_audit_log",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminPrincipal {
    pub principal_id: String,
    pub tenant_id: i64,
    pub authenticated_methods: Vec<String>,
    pub checked_relation: Option<SystemAdminRelation>,
    pub checked_object: Option<String>,
}

impl From<&auth::Claims> for AdminPrincipal {
    fn from(claims: &auth::Claims) -> Self {
        Self {
            principal_id: claims.sub.clone(),
            tenant_id: claims.tenant_id,
            authenticated_methods: vec!["bearer".to_string()],
            checked_relation: None,
            checked_object: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct BootstrapMarker {
    schema: &'static str,
    mesh_id: String,
    authz_realm_id: String,
    completed_at: String,
}

#[derive(Debug, Clone, Serialize)]
struct BootstrapCredentialFile<'a> {
    schema: &'static str,
    mesh_id: &'a str,
    tenant_id: i64,
    app_id: i64,
    app_name: &'a str,
    client_id: &'a str,
    client_secret: &'a str,
}

#[derive(Clone, PartialEq, Message)]
struct BootstrapMarkerProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    mesh_id: String,
    #[prost(string, tag = "3")]
    authz_realm_id: String,
    #[prost(string, tag = "4")]
    completed_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct BootstrapMarkerRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(bytes, tag = "3")]
    marker_bytes: Vec<u8>,
}

impl From<&BootstrapMarker> for BootstrapMarkerProto {
    fn from(marker: &BootstrapMarker) -> Self {
        Self {
            schema: marker.schema.to_string(),
            mesh_id: marker.mesh_id.clone(),
            authz_realm_id: marker.authz_realm_id.clone(),
            completed_at: marker.completed_at.clone(),
        }
    }
}

fn encode_bootstrap_marker(marker: &BootstrapMarker) -> Vec<u8> {
    encode_deterministic_proto(&BootstrapMarkerProto::from(marker))
}

fn encode_bootstrap_marker_row(marker: &BootstrapMarker) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(&BootstrapMarkerRowProto {
        common: Some(core_meta_committed_row_common(
            SYSTEM_REALM_ID,
            core_meta_root_key_hash(&format!("system-realm-bootstrap/{}", marker.mesh_id)),
            1,
            format!("system-realm-bootstrap:{}", marker.mesh_id),
            unix_nanos_from_rfc3339(&marker.completed_at),
        )),
        schema: "anvil.coremeta.system_bootstrap_marker.v1".to_string(),
        marker_bytes: encode_bootstrap_marker(marker),
    }))
}

pub async fn ensure_bootstrapped(
    config: &Config,
    persistence: &Persistence,
    storage: &Storage,
    secret_keyring: &EncryptionKeyring,
) -> Result<()> {
    let mesh_id = normalized_mesh_id(&config.mesh_id);
    if bootstrap_marker_exists(storage, &mesh_id).await? {
        reject_stale_bootstrap_config(config)?;
        install_system_schema(storage, persistence)
            .await
            .context("upgrade system authz schema")?;
        return Ok(());
    }

    let fence_owner = format!("system-realm-bootstrap-{}", uuid::Uuid::new_v4().simple());
    let Some(_fence) = acquire_bootstrap_fence(storage, &mesh_id, &fence_owner).await? else {
        reject_stale_bootstrap_config(config)?;
        return Ok(());
    };
    if bootstrap_marker_exists(storage, &mesh_id).await? {
        reject_stale_bootstrap_config(config)?;
        return Ok(());
    }

    let (subject_kind, subject_id) =
        resolve_bootstrap_subject(config, persistence, secret_keyring).await?;

    install_system_schema(storage, persistence)
        .await
        .context("install system authz schema")?;
    write_system_relation_tuples(
        persistence,
        &mesh_id,
        &subject_kind,
        &subject_id,
        &config.bootstrap_node_ids,
    )
    .await
    .context("write system relation tuples")?;
    write_bootstrap_marker(storage, &mesh_id)
        .await
        .context("write system bootstrap marker")
}

pub async fn check_admin_relation(
    storage: &Storage,
    mesh_id: &str,
    claims: &auth::Claims,
    relation: SystemAdminRelation,
) -> Result<bool> {
    let object_id = system_mesh_object_id(mesh_id);
    let revision = authz_journal::latest_authz_revision(storage, SYSTEM_STORAGE_TENANT_ID).await?;
    authz_journal::resolve_permission_at_revision(
        storage,
        SYSTEM_STORAGE_TENANT_ID,
        &system_namespace(),
        &object_id,
        relation.as_str(),
        SYSTEM_ADMIN_SUBJECT_KIND_APP,
        &claims.sub,
        "",
        revision,
    )
    .await
}

pub(crate) async fn check_internal_node_access(
    storage: &Storage,
    core_store: &CoreStore,
    mesh_id: &str,
    claims: &auth::Claims,
    node_id: &str,
    required_capability: NodeCapability,
) -> Result<bool> {
    if claims.tenant_id != SYSTEM_STORAGE_TENANT_ID
        || claims.sub != node_id
        || node_id.trim().is_empty()
    {
        return Ok(false);
    }

    let granted = authz_journal::check_current_authz_tuple_with_core_store(
        storage,
        core_store,
        SYSTEM_STORAGE_TENANT_ID,
        &system_namespace(),
        SYSTEM_OBJECT_ID,
        "manage_nodes_grant",
        SYSTEM_ADMIN_SUBJECT_KIND_APP,
        node_id,
        "",
    )
    .await?
    .is_some();
    if !granted {
        return Ok(false);
    }

    let nodes =
        crate::mesh_lifecycle::list_node_projections_with_core_store(core_store, None, None)?;
    Ok(nodes.into_iter().any(|node| {
        node.node_id == node_id
            && node.mesh_id == mesh_id
            && node.state == LifecycleState::Active
            && node.capabilities.contains(&required_capability)
    }))
}

pub async fn principal_has_any_admin_relation(
    storage: &Storage,
    mesh_id: &str,
    app_id: i64,
) -> Result<bool> {
    let claims = auth::Claims {
        sub: app_id.to_string(),
        exp: usize::MAX,
        tenant_id: 0,
        jti: None,
    };
    for relation in all_admin_relations() {
        if check_admin_relation(storage, mesh_id, &claims, *relation).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn system_namespace() -> String {
    encode_realm_namespace(SYSTEM_REALM_ID, SYSTEM_NAMESPACE)
}

pub fn system_mesh_object_id(_mesh_id: &str) -> String {
    SYSTEM_OBJECT_ID.to_string()
}

fn normalized_mesh_id(mesh_id: &str) -> String {
    let trimmed = mesh_id.trim();
    if trimmed.is_empty() {
        "default".to_string()
    } else {
        trimmed.to_string()
    }
}

fn all_admin_relations() -> &'static [SystemAdminRelation] {
    &[
        SystemAdminRelation::BootstrapAdmin,
        SystemAdminRelation::Admin,
        SystemAdminRelation::ManageSystem,
        SystemAdminRelation::ViewSystem,
        SystemAdminRelation::ManageAdminPrincipals,
        SystemAdminRelation::RotateSecretKeys,
        SystemAdminRelation::ManagePersonalDbSigningKeys,
        SystemAdminRelation::ManageTenants,
        SystemAdminRelation::ManageApps,
        SystemAdminRelation::ManagePolicies,
        SystemAdminRelation::ManageBuckets,
        SystemAdminRelation::ManageNodes,
        SystemAdminRelation::ManageCells,
        SystemAdminRelation::ManageRegions,
        SystemAdminRelation::ManagePartitions,
        SystemAdminRelation::ManageRouting,
        SystemAdminRelation::ManageHostAliases,
        SystemAdminRelation::ManageLinks,
        SystemAdminRelation::RunRepair,
        SystemAdminRelation::ViewDiagnostics,
        SystemAdminRelation::ViewAuditLog,
    ]
}

async fn bootstrap_marker_exists(storage: &Storage, mesh_id: &str) -> Result<bool> {
    Ok(CoreStore::new(storage.clone())
        .await?
        .read_coremeta_row(
            CF_MESH,
            TABLE_SYSTEM_BOOTSTRAP_MARKER_ROW,
            &bootstrap_marker_tuple_key(mesh_id)?,
        )?
        .is_some())
}

async fn acquire_bootstrap_fence(
    storage: &Storage,
    mesh_id: &str,
    owner: &str,
) -> Result<Option<crate::core_store::FencedPermit>> {
    let store = CoreStore::new(storage.clone()).await?;
    let fence_name = format!("system-realm-bootstrap-{mesh_id}");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        match store
            .acquire_fence(AcquireFence {
                fence_name: fence_name.clone(),
                authenticated_principal: owner.to_string(),
                ttl_ms: 60_000,
            })
            .await
        {
            Ok(permit) => return Ok(Some(permit)),
            Err(err) => {
                if bootstrap_marker_exists(storage, mesh_id).await? {
                    return Ok(None);
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(err).context("acquire system realm bootstrap fence");
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
}

fn reject_stale_bootstrap_config(config: &Config) -> Result<()> {
    if !config.bootstrap_system_admin_app_name.trim().is_empty()
        || !config
            .bootstrap_system_admin_credential_output_path
            .trim()
            .is_empty()
        || !config.bootstrap_system_admin_subject_kind.trim().is_empty()
        || !config.bootstrap_system_admin_subject_id.trim().is_empty()
    {
        tracing::warn!(
            "system realm already exists; first-boot bootstrap system admin configuration is ignored"
        );
    }
    Ok(())
}

async fn resolve_bootstrap_subject(
    config: &Config,
    persistence: &Persistence,
    secret_keyring: &EncryptionKeyring,
) -> Result<(String, String)> {
    let subject_kind = config.bootstrap_system_admin_subject_kind.trim();
    let subject_id = config.bootstrap_system_admin_subject_id.trim();
    if !subject_kind.is_empty() || !subject_id.is_empty() {
        if subject_kind.is_empty() || subject_id.is_empty() {
            return Err(anyhow!(
                "both bootstrap_system_admin_subject_kind and bootstrap_system_admin_subject_id are required"
            ));
        }
        return Ok((subject_kind.to_string(), subject_id.to_string()));
    }

    let app_name = config.bootstrap_system_admin_app_name.trim();
    let output_path = config.bootstrap_system_admin_credential_output_path.trim();
    if app_name.is_empty() || output_path.is_empty() {
        return Err(anyhow!(
            "system realm is missing; configure bootstrap_system_admin_app_name and bootstrap_system_admin_credential_output_path, or configure an explicit bootstrap system admin subject"
        ));
    }
    let output_path = Path::new(output_path);
    reject_bootstrap_credential_output_path(config, output_path)?;

    let tenant_id = SYSTEM_STORAGE_TENANT_ID;

    let existing = persistence
        .get_app_by_tenant_name(tenant_id, app_name)
        .await?;
    let app = match existing {
        Some(app) => app,
        None => {
            create_bootstrap_app(
                config,
                persistence,
                secret_keyring,
                tenant_id,
                app_name,
                output_path,
            )
            .await?
        }
    };
    Ok((
        SYSTEM_ADMIN_SUBJECT_KIND_APP.to_string(),
        app.id.to_string(),
    ))
}

fn reject_bootstrap_credential_output_path(config: &Config, output_path: &Path) -> Result<()> {
    crate::storage::ensure_operator_path_outside_storage(
        &config.storage_path,
        output_path,
        "bootstrap_system_admin_credential_output_path",
        "bootstrap credential JSON export",
    )
}

async fn create_bootstrap_app(
    config: &Config,
    persistence: &Persistence,
    secret_keyring: &EncryptionKeyring,
    tenant_id: i64,
    app_name: &str,
    output_path: &Path,
) -> Result<App> {
    let client_id = format!("app_{}", uuid::Uuid::new_v4().simple());
    let client_secret = format!("secret_{}", uuid::Uuid::new_v4().simple());
    let encrypted_secret = secret_keyring.encrypt(client_secret.as_bytes())?;
    let app = persistence
        .create_app(tenant_id, app_name, &client_id, &encrypted_secret)
        .await?;

    let credential = BootstrapCredentialFile {
        schema: "anvil.bootstrap.admin_credential.v1",
        mesh_id: &config.mesh_id,
        tenant_id,
        app_id: app.id,
        app_name,
        client_id: &client_id,
        client_secret: &client_secret,
    };
    write_bootstrap_credential(output_path, &credential)
        .with_context(|| format!("write bootstrap credential to {}", output_path.display()))?;

    Ok(app)
}

fn write_bootstrap_credential(path: &Path, credential: &BootstrapCredentialFile<'_>) -> Result<()> {
    // Operator export only; reject_bootstrap_credential_output_path keeps it outside Anvil storage.
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let body = serde_json::to_vec_pretty(credential)?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    let mut file = options.open(path)?;
    use std::io::Write;
    file.write_all(&body)?;
    file.write_all(b"\n")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

async fn install_system_schema(storage: &Storage, persistence: &Persistence) -> Result<()> {
    let namespaces = system_mesh_schema();
    let revision = authz_realm_schema::put_schema_revision(
        storage,
        SYSTEM_STORAGE_TENANT_ID,
        SYSTEM_SCHEMA_ID,
        namespaces.clone(),
        "system-realm-bootstrap",
        "install built-in system realm schema",
    )
    .await?;
    match authz_realm_schema::read_schema_binding(
        storage,
        SYSTEM_STORAGE_TENANT_ID,
        SYSTEM_REALM_ID,
    )
    .await?
    {
        Some(binding) if binding.schema_ref == revision.schema_ref => {}
        Some(binding) => {
            authz_realm_schema::bind_schema(
                storage,
                SYSTEM_STORAGE_TENANT_ID,
                SYSTEM_REALM_ID,
                revision.schema_ref,
                Some(binding.binding_generation),
                "system-realm-bootstrap",
                "bind built-in system realm schema",
            )
            .await?;
        }
        None => {
            match authz_realm_schema::bind_schema(
                storage,
                SYSTEM_STORAGE_TENANT_ID,
                SYSTEM_REALM_ID,
                revision.schema_ref.clone(),
                None,
                "system-realm-bootstrap",
                "bind built-in system realm schema",
            )
            .await
            {
                Ok(_) => {}
                Err(err) => {
                    let Some(binding) = authz_realm_schema::read_schema_binding(
                        storage,
                        SYSTEM_STORAGE_TENANT_ID,
                        SYSTEM_REALM_ID,
                    )
                    .await?
                    else {
                        return Err(err);
                    };
                    if binding.schema_ref != revision.schema_ref {
                        return Err(err);
                    }
                }
            }
        }
    }
    Ok(())
}

fn system_mesh_schema() -> Vec<AuthzNamespaceSchema> {
    vec![
        system_namespace_schema(),
        storage_tenant_namespace_schema(),
        bucket_namespace_schema(),
        object_namespace_schema(),
        stream_namespace_schema(),
        index_namespace_schema(),
        authz_realm_namespace_schema(),
        registry_namespace_schema(),
        personaldb_group_namespace_schema(),
        region_namespace_schema(),
        cell_namespace_schema(),
        node_namespace_schema(),
        partition_namespace_schema(),
    ]
}

fn system_namespace_schema() -> AuthzNamespaceSchema {
    AuthzNamespaceSchema {
        namespace: SYSTEM_NAMESPACE.to_string(),
        relations: relations(
            SYSTEM_NAMESPACE,
            &[
                relation("bootstrap_admin", &[]),
                relation("admin", &[inherit_rule("bootstrap_admin")]),
                relation("auditor", &[]),
                relation("operator", &[]),
                relation("support", &[]),
                relation(
                    "manage_system",
                    &[inherit_rule("bootstrap_admin"), inherit_rule("admin")],
                ),
                relation(
                    "view_system",
                    &[inherit_rule("manage_system"), inherit_rule("auditor")],
                ),
                relation(
                    "manage_admin_principals",
                    &[inherit_rule("bootstrap_admin")],
                ),
                relation(
                    "rotate_secret_keys",
                    &[inherit_rule("bootstrap_admin"), inherit_rule("admin")],
                ),
                relation(
                    "manage_personaldb_signing_keys",
                    &[inherit_rule("bootstrap_admin"), inherit_rule("admin")],
                ),
                relation(
                    "manage_regions",
                    &[inherit_rule("admin"), inherit_rule("operator")],
                ),
                relation(
                    "manage_cells",
                    &[inherit_rule("admin"), inherit_rule("operator")],
                ),
                relation(
                    "manage_nodes",
                    &[inherit_rule("admin"), inherit_rule("operator")],
                ),
                relation(
                    "manage_partitions",
                    &[inherit_rule("admin"), inherit_rule("operator")],
                ),
                relation("manage_tenants", &[inherit_rule("manage_system")]),
                relation("manage_apps", &[inherit_rule("manage_system")]),
                relation("manage_policies", &[inherit_rule("manage_system")]),
                relation("manage_buckets", &[inherit_rule("manage_system")]),
                relation("manage_host_aliases", &[inherit_rule("manage_system")]),
                relation("manage_links", &[inherit_rule("manage_system")]),
                relation(
                    "run_repair",
                    &[inherit_rule("manage_system"), inherit_rule("operator")],
                ),
                relation(
                    "view_diagnostics",
                    &[
                        inherit_rule("view_system"),
                        inherit_rule("operator"),
                        inherit_rule("support"),
                    ],
                ),
                relation(
                    "view_audit_log",
                    &[inherit_rule("view_system"), inherit_rule("auditor")],
                ),
            ],
        ),
        schema_json: system_schema_json(SYSTEM_NAMESPACE),
        schema_hash: String::new(),
        schema_version: 0,
        authz_revision: 0,
        applied_at: String::new(),
    }
}

fn storage_tenant_namespace_schema() -> AuthzNamespaceSchema {
    AuthzNamespaceSchema {
        namespace: SYSTEM_STORAGE_TENANT_NAMESPACE.to_string(),
        relations: relations(
            SYSTEM_STORAGE_TENANT_NAMESPACE,
            &[
                relation("owner", &[]),
                relation("admin", &[]),
                relation("writer", &[]),
                relation("reader", &[]),
                relation("auditor", &[]),
                relation(
                    "manage_tenant",
                    &[inherit_rule("owner"), inherit_rule("admin")],
                ),
                relation("create_bucket", &[inherit_rule("manage_tenant")]),
                relation(
                    "list_buckets",
                    &[inherit_rule("manage_tenant"), inherit_rule("auditor")],
                ),
                relation(
                    "read_tenant",
                    &[inherit_rule("manage_tenant"), inherit_rule("auditor")],
                ),
                relation("grant_access", &[inherit_rule("manage_tenant")]),
                relation("revoke_access", &[inherit_rule("manage_tenant")]),
                relation(
                    "read_access_grants",
                    &[inherit_rule("manage_tenant"), inherit_rule("auditor")],
                ),
                relation(
                    "lease_read",
                    &[inherit_rule("manage_tenant"), inherit_rule("auditor")],
                ),
                relation("lease_write", &[inherit_rule("manage_tenant")]),
                relation("lease_admin", &[inherit_rule("manage_tenant")]),
            ],
        ),
        schema_json: system_schema_json(SYSTEM_STORAGE_TENANT_NAMESPACE),
        schema_hash: String::new(),
        schema_version: 0,
        authz_revision: 0,
        applied_at: String::new(),
    }
}

fn bucket_namespace_schema() -> AuthzNamespaceSchema {
    AuthzNamespaceSchema {
        namespace: SYSTEM_BUCKET_NAMESPACE.to_string(),
        relations: relations(
            SYSTEM_BUCKET_NAMESPACE,
            &[
                relation("parent_tenant", &[]),
                relation("owner", &[]),
                relation("admin", &[]),
                relation("writer", &[]),
                relation("reader", &[]),
                relation("auditor", &[]),
                relation(
                    "manage_bucket",
                    &[
                        inherit_rule("owner"),
                        inherit_rule("admin"),
                        computed_rule("parent_tenant", "manage_tenant"),
                    ],
                ),
                relation(
                    "put_object",
                    &[inherit_rule("manage_bucket"), inherit_rule("writer")],
                ),
                relation(
                    "get_object",
                    &[inherit_rule("manage_bucket"), inherit_rule("reader")],
                ),
                relation(
                    "list_objects",
                    &[
                        inherit_rule("manage_bucket"),
                        inherit_rule("reader"),
                        inherit_rule("auditor"),
                    ],
                ),
                relation(
                    "delete_object",
                    &[inherit_rule("manage_bucket"), inherit_rule("writer")],
                ),
                relation(
                    "manage_links",
                    &[inherit_rule("manage_bucket"), inherit_rule("writer")],
                ),
                relation(
                    "manage_indexes",
                    &[inherit_rule("manage_bucket"), inherit_rule("admin")],
                ),
                relation(
                    "query_indexes",
                    &[inherit_rule("manage_bucket"), inherit_rule("reader")],
                ),
                relation(
                    "manage_boundary_schema",
                    &[inherit_rule("manage_bucket"), inherit_rule("admin")],
                ),
            ],
        ),
        schema_json: system_schema_json(SYSTEM_BUCKET_NAMESPACE),
        schema_hash: String::new(),
        schema_version: 0,
        authz_revision: 0,
        applied_at: String::new(),
    }
}

fn object_namespace_schema() -> AuthzNamespaceSchema {
    AuthzNamespaceSchema {
        namespace: SYSTEM_OBJECT_NAMESPACE.to_string(),
        relations: relations(
            SYSTEM_OBJECT_NAMESPACE,
            &[
                relation("parent_bucket", &[]),
                relation("owner", &[]),
                relation("reader", &[]),
                relation("writer", &[]),
                relation(
                    "get",
                    &[
                        inherit_rule("owner"),
                        inherit_rule("reader"),
                        computed_rule("parent_bucket", "get_object"),
                    ],
                ),
                relation(
                    "put",
                    &[
                        inherit_rule("owner"),
                        inherit_rule("writer"),
                        computed_rule("parent_bucket", "put_object"),
                    ],
                ),
                relation(
                    "delete",
                    &[
                        inherit_rule("owner"),
                        inherit_rule("writer"),
                        computed_rule("parent_bucket", "delete_object"),
                    ],
                ),
                relation(
                    "link",
                    &[
                        inherit_rule("owner"),
                        inherit_rule("writer"),
                        computed_rule("parent_bucket", "manage_links"),
                    ],
                ),
            ],
        ),
        schema_json: system_schema_json(SYSTEM_OBJECT_NAMESPACE),
        schema_hash: String::new(),
        schema_version: 0,
        authz_revision: 0,
        applied_at: String::new(),
    }
}

fn stream_namespace_schema() -> AuthzNamespaceSchema {
    AuthzNamespaceSchema {
        namespace: SYSTEM_STREAM_NAMESPACE.to_string(),
        relations: relations(
            SYSTEM_STREAM_NAMESPACE,
            &[
                relation("parent_bucket", &[]),
                relation("owner", &[]),
                relation("producer", &[]),
                relation("consumer", &[]),
                relation(
                    "append",
                    &[
                        inherit_rule("owner"),
                        inherit_rule("producer"),
                        computed_rule("parent_bucket", "put_object"),
                    ],
                ),
                relation(
                    "read",
                    &[
                        inherit_rule("owner"),
                        inherit_rule("consumer"),
                        computed_rule("parent_bucket", "get_object"),
                    ],
                ),
                relation(
                    "seal_segment",
                    &[
                        inherit_rule("owner"),
                        computed_rule("parent_bucket", "manage_bucket"),
                    ],
                ),
            ],
        ),
        schema_json: system_schema_json(SYSTEM_STREAM_NAMESPACE),
        schema_hash: String::new(),
        schema_version: 0,
        authz_revision: 0,
        applied_at: String::new(),
    }
}

fn index_namespace_schema() -> AuthzNamespaceSchema {
    AuthzNamespaceSchema {
        namespace: SYSTEM_INDEX_NAMESPACE.to_string(),
        relations: relations(
            SYSTEM_INDEX_NAMESPACE,
            &[
                relation("parent_bucket", &[]),
                relation("owner", &[]),
                relation("reader", &[]),
                relation("writer", &[]),
                relation(
                    "define",
                    &[
                        inherit_rule("owner"),
                        computed_rule("parent_bucket", "manage_indexes"),
                    ],
                ),
                relation(
                    "query",
                    &[
                        inherit_rule("owner"),
                        inherit_rule("reader"),
                        computed_rule("parent_bucket", "query_indexes"),
                    ],
                ),
                relation(
                    "repair",
                    &[
                        inherit_rule("owner"),
                        computed_rule("parent_bucket", "manage_indexes"),
                    ],
                ),
            ],
        ),
        schema_json: system_schema_json(SYSTEM_INDEX_NAMESPACE),
        schema_hash: String::new(),
        schema_version: 0,
        authz_revision: 0,
        applied_at: String::new(),
    }
}

fn authz_realm_namespace_schema() -> AuthzNamespaceSchema {
    AuthzNamespaceSchema {
        namespace: SYSTEM_AUTHZ_REALM_NAMESPACE.to_string(),
        relations: relations(
            SYSTEM_AUTHZ_REALM_NAMESPACE,
            &[
                relation("parent_tenant", &[]),
                relation("owner", &[]),
                relation("schema_admin", &[]),
                relation("tuple_writer", &[]),
                relation("checker", &[]),
                relation("auditor", &[]),
                relation(
                    "put_schema",
                    &[
                        inherit_rule("owner"),
                        inherit_rule("schema_admin"),
                        computed_rule("parent_tenant", "manage_tenant"),
                    ],
                ),
                relation(
                    "bind_schema",
                    &[
                        inherit_rule("owner"),
                        inherit_rule("schema_admin"),
                        computed_rule("parent_tenant", "manage_tenant"),
                    ],
                ),
                relation(
                    "write_tuples",
                    &[
                        inherit_rule("owner"),
                        inherit_rule("tuple_writer"),
                        computed_rule("parent_tenant", "manage_tenant"),
                    ],
                ),
                relation(
                    "check",
                    &[
                        inherit_rule("owner"),
                        inherit_rule("checker"),
                        inherit_rule("auditor"),
                        computed_rule("parent_tenant", "read_tenant"),
                    ],
                ),
                relation(
                    "list",
                    &[
                        inherit_rule("owner"),
                        inherit_rule("auditor"),
                        computed_rule("parent_tenant", "read_tenant"),
                    ],
                ),
            ],
        ),
        schema_json: system_schema_json(SYSTEM_AUTHZ_REALM_NAMESPACE),
        schema_hash: String::new(),
        schema_version: 0,
        authz_revision: 0,
        applied_at: String::new(),
    }
}

fn registry_namespace_schema() -> AuthzNamespaceSchema {
    AuthzNamespaceSchema {
        namespace: SYSTEM_REGISTRY_NAMESPACE.to_string(),
        relations: relations(
            SYSTEM_REGISTRY_NAMESPACE,
            &[
                relation("parent_tenant", &[]),
                relation("owner", &[]),
                relation("publisher", &[]),
                relation("reader", &[]),
                relation(
                    "publish",
                    &[
                        inherit_rule("owner"),
                        inherit_rule("publisher"),
                        computed_rule("parent_tenant", "manage_tenant"),
                    ],
                ),
                relation(
                    "read",
                    &[
                        inherit_rule("owner"),
                        inherit_rule("reader"),
                        computed_rule("parent_tenant", "read_tenant"),
                    ],
                ),
                relation(
                    "manage_refs",
                    &[
                        inherit_rule("owner"),
                        inherit_rule("publisher"),
                        computed_rule("parent_tenant", "manage_tenant"),
                    ],
                ),
            ],
        ),
        schema_json: system_schema_json(SYSTEM_REGISTRY_NAMESPACE),
        schema_hash: String::new(),
        schema_version: 0,
        authz_revision: 0,
        applied_at: String::new(),
    }
}

fn personaldb_group_namespace_schema() -> AuthzNamespaceSchema {
    AuthzNamespaceSchema {
        namespace: SYSTEM_PERSONALDB_GROUP_NAMESPACE.to_string(),
        relations: relations(
            SYSTEM_PERSONALDB_GROUP_NAMESPACE,
            &[
                relation("parent_tenant", &[]),
                relation("owner", &[]),
                relation("writer", &[]),
                relation("reader", &[]),
                relation("witness", &[inherit_rule("owner")]),
                relation(
                    "apply_changeset",
                    &[inherit_rule("owner"), inherit_rule("writer")],
                ),
                relation(
                    "get_snapshot",
                    &[inherit_rule("owner"), inherit_rule("reader")],
                ),
                relation("watch", &[inherit_rule("owner"), inherit_rule("reader")]),
            ],
        ),
        schema_json: system_schema_json(SYSTEM_PERSONALDB_GROUP_NAMESPACE),
        schema_hash: String::new(),
        schema_version: 0,
        authz_revision: 0,
        applied_at: String::new(),
    }
}

fn region_namespace_schema() -> AuthzNamespaceSchema {
    AuthzNamespaceSchema {
        namespace: SYSTEM_REGION_NAMESPACE.to_string(),
        relations: relations(
            SYSTEM_REGION_NAMESPACE,
            &[
                relation("system", &[]),
                relation("manage", &[computed_rule("system", "manage_regions")]),
                relation("view", &[computed_rule("system", "view_system")]),
            ],
        ),
        schema_json: system_schema_json(SYSTEM_REGION_NAMESPACE),
        schema_hash: String::new(),
        schema_version: 0,
        authz_revision: 0,
        applied_at: String::new(),
    }
}

fn cell_namespace_schema() -> AuthzNamespaceSchema {
    AuthzNamespaceSchema {
        namespace: SYSTEM_CELL_NAMESPACE.to_string(),
        relations: relations(
            SYSTEM_CELL_NAMESPACE,
            &[
                relation("parent_region", &[]),
                relation("manage", &[computed_rule("parent_region", "manage")]),
                relation("view", &[computed_rule("parent_region", "view")]),
            ],
        ),
        schema_json: system_schema_json(SYSTEM_CELL_NAMESPACE),
        schema_hash: String::new(),
        schema_version: 0,
        authz_revision: 0,
        applied_at: String::new(),
    }
}

fn node_namespace_schema() -> AuthzNamespaceSchema {
    AuthzNamespaceSchema {
        namespace: SYSTEM_NODE_NAMESPACE.to_string(),
        relations: relations(
            SYSTEM_NODE_NAMESPACE,
            &[
                relation("parent_cell", &[]),
                relation("manage", &[computed_rule("parent_cell", "manage")]),
                relation("drain", &[computed_rule("parent_cell", "manage")]),
                relation("view", &[computed_rule("parent_cell", "view")]),
            ],
        ),
        schema_json: system_schema_json(SYSTEM_NODE_NAMESPACE),
        schema_hash: String::new(),
        schema_version: 0,
        authz_revision: 0,
        applied_at: String::new(),
    }
}

fn partition_namespace_schema() -> AuthzNamespaceSchema {
    AuthzNamespaceSchema {
        namespace: SYSTEM_PARTITION_NAMESPACE.to_string(),
        relations: relations(
            SYSTEM_PARTITION_NAMESPACE,
            &[
                relation("system", &[]),
                relation("move", &[computed_rule("system", "manage_partitions")]),
                relation("view", &[computed_rule("system", "view_system")]),
            ],
        ),
        schema_json: system_schema_json(SYSTEM_PARTITION_NAMESPACE),
        schema_hash: String::new(),
        schema_version: 0,
        authz_revision: 0,
        applied_at: String::new(),
    }
}

fn relations(namespace: &str, definitions: &[AuthzRelationSchema]) -> Vec<AuthzRelationSchema> {
    let mut members = Vec::with_capacity(definitions.len() * 2);
    for definition in definitions {
        if definition.rules.is_empty() {
            let mut relation = definition.clone();
            relation.member_kind = AuthzSchemaMemberKind::DirectRelation as i32;
            relation.allowed_subjects = direct_relation_subjects(namespace, &relation.relation);
            members.push(relation);
            continue;
        }

        let grant_relation = format!("{}_grant", definition.relation);
        members.push(AuthzRelationSchema {
            relation: grant_relation.clone(),
            rules: Vec::new(),
            member_kind: AuthzSchemaMemberKind::DirectRelation as i32,
            allowed_subjects: vec![any_subject(SYSTEM_ADMIN_SUBJECT_KIND_APP)],
        });
        let mut permission = definition.clone();
        permission.member_kind = AuthzSchemaMemberKind::Permission as i32;
        permission.allowed_subjects.clear();
        permission.rules.insert(0, inherit_rule(&grant_relation));
        members.push(permission);
    }
    members
}

fn relation(name: &str, rules: &[AuthzRelationRule]) -> AuthzRelationSchema {
    AuthzRelationSchema {
        relation: name.to_string(),
        rules: rules.to_vec(),
        member_kind: AuthzSchemaMemberKind::Unspecified as i32,
        allowed_subjects: Vec::new(),
    }
}

fn direct_relation_subjects(namespace: &str, relation: &str) -> Vec<AuthzAllowedSubject> {
    match relation {
        "parent_tenant" => vec![any_subject(SYSTEM_STORAGE_TENANT_NAMESPACE)],
        "parent_bucket" => vec![any_subject(SYSTEM_BUCKET_NAMESPACE)],
        "parent_region" => vec![any_subject(SYSTEM_REGION_NAMESPACE)],
        "parent_cell" => vec![any_subject(SYSTEM_CELL_NAMESPACE)],
        "system" => vec![exact_subject(SYSTEM_NAMESPACE, SYSTEM_OBJECT_ID)],
        "reader" if namespace == SYSTEM_BUCKET_NAMESPACE => {
            vec![any_subject(SYSTEM_ADMIN_SUBJECT_KIND_APP), public_subject()]
        }
        _ => vec![any_subject(SYSTEM_ADMIN_SUBJECT_KIND_APP)],
    }
}

fn any_subject(subject_kind: &str) -> AuthzAllowedSubject {
    AuthzAllowedSubject {
        selector_kind: AuthzSubjectSelectorKind::AnyCanonicalId as i32,
        subject_kind: subject_kind.to_string(),
        subject_id: String::new(),
    }
}

fn exact_subject(subject_kind: &str, subject_id: &str) -> AuthzAllowedSubject {
    AuthzAllowedSubject {
        selector_kind: AuthzSubjectSelectorKind::Exact as i32,
        subject_kind: subject_kind.to_string(),
        subject_id: subject_id.to_string(),
    }
}

fn public_subject() -> AuthzAllowedSubject {
    AuthzAllowedSubject {
        selector_kind: AuthzSubjectSelectorKind::Public as i32,
        subject_kind: String::new(),
        subject_id: String::new(),
    }
}

fn system_schema_json(namespace: &str) -> String {
    serde_json::json!({
        "schema": "anvil.system.authz_schema.v2",
        "namespace": namespace,
        "description": "Built-in Anvil system realm. Public tenant APIs cannot mutate this schema."
    })
    .to_string()
}

fn inherit_rule(relation: &str) -> AuthzRelationRule {
    AuthzRelationRule {
        kind: "inherit".to_string(),
        relation: relation.to_string(),
        tuple_relation: String::new(),
        target_relation: String::new(),
    }
}

fn computed_rule(tuple_relation: &str, target_relation: &str) -> AuthzRelationRule {
    AuthzRelationRule {
        kind: "computed".to_string(),
        relation: String::new(),
        tuple_relation: tuple_relation.to_string(),
        target_relation: target_relation.to_string(),
    }
}

async fn write_system_relation_tuples(
    persistence: &Persistence,
    _mesh_id: &str,
    subject_kind: &str,
    subject_id: &str,
    bootstrap_node_ids: &[String],
) -> Result<()> {
    let namespace = system_namespace();
    let mut mutations = vec![AuthzTupleBatchMutation {
        namespace: namespace.clone(),
        object_id: SYSTEM_OBJECT_ID.to_string(),
        relation: "bootstrap_admin".to_string(),
        subject_kind: subject_kind.to_string(),
        subject_id: subject_id.to_string(),
        caveat_hash: String::new(),
        operation: "add".to_string(),
        reason: "grant initial system bootstrap administrator".to_string(),
    }];
    let mut unique_node_ids = std::collections::BTreeSet::new();
    for node_id in bootstrap_node_ids {
        let node_id = node_id.trim();
        if node_id.is_empty() || !unique_node_ids.insert(node_id) {
            continue;
        }
        mutations.push(AuthzTupleBatchMutation {
            namespace: namespace.clone(),
            object_id: SYSTEM_OBJECT_ID.to_string(),
            relation: "manage_nodes_grant".to_string(),
            subject_kind: SYSTEM_ADMIN_SUBJECT_KIND_APP.to_string(),
            subject_id: node_id.to_string(),
            caveat_hash: String::new(),
            operation: "add".to_string(),
            reason: "admit mesh genesis node principal".to_string(),
        });
    }
    persistence
        .write_authz_tuple_batch(
            SYSTEM_STORAGE_TENANT_ID,
            mutations,
            "system-realm-bootstrap",
        )
        .await?;
    Ok(())
}

async fn write_bootstrap_marker(storage: &Storage, mesh_id: &str) -> Result<()> {
    let marker = BootstrapMarker {
        schema: "anvil.system_realm.bootstrap_marker.v1",
        mesh_id: mesh_id.to_string(),
        authz_realm_id: SYSTEM_REALM_ID.to_string(),
        completed_at: chrono::Utc::now().to_rfc3339(),
    };
    let payload = encode_bootstrap_marker_row(&marker)?;
    let tuple_key = bootstrap_marker_tuple_key(mesh_id)?;
    let op = CoreMetaBatchOp {
        cf: CF_MESH,
        table_id: TABLE_SYSTEM_BOOTSTRAP_MARKER_ROW,
        tuple_key: &tuple_key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(&payload),
    };
    commit_coremeta_batch_for_storage(
        storage,
        &format!("system-realm-bootstrap:{}", uuid::Uuid::new_v4().simple()),
        &[op],
        &[crate::core_store::CoreMetaRootPublication::new(
            format!("system-realm-bootstrap/{mesh_id}"),
            crate::formats::writer::WriterFamily::MeshControl,
        )],
    )
    .await?;
    Ok(())
}

fn bootstrap_marker_tuple_key(mesh_id: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("system-realm-bootstrap"),
        CoreMetaTuplePart::Utf8(mesh_id),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_config(storage_path: &std::path::Path) -> Config {
        Config {
            jwt_secret: "test-secret".to_string(),
            anvil_secret_encryption_key:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            public_api_addr: "test-node".to_string(),
            api_listen_addr: "127.0.0.1:0".to_string(),
            admin_listen_addr: "127.0.0.1:0".to_string(),
            mesh_id: "mesh-test".to_string(),
            region: "test-region".to_string(),
            storage_path: storage_path.to_string_lossy().to_string(),
            bootstrap_system_admin_subject_kind: "app".to_string(),
            bootstrap_system_admin_subject_id: "admin-principal".to_string(),
            ..Config::default()
        }
    }

    async fn install_active_internal_node(storage: &Storage, mesh_id: &str, node_id: &str) {
        let region = crate::mesh_lifecycle::create_region(
            storage,
            crate::mesh_lifecycle::CreateRegionDescriptor {
                mesh_id: mesh_id.to_string(),
                region: "test-region".to_string(),
                public_base_url: "https://test-region.anvil.invalid".to_string(),
                virtual_host_suffix: "test-region.anvil.invalid".to_string(),
                placement_weight: 100,
                default_cell: Some("test-cell".to_string()),
            },
        )
        .await
        .unwrap();
        let cell = crate::mesh_lifecycle::register_cell(
            storage,
            crate::mesh_lifecycle::RegisterCellDescriptor {
                mesh_id: mesh_id.to_string(),
                region: region.region.clone(),
                cell_id: "test-cell".to_string(),
                placement_weight: 100,
                failure_domain: "test-rack".to_string(),
            },
        )
        .await
        .unwrap();
        crate::mesh_lifecycle::transition_cell(
            storage,
            &region.region,
            &cell.cell_id,
            cell.generation,
            LifecycleState::Active,
        )
        .await
        .unwrap();
        crate::mesh_lifecycle::transition_region(
            storage,
            &region.region,
            region.generation,
            LifecycleState::Active,
        )
        .await
        .unwrap();
        let node = crate::mesh_lifecycle::register_node(
            storage,
            crate::mesh_lifecycle::RegisterNodeDescriptor {
                mesh_id: mesh_id.to_string(),
                node_id: node_id.to_string(),
                region: region.region,
                cell_id: cell.cell_id,
                receipt_signing_public_key: crate::node_signing::NodeSigningKeypair::generate()
                    .unwrap()
                    .public_key_bytes()
                    .to_vec(),
                public_api_addr: "http://127.0.0.1:50051".to_string(),
                capabilities: vec![NodeCapability::Metadata, NodeCapability::Object],
                capacity_json: "{}".to_string(),
            },
        )
        .await
        .unwrap();
        crate::mesh_lifecycle::transition_node(
            storage,
            node_id,
            node.generation,
            LifecycleState::Active,
            None,
        )
        .await
        .unwrap();
    }

    #[test]
    fn bootstrap_marker_encoding_is_canonical_protobuf_not_json() {
        let marker = BootstrapMarker {
            schema: "anvil.system_realm.bootstrap_marker.v1",
            mesh_id: "mesh-a".to_string(),
            authz_realm_id: SYSTEM_REALM_ID.to_string(),
            completed_at: "2026-07-09T00:00:00Z".to_string(),
        };
        let bytes = encode_bootstrap_marker(&marker);
        assert!(serde_json::from_slice::<serde_json::Value>(&bytes).is_err());
        let decoded = crate::core_store::decode_deterministic_proto::<BootstrapMarkerProto>(
            &bytes,
            "system realm bootstrap marker",
        )
        .unwrap();
        assert_eq!(decoded.mesh_id, marker.mesh_id);
        assert_eq!(decoded.authz_realm_id, marker.authz_realm_id);
    }

    #[tokio::test]
    async fn system_realm_bootstrap_creates_builtin_schema_and_owner_tuple() {
        let temp = tempdir().unwrap();
        let config = test_config(temp.path());
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let persistence = Persistence::new(&config).unwrap();
        let keyring = config.secret_keyring().unwrap();

        ensure_bootstrapped(&config, &persistence, &storage, &keyring)
            .await
            .unwrap();

        let allowed = check_admin_relation(
            &storage,
            &config.mesh_id,
            &auth::Claims {
                sub: "admin-principal".to_string(),
                exp: usize::MAX,
                tenant_id: 0,
                jti: None,
            },
            SystemAdminRelation::ManageNodes,
        )
        .await
        .unwrap();
        assert!(allowed);

        let denied = check_admin_relation(
            &storage,
            &config.mesh_id,
            &auth::Claims {
                sub: "ordinary-app".to_string(),
                exp: usize::MAX,
                tenant_id: 0,
                jti: None,
            },
            SystemAdminRelation::ManageNodes,
        )
        .await
        .unwrap();
        assert!(!denied);
    }

    #[tokio::test]
    async fn internal_node_access_requires_direct_grant_active_membership_and_capability() {
        let temp = tempdir().unwrap();
        let mut config = test_config(temp.path());
        config.bootstrap_node_ids = vec!["node-a".to_string()];
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let persistence = Persistence::new(&config).unwrap();
        let keyring = config.secret_keyring().unwrap();

        install_active_internal_node(&storage, &config.mesh_id, "node-a").await;
        ensure_bootstrapped(&config, &persistence, &storage, &keyring)
            .await
            .unwrap();
        let core_store = CoreStore::new(storage.clone()).await.unwrap();
        let claims = auth::Claims {
            sub: "node-a".to_string(),
            exp: usize::MAX,
            tenant_id: SYSTEM_STORAGE_TENANT_ID,
            jti: None,
        };

        assert!(
            check_internal_node_access(
                &storage,
                &core_store,
                &config.mesh_id,
                &claims,
                "node-a",
                NodeCapability::Metadata,
            )
            .await
            .unwrap()
        );
        assert!(
            !check_internal_node_access(
                &storage,
                &core_store,
                &config.mesh_id,
                &claims,
                "node-a",
                NodeCapability::Gateway,
            )
            .await
            .unwrap()
        );
        assert!(
            !check_internal_node_access(
                &storage,
                &core_store,
                "another-mesh",
                &claims,
                "node-a",
                NodeCapability::Metadata,
            )
            .await
            .unwrap()
        );
        let forged_claims = auth::Claims {
            sub: "node-b".to_string(),
            ..claims
        };
        assert!(
            !check_internal_node_access(
                &storage,
                &core_store,
                &config.mesh_id,
                &forged_claims,
                "node-a",
                NodeCapability::Metadata,
            )
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn system_realm_bootstrap_existing_realm_does_not_grant_again() {
        let temp = tempdir().unwrap();
        let config = test_config(temp.path());
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let persistence = Persistence::new(&config).unwrap();
        let keyring = config.secret_keyring().unwrap();

        ensure_bootstrapped(&config, &persistence, &storage, &keyring)
            .await
            .unwrap();

        ensure_bootstrapped(&config, &persistence, &storage, &keyring)
            .await
            .unwrap();

        let denied = check_admin_relation(
            &storage,
            &config.mesh_id,
            &auth::Claims {
                sub: "new-bootstrap-subject".to_string(),
                exp: usize::MAX,
                tenant_id: 0,
                jti: None,
            },
            SystemAdminRelation::ManageNodes,
        )
        .await
        .unwrap();
        assert!(!denied);
    }

    #[tokio::test]
    async fn system_realm_bootstrap_missing_config_fails_closed() {
        let temp = tempdir().unwrap();
        let mut config = test_config(temp.path());
        config.bootstrap_system_admin_subject_kind.clear();
        config.bootstrap_system_admin_subject_id.clear();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let persistence = Persistence::new(&config).unwrap();
        let keyring = config.secret_keyring().unwrap();

        let err = ensure_bootstrapped(&config, &persistence, &storage, &keyring)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("system realm is missing"));
    }

    #[tokio::test]
    async fn generated_bootstrap_admin_credential_uses_reserved_system_tenant() {
        let temp = tempdir().unwrap();
        let storage_path = temp.path().join("storage");
        let credential_path = temp.path().join("system-admin.json");
        let mut config = test_config(&storage_path);
        config.storage_path = storage_path.to_string_lossy().to_string();
        config.bootstrap_system_admin_subject_kind.clear();
        config.bootstrap_system_admin_subject_id.clear();
        config.bootstrap_system_admin_app_name = "system-admin".to_string();
        config.bootstrap_system_admin_credential_output_path =
            credential_path.to_string_lossy().to_string();

        let storage = Storage::new_at(&storage_path).await.unwrap();
        let persistence = Persistence::new(&config).unwrap();
        let keyring = config.secret_keyring().unwrap();

        ensure_bootstrapped(&config, &persistence, &storage, &keyring)
            .await
            .unwrap();

        let credential: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&credential_path).unwrap()).unwrap();
        assert_eq!(
            credential["tenant_id"].as_i64().unwrap(),
            SYSTEM_STORAGE_TENANT_ID
        );
        assert!(
            persistence
                .get_tenant_by_name("system")
                .await
                .unwrap()
                .is_none()
        );

        let client_id = credential["client_id"].as_str().unwrap();
        let app_details = persistence
            .get_app_by_client_id(client_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(app_details.tenant_id, SYSTEM_STORAGE_TENANT_ID);

        let token = auth::JwtManager::new(config.jwt_secret.clone())
            .mint_token(app_details.id.to_string(), app_details.tenant_id)
            .unwrap();
        let claims = auth::JwtManager::new(config.jwt_secret.clone())
            .verify_token(&token)
            .unwrap();
        assert_eq!(claims.tenant_id, SYSTEM_STORAGE_TENANT_ID);

        let allowed = check_admin_relation(
            &storage,
            &config.mesh_id,
            &claims,
            SystemAdminRelation::ManageTenants,
        )
        .await
        .unwrap();
        assert!(allowed);
    }

    #[test]
    fn bootstrap_credential_output_path_must_not_live_under_storage_path() {
        let temp = tempdir().unwrap();
        let storage_path = temp.path().join("storage");
        std::fs::create_dir_all(&storage_path).unwrap();
        let mut config = test_config(&storage_path);

        let storage_sidecar = storage_path.join("bootstrap-admin.json");
        let err = reject_bootstrap_credential_output_path(&config, &storage_sidecar).unwrap_err();
        assert!(
            err.to_string().contains("must be outside storage_path"),
            "unexpected error: {err:#}"
        );

        config.bootstrap_system_admin_credential_output_path = temp
            .path()
            .join("bootstrap-admin.json")
            .to_string_lossy()
            .to_string();
        reject_bootstrap_credential_output_path(
            &config,
            Path::new(&config.bootstrap_system_admin_credential_output_path),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn system_realm_bootstrap_runs_before_listeners_start() {
        let temp = tempdir().unwrap();
        let storage_path = temp.path().join("storage");
        let mut config = test_config(&storage_path);
        config.storage_path = storage_path.to_string_lossy().to_string();
        config.bootstrap_system_admin_subject_kind.clear();
        config.bootstrap_system_admin_subject_id.clear();

        let err = crate::AppState::new(config, crate::test_support::personaldb_protocol_keyring())
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("system realm is missing"),
            "unexpected error: {err:#}"
        );
    }

    #[tokio::test]
    async fn system_realm_bootstrap_two_node_race_has_one_winner() {
        let temp = tempdir().unwrap();
        let config = test_config(temp.path());
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let persistence = Persistence::new(&config).unwrap();
        let keyring = config.secret_keyring().unwrap();

        let (left, right) = tokio::join!(
            ensure_bootstrapped(&config, &persistence, &storage, &keyring),
            ensure_bootstrapped(&config, &persistence, &storage, &keyring),
        );

        left.unwrap();
        right.unwrap();
        let marker = bootstrap_marker_exists(&storage, &config.mesh_id)
            .await
            .unwrap();
        assert!(marker);
        let allowed = check_admin_relation(
            &storage,
            &config.mesh_id,
            &auth::Claims {
                sub: "admin-principal".to_string(),
                exp: usize::MAX,
                tenant_id: 0,
                jti: None,
            },
            SystemAdminRelation::ManageRegions,
        )
        .await
        .unwrap();
        assert!(allowed);
    }

    #[tokio::test]
    async fn system_realm_bootstrap_partial_crash_recovers_idempotently() {
        let temp = tempdir().unwrap();
        let config = test_config(temp.path());
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let persistence = Persistence::new(&config).unwrap();
        let keyring = config.secret_keyring().unwrap();

        install_system_schema(&storage, &persistence).await.unwrap();
        assert!(
            !bootstrap_marker_exists(&storage, &config.mesh_id)
                .await
                .unwrap()
        );

        ensure_bootstrapped(&config, &persistence, &storage, &keyring)
            .await
            .unwrap();
        assert!(
            bootstrap_marker_exists(&storage, &config.mesh_id)
                .await
                .unwrap()
        );
        for relation in [
            SystemAdminRelation::ManageSecretEncryptionKeys,
            SystemAdminRelation::ManagePersonalDbSigningKeys,
        ] {
            let allowed = check_admin_relation(
                &storage,
                &config.mesh_id,
                &auth::Claims {
                    sub: "admin-principal".to_string(),
                    exp: usize::MAX,
                    tenant_id: 0,
                    jti: None,
                },
                relation,
            )
            .await
            .unwrap();
            assert!(allowed, "bootstrap admin is missing {}", relation.as_str());
        }
    }

    #[tokio::test]
    async fn bootstrap_credential_is_not_accepted_by_public_or_admin_api() {
        let temp = tempdir().unwrap();
        let config = test_config(temp.path());
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let persistence = Persistence::new(&config).unwrap();
        let keyring = config.secret_keyring().unwrap();

        ensure_bootstrapped(&config, &persistence, &storage, &keyring)
            .await
            .unwrap();
        let forged_claims = auth::Claims {
            sub: "forged-bootstrap-token".to_string(),
            exp: usize::MAX,
            tenant_id: 0,
            jti: None,
        };
        let denied = check_admin_relation(
            &storage,
            &config.mesh_id,
            &forged_claims,
            SystemAdminRelation::ManageTenants,
        )
        .await
        .unwrap();
        assert!(!denied);
    }
}
