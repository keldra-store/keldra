use crate::anvil_api::AuthzNamespaceSchema;
use crate::authz_coremeta_payload::{decode_authz_payload_row, encode_authz_payload_row};
use crate::authz_head::{self, AuthzHeadMutation, AuthzHeadSnapshot};
use crate::core_store::{
    CF_AUTHZ, CoreMetaRecord, CoreMetaTuplePart, CoreMutationBatch, CoreMutationOperation,
    CoreMutationPrecondition, CoreMutationRootPublication, CoreStore, TABLE_AUTHZ_SCHEMA_ROW,
    core_meta_committed_row_common, core_meta_payload_digest, core_meta_record_tuple_key,
    core_meta_root_key_hash, core_meta_tuple_key, decode_deterministic_proto,
    encode_deterministic_proto,
};
use crate::formats::{hash32, writer::WriterFamily};
use crate::persistence::AuthzSchemaBindingPrecondition;
use crate::storage::Storage;
use anyhow::{Result, anyhow};
use prost::Message;
use serde::{Deserialize, Serialize};

const AUTHZ_SCHEMA_REVISION_ROW_KIND: &str = "schema_revision";
const AUTHZ_SCHEMA_LATEST_ROW_KIND: &str = "schema_latest";
const AUTHZ_SCHEMA_DIGEST_ROW_KIND: &str = "schema_digest";
const AUTHZ_SCHEMA_BINDING_ROW_KIND: &str = "schema_binding";
pub const AUTHZ_SCHEMA_PAGE_MAX: usize = 1000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredSchemaRef {
    pub schema_id: String,
    pub schema_revision: u64,
    pub schema_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredAuthzSchemaRevision {
    pub schema_ref: StoredSchemaRef,
    pub namespaces: Vec<AuthzNamespaceSchema>,
    pub authz_revision: u64,
    pub written_by: String,
    pub reason: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredAuthzSchemaBinding {
    pub realm_id: String,
    pub schema_ref: StoredSchemaRef,
    pub binding_generation: u64,
    pub authz_revision: u64,
    pub written_by: String,
    pub reason: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct StoredAuthzSchemaRevisionPage {
    pub records: Vec<StoredAuthzSchemaRevision>,
    pub next_tuple_key: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct StoredAuthzSchemaBindingPage {
    pub records: Vec<StoredAuthzSchemaBinding>,
    pub next_tuple_key: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct BoundAuthzSchemaSnapshot {
    pub schema: Option<StoredAuthzSchemaRevision>,
    pub binding_precondition: AuthzSchemaBindingPrecondition,
}

#[derive(Clone, PartialEq, Message)]
struct StoredSchemaRefProto {
    #[prost(string, tag = "1")]
    schema_id: String,
    #[prost(uint64, tag = "2")]
    schema_revision: u64,
    #[prost(string, tag = "3")]
    schema_digest: String,
}

#[derive(Clone, PartialEq, Message)]
struct StoredAuthzSchemaRevisionProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(message, optional, tag = "2")]
    schema_ref: Option<StoredSchemaRefProto>,
    #[prost(message, repeated, tag = "3")]
    namespaces: Vec<AuthzNamespaceSchemaProto>,
    #[prost(uint64, tag = "4")]
    authz_revision: u64,
    #[prost(string, tag = "5")]
    written_by: String,
    #[prost(string, tag = "6")]
    reason: String,
    #[prost(string, tag = "7")]
    created_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct StoredAuthzSchemaBindingProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    realm_id: String,
    #[prost(message, optional, tag = "3")]
    schema_ref: Option<StoredSchemaRefProto>,
    #[prost(uint64, tag = "4")]
    binding_generation: u64,
    #[prost(uint64, tag = "5")]
    authz_revision: u64,
    #[prost(string, tag = "6")]
    written_by: String,
    #[prost(string, tag = "7")]
    reason: String,
    #[prost(string, tag = "8")]
    updated_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzNamespaceSchemaProto {
    #[prost(string, tag = "1")]
    namespace: String,
    #[prost(message, repeated, tag = "2")]
    relations: Vec<AuthzRelationSchemaProto>,
    #[prost(string, tag = "3")]
    schema_json: String,
    #[prost(string, tag = "4")]
    schema_hash: String,
    #[prost(uint64, tag = "5")]
    schema_version: u64,
    #[prost(uint64, tag = "6")]
    authz_revision: u64,
    #[prost(string, tag = "7")]
    applied_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzNamespaceSetProto {
    #[prost(message, repeated, tag = "1")]
    namespaces: Vec<AuthzNamespaceSchemaProto>,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzRelationSchemaProto {
    #[prost(string, tag = "1")]
    relation: String,
    #[prost(message, repeated, tag = "2")]
    rules: Vec<AuthzRelationRuleProto>,
    #[prost(int32, tag = "3")]
    member_kind: i32,
    #[prost(message, repeated, tag = "4")]
    allowed_subjects: Vec<AuthzAllowedSubjectProto>,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzAllowedSubjectProto {
    #[prost(int32, tag = "1")]
    selector_kind: i32,
    #[prost(string, tag = "2")]
    subject_kind: String,
    #[prost(string, tag = "3")]
    subject_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzRelationRuleProto {
    #[prost(string, tag = "1")]
    kind: String,
    #[prost(string, tag = "2")]
    relation: String,
    #[prost(string, tag = "3")]
    tuple_relation: String,
    #[prost(string, tag = "4")]
    target_relation: String,
}

pub async fn put_schema_revision(
    storage: &Storage,
    tenant_id: i64,
    schema_id: &str,
    mut namespaces: Vec<AuthzNamespaceSchema>,
    written_by: &str,
    reason: &str,
) -> Result<StoredAuthzSchemaRevision> {
    let write_lock = authz_head::tenant_write_lock(tenant_id)?;
    let _guard = write_lock.lock().await;
    validate_schema_id(schema_id)?;
    crate::authz_schema_contract::validate_schema_set(&namespaces)?;
    crate::authz_schema_contract::canonicalize_schema_set(&mut namespaces);
    let schema_digest = schema_digest(&namespaces)?;
    if let Some(existing) =
        find_schema_by_digest(storage, tenant_id, schema_id, &schema_digest).await?
    {
        return Ok(existing);
    }
    let latest_key = schema_latest_tuple_key(tenant_id, schema_id)?;
    let latest =
        read_proto_row_snapshot::<StoredAuthzSchemaRevision>(storage, tenant_id, latest_key)
            .await?;
    let next_revision = latest
        .as_ref()
        .map(|(record, _)| record.schema_ref.schema_revision.saturating_add(1))
        .unwrap_or(1);
    let head_snapshot = authz_head::read(storage, tenant_id).await?;
    let authz_revision = head_snapshot
        .head
        .committed_revision
        .checked_add(1)
        .ok_or_else(|| anyhow!("authorization revision overflow"))?;
    let record = StoredAuthzSchemaRevision {
        schema_ref: StoredSchemaRef {
            schema_id: schema_id.to_string(),
            schema_revision: next_revision,
            schema_digest,
        },
        namespaces,
        authz_revision,
        written_by: written_by.to_string(),
        reason: reason.to_string(),
        created_at: chrono::Utc::now().to_rfc3339(),
    };
    match write_schema_record(
        storage,
        tenant_id,
        &record,
        &head_snapshot,
        latest.map(|(_, payload_hash)| payload_hash),
    )
    .await
    {
        Ok(()) => {
            crate::authz_journal::materialize_authz_tuple_segment(
                storage,
                tenant_id,
                authz_revision,
            )
            .await?;
            Ok(record)
        }
        Err(err) => {
            // Concurrent bootstrap/schema writers may race on the same deterministic
            // revision ref. If the winner wrote the identical schema digest, the
            // operation is idempotent; otherwise surface the conflict.
            if let Some(existing) = find_schema_by_digest(
                storage,
                tenant_id,
                schema_id,
                &record.schema_ref.schema_digest,
            )
            .await?
            {
                Ok(existing)
            } else {
                Err(err)
            }
        }
    }
}

pub async fn read_schema_revision(
    storage: &Storage,
    tenant_id: i64,
    schema_id: &str,
    revision: Option<u64>,
) -> Result<Option<StoredAuthzSchemaRevision>> {
    validate_schema_id(schema_id)?;
    match revision {
        Some(revision) => {
            read_proto_row(
                storage,
                tenant_id,
                schema_revision_tuple_key(tenant_id, schema_id, revision)?,
            )
            .await
        }
        None => read_latest_schema_revision(storage, tenant_id, schema_id).await,
    }
}

pub async fn bind_schema(
    storage: &Storage,
    tenant_id: i64,
    realm_id: &str,
    schema_ref: StoredSchemaRef,
    expected_generation: Option<u64>,
    written_by: &str,
    reason: &str,
) -> Result<StoredAuthzSchemaBinding> {
    let write_lock = authz_head::tenant_write_lock(tenant_id)?;
    let _guard = write_lock.lock().await;
    validate_realm_id(realm_id)?;
    let schema = read_schema_revision(
        storage,
        tenant_id,
        &schema_ref.schema_id,
        Some(schema_ref.schema_revision),
    )
    .await?
    .ok_or_else(|| anyhow!("authorization schema revision not found"))?;
    validate_stored_schema_revision(&schema)?;
    if schema.schema_ref != schema_ref {
        return Err(anyhow!("authorization schema reference digest mismatch"));
    }
    let binding_tuple_key = schema_binding_tuple_key(tenant_id, realm_id)?;
    let current =
        read_proto_row_snapshot::<StoredAuthzSchemaBinding>(storage, tenant_id, binding_tuple_key)
            .await?;
    let actual = current
        .as_ref()
        .map(|(binding, _)| binding.binding_generation);
    match (expected_generation, actual) {
        (None, None) | (Some(0), None) => {}
        (Some(expected), Some(actual)) if expected == actual => {}
        _ => return Err(anyhow!("schema binding generation conflict")),
    }
    let head_snapshot = authz_head::read(storage, tenant_id).await?;
    let authz_revision = head_snapshot
        .head
        .committed_revision
        .checked_add(1)
        .ok_or_else(|| anyhow!("authorization revision overflow"))?;
    let binding = StoredAuthzSchemaBinding {
        realm_id: realm_id.to_string(),
        schema_ref,
        binding_generation: actual.map(|value| value.saturating_add(1)).unwrap_or(1),
        authz_revision,
        written_by: written_by.to_string(),
        reason: reason.to_string(),
        updated_at: chrono::Utc::now().to_rfc3339(),
    };
    write_binding_record(
        storage,
        tenant_id,
        &binding,
        &head_snapshot,
        current.map(|(_, payload_hash)| payload_hash),
    )
    .await?;
    crate::authz_journal::materialize_authz_tuple_segment(storage, tenant_id, authz_revision)
        .await?;
    Ok(binding)
}

pub async fn read_schema_binding(
    storage: &Storage,
    tenant_id: i64,
    realm_id: &str,
) -> Result<Option<StoredAuthzSchemaBinding>> {
    validate_realm_id(realm_id)?;
    read_proto_row(
        storage,
        tenant_id,
        schema_binding_tuple_key(tenant_id, realm_id)?,
    )
    .await
}

pub async fn read_bound_schema_snapshot(
    storage: &Storage,
    tenant_id: i64,
    realm_id: &str,
) -> Result<BoundAuthzSchemaSnapshot> {
    validate_realm_id(realm_id)?;
    let tuple_key = schema_binding_tuple_key(tenant_id, realm_id)?;
    let store = CoreStore::new(storage.clone()).await?;
    let Some(payload) = store.read_coremeta_row(CF_AUTHZ, TABLE_AUTHZ_SCHEMA_ROW, &tuple_key)?
    else {
        return Ok(BoundAuthzSchemaSnapshot {
            schema: None,
            binding_precondition: AuthzSchemaBindingPrecondition {
                tuple_key,
                expected_payload_hash: None,
            },
        });
    };
    let binding =
        decode_schema_record_row::<StoredAuthzSchemaBinding>(storage, tenant_id, &payload).await?;
    let schema = read_schema_revision(
        storage,
        tenant_id,
        &binding.schema_ref.schema_id,
        Some(binding.schema_ref.schema_revision),
    )
    .await?
    .ok_or_else(|| anyhow!("bound authorization schema revision not found"))?;
    validate_stored_schema_revision(&schema)?;
    if schema.schema_ref != binding.schema_ref {
        return Err(anyhow!("bound authorization schema reference mismatch"));
    }
    Ok(BoundAuthzSchemaSnapshot {
        schema: Some(schema),
        binding_precondition: AuthzSchemaBindingPrecondition {
            tuple_key,
            expected_payload_hash: Some(core_meta_payload_digest(TABLE_AUTHZ_SCHEMA_ROW, &payload)),
        },
    })
}

pub async fn page_schema_revisions(
    storage: &Storage,
    tenant_id: i64,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> Result<StoredAuthzSchemaRevisionPage> {
    validate_storage_tenant(tenant_id)?;
    let store = CoreStore::new(storage.clone()).await?;
    let prefix = core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(AUTHZ_SCHEMA_REVISION_ROW_KIND),
        CoreMetaTuplePart::I64(tenant_id),
    ])?;
    let (rows, next_tuple_key) =
        scan_schema_rows_page(&store, &prefix, after_tuple_key, page_size)?;
    let mut records = Vec::with_capacity(rows.len());
    for row in rows {
        records.push(
            decode_schema_record_row::<StoredAuthzSchemaRevision>(storage, tenant_id, &row.payload)
                .await?,
        );
    }
    Ok(StoredAuthzSchemaRevisionPage {
        records,
        next_tuple_key,
    })
}

pub async fn page_schema_bindings(
    storage: &Storage,
    tenant_id: i64,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> Result<StoredAuthzSchemaBindingPage> {
    validate_storage_tenant(tenant_id)?;
    let store = CoreStore::new(storage.clone()).await?;
    let prefix = core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(AUTHZ_SCHEMA_BINDING_ROW_KIND),
        CoreMetaTuplePart::I64(tenant_id),
    ])?;
    let (rows, next_tuple_key) =
        scan_schema_rows_page(&store, &prefix, after_tuple_key, page_size)?;
    let mut records = Vec::with_capacity(rows.len());
    for row in rows {
        records.push(
            decode_schema_record_row::<StoredAuthzSchemaBinding>(storage, tenant_id, &row.payload)
                .await?,
        );
    }
    Ok(StoredAuthzSchemaBindingPage {
        records,
        next_tuple_key,
    })
}

fn scan_schema_rows_page(
    store: &CoreStore,
    prefix: &[u8],
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> Result<(Vec<CoreMetaRecord>, Option<Vec<u8>>)> {
    if !(1..=AUTHZ_SCHEMA_PAGE_MAX).contains(&page_size) {
        return Err(anyhow!(
            "authorization schema page size must be between 1 and {AUTHZ_SCHEMA_PAGE_MAX}"
        ));
    }
    if after_tuple_key
        .is_some_and(|cursor| cursor.len() <= prefix.len() || !cursor.starts_with(prefix))
    {
        return Err(anyhow!(
            "authorization schema cursor is outside the tenant prefix"
        ));
    }
    let mut rows = store.scan_coremeta_prefix_page(
        CF_AUTHZ,
        TABLE_AUTHZ_SCHEMA_ROW,
        prefix,
        after_tuple_key,
        page_size + 1,
    )?;
    let has_more = rows.len() > page_size;
    if has_more {
        rows.truncate(page_size);
    }
    let next_tuple_key = if has_more {
        Some(
            core_meta_record_tuple_key(
                &rows
                    .last()
                    .ok_or_else(|| anyhow!("authorization schema page is empty"))?
                    .key,
            )?
            .to_vec(),
        )
    } else {
        None
    };
    Ok((rows, next_tuple_key))
}

pub async fn read_bound_namespace_schema(
    storage: &Storage,
    tenant_id: i64,
    realm_id: &str,
    namespace: &str,
) -> Result<Option<AuthzNamespaceSchema>> {
    validate_realm_id(realm_id)?;
    validate_component(namespace, "authorization namespace")?;
    let Some(binding) = read_schema_binding(storage, tenant_id, realm_id).await? else {
        return Ok(None);
    };
    let Some(revision) = read_schema_revision(
        storage,
        tenant_id,
        &binding.schema_ref.schema_id,
        Some(binding.schema_ref.schema_revision),
    )
    .await?
    else {
        return Err(anyhow!("bound authorization schema revision not found"));
    };
    validate_stored_schema_revision(&revision)?;
    if revision.schema_ref != binding.schema_ref {
        return Err(anyhow!("bound authorization schema reference mismatch"));
    }
    Ok(revision
        .namespaces
        .into_iter()
        .find(|schema| schema.namespace == namespace))
}

async fn write_schema_record(
    storage: &Storage,
    tenant_id: i64,
    record: &StoredAuthzSchemaRevision,
    head_snapshot: &AuthzHeadSnapshot,
    expected_latest_payload_hash: Option<String>,
) -> Result<()> {
    let revision_key = schema_revision_tuple_key(
        tenant_id,
        &record.schema_ref.schema_id,
        record.schema_ref.schema_revision,
    )?;
    let latest_key = schema_latest_tuple_key(tenant_id, &record.schema_ref.schema_id)?;
    let digest_key = schema_digest_tuple_key(
        tenant_id,
        &record.schema_ref.schema_id,
        &record.schema_ref.schema_digest,
    )?;
    let transaction_id = schema_revision_transaction_id(tenant_id, record);
    let partition_id = authz_head::transaction_partition(tenant_id);
    let head = authz_head::advance(
        head_snapshot,
        &transaction_id,
        AuthzHeadMutation::SchemaRevision,
    )?;
    if head.committed_revision != record.authz_revision {
        return Err(anyhow!(
            "authorization schema revision does not advance the authorization head"
        ));
    }
    let operations = vec![
        proto_put_operation(
            storage,
            tenant_id,
            &partition_id,
            revision_key.clone(),
            record,
            &transaction_id,
        )
        .await?,
        proto_put_operation(
            storage,
            tenant_id,
            &partition_id,
            latest_key,
            record,
            &transaction_id,
        )
        .await?,
        proto_put_operation(
            storage,
            tenant_id,
            &partition_id,
            digest_key.clone(),
            record,
            &transaction_id,
        )
        .await?,
        authz_head::put_operation(&partition_id, &transaction_id, &head)?,
    ];
    CoreStore::new(storage.clone())
        .await?
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id,
            scope_partition: partition_id.clone(),
            committed_by_principal: authz_head::transaction_principal(tenant_id),
            root_publications: authz_schema_root_publications(&partition_id, tenant_id),
            preconditions: vec![
                CoreMutationPrecondition::CoreMetaRow {
                    cf: CF_AUTHZ.to_string(),
                    table_id: TABLE_AUTHZ_SCHEMA_ROW,
                    tuple_key: revision_key,
                    expected_payload_hash: None,
                    require_absent: true,
                    require_present: false,
                },
                CoreMutationPrecondition::CoreMetaRow {
                    cf: CF_AUTHZ.to_string(),
                    table_id: TABLE_AUTHZ_SCHEMA_ROW,
                    tuple_key: schema_latest_tuple_key(tenant_id, &record.schema_ref.schema_id)?,
                    expected_payload_hash: expected_latest_payload_hash.clone(),
                    require_absent: expected_latest_payload_hash.is_none(),
                    require_present: expected_latest_payload_hash.is_some(),
                },
                CoreMutationPrecondition::CoreMetaRow {
                    cf: CF_AUTHZ.to_string(),
                    table_id: TABLE_AUTHZ_SCHEMA_ROW,
                    tuple_key: digest_key,
                    expected_payload_hash: None,
                    require_absent: true,
                    require_present: false,
                },
                authz_head::precondition(head_snapshot)?,
            ],
            operations,
        })
        .await?;
    Ok(())
}

async fn write_binding_record(
    storage: &Storage,
    tenant_id: i64,
    binding: &StoredAuthzSchemaBinding,
    head_snapshot: &AuthzHeadSnapshot,
    expected_payload_hash: Option<String>,
) -> Result<()> {
    let tuple_key = schema_binding_tuple_key(tenant_id, &binding.realm_id)?;
    let transaction_id = schema_binding_transaction_id(tenant_id, binding);
    let partition_id = authz_head::transaction_partition(tenant_id);
    let head = authz_head::advance(
        head_snapshot,
        &transaction_id,
        AuthzHeadMutation::SchemaBinding {
            realm_id: &binding.realm_id,
            schema_id: &binding.schema_ref.schema_id,
            schema_revision: binding.schema_ref.schema_revision,
            schema_digest: &binding.schema_ref.schema_digest,
            binding_generation: binding.binding_generation,
        },
    )?;
    if head.committed_revision != binding.authz_revision {
        return Err(anyhow!(
            "authorization schema binding does not advance the authorization head"
        ));
    }
    let operation = proto_put_operation(
        storage,
        tenant_id,
        &partition_id,
        tuple_key,
        binding,
        &transaction_id,
    )
    .await?;
    let head_operation = authz_head::put_operation(&partition_id, &transaction_id, &head)?;
    CoreStore::new(storage.clone())
        .await?
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id,
            scope_partition: partition_id.clone(),
            committed_by_principal: authz_head::transaction_principal(tenant_id),
            root_publications: authz_schema_root_publications(&partition_id, tenant_id),
            preconditions: vec![
                CoreMutationPrecondition::CoreMetaRow {
                    cf: CF_AUTHZ.to_string(),
                    table_id: TABLE_AUTHZ_SCHEMA_ROW,
                    tuple_key: schema_binding_tuple_key(tenant_id, &binding.realm_id)?,
                    expected_payload_hash: expected_payload_hash.clone(),
                    require_absent: expected_payload_hash.is_none(),
                    require_present: expected_payload_hash.is_some(),
                },
                authz_head::precondition(head_snapshot)?,
            ],
            operations: vec![operation, head_operation],
        })
        .await?;
    Ok(())
}

fn authz_schema_root_publications(
    coordinator_root: &str,
    tenant_id: i64,
) -> Vec<CoreMutationRootPublication> {
    vec![
        CoreMutationRootPublication::new(coordinator_root, WriterFamily::CoreControl.as_str())
            .coordinator(),
        CoreMutationRootPublication::new(
            authz_head::root_anchor_key(tenant_id),
            WriterFamily::Authz.as_str(),
        ),
    ]
}

fn schema_revision_transaction_id(tenant_id: i64, record: &StoredAuthzSchemaRevision) -> String {
    format!(
        "authz-schema:{tenant_id}:{}:{}",
        record.schema_ref.schema_revision, record.schema_ref.schema_digest
    )
}

fn schema_binding_transaction_id(tenant_id: i64, binding: &StoredAuthzSchemaBinding) -> String {
    let identity = format!(
        "{tenant_id}\0{}\0{}\0{}\0{}\0{}",
        binding.realm_id,
        binding.schema_ref.schema_id,
        binding.schema_ref.schema_revision,
        binding.schema_ref.schema_digest,
        binding.binding_generation
    );
    format!(
        "authz-schema-binding:{}",
        hex::encode(hash32(identity.as_bytes()))
    )
}

async fn read_latest_schema_revision(
    storage: &Storage,
    tenant_id: i64,
    schema_id: &str,
) -> Result<Option<StoredAuthzSchemaRevision>> {
    read_proto_row(
        storage,
        tenant_id,
        schema_latest_tuple_key(tenant_id, schema_id)?,
    )
    .await
}

async fn find_schema_by_digest(
    storage: &Storage,
    tenant_id: i64,
    schema_id: &str,
    digest: &str,
) -> Result<Option<StoredAuthzSchemaRevision>> {
    let store = CoreStore::new(storage.clone()).await?;
    let Some(payload) = store.read_coremeta_row(
        CF_AUTHZ,
        TABLE_AUTHZ_SCHEMA_ROW,
        &schema_digest_tuple_key(tenant_id, schema_id, digest)?,
    )?
    else {
        return Ok(None);
    };
    let record =
        decode_schema_record_row::<StoredAuthzSchemaRevision>(storage, tenant_id, &payload).await?;
    if record.schema_ref.schema_id != schema_id || record.schema_ref.schema_digest != digest {
        return Err(anyhow!("authorization schema digest lookup row mismatch"));
    }
    Ok(Some(record))
}

async fn read_proto_row<T: AuthzSchemaRecordCodec>(
    storage: &Storage,
    tenant_id: i64,
    tuple_key: Vec<u8>,
) -> Result<Option<T>> {
    Ok(read_proto_row_snapshot::<T>(storage, tenant_id, tuple_key)
        .await?
        .map(|(record, _)| record))
}

async fn read_proto_row_snapshot<T: AuthzSchemaRecordCodec>(
    storage: &Storage,
    tenant_id: i64,
    tuple_key: Vec<u8>,
) -> Result<Option<(T, String)>> {
    let store = CoreStore::new(storage.clone()).await?;
    let Some(bytes) = store.read_coremeta_row(CF_AUTHZ, TABLE_AUTHZ_SCHEMA_ROW, &tuple_key)? else {
        return Ok(None);
    };
    let payload_hash = core_meta_payload_digest(TABLE_AUTHZ_SCHEMA_ROW, &bytes);
    Ok(Some((
        decode_schema_record_row::<T>(storage, tenant_id, &bytes).await?,
        payload_hash,
    )))
}

async fn proto_put_operation<T: AuthzSchemaRecordCodec>(
    storage: &Storage,
    tenant_id: i64,
    partition_id: &str,
    tuple_key: Vec<u8>,
    value: &T,
    transaction_id: &str,
) -> Result<CoreMutationOperation> {
    let record_payload = value.encode_record(tenant_id, transaction_id)?;
    let payload = encode_authz_payload_row(
        storage,
        T::row_common(value, tenant_id, transaction_id)?,
        T::payload_kind(),
        &hex::encode(&tuple_key),
        T::payload_generation(value),
        transaction_id,
        record_payload,
    )
    .await?;
    Ok(CoreMutationOperation::CoreMetaPut {
        partition_id: partition_id.to_string(),
        cf: CF_AUTHZ.to_string(),
        table_id: TABLE_AUTHZ_SCHEMA_ROW,
        tuple_key,
        payload,
    })
}

trait AuthzSchemaRecordCodec: Sized {
    fn payload_kind() -> &'static str;
    fn payload_generation(&self) -> u64;
    fn row_common(
        &self,
        tenant_id: i64,
        transaction_id: &str,
    ) -> Result<crate::core_store::CoreMetaRowCommonProto>;
    fn encode_record(&self, tenant_id: i64, transaction_id: &str) -> Result<Vec<u8>>;
    fn decode_record(bytes: &[u8], tenant_id: i64) -> Result<Self>;
}

impl AuthzSchemaRecordCodec for StoredAuthzSchemaRevision {
    fn payload_kind() -> &'static str {
        AUTHZ_SCHEMA_REVISION_ROW_KIND
    }

    fn payload_generation(&self) -> u64 {
        self.schema_ref.schema_revision
    }

    fn row_common(
        &self,
        tenant_id: i64,
        transaction_id: &str,
    ) -> Result<crate::core_store::CoreMetaRowCommonProto> {
        schema_revision_common(self, tenant_id, transaction_id)
    }

    fn encode_record(&self, tenant_id: i64, transaction_id: &str) -> Result<Vec<u8>> {
        Ok(encode_deterministic_proto(&schema_revision_to_proto(
            self,
            tenant_id,
            transaction_id,
        )?))
    }

    fn decode_record(bytes: &[u8], tenant_id: i64) -> Result<Self> {
        schema_revision_from_proto(
            decode_deterministic_proto::<StoredAuthzSchemaRevisionProto>(
                bytes,
                "authorization schema revision",
            )?,
            tenant_id,
        )
    }
}

impl AuthzSchemaRecordCodec for StoredAuthzSchemaBinding {
    fn payload_kind() -> &'static str {
        AUTHZ_SCHEMA_BINDING_ROW_KIND
    }

    fn payload_generation(&self) -> u64 {
        self.binding_generation
    }

    fn row_common(
        &self,
        tenant_id: i64,
        transaction_id: &str,
    ) -> Result<crate::core_store::CoreMetaRowCommonProto> {
        schema_binding_common(self, tenant_id, transaction_id)
    }

    fn encode_record(&self, tenant_id: i64, transaction_id: &str) -> Result<Vec<u8>> {
        Ok(encode_deterministic_proto(&schema_binding_to_proto(
            self,
            tenant_id,
            transaction_id,
        )?))
    }

    fn decode_record(bytes: &[u8], tenant_id: i64) -> Result<Self> {
        schema_binding_from_proto(
            decode_deterministic_proto::<StoredAuthzSchemaBindingProto>(
                bytes,
                "authorization schema binding",
            )?,
            tenant_id,
        )
    }
}

async fn decode_schema_record_row<T: AuthzSchemaRecordCodec>(
    storage: &Storage,
    tenant_id: i64,
    row_payload: &[u8],
) -> Result<T> {
    let record_payload =
        decode_authz_payload_row(storage, tenant_id, row_payload, T::payload_kind()).await?;
    T::decode_record(&record_payload, tenant_id)
}

fn schema_ref_to_proto(schema_ref: &StoredSchemaRef) -> StoredSchemaRefProto {
    StoredSchemaRefProto {
        schema_id: schema_ref.schema_id.clone(),
        schema_revision: schema_ref.schema_revision,
        schema_digest: schema_ref.schema_digest.clone(),
    }
}

fn schema_ref_from_proto(proto: StoredSchemaRefProto) -> StoredSchemaRef {
    StoredSchemaRef {
        schema_id: proto.schema_id,
        schema_revision: proto.schema_revision,
        schema_digest: proto.schema_digest,
    }
}

fn schema_revision_to_proto(
    record: &StoredAuthzSchemaRevision,
    tenant_id: i64,
    transaction_id: &str,
) -> Result<StoredAuthzSchemaRevisionProto> {
    Ok(StoredAuthzSchemaRevisionProto {
        common: Some(schema_revision_common(record, tenant_id, transaction_id)?),
        schema_ref: Some(schema_ref_to_proto(&record.schema_ref)),
        namespaces: record.namespaces.iter().map(namespace_to_proto).collect(),
        authz_revision: record.authz_revision,
        written_by: record.written_by.clone(),
        reason: record.reason.clone(),
        created_at: record.created_at.clone(),
    })
}

fn schema_revision_common(
    record: &StoredAuthzSchemaRevision,
    tenant_id: i64,
    transaction_id: &str,
) -> Result<crate::core_store::CoreMetaRowCommonProto> {
    Ok(core_meta_committed_row_common(
        format!("tenant/{tenant_id}/authz"),
        core_meta_root_key_hash(&format!("authz/{tenant_id}")),
        record.authz_revision,
        transaction_id,
        timestamp_nanos(&record.created_at, "authorization schema created_at")?,
    ))
}

fn schema_revision_from_proto(
    proto: StoredAuthzSchemaRevisionProto,
    tenant_id: i64,
) -> Result<StoredAuthzSchemaRevision> {
    let common = proto
        .common
        .clone()
        .ok_or_else(|| anyhow!("authorization schema revision row missing CoreMeta common"))?;
    let record = StoredAuthzSchemaRevision {
        schema_ref: schema_ref_from_proto(
            proto
                .schema_ref
                .ok_or_else(|| anyhow!("authorization schema revision missing schema_ref"))?,
        ),
        namespaces: proto
            .namespaces
            .into_iter()
            .map(namespace_from_proto)
            .collect(),
        authz_revision: proto.authz_revision,
        written_by: proto.written_by,
        reason: proto.reason,
        created_at: proto.created_at,
    };
    let transaction_id = schema_revision_transaction_id(tenant_id, &record);
    if common != schema_revision_common(&record, tenant_id, &transaction_id)? {
        return Err(anyhow!(
            "authorization schema revision CoreMeta common mismatch"
        ));
    }
    Ok(record)
}

fn schema_binding_to_proto(
    record: &StoredAuthzSchemaBinding,
    tenant_id: i64,
    transaction_id: &str,
) -> Result<StoredAuthzSchemaBindingProto> {
    Ok(StoredAuthzSchemaBindingProto {
        common: Some(schema_binding_common(record, tenant_id, transaction_id)?),
        realm_id: record.realm_id.clone(),
        schema_ref: Some(schema_ref_to_proto(&record.schema_ref)),
        binding_generation: record.binding_generation,
        authz_revision: record.authz_revision,
        written_by: record.written_by.clone(),
        reason: record.reason.clone(),
        updated_at: record.updated_at.clone(),
    })
}

fn schema_binding_common(
    record: &StoredAuthzSchemaBinding,
    tenant_id: i64,
    transaction_id: &str,
) -> Result<crate::core_store::CoreMetaRowCommonProto> {
    Ok(core_meta_committed_row_common(
        format!("tenant/{tenant_id}/authz"),
        core_meta_root_key_hash(&format!("authz/{tenant_id}")),
        record.authz_revision,
        transaction_id,
        timestamp_nanos(&record.updated_at, "authorization binding updated_at")?,
    ))
}

fn schema_binding_from_proto(
    proto: StoredAuthzSchemaBindingProto,
    tenant_id: i64,
) -> Result<StoredAuthzSchemaBinding> {
    let common = proto
        .common
        .clone()
        .ok_or_else(|| anyhow!("authorization schema binding row missing CoreMeta common"))?;
    let binding = StoredAuthzSchemaBinding {
        realm_id: proto.realm_id,
        schema_ref: schema_ref_from_proto(
            proto
                .schema_ref
                .ok_or_else(|| anyhow!("authorization schema binding missing schema_ref"))?,
        ),
        binding_generation: proto.binding_generation,
        authz_revision: proto.authz_revision,
        written_by: proto.written_by,
        reason: proto.reason,
        updated_at: proto.updated_at,
    };
    let transaction_id = schema_binding_transaction_id(tenant_id, &binding);
    if common != schema_binding_common(&binding, tenant_id, &transaction_id)? {
        return Err(anyhow!(
            "authorization schema binding CoreMeta common mismatch"
        ));
    }
    Ok(binding)
}

fn timestamp_nanos(value: &str, label: &str) -> Result<u64> {
    let timestamp = chrono::DateTime::parse_from_rfc3339(value)
        .map_err(|error| anyhow!("{label} is invalid: {error}"))?
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("{label} is outside the supported range"))?;
    u64::try_from(timestamp).map_err(|_| anyhow!("{label} must not precede the Unix epoch"))
}

fn namespace_to_proto(namespace: &AuthzNamespaceSchema) -> AuthzNamespaceSchemaProto {
    AuthzNamespaceSchemaProto {
        namespace: namespace.namespace.clone(),
        relations: namespace.relations.iter().map(relation_to_proto).collect(),
        schema_json: namespace.schema_json.clone(),
        schema_hash: namespace.schema_hash.clone(),
        schema_version: namespace.schema_version,
        authz_revision: namespace.authz_revision,
        applied_at: namespace.applied_at.clone(),
    }
}

fn namespace_from_proto(proto: AuthzNamespaceSchemaProto) -> AuthzNamespaceSchema {
    AuthzNamespaceSchema {
        namespace: proto.namespace,
        relations: proto
            .relations
            .into_iter()
            .map(relation_from_proto)
            .collect(),
        schema_json: proto.schema_json,
        schema_hash: proto.schema_hash,
        schema_version: proto.schema_version,
        authz_revision: proto.authz_revision,
        applied_at: proto.applied_at,
    }
}

fn relation_to_proto(relation: &crate::anvil_api::AuthzRelationSchema) -> AuthzRelationSchemaProto {
    AuthzRelationSchemaProto {
        relation: relation.relation.clone(),
        rules: relation.rules.iter().map(rule_to_proto).collect(),
        member_kind: relation.member_kind,
        allowed_subjects: relation
            .allowed_subjects
            .iter()
            .map(allowed_subject_to_proto)
            .collect(),
    }
}

fn relation_from_proto(proto: AuthzRelationSchemaProto) -> crate::anvil_api::AuthzRelationSchema {
    crate::anvil_api::AuthzRelationSchema {
        relation: proto.relation,
        rules: proto.rules.into_iter().map(rule_from_proto).collect(),
        member_kind: proto.member_kind,
        allowed_subjects: proto
            .allowed_subjects
            .into_iter()
            .map(allowed_subject_from_proto)
            .collect(),
    }
}

fn allowed_subject_to_proto(
    selector: &crate::anvil_api::AuthzAllowedSubject,
) -> AuthzAllowedSubjectProto {
    AuthzAllowedSubjectProto {
        selector_kind: selector.selector_kind,
        subject_kind: selector.subject_kind.clone(),
        subject_id: selector.subject_id.clone(),
    }
}

fn allowed_subject_from_proto(
    proto: AuthzAllowedSubjectProto,
) -> crate::anvil_api::AuthzAllowedSubject {
    crate::anvil_api::AuthzAllowedSubject {
        selector_kind: proto.selector_kind,
        subject_kind: proto.subject_kind,
        subject_id: proto.subject_id,
    }
}

fn rule_to_proto(rule: &crate::anvil_api::AuthzRelationRule) -> AuthzRelationRuleProto {
    AuthzRelationRuleProto {
        kind: rule.kind.clone(),
        relation: rule.relation.clone(),
        tuple_relation: rule.tuple_relation.clone(),
        target_relation: rule.target_relation.clone(),
    }
}

fn rule_from_proto(proto: AuthzRelationRuleProto) -> crate::anvil_api::AuthzRelationRule {
    crate::anvil_api::AuthzRelationRule {
        kind: proto.kind,
        relation: proto.relation,
        tuple_relation: proto.tuple_relation,
        target_relation: proto.target_relation,
    }
}

fn schema_digest(namespaces: &[AuthzNamespaceSchema]) -> Result<String> {
    let mut namespaces = namespaces.to_vec();
    crate::authz_schema_contract::canonicalize_schema_set(&mut namespaces);
    let bytes = encode_deterministic_proto(&AuthzNamespaceSetProto {
        namespaces: namespaces.iter().map(namespace_to_proto).collect(),
    });
    Ok(hex::encode(hash32(&bytes)))
}

fn validate_stored_schema_revision(record: &StoredAuthzSchemaRevision) -> Result<()> {
    if record.schema_ref.schema_revision == 0 {
        return Err(anyhow!("authorization schema revision must be nonzero"));
    }
    crate::authz_schema_contract::validate_schema_set(&record.namespaces)?;
    let digest = schema_digest(&record.namespaces)?;
    if digest != record.schema_ref.schema_digest {
        return Err(anyhow!("authorization schema revision digest mismatch"));
    }
    Ok(())
}

fn validate_schema_id(value: &str) -> Result<()> {
    validate_component(value, "authorization schema id")
}

fn validate_realm_id(value: &str) -> Result<()> {
    if value == crate::system_realm::SYSTEM_REALM_ID {
        return Ok(());
    }
    validate_component(value, "authorization realm id")
}

fn validate_component(value: &str, name: &str) -> Result<()> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains(':')
        || value.chars().any(char::is_control)
    {
        Err(anyhow!("invalid {name}"))
    } else {
        Ok(())
    }
}

fn schema_digest_tuple_key(tenant_id: i64, schema_id: &str, digest: &str) -> Result<Vec<u8>> {
    validate_storage_tenant(tenant_id)?;
    validate_schema_id(schema_id)?;
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!(
            "authorization schema digest must be a SHA-256 hex digest"
        ));
    }
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(AUTHZ_SCHEMA_DIGEST_ROW_KIND),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(schema_id),
        CoreMetaTuplePart::Utf8(digest),
    ])
}

fn schema_revision_tuple_key(tenant_id: i64, schema_id: &str, revision: u64) -> Result<Vec<u8>> {
    if revision == 0 {
        return Err(anyhow!("authorization schema revision must be nonzero"));
    }
    validate_storage_tenant(tenant_id)?;
    validate_schema_id(schema_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(AUTHZ_SCHEMA_REVISION_ROW_KIND),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(schema_id),
        CoreMetaTuplePart::U64(revision),
    ])
}

fn schema_latest_tuple_key(tenant_id: i64, schema_id: &str) -> Result<Vec<u8>> {
    validate_storage_tenant(tenant_id)?;
    validate_schema_id(schema_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(AUTHZ_SCHEMA_LATEST_ROW_KIND),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(schema_id),
    ])
}

fn schema_binding_tuple_key(tenant_id: i64, realm_id: &str) -> Result<Vec<u8>> {
    validate_storage_tenant(tenant_id)?;
    validate_realm_id(realm_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(AUTHZ_SCHEMA_BINDING_ROW_KIND),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(realm_id),
    ])
}

fn validate_storage_tenant(tenant_id: i64) -> Result<()> {
    if tenant_id < 0 {
        Err(anyhow!(
            "authorization storage tenant id must be nonnegative"
        ))
    } else {
        Ok(())
    }
}
