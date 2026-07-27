use crate::anvil_api::AuthzNamespaceSchema;
use crate::authz_head::{self, AuthzHeadMutation};
use crate::core_store::{
    CF_AUTHZ, CoreMetaTuplePart, TABLE_AUTHZ_SCHEMA_ROW, core_meta_tuple_key,
    decode_deterministic_proto, encode_deterministic_proto,
};
use crate::formats::hash32;
use crate::storage::Storage;
use anyhow::{Result, anyhow, bail};
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
pub struct MvccBoundAuthzSchemaSnapshot {
    pub schema: Option<StoredAuthzSchemaRevision>,
    pub binding_key: crate::mvcc_transaction::LogicalKey,
    pub binding_predicate: crate::mvcc_transaction::PredicateKind,
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
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    schema_id: &str,
    mut namespaces: Vec<AuthzNamespaceSchema>,
    written_by: &str,
    reason: &str,
    caller_binding: Option<crate::authz_journal::AuthzTransactionBinding<'_>>,
) -> Result<StoredAuthzSchemaRevision> {
    validate_schema_id(schema_id)?;
    crate::authz_schema_contract::validate_schema_set(&namespaces)?;
    crate::authz_schema_contract::canonicalize_schema_set(&mut namespaces);
    let canonical_schema_digest = schema_digest(&namespaces)?;
    let digest_key = schema_digest_tuple_key(tenant_id, schema_id, &canonical_schema_digest)?;
    if let Some(existing) = read_proto_row_latest_mvcc::<StoredAuthzSchemaRevision>(
        storage,
        mvcc,
        tenant_id,
        digest_key.clone(),
    )
    .await?
    {
        return Ok(existing);
    }
    let principal = caller_binding
        .map(|binding| binding.principal.to_string())
        .unwrap_or_else(|| authz_head::transaction_principal(tenant_id));
    let idempotency_key = format!("authz-schema:{tenant_id}:{schema_id}:{canonical_schema_digest}");
    let now_unix_ms = current_unix_ms();
    let handle = if caller_binding.is_none() {
        Some(
            mvcc.open_transactions
                .begin(
                    mvcc.runtime.as_ref(),
                    mvcc.cluster_id(),
                    &principal,
                    idempotency_key,
                    std::time::Duration::from_secs(30),
                    crate::mvcc_transaction::DurabilityLevel::Quorum,
                    crate::mvcc_transaction::ReadConsistency::Linearized,
                    now_unix_ms,
                )
                .await?,
        )
    } else {
        None
    };
    let transaction_id = caller_binding
        .map(|binding| binding.transaction_id)
        .or_else(|| handle.as_ref().map(|handle| handle.transaction_id.as_str()))
        .expect("schema transaction binding exists");
    let latest_key = schema_latest_tuple_key(tenant_id, schema_id)?;
    let latest = read_proto_row_transaction_mvcc::<StoredAuthzSchemaRevision>(
        storage,
        mvcc,
        transaction_id,
        &principal,
        tenant_id,
        latest_key.clone(),
    )
    .await?;
    let next_revision = latest
        .as_ref()
        .map(|record| record.schema_ref.schema_revision.saturating_add(1))
        .unwrap_or(1);
    let head_snapshot = authz_head::read_mvcc(mvcc, transaction_id, &principal, tenant_id)?;
    let authz_revision = head_snapshot
        .head
        .committed_revision
        .checked_add(1)
        .ok_or_else(|| anyhow!("authorization revision overflow"))?;
    let created_at = chrono::Utc::now().to_rfc3339();
    for namespace in &mut namespaces {
        let schema_hash = schema_digest(&[namespace.clone()])?;
        namespace.schema_hash = schema_hash;
        namespace.schema_version = next_revision;
        namespace.authz_revision = authz_revision;
        namespace.applied_at = created_at.clone();
    }
    let record = StoredAuthzSchemaRevision {
        schema_ref: StoredSchemaRef {
            schema_id: schema_id.to_string(),
            schema_revision: next_revision,
            schema_digest: canonical_schema_digest,
        },
        namespaces,
        authz_revision,
        written_by: written_by.to_string(),
        reason: reason.to_string(),
        created_at,
    };
    let revision_key = schema_revision_tuple_key(tenant_id, schema_id, next_revision)?;
    let head = authz_head::advance_mvcc(
        &head_snapshot,
        transaction_id,
        AuthzHeadMutation::SchemaRevision,
    )?;
    let payload = record.encode_record(tenant_id, transaction_id)?;
    let mutations = vec![
        schema_row_mutation(revision_key.clone(), payload.clone())?,
        schema_row_mutation(latest_key.clone(), payload.clone())?,
        schema_row_mutation(digest_key.clone(), payload)?,
        authz_head::mvcc_mutation(&head_snapshot, &head, transaction_id)?,
    ];
    let latest_predicate = schema_key_predicate(mvcc, transaction_id, &principal, &latest_key)?;
    mvcc.stage_product_mutations(transaction_id, &principal, mutations, now_unix_ms)?;
    for (tuple_key, predicate) in [
        (revision_key, crate::mvcc_transaction::PredicateKind::Absent),
        (latest_key, latest_predicate),
        (digest_key, crate::mvcc_transaction::PredicateKind::Absent),
    ] {
        mvcc.stage_predicate(
            transaction_id,
            &principal,
            schema_logical_key(&tuple_key)?,
            predicate,
            now_unix_ms,
        )?;
    }
    mvcc.stage_predicate(
        transaction_id,
        &principal,
        head_snapshot.key,
        head_snapshot.predicate,
        now_unix_ms,
    )?;
    if caller_binding.is_none() {
        commit_schema_transaction(mvcc, transaction_id, &principal).await?;
    }
    Ok(record)
}

pub async fn read_schema_revision(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    schema_id: &str,
    revision: Option<u64>,
) -> Result<Option<StoredAuthzSchemaRevision>> {
    validate_schema_id(schema_id)?;
    match revision {
        Some(revision) => {
            read_proto_row_latest_mvcc(
                storage,
                mvcc,
                tenant_id,
                schema_revision_tuple_key(tenant_id, schema_id, revision)?,
            )
            .await
        }
        None => {
            read_proto_row_latest_mvcc(
                storage,
                mvcc,
                tenant_id,
                schema_latest_tuple_key(tenant_id, schema_id)?,
            )
            .await
        }
    }
}

pub async fn bind_schema(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    realm_id: &str,
    schema_ref: StoredSchemaRef,
    expected_generation: Option<u64>,
    written_by: &str,
    reason: &str,
    caller_binding: Option<crate::authz_journal::AuthzTransactionBinding<'_>>,
) -> Result<StoredAuthzSchemaBinding> {
    validate_realm_id(realm_id)?;
    let revision_key =
        schema_revision_tuple_key(tenant_id, &schema_ref.schema_id, schema_ref.schema_revision)?;
    let schema = read_proto_row_latest_mvcc::<StoredAuthzSchemaRevision>(
        storage,
        mvcc,
        tenant_id,
        revision_key,
    )
    .await?
    .ok_or_else(|| anyhow!("authorization schema revision not found"))?;
    validate_stored_schema_revision(&schema)?;
    if schema.schema_ref != schema_ref {
        bail!("authorization schema reference digest mismatch");
    }
    let tuple_key = schema_binding_tuple_key(tenant_id, realm_id)?;
    let current = read_proto_row_latest_mvcc::<StoredAuthzSchemaBinding>(
        storage,
        mvcc,
        tenant_id,
        tuple_key.clone(),
    )
    .await?;
    let actual = current.as_ref().map(|binding| binding.binding_generation);
    match (expected_generation, actual) {
        (None, None) | (Some(0), None) => {}
        (Some(expected), Some(actual)) if expected == actual => {}
        _ => bail!("schema binding generation conflict"),
    }
    let principal = caller_binding
        .map(|binding| binding.principal.to_string())
        .unwrap_or_else(|| authz_head::transaction_principal(tenant_id));
    let idempotency_key = format!(
        "authz-schema-binding:{tenant_id}:{realm_id}:{}:{}",
        schema_ref.schema_id,
        actual.unwrap_or(0).saturating_add(1)
    );
    let now_unix_ms = current_unix_ms();
    let handle = if caller_binding.is_none() {
        Some(
            mvcc.open_transactions
                .begin(
                    mvcc.runtime.as_ref(),
                    mvcc.cluster_id(),
                    &principal,
                    idempotency_key,
                    std::time::Duration::from_secs(30),
                    crate::mvcc_transaction::DurabilityLevel::Quorum,
                    crate::mvcc_transaction::ReadConsistency::Linearized,
                    now_unix_ms,
                )
                .await?,
        )
    } else {
        None
    };
    let transaction_id = caller_binding
        .map(|binding| binding.transaction_id)
        .or_else(|| handle.as_ref().map(|handle| handle.transaction_id.as_str()))
        .expect("schema binding transaction exists");
    let current = read_proto_row_transaction_mvcc::<StoredAuthzSchemaBinding>(
        storage,
        mvcc,
        transaction_id,
        &principal,
        tenant_id,
        tuple_key.clone(),
    )
    .await?;
    let actual_at_snapshot = current.as_ref().map(|binding| binding.binding_generation);
    if actual_at_snapshot != actual {
        bail!("schema binding generation changed before transaction snapshot");
    }
    let head_snapshot = authz_head::read_mvcc(mvcc, transaction_id, &principal, tenant_id)?;
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
    let head = authz_head::advance_mvcc(
        &head_snapshot,
        transaction_id,
        AuthzHeadMutation::SchemaBinding {
            realm_id,
            schema_id: &binding.schema_ref.schema_id,
            schema_revision: binding.schema_ref.schema_revision,
            schema_digest: &binding.schema_ref.schema_digest,
            binding_generation: binding.binding_generation,
        },
    )?;
    let binding_predicate = schema_key_predicate(mvcc, transaction_id, &principal, &tuple_key)?;
    mvcc.stage_product_mutations(
        transaction_id,
        &principal,
        vec![
            schema_row_mutation(
                tuple_key.clone(),
                binding.encode_record(tenant_id, transaction_id)?,
            )?,
            authz_head::mvcc_mutation(&head_snapshot, &head, transaction_id)?,
        ],
        now_unix_ms,
    )?;
    mvcc.stage_predicate(
        transaction_id,
        &principal,
        schema_logical_key(&tuple_key)?,
        binding_predicate,
        now_unix_ms,
    )?;
    mvcc.stage_predicate(
        transaction_id,
        &principal,
        head_snapshot.key,
        head_snapshot.predicate,
        now_unix_ms,
    )?;
    if caller_binding.is_none() {
        commit_schema_transaction(mvcc, transaction_id, &principal).await?;
    }
    Ok(binding)
}

pub async fn read_schema_binding(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    realm_id: &str,
) -> Result<Option<StoredAuthzSchemaBinding>> {
    validate_realm_id(realm_id)?;
    read_proto_row_latest_mvcc(
        storage,
        mvcc,
        tenant_id,
        schema_binding_tuple_key(tenant_id, realm_id)?,
    )
    .await
}

pub async fn read_bound_schema_snapshot_mvcc(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    tenant_id: i64,
    realm_id: &str,
) -> Result<MvccBoundAuthzSchemaSnapshot> {
    validate_realm_id(realm_id)?;
    let binding_key = crate::mvcc_product::coremeta_logical_key(
        CF_AUTHZ,
        TABLE_AUTHZ_SCHEMA_ROW,
        &schema_binding_tuple_key(tenant_id, realm_id)?,
    )?;
    let Some(payload) = mvcc.read_transaction_value(transaction_id, principal, &binding_key)?
    else {
        return Ok(MvccBoundAuthzSchemaSnapshot {
            schema: None,
            binding_key,
            binding_predicate: crate::mvcc_transaction::PredicateKind::Absent,
        });
    };
    let binding =
        decode_schema_record_row::<StoredAuthzSchemaBinding>(storage, tenant_id, &payload).await?;
    let revision_key = crate::mvcc_product::coremeta_logical_key(
        CF_AUTHZ,
        TABLE_AUTHZ_SCHEMA_ROW,
        &schema_revision_tuple_key(
            tenant_id,
            &binding.schema_ref.schema_id,
            binding.schema_ref.schema_revision,
        )?,
    )?;
    let schema_payload = mvcc
        .read_transaction_value(transaction_id, principal, &revision_key)?
        .ok_or_else(|| anyhow!("bound authorization schema revision not found"))?;
    let schema =
        decode_schema_record_row::<StoredAuthzSchemaRevision>(storage, tenant_id, &schema_payload)
            .await?;
    validate_stored_schema_revision(&schema)?;
    if schema.schema_ref != binding.schema_ref {
        return Err(anyhow!("bound authorization schema reference mismatch"));
    }
    Ok(MvccBoundAuthzSchemaSnapshot {
        schema: Some(schema),
        binding_key,
        binding_predicate: crate::mvcc_transaction::PredicateKind::ValueHash(
            *blake3::hash(&payload).as_bytes(),
        ),
    })
}

pub async fn read_bound_namespace_schema_mvcc_at(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    realm_id: &str,
    namespace: &str,
    snapshot_version: u64,
) -> Result<Option<AuthzNamespaceSchema>> {
    validate_realm_id(realm_id)?;
    let binding_key = crate::mvcc_product::coremeta_logical_key(
        CF_AUTHZ,
        TABLE_AUTHZ_SCHEMA_ROW,
        &schema_binding_tuple_key(tenant_id, realm_id)?,
    )?;
    let Some(binding_row) = mvcc.runtime.read_at(&binding_key, snapshot_version)? else {
        return Ok(None);
    };
    let binding = decode_schema_record_row::<StoredAuthzSchemaBinding>(
        storage,
        tenant_id,
        &binding_row.value,
    )
    .await?;
    let revision_key = crate::mvcc_product::coremeta_logical_key(
        CF_AUTHZ,
        TABLE_AUTHZ_SCHEMA_ROW,
        &schema_revision_tuple_key(
            tenant_id,
            &binding.schema_ref.schema_id,
            binding.schema_ref.schema_revision,
        )?,
    )?;
    let schema_row = mvcc
        .runtime
        .read_at(&revision_key, snapshot_version)?
        .ok_or_else(|| anyhow!("bound authorization schema revision not found"))?;
    let schema = decode_schema_record_row::<StoredAuthzSchemaRevision>(
        storage,
        tenant_id,
        &schema_row.value,
    )
    .await?;
    validate_stored_schema_revision(&schema)?;
    if schema.schema_ref != binding.schema_ref {
        bail!("bound authorization schema reference mismatch");
    }
    Ok(schema
        .namespaces
        .into_iter()
        .find(|candidate| candidate.namespace == namespace))
}

pub fn page_schema_revisions(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot_version: u64,
    tenant_id: i64,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> Result<StoredAuthzSchemaRevisionPage> {
    validate_storage_tenant(tenant_id)?;
    let prefix = core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(AUTHZ_SCHEMA_REVISION_ROW_KIND),
        CoreMetaTuplePart::I64(tenant_id),
    ])?;
    let (rows, next_tuple_key) =
        scan_schema_rows_page(mvcc, snapshot_version, &prefix, after_tuple_key, page_size)?;
    let records = rows
        .into_iter()
        .map(|(_, payload)| StoredAuthzSchemaRevision::decode_record(&payload, tenant_id))
        .collect::<Result<Vec<_>>>()?;
    Ok(StoredAuthzSchemaRevisionPage {
        records,
        next_tuple_key,
    })
}

pub fn page_schema_bindings(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot_version: u64,
    tenant_id: i64,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> Result<StoredAuthzSchemaBindingPage> {
    validate_storage_tenant(tenant_id)?;
    let prefix = core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(AUTHZ_SCHEMA_BINDING_ROW_KIND),
        CoreMetaTuplePart::I64(tenant_id),
    ])?;
    let (rows, next_tuple_key) =
        scan_schema_rows_page(mvcc, snapshot_version, &prefix, after_tuple_key, page_size)?;
    let records = rows
        .into_iter()
        .map(|(_, payload)| StoredAuthzSchemaBinding::decode_record(&payload, tenant_id))
        .collect::<Result<Vec<_>>>()?;
    Ok(StoredAuthzSchemaBindingPage {
        records,
        next_tuple_key,
    })
}

fn scan_schema_rows_page(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot_version: u64,
    prefix: &[u8],
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>)> {
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
    let application_prefix = crate::mvcc_product::coremeta_application_prefix(CF_AUTHZ, prefix)?;
    let mut rows = mvcc
        .runtime
        .scan_table_prefix_at(
            TABLE_AUTHZ_SCHEMA_ROW,
            &application_prefix,
            snapshot_version,
        )?
        .into_iter()
        .map(|(key, row)| {
            Ok((
                crate::mvcc_product::coremeta_tuple_from_logical_key(&key, CF_AUTHZ)?.to_vec(),
                row.value,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    rows.retain(|(tuple_key, _)| after_tuple_key.is_none_or(|after| tuple_key.as_slice() > after));
    rows.truncate(page_size.saturating_add(1));
    let has_more = rows.len() > page_size;
    if has_more {
        rows.truncate(page_size);
    }
    let next_tuple_key = if has_more {
        Some(
            rows.last()
                .ok_or_else(|| anyhow!("authorization schema page is empty"))?
                .0
                .clone(),
        )
    } else {
        None
    };
    Ok((rows, next_tuple_key))
}

fn schema_logical_key(tuple_key: &[u8]) -> Result<crate::mvcc_transaction::LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(CF_AUTHZ, TABLE_AUTHZ_SCHEMA_ROW, tuple_key)
}

fn schema_row_mutation(
    tuple_key: Vec<u8>,
    payload: Vec<u8>,
) -> Result<crate::mvcc_product::ProductMutation> {
    Ok(crate::mvcc_product::ProductMutation::put(
        schema_logical_key(&tuple_key)?,
        payload,
    ))
}

fn schema_key_predicate(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    tuple_key: &[u8],
) -> Result<crate::mvcc_transaction::PredicateKind> {
    Ok(
        match mvcc.read_transaction_value(
            transaction_id,
            principal,
            &schema_logical_key(tuple_key)?,
        )? {
            Some(payload) => crate::mvcc_transaction::PredicateKind::ValueHash(
                *blake3::hash(&payload).as_bytes(),
            ),
            None => crate::mvcc_transaction::PredicateKind::Absent,
        },
    )
}

async fn read_proto_row_latest_mvcc<T: AuthzSchemaRecordCodec>(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    tuple_key: Vec<u8>,
) -> Result<Option<T>> {
    let Some(payload) = mvcc.read_latest_value(&schema_logical_key(&tuple_key)?)? else {
        return Ok(None);
    };
    decode_schema_record_row::<T>(storage, tenant_id, &payload)
        .await
        .map(Some)
}

async fn read_proto_row_transaction_mvcc<T: AuthzSchemaRecordCodec>(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    tenant_id: i64,
    tuple_key: Vec<u8>,
) -> Result<Option<T>> {
    let Some(payload) =
        mvcc.read_transaction_value(transaction_id, principal, &schema_logical_key(&tuple_key)?)?
    else {
        return Ok(None);
    };
    decode_schema_record_row::<T>(storage, tenant_id, &payload)
        .await
        .map(Some)
}

async fn commit_schema_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
) -> Result<()> {
    let outcome = mvcc
        .open_transactions
        .commit(
            mvcc.runtime.as_ref(),
            transaction_id,
            principal,
            current_unix_ms(),
        )
        .await?;
    match outcome.certification {
        crate::mvcc_transaction::CertificationResult::Committed { .. } => Ok(()),
        crate::mvcc_transaction::CertificationResult::Aborted { reason } => {
            bail!("authorization schema transaction aborted: {reason:?}")
        }
    }
}

fn current_unix_ms() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default()
}

trait AuthzSchemaRecordCodec: Sized {
    fn encode_record(&self, tenant_id: i64, transaction_id: &str) -> Result<Vec<u8>>;
    fn decode_record(bytes: &[u8], tenant_id: i64) -> Result<Self>;
}

impl AuthzSchemaRecordCodec for StoredAuthzSchemaRevision {
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
    _storage: &Storage,
    tenant_id: i64,
    row_payload: &[u8],
) -> Result<T> {
    T::decode_record(row_payload, tenant_id)
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
    _tenant_id: i64,
    _transaction_id: &str,
) -> Result<StoredAuthzSchemaRevisionProto> {
    Ok(StoredAuthzSchemaRevisionProto {
        schema_ref: Some(schema_ref_to_proto(&record.schema_ref)),
        namespaces: record.namespaces.iter().map(namespace_to_proto).collect(),
        authz_revision: record.authz_revision,
        written_by: record.written_by.clone(),
        reason: record.reason.clone(),
        created_at: record.created_at.clone(),
    })
}

fn schema_revision_from_proto(
    proto: StoredAuthzSchemaRevisionProto,
    _tenant_id: i64,
) -> Result<StoredAuthzSchemaRevision> {
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
    Ok(record)
}

fn schema_binding_to_proto(
    record: &StoredAuthzSchemaBinding,
    _tenant_id: i64,
    _transaction_id: &str,
) -> Result<StoredAuthzSchemaBindingProto> {
    Ok(StoredAuthzSchemaBindingProto {
        realm_id: record.realm_id.clone(),
        schema_ref: Some(schema_ref_to_proto(&record.schema_ref)),
        binding_generation: record.binding_generation,
        authz_revision: record.authz_revision,
        written_by: record.written_by.clone(),
        reason: record.reason.clone(),
        updated_at: record.updated_at.clone(),
    })
}

fn schema_binding_from_proto(
    proto: StoredAuthzSchemaBindingProto,
    _tenant_id: i64,
) -> Result<StoredAuthzSchemaBinding> {
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
    for namespace in &mut namespaces {
        // Publication metadata is assigned by the authoritative MVCC schema
        // revision. It must not change the identity of otherwise identical
        // schema input or retries would allocate a fresh revision.
        namespace.schema_hash.clear();
        namespace.schema_version = 0;
        namespace.authz_revision = 0;
        namespace.applied_at.clear();
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_identity_excludes_mvcc_publication_metadata() {
        let input = AuthzNamespaceSchema {
            namespace: "document".to_string(),
            relations: Vec::new(),
            schema_json: "{}".to_string(),
            schema_hash: String::new(),
            schema_version: 0,
            authz_revision: 0,
            applied_at: String::new(),
        };
        let mut published = input.clone();
        published.schema_hash = "derived-hash".to_string();
        published.schema_version = 9;
        published.authz_revision = 14;
        published.applied_at = "2026-07-27T00:00:00Z".to_string();
        assert_eq!(
            schema_digest(&[input]).unwrap(),
            schema_digest(&[published]).unwrap()
        );
    }
}
