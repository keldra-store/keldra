use crate::authz_head::{self, AuthzHeadMutation, AuthzHeadSnapshot};
use crate::authz_realm_schema;
use crate::authz_schema_contract::{
    AuthzSchemaContractError, AuthzTupleShape, validate_tuple_batch,
};
use crate::authz_scope::{DEFAULT_AUTHZ_REALM_ID, split_realm_namespace};
use crate::authz_segment;
use crate::core_store::{
    CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition,
    CoreMutationRootPublication, CoreStore, ReadStream,
};
use crate::formats::{Hash32, hash32, writer::WriterFamily};
use crate::partition_fence::{PartitionWritePermit, partition_write_precondition};
use crate::persistence::AuthzTupleRecord;
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow, bail};
use prost::Message;
use std::collections::BTreeSet;

mod idempotency;
mod materialization;
mod projection;
pub(crate) mod resolver;
mod storage_resolver;

pub(crate) use projection::{AuthzProjectionPageError, AuthzTupleProjectionPage};
#[cfg(test)]
pub(crate) use storage_resolver::AuthzResolutionOutcome;

const AUTHZ_TUPLE_JOURNAL_BODY_SCHEMA: &str = "anvil.authz_tuple.journal_body.v1";
const AUTHZ_TUPLE_BATCH_JOURNAL_BODY_SCHEMA: &str = "anvil.authz_tuple.batch_journal_body.v1";
const AUTHZ_TUPLE_RECORD_KIND: &str = "authz_tuple";
const AUTHZ_TUPLE_BATCH_RECORD_KIND: &str = "authz_tuple_batch";

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum AuthzTupleOperationProto {
    Unspecified = 0,
    Add = 1,
    Remove = 2,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzTupleRecordProto {
    #[prost(int64, tag = "1")]
    revision: i64,
    #[prost(uint32, tag = "2")]
    revision_ordinal: u32,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(string, tag = "4")]
    namespace: String,
    #[prost(string, tag = "5")]
    object_id: String,
    #[prost(string, tag = "6")]
    relation: String,
    #[prost(string, tag = "7")]
    subject_kind: String,
    #[prost(string, tag = "8")]
    subject_id: String,
    #[prost(string, tag = "9")]
    caveat_hash: String,
    #[prost(enumeration = "AuthzTupleOperationProto", tag = "10")]
    operation: i32,
    #[prost(string, tag = "11")]
    written_by: String,
    #[prost(string, tag = "12")]
    reason: String,
    #[prost(string, tag = "13")]
    mutation_id: String,
    #[prost(string, tag = "14")]
    record_hash: String,
    #[prost(int64, tag = "15")]
    written_at_unix_nanos: i64,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzTupleJournalBodyProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(message, optional, tag = "2")]
    record: Option<AuthzTupleRecordProto>,
    #[prost(uint64, tag = "3")]
    fence_token: u64,
    #[prost(string, tag = "4")]
    mutation_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzTupleBatchJournalBodyProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(int64, tag = "2")]
    revision: i64,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(message, repeated, tag = "4")]
    records: Vec<AuthzTupleRecordProto>,
    #[prost(uint64, tag = "5")]
    fence_token: u64,
    #[prost(string, tag = "6")]
    mutation_id: String,
}

#[derive(Clone, Copy)]
pub struct AuthzTupleWrite<'a> {
    pub tenant_id: i64,
    pub namespace: &'a str,
    pub object_id: &'a str,
    pub relation: &'a str,
    pub subject_kind: &'a str,
    pub subject_id: &'a str,
    pub caveat_hash: &'a str,
    pub operation: &'a str,
    pub written_by: &'a str,
    pub reason: &'a str,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuthzTupleFilter {
    pub realm_id: Option<String>,
    pub namespace: Option<String>,
    pub object_id: Option<String>,
    pub relation: Option<String>,
    pub subject_kind: Option<String>,
    pub subject_id: Option<String>,
    pub caveat_hash: Option<String>,
}

pub(crate) async fn page_current_authz_tuples(
    storage: &Storage,
    tenant_id: i64,
    filter: &AuthzTupleFilter,
    expected_revision: i64,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> std::result::Result<AuthzTupleProjectionPage, AuthzProjectionPageError> {
    projection::page_current_records(
        storage,
        tenant_id,
        filter,
        expected_revision,
        after_tuple_key,
        page_size,
    )
    .await
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AuthzSubjectRef {
    pub subject_kind: String,
    pub subject_id: String,
    pub caveat_hash: String,
}

#[derive(Debug, Clone)]
pub(crate) struct AuthzObjectListPage {
    pub object_ids: Vec<String>,
    pub next_object_id: Option<String>,
    pub tuple_rows_visited: usize,
    pub resolution_rows_visited: usize,
    pub graph_nodes_visited: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct AuthzSubjectListPage {
    pub subjects: Vec<AuthzSubjectRef>,
    pub next_subject_position: Option<String>,
    pub tuple_rows_visited: usize,
    pub graph_nodes_visited: usize,
}

pub(crate) async fn write_authz_tuple_with_permit(
    storage: &Storage,
    input: AuthzTupleWrite<'_>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<AuthzTupleRecord> {
    require_authz_permit(input.tenant_id, permit)?;
    validate_optional_caveat_hash(input.caveat_hash)?;
    let write_lock = authz_head::tenant_write_lock(input.tenant_id)?;
    let _guard = write_lock.lock().await;
    let schema_binding_precondition =
        validate_writes_against_bound_schema(storage, std::slice::from_ref(&input), None).await?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    write_authz_tuple_inner(
        storage,
        input,
        permit.fence_token,
        Some(partition_precondition),
        schema_binding_precondition,
    )
    .await
}

pub(crate) async fn write_authz_tuple_batch_with_permit(
    storage: &Storage,
    inputs: Vec<AuthzTupleWrite<'_>>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<Vec<AuthzTupleRecord>> {
    let Some(first) = inputs.first() else {
        return Err(anyhow!("authz tuple batch must not be empty"));
    };
    let tenant_id = first.tenant_id;
    require_authz_permit(tenant_id, permit)?;
    for input in &inputs {
        if input.tenant_id != tenant_id {
            return Err(anyhow!("authz tuple batch must target one tenant"));
        }
        validate_optional_caveat_hash(input.caveat_hash)?;
    }
    let write_lock = authz_head::tenant_write_lock(tenant_id)?;
    let _guard = write_lock.lock().await;
    let schema_binding_precondition =
        validate_writes_against_bound_schema(storage, &inputs, None).await?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    write_authz_tuple_batch_inner(
        storage,
        inputs,
        permit.fence_token,
        Some(partition_precondition),
        schema_binding_precondition,
    )
    .await
}

pub(crate) async fn replay_authz_tuple_batch(
    storage: &Storage,
    inputs: &[AuthzTupleWrite<'_>],
    options: &crate::persistence::AuthzTupleBatchWriteOptions,
) -> Result<Option<crate::persistence::AuthzTupleBatchWriteOutcome>> {
    idempotency::replay(storage, inputs, options).await
}

pub(crate) async fn write_authz_tuple_batch_conditionally_with_permit(
    storage: &Storage,
    inputs: Vec<AuthzTupleWrite<'_>>,
    options: &crate::persistence::AuthzTupleBatchWriteOptions,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<crate::persistence::AuthzTupleBatchWriteOutcome> {
    let Some(first) = inputs.first() else {
        return Err(anyhow!("authz tuple batch must not be empty"));
    };
    let tenant_id = first.tenant_id;
    require_authz_permit(tenant_id, permit)?;
    for input in &inputs {
        if input.tenant_id != tenant_id {
            return Err(anyhow!("authz tuple batch must target one tenant"));
        }
        validate_optional_caveat_hash(input.caveat_hash)?;
    }
    if let Some(operation_id) = options.operation_id.as_deref() {
        idempotency::validate_operation_id(operation_id)?;
    }
    let write_lock = authz_head::tenant_write_lock(tenant_id)?;
    let _guard = write_lock.lock().await;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    write_authz_tuple_batch_conditionally_inner(
        storage,
        inputs,
        options,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

pub(crate) fn validate_authz_batch_operation_id(operation_id: &str) -> Result<()> {
    idempotency::validate_operation_id(operation_id)
}

async fn validate_writes_against_bound_schema(
    storage: &Storage,
    inputs: &[AuthzTupleWrite<'_>],
    expected_realm_id: Option<&str>,
) -> Result<crate::persistence::AuthzSchemaBindingPrecondition> {
    let first = inputs
        .first()
        .ok_or_else(|| anyhow!("authz tuple batch must not be empty"))?;
    let realm_id = split_realm_namespace(first.namespace)
        .map(|(realm_id, _)| realm_id)
        .unwrap_or_else(|| DEFAULT_AUTHZ_REALM_ID.to_string());
    if expected_realm_id.is_some_and(|expected| expected != realm_id) {
        return Err(AuthzSchemaContractError::new(
            "authorization tuple scope does not match the conditional batch realm",
        )
        .into());
    }
    let snapshot =
        authz_realm_schema::read_bound_schema_snapshot(storage, first.tenant_id, &realm_id).await?;
    let schema = snapshot.schema.as_ref().ok_or_else(|| {
        AuthzSchemaContractError::new(format!(
            "authorization realm {realm_id} has no bound schema revision"
        ))
    })?;
    let tuples = inputs
        .iter()
        .map(|input| AuthzTupleShape {
            namespace: input.namespace,
            object_id: input.object_id,
            relation: input.relation,
            subject_kind: input.subject_kind,
            subject_id: input.subject_id,
            operation: input.operation,
        })
        .collect::<Vec<_>>();
    validate_tuple_batch(&schema.namespaces, &realm_id, &tuples)?;
    Ok(snapshot.binding_precondition)
}

fn handle_schema_fenced_write_result(
    storage: &Storage,
    schema_binding_precondition: &crate::persistence::AuthzSchemaBindingPrecondition,
    result: Result<()>,
) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(_)
            if !idempotency::schema_binding_is_current(storage, schema_binding_precondition)? =>
        {
            Err(anyhow!(
                crate::persistence::AuthzTupleBatchWriteError::SchemaBindingChanged
            ))
        }
        Err(error) => Err(error),
    }
}

async fn write_authz_tuple_inner(
    storage: &Storage,
    input: AuthzTupleWrite<'_>,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    schema_binding_precondition: crate::persistence::AuthzSchemaBindingPrecondition,
) -> Result<AuthzTupleRecord> {
    validate_optional_caveat_hash(input.caveat_hash)?;
    let head_snapshot = authz_head::read(storage, input.tenant_id).await?;
    let revision = i64::try_from(head_snapshot.head.committed_revision)?
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("authz revision overflow"))?;
    let record = build_authz_tuple_record(input, revision, 0)?;
    let write_result = append_authz_tuple_record_inner(
        storage,
        &record,
        fence_token,
        partition_precondition,
        Some(&schema_binding_precondition),
        &head_snapshot,
    )
    .await;
    handle_schema_fenced_write_result(storage, &schema_binding_precondition, write_result)?;
    Ok(record)
}

async fn write_authz_tuple_batch_inner(
    storage: &Storage,
    inputs: Vec<AuthzTupleWrite<'_>>,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    schema_binding_precondition: crate::persistence::AuthzSchemaBindingPrecondition,
) -> Result<Vec<AuthzTupleRecord>> {
    let tenant_id = inputs
        .first()
        .ok_or_else(|| anyhow!("authz tuple batch must not be empty"))?
        .tenant_id;
    let head_snapshot = authz_head::read(storage, tenant_id).await?;
    let revision = i64::try_from(head_snapshot.head.committed_revision)?
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("authz revision overflow"))?;
    let mut records = Vec::with_capacity(inputs.len());
    for (idx, input) in inputs.into_iter().enumerate() {
        records.push(build_authz_tuple_record(
            input,
            revision,
            u32::try_from(idx).context("authz tuple batch ordinal overflow")?,
        )?);
    }
    let write_result = append_authz_tuple_batch_inner(
        storage,
        tenant_id,
        &records,
        fence_token,
        partition_precondition,
        None,
        Some(&schema_binding_precondition),
        &head_snapshot,
    )
    .await;
    handle_schema_fenced_write_result(storage, &schema_binding_precondition, write_result)?;
    Ok(records)
}

async fn write_authz_tuple_batch_conditionally_inner(
    storage: &Storage,
    inputs: Vec<AuthzTupleWrite<'_>>,
    options: &crate::persistence::AuthzTupleBatchWriteOptions,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<crate::persistence::AuthzTupleBatchWriteOutcome> {
    if let Some(replay) = idempotency::replay(storage, &inputs, options).await? {
        return Ok(replay);
    }
    let schema_binding_precondition = validate_writes_against_bound_schema(
        storage,
        &inputs,
        Some(options.authz_realm_id.as_str()),
    )
    .await?;
    let tenant_id = inputs
        .first()
        .ok_or_else(|| anyhow!("authz tuple batch must not be empty"))?
        .tenant_id;
    let head_snapshot = authz_head::read(storage, tenant_id).await?;
    let current_revision = i64::try_from(head_snapshot.head.committed_revision)
        .context("authorization revision exceeds i64")?;
    if let Some(expected) = options.expected_revision
        && expected != current_revision
    {
        return Err(anyhow!(
            crate::persistence::AuthzTupleBatchWriteError::RevisionConflict {
                expected,
                actual: current_revision,
            }
        ));
    }
    let revision = current_revision
        .checked_add(1)
        .ok_or_else(|| anyhow!("authz revision overflow"))?;
    let mut records = Vec::with_capacity(inputs.len());
    for (idx, input) in inputs.iter().copied().enumerate() {
        records.push(build_authz_tuple_record(
            input,
            revision,
            u32::try_from(idx).context("authz tuple batch ordinal overflow")?,
        )?);
    }
    let receipt = idempotency::prepare_receipt(&inputs, options, &records)?;
    let write_result = append_authz_tuple_batch_inner(
        storage,
        tenant_id,
        &records,
        fence_token,
        partition_precondition,
        receipt.as_ref(),
        Some(&schema_binding_precondition),
        &head_snapshot,
    )
    .await;
    if let Err(error) = write_result {
        if let Some(replay) = idempotency::replay(storage, &inputs, options).await? {
            return Ok(replay);
        }
        if !idempotency::schema_binding_is_current(storage, &schema_binding_precondition)? {
            return Err(anyhow!(
                crate::persistence::AuthzTupleBatchWriteError::SchemaBindingChanged
            ));
        }
        return Err(error);
    }
    Ok(crate::persistence::AuthzTupleBatchWriteOutcome {
        records,
        replayed: false,
    })
}

fn build_authz_tuple_record(
    input: AuthzTupleWrite<'_>,
    revision: i64,
    revision_ordinal: u32,
) -> Result<AuthzTupleRecord> {
    let written_at = chrono::Utc::now();
    let mutation_id = uuid::Uuid::new_v4();
    let record_hash = authz_record_hash(AuthzRecordHashInput {
        revision,
        revision_ordinal,
        tenant_id: input.tenant_id,
        namespace: input.namespace,
        object_id: input.object_id,
        relation: input.relation,
        subject_kind: input.subject_kind,
        subject_id: input.subject_id,
        caveat_hash: input.caveat_hash,
        operation: input.operation,
        written_by: input.written_by,
        reason: input.reason,
    });
    Ok(AuthzTupleRecord {
        revision,
        revision_ordinal,
        tenant_id: input.tenant_id,
        namespace: input.namespace.to_string(),
        object_id: input.object_id.to_string(),
        relation: input.relation.to_string(),
        subject_kind: input.subject_kind.to_string(),
        subject_id: input.subject_id.to_string(),
        caveat_hash: input.caveat_hash.to_string(),
        operation: input.operation.to_string(),
        written_by: input.written_by.to_string(),
        reason: input.reason.to_string(),
        mutation_id,
        record_hash,
        written_at,
    })
}

#[cfg(test)]
pub(crate) async fn test_append_authz_tuple_record_unfenced(
    storage: &Storage,
    record: &AuthzTupleRecord,
) -> Result<()> {
    let head_snapshot = authz_head::read(storage, record.tenant_id).await?;
    append_authz_tuple_record_inner(storage, record, 0, None, None, &head_snapshot).await
}

#[cfg(test)]
pub(crate) async fn append_authz_tuple_record_with_permit(
    storage: &Storage,
    record: &AuthzTupleRecord,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    require_authz_permit(record.tenant_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    let head_snapshot = authz_head::read(storage, record.tenant_id).await?;
    append_authz_tuple_record_inner(
        storage,
        record,
        permit.fence_token,
        Some(partition_precondition),
        None,
        &head_snapshot,
    )
    .await
}

async fn append_authz_tuple_record_inner(
    storage: &Storage,
    record: &AuthzTupleRecord,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    schema_binding_precondition: Option<&crate::persistence::AuthzSchemaBindingPrecondition>,
    head_snapshot: &AuthzHeadSnapshot,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = authz_tuple_stream_id(record.tenant_id);
    let payload = encode_authz_tuple_journal_body(record, fence_token)?;
    let transaction_id = format!("authz-tuple:{}", record.mutation_id);
    let partition_id = authz_head::transaction_partition(record.tenant_id);
    let head = authz_head::advance(
        head_snapshot,
        &transaction_id,
        AuthzHeadMutation::TupleBatch {
            journal_payload: &payload,
            fence_token,
        },
    )?;
    if i64::try_from(head.committed_revision)? != record.revision {
        return Err(anyhow!(
            "authorization tuple revision does not advance the authorization head"
        ));
    }
    let mut preconditions = partition_precondition.into_iter().collect::<Vec<_>>();
    if let Some(schema_binding_precondition) = schema_binding_precondition {
        preconditions.push(idempotency::schema_binding_precondition(
            schema_binding_precondition,
        ));
    }
    preconditions.push(authz_head::precondition(head_snapshot)?);
    let mut operations = vec![CoreMutationOperation::StreamAppend {
        partition_id: partition_id.clone(),
        stream_id,
        record_kind: AUTHZ_TUPLE_RECORD_KIND.to_string(),
        payload,
        idempotency_key: Some(transaction_id.clone()),
    }];
    operations.extend(
        projection::current_operations(
            storage,
            std::slice::from_ref(record),
            &partition_id,
            &transaction_id,
        )
        .await?,
    );
    operations.push(authz_head::put_operation(
        &partition_id,
        &transaction_id,
        &head,
    )?);
    let step_started_at = std::time::Instant::now();
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id,
            scope_partition: partition_id.clone(),
            committed_by_principal: authz_head::transaction_principal(record.tenant_id),
            root_publications: authz_mutation_root_publications(&partition_id, record.tenant_id),
            preconditions,
            operations,
        })
        .await?;
    crate::emit_test_timing(
        "authz_journal.append_record commit_mutation_batch",
        step_started_at.elapsed(),
    );
    record_authz_materialization_deferred(
        record.tenant_id,
        record.revision,
        std::slice::from_ref(record).len(),
    );
    Ok(())
}

async fn append_authz_tuple_batch_inner(
    storage: &Storage,
    tenant_id: i64,
    records: &[AuthzTupleRecord],
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    idempotency_receipt: Option<&idempotency::PreparedAuthzIdempotencyReceipt>,
    schema_binding_precondition: Option<&crate::persistence::AuthzSchemaBindingPrecondition>,
    head_snapshot: &AuthzHeadSnapshot,
) -> Result<()> {
    if records.is_empty() {
        return Err(anyhow!("authz tuple batch must not be empty"));
    }
    let revision = records[0].revision;
    if records
        .iter()
        .any(|record| record.tenant_id != tenant_id || record.revision != revision)
    {
        return Err(anyhow!(
            "authz tuple batch records must target one tenant and revision"
        ));
    }
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = authz_tuple_stream_id(tenant_id);
    let payload = encode_authz_tuple_batch_journal_body(tenant_id, revision, records, fence_token)?;

    let partition_id = authz_head::transaction_partition(tenant_id);
    let step_started_at = std::time::Instant::now();
    let transaction_id = idempotency_receipt
        .map(|receipt| receipt.transaction_id.clone())
        .unwrap_or_else(|| format!("authz-tuple-batch:{tenant_id}:{revision}"));
    let head = authz_head::advance(
        head_snapshot,
        &transaction_id,
        AuthzHeadMutation::TupleBatch {
            journal_payload: &payload,
            fence_token,
        },
    )?;
    if i64::try_from(head.committed_revision)? != revision {
        return Err(anyhow!(
            "authorization tuple batch revision does not advance the authorization head"
        ));
    }
    let mut preconditions = partition_precondition.into_iter().collect::<Vec<_>>();
    if let Some(schema_binding_precondition) = schema_binding_precondition {
        preconditions.push(idempotency::schema_binding_precondition(
            schema_binding_precondition,
        ));
    }
    if let Some(receipt) = idempotency_receipt {
        preconditions.push(idempotency::receipt_precondition(receipt));
    }
    preconditions.push(authz_head::precondition(head_snapshot)?);
    let mut operations = vec![CoreMutationOperation::StreamAppend {
        partition_id: partition_id.clone(),
        stream_id,
        record_kind: AUTHZ_TUPLE_BATCH_RECORD_KIND.to_string(),
        payload,
        idempotency_key: Some(transaction_id.clone()),
    }];
    if let Some(receipt) = idempotency_receipt {
        operations.push(idempotency::receipt_operation(&partition_id, receipt));
    }
    operations.extend(
        projection::current_operations(storage, records, &partition_id, &transaction_id).await?,
    );
    operations.push(authz_head::put_operation(
        &partition_id,
        &transaction_id,
        &head,
    )?);
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id,
            scope_partition: partition_id.clone(),
            committed_by_principal: authz_head::transaction_principal(tenant_id),
            root_publications: authz_mutation_root_publications(&partition_id, tenant_id),
            preconditions,
            operations,
        })
        .await?;
    crate::emit_test_timing(
        "authz_journal.append_batch commit_mutation_batch",
        step_started_at.elapsed(),
    );
    record_authz_materialization_deferred(tenant_id, revision, records.len());
    Ok(())
}

fn authz_mutation_root_publications(
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

fn record_authz_materialization_deferred(tenant_id: i64, revision: i64, record_count: usize) {
    let tenant_id = tenant_id.to_string();
    let revision = revision.to_string();
    crate::perf::record_counter(
        "authz_materialization_deferred",
        &[("tenant_id", &tenant_id), ("revision", &revision)],
        record_count as u64,
    );
    crate::emit_test_timing(
        "authz_journal.materialize_segment deferred",
        std::time::Duration::ZERO,
    );
}

pub(crate) use materialization::{
    AuthzMaterializationOutcome, materialize_authz_derived_state_at_revision,
    materialize_authz_derived_state_through_revision, materialize_authz_tuple_segment,
    materialize_authz_tuple_segment_at_revision, rebuild_authz_materialization_at_revision,
};

pub async fn latest_authz_revision(storage: &Storage, tenant_id: i64) -> Result<i64> {
    i64::try_from(
        authz_head::read(storage, tenant_id)
            .await?
            .head
            .committed_revision,
    )
    .context("authorization revision exceeds i64")
}

pub(crate) async fn latest_authz_tuple_revision(storage: &Storage, tenant_id: i64) -> Result<i64> {
    i64::try_from(
        authz_head::read(storage, tenant_id)
            .await?
            .head
            .tuple_revision,
    )
    .context("authorization tuple revision exceeds i64")
}

#[allow(clippy::too_many_arguments)]
pub async fn check_authz_tuple(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
) -> Result<Option<AuthzTupleRecord>> {
    check_authz_tuple_at_revision(
        storage,
        tenant_id,
        namespace,
        object_id,
        relation,
        subject_kind,
        subject_id,
        caveat_hash,
        i64::MAX,
    )
    .await
}

pub fn validate_optional_caveat_hash(value: &str) -> Result<()> {
    if value.is_empty() {
        return Ok(());
    }
    if value.len() == 64 && value.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        Ok(())
    } else {
        Err(anyhow!("caveat_hash must be empty or hex32"))
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn check_authz_tuple_at_revision(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
    revision: i64,
) -> Result<Option<AuthzTupleRecord>> {
    if revision < 0 {
        return Err(anyhow!("authorization revision must be non-negative"));
    }
    let current_revision = latest_authz_revision(storage, tenant_id).await?;
    if revision != i64::MAX && revision > current_revision {
        return Err(anyhow!(
            "AuthzRevisionUnavailable: current authorization revision is {current_revision}, requested {revision}"
        ));
    }
    if revision == i64::MAX || revision >= latest_authz_tuple_revision(storage, tenant_id).await? {
        return projection::read_current_record(
            storage,
            tenant_id,
            namespace,
            object_id,
            relation,
            subject_kind,
            subject_id,
            caveat_hash,
        )
        .await;
    }
    Ok(authz_segment::lookup_materialized_tuple_at_revision(
        storage,
        tenant_id,
        namespace,
        object_id,
        relation,
        subject_kind,
        subject_id,
        caveat_hash,
        u64::try_from(revision)?,
    )
    .await?
    .record)
}

/// Resolve an exact current tuple through an already-open CoreStore. Internal
/// node authentication uses this point lookup because its grant is a direct
/// system-realm edge; invoking the general Zanzibar graph resolver for every
/// replication frame would add no authorization semantics and can recursively
/// contend with the recovery traffic it is authorizing.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn check_current_authz_tuple_with_core_store(
    storage: &Storage,
    core_store: &CoreStore,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
) -> Result<Option<AuthzTupleRecord>> {
    projection::read_current_record_with_core_store(
        storage,
        core_store,
        tenant_id,
        namespace,
        object_id,
        relation,
        subject_kind,
        subject_id,
        caveat_hash,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn resolve_permission_at_revision(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
    revision: i64,
) -> Result<bool> {
    if revision == i64::MAX {
        return resolve_current_permission(
            storage,
            tenant_id,
            namespace,
            object_id,
            relation,
            subject_kind,
            subject_id,
            caveat_hash,
        )
        .await;
    }
    if revision < 0 {
        return Err(anyhow!("authorization revision must be non-negative"));
    }

    let current_revision = latest_authz_revision(storage, tenant_id).await?;
    if revision > current_revision {
        return Err(anyhow!(
            "AuthzRevisionUnavailable: current authorization revision is {current_revision}, requested {revision}"
        ));
    }
    if revision == current_revision {
        match resolve_current_permission_at_expected_revision(
            storage,
            tenant_id,
            namespace,
            object_id,
            relation,
            subject_kind,
            subject_id,
            caveat_hash,
            revision,
        )
        .await
        {
            Ok(outcome) => return Ok(outcome.allowed),
            Err(_error) if latest_authz_revision(storage, tenant_id).await? != revision => {}
            Err(error) => return Err(error),
        }
    }

    Ok(authz_segment::resolve_materialized_permission_at_revision(
        storage,
        tenant_id,
        namespace,
        object_id,
        relation,
        subject_kind,
        subject_id,
        caveat_hash,
        u64::try_from(revision)?,
    )
    .await?
    .allowed)
}

#[allow(clippy::too_many_arguments)]
async fn resolve_current_permission_at_expected_revision(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
    expected_revision: i64,
) -> Result<storage_resolver::AuthzResolutionOutcome> {
    storage_resolver::resolve_at_current_revision(
        storage,
        tenant_id,
        resolver::UsersetRef {
            namespace: namespace.to_string(),
            object_id: object_id.to_string(),
            relation: relation.to_string(),
        },
        resolver::SubjectRef {
            kind: subject_kind.to_string(),
            id: subject_id.to_string(),
            caveat_hash: caveat_hash.to_string(),
        },
        expected_revision,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn resolve_current_permission(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
) -> Result<bool> {
    for _ in 0..3 {
        let revision = latest_authz_revision(storage, tenant_id).await?;
        match resolve_current_permission_at_expected_revision(
            storage,
            tenant_id,
            namespace,
            object_id,
            relation,
            subject_kind,
            subject_id,
            caveat_hash,
            revision,
        )
        .await
        {
            Ok(outcome) => return Ok(outcome.allowed),
            Err(_error) if latest_authz_revision(storage, tenant_id).await? != revision => continue,
            Err(error) => return Err(error),
        }
    }
    Err(anyhow!(
        "AuthzRevisionUnavailable: authorization revision changed during three resolution attempts"
    ))
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn resolve_current_permission_with_stats(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
) -> Result<AuthzResolutionOutcome> {
    let revision = latest_authz_revision(storage, tenant_id).await?;
    resolve_current_permission_at_expected_revision(
        storage,
        tenant_id,
        namespace,
        object_id,
        relation,
        subject_kind,
        subject_id,
        caveat_hash,
        revision,
    )
    .await
}

pub async fn list_authz_tuple_log(
    storage: &Storage,
    tenant_id: i64,
    after_revision: i64,
    namespace: &str,
    limit: usize,
) -> Result<Vec<AuthzTupleRecord>> {
    Ok(
        list_authz_tuple_log_page(storage, tenant_id, after_revision, namespace, limit)
            .await?
            .records,
    )
}

pub(crate) async fn collect_authz_tuple_log_for_rebuild(
    storage: &Storage,
    tenant_id: i64,
    through_revision: Option<i64>,
) -> Result<Vec<AuthzTupleRecord>> {
    if through_revision.is_some_and(|revision| revision < 0) {
        bail!("authorization rebuild revision must be non-negative");
    }
    materialization::collect_authz_tuple_records_for_rebuild(
        storage,
        tenant_id,
        through_revision.map(u64::try_from).transpose()?,
    )
    .await
}

#[derive(Debug, Clone)]
pub struct AuthzTupleLogPage {
    pub records: Vec<AuthzTupleRecord>,
    pub next_revision: i64,
    pub has_more: bool,
}

pub async fn list_authz_tuple_log_page(
    storage: &Storage,
    tenant_id: i64,
    after_revision: i64,
    namespace: &str,
    limit: usize,
) -> Result<AuthzTupleLogPage> {
    if after_revision < 0 {
        return Err(anyhow!("authorization watch revision must be non-negative"));
    }
    let core_store = CoreStore::new(storage.clone()).await?;
    let page = core_store
        .read_stream_page(ReadStream {
            stream_id: authz_tuple_stream_id(tenant_id),
            after_sequence: u64::try_from(after_revision)?,
            limit,
        })
        .await?;
    let next_revision = i64::try_from(page.next_sequence)
        .map_err(|_| anyhow!("authorization watch revision exceeds i64"))?;
    let mut records = Vec::new();
    for stream_record in page.records {
        let mut decoded = match stream_record.record_kind.as_str() {
            AUTHZ_TUPLE_RECORD_KIND => {
                vec![decode_authz_tuple_journal_body(&stream_record.payload)?]
            }
            AUTHZ_TUPLE_BATCH_RECORD_KIND => {
                decode_authz_tuple_batch_journal_body(&stream_record.payload)?
            }
            _ => return Err(anyhow!("authorization tuple stream record kind mismatch")),
        };
        decoded.retain(|record| namespace.is_empty() || record.namespace == namespace);
        records.extend(decoded);
    }
    records.sort_by_key(|record| (record.revision, record.revision_ordinal));
    Ok(AuthzTupleLogPage {
        records,
        next_revision,
        has_more: page.has_more,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn list_current_authz_objects_page(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
    revision: i64,
    after_object_id: Option<&str>,
    page_size: usize,
) -> Result<AuthzObjectListPage> {
    const SOURCE_CHUNK_ROWS: usize = 256;
    const MAX_PAGE_CANDIDATES: usize = 16_384;
    const MAX_LIST_RESOLUTION_ROWS: usize = 16_384;
    const MAX_LIST_GRAPH_NODES: usize = 4_096;

    if !(1..=1000).contains(&page_size) {
        return Err(anyhow!("authz object page size must be between 1 and 1000"));
    }

    let subject = resolver::SubjectRef {
        kind: subject_kind.to_string(),
        id: subject_id.to_string(),
        caveat_hash: caveat_hash.to_string(),
    };
    let candidate_budget = page_size
        .saturating_mul(16)
        .saturating_add(1)
        .clamp(page_size.saturating_add(1), MAX_PAGE_CANDIDATES);
    let mut object_ids = Vec::with_capacity(page_size);
    let mut scan_after = after_object_id.map(str::to_string);
    let mut last_processed = None;
    let mut tuple_rows_visited = 0_usize;
    let mut resolution_rows_visited = 0_usize;
    let mut graph_nodes_visited = 0_usize;
    let mut source_has_more = false;

    'source: while tuple_rows_visited < candidate_budget && object_ids.len() < page_size {
        let source_limit = (candidate_budget - tuple_rows_visited).min(SOURCE_CHUNK_ROWS);
        let candidates = projection::page_current_object_candidates(
            storage,
            tenant_id,
            namespace,
            revision,
            scan_after.as_deref(),
            source_limit,
        )
        .await
        .map_err(anyhow::Error::new)?;
        tuple_rows_visited = tuple_rows_visited
            .checked_add(candidates.candidates_visited)
            .ok_or_else(|| anyhow!("authorization list tuple row count overflow"))?;
        let candidate_count = candidates.object_ids.len();
        for (candidate_index, object_id) in candidates.object_ids.into_iter().enumerate() {
            let outcome = storage_resolver::resolve_at_current_revision(
                storage,
                tenant_id,
                resolver::UsersetRef {
                    namespace: namespace.to_string(),
                    object_id: object_id.clone(),
                    relation: relation.to_string(),
                },
                subject.clone(),
                revision,
            )
            .await?;
            let next_resolution_rows = resolution_rows_visited
                .checked_add(outcome.stats.projection_rows_visited)
                .ok_or_else(|| anyhow!("authorization list resolution row count overflow"))?;
            let next_graph_nodes = graph_nodes_visited
                .checked_add(outcome.stats.graph_nodes_visited)
                .ok_or_else(|| anyhow!("authorization list graph node count overflow"))?;
            if (next_resolution_rows > MAX_LIST_RESOLUTION_ROWS
                || next_graph_nodes > MAX_LIST_GRAPH_NODES)
                && last_processed.is_some()
            {
                source_has_more = true;
                break 'source;
            }
            if next_resolution_rows > MAX_LIST_RESOLUTION_ROWS
                || next_graph_nodes > MAX_LIST_GRAPH_NODES
            {
                return Err(anyhow!(
                    "AuthzGraphBreadthExceeded: one object exceeds the authorization list resolution budget"
                ));
            }
            resolution_rows_visited = next_resolution_rows;
            graph_nodes_visited = next_graph_nodes;
            last_processed = Some(object_id.clone());
            if outcome.allowed {
                object_ids.push(object_id);
                if object_ids.len() == page_size {
                    source_has_more = candidate_index + 1 < candidate_count
                        || candidates.next_object_id.is_some();
                    break 'source;
                }
            }
        }
        let Some(next_object_id) = candidates.next_object_id else {
            source_has_more = false;
            break;
        };
        if scan_after.as_ref() == Some(&next_object_id) {
            return Err(anyhow!(
                "authorization object source did not advance its continuation"
            ));
        }
        scan_after = Some(next_object_id);
        source_has_more = true;
    }
    let next_object_id = source_has_more.then_some(last_processed).flatten();
    if source_has_more && next_object_id.is_none() {
        return Err(anyhow!(
            "authorization object page stopped without a continuation"
        ));
    }

    Ok(AuthzObjectListPage {
        object_ids,
        next_object_id,
        tuple_rows_visited,
        resolution_rows_visited,
        graph_nodes_visited,
    })
}

pub(crate) async fn list_current_authz_subjects_page(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: Option<&str>,
    revision: i64,
    after_subject_position: Option<&str>,
    page_size: usize,
) -> Result<AuthzSubjectListPage> {
    if !(1..=1000).contains(&page_size) {
        return Err(anyhow!(
            "authz subject page size must be between 1 and 1000"
        ));
    }
    let outcome = storage_resolver::collect_subjects_at_current_revision(
        storage,
        tenant_id,
        resolver::UsersetRef {
            namespace: namespace.to_string(),
            object_id: object_id.to_string(),
            relation: relation.to_string(),
        },
        revision,
    )
    .await?;
    let subjects = outcome
        .subjects
        .into_iter()
        .filter(|subject| subject_kind.is_none_or(|kind| subject.kind == kind))
        .map(|subject| AuthzSubjectRef {
            subject_kind: subject.kind,
            subject_id: subject.id,
            caveat_hash: subject.caveat_hash,
        })
        .collect::<Vec<_>>();
    let start = after_subject_position
        .map(|position| {
            subjects.partition_point(|subject| authz_subject_position(subject).as_str() <= position)
        })
        .unwrap_or_default();
    let mut page = subjects
        .into_iter()
        .skip(start)
        .take(page_size.saturating_add(1))
        .collect::<Vec<_>>();
    let has_more = page.len() > page_size;
    if has_more {
        page.truncate(page_size);
    }
    let next_subject_position = has_more
        .then(|| page.last().map(authz_subject_position))
        .flatten();
    Ok(AuthzSubjectListPage {
        subjects: page,
        next_subject_position,
        tuple_rows_visited: outcome.stats.projection_rows_visited,
        graph_nodes_visited: outcome.stats.graph_nodes_visited,
    })
}

fn authz_subject_position(subject: &AuthzSubjectRef) -> String {
    format!(
        "{}\0{}\0{}",
        subject.subject_kind, subject.subject_id, subject.caveat_hash
    )
}

fn matches_authz_tuple_filter(record: &AuthzTupleRecord, filter: &AuthzTupleFilter) -> bool {
    filter.realm_id.as_ref().is_none_or(|value| {
        split_realm_namespace(&record.namespace)
            .map(|(realm_id, _)| realm_id)
            .unwrap_or_else(|| DEFAULT_AUTHZ_REALM_ID.to_string())
            == *value
    }) && filter
        .namespace
        .as_ref()
        .is_none_or(|value| record.namespace == *value)
        && filter
            .object_id
            .as_ref()
            .is_none_or(|value| record.object_id == *value)
        && filter
            .relation
            .as_ref()
            .is_none_or(|value| record.relation == *value)
        && filter
            .subject_kind
            .as_ref()
            .is_none_or(|value| record.subject_kind == *value)
        && filter
            .subject_id
            .as_ref()
            .is_none_or(|value| record.subject_id == *value)
        && filter
            .caveat_hash
            .as_ref()
            .is_none_or(|value| record.caveat_hash == *value)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TupleViewKey {
    pub(crate) namespace: String,
    pub(crate) object_id: String,
    pub(crate) relation: String,
    pub(crate) subject_kind: String,
    pub(crate) subject_id: String,
    pub(crate) caveat_hash: String,
}

impl From<&AuthzTupleRecord> for TupleViewKey {
    fn from(record: &AuthzTupleRecord) -> Self {
        Self {
            namespace: record.namespace.clone(),
            object_id: record.object_id.clone(),
            relation: record.relation.clone(),
            subject_kind: record.subject_kind.clone(),
            subject_id: record.subject_id.clone(),
            caveat_hash: record.caveat_hash.clone(),
        }
    }
}

fn encode_authz_tuple_journal_body(record: &AuthzTupleRecord, fence_token: u64) -> Result<Vec<u8>> {
    encode_deterministic_proto(&AuthzTupleJournalBodyProto {
        schema: AUTHZ_TUPLE_JOURNAL_BODY_SCHEMA.to_string(),
        record: Some(authz_record_to_proto(record)?),
        fence_token,
        mutation_id: record.mutation_id.to_string(),
    })
}

fn encode_authz_tuple_batch_journal_body(
    tenant_id: i64,
    revision: i64,
    records: &[AuthzTupleRecord],
    fence_token: u64,
) -> Result<Vec<u8>> {
    let mutation_id = records
        .first()
        .ok_or_else(|| anyhow!("authz tuple batch body must not be empty"))?
        .mutation_id
        .to_string();
    encode_deterministic_proto(&AuthzTupleBatchJournalBodyProto {
        schema: AUTHZ_TUPLE_BATCH_JOURNAL_BODY_SCHEMA.to_string(),
        revision,
        tenant_id,
        records: records
            .iter()
            .map(authz_record_to_proto)
            .collect::<Result<Vec<_>>>()?,
        fence_token,
        mutation_id,
    })
}

fn decode_authz_tuple_journal_body(bytes: &[u8]) -> Result<AuthzTupleRecord> {
    let body = AuthzTupleJournalBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&body, bytes, "authz tuple journal body")?;
    if body.schema != AUTHZ_TUPLE_JOURNAL_BODY_SCHEMA {
        return Err(anyhow!("authz tuple journal body schema mismatch"));
    }
    let _mutation_id = uuid::Uuid::parse_str(&body.mutation_id)
        .context("authz tuple journal body mutation_id is not a UUID")?;
    let record = authz_record_from_proto(
        body.record
            .ok_or_else(|| anyhow!("authz tuple journal body is missing record"))?,
    )?;
    if record.mutation_id.to_string() != body.mutation_id {
        return Err(anyhow!(
            "authz tuple journal body mutation_id does not match record"
        ));
    }
    Ok(record)
}

fn decode_authz_tuple_batch_journal_body(bytes: &[u8]) -> Result<Vec<AuthzTupleRecord>> {
    let body = AuthzTupleBatchJournalBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&body, bytes, "authz tuple batch journal body")?;
    if body.schema != AUTHZ_TUPLE_BATCH_JOURNAL_BODY_SCHEMA {
        return Err(anyhow!("authz tuple batch journal body schema mismatch"));
    }
    let _mutation_id = uuid::Uuid::parse_str(&body.mutation_id)
        .context("authz tuple batch journal body mutation_id is not a UUID")?;
    if body.records.is_empty() {
        return Err(anyhow!("authz tuple batch journal body is empty"));
    }
    body.records
        .into_iter()
        .map(|record| {
            if record.tenant_id != body.tenant_id || record.revision != body.revision {
                return Err(anyhow!("authz tuple batch body contains mismatched record"));
            }
            authz_record_from_proto(record)
        })
        .collect()
}

fn decode_authz_tuple_journal_body_fence(bytes: &[u8]) -> Result<u64> {
    let body = AuthzTupleJournalBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&body, bytes, "authz tuple journal body")?;
    if body.schema != AUTHZ_TUPLE_JOURNAL_BODY_SCHEMA {
        return Err(anyhow!("authz tuple journal body schema mismatch"));
    }
    let _mutation_id = uuid::Uuid::parse_str(&body.mutation_id)
        .context("authz tuple journal body mutation_id is not a UUID")?;
    Ok(body.fence_token)
}

fn decode_authz_tuple_batch_journal_body_fence(bytes: &[u8]) -> Result<u64> {
    let body = AuthzTupleBatchJournalBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&body, bytes, "authz tuple batch journal body")?;
    if body.schema != AUTHZ_TUPLE_BATCH_JOURNAL_BODY_SCHEMA {
        return Err(anyhow!("authz tuple batch journal body schema mismatch"));
    }
    let _mutation_id = uuid::Uuid::parse_str(&body.mutation_id)
        .context("authz tuple batch journal body mutation_id is not a UUID")?;
    Ok(body.fence_token)
}

fn authz_record_to_proto(record: &AuthzTupleRecord) -> Result<AuthzTupleRecordProto> {
    let written_at_unix_nanos = record
        .written_at
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("authz tuple timestamp cannot be represented in nanoseconds"))?;
    Ok(AuthzTupleRecordProto {
        revision: record.revision,
        revision_ordinal: record.revision_ordinal,
        tenant_id: record.tenant_id,
        namespace: record.namespace.clone(),
        object_id: record.object_id.clone(),
        relation: record.relation.clone(),
        subject_kind: record.subject_kind.clone(),
        subject_id: record.subject_id.clone(),
        caveat_hash: record.caveat_hash.clone(),
        operation: authz_operation_to_proto(&record.operation)? as i32,
        written_by: record.written_by.clone(),
        reason: record.reason.clone(),
        mutation_id: record.mutation_id.to_string(),
        record_hash: record.record_hash.clone(),
        written_at_unix_nanos,
    })
}

fn authz_record_from_proto(proto: AuthzTupleRecordProto) -> Result<AuthzTupleRecord> {
    Ok(AuthzTupleRecord {
        revision: proto.revision,
        revision_ordinal: proto.revision_ordinal,
        tenant_id: proto.tenant_id,
        namespace: proto.namespace,
        object_id: proto.object_id,
        relation: proto.relation,
        subject_kind: proto.subject_kind,
        subject_id: proto.subject_id,
        caveat_hash: proto.caveat_hash,
        operation: authz_operation_from_proto(proto.operation)?.to_string(),
        written_by: proto.written_by,
        reason: proto.reason,
        mutation_id: uuid::Uuid::parse_str(&proto.mutation_id)
            .context("authz tuple mutation_id is not a UUID")?,
        record_hash: proto.record_hash,
        written_at: chrono::DateTime::from_timestamp_nanos(proto.written_at_unix_nanos),
    })
}

fn authz_operation_to_proto(operation: &str) -> Result<AuthzTupleOperationProto> {
    match operation {
        "add" => Ok(AuthzTupleOperationProto::Add),
        "remove" => Ok(AuthzTupleOperationProto::Remove),
        other => Err(anyhow!("unsupported authz tuple operation {other}")),
    }
}

fn authz_operation_from_proto(operation: i32) -> Result<&'static str> {
    match AuthzTupleOperationProto::try_from(operation)
        .map_err(|_| anyhow!("unknown authz tuple operation {operation}"))?
    {
        AuthzTupleOperationProto::Add => Ok("add"),
        AuthzTupleOperationProto::Remove => Ok("remove"),
        AuthzTupleOperationProto::Unspecified => {
            Err(anyhow!("authz tuple operation must be specified"))
        }
    }
}

fn encode_deterministic_proto(message: &impl Message) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes)?;
    Ok(bytes)
}

fn ensure_deterministic_proto(message: &impl Message, bytes: &[u8], label: &str) -> Result<()> {
    let encoded = encode_deterministic_proto(message)?;
    if encoded != bytes {
        return Err(anyhow!("{label} is not deterministically encoded"));
    }
    Ok(())
}

pub fn authz_partition_id(tenant_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/authz_tuple").as_bytes())
}

pub(crate) fn authz_tuple_stream_id(tenant_id: i64) -> String {
    format!("authz_tuple:tenant:{tenant_id}")
}

pub(crate) async fn latest_authz_journal_fence_token(
    storage: &Storage,
    tenant_id: i64,
) -> Result<u64> {
    Ok(authz_head::read(storage, tenant_id)
        .await?
        .head
        .tuple_fence_token)
}

#[cfg(test)]
pub(crate) async fn read_authz_frame_fences_for_test(
    storage: &Storage,
    tenant_id: i64,
) -> Result<Vec<u64>> {
    read_authz_journal_payload_fences(storage, tenant_id).await
}

#[cfg(test)]
async fn read_authz_journal_payload_fences(storage: &Storage, tenant_id: i64) -> Result<Vec<u64>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut fences = Vec::new();
    let mut after_sequence = 0;
    loop {
        let page = core_store
            .read_stream_page(ReadStream {
                stream_id: authz_tuple_stream_id(tenant_id),
                after_sequence,
                limit: 1_000,
            })
            .await?;
        fences.extend(page.records.into_iter().filter_map(
            |record| match record.record_kind.as_str() {
                AUTHZ_TUPLE_RECORD_KIND => {
                    Some(decode_authz_tuple_journal_body_fence(&record.payload))
                }
                AUTHZ_TUPLE_BATCH_RECORD_KIND => {
                    Some(decode_authz_tuple_batch_journal_body_fence(&record.payload))
                }
                _ => None,
            },
        ));
        if !page.has_more {
            break;
        }
        if page.next_sequence <= after_sequence {
            bail!("authorization fence page did not advance its continuation");
        }
        after_sequence = page.next_sequence;
    }
    fences.into_iter().collect()
}

fn require_authz_permit(tenant_id: i64, permit: &PartitionWritePermit) -> Result<()> {
    if permit.partition_family != "authz_tuple"
        || permit.partition_id != hex::encode(authz_partition_id(tenant_id))
    {
        return Err(anyhow!(
            "partition write permit does not target this authorization tuple partition"
        ));
    }
    Ok(())
}

struct AuthzRecordHashInput<'a> {
    revision: i64,
    revision_ordinal: u32,
    tenant_id: i64,
    namespace: &'a str,
    object_id: &'a str,
    relation: &'a str,
    subject_kind: &'a str,
    subject_id: &'a str,
    caveat_hash: &'a str,
    operation: &'a str,
    written_by: &'a str,
    reason: &'a str,
}

fn authz_record_hash(input: AuthzRecordHashInput<'_>) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&input.revision.to_le_bytes());
    hasher.update(&input.revision_ordinal.to_le_bytes());
    hasher.update(&input.tenant_id.to_le_bytes());
    for part in [
        input.namespace,
        input.object_id,
        input.relation,
        input.subject_kind,
        input.subject_id,
        input.caveat_hash,
        input.operation,
        input.written_by,
        input.reason,
    ] {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

#[cfg(test)]
mod tests;
