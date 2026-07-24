use super::{
    AuthzTupleRecordProto, AuthzTupleWrite, authz_record_from_proto, authz_record_to_proto,
};
use crate::{
    core_store::{
        CF_AUTHZ, CoreMetaRowCommonProto, CoreMetaStore, CoreMetaTuplePart,
        CoreMetaVisibilityState, CoreMutationOperation, CoreMutationPrecondition, CoreStore,
        TABLE_AUTHZ_IDEMPOTENCY_RECEIPT_ROW, TABLE_AUTHZ_SCHEMA_ROW,
        core_meta_committed_row_common, core_meta_payload_digest, core_meta_root_key_hash,
        core_meta_tuple_key,
    },
    persistence::{
        AuthzSchemaBindingPrecondition, AuthzTupleBatchWriteError, AuthzTupleBatchWriteOptions,
        AuthzTupleBatchWriteOutcome,
    },
    storage::Storage,
};
use anyhow::{Context, Result, anyhow, bail};
use prost::Message;

pub(crate) const MAX_AUTHZ_BATCH_OPERATION_ID_BYTES: usize = 128;

const AUTHZ_IDEMPOTENCY_RECEIPT_SCHEMA: &str = "anvil.authz.idempotency_receipt.v2";
const CANONICAL_AUTHZ_BATCH_REQUEST_SCHEMA: &str = "anvil.authz.canonical_tuple_batch_request.v1";
const AUTHZ_IDEMPOTENCY_ROW_KIND: &str = "authz-batch-idempotency";

#[derive(Clone, PartialEq, Message)]
struct CanonicalAuthzTupleMutationProto {
    #[prost(string, tag = "1")]
    namespace: String,
    #[prost(string, tag = "2")]
    object_id: String,
    #[prost(string, tag = "3")]
    relation: String,
    #[prost(string, tag = "4")]
    subject_kind: String,
    #[prost(string, tag = "5")]
    subject_id: String,
    #[prost(string, tag = "6")]
    caveat_hash: String,
    #[prost(string, tag = "7")]
    operation: String,
    #[prost(string, tag = "8")]
    reason: String,
}

#[derive(Clone, PartialEq, Message)]
struct CanonicalAuthzTupleBatchRequestProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(string, tag = "3")]
    principal: String,
    #[prost(string, tag = "4")]
    authz_realm_id: String,
    #[prost(string, tag = "5")]
    operation_id: String,
    #[prost(int64, optional, tag = "6")]
    expected_revision: Option<i64>,
    #[prost(message, repeated, tag = "7")]
    mutations: Vec<CanonicalAuthzTupleMutationProto>,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzIdempotencyReceiptProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(string, tag = "4")]
    principal: String,
    #[prost(string, tag = "5")]
    authz_realm_id: String,
    #[prost(string, tag = "6")]
    operation_id: String,
    #[prost(string, tag = "7")]
    request_hash: String,
    #[prost(int64, tag = "8")]
    revision: i64,
    #[prost(uint32, tag = "9")]
    mutation_count: u32,
    #[prost(string, tag = "10")]
    results_hash: String,
    #[prost(int64, tag = "11")]
    committed_at_unix_nanos: i64,
    #[prost(string, tag = "12")]
    receipt_hash: String,
    #[prost(message, repeated, tag = "13")]
    records: Vec<AuthzTupleRecordProto>,
}

#[derive(Debug, Clone)]
struct AuthzIdempotencyReceipt {
    tenant_id: i64,
    principal: String,
    authz_realm_id: String,
    operation_id: String,
    request_hash: String,
    revision: i64,
    mutation_count: u32,
    results_hash: String,
    committed_at_unix_nanos: i64,
    receipt_hash: String,
    records: Vec<crate::persistence::AuthzTupleRecord>,
}

pub(super) struct PreparedAuthzIdempotencyReceipt {
    pub(super) tuple_key: Vec<u8>,
    pub(super) payload: Vec<u8>,
    pub(super) transaction_id: String,
}

pub(crate) fn validate_operation_id(operation_id: &str) -> Result<()> {
    if operation_id.is_empty() {
        bail!("operation_id must not be empty when provided");
    }
    if operation_id.len() > MAX_AUTHZ_BATCH_OPERATION_ID_BYTES {
        bail!(
            "operation_id must contain no more than {MAX_AUTHZ_BATCH_OPERATION_ID_BYTES} UTF-8 bytes"
        );
    }
    if operation_id.chars().any(char::is_control) {
        bail!("operation_id must not contain control characters");
    }
    Ok(())
}

pub(super) async fn replay(
    storage: &Storage,
    inputs: &[AuthzTupleWrite<'_>],
    options: &AuthzTupleBatchWriteOptions,
) -> Result<Option<AuthzTupleBatchWriteOutcome>> {
    let Some(operation_id) = options.operation_id.as_deref() else {
        return Ok(None);
    };
    validate_operation_id(operation_id)?;
    let (tenant_id, principal) = batch_context(inputs)?;
    let request_hash = canonical_request_hash(inputs, options, operation_id)?;
    let store = CoreStore::new(storage.clone()).await?;
    let Some(receipt) = read_receipt(&store, tenant_id, principal, operation_id)? else {
        return Ok(None);
    };
    validate_receipt_context(
        &receipt,
        tenant_id,
        principal,
        &options.authz_realm_id,
        operation_id,
    )?;
    if receipt.request_hash != request_hash {
        return Err(anyhow!(AuthzTupleBatchWriteError::OperationConflict));
    }
    let records = receipt.records.clone();
    if records.len() != receipt.mutation_count as usize
        || batch_results_hash(receipt.revision, &records) != receipt.results_hash
    {
        bail!("authorization idempotency receipt results do not match the tuple journal");
    }
    Ok(Some(AuthzTupleBatchWriteOutcome {
        records,
        replayed: true,
    }))
}

pub(super) fn prepare_receipt(
    inputs: &[AuthzTupleWrite<'_>],
    options: &AuthzTupleBatchWriteOptions,
    records: &[crate::persistence::AuthzTupleRecord],
) -> Result<Option<PreparedAuthzIdempotencyReceipt>> {
    let Some(operation_id) = options.operation_id.as_deref() else {
        return Ok(None);
    };
    validate_operation_id(operation_id)?;
    let (tenant_id, principal) = batch_context(inputs)?;
    let revision = records
        .first()
        .ok_or_else(|| anyhow!("authorization idempotency receipt requires batch records"))?
        .revision;
    let committed_at_unix_nanos = records
        .iter()
        .map(|record| record.written_at.timestamp_nanos_opt())
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| anyhow!("authorization batch timestamp is outside the supported range"))?
        .into_iter()
        .max()
        .unwrap_or_default();
    let mut receipt = AuthzIdempotencyReceipt {
        tenant_id,
        principal: principal.to_string(),
        authz_realm_id: options.authz_realm_id.clone(),
        operation_id: operation_id.to_string(),
        request_hash: canonical_request_hash(inputs, options, operation_id)?,
        revision,
        mutation_count: u32::try_from(records.len())
            .context("authorization idempotency mutation count exceeds u32")?,
        results_hash: batch_results_hash(revision, records),
        committed_at_unix_nanos,
        receipt_hash: String::new(),
        records: records.to_vec(),
    };
    receipt.receipt_hash = receipt_hash(&receipt)?;
    let operation_key_hash = operation_key_hash(tenant_id, principal, operation_id);
    Ok(Some(PreparedAuthzIdempotencyReceipt {
        tuple_key: receipt_tuple_key(tenant_id, &operation_key_hash)?,
        payload: encode_receipt(&receipt)?,
        transaction_id: receipt_transaction_id(&operation_key_hash),
    }))
}

pub(super) fn receipt_precondition(
    receipt: &PreparedAuthzIdempotencyReceipt,
) -> CoreMutationPrecondition {
    CoreMutationPrecondition::CoreMetaRow {
        cf: CF_AUTHZ.to_string(),
        table_id: TABLE_AUTHZ_IDEMPOTENCY_RECEIPT_ROW,
        tuple_key: receipt.tuple_key.clone(),
        expected_payload_hash: None,
        require_absent: true,
        require_present: false,
    }
}

pub(super) fn receipt_operation(
    partition_id: &str,
    receipt: &PreparedAuthzIdempotencyReceipt,
) -> CoreMutationOperation {
    CoreMutationOperation::CoreMetaPut {
        partition_id: partition_id.to_string(),
        cf: CF_AUTHZ.to_string(),
        table_id: TABLE_AUTHZ_IDEMPOTENCY_RECEIPT_ROW,
        tuple_key: receipt.tuple_key.clone(),
        payload: receipt.payload.clone(),
    }
}

pub(super) fn schema_binding_precondition(
    fence: &AuthzSchemaBindingPrecondition,
) -> CoreMutationPrecondition {
    CoreMutationPrecondition::CoreMetaRow {
        cf: CF_AUTHZ.to_string(),
        table_id: TABLE_AUTHZ_SCHEMA_ROW,
        tuple_key: fence.tuple_key.clone(),
        expected_payload_hash: fence.expected_payload_hash.clone(),
        require_absent: fence.expected_payload_hash.is_none(),
        require_present: fence.expected_payload_hash.is_some(),
    }
}

pub(super) fn schema_binding_is_current(
    storage: &Storage,
    fence: &AuthzSchemaBindingPrecondition,
) -> Result<bool> {
    // Compare the physical row used by the mutation precondition, not a visibility projection.
    let current = CoreMetaStore::open(storage.core_store_meta_path())?.get(
        CF_AUTHZ,
        TABLE_AUTHZ_SCHEMA_ROW,
        &fence.tuple_key,
    )?;
    Ok(match (current, fence.expected_payload_hash.as_deref()) {
        (None, None) => true,
        (Some(payload), Some(expected)) => {
            core_meta_payload_digest(TABLE_AUTHZ_SCHEMA_ROW, &payload) == expected
        }
        _ => false,
    })
}

fn batch_context<'a>(inputs: &'a [AuthzTupleWrite<'a>]) -> Result<(i64, &'a str)> {
    let first = inputs
        .first()
        .ok_or_else(|| anyhow!("authz tuple batch must not be empty"))?;
    if inputs
        .iter()
        .any(|input| input.tenant_id != first.tenant_id || input.written_by != first.written_by)
    {
        bail!("authz tuple batch idempotency context must target one tenant and principal");
    }
    Ok((first.tenant_id, first.written_by))
}

fn canonical_request_hash(
    inputs: &[AuthzTupleWrite<'_>],
    options: &AuthzTupleBatchWriteOptions,
    operation_id: &str,
) -> Result<String> {
    let (tenant_id, principal) = batch_context(inputs)?;
    let canonical = CanonicalAuthzTupleBatchRequestProto {
        schema: CANONICAL_AUTHZ_BATCH_REQUEST_SCHEMA.to_string(),
        tenant_id,
        principal: principal.to_string(),
        authz_realm_id: options.authz_realm_id.clone(),
        operation_id: operation_id.to_string(),
        expected_revision: options.expected_revision,
        mutations: inputs
            .iter()
            .map(|input| CanonicalAuthzTupleMutationProto {
                namespace: input.namespace.to_string(),
                object_id: input.object_id.to_string(),
                relation: input.relation.to_string(),
                subject_kind: input.subject_kind.to_string(),
                subject_id: input.subject_id.to_string(),
                caveat_hash: input.caveat_hash.to_string(),
                operation: input.operation.to_string(),
                reason: input.reason.to_string(),
            })
            .collect(),
    };
    Ok(hash_bytes(&canonical.encode_to_vec()))
}

fn operation_key_hash(tenant_id: i64, principal: &str, operation_id: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.authz.idempotency_key.v1");
    hasher.update(&tenant_id.to_le_bytes());
    hash_part(&mut hasher, principal);
    hash_part(&mut hasher, operation_id);
    format!("blake3:{}", hasher.finalize().to_hex())
}

fn receipt_tuple_key(tenant_id: i64, operation_key_hash: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(AUTHZ_IDEMPOTENCY_ROW_KIND),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Hash(operation_key_hash),
    ])
}

fn read_receipt(
    store: &CoreStore,
    tenant_id: i64,
    principal: &str,
    operation_id: &str,
) -> Result<Option<AuthzIdempotencyReceipt>> {
    let operation_key_hash = operation_key_hash(tenant_id, principal, operation_id);
    let Some(payload) = store.read_coremeta_row(
        CF_AUTHZ,
        TABLE_AUTHZ_IDEMPOTENCY_RECEIPT_ROW,
        &receipt_tuple_key(tenant_id, &operation_key_hash)?,
    )?
    else {
        return Ok(None);
    };
    decode_receipt(&payload).map(Some)
}

fn encode_receipt(receipt: &AuthzIdempotencyReceipt) -> Result<Vec<u8>> {
    Ok(receipt_to_proto(receipt)?.encode_to_vec())
}

fn decode_receipt(bytes: &[u8]) -> Result<AuthzIdempotencyReceipt> {
    let proto = AuthzIdempotencyReceiptProto::decode(bytes)?;
    if proto.encode_to_vec() != bytes {
        bail!("authorization idempotency receipt is not deterministically encoded");
    }
    if proto.schema != AUTHZ_IDEMPOTENCY_RECEIPT_SCHEMA {
        bail!("authorization idempotency receipt schema mismatch");
    }
    let records = proto
        .records
        .iter()
        .cloned()
        .map(authz_record_from_proto)
        .collect::<Result<Vec<_>>>()?;
    let common = proto
        .common
        .clone()
        .ok_or_else(|| anyhow!("authorization idempotency receipt is missing CoreMeta common"))?;
    let receipt = AuthzIdempotencyReceipt {
        tenant_id: proto.tenant_id,
        principal: proto.principal,
        authz_realm_id: proto.authz_realm_id,
        operation_id: proto.operation_id,
        request_hash: proto.request_hash,
        revision: proto.revision,
        mutation_count: proto.mutation_count,
        results_hash: proto.results_hash,
        committed_at_unix_nanos: proto.committed_at_unix_nanos,
        receipt_hash: proto.receipt_hash,
        records,
    };
    validate_receipt(&receipt, &common)?;
    Ok(receipt)
}

fn receipt_to_proto(receipt: &AuthzIdempotencyReceipt) -> Result<AuthzIdempotencyReceiptProto> {
    Ok(AuthzIdempotencyReceiptProto {
        common: Some(receipt_common(receipt)),
        schema: AUTHZ_IDEMPOTENCY_RECEIPT_SCHEMA.to_string(),
        tenant_id: receipt.tenant_id,
        principal: receipt.principal.clone(),
        authz_realm_id: receipt.authz_realm_id.clone(),
        operation_id: receipt.operation_id.clone(),
        request_hash: receipt.request_hash.clone(),
        revision: receipt.revision,
        mutation_count: receipt.mutation_count,
        results_hash: receipt.results_hash.clone(),
        committed_at_unix_nanos: receipt.committed_at_unix_nanos,
        receipt_hash: receipt.receipt_hash.clone(),
        records: receipt
            .records
            .iter()
            .map(authz_record_to_proto)
            .collect::<Result<Vec<_>>>()?,
    })
}

fn receipt_common(receipt: &AuthzIdempotencyReceipt) -> CoreMetaRowCommonProto {
    let operation_key_hash =
        operation_key_hash(receipt.tenant_id, &receipt.principal, &receipt.operation_id);
    core_meta_committed_row_common(
        format!("tenant/{}/authz", receipt.tenant_id),
        core_meta_root_key_hash(&format!("authz/{}", receipt.tenant_id)),
        receipt.revision.max(0) as u64,
        receipt_transaction_id(&operation_key_hash),
        receipt.committed_at_unix_nanos.max(0) as u64,
    )
}

fn receipt_transaction_id(operation_key_hash: &str) -> String {
    format!(
        "authz-tuple-batch-idempotent:{}",
        operation_key_hash
            .strip_prefix("blake3:")
            .unwrap_or(operation_key_hash)
    )
}

fn validate_receipt(
    receipt: &AuthzIdempotencyReceipt,
    common: &CoreMetaRowCommonProto,
) -> Result<()> {
    validate_operation_id(&receipt.operation_id)?;
    if receipt.tenant_id < 0 || receipt.revision <= 0 || receipt.mutation_count == 0 {
        bail!("authorization idempotency receipt has invalid numeric fields");
    }
    if receipt.records.len() != receipt.mutation_count as usize
        || receipt.records.iter().any(|record| {
            record.tenant_id != receipt.tenant_id || record.revision != receipt.revision
        })
        || batch_results_hash(receipt.revision, &receipt.records) != receipt.results_hash
    {
        bail!("authorization idempotency receipt result records mismatch");
    }
    validate_hash(&receipt.request_hash, "request hash")?;
    validate_hash(&receipt.results_hash, "results hash")?;
    validate_hash(&receipt.receipt_hash, "receipt hash")?;
    let expected_common = receipt_common(receipt);
    if common.realm_id != expected_common.realm_id
        || common.root_key_hash != expected_common.root_key_hash
        || common.root_generation != expected_common.root_generation
        || common.transaction_id != expected_common.transaction_id
        || common.created_at_unix_nanos != expected_common.created_at_unix_nanos
        || common.visibility_state_enum() != CoreMetaVisibilityState::Committed
    {
        bail!("authorization idempotency receipt CoreMeta common mismatch");
    }
    if receipt_hash(receipt)? != receipt.receipt_hash {
        bail!("authorization idempotency receipt hash mismatch");
    }
    Ok(())
}

fn validate_receipt_context(
    receipt: &AuthzIdempotencyReceipt,
    tenant_id: i64,
    principal: &str,
    authz_realm_id: &str,
    operation_id: &str,
) -> Result<()> {
    if receipt.tenant_id != tenant_id
        || receipt.principal != principal
        || receipt.authz_realm_id != authz_realm_id
        || receipt.operation_id != operation_id
    {
        bail!("authorization idempotency receipt context mismatch");
    }
    Ok(())
}

fn receipt_hash(receipt: &AuthzIdempotencyReceipt) -> Result<String> {
    let mut proto = receipt_to_proto(receipt)?;
    proto.receipt_hash.clear();
    Ok(hash_bytes(&proto.encode_to_vec()))
}

fn batch_results_hash(revision: i64, records: &[crate::persistence::AuthzTupleRecord]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.authz.idempotency_results.v1");
    hasher.update(&revision.to_le_bytes());
    hasher.update(&(records.len() as u64).to_le_bytes());
    for record in records {
        hasher.update(&record.revision_ordinal.to_le_bytes());
        hash_part(&mut hasher, &record.record_hash);
    }
    format!("blake3:{}", hasher.finalize().to_hex())
}

fn hash_bytes(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

fn hash_part(hasher: &mut blake3::Hasher, value: &str) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value.as_bytes());
}

fn validate_hash(value: &str, label: &str) -> Result<()> {
    let Some(hex) = value.strip_prefix("blake3:") else {
        bail!("authorization idempotency {label} must use blake3 format");
    };
    if hex.len() != 64 || !hex.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        bail!("authorization idempotency {label} must contain a 32-byte hex digest");
    }
    Ok(())
}
