use crate::{
    anvil_api::NativeMutationContext,
    core_store::{
        CF_TRANSACTIONS, CoreMetaTuplePart, TABLE_NATIVE_IDEMPOTENCY_ROW, core_meta_tuple_key,
    },
    mvcc_bootstrap::MvccSubsystem,
    mvcc_product::ProductMutation,
    mvcc_transaction::{DurabilityLevel, PredicateKind},
};
use prost::Message;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value as JsonValue;
use tonic::Status;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NativeIdempotencyTarget {
    pub operation: String,
    pub bucket_name: String,
    pub object_key: String,
    #[serde(default)]
    pub parameters: JsonValue,
}

impl NativeIdempotencyTarget {
    pub fn new(
        operation: impl Into<String>,
        bucket_name: impl Into<String>,
        object_key: impl Into<String>,
    ) -> Self {
        Self {
            operation: operation.into(),
            bucket_name: bucket_name.into(),
            object_key: object_key.into(),
            parameters: JsonValue::Null,
        }
    }

    pub fn with_parameters(mut self, parameters: JsonValue) -> Self {
        self.parameters = parameters;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NativeIdempotencyRecord {
    format_version: u16,
    tenant_id: i64,
    bucket_id: i64,
    principal: String,
    idempotency_key: String,
    transaction_id: Option<String>,
    request_id: String,
    target: NativeIdempotencyTarget,
    response_json: JsonValue,
    response_hash: String,
    record_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NativeGenericResult {
    target: NativeIdempotencyTarget,
    response_json: JsonValue,
}

pub(crate) fn stage_generic_result<T: Serialize>(
    mvcc: &MvccSubsystem,
    transaction_id: &str,
    transaction_principal: &str,
    context: &NativeMutationContext,
    target: &NativeIdempotencyTarget,
    response: &T,
) -> Result<(), Status> {
    let payload = serde_json::to_vec(&NativeGenericResult {
        target: target.clone(),
        response_json: serde_json::to_value(response)
            .map_err(|error| Status::internal(error.to_string()))?,
    })
    .map_err(|error| Status::internal(error.to_string()))?;
    mvcc.open_transactions
        .add_idempotency_result(
            transaction_id,
            transaction_principal,
            crate::mvcc_transaction::IdempotencyResult {
                namespace: generic_namespace(context),
                key: context.idempotency_key.clone(),
                payload,
            },
            current_unix_ms(),
        )
        .map_err(|error| Status::failed_precondition(error.to_string()))
}

pub(crate) fn load_generic_response<T: DeserializeOwned>(
    mvcc: &MvccSubsystem,
    transaction_id: &str,
    context: &NativeMutationContext,
    target: &NativeIdempotencyTarget,
) -> Result<Option<T>, Status> {
    let Some(record) = mvcc
        .runtime
        .local_store()
        .committed_idempotency_result(
            transaction_id,
            &generic_namespace(context),
            &context.idempotency_key,
        )
        .map_err(|error| Status::internal(error.to_string()))?
    else {
        return Ok(None);
    };
    let result: NativeGenericResult = serde_json::from_slice(&record.result.payload)
        .map_err(|error| Status::data_loss(error.to_string()))?;
    if &result.target != target {
        return Err(Status::failed_precondition(
            "Native idempotency key already used for different input",
        ));
    }
    serde_json::from_value(result.response_json)
        .map(Some)
        .map_err(|error| Status::data_loss(error.to_string()))
}

pub(crate) fn generic_result_exists(
    mvcc: &MvccSubsystem,
    transaction_id: &str,
    context: &NativeMutationContext,
) -> Result<bool, Status> {
    mvcc.runtime
        .local_store()
        .committed_idempotency_result(
            transaction_id,
            &generic_namespace(context),
            &context.idempotency_key,
        )
        .map(|result| result.is_some())
        .map_err(|error| Status::internal(error.to_string()))
}

fn generic_namespace(context: &NativeMutationContext) -> String {
    format!(
        "native/{}/{}/{}",
        context.tenant_id, context.bucket_id, context.principal
    )
}

#[derive(Clone, PartialEq, Message)]
struct NativeIdempotencyTargetProto {
    #[prost(string, tag = "1")]
    operation: String,
    #[prost(string, tag = "2")]
    bucket_name: String,
    #[prost(string, tag = "3")]
    object_key: String,
    #[prost(bytes, tag = "4")]
    parameters_json: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct NativeIdempotencyRecordBodyProto {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(int64, tag = "3")]
    bucket_id: i64,
    #[prost(string, tag = "4")]
    principal: String,
    #[prost(string, tag = "5")]
    idempotency_key: String,
    #[prost(string, tag = "6")]
    request_id: String,
    #[prost(message, optional, tag = "7")]
    target: Option<NativeIdempotencyTargetProto>,
    #[prost(bytes, tag = "8")]
    response_json: Vec<u8>,
    #[prost(string, tag = "9")]
    response_hash: String,
    #[prost(string, tag = "10")]
    record_hash: String,
    #[prost(string, optional, tag = "11")]
    transaction_id: Option<String>,
}

pub async fn load_response<T>(
    mvcc: &MvccSubsystem,
    context: &NativeMutationContext,
    target: &NativeIdempotencyTarget,
) -> Result<Option<T>, Status>
where
    T: DeserializeOwned,
{
    linearize_response_read(mvcc).await?;
    let Some(record) = read_record(mvcc, context)? else {
        return Ok(None);
    };
    validate_record_context(&record, context, target)?;
    let response = serde_json::from_value(record.response_json)
        .map_err(|e| Status::internal(format!("Invalid native idempotency response: {e}")))?;
    Ok(Some(response))
}

/// Checks the target-independent identity of a durable native response.
///
/// Streaming mutations cannot construct their complete target until they have
/// consumed and hashed the request body.  This lets a retry discover a
/// committed response before creating a new local transaction draft; only the
/// replay case then consumes the body and calls [`load_response`] to validate
/// the complete target.
pub(crate) async fn response_exists(
    mvcc: &MvccSubsystem,
    context: &NativeMutationContext,
) -> Result<bool, Status> {
    linearize_response_read(mvcc).await?;
    read_record(mvcc, context).map(|record| record.is_some())
}

async fn linearize_response_read(mvcc: &MvccSubsystem) -> Result<(), Status> {
    // A retry may arrive at a follower immediately after another coordinator
    // committed the mutation. Establish the cluster read point and wait for
    // this node to apply through it before consulting the durable response.
    // This keeps replay local to the contacted node without treating temporary
    // follower lag as a missing idempotency record and re-proposing the same
    // deterministic transaction.
    mvcc.runtime
        .snapshot(crate::mvcc_transaction::ReadConsistency::Linearized)
        .await
        .map(|_| ())
        .map_err(|error| Status::unavailable(error.to_string()))
}

pub async fn store_response<T>(
    mvcc: &MvccSubsystem,
    context: &NativeMutationContext,
    target: &NativeIdempotencyTarget,
    response: &T,
) -> Result<(), Status>
where
    T: Serialize,
{
    if let Some(record) = read_record(mvcc, context)? {
        validate_record_context(&record, context, target)?;
        return Ok(());
    }

    let response_json = serde_json::to_value(response)
        .map_err(|e| Status::internal(format!("Serialize native idempotency response: {e}")))?;
    let response_hash = native_response_hash(&response_json)?;
    let mut record = NativeIdempotencyRecord {
        format_version: 3,
        tenant_id: context.tenant_id,
        bucket_id: context.bucket_id,
        principal: context.principal.clone(),
        idempotency_key: context.idempotency_key.clone(),
        transaction_id: context.transaction_id.clone(),
        request_id: context.request_id.clone(),
        target: target.clone(),
        response_json,
        response_hash,
        record_hash: String::new(),
    };
    record.record_hash = record_hash(&record)?;

    let bytes = encode_record(&record)?;
    let logical_key = record_logical_key(context)?;
    if let Some(transaction_id) = context.transaction_id.as_deref() {
        mvcc.stage_product_mutations(
            transaction_id,
            &native_transaction_principal_from_context(context),
            vec![ProductMutation::put(logical_key.clone(), bytes)],
            current_unix_ms(),
        )
        .map_err(|error| Status::internal(error.to_string()))?;
        mvcc.stage_predicate(
            transaction_id,
            &native_transaction_principal_from_context(context),
            logical_key,
            PredicateKind::Absent,
            current_unix_ms(),
        )
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
        return Ok(());
    }

    let principal = native_transaction_principal_from_context(context);
    let idempotency_key = format!("native-idempotency:{}", record.record_hash);
    if let Err(error) = mvcc
        .autocommit_product_mutations_with_predicates(
            &principal,
            &idempotency_key,
            vec![ProductMutation::put(logical_key.clone(), bytes)],
            vec![(logical_key, PredicateKind::Absent)],
            DurabilityLevel::Quorum,
            current_unix_ms(),
        )
        .await
    {
        if let Some(existing) = read_record(mvcc, context)? {
            validate_record_context(&existing, context, target)?;
            return Ok(());
        }
        return Err(Status::internal(error.to_string()));
    }
    Ok(())
}

pub(crate) async fn prepare_response_in_transaction<T>(
    mvcc: &MvccSubsystem,
    context: &NativeMutationContext,
    target: &NativeIdempotencyTarget,
    response: &T,
) -> Result<crate::mvcc_product::ProductMutationPlan, Status>
where
    T: Serialize,
{
    let transaction_id = context
        .transaction_id
        .as_deref()
        .ok_or_else(|| Status::failed_precondition("TransactionRequired"))?;
    if let Some(record) = read_record(mvcc, context)? {
        validate_record_context(&record, context, target)?;
        return Err(Status::already_exists("NativeIdempotencyRecordExists"));
    }

    let response_json = serde_json::to_value(response).map_err(|error| {
        Status::internal(format!("Serialize native idempotency response: {error}"))
    })?;
    let response_hash = native_response_hash(&response_json)?;
    let mut record = NativeIdempotencyRecord {
        format_version: 3,
        tenant_id: context.tenant_id,
        bucket_id: context.bucket_id,
        principal: context.principal.clone(),
        idempotency_key: context.idempotency_key.clone(),
        transaction_id: context.transaction_id.clone(),
        request_id: context.request_id.clone(),
        target: target.clone(),
        response_json,
        response_hash,
        record_hash: String::new(),
    };
    record.record_hash = record_hash(&record)?;
    let payload = encode_record(&record)?;
    let logical_key = record_logical_key(context)?;
    mvcc.stage_product_mutations(
        transaction_id,
        &native_transaction_principal_from_context(context),
        vec![ProductMutation::put(logical_key.clone(), payload)],
        current_unix_ms(),
    )
    .map_err(|error| Status::internal(error.to_string()))?;
    mvcc.stage_predicate(
        transaction_id,
        &native_transaction_principal_from_context(context),
        logical_key,
        PredicateKind::Absent,
        current_unix_ms(),
    )
    .map_err(|error| Status::failed_precondition(error.to_string()))?;

    Ok(crate::mvcc_product::ProductMutationPlan::default())
}

pub(crate) async fn prepare_response_for_implicit_batch<T>(
    mvcc: &MvccSubsystem,
    context: &NativeMutationContext,
    target: &NativeIdempotencyTarget,
    response: &T,
) -> Result<crate::mvcc_product::ProductMutationPlan, Status>
where
    T: Serialize,
{
    if context.transaction_id.is_some() {
        return Err(Status::failed_precondition("ImplicitMutationBatchRequired"));
    }
    if let Some(record) = read_record(mvcc, context)? {
        validate_record_context(&record, context, target)?;
        return Err(Status::already_exists("NativeIdempotencyRecordExists"));
    }

    let response_json = serde_json::to_value(response).map_err(|error| {
        Status::internal(format!("Serialize native idempotency response: {error}"))
    })?;
    let response_hash = native_response_hash(&response_json)?;
    let mut record = NativeIdempotencyRecord {
        format_version: 3,
        tenant_id: context.tenant_id,
        bucket_id: context.bucket_id,
        principal: context.principal.clone(),
        idempotency_key: context.idempotency_key.clone(),
        transaction_id: None,
        request_id: context.request_id.clone(),
        target: target.clone(),
        response_json,
        response_hash,
        record_hash: String::new(),
    };
    record.record_hash = record_hash(&record)?;
    let payload = encode_record(&record)?;
    let logical_key = record_logical_key(context)?;
    Ok(crate::mvcc_product::ProductMutationPlan {
        mutations: vec![ProductMutation::put(logical_key.clone(), payload)],
        predicates: vec![(logical_key, PredicateKind::Absent)],
        outbox_events: Vec::new(),
    })
}

fn read_record(
    mvcc: &MvccSubsystem,
    context: &NativeMutationContext,
) -> Result<Option<NativeIdempotencyRecord>, Status> {
    let logical_key = record_logical_key(context)?;
    let bytes = if let Some(transaction_id) = context.transaction_id.as_deref() {
        mvcc.read_transaction_value(
            transaction_id,
            &native_transaction_principal_from_context(context),
            &logical_key,
        )
        .map_err(|error| Status::internal(error.to_string()))?
    } else {
        mvcc.read_latest_value(&logical_key)
            .map_err(|error| Status::internal(error.to_string()))?
    };
    bytes
        .map(|bytes| decode_committed_record(&bytes))
        .transpose()
}

fn record_logical_key(
    context: &NativeMutationContext,
) -> Result<crate::mvcc_transaction::LogicalKey, Status> {
    crate::mvcc_product::coremeta_logical_key(
        CF_TRANSACTIONS,
        TABLE_NATIVE_IDEMPOTENCY_ROW,
        &record_tuple_key(context)?,
    )
    .map_err(|error| Status::internal(error.to_string()))
}

fn validate_record_context(
    record: &NativeIdempotencyRecord,
    context: &NativeMutationContext,
    target: &NativeIdempotencyTarget,
) -> Result<(), Status> {
    if record.tenant_id != context.tenant_id
        || record.bucket_id != context.bucket_id
        || record.principal != context.principal
        || record.idempotency_key != context.idempotency_key
        || record.transaction_id != context.transaction_id
    {
        return Err(Status::permission_denied(
            "Native idempotency record context mismatch",
        ));
    }
    if &record.target != target {
        return Err(Status::failed_precondition(
            "Native idempotency key already used for a different mutation target",
        ));
    }
    Ok(())
}

fn record_key_hash(context: &NativeMutationContext) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&context.tenant_id.to_le_bytes());
    hasher.update(&context.bucket_id.to_le_bytes());
    hasher.update(context.principal.as_bytes());
    hasher.update(&[0]);
    hasher.update(context.idempotency_key.as_bytes());
    if let Some(transaction_id) = context.transaction_id.as_deref() {
        hasher.update(&[0]);
        hasher.update(transaction_id.as_bytes());
    }
    format!("blake3:{}", hasher.finalize().to_hex())
}

fn record_tuple_key(context: &NativeMutationContext) -> Result<Vec<u8>, Status> {
    let hash = record_key_hash(context);
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("native_idempotency"),
        CoreMetaTuplePart::I64(context.tenant_id),
        CoreMetaTuplePart::I64(context.bucket_id),
        CoreMetaTuplePart::Hash(&hash),
    ])
    .map_err(|e| Status::internal(e.to_string()))
}

pub(crate) fn native_transaction_principal_from_context(context: &NativeMutationContext) -> String {
    format!(
        "tenant/{}/principal/{}",
        context.tenant_id, context.principal
    )
}

fn current_unix_ms() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default()
}

fn encode_record(record: &NativeIdempotencyRecord) -> Result<Vec<u8>, Status> {
    let proto = record_to_proto(record)?;
    let mut bytes = Vec::new();
    proto
        .encode(&mut bytes)
        .map_err(|e| Status::internal(format!("Encode native idempotency record: {e}")))?;
    Ok(bytes)
}

fn decode_committed_record(bytes: &[u8]) -> Result<NativeIdempotencyRecord, Status> {
    let proto = NativeIdempotencyRecordBodyProto::decode(bytes)
        .map_err(|e| Status::internal(format!("Invalid native idempotency record: {e}")))?;
    let record = record_from_proto(proto)?;
    if record.format_version != 3 {
        return Err(Status::data_loss(
            "Native idempotency format version is unsupported",
        ));
    }
    if record.response_hash != native_response_hash(&record.response_json)? {
        return Err(Status::data_loss(
            "Native idempotency response hash mismatch",
        ));
    }
    if record.record_hash != record_hash(&record)? {
        return Err(Status::data_loss("Native idempotency record hash mismatch"));
    }
    Ok(record)
}

fn record_to_proto(
    record: &NativeIdempotencyRecord,
) -> Result<NativeIdempotencyRecordBodyProto, Status> {
    Ok(NativeIdempotencyRecordBodyProto {
        format_version: u32::from(record.format_version),
        tenant_id: record.tenant_id,
        bucket_id: record.bucket_id,
        principal: record.principal.clone(),
        idempotency_key: record.idempotency_key.clone(),
        transaction_id: record.transaction_id.clone(),
        request_id: record.request_id.clone(),
        target: Some(target_to_proto(&record.target)?),
        response_json: json_to_vec(&record.response_json, "native idempotency response")?,
        response_hash: record.response_hash.clone(),
        record_hash: record.record_hash.clone(),
    })
}

fn record_from_proto(
    proto: NativeIdempotencyRecordBodyProto,
) -> Result<NativeIdempotencyRecord, Status> {
    Ok(NativeIdempotencyRecord {
        format_version: proto
            .format_version
            .try_into()
            .map_err(|_| Status::internal("Native idempotency format version exceeds u16"))?,
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        principal: proto.principal,
        idempotency_key: proto.idempotency_key,
        transaction_id: proto.transaction_id,
        request_id: proto.request_id,
        target: target_from_proto(
            proto
                .target
                .ok_or_else(|| Status::internal("Native idempotency target missing"))?,
        )?,
        response_json: vec_to_json(&proto.response_json, "native idempotency response")?,
        response_hash: proto.response_hash,
        record_hash: proto.record_hash,
    })
}

fn target_to_proto(
    target: &NativeIdempotencyTarget,
) -> Result<NativeIdempotencyTargetProto, Status> {
    Ok(NativeIdempotencyTargetProto {
        operation: target.operation.clone(),
        bucket_name: target.bucket_name.clone(),
        object_key: target.object_key.clone(),
        parameters_json: json_to_vec(&target.parameters, "native idempotency target parameters")?,
    })
}

fn target_from_proto(
    proto: NativeIdempotencyTargetProto,
) -> Result<NativeIdempotencyTarget, Status> {
    Ok(NativeIdempotencyTarget {
        operation: proto.operation,
        bucket_name: proto.bucket_name,
        object_key: proto.object_key,
        parameters: vec_to_json(
            &proto.parameters_json,
            "native idempotency target parameters",
        )?,
    })
}

fn json_to_vec(value: &JsonValue, label: &str) -> Result<Vec<u8>, Status> {
    serde_json::to_vec(value).map_err(|e| Status::internal(format!("Serialize {label}: {e}")))
}

fn vec_to_json(bytes: &[u8], label: &str) -> Result<JsonValue, Status> {
    serde_json::from_slice(bytes).map_err(|e| Status::internal(format!("Invalid {label}: {e}")))
}

fn native_response_hash(response: &JsonValue) -> Result<String, Status> {
    let bytes = serde_json::to_vec(response).map_err(|e| {
        Status::internal(format!("Serialize native idempotency response hash: {e}"))
    })?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

fn record_hash(record: &NativeIdempotencyRecord) -> Result<String, Status> {
    let mut input = record_to_proto(record)?;
    input.record_hash.clear();
    let mut bytes = Vec::new();
    input
        .encode(&mut bytes)
        .map_err(|e| Status::internal(format!("Hash native idempotency record: {e}")))?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{core_store::CoreMetaStore, storage::Storage};
    use serde_json::json;

    fn context() -> NativeMutationContext {
        NativeMutationContext {
            tenant_id: 7,
            bucket_id: 42,
            principal: "user:alice".to_string(),
            request_id: "req-1".to_string(),
            precondition: String::new(),
            authz_zookie_optional: String::new(),
            idempotency_key: "idem-1".to_string(),
            transaction_id: None,
            write_visibility: None,
        }
    }

    #[test]
    fn native_idempotency_record_is_a_self_validating_mvcc_value() {
        let context = context();
        let response = json!({"version_id": "v1", "committed": true});
        let mut record = NativeIdempotencyRecord {
            format_version: 3,
            tenant_id: context.tenant_id,
            bucket_id: context.bucket_id,
            principal: context.principal,
            idempotency_key: context.idempotency_key,
            transaction_id: context.transaction_id,
            request_id: context.request_id,
            target: NativeIdempotencyTarget::new("PutObject", "docs", "a.txt"),
            response_hash: native_response_hash(&response).unwrap(),
            response_json: response,
            record_hash: String::new(),
        };
        record.record_hash = record_hash(&record).unwrap();
        let encoded = encode_record(&record).unwrap();
        assert_eq!(
            decode_committed_record(&encoded).unwrap().record_hash,
            record.record_hash
        );

        let mut tampered = NativeIdempotencyRecordBodyProto::decode(encoded.as_slice()).unwrap();
        tampered.response_json = serde_json::to_vec(&json!({"version_id": "other"})).unwrap();
        let mut tampered_bytes = Vec::new();
        tampered.encode(&mut tampered_bytes).unwrap();
        assert!(decode_committed_record(&tampered_bytes).is_err());
    }

    #[tokio::test]
    async fn native_idempotency_keys_are_scoped_by_transaction_id() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let config = crate::Config {
            node_id: "node-a".into(),
            public_api_addr: "127.0.0.1:50051".into(),
            storage_path: tmp.path().to_string_lossy().into_owned(),
            anvil_secret_encryption_key:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            ..crate::Config::default()
        };
        let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
        let mvcc = MvccSubsystem::bootstrap(&config, meta.database())
            .await
            .unwrap();
        let mut seed = crate::mvcc_transaction::TransactionBundleBuilder::new(
            "default",
            "native-idempotency-seed",
            0,
            "test",
            crate::mvcc_transaction::HierarchicalRangeStampScheme::new(),
        );
        seed.put(
            crate::mvcc_transaction::LogicalKey {
                table_id: u16::MAX,
                application_key: b"seed".to_vec(),
            },
            Vec::new(),
        );
        mvcc.runtime
            .local_store()
            .apply_certified_bundle(1, &seed.build().unwrap())
            .unwrap();
        let principal = native_transaction_principal_from_context(&context());
        let begin = |idempotency_key: &'static str| {
            mvcc.open_transactions.begin(
                &*mvcc.runtime,
                "default",
                principal.clone(),
                idempotency_key,
                std::time::Duration::from_secs(30),
                crate::mvcc_transaction::DurabilityLevel::Local,
                crate::mvcc_transaction::ReadConsistency::LocalSnapshot,
                current_unix_ms(),
            )
        };
        let mut tx_context = context();
        tx_context.transaction_id = Some(begin("native-idem-tx-1").await.unwrap().transaction_id);
        let target = NativeIdempotencyTarget::new("PutObject", "docs", "a.txt");

        store_response(&mvcc, &tx_context, &target, &json!({"state": "staged"}))
            .await
            .unwrap();

        let mut other_tx_context = tx_context.clone();
        other_tx_context.transaction_id =
            Some(begin("native-idem-tx-2").await.unwrap().transaction_id);
        assert!(
            load_response::<serde_json::Value>(&mvcc, &other_tx_context, &target)
                .await
                .unwrap()
                .is_none()
        );

        let replay = load_response::<serde_json::Value>(&mvcc, &tx_context, &target)
            .await
            .unwrap()
            .expect("same transaction idempotency replay");
        assert_eq!(replay, json!({"state": "staged"}));
    }
}
