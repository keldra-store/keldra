use super::*;
use prost::Message;

const GATEWAY_METADATA_PAGE_MAX: usize = 1000;
pub(super) const GATEWAY_METADATA_CANDIDATE_GENERATION: u64 = 1;
pub(super) const GATEWAY_METADATA_CANDIDATE_TRANSACTION_ID: &str = "gateway-metadata-candidate";

#[derive(Clone, PartialEq, Message)]
pub(super) struct GatewayMetadataRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    row_kind: String,
    #[prost(string, tag = "4")]
    row_key: String,
    #[prost(uint64, tag = "5")]
    generation: u64,
    #[prost(bytes, tag = "6")]
    record_payload: Vec<u8>,
    #[prost(string, tag = "7")]
    record_payload_hash: String,
    #[prost(string, tag = "8")]
    updated_at: String,
}

#[derive(Debug, Clone)]
pub(super) struct GatewayStoredRecord<T> {
    pub(super) row_kind: String,
    pub(super) row_key: String,
    pub(super) generation: u64,
    pub(super) payload_hash: String,
    pub(super) updated_at: String,
    pub(super) record: T,
}

#[derive(Debug, Clone)]
pub(super) struct GatewayStoredRecordPage<T> {
    pub(super) records: Vec<GatewayStoredRecord<T>>,
    pub(super) next_tuple_key: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct GatewayStoredHandle {
    pub row_kind: String,
    pub row_key: String,
    pub generation: u64,
    pub payload_hash: String,
    pub updated_at: String,
}

impl<T> GatewayStoredRecord<T> {
    pub(super) fn stored_handle(&self) -> GatewayStoredHandle {
        GatewayStoredHandle {
            row_kind: self.row_kind.clone(),
            row_key: self.row_key.clone(),
            generation: self.generation,
            payload_hash: self.payload_hash.clone(),
            updated_at: self.updated_at.clone(),
        }
    }
}
pub(super) fn gateway_metadata_tuple_key(row_kind: &str, row_key: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("gateway"),
        CoreMetaTuplePart::Utf8(row_kind),
        CoreMetaTuplePart::Utf8(row_key),
    ])
}

pub(super) fn gateway_metadata_tuple_prefix(row_kind: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("gateway"),
        CoreMetaTuplePart::Utf8(row_kind),
    ])
}

pub(super) fn gateway_metadata_root_key_hash(row_kind: &str, row_key: &str) -> String {
    core_meta_root_key_hash(&gateway_metadata_root_anchor_key(row_kind, row_key))
}

pub(super) fn gateway_metadata_root_anchor_key(row_kind: &str, row_key: &str) -> String {
    format!("gateway/{row_kind}/{row_key}")
}

pub(super) fn gateway_realm_id<T: GatewayRecordCodec>(record: &T) -> String {
    let payload = match encode_gateway_record(record) {
        Ok(payload) => payload,
        Err(_) => return "gateway".to_string(),
    };
    gateway_payload_realm_id(&payload)
}

fn gateway_payload_realm_id(payload: &[u8]) -> String {
    let hash = hash32(&payload);
    format!("gateway/{}", hex::encode(hash))
}

pub(super) fn encode_gateway_metadata_row<T: GatewayRecordCodec>(
    row_kind: &str,
    row_key: &str,
    generation: u64,
    record: &T,
) -> Result<Vec<u8>> {
    let record_payload = encode_gateway_record(record)?;
    let row = GatewayMetadataRowProto {
        common: Some(core_meta_committed_row_common(
            gateway_payload_realm_id(&record_payload),
            gateway_metadata_root_key_hash(row_kind, row_key),
            GATEWAY_METADATA_CANDIDATE_GENERATION,
            GATEWAY_METADATA_CANDIDATE_TRANSACTION_ID,
            0,
        )),
        schema: GATEWAY_METADATA_ROW_SCHEMA.to_string(),
        row_kind: row_kind.to_string(),
        row_key: row_key.to_string(),
        generation,
        record_payload_hash: format!("sha256:{}", sha256_hex(&record_payload)),
        record_payload,
        updated_at: now_rfc3339(),
    };
    Ok(encode_deterministic_proto(&row))
}

pub(super) fn decode_gateway_metadata_row<T: GatewayRecordCodec>(
    row_kind: &str,
    row_key: &str,
    bytes: &[u8],
) -> Result<GatewayStoredRecord<T>> {
    let row = decode_deterministic_proto::<GatewayMetadataRowProto>(bytes, "gateway metadata row")?;
    validate_gateway_metadata_row(&row, row_kind, row_key)?;
    let record = decode_gateway_record(&row.record_payload)?;
    Ok(GatewayStoredRecord {
        row_kind: row.row_kind,
        row_key: row.row_key,
        generation: row.generation,
        payload_hash: row.record_payload_hash,
        updated_at: row.updated_at,
        record,
    })
}

fn validate_gateway_metadata_row(
    row: &GatewayMetadataRowProto,
    row_kind: &str,
    row_key: &str,
) -> Result<()> {
    if row.schema != GATEWAY_METADATA_ROW_SCHEMA
        || row.row_kind != row_kind
        || row.row_key != row_key
        || row.generation == 0
    {
        bail!("gateway metadata row scope mismatch");
    }
    let payload_hash = format!("sha256:{}", sha256_hex(&row.record_payload));
    if row.record_payload_hash != payload_hash {
        bail!("gateway metadata row payload hash mismatch");
    }
    let common = row
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("gateway metadata row is missing common metadata"))?;
    let expected_common = core_meta_committed_row_common(
        gateway_payload_realm_id(&row.record_payload),
        gateway_metadata_root_key_hash(row_kind, row_key),
        GATEWAY_METADATA_CANDIDATE_GENERATION,
        GATEWAY_METADATA_CANDIDATE_TRANSACTION_ID,
        0,
    );
    if common.realm_id != expected_common.realm_id
        || common.root_key_hash != expected_common.root_key_hash
        || common.root_generation == 0
        || common.transaction_id.is_empty()
        || common.visibility_state != crate::core_store::CoreMetaVisibilityState::Committed as i32
        || common.payload_schema_version != expected_common.payload_schema_version
    {
        bail!("gateway metadata row has invalid physical common metadata");
    }
    Ok(())
}

pub(super) async fn read_record_row<T: GatewayRecordCodec>(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    row_kind: &str,
    row_key: &str,
) -> Result<Option<GatewayStoredRecord<T>>> {
    read_record_row_at(mvcc, row_kind, row_key, mvcc.runtime.applied_version()?)
}

pub(super) fn read_record_row_at<T: GatewayRecordCodec>(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    row_kind: &str,
    row_key: &str,
    snapshot: u64,
) -> Result<Option<GatewayStoredRecord<T>>> {
    let tuple_key = gateway_metadata_tuple_key(row_kind, row_key)?;
    let logical_key = crate::mvcc_product::coremeta_logical_key(
        CF_REGISTRY,
        TABLE_GATEWAY_METADATA_ROW,
        &tuple_key,
    )?;
    mvcc.runtime
        .read_at(&logical_key, snapshot)?
        .map(|row| decode_gateway_metadata_row(row_kind, row_key, &row.value))
        .transpose()
}

pub(super) async fn put_record_row<T: GatewayRecordCodec>(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    row_kind: &str,
    row_key: &str,
    record: &T,
    require_absent: bool,
    expected_generation: Option<u64>,
) -> Result<GatewayStoredRecord<T>> {
    let tuple_key = gateway_metadata_tuple_key(row_kind, row_key)?;
    let logical_key = crate::mvcc_product::coremeta_logical_key(
        CF_REGISTRY,
        TABLE_GATEWAY_METADATA_ROW,
        &tuple_key,
    )?;
    let current_bytes = mvcc.read_latest_value(&logical_key)?;
    let current = current_bytes
        .map(|bytes| decode_gateway_metadata_row::<T>(row_kind, row_key, &bytes))
        .transpose()?;
    if require_absent && current.is_some() {
        bail!("gateway metadata row {row_kind}/{row_key} already exists");
    }
    if let Some(expected_generation) = expected_generation {
        let actual = current.as_ref().map(|value| value.generation);
        if actual != Some(expected_generation) {
            bail!("gateway metadata row {row_kind}/{row_key} generation mismatch");
        }
    }
    let generation = current
        .as_ref()
        .map(|value| value.generation + 1)
        .unwrap_or(1);
    let payload = encode_gateway_metadata_row(row_kind, row_key, generation, record)?;
    let predicate = current_bytes
        .as_ref()
        .map(|bytes| {
            crate::mvcc_transaction::PredicateKind::ValueHash(*blake3::hash(bytes).as_bytes())
        })
        .unwrap_or(crate::mvcc_transaction::PredicateKind::Absent);
    mvcc.autocommit_product_mutations_with_predicates(
        "gateway-registry",
        &format!("gateway-row:{}", hex::encode(hash32(&payload))),
        vec![crate::mvcc_product::ProductMutation::put(
            logical_key.clone(),
            payload.clone(),
        )],
        vec![(logical_key, predicate)],
        crate::mvcc_transaction::DurabilityLevel::Quorum,
        u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
    )
    .await?;
    decode_gateway_metadata_row(row_kind, row_key, &payload)
}

pub(super) async fn put_record_row_in_transaction<T: GatewayRecordCodec>(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    row_kind: &str,
    row_key: &str,
    record: &T,
    require_absent: bool,
    expected_generation: Option<u64>,
    transaction_id: &str,
    principal: &str,
) -> Result<GatewayStoredRecord<T>> {
    let tuple_key = gateway_metadata_tuple_key(row_kind, row_key)?;
    let logical_key = crate::mvcc_product::coremeta_logical_key(
        CF_REGISTRY,
        TABLE_GATEWAY_METADATA_ROW,
        &tuple_key,
    )?;
    let base_payload = mvcc.read_transaction_value(transaction_id, principal, &logical_key)?;
    let current = base_payload
        .as_deref()
        .map(|bytes| decode_gateway_metadata_row::<T>(row_kind, row_key, bytes))
        .transpose()?;
    if require_absent && current.is_some() {
        bail!("gateway metadata row {row_kind}/{row_key} already exists");
    }
    if let Some(expected_generation) = expected_generation {
        let actual = current.as_ref().map(|value| value.generation);
        if actual != Some(expected_generation) {
            bail!("gateway metadata row {row_kind}/{row_key} generation mismatch");
        }
    }
    let generation = current
        .as_ref()
        .map(|value| value.generation + 1)
        .unwrap_or(1);
    let payload = encode_gateway_metadata_row(row_kind, row_key, generation, record)?;
    let predicate = base_payload
        .as_ref()
        .map(|bytes| {
            crate::mvcc_transaction::PredicateKind::ValueHash(*blake3::hash(bytes).as_bytes())
        })
        .unwrap_or(crate::mvcc_transaction::PredicateKind::Absent);
    mvcc.stage_product_mutations(
        transaction_id,
        principal,
        vec![crate::mvcc_product::ProductMutation::put(
            logical_key.clone(),
            payload.clone(),
        )],
        u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
    )?;
    mvcc.stage_predicate(
        transaction_id,
        principal,
        logical_key,
        predicate,
        u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
    )?;
    decode_gateway_metadata_row(row_kind, row_key, &payload)
}

pub(super) async fn read_record_row_in_transaction<T: GatewayRecordCodec>(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    row_kind: &str,
    row_key: &str,
    transaction_id: &str,
    principal: &str,
) -> Result<Option<GatewayStoredRecord<T>>> {
    let _ = storage;
    let tuple_key = gateway_metadata_tuple_key(row_kind, row_key)?;
    let logical_key = crate::mvcc_product::coremeta_logical_key(
        CF_REGISTRY,
        TABLE_GATEWAY_METADATA_ROW,
        &tuple_key,
    )?;
    let value = mvcc.read_transaction_value(transaction_id, principal, &logical_key)?;
    let predicate = value
        .as_ref()
        .map(|bytes| {
            crate::mvcc_transaction::PredicateKind::ValueHash(*blake3::hash(bytes).as_bytes())
        })
        .unwrap_or(crate::mvcc_transaction::PredicateKind::Absent);
    mvcc.stage_predicate(
        transaction_id,
        principal,
        logical_key,
        predicate,
        u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
    )?;
    value
        .map(|row| decode_gateway_metadata_row::<T>(row_kind, row_key, &row))
        .transpose()
}

pub(super) async fn put_upload_session_start_rows(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    session_key: &str,
    idempotency_key: &str,
    record: &GatewayUploadSessionRecord,
) -> Result<GatewayStoredRecord<GatewayUploadSessionRecord>> {
    let session_tuple_key = gateway_metadata_tuple_key(GATEWAY_ROW_UPLOAD_SESSION, session_key)?;
    let idempotency_tuple_key =
        gateway_metadata_tuple_key(GATEWAY_ROW_UPLOAD_IDEMPOTENCY, idempotency_key)?;
    let session_logical_key = crate::mvcc_product::coremeta_logical_key(
        CF_REGISTRY,
        TABLE_GATEWAY_METADATA_ROW,
        &session_tuple_key,
    )?;
    let idempotency_logical_key = crate::mvcc_product::coremeta_logical_key(
        CF_REGISTRY,
        TABLE_GATEWAY_METADATA_ROW,
        &idempotency_tuple_key,
    )?;
    if mvcc.read_latest_value(&session_logical_key)?.is_some()
        || mvcc.read_latest_value(&idempotency_logical_key)?.is_some()
    {
        bail!("gateway upload session metadata row already exists");
    }
    let session_payload =
        encode_gateway_metadata_row(GATEWAY_ROW_UPLOAD_SESSION, session_key, 1, record)?;
    let idempotency_payload =
        encode_gateway_metadata_row(GATEWAY_ROW_UPLOAD_IDEMPOTENCY, idempotency_key, 1, record)?;
    mvcc.autocommit_product_mutations_with_predicates(
        "gateway-upload",
        &format!("gateway-upload-session:{session_key}"),
        vec![
            crate::mvcc_product::ProductMutation::put(
                session_logical_key.clone(),
                session_payload.clone(),
            ),
            crate::mvcc_product::ProductMutation::put(
                idempotency_logical_key.clone(),
                idempotency_payload,
            ),
        ],
        vec![
            (
                session_logical_key,
                crate::mvcc_transaction::PredicateKind::Absent,
            ),
            (
                idempotency_logical_key,
                crate::mvcc_transaction::PredicateKind::Absent,
            ),
        ],
        crate::mvcc_transaction::DurabilityLevel::Quorum,
        u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
    )
    .await?;
    decode_gateway_metadata_row(GATEWAY_ROW_UPLOAD_SESSION, session_key, &session_payload)
}

pub(super) async fn list_record_rows<T: GatewayRecordCodec>(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    row_kind: &str,
    snapshot: u64,
    after_application_key: Option<&[u8]>,
    page_size: usize,
) -> Result<GatewayStoredRecordPage<T>> {
    if !(1..=GATEWAY_METADATA_PAGE_MAX).contains(&page_size) {
        bail!("gateway metadata page size must be between 1 and {GATEWAY_METADATA_PAGE_MAX}");
    }
    let application_prefix = crate::mvcc_product::coremeta_application_prefix(
        CF_REGISTRY,
        &gateway_metadata_tuple_prefix(row_kind)?,
    )?;
    let mut rows = mvcc.runtime.scan_table_prefix_at(
        TABLE_GATEWAY_METADATA_ROW,
        &application_prefix,
        snapshot,
    )?;
    if let Some(after) = after_application_key {
        rows.retain(|(key, _)| key.application_key.as_slice() > after);
    }
    let has_more = rows.len() > page_size;
    if has_more {
        rows.truncate(page_size);
    }
    let next_tuple_key = if has_more {
        Some(
            rows.last()
                .ok_or_else(|| anyhow!("gateway metadata page continuation has no row"))?
                .0
                .application_key
                .clone(),
        )
    } else {
        None
    };
    let mut records = Vec::with_capacity(rows.len());
    for (key, row) in rows {
        let proto = decode_deterministic_proto::<GatewayMetadataRowProto>(
            &row.value,
            "gateway metadata row",
        )?;
        if key
            != crate::mvcc_product::coremeta_logical_key(
                CF_REGISTRY,
                TABLE_GATEWAY_METADATA_ROW,
                &gateway_metadata_tuple_key(row_kind, &proto.row_key)?,
            )?
        {
            bail!("gateway metadata physical row key mismatch");
        }
        records.push(decode_gateway_metadata_row(
            row_kind,
            &proto.row_key,
            &row.value,
        )?);
    }
    Ok(GatewayStoredRecordPage {
        records,
        next_tuple_key,
    })
}
