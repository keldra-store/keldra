use crate::core_store::{
    CF_OBJECT_HEADS, CF_STREAM_HEADS, CF_STREAM_RECORDS, CoreMetaTuplePart,
    TABLE_MANIFEST_CAS_CURRENT_ROW, TABLE_STREAM_HEAD_ROW, TABLE_STREAM_RECORD_INDEX_ROW,
    core_meta_committed_row_common, core_meta_root_key_hash, core_meta_tuple_key,
};
use crate::formats::{Hash32, hash32};
use crate::partition_fence::PartitionWritePermit;
use crate::persistence::{ManifestCasResult, MetadataMutationReceipt};
use crate::storage::Storage;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

const MANIFEST_CAS_BODY_SCHEMA: &str = "anvil.core.manifest_cas.body.v1";
const MANIFEST_CAS_CURRENT_ROW_SCHEMA: &str = "anvil.core.manifest_cas.current_row.v1";
const MANIFEST_CAS_CURRENT_ROW_KEY_PREFIX: &str = "manifest_cas_current";
const MANIFEST_CAS_CURRENT_ROW_MAX_PROTO_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManifestBody {
    tenant_id: i64,
    bucket_id: i64,
    object_key: String,
    revision: i64,
    manifest_hash: String,
    manifest: JsonValue,
    updated_at: DateTime<Utc>,
}

#[derive(Clone, PartialEq, Message)]
struct ManifestBodyProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(int64, tag = "3")]
    bucket_id: i64,
    #[prost(string, tag = "4")]
    object_key: String,
    #[prost(int64, tag = "5")]
    revision: i64,
    #[prost(string, tag = "6")]
    manifest_hash: String,
    #[prost(bytes = "vec", tag = "7")]
    manifest_json: Vec<u8>,
    #[prost(string, tag = "8")]
    updated_at: String,
    #[prost(uint64, tag = "9")]
    fence_token: u64,
    #[prost(string, tag = "10")]
    mutation_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct ManifestCurrentRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(int64, tag = "4")]
    bucket_id: i64,
    #[prost(string, tag = "5")]
    object_key: String,
    #[prost(int64, tag = "6")]
    revision: i64,
    #[prost(string, tag = "7")]
    manifest_hash: String,
    #[prost(string, tag = "8")]
    updated_at: String,
}

#[derive(Debug, Clone)]
struct ManifestCurrentRow {
    tenant_id: i64,
    bucket_id: i64,
    object_key: String,
    revision: i64,
    root_generation: u64,
    manifest_hash: String,
    updated_at: DateTime<Utc>,
    transaction_id: String,
    created_at_unix_nanos: u64,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn compare_and_swap_manifest_with_permit(
    _storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
    expected_revision: i64,
    manifest: JsonValue,
    manifest_hash: &str,
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
) -> Result<Option<ManifestCasResult>> {
    require_manifest_cas_permit(tenant_id, bucket_id, permit)?;
    compare_and_swap_manifest_inner(
        mvcc,
        tenant_id,
        bucket_id,
        object_key,
        expected_revision,
        manifest,
        manifest_hash,
        permit.fence_token,
        None,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn compare_and_swap_manifest_with_permit_in_transaction(
    _storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
    expected_revision: i64,
    manifest: JsonValue,
    manifest_hash: &str,
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<Option<ManifestCasResult>> {
    require_manifest_cas_permit(tenant_id, bucket_id, permit)?;
    compare_and_swap_manifest_inner(
        mvcc,
        tenant_id,
        bucket_id,
        object_key,
        expected_revision,
        manifest,
        manifest_hash,
        permit.fence_token,
        Some(transaction_id),
        Some(transaction_principal),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn compare_and_swap_manifest_inner(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
    expected_revision: i64,
    manifest: JsonValue,
    manifest_hash: &str,
    fence_token: u64,
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<Option<ManifestCasResult>> {
    let current = current_revision(
        mvcc,
        tenant_id,
        bucket_id,
        object_key,
        transaction_id.zip(transaction_principal),
    )
    .await?;
    if expected_revision != current {
        return Ok(None);
    }
    let revision = current
        .checked_add(1)
        .ok_or_else(|| anyhow!("manifest revision overflow"))?;
    let receipt = append_manifest(
        mvcc,
        ManifestBody {
            tenant_id,
            bucket_id,
            object_key: object_key.to_string(),
            revision,
            manifest_hash: manifest_hash.to_string(),
            manifest,
            updated_at: Utc::now(),
        },
        fence_token,
        transaction_id,
        transaction_principal,
    )
    .await?;
    Ok(Some(ManifestCasResult {
        revision,
        manifest_hash: manifest_hash.to_string(),
        receipt,
    }))
}

async fn current_revision(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
    transaction: Option<(&str, &str)>,
) -> Result<i64> {
    let payload = manifest_current_payload(mvcc, tenant_id, bucket_id, object_key, transaction)?;
    Ok(payload
        .map(|payload| decode_manifest_current_row(&payload))
        .transpose()?
        .map(|row| row.revision)
        .unwrap_or(0))
}

async fn append_manifest(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    body: ManifestBody,
    fence_token: u64,
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<MetadataMutationReceipt> {
    let mutation_id = uuid::Uuid::new_v4();
    let staged_transaction = transaction_id.is_some();
    match (transaction_id, transaction_principal) {
        (Some(_), Some(_)) | (None, None) => {}
        _ => {
            return Err(anyhow!(
                "manifest transaction id and principal must be provided together"
            ));
        }
    }
    let transaction_id = transaction_id.map(ToOwned::to_owned).unwrap_or_else(|| {
        format!(
            "manifest-cas:{}:{}:{mutation_id}",
            body.tenant_id, body.bucket_id
        )
    });
    let body_bytes = encode_manifest_body(&body, fence_token, mutation_id)?;
    let payload_hash = hex::encode(hash32(&body_bytes));
    let current_payload = manifest_current_payload(
        mvcc,
        body.tenant_id,
        body.bucket_id,
        &body.object_key,
        staged_transaction
            .then_some(transaction_id.as_str())
            .zip(transaction_principal),
    )?;
    let root_generation = current_payload
        .as_deref()
        .map(decode_manifest_current_row)
        .transpose()?
        .map(|row| {
            row.root_generation
                .checked_add(1)
                .ok_or_else(|| anyhow!("manifest generation overflow"))
        })
        .transpose()?
        .unwrap_or(1);
    let predicate_kind = current_payload
        .as_ref()
        .map(|payload| {
            crate::mvcc_transaction::PredicateKind::ValueHash(*blake3::hash(payload).as_bytes())
        })
        .unwrap_or(crate::mvcc_transaction::PredicateKind::Absent);
    let predicate_key = crate::mvcc_product::coremeta_logical_key(
        CF_OBJECT_HEADS,
        TABLE_MANIFEST_CAS_CURRENT_ROW,
        &manifest_current_row_key(body.tenant_id, body.bucket_id, &body.object_key)?,
    )?;
    let current_update = manifest_current_row_update_from_payload(
        &body,
        root_generation,
        &transaction_id,
        current_payload,
    )?;
    let current_payload = encode_manifest_current_row(&current_update.row)?;
    let head_key = manifest_event_head_key(body.tenant_id, body.bucket_id)?;
    let head_payload = manifest_current_payload_by_key(
        mvcc,
        &head_key,
        staged_transaction
            .then_some(transaction_id.as_str())
            .zip(transaction_principal),
    )?;
    let sequence = decode_manifest_event_head(head_payload.as_deref())?
        .checked_add(1)
        .ok_or_else(|| anyhow!("manifest event cursor overflow"))?;
    let event_key = manifest_event_key(body.tenant_id, body.bucket_id, sequence)?;
    let mut predicates = vec![
        (predicate_key.clone(), predicate_kind),
        (
            event_key.clone(),
            crate::mvcc_transaction::PredicateKind::Absent,
        ),
        (
            head_key.clone(),
            head_payload.as_ref().map_or(
                crate::mvcc_transaction::PredicateKind::Absent,
                |payload| {
                    crate::mvcc_transaction::PredicateKind::ValueHash(
                        *blake3::hash(payload).as_bytes(),
                    )
                },
            ),
        ),
    ];
    let mutations = vec![
        crate::mvcc_product::ProductMutation::put(event_key, body_bytes),
        crate::mvcc_product::ProductMutation::put(head_key, sequence.to_be_bytes().to_vec()),
        crate::mvcc_product::ProductMutation::put(predicate_key, current_payload),
    ];
    let now_unix_ms = u64::try_from(Utc::now().timestamp_millis())
        .map_err(|_| anyhow!("manifest timestamp predates Unix epoch"))?;
    if staged_transaction {
        let principal =
            transaction_principal.ok_or_else(|| anyhow!("transaction principal is required"))?;
        let preexisting = mvcc
            .open_transactions
            .staged_writes(&transaction_id, principal)?
            .into_iter()
            .map(|write| write.key().clone())
            .collect::<std::collections::BTreeSet<_>>();
        let snapshot = mvcc
            .open_transactions
            .handle(&transaction_id)?
            .snapshot_version;
        predicates = predicates
            .into_iter()
            .filter_map(|(key, _)| {
                if preexisting.contains(&key) {
                    return None;
                }
                Some(mvcc.runtime.read_at(&key, snapshot).map(|row| {
                    let predicate =
                        row.map_or(crate::mvcc_transaction::PredicateKind::Absent, |row| {
                            crate::mvcc_transaction::PredicateKind::ValueHash(
                                *blake3::hash(&row.value).as_bytes(),
                            )
                        });
                    (key, predicate)
                }))
            })
            .collect::<Result<Vec<_>>>()?;
        mvcc.stage_product_mutations(&transaction_id, principal, mutations, now_unix_ms)?;
        for (key, predicate) in predicates {
            mvcc.stage_predicate(&transaction_id, principal, key, predicate, now_unix_ms)?;
        }
        return Ok(MetadataMutationReceipt {
            mutation_id,
            payload_hash: payload_hash.clone(),
            record_hash: payload_hash,
            watch_cursor: sequence,
        });
    }
    mvcc.autocommit_product_mutations_with_predicates(
        &manifest_cas_partition_principal(body.tenant_id, body.bucket_id),
        &transaction_id,
        mutations,
        predicates,
        crate::mvcc_transaction::DurabilityLevel::Quorum,
        now_unix_ms,
    )
    .await?;
    Ok(MetadataMutationReceipt {
        mutation_id,
        payload_hash: payload_hash.clone(),
        record_hash: payload_hash,
        watch_cursor: sequence,
    })
}

pub fn manifest_cas_partition_id(tenant_id: i64, bucket_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket/{bucket_id}/manifest_cas").as_bytes())
}

fn manifest_cas_partition_principal(tenant_id: i64, bucket_id: i64) -> String {
    format!("partition-owner:manifest_cas:{tenant_id}:{bucket_id}")
}

fn encode_manifest_body(
    body: &ManifestBody,
    fence_token: u64,
    mutation_id: uuid::Uuid,
) -> Result<Vec<u8>> {
    let proto = ManifestBodyProto {
        schema: MANIFEST_CAS_BODY_SCHEMA.to_string(),
        tenant_id: body.tenant_id,
        bucket_id: body.bucket_id,
        object_key: body.object_key.clone(),
        revision: body.revision,
        manifest_hash: body.manifest_hash.clone(),
        manifest_json: canonical_json_bytes(&body.manifest)?,
        updated_at: body.updated_at.to_rfc3339(),
        fence_token,
        mutation_id: mutation_id.to_string(),
    };
    encode_deterministic_proto(&proto)
}

fn encode_deterministic_proto(message: &impl Message) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes)?;
    Ok(bytes)
}

#[derive(Debug, Clone)]
struct ManifestCurrentRowUpdate {
    row: ManifestCurrentRow,
}

fn manifest_current_row_update_from_payload(
    body: &ManifestBody,
    root_generation: u64,
    transaction_id: &str,
    current_payload: Option<Vec<u8>>,
) -> Result<ManifestCurrentRowUpdate> {
    if root_generation == 0 {
        return Err(anyhow!(
            "manifest current CoreMeta row root generation must be positive"
        ));
    }
    let current = current_payload
        .as_deref()
        .map(decode_manifest_current_row)
        .transpose()?;
    let expected_previous = body.revision.saturating_sub(1);
    if current.as_ref().map(|row| row.revision).unwrap_or(0) != expected_previous {
        return Err(anyhow!("manifest CAS current row revision mismatch"));
    }
    Ok(ManifestCurrentRowUpdate {
        row: ManifestCurrentRow {
            tenant_id: body.tenant_id,
            bucket_id: body.bucket_id,
            object_key: body.object_key.clone(),
            revision: body.revision,
            root_generation,
            manifest_hash: body.manifest_hash.clone(),
            updated_at: body.updated_at,
            transaction_id: transaction_id.to_string(),
            created_at_unix_nanos: current_unix_nanos()?,
        },
    })
}

fn manifest_current_payload(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
    transaction: Option<(&str, &str)>,
) -> Result<Option<Vec<u8>>> {
    let key = manifest_current_row_key(tenant_id, bucket_id, object_key)?;
    if let Some((transaction_id, transaction_principal)) = transaction {
        let logical_key = crate::mvcc_product::coremeta_logical_key(
            CF_OBJECT_HEADS,
            TABLE_MANIFEST_CAS_CURRENT_ROW,
            &key,
        )?;
        return mvcc.read_transaction_value(transaction_id, transaction_principal, &logical_key);
    }
    let logical_key = crate::mvcc_product::coremeta_logical_key(
        CF_OBJECT_HEADS,
        TABLE_MANIFEST_CAS_CURRENT_ROW,
        &key,
    )?;
    let snapshot = mvcc.runtime.applied_version()?;
    Ok(mvcc
        .runtime
        .read_at(&logical_key, snapshot)?
        .map(|row| row.value))
}

fn encode_manifest_current_row(row: &ManifestCurrentRow) -> Result<Vec<u8>> {
    if row.revision < 0 {
        return Err(anyhow!(
            "manifest current CoreMeta row revision is negative"
        ));
    }
    if row.root_generation == 0 {
        return Err(anyhow!(
            "manifest current CoreMeta row root generation must be positive"
        ));
    }
    let proto = ManifestCurrentRowProto {
        schema: MANIFEST_CAS_CURRENT_ROW_SCHEMA.to_string(),
        common: Some(core_meta_committed_row_common(
            manifest_cas_realm_id(row.tenant_id),
            core_meta_root_key_hash(&manifest_cas_current_root_key(row.tenant_id, row.bucket_id)),
            row.root_generation,
            &row.transaction_id,
            row.created_at_unix_nanos,
        )),
        tenant_id: row.tenant_id,
        bucket_id: row.bucket_id,
        object_key: row.object_key.clone(),
        revision: row.revision,
        manifest_hash: row.manifest_hash.clone(),
        updated_at: row.updated_at.to_rfc3339(),
    };
    let bytes = encode_deterministic_proto(&proto)?;
    if bytes.len() > MANIFEST_CAS_CURRENT_ROW_MAX_PROTO_BYTES {
        return Err(anyhow!("manifest current CoreMeta row exceeds size limit"));
    }
    Ok(bytes)
}

fn decode_manifest_current_row(bytes: &[u8]) -> Result<ManifestCurrentRow> {
    if bytes.len() > MANIFEST_CAS_CURRENT_ROW_MAX_PROTO_BYTES {
        return Err(anyhow!("manifest current CoreMeta row exceeds size limit"));
    }
    let proto = ManifestCurrentRowProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "manifest CAS current row")?;
    if proto.schema != MANIFEST_CAS_CURRENT_ROW_SCHEMA {
        return Err(anyhow!("manifest CAS current row schema mismatch"));
    }
    let common = proto
        .common
        .ok_or_else(|| anyhow!("manifest CAS current row missing common metadata"))?;
    if common.realm_id != manifest_cas_realm_id(proto.tenant_id) {
        return Err(anyhow!("manifest CAS current row realm mismatch"));
    }
    if common.root_key_hash
        != core_meta_root_key_hash(&manifest_cas_current_root_key(
            proto.tenant_id,
            proto.bucket_id,
        ))
    {
        return Err(anyhow!("manifest CAS current row root mismatch"));
    }
    if common.visibility_state != crate::core_store::CoreMetaVisibilityState::Committed as i32 {
        return Err(anyhow!("manifest CAS current row is not committed"));
    }
    if common.root_generation == 0 {
        return Err(anyhow!(
            "manifest CAS current row has an invalid root generation"
        ));
    }
    Ok(ManifestCurrentRow {
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        object_key: proto.object_key,
        revision: proto.revision,
        root_generation: common.root_generation,
        manifest_hash: proto.manifest_hash,
        updated_at: DateTime::parse_from_rfc3339(&proto.updated_at)?.with_timezone(&Utc),
        transaction_id: common.transaction_id,
        created_at_unix_nanos: common.created_at_unix_nanos,
    })
}

fn manifest_current_row_key(tenant_id: i64, bucket_id: i64, object_key: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(MANIFEST_CAS_CURRENT_ROW_KEY_PREFIX),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::I64(bucket_id),
        CoreMetaTuplePart::Utf8(object_key),
    ])
}

fn manifest_current_payload_by_key(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    key: &crate::mvcc_transaction::LogicalKey,
    transaction: Option<(&str, &str)>,
) -> Result<Option<Vec<u8>>> {
    if let Some((transaction_id, principal)) = transaction {
        mvcc.read_transaction_value(transaction_id, principal, key)
    } else {
        mvcc.read_latest_value(key)
    }
}

fn manifest_event_head_key(
    tenant_id: i64,
    bucket_id: i64,
) -> Result<crate::mvcc_transaction::LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(
        CF_STREAM_HEADS,
        TABLE_STREAM_HEAD_ROW,
        &core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("manifest-cas-event-head"),
            CoreMetaTuplePart::I64(tenant_id),
            CoreMetaTuplePart::I64(bucket_id),
        ])?,
    )
}

fn manifest_event_key(
    tenant_id: i64,
    bucket_id: i64,
    sequence: u64,
) -> Result<crate::mvcc_transaction::LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(
        CF_STREAM_RECORDS,
        TABLE_STREAM_RECORD_INDEX_ROW,
        &core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("manifest-cas-event"),
            CoreMetaTuplePart::I64(tenant_id),
            CoreMetaTuplePart::I64(bucket_id),
            CoreMetaTuplePart::U64(sequence),
        ])?,
    )
}

fn decode_manifest_event_head(payload: Option<&[u8]>) -> Result<u64> {
    let Some(payload) = payload else {
        return Ok(0);
    };
    Ok(u64::from_be_bytes(payload.try_into().map_err(|_| {
        anyhow!("manifest event head has invalid length")
    })?))
}

fn manifest_cas_realm_id(tenant_id: i64) -> String {
    format!("tenant/{tenant_id}")
}

fn manifest_cas_current_root_key(tenant_id: i64, bucket_id: i64) -> String {
    format!("tenant/{tenant_id}/bucket/{bucket_id}/manifest_cas/current")
}

fn current_unix_nanos() -> Result<u64> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| anyhow!("system clock is before Unix epoch"))?;
    Ok(now
        .as_secs()
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::from(now.subsec_nanos())))
}

fn canonical_json_bytes(value: &JsonValue) -> Result<Vec<u8>> {
    serde_json::to_vec(&canonical_json(value)).map_err(Into::into)
}

fn canonical_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Array(values) => JsonValue::Array(values.iter().map(canonical_json).collect()),
        JsonValue::Object(values) => {
            let mut sorted = serde_json::Map::new();
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                sorted.insert(key.clone(), canonical_json(&values[key]));
            }
            JsonValue::Object(sorted)
        }
        scalar => scalar.clone(),
    }
}

fn ensure_deterministic_proto(message: &impl Message, bytes: &[u8], label: &str) -> Result<()> {
    if encode_deterministic_proto(message)? != bytes {
        return Err(anyhow!("{label} is not deterministically encoded"));
    }
    Ok(())
}

fn require_manifest_cas_permit(
    tenant_id: i64,
    bucket_id: i64,
    permit: &PartitionWritePermit,
) -> Result<()> {
    let expected_partition_id = hex::encode(manifest_cas_partition_id(tenant_id, bucket_id));
    if permit.partition_family != "manifest_cas" || permit.partition_id != expected_partition_id {
        anyhow::bail!("manifest CAS write permit targets a different partition");
    }
    Ok(())
}
