use crate::core_store::{
    CF_OBJECT_HEADS, CoreMetaTuplePart, CoreMutationBatch, CoreMutationOperation,
    CoreMutationPrecondition, CoreMutationRootPublication, CoreStore, CoreTransaction,
    CoreTransactionUpdate, TABLE_MANIFEST_CAS_CURRENT_ROW, core_meta_committed_row_common,
    core_meta_payload_digest, core_meta_root_key_hash, core_meta_tuple_key,
};
use crate::formats::{Hash32, hash32, writer::WriterFamily};
use crate::partition_fence::{PartitionWritePermit, partition_write_precondition};
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

#[cfg(test)]
async fn compare_and_swap_manifest(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
    expected_revision: i64,
    manifest: JsonValue,
    manifest_hash: &str,
) -> Result<Option<ManifestCasResult>> {
    compare_and_swap_manifest_inner(
        storage,
        tenant_id,
        bucket_id,
        object_key,
        expected_revision,
        manifest,
        manifest_hash,
        0,
        None,
        None,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn compare_and_swap_manifest_with_permit(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
    expected_revision: i64,
    manifest: JsonValue,
    manifest_hash: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<Option<ManifestCasResult>> {
    require_manifest_cas_permit(tenant_id, bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    compare_and_swap_manifest_inner(
        storage,
        tenant_id,
        bucket_id,
        object_key,
        expected_revision,
        manifest,
        manifest_hash,
        permit.fence_token,
        Some(partition_precondition),
        None,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn compare_and_swap_manifest_with_permit_in_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
    expected_revision: i64,
    manifest: JsonValue,
    manifest_hash: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<Option<ManifestCasResult>> {
    require_manifest_cas_permit(tenant_id, bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    compare_and_swap_manifest_inner(
        storage,
        tenant_id,
        bucket_id,
        object_key,
        expected_revision,
        manifest,
        manifest_hash,
        permit.fence_token,
        Some(partition_precondition),
        Some(transaction_id),
        Some(transaction_principal),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn compare_and_swap_manifest_inner(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
    expected_revision: i64,
    manifest: JsonValue,
    manifest_hash: &str,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<Option<ManifestCasResult>> {
    let current = current_revision(
        storage,
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
        storage,
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
        partition_precondition,
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
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
    transaction: Option<(&str, &str)>,
) -> Result<i64> {
    let payload = manifest_current_payload_for_optional_transaction(
        storage,
        tenant_id,
        bucket_id,
        object_key,
        transaction,
    )
    .await?;
    Ok(payload
        .map(|payload| decode_manifest_current_row(&payload))
        .transpose()?
        .map(|row| row.revision)
        .unwrap_or(0))
}

async fn append_manifest(
    storage: &Storage,
    body: ManifestBody,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<MetadataMutationReceipt> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = manifest_cas_stream_id(body.tenant_id, body.bucket_id);
    let mutation_id = uuid::Uuid::new_v4();
    let staged_transaction = transaction_id.is_some();
    let explicit_transaction = match (transaction_id, transaction_principal) {
        (Some(transaction_id), Some(transaction_principal)) => Some(
            core_store
                .read_explicit_transaction_for_principal(transaction_id, transaction_principal)
                .await?,
        ),
        (None, None) => None,
        _ => {
            return Err(anyhow!(
                "manifest transaction id and principal must be provided together"
            ));
        }
    };
    let transaction_id = transaction_id.map(ToOwned::to_owned).unwrap_or_else(|| {
        format!(
            "manifest-cas:{}:{}:{mutation_id}",
            body.tenant_id, body.bucket_id
        )
    });
    let body_bytes = encode_manifest_body(&body, fence_token, mutation_id)?;
    let payload_hash = hex::encode(hash32(&body_bytes));
    let partition_id = hex::encode(manifest_cas_partition_id(body.tenant_id, body.bucket_id));
    let data_root = manifest_cas_current_root_key(body.tenant_id, body.bucket_id);
    if explicit_transaction
        .as_ref()
        .is_some_and(|transaction| transaction.root_anchor_key != data_root)
    {
        return Err(anyhow!(
            "manifest transaction targets a different CoreMeta root"
        ));
    }
    let scope_partition = explicit_transaction
        .as_ref()
        .map(|transaction| transaction.scope_partition.clone())
        .unwrap_or_else(|| partition_id.clone());
    let root_generation = match explicit_transaction.as_ref() {
        Some(transaction) => {
            core_store
                .infer_explicit_transaction_commit_root_generation(transaction)
                .await?
        }
        None => next_manifest_cas_root_generation(&core_store, &data_root).await?,
    };
    let root_publications = manifest_root_publications(data_root, scope_partition.clone());
    let current_payload = manifest_current_payload_for_optional_transaction(
        storage,
        body.tenant_id,
        body.bucket_id,
        &body.object_key,
        Some(transaction_id.as_str()).zip(transaction_principal),
    )
    .await?;
    let current_update = manifest_current_row_update_from_payload(
        &body,
        root_generation,
        &transaction_id,
        current_payload,
    )?;
    let current_payload = encode_manifest_current_row(&current_update.row)?;
    let mut preconditions: Vec<_> = partition_precondition.into_iter().collect();
    preconditions.push(current_update.precondition.clone());
    let batch = CoreMutationBatch {
        transaction_id: transaction_id.clone(),
        scope_partition: scope_partition.clone(),
        committed_by_principal: transaction_principal
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| manifest_cas_partition_principal(body.tenant_id, body.bucket_id)),
        root_publications,
        preconditions,
        operations: vec![
            CoreMutationOperation::StreamAppend {
                partition_id: scope_partition.clone(),
                stream_id,
                record_kind: "manifest_cas".to_string(),
                payload: body_bytes,
                idempotency_key: Some(format!(
                    "manifest-cas:{}:{}:{mutation_id}",
                    body.tenant_id, body.bucket_id
                )),
            },
            CoreMutationOperation::CoreMetaPut {
                partition_id: scope_partition,
                cf: CF_OBJECT_HEADS.to_string(),
                table_id: TABLE_MANIFEST_CAS_CURRENT_ROW,
                tuple_key: manifest_current_row_key(
                    current_update.row.tenant_id,
                    current_update.row.bucket_id,
                    &current_update.row.object_key,
                )?,
                payload: current_payload,
            },
        ],
    };
    let batch_receipt = if staged_transaction {
        core_store.stage_explicit_transaction_batch(batch).await?
    } else {
        core_store.commit_mutation_batch(batch).await?
    };
    let stream_update = batch_receipt
        .visible_updates
        .iter()
        .find_map(|update| match update {
            CoreTransactionUpdate::StreamAppend {
                visible_sequence,
                prepared_record_hash,
                ..
            } => Some((*visible_sequence, prepared_record_hash.clone())),
            CoreTransactionUpdate::CoreMetaPut { .. }
            | CoreTransactionUpdate::CoreMetaDelete { .. } => None,
        })
        .ok_or_else(|| anyhow!("manifest CAS batch did not append a stream record"))?;
    let receipt = MetadataMutationReceipt {
        mutation_id,
        payload_hash,
        record_hash: stream_update.1,
        watch_cursor: stream_update.0,
    };
    Ok(receipt)
}

#[cfg(test)]
async fn read_manifest_bodies(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Vec<ManifestBody>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = manifest_cas_stream_id(tenant_id, bucket_id);
    let mut after_sequence = 0;
    let mut bodies = Vec::new();
    loop {
        let page = core_store
            .read_stream_page(crate::core_store::ReadStream {
                stream_id: stream_id.clone(),
                after_sequence,
                limit: 256,
            })
            .await?;
        for record in page.records {
            if record.record_kind == "manifest_cas" {
                bodies.push(decode_manifest_body(&record.payload)?);
            }
        }
        if !page.has_more {
            break;
        }
        after_sequence = page.next_sequence;
    }
    Ok(bodies)
}

pub async fn materialize_committed_manifest_cas_transaction(
    storage: &Storage,
    transaction: &CoreTransaction,
) -> Result<usize> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut materialized = 0usize;
    for update in &transaction.visible_updates {
        let CoreTransactionUpdate::StreamAppend {
            stream_id,
            visible_sequence,
            prepared_record_hash,
            ..
        } = update
        else {
            continue;
        };
        let Some((tenant_id, bucket_id)) = parse_manifest_cas_stream_id(stream_id) else {
            continue;
        };
        let records = core_store
            .read_stream(crate::core_store::ReadStream {
                stream_id: stream_id.clone(),
                after_sequence: visible_sequence.saturating_sub(1),
                limit: 1,
            })
            .await?;
        let Some(record) = records.into_iter().find(|record| {
            record.sequence == *visible_sequence && &record.event_hash == prepared_record_hash
        }) else {
            return Err(anyhow!(
                "manifest CAS transaction {} committed stream record {stream_id}:{visible_sequence} is not readable",
                transaction.transaction_id
            ));
        };
        if record.record_kind != "manifest_cas" {
            continue;
        }
        let body = decode_manifest_body(&record.payload)?;
        if body.tenant_id != tenant_id || body.bucket_id != bucket_id {
            return Err(anyhow!(
                "manifest CAS transaction {} stream scope does not match payload",
                transaction.transaction_id
            ));
        }
        let current =
            read_manifest_current_row(&core_store, tenant_id, bucket_id, &body.object_key)?
                .ok_or_else(|| {
                    anyhow!(
                        "manifest CAS transaction {} committed without its current projection",
                        transaction.transaction_id
                    )
                })?;
        if current.revision != body.revision || current.manifest_hash != body.manifest_hash {
            return Err(anyhow!(
                "manifest CAS transaction {} current projection does not match committed stream record",
                transaction.transaction_id
            ));
        }
        materialized += 1;
    }
    Ok(materialized)
}

pub fn manifest_cas_partition_id(tenant_id: i64, bucket_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket/{bucket_id}/manifest_cas").as_bytes())
}

fn manifest_cas_stream_id(tenant_id: i64, bucket_id: i64) -> String {
    format!("manifest_cas:tenant:{tenant_id}:bucket:{bucket_id}")
}

fn parse_manifest_cas_stream_id(stream_id: &str) -> Option<(i64, i64)> {
    let rest = stream_id.strip_prefix("manifest_cas:tenant:")?;
    let (tenant, bucket_part) = rest.split_once(":bucket:")?;
    Some((tenant.parse().ok()?, bucket_part.parse().ok()?))
}

fn manifest_cas_partition_principal(tenant_id: i64, bucket_id: i64) -> String {
    format!("partition-owner:manifest_cas:{tenant_id}:{bucket_id}")
}

#[cfg(test)]
pub(crate) async fn read_manifest_frame_fences_for_test(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Vec<u64>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = manifest_cas_stream_id(tenant_id, bucket_id);
    let mut after_sequence = 0;
    let mut fences = Vec::new();
    loop {
        let page = core_store
            .read_stream_page(crate::core_store::ReadStream {
                stream_id: stream_id.clone(),
                after_sequence,
                limit: 256,
            })
            .await?;
        for record in page.records {
            if record.record_kind == "manifest_cas" {
                fences.push(decode_manifest_body_fence(&record.payload)?);
            }
        }
        if !page.has_more {
            break;
        }
        after_sequence = page.next_sequence;
    }
    Ok(fences)
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

fn decode_manifest_body(bytes: &[u8]) -> Result<ManifestBody> {
    let proto = ManifestBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "manifest CAS body")?;
    if proto.schema != MANIFEST_CAS_BODY_SCHEMA {
        return Err(anyhow!("manifest CAS body schema mismatch"));
    }
    Ok(ManifestBody {
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        object_key: proto.object_key,
        revision: proto.revision,
        manifest_hash: proto.manifest_hash,
        manifest: decode_canonical_json(&proto.manifest_json, "manifest CAS body manifest")?,
        updated_at: DateTime::parse_from_rfc3339(&proto.updated_at)?.with_timezone(&Utc),
    })
}

#[cfg(test)]
fn decode_manifest_body_fence(bytes: &[u8]) -> Result<u64> {
    let proto = ManifestBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "manifest CAS body")?;
    if proto.schema != MANIFEST_CAS_BODY_SCHEMA {
        return Err(anyhow!("manifest CAS body schema mismatch"));
    }
    Ok(proto.fence_token)
}

fn encode_deterministic_proto(message: &impl Message) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes)?;
    Ok(bytes)
}

#[derive(Debug, Clone)]
struct ManifestCurrentRowUpdate {
    precondition: CoreMutationPrecondition,
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
    let key = manifest_current_row_key(body.tenant_id, body.bucket_id, &body.object_key)?;
    Ok(ManifestCurrentRowUpdate {
        precondition: CoreMutationPrecondition::CoreMetaRow {
            cf: CF_OBJECT_HEADS.to_string(),
            table_id: TABLE_MANIFEST_CAS_CURRENT_ROW,
            tuple_key: key,
            expected_payload_hash: current_payload
                .as_ref()
                .map(|payload| core_meta_payload_digest(TABLE_MANIFEST_CAS_CURRENT_ROW, payload)),
            require_absent: current_payload.is_none(),
            require_present: current_payload.is_some(),
        },
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

async fn manifest_current_payload_for_optional_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
    transaction: Option<(&str, &str)>,
) -> Result<Option<Vec<u8>>> {
    let key = manifest_current_row_key(tenant_id, bucket_id, object_key)?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut current =
        core_store.read_coremeta_row(CF_OBJECT_HEADS, TABLE_MANIFEST_CAS_CURRENT_ROW, &key)?;
    let Some((transaction_id, transaction_principal)) = transaction else {
        return Ok(current);
    };
    let transaction = core_store
        .read_explicit_transaction_for_principal(transaction_id, transaction_principal)
        .await?;
    for update in transaction.visible_updates {
        match update {
            CoreTransactionUpdate::CoreMetaPut {
                cf,
                table_id,
                tuple_key,
                payload,
                ..
            } if cf == CF_OBJECT_HEADS
                && table_id == TABLE_MANIFEST_CAS_CURRENT_ROW
                && tuple_key == key =>
            {
                current = Some(payload)
            }
            CoreTransactionUpdate::CoreMetaDelete {
                cf,
                table_id,
                tuple_key,
                ..
            } if cf == CF_OBJECT_HEADS
                && table_id == TABLE_MANIFEST_CAS_CURRENT_ROW
                && tuple_key == key =>
            {
                current = None
            }
            _ => {}
        }
    }
    Ok(current)
}

fn read_manifest_current_payload(
    store: &CoreStore,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
) -> Result<Option<Vec<u8>>> {
    store.read_coremeta_row(
        CF_OBJECT_HEADS,
        TABLE_MANIFEST_CAS_CURRENT_ROW,
        &manifest_current_row_key(tenant_id, bucket_id, object_key)?,
    )
}

fn read_manifest_current_row(
    store: &CoreStore,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
) -> Result<Option<ManifestCurrentRow>> {
    let Some(payload) = read_manifest_current_payload(store, tenant_id, bucket_id, object_key)?
    else {
        return Ok(None);
    };
    let row = decode_manifest_current_row(&payload)?;
    if row.tenant_id != tenant_id || row.bucket_id != bucket_id || row.object_key != object_key {
        return Err(anyhow!("manifest CAS current CoreMeta row scope mismatch"));
    }
    Ok(Some(row))
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

fn manifest_cas_realm_id(tenant_id: i64) -> String {
    format!("tenant/{tenant_id}")
}

fn manifest_cas_current_root_key(tenant_id: i64, bucket_id: i64) -> String {
    format!("tenant/{tenant_id}/bucket/{bucket_id}/manifest_cas/current")
}

async fn next_manifest_cas_root_generation(
    core_store: &CoreStore,
    root_anchor_key: &str,
) -> Result<u64> {
    let current = match core_store
        .read_internal_root_anchor(root_anchor_key, 0)
        .await
    {
        Ok(anchor) => anchor.generation,
        Err(error) if manifest_cas_root_anchor_is_missing(&error) => 0,
        Err(error) => return Err(error),
    };
    current
        .checked_add(1)
        .ok_or_else(|| anyhow!("manifest CAS CoreMeta root generation overflow"))
}

fn manifest_cas_root_anchor_is_missing(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .to_string()
            .contains("CoreStore root anchor not found")
    })
}

fn manifest_root_publications(
    data_root: String,
    coordinator_root: String,
) -> Vec<CoreMutationRootPublication> {
    if data_root == coordinator_root {
        return vec![CoreMutationRootPublication {
            root_anchor_key: data_root,
            writer_families: vec![
                WriterFamily::CoreControl.as_str().to_string(),
                WriterFamily::ObjectBlob.as_str().to_string(),
            ],
            transaction_coordinator: true,
        }];
    }

    vec![
        CoreMutationRootPublication::new(coordinator_root, WriterFamily::CoreControl.as_str())
            .coordinator(),
        CoreMutationRootPublication::new(data_root, WriterFamily::ObjectBlob.as_str()),
    ]
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

fn decode_canonical_json(bytes: &[u8], label: &str) -> Result<JsonValue> {
    let value: JsonValue = serde_json::from_slice(bytes)?;
    if canonical_json_bytes(&value)? != bytes {
        return Err(anyhow!("{label} is not canonical JSON"));
    }
    Ok(value)
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    const KEY: &[u8] = b"manifest journal partition owner key";

    #[tokio::test]
    async fn manifest_journal_enforces_compare_and_swap() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        assert!(
            compare_and_swap_manifest(&storage, 1, 2, "manifest.json", 1, json!({}), "bad")
                .await
                .unwrap()
                .is_none()
        );
        let first =
            compare_and_swap_manifest(&storage, 1, 2, "manifest.json", 0, json!({"a":1}), "hash-a")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(first.revision, 1);
        let second =
            compare_and_swap_manifest(&storage, 1, 2, "manifest.json", 1, json!({"a":2}), "hash-b")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(second.revision, 2);
    }

    #[tokio::test]
    async fn manifest_journal_advances_one_bucket_root_generation_per_mutation() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();

        let first_object = compare_and_swap_manifest(
            &storage,
            1,
            2,
            "first.json",
            0,
            json!({"revision": 1}),
            "first-1",
        )
        .await
        .unwrap()
        .unwrap();
        let second_object = compare_and_swap_manifest(
            &storage,
            1,
            2,
            "second.json",
            0,
            json!({"revision": 1}),
            "second-1",
        )
        .await
        .unwrap()
        .unwrap();
        let first_object_again = compare_and_swap_manifest(
            &storage,
            1,
            2,
            "first.json",
            1,
            json!({"revision": 2}),
            "first-2",
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(first_object.revision, 1);
        assert_eq!(second_object.revision, 1);
        assert_eq!(first_object_again.revision, 2);

        let store = CoreStore::new(storage.clone()).await.unwrap();
        let first = read_manifest_current_row(&store, 1, 2, "first.json")
            .unwrap()
            .unwrap();
        let second = read_manifest_current_row(&store, 1, 2, "second.json")
            .unwrap()
            .unwrap();
        assert_eq!(first.revision, 2);
        assert_eq!(first.root_generation, 3);
        assert_eq!(second.revision, 1);
        assert_eq!(second.root_generation, 2);

        let root = CoreStore::new(storage.clone())
            .await
            .unwrap()
            .read_internal_root_anchor(&manifest_cas_current_root_key(1, 2), 0)
            .await
            .unwrap();
        assert_eq!(root.generation, 3);
    }

    #[test]
    fn manifest_current_row_keeps_domain_revision_separate_from_root_generation() {
        let row = ManifestCurrentRow {
            tenant_id: 1,
            bucket_id: 2,
            object_key: "manifest.json".to_string(),
            revision: 41,
            root_generation: 7,
            manifest_hash: "manifest-hash".to_string(),
            updated_at: Utc::now(),
            transaction_id: "manifest-transaction".to_string(),
            created_at_unix_nanos: 1,
        };

        let decoded =
            decode_manifest_current_row(&encode_manifest_current_row(&row).unwrap()).unwrap();
        assert_eq!(decoded.revision, 41);
        assert_eq!(decoded.root_generation, 7);
    }

    #[test]
    fn manifest_transaction_scope_keeps_one_canonical_root_publication() {
        let root = manifest_cas_current_root_key(1, 2);
        let publications = manifest_root_publications(root.clone(), root.clone());

        assert_eq!(publications.len(), 1);
        assert_eq!(publications[0].root_anchor_key, root);
        assert_eq!(
            publications[0].writer_families,
            vec![
                WriterFamily::CoreControl.as_str().to_string(),
                WriterFamily::ObjectBlob.as_str().to_string(),
            ]
        );
        assert!(publications[0].transaction_coordinator);
    }

    #[tokio::test]
    pub(crate) async fn manifest_cas_with_permit_writes_fenced_protobuf_record_and_current_row() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, 1, 2, "node-a").await;
        let permit = owner.write_permit().unwrap();

        let result = compare_and_swap_manifest_with_permit(
            &storage,
            1,
            2,
            "manifest.json",
            0,
            json!({"a":1}),
            "hash-a",
            &permit,
            KEY,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result.revision, 1);

        let bodies = read_manifest_bodies(&storage, 1, 2).await.unwrap();
        assert_eq!(bodies.len(), 1);
        assert_eq!(bodies[0].revision, 1);
        assert_eq!(bodies[0].manifest_hash, "hash-a");
        let fences = read_manifest_frame_fences_for_test(&storage, 1, 2)
            .await
            .unwrap();
        assert_eq!(fences, vec![permit.fence_token]);

        let store = CoreStore::new(storage.clone()).await.unwrap();
        let current = read_manifest_current_row(&store, 1, 2, "manifest.json")
            .unwrap()
            .expect("manifest CAS current row");
        assert_eq!(current.revision, 1);
        assert_eq!(current.root_generation, 1);
        assert_eq!(current.manifest_hash, "hash-a");
    }

    #[tokio::test]
    pub(crate) async fn manifest_cas_with_permit_rejects_stale_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, 1, 2, "node-a").await;
        let stale_permit = owner.write_permit().unwrap();
        let newer = ready_owner(&storage, 1, 2, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err = compare_and_swap_manifest_with_permit(
            &storage,
            1,
            2,
            "manifest.json",
            0,
            json!({"a":1}),
            "hash-a",
            &stale_permit,
            KEY,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("write permit owner is not current")
        );
    }

    #[tokio::test]
    pub(crate) async fn manifest_cas_batch_rejects_stale_partition_precondition() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, 1, 2, "node-a").await;
        let stale_permit = owner.write_permit().unwrap();
        let stale_precondition = partition_write_precondition(&storage, &stale_permit, KEY)
            .await
            .unwrap();
        let newer = ready_owner(&storage, 1, 2, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err = compare_and_swap_manifest_inner(
            &storage,
            1,
            2,
            "manifest.json",
            0,
            json!({"a":1}),
            "hash-a",
            stale_permit.fence_token,
            Some(stale_precondition),
            None,
            None,
        )
        .await
        .unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("generation mismatch")
                || message.contains("target mismatch")
                || message.contains("precondition failed"),
            "unexpected stale precondition error: {message}"
        );

        compare_and_swap_manifest_with_permit(
            &storage,
            1,
            2,
            "manifest.json",
            0,
            json!({"a":1}),
            "hash-a",
            &newer.write_permit().unwrap(),
            KEY,
        )
        .await
        .unwrap()
        .unwrap();
    }

    async fn ready_owner(
        storage: &Storage,
        tenant_id: i64,
        bucket_id: i64,
        owner_node_id: &str,
    ) -> crate::partition_fence::PartitionOwnerState {
        let family = "manifest_cas".to_string();
        let id = hex::encode(manifest_cas_partition_id(tenant_id, bucket_id));
        crate::partition_fence::ready_partition_owner_for_test(
            storage,
            family,
            id,
            owner_node_id,
            0,
            hex::encode([0; 32]),
            hex::encode([1; 32]),
            KEY,
        )
        .await
    }
}
