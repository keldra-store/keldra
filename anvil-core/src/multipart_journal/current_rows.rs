use super::{
    MULTIPART_CURRENT_ROW_KEY_PREFIX, MultipartMutationKind, MultipartPartCurrentRow,
    MultipartUploadCurrentRow, current_part_payload, current_upload_payload,
    decode_committed_upload_current_row, decode_part_current_row, decode_upload_current_row,
    encode_part_current_row, encode_upload_current_row, multipart_active_upload_key,
    multipart_part_row_key, multipart_upload_id_head_key, multipart_upload_row_key,
};
use crate::core_store::{
    CF_OBJECT_HEADS, CoreMetaTuplePart, CoreMutationOperation, CoreMutationPrecondition, CoreStore,
    CoreTransaction, CoreTransactionUpdate, TABLE_MULTIPART_PART_CURRENT_ROW,
    TABLE_MULTIPART_UPLOAD_CURRENT_ROW, canonical_coremeta_cf_name, core_meta_payload_digest,
    core_meta_tuple_key,
};
use crate::persistence::{MultipartUpload, MultipartUploadPart};
use anyhow::{Result, anyhow};
use chrono::Utc;

const MULTIPART_ACTIVE_COUNT_SENTINEL_KEY: &str = "__anvil_multipart_active_count__";

#[derive(Debug, Clone)]
pub(super) struct MultipartActiveCountCurrentRow {
    pub(super) tenant_id: i64,
    pub(super) bucket_id: i64,
    pub(super) active_count: u64,
    pub(super) logical_revision: u64,
}

pub(super) fn multipart_active_count_key(bucket_id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(MULTIPART_CURRENT_ROW_KEY_PREFIX),
        CoreMetaTuplePart::Utf8("active_count"),
        CoreMetaTuplePart::I64(bucket_id),
    ])
}

pub(super) fn stage_active_count_update(
    update: &mut MultipartCurrentRowUpdate,
    current_payload: Option<&Vec<u8>>,
    tenant_id: i64,
    bucket_id: i64,
    event: MultipartMutationKind,
) -> Result<()> {
    let current = current_payload
        .map(|payload| decode_active_count_current_row(payload))
        .transpose()?;
    let current_count = current
        .as_ref()
        .map(|row| {
            if row.tenant_id != tenant_id || row.bucket_id != bucket_id {
                return Err(anyhow!("multipart active count scope mismatch"));
            }
            Ok(row.active_count)
        })
        .transpose()?
        .unwrap_or(0);
    let active_count = match event {
        MultipartMutationKind::CreateUpload => current_count.checked_add(1),
        MultipartMutationKind::CompleteUpload | MultipartMutationKind::AbortUpload => {
            current_count.checked_sub(1)
        }
        MultipartMutationKind::UpsertPart => return Ok(()),
    }
    .ok_or_else(|| anyhow!("multipart active upload count overflow or underflow"))?;
    let logical_revision = current
        .as_ref()
        .map(|row| row.logical_revision)
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| anyhow!("multipart active count logical revision overflow"))?;
    update.preconditions.push(coremeta_row_precondition(
        TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
        multipart_active_count_key(bucket_id)?,
        current_payload,
        current_payload.is_none(),
        current_payload.is_some(),
    ));
    update.active_count_row = Some(MultipartActiveCountCurrentRow {
        tenant_id,
        bucket_id,
        active_count,
        logical_revision,
    });
    Ok(())
}

pub(super) fn encode_active_count_current_row(
    row: &MultipartActiveCountCurrentRow,
) -> Result<Vec<u8>> {
    let count = i64::try_from(row.active_count)
        .map_err(|_| anyhow!("multipart active upload count exceeds i64"))?;
    encode_upload_current_row(&MultipartUploadCurrentRow {
        upload: MultipartUpload {
            id: count,
            tenant_id: row.tenant_id,
            bucket_id: row.bucket_id,
            key: MULTIPART_ACTIVE_COUNT_SENTINEL_KEY.to_string(),
            upload_id: uuid::Uuid::nil(),
            created_at: Utc::now(),
            completed_at: None,
            aborted_at: None,
        },
        logical_revision: row.logical_revision,
    })
}

fn decode_active_count_current_row(bytes: &[u8]) -> Result<MultipartActiveCountCurrentRow> {
    decode_active_count_row(decode_upload_current_row(bytes)?)
}

fn decode_active_count_row(
    row: MultipartUploadCurrentRow,
) -> Result<MultipartActiveCountCurrentRow> {
    if row.upload.id < 0
        || row.upload.key != MULTIPART_ACTIVE_COUNT_SENTINEL_KEY
        || !row.upload.upload_id.is_nil()
        || row.upload.completed_at.is_some()
        || row.upload.aborted_at.is_some()
    {
        return Err(anyhow!("multipart active count CoreMeta row is invalid"));
    }
    let active_count = u64::try_from(row.upload.id)
        .map_err(|_| anyhow!("multipart active upload count is negative"))?;
    Ok(MultipartActiveCountCurrentRow {
        tenant_id: row.upload.tenant_id,
        bucket_id: row.upload.bucket_id,
        active_count,
        logical_revision: row.logical_revision,
    })
}

pub(super) fn active_count_value(bytes: &[u8], bucket_id: i64) -> Result<u64> {
    let row = decode_active_count_row(decode_committed_upload_current_row(bytes)?)?;
    if row.bucket_id != bucket_id {
        return Err(anyhow!("multipart active count bucket scope mismatch"));
    }
    Ok(row.active_count)
}

#[derive(Debug, Clone, Default)]
pub(super) struct MultipartCurrentRowUpdate {
    pub(super) preconditions: Vec<CoreMutationPrecondition>,
    pub(super) upload_row: Option<MultipartUploadCurrentRow>,
    pub(super) part_row: Option<MultipartPartCurrentRow>,
    pub(super) active_count_row: Option<MultipartActiveCountCurrentRow>,
}

pub(super) fn multipart_current_row_update(
    store: &CoreStore,
    tenant_id: i64,
    bucket_id: i64,
    event: MultipartMutationKind,
    upload: Option<&MultipartUpload>,
    part: Option<&MultipartUploadPart>,
) -> Result<MultipartCurrentRowUpdate> {
    let mut update = MultipartCurrentRowUpdate::default();
    match event {
        MultipartMutationKind::CreateUpload
        | MultipartMutationKind::CompleteUpload
        | MultipartMutationKind::AbortUpload => {
            let upload = upload.ok_or_else(|| anyhow!("multipart upload event missing upload"))?;
            let (payload, current) =
                current_upload_payload(store, tenant_id, bucket_id, upload.id)?;
            update.preconditions.push(coremeta_row_precondition(
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                multipart_upload_row_key(tenant_id, bucket_id, upload.id)?,
                payload.as_ref(),
                event == MultipartMutationKind::CreateUpload,
                event != MultipartMutationKind::CreateUpload,
            ));
            let id_head_key = multipart_upload_id_head_key(upload.id)?;
            let id_head_payload = store.read_coremeta_row(
                CF_OBJECT_HEADS,
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                &id_head_key,
            )?;
            update.preconditions.push(coremeta_row_precondition(
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                id_head_key,
                id_head_payload.as_ref(),
                event == MultipartMutationKind::CreateUpload,
                event != MultipartMutationKind::CreateUpload,
            ));
            let active_key = multipart_active_upload_key(bucket_id, &upload.key, upload.upload_id)?;
            let active_payload = store.read_coremeta_row(
                CF_OBJECT_HEADS,
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                &active_key,
            )?;
            update.preconditions.push(coremeta_row_precondition(
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                active_key,
                active_payload.as_ref(),
                event == MultipartMutationKind::CreateUpload,
                event != MultipartMutationKind::CreateUpload,
            ));
            let active_count_payload = store.read_coremeta_row(
                CF_OBJECT_HEADS,
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                &multipart_active_count_key(bucket_id)?,
            )?;
            stage_active_count_update(
                &mut update,
                active_count_payload.as_ref(),
                tenant_id,
                bucket_id,
                event,
            )?;
            let logical_revision = current
                .as_ref()
                .map(|row| row.logical_revision)
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| anyhow!("multipart upload logical revision overflow"))?;
            update.upload_row = Some(MultipartUploadCurrentRow {
                upload: upload.clone(),
                logical_revision,
            });
        }
        MultipartMutationKind::UpsertPart => {
            let part = part.ok_or_else(|| anyhow!("multipart part event missing part"))?;
            let upload_payload = store.read_coremeta_row(
                CF_OBJECT_HEADS,
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                &multipart_upload_row_key(tenant_id, bucket_id, part.upload_id)?,
            )?;
            update.preconditions.push(coremeta_row_precondition(
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                multipart_upload_row_key(tenant_id, bucket_id, part.upload_id)?,
                upload_payload.as_ref(),
                false,
                true,
            ));
            let (payload, current) = current_part_payload(
                store,
                tenant_id,
                bucket_id,
                part.upload_id,
                part.part_number,
            )?;
            update.preconditions.push(coremeta_row_precondition(
                TABLE_MULTIPART_PART_CURRENT_ROW,
                multipart_part_row_key(tenant_id, bucket_id, part.upload_id, part.part_number)?,
                payload.as_ref(),
                payload.is_none(),
                payload.is_some(),
            ));
            update.part_row = Some(MultipartPartCurrentRow {
                tenant_id,
                bucket_id,
                part: part.clone(),
                logical_revision: current
                    .as_ref()
                    .map(|row| row.logical_revision)
                    .unwrap_or(0)
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("multipart part logical revision overflow"))?,
            });
        }
    }
    Ok(update)
}

pub(super) fn multipart_current_row_update_with_transaction(
    store: &CoreStore,
    transaction: &CoreTransaction,
    tenant_id: i64,
    bucket_id: i64,
    event: MultipartMutationKind,
    upload: Option<&MultipartUpload>,
    part: Option<&MultipartUploadPart>,
) -> Result<MultipartCurrentRowUpdate> {
    let mut update = MultipartCurrentRowUpdate::default();
    match event {
        MultipartMutationKind::CreateUpload
        | MultipartMutationKind::CompleteUpload
        | MultipartMutationKind::AbortUpload => {
            let upload = upload.ok_or_else(|| anyhow!("multipart upload event missing upload"))?;
            let key = multipart_upload_row_key(tenant_id, bucket_id, upload.id)?;
            let payload = coremeta_payload_visible_to_transaction(
                store,
                transaction,
                CF_OBJECT_HEADS,
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                &key,
            )?;
            let current = payload
                .as_deref()
                .map(decode_upload_current_row)
                .transpose()?;
            update.preconditions.push(coremeta_row_precondition(
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                key,
                payload.as_ref(),
                event == MultipartMutationKind::CreateUpload,
                event != MultipartMutationKind::CreateUpload,
            ));
            let id_head_key = multipart_upload_id_head_key(upload.id)?;
            let id_head_payload = coremeta_payload_visible_to_transaction(
                store,
                transaction,
                CF_OBJECT_HEADS,
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                &id_head_key,
            )?;
            update.preconditions.push(coremeta_row_precondition(
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                id_head_key,
                id_head_payload.as_ref(),
                event == MultipartMutationKind::CreateUpload,
                event != MultipartMutationKind::CreateUpload,
            ));
            let active_key = multipart_active_upload_key(bucket_id, &upload.key, upload.upload_id)?;
            let active_payload = coremeta_payload_visible_to_transaction(
                store,
                transaction,
                CF_OBJECT_HEADS,
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                &active_key,
            )?;
            update.preconditions.push(coremeta_row_precondition(
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                active_key,
                active_payload.as_ref(),
                event == MultipartMutationKind::CreateUpload,
                event != MultipartMutationKind::CreateUpload,
            ));
            let active_count_key = multipart_active_count_key(bucket_id)?;
            let active_count_payload = coremeta_payload_visible_to_transaction(
                store,
                transaction,
                CF_OBJECT_HEADS,
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                &active_count_key,
            )?;
            stage_active_count_update(
                &mut update,
                active_count_payload.as_ref(),
                tenant_id,
                bucket_id,
                event,
            )?;
            let logical_revision = current
                .as_ref()
                .map(|row| row.logical_revision)
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| anyhow!("multipart upload logical revision overflow"))?;
            update.upload_row = Some(MultipartUploadCurrentRow {
                upload: upload.clone(),
                logical_revision,
            });
        }
        MultipartMutationKind::UpsertPart => {
            let part = part.ok_or_else(|| anyhow!("multipart part event missing part"))?;
            let upload_key = multipart_upload_row_key(tenant_id, bucket_id, part.upload_id)?;
            let upload_payload = coremeta_payload_visible_to_transaction(
                store,
                transaction,
                CF_OBJECT_HEADS,
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                &upload_key,
            )?;
            update.preconditions.push(coremeta_row_precondition(
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                upload_key,
                upload_payload.as_ref(),
                false,
                true,
            ));

            let part_key =
                multipart_part_row_key(tenant_id, bucket_id, part.upload_id, part.part_number)?;
            let payload = coremeta_payload_visible_to_transaction(
                store,
                transaction,
                CF_OBJECT_HEADS,
                TABLE_MULTIPART_PART_CURRENT_ROW,
                &part_key,
            )?;
            let current = payload
                .as_deref()
                .map(decode_part_current_row)
                .transpose()?;
            update.preconditions.push(coremeta_row_precondition(
                TABLE_MULTIPART_PART_CURRENT_ROW,
                part_key,
                payload.as_ref(),
                payload.is_none(),
                payload.is_some(),
            ));
            update.part_row = Some(MultipartPartCurrentRow {
                tenant_id,
                bucket_id,
                part: part.clone(),
                logical_revision: current
                    .as_ref()
                    .map(|row| row.logical_revision)
                    .unwrap_or(0)
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("multipart part logical revision overflow"))?,
            });
        }
    }
    Ok(update)
}

pub(super) fn coremeta_payload_visible_to_transaction(
    store: &CoreStore,
    transaction: &CoreTransaction,
    cf: &str,
    table_id: u16,
    tuple_key: &[u8],
) -> Result<Option<Vec<u8>>> {
    let cf = canonical_coremeta_cf_name(cf)?;
    let mut current = store.read_coremeta_row(cf, table_id, tuple_key)?;
    for update in &transaction.visible_updates {
        match update {
            CoreTransactionUpdate::CoreMetaPut {
                cf: update_cf,
                table_id: update_table_id,
                tuple_key: update_key,
                payload,
                ..
            } => {
                if canonical_coremeta_cf_name(update_cf)? == cf
                    && *update_table_id == table_id
                    && update_key == tuple_key
                {
                    current = Some(payload.clone());
                }
            }
            CoreTransactionUpdate::CoreMetaDelete {
                cf: update_cf,
                table_id: update_table_id,
                tuple_key: update_key,
                ..
            } => {
                if canonical_coremeta_cf_name(update_cf)? == cf
                    && *update_table_id == table_id
                    && update_key == tuple_key
                {
                    current = None;
                }
            }
            _ => {}
        }
    }
    Ok(current)
}

pub(super) fn multipart_current_row_operations(
    update: &MultipartCurrentRowUpdate,
    partition_id: &str,
) -> Result<Vec<CoreMutationOperation>> {
    let mut operations = Vec::new();
    if let Some(row) = update.upload_row.as_ref() {
        // Every index copy carries the same domain revision; CoreStore binds publication common.
        let payload = encode_upload_current_row(row)?;
        operations.push(CoreMutationOperation::CoreMetaPut {
            partition_id: partition_id.to_string(),
            cf: CF_OBJECT_HEADS.to_string(),
            table_id: TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
            tuple_key: multipart_upload_row_key(
                row.upload.tenant_id,
                row.upload.bucket_id,
                row.upload.id,
            )?,
            payload: payload.clone(),
        });
        operations.push(CoreMutationOperation::CoreMetaPut {
            partition_id: partition_id.to_string(),
            cf: CF_OBJECT_HEADS.to_string(),
            table_id: TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
            tuple_key: multipart_upload_id_head_key(row.upload.id)?,
            payload: payload.clone(),
        });
        operations.push(CoreMutationOperation::CoreMetaPut {
            partition_id: partition_id.to_string(),
            cf: CF_OBJECT_HEADS.to_string(),
            table_id: TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
            tuple_key: multipart_active_upload_key(
                row.upload.bucket_id,
                &row.upload.key,
                row.upload.upload_id,
            )?,
            payload,
        });
    }
    if let Some(row) = update.part_row.as_ref() {
        operations.push(CoreMutationOperation::CoreMetaPut {
            partition_id: partition_id.to_string(),
            cf: CF_OBJECT_HEADS.to_string(),
            table_id: TABLE_MULTIPART_PART_CURRENT_ROW,
            tuple_key: multipart_part_row_key(
                row.tenant_id,
                row.bucket_id,
                row.part.upload_id,
                row.part.part_number,
            )?,
            payload: encode_part_current_row(row)?,
        });
    }
    if let Some(row) = update.active_count_row.as_ref() {
        operations.push(CoreMutationOperation::CoreMetaPut {
            partition_id: partition_id.to_string(),
            cf: CF_OBJECT_HEADS.to_string(),
            table_id: TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
            tuple_key: multipart_active_count_key(row.bucket_id)?,
            payload: encode_active_count_current_row(row)?,
        });
    }
    Ok(operations)
}

fn coremeta_row_precondition(
    table_id: u16,
    tuple_key: Vec<u8>,
    current_payload: Option<&Vec<u8>>,
    require_absent: bool,
    require_present: bool,
) -> CoreMutationPrecondition {
    CoreMutationPrecondition::CoreMetaRow {
        cf: CF_OBJECT_HEADS.to_string(),
        table_id,
        tuple_key,
        expected_payload_hash: current_payload
            .map(|payload| core_meta_payload_digest(table_id, payload)),
        require_absent,
        require_present,
    }
}
