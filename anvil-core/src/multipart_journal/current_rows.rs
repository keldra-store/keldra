use super::{
    MULTIPART_CURRENT_ROW_KEY_PREFIX, MultipartMutationKind, MultipartPartCurrentRow,
    MultipartUploadCurrentRow, current_part_payload, current_upload_payload,
    decode_committed_upload_current_row, decode_part_current_row, decode_upload_current_row,
    encode_part_current_row, encode_upload_current_row, multipart_active_upload_key,
    multipart_part_row_key, multipart_upload_id_head_key, multipart_upload_row_key,
};
use crate::core_store::{
    CF_OBJECT_HEADS, CoreMetaTuplePart, TABLE_MULTIPART_PART_CURRENT_ROW,
    TABLE_MULTIPART_UPLOAD_CURRENT_ROW, core_meta_tuple_key,
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
    update.preconditions.push(mvcc_row_precondition(
        TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
        multipart_active_count_key(bucket_id)?,
        current_payload,
        current_payload.is_none(),
        current_payload.is_some(),
    )?);
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
    pub(super) preconditions: Vec<(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )>,
    pub(super) upload_row: Option<MultipartUploadCurrentRow>,
    pub(super) part_row: Option<MultipartPartCurrentRow>,
    pub(super) active_count_row: Option<MultipartActiveCountCurrentRow>,
    pub(super) remove_active_upload: bool,
}

pub(super) fn multipart_current_row_update(
    mut read: impl FnMut(u16, &[u8]) -> Result<Option<Vec<u8>>>,
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
                current_upload_payload(&mut read, tenant_id, bucket_id, upload.id)?;
            update.preconditions.push(mvcc_row_precondition(
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                multipart_upload_row_key(tenant_id, bucket_id, upload.id)?,
                payload.as_ref(),
                event == MultipartMutationKind::CreateUpload,
                event != MultipartMutationKind::CreateUpload,
            )?);
            let id_head_key = multipart_upload_id_head_key(upload.id)?;
            let id_head_payload = read(TABLE_MULTIPART_UPLOAD_CURRENT_ROW, &id_head_key)?;
            update.preconditions.push(mvcc_row_precondition(
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                id_head_key,
                id_head_payload.as_ref(),
                event == MultipartMutationKind::CreateUpload,
                event != MultipartMutationKind::CreateUpload,
            )?);
            let active_key = multipart_active_upload_key(bucket_id, &upload.key, upload.upload_id)?;
            let active_payload = read(TABLE_MULTIPART_UPLOAD_CURRENT_ROW, &active_key)?;
            update.preconditions.push(mvcc_row_precondition(
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                active_key,
                active_payload.as_ref(),
                event == MultipartMutationKind::CreateUpload,
                event != MultipartMutationKind::CreateUpload,
            )?);
            let active_count_payload = read(
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
            update.remove_active_upload = matches!(
                event,
                MultipartMutationKind::CompleteUpload | MultipartMutationKind::AbortUpload
            );
        }
        MultipartMutationKind::UpsertPart => {
            let part = part.ok_or_else(|| anyhow!("multipart part event missing part"))?;
            let upload_payload = read(
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                &multipart_upload_row_key(tenant_id, bucket_id, part.upload_id)?,
            )?;
            update.preconditions.push(mvcc_row_precondition(
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                multipart_upload_row_key(tenant_id, bucket_id, part.upload_id)?,
                upload_payload.as_ref(),
                false,
                true,
            )?);
            let (payload, current) = current_part_payload(
                &mut read,
                tenant_id,
                bucket_id,
                part.upload_id,
                part.part_number,
            )?;
            update.preconditions.push(mvcc_row_precondition(
                TABLE_MULTIPART_PART_CURRENT_ROW,
                multipart_part_row_key(tenant_id, bucket_id, part.upload_id, part.part_number)?,
                payload.as_ref(),
                payload.is_none(),
                payload.is_some(),
            )?);
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

pub(super) fn multipart_current_row_operations(
    update: &MultipartCurrentRowUpdate,
) -> Result<Vec<crate::mvcc_product::ProductMutation>> {
    let mut operations = Vec::new();
    if let Some(row) = update.upload_row.as_ref() {
        // Every index copy carries the same domain revision in one MVCC commit.
        let payload = encode_upload_current_row(row)?;
        operations.push(crate::mvcc_product::ProductMutation::put(
            logical_key(
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                &multipart_upload_row_key(
                    row.upload.tenant_id,
                    row.upload.bucket_id,
                    row.upload.id,
                )?,
            )?,
            payload.clone(),
        ));
        operations.push(crate::mvcc_product::ProductMutation::put(
            logical_key(
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                &multipart_upload_id_head_key(row.upload.id)?,
            )?,
            payload.clone(),
        ));
        let active_key = logical_key(
            TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
            &multipart_active_upload_key(
                row.upload.bucket_id,
                &row.upload.key,
                row.upload.upload_id,
            )?,
        )?;
        operations.push(if update.remove_active_upload {
            crate::mvcc_product::ProductMutation::delete(active_key)
        } else {
            crate::mvcc_product::ProductMutation::put(active_key, payload)
        });
    }
    if let Some(row) = update.part_row.as_ref() {
        operations.push(crate::mvcc_product::ProductMutation::put(
            logical_key(
                TABLE_MULTIPART_PART_CURRENT_ROW,
                &multipart_part_row_key(
                    row.tenant_id,
                    row.bucket_id,
                    row.part.upload_id,
                    row.part.part_number,
                )?,
            )?,
            encode_part_current_row(row)?,
        ));
    }
    if let Some(row) = update.active_count_row.as_ref() {
        operations.push(crate::mvcc_product::ProductMutation::put(
            logical_key(
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                &multipart_active_count_key(row.bucket_id)?,
            )?,
            encode_active_count_current_row(row)?,
        ));
    }
    Ok(operations)
}

fn mvcc_row_precondition(
    table_id: u16,
    tuple_key: Vec<u8>,
    current_payload: Option<&Vec<u8>>,
    require_absent: bool,
    require_present: bool,
) -> Result<(
    crate::mvcc_transaction::LogicalKey,
    crate::mvcc_transaction::PredicateKind,
)> {
    if current_payload.is_none() && require_present {
        return Err(anyhow!("required multipart MVCC row is missing"));
    }
    if current_payload.is_some() && require_absent {
        return Err(anyhow!("multipart MVCC row already exists"));
    }
    Ok((
        logical_key(table_id, &tuple_key)?,
        match current_payload {
            Some(payload) => {
                crate::mvcc_transaction::PredicateKind::ValueHash(*blake3::hash(payload).as_bytes())
            }
            None if require_absent => crate::mvcc_transaction::PredicateKind::Absent,
            None => crate::mvcc_transaction::PredicateKind::Absent,
        },
    ))
}

fn logical_key(table_id: u16, tuple_key: &[u8]) -> Result<crate::mvcc_transaction::LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(CF_OBJECT_HEADS, table_id, tuple_key)
}
