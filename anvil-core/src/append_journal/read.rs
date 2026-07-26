use super::*;

const APPEND_READ_PAGE_MAX_ROWS: usize = 1_000;

#[derive(Debug, Clone)]
pub struct AppendStreamRecordPage {
    pub records: Vec<AppendStreamRecord>,
    pub next_sequence: u64,
    pub has_more: bool,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AppendStreamPage {
    pub streams: Vec<AppendStream>,
    pub next_stream_id: Option<String>,
    pub next_cursor: Option<String>,
}

pub fn list_append_stream_records_page_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    stream: &AppendStream,
    after_cursor: Option<&str>,
    limit: usize,
) -> Result<AppendStreamRecordPage> {
    ensure_page_size(limit)?;
    let (snapshot, after_sequence) =
        decode_append_cursor(after_cursor, mvcc.runtime.applied_version()?)?;
    let stream_id = append_record_stream_id(stream)?;
    let prefix = crate::mvcc_product::stream_logical_key(
        crate::core_store::TABLE_STREAM_RECORD_INDEX_ROW,
        &stream_id,
        None,
    )?;
    let mut records = mvcc
        .runtime
        .scan_table_prefix_at(
            crate::core_store::TABLE_STREAM_RECORD_INDEX_ROW,
            &prefix.application_key,
            snapshot,
        )?
        .into_iter()
        .map(|(_, row)| {
            let (kind, payload) = crate::mvcc_product::decode_stream_record_value(&row.value)?;
            if kind != "append_metadata.record" {
                bail!("append record stream contains a different record kind");
            }
            let body = decode_append_body(&payload)?;
            body.record
                .ok_or_else(|| anyhow!("append record event is missing record"))
        })
        .collect::<Result<Vec<_>>>()?;
    records.sort_by_key(|record| record.record_sequence);
    records
        .retain(|record| u64::try_from(record.record_sequence).is_ok_and(|v| v > after_sequence));
    let has_more = records.len() > limit;
    records.truncate(limit);
    let next_sequence = records
        .last()
        .and_then(|record| u64::try_from(record.record_sequence).ok())
        .unwrap_or(after_sequence);
    Ok(AppendStreamRecordPage {
        next_cursor: has_more.then(|| encode_append_cursor(snapshot, next_sequence)),
        records,
        next_sequence,
        has_more,
    })
}

pub fn list_append_streams_page_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    after_cursor: Option<&str>,
    limit: usize,
) -> Result<AppendStreamPage> {
    ensure_page_size(limit)?;
    let (snapshot, after_ordinal) =
        decode_append_cursor(after_cursor, mvcc.runtime.applied_version()?)?;
    let mut streams = mvcc
        .runtime
        .scan_table_prefix_at(
            crate::core_store::TABLE_STREAM_HEAD_ROW,
            crate::mvcc_product::stream_table_prefix(),
            snapshot,
        )?
        .into_iter()
        .map(|(_, row)| {
            let body = decode_append_body(&row.value)?;
            body.stream
                .ok_or_else(|| anyhow!("append state event is missing stream"))
        })
        .collect::<Result<Vec<_>>>()?;
    streams.retain(|stream| {
        stream.tenant_id == tenant_id
            && stream.bucket_id == bucket_id
            && append_state_stream_id(stream).is_ok()
    });
    streams.sort_by_key(|stream| stream.id);
    streams.retain(|stream| u64::try_from(stream.id).is_ok_and(|id| id > after_ordinal));
    let has_more = streams.len() > limit;
    streams.truncate(limit);
    let next_ordinal = streams
        .last()
        .and_then(|stream| u64::try_from(stream.id).ok())
        .unwrap_or(after_ordinal);
    Ok(AppendStreamPage {
        next_stream_id: streams.last().map(|stream| stream.stream_id.to_string()),
        next_cursor: has_more.then(|| encode_append_cursor(snapshot, next_ordinal)),
        streams,
    })
}

fn encode_append_cursor(snapshot: u64, ordinal: u64) -> String {
    format!("mvcc:{snapshot}:{ordinal}")
}

fn decode_append_cursor(cursor: Option<&str>, latest_snapshot: u64) -> Result<(u64, u64)> {
    let Some(cursor) = cursor else {
        return Ok((latest_snapshot, 0));
    };
    let mut parts = cursor.split(':');
    if parts.next() != Some("mvcc") {
        bail!("append page cursor has an invalid schema");
    }
    let snapshot = parts
        .next()
        .ok_or_else(|| anyhow!("append page cursor is missing snapshot"))?
        .parse()?;
    let ordinal = parts
        .next()
        .ok_or_else(|| anyhow!("append page cursor is missing ordinal"))?
        .parse()?;
    if parts.next().is_some() {
        bail!("append page cursor has trailing fields");
    }
    Ok((snapshot, ordinal))
}

pub async fn get_active_append_stream_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    stream_key: &str,
    stream_id: uuid::Uuid,
) -> Result<Option<AppendStream>> {
    get_active_append_stream_for_optional_transaction(
        mvcc, tenant_id, bucket_id, stream_key, stream_id, None,
    )
    .await
}

pub async fn get_active_append_stream_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    stream_key: &str,
    stream_id: uuid::Uuid,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<Option<AppendStream>> {
    get_active_append_stream_for_optional_transaction(
        mvcc,
        tenant_id,
        bucket_id,
        stream_key,
        stream_id,
        Some((transaction_id, transaction_principal)),
    )
    .await
}

pub(super) async fn get_active_append_stream_for_optional_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    stream_key: &str,
    stream_id: uuid::Uuid,
    transaction: Option<(&str, &str)>,
) -> Result<Option<AppendStream>> {
    let state_stream_id =
        append_state_stream_id_for_identity(tenant_id, bucket_id, stream_key, stream_id)?;
    if let Some((transaction_id, principal)) = transaction {
        let key = crate::mvcc_product::stream_logical_key(
            crate::core_store::TABLE_STREAM_HEAD_ROW,
            &state_stream_id,
            None,
        )?;
        return mvcc
            .read_transaction_value(transaction_id, principal, &key)?
            .map(|payload| {
                decode_active_stream_state(
                    &state_stream_id,
                    tenant_id,
                    bucket_id,
                    stream_key,
                    stream_id,
                    &payload,
                )
            })
            .transpose();
    }
    let key = crate::mvcc_product::stream_logical_key(
        crate::core_store::TABLE_STREAM_HEAD_ROW,
        &state_stream_id,
        None,
    )?;
    mvcc.read_latest_value(&key)?
        .map(|payload| {
            decode_active_stream_state(
                &state_stream_id,
                tenant_id,
                bucket_id,
                stream_key,
                stream_id,
                &payload,
            )
        })
        .transpose()
}

fn decode_active_stream_state(
    state_stream_id: &str,
    tenant_id: i64,
    bucket_id: i64,
    stream_key: &str,
    stream_id: uuid::Uuid,
    payload: &[u8],
) -> Result<AppendStream> {
    let body = decode_append_body(payload)?;
    if !matches!(body.event.as_str(), "create_stream" | "seal_stream") {
        bail!("append stream state contains a non-state event");
    }
    let stream = body
        .stream
        .ok_or_else(|| anyhow!("append stream state event is missing stream"))?;
    if stream.tenant_id != tenant_id
        || stream.bucket_id != bucket_id
        || stream.stream_key != stream_key
        || stream.stream_id != stream_id
        || append_state_stream_id(&stream)? != state_stream_id
    {
        bail!("append stream state event does not match its physical stream");
    }
    Ok(stream)
}

pub fn append_stream_has_records(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    stream: &AppendStream,
    transaction: Option<(&str, &str)>,
) -> Result<bool> {
    if let Some((transaction_id, principal)) = transaction {
        let key = crate::mvcc_product::stream_logical_key(
            crate::core_store::TABLE_STREAM_HEAD_ROW,
            &append_record_stream_id(stream)?,
            None,
        )?;
        return Ok(mvcc
            .read_transaction_value(transaction_id, principal, &key)?
            .is_some());
    }
    let key = crate::mvcc_product::stream_logical_key(
        crate::core_store::TABLE_STREAM_HEAD_ROW,
        &append_record_stream_id(stream)?,
        None,
    )?;
    Ok(mvcc.read_latest_value(&key)?.is_some())
}

pub fn append_record_source_cursor_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<u128> {
    let key = crate::mvcc_product::stream_logical_key(
        crate::core_store::TABLE_STREAM_HEAD_ROW,
        &append_record_cursor_stream_id(tenant_id, bucket_id),
        None,
    )?;
    let Some(payload) = mvcc.read_latest_value(&key)? else {
        return Ok(0);
    };
    let record = decode_append_body(&payload)?
        .record
        .ok_or_else(|| anyhow!("append record cursor event is missing record"))?;
    u128::try_from(record.id).map_err(|_| anyhow!("append record cursor id is negative"))
}

pub(super) fn append_state_stream_id(stream: &AppendStream) -> Result<String> {
    append_state_stream_id_for_identity(
        stream.tenant_id,
        stream.bucket_id,
        &stream.stream_key,
        stream.stream_id,
    )
}

pub(super) fn append_record_stream_id(stream: &AppendStream) -> Result<String> {
    Ok(format!(
        "append_records:tenant:{}:bucket:{}:{}",
        stream.tenant_id,
        stream.bucket_id,
        append_stream_identity_hash(&stream.stream_key, stream.stream_id)
    ))
}

pub(super) fn append_record_cursor_stream_id(tenant_id: i64, bucket_id: i64) -> String {
    format!("append_record_cursor:tenant:{tenant_id}:bucket:{bucket_id}")
}

fn append_state_stream_id_for_identity(
    tenant_id: i64,
    bucket_id: i64,
    stream_key: &str,
    stream_id: uuid::Uuid,
) -> Result<String> {
    if stream_key.is_empty() {
        bail!("append stream key must not be empty");
    }
    Ok(format!(
        "append_state:tenant:{tenant_id}:bucket:{bucket_id}:{}",
        append_stream_identity_hash(stream_key, stream_id)
    ))
}

fn append_state_stream_prefix(tenant_id: i64, bucket_id: i64) -> String {
    format!("append_state:tenant:{tenant_id}:bucket:{bucket_id}:")
}

fn append_stream_identity_hash(stream_key: &str, stream_id: uuid::Uuid) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&(stream_key.len() as u64).to_be_bytes());
    hasher.update(stream_key.as_bytes());
    hasher.update(stream_id.as_bytes());
    hasher.finalize().to_hex().to_string()
}

fn ensure_page_size(limit: usize) -> Result<()> {
    if !(1..=APPEND_READ_PAGE_MAX_ROWS).contains(&limit) {
        bail!("append journal page size must be between 1 and {APPEND_READ_PAGE_MAX_ROWS}");
    }
    Ok(())
}
