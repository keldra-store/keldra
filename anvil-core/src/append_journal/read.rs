use super::*;

const APPEND_READ_PAGE_MAX_ROWS: usize = 1_000;

#[derive(Debug, Clone)]
pub struct AppendStreamRecordPage {
    pub records: Vec<AppendStreamRecord>,
    pub next_sequence: u64,
    pub has_more: bool,
}

#[derive(Debug, Clone)]
pub struct AppendStreamPage {
    pub streams: Vec<AppendStream>,
    pub next_stream_id: Option<String>,
}

pub async fn get_active_append_stream(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    stream_key: &str,
    stream_id: uuid::Uuid,
) -> Result<Option<AppendStream>> {
    get_active_append_stream_for_optional_transaction(
        storage, tenant_id, bucket_id, stream_key, stream_id, None,
    )
    .await
}

pub async fn get_active_append_stream_in_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    stream_key: &str,
    stream_id: uuid::Uuid,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<Option<AppendStream>> {
    get_active_append_stream_for_optional_transaction(
        storage,
        tenant_id,
        bucket_id,
        stream_key,
        stream_id,
        Some((transaction_id, transaction_principal)),
    )
    .await
}

pub(super) async fn get_active_append_stream_for_optional_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    stream_key: &str,
    stream_id: uuid::Uuid,
    transaction: Option<(&str, &str)>,
) -> Result<Option<AppendStream>> {
    let state_stream_id =
        append_state_stream_id_for_identity(tenant_id, bucket_id, stream_key, stream_id)?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let Some(sequence) =
        latest_visible_sequence(&core_store, &state_stream_id, transaction).await?
    else {
        return Ok(None);
    };
    let Some(record) = read_record_at(&core_store, &state_stream_id, sequence, transaction).await?
    else {
        return Err(anyhow!(
            "append stream state head {state_stream_id}:{sequence} is not readable"
        ));
    };
    if record.record_kind != "append_metadata.state" {
        bail!("append stream state contains a different record kind");
    }
    let body = decode_append_body(&record.payload)?;
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
    Ok(Some(stream))
}

pub async fn list_append_stream_records_page(
    storage: &Storage,
    stream: &AppendStream,
    after_sequence: u64,
    limit: usize,
) -> Result<AppendStreamRecordPage> {
    ensure_page_size(limit)?;
    let stream_id = append_record_stream_id(stream)?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let page = core_store
        .read_stream_page(ReadStream {
            stream_id: stream_id.clone(),
            after_sequence,
            limit,
        })
        .await?;
    let mut records = Vec::with_capacity(page.records.len());
    for source in page.records {
        if source.record_kind != "append_metadata.record" {
            bail!("append record stream contains a different record kind");
        }
        let body = decode_append_body(&source.payload)?;
        if body.event != "append_record" {
            bail!("append record stream contains a different event");
        }
        let record = body
            .record
            .ok_or_else(|| anyhow!("append record event is missing record"))?;
        let source_sequence = i64::try_from(source.sequence)
            .map_err(|_| anyhow!("append record sequence exceeds the supported range"))?;
        if record.stream_id != stream.id || record.record_sequence != source_sequence {
            bail!("append record event does not match its physical stream");
        }
        records.push(record);
    }
    Ok(AppendStreamRecordPage {
        records,
        next_sequence: page.next_sequence,
        has_more: page.has_more,
    })
}

pub async fn list_append_streams_page(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    after_stream_id: Option<&str>,
    limit: usize,
) -> Result<AppendStreamPage> {
    ensure_page_size(limit)?;
    let prefix = append_state_stream_prefix(tenant_id, bucket_id);
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_ids = core_store
        .list_stream_ids_page(&prefix, after_stream_id, limit + 1)
        .await?;
    let has_more = stream_ids.len() > limit;
    let visible = if has_more {
        &stream_ids[..limit]
    } else {
        &stream_ids[..]
    };
    let mut streams = Vec::with_capacity(visible.len());
    for state_stream_id in visible {
        let Some(head) = core_store
            .visible_stream_head_metadata(state_stream_id)
            .await?
        else {
            continue;
        };
        let record = read_record_at(&core_store, state_stream_id, head.sequence, None)
            .await?
            .ok_or_else(|| anyhow!("append state stream head is not readable"))?;
        if record.record_kind != "append_metadata.state" {
            bail!("append stream state contains a different record kind");
        }
        let body = decode_append_body(&record.payload)?;
        if !matches!(body.event.as_str(), "create_stream" | "seal_stream") {
            bail!("append stream state contains a non-state event");
        }
        let stream = body
            .stream
            .ok_or_else(|| anyhow!("append state event is missing stream"))?;
        if stream.tenant_id != tenant_id
            || stream.bucket_id != bucket_id
            || append_state_stream_id(&stream)? != *state_stream_id
        {
            bail!("append state event does not match its physical stream");
        }
        streams.push(stream);
    }
    Ok(AppendStreamPage {
        streams,
        next_stream_id: has_more.then(|| visible.last().cloned()).flatten(),
    })
}

pub async fn append_stream_has_records(
    storage: &Storage,
    stream: &AppendStream,
    transaction: Option<(&str, &str)>,
) -> Result<bool> {
    let core_store = CoreStore::new(storage.clone()).await?;
    Ok(
        latest_visible_sequence(&core_store, &append_record_stream_id(stream)?, transaction)
            .await?
            .is_some(),
    )
}

pub async fn append_record_source_cursor(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<u128> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = append_record_cursor_stream_id(tenant_id, bucket_id);
    let Some(head) = core_store.visible_stream_head_metadata(&stream_id).await? else {
        return Ok(0);
    };
    let record = read_record_at(&core_store, &stream_id, head.sequence, None)
        .await?
        .ok_or_else(|| anyhow!("append record cursor head is not readable"))?;
    if record.record_kind != "append_metadata.record_cursor" {
        bail!("append record cursor contains a different record kind");
    }
    let body = decode_append_body(&record.payload)?;
    if body.event != "append_record" {
        bail!("append record cursor contains a different event");
    }
    let record = body
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

async fn latest_visible_sequence(
    core_store: &CoreStore,
    stream_id: &str,
    transaction: Option<(&str, &str)>,
) -> Result<Option<u64>> {
    let mut sequence = core_store
        .visible_stream_head_metadata(stream_id)
        .await?
        .map(|metadata| metadata.sequence);
    if let Some((transaction_id, principal)) = transaction {
        let transaction = core_store
            .read_explicit_transaction_for_principal(transaction_id, principal)
            .await?;
        for update in transaction.visible_updates {
            if let CoreTransactionUpdate::StreamAppend {
                stream_id: update_stream_id,
                visible_sequence,
                ..
            } = update
                && update_stream_id == stream_id
            {
                sequence = Some(sequence.unwrap_or(0).max(visible_sequence));
            }
        }
    }
    Ok(sequence.filter(|sequence| *sequence > 0))
}

async fn read_record_at(
    core_store: &CoreStore,
    stream_id: &str,
    sequence: u64,
    transaction: Option<(&str, &str)>,
) -> Result<Option<StreamRecord>> {
    let input = ReadStream {
        stream_id: stream_id.to_string(),
        after_sequence: sequence.saturating_sub(1),
        limit: 1,
    };
    let records = if let Some((transaction_id, principal)) = transaction {
        core_store
            .read_stream_visible_to_transaction(input, transaction_id, principal)
            .await?
    } else {
        core_store.read_stream_page(input).await?.records
    };
    Ok(records
        .into_iter()
        .find(|record| record.sequence == sequence))
}

fn ensure_page_size(limit: usize) -> Result<()> {
    if !(1..=APPEND_READ_PAGE_MAX_ROWS).contains(&limit) {
        bail!("append journal page size must be between 1 and {APPEND_READ_PAGE_MAX_ROWS}");
    }
    Ok(())
}
