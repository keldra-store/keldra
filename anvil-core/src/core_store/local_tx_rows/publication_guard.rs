use super::*;

const PUBLICATION_GUARD_SCHEMA: &str = "anvil.core.publication_guard.v1";

#[derive(Clone, PartialEq, Message)]
struct CoreMetaValueEnvelopeProto {
    #[prost(uint32, tag = "1")]
    table_id: u32,
    #[prost(uint32, tag = "2")]
    schema_version: u32,
    #[prost(bytes, tag = "3")]
    payload: Vec<u8>,
    #[prost(string, tag = "4")]
    payload_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::core_store::local) struct CorePublicationGuardSummary {
    pub(in crate::core_store::local) context_hash: String,
    pub(in crate::core_store::local) transaction_expires_at_unix_nanos: u64,
    pub(in crate::core_store::local) visible_update_count: u64,
    pub(in crate::core_store::local) precondition_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::core_store::local) struct CorePublicationGuardContext {
    pub(in crate::core_store::local) summary: CorePublicationGuardSummary,
    pub(in crate::core_store::local) transaction: CoreTransaction,
    pub(in crate::core_store::local) preconditions: Vec<CoreTransactionPreconditionRow>,
}

struct EncodedPublicationGuardRows {
    summary: CorePublicationGuardSummary,
    header: CoreTransactionHeaderRow,
    updates: Vec<CoreTransactionStagedUpdateRow>,
    preconditions: Vec<CoreTransactionPreconditionRow>,
}

pub(in crate::core_store::local) fn publication_guard_summary(
    transaction_id: &str,
    rows: &[&CoreMetaEncodedOwnedRow],
) -> Result<Option<CorePublicationGuardSummary>> {
    Ok(collect_publication_guard_rows(transaction_id, rows)?.map(|rows| rows.summary))
}

pub(in crate::core_store::local) async fn hydrate_publication_guard_context(
    store: &CoreStore,
    transaction_id: &str,
    rows: &[&CoreMetaEncodedOwnedRow],
) -> Result<Option<CorePublicationGuardContext>> {
    let Some(encoded) = collect_publication_guard_rows(transaction_id, rows)? else {
        return Ok(None);
    };
    let mut transaction = encoded.header.transaction;
    let mut visible_updates = Vec::with_capacity(encoded.updates.len());
    for update in encoded.updates {
        visible_updates.push(store.transaction_update_from_row(update).await?);
    }
    transaction.visible_updates = visible_updates;
    validate_transaction_root_scope(&transaction)?;
    Ok(Some(CorePublicationGuardContext {
        summary: encoded.summary,
        transaction,
        preconditions: encoded.preconditions,
    }))
}

fn collect_publication_guard_rows(
    transaction_id: &str,
    rows: &[&CoreMetaEncodedOwnedRow],
) -> Result<Option<EncodedPublicationGuardRows>> {
    validate_logical_id(transaction_id, "CoreMeta publication guard transaction id")?;
    let header_key = transaction_header_tuple_key(transaction_id)?;
    let update_prefix = core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("transaction"),
        CoreMetaTuplePart::Utf8(transaction_id),
        CoreMetaTuplePart::Utf8("update"),
    ])?;
    let precondition_prefix = core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("transaction"),
        CoreMetaTuplePart::Utf8(transaction_id),
        CoreMetaTuplePart::Utf8("precondition"),
    ])?;
    let mut transaction_rows = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    for row in rows {
        if row.cf != CF_TRANSACTIONS
            || core_meta_record_table_id(&row.core_meta_key)? != TABLE_EXPLICIT_TRANSACTION_ROW
        {
            continue;
        }
        let tuple_key = core_meta_record_tuple_key(&row.core_meta_key)?.to_vec();
        if tuple_key != header_key
            && !tuple_key.starts_with(&update_prefix)
            && !tuple_key.starts_with(&precondition_prefix)
        {
            continue;
        }
        if row.delete_marker {
            bail!("CoreMeta publication guard contains a deleted transaction row");
        }
        if transaction_rows
            .insert(
                tuple_key,
                decode_publication_guard_envelope(&row.value_envelope)?,
            )
            .is_some()
        {
            bail!("CoreMeta publication guard contains a duplicate transaction row");
        }
    }

    let Some(header_payload) = transaction_rows.get(&header_key) else {
        return Ok(None);
    };
    let header = decode_transaction_header_row(header_payload, transaction_id)?;
    let update_capacity = usize::try_from(header.visible_update_count)
        .map_err(|_| anyhow!("CoreMeta publication guard update count exceeds usize"))?;
    let precondition_capacity = usize::try_from(header.precondition_count)
        .map_err(|_| anyhow!("CoreMeta publication guard precondition count exceeds usize"))?;
    let mut updates = Vec::with_capacity(update_capacity);
    let mut preconditions = Vec::with_capacity(precondition_capacity);
    let mut hash_parts = Vec::with_capacity(
        1usize
            .saturating_add(update_capacity)
            .saturating_add(precondition_capacity),
    );
    hash_parts.push((header_key, header_payload.clone()));

    for ordinal in 0..header.visible_update_count {
        let key = transaction_update_tuple_key(transaction_id, ordinal)?;
        let payload = transaction_rows
            .get(&key)
            .ok_or_else(|| anyhow!("CoreMeta publication guard staged update row is missing"))?;
        let row = decode_transaction_update_row(payload, &header.transaction)?;
        if row.ordinal != ordinal {
            bail!("CoreMeta publication guard staged update ordinal mismatch");
        }
        updates.push(row);
        hash_parts.push((key, payload.clone()));
    }
    for ordinal in 0..header.precondition_count {
        let key = transaction_precondition_tuple_key(transaction_id, ordinal)?;
        let payload = transaction_rows
            .get(&key)
            .ok_or_else(|| anyhow!("CoreMeta publication guard precondition row is missing"))?;
        let row = decode_transaction_precondition_row(payload, &header.transaction)?;
        if row.ordinal != ordinal {
            bail!("CoreMeta publication guard precondition ordinal mismatch");
        }
        preconditions.push(row);
        hash_parts.push((key, payload.clone()));
    }
    let expected_row_count = 1_u64
        .checked_add(header.visible_update_count)
        .and_then(|count| count.checked_add(header.precondition_count))
        .ok_or_else(|| anyhow!("CoreMeta publication guard row count overflow"))?;
    if u64::try_from(transaction_rows.len()).ok() != Some(expected_row_count) {
        bail!("CoreMeta publication guard contains unexpected transaction rows");
    }
    if preconditions
        .iter()
        .any(|row| row.visible_update_boundary > header.visible_update_count)
    {
        bail!("CoreMeta publication guard precondition boundary exceeds staged updates");
    }
    if preconditions
        .windows(2)
        .any(|rows| rows[0].visible_update_boundary > rows[1].visible_update_boundary)
    {
        bail!("CoreMeta publication guard precondition boundaries are not monotonic");
    }

    hash_parts.sort_by(|left, right| left.0.cmp(&right.0));
    let mut hash_input = Vec::new();
    append_guard_hash_part(&mut hash_input, PUBLICATION_GUARD_SCHEMA.as_bytes());
    append_guard_hash_part(&mut hash_input, transaction_id.as_bytes());
    for (key, payload) in hash_parts {
        append_guard_hash_part(&mut hash_input, &key);
        append_guard_hash_part(&mut hash_input, &payload);
    }
    let summary = CorePublicationGuardSummary {
        context_hash: format!("sha256:{}", sha256_hex(&hash_input)),
        transaction_expires_at_unix_nanos: header.transaction.expires_at_unix_nanos,
        visible_update_count: header.visible_update_count,
        precondition_count: header.precondition_count,
    };
    Ok(Some(EncodedPublicationGuardRows {
        summary,
        header,
        updates,
        preconditions,
    }))
}

fn decode_publication_guard_envelope(bytes: &[u8]) -> Result<Vec<u8>> {
    let envelope = CoreMetaValueEnvelopeProto::decode(bytes)?;
    let mut canonical = Vec::with_capacity(envelope.encoded_len());
    envelope.encode(&mut canonical)?;
    if canonical != bytes {
        bail!("CoreMeta publication guard envelope is not deterministically encoded");
    }
    if envelope.table_id != u32::from(TABLE_EXPLICIT_TRANSACTION_ROW)
        || envelope.schema_version != 1
        || envelope.payload_hash
            != core_meta_payload_digest(TABLE_EXPLICIT_TRANSACTION_ROW, &envelope.payload)
    {
        bail!("CoreMeta publication guard transaction envelope is invalid");
    }
    Ok(envelope.payload)
}

fn append_guard_hash_part(output: &mut Vec<u8>, value: &[u8]) {
    output.extend_from_slice(&(value.len() as u64).to_le_bytes());
    output.extend_from_slice(value);
}
