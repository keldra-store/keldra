use super::*;
use crate::core_store::stream_event_hash_proto::encode_stream_event_hash_input;

pub(super) fn event_hash_input(record: &StreamRecord) -> Result<Vec<u8>> {
    encode_stream_event_hash_input(record)
}

pub(super) fn build_stream_record_after_head(
    input: AppendStreamRecord,
    idempotency_key_hash: Option<String>,
    authenticated_principal: String,
    last_sequence: u64,
    previous_event_hash: String,
    created_at: String,
) -> Result<StreamRecord> {
    let sequence = last_sequence
        .checked_add(1)
        .ok_or_else(|| anyhow!("CoreStore stream sequence overflow"))?;
    let payload_hash = format!("sha256:{}", sha256_hex(&input.payload));
    let mut record = StreamRecord {
        schema: CORE_WATCH_EVENT_SCHEMA.to_string(),
        cursor: format!("{}:{sequence:020}", input.stream_id),
        stream_id: input.stream_id,
        partition_id: input.partition_id,
        sequence,
        previous_event_hash,
        event_hash: String::new(),
        record_kind: input.record_kind,
        payload_hash,
        payload: input.payload,
        content_type: input.content_type,
        user_metadata_json: input.user_metadata_json,
        authenticated_principal,
        transaction_id: input.transaction_id,
        idempotency_key_hash,
        created_at,
    };
    record.event_hash = format!("sha256:{}", sha256_hex(&event_hash_input(&record)?));
    Ok(record)
}

pub(super) fn validate_stream_record_index_row_metadata(
    stream_id: &str,
    row: &StoredStreamRecordIndexRow,
) -> Result<()> {
    if row.schema != "anvil.core.stream_record_index.v1" {
        bail!("CoreStore stream record metadata row has invalid schema");
    }
    if row.stream_id != stream_id {
        bail!("CoreStore stream record metadata row has invalid scope");
    }
    if row.authenticated_principal.len() > 4 * 1024 || row.authenticated_principal.contains('\0') {
        bail!("CoreStore stream record authenticated principal is invalid");
    }
    Ok(())
}

pub(super) fn verify_stream_record_after_head(
    stream_id: &str,
    previous_sequence: u64,
    previous_event_hash: &str,
    record: &StreamRecord,
) -> Result<()> {
    if record.stream_id != stream_id {
        bail!("CoreStore stream record metadata row has invalid scope");
    }
    let expected_sequence = previous_sequence
        .checked_add(1)
        .ok_or_else(|| anyhow!("CoreStore stream sequence overflow"))?;
    if record.sequence != expected_sequence {
        bail!(
            "CoreStore stream {} has sequence gap: expected {}, got {}",
            record.stream_id,
            expected_sequence,
            record.sequence
        );
    }
    let expected_cursor = format!("{stream_id}:{expected_sequence:020}");
    if record.cursor != expected_cursor {
        bail!("CoreStore stream {} cursor mismatch", record.stream_id);
    }
    if record.previous_event_hash != previous_event_hash {
        bail!("CoreStore stream {} hash chain mismatch", record.stream_id);
    }
    let expected_payload_hash = format!("sha256:{}", sha256_hex(&record.payload));
    if record.payload_hash != expected_payload_hash {
        bail!(
            "CoreStore stream {} payload hash mismatch",
            record.stream_id
        );
    }
    let expected_hash = format!("sha256:{}", sha256_hex(&event_hash_input(record)?));
    if record.event_hash != expected_hash {
        bail!("CoreStore stream {} event hash mismatch", record.stream_id);
    }
    Ok(())
}

pub(super) fn unix_timestamp_nanos() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    u64::try_from(now.as_nanos()).unwrap_or(u64::MAX)
}

pub(super) fn read_exact<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| anyhow!("CoreStore frame offset overflow"))?;
    if end > bytes.len() {
        bail!("CoreStore frame ended unexpectedly");
    }
    let slice = &bytes[*offset..end];
    *offset = end;
    Ok(slice)
}

pub(super) fn read_u16_le(bytes: &[u8], offset: &mut usize) -> Result<u16> {
    let raw = read_exact(bytes, offset, 2)?;
    Ok(u16::from_le_bytes(raw.try_into()?))
}

pub(super) fn read_u32_le(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    let raw = read_exact(bytes, offset, 4)?;
    Ok(u32::from_le_bytes(raw.try_into()?))
}

pub(super) fn read_u64_le(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    let raw = read_exact(bytes, offset, 8)?;
    Ok(u64::from_le_bytes(raw.try_into()?))
}

pub(super) fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ 0x82f6_3b78;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}
