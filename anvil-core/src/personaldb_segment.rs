use crate::core_store::{CorePipelinePolicy, CoreStore, CoreTraceContext};
use crate::formats::{
    FileFamily, Hash32, decode_writer_segment, encode_writer_segment_header, hash32,
    header_field_string, header_field_u64,
    personaldb::{PersonalDbLogRecord, validate_personaldb_log_chain},
    required_header_string, required_header_u64, single_body_range_index,
    table::{TableRow, WriterBodyTable, decode_writer_body_tables, encode_writer_body_tables},
    unix_nanos_from_rfc3339,
    writer::{
        WriterFamily, WriterSegmentBuildInput, build_writer_segment_logical_file,
        canonical_logical_file_id,
    },
};
use crate::personaldb_coremeta::{
    PERSONALDB_DATA_LOCATOR_PAGE_MAX, list_personaldb_data_locator_rows,
    read_personaldb_data_locator_bytes, read_personaldb_data_locator_row,
    write_personaldb_logical_file_as_data_locator_with_preconditions,
};
use crate::storage::Storage;
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

const PERSONALDB_LOG_SEGMENT_REF_PREFIX: &str = "personaldb_log_segment:";
const TABLE_PERSONALDB_GROUP_DESCRIPTOR: u16 = 0x0601;
const TABLE_PERSONALDB_CHANGESET: u16 = 0x0602;
const TABLE_PERSONALDB_WITNESS_RECORD: u16 = 0x0605;
const TABLE_PERSONALDB_OWNER_TRANSFER: u16 = 0x0606;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersonalDbLogSegmentHeader {
    pub tenant_id: String,
    pub database_id: String,
    pub start_log_index: u64,
    pub end_log_index: u64,
    pub base_log_hash: String,
    pub policy_epoch: u64,
    pub membership_epoch: u64,
    pub schema_hash: String,
    pub source_fence_token: u64,
    pub codec: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedPersonalDbLogSegment {
    pub header: PersonalDbLogSegmentHeader,
    pub records: Vec<PersonalDbLogRecord>,
}

#[derive(Debug, Clone)]
pub struct PersonalDbLogSegmentWrite<'a> {
    pub tenant_id: i64,
    pub database_id: &'a str,
    pub schema_hash: Hash32,
    pub source_fence_token: u64,
    pub records: &'a [PersonalDbLogRecord],
}

pub async fn write_personaldb_log_segment(
    storage: &Storage,
    input: PersonalDbLogSegmentWrite<'_>,
) -> Result<String> {
    if input.source_fence_token == 0 {
        return Err(anyhow!(
            "personaldb log segment source fence token must be nonzero"
        ));
    }
    validate_log_segment_records(input.records)?;
    let start_log_index = input.records.first().expect("validated nonempty").log_index;
    let end_log_index = input.records.last().expect("validated nonempty").log_index;
    let base_log_hash = input
        .records
        .first()
        .expect("validated nonempty")
        .previous_log_hash;
    let policy_epoch = common_policy_epoch(input.records)?;
    let membership_epoch = common_membership_epoch(input.records)?;
    let body = encode_log_segment_body(input.records)?;
    let segment_hash = hash32(&body);
    let ref_name = personaldb_log_segment_ref_name(
        input.tenant_id,
        input.database_id,
        start_log_index,
        end_log_index,
        &hex::encode(segment_hash),
    )?;
    let logical_file_id = canonical_logical_file_id(
        WriterFamily::PersonalDb,
        end_log_index,
        &ref_name,
        &segment_hash,
    );

    let header = PersonalDbLogSegmentHeader {
        tenant_id: input.tenant_id.to_string(),
        database_id: input.database_id.to_string(),
        start_log_index,
        end_log_index,
        base_log_hash: hex::encode(base_log_hash),
        policy_epoch,
        membership_epoch,
        schema_hash: hex::encode(input.schema_hash),
        source_fence_token: input.source_fence_token,
        codec: "writer-body-table-v1".to_string(),
        created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    };
    let (first_hash, last_hash) = record_hash_bounds(input.records);
    let header_proto = encode_personaldb_log_header_proto(&logical_file_id, &header);
    let range_index = single_body_range_index(
        body.len(),
        input.records.len() as u64,
        first_hash,
        last_hash,
    )?;
    let transaction_id = format!(
        "personaldb-log-segment:{}:{}:{}:{}",
        input.tenant_id, input.database_id, start_log_index, end_log_index
    );
    let built_segment = build_writer_segment_logical_file(WriterSegmentBuildInput {
        file_family: FileFamily::PersonalDbLogSegment,
        writer_family: WriterFamily::PersonalDb,
        writer_generation: end_log_index,
        logical_file_id,
        header_proto,
        body,
        range_index,
        record_count: input.records.len() as u64,
        first_record_hash: first_hash,
        last_record_hash: last_hash,
        boundary_values: Vec::new(),
        mutation_id: transaction_id.clone(),
        region_id: "local".to_string(),
        pipeline_policy: CorePipelinePolicy::default(),
        trace_context: CoreTraceContext::default(),
    })?;
    write_personaldb_logical_file_as_data_locator_with_preconditions(
        storage,
        input.tenant_id,
        input.database_id,
        &ref_name,
        "log_segment",
        built_segment
            .logical_file
            .into_write_logical_file_request()?,
        hex::encode(segment_hash),
        vec![
            format!("start_log_index:{start_log_index:020}"),
            format!("end_log_index:{end_log_index:020}"),
        ],
        transaction_id,
        &[],
    )
    .await?;
    Ok(ref_name)
}

pub fn preview_personaldb_log_segment_ref(input: PersonalDbLogSegmentWrite<'_>) -> Result<String> {
    if input.source_fence_token == 0 {
        return Err(anyhow!(
            "personaldb log segment source fence token must be nonzero"
        ));
    }
    validate_log_segment_records(input.records)?;
    let start_log_index = input.records.first().expect("validated nonempty").log_index;
    let end_log_index = input.records.last().expect("validated nonempty").log_index;
    let body = encode_log_segment_body(input.records)?;
    let segment_hash = hash32(&body);
    personaldb_log_segment_ref_name(
        input.tenant_id,
        input.database_id,
        start_log_index,
        end_log_index,
        &hex::encode(segment_hash),
    )
}

pub async fn read_personaldb_log_segment(
    storage: &Storage,
    segment_ref: &str,
) -> Result<DecodedPersonalDbLogSegment> {
    let (tenant_id, database_id) = personaldb_log_segment_ref_scope(segment_ref)?;
    let row = read_personaldb_data_locator_row(storage, tenant_id, &database_id, segment_ref)
        .await?
        .ok_or_else(|| anyhow!("personaldb log segment CoreMeta row is missing"))?;
    if row.data_kind != "log_segment" {
        return Err(anyhow!(
            "personaldb log segment locator has wrong data kind"
        ));
    }
    let bytes = read_personaldb_data_locator_bytes(storage, &row).await?;
    decode_personaldb_log_segment(&bytes)
}

pub async fn list_personaldb_log_segment_refs(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
) -> Result<Vec<String>> {
    let prefix = personaldb_log_segment_ref_prefix(tenant_id, database_id)?;
    let mut refs = Vec::new();
    let mut after_tuple_key = None;
    loop {
        let page = list_personaldb_data_locator_rows(
            storage,
            tenant_id,
            database_id,
            after_tuple_key.as_deref(),
            PERSONALDB_DATA_LOCATOR_PAGE_MAX,
        )
        .await?;
        refs.extend(
            page.rows
                .into_iter()
                .filter(|row| row.data_kind == "log_segment" && row.data_id.starts_with(&prefix))
                .map(|row| row.data_id),
        );
        let Some(next_tuple_key) = page.next_tuple_key else {
            break;
        };
        after_tuple_key = Some(next_tuple_key);
    }
    refs.sort();
    Ok(refs)
}

pub fn decode_personaldb_log_segment(bytes: &[u8]) -> Result<DecodedPersonalDbLogSegment> {
    let segment = decode_writer_segment(bytes, FileFamily::PersonalDbLogSegment)?;
    let header = decode_personaldb_log_header_proto(&segment.header)?;
    let records = decode_log_segment_body(segment.body)?;
    validate_log_segment_records(&records)?;
    validate_header_matches_records(&header, &records)?;
    Ok(DecodedPersonalDbLogSegment { header, records })
}

fn encode_personaldb_log_header_proto(
    logical_file_id: &str,
    header: &PersonalDbLogSegmentHeader,
) -> Vec<u8> {
    encode_writer_segment_header(
        "anvil.personaldb.log_segment_header.v1",
        logical_file_id,
        FileFamily::PersonalDbLogSegment,
        header.end_log_index,
        None,
        None,
        unix_nanos_from_rfc3339(&header.created_at),
        vec![
            header_field_string("tenant_id", header.tenant_id.clone()),
            header_field_string("database_id", header.database_id.clone()),
            header_field_u64("start_log_index", header.start_log_index),
            header_field_string("base_log_hash", header.base_log_hash.clone()),
            header_field_u64("policy_epoch", header.policy_epoch),
            header_field_u64("membership_epoch", header.membership_epoch),
            header_field_string("schema_hash", header.schema_hash.clone()),
            header_field_u64("source_fence_token", header.source_fence_token),
            header_field_string("codec", header.codec.clone()),
            header_field_string("created_at", header.created_at.clone()),
        ],
    )
}

fn decode_personaldb_log_header_proto(
    header: &crate::formats::WriterSegmentHeaderProto,
) -> Result<PersonalDbLogSegmentHeader> {
    Ok(PersonalDbLogSegmentHeader {
        tenant_id: required_header_string(header, "tenant_id")?,
        database_id: required_header_string(header, "database_id")?,
        start_log_index: required_header_u64(header, "start_log_index")?,
        end_log_index: header.writer_generation,
        base_log_hash: required_header_string(header, "base_log_hash")?,
        policy_epoch: required_header_u64(header, "policy_epoch")?,
        membership_epoch: required_header_u64(header, "membership_epoch")?,
        schema_hash: required_header_string(header, "schema_hash")?,
        source_fence_token: required_header_u64(header, "source_fence_token")?,
        codec: required_header_string(header, "codec")?,
        created_at: required_header_string(header, "created_at")?,
    })
}

fn encode_log_segment_body(records: &[PersonalDbLogRecord]) -> Result<Vec<u8>> {
    let changeset_rows = records
        .iter()
        .map(|record| TableRow {
            key: record.log_index.to_be_bytes().to_vec(),
            value: record.encode(),
        })
        .collect();
    encode_writer_body_tables(&[
        WriterBodyTable {
            table_id: TABLE_PERSONALDB_GROUP_DESCRIPTOR,
            row_type_id: TABLE_PERSONALDB_GROUP_DESCRIPTOR,
            rows: Vec::new(),
        },
        WriterBodyTable {
            table_id: TABLE_PERSONALDB_CHANGESET,
            row_type_id: TABLE_PERSONALDB_CHANGESET,
            rows: changeset_rows,
        },
        WriterBodyTable {
            table_id: TABLE_PERSONALDB_WITNESS_RECORD,
            row_type_id: TABLE_PERSONALDB_WITNESS_RECORD,
            rows: Vec::new(),
        },
        WriterBodyTable {
            table_id: TABLE_PERSONALDB_OWNER_TRANSFER,
            row_type_id: TABLE_PERSONALDB_OWNER_TRANSFER,
            rows: Vec::new(),
        },
    ])
    .map_err(anyhow::Error::from)
}

fn decode_log_segment_body(input: &[u8]) -> Result<Vec<PersonalDbLogRecord>> {
    let mut records = Vec::new();
    for table in decode_writer_body_tables(input)? {
        if table.table_id != TABLE_PERSONALDB_CHANGESET {
            continue;
        }
        for row in table.rows {
            let (record, used) = PersonalDbLogRecord::decode(&row.value)?;
            if used != row.value.len() {
                return Err(anyhow!("personaldb log row has trailing bytes"));
            }
            records.push(record);
        }
    }
    Ok(records)
}

fn validate_log_segment_records(records: &[PersonalDbLogRecord]) -> Result<()> {
    if records.is_empty() {
        return Err(anyhow!(
            "personaldb log segment must contain at least one record"
        ));
    }
    validate_personaldb_log_chain(records)?;
    let _ = common_policy_epoch(records)?;
    let _ = common_membership_epoch(records)?;
    Ok(())
}

fn personaldb_log_segment_ref_prefix(tenant_id: i64, database_id: &str) -> Result<String> {
    if tenant_id < 0 {
        return Err(anyhow!(
            "personaldb log segment tenant id must be nonnegative"
        ));
    }
    require_safe_component(database_id, "database_id")?;
    Ok(format!(
        "{PERSONALDB_LOG_SEGMENT_REF_PREFIX}tenant:{tenant_id}:database:{database_id}:"
    ))
}

fn personaldb_log_segment_ref_name(
    tenant_id: i64,
    database_id: &str,
    start_log_index: u64,
    end_log_index: u64,
    segment_hash: &str,
) -> Result<String> {
    validate_hex32(segment_hash, "segment_hash")?;
    Ok(format!(
        "{}start:{start_log_index:020}:end:{end_log_index:020}:hash:{segment_hash}",
        personaldb_log_segment_ref_prefix(tenant_id, database_id)?
    ))
}

fn personaldb_log_segment_ref_scope(segment_ref: &str) -> Result<(i64, String)> {
    let tenant_marker = "tenant:";
    let database_marker = ":database:";
    let tenant_start = segment_ref
        .find(tenant_marker)
        .ok_or_else(|| anyhow!("personaldb log segment ref is missing tenant"))?
        + tenant_marker.len();
    let database_marker_offset = segment_ref[tenant_start..]
        .find(database_marker)
        .ok_or_else(|| anyhow!("personaldb log segment ref is missing database"))?
        + tenant_start;
    let tenant_id = segment_ref[tenant_start..database_marker_offset]
        .parse::<i64>()
        .map_err(|_| anyhow!("personaldb log segment ref tenant is invalid"))?;
    let database_start = database_marker_offset + database_marker.len();
    let database_end = segment_ref[database_start..]
        .find(':')
        .map(|offset| database_start + offset)
        .unwrap_or(segment_ref.len());
    let database_id = segment_ref[database_start..database_end].to_string();
    if !segment_ref.starts_with(&personaldb_log_segment_ref_prefix(tenant_id, &database_id)?) {
        return Err(anyhow!("personaldb log segment ref has invalid prefix"));
    }
    Ok((tenant_id, database_id))
}

fn require_safe_component(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.contains(':')
        || value.chars().any(char::is_control)
    {
        return Err(anyhow!("{field} is not a safe component"));
    }
    Ok(())
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(())
}

fn validate_header_matches_records(
    header: &PersonalDbLogSegmentHeader,
    records: &[PersonalDbLogRecord],
) -> Result<()> {
    let first = records.first().expect("validated nonempty");
    let last = records.last().expect("validated nonempty");
    if header.start_log_index != first.log_index || header.end_log_index != last.log_index {
        return Err(anyhow!(
            "personaldb log segment header range does not match body"
        ));
    }
    if header.base_log_hash != hex::encode(first.previous_log_hash) {
        return Err(anyhow!(
            "personaldb log segment base hash does not match body"
        ));
    }
    if header.policy_epoch != common_policy_epoch(records)? {
        return Err(anyhow!(
            "personaldb log segment policy epoch does not match body"
        ));
    }
    if header.membership_epoch != common_membership_epoch(records)? {
        return Err(anyhow!(
            "personaldb log segment membership epoch does not match body"
        ));
    }
    Ok(())
}

fn common_policy_epoch(records: &[PersonalDbLogRecord]) -> Result<u64> {
    let expected = records.first().expect("validated nonempty").policy_epoch;
    if records.iter().all(|record| record.policy_epoch == expected) {
        Ok(expected)
    } else {
        Err(anyhow!(
            "personaldb log segment records span multiple policy epochs"
        ))
    }
}

fn common_membership_epoch(records: &[PersonalDbLogRecord]) -> Result<u64> {
    let expected = records
        .first()
        .expect("validated nonempty")
        .membership_epoch;
    if records
        .iter()
        .all(|record| record.membership_epoch == expected)
    {
        Ok(expected)
    } else {
        Err(anyhow!(
            "personaldb log segment records span multiple membership epochs"
        ))
    }
}

fn record_hash_bounds(records: &[PersonalDbLogRecord]) -> (Hash32, Hash32) {
    let first = records
        .first()
        .map(|record| hash32(&record.encode()))
        .unwrap_or([0; 32]);
    let last = records
        .last()
        .map(|record| hash32(&record.encode()))
        .unwrap_or([0; 32]);
    (first, last)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn record(log_index: u64, previous_log_hash: Hash32) -> PersonalDbLogRecord {
        PersonalDbLogRecord::new(
            log_index,
            10,
            20,
            30,
            previous_log_hash,
            [1; 32],
            [2; 32],
            [3; 32],
            format!("log/payloads/by-index/{log_index:020}-payload.sqlite-changeset").into_bytes(),
            format!("log/certificates/{log_index:020}-cert.certificate.pb").into_bytes(),
            format!("personaldb-test-certificate:{log_index}").into_bytes(),
        )
    }

    #[tokio::test]
    async fn personaldb_log_segment_round_trips_with_common_envelope() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first = record(1, [0; 32]);
        let second = record(2, first.entry_hash);
        let records = vec![first, second];

        let segment_ref = write_personaldb_log_segment(
            &storage,
            PersonalDbLogSegmentWrite {
                tenant_id: 9,
                database_id: "db-alpha",
                schema_hash: [9; 32],
                source_fence_token: 77,
                records: &records,
            },
        )
        .await
        .unwrap();
        assert!(segment_ref.starts_with("personaldb_log_segment:tenant:9:database:db-alpha:"));
        assert!(segment_ref.contains("start:00000000000000000001:end:00000000000000000002:"));

        let decoded = read_personaldb_log_segment(&storage, &segment_ref)
            .await
            .unwrap();
        assert_eq!(decoded.header.tenant_id, "9");
        assert_eq!(decoded.header.database_id, "db-alpha");
        assert_eq!(decoded.header.start_log_index, 1);
        assert_eq!(decoded.header.end_log_index, 2);
        assert_eq!(decoded.header.source_fence_token, 77);
        assert_eq!(decoded.records, records);
    }

    #[tokio::test]
    async fn personaldb_log_segment_rejects_non_contiguous_chain() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first = record(1, [0; 32]);
        let second = record(3, first.entry_hash);
        let err = write_personaldb_log_segment(
            &storage,
            PersonalDbLogSegmentWrite {
                tenant_id: 9,
                database_id: "db-alpha",
                schema_hash: [9; 32],
                source_fence_token: 77,
                records: &[first, second],
            },
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("log index is not contiguous"));
    }

    #[tokio::test]
    async fn personaldb_log_segment_rejects_zero_source_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let record = record(1, [0; 32]);

        let err = write_personaldb_log_segment(
            &storage,
            PersonalDbLogSegmentWrite {
                tenant_id: 9,
                database_id: "db-alpha",
                schema_hash: [9; 32],
                source_fence_token: 0,
                records: &[record],
            },
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("source fence token must be nonzero")
        );
    }

    #[tokio::test]
    async fn personaldb_log_segment_footer_protects_body() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let record = record(1, [0; 32]);
        let segment_ref = write_personaldb_log_segment(
            &storage,
            PersonalDbLogSegmentWrite {
                tenant_id: 9,
                database_id: "db-alpha",
                schema_hash: [9; 32],
                source_fence_token: 77,
                records: &[record],
            },
        )
        .await
        .unwrap();
        let row = read_personaldb_data_locator_row(&storage, 9, "db-alpha", &segment_ref)
            .await
            .unwrap()
            .expect("segment CoreMeta row exists");
        let mut bytes = read_personaldb_data_locator_bytes(&storage, &row)
            .await
            .unwrap();
        bytes[crate::formats::WRITER_SEGMENT_FIXED_HEADER_LEN + 1] ^= 1;
        assert!(decode_personaldb_log_segment(&bytes).is_err());
    }
}
