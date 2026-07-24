use crate::{
    formats::{
        FileFamily, Hash32, decode_writer_segment, encode_writer_segment,
        encode_writer_segment_header, hash32, header_field_string,
        personaldb::RowIndexRecord,
        required_header_string, single_body_range_index,
        table::{TableRow, WriterBodyTable, decode_writer_body_tables, encode_writer_body_tables},
        unix_nanos_from_rfc3339,
    },
    personaldb_coremeta::{
        personaldb_payload_hash, read_personaldb_data_locator_bytes,
        read_personaldb_data_locator_row, write_personaldb_bytes_as_data_locator,
    },
    storage::Storage,
};
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

const PERSONALDB_ROW_INDEX_DATA_PREFIX: &str = "personaldb_row_index:";
const PERSONALDB_ROW_INDEX_KIND: &str = "row_index";
const TABLE_PERSONALDB_PROJECTION_PAGE: u16 = 0x0604;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersonalDbRowIndexHeader {
    pub tenant_id: String,
    pub database_id: String,
    pub generation: u64,
    pub source_hash: String,
    pub key_order: String,
    pub codec: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedPersonalDbRowIndex {
    pub header: PersonalDbRowIndexHeader,
    pub records: Vec<RowIndexRecord>,
}

#[derive(Debug, Clone)]
pub struct PersonalDbRowIndexWrite<'a> {
    pub tenant_id: i64,
    pub database_id: &'a str,
    pub generation: u64,
    pub source_hash: Hash32,
    pub records: &'a [RowIndexRecord],
}

pub async fn write_personaldb_row_index(
    storage: &Storage,
    input: PersonalDbRowIndexWrite<'_>,
) -> Result<String> {
    let mut records = input.records.to_vec();
    records.sort_by(compare_row_index_records);
    let body = encode_row_index_body(&records)?;
    let source_hash_hex = hex::encode(input.source_hash);
    let data_id = personaldb_row_index_data_id(
        input.tenant_id,
        input.database_id,
        input.generation,
        &source_hash_hex,
    )?;

    let header = PersonalDbRowIndexHeader {
        tenant_id: input.tenant_id.to_string(),
        database_id: input.database_id.to_string(),
        generation: input.generation,
        source_hash: hex::encode(input.source_hash),
        key_order: "database_id_table_hash_primary_key_hash".to_string(),
        codec: "writer-body-table-v1".to_string(),
        created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    };
    let (first_hash, last_hash) = record_hash_bounds(&records);
    let header_proto = encode_personaldb_row_index_header_proto(&data_id, &header);
    let range_index =
        single_body_range_index(body.len(), records.len() as u64, first_hash, last_hash)?;
    let encoded = encode_writer_segment(
        FileFamily::PersonalDbRowIndex,
        0,
        header_proto,
        &body,
        &range_index,
        records.len() as u64,
        first_hash,
        last_hash,
    )?;

    write_personaldb_bytes_as_data_locator(
        storage,
        input.tenant_id,
        input.database_id,
        &data_id,
        PERSONALDB_ROW_INDEX_KIND,
        input.generation,
        encoded.bytes.clone(),
        personaldb_payload_hash(&encoded.bytes),
        vec![source_hash_hex],
        format!(
            "personaldb-row-index:{}:{}:{}",
            input.tenant_id, input.database_id, input.generation
        ),
    )
    .await?;
    Ok(data_id)
}

pub async fn read_personaldb_row_index(
    storage: &Storage,
    row_index_data_id: &str,
) -> Result<DecodedPersonalDbRowIndex> {
    let (tenant_id, database_id, _generation, _source_hash) =
        parse_personaldb_row_index_data_id(row_index_data_id)?;
    let row = read_personaldb_data_locator_row(storage, tenant_id, &database_id, row_index_data_id)
        .await?
        .ok_or_else(|| anyhow!("personaldb row index CoreMeta row is missing"))?;
    if row.data_kind != PERSONALDB_ROW_INDEX_KIND {
        return Err(anyhow!("personaldb row index CoreMeta row kind mismatch"));
    }
    let bytes = read_personaldb_data_locator_bytes(storage, &row).await?;
    decode_personaldb_row_index(&bytes)
}

pub fn decode_personaldb_row_index(bytes: &[u8]) -> Result<DecodedPersonalDbRowIndex> {
    let segment = decode_writer_segment(bytes, FileFamily::PersonalDbRowIndex)?;
    let header = decode_personaldb_row_index_header_proto(&segment.header)?;
    let records = decode_row_index_body(segment.body)?;
    ensure_sorted(&records)?;
    Ok(DecodedPersonalDbRowIndex { header, records })
}

fn encode_personaldb_row_index_header_proto(
    logical_file_id: &str,
    header: &PersonalDbRowIndexHeader,
) -> Vec<u8> {
    encode_writer_segment_header(
        "anvil.personaldb.row_index_header.v1",
        logical_file_id,
        FileFamily::PersonalDbRowIndex,
        header.generation,
        None,
        None,
        unix_nanos_from_rfc3339(&header.created_at),
        vec![
            header_field_string("tenant_id", header.tenant_id.clone()),
            header_field_string("database_id", header.database_id.clone()),
            header_field_string("source_hash", header.source_hash.clone()),
            header_field_string("key_order", header.key_order.clone()),
            header_field_string("codec", header.codec.clone()),
            header_field_string("created_at", header.created_at.clone()),
        ],
    )
}

fn decode_personaldb_row_index_header_proto(
    header: &crate::formats::WriterSegmentHeaderProto,
) -> Result<PersonalDbRowIndexHeader> {
    Ok(PersonalDbRowIndexHeader {
        tenant_id: required_header_string(header, "tenant_id")?,
        database_id: required_header_string(header, "database_id")?,
        generation: header.writer_generation,
        source_hash: required_header_string(header, "source_hash")?,
        key_order: required_header_string(header, "key_order")?,
        codec: required_header_string(header, "codec")?,
        created_at: required_header_string(header, "created_at")?,
    })
}

pub fn personaldb_row_index_data_id(
    tenant_id: i64,
    database_id: &str,
    generation: u64,
    source_hash: &str,
) -> Result<String> {
    if tenant_id < 0 {
        return Err(anyhow!(
            "personaldb row index tenant id must be nonnegative"
        ));
    }
    require_safe_component(database_id, "database_id")?;
    validate_hex32(source_hash, "source_hash")?;
    Ok(format!(
        "{PERSONALDB_ROW_INDEX_DATA_PREFIX}tenant:{tenant_id}:database:{database_id}:generation:{generation:020}:source:{source_hash}"
    ))
}

fn parse_personaldb_row_index_data_id(data_id: &str) -> Result<(i64, String, u64, String)> {
    let Some(rest) = data_id.strip_prefix(PERSONALDB_ROW_INDEX_DATA_PREFIX) else {
        return Err(anyhow!("personaldb row index data id has invalid prefix"));
    };
    let parts: Vec<&str> = rest.split(':').collect();
    if parts.len() != 8
        || parts[0] != "tenant"
        || parts[2] != "database"
        || parts[4] != "generation"
        || parts[6] != "source"
    {
        return Err(anyhow!("personaldb row index data id has invalid shape"));
    }
    let tenant_id = parts[1]
        .parse::<i64>()
        .map_err(|_| anyhow!("personaldb row index tenant id is invalid"))?;
    if tenant_id < 0 {
        return Err(anyhow!(
            "personaldb row index tenant id must be nonnegative"
        ));
    }
    require_safe_component(parts[3], "database_id")?;
    let generation = parts[5]
        .parse::<u64>()
        .map_err(|_| anyhow!("personaldb row index generation is invalid"))?;
    validate_hex32(parts[7], "source_hash")?;
    Ok((
        tenant_id,
        parts[3].to_string(),
        generation,
        parts[7].to_string(),
    ))
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

fn encode_row_index_body(records: &[RowIndexRecord]) -> Result<Vec<u8>> {
    let rows = records
        .iter()
        .map(|record| TableRow {
            key: row_index_key(record),
            value: record.encode(),
        })
        .collect::<Vec<_>>();
    encode_writer_body_tables(&[WriterBodyTable {
        table_id: TABLE_PERSONALDB_PROJECTION_PAGE,
        row_type_id: TABLE_PERSONALDB_PROJECTION_PAGE,
        rows,
    }])
    .map_err(anyhow::Error::from)
}

fn decode_row_index_body(input: &[u8]) -> Result<Vec<RowIndexRecord>> {
    let mut records = Vec::new();
    for table in decode_writer_body_tables(input)? {
        if table.table_id != TABLE_PERSONALDB_PROJECTION_PAGE {
            return Err(anyhow!(
                "personaldb row index segment contains unexpected table {:04x}",
                table.table_id
            ));
        }
        for row in table.rows {
            let (record, used) = RowIndexRecord::decode(&row.value)?;
            if used != row.value.len() {
                return Err(anyhow!("personaldb row index row has trailing bytes"));
            }
            if row.key != row_index_key(&record) {
                return Err(anyhow!(
                    "personaldb row index row key does not match encoded record"
                ));
            }
            records.push(record);
        }
    }
    Ok(records)
}

fn row_index_key(record: &RowIndexRecord) -> Vec<u8> {
    let mut key = Vec::new();
    key.extend_from_slice(&record.database_id);
    key.push(0);
    key.extend_from_slice(&record.table_name_hash);
    key.extend_from_slice(&record.primary_key_hash);
    key
}

fn ensure_sorted(records: &[RowIndexRecord]) -> Result<()> {
    if records
        .windows(2)
        .all(|pair| compare_row_index_records(&pair[0], &pair[1]).is_le())
    {
        Ok(())
    } else {
        Err(anyhow!(
            "personaldb row index records are not sorted by database, table, and primary key"
        ))
    }
}

fn compare_row_index_records(left: &RowIndexRecord, right: &RowIndexRecord) -> std::cmp::Ordering {
    left.database_id
        .cmp(&right.database_id)
        .then_with(|| left.table_name_hash.cmp(&right.table_name_hash))
        .then_with(|| left.primary_key_hash.cmp(&right.primary_key_hash))
}

fn record_hash_bounds(records: &[RowIndexRecord]) -> (Hash32, Hash32) {
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

    fn row(primary_hash: u8) -> RowIndexRecord {
        RowIndexRecord::new(
            b"db-alpha".to_vec(),
            [3; 32],
            [primary_hash; 32],
            b"invoice".to_vec(),
            format!("invoice-{primary_hash}").into_bytes(),
            Vec::new(),
            b"user:alice".to_vec(),
            b"org:alpha".to_vec(),
            u64::from(primary_hash),
            9,
            [4; 32],
            1_717_000_000 + i64::from(primary_hash),
        )
    }

    #[tokio::test]
    async fn personaldb_row_index_round_trips_sorted_records() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let records = vec![row(9), row(1), row(5)];
        let row_index_data_id = write_personaldb_row_index(
            &storage,
            PersonalDbRowIndexWrite {
                tenant_id: 4,
                database_id: "db-alpha",
                generation: 12,
                source_hash: [7; 32],
                records: &records,
            },
        )
        .await
        .unwrap();
        assert!(row_index_data_id.starts_with(
            "personaldb_row_index:tenant:4:database:db-alpha:generation:00000000000000000012:"
        ));

        let decoded = read_personaldb_row_index(&storage, &row_index_data_id)
            .await
            .unwrap();
        assert_eq!(decoded.header.tenant_id, "4");
        assert_eq!(decoded.header.database_id, "db-alpha");
        assert_eq!(decoded.header.generation, 12);
        assert_eq!(decoded.records.len(), 3);
        assert_eq!(decoded.records[0].primary_key_hash, [1; 32]);
        assert_eq!(decoded.records[2].primary_key_hash, [9; 32]);
    }

    #[tokio::test]
    async fn personaldb_row_index_footer_protects_body() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let row_index_data_id = write_personaldb_row_index(
            &storage,
            PersonalDbRowIndexWrite {
                tenant_id: 4,
                database_id: "db-alpha",
                generation: 12,
                source_hash: [7; 32],
                records: &[row(1)],
            },
        )
        .await
        .unwrap();
        let row = read_personaldb_data_locator_row(&storage, 4, "db-alpha", &row_index_data_id)
            .await
            .unwrap()
            .expect("row index CoreMeta row exists");
        let mut bytes = read_personaldb_data_locator_bytes(&storage, &row)
            .await
            .unwrap();
        bytes[crate::formats::WRITER_SEGMENT_FIXED_HEADER_LEN + 1] ^= 1;
        assert!(decode_personaldb_row_index(&bytes).is_err());
    }

    #[test]
    fn personaldb_row_index_rejects_unsorted_body() {
        let records = vec![row(9), row(1)];
        assert!(encode_row_index_body(&records).is_err());
        assert!(ensure_sorted(&records).is_err());
    }
}
