use crate::{
    core_store::{CoreBoundaryValue, CorePipelinePolicy, CoreStore, CoreTraceContext, GetBlob},
    formats::{
        FileFamily, Hash32, decode_writer_segment, encode_writer_segment_header, hash32,
        header_field_string, header_field_u64, required_header_string, required_header_u64,
        single_body_range_index,
        table::{TableRow, WriterBodyTable, decode_writer_body_tables, encode_writer_body_tables},
        unix_nanos_from_rfc3339,
        writer::{
            WriterBuildOutput, WriterFamily, WriterSegmentBuildInput,
            build_writer_segment_logical_file, canonical_logical_file_id,
        },
    },
    storage::Storage,
    writer_segment_catalog::{
        WriterSegmentCatalogRecord, read_writer_segment_catalog_record,
        write_writer_segment_catalog_record,
    },
};
use anyhow::{Context, Result, anyhow};

const MESH_CONTROL_SEGMENT_REF_PREFIX: &str = "mesh_control_segment:";
const MESH_CONTROL_SEGMENT_CATALOG_FAMILY: &str = "mesh_control";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshControlSegmentHeader {
    pub mesh_id: String,
    pub stream_family: String,
    pub partition: String,
    pub generation: u64,
    pub event_kind: String,
    pub source_cursor: u64,
    pub placement_epoch: u64,
    pub key_order: String,
    pub codec: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshControlSegmentRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct MeshControlSegmentWrite<'a> {
    pub mesh_id: &'a str,
    pub stream_family: &'a str,
    pub partition: &'a str,
    pub generation: u64,
    pub event_kind: &'a str,
    pub source_cursor: u64,
    pub placement_epoch: u64,
    pub boundary_values: &'a [CoreBoundaryValue],
    pub records: &'a [MeshControlSegmentRecord],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedMeshControlSegment {
    pub header: MeshControlSegmentHeader,
    pub records: Vec<MeshControlSegmentRecord>,
}

pub async fn write_mesh_control_segment(
    storage: &Storage,
    write: MeshControlSegmentWrite<'_>,
) -> Result<String> {
    let mut records = write.records.to_vec();
    records.sort_by(|left, right| left.key.cmp(&right.key));
    let table_id = mesh_control_table_id(write.event_kind);
    let table_rows = records
        .iter()
        .map(|record| TableRow {
            key: record.key.clone(),
            value: record.value.clone(),
        })
        .collect::<Vec<_>>();
    let body = encode_writer_body_tables(&mesh_control_writer_tables(table_id, table_rows))?;
    let (first_hash, last_hash) = record_hash_bounds(&records);
    let segment_hash = hash32(&body);
    let ref_name = mesh_control_segment_ref_name(
        write.mesh_id,
        write.stream_family,
        write.partition,
        write.generation,
        write.event_kind,
        &hex::encode(segment_hash),
    )?;
    let logical_file_id = canonical_logical_file_id(
        WriterFamily::MeshControl,
        write.generation,
        &ref_name,
        &segment_hash,
    );
    let header = MeshControlSegmentHeader {
        mesh_id: write.mesh_id.to_string(),
        stream_family: write.stream_family.to_string(),
        partition: write.partition.to_string(),
        generation: write.generation,
        event_kind: write.event_kind.to_string(),
        source_cursor: write.source_cursor,
        placement_epoch: write.placement_epoch,
        key_order: "mesh_control_key".to_string(),
        codec: "writer-body-table-v1".to_string(),
        created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    };
    let header_proto = encode_mesh_control_header_proto(&logical_file_id, &header);
    let range_index =
        single_body_range_index(body.len(), records.len() as u64, first_hash, last_hash)?;
    let built_segment = build_writer_segment_logical_file(WriterSegmentBuildInput {
        file_family: FileFamily::MeshControlSegment,
        writer_family: WriterFamily::MeshControl,
        writer_generation: write.generation,
        logical_file_id,
        header_proto,
        body,
        range_index,
        record_count: records.len() as u64,
        first_record_hash: first_hash,
        last_record_hash: last_hash,
        boundary_values: write.boundary_values.to_vec(),
        mutation_id: format!(
            "mesh-control-segment:{}:{}:{}:{}:{}",
            write.mesh_id, write.stream_family, write.partition, write.event_kind, write.generation
        ),
        region_id: "local".to_string(),
        pipeline_policy: CorePipelinePolicy::default(),
        trace_context: CoreTraceContext::default(),
    })?;

    let store = CoreStore::new(storage.clone()).await?;
    let receipt = store
        .write_format_build_output(WriterBuildOutput {
            logical_files: vec![built_segment.logical_file],
            core_meta_mutations: Vec::new(),
            core_meta_root_publications: Vec::new(),
        })
        .await
        .with_context(|| format!("write mesh-control logical file output for {ref_name}"))?;
    let object_ref = receipt
        .written_object_refs
        .first()
        .cloned()
        .ok_or_else(|| anyhow!("CoreFormatWriter returned no mesh control object"))?;
    write_writer_segment_catalog_record(
        storage,
        &WriterSegmentCatalogRecord {
            family: MESH_CONTROL_SEGMENT_CATALOG_FAMILY.to_string(),
            scope: mesh_control_segment_scope(
                write.mesh_id,
                write.stream_family,
                write.partition,
                write.event_kind,
            )?,
            segment_ref: ref_name.clone(),
            core_object_ref_target: crate::core_store::encode_core_object_ref_target(&object_ref)?,
            segment_hash: hex::encode(segment_hash),
            segment_length: object_ref.logical_size,
            generation: write.generation,
            source_cursor: write.source_cursor,
            created_at_unix_nanos: unix_nanos_from_rfc3339(&header.created_at),
        },
        &[],
    )
    .await
    .with_context(|| format!("write mesh-control segment catalog row for {ref_name}"))?;
    Ok(ref_name)
}

pub async fn read_mesh_control_segment(
    storage: &Storage,
    segment_ref: &str,
) -> Result<DecodedMeshControlSegment> {
    let parsed = parse_mesh_control_segment_ref(segment_ref)?;
    let record = read_writer_segment_catalog_record(
        storage,
        MESH_CONTROL_SEGMENT_CATALOG_FAMILY,
        &mesh_control_segment_scope(
            &parsed.mesh_id,
            &parsed.stream_family,
            &parsed.partition,
            &parsed.event_kind,
        )?,
        parsed.generation,
        segment_ref,
    )
    .await?
    .ok_or_else(|| anyhow!("mesh control segment catalog row is missing"))?;
    let store = CoreStore::new(storage.clone()).await?;
    let bytes = store
        .get_blob(GetBlob {
            object_ref: crate::core_store::decode_core_object_ref_target(
                &record.core_object_ref_target,
            )?,
        })
        .await?;
    decode_mesh_control_segment(&bytes)
}

pub fn decode_mesh_control_segment(bytes: &[u8]) -> Result<DecodedMeshControlSegment> {
    let segment = decode_writer_segment(bytes, FileFamily::MeshControlSegment)?;
    let header = decode_mesh_control_header_proto(&segment.header)?;
    let mut records = Vec::new();
    for table in decode_writer_body_tables(segment.body)? {
        for row in table.rows {
            records.push(MeshControlSegmentRecord {
                key: row.key,
                value: row.value,
            });
        }
    }
    Ok(DecodedMeshControlSegment { header, records })
}

fn encode_mesh_control_header_proto(
    logical_file_id: &str,
    header: &MeshControlSegmentHeader,
) -> Vec<u8> {
    encode_writer_segment_header(
        "anvil.mesh_control.segment_header.v1",
        logical_file_id,
        FileFamily::MeshControlSegment,
        header.generation,
        None,
        None,
        unix_nanos_from_rfc3339(&header.created_at),
        vec![
            header_field_string("mesh_id", header.mesh_id.clone()),
            header_field_string("stream_family", header.stream_family.clone()),
            header_field_string("partition", header.partition.clone()),
            header_field_string("event_kind", header.event_kind.clone()),
            header_field_u64("source_cursor", header.source_cursor),
            header_field_u64("placement_epoch", header.placement_epoch),
            header_field_string("key_order", header.key_order.clone()),
            header_field_string("created_at", header.created_at.clone()),
            header_field_string("codec", header.codec.clone()),
        ],
    )
}

fn decode_mesh_control_header_proto(
    header: &crate::formats::WriterSegmentHeaderProto,
) -> Result<MeshControlSegmentHeader> {
    Ok(MeshControlSegmentHeader {
        mesh_id: required_header_string(header, "mesh_id")?,
        stream_family: required_header_string(header, "stream_family")?,
        partition: required_header_string(header, "partition")?,
        generation: header.writer_generation,
        event_kind: required_header_string(header, "event_kind")?,
        source_cursor: required_header_u64(header, "source_cursor")?,
        placement_epoch: required_header_u64(header, "placement_epoch")?,
        key_order: required_header_string(header, "key_order")?,
        created_at: required_header_string(header, "created_at")?,
        codec: required_header_string(header, "codec")?,
    })
}

fn mesh_control_segment_ref_name(
    mesh_id: &str,
    stream_family: &str,
    partition: &str,
    generation: u64,
    event_kind: &str,
    body_hash: &str,
) -> Result<String> {
    validate_segment_component(mesh_id, "mesh id")?;
    validate_segment_component(stream_family, "mesh control stream family")?;
    validate_segment_component(partition, "mesh control partition")?;
    validate_segment_component(event_kind, "mesh control event kind")?;
    Ok(format!(
        "{MESH_CONTROL_SEGMENT_REF_PREFIX}mesh:{mesh_id}:stream:{stream_family}:partition:{partition}:kind:{event_kind}:generation:{generation:020}:hash:{body_hash}"
    ))
}

#[derive(Debug, Clone)]
struct ParsedMeshControlSegmentRef {
    mesh_id: String,
    stream_family: String,
    partition: String,
    event_kind: String,
    generation: u64,
}

fn parse_mesh_control_segment_ref(segment_ref: &str) -> Result<ParsedMeshControlSegmentRef> {
    let parts = segment_ref.split(':').collect::<Vec<_>>();
    if parts.len() != 13
        || parts[0] != "mesh_control_segment"
        || parts[1] != "mesh"
        || parts[3] != "stream"
        || parts[5] != "partition"
        || parts[7] != "kind"
        || parts[9] != "generation"
        || parts[11] != "hash"
    {
        return Err(anyhow!("mesh control segment ref is malformed"));
    }
    validate_segment_component(parts[2], "mesh id")?;
    validate_segment_component(parts[4], "mesh control stream family")?;
    validate_segment_component(parts[6], "mesh control partition")?;
    validate_segment_component(parts[8], "mesh control event kind")?;
    let generation = parts[10]
        .parse::<u64>()
        .map_err(|_| anyhow!("mesh control segment ref generation is invalid"))?;
    if parts[12].is_empty() {
        return Err(anyhow!("mesh control segment ref hash is empty"));
    }
    Ok(ParsedMeshControlSegmentRef {
        mesh_id: parts[2].to_string(),
        stream_family: parts[4].to_string(),
        partition: parts[6].to_string(),
        event_kind: parts[8].to_string(),
        generation,
    })
}

fn mesh_control_segment_scope(
    mesh_id: &str,
    stream_family: &str,
    partition: &str,
    event_kind: &str,
) -> Result<String> {
    validate_segment_component(mesh_id, "mesh id")?;
    validate_segment_component(stream_family, "mesh control stream family")?;
    validate_segment_component(partition, "mesh control partition")?;
    validate_segment_component(event_kind, "mesh control event kind")?;
    Ok(format!(
        "{mesh_id}/{stream_family}/{partition}/{event_kind}"
    ))
}

fn validate_segment_component(value: &str, field: &'static str) -> Result<()> {
    if value.trim().is_empty()
        || value
            .as_bytes()
            .iter()
            .any(|byte| !(byte.is_ascii_alphanumeric() || matches!(*byte, b'.' | b'_' | b'-')))
    {
        return Err(anyhow!("{field} is invalid"));
    }
    Ok(())
}

fn mesh_control_table_id(event_kind: &str) -> u16 {
    let kind = event_kind.to_ascii_lowercase();
    if kind.contains("region") {
        0x0801
    } else if kind.contains("cell") {
        0x0802
    } else if kind.contains("node") {
        0x0803
    } else if kind.contains("partition") {
        0x0804
    } else if kind.contains("root") || kind.contains("owner") {
        0x0805
    } else if kind.contains("repair") {
        0x0807
    } else if kind.contains("anti_entropy") || kind.contains("checkpoint") {
        0x0808
    } else {
        0x0806
    }
}

fn mesh_control_writer_tables(
    active_table_id: u16,
    active_rows: Vec<TableRow>,
) -> Vec<WriterBodyTable> {
    [0x0801, 0x0802, 0x0803, 0x0804, 0x0805, 0x0806]
        .into_iter()
        .chain([0x0807, 0x0808])
        .map(|table_id| WriterBodyTable {
            table_id,
            row_type_id: table_id,
            rows: if table_id == active_table_id {
                active_rows.clone()
            } else {
                Vec::new()
            },
        })
        .collect()
}

fn record_hash_bounds(records: &[MeshControlSegmentRecord]) -> (Hash32, Hash32) {
    let first = records
        .first()
        .map(mesh_control_record_hash)
        .unwrap_or([0; 32]);
    let last = records
        .last()
        .map(mesh_control_record_hash)
        .unwrap_or([0; 32]);
    (first, last)
}

fn mesh_control_record_hash(record: &MeshControlSegmentRecord) -> Hash32 {
    let mut bytes = Vec::with_capacity(record.key.len() + record.value.len() + 16);
    bytes.extend_from_slice(&(record.key.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&record.key);
    bytes.extend_from_slice(&(record.value.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&record.value);
    hash32(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn mesh_control_segment_writes_and_reads_through_corestore() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let segment_ref = write_mesh_control_segment(
            &storage,
            MeshControlSegmentWrite {
                mesh_id: "mesh-a",
                stream_family: "bucket_locator",
                partition: "00a7",
                generation: 1,
                event_kind: "upsert",
                source_cursor: 1,
                placement_epoch: 1,
                boundary_values: &[],
                records: &[MeshControlSegmentRecord {
                    key: b"bucket-a".to_vec(),
                    value: b"payload".to_vec(),
                }],
            },
        )
        .await
        .unwrap();

        let decoded = read_mesh_control_segment(&storage, &segment_ref)
            .await
            .unwrap();
        assert_eq!(decoded.header.mesh_id, "mesh-a");
        assert_eq!(decoded.header.stream_family, "bucket_locator");
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.records[0].value, b"payload");
    }
}
