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
use anyhow::{Result, anyhow};

const REGISTRY_SEGMENT_REF_PREFIX: &str = "registry_segment:";
const REGISTRY_SEGMENT_CATALOG_FAMILY: &str = "registry";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistrySegmentHeader {
    pub registry_kind: String,
    pub namespace: String,
    pub repository: String,
    pub generation: u64,
    pub record_kind: String,
    pub source_cursor: u64,
    pub key_order: String,
    pub codec: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistrySegmentRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct RegistrySegmentWrite<'a> {
    pub registry_kind: &'a str,
    pub namespace: &'a str,
    pub repository: &'a str,
    pub generation: u64,
    pub record_kind: &'a str,
    pub source_cursor: u64,
    pub boundary_values: &'a [CoreBoundaryValue],
    pub records: &'a [RegistrySegmentRecord],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedRegistrySegment {
    pub header: RegistrySegmentHeader,
    pub records: Vec<RegistrySegmentRecord>,
}

pub async fn write_registry_segment(
    storage: &Storage,
    write: RegistrySegmentWrite<'_>,
) -> Result<String> {
    let mut records = write.records.to_vec();
    records.sort_by(|left, right| left.key.cmp(&right.key));
    let table_id = registry_table_id(write.record_kind)?;
    let table_rows = records
        .iter()
        .map(|record| TableRow {
            key: record.key.clone(),
            value: record.value.clone(),
        })
        .collect::<Vec<_>>();
    let body = encode_writer_body_tables(&registry_writer_tables(table_id, table_rows))?;
    let (first_hash, last_hash) = record_hash_bounds(&records);
    let segment_hash = hash32(&body);
    let ref_name = registry_segment_ref_name(
        write.registry_kind,
        write.namespace,
        write.repository,
        write.generation,
        write.record_kind,
        &hex::encode(segment_hash),
    )?;
    let logical_file_id = canonical_logical_file_id(
        WriterFamily::Registry,
        write.generation,
        &ref_name,
        &segment_hash,
    );
    let header = RegistrySegmentHeader {
        registry_kind: write.registry_kind.to_string(),
        namespace: write.namespace.to_string(),
        repository: write.repository.to_string(),
        generation: write.generation,
        record_kind: write.record_kind.to_string(),
        source_cursor: write.source_cursor,
        key_order: "registry_key".to_string(),
        codec: "writer-body-table-v1".to_string(),
        created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    };
    let header_proto = encode_registry_header_proto(&logical_file_id, &header);
    let range_index =
        single_body_range_index(body.len(), records.len() as u64, first_hash, last_hash)?;
    let built_segment = build_writer_segment_logical_file(WriterSegmentBuildInput {
        file_family: FileFamily::RegistrySegment,
        writer_family: WriterFamily::Registry,
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
            "registry-segment:{}:{}:{}:{}:{}",
            write.registry_kind,
            write.namespace,
            write.repository,
            write.record_kind,
            write.generation
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
        .await?;
    let object_ref = receipt
        .written_object_refs
        .first()
        .cloned()
        .ok_or_else(|| anyhow!("CoreFormatWriter returned no registry object"))?;
    write_writer_segment_catalog_record(
        storage,
        &WriterSegmentCatalogRecord {
            family: REGISTRY_SEGMENT_CATALOG_FAMILY.to_string(),
            scope: registry_segment_scope(
                write.registry_kind,
                write.namespace,
                write.repository,
                write.record_kind,
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
    .await?;
    Ok(ref_name)
}

pub async fn read_registry_segment(
    storage: &Storage,
    segment_ref: &str,
) -> Result<DecodedRegistrySegment> {
    let parsed = parse_registry_segment_ref(segment_ref)?;
    let record = read_writer_segment_catalog_record(
        storage,
        REGISTRY_SEGMENT_CATALOG_FAMILY,
        &registry_segment_scope(
            &parsed.registry_kind,
            &parsed.namespace,
            &parsed.repository,
            &parsed.record_kind,
        )?,
        parsed.generation,
        segment_ref,
    )
    .await?
    .ok_or_else(|| anyhow!("registry segment catalog row is missing"))?;
    let store = CoreStore::new(storage.clone()).await?;
    let bytes = store
        .get_blob(GetBlob {
            object_ref: crate::core_store::decode_core_object_ref_target(
                &record.core_object_ref_target,
            )?,
        })
        .await?;
    decode_registry_segment(&bytes)
}

pub fn decode_registry_segment(bytes: &[u8]) -> Result<DecodedRegistrySegment> {
    let segment = decode_writer_segment(bytes, FileFamily::RegistrySegment)?;
    let header = decode_registry_header_proto(&segment.header)?;
    let mut records = Vec::new();
    for table in decode_writer_body_tables(segment.body)? {
        for row in table.rows {
            records.push(RegistrySegmentRecord {
                key: row.key,
                value: row.value,
            });
        }
    }
    Ok(DecodedRegistrySegment { header, records })
}

fn encode_registry_header_proto(logical_file_id: &str, header: &RegistrySegmentHeader) -> Vec<u8> {
    encode_writer_segment_header(
        "anvil.registry.segment_header.v1",
        logical_file_id,
        FileFamily::RegistrySegment,
        header.generation,
        None,
        None,
        unix_nanos_from_rfc3339(&header.created_at),
        vec![
            header_field_string("registry_kind", header.registry_kind.clone()),
            header_field_string("namespace", header.namespace.clone()),
            header_field_string("repository", header.repository.clone()),
            header_field_string("record_kind", header.record_kind.clone()),
            header_field_u64("source_cursor", header.source_cursor),
            header_field_string("key_order", header.key_order.clone()),
            header_field_string("created_at", header.created_at.clone()),
            header_field_string("codec", header.codec.clone()),
        ],
    )
}

fn decode_registry_header_proto(
    header: &crate::formats::WriterSegmentHeaderProto,
) -> Result<RegistrySegmentHeader> {
    Ok(RegistrySegmentHeader {
        registry_kind: required_header_string(header, "registry_kind")?,
        namespace: required_header_string(header, "namespace")?,
        repository: required_header_string(header, "repository")?,
        generation: header.writer_generation,
        record_kind: required_header_string(header, "record_kind")?,
        source_cursor: required_header_u64(header, "source_cursor")?,
        key_order: required_header_string(header, "key_order")?,
        created_at: required_header_string(header, "created_at")?,
        codec: required_header_string(header, "codec")?,
    })
}

fn registry_segment_ref_name(
    registry_kind: &str,
    namespace: &str,
    repository: &str,
    generation: u64,
    record_kind: &str,
    body_hash: &str,
) -> Result<String> {
    validate_segment_component(registry_kind, "registry kind")?;
    validate_segment_component(namespace, "registry namespace")?;
    validate_segment_component(repository, "registry repository")?;
    validate_segment_component(record_kind, "registry record kind")?;
    Ok(format!(
        "{REGISTRY_SEGMENT_REF_PREFIX}registry:{registry_kind}:namespace:{namespace}:repository:{repository}:kind:{record_kind}:generation:{generation:020}:hash:{body_hash}"
    ))
}

#[derive(Debug, Clone)]
struct ParsedRegistrySegmentRef {
    registry_kind: String,
    namespace: String,
    repository: String,
    record_kind: String,
    generation: u64,
}

fn parse_registry_segment_ref(segment_ref: &str) -> Result<ParsedRegistrySegmentRef> {
    let parts = segment_ref.split(':').collect::<Vec<_>>();
    if parts.len() != 13
        || parts[0] != "registry_segment"
        || parts[1] != "registry"
        || parts[3] != "namespace"
        || parts[5] != "repository"
        || parts[7] != "kind"
        || parts[9] != "generation"
        || parts[11] != "hash"
    {
        return Err(anyhow!("registry segment ref is malformed"));
    }
    validate_segment_component(parts[2], "registry kind")?;
    validate_segment_component(parts[4], "registry namespace")?;
    validate_segment_component(parts[6], "registry repository")?;
    validate_segment_component(parts[8], "registry record kind")?;
    let generation = parts[10]
        .parse::<u64>()
        .map_err(|_| anyhow!("registry segment ref generation is invalid"))?;
    if parts[12].is_empty() {
        return Err(anyhow!("registry segment ref hash is empty"));
    }
    Ok(ParsedRegistrySegmentRef {
        registry_kind: parts[2].to_string(),
        namespace: parts[4].to_string(),
        repository: parts[6].to_string(),
        record_kind: parts[8].to_string(),
        generation,
    })
}

fn registry_segment_scope(
    registry_kind: &str,
    namespace: &str,
    repository: &str,
    record_kind: &str,
) -> Result<String> {
    validate_segment_component(registry_kind, "registry kind")?;
    validate_segment_component(namespace, "registry namespace")?;
    validate_segment_component(repository, "registry repository")?;
    validate_segment_component(record_kind, "registry record kind")?;
    Ok(format!(
        "{registry_kind}/{namespace}/{repository}/{record_kind}"
    ))
}

fn validate_segment_component(value: &str, field: &'static str) -> Result<()> {
    if value.trim().is_empty()
        || value.as_bytes().iter().any(|byte| {
            !(byte.is_ascii_alphanumeric() || matches!(*byte, b'.' | b'_' | b'-' | b'/'))
        })
    {
        return Err(anyhow!("{field} is invalid"));
    }
    Ok(())
}

fn registry_table_id(record_kind: &str) -> Result<u16> {
    match record_kind {
        "namespace" => Ok(0x0701),
        "repository" | "package" => Ok(0x0702),
        "version" | "manifest" => Ok(0x0703),
        "blob" => Ok(0x0704),
        "tag" | "dist_tag" | "ref" => Ok(0x0705),
        "credential" | "credential_policy" => Ok(0x0706),
        other => Err(anyhow!(
            "registry record kind {other} does not map to the RFC writer table registry"
        )),
    }
}

fn registry_writer_tables(
    active_table_id: u16,
    active_rows: Vec<TableRow>,
) -> Vec<WriterBodyTable> {
    [0x0701, 0x0702, 0x0703, 0x0704, 0x0705]
        .into_iter()
        .chain(std::iter::once(0x0706))
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

fn record_hash_bounds(records: &[RegistrySegmentRecord]) -> (Hash32, Hash32) {
    let first = records.first().map(registry_record_hash).unwrap_or([0; 32]);
    let last = records.last().map(registry_record_hash).unwrap_or([0; 32]);
    (first, last)
}

fn registry_record_hash(record: &RegistrySegmentRecord) -> Hash32 {
    let mut bytes = Vec::with_capacity(record.key.len() + record.value.len() + 16);
    bytes.extend_from_slice(&(record.key.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&record.key);
    bytes.extend_from_slice(&(record.value.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&record.value);
    hash32(&bytes)
}
