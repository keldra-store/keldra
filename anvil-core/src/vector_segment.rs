use crate::{
    core_store::{
        CoreBoundaryValue, CoreMutationPrecondition, CoreObjectRef, CorePipelinePolicy, CoreStore,
        CoreTraceContext, GetBlob,
    },
    formats::{
        FileFamily, Hash32, decode_writer_segment, encode_writer_segment_header, hash32,
        header_field_string, header_field_u64, optional_header_string, required_header_string,
        required_header_u64, single_body_range_index,
        table::{TableRow, WriterBodyTable, decode_writer_body_tables, encode_writer_body_tables},
        unix_nanos_from_rfc3339,
        vector::{
            HnswGraph, VECTOR_BODY_HEADER_LEN, VECTOR_RECORD_LEN, VectorBodyHeader, VectorMetric,
            VectorModality, VectorPayload, VectorRecord, VectorSearchResult, vector_score,
        },
        writer::{
            WriterBuildOutput, WriterFamily, WriterSegmentBuildInput,
            build_writer_segment_logical_file, canonical_logical_file_id,
        },
    },
    index_coremeta::{self, IndexSegmentCoreMetaRecord},
    storage::Storage,
    vector_hnsw::{build_hnsw_graph_for_entries, validate_hnsw_graph},
    writer_segment_catalog::{WriterSegmentCatalogRecord, write_writer_segment_catalog_record},
    writer_segment_range::RangeAddressedWriterSegment,
};
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const VECTOR_SEGMENT_REF_PREFIX: &str = "vector_segment:";
const VECTOR_ANN_KIND_HNSW_V1: u16 = 1;
const VECTOR_BLOCK_LEN_LEN: usize = 8;
const VECTOR_BLOCK_COUNT_LEN: usize = 8;
const VECTOR_BLOCK_CRC_LEN: usize = 4;
const VECTOR_ANN_KIND_LEN: usize = 2;
const VECTOR_ANN_LEN_LEN: usize = 8;
const VECTOR_ANN_CRC_LEN: usize = 4;
const TABLE_VECTOR_HEADER: u16 = 0x0301;
const TABLE_VECTOR_BLOCK: u16 = 0x0302;
const TABLE_VECTOR_HNSW: u16 = 0x0303;
const TABLE_VECTOR_ENTRYPOINT: u16 = 0x0304;
const TABLE_VECTOR_ID_MAP: u16 = 0x0305;
const TABLE_VECTOR_ENTRY_BY_ID: u16 = 0x0306;
const TABLE_VECTOR_DELETE_BITMAP: u16 = 0x0307;
const TABLE_VECTOR_HNSW_BY_NODE: u16 = 0x0308;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorSegmentHeader {
    pub schema: String,
    pub index_id: String,
    pub definition_hash: String,
    pub generation: u64,
    pub dimension: u16,
    pub metric: String,
    pub algorithm: String,
    pub embedding_provider: String,
    pub embedding_model: String,
    pub embedding_model_version: Option<String>,
    pub embedding_normalisation: String,
    pub embedding_chunking_hash: String,
    pub extractor_definition_hash: String,
    pub embedding_provenance_hash: String,
    pub modality: String,
    pub vector_count: u64,
    pub hnsw_m: u16,
    pub hnsw_ef_construction: u16,
    pub ann_format_hash: String,
    pub source_cursor: u64,
    pub authz_revision: u64,
    pub codec: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorSegmentEntry {
    pub source_id_binary: Vec<u8>,
    pub source_generation: u64,
    pub labels: Vec<u64>,
    pub record: VectorRecord,
    pub payload: VectorPayload,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecodedVectorSegment {
    pub header: VectorSegmentHeader,
    pub body_header: VectorBodyHeader,
    pub entries: Vec<VectorSegmentEntry>,
    pub hnsw_graph: HnswGraph,
    pub deleted_bitset: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct VectorSegmentWrite<'a> {
    pub index_id: &'a str,
    pub definition_hash: &'a str,
    pub generation: u64,
    pub dimension: u16,
    pub metric: VectorMetric,
    pub embedding_provider: &'a str,
    pub embedding_model: &'a str,
    pub embedding_model_version: Option<&'a str>,
    pub embedding_normalisation: &'a str,
    pub embedding_chunking_hash: &'a str,
    pub extractor_definition_hash: &'a str,
    pub embedding_provenance_hash: &'a str,
    pub modality: VectorModality,
    pub hnsw_m: u16,
    pub hnsw_ef_construction: u16,
    pub source_cursor: u64,
    pub authz_revision: u64,
    pub boundary_values: &'a [CoreBoundaryValue],
    pub entries: &'a [VectorSegmentEntry],
    pub deleted_bitset: &'a [u8],
}

#[derive(Debug, Clone)]
pub(crate) struct StagedVectorSegment {
    pub(crate) segment_ref: String,
    pub(crate) segment_hash: String,
    locator: IndexSegmentCoreMetaRecord,
    catalog: WriterSegmentCatalogRecord,
}

pub async fn write_vector_segment(
    storage: &Storage,
    write: VectorSegmentWrite<'_>,
) -> Result<String> {
    let staged = stage_vector_segment(storage, write).await?;
    publish_vector_segment_catalog(storage, &staged, &[]).await?;
    publish_vector_segment_locator(storage, &staged, &[]).await?;
    Ok(staged.segment_ref)
}

pub(crate) async fn stage_vector_segment(
    storage: &Storage,
    write: VectorSegmentWrite<'_>,
) -> Result<StagedVectorSegment> {
    let mut entries = write.entries.to_vec();
    validate_entries(write.dimension, write.metric, write.modality, &entries)?;
    entries.sort_by_key(|entry| entry.record.vector_id);
    validate_deleted_bitset(write.deleted_bitset, entries.len())?;
    let hnsw_graph = build_hnsw_graph_for_entries(
        &entries,
        write.metric,
        write.hnsw_m,
        write.hnsw_ef_construction,
    )?;
    let body = encode_vector_body(&mut entries, &hnsw_graph, write.deleted_bitset)?;
    let segment_hash = hash32(&body);
    let ref_name =
        vector_segment_ref_name(write.index_id, write.generation, &hex::encode(segment_hash))?;
    let logical_file_id = canonical_logical_file_id(
        WriterFamily::Vector,
        write.generation,
        &ref_name,
        &segment_hash,
    );

    let created_at_unix_nanos = index_coremeta::deterministic_index_publication_nanos(
        write.index_id,
        "vector_segment",
        write.generation,
        u128::from(write.source_cursor),
        &hex::encode(segment_hash),
    );
    let header = VectorSegmentHeader {
        schema: "anvil.index.vector_segment_header.v1".to_string(),
        index_id: write.index_id.to_string(),
        definition_hash: write.definition_hash.to_string(),
        generation: write.generation,
        dimension: write.dimension,
        metric: write.metric.as_name().to_string(),
        algorithm: "hnsw".to_string(),
        embedding_provider: write.embedding_provider.to_string(),
        embedding_model: write.embedding_model.to_string(),
        embedding_model_version: write.embedding_model_version.map(str::to_string),
        embedding_normalisation: write.embedding_normalisation.to_string(),
        embedding_chunking_hash: write.embedding_chunking_hash.to_string(),
        extractor_definition_hash: write.extractor_definition_hash.to_string(),
        embedding_provenance_hash: write.embedding_provenance_hash.to_string(),
        modality: write.modality.as_name().to_string(),
        vector_count: entries.len() as u64,
        hnsw_m: write.hnsw_m,
        hnsw_ef_construction: write.hnsw_ef_construction,
        ann_format_hash: hnsw_ann_format_hash(),
        source_cursor: write.source_cursor,
        authz_revision: write.authz_revision,
        codec: "f32_le_v1".to_string(),
        created_at: chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(created_at_unix_nanos)
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    };
    let (first_hash, last_hash) = record_hash_bounds(&entries);
    let header_proto = encode_vector_header_proto(&logical_file_id, &header);
    let range_index =
        single_body_range_index(body.len(), entries.len() as u64, first_hash, last_hash)?;
    let built_segment = build_writer_segment_logical_file(WriterSegmentBuildInput {
        file_family: FileFamily::VectorSegment,
        writer_family: WriterFamily::Vector,
        writer_generation: write.generation,
        logical_file_id,
        header_proto,
        body,
        range_index,
        record_count: entries.len() as u64,
        first_record_hash: first_hash,
        last_record_hash: last_hash,
        boundary_values: write.boundary_values.to_vec(),
        mutation_id: format!("vector-segment:{}:{}", write.index_id, write.generation),
        region_id: "local".to_string(),
        pipeline_policy: CorePipelinePolicy::default(),
        trace_context: CoreTraceContext::default(),
    })?;
    let segment_length = built_segment.encoded.bytes.len() as u64;
    let segment_file_hash = blake3::hash(&built_segment.encoded.bytes)
        .to_hex()
        .to_string();

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
        .ok_or_else(|| anyhow!("CoreFormatWriter returned no vector object"))?;
    let core_object_ref_target = encode_core_object_ref_target(&object_ref)?;
    let locator = IndexSegmentCoreMetaRecord {
        index_id: write.index_id.to_string(),
        index_kind: "vector".to_string(),
        writer_family: WriterFamily::Vector.as_str().to_string(),
        segment_ref: ref_name.clone(),
        core_object_ref_target: core_object_ref_target.clone(),
        segment_hash: segment_file_hash.clone(),
        segment_length,
        generation: write.generation,
        source_kind: "object_current".to_string(),
        source_cursor: write.source_cursor,
        authz_realm_id: "default".to_string(),
        authz_scope_hash: index_coremeta::segment_authz_scope_hash("vector", "per_row_label"),
        authz_revision: write.authz_revision,
        row_count: entries.len() as u64,
        field_names: Vec::new(),
        created_at_unix_nanos: u64::try_from(created_at_unix_nanos)
            .map_err(|_| anyhow!("vector segment timestamp is negative"))?,
    };
    let catalog = WriterSegmentCatalogRecord {
        family: WriterFamily::Vector.as_str().to_string(),
        scope: write.index_id.to_string(),
        segment_ref: ref_name.clone(),
        core_object_ref_target,
        segment_hash: segment_file_hash.clone(),
        segment_length,
        generation: write.generation,
        source_cursor: write.source_cursor,
        created_at_unix_nanos: locator.created_at_unix_nanos,
    };
    Ok(StagedVectorSegment {
        segment_ref: ref_name,
        segment_hash: segment_file_hash,
        locator,
        catalog,
    })
}

pub(crate) async fn publish_vector_segment_catalog(
    storage: &Storage,
    staged: &StagedVectorSegment,
    additional_preconditions: &[CoreMutationPrecondition],
) -> Result<()> {
    write_writer_segment_catalog_record(storage, &staged.catalog, additional_preconditions).await
}

pub(crate) async fn publish_vector_segment_locator(
    storage: &Storage,
    staged: &StagedVectorSegment,
    additional_preconditions: &[CoreMutationPrecondition],
) -> Result<()> {
    index_coremeta::write_index_segment_coremeta_record(
        storage,
        &staged.locator,
        additional_preconditions,
    )
    .await
}

pub async fn read_vector_segment(
    storage: &Storage,
    segment_ref: &str,
) -> Result<DecodedVectorSegment> {
    let bytes = read_vector_segment_bytes(storage, segment_ref).await?;
    decode_vector_segment(&bytes)
}

pub async fn read_vector_segment_bytes(storage: &Storage, segment_ref: &str) -> Result<Vec<u8>> {
    let store = CoreStore::new(storage.clone()).await?;
    let index_id = vector_index_id_from_segment_ref(segment_ref)?;
    let segment =
        index_coremeta::read_index_segment_coremeta_record_by_ref(storage, &index_id, segment_ref)
            .await?
            .ok_or_else(|| anyhow!("vector segment CoreMeta row is missing"))?;
    store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&segment.core_object_ref_target)?,
        })
        .await
}

pub async fn read_vector_segment_header(
    storage: &Storage,
    segment_ref: &str,
) -> Result<VectorSegmentHeader> {
    let segment =
        RangeAddressedWriterSegment::open(storage, segment_ref, FileFamily::VectorSegment).await?;
    let header = decode_vector_header_proto(&segment.header)?;
    validate_vector_segment_header(&header)?;
    Ok(header)
}

pub async fn read_latest_vector_segment(
    storage: &Storage,
    index_id: &str,
) -> Result<Option<DecodedVectorSegment>> {
    let Some(segment_ref) = latest_vector_segment_ref(storage, index_id).await? else {
        return Ok(None);
    };
    Ok(Some(read_vector_segment(storage, &segment_ref).await?))
}

pub async fn query_latest_vector_segment_ranges(
    storage: &Storage,
    index_id: &str,
    query: &[f32],
    metric: VectorMetric,
    authorized_labels: Option<&BTreeSet<Hash32>>,
    limit: usize,
) -> Result<Option<(VectorSegmentHeader, Vec<VectorSearchResult>)>> {
    let Some(segment_ref) = latest_vector_segment_ref(storage, index_id).await? else {
        return Ok(None);
    };
    query_vector_segment_ranges(
        storage,
        &segment_ref,
        query,
        metric,
        authorized_labels,
        limit,
    )
    .await
    .map(Some)
}

pub async fn query_vector_segment_ranges(
    storage: &Storage,
    segment_ref: &str,
    query: &[f32],
    metric: VectorMetric,
    authorized_labels: Option<&BTreeSet<Hash32>>,
    limit: usize,
) -> Result<(VectorSegmentHeader, Vec<VectorSearchResult>)> {
    let segment =
        RangeAddressedWriterSegment::open(storage, segment_ref, FileFamily::VectorSegment).await?;
    let header = decode_vector_header_proto(&segment.header)?;
    validate_vector_segment_header(&header)?;
    if query.len() != usize::from(header.dimension) {
        return Err(anyhow!("vector query dimension mismatch"));
    }
    if limit == 0 || header.vector_count == 0 {
        return Ok((header, Vec::new()));
    }
    let directory = segment.read_body_table_directory().await?;
    let entrypoint_table =
        RangeAddressedWriterSegment::table_entry(&directory, TABLE_VECTOR_ENTRYPOINT)?;
    let entrypoint_rows = crate::formats::table::decode_writer_body_table(
        entrypoint_table,
        &segment.read_table_bytes(entrypoint_table).await?,
    )?;
    let entrypoints = entrypoint_rows
        .rows
        .into_iter()
        .find(|row| row.key == b"entrypoint".as_slice())
        .map(|row| decode_hnsw_entrypoints(&row.value))
        .transpose()?
        .ok_or_else(|| anyhow!("vector HNSW entrypoint row missing"))?;
    let entry_table =
        RangeAddressedWriterSegment::table_entry(&directory, TABLE_VECTOR_ENTRY_BY_ID)?;
    let graph_table =
        RangeAddressedWriterSegment::table_entry(&directory, TABLE_VECTOR_HNSW_BY_NODE)?;
    let mut entry_reader = RangeVectorEntryReader {
        segment: &segment,
        entry_table,
        dimension: header.dimension,
        cache: BTreeMap::new(),
    };
    let mut graph_reader = RangeVectorGraphReader {
        segment: &segment,
        graph_table,
        cache: BTreeMap::new(),
    };
    let mut hits = query_hnsw_graph_with_range_reader(
        &entrypoints,
        &mut graph_reader,
        &mut entry_reader,
        query,
        metric,
        authorized_labels,
        limit,
        usize::from(header.hnsw_ef_construction).max(limit.saturating_mul(20).max(80)),
    )
    .await?;
    if hits.len() < limit {
        fill_vector_search_results_from_entry_table(
            &segment,
            entry_table,
            header.dimension,
            query,
            metric,
            authorized_labels,
            limit,
            &mut hits,
        )
        .await?;
    }
    Ok((header, hits))
}

async fn fill_vector_search_results_from_entry_table(
    segment: &RangeAddressedWriterSegment,
    entry_table: &crate::formats::table::WriterBodyTableDirectoryEntry,
    dimension: u16,
    query: &[f32],
    metric: VectorMetric,
    authorized_labels: Option<&BTreeSet<Hash32>>,
    limit: usize,
    hits: &mut Vec<VectorSearchResult>,
) -> Result<()> {
    let mut seen = hits
        .iter()
        .map(|hit| hit.vector_id)
        .collect::<BTreeSet<_>>();
    let table = crate::formats::table::decode_writer_body_table(
        entry_table,
        &segment.read_table_bytes(entry_table).await?,
    )?;
    for row in table.rows {
        let entry = decode_vector_entry_row(&row.value, dimension)?;
        if !seen.insert(entry.record.vector_id) {
            continue;
        }
        if !authorized_labels.is_none_or(|labels| labels.contains(&entry.record.authz_label_hash)) {
            continue;
        }
        hits.push(VectorSearchResult {
            vector_id: entry.record.vector_id,
            source_id_binary: entry.source_id_binary,
            score: vector_score(query, &entry.payload.values, metric)?,
            object_version_id: entry.record.object_version_id,
            chunk_id: entry.record.chunk_id,
            source_start: entry.record.source_start,
            source_len: entry.record.source_len,
        });
    }
    hits.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.vector_id.cmp(&right.vector_id))
    });
    hits.truncate(limit);
    Ok(())
}

pub async fn latest_vector_segment_ref(
    storage: &Storage,
    index_id: &str,
) -> Result<Option<String>> {
    require_safe_component(index_id, "vector index id")?;
    Ok(
        index_coremeta::latest_index_segment_coremeta_record_for_family(
            storage,
            index_id,
            WriterFamily::Vector.as_str(),
        )
        .await?
        .map(|record| record.segment_ref),
    )
}

pub(crate) async fn vector_segment_hash_exists(
    storage: &Storage,
    index_id: &str,
    generation: u64,
    expected_segment_hash: &str,
) -> Result<bool> {
    require_safe_component(index_id, "vector index id")?;
    validate_hex32(expected_segment_hash, "vector expected segment hash")?;
    Ok(
        index_coremeta::index_segment_coremeta_record_for_family_generation(
            storage,
            index_id,
            WriterFamily::Vector.as_str(),
            generation,
        )
        .await?
        .is_some_and(|record| record.segment_hash == expected_segment_hash),
    )
}

pub fn decode_vector_segment(bytes: &[u8]) -> Result<DecodedVectorSegment> {
    let segment = decode_writer_segment(bytes, FileFamily::VectorSegment)?;
    let header = decode_vector_header_proto(&segment.header)?;
    validate_vector_segment_header(&header)?;
    let metric = VectorMetric::from_name(&header.metric)?;
    let modality = VectorModality::from_name(&header.modality)?;
    let decoded = decode_vector_body(segment.body, header.dimension)?;
    if header.vector_count != decoded.entries.len() as u64 {
        return Err(anyhow!(
            "vector segment header count does not match decoded entries"
        ));
    }
    validate_entries(header.dimension, metric, modality, &decoded.entries)?;
    validate_hnsw_graph(&decoded.hnsw_graph, &decoded.entries, header.hnsw_m)?;
    validate_deleted_bitset(&decoded.deleted_bitset, decoded.entries.len())?;
    Ok(DecodedVectorSegment { header, ..decoded })
}

fn encode_vector_header_proto(logical_file_id: &str, header: &VectorSegmentHeader) -> Vec<u8> {
    let mut fields = vec![
        header_field_string("index_id", header.index_id.clone()),
        header_field_string("definition_hash", header.definition_hash.clone()),
        header_field_u64("dimension", u64::from(header.dimension)),
        header_field_string("metric", header.metric.clone()),
        header_field_string("algorithm", header.algorithm.clone()),
        header_field_string("embedding_provider", header.embedding_provider.clone()),
        header_field_string("embedding_model", header.embedding_model.clone()),
        header_field_string(
            "embedding_normalisation",
            header.embedding_normalisation.clone(),
        ),
        header_field_string(
            "embedding_chunking_hash",
            header.embedding_chunking_hash.clone(),
        ),
        header_field_string(
            "extractor_definition_hash",
            header.extractor_definition_hash.clone(),
        ),
        header_field_string(
            "embedding_provenance_hash",
            header.embedding_provenance_hash.clone(),
        ),
        header_field_string("modality", header.modality.clone()),
        header_field_u64("vector_count", header.vector_count),
        header_field_u64("hnsw_m", u64::from(header.hnsw_m)),
        header_field_u64(
            "hnsw_ef_construction",
            u64::from(header.hnsw_ef_construction),
        ),
        header_field_string("ann_format_hash", header.ann_format_hash.clone()),
        header_field_u64("source_cursor", header.source_cursor),
        header_field_u64("authz_revision", header.authz_revision),
        header_field_string("codec", header.codec.clone()),
        header_field_string("created_at", header.created_at.clone()),
    ];
    if let Some(version) = &header.embedding_model_version {
        fields.push(header_field_string(
            "embedding_model_version",
            version.clone(),
        ));
    }
    encode_writer_segment_header(
        "anvil.index.vector_segment_header.v1",
        logical_file_id,
        FileFamily::VectorSegment,
        header.generation,
        None,
        None,
        unix_nanos_from_rfc3339(&header.created_at),
        fields,
    )
}

fn decode_vector_header_proto(
    header: &crate::formats::WriterSegmentHeaderProto,
) -> Result<VectorSegmentHeader> {
    Ok(VectorSegmentHeader {
        schema: header.schema.clone(),
        index_id: required_header_string(header, "index_id")?,
        definition_hash: required_header_string(header, "definition_hash")?,
        generation: header.writer_generation,
        dimension: u16::try_from(required_header_u64(header, "dimension")?)
            .context("vector header dimension exceeds u16")?,
        metric: required_header_string(header, "metric")?,
        algorithm: required_header_string(header, "algorithm")?,
        embedding_provider: required_header_string(header, "embedding_provider")?,
        embedding_model: required_header_string(header, "embedding_model")?,
        embedding_model_version: optional_header_string(header, "embedding_model_version")?,
        embedding_normalisation: required_header_string(header, "embedding_normalisation")?,
        embedding_chunking_hash: required_header_string(header, "embedding_chunking_hash")?,
        extractor_definition_hash: required_header_string(header, "extractor_definition_hash")?,
        embedding_provenance_hash: required_header_string(header, "embedding_provenance_hash")?,
        modality: required_header_string(header, "modality")?,
        vector_count: required_header_u64(header, "vector_count")?,
        hnsw_m: u16::try_from(required_header_u64(header, "hnsw_m")?)
            .context("vector header hnsw_m exceeds u16")?,
        hnsw_ef_construction: u16::try_from(required_header_u64(header, "hnsw_ef_construction")?)
            .context("vector header hnsw_ef_construction exceeds u16")?,
        ann_format_hash: required_header_string(header, "ann_format_hash")?,
        source_cursor: required_header_u64(header, "source_cursor")?,
        authz_revision: required_header_u64(header, "authz_revision")?,
        codec: required_header_string(header, "codec")?,
        created_at: required_header_string(header, "created_at")?,
    })
}

fn validate_vector_segment_header(header: &VectorSegmentHeader) -> Result<()> {
    if header.schema != "anvil.index.vector_segment_header.v1" {
        return Err(anyhow!("vector segment header schema is unsupported"));
    }
    if header.definition_hash.is_empty() {
        return Err(anyhow!("vector segment definition hash is missing"));
    }
    if header.algorithm != "hnsw" {
        return Err(anyhow!("vector segment algorithm is unsupported"));
    }
    if header.ann_format_hash != hnsw_ann_format_hash() {
        return Err(anyhow!("vector segment ann format hash is unsupported"));
    }
    if header.codec != "f32_le_v1" {
        return Err(anyhow!("vector segment codec is unsupported"));
    }
    Ok(())
}

fn encode_vector_body(
    entries: &mut [VectorSegmentEntry],
    graph: &HnswGraph,
    deleted_bitset: &[u8],
) -> Result<Vec<u8>> {
    let vector_block_bytes = encode_vector_blocks(entries)?;
    let source_table_bytes = encode_source_table(entries)?;
    let mut record_table = Vec::with_capacity(entries.len() * VECTOR_RECORD_LEN);
    for entry in entries.iter() {
        record_table.extend_from_slice(&entry.record.encode());
    }
    let ann_block_bytes = encode_hnsw_ann_block(graph);
    let entrypoint_bytes = encode_hnsw_entrypoints(graph);
    let body_header = VectorBodyHeader {
        vector_count: entries.len() as u64,
        record_table_offset: 0,
        vector_blocks_offset: 0,
        ann_blocks_offset: 0,
        deleted_bitset_offset: 0,
    };
    encode_writer_body_tables(&[
        WriterBodyTable {
            table_id: TABLE_VECTOR_HEADER,
            row_type_id: TABLE_VECTOR_HEADER,
            rows: vec![TableRow {
                key: b"vector-record-table".to_vec(),
                value: [body_header.encode(), record_table].concat(),
            }],
        },
        WriterBodyTable {
            table_id: TABLE_VECTOR_BLOCK,
            row_type_id: TABLE_VECTOR_BLOCK,
            rows: vec![TableRow {
                key: b"vector-block-0".to_vec(),
                value: vector_block_bytes,
            }],
        },
        WriterBodyTable {
            table_id: TABLE_VECTOR_HNSW,
            row_type_id: TABLE_VECTOR_HNSW,
            rows: vec![TableRow {
                key: b"hnsw-layer-table".to_vec(),
                value: ann_block_bytes.clone(),
            }],
        },
        WriterBodyTable {
            table_id: TABLE_VECTOR_ENTRYPOINT,
            row_type_id: TABLE_VECTOR_ENTRYPOINT,
            rows: vec![TableRow {
                key: b"entrypoint".to_vec(),
                value: entrypoint_bytes,
            }],
        },
        WriterBodyTable {
            table_id: TABLE_VECTOR_HNSW_BY_NODE,
            row_type_id: TABLE_VECTOR_HNSW_BY_NODE,
            rows: hnsw_adjacency_rows(graph),
        },
        WriterBodyTable {
            table_id: TABLE_VECTOR_ID_MAP,
            row_type_id: TABLE_VECTOR_ID_MAP,
            rows: vec![TableRow {
                key: b"source-table".to_vec(),
                value: source_table_bytes,
            }],
        },
        WriterBodyTable {
            table_id: TABLE_VECTOR_ENTRY_BY_ID,
            row_type_id: TABLE_VECTOR_ENTRY_BY_ID,
            rows: vector_entry_rows(entries)?,
        },
        WriterBodyTable {
            table_id: TABLE_VECTOR_DELETE_BITMAP,
            row_type_id: TABLE_VECTOR_DELETE_BITMAP,
            rows: vec![TableRow {
                key: b"delete-bitmap".to_vec(),
                value: deleted_bitset.to_vec(),
            }],
        },
    ])
    .map_err(anyhow::Error::from)
}

fn decode_vector_body(body: &[u8], dimension: u16) -> Result<DecodedVectorSegment> {
    let tables = decode_vector_tables(body)?;
    let body_header = VectorBodyHeader::decode(&tables.record_table)?;
    let record_count =
        usize::try_from(body_header.vector_count).context("vector count exceeds usize")?;
    let source_entries = decode_source_table(&tables.source_table, record_count)?;
    let table_len = record_count
        .checked_mul(VECTOR_RECORD_LEN)
        .ok_or_else(|| anyhow!("vector table length overflow"))?;
    let record_table = &tables.record_table[VECTOR_BODY_HEADER_LEN..];
    if record_table.len() != table_len {
        return Err(anyhow!(
            "vector table offset range does not match vector count"
        ));
    }
    let mut records = Vec::with_capacity(record_count);
    let mut cursor = 0usize;
    for _ in 0..record_count {
        let (record, used) = VectorRecord::decode(&record_table[cursor..])?;
        records.push(record);
        cursor += used;
    }
    let payloads = decode_vector_blocks(&tables.vector_blocks, record_count, dimension)?;
    let mut entries = Vec::with_capacity(records.len());
    for (idx, record) in records.into_iter().enumerate() {
        let source = source_entries
            .get(idx)
            .ok_or_else(|| anyhow!("vector source table entry is missing"))?;
        entries.push(VectorSegmentEntry {
            source_id_binary: source.source_id_binary.clone(),
            source_generation: source.source_generation,
            labels: source.labels.clone(),
            record,
            payload: payloads
                .get(idx)
                .ok_or_else(|| anyhow!("vector payload block entry is missing"))?
                .clone(),
        });
    }
    let hnsw_graph = decode_hnsw_ann_block(&tables.hnsw)?;
    let deleted_bitset = tables.deleted_bitset;
    Ok(DecodedVectorSegment {
        header: VectorSegmentHeader {
            schema: String::new(),
            index_id: String::new(),
            definition_hash: String::new(),
            generation: 0,
            dimension,
            metric: String::new(),
            algorithm: String::new(),
            embedding_provider: String::new(),
            embedding_model_version: None,
            embedding_normalisation: String::new(),
            embedding_chunking_hash: String::new(),
            extractor_definition_hash: String::new(),
            embedding_provenance_hash: String::new(),
            embedding_model: String::new(),
            modality: String::new(),
            vector_count: 0,
            hnsw_m: 0,
            hnsw_ef_construction: 0,
            ann_format_hash: String::new(),
            source_cursor: 0,
            authz_revision: 0,
            codec: String::new(),
            created_at: String::new(),
        },
        body_header,
        entries,
        hnsw_graph,
        deleted_bitset,
    })
}

struct VectorTables {
    record_table: Vec<u8>,
    vector_blocks: Vec<u8>,
    hnsw: Vec<u8>,
    source_table: Vec<u8>,
    deleted_bitset: Vec<u8>,
}

fn decode_vector_tables(body: &[u8]) -> Result<VectorTables> {
    let mut record_table = None;
    let mut vector_blocks = None;
    let mut hnsw = None;
    let mut source_table = None;
    let mut deleted_bitset = Some(Vec::new());
    for table in decode_writer_body_tables(body)? {
        let value = table.rows.into_iter().next().map(|row| row.value);
        match table.table_id {
            TABLE_VECTOR_HEADER => record_table = value,
            TABLE_VECTOR_BLOCK => vector_blocks = value,
            TABLE_VECTOR_HNSW => hnsw = value,
            TABLE_VECTOR_ID_MAP => source_table = value,
            TABLE_VECTOR_DELETE_BITMAP => deleted_bitset = value,
            _ => {}
        }
    }
    Ok(VectorTables {
        record_table: record_table.ok_or_else(|| anyhow!("vector record table missing"))?,
        vector_blocks: vector_blocks.ok_or_else(|| anyhow!("vector block table missing"))?,
        hnsw: hnsw.ok_or_else(|| anyhow!("vector HNSW table missing"))?,
        source_table: source_table.ok_or_else(|| anyhow!("vector source table missing"))?,
        deleted_bitset: deleted_bitset.unwrap_or_default(),
    })
}

fn hnsw_adjacency_rows(graph: &HnswGraph) -> Vec<TableRow> {
    let mut rows = Vec::new();
    for layer in &graph.layers {
        for node in &layer.node_adjacencies {
            rows.push(TableRow {
                key: hnsw_adjacency_key(layer.layer_index, node.vector_id),
                value: encode_hnsw_neighbors(&node.neighbors),
            });
        }
    }
    rows.sort_by(|left, right| left.key.cmp(&right.key));
    rows
}

fn hnsw_adjacency_key(layer: u16, vector_id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(10);
    key.extend_from_slice(&layer.to_be_bytes());
    key.extend_from_slice(&vector_id.to_be_bytes());
    key
}

fn encode_hnsw_neighbors(neighbors: &[u64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + neighbors.len() * 8);
    out.extend_from_slice(&(neighbors.len() as u32).to_le_bytes());
    for neighbor in neighbors {
        out.extend_from_slice(&neighbor.to_le_bytes());
    }
    out
}

fn decode_hnsw_neighbors(input: &[u8]) -> Result<Vec<u64>> {
    if input.len() < 4 {
        return Err(anyhow!("vector HNSW neighbor row missing count"));
    }
    let count = u32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
    let expected = 4usize
        .checked_add(count.saturating_mul(8))
        .ok_or_else(|| anyhow!("vector HNSW neighbor row length overflow"))?;
    if input.len() != expected {
        return Err(anyhow!("vector HNSW neighbor row length mismatch"));
    }
    let mut neighbors = Vec::with_capacity(count);
    let mut cursor = 4usize;
    for _ in 0..count {
        neighbors.push(u64::from_le_bytes(
            input[cursor..cursor + 8].try_into().unwrap(),
        ));
        cursor += 8;
    }
    Ok(neighbors)
}

fn encode_hnsw_entrypoints(graph: &HnswGraph) -> Vec<u8> {
    let mut layers = graph
        .layers
        .iter()
        .filter_map(|layer| {
            layer
                .node_adjacencies
                .first()
                .map(|node| (layer.layer_index, node.vector_id))
        })
        .collect::<Vec<_>>();
    layers.sort_by(|left, right| right.0.cmp(&left.0));
    let mut out = Vec::with_capacity(4 + layers.len() * 10);
    out.extend_from_slice(&(layers.len() as u32).to_le_bytes());
    for (layer, vector_id) in layers {
        out.extend_from_slice(&layer.to_le_bytes());
        out.extend_from_slice(&vector_id.to_le_bytes());
    }
    out
}

fn decode_hnsw_entrypoints(input: &[u8]) -> Result<Vec<(u16, u64)>> {
    if input.len() < 4 {
        return Err(anyhow!("vector HNSW entrypoint row missing count"));
    }
    let count = u32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
    let expected = 4usize
        .checked_add(count.saturating_mul(10))
        .ok_or_else(|| anyhow!("vector HNSW entrypoint length overflow"))?;
    if input.len() != expected {
        return Err(anyhow!("vector HNSW entrypoint length mismatch"));
    }
    let mut out = Vec::with_capacity(count);
    let mut cursor = 4usize;
    for _ in 0..count {
        let layer = u16::from_le_bytes(input[cursor..cursor + 2].try_into().unwrap());
        cursor += 2;
        let vector_id = u64::from_le_bytes(input[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        out.push((layer, vector_id));
    }
    Ok(out)
}

fn vector_entry_rows(entries: &[VectorSegmentEntry]) -> Result<Vec<TableRow>> {
    let mut rows = entries
        .iter()
        .map(|entry| {
            Ok(TableRow {
                key: vector_entry_key(entry.record.vector_id),
                value: encode_vector_entry_row(entry)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    rows.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(rows)
}

fn vector_entry_key(vector_id: u64) -> Vec<u8> {
    vector_id.to_be_bytes().to_vec()
}

fn encode_vector_entry_row(entry: &VectorSegmentEntry) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(&entry.record.encode());
    let payload = entry.payload.encode()?;
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&payload);
    let source_len = u32::try_from(entry.source_id_binary.len())
        .context("vector entry source id exceeds u32")?;
    out.extend_from_slice(&source_len.to_le_bytes());
    out.extend_from_slice(&entry.source_id_binary);
    out.extend_from_slice(&entry.source_generation.to_le_bytes());
    let label_count =
        u32::try_from(entry.labels.len()).context("vector entry labels exceed u32")?;
    out.extend_from_slice(&label_count.to_le_bytes());
    for label in &entry.labels {
        out.extend_from_slice(&label.to_le_bytes());
    }
    Ok(out)
}

fn decode_vector_entry_row(input: &[u8], dimension: u16) -> Result<VectorSegmentEntry> {
    let (record, mut cursor) = VectorRecord::decode(input)?;
    if input.len().saturating_sub(cursor) < 4 {
        return Err(anyhow!("vector entry row missing payload length"));
    }
    let payload_len = u32::from_le_bytes(input[cursor..cursor + 4].try_into().unwrap()) as usize;
    cursor += 4;
    if input.len().saturating_sub(cursor) < payload_len + 4 {
        return Err(anyhow!("vector entry row is truncated"));
    }
    let payload = VectorPayload::decode(&input[cursor..cursor + payload_len], dimension)?;
    cursor += payload_len;
    let source_len = u32::from_le_bytes(input[cursor..cursor + 4].try_into().unwrap()) as usize;
    cursor += 4;
    if input.len().saturating_sub(cursor) < source_len + 8 + 4 {
        return Err(anyhow!("vector entry source fields are truncated"));
    }
    let source_id_binary = input[cursor..cursor + source_len].to_vec();
    cursor += source_len;
    let source_generation = u64::from_le_bytes(input[cursor..cursor + 8].try_into().unwrap());
    cursor += 8;
    let label_count = u32::from_le_bytes(input[cursor..cursor + 4].try_into().unwrap()) as usize;
    cursor += 4;
    let label_bytes = label_count
        .checked_mul(8)
        .ok_or_else(|| anyhow!("vector entry label bytes overflow"))?;
    if input.len().saturating_sub(cursor) < label_bytes {
        return Err(anyhow!("vector entry labels are truncated"));
    }
    let mut labels = Vec::with_capacity(label_count);
    for _ in 0..label_count {
        labels.push(u64::from_le_bytes(
            input[cursor..cursor + 8].try_into().unwrap(),
        ));
        cursor += 8;
    }
    if cursor != input.len() {
        return Err(anyhow!("vector entry row has trailing bytes"));
    }
    Ok(VectorSegmentEntry {
        source_id_binary,
        source_generation,
        labels,
        record,
        payload,
    })
}

fn encode_vector_blocks(entries: &mut [VectorSegmentEntry]) -> Result<Vec<u8>> {
    let mut raw_vectors = Vec::new();
    for (ordinal, entry) in entries.iter_mut().enumerate() {
        let vector_offset = ordinal
            .checked_mul(entry.payload.dimension as usize)
            .and_then(|value| value.checked_mul(4))
            .ok_or_else(|| anyhow!("vector block offset overflow"))?;
        entry.record.vector_payload_offset =
            u64::try_from(vector_offset).context("vector block offset exceeds u64")?;
        raw_vectors.extend_from_slice(&entry.payload.encode()?);
    }

    let vector_count = entries.len() as u64;
    let block_len = VECTOR_BLOCK_COUNT_LEN
        .checked_add(raw_vectors.len())
        .and_then(|value| value.checked_add(VECTOR_BLOCK_CRC_LEN))
        .ok_or_else(|| anyhow!("vector block length overflow"))?;
    let mut crc_input = Vec::with_capacity(VECTOR_BLOCK_COUNT_LEN + raw_vectors.len());
    crc_input.extend_from_slice(&vector_count.to_le_bytes());
    crc_input.extend_from_slice(&raw_vectors);

    let mut out = Vec::with_capacity(VECTOR_BLOCK_LEN_LEN + block_len);
    out.extend_from_slice(&(block_len as u64).to_le_bytes());
    out.extend_from_slice(&vector_count.to_le_bytes());
    out.extend_from_slice(&raw_vectors);
    out.extend_from_slice(&crc32c(&crc_input).to_le_bytes());
    Ok(out)
}

fn decode_vector_blocks(
    input: &[u8],
    expected_count: usize,
    dimension: u16,
) -> Result<Vec<VectorPayload>> {
    if input.len() < VECTOR_BLOCK_LEN_LEN + VECTOR_BLOCK_COUNT_LEN + VECTOR_BLOCK_CRC_LEN {
        return Err(anyhow!("vector block is truncated"));
    }
    let block_len = u64::from_le_bytes(input[0..8].try_into().unwrap());
    let block_len = usize::try_from(block_len).context("vector block length exceeds usize")?;
    if block_len + VECTOR_BLOCK_LEN_LEN != input.len() {
        return Err(anyhow!("vector block length does not match payload"));
    }

    let vector_count = u64::from_le_bytes(input[8..16].try_into().unwrap());
    let vector_count = usize::try_from(vector_count).context("vector block count exceeds usize")?;
    if vector_count != expected_count {
        return Err(anyhow!("vector block count does not match segment count"));
    }
    let raw_vectors_start = VECTOR_BLOCK_LEN_LEN + VECTOR_BLOCK_COUNT_LEN;
    let raw_vectors_end = input
        .len()
        .checked_sub(VECTOR_BLOCK_CRC_LEN)
        .ok_or_else(|| anyhow!("vector block is shorter than crc"))?;
    let actual_crc = u32::from_le_bytes(input[raw_vectors_end..].try_into().unwrap());
    if crc32c(&input[VECTOR_BLOCK_LEN_LEN..raw_vectors_end]) != actual_crc {
        return Err(anyhow!("vector block crc32c mismatch"));
    }

    let vector_bytes = usize::from(dimension)
        .checked_mul(4)
        .ok_or_else(|| anyhow!("vector dimension byte length overflow"))?;
    let raw_vectors = &input[raw_vectors_start..raw_vectors_end];
    if raw_vectors.len()
        != expected_count
            .checked_mul(vector_bytes)
            .ok_or_else(|| anyhow!("vector block raw length overflow"))?
    {
        return Err(anyhow!(
            "vector block raw vector length does not match dimension"
        ));
    }

    raw_vectors
        .chunks_exact(vector_bytes)
        .map(|chunk| VectorPayload::decode(chunk, dimension).map_err(anyhow::Error::from))
        .collect()
}

fn encode_hnsw_ann_block(graph: &HnswGraph) -> Vec<u8> {
    let ann_bytes = graph.encode();
    let mut crc_input =
        Vec::with_capacity(VECTOR_ANN_KIND_LEN + VECTOR_ANN_LEN_LEN + ann_bytes.len());
    crc_input.extend_from_slice(&VECTOR_ANN_KIND_HNSW_V1.to_le_bytes());
    crc_input.extend_from_slice(&(ann_bytes.len() as u64).to_le_bytes());
    crc_input.extend_from_slice(&ann_bytes);

    let mut out = Vec::with_capacity(crc_input.len() + VECTOR_ANN_CRC_LEN);
    out.extend_from_slice(&crc_input);
    out.extend_from_slice(&crc32c(&crc_input).to_le_bytes());
    out
}

fn decode_hnsw_ann_block(input: &[u8]) -> Result<HnswGraph> {
    if input.len() < VECTOR_ANN_KIND_LEN + VECTOR_ANN_LEN_LEN + VECTOR_ANN_CRC_LEN {
        return Err(anyhow!("ann block is truncated"));
    }
    let ann_kind = u16::from_le_bytes(input[0..2].try_into().unwrap());
    if ann_kind != VECTOR_ANN_KIND_HNSW_V1 {
        return Err(anyhow!("unsupported vector ann kind {ann_kind}"));
    }
    let ann_len = u64::from_le_bytes(input[2..10].try_into().unwrap());
    let ann_len = usize::try_from(ann_len).context("ann block length exceeds usize")?;
    let ann_start = VECTOR_ANN_KIND_LEN + VECTOR_ANN_LEN_LEN;
    let ann_end = ann_start
        .checked_add(ann_len)
        .ok_or_else(|| anyhow!("ann block length overflow"))?;
    if ann_end + VECTOR_ANN_CRC_LEN != input.len() {
        return Err(anyhow!("ann block length does not match payload"));
    }
    let expected_crc = u32::from_le_bytes(input[ann_end..].try_into().unwrap());
    if crc32c(&input[..ann_end]) != expected_crc {
        return Err(anyhow!("ann block crc32c mismatch"));
    }
    Ok(HnswGraph::decode(&input[ann_start..ann_end])?)
}

fn hnsw_ann_format_hash() -> String {
    let description = b"anvil.vector.ann.hnsw_v1:ann_kind:u16le,ann_len:u64le,hnsw:layer_count:u16le,node_count:u64le,layers[layer_index:u16le,node_count:u64le,node[vector_id:u64le,neighbor_count:u16le,neighbors:u64le*]],crc32c:u32le";
    format!("blake3:{}", blake3::hash(description).to_hex())
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0x82f6_3b78 & mask);
        }
    }
    !crc
}

struct RangeVectorEntryReader<'a> {
    segment: &'a RangeAddressedWriterSegment,
    entry_table: &'a crate::formats::table::WriterBodyTableDirectoryEntry,
    dimension: u16,
    cache: BTreeMap<u64, VectorSegmentEntry>,
}

impl RangeVectorEntryReader<'_> {
    async fn get(&mut self, vector_id: u64) -> Result<Option<VectorSegmentEntry>> {
        if let Some(entry) = self.cache.get(&vector_id) {
            return Ok(Some(entry.clone()));
        }
        let key = vector_entry_key(vector_id);
        let rows = self
            .segment
            .read_table_pages_matching_key_prefix(self.entry_table, &key)
            .await?;
        let Some(row) = rows.into_iter().find(|row| row.key == key) else {
            return Ok(None);
        };
        let entry = decode_vector_entry_row(&row.value, self.dimension)?;
        self.cache.insert(vector_id, entry.clone());
        Ok(Some(entry))
    }
}

struct RangeVectorGraphReader<'a> {
    segment: &'a RangeAddressedWriterSegment,
    graph_table: &'a crate::formats::table::WriterBodyTableDirectoryEntry,
    cache: BTreeMap<(u16, u64), Vec<u64>>,
}

impl RangeVectorGraphReader<'_> {
    async fn graph_adjacency_by_layer(&mut self, layer: u16, vector_id: u64) -> Result<Vec<u64>> {
        let key = hnsw_adjacency_key(layer, vector_id);
        let rows = self
            .segment
            .read_table_pages_matching_key_prefix(self.graph_table, &key)
            .await?;
        rows.into_iter()
            .find(|row| row.key == key)
            .map(|row| decode_hnsw_neighbors(&row.value))
            .transpose()
            .map(|neighbors| neighbors.unwrap_or_default())
    }

    async fn neighbors(&mut self, layer: u16, vector_id: u64) -> Result<Vec<u64>> {
        let cache_key = (layer, vector_id);
        if let Some(neighbors) = self.cache.get(&cache_key) {
            return Ok(neighbors.clone());
        }
        let neighbors = self.graph_adjacency_by_layer(layer, vector_id).await?;
        self.cache.insert(cache_key, neighbors.clone());
        Ok(neighbors)
    }
}

async fn query_hnsw_graph_with_range_reader(
    entrypoints: &[(u16, u64)],
    graph_reader: &mut RangeVectorGraphReader<'_>,
    entry_reader: &mut RangeVectorEntryReader<'_>,
    query: &[f32],
    metric: VectorMetric,
    authorized_labels: Option<&BTreeSet<Hash32>>,
    limit: usize,
    ef_search: usize,
) -> Result<Vec<VectorSearchResult>> {
    if limit == 0 || entrypoints.is_empty() {
        return Ok(Vec::new());
    }
    let mut current_id = entrypoints[0].1;
    let Some(current_entry) = entry_reader.get(current_id).await? else {
        return Ok(Vec::new());
    };
    let mut current_score = vector_score(query, &current_entry.payload.values, metric)?;

    for (layer, _) in entrypoints.iter().copied().filter(|(layer, _)| *layer > 0) {
        let mut improved = true;
        while improved {
            improved = false;
            for neighbor in graph_reader.neighbors(layer, current_id).await? {
                let Some(entry) = entry_reader.get(neighbor).await? else {
                    continue;
                };
                let score = vector_score(query, &entry.payload.values, metric)?;
                if score > current_score {
                    current_id = neighbor;
                    current_score = score;
                    improved = true;
                }
            }
        }
    }

    let mut visited = BTreeSet::new();
    let mut frontier = vec![current_id];
    let mut scored = Vec::new();
    while let Some(vector_id) = frontier.pop() {
        if !visited.insert(vector_id) || visited.len() > ef_search.max(limit) {
            continue;
        }
        let Some(entry) = entry_reader.get(vector_id).await? else {
            continue;
        };
        let score = vector_score(query, &entry.payload.values, metric)?;
        scored.push((score, vector_id, entry.clone()));
        let mut neighbors = graph_reader.neighbors(0, vector_id).await?;
        neighbors.sort_by(|left, right| {
            let left_seen = visited.contains(left);
            let right_seen = visited.contains(right);
            left_seen.cmp(&right_seen).then_with(|| left.cmp(right))
        });
        frontier.extend(neighbors.into_iter().filter(|id| !visited.contains(id)));
        scored.sort_by(|left, right| {
            right
                .0
                .partial_cmp(&left.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.1.cmp(&right.1))
        });
        if scored.len() > ef_search {
            scored.truncate(ef_search);
        }
    }

    let mut hits = scored
        .into_iter()
        .filter(|(_, _, entry)| {
            authorized_labels.is_none_or(|labels| labels.contains(&entry.record.authz_label_hash))
        })
        .map(|(score, _, entry)| VectorSearchResult {
            vector_id: entry.record.vector_id,
            source_id_binary: entry.source_id_binary.clone(),
            score,
            object_version_id: entry.record.object_version_id,
            chunk_id: entry.record.chunk_id,
            source_start: entry.record.source_start,
            source_len: entry.record.source_len,
        })
        .collect::<Vec<_>>();
    hits.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.vector_id.cmp(&right.vector_id))
    });
    hits.truncate(limit);
    Ok(hits)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VectorSourceEntry {
    source_id_binary: Vec<u8>,
    source_generation: u64,
    labels: Vec<u64>,
}

fn encode_source_table(entries: &[VectorSegmentEntry]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for entry in entries {
        let source_len = u32::try_from(entry.source_id_binary.len())
            .context("vector source id binary length exceeds u32")?;
        out.extend_from_slice(&source_len.to_le_bytes());
        out.extend_from_slice(&entry.source_id_binary);
        out.extend_from_slice(&entry.source_generation.to_le_bytes());
        out.extend_from_slice(&entry.record.chunk_id.to_le_bytes());
        let label_count =
            u32::try_from(entry.labels.len()).context("vector label count exceeds u32")?;
        out.extend_from_slice(&label_count.to_le_bytes());
        for label in &entry.labels {
            out.extend_from_slice(&label.to_le_bytes());
        }
    }
    Ok(out)
}

fn decode_source_table(input: &[u8], expected_count: usize) -> Result<Vec<VectorSourceEntry>> {
    if input.len() < 8 {
        return Err(anyhow!("vector source table is shorter than count"));
    }
    let count = u64::from_le_bytes(input[0..8].try_into().unwrap());
    let count = usize::try_from(count).context("vector source count exceeds usize")?;
    if count != expected_count {
        return Err(anyhow!(
            "vector source table count does not match vector count"
        ));
    }
    let mut entries = Vec::with_capacity(count);
    let mut cursor = 8usize;
    for _ in 0..count {
        if input.len().saturating_sub(cursor) < 4 {
            return Err(anyhow!("vector source entry missing source id length"));
        }
        let source_len = u32::from_le_bytes(input[cursor..cursor + 4].try_into().unwrap());
        cursor += 4;
        let source_len = usize::try_from(source_len).context("source id length exceeds usize")?;
        if input.len().saturating_sub(cursor) < source_len + 8 + 4 + 4 {
            return Err(anyhow!("vector source entry is truncated"));
        }
        let source_id_binary = input[cursor..cursor + source_len].to_vec();
        cursor += source_len;
        let source_generation = u64::from_le_bytes(input[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        let _chunk_ordinal = u32::from_le_bytes(input[cursor..cursor + 4].try_into().unwrap());
        cursor += 4;
        let label_count = u32::from_le_bytes(input[cursor..cursor + 4].try_into().unwrap());
        cursor += 4;
        let label_count = usize::try_from(label_count).context("label count exceeds usize")?;
        let label_bytes = label_count
            .checked_mul(8)
            .ok_or_else(|| anyhow!("label byte length overflow"))?;
        if input.len().saturating_sub(cursor) < label_bytes {
            return Err(anyhow!("vector source labels are truncated"));
        }
        let mut labels = Vec::with_capacity(label_count);
        for _ in 0..label_count {
            labels.push(u64::from_le_bytes(
                input[cursor..cursor + 8].try_into().unwrap(),
            ));
            cursor += 8;
        }
        entries.push(VectorSourceEntry {
            source_id_binary,
            source_generation,
            labels,
        });
    }
    if cursor != input.len() {
        return Err(anyhow!("vector source table has trailing bytes"));
    }
    Ok(entries)
}

fn validate_entries(
    dimension: u16,
    metric: VectorMetric,
    modality: VectorModality,
    entries: &[VectorSegmentEntry],
) -> Result<()> {
    for entry in entries {
        if entry.payload.dimension != dimension
            || entry.record.dimension != dimension
            || entry.record.metric != metric as u8
            || entry.record.modality != modality as u8
        {
            return Err(anyhow!("vector entry does not match segment header"));
        }
        if entry.source_id_binary.is_empty() {
            return Err(anyhow!("vector entry source id is empty"));
        }
    }
    Ok(())
}

fn validate_deleted_bitset(deleted_bitset: &[u8], entry_count: usize) -> Result<()> {
    let expected_len = entry_count.div_ceil(8);
    if deleted_bitset.len() != expected_len {
        return Err(anyhow!(
            "vector deleted bitset length does not match entry count"
        ));
    }
    Ok(())
}

fn record_hash_bounds(entries: &[VectorSegmentEntry]) -> (Hash32, Hash32) {
    let first = entries
        .first()
        .map(|entry| hash32(&entry.record.encode()))
        .unwrap_or([0; 32]);
    let last = entries
        .last()
        .map(|entry| hash32(&entry.record.encode()))
        .unwrap_or([0; 32]);
    (first, last)
}

fn vector_segment_ref_prefix(index_id: &str) -> Result<String> {
    require_safe_component(index_id, "vector index id")?;
    Ok(format!("{VECTOR_SEGMENT_REF_PREFIX}index:{index_id}:"))
}

fn vector_segment_ref_name(index_id: &str, generation: u64, segment_hash: &str) -> Result<String> {
    validate_hex32(segment_hash, "vector segment hash")?;
    Ok(format!(
        "{}generation:{generation:020}:hash:{segment_hash}",
        vector_segment_ref_prefix(index_id)?
    ))
}

fn vector_index_id_from_segment_ref(segment_ref: &str) -> Result<String> {
    let rest = segment_ref
        .strip_prefix(VECTOR_SEGMENT_REF_PREFIX)
        .ok_or_else(|| anyhow!("vector segment ref has invalid prefix"))?;
    let rest = rest
        .strip_prefix("index:")
        .ok_or_else(|| anyhow!("vector segment ref is missing index component"))?;
    let (index_id, _) = rest
        .split_once(":generation:")
        .ok_or_else(|| anyhow!("vector segment ref is missing generation component"))?;
    require_safe_component(index_id, "vector index id")?;
    Ok(index_id.to_string())
}

fn generation_from_ref(ref_name: &str) -> Option<u64> {
    ref_name
        .rsplit_once(":generation:")?
        .1
        .split(':')
        .next()?
        .parse()
        .ok()
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

fn encode_core_object_ref_target(object_ref: &CoreObjectRef) -> Result<String> {
    crate::core_store::encode_core_object_ref_target(object_ref)
}

fn decode_core_object_ref_target(target: &str) -> Result<CoreObjectRef> {
    crate::core_store::decode_core_object_ref_target(target)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn entry(vector_id: u64, values: Vec<f32>) -> VectorSegmentEntry {
        VectorSegmentEntry {
            source_id_binary: vec![vector_id as u8, 0xaa],
            source_generation: vector_id * 10,
            labels: if vector_id == 1 { vec![42] } else { Vec::new() },
            record: VectorRecord {
                vector_id,
                object_version_id: [vector_id as u8; 16],
                chunk_id: vector_id as u32,
                modality: VectorModality::Text as u8,
                metric: VectorMetric::Cosine as u8,
                dimension: 3,
                vector_payload_offset: 0,
                source_start: vector_id * 100,
                source_len: 20,
                authz_label_hash: [7; 32],
                metadata_filter_bits: 0,
            },
            payload: VectorPayload {
                dimension: 3,
                values,
            },
        }
    }

    #[tokio::test]
    async fn vector_segment_round_trips_payloads_and_graph() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let entries = vec![entry(2, vec![0.0, 1.0, 0.0]), entry(1, vec![1.0, 0.0, 0.0])];
        let segment_ref = write_vector_segment(
            &storage,
            VectorSegmentWrite {
                index_id: "vector-alpha",
                definition_hash: "blake3:test-definition",
                generation: 6,
                dimension: 3,
                metric: VectorMetric::Cosine,
                embedding_provider: "test_only",
                embedding_model_version: None,
                embedding_normalisation: "unit_l2",
                embedding_chunking_hash: "blake3:test-chunking",
                extractor_definition_hash: "blake3:test-extractor",
                embedding_provenance_hash: "blake3:test-provenance",
                embedding_model: "embedding-v1",
                modality: VectorModality::Text,
                hnsw_m: 32,
                hnsw_ef_construction: 200,
                source_cursor: 88,
                authz_revision: 9,
                boundary_values: &[],
                entries: &entries,
                deleted_bitset: &[0],
            },
        )
        .await
        .unwrap();
        assert!(segment_ref.starts_with(VECTOR_SEGMENT_REF_PREFIX));

        let decoded = read_vector_segment(&storage, &segment_ref).await.unwrap();
        assert_eq!(decoded.header.index_id, "vector-alpha");
        assert_eq!(
            decoded.header.schema,
            "anvil.index.vector_segment_header.v1"
        );
        assert_eq!(decoded.header.definition_hash, "blake3:test-definition");
        assert_eq!(decoded.header.dimension, 3);
        assert_eq!(decoded.header.metric, "cosine");
        assert_eq!(decoded.header.algorithm, "hnsw");
        assert_eq!(decoded.header.embedding_provider, "test_only");
        assert_eq!(decoded.header.embedding_model, "embedding-v1");
        assert_eq!(decoded.header.embedding_normalisation, "unit_l2");
        assert_eq!(
            decoded.header.embedding_chunking_hash,
            "blake3:test-chunking"
        );
        assert_eq!(
            decoded.header.extractor_definition_hash,
            "blake3:test-extractor"
        );
        assert_eq!(
            decoded.header.embedding_provenance_hash,
            "blake3:test-provenance"
        );
        assert_eq!(decoded.header.vector_count, 2);
        assert_eq!(decoded.header.ann_format_hash, hnsw_ann_format_hash());
        assert_eq!(decoded.header.codec, "f32_le_v1");
        assert_eq!(decoded.entries.len(), 2);
        assert_eq!(decoded.entries[0].record.vector_id, 1);
        assert_eq!(decoded.entries[1].record.vector_id, 2);
        assert_eq!(decoded.entries[0].source_id_binary, vec![1, 0xaa]);
        assert_eq!(decoded.entries[0].source_generation, 10);
        assert_eq!(decoded.entries[0].labels, vec![42]);
        assert_eq!(decoded.entries[1].source_id_binary, vec![2, 0xaa]);
        assert_eq!(decoded.entries[1].source_generation, 20);
        assert!(decoded.entries[1].labels.is_empty());
        assert_eq!(decoded.body_header.record_table_offset, 0);
        assert_eq!(decoded.body_header.vector_blocks_offset, 0);
        assert_eq!(decoded.body_header.ann_blocks_offset, 0);
        assert_eq!(decoded.body_header.deleted_bitset_offset, 0);
        assert_eq!(decoded.hnsw_graph.node_count, 2);
        assert!(!decoded.hnsw_graph.layers.is_empty());
        assert_eq!(decoded.deleted_bitset, vec![0]);
    }

    fn corrupt_writer_table_value(body: Vec<u8>, table_id: u16) -> Vec<u8> {
        let mut tables = decode_writer_body_tables(&body).unwrap();
        let table = tables
            .iter_mut()
            .find(|table| table.table_id == table_id)
            .expect("writer table exists");
        let row = table.rows.first_mut().expect("writer table row exists");
        let idx = row.value.len().checked_sub(1).expect("writer table value");
        row.value[idx] ^= 1;
        let encoded_tables = tables
            .into_iter()
            .map(|table| WriterBodyTable {
                table_id: table.table_id,
                row_type_id: table.row_type_id,
                rows: table.rows,
            })
            .collect::<Vec<_>>();
        encode_writer_body_tables(&encoded_tables).unwrap()
    }

    #[test]
    fn vector_body_rejects_corrupt_vector_block_crc32c() {
        let mut entries = vec![entry(1, vec![1.0, 0.0, 0.0])];
        let graph = build_hnsw_graph_for_entries(&entries, VectorMetric::Cosine, 32, 200).unwrap();
        let body = encode_vector_body(&mut entries, &graph, &[0]).unwrap();
        let body = corrupt_writer_table_value(body, TABLE_VECTOR_BLOCK);

        assert!(
            decode_vector_body(&body, 3)
                .unwrap_err()
                .to_string()
                .contains("vector block crc32c mismatch")
        );
    }

    #[test]
    fn vector_body_rejects_corrupt_ann_block_crc32c() {
        let mut entries = vec![entry(1, vec![1.0, 0.0, 0.0])];
        let graph = build_hnsw_graph_for_entries(&entries, VectorMetric::Cosine, 32, 200).unwrap();
        let body = encode_vector_body(&mut entries, &graph, &[0]).unwrap();
        let body = corrupt_writer_table_value(body, TABLE_VECTOR_HNSW);

        assert!(
            decode_vector_body(&body, 3)
                .unwrap_err()
                .to_string()
                .contains("ann block crc32c mismatch")
        );
    }

    #[tokio::test]
    async fn vector_segment_footer_protects_body() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let segment_ref = write_vector_segment(
            &storage,
            VectorSegmentWrite {
                index_id: "vector-alpha",
                definition_hash: "blake3:test-definition",
                generation: 6,
                dimension: 3,
                metric: VectorMetric::Cosine,
                embedding_provider: "test_only",
                embedding_model_version: None,
                embedding_normalisation: "unit_l2",
                embedding_chunking_hash: "blake3:test-chunking",
                extractor_definition_hash: "blake3:test-extractor",
                embedding_provenance_hash: "blake3:test-provenance",
                embedding_model: "embedding-v1",
                modality: VectorModality::Text,
                hnsw_m: 32,
                hnsw_ef_construction: 200,
                source_cursor: 88,
                authz_revision: 9,
                boundary_values: &[],
                entries: &[entry(1, vec![1.0, 0.0, 0.0])],
                deleted_bitset: &[0],
            },
        )
        .await
        .unwrap();
        let mut bytes = read_vector_segment_bytes(&storage, &segment_ref)
            .await
            .unwrap();
        bytes[crate::formats::WRITER_SEGMENT_FIXED_HEADER_LEN + 1] ^= 1;
        assert!(decode_vector_segment(&bytes).is_err());
    }

    #[tokio::test]
    async fn vector_segment_rejects_deleted_bitset_length_mismatch() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let err = write_vector_segment(
            &storage,
            VectorSegmentWrite {
                index_id: "vector-alpha",
                definition_hash: "blake3:test-definition",
                generation: 6,
                dimension: 3,
                metric: VectorMetric::Cosine,
                embedding_provider: "test_only",
                embedding_model_version: None,
                embedding_normalisation: "unit_l2",
                embedding_chunking_hash: "blake3:test-chunking",
                extractor_definition_hash: "blake3:test-extractor",
                embedding_provenance_hash: "blake3:test-provenance",
                embedding_model: "embedding-v1",
                modality: VectorModality::Text,
                hnsw_m: 32,
                hnsw_ef_construction: 200,
                source_cursor: 88,
                authz_revision: 9,
                boundary_values: &[],
                entries: &[entry(1, vec![1.0, 0.0, 0.0])],
                deleted_bitset: &[],
            },
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("vector deleted bitset length does not match entry count")
        );
    }

    #[tokio::test]
    async fn latest_vector_segment_selects_highest_generation() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let entries = [entry(1, vec![1.0, 0.0, 0.0])];
        for generation in [1, 3, 2] {
            write_vector_segment(
                &storage,
                VectorSegmentWrite {
                    index_id: "vector-alpha",
                    definition_hash: "blake3:test-definition",
                    generation,
                    dimension: 3,
                    metric: VectorMetric::Cosine,
                    embedding_provider: "test_only",
                    embedding_model: "embedding-v1",
                    embedding_model_version: None,
                    embedding_normalisation: "unit_l2",
                    embedding_chunking_hash: "blake3:test-chunking",
                    extractor_definition_hash: "blake3:test-extractor",
                    embedding_provenance_hash: "blake3:test-provenance",
                    modality: VectorModality::Text,
                    hnsw_m: 32,
                    hnsw_ef_construction: 200,
                    source_cursor: generation,
                    authz_revision: 0,
                    boundary_values: &[],
                    entries: &entries,
                    deleted_bitset: &[0],
                },
            )
            .await
            .unwrap();
        }
        let latest = read_latest_vector_segment(&storage, "vector-alpha")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.header.generation, 3);
        assert!(
            latest_vector_segment_ref(&storage, "../escape")
                .await
                .is_err()
        );
    }
}
