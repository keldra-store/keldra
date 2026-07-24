use crate::{
    core_store::{
        CoreBoundaryValue, CoreMutationPrecondition, CoreObjectRef, CorePipelinePolicy, CoreStore,
        CoreTraceContext, GetBlob,
    },
    formats::{
        FileFamily, Hash32, decode_writer_segment, encode_writer_segment_header,
        full_text::{
            BuiltFullTextPostings, FullTextBodyHeader, Posting, TermEntry, decode_postings,
        },
        hash32, header_field_bytes, header_field_string, header_field_u64, required_header_bytes,
        required_header_string, required_header_u64, single_body_range_index,
        table::{
            TableRow, WriterBodyTable, decode_writer_body_table, decode_writer_body_tables,
            encode_writer_body_tables,
        },
        unix_nanos_from_rfc3339,
        writer::{
            WriterBuildOutput, WriterFamily, WriterSegmentBuildInput,
            build_writer_segment_logical_file, canonical_logical_file_id,
        },
    },
    index_coremeta::{self, IndexSegmentCoreMetaRecord},
    storage::Storage,
    writer_segment_catalog::{WriterSegmentCatalogRecord, write_writer_segment_catalog_record},
    writer_segment_range::RangeAddressedWriterSegment,
};
use anyhow::{Context, Result, anyhow};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, convert::TryInto};

const FULL_TEXT_SEGMENT_REF_PREFIX: &str = "full_text_segment:";

const FULL_TEXT_POSTINGS_BLOCK_MAGIC: &[u8; 8] = b"ANPOST1\0";
const FULL_TEXT_POSTINGS_BLOCK_VERSION: u16 = 1;
const FULL_TEXT_POSTINGS_SKIP_STRIDE: u32 = 128;
const FULL_TEXT_POSTINGS_BLOCK_HEADER_LEN: usize = 8 + 2 + 2 + 4 + 8 + 8 + 4;
const FULL_TEXT_SKIP_ENTRY_LEN: usize = 8 + 8 + 8 + 2 + 16;
const TABLE_FULL_TEXT_FIELD_CATALOG: u16 = 0x0201;
const TABLE_FULL_TEXT_ANALYSER_CATALOG: u16 = 0x0202;
const TABLE_FULL_TEXT_TERM_DICTIONARY: u16 = 0x0203;
const TABLE_FULL_TEXT_POSTINGS_BLOCK: u16 = 0x0204;
const TABLE_FULL_TEXT_POSTINGS_BY_TERM: u16 = 0x0205;
const TABLE_FULL_TEXT_STORED_FIELDS: u16 = 0x0207;

#[derive(Clone, PartialEq, Message)]
struct FullTextFieldCatalogProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    index_id: String,
    #[prost(uint64, tag = "3")]
    generation: u64,
    #[prost(uint64, tag = "4")]
    dictionary_bytes_len: u64,
    #[prost(uint64, tag = "5")]
    term_count: u64,
}

#[derive(Clone, PartialEq, Message)]
struct FullTextAnalyserCatalogProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(bytes, tag = "2")]
    tokenizer_json: Vec<u8>,
    #[prost(bytes, tag = "3")]
    scorer_json: Vec<u8>,
    #[prost(string, tag = "4")]
    codec: String,
}

#[derive(Clone, PartialEq, Message)]
struct FullTextDocumentTableProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(message, repeated, tag = "2")]
    rows: Vec<FullTextDocumentTableRowProto>,
}

#[derive(Clone, PartialEq, Message)]
struct FullTextDocumentTableRowProto {
    #[prost(uint64, tag = "1")]
    document_id: u64,
    #[prost(uint32, tag = "2")]
    field_id: u32,
    #[prost(string, tag = "3")]
    object_key: String,
    #[prost(string, tag = "4")]
    version_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullTextDocumentRef {
    pub document_id: u64,
    pub field_id: u16,
    pub object_key: String,
    pub version_id: uuid::Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullTextDocumentTableRow {
    pub document_id: u64,
    pub field_id: u16,
    pub object_key: String,
    pub version_id: uuid::Uuid,
}

pub fn encode_full_text_document_table(rows: &[FullTextDocumentTableRow]) -> Result<Vec<u8>> {
    encode_proto_message(FullTextDocumentTableProto {
        schema: "anvil.index.full_text.document_table.v1".to_string(),
        rows: rows
            .iter()
            .map(|row| FullTextDocumentTableRowProto {
                document_id: row.document_id,
                field_id: u32::from(row.field_id),
                object_key: row.object_key.clone(),
                version_id: row.version_id.to_string(),
            })
            .collect(),
    })
}

pub fn decode_full_text_document_table(
    bytes: &[u8],
) -> Result<BTreeMap<(u64, u16), FullTextDocumentRef>> {
    let proto =
        FullTextDocumentTableProto::decode(bytes).context("decode full text document table")?;
    if proto.schema != "anvil.index.full_text.document_table.v1" {
        return Err(anyhow!(
            "full text document table schema mismatch: found {:?} in {} bytes",
            proto.schema,
            bytes.len()
        ));
    }
    let mut out = BTreeMap::new();
    for row in proto.rows {
        let field_id =
            u16::try_from(row.field_id).context("full text document table field id exceeds u16")?;
        let version_id = uuid::Uuid::parse_str(&row.version_id)
            .context("full text document table version id is not a UUID")?;
        out.insert(
            (row.document_id, field_id),
            FullTextDocumentRef {
                document_id: row.document_id,
                field_id,
                object_key: row.object_key,
                version_id,
            },
        );
    }
    Ok(out)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullTextSkipEntry {
    pub posting_index: u64,
    pub postings_offset: u64,
    pub document_id: u64,
    pub field_id: u16,
    pub object_version_id: [u8; 16],
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FullTextSegmentHeader {
    pub index_id: String,
    pub generation: u64,
    pub tokenizer: serde_json::Value,
    pub scorer: serde_json::Value,
    pub source_cursor: u64,
    pub authz_revision: u64,
    pub dictionary_bytes_len: u64,
    pub term_count: u64,
    pub postings_bytes_len: u64,
    pub posting_count: u64,
    pub codec: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedFullTextSegment {
    pub header: FullTextSegmentHeader,
    pub body_header: FullTextBodyHeader,
    pub terms: Vec<TermEntry>,
    pub postings: Vec<Posting>,
    pub posting_skips: Vec<FullTextSkipEntry>,
    pub postings_bytes: Vec<u8>,
    pub document_table: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct FullTextSegmentWrite<'a> {
    pub index_id: &'a str,
    pub generation: u64,
    pub tokenizer: serde_json::Value,
    pub scorer: serde_json::Value,
    pub source_cursor: u64,
    pub authz_revision: u64,
    pub boundary_values: &'a [CoreBoundaryValue],
    pub built_postings: &'a BuiltFullTextPostings,
    pub document_table: &'a [u8],
}

#[derive(Debug, Clone)]
pub(crate) struct StagedFullTextSegment {
    pub(crate) segment_ref: String,
    pub(crate) segment_hash: String,
    locator: IndexSegmentCoreMetaRecord,
    catalog: WriterSegmentCatalogRecord,
}

pub async fn write_full_text_segment(
    storage: &Storage,
    write: FullTextSegmentWrite<'_>,
) -> Result<String> {
    let staged = stage_full_text_segment(storage, write).await?;
    publish_full_text_segment_catalog(storage, &staged, &[]).await?;
    publish_full_text_segment_locator(storage, &staged, &[]).await?;
    Ok(staged.segment_ref)
}

pub(crate) async fn stage_full_text_segment(
    storage: &Storage,
    write: FullTextSegmentWrite<'_>,
) -> Result<StagedFullTextSegment> {
    let encoded_terms = encode_terms(&write.built_postings.terms);
    let encoded_postings_block = encode_postings_block(
        &write.built_postings.postings,
        &write.built_postings.postings_bytes,
    )?;
    let postings_bytes = zstd::stream::encode_all(&encoded_postings_block[..], 3)
        .context("compress full text postings block")?;
    let mut timestamp_seed = Vec::new();
    timestamp_seed.extend_from_slice(&encoded_terms);
    timestamp_seed.extend_from_slice(&postings_bytes);
    timestamp_seed.extend_from_slice(write.document_table);
    let timestamp_digest = blake3::hash(&timestamp_seed).to_hex().to_string();
    let created_at_unix_nanos = index_coremeta::deterministic_index_publication_nanos(
        write.index_id,
        "full_text_segment",
        write.generation,
        u128::from(write.source_cursor),
        &timestamp_digest,
    );
    let header = FullTextSegmentHeader {
        index_id: write.index_id.to_string(),
        generation: write.generation,
        tokenizer: write.tokenizer,
        scorer: write.scorer,
        source_cursor: write.source_cursor,
        authz_revision: write.authz_revision,
        dictionary_bytes_len: encoded_terms.len() as u64,
        term_count: write.built_postings.terms.len() as u64,
        postings_bytes_len: postings_bytes.len() as u64,
        posting_count: write.built_postings.postings.len() as u64,
        codec: "zstd".to_string(),
        created_at: chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(created_at_unix_nanos)
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    };
    let body = encode_full_text_body(
        &header,
        &encoded_terms,
        &postings_bytes,
        &write.built_postings.postings_bytes,
        write.document_table,
    )?;
    let body_hash = hash32(&body);
    let ref_name =
        full_text_segment_ref_name(write.index_id, write.generation, &hex::encode(body_hash))?;
    let logical_file_id = canonical_logical_file_id(
        WriterFamily::FullText,
        write.generation,
        &ref_name,
        &body_hash,
    );
    let (first_hash, last_hash) = term_hash_bounds(&write.built_postings.terms);
    let header_proto = encode_full_text_header_proto(&logical_file_id, &header)?;
    let range_index = single_body_range_index(
        body.len(),
        write.built_postings.terms.len() as u64,
        first_hash,
        last_hash,
    )?;
    let built_segment = build_writer_segment_logical_file(WriterSegmentBuildInput {
        file_family: FileFamily::FullTextSegment,
        writer_family: WriterFamily::FullText,
        writer_generation: write.generation,
        logical_file_id,
        header_proto,
        body,
        range_index,
        record_count: write.built_postings.terms.len() as u64,
        first_record_hash: first_hash,
        last_record_hash: last_hash,
        boundary_values: write.boundary_values.to_vec(),
        mutation_id: format!("full-text-segment:{}:{}", write.index_id, write.generation),
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
        .ok_or_else(|| anyhow!("CoreFormatWriter returned no full text object"))?;
    let core_object_ref_target = encode_core_object_ref_target(&object_ref)?;
    let locator = IndexSegmentCoreMetaRecord {
        index_id: write.index_id.to_string(),
        index_kind: "full_text".to_string(),
        writer_family: WriterFamily::FullText.as_str().to_string(),
        segment_ref: ref_name.clone(),
        core_object_ref_target: core_object_ref_target.clone(),
        segment_hash: segment_file_hash.clone(),
        segment_length,
        generation: write.generation,
        source_kind: "object_current".to_string(),
        source_cursor: write.source_cursor,
        authz_realm_id: "default".to_string(),
        authz_scope_hash: index_coremeta::segment_authz_scope_hash("full_text", "per_row_label"),
        authz_revision: write.authz_revision,
        row_count: write.built_postings.terms.len() as u64,
        field_names: Vec::new(),
        created_at_unix_nanos: u64::try_from(created_at_unix_nanos)
            .map_err(|_| anyhow!("full text segment timestamp is negative"))?,
    };
    let catalog = WriterSegmentCatalogRecord {
        family: WriterFamily::FullText.as_str().to_string(),
        scope: write.index_id.to_string(),
        segment_ref: ref_name.clone(),
        core_object_ref_target,
        segment_hash: segment_file_hash.clone(),
        segment_length,
        generation: write.generation,
        source_cursor: write.source_cursor,
        created_at_unix_nanos: locator.created_at_unix_nanos,
    };
    Ok(StagedFullTextSegment {
        segment_ref: ref_name,
        segment_hash: segment_file_hash,
        locator,
        catalog,
    })
}

pub(crate) async fn publish_full_text_segment_catalog(
    storage: &Storage,
    staged: &StagedFullTextSegment,
    additional_preconditions: &[CoreMutationPrecondition],
) -> Result<()> {
    write_writer_segment_catalog_record(storage, &staged.catalog, additional_preconditions).await
}

pub(crate) async fn publish_full_text_segment_locator(
    storage: &Storage,
    staged: &StagedFullTextSegment,
    additional_preconditions: &[CoreMutationPrecondition],
) -> Result<()> {
    index_coremeta::write_index_segment_coremeta_record(
        storage,
        &staged.locator,
        additional_preconditions,
    )
    .await
}

pub async fn read_full_text_segment(
    storage: &Storage,
    segment_ref: &str,
) -> Result<DecodedFullTextSegment> {
    let bytes = read_full_text_segment_bytes(storage, segment_ref).await?;
    decode_full_text_segment(&bytes)
}

pub async fn read_full_text_segment_bytes(storage: &Storage, segment_ref: &str) -> Result<Vec<u8>> {
    let store = CoreStore::new(storage.clone()).await?;
    let index_id = full_text_index_id_from_segment_ref(segment_ref)?;
    let segment =
        index_coremeta::read_index_segment_coremeta_record_by_ref(storage, &index_id, segment_ref)
            .await?
            .ok_or_else(|| anyhow!("full text segment CoreMeta row is missing"))?;
    store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&segment.core_object_ref_target)?,
        })
        .await
}

pub async fn read_latest_full_text_segment(
    storage: &Storage,
    index_id: &str,
) -> Result<Option<DecodedFullTextSegment>> {
    let Some(segment_ref) = latest_full_text_segment_ref(storage, index_id).await? else {
        return Ok(None);
    };
    Ok(Some(read_full_text_segment(storage, &segment_ref).await?))
}

pub async fn read_latest_full_text_segment_terms(
    storage: &Storage,
    index_id: &str,
    query_terms: &[Vec<u8>],
) -> Result<Option<DecodedFullTextSegment>> {
    let Some(segment_ref) = latest_full_text_segment_ref(storage, index_id).await? else {
        return Ok(None);
    };
    read_full_text_segment_terms(storage, &segment_ref, query_terms)
        .await
        .map(Some)
}

pub async fn read_full_text_segment_terms(
    storage: &Storage,
    segment_ref: &str,
    query_terms: &[Vec<u8>],
) -> Result<DecodedFullTextSegment> {
    let segment =
        RangeAddressedWriterSegment::open(storage, segment_ref, FileFamily::FullTextSegment)
            .await?;
    let header = decode_full_text_header_proto(&segment.header)?;
    let directory = segment.read_body_table_directory().await?;
    let dictionary_entry =
        RangeAddressedWriterSegment::table_entry(&directory, TABLE_FULL_TEXT_TERM_DICTIONARY)?;
    let postings_entry =
        RangeAddressedWriterSegment::table_entry(&directory, TABLE_FULL_TEXT_POSTINGS_BY_TERM)?;
    let stored_fields_entry =
        RangeAddressedWriterSegment::table_entry(&directory, TABLE_FULL_TEXT_STORED_FIELDS)?;

    let mut terms = Vec::new();
    let mut postings_bytes = Vec::new();
    let mut postings = Vec::new();
    for term_bytes in query_terms {
        let dictionary_rows = segment
            .read_table_pages_matching_key_prefix(dictionary_entry, term_bytes)
            .await?;
        let Some(dictionary_row) = dictionary_rows
            .into_iter()
            .find(|row| row.key == *term_bytes)
        else {
            continue;
        };
        let (mut term, used) = TermEntry::decode(&dictionary_row.value)?;
        if used != dictionary_row.value.len() {
            return Err(anyhow!("full text dictionary row has trailing bytes"));
        }
        let posting_rows = segment
            .read_table_pages_matching_key_prefix(postings_entry, term_bytes)
            .await?;
        let Some(posting_row) = posting_rows.into_iter().find(|row| row.key == *term_bytes) else {
            continue;
        };
        let offset = postings_bytes.len() as u64;
        let postings_for_term = decode_postings(&posting_row.value)?;
        term.postings_offset = offset;
        term.postings_len = u32::try_from(posting_row.value.len())
            .context("full text term postings row exceeds u32")?;
        term.doc_frequency = postings_for_term.len().min(u32::MAX as usize) as u32;
        postings_bytes.extend_from_slice(&posting_row.value);
        postings.extend(postings_for_term);
        terms.push(term);
    }

    let stored_fields_bytes = segment.read_table_bytes(stored_fields_entry).await?;
    let stored_fields_table = decode_writer_body_table(stored_fields_entry, &stored_fields_bytes)?;
    let document_table = stored_fields_table
        .rows
        .into_iter()
        .find(|row| row.key == b"document-table".as_slice())
        .map(|row| row.value)
        .ok_or_else(|| anyhow!("full text document table missing"))?;

    Ok(DecodedFullTextSegment {
        header,
        body_header: FullTextBodyHeader {
            dictionary_block_count: 1,
            postings_block_count: query_terms.len().min(u32::MAX as usize) as u32,
            document_table_offset: 0,
        },
        terms,
        postings,
        posting_skips: Vec::new(),
        postings_bytes,
        document_table,
    })
}

pub async fn latest_full_text_segment_ref(
    storage: &Storage,
    index_id: &str,
) -> Result<Option<String>> {
    require_safe_component(index_id, "full text index id")?;
    Ok(
        index_coremeta::latest_index_segment_coremeta_record_for_family(
            storage,
            index_id,
            WriterFamily::FullText.as_str(),
        )
        .await?
        .map(|record| record.segment_ref),
    )
}

pub(crate) async fn full_text_segment_hash_exists(
    storage: &Storage,
    index_id: &str,
    generation: u64,
    expected_segment_hash: &str,
) -> Result<bool> {
    require_safe_component(index_id, "full text index id")?;
    validate_hex32(expected_segment_hash, "full text expected segment hash")?;
    Ok(
        index_coremeta::index_segment_coremeta_record_for_family_generation(
            storage,
            index_id,
            WriterFamily::FullText.as_str(),
            generation,
        )
        .await?
        .is_some_and(|record| record.segment_hash == expected_segment_hash),
    )
}

pub fn decode_full_text_segment(bytes: &[u8]) -> Result<DecodedFullTextSegment> {
    let segment = decode_writer_segment(bytes, FileFamily::FullTextSegment)?;
    let header = decode_full_text_header_proto(&segment.header)?;
    decode_full_text_body(header, segment.body)
}

fn encode_full_text_header_proto(
    logical_file_id: &str,
    header: &FullTextSegmentHeader,
) -> Result<Vec<u8>> {
    Ok(encode_writer_segment_header(
        "anvil.index.full_text_segment_header.v1",
        logical_file_id,
        FileFamily::FullTextSegment,
        header.generation,
        None,
        None,
        unix_nanos_from_rfc3339(&header.created_at),
        vec![
            header_field_string("index_id", header.index_id.clone()),
            header_field_bytes("tokenizer_json", canonical_json_bytes(&header.tokenizer)?),
            header_field_bytes("scorer_json", canonical_json_bytes(&header.scorer)?),
            header_field_u64("source_cursor", header.source_cursor),
            header_field_u64("authz_revision", header.authz_revision),
            header_field_u64("dictionary_bytes_len", header.dictionary_bytes_len),
            header_field_u64("term_count", header.term_count),
            header_field_u64("postings_bytes_len", header.postings_bytes_len),
            header_field_u64("posting_count", header.posting_count),
            header_field_string("codec", header.codec.clone()),
            header_field_string("created_at", header.created_at.clone()),
        ],
    ))
}

fn decode_full_text_header_proto(
    header: &crate::formats::WriterSegmentHeaderProto,
) -> Result<FullTextSegmentHeader> {
    Ok(FullTextSegmentHeader {
        index_id: required_header_string(header, "index_id")?,
        generation: header.writer_generation,
        tokenizer: serde_json::from_slice(&required_header_bytes(header, "tokenizer_json")?)?,
        scorer: serde_json::from_slice(&required_header_bytes(header, "scorer_json")?)?,
        source_cursor: required_header_u64(header, "source_cursor")?,
        authz_revision: required_header_u64(header, "authz_revision")?,
        dictionary_bytes_len: required_header_u64(header, "dictionary_bytes_len")?,
        term_count: required_header_u64(header, "term_count")?,
        postings_bytes_len: required_header_u64(header, "postings_bytes_len")?,
        posting_count: required_header_u64(header, "posting_count")?,
        codec: required_header_string(header, "codec")?,
        created_at: required_header_string(header, "created_at")?,
    })
}

fn canonical_json_bytes(value: &serde_json::Value) -> Result<Vec<u8>> {
    serde_json::to_vec(&canonical_json(value)).context("encode canonical full text JSON metadata")
}

fn canonical_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(canonical_json).collect())
        }
        serde_json::Value::Object(values) => {
            let mut sorted = serde_json::Map::new();
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                sorted.insert(key.clone(), canonical_json(&values[key]));
            }
            serde_json::Value::Object(sorted)
        }
        other => other.clone(),
    }
}

fn encode_full_text_body(
    header: &FullTextSegmentHeader,
    dictionary_bytes: &[u8],
    postings_bytes: &[u8],
    raw_postings_bytes: &[u8],
    document_table: &[u8],
) -> Result<Vec<u8>> {
    let term_rows = dictionary_rows(dictionary_bytes)?;
    encode_writer_body_tables(&[
        WriterBodyTable {
            table_id: TABLE_FULL_TEXT_FIELD_CATALOG,
            row_type_id: TABLE_FULL_TEXT_FIELD_CATALOG,
            rows: vec![TableRow {
                key: header.index_id.as_bytes().to_vec(),
                value: encode_full_text_field_catalog(header)?,
            }],
        },
        WriterBodyTable {
            table_id: TABLE_FULL_TEXT_ANALYSER_CATALOG,
            row_type_id: TABLE_FULL_TEXT_ANALYSER_CATALOG,
            rows: vec![TableRow {
                key: full_text_analyser_catalog_key(header)?,
                value: encode_full_text_analyser_catalog(header)?,
            }],
        },
        WriterBodyTable {
            table_id: TABLE_FULL_TEXT_TERM_DICTIONARY,
            row_type_id: TABLE_FULL_TEXT_TERM_DICTIONARY,
            rows: term_rows,
        },
        WriterBodyTable {
            table_id: TABLE_FULL_TEXT_POSTINGS_BLOCK,
            row_type_id: TABLE_FULL_TEXT_POSTINGS_BLOCK,
            rows: vec![TableRow {
                key: b"postings-block-0".to_vec(),
                value: postings_bytes.to_vec(),
            }],
        },
        WriterBodyTable {
            table_id: TABLE_FULL_TEXT_POSTINGS_BY_TERM,
            row_type_id: TABLE_FULL_TEXT_POSTINGS_BY_TERM,
            rows: postings_by_term_rows(dictionary_bytes, raw_postings_bytes)?,
        },
        WriterBodyTable {
            table_id: TABLE_FULL_TEXT_STORED_FIELDS,
            row_type_id: TABLE_FULL_TEXT_STORED_FIELDS,
            rows: vec![TableRow {
                key: b"document-table".to_vec(),
                value: document_table.to_vec(),
            }],
        },
    ])
    .map_err(anyhow::Error::from)
}

fn encode_full_text_field_catalog(header: &FullTextSegmentHeader) -> Result<Vec<u8>> {
    encode_proto_message(FullTextFieldCatalogProto {
        schema: "anvil.index.full_text.field_catalog.v1".to_string(),
        index_id: header.index_id.clone(),
        generation: header.generation,
        dictionary_bytes_len: header.dictionary_bytes_len,
        term_count: header.term_count,
    })
}

fn encode_full_text_analyser_catalog(header: &FullTextSegmentHeader) -> Result<Vec<u8>> {
    encode_proto_message(FullTextAnalyserCatalogProto {
        schema: "anvil.index.full_text.analyser_catalog.v1".to_string(),
        tokenizer_json: canonical_json_bytes(&header.tokenizer)?,
        scorer_json: canonical_json_bytes(&header.scorer)?,
        codec: header.codec.clone(),
    })
}

fn full_text_analyser_catalog_key(header: &FullTextSegmentHeader) -> Result<Vec<u8>> {
    let mut key = Vec::new();
    key.extend_from_slice(&hash32(&canonical_json_bytes(&header.tokenizer)?));
    key.extend_from_slice(&hash32(&canonical_json_bytes(&header.scorer)?));
    Ok(key)
}

fn encode_proto_message(message: impl Message) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    message.encode(&mut bytes)?;
    Ok(bytes)
}

fn decode_full_text_body(
    header: FullTextSegmentHeader,
    body: &[u8],
) -> Result<DecodedFullTextSegment> {
    let (dictionary_bytes, encoded_postings_bytes, document_table) = decode_full_text_tables(body)?;
    if dictionary_bytes.len() as u64 != header.dictionary_bytes_len {
        return Err(anyhow!("full text dictionary length does not match header"));
    }
    if encoded_postings_bytes.len() as u64 != header.postings_bytes_len {
        return Err(anyhow!("full text postings length does not match header"));
    }
    let terms = decode_terms(&dictionary_bytes, header.term_count)?;
    let encoded_postings_block = match header.codec.as_str() {
        "zstd" => zstd::stream::decode_all(encoded_postings_bytes.as_slice())
            .context("decompress full text postings block")?,
        other => return Err(anyhow!("unsupported full text postings codec {other}")),
    };
    let (postings_bytes, posting_skips) =
        decode_postings_block(&encoded_postings_block, header.posting_count)?;
    let postings = decode_postings(&postings_bytes)?;
    if postings.len() as u64 != header.posting_count {
        return Err(anyhow!("full text posting count does not match header"));
    }
    Ok(DecodedFullTextSegment {
        header,
        body_header: FullTextBodyHeader {
            dictionary_block_count: 1,
            postings_block_count: 1,
            document_table_offset: 0,
        },
        terms,
        postings,
        posting_skips,
        postings_bytes,
        document_table,
    })
}

fn dictionary_rows(dictionary_bytes: &[u8]) -> Result<Vec<TableRow>> {
    let mut rows = Vec::new();
    let mut input = dictionary_bytes;
    while !input.is_empty() {
        let (term, used) = TermEntry::decode(input)?;
        let encoded = term.encode();
        rows.push(TableRow {
            key: term.term_utf8.clone(),
            value: encoded,
        });
        input = &input[used..];
    }
    rows.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(rows)
}

fn postings_by_term_rows(
    dictionary_bytes: &[u8],
    raw_postings_bytes: &[u8],
) -> Result<Vec<TableRow>> {
    let mut rows = Vec::new();
    let mut input = dictionary_bytes;
    while !input.is_empty() {
        let (term, used) = TermEntry::decode(input)?;
        let start = usize::try_from(term.postings_offset)
            .context("full text term postings offset exceeds usize")?;
        let len = usize::try_from(term.postings_len)
            .context("full text term postings length exceeds usize")?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| anyhow!("full text term postings range overflow"))?;
        let postings = raw_postings_bytes
            .get(start..end)
            .ok_or_else(|| anyhow!("full text term postings range is outside postings bytes"))?;
        rows.push(TableRow {
            key: term.term_utf8.clone(),
            value: postings.to_vec(),
        });
        input = &input[used..];
    }
    rows.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(rows)
}

fn decode_full_text_tables(body: &[u8]) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    let tables = decode_writer_body_tables(body)?;
    let mut dictionary_bytes = Vec::new();
    let mut postings_bytes = None;
    let mut document_table = None;
    for table in tables {
        match table.table_id {
            TABLE_FULL_TEXT_TERM_DICTIONARY => {
                for row in table.rows {
                    dictionary_bytes.extend_from_slice(&row.value);
                }
            }
            TABLE_FULL_TEXT_POSTINGS_BLOCK => {
                let row = table
                    .rows
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("full text postings table is empty"))?;
                postings_bytes = Some(row.value);
            }
            TABLE_FULL_TEXT_STORED_FIELDS => {
                let row = table
                    .rows
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("full text document table is empty"))?;
                document_table = Some(row.value);
            }
            _ => {}
        }
    }
    Ok((
        dictionary_bytes,
        postings_bytes.ok_or_else(|| anyhow!("full text postings table missing"))?,
        document_table.ok_or_else(|| anyhow!("full text document table missing"))?,
    ))
}

fn encode_postings_block(postings: &[Posting], raw_postings_bytes: &[u8]) -> Result<Vec<u8>> {
    let skip_entries = build_skip_entries(postings, raw_postings_bytes)?;
    let skip_bytes_len = skip_entries
        .len()
        .checked_mul(FULL_TEXT_SKIP_ENTRY_LEN)
        .ok_or_else(|| anyhow!("full text skip table length overflow"))?;
    let capacity = FULL_TEXT_POSTINGS_BLOCK_HEADER_LEN
        .checked_add(skip_bytes_len)
        .and_then(|value| value.checked_add(raw_postings_bytes.len()))
        .ok_or_else(|| anyhow!("full text postings block length overflow"))?;
    let mut out = Vec::with_capacity(capacity);
    out.extend_from_slice(FULL_TEXT_POSTINGS_BLOCK_MAGIC);
    out.extend_from_slice(&FULL_TEXT_POSTINGS_BLOCK_VERSION.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&FULL_TEXT_POSTINGS_SKIP_STRIDE.to_le_bytes());
    out.extend_from_slice(&(postings.len() as u64).to_le_bytes());
    out.extend_from_slice(&(raw_postings_bytes.len() as u64).to_le_bytes());
    out.extend_from_slice(&(skip_entries.len() as u32).to_le_bytes());
    for entry in &skip_entries {
        out.extend_from_slice(&entry.posting_index.to_le_bytes());
        out.extend_from_slice(&entry.postings_offset.to_le_bytes());
        out.extend_from_slice(&entry.document_id.to_le_bytes());
        out.extend_from_slice(&entry.field_id.to_le_bytes());
        out.extend_from_slice(&entry.object_version_id);
    }
    out.extend_from_slice(raw_postings_bytes);
    Ok(out)
}

fn build_skip_entries(
    postings: &[Posting],
    raw_postings_bytes: &[u8],
) -> Result<Vec<FullTextSkipEntry>> {
    let mut skips = Vec::new();
    let mut cursor = 0usize;
    for (idx, posting) in postings.iter().enumerate() {
        let encoded = posting.encode();
        let end = cursor
            .checked_add(encoded.len())
            .ok_or_else(|| anyhow!("full text posting offset overflow"))?;
        if raw_postings_bytes.get(cursor..end) != Some(encoded.as_slice()) {
            return Err(anyhow!(
                "full text raw postings bytes do not match posting entries"
            ));
        }
        if idx % FULL_TEXT_POSTINGS_SKIP_STRIDE as usize == 0 {
            skips.push(FullTextSkipEntry {
                posting_index: idx as u64,
                postings_offset: cursor as u64,
                document_id: posting.document_id,
                field_id: posting.field_id,
                object_version_id: posting.object_version_id,
            });
        }
        cursor = end;
    }
    if cursor != raw_postings_bytes.len() {
        return Err(anyhow!(
            "full text raw postings bytes contain trailing data"
        ));
    }
    Ok(skips)
}

fn decode_postings_block(
    input: &[u8],
    expected_posting_count: u64,
) -> Result<(Vec<u8>, Vec<FullTextSkipEntry>)> {
    if input.len() < FULL_TEXT_POSTINGS_BLOCK_HEADER_LEN {
        return Err(anyhow!("full text postings block is shorter than header"));
    }
    if &input[0..8] != FULL_TEXT_POSTINGS_BLOCK_MAGIC {
        return Err(anyhow!("full text postings block magic mismatch"));
    }
    let version = u16::from_le_bytes(input[8..10].try_into().unwrap());
    if version != FULL_TEXT_POSTINGS_BLOCK_VERSION {
        return Err(anyhow!(
            "unsupported full text postings block version {version}"
        ));
    }
    let reserved = u16::from_le_bytes(input[10..12].try_into().unwrap());
    if reserved != 0 {
        return Err(anyhow!(
            "full text postings block reserved bytes are non-zero"
        ));
    }
    let skip_stride = u32::from_le_bytes(input[12..16].try_into().unwrap());
    if skip_stride != FULL_TEXT_POSTINGS_SKIP_STRIDE {
        return Err(anyhow!(
            "unsupported full text postings skip stride {skip_stride}"
        ));
    }
    let posting_count = u64::from_le_bytes(input[16..24].try_into().unwrap());
    if posting_count != expected_posting_count {
        return Err(anyhow!(
            "full text postings block count does not match segment header"
        ));
    }
    let raw_postings_len = usize::try_from(u64::from_le_bytes(input[24..32].try_into().unwrap()))
        .context("full text raw postings length exceeds usize")?;
    let skip_count = usize::try_from(u32::from_le_bytes(input[32..36].try_into().unwrap()))
        .context("full text skip count exceeds usize")?;
    let skip_bytes_len = skip_count
        .checked_mul(FULL_TEXT_SKIP_ENTRY_LEN)
        .ok_or_else(|| anyhow!("full text skip table length overflow"))?;
    let raw_start = FULL_TEXT_POSTINGS_BLOCK_HEADER_LEN
        .checked_add(skip_bytes_len)
        .ok_or_else(|| anyhow!("full text postings block raw start overflow"))?;
    let raw_end = raw_start
        .checked_add(raw_postings_len)
        .ok_or_else(|| anyhow!("full text postings block raw end overflow"))?;
    if raw_end != input.len() {
        return Err(anyhow!(
            "full text postings block lengths do not match payload length"
        ));
    }

    let mut skips = Vec::with_capacity(skip_count);
    let mut cursor = FULL_TEXT_POSTINGS_BLOCK_HEADER_LEN;
    for _ in 0..skip_count {
        skips.push(FullTextSkipEntry {
            posting_index: u64::from_le_bytes(input[cursor..cursor + 8].try_into().unwrap()),
            postings_offset: u64::from_le_bytes(input[cursor + 8..cursor + 16].try_into().unwrap()),
            document_id: u64::from_le_bytes(input[cursor + 16..cursor + 24].try_into().unwrap()),
            field_id: u16::from_le_bytes(input[cursor + 24..cursor + 26].try_into().unwrap()),
            object_version_id: input[cursor + 26..cursor + 42].try_into().unwrap(),
        });
        cursor += FULL_TEXT_SKIP_ENTRY_LEN;
    }
    let raw_postings = input[raw_start..raw_end].to_vec();
    validate_skip_entries(&raw_postings, &skips, posting_count)?;
    Ok((raw_postings, skips))
}

fn validate_skip_entries(
    raw_postings_bytes: &[u8],
    skips: &[FullTextSkipEntry],
    posting_count: u64,
) -> Result<()> {
    let expected_skip_count = if posting_count == 0 {
        0
    } else {
        ((posting_count - 1) / FULL_TEXT_POSTINGS_SKIP_STRIDE as u64) + 1
    };
    if skips.len() as u64 != expected_skip_count {
        return Err(anyhow!("full text skip count does not match posting count"));
    }

    let mut next_skip = 0usize;
    let mut cursor = 0usize;
    let mut posting_index = 0u64;
    while cursor < raw_postings_bytes.len() {
        let (posting, used) = Posting::decode(&raw_postings_bytes[cursor..])
            .map_err(|err| anyhow!("decode full text posting for skip validation: {err}"))?;
        if posting_index % FULL_TEXT_POSTINGS_SKIP_STRIDE as u64 == 0 {
            let Some(skip) = skips.get(next_skip) else {
                return Err(anyhow!("full text skip table ended early"));
            };
            if skip.posting_index != posting_index
                || skip.postings_offset != cursor as u64
                || skip.document_id != posting.document_id
                || skip.field_id != posting.field_id
                || skip.object_version_id != posting.object_version_id
            {
                return Err(anyhow!(
                    "full text skip entry does not match posting at index {posting_index}"
                ));
            }
            next_skip += 1;
        }
        cursor = cursor
            .checked_add(used)
            .ok_or_else(|| anyhow!("full text posting cursor overflow"))?;
        posting_index += 1;
    }
    if posting_index != posting_count {
        return Err(anyhow!(
            "full text decoded posting count does not match block header"
        ));
    }
    if next_skip != skips.len() {
        return Err(anyhow!("full text skip table contains trailing entries"));
    }
    Ok(())
}

fn encode_terms(terms: &[TermEntry]) -> Vec<u8> {
    let len = terms.iter().map(|term| term.encode().len()).sum();
    let mut out = Vec::with_capacity(len);
    for term in terms {
        out.extend_from_slice(&term.encode());
    }
    out
}

fn decode_terms(mut input: &[u8], expected_count: u64) -> Result<Vec<TermEntry>> {
    let mut terms = Vec::with_capacity(expected_count as usize);
    for _ in 0..expected_count {
        let (term, used) = TermEntry::decode(input)?;
        terms.push(term);
        input = &input[used..];
    }
    if !input.is_empty() {
        return Err(anyhow!("full text dictionary has trailing bytes"));
    }
    Ok(terms)
}

fn term_hash_bounds(terms: &[TermEntry]) -> (Hash32, Hash32) {
    let first = terms
        .first()
        .map(|term| hash32(&term.encode()))
        .unwrap_or([0; 32]);
    let last = terms
        .last()
        .map(|term| hash32(&term.encode()))
        .unwrap_or([0; 32]);
    (first, last)
}

fn full_text_segment_ref_prefix(index_id: &str) -> Result<String> {
    require_safe_component(index_id, "full text index id")?;
    Ok(format!("{FULL_TEXT_SEGMENT_REF_PREFIX}index:{index_id}:"))
}

fn full_text_segment_ref_name(
    index_id: &str,
    generation: u64,
    segment_hash: &str,
) -> Result<String> {
    validate_hex32(segment_hash, "full text segment hash")?;
    Ok(format!(
        "{}generation:{generation:020}:hash:{segment_hash}",
        full_text_segment_ref_prefix(index_id)?
    ))
}

fn full_text_index_id_from_segment_ref(segment_ref: &str) -> Result<String> {
    let rest = segment_ref
        .strip_prefix(FULL_TEXT_SEGMENT_REF_PREFIX)
        .ok_or_else(|| anyhow!("full text segment ref has invalid prefix"))?;
    let rest = rest
        .strip_prefix("index:")
        .ok_or_else(|| anyhow!("full text segment ref is missing index component"))?;
    let (index_id, _) = rest
        .split_once(":generation:")
        .ok_or_else(|| anyhow!("full text segment ref is missing generation component"))?;
    require_safe_component(index_id, "full text index id")?;
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
    use crate::formats::full_text::{FullTextDocument, TokenizerConfig, build_full_text_postings};
    use tempfile::tempdir;

    #[tokio::test]
    async fn full_text_segment_round_trips_built_postings() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let config = TokenizerConfig::default();
        let built = build_full_text_postings(
            &[
                FullTextDocument {
                    document_id: 1,
                    field_id: 1,
                    object_version_id: [1; 16],
                    authz_label_hash: [2; 32],
                    text: "Alpha beta alpha",
                },
                FullTextDocument {
                    document_id: 2,
                    field_id: 1,
                    object_version_id: [3; 16],
                    authz_label_hash: [4; 32],
                    text: "beta gamma",
                },
            ],
            &config,
        );
        let document_table = br#"{"documents":[1,2]}"#;

        let segment_ref = write_full_text_segment(
            &storage,
            FullTextSegmentWrite {
                index_id: "index-alpha",
                generation: 5,
                tokenizer: serde_json::json!({"language": "simple"}),
                scorer: serde_json::json!({"kind": "bm25"}),
                source_cursor: 44,
                authz_revision: 7,
                boundary_values: &[],
                built_postings: &built,
                document_table,
            },
        )
        .await
        .unwrap();
        assert!(segment_ref.starts_with(FULL_TEXT_SEGMENT_REF_PREFIX));

        let decoded = read_full_text_segment(&storage, &segment_ref)
            .await
            .unwrap();
        assert_eq!(decoded.header.index_id, "index-alpha");
        assert_eq!(decoded.header.source_cursor, 44);
        assert_eq!(decoded.header.authz_revision, 7);
        assert_eq!(decoded.header.codec, "zstd");
        let expected_block = encode_postings_block(&built.postings, &built.postings_bytes).unwrap();
        let expected_compressed = zstd::stream::encode_all(&expected_block[..], 3).unwrap();
        assert_ne!(
            expected_compressed, expected_block,
            "test fixture should prove on-disk postings are compressed bytes"
        );
        assert_eq!(
            decoded.header.postings_bytes_len,
            expected_compressed.len() as u64
        );
        assert_eq!(decoded.terms, built.terms);
        assert_eq!(decoded.postings, built.postings);
        assert_eq!(
            decoded.posting_skips,
            build_skip_entries(&built.postings, &built.postings_bytes).unwrap()
        );
        assert_eq!(decoded.postings_bytes, built.postings_bytes);
        assert_eq!(decoded.document_table, document_table);
    }

    #[tokio::test]
    async fn full_text_segment_writes_skip_data_every_128_postings() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let config = TokenizerConfig::default();
        let documents = (0..260)
            .map(|idx| FullTextDocument {
                document_id: idx + 1,
                field_id: 1,
                object_version_id: [idx as u8; 16],
                authz_label_hash: [9; 32],
                text: "shared",
            })
            .collect::<Vec<_>>();
        let built = build_full_text_postings(&documents, &config);

        let segment_ref = write_full_text_segment(
            &storage,
            FullTextSegmentWrite {
                index_id: "index-shared",
                generation: 1,
                tokenizer: serde_json::json!({"language": "simple"}),
                scorer: serde_json::json!({"kind": "bm25"}),
                source_cursor: 260,
                authz_revision: 11,
                boundary_values: &[],
                built_postings: &built,
                document_table: b"",
            },
        )
        .await
        .unwrap();

        let decoded = read_full_text_segment(&storage, &segment_ref)
            .await
            .unwrap();
        assert_eq!(decoded.postings.len(), 260);
        assert_eq!(decoded.postings, built.postings);
        assert_eq!(decoded.postings_bytes, built.postings_bytes);
        assert_eq!(
            decoded
                .posting_skips
                .iter()
                .map(|entry| entry.posting_index)
                .collect::<Vec<_>>(),
            vec![0, 128, 256]
        );

        let mut cursor = 0u64;
        let mut expected_offsets = Vec::new();
        for (idx, posting) in built.postings.iter().enumerate() {
            if idx % FULL_TEXT_POSTINGS_SKIP_STRIDE as usize == 0 {
                expected_offsets.push(cursor);
            }
            cursor += posting.encode().len() as u64;
        }
        assert_eq!(
            decoded
                .posting_skips
                .iter()
                .map(|entry| entry.postings_offset)
                .collect::<Vec<_>>(),
            expected_offsets
        );
    }

    #[test]
    fn full_text_postings_block_rejects_corrupt_skip_data() {
        let documents = (0..130)
            .map(|idx| FullTextDocument {
                document_id: idx + 1,
                field_id: 1,
                object_version_id: [idx as u8; 16],
                authz_label_hash: [8; 32],
                text: "shared",
            })
            .collect::<Vec<_>>();
        let built = build_full_text_postings(&documents, &TokenizerConfig::default());
        let mut encoded = encode_postings_block(&built.postings, &built.postings_bytes).unwrap();

        let second_skip_offset = FULL_TEXT_POSTINGS_BLOCK_HEADER_LEN + FULL_TEXT_SKIP_ENTRY_LEN + 8;
        encoded[second_skip_offset] ^= 1;

        assert!(
            decode_postings_block(&encoded, built.postings.len() as u64)
                .unwrap_err()
                .to_string()
                .contains("skip entry does not match")
        );
    }

    #[tokio::test]
    async fn full_text_segment_footer_protects_body() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let built = build_full_text_postings(&[], &TokenizerConfig::default());
        let segment_ref = write_full_text_segment(
            &storage,
            FullTextSegmentWrite {
                index_id: "index-alpha",
                generation: 5,
                tokenizer: serde_json::json!({}),
                scorer: serde_json::json!({}),
                source_cursor: 0,
                authz_revision: 0,
                boundary_values: &[],
                built_postings: &built,
                document_table: b"",
            },
        )
        .await
        .unwrap();
        let mut bytes = read_full_text_segment_bytes(&storage, &segment_ref)
            .await
            .unwrap();
        bytes[crate::formats::WRITER_SEGMENT_FIXED_HEADER_LEN + 1] ^= 1;
        assert!(decode_full_text_segment(&bytes).is_err());
    }

    #[tokio::test]
    async fn latest_full_text_segment_selects_highest_generation() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let built = build_full_text_postings(&[], &TokenizerConfig::default());
        for generation in [1, 3, 2] {
            write_full_text_segment(
                &storage,
                FullTextSegmentWrite {
                    index_id: "index-alpha",
                    generation,
                    tokenizer: serde_json::json!({}),
                    scorer: serde_json::json!({}),
                    source_cursor: generation,
                    authz_revision: 0,
                    boundary_values: &[],
                    built_postings: &built,
                    document_table: b"",
                },
            )
            .await
            .unwrap();
        }
        let latest = read_latest_full_text_segment(&storage, "index-alpha")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.header.generation, 3);
        assert!(
            latest_full_text_segment_ref(&storage, "../escape")
                .await
                .is_err()
        );
    }
}
