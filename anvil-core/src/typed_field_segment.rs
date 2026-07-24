use crate::{
    core_store::{
        CoreBoundaryValue, CoreMutationPrecondition, CoreObjectRef, CorePipelinePolicy, CoreStore,
        CoreTraceContext, EncodedTypedValue, GetBlob, SourceId, TypedFieldValue,
    },
    formats::{
        FileFamily, Hash32, decode_writer_segment, encode_writer_segment_header, hash32,
        header_field_string, header_field_strings, header_field_u64, required_header_string,
        required_header_strings, required_header_u64, single_body_range_index,
        table::{TableRow, WriterBodyTable, decode_writer_body_tables, encode_writer_body_tables},
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
use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

const TYPED_FIELD_SEGMENT_REF_PREFIX: &str = "typed_field_segment:";
const TYPED_FIELD_BODY_MAGIC: &[u8; 8] = b"ANVTFRW1";
const TYPED_FIELD_BODY_VERSION: u16 = 1;
const TABLE_TYPED_FIELD_CATALOG: u16 = 0x0401;
const TABLE_TYPED_SORTED_COLUMN: u16 = 0x0402;
const TABLE_TYPED_FIELD_VALUE_INDEX: u16 = 0x0403;
const TABLE_TYPED_RANGE_FENCE: u16 = 0x0404;
const TABLE_TYPED_ROW_BY_ORDINAL: u16 = 0x0405;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TypedFieldSegmentHeader {
    pub index_id: String,
    pub generation: u64,
    pub source_kind: String,
    pub source_cursor: u64,
    pub authz_revision: u64,
    pub definition_hash: String,
    pub row_count: u64,
    pub field_names: Vec<String>,
    pub codec: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TypedFieldSegmentRow {
    pub object_key: String,
    pub object_version_id: String,
    pub source_identity: String,
    #[serde(default)]
    pub values: BTreeMap<String, JsonValue>,
    #[serde(default)]
    pub encoded_values: BTreeMap<String, Vec<u8>>,
    #[serde(default)]
    pub source_id_binary: Vec<u8>,
    #[serde(default)]
    pub value_flags: u32,
    pub authz_label_hash: String,
    pub authz_revision: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecodedTypedFieldSegment {
    pub header: TypedFieldSegmentHeader,
    pub rows: Vec<TypedFieldSegmentRow>,
    pub value_index: Vec<TypedFieldValueIndexEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedFieldValueIndexEntry {
    pub field_name: String,
    pub encoded_value: Vec<u8>,
    pub source_identity: String,
    pub row_ordinal: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TypedFieldValueIndexLookup {
    pub field_name: String,
    pub encoded_value: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct TypedFieldSegmentWrite<'a> {
    pub index_id: &'a str,
    pub generation: u64,
    pub source_kind: &'a str,
    pub source_cursor: u64,
    pub authz_revision: u64,
    pub boundary_values: &'a [CoreBoundaryValue],
    pub definition_hash: &'a str,
    pub field_names: &'a [String],
    pub rows: &'a [TypedFieldSegmentRow],
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct StoredFields {
    object_key: String,
    object_version_id: String,
    source_identity: String,
    values: BTreeMap<String, JsonValue>,
    authz_label_hash: String,
    authz_revision: u64,
}

#[derive(Clone, PartialEq, Message)]
struct StoredFieldsProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    object_key: String,
    #[prost(string, tag = "3")]
    object_version_id: String,
    #[prost(string, tag = "4")]
    source_identity: String,
    #[prost(message, repeated, tag = "5")]
    values: Vec<StoredJsonFieldProto>,
    #[prost(string, tag = "6")]
    authz_label_hash: String,
    #[prost(uint64, tag = "7")]
    authz_revision: u64,
}

#[derive(Clone, PartialEq, Message)]
struct StoredJsonFieldProto {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(message, optional, tag = "2")]
    value: Option<StoredJsonValueProto>,
}

#[derive(Clone, PartialEq, Message)]
struct StoredJsonValueProto {
    #[prost(string, tag = "1")]
    kind: String,
    #[prost(string, tag = "2")]
    string_value: String,
    #[prost(bool, tag = "3")]
    bool_value: bool,
    #[prost(int64, tag = "4")]
    int64_value: i64,
    #[prost(uint64, tag = "5")]
    uint64_value: u64,
    #[prost(double, tag = "6")]
    f64_value: f64,
    #[prost(message, repeated, tag = "7")]
    array_values: Vec<StoredJsonValueProto>,
    #[prost(message, repeated, tag = "8")]
    object_fields: Vec<StoredJsonFieldProto>,
}

#[derive(Debug, Clone)]
pub(crate) struct StagedTypedFieldSegment {
    pub(crate) segment_ref: String,
    pub(crate) segment_hash: String,
    locator: IndexSegmentCoreMetaRecord,
    catalog: WriterSegmentCatalogRecord,
}

pub async fn write_typed_field_segment(
    storage: &Storage,
    write: TypedFieldSegmentWrite<'_>,
) -> Result<String> {
    let staged = stage_typed_field_segment(storage, write).await?;
    publish_typed_field_segment_catalog(storage, &staged, &[]).await?;
    publish_typed_field_segment_locator(storage, &staged, &[]).await?;
    Ok(staged.segment_ref)
}

pub(crate) async fn stage_typed_field_segment(
    storage: &Storage,
    write: TypedFieldSegmentWrite<'_>,
) -> Result<StagedTypedFieldSegment> {
    validate_hex32(write.definition_hash, "typed field definition hash")?;
    let mut rows = write.rows.to_vec();
    rows.sort_by(|left, right| left.source_identity.cmp(&right.source_identity));
    for row in &mut rows {
        if row.encoded_values.is_empty() {
            row.encoded_values = encode_row_values(&row.values)?;
        }
        if row.source_id_binary.is_empty() {
            row.source_id_binary = row.source_identity.as_bytes().to_vec();
        }
    }

    let body = encode_typed_field_body(write.field_names, &rows)?;
    let segment_hash = hash32(&body);
    let ref_name =
        typed_field_segment_ref_name(write.index_id, write.generation, &hex::encode(segment_hash))?;
    let logical_file_id = canonical_logical_file_id(
        WriterFamily::TypedMetadata,
        write.generation,
        &ref_name,
        &segment_hash,
    );

    let created_at_unix_nanos = index_coremeta::deterministic_index_publication_nanos(
        write.index_id,
        "typed_field_segment",
        write.generation,
        u128::from(write.source_cursor),
        &hex::encode(segment_hash),
    );
    let header = TypedFieldSegmentHeader {
        index_id: write.index_id.to_string(),
        generation: write.generation,
        source_kind: write.source_kind.to_string(),
        source_cursor: write.source_cursor,
        authz_revision: write.authz_revision,
        definition_hash: write.definition_hash.to_string(),
        row_count: rows.len() as u64,
        field_names: write.field_names.to_vec(),
        codec: "typed-row-binary-v1".to_string(),
        created_at: chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(created_at_unix_nanos)
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    };
    let (first_hash, last_hash) = source_identity_hash_bounds(&rows);
    let header_proto = encode_typed_field_header_proto(&logical_file_id, &header);
    let range_index =
        single_body_range_index(body.len(), rows.len() as u64, first_hash, last_hash)?;
    let built_segment = build_writer_segment_logical_file(WriterSegmentBuildInput {
        file_family: FileFamily::TypedFieldSegment,
        writer_family: WriterFamily::TypedMetadata,
        writer_generation: write.generation,
        logical_file_id,
        header_proto,
        body,
        range_index,
        record_count: rows.len() as u64,
        first_record_hash: first_hash,
        last_record_hash: last_hash,
        boundary_values: write.boundary_values.to_vec(),
        mutation_id: format!(
            "typed-field-segment:{}:{}",
            write.index_id, write.generation
        ),
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
        .ok_or_else(|| anyhow!("CoreFormatWriter returned no typed object"))?;
    let core_object_ref_target = encode_core_object_ref_target(&object_ref)?;
    let locator = IndexSegmentCoreMetaRecord {
        index_id: write.index_id.to_string(),
        index_kind: index_coremeta::typed_segment_index_kind(write.source_kind).to_string(),
        writer_family: WriterFamily::TypedMetadata.as_str().to_string(),
        segment_ref: ref_name.clone(),
        core_object_ref_target: core_object_ref_target.clone(),
        segment_hash: segment_file_hash.clone(),
        segment_length,
        generation: write.generation,
        source_kind: write.source_kind.to_string(),
        source_cursor: write.source_cursor,
        authz_realm_id: "default".to_string(),
        authz_scope_hash: index_coremeta::segment_authz_scope_hash(
            index_coremeta::typed_segment_index_kind(write.source_kind),
            "per_row_label",
        ),
        authz_revision: write.authz_revision,
        row_count: rows.len() as u64,
        field_names: write.field_names.to_vec(),
        created_at_unix_nanos: u64::try_from(created_at_unix_nanos)
            .map_err(|_| anyhow!("typed field segment timestamp is negative"))?,
    };
    let catalog = WriterSegmentCatalogRecord {
        family: WriterFamily::TypedMetadata.as_str().to_string(),
        scope: write.index_id.to_string(),
        segment_ref: ref_name.clone(),
        core_object_ref_target,
        segment_hash: segment_file_hash.clone(),
        segment_length,
        generation: write.generation,
        source_cursor: write.source_cursor,
        created_at_unix_nanos: locator.created_at_unix_nanos,
    };
    Ok(StagedTypedFieldSegment {
        segment_ref: ref_name,
        segment_hash: segment_file_hash,
        locator,
        catalog,
    })
}

pub(crate) async fn publish_typed_field_segment_catalog(
    storage: &Storage,
    staged: &StagedTypedFieldSegment,
    additional_preconditions: &[CoreMutationPrecondition],
) -> Result<()> {
    write_writer_segment_catalog_record(storage, &staged.catalog, additional_preconditions).await
}

pub(crate) async fn publish_typed_field_segment_locator(
    storage: &Storage,
    staged: &StagedTypedFieldSegment,
    additional_preconditions: &[CoreMutationPrecondition],
) -> Result<()> {
    index_coremeta::write_index_segment_coremeta_record(
        storage,
        &staged.locator,
        additional_preconditions,
    )
    .await
}

pub async fn read_typed_field_segment(
    storage: &Storage,
    segment_ref: &str,
) -> Result<DecodedTypedFieldSegment> {
    let bytes = read_typed_field_segment_bytes(storage, segment_ref).await?;
    decode_typed_field_segment(&bytes)
}

pub async fn read_typed_field_segment_bytes(
    storage: &Storage,
    segment_ref: &str,
) -> Result<Vec<u8>> {
    let store = CoreStore::new(storage.clone()).await?;
    let index_id = typed_field_index_id_from_segment_ref(segment_ref)?;
    let segment =
        index_coremeta::read_index_segment_coremeta_record_by_ref(storage, &index_id, segment_ref)
            .await?
            .ok_or_else(|| anyhow!("typed field segment CoreMeta row is missing"))?;
    store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&segment.core_object_ref_target)?,
        })
        .await
}

pub async fn read_latest_typed_field_segment(
    storage: &Storage,
    index_id: &str,
) -> Result<Option<DecodedTypedFieldSegment>> {
    let Some(segment_ref) = latest_typed_field_segment_ref(storage, index_id).await? else {
        return Ok(None);
    };
    Ok(Some(read_typed_field_segment(storage, &segment_ref).await?))
}

pub async fn read_typed_field_segment_header(
    storage: &Storage,
    segment_ref: &str,
) -> Result<TypedFieldSegmentHeader> {
    let segment =
        RangeAddressedWriterSegment::open(storage, segment_ref, FileFamily::TypedFieldSegment)
            .await?;
    decode_typed_field_header_proto(&segment.header).map_err(anyhow::Error::from)
}

pub async fn read_typed_field_segment_rows_by_ordinals(
    storage: &Storage,
    segment_ref: &str,
    ordinals: impl IntoIterator<Item = usize>,
) -> Result<DecodedTypedFieldSegment> {
    let segment =
        RangeAddressedWriterSegment::open(storage, segment_ref, FileFamily::TypedFieldSegment)
            .await?;
    let header = decode_typed_field_header_proto(&segment.header)?;
    let rows = read_typed_field_rows_by_ordinals_from_segment(&segment, &header, ordinals).await?;
    let value_index = build_value_index_entries_from_rows(&rows)?;
    Ok(DecodedTypedFieldSegment {
        header,
        rows,
        value_index,
    })
}

pub async fn read_typed_field_rows_by_ordinals(
    storage: &Storage,
    segment_ref: &str,
    ordinals: impl IntoIterator<Item = usize>,
) -> Result<Vec<TypedFieldSegmentRow>> {
    let segment =
        RangeAddressedWriterSegment::open(storage, segment_ref, FileFamily::TypedFieldSegment)
            .await?;
    let header = decode_typed_field_header_proto(&segment.header)?;
    read_typed_field_rows_by_ordinals_from_segment(&segment, &header, ordinals).await
}

pub async fn read_typed_field_value_index_entries(
    storage: &Storage,
    segment_ref: &str,
    lookups: impl IntoIterator<Item = TypedFieldValueIndexLookup>,
) -> Result<Vec<TypedFieldValueIndexEntry>> {
    let segment =
        RangeAddressedWriterSegment::open(storage, segment_ref, FileFamily::TypedFieldSegment)
            .await?;
    let directory = segment.read_body_table_directory().await?;
    let value_index_table =
        RangeAddressedWriterSegment::table_entry(&directory, TABLE_TYPED_FIELD_VALUE_INDEX)?;
    let mut entries = Vec::new();
    let mut seen = BTreeSet::new();
    for lookup in lookups {
        let prefix = typed_value_index_key_prefix(
            lookup.field_name.as_str(),
            lookup.encoded_value.as_deref(),
        )?;
        for row in segment
            .read_table_pages_matching_key_prefix(value_index_table, &prefix)
            .await?
        {
            if !row.key.starts_with(&prefix) {
                continue;
            }
            let (field_name, encoded_value, source_identity) =
                decode_typed_value_index_key(&row.key)?;
            if field_name != lookup.field_name {
                continue;
            }
            if let Some(expected) = lookup.encoded_value.as_ref()
                && encoded_value != *expected
            {
                continue;
            }
            let row_ordinal = decode_typed_value_index_value(&row.value)?;
            if seen.insert((
                field_name.clone(),
                encoded_value.clone(),
                source_identity.clone(),
                row_ordinal,
            )) {
                entries.push(TypedFieldValueIndexEntry {
                    field_name,
                    encoded_value,
                    source_identity,
                    row_ordinal,
                });
            }
        }
    }
    entries.sort_by(|left, right| {
        left.field_name
            .cmp(&right.field_name)
            .then(left.encoded_value.cmp(&right.encoded_value))
            .then(left.source_identity.cmp(&right.source_identity))
            .then(left.row_ordinal.cmp(&right.row_ordinal))
    });
    Ok(entries)
}

async fn read_typed_field_rows_by_ordinals_from_segment(
    segment: &RangeAddressedWriterSegment,
    header: &TypedFieldSegmentHeader,
    ordinals: impl IntoIterator<Item = usize>,
) -> Result<Vec<TypedFieldSegmentRow>> {
    let directory = segment.read_body_table_directory().await?;
    let row_table =
        RangeAddressedWriterSegment::table_entry(&directory, TABLE_TYPED_ROW_BY_ORDINAL)?;
    let mut ordinals = ordinals.into_iter().collect::<BTreeSet<_>>();
    let mut rows = Vec::new();
    while let Some(ordinal) = ordinals.pop_first() {
        let key = typed_row_ordinal_key(ordinal);
        let row_pages = segment
            .read_table_pages_matching_key_prefix(row_table, &key)
            .await?;
        let Some(row) = row_pages.into_iter().find(|row| row.key == key) else {
            continue;
        };
        let mut cursor = ByteCursor::new(&row.value);
        let decoded = decode_typed_field_row(&header.field_names, &mut cursor)?;
        if !cursor.is_empty() {
            bail!("typed field ordinal row has trailing bytes");
        }
        rows.push(decoded);
    }
    Ok(rows)
}

pub async fn latest_typed_field_segment_ref(
    storage: &Storage,
    index_id: &str,
) -> Result<Option<String>> {
    Ok(
        index_coremeta::latest_index_segment_coremeta_record(storage, index_id)
            .await?
            .map(|record| record.segment_ref),
    )
}

pub fn decode_typed_field_segment(bytes: &[u8]) -> Result<DecodedTypedFieldSegment> {
    let segment = decode_writer_segment(bytes, FileFamily::TypedFieldSegment)?;
    let header = decode_typed_field_header_proto(&segment.header)?;
    if header.codec != "typed-row-binary-v1" {
        return Err(anyhow!(
            "unsupported typed field segment codec {}",
            header.codec
        ));
    }
    let rows = decode_typed_field_body(&header.field_names, &segment.body)
        .context("decode typed field rows")?;
    if rows.len() as u64 != header.row_count {
        return Err(anyhow!("typed field segment row count mismatch"));
    }
    let value_index = decode_typed_field_value_index(&segment.body, &rows)
        .context("decode typed field value index")?;
    Ok(DecodedTypedFieldSegment {
        header,
        rows,
        value_index,
    })
}

fn encode_typed_field_header_proto(
    logical_file_id: &str,
    header: &TypedFieldSegmentHeader,
) -> Vec<u8> {
    encode_writer_segment_header(
        "anvil.index.typed_field_segment_header.v1",
        logical_file_id,
        FileFamily::TypedFieldSegment,
        header.generation,
        None,
        None,
        unix_nanos_from_rfc3339(&header.created_at),
        vec![
            header_field_string("index_id", header.index_id.clone()),
            header_field_string("source_kind", header.source_kind.clone()),
            header_field_u64("source_cursor", header.source_cursor),
            header_field_u64("authz_revision", header.authz_revision),
            header_field_string("definition_hash", header.definition_hash.clone()),
            header_field_u64("row_count", header.row_count),
            header_field_strings("field_names", header.field_names.clone()),
            header_field_string("codec", header.codec.clone()),
            header_field_string("created_at", header.created_at.clone()),
        ],
    )
}

fn decode_typed_field_header_proto(
    header: &crate::formats::WriterSegmentHeaderProto,
) -> Result<TypedFieldSegmentHeader> {
    Ok(TypedFieldSegmentHeader {
        index_id: required_header_string(header, "index_id")?,
        generation: header.writer_generation,
        source_kind: required_header_string(header, "source_kind")?,
        source_cursor: required_header_u64(header, "source_cursor")?,
        authz_revision: required_header_u64(header, "authz_revision")?,
        definition_hash: required_header_string(header, "definition_hash")?,
        row_count: required_header_u64(header, "row_count")?,
        field_names: required_header_strings(header, "field_names")?,
        codec: required_header_string(header, "codec")?,
        created_at: required_header_string(header, "created_at")?,
    })
}

pub fn encode_row_values(
    values: &BTreeMap<String, JsonValue>,
) -> Result<BTreeMap<String, Vec<u8>>> {
    let mut encoded = BTreeMap::new();
    for (field, value) in values {
        let typed = json_value_to_typed_field_value(value)?;
        encoded.insert(
            field.clone(),
            EncodedTypedValue::for_ordered_value(&typed, false)?.bytes,
        );
    }
    Ok(encoded)
}

pub fn encode_json_value_for_typed_index(value: &JsonValue) -> Result<Vec<u8>> {
    let typed = json_value_to_typed_field_value(value)?;
    Ok(EncodedTypedValue::for_ordered_value(&typed, false)?.bytes)
}

pub fn source_id_binary(source_id: &SourceId) -> Result<Vec<u8>> {
    source_id.encode_binary()
}

fn encode_typed_field_body(
    field_names: &[String],
    rows: &[TypedFieldSegmentRow],
) -> Result<Vec<u8>> {
    let mut table_rows = Vec::with_capacity(rows.len());
    let mut value_index_rows = Vec::new();
    for (ordinal, row) in rows.iter().enumerate() {
        let mut value = Vec::new();
        encode_typed_field_row(&mut value, field_names, row)?;
        let key = if row.source_id_binary.is_empty() {
            row.source_identity.as_bytes().to_vec()
        } else {
            row.source_id_binary.clone()
        };
        for field_name in field_names {
            let encoded_value = row
                .encoded_values
                .get(field_name)
                .cloned()
                .unwrap_or_else(|| vec![0x01]);
            value_index_rows.push(TableRow {
                key: typed_value_index_key(field_name, &encoded_value, &row.source_identity)?,
                value: typed_value_index_value(ordinal)?,
            });
        }
        table_rows.push(TableRow { key, value });
    }
    table_rows.sort_by(|left, right| left.key.cmp(&right.key));
    value_index_rows.sort_by(|left, right| left.key.cmp(&right.key));
    let mut field_catalog_rows = field_names
        .iter()
        .map(|field| TableRow {
            key: field.as_bytes().to_vec(),
            value: field.as_bytes().to_vec(),
        })
        .collect::<Vec<_>>();
    field_catalog_rows.sort_by(|left, right| left.key.cmp(&right.key));
    let range_fence_rows = rows
        .first()
        .zip(rows.last())
        .map(|(first, last)| TableRow {
            key: b"body-range".to_vec(),
            value: format!(
                "{}\n{}",
                first.source_identity.as_str(),
                last.source_identity.as_str()
            )
            .into_bytes(),
        })
        .into_iter()
        .collect::<Vec<_>>();
    encode_writer_body_tables(&[
        WriterBodyTable {
            table_id: TABLE_TYPED_FIELD_CATALOG,
            row_type_id: TABLE_TYPED_FIELD_CATALOG,
            rows: field_catalog_rows,
        },
        WriterBodyTable {
            table_id: TABLE_TYPED_SORTED_COLUMN,
            row_type_id: TABLE_TYPED_SORTED_COLUMN,
            rows: table_rows,
        },
        WriterBodyTable {
            table_id: TABLE_TYPED_FIELD_VALUE_INDEX,
            row_type_id: TABLE_TYPED_FIELD_VALUE_INDEX,
            rows: value_index_rows,
        },
        WriterBodyTable {
            table_id: TABLE_TYPED_RANGE_FENCE,
            row_type_id: TABLE_TYPED_RANGE_FENCE,
            rows: range_fence_rows,
        },
        WriterBodyTable {
            table_id: TABLE_TYPED_ROW_BY_ORDINAL,
            row_type_id: TABLE_TYPED_ROW_BY_ORDINAL,
            rows: typed_rows_by_ordinal_rows(field_names, rows)?,
        },
    ])
    .map_err(anyhow::Error::from)
}

fn typed_rows_by_ordinal_rows(
    field_names: &[String],
    rows: &[TypedFieldSegmentRow],
) -> Result<Vec<TableRow>> {
    rows.iter()
        .enumerate()
        .map(|(ordinal, row)| {
            let mut value = Vec::new();
            encode_typed_field_row(&mut value, field_names, row)?;
            Ok(TableRow {
                key: typed_row_ordinal_key(ordinal),
                value,
            })
        })
        .collect()
}

fn typed_row_ordinal_key(ordinal: usize) -> Vec<u8> {
    (ordinal as u64).to_be_bytes().to_vec()
}

fn build_value_index_entries_from_rows(
    rows: &[TypedFieldSegmentRow],
) -> Result<Vec<TypedFieldValueIndexEntry>> {
    let mut entries = Vec::new();
    for (row_ordinal, row) in rows.iter().enumerate() {
        for (field_name, encoded_value) in &row.encoded_values {
            entries.push(TypedFieldValueIndexEntry {
                field_name: field_name.clone(),
                encoded_value: encoded_value.clone(),
                source_identity: row.source_identity.clone(),
                row_ordinal,
            });
        }
    }
    entries.sort_by(|left, right| {
        left.field_name
            .cmp(&right.field_name)
            .then(left.encoded_value.cmp(&right.encoded_value))
            .then(left.source_identity.cmp(&right.source_identity))
    });
    Ok(entries)
}

fn encode_typed_field_row(
    out: &mut Vec<u8>,
    field_names: &[String],
    row: &TypedFieldSegmentRow,
) -> Result<()> {
    let row_start = out.len();
    let key_count =
        u16::try_from(field_names.len()).map_err(|_| anyhow!("too many typed fields"))?;
    out.extend_from_slice(&key_count.to_le_bytes());
    for field in field_names {
        let encoded = row
            .encoded_values
            .get(field)
            .cloned()
            .unwrap_or_else(|| vec![0x01]);
        push_len_bytes(out, &encoded)?;
    }
    push_len_bytes(out, &row.source_id_binary)?;
    out.extend_from_slice(&row.value_flags.to_le_bytes());
    let stored = StoredFields {
        object_key: row.object_key.clone(),
        object_version_id: row.object_version_id.clone(),
        source_identity: row.source_identity.clone(),
        values: row.values.clone(),
        authz_label_hash: row.authz_label_hash.clone(),
        authz_revision: row.authz_revision,
    };
    let stored_fields = encode_stored_fields(&stored)?;
    push_len_bytes(out, &stored_fields)?;
    let row_hash = Sha256::digest(&out[row_start..]);
    out.extend_from_slice(&row_hash);
    Ok(())
}

fn decode_typed_field_body(
    field_names: &[String],
    input: &[u8],
) -> Result<Vec<TypedFieldSegmentRow>> {
    let tables = decode_writer_body_tables(input)?;
    let mut rows = Vec::new();
    for table in tables {
        if table.table_id != TABLE_TYPED_SORTED_COLUMN {
            continue;
        }
        rows.reserve(table.rows.len());
        for row in table.rows {
            let mut cursor = ByteCursor::new(&row.value);
            let decoded = decode_typed_field_row(field_names, &mut cursor)?;
            if !cursor.is_empty() {
                bail!("typed field row has trailing bytes");
            }
            rows.push(decoded);
        }
    }
    Ok(rows)
}

fn decode_typed_field_value_index(
    input: &[u8],
    rows: &[TypedFieldSegmentRow],
) -> Result<Vec<TypedFieldValueIndexEntry>> {
    let tables = decode_writer_body_tables(input)?;
    let mut entries = Vec::new();
    for table in tables {
        if table.table_id != TABLE_TYPED_FIELD_VALUE_INDEX {
            continue;
        }
        entries.reserve(table.rows.len());
        for row in table.rows {
            let (field_name, encoded_value, source_identity) =
                decode_typed_value_index_key(&row.key)?;
            let row_ordinal = decode_typed_value_index_value(&row.value)?;
            let Some(source_row) = rows.get(row_ordinal) else {
                bail!("typed field value index row ordinal out of range");
            };
            if source_row.source_identity != source_identity {
                bail!("typed field value index source identity mismatch");
            }
            entries.push(TypedFieldValueIndexEntry {
                field_name,
                encoded_value,
                source_identity,
                row_ordinal,
            });
        }
    }
    Ok(entries)
}

fn typed_value_index_key(
    field_name: &str,
    encoded_value: &[u8],
    source_identity: &str,
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    push_len_bytes(&mut out, field_name.as_bytes())?;
    push_len_bytes(&mut out, encoded_value)?;
    push_len_bytes(&mut out, source_identity.as_bytes())?;
    Ok(out)
}

fn typed_value_index_key_prefix(field_name: &str, encoded_value: Option<&[u8]>) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    push_len_bytes(&mut out, field_name.as_bytes())?;
    if let Some(encoded_value) = encoded_value {
        push_len_bytes(&mut out, encoded_value)?;
    }
    Ok(out)
}

fn decode_typed_value_index_key(bytes: &[u8]) -> Result<(String, Vec<u8>, String)> {
    let mut cursor = ByteCursor::new(bytes);
    let field_name = std::str::from_utf8(cursor.read_len_bytes()?)
        .context("typed field value index field name is not UTF-8")?
        .to_string();
    let encoded_value = cursor.read_len_bytes()?.to_vec();
    let source_identity = std::str::from_utf8(cursor.read_len_bytes()?)
        .context("typed field value index source identity is not UTF-8")?
        .to_string();
    if !cursor.is_empty() {
        bail!("typed field value index key has trailing bytes");
    }
    Ok((field_name, encoded_value, source_identity))
}

fn typed_value_index_value(row_ordinal: usize) -> Result<Vec<u8>> {
    let row_ordinal = u64::try_from(row_ordinal)
        .map_err(|_| anyhow!("typed field value index ordinal overflow"))?;
    Ok(row_ordinal.to_le_bytes().to_vec())
}

fn decode_typed_value_index_value(bytes: &[u8]) -> Result<usize> {
    if bytes.len() != 8 {
        bail!("typed field value index ordinal length mismatch");
    }
    let row_ordinal = u64::from_le_bytes(bytes.try_into().expect("ordinal is eight bytes"));
    usize::try_from(row_ordinal)
        .map_err(|_| anyhow!("typed field value index ordinal exceeds usize"))
}

fn decode_typed_field_row(
    field_names: &[String],
    cursor: &mut ByteCursor<'_>,
) -> Result<TypedFieldSegmentRow> {
    let row_start = cursor.position();
    let key_count = cursor.read_u16()? as usize;
    if key_count != field_names.len() {
        bail!("typed field row key count mismatch");
    }
    let mut encoded_values = BTreeMap::new();
    for field in field_names {
        encoded_values.insert(field.clone(), cursor.read_len_bytes()?.to_vec());
    }
    let source_id_binary = cursor.read_len_bytes()?.to_vec();
    let value_flags = cursor.read_u32()?;
    let stored_fields = cursor.read_len_bytes()?.to_vec();
    let row_hash_offset = cursor.position();
    let expected_hash = cursor.read_bytes(32)?;
    let actual_hash = Sha256::digest(&cursor.input[row_start..row_hash_offset]);
    if expected_hash != &actual_hash[..] {
        bail!("typed field row hash mismatch");
    }
    let stored = decode_stored_fields(&stored_fields)?;
    Ok(TypedFieldSegmentRow {
        object_key: stored.object_key,
        object_version_id: stored.object_version_id,
        source_identity: stored.source_identity,
        values: stored.values,
        encoded_values,
        source_id_binary,
        value_flags,
        authz_label_hash: stored.authz_label_hash,
        authz_revision: stored.authz_revision,
    })
}

fn encode_stored_fields(stored: &StoredFields) -> Result<Vec<u8>> {
    let proto = StoredFieldsProto {
        schema: "anvil.index.typed_field.stored_fields.v1".to_string(),
        object_key: stored.object_key.clone(),
        object_version_id: stored.object_version_id.clone(),
        source_identity: stored.source_identity.clone(),
        values: stored
            .values
            .iter()
            .map(|(name, value)| StoredJsonFieldProto {
                name: name.clone(),
                value: Some(json_value_to_proto(value)),
            })
            .collect(),
        authz_label_hash: stored.authz_label_hash.clone(),
        authz_revision: stored.authz_revision,
    };
    encode_deterministic_proto(&proto)
}

fn decode_stored_fields(bytes: &[u8]) -> Result<StoredFields> {
    let proto = StoredFieldsProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "typed field stored fields")?;
    if proto.schema != "anvil.index.typed_field.stored_fields.v1" {
        bail!("typed field stored fields schema mismatch");
    }
    ensure_stored_json_fields_sorted(&proto.values, "typed field stored values")?;
    Ok(StoredFields {
        object_key: proto.object_key,
        object_version_id: proto.object_version_id,
        source_identity: proto.source_identity,
        values: proto
            .values
            .into_iter()
            .map(|field| {
                let value = field
                    .value
                    .ok_or_else(|| anyhow!("typed field stored JSON value missing"))?;
                Ok((field.name, json_value_from_proto(value)?))
            })
            .collect::<Result<BTreeMap<_, _>>>()?,
        authz_label_hash: proto.authz_label_hash,
        authz_revision: proto.authz_revision,
    })
}

fn ensure_stored_json_fields_sorted(fields: &[StoredJsonFieldProto], label: &str) -> Result<()> {
    let mut previous: Option<&str> = None;
    for field in fields {
        if previous.is_some_and(|previous| previous >= field.name.as_str()) {
            bail!("{label} are not canonical sorted unique fields");
        }
        if let Some(value) = &field.value {
            ensure_stored_json_value_sorted(value, label)?;
        }
        previous = Some(field.name.as_str());
    }
    Ok(())
}

fn ensure_stored_json_value_sorted(value: &StoredJsonValueProto, label: &str) -> Result<()> {
    for child in &value.array_values {
        ensure_stored_json_value_sorted(child, label)?;
    }
    ensure_stored_json_fields_sorted(&value.object_fields, label)
}

fn json_value_to_proto(value: &JsonValue) -> StoredJsonValueProto {
    match value {
        JsonValue::Null => StoredJsonValueProto {
            kind: "null".to_string(),
            ..Default::default()
        },
        JsonValue::Bool(value) => StoredJsonValueProto {
            kind: "bool".to_string(),
            bool_value: *value,
            ..Default::default()
        },
        JsonValue::Number(value) => {
            if let Some(value) = value.as_i64() {
                StoredJsonValueProto {
                    kind: "i64".to_string(),
                    int64_value: value,
                    ..Default::default()
                }
            } else if let Some(value) = value.as_u64() {
                StoredJsonValueProto {
                    kind: "u64".to_string(),
                    uint64_value: value,
                    ..Default::default()
                }
            } else {
                StoredJsonValueProto {
                    kind: "f64".to_string(),
                    f64_value: value.as_f64().unwrap_or_default(),
                    ..Default::default()
                }
            }
        }
        JsonValue::String(value) => StoredJsonValueProto {
            kind: "string".to_string(),
            string_value: value.clone(),
            ..Default::default()
        },
        JsonValue::Array(values) => StoredJsonValueProto {
            kind: "array".to_string(),
            array_values: values.iter().map(json_value_to_proto).collect(),
            ..Default::default()
        },
        JsonValue::Object(values) => {
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort();
            StoredJsonValueProto {
                kind: "object".to_string(),
                object_fields: keys
                    .into_iter()
                    .map(|name| StoredJsonFieldProto {
                        name: name.clone(),
                        value: Some(json_value_to_proto(&values[name])),
                    })
                    .collect(),
                ..Default::default()
            }
        }
    }
}

fn json_value_from_proto(value: StoredJsonValueProto) -> Result<JsonValue> {
    match value.kind.as_str() {
        "null" => Ok(JsonValue::Null),
        "bool" => Ok(JsonValue::Bool(value.bool_value)),
        "i64" => Ok(JsonValue::Number(value.int64_value.into())),
        "u64" => Ok(JsonValue::Number(value.uint64_value.into())),
        "f64" => serde_json::Number::from_f64(value.f64_value)
            .map(JsonValue::Number)
            .ok_or_else(|| anyhow!("typed field stored JSON f64 is not finite")),
        "string" => Ok(JsonValue::String(value.string_value)),
        "array" => value
            .array_values
            .into_iter()
            .map(json_value_from_proto)
            .collect::<Result<Vec<_>>>()
            .map(JsonValue::Array),
        "object" => {
            let mut out = serde_json::Map::new();
            for field in value.object_fields {
                let field_value = field
                    .value
                    .ok_or_else(|| anyhow!("typed field stored object field value missing"))?;
                out.insert(field.name, json_value_from_proto(field_value)?);
            }
            Ok(JsonValue::Object(out))
        }
        _ => bail!("typed field stored JSON value kind is unsupported"),
    }
}

fn encode_deterministic_proto(message: &impl Message) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes)?;
    Ok(bytes)
}

fn ensure_deterministic_proto(message: &impl Message, bytes: &[u8], label: &str) -> Result<()> {
    if encode_deterministic_proto(message)? != bytes {
        bail!("{label} protobuf is not deterministic canonical encoding");
    }
    Ok(())
}

fn json_value_to_typed_field_value(value: &JsonValue) -> Result<TypedFieldValue> {
    match value {
        JsonValue::Null => Ok(TypedFieldValue::Null),
        JsonValue::Bool(value) => Ok(TypedFieldValue::Bool(*value)),
        JsonValue::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(TypedFieldValue::Int64(value))
            } else if let Some(value) = value.as_u64() {
                Ok(TypedFieldValue::Uint64(value))
            } else if let Some(value) = value.as_f64() {
                Ok(TypedFieldValue::Float64(value))
            } else {
                bail!("unsupported JSON number for typed field encoding")
            }
        }
        JsonValue::String(value) => Ok(TypedFieldValue::String(value.clone())),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            Ok(TypedFieldValue::String(canonical_json_string(value)?))
        }
    }
}

fn canonical_json_string(value: &JsonValue) -> Result<String> {
    serde_json::to_string(&canonical_json_value(value))
        .context("encode canonical typed field JSON value")
}

fn canonical_json_value(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Array(values) => {
            JsonValue::Array(values.iter().map(canonical_json_value).collect())
        }
        JsonValue::Object(values) => {
            let mut sorted = serde_json::Map::new();
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                sorted.insert(key.clone(), canonical_json_value(&values[key]));
            }
            JsonValue::Object(sorted)
        }
        other => other.clone(),
    }
}

struct ByteCursor<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> ByteCursor<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn position(&self) -> usize {
        self.offset
    }

    fn is_empty(&self) -> bool {
        self.offset == self.input.len()
    }

    fn read_u16(&mut self) -> Result<u16> {
        let bytes = self.read_bytes(2)?;
        Ok(u16::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn read_u64(&mut self) -> Result<u64> {
        let bytes = self.read_bytes(8)?;
        Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn read_len_bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.read_u32()? as usize;
        self.read_bytes(len)
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| anyhow!("typed field segment offset overflow"))?;
        if end > self.input.len() {
            bail!("typed field segment truncated");
        }
        let bytes = &self.input[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_len_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let len = u32::try_from(value.len()).map_err(|_| anyhow!("typed field value too large"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value);
    Ok(())
}

fn typed_field_segment_ref_name(
    index_id: &str,
    generation: u64,
    segment_hash: &str,
) -> Result<String> {
    validate_hex32(segment_hash, "typed field segment hash")?;
    Ok(format!(
        "{}{}:{}:{}",
        TYPED_FIELD_SEGMENT_REF_PREFIX,
        URL_SAFE_NO_PAD.encode(index_id.as_bytes()),
        generation,
        segment_hash
    ))
}

fn typed_field_segment_ref_prefix(index_id: &str) -> Result<String> {
    Ok(format!(
        "{}{}:",
        TYPED_FIELD_SEGMENT_REF_PREFIX,
        URL_SAFE_NO_PAD.encode(index_id.as_bytes())
    ))
}

fn typed_field_index_id_from_segment_ref(segment_ref: &str) -> Result<String> {
    let rest = segment_ref
        .strip_prefix(TYPED_FIELD_SEGMENT_REF_PREFIX)
        .ok_or_else(|| anyhow!("typed field segment ref has invalid prefix"))?;
    let encoded_index = rest
        .split(':')
        .next()
        .ok_or_else(|| anyhow!("typed field segment ref is missing index component"))?;
    let bytes = URL_SAFE_NO_PAD.decode(encoded_index)?;
    let index_id = String::from_utf8(bytes)
        .map_err(|_| anyhow!("typed field segment ref index id is not UTF-8"))?;
    Ok(index_id)
}

fn generation_from_ref(value: &str) -> Option<u64> {
    value.rsplit(':').nth(1)?.parse().ok()
}

fn source_identity_hash_bounds(rows: &[TypedFieldSegmentRow]) -> (Hash32, Hash32) {
    let first = rows
        .first()
        .map(|row| hash32(row.source_identity.as_bytes()))
        .unwrap_or([0u8; 32]);
    let last = rows
        .last()
        .map(|row| hash32(row.source_identity.as_bytes()))
        .unwrap_or([0u8; 32]);
    (first, last)
}

fn encode_core_object_ref_target(object_ref: &CoreObjectRef) -> Result<String> {
    crate::core_store::encode_core_object_ref_target(object_ref)
}

fn decode_core_object_ref_target(value: &str) -> Result<CoreObjectRef> {
    crate::core_store::decode_core_object_ref_target(value)
}

fn validate_hex32(value: &str, label: &str) -> Result<()> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("{label} must be 32 hex bytes");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn typed_field_segment_round_trips_through_core_store() {
        let dir = tempdir().unwrap();
        let storage = Storage::new_at(dir.path()).await.unwrap();
        let mut values = BTreeMap::new();
        values.insert(
            "status".to_string(),
            JsonValue::String("pending".to_string()),
        );
        values.insert("priority".to_string(), JsonValue::Number(10.into()));
        let row = TypedFieldSegmentRow {
            object_key: "queue/item-1.json".to_string(),
            object_version_id: uuid::Uuid::new_v4().to_string(),
            source_identity: "queue/item-1.json#1".to_string(),
            encoded_values: encode_row_values(&values).unwrap(),
            source_id_binary: b"source-id".to_vec(),
            value_flags: 0,
            values,
            authz_label_hash: hex::encode([7u8; 32]),
            authz_revision: 9,
        };
        let definition_hash = blake3::hash(b"definition").to_hex().to_string();
        let segment_ref = write_typed_field_segment(
            &storage,
            TypedFieldSegmentWrite {
                index_id: "tenant:bucket:index",
                generation: 1,
                source_kind: "object_current",
                source_cursor: 12,
                authz_revision: 9,
                boundary_values: &[],
                definition_hash: &definition_hash,
                field_names: &["status".to_string(), "priority".to_string()],
                rows: &[row.clone()],
            },
        )
        .await
        .unwrap();

        let decoded = read_typed_field_segment(&storage, &segment_ref)
            .await
            .unwrap();
        assert_eq!(decoded.header.index_id, "tenant:bucket:index");
        assert_eq!(decoded.header.source_cursor, 12);
        assert_eq!(decoded.header.codec, "typed-row-binary-v1");
        assert_eq!(decoded.rows, vec![row]);
        assert_eq!(decoded.value_index.len(), 2);
        assert!(decoded.value_index.iter().any(|entry| {
            entry.field_name == "status"
                && entry.encoded_value
                    == encode_json_value_for_typed_index(&JsonValue::String("pending".to_string()))
                        .unwrap()
                && entry.row_ordinal == 0
        }));
        assert!(decoded.value_index.iter().any(|entry| {
            entry.field_name == "priority"
                && entry.encoded_value
                    == encode_json_value_for_typed_index(&JsonValue::Number(10.into())).unwrap()
                && entry.row_ordinal == 0
        }));
        assert_eq!(
            latest_typed_field_segment_ref(&storage, "tenant:bucket:index")
                .await
                .unwrap(),
            Some(segment_ref)
        );
    }
}
