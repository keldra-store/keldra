use crate::{
    core_store::{CoreObjectRef, CorePipelinePolicy, CoreStore, CoreTraceContext, GetBlob},
    formats::{
        FileFamily, Hash32, decode_writer_segment, encode_writer_segment_header,
        git::{GitHashAlgorithm, GitSourceRecord},
        hash32, header_field_string, required_header_string, single_body_range_index,
        table::{TableRow, WriterBodyTable, decode_writer_body_tables, encode_writer_body_tables},
        unix_nanos_from_rfc3339,
        writer::{
            WriterBuildOutput, WriterFamily, WriterSegmentBuildInput,
            build_writer_segment_logical_file, canonical_logical_file_id,
        },
    },
    storage::Storage,
    writer_segment_catalog::{
        WriterSegmentCatalogRecord, latest_writer_segment_catalog_record,
        read_writer_segment_catalog_record, write_writer_segment_catalog_record,
    },
};
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

const GIT_SOURCE_INDEX_REF_PREFIX: &str = "git_source_index:";
const GIT_SOURCE_INDEX_CATALOG_FAMILY: &str = "git_source_index";
const TABLE_GIT_SOURCE_RECORD: u16 = 0x0901;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GitSourceIndexHeader {
    pub tenant_id: String,
    pub repository_id: String,
    pub generation: u64,
    pub source_hash: String,
    pub hash_algorithm: String,
    pub key_order: String,
    pub codec: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedGitSourceIndex {
    pub header: GitSourceIndexHeader,
    pub records: Vec<GitSourceRecord>,
}

#[derive(Debug, Clone)]
pub struct GitSourceIndexWrite<'a> {
    pub tenant_id: i64,
    pub repository_id: &'a str,
    pub generation: u64,
    pub source_hash: Hash32,
    pub hash_algorithm: GitHashAlgorithm,
    pub records: &'a [GitSourceRecord],
}

pub async fn write_git_source_index(
    storage: &Storage,
    input: GitSourceIndexWrite<'_>,
) -> Result<String> {
    let mut records = input.records.to_vec();
    ensure_record_algorithms(input.hash_algorithm, &records)?;
    records.sort_by(compare_git_source_records);
    let body = encode_git_source_body(&records)?;
    let source_hash = hex::encode(input.source_hash);
    let segment_hash = hash32(&body);
    let ref_name = git_source_index_ref_name(
        input.tenant_id,
        input.repository_id,
        input.generation,
        &source_hash,
    )?;
    let logical_file_id = canonical_logical_file_id(
        WriterFamily::GitSource,
        input.generation,
        &ref_name,
        &segment_hash,
    );

    let header = GitSourceIndexHeader {
        tenant_id: input.tenant_id.to_string(),
        repository_id: input.repository_id.to_string(),
        generation: input.generation,
        source_hash,
        hash_algorithm: hash_algorithm_name(input.hash_algorithm).to_string(),
        key_order: "repository_commit_tree_path_object".to_string(),
        codec: "writer-body-table-v1".to_string(),
        created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    };
    let (first_hash, last_hash) = record_hash_bounds(&records);
    let header_proto = encode_git_source_header_proto(&logical_file_id, &header);
    let range_index =
        single_body_range_index(body.len(), records.len() as u64, first_hash, last_hash)?;
    let built_segment = build_writer_segment_logical_file(WriterSegmentBuildInput {
        file_family: FileFamily::GitSourceIndex,
        writer_family: WriterFamily::GitSource,
        writer_generation: input.generation,
        logical_file_id,
        header_proto,
        body,
        range_index,
        record_count: records.len() as u64,
        first_record_hash: first_hash,
        last_record_hash: last_hash,
        boundary_values: Vec::new(),
        mutation_id: format!(
            "git-source-index:{}:{}:{}",
            input.tenant_id, input.repository_id, input.generation
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
        .ok_or_else(|| anyhow!("CoreFormatWriter returned no git source object"))?;
    write_writer_segment_catalog_record(
        storage,
        &WriterSegmentCatalogRecord {
            family: GIT_SOURCE_INDEX_CATALOG_FAMILY.to_string(),
            scope: git_source_index_scope(input.tenant_id, input.repository_id)?,
            segment_ref: ref_name.clone(),
            core_object_ref_target: encode_core_object_ref_target(&object_ref)?,
            segment_hash: hex::encode(segment_hash),
            segment_length: object_ref.logical_size,
            generation: input.generation,
            source_cursor: input.generation,
            created_at_unix_nanos: unix_nanos_from_rfc3339(&header.created_at),
        },
        &[],
    )
    .await?;
    Ok(ref_name)
}

pub async fn read_git_source_index(
    storage: &Storage,
    index_ref: &str,
) -> Result<DecodedGitSourceIndex> {
    let bytes = read_git_source_index_bytes(storage, index_ref).await?;
    decode_git_source_index(&bytes)
}

pub async fn read_git_source_index_bytes(storage: &Storage, index_ref: &str) -> Result<Vec<u8>> {
    let parsed = parse_git_source_index_ref(index_ref)?;
    let record = read_writer_segment_catalog_record(
        storage,
        GIT_SOURCE_INDEX_CATALOG_FAMILY,
        &git_source_index_scope(parsed.tenant_id, &parsed.repository_id)?,
        parsed.generation,
        index_ref,
    )
    .await?
    .ok_or_else(|| anyhow!("git source index catalog row is missing"))?;
    let store = CoreStore::new(storage.clone()).await?;
    store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&record.core_object_ref_target)?,
        })
        .await
}

pub async fn latest_git_source_index_ref(
    storage: &Storage,
    tenant_id: i64,
    repository_id: &str,
) -> Result<Option<String>> {
    Ok(latest_writer_segment_catalog_record(
        storage,
        GIT_SOURCE_INDEX_CATALOG_FAMILY,
        &git_source_index_scope(tenant_id, repository_id)?,
    )
    .await?
    .map(|record| record.segment_ref))
}

pub fn decode_git_source_index(bytes: &[u8]) -> Result<DecodedGitSourceIndex> {
    let segment = decode_writer_segment(bytes, FileFamily::GitSourceIndex)?;
    let header = decode_git_source_header_proto(&segment.header)?;
    let hash_algorithm = parse_hash_algorithm(&header.hash_algorithm)?;
    let records = decode_git_source_body(segment.body, hash_algorithm)?;
    ensure_sorted(&records)?;
    Ok(DecodedGitSourceIndex { header, records })
}

fn encode_git_source_header_proto(logical_file_id: &str, header: &GitSourceIndexHeader) -> Vec<u8> {
    encode_writer_segment_header(
        "anvil.git.source_index_header.v1",
        logical_file_id,
        FileFamily::GitSourceIndex,
        header.generation,
        None,
        None,
        unix_nanos_from_rfc3339(&header.created_at),
        vec![
            header_field_string("tenant_id", header.tenant_id.clone()),
            header_field_string("repository_id", header.repository_id.clone()),
            header_field_string("source_hash", header.source_hash.clone()),
            header_field_string("hash_algorithm", header.hash_algorithm.clone()),
            header_field_string("key_order", header.key_order.clone()),
            header_field_string("codec", header.codec.clone()),
            header_field_string("created_at", header.created_at.clone()),
        ],
    )
}

fn decode_git_source_header_proto(
    header: &crate::formats::WriterSegmentHeaderProto,
) -> Result<GitSourceIndexHeader> {
    Ok(GitSourceIndexHeader {
        tenant_id: required_header_string(header, "tenant_id")?,
        repository_id: required_header_string(header, "repository_id")?,
        generation: header.writer_generation,
        source_hash: required_header_string(header, "source_hash")?,
        hash_algorithm: required_header_string(header, "hash_algorithm")?,
        key_order: required_header_string(header, "key_order")?,
        codec: required_header_string(header, "codec")?,
        created_at: required_header_string(header, "created_at")?,
    })
}

fn encode_git_source_body(records: &[GitSourceRecord]) -> Result<Vec<u8>> {
    let rows = records
        .iter()
        .map(|record| TableRow {
            key: git_source_record_key(record),
            value: record.encode(),
        })
        .collect::<Vec<_>>();
    encode_writer_body_tables(&[WriterBodyTable {
        table_id: TABLE_GIT_SOURCE_RECORD,
        row_type_id: TABLE_GIT_SOURCE_RECORD,
        rows,
    }])
    .map_err(anyhow::Error::from)
}

fn decode_git_source_body(
    input: &[u8],
    hash_algorithm: GitHashAlgorithm,
) -> Result<Vec<GitSourceRecord>> {
    let mut records = Vec::new();
    for table in decode_writer_body_tables(input)? {
        if table.table_id != TABLE_GIT_SOURCE_RECORD {
            return Err(anyhow!(
                "git source index contains unexpected table {:04x}",
                table.table_id
            ));
        }
        for row in table.rows {
            let (record, used) = GitSourceRecord::decode(&row.value, hash_algorithm)?;
            if used != row.value.len() {
                return Err(anyhow!("git source index row has trailing bytes"));
            }
            if row.key != git_source_record_key(&record) {
                return Err(anyhow!(
                    "git source index row key does not match encoded record"
                ));
            }
            records.push(record);
        }
    }
    Ok(records)
}

fn git_source_record_key(record: &GitSourceRecord) -> Vec<u8> {
    let mut key = Vec::new();
    key.extend_from_slice(&record.repository_id);
    key.push(0);
    key.extend_from_slice(&record.commit_id);
    key.push(0);
    key.extend_from_slice(&hash32(&record.tree_path));
    key.extend_from_slice(&record.object_id);
    key
}

fn ensure_record_algorithms(
    hash_algorithm: GitHashAlgorithm,
    records: &[GitSourceRecord],
) -> Result<()> {
    let len = hash_algorithm.object_id_len();
    if records
        .iter()
        .all(|record| record.commit_id.len() == len && record.object_id.len() == len)
    {
        Ok(())
    } else {
        Err(anyhow!(
            "git source records do not match declared hash algorithm"
        ))
    }
}

fn ensure_sorted(records: &[GitSourceRecord]) -> Result<()> {
    if records
        .windows(2)
        .all(|pair| compare_git_source_records(&pair[0], &pair[1]).is_le())
    {
        Ok(())
    } else {
        Err(anyhow!("git source records are not sorted"))
    }
}

fn compare_git_source_records(
    left: &GitSourceRecord,
    right: &GitSourceRecord,
) -> std::cmp::Ordering {
    left.repository_id
        .cmp(&right.repository_id)
        .then_with(|| left.commit_id.cmp(&right.commit_id))
        .then_with(|| hash32(&left.tree_path).cmp(&hash32(&right.tree_path)))
        .then_with(|| left.object_id.cmp(&right.object_id))
}

fn hash_algorithm_name(hash_algorithm: GitHashAlgorithm) -> &'static str {
    match hash_algorithm {
        GitHashAlgorithm::Sha1 => "sha1",
        GitHashAlgorithm::Sha256 => "sha256",
    }
}

fn parse_hash_algorithm(value: &str) -> Result<GitHashAlgorithm> {
    match value {
        "sha1" => Ok(GitHashAlgorithm::Sha1),
        "sha256" => Ok(GitHashAlgorithm::Sha256),
        other => Err(anyhow!("unsupported git hash algorithm {other}")),
    }
}

fn record_hash_bounds(records: &[GitSourceRecord]) -> (Hash32, Hash32) {
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

fn git_source_index_ref_prefix(tenant_id: i64, repository_id: &str) -> Result<String> {
    require_safe_component(repository_id, "git repository id")?;
    Ok(format!(
        "{GIT_SOURCE_INDEX_REF_PREFIX}tenant:{tenant_id}:repository:{repository_id}:"
    ))
}

#[derive(Debug, Clone)]
struct ParsedGitSourceIndexRef {
    tenant_id: i64,
    repository_id: String,
    generation: u64,
}

fn parse_git_source_index_ref(index_ref: &str) -> Result<ParsedGitSourceIndexRef> {
    let parts = index_ref.split(':').collect::<Vec<_>>();
    if parts.len() != 9
        || parts[0] != "git_source_index"
        || parts[1] != "tenant"
        || parts[3] != "repository"
        || parts[5] != "generation"
        || parts[7] != "source"
    {
        return Err(anyhow!("git source index ref is malformed"));
    }
    let tenant_id = parts[2]
        .parse::<i64>()
        .map_err(|_| anyhow!("git source index ref tenant id is invalid"))?;
    let generation = parts[6]
        .parse::<u64>()
        .map_err(|_| anyhow!("git source index ref generation is invalid"))?;
    if tenant_id <= 0 {
        return Err(anyhow!("git source index ref tenant id must be positive"));
    }
    if generation == 0 {
        return Err(anyhow!("git source index ref generation must be positive"));
    }
    require_safe_component(parts[4], "git repository id")?;
    validate_hex32(parts[8], "git source hash")?;
    Ok(ParsedGitSourceIndexRef {
        tenant_id,
        repository_id: parts[4].to_string(),
        generation,
    })
}

fn git_source_index_scope(tenant_id: i64, repository_id: &str) -> Result<String> {
    if tenant_id <= 0 {
        return Err(anyhow!("git source index tenant id must be positive"));
    }
    require_safe_component(repository_id, "git repository id")?;
    Ok(format!("tenant/{tenant_id}/repository/{repository_id}"))
}

fn git_source_index_ref_name(
    tenant_id: i64,
    repository_id: &str,
    generation: u64,
    source_hash: &str,
) -> Result<String> {
    validate_hex32(source_hash, "git source hash")?;
    Ok(format!(
        "{}generation:{generation:020}:source:{source_hash}",
        git_source_index_ref_prefix(tenant_id, repository_id)?
    ))
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(())
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

    fn record(commit: u8, path: &str, object: u8) -> GitSourceRecord {
        GitSourceRecord::new(
            GitHashAlgorithm::Sha1,
            b"repo-alpha".to_vec(),
            vec![commit; 20],
            vec![object; 20],
            path.as_bytes().to_vec(),
            u64::from(object) * 100,
            44,
            [9; 16],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn git_source_index_round_trips_sorted_records() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let records = vec![
            record(2, "src/lib.rs", 2),
            record(1, "README.md", 9),
            record(1, "src/main.rs", 1),
        ];

        let index_ref = write_git_source_index(
            &storage,
            GitSourceIndexWrite {
                tenant_id: 5,
                repository_id: "repo-alpha",
                generation: 3,
                source_hash: [8; 32],
                hash_algorithm: GitHashAlgorithm::Sha1,
                records: &records,
            },
        )
        .await
        .unwrap();
        assert!(index_ref.starts_with("git_source_index:tenant:5:repository:repo-alpha:"));

        let decoded = read_git_source_index(&storage, &index_ref).await.unwrap();
        assert_eq!(decoded.header.repository_id, "repo-alpha");
        assert_eq!(decoded.header.hash_algorithm, "sha1");
        assert_eq!(decoded.records.len(), 3);
        ensure_sorted(&decoded.records).unwrap();
    }

    #[tokio::test]
    async fn git_source_index_footer_protects_body() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let index_ref = write_git_source_index(
            &storage,
            GitSourceIndexWrite {
                tenant_id: 5,
                repository_id: "repo-alpha",
                generation: 3,
                source_hash: [8; 32],
                hash_algorithm: GitHashAlgorithm::Sha1,
                records: &[record(1, "README.md", 1)],
            },
        )
        .await
        .unwrap();
        let mut bytes = read_git_source_index_bytes(&storage, &index_ref)
            .await
            .unwrap();
        bytes[crate::formats::WRITER_SEGMENT_FIXED_HEADER_LEN + 1] ^= 1;
        assert!(decode_git_source_index(&bytes).is_err());
    }

    #[test]
    fn git_source_index_rejects_wrong_record_algorithm() {
        let record = GitSourceRecord::new(
            GitHashAlgorithm::Sha1,
            b"repo-alpha".to_vec(),
            vec![1; 20],
            vec![2; 20],
            b"README.md".to_vec(),
            0,
            12,
            [4; 16],
        )
        .unwrap();
        assert!(ensure_record_algorithms(GitHashAlgorithm::Sha256, &[record]).is_err());
    }
}
