use crate::{
    formats::git::GitSourceRecord,
    git_source_index::{DecodedGitSourceIndex, latest_git_source_index_ref, read_git_source_index},
    storage::Storage,
};
use anyhow::{Result, anyhow};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitObjectLookup {
    pub repository_id: String,
    pub commit_id: Vec<u8>,
    pub object_id: Vec<u8>,
    pub tree_path: String,
    pub blob_start: u64,
    pub blob_len: u64,
    pub pack_object_version_id: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitTreeEntry {
    pub tree_path: String,
    pub object_id: Vec<u8>,
    pub blob_start: u64,
    pub blob_len: u64,
    pub pack_object_version_id: [u8; 16],
}

pub async fn read_latest_git_source_index(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    repository_id: &str,
) -> Result<Option<DecodedGitSourceIndex>> {
    let Some(index_ref) = latest_git_source_index_ref(mvcc, tenant_id, repository_id).await? else {
        return Ok(None);
    };
    Ok(Some(
        read_git_source_index(storage, mvcc, &index_ref).await?,
    ))
}

pub fn get_git_object(
    index: &DecodedGitSourceIndex,
    object_id: &[u8],
) -> Result<Vec<GitObjectLookup>> {
    let mut matches = index
        .records
        .iter()
        .filter(|record| record.object_id == object_id)
        .map(record_to_lookup)
        .collect::<Result<Vec<_>>>()?;
    matches.sort_by(|left, right| {
        left.commit_id
            .cmp(&right.commit_id)
            .then_with(|| left.tree_path.cmp(&right.tree_path))
    });
    Ok(matches)
}

pub fn get_git_blob_by_path(
    index: &DecodedGitSourceIndex,
    commit_id: &[u8],
    tree_path: &str,
) -> Result<Option<GitObjectLookup>> {
    let normalized = normalize_tree_path(tree_path)?;
    index
        .records
        .iter()
        .find(|record| record.commit_id == commit_id && record.tree_path == normalized.as_bytes())
        .map(record_to_lookup)
        .transpose()
}

pub fn list_git_tree(
    index: &DecodedGitSourceIndex,
    commit_id: &[u8],
    prefix: &str,
) -> Result<Vec<GitTreeEntry>> {
    let normalized_prefix = normalize_prefix(prefix)?;
    let mut entries = index
        .records
        .iter()
        .filter(|record| {
            record.commit_id == commit_id
                && std::str::from_utf8(&record.tree_path)
                    .is_ok_and(|path| path.starts_with(&normalized_prefix))
        })
        .map(record_to_tree_entry)
        .collect::<Result<Vec<_>>>()?;
    entries.sort_by(|left, right| left.tree_path.cmp(&right.tree_path));
    Ok(entries)
}

fn record_to_lookup(record: &GitSourceRecord) -> Result<GitObjectLookup> {
    Ok(GitObjectLookup {
        repository_id: String::from_utf8(record.repository_id.clone())?,
        commit_id: record.commit_id.clone(),
        object_id: record.object_id.clone(),
        tree_path: tree_path_string(record)?,
        blob_start: record.blob_start,
        blob_len: record.blob_len,
        pack_object_version_id: record.pack_object_version_id,
    })
}

fn record_to_tree_entry(record: &GitSourceRecord) -> Result<GitTreeEntry> {
    Ok(GitTreeEntry {
        tree_path: tree_path_string(record)?,
        object_id: record.object_id.clone(),
        blob_start: record.blob_start,
        blob_len: record.blob_len,
        pack_object_version_id: record.pack_object_version_id,
    })
}

fn tree_path_string(record: &GitSourceRecord) -> Result<String> {
    Ok(std::str::from_utf8(&record.tree_path)?.to_string())
}

fn normalize_tree_path(path: &str) -> Result<String> {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() || trimmed.contains("..") || trimmed.contains('\\') {
        return Err(anyhow!("invalid git tree path"));
    }
    Ok(trimmed.to_string())
}

fn normalize_prefix(prefix: &str) -> Result<String> {
    if prefix.is_empty() || prefix == "/" {
        return Ok(String::new());
    }
    let normalized = normalize_tree_path(prefix)?;
    Ok(if normalized.ends_with('/') {
        normalized
    } else {
        format!("{normalized}/")
    })
}
