//! Bounded pure-CPU filtering and top-k work for projection queries.

use std::collections::BTreeMap;

use crate::query_bounds::replace_retained_bytes;
use crate::{DocumentRef, IndexError};

use super::{
    GitPayload, GitSourceRecord, ProjectionQueryCandidate, RetainedGitRecord, TensorPayload,
    TensorRecord, git_object_primary, git_path_primary, newer_projection_document, take_git_record,
    tensor_primary,
};

pub(super) fn merge_git_path_candidates(
    mut selected: Option<(DocumentRef, GitSourceRecord)>,
    candidates: Vec<ProjectionQueryCandidate<GitPayload>>,
) -> Result<Option<(DocumentRef, GitSourceRecord)>, IndexError> {
    for candidate in candidates {
        let record = take_git_record(candidate.payload, candidate.key.position)?;
        if git_path_primary(&record)? != candidate.key.primary {
            return Err(IndexError::InvalidFormat("Git path key mismatch"));
        }
        if selected
            .as_ref()
            .is_none_or(|(current, _)| newer_projection_document(&candidate.document, current))
        {
            selected = Some((candidate.document, record));
        }
    }
    Ok(selected)
}

pub(super) fn merge_git_tree_candidates(
    mut selected: BTreeMap<String, RetainedGitRecord>,
    mut retained_bytes: usize,
    candidates: Vec<ProjectionQueryCandidate<GitPayload>>,
    after_path: Option<&str>,
    limit: usize,
) -> Result<(BTreeMap<String, RetainedGitRecord>, usize), IndexError> {
    for candidate in candidates {
        let record = take_git_record(candidate.payload, candidate.key.position)?;
        if git_path_primary(&record)? != candidate.key.primary {
            return Err(IndexError::InvalidFormat("Git path key mismatch"));
        }
        if after_path.is_some_and(|after| record.tree_path.as_str() <= after) {
            continue;
        }
        let path = record.tree_path.clone();
        if selected
            .get(&path)
            .is_none_or(|current| newer_projection_document(&candidate.document, &current.document))
        {
            let value = RetainedGitRecord::new(candidate.document, record, path.len());
            let added = value.resident_bytes;
            let replaced = selected.insert(path, value);
            let mut removed = replaced.as_ref().map_or(0, |value| value.resident_bytes);
            removed = removed.saturating_add(
                (selected.len() > limit)
                    .then(|| {
                        selected
                            .pop_last()
                            .map_or(0, |(_, value)| value.resident_bytes)
                    })
                    .unwrap_or(0),
            );
            retained_bytes = replace_retained_bytes(retained_bytes, added, removed)?;
        }
    }
    Ok((selected, retained_bytes))
}

pub(super) fn merge_git_object_candidates(
    mut selected: BTreeMap<(String, String), RetainedGitRecord>,
    mut retained_bytes: usize,
    candidates: Vec<ProjectionQueryCandidate<GitPayload>>,
    limit: usize,
) -> Result<(BTreeMap<(String, String), RetainedGitRecord>, usize), IndexError> {
    for candidate in candidates {
        let record = take_git_record(candidate.payload, candidate.key.position)?;
        if git_object_primary(&record)? != candidate.key.primary {
            return Err(IndexError::InvalidFormat("Git object key mismatch"));
        }
        let key = (record.commit_id.clone(), record.tree_path.clone());
        if selected
            .get(&key)
            .is_none_or(|current| newer_projection_document(&candidate.document, &current.document))
        {
            let key_bytes = key.0.len().saturating_add(key.1.len());
            let value = RetainedGitRecord::new(candidate.document, record, key_bytes);
            let added = value.resident_bytes;
            let replaced = selected.insert(key, value);
            let mut removed = replaced.map_or(0, |value| value.resident_bytes);
            removed = removed.saturating_add(
                (selected.len() > limit)
                    .then(|| {
                        selected
                            .pop_last()
                            .map_or(0, |(_, value)| value.resident_bytes)
                    })
                    .unwrap_or(0),
            );
            retained_bytes = replace_retained_bytes(retained_bytes, added, removed)?;
        }
    }
    Ok((selected, retained_bytes))
}

pub(super) fn merge_tensor_candidates(
    mut selected: Option<(DocumentRef, TensorRecord)>,
    candidates: Vec<ProjectionQueryCandidate<TensorPayload>>,
) -> Result<Option<(DocumentRef, TensorRecord)>, IndexError> {
    for candidate in candidates {
        let record = candidate
            .payload
            .payload
            .0
            .get(candidate.key.position as usize)
            .cloned()
            .ok_or(IndexError::InvalidFormat("tensor key record slot"))?;
        if tensor_primary(&record)? != candidate.key.primary {
            return Err(IndexError::InvalidFormat("tensor key mismatch"));
        }
        if selected
            .as_ref()
            .is_none_or(|(current, _)| newer_projection_document(&candidate.document, current))
        {
            selected = Some((candidate.document, record));
        }
    }
    Ok(selected)
}
