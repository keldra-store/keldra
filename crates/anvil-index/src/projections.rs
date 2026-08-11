//! Bounded Git-source and tensor projection runs.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::codec::{Decoder, Encoder, encode_component};
use crate::query_bounds::replace_retained_bytes;
use crate::routed::{
    ROUTED_ROW_RESIDENT_OVERHEAD_BYTES, RoutedComponentWriter, RoutedCursor, RoutedRow,
};
use crate::run::{ComponentTree, RunStatistics, RunView, find_leaf, open_run, seal_run_root};
use crate::segment::{
    DEFAULT_COMPONENT_BLOCK_BYTES, DocumentComponentWriter, DocumentRecord, DocumentState,
    MutationBuffer, PATH_CHANGES_TAG, PathChange, PathComponentWriter, PathRunCursor,
    document_by_ordinal, is_latest_live, path_change_in_tree,
};
use crate::succinct::{decode_elias_fano_with_budget, encode_elias_fano};
use crate::{
    ComponentCodec, DocumentRef, IndexBlockSink, IndexDirectoryRead, IndexError, IndexKind,
    IndexMutation, SealedRun, SegmentBuildOptions, SegmentPush,
};

pub(crate) const RECORDS_TAG: u8 = 60;
const PRIMARY_KEY_TAG: u8 = 61;
const SECONDARY_KEY_TAG: u8 = 62;

#[path = "projections/compaction_cache.rs"]
mod compaction_cache;

#[path = "projections/parallel_compaction.rs"]
mod parallel_compaction;

macro_rules! builder_state {
    () => {
        pub fn resident_bytes(&self) -> usize {
            self.buffer.resident_bytes()
        }

        pub fn seal_workspace_bytes(&self) -> Result<usize, IndexError> {
            self.buffer.seal_workspace_bytes()
        }

        pub fn is_empty(&self) -> bool {
            self.buffer.is_empty()
        }
    };
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitSourceRecord {
    pub repository_id: String,
    pub commit_id: String,
    pub tree_path: String,
    pub object_id: String,
    pub pack_path: String,
    pub pack_version: u64,
    pub offset: u64,
    pub length: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitSourceDocument {
    pub document: DocumentRef,
    pub records: Vec<GitSourceRecord>,
}

pub struct GitSourceSegmentBuilder {
    buffer: MutationBuffer<GitSourceDocument>,
}

impl GitSourceSegmentBuilder {
    pub fn new(options: SegmentBuildOptions) -> Result<Self, IndexError> {
        Ok(Self {
            buffer: MutationBuffer::new(options)?,
        })
    }

    pub fn estimate_mutation(mutation: &IndexMutation<GitSourceDocument>) -> usize {
        match mutation {
            IndexMutation::Upsert(document) => document
                .records
                .iter()
                .fold(0usize, |size, record| {
                    size.saturating_add(GitPayload::record_bytes(record))
                })
                .max(document.records.iter().fold(0usize, |size, record| {
                    size.saturating_add(git_key_resident_bytes(record))
                })),
            IndexMutation::Remove(document) => document.path.len(),
        }
    }

    pub fn try_push(
        &mut self,
        mutation: IndexMutation<GitSourceDocument>,
    ) -> Result<SegmentPush<GitSourceDocument>, IndexError> {
        if let IndexMutation::Upsert(document) = &mutation {
            validate_git_records(&document.records)?;
            preflight_projection_row(git_encoded_bytes(&document.records))?;
        }
        let estimate = Self::estimate_mutation(&mutation);
        self.buffer
            .try_push(mutation, estimate, |document| &document.document)
    }

    builder_state!();

    pub async fn seal<S: IndexBlockSink>(
        self,
        sink: &mut S,
    ) -> Result<Option<SealedRun>, IndexError> {
        self.seal_with_target(sink, DEFAULT_COMPONENT_BLOCK_BYTES)
            .await
    }

    async fn seal_with_target<S: IndexBlockSink>(
        self,
        sink: &mut S,
        target_block_bytes: usize,
    ) -> Result<Option<SealedRun>, IndexError> {
        seal_projection(
            IndexKind::GitSource,
            self.buffer,
            |document| (document.document, GitPayload(document.records)),
            sink,
            target_block_bytes,
        )
        .await
    }
}

pub struct GitSourceEngine;

impl GitSourceEngine {
    pub fn builder(options: SegmentBuildOptions) -> Result<GitSourceSegmentBuilder, IndexError> {
        GitSourceSegmentBuilder::new(options)
    }

    pub async fn get_by_path<D: IndexDirectoryRead>(
        runs: &[D],
        repository_id: &str,
        commit_id: &str,
        tree_path: &str,
    ) -> Result<Option<GitSourceRecord>, IndexError> {
        validate_query_text("repository ID", repository_id, false)?;
        validate_query_text("commit ID", commit_id, false)?;
        validate_query_text("Git tree path", tree_path, false)?;
        let views = open_views(runs, IndexKind::GitSource).await?;
        let prefix = composite_prefix(&[repository_id, commit_id, tree_path], true)?;
        let mut selected = None::<(DocumentRef, GitSourceRecord)>;
        for (run, view) in runs.iter().zip(&views) {
            let Some(root) = view.component_optional(PRIMARY_KEY_TAG) else {
                continue;
            };
            let mut cursor = RoutedCursor::new(run, root.clone(), Some(prefix.clone()));
            while let Some(row) = cursor.next().await? {
                let document = document_by_ordinal(run, view, row.ordinal).await?;
                if !is_latest_live(runs, &views, &document).await? {
                    continue;
                }
                let payload = ordinal_row::<D, GitPayload>(run, view, row.ordinal).await?;
                let record = take_git_record(payload, row.position)?;
                if git_path_primary(&record)? != row.primary {
                    return Err(IndexError::InvalidFormat("Git path key mismatch"));
                }
                if selected
                    .as_ref()
                    .is_none_or(|(current, _)| newer_projection_document(&document, current))
                {
                    selected = Some((document, record));
                }
            }
        }
        Ok(selected.map(|(_, record)| record))
    }

    pub async fn list_tree<D: IndexDirectoryRead>(
        runs: &[D],
        repository_id: &str,
        commit_id: &str,
        tree_prefix: &str,
        after_path: Option<&str>,
        limit: usize,
    ) -> Result<Vec<GitSourceRecord>, IndexError> {
        validate_query_text("repository ID", repository_id, false)?;
        validate_query_text("commit ID", commit_id, false)?;
        validate_query_text("Git tree prefix", tree_prefix, true)?;
        if let Some(after) = after_path {
            validate_query_text("Git tree cursor", after, true)?;
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let views = open_views(runs, IndexKind::GitSource).await?;
        let prefix = composite_prefix(&[repository_id, commit_id, tree_prefix], false)?;
        let mut selected = BTreeMap::<String, RetainedGitRecord>::new();
        let mut retained_bytes = 0usize;
        for (run, view) in runs.iter().zip(&views) {
            let Some(root) = view.component_optional(PRIMARY_KEY_TAG) else {
                continue;
            };
            let mut cursor = RoutedCursor::new(run, root.clone(), Some(prefix.clone()));
            while let Some(row) = cursor.next().await? {
                let document = document_by_ordinal(run, view, row.ordinal).await?;
                if !is_latest_live(runs, &views, &document).await? {
                    continue;
                }
                let payload = ordinal_row::<D, GitPayload>(run, view, row.ordinal).await?;
                let record = take_git_record(payload, row.position)?;
                if git_path_primary(&record)? != row.primary {
                    return Err(IndexError::InvalidFormat("Git path key mismatch"));
                }
                if after_path.is_some_and(|after| record.tree_path.as_str() <= after) {
                    continue;
                }
                let path = record.tree_path.clone();
                if selected
                    .get(&path)
                    .is_none_or(|current| newer_projection_document(&document, &current.document))
                {
                    let value = RetainedGitRecord::new(document, record, path.len());
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
        }
        Ok(selected.into_values().map(|value| value.record).collect())
    }

    pub async fn get_object<D: IndexDirectoryRead>(
        runs: &[D],
        repository_id: &str,
        object_id: &str,
        limit: usize,
    ) -> Result<Vec<GitSourceRecord>, IndexError> {
        validate_query_text("repository ID", repository_id, false)?;
        validate_query_text("Git object ID", object_id, false)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let views = open_views(runs, IndexKind::GitSource).await?;
        let prefix = composite_prefix(&[repository_id, object_id], true)?;
        let mut selected = BTreeMap::<(String, String), RetainedGitRecord>::new();
        let mut retained_bytes = 0usize;
        for (run, view) in runs.iter().zip(&views) {
            let Some(root) = view.component_optional(SECONDARY_KEY_TAG) else {
                continue;
            };
            let mut cursor = RoutedCursor::new(run, root.clone(), Some(prefix.clone()));
            while let Some(row) = cursor.next().await? {
                let document = document_by_ordinal(run, view, row.ordinal).await?;
                if !is_latest_live(runs, &views, &document).await? {
                    continue;
                }
                let payload = ordinal_row::<D, GitPayload>(run, view, row.ordinal).await?;
                let record = take_git_record(payload, row.position)?;
                if git_object_primary(&record)? != row.primary {
                    return Err(IndexError::InvalidFormat("Git object key mismatch"));
                }
                let key = (record.commit_id.clone(), record.tree_path.clone());
                if selected
                    .get(&key)
                    .is_none_or(|current| newer_projection_document(&document, &current.document))
                {
                    let value = RetainedGitRecord::new(
                        document,
                        record,
                        key.0.len().saturating_add(key.1.len()),
                    );
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
        }
        Ok(selected.into_values().map(|value| value.record).collect())
    }

    pub async fn merge_runs<D, S>(
        runs: &[D],
        output_level: u8,
        sink: &mut S,
    ) -> Result<SealedRun, IndexError>
    where
        D: IndexDirectoryRead,
        S: IndexBlockSink + IndexDirectoryRead,
    {
        merge_projection::<D, S, GitPayload>(
            runs,
            IndexKind::GitSource,
            output_level,
            DEFAULT_COMPONENT_BLOCK_BYTES,
            sink,
        )
        .await
    }

    /// Compact path, record, and both Git routing components through bounded
    /// deterministic lanes into one format-valid immutable run.
    pub async fn merge_runs_parallel<D, S, E>(
        runs: &[D],
        output_level: u8,
        sink: &mut S,
        parallelism: crate::compaction::CompactionParallelism,
        progress: crate::compaction::CompactionProgress,
        executor: E,
    ) -> Result<SealedRun, IndexError>
    where
        D: IndexDirectoryRead + Clone + 'static,
        S: IndexBlockSink + IndexDirectoryRead + Clone + 'static,
        E: crate::compaction::CompactionExecutor,
    {
        parallel_compaction::merge_projection_parallel::<D, S, GitPayload, E>(
            runs,
            IndexKind::GitSource,
            output_level,
            DEFAULT_COMPONENT_BLOCK_BYTES,
            sink,
            parallelism,
            progress,
            executor,
        )
        .await
    }
}

struct RetainedGitRecord {
    document: DocumentRef,
    record: GitSourceRecord,
    resident_bytes: usize,
}

impl RetainedGitRecord {
    fn new(document: DocumentRef, record: GitSourceRecord, key_bytes: usize) -> Self {
        let resident_bytes = std::mem::size_of::<Self>()
            .saturating_add(document.path.len())
            .saturating_add(GitPayload::record_bytes(&record))
            .saturating_add(key_bytes);
        Self {
            document,
            record,
            resident_bytes,
        }
    }
}

fn take_git_record(
    row: OrdinalRow<GitPayload>,
    position: u32,
) -> Result<GitSourceRecord, IndexError> {
    row.payload
        .0
        .into_iter()
        .nth(position as usize)
        .ok_or(IndexError::InvalidFormat("Git key record slot"))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TensorRecord {
    pub model_id: String,
    pub tensor_name: String,
    pub source_path: String,
    pub source_version: u64,
    pub offset: u64,
    pub length: u64,
    pub dtype: String,
    pub shape: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TensorDocument {
    pub document: DocumentRef,
    pub records: Vec<TensorRecord>,
}

pub struct TensorSegmentBuilder {
    buffer: MutationBuffer<TensorDocument>,
}

impl TensorSegmentBuilder {
    pub fn new(options: SegmentBuildOptions) -> Result<Self, IndexError> {
        Ok(Self {
            buffer: MutationBuffer::new(options)?,
        })
    }

    pub fn estimate_mutation(mutation: &IndexMutation<TensorDocument>) -> usize {
        match mutation {
            IndexMutation::Upsert(document) => document
                .records
                .iter()
                .fold(0usize, |size, record| {
                    size.saturating_add(TensorPayload::record_bytes(record))
                })
                .max(document.records.iter().fold(0usize, |size, record| {
                    size.saturating_add(tensor_key_resident_bytes(record))
                })),
            IndexMutation::Remove(document) => document.path.len(),
        }
    }

    pub fn try_push(
        &mut self,
        mutation: IndexMutation<TensorDocument>,
    ) -> Result<SegmentPush<TensorDocument>, IndexError> {
        if let IndexMutation::Upsert(document) = &mutation {
            validate_tensor_records(&document.records)?;
            preflight_projection_row(tensor_encoded_bytes(&document.records))?;
        }
        let estimate = Self::estimate_mutation(&mutation);
        self.buffer
            .try_push(mutation, estimate, |document| &document.document)
    }

    builder_state!();

    pub async fn seal<S: IndexBlockSink>(
        self,
        sink: &mut S,
    ) -> Result<Option<SealedRun>, IndexError> {
        self.seal_with_target(sink, DEFAULT_COMPONENT_BLOCK_BYTES)
            .await
    }

    async fn seal_with_target<S: IndexBlockSink>(
        self,
        sink: &mut S,
        target_block_bytes: usize,
    ) -> Result<Option<SealedRun>, IndexError> {
        seal_projection(
            IndexKind::Tensor,
            self.buffer,
            |document| (document.document, TensorPayload(document.records)),
            sink,
            target_block_bytes,
        )
        .await
    }
}

pub struct TensorProjectionEngine;

impl TensorProjectionEngine {
    pub fn builder(options: SegmentBuildOptions) -> Result<TensorSegmentBuilder, IndexError> {
        TensorSegmentBuilder::new(options)
    }

    pub async fn get<D: IndexDirectoryRead>(
        runs: &[D],
        model_id: &str,
        tensor_name: &str,
    ) -> Result<Option<TensorRecord>, IndexError> {
        validate_query_text("model ID", model_id, false)?;
        validate_query_text("tensor name", tensor_name, false)?;
        let views = open_views(runs, IndexKind::Tensor).await?;
        let prefix = composite_prefix(&[model_id, tensor_name], true)?;
        let mut selected = None::<(DocumentRef, TensorRecord)>;
        for (run, view) in runs.iter().zip(&views) {
            let Some(root) = view.component_optional(PRIMARY_KEY_TAG) else {
                continue;
            };
            let mut cursor = RoutedCursor::new(run, root.clone(), Some(prefix.clone()));
            while let Some(row) = cursor.next().await? {
                let document = document_by_ordinal(run, view, row.ordinal).await?;
                if !is_latest_live(runs, &views, &document).await? {
                    continue;
                }
                let payload = ordinal_row::<D, TensorPayload>(run, view, row.ordinal).await?;
                let record = payload
                    .payload
                    .0
                    .get(row.position as usize)
                    .cloned()
                    .ok_or(IndexError::InvalidFormat("tensor key record slot"))?;
                if tensor_primary(&record)? != row.primary {
                    return Err(IndexError::InvalidFormat("tensor key mismatch"));
                }
                if selected
                    .as_ref()
                    .is_none_or(|(current, _)| newer_projection_document(&document, current))
                {
                    selected = Some((document, record));
                }
            }
        }
        Ok(selected.map(|(_, record)| record))
    }

    pub async fn merge_runs<D, S>(
        runs: &[D],
        output_level: u8,
        sink: &mut S,
    ) -> Result<SealedRun, IndexError>
    where
        D: IndexDirectoryRead,
        S: IndexBlockSink + IndexDirectoryRead,
    {
        merge_projection::<D, S, TensorPayload>(
            runs,
            IndexKind::Tensor,
            output_level,
            DEFAULT_COMPONENT_BLOCK_BYTES,
            sink,
        )
        .await
    }

    /// Compact path, record, and tensor routing components through bounded
    /// deterministic lanes into one format-valid immutable run.
    pub async fn merge_runs_parallel<D, S, E>(
        runs: &[D],
        output_level: u8,
        sink: &mut S,
        parallelism: crate::compaction::CompactionParallelism,
        progress: crate::compaction::CompactionProgress,
        executor: E,
    ) -> Result<SealedRun, IndexError>
    where
        D: IndexDirectoryRead + Clone + 'static,
        S: IndexBlockSink + IndexDirectoryRead + Clone + 'static,
        E: crate::compaction::CompactionExecutor,
    {
        parallel_compaction::merge_projection_parallel::<D, S, TensorPayload, E>(
            runs,
            IndexKind::Tensor,
            output_level,
            DEFAULT_COMPONENT_BLOCK_BYTES,
            sink,
            parallelism,
            progress,
            executor,
        )
        .await
    }
}

trait ProjectionPayload: Sized {
    fn encoded_bytes(&self) -> usize;
    fn encode(&self, output: &mut Encoder) -> Result<(), IndexError>;
    fn decode(decoder: &mut Decoder<'_>) -> Result<Self, IndexError>;
    fn key_tags() -> &'static [u8];
    fn key_rows(&self, ordinal: u64) -> Result<Vec<(u8, RoutedRow)>, IndexError>;
}

fn preflight_projection_row(encoded_payload_bytes: usize) -> Result<(), IndexError> {
    let needed = encoded_payload_bytes.saturating_add(16);
    if needed > DEFAULT_COMPONENT_BLOCK_BYTES {
        return Err(IndexError::ResourceLimit {
            needed,
            limit: DEFAULT_COMPONENT_BLOCK_BYTES,
        });
    }
    Ok(())
}

fn git_encoded_bytes(records: &[GitSourceRecord]) -> usize {
    records.iter().fold(4usize, |size, record| {
        size.saturating_add(44)
            .saturating_add(record.repository_id.len())
            .saturating_add(record.commit_id.len())
            .saturating_add(record.tree_path.len())
            .saturating_add(record.object_id.len())
            .saturating_add(record.pack_path.len())
    })
}

fn tensor_encoded_bytes(records: &[TensorRecord]) -> usize {
    records.iter().fold(4usize, |size, record| {
        size.saturating_add(44)
            .saturating_add(record.model_id.len())
            .saturating_add(record.tensor_name.len())
            .saturating_add(record.source_path.len())
            .saturating_add(record.dtype.len())
            .saturating_add(record.shape.len().saturating_mul(8))
    })
}

#[derive(Clone, Debug)]
struct GitPayload(Vec<GitSourceRecord>);

impl GitPayload {
    fn record_bytes(record: &GitSourceRecord) -> usize {
        record
            .repository_id
            .len()
            .saturating_add(record.commit_id.len())
            .saturating_add(record.tree_path.len())
            .saturating_add(record.object_id.len())
            .saturating_add(record.pack_path.len())
            .saturating_add(80)
    }
}

impl ProjectionPayload for GitPayload {
    fn encoded_bytes(&self) -> usize {
        self.0.iter().fold(4usize, |size, record| {
            size.saturating_add(Self::record_bytes(record))
        })
    }

    fn encode(&self, output: &mut Encoder) -> Result<(), IndexError> {
        output.u32(self.0.len())?;
        for record in &self.0 {
            output.string(&record.repository_id)?;
            output.string(&record.commit_id)?;
            output.string(&record.tree_path)?;
            output.string(&record.object_id)?;
            output.string(&record.pack_path)?;
            output.u64(record.pack_version);
            output.u64(record.offset);
            output.u64(record.length);
        }
        Ok(())
    }

    fn decode(decoder: &mut Decoder<'_>) -> Result<Self, IndexError> {
        let count = decoder.u32()? as usize;
        decoder.guard_count::<GitSourceRecord>(count, 44)?;
        let mut records = Vec::with_capacity(count);
        for _ in 0..count {
            records.push(GitSourceRecord {
                repository_id: decoder.string()?,
                commit_id: decoder.string()?,
                tree_path: decoder.string()?,
                object_id: decoder.string()?,
                pack_path: decoder.string()?,
                pack_version: decoder.u64()?,
                offset: decoder.u64()?,
                length: decoder.u64()?,
            });
        }
        validate_git_records(&records)
            .map_err(|_| IndexError::InvalidFormat("invalid Git projection record"))?;
        Ok(Self(records))
    }

    fn key_tags() -> &'static [u8] {
        &[PRIMARY_KEY_TAG, SECONDARY_KEY_TAG]
    }

    fn key_rows(&self, ordinal: u64) -> Result<Vec<(u8, RoutedRow)>, IndexError> {
        let mut rows = Vec::with_capacity(self.0.len().saturating_mul(2));
        for (slot, record) in self.0.iter().enumerate() {
            let slot = u32::try_from(slot).map_err(|_| IndexError::OffsetOverflow)?;
            rows.push((
                PRIMARY_KEY_TAG,
                RoutedRow::new(git_path_primary(record)?, ordinal, slot)?,
            ));
            rows.push((
                SECONDARY_KEY_TAG,
                RoutedRow::new(git_object_primary(record)?, ordinal, slot)?,
            ));
        }
        Ok(rows)
    }
}

#[derive(Clone, Debug)]
struct TensorPayload(Vec<TensorRecord>);

impl TensorPayload {
    fn record_bytes(record: &TensorRecord) -> usize {
        record
            .model_id
            .len()
            .saturating_add(record.tensor_name.len())
            .saturating_add(record.source_path.len())
            .saturating_add(record.dtype.len())
            .saturating_add(record.shape.len().saturating_mul(8))
            .saturating_add(64)
    }
}

impl ProjectionPayload for TensorPayload {
    fn encoded_bytes(&self) -> usize {
        self.0.iter().fold(4usize, |size, record| {
            size.saturating_add(Self::record_bytes(record))
        })
    }

    fn encode(&self, output: &mut Encoder) -> Result<(), IndexError> {
        output.u32(self.0.len())?;
        for record in &self.0 {
            output.string(&record.model_id)?;
            output.string(&record.tensor_name)?;
            output.string(&record.source_path)?;
            output.u64(record.source_version);
            output.u64(record.offset);
            output.u64(record.length);
            output.string(&record.dtype)?;
            output.u32(record.shape.len())?;
            for dimension in &record.shape {
                output.u64(*dimension);
            }
        }
        Ok(())
    }

    fn decode(decoder: &mut Decoder<'_>) -> Result<Self, IndexError> {
        let count = decoder.u32()? as usize;
        decoder.guard_count::<TensorRecord>(count, 44)?;
        let mut records = Vec::with_capacity(count);
        for _ in 0..count {
            let model_id = decoder.string()?;
            let tensor_name = decoder.string()?;
            let source_path = decoder.string()?;
            let source_version = decoder.u64()?;
            let offset = decoder.u64()?;
            let length = decoder.u64()?;
            let dtype = decoder.string()?;
            let shape_count = decoder.u32()? as usize;
            decoder.guard_count::<u64>(shape_count, 8)?;
            let mut shape = Vec::with_capacity(shape_count);
            for _ in 0..shape_count {
                shape.push(decoder.u64()?);
            }
            records.push(TensorRecord {
                model_id,
                tensor_name,
                source_path,
                source_version,
                offset,
                length,
                dtype,
                shape,
            });
        }
        validate_tensor_records(&records)
            .map_err(|_| IndexError::InvalidFormat("invalid tensor projection record"))?;
        Ok(Self(records))
    }

    fn key_tags() -> &'static [u8] {
        &[PRIMARY_KEY_TAG]
    }

    fn key_rows(&self, ordinal: u64) -> Result<Vec<(u8, RoutedRow)>, IndexError> {
        self.0
            .iter()
            .enumerate()
            .map(|(slot, record)| {
                Ok((
                    PRIMARY_KEY_TAG,
                    RoutedRow::new(
                        tensor_primary(record)?,
                        ordinal,
                        u32::try_from(slot).map_err(|_| IndexError::OffsetOverflow)?,
                    )?,
                ))
            })
            .collect()
    }
}

#[derive(Debug)]
struct OrdinalRow<T> {
    ordinal: u64,
    payload: T,
}

struct OrdinalComponentWriter<T> {
    kind: IndexKind,
    level: u8,
    target_bytes: usize,
    estimated_bytes: usize,
    rows: Vec<OrdinalRow<T>>,
    tree: crate::run::RoutingTreeBuilder,
}

impl<T: ProjectionPayload> OrdinalComponentWriter<T> {
    fn new(kind: IndexKind, level: u8, target_bytes: usize) -> Self {
        Self {
            kind,
            level,
            target_bytes: target_bytes.max(256),
            estimated_bytes: 0,
            rows: Vec::new(),
            tree: crate::run::RoutingTreeBuilder::new(kind, RECORDS_TAG),
        }
    }

    async fn push<S: IndexBlockSink>(
        &mut self,
        row: OrdinalRow<T>,
        sink: &mut S,
    ) -> Result<(), IndexError> {
        let row_bytes = row.payload.encoded_bytes().saturating_add(16);
        if !self.rows.is_empty()
            && self.estimated_bytes.saturating_add(row_bytes) > self.target_bytes
        {
            self.flush(sink).await?;
        }
        if self
            .rows
            .last()
            .is_some_and(|previous| previous.ordinal >= row.ordinal)
        {
            return Err(IndexError::UnsortedRecords);
        }
        self.estimated_bytes = self.estimated_bytes.saturating_add(row_bytes);
        self.rows.push(row);
        Ok(())
    }

    async fn flush<S: IndexBlockSink>(&mut self, sink: &mut S) -> Result<(), IndexError> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.rows);
        self.estimated_bytes = 0;
        let codec = if self.level == 0 {
            ComponentCodec::FixedRows
        } else {
            ComponentCodec::PrefixEliasFano
        };
        let body = encode_ordinal_rows(&rows, codec)?;
        let first = ordinal_key(rows.first().unwrap().ordinal);
        let last = ordinal_key(rows.last().unwrap().ordinal);
        let bytes = encode_component(self.kind, RECORDS_TAG, codec, body)?;
        self.tree
            .emit_leaf(
                crate::GeneratedBlock::new(
                    self.kind,
                    RECORDS_TAG,
                    codec,
                    0,
                    first,
                    last,
                    rows.len() as u64,
                    bytes,
                )?,
                sink,
            )
            .await
    }

    async fn finish<S: IndexBlockSink>(
        mut self,
        sink: &mut S,
    ) -> Result<ComponentTree, IndexError> {
        self.flush(sink).await?;
        self.tree.finish(sink).await
    }
}

fn encode_ordinal_rows<T: ProjectionPayload>(
    rows: &[OrdinalRow<T>],
    codec: ComponentCodec,
) -> Result<Vec<u8>, IndexError> {
    let mut output = Encoder::default();
    output.u32(rows.len())?;
    if codec == ComponentCodec::PrefixEliasFano {
        output.bytes(&encode_elias_fano(
            &rows.iter().map(|row| row.ordinal).collect::<Vec<_>>(),
        )?)?;
    }
    for row in rows {
        if codec == ComponentCodec::FixedRows {
            output.u64(row.ordinal);
        }
        row.payload.encode(&mut output)?;
    }
    Ok(output.finish())
}

fn decode_ordinal_rows<T: ProjectionPayload>(
    bytes: &[u8],
    codec: ComponentCodec,
) -> Result<Vec<OrdinalRow<T>>, IndexError> {
    let mut decoder = Decoder::new(bytes);
    let count = decoder.u32()? as usize;
    decoder.guard_count::<OrdinalRow<T>>(count, 4)?;
    let ordinals = if codec == ComponentCodec::PrefixEliasFano {
        let budget = decoder.budget();
        let sequence = decode_elias_fano_with_budget(decoder.bytes()?, budget)?;
        if sequence.len() != count {
            return Err(IndexError::InvalidFormat("projection ordinal count"));
        }
        Some(sequence)
    } else if codec == ComponentCodec::FixedRows {
        None
    } else {
        return Err(IndexError::InvalidFormat("projection block codec"));
    };
    let mut rows = Vec::with_capacity(count);
    for index in 0..count {
        rows.push(OrdinalRow {
            ordinal: match &ordinals {
                Some(values) => values.get(index)?,
                None => decoder.u64()?,
            },
            payload: T::decode(&mut decoder)?,
        });
    }
    decoder.finish()?;
    if rows.is_empty()
        || rows
            .windows(2)
            .any(|pair| pair[0].ordinal >= pair[1].ordinal)
    {
        return Err(IndexError::InvalidFormat("projection ordinal order"));
    }
    Ok(rows)
}

async fn read_ordinal_block<D: IndexDirectoryRead, T: ProjectionPayload>(
    directory: &D,
    descriptor: &crate::BlockDescriptor,
) -> Result<Vec<OrdinalRow<T>>, IndexError> {
    let block = crate::run::read_leaf(directory, descriptor).await?;
    let rows = decode_ordinal_rows(block.body(), descriptor.codec)?;
    if rows.first().map(|row| ordinal_key(row.ordinal)) != Some(descriptor.minimum_key.clone())
        || rows.last().map(|row| ordinal_key(row.ordinal)) != Some(descriptor.maximum_key.clone())
        || rows.len() as u64 != descriptor.element_count
    {
        return Err(IndexError::InvalidFormat("projection block descriptor"));
    }
    Ok(rows)
}

async fn ordinal_row<D: IndexDirectoryRead, T: ProjectionPayload>(
    directory: &D,
    view: &RunView,
    ordinal: u64,
) -> Result<OrdinalRow<T>, IndexError> {
    let root = view
        .component_optional(RECORDS_TAG)
        .ok_or(IndexError::InvalidFormat("missing projection component"))?;
    let descriptor = find_leaf(directory, root, &ordinal_key(ordinal))
        .await?
        .ok_or(IndexError::InvalidFormat("missing projection ordinal"))?;
    let rows = read_ordinal_block(directory, &descriptor).await?;
    let index = rows
        .binary_search_by_key(&ordinal, |row| row.ordinal)
        .map_err(|_| IndexError::InvalidFormat("missing projection ordinal"))?;
    Ok(rows.into_iter().nth(index).unwrap())
}

async fn seal_projection<T, P, S>(
    kind: IndexKind,
    buffer: MutationBuffer<T>,
    project: impl Fn(T) -> (DocumentRef, P),
    sink: &mut S,
    target_block_bytes: usize,
) -> Result<Option<SealedRun>, IndexError>
where
    P: ProjectionPayload,
    S: IndexBlockSink,
{
    if buffer.is_empty() {
        return Ok(None);
    }
    let level = buffer.level();
    let entries = buffer.into_entries();
    let mut paths = PathComponentWriter::new(kind, level, target_block_bytes);
    let mut documents = DocumentComponentWriter::new(kind, level, target_block_bytes);
    let mut projections = OrdinalComponentWriter::new(kind, level, target_block_bytes);
    let mut keyed = BTreeMap::<u8, Vec<RoutedRow>>::new();
    let mut live = 0u64;
    let mut minimum_version = u64::MAX;
    let mut maximum_version = 0u64;
    let mutation_count = entries.len() as u64;
    for entry in entries.into_values() {
        match entry.mutation {
            IndexMutation::Upsert(value) => {
                let (document, payload) = project(value);
                let ordinal = live;
                let key_rows = payload.key_rows(ordinal)?;
                live += 1;
                minimum_version = minimum_version.min(document.version);
                maximum_version = maximum_version.max(document.version);
                paths
                    .push(
                        PathChange {
                            document: document.clone(),
                            state: DocumentState::Live,
                            document_ordinal: Some(ordinal),
                        },
                        sink,
                    )
                    .await?;
                documents
                    .push(DocumentRecord { ordinal, document }, sink)
                    .await?;
                projections
                    .push(OrdinalRow { ordinal, payload }, sink)
                    .await?;
                for (tag, row) in key_rows {
                    keyed.entry(tag).or_default().push(row);
                }
            }
            IndexMutation::Remove(document) => {
                minimum_version = minimum_version.min(document.version);
                maximum_version = maximum_version.max(document.version);
                paths
                    .push(
                        PathChange {
                            document,
                            state: DocumentState::Removed,
                            document_ordinal: None,
                        },
                        sink,
                    )
                    .await?;
            }
        }
    }
    let mut components = vec![paths.finish(sink).await?];
    if live > 0 {
        components.push(documents.finish(sink).await?);
        components.push(projections.finish(sink).await?);
        for tag in P::key_tags() {
            let mut rows = keyed.remove(tag).unwrap_or_default();
            rows.sort_by(RoutedRow::compare);
            if rows
                .windows(2)
                .any(|pair| pair[0].compare(&pair[1]) != std::cmp::Ordering::Less)
            {
                return Err(IndexError::InvalidDefinition(
                    "projection query keys must be unique within one source run".into(),
                ));
            }
            let mut writer = RoutedComponentWriter::new(kind, *tag, level, target_block_bytes);
            for row in rows {
                writer.push(row, sink).await?;
            }
            if let Some(tree) = writer.finish(sink).await? {
                components.push(tree);
            }
        }
    }
    Ok(Some(seal_run_root(
        kind,
        level,
        RunStatistics {
            mutation_count,
            live_document_count: live,
            minimum_version,
            maximum_version,
        },
        components,
    )?))
}

async fn merge_projection<D, S, T>(
    runs: &[D],
    kind: IndexKind,
    output_level: u8,
    target_block_bytes: usize,
    sink: &mut S,
) -> Result<SealedRun, IndexError>
where
    D: IndexDirectoryRead,
    S: IndexBlockSink + IndexDirectoryRead,
    T: ProjectionPayload,
{
    if runs.is_empty() || output_level == 0 {
        return Err(IndexError::InvalidDefinition(
            "projection compaction requires input runs and an L1+ output level".into(),
        ));
    }
    let views = open_views(runs, kind).await?;
    let mut cursors = Vec::with_capacity(runs.len());
    for (run, view) in runs.iter().zip(&views) {
        cursors.push(PathRunCursor::new(
            run,
            view.component(PATH_CHANGES_TAG)?.clone(),
        ));
    }
    let mut current = Vec::with_capacity(cursors.len());
    for cursor in &mut cursors {
        current.push(cursor.next().await?);
    }
    let mut paths = PathComponentWriter::new(kind, output_level, target_block_bytes);
    let mut documents = DocumentComponentWriter::new(kind, output_level, target_block_bytes);
    let mut projections = OrdinalComponentWriter::new(kind, output_level, target_block_bytes);
    let mut mutation_count = 0u64;
    let mut live = 0u64;
    let mut minimum_version = u64::MAX;
    let mut maximum_version = 0u64;
    loop {
        let Some(path) = current
            .iter()
            .flatten()
            .map(|row| row.document.path.as_str())
            .min()
            .map(str::to_owned)
        else {
            break;
        };
        let mut winner = None::<(usize, PathChange)>;
        for (run_index, row) in current.iter().enumerate() {
            let Some(row) = row.as_ref().filter(|row| row.document.path == path) else {
                continue;
            };
            if winner.as_ref().is_none_or(|(current_index, current)| {
                row.document.version > current.document.version
                    || (row.document.version == current.document.version
                        && run_index < *current_index)
            }) {
                winner = Some((run_index, row.clone()));
            }
        }
        for (run_index, row) in current.iter_mut().enumerate() {
            if row.as_ref().is_some_and(|row| row.document.path == path) {
                *row = cursors[run_index].next().await?;
            }
        }
        let (winner_run, mut winner) = winner.unwrap();
        if winner.state == DocumentState::Live {
            let old_ordinal = winner
                .document_ordinal
                .ok_or(IndexError::InvalidFormat("live projection has no ordinal"))?;
            let source_document =
                document_by_ordinal(&runs[winner_run], &views[winner_run], old_ordinal).await?;
            if source_document != winner.document {
                return Err(IndexError::InvalidFormat("projection document mismatch"));
            }
            let payload = ordinal_row::<D, T>(&runs[winner_run], &views[winner_run], old_ordinal)
                .await?
                .payload;
            let ordinal = live;
            live += 1;
            winner.document_ordinal = Some(ordinal);
            documents
                .push(
                    DocumentRecord {
                        ordinal,
                        document: winner.document.clone(),
                    },
                    sink,
                )
                .await?;
            projections
                .push(OrdinalRow { ordinal, payload }, sink)
                .await?;
        } else {
            winner.document_ordinal = None;
        }
        minimum_version = minimum_version.min(winner.document.version);
        maximum_version = maximum_version.max(winner.document.version);
        mutation_count += 1;
        paths.push(winner, sink).await?;
    }
    if mutation_count == 0 {
        return Err(IndexError::InvalidDefinition(
            "projection compaction produced no changes".into(),
        ));
    }
    let path_tree = paths.finish(sink).await?;
    let path_root = path_tree.root.clone();
    let mut components = vec![path_tree];
    if live > 0 {
        components.push(documents.finish(sink).await?);
        components.push(projections.finish(sink).await?);
        for tag in T::key_tags() {
            if let Some(tree) = merge_routed_component(
                runs,
                &views,
                kind,
                *tag,
                output_level,
                target_block_bytes,
                &path_root,
                sink,
            )
            .await?
            {
                components.push(tree);
            }
        }
    }
    seal_run_root(
        kind,
        output_level,
        RunStatistics {
            mutation_count,
            live_document_count: live,
            minimum_version,
            maximum_version,
        },
        components,
    )
}

async fn merge_routed_component<D, S>(
    runs: &[D],
    views: &[RunView],
    kind: IndexKind,
    tag: u8,
    output_level: u8,
    target_block_bytes: usize,
    output_path_root: &crate::BlockDescriptor,
    sink: &mut S,
) -> Result<Option<ComponentTree>, IndexError>
where
    D: IndexDirectoryRead,
    S: IndexBlockSink + IndexDirectoryRead,
{
    let mut cursors = runs
        .iter()
        .zip(views)
        .map(|(run, view)| {
            view.component_optional(tag)
                .cloned()
                .map(|root| RoutedCursor::new(run, root, None))
        })
        .collect::<Vec<_>>();
    let mut current = Vec::with_capacity(cursors.len());
    for cursor in &mut cursors {
        current.push(match cursor {
            Some(cursor) => cursor.next().await?,
            None => None,
        });
    }
    let mut writer = RoutedComponentWriter::new(kind, tag, output_level, target_block_bytes);
    loop {
        let Some(primary) = current
            .iter()
            .flatten()
            .map(|row| row.primary.as_slice())
            .min()
            .map(<[u8]>::to_vec)
        else {
            break;
        };
        while current.iter().flatten().any(|row| row.primary == primary) {
            let mut documents = vec![None; current.len()];
            for run_index in 0..current.len() {
                let Some(row) = current[run_index]
                    .as_ref()
                    .filter(|row| row.primary == primary)
                else {
                    continue;
                };
                documents[run_index] = Some(
                    document_by_ordinal(&runs[run_index], &views[run_index], row.ordinal).await?,
                );
            }
            let path = documents
                .iter()
                .flatten()
                .map(|document| document.path.as_str())
                .min()
                .ok_or(IndexError::InvalidFormat("projection key without document"))?
                .to_owned();
            let mut winner = None::<usize>;
            for (run_index, document) in documents.iter().enumerate() {
                let Some(document) = document.as_ref().filter(|document| document.path == path)
                else {
                    continue;
                };
                if winner.is_none_or(|current_index| {
                    let current = documents[current_index].as_ref().unwrap();
                    document.version > current.version
                        || (document.version == current.version && run_index < current_index)
                }) {
                    winner = Some(run_index);
                }
            }
            let winner_index = winner.unwrap();
            let winner_document = documents[winner_index].as_ref().unwrap().clone();
            let winner_row = current[winner_index].as_ref().unwrap().clone();
            for run_index in 0..current.len() {
                if documents[run_index]
                    .as_ref()
                    .is_some_and(|document| document.path == path)
                {
                    current[run_index] = cursors[run_index]
                        .as_mut()
                        .expect("a current row always has a cursor")
                        .next()
                        .await?;
                }
            }
            let Some(output) = path_change_in_tree(&*sink, output_path_root, &path).await? else {
                return Err(IndexError::InvalidFormat(
                    "compacted path missing from staged output",
                ));
            };
            if output.state == DocumentState::Live
                && output.document.version == winner_document.version
            {
                let ordinal = output.document_ordinal.ok_or(IndexError::InvalidFormat(
                    "compacted live path has no ordinal",
                ))?;
                writer.push(winner_row.with_ordinal(ordinal), sink).await?;
            }
        }
    }
    writer.finish(sink).await
}

async fn open_views<D: IndexDirectoryRead>(
    runs: &[D],
    kind: IndexKind,
) -> Result<Vec<RunView>, IndexError> {
    let mut views = Vec::with_capacity(runs.len());
    for run in runs {
        views.push(open_run(run, kind).await?);
    }
    Ok(views)
}

fn ordinal_key(ordinal: u64) -> Vec<u8> {
    ordinal.to_be_bytes().to_vec()
}

fn newer_projection_document(candidate: &DocumentRef, current: &DocumentRef) -> bool {
    candidate.version > current.version
        || (candidate.version == current.version && candidate.path > current.path)
}

fn composite_prefix(parts: &[&str], terminate_last: bool) -> Result<Vec<u8>, IndexError> {
    let primary = join_components(parts, terminate_last);
    if primary.len() > crate::MAX_INDEX_ROUTING_KEY_BYTES.saturating_sub(12) {
        return Err(IndexError::InvalidQuery(
            "projection query key exceeds the format limit".into(),
        ));
    }
    Ok(primary)
}

fn projection_primary(parts: &[&str]) -> Result<Vec<u8>, IndexError> {
    let primary = join_components(parts, true);
    if primary.len() > crate::MAX_INDEX_ROUTING_KEY_BYTES.saturating_sub(12) {
        return Err(IndexError::InvalidDefinition(
            "projection query key exceeds the format limit".into(),
        ));
    }
    Ok(primary)
}

fn join_components(parts: &[&str], terminate_last: bool) -> Vec<u8> {
    let separators = parts.len().saturating_sub(1) + usize::from(terminate_last);
    let mut key = Vec::with_capacity(
        parts
            .iter()
            .map(|part| part.len())
            .sum::<usize>()
            .saturating_add(separators),
    );
    for (index, part) in parts.iter().enumerate() {
        key.extend_from_slice(part.as_bytes());
        if index + 1 < parts.len() || terminate_last {
            key.push(0);
        }
    }
    key
}

fn git_path_primary(record: &GitSourceRecord) -> Result<Vec<u8>, IndexError> {
    projection_primary(&[&record.repository_id, &record.commit_id, &record.tree_path])
}

fn git_object_primary(record: &GitSourceRecord) -> Result<Vec<u8>, IndexError> {
    projection_primary(&[
        &record.repository_id,
        &record.object_id,
        &record.commit_id,
        &record.tree_path,
    ])
}

fn tensor_primary(record: &TensorRecord) -> Result<Vec<u8>, IndexError> {
    projection_primary(&[&record.model_id, &record.tensor_name])
}

fn git_key_resident_bytes(record: &GitSourceRecord) -> usize {
    let path_key = record
        .repository_id
        .len()
        .saturating_add(record.commit_id.len())
        .saturating_add(record.tree_path.len())
        .saturating_add(3 + ROUTED_ROW_RESIDENT_OVERHEAD_BYTES);
    let object_key = record
        .repository_id
        .len()
        .saturating_add(record.object_id.len())
        .saturating_add(record.commit_id.len())
        .saturating_add(record.tree_path.len())
        .saturating_add(4 + ROUTED_ROW_RESIDENT_OVERHEAD_BYTES);
    path_key.saturating_add(object_key)
}

fn tensor_key_resident_bytes(record: &TensorRecord) -> usize {
    record
        .model_id
        .len()
        .saturating_add(record.tensor_name.len())
        .saturating_add(2 + ROUTED_ROW_RESIDENT_OVERHEAD_BYTES)
}

fn validate_text(label: &str, value: &str) -> Result<(), IndexError> {
    if value.is_empty() || value.contains('\0') {
        return Err(IndexError::InvalidDefinition(format!(
            "{label} must be non-empty and contain no NUL"
        )));
    }
    Ok(())
}

fn validate_query_text(label: &str, value: &str, empty_allowed: bool) -> Result<(), IndexError> {
    if (!empty_allowed && value.is_empty()) || value.contains('\0') {
        return Err(IndexError::InvalidQuery(format!("invalid {label}")));
    }
    Ok(())
}

fn validate_git_records(records: &[GitSourceRecord]) -> Result<(), IndexError> {
    let mut keys = BTreeSet::new();
    for record in records {
        validate_text("repository ID", &record.repository_id)?;
        validate_text("commit ID", &record.commit_id)?;
        validate_text("Git tree path", &record.tree_path)?;
        validate_text("Git object ID", &record.object_id)?;
        validate_text("Git pack path", &record.pack_path)?;
        if !keys.insert((&record.repository_id, &record.commit_id, &record.tree_path)) {
            return Err(IndexError::InvalidDefinition(
                "one source document contains a duplicate Git commit path".into(),
            ));
        }
    }
    Ok(())
}

fn validate_tensor_records(records: &[TensorRecord]) -> Result<(), IndexError> {
    let mut keys = BTreeSet::new();
    for record in records {
        validate_text("model ID", &record.model_id)?;
        validate_text("tensor name", &record.tensor_name)?;
        validate_text("tensor source path", &record.source_path)?;
        validate_text("tensor dtype", &record.dtype)?;
        if !keys.insert((&record.model_id, &record.tensor_name)) {
            return Err(IndexError::InvalidDefinition(
                "one source document contains a duplicate model tensor".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::io::tests::{MemoryBlockSink, MemoryDirectory};

    use super::*;

    fn git_document(
        path: &str,
        version: u64,
        tree_path: &str,
        object_id: &str,
    ) -> IndexMutation<GitSourceDocument> {
        IndexMutation::Upsert(GitSourceDocument {
            document: DocumentRef {
                path: path.into(),
                version,
            },
            records: vec![GitSourceRecord {
                repository_id: "repo".into(),
                commit_id: "abc".into(),
                tree_path: tree_path.into(),
                object_id: object_id.into(),
                pack_path: "/pack".into(),
                pack_version: version,
                offset: 1,
                length: 2,
            }],
        })
    }

    async fn git_run(
        mutations: impl IntoIterator<Item = IndexMutation<GitSourceDocument>>,
        level: u8,
        target: usize,
    ) -> (MemoryBlockSink, SealedRun) {
        let mut builder =
            GitSourceSegmentBuilder::new(SegmentBuildOptions::for_level(64 * 1024, level).unwrap())
                .unwrap();
        for mutation in mutations {
            assert!(matches!(
                builder.try_push(mutation).unwrap(),
                SegmentPush::Accepted
            ));
        }
        let mut sink = MemoryBlockSink::default();
        let run = builder
            .seal_with_target(&mut sink, target)
            .await
            .unwrap()
            .unwrap();
        (sink, run)
    }

    fn directory(sink: &MemoryBlockSink, run: SealedRun) -> MemoryDirectory {
        sink.directory_with_root(run.into_root())
    }

    #[tokio::test]
    async fn git_updates_tombstones_and_streaming_compaction_are_equivalent() {
        let (old_sink, old_run) = git_run(
            [
                git_document("/a", 1, "src/lib.rs", "old"),
                git_document("/b", 1, "src/gone.rs", "gone"),
            ],
            0,
            96,
        )
        .await;
        let old = directory(&old_sink, old_run);
        let (new_sink, new_run) = git_run(
            [
                git_document("/a", 2, "src/lib.rs", "new"),
                IndexMutation::Remove(DocumentRef {
                    path: "/b".into(),
                    version: 2,
                }),
            ],
            0,
            96,
        )
        .await;
        let new = directory(&new_sink, new_run);
        let runs = [new, old];
        assert_eq!(
            GitSourceEngine::get_by_path(&runs, "repo", "abc", "src/lib.rs")
                .await
                .unwrap()
                .unwrap()
                .object_id,
            "new"
        );
        assert!(
            GitSourceEngine::get_by_path(&runs, "repo", "abc", "src/gone.rs")
                .await
                .unwrap()
                .is_none()
        );

        let mut merged_sink = MemoryBlockSink::default();
        let merged = merge_projection::<_, _, GitPayload>(
            &runs,
            IndexKind::GitSource,
            1,
            96,
            &mut merged_sink,
        )
        .await
        .unwrap();
        let merged = [directory(&merged_sink, merged)];
        assert_eq!(
            GitSourceEngine::get_by_path(&merged, "repo", "abc", "src/lib.rs")
                .await
                .unwrap()
                .unwrap()
                .object_id,
            "new"
        );
        assert!(
            GitSourceEngine::get_by_path(&merged, "repo", "abc", "src/gone.rs")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn deterministic_tiny_blocks_and_corrupt_root_are_detected() {
        let mutations = (0..80)
            .map(|index| {
                git_document(
                    &format!("/source/{index:04}"),
                    1,
                    &format!("src/{index:04}.rs"),
                    &format!("object-{index:04}"),
                )
            })
            .collect::<Vec<_>>();
        let (first_sink, first) = git_run(mutations.clone(), 1, 128).await;
        let (second_sink, second) = git_run(mutations, 1, 128).await;
        assert_eq!(first.descriptor().hash, second.descriptor().hash);
        assert_eq!(first_sink.len(), second_sink.len());
        assert!(first_sink.len() > 6);

        let (_, mut root) = first.into_root().into_parts();
        *root.last_mut().unwrap() ^= 1;
        let corrupt = MemoryDirectory::new([(crate::run::RUN_ROOT_FILE.into(), root)]);
        assert!(matches!(
            GitSourceEngine::get_by_path(&[corrupt], "repo", "abc", "src/0000.rs").await,
            Err(IndexError::Integrity)
        ));
    }

    #[tokio::test]
    async fn git_hot_object_prefix_stays_complete_with_tiny_blocks() {
        let mutations = (0..120)
            .map(|index| {
                git_document(
                    &format!("/source/{index:04}"),
                    1,
                    &format!("src/{index:04}.rs"),
                    "shared-object",
                )
            })
            .collect::<Vec<_>>();
        let (sink, run) = git_run(mutations, 1, 128).await;
        let hits =
            GitSourceEngine::get_object(&[directory(&sink, run)], "repo", "shared-object", 25)
                .await
                .unwrap();
        assert_eq!(hits.len(), 25);
        assert_eq!(hits.first().unwrap().tree_path, "src/0000.rs");
        assert_eq!(hits.last().unwrap().tree_path, "src/0024.rs");
    }

    #[tokio::test]
    async fn tensor_projection_hides_a_deleted_source() {
        let source = DocumentRef {
            path: "/manifest".into(),
            version: 1,
        };
        let mut old = TensorSegmentBuilder::new(SegmentBuildOptions::new(4096).unwrap()).unwrap();
        old.try_push(IndexMutation::Upsert(TensorDocument {
            document: source.clone(),
            records: vec![TensorRecord {
                model_id: "model".into(),
                tensor_name: "weight".into(),
                source_path: "/data".into(),
                source_version: 1,
                offset: 0,
                length: 4,
                dtype: "F32".into(),
                shape: vec![1],
            }],
        }))
        .unwrap();
        let mut old_sink = MemoryBlockSink::default();
        let old_run = old.seal(&mut old_sink).await.unwrap().unwrap();
        let old = directory(&old_sink, old_run);
        let mut new = TensorSegmentBuilder::new(SegmentBuildOptions::new(4096).unwrap()).unwrap();
        new.try_push(IndexMutation::Remove(DocumentRef {
            version: 2,
            ..source
        }))
        .unwrap();
        let mut new_sink = MemoryBlockSink::default();
        let new_run = new.seal(&mut new_sink).await.unwrap().unwrap();
        let new = directory(&new_sink, new_run);
        assert!(
            TensorProjectionEngine::get(&[new, old], "model", "weight")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn tensor_output_is_deterministic_and_rejects_an_oversized_source() {
        let build = |suffix: &str| {
            let mut builder =
                TensorSegmentBuilder::new(SegmentBuildOptions::for_level(64 * 1024, 1).unwrap())
                    .unwrap();
            for index in 0..80 {
                builder
                    .try_push(IndexMutation::Upsert(TensorDocument {
                        document: DocumentRef {
                            path: format!("/manifest/{index:04}"),
                            version: 1,
                        },
                        records: vec![TensorRecord {
                            model_id: "model".into(),
                            tensor_name: format!("weight-{index:04}"),
                            source_path: format!("/data/{suffix}"),
                            source_version: 1,
                            offset: index,
                            length: 4,
                            dtype: "F32".into(),
                            shape: vec![1],
                        }],
                    }))
                    .unwrap();
            }
            builder
        };
        let mut first_sink = MemoryBlockSink::default();
        let first = build("stable")
            .seal_with_target(&mut first_sink, 128)
            .await
            .unwrap()
            .unwrap();
        let mut second_sink = MemoryBlockSink::default();
        let second = build("stable")
            .seal_with_target(&mut second_sink, 128)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.descriptor().hash, second.descriptor().hash);
        assert_eq!(first_sink.len(), second_sink.len());
        assert!(first_sink.len() > 6);

        let mut bounded =
            TensorSegmentBuilder::new(SegmentBuildOptions::new(256).unwrap()).unwrap();
        let oversized = IndexMutation::Upsert(TensorDocument {
            document: DocumentRef {
                path: "/oversized".into(),
                version: 1,
            },
            records: vec![TensorRecord {
                model_id: "model".into(),
                tensor_name: "weight".into(),
                source_path: "x".repeat(1024),
                source_version: 1,
                offset: 0,
                length: 4,
                dtype: "F32".into(),
                shape: vec![1],
            }],
        });
        assert!(matches!(
            bounded.try_push(oversized),
            Err(IndexError::ResourceLimit { .. })
        ));
    }

    #[test]
    fn projection_rows_larger_than_one_block_fail_preflight() {
        assert!(matches!(
            preflight_projection_row(DEFAULT_COMPONENT_BLOCK_BYTES + 1),
            Err(IndexError::ResourceLimit { .. })
        ));
    }

    #[test]
    fn oversized_tensor_payload_fails_before_buffer_admission() {
        let shape = vec![1; DEFAULT_COMPONENT_BLOCK_BYTES / 8 + 1];
        let mut builder = TensorSegmentBuilder::new(
            SegmentBuildOptions::new(DEFAULT_COMPONENT_BLOCK_BYTES * 2).unwrap(),
        )
        .unwrap();
        let result = builder.try_push(IndexMutation::Upsert(TensorDocument {
            document: DocumentRef {
                path: "/large".into(),
                version: 1,
            },
            records: vec![TensorRecord {
                model_id: "model".into(),
                tensor_name: "weights".into(),
                source_path: "/payload".into(),
                source_version: 1,
                offset: 0,
                length: 1,
                dtype: "u8".into(),
                shape,
            }],
        }));
        assert!(matches!(result, Err(IndexError::ResourceLimit { .. })));
        assert!(builder.is_empty());
    }

    include!("projections/query_bounds_tests.rs");
    include!("projections/parallel_compaction_tests.rs");
}
