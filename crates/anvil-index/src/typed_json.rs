//! Bounded typed-JSON and fixed object-metadata runs.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::codec::{Decoder, Encoder};
use crate::routed::{ROUTED_ROW_RESIDENT_OVERHEAD_BYTES, RoutedRow};
use crate::run::{ComponentTree, RunStatistics, RunView, open_run, seal_run_root};
use crate::segment::{
    DEFAULT_COMPONENT_BLOCK_BYTES, DocumentComponentWriter, DocumentRecord, DocumentState,
    LatestLiveProbe, MutationBuffer, PATH_CHANGES_TAG, PathChange, PathComponentWriter,
    PathRunCursor, document_by_ordinal,
};
use crate::{
    DocumentRef, IndexBlockSink, IndexDirectoryRead, IndexError, IndexKind, IndexMutation,
    SealedRun, SegmentBuildOptions, SegmentPush,
};

pub(crate) const ROWS_TAG: u8 = 20;
pub(crate) const KEYS_TAG: u8 = 21;
const EXISTS_VALUE_TAG: u8 = 4;
const UNSIGNED_VALUE_TAG: u8 = 5;
const EXISTS_POSITION: u32 = u32::MAX;

#[path = "typed_json/query.rs"]
mod query;
use query::query_typed;

#[path = "typed_json/compaction_cache.rs"]
mod compaction_cache;
use compaction_cache::CompactionPointCache;

#[path = "typed_json/identity.rs"]
mod identity;
#[cfg(test)]
use crate::run::LeafCursor;
#[cfg(test)]
use crate::{ComponentCodec, MAX_INDEX_DECODED_BLOCK_BYTES};
#[cfg(test)]
use identity::{
    ELIAS_FANO_DECODED_BYTES_PER_VALUE, ELIAS_FANO_DECODED_FIXED_BYTES, decode_typed_rows,
    read_typed_block,
};
pub(crate) use identity::{TypedComponentWriter, TypedRow};
use identity::{TypedCursor, range_local_ordinal, typed_row};

#[path = "typed_json/postings.rs"]
mod postings;
pub(crate) use postings::PostingComponentWriter;
use postings::PostingCursor;

#[path = "typed_json/key_rebuild.rs"]
mod key_rebuild;
#[path = "typed_json/parallel_compaction.rs"]
mod parallel_compaction;

#[cfg(test)]
#[path = "typed_json/compaction_cache_tests.rs"]
mod compaction_cache_tests;

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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Number(f64),
    /// Exact unsigned value used by Anvil's fixed object-head metadata fields.
    /// Typed JSON numbers remain `Number`; this variant prevents u64 metadata
    /// identities and counters from losing precision through f64 conversion.
    Unsigned(u64),
    String(String),
}

impl ScalarValue {
    pub fn from_json(value: &serde_json::Value) -> Option<Self> {
        match value {
            serde_json::Value::Null => Some(Self::Null),
            serde_json::Value::Bool(value) => Some(Self::Boolean(*value)),
            serde_json::Value::Number(value) => value.as_f64().map(Self::Number),
            serde_json::Value::String(value) => Some(Self::String(value.clone())),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => None,
        }
    }

    fn compare(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Null, Self::Null) => Some(Ordering::Equal),
            (Self::Boolean(left), Self::Boolean(right)) => Some(left.cmp(right)),
            (Self::Number(left), Self::Number(right)) => left.partial_cmp(right),
            (Self::Unsigned(left), Self::Unsigned(right)) => Some(left.cmp(right)),
            (Self::String(left), Self::String(right)) => Some(left.cmp(right)),
            _ => None,
        }
    }
}

/// Only the configured fields selected by the streaming source parser. Array
/// values contribute multiple scalars; no complete JSON document crosses the
/// index-engine admission boundary.
pub type SelectedScalarFields = BTreeMap<String, Vec<ScalarValue>>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypedField {
    pub name: String,
    pub json_pointer: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypedJsonDefinition {
    pub fields: Vec<TypedField>,
}

impl TypedJsonDefinition {
    pub fn validate(&self) -> Result<(), IndexError> {
        if self.fields.is_empty() {
            return Err(IndexError::InvalidDefinition(
                "typed JSON index needs at least one field".into(),
            ));
        }
        let mut names = BTreeSet::new();
        for field in &self.fields {
            if field.name.is_empty()
                || field.name.contains('\0')
                || (!field.json_pointer.is_empty() && !field.json_pointer.starts_with('/'))
            {
                return Err(IndexError::InvalidDefinition(format!(
                    "invalid typed JSON field `{}`",
                    field.name
                )));
            }
            if !names.insert(&field.name) {
                return Err(IndexError::InvalidDefinition(format!(
                    "duplicate typed JSON field `{}`",
                    field.name
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TypedJsonDocument {
    pub document: DocumentRef,
    pub fields: SelectedScalarFields,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetadataDocument {
    pub document: DocumentRef,
    pub fields: SelectedScalarFields,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TypedPredicate {
    Exists {
        field: String,
    },
    Equal {
        field: String,
        value: ScalarValue,
    },
    In {
        field: String,
        values: Vec<ScalarValue>,
    },
    LessThan {
        field: String,
        value: ScalarValue,
    },
    LessThanOrEqual {
        field: String,
        value: ScalarValue,
    },
    GreaterThan {
        field: String,
        value: ScalarValue,
    },
    GreaterThanOrEqual {
        field: String,
        value: ScalarValue,
    },
    Prefix {
        field: String,
        prefix: String,
    },
}

impl TypedPredicate {
    fn field(&self) -> &str {
        match self {
            Self::Exists { field }
            | Self::Equal { field, .. }
            | Self::In { field, .. }
            | Self::LessThan { field, .. }
            | Self::LessThanOrEqual { field, .. }
            | Self::GreaterThan { field, .. }
            | Self::GreaterThanOrEqual { field, .. }
            | Self::Prefix { field, .. } => field,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypedOrder {
    pub field: String,
    pub descending: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TypedQuery {
    pub predicates: Vec<TypedPredicate>,
    pub order: Vec<TypedOrder>,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TypedHit {
    pub document: DocumentRef,
    pub fields: BTreeMap<String, Vec<ScalarValue>>,
}

/// Exclusive continuation key in the exact final order of a typed query.
/// Values correspond positionally to the query's order fields; `None` is the
/// existing missing-value sort key. The document is the final stable tie-break.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TypedQueryCursor {
    pub values: Vec<Option<ScalarValue>>,
    pub document: DocumentRef,
}

impl TypedQueryCursor {
    pub fn from_hit(hit: &TypedHit, order: &[TypedOrder]) -> Self {
        Self {
            values: order
                .iter()
                .map(|specification| {
                    hit.fields
                        .get(&specification.field)
                        .and_then(|values| values.first())
                        .cloned()
                })
                .collect(),
            document: hit.document.clone(),
        }
    }
}

pub struct TypedJsonSegmentBuilder {
    definition: TypedJsonDefinition,
    buffer: MutationBuffer<TypedJsonDocument>,
}

impl TypedJsonSegmentBuilder {
    pub fn new(
        definition: TypedJsonDefinition,
        options: SegmentBuildOptions,
    ) -> Result<Self, IndexError> {
        definition.validate()?;
        Ok(Self {
            definition,
            buffer: MutationBuffer::new(options)?,
        })
    }

    pub fn estimate_mutation(mutation: &IndexMutation<TypedJsonDocument>) -> usize {
        match mutation {
            IndexMutation::Upsert(document) => estimate_selected_fields(&document.fields),
            IndexMutation::Remove(document) => document.path.len(),
        }
    }

    pub fn try_push(
        &mut self,
        mutation: IndexMutation<TypedJsonDocument>,
    ) -> Result<SegmentPush<TypedJsonDocument>, IndexError> {
        let estimate = Self::estimate_mutation(&mutation);
        if let IndexMutation::Upsert(document) = &mutation {
            validate_selected_fields(&self.definition, &document.fields)?;
            preflight_typed_row(&document.fields)?;
        }
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
        let Self { buffer, .. } = self;
        seal_typed(
            IndexKind::TypedJson,
            buffer,
            |document| (document.document, TypedPayload::canonical(document.fields)),
            sink,
            target_block_bytes,
        )
        .await
    }
}

pub struct MetadataSegmentBuilder {
    definition: TypedJsonDefinition,
    buffer: MutationBuffer<MetadataDocument>,
}

impl MetadataSegmentBuilder {
    pub fn new(
        definition: TypedJsonDefinition,
        options: SegmentBuildOptions,
    ) -> Result<Self, IndexError> {
        definition.validate()?;
        Ok(Self {
            definition,
            buffer: MutationBuffer::new(options)?,
        })
    }

    pub fn estimate_mutation(mutation: &IndexMutation<MetadataDocument>) -> usize {
        match mutation {
            IndexMutation::Upsert(document) => estimate_selected_fields(&document.fields),
            IndexMutation::Remove(document) => document.path.len(),
        }
    }

    pub fn try_push(
        &mut self,
        mutation: IndexMutation<MetadataDocument>,
    ) -> Result<SegmentPush<MetadataDocument>, IndexError> {
        let estimate = Self::estimate_mutation(&mutation);
        if let IndexMutation::Upsert(document) = &mutation {
            validate_selected_fields(&self.definition, &document.fields)?;
            preflight_typed_row(&document.fields)?;
        }
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
        let Self { buffer, .. } = self;
        seal_typed(
            IndexKind::MetadataFilter,
            buffer,
            |document| (document.document, TypedPayload::canonical(document.fields)),
            sink,
            target_block_bytes,
        )
        .await
    }
}

pub struct TypedJsonEngine;

impl TypedJsonEngine {
    pub fn builder(
        definition: TypedJsonDefinition,
        options: SegmentBuildOptions,
    ) -> Result<TypedJsonSegmentBuilder, IndexError> {
        TypedJsonSegmentBuilder::new(definition, options)
    }

    pub async fn query<D: IndexDirectoryRead>(
        runs: &[D],
        definition: &TypedJsonDefinition,
        query: &TypedQuery,
    ) -> Result<Vec<TypedHit>, IndexError> {
        Self::query_after(runs, definition, query, None).await
    }

    pub async fn query_after<D: IndexDirectoryRead>(
        runs: &[D],
        definition: &TypedJsonDefinition,
        query: &TypedQuery,
        after: Option<&TypedQueryCursor>,
    ) -> Result<Vec<TypedHit>, IndexError> {
        query_typed(runs, definition, query, IndexKind::TypedJson, after).await
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
        merge_typed(
            runs,
            IndexKind::TypedJson,
            output_level,
            DEFAULT_COMPONENT_BLOCK_BYTES,
            sink,
        )
        .await
    }

    /// Compact deterministic path and typed-key ranges concurrently into one
    /// format-valid immutable run.
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
        parallel_compaction::merge_typed_parallel(
            runs,
            IndexKind::TypedJson,
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

pub struct MetadataFilterEngine;

impl MetadataFilterEngine {
    pub fn builder(
        definition: TypedJsonDefinition,
        options: SegmentBuildOptions,
    ) -> Result<MetadataSegmentBuilder, IndexError> {
        MetadataSegmentBuilder::new(definition, options)
    }

    pub fn definition_for_fields(
        fields: impl IntoIterator<Item = String>,
    ) -> Result<TypedJsonDefinition, IndexError> {
        let definition = TypedJsonDefinition {
            fields: fields
                .into_iter()
                .map(|name| TypedField {
                    json_pointer: format!("/{}", escape_json_pointer(&name)),
                    name,
                })
                .collect(),
        };
        definition.validate()?;
        Ok(definition)
    }

    pub async fn query<D: IndexDirectoryRead>(
        runs: &[D],
        definition: &TypedJsonDefinition,
        query: &TypedQuery,
    ) -> Result<Vec<TypedHit>, IndexError> {
        Self::query_after(runs, definition, query, None).await
    }

    pub async fn query_after<D: IndexDirectoryRead>(
        runs: &[D],
        definition: &TypedJsonDefinition,
        query: &TypedQuery,
        after: Option<&TypedQueryCursor>,
    ) -> Result<Vec<TypedHit>, IndexError> {
        query_typed(runs, definition, query, IndexKind::MetadataFilter, after).await
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
        merge_typed(
            runs,
            IndexKind::MetadataFilter,
            output_level,
            DEFAULT_COMPONENT_BLOCK_BYTES,
            sink,
        )
        .await
    }

    /// Compact deterministic path and metadata-key ranges concurrently into
    /// one format-valid immutable run.
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
        parallel_compaction::merge_typed_parallel(
            runs,
            IndexKind::MetadataFilter,
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

#[derive(Clone, Debug)]
pub(crate) struct TypedPayload {
    pub(crate) fields: BTreeMap<String, Vec<ScalarValue>>,
}

impl TypedPayload {
    pub(crate) fn canonical(fields: SelectedScalarFields) -> Self {
        Self {
            fields: fields
                .into_iter()
                .filter_map(|(field, values)| {
                    let values = canonicalize_values(values);
                    (!values.is_empty()).then_some((field, values))
                })
                .collect(),
        }
    }

    fn encoded_bytes(&self) -> usize {
        self.fields.iter().fold(4usize, |size, (field, values)| {
            values.iter().fold(
                size.saturating_add(field.len()).saturating_add(8),
                |size, value| size.saturating_add(scalar_bytes(value)),
            )
        })
    }

    fn decoded_resident_bytes(&self) -> usize {
        self.fields.iter().fold(0usize, |size, (field, values)| {
            let values_bytes = values.iter().fold(
                values
                    .len()
                    .saturating_mul(std::mem::size_of::<ScalarValue>()),
                |size, value| {
                    size.saturating_add(match value {
                        ScalarValue::String(value) => value.len(),
                        ScalarValue::Null
                        | ScalarValue::Boolean(_)
                        | ScalarValue::Number(_)
                        | ScalarValue::Unsigned(_) => 0,
                    })
                },
            );
            size.saturating_add(std::mem::size_of::<(String, Vec<ScalarValue>)>())
                .saturating_add(64)
                .saturating_add(field.len())
                .saturating_add(values_bytes)
        })
    }

    fn encode(&self, output: &mut Encoder) -> Result<(), IndexError> {
        output.u32(self.fields.len())?;
        for (field, values) in &self.fields {
            output.string(field)?;
            output.u32(values.len())?;
            for value in values {
                encode_scalar(output, value)?;
            }
        }
        Ok(())
    }

    fn decode(decoder: &mut Decoder<'_>) -> Result<Self, IndexError> {
        let field_count = decoder.u32()? as usize;
        decoder.guard_count::<(String, Vec<ScalarValue>)>(field_count, 8)?;
        decoder.charge(
            field_count
                .checked_mul(64)
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        let mut fields = BTreeMap::new();
        for _ in 0..field_count {
            let field = decoder.string()?;
            let value_count = decoder.u32()? as usize;
            decoder.guard_count::<ScalarValue>(value_count, 1)?;
            let mut values = Vec::with_capacity(value_count);
            for _ in 0..value_count {
                values.push(decode_scalar(decoder)?);
            }
            if field.is_empty()
                || field.contains('\0')
                || values.is_empty()
                || !values_are_canonical(&values)
                || fields.insert(field, values).is_some()
            {
                return Err(IndexError::InvalidFormat("canonical typed fields"));
            }
        }
        Ok(Self { fields })
    }

    pub(crate) fn key_rows(&self, ordinal: u64) -> Result<Vec<RoutedRow>, IndexError> {
        let mut rows = Vec::new();
        let mut position = 0u32;
        for (field, values) in &self.fields {
            rows.push(typed_exists_row(field, ordinal)?);
            for value in values {
                rows.push(RoutedRow::new(
                    typed_primary(field, value)?,
                    ordinal,
                    position,
                )?);
                position = position.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
            }
        }
        Ok(rows)
    }

    fn matches_key(&self, primary: &[u8], position: u32) -> Result<bool, IndexError> {
        if position == EXISTS_POSITION {
            for field in self.fields.keys() {
                if typed_exists_primary(field)? == primary {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
        let target = usize::try_from(position).map_err(|_| IndexError::OffsetOverflow)?;
        let mut current = 0usize;
        for (field, values) in &self.fields {
            for value in values {
                if current == target {
                    return Ok(typed_primary(field, value)? == primary);
                }
                current += 1;
            }
        }
        Ok(false)
    }
}

async fn seal_typed<T, S>(
    kind: IndexKind,
    buffer: MutationBuffer<T>,
    project: impl Fn(T) -> (DocumentRef, TypedPayload),
    sink: &mut S,
    target_block_bytes: usize,
) -> Result<Option<SealedRun>, IndexError>
where
    S: IndexBlockSink,
{
    if buffer.is_empty() {
        return Ok(None);
    }
    let level = buffer.level();
    let entries = buffer.into_entries();
    let mutation_count = entries.len() as u64;
    let mut paths = PathComponentWriter::new(kind, level, target_block_bytes);
    let mut documents = DocumentComponentWriter::new(kind, level, target_block_bytes);
    let mut typed = TypedComponentWriter::new(kind, level, target_block_bytes);
    let mut keys = Vec::<RoutedRow>::new();
    let mut live = 0u64;
    let mut minimum_version = u64::MAX;
    let mut maximum_version = 0u64;
    for entry in entries.into_values() {
        match entry.mutation {
            IndexMutation::Upsert(value) => {
                let (document, payload) = project(value);
                let ordinal = range_local_ordinal(0, live)?;
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
                typed.push(TypedRow { ordinal, payload }, sink).await?;
                keys.extend(key_rows);
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
        components.push(typed.finish(sink).await?);
        keys.sort_by(RoutedRow::compare);
        if keys
            .windows(2)
            .any(|pair| pair[0].compare(&pair[1]) != Ordering::Less)
        {
            return Err(IndexError::InvalidDefinition(
                "typed query keys must be unique within one source run".into(),
            ));
        }
        let mut writer = PostingComponentWriter::new(kind, target_block_bytes);
        for row in keys {
            writer.push(row, sink).await?;
        }
        if let Some(tree) = writer.finish(sink).await? {
            components.push(tree);
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

async fn merge_typed<D, S>(
    runs: &[D],
    kind: IndexKind,
    output_level: u8,
    target_block_bytes: usize,
    sink: &mut S,
) -> Result<SealedRun, IndexError>
where
    D: IndexDirectoryRead,
    S: IndexBlockSink + IndexDirectoryRead,
{
    if runs.is_empty() || output_level == 0 {
        return Err(IndexError::InvalidDefinition(
            "typed compaction requires input runs and an L1+ output level".into(),
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
    let mut typed = TypedComponentWriter::new(kind, output_level, target_block_bytes);
    let mut point_cache = CompactionPointCache::default();
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
                .ok_or(IndexError::InvalidFormat("live typed row has no ordinal"))?;
            let source_document = point_cache
                .document(&runs[winner_run], &views[winner_run], old_ordinal)
                .await?;
            if source_document != winner.document {
                return Err(IndexError::InvalidFormat("typed document mismatch"));
            }
            let payload = point_cache
                .typed(&runs[winner_run], &views[winner_run], old_ordinal)
                .await?
                .payload;
            let ordinal = range_local_ordinal(0, live)?;
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
            typed.push(TypedRow { ordinal, payload }, sink).await?;
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
            "typed compaction produced no changes".into(),
        ));
    }
    drop(current);
    drop(cursors);
    let path_tree = paths.finish(sink).await?;
    let path_root = path_tree.root.clone();
    let mut components = vec![path_tree];
    if live > 0 {
        components.push(documents.finish(sink).await?);
        components.push(typed.finish(sink).await?);
        drop(point_cache);
        if let Some(tree) =
            merge_typed_keys(runs, &views, kind, target_block_bytes, &path_root, sink).await?
        {
            components.push(tree);
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

async fn merge_typed_keys<D, S>(
    runs: &[D],
    views: &[RunView],
    kind: IndexKind,
    target_block_bytes: usize,
    output_path_root: &crate::BlockDescriptor,
    sink: &mut S,
) -> Result<Option<ComponentTree>, IndexError>
where
    D: IndexDirectoryRead,
    S: IndexBlockSink + IndexDirectoryRead,
{
    let mut point_cache = CompactionPointCache::default();
    let mut cursors = runs
        .iter()
        .zip(views)
        .map(|(run, view)| {
            view.component_optional(KEYS_TAG)
                .cloned()
                .map(|root| PostingCursor::new(run, root, None))
        })
        .collect::<Vec<_>>();
    let mut current = Vec::with_capacity(cursors.len());
    for cursor in &mut cursors {
        current.push(match cursor {
            Some(cursor) => cursor.next().await?,
            None => None,
        });
    }
    let mut writer = PostingComponentWriter::new(kind, target_block_bytes);
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
                    point_cache
                        .document(&runs[run_index], &views[run_index], row.ordinal)
                        .await?,
                );
            }
            let path = documents
                .iter()
                .flatten()
                .map(|document| document.path.as_str())
                .min()
                .ok_or(IndexError::InvalidFormat("typed key without document"))?
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
            let Some(output) = point_cache.path(&*sink, output_path_root, &path).await? else {
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

fn validate_query(definition: &TypedJsonDefinition, query: &TypedQuery) -> Result<(), IndexError> {
    definition.validate()?;
    let fields = definition
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<BTreeSet<_>>();
    for field in query
        .predicates
        .iter()
        .map(TypedPredicate::field)
        .chain(query.order.iter().map(|order| order.field.as_str()))
    {
        if !fields.contains(field) {
            return Err(IndexError::InvalidQuery(format!(
                "field `{field}` is not part of this index"
            )));
        }
    }
    if query.predicates.iter().any(
        |predicate| matches!(predicate, TypedPredicate::In { values, .. } if values.is_empty()),
    ) {
        return Err(IndexError::InvalidQuery(
            "typed IN predicates require at least one value".into(),
        ));
    }
    for value in query.predicates.iter().flat_map(predicate_values) {
        if matches!(value, ScalarValue::Number(number) if !number.is_finite()) {
            return Err(IndexError::InvalidQuery(
                "typed query numbers must be finite".into(),
            ));
        }
    }
    Ok(())
}

fn predicate_values(predicate: &TypedPredicate) -> &[ScalarValue] {
    match predicate {
        TypedPredicate::Equal { value, .. }
        | TypedPredicate::LessThan { value, .. }
        | TypedPredicate::LessThanOrEqual { value, .. }
        | TypedPredicate::GreaterThan { value, .. }
        | TypedPredicate::GreaterThanOrEqual { value, .. } => std::slice::from_ref(value),
        TypedPredicate::In { values, .. } => values,
        TypedPredicate::Exists { .. } | TypedPredicate::Prefix { .. } => &[],
    }
}

fn query_driver_ranges(
    predicates: &[TypedPredicate],
) -> Result<Option<Vec<crate::compaction::KeyRange>>, IndexError> {
    let Some(predicate) = predicates.iter().min_by_key(|predicate| match predicate {
        TypedPredicate::Equal { .. } => 0,
        TypedPredicate::Prefix { .. } => 1,
        TypedPredicate::In { .. } => 2,
        TypedPredicate::LessThan { .. }
        | TypedPredicate::LessThanOrEqual { .. }
        | TypedPredicate::GreaterThan { .. }
        | TypedPredicate::GreaterThanOrEqual { .. } => 3,
        TypedPredicate::Exists { .. } => 4,
    }) else {
        return Ok(None);
    };
    let ranges = match predicate {
        TypedPredicate::Exists { field } => {
            vec![query_prefix_range(typed_exists_primary(field).map_err(
                |_| IndexError::InvalidQuery("typed query key exceeds the format limit".into()),
            )?)]
        }
        TypedPredicate::Equal { field, value } => {
            vec![query_prefix_range(typed_query_primary(field, value)?)]
        }
        TypedPredicate::In { field, values } => {
            let mut keys = values
                .iter()
                .map(|value| typed_query_primary(field, value))
                .collect::<Result<Vec<_>, _>>()?;
            keys.sort();
            keys.dedup();
            keys.into_iter().map(query_prefix_range).collect()
        }
        TypedPredicate::Prefix { field, prefix } => {
            vec![query_prefix_range(typed_string_prefix(field, prefix)?)]
        }
        TypedPredicate::LessThan { field, value } => query_nonempty_range(
            typed_scalar_prefix(field, value)?,
            Some(typed_query_primary(field, value)?),
        )
        .into_iter()
        .collect(),
        TypedPredicate::LessThanOrEqual { field, value } => {
            let scalar_prefix = typed_scalar_prefix(field, value)?;
            let value = typed_query_primary(field, value)?;
            query_nonempty_range(scalar_prefix, crate::routed::prefix_successor(&value))
                .into_iter()
                .collect()
        }
        TypedPredicate::GreaterThan { field, value } => {
            let scalar_prefix = typed_scalar_prefix(field, value)?;
            query_nonempty_range(
                crate::routed::prefix_successor(&typed_query_primary(field, value)?)
                    .unwrap_or_else(|| scalar_prefix.clone()),
                crate::routed::prefix_successor(&scalar_prefix),
            )
            .into_iter()
            .collect()
        }
        TypedPredicate::GreaterThanOrEqual { field, value } => {
            let scalar_prefix = typed_scalar_prefix(field, value)?;
            query_nonempty_range(
                typed_query_primary(field, value)?,
                crate::routed::prefix_successor(&scalar_prefix),
            )
            .into_iter()
            .collect()
        }
    };
    Ok(Some(ranges))
}

fn typed_query_primary(field: &str, value: &ScalarValue) -> Result<Vec<u8>, IndexError> {
    typed_primary(field, value)
        .map_err(|_| IndexError::InvalidQuery("typed query key exceeds the format limit".into()))
}

fn typed_scalar_prefix(field: &str, value: &ScalarValue) -> Result<Vec<u8>, IndexError> {
    let mut key = typed_field_prefix(field)?;
    key.push(match value {
        ScalarValue::Null => 0,
        ScalarValue::Boolean(_) => 1,
        ScalarValue::Number(_) => 2,
        ScalarValue::String(_) => 3,
        ScalarValue::Unsigned(_) => UNSIGNED_VALUE_TAG,
    });
    validate_query_key(key)
}

fn typed_exists_primary(field: &str) -> Result<Vec<u8>, IndexError> {
    let mut key = Vec::with_capacity(field.len().saturating_add(2));
    key.extend_from_slice(field.as_bytes());
    key.push(0);
    key.push(EXISTS_VALUE_TAG);
    if key.len() > crate::MAX_INDEX_ROUTING_KEY_BYTES.saturating_sub(12) {
        return Err(IndexError::InvalidDefinition(
            "typed query key exceeds the format limit".into(),
        ));
    }
    Ok(key)
}

pub(crate) fn typed_exists_row(field: &str, ordinal: u64) -> Result<RoutedRow, IndexError> {
    RoutedRow::new(typed_exists_primary(field)?, ordinal, EXISTS_POSITION)
}

fn query_prefix_range(prefix: Vec<u8>) -> crate::compaction::KeyRange {
    crate::compaction::KeyRange {
        upper: crate::routed::prefix_successor(&prefix),
        lower: Some(prefix),
    }
}

fn query_nonempty_range(
    lower: Vec<u8>,
    upper: Option<Vec<u8>>,
) -> Option<crate::compaction::KeyRange> {
    if upper
        .as_ref()
        .is_some_and(|upper| lower.as_slice() >= upper.as_slice())
    {
        None
    } else {
        Some(crate::compaction::KeyRange {
            lower: Some(lower),
            upper,
        })
    }
}

fn typed_field_prefix(field: &str) -> Result<Vec<u8>, IndexError> {
    let mut key = Vec::with_capacity(field.len().saturating_add(1));
    key.extend_from_slice(field.as_bytes());
    key.push(0);
    validate_query_key(key)
}

fn typed_string_prefix(field: &str, prefix: &str) -> Result<Vec<u8>, IndexError> {
    let mut key = typed_field_prefix(field)?;
    key.push(3);
    append_escaped_string(&mut key, prefix, false);
    validate_query_key(key)
}

fn validate_query_key(key: Vec<u8>) -> Result<Vec<u8>, IndexError> {
    if key.len() > crate::MAX_INDEX_ROUTING_KEY_BYTES.saturating_sub(12) {
        return Err(IndexError::InvalidQuery(
            "typed query key exceeds the format limit".into(),
        ));
    }
    Ok(key)
}

pub(crate) fn validate_selected_fields(
    definition: &TypedJsonDefinition,
    fields: &SelectedScalarFields,
) -> Result<(), IndexError> {
    let allowed = definition
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<BTreeSet<_>>();
    for (field, values) in fields {
        if !allowed.contains(field.as_str()) || values.is_empty() {
            return Err(IndexError::InvalidDefinition(format!(
                "invalid selected field `{field}`"
            )));
        }
        for value in values {
            typed_primary(field, value)?;
        }
    }
    Ok(())
}

fn canonicalize_values(values: Vec<ScalarValue>) -> Vec<ScalarValue> {
    let mut keyed = values
        .into_iter()
        .filter_map(|value| sortable_scalar_key(&value).ok().map(|key| (key, value)))
        .collect::<Vec<_>>();
    keyed.sort_by(|left, right| left.0.cmp(&right.0));
    keyed.dedup_by(|left, right| left.0 == right.0);
    keyed.into_iter().map(|(_, value)| value).collect()
}

fn values_are_canonical(values: &[ScalarValue]) -> bool {
    let mut previous = None::<Vec<u8>>;
    for value in values {
        let Ok(key) = sortable_scalar_key(value) else {
            return false;
        };
        if previous.as_ref().is_some_and(|previous| previous >= &key) {
            return false;
        }
        previous = Some(key);
    }
    true
}

fn row_accepts(fields: &BTreeMap<String, Vec<ScalarValue>>, predicate: &TypedPredicate) -> bool {
    let Some(values) = fields.get(predicate.field()) else {
        return false;
    };
    match predicate {
        TypedPredicate::Exists { .. } => true,
        TypedPredicate::Equal { value, .. } => values.iter().any(|actual| actual == value),
        TypedPredicate::In {
            values: expected, ..
        } => values.iter().any(|actual| expected.contains(actual)),
        TypedPredicate::LessThan { value, .. } => values
            .iter()
            .any(|actual| actual.compare(value) == Some(Ordering::Less)),
        TypedPredicate::LessThanOrEqual { value, .. } => values.iter().any(|actual| {
            matches!(
                actual.compare(value),
                Some(Ordering::Less | Ordering::Equal)
            )
        }),
        TypedPredicate::GreaterThan { value, .. } => values
            .iter()
            .any(|actual| actual.compare(value) == Some(Ordering::Greater)),
        TypedPredicate::GreaterThanOrEqual { value, .. } => values.iter().any(|actual| {
            matches!(
                actual.compare(value),
                Some(Ordering::Greater | Ordering::Equal)
            )
        }),
        TypedPredicate::Prefix { prefix, .. } => values.iter().any(
            |actual| matches!(actual, ScalarValue::String(value) if value.starts_with(prefix)),
        ),
    }
}

fn compare_hits(left: &TypedHit, right: &TypedHit, order: &[TypedOrder]) -> Ordering {
    for specification in order {
        let left_value = left
            .fields
            .get(&specification.field)
            .and_then(|values| values.first());
        let right_value = right
            .fields
            .get(&specification.field)
            .and_then(|values| values.first());
        let ordering = compare_order_value(left_value, right_value, specification.descending);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.document.cmp(&right.document)
}

fn compare_hit_to_cursor(
    hit: &TypedHit,
    cursor: &TypedQueryCursor,
    order: &[TypedOrder],
) -> Ordering {
    for (index, specification) in order.iter().enumerate() {
        let hit_value = hit
            .fields
            .get(&specification.field)
            .and_then(|values| values.first());
        let ordering = compare_order_value(
            hit_value,
            cursor.values[index].as_ref(),
            specification.descending,
        );
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    hit.document.cmp(&cursor.document)
}

fn compare_order_value(
    left: Option<&ScalarValue>,
    right: Option<&ScalarValue>,
    descending: bool,
) -> Ordering {
    let ordering = match (left, right) {
        (Some(left), Some(right)) => left.compare(right).unwrap_or(Ordering::Equal),
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
    };
    if descending {
        ordering.reverse()
    } else {
        ordering
    }
}

fn validate_typed_cursor(
    cursor: Option<&TypedQueryCursor>,
    query: &TypedQuery,
) -> Result<(), IndexError> {
    let Some(cursor) = cursor else {
        return Ok(());
    };
    if cursor.values.len() != query.order.len()
        || cursor.document.validate().is_err()
        || cursor
            .values
            .iter()
            .flatten()
            .any(|value| matches!(value, ScalarValue::Number(number) if !number.is_finite()))
    {
        return Err(IndexError::InvalidQuery(
            "invalid typed query continuation".into(),
        ));
    }
    Ok(())
}

fn encode_scalar(output: &mut Encoder, value: &ScalarValue) -> Result<(), IndexError> {
    match value {
        ScalarValue::Null => output.u8(0),
        ScalarValue::Boolean(value) => {
            output.u8(1);
            output.bool(*value);
        }
        ScalarValue::Number(value) => {
            if !value.is_finite() {
                return Err(IndexError::InvalidDefinition(
                    "typed JSON numbers must be finite".into(),
                ));
            }
            output.u8(2);
            output.f64(if *value == 0.0 { 0.0 } else { *value });
        }
        ScalarValue::Unsigned(value) => {
            output.u8(UNSIGNED_VALUE_TAG);
            output.u64(*value);
        }
        ScalarValue::String(value) => {
            output.u8(3);
            output.string(value)?;
        }
    }
    Ok(())
}

fn decode_scalar(decoder: &mut Decoder<'_>) -> Result<ScalarValue, IndexError> {
    match decoder.u8()? {
        0 => Ok(ScalarValue::Null),
        1 => Ok(ScalarValue::Boolean(decoder.bool()?)),
        2 => {
            let value = decoder.f64()?;
            if value.is_finite() {
                Ok(ScalarValue::Number(if value == 0.0 { 0.0 } else { value }))
            } else {
                Err(IndexError::InvalidFormat("non-finite typed number"))
            }
        }
        3 => Ok(ScalarValue::String(decoder.string()?)),
        UNSIGNED_VALUE_TAG => Ok(ScalarValue::Unsigned(decoder.u64()?)),
        _ => Err(IndexError::InvalidFormat("typed scalar tag")),
    }
}

fn typed_primary(field: &str, value: &ScalarValue) -> Result<Vec<u8>, IndexError> {
    let mut key = Vec::with_capacity(field.len().saturating_add(scalar_bytes(value) + 1));
    key.extend_from_slice(field.as_bytes());
    key.push(0);
    key.extend_from_slice(&sortable_scalar_key(value)?);
    if key.len() > crate::MAX_INDEX_ROUTING_KEY_BYTES.saturating_sub(12) {
        return Err(IndexError::InvalidDefinition(
            "typed query key exceeds the format limit".into(),
        ));
    }
    Ok(key)
}

fn sortable_scalar_key(value: &ScalarValue) -> Result<Vec<u8>, IndexError> {
    let mut key = Vec::with_capacity(scalar_bytes(value));
    match value {
        ScalarValue::Null => key.push(0),
        ScalarValue::Boolean(value) => {
            key.push(1);
            key.push(u8::from(*value));
        }
        ScalarValue::Number(value) => {
            if !value.is_finite() {
                return Err(IndexError::InvalidDefinition(
                    "typed JSON numbers must be finite".into(),
                ));
            }
            key.push(2);
            let normalized = if *value == 0.0 { 0.0 } else { *value };
            let bits = normalized.to_bits();
            let ordered = if bits >> 63 == 1 {
                !bits
            } else {
                bits ^ (1 << 63)
            };
            key.extend_from_slice(&ordered.to_be_bytes());
        }
        ScalarValue::Unsigned(value) => {
            key.push(UNSIGNED_VALUE_TAG);
            key.extend_from_slice(&value.to_be_bytes());
        }
        ScalarValue::String(value) => {
            key.push(3);
            append_escaped_string(&mut key, value, true);
        }
    }
    Ok(key)
}

fn append_escaped_string(output: &mut Vec<u8>, value: &str, terminate: bool) {
    for byte in value.as_bytes() {
        if *byte == 0 {
            output.extend_from_slice(&[0, u8::MAX]);
        } else {
            output.push(*byte);
        }
    }
    if terminate {
        output.extend_from_slice(&[0, 0]);
    }
}

fn scalar_bytes(value: &ScalarValue) -> usize {
    match value {
        ScalarValue::Null => 1,
        ScalarValue::Boolean(_) => 2,
        ScalarValue::Number(_) | ScalarValue::Unsigned(_) => 9,
        ScalarValue::String(value) => value.len().saturating_add(5),
    }
}

fn estimate_selected_fields(fields: &SelectedScalarFields) -> usize {
    let mut source = 0usize;
    let mut derived = 0usize;
    for (field, values) in fields {
        source = source.saturating_add(field.len()).saturating_add(64);
        derived = derived
            .saturating_add(field.len())
            .saturating_add(2)
            .saturating_add(ROUTED_ROW_RESIDENT_OVERHEAD_BYTES);
        for value in values {
            source = source.saturating_add(scalar_resident_bytes(value));
            derived = derived
                .saturating_add(field.len())
                .saturating_add(1)
                .saturating_add(sortable_scalar_bytes(value))
                .saturating_add(ROUTED_ROW_RESIDENT_OVERHEAD_BYTES);
        }
    }
    source.max(derived)
}

pub(crate) fn preflight_typed_row(fields: &SelectedScalarFields) -> Result<(), IndexError> {
    let encoded = fields.iter().fold(4usize, |size, (field, values)| {
        values.iter().fold(
            size.saturating_add(field.len()).saturating_add(8),
            |size, value| size.saturating_add(scalar_bytes(value)),
        )
    });
    let needed = encoded.saturating_add(16);
    if needed > DEFAULT_COMPONENT_BLOCK_BYTES {
        return Err(IndexError::ResourceLimit {
            needed,
            limit: DEFAULT_COMPONENT_BLOCK_BYTES,
        });
    }
    Ok(())
}

fn scalar_resident_bytes(value: &ScalarValue) -> usize {
    match value {
        ScalarValue::String(value) => value.len().saturating_add(48),
        ScalarValue::Null
        | ScalarValue::Boolean(_)
        | ScalarValue::Number(_)
        | ScalarValue::Unsigned(_) => 32,
    }
}

fn sortable_scalar_bytes(value: &ScalarValue) -> usize {
    match value {
        ScalarValue::Null => 1,
        ScalarValue::Boolean(_) => 2,
        ScalarValue::Number(_) | ScalarValue::Unsigned(_) => 9,
        ScalarValue::String(value) => value
            .len()
            .saturating_add(value.as_bytes().iter().filter(|byte| **byte == 0).count())
            .saturating_add(3),
    }
}

fn ordinal_key(ordinal: u64) -> Vec<u8> {
    ordinal.to_be_bytes().to_vec()
}

fn escape_json_pointer(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

#[cfg(test)]
mod tests {
    use crate::io::tests::{MemoryBlockSink, MemoryDirectory};

    use super::*;

    fn definition() -> TypedJsonDefinition {
        TypedJsonDefinition {
            fields: vec![
                TypedField {
                    name: "status".into(),
                    json_pointer: "/status".into(),
                },
                TypedField {
                    name: "amount".into(),
                    json_pointer: "/amount".into(),
                },
            ],
        }
    }

    fn selected(status: Vec<ScalarValue>, amount: f64) -> SelectedScalarFields {
        BTreeMap::from([
            ("status".into(), status),
            ("amount".into(), vec![ScalarValue::Number(amount)]),
        ])
    }

    fn upsert(
        path: &str,
        version: u64,
        status: &str,
        amount: f64,
    ) -> IndexMutation<TypedJsonDocument> {
        IndexMutation::Upsert(TypedJsonDocument {
            document: DocumentRef {
                path: path.into(),
                version,
            },
            fields: selected(vec![ScalarValue::String(status.into())], amount),
        })
    }

    fn upsert_fields(
        path: &str,
        version: u64,
        fields: SelectedScalarFields,
    ) -> IndexMutation<TypedJsonDocument> {
        IndexMutation::Upsert(TypedJsonDocument {
            document: DocumentRef {
                path: path.into(),
                version,
            },
            fields,
        })
    }

    async fn build_run(
        mutations: impl IntoIterator<Item = IndexMutation<TypedJsonDocument>>,
        level: u8,
        target: usize,
    ) -> (MemoryBlockSink, SealedRun) {
        let mut builder = TypedJsonSegmentBuilder::new(
            definition(),
            SegmentBuildOptions::for_level(128 * 1024, level).unwrap(),
        )
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

    fn exists_query() -> TypedQuery {
        TypedQuery {
            predicates: vec![TypedPredicate::Exists {
                field: "status".into(),
            }],
            order: vec![TypedOrder {
                field: "amount".into(),
                descending: false,
            }],
            limit: 10,
        }
    }

    #[tokio::test]
    async fn update_delete_and_streaming_compaction_are_equivalent() {
        let (old_sink, old_run) = build_run(
            [
                upsert("/a", 1, "old", 1.0),
                upsert("/b", 1, "old", 2.0),
                upsert("/c", 1, "kept", 4.0),
            ],
            0,
            96,
        )
        .await;
        let old = directory(&old_sink, old_run);
        let (new_sink, new_run) = build_run(
            [
                upsert("/a", 2, "new", 3.0),
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
        let expected = TypedJsonEngine::query(&runs, &definition(), &exists_query())
            .await
            .unwrap();
        assert_eq!(
            expected
                .iter()
                .map(|hit| (hit.document.path.as_str(), hit.document.version))
                .collect::<Vec<_>>(),
            [("/a", 2), ("/c", 1)]
        );
        let cursor = TypedQueryCursor::from_hit(&expected[0], &exists_query().order);
        assert_eq!(
            TypedJsonEngine::query_after(&runs, &definition(), &exists_query(), Some(&cursor),)
                .await
                .unwrap(),
            expected[1..]
        );

        let mut merged_sink = MemoryBlockSink::default();
        let merged = merge_typed(&runs, IndexKind::TypedJson, 1, 96, &mut merged_sink)
            .await
            .unwrap();
        let merged = [directory(&merged_sink, merged)];
        assert_eq!(
            TypedJsonEngine::query(&merged, &definition(), &exists_query())
                .await
                .unwrap(),
            expected
        );
    }

    #[tokio::test]
    async fn typed_predicates_and_array_dedup_are_canonical() {
        let (sink, run) = build_run(
            [
                upsert_fields(
                    "/a",
                    1,
                    selected(
                        vec![
                            ScalarValue::String("active".into()),
                            ScalarValue::String("active".into()),
                        ],
                        10.0,
                    ),
                ),
                upsert("/b", 1, "inactive", 20.0),
            ],
            1,
            128,
        )
        .await;
        let runs = [directory(&sink, run)];
        let hits = TypedJsonEngine::query(
            &runs,
            &definition(),
            &TypedQuery {
                predicates: vec![
                    TypedPredicate::Prefix {
                        field: "status".into(),
                        prefix: "act".into(),
                    },
                    TypedPredicate::LessThan {
                        field: "amount".into(),
                        value: ScalarValue::Number(15.0),
                    },
                ],
                order: Vec::new(),
                limit: 10,
            },
        )
        .await
        .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].document.path, "/a");
        assert_eq!(hits[0].fields["status"].len(), 1);
    }

    #[tokio::test]
    async fn deterministic_multi_block_output_rejects_corrupt_root() {
        let mutations = (0..100)
            .map(|index| upsert(&format!("/common/{index:04}"), 1, "active", index as f64))
            .collect::<Vec<_>>();
        let (first_sink, first) = build_run(mutations.clone(), 1, 128).await;
        let (second_sink, second) = build_run(mutations, 1, 128).await;
        assert_eq!(first.descriptor().hash, second.descriptor().hash);
        assert_eq!(first_sink.len(), second_sink.len());
        assert!(first_sink.len() > 6);

        let (_, mut root) = first.into_root().into_parts();
        *root.last_mut().unwrap() ^= 1;
        let corrupt = MemoryDirectory::new([(crate::run::RUN_ROOT_FILE.into(), root)]);
        assert!(matches!(
            TypedJsonEngine::query(&[corrupt], &definition(), &exists_query()).await,
            Err(IndexError::Integrity)
        ));
    }

    #[tokio::test]
    async fn metadata_filter_is_deterministic_and_uses_the_public_query_path() {
        let build = || {
            let mut builder = MetadataSegmentBuilder::new(
                definition(),
                SegmentBuildOptions::for_level(64 * 1024, 1).unwrap(),
            )
            .unwrap();
            for index in 0..80 {
                let fields = selected(
                    vec![ScalarValue::String(
                        if index % 2 == 0 { "active" } else { "inactive" }.into(),
                    )],
                    index as f64,
                );
                builder
                    .try_push(IndexMutation::Upsert(MetadataDocument {
                        document: DocumentRef {
                            path: format!("/metadata/{index:04}"),
                            version: 1,
                        },
                        fields,
                    }))
                    .unwrap();
            }
            builder
        };
        let mut first_sink = MemoryBlockSink::default();
        let first = build()
            .seal_with_target(&mut first_sink, 128)
            .await
            .unwrap()
            .unwrap();
        let mut second_sink = MemoryBlockSink::default();
        let second = build()
            .seal_with_target(&mut second_sink, 128)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.descriptor().hash, second.descriptor().hash);
        assert_eq!(first_sink.len(), second_sink.len());
        assert!(first_sink.len() > 6);

        let hits = MetadataFilterEngine::query(
            &[directory(&first_sink, first)],
            &definition(),
            &TypedQuery {
                predicates: vec![TypedPredicate::Equal {
                    field: "status".into(),
                    value: ScalarValue::String("active".into()),
                }],
                order: Vec::new(),
                limit: 100,
            },
        )
        .await
        .unwrap();
        assert_eq!(hits.len(), 40);
        assert!(
            hits.iter()
                .all(|hit| hit.fields["status"] == [ScalarValue::String("active".into())])
        );
    }

    #[test]
    fn builder_returns_unadmitted_mutation_without_crossing_cap() {
        let options = SegmentBuildOptions::new(1_024).unwrap();
        let mut builder = TypedJsonSegmentBuilder::new(definition(), options).unwrap();
        assert!(matches!(
            builder.try_push(upsert("/a", 1, "active", 1.0)).unwrap(),
            SegmentPush::Accepted
        ));
        assert!(matches!(
            builder.try_push(upsert("/b", 1, "active", 2.0)).unwrap(),
            SegmentPush::Full(_)
        ));
        assert!(builder.resident_bytes() <= options.max_resident_bytes);
    }

    include!("typed_json/writer_resident_tests.rs");
    include!("typed_json/query_bounds_tests.rs");
    include!("typed_json/semantics_tests.rs");
    include!("typed_json/unsigned_metadata_tests.rs");
    include!("typed_json/parallel_compaction_tests.rs");
}
