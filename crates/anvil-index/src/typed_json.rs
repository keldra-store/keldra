//! Bounded typed-JSON and fixed object-metadata runs.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};

use crate::codec::{Decoder, Encoder, encode_component};
use crate::routed::{
    ROUTED_ROW_RESIDENT_OVERHEAD_BYTES, RoutedComponentWriter, RoutedCursor, RoutedRow,
};
use crate::run::{
    ComponentTree, LeafCursor, RunStatistics, RunView, find_leaf, open_run, seal_run_root,
};
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

pub(crate) const ROWS_TAG: u8 = 20;
const KEYS_TAG: u8 = 21;

#[path = "typed_json/query.rs"]
mod query;
use query::query_typed;

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
}

#[derive(Clone, Debug)]
struct TypedPayload {
    fields: BTreeMap<String, Vec<ScalarValue>>,
}

impl TypedPayload {
    fn canonical(fields: SelectedScalarFields) -> Self {
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

    fn key_rows(&self, ordinal: u64) -> Result<Vec<RoutedRow>, IndexError> {
        let mut rows = Vec::new();
        for (field, values) in &self.fields {
            for value in values {
                let position = u32::try_from(rows.len()).map_err(|_| IndexError::OffsetOverflow)?;
                rows.push(RoutedRow::new(
                    typed_primary(field, value)?,
                    ordinal,
                    position,
                )?);
            }
        }
        Ok(rows)
    }

    fn matches_key(&self, primary: &[u8], position: u32) -> Result<bool, IndexError> {
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

#[derive(Debug)]
struct TypedRow {
    ordinal: u64,
    payload: TypedPayload,
}

struct TypedComponentWriter {
    kind: IndexKind,
    level: u8,
    target_bytes: usize,
    estimated_bytes: usize,
    rows: Vec<TypedRow>,
    tree: crate::run::RoutingTreeBuilder,
}

impl TypedComponentWriter {
    fn new(kind: IndexKind, level: u8, target_bytes: usize) -> Self {
        Self {
            kind,
            level,
            target_bytes: target_bytes.max(256),
            estimated_bytes: 0,
            rows: Vec::new(),
            tree: crate::run::RoutingTreeBuilder::new(kind, ROWS_TAG),
        }
    }

    async fn push<S: IndexBlockSink>(
        &mut self,
        row: TypedRow,
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
        let body = encode_typed_rows(&rows, codec)?;
        let bytes = encode_component(self.kind, ROWS_TAG, codec, body)?;
        self.tree
            .emit_leaf(
                crate::GeneratedBlock::new(
                    self.kind,
                    ROWS_TAG,
                    codec,
                    0,
                    ordinal_key(rows.first().unwrap().ordinal),
                    ordinal_key(rows.last().unwrap().ordinal),
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

fn encode_typed_rows(rows: &[TypedRow], codec: ComponentCodec) -> Result<Vec<u8>, IndexError> {
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

fn decode_typed_rows(bytes: &[u8], codec: ComponentCodec) -> Result<Vec<TypedRow>, IndexError> {
    let mut decoder = Decoder::new(bytes);
    let count = decoder.u32()? as usize;
    decoder.guard_count::<TypedRow>(count, 4)?;
    let ordinals = if codec == ComponentCodec::PrefixEliasFano {
        let budget = decoder.budget();
        let sequence = decode_elias_fano_with_budget(decoder.bytes()?, budget)?;
        if sequence.len() != count {
            return Err(IndexError::InvalidFormat("typed ordinal count"));
        }
        Some(sequence)
    } else if codec == ComponentCodec::FixedRows {
        None
    } else {
        return Err(IndexError::InvalidFormat("typed block codec"));
    };
    let mut rows = Vec::with_capacity(count);
    for index in 0..count {
        rows.push(TypedRow {
            ordinal: match &ordinals {
                Some(values) => values.get(index)?,
                None => decoder.u64()?,
            },
            payload: TypedPayload::decode(&mut decoder)?,
        });
    }
    decoder.finish()?;
    if rows.is_empty()
        || rows
            .windows(2)
            .any(|pair| pair[0].ordinal >= pair[1].ordinal)
    {
        return Err(IndexError::InvalidFormat("typed ordinal order"));
    }
    Ok(rows)
}

async fn read_typed_block<D: IndexDirectoryRead>(
    directory: &D,
    descriptor: &crate::BlockDescriptor,
) -> Result<Vec<TypedRow>, IndexError> {
    let block = crate::run::read_leaf(directory, descriptor).await?;
    let rows = decode_typed_rows(block.body(), descriptor.codec)?;
    if rows.first().map(|row| ordinal_key(row.ordinal)) != Some(descriptor.minimum_key.clone())
        || rows.last().map(|row| ordinal_key(row.ordinal)) != Some(descriptor.maximum_key.clone())
        || rows.len() as u64 != descriptor.element_count
    {
        return Err(IndexError::InvalidFormat("typed block descriptor"));
    }
    Ok(rows)
}

struct TypedCursor<'a, D> {
    directory: &'a D,
    leaves: LeafCursor<'a, D>,
    rows: VecDeque<TypedRow>,
}

impl<'a, D: IndexDirectoryRead> TypedCursor<'a, D> {
    fn new(directory: &'a D, root: crate::BlockDescriptor) -> Self {
        Self {
            directory,
            leaves: LeafCursor::new(directory, root),
            rows: VecDeque::new(),
        }
    }

    async fn next(&mut self) -> Result<Option<TypedRow>, IndexError> {
        loop {
            if let Some(row) = self.rows.pop_front() {
                return Ok(Some(row));
            }
            let Some(descriptor) = self.leaves.next().await? else {
                return Ok(None);
            };
            self.rows = read_typed_block(self.directory, &descriptor).await?.into();
        }
    }
}

async fn typed_row<D: IndexDirectoryRead>(
    directory: &D,
    view: &RunView,
    ordinal: u64,
) -> Result<TypedRow, IndexError> {
    let root = view
        .component_optional(ROWS_TAG)
        .ok_or(IndexError::InvalidFormat("missing typed component"))?;
    let descriptor = find_leaf(directory, root, &ordinal_key(ordinal))
        .await?
        .ok_or(IndexError::InvalidFormat("missing typed ordinal"))?;
    let rows = read_typed_block(directory, &descriptor).await?;
    let index = rows
        .binary_search_by_key(&ordinal, |row| row.ordinal)
        .map_err(|_| IndexError::InvalidFormat("missing typed ordinal"))?;
    Ok(rows.into_iter().nth(index).unwrap())
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
        let mut writer = RoutedComponentWriter::new(kind, KEYS_TAG, level, target_block_bytes);
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
            let source_document =
                document_by_ordinal(&runs[winner_run], &views[winner_run], old_ordinal).await?;
            if source_document != winner.document {
                return Err(IndexError::InvalidFormat("typed document mismatch"));
            }
            let payload = typed_row(&runs[winner_run], &views[winner_run], old_ordinal)
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
    let path_tree = paths.finish(sink).await?;
    let path_root = path_tree.root.clone();
    let mut components = vec![path_tree];
    if live > 0 {
        components.push(documents.finish(sink).await?);
        components.push(typed.finish(sink).await?);
        if let Some(tree) = merge_typed_keys(
            runs,
            &views,
            kind,
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
            view.component_optional(KEYS_TAG)
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
    let mut writer = RoutedComponentWriter::new(kind, KEYS_TAG, output_level, target_block_bytes);
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

fn query_driver_prefix(predicates: &[TypedPredicate]) -> Result<Option<Vec<u8>>, IndexError> {
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
    let prefix = match predicate {
        TypedPredicate::Equal { field, value } => typed_primary(field, value).map_err(|_| {
            IndexError::InvalidQuery("typed equality key exceeds the format limit".into())
        })?,
        TypedPredicate::Prefix { field, prefix } => typed_string_prefix(field, prefix)?,
        predicate => typed_field_prefix(predicate.field())?,
    };
    Ok(Some(prefix))
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

fn validate_selected_fields(
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
        ScalarValue::Number(_) => 9,
        ScalarValue::String(value) => value.len().saturating_add(5),
    }
}

fn estimate_selected_fields(fields: &SelectedScalarFields) -> usize {
    let mut source = 0usize;
    let mut derived = 0usize;
    for (field, values) in fields {
        source = source.saturating_add(field.len()).saturating_add(64);
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

fn preflight_typed_row(fields: &SelectedScalarFields) -> Result<(), IndexError> {
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
        ScalarValue::Null | ScalarValue::Boolean(_) | ScalarValue::Number(_) => 32,
    }
}

fn sortable_scalar_bytes(value: &ScalarValue) -> usize {
    match value {
        ScalarValue::Null => 1,
        ScalarValue::Boolean(_) => 2,
        ScalarValue::Number(_) => 9,
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
            SegmentBuildOptions::for_level(64 * 1024, level).unwrap(),
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
        let options = SegmentBuildOptions::new(500).unwrap();
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

    #[test]
    fn metadata_builder_rejects_a_source_larger_than_its_budget() {
        let mut builder =
            MetadataSegmentBuilder::new(definition(), SegmentBuildOptions::new(256).unwrap())
                .unwrap();
        let fields = selected(vec![ScalarValue::String("x".repeat(1024))], 1.0);
        assert!(matches!(
            builder.try_push(IndexMutation::Upsert(MetadataDocument {
                document: DocumentRef {
                    path: "/oversized".into(),
                    version: 1,
                },
                fields,
            })),
            Err(IndexError::ResourceLimit { .. })
        ));
    }

    #[test]
    fn corrupt_typed_row_count_is_rejected_before_allocation() {
        assert_eq!(
            decode_typed_rows(&u32::MAX.to_le_bytes(), ComponentCodec::FixedRows).unwrap_err(),
            IndexError::InvalidFormat("index component element count")
        );
    }

    #[test]
    fn typed_row_too_large_for_one_block_fails_before_admission() {
        let values = (0..1_100)
            .map(|index| ScalarValue::String(format!("{index:04}{}", "x".repeat(3_996))))
            .collect();
        let fields = BTreeMap::from([("status".into(), values)]);
        assert!(matches!(
            preflight_typed_row(&fields),
            Err(IndexError::ResourceLimit { .. })
        ));
    }

    include!("typed_json/query_bounds_tests.rs");
}
