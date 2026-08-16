//! Bounded, selective JSON projection for streaming index construction.
//!
//! The parser walks the source once and only retains values selected by the
//! index definition. Unrelated subtrees are validated and discarded through a
//! fixed-size input buffer, so their size does not become index-builder memory.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{ErrorKind, Read};

use anvil_index::IndexError;
use anvil_index::v4::ScalarValue;

pub(crate) type SelectedScalarFields = BTreeMap<String, SelectedScalarField>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SelectedScalarField {
    pub values: Vec<ScalarValue>,
    pub from_array: bool,
}

const INPUT_BUFFER_BYTES: usize = 8 * 1024;
const MAX_JSON_DEPTH: usize = 128;
const MAP_ENTRY_FLOOR_BYTES: usize = 64;
const SCALAR_VALUE_BYTES: usize = 32;
const VECTOR_FLOOR_BYTES: usize = 24;
const TARGET_DESCRIPTOR_FLOOR_BYTES: usize = 64;
const POINTER_BYTE_COPIES: usize = 2;
const POINTER_TOKEN_FLOOR_BYTES: usize = 24;
const MAX_CAPTURED_NUMBER_BYTES: usize = 128;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ProjectionSelection {
    Scalars(Vec<(String, String)>),
    Strings(Vec<(String, String)>),
    Vector {
        pointer: String,
        dimensions: usize,
        normalize: bool,
    },
    Hybrid {
        strings: Vec<(String, String)>,
        vector_pointer: String,
        dimensions: usize,
        normalize: bool,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ProjectedJson {
    Scalars(SelectedScalarFields),
    Strings(BTreeMap<String, String>),
    Vector(Vec<f32>),
    Hybrid {
        strings: BTreeMap<String, String>,
        vector: Vec<f32>,
    },
}

/// Returns the fixed bytes a projection needs before it can retain source
/// values. Calling this before parsing prevents a definition with oversized
/// selected names or vector dimensions from causing an allocation first.
pub(crate) fn projection_floor_bytes(selection: &ProjectionSelection) -> Result<usize, IndexError> {
    let mut names = BTreeSet::new();
    match selection {
        ProjectionSelection::Scalars(fields) | ProjectionSelection::Strings(fields) => {
            validate_fields(fields, &mut names)
        }
        ProjectionSelection::Vector {
            pointer,
            dimensions,
            ..
        } => checked_add(pointer_floor(pointer)?, vector_floor(*dimensions)?),
        ProjectionSelection::Hybrid {
            strings,
            vector_pointer,
            dimensions,
            ..
        } => checked_add(
            validate_fields(strings, &mut names)?,
            checked_add(pointer_floor(vector_pointer)?, vector_floor(*dimensions)?)?,
        ),
    }
}

/// Projects the selected JSON values without materializing the source
/// document. Invalid JSON and documents that do not match the requested shape
/// are skipped. A selected value that exceeds its construction allowance fails
/// closed with [`IndexError::ResourceLimit`].
pub(crate) fn project_json(
    source: &mut dyn Read,
    selection: &ProjectionSelection,
    max_selected_bytes: usize,
) -> Result<Option<ProjectedJson>, IndexError> {
    let floor = projection_floor_bytes(selection)?;
    if floor > max_selected_bytes {
        return Err(IndexError::ResourceLimit {
            needed: floor,
            limit: max_selected_bytes,
        });
    }

    let targets = compile_targets(selection)?;
    let mut slots = (0..targets.len())
        .map(|_| SelectedValue::Missing)
        .collect::<Vec<_>>();
    let candidates = (0..targets.len()).collect::<Vec<_>>();
    let mut budget = SelectedBudget {
        used: floor,
        limit: max_selected_bytes,
    };
    let mut parser = ProjectionParser {
        input: Input::new(source),
        targets: &targets,
        slots: &mut slots,
        budget: &mut budget,
    };

    let parsed = parser
        .skip_whitespace()
        .and_then(|()| parser.parse_value(&candidates, 0, &[]))
        .and_then(|()| parser.skip_whitespace())
        .and_then(|()| {
            if parser.input.peek()?.is_none() {
                Ok(())
            } else {
                Err(ProjectionFailure::Malformed)
            }
        });
    match parsed {
        Ok(()) => finish_projection(selection, &targets, slots),
        Err(ProjectionFailure::Malformed) => Ok(None),
        Err(ProjectionFailure::Index(error)) => Err(error),
    }
}

fn validate_fields<'a>(
    fields: &'a [(String, String)],
    names: &mut BTreeSet<&'a str>,
) -> Result<usize, IndexError> {
    if fields.is_empty() {
        return Err(IndexError::InvalidDefinition(
            "JSON projection needs at least one field".into(),
        ));
    }
    let mut floor = 0usize;
    for (name, pointer) in fields {
        if name.is_empty() || name.contains('\0') || !names.insert(name.as_str()) {
            return Err(IndexError::InvalidDefinition(format!(
                "invalid or duplicate JSON projection field `{name}`"
            )));
        }
        floor = checked_add(
            floor,
            checked_add(
                checked_add(MAP_ENTRY_FLOOR_BYTES, name.len())?,
                pointer_floor(pointer)?,
            )?,
        )?;
    }
    Ok(floor)
}

fn vector_floor(dimensions: usize) -> Result<usize, IndexError> {
    if dimensions == 0 {
        return Err(IndexError::InvalidDefinition(
            "vector dimensions must be greater than zero".into(),
        ));
    }
    checked_add(
        VECTOR_FLOOR_BYTES,
        dimensions
            .checked_mul(std::mem::size_of::<f32>())
            .ok_or_else(|| {
                IndexError::InvalidDefinition("vector dimensions exceed this platform".into())
            })?,
    )
}

fn checked_add(left: usize, right: usize) -> Result<usize, IndexError> {
    left.checked_add(right).ok_or_else(|| {
        IndexError::InvalidDefinition("JSON projection size exceeds this platform".into())
    })
}

fn validate_pointer(pointer: &str) -> Result<(), IndexError> {
    if pointer.is_empty() {
        return Ok(());
    }
    let Some(encoded) = pointer.strip_prefix('/') else {
        return Err(IndexError::InvalidDefinition(format!(
            "JSON pointer `{pointer}` must be empty or start with `/`"
        )));
    };
    let bytes = encoded.as_bytes();
    let mut position = 0usize;
    while position < bytes.len() {
        if bytes[position] != b'~' {
            position += 1;
            continue;
        }
        if !matches!(bytes.get(position + 1), Some(b'0' | b'1')) {
            return Err(IndexError::InvalidDefinition(format!(
                "JSON pointer `{pointer}` contains an invalid escape"
            )));
        }
        position += 2;
    }
    Ok(())
}

fn pointer_floor(pointer: &str) -> Result<usize, IndexError> {
    validate_pointer(pointer)?;
    let token_count = if pointer.is_empty() {
        0
    } else {
        pointer[1..].split('/').count()
    };
    let duplicated_bytes = pointer
        .len()
        .checked_mul(POINTER_BYTE_COPIES)
        .ok_or_else(|| {
            IndexError::InvalidDefinition("JSON pointer size exceeds this platform".into())
        })?;
    let tokens = token_count
        .checked_mul(POINTER_TOKEN_FLOOR_BYTES)
        .ok_or_else(|| {
            IndexError::InvalidDefinition("JSON pointer token count exceeds this platform".into())
        })?;
    checked_add(
        TARGET_DESCRIPTOR_FLOOR_BYTES,
        checked_add(duplicated_bytes, tokens)?,
    )
}

#[derive(Clone, Debug)]
struct ProjectionTarget {
    name: Option<String>,
    tokens: Vec<String>,
    kind: TargetKind,
}

#[derive(Clone, Copy, Debug)]
enum TargetKind {
    Scalar,
    String,
    Vector { dimensions: usize, normalize: bool },
}

fn compile_targets(selection: &ProjectionSelection) -> Result<Vec<ProjectionTarget>, IndexError> {
    match selection {
        ProjectionSelection::Scalars(fields) => fields
            .iter()
            .map(|(name, pointer)| target(Some(name), pointer, TargetKind::Scalar))
            .collect(),
        ProjectionSelection::Strings(fields) => fields
            .iter()
            .map(|(name, pointer)| target(Some(name), pointer, TargetKind::String))
            .collect(),
        ProjectionSelection::Vector {
            pointer,
            dimensions,
            normalize,
        } => Ok(vec![target(
            None,
            pointer,
            TargetKind::Vector {
                dimensions: *dimensions,
                normalize: *normalize,
            },
        )?]),
        ProjectionSelection::Hybrid {
            strings,
            vector_pointer,
            dimensions,
            normalize,
        } => {
            let mut targets = strings
                .iter()
                .map(|(name, pointer)| target(Some(name), pointer, TargetKind::String))
                .collect::<Result<Vec<_>, _>>()?;
            targets.push(target(
                None,
                vector_pointer,
                TargetKind::Vector {
                    dimensions: *dimensions,
                    normalize: *normalize,
                },
            )?);
            Ok(targets)
        }
    }
}

fn target(
    name: Option<&String>,
    pointer: &str,
    kind: TargetKind,
) -> Result<ProjectionTarget, IndexError> {
    Ok(ProjectionTarget {
        name: name.cloned(),
        tokens: decode_pointer(pointer)?,
        kind,
    })
}

fn decode_pointer(pointer: &str) -> Result<Vec<String>, IndexError> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    let Some(encoded) = pointer.strip_prefix('/') else {
        return Err(IndexError::InvalidDefinition(format!(
            "JSON pointer `{pointer}` must be empty or start with `/`"
        )));
    };
    encoded
        .split('/')
        .map(|token| {
            let mut decoded = String::with_capacity(token.len());
            let mut bytes = token.as_bytes().iter().copied();
            while let Some(byte) = bytes.next() {
                if byte != b'~' {
                    decoded.push(byte as char);
                    continue;
                }
                match bytes.next() {
                    Some(b'0') => decoded.push('~'),
                    Some(b'1') => decoded.push('/'),
                    _ => {
                        return Err(IndexError::InvalidDefinition(format!(
                            "JSON pointer `{pointer}` contains an invalid escape"
                        )));
                    }
                }
            }
            // Bytes above 0x7f were copied one by one. Rebuild from the encoded
            // token when it contains UTF-8 so non-ASCII field names remain exact.
            if token.is_ascii() {
                Ok(decoded)
            } else {
                decode_pointer_token_utf8(token, pointer)
            }
        })
        .collect()
}

fn decode_pointer_token_utf8(token: &str, pointer: &str) -> Result<String, IndexError> {
    let mut decoded = Vec::with_capacity(token.len());
    let bytes = token.as_bytes();
    let mut position = 0usize;
    while position < bytes.len() {
        if bytes[position] != b'~' {
            decoded.push(bytes[position]);
            position += 1;
            continue;
        }
        let replacement = match bytes.get(position + 1) {
            Some(b'0') => b'~',
            Some(b'1') => b'/',
            _ => {
                return Err(IndexError::InvalidDefinition(format!(
                    "JSON pointer `{pointer}` contains an invalid escape"
                )));
            }
        };
        decoded.push(replacement);
        position += 2;
    }
    String::from_utf8(decoded)
        .map_err(|_| IndexError::InvalidDefinition("JSON pointer is not UTF-8".into()))
}

#[derive(Debug)]
enum SelectedValue {
    Missing,
    Invalid,
    Scalars(SelectedScalarField),
    String(String),
    Vector(Vec<f32>),
}

fn finish_projection(
    selection: &ProjectionSelection,
    targets: &[ProjectionTarget],
    mut slots: Vec<SelectedValue>,
) -> Result<Option<ProjectedJson>, IndexError> {
    match selection {
        ProjectionSelection::Scalars(_) => {
            let mut fields = BTreeMap::new();
            for (target, slot) in targets.iter().zip(slots.drain(..)) {
                if let SelectedValue::Scalars(field) = slot {
                    if field.from_array || !field.values.is_empty() {
                        fields.insert(target.name.clone().expect("scalar target name"), field);
                    }
                }
            }
            // A valid in-scope Typed JSON object with no selected fields still
            // belongs to the index. Missing remains distinct from explicit
            // null for predicate-free, NOT Exists, and ordered queries.
            Ok(Some(ProjectedJson::Scalars(fields)))
        }
        ProjectionSelection::Strings(_) => {
            let strings = collect_strings(targets, slots.into_iter());
            Ok((!strings.is_empty()).then_some(ProjectedJson::Strings(strings)))
        }
        ProjectionSelection::Vector { .. } => {
            Ok(finish_vector(&targets[0], slots.remove(0)).map(ProjectedJson::Vector))
        }
        ProjectionSelection::Hybrid { strings, .. } => {
            let vector_index = strings.len();
            let vector = finish_vector(&targets[vector_index], slots.remove(vector_index));
            let text = collect_strings(targets, slots.into_iter());
            Ok(match (text.is_empty(), vector) {
                (false, Some(vector)) => Some(ProjectedJson::Hybrid {
                    strings: text,
                    vector,
                }),
                _ => None,
            })
        }
    }
}

fn collect_strings(
    targets: &[ProjectionTarget],
    slots: impl Iterator<Item = SelectedValue>,
) -> BTreeMap<String, String> {
    targets
        .iter()
        .zip(slots)
        .filter_map(|(target, slot)| match slot {
            SelectedValue::String(value) => {
                Some((target.name.clone().expect("string target name"), value))
            }
            _ => None,
        })
        .collect()
}

fn finish_vector(target: &ProjectionTarget, slot: SelectedValue) -> Option<Vec<f32>> {
    let TargetKind::Vector {
        dimensions,
        normalize,
    } = target.kind
    else {
        return None;
    };
    let SelectedValue::Vector(mut values) = slot else {
        return None;
    };
    if values.len() != dimensions || values.iter().any(|value| !value.is_finite()) {
        return None;
    }
    if normalize {
        let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
        if norm == 0.0 || !norm.is_finite() {
            return None;
        }
        values.iter_mut().for_each(|value| *value /= norm);
    }
    Some(values)
}

struct SelectedBudget {
    used: usize,
    limit: usize,
}

impl SelectedBudget {
    fn reserve(&mut self, bytes: usize) -> ProjectionResult<()> {
        let needed = self.used.checked_add(bytes).unwrap_or(usize::MAX);
        if needed > self.limit {
            return Err(ProjectionFailure::Index(IndexError::ResourceLimit {
                needed,
                limit: self.limit,
            }));
        }
        self.used = needed;
        Ok(())
    }
}

struct ProjectionParser<'a, 'reader> {
    input: Input<'reader>,
    targets: &'a [ProjectionTarget],
    slots: &'a mut [SelectedValue],
    budget: &'a mut SelectedBudget,
}

impl ProjectionParser<'_, '_> {
    fn parse_value(
        &mut self,
        candidates: &[usize],
        depth: usize,
        array_collectors: &[usize],
    ) -> ProjectionResult<()> {
        if depth > MAX_JSON_DEPTH {
            return Err(ProjectionFailure::Malformed);
        }
        self.skip_whitespace()?;
        match self.input.peek()? {
            Some(b'{') => {
                self.invalidate(array_collectors);
                self.invalidate_exact(candidates, depth);
                self.parse_object(candidates, depth)
            }
            Some(b'[') => {
                self.invalidate(array_collectors);
                self.parse_array(candidates, depth)
            }
            Some(b'"') => self.parse_string_value(candidates, depth, array_collectors),
            Some(b't') => {
                self.input.literal(b"true")?;
                self.project_scalar(
                    candidates,
                    depth,
                    array_collectors,
                    ScalarValue::Boolean(true),
                )
            }
            Some(b'f') => {
                self.input.literal(b"false")?;
                self.project_scalar(
                    candidates,
                    depth,
                    array_collectors,
                    ScalarValue::Boolean(false),
                )
            }
            Some(b'n') => {
                self.input.literal(b"null")?;
                self.project_scalar(candidates, depth, array_collectors, ScalarValue::Null)
            }
            Some(b'-' | b'0'..=b'9') => {
                let capture = self.has_numeric_consumer(candidates, depth, array_collectors);
                let number = self.input.number(capture)?;
                match number {
                    Some(number) => {
                        self.project_scalar(candidates, depth, array_collectors, number)
                    }
                    _ => {
                        self.invalidate_exact(candidates, depth);
                        self.invalidate(array_collectors);
                        Ok(())
                    }
                }
            }
            _ => Err(ProjectionFailure::Malformed),
        }
    }

    fn parse_object(&mut self, candidates: &[usize], depth: usize) -> ProjectionResult<()> {
        self.input.expect(b'{')?;
        self.skip_whitespace()?;
        if self.input.take_if(b'}')? {
            return Ok(());
        }

        let max_key_bytes = candidates
            .iter()
            .filter_map(|index| self.targets[*index].tokens.get(depth))
            .map(String::len)
            .max()
            .unwrap_or(0);
        loop {
            let key = self.input.key(max_key_bytes)?;
            self.skip_whitespace()?;
            self.input.expect(b':')?;
            let children = if key.overflowed {
                Vec::new()
            } else {
                candidates
                    .iter()
                    .copied()
                    .filter(|index| {
                        self.targets[*index]
                            .tokens
                            .get(depth)
                            .is_some_and(|token| token.as_bytes() == key.bytes)
                    })
                    .collect::<Vec<_>>()
            };
            self.reset(&children);
            self.parse_value(&children, depth + 1, &[])?;
            self.skip_whitespace()?;
            if self.input.take_if(b'}')? {
                return Ok(());
            }
            self.input.expect(b',')?;
            self.skip_whitespace()?;
        }
    }

    fn parse_array(&mut self, candidates: &[usize], depth: usize) -> ProjectionResult<()> {
        self.input.expect(b'[')?;
        let exact = self.exact(candidates, depth);
        let mut collectors = Vec::new();
        for index in exact {
            match self.targets[index].kind {
                TargetKind::Scalar => {
                    self.slots[index] = SelectedValue::Scalars(SelectedScalarField {
                        values: Vec::new(),
                        from_array: true,
                    });
                    collectors.push(index);
                }
                TargetKind::Vector { dimensions, .. } => {
                    let mut values = Vec::new();
                    values.try_reserve_exact(dimensions).map_err(|_| {
                        ProjectionFailure::Index(IndexError::ResourceLimit {
                            needed: usize::MAX,
                            limit: self.budget.limit,
                        })
                    })?;
                    self.slots[index] = SelectedValue::Vector(values);
                    collectors.push(index);
                }
                TargetKind::String => self.slots[index] = SelectedValue::Invalid,
            }
        }

        self.skip_whitespace()?;
        if self.input.take_if(b']')? {
            return Ok(());
        }
        let mut element = 0usize;
        loop {
            let children = candidates
                .iter()
                .copied()
                .filter(|index| {
                    self.targets[*index]
                        .tokens
                        .get(depth)
                        .is_some_and(|token| array_index_matches(token, element))
                })
                .collect::<Vec<_>>();
            self.parse_value(&children, depth + 1, &collectors)?;
            element = element.checked_add(1).ok_or(ProjectionFailure::Malformed)?;
            self.skip_whitespace()?;
            if self.input.take_if(b']')? {
                return Ok(());
            }
            self.input.expect(b',')?;
            self.skip_whitespace()?;
        }
    }

    fn parse_string_value(
        &mut self,
        candidates: &[usize],
        depth: usize,
        array_collectors: &[usize],
    ) -> ProjectionResult<()> {
        let exact = self.exact(candidates, depth);
        for index in &exact {
            if matches!(self.targets[*index].kind, TargetKind::Vector { .. }) {
                self.slots[*index] = SelectedValue::Invalid;
            }
        }
        for index in array_collectors {
            if matches!(self.targets[*index].kind, TargetKind::Vector { .. }) {
                self.slots[*index] = SelectedValue::Invalid;
            }
        }

        let scalar_targets = exact
            .iter()
            .chain(array_collectors)
            .copied()
            .filter(|index| {
                matches!(self.targets[*index].kind, TargetKind::Scalar)
                    && matches!(
                        self.slots[*index],
                        SelectedValue::Missing | SelectedValue::Scalars(_)
                    )
            })
            .collect::<Vec<_>>();
        let string_targets = exact
            .iter()
            .copied()
            .filter(|index| matches!(self.targets[*index].kind, TargetKind::String))
            .collect::<Vec<_>>();
        self.budget.reserve(
            scalar_targets
                .len()
                .checked_mul(SCALAR_VALUE_BYTES)
                .unwrap_or(usize::MAX),
        )?;
        let copies = scalar_targets
            .len()
            .checked_add(string_targets.len())
            .unwrap_or(usize::MAX);
        if copies == 0 {
            return self.input.string(|_| Ok(()));
        }
        let mut value = String::new();
        {
            let budget = &mut *self.budget;
            self.input.string(|bytes| {
                budget.reserve(bytes.len().checked_mul(copies).unwrap_or(usize::MAX))?;
                value.try_reserve_exact(bytes.len()).map_err(|_| {
                    ProjectionFailure::Index(IndexError::ResourceLimit {
                        needed: usize::MAX,
                        limit: budget.limit,
                    })
                })?;
                value.push_str(
                    std::str::from_utf8(bytes).map_err(|_| ProjectionFailure::Malformed)?,
                );
                Ok(())
            })?;
        }

        let mut consumers = scalar_targets
            .into_iter()
            .map(|index| (index, true))
            .chain(string_targets.into_iter().map(|index| (index, false)))
            .collect::<Vec<_>>();
        let mut source = Some(value);
        let consumer_count = consumers.len();
        for (position, (index, scalar)) in consumers.drain(..).enumerate() {
            let selected = if position + 1 == consumer_count {
                source.take().expect("last JSON string consumer")
            } else {
                source.as_ref().expect("JSON string source").clone()
            };
            if !scalar {
                self.slots[index] = SelectedValue::String(selected);
                continue;
            }
            match &mut self.slots[index] {
                SelectedValue::Missing => {
                    self.slots[index] = SelectedValue::Scalars(SelectedScalarField {
                        values: vec![ScalarValue::String(selected)],
                        from_array: false,
                    });
                }
                SelectedValue::Scalars(field) => {
                    field.values.try_reserve_exact(1).map_err(|_| {
                        ProjectionFailure::Index(IndexError::ResourceLimit {
                            needed: usize::MAX,
                            limit: self.budget.limit,
                        })
                    })?;
                    field.values.push(ScalarValue::String(selected));
                }
                SelectedValue::Invalid | SelectedValue::String(_) | SelectedValue::Vector(_) => {}
            }
        }
        Ok(())
    }

    fn project_scalar(
        &mut self,
        candidates: &[usize],
        depth: usize,
        array_collectors: &[usize],
        value: ScalarValue,
    ) -> ProjectionResult<()> {
        let exact = self.exact(candidates, depth);
        for index in &exact {
            if !matches!(self.targets[*index].kind, TargetKind::Scalar) {
                self.slots[*index] = SelectedValue::Invalid;
            }
        }
        for index in array_collectors {
            if matches!(self.targets[*index].kind, TargetKind::Vector { .. }) {
                let number = match &value {
                    ScalarValue::Number(bits) => f64::from_bits(*bits) as f32,
                    ScalarValue::Signed(value) => *value as f32,
                    ScalarValue::Unsigned(value) => *value as f32,
                    _ => {
                        self.slots[*index] = SelectedValue::Invalid;
                        continue;
                    }
                };
                if !number.is_finite() {
                    self.slots[*index] = SelectedValue::Invalid;
                    continue;
                }
                let dimensions = vector_dimensions(&self.targets[*index]);
                let full = match &mut self.slots[*index] {
                    SelectedValue::Vector(values) if values.len() < dimensions => {
                        values.push(number);
                        false
                    }
                    SelectedValue::Vector(_) => true,
                    _ => false,
                };
                if full {
                    self.slots[*index] = SelectedValue::Invalid;
                }
            }
        }

        let scalar_targets = exact
            .into_iter()
            .chain(array_collectors.iter().copied())
            .filter(|index| matches!(self.targets[*index].kind, TargetKind::Scalar))
            .collect::<Vec<_>>();
        self.budget.reserve(
            scalar_targets
                .len()
                .checked_mul(SCALAR_VALUE_BYTES)
                .unwrap_or(usize::MAX),
        )?;
        for index in scalar_targets {
            match &mut self.slots[index] {
                SelectedValue::Missing => {
                    self.slots[index] = SelectedValue::Scalars(SelectedScalarField {
                        values: vec![value.clone()],
                        from_array: false,
                    });
                }
                SelectedValue::Scalars(field) => {
                    field.values.try_reserve_exact(1).map_err(|_| {
                        ProjectionFailure::Index(IndexError::ResourceLimit {
                            needed: usize::MAX,
                            limit: self.budget.limit,
                        })
                    })?;
                    field.values.push(value.clone());
                }
                SelectedValue::Invalid | SelectedValue::String(_) | SelectedValue::Vector(_) => {}
            }
        }
        Ok(())
    }

    fn has_numeric_consumer(
        &self,
        candidates: &[usize],
        depth: usize,
        array_collectors: &[usize],
    ) -> bool {
        candidates
            .iter()
            .filter(|index| self.targets[**index].tokens.len() == depth)
            .chain(array_collectors.iter())
            .any(|index| {
                matches!(
                    self.targets[*index].kind,
                    TargetKind::Scalar | TargetKind::Vector { .. }
                )
            })
    }

    fn exact(&self, candidates: &[usize], depth: usize) -> Vec<usize> {
        candidates
            .iter()
            .copied()
            .filter(|index| self.targets[*index].tokens.len() == depth)
            .collect()
    }

    fn invalidate_exact(&mut self, candidates: &[usize], depth: usize) {
        for index in self.exact(candidates, depth) {
            self.slots[index] = SelectedValue::Invalid;
        }
    }

    fn invalidate(&mut self, indices: &[usize]) {
        for index in indices {
            self.slots[*index] = SelectedValue::Invalid;
        }
    }

    fn reset(&mut self, indices: &[usize]) {
        for index in indices {
            self.slots[*index] = SelectedValue::Missing;
        }
    }

    fn skip_whitespace(&mut self) -> ProjectionResult<()> {
        while matches!(self.input.peek()?, Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.input.next()?;
        }
        Ok(())
    }
}

fn vector_dimensions(target: &ProjectionTarget) -> usize {
    match target.kind {
        TargetKind::Vector { dimensions, .. } => dimensions,
        TargetKind::Scalar | TargetKind::String => 0,
    }
}

fn array_index_matches(token: &str, index: usize) -> bool {
    !token.is_empty()
        && token.bytes().all(|byte| byte.is_ascii_digit())
        && token.parse::<usize>().ok() == Some(index)
}

struct Input<'a> {
    reader: &'a mut dyn Read,
    bytes: [u8; INPUT_BUFFER_BYTES],
    start: usize,
    end: usize,
}

impl<'a> Input<'a> {
    fn new(reader: &'a mut dyn Read) -> Self {
        Self {
            reader,
            bytes: [0; INPUT_BUFFER_BYTES],
            start: 0,
            end: 0,
        }
    }

    fn fill(&mut self) -> ProjectionResult<()> {
        if self.start < self.end {
            return Ok(());
        }
        loop {
            match self.reader.read(&mut self.bytes) {
                Ok(read) => {
                    self.start = 0;
                    self.end = read;
                    return Ok(());
                }
                Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                Err(error) => {
                    return Err(ProjectionFailure::Index(IndexError::Io(error.to_string())));
                }
            }
        }
    }

    fn peek(&mut self) -> ProjectionResult<Option<u8>> {
        self.fill()?;
        Ok((self.start < self.end).then(|| self.bytes[self.start]))
    }

    fn next(&mut self) -> ProjectionResult<Option<u8>> {
        let byte = self.peek()?;
        if byte.is_some() {
            self.start += 1;
        }
        Ok(byte)
    }

    fn expect(&mut self, expected: u8) -> ProjectionResult<()> {
        if self.next()? == Some(expected) {
            Ok(())
        } else {
            Err(ProjectionFailure::Malformed)
        }
    }

    fn take_if(&mut self, expected: u8) -> ProjectionResult<bool> {
        if self.peek()? == Some(expected) {
            self.start += 1;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn literal(&mut self, literal: &[u8]) -> ProjectionResult<()> {
        for expected in literal {
            self.expect(*expected)?;
        }
        Ok(())
    }

    fn key(&mut self, max_bytes: usize) -> ProjectionResult<KeyBytes> {
        let mut key = KeyBytes {
            bytes: Vec::new(),
            overflowed: false,
        };
        self.string(|bytes| {
            if key.overflowed {
                return Ok(());
            }
            let Some(length) = key.bytes.len().checked_add(bytes.len()) else {
                key.overflowed = true;
                return Ok(());
            };
            if length > max_bytes {
                key.bytes.clear();
                key.overflowed = true;
                return Ok(());
            }
            key.bytes
                .try_reserve_exact(bytes.len())
                .map_err(|_| ProjectionFailure::Malformed)?;
            key.bytes.extend_from_slice(bytes);
            Ok(())
        })?;
        Ok(key)
    }

    fn string(
        &mut self,
        mut output: impl FnMut(&[u8]) -> ProjectionResult<()>,
    ) -> ProjectionResult<()> {
        self.expect(b'"')?;
        loop {
            self.fill()?;
            if self.start == self.end {
                return Err(ProjectionFailure::Malformed);
            }
            let run_start = self.start;
            let mut run_end = run_start;
            while run_end < self.end {
                match self.bytes[run_end] {
                    b'"' | b'\\' | 0x00..=0x1f | 0x80..=0xff => break,
                    _ => run_end += 1,
                }
            }
            if run_end > run_start {
                output(&self.bytes[run_start..run_end])?;
                self.start = run_end;
                continue;
            }
            let byte = self.next()?.ok_or(ProjectionFailure::Malformed)?;
            match byte {
                b'"' => return Ok(()),
                b'\\' => self.string_escape(&mut output)?,
                0x00..=0x1f => return Err(ProjectionFailure::Malformed),
                0x20..=0x7f => output(&[byte])?,
                _ => {
                    let width = utf8_width(byte).ok_or(ProjectionFailure::Malformed)?;
                    let mut encoded = [0u8; 4];
                    encoded[0] = byte;
                    for slot in encoded.iter_mut().take(width).skip(1) {
                        *slot = self.next()?.ok_or(ProjectionFailure::Malformed)?;
                        if *slot & 0xc0 != 0x80 {
                            return Err(ProjectionFailure::Malformed);
                        }
                    }
                    std::str::from_utf8(&encoded[..width])
                        .map_err(|_| ProjectionFailure::Malformed)?;
                    output(&encoded[..width])?;
                }
            }
        }
    }

    fn string_escape(
        &mut self,
        output: &mut impl FnMut(&[u8]) -> ProjectionResult<()>,
    ) -> ProjectionResult<()> {
        match self.next()?.ok_or(ProjectionFailure::Malformed)? {
            b'"' => output(b"\"")?,
            b'\\' => output(b"\\")?,
            b'/' => output(b"/")?,
            b'b' => output(&[8])?,
            b'f' => output(&[12])?,
            b'n' => output(b"\n")?,
            b'r' => output(b"\r")?,
            b't' => output(b"\t")?,
            b'u' => {
                let first = self.hex_quad()?;
                let scalar = if (0xd800..=0xdbff).contains(&first) {
                    self.expect(b'\\')?;
                    self.expect(b'u')?;
                    let second = self.hex_quad()?;
                    if !(0xdc00..=0xdfff).contains(&second) {
                        return Err(ProjectionFailure::Malformed);
                    }
                    0x10000 + (((first - 0xd800) as u32) << 10) + (second - 0xdc00) as u32
                } else if (0xdc00..=0xdfff).contains(&first) {
                    return Err(ProjectionFailure::Malformed);
                } else {
                    first as u32
                };
                let character = char::from_u32(scalar).ok_or(ProjectionFailure::Malformed)?;
                let mut encoded = [0u8; 4];
                output(character.encode_utf8(&mut encoded).as_bytes())?;
            }
            _ => return Err(ProjectionFailure::Malformed),
        }
        Ok(())
    }

    fn hex_quad(&mut self) -> ProjectionResult<u16> {
        let mut value = 0u16;
        for _ in 0..4 {
            let digit = match self.next()?.ok_or(ProjectionFailure::Malformed)? {
                byte @ b'0'..=b'9' => (byte - b'0') as u16,
                byte @ b'a'..=b'f' => (byte - b'a' + 10) as u16,
                byte @ b'A'..=b'F' => (byte - b'A' + 10) as u16,
                _ => return Err(ProjectionFailure::Malformed),
            };
            value = (value << 4) | digit;
        }
        Ok(value)
    }

    fn number(&mut self, capture: bool) -> ProjectionResult<Option<ScalarValue>> {
        let mut bytes = [0u8; MAX_CAPTURED_NUMBER_BYTES];
        let mut length = 0usize;
        let mut overflowed = false;
        let mut negative = false;
        let mut fractional = false;
        let mut exponent = false;
        let record = |byte: u8,
                      bytes: &mut [u8; MAX_CAPTURED_NUMBER_BYTES],
                      length: &mut usize,
                      overflowed: &mut bool| {
            if capture && *length < bytes.len() {
                bytes[*length] = byte;
                *length += 1;
            } else if capture {
                *overflowed = true;
            }
        };

        if self.take_if(b'-')? {
            negative = true;
            record(b'-', &mut bytes, &mut length, &mut overflowed);
        }
        match self.next()?.ok_or(ProjectionFailure::Malformed)? {
            b'0' => {
                record(b'0', &mut bytes, &mut length, &mut overflowed);
                if matches!(self.peek()?, Some(b'0'..=b'9')) {
                    return Err(ProjectionFailure::Malformed);
                }
            }
            first @ b'1'..=b'9' => {
                record(first, &mut bytes, &mut length, &mut overflowed);
                while let Some(byte @ b'0'..=b'9') = self.peek()? {
                    self.next()?;
                    record(byte, &mut bytes, &mut length, &mut overflowed);
                }
            }
            _ => return Err(ProjectionFailure::Malformed),
        }
        if self.take_if(b'.')? {
            fractional = true;
            record(b'.', &mut bytes, &mut length, &mut overflowed);
            let first = self.next()?.ok_or(ProjectionFailure::Malformed)?;
            if !first.is_ascii_digit() {
                return Err(ProjectionFailure::Malformed);
            }
            record(first, &mut bytes, &mut length, &mut overflowed);
            while let Some(byte @ b'0'..=b'9') = self.peek()? {
                self.next()?;
                record(byte, &mut bytes, &mut length, &mut overflowed);
            }
        }
        if matches!(self.peek()?, Some(b'e' | b'E')) {
            exponent = true;
            let exponent = self.next()?.expect("peeked exponent");
            record(exponent, &mut bytes, &mut length, &mut overflowed);
            if matches!(self.peek()?, Some(b'+' | b'-')) {
                let sign = self.next()?.expect("peeked exponent sign");
                record(sign, &mut bytes, &mut length, &mut overflowed);
            }
            let first = self.next()?.ok_or(ProjectionFailure::Malformed)?;
            if !first.is_ascii_digit() {
                return Err(ProjectionFailure::Malformed);
            }
            record(first, &mut bytes, &mut length, &mut overflowed);
            while let Some(byte @ b'0'..=b'9') = self.peek()? {
                self.next()?;
                record(byte, &mut bytes, &mut length, &mut overflowed);
            }
        }

        if !capture || overflowed {
            return Ok(None);
        }
        let encoded =
            std::str::from_utf8(&bytes[..length]).map_err(|_| ProjectionFailure::Malformed)?;
        if !fractional && !exponent {
            if encoded == "-0" {
                return Ok(Some(ScalarValue::Signed(0)));
            }
            if negative && let Ok(value) = encoded.parse::<i64>() {
                return Ok(Some(ScalarValue::Signed(value)));
            }
            if !negative && let Ok(value) = encoded.parse::<u64>() {
                return Ok(Some(ScalarValue::Unsigned(value)));
            }
        }
        let Some(number) = encoded
            .parse::<f64>()
            .ok()
            .filter(|number| number.is_finite())
        else {
            return Ok(None);
        };
        Ok(Some(
            ScalarValue::number(number).map_err(ProjectionFailure::Index)?,
        ))
    }
}

fn utf8_width(first: u8) -> Option<usize> {
    match first {
        0xc2..=0xdf => Some(2),
        0xe0..=0xef => Some(3),
        0xf0..=0xf4 => Some(4),
        _ => None,
    }
}

struct KeyBytes {
    bytes: Vec<u8>,
    overflowed: bool,
}

#[derive(Debug)]
enum ProjectionFailure {
    Malformed,
    Index(IndexError),
}

type ProjectionResult<T> = Result<T, ProjectionFailure>;

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    fn project(
        json: impl AsRef<[u8]>,
        selection: &ProjectionSelection,
        extra_bytes: usize,
    ) -> Result<Option<ProjectedJson>, IndexError> {
        let mut input = Cursor::new(json.as_ref());
        project_json(
            &mut input,
            selection,
            projection_floor_bytes(selection).unwrap() + extra_bytes,
        )
    }

    #[test]
    fn skips_a_large_unrelated_body_without_charging_it() {
        let selection = ProjectionSelection::Strings(vec![("title".into(), "/title".into())]);
        let mut json = br#"{"body":""#.to_vec();
        json.extend(std::iter::repeat_n(b'x', 2 * 1024 * 1024));
        json.extend_from_slice(br#"","nested":{"ignored":[1,2,3]},"title":"bounded"}"#);

        assert_eq!(
            project(json, &selection, "bounded".len()).unwrap(),
            Some(ProjectedJson::Strings(BTreeMap::from([(
                "title".into(),
                "bounded".into()
            )])))
        );
    }

    #[test]
    fn decodes_escaped_pointer_tokens_and_json_strings() {
        let selection =
            ProjectionSelection::Strings(vec![("selected".into(), "/a~1b/~0key".into())]);
        assert_eq!(
            project(br#"{"a/b":{"~key":"hello \uD83D\uDE80"}}"#, &selection, 64,).unwrap(),
            Some(ProjectedJson::Strings(BTreeMap::from([(
                "selected".into(),
                "hello 🚀".into(),
            )])))
        );
    }

    #[test]
    fn rejects_a_selected_string_before_it_exceeds_the_allowance() {
        let selection = ProjectionSelection::Strings(vec![("value".into(), "/value".into())]);
        let error = project(
            format!(r#"{{"value":"{}"}}"#, "x".repeat(1_024)),
            &selection,
            32,
        )
        .unwrap_err();
        assert!(matches!(error, IndexError::ResourceLimit { .. }));
    }

    #[test]
    fn charges_a_long_pointer_before_compiling_its_tokens() {
        let short = ProjectionSelection::Strings(vec![("value".into(), "/x".into())]);
        let long_pointer = format!("/{}", "x".repeat(16 * 1024));
        let long = ProjectionSelection::Strings(vec![("value".into(), long_pointer.clone())]);
        let short_floor = projection_floor_bytes(&short).unwrap();
        let long_floor = projection_floor_bytes(&long).unwrap();
        assert!(
            long_floor >= short_floor + POINTER_BYTE_COPIES * (long_pointer.len() - "/x".len())
        );

        let mut input = Cursor::new(br#"{"x":"value"}"#);
        assert!(matches!(
            project_json(&mut input, &long, long_floor - 1),
            Err(IndexError::ResourceLimit { .. })
        ));
    }

    #[test]
    fn selects_typed_scalar_arrays() {
        let selection = ProjectionSelection::Scalars(vec![
            ("tags".into(), "/tags".into()),
            ("count".into(), "/count".into()),
        ]);
        assert_eq!(
            project(
                br#"{"tags":["one",2,true,null],"count":3}"#,
                &selection,
                512,
            )
            .unwrap(),
            Some(ProjectedJson::Scalars(BTreeMap::from([
                (
                    "count".into(),
                    SelectedScalarField {
                        values: vec![ScalarValue::Unsigned(3)],
                        from_array: false,
                    },
                ),
                (
                    "tags".into(),
                    SelectedScalarField {
                        values: vec![
                            ScalarValue::String("one".into()),
                            ScalarValue::Unsigned(2),
                            ScalarValue::Boolean(true),
                            ScalarValue::Null,
                        ],
                        from_array: true,
                    },
                ),
            ])))
        );
    }

    #[test]
    fn valid_scalar_projection_preserves_an_all_missing_document() {
        let selection = ProjectionSelection::Scalars(vec![
            ("state".into(), "/state".into()),
            ("modified".into(), "/modified".into()),
        ]);

        assert_eq!(
            project(br#"{"other":"value"}"#, &selection, 64).unwrap(),
            Some(ProjectedJson::Scalars(BTreeMap::new()))
        );
    }

    #[test]
    fn preserves_unsigned_integers_and_canonicalizes_lexical_negative_zero() {
        let selection = ProjectionSelection::Scalars(vec![
            ("maximum".into(), "/maximum".into()),
            ("zero".into(), "/zero".into()),
            ("negative_zero".into(), "/negative_zero".into()),
            ("negative".into(), "/negative".into()),
            ("decimal".into(), "/decimal".into()),
            ("exponent".into(), "/exponent".into()),
        ]);
        let Some(ProjectedJson::Scalars(fields)) = project(
            br#"{"maximum":18446744073709551615,"zero":0,"negative_zero":-0,"negative":-2,"decimal":2.0,"exponent":2e0}"#,
            &selection,
            1_024,
        )
        .unwrap()
        else {
            panic!("expected scalar projection")
        };

        assert_eq!(fields["maximum"].values, [ScalarValue::Unsigned(u64::MAX)]);
        assert_eq!(fields["zero"].values, [ScalarValue::Unsigned(0)]);
        assert_eq!(fields["negative_zero"].values, [ScalarValue::Signed(0)]);
        assert_eq!(fields["negative"].values, [ScalarValue::Signed(-2)]);
        assert_eq!(
            fields["decimal"].values,
            [ScalarValue::number(2.0).unwrap()]
        );
        assert_eq!(
            fields["exponent"].values,
            [ScalarValue::number(2.0).unwrap()]
        );
    }

    #[test]
    fn validates_vector_dimensions_and_normalizes() {
        let selection = ProjectionSelection::Vector {
            pointer: "/embedding".into(),
            dimensions: 2,
            normalize: true,
        };
        let Some(ProjectedJson::Vector(vector)) =
            project(br#"{"ignored":[1,2,3],"embedding":[3,4]}"#, &selection, 64).unwrap()
        else {
            panic!("expected vector projection");
        };
        assert!((vector[0] - 0.6).abs() < f32::EPSILON);
        assert!((vector[1] - 0.8).abs() < f32::EPSILON);

        assert_eq!(
            project(br#"{"embedding":[1,2,3]}"#, &selection, 64).unwrap(),
            None
        );
    }

    #[test]
    fn hybrid_requires_a_vector_and_at_least_one_selected_string() {
        let selection = ProjectionSelection::Hybrid {
            strings: vec![
                ("title".into(), "/title".into()),
                ("summary".into(), "/summary".into()),
            ],
            vector_pointer: "/embedding".into(),
            dimensions: 2,
            normalize: false,
        };
        assert_eq!(
            project(br#"{"title":"indexed","embedding":[1,2]}"#, &selection, 128,).unwrap(),
            Some(ProjectedJson::Hybrid {
                strings: BTreeMap::from([("title".into(), "indexed".into())]),
                vector: vec![1.0, 2.0],
            })
        );
        assert_eq!(
            project(br#"{"title":"indexed"}"#, &selection, 128).unwrap(),
            None
        );
    }

    #[test]
    fn malformed_missing_and_type_mismatches_are_skipped() {
        let selection = ProjectionSelection::Vector {
            pointer: "/embedding".into(),
            dimensions: 2,
            normalize: false,
        };
        for json in [
            br#"{"embedding":[1,}"#.as_slice(),
            br#"{"other":[1,2]}"#,
            br#"{"embedding":"not a vector"}"#,
            br#"{"embedding":[1,false]}"#,
        ] {
            assert_eq!(project(json, &selection, 64).unwrap(), None);
        }
    }
}
