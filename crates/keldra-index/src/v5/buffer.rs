use std::collections::BTreeMap;

use crate::IndexError;

use super::{
    CanonicalRecipeState, ComponentIdentity, DocumentHead, ProjectedDocumentDelta,
    ProjectedDocumentState, RecipeIdentity, StableDocumentKey, encode_projected_document_state,
};

const SEGMENT_MAGIC: &[u8; 8] = b"K5DELTA1";
const SEGMENT_FORMAT: u16 = 1;
const ENTRY_ACCOUNTING_BYTES: usize = 160;
const COMPONENT_ACCOUNTING_BYTES: usize = 192;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentDeltaRecord {
    pub stable_key: StableDocumentKey,
    pub replacement: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedComponentDelta {
    pub component: ComponentIdentity,
    pub records: Vec<ComponentDeltaRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedComponentDelta {
    pub component: ComponentIdentity,
    pub hash: [u8; 32],
    pub encoded_bytes: u64,
    pub logical_bytes: u64,
    pub bytes: Vec<u8>,
    pub records: u64,
}

/// One byte-accounted mutation buffer shared by all dirty recipes in a source
/// partition. No logical definition receives a private reservation.
#[derive(Clone)]
pub struct ProjectionMutationBuffer {
    limit_bytes: usize,
    used_bytes: usize,
    components: BTreeMap<ComponentIdentity, BTreeMap<StableDocumentKey, Option<Vec<u8>>>>,
}

impl ProjectionMutationBuffer {
    pub fn new(limit_bytes: usize) -> Result<Self, IndexError> {
        if limit_bytes < COMPONENT_ACCOUNTING_BYTES + ENTRY_ACCOUNTING_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: COMPONENT_ACCOUNTING_BYTES + ENTRY_ACCOUNTING_BYTES,
                limit: limit_bytes,
            });
        }
        Ok(Self {
            limit_bytes,
            used_bytes: 0,
            components: BTreeMap::new(),
        })
    }

    pub const fn used_bytes(&self) -> usize {
        self.used_bytes
    }

    pub fn component_count(&self) -> usize {
        self.components.len()
    }

    pub fn is_empty(&self) -> bool {
        self.components.is_empty()
    }

    /// Newest projected-state update already coalesced in this unsealed
    /// buffer. Outer `None` means no buffered update; inner `None` is an
    /// explicit tombstone and must not fall back to the preceding generation.
    pub fn projected_state_update(
        &self,
        stable_key: StableDocumentKey,
    ) -> Result<Option<Option<ProjectedDocumentState>>, IndexError> {
        self.components
            .get(&ComponentIdentity::ProjectedState)
            .and_then(|records| records.get(&stable_key))
            .map(|replacement| {
                replacement
                    .as_deref()
                    .map(super::decode_projected_document_state)
                    .transpose()
            })
            .transpose()
    }

    /// Compares and coalesces one exact projected state into independently
    /// replaceable physical components. The complete state record is retained
    /// for the next HOT-equivalent comparison; unchanged query components are
    /// not rewritten. Failure leaves the complete buffer unchanged.
    pub fn apply_state(
        &mut self,
        state: &ProjectedDocumentState,
        previous: Option<&ProjectedDocumentState>,
    ) -> Result<(), IndexError> {
        state.validate()?;
        let stable_key = state.head.stable_key;
        let delta = state.delta_from(previous)?;
        let incoming = vec![(
            ComponentIdentity::ProjectedState,
            stable_key,
            Some(encode_projected_document_state(state)?),
        )];
        self.apply_delta(stable_key, delta, incoming)
    }

    /// Atomically applies the complete expanded-record result for one exact
    /// source object and maintains its bounded delete/shrink locator.
    ///
    /// The caller loads `previous` through the preceding generation's
    /// `SourceRecords` entry. This method uses a transactional buffer clone;
    /// runtime admission must therefore reserve twice the configured buffer
    /// limit while it is active.
    pub fn apply_source_states(
        &mut self,
        source_scope: [u8; 32],
        source_path: &str,
        source_version: u64,
        current: Vec<ProjectedDocumentState>,
        previous: Vec<ProjectedDocumentState>,
    ) -> Result<(), IndexError> {
        if source_scope == [0; 32] || source_version == 0 {
            return Err(IndexError::InvalidDefinition(
                "projected source update has an invalid identity".into(),
            ));
        }
        let current =
            validate_source_state_set(source_scope, source_path, Some(source_version), current)?;
        let previous = validate_source_state_set(source_scope, source_path, None, previous)?;
        let mut working = self.clone();
        for (key, state) in &current {
            working.apply_state(state, previous.get(key))?;
        }
        for (key, state) in &previous {
            if current.contains_key(key) {
                continue;
            }
            let deleted = ProjectedDocumentState::new(
                source_scope,
                DocumentHead::new(
                    source_scope,
                    source_path.into(),
                    state.head.source_record,
                    source_version,
                    None,
                    false,
                )?,
                Vec::new(),
                Vec::new(),
            )?;
            working.apply_state(&deleted, Some(state))?;
        }
        let current_keys = current.keys().copied().collect::<Vec<_>>();
        let previous_keys = previous.keys().copied().collect::<Vec<_>>();
        if current_keys != previous_keys {
            let locator_key = StableDocumentKey::derive(source_scope, source_path, 0)?;
            let replacement = (!current_keys.is_empty())
                .then(|| encode_source_records(source_path, &current_keys))
                .transpose()?;
            working.apply_delta(
                locator_key,
                ProjectedDocumentDelta {
                    head: None,
                    memberships: Vec::new(),
                    fields: Vec::new(),
                },
                vec![(ComponentIdentity::SourceRecords, locator_key, replacement)],
            )?;
        }
        *self = working;
        Ok(())
    }

    fn apply_delta(
        &mut self,
        stable_key: StableDocumentKey,
        delta: ProjectedDocumentDelta,
        mut incoming: Vec<(ComponentIdentity, StableDocumentKey, Option<Vec<u8>>)>,
    ) -> Result<(), IndexError> {
        if let Some(head) = delta.head {
            if head.stable_key != stable_key {
                return Err(IndexError::InvalidDefinition(
                    "projected delta crossed its stable document key".into(),
                ));
            }
            incoming.push((
                ComponentIdentity::DocumentHead,
                stable_key,
                Some(encode_document_head(&head)?),
            ));
        }
        for change in delta.memberships {
            incoming.push((
                ComponentIdentity::Membership(change.recipe),
                stable_key,
                change
                    .replacement
                    .map(|state| encode_recipe_state(change.recipe, state))
                    .transpose()?,
            ));
        }
        for change in delta.fields {
            incoming.push((
                ComponentIdentity::Field(change.recipe),
                stable_key,
                change
                    .replacement
                    .map(|state| encode_recipe_state(change.recipe, state))
                    .transpose()?,
            ));
        }
        incoming.sort_by_key(|entry| entry.0);
        if incoming.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(IndexError::InvalidDefinition(
                "one projected delta changed a component more than once".into(),
            ));
        }

        let mut projected = self.used_bytes;
        for (component, key, replacement) in &incoming {
            let existing = self
                .components
                .get(component)
                .and_then(|records| records.get(key));
            projected = projected
                .checked_sub(existing.map_or(0, accounted_record_bytes))
                .ok_or(IndexError::OffsetOverflow)?;
            if existing.is_none() && !self.components.contains_key(component) {
                projected = projected
                    .checked_add(COMPONENT_ACCOUNTING_BYTES)
                    .ok_or(IndexError::OffsetOverflow)?;
            }
            projected = projected
                .checked_add(accounted_record_bytes(replacement))
                .ok_or(IndexError::OffsetOverflow)?;
        }
        if projected > self.limit_bytes {
            return Err(IndexError::ResourceLimit {
                needed: projected,
                limit: self.limit_bytes,
            });
        }

        for (component, key, replacement) in incoming {
            self.components
                .entry(component)
                .or_default()
                .insert(key, replacement);
        }
        self.used_bytes = projected;
        Ok(())
    }

    pub fn seal(self) -> Result<Vec<SealedComponentDelta>, IndexError> {
        self.components
            .into_iter()
            .map(|(component, records)| seal_component(component, records))
            .collect()
    }
}

fn validate_source_state_set(
    source_scope: [u8; 32],
    source_path: &str,
    exact_version: Option<u64>,
    states: Vec<ProjectedDocumentState>,
) -> Result<BTreeMap<StableDocumentKey, ProjectedDocumentState>, IndexError> {
    let mut indexed = BTreeMap::new();
    for (ordinal, state) in states.into_iter().enumerate() {
        state.validate()?;
        if state.source_scope != source_scope
            || state.head.source_path != source_path
            || state.head.source_record
                != u32::try_from(ordinal).map_err(|_| IndexError::OffsetOverflow)?
            || exact_version.is_some_and(|version| state.head.source_version != version)
            || !state.head.live
            || indexed.insert(state.head.stable_key, state).is_some()
        {
            return Err(IndexError::InvalidDefinition(
                "projected source records are not one exact contiguous expansion".into(),
            ));
        }
    }
    Ok(indexed)
}

fn encode_source_records(
    source_path: &str,
    stable_keys: &[StableDocumentKey],
) -> Result<Vec<u8>, IndexError> {
    let mut bytes = Vec::new();
    put_bytes(&mut bytes, source_path.as_bytes())?;
    put_u32(
        &mut bytes,
        u32::try_from(stable_keys.len()).map_err(|_| IndexError::OffsetOverflow)?,
    );
    for key in stable_keys {
        bytes.extend_from_slice(&key.bytes());
    }
    Ok(bytes)
}

/// Decode and verify the exact stable-key expansion retained for one source
/// object. Callers use this locator before loading prior projected states, so a
/// malformed path or ordinal/key mismatch fails before any component lookup.
pub fn decode_source_records(
    source_scope: [u8; 32],
    expected_path: &str,
    bytes: &[u8],
) -> Result<Vec<StableDocumentKey>, IndexError> {
    let mut input = Decoder::new(bytes);
    let path_length = input.u32()? as usize;
    let path = std::str::from_utf8(input.take(path_length)?)
        .map_err(|_| IndexError::Decode("source-record path is not UTF-8".into()))?;
    let count = input.u32()? as usize;
    if path != expected_path || count == 0 || count > input.remaining() / 32 {
        return Err(IndexError::InvalidFormat(
            "source-record locator is invalid or unbounded",
        ));
    }
    let mut keys = Vec::with_capacity(count);
    for ordinal in 0..count {
        let key = StableDocumentKey::from_bytes(input.array_32()?)?;
        let ordinal = u32::try_from(ordinal).map_err(|_| IndexError::OffsetOverflow)?;
        if key != StableDocumentKey::derive(source_scope, expected_path, ordinal)? {
            return Err(IndexError::Integrity);
        }
        keys.push(key);
    }
    input.finish()?;
    Ok(keys)
}

fn accounted_record_bytes(value: &Option<Vec<u8>>) -> usize {
    ENTRY_ACCOUNTING_BYTES.saturating_add(value.as_ref().map_or(0, Vec::len))
}

fn encode_recipe_state(
    expected: RecipeIdentity,
    state: CanonicalRecipeState,
) -> Result<Vec<u8>, IndexError> {
    if state.recipe != expected || state.digest != *blake3::hash(&state.value).as_bytes() {
        return Err(IndexError::InvalidDefinition(
            "projected recipe delta does not match its component".into(),
        ));
    }
    Ok(state.value)
}

fn encode_document_head(head: &DocumentHead) -> Result<Vec<u8>, IndexError> {
    let mut bytes = Vec::new();
    put_bytes(&mut bytes, head.source_path.as_bytes())?;
    put_u32(&mut bytes, head.source_record);
    put_u64(&mut bytes, head.source_version);
    bytes.push(u8::from(head.live));
    match &head.result {
        Some(result) => {
            bytes.push(1);
            put_bytes(&mut bytes, result.path.as_bytes())?;
            put_u64(&mut bytes, result.version);
        }
        None => bytes.push(0),
    }
    Ok(bytes)
}

pub(super) fn seal_component(
    component: ComponentIdentity,
    records: BTreeMap<StableDocumentKey, Option<Vec<u8>>>,
) -> Result<SealedComponentDelta, IndexError> {
    if records.is_empty() {
        return Err(IndexError::InvalidDefinition(
            "cannot seal an empty projection component".into(),
        ));
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(SEGMENT_MAGIC);
    put_u16(&mut bytes, SEGMENT_FORMAT);
    put_component(&mut bytes, component);
    put_u64(&mut bytes, records.len() as u64);
    let mut logical_bytes = 0_u64;
    for (key, replacement) in records {
        bytes.extend_from_slice(&key.bytes());
        logical_bytes = logical_bytes
            .checked_add(33)
            .ok_or(IndexError::OffsetOverflow)?;
        match replacement {
            Some(value) => {
                bytes.push(1);
                put_bytes_u64(&mut bytes, &value)?;
                logical_bytes = logical_bytes
                    .checked_add(value.len() as u64)
                    .ok_or(IndexError::OffsetOverflow)?;
            }
            None => bytes.push(0),
        }
    }
    let integrity = *blake3::hash(&bytes).as_bytes();
    bytes.extend_from_slice(&integrity);
    let artifact_hash = *blake3::hash(&bytes).as_bytes();
    let encoded_bytes = bytes.len() as u64;
    let records = decode_component_delta(&bytes)?.len() as u64;
    Ok(SealedComponentDelta {
        component,
        hash: artifact_hash,
        encoded_bytes,
        logical_bytes,
        bytes,
        records,
    })
}

pub fn decode_component_delta(bytes: &[u8]) -> Result<Vec<ComponentDeltaRecord>, IndexError> {
    Ok(decode_component_delta_segment(bytes)?.records)
}

pub fn decode_component_delta_segment(bytes: &[u8]) -> Result<DecodedComponentDelta, IndexError> {
    let split = bytes
        .len()
        .checked_sub(32)
        .ok_or(IndexError::UnexpectedEof {
            expected: 32,
            actual: bytes.len() as u64,
        })?;
    let (payload, integrity) = bytes.split_at(split);
    if blake3::hash(payload).as_bytes() != integrity {
        return Err(IndexError::Integrity);
    }
    let mut input = Decoder::new(payload);
    input.expect(SEGMENT_MAGIC)?;
    if input.u16()? != SEGMENT_FORMAT {
        return Err(IndexError::InvalidFormat(
            "projection delta segment format is unsupported",
        ));
    }
    let component = input.component()?;
    let count = usize::try_from(input.u64()?).map_err(|_| IndexError::OffsetOverflow)?;
    if count == 0 || count > input.remaining() / 33 {
        return Err(IndexError::InvalidFormat(
            "projection delta record count is unbounded",
        ));
    }
    let mut output = Vec::with_capacity(count);
    for _ in 0..count {
        let stable_key = StableDocumentKey::from_bytes(input.array_32()?)?;
        let replacement = match input.byte()? {
            0 => None,
            1 => {
                let length =
                    usize::try_from(input.u64()?).map_err(|_| IndexError::OffsetOverflow)?;
                Some(input.take(length)?.to_vec())
            }
            _ => return Err(IndexError::Decode("delta presence is invalid".into())),
        };
        if output
            .last()
            .is_some_and(|previous: &ComponentDeltaRecord| previous.stable_key >= stable_key)
        {
            return Err(IndexError::UnsortedRecords);
        }
        output.push(ComponentDeltaRecord {
            stable_key,
            replacement,
        });
    }
    input.finish()?;
    Ok(DecodedComponentDelta {
        component,
        records: output,
    })
}

fn put_component(out: &mut Vec<u8>, component: ComponentIdentity) {
    match component {
        ComponentIdentity::DocumentHead => out.push(1),
        ComponentIdentity::ProjectedState => out.push(5),
        ComponentIdentity::SourceRecords => out.push(6),
        ComponentIdentity::Membership(recipe) => {
            out.push(2);
            out.extend_from_slice(&recipe.bytes());
        }
        ComponentIdentity::Field(recipe) => {
            out.push(3);
            out.extend_from_slice(&recipe.bytes());
        }
        ComponentIdentity::Order(recipe) => {
            out.push(4);
            out.extend_from_slice(&recipe.bytes());
        }
    }
}

fn put_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), IndexError> {
    put_u32(
        out,
        u32::try_from(value.len()).map_err(|_| IndexError::OffsetOverflow)?,
    );
    out.extend_from_slice(value);
    Ok(())
}
fn put_bytes_u64(out: &mut Vec<u8>, value: &[u8]) -> Result<(), IndexError> {
    put_u64(
        out,
        u64::try_from(value.len()).map_err(|_| IndexError::OffsetOverflow)?,
    );
    out.extend_from_slice(value);
    Ok(())
}
fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}
fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}
fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn take(&mut self, length: usize) -> Result<&'a [u8], IndexError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(IndexError::OffsetOverflow)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(IndexError::UnexpectedEof {
                expected: end as u64,
                actual: self.bytes.len() as u64,
            })?;
        self.offset = end;
        Ok(value)
    }
    fn expect(&mut self, expected: &[u8]) -> Result<(), IndexError> {
        if self.take(expected.len())? == expected {
            Ok(())
        } else {
            Err(IndexError::InvalidFormat(
                "projection delta magic is invalid",
            ))
        }
    }
    fn byte(&mut self) -> Result<u8, IndexError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, IndexError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, IndexError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, IndexError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn array_32(&mut self) -> Result<[u8; 32], IndexError> {
        Ok(self.take(32)?.try_into().unwrap())
    }
    fn component(&mut self) -> Result<ComponentIdentity, IndexError> {
        match self.byte()? {
            1 => Ok(ComponentIdentity::DocumentHead),
            5 => Ok(ComponentIdentity::ProjectedState),
            6 => Ok(ComponentIdentity::SourceRecords),
            2 => Ok(ComponentIdentity::Membership(RecipeIdentity::new(
                self.array_32()?,
            )?)),
            3 => Ok(ComponentIdentity::Field(RecipeIdentity::new(
                self.array_32()?,
            )?)),
            4 => Ok(ComponentIdentity::Order(RecipeIdentity::new(
                self.array_32()?,
            )?)),
            _ => Err(IndexError::Decode("projection component is unknown".into())),
        }
    }
    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }
    fn finish(self) -> Result<(), IndexError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(IndexError::Decode(
                "projection delta has trailing bytes".into(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v5::{CanonicalRecipeState, ProjectedDocumentState};

    fn recipe(byte: u8) -> RecipeIdentity {
        RecipeIdentity::new([byte; 32]).unwrap()
    }
    fn state(version: u64, first: &[u8], second: &[u8]) -> ProjectedDocumentState {
        state_record(version, 0, first, second)
    }
    fn state_record(
        version: u64,
        record: u32,
        first: &[u8],
        second: &[u8],
    ) -> ProjectedDocumentState {
        let scope = [9; 32];
        ProjectedDocumentState::new(
            scope,
            DocumentHead::new(scope, "objects/a".into(), record, version, None, true).unwrap(),
            vec![CanonicalRecipeState::new(recipe(1), vec![1]).unwrap()],
            vec![
                CanonicalRecipeState::new(recipe(2), first.to_vec()).unwrap(),
                CanonicalRecipeState::new(recipe(3), second.to_vec()).unwrap(),
            ],
        )
        .unwrap()
    }

    #[test]
    fn projection_preserving_update_seals_state_and_head_but_no_query_component() {
        let old = state(1, b"stable", b"also stable");
        let new = state(2, b"stable", b"also stable");
        let mut buffer = ProjectionMutationBuffer::new(16 * 1024).unwrap();
        buffer.apply_state(&new, Some(&old)).unwrap();
        let sealed = buffer.seal().unwrap();
        assert_eq!(sealed.len(), 2);
        assert_eq!(sealed[0].component, ComponentIdentity::DocumentHead);
        assert_eq!(sealed[1].component, ComponentIdentity::ProjectedState);
        for segment in sealed {
            let decoded = decode_component_delta_segment(&segment.bytes).unwrap();
            assert_eq!(decoded.component, segment.component);
            assert_eq!(decoded.records.len(), 1);
        }
    }

    #[test]
    fn pathological_repeated_unindexed_updates_coalesce_without_field_amplification() {
        let mut previous = state(1, b"stable", b"also stable");
        let mut buffer = ProjectionMutationBuffer::new(16 * 1024).unwrap();
        for version in 2..=10_001 {
            let current = state(version, b"stable", b"also stable");
            buffer.apply_state(&current, Some(&previous)).unwrap();
            previous = current;
        }

        let sealed = buffer.seal().unwrap();
        assert_eq!(
            sealed
                .iter()
                .map(|segment| segment.component)
                .collect::<Vec<_>>(),
            vec![
                ComponentIdentity::DocumentHead,
                ComponentIdentity::ProjectedState,
            ]
        );
        assert!(sealed.iter().all(|segment| segment.records == 1));
        assert!(sealed.iter().all(|segment| {
            !matches!(
                segment.component,
                ComponentIdentity::Membership(_)
                    | ComponentIdentity::Field(_)
                    | ComponentIdentity::Order(_)
            )
        }));
        assert!(
            sealed
                .iter()
                .map(|segment| segment.encoded_bytes)
                .sum::<u64>()
                < 1_024,
            "ten thousand source versions must collapse to one bounded head/state delta"
        );
    }

    #[test]
    fn one_field_change_does_not_seal_the_other_field() {
        let old = state(1, b"old", b"stable");
        let new = state(2, b"new", b"stable");
        let mut buffer = ProjectionMutationBuffer::new(16 * 1024).unwrap();
        buffer.apply_state(&new, Some(&old)).unwrap();
        let sealed = buffer.seal().unwrap();
        assert_eq!(sealed.len(), 3);
        assert!(
            sealed
                .iter()
                .any(|segment| segment.component == ComponentIdentity::DocumentHead)
        );
        assert!(
            sealed
                .iter()
                .any(|segment| segment.component == ComponentIdentity::Field(recipe(2)))
        );
        assert!(
            sealed
                .iter()
                .any(|segment| segment.component == ComponentIdentity::ProjectedState)
        );
        assert!(
            !sealed
                .iter()
                .any(|segment| segment.component == ComponentIdentity::Field(recipe(3)))
        );
        for segment in sealed {
            assert_eq!(decode_component_delta(&segment.bytes).unwrap().len(), 1);
        }
    }

    #[test]
    fn failed_admission_does_not_partially_mutate_the_buffer() {
        let state = state(1, &vec![1; 4096], b"small");
        let mut buffer = ProjectionMutationBuffer::new(1024).unwrap();
        assert!(matches!(
            buffer.apply_state(&state, None),
            Err(IndexError::ResourceLimit { .. })
        ));
        assert!(buffer.is_empty());
        assert_eq!(buffer.used_bytes(), 0);
    }

    #[test]
    fn delta_decoder_rejects_an_impossible_record_count_before_allocation() {
        let state = state(1, b"a", b"b");
        let mut buffer = ProjectionMutationBuffer::new(16 * 1024).unwrap();
        buffer.apply_state(&state, None).unwrap();
        let mut bytes = buffer.seal().unwrap().remove(0).bytes;
        let count_offset = SEGMENT_MAGIC.len() + 2 + 1;
        bytes[count_offset..count_offset + 8].copy_from_slice(&u64::MAX.to_le_bytes());
        let integrity_offset = bytes.len() - 32;
        let integrity = *blake3::hash(&bytes[..integrity_offset]).as_bytes();
        bytes[integrity_offset..].copy_from_slice(&integrity);
        assert!(matches!(
            decode_component_delta_segment(&bytes),
            Err(IndexError::OffsetOverflow)
                | Err(IndexError::InvalidFormat(
                    "projection delta record count is unbounded"
                ))
        ));
    }

    #[test]
    fn complete_source_update_tracks_expansion_and_tombstones_removed_records() {
        let previous = vec![
            state_record(1, 0, b"first", b"stable"),
            state_record(1, 1, b"removed", b"stable"),
        ];
        let current = vec![state_record(2, 0, b"first", b"stable")];
        let current_key = current[0].head.stable_key;
        let mut buffer = ProjectionMutationBuffer::new(64 * 1024).unwrap();
        buffer
            .apply_source_states([9; 32], "objects/a", 2, current, previous)
            .unwrap();
        let sealed = buffer.seal().unwrap();
        let locator = sealed
            .iter()
            .find(|delta| delta.component == ComponentIdentity::SourceRecords)
            .unwrap();
        let locator_records = decode_component_delta(locator.bytes.as_slice()).unwrap();
        assert_eq!(locator_records.len(), 1);
        assert_eq!(
            decode_source_records(
                [9; 32],
                "objects/a",
                locator_records[0].replacement.as_deref().unwrap(),
            )
            .unwrap(),
            vec![current_key]
        );
        let membership = sealed
            .iter()
            .find(|delta| delta.component == ComponentIdentity::Membership(recipe(1)))
            .unwrap();
        let membership = decode_component_delta_segment(&membership.bytes).unwrap();
        assert_eq!(membership.records.len(), 1);
        assert!(membership.records[0].replacement.is_none());
    }

    #[test]
    fn complete_source_admission_failure_rolls_back_every_component() {
        let previous = vec![state_record(1, 0, b"old", b"stable")];
        let current = vec![state_record(2, 0, &vec![3; 4096], b"stable")];
        let mut buffer = ProjectionMutationBuffer::new(1024).unwrap();
        assert!(matches!(
            buffer.apply_source_states([9; 32], "objects/a", 2, current, previous),
            Err(IndexError::ResourceLimit { .. })
        ));
        assert!(buffer.is_empty());
        assert_eq!(buffer.used_bytes(), 0);
    }
}
