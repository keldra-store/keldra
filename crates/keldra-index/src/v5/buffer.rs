use std::collections::BTreeMap;

use crate::IndexError;

use super::{
    CanonicalRecipeState, ComponentIdentity, ComponentRoot, DocumentHead, ProjectedDocumentDelta,
    RecipeIdentity, StableDocumentKey,
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
pub struct SealedComponentDelta {
    pub root: ComponentRoot,
    pub bytes: Vec<u8>,
    pub records: u64,
}

/// One byte-accounted mutation buffer shared by all dirty recipes in a source
/// partition. No logical definition receives a private reservation.
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

    /// Coalesces an exact document delta into its independently replaceable
    /// physical components. Failure leaves the complete buffer unchanged.
    pub fn apply(
        &mut self,
        stable_key: StableDocumentKey,
        delta: ProjectedDocumentDelta,
    ) -> Result<(), IndexError> {
        let mut incoming = Vec::new();
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

fn seal_component(
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
        root: ComponentRoot::new(component, artifact_hash, encoded_bytes, logical_bytes)?,
        bytes,
        records,
    })
}

pub fn decode_component_delta(bytes: &[u8]) -> Result<Vec<ComponentDeltaRecord>, IndexError> {
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
    let _component = input.component()?;
    let count = usize::try_from(input.u64()?).map_err(|_| IndexError::OffsetOverflow)?;
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
    Ok(output)
}

fn put_component(out: &mut Vec<u8>, component: ComponentIdentity) {
    match component {
        ComponentIdentity::DocumentHead => out.push(1),
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
    fn u64(&mut self) -> Result<u64, IndexError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn array_32(&mut self) -> Result<[u8; 32], IndexError> {
        Ok(self.take(32)?.try_into().unwrap())
    }
    fn component(&mut self) -> Result<ComponentIdentity, IndexError> {
        match self.byte()? {
            1 => Ok(ComponentIdentity::DocumentHead),
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
        let scope = [9; 32];
        ProjectedDocumentState::new(
            scope,
            DocumentHead::new(scope, "objects/a".into(), 0, version, None, true).unwrap(),
            vec![CanonicalRecipeState::new(recipe(1), vec![1]).unwrap()],
            vec![
                CanonicalRecipeState::new(recipe(2), first.to_vec()).unwrap(),
                CanonicalRecipeState::new(recipe(3), second.to_vec()).unwrap(),
            ],
        )
        .unwrap()
    }

    #[test]
    fn projection_preserving_update_seals_only_the_head_component() {
        let old = state(1, b"stable", b"also stable");
        let new = state(2, b"stable", b"also stable");
        let mut buffer = ProjectionMutationBuffer::new(16 * 1024).unwrap();
        buffer
            .apply(new.head.stable_key, new.delta_from(Some(&old)).unwrap())
            .unwrap();
        let sealed = buffer.seal().unwrap();
        assert_eq!(sealed.len(), 1);
        assert_eq!(sealed[0].root.component, ComponentIdentity::DocumentHead);
    }

    #[test]
    fn one_field_change_does_not_seal_the_other_field() {
        let old = state(1, b"old", b"stable");
        let new = state(2, b"new", b"stable");
        let mut buffer = ProjectionMutationBuffer::new(16 * 1024).unwrap();
        buffer
            .apply(new.head.stable_key, new.delta_from(Some(&old)).unwrap())
            .unwrap();
        let sealed = buffer.seal().unwrap();
        assert_eq!(sealed.len(), 2);
        assert!(
            sealed
                .iter()
                .any(|segment| segment.root.component == ComponentIdentity::DocumentHead)
        );
        assert!(
            sealed
                .iter()
                .any(|segment| segment.root.component == ComponentIdentity::Field(recipe(2)))
        );
        assert!(
            !sealed
                .iter()
                .any(|segment| segment.root.component == ComponentIdentity::Field(recipe(3)))
        );
        for segment in sealed {
            assert_eq!(decode_component_delta(&segment.bytes).unwrap().len(), 1);
        }
    }

    #[test]
    fn failed_admission_does_not_partially_mutate_the_buffer() {
        let state = state(1, &vec![1; 4096], b"small");
        let delta = state.delta_from(None).unwrap();
        let mut buffer = ProjectionMutationBuffer::new(1024).unwrap();
        assert!(matches!(
            buffer.apply(state.head.stable_key, delta),
            Err(IndexError::ResourceLimit { .. })
        ));
        assert!(buffer.is_empty());
        assert_eq!(buffer.used_bytes(), 0);
    }
}
