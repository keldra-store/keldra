//! Deliberate V1 binary encoding for Anvil-owned Raft storage and wire values.
//!
//! This is intentionally private to the consensus crate. It is not a general
//! persistence framework: the domain tag at every call site identifies the
//! exact Raft value being encoded, and a decoder refuses values from another
//! domain. The payload encoding is deterministic, fixed-width, and bounded.

use std::fmt;

use serde::{
    Serialize,
    de::{
        self, DeserializeOwned, DeserializeSeed, EnumAccess, IntoDeserializer, MapAccess,
        SeqAccess, VariantAccess, Visitor,
    },
    ser::{
        self, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
};

use crate::MAX_CONSENSUS_RPC_PAYLOAD_BYTES;

const MAGIC: &[u8; 8] = b"ANVRAFT\0";
const VERSION: u16 = 1;
const HEADER_LEN: usize = 16;

// Durable aggregate state and snapshots can contain many individually bounded
// log-like values. Keep their envelope bound explicit and finite without
// weakening the 64 MiB wire/log or nested byte-string limits.
const MAX_DURABLE_AGGREGATE_PAYLOAD_BYTES: usize = 512 * 1024 * 1024;
const MAX_NESTED_BYTE_STRING_BYTES: usize = MAX_CONSENSUS_RPC_PAYLOAD_BYTES;
const MAX_STRING_BYTES: usize = 1024 * 1024;
const MAX_COLLECTION_ELEMENTS: usize = 16 * 1024 * 1024;
const MAX_NESTING_DEPTH: usize = 64;

/// Stable type discriminator carried by every V1 envelope.
///
/// Values are explicit and append-only. Reusing an existing value for another
/// Rust type would defeat the cross-domain rejection this enum provides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub(crate) enum ValueKind {
    StoredVote = 1,
    StoredLogEntry = 2,
    StoredPurgedLogId = 3,
    StoredCertificationState = 4,
    StoredOpenRaftState = 5,
    CertificationSnapshot = 6,
    OpenRaftSnapshot = 7,

    AppendEntriesRequest = 0x100,
    AppendEntriesResponse = 0x101,
    VoteRequest = 0x102,
    VoteResponse = 0x103,
    InstallSnapshotRequest = 0x104,
    InstallSnapshotResponse = 0x105,
    ForwardCertifyRequest = 0x106,
    ForwardCertifyResponse = 0x107,
    ForwardLinearizedReadResponse = 0x108,
    ForwardTransactionOutcomeRequest = 0x109,
    ForwardTransactionOutcomeResponse = 0x10a,
}

const fn envelope_payload_limit(kind: ValueKind) -> usize {
    match kind {
        ValueKind::StoredCertificationState
        | ValueKind::StoredOpenRaftState
        | ValueKind::CertificationSnapshot
        | ValueKind::OpenRaftSnapshot => MAX_DURABLE_AGGREGATE_PAYLOAD_BYTES,
        _ => MAX_CONSENSUS_RPC_PAYLOAD_BYTES,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CodecError(String);

impl CodecError {
    fn message(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for CodecError {}

impl ser::Error for CodecError {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self::message(message.to_string())
    }
}

impl de::Error for CodecError {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self::message(message.to_string())
    }
}

pub(crate) fn encode<T: Serialize + ?Sized>(
    kind: ValueKind,
    value: &T,
) -> Result<Vec<u8>, CodecError> {
    let mut serializer = PayloadSerializer::new(envelope_payload_limit(kind));
    value.serialize(&mut serializer)?;
    let payload = serializer.finish()?;
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| CodecError::message("Raft value exceeds the V1 length field"))?;

    let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len());
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&VERSION.to_be_bytes());
    bytes.extend_from_slice(&(kind as u16).to_be_bytes());
    bytes.extend_from_slice(&payload_len.to_be_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

pub(crate) fn decode<T: DeserializeOwned>(
    expected_kind: ValueKind,
    bytes: &[u8],
) -> Result<T, CodecError> {
    if bytes.len() < HEADER_LEN {
        return Err(CodecError::message("truncated Raft V1 envelope"));
    }
    if &bytes[..MAGIC.len()] != MAGIC {
        return Err(CodecError::message("invalid Raft V1 envelope magic"));
    }

    let version = u16::from_be_bytes(
        bytes[8..10]
            .try_into()
            .expect("fixed envelope version slice"),
    );
    if version != VERSION {
        return Err(CodecError::message(format!(
            "unsupported Raft binary format version {version}"
        )));
    }

    let actual_kind =
        u16::from_be_bytes(bytes[10..12].try_into().expect("fixed envelope kind slice"));
    if actual_kind != expected_kind as u16 {
        return Err(CodecError::message(format!(
            "Raft value domain mismatch: expected {}, received {actual_kind}",
            expected_kind as u16
        )));
    }

    let declared_len = u32::from_be_bytes(
        bytes[12..16]
            .try_into()
            .expect("fixed envelope length slice"),
    ) as usize;
    let payload_limit = envelope_payload_limit(expected_kind);
    if declared_len > payload_limit {
        return Err(CodecError::message(format!(
            "Raft payload length {declared_len} exceeds {payload_limit}"
        )));
    }
    if bytes.len() != HEADER_LEN + declared_len {
        return Err(CodecError::message(
            "Raft envelope length does not match its payload",
        ));
    }

    let mut deserializer = PayloadDeserializer::new(&bytes[HEADER_LEN..]);
    let value = T::deserialize(&mut deserializer)?;
    if !deserializer.is_finished() {
        return Err(CodecError::message("trailing bytes in Raft V1 payload"));
    }
    Ok(value)
}

struct PayloadSerializer {
    output: Vec<u8>,
    depth: usize,
    payload_limit: usize,
}

impl PayloadSerializer {
    fn new(payload_limit: usize) -> Self {
        Self {
            output: Vec::new(),
            depth: 0,
            payload_limit,
        }
    }

    fn finish(self) -> Result<Vec<u8>, CodecError> {
        if self.depth != 0 {
            return Err(CodecError::message(
                "unbalanced container in Raft V1 encoder",
            ));
        }
        Ok(self.output)
    }

    fn append(&mut self, bytes: &[u8]) -> Result<(), CodecError> {
        let next_len = self
            .output
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| CodecError::message("Raft payload length overflow"))?;
        if next_len > self.payload_limit {
            return Err(CodecError::message(format!(
                "Raft payload exceeds {} bytes",
                self.payload_limit
            )));
        }
        self.output.extend_from_slice(bytes);
        Ok(())
    }

    fn append_u8(&mut self, value: u8) -> Result<(), CodecError> {
        self.append(&[value])
    }

    fn append_len(
        &mut self,
        len: usize,
        maximum: usize,
        description: &str,
    ) -> Result<(), CodecError> {
        if len > maximum {
            return Err(CodecError::message(format!(
                "{description} length {len} exceeds {maximum}"
            )));
        }
        let len = u32::try_from(len)
            .map_err(|_| CodecError::message(format!("{description} length exceeds u32")))?;
        self.append(&len.to_be_bytes())
    }

    fn enter(&mut self) -> Result<(), CodecError> {
        if self.depth >= MAX_NESTING_DEPTH {
            return Err(CodecError::message(format!(
                "Raft value nesting exceeds {MAX_NESTING_DEPTH}"
            )));
        }
        self.depth += 1;
        Ok(())
    }
}

struct Compound<'a> {
    serializer: &'a mut PayloadSerializer,
    expected: usize,
    written: usize,
    map_waiting_for_value: bool,
    finished: bool,
}

impl<'a> Compound<'a> {
    fn new(serializer: &'a mut PayloadSerializer, expected: usize) -> Result<Self, CodecError> {
        serializer.enter()?;
        Ok(Self {
            serializer,
            expected,
            written: 0,
            map_waiting_for_value: false,
            finished: false,
        })
    }

    fn element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), CodecError> {
        if self.written >= self.expected {
            return Err(CodecError::message(
                "container produced more elements than declared",
            ));
        }
        value.serialize(&mut *self.serializer)?;
        self.written += 1;
        Ok(())
    }

    fn finish(mut self) -> Result<(), CodecError> {
        if self.written != self.expected || self.map_waiting_for_value {
            return Err(CodecError::message(format!(
                "container declared {} elements but produced {}",
                self.expected, self.written
            )));
        }
        self.finished = true;
        self.serializer.depth -= 1;
        Ok(())
    }
}

impl Drop for Compound<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.serializer.depth = self.serializer.depth.saturating_sub(1);
        }
    }
}

impl<'a> ser::Serializer for &'a mut PayloadSerializer {
    type Ok = ();
    type Error = CodecError;
    type SerializeSeq = Compound<'a>;
    type SerializeTuple = Compound<'a>;
    type SerializeTupleStruct = Compound<'a>;
    type SerializeTupleVariant = Compound<'a>;
    type SerializeMap = Compound<'a>;
    type SerializeStruct = Compound<'a>;
    type SerializeStructVariant = Compound<'a>;

    fn serialize_bool(self, value: bool) -> Result<Self::Ok, Self::Error> {
        self.append_u8(u8::from(value))
    }

    fn serialize_i8(self, value: i8) -> Result<Self::Ok, Self::Error> {
        self.append(&value.to_be_bytes())
    }

    fn serialize_i16(self, value: i16) -> Result<Self::Ok, Self::Error> {
        self.append(&value.to_be_bytes())
    }

    fn serialize_i32(self, value: i32) -> Result<Self::Ok, Self::Error> {
        self.append(&value.to_be_bytes())
    }

    fn serialize_i64(self, value: i64) -> Result<Self::Ok, Self::Error> {
        self.append(&value.to_be_bytes())
    }

    fn serialize_i128(self, value: i128) -> Result<Self::Ok, Self::Error> {
        self.append(&value.to_be_bytes())
    }

    fn serialize_u8(self, value: u8) -> Result<Self::Ok, Self::Error> {
        self.append_u8(value)
    }

    fn serialize_u16(self, value: u16) -> Result<Self::Ok, Self::Error> {
        self.append(&value.to_be_bytes())
    }

    fn serialize_u32(self, value: u32) -> Result<Self::Ok, Self::Error> {
        self.append(&value.to_be_bytes())
    }

    fn serialize_u64(self, value: u64) -> Result<Self::Ok, Self::Error> {
        self.append(&value.to_be_bytes())
    }

    fn serialize_u128(self, value: u128) -> Result<Self::Ok, Self::Error> {
        self.append(&value.to_be_bytes())
    }

    fn serialize_f32(self, value: f32) -> Result<Self::Ok, Self::Error> {
        self.append(&value.to_bits().to_be_bytes())
    }

    fn serialize_f64(self, value: f64) -> Result<Self::Ok, Self::Error> {
        self.append(&value.to_bits().to_be_bytes())
    }

    fn serialize_char(self, value: char) -> Result<Self::Ok, Self::Error> {
        self.serialize_u32(value as u32)
    }

    fn serialize_str(self, value: &str) -> Result<Self::Ok, Self::Error> {
        self.append_len(value.len(), MAX_STRING_BYTES, "string")?;
        self.append(value.as_bytes())
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.append_len(value.len(), MAX_NESTED_BYTE_STRING_BYTES, "byte string")?;
        self.append(value)
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.append_u8(0)
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        self.append_u8(1)?;
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.serialize_u32(_variant_index)
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        self.serialize_u32(variant_index)?;
        value.serialize(self)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        let len = len.ok_or_else(|| {
            CodecError::message("Raft V1 sequences must declare their element count")
        })?;
        self.append_len(len, MAX_COLLECTION_ELEMENTS, "sequence")?;
        Compound::new(self, len)
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Compound::new(self, len)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Compound::new(self, len)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.serialize_u32(variant_index)?;
        Compound::new(self, len)
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        let len =
            len.ok_or_else(|| CodecError::message("Raft V1 maps must declare their entry count"))?;
        self.append_len(len, MAX_COLLECTION_ELEMENTS, "map")?;
        let item_count = len
            .checked_mul(2)
            .ok_or_else(|| CodecError::message("Raft map item count overflow"))?;
        Compound::new(self, item_count)
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Compound::new(self, len)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.serialize_u32(variant_index)?;
        Compound::new(self, len)
    }

    fn collect_str<T: fmt::Display + ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        self.serialize_str(&value.to_string())
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

impl SerializeSeq for Compound<'_> {
    type Ok = ();
    type Error = CodecError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.element(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.finish()
    }
}

impl SerializeTuple for Compound<'_> {
    type Ok = ();
    type Error = CodecError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.element(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.finish()
    }
}

impl SerializeTupleStruct for Compound<'_> {
    type Ok = ();
    type Error = CodecError;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.element(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.finish()
    }
}

impl SerializeTupleVariant for Compound<'_> {
    type Ok = ();
    type Error = CodecError;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.element(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.finish()
    }
}

impl SerializeMap for Compound<'_> {
    type Ok = ();
    type Error = CodecError;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Self::Error> {
        if self.map_waiting_for_value {
            return Err(CodecError::message(
                "Raft map emitted a second key before its value",
            ));
        }
        self.element(key)?;
        self.map_waiting_for_value = true;
        Ok(())
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        if !self.map_waiting_for_value {
            return Err(CodecError::message(
                "Raft map emitted a value without a key",
            ));
        }
        self.element(value)?;
        self.map_waiting_for_value = false;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.finish()
    }
}

impl SerializeStruct for Compound<'_> {
    type Ok = ();
    type Error = CodecError;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.element(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.finish()
    }
}

impl SerializeStructVariant for Compound<'_> {
    type Ok = ();
    type Error = CodecError;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.element(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.finish()
    }
}

struct PayloadDeserializer<'de> {
    input: &'de [u8],
    offset: usize,
    depth: usize,
}

impl<'de> PayloadDeserializer<'de> {
    fn new(input: &'de [u8]) -> Self {
        Self {
            input,
            offset: 0,
            depth: 0,
        }
    }

    fn is_finished(&self) -> bool {
        self.offset == self.input.len()
    }

    fn take(&mut self, len: usize) -> Result<&'de [u8], CodecError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| CodecError::message("Raft payload offset overflow"))?;
        if end > self.input.len() {
            return Err(CodecError::message("truncated Raft V1 payload"));
        }
        let bytes = &self.input[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        self.take(N)?
            .try_into()
            .map_err(|_| CodecError::message("truncated fixed-width Raft value"))
    }

    fn take_u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }

    fn take_len(&mut self, maximum: usize, description: &str) -> Result<usize, CodecError> {
        let len = u32::from_be_bytes(self.take_array::<4>()?) as usize;
        if len > maximum {
            return Err(CodecError::message(format!(
                "{description} length {len} exceeds {maximum}"
            )));
        }
        Ok(len)
    }

    fn enter(&mut self) -> Result<(), CodecError> {
        if self.depth >= MAX_NESTING_DEPTH {
            return Err(CodecError::message(format!(
                "Raft value nesting exceeds {MAX_NESTING_DEPTH}"
            )));
        }
        self.depth += 1;
        Ok(())
    }

    fn visit_counted_seq<V>(&mut self, len: usize, visitor: V) -> Result<V::Value, CodecError>
    where
        V: Visitor<'de>,
    {
        self.enter()?;
        let mut access = CountedSeqAccess {
            deserializer: self,
            remaining: len,
        };
        let result = visitor.visit_seq(&mut access);
        let remaining = access.remaining;
        drop(access);
        self.depth -= 1;
        let value = result?;
        if remaining != 0 {
            return Err(CodecError::message(format!(
                "sequence decoder left {remaining} elements unread"
            )));
        }
        Ok(value)
    }
}

struct CountedSeqAccess<'a, 'de> {
    deserializer: &'a mut PayloadDeserializer<'de>,
    remaining: usize,
}

impl<'de> SeqAccess<'de> for CountedSeqAccess<'_, 'de> {
    type Error = CodecError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        seed.deserialize(&mut *self.deserializer).map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        // Avoid allowing an authenticated but malformed peer to force a large
        // eager allocation. Collections grow as their elements are validated.
        Some(self.remaining.min(4096))
    }
}

struct CountedMapAccess<'a, 'de> {
    deserializer: &'a mut PayloadDeserializer<'de>,
    remaining: usize,
    waiting_for_value: bool,
}

impl<'de> MapAccess<'de> for CountedMapAccess<'_, 'de> {
    type Error = CodecError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        if self.waiting_for_value {
            return Err(CodecError::message(
                "Raft map decoder requested a key before its value",
            ));
        }
        if self.remaining == 0 {
            return Ok(None);
        }
        self.waiting_for_value = true;
        seed.deserialize(&mut *self.deserializer).map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        if !self.waiting_for_value {
            return Err(CodecError::message(
                "Raft map decoder requested a value without a key",
            ));
        }
        let value = seed.deserialize(&mut *self.deserializer)?;
        self.waiting_for_value = false;
        self.remaining -= 1;
        Ok(value)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.remaining.min(4096))
    }
}

struct EnumDecoder<'a, 'de> {
    deserializer: &'a mut PayloadDeserializer<'de>,
    variant: u32,
}

struct VariantDecoder<'a, 'de> {
    deserializer: &'a mut PayloadDeserializer<'de>,
}

impl<'de, 'a> EnumAccess<'de> for EnumDecoder<'a, 'de> {
    type Error = CodecError;
    type Variant = VariantDecoder<'a, 'de>;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let variant = seed.deserialize(self.variant.into_deserializer())?;
        Ok((
            variant,
            VariantDecoder {
                deserializer: self.deserializer,
            },
        ))
    }
}

impl<'de> VariantAccess<'de> for VariantDecoder<'_, 'de> {
    type Error = CodecError;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        seed.deserialize(self.deserializer)
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        de::Deserializer::deserialize_tuple(self.deserializer, len, visitor)
    }

    fn struct_variant<V>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        de::Deserializer::deserialize_tuple(self.deserializer, fields.len(), visitor)
    }
}

impl<'de> de::Deserializer<'de> for &mut PayloadDeserializer<'de> {
    type Error = CodecError;

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(CodecError::message(
            "self-describing values are not supported by Raft V1",
        ))
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.take_u8()? {
            0 => visitor.visit_bool(false),
            1 => visitor.visit_bool(true),
            tag => Err(CodecError::message(format!(
                "invalid Raft boolean tag {tag}"
            ))),
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i8(i8::from_be_bytes(self.take_array()?))
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i16(i16::from_be_bytes(self.take_array()?))
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i32(i32::from_be_bytes(self.take_array()?))
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i64(i64::from_be_bytes(self.take_array()?))
    }

    fn deserialize_i128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i128(i128::from_be_bytes(self.take_array()?))
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u8(self.take_u8()?)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u16(u16::from_be_bytes(self.take_array()?))
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u32(u32::from_be_bytes(self.take_array()?))
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u64(u64::from_be_bytes(self.take_array()?))
    }

    fn deserialize_u128<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u128(u128::from_be_bytes(self.take_array()?))
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_f32(f32::from_bits(u32::from_be_bytes(self.take_array()?)))
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_f64(f64::from_bits(u64::from_be_bytes(self.take_array()?)))
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let scalar = u32::from_be_bytes(self.take_array()?);
        let value = char::from_u32(scalar)
            .ok_or_else(|| CodecError::message(format!("invalid Unicode scalar {scalar}")))?;
        visitor.visit_char(value)
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let len = self.take_len(MAX_STRING_BYTES, "string")?;
        let bytes = self.take(len)?;
        let value = std::str::from_utf8(bytes)
            .map_err(|error| CodecError::message(format!("invalid Raft UTF-8 string: {error}")))?;
        visitor.visit_borrowed_str(value)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let len = self.take_len(MAX_STRING_BYTES, "string")?;
        let bytes = self.take(len)?;
        let value = std::str::from_utf8(bytes)
            .map_err(|error| CodecError::message(format!("invalid Raft UTF-8 string: {error}")))?;
        visitor.visit_string(value.to_owned())
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let len = self.take_len(MAX_NESTED_BYTE_STRING_BYTES, "byte string")?;
        visitor.visit_borrowed_bytes(self.take(len)?)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let len = self.take_len(MAX_NESTED_BYTE_STRING_BYTES, "byte string")?;
        visitor.visit_byte_buf(self.take(len)?.to_vec())
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.take_u8()? {
            0 => visitor.visit_none(),
            1 => visitor.visit_some(self),
            tag => Err(CodecError::message(format!(
                "invalid Raft option tag {tag}"
            ))),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let len = self.take_len(MAX_COLLECTION_ELEMENTS, "sequence")?;
        self.visit_counted_seq(len, visitor)
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if len > MAX_COLLECTION_ELEMENTS {
            return Err(CodecError::message("tuple exceeds Raft V1 element limit"));
        }
        self.visit_counted_seq(len, visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let len = self.take_len(MAX_COLLECTION_ELEMENTS, "map")?;
        self.enter()?;
        let mut access = CountedMapAccess {
            deserializer: self,
            remaining: len,
            waiting_for_value: false,
        };
        let result = visitor.visit_map(&mut access);
        let remaining = access.remaining;
        let waiting_for_value = access.waiting_for_value;
        drop(access);
        self.depth -= 1;
        let value = result?;
        if remaining != 0 || waiting_for_value {
            return Err(CodecError::message(format!(
                "map decoder left {remaining} entries unread"
            )));
        }
        Ok(value)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_tuple(fields.len(), visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let variant = u32::from_be_bytes(self.take_array()?);
        self.enter()?;
        let result = visitor.visit_enum(EnumDecoder {
            deserializer: self,
            variant,
        });
        self.depth -= 1;
        result
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(CodecError::message(
            "ignored self-describing values are not supported by Raft V1",
        ))
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Example {
        number: u16,
        label: String,
        optional: Option<u8>,
    }

    fn envelope(kind: ValueKind, payload: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len());
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&VERSION.to_be_bytes());
        bytes.extend_from_slice(&(kind as u16).to_be_bytes());
        bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }

    fn envelope_header(kind: ValueKind, declared_len: usize) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(HEADER_LEN);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&VERSION.to_be_bytes());
        bytes.extend_from_slice(&(kind as u16).to_be_bytes());
        bytes.extend_from_slice(&(declared_len as u32).to_be_bytes());
        bytes
    }

    #[test]
    fn envelope_caps_distinguish_durable_aggregates_from_rpc_and_log_values() {
        for kind in [
            ValueKind::StoredCertificationState,
            ValueKind::StoredOpenRaftState,
            ValueKind::CertificationSnapshot,
            ValueKind::OpenRaftSnapshot,
        ] {
            assert_eq!(
                envelope_payload_limit(kind),
                MAX_DURABLE_AGGREGATE_PAYLOAD_BYTES
            );
        }

        for kind in [
            ValueKind::StoredVote,
            ValueKind::StoredLogEntry,
            ValueKind::StoredPurgedLogId,
            ValueKind::AppendEntriesRequest,
            ValueKind::InstallSnapshotRequest,
            ValueKind::ForwardCertifyRequest,
        ] {
            assert_eq!(
                envelope_payload_limit(kind),
                MAX_CONSENSUS_RPC_PAYLOAD_BYTES
            );
        }
        assert_eq!(
            MAX_NESTED_BYTE_STRING_BYTES,
            MAX_CONSENSUS_RPC_PAYLOAD_BYTES
        );
    }

    #[test]
    fn decoder_applies_the_kind_specific_cap_before_payload_allocation() {
        let above_rpc_limit = MAX_CONSENSUS_RPC_PAYLOAD_BYTES + 1;
        let rpc_error = decode::<()>(
            ValueKind::AppendEntriesRequest,
            &envelope_header(ValueKind::AppendEntriesRequest, above_rpc_limit),
        )
        .unwrap_err();
        assert!(rpc_error.to_string().contains("exceeds"));

        let durable_error = decode::<()>(
            ValueKind::StoredOpenRaftState,
            &envelope_header(ValueKind::StoredOpenRaftState, above_rpc_limit),
        )
        .unwrap_err();
        assert!(
            durable_error
                .to_string()
                .contains("envelope length does not match")
        );

        let above_durable_limit = MAX_DURABLE_AGGREGATE_PAYLOAD_BYTES + 1;
        let durable_limit_error = decode::<()>(
            ValueKind::OpenRaftSnapshot,
            &envelope_header(ValueKind::OpenRaftSnapshot, above_durable_limit),
        )
        .unwrap_err();
        assert!(durable_limit_error.to_string().contains("exceeds"));
    }

    #[test]
    fn v1_round_trip_is_deterministic_and_fixed_width() {
        let value = Example {
            number: 7,
            label: "hi".into(),
            optional: Some(9),
        };
        let first = encode(ValueKind::StoredVote, &value).unwrap();
        let second = encode(ValueKind::StoredVote, &value).unwrap();

        assert_eq!(first, second);
        assert_eq!(
            first,
            [
                MAGIC.as_slice(),
                VERSION.to_be_bytes().as_slice(),
                (ValueKind::StoredVote as u16).to_be_bytes().as_slice(),
                10_u32.to_be_bytes().as_slice(),
                &[0, 7, 0, 0, 0, 2, b'h', b'i', 1, 9],
            ]
            .concat()
        );
        assert_eq!(
            decode::<Example>(ValueKind::StoredVote, &first).unwrap(),
            value
        );
    }

    #[test]
    fn every_truncation_and_trailing_data_is_rejected() {
        let encoded = encode(ValueKind::VoteRequest, &vec![1_u64, 2, 3]).unwrap();
        for end in 0..encoded.len() {
            assert!(
                decode::<Vec<u64>>(ValueKind::VoteRequest, &encoded[..end]).is_err(),
                "accepted truncation at byte {end}"
            );
        }

        let mut trailing = encoded;
        trailing.push(0);
        assert!(decode::<Vec<u64>>(ValueKind::VoteRequest, &trailing).is_err());
    }

    #[test]
    fn wrong_version_domain_and_invalid_tags_are_rejected() {
        let encoded = encode(ValueKind::VoteRequest, &Some(4_u64)).unwrap();
        assert!(decode::<Option<u64>>(ValueKind::VoteResponse, &encoded).is_err());

        let mut wrong_version = encoded.clone();
        wrong_version[8..10].copy_from_slice(&2_u16.to_be_bytes());
        assert!(decode::<Option<u64>>(ValueKind::VoteRequest, &wrong_version).is_err());

        let invalid_option = envelope(ValueKind::VoteRequest, &[2]);
        assert!(decode::<Option<u64>>(ValueKind::VoteRequest, &invalid_option).is_err());
    }

    #[test]
    fn decoder_rejects_oversized_lengths_before_allocating() {
        let oversized_sequence = envelope(
            ValueKind::InstallSnapshotRequest,
            &((MAX_COLLECTION_ELEMENTS as u32) + 1).to_be_bytes(),
        );
        assert!(decode::<Vec<u8>>(ValueKind::InstallSnapshotRequest, &oversized_sequence).is_err());

        let oversized_string = envelope(
            ValueKind::ForwardCertifyRequest,
            &((MAX_STRING_BYTES as u32) + 1).to_be_bytes(),
        );
        assert!(decode::<String>(ValueKind::ForwardCertifyRequest, &oversized_string).is_err());
    }
}
