use std::cell::Cell;
use std::rc::Rc;

use crate::io::read_exact_at;
use crate::{
    ComponentCodec, INDEX_FORMAT_VERSION, IndexError, IndexFileRead, IndexKind,
    MAX_INDEX_BLOCK_BYTES, MAX_INDEX_DECODED_BLOCK_BYTES,
};

const COMPONENT_MAGIC: &[u8; 8] = b"ANVIDX02";
const HEADER_BYTES: usize = 54;
const CODEC_REVISION: u8 = 1;

pub(crate) fn encode_component(
    kind: IndexKind,
    component_tag: u8,
    codec: ComponentCodec,
    body: Vec<u8>,
) -> Result<Vec<u8>, IndexError> {
    let body_length = u64::try_from(body.len()).map_err(|_| IndexError::OffsetOverflow)?;
    let encoded_length = HEADER_BYTES
        .checked_add(body.len())
        .ok_or(IndexError::OffsetOverflow)?;
    if encoded_length > MAX_INDEX_BLOCK_BYTES {
        return Err(IndexError::ResourceLimit {
            needed: encoded_length,
            limit: MAX_INDEX_BLOCK_BYTES,
        });
    }
    let mut output = Vec::with_capacity(encoded_length);
    output.extend_from_slice(COMPONENT_MAGIC);
    output.extend_from_slice(&INDEX_FORMAT_VERSION.to_le_bytes());
    output.push(kind as u8);
    output.push(component_tag);
    output.push(codec as u8);
    output.push(CODEC_REVISION);
    output.extend_from_slice(&body_length.to_le_bytes());
    output.extend_from_slice(blake3::hash(&body).as_bytes());
    output.extend_from_slice(&body);
    Ok(output)
}

pub(crate) async fn read_component_file<F: IndexFileRead>(
    file: &F,
    expected_kind: IndexKind,
    expected_component_tag: u8,
    expected_codecs: &[ComponentCodec],
) -> Result<DecodedComponent<Vec<u8>>, IndexError> {
    let header = read_exact_at(file, 0, HEADER_BYTES).await?;
    let header = header.as_ref();
    if &header[..8] != COMPONENT_MAGIC {
        return Err(IndexError::InvalidFormat("index component magic"));
    }
    if u16::from_le_bytes(header[8..10].try_into().unwrap()) != INDEX_FORMAT_VERSION {
        return Err(IndexError::InvalidFormat(
            "unsupported index component version",
        ));
    }
    if IndexKind::from_tag(header[10])? != expected_kind || header[11] != expected_component_tag {
        return Err(IndexError::InvalidFormat("index component identity"));
    }
    let codec = ComponentCodec::from_tag(header[12])?;
    if header[13] != CODEC_REVISION || !expected_codecs.contains(&codec) {
        return Err(IndexError::InvalidFormat("index component codec"));
    }
    let length = usize::try_from(u64::from_le_bytes(header[14..22].try_into().unwrap()))
        .map_err(|_| IndexError::OffsetOverflow)?;
    if HEADER_BYTES
        .checked_add(length)
        .is_none_or(|length| length > MAX_INDEX_BLOCK_BYTES)
    {
        return Err(IndexError::InvalidFormat("index component length"));
    }
    let body = read_exact_at(file, HEADER_BYTES as u64, length).await?;
    if blake3::hash(body.as_ref()).as_bytes() != &header[22..54] {
        return Err(IndexError::Integrity);
    }
    let end = u64::try_from(HEADER_BYTES)
        .ok()
        .and_then(|header| {
            u64::try_from(length)
                .ok()
                .and_then(|length| header.checked_add(length))
        })
        .ok_or(IndexError::OffsetOverflow)?;
    if !file.read_at(end, 1).await?.as_ref().is_empty() {
        return Err(IndexError::InvalidFormat("trailing index component bytes"));
    }
    Ok(DecodedComponent {
        encoded_bytes: u64::try_from(HEADER_BYTES)
            .ok()
            .and_then(|header| header.checked_add(u64::try_from(length).ok()?))
            .ok_or(IndexError::OffsetOverflow)?,
        body: body.into_vec(),
    })
}

pub(crate) fn decode_component_bytes<'a>(
    bytes: &'a [u8],
    expected_kind: IndexKind,
    expected_component_tag: u8,
    expected_codecs: &[ComponentCodec],
) -> Result<DecodedComponent<&'a [u8]>, IndexError> {
    if bytes.len() < HEADER_BYTES
        || bytes.len() > MAX_INDEX_BLOCK_BYTES
        || &bytes[..8] != COMPONENT_MAGIC
    {
        return Err(IndexError::InvalidFormat("index component magic"));
    }
    if u16::from_le_bytes(bytes[8..10].try_into().unwrap()) != INDEX_FORMAT_VERSION {
        return Err(IndexError::InvalidFormat(
            "unsupported index component version",
        ));
    }
    if IndexKind::from_tag(bytes[10])? != expected_kind || bytes[11] != expected_component_tag {
        return Err(IndexError::InvalidFormat("index component identity"));
    }
    let codec = ComponentCodec::from_tag(bytes[12])?;
    if bytes[13] != CODEC_REVISION || !expected_codecs.contains(&codec) {
        return Err(IndexError::InvalidFormat("index component codec"));
    }
    let length = usize::try_from(u64::from_le_bytes(bytes[14..22].try_into().unwrap()))
        .map_err(|_| IndexError::OffsetOverflow)?;
    if HEADER_BYTES.checked_add(length) != Some(bytes.len()) {
        return Err(IndexError::InvalidFormat("index component length"));
    }
    let body = &bytes[HEADER_BYTES..];
    if blake3::hash(&body).as_bytes() != &bytes[22..54] {
        return Err(IndexError::Integrity);
    }
    Ok(DecodedComponent {
        encoded_bytes: u64::try_from(bytes.len()).map_err(|_| IndexError::OffsetOverflow)?,
        body,
    })
}

#[derive(Debug)]
pub(crate) struct DecodedComponent<B> {
    pub(crate) encoded_bytes: u64,
    pub(crate) body: B,
}

#[derive(Debug, Default)]
pub(crate) struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    pub(crate) fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub(crate) fn bool(&mut self, value: bool) {
        self.u8(u8::from(value));
    }

    pub(crate) fn u32(&mut self, value: usize) -> Result<(), IndexError> {
        self.bytes.extend_from_slice(
            &u32::try_from(value)
                .map_err(|_| IndexError::OffsetOverflow)?
                .to_le_bytes(),
        );
        Ok(())
    }

    pub(crate) fn raw_u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn f32(&mut self, value: f32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn f64(&mut self, value: f64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn bytes(&mut self, value: &[u8]) -> Result<(), IndexError> {
        self.u32(value.len())?;
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    pub(crate) fn raw_bytes(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    pub(crate) fn string(&mut self, value: &str) -> Result<(), IndexError> {
        self.bytes(value.as_bytes())
    }

    pub(crate) fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

#[derive(Debug)]
pub(crate) struct Decoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
    budget: DecodeBudget,
}

#[derive(Clone, Debug)]
pub(crate) struct DecodeBudget {
    used: Rc<Cell<usize>>,
}

impl DecodeBudget {
    pub(crate) fn new() -> Self {
        Self {
            used: Rc::new(Cell::new(0)),
        }
    }

    pub(crate) fn charge(&self, bytes: usize) -> Result<(), IndexError> {
        let used = self
            .used
            .get()
            .checked_add(bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        if used > MAX_INDEX_DECODED_BLOCK_BYTES {
            return Err(IndexError::InvalidFormat(
                "decoded index component exceeds memory limit",
            ));
        }
        self.used.set(used);
        Ok(())
    }

    pub(crate) fn checkpoint(&self) -> usize {
        self.used.get()
    }

    pub(crate) fn rewind(&self, checkpoint: usize) {
        debug_assert!(checkpoint <= self.used.get());
        self.used.set(checkpoint);
    }
}

impl<'a> Decoder<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self::with_budget(bytes, DecodeBudget::new())
    }

    pub(crate) fn with_budget(bytes: &'a [u8], budget: DecodeBudget) -> Self {
        Self {
            bytes,
            cursor: 0,
            budget,
        }
    }

    pub(crate) fn budget(&self) -> DecodeBudget {
        self.budget.clone()
    }

    pub(crate) fn charge(&self, bytes: usize) -> Result<(), IndexError> {
        self.budget.charge(bytes)
    }

    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len() - self.cursor
    }

    /// Rejects corrupt counts before they can drive a large allocation. The
    /// encoded representation must contain at least `minimum_encoded_bytes`
    /// per element and the expanded fixed-size part must fit in the fixed
    /// shared decoded-block budget. This is deliberately larger than the
    /// encoded block: valid compact columns expand when reconstructed.
    pub(crate) fn guard_count<T>(
        &self,
        count: usize,
        minimum_encoded_bytes: usize,
    ) -> Result<(), IndexError> {
        let encoded = count
            .checked_mul(minimum_encoded_bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        let resident = count
            .checked_mul(std::mem::size_of::<T>().max(1))
            .ok_or(IndexError::OffsetOverflow)?;
        if encoded > self.remaining() {
            return Err(IndexError::InvalidFormat("index component element count"));
        }
        self.charge(resident)?;
        Ok(())
    }

    pub(crate) fn u8(&mut self) -> Result<u8, IndexError> {
        let value = *self
            .bytes
            .get(self.cursor)
            .ok_or(IndexError::InvalidFormat("truncated index component"))?;
        self.cursor += 1;
        Ok(value)
    }

    pub(crate) fn bool(&mut self) -> Result<bool, IndexError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(IndexError::InvalidFormat("invalid encoded boolean")),
        }
    }

    pub(crate) fn u32(&mut self) -> Result<u32, IndexError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, IndexError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    pub(crate) fn f32(&mut self) -> Result<f32, IndexError> {
        Ok(f32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    pub(crate) fn f64(&mut self) -> Result<f64, IndexError> {
        Ok(f64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    pub(crate) fn bytes(&mut self) -> Result<&'a [u8], IndexError> {
        let length = self.u32()? as usize;
        self.take(length)
    }

    pub(crate) fn fixed(&mut self, length: usize) -> Result<&'a [u8], IndexError> {
        self.take(length)
    }

    pub(crate) fn string(&mut self) -> Result<String, IndexError> {
        let bytes = self.bytes()?;
        self.charge(bytes.len())?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| IndexError::InvalidFormat("index component UTF-8"))
    }

    pub(crate) fn finish(&self) -> Result<(), IndexError> {
        if self.cursor == self.bytes.len() {
            Ok(())
        } else {
            Err(IndexError::InvalidFormat(
                "trailing index component body bytes",
            ))
        }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], IndexError> {
        let end = self
            .cursor
            .checked_add(length)
            .ok_or(IndexError::OffsetOverflow)?;
        let value = self
            .bytes
            .get(self.cursor..end)
            .ok_or(IndexError::InvalidFormat("truncated index component"))?;
        self.cursor = end;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use crate::io::tests::MemoryFile;

    use super::*;

    #[tokio::test]
    async fn v2_envelope_is_portable_and_integrity_checked() {
        let encoded = encode_component(
            IndexKind::Path,
            9,
            ComponentCodec::FixedRows,
            b"body".to_vec(),
        )
        .unwrap();
        let file = MemoryFile::new(encoded.clone());
        assert_eq!(
            read_component_file(&file, IndexKind::Path, 9, &[ComponentCodec::FixedRows],)
                .await
                .unwrap()
                .body,
            b"body"
        );

        let mut corrupt = encoded;
        *corrupt.last_mut().unwrap() ^= 1;
        let file = MemoryFile::new(corrupt);
        assert_eq!(
            read_component_file(&file, IndexKind::Path, 9, &[ComponentCodec::FixedRows],)
                .await
                .unwrap_err(),
            IndexError::Integrity
        );
    }

    #[tokio::test]
    async fn oversized_declared_body_is_rejected_before_reading_it() {
        let mut encoded =
            encode_component(IndexKind::Path, 9, ComponentCodec::FixedRows, Vec::new()).unwrap();
        encoded[14..22].copy_from_slice(&(MAX_INDEX_BLOCK_BYTES as u64).to_le_bytes());
        let file = MemoryFile::new(encoded);
        assert_eq!(
            read_component_file(&file, IndexKind::Path, 9, &[ComponentCodec::FixedRows])
                .await
                .unwrap_err(),
            IndexError::InvalidFormat("index component length")
        );
    }

    #[test]
    fn nested_decoders_share_one_decoded_block_budget() {
        let budget = DecodeBudget::new();
        let first = Decoder::with_budget(&[], budget.clone());
        let second = Decoder::with_budget(&[], budget);
        first.charge(MAX_INDEX_DECODED_BLOCK_BYTES - 1).unwrap();
        assert_eq!(
            second.charge(2).unwrap_err(),
            IndexError::InvalidFormat("decoded index component exceeds memory limit")
        );
    }
}
