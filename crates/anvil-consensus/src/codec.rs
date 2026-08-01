use bincode::Options;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

pub(crate) const MAX_ENCODED_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum CodecError {
    #[error("encoded consensus value exceeds {MAX_ENCODED_BYTES} bytes")]
    TooLarge,
    #[error("consensus binary codec error: {0}")]
    Invalid(String),
}

fn options() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_ENCODED_BYTES as u64)
        .reject_trailing_bytes()
}

pub(crate) fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, CodecError> {
    let bytes = options()
        .serialize(value)
        .map_err(|error| CodecError::Invalid(error.to_string()))?;
    if bytes.len() > MAX_ENCODED_BYTES {
        return Err(CodecError::TooLarge);
    }
    Ok(bytes)
}

pub(crate) fn encoded_len<T: Serialize + ?Sized>(value: &T) -> Result<u64, CodecError> {
    let bytes = options()
        .serialized_size(value)
        .map_err(|error| CodecError::Invalid(error.to_string()))?;
    if bytes > MAX_ENCODED_BYTES as u64 {
        return Err(CodecError::TooLarge);
    }
    Ok(bytes)
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
    if bytes.len() > MAX_ENCODED_BYTES {
        return Err(CodecError::TooLarge);
    }
    options()
        .deserialize(bytes)
        .map_err(|error| CodecError::Invalid(error.to_string()))
}
