use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum IndexError {
    #[error("index file `{0}` does not exist")]
    FileNotFound(String),
    #[error("index file ended at {actual} bytes; {expected} bytes were required")]
    UnexpectedEof { expected: u64, actual: u64 },
    #[error("index offset or length is outside the supported range")]
    OffsetOverflow,
    #[error("invalid index format: {0}")]
    InvalidFormat(&'static str),
    #[error("index bytes failed their BLAKE3 integrity check")]
    Integrity,
    #[error("index records must have unique keys in ascending byte order")]
    UnsortedRecords,
    #[error("invalid index definition: {0}")]
    InvalidDefinition(String),
    #[error("invalid index query: {0}")]
    InvalidQuery(String),
    #[error("index payload could not be encoded: {0}")]
    Encode(String),
    #[error("index payload could not be decoded: {0}")]
    Decode(String),
    #[error("index I/O failed: {0}")]
    Io(String),
    #[error("index memory limit is {limit} bytes but this operation needs {needed} bytes")]
    ResourceLimit { needed: usize, limit: usize },
}
