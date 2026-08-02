use crate::model::ObjectPath;

/// Failures are explicit and side-effect free: no write has happened when this
/// crate returns an error.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EngineError {
    #[error("invalid program definition: {0}")]
    InvalidDefinition(String),
    #[error("invalid invocation: {0}")]
    InvalidInvocation(String),
    #[error("snapshot read failed: {0}")]
    Read(String),
    #[error("head precondition failed for {path:?}: {reason}")]
    HeadPrecondition { path: ObjectPath, reason: String },
    #[error("program concurrency requirement failed for {path:?}: {reason}")]
    ProgramConcurrency { path: ObjectPath, reason: String },
    #[error("assertion {index} failed: {reason}")]
    Assertion { index: usize, reason: String },
    #[error("operation {index} failed: {reason}")]
    Operation { index: usize, reason: String },
    #[error("return `{name}` failed: {reason}")]
    Return { name: String, reason: String },
    #[error("reader returned an invalid snapshot: {0}")]
    InvalidSnapshot(String),
    #[error("program execution limit exceeded: {0}")]
    LimitExceeded(String),
}
