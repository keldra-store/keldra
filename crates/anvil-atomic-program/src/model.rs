use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::template::PathTemplate;

/// The only definition schema understood by this crate.
pub const DEFINITION_SCHEMA_VERSION: u16 = 1;

/// Canonical object-address limits shared with the storage kernel.
pub const MAX_OBJECT_TENANT_BYTES: usize = 256;
pub const MAX_OBJECT_BUCKET_BYTES: usize = 256;
pub const MAX_OBJECT_PATH_BYTES: usize = 4096;

/// A canonical object address. Ordering is the canonical local lock order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectPath {
    pub tenant: String,
    pub bucket: String,
    pub path: String,
}

impl ObjectPath {
    pub fn new(
        tenant: impl Into<String>,
        bucket: impl Into<String>,
        path: impl Into<String>,
    ) -> Result<Self, String> {
        let path = Self {
            tenant: tenant.into(),
            bucket: bucket.into(),
            path: path.into(),
        };
        if !canonical_component(&path.tenant, MAX_OBJECT_TENANT_BYTES)
            || !canonical_component(&path.bucket, MAX_OBJECT_BUCKET_BYTES)
            || path.path.is_empty()
            || path.path.len() > MAX_OBJECT_PATH_BYTES
            || path.path.starts_with('/')
            || path.path.ends_with('/')
            || path
                .path
                .split('/')
                .any(|segment| segment.is_empty() || matches!(segment, "." | ".."))
            || path.path.chars().any(char::is_control)
        {
            return Err("object address has an invalid tenant, bucket, or path".into());
        }
        Ok(path)
    }
}

/// Tenant identity comes from authenticated request context, never program input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvocationContext {
    pub tenant: String,
}

impl InvocationContext {
    pub fn new(tenant: impl Into<String>) -> Result<Self, String> {
        let tenant = tenant.into();
        if !canonical_component(&tenant, MAX_OBJECT_TENANT_BYTES) {
            return Err(format!(
                "tenant must be one canonical component of at most {MAX_OBJECT_TENANT_BYTES} bytes"
            ));
        }
        Ok(Self { tenant })
    }
}

fn canonical_component(value: &str, maximum_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum_bytes
        && !value.contains('/')
        && !value.chars().any(char::is_control)
}

/// Hard limits are part of the immutable definition, not invocation hints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramCaps {
    pub max_paths: usize,
    pub max_writes: usize,
    pub max_operations: usize,
    pub max_input_bytes: usize,
    pub max_document_bytes: usize,
}

impl Default for ProgramCaps {
    fn default() -> Self {
        Self {
            max_paths: 16,
            max_writes: 16,
            max_operations: 64,
            max_input_bytes: 1024 * 1024,
            max_document_bytes: 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Cardinality {
    One,
    Optional,
    Repeated { max: usize },
}

impl Cardinality {
    pub(crate) fn maximum(self) -> usize {
        match self {
            Self::One | Self::Optional => 1,
            Self::Repeated { max } => max,
        }
    }

    pub(crate) fn accepts(self, count: usize) -> bool {
        match self {
            Self::One => count == 1,
            Self::Optional => count <= 1,
            Self::Repeated { max } => count <= max,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DocumentAccess {
    ReadOnly,
    ReadWrite,
}

/// Conservative authorization intent for one fully expanded path.
///
/// These flags are derived from the immutable program definition before any
/// lock or read is taken. They describe every operation the bounded program
/// can perform on this particular document reference, not merely the write it
/// happens to produce for one invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProgramPathIntent {
    pub get: bool,
    pub put: bool,
    pub delete: bool,
}

/// One fully expanded document path that the caller must authorize before
/// locks or reads are taken. Zanzibar relation names remain an API-layer
/// concern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpandedProgramPath {
    pub path: ObjectPath,
    pub intent: ProgramPathIntent,
}

/// One named set of paths accepted by a program.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DocumentSpec {
    pub name: String,
    pub path: PathTemplate,
    pub cardinality: Cardinality,
    pub access: DocumentAccess,
    #[serde(default)]
    pub allow_initial_json: bool,
}

/// A static document position in the bounded program.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DocumentRef {
    pub slot: String,
    #[serde(default)]
    pub index: usize,
}

impl DocumentRef {
    pub fn one(slot: impl Into<String>) -> Self {
        Self {
            slot: slot.into(),
            index: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonPointerRef {
    pub document: DocumentRef,
    /// An RFC 6901 JSON pointer. The empty string selects the whole value.
    pub pointer: String,
}

impl JsonPointerRef {
    pub fn new(document: DocumentRef, pointer: impl Into<String>) -> Self {
        Self {
            document,
            pointer: pointer.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DocumentView {
    Before,
    Current,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DocumentValueRef {
    pub value: JsonPointerRef,
    pub view: DocumentView,
}

/// Scalar or structured JSON supplied without executing client code.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum InputValue {
    Literal { value: Value },
    Input { name: String },
}

/// A value used by an operation after its source view is selected explicitly.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ValueSource {
    Literal { value: Value },
    Input { name: String },
    Document { source: DocumentValueRef },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Comparison {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// Exact integer semantics. JSON floating-point coercion is never permitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum IntegerType {
    I64 { min: Option<i64>, max: Option<i64> },
    U64 { min: Option<u64>, max: Option<u64> },
}

/// Assertions are evaluated only against the stored preimage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Assertion {
    Exists {
        document: DocumentRef,
    },
    Absent {
        document: DocumentRef,
    },
    JsonEqual {
        actual: JsonPointerRef,
        expected: InputValue,
    },
    IntegerCompare {
        actual: JsonPointerRef,
        comparison: Comparison,
        expected: InputValue,
        numeric_type: IntegerType,
    },
}

/// The bounded deterministic mutation language.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    SetValue {
        target: JsonPointerRef,
        value: ValueSource,
    },
    /// Removing the root deletes the document; removing another pointer edits JSON.
    RemoveValue { target: JsonPointerRef },
    CheckedIntegerAdd {
        target: JsonPointerRef,
        delta: InputValue,
        numeric_type: IntegerType,
    },
    CopyValue {
        source: DocumentValueRef,
        target: JsonPointerRef,
    },
    /// Replace a complete document with an opaque invocation blob.
    ReplaceOpaque {
        document: DocumentRef,
        input: String,
        content_type: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReturnDefinition {
    pub name: String,
    pub value: DocumentValueRef,
}

/// An immutable program definition stored as an ordinary object below
/// `_anvil/programs/`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramDefinition {
    pub schema_version: u16,
    pub documents: Vec<DocumentSpec>,
    #[serde(default)]
    pub assertions: Vec<Assertion>,
    #[serde(default)]
    pub operations: Vec<Operation>,
    #[serde(default)]
    pub returns: Vec<ReturnDefinition>,
    pub caps: ProgramCaps,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExpectedHead {
    Any,
    Absent,
    Version { version: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PathBinding {
    pub path: ObjectPath,
    #[serde(default)]
    pub template_values: BTreeMap<String, String>,
    pub expected_head: ExpectedHead,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_json: Option<Value>,
}

/// Concrete paths and values for one program execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProgramInvocation {
    /// BLAKE3 identity of the full ordinary object address from which the
    /// server loaded this program definition.
    pub program_path_hash: [u8; 32],
    pub command_id: String,
    /// Canonical lowercase hex for a 32-byte digest constructed by the server.
    pub input_fingerprint: String,
    #[serde(default)]
    pub arguments: BTreeMap<String, String>,
    #[serde(default)]
    pub inputs: BTreeMap<String, Value>,
    #[serde(default)]
    pub blobs: BTreeMap<String, Vec<u8>>,
    #[serde(default)]
    pub bindings: BTreeMap<String, Vec<PathBinding>>,
}

/// The caller-controlled portion of an invocation. Identity and the input
/// fingerprint are constructed by the server so every client gets identical
/// idempotency semantics.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramInput {
    #[serde(default)]
    pub arguments: BTreeMap<String, String>,
    #[serde(default)]
    pub inputs: BTreeMap<String, Value>,
    #[serde(default)]
    pub blobs: BTreeMap<String, Vec<u8>>,
    #[serde(default)]
    pub bindings: BTreeMap<String, Vec<PathBinding>>,
}

impl ProgramInvocation {
    pub fn from_input(
        program_path_hash: [u8; 32],
        command_id: impl Into<String>,
        input: ProgramInput,
    ) -> Result<Self, String> {
        let canonical_input = serde_json::to_vec(&input)
            .map_err(|error| format!("cannot encode program input: {error}"))?;
        let mut fingerprint = blake3::Hasher::new();
        fingerprint.update(b"anvil.atomic-program.input.v2");
        fingerprint.update(&program_path_hash);
        fingerprint.update(&canonical_input);
        Ok(Self {
            program_path_hash,
            command_id: command_id.into(),
            input_fingerprint: fingerprint.finalize().to_hex().to_string(),
            arguments: input.arguments,
            inputs: input.inputs,
            blobs: input.blobs,
            bindings: input.bindings,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum StoredValue {
    Json(Value),
    Opaque(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VersionedDocument {
    pub version: String,
    /// `None` is a versioned tombstone, distinct from a path never seen.
    pub value: Option<StoredValue>,
    pub content_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProgramSnapshot {
    /// Missing entries are treated as absent.
    pub documents: BTreeMap<ObjectPath, VersionedDocument>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ObservedHead {
    NeverExisted,
    Version { version: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeadPrecondition {
    pub path: ObjectPath,
    pub expected: ObservedHead,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VersionedWrite {
    pub path: ObjectPath,
    pub expected: ObservedHead,
    /// `None` is a tombstone/delete.
    pub value: Option<StoredValue>,
    pub content_type: Option<String>,
}

/// Stored atomically with document heads. It is the durable replay contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommandReceipt {
    /// BLAKE3 identity of the full ordinary program object address.
    pub program_path_hash: [u8; 32],
    pub command_id: String,
    pub input_fingerprint: String,
    pub outputs: BTreeMap<String, Value>,
}

/// A storage-neutral atomic apply request produced after deterministic evaluation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AtomicWriteBundle {
    pub head_preconditions: Vec<HeadPrecondition>,
    pub writes: Vec<VersionedWrite>,
    pub receipt: CommandReceipt,
    pub outputs: BTreeMap<String, Value>,
}
