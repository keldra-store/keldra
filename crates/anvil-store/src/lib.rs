//! Anvil's storage kernel.
//!
//! The kernel deliberately has no transaction lifecycle, snapshot protocol or
//! knowledge of payload contents. It stores immutable object versions and
//! atomically moves current heads subject to small CAS preconditions.

mod authz;
mod blob;
mod bootstrap;
mod clock;
mod key;
mod model;
mod program;
mod store;
mod watch;

pub use authz::{
    AtomicRealmBinding, AuthzBatchCheck, AuthzConsistency, AuthzRepository, AuthzRevision,
    AuthzScope, AuthzStoreError, AuthzStoreLimits, BindSchemaRequest, BoundRealm,
    ProtectedRealmOwnership, PublishSchemaRequest, PublishedSchema, RealmBinding, RealmSnapshot,
    SYSTEM_STORAGE_TENANT_ID, SchemaDigest, SchemaId, SchemaRef, StorageTenantId,
    TupleBatchReceipt, TupleBatchRequest, TupleMutation, TupleMutationKind,
};
pub use blob::{BlobReader, BlobRef, BlobStore, BlobUpload};
pub use bootstrap::{
    ApplicationCredential, CredentialRepository, SYSTEM_BOOTSTRAP_VERSION, SYSTEM_SCHEMA_ID,
    SystemBootstrapError, SystemBootstrapRequest, SystemBootstrapState, system_schema,
};
pub use clock::VersionClock;
pub use key::ObjectKey;
pub use model::{
    BatchOperation, BatchOutcome, BucketPolicy, DeleteRequest, Head, INLINE_PAYLOAD_MAX_BYTES,
    InlinePayload, MAX_BUCKET_POLICY_PREFIX_BYTES, MAX_BUCKET_POLICY_PREFIXES, MutationError,
    MutationReceipt, Object, Precondition, PublishRequest, PutRequest, Version, VersionId,
};
pub use program::{
    AppliedProgramCommit, AppliedProgramReceipt, OutboxEntry, OutboxPayload, PreparedArtifact,
    PreparedArtifactBatch, PreparedArtifactFuture, PreparedArtifactKind, PreparedArtifactRef,
    PreparedArtifactRepository, PreparedBundleHash, PreparedBundleRef, PreparedProgramBundle,
    ProgramCommit, ProgramDurabilityClassHash, ProgramDurabilityEvidence,
    ProgramDurabilityEvidenceHash, ProgramDurabilityScope, ProgramExecutionLease, ProgramHash,
    ProgramStoreError, StoreProgramEngine, VerifiedProgramDefinition,
};
pub use store::{BatchGetSelection, Store, StoreOptions};
pub use watch::{InvalidationStateHint, LocalInvalidation, MAX_LOCAL_INVALIDATION_SCAN_RECORDS};
