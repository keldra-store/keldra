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
    DEFAULT_AUTHZ_RECEIPT_MAX_BYTES, DEFAULT_AUTHZ_RECEIPT_MAX_ENTRIES,
    DEFAULT_AUTHZ_RECEIPT_RETENTION_SECONDS, ProtectedRealmOwnership, PublishSchemaRequest,
    PublishedSchema, RealmBinding, RealmSnapshot, SYSTEM_STORAGE_TENANT_ID, SchemaDigest, SchemaId,
    SchemaRef, StorageTenantId, TupleBatchReceipt, TupleBatchRequest, TupleMutation,
    TupleMutationKind,
};
pub use blob::{AWAITING_PUBLISH, BlobReader, BlobRef, BlobReferenceState, BlobStore, BlobUpload};
pub use bootstrap::{
    ApplicationCredential, ApplicationCredentialRequest, ApplicationRoleTarget,
    BucketApplicationRole, CreateBucketReceipt, CreateBucketRequest, CredentialMutationReceipt,
    CredentialRepository, CredentialRepositoryError, ProvisionTenantReceipt,
    ProvisionTenantRequest, SYSTEM_BOOTSTRAP_VERSION, SYSTEM_SCHEMA_ID, SetApplicationRoleReceipt,
    SetApplicationRoleRequest, SystemApplicationRole, SystemBootstrapError, SystemBootstrapRequest,
    SystemBootstrapState, TenantApplicationRole, system_schema,
};
pub use clock::VersionClock;
pub use key::ObjectKey;
pub use model::{
    BatchOperation, BatchOutcome, BucketPolicy, DeleteRequest, DeleteRetainedVersionOutcome,
    Durability, Head, MAX_BUCKET_POLICY_PREFIX_BYTES, MAX_BUCKET_POLICY_PREFIXES, MutationError,
    MutationReceipt, Object, ObjectVersioning, Precondition, PublishRequest, PutMode, PutRequest,
    SMALL_BLOB_MAX_BYTES, Version, VersionId,
};
pub use program::{
    AppliedProgramCommit, CommittedProgramResult, PreparedBundleHash, PreparedBundleRef,
    PreparedProgramBundle, ProgramCommit, ProgramDurabilityClassHash, ProgramDurabilityEvidence,
    ProgramDurabilityEvidenceHash, ProgramDurabilityScope, ProgramExecutionLease, ProgramHash,
    ProgramStoreError, PublishedProgramVersion, StoreProgramEngine, VerifiedProgramDefinition,
};
pub use store::{
    BatchGetSelection, DEFAULT_AWAITING_PUBLISH_TTL_SECONDS, DEFAULT_MUTATION_RECEIPT_MAX_BYTES,
    DEFAULT_MUTATION_RECEIPT_MAX_ENTRIES, DEFAULT_MUTATION_RECEIPT_RETENTION_SECONDS,
    ListObjectsPage, MAX_LIST_OBJECT_VERSIONS, MAX_LIST_OBJECTS, MutationReceiptRetention,
    OpenedObject, Store, StoreOptions,
};
pub use watch::{
    DEFAULT_WATCH_MAX_BYTES, DEFAULT_WATCH_MAX_ENTRIES, InvalidationStateHint, LocalChange,
    LocalInvalidation, MAX_LOCAL_INVALIDATION_SCAN_RECORDS, ObjectHeadChange, ObjectHeadChangeKind,
    SourceId, WatchCursor, WatchError, WatchJournalStatus, WatchPage, WatchRetention, WatchScope,
    WatchStart,
};
